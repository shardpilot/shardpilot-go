package shardpilot

// Review round 16 — regression pins. Each test fails on the pre-fix tree
// for its finding's exact reason (verified mechanically via targeted
// temporary reverts of the fix, with the test seams retained).

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── G16-1: the BUILT batch re-checks the purge state at handoff ─────────────

func TestBuiltBatchHandoffRechecksPurge(t *testing.T) {
	run := func(t *testing.T, batchSize int, drive func(t *testing.T, c *Client, capture *expWireCapture)) {
		script := &expScript{}
		script.push(200, expAssignedBody("1"))
		capture := &expWireCapture{}
		server := newExperimentServer(t, script, capture)
		defer server.Close()
		client := newExperimentClient(t, server.URL, func(cfg *Config) {
			cfg.BatchSize = batchSize
		})
		defer func() { _ = client.Close(context.Background()) }()
		client.SetConsent(true)

		// The real-subjects sentinel lands AFTER the dispatch-point purge
		// check and buildBatchIsolating produced the wire request, but
		// BEFORE publishRequestResult hands it to transport — the
		// check-then-build window. The seam fires on the worker goroutine
		// (both the dispatch and flush paths run there); the sentinel
		// machinery takes its own locks.
		var fired atomic.Bool
		client.builtBatchHandoffSeam = func(batch []Event) {
			if len(batch) == 0 || !fired.CompareAndSwap(false, true) {
				return
			}
			client.exp.mu.Lock()
			client.exp.applySentinelWithdrawalLocked("g16-1-scope", time.Now().UnixMilli())
			client.exp.mu.Unlock()
			client.purgeWithdrawnExperimentFacts()
		}
		// The fetch applies the assignment and enqueues its automatic
		// exposure fact; the armed seam catches the batch that carries it.
		if result := fetchAssignment(t, client, expTestScopeKey); result.VariantKey != "treatment" {
			t.Fatalf("setup fetch: %+v", result)
		}
		drive(t, client, capture)
		if !fired.Load() {
			t.Fatalf("test shape: the built-batch handoff seam never fired")
		}
		// The surviving pipeline still delivers ordinary work.
		if err := client.Enqueue(Event{Name: "filler_after_sentinel"}); err != nil {
			t.Fatalf("filler enqueue: %v", err)
		}
		if err := client.Flush(context.Background()); err != nil {
			t.Fatalf("filler flush: %v", err)
		}
		waitFor(t, 5*time.Second, "the filler delivers", func() bool {
			capture.mu.Lock()
			defer capture.mu.Unlock()
			for _, envelope := range capture.envelopes {
				if envelope["event_name"] == "filler_after_sentinel" {
					return true
				}
			}
			return false
		})
		if got := capture.exposures(); len(got) != 0 {
			t.Fatalf("a WITHDRAWN experiment fact egressed after the real-subjects purge: the sentinel landed between the dispatch-point check and the transport handoff, and the BUILT request was published without a post-build re-check (%v)", got)
		}
	}
	t.Run("worker_dispatch", func(t *testing.T) {
		// BatchSize 1: the enqueued exposure fact makes the worker publish
		// through publishWorkerBatch on its own. The batch settles either
		// as a counted drop (the handoff re-check withheld it) or as a
		// delivered exposure (the pre-fix egress the final assert names).
		run(t, 1, func(t *testing.T, c *Client, capture *expWireCapture) {
			waitFor(t, 5*time.Second, "the worker settles the fact batch", func() bool {
				return c.Snapshot().Dropped >= 1 || len(capture.exposures()) > 0
			})
		})
	})
	t.Run("explicit_flush", func(t *testing.T) {
		// BatchSize 8: nothing auto-publishes; the explicit Flush executes
		// flushAvailable's build on the worker.
		run(t, 8, func(t *testing.T, c *Client, capture *expWireCapture) {
			if err := c.Flush(context.Background()); err != nil {
				t.Fatalf("flush: %v", err)
			}
		})
	})
}

// ── G16-2: the sentinel cancels frozen capture debts ────────────────────────

func TestSentinelCancelsFrozenCaptureDebts(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer func() { _ = client.Close(context.Background()) }()
	client.SetConsent(true) // persisted grant: the spool write gate is open

	// A prior kill drop left a FROZEN captureFirst debt (its spool append
	// failed at drop time; the payload carries pre-sentinel facts).
	frozen := spoolEntry{id: "g16-2-frozen", ts: time.Now().UTC().Format(time.RFC3339Nano), raw: round5FactRaw("g16-2-frozen")}
	client.exp.mu.Lock()
	client.exp.durablePending[scopedIntentKey("g16-2-scope", "exp-frozen")] = expOwedSync{
		asOf: 1, drop: true, scope: "g16-2-scope", experimentKey: "exp-frozen",
		captureFirst: true, captureEntries: []spoolEntry{frozen},
	}
	client.exp.mu.Unlock()

	// The sentinel lands while the record clear FAILS: the clear becomes
	// owed (tombstoned) — and the under-lock spool sweep has already run.
	breakExperimentStorage(t, client)
	client.exp.mu.Lock()
	client.exp.applySentinelWithdrawalLocked("g16-2-scope", time.Now().UnixMilli())
	pendingClear := client.exp.durableClearPending
	client.exp.mu.Unlock()
	if !pendingClear {
		t.Fatalf("test shape: the failed clear must leave the whole-record clear owed")
	}

	// The next retry cycle runs while the record clear STILL fails — the
	// capture retry is not gated on it, and the spool itself is healthy.
	client.exp.retryDurableSync()
	client.spool.mu.Lock()
	_, respooled := client.spool.ids["g16-2-frozen"]
	client.spool.mu.Unlock()
	if respooled {
		t.Fatalf("the frozen capture debt survived the sentinel and its retry RE-APPENDED pre-sentinel exposure payloads into the spool AFTER sentinelSpoolPurgeUnderLock swept it: a crash/restart reloads them with no in-memory purge epoch and resends withdrawn subject-fact keys")
	}

	// The owed record clear itself is untouched by the cancellation and
	// still lands once storage heals.
	restoreExperimentStorage(t, client)
	client.exp.retryDurableSync()
	client.exp.mu.Lock()
	stillOwed := client.exp.durableClearPending
	client.exp.mu.Unlock()
	if stillOwed {
		t.Fatalf("the owed whole-record clear must still land on heal (the sentinel cancels only the frozen capture payloads, never the clear)")
	}
	client.spool.mu.Lock()
	_, respooledLate := client.spool.ids["g16-2-frozen"]
	client.spool.mu.Unlock()
	if respooledLate {
		t.Fatalf("the healed retry cycle must not respool the cancelled capture payload either")
	}
}

// ── G16-3: fact stamps ride the entry snapshot ──────────────────────────────

func TestFactStampRidesEntrySnapshot(t *testing.T) {
	stage := func(t *testing.T, seamStage string, emit func(t *testing.T, c *Client), countRaced func(capture *expWireCapture) int) {
		script := &expScript{}
		script.push(200, expAssignedBody("1"))
		capture := &expWireCapture{}
		server := newExperimentServer(t, script, capture)
		defer server.Close()
		client := newExperimentClient(t, server.URL, nil)
		defer func() { _ = client.Close(context.Background()) }()
		client.SetConsent(true)
		if result := fetchAssignment(t, client, expTestScopeKey); result.VariantKey != "treatment" {
			t.Fatalf("setup fetch: %+v", result)
		}
		// Deliver the automatic exposure so the raced emission below is the
		// only fact in flight.
		if err := client.Flush(context.Background()); err != nil {
			t.Fatalf("setup flush: %v", err)
		}

		// The sentinel's DECISIVE half (under e.mu) lands INSIDE the
		// emission — after the (entry, epoch) snapshot left e.mu, before
		// the fact builder runs. The off-lock pipeline purge stays blocked
		// on emitMu (the emission holds it) exactly like production, and
		// runs right after the call returns.
		flipped := false
		client.exp.consentRaceSeam = func(stage string) {
			if stage == seamStage && !flipped {
				flipped = true
				client.exp.mu.Lock()
				client.exp.applySentinelWithdrawalLocked("g16-3-scope", time.Now().UnixMilli())
				client.exp.mu.Unlock()
			}
		}
		emit(t, client)
		if !flipped {
			t.Fatalf("test shape: the %s seam never fired", seamStage)
		}
		client.purgeWithdrawnExperimentFacts()
		if err := client.Flush(context.Background()); err != nil {
			t.Fatalf("post-purge flush: %v", err)
		}
		if raced := countRaced(capture); raced != 0 {
			t.Fatalf("a fact built from a PRE-sentinel entry snapshot egressed after the purge: the builder stamped it with the post-sentinel epoch (loaded after the snapshot left e.mu), so the purge blocked on emitMu treated the withdrawn subject-fact key as fresh (%d raced fact(s) delivered)", raced)
		}
	}
	t.Run("outcome_build", func(t *testing.T) {
		stage(t, "outcome_build", func(t *testing.T, c *Client) {
			if err := c.TrackExperimentOutcome(expTestScopeKey, "score", 1.5); err != nil {
				t.Fatalf("outcome: %v", err)
			}
		}, func(capture *expWireCapture) int {
			capture.mu.Lock()
			defer capture.mu.Unlock()
			raced := 0
			for _, envelope := range capture.envelopes {
				if envelope["event_name"] == experimentOutcomeName {
					raced++
				}
			}
			return raced
		})
	})
	t.Run("exposure_build", func(t *testing.T) {
		stage(t, "exposure_build", func(t *testing.T, c *Client) {
			if err := c.TrackExperimentExposure(expTestScopeKey); err != nil {
				t.Fatalf("explicit exposure: %v", err)
			}
		}, func(capture *expWireCapture) int {
			// The automatic arm-0 exposure delivered in the setup; anything
			// beyond it is the raced explicit re-arm.
			return len(capture.exposures()) - 1
		})
	})
}

// ── G16-4: the re-mint re-checks consent at its commit point ────────────────

func TestRemintAbortsOnRacedDenial(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))                                                             // seed entry under the OLD subject
	script.push(400, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`) // grammar sentinel -> re-mint
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer func() { _ = client.Close(context.Background()) }()
	client.SetConsent(true)
	if result := fetchAssignment(t, client, expTestScopeKey); result.VariantKey != "treatment" {
		t.Fatalf("seed fetch: %+v", result)
	}
	client.exp.mu.Lock()
	oldSubject := client.exp.subjectID
	client.exp.mu.Unlock()
	if oldSubject == "" {
		t.Fatalf("test shape: the seed fetch must have minted and adopted a subject")
	}

	// The denial's FAST half flips the consent atomic inside the re-mint's
	// commit window: after the settle-time consent read admitted, after the
	// re-mint adopted (and persisted) a fresh subject, before the commit
	// re-check — the slow half's e.mu purge queues behind the settle.
	flipped := false
	client.exp.consentRaceSeam = func(stage string) {
		if stage == "remint_adopted" && !flipped {
			flipped = true
			client.consent.Store(consentStateDenied)
		}
	}
	_, err := client.FetchExperimentAssignment(context.Background(), "exp-other", nil)
	if !flipped {
		t.Fatalf("test shape: the remint_adopted seam never fired")
	}
	if !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("the raced re-mint must surface the denial, got %v", err)
	}

	client.exp.mu.Lock()
	reminted := client.exp.reminted
	subjectNow := client.exp.subjectID
	_, entryKept := client.exp.entries[expTestScopeKey]
	client.exp.mu.Unlock()
	if data, err := os.ReadFile(filepath.Join(spoolDir, expSubjectFileName)); err == nil {
		if got := strings.TrimSpace(string(data)); got != oldSubject {
			t.Fatalf("a session the plane refuses persisted a freshly minted subject id (%q): the re-mint committed durable state across the revocation, violating the zero-state guarantee the initial lazy mint's commit-point undo protects", got)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("subject file read: %v", err)
	}
	if subjectNow != oldSubject {
		t.Fatalf("the refused re-mint's rotation survived in memory (subject %q -> %q)", oldSubject, subjectNow)
	}
	if reminted {
		t.Fatalf("the refused re-mint spent the one-shot budget: a later granted session can never heal the rejected subject")
	}
	if !entryKept {
		t.Fatalf("the refused re-mint's rotation wiped the cached assignments and the abort did not restore them")
	}
}

// ── G16-5: dark-client withdrawal-marker adjudication ───────────────────────

// TestDarkClientHonorsReadableWithdrawalMarker pins the adjudicated fleet
// posture (unchanged by round 16): a READABLE withdrawal marker is a durable
// purge debt and is honored — and spent — even while experiments are dark;
// only the named ids are filtered, and the ordinary spool loads and serves.
func TestDarkClientHonorsReadableWithdrawalMarker(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	stageSpooledResendState(t, server.URL, spoolDir)
	if err := os.WriteFile(spoolWithdrawnPath(spoolDir), []byte(`["staged-fact"]`), 0o600); err != nil {
		t.Fatalf("planting the marker: %v", err)
	}

	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.ExperimentsEnabled = false
		cfg.SpoolDir = spoolDir
	})
	defer func() { _ = client.Close(context.Background()) }()
	if client.exp != nil {
		t.Fatalf("test shape: the client must be dark")
	}
	client.spool.mu.Lock()
	_, factLoaded := client.spool.ids["staged-fact"]
	_, hostLoaded := client.spool.ids["staged-host"]
	client.spool.mu.Unlock()
	if factLoaded {
		t.Fatalf("the dark client reloaded a fact the durable withdrawal marker names: purge-debt honor is flag-independent")
	}
	if !hostLoaded {
		t.Fatalf("the readable marker names exactly the withdrawn ids; the ordinary entry must load")
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	delivered := false
	capture.mu.Lock()
	for _, envelope := range capture.envelopes {
		if envelope["event_id"] == "staged-host" {
			delivered = true
		}
		if envelope["event_id"] == "staged-fact" {
			capture.mu.Unlock()
			t.Fatalf("the withdrawn fact egressed from a dark client")
		}
	}
	capture.mu.Unlock()
	if !delivered {
		t.Fatalf("the ordinary spooled event must still resend from the dark client")
	}
	if _, err := os.Stat(spoolWithdrawnPath(spoolDir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the honored marker must SPEND once the clean rewrite lands (err=%v)", err)
	}
}

// TestDarkClientDamagedMarkerFailsClosedWithinFactClass is the round-16
// remedy: a PRESENT-but-unusable marker in a DARK client fails closed within
// the experiment-fact class alone — the class is dropped terminal (the
// smallest knowable superset of the unreadable id set), the ordinary spool
// stays alive and serving, and the marker stays owed on disk for a future
// experiments-enabled session — never the whole-spool owed-wipe rung.
func TestDarkClientDamagedMarkerFailsClosedWithinFactClass(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	stageSpooledResendState(t, server.URL, spoolDir)
	if err := os.WriteFile(spoolWithdrawnPath(spoolDir), []byte(`{"not":`), 0o600); err != nil {
		t.Fatalf("planting the damaged marker: %v", err)
	}

	var lettersMu sync.Mutex
	var letters []SpoolDeadLetter
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.ExperimentsEnabled = false
		cfg.SpoolDir = spoolDir
		cfg.OnSpoolDeadLetter = func(letter SpoolDeadLetter) {
			lettersMu.Lock()
			letters = append(letters, letter)
			lettersMu.Unlock()
		}
	})
	defer func() { _ = client.Close(context.Background()) }()

	if client.spool.owedWipe() {
		t.Fatalf("a damaged EXPERIMENT withdrawal marker escalated a DARK client to the whole-spool owed-wipe rung: the ordinary spool is held hostage by an experiment-only persistence artifact")
	}
	client.spool.mu.Lock()
	_, factLoaded := client.spool.ids["staged-fact"]
	_, hostLoaded := client.spool.ids["staged-host"]
	client.spool.mu.Unlock()
	if factLoaded {
		t.Fatalf("the unknowable withdrawal must fail closed over the whole experiment-fact class: the fact reloaded for resend")
	}
	if !hostLoaded {
		t.Fatalf("the ordinary entry must survive the class-scoped fail-closed (whole-spool wipe behavior)")
	}
	factLetter := false
	lettersMu.Lock()
	for _, letter := range letters {
		for _, envelope := range letter.Envelopes {
			if strings.Contains(string(envelope), "staged-fact") {
				if letter.Reason != SpoolDropTerminal {
					lettersMu.Unlock()
					t.Fatalf("the class-dropped fact must dead-letter terminal, got %q", letter.Reason)
				}
				factLetter = true
			}
		}
	}
	lettersMu.Unlock()
	if !factLetter {
		t.Fatalf("the class-dropped fact must surface through the dead-letter callback, never vanish silently")
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	delivered := false
	capture.mu.Lock()
	for _, envelope := range capture.envelopes {
		if envelope["event_id"] == "staged-host" {
			delivered = true
		}
	}
	capture.mu.Unlock()
	if !delivered {
		t.Fatalf("the ordinary spool must stay SERVING under the damaged marker (dark client)")
	}
	if _, err := os.Stat(spoolWithdrawnPath(spoolDir)); err != nil {
		t.Fatalf("the damaged marker must stay ON DISK — owed, unspent — for a future experiments-enabled session to adjudicate (err=%v)", err)
	}
	if wipeOwedMarkerExists(spoolDir) {
		t.Fatalf("the dark remedy must not create the wipe-owed marker")
	}
}

// TestEnabledClientDamagedMarkerKeepsWipeRung pins the other half of the
// adjudication: with experiments ENABLED the damaged marker keeps its
// established escalation to the owed-wipe rung (total destruction subsumes
// the unknowable partial withdrawal).
func TestEnabledClientDamagedMarkerKeepsWipeRung(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	stageSpooledResendState(t, server.URL, spoolDir)
	if err := os.WriteFile(spoolWithdrawnPath(spoolDir), []byte(`{"not":`), 0o600); err != nil {
		t.Fatalf("planting the damaged marker: %v", err)
	}
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer func() { _ = client.Close(context.Background()) }()
	if !client.spool.owedWipe() {
		t.Fatalf("an experiments-enabled client must keep the damaged-marker wipe-rung escalation")
	}
	client.spool.mu.Lock()
	loaded := len(client.spool.entries)
	client.spool.mu.Unlock()
	if loaded != 0 {
		t.Fatalf("the owed wipe must keep the record unloaded, got %d entries", loaded)
	}
}
