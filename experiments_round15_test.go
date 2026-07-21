package shardpilot

// Review round 15 — regression pins. Each test fails on the pre-fix tree
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

// stageSpooledResendState persists a granted consent record plus one
// experiment fact and one host event in the spool, so the NEXT client on
// the directory loads both as resend work (the crash-relaunch idiom).
func stageSpooledResendState(t *testing.T, serverURL, spoolDir string) {
	t.Helper()
	seed := newExperimentClient(t, serverURL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	seed.SetConsent(true)
	if err := seed.Close(context.Background()); err != nil {
		t.Fatalf("seed close: %v", err)
	}
	s := newDiskSpool(Config{SpoolDir: spoolDir, SpoolMaxEvents: 100, SpoolMaxBytes: 1 << 20})
	now := time.Now()
	entries := []spoolEntry{
		{id: "staged-fact", ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw("staged-fact"), internalFact: true},
		{id: "staged-host", ts: now.UTC().Format(time.RFC3339Nano), raw: round5HostRaw("staged-host")},
	}
	if refused, added, _, _, persistFailed := s.append(entries, 0, false, now, func() bool { return true }); refused || len(added) != 2 || persistFailed {
		t.Fatalf("staging the spooled entries failed (refused=%v added=%d persistFailed=%v)", refused, len(added), persistFailed)
	}
}

// ── G15-1: pulled spool chunks re-check the purge state at handoff ──────────

func TestSpoolResendHandoffRechecksPulledChunk(t *testing.T) {
	run := func(t *testing.T, drive func(t *testing.T, c *Client, capture *expWireCapture)) {
		capture := &expWireCapture{}
		server := newExperimentServer(t, &expScript{}, capture)
		defer server.Close()
		spoolDir := t.TempDir()
		stageSpooledResendState(t, server.URL, spoolDir)

		var lettersMu sync.Mutex
		var letters []SpoolDeadLetter
		client := newExperimentClient(t, server.URL, func(cfg *Config) {
			cfg.SpoolDir = spoolDir
			// One-entry chunks: the pulled fact rides its own chunk, and
			// the empty-after-filter continue path is exercised too.
			cfg.BatchSize = 1
			cfg.OnSpoolDeadLetter = func(letter SpoolDeadLetter) {
				lettersMu.Lock()
				letters = append(letters, letter)
				lettersMu.Unlock()
			}
		})
		defer func() { _ = client.Close(context.Background()) }()
		if !client.spool.hasResendWork() {
			t.Fatalf("test shape: the relaunch must load resend work")
		}

		// The real-subjects sentinel lands BETWEEN the pull and the
		// transport handoff: the mirror sweeps run (the locked leg and the
		// off-lock pipeline purge — the full live sequence), but the pulled
		// chunk is local raw bytes no mirror sweep can reach. The seam
		// fires on the WORKER goroutine (the resend paths are
		// worker-owned); the sentinel machinery takes its own locks.
		var fired atomic.Bool
		client.spoolResendHandoffSeam = func([]spoolEntry) {
			if !fired.CompareAndSwap(false, true) {
				return
			}
			client.exp.mu.Lock()
			client.exp.applySentinelWithdrawalLocked("g15-1-scope", time.Now().UnixMilli())
			client.exp.mu.Unlock()
			client.purgeWithdrawnExperimentFacts()
		}
		drive(t, client, capture)
		if !fired.Load() {
			t.Fatalf("test shape: the handoff seam never fired (no chunk pulled)")
		}
		client.drainDeferredSpoolLetters()

		if got := capture.exposures(); len(got) != 0 {
			t.Fatalf("a WITHDRAWN experiment fact egressed after the real-subjects purge: the pulled resend chunk bypasses dropWithdrawnExperimentFacts and was published without a handoff re-check (%v)", got)
		}
		hostDelivered := false
		capture.mu.Lock()
		for _, envelope := range capture.envelopes {
			if envelope["event_id"] == "staged-host" {
				hostDelivered = true
			}
		}
		capture.mu.Unlock()
		if !hostDelivered {
			t.Fatalf("the surviving host member must still publish from the filtered chunk")
		}
		factLetters := 0
		lettersMu.Lock()
		for _, letter := range letters {
			for _, envelope := range letter.Envelopes {
				if strings.Contains(string(envelope), "staged-fact") {
					factLetters++
				}
			}
		}
		lettersMu.Unlock()
		if factLetters != 1 {
			t.Fatalf("the withdrawn fact must dead-letter exactly once (the sweep's accounting; the handoff filter adds nothing), got %d", factLetters)
		}
	}
	t.Run("automatic_resend", func(t *testing.T) {
		// A fresh enqueue (BatchSize 1) makes the worker publish: its
		// automatic path resends spooled chunks FIRST (resendSpooledChunks
		// runs on the worker, before the fresh batch), so the filler's
		// delivery proves the resend leg completed.
		run(t, func(t *testing.T, c *Client, capture *expWireCapture) {
			if err := c.Enqueue(Event{Name: "filler_resend"}); err != nil {
				t.Fatalf("filler enqueue: %v", err)
			}
			waitFor(t, 5*time.Second, "the worker resends the spooled chunks and delivers the filler", func() bool {
				capture.mu.Lock()
				defer capture.mu.Unlock()
				for _, envelope := range capture.envelopes {
					if envelope["event_name"] == "filler_resend" {
						return true
					}
				}
				return false
			})
		})
	})
	t.Run("explicit_flush", func(t *testing.T) {
		// The explicit Flush executes flushSpooledChunks on the worker and
		// returns after the worker's reply.
		run(t, func(t *testing.T, c *Client, capture *expWireCapture) {
			if err := c.Flush(context.Background()); err != nil {
				t.Fatalf("flush: %v", err)
			}
		})
	})
}

// ── G15-2: the sentinel's spool purge is crash-durable ──────────────────────

func TestSentinelSpoolPurgeCrashDurable(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	stageSpooledResendState(t, server.URL, spoolDir)

	// The sentinel's LOCKED durable commit runs, then the process
	// "crashes" before the off-lock pipeline purge: purgeWithdrawnExperimentFacts
	// is never called and the client is deliberately abandoned un-closed
	// (a Close would flush — a dead process does neither).
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	client.exp.mu.Lock()
	client.exp.applySentinelWithdrawalLocked("g15-2-scope", time.Now().UnixMilli())
	client.exp.mu.Unlock()

	// The next launch: initSpool loads BEFORE any experiment preload could
	// learn of the withdrawal, and loaded entries carry purge epoch zero —
	// the durable spool state alone must keep the withdrawn fact out of
	// the resend queue.
	restarted := newDiskSpool(Config{SpoolDir: spoolDir, SpoolMaxEvents: 100, SpoolMaxBytes: 1 << 20})
	restarted.load(time.Now())
	restarted.mu.Lock()
	var loadedIDs []string
	for _, entry := range restarted.entries {
		loadedIDs = append(loadedIDs, entry.id)
	}
	resendCount := len(restarted.resend)
	restarted.mu.Unlock()
	for _, id := range loadedIDs {
		if id == "staged-fact" {
			t.Fatalf("the launch after the crash reloaded the WITHDRAWN experiment fact for resend: the sentinel's durable commit (record clear/tombstone) landed without the spool withdrawal marker, so the next process resends subject-fact keys the durable state says were withdrawn (loaded %v)", loadedIDs)
		}
	}
	if len(loadedIDs) != 1 || resendCount != 1 {
		t.Fatalf("exactly the surviving host entry must reload (loaded %v, resend %d)", loadedIDs, resendCount)
	}
}

// ── G15-3: raced consent refusals map to the documented consent errors ──────

func TestRacedConsentRefusalMapsToConsentError(t *testing.T) {
	cases := []struct {
		name  string
		floor bool
		state int32
		want  error
	}{
		{"denied", false, consentStateDenied, ErrConsentDenied},
		{"unknown_under_floor", true, consentStateUnknown, ErrConsentUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			script := &expScript{}
			script.push(200, expAssignedBody("1"))
			capture := &expWireCapture{}
			server := newExperimentServer(t, script, capture)
			defer server.Close()
			client := newExperimentClient(t, server.URL, func(cfg *Config) {
				if tc.floor {
					cfg.ConsentFloor = &ConsentFloorConfig{}
				}
			})
			defer func() { _ = client.Close(context.Background()) }()
			client.SetConsent(true)
			if result := fetchAssignment(t, client, expTestScopeKey); result.VariantKey != "treatment" {
				t.Fatalf("setup fetch: %+v", result)
			}
			// The flip lands INSIDE the emission: after its own consent
			// pre-check admitted, before the fact intake's gate re-check —
			// the raced-refusal window the seam pins open.
			flipped := false
			client.exp.consentRaceSeam = func(stage string) {
				if stage == "exposure_enqueue" && !flipped {
					flipped = true
					client.consent.Store(tc.state)
				}
			}
			err := client.TrackExperimentExposure(expTestScopeKey)
			if !flipped {
				t.Fatalf("test shape: the enqueue seam never fired")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("a consent flip racing into the fact intake must surface the documented consent refusal %v, got %v", tc.want, err)
			}
			if errors.Is(err, ErrInvalidExperimentFact) {
				t.Fatalf("the raced refusal must not read as an invalid fact: %v", err)
			}
		})
	}
}

// ── G15-4: the retry-sync cycle drains deferred dead-letters ────────────────

func TestRetrySyncDrainsDeferredSpoolLetters(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	var lettersMu sync.Mutex
	var letters []SpoolDeadLetter
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.SpoolDir = spoolDir
		cfg.OnSpoolDeadLetter = func(letter SpoolDeadLetter) {
			lettersMu.Lock()
			letters = append(letters, letter)
			lettersMu.Unlock()
		}
	})
	defer func() { _ = client.Close(context.Background()) }()

	// A kill drop's owed capture+delete pair survives from an earlier
	// grant; consent is DENIED by the time the lane retries it. The
	// capture retry policy-refuses under e.mu — deferring a consent
	// dead-letter — and the cycle then takes its consent early-return on
	// the very next line: with no other activity, only the cycle itself
	// can dispatch the letter.
	entry := spoolEntry{id: "g15-4-owed", ts: time.Now().UTC().Format(time.RFC3339Nano), raw: round5FactRaw("g15-4-owed"), internalFact: true}
	client.exp.mu.Lock()
	client.exp.durablePending[scopedIntentKey("g15-4-scope", "exp-owed")] = expOwedSync{
		asOf: 1, drop: true, scope: "g15-4-scope", experimentKey: "exp-owed",
		captureFirst: true, captureEntries: []spoolEntry{entry},
	}
	client.exp.mu.Unlock()
	client.consent.Store(consentStateDenied)

	client.experimentCycle(context.Background())

	lettersMu.Lock()
	defer lettersMu.Unlock()
	found := false
	for _, letter := range letters {
		for _, envelope := range letter.Envelopes {
			if strings.Contains(string(envelope), "g15-4-owed") {
				if letter.Reason != SpoolDropConsent {
					t.Fatalf("the policy-refused capture must dead-letter as a consent drop, got %q", letter.Reason)
				}
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("the capture retry's deferred dead-letter did not fire within the retry-sync cycle: on a quiet client OnSpoolDeadLetter is delayed until an unrelated settle or Close")
	}
}

// ── G15-5: SpoolDir privacy is established before the experiment preload ────

func TestPreloadEstablishesSpoolDirPrivacy(t *testing.T) {
	plant := func(t *testing.T, serverURL, spoolDir string) {
		t.Helper()
		subject := "spcid_" + strings.Repeat("c", 32)
		cfg := Config{
			WorkspaceID:     "workspace-test",
			AppID:           "app-test",
			EnvironmentID:   "develop",
			RemoteConfigURL: serverURL,
			APIKey:          "test-exp-key",
			SpoolDir:        spoolDir,
		}
		es := newExperimentsState(cfg)
		es.mu.Lock()
		scope := es.scopeForLocked(subject)
		saved := es.saveDurableRecordLocked(&expDurableRecord{
			Scope: scope,
			Entries: map[string]expEntry{
				"exp-loose": {
					AssignmentKey:  "asgn_loose",
					VariantKey:     "treatment",
					Version:        1,
					AssignmentUnit: experimentAssignmentUnitClientID,
					SubjectFactKey: "sfk1_" + strings.Repeat("a", 64),
					SubjectKey:     subject,
					FetchedAtMS:    time.Now().UnixMilli(),
				},
			},
		})
		es.mu.Unlock()
		if !saved {
			t.Fatalf("planting the experiment record failed")
		}
		if err := os.WriteFile(filepath.Join(spoolDir, expSubjectFileName), []byte(subject+"\n"), 0o644); err != nil {
			t.Fatalf("planting the subject file: %v", err)
		}
		// The state dir arrives LOOSE — and with no persisted consent
		// grant, initSpool takes its purge path and never runs the load
		// path's tighten: the preload is the first touch.
		if err := os.Chmod(spoolDir, 0o755); err != nil {
			t.Fatalf("loosening the dir: %v", err)
		}
	}

	t.Run("refused_tighten_fails_preload_closed", func(t *testing.T) {
		capture := &expWireCapture{}
		server := newExperimentServer(t, &expScript{}, capture)
		defer server.Close()
		spoolDir := t.TempDir()
		plant(t, server.URL, spoolDir)
		client := newExperimentClient(t, server.URL, func(cfg *Config) {
			cfg.SpoolDir = spoolDir
			cfg.experimentDirChmodForTests = func(string, os.FileMode) error {
				return errors.New("tighten refused")
			}
		})
		defer func() { _ = client.Close(context.Background()) }()
		if variant := client.ExperimentVariant("exp-loose"); variant != "" {
			t.Fatalf("the preload SERVED a variant (%q) seeded from a directory whose privacy could not be established: a loose/untrusted SpoolDir must fail the preload closed exactly like the spool and consent-floor startup paths", variant)
		}
		client.exp.mu.Lock()
		entries := len(client.exp.entries)
		subjectChecked := client.exp.subjectChecked
		subjectID := client.exp.subjectID
		client.exp.mu.Unlock()
		if entries != 0 || !subjectChecked || subjectID != "" {
			t.Fatalf("the refused preload must read nothing from the untrusted dir — the lazy subject read included (entries=%d subjectChecked=%v subject=%q)", entries, subjectChecked, subjectID)
		}
		if got := client.Snapshot().LastError; got != "experiment_dir_private_failed" {
			t.Fatalf("the refusal must be diagnosed, LastError=%q", got)
		}
	})

	t.Run("loose_dir_tightens_before_load", func(t *testing.T) {
		capture := &expWireCapture{}
		server := newExperimentServer(t, &expScript{}, capture)
		defer server.Close()
		spoolDir := t.TempDir()
		plant(t, server.URL, spoolDir)
		client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
		defer func() { _ = client.Close(context.Background()) }()
		info, err := os.Stat(spoolDir)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o700 {
			t.Fatalf("the preload read persisted experiment state without establishing the directory's privacy first (dir still %o)", perm)
		}
		if variant := client.ExperimentVariant("exp-loose"); variant != "treatment" {
			t.Fatalf("with privacy established (tightened to 0700), the preload must serve the persisted assignment, got %q", variant)
		}
	})
}

// ── G15-6: explicit exposure arms survive a raced purge ─────────────────────

func TestExplicitExposureArmSurvivesRacedPurge(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer func() { _ = client.Close(context.Background()) }()
	client.SetConsent(true)
	if result := fetchAssignment(t, client, expTestScopeKey); result.Version != 1 {
		t.Fatalf("setup fetch: %+v", result)
	}
	client.exp.mu.Lock()
	subject := client.exp.currentSubjectIDLocked()
	marker := client.exp.sessionMarker
	client.exp.mu.Unlock()

	// Arm 1: a clean explicit re-arm on top of the automatic arm 0.
	if err := client.TrackExperimentExposure(expTestScopeKey); err != nil {
		t.Fatalf("explicit exposure 1: %v", err)
	}
	// Arm 2: the enqueue SUCCEEDS — the fact is in the pipeline, its arm
	// is spent — and a consent purge lands before the post-enqueue
	// high-water update (the seam models SetConsent(false) racing the
	// call at exactly that point).
	armed := false
	client.exp.consentRaceSeam = func(stage string) {
		if stage == "exposure_enqueued" && armed {
			armed = false
			client.exp.onAnalyticsPurge()
		}
	}
	armed = true
	if err := client.TrackExperimentExposure(expTestScopeKey); err != nil {
		t.Fatalf("explicit exposure 2 (raced): %v", err)
	}
	// Arm 3: the next explicit re-arm must take a FRESH arm — a distinct
	// deterministic id — never re-derive the raced arm 2's.
	if err := client.TrackExperimentExposure(expTestScopeKey); err != nil {
		t.Fatalf("explicit exposure 3: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	idArm2 := experimentExposureEventID(marker, subject, expTestScopeKey, 1, 2)
	idArm3 := experimentExposureEventID(marker, subject, expTestScopeKey, 1, 3)
	arm2Seen, arm3Seen := 0, 0
	for _, exposure := range capture.exposures() {
		switch exposure["event_id"] {
		case idArm2:
			arm2Seen++
		case idArm3:
			arm3Seen++
		}
	}
	if arm3Seen != 1 || arm2Seen != 1 {
		t.Fatalf("the explicit re-arm after the raced purge reused a spent arm: the skipped high-water update forgot an arm already handed to the pipeline, the next exposure re-derived the SAME deterministic id, and the server's de-dupe would collapse a real re-exposure (arm2 facts=%d, arm3 facts=%d)", arm2Seen, arm3Seen)
	}
}
