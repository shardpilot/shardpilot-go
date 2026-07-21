package shardpilot

// Review round 10 — regression pins. Each test fails on the pre-fix tree
// for its finding's exact reason (verified mechanically via targeted
// temporary reverts of the fix, with the test seams retained).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ── finding 1 (client.go): the bulk drain re-settles the epoch per event ────

func TestFlushDrainResettlesConsentEpochMidDrain(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.ExperimentsEnabled = false })
	client.SetConsent(true)
	// The drain path is worker-owned: stop the worker first (the
	// close-remnant idiom), then drive the drain directly.
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A worker-held pre-denial batch (settled at epoch 0), one more
	// pre-denial event already queued, and — landing MID-drain, after the
	// drain's one-shot boundary settle — a denial → re-grant whose fresh
	// post-grant event joins the queue behind it.
	held := []Event{{ID: "stale-held", Name: "stale_pre_denial", AnonymousID: "anon-test", intakeConsentEpoch: 0}}
	client.queue.enqueue(Event{ID: "stale-queued", Name: "stale_queued_pre_denial", AnonymousID: "anon-test", intakeConsentEpoch: 0})
	client.drainReceiveSeam = func(event Event) {
		if event.ID != "stale-queued" {
			return
		}
		// The denial's fast half bumps the epoch; the re-grant's fresh
		// event carries the moved intake stamp.
		client.consentEpoch.Add(1)
		client.queue.enqueue(Event{ID: "survivor", Name: "fresh_post_grant", AnonymousID: "anon-test", intakeConsentEpoch: 1})
	}
	seenEpoch := uint64(0)
	backoff := 0
	batch := client.drainQueueAdmitted(held, &seenEpoch, &backoff)
	// The next dispatch settles the boundary — the moment a mixed batch
	// would be discarded whole.
	batch = client.dropBatchOnConsentEpoch(batch, &seenEpoch, &backoff)

	var names []string
	for _, event := range batch {
		names = append(names, event.Name)
	}
	joined := strings.Join(names, ",")
	if !strings.Contains(joined, "fresh_post_grant") {
		t.Fatalf("the post-grant event was lost with the pre-denial batch — the bulk drain must re-settle the consent epoch before a newer-stamped event joins an older-epoch batch (batch: %q)", joined)
	}
	if strings.Contains(joined, "stale") {
		t.Fatalf("pre-denial events must not survive into the granted period (batch: %q)", joined)
	}
}

// ── finding 2 (experiments.go): the mint commit re-checks consent ───────────

func TestRacingDenialAbortsSubjectMintCommit(t *testing.T) {
	for _, flavor := range []struct {
		name  string
		state int32
	}{
		{"denied", consentStateDenied},
		{"denied_forced_minor", consentStateDeniedForcedMinor},
	} {
		t.Run(flavor.name, func(t *testing.T) {
			script := &expScript{}
			script.push(200, expAssignedBody("1"))
			capture := &expWireCapture{}
			server := newExperimentServer(t, script, capture)
			defer server.Close()
			spoolDir := t.TempDir()
			client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
			defer client.Close(context.Background())
			client.SetConsent(true)

			// The denial's fast-half FLIP lands between the pre-mint check
			// and the adopt/persist — the seam SetConsentDecision opens by
			// storing the atomic outside e.mu.
			client.exp.mu.Lock()
			client.exp.consentRaceSeam = func(stage string) {
				if stage == "mint_adopted" {
					client.consent.Store(flavor.state)
				}
			}
			client.exp.mu.Unlock()

			if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrConsentDenied) {
				t.Fatalf("expected the refusal from the mint commit re-check, got %v", err)
			}
			client.exp.mu.Lock()
			subject, memory := client.exp.subjectID, client.exp.memorySubjectID
			client.exp.consentRaceSeam = nil
			client.exp.mu.Unlock()
			if subject != "" || memory != "" {
				t.Fatalf("a mint whose commit found consent revoked must not adopt — a refused session mints no subject state, got %q / %q", subject, memory)
			}
			if _, err := os.Stat(filepath.Join(spoolDir, expSubjectFileName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the racing denial's mint persisted a spcid_ subject file for a refused session (stat err=%v)", err)
			}

			// Heals: a real re-grant mints fresh and the plane serves.
			client.SetConsent(true)
			result := fetchAssignment(t, client, expTestScopeKey)
			if !result.Assigned {
				t.Fatalf("the re-granted fetch must assign, got %+v", result)
			}
			if _, err := os.Stat(filepath.Join(spoolDir, expSubjectFileName)); err != nil {
				t.Fatalf("the granted mint must persist: %v", err)
			}
		})
	}
}

// ── finding 5 (experiments.go): not-assigned versions share positivity ──────

func TestNonPositiveNotAssignedVersionIsMalformed(t *testing.T) {
	scope := expTestRequestScope()
	for _, body := range []string{
		`{"assigned":false,"reason":"kill_switch","version":-1}`,
		`{"assigned":false,"reason":"kill_switch","version":0}`,
		`{"assigned":false,"version":-7}`,
	} {
		if _, _, ok := parseExperimentVerdict(expTestResponse(200, body), scope, 42); ok {
			t.Fatalf("body %q must classify malformed: a PRESENT non-positive version is not a verdict this contract sends, and an authoritative reading would drop the cached assignment", body)
		}
	}
	// The absent-version traffic-gate shape stays tolerated.
	if _, _, ok := parseExperimentVerdict(expTestResponse(200, `{"assigned":false,"reason":"kill_switch"}`), scope, 42); !ok {
		t.Fatalf("an absent version must stay tolerated on the not-assigned shape")
	}

	// End to end: the malformed 200 takes the transient path — the cache
	// retained and served stale — never the authoritative drop.
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, `{"assigned":false,"reason":"kill_switch","version":-1}`)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())
	client.SetConsent(true)
	if result := fetchAssignment(t, client, expTestScopeKey); !result.Assigned {
		t.Fatalf("setup fetch: %+v", result)
	}
	result := fetchAssignment(t, client, expTestScopeKey)
	if !result.FromCache || result.VariantKey != "treatment" {
		t.Fatalf("the malformed not-assigned 200 must serve the retained cache (transient path), got %+v", result)
	}
	client.exp.mu.Lock()
	entry := client.exp.entries[expTestScopeKey]
	client.exp.mu.Unlock()
	if entry == nil || entry.Version != 1 {
		t.Fatalf("the cached assignment must survive the malformed verdict, got %+v", entry)
	}
}

// ── finding 6 (experiments.go): the install commit re-checks consent ────────

func TestRacingDenialDiscardsInstallKeepsRetainedCache(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, expAssignedBody("2"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())
	client.SetConsent(true)
	if result := fetchAssignment(t, client, expTestScopeKey); result.Version != 1 {
		t.Fatalf("setup fetch: %+v", result)
	}

	// The denial completes between the settle's pre-lock consent gate and
	// the install commit.
	client.exp.mu.Lock()
	client.exp.consentRaceSeam = func(stage string) {
		if stage == "settle_locked" {
			client.consent.Store(consentStateDenied)
		}
	}
	client.exp.mu.Unlock()
	_, fetchErr := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
	client.exp.mu.Lock()
	client.exp.consentRaceSeam = nil
	entry := client.exp.entries[expTestScopeKey]
	armedV2 := false
	for _, owed := range client.exp.pendingExposure[expTestScopeKey] {
		if owed.entry.Version == 2 {
			armedV2 = true
		}
	}
	client.exp.mu.Unlock()
	if entry == nil || entry.Version != 1 {
		t.Fatalf("nothing installs after consent closes: the racing 200 must not replace the retained entry, got %+v", entry)
	}
	if armedV2 {
		t.Fatalf("the discarded install must not arm exposure debt while the plane is refused")
	}
	if !errors.Is(fetchErr, ErrConsentDenied) {
		t.Fatalf("the caller receives the refusal, never an assignment fetched across the revoked interval, got %v", fetchErr)
	}
	record, err := os.ReadFile(filepath.Join(spoolDir, expCacheFileName))
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	if strings.Contains(string(record), `"version":2`) {
		t.Fatalf("the discarded install must not reach the durable record, got %s", record)
	}
	// A re-grant re-serves the RETAINED assignment, not the discarded one.
	client.SetConsent(true)
	if variant := client.ExperimentVariant(expTestScopeKey); variant != "treatment" {
		t.Fatalf("the re-granted getter must serve the retained v1 assignment, got %q", variant)
	}
}

// ── finding 6, the partition's other half (fleet contract, defold R22):
// destructive verdicts from a request dispatched under grant still apply ────

func TestRacingDenialStillAppliesServerWithdrawal(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, `{"assigned":false,"reason":"kill_switch","version":2}`)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())
	client.SetConsent(true)
	if result := fetchAssignment(t, client, expTestScopeKey); result.Version != 1 {
		t.Fatalf("setup fetch: %+v", result)
	}

	client.exp.mu.Lock()
	client.exp.consentRaceSeam = func(stage string) {
		if stage == "settle_locked" {
			client.consent.Store(consentStateDenied)
		}
	}
	client.exp.mu.Unlock()
	_, fetchErr := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
	client.exp.mu.Lock()
	client.exp.consentRaceSeam = nil
	entry := client.exp.entries[expTestScopeKey]
	client.exp.mu.Unlock()
	if entry != nil {
		t.Fatalf("the fleet partition applies DESTRUCTIVE verdicts through a racing denial: the cache is retained across denial by design, so a discarded kill would re-serve the killed assignment at the next re-grant, got %+v", entry)
	}
	if !errors.Is(fetchErr, ErrConsentDenied) {
		t.Fatalf("the caller receives the refusal, never a verdict settled across the closed window, got %v", fetchErr)
	}
	record, err := os.ReadFile(filepath.Join(spoolDir, expCacheFileName))
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	if strings.Contains(string(record), "asgn_abc") {
		t.Fatalf("the server-directed withdrawal must land on the DURABLE record too, got %s", record)
	}
	// A re-grant must NOT re-serve the killed assignment.
	client.SetConsent(true)
	if variant := client.ExperimentVariant(expTestScopeKey); variant != "" {
		t.Fatalf("the killed assignment must not re-serve after the re-grant, got %q", variant)
	}
}

// ── fleet partition at the auth-epoch gate (the defold D5 shape) ────────────

func TestStaleEpochServerWithdrawalStillLandsDurably(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())
	client.SetConsent(true)
	if result := fetchAssignment(t, client, expTestScopeKey); result.Version != 1 {
		t.Fatalf("setup fetch: %+v", result)
	}

	// A sibling request's 401 latches: memory clears, the auth epoch
	// moves, the DURABLE record is retained. The kill verdict already in
	// flight then settles under the OLD epoch.
	e := client.exp
	e.mu.Lock()
	staleEpoch := e.authEpoch
	e.authBlocked = true
	e.authEpoch++
	e.entries = make(map[string]*expEntry)
	e.fetchSeq++
	seq := e.fetchSeq
	subject := e.currentSubjectIDLocked()
	scope := e.scopeForLocked(subject)
	_, persistFailed, _ := e.installLocked(seq, scope, expTestScopeKey, expOutcome{authoritative: true, dropEntry: true}, staleEpoch, client.clock.Now().UnixMilli())
	stillLatched := e.authBlocked
	e.mu.Unlock()
	if persistFailed {
		t.Fatalf("the stale-epoch drop must persist cleanly")
	}
	if !stillLatched {
		t.Fatalf("a stale-epoch verdict must never unlatch — only a fetch started after the latch may")
	}
	record, err := os.ReadFile(filepath.Join(spoolDir, expCacheFileName))
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	if strings.Contains(string(record), "asgn_abc") {
		t.Fatalf("the fleet partition applies a stale-epoch server withdrawal to the DURABLE record — retained, a later unlatch or re-init would re-serve the killed assignment (record: %s)", record)
	}
}

// ── finding 3 (spool.go): the marker spend requires the durable unlink ──────

func TestWithdrawnMarkerSpendRequiresDurableUnlink(t *testing.T) {
	t.Run("spend_sync_failure_holds_the_debt", func(t *testing.T) {
		dir := t.TempDir()
		s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: 100, SpoolMaxBytes: 1 << 20})
		now := time.Now()
		entry := spoolEntry{id: "fact-durable-unlink", ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw("fact-durable-unlink")}
		if refused, added, _, _, _ := s.append([]spoolEntry{entry}, 0, false, now, func() bool { return true }); refused || len(added) != 1 {
			t.Fatalf("test setup: append failed")
		}
		s.mu.Lock()
		lastRemoved := ""
		s.removeFn = func(path string) error { lastRemoved = filepath.Base(path); return os.Remove(path) }
		s.syncFn = func(string) error {
			if lastRemoved == spoolWithdrawnFileName {
				return errors.New("dir sync refused")
			}
			return syncDir(dir)
		}
		s.mu.Unlock()
		removed, persistFailed := s.removeMatching(withdrawnExperimentFactRaw, 1)
		if len(removed) != 1 {
			t.Fatalf("test setup: withdrawal removed %d", len(removed))
		}
		if !persistFailed {
			t.Fatalf("an unlink whose directory sync failed is NOT a completed spend: POSIX can resurrect the marker in a crash, and a stale marker honored after the owed state cleared condemns same-id FRESH facts spooled after re-enable")
		}
		s.mu.Lock()
		owed, ids := s.withdrawnMarkerOwed, len(s.withdrawnOwedIDs)
		s.mu.Unlock()
		if !owed || ids == 0 {
			t.Fatalf("the withdrawal debt must hold until the unlink is durably synced (owed=%v ids=%d)", owed, ids)
		}
		// Storage heals: the retried save syncs (the missing-file path must
		// sync too — the earlier attempt already unlinked) and the spend
		// completes.
		s.mu.Lock()
		s.syncFn = syncDir
		s.mu.Unlock()
		if attempted, failed := s.retryPersist(); !attempted || failed {
			t.Fatalf("the healed retry must complete, attempted=%v failed=%v", attempted, failed)
		}
		s.mu.Lock()
		owed = s.withdrawnMarkerOwed
		s.mu.Unlock()
		if owed {
			t.Fatalf("the healed spend must clear the debt")
		}
	})
	t.Run("durable_order", func(t *testing.T) {
		dir := t.TempDir()
		s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: 100, SpoolMaxBytes: 1 << 20})
		now := time.Now()
		entry := spoolEntry{id: "fact-order", ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw("fact-order")}
		if refused, added, _, _, _ := s.append([]spoolEntry{entry}, 0, false, now, func() bool { return true }); refused || len(added) != 1 {
			t.Fatalf("test setup: append failed")
		}
		s.mu.Lock()
		var ops []string
		s.removeFn = func(path string) error { ops = append(ops, "rm:"+filepath.Base(path)); return os.Remove(path) }
		s.syncFn = func(string) error { ops = append(ops, "sync"); return syncDir(dir) }
		s.mu.Unlock()
		if removed, persistFailed := s.removeMatching(withdrawnExperimentFactRaw, 1); len(removed) != 1 || persistFailed {
			t.Fatalf("withdrawal: removed=%d persistFailed=%v", len(removed), persistFailed)
		}
		got := strings.Join(ops, ",")
		want := "sync,rm:" + spoolWithdrawnFileName + ",sync"
		if got != want {
			t.Fatalf("the withdrawal marker must follow the recovery-marker durability idiom — directory sync at the write AND after the spend's unlink:\n got %s\nwant %s", got, want)
		}
	})
}

// ── finding 4 (spool.go): the marker read bound is capacity-derived ─────────

func TestWithdrawnMarkerReadBoundScalesWithConfig(t *testing.T) {
	t.Run("large_configured_marker_honored", func(t *testing.T) {
		dir := t.TempDir()
		cfg := Config{SpoolDir: dir, SpoolMaxEvents: 20000, SpoolMaxBytes: 64 << 20}
		s := newDiskSpool(cfg)
		now := time.Now()
		entries := []spoolEntry{
			{id: "fact-large-0", ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw("fact-large-0")},
			{id: "fact-large-1", ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw("fact-large-1")},
		}
		if refused, added, _, _, _ := s.append(entries, 0, false, now, func() bool { return true }); refused || len(added) != 2 {
			t.Fatalf("test setup: append failed")
		}
		// A crash-interrupted purge left a marker with >6k UUID-sized ids —
		// past the legacy 256 KiB constant, legitimate for this config.
		ids := []string{"fact-large-0", "fact-large-1"}
		for i := 0; i < 7000; i++ {
			ids = append(ids, fmt.Sprintf("00000000-0000-7000-8000-%012d", i))
		}
		payload, err := json.Marshal(ids)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if int64(len(payload)) <= int64(spoolWithdrawnReadLimit) {
			t.Fatalf("test shape: the marker must exceed the legacy bound, got %d bytes", len(payload))
		}
		if err := os.WriteFile(spoolWithdrawnPath(dir), payload, 0o600); err != nil {
			t.Fatalf("write marker: %v", err)
		}
		restarted := newDiskSpool(cfg)
		restarted.load(time.Now())
		restarted.mu.Lock()
		loaded := len(restarted.entries)
		restarted.mu.Unlock()
		if loaded != 0 {
			t.Fatalf("a legitimately large marker read as ABSENT: %d withdrawn fact(s) reloaded for resend — the read bound must cover the maximum marker this configuration can write", loaded)
		}
		// The honored marker backed load's clean rewrite and spent with it.
		if _, err := os.Stat(spoolWithdrawnPath(dir)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the honored marker must spend with load's clean rewrite, stat err=%v", err)
		}
		record, err := os.ReadFile(filepath.Join(dir, "spool.json"))
		if err != nil {
			t.Fatalf("read spool: %v", err)
		}
		if strings.Contains(string(record), "fact-large-") {
			t.Fatalf("the rewritten record must exclude the withdrawn facts, got %s", record)
		}
	})
	t.Run("damaged_marker_fails_closed", func(t *testing.T) {
		dir := t.TempDir()
		// The wipe-rung escalation asserted below is the EXPERIMENTS-ENABLED
		// posture (round 16 scoped the damaged-marker remedy by the opt-in:
		// a dark client fails closed within the experiment-fact class
		// instead — see TestDarkClientDamagedMarkerFailsClosedWithinFactClass).
		cfg := Config{SpoolDir: dir, SpoolMaxEvents: 100, SpoolMaxBytes: 1 << 20, ExperimentsEnabled: true}
		s := newDiskSpool(cfg)
		now := time.Now()
		entry := spoolEntry{id: "fact-damaged", ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw("fact-damaged")}
		if refused, added, _, _, _ := s.append([]spoolEntry{entry}, 0, false, now, func() bool { return true }); refused || len(added) != 1 {
			t.Fatalf("test setup: append failed")
		}
		if err := os.WriteFile(spoolWithdrawnPath(dir), []byte("{corrupt"), 0o600); err != nil {
			t.Fatalf("write marker: %v", err)
		}
		restarted := newDiskSpool(cfg)
		outcome := restarted.load(time.Now())
		restarted.mu.Lock()
		owedWipe := restarted.owed
		loaded := len(restarted.entries)
		restarted.mu.Unlock()
		if loaded != 0 || len(outcome.expired) != 0 {
			t.Fatalf("a PRESENT-but-corrupt marker collapsed to absent and the record loaded for resend (%d entries) — the withdrawal's id set is unknowable and must fail CLOSED", loaded)
		}
		if !owedWipe {
			t.Fatalf("the damaged marker must escalate to the owed-wipe rung (unknowable partial withdrawal subsumed by total destruction)")
		}
		// The wipe settles durably and the spool reopens clean: the stale
		// marker cannot condemn facts spooled after re-enable.
		if !restarted.settleOwedWipe() {
			t.Fatalf("the wipe must settle")
		}
		if _, err := os.Stat(spoolWithdrawnPath(dir)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the settle must durably remove the damaged marker, stat err=%v", err)
		}
		fresh := spoolEntry{id: "fact-fresh", ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw("fact-fresh")}
		if refused, added, _, _, _ := restarted.append([]spoolEntry{fresh}, 0, false, now, func() bool { return true }); refused || len(added) != 1 {
			t.Fatalf("the reopened spool must admit fresh facts (refused=%v added=%d)", refused, len(added))
		}
	})
}

// ── finding 7 (spool.go): a failed marker write keeps the full set ──────────

func TestFailedMarkerWriteStillBacksMergeWithFullSet(t *testing.T) {
	dir := t.TempDir()
	total := spoolSettledMemory + 4
	s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: total + 10, SpoolMaxBytes: 32 << 20})
	now := time.Now()
	entries := make([]spoolEntry, 0, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("fact-%05d", i)
		entries = append(entries, spoolEntry{id: id, ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw(id)})
	}
	if refused, added, _, _, _ := s.append(entries, 0, false, now, func() bool { return true }); refused || len(added) != total {
		t.Fatalf("test setup: append failed (added %d)", len(added))
	}
	// The marker WRITE fails; the record rewrite stays healthy.
	s.mu.Lock()
	s.renameFn = func(oldpath, newpath string) error {
		if strings.HasSuffix(newpath, spoolWithdrawnFileName) {
			return errors.New("marker write refused")
		}
		return os.Rename(oldpath, newpath)
	}
	s.mu.Unlock()
	removed, persistFailed := s.removeMatching(withdrawnExperimentFactRaw, 1)
	if len(removed) != total {
		t.Fatalf("withdrawal removed %d", len(removed))
	}
	if persistFailed {
		t.Fatalf("a clean record rewrite backed by the in-memory full set IS a clean save, got persistFailed")
	}
	data, err := os.ReadFile(filepath.Join(dir, "spool.json"))
	if err != nil {
		t.Fatalf("read spool: %v", err)
	}
	if strings.Contains(string(data), experimentExposureName) {
		t.Fatalf("with the marker write failed, the merge fell back to the bounded settled cache and resurrected the oldest withdrawn ids into a 'clean' record — the in-memory FULL id set must back the save while the debt is owed")
	}
	s.mu.Lock()
	owed := s.withdrawnMarkerOwed
	s.mu.Unlock()
	if owed {
		t.Fatalf("the debt must spend with the clean rewrite (no marker file exists to remove)")
	}
}
