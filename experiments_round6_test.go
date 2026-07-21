package shardpilot

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// ── review round 6 ──────────────────────────────────────────────────────────

// Finding 1 (P1), as re-ruled by review round 11: the purge epoch bumps
// UNDER e.mu with the sentinel's decisive state change — BEFORE the queue
// drain — so no post-sentinel fact can carry a pre-sentinel stamp. The
// worker TOCTOU that the old bump-after-drain ordering guarded against (a
// dispatch point observes the new epoch against an empty batch, advances
// its seen mark, then steals a still-queued withdrawn fact the purge's
// filter has not drained yet) is closed at batch ADMISSION instead:
// admitReceivedEvent drops withdrawn facts per event, the intake-epoch
// idiom applied to the fact stamp. The emulation below reproduces the
// worker's exact dispatch-plus-admission discipline against the real
// sentinel bump + purge pair, under -race: no withdrawn fact may ever
// survive in the held batch at a dispatch point.
func TestWithdrawnFactNeverSurvivesWorkerAdmission(t *testing.T) {
	script := &expScript{}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	// The emulation IS the worker: stop the real one first.
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	entry := &expEntry{
		VariantKey:     "treatment",
		Version:        1,
		AssignmentUnit: experimentAssignmentUnitClientID,
		SubjectFactKey: "sfk1_" + strings.Repeat("a", 64),
		SubjectKey:     "spcid_" + strings.Repeat("b", 32),
	}

	red := false
	for i := 0; i < 300 && !red; i++ {
		// The facts are built BEFORE this iteration's sentinel: their
		// stamps predate the bump, so every one of them is withdrawn.
		factEvent, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-toctou", entry, "", client.exp.sessionMarker, client.expFactPurgeEpoch.Load())
		if skip != "" {
			t.Fatalf("test setup: fact build refused (%s)", skip)
		}
		// Many queued facts stretch the drain, so the emulator can race it
		// realistically on both sides of the sentinel's bump.
		for n := 0; n < 12; n++ {
			if !client.queue.enqueue(factEvent) {
				t.Fatalf("test setup: enqueue refused")
			}
		}
		seenConsent := client.consentEpoch.Load()
		backoff := 0
		var batch []Event
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			// The settle's sentinel sequence: the decisive bump under
			// e.mu, then the off-lock pipeline purge.
			client.exp.mu.Lock()
			client.exp.factPurgeEpochBumpFn()
			client.exp.mu.Unlock()
			client.purgeWithdrawnExperimentFacts()
		}()
		// The worker's discipline: a dispatch-point filter, then per-event
		// ADMISSION — exactly the production receive path.
		for spins := 0; spins < 20000; spins++ {
			batch = client.dropBatchOnConsentEpoch(batch, &seenConsent, &backoff)
			select {
			case ev := <-client.queue.ch:
				batch = client.admitReceivedEvent(batch, ev, &seenConsent, &backoff)
			default:
			}
			runtime.Gosched()
		}
		wg.Wait()
		// The final dispatch point before any send.
		batch = client.dropBatchOnConsentEpoch(batch, &seenConsent, &backoff)
		final := client.expFactPurgeEpoch.Load()
		for _, ev := range batch {
			if isExperimentFactClassEvent(ev) && ev.expFactEpoch < final {
				red = true
			}
		}
		batch = batch[:0]
		client.queue.drainAll()
	}
	if red {
		t.Fatalf("a withdrawn fact survived in the worker's held batch past a dispatch point: the admission check must drop old-stamp facts a receive steals from the purge's drain")
	}
}

// Finding 2: a drop whose owed-exposure capture could NOT be made durable
// keeps the durable record intact — the capture+delete pair retries
// together, and the record converges only once the replay source exists.
func TestCaptureFailureKeepsCacheUntilPairLands(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(404, ``)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.SpoolDir = spoolDir
		cfg.BatchSize = 1
		cfg.BufferSize = 1
	})
	defer func() {
		capture.setStatus(http.StatusAccepted)
		_ = client.Close(context.Background())
	}()
	client.SetConsent(true)
	parkWorkerWithFullQueue(t, client, capture)
	fetchAssignment(t, client, expTestScopeKey) // owed exposure (queue full)

	// The SPOOL's record rewrite starts failing: the capture cannot land.
	client.spool.mu.Lock()
	client.spool.renameFn = func(oldpath, newpath string) error {
		if strings.HasSuffix(newpath, spoolFileName) {
			return errors.New("disk full")
		}
		return os.Rename(oldpath, newpath)
	}
	client.spool.mu.Unlock()

	result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
	if err != nil || result.Assigned || result.Code != "not_found" {
		t.Fatalf("the kill fetch must land not_found, got %+v err=%v", result, err)
	}
	// Serving stopped, but the durable record keeps the entry: the delete
	// must not outrun the replay source.
	if v := client.ExperimentVariant(expTestScopeKey); v != "" {
		t.Fatalf("serving must stop at the drop, got %q", v)
	}
	record, err := os.ReadFile(filepath.Join(spoolDir, expCacheFileName))
	if err != nil || !strings.Contains(string(record), expTestScopeKey) {
		t.Fatalf("the durable record must keep the entry while the capture is owed (err=%v)", err)
	}

	// Storage heals: the retry lands the capture, then converges the record.
	client.spool.mu.Lock()
	client.spool.renameFn = os.Rename
	client.spool.mu.Unlock()
	client.exp.retryDurableSync()

	spool, err := os.ReadFile(filepath.Join(spoolDir, spoolFileName))
	if err != nil || !strings.Contains(string(spool), experimentExposureName) {
		t.Fatalf("the healed retry must land the captured fact (err=%v)", err)
	}
	record, err = os.ReadFile(filepath.Join(spoolDir, expCacheFileName))
	if err != nil || strings.Contains(string(record), expTestScopeKey) {
		t.Fatalf("the record must converge once the pair lands (err=%v)", err)
	}
}

// Finding 3: a grammar re-mint whose subject persist fails durably condemns
// the OLD scope — a crash must not let the next launch preload and serve
// the server-rejected subject's record.
func TestFailedRemintPersistCondemnsOldScope(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(400, `{"error":"`+expSentinelSubjectGrammar+`"}`)
	script.push(200, expAssignedBody("2"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())
	fetchAssignment(t, client, expTestScopeKey)

	// The rotation's subject replace will fail: a non-empty directory sits
	// where the file must land. Record saves are seam-blocked so the retry
	// install cannot spend the tombstone with a NEW-scope save.
	subjectPath := filepath.Join(spoolDir, expSubjectFileName)
	if err := os.Remove(subjectPath); err != nil {
		t.Fatalf("test setup: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(subjectPath, "blocker"), 0o700); err != nil {
		t.Fatalf("test setup: %v", err)
	}
	client.exp.mu.Lock()
	client.exp.failDurableWritesForTests = true
	client.exp.revalidateAtMS = 1
	client.exp.mu.Unlock()

	client.experimentCycle(context.Background())

	if _, err := os.Stat(filepath.Join(spoolDir, expTombstoneFileName)); err != nil {
		t.Fatalf("the failed re-mint persist must condemn the old scope durably: %v", err)
	}

	// The "next launch": the old record is on disk, but the condemnation
	// refuses it — the rejected subject's assignments never serve again.
	client2 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client2.Close(context.Background())
	if v := client2.ExperimentVariant(expTestScopeKey); v != "" {
		t.Fatalf("a condemned scope's record must not serve at the next launch, got %q", v)
	}
}

// Finding 4: the tombstone spend is part of the durable save — and a save
// for ANY scope spends ANY tombstone, because the record it condemned was
// just replaced (this also cleans the stale old-scope tombstone a failed
// re-mint persist leaves once a new-scope save lands).
func TestTombstoneSpendsOnAnyScopeSave(t *testing.T) {
	script := &expScript{}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	e := client.exp
	e.mu.Lock()
	if !e.writeCondemnationTombstoneLocked("scope-A") {
		e.mu.Unlock()
		t.Fatalf("test setup: tombstone write failed")
	}
	saved := e.saveDurableRecordLocked(&expDurableRecord{
		Scope:   "scope-B",
		Entries: map[string]expEntry{},
	})
	e.mu.Unlock()
	if !saved {
		t.Fatalf("the save (spend included) must report landed")
	}
	if _, err := os.Stat(filepath.Join(spoolDir, expTombstoneFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a landed save must durably spend ANY tombstone, got %v", err)
	}
}

// Finding 5 (P3): a post-purge respool withholds withdrawn facts WITHOUT
// counting them — the worker-batch filter counts the same events exactly
// once at its next dispatch point.
func TestRespoolDropsCountOnce(t *testing.T) {
	script := &expScript{}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	client.SetConsent(true)
	// The batch filter is worker-owned: stop the worker and drive both
	// halves from this goroutine.
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	entry := &expEntry{
		VariantKey:     "treatment",
		Version:        1,
		AssignmentUnit: experimentAssignmentUnitClientID,
		SubjectFactKey: "sfk1_" + strings.Repeat("a", 64),
		SubjectKey:     "spcid_" + strings.Repeat("b", 32),
	}
	factEvent, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-count", entry, "", client.exp.sessionMarker, client.expFactPurgeEpoch.Load())
	if skip != "" {
		t.Fatalf("test setup: fact build refused (%s)", skip)
	}
	request, err := client.buildBatch([]Event{factEvent})
	if err != nil {
		t.Fatalf("test setup: buildBatch: %v", err)
	}
	client.expFactPurgeEpoch.Add(1) // the purge lands while "in transport"

	before := client.Snapshot().Dropped
	client.spoolFailedBatch(request, errors.New("http 500"), false)
	if got := client.Snapshot().Dropped; got != before {
		t.Fatalf("the respool filter must not count (the batch filter will), got %d -> %d", before, got)
	}
	backoff := 3
	kept := client.dropWithdrawnExperimentFacts([]Event{factEvent}, &backoff)
	if len(kept) != 0 {
		t.Fatalf("the batch filter must drop the fact, got %v", kept)
	}
	if got := client.Snapshot().Dropped; got != before+1 {
		t.Fatalf("the fact must count exactly once, got %d -> %d", before, got)
	}
}

// ── unreal round-4 parity pins ──────────────────────────────────────────────

// The withdrawn-fact matchers decide on the TOP-LEVEL event name and the
// typed assignment_key prop — never substring matching over serialized
// envelopes: user events merely MENTIONING the fact name (or carrying
// sfk1-shaped strings in other props) are untouched.
func TestWithdrawnMatchingIsTypedNotSubstring(t *testing.T) {
	hostEvent := Event{
		Name:        "host_experiment_exposure_notes",
		AnonymousID: "anon-test",
		Props: map[string]any{
			"note":     "experiment_exposure",
			"look_key": "sfk1_" + strings.Repeat("a", 64),
		},
	}
	if isExperimentFactClassEvent(hostEvent) {
		t.Fatalf("a host event mentioning the fact name must not match")
	}
	hostRaw := []byte(`{"event_id":"h1","event_name":"host_experiment_exposure_notes","props":{"note":"experiment_exposure","look_key":"sfk1_` + strings.Repeat("a", 64) + `"}}`)
	if withdrawnExperimentFactRaw(hostRaw) {
		t.Fatalf("a host envelope mentioning the fact name must not match")
	}
	factRaw := []byte(`{"event_id":"f1","event_name":"experiment_exposure","props":{"assignment_key":"sfk1_` + strings.Repeat("a", 64) + `"}}`)
	if !withdrawnExperimentFactRaw(factRaw) {
		t.Fatalf("a real fact envelope must match")
	}
}

// Spool retention decides on the envelope's event_ts, never on timestamp
// bits inside event ids — the deterministic (hash-derived) exposure ids
// carry no honest time and must not order eviction or aging.
func TestSpoolAgingUsesEventTSNotID(t *testing.T) {
	deterministicID := experimentExposureEventID("marker", "spcid_"+strings.Repeat("b", 32), "exp", 1, 0)
	old := spoolEntry{id: deterministicID, ts: time.Now().Add(-8 * 24 * time.Hour).UTC().Format(time.RFC3339Nano), raw: []byte(`{}`)}
	fresh := spoolEntry{id: deterministicID, ts: time.Now().UTC().Format(time.RFC3339Nano), raw: []byte(`{}`)}
	if !spoolEntryExpired(old, time.Now()) {
		t.Fatalf("an old event_ts must expire regardless of the id")
	}
	if spoolEntryExpired(fresh, time.Now()) {
		t.Fatalf("a fresh event_ts must survive regardless of the id")
	}
}
