package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ── review round 5 ──────────────────────────────────────────────────────────

func round5FactRaw(id string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"event_id":%q,"event_ts":%q,"event_name":"experiment_exposure","props":{"assignment_key":"sfk1_%s"}}`,
		id, time.Now().UTC().Format(time.RFC3339Nano), strings.Repeat("a", 64)))
}

func round5HostRaw(id string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"event_id":%q,"event_ts":%q,"event_name":"host_event"}`,
		id, time.Now().UTC().Format(time.RFC3339Nano)))
}

// Finding 1 (P1): a terminal withdrawal whose record rewrite fails must
// still be durable — the withdrawn ids persist in the marker BEFORE the
// mirror forgets them, and the next process honors the marker at load
// instead of resending the withdrawn facts.
func TestWithdrawnSpoolRemovalSurvivesFailedSave(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{SpoolDir: dir, SpoolMaxEvents: 100, SpoolMaxBytes: 1 << 20}
	s := newDiskSpool(cfg)
	now := time.Now()
	entries := []spoolEntry{
		{id: "fact-1", ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw("fact-1")},
		{id: "host-1", ts: now.UTC().Format(time.RFC3339Nano), raw: round5HostRaw("host-1")},
	}
	refused, added, _, _, persistFailed := s.append(entries, 0, false, now, func() bool { return true })
	if refused || len(added) != 2 || persistFailed {
		t.Fatalf("test setup: append failed (refused=%v added=%d persistFailed=%v)", refused, len(added), persistFailed)
	}

	// The RECORD rewrite starts failing exactly when the withdrawal runs
	// (the small marker write still lands — the finding's scenario).
	s.renameFn = func(oldpath, newpath string) error {
		if strings.HasSuffix(newpath, spoolFileName) {
			return errors.New("disk full")
		}
		return os.Rename(oldpath, newpath)
	}
	removed, persistFailed := s.removeMatching(withdrawnExperimentFactRaw, 1)
	if len(removed) != 1 || removed[0].id != "fact-1" {
		t.Fatalf("expected the fact withdrawn, got %v", removed)
	}
	if !persistFailed {
		t.Fatalf("the failed rewrite must be reported")
	}
	if _, err := os.Stat(spoolWithdrawnPath(dir)); err != nil {
		t.Fatalf("the withdrawal marker must persist before the mirror forgets: %v", err)
	}

	// "Restart": a fresh spool over the same dir — the stale record still
	// contains fact-1, but the marker withdraws it at load; the load's own
	// rewrite then lands and spends the marker.
	s2 := newDiskSpool(cfg)
	s2.load(now)
	for _, entry := range s2.entries {
		if entry.id == "fact-1" {
			t.Fatalf("a withdrawn fact resurrected across the restart")
		}
	}
	found := false
	for _, entry := range s2.entries {
		if entry.id == "host-1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the host event must survive the withdrawal")
	}
	if _, err := os.Stat(spoolWithdrawnPath(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the marker must spend once a clean rewrite lands, got %v", err)
	}
}

// Finding 2 (P1): outcome emission serializes with the sentinel purge —
// under the emit lock an outcome either enqueues before the purge's filter
// (and is caught) or reads the already-cleared cache after it (and
// refuses). Pre-fix, TrackExperimentOutcome completed while the purge lock
// was held.
func TestOutcomeFactsSerializeWithSentinelPurge(t *testing.T) {
	script := &expScript{}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())
	fetchAssignment(t, client, expTestScopeKey)

	e := client.exp
	e.emitMu.Lock()
	done := make(chan error, 1)
	go func() { done <- client.TrackExperimentOutcome(expTestScopeKey, "score", 1) }()
	select {
	case err := <-done:
		e.emitMu.Unlock()
		t.Fatalf("the outcome must serialize with the purge lock, returned early: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	e.emitMu.Unlock()
	if err := <-done; err != nil {
		t.Fatalf("the outcome must complete once the lock releases: %v", err)
	}
}

// Finding 3 (P1): a batch in transport when the sentinel lands can fail
// retriably AFTER the purge — the respool re-filters its withdrawn facts
// instead of writing them back into the pipeline the purge just cleaned.
func TestPostPurgeRespoolFiltersWithdrawnFacts(t *testing.T) {
	script := &expScript{}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())
	client.SetConsent(true)
	fetchAssignment(t, client, expTestScopeKey)

	client.exp.mu.Lock()
	entry := client.exp.entries[expTestScopeKey]
	client.exp.mu.Unlock()
	factEvent, skip := client.buildExperimentFactEvent(experimentExposureName, expTestScopeKey, entry, "", client.exp.sessionMarker, client.expFactPurgeEpoch.Load())
	if skip != "" {
		t.Fatalf("test setup: fact build refused (%s)", skip)
	}
	request, err := client.buildBatch([]Event{factEvent, {Name: "host_in_flight", AnonymousID: "anon-test"}})
	if err != nil {
		t.Fatalf("test setup: buildBatch: %v", err)
	}

	// The sentinel purge lands while the batch is "in transport".
	client.expFactPurgeEpoch.Add(1)
	client.spoolFailedBatch(request, errors.New("http 500"), false)

	data, err := os.ReadFile(filepath.Join(spoolDir, "spool.json"))
	if err != nil {
		t.Fatalf("read spool: %v", err)
	}
	if strings.Contains(string(data), experimentExposureName) {
		t.Fatalf("a withdrawn fact must not respool after the purge")
	}
	if !strings.Contains(string(data), "host_in_flight") {
		t.Fatalf("the surviving host event must still spool")
	}
}

// Pin (pre-existing mechanics): a resend chunk requeued after the purge
// removed its entries from the mirror is skipped at requeue AND at the next
// pull — the withdrawn envelope cannot re-enter the resend queue.
func TestWithdrawnChunkRequeueIsSkipped(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{SpoolDir: dir, SpoolMaxEvents: 100, SpoolMaxBytes: 1 << 20}
	s := newDiskSpool(cfg)
	now := time.Now()
	entry := spoolEntry{id: "fact-9", ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw("fact-9")}
	if refused, added, _, _, _ := s.append([]spoolEntry{entry}, 0, false, now, func() bool { return true }); refused || len(added) != 1 {
		t.Fatalf("test setup: append failed")
	}
	if removed, _ := s.removeMatching(withdrawnExperimentFactRaw, 1); len(removed) != 1 {
		t.Fatalf("test setup: withdrawal did not remove the fact")
	}
	// The chunk was in transport during the withdrawal and now requeues.
	s.requeueResend([]spoolEntry{entry})
	chunk, _, _ := s.pullResendChunk(10, now)
	if len(chunk) != 0 {
		t.Fatalf("a withdrawn envelope must not be pulled for resend, got %v", chunk)
	}
}

// Findings 4 + 6: the worker-batch sentinel filter resets the backoff
// streak when it drains the batch to empty (the consent-drop discipline),
// and PARTIAL removal preserves the surviving members' exact retained wire
// bytes instead of clearing the whole retained request.
func TestSentinelBatchFilterBackoffAndRetainedBytes(t *testing.T) {
	script := &expScript{}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	// The filter is worker-goroutine-owned state (workerSeenExpFactPurge,
	// retainedRequest): stop the worker first so this test goroutine is its
	// sole accessor — production only ever runs it on the worker.
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
	factEvent, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-unit", entry, "", client.exp.sessionMarker, client.expFactPurgeEpoch.Load())
	if skip != "" {
		t.Fatalf("test setup: fact build refused (%s)", skip)
	}
	hostEvent := Event{Name: "host_kept", AnonymousID: "anon-test"}

	// Full drain: the streak resets.
	backoff := 7
	client.expFactPurgeEpoch.Add(1)
	kept := client.dropWithdrawnExperimentFacts([]Event{factEvent}, &backoff)
	if len(kept) != 0 {
		t.Fatalf("the withdrawn fact must filter, got %v", kept)
	}
	if backoff != 0 {
		t.Fatalf("a drained batch takes its backoff streak with it, got %d", backoff)
	}

	// Partial removal: the host member keeps its exact retained bytes and
	// the streak stays (the batch survives).
	retained, err := client.buildBatch([]Event{factEvent, hostEvent})
	if err != nil {
		t.Fatalf("test setup: buildBatch: %v", err)
	}
	hostRaw := string(retained.rawEvents[1])
	client.retainedRequest = retained
	backoff = 7
	client.expFactPurgeEpoch.Add(1)
	kept = client.dropWithdrawnExperimentFacts([]Event{factEvent, hostEvent}, &backoff)
	if len(kept) != 1 || kept[0].Name != "host_kept" {
		t.Fatalf("the host member must survive, got %v", kept)
	}
	if backoff != 7 {
		t.Fatalf("a surviving batch keeps its streak, got %d", backoff)
	}
	if len(client.retainedRequest.Events) != 1 || string(client.retainedRequest.rawEvents[0]) != hostRaw {
		t.Fatalf("the surviving member must keep its exact retained bytes")
	}
}

// Finding 5: the sentinel resets the exposure dedupe slate — a purged
// queued-undelivered automatic exposure must not suppress the re-emission
// when a later authorized fetch reinstalls the assignment in the same
// session.
func TestSentinelClearsExposureDedupe(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(403, `{"error":"`+expSentinelRealSubjectsDisabled+`"}`)
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.BatchSize = 1
		cfg.BufferSize = 4
	})
	defer client.Close(context.Background())

	// Park the worker so the automatic exposure stays queue-resident.
	capture.setStatus(http.StatusInternalServerError)
	if err := client.Enqueue(Event{Name: "filler_parked"}); err != nil {
		t.Fatalf("filler: %v", err)
	}
	waitFor(t, 5*time.Second, "the worker parks on the failing ingest", func() bool { return capture.hitCount() >= 1 })
	fetchAssignment(t, client, expTestScopeKey) // exposure fact -> queue; tuple marked

	// The sentinel lands: the queued fact purges undelivered.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil {
		t.Fatalf("the sentinel fetch must fail closed")
	}
	// A later authorized fetch reinstalls the same assignment this session.
	result := fetchAssignment(t, client, expTestScopeKey)
	if !result.Assigned {
		t.Fatalf("the authorized re-fetch must serve, got %+v", result)
	}

	capture.setStatus(http.StatusAccepted)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := capture.exposures(); len(got) == 0 {
		t.Fatalf("the reinstalled assignment's exposure must re-emit after the purge cleared the dedupe slate")
	}
}

// Finding 7: `Retry-After: 0` is the latest fence-gated server word on
// pacing — it CLEARS a previously armed deferral instead of leaving the
// lane parked to the stale deadline.
func TestZeroHintClearsStaleDeferral(t *testing.T) {
	e := &experimentsState{}
	e.retryAfterMS = 999999
	e.paceTransientLocked(1000, 0, true)
	if e.retryAfterMS != 0 {
		t.Fatalf("a present zero hint must clear the stale deferral, got %d", e.retryAfterMS)
	}
}

// Fleet contract (narrow drop-time capture): a kill/not-assigned drop that
// lands while the entry's exposure is still owed only in memory durably
// captures the owed fact into the spool BEFORE the delete — a process death
// before the next sweep replays it at the next launch.
func TestDroppedEntryOwedExposureSurvivesProcessDeath(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(404, ``)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()

	// Session 1: the assignment applies under a FULL queue (the exposure
	// stays owed in memory), then the kill lands.
	client1 := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.SpoolDir = spoolDir
		cfg.BatchSize = 1
		cfg.BufferSize = 1
	})
	client1.SetConsent(true)
	parkWorkerWithFullQueue(t, client1, capture)
	fetchAssignment(t, client1, expTestScopeKey) // owed exposure (queue full)
	result, err := client1.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
	if err != nil || result.Assigned || result.Code != "not_found" {
		t.Fatalf("the kill fetch must land not_found, got %+v err=%v", result, err)
	}
	data, err := os.ReadFile(filepath.Join(spoolDir, "spool.json"))
	if err != nil || !strings.Contains(string(data), experimentExposureName) {
		t.Fatalf("the owed exposure must be captured durably at the drop (err=%v)", err)
	}
	// Simulated process death: client1 is never closed. Its goroutines are
	// torn down at test-process exit; the cleanup close below runs only
	// AFTER the replay assertions and merely stops them.
	t.Cleanup(func() {
		capture.setStatus(http.StatusAccepted)
		_ = client1.Close(context.Background())
	})

	// Session 2: the spool replays the captured fact.
	capture.setStatus(http.StatusAccepted)
	client2 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client2.Close(context.Background())
	if err := client2.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	exposures := capture.exposures()
	if len(exposures) == 0 {
		t.Fatalf("the captured owed exposure must replay at the next launch")
	}
	props, _ := exposures[0]["props"].(map[string]any)
	key, _ := props["assignment_key"].(string)
	if !strings.HasPrefix(key, "sfk1_") {
		t.Fatalf("the replayed fact must carry the server-minted fact key, got %q", key)
	}
}

// Fleet-contract status-table refinements: an unexpected status SETTLES the
// per-key fence (the server answered — an older in-flight response must not
// overwrite the newer observation) while the auth latch stays invariant in
// BOTH directions.
func TestUnexpectedStatusSettlesFenceAndKeepsLatch(t *testing.T) {
	script := &expScript{}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())
	fetchAssignment(t, client, expTestScopeKey)

	e := client.exp
	e.mu.Lock()
	subject := e.currentSubjectIDLocked()
	scope := e.scopeForLocked(subject)
	fenceKey := scope + rcScopeSeparator + expTestScopeKey
	seqBefore := e.settled[fenceKey]
	epoch := e.authEpoch
	// A 409-class outcome at a fresh sequence: the fence must settle.
	e.fetchSeq++
	seq409 := e.fetchSeq
	e.installLocked(seq409, scope, expTestScopeKey, expOutcome{transient: true, authoritative: true}, epoch, 1000)
	settled := e.settled[fenceKey]
	latchedAfter := e.authBlocked
	e.mu.Unlock()
	if settled != seq409 || settled <= seqBefore {
		t.Fatalf("an unexpected status must settle the per-key fence, got %d (before %d)", settled, seqBefore)
	}
	if latchedAfter {
		t.Fatalf("an unexpected status must never latch")
	}

	// Latch invariance the other way: a latched plane stays latched.
	e.mu.Lock()
	e.authBlocked = true
	epoch = e.authEpoch
	e.fetchSeq++
	e.installLocked(e.fetchSeq, scope, expTestScopeKey, expOutcome{transient: true, authoritative: true}, epoch, 2000)
	stillLatched := e.authBlocked
	// And an older AUTHORITATIVE response behind the settled fence is
	// discarded: the cache stays exactly as the fence left it.
	entriesBefore := len(e.entries)
	e.installLocked(seq409-1, scope, expTestScopeKey, expOutcome{authoritative: true, dropEntry: true}, epoch, 3000)
	entriesAfter := len(e.entries)
	e.mu.Unlock()
	if !stillLatched {
		t.Fatalf("an unexpected status must never unlatch a latched plane")
	}
	if entriesAfter != entriesBefore {
		t.Fatalf("a fence-losing older response must be discarded, entries %d -> %d", entriesBefore, entriesAfter)
	}
}
