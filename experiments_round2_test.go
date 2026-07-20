package shardpilot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Round-2 review regressions plus the unity round-1 cross-SDK classes.

// Finding 1 (P3): getters normalize the experiment key before map use.
func TestGetterTrimsExperimentKey(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	if v := client.ExperimentVariant("  " + expTestScopeKey + " "); v != "treatment" {
		t.Fatalf("getters must trim the key before map use, got %q", v)
	}
	if p := client.ExperimentVariantPayload(" " + expTestScopeKey); p == nil {
		t.Fatalf("payload getter must trim the key too")
	}
}

// Finding 2: the lane re-checks that a key is still cached immediately
// before dispatching its revalidation — a concurrently dropped experiment
// is never re-fetched (and so can never be reinstalled by the lane).
func TestLaneSkipsKeysDroppedAfterSnapshot(t *testing.T) {
	script := &expScript{}
	laneGate := make(chan struct{})
	script.gates = map[int]chan struct{}{2: laneGate} // request 2 = the lane's exp-a fetch
	script.push(200, strings.Replace(expAssignedBody("1"), expTestScopeKey, "exp-a", 1))
	script.push(200, strings.Replace(expAssignedBody("1"), expTestScopeKey, "exp-b", 1))
	script.push(200, strings.Replace(expAssignedBody("2"), expTestScopeKey, "exp-a", 1)) // lane exp-a
	script.push(404, `{"error":"published experiment not found"}`)                       // host exp-b drop
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	fetchAssignment(t, client, "exp-a")
	fetchAssignment(t, client, "exp-b")
	client.exp.mu.Lock()
	client.exp.revalidateAtMS = 1
	client.exp.mu.Unlock()
	cycleDone := make(chan struct{})
	go func() {
		client.experimentCycle(context.Background())
		close(cycleDone)
	}()
	// The lane snapshot holds [exp-a, exp-b]; its exp-a fetch parks on the
	// gate. Meanwhile a host fetch DROPS exp-b (404).
	waitFor(t, 5*time.Second, "the lane's exp-a fetch reaches the server", func() bool { return script.requestCount() == 3 })
	if result, err := client.FetchExperimentAssignment(context.Background(), "exp-b", nil); err != nil || result.Code != "not_found" {
		t.Fatalf("host drop: %+v / %v", result, err)
	}
	close(laneGate)
	<-cycleDone
	// The lane must have SKIPPED exp-b: no fifth request.
	if script.requestCount() != 4 {
		t.Fatalf("a vanished key must not be revalidated (or reinstalled), got %d requests", script.requestCount())
	}
	if v := client.ExperimentVariant("exp-b"); v != "" {
		t.Fatalf("the dropped experiment must stay dropped, got %q", v)
	}
}

// Finding 3: durable intents are keyed by (scope, experiment) — a
// post-rotation failed WRITE for the same experiment key must not overwrite
// the retired scope's owed DROP.
func TestOwedDropSurvivesSameKeyWriteAfterRotation(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))                                                             // exp-checkout @ scope A
	script.push(200, `{"assigned":false,"reason":"kill_switch"}`)                                      // kill: owed drop @ A (broken storage)
	script.push(400, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`) // exp-other: rotation
	script.push(200, strings.Replace(expAssignedBody("1"), expTestScopeKey, "exp-other", 1))           // remint retry
	script.push(200, expAssignedBody("2"))                                                             // exp-checkout re-fetched @ scope B: owed WRITE @ B
	script.push(401, `{"error":"invalid runtime token"}`)                                              // ordinary latch: cancels the owed write
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey) // persisted under scope A
	breakExperimentStorage(t, client)
	if result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err != nil || result.Assigned {
		t.Fatalf("kill: %+v / %v", result, err)
	}
	// Rotation via another experiment's grammar sentinel.
	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-other", nil); err != nil {
		t.Fatalf("remint fetch: %v", err)
	}
	// Re-fetch the SAME experiment under the new scope; its durable write
	// fails and must coexist with the retired scope's owed drop.
	fetchAssignment(t, client, expTestScopeKey)
	client.exp.mu.Lock()
	dropIntents, writeIntents := 0, 0
	for _, pending := range client.exp.durablePending {
		if pending.experimentKey != expTestScopeKey {
			continue
		}
		if pending.drop {
			dropIntents++
		} else {
			writeIntents++
		}
	}
	client.exp.mu.Unlock()
	if dropIntents != 1 || writeIntents != 1 {
		t.Fatalf("the owed drop and the post-rotation owed write must coexist (scope-keyed), got drops=%d writes=%d", dropIntents, writeIntents)
	}
	// The ordinary latch cancels the owed WRITE and keeps the retired
	// scope's owed DROP; once storage recovers, the drop lands against the
	// RETIRED record.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil {
		t.Fatalf("expected the latch")
	}
	restoreExperimentStorage(t, client)
	client.experimentCycle(context.Background())
	client.exp.mu.Lock()
	remaining := len(client.exp.durablePending)
	client.exp.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("the retired-scope drop must land or settle, %d intents remain", remaining)
	}
	if record := readExperimentRecord(t, spoolDir); record != nil {
		if _, still := record.Entries[expTestScopeKey]; still {
			t.Fatalf("the killed entry must not survive on the retired record: %+v", record)
		}
	}
}

// Finding 4: the FIRST revalidation arm is jittered too.
func TestFirstRevalidationArmIsJittered(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	clock := &expFakeClock{now: time.Now()}
	client := newExperimentClient(t, server.URL, nil)
	client.clock = clock
	client.jitter = func() float64 { return 0 } // low edge of the ±10% window
	fetchAssignment(t, client, expTestScopeKey)
	defer client.Close(context.Background())

	client.exp.mu.Lock()
	armedIn := client.exp.revalidateAtMS - clock.Now().UnixMilli()
	client.exp.mu.Unlock()
	// factor = 1 + (0*2-1)*0.1 = 0.9 → 270s. The unjittered midpoint would
	// be exactly 300s — the herding this fix removes.
	if armedIn != 270000 {
		t.Fatalf("the first arm must ride the jitter seam (expected 270000ms, got %d)", armedIn)
	}
}

// Finding 5: close-time owed exposures drain in a loop until nothing is
// owed — a bounded queue admitting one fact per pass must not cost the
// rest their facts.
func TestCloseDrainsAllOwedExposures(t *testing.T) {
	script := &expScript{}
	for version := 1; version <= 3; version++ {
		script.push(200, strings.Replace(expAssignedBody("1"), `"version":1`, fmt.Sprintf(`"version":%d`, version), 1))
	}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.BatchSize = 1
		cfg.BufferSize = 1
	})

	// Three distinct applications, all owed (parked worker + full queue).
	parkWorkerWithFullQueue(t, client, capture)
	for i := 0; i < 3; i++ {
		fetchAssignment(t, client, expTestScopeKey)
	}
	client.exp.mu.Lock()
	owed := len(client.exp.pendingExposure[expTestScopeKey])
	client.exp.mu.Unlock()
	if owed != 3 {
		t.Fatalf("precondition: three owed applications, got %d", owed)
	}
	capture.setStatus(202)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if facts := capture.exposures(); len(facts) != 3 {
		t.Fatalf("every owed application must exit with its fact, got %d of 3", len(facts))
	}
}

// Unity round-1 class (1): stale serving requires the request's attribute
// set to match the cached entry's — another cohort's variant is never
// served over a transient.
func TestStaleServeRequiresMatchingAttributes(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(503, ``)
	script.push(503, ``)
	script.push(503, ``)
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, map[string]string{"geo": "US"}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	// Same attribute set: served from cache over the 503.
	result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, map[string]string{"geo": "US"})
	if err != nil || !result.FromCache || result.Code != "transient_503" {
		t.Fatalf("matching attributes must serve the cache, got %+v / %v", result, err)
	}
	// A different cohort's request gets the CLOSED transient failure.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, map[string]string{"geo": "CA"}); err == nil || !strings.Contains(err.Error(), "transient_503") {
		t.Fatalf("a mismatched attribute set must not be served another cohort's cache, got %v", err)
	}
	// An attribute-less request is its own (empty) set: closed too.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil || !strings.Contains(err.Error(), "transient_503") {
		t.Fatalf("an attribute-less request must not be served an attributed cache, got %v", err)
	}
}

// Unity round-1 class (3): a failed sentinel clear writes a durable
// condemnation tombstone, and the NEXT process refuses the withdrawn
// record and re-attempts the clear.
func TestCondemnationTombstoneSurvivesRestart(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(403, `{"error":"experiment real-subject assignment is disabled"}`)
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client1 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })

	fetchAssignment(t, client1, expTestScopeKey)
	// Storage (record writes/clears) fails; the sentinel's clear cannot
	// land — the tombstone must still be written (it is the recovery
	// mechanism for exactly this failure).
	breakExperimentStorage(t, client1)
	if _, err := client1.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil {
		t.Fatalf("expected the sentinel refusal")
	}
	if _, err := os.Stat(filepath.Join(spoolDir, expTombstoneFileName)); err != nil {
		t.Fatalf("the failed clear must write the condemnation tombstone: %v", err)
	}
	// The withdrawn record is still on disk (the clear failed) — and the
	// process dies here (no retry cycle).
	if record := readExperimentRecord(t, spoolDir); record == nil || len(record.Entries) != 1 {
		t.Fatalf("precondition: the withdrawn record survived the failed clear")
	}
	_ = client1.Close(context.Background())

	// The NEXT process must refuse the condemned record — nothing serves —
	// and its first cycle re-attempts the clear, removing record AND
	// tombstone.
	client2 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client2.Close(context.Background())
	if v := client2.ExperimentVariant(expTestScopeKey); v != "" {
		t.Fatalf("a condemned record must never serve after a restart, got %q", v)
	}
	client2.experimentCycle(context.Background())
	if _, err := os.Stat(filepath.Join(spoolDir, expCacheFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the re-attempted clear must remove the withdrawn record, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(spoolDir, expTombstoneFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a landed clear must spend the tombstone, got %v", err)
	}
}

// Unity round-1 class (dual-client mint): the initial mint publishes
// create-only and converges on a racing winner's id.
func TestInitialMintConvergesOnRacingWinner(t *testing.T) {
	script := &expScript{}
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	// Simulate the race window: this client's read found no file...
	client.exp.mu.Lock()
	client.exp.subjectChecked = true
	client.exp.mu.Unlock()
	// ...then another process publishes a valid id...
	winner := "spcid_" + strings.Repeat("f", 32)
	if err := os.WriteFile(filepath.Join(spoolDir, expSubjectFileName), []byte(winner+"\n"), 0o600); err != nil {
		t.Fatalf("plant: %v", err)
	}
	// ...and this client's mint must lose the create-only publish and
	// converge on the winner.
	fetchAssignment(t, client, expTestScopeKey)
	if got := script.request(0).URL.Query().Get("subject_key"); got != winner {
		t.Fatalf("the initial mint must converge on the racing winner, got %q want %q", got, winner)
	}
	data, err := os.ReadFile(filepath.Join(spoolDir, expSubjectFileName))
	if err != nil || strings.TrimSpace(string(data)) != winner {
		t.Fatalf("the winner's file must be untouched: %q / %v", data, err)
	}
}

// Unity round-2 class (3): retryable sweep refusals never count as Dropped
// — only terminal outcomes do.
func TestRetryableSweepRefusalsDoNotCountDropped(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	// Deny: the purge re-arms the exposure; repeated consent-closed sweep
	// attempts are retryable and must not inflate Dropped.
	client.SetConsent(false)
	droppedAfterDeny := client.Snapshot().Dropped
	for i := 0; i < 5; i++ {
		client.experimentCycle(context.Background())
	}
	if got := client.Snapshot().Dropped; got != droppedAfterDeny {
		t.Fatalf("consent-closed sweep retries must not count as Dropped: %d -> %d", droppedAfterDeny, got)
	}
	client.exp.mu.Lock()
	owed := len(client.exp.pendingExposure)
	client.exp.mu.Unlock()
	if owed == 0 {
		t.Fatalf("the owed snapshot must survive the refused retries")
	}
}

// Unity round-2 class (4): the first subject mint is serialized — two
// concurrent first fetches use exactly one id.
func TestConcurrentFirstFetchesShareOneMintedSubject(t *testing.T) {
	script := &expScript{}
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = t.TempDir() })
	defer client.Close(context.Background())

	done := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			_, _ = client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
		}()
	}
	<-done
	<-done
	if script.requestCount() != 2 {
		t.Fatalf("expected both fetches to dispatch, got %d", script.requestCount())
	}
	first := script.request(0).URL.Query().Get("subject_key")
	second := script.request(1).URL.Query().Get("subject_key")
	if first != second || !validExperimentSubjectID(first) {
		t.Fatalf("concurrent first fetches must share one minted subject: %q vs %q", first, second)
	}
}

// Round-3 finding 1: a revalidation whose entry vanished aborts INSIDE the
// fetch lock, before any dispatch — the TOCTOU gap between the lane's
// pre-dispatch check and the fetch cannot reinstall a dropped experiment.
func TestRevalidationWithoutEntryDoesNotDispatch(t *testing.T) {
	script := &expScript{}
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	_, err := client.fetchExperimentAssignment(context.Background(), expTestScopeKey, nil, true, nil)
	if err == nil || !strings.Contains(err.Error(), "revalidation_entry_vanished") {
		t.Fatalf("a revalidation for an uncached key must abort, got %v", err)
	}
	if script.requestCount() != 0 {
		t.Fatalf("nothing may be dispatched for a vanished key, got %d requests", script.requestCount())
	}
	if v := client.ExperimentVariant(expTestScopeKey); v != "" {
		t.Fatalf("nothing may be reinstalled, got %q", v)
	}
}

// Round-3 finding 2: close housekeeping tears the consumer down BEFORE the
// last durable retry, so a lane response settling in the close window is
// discarded outright instead of minting an owed intent the exiting process
// can never retry.
func TestLateLaneSettleAfterCloseHousekeepingIsDiscarded(t *testing.T) {
	script := &expScript{}
	laneGate := make(chan struct{})
	script.gates = map[int]chan struct{}{1: laneGate}
	script.push(200, expAssignedBody("1"))
	script.push(200, `{"assigned":false,"reason":"kill_switch"}`) // the gated lane response
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	// A lane revalidation is in flight (parked at the server) when close
	// housekeeping runs.
	fetchDone := make(chan struct{})
	go func() {
		_, _ = client.fetchExperimentAssignment(context.Background(), expTestScopeKey, nil, true, nil)
		close(fetchDone)
	}()
	waitFor(t, 5*time.Second, "the lane fetch reaches the server", func() bool { return script.requestCount() == 2 })
	// Housekeeping (teardown + the LAST durable retry) runs, then the
	// response lands.
	client.closeExperimentPreFlush()
	close(laneGate)
	<-fetchDone
	client.exp.mu.Lock()
	owed := len(client.exp.durablePending)
	entryStillServedInternally := client.exp.entries[expTestScopeKey] != nil
	client.exp.mu.Unlock()
	if owed != 0 {
		t.Fatalf("a post-teardown settle must not mint owed intents, got %d", owed)
	}
	if !entryStillServedInternally {
		t.Fatalf("a post-teardown settle must not install (the discarded kill re-arrives at the next launch)")
	}
	if record := readExperimentRecord(t, spoolDir); record == nil || len(record.Entries) != 1 {
		t.Fatalf("the durable record must be untouched by the discarded settle: %+v", record)
	}
}

// Round-3 finding 5: a deferral armed mid-batch (Retry-After or backoff)
// stops the rest of the batch.
func TestBatchStopsWhenDeferralArms(t *testing.T) {
	script := &expScript{}
	script.push(200, strings.Replace(expAssignedBody("1"), expTestScopeKey, "exp-a", 1))
	script.push(200, strings.Replace(expAssignedBody("1"), expTestScopeKey, "exp-b", 1))
	script.pushRetryAfter(429, ``, "120") // lane exp-a: arms the plane-wide pacing
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	clock := &expFakeClock{now: time.Now()}
	client := newExperimentClient(t, server.URL, nil)
	client.clock = clock
	defer client.Close(context.Background())

	fetchAssignment(t, client, "exp-a")
	fetchAssignment(t, client, "exp-b")
	client.exp.mu.Lock()
	client.exp.revalidateAtMS = 1
	client.exp.mu.Unlock()
	client.experimentCycle(context.Background())
	if script.requestCount() != 3 {
		t.Fatalf("the armed deferral must stop the batch after exp-a's 429, got %d requests", script.requestCount())
	}
}

// Round-3 finding 4: owed exposure snapshots still stranded at teardown are
// COUNTED (Stats.Dropped + a distinct diagnostic), never silently lost.
func TestCloseCountsDiscardedOwedExposures(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, strings.Replace(expAssignedBody("1"), `"version":1`, `"version":2`, 1))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.BatchSize = 1
		cfg.BufferSize = 1
	})

	// Two owed applications against a jammed pipeline that NEVER recovers.
	parkWorkerWithFullQueue(t, client, capture)
	fetchAssignment(t, client, expTestScopeKey)
	fetchAssignment(t, client, expTestScopeKey)
	client.exp.mu.Lock()
	owed := len(client.exp.pendingExposure[expTestScopeKey])
	client.exp.mu.Unlock()
	if owed != 2 {
		t.Fatalf("precondition: two owed applications, got %d", owed)
	}
	before := client.Snapshot().Dropped
	closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = client.Close(closeCtx) // the ingest stays 500: the drain cannot progress
	snapshot := client.Snapshot()
	if snapshot.Dropped < before+2 {
		t.Fatalf("stranded owed exposures must count in Stats.Dropped: %d -> %d", before, snapshot.Dropped)
	}
	if snapshot.LastError != "experiment_exposures_discarded_at_close" {
		t.Fatalf("the discard must carry its distinct diagnostic, got %q", snapshot.LastError)
	}
}

// Round-3 finding 6: the assignment plane parses Retry-After with the batch
// transport's parser — HTTP-dates included (its documented contract),
// unlike the remote-config route's digits-only parse.
func TestAssignmentRetryAfterHTTPDateParses(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.pushRetryAfter(429, ``, time.Now().Add(90*time.Second).UTC().Format(http.TimeFormat))
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	clock := &expFakeClock{now: time.Now()}
	client := newExperimentClient(t, server.URL, nil)
	client.clock = clock
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	result := fetchAssignment(t, client, expTestScopeKey) // the dated 429
	if !result.FromCache || result.Code != "transient_429" {
		t.Fatalf("expected the paced cache serve, got %+v", result)
	}
	// Inside the dated window the cadence is parked...
	clock.advance(30 * time.Second)
	client.exp.mu.Lock()
	client.exp.revalidateAtMS = clock.Now().UnixMilli() - 1
	parkedUntil := client.exp.retryAfterMS
	client.exp.mu.Unlock()
	if parkedUntil == 0 {
		t.Fatalf("an HTTP-date Retry-After must arm the pacing (batch-parser parity)")
	}
	requestsBefore := script.requestCount()
	client.experimentCycle(context.Background())
	if script.requestCount() != requestsBefore {
		t.Fatalf("the dated window must park the cadence")
	}
	// ...and past it the cadence resumes.
	clock.advance(180 * time.Second)
	client.exp.mu.Lock()
	client.exp.revalidateAtMS = clock.Now().UnixMilli() - 1
	client.exp.mu.Unlock()
	client.experimentCycle(context.Background())
	if script.requestCount() != requestsBefore+1 {
		t.Fatalf("an expired dated window must release the cadence")
	}
}
