package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Round-1 review regressions plus the defold R9/R10 cross-check pins.

// Finding 1 + R10: the fact lane's privacy boundary — a non-sfk1_ subject
// fact key (a raw spcid_ echo included) must never ride assignment_key.
func TestSubjectFactKeyGrammarGuardsTheFactLane(t *testing.T) {
	rawSubject := "spcid_" + strings.Repeat("a", 32)

	// Parse-time: a 200 whose subject_fact_key is present but non-sfk1_ is
	// malformed, never installed.
	badEcho := strings.Replace(expAssignedBody("1"),
		`"subject_fact_key":"sfk1_`+strings.Repeat("a", 64)+`"`,
		`"subject_fact_key":"`+rawSubject+`"`, 1)
	if _, _, ok := parseExperimentVerdict(expTestResponse(200, badEcho), expTestRequestScope(), 42); ok {
		t.Fatalf("a raw-subject echo in subject_fact_key must classify malformed")
	}

	// Cache-restore degrade: a stored record with a malformed fact key
	// still serves the assignment but produces NO fact.
	script := &expScript{}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client1 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	fetchAssignment(t, client1, expTestScopeKey)
	if err := client1.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Corrupt the stored fact key into the raw subject id.
	recordPath := filepath.Join(spoolDir, expCacheFileName)
	data, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	corrupted := strings.Replace(string(data), "sfk1_"+strings.Repeat("a", 64), rawSubject, 1)
	if corrupted == string(data) {
		t.Fatalf("test setup: fact key not found in the record")
	}
	if err := os.WriteFile(recordPath, []byte(corrupted), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	client2 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client2.Close(context.Background())
	if v := client2.ExperimentVariant(expTestScopeKey); v != "treatment" {
		t.Fatalf("the assignment still serves degraded, got %q", v)
	}
	client2.experimentCycle(context.Background()) // sweeps the restored owed exposure
	if err := client2.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for _, fact := range capture.exposures() {
		key := fact["props"].(map[string]any)["assignment_key"]
		if s, _ := key.(string); strings.HasPrefix(s, "spcid_") {
			t.Fatalf("the raw subject id egressed as assignment_key: %v", key)
		}
	}
	if err := client2.TrackExperimentExposure(expTestScopeKey); !errors.Is(err, ErrExperimentFactUnavailable) {
		t.Fatalf("a malformed stored fact key must refuse facts, got %v", err)
	}
}

// Finding 11: unknown assignment units are not installable verdicts.
func TestUnknownAssignmentUnitIsMalformed(t *testing.T) {
	body := strings.Replace(expAssignedBody("1"), `"assignment_unit":"client_id"`, `"assignment_unit":"device_cluster"`, 1)
	if _, _, ok := parseExperimentVerdict(expTestResponse(200, body), expTestRequestScope(), 42); ok {
		t.Fatalf("an unknown assignment_unit must classify malformed")
	}
	// And a stored record with one is a miss.
	entries := sanitizeExperimentEntries(map[string]expEntry{
		"exp": {AssignmentKey: "a", VariantKey: "v", Version: 1, AssignmentUnit: "device_cluster", FetchedAtMS: 5},
	})
	if len(entries) != 0 {
		t.Fatalf("a stored unknown unit must sanitize to a miss, got %v", entries)
	}
}

// R9/R10: presence vs type split — an explicit-null (or non-string) reason
// or echo member is malformed, never coerced to the absent shape.
func TestNullReasonAndEchoesAreMalformed(t *testing.T) {
	scope := expTestRequestScope()
	malformed := []string{
		`{"assigned":false,"reason":null}`,
		`{"assigned":false,"reason":7}`,
		`{"assigned":false,"reason":{}}`,
		`{"assigned":false,"experiment_key":null}`,
		`{"assigned":false,"app_key":null}`,
		`{"assigned":false,"environment_key":42}`,
	}
	for _, body := range malformed {
		if _, _, ok := parseExperimentVerdict(expTestResponse(200, body), scope, 42); ok {
			t.Fatalf("body %q must classify malformed (presence/type split)", body)
		}
	}
	// Absent members stay tolerated.
	if _, _, ok := parseExperimentVerdict(expTestResponse(200, `{"assigned":false}`), scope, 42); !ok {
		t.Fatalf("absent members must stay tolerated")
	}
}

// Finding 2: the grammar-remint retry is bounded by the CALLER's context.
func TestRemintRetryHonorsCallerContext(t *testing.T) {
	script := &expScript{}
	retryGate := make(chan struct{})
	script.gates = map[int]chan struct{}{1: retryGate} // request 1 = the remint retry
	script.push(400, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`)
	script.push(200, expAssignedBody("1"))
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.HTTPTimeout = 30 * time.Second // the SDK-internal bound must NOT be what ends the call
	})
	defer client.Close(context.Background())
	defer close(retryGate)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := client.FetchExperimentAssignment(ctx, expTestScopeKey, nil)
	elapsed := time.Since(start)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the caller's deadline to end the retry, got %v", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("the caller context must bound the WHOLE call incl. the retry, took %v", elapsed)
	}
}

// Finding 3: a hung automatic revalidation fetch must not block Close.
func TestHungRevalidationFetchDoesNotBlockClose(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	hang := make(chan struct{})
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.HTTPTimeout = 30 * time.Second
	})
	fetchAssignment(t, client, expTestScopeKey)
	// Arm the gate AFTER the host fetch so only the lane's fetch hangs.
	script.mu.Lock()
	script.gate = hang
	script.mu.Unlock()
	// Drive one lane cycle in a goroutine with the lane's own stop-bound
	// context — exactly what runExperimentsLane does.
	laneCtx, cancelLane := context.WithCancel(context.Background())
	go func() {
		<-client.stop
		cancelLane()
	}()
	client.exp.mu.Lock()
	client.exp.revalidateAtMS = 1 // long expired: the cycle fetches now
	client.exp.mu.Unlock()
	cycleDone := make(chan struct{})
	go func() {
		client.experimentCycle(laneCtx)
		close(cycleDone)
	}()
	waitFor(t, 5*time.Second, "the lane fetch reaches the server", func() bool { return script.requestCount() >= 2 })

	start := time.Now()
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Close(closeCtx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("Close must not wait out a hung lane GET, took %v", elapsed)
	}
	select {
	case <-cycleDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("the stop-cancelled lane fetch must abort")
	}
	close(hang)
}

// Finding 4: an auth latch set mid-batch stops the automatic batch — and a
// later scripted 200 never reaches the wire to clear it from the lane.
func TestLatchStopsRevalidationBatch(t *testing.T) {
	script := &expScript{}
	script.push(200, strings.Replace(expAssignedBody("1"), expTestScopeKey, "exp-a", 1)) // host fetch A
	script.push(200, strings.Replace(expAssignedBody("1"), expTestScopeKey, "exp-b", 1)) // host fetch B
	script.push(401, `{"error":"invalid runtime token"}`)                                // lane fetch exp-a: latch
	script.push(200, strings.Replace(expAssignedBody("2"), expTestScopeKey, "exp-b", 1)) // must never be asked
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	fetchAssignment(t, client, "exp-a")
	fetchAssignment(t, client, "exp-b")
	client.exp.mu.Lock()
	client.exp.revalidateAtMS = 1
	client.exp.mu.Unlock()
	client.experimentCycle(context.Background())
	if script.requestCount() != 3 {
		t.Fatalf("the latch must stop the batch after the refusing fetch: %d requests", script.requestCount())
	}
	client.exp.mu.Lock()
	latched := client.exp.authBlocked
	client.exp.mu.Unlock()
	if !latched {
		t.Fatalf("the lane's own 401 must latch the plane")
	}
	if v := client.ExperimentVariant("exp-b"); v != "" {
		t.Fatalf("the latched plane serves nothing, got %q", v)
	}
}

// Finding 5 / R10: restored attributes re-validate against the live
// vocabulary before riding a revalidation fetch.
func TestRestoredAttributesAreRenormalized(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client1 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	if _, err := client1.FetchExperimentAssignment(context.Background(), expTestScopeKey, map[string]string{"geo": "DE"}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if err := client1.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Corrupt the stored attributes: an invented name, an oversized value.
	recordPath := filepath.Join(spoolDir, expCacheFileName)
	data, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var record expDurableRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode: %v", err)
	}
	entry := record.Entries[expTestScopeKey]
	entry.Attributes = []expAttribute{
		{Name: "geo", Value: "DE"},
		{Name: "not_in_vocabulary", Value: "x"},
		{Name: "app_version", Value: strings.Repeat("v", 600)},
	}
	record.Entries[expTestScopeKey] = entry
	raw, _ := json.Marshal(record)
	if err := os.WriteFile(recordPath, raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	client2 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client2.Close(context.Background())
	client2.exp.mu.Lock()
	client2.exp.revalidateAtMS = 1
	client2.exp.mu.Unlock()
	client2.experimentCycle(context.Background())
	waitFor(t, 5*time.Second, "the revalidation fetch", func() bool { return script.requestCount() >= 2 })
	query := script.request(1).URL.Query()
	if query.Get("geo") != "DE" {
		t.Fatalf("the valid restored attribute must ride, got %v", query)
	}
	if _, bad := query["not_in_vocabulary"]; bad {
		t.Fatalf("an out-of-vocabulary restored attribute must never be sent")
	}
	if _, bad := query["app_version"]; bad {
		t.Fatalf("an oversized restored value must never be sent")
	}
}

// Finding 6: the persisted subject id is read through a hard cap.
func TestOversizedSubjectFileIsAMiss(t *testing.T) {
	script := &expScript{}
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	planted := "spcid_" + strings.Repeat("a", 32) + strings.Repeat("#", 8192)
	if err := os.WriteFile(filepath.Join(spoolDir, expSubjectFileName), []byte(planted), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())
	fetchAssignment(t, client, expTestScopeKey)
	subject := script.request(0).URL.Query().Get("subject_key")
	if !validExperimentSubjectID(subject) || len(subject) != 38 {
		t.Fatalf("an over-cap subject file must read as a miss and re-mint, got %q", subject)
	}
}

// Finding 7: internal facts count in Stats.Enqueued.
func TestExperimentFactsCountInStats(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	before := client.Snapshot().Enqueued
	fetchAssignment(t, client, expTestScopeKey) // auto exposure
	if err := client.TrackExperimentOutcome(expTestScopeKey, "score", 1); err != nil {
		t.Fatalf("outcome: %v", err)
	}
	if got := client.Snapshot().Enqueued - before; got != 2 {
		t.Fatalf("expected both facts counted in Stats.Enqueued, got %d", got)
	}
}

// Finding 8: a fenced-out stale 429's Retry-After never parks the cadence.
func TestStaleRetryAfterDoesNotParkCadence(t *testing.T) {
	script := &expScript{}
	slowGate := make(chan struct{})
	script.gates = map[int]chan struct{}{0: slowGate}
	script.pushRetryAfter(429, ``, "86400") // request 0: the SLOW stale 429
	script.push(200, expAssignedBody("2"))  // request 1: the fast fresh verdict
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	slowDone := make(chan struct{})
	go func() {
		_, _ = client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
		close(slowDone)
	}()
	waitFor(t, 5*time.Second, "slow request reaches the server", func() bool { return script.requestCount() == 1 })
	fetchAssignment(t, client, expTestScopeKey) // settles fresh 200
	close(slowGate)                             // the stale 429 lands AFTER the newer settled outcome
	<-slowDone

	client.exp.mu.Lock()
	parkedUntil := client.exp.retryAfterMS
	client.exp.mu.Unlock()
	if parkedUntil != 0 {
		t.Fatalf("a fenced-out stale 429 must not park the cadence (retryAfterMS=%d)", parkedUntil)
	}
}

// Finding 9: experiment state files are written with the private-tightening
// chmod hook.
func TestExperimentFilesAreTightened(t *testing.T) {
	script := &expScript{}
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	// A pre-existing loose cache file: the write must tighten it.
	loose := filepath.Join(spoolDir, expCacheFileName)
	if err := os.WriteFile(loose, []byte(`{}`), 0o644); err != nil {
		t.Fatalf("plant: %v", err)
	}
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())
	fetchAssignment(t, client, expTestScopeKey)
	for _, name := range []string{expCacheFileName, expSubjectFileName} {
		info, err := os.Stat(filepath.Join(spoolDir, name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if mode := info.Mode().Perm(); mode != 0o600 {
			t.Fatalf("%s must be private (0600), got %v", name, mode)
		}
	}
	if info, err := os.Stat(spoolDir); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("the state dir must be tightened to 0700, got %v (%v)", info.Mode().Perm(), err)
	}
}

// R9: a grammar-400 with the one-shot budget already spent drops the cached
// entry durably (permanent-400 semantics), while stale rejects stay
// discarded whole.
func TestSpentBudgetGrammar400DropsEntry(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(400, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`)
	script.push(200, expAssignedBody("2"))
	script.push(400, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`)
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey) // v1
	fetchAssignment(t, client, expTestScopeKey) // grammar 400 -> remint -> v2 (budget spent)
	if v := client.ExperimentVariant(expTestScopeKey); v != "treatment" {
		t.Fatalf("precondition: v2 serves, got %q", v)
	}
	// The SECOND grammar 400: budget spent, current outcome -> durable drop.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil || !strings.Contains(err.Error(), "bad_request") {
		t.Fatalf("expected bad_request, got %v", err)
	}
	if v := client.ExperimentVariant(expTestScopeKey); v != "" {
		t.Fatalf("a spent-budget grammar 400 must drop the entry, got %q", v)
	}
	record := readExperimentRecord(t, spoolDir)
	if record != nil && len(record.Entries) != 0 {
		t.Fatalf("the drop must land durably, got %+v", record)
	}
}

// R10: durable intents carry their decision scope — a subject rotation
// cancels owed WRITES, while an owed DROP lands against the RETIRED
// record without consulting memory.
func TestScopedOwedIntentsAcrossSubjectRotation(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, `{"assigned":false,"reason":"kill_switch"}`)
	script.push(400, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`)
	script.push(200, strings.Replace(expAssignedBody("2"), expTestScopeKey, "exp-other", 1))
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
	// The owed DROP was decided against scope A. Now the subject rotates
	// (grammar remint on another experiment).
	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-other", nil); err != nil {
		t.Fatalf("remint fetch: %v", err)
	}
	restoreExperimentStorage(t, client)
	client.experimentCycle(context.Background())
	client.exp.mu.Lock()
	owed := len(client.exp.durablePending)
	client.exp.mu.Unlock()
	if owed != 0 {
		t.Fatalf("the foreign-scope drop must land or settle, %d remain", owed)
	}
	// The disk holds either the NEW scope's record (the rotation's write
	// replaced the file — the retired record is gone with its killed
	// entry) or the retired record without the killed key. Either way the
	// killed variant is not reload truth.
	if record := readExperimentRecord(t, spoolDir); record != nil {
		if _, still := record.Entries[expTestScopeKey]; still && record.Scope != "" {
			// Only acceptable if this is the NEW scope's record and the key
			// belongs to it (it does not — the new scope never fetched it).
			client.exp.mu.Lock()
			currentScope := client.exp.scopeForLocked(client.exp.currentSubjectIDLocked())
			client.exp.mu.Unlock()
			if record.Scope != currentScope {
				t.Fatalf("the killed entry survived on the retired record: %+v", record)
			}
			t.Fatalf("the killed entry leaked into the new scope's record: %+v", record)
		}
	}
}

// R9/R10: one working save folds owed sibling intents instead of leaving
// them for the retry cycle.
func TestCombinedSaveFoldsOwedSiblings(t *testing.T) {
	script := &expScript{}
	script.push(200, strings.Replace(expAssignedBody("1"), expTestScopeKey, "exp-a", 1))
	script.push(200, `{"assigned":false,"reason":"kill_switch"}`) // exp-a killed (owed under broken storage)
	script.push(200, strings.Replace(expAssignedBody("3"), expTestScopeKey, "exp-b", 1))
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	fetchAssignment(t, client, "exp-a")
	breakExperimentStorage(t, client)
	if result, err := client.FetchExperimentAssignment(context.Background(), "exp-a", nil); err != nil || result.Assigned {
		t.Fatalf("kill: %+v / %v", result, err)
	}
	restoreExperimentStorage(t, client)
	// exp-b's fresh install save must FOLD exp-a's owed drop — no cycle.
	fetchAssignment(t, client, "exp-b")
	client.exp.mu.Lock()
	owed := len(client.exp.durablePending)
	client.exp.mu.Unlock()
	if owed != 0 {
		t.Fatalf("the working save must fold the owed sibling drop, %d remain", owed)
	}
	record := readExperimentRecord(t, spoolDir)
	if record == nil || len(record.Entries) != 1 {
		t.Fatalf("expected exactly exp-b on disk, got %+v", record)
	}
	if _, killed := record.Entries["exp-a"]; killed {
		t.Fatalf("the folded drop must have removed exp-a")
	}
}

// R10: a consent purge discards owed snapshots first and re-arms live
// entries only — a since-dropped entry's owed fact does not re-emit into
// the re-granted session.
func TestPurgeDiscardsDeadOwedAndReArmsLiveOnly(t *testing.T) {
	script := &expScript{}
	capture := &expWireCapture{}
	script.push(200, expAssignedBody("1"))
	script.push(200, `{"assigned":false,"reason":"kill_switch","version":1}`)
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.BatchSize = 1
		cfg.BufferSize = 1
	})
	defer client.Close(context.Background())

	// Park the worker and fill the queue: the application's exposure stays
	// OWED, then the kill drops the live entry — the owed snapshot is now
	// DEAD (its entry is gone).
	parkWorkerWithFullQueue(t, client, capture)
	fetchAssignment(t, client, expTestScopeKey)
	if result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err != nil || result.Assigned {
		t.Fatalf("kill: %+v / %v", result, err)
	}
	client.exp.mu.Lock()
	owedBefore := len(client.exp.pendingExposure[expTestScopeKey])
	client.exp.mu.Unlock()
	if owedBefore == 0 {
		t.Fatalf("precondition: the dead owed snapshot exists")
	}
	capture.setStatus(202)
	client.queue.drainAll()
	// The purge: dead owed discarded, nothing live to re-arm.
	client.SetConsent(false)
	client.SetConsent(true)
	client.exp.mu.Lock()
	owedAfter := 0
	for _, list := range client.exp.pendingExposure {
		owedAfter += len(list)
	}
	client.exp.mu.Unlock()
	if owedAfter != 0 {
		t.Fatalf("the purge must discard dead owed snapshots, %d remain", owedAfter)
	}
	client.experimentCycle(context.Background())
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if facts := capture.exposures(); len(facts) != 0 {
		t.Fatalf("a dead owed fact must not re-emit after the purge, got %d", len(facts))
	}
}

// Regression pin for the folded-save fence: a folded owed drop still yields
// to a strictly-fresher stored sibling entry.
func TestFoldedDropYieldsToFresherStoredSibling(t *testing.T) {
	e := newExperimentsState(Config{
		WorkspaceID:     "ws",
		AppID:           "app",
		EnvironmentID:   "dev",
		APIKey:          "k",
		RemoteConfigURL: "https://cp.example",
	})
	scope := "scope-a"
	// The record holds exp-x stamped at 100; an owed drop decided at 50
	// (a rollback-era decision) must yield on the fold path.
	record := &expDurableRecord{Scope: scope, Entries: map[string]expEntry{
		"exp-x": {AssignmentKey: "a", VariantKey: "v", Version: 1, AssignmentUnit: "client_id", FetchedAtMS: 100},
	}}
	e.durablePending["exp-x"] = expOwedSync{asOf: 50, drop: true, scope: scope}
	folded := e.foldOwedIntentsLocked(record, scope, "")
	if len(folded) != 1 || folded[0] != "exp-x" {
		t.Fatalf("the outranked drop must settle via the fold, got %v", folded)
	}
	if _, still := record.Entries["exp-x"]; !still {
		t.Fatalf("the fresher stored sibling must survive the outranked drop")
	}
}

var _ = fmt.Sprintf // keep fmt imported if assertions above change
