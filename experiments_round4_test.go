package shardpilot

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// ── review round 4 ──────────────────────────────────────────────────────────

// Finding 1: an over-limit error body is a truncated VIEW and never a
// sentinel, even when its in-limit prefix unmarshals (sentinel JSON followed
// by padding); it reads as generic for its status.
func TestOverLimitSentinelBodiesReadGeneric(t *testing.T) {
	scope := expTestRequestScope()
	pad := strings.Repeat(" ", expMaxBodyBytes)

	over403 := expTestResponse(403, `{"error":"`+expSentinelRealSubjectsDisabled+`"}`+pad)
	_, outcome, failure := applyExperimentAssignment(nil, over403, scope, 42)
	if failure != "unauthorized" || !outcome.authBlocked || !outcome.authoritative {
		t.Fatalf("an over-limit 403 still fails closed generically, got %+v failure=%q", outcome, failure)
	}
	if outcome.dropAll {
		t.Fatalf("an over-limit 403 body must never read as the real-subjects sentinel")
	}

	over400 := expTestResponse(400, `{"error":"`+expSentinelSubjectGrammar+`"}`+pad)
	_, outcome400, _ := applyExperimentAssignment(nil, over400, scope, 42)
	if outcome400.remint {
		t.Fatalf("an over-limit 400 body must never read as the grammar sentinel")
	}
	if !outcome400.dropEntry || !outcome400.authoritative {
		t.Fatalf("an over-limit 400 reads as the generic permanent 400, got %+v", outcome400)
	}

	// In-limit sanity: the exact sentinels still classify.
	_, sentinel403, _ := applyExperimentAssignment(nil,
		expTestResponse(403, `{"error":"`+expSentinelRealSubjectsDisabled+`"}`), scope, 42)
	if !sentinel403.dropAll {
		t.Fatalf("the in-limit real-subjects sentinel must still classify")
	}
	_, sentinel400, _ := applyExperimentAssignment(nil,
		expTestResponse(400, `{"error":"`+expSentinelSubjectGrammar+`"}`), scope, 42)
	if !sentinel400.remint {
		t.Fatalf("the in-limit grammar sentinel must still classify")
	}
}

// Finding 2: `Retry-After: 0` is the server's explicit "retry immediately"
// — a PRESENT hint arms no deferral and never feeds the backoff streak; only
// an ABSENT hint backs off.
func TestZeroRetryAfterHintSkipsDeferralAndBackoff(t *testing.T) {
	e := &experimentsState{}
	for i := 0; i < 3; i++ {
		e.paceTransientLocked(1000, 0, true)
	}
	if e.retryAfterMS != 0 {
		t.Fatalf("a present zero hint must not arm a deferral, got %d", e.retryAfterMS)
	}
	if e.backoffAttempt != 0 {
		t.Fatalf("a present zero hint must not feed the backoff streak, got %d", e.backoffAttempt)
	}
	// The absent-hint backoff still engages on the second consecutive
	// failure.
	e.paceTransientLocked(1000, 0, false)
	e.paceTransientLocked(1000, 0, false)
	if e.retryAfterMS == 0 {
		t.Fatalf("the absent-hint backoff must still arm")
	}
	// Classifier row: a literal `Retry-After: 0` propagates PRESENT with
	// zero seconds, so the pacing sees the hint rather than a silence.
	resp := expTestResponse(429, ``)
	resp.retryAfterRaw = "0"
	_, outcome, _ := applyExperimentAssignment(nil, resp, expTestRequestScope(), 42)
	if !outcome.retryAfterPresent || outcome.retryAfterSeconds != 0 {
		t.Fatalf("Retry-After: 0 must classify present-zero, got %+v", outcome)
	}
}

// Finding 3: the grammar-remint RETRY is the one legitimate entry-less
// revalidation dispatch — the rotation it rides just cleared the cache by
// design. The vanished-entry guard must not kill the lane path's one-shot
// self-heal.
func TestLaneRemintRetryStillDispatches(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(400, `{"error":"`+expSentinelSubjectGrammar+`"}`)
	script.push(200, expAssignedBody("2"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = t.TempDir() })
	defer client.Close(context.Background())

	result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey,
		map[string]string{"custom_attribute_plan": "pro"})
	if err != nil || !result.Assigned {
		t.Fatalf("seed fetch: %v %+v", err, result)
	}

	// Make the revalidation due and run one lane cycle: it dispatches the
	// revalidation (400 grammar sentinel), re-mints, and the RETRY must
	// still dispatch — with the fresh subject and the exact attribute set —
	// and reinstall from its 200.
	client.exp.mu.Lock()
	client.exp.revalidateAtMS = 1
	client.exp.mu.Unlock()
	client.experimentCycle(context.Background())

	if got := script.requestCount(); got != 3 {
		t.Fatalf("expected seed + revalidation + remint retry = 3 requests, got %d", got)
	}
	seedQuery := script.request(0).URL.Query()
	retryQuery := script.request(2).URL.Query()
	if seedQuery.Get("subject_key") == retryQuery.Get("subject_key") {
		t.Fatalf("the remint retry must carry the freshly minted subject")
	}
	if retryQuery.Get("custom_attribute_plan") != "pro" {
		t.Fatalf("the remint retry must carry the rejected request's exact attribute set, got %v", retryQuery)
	}
	if v := client.ExperimentVariant(expTestScopeKey); v != "treatment" {
		t.Fatalf("the self-healed assignment must serve, got %q", v)
	}
}

// Finding 4: armExposureLocked refreshes a same-(session, tuple) tail
// snapshot IN PLACE, so the sweep must copy the snapshot's fields under the
// lock before emitting. Run under -race: the pre-fix sweep read head.entry
// after unlocking and raced the refresh.
func TestOwedSweepAndArmRefreshDoNotRace(t *testing.T) {
	script := &expScript{}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.BatchSize = 1
		cfg.BufferSize = 1
	})
	defer client.Close(context.Background())
	parkWorkerWithFullQueue(t, client, capture)
	fetchAssignment(t, client, expTestScopeKey) // owed snapshot armed (queue full)

	e := client.exp
	e.mu.Lock()
	entry := e.entries[expTestScopeKey]
	e.mu.Unlock()
	if entry == nil {
		t.Fatalf("test setup: no cached entry")
	}
	// Time-bounded loops so the two sides genuinely overlap: a count-bounded
	// arm loop can finish before the sweep's first unlocked read, and the
	// detector only flags accesses it observes running concurrently.
	deadline := time.Now().Add(150 * time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			client.sweepExperimentExposures(expTestScopeKey)
		}
	}()
	for time.Now().Before(deadline) {
		e.mu.Lock()
		e.armExposureLocked(expTestScopeKey, entry) // same tuple: in-place refresh
		e.mu.Unlock()
		runtime.Gosched()
	}
	wg.Wait()
	capture.setStatus(http.StatusAccepted)
}

// ── unreal round-2 parity: the sentinel withdraws PIPELINE-resident facts ───

// Queue-resident: an exposure fact already accepted into the shared queue
// must not ship once the real-subjects sentinel lands; host events in the
// same queue survive.
func TestSentinelPurgesQueuedExperimentFacts(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(403, `{"error":"`+expSentinelRealSubjectsDisabled+`"}`)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.BatchSize = 1
		cfg.BufferSize = 4
	})
	defer client.Close(context.Background())

	// Park the worker on a failing ingest holding a filler, so everything
	// enqueued after stays QUEUE-resident.
	capture.setStatus(http.StatusInternalServerError)
	if err := client.Enqueue(Event{Name: "filler_parked"}); err != nil {
		t.Fatalf("filler: %v", err)
	}
	waitFor(t, 5*time.Second, "the worker parks on the failing ingest", func() bool { return capture.hitCount() >= 1 })

	fetchAssignment(t, client, expTestScopeKey) // auto exposure fact -> queue
	if err := client.Enqueue(Event{Name: "host_survivor"}); err != nil {
		t.Fatalf("host survivor: %v", err)
	}

	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil {
		t.Fatalf("the sentinel fetch must fail closed")
	}

	capture.setStatus(http.StatusAccepted)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := capture.exposures(); len(got) != 0 {
		t.Fatalf("a queue-resident exposure fact shipped after the sentinel: %v", got)
	}
	delivered := map[string]bool{}
	capture.mu.Lock()
	for _, envelope := range capture.envelopes {
		name, _ := envelope["event_name"].(string)
		delivered[name] = true
	}
	capture.mu.Unlock()
	if !delivered["filler_parked"] || !delivered["host_survivor"] {
		t.Fatalf("host events must survive the sentinel purge, delivered=%v", delivered)
	}
	if client.Snapshot().Dropped == 0 {
		t.Fatalf("the withdrawn fact must count in Stats.Dropped")
	}
}

// Worker-batch-resident: a fact the worker already pulled into its held
// batch filters at the next dispatch point — the post-sentinel retry must
// not redeliver it.
func TestSentinelPurgesWorkerBatchFacts(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(403, `{"error":"`+expSentinelRealSubjectsDisabled+`"}`)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.BatchSize = 1
		cfg.BufferSize = 4
	})
	defer client.Close(context.Background())

	capture.setStatus(http.StatusInternalServerError)
	fetchAssignment(t, client, expTestScopeKey) // exposure fact -> queue -> worker batch
	waitFor(t, 5*time.Second, "the worker parks holding the fact", func() bool { return capture.hitCount() >= 1 })

	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil {
		t.Fatalf("the sentinel fetch must fail closed")
	}

	capture.setStatus(http.StatusAccepted)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := capture.exposures(); len(got) != 0 {
		t.Fatalf("a batch-resident exposure fact shipped after the sentinel: %v", got)
	}
	if err := client.Enqueue(Event{Name: "post_sentinel_host"}); err != nil {
		t.Fatalf("post-sentinel host event: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush 2: %v", err)
	}
	found := false
	capture.mu.Lock()
	for _, envelope := range capture.envelopes {
		if envelope["event_name"] == "post_sentinel_host" {
			found = true
		}
	}
	capture.mu.Unlock()
	if !found {
		t.Fatalf("host events must keep flowing after the sentinel purge")
	}
}

// Spool-resident: a fact spooled to disk by an earlier session must be
// removed (and dead-lettered SpoolDropTerminal) when the sentinel lands;
// spooled host events survive and deliver.
func TestSentinelRemovesSpooledExperimentFacts(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(403, `{"error":"`+expSentinelRealSubjectsDisabled+`"}`)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()

	// Session 1: the exposure fact and a host event both spool at close
	// (the ingest refuses; the spool accepts only under a durable analytics
	// grant, so record one first).
	capture.setStatus(http.StatusInternalServerError)
	client1 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	client1.SetConsent(true)
	fetchAssignment(t, client1, expTestScopeKey)
	if err := client1.Enqueue(Event{Name: "host_spooled"}); err != nil {
		t.Fatalf("host event: %v", err)
	}
	// Close reports the refused delivery; the batch spools regardless — the
	// spool content check below is the setup's real gate.
	_ = client1.Close(context.Background())
	spoolPath := filepath.Join(spoolDir, "spool.json")
	data, err := os.ReadFile(spoolPath)
	if err != nil || !strings.Contains(string(data), experimentExposureName) {
		t.Fatalf("test setup: the exposure fact must be spooled (err=%v)", err)
	}

	// Session 2: the sentinel lands before any resend — the spooled fact is
	// withdrawn and dead-lettered; the host event stays and delivers.
	var letters []SpoolDeadLetter
	var lettersMu sync.Mutex
	client2 := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.SpoolDir = spoolDir
		cfg.OnSpoolDeadLetter = func(letter SpoolDeadLetter) {
			lettersMu.Lock()
			letters = append(letters, letter)
			lettersMu.Unlock()
		}
	})
	defer client2.Close(context.Background())
	if _, err := client2.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil {
		t.Fatalf("the sentinel fetch must fail closed")
	}

	lettersMu.Lock()
	sawTerminalFact := false
	for _, letter := range letters {
		for _, envelope := range letter.Envelopes {
			if letter.Reason == SpoolDropTerminal && strings.Contains(string(envelope), experimentExposureName) {
				sawTerminalFact = true
			}
		}
	}
	lettersMu.Unlock()
	if !sawTerminalFact {
		t.Fatalf("the withdrawn spooled fact must dead-letter SpoolDropTerminal, got %v", letters)
	}
	data, err = os.ReadFile(spoolPath)
	if err == nil && strings.Contains(string(data), experimentExposureName) {
		t.Fatalf("the spooled fact must be removed from the record")
	}

	capture.setStatus(http.StatusAccepted)
	if err := client2.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := capture.exposures(); len(got) != 0 {
		t.Fatalf("a spool-resident exposure fact shipped after the sentinel: %v", got)
	}
	foundHost := false
	capture.mu.Lock()
	for _, envelope := range capture.envelopes {
		if envelope["event_name"] == "host_spooled" {
			foundHost = true
		}
	}
	capture.mu.Unlock()
	if !foundHost {
		t.Fatalf("the spooled host event must survive the purge and deliver")
	}
}
