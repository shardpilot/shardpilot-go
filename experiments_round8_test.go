package shardpilot

// Round-8 review regression tests:
//
//  0. The worker's receive-time consent-epoch boundary: a deny →
//     quick-re-grant round trip must not fold post-grant events into the
//     stale pre-denial batch (where the next epoch drop would silently
//     discard them), and a pre-denial event stolen from the denial's queue
//     drain must still die by its intake stamp.
//  1. Client-sourced experiment facts carry the arm-time session identity
//     (session_id), while backend-source host events keep the contract's
//     session_id carve-out.
//  2. A consent denial landing while an assignment GET is in flight aborts
//     the request at the transport (granted-only plane, forced-minor's
//     zero-traffic promise) instead of letting it run out its window with
//     only the settle-time discard.
//  3. The sentinel's spool sweep cannot withdraw a FRESH post-sentinel
//     fact (re-ruled by round 11): the decisive epoch bump lands under
//     e.mu before the pipeline purge, so a fact born in the gap carries
//     the post-sentinel stamp and the sweep's epoch guard spares it.
//  4. A transient park pulls the pre-armed revalidation deadline DOWN, so
//     the parked batch's failed and skipped keys retry at the pacing
//     deadline instead of stranding until the full 300s interval.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// byName returns the delivered envelopes carrying the given event name.
func (w *expWireCapture) byName(name string) []map[string]any {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []map[string]any
	for _, envelope := range w.envelopes {
		if envelope["event_name"] == name {
			out = append(out, envelope)
		}
	}
	return out
}

// ── finding 0: epoch partitions batches ─────────────────────────────────────

func TestDenyRegrantPreservesPostGrantEventsInParkedWorkerBatch(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	// Plain analytics path: the boundary is a worker behavior, not an
	// experiments one. BatchSize 8 keeps the worker parked on a partial
	// batch; the hour-long flush interval keeps the ticker out of the way.
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.ExperimentsEnabled = false
	})
	defer client.Close(context.Background())

	if err := client.Enqueue(Event{Name: "pre_denial"}); err != nil {
		t.Fatalf("enqueue pre_denial: %v", err)
	}
	waitFor(t, 5*time.Second, "the worker pulls the pre-denial event into its held batch", func() bool {
		return len(client.queue.ch) == 0
	})

	// The denial condemns the held event; the quick re-grant admits fresh
	// ones while the worker is still parked with the stale batch.
	client.SetConsent(false)
	client.SetConsent(true)
	if err := client.Enqueue(Event{Name: "post_grant"}); err != nil {
		t.Fatalf("enqueue post_grant: %v", err)
	}
	waitFor(t, 5*time.Second, "the worker pulls the post-grant event", func() bool {
		return len(client.queue.ch) == 0
	})
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if got := len(capture.byName("post_grant")); got != 1 {
		t.Fatalf("the post-grant event must survive the epoch boundary and publish exactly once, got %d deliveries", got)
	}
	if got := len(capture.byName("pre_denial")); got != 0 {
		t.Fatalf("the pre-denial event must never publish, got %d deliveries", got)
	}
	if dropped := client.Snapshot().Dropped; dropped != 1 {
		t.Fatalf("exactly the pre-denial event counts as dropped, got %d", dropped)
	}
}

func TestConsentEpochBoundaryDropsStolenStaleEventAtAdmission(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.ExperimentsEnabled = false
	})
	// The admission boundary is worker-goroutine-owned state: stop the
	// worker so this test goroutine is its sole accessor.
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	seen := client.consentEpoch.Load()
	droppedBefore := client.Snapshot().Dropped
	batch := []Event{{Name: "held_pre_denial", intakeConsentEpoch: seen}}
	// A denial moves the epoch while the worker "held" the batch. The
	// worker then receives an event its select stole from the denial's
	// queue drain: admitted pre-denial, stamped with the old epoch.
	client.consentEpoch.Add(1)
	backoff := 3
	batch = client.admitReceivedEvent(batch, Event{Name: "stolen_pre_denial", intakeConsentEpoch: seen}, &seen, &backoff)
	if len(batch) != 0 {
		t.Fatalf("the boundary must drop the held batch AND refuse the stolen stale-stamped event, got %d retained", len(batch))
	}
	if backoff != 0 {
		t.Fatalf("the discarded batch takes its backoff streak with it, got %d", backoff)
	}
	if seen != client.consentEpoch.Load() {
		t.Fatalf("the boundary must settle the seen epoch")
	}
	// An event admitted under the settled epoch joins the fresh batch.
	batch = client.admitReceivedEvent(batch, Event{Name: "fresh_post_grant", intakeConsentEpoch: seen}, &seen, &backoff)
	if len(batch) != 1 || batch[0].Name != "fresh_post_grant" {
		t.Fatalf("a current-epoch event must join the batch, got %v", batch)
	}
	if dropped := client.Snapshot().Dropped - droppedBefore; dropped != 2 {
		t.Fatalf("the held and the stolen event count once each, got %d", dropped)
	}
}

// ── finding 1: session identity on client-sourced facts ─────────────────────

func TestExperimentFactsCarrySessionIdentity(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	// The harness default is the finding's exact configuration: a normal
	// backend-source client whose experiment facts override to
	// source=client — entering the class the ingest contract requires
	// session_id on.
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	if err := client.TrackExperimentOutcome(expTestScopeKey, "score", 1); err != nil {
		t.Fatalf("outcome: %v", err)
	}
	if err := client.Enqueue(Event{Name: "host_backend_event"}); err != nil {
		t.Fatalf("host event: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	client.exp.mu.Lock()
	marker := client.exp.sessionMarker
	client.exp.mu.Unlock()

	exposures := capture.exposures()
	if len(exposures) != 1 {
		t.Fatalf("precondition: one exposure delivered, got %d", len(exposures))
	}
	if got := exposures[0]["session_id"]; got != marker {
		t.Fatalf("the exposure must carry the arm-time session identity %q, got %v", marker, got)
	}
	outcomes := capture.byName(experimentOutcomeName)
	if len(outcomes) != 1 {
		t.Fatalf("precondition: one outcome delivered, got %d", len(outcomes))
	}
	if got := outcomes[0]["session_id"]; got != marker {
		t.Fatalf("the outcome must carry the session identity %q, got %v", marker, got)
	}
	// The backend-source carve-out is untouched: a host event under this
	// configuration publishes as source=backend with no session_id.
	hosts := capture.byName("host_backend_event")
	if len(hosts) != 1 {
		t.Fatalf("precondition: the host event delivered, got %d", len(hosts))
	}
	if got, present := hosts[0]["session_id"]; present {
		t.Fatalf("a backend-source host event keeps the session_id carve-out, got %v", got)
	}
	if got := hosts[0]["source"]; got != "backend" {
		t.Fatalf("the host event's source must stay backend, got %v", got)
	}
}

// ── finding 2: denial aborts the in-flight assignment GET ───────────────────

func TestConsentDenialAbortsInFlightAssignmentGET(t *testing.T) {
	entered := make(chan struct{})
	var enterOnce sync.Once
	var handlerSawAbort atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc(expAssignmentRoute, func(w http.ResponseWriter, r *http.Request) {
		enterOnce.Do(func() { close(entered) })
		select {
		case <-r.Context().Done():
			handlerSawAbort.Store(true)
		case <-time.After(3 * time.Second):
			// The guard: without the denial gate the request parks here
			// for its full window while the plane is already refused.
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})
	mux.HandleFunc("/v1/consent", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{}`))
	})
	capture := &expWireCapture{}
	mux.HandleFunc("/", capture.handler())
	server := httptest.NewServer(mux)
	defer server.Close()

	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.HTTPTimeout = 5 * time.Second
	})
	defer client.Close(context.Background())

	fetchErr := make(chan error, 1)
	go func() {
		_, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
		fetchErr <- err
	}()
	<-entered
	denyAt := time.Now()
	client.SetConsent(false)

	select {
	case err := <-fetchErr:
		if !errors.Is(err, ErrConsentDenied) {
			t.Fatalf("a denial-aborted fetch must surface the refusal, got %v", err)
		}
		if elapsed := time.Since(denyAt); elapsed > 1500*time.Millisecond {
			t.Fatalf("the GET must abort AT the denial, not run out its transport window (returned %v after the denial)", elapsed)
		}
	case <-time.After(4 * time.Second):
		t.Fatalf("the fetch stayed in flight long after the denial")
	}
	waitFor(t, 2*time.Second, "the parked handler observes the aborted request", handlerSawAbort.Load)

	// An aborted wire exchange is a refusal, never an endpoint outcome:
	// nothing paces.
	client.exp.mu.Lock()
	retryAfter, backoff := client.exp.retryAfterMS, client.exp.backoffAttempt
	client.exp.mu.Unlock()
	if retryAfter != 0 || backoff != 0 {
		t.Fatalf("an aborted fetch must not pace the plane (retryAfterMS=%d, backoffAttempt=%d)", retryAfter, backoff)
	}
}

// ── finding 3: the spool sweep spares fresh post-purge facts ────────────────

func TestSentinelSpoolSweepSparesFreshPostPurgeFacts(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.SpoolDir = spoolDir
		cfg.SpoolMaxEvents = 10000
		cfg.SpoolMaxBytes = 1 << 22
	})
	defer client.Close(context.Background())
	// Spooling is grant-only: the failed-publish path refuses to persist
	// for an undecided actor.
	client.SetConsent(true)

	entry := &expEntry{
		VariantKey:     "treatment",
		Version:        1,
		AssignmentUnit: experimentAssignmentUnitClientID,
		SubjectFactKey: "sfk1_" + strings.Repeat("a", 64),
		SubjectKey:     "spcid_" + strings.Repeat("b", 32),
	}

	spoolPath := filepath.Join(spoolDir, "spool.json")
	for i := 0; i < 60; i++ {
		// Seed one withdrawn (pre-purge) fact: the observable for WHEN the
		// sweep ran relative to the emit window's release.
		seedID := fmt.Sprintf("expfact-seed-%04d", i)
		seedEvent, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-race", entry, seedID, client.exp.sessionMarker)
		if skip != "" {
			t.Fatalf("iteration %d: seed build refused (%s)", i, skip)
		}
		seedRequest, err := client.buildBatch([]Event{seedEvent})
		if err != nil {
			t.Fatalf("iteration %d: buildBatch: %v", i, err)
		}
		client.spoolFailedBatch(seedRequest, errors.New("http 500"), false)
		if data, err := os.ReadFile(spoolPath); err != nil || !strings.Contains(string(data), seedID) {
			t.Fatalf("iteration %d: the seed fact must be spool-resident before the purge (err=%v)", i, err)
		}

		// Round 11 re-ruled the protection from mutual exclusion to STAMPS:
		// the sentinel's decisive bump happens under e.mu BEFORE the
		// pipeline purge even starts, so a fresh fact born anywhere in the
		// gap — after the sentinel, before (or racing) the purge's spool
		// sweep — carries the post-sentinel stamp and is spared by the
		// sweep's epoch guard, while everything stamped before the
		// sentinel is withdrawn. Model exactly that worst case: the fresh
		// fact is built and spooled BETWEEN the bump and the sweep.
		client.exp.mu.Lock()
		client.exp.factPurgeEpochBumpFn()
		client.exp.mu.Unlock()

		freshID := fmt.Sprintf("expfact-fresh-%04d", i)
		freshEvent, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-race", entry, freshID, client.exp.sessionMarker)
		if skip != "" {
			t.Fatalf("iteration %d: fresh build refused (%s)", i, skip)
		}
		freshRequest, err := client.buildBatch([]Event{freshEvent})
		if err != nil {
			t.Fatalf("iteration %d: buildBatch: %v", i, err)
		}
		client.spoolFailedBatch(freshRequest, errors.New("http 500"), false)

		// The pipeline purge sweeps AFTER the gap-born fact reached the
		// spool: stamp-aware, it withdraws the pre-sentinel seed and
		// spares the post-sentinel fact.
		client.purgeWithdrawnExperimentFacts()
		data, err := os.ReadFile(spoolPath)
		if err != nil {
			t.Fatalf("iteration %d: read spool: %v", i, err)
		}
		if strings.Contains(string(data), seedID) {
			t.Fatalf("iteration %d: the pre-sentinel fact must be withdrawn by the sweep, spool: %s", i, data)
		}
		if !strings.Contains(string(data), freshID) {
			t.Fatalf("iteration %d: the fresh post-sentinel fact must survive the sweep that follows it, spool: %s", i, data)
		}
	}
}

// ── finding 4: a transient park pulls the cadence deadline down ─────────────

func TestTransientParkPullsCadenceDeadlineDown(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.pushRetryAfter(429, ``, "5")
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	clock := &expFakeClock{now: time.Now()}
	client := newExperimentClient(t, server.URL, nil)
	client.clock = clock
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	if script.requestCount() != 1 {
		t.Fatalf("precondition: the install fetch, got %d", script.requestCount())
	}

	// The due cycle pre-arms the next interval, then its batch parks on the
	// 429's Retry-After: 5.
	clock.advance(331 * time.Second)
	client.experimentCycle(context.Background())
	if script.requestCount() != 2 {
		t.Fatalf("the due cycle must dispatch the revalidation, got %d requests", script.requestCount())
	}

	// Past the park the plane must probe again AT the pacing deadline —
	// the documented Retry-After recovery — not sit out the ~300s interval
	// the cycle pre-armed before the failure.
	clock.advance(6 * time.Second)
	client.experimentCycle(context.Background())
	if script.requestCount() != 3 {
		t.Fatalf("the expired park must release the revalidation at the pacing deadline, got %d requests (the pre-armed interval strands the recovery)", script.requestCount())
	}
}
