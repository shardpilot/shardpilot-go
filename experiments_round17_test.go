package shardpilot

// Review round 17 — regression pins. Each test fails on the pre-fix tree
// for its finding's exact reason (verified mechanically via targeted
// temporary reverts of the fix, with the test seams retained).

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// ── G17-1: the first transient pulls the cadence down to the backoff ────────

func TestFirstTransientPullsCadenceToBackoff(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(500, `{"error":"backend unavailable"}`) // transient, no Retry-After
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer func() { _ = client.Close(context.Background()) }()
	client.SetConsent(true)
	if result := fetchAssignment(t, client, expTestScopeKey); result.VariantKey != "treatment" {
		t.Fatalf("seed fetch: %+v", result)
	}

	// The cadence is due; the cycle arms the NEXT 300s interval before
	// dispatching, then the revalidation's FIRST transient (hint-less)
	// must pull that pre-armed deadline down to the documented base
	// backoff — not leave the first outage probe waiting out the interval.
	client.exp.mu.Lock()
	client.exp.revalidateAtMS = 1
	client.exp.mu.Unlock()
	startMS := time.Now().UnixMilli()
	client.experimentCycle(context.Background())

	client.exp.mu.Lock()
	nextAtMS := client.exp.revalidateAtMS
	attempt := client.exp.backoffAttempt
	client.exp.mu.Unlock()
	if attempt != 1 {
		t.Fatalf("test shape: the hint-less transient must start the backoff streak, got attempt=%d", attempt)
	}
	if nextAtMS <= startMS {
		t.Fatalf("the cadence must stay armed after the transient, got %d", nextAtMS)
	}
	// Base backoff is 1s; anything within a generous few seconds proves the
	// pull-down, while the pre-fix full interval sits ~300s out.
	if nextAtMS > startMS+5000 {
		t.Fatalf("the FIRST hint-less transient left the pre-armed 300s cadence standing (next probe in %dms): a brief blip delays kill-switch/republish convergence by a full interval instead of the documented base backoff", nextAtMS-startMS)
	}
}

// ── G17-2: sentinel purges withdraw only SDK-authored facts ─────────────────

func TestSentinelSparesHostLookalikeEvents(t *testing.T) {
	sfk := "sfk1_" + strings.Repeat("a", 64)
	t.Run("queue_and_worker_legs", func(t *testing.T) {
		script := &expScript{}
		script.push(200, expAssignedBody("1"))
		capture := &expWireCapture{}
		server := newExperimentServer(t, script, capture)
		defer server.Close()
		client := newExperimentClient(t, server.URL, nil)
		defer func() { _ = client.Close(context.Background()) }()
		client.SetConsent(true)
		// The genuine SDK fact: the fetch's automatic exposure, held in the
		// pipeline (BatchSize 8, no flush yet).
		if result := fetchAssignment(t, client, expTestScopeKey); result.VariantKey != "treatment" {
			t.Fatalf("setup fetch: %+v", result)
		}
		// The HOST-authored lookalike: reserved-looking name + sfk1_-shaped
		// assignment_key through the public intake — never an SDK fact.
		if err := client.Enqueue(Event{ID: "host-lookalike", Name: experimentExposureName, Props: map[string]any{"assignment_key": sfk}}); err != nil {
			t.Fatalf("host enqueue: %v", err)
		}
		client.exp.mu.Lock()
		client.exp.applySentinelWithdrawalLocked("g17-2-scope", time.Now().UnixMilli())
		client.exp.mu.Unlock()
		client.purgeWithdrawnExperimentFacts()
		if err := client.Flush(context.Background()); err != nil {
			t.Fatalf("flush: %v", err)
		}
		exposures := capture.exposures()
		hostDelivered := false
		for _, envelope := range exposures {
			if envelope["event_id"] == "host-lookalike" {
				hostDelivered = true
			}
		}
		if !hostDelivered {
			t.Fatalf("the real-subjects sentinel dropped a HOST-authored event that merely uses the reserved-looking name + sfk1_ prop combination: the purge must require the SDK-internal authorship marker, never match public shape alone (delivered exposures: %v)", exposures)
		}
		if len(exposures) != 1 {
			t.Fatalf("the genuine SDK fact must still be withdrawn (expected only the host lookalike, got %d exposure envelopes: %v)", len(exposures), exposures)
		}
	})
	t.Run("spool_and_chunk_legs", func(t *testing.T) {
		script := &expScript{}
		script.push(200, expAssignedBody("1"))
		capture := &expWireCapture{}
		server := newExperimentServer(t, script, capture)
		defer server.Close()
		spoolDir := t.TempDir()
		client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
		defer func() { _ = client.Close(context.Background()) }()
		client.SetConsent(true)
		if result := fetchAssignment(t, client, expTestScopeKey); result.VariantKey != "treatment" {
			t.Fatalf("setup fetch: %+v", result)
		}
		client.exp.mu.Lock()
		entry := client.exp.entries[expTestScopeKey]
		marker := client.exp.sessionMarker
		client.exp.mu.Unlock()
		sdkFact, skip := client.buildExperimentFactEvent(experimentExposureName, expTestScopeKey, entry, "sdk-spooled-fact", marker, client.expFactPurgeEpoch.Load())
		if skip != "" {
			t.Fatalf("fact build refused (%s)", skip)
		}
		hostLookalike := Event{ID: "host-spooled-lookalike", Name: experimentExposureName, AnonymousID: "anon-test", Props: map[string]any{"assignment_key": sfk}}
		request, err := client.buildBatch([]Event{sdkFact, hostLookalike})
		if err != nil {
			t.Fatalf("buildBatch: %v", err)
		}
		client.spoolFailedBatch(request, fmt.Errorf("http 500"), false)
		client.spool.mu.Lock()
		_, sdkSpooled := client.spool.ids["sdk-spooled-fact"]
		_, hostSpooled := client.spool.ids["host-spooled-lookalike"]
		client.spool.mu.Unlock()
		if !sdkSpooled || !hostSpooled {
			t.Fatalf("test shape: both entries must be spooled (sdk=%v host=%v)", sdkSpooled, hostSpooled)
		}

		client.exp.mu.Lock()
		client.exp.applySentinelWithdrawalLocked("g17-2-scope", time.Now().UnixMilli())
		client.exp.mu.Unlock()
		client.purgeWithdrawnExperimentFacts()
		client.spool.mu.Lock()
		_, sdkKept := client.spool.ids["sdk-spooled-fact"]
		_, hostKept := client.spool.ids["host-spooled-lookalike"]
		client.spool.mu.Unlock()
		if hostKept == false {
			t.Fatalf("the sentinel's spool sweep removed a HOST-authored envelope that merely resembles an experiment fact: the raw-shape match must be paired with the persisted SDK-authorship flag")
		}
		if sdkKept {
			t.Fatalf("the genuine SDK fact must still be swept from the spool")
		}

		// The pulled-chunk transport handoff shares the predicate: a host
		// lookalike entry (no authorship flag) passes, the stale-stamped SDK
		// fact is withheld.
		chunk := []spoolEntry{
			{id: "chunk-host", ts: time.Now().UTC().Format(time.RFC3339Nano), raw: round5FactRaw("chunk-host")},
			{id: "chunk-fact", ts: time.Now().UTC().Format(time.RFC3339Nano), raw: round5FactRaw("chunk-fact"), internalFact: true},
		}
		kept := client.dropWithdrawnSpoolChunkMembers(chunk)
		keptIDs := make([]string, 0, len(kept))
		for _, member := range kept {
			keptIDs = append(keptIDs, member.id)
		}
		joined := strings.Join(keptIDs, ",")
		if !strings.Contains(joined, "chunk-host") {
			t.Fatalf("the chunk handoff withheld a host-authored member (no SDK-authorship flag): kept %q", joined)
		}
		if strings.Contains(joined, "chunk-fact") {
			t.Fatalf("the chunk handoff must still withhold the withdrawn SDK fact: kept %q", joined)
		}
	})
}

// ── G17-3: the sentinel purge never reorders the queue ──────────────────────

func TestSentinelPurgePreservesQueueOrder(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		// One event per wire batch: the capture's envelope sequence IS the
		// worker's publish order.
		cfg.BatchSize = 1
		cfg.BufferSize = 64
	})
	defer func() { _ = client.Close(context.Background()) }()
	client.SetConsent(true)
	entry := &expEntry{
		VariantKey:     "treatment",
		Version:        1,
		AssignmentUnit: experimentAssignmentUnitClientID,
		SubjectFactKey: "sfk1_" + strings.Repeat("a", 64),
		SubjectKey:     "spcid_" + strings.Repeat("b", 32),
	}

	// Each iteration races the sentinel purge against the worker's live
	// receive with ordered host events (and one withdrawn-stamp fact)
	// sharing the queue. The purge must never reorder the host events: a
	// drain/re-enqueue filter holds an earlier keeper out of the channel
	// while the worker can still receive a later one. The purge goroutine
	// re-runs the purge in a tight loop — every pass over the still-queued
	// residue is a fresh drain window against the worker's concurrent
	// receive (post-fix each pass is a queue no-op, so the loop costs
	// nothing and reorders nothing).
	const (
		iterations     = 30
		hostsPerRound  = 8
		purgesPerRound = 25
	)
	for i := 0; i < iterations; i++ {
		staleFact, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-order", entry, fmt.Sprintf("order-fact-%03d", i), client.exp.sessionMarker, client.expFactPurgeEpoch.Load())
		if skip != "" {
			t.Fatalf("iteration %d: fact build refused (%s)", i, skip)
		}
		hostIDs := make([]string, 0, hostsPerRound)
		for h := 0; h < hostsPerRound; h++ {
			hostIDs = append(hostIDs, fmt.Sprintf("order-%03d-%02d", i, h))
		}
		if err := client.enqueueExperimentFact(staleFact, false); err != nil {
			t.Fatalf("iteration %d: fact enqueue: %v", i, err)
		}
		for _, id := range hostIDs {
			if err := client.Enqueue(Event{ID: id, Name: "host_ordered"}); err != nil {
				t.Fatalf("iteration %d: enqueue %s: %v", i, id, err)
			}
		}
		var purgeDone sync.WaitGroup
		purgeDone.Add(1)
		go func() {
			defer purgeDone.Done()
			client.exp.mu.Lock()
			client.exp.applySentinelWithdrawalLocked("g17-3-scope", time.Now().UnixMilli())
			client.exp.mu.Unlock()
			for j := 0; j < purgesPerRound; j++ {
				client.purgeWithdrawnExperimentFacts()
			}
		}()
		purgeDone.Wait()
		waitFor(t, 10*time.Second, "the iteration's host events deliver", func() bool {
			capture.mu.Lock()
			defer capture.mu.Unlock()
			seen := 0
			for _, envelope := range capture.envelopes {
				for _, id := range hostIDs {
					if envelope["event_id"] == id {
						seen++
					}
				}
			}
			return seen == len(hostIDs)
		})
	}

	capture.mu.Lock()
	positions := make(map[string]int, iterations*hostsPerRound)
	for index, envelope := range capture.envelopes {
		if id, _ := envelope["event_id"].(string); id != "" {
			positions[id] = index
		}
	}
	capture.mu.Unlock()
	for i := 0; i < iterations; i++ {
		for h := 1; h < hostsPerRound; h++ {
			earlier := positions[fmt.Sprintf("order-%03d-%02d", i, h-1)]
			later := positions[fmt.Sprintf("order-%03d-%02d", i, h)]
			if earlier > later {
				t.Fatalf("iteration %d: the sentinel purge REORDERED unrelated host events that merely shared the queue (event %02d delivered at %d, event %02d at %d): the drain/re-enqueue filter let the worker receive a later event while an earlier keeper was held out of the channel", i, h-1, earlier, h, later)
			}
		}
	}
}
