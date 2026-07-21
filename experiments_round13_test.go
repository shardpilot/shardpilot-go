package shardpilot

// Review round 13 — regression pin. Fails on the pre-fix tree for the
// finding's exact reason (verified mechanically via a targeted temporary
// revert of the fix).

import (
	"context"
	"strings"
	"testing"
	"time"
)

// ── G13-1: unlatching restores the retained assignments ─────────────────────

func TestUnlatchRestoresRetainedAssignments(t *testing.T) {
	const otherKey = "exp-latched-b"
	// Assigned body for the second experiment: echo fields absent
	// (tolerated), distinct variant so the getters' source is unambiguous.
	otherBody := `{"assigned":true,"version":1,"assignment_key":"asgn_b","variant_key":"treatment-b",` +
		`"subject_fact_key":"sfk1_` + strings.Repeat("b", 64) + `",` +
		`"boundary":{"assignment_unit":"client_id"}}`

	script := &expScript{}
	script.push(200, expAssignedBody("1")) // install A
	script.push(200, otherBody)            // install B
	script.push(401, ``)                   // latch (fetch for A)
	script.push(200, expAssignedBody("1")) // authorized fetch for A: unlatches
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	clock := &expFakeClock{now: time.Now()}
	client := newExperimentClient(t, server.URL, nil)
	client.clock = clock
	defer client.Close(context.Background())
	client.SetConsent(true)

	if result := fetchAssignment(t, client, expTestScopeKey); result.Version != 1 {
		t.Fatalf("setup fetch A: %+v", result)
	}
	if result, err := client.FetchExperimentAssignment(context.Background(), otherKey, nil); err != nil || result.VariantKey != "treatment-b" {
		t.Fatalf("setup fetch B: %+v err=%v", result, err)
	}

	// An ordinary 401 latches: both entries leave memory, the assignments
	// are intentionally RETAINED.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil ||
		!strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("the 401 fetch must fail closed, got %v", err)
	}
	if variant := client.ExperimentVariant(otherKey); variant != "" {
		t.Fatalf("test shape: the latch must stop serving, got %q", variant)
	}

	// A later host fetch for A alone succeeds and unlatches.
	if result := fetchAssignment(t, client, expTestScopeKey); result.Version != 1 {
		t.Fatalf("unlatch fetch A: %+v", result)
	}

	// The OTHER retained assignment must serve again — the durable cache
	// was retained across the latch, and the unlatch must not strand it
	// until a process restart.
	if variant := client.ExperimentVariant(otherKey); variant != "treatment-b" {
		t.Fatalf("the unlatch restored only the fetched experiment: the other RETAINED assignment stays unserved (ExperimentVariant %q, want %q)", variant, "treatment-b")
	}

	// And the revalidation lane must probe it again: the lane iterates
	// e.entries, so a stranded assignment would never be revalidated (kill
	// reach lost) until a restart.
	requestsBefore := script.requestCount()
	clock.advance(331 * time.Second)
	client.experimentCycle(context.Background())
	probedOther := false
	for i := requestsBefore; i < script.requestCount(); i++ {
		if strings.Contains(script.request(i).URL.RawQuery, "experiment_key="+otherKey) {
			probedOther = true
		}
	}
	if !probedOther {
		t.Fatalf("the revalidation lane never probed the retained assignment after the unlatch (requests %d..%d) — stranded outside e.entries, it gets no kill-switch reach until a process restart", requestsBefore, script.requestCount()-1)
	}
}
