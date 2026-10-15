package shardpilot

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ── regressions, part 7 ─────────────────────────────────────────────────────

// Finding 1: the capture gate releases only when the FROZEN payload is
// durably spooled — the live sweep emptying the owed snapshots into the
// volatile queue is not durability, and pre-fix the retry released the gate
// on exactly that (len(owed)==0) and deleted the record's entry with the
// fact still queue-resident.
func TestCaptureGateHoldsUntilDurable(t *testing.T) {
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
	failApplicationHandoffStorage(client)
	fetchAndApply(t, client, expTestScopeKey) // owed exposure (queue full)
	if client.Snapshot().SpoolPersistFailed == 0 {
		t.Fatal("the actual sealed handoff must reach its storage failure")
	}

	// The handoff's disk fault also refuses the drop-time capture.
	if result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err != nil || result.Code != "not_found" {
		t.Fatalf("the kill fetch must land not_found, got %+v err=%v", result, err)
	}

	// Analytics recovers and frees the queue, but the failed durable
	// handoff still holds the owed snapshot until storage also recovers.
	capture.setStatus(http.StatusAccepted)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	client.sweepExperimentExposures(expTestScopeKey)
	if owed := client.owedExperimentExposureCount(); owed != 1 {
		t.Fatalf("the failed durable handoff must keep its snapshot owed, got %d", owed)
	}

	// The retry with the spool still broken must NOT release the gate: the
	// record keeps the entry.
	client.exp.retryDurableSync()
	if !durableRecordHolds(t, spoolDir, expTestScopeKey) {
		t.Fatalf("the record must keep the entry until the FROZEN capture is durable")
	}

	// Storage heals: the frozen payload lands, and only then can the
	// retained assignment record converge.
	client.spool.mu.Lock()
	client.spool.renameFn = os.Rename
	client.spool.appendFn = appendPrivateFile
	client.spool.mu.Unlock()
	client.exp.retryDurableSync()
	if durableRecordHolds(t, spoolDir, expTestScopeKey) {
		t.Fatalf("the record must converge once the pair lands")
	}
}

// Finding 2 (pin): the close-remnant spool is a transport handoff — a
// withdrawn fact held in the worker's batch when the client closes
// mid-outage must not be spooled for the next launch to resend. (The
// dispatch-boundary filter already caught every in-process sequence we
// could stage — the close flush's loop-top filter runs before the remnant
// — so this pins the remnant gate structurally rather than regressing a
// reachable pre-fix loss.)
func TestCloseRemnantFiltersWithdrawnFacts(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(403, `{"error":{"code":"`+expSentinelRealSubjectsDisabled+`"}}`)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.SpoolDir = spoolDir
		cfg.BatchSize = 1
		cfg.BufferSize = 4
	})
	client.SetConsent(true)

	capture.setStatus(http.StatusInternalServerError)
	fetchAndApply(t, client, expTestScopeKey) // fact -> queue -> worker batch
	waitFor(t, 5*time.Second, "the worker parks holding the fact", func() bool { return capture.hitCount() >= 1 })
	if err := client.Enqueue(Event{Name: "host_close_survivor"}); err != nil {
		t.Fatalf("host event: %v", err)
	}
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil {
		t.Fatalf("the sentinel fetch must fail closed")
	}

	// Close with the ingest still down: the remnant spools — minus the
	// withdrawn fact.
	_ = client.Close(context.Background())
	data, err := os.ReadFile(filepath.Join(spoolDir, spoolFileName))
	if err != nil {
		t.Fatalf("the close remnant must spool the host event: %v", err)
	}
	if strings.Contains(string(data), experimentExposureName) {
		t.Fatalf("a withdrawn fact must not ride the close-remnant spool")
	}
	if !strings.Contains(string(data), "host_close_survivor") {
		t.Fatalf("the host event must survive the remnant filter")
	}
}

// Finding 3: withdrawal is REGISTRATION-scoped — a fresh post-purge fact
// (a new authorized assignment after the platform re-enabled real subjects)
// carries the current purge epoch and must never be dropped by a worker's
// lagging batch filter.
func TestPostPurgeFreshExposureSurvivesFilter(t *testing.T) {
	script := &expScript{}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	// The filter is worker-owned: stop the worker so this goroutine is its
	// sole accessor.
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
	stale, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-stale", entry, "", client.exp.sessionMarker, client.expFactPurgeEpoch.Load())
	if skip != "" {
		t.Fatalf("test setup: fact build refused (%s)", skip)
	}
	client.expFactPurgeEpoch.Add(1) // the sentinel lands
	fresh, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-fresh", entry, "", client.exp.sessionMarker, client.expFactPurgeEpoch.Load())
	if skip != "" {
		t.Fatalf("test setup: fact build refused (%s)", skip)
	}
	backoff := 0
	kept := client.dropWithdrawnExperimentFacts([]Event{stale, fresh}, &backoff)
	if len(kept) != 1 {
		t.Fatalf("exactly the stale fact must be withdrawn, got %v", kept)
	}
	keptKey, _ := kept[0].Props["experiment_key"].(string)
	if keptKey != "exp-fresh" {
		t.Fatalf("the fresh post-purge fact must survive, got %q", keptKey)
	}
}
