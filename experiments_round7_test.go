package shardpilot

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ── review round 7 ──────────────────────────────────────────────────────────

// Finding 1 (P1): the capture gate releases only when the FROZEN payload is
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
	fetchAssignment(t, client, expTestScopeKey) // owed exposure (queue full)

	// The spool's record rewrite fails: the capture cannot land.
	client.spool.mu.Lock()
	client.spool.renameFn = func(oldpath, newpath string) error {
		if strings.HasSuffix(newpath, spoolFileName) {
			return errors.New("disk full")
		}
		return os.Rename(oldpath, newpath)
	}
	client.spool.mu.Unlock()
	if result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err != nil || result.Code != "not_found" {
		t.Fatalf("the kill fetch must land not_found, got %+v err=%v", result, err)
	}

	// The live sweep drains the owed snapshot into the QUEUE (the ingest
	// recovers and the flush frees room) — volatile residency only.
	capture.setStatus(http.StatusAccepted)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	client.sweepExperimentExposures(expTestScopeKey)
	if owed := client.owedExperimentExposureCount(); owed != 0 {
		t.Fatalf("test setup: the sweep must empty the owed snapshots, got %d", owed)
	}

	// The retry with the spool still broken must NOT release the gate: the
	// record keeps the entry.
	client.exp.retryDurableSync()
	record, err := os.ReadFile(filepath.Join(spoolDir, expCacheFileName))
	if err != nil || !strings.Contains(string(record), expTestScopeKey) {
		t.Fatalf("the record must keep the entry until the FROZEN capture is durable (err=%v)", err)
	}

	// Storage heals: the frozen payload lands (or settles as already
	// DELIVERED — the live copy was published and acked above, and a
	// delivered fact outranks a durable-pending one; the spool settles the
	// same-id entry by design) and the record converges.
	client.spool.mu.Lock()
	client.spool.renameFn = os.Rename
	client.spool.mu.Unlock()
	client.exp.retryDurableSync()
	record, err = os.ReadFile(filepath.Join(spoolDir, expCacheFileName))
	if err != nil || strings.Contains(string(record), expTestScopeKey) {
		t.Fatalf("the record must converge once the pair lands (err=%v)", err)
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
	script.push(403, `{"error":"`+expSentinelRealSubjectsDisabled+`"}`)
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
	fetchAssignment(t, client, expTestScopeKey) // fact -> queue -> worker batch
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
	stale, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-stale", entry, "", client.exp.sessionMarker)
	if skip != "" {
		t.Fatalf("test setup: fact build refused (%s)", skip)
	}
	client.expFactPurgeEpoch.Add(1) // the sentinel lands
	fresh, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-fresh", entry, "", client.exp.sessionMarker)
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
