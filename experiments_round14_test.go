package shardpilot

// Review round 14 — regression pin. Fails on the pre-fix tree for the
// finding's exact reason (verified mechanically via a targeted temporary
// revert of the fix).

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ── G14-1: the close remnant filters facts drained past the seen epoch ──────

func TestCloseRemnantFiltersFactsDrainedPastSeenEpoch(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	// The spool handoff is grant-gated: persist the grant first, then stop
	// the real worker — the emulation IS the stop path.
	client.SetConsent(true)
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
	// Built BEFORE the sentinel: the fact carries the pre-sentinel stamp.
	factEvent, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-remnant", entry, "remnant-fact", client.exp.sessionMarker, client.expFactPurgeEpoch.Load())
	if skip != "" {
		t.Fatalf("fact build refused (%s)", skip)
	}
	if !client.queue.enqueue(factEvent) {
		t.Fatalf("enqueue refused")
	}

	// The sentinel's decisive bump lands, and the worker's seen mark has
	// ALREADY advanced (a dispatch point observed the move against its
	// held batch before any queue drain) — the finding's exact window.
	client.exp.mu.Lock()
	client.exp.factPurgeEpochBumpFn()
	client.exp.mu.Unlock()
	seenBackoff := 0
	_ = client.dropWithdrawnExperimentFacts(nil, &seenBackoff)

	// The stop path drains the queue and hands the remnant to the spool.
	droppedBefore := client.Snapshot().Dropped
	client.spoolCloseRemnant(nil)

	spoolPath := filepath.Join(spoolDir, "spool.json")
	if data, err := os.ReadFile(spoolPath); err == nil && strings.Contains(string(data), "remnant-fact") {
		t.Fatalf("the close remnant spooled a WITHDRAWN pre-sentinel fact: the seen-epoch gate short-circuited on the drained member, it was rebuilt under the current request epoch past spoolFailedBatch's re-filter, and its subject-fact key is durably on disk (spool: %s)", data)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read spool: %v", err)
	}
	if delta := client.Snapshot().Dropped - droppedBefore; delta != 1 {
		t.Fatalf("the withdrawn remnant member must count exactly once, got %d", delta)
	}

	// Simulated crash before any sentinel spool sweep: the next launch
	// must not resurrect the withdrawn subject-fact key.
	restarted := newDiskSpool(Config{SpoolDir: spoolDir, SpoolMaxEvents: 100, SpoolMaxBytes: 1 << 20})
	restarted.load(time.Now())
	restarted.mu.Lock()
	loaded := len(restarted.entries)
	restarted.mu.Unlock()
	if loaded != 0 {
		t.Fatalf("the launch after the crash resurrected %d withdrawn fact(s) from the spool", loaded)
	}
}
