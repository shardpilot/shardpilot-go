package shardpilot

// Review round 12 — regression pins. Each test fails on the pre-fix tree
// for its finding's exact reason (verified mechanically via targeted
// temporary reverts of the fix, with the test seams retained).

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ── G12-1: owed exposures survive a consent purge under an auth latch ───────

func TestOwedExposureSurvivesAuthLatchedConsentPurge(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(401, ``)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.BatchSize = 1
		cfg.BufferSize = 1
	})
	defer func() {
		capture.setStatus(http.StatusAccepted)
		_ = client.Close(context.Background())
	}()
	client.SetConsent(true)

	// The assignment installs while the analytics queue is FULL: the
	// exposure fact cannot enqueue and stays owed as a pending snapshot.
	parkWorkerWithFullQueue(t, client, capture)
	if result := fetchAssignment(t, client, expTestScopeKey); result.Version != 1 {
		t.Fatalf("setup fetch: %+v", result)
	}
	client.exp.mu.Lock()
	pendingBefore := len(client.exp.pendingExposure[expTestScopeKey])
	subject := client.exp.currentSubjectIDLocked()
	marker := client.exp.sessionMarker
	client.exp.mu.Unlock()
	if pendingBefore == 0 {
		t.Fatalf("test shape: the exposure must be owed (queue full), pendingExposure empty")
	}

	// An ordinary 401 latches: memory serving clears, the durable record —
	// and the treatment's owed exposure — are intentionally RETAINED.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil ||
		!strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("the 401 fetch must fail closed, got %v", err)
	}

	// The denial purges while the latch is active; the re-grant reopens
	// the plane.
	client.SetConsent(false)
	client.SetConsent(true)
	capture.setStatus(http.StatusAccepted)

	// The retained treatment's exposure must have re-armed at the purge:
	// the sweep emits it and the flush delivers it.
	client.sweepAllExperimentExposures()
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	wantID := experimentExposureEventID(marker, subject, expTestScopeKey, 1, 0)
	delivered := false
	for _, exposure := range capture.exposures() {
		if id, _ := exposure["event_id"].(string); id == wantID {
			delivered = true
		}
	}
	if !delivered {
		t.Fatalf("the latch-retained treatment's exposure was silently lost across the consent purge: nothing re-armed for the re-granted session to sweep (the latch cleared e.entries, so the entries-only re-arm pass missed the RETAINED assignment)")
	}
}

// ── G12-2: full spool purges spend the stale withdrawal marker ──────────────

func TestPurgeSpendsStaleWithdrawalMarker(t *testing.T) {
	writeMarker := func(t *testing.T, dir string, ids []string) {
		t.Helper()
		payload, err := json.Marshal(ids)
		if err != nil {
			t.Fatalf("marshal marker: %v", err)
		}
		if err := os.WriteFile(spoolWithdrawnPath(dir), payload, 0o600); err != nil {
			t.Fatalf("write marker: %v", err)
		}
	}

	t.Run("purge_spends_marker_durably", func(t *testing.T) {
		dir := t.TempDir()
		s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: 100, SpoolMaxBytes: 1 << 20})
		writeMarker(t, dir, []string{"fact-stale"})
		var ops []string
		s.mu.Lock()
		s.removeFn = func(path string) error { ops = append(ops, "rm:"+filepath.Base(path)); return os.Remove(path) }
		s.syncFn = func(string) error { ops = append(ops, "sync"); return syncDir(dir) }
		s.mu.Unlock()
		if _, err := s.purge(); err != nil {
			t.Fatalf("purge: %v", err)
		}
		if _, err := os.Stat(spoolWithdrawnPath(dir)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the full purge must spend the stale withdrawal marker — left on disk, a later granted restart honors it against fresh facts (stat err=%v)", err)
		}
		joined := strings.Join(ops, ",")
		markerAt := strings.Index(joined, "rm:"+spoolWithdrawnFileName)
		if markerAt < 0 {
			t.Fatalf("the purge must unlink the withdrawal marker, ops: %s", joined)
		}
		if !strings.Contains(joined[markerAt:], "sync") {
			t.Fatalf("the marker's unlink must be followed by a parent-directory sync before the purge reports complete (the durable-unlink discipline), ops: %s", joined)
		}
	})

	t.Run("failed_spend_fails_closed_onto_wipe_rung", func(t *testing.T) {
		dir := t.TempDir()
		s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: 100, SpoolMaxBytes: 1 << 20})
		writeMarker(t, dir, []string{"fact-stale"})
		syncHealthy := false
		s.mu.Lock()
		s.syncFn = func(string) error {
			if !syncHealthy {
				return errors.New("dir sync refused")
			}
			return syncDir(dir)
		}
		s.mu.Unlock()
		if _, err := s.purge(); err == nil {
			t.Fatalf("a marker spend that could not be made durable must not report the purge complete")
		}
		if !s.owedWipe() {
			t.Fatalf("the failed spend must fail closed onto the owed-wipe rung (disk work refused until the wipe settles durably)")
		}
		// Storage heals: the wipe settles, removing both markers durably.
		syncHealthy = true
		if !s.settleOwedWipe() {
			t.Fatalf("the healed wipe must settle")
		}
		if _, err := os.Stat(spoolWithdrawnPath(dir)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the settled wipe must leave no withdrawal marker (stat err=%v)", err)
		}
	})

	t.Run("granted_restart_spools_fresh_same_id_uncondemned", func(t *testing.T) {
		dir := t.TempDir()
		cfg := Config{SpoolDir: dir, SpoolMaxEvents: 100, SpoolMaxBytes: 1 << 20}
		// A previous withdrawal left the marker; the next start has no
		// persisted grant and purges.
		writeMarker(t, dir, []string{"fact-recur"})
		s := newDiskSpool(cfg)
		if _, err := s.purge(); err != nil {
			t.Fatalf("purge: %v", err)
		}
		// A later granted session spools a FRESH fact that legitimately
		// reuses the id (deterministic event ids recur by design).
		now := time.Now()
		fresh := spoolEntry{id: "fact-recur", ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw("fact-recur"), internalFact: true}
		if refused, added, _, _, persistFailed := s.append([]spoolEntry{fresh}, 0, false, now, func() bool { return true }); refused || len(added) != 1 || persistFailed {
			t.Fatalf("the reopened spool must admit and persist the fresh fact (refused=%v added=%d persistFailed=%v)", refused, len(added), persistFailed)
		}
		// The granted restart reloads it — the stale marker must not
		// condemn it.
		restarted := newDiskSpool(cfg)
		restarted.load(time.Now())
		restarted.mu.Lock()
		loaded := len(restarted.entries)
		restarted.mu.Unlock()
		if loaded != 1 {
			t.Fatalf("the stale withdrawal marker survived the purge and condemned a fresh same-id fact at the granted restart (loaded %d of 1)", loaded)
		}
	})
}
