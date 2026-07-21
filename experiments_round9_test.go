package shardpilot

// Round-9 review regression tests:
//
//  1. (P1) The drop-time capture path refuses anonymous-only experiment
//     facts under a user-scoped floor — the mirror of intake's actor gate;
//     the durable drop proceeds without capture (consent-first).
//  2. A failed withdrawal-marker spend reports as a failed save and keeps
//     the spool dirty, so the flush-cadence retry re-attempts the spend.
//  3. The close-remnant spool applies the consent-epoch boundary: stale
//     pre-denial members die by their intake stamp; fresh members spool.
//  4. The sentinel's spool sweep spares entries stamped at its own
//     post-bump epoch (the e.mu-only capture append's race partner).
//  5. Filtering a withdrawn fact keeps the surviving retained PREFIX bytes
//     even when the batch had appended tail members beyond the prefix.
//  6. A mid-cycle Retry-After: 0 pulls the pre-armed cadence deadline down
//     (retry NOW), completing the round-8 pull-down.
//  7. A refused session mints no subject state (structural pin for the
//     pre-mint consent re-check).
//  8. A fresh same-experiment install preserves an unlanded capture debt
//     (the frozen payload survives the shared intent key).
//  9. A withdrawal larger than the bounded settled-id memory still keeps
//     every withdrawn id out of the reload-and-merge save.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ── finding 1 (P1): capture refuses anonymous-only facts under user floor ───

func TestCaptureRefusesAnonymousOnlyFactUnderUserScopedFloor(t *testing.T) {
	script := &expScript{}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.SpoolDir = spoolDir
		cfg.UserID = "user-1"
		cfg.ConsentFloor = &ConsentFloorConfig{}
	})
	defer client.Close(context.Background())
	client.SetConsent(true)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("grant receipt flush: %v", err)
	}

	entry := &expEntry{
		VariantKey:     "treatment",
		Version:        1,
		AssignmentUnit: experimentAssignmentUnitClientID,
		SubjectFactKey: "sfk1_" + strings.Repeat("a", 64),
		SubjectKey:     "spcid_" + strings.Repeat("b", 32),
	}
	owed := []*expOwedExposure{{entry: entry, session: client.exp.sessionMarker}}
	ok, frozen := client.captureOwedExposuresForDrop("exp-cap", owed)
	if !ok || frozen != nil {
		t.Fatalf("a policy-refused capture is moot (the drop proceeds without it), got ok=%v frozen=%d", ok, len(frozen))
	}
	// The refused fact must have gained NO disk retention: the floor's
	// grant covers the user actor, and the fact ships anonymous-only.
	if data, err := os.ReadFile(filepath.Join(spoolDir, "spool.json")); err == nil {
		if strings.Contains(string(data), experimentExposureName) {
			t.Fatalf("an anonymous-only fact must not spool under a user-scoped floor, spool: %s", data)
		}
	}
}

// ── finding 2: marker spend failure is a failed save ────────────────────────

func TestFailedWithdrawalMarkerSpendKeepsSaveDirty(t *testing.T) {
	dir := t.TempDir()
	s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: 100, SpoolMaxBytes: 1 << 20})
	now := time.Now()
	entry := spoolEntry{id: "fact-marker", ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw("fact-marker"), internalFact: true}
	if refused, added, _, _, _ := s.append([]spoolEntry{entry}, 0, false, now, func() bool { return true }); refused || len(added) != 1 {
		t.Fatalf("test setup: append failed")
	}
	s.mu.Lock()
	s.removeFn = func(string) error { return errors.New("marker busy") }
	s.mu.Unlock()
	removed, persistFailed := s.removeMatching(withdrawnExperimentFactRaw, 1)
	if len(removed) != 1 {
		t.Fatalf("test setup: withdrawal removed %d", len(removed))
	}
	if !persistFailed {
		t.Fatalf("a failed marker spend must report as a failed save — a silently surviving marker condemns a same-id fresh fact at the next load")
	}
	if _, err := os.Stat(spoolWithdrawnPath(dir)); err != nil {
		t.Fatalf("test invariant: the unspent marker file must still exist: %v", err)
	}
	// Storage heals: the flush-cadence retry spends the marker.
	s.mu.Lock()
	s.removeFn = os.Remove
	s.mu.Unlock()
	if attempted, failed := s.retryPersist(); !attempted || failed {
		t.Fatalf("the dirty spool must retry and succeed, attempted=%v failed=%v", attempted, failed)
	}
	if _, err := os.Stat(spoolWithdrawnPath(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the retried save must spend the marker, stat err=%v", err)
	}
}

// ── finding 3: close remnant settles the consent epoch ──────────────────────

func TestCloseRemnantDropsPreDenialEvents(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.SpoolDir = spoolDir
		cfg.ExperimentsEnabled = false
	})
	client.SetConsent(true)
	// The remnant path is worker-owned: stop the worker first.
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A denial bumped the epoch while the worker held its batch; the
	// re-grant enqueued a fresh event; Close's abandoned flush never let
	// the worker observe the boundary.
	client.consentEpoch.Add(1)
	client.queue.enqueue(Event{ID: "fresh-1", Name: "fresh_post_grant", AnonymousID: "anon-test", intakeConsentEpoch: 1})
	held := []Event{{ID: "stale-1", Name: "stale_pre_denial", AnonymousID: "anon-test", intakeConsentEpoch: 0}}
	droppedBefore := client.Snapshot().Dropped
	client.spoolCloseRemnant(held)

	data, err := os.ReadFile(filepath.Join(spoolDir, "spool.json"))
	if err != nil {
		t.Fatalf("read spool: %v", err)
	}
	if !strings.Contains(string(data), "fresh_post_grant") {
		t.Fatalf("the post-grant remnant member must spool, got %s", data)
	}
	if strings.Contains(string(data), "stale_pre_denial") {
		t.Fatalf("a pre-denial member must never gain durable retention at close, got %s", data)
	}
	if dropped := client.Snapshot().Dropped - droppedBefore; dropped != 1 {
		t.Fatalf("the condemned member counts exactly once, got %d", dropped)
	}
}

// ── finding 4: the sweep spares current-epoch entries ───────────────────────

func TestSentinelSweepSparesEpochStampedCapture(t *testing.T) {
	dir := t.TempDir()
	s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: 100, SpoolMaxBytes: 1 << 20})
	now := time.Now()
	stale := spoolEntry{id: "fact-stale", ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw("fact-stale"), expFactEpoch: 1, internalFact: true}
	fresh := spoolEntry{id: "fact-fresh", ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw("fact-fresh"), expFactEpoch: 2, internalFact: true}
	if refused, added, _, _, _ := s.append([]spoolEntry{stale, fresh}, 0, false, now, func() bool { return true }); refused || len(added) != 2 {
		t.Fatalf("test setup: append failed")
	}
	removed, persistFailed := s.removeMatching(withdrawnExperimentFactRaw, 2)
	if persistFailed {
		t.Fatalf("save failed unexpectedly")
	}
	if len(removed) != 1 || removed[0].id != "fact-stale" {
		t.Fatalf("the sweep must withdraw only entries predating its epoch, removed %v", removed)
	}
	data, err := os.ReadFile(filepath.Join(dir, "spool.json"))
	if err != nil {
		t.Fatalf("read spool: %v", err)
	}
	if !strings.Contains(string(data), "fact-fresh") || strings.Contains(string(data), "fact-stale") {
		t.Fatalf("the current-epoch capture must survive the sweep, got %s", data)
	}
}

// ── finding 5: retained prefix survives the withdrawn filter ────────────────

func TestWithdrawnFilterKeepsRetainedPrefixWithAppendedTail(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
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
	factEvent, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-prefix", entry, "", client.exp.sessionMarker, client.expFactPurgeEpoch.Load())
	if skip != "" {
		t.Fatalf("fact build refused (%s)", skip)
	}
	host1 := Event{ID: "host-1", Name: "host_kept", AnonymousID: "anon-test"}
	host2 := Event{ID: "host-2", Name: "host_late", AnonymousID: "anon-test"}
	retained, err := client.buildBatch([]Event{factEvent, host1})
	if err != nil {
		t.Fatalf("buildBatch: %v", err)
	}
	host1Raw := string(retained.rawEvents[1])
	client.retainedRequest = retained

	// A later queued member joined the batch past the retained prefix, then
	// the sentinel withdrew the fact.
	batch := []Event{factEvent, host1, host2}
	backoff := 5
	client.expFactPurgeEpoch.Add(1)
	kept := client.dropWithdrawnExperimentFacts(batch, &backoff)
	if len(kept) != 2 {
		t.Fatalf("expected the two host events kept, got %d", len(kept))
	}
	if len(client.retainedRequest.Events) != 1 || string(client.retainedRequest.rawEvents[0]) != host1Raw {
		t.Fatalf("the surviving retained PREFIX must keep its exact bytes (len=%d) — a wholesale clear re-marshals it and breaks byte identity", len(client.retainedRequest.Events))
	}
	request, _, poisoned := client.buildBatchIsolating(kept, client.retainedRequest)
	if len(poisoned) != 0 || len(request.rawEvents) != 2 || string(request.rawEvents[0]) != host1Raw {
		t.Fatalf("the rebuild must reuse the surviving prefix bytes verbatim")
	}
}

// ── finding 6: present-zero pulls the cadence down ──────────────────────────

func TestZeroRetryAfterMidCycleRevalidatesImmediately(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.pushRetryAfter(429, ``, "0")
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	clock := &expFakeClock{now: time.Now()}
	client := newExperimentClient(t, server.URL, nil)
	client.clock = clock
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	clock.advance(331 * time.Second)
	client.experimentCycle(context.Background())
	if script.requestCount() != 2 {
		t.Fatalf("the due cycle must dispatch the revalidation, got %d", script.requestCount())
	}
	// The server said retry NOW (Retry-After: 0): the next lane tick must
	// re-dispatch instead of sitting out the pre-armed ~300s interval.
	clock.advance(1 * time.Second)
	client.experimentCycle(context.Background())
	if script.requestCount() != 3 {
		t.Fatalf("a present-zero hint must release the cadence immediately, got %d requests", script.requestCount())
	}
}

// ── finding 7: refused sessions mint no subject state (structural pin) ──────

func TestDeniedFetchMintsNoSubjectState(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	client.SetConsent(false)
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected the refusal, got %v", err)
	}
	client.exp.mu.Lock()
	subject, memory := client.exp.subjectID, client.exp.memorySubjectID
	client.exp.mu.Unlock()
	if subject != "" || memory != "" {
		t.Fatalf("a refused session must mint no subject state, got %q / %q", subject, memory)
	}
	if _, err := os.Stat(filepath.Join(spoolDir, expSubjectFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("no subject file may exist for a refused session, stat err=%v", err)
	}
}

// ── finding 8: reinstall preserves the unlanded capture debt ────────────────

func TestReinstallPreservesUnlandedCaptureDebt(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(404, ``)
	script.push(200, expAssignedBody("2"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.SpoolDir = spoolDir
		cfg.BatchSize = 1
		cfg.BufferSize = 1
	})
	// A FROZEN clock forces the kill's asOf and the reinstall's stamp into
	// the same millisecond — the collision a fast runner hits with the
	// real clock — so the pending-intent stamp raise is exercised
	// deterministically on every runtime.
	client.clock = &expFakeClock{now: time.Now()}
	defer func() {
		capture.setStatus(http.StatusAccepted)
		_ = client.Close(context.Background())
	}()
	client.SetConsent(true)
	parkWorkerWithFullQueue(t, client, capture)
	fetchAssignment(t, client, expTestScopeKey) // owed exposure (queue full)

	// The spool cannot persist: the kill's capture freezes in the pair.
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
	client.exp.mu.Lock()
	pairs := 0
	for _, pending := range client.exp.durablePending {
		if pending.captureFirst && len(pending.captureEntries) > 0 {
			pairs++
		}
	}
	client.exp.mu.Unlock()
	if pairs != 1 {
		t.Fatalf("test setup: expected the armed capture pair, got %d", pairs)
	}

	// A FRESH assignment for the same experiment installs (the record file
	// writes fine — only the spool is broken): the shared (scope,
	// experiment) intent key must not clear the unlanded capture debt.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err != nil {
		t.Fatalf("reinstall fetch: %v", err)
	}
	client.exp.mu.Lock()
	var carried expOwedSync
	pairs = 0
	for _, pending := range client.exp.durablePending {
		if pending.captureFirst && len(pending.captureEntries) > 0 {
			carried = pending
			pairs++
		}
	}
	client.exp.mu.Unlock()
	if pairs != 1 {
		t.Fatalf("the reinstall cleared the unlanded capture debt — the old owed exposure lost its only durable replay source (pairs=%d)", pairs)
	}
	if len(carried.captureEntries) == 0 {
		t.Fatalf("the carried debt must keep its frozen payload")
	}

	// Storage heals: the frozen payload lands, and the fresh assignment's
	// record survives the pair's outranked delete half.
	client.spool.mu.Lock()
	client.spool.renameFn = os.Rename
	client.spool.mu.Unlock()
	client.exp.retryDurableSync()
	record, err := os.ReadFile(filepath.Join(spoolDir, expCacheFileName))
	if err != nil || !strings.Contains(string(record), expTestScopeKey) {
		t.Fatalf("the fresh install must survive the settled pair (err=%v)", err)
	}
	client.exp.mu.Lock()
	remaining := len(client.exp.durablePending)
	client.exp.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("the pair must settle once the capture lands, %d intents remain", remaining)
	}
	spool, err := os.ReadFile(filepath.Join(spoolDir, "spool.json"))
	if err != nil || !strings.Contains(string(spool), `"experiment_version":1`) {
		t.Fatalf("the frozen v1 exposure must be durably captured (err=%v)", err)
	}
}

// ── finding 9: withdrawals larger than the settled memory ───────────────────

func TestWithdrawalMergeExcludesBeyondSettledMemory(t *testing.T) {
	dir := t.TempDir()
	total := spoolSettledMemory + 4
	s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: total + 10, SpoolMaxBytes: 32 << 20})
	now := time.Now()
	entries := make([]spoolEntry, 0, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("fact-%05d", i)
		entries = append(entries, spoolEntry{id: id, ts: now.UTC().Format(time.RFC3339Nano), raw: round5FactRaw(id), internalFact: true})
	}
	if refused, added, _, _, _ := s.append(entries, 0, false, now, func() bool { return true }); refused || len(added) != total {
		t.Fatalf("test setup: append failed (added %d)", len(added))
	}
	removed, persistFailed := s.removeMatching(withdrawnExperimentFactRaw, 1)
	if len(removed) != total || persistFailed {
		t.Fatalf("withdrawal removed %d (persistFailed=%v)", len(removed), persistFailed)
	}
	data, err := os.ReadFile(filepath.Join(dir, "spool.json"))
	if err != nil {
		t.Fatalf("read spool: %v", err)
	}
	if strings.Contains(string(data), experimentExposureName) {
		t.Fatalf("the merge resurrected withdrawn facts evicted from the bounded settled memory (the marker's full set must back the exclusion)")
	}
	if _, err := os.Stat(spoolWithdrawnPath(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the marker must spend with the clean rewrite, stat err=%v", err)
	}
}
