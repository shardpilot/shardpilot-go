package shardpilot

// Review round 11 — regression pins. Each test fails on the pre-fix tree
// for its finding's exact reason (verified mechanically via targeted
// temporary reverts of the fix, with the test seams retained).

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── GF1: destructive verdicts process through consent flips that fully
// precede the settle (and, GF-P1 parity, through a deny → re-grant that
// completed across the flight) ──────────────────────────────────────────────

func TestDenialBeforeSettleStillLandsDestructiveVerdicts(t *testing.T) {
	for _, flavor := range []struct {
		name string
		flip func(client *Client)
	}{
		{"denial_lands_before_settle", func(client *Client) {
			// The denial's fast half completed in full — state flipped AND
			// epoch bumped — before the settle's first consent read.
			client.consent.Store(consentStateDenied)
			client.consentEpoch.Add(1)
		}},
		{"deny_regrant_completes_before_settle", func(client *Client) {
			// The full deny → re-grant round trip completed: the CURRENT
			// state admits again, and only the moved denial epoch records
			// that the response crossed a closed interval.
			client.consent.Store(consentStateDenied)
			client.consentEpoch.Add(1)
			client.consent.Store(consentStateGranted)
		}},
	} {
		t.Run(flavor.name, func(t *testing.T) {
			script := &expScript{}
			script.push(200, expAssignedBody("1"))
			script.push(200, `{"assigned":false,"reason":"kill_switch","version":2}`)
			capture := &expWireCapture{}
			server := newExperimentServer(t, script, capture)
			defer server.Close()
			spoolDir := t.TempDir()
			client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
			defer client.Close(context.Background())
			client.SetConsent(true)
			if result := fetchAssignment(t, client, expTestScopeKey); result.Version != 1 {
				t.Fatalf("setup fetch: %+v", result)
			}

			client.exp.mu.Lock()
			client.exp.consentRaceSeam = func(stage string) {
				if stage == "settle_entry" {
					flavor.flip(client)
				}
			}
			client.exp.mu.Unlock()
			_, fetchErr := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
			client.exp.mu.Lock()
			client.exp.consentRaceSeam = nil
			entry := client.exp.entries[expTestScopeKey]
			client.exp.mu.Unlock()
			if entry != nil {
				t.Fatalf("a denial in hand before the settle must not discard the kill_switch verdict wholesale: the cache is retained across denial by design, so the dropped withdrawal would re-serve the killed assignment after a re-grant, got %+v", entry)
			}
			if !errors.Is(fetchErr, ErrConsentDenied) {
				t.Fatalf("the caller receives the refusal the interrupting denial owed it, got %v", fetchErr)
			}
			record, err := os.ReadFile(filepath.Join(spoolDir, expCacheFileName))
			if err != nil {
				t.Fatalf("read cache: %v", err)
			}
			if strings.Contains(string(record), "asgn_abc") {
				t.Fatalf("the server-directed withdrawal must land on the DURABLE record too, got %s", record)
			}
			// A (re-)grant must serve nothing: the server killed the
			// assignment while consent was interrupted.
			client.SetConsent(true)
			if variant := client.ExperimentVariant(expTestScopeKey); variant != "" {
				t.Fatalf("the killed assignment must not re-serve after the re-grant, got %q", variant)
			}
		})
	}
}

// ── GF-P1 (defold R23 parity): a deny → re-grant completing across the
// flight discards the CONSTRUCTIVE install even though the current state
// admits again ──────────────────────────────────────────────────────────────

func TestDenyRegrantAcrossFlightDiscardsConstructiveInstall(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, expAssignedBody("2"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())
	client.SetConsent(true)
	if result := fetchAssignment(t, client, expTestScopeKey); result.Version != 1 {
		t.Fatalf("setup fetch: %+v", result)
	}

	// The deny → re-grant round trip completes before the settle: the
	// current-state re-check alone would pass it, and the pre-revocation
	// 200 would install constructively — an assignment fetched across the
	// revoked interval.
	client.exp.mu.Lock()
	client.exp.consentRaceSeam = func(stage string) {
		if stage == "settle_entry" {
			client.consent.Store(consentStateDenied)
			client.consentEpoch.Add(1)
			client.consent.Store(consentStateGranted)
		}
	}
	client.exp.mu.Unlock()
	_, fetchErr := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
	client.exp.mu.Lock()
	client.exp.consentRaceSeam = nil
	entry := client.exp.entries[expTestScopeKey]
	armedV2 := false
	for _, owed := range client.exp.pendingExposure[expTestScopeKey] {
		if owed.entry.Version == 2 {
			armedV2 = true
		}
	}
	client.exp.mu.Unlock()
	if entry == nil || entry.Version != 1 {
		t.Fatalf("the consent-interval fence must discard the constructive install: the settle compares the dispatch-time denial epoch, not just the current state, got %+v", entry)
	}
	if armedV2 {
		t.Fatalf("the discarded install must not arm exposure debt for the interval-crossing response")
	}
	if !errors.Is(fetchErr, ErrConsentDenied) {
		t.Fatalf("the caller receives the refusal the interrupting denial owed it (the gate-abort precedent), got %v", fetchErr)
	}
	record, err := os.ReadFile(filepath.Join(spoolDir, expCacheFileName))
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	if strings.Contains(string(record), `"version":2`) {
		t.Fatalf("the discarded install must not reach the durable record, got %s", record)
	}
	// The retained v1 keeps serving under the re-granted plane.
	if variant := client.ExperimentVariant(expTestScopeKey); variant != "treatment" {
		t.Fatalf("the retained v1 assignment must keep serving, got %q", variant)
	}
}

// ── GF2: the mint undo owns a partially persisted subject file ──────────────

func TestUnmintOwnsPartiallyPersistedSubjectFile(t *testing.T) {
	t.Run("failed_write_still_undoes_durably", func(t *testing.T) {
		script := &expScript{}
		script.push(200, expAssignedBody("1"))
		capture := &expWireCapture{}
		server := newExperimentServer(t, script, capture)
		defer server.Close()
		spoolDir := t.TempDir()
		client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
		defer client.Close(context.Background())
		client.SetConsent(true)

		subjectPath := filepath.Join(spoolDir, expSubjectFileName)
		var syncOps []string
		client.exp.mu.Lock()
		// The write PUBLISHES the file and then reports failure — the
		// rename/link landed, the parent-directory sync did not.
		client.exp.writeSubjectFileForTests = func(path string, payload []byte, initialMint bool) error {
			if err := os.WriteFile(path, payload, 0o600); err != nil {
				t.Errorf("seam write: %v", err)
			}
			return errors.New("parent sync failed after the rename landed")
		}
		client.exp.syncDirFn = func(dir string) error {
			syncOps = append(syncOps, "sync")
			return syncDir(dir)
		}
		client.exp.consentRaceSeam = func(stage string) {
			if stage == "mint_adopted" {
				client.consent.Store(consentStateDenied)
				client.consentEpoch.Add(1)
			}
		}
		client.exp.mu.Unlock()

		if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrConsentDenied) {
			t.Fatalf("expected the refusal from the mint commit re-check, got %v", err)
		}
		client.exp.mu.Lock()
		client.exp.consentRaceSeam = nil
		client.exp.writeSubjectFileForTests = nil
		subject := client.exp.subjectID
		recordedSyncs := len(syncOps)
		client.exp.mu.Unlock()
		if subject != "" {
			t.Fatalf("the refused mint must leave memory clean, got %q", subject)
		}
		if _, err := os.Stat(subjectPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("a mint whose write REPORTED failure can still have published the file (the parent sync failed after the rename): the undo must own and remove it, or the refused session's spcid_ survives for a later granted session (stat err=%v)", err)
		}
		if recordedSyncs == 0 {
			t.Fatalf("the undo's unlink must be made durable with a parent-directory sync — an unsynced unlink can resurrect the refused session's subject file")
		}
	})

	t.Run("converged_winner_file_is_never_removed", func(t *testing.T) {
		script := &expScript{}
		script.push(200, expAssignedBody("1"))
		capture := &expWireCapture{}
		server := newExperimentServer(t, script, capture)
		defer server.Close()
		spoolDir := t.TempDir()
		client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
		defer client.Close(context.Background())
		client.SetConsent(true)

		// Another process wins the initial publish race BETWEEN this
		// client's miss-read and its create-only link: model the miss by
		// marking the subject checked-and-absent, then plant the winner.
		winner := "spcid_" + strings.Repeat("c", 32)
		subjectPath := filepath.Join(spoolDir, expSubjectFileName)
		if err := os.WriteFile(subjectPath, []byte(winner+"\n"), 0o600); err != nil {
			t.Fatalf("plant winner: %v", err)
		}
		client.exp.mu.Lock()
		client.exp.subjectChecked = true
		client.exp.subjectID = ""
		client.exp.consentRaceSeam = func(stage string) {
			if stage == "mint_adopted" {
				client.consent.Store(consentStateDenied)
				client.consentEpoch.Add(1)
			}
		}
		client.exp.mu.Unlock()

		if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrConsentDenied) {
			t.Fatalf("expected the refusal from the mint commit re-check, got %v", err)
		}
		client.exp.mu.Lock()
		client.exp.consentRaceSeam = nil
		client.exp.mu.Unlock()
		data, err := os.ReadFile(subjectPath)
		if err != nil || strings.TrimSpace(string(data)) != winner {
			t.Fatalf("the converged-on-winner file is another process's subject state and must survive the undo untouched (data=%q err=%v)", data, err)
		}
	})
}

// ── GF3: the tombstone spend's missing-file path syncs too ──────────────────

func TestTombstoneSpendMissingFileStillSyncs(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())
	e := client.exp

	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.writeCondemnationTombstoneLocked("scope-x") {
		t.Fatalf("setup: tombstone write must land")
	}
	var ops []string
	syncHealthy := false
	e.syncDirFn = func(dir string) error {
		ops = append(ops, "sync")
		if !syncHealthy {
			return errors.New("dir sync refused")
		}
		return syncDir(dir)
	}

	// Attempt 1: the unlink LANDS in the namespace, its directory sync
	// fails — the spend must report NOT landed (round-10 discipline).
	if e.clearCondemnationTombstoneLocked("") {
		t.Fatalf("an unlink whose directory sync failed is not a completed spend")
	}
	if _, err := os.Stat(e.tombstoneFilePath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("test shape: the first attempt's unlink must have landed in the namespace (stat err=%v)", err)
	}

	// Attempt 2 (the retry): os.Remove now reports ErrNotExist — but the
	// PRIOR unlink was never made durable, so the missing-file path must
	// sync the parent before the spend reports landed, and a still-failing
	// sync must keep failing CLOSED.
	opsBefore := len(ops)
	if e.clearCondemnationTombstoneLocked("") {
		t.Fatalf("the ErrNotExist retry must not report the spend durable while the parent sync still fails — a crash could resurrect the stale tombstone and condemn the NEXT record written for its scope")
	}
	if len(ops) == opsBefore {
		t.Fatalf("the ErrNotExist retry must attempt the parent-directory sync (the missing-file-path-syncs-too discipline)")
	}

	// Storage heals: the retry syncs and the spend completes.
	syncHealthy = true
	opsBefore = len(ops)
	if !e.clearCondemnationTombstoneLocked("") {
		t.Fatalf("the healed retry must complete the spend")
	}
	if len(ops) == opsBefore {
		t.Fatalf("the healed spend must be backed by a recorded parent sync")
	}
}

// ── GF4: a fresh fact born in the sentinel → pipeline-purge gap survives ────

func TestSentinelGapFactSurvivesPipelinePurge(t *testing.T) {
	t.Run("queue_filter_spares_gap_born_fact", func(t *testing.T) {
		capture := &expWireCapture{}
		server := newExperimentServer(t, &expScript{}, capture)
		defer server.Close()
		client := newExperimentClient(t, server.URL, nil)
		// The queue must hold the facts across the purge: stop the worker
		// so nothing drains it concurrently.
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

		// A fact built and queued BEFORE the sentinel: withdrawn.
		staleEvent, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-gap", entry, "stale-fact", client.exp.sessionMarker)
		if skip != "" {
			t.Fatalf("stale build refused (%s)", skip)
		}
		if !client.queue.enqueue(staleEvent) {
			t.Fatalf("enqueue stale")
		}

		// The sentinel's decisive moment (under e.mu), exactly as
		// applySentinelWithdrawalLocked runs it.
		client.exp.mu.Lock()
		client.exp.factPurgeEpochBumpFn()
		client.exp.mu.Unlock()

		// The GAP: a fresh authorized fetch installs and enqueues its
		// exposure after the sentinel released e.mu but before the purge
		// takes emitMu. The fact is stamped with the post-sentinel epoch.
		freshEvent, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-gap", entry, "fresh-fact", client.exp.sessionMarker)
		if skip != "" {
			t.Fatalf("fresh build refused (%s)", skip)
		}
		if !client.queue.enqueue(freshEvent) {
			t.Fatalf("enqueue fresh")
		}

		client.purgeWithdrawnExperimentFacts()

		var survivors []string
		for _, event := range drainQueuedEvents(client) {
			survivors = append(survivors, event.ID)
		}
		joined := strings.Join(survivors, ",")
		if !strings.Contains(joined, "fresh-fact") {
			t.Fatalf("the class-only queue sweep withdrew a FRESH post-sentinel fact born in the emitMu gap — the filter must compare the per-fact epoch stamp (survivors: %q)", joined)
		}
		if strings.Contains(joined, "stale-fact") {
			t.Fatalf("the pre-sentinel fact must be withdrawn (survivors: %q)", joined)
		}
	})

	t.Run("worker_admission_drops_stolen_withdrawn_fact", func(t *testing.T) {
		capture := &expWireCapture{}
		server := newExperimentServer(t, &expScript{}, capture)
		defer server.Close()
		client := newExperimentClient(t, server.URL, nil)
		// Worker-owned state (seen marks): stop the real worker first.
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
		staleEvent, skip := client.buildExperimentFactEvent(experimentExposureName, "exp-steal", entry, "stolen-fact", client.exp.sessionMarker)
		if skip != "" {
			t.Fatalf("stale build refused (%s)", skip)
		}

		// The sentinel bumps; a dispatch point observes the move against
		// an empty batch and advances the worker's seen mark.
		client.exp.mu.Lock()
		client.exp.factPurgeEpochBumpFn()
		client.exp.mu.Unlock()
		seenConsent := client.consentEpoch.Load()
		backoff := 0
		batch := client.dropBatchOnConsentEpoch(nil, &seenConsent, &backoff)

		// The receive then steals the withdrawn old-stamp fact from the
		// queue before the purge's filter drained it: admission must drop
		// it — no later dispatch point will (the seen mark already
		// matches).
		droppedBefore := client.Snapshot().Dropped
		batch = client.admitReceivedEvent(batch, staleEvent, &seenConsent, &backoff)
		if len(batch) != 0 {
			t.Fatalf("a withdrawn fact stolen after the seen mark advanced must be dropped at ADMISSION, got %d in the batch — it would ship under a matching seen epoch, unfiltered", len(batch))
		}
		if dropped := client.Snapshot().Dropped - droppedBefore; dropped != 1 {
			t.Fatalf("the stolen fact counts exactly once, got %d", dropped)
		}
	})
}

// ── GF5: an owed real-subjects clear settles only with a durable absence ────

func TestOwedClearAbsentRecordSettlesOnlyWithDurableSync(t *testing.T) {
	capture := &expWireCapture{}
	server := newExperimentServer(t, &expScript{}, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())
	e := client.exp

	// The sentinel's clear ran earlier: the experiments.json unlink landed
	// in the NAMESPACE but its directory sync failed, leaving the clear
	// owed with nothing at the path — exactly what a crash can resurrect.
	var ops []string
	syncHealthy := false
	e.mu.Lock()
	e.durableClearPending = true
	e.durableClearAsOf = 41
	e.durableClearScope = "scope-y"
	e.syncDirFn = func(dir string) error {
		ops = append(ops, "sync")
		if !syncHealthy {
			return errors.New("dir sync refused")
		}
		return syncDir(dir)
	}
	e.mu.Unlock()

	// The retry finds no record in the namespace. Absence alone is NOT
	// durable absence: the settle must route through the clear's
	// missing-file path (parent sync) and stay OWED while the sync fails.
	e.retryDurableSync()
	e.mu.Lock()
	pending := e.durableClearPending
	syncsRecorded := len(ops)
	e.mu.Unlock()
	if syncsRecorded == 0 {
		t.Fatalf("the absent-record retry must attempt the parent-directory sync — namespace absence can be a prior unlink whose sync never landed, and a crash would resurrect the withdrawn record")
	}
	if !pending {
		t.Fatalf("the clear must stay OWED while its durable-absence sync keeps failing — reporting it landed leaves the resurrection window open")
	}

	// Storage heals: the next retry lands the clear durably.
	syncHealthy = true
	e.retryDurableSync()
	e.mu.Lock()
	pending = e.durableClearPending
	e.mu.Unlock()
	if pending {
		t.Fatalf("the healed retry must settle the owed clear")
	}
}

// ── GF6: consent purges preserve the explicit re-arm counter ────────────────

func TestExplicitArmSurvivesConsentPurge(t *testing.T) {
	script := &expScript{}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())
	client.SetConsent(true)

	// The automatic arm-0 fact, then an explicit re-arm (arm 1) — both
	// published.
	fetchAssignment(t, client, expTestScopeKey)
	if err := client.TrackExperimentExposure(expTestScopeKey); err != nil {
		t.Fatalf("explicit re-arm: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	client.exp.mu.Lock()
	marker := client.exp.sessionMarker
	subject := client.exp.entries[expTestScopeKey].SubjectKey
	client.exp.mu.Unlock()
	arm1ID := experimentExposureEventID(marker, subject, expTestScopeKey, 1, 1)
	arm2ID := experimentExposureEventID(marker, subject, expTestScopeKey, 1, 2)

	// The denial purges queued facts and re-arms the session's emissions;
	// the re-grant restores the plane.
	client.SetConsent(false)
	client.SetConsent(true)
	// The automatic re-emission drains (same deterministic arm-0 id: the
	// published survivor collapses server-side).
	client.sweepAllExperimentExposures()

	// The session already handed out arm 1: the next explicit re-arm is a
	// REAL new re-exposure and must take arm 2 — a purge that reset the
	// counter would reuse arm 1, derive the same event id, and the
	// server's de-dupe would undercount it.
	if err := client.TrackExperimentExposure(expTestScopeKey); err != nil {
		t.Fatalf("post-re-grant explicit re-arm: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	var ids []string
	for _, exposure := range capture.exposures() {
		if id, ok := exposure["event_id"].(string); ok {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		t.Fatalf("no exposures delivered")
	}
	lastID := ids[len(ids)-1]
	if lastID == arm1ID {
		t.Fatalf("the post-re-grant explicit re-arm reused arm 1 — the same deterministic id as the pre-denial explicit fact, which the server de-dupes, undercounting a real re-exposure (ids: %v)", ids)
	}
	if lastID != arm2ID {
		t.Fatalf("the post-re-grant explicit re-arm must continue from the session high-water (arm 2 id %s), got %s (ids: %v)", arm2ID, lastID, ids)
	}
}

// The sentinel's slate reset follows the same id-domain rule: the arm
// high-water survives, so a post-re-enable explicit re-arm never collides
// with a delivered pre-sentinel fact.
func TestExplicitArmSurvivesSentinelSlateReset(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(403, `{"error":"`+expSentinelRealSubjectsDisabled+`"}`)
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())
	client.SetConsent(true)

	fetchAssignment(t, client, expTestScopeKey)
	if err := client.TrackExperimentExposure(expTestScopeKey); err != nil {
		t.Fatalf("explicit re-arm: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	client.exp.mu.Lock()
	marker := client.exp.sessionMarker
	subject := client.exp.entries[expTestScopeKey].SubjectKey
	client.exp.mu.Unlock()
	arm1ID := experimentExposureEventID(marker, subject, expTestScopeKey, 1, 1)
	arm2ID := experimentExposureEventID(marker, subject, expTestScopeKey, 1, 2)

	// The sentinel withdraws the plane; the platform then re-enables and
	// the SAME tuple (experiment, version, subject) is re-fetched.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil {
		t.Fatalf("the sentinel fetch must fail closed")
	}
	if result := fetchAssignment(t, client, expTestScopeKey); !result.Assigned {
		t.Fatalf("the re-enabled fetch must assign, got %+v", result)
	}
	if err := client.TrackExperimentExposure(expTestScopeKey); err != nil {
		t.Fatalf("post-re-enable explicit re-arm: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	var ids []string
	for _, exposure := range capture.exposures() {
		if id, ok := exposure["event_id"].(string); ok {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		t.Fatalf("no exposures delivered")
	}
	lastID := ids[len(ids)-1]
	if lastID == arm1ID {
		t.Fatalf("the post-re-enable explicit re-arm reused arm 1 — colliding with the delivered pre-sentinel fact's id (ids: %v)", ids)
	}
	if lastID != arm2ID {
		t.Fatalf("the post-re-enable explicit re-arm must continue from the session high-water (arm 2 id %s), got %s (ids: %v)", arm2ID, lastID, ids)
	}
}

// ── GF-P2 (defold R23 parity): an explicit null version is present, not
// absent — malformed on every shape ─────────────────────────────────────────

func TestNullVersionIsMalformedNotAbsent(t *testing.T) {
	scope := expTestRequestScope()
	for _, body := range []string{
		`{"assigned":false,"version":null}`,
		`{"assigned":false,"reason":"kill_switch","version":null}`,
		`{"assigned":true,"assignment_key":"asgn_abc","variant_key":"treatment","version":null,"boundary":{"assignment_unit":"client_id"}}`,
	} {
		if _, _, ok := parseExperimentVerdict(expTestResponse(200, body), scope, 42); ok {
			t.Fatalf("body %q must classify malformed: a bare-pointer decode collapses an explicit null into the ABSENT shape, slipping the positivity rule into the authoritative paths", body)
		}
	}
	// The genuinely absent version keeps the traffic-gate tolerance.
	if _, _, ok := parseExperimentVerdict(expTestResponse(200, `{"assigned":false}`), scope, 42); !ok {
		t.Fatalf("an absent version must stay tolerated on the not-assigned shape")
	}

	// End to end: the exact finding body takes the transient path — the
	// cache retained and served stale — never the authoritative drop.
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, `{"assigned":false,"version":null}`)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())
	client.SetConsent(true)
	if result := fetchAssignment(t, client, expTestScopeKey); !result.Assigned {
		t.Fatalf("setup fetch: %+v", result)
	}
	result := fetchAssignment(t, client, expTestScopeKey)
	if !result.FromCache || result.VariantKey != "treatment" || result.Code != "malformed_response" {
		t.Fatalf("the null-version 200 must serve the retained cache over malformed_response, got %+v", result)
	}
	client.exp.mu.Lock()
	entry := client.exp.entries[expTestScopeKey]
	client.exp.mu.Unlock()
	if entry == nil || entry.Version != 1 {
		t.Fatalf("the cached assignment must survive the null-version verdict, got %+v", entry)
	}
}
