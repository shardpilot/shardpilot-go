package shardpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// releaseOnCleanup prevents an assertion from stranding a fixture handler.
func releaseOnCleanup(t *testing.T) (chan struct{}, func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return gate, release
}

func TestHostAgeCallOrderPrecedesDispatchOrder(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.script.push(http.StatusServiceUnavailable, `{}`)
	rig.script.push(http.StatusOK, ageGolden(t, "adult"))
	// Remove the repeated setup answer before either new request arrives.
	rig.script.mu.Lock()
	rig.script.responses = rig.script.responses[1:]
	rig.script.mu.Unlock()
	gate, release := releaseOnCleanup(t)
	entered := make(chan struct{})
	var declarations atomic.Int32
	rig.client.exp.mu.Lock()
	rig.client.exp.consentRaceSeam = func(stage string) {
		if stage == "host_declared" && declarations.Add(1) == 1 {
			close(entered)
			<-gate
		}
	}
	rig.client.exp.mu.Unlock()
	done := make(chan ExperimentAssignmentResult, 1)
	go func() {
		result, _ := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil)
		done <- result
	}()
	<-entered
	_, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
	if err == nil {
		t.Fatal("the non-adult request must reach the transient fixture")
	}
	release()
	late := <-done
	if rig.script.requestCount() != 3 {
		t.Fatalf("actual assignment requests=%d, want setup, non-adult, then delayed adult", rig.script.requestCount())
	}
	if late.Assigned || rig.client.ExperimentVariant(ageGoldenExperiment) != "" {
		t.Fatal("an adult call that began earlier reinstalled after the later non-adult withdrawal")
	}
}

func TestAHostCallCannotJoinALaterAuthorizationEpoch(t *testing.T) {
	for _, handle := range []bool{false, true} {
		name := "client"
		if handle {
			name = "handle"
		}
		t.Run(name, func(t *testing.T) {
			rig := newAgeWithdrawRig(t, "", nil, ageGolden(t, "adult"))
			rig.script.mu.Lock()
			rig.script.responses = []expScriptResponse{{status: http.StatusUnauthorized, body: `{}`}, {status: http.StatusOK, body: ageGolden(t, "adult")}}
			rig.script.mu.Unlock()
			fetch := rig.client.FetchExperimentAssignmentWithAgeBand
			variant := rig.client.ExperimentVariant
			stage := "host_declared"
			if handle {
				s := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
				fetch = s.FetchWithAgeBand
				variant = s.Variant
				stage = "subject_declared"
			}
			gate, release := releaseOnCleanup(t)
			entered := make(chan struct{})
			var calls atomic.Int32
			rig.client.exp.mu.Lock()
			rig.client.exp.consentRaceSeam = func(at string) {
				if at == stage && calls.Add(1) == 1 {
					close(entered)
					<-gate
				}
			}
			rig.client.exp.mu.Unlock()
			type answer struct {
				result ExperimentAssignmentResult
				err    error
			}
			done := make(chan answer, 1)
			go func() {
				result, err := fetch(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil)
				done <- answer{result, err}
			}()
			<-entered
			if _, err := fetch(context.Background(), "other-banner", ExperimentAgeBandAdult, nil); err == nil {
				t.Fatal("the intervening request must reach the authorization refusal")
			}
			release()
			old := <-done
			if old.err == nil || old.result.Assigned || variant(ageGoldenExperiment) != "" {
				t.Fatalf("an earlier host call joined the later authorization epoch: %+v %v", old.result, old.err)
			}
		})
	}
}

func TestAdultCallFencesAnOlderNonAdultAnswer(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageGolden(t, "under_threshold"), ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	oldGate, releaseOld := releaseOnCleanup(t)
	adultGate, releaseAdult := releaseOnCleanup(t)
	rig.script.gates = map[int]chan struct{}{1: oldGate, 2: adultGate}
	oldDone := make(chan struct{})
	go func() {
		_, _ = rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
		close(oldDone)
	}()
	waitFor(t, 5*time.Second, "non-adult dispatch", func() bool { return rig.script.requestCount() == 2 })
	adultDone := make(chan struct{})
	go func() {
		_, _ = rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil)
		close(adultDone)
	}()
	waitFor(t, 5*time.Second, "later adult dispatch", func() bool { return rig.script.requestCount() == 3 })
	releaseOld()
	<-oldDone
	if got := rig.client.owedExperimentExposureCount(); got != 2 {
		t.Errorf("older non-adult answer withdrew %d of the two applications after the adult call", 2-got)
	}
	releaseAdult()
	<-adultDone
}

func TestAnOldAuthorizationEpochCannotConfirmRestoredAge(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	if err := rig.client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted := rig.launch(t, nil)
	restarted.SetConsent(true)
	rig.script.mu.Lock()
	rig.script.responses = []expScriptResponse{
		{status: http.StatusUnauthorized, body: `{}`},
		{status: http.StatusOK, body: ageGolden(t, "adult")},
		{status: http.StatusOK, body: ageGoldenForKey(t, "adult", "other-banner")},
	}
	rig.script.mu.Unlock()
	gate, release := releaseOnCleanup(t)
	entered := make(chan struct{})
	var calls atomic.Int32
	restarted.exp.mu.Lock()
	restarted.exp.consentRaceSeam = func(stage string) {
		if stage == "host_declared" && calls.Add(1) == 1 {
			close(entered)
			<-gate
		}
	}
	restarted.exp.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		_, err := restarted.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil)
		done <- err
	}()
	<-entered
	if _, err := restarted.FetchExperimentAssignmentWithAgeBand(context.Background(), "other-banner", ExperimentAgeBandAdult, nil); err == nil {
		t.Fatal("the intervening request must reach the authorization refusal")
	}
	release()
	if err := <-done; err == nil {
		t.Fatal("the earlier call must remain unauthorized")
	}
	fetchAdultAssignment(t, restarted, "other-banner")
	if got := restarted.ExperimentVariant(ageGoldenExperiment); got != "" {
		t.Errorf("an old authorization epoch confirmed restored age: %q", got)
	}
}

func TestPlaneRefusalsBypassThePerKeyFence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{{"unauthorized", 401, `{}`}, {"forbidden", 403, `{}`}, {"disabled", 403, `{"error":{"code":"real_subjects_disabled"}}`}} {
		t.Run(tc.name, func(t *testing.T) {
			other := "other-banner"
			rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageGoldenForKey(t, "adult", other))
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			fetchAdultAssignment(t, rig.client, other)
			rig.script.mu.Lock()
			rig.script.responses = nil
			rig.script.mu.Unlock()
			rig.script.push(tc.status, tc.body)
			rig.script.push(http.StatusOK, ageGolden(t, "adult"))
			gate, release := releaseOnCleanup(t)
			rig.script.gates = map[int]chan struct{}{2: gate}
			done := make(chan struct{})
			go func() {
				_, _ = rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil)
				close(done)
			}()
			waitFor(t, 5*time.Second, "older request dispatch", func() bool { return rig.script.requestCount() == 3 })
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			if got := rig.client.ExperimentVariant(other); got == "" {
				t.Fatal("positive control: sibling assignment must serve before the refusal")
			}
			release()
			<-done
			if got := rig.client.ExperimentVariant(other); got != "" {
				t.Errorf("plane-wide %s was discarded by another experiment's sequence fence: %q", tc.name, got)
			}
		})
	}
}

func TestFactsWaitForThePendingAgeAnswer(t *testing.T) {
	for _, spoolFirst := range []bool{false, true} {
		for _, reason := range []string{"age_ineligible", "kill_switch"} {
			t.Run(reason+map[bool]string{false: "/queued", true: "/spooled"}[spoolFirst], func(t *testing.T) {
				rig := newAgeWithdrawRig(t, t.TempDir(), func(cfg *Config) { cfg.HTTPTimeout = 10 * time.Second }, ageGolden(t, "adult"), ageRefusalBody(t, reason))
				fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
				rig.applyAndMeasure(t, ageGoldenExperiment)
				rig.client.experimentCycle(context.Background())
				if spoolFirst {
					rig.capture.setStatus(http.StatusServiceUnavailable)
					_ = rig.client.Flush(context.Background())
					if spooledExperimentFacts(rig.client, ageGoldenExperiment) != 2 {
						t.Fatal("setup must spool both new facts")
					}
				}
				gate, release := releaseOnCleanup(t)
				rig.script.gates = map[int]chan struct{}{1: gate}
				done := make(chan struct{})
				go func() {
					_, _ = rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
					close(done)
				}()
				waitFor(t, 5*time.Second, "pending age request", func() bool { return rig.script.requestCount() == 2 })
				rig.capture.setStatus(http.StatusAccepted)
				if err := rig.client.Enqueue(Event{Name: "host.allowed"}); err != nil {
					t.Fatal(err)
				}
				_ = rig.client.Flush(context.Background())
				if got := len(rig.capture.byName("host.allowed")); got != 1 {
					t.Errorf("ordinary work blocked by quarantined facts: got %d", got)
				}
				if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != 0 {
					t.Errorf("%d facts crossed ingest before the age answer", got)
				}
				release()
				<-done
				flushOrFail(t, rig.client)
				want := 0
				if reason == "kill_switch" {
					want = 2
				}
				if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != want {
					t.Errorf("after %s delivered=%d want=%d", reason, got, want)
				}
			})
		}
	}
}

func TestPendingAgeSurvivesAnUncleanRestart(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		mode := "explicit"
		if automatic {
			mode = "automatic"
		}
		t.Run(mode, func(t *testing.T) {

			for _, failStateWrite := range []bool{false, true} {
				name := "ordinary_storage"
				if failStateWrite {
					name = "state_write_fails"
				}
				t.Run(name, func(t *testing.T) {

					rig := newAgeWithdrawRig(t, t.TempDir(), func(cfg *Config) { cfg.HTTPTimeout = 10 * time.Second }, ageGolden(t, "adult"), ageRefusalBody(t, "kill_switch"))
					fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
					if err := rig.client.Enqueue(Event{Name: "host.before"}); err != nil {
						t.Fatal(err)
					}
					rig.sealAndSpool(t, 2)
					if failStateWrite {
						rig.client.exp.mu.Lock()
						rig.client.exp.failDurableWritesForTests = true
						record := rig.client.exp.loadDurableRecordLocked()
						failed := !rig.client.exp.saveDurableRecordLocked(record)
						rig.client.exp.mu.Unlock()
						if !failed {
							t.Fatal("the durable-state write fault must actually fire")
						}
					}
					gate, release := releaseOnCleanup(t)
					defer release()
					rig.script.gates = map[int]chan struct{}{1: gate}
					done := make(chan struct{})
					go func() {
						_, _ = rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
						close(done)
					}()
					waitFor(t, 5*time.Second, "pending age request", func() bool { return rig.script.requestCount() == 2 })
					// Snapshot only bytes already durable while the first client remains
					// alive; Close must not settle its pending declaration before the copy.
					crashImage := t.TempDir()
					entries, err := os.ReadDir(rig.spool)
					if err != nil {
						t.Fatal(err)
					}
					for _, entry := range entries {
						if !entry.Type().IsRegular() {
							continue
						}
						data, err := os.ReadFile(filepath.Join(rig.spool, entry.Name()))
						if err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(crashImage, entry.Name()), data, 0600); err != nil {
							t.Fatal(err)
						}
					}
					restarted := newExperimentClient(t, rig.server.URL, func(cfg *Config) {
						cfg.AppKey = "exposure-app"
						cfg.SpoolDir = crashImage
						if automatic {
							cfg.FlushInterval = 20 * time.Millisecond
						}
					})
					t.Cleanup(func() {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						_ = restarted.Close(ctx)
					})
					restarted.SetConsent(true)
					if got := restarted.ExperimentVariant(ageGoldenExperiment); got != "" {
						t.Errorf("unclean restart served %q from the earlier adult declaration", got)
					}
					rig.capture.setStatus(http.StatusAccepted)
					if automatic {
						waitFor(t, 5*time.Second, "ordinary replay during age quarantine", func() bool { return len(rig.capture.byName("host.before")) > 0 })
					} else {
						_ = restarted.Flush(context.Background())
					}
					if got := len(rig.capture.byName("host.before")); got != 1 {
						t.Errorf("ordinary spooled sibling must replay exactly once: %d", got)
					}
					if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != 0 {
						t.Errorf("unclean restart replayed %d facts before the host resolved age", got)
					}
					release()
					<-done
					rig.script.mu.Lock()
					rig.script.responses = nil
					rig.script.mu.Unlock()
					rig.script.push(http.StatusOK, ageGolden(t, "adult"))
					fetchAdultAssignment(t, restarted, ageGoldenExperiment)
					flushOrFail(t, restarted)
					if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != 2 {
						t.Errorf("fresh adult declaration must release the retained facts: got %d", got)
					}
				})
			}
		})
	}
}

func TestAnUnsavedAssignmentKeyCannotEnterThePipeline(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	rig.client.exp.mu.Lock()
	rig.client.exp.failDurableWritesForTests = true
	rig.client.exp.mu.Unlock()
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	rig.client.experimentCycle(context.Background())
	_ = rig.client.Flush(context.Background())
	if len(rig.script.apply.requestsSoFar()) == 0 {
		t.Fatal("the actual apply endpoint must have been called")
	}
	if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != 0 {
		t.Errorf("delivered %d new facts whose key was never durable", got)
	}
	if got := rig.client.owedExperimentExposureCount(); got != 2 {
		t.Errorf("unsaved fact key must keep two applications owed, got %d", got)
	}
	rig.client.exp.mu.Lock()
	rig.client.exp.failDurableWritesForTests = false
	rig.client.exp.mu.Unlock()
	rig.client.experimentCycle(context.Background())
	flushOrFail(t, rig.client)
	if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != 2 {
		t.Errorf("after durable recovery delivered=%d want=2", got)
	}
}

func TestAgeQuarantinePreservesTransportPacing(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), func(cfg *Config) { cfg.HTTPTimeout = 10 * time.Second }, ageGolden(t, "adult"), ageRefusalBody(t, "kill_switch"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.sealAndSpool(t, 2)
	gate, release := releaseOnCleanup(t)
	rig.script.gates = map[int]chan struct{}{1: gate}
	done := make(chan struct{})
	go func() {
		_, _ = rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
		close(done)
	}()
	waitFor(t, 5*time.Second, "pending age request", func() bool { return rig.script.requestCount() == 2 })
	failedBatches := rig.client.Snapshot().FailedBatches
	err := rig.client.Flush(context.Background())
	if !errors.Is(err, errExperimentAgePending) {
		t.Fatalf("the actual held flush must report the age gate: %v", err)
	}
	deadline := rig.client.clock.Now().Add(time.Minute)
	deferUntil, attempt := deadline, 3
	// Probe the real Flush error without mutating the live worker's pacing
	// bookkeeping from this goroutine. The durable checks below stay live.
	pacing := &Client{clock: rig.client.clock}
	pacing.applyRetryPacing(err, &deferUntil, &attempt)
	if !deferUntil.Equal(deadline) || attempt != 3 {
		t.Error("age quarantine changed the ingest retry schedule")
	}
	if failed := rig.client.spool.storeRetryDeadline(deadline.UnixMilli()); failed {
		t.Fatal("the prior retry window must actually persist")
	}
	rig.client.spoolStoreResendDeadline(err)
	data, readErr := os.ReadFile(filepath.Join(rig.spool, "spool.json"))
	var stored struct {
		RetryAfterUntilMS int64 `json:"retry_after_until_ms"`
	}
	if readErr != nil || json.NewDecoder(bytes.NewReader(data)).Decode(&stored) != nil {
		t.Fatalf("read the actual durable retry window: %v", readErr)
	}
	if stored.RetryAfterUntilMS != deadline.UnixMilli() {
		t.Error("age quarantine withdrew the durable ingest retry window")
	}
	if rig.client.Snapshot().FailedBatches != failedBatches {
		t.Error("an unattempted age-held publish counted as an ingest failure")
	}
	release()
	<-done
}

func TestRestartNeedsAHostDecisionBeforeCacheOrRevalidation(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	if err := rig.client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	rig.script.mu.Lock()
	rig.script.responses = nil
	rig.script.mu.Unlock()
	rig.script.push(http.StatusServiceUnavailable, `{}`)
	restarted := rig.launch(t, nil)
	restarted.exp.mu.Lock()
	restarted.exp.revalidateAtMS = 1
	restarted.exp.mu.Unlock()
	restarted.experimentCycle(context.Background())
	if rig.script.requestCount() != 1 {
		t.Error("automatic revalidation reused a prior process's age declaration")
	}
	answer, err := restarted.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil)
	if err == nil || answer.Assigned || answer.FromCache || restarted.ExperimentVariant(ageGoldenExperiment) != "" {
		t.Errorf("transient response substituted for the post-launch decision: %+v %v", answer, err)
	}
	rig.script.mu.Lock()
	rig.script.responses = nil
	rig.script.mu.Unlock()
	rig.script.push(http.StatusOK, ageGolden(t, "adult"))
	fetchAdultAssignment(t, restarted, ageGoldenExperiment)
	if restarted.ExperimentVariant(ageGoldenExperiment) != "control" {
		t.Fatal("a fresh assigned decision must release serving")
	}
}

func TestAnAgeDeclarationBetweenBuildAndHandoffHoldsTheFact(t *testing.T) {
	for _, reason := range []string{"age_ineligible", "kill_switch"} {
		t.Run(reason, func(t *testing.T) {
			rig := newAgeWithdrawRig(t, t.TempDir(), func(cfg *Config) { cfg.HTTPTimeout = 10 * time.Second }, ageGolden(t, "adult"), ageRefusalBody(t, reason))
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)
			rig.client.experimentCycle(context.Background())
			built := make(chan struct{})
			buildGate, releaseBuild := releaseOnCleanup(t)
			ageGate, releaseAge := releaseOnCleanup(t)
			var once sync.Once
			rig.client.builtBatchHandoffSeam = func([]Event) { once.Do(func() { close(built); <-buildGate }) }
			rig.script.gates = map[int]chan struct{}{1: ageGate}
			flushed := make(chan error, 1)
			go func() { flushed <- rig.client.Flush(context.Background()) }()
			<-built
			fetched := make(chan struct{})
			go func() {
				_, _ = rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
				close(fetched)
			}()
			waitFor(t, 5*time.Second, "age declared after actual batch build", func() bool { return rig.script.requestCount() == 2 })
			releaseBuild()
			_ = <-flushed
			if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != 0 {
				t.Errorf("late declaration let %d built facts cross the handoff", got)
			}
			releaseAge()
			<-fetched
			flushOrFail(t, rig.client)
			want := 0
			if reason == "kill_switch" {
				want = 2
			}
			if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != want {
				t.Errorf("after %s delivered=%d want=%d", reason, got, want)
			}
		})
	}
}
