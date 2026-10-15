package shardpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnsealedApplicationsSurviveAProcessImage(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.script.apply.push(http.StatusServiceUnavailable, `{}`)
	rig.script.outcome.push(http.StatusServiceUnavailable, `{}`)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	if err := rig.client.TrackExperimentExposure(ageGoldenExperiment); err != nil {
		t.Fatal(err)
	}
	rig.client.experimentCycle(context.Background())
	if len(rig.script.apply.requestsSoFar()) == 0 {
		t.Fatal("the actual apply endpoint must have refused an application")
	}
	rig.client.exp.mu.Lock()
	var want []string
	alreadySealed := false
	for _, record := range rig.client.exp.pendingExposure[ageGoldenExperiment] {
		if record.sealed != nil {
			alreadySealed = true
		}
		id := record.app.exposureID
		if record.outcome != nil {
			id = record.outcome.outcomeID
		}
		want = append(want, id)
	}
	rig.client.exp.mu.Unlock()
	if alreadySealed {
		t.Fatal("the crash image must precede sealing")
	}
	sort.Strings(want)
	if len(want) != 3 {
		t.Fatalf("setup must owe automatic/extra exposure and outcome: %v", want)
	}
	image := t.TempDir()
	files, err := os.ReadDir(rig.spool)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if !file.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(rig.spool, file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(image, file.Name()), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// The image contains only bytes already durable before the clean-up;
	// Close cannot add its final disposition to the simulated process exit.
	_ = rig.client.Close(context.Background())
	rig.script.apply.mu.Lock()
	rig.script.apply.responses = nil
	applyBefore := len(rig.script.apply.requests)
	rig.script.apply.mu.Unlock()
	rig.script.outcome.mu.Lock()
	rig.script.outcome.responses = nil
	outcomeBefore := len(rig.script.outcome.requests)
	rig.script.outcome.mu.Unlock()
	restarted := rig.launch(t, func(cfg *Config) { cfg.SpoolDir = image })
	restarted.SetConsent(true)
	restarted.experimentCycle(context.Background())
	if len(rig.script.apply.requestsSoFar()) != applyBefore || len(rig.script.outcome.requestsSoFar()) != outcomeBefore {
		t.Error("restored applications bypassed the current launch's host decision")
	}
	fetchAdultAssignment(t, restarted, ageGoldenExperiment)
	restarted.experimentCycle(context.Background())
	flushOrFail(t, restarted)
	var got []string
	for _, request := range rig.script.apply.requestsSoFar()[applyBefore:] {
		id, _ := request.body["exposure_id"].(string)
		got = append(got, id)
	}
	for _, request := range rig.script.outcome.requestsSoFar()[outcomeBefore:] {
		id, _ := request.body["outcome_id"].(string)
		got = append(got, id)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("process exit lost or reminted unsealed application IDs: got=%v want=%v", got, want)
	}
	if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != 3 {
		t.Errorf("the restored automatic/extra exposure and outcome must deliver: got=%d", got)
	}
}

func TestMissingSealedScopeKeepsTheApplication(t *testing.T) {
	type scopeCase struct{ field, mode string }
	cases := []scopeCase{{"", "valid"}}
	for _, field := range []string{"workspace_key", "app_key", "environment_key"} {
		for _, mode := range []string{"missing", "empty", "null", "wrong_type", "blank", "foreign"} {
			cases = append(cases, scopeCase{field, mode})
		}
	}
	for _, tc := range cases {
		for _, eventName := range []string{experimentExposureName, experimentOutcomeName} {
			for _, handle := range []bool{false, true} {
				kind := "client"
				if handle {
					kind = "handle"
				}
				t.Run(tc.mode+"/"+tc.field+"/"+eventName+"/"+kind, func(t *testing.T) {
					rig := newExpHopRig(t, nil)
					fact := map[string]any{
						"event_id": expStubFactID("synthetic-application"), "event_name": eventName,
						"event_ts":      time.Now().UTC().Format(time.RFC3339Nano),
						"workspace_key": "workspace-test", "app_key": "app-test", "environment_key": "develop",
						"props": map[string]any{"experiment_key": expTestScopeKey, "experiment_version": 1, "variant_key": "treatment", "assignment_key": "sfk1_" + strings.Repeat("a", 64), "assignment_unit": "client_id", "attestation": "client_attested"},
					}
					if eventName == experimentOutcomeName {
						props := fact["props"].(map[string]any)
						props["outcome_key"], props["outcome_value"] = "score", 1
					}
					switch tc.mode {
					case "missing":
						delete(fact, tc.field)
					case "empty":
						fact[tc.field] = ""
					case "null":
						fact[tc.field] = nil
					case "wrong_type":
						fact[tc.field] = 17
					case "blank":
						fact[tc.field] = " \t "
					case "foreign":
						fact[tc.field] = "different-scope"
					}
					encoded, err := json.Marshal(map[string]any{"fact": fact, "seal": expStubSeal})
					if err != nil {
						t.Fatal(err)
					}
					stub := &rig.script.apply
					if eventName == experimentOutcomeName {
						stub = &rig.script.outcome
					}
					stub.push(http.StatusOK, string(encoded))
					owed := rig.owed
					if handle {
						s := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
						if _, err := s.Fetch(context.Background(), expTestScopeKey, nil); err != nil {
							t.Fatal(err)
						}
						s.Apply(expTestScopeKey)
						if eventName == experimentOutcomeName {
							if err := s.TrackOutcome(expTestScopeKey, "score", 1); err != nil {
								t.Fatal(err)
							}
						}
						owed = s.owedCount
					} else {
						fetchAssignment(t, rig.client, expTestScopeKey)
						rig.client.ApplyExperimentVariant(expTestScopeKey)
						if eventName == experimentOutcomeName {
							if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "score", 1); err != nil {
								t.Fatal(err)
							}
						}
					}
					rig.cycle()
					if len(stub.requestsSoFar()) != 1 {
						t.Fatal("the actual apply endpoint must return the malformed fact once")
					}
					stats := rig.client.Snapshot()
					wantOwed, wantExposure, wantOutcome := 1, uint64(0), uint64(0)
					if tc.mode == "valid" || tc.mode == "foreign" {
						wantOwed = 0
					}
					if tc.mode == "foreign" {
						if eventName == experimentExposureName {
							wantExposure = 1
						} else {
							wantOutcome = 1
						}
					}
					if owed() != wantOwed || stats.ExperimentExposureDrops[expDropForeignScope] != wantExposure || stats.ExperimentOutcomeDrops[expDropForeignScope] != wantOutcome {
						t.Errorf("scope %s/%s disposition: owed=%d want=%d exposure=%v outcome=%v", tc.mode, tc.field, owed(), wantOwed, stats.ExperimentExposureDrops, stats.ExperimentOutcomeDrops)
					}
					if wantOwed == 0 {
						flushOrFail(t, rig.client)
						wantDelivered := 0
						if tc.mode == "valid" {
							wantDelivered = 1
						}
						if got := len(rig.capture.byName(eventName)); got != wantDelivered {
							t.Errorf("scope control %s actually delivered=%d want=%d", tc.mode, got, wantDelivered)
						}
					}
					if wantOwed == 1 {
						rig.client.exp.mu.Lock()
						paced := rig.client.exp.backoffAttempt > 0
						rig.client.exp.mu.Unlock()
						if !paced {
							t.Error("an unusable fact did not pace its retry")
						}
					}
				})
			}
		}
	}
}

func storedApplicationCount(t *testing.T, dir string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "spool.json"))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	doc := parseSpoolDocument(data)
	if !doc.ok {
		t.Fatal("the actual application spool must be readable")
	}
	count := 0
	for _, entry := range doc.live() {
		if entry.application {
			count++
		}
	}
	return count
}

func TestApplicationWithdrawalAlsoSettlesItsDurableCopy(t *testing.T) {
	for _, reason := range []string{"age", "sentinel", "denial", "terminal"} {
		t.Run(reason, func(t *testing.T) {
			rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)
			if got := storedApplicationCount(t, rig.spool); got != 2 {
				t.Fatalf("the two applications must actually be durable before withdrawal: %d", got)
			}
			switch reason {
			case "age", "sentinel":
				rig.script.mu.Lock()
				rig.script.responses = nil
				rig.script.mu.Unlock()
				if reason == "age" {
					rig.script.push(200, ageRefusalBody(t, "age_ineligible"))
					if _, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil); err != nil {
						t.Fatal(err)
					}
				} else {
					rig.script.push(403, `{"error":{"code":"real_subjects_disabled"}}`)
					if _, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err == nil {
						t.Fatal("the actual sentinel must refuse its caller")
					}
				}
			case "denial":
				rig.client.SetConsent(false)
			case "terminal":
				rig.script.apply.push(http.StatusNotFound, `{}`)
				rig.script.outcome.push(http.StatusNotFound, `{}`)
				rig.client.experimentCycle(context.Background())
				if len(rig.script.apply.requestsSoFar()) != 1 || len(rig.script.outcome.requestsSoFar()) != 1 {
					t.Fatal("both actual endpoints must return the terminal refusal")
				}
			}
			if got := storedApplicationCount(t, rig.spool); got != 0 {
				t.Errorf("%s left %d withdrawn applications durable", reason, got)
			}
		})
	}
}

func TestCloseDistinguishesDurableAndMemoryOnlyApplications(t *testing.T) {
	for _, durable := range []bool{false, true} {
		name, dir := "memory", ""
		if durable {
			name, dir = "durable", t.TempDir()
		}
		t.Run(name, func(t *testing.T) {
			rig := newAgeWithdrawRig(t, dir, nil, ageGolden(t, "adult"))
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.script.apply.push(http.StatusServiceUnavailable, `{}`)
			rig.script.outcome.push(http.StatusServiceUnavailable, `{}`)
			rig.applyAndMeasure(t, ageGoldenExperiment)
			_ = rig.client.Close(context.Background())
			want := uint64(1)
			if durable {
				want = 0
				if storedApplicationCount(t, dir) != 2 {
					t.Fatal("Close must leave both durable applications replayable")
				}
			}
			stats := rig.client.Snapshot()
			if stats.ExperimentExposureDrops[expDropUnsealedAtClose] != want || stats.ExperimentOutcomeDrops[expDropUnsealedAtClose] != want {
				t.Errorf("Close called durable applications lost: exposure=%v outcome=%v want each=%d", stats.ExperimentExposureDrops, stats.ExperimentOutcomeDrops, want)
			}
		})
	}
}

func TestTheApplicationBoundRetiresItsDurableOldestMember(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	for i := 0; i < expMaxOwedExposures; i++ {
		if err := rig.client.TrackExperimentExposure(ageGoldenExperiment); err != nil {
			t.Fatal(err)
		}
	}
	if got := storedApplicationCount(t, rig.spool); got != expMaxOwedExposures {
		t.Errorf("the owed bound retained an evicted durable application: got=%d want=%d", got, expMaxOwedExposures)
	}
}

func copyApplicationImage(t *testing.T, dir string) string {
	t.Helper()
	image := t.TempDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(image, entry.Name()), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return image
}

func TestAnInterruptedApplicationHandoffDoesNotReplayBothForms(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	var faulted atomic.Bool
	rig.client.spool.mu.Lock()
	rig.client.spool.renameFn = func(old, next string) error {
		if err := os.Rename(old, next); err != nil {
			return err
		}
		if faulted.CompareAndSwap(false, true) {
			return errors.New("synthetic lost handoff acknowledgement")
		}
		return nil
	}
	rig.client.spool.mu.Unlock()
	rig.client.experimentCycle(context.Background())
	if !faulted.Load() {
		t.Fatal("the actual atomic replacement must reach its injected failure")
	}
	if len(rig.capture.exposures()) != 0 {
		t.Fatal("a handoff whose durability is unconfirmed must not publish")
	}
	rig.client.spool.mu.Lock()
	rig.client.spool.renameFn = os.Rename
	err := rig.client.spool.saveLocked()
	rig.client.spool.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if storedApplicationCount(t, rig.spool) != 1 {
		t.Fatal("the uncertain write and later merge must actually retain both forms")
	}
	image := copyApplicationImage(t, rig.spool)
	// Dispose of the live fixture only after taking the process image. Its
	// denial cannot alter the image, and prevents clean-up from publishing.
	rig.client.SetConsent(false)
	_ = rig.client.Close(context.Background())
	applyBefore := len(rig.script.apply.requestsSoFar())
	restarted := rig.launch(t, func(cfg *Config) { cfg.SpoolDir = image })
	restarted.SetConsent(true)
	if got := restarted.owedExperimentExposureCount(); got != 0 {
		t.Errorf("a durable sealed handoff also reloaded its unsealed form: owed=%d", got)
	}
	fetchAdultAssignment(t, restarted, ageGoldenExperiment)
	restarted.experimentCycle(context.Background())
	flushOrFail(t, restarted)
	if len(rig.script.apply.requestsSoFar()) != applyBefore {
		t.Error("recovery sealed an application already represented by a durable fact")
	}
	if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != 1 {
		t.Errorf("recovery published both forms of the application: facts=%d", got)
	}
}

func TestApplicationHandoffKeepsItsFirstEncodedBytes(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	rig.client.experimentCycle(context.Background())
	rig.client.spool.mu.Lock()
	var saved json.RawMessage
	for _, entry := range rig.client.spool.entries {
		if !entry.application {
			saved = append(json.RawMessage(nil), entry.raw...)
		}
	}
	rig.client.spool.mu.Unlock()
	if len(saved) == 0 {
		t.Fatal("the sealed handoff must actually be durable before queue admission")
	}
	flushOrFail(t, rig.client)
	if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != 1 {
		t.Fatalf("the actual analytics request must deliver the sealed fact: %d", got)
	}
	var batch struct {
		Events []json.RawMessage `json:"events"`
	}
	for _, raw := range rig.capture.rawBatches() {
		if json.Unmarshal(raw, &batch) != nil {
			t.Fatal("the actual analytics body must decode")
		}
		for _, event := range batch.Events {
			if bytes.Contains(event, []byte(`"application_id"`)) || bytes.Contains(event, []byte(`"application_extra"`)) {
				t.Error("local application metadata reached the analytics wire")
			}
			if !bytes.Equal(event, saved) {
				t.Error("the application handoff was encoded again after its durable copy")
			}
		}
	}
}

func TestApplicationReplayUsesTheSpoolGrantAndIndependentSubject(t *testing.T) {
	for _, missingCache := range []bool{false, true} {
		name := "cached"
		if missingCache {
			name = "assignment_write_failed"
		}
		t.Run(name, func(t *testing.T) {
			rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
			rig.client.exp.mu.Lock()
			rig.client.exp.failDurableWritesForTests = missingCache
			rig.client.exp.mu.Unlock()
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.client.ApplyExperimentVariant(ageGoldenExperiment)
			if got := storedApplicationCount(t, rig.spool); got != 1 {
				t.Fatalf("the actual application must persist independently: %d", got)
			}
			if missingCache {
				if _, err := os.Stat(filepath.Join(rig.spool, "experiments.json")); !os.IsNotExist(err) {
					t.Fatal("the injected cache write failure must actually leave no assignment record")
				}
			}
			image := copyApplicationImage(t, rig.spool)
			rig.client.SetConsent(false)
			_ = rig.client.Close(context.Background())
			before := len(rig.script.apply.requestsSoFar())
			restarted := rig.launch(t, func(cfg *Config) { cfg.SpoolDir = image })
			fetchAdultAssignment(t, restarted, ageGoldenExperiment)
			restarted.experimentCycle(context.Background())
			if len(rig.script.apply.requestsSoFar()) != before {
				t.Error("an unsealed replay bypassed the persisted-write grant")
			}
			restarted.SetConsent(true)
			restarted.experimentCycle(context.Background())
			flushOrFail(t, restarted)
			if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != 1 {
				t.Errorf("the independently durable application did not replay after grant: %d", got)
			}
		})
	}
}

func TestApplyBudgetRotatesAcrossTheWholePendingKeySet(t *testing.T) {
	first, last := "a-first", "z-last"
	rig := newAgeWithdrawRig(t, "", nil, ageGoldenForKey(t, "adult", first), ageGoldenForKey(t, "adult", last))
	fetchAdultAssignment(t, rig.client, first)
	fetchAdultAssignment(t, rig.client, last)
	rig.client.ApplyExperimentVariant(first)
	for i := 1; i < expMaxApplyRequestsPerCycle; i++ {
		if err := rig.client.TrackExperimentExposure(first); err != nil {
			t.Fatal(err)
		}
	}
	rig.client.ApplyExperimentVariant(last)
	rig.client.experimentCycle(context.Background())
	if got := len(rig.script.apply.requestsSoFar()); got != expMaxApplyRequestsPerCycle {
		t.Fatalf("the first cycle must execute exactly its request budget: %d", got)
	}
	for i := 0; i < expMaxApplyRequestsPerCycle; i++ {
		if err := rig.client.TrackExperimentExposure(first); err != nil {
			t.Fatal(err)
		}
	}
	rig.client.experimentCycle(context.Background())
	requests := rig.script.apply.requestsSoFar()
	if len(requests) > 2*expMaxApplyRequestsPerCycle {
		t.Error("rotating the keys spent more than the two-cycle budget")
	}
	found := false
	for _, request := range requests {
		found = found || request.body["experiment_key"] == last
	}
	if !found {
		t.Error("refilling the first key starved the later key for both cycles")
	}
}

func TestApplicationIDMintFailuresAreCountedByFactKind(t *testing.T) {
	for _, handle := range []bool{false, true} {
		for _, outcome := range []bool{false, true} {
			kind, path := "exposure", "client"
			if outcome {
				kind = "outcome"
			}
			if handle {
				path = "handle"
			}
			t.Run(path+"/"+kind, func(t *testing.T) {
				var fail atomic.Bool
				var calls atomic.Int32
				rig := newAgeWithdrawRig(t, "", func(cfg *Config) {
					cfg.experimentIDMinterForTests = func() (string, error) {
						calls.Add(1)
						if fail.Load() {
							return "", errors.New("synthetic entropy failure")
						}
						return newExperimentApplicationID()
					}
				}, ageGolden(t, "adult"))
				apply := rig.client.ApplyExperimentVariant
				track := rig.client.TrackExperimentOutcome
				if handle {
					s := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
					if _, err := s.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err != nil {
						t.Fatal(err)
					}
					apply, track = s.Apply, s.TrackOutcome
				} else {
					fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
				}
				if outcome {
					if variant, _ := apply(ageGoldenExperiment); variant != "control" {
						t.Fatal("a real application must precede the failed outcome mint")
					}
				}
				fail.Store(true)
				if outcome {
					if err := track(ageGoldenExperiment, "score", 1); !errors.Is(err, ErrExperimentFactUnavailable) {
						t.Fatalf("the actual failed outcome mint must refuse: %v", err)
					}
				} else if variant, _ := apply(ageGoldenExperiment); variant != "control" {
					t.Fatal("mint failure must keep the documented variant-serving behavior")
				}
				wantCalls := int32(1)
				if outcome {
					wantCalls++
				}
				if calls.Load() != wantCalls {
					t.Fatal("the actual producer must reach the minter")
				}
				stats := rig.client.Snapshot()
				got := stats.ExperimentExposureDrops[expDropNotRecordable]
				if outcome {
					got = stats.ExperimentOutcomeDrops[expDropNotRecordable]
				}
				if got != 1 {
					t.Errorf("the failed %s mint was not counted: got=%d", kind, got)
				}
			})
		}
	}
}

func TestWithdrawnApplyAnswersCannotChangeThePlane(t *testing.T) {
	answers := []struct {
		name   string
		status int
		body   string
	}{
		{"sealed", 0, ""},
		{"terminal", 404, `{}`},
		{"transient", 503, `{}`},
		{"disabled", 403, `{"error":{"code":"experiment_apply_disabled"}}`},
		{"sentinel", 403, `{"error":{"code":"real_subjects_disabled"}}`},
		{"conflict", 409, `{"error":{"code":"version_mismatch"}}`},
		{"age", 409, ageApplyRefusal},
	}
	for _, handle := range []bool{false, true} {
		for _, answer := range answers {
			t.Run(strconv.FormatBool(handle)+"/"+answer.name, func(t *testing.T) {
				rig := newAgeWithdrawRig(t, "", nil, ageGolden(t, "adult"))
				var subject *ExperimentSubject
				if handle {
					subject = newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
					if _, err := subject.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err != nil {
						t.Fatal(err)
					}
					subject.Apply(ageGoldenExperiment)
				} else {
					fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
					rig.client.ApplyExperimentVariant(ageGoldenExperiment)
				}
				rig.client.experimentCycle(context.Background())
				flushOrFail(t, rig.client)
				if len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)) != 1 {
					t.Fatal("the live apply response must first publish a passing control")
				}
				endpoint := &rig.script.apply
				stage := "apply_answer"
				if handle {
					endpoint, stage = &rig.script.outcome, "subject_apply_answer"
					if err := subject.TrackOutcome(ageGoldenExperiment, "score", 1); err != nil {
						t.Fatal(err)
					}
				} else if err := rig.client.TrackExperimentExposure(ageGoldenExperiment); err != nil {
					t.Fatal(err)
				}
				if answer.status != 0 {
					endpoint.push(answer.status, answer.body)
				}
				before := len(endpoint.requestsSoFar())
				e := rig.client.exp
				deadline := rig.client.clock.Now().Add(time.Minute).UnixMilli()
				e.mu.Lock()
				e.consentRaceSeam = func(got string) {
					if got != stage {
						return
					}
					if handle {
						if err := subject.SetConsent(ExperimentSubjectConsentDenied); err != nil {
							t.Fatal(err)
						}
					} else {
						rig.client.SetConsent(false)
					}
					// This is newer shared pacing knowledge, installed after the
					// old response completed and before its classification.
					e.mu.Lock()
					e.backoffAttempt, e.retryAfterMS = 7, deadline
					e.mu.Unlock()
				}
				e.mu.Unlock()
				rig.client.experimentCycle(context.Background())
				e.mu.Lock()
				e.consentRaceSeam = nil
				attempt, retry, blocked := e.backoffAttempt, e.retryAfterMS, e.applyBlocked
				sentinel, refetches := e.sentinelEpoch.Load(), len(e.refetchAfterApply)
				e.mu.Unlock()
				if len(endpoint.requestsSoFar()) != before+1 {
					t.Fatal("the actual response must reach the withdrawal boundary")
				}
				if attempt != 7 || retry != deadline || blocked || sentinel != 0 || refetches != 0 {
					t.Errorf("withdrawn answer changed newer plane state: attempt=%d retry=%d want=%d blocked=%v sentinel=%d refetches=%d", attempt, retry, deadline, blocked, sentinel, refetches)
				}
				stats := rig.client.Snapshot()
				if stats.ExperimentExposureDrops[expDropApplyRefused]+stats.ExperimentOutcomeDrops[expDropApplyRefused]+stats.ExperimentExposureDrops[expDropAgeIneligible]+stats.ExperimentOutcomeDrops[expDropAgeIneligible] != 0 {
					t.Errorf("a withdrawn answer counted its already retired application again: exposure=%v outcome=%v", stats.ExperimentExposureDrops, stats.ExperimentOutcomeDrops)
				}
			})
		}
	}
}

func TestWithdrawalCountsQueuedPointApplicationsOnce(t *testing.T) {
	for _, durable := range []bool{false, true} {
		for _, settled := range []bool{false, true} {
			t.Run("durable="+strconv.FormatBool(durable)+"/settled="+strconv.FormatBool(settled), func(t *testing.T) {
				dir := ""
				if durable {
					dir = t.TempDir()
				}
				rig := newAgeWithdrawRig(t, dir, nil, ageGolden(t, "adult"))
				fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
				rig.client.ApplyExperimentVariant(ageGoldenExperiment)
				rig.client.experimentCycle(context.Background())
				flushOrFail(t, rig.client)
				if len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)) != 1 {
					t.Fatal("the automatic exposure must really have settled")
				}
				if err := rig.client.TrackExperimentExposure(ageGoldenExperiment); err != nil {
					t.Fatal(err)
				}
				if err := rig.client.TrackExperimentOutcome(ageGoldenExperiment, "score", 1); err != nil {
					t.Fatal(err)
				}
				rig.client.experimentCycle(context.Background())
				if len(rig.script.apply.requestsSoFar()) != 2 || len(rig.script.outcome.requestsSoFar()) != 1 {
					t.Fatal("the two actual point applications must seal")
				}
				if rig.client.owedExperimentExposureCount() != 0 {
					t.Fatal("the point applications must already have left the owed queue")
				}
				if settled {
					flushOrFail(t, rig.client)
				}
				rig.client.SetConsent(false)
				_ = rig.client.Flush(context.Background()) // settle the worker-held denial too
				want := uint64(1)
				if settled {
					want = 0
				}
				stats := rig.client.Snapshot()
				if stats.ExperimentExposureDrops[expDropConsentWithdrawn] != want || stats.ExperimentOutcomeDrops[expDropConsentWithdrawn] != want {
					t.Errorf("withdrawal omitted queued point applications or recounted settled ones: exposure=%v outcome=%v want=%d", stats.ExperimentExposureDrops, stats.ExperimentOutcomeDrops, want)
				}
				if rig.client.owedExperimentExposureCount() != 1 {
					t.Fatal("only the automatic application may re-arm")
				}
			})
		}
	}
}

func TestApplicationHandoffSharesItsOriginalDeliveryClock(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	rig.client.exp.mu.Lock()
	delivery := rig.client.exp.pendingExposure[ageGoldenExperiment][0].intentDelivery
	rig.client.exp.mu.Unlock()
	if delivery == nil {
		t.Fatal("the admitted durable application must have its intake clock")
	}
	rig.client.experimentCycle(context.Background())
	flushOrFail(t, rig.client)
	if len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)) != 1 {
		t.Fatal("the actual transport must have attempted the fact")
	}
	if delivery.ambiguousMS.Load() == 0 {
		t.Error("queue admission replaced the durable application's delivery clock")
	}
}

func TestApplicationStorageFailuresHoldTheActualWire(t *testing.T) {
	for _, atAdmission := range []bool{false, true} {
		t.Run("admission="+strconv.FormatBool(atAdmission), func(t *testing.T) {
			rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			var faults atomic.Int32
			fail := func(string, string) error { faults.Add(1); return errors.New("synthetic disk failure") }
			if atAdmission {
				rig.client.spool.mu.Lock()
				rig.client.spool.renameFn = fail
				rig.client.spool.mu.Unlock()
			}
			rig.client.ApplyExperimentVariant(ageGoldenExperiment)
			rig.client.exp.mu.Lock()
			id := rig.client.exp.pendingExposure[ageGoldenExperiment][0].app.exposureID
			rig.client.exp.mu.Unlock()
			if !atAdmission {
				rig.client.spool.mu.Lock()
				rig.client.spool.renameFn = fail
				rig.client.spool.mu.Unlock()
			}
			rig.client.experimentCycle(context.Background())
			if faults.Load() == 0 {
				t.Fatal("the actual durable write must reach the failing primitive")
			}
			if rig.client.owedExperimentExposureCount() != 1 {
				t.Fatal("the failed durable write must keep the actual application owed")
			}
			if len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)) != 0 {
				t.Fatal("an unconfirmed durable write must never publish")
			}
			if atAdmission && len(rig.script.apply.requestsSoFar()) != 0 {
				t.Error("the unpersisted application reached the apply wire")
			}
			rig.client.spool.mu.Lock()
			rig.client.spool.renameFn = os.Rename
			rig.client.spool.mu.Unlock()
			rig.client.experimentCycle(context.Background())
			flushOrFail(t, rig.client)
			if len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)) != 1 {
				t.Fatal("repairing the primitive must deliver the held application exactly once")
			}
			requests := rig.script.apply.requestsSoFar()
			if len(requests) != 1 || requests[0].body["exposure_id"] != id {
				t.Error("disk failure reminted or resent the application")
			}
		})
	}
}

func TestPointWithdrawalAtTheEnqueueBoundaryCountsOnce(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(strconv.FormatBool(durable), func(t *testing.T) {
			dir := ""
			if durable {
				dir = t.TempDir()
			}
			rig := newAgeWithdrawRig(t, dir, nil, ageGolden(t, "adult"))
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.client.ApplyExperimentVariant(ageGoldenExperiment)
			rig.client.experimentCycle(context.Background())
			flushOrFail(t, rig.client)
			if err := rig.client.TrackExperimentExposure(ageGoldenExperiment); err != nil {
				t.Fatal(err)
			}
			if err := rig.client.TrackExperimentOutcome(ageGoldenExperiment, "score", 1); err != nil {
				t.Fatal(err)
			}
			e := rig.client.exp
			e.mu.Lock()
			e.consentRaceSeam = func(stage string) {
				if stage == "exposure_enqueued" {
					rig.client.SetConsent(false)
				}
			}
			e.mu.Unlock()
			rig.client.experimentCycle(context.Background())
			e.mu.Lock()
			e.consentRaceSeam = nil
			e.mu.Unlock()
			_ = rig.client.Flush(context.Background())
			stats := rig.client.Snapshot()
			if stats.ExperimentExposureDrops[expDropConsentWithdrawn] != 1 || stats.ExperimentOutcomeDrops[expDropConsentWithdrawn] != 1 {
				t.Fatalf("handoff and owed copies counted twice: exposure=%v outcome=%v", stats.ExperimentExposureDrops, stats.ExperimentOutcomeDrops)
			}
			if len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)) != 1 {
				t.Fatal("only the settled automatic control may reach ingest")
			}
		})
	}
}

func TestOnlyRetainedApplicationsMayReachTheApplyWire(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run("expired="+strconv.FormatBool(expired), func(t *testing.T) {
			rig := newAgeWithdrawRig(t, t.TempDir(), func(cfg *Config) {
				if !expired {
					cfg.SpoolMaxBytes = 1
				}
			}, ageGolden(t, "adult"))
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			if variant, _ := rig.client.ApplyExperimentVariant(ageGoldenExperiment); variant != "control" {
				t.Fatal("storage limits must preserve documented variant serving")
			}
			if expired {
				rig.client.exp.mu.Lock()
				rig.client.exp.pendingExposure[ageGoldenExperiment][0].intentDelivery.intakeMS = time.Now().Add(-8 * 24 * time.Hour).UnixMilli()
				rig.client.exp.mu.Unlock()
			}
			rig.client.experimentCycle(context.Background())
			if len(rig.script.apply.requestsSoFar()) != 0 {
				t.Error("an application outside the durable retention bounds reached the apply wire")
			}
			if !expired && storedApplicationCount(t, rig.spool) != 0 {
				t.Fatal("the actual byte cap must evict this application")
			}
		})
	}
}

func TestApplicationStateDoesNotCallHostCodeUnderThePlaneLock(t *testing.T) {
	var observed atomic.Pointer[Client]
	var lockedCalls atomic.Int32
	rig := newAgeWithdrawRig(t, t.TempDir(), func(cfg *Config) {
		cfg.Logger = rejectionLoggerFunc(func(string, ...any) {
			client := observed.Load()
			if client == nil {
				return
			}
			if !client.exp.mu.TryLock() {
				lockedCalls.Add(1)
				return
			}
			client.exp.mu.Unlock()
		})
	}, ageGolden(t, "adult"))
	observed.Store(rig.client)
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.spool.mu.Lock()
	rig.client.spool.renameFn = func(string, string) error { return errors.New("synthetic disk failure") }
	rig.client.spool.mu.Unlock()
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	rig.client.experimentCycle(context.Background())
	if rig.client.Snapshot().SpoolPersistFailed == 0 {
		t.Fatal("the actual spool write must fail")
	}
	if lockedCalls.Load() != 0 {
		t.Error("application persistence invoked the host logger while holding the plane lock")
	}
	rig.client.spool.mu.Lock()
	rig.client.spool.renameFn = os.Rename
	rig.client.spool.mu.Unlock()
}

func TestLocalApplicationCannotUseTheAnalyticsTransport(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	rig.client.spool.mu.Lock()
	entry := rig.client.spool.entries[0]
	rig.client.spool.mu.Unlock()
	if !entry.application {
		t.Fatal("the subject must be the actual retained local application")
	}
	request := batchRequest{rawEvents: []json.RawMessage{entry.raw}, retryEntries: []spoolEntry{entry}}
	_, err := rig.client.publishRequestResult(context.Background(), request, 1)
	if !errors.Is(err, errLocalExperimentApplication) {
		t.Fatalf("local application was admitted to the transport: %v", err)
	}
	if len(rig.capture.rawBatches()) != 0 {
		t.Fatal("local subject state reached the actual analytics endpoint")
	}
}

func TestApplicationCapsDoNotExposeLocalSubjectState(t *testing.T) {
	var letters atomic.Int32
	var leaked atomic.Bool
	rig := newAgeWithdrawRig(t, t.TempDir(), func(cfg *Config) {
		cfg.SpoolMaxEvents = 1
		cfg.OnSpoolDeadLetter = func(letter SpoolDeadLetter) {
			if letter.Reason != SpoolDropCapacity {
				return
			}
			letters.Add(1)
			for _, envelope := range letter.Envelopes {
				if len(envelope) != 0 {
					leaked.Store(true)
				}
			}
		}
	}, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	rig.client.drainDeferredSpoolLetters()
	if storedApplicationCount(t, rig.spool) != 1 || rig.client.Snapshot().SpoolEvicted != 1 || letters.Load() != 1 {
		t.Fatal("the actual count cap and capacity callback must execute")
	}
	if leaked.Load() {
		t.Fatal("the capacity callback exposed an unsealed application's subject state")
	}
}

// Fail after the real apply response, before its durable sealed handoff.
// Failing after enqueue no longer stages capture debt: the handoff is then
// already durable, even when the analytics queue is full.
func failApplicationHandoffStorage(client *Client) {
	client.exp.mu.Lock()
	client.exp.consentRaceSeam = func(stage string) {
		if stage != "apply_answer" {
			return
		}
		client.exp.mu.Lock()
		client.exp.consentRaceSeam = nil
		client.exp.mu.Unlock()
		client.spool.mu.Lock()
		client.spool.renameFn = func(old, next string) error {
			if strings.HasSuffix(next, spoolFileName) {
				return errors.New("synthetic handoff disk failure")
			}
			return os.Rename(old, next)
		}
		client.spool.appendFn = func(path string, payload []byte) error {
			if strings.HasSuffix(path, spoolFileName) {
				return errors.New("synthetic capture disk failure")
			}
			return appendPrivateFile(path, payload)
		}
		client.spool.mu.Unlock()
	}
	client.exp.mu.Unlock()
}

func TestDropCaptureRetainsOnlyOneReplaySource(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	failApplicationHandoffStorage(rig.client)
	rig.client.experimentCycle(context.Background())
	if len(rig.script.apply.requestsSoFar()) != 1 || len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)) != 0 {
		t.Fatal("the actual apply must seal, and the failing handoff must block analytics")
	}
	rig.script.mu.Lock()
	rig.script.responses = nil
	rig.script.mu.Unlock()
	rig.script.push(200, ageRefusalBody(t, "kill_switch"))
	if result, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err != nil || result.Reason != "kill_switch" {
		t.Fatalf("the real kill must freeze the capture: %+v %v", result, err)
	}
	rig.client.spool.mu.Lock()
	rig.client.spool.renameFn, rig.client.spool.appendFn = os.Rename, appendPrivateFile
	rig.client.spool.mu.Unlock()
	rig.client.exp.retryDurableSync()
	image := copyApplicationImage(t, rig.spool)
	rig.client.SetConsent(false)
	_ = rig.client.Close(context.Background())
	before := len(rig.script.apply.requestsSoFar())
	restarted := rig.launch(t, func(cfg *Config) { cfg.SpoolDir = image })
	restarted.SetConsent(true)
	if got := restarted.owedExperimentExposureCount(); got != 0 {
		t.Errorf("the drop capture also restored its unsealed replay source: owed=%d", got)
	}
	rig.script.mu.Lock()
	rig.script.responses = nil
	rig.script.mu.Unlock()
	rig.script.push(200, ageGolden(t, "adult"))
	fetchAdultAssignment(t, restarted, ageGoldenExperiment)
	restarted.experimentCycle(context.Background())
	flushOrFail(t, restarted)
	if len(rig.script.apply.requestsSoFar()) != before {
		t.Error("recovery reapplied an application already sealed by drop capture")
	}
	if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != 1 {
		t.Errorf("drop-capture recovery delivered both forms: %d", got)
	}
}

func TestAgeWithdrawalBeforeDurableHandoffCannotRecreateTheFact(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	var fired atomic.Bool
	var refusalErr error
	e := rig.client.exp
	e.mu.Lock()
	e.consentRaceSeam = func(stage string) {
		if stage != "exposure_build" || !fired.CompareAndSwap(false, true) {
			return
		}
		_, refusalErr = rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
	}
	e.mu.Unlock()
	rig.client.experimentCycle(context.Background())
	e.mu.Lock()
	e.consentRaceSeam = nil
	e.mu.Unlock()
	if !fired.Load() || refusalErr != nil || len(rig.script.apply.requestsSoFar()) != 1 {
		t.Fatalf("the actual sealed application must cross an actual age refusal: fired=%v err=%v", fired.Load(), refusalErr)
	}
	if got := spooledExperimentFacts(rig.client, ageGoldenExperiment); got != 0 {
		t.Errorf("a post-withdrawal handoff recreated a durable fact: %d", got)
	}
	image := copyApplicationImage(t, rig.spool)
	rig.client.SetConsent(false)
	_ = rig.client.Close(context.Background())
	restarted := rig.launch(t, func(cfg *Config) { cfg.SpoolDir = image })
	restarted.SetConsent(true)
	rig.script.mu.Lock()
	rig.script.responses = nil
	rig.script.mu.Unlock()
	rig.script.push(200, ageGolden(t, "adult"))
	fetchAdultAssignment(t, restarted, ageGoldenExperiment)
	flushOrFail(t, restarted)
	if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != 0 {
		t.Errorf("restart released a fact recreated after its age withdrawal: %d", got)
	}
}

func TestAnOversizeSealDoesNotHoldTheDurableApplication(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(strconv.FormatBool(durable), func(t *testing.T) {
			dir := ""
			if durable {
				dir = t.TempDir()
			}
			rig := newAgeWithdrawRig(t, dir, nil, ageGolden(t, "adult"))
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.script.apply.padProps = 3 << 10
			rig.client.ApplyExperimentVariant(ageGoldenExperiment)
			rig.client.experimentCycle(context.Background())
			if len(rig.script.apply.requestsSoFar()) != 1 {
				t.Fatal("the actual endpoint must return its oversized sealed fact")
			}
			if rig.client.owedExperimentExposureCount() != 0 || rig.client.Snapshot().ExperimentExposureDrops[expDropEventTooLarge] != 1 {
				t.Error("the oversize seal kept its application owed instead of the terminal size disposition")
			}
			if durable && storedApplicationCount(t, dir) != 0 {
				t.Error("the terminal oversize application remained replayable")
			}
			if len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)) != 0 {
				t.Fatal("the oversized fact reached analytics")
			}
		})
	}
}

func TestSealedCapacityEvictionIsNotAnIOFailure(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	rig.script.apply.padProps = 512
	rig.client.spool.mu.Lock()
	limit := len(rig.client.spool.entries[0].raw) + 1
	rig.client.spool.maxBytes = limit
	rig.client.spool.mu.Unlock()
	rig.client.experimentCycle(context.Background())
	rig.client.exp.mu.Lock()
	records := rig.client.exp.pendingExposure[ageGoldenExperiment]
	if len(records) != 1 || records[0].intentEvent == nil {
		rig.client.exp.mu.Unlock()
		t.Fatal("the actual application must seal and remain pending at its capacity limit")
	}
	event := *records[0].intentEvent
	rig.client.exp.mu.Unlock()
	request, err := rig.client.buildBatch([]Event{event})
	if err != nil || len(request.rawEvents) != 1 {
		t.Fatalf("the sealed control must build: %v", err)
	}
	if n := len(request.rawEvents[0]); n <= limit || n > 2048 {
		t.Fatalf("the real sealed envelope must exceed only the configured capacity: size=%d limit=%d", n, limit)
	}
	_ = rig.client.Flush(context.Background())
	stats := rig.client.Snapshot()
	if stats.SpoolEvicted == 0 {
		t.Fatal("the actual capacity eviction must run")
	}
	if stats.SpoolPersistFailed != 0 {
		t.Errorf("a successful capacity rewrite was reported as an IO failure: %d", stats.SpoolPersistFailed)
	}
}
