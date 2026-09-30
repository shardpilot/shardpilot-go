package shardpilot

// An age_ineligible refusal withdraws what the refused subject still owes
// for the experiment: the owed exposures and outcomes, their facts already
// in the analytics pipeline (queue, worker batches, spool), and the
// application a new outcome would follow. Every other not-assigned reason
// keeps the owed applications: an application that already happened is a
// fact. Synthetic data only: the captured age goldens and the in-process
// apply stubs.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ageGoldenFactKey is the subject fact key the captured age goldens carry,
// read from the golden itself.
func ageGoldenFactKey(t *testing.T) string {
	t.Helper()
	var golden struct {
		SubjectFactKey string `json:"subject_fact_key"`
	}
	if err := json.Unmarshal([]byte(ageGolden(t, "adult")), &golden); err != nil || !expSubjectFactKeyPattern.MatchString(golden.SubjectFactKey) {
		t.Fatalf("the adult golden must carry a subject fact key: %q %v", golden.SubjectFactKey, err)
	}
	return golden.SubjectFactKey
}

// ageApplyRefusal is the apply routes' age refusal.
const ageApplyRefusal = `{"error":"not_assigned","reason":"age_ineligible"}`

// ageRefusalBody is the captured under_threshold refusal with its reason
// replaced; an empty reason removes the member (the traffic-gate shape).
func ageRefusalBody(t *testing.T, reason string) string {
	t.Helper()
	body := ageGolden(t, "under_threshold")
	if reason == "" {
		return strings.Replace(body, `"reason":"age_ineligible",`, "", 1)
	}
	return strings.Replace(body, `"age_ineligible"`, `"`+reason+`"`, 1)
}

// ageGoldenForKey is a golden answering for another experiment key.
func ageGoldenForKey(t *testing.T, name, key string) string {
	t.Helper()
	return strings.Replace(ageGolden(t, name), `"experiment_key":"`+ageGoldenExperiment+`"`, `"experiment_key":"`+key+`"`, 1)
}

type ageWithdrawRig struct {
	script  *expScript
	capture *expWireCapture
	server  *httptest.Server
	spool   string
	client  *Client
}

// newAgeWithdrawRig is a granted client against the stub routes, the
// assignment route answering bodies in order. The apply stubs seal each
// fact with the subject fact key the goldens name, as the platform does.
func newAgeWithdrawRig(t *testing.T, spool string, mutate func(*Config), bodies ...string) *ageWithdrawRig {
	t.Helper()
	script := &expScript{}
	for _, body := range bodies {
		script.push(200, body)
	}
	script.apply.assignmentKey = ageGoldenFactKey(t)
	script.outcome.assignmentKey = ageGoldenFactKey(t)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	t.Cleanup(server.Close)
	rig := &ageWithdrawRig{script: script, capture: capture, server: server, spool: spool}
	rig.client = rig.launch(t, mutate)
	rig.client.SetConsent(true)
	return rig
}

// launch constructs a client on the rig's server and spool directory.
func (r *ageWithdrawRig) launch(t *testing.T, mutate func(*Config)) *Client {
	t.Helper()
	client := newExperimentClient(t, r.server.URL, func(cfg *Config) {
		cfg.AppID = "exposure-app"
		cfg.SpoolDir = r.spool
		if mutate != nil {
			mutate(cfg)
		}
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Close(ctx)
	})
	return client
}

// applyAndMeasure is the host applying the served variant and recording
// one outcome against that application: two owed applications.
func (r *ageWithdrawRig) applyAndMeasure(t *testing.T, key string) {
	t.Helper()
	if variant, _ := r.client.ApplyExperimentVariant(key); variant != "control" {
		t.Fatalf("setup: apply served %q for %q", variant, key)
	}
	if err := r.client.TrackExperimentOutcome(key, "score", 1); err != nil {
		t.Fatalf("setup: outcome for %q: %v", key, err)
	}
}

func (r *ageWithdrawRig) refuse(t *testing.T, reason string) {
	t.Helper()
	result, err := r.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
	if err != nil || result.Assigned || result.Reason != reason {
		t.Fatalf("setup: the refusal must land as %q, got %+v err=%v", reason, result, err)
	}
}

func (r *ageWithdrawRig) applyRequests() (exposures, outcomes int) {
	return len(r.script.apply.requestsSoFar()), len(r.script.outcome.requestsSoFar())
}

func (r *ageWithdrawRig) drops(reason string) (exposures, outcomes uint64) {
	stats := r.client.Snapshot()
	return stats.ExperimentExposureDrops[reason], stats.ExperimentOutcomeDrops[reason]
}

// deliveredExperimentFacts are the exposure and outcome facts of the
// experiment the analytics ingest accepted.
func deliveredExperimentFacts(capture *expWireCapture, experimentKey string) []map[string]any {
	var out []map[string]any
	for _, name := range []string{experimentExposureName, experimentOutcomeName} {
		for _, envelope := range capture.byName(name) {
			if props, _ := envelope["props"].(map[string]any); props["experiment_key"] == experimentKey {
				out = append(out, envelope)
			}
		}
	}
	return out
}

// spooledExperimentFacts counts the exposure and outcome facts of the
// experiment in the client's spool.
func spooledExperimentFacts(c *Client, experimentKey string) int {
	if c.spool == nil {
		return 0
	}
	c.spool.mu.Lock()
	defer c.spool.mu.Unlock()
	count := 0
	for _, entry := range c.spool.entries {
		var wire struct {
			EventName string         `json:"event_name"`
			Props     map[string]any `json:"props"`
		}
		if json.Unmarshal(entry.raw, &wire) != nil {
			continue
		}
		if (wire.EventName == experimentExposureName || wire.EventName == experimentOutcomeName) && wire.Props["experiment_key"] == experimentKey {
			count++
		}
	}
	return count
}

// durableRecordHolds reports whether the durable assignment record on disk
// still holds an entry for the experiment. (Its fact-key history may name
// the experiment without one.)
func durableRecordHolds(t *testing.T, dir, experimentKey string) bool {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, expCacheFileName))
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	if err != nil {
		t.Fatalf("reading the durable record: %v", err)
	}
	var record struct {
		Entries map[string]json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decoding the durable record: %v", err)
	}
	_, held := record.Entries[experimentKey]
	return held
}

// ── the assignment route ────────────────────────────────────────────────────

// The captured scene: an adult assignment is applied and measured, then the
// platform refuses the subject as age_ineligible. Nothing is served, nothing
// owed survives, a new outcome is refused, and after the lane's cycle and a
// flush no apply request was sent and no fact of the experiment delivered.
func TestAnAgeRefusalWithdrawsTheOwedApplications(t *testing.T) {
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	if owed := rig.client.owedExperimentExposureCount(); owed != 2 {
		t.Fatalf("setup: %d owed application(s), want the exposure and the outcome", owed)
	}

	rig.refuse(t, "age_ineligible")
	if variant := rig.client.ExperimentVariant(ageGoldenExperiment); variant != "" {
		t.Errorf("the refused assignment is still served: %q", variant)
	}
	if owed := rig.client.owedExperimentExposureCount(); owed != 0 {
		t.Errorf("%d owed application(s) survive the age refusal", owed)
	}
	if exposures, outcomes := rig.drops("age_ineligible"); exposures != 1 || outcomes != 1 {
		t.Errorf("the withdrawn applications must be counted age_ineligible: exposure=%d outcome=%d", exposures, outcomes)
	}
	if err := rig.client.TrackExperimentOutcome(ageGoldenExperiment, "score", 2); !errors.Is(err, ErrExperimentNoAssignment) {
		t.Errorf("a new outcome on the withdrawn application must be refused locally with ErrExperimentNoAssignment, got %v", err)
	}
	if err := rig.client.TrackExperimentExposure(ageGoldenExperiment); !errors.Is(err, ErrExperimentNoAssignment) {
		t.Errorf("a new exposure of the refused assignment must be refused, got %v", err)
	}

	rig.client.experimentCycle(context.Background())
	if exposures, outcomes := rig.applyRequests(); exposures+outcomes != 0 {
		t.Errorf("after the age refusal the lane still sent %d exposure and %d outcome apply request(s)", exposures, outcomes)
	}
	flushOrFail(t, rig.client)
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 0 {
		t.Errorf("after the age refusal %d fact(s) of the experiment were delivered", len(delivered))
	}
	if durableRecordHolds(t, spool, ageGoldenExperiment) {
		t.Errorf("the refused assignment is still in the durable record")
	}
}

// Every other not-assigned reason keeps today's behaviour: the drop stops
// serving, and the applications that already happened are still recorded.
func TestOtherNotAssignedReasonsKeepTheOwedApplications(t *testing.T) {
	for _, reason := range []string{"kill_switch", "targeting_unmatched", ""} {
		name := reason
		if name == "" {
			name = "traffic_gate"
		}
		t.Run(name, func(t *testing.T) {
			rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, reason))
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)

			rig.refuse(t, reason)
			if variant := rig.client.ExperimentVariant(ageGoldenExperiment); variant != "" {
				t.Errorf("control: the dropped assignment is still served: %q", variant)
			}
			if owed := rig.client.owedExperimentExposureCount(); owed != 2 {
				t.Errorf("%d owed application(s) after %s, want both kept", owed, name)
			}
			if err := rig.client.TrackExperimentOutcome(ageGoldenExperiment, "score", 2); err != nil {
				t.Errorf("an outcome still follows the last application after %s: %v", name, err)
			}
			rig.client.experimentCycle(context.Background())
			if exposures, outcomes := rig.applyRequests(); exposures != 1 || outcomes != 2 {
				t.Errorf("after %s the lane must send the owed applications: exposure=%d outcome=%d, want 1 and 2", name, exposures, outcomes)
			}
			flushOrFail(t, rig.client)
			if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 3 {
				t.Errorf("after %s %d fact(s) delivered, want the exposure and both outcomes", name, len(delivered))
			}
			if exposures, outcomes := rig.drops("age_ineligible"); exposures+outcomes != 0 {
				t.Errorf("%s must not count age_ineligible drops: exposure=%d outcome=%d", name, exposures, outcomes)
			}
		})
	}
}

// The refusal withdraws one experiment: a second experiment's owed
// applications and its facts already in the pipeline — sealed with the same
// subject fact key — are still delivered.
func TestAnAgeRefusalLeavesOtherExperimentsDelivering(t *testing.T) {
	const other = "exposure-banner-2"
	rig := newAgeWithdrawRig(t, t.TempDir(), nil,
		ageGolden(t, "adult"), ageGoldenForKey(t, "adult", other), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	fetchAdultAssignment(t, rig.client, other)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	rig.applyAndMeasure(t, other)
	// Both experiments' applications are sealed and handed to the queue;
	// the worker takes them into its held batch (BatchSize 8: no publish).
	rig.client.experimentCycle(context.Background())
	waitFor(t, 5*time.Second, "the worker holds the sealed facts", func() bool { return len(rig.client.queue.ch) == 0 })
	// And more is owed for both.
	if err := rig.client.TrackExperimentOutcome(ageGoldenExperiment, "score", 2); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := rig.client.TrackExperimentExposure(other); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := rig.client.TrackExperimentOutcome(other, "score", 2); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if owed := rig.client.owedExperimentExposureCount(); owed != 3 {
		t.Fatalf("setup: %d owed application(s), want 3", owed)
	}

	rig.refuse(t, "age_ineligible")
	if variant := rig.client.ExperimentVariant(other); variant != "control" {
		t.Errorf("the other experiment's assignment must still be served, got %q", variant)
	}
	if owed := rig.client.owedExperimentExposureCount(); owed != 2 {
		t.Errorf("%d owed application(s) after the refusal, want the other experiment's 2", owed)
	}
	if exposures, outcomes := rig.drops("age_ineligible"); exposures != 0 || outcomes != 1 {
		t.Errorf("only the refused experiment's owed outcome is withdrawn: exposure=%d outcome=%d", exposures, outcomes)
	}
	if err := rig.client.TrackExperimentOutcome(other, "score", 3); err != nil {
		t.Errorf("the other experiment still takes outcomes: %v", err)
	}
	rig.client.experimentCycle(context.Background())
	flushOrFail(t, rig.client)
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 0 {
		t.Errorf("%d fact(s) of the refused experiment delivered (queued before the refusal or owed)", len(delivered))
	}
	if delivered := deliveredExperimentFacts(rig.capture, other); len(delivered) != 5 {
		t.Errorf("the other experiment delivered %d fact(s), want its 2 exposures and 3 outcomes", len(delivered))
	}
}

// Facts are matched by the refused subject's fact key too: a fact of the
// same experiment sealed under another subject fact key is not withdrawn.
func TestAnAgeRefusalMatchesTheSubjectFactKey(t *testing.T) {
	otherSubjectKey := "sfk1_" + strings.Repeat("c", 64)
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.script.apply.mu.Lock()
	rig.script.apply.assignmentKey = otherSubjectKey
	rig.script.apply.mu.Unlock()
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	rig.client.experimentCycle(context.Background())
	rig.script.apply.mu.Lock()
	rig.script.apply.assignmentKey = ageGoldenFactKey(t)
	rig.script.apply.mu.Unlock()
	if err := rig.client.TrackExperimentExposure(ageGoldenExperiment); err != nil {
		t.Fatalf("setup: %v", err)
	}
	rig.client.experimentCycle(context.Background())
	if exposures, _ := rig.applyRequests(); exposures != 2 {
		t.Fatalf("setup: %d exposure apply request(s), want 2", exposures)
	}

	rig.refuse(t, "age_ineligible")
	flushOrFail(t, rig.client)
	delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment)
	if len(delivered) != 1 {
		t.Fatalf("%d fact(s) delivered, want only the one sealed under another subject fact key", len(delivered))
	}
	if props, _ := delivered[0]["props"].(map[string]any); props["assignment_key"] != otherSubjectKey {
		t.Errorf("the delivered fact must be the other subject's, got %v", props["assignment_key"])
	}
}

// ageRefusalBodyWithoutFactKey is the captured age refusal with its
// subject_fact_key member removed.
func ageRefusalBodyWithoutFactKey(t *testing.T) string {
	t.Helper()
	body := ageRefusalBody(t, "age_ineligible")
	stripped := strings.Replace(body, `"subject_fact_key":"`+ageGoldenFactKey(t)+`",`, "", 1)
	if strings.Contains(stripped, `"subject_fact_key":`) {
		t.Fatalf("setup: the refusal golden must lose its subject_fact_key member")
	}
	return stripped
}

// A refusal that names no subject fact key, for a subject that has no local
// record to supply one, withdraws nothing from the pipeline: there, the
// refused subject's facts cannot be told apart from another subject's facts
// of the same experiment. The other subject is the one before a subject
// re-mint: its application and outcome are sealed and accepted (held by the
// worker, or spooled by a failed publish), then the platform rejects the
// persisted subject id's grammar, the SDK re-mints (the previous subject's
// facts are deliberately kept) and retries, and the retry is refused as
// age_ineligible without a subject_fact_key. The new subject has no
// assignment, owed application or last application to take a key from.
// The previous subject's facts stay in the pipeline and are delivered.
func TestAnAgeRefusalWithoutAFactKeyLeavesAnotherSubjectsFacts(t *testing.T) {
	for _, staging := range []string{"held_by_the_worker", "spooled"} {
		t.Run(staging, func(t *testing.T) {
			ctx := context.Background()
			rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
			rig.script.push(http.StatusBadRequest, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`)
			rig.script.push(http.StatusOK, ageRefusalBodyWithoutFactKey(t))
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)
			if staging == "spooled" {
				rig.capture.setStatus(http.StatusInternalServerError)
			}
			rig.client.experimentCycle(ctx)
			if staging == "spooled" {
				if err := rig.client.Flush(ctx); err == nil {
					t.Fatalf("setup: the flush must fail against the failing ingest")
				}
				if spooled := spooledExperimentFacts(rig.client, ageGoldenExperiment); spooled != 2 {
					t.Fatalf("setup: %d fact(s) spooled by the failed publish, want 2", spooled)
				}
			} else {
				waitFor(t, 5*time.Second, "the worker holds the sealed facts", func() bool { return len(rig.client.queue.ch) == 0 })
			}
			subject := func() string {
				rig.client.exp.mu.Lock()
				defer rig.client.exp.mu.Unlock()
				return rig.client.exp.currentSubjectIDLocked()
			}
			previousSubject := subject()

			result, err := rig.client.FetchExperimentAssignmentWithAgeBand(ctx, ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
			if err != nil || result.Assigned || result.Reason != "age_ineligible" {
				t.Fatalf("setup: the re-minted retry must be refused as age_ineligible, got %+v err=%v", result, err)
			}
			if fetches := rig.script.requestCount(); fetches != 3 || subject() == previousSubject {
				t.Fatalf("setup: the grammar reject must re-mint the subject and retry (%d fetches)", fetches)
			}

			if staging == "spooled" {
				if spooled := spooledExperimentFacts(rig.client, ageGoldenExperiment); spooled != 2 {
					t.Errorf("a refusal without a subject fact key withdrew %d of the previous subject's 2 spooled fact(s)", 2-spooled)
				}
				rig.capture.setStatus(http.StatusAccepted)
			}
			flushOrFail(t, rig.client)
			delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment)
			if len(delivered) != 2 {
				t.Errorf("the previous subject's facts must still be delivered: %d of 2 delivered", len(delivered))
			}
			for _, envelope := range delivered {
				if props, _ := envelope["props"].(map[string]any); props["assignment_key"] != ageGoldenFactKey(t) {
					t.Errorf("a delivered fact is not the previous subject's: assignment_key %v", props["assignment_key"])
				}
			}
		})
	}
}

// The facts owed or queued before the refusal leave no durable copy: the
// refusal writes none, a copy spooled before it is withdrawn from the
// spool, and a relaunch on the same spool delivers nothing of them. Under a
// kill switch the same copies are kept (the drop-time capture included) and
// the relaunch delivers them.
func TestAnAgeRefusalLeavesNoDurableCopyToReplay(t *testing.T) {
	type staging struct {
		name   string
		config func(*Config)
		// stage leaves the experiment's sealed facts either spooled by a
		// failed publish or owed in memory under a full queue, with the
		// ingest failing; it returns how many sealed facts it left.
		stage func(t *testing.T, rig *ageWithdrawRig) int
		// captured is how many facts the kill switch's drop-time capture
		// adds to the spool.
		captured int
	}
	stagings := []staging{
		{
			name: "spooled_before_the_refusal",
			stage: func(t *testing.T, rig *ageWithdrawRig) int {
				rig.capture.setStatus(http.StatusInternalServerError)
				rig.client.experimentCycle(context.Background())
				if err := rig.client.Flush(context.Background()); err == nil {
					t.Fatalf("setup: the flush must fail against the failing ingest")
				}
				if spooled := spooledExperimentFacts(rig.client, ageGoldenExperiment); spooled != 2 {
					t.Fatalf("setup: %d fact(s) spooled by the failed publish, want 2", spooled)
				}
				return 2
			},
		},
		{
			name:   "owed_at_the_refusal",
			config: func(cfg *Config) { cfg.BatchSize, cfg.BufferSize = 1, 1 },
			stage: func(t *testing.T, rig *ageWithdrawRig) int {
				parkWorkerWithFullQueue(t, rig.client, rig.capture)
				// The head application is sealed; the full queue keeps it
				// owed, and the outcome behind it waits unsealed.
				rig.client.experimentCycle(context.Background())
				if exposures, _ := rig.applyRequests(); exposures != 1 {
					t.Fatalf("setup: %d exposure apply request(s), want 1", exposures)
				}
				if owed := rig.client.owedExperimentExposureCount(); owed != 2 {
					t.Fatalf("setup: %d owed application(s), want 2", owed)
				}
				return 1
			},
			captured: 1,
		},
	}
	for _, stage := range stagings {
		for _, reason := range []string{"age_ineligible", "kill_switch"} {
			t.Run(stage.name+"/"+reason, func(t *testing.T) {
				spool := t.TempDir()
				rig := newAgeWithdrawRig(t, spool, stage.config, ageGolden(t, "adult"), ageRefusalBody(t, reason))
				fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
				rig.applyAndMeasure(t, ageGoldenExperiment)
				sealed := stage.stage(t, rig)
				spooledBefore := spooledExperimentFacts(rig.client, ageGoldenExperiment)

				rig.refuse(t, reason)
				spooled := spooledExperimentFacts(rig.client, ageGoldenExperiment)
				if reason == "age_ineligible" && spooled != 0 {
					t.Errorf("%d fact(s) of the refused experiment remain spooled after the age refusal", spooled)
				}
				if reason == "kill_switch" && spooled != spooledBefore+stage.captured {
					t.Errorf("control: the kill switch keeps the spooled facts and captures the owed ones: %d spooled, want %d", spooled, spooledBefore+stage.captured)
				}

				// Exit with the ingest still down, then relaunch on the same
				// spool with the ingest healthy.
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = rig.client.Close(ctx)
				cancel()
				rig.capture.setStatus(http.StatusAccepted)
				relaunched := rig.launch(t, stage.config)
				flushOrFail(t, relaunched)
				delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment)
				if reason == "age_ineligible" && len(delivered) != 0 {
					t.Errorf("the relaunch replayed %d fact(s) of the refused experiment", len(delivered))
				}
				if reason == "kill_switch" && len(delivered) != sealed {
					t.Errorf("control: the relaunch must replay the kill-dropped experiment's %d durable fact(s), got %d", sealed, len(delivered))
				}
			})
		}
	}
}

// A pulled spool chunk is re-checked at its transport handoff: a refusal
// landing after the pull keeps the refused experiment's restored facts off
// the wire.
func TestAnAgeRefusalWithholdsAPulledSpoolChunk(t *testing.T) {
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	rig.capture.setStatus(http.StatusInternalServerError)
	rig.client.experimentCycle(context.Background())
	if err := rig.client.Flush(context.Background()); err == nil {
		t.Fatalf("setup: the flush must fail against the failing ingest")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = rig.client.Close(ctx)
	cancel()

	// The relaunch restores the declared assignment and the spooled facts;
	// the refusal lands while the resend chunk is pulled but not yet sent.
	rig.capture.setStatus(http.StatusAccepted)
	relaunched := rig.launch(t, nil)
	if variant := relaunched.ExperimentVariant(ageGoldenExperiment); variant != "control" {
		t.Fatalf("setup: the relaunch must restore the declared assignment, got %q", variant)
	}
	var refused atomic.Bool
	var refusalErr error
	relaunched.spoolResendHandoffSeam = func(chunk []spoolEntry) {
		if !refused.CompareAndSwap(false, true) {
			return
		}
		_, refusalErr = relaunched.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
	}
	flushOrFail(t, relaunched)
	if !refused.Load() || refusalErr != nil {
		t.Fatalf("setup: the refusal must land at the chunk handoff (fired=%v err=%v)", refused.Load(), refusalErr)
	}
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 0 {
		t.Errorf("a pulled chunk published %d fact(s) of the refused experiment", len(delivered))
	}
	if spooled := spooledExperimentFacts(relaunched, ageGoldenExperiment); spooled != 0 {
		t.Errorf("%d fact(s) of the refused experiment remain spooled", spooled)
	}
}

// A resend cannot publish a spooled fact the refusal withdraws, however it
// interleaves with the refusal: its generation is published in the same
// spool-lock hold as its spool sweep. The seam runs a resend the moment the
// generation is visible, whenever the spool lock is free for its pull there;
// when it is held, the resend runs after the sweep.
func TestAnAgeRefusalPublishesItsGenerationWithItsSpoolSweep(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	rig.capture.setStatus(http.StatusInternalServerError)
	rig.client.experimentCycle(context.Background())
	if err := rig.client.Flush(context.Background()); err == nil {
		t.Fatalf("setup: the flush must fail against the failing ingest")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = rig.client.Close(ctx)
	rig.capture.setStatus(http.StatusAccepted)
	rig.client = rig.launch(t, nil)
	var fired atomic.Bool
	var resendErr error
	rig.client.keyWithdrawPublishedSeam = func() {
		if !fired.CompareAndSwap(false, true) || !rig.client.spool.mu.TryLock() {
			return
		}
		rig.client.spool.mu.Unlock()
		resendErr = rig.client.Flush(ctx)
	}
	rig.refuse(t, "age_ineligible")
	if !fired.Load() || resendErr != nil {
		t.Fatalf("setup: the seam must fire (fired=%v resend err=%v)", fired.Load(), resendErr)
	}
	flushOrFail(t, rig.client)
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 0 {
		t.Errorf("a resend published %d spooled fact(s) of the refused experiment", len(delivered))
	}
}

// A fact on the wire when the refusal lands is wire-ambiguous: if that send
// fails it is neither respooled nor re-sent; if it succeeds it was
// delivered. Either way it is not counted as a withdrawn application.
func TestAnAgeRefusalNeverResendsAFactInFlight(t *testing.T) {
	for _, answer := range []int{http.StatusInternalServerError, http.StatusAccepted} {
		t.Run(strconv.Itoa(answer), func(t *testing.T) {
			script := &expScript{}
			script.push(200, ageGolden(t, "adult"))
			script.push(200, ageRefusalBody(t, "age_ineligible"))
			script.apply.assignmentKey = ageGoldenFactKey(t)
			script.outcome.assignmentKey = ageGoldenFactKey(t)
			capture := &expWireCapture{}
			var client *Client
			var once sync.Once
			var refusalErr error
			mux := http.NewServeMux()
			mux.HandleFunc(expAssignmentRoute, script.handler(t))
			mux.HandleFunc(expExposureApplyRoute, script.apply.handler(t))
			script.outcome.outcome = true
			mux.HandleFunc(expOutcomeApplyRoute, script.outcome.handler(t))
			mux.HandleFunc("/v1/consent", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte(`{}`))
			})
			ingest := capture.handler(t)
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				inFlight := false
				once.Do(func() {
					// The first batch is on the wire: the refusal lands now.
					inFlight = true
					_, refusalErr = client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
				})
				if inFlight && answer != http.StatusAccepted {
					w.WriteHeader(answer)
					return
				}
				ingest(w, r)
			})
			server := httptest.NewServer(mux)
			defer server.Close()
			rig := &ageWithdrawRig{script: script, capture: capture, server: server, spool: t.TempDir()}
			client = rig.launch(t, nil)
			rig.client = client
			client.SetConsent(true)
			fetchAdultAssignment(t, client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)
			client.experimentCycle(context.Background())

			err := client.Flush(context.Background())
			if refusalErr != nil {
				t.Fatalf("setup: the refusal: %v", refusalErr)
			}
			if answer == http.StatusAccepted {
				if err != nil {
					t.Fatalf("flush: %v", err)
				}
				if delivered := deliveredExperimentFacts(capture, ageGoldenExperiment); len(delivered) != 2 {
					t.Errorf("the batch in flight delivered %d fact(s), want both", len(delivered))
				}
			} else {
				if err == nil {
					t.Fatalf("setup: the flush in flight must fail")
				}
				if spooled := spooledExperimentFacts(client, ageGoldenExperiment); spooled != 0 {
					t.Errorf("the failed send respooled %d fact(s) the refusal withdrew", spooled)
				}
				flushOrFail(t, client)
				if delivered := deliveredExperimentFacts(capture, ageGoldenExperiment); len(delivered) != 0 {
					t.Errorf("the failed send was re-sent: %d fact(s) delivered", len(delivered))
				}
			}
			if exposures, outcomes := rig.drops("age_ineligible"); exposures+outcomes != 0 {
				t.Errorf("facts in flight must not be counted as withdrawn applications: exposure=%d outcome=%d", exposures, outcomes)
			}
		})
	}
}

// A refusal landing after a failed batch's respool filter and before its
// spool append is caught by the append's own re-check, under the spool
// lock: the withdrawn facts never reach disk, and a relaunch replays none.
func TestAnAgeRefusalBeforeTheRespoolAppendLeavesNoDurableCopy(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	rig.client.experimentCycle(context.Background())
	var fired atomic.Bool
	var refusalErr error
	rig.client.respoolAppendSeam = func(eligible []spoolEntry) {
		if len(eligible) == 0 || !fired.CompareAndSwap(false, true) {
			return
		}
		_, refusalErr = rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
	}
	rig.capture.setStatus(http.StatusInternalServerError)
	if err := rig.client.Flush(context.Background()); err == nil {
		t.Fatalf("setup: the flush must fail against the failing ingest")
	}
	if !fired.Load() || refusalErr != nil {
		t.Fatalf("setup: the refusal must land before the respool append (fired=%v err=%v)", fired.Load(), refusalErr)
	}
	if spooled := spooledExperimentFacts(rig.client, ageGoldenExperiment); spooled != 0 {
		t.Errorf("the respool appended %d fact(s) the refusal withdrew", spooled)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = rig.client.Close(ctx)
	cancel()
	rig.capture.setStatus(http.StatusAccepted)
	flushOrFail(t, rig.launch(t, nil))
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 0 {
		t.Errorf("the relaunch replayed %d fact(s) of the refused experiment", len(delivered))
	}
}

// Queued facts the worker has not taken yet die at its receive.
func TestAnAgeRefusalWithdrawsQueuedFacts(t *testing.T) {
	rig := newAgeWithdrawRig(t, "", func(cfg *Config) { cfg.BatchSize, cfg.BufferSize = 1, 8 }, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	// The worker parks holding a host event (BatchSize 1), so the sealed
	// facts stay in the queue.
	rig.capture.setStatus(http.StatusInternalServerError)
	if err := rig.client.Enqueue(Event{Name: "host_event"}); err != nil {
		t.Fatalf("setup: %v", err)
	}
	waitFor(t, 5*time.Second, "the worker parks on the failing ingest", func() bool { return rig.capture.hitCount() >= 1 })
	rig.client.experimentCycle(context.Background())
	if queued := len(rig.client.queue.ch); queued != 2 {
		t.Fatalf("setup: %d fact(s) queued, want 2", queued)
	}

	rig.refuse(t, "age_ineligible")
	rig.capture.setStatus(http.StatusAccepted)
	flushOrFail(t, rig.client)
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 0 {
		t.Errorf("%d queued fact(s) of the refused experiment delivered", len(delivered))
	}
	if hosts := rig.capture.byName("host_event"); len(hosts) != 1 {
		t.Errorf("the host event must still be delivered, got %d", len(hosts))
	}
}

// ── the automatic revalidation ──────────────────────────────────────────────

// The refusal reaches the same withdrawal through the lane's revalidation:
// the cycle's own sweep, which runs after it, finds nothing to send.
func TestAnAgeRefusalOnRevalidationWithdrawsTheOwedApplications(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	clock := &expFakeClock{now: time.Now()}
	rig.client.clock = clock
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)

	clock.advance(400 * time.Second)
	rig.client.experimentCycle(context.Background())
	if fetches := rig.script.requestCount(); fetches != 2 {
		t.Fatalf("setup: the cycle must revalidate once, got %d assignment request(s)", fetches)
	}
	if query := rig.script.request(1).URL.Query(); query.Get("age_band") != "adult" {
		t.Fatalf("setup: the revalidation re-sends the declaration, got %v", query)
	}
	if variant := rig.client.ExperimentVariant(ageGoldenExperiment); variant != "" {
		t.Errorf("the refused assignment is still served: %q", variant)
	}
	if owed := rig.client.owedExperimentExposureCount(); owed != 0 {
		t.Errorf("%d owed application(s) survive the refusal on revalidation", owed)
	}
	if exposures, outcomes := rig.applyRequests(); exposures+outcomes != 0 {
		t.Errorf("the cycle sent %d exposure and %d outcome apply request(s) after the refusal", exposures, outcomes)
	}
	if err := rig.client.TrackExperimentOutcome(ageGoldenExperiment, "score", 2); err == nil {
		t.Errorf("a new outcome on the withdrawn application must be refused")
	}
	flushOrFail(t, rig.client)
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 0 {
		t.Errorf("%d fact(s) of the refused experiment delivered", len(delivered))
	}
}

// A refusal whose fetch was in flight when an auth latch landed still
// withdraws (the epoch-stale destructive carve-out).
func TestAnAgeRefusalRacingAnAuthLatchStillWithdraws(t *testing.T) {
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil)
	rig.script.push(200, ageGolden(t, "adult"))
	rig.script.push(200, ageRefusalBody(t, "age_ineligible"))
	rig.script.push(401, `{"error":"unauthorized"}`)
	release := make(chan struct{})
	rig.script.mu.Lock()
	rig.script.gates = map[int]chan struct{}{1: release}
	rig.script.mu.Unlock()
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)

	refused := make(chan error, 1)
	go func() {
		_, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
		refused <- err
	}()
	waitFor(t, 5*time.Second, "the refusal is in flight", func() bool { return rig.script.requestCount() >= 2 })
	// Another experiment's fetch latches the plane (its own fence key, so
	// the refusal in flight is not fenced out, only epoch-stale).
	if _, err := rig.client.FetchExperimentAssignment(context.Background(), "latch-probe", nil); err == nil {
		t.Fatalf("setup: the 401 must latch")
	}
	close(release)
	<-refused

	if owed := rig.client.owedExperimentExposureCount(); owed != 0 {
		t.Errorf("%d owed application(s) survive an age refusal that raced a latch", owed)
	}
	if durableRecordHolds(t, spool, ageGoldenExperiment) {
		t.Errorf("the refused assignment is still in the durable record")
	}
	rig.client.exp.mu.Lock()
	retained := rig.client.exp.latchRetained[ageGoldenExperiment]
	rig.client.exp.mu.Unlock()
	if retained != nil {
		t.Errorf("the refused assignment is still retained for an unlatch")
	}
}

// ── the apply routes ────────────────────────────────────────────────────────

// An apply route answering 409 not_assigned/age_ineligible withdraws the
// subject's owed applications of the experiment and its facts, and stops
// serving the assignment, durably.
func TestAnApplyRouteAgeRefusalWithdrawsTheApplications(t *testing.T) {
	t.Run("exposure_route", func(t *testing.T) {
		spool := t.TempDir()
		rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"))
		rig.script.apply.push(http.StatusConflict, ageApplyRefusal)
		fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
		rig.applyAndMeasure(t, ageGoldenExperiment)

		rig.client.experimentCycle(context.Background())
		if exposures, outcomes := rig.applyRequests(); exposures != 1 || outcomes != 0 {
			t.Errorf("apply requests: exposure=%d outcome=%d, want the refused exposure only", exposures, outcomes)
		}
		assertApplyRouteAgeWithdrawal(t, rig, spool, 1, 1)
	})
	t.Run("outcome_route", func(t *testing.T) {
		spool := t.TempDir()
		rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"))
		rig.script.outcome.push(http.StatusConflict, ageApplyRefusal)
		fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
		rig.applyAndMeasure(t, ageGoldenExperiment)

		// The exposure is sealed and queued first; the outcome's refusal
		// then withdraws it from the pipeline too.
		rig.client.experimentCycle(context.Background())
		if exposures, outcomes := rig.applyRequests(); exposures != 1 || outcomes != 1 {
			t.Errorf("apply requests: exposure=%d outcome=%d, want 1 and 1", exposures, outcomes)
		}
		assertApplyRouteAgeWithdrawal(t, rig, spool, 0, 1)
	})
}

func assertApplyRouteAgeWithdrawal(t *testing.T, rig *ageWithdrawRig, spool string, exposureDrops, outcomeDrops uint64) {
	t.Helper()
	if owed := rig.client.owedExperimentExposureCount(); owed != 0 {
		t.Errorf("%d owed application(s) survive the apply route's age refusal", owed)
	}
	if exposures, outcomes := rig.drops("age_ineligible"); exposures != exposureDrops || outcomes != outcomeDrops {
		t.Errorf("age_ineligible drops: exposure=%d outcome=%d, want %d and %d", exposures, outcomes, exposureDrops, outcomeDrops)
	}
	if exposures, outcomes := rig.drops("apply_refused"); exposures+outcomes != 0 {
		t.Errorf("the age refusal must not be counted apply_refused: exposure=%d outcome=%d", exposures, outcomes)
	}
	rig.client.exp.mu.Lock()
	blocked := rig.client.exp.applyBlocked
	rig.client.exp.mu.Unlock()
	if blocked {
		t.Errorf("the age refusal paused the apply hop")
	}
	if variant := rig.client.ExperimentVariant(ageGoldenExperiment); variant != "" {
		t.Errorf("the assignment is still served after the apply route refused it: %q", variant)
	}
	if err := rig.client.TrackExperimentOutcome(ageGoldenExperiment, "score", 2); err == nil {
		t.Errorf("a new outcome on the withdrawn application must be refused")
	}
	rig.client.experimentCycle(context.Background())
	flushOrFail(t, rig.client)
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 0 {
		t.Errorf("%d fact(s) of the refused experiment delivered", len(delivered))
	}
	if durableRecordHolds(t, spool, ageGoldenExperiment) {
		t.Errorf("the refused assignment is still in the durable record")
	}
}

// The apply route's refusal withdraws spooled facts while the exposure sweep
// holds its emission lock. Their dead-letters dispatch once the sweep has
// released that lock, in the same sweep call: an OnSpoolDeadLetter callback
// that re-enters the SDK with a fetch, whose install sweeps the experiment
// again, completes instead of blocking forever on the lock.
func TestAnApplyRouteAgeRefusalDeadLettersOutsideTheSweep(t *testing.T) {
	ctx := context.Background()
	var client atomic.Pointer[Client]
	var deadLettered atomic.Int64
	var once sync.Once
	reentered := make(chan error, 1)
	rig := newAgeWithdrawRig(t, t.TempDir(), func(cfg *Config) {
		cfg.OnSpoolDeadLetter = func(letter SpoolDeadLetter) {
			if letter.Reason != SpoolDropTerminal {
				return
			}
			deadLettered.Add(int64(len(letter.Envelopes)))
			once.Do(func() {
				// The integrator fetches the experiment again: an adult
				// assignment installs, and the install sweeps.
				result, err := client.Load().FetchExperimentAssignmentWithAgeBand(ctx, ageGoldenExperiment, ExperimentAgeBandAdult, nil)
				if err == nil && !result.Assigned {
					err = errors.New("nothing installed")
				}
				reentered <- err
			})
		}
	}, ageGolden(t, "adult"), ageGolden(t, "adult"))
	client.Store(rig.client)
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	// The application and the outcome are sealed, and spooled by a failed
	// publish.
	rig.capture.setStatus(http.StatusInternalServerError)
	rig.client.experimentCycle(ctx)
	if err := rig.client.Flush(ctx); err == nil {
		t.Fatalf("setup: the flush must fail against the failing ingest")
	}
	if spooled := spooledExperimentFacts(rig.client, ageGoldenExperiment); spooled != 2 {
		t.Fatalf("setup: %d fact(s) spooled by the failed publish, want 2", spooled)
	}
	// One more application, which the exposure apply route refuses.
	if err := rig.client.TrackExperimentExposure(ageGoldenExperiment); err != nil {
		t.Fatalf("setup: %v", err)
	}
	rig.script.apply.push(http.StatusConflict, ageApplyRefusal)

	done := make(chan struct{})
	go func() {
		defer close(done)
		rig.client.experimentCycle(ctx)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("the lane's cycle did not return within 5s: the dead-letter callback re-entered a sweep while the refused application's sweep still held the emission lock")
	}
	select {
	case err := <-reentered:
		if err != nil {
			t.Errorf("the re-entered fetch must install the new assignment: %v", err)
		}
	default:
		t.Errorf("the withdrawn spooled facts were not dead-lettered during the sweep call")
	}
	if letters := deadLettered.Load(); letters != 2 {
		t.Errorf("the refusal must dead-letter the 2 withdrawn spooled facts, got %d", letters)
	}
}

// Any other refusal from an apply route keeps apply_refused for that one
// application: the rest is sent and delivered, and the assignment served.
func TestOtherApplyRefusalsDropOnlyTheApplication(t *testing.T) {
	for _, refusal := range []struct {
		name   string
		status int
		body   string
	}{
		{"400", http.StatusBadRequest, `{"error":"refused"}`},
		{"422", http.StatusUnprocessableEntity, `{"error":"refused"}`},
		{"409_other_error", http.StatusConflict, `{"error":"refused"}`},
		{"409_kill_switch", http.StatusConflict, `{"error":"not_assigned","reason":"kill_switch"}`},
		{"409_no_reason", http.StatusConflict, `{"error":"not_assigned"}`},
		{"422_age_body", http.StatusUnprocessableEntity, ageApplyRefusal},
	} {
		t.Run(refusal.name, func(t *testing.T) {
			rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
			rig.script.apply.push(refusal.status, refusal.body)
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)
			rig.client.experimentCycle(context.Background())
			flushOrFail(t, rig.client)
			if exposures, outcomes := rig.applyRequests(); exposures != 1 || outcomes != 1 {
				t.Errorf("apply requests: exposure=%d outcome=%d, want 1 and 1", exposures, outcomes)
			}
			if exposures, outcomes := rig.drops("apply_refused"); exposures != 1 || outcomes != 0 {
				t.Errorf("apply_refused drops: exposure=%d outcome=%d, want the exposure only", exposures, outcomes)
			}
			if exposures, outcomes := rig.drops("age_ineligible"); exposures+outcomes != 0 {
				t.Errorf("age_ineligible drops: exposure=%d outcome=%d, want none", exposures, outcomes)
			}
			if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 1 || delivered[0]["event_name"] != experimentOutcomeName {
				t.Errorf("the outcome must still be delivered, got %d fact(s)", len(delivered))
			}
			if variant := rig.client.ExperimentVariant(ageGoldenExperiment); variant != "control" {
				t.Errorf("the assignment must still be served, got %q", variant)
			}
		})
	}
}

// An application in flight to the apply route when the refusal lands is
// withdrawn with the rest: its sealed answer is discarded, and nothing
// behind it is sent.
func TestAnAgeRefusalDuringTheApplyHopDiscardsTheSeal(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	var once sync.Once
	var refusalErr error
	rig.script.apply.mu.Lock()
	rig.script.apply.onRequest = func() {
		once.Do(func() {
			_, refusalErr = rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
		})
	}
	rig.script.apply.mu.Unlock()

	rig.client.experimentCycle(context.Background())
	if refusalErr != nil {
		t.Fatalf("setup: the refusal: %v", refusalErr)
	}
	if exposures, outcomes := rig.applyRequests(); exposures != 1 || outcomes != 0 {
		t.Errorf("apply requests: exposure=%d outcome=%d, want only the one in flight", exposures, outcomes)
	}
	flushOrFail(t, rig.client)
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 0 {
		t.Errorf("the application sealed across the refusal delivered %d fact(s)", len(delivered))
	}
	if exposures, outcomes := rig.drops("age_ineligible"); exposures != 1 || outcomes != 1 {
		t.Errorf("age_ineligible drops: exposure=%d outcome=%d, want 1 and 1", exposures, outcomes)
	}
}

// An earlier drop's capture that never reached the spool (a frozen capture
// debt) loses the refused experiment's facts: a later retry must not spool
// them.
func TestAnAgeRefusalCancelsAFrozenCaptureOfTheExperiment(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	fact := func(id, experimentKey string) spoolEntry {
		raw := `{"event_id":"` + id + `","event_ts":"` + time.Now().UTC().Format(time.RFC3339Nano) +
			`","event_name":"experiment_exposure","props":{"experiment_key":"` + experimentKey +
			`","assignment_key":"` + ageGoldenFactKey(t) + `"}}`
		return spoolEntry{id: id, ts: time.Now().UTC().Format(time.RFC3339Nano), raw: json.RawMessage(raw), internalFact: true}
	}
	e := rig.client.exp
	e.mu.Lock()
	scope := e.scopeForLocked(e.currentSubjectIDLocked())
	e.durablePending[scopedIntentKey(scope, ageGoldenExperiment)] = expOwedSync{
		asOf: 1, drop: true, scope: scope, experimentKey: ageGoldenExperiment,
		captureFirst: true, captureEntries: []spoolEntry{fact("frozen-refused", ageGoldenExperiment)},
	}
	e.durablePending[scopedIntentKey(scope, "exposure-banner-2")] = expOwedSync{
		asOf: 1, drop: true, scope: scope, experimentKey: "exposure-banner-2",
		captureFirst: true, captureEntries: []spoolEntry{fact("frozen-other", "exposure-banner-2")},
	}
	e.mu.Unlock()

	rig.refuse(t, "age_ineligible")
	e.retryDurableSync()
	rig.client.spool.mu.Lock()
	_, refusedSpooled := rig.client.spool.ids["frozen-refused"]
	_, otherSpooled := rig.client.spool.ids["frozen-other"]
	rig.client.spool.mu.Unlock()
	if refusedSpooled {
		t.Errorf("the capture retry spooled a fact of the refused experiment after the refusal")
	}
	if !otherSpooled {
		t.Errorf("control: another experiment's frozen capture must still land")
	}
}

// A refusal landing between the worker's dispatch-point check and the
// transport handoff of the batch it built is caught by the re-check of the
// built batch.
func TestAnAgeRefusalAfterTheBatchIsBuiltIsWithheld(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	var fired atomic.Bool
	var refusalErr error
	rig.client.builtBatchHandoffSeam = func(batch []Event) {
		if len(batch) == 0 || !fired.CompareAndSwap(false, true) {
			return
		}
		_, refusalErr = rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
	}
	rig.client.experimentCycle(context.Background())
	flushOrFail(t, rig.client)
	if !fired.Load() || refusalErr != nil {
		t.Fatalf("setup: the refusal must land at the built-batch handoff (fired=%v err=%v)", fired.Load(), refusalErr)
	}
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 0 {
		t.Errorf("the built batch published %d fact(s) of the refused experiment", len(delivered))
	}
}

// The withdrawal is of what was owed before the refusal: after the host
// declares again and a new assignment is served, a new application and its
// outcome are recorded and delivered.
func TestANewAssignmentAfterAnAgeRefusalRecordsAgain(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"), ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	// The application is sealed and handed to the pipeline before the
	// refusal: its tuple is marked delivered for this session.
	rig.client.experimentCycle(context.Background())
	rig.refuse(t, "age_ineligible")

	if result := fetchAdultAssignment(t, rig.client, ageGoldenExperiment); !result.Assigned {
		t.Fatalf("setup: the new declaration must be assigned, got %+v", result)
	}
	rig.applyAndMeasure(t, ageGoldenExperiment)
	rig.client.experimentCycle(context.Background())
	flushOrFail(t, rig.client)
	delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment)
	names := map[any]int{}
	for _, envelope := range delivered {
		names[envelope["event_name"]]++
	}
	if len(delivered) != 2 || names[experimentExposureName] != 1 || names[experimentOutcomeName] != 1 {
		t.Errorf("the new application must deliver one exposure and one outcome, got %v", names)
	}
}

// ── the fact-key history ────────────────────────────────────────────────────

// factKeyHistoryInMemory is the current scope's fact-key history for the
// experiment, as the client holds it.
func factKeyHistoryInMemory(c *Client, experimentKey string) []string {
	e := c.exp
	e.mu.Lock()
	defer e.mu.Unlock()
	subject := e.currentSubjectIDLocked()
	if subject == "" || e.factKeys == nil || e.factKeys.scope != e.scopeForLocked(subject) {
		return nil
	}
	return append([]string(nil), e.factKeys.history[experimentKey]...)
}

// persistedFactKeyHistory is the experiment's fact-key history in the
// durable record on disk.
func persistedFactKeyHistory(t *testing.T, dir, experimentKey string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, expCacheFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("reading the durable record: %v", err)
	}
	var record struct {
		FactKeyHistory map[string][]string `json:"fact_key_history"`
	}
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decoding the durable record: %v", err)
	}
	return record.FactKeyHistory[experimentKey]
}

// keyWithdrawalKeys are the subject fact keys the client's record of age
// refusals still names for the experiment.
func keyWithdrawalKeys(c *Client, experimentKey string) []string {
	c.expKeyWithdrawMu.Lock()
	defer c.expKeyWithdrawMu.Unlock()
	var keys []string
	for factKey := range c.expKeyWithdrawals[experimentKey] {
		keys = append(keys, factKey)
	}
	return keys
}

func holdsFactKey(keys []string, factKey string) bool {
	for _, key := range keys {
		if key == factKey {
			return true
		}
	}
	return false
}

// republishedGolden is a golden rewritten as version 2 of the experiment,
// under another subject fact key: what the platform answers after a
// republish rotates the key.
func republishedGolden(t *testing.T, name, factKey string) string {
	t.Helper()
	body := strings.Replace(ageGolden(t, name), `"version":1`, `"version":2`, 1)
	return strings.Replace(body, ageGoldenFactKey(t), factKey, 1)
}

// sealWithKey makes both apply stubs seal their facts under factKey.
func (r *ageWithdrawRig) sealWithKey(factKey string) {
	for _, stub := range []*expApplyStub{&r.script.apply, &r.script.outcome} {
		stub.mu.Lock()
		stub.assignmentKey = factKey
		stub.mu.Unlock()
	}
}

// sealAndSpool applies and measures the served assignment, then seals the two
// applications and spools their facts through a failing publish: the worker
// keeps them for its retry, and the spool holds them.
func (r *ageWithdrawRig) sealAndSpool(t *testing.T, want int) {
	t.Helper()
	r.applyAndMeasure(t, ageGoldenExperiment)
	r.capture.setStatus(http.StatusInternalServerError)
	r.client.experimentCycle(context.Background())
	_ = r.client.Flush(context.Background())
	if spooled := spooledExperimentFacts(r.client, ageGoldenExperiment); spooled != want {
		t.Fatalf("setup: %d fact(s) spooled, want %d", spooled, want)
	}
}

// relaunch closes the client and constructs another on the same spool, the
// ingest answering status meanwhile.
func (r *ageWithdrawRig) relaunch(t *testing.T, status int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = r.client.Close(ctx)
	cancel()
	r.capture.setStatus(status)
	r.client = r.launch(t, nil)
	r.client.SetConsent(true)
}

// deliveredByKey counts the experiment's delivered facts by assignment_key.
func deliveredByKey(capture *expWireCapture) map[any]int {
	byKey := map[any]int{}
	for _, envelope := range deliveredExperimentFacts(capture, ageGoldenExperiment) {
		props, _ := envelope["props"].(map[string]any)
		byKey[props["assignment_key"]]++
	}
	return byKey
}

// shardpilot/shardpilot-go#138 R5: facts built under version 1's key are
// spooled; a republish installs version 2 under another key, and facts are
// built under it; the age refusal echoes version 2's key. The history names
// version 1's key, so the facts of both keys are withdrawn and none is
// delivered — in the same process, and after a relaunch that restores the
// spool. One cycle later the withdrawn keys have no live fact left and leave
// the history.
func TestAnAgeRefusalWithdrawsFactsUnderARetiredKey(t *testing.T) {
	for _, relaunch := range []bool{false, true} {
		name := "same_process"
		if relaunch {
			name = "after_relaunch"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			keyA := ageGoldenFactKey(t)
			keyB := "sfk1_" + strings.Repeat("d", 64)
			spool := t.TempDir()
			rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"),
				republishedGolden(t, "adult", keyB), republishedGolden(t, "under_threshold", keyB))
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.sealAndSpool(t, 2)
			if result := fetchAdultAssignment(t, rig.client, ageGoldenExperiment); result.Version != 2 {
				t.Fatalf("setup: the republish must install version 2, got %+v", result)
			}
			rig.sealWithKey(keyB)
			rig.sealAndSpool(t, 4)
			if relaunch {
				rig.relaunch(t, http.StatusServiceUnavailable)
			}

			rig.refuse(t, "age_ineligible")
			left := spooledExperimentFacts(rig.client, ageGoldenExperiment)
			rig.capture.setStatus(http.StatusAccepted)
			flushOrFail(t, rig.client)
			byKey := deliveredByKey(rig.capture)
			t.Logf("spooled after refusal=%d; delivered by key: A=%d B=%d", left, byKey[keyA], byKey[keyB])
			if left != 0 || byKey[keyA] != 0 || byKey[keyB] != 0 {
				t.Errorf("the refusal echoing version 2's key must withdraw the facts of both keys: %d left spooled, delivered A=%d B=%d", left, byKey[keyA], byKey[keyB])
			}

			rig.client.experimentCycle(ctx)
			if held := factKeyHistoryInMemory(rig.client, ageGoldenExperiment); len(held) != 0 {
				t.Errorf("the withdrawn keys have no live fact left, yet the history holds %v", held)
			}
			if held := persistedFactKeyHistory(t, spool, ageGoldenExperiment); len(held) != 0 {
				t.Errorf("the withdrawn keys have no live fact left, yet the persisted history holds %v", held)
			}
		})
	}
}

// A retired key leaves the history once its last fact goes: version 1's
// facts are spooled, the republish retires their key, and the history holds
// it, in memory and in the persisted record, until the facts are delivered;
// the next cycle prunes it from both.
func TestARetiredKeyLeavesTheHistoryWithItsLastFact(t *testing.T) {
	keyA := ageGoldenFactKey(t)
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"), republishedGolden(t, "adult", "sfk1_"+strings.Repeat("d", 64)))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.sealAndSpool(t, 2)
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.experimentCycle(context.Background())
	if !holdsFactKey(factKeyHistoryInMemory(rig.client, ageGoldenExperiment), keyA) ||
		!holdsFactKey(persistedFactKeyHistory(t, spool, ageGoldenExperiment), keyA) {
		t.Fatalf("setup: the republish must retire version 1's key into the history while its facts live (memory %v, disk %v)",
			factKeyHistoryInMemory(rig.client, ageGoldenExperiment), persistedFactKeyHistory(t, spool, ageGoldenExperiment))
	}

	rig.capture.setStatus(http.StatusAccepted)
	flushOrFail(t, rig.client)
	if delivered := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); delivered != 2 {
		t.Fatalf("setup: %d of version 1's 2 facts delivered", delivered)
	}
	rig.client.experimentCycle(context.Background())
	memory, disk := factKeyHistoryInMemory(rig.client, ageGoldenExperiment), persistedFactKeyHistory(t, spool, ageGoldenExperiment)
	t.Logf("after the last fact of the retired key was delivered: history in memory %v, persisted %v", memory, disk)
	if holdsFactKey(memory, keyA) || holdsFactKey(disk, keyA) {
		t.Errorf("the retired key's last fact was delivered, yet the history keeps it (memory %v, disk %v)", memory, disk)
	}
}

// A retired key stays in the history while a fact under it is spooled: kept
// across the republish, present in the persisted record, and still there
// after a relaunch, whose prune runs once the spool is loaded.
func TestARetiredKeyStaysInTheHistoryWhileItsFactsAreSpooled(t *testing.T) {
	keyA := ageGoldenFactKey(t)
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"), republishedGolden(t, "adult", "sfk1_"+strings.Repeat("d", 64)))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.sealAndSpool(t, 2)
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.experimentCycle(context.Background())
	for _, stage := range []string{"after the republish", "after a relaunch"} {
		if stage == "after a relaunch" {
			rig.relaunch(t, http.StatusServiceUnavailable)
			rig.client.experimentCycle(context.Background())
			if spooled := spooledExperimentFacts(rig.client, ageGoldenExperiment); spooled != 2 {
				t.Fatalf("setup: the relaunch restored %d of the 2 spooled facts", spooled)
			}
		}
		memory, disk := factKeyHistoryInMemory(rig.client, ageGoldenExperiment), persistedFactKeyHistory(t, spool, ageGoldenExperiment)
		t.Logf("%s: history in memory %v, persisted %v", stage, memory, disk)
		if !holdsFactKey(memory, keyA) || !holdsFactKey(disk, keyA) {
			t.Errorf("%s the retired key's facts are still spooled, yet the history lost it (memory %v, disk %v)", stage, memory, disk)
		}
	}
}

// A key whose entry never reached the disk is still retired by the drop:
// the entry's own write fails and its facts are spooled; storage recovers,
// and a kill switch drops the entry, which finds nothing stored to delete.
// The drop writes the key into the persisted history, so after a relaunch a
// refusal without a subject fact key still withdraws the spooled facts.
func TestAKeyWhoseEntryNeverLandedIsRetiredByTheDrop(t *testing.T) {
	keyA := ageGoldenFactKey(t)
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"), ageRefusalBody(t, "kill_switch"), ageRefusalBodyWithoutFactKey(t))
	rig.client.exp.mu.Lock()
	rig.client.exp.failDurableWritesForTests = true
	rig.client.exp.mu.Unlock()
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.sealAndSpool(t, 2)
	if durableRecordHolds(t, spool, ageGoldenExperiment) {
		t.Fatalf("setup: the entry's write must not land")
	}
	rig.client.exp.mu.Lock()
	rig.client.exp.failDurableWritesForTests = false
	rig.client.exp.mu.Unlock()
	rig.refuse(t, "kill_switch")
	disk := persistedFactKeyHistory(t, spool, ageGoldenExperiment)
	t.Logf("after the kill switch dropped the entry that never landed: persisted history %v", disk)
	if !holdsFactKey(disk, keyA) {
		t.Errorf("the drop took the key out of the entries, yet the persisted history lacks it: %v", disk)
	}

	rig.relaunch(t, http.StatusServiceUnavailable)
	rig.refuse(t, "age_ineligible")
	left := spooledExperimentFacts(rig.client, ageGoldenExperiment)
	rig.capture.setStatus(http.StatusAccepted)
	flushOrFail(t, rig.client)
	byKey := deliveredByKey(rig.capture)
	t.Logf("after a relaunch and a refusal without a subject fact key: spooled=%d; delivered under the key=%d", left, byKey[keyA])
	if left != 0 || byKey[keyA] != 0 {
		t.Errorf("the spooled facts under the dropped entry's key must be withdrawn: %d left spooled, %d delivered", left, byKey[keyA])
	}
}

// A restored entry's key is retired by the write that replaces it even when
// the record reads back empty at that moment: the retirement diffs against
// the memory copy the load seeded, not a fresh read. Version 1's facts are
// spooled, a relaunch restores its entry, the record then reads as corrupt,
// and a republish installs version 2; a refusal echoing version 2's key
// still withdraws version 1's facts.
func TestARestoredKeyIsRetiredWhenTheRecordReadsEmpty(t *testing.T) {
	keyA := ageGoldenFactKey(t)
	keyB := "sfk1_" + strings.Repeat("d", 64)
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"),
		republishedGolden(t, "adult", keyB), republishedGolden(t, "under_threshold", keyB))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.sealAndSpool(t, 2)
	rig.relaunch(t, http.StatusServiceUnavailable)
	if variant := rig.client.ExperimentVariant(ageGoldenExperiment); variant != "control" {
		t.Fatalf("setup: the relaunch must restore version 1's entry, got %q", variant)
	}
	if err := os.WriteFile(filepath.Join(spool, expCacheFileName), []byte("{"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if result := fetchAdultAssignment(t, rig.client, ageGoldenExperiment); result.Version != 2 {
		t.Fatalf("setup: the republish must install version 2, got %+v", result)
	}

	rig.refuse(t, "age_ineligible")
	left := spooledExperimentFacts(rig.client, ageGoldenExperiment)
	rig.capture.setStatus(http.StatusAccepted)
	flushOrFail(t, rig.client)
	byKey := deliveredByKey(rig.capture)
	t.Logf("spooled after refusal=%d; delivered under the restored key=%d", left, byKey[keyA])
	if left != 0 || byKey[keyA] != 0 {
		t.Errorf("the refusal echoing version 2's key must withdraw the restored version 1's facts: %d left spooled, %d delivered", left, byKey[keyA])
	}
}

// experimentRecordBeforeTheHistory is the durable record the SDK wrote at
// 193c038 (the v0.7.1-alpha format) for the adult golden, byte for byte
// except the placeholders: no fact_key_history member. The test fills in the
// stub server, the golden's own keys, and a visibly synthetic subject id.
const experimentRecordBeforeTheHistory = `{"scope":"workspace-test\u001fexposure-app\u001fdevelop\u001f{{subject}}\u001f{{server}}\u001f94f43421ce6c39f1","entries":{"exposure-banner":{"assignment_key":"{{assignment_key}}","variant_key":"control","variant_payload":{"copy":"Control"},"version":1,"assignment_unit":"client_id","subject_fact_key":"{{subject_fact_key}}","subject_key":"{{subject}}","attributes":[{"name":"age_band","value":"adult"}],"fetched_at_ms":1790726194423,"served":{"revision":1,"kill_gate":false,"at":"2026-09-28T11:32:49.479Z"}}}}
`

// A record written before the history existed loads with an empty history
// and serves as before: the assignment is restored without a fetch, and an
// application of it is sealed and delivered under its key.
func TestARecordWithoutAFactKeyHistoryLoads(t *testing.T) {
	script := &expScript{}
	script.apply.assignmentKey = ageGoldenFactKey(t)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	t.Cleanup(server.Close)
	spool := t.TempDir()
	subject := "spcid_" + strings.Repeat("0a", 16)
	if err := os.WriteFile(filepath.Join(spool, expSubjectFileName), []byte(subject+"\n"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	var golden struct {
		AssignmentKey string `json:"assignment_key"`
	}
	if err := json.Unmarshal([]byte(ageGolden(t, "adult")), &golden); err != nil || golden.AssignmentKey == "" {
		t.Fatalf("the adult golden must carry an assignment key: %q %v", golden.AssignmentKey, err)
	}
	record := strings.NewReplacer(
		"{{server}}", server.URL,
		"{{subject}}", subject,
		"{{assignment_key}}", golden.AssignmentKey,
		"{{subject_fact_key}}", ageGoldenFactKey(t),
	).Replace(experimentRecordBeforeTheHistory)
	if err := os.WriteFile(filepath.Join(spool, expCacheFileName), []byte(record), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	rig := &ageWithdrawRig{script: script, capture: capture, server: server, spool: spool}
	rig.client = rig.launch(t, nil)
	rig.client.SetConsent(true)

	if variant := rig.client.ExperimentVariant(ageGoldenExperiment); variant != "control" || script.requestCount() != 0 {
		t.Fatalf("the record must restore its assignment without a fetch: served %q after %d fetch(es)", variant, script.requestCount())
	}
	if held := factKeyHistoryInMemory(rig.client, ageGoldenExperiment); len(held) != 0 {
		t.Errorf("a record without the section must load an empty history, got %v", held)
	}
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	rig.client.experimentCycle(context.Background())
	flushOrFail(t, rig.client)
	byKey := deliveredByKey(capture)
	t.Logf("restored from a record without the section: served %q; delivered by key: %v", rig.client.ExperimentVariant(ageGoldenExperiment), byKey)
	if byKey[ageGoldenFactKey(t)] != 1 {
		t.Errorf("the restored assignment's application must be delivered under its key, got %v", byKey)
	}
}

// adultGoldenWithoutFactKey is the adult golden with its subject_fact_key
// member removed: an assignment the apply endpoint mints the key for.
func adultGoldenWithoutFactKey(t *testing.T) string {
	t.Helper()
	body := strings.Replace(ageGolden(t, "adult"), `"subject_fact_key":"`+ageGoldenFactKey(t)+`",`, "", 1)
	if strings.Contains(body, `"subject_fact_key":`) {
		t.Fatalf("setup: the adult golden must lose its subject_fact_key member")
	}
	return body
}

// shardpilot/shardpilot-go#145: an assignment without a subject fact key;
// the apply endpoint mints the key its facts carry. The minted key is in the
// persisted history once the facts are sealed, before any of them is
// spooled, and a later refusal without a subject fact key withdraws them —
// in the same process, and after a relaunch.
func TestAnAgeRefusalWithdrawsFactsUnderAMintedKey(t *testing.T) {
	minted := "sfk1_" + strings.Repeat("e", 64)
	for _, relaunch := range []bool{false, true} {
		name := "same_process"
		if relaunch {
			name = "after_relaunch"
		}
		t.Run(name, func(t *testing.T) {
			spool := t.TempDir()
			rig := newAgeWithdrawRig(t, spool, nil, adultGoldenWithoutFactKey(t), ageRefusalBodyWithoutFactKey(t))
			rig.sealWithKey(minted)
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)
			rig.capture.setStatus(http.StatusInternalServerError)
			rig.client.experimentCycle(context.Background())
			if held := persistedFactKeyHistory(t, spool, ageGoldenExperiment); !holdsFactKey(held, minted) {
				t.Errorf("the sealed facts carry the minted key, yet the persisted history holds %v", held)
			}
			_ = rig.client.Flush(context.Background())
			if spooled := spooledExperimentFacts(rig.client, ageGoldenExperiment); spooled != 2 {
				t.Fatalf("setup: %d fact(s) spooled, want 2", spooled)
			}
			if relaunch {
				rig.relaunch(t, http.StatusServiceUnavailable)
			}

			rig.refuse(t, "age_ineligible")
			left := spooledExperimentFacts(rig.client, ageGoldenExperiment)
			rig.capture.setStatus(http.StatusAccepted)
			flushOrFail(t, rig.client)
			byKey := deliveredByKey(rig.capture)
			t.Logf("spooled after refusal=%d; delivered under the minted key=%d", left, byKey[minted])
			if left != 0 || byKey[minted] != 0 {
				t.Errorf("a refusal without a subject fact key must withdraw the facts under the minted key: %d left spooled, %d delivered", left, byKey[minted])
			}
		})
	}
}

// The minted key is durable before its fact exists: while the record cannot
// be written, the sealed answer is discarded and nothing is queued or
// spooled; once it can, the application is sealed again and delivered.
func TestAMintedKeyIsDurableBeforeItsFact(t *testing.T) {
	minted := "sfk1_" + strings.Repeat("e", 64)
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil, adultGoldenWithoutFactKey(t))
	rig.sealWithKey(minted)
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	rig.client.exp.mu.Lock()
	rig.client.exp.failDurableWritesForTests = true
	rig.client.exp.mu.Unlock()
	rig.client.experimentCycle(context.Background())
	flushOrFail(t, rig.client)
	owed := rig.client.owedExperimentExposureCount()
	delivered := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment))
	t.Logf("record unwritable: apply requests=%d, owed=%d, delivered=%d", len(rig.script.apply.requestsSoFar()), owed, delivered)
	if owed != 1 || delivered != 0 {
		t.Errorf("a fact under a minted key the record could not hold must not exist yet: %d owed, %d delivered", owed, delivered)
	}

	rig.client.exp.mu.Lock()
	rig.client.exp.failDurableWritesForTests = false
	rig.client.exp.mu.Unlock()
	rig.client.experimentCycle(context.Background())
	flushOrFail(t, rig.client)
	if held := persistedFactKeyHistory(t, spool, ageGoldenExperiment); !holdsFactKey(held, minted) {
		t.Errorf("the minted key must be persisted, got %v", held)
	}
	if byKey := deliveredByKey(rig.capture); byKey[minted] != 1 {
		t.Errorf("once the record holds the key the application is sealed again and delivered, got %v", byKey)
	}
}

// Control: another subject's facts under a minted key survive. The previous
// subject's application and outcome are sealed under a minted key and
// accepted (held by the worker, or spooled); a re-mint rotates the subject,
// and the new subject's refusal carries no subject fact key. The minted key
// is the previous subject's, not the new one's: its facts are delivered.
func TestAnAgeRefusalLeavesAnotherSubjectsMintedKeyFacts(t *testing.T) {
	minted := "sfk1_" + strings.Repeat("e", 64)
	for _, staging := range []string{"held_by_the_worker", "spooled"} {
		t.Run(staging, func(t *testing.T) {
			ctx := context.Background()
			rig := newAgeWithdrawRig(t, t.TempDir(), nil, adultGoldenWithoutFactKey(t))
			rig.script.push(http.StatusBadRequest, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`)
			rig.script.push(http.StatusOK, ageRefusalBodyWithoutFactKey(t))
			rig.sealWithKey(minted)
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)
			if staging == "spooled" {
				rig.capture.setStatus(http.StatusInternalServerError)
			}
			rig.client.experimentCycle(ctx)
			if staging == "spooled" {
				_ = rig.client.Flush(ctx)
				if spooled := spooledExperimentFacts(rig.client, ageGoldenExperiment); spooled != 2 {
					t.Fatalf("setup: %d fact(s) spooled, want 2", spooled)
				}
			} else {
				waitFor(t, 5*time.Second, "the worker holds the sealed facts", func() bool { return len(rig.client.queue.ch) == 0 })
			}

			result, err := rig.client.FetchExperimentAssignmentWithAgeBand(ctx, ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
			if err != nil || result.Reason != "age_ineligible" || rig.script.requestCount() != 3 {
				t.Fatalf("setup: the re-minted retry must be refused as age_ineligible, got %+v err=%v after %d fetch(es)", result, err, rig.script.requestCount())
			}
			rig.capture.setStatus(http.StatusAccepted)
			flushOrFail(t, rig.client)
			if byKey := deliveredByKey(rig.capture); byKey[minted] != 2 {
				t.Errorf("the previous subject's facts under its minted key must be delivered: %v", byKey)
			}
		})
	}
}

// The age refusals' withdrawal record is bounded like the history: an entry
// stays while a fact it withdraws is still held by the worker (for a retry),
// and goes the cycle after the worker dropped the last one.
func TestAKeyWithdrawalIsForgottenWithItsLastFact(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.sealAndSpool(t, 2)
	rig.refuse(t, "age_ineligible")
	rig.client.experimentCycle(context.Background())
	if held := keyWithdrawalKeys(rig.client, ageGoldenExperiment); !holdsFactKey(held, ageGoldenFactKey(t)) {
		t.Errorf("the worker still holds withdrawn facts, yet their withdrawal is forgotten: %v", held)
	}

	rig.capture.setStatus(http.StatusAccepted)
	flushOrFail(t, rig.client)
	rig.client.experimentCycle(context.Background())
	held := keyWithdrawalKeys(rig.client, ageGoldenExperiment)
	delivered := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment))
	t.Logf("after the worker dropped the last withdrawn fact: withdrawal record %v; delivered %d", held, delivered)
	if len(held) != 0 {
		t.Errorf("no fact under the withdrawn key is left, yet the withdrawal record keeps %v", held)
	}
	if delivered != 0 {
		t.Errorf("%d withdrawn fact(s) delivered", delivered)
	}
}

// A fact in its emit window keeps the withdrawal that condemns it: the
// refusal lands after the fact is built from its owed record (withdrawing
// the record) and before the fact is queued. A prune started in that window
// waits for the emission, so the queued fact still meets its withdrawal at
// the worker and is not delivered.
func TestAPruneWaitsOutAFactInItsEmitWindow(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	var fired atomic.Bool
	var refusalErr error
	pruned := make(chan struct{})
	prunedInTheWindow := false
	rig.client.exp.mu.Lock()
	rig.client.exp.consentRaceSeam = func(stage string) {
		if stage != "exposure_enqueue" || !fired.CompareAndSwap(false, true) {
			return
		}
		_, refusalErr = rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
		go func() {
			rig.client.pruneExperimentFactKeys()
			close(pruned)
		}()
		select {
		case <-pruned:
			prunedInTheWindow = true
		case <-time.After(200 * time.Millisecond):
		}
	}
	rig.client.exp.mu.Unlock()
	rig.client.experimentCycle(context.Background())
	<-pruned
	if !fired.Load() || refusalErr != nil {
		t.Fatalf("setup: the refusal must land in the emit window (fired=%v err=%v)", fired.Load(), refusalErr)
	}
	flushOrFail(t, rig.client)
	delivered := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment))
	t.Logf("prune completed inside the emit window=%v; delivered %d", prunedInTheWindow, delivered)
	if delivered != 0 {
		t.Errorf("the fact built before the refusal was delivered: the prune took its withdrawal in the emit window")
	}
}

// A pulled spool chunk keeps the withdrawal that condemns its members: the
// refusal sweeps them from the mirror while the worker holds the chunk, and
// a prune in that moment must not forget the withdrawal the chunk's handoff
// re-check reads.
func TestAPruneWaitsOutAPulledSpoolChunk(t *testing.T) {
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.sealAndSpool(t, 2)
	rig.relaunch(t, http.StatusAccepted)
	var fired atomic.Bool
	var refusalErr error
	rig.client.spoolResendHandoffSeam = func(chunk []spoolEntry) {
		if !fired.CompareAndSwap(false, true) {
			return
		}
		_, refusalErr = rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
		rig.client.pruneExperimentFactKeys()
	}
	flushOrFail(t, rig.client)
	if !fired.Load() || refusalErr != nil {
		t.Fatalf("setup: the refusal must land at the chunk handoff (fired=%v err=%v)", fired.Load(), refusalErr)
	}
	delivered := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment))
	t.Logf("a prune ran while the worker held the pulled chunk; delivered %d", delivered)
	if delivered != 0 {
		t.Errorf("the pulled chunk published %d fact(s) the refusal withdrew", delivered)
	}
}
