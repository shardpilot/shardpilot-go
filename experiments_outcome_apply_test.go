package shardpilot

// Outcomes through the platform's outcome apply endpoint: an outcome is
// owed like an exposure, follows the session's most recent application of
// the experiment, and is posted only as the fact the platform sealed.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

var expOutcomeIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

// newAppliedOutcomeRig is a hop rig whose assignment the host has applied,
// with the exposure already sealed and delivered, so the outcome route is
// the only one left to answer.
func newAppliedOutcomeRig(t *testing.T, mutate func(*Config), bodies ...string) *expHopRig {
	t.Helper()
	rig := newExpHopRig(t, mutate, bodies...)
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.cycle()
	flushOrFail(t, rig.client)
	if got := len(rig.capture.exposures()); got != 1 {
		t.Fatalf("control: the application's exposure is delivered, got %d", got)
	}
	return rig
}

func (r *expHopRig) outcomeRequests() []expApplyRequest { return r.script.outcome.requestsSoFar() }

func (r *expHopRig) outcomeDrops(code string) uint64 {
	return r.client.Snapshot().ExperimentOutcomeDrops[code]
}

// ── the hop ─────────────────────────────────────────────────────────────────

func TestAnOutcomeIsSealedByTheOutcomeApplyHopAndDeliveredVerbatim(t *testing.T) {
	rig := newAppliedOutcomeRig(t, nil)
	application := rig.script.apply.requestsSoFar()[0].body
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 42); err != nil {
		t.Fatalf("TrackExperimentOutcome: %v", err)
	}
	if got := len(rig.outcomeRequests()); got != 0 {
		t.Fatalf("the host call must not reach the network, got %d request(s)", got)
	}
	rig.cycle()
	requests := rig.outcomeRequests()
	if len(requests) != 1 {
		t.Fatalf("one outcome, one outcome apply request: got %d", len(requests))
	}
	request := requests[0]
	if request.header.Get("Authorization") != "Bearer test-exp-key" {
		t.Fatalf("the outcome request carries the assignment host's key, got %q", request.header.Get("Authorization"))
	}
	body := request.body
	members := make([]string, 0, len(body))
	for name := range body {
		members = append(members, name)
	}
	sort.Strings(members)
	want := []string{"app_key", "applied_at", "attributes", "environment_key", "experiment_key", "experiment_version",
		"occurred_at", "outcome_id", "outcome_key", "outcome_value", "served_at", "served_kill_gate", "served_revision",
		"subject_key", "variant_key"}
	if !reflect.DeepEqual(members, want) {
		t.Fatalf("the outcome request carries exactly the application and the outcome:\ngot  %v\nwant %v", members, want)
	}
	for _, member := range []string{"app_key", "environment_key", "experiment_key", "experiment_version", "subject_key",
		"variant_key", "served_revision", "served_kill_gate", "served_at", "applied_at"} {
		if !reflect.DeepEqual(body[member], application[member]) {
			t.Fatalf("%s must be the application's: got %v, the application sent %v", member, body[member], application[member])
		}
	}
	outcomeID, _ := body["outcome_id"].(string)
	if !expOutcomeIDPattern.MatchString(outcomeID) || body["outcome_key"] != "purchase" || body["outcome_value"] != json.Number("42") {
		t.Fatalf("the outcome's own members: %v", body)
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, body["occurred_at"].(string))
	appliedAt, _ := time.Parse(time.RFC3339Nano, application["applied_at"].(string))
	if err != nil || occurredAt.Before(appliedAt) {
		t.Fatalf("occurred_at is RFC 3339 and not before applied_at: occurred_at=%v applied_at=%v err=%v", body["occurred_at"], application["applied_at"], err)
	}

	flushOrFail(t, rig.client)
	facts := rig.capture.byName(experimentOutcomeName)
	if len(facts) != 1 {
		t.Fatalf("the sealed outcome is delivered once, got %d", len(facts))
	}
	delivered, sealed := facts[0], rig.script.outcome.sealedFacts()[0]
	if delivered["event_id"] != expStubFactID(outcomeID) || delivered["event_id"] != sealed["event_id"] ||
		delivered["event_ts"] != sealed["event_ts"] || delivered["attestation_seal"] != expStubSeal ||
		delivered["source"] != "client" || delivered["anonymous_id"] != "anon-test" || delivered["user_id"] != nil ||
		delivered["session_id"] == nil {
		t.Fatalf("the envelope carries the sealed members verbatim and the SDK's own identity:\ndelivered %v\nsealed    %v", delivered, sealed)
	}
	posted := false
	for _, batch := range rig.capture.rawBatches() {
		if bytes.Contains(batch, []byte(`"outcome_value":42`)) {
			posted = true
		}
	}
	if !posted {
		t.Fatal("the posted props carry the sealed outcome_value as an integer")
	}
}

// ── which application an outcome follows ────────────────────────────────────

func TestAnOutcomeRequiresAnApplication(t *testing.T) {
	rig := newExpHopRig(t, nil)
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); !errors.Is(err, ErrExperimentNoAssignment) {
		t.Fatalf("no assignment and no application: ErrExperimentNoAssignment, got %v", err)
	}
	fetchAssignment(t, rig.client, expTestScopeKey)
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); !errors.Is(err, ErrExperimentNotApplied) {
		t.Fatalf("a served assignment the session has not applied: ErrExperimentNotApplied, got %v", err)
	}
	rig.cycle()
	if got := len(rig.outcomeRequests()); got != 0 {
		t.Fatalf("a refused outcome sends nothing, got %d request(s)", got)
	}
}

// The outcome follows the session's most recent application of the
// experiment, whatever the assignment is now: a newer version the host has
// not applied yet does not move it, a drop does not end it, and the next
// application does.
func TestAnOutcomeFollowsTheMostRecentApplication(t *testing.T) {
	v3 := expAssignedServedBody(expServedTrio)
	v4 := strings.Replace(v3, `"version":3`, `"version":4`, 1)
	rig := newExpHopRig(t, nil, v3, v4, `{"assigned":false}`)
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	fetchAssignment(t, rig.client, expTestScopeKey) // installs v4, not applied
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); err != nil {
		t.Fatalf("TrackExperimentOutcome after a new version: %v", err)
	}
	rig.client.ApplyExperimentVariant(expTestScopeKey) // applies v4
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 2); err != nil {
		t.Fatalf("TrackExperimentOutcome after applying v4: %v", err)
	}
	if result := fetchAssignment(t, rig.client, expTestScopeKey); result.Assigned {
		t.Fatal("control: the third fetch drops the assignment")
	}
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 3); err != nil {
		t.Fatalf("TrackExperimentOutcome after the assignment was dropped: %v", err)
	}
	rig.cycle()
	requests := rig.outcomeRequests()
	if len(requests) != 3 {
		t.Fatalf("three outcomes, three requests: got %d", len(requests))
	}
	applications := rig.script.apply.requestsSoFar()
	if len(applications) != 2 {
		t.Fatalf("control: two applications were sent, got %d", len(applications))
	}
	for i, want := range []map[string]any{applications[0].body, applications[1].body, applications[1].body} {
		got := requests[i].body
		if got["experiment_version"] != want["experiment_version"] || got["applied_at"] != want["applied_at"] {
			t.Fatalf("outcome %d follows the most recent application: version=%v applied_at=%v, want version=%v applied_at=%v",
				i+1, got["experiment_version"], got["applied_at"], want["experiment_version"], want["applied_at"])
		}
	}
}

// ── refused at the call ─────────────────────────────────────────────────────

func TestAnOutcomeValueMustBeAnIntegerWithinTwoToThe53(t *testing.T) {
	rig := newAppliedOutcomeRig(t, nil)
	for _, value := range []float64{1.5, -0.25, 9007199254740994, -9007199254740994, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", value); !errors.Is(err, ErrInvalidExperimentFact) {
			t.Fatalf("value %v is refused with ErrInvalidExperimentFact, got %v", value, err)
		}
	}
	if rig.owed() != 0 {
		t.Fatalf("a refused outcome is not owed, got %d", rig.owed())
	}
	for _, value := range []float64{9007199254740992, -9007199254740992, math.Copysign(0, -1), 7} {
		if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", value); err != nil {
			t.Fatalf("value %v is accepted, got %v", value, err)
		}
	}
	rig.cycle()
	var sent []any
	for _, request := range rig.outcomeRequests() {
		sent = append(sent, request.body["outcome_value"])
	}
	want := []any{json.Number("9007199254740992"), json.Number("-9007199254740992"), json.Number("0"), json.Number("7")}
	if !reflect.DeepEqual(sent, want) {
		t.Fatalf("accepted values are sent as JSON integers:\ngot  %v\nwant %v", sent, want)
	}
}

func TestAnOutcomeKeyMustBeInThePlatformGrammar(t *testing.T) {
	rig := newAppliedOutcomeRig(t, nil)
	for _, key := range []string{"", "  ", "a b", "a/b", "a@b", "é", strings.Repeat("a", 129), "10.0.0.1", "::1", "2001:db8::1"} {
		if err := rig.client.TrackExperimentOutcome(expTestScopeKey, key, 1); !errors.Is(err, ErrInvalidExperimentFact) {
			t.Fatalf("outcome key %q is refused with ErrInvalidExperimentFact, got %v", key, err)
		}
	}
	for _, key := range []string{"purchase", "level.up:2-x_y", strings.Repeat("a", 128)} {
		if err := rig.client.TrackExperimentOutcome(expTestScopeKey, key, 1); err != nil {
			t.Fatalf("outcome key %q is accepted, got %v", key, err)
		}
	}
	rig.cycle()
	if got := len(rig.outcomeRequests()); got != 3 {
		t.Fatalf("only the accepted keys are sent: got %d request(s)", got)
	}
}

// A clock stepped back past the application refuses the outcome at the
// call: the platform refuses an occurred_at earlier than applied_at, and an
// outcome queued only to be refused would be lost silently later.
func TestAnOutcomeBeforeItsApplicationIsRefused(t *testing.T) {
	rig := newExpHopRig(t, nil)
	clock := &expFakeClock{now: time.Now()}
	rig.client.clock = clock
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	clock.advance(-time.Second)
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); !errors.Is(err, ErrInvalidExperimentFact) {
		t.Fatalf("an outcome earlier than its application is refused, got %v", err)
	}
	clock.advance(2 * time.Second)
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); err != nil {
		t.Fatalf("control: an outcome after its application is accepted, got %v", err)
	}
}

// An assignment that cannot be recorded cannot carry an outcome either; the
// refusal is counted like the application's.
func TestAnUnrecordableAssignmentRefusesAnOutcomeAndCountsIt(t *testing.T) {
	rig := newExpHopRig(t, nil, expAssignedBodyUnserved("3"))
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); !errors.Is(err, ErrExperimentFactUnavailable) {
		t.Fatalf("an unrecordable assignment refuses the outcome with ErrExperimentFactUnavailable, got %v", err)
	}
	if rig.outcomeDrops("not_recordable") != 1 || rig.drops("not_recordable") != 1 {
		t.Fatalf("each refusal is counted in its own map: outcome=%v exposure=%v",
			rig.client.Snapshot().ExperimentOutcomeDrops, rig.client.Snapshot().ExperimentExposureDrops)
	}
}

// The call only accepts the outcome: a full analytics queue is not its
// concern any more.
func TestAnOutcomeNeverReportsErrQueueFull(t *testing.T) {
	rig := newAppliedOutcomeRig(t, func(cfg *Config) {
		cfg.BatchSize = 1
		cfg.BufferSize = 1
	})
	parkWorkerWithFullQueue(t, rig.client, rig.capture)
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); err != nil {
		t.Fatalf("a full queue does not refuse the outcome, got %v", err)
	}
	if rig.owed() != 1 {
		t.Fatalf("the outcome is owed, got %d", rig.owed())
	}
	rig.capture.setStatus(202)
}

// ── dispositions and counts ─────────────────────────────────────────────────

func TestAnOutcomeRetryReusesItsOutcomeID(t *testing.T) {
	rig := newAppliedOutcomeRig(t, nil)
	rig.script.outcome.push(503, ``)
	rig.script.outcome.push(200, expApplyEcho)
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); err != nil {
		t.Fatalf("TrackExperimentOutcome: %v", err)
	}
	rig.cycle()
	rig.cycle()
	flushOrFail(t, rig.client)
	requests := rig.outcomeRequests()
	if len(requests) != 2 || requests[0].body["outcome_id"] != requests[1].body["outcome_id"] {
		t.Fatalf("a retried outcome re-sends its outcome_id: %d request(s)", len(requests))
	}
	if got := len(rig.capture.byName(experimentOutcomeName)); got != 1 {
		t.Fatalf("the retried outcome is delivered once, got %d", got)
	}
}

func TestAnOutcomeRefusalIsCountedAsAnOutcomeDrop(t *testing.T) {
	rig := newAppliedOutcomeRig(t, nil)
	rig.script.outcome.push(422, `{"error":"refused"}`)
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); err != nil {
		t.Fatalf("TrackExperimentOutcome: %v", err)
	}
	rig.cycle()
	if rig.owed() != 0 || rig.outcomeDrops("apply_refused") != 1 || rig.drops("apply_refused") != 0 {
		t.Fatalf("a refused outcome is dropped and counted as an outcome: owed=%d outcome=%v exposure=%v",
			rig.owed(), rig.client.Snapshot().ExperimentOutcomeDrops, rig.client.Snapshot().ExperimentExposureDrops)
	}
}

func TestTheSentinelWithdrawsAnOwedOutcome(t *testing.T) {
	rig := newAppliedOutcomeRig(t, nil)
	rig.script.outcome.push(403, `{"error":"`+expSentinelRealSubjectsDisabled+`"}`)
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); err != nil {
		t.Fatalf("TrackExperimentOutcome: %v", err)
	}
	rig.cycle()
	if rig.owed() != 0 || rig.outcomeDrops("real_subjects_disabled") != 1 {
		t.Fatalf("the sentinel withdraws the owed outcome and counts it: owed=%d outcome=%v", rig.owed(), rig.client.Snapshot().ExperimentOutcomeDrops)
	}
	if variant := rig.client.ExperimentVariant(expTestScopeKey); variant != "" {
		t.Fatalf("the sentinel withdraws the assignment too, got %q", variant)
	}
}

// A consent withdrawal discards an owed outcome — a point event of the
// withdrawn period — and never re-arms it.
func TestAConsentPurgeDiscardsAnOwedOutcomeAndCountsIt(t *testing.T) {
	rig := newAppliedOutcomeRig(t, nil)
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); err != nil {
		t.Fatalf("TrackExperimentOutcome: %v", err)
	}
	rig.client.SetConsent(false)
	rig.client.SetConsent(true)
	if rig.outcomeDrops("consent_withdrawn") != 1 || rig.drops("consent_withdrawn") != 0 {
		t.Fatalf("the discarded outcome is counted as an outcome: outcome=%v exposure=%v",
			rig.client.Snapshot().ExperimentOutcomeDrops, rig.client.Snapshot().ExperimentExposureDrops)
	}
	rig.cycle()
	if got := len(rig.outcomeRequests()); got != 0 {
		t.Fatalf("a withdrawn outcome is never sent, got %d request(s)", got)
	}
}

func TestCloseCountsAnUnsealedOutcome(t *testing.T) {
	rig := newAppliedOutcomeRig(t, nil)
	rig.script.outcome.push(503, ``)
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); err != nil {
		t.Fatalf("TrackExperimentOutcome: %v", err)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = rig.client.Close(closeCtx)
	if n := rig.outcomeDrops("unsealed_at_close"); n != 1 {
		t.Fatalf("an outcome Close could not seal is counted, got %d", n)
	}
}

// Outcomes share the experiment's bounded owed queue: the oldest is dropped
// and counted as an outcome.
func TestOutcomesShareTheOwedBound(t *testing.T) {
	rig := newAppliedOutcomeRig(t, nil)
	for i := 0; i <= expMaxOwedExposures; i++ {
		if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", float64(i)); err != nil {
			t.Fatalf("TrackExperimentOutcome %d: %v", i, err)
		}
	}
	if rig.owed() != expMaxOwedExposures || rig.outcomeDrops("owed_bound_exceeded") != 1 {
		t.Fatalf("the bound drops the oldest outcome and counts it: owed=%d outcome=%v", rig.owed(), rig.client.Snapshot().ExperimentOutcomeDrops)
	}
	rig.cycle()
	if first := rig.outcomeRequests()[0].body["outcome_value"]; first != json.Number("1") {
		t.Fatalf("the oldest outcome is the one dropped: the first sent has value %v", first)
	}
}

// A repeat ApplyExperimentVariant of a delivered tuple records nothing, so
// it does not move the application an outcome follows either.
func TestARepeatApplicationDoesNotMoveTheOutcomesApplication(t *testing.T) {
	rig := newExpHopRig(t, nil)
	clock := &expFakeClock{now: time.Now()}
	rig.client.clock = clock
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.cycle()
	flushOrFail(t, rig.client)
	clock.advance(2 * time.Second)
	rig.client.ApplyExperimentVariant(expTestScopeKey) // delivered tuple: records nothing
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); err != nil {
		t.Fatalf("TrackExperimentOutcome: %v", err)
	}
	rig.cycle()
	applications, outcomes := rig.script.apply.requestsSoFar(), rig.outcomeRequests()
	if len(applications) != 1 || len(outcomes) != 1 {
		t.Fatalf("control: one application and one outcome sent, got %d and %d", len(applications), len(outcomes))
	}
	if got, want := outcomes[0].body["applied_at"], applications[0].body["applied_at"]; got != want {
		t.Fatalf("the outcome follows the recorded application (applied_at %v), got %v", want, got)
	}
}

// A subject re-mint (the platform refused the subject id's grammar) leaves
// no application to follow: an outcome of the rejected subject would be
// refused anyway.
func TestASubjectRemintLeavesNoApplicationToFollow(t *testing.T) {
	rig := newExpHopRig(t, nil, expAssignedBody("1"))
	rig.script.push(400, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`)
	rig.script.push(200, expAssignedBody("2"))
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); err != nil {
		t.Fatalf("control: an outcome of the applied subject is accepted, got %v", err)
	}
	if result := fetchAssignment(t, rig.client, expTestScopeKey); !result.Assigned || result.Version != 2 {
		t.Fatalf("control: the re-minted retry installs version 2, got %+v", result)
	}
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); !errors.Is(err, ErrExperimentNotApplied) {
		t.Fatalf("after a re-mint the new subject has applied nothing: ErrExperimentNotApplied, got %v", err)
	}
}

// A 200 from the outcome route that seals anything but an experiment_outcome
// is not a usable fact: the outcome stays owed, nothing is delivered.
func TestAnOutcomeSealedAsAnotherEventIsNotDelivered(t *testing.T) {
	rig := newAppliedOutcomeRig(t, nil)
	fact, _ := json.Marshal(map[string]any{
		"fact": map[string]any{
			"event_id": expStubFactID("wrong-kind"), "event_name": experimentExposureName,
			"event_ts": "2026-09-27T00:00:00+00:00", "workspace_id": "workspace-test", "app_id": "app-test",
			"environment_id": "develop", "props": map[string]any{"experiment_key": expTestScopeKey},
		},
		"seal": expStubSeal,
	})
	rig.script.outcome.push(200, string(fact))
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); err != nil {
		t.Fatalf("TrackExperimentOutcome: %v", err)
	}
	rig.cycle()
	flushOrFail(t, rig.client)
	if rig.owed() != 1 || len(rig.capture.exposures()) != 1 || len(rig.capture.byName(experimentOutcomeName)) != 0 {
		t.Fatalf("a fact sealed as another event is kept, not delivered: owed=%d exposures=%d outcomes=%d",
			rig.owed(), len(rig.capture.exposures()), len(rig.capture.byName(experimentOutcomeName)))
	}
}
