package shardpilot

// Outcomes and age declarations on an ExperimentSubject handle: an outcome
// follows the handle's own application, and an age refusal withdraws what
// the handle owes for the experiment, its queued facts included.

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
)

// subjectAgeRig is an age rig seen through the hop rig's helpers.
func subjectAgeRig(t *testing.T, bodies ...string) *expHopRig {
	t.Helper()
	rig := newAgeWithdrawRig(t, "", nil, bodies...)
	return &expHopRig{script: rig.script, capture: rig.capture, client: rig.client}
}

func TestAHandlesOutcomeFollowsItsOwnApplication(t *testing.T) {
	rig := newExpHopRig(t, nil)
	s := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
	subjectFetchOrFail(t, s)
	s.Apply(expTestScopeKey)
	rig.cycle()
	application := rig.script.apply.requestsSoFar()[0].body
	if err := s.TrackOutcome(expTestScopeKey, "purchase", 42); err != nil {
		t.Fatalf("TrackOutcome: %v", err)
	}
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); !errors.Is(err, ErrExperimentNoAssignment) {
		t.Fatalf("the client's own outcome = %v, want ErrExperimentNoAssignment: the handle's application is not the client's", err)
	}
	rig.cycle()
	requests := rig.outcomeRequests()
	if len(requests) != 1 {
		t.Fatalf("outcome apply requests = %d, want 1", len(requests))
	}
	body := requests[0].body
	for _, member := range []string{"subject_key", "experiment_version", "variant_key", "applied_at"} {
		if !reflect.DeepEqual(body[member], application[member]) {
			t.Fatalf("%s = %v, want the handle's application's %v", member, body[member], application[member])
		}
	}
	if body["subject_key"] != expSubjectA || body["outcome_key"] != "purchase" || body["outcome_value"] != json.Number("42") {
		t.Fatalf("outcome request = %v", body)
	}
	flushOrFail(t, rig.client)
	facts := rig.capture.byName(experimentOutcomeName)
	if len(facts) != 1 || facts[0]["anonymous_id"] != "player-a" || facts[0]["user_id"] != nil {
		t.Fatalf("delivered outcomes %v, want one with the player's anonymous_id and no user_id", facts)
	}
}

func TestAHandlesOutcomeIsRefusedAsTheClientsIs(t *testing.T) {
	rig := newExpHopRig(t, nil)
	s := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
	if err := s.TrackOutcome(expTestScopeKey, "purchase", 1); !errors.Is(err, ErrExperimentNoAssignment) {
		t.Fatalf("before a fetch: %v, want ErrExperimentNoAssignment", err)
	}
	subjectFetchOrFail(t, s)
	if err := s.TrackOutcome(expTestScopeKey, "purchase", 1); !errors.Is(err, ErrExperimentNotApplied) {
		t.Fatalf("before an application: %v, want ErrExperimentNotApplied", err)
	}
	s.Apply(expTestScopeKey)
	for _, tc := range []struct {
		key   string
		value float64
	}{{"", 1}, {"10.0.0.1", 1}, {"purchase", 1.5}, {"purchase", math.Inf(1)}} {
		if err := s.TrackOutcome(expTestScopeKey, tc.key, tc.value); !errors.Is(err, ErrInvalidExperimentFact) {
			t.Fatalf("TrackOutcome(%q, %v) = %v, want ErrInvalidExperimentFact", tc.key, tc.value, err)
		}
	}
	anonymous := newSubjectOrFail(t, rig.client, expSubjectB, "")
	subjectFetchOrFail(t, anonymous)
	if err := anonymous.TrackOutcome(expTestScopeKey, "purchase", 1); !errors.Is(err, ErrExperimentFactUnavailable) {
		t.Fatalf("a handle without an AnonymousID: %v, want ErrExperimentFactUnavailable", err)
	}
	if got := rig.outcomeDrops(expDropNotRecordable); got != 1 {
		t.Fatalf("ExperimentOutcomeDrops[not_recordable] = %d, want 1", got)
	}
	if err := s.SetConsent(ExperimentSubjectConsentDenied); err != nil {
		t.Fatal(err)
	}
	if err := s.TrackOutcome(expTestScopeKey, "purchase", 1); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("a denied player: %v, want ErrConsentDenied", err)
	}
	rig.cycle()
	if got := len(rig.outcomeRequests()); got != 0 {
		t.Fatalf("outcome apply requests = %d, want none", got)
	}
}

// TestAHandlesAgeRefusalWithdrawsItsQueuedFacts: the refusal withdraws the
// owed outcome, the exposure already in the queue and the application an
// outcome would follow; another player's fact of the experiment is sent.
func TestAHandlesAgeRefusalWithdrawsItsQueuedFacts(t *testing.T) {
	rig := subjectAgeRig(t, ageGolden(t, "adult"), ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"), ageGolden(t, "adult"))
	a := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
	b := newSubjectOrFail(t, rig.client, expSubjectB, "player-b")
	for _, s := range []*ExperimentSubject{a, b} {
		if _, err := s.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err != nil {
			t.Fatalf("setup: %v", err)
		}
		s.Apply(ageGoldenExperiment)
	}
	rig.cycle()
	if a.owedCount() != 0 || b.owedCount() != 0 {
		t.Fatal("control: both exposures are handed to the queue")
	}
	if err := a.TrackOutcome(ageGoldenExperiment, "score", 1); err != nil {
		t.Fatalf("setup: %v", err)
	}
	result, err := a.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
	if err != nil || result.Assigned || result.Reason != "age_ineligible" {
		t.Fatalf("the refusal = %+v, %v", result, err)
	}
	if got := rig.outcomeDrops(expDropAgeIneligible); got != 1 {
		t.Fatalf("ExperimentOutcomeDrops[age_ineligible] = %d, want the owed outcome", got)
	}
	if err := a.TrackOutcome(ageGoldenExperiment, "score", 1); !errors.Is(err, ErrExperimentNoAssignment) {
		t.Fatalf("an outcome after the refusal: %v, want ErrExperimentNoAssignment", err)
	}
	flushOrFail(t, rig.client)
	facts := deliveredExperimentFacts(rig.capture, ageGoldenExperiment)
	if len(facts) != 1 || facts[0]["anonymous_id"] != "player-b" {
		t.Fatalf("delivered %v, want only player b's exposure", facts)
	}
	if dropped := rig.client.Snapshot().Dropped; dropped != 1 {
		t.Fatalf("Dropped = %d, want player a's queued exposure", dropped)
	}
	// A later adult assignment of the same version records afresh.
	if _, err := a.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err != nil {
		t.Fatalf("re-declaring adult: %v", err)
	}
	a.Apply(ageGoldenExperiment)
	if owed := a.owedCount(); owed != 1 {
		t.Fatalf("%d owed application(s) after re-applying, want 1", owed)
	}
}

// TestAHandlesApplyRouteAgeRefusalWithdrawsItsQueuedFacts: the outcome
// route's age refusal withdraws the exposure already in the queue.
func TestAHandlesApplyRouteAgeRefusalWithdrawsItsQueuedFacts(t *testing.T) {
	rig := subjectAgeRig(t, ageGolden(t, "adult"))
	s := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
	if _, err := s.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err != nil {
		t.Fatalf("setup: %v", err)
	}
	s.Apply(ageGoldenExperiment)
	rig.cycle()
	if err := s.TrackOutcome(ageGoldenExperiment, "score", 1); err != nil {
		t.Fatalf("setup: %v", err)
	}
	rig.script.outcome.push(409, ageApplyRefusal)
	rig.cycle()
	if got := rig.outcomeDrops(expDropAgeIneligible); got != 1 {
		t.Fatalf("ExperimentOutcomeDrops[age_ineligible] = %d, want the refused outcome", got)
	}
	flushOrFail(t, rig.client)
	if facts := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(facts) != 0 {
		t.Fatalf("delivered %v after the age refusal, want nothing", facts)
	}
}

// TestAHandlesNonAdultDeclarationTakesEffectAtOnce: while the declaring
// fetch is in flight the assignment is neither served nor recorded, and a
// fetch that ends without an answer withdraws what the handle owes.
func TestAHandlesNonAdultDeclarationTakesEffectAtOnce(t *testing.T) {
	rig := subjectAgeRig(t, ageGolden(t, "adult"))
	s := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
	if _, err := s.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err != nil {
		t.Fatalf("setup: %v", err)
	}
	s.Apply(ageGoldenExperiment)
	if err := s.TrackOutcome(ageGoldenExperiment, "score", 1); err != nil {
		t.Fatalf("setup: %v", err)
	}
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock) // a failure must not leave the server's handler held
	rig.script.mu.Lock()
	rig.script.gates = map[int]chan struct{}{1: release}
	rig.script.mu.Unlock()
	rig.answerNext(expScriptResponse{status: 503, body: `{}`})
	declared := make(chan error, 1)
	go func() {
		_, err := s.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
		declared <- err
	}()
	waitForRequests(t, rig, 2)
	if variant := s.Variant(ageGoldenExperiment); variant != "" {
		t.Fatalf("while the non-adult declaration is in flight the handle serves %q", variant)
	}
	if err := s.TrackOutcome(ageGoldenExperiment, "score", 2); !errors.Is(err, ErrExperimentNoAssignment) {
		t.Fatalf("an outcome during the declaration: %v, want ErrExperimentNoAssignment", err)
	}
	unblock()
	if err := <-declared; err == nil {
		t.Fatal("control: the 503 fails the declaring fetch")
	}
	if exposures, outcomes := rig.drops(expDropAgeIneligible), rig.outcomeDrops(expDropAgeIneligible); exposures != 1 || outcomes != 1 {
		t.Fatalf("age_ineligible drops = %d exposure(s), %d outcome(s); want the owed application and outcome", exposures, outcomes)
	}
	if owed := s.owedCount(); owed != 0 {
		t.Fatalf("%d record(s) still owed", owed)
	}
	if variant := s.Variant(ageGoldenExperiment); variant != "" {
		t.Fatalf("after an unanswered non-adult declaration the handle serves %q", variant)
	}
}

func TestAHandlesFetchWithAgeBandDeclaresTheBand(t *testing.T) {
	rig := newExpHopRig(t, nil)
	s := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
	if _, err := s.FetchWithAgeBand(context.Background(), expTestScopeKey, ExperimentAgeBand("teen"), nil); !errors.Is(err, ErrInvalidExperimentAgeBand) {
		t.Fatalf("an unknown band: %v, want ErrInvalidExperimentAgeBand", err)
	}
	if _, err := s.FetchWithAgeBand(context.Background(), expTestScopeKey, ExperimentAgeBandAdult, map[string]string{"age_band": "unknown"}); !errors.Is(err, ErrInvalidExperimentAgeBand) {
		t.Fatalf("a conflicting age_band: %v, want ErrInvalidExperimentAgeBand", err)
	}
	if got := rig.script.requestCount(); got != 0 {
		t.Fatalf("a refused declaration sent %d request(s)", got)
	}
	attributes := map[string]string{}
	if _, err := s.FetchWithAgeBand(context.Background(), expTestScopeKey, ExperimentAgeBandAdult, attributes); err != nil {
		t.Fatalf("FetchWithAgeBand: %v", err)
	}
	if got := rig.script.request(0).URL.Query().Get("age_band"); got != "adult" || len(attributes) != 0 {
		t.Fatalf("age_band = %q and the caller's attributes %v; want adult sent and the map untouched", got, attributes)
	}
}
