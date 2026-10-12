package shardpilot

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

// One process, many players: an ExperimentSubject handle fetches, serves
// and records for its own subject and identity only.

const (
	expSubjectA = "spcid_player-a-0000000000000000000001"
	expSubjectB = "spcid_player-b-0000000000000000000002"
)

func expControlServedBody() string {
	return strings.Replace(expAssignedServedBody(expServedTrio), `"variant_key":"treatment"`, `"variant_key":"control"`, 1)
}

func newSubjectOrFail(t *testing.T, c *Client, subjectKey, anonymousID string) *ExperimentSubject {
	t.Helper()
	s, err := c.ExperimentSubject(ExperimentSubjectConfig{SubjectKey: subjectKey, AnonymousID: anonymousID, Consent: ExperimentSubjectConsentGranted})
	if err != nil {
		t.Fatalf("ExperimentSubject(%q): %v", subjectKey, err)
	}
	return s
}

func subjectFetchOrFail(t *testing.T, s *ExperimentSubject) ExperimentAssignmentResult {
	t.Helper()
	result, err := s.Fetch(context.Background(), expTestScopeKey, nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	return result
}

// answerNext replaces the scripted assignment answers.
func (r *expHopRig) answerNext(responses ...expScriptResponse) {
	r.script.mu.Lock()
	defer r.script.mu.Unlock()
	r.script.responses = responses
}

func (r *expHopRig) assignmentSubjects() []string {
	subjects := make([]string, 0, r.script.requestCount())
	for i := 0; i < r.script.requestCount(); i++ {
		subjects = append(subjects, r.script.request(i).URL.Query().Get("subject_key"))
	}
	return subjects
}

func (r *expHopRig) processSubjectMinted() bool {
	e := r.client.exp
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.subjectID != ""
}

func TestExperimentSubjectsFetchServeAndRecordForTheirOwnPlayer(t *testing.T) {
	rig := newExpHopRig(t, nil, expAssignedServedBody(expServedTrio), expControlServedBody())
	a := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
	b := newSubjectOrFail(t, rig.client, expSubjectB, "player-b")
	if got := subjectFetchOrFail(t, a); got.VariantKey != "treatment" {
		t.Fatalf("player a's verdict = %+v, want treatment", got)
	}
	if got := subjectFetchOrFail(t, b); got.VariantKey != "control" {
		t.Fatalf("player b's verdict = %+v, want control", got)
	}
	if got := rig.assignmentSubjects(); len(got) != 2 || got[0] != expSubjectA || got[1] != expSubjectB {
		t.Fatalf("assignment subject_key per request = %v, want each handle's own", got)
	}
	if a.Variant(expTestScopeKey) != "treatment" || b.Variant(expTestScopeKey) != "control" {
		t.Fatalf("each handle serves its own assignment: a=%q b=%q", a.Variant(expTestScopeKey), b.Variant(expTestScopeKey))
	}
	if got := rig.client.ExperimentVariant(expTestScopeKey); got != "" {
		t.Fatalf("a handle's assignment leaked into the client's own plane: %q", got)
	}
	if rig.processSubjectMinted() {
		t.Fatal("the client minted its own subject for a handle's fetch")
	}

	if variant, _ := a.Apply(expTestScopeKey); variant != "treatment" {
		t.Fatalf("Apply served %q", variant)
	}
	a.Apply(expTestScopeKey)
	b.Apply(expTestScopeKey)
	rig.cycle()
	flushOrFail(t, rig.client)
	requests := rig.script.apply.requestsSoFar()
	if len(requests) != 2 {
		t.Fatalf("apply requests = %d, want one per handle (an application records once per handle)", len(requests))
	}
	appliedSubjects := map[string]string{}
	for _, request := range requests {
		subject, _ := request.body["subject_key"].(string)
		variant, _ := request.body["variant_key"].(string)
		appliedSubjects[subject] = variant
	}
	if appliedSubjects[expSubjectA] != "treatment" || appliedSubjects[expSubjectB] != "control" {
		t.Fatalf("applications by subject = %v", appliedSubjects)
	}
	identities := map[string]bool{}
	for _, fact := range rig.capture.exposures() {
		anonymousID, _ := fact["anonymous_id"].(string)
		identities[anonymousID] = true
		if _, present := fact["user_id"]; present {
			t.Fatalf("an experiment fact carries user_id: %v", fact)
		}
	}
	if len(identities) != 2 || !identities["player-a"] || !identities["player-b"] {
		t.Fatalf("exposure identities = %v, want each player's own and never the client's", identities)
	}
}

func TestExperimentSubjectConsentGatesEveryCall(t *testing.T) {
	rig := newExpHopRig(t, nil)
	s, err := rig.client.ExperimentSubject(ExperimentSubjectConfig{SubjectKey: expSubjectA, AnonymousID: "player-a"})
	if err != nil {
		t.Fatalf("ExperimentSubject: %v", err)
	}
	if _, err := s.Fetch(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("undecided player: Fetch = %v, want ErrConsentUnknown", err)
	}
	if err := s.SetConsent(ExperimentSubjectConsentDenied); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Fetch(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("denied player: Fetch = %v, want ErrConsentDenied", err)
	}
	if got := rig.script.requestCount(); got != 0 {
		t.Fatalf("a refused player's fetch reached the network (%d request(s))", got)
	}

	if err := s.SetConsent(ExperimentSubjectConsentGranted); err != nil {
		t.Fatal(err)
	}
	subjectFetchOrFail(t, s)
	s.Apply(expTestScopeKey)
	if err := s.SetConsent(ExperimentSubjectConsentDenied); err != nil {
		t.Fatal(err)
	}
	if got := rig.drops(expDropConsentWithdrawn); got != 1 {
		t.Fatalf("consent_withdrawn = %d, want the owed application", got)
	}
	if variant, _ := s.Apply(expTestScopeKey); variant != "" || s.Variant(expTestScopeKey) != "" {
		t.Fatal("a denied player is still served")
	}
	rig.cycle()
	flushOrFail(t, rig.client)
	if len(rig.script.apply.requestsSoFar()) != 0 || len(rig.capture.exposures()) != 0 {
		t.Fatal("a denied player's application was delivered")
	}

	// The client's own denial refuses a granted player too.
	if err := s.SetConsent(ExperimentSubjectConsentGranted); err != nil {
		t.Fatal(err)
	}
	rig.client.SetConsent(false)
	before := rig.script.requestCount()
	if _, err := s.Fetch(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("client denied: Fetch = %v, want ErrConsentDenied", err)
	}
	if rig.script.requestCount() != before {
		t.Fatal("a fetch under the client's denial reached the network")
	}
}

func TestExperimentSubjectRefusesAnInvalidSubject(t *testing.T) {
	rig := newExpHopRig(t, nil)
	for _, cfg := range []ExperimentSubjectConfig{
		{SubjectKey: "player-1"},
		{SubjectKey: ""},
		{SubjectKey: "spcid_short"},
		{SubjectKey: expSubjectA, AnonymousID: strings.Repeat("a", 513)},
		{SubjectKey: expSubjectA, Consent: ExperimentSubjectConsent(7)},
	} {
		if _, err := rig.client.ExperimentSubject(cfg); !errors.Is(err, ErrInvalidExperimentSubject) {
			t.Fatalf("ExperimentSubject(%+v) = %v, want ErrInvalidExperimentSubject", cfg, err)
		}
	}

	// A subject the platform refuses is the host's to fix: no re-mint, no
	// retry under another subject.
	rig.answerNext(expScriptResponse{status: 400, body: `{"error":{"code":"` + expSentinelSubjectGrammar + `"}}`})
	s := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
	if _, err := s.Fetch(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrInvalidExperimentSubject) {
		t.Fatalf("Fetch = %v, want ErrInvalidExperimentSubject", err)
	}
	if got := rig.assignmentSubjects(); len(got) != 1 || got[0] != expSubjectA {
		t.Fatalf("assignment requests = %v, want the one request with the handle's subject", got)
	}
	if rig.processSubjectMinted() {
		t.Fatal("the refusal minted a client subject")
	}
}

func TestExperimentSubjectRequiresTheExperimentPlane(t *testing.T) {
	rig := newExpHopRig(t, func(cfg *Config) { cfg.ExperimentsEnabled = false })
	if _, err := rig.client.ExperimentSubject(ExperimentSubjectConfig{SubjectKey: expSubjectA}); !errors.Is(err, ErrExperimentsNotConfigured) {
		t.Fatalf("ExperimentSubject = %v, want ErrExperimentsNotConfigured", err)
	}
}

func TestExperimentSubjectCloseDeliversItsApplications(t *testing.T) {
	rig := newExpHopRig(t, nil)
	s := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
	subjectFetchOrFail(t, s)
	s.Apply(expTestScopeKey)
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := len(rig.script.apply.requestsSoFar()); got != 1 {
		t.Fatalf("Close made %d apply request(s), want 1", got)
	}
	flushOrFail(t, rig.client)
	if facts := rig.capture.exposures(); len(facts) != 1 || facts[0]["anonymous_id"] != "player-a" {
		t.Fatalf("delivered exposures = %v, want the player's one", facts)
	}
	if _, err := s.Fetch(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Fetch after Close = %v, want ErrClosed", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("a second Close = %v", err)
	}

	// The client's Close does the same for a handle still open.
	other := newSubjectOrFail(t, rig.client, expSubjectB, "player-b")
	subjectFetchOrFail(t, other)
	other.Apply(expTestScopeKey)
	if err := rig.client.Close(context.Background()); err != nil {
		t.Fatalf("client Close: %v", err)
	}
	if got := len(rig.script.apply.requestsSoFar()); got != 2 {
		t.Fatalf("apply requests after the client's Close = %d, want 2", got)
	}
	identities := map[string]bool{}
	for _, fact := range rig.capture.exposures() {
		identities[fact["anonymous_id"].(string)] = true
	}
	if !identities["player-b"] {
		t.Fatalf("the client's Close did not deliver the open handle's application: %v", identities)
	}
}

func TestExperimentSubjectCredentialLatchIsPlaneWide(t *testing.T) {
	rig := newExpHopRig(t, nil, expAssignedServedBody(expServedTrio))
	a := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
	b := newSubjectOrFail(t, rig.client, expSubjectB, "player-b")
	subjectFetchOrFail(t, a)
	rig.answerNext(expScriptResponse{status: 401, body: `{"error":{"code":"unauthorized"}}`}, expScriptResponse{status: 200, body: expAssignedServedBody(expServedTrio)})
	if _, err := b.Fetch(context.Background(), expTestScopeKey, nil); err == nil {
		t.Fatal("a 401 must fail the fetch")
	}
	if got := a.Variant(expTestScopeKey); got != "" {
		t.Fatalf("the shared credential failed, yet another handle still serves %q", got)
	}
	subjectFetchOrFail(t, a)
	if got := a.Variant(expTestScopeKey); got != "treatment" {
		t.Fatalf("an authorized fetch after the latch serves again: got %q", got)
	}
}

// TestExperimentSubjectsRunConcurrently drives handles, the lane, consent
// changes and the client's denial at once (run it with -race): no call
// deadlocks, and Close still ends every handle.
func TestExperimentSubjectsRunConcurrently(t *testing.T) {
	rig := newExpHopRig(t, nil)
	subjects := []*ExperimentSubject{
		newSubjectOrFail(t, rig.client, expSubjectA, "player-a"),
		newSubjectOrFail(t, rig.client, expSubjectB, "player-b"),
	}
	var wg sync.WaitGroup
	for _, s := range subjects {
		wg.Add(1)
		go func(s *ExperimentSubject) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				_, _ = s.Fetch(context.Background(), expTestScopeKey, nil)
				s.Apply(expTestScopeKey)
				if i%5 == 4 {
					_ = s.SetConsent(ExperimentSubjectConsentDenied)
					_ = s.SetConsent(ExperimentSubjectConsentGranted)
				}
			}
		}(s)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			rig.cycle()
			if i == 10 {
				rig.client.SetConsent(false)
				rig.client.SetConsent(true)
			}
		}
	}()
	wg.Wait()
	if err := rig.client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, s := range subjects {
		if _, err := s.Fetch(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrClosed) {
			t.Fatalf("a handle outlived the client's Close: %v", err)
		}
	}
}

// TestAnOversizeSealedFactLeavesAHandlesQueue: a handle's sealed fact that
// ingest would reject is dropped once and leaves the handle's queue, so the
// handle's later facts are delivered.
func TestAnOversizeSealedFactLeavesAHandlesQueue(t *testing.T) {
	rig := newExpHopRig(t, nil)
	s := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
	rig.script.apply.mu.Lock()
	rig.script.apply.padProps = 3 * maxEventSceneBytes
	rig.script.apply.mu.Unlock()
	subjectFetchOrFail(t, s)
	s.Apply(expTestScopeKey)
	rig.cycle()

	rig.script.apply.mu.Lock()
	rig.script.apply.padProps = 0
	rig.script.apply.mu.Unlock()
	const second = "exp-second"
	rig.answerNext(expScriptResponse{status: 200, body: expServedBodyForKey(second)})
	if _, err := s.Fetch(context.Background(), second, nil); err != nil {
		t.Fatalf("Fetch(%q): %v", second, err)
	}
	s.Apply(second)
	rig.cycle()
	rig.cycle()
	flushOrFail(t, rig.client)
	if owed := s.owedCount(); owed != 0 {
		t.Fatalf("%d application(s) still owed: the oversize fact holds the handle's queue", owed)
	}
	if got := rig.drops(expDropEventTooLarge); got != 1 {
		t.Fatalf("ExperimentExposureDrops[event_too_large] = %d, want 1", got)
	}
	facts := rig.capture.exposures()
	if len(facts) != 1 {
		t.Fatalf("delivered %d exposure(s), want the second experiment's one", len(facts))
	}
	if props, _ := facts[0]["props"].(map[string]any); props["experiment_key"] != second {
		t.Fatalf("delivered %v, want the second experiment's exposure", facts[0])
	}
}
