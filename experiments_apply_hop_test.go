package shardpilot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// The apply hop: an application is sealed by the platform's exposure apply
// endpoint on the background lane (never inside a host call), and the fact it
// returns is posted verbatim, with its seal, on the analytics lane.

// expApplyStub is the stub exposure apply endpoint. By default it seals by
// echoing the request: the fact id is derived from exposure_id, the event
// time is applied_at in an equivalent but non-canonical spelling ("+00:00"
// for "Z", so a client that re-formats the time instead of carrying it
// verbatim is caught), the scope is the configured test scope. Pushed
// responses are served in order, the last repeating.
type expApplyStub struct {
	mu        sync.Mutex
	responses []expScriptResponse
	requests  []expApplyRequest
	facts     []map[string]any
	workspace string // the sealed fact's workspace_id; "" means the test client's
	// onRequest, when set, runs while a request is being answered (outside
	// the stub's lock): the window in which the SDK's hop is in flight.
	onRequest func()
}

type expApplyRequest struct {
	header http.Header
	body   map[string]any
}

const (
	expApplyEcho = "\x00echo"
	expStubSeal  = "sps1.0123456789abcdef.c3R1Yi1zZWFs"
)

func (s *expApplyStub) push(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, expScriptResponse{status: status, body: body})
}

func (s *expApplyStub) pushRetryAfter(status int, body, retryAfter string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, expScriptResponse{status: status, body: body, retryAfter: retryAfter})
}

func (s *expApplyStub) requestsSoFar() []expApplyRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]expApplyRequest(nil), s.requests...)
}

func (s *expApplyStub) sealedFacts() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]map[string]any(nil), s.facts...)
}

// expStubFactID is the stub's fact id for an exposure_id.
func expStubFactID(exposureID string) string {
	sum := sha256.Sum256([]byte(exposureID))
	return "cpx1_" + hex.EncodeToString(sum[:])
}

func (s *expApplyStub) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		s.mu.Lock()
		s.requests = append(s.requests, expApplyRequest{header: r.Header.Clone(), body: body})
		response := expScriptResponse{status: 200, body: expApplyEcho}
		switch {
		case len(s.responses) == 1:
			response = s.responses[0]
		case len(s.responses) > 1:
			response = s.responses[0]
			s.responses = s.responses[1:]
		}
		payload := response.body
		if response.status == 200 && payload == expApplyEcho {
			workspace := s.workspace
			if workspace == "" {
				workspace = "workspace-test"
			}
			exposureID, _ := body["exposure_id"].(string)
			appliedAt, _ := body["applied_at"].(string)
			fact := map[string]any{
				"event_id":       expStubFactID(exposureID),
				"event_name":     experimentExposureName,
				"event_ts":       strings.Replace(appliedAt, "Z", "+00:00", 1),
				"workspace_id":   workspace,
				"app_id":         body["app_key"],
				"environment_id": body["environment_key"],
				"props": map[string]any{
					"experiment_key":     body["experiment_key"],
					"experiment_version": body["experiment_version"],
					"assignment_key":     "sfk1_" + strings.Repeat("a", 64),
					"variant_key":        body["variant_key"],
					"assignment_unit":    "client_id",
					"attestation":        "client_attested",
				},
			}
			s.facts = append(s.facts, fact)
			encoded, _ := json.Marshal(map[string]any{"fact": fact, "seal": expStubSeal})
			payload = string(encoded)
		}
		hook := s.onRequest
		s.mu.Unlock()
		if hook != nil {
			hook()
		}
		if response.retryAfter != "" {
			w.Header().Set("Retry-After", response.retryAfter)
		}
		if response.status >= 300 && response.status < 400 {
			w.Header().Set("Location", "/elsewhere")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.status)
		_, _ = w.Write([]byte(payload))
	}
}

// expHopRig is one client against the stub, with the served state pinned on
// every assignment it fetches.
type expHopRig struct {
	script  *expScript
	capture *expWireCapture
	client  *Client
}

func newExpHopRig(t *testing.T, mutate func(*Config), bodies ...string) *expHopRig {
	t.Helper()
	script := &expScript{}
	if len(bodies) == 0 {
		bodies = []string{expAssignedServedBody(expServedTrio)}
	}
	for _, body := range bodies {
		script.push(200, body)
	}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	t.Cleanup(server.Close)
	client := newExperimentClient(t, server.URL, mutate)
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	client.SetConsent(true)
	return &expHopRig{script: script, capture: capture, client: client}
}

func expServedBodyForKey(key string) string {
	return strings.Replace(expAssignedServedBody(expServedTrio), `"experiment_key":"`+expTestScopeKey+`"`, `"experiment_key":"`+key+`"`, 1)
}

func (r *expHopRig) cycle() { r.client.experimentCycle(context.Background()) }

func (r *expHopRig) owed() int { return r.client.owedExperimentExposureCount() }

func (r *expHopRig) drops(code string) uint64 {
	return r.client.Snapshot().ExperimentExposureDrops[code]
}

var expExposureIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

func TestAnApplicationIsSealedByTheApplyHopAndDeliveredVerbatim(t *testing.T) {
	rig := newExpHopRig(t, nil)
	fetchAssignment(t, rig.client, expTestScopeKey)
	if variant, _ := rig.client.ApplyExperimentVariant(expTestScopeKey); variant != "treatment" {
		t.Fatalf("the application must serve the variant, got %q", variant)
	}
	if got := len(rig.script.apply.requestsSoFar()); got != 0 {
		t.Fatalf("the application must not reach the network inside the host call, got %d apply request(s)", got)
	}
	// Nor inside another host call: a fetch that settles while the
	// application is owed does not send it either.
	fetchAssignment(t, rig.client, expTestScopeKey)
	if got := len(rig.script.apply.requestsSoFar()); got != 0 {
		t.Fatalf("a host fetch must not send an owed application, got %d apply request(s)", got)
	}
	rig.cycle()
	requests := rig.script.apply.requestsSoFar()
	if len(requests) != 1 {
		t.Fatalf("the lane must send the application once, got %d request(s)", len(requests))
	}
	request := requests[0]
	if got := request.header.Get("Authorization"); got != "Bearer test-exp-key" {
		t.Fatalf("the hop authenticates with the publishable key, got %q", got)
	}
	want := map[string]bool{
		"app_key": true, "environment_key": true, "experiment_key": true, "experiment_version": true,
		"subject_key": true, "variant_key": true, "exposure_id": true, "served_revision": true,
		"served_kill_gate": true, "served_at": true, "applied_at": true, "attributes": true,
	}
	for member := range request.body {
		if !want[member] {
			t.Fatalf("the request carries %q, which is not the application's; no identity or session member may ride it: %v", member, request.body)
		}
	}
	for member := range want {
		if _, present := request.body[member]; !present {
			t.Fatalf("the request lacks %q: %v", member, request.body)
		}
	}
	body := request.body
	subject, _ := body["subject_key"].(string)
	exposureID, _ := body["exposure_id"].(string)
	appliedAt, _ := body["applied_at"].(string)
	if body["app_key"] != "app-test" || body["environment_key"] != "develop" || body["experiment_key"] != expTestScopeKey ||
		body["experiment_version"] != float64(3) || body["variant_key"] != "treatment" || !strings.HasPrefix(subject, "spcid_") ||
		!expExposureIDPattern.MatchString(exposureID) || body["served_revision"] != float64(7) || body["served_kill_gate"] != true ||
		body["served_at"] != "2026-09-27T01:00:00.123456789Z" {
		t.Fatalf("the request must carry the pinned application: %v", body)
	}
	if _, err := time.Parse(time.RFC3339Nano, appliedAt); err != nil {
		t.Fatalf("applied_at must be RFC 3339, got %q", appliedAt)
	}
	if attributes, ok := body["attributes"].(map[string]any); !ok || len(attributes) != 0 {
		t.Fatalf("the fetch sent no attributes, so the application carries an empty set, got %v", body["attributes"])
	}

	flushOrFail(t, rig.client)
	facts := rig.capture.exposures()
	if len(facts) != 1 {
		t.Fatalf("the sealed fact must be delivered once, got %d", len(facts))
	}
	delivered, sealed := facts[0], rig.script.apply.sealedFacts()[0]
	if delivered["event_id"] != sealed["event_id"] || delivered["event_ts"] != sealed["event_ts"] ||
		delivered["workspace_id"] != sealed["workspace_id"] || delivered["app_id"] != sealed["app_id"] ||
		delivered["environment_id"] != sealed["environment_id"] {
		t.Fatalf("the envelope must carry the sealed members verbatim:\ndelivered %v\nsealed    %v", delivered, sealed)
	}
	sealedProps, _ := json.Marshal(sealed["props"])
	var wantProps map[string]any
	_ = json.Unmarshal(sealedProps, &wantProps)
	if !reflect.DeepEqual(delivered["props"], wantProps) {
		t.Fatalf("the props must be the sealed props exactly:\ndelivered %v\nsealed    %v", delivered["props"], wantProps)
	}
	if delivered["attestation_seal"] != expStubSeal || delivered["source"] != "client" ||
		delivered["anonymous_id"] != "anon-test" || delivered["session_id"] == nil || delivered["user_id"] != nil {
		t.Fatalf("the envelope must carry the seal and the SDK's own client identity and session: %v", delivered)
	}
	if strings.Contains(mustJSON(t, delivered), subject) {
		t.Fatal("the raw subject must never enter an analytics event")
	}
}

// A consent purge re-arms an applied tuple with its ORIGINAL exposure_id, so
// the platform derives the same fact id and a fact that survived collapses.
func TestAPurgeReArmReSendsTheSameExposureID(t *testing.T) {
	rig := newExpHopRig(t, nil)
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.cycle()
	flushOrFail(t, rig.client)
	rig.client.SetConsent(false)
	rig.client.SetConsent(true)
	rig.cycle()
	flushOrFail(t, rig.client)
	requests := rig.script.apply.requestsSoFar()
	if len(requests) != 2 || requests[0].body["exposure_id"] != requests[1].body["exposure_id"] ||
		requests[0].body["applied_at"] != requests[1].body["applied_at"] {
		t.Fatalf("the re-armed application must be re-sent with its original exposure_id and applied_at: %d request(s)", len(requests))
	}
	facts := rig.capture.exposures()
	if len(facts) != 2 || facts[0]["event_id"] != facts[1]["event_id"] {
		t.Fatalf("both deliveries must carry one fact id, got %d fact(s)", len(facts))
	}
}

// An extra exposure (TrackExperimentExposure) is a new application with its
// own exposure_id.
func TestAnExtraExposureIsItsOwnApplication(t *testing.T) {
	rig := newExpHopRig(t, nil)
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	if err := rig.client.TrackExperimentExposure(expTestScopeKey); err != nil {
		t.Fatalf("TrackExperimentExposure: %v", err)
	}
	if got := len(rig.script.apply.requestsSoFar()); got != 0 {
		t.Fatalf("an extra exposure must not reach the network inside the host call either, got %d", got)
	}
	rig.cycle()
	flushOrFail(t, rig.client)
	requests := rig.script.apply.requestsSoFar()
	if len(requests) != 2 || requests[0].body["exposure_id"] == requests[1].body["exposure_id"] {
		t.Fatalf("two applications must carry two exposure_ids, got %d request(s)", len(requests))
	}
	if facts := rig.capture.exposures(); len(facts) != 2 || facts[0]["event_id"] == facts[1]["event_id"] {
		t.Fatalf("two applications must deliver two facts, got %d", len(facts))
	}
}

// ── dispositions ────────────────────────────────────────────────────────────

func TestAnApplyRefusalDropsTheApplication(t *testing.T) {
	for _, status := range []int{400, 404, 409, 422} {
		rig := newExpHopRig(t, nil)
		rig.script.apply.push(status, `{"error":"refused"}`)
		fetchAssignment(t, rig.client, expTestScopeKey)
		rig.client.ApplyExperimentVariant(expTestScopeKey)
		rig.cycle()
		rig.cycle()
		flushOrFail(t, rig.client)
		if rig.owed() != 0 || rig.drops("apply_refused") != 1 || len(rig.script.apply.requestsSoFar()) != 1 ||
			len(rig.capture.exposures()) != 0 {
			t.Fatalf("%d: a refused application is dropped once and counted apply_refused: owed=%d drops=%d requests=%d facts=%d",
				status, rig.owed(), rig.drops("apply_refused"), len(rig.script.apply.requestsSoFar()), len(rig.capture.exposures()))
		}
	}
}

func TestAnAuthRefusalKeepsTheApplicationUntilAnAuthorizedFetch(t *testing.T) {
	for _, status := range []int{401, 403} {
		rig := newExpHopRig(t, nil)
		rig.script.apply.push(status, `{"error":"unauthorized"}`)
		rig.script.apply.push(200, expApplyEcho)
		fetchAssignment(t, rig.client, expTestScopeKey)
		rig.client.ApplyExperimentVariant(expTestScopeKey)
		rig.cycle()
		rig.cycle()
		if rig.owed() != 1 || len(rig.script.apply.requestsSoFar()) != 1 {
			t.Fatalf("%d: the application stays owed and the lane stops sending: owed=%d requests=%d",
				status, rig.owed(), len(rig.script.apply.requestsSoFar()))
		}
		fetchAssignment(t, rig.client, expTestScopeKey)
		rig.cycle()
		flushOrFail(t, rig.client)
		if len(rig.script.apply.requestsSoFar()) != 2 || len(rig.capture.exposures()) != 1 || rig.owed() != 0 {
			t.Fatalf("%d: an authorized fetch resumes the hop and the application is delivered: requests=%d facts=%d owed=%d",
				status, len(rig.script.apply.requestsSoFar()), len(rig.capture.exposures()), rig.owed())
		}
	}
}

func TestTheRealSubjectsSentinelDropsEveryOwedApplication(t *testing.T) {
	rig := newExpHopRig(t, nil, expAssignedServedBody(expServedTrio), expServedBodyForKey(expSecondKey))
	rig.script.apply.push(403, `{"error":"`+expSentinelRealSubjectsDisabled+`"}`)
	fetchAssignment(t, rig.client, expTestScopeKey)
	fetchAssignment(t, rig.client, expSecondKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.client.ApplyExperimentVariant(expSecondKey)
	rig.cycle()
	if rig.owed() != 0 || rig.drops("real_subjects_disabled") != 2 {
		t.Fatalf("the sentinel drops every owed application and counts each: owed=%d drops=%d", rig.owed(), rig.drops("real_subjects_disabled"))
	}
}

func TestATransientRefusalKeepsTheApplicationAndHonoursRetryAfter(t *testing.T) {
	rig := newExpHopRig(t, nil)
	clock := &expFakeClock{now: time.Now()}
	rig.client.clock = clock
	rig.script.apply.pushRetryAfter(503, ``, "30")
	rig.script.apply.push(200, expApplyEcho)
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.cycle()
	rig.cycle()
	if rig.owed() != 1 || len(rig.script.apply.requestsSoFar()) != 1 {
		t.Fatalf("inside the Retry-After window the application stays owed and unsent: owed=%d requests=%d",
			rig.owed(), len(rig.script.apply.requestsSoFar()))
	}
	clock.advance(31 * time.Second)
	rig.cycle()
	flushOrFail(t, rig.client)
	if len(rig.capture.exposures()) != 1 || rig.owed() != 0 {
		t.Fatalf("after the window the application is sealed and delivered: facts=%d owed=%d", len(rig.capture.exposures()), rig.owed())
	}
	for _, status := range []int{429, 500, 302} {
		rig := newExpHopRig(t, nil)
		rig.script.apply.push(status, ``)
		fetchAssignment(t, rig.client, expTestScopeKey)
		rig.client.ApplyExperimentVariant(expTestScopeKey)
		rig.cycle()
		if rig.owed() != 1 || len(rig.client.Snapshot().ExperimentExposureDrops) != 0 {
			t.Fatalf("%d: the application is kept, nothing dropped: owed=%d drops=%v", status, rig.owed(), rig.client.Snapshot().ExperimentExposureDrops)
		}
	}
}

func TestASealedFactForAnotherScopeIsDropped(t *testing.T) {
	rig := newExpHopRig(t, nil)
	rig.script.apply.workspace = "other-workspace"
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.cycle()
	flushOrFail(t, rig.client)
	if rig.owed() != 0 || rig.drops("foreign_scope") != 1 || len(rig.capture.exposures()) != 0 {
		t.Fatalf("a fact sealed for another workspace is dropped and counted foreign_scope: owed=%d drops=%d facts=%d",
			rig.owed(), rig.drops("foreign_scope"), len(rig.capture.exposures()))
	}
}

func TestTheOwedBoundDropsTheOldestApplicationWithACounter(t *testing.T) {
	rig := newExpHopRig(t, nil)
	rig.script.apply.push(503, ``)
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	for i := 0; i < expMaxOwedExposures+1; i++ {
		if err := rig.client.TrackExperimentExposure(expTestScopeKey); err != nil {
			t.Fatalf("extra exposure %d: %v", i, err)
		}
	}
	if rig.owed() != expMaxOwedExposures || rig.drops("owed_bound_exceeded") != 2 {
		t.Fatalf("the bounded FIFO keeps %d and counts each drop: owed=%d drops=%d", expMaxOwedExposures, rig.owed(), rig.drops("owed_bound_exceeded"))
	}
}

// At Close, an application still unsealed gets one bounded attempt; what is
// sealed is delivered and what cannot be sealed is counted.
func TestCloseDeliversWhatItCanSealAndCountsTheRest(t *testing.T) {
	rig := newExpHopRig(t, nil, expAssignedServedBody(expServedTrio), expServedBodyForKey(expSecondKey))
	rig.script.apply.push(200, expApplyEcho)
	rig.script.apply.push(503, ``)
	fetchAssignment(t, rig.client, expTestScopeKey)
	fetchAssignment(t, rig.client, expSecondKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.client.ApplyExperimentVariant(expSecondKey)
	if err := rig.client.Close(context.Background()); err != nil && !strings.Contains(err.Error(), "discard") {
		t.Fatalf("close: %v", err)
	}
	facts := rig.capture.exposures()
	if len(facts) != 1 || facts[0]["props"].(map[string]any)["experiment_key"] != expTestScopeKey {
		t.Fatalf("the application sealed at close must be delivered, and only it: %d fact(s)", len(facts))
	}
	if rig.drops("unsealed_at_close") != 1 {
		t.Fatalf("the application that could not be sealed at close is counted unsealed_at_close, got %d", rig.drops("unsealed_at_close"))
	}
}

// An application whose assignment is then dropped is still sealed online —
// the endpoint judges it as of the state it was served from — and delivered.
func TestAnApplicationOfADroppedAssignmentIsStillSealed(t *testing.T) {
	rig := newExpHopRig(t, nil, expAssignedServedBody(expServedTrio), `{"assigned":false}`)
	rig.script.apply.push(503, ``)
	rig.script.apply.push(200, expApplyEcho)
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.cycle()
	if result := fetchAssignment(t, rig.client, expTestScopeKey); result.Assigned {
		t.Fatal("control: the second fetch must drop the assignment")
	}
	rig.cycle()
	flushOrFail(t, rig.client)
	if len(rig.capture.exposures()) != 1 || rig.owed() != 0 || len(rig.client.Snapshot().ExperimentExposureDrops) != 0 {
		t.Fatalf("the earlier application is sealed and delivered after the drop: facts=%d owed=%d drops=%v",
			len(rig.capture.exposures()), rig.owed(), rig.client.Snapshot().ExperimentExposureDrops)
	}
}

// An assignment without the served pin — or with no identity to post the
// fact under — is served and not recordable; a fetch that re-serves it with
// the pin makes the next application record.
func TestAnAssignmentThatCannotBeRecordedIsServedAndCounted(t *testing.T) {
	rig := newExpHopRig(t, nil, expAssignedBodyUnserved("3"), expAssignedServedBody(expServedTrio))
	fetchAssignment(t, rig.client, expTestScopeKey)
	if variant, _ := rig.client.ApplyExperimentVariant(expTestScopeKey); variant != "treatment" {
		t.Fatalf("an unpinned assignment still serves, got %q", variant)
	}
	rig.cycle()
	if rig.drops("not_recordable") != 1 || len(rig.script.apply.requestsSoFar()) != 0 || rig.owed() != 0 {
		t.Fatalf("an unpinned assignment is counted not_recordable and nothing is sent: drops=%d requests=%d owed=%d",
			rig.drops("not_recordable"), len(rig.script.apply.requestsSoFar()), rig.owed())
	}
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.cycle()
	flushOrFail(t, rig.client)
	if len(rig.capture.exposures()) != 1 {
		t.Fatalf("re-served with the pin, the next application records, got %d fact(s)", len(rig.capture.exposures()))
	}

	anonymous := newExpHopRig(t, func(cfg *Config) { cfg.AnonymousID = "" })
	fetchAssignment(t, anonymous.client, expTestScopeKey)
	anonymous.client.ApplyExperimentVariant(expTestScopeKey)
	anonymous.cycle()
	if anonymous.drops("not_recordable") != 1 || len(anonymous.script.apply.requestsSoFar()) != 0 {
		t.Fatalf("with no identity to post under, the application is not recordable: drops=%d requests=%d",
			anonymous.drops("not_recordable"), len(anonymous.script.apply.requestsSoFar()))
	}
}

// A consent purge that lands while the hop is in flight discards the record
// the hop was sealing and re-arms its application; the answer is dropped and
// the re-armed application is delivered exactly once.
func TestAPurgeDuringTheHopDeliversTheApplicationOnce(t *testing.T) {
	rig := newExpHopRig(t, nil)
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	raced := false
	rig.script.apply.mu.Lock()
	rig.script.apply.onRequest = func() {
		if !raced {
			raced = true
			rig.client.SetConsent(false)
			rig.client.SetConsent(true)
		}
	}
	rig.script.apply.mu.Unlock()
	rig.cycle()
	if !raced {
		t.Fatal("control: the purge must land during the hop")
	}
	rig.cycle()
	flushOrFail(t, rig.client)
	requests := rig.script.apply.requestsSoFar()
	if len(requests) != 2 || requests[0].body["exposure_id"] != requests[1].body["exposure_id"] {
		t.Fatalf("the re-armed application must be re-sent with its identity, got %d request(s)", len(requests))
	}
	if got := len(rig.capture.exposures()); got != 1 {
		t.Fatalf("the application must be delivered exactly once, got %d", got)
	}
}

// After a real-subjects sentinel and a re-enable, an application made first
// by TrackExperimentExposure is the session's own: a denial that drains it
// before delivery re-arms it, and the re-grant delivers it.
func TestAFirstTrackedApplicationAfterASentinelSurvivesADenial(t *testing.T) {
	rig := newExpHopRig(t, nil, expAssignedServedBody(expServedTrio), `{"error":"`+expSentinelRealSubjectsDisabled+`"}`, expAssignedServedBody(expServedTrio))
	rig.script.responses[1].status = 403
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.cycle()
	flushOrFail(t, rig.client)
	if _, err := rig.client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil {
		t.Fatal("control: the sentinel fetch must fail closed")
	}
	fetchAssignment(t, rig.client, expTestScopeKey)
	if err := rig.client.TrackExperimentExposure(expTestScopeKey); err != nil {
		t.Fatalf("TrackExperimentExposure after the re-enable: %v", err)
	}
	rig.client.SetConsent(false)
	rig.client.SetConsent(true)
	rig.cycle()
	flushOrFail(t, rig.client)
	requests := rig.script.apply.requestsSoFar()
	if len(requests) != 2 || requests[0].body["exposure_id"] == requests[1].body["exposure_id"] {
		t.Fatalf("the post-sentinel application is a new one, sent once after the re-grant: %d request(s)", len(requests))
	}
	tracked, _ := requests[1].body["exposure_id"].(string)
	seen := 0
	for _, fact := range rig.capture.exposures() {
		if fact["event_id"] == expStubFactID(tracked) {
			seen++
		}
	}
	if seen != 1 {
		t.Fatalf("the tracked application's only exposure was lost to the denial (delivered %d)", seen)
	}
}

// ── every owed application the plane gives up on is counted ─────────────────

// A consent withdrawal re-arms only this session's applications of live
// assignments. The owed applications it does NOT re-arm — an extra one, or
// one whose assignment has since been dropped — are counted
// consent_withdrawn, and the re-armed one is not.
func TestAConsentPurgeCountsTheApplicationsItDiscards(t *testing.T) {
	t.Run("an extra application", func(t *testing.T) {
		rig := newExpHopRig(t, nil)
		fetchAssignment(t, rig.client, expTestScopeKey)
		rig.client.ApplyExperimentVariant(expTestScopeKey)
		if err := rig.client.TrackExperimentExposure(expTestScopeKey); err != nil {
			t.Fatalf("TrackExperimentExposure: %v", err)
		}
		if rig.owed() != 2 {
			t.Fatalf("control: two owed applications before the purge, got %d", rig.owed())
		}
		rig.client.SetConsent(false)
		rig.client.SetConsent(true)
		if rig.owed() != 1 || rig.drops("consent_withdrawn") != 1 {
			t.Fatalf("the purge re-arms the applied tuple and counts the discarded extra once: owed=%d drops=%v",
				rig.owed(), rig.client.Snapshot().ExperimentExposureDrops)
		}
		rig.cycle()
		flushOrFail(t, rig.client)
		if got := len(rig.capture.exposures()); got != 1 {
			t.Fatalf("the re-armed application is still delivered, got %d fact(s)", got)
		}
	})
	t.Run("an application of a dropped assignment", func(t *testing.T) {
		rig := newExpHopRig(t, nil, expAssignedServedBody(expServedTrio), `{"assigned":false}`)
		rig.script.apply.push(503, ``)
		fetchAssignment(t, rig.client, expTestScopeKey)
		rig.client.ApplyExperimentVariant(expTestScopeKey)
		rig.cycle()
		if result := fetchAssignment(t, rig.client, expTestScopeKey); result.Assigned {
			t.Fatal("control: the second fetch must drop the assignment")
		}
		if rig.owed() != 1 {
			t.Fatalf("control: the transient refusal keeps the application owed, got %d", rig.owed())
		}
		rig.client.SetConsent(false)
		rig.client.SetConsent(true)
		if rig.owed() != 0 || rig.drops("consent_withdrawn") != 1 {
			t.Fatalf("the purge does not re-arm a dropped assignment and counts its application: owed=%d drops=%v",
				rig.owed(), rig.client.Snapshot().ExperimentExposureDrops)
		}
	})
}

// A sealed application Close could not hand to the queue is counted
// undelivered_at_close beside the unsealed one's unsealed_at_close: the drop
// map accounts for every application the close remnant loses, the same
// total Stats.Dropped carries.
func TestCloseCountsASealedApplicationItCouldNotDeliver(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, strings.Replace(expAssignedBody("1"), `"version":1`, `"version":2`, 1))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.BatchSize = 1
		cfg.BufferSize = 1
	})
	parkWorkerWithFullQueue(t, client, capture)
	fetchAndApply(t, client, expTestScopeKey)
	fetchAndApply(t, client, expTestScopeKey)
	client.exp.mu.Lock()
	sealed, unsealed := 0, 0
	for _, owed := range client.exp.pendingExposure[expTestScopeKey] {
		if owed.sealed != nil {
			sealed++
		} else {
			unsealed++
		}
	}
	client.exp.mu.Unlock()
	if sealed == 0 {
		t.Fatalf("control: at least one owed application must already be sealed (sealed=%d unsealed=%d)", sealed, unsealed)
	}
	before := client.Snapshot().Dropped
	closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = client.Close(closeCtx)
	snapshot := client.Snapshot()
	if snapshot.ExperimentExposureDrops["undelivered_at_close"] != uint64(sealed) ||
		snapshot.ExperimentExposureDrops["unsealed_at_close"] != uint64(unsealed) {
		t.Fatalf("each application lost at close is counted by why: sealed=%d unsealed=%d drops=%v",
			sealed, unsealed, snapshot.ExperimentExposureDrops)
	}
	var counted uint64
	for _, n := range snapshot.ExperimentExposureDrops {
		counted += n
	}
	if lost := snapshot.Dropped - before; counted != lost {
		t.Fatalf("the drop map must account for every application Stats.Dropped counts at close: map=%d dropped=%d", counted, lost)
	}
}

// ── round 1 ─────────────────────────────────────────────────────────────────

// Once the tuple's application is delivered, a repeat ApplyExperimentVariant
// records nothing: the exposure is recorded once per (experiment, version,
// subject, session). A consent purge then re-sends the DELIVERED
// application's exposure_id — never a second id the repeat call minted, which
// the platform would record as a second treatment.
func TestARepeatApplicationAfterDeliveryRecordsNothing(t *testing.T) {
	rig := newExpHopRig(t, nil)
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.cycle()
	flushOrFail(t, rig.client)
	first, _ := rig.script.apply.requestsSoFar()[0].body["exposure_id"].(string)
	if first == "" {
		t.Fatal("control: the first application must have been sent")
	}
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	if rig.owed() != 0 {
		t.Fatalf("a repeat application of a delivered tuple must not be owed, got %d", rig.owed())
	}
	// With an extra application at the tail, the repeat call still records
	// nothing, and the purge re-arms the delivered application.
	if err := rig.client.TrackExperimentExposure(expTestScopeKey); err != nil {
		t.Fatalf("TrackExperimentExposure: %v", err)
	}
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	if rig.owed() != 1 {
		t.Fatalf("only the extra application is owed, got %d", rig.owed())
	}
	rig.client.SetConsent(false)
	rig.client.SetConsent(true)
	rig.cycle()
	flushOrFail(t, rig.client)
	requests := rig.script.apply.requestsSoFar()
	if len(requests) != 2 {
		t.Fatalf("the purge re-sends the delivered application only, got %d request(s)", len(requests))
	}
	if again, _ := requests[1].body["exposure_id"].(string); again != first {
		t.Fatalf("the re-armed application must carry the delivered exposure_id %q, got %q", first, again)
	}
}

// The apply request is bounded by Config.HTTPTimeout even when the
// integrator's HTTPClient has no Timeout: a silent endpoint keeps the
// application owed instead of holding the lane.
func TestAnApplyRequestIsBoundedByHTTPTimeout(t *testing.T) {
	rig := newExpHopRig(t, func(cfg *Config) {
		cfg.HTTPClient = &http.Client{}
		cfg.HTTPTimeout = 200 * time.Millisecond
	})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	rig.script.apply.onRequest = func() { <-release }
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	done := make(chan struct{})
	go func() {
		rig.cycle()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		once.Do(func() { close(release) })
		<-done
		t.Fatal("the lane cycle waited past HTTPTimeout on a silent apply endpoint")
	}
	if rig.owed() != 1 {
		t.Fatalf("the timed-out application stays owed, got %d", rig.owed())
	}
}

// Close is not held by the lane's in-flight apply request: it cancels the
// lane's work first, so its own context bounds it.
func TestCloseDoesNotWaitOutTheLanesInFlightApply(t *testing.T) {
	rig := newExpHopRig(t, func(cfg *Config) { cfg.HTTPTimeout = 10 * time.Second })
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	rig.script.apply.onRequest = func() { <-release }
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.client.exp.mu.Lock()
	rig.client.exp.laneParkedForTests = false
	rig.client.exp.mu.Unlock()
	waitFor(t, 5*time.Second, "the lane's apply request is in flight", func() bool {
		return len(rig.script.apply.requestsSoFar()) >= 1
	})
	closeCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	closed := make(chan struct{})
	go func() {
		_ = rig.client.Close(closeCtx)
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		once.Do(func() { close(release) })
		<-closed
		t.Fatalf("Close waited %s on the lane's in-flight apply request", time.Since(started).Round(time.Millisecond))
	}
	if n := rig.client.Snapshot().ExperimentExposureDrops["unsealed_at_close"]; n != 1 {
		t.Fatalf("the application Close could not seal is counted unsealed_at_close, got %d", n)
	}
}
