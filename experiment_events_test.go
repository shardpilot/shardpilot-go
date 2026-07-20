package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// envelopeCapture records the full decoded wire envelopes of every batch
// publish, so producer tests can pin the exact experiment fact shape.
type envelopeCapture struct {
	mu        sync.Mutex
	envelopes []map[string]any
}

func (c *envelopeCapture) all() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.envelopes...)
}

func newIngestCaptureServer(t *testing.T) (*envelopeCapture, *httptest.Server) {
	t.Helper()
	capture := &envelopeCapture{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/events:batch":
			var request struct {
				Events []map[string]any `json:"events"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode batch request: %v", err)
			}
			capture.mu.Lock()
			capture.envelopes = append(capture.envelopes, request.Events...)
			count := len(request.Events)
			capture.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprintf(w, `{"accepted":%d,"rejected":0,"duplicates":0}`, count)
		case "/v1/consent":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"recorded":true,"replayed":false}`)
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	return capture, server
}

// clientIDAssignment is an assigned client_id-unit verdict fixture.
func clientIDAssignment() ExperimentAssignment {
	return ExperimentAssignment{
		AppKey:         "app-test",
		EnvironmentKey: "develop",
		ExperimentKey:  "exp-checkout",
		Version:        3,
		Assigned:       true,
		AssignmentKey:  "asgn_0123456789abcdef0123456789abcdef",
		VariantKey:     "variant_b",
		SubjectFactKey: testSubjectFactKey,
		Boundary:       ExperimentAssignmentBoundary{AssignmentUnit: "client_id"},
	}
}

// syntheticAssignment is an assigned synthetic_subject_key-unit verdict.
func syntheticAssignment() ExperimentAssignment {
	return ExperimentAssignment{
		AppKey:         "app-test",
		EnvironmentKey: "develop",
		ExperimentKey:  "exp-onboarding",
		Version:        7,
		Assigned:       true,
		AssignmentKey:  "asgn_feedfacefeedfacefeedfacefeedface",
		VariantKey:     "control",
		Boundary:       ExperimentAssignmentBoundary{AssignmentUnit: "synthetic_subject_key"},
	}
}

func TestExperimentProducersRequireOptIn(t *testing.T) {
	_, server := newIngestCaptureServer(t)
	defer server.Close()
	client := newExperimentsClient(t, server.URL, "", func(cfg *Config) {
		cfg.ExperimentsURL = ""
		cfg.ExperimentSubjectKey = ""
	})
	defer client.Close(context.Background())

	assignment := clientIDAssignment()
	if err := client.TrackExperimentExposure(context.Background(), assignment); !errors.Is(err, ErrExperimentsNotConfigured) {
		t.Fatalf("expected ErrExperimentsNotConfigured, got %v", err)
	}
	if err := client.EnqueueExperimentExposure(assignment); !errors.Is(err, ErrExperimentsNotConfigured) {
		t.Fatalf("expected ErrExperimentsNotConfigured, got %v", err)
	}
	if err := client.TrackExperimentOutcome(context.Background(), assignment, "k", 1); !errors.Is(err, ErrExperimentsNotConfigured) {
		t.Fatalf("expected ErrExperimentsNotConfigured, got %v", err)
	}
	if err := client.EnqueueExperimentOutcome(assignment, "k", 1); !errors.Is(err, ErrExperimentsNotConfigured) {
		t.Fatalf("expected ErrExperimentsNotConfigured, got %v", err)
	}
}

func TestExperimentExposureWireContract(t *testing.T) {
	capture, server := newIngestCaptureServer(t)
	defer server.Close()
	// Source is BACKEND and a default UserID is configured: the experiment
	// fact must still ship source "client" with NO user_id — the ingest
	// contract for these event names — while anonymous_id carries the SDK
	// client identity. (Floor off, consent unknown: this SDK's documented
	// server-side posture keeps the pipeline open — the producers inherit
	// it unchanged.)
	client := newExperimentsClient(t, server.URL, "", func(cfg *Config) {
		cfg.UserID = "user-42"
	})
	defer client.Close(context.Background())

	if err := client.EnqueueExperimentExposure(clientIDAssignment()); err != nil {
		t.Fatalf("EnqueueExperimentExposure: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	envelopes := capture.all()
	if len(envelopes) != 1 {
		t.Fatalf("expected exactly one envelope, got %d", len(envelopes))
	}
	envelope := envelopes[0]
	if envelope["event_name"] != "experiment_exposure" {
		t.Fatalf("unexpected event name %v", envelope["event_name"])
	}
	if envelope["source"] != "client" {
		t.Fatalf("experiment facts must ship source \"client\" (config says backend), got %v", envelope["source"])
	}
	if _, present := envelope["user_id"]; present {
		t.Fatalf("experiment facts must omit user_id even with Config.UserID set, got %v", envelope["user_id"])
	}
	if envelope["anonymous_id"] != "anon-exp-1" {
		t.Fatalf("experiment facts must carry the SDK client identity as anonymous_id, got %v", envelope["anonymous_id"])
	}
	if _, present := envelope["session_id"]; present {
		t.Fatalf("expected no session_id on the fact, got %v", envelope["session_id"])
	}
	props, ok := envelope["props"].(map[string]any)
	if !ok {
		t.Fatalf("expected props, got %v", envelope["props"])
	}
	want := map[string]any{
		"experiment_key":     "exp-checkout",
		"experiment_version": float64(3),
		"assignment_key":     testSubjectFactKey, // the sfk1_ subject — NEVER the raw spcid
		"variant_key":        "variant_b",
		"assignment_unit":    "client_id",
	}
	if len(props) != len(want) {
		t.Fatalf("props must be exactly the allowlist, got %v", props)
	}
	for key, value := range want {
		if props[key] != value {
			t.Fatalf("expected props[%s]=%v, got %v", key, value, props[key])
		}
	}
	if strings.Contains(fmt.Sprint(props), testExperimentSubjectKey) {
		t.Fatal("the raw spcid subject key must never ride experiment props")
	}
}

func TestExperimentOutcomeWireContract(t *testing.T) {
	capture, server := newIngestCaptureServer(t)
	defer server.Close()
	client := newExperimentsClient(t, server.URL, "", nil)
	defer client.Close(context.Background())

	if err := client.TrackExperimentOutcome(context.Background(), syntheticAssignment(), "checkout_completed", true); err != nil {
		t.Fatalf("TrackExperimentOutcome: %v", err)
	}
	envelopes := capture.all()
	if len(envelopes) != 1 {
		t.Fatalf("expected exactly one envelope, got %d", len(envelopes))
	}
	envelope := envelopes[0]
	if envelope["event_name"] != "experiment_outcome" {
		t.Fatalf("unexpected event name %v", envelope["event_name"])
	}
	props, ok := envelope["props"].(map[string]any)
	if !ok {
		t.Fatalf("expected props, got %v", envelope["props"])
	}
	want := map[string]any{
		"experiment_key":     "exp-onboarding",
		"experiment_version": float64(7),
		"assignment_key":     "asgn_feedfacefeedfacefeedfacefeedface", // synthetic unit: the assignment key itself
		"variant_key":        "control",
		"assignment_unit":    "synthetic_subject_key",
		"outcome_key":        "checkout_completed",
		"outcome_value":      true,
	}
	if len(props) != len(want) {
		t.Fatalf("props must be exactly the allowlist, got %v", props)
	}
	for key, value := range want {
		if props[key] != value {
			t.Fatalf("expected props[%s]=%v, got %v", key, value, props[key])
		}
	}

	// Outcomes are NOT deduplicated: a second admitted call emits again.
	if err := client.TrackExperimentOutcome(context.Background(), syntheticAssignment(), "checkout_completed", 12.5); err != nil {
		t.Fatalf("second TrackExperimentOutcome: %v", err)
	}
	if got := len(capture.all()); got != 2 {
		t.Fatalf("expected outcomes unduplicated, got %d envelopes", got)
	}
}

func TestExperimentExposureDedupePerAssignmentKey(t *testing.T) {
	capture, server := newIngestCaptureServer(t)
	defer server.Close()
	client := newExperimentsClient(t, server.URL, "", nil)
	defer client.Close(context.Background())

	assignment := clientIDAssignment()
	if err := client.EnqueueExperimentExposure(assignment); err != nil {
		t.Fatalf("first exposure: %v", err)
	}
	// The duplicate is a designed no-op: nil, nothing queued.
	if err := client.EnqueueExperimentExposure(assignment); err != nil {
		t.Fatalf("duplicate exposure must be a nil no-op, got %v", err)
	}
	if err := client.TrackExperimentExposure(context.Background(), assignment); err != nil {
		t.Fatalf("duplicate Track exposure must be a nil no-op, got %v", err)
	}

	// The dedupe identity is the FULL assignment identity: a different
	// assignment key, a different VERSION of the same experiment, or a
	// different EXPERIMENT sharing an assignment key each get their own
	// exposure.
	otherKey := clientIDAssignment()
	otherKey.AssignmentKey = "asgn_feedfacefeedfacefeedfacefeedface"
	otherVersion := clientIDAssignment()
	otherVersion.Version = 4
	otherExperiment := clientIDAssignment()
	otherExperiment.ExperimentKey = "exp-onboarding"
	for _, variant := range []ExperimentAssignment{otherKey, otherVersion, otherExperiment} {
		if err := client.EnqueueExperimentExposure(variant); err != nil {
			t.Fatalf("distinct-identity exposure: %v", err)
		}
	}
	// And each of those identities dedupes on its own repeat.
	if err := client.EnqueueExperimentExposure(otherVersion); err != nil {
		t.Fatalf("duplicate of the other version must be a nil no-op, got %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := len(capture.all()); got != 4 {
		t.Fatalf("expected exactly one exposure per assignment identity, got %d envelopes", got)
	}
}

func TestExperimentProducersRejectCrossScopeAssignments(t *testing.T) {
	capture, server := newIngestCaptureServer(t)
	defer server.Close()
	client := newExperimentsClient(t, server.URL, "", nil)
	defer client.Close(context.Background())

	// A verdict fetched for another app/environment scope must never build
	// facts under this client's envelope scope.
	otherApp := clientIDAssignment()
	otherApp.AppKey = "app-other"
	otherEnv := clientIDAssignment()
	otherEnv.EnvironmentKey = "production"
	unscoped := clientIDAssignment()
	unscoped.AppKey, unscoped.EnvironmentKey = "", ""
	for _, assignment := range []ExperimentAssignment{otherApp, otherEnv, unscoped} {
		if err := client.TrackExperimentExposure(context.Background(), assignment); !errors.Is(err, ErrExperimentScopeMismatch) {
			t.Fatalf("expected ErrExperimentScopeMismatch from Track exposure, got %v", err)
		}
		if err := client.EnqueueExperimentExposure(assignment); !errors.Is(err, ErrExperimentScopeMismatch) {
			t.Fatalf("expected ErrExperimentScopeMismatch from Enqueue exposure, got %v", err)
		}
		if err := client.TrackExperimentOutcome(context.Background(), assignment, "k", 1); !errors.Is(err, ErrExperimentScopeMismatch) {
			t.Fatalf("expected ErrExperimentScopeMismatch from Track outcome, got %v", err)
		}
		if err := client.EnqueueExperimentOutcome(assignment, "k", 1); !errors.Is(err, ErrExperimentScopeMismatch) {
			t.Fatalf("expected ErrExperimentScopeMismatch from Enqueue outcome, got %v", err)
		}
		// Distinct from the generic fact-contract refusal.
		if err := client.EnqueueExperimentExposure(assignment); errors.Is(err, ErrInvalidExperimentFact) {
			t.Fatalf("scope mismatch must not read as ErrInvalidExperimentFact, got %v", err)
		}
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := len(capture.all()); got != 0 {
		t.Fatalf("cross-scope refusals must queue nothing, got %d envelopes", got)
	}

	// The refusals claimed nothing: the correctly scoped exposure for the
	// same assignment key still emits.
	if err := client.EnqueueExperimentExposure(clientIDAssignment()); err != nil {
		t.Fatalf("in-scope exposure after refusals: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := len(capture.all()); got != 1 {
		t.Fatalf("expected the in-scope exposure delivered, got %d envelopes", got)
	}
}

func TestExperimentFactSpoolRetentionUnderFloorWithUserID(t *testing.T) {
	// The floor's spool actor-eligibility must reach the SAME verdict
	// intake did for the SDK's own experiment facts: their envelopes omit
	// user_id BY WIRE CONTRACT while carrying the configured client
	// identity as anonymous_id — under a configured UserID that is not an
	// actor override, and a retryably failed batch must RETAIN the fact on
	// disk, never dead-letter it as a consent drop.
	state, server := newFloorTestServer(t)
	defer server.Close()

	var deadMu sync.Mutex
	var dead []SpoolDeadLetter
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		cfg.UserID = "user-42"
		cfg.APIKey = "test-exp-key"
		cfg.ExperimentsURL = server.URL + "/api/cp/v1"
		cfg.ExperimentSubjectKey = testExperimentSubjectKey
		cfg.OnSpoolDeadLetter = func(letter SpoolDeadLetter) {
			deadMu.Lock()
			dead = append(dead, letter)
			deadMu.Unlock()
		}
	})
	defer client.Close(context.Background())

	client.SetConsent(true)
	if err := client.EnqueueExperimentExposure(clientIDAssignment()); err != nil {
		t.Fatalf("EnqueueExperimentExposure under a grant: %v", err)
	}

	// A retryable batch failure spools the fact under the persisted grant.
	state.setBatchOutcome(http.StatusInternalServerError)
	if err := client.Flush(context.Background()); err == nil {
		t.Fatal("expected the failed flush to report the retryable error")
	}
	deadMu.Lock()
	for _, letter := range dead {
		if letter.Reason == SpoolDropConsent {
			deadMu.Unlock()
			t.Fatalf("the SDK's own experiment fact must not dead-letter as a consent drop: %+v", letter)
		}
	}
	deadMu.Unlock()
	if stats := client.Snapshot(); stats.Spooled != 1 {
		t.Fatalf("expected the fact retained in the spool, got %+v", stats)
	}
	firstBatch := state.batchIDsSince(0)
	if len(firstBatch) != 1 {
		t.Fatalf("expected the one failed batch attempt, got %v", firstBatch)
	}

	// And the retained fact delivers on the retry, byte-identical id.
	state.setBatchOutcome(0)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("recovery flush: %v", err)
	}
	all := state.batchIDsSince(0)
	if len(all) < 2 || all[len(all)-1] != firstBatch[0] {
		t.Fatalf("expected the retained fact re-delivered under its original id %q, got %v", firstBatch[0], all)
	}
}

func TestExperimentExposureConcurrentDuplicateWaitsForConversion(t *testing.T) {
	// Two goroutines race the same assignment key: the duplicate must not
	// report "emitted" off a pending attempt — if the first attempt fails
	// and re-arms, the waiting duplicate takes over and performs its own
	// emission, so exactly one exposure lands.
	capture := &envelopeCapture{}
	var batchCalls atomic.Int64
	firstArrived := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events:batch" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"recorded":true}`)
			return
		}
		if batchCalls.Add(1) == 1 {
			firstArrived <- struct{}{}
			<-releaseFirst
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `{"error":{"code":"internal_error","message":"boom"}}`)
			return
		}
		var request struct {
			Events []map[string]any `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode batch request: %v", err)
		}
		capture.mu.Lock()
		capture.envelopes = append(capture.envelopes, request.Events...)
		count := len(request.Events)
		capture.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"accepted":%d,"rejected":0,"duplicates":0}`, count)
	}))
	defer server.Close()

	client := newExperimentsClient(t, server.URL, "", func(cfg *Config) {
		cfg.HTTPTimeout = 5 * time.Second
	})
	defer client.Close(context.Background())

	first := make(chan error, 1)
	go func() {
		first <- client.TrackExperimentExposure(context.Background(), clientIDAssignment())
	}()
	<-firstArrived // the first attempt owns the reservation and is on the wire

	second := make(chan error, 1)
	go func() {
		second <- client.TrackExperimentExposure(context.Background(), clientIDAssignment())
	}()

	// Release the first attempt into its 500: it fails and re-arms; the
	// waiting duplicate takes over and emits for real.
	close(releaseFirst)
	if err := <-first; err == nil {
		t.Fatal("expected the first attempt's retryable failure surfaced")
	}
	if err := <-second; err != nil {
		t.Fatalf("expected the waiting duplicate to take over and emit, got %v", err)
	}
	if got := len(capture.all()); got != 1 {
		t.Fatalf("expected exactly one exposure on the wire, got %d", got)
	}
	if got := batchCalls.Load(); got != 2 {
		t.Fatalf("expected exactly two batch attempts (failed + converted), got %d", got)
	}

	// And after conversion, further duplicates are immediate no-ops.
	if err := client.TrackExperimentExposure(context.Background(), clientIDAssignment()); err != nil {
		t.Fatalf("post-conversion duplicate must be a nil no-op, got %v", err)
	}
	if got := len(capture.all()); got != 1 {
		t.Fatalf("post-conversion duplicate must emit nothing, got %d envelopes", got)
	}
}

func TestExperimentProducersFloorConsentGating(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		cfg.APIKey = "test-exp-key"
		cfg.ExperimentsURL = server.URL + "/api/cp/v1"
		cfg.ExperimentSubjectKey = testExperimentSubjectKey
	})
	defer client.Close(context.Background())

	assignment := clientIDAssignment()

	// UNDECIDED under the floor: every producer path refuses — consent
	// unknown means DROP, nothing queued, nothing spooled, nothing on the
	// wire.
	if err := client.TrackExperimentExposure(context.Background(), assignment); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("expected ErrConsentUnknown from Track exposure, got %v", err)
	}
	if err := client.EnqueueExperimentExposure(assignment); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("expected ErrConsentUnknown from Enqueue exposure, got %v", err)
	}
	if err := client.EnqueueExperimentOutcome(assignment, "k", 1); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("expected ErrConsentUnknown from Enqueue outcome, got %v", err)
	}

	// DENIED: same drop with the distinct denial code.
	client.SetConsent(false)
	if err := client.EnqueueExperimentExposure(assignment); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected ErrConsentDenied, got %v", err)
	}
	if err := client.TrackExperimentOutcome(context.Background(), assignment, "k", 1); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected ErrConsentDenied, got %v", err)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("refused facts must never reach the wire, saw %d batches", got)
	}
	if stats := client.Snapshot(); stats.Dropped != 5 {
		t.Fatalf("expected all five refused facts counted dropped, got %+v", stats)
	}

	// GRANTED: the refused exposure re-arms (its dedupe slot was released)
	// and the fact is queued and delivered.
	client.SetConsent(true)
	if err := client.EnqueueExperimentExposure(assignment); err != nil {
		t.Fatalf("expected the exposure admitted under a grant, got %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := state.batchCount(); got != 1 {
		t.Fatalf("expected the granted exposure delivered, got %d batches", got)
	}
}

func TestExperimentProducerValidation(t *testing.T) {
	_, server := newIngestCaptureServer(t)
	defer server.Close()
	client := newExperimentsClient(t, server.URL, "", func(cfg *Config) {
		// Headroom for the admitted-values sweep below: this test asserts
		// validation verdicts, not queue backpressure.
		cfg.BufferSize = 64
	})
	defer client.Close(context.Background())

	notAssigned := clientIDAssignment()
	notAssigned.Assigned = false
	if err := client.EnqueueExperimentExposure(notAssigned); !errors.Is(err, ErrExperimentNotAssigned) {
		t.Fatalf("expected ErrExperimentNotAssigned, got %v", err)
	}
	if err := client.EnqueueExperimentExposure(ExperimentAssignment{}); !errors.Is(err, ErrExperimentNotAssigned) {
		t.Fatalf("expected ErrExperimentNotAssigned for the zero verdict, got %v", err)
	}

	invalid := []struct {
		name   string
		mutate func(*ExperimentAssignment)
	}{
		{"missing variant key", func(a *ExperimentAssignment) { a.VariantKey = "" }},
		{"missing assignment key", func(a *ExperimentAssignment) { a.AssignmentKey = "" }},
		{"missing experiment key", func(a *ExperimentAssignment) { a.ExperimentKey = "" }},
		{"client_id unit without sfk", func(a *ExperimentAssignment) { a.SubjectFactKey = "" }},
		{"client_id unit with a raw spcid as sfk", func(a *ExperimentAssignment) { a.SubjectFactKey = testExperimentSubjectKey }},
		{"client_id unit with malformed sfk", func(a *ExperimentAssignment) { a.SubjectFactKey = "sfk1_SHOUTING" }},
		{"unknown assignment unit", func(a *ExperimentAssignment) { a.Boundary.AssignmentUnit = "device_id" }},
		{"empty assignment unit", func(a *ExperimentAssignment) { a.Boundary.AssignmentUnit = "" }},
	}
	for _, tc := range invalid {
		assignment := clientIDAssignment()
		tc.mutate(&assignment)
		if err := client.EnqueueExperimentExposure(assignment); !errors.Is(err, ErrInvalidExperimentFact) {
			t.Fatalf("%s: expected ErrInvalidExperimentFact, got %v", tc.name, err)
		}
	}

	// Outcome-specific validation: outcome_key required; outcome_value must
	// be a finite number or a boolean.
	valid := clientIDAssignment()
	if err := client.EnqueueExperimentOutcome(valid, "  ", 1); !errors.Is(err, ErrInvalidExperimentFact) {
		t.Fatalf("expected ErrInvalidExperimentFact for an empty outcome key, got %v", err)
	}
	for _, value := range []any{math.NaN(), math.Inf(1), math.Inf(-1), "twelve", nil, []int{1}, map[string]any{}} {
		if err := client.EnqueueExperimentOutcome(valid, "k", value); !errors.Is(err, ErrInvalidExperimentFact) {
			t.Fatalf("expected ErrInvalidExperimentFact for outcome value %v, got %v", value, err)
		}
	}
	for _, value := range []any{true, false, 3, int64(-7), uint32(9), 2.5, float32(1.5)} {
		if err := client.EnqueueExperimentOutcome(valid, "k", value); err != nil {
			t.Fatalf("expected outcome value %v admitted, got %v", value, err)
		}
	}

	// Missing Config.AnonymousID: the fact cannot satisfy the erasure
	// contract and is refused whole.
	noAnon := newExperimentsClient(t, server.URL, "", func(cfg *Config) {
		cfg.AnonymousID = ""
	})
	defer noAnon.Close(context.Background())
	if err := noAnon.EnqueueExperimentExposure(clientIDAssignment()); !errors.Is(err, ErrInvalidExperimentFact) {
		t.Fatalf("expected ErrInvalidExperimentFact without Config.AnonymousID, got %v", err)
	}

	// A refused exposure released its dedupe slot: the valid retry emits.
	if err := client.EnqueueExperimentExposure(clientIDAssignment()); err != nil {
		t.Fatalf("expected the valid exposure admitted after refusals, got %v", err)
	}
}
