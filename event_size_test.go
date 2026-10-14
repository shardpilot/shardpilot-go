package shardpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// maxEventSceneBytes is the ingest per-event cap these scenes measure against.
const maxEventSceneBytes = 2048

// sizeSceneServer records every event element the batch route receives,
// byte for byte.
type sizeSceneServer struct {
	mu       sync.Mutex
	elements []json.RawMessage
}

func (s *sizeSceneServer) received() []json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]json.RawMessage(nil), s.elements...)
}

func newSizeSceneClient(t *testing.T) (*Client, *sizeSceneServer) {
	t.Helper()
	scene := &sizeSceneServer{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Events []json.RawMessage `json:"events"`
		}
		if err := json.NewDecoder(ingestRequestBody(t, r)).Decode(&body); err != nil {
			t.Errorf("decode batch: %v", err)
		}
		scene.mu.Lock()
		scene.elements = append(scene.elements, body.Events...)
		scene.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":0,"rejected":0,"duplicates":0}`))
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(Config{
		IngestURL:                 server.URL,
		ServiceToken:              "service-fixture",
		WorkspaceKey:              "workspace-test",
		AppKey:                    "app-test",
		EnvironmentKey:            "develop",
		Source:                    SourceBackend,
		AnonymousID:               "synthetic-anon",
		BatchSize:                 10,
		BufferSize:                16,
		FlushInterval:             time.Hour,
		HTTPTimeout:               time.Second,
		DisableRequestCompression: true,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client, scene
}

// sizedEvent returns an event whose wire element is exactly size bytes.
func sizedEvent(t *testing.T, client *Client, id string, size int) Event {
	t.Helper()
	event := Event{
		ID:        id,
		Name:      "size_scene",
		Timestamp: time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC),
		Props:     map[string]any{"pad": ""},
	}
	envelope, err := client.buildEnvelope(event)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if size < len(raw) {
		t.Fatalf("scene shape: the smallest element is %d bytes", len(raw))
	}
	event.Props = map[string]any{"pad": strings.Repeat("a", size-len(raw))}
	return event
}

// TestEventSizePrecheckBoundary: the ingest rule on the exact wire element.
// 2048 bytes is admitted and sent; 2049 is refused at intake, counted as
// dropped, and never sent.
func TestEventSizePrecheckBoundary(t *testing.T) {
	for _, path := range []string{"Track", "Enqueue"} {
		t.Run(path, func(t *testing.T) {
			client, scene := newSizeSceneClient(t)
			admit := func(event Event) error {
				if path == "Track" {
					return client.Track(context.Background(), event)
				}
				return client.Enqueue(event)
			}
			fits := sizedEvent(t, client, "0199a0b0-0000-7000-8000-000000000001", maxEventSceneBytes)
			if err := admit(fits); err != nil {
				t.Fatalf("a %d-byte event was refused: %v", maxEventSceneBytes, err)
			}
			over := sizedEvent(t, client, "0199a0b0-0000-7000-8000-000000000002", maxEventSceneBytes+1)
			if err := admit(over); !errors.Is(err, ErrEventTooLarge) {
				t.Fatalf("a %d-byte event: err = %v, want ErrEventTooLarge", maxEventSceneBytes+1, err)
			}
			if err := client.Flush(context.Background()); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			got := scene.received()
			if len(got) != 1 || len(bytes.TrimSpace(got[0])) != maxEventSceneBytes {
				t.Fatalf("sent %d elements (want the one %d-byte element)", len(got), maxEventSceneBytes)
			}
			if dropped := client.Snapshot().Dropped; dropped != 1 {
				t.Fatalf("Dropped = %d, want 1", dropped)
			}
		})
	}
}

// TestEventSizePrecheckSendsTheMeasuredBytes: the bytes measured at intake
// are the bytes sent. A nested value the caller changes after Enqueue does
// not reach the wire, so it cannot carry the element past the limit.
func TestEventSizePrecheckSendsTheMeasuredBytes(t *testing.T) {
	client, scene := newSizeSceneClient(t)
	nested := map[string]any{"note": "short"}
	event := Event{ID: "0199a0b0-0000-7000-8000-000000000003", Name: "size_scene", Props: map[string]any{"nested": nested}}
	if err := client.Enqueue(event); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	nested["note"] = strings.Repeat("b", 3000)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	got := scene.received()
	if len(got) != 1 {
		t.Fatalf("sent %d elements, want 1", len(got))
	}
	if size := len(bytes.TrimSpace(got[0])); size > maxEventSceneBytes || !bytes.Contains(got[0], []byte(`"note":"short"`)) {
		t.Fatalf("sent a %d-byte element that is not the measured one: %.120s", size, got[0])
	}
}

// TestEventSizePrecheckRefusesOnlyTheOversizeEvent: the refused event never
// joins a batch, so its batchmates are sent.
func TestEventSizePrecheckRefusesOnlyTheOversizeEvent(t *testing.T) {
	client, scene := newSizeSceneClient(t)
	small := func(id string) Event { return Event{ID: id, Name: "size_scene"} }
	if err := client.Enqueue(small("0199a0b0-0000-7000-8000-000000000004")); err != nil {
		t.Fatal(err)
	}
	if err := client.Enqueue(sizedEvent(t, client, "0199a0b0-0000-7000-8000-000000000005", 4096)); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("oversize Enqueue = %v, want ErrEventTooLarge", err)
	}
	if err := client.Enqueue(small("0199a0b0-0000-7000-8000-000000000006")); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := scene.received(); len(got) != 2 {
		t.Fatalf("sent %d elements, want the 2 small events", len(got))
	}
}

// TestEventSizePrecheckAppliesToExperimentFacts: the SDK's own facts follow
// the same rule; ingest exempts only the retired experiment major.
func TestEventSizePrecheckAppliesToExperimentFacts(t *testing.T) {
	client, scene := newSizeSceneClient(t)
	fact := Event{
		Name:           experimentExposureName,
		omitUserID:     true,
		sourceOverride: SourceClient,
		Props:          map[string]any{"pad": strings.Repeat("c", 4096)},
	}
	if _, err := client.enqueueExperimentFact(fact, false); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("oversize fact intake = %v, want ErrEventTooLarge", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := scene.received(); len(got) != 0 {
		t.Fatalf("an oversize fact was sent (%d elements)", len(got))
	}
}

// TestAnOversizeSealedFactLeavesTheOwedQueue: a sealed fact ingest would
// reject is dropped once and leaves the owed queue, so it neither repeats
// every cycle nor holds the experiment's later facts.
func TestAnOversizeSealedFactLeavesTheOwedQueue(t *testing.T) {
	rig := newExpHopRig(t, nil)
	rig.script.apply.padProps = 3 * maxEventSceneBytes
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 1); err != nil {
		t.Fatalf("TrackExperimentOutcome: %v", err)
	}
	for range 3 {
		rig.cycle()
	}
	flushOrFail(t, rig.client)
	if owed := rig.owed(); owed != 0 {
		t.Fatalf("%d application(s) still owed: the oversize fact holds the queue", owed)
	}
	if got := rig.drops("event_too_large"); got != 1 {
		t.Fatalf("ExperimentExposureDrops[event_too_large] = %d, want 1", got)
	}
	if dropped := rig.client.Snapshot().Dropped; dropped != 0 {
		t.Fatalf("Dropped = %d: a fact refused before the queue is counted in its kind's drop map only", dropped)
	}
	if got := len(rig.capture.exposures()); got != 0 {
		t.Fatalf("the oversize exposure was sent (%d)", got)
	}
	var outcomes int
	for _, raw := range rig.capture.rawBatches() {
		outcomes += bytes.Count(raw, []byte(`"event_name":"`+experimentOutcomeName+`"`))
	}
	if outcomes != 1 {
		t.Fatalf("delivered %d outcome(s) behind the oversize exposure, want 1", outcomes)
	}
}

// TestAnEventLeftUnencodedAtIntakeIsMeasuredAtPublish: an event intake could
// not encode (a nested value that does not marshal) is measured when its
// batch builds it. Mutated past the limit after Enqueue, it is dropped and
// counted, never sent, and its batchmate is.
func TestAnEventLeftUnencodedAtIntakeIsMeasuredAtPublish(t *testing.T) {
	client, scene := newSizeSceneClient(t)
	nested := map[string]any{"value": make(chan int)}
	unencoded := Event{ID: "0199a0b0-0000-7000-8000-000000000010", Name: "size_scene", Props: map[string]any{"nested": nested}}
	if err := client.Enqueue(unencoded); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Enqueue(sizedEvent(t, client, "0199a0b0-0000-7000-8000-000000000011", 400)); err != nil {
		t.Fatalf("Enqueue batchmate: %v", err)
	}
	nested["value"] = strings.Repeat("a", 3000)
	_ = client.Flush(context.Background())
	got := scene.received()
	if len(got) != 1 || !bytes.Contains(got[0], []byte("000000000011")) {
		t.Fatalf("sent %d element(s); want only the batchmate", len(got))
	}
	if dropped := client.Snapshot().Dropped; dropped != 1 {
		t.Fatalf("Dropped = %d, want the oversize event", dropped)
	}
}

// flipMarshaler fails its first MarshalJSON and returns 3,000 bytes after:
// intake leaves the event unencoded, and its publish build measures it.
type flipMarshaler struct{ calls *atomic.Int32 }

func (m flipMarshaler) MarshalJSON() ([]byte, error) {
	if m.calls.Add(1) == 1 {
		return nil, errors.New("not yet")
	}
	return json.Marshal(strings.Repeat("a", 3000))
}

// TestTrackCountsAnEventOversizeAtPublishAsDropped: Track refuses such an
// event as intake would, counted once in Dropped and never as a failed
// batch, and sends nothing.
func TestTrackCountsAnEventOversizeAtPublishAsDropped(t *testing.T) {
	client, scene := newSizeSceneClient(t)
	event := Event{ID: "0199a0b0-0000-7000-8000-000000000012", Name: "size_scene", Props: map[string]any{"value": flipMarshaler{calls: new(atomic.Int32)}}}
	if err := client.Track(context.Background(), event); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("Track = %v, want ErrEventTooLarge", err)
	}
	if got := len(scene.received()); got != 0 {
		t.Fatalf("sent %d element(s), want none", got)
	}
	if stats := client.Snapshot(); stats.Dropped != 1 || stats.FailedBatches != 0 {
		t.Fatalf("Dropped = %d, FailedBatches = %d; want 1 and 0", stats.Dropped, stats.FailedBatches)
	}
}

// sentinelMarshaler returns the SDK's own size sentinel from MarshalJSON,
// so no bytes are ever produced to measure.
type sentinelMarshaler struct{}

func (sentinelMarshaler) MarshalJSON() ([]byte, error) { return nil, ErrEventTooLarge }

// TestTrackReportsAMarshalerReturningTheSizeSentinelAsAnEncodeFailure: an
// event that never marshaled was not measured, so Track reports the build
// failure, not a size drop.
func TestTrackReportsAMarshalerReturningTheSizeSentinelAsAnEncodeFailure(t *testing.T) {
	client, scene := newSizeSceneClient(t)
	event := Event{ID: "0199a0b0-0000-7000-8000-000000000013", Name: "size_scene", Props: map[string]any{"value": sentinelMarshaler{}}}
	var encodeErr *EncodeError
	if err := client.Track(context.Background(), event); !errors.As(err, &encodeErr) {
		t.Fatalf("Track = %v, want an *EncodeError", err)
	}
	if got := len(scene.received()); got != 0 {
		t.Fatalf("sent %d element(s), want none", got)
	}
	if stats := client.Snapshot(); stats.Dropped != 0 || stats.FailedBatches != 1 {
		t.Fatalf("Dropped = %d, FailedBatches = %d; want 0 and 1", stats.Dropped, stats.FailedBatches)
	}
}

// TestDropTimeCaptureSpoolsTheSiblingsOfAnOversizeFact: a dropped
// assignment's capture leaves out a sealed fact over the cap, which has no
// capturable form, and still spools its siblings before the durable delete.
func TestDropTimeCaptureSpoolsTheSiblingsOfAnOversizeFact(t *testing.T) {
	server := newExperimentServer(t, &expScript{}, &expWireCapture{})
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = t.TempDir() })
	defer client.Close(context.Background())
	client.SetConsent(true)
	sealed := func(id, pad string) *expOwedExposure {
		fact := `{"event_id":"` + expStubFactID(id) + `","event_name":"experiment_exposure","event_ts":"2026-09-27T00:00:00Z",` +
			`"workspace_key":"workspace-test","app_key":"app-test","environment_key":"develop","props":{"experiment_key":"exp-cap",` +
			`"experiment_version":1,"variant_key":"treatment` + pad + `","assignment_unit":"client_id","attestation":"client_attested"}}`
		return &expOwedExposure{entry: &expEntry{VariantKey: "treatment", Version: 1}, session: client.exp.sessionMarker,
			sealed: &expSealedFact{fact: json.RawMessage(fact), seal: expStubSeal}}
	}
	owed := []*expOwedExposure{sealed(strings.Repeat("c", 32), strings.Repeat("x", 3000)), sealed(strings.Repeat("d", 32), "")}
	if ok, _ := client.captureOwedExposuresForDrop("exp-cap", owed); !ok {
		t.Fatalf("the capture must be durable")
	}
	if got := spooledExperimentFacts(client, "exp-cap"); got != 1 {
		t.Fatalf("%d fact(s) spooled; want only the sibling of the oversize fact", got)
	}
}
