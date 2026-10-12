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
		WorkspaceID:               "workspace-test",
		AppID:                     "app-test",
		EnvironmentID:             "develop",
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
