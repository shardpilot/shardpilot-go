package crash

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var benchmarkEmitSink Event

func TestClientEmitRoundTrip(t *testing.T) {
	var received Event
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer workspace-api-key-test" {
			t.Fatalf("unexpected Authorization header: %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("unexpected Content-Type header: %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		for _, disallowed := range []string{"@", "198.51.100.40", "2001:db8::40", "header.eyJzdWIiOiJ0ZXN0In0.signature"} {
			if strings.Contains(string(body), disallowed) {
				t.Fatalf("payload leaked disallowed fixture %q: %s", disallowed, body)
			}
		}
		if err := json.Unmarshal(body, &received); err != nil {
			t.Fatalf("decode event: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client, err := NewClient(ClientOptions{
		IngestURL: server.URL,
		APIKey:    "workspace-api-key-test",
		Sampler:   alwaysSampler{},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client.RecordBreadcrumb("session_start")
	client.RecordBreadcrumb("match.round-start")
	client.RecordBreadcrumb("screen_open")

	event := Event{
		AppVersion:  "sample@example.invalid",
		BuildID:     "header.eyJzdWIiOiJ0ZXN0In0.signature",
		OS:          OSInfo{Name: "linux", Version: "198.51.100.40"},
		DeviceClass: DeviceClassDesktop,
		StackFrames: []Frame{{
			Function: "main.run",
			File:     "2001:db8::40",
			Line:     42,
			Module:   "device_raw_identifier",
		}},
		ThreadState: ThreadStateMain,
		SessionID:   "sha256-session-hash-test",
	}

	if err := client.Emit(context.Background(), event); err != nil {
		t.Fatalf("Emit returned error: %v", err)
	}

	if !isUUIDv7(received.CrashID) {
		t.Fatalf("expected generated UUIDv7 crash id, got %q", received.CrashID)
	}
	if received.OccurredAt.IsZero() || received.OccurredAt.Location() != time.UTC {
		t.Fatalf("expected non-zero UTC occurred_at, got %v", received.OccurredAt)
	}
	if len(received.Breadcrumbs) != 3 {
		t.Fatalf("expected ring breadcrumbs in payload, got %#v", received.Breadcrumbs)
	}
	if received.AppVersion != "" || received.BuildID != "" || received.OS.Version != "" {
		t.Fatalf("expected unsafe optional strings to be stripped: %#v", received)
	}
	if got := received.StackFrames[0].File; got != "" {
		t.Fatalf("expected unsafe frame file stripped, got %q", got)
	}
	if got := received.StackFrames[0].Module; got != "" {
		t.Fatalf("expected unsafe frame module stripped, got %q", got)
	}
	assertEventHasNoDisallowedStrings(t, received)
}

func TestClientEmitUsesCallerBreadcrumbs(t *testing.T) {
	var received Event
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Fatalf("decode event: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client, err := NewClient(ClientOptions{IngestURL: server.URL, APIKey: "workspace-api-key-test", Sampler: alwaysSampler{}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client.RecordBreadcrumb("ring_event")

	event := validEvent(t)
	event.Breadcrumbs = []Breadcrumb{{Name: "caller_event", Timestamp: time.Unix(1700000100, 0)}}
	if err := client.Emit(context.Background(), event); err != nil {
		t.Fatalf("Emit returned error: %v", err)
	}

	if len(received.Breadcrumbs) != 1 || received.Breadcrumbs[0].Name != "caller_event" {
		t.Fatalf("expected caller breadcrumbs to win, got %#v", received.Breadcrumbs)
	}
	if received.Breadcrumbs[0].Timestamp.Location() != time.UTC {
		t.Fatalf("expected caller breadcrumb timestamp normalized to UTC")
	}
}

func TestClientEmitRejectsInvalidEventBeforePost(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client, err := NewClient(ClientOptions{IngestURL: server.URL, APIKey: "workspace-api-key-test", Sampler: alwaysSampler{}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	event := validEvent(t)
	event.SessionID = "player_session_hash"

	if err := client.Emit(context.Background(), event); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("expected invalid event not to post, got %d requests", got)
	}
}

func TestClientEmitFatalBypassesSampler(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client, err := NewClient(ClientOptions{IngestURL: server.URL, APIKey: "workspace-api-key-test", Sampler: neverSampler{}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if err := client.Emit(context.Background(), validEvent(t)); err != nil {
		t.Fatalf("Emit returned error: %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("expected non-fatal event to be sampled out, got %d requests", got)
	}
	if err := client.EmitFatal(context.Background(), validEvent(t)); err != nil {
		t.Fatalf("EmitFatal returned error: %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("expected fatal event to bypass sampler, got %d requests", got)
	}
}

func TestDefaultSamplerEmitsTenPercent(t *testing.T) {
	sampler := newDefaultSampler()
	var emitted int
	for i := 0; i < 100; i++ {
		if sampler.ShouldEmit(Event{}) {
			emitted++
		}
	}
	if emitted != 10 {
		t.Fatalf("expected deterministic 10%% default sampling, got %d%%", emitted)
	}
}

func TestNewClientDefaultHTTPTimeout(t *testing.T) {
	client, err := NewClient(ClientOptions{IngestURL: "https://ingest.example.invalid/crashes", APIKey: "workspace-api-key-test"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if client.httpClient.Timeout != defaultHTTPTimeout {
		t.Fatalf("expected default HTTP timeout %v, got %v", defaultHTTPTimeout, client.httpClient.Timeout)
	}
}

func BenchmarkClientEmit(b *testing.B) {
	client, err := NewClient(ClientOptions{
		IngestURL: "https://ingest.example.invalid/crashes",
		APIKey:    "workspace-api-key-test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			_, _ = io.Copy(io.Discard, req.Body)
			return &http.Response{
				StatusCode: http.StatusAccepted,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    req,
			}, nil
		})},
		Sampler: alwaysSampler{},
	})
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	event := validEventForBenchmark()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Emit(context.Background(), event); err != nil {
			b.Fatalf("Emit: %v", err)
		}
		benchmarkEmitSink = event
	}
}

type alwaysSampler struct{}

func (alwaysSampler) ShouldEmit(Event) bool { return true }

type neverSampler struct{}

func (neverSampler) ShouldEmit(Event) bool { return false }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func validEventForBenchmark() Event {
	return Event{
		CrashID:     "018bcfe5-5680-7cc8-a7b8-7f6b0a5969de",
		AppVersion:  "0.2.0-alpha-bench",
		BuildID:     "build-bench",
		OS:          OSInfo{Name: "linux", Version: "bench"},
		DeviceClass: DeviceClassDesktop,
		StackFrames: []Frame{{Function: "main.run", File: "main.go", Line: 42, Module: "bench-module"}},
		Breadcrumbs: []Breadcrumb{{Name: "screen_open", Timestamp: time.Unix(1700000001, 0).UTC()}},
		ThreadState: ThreadStateMain,
		SessionID:   "sha256-session-hash-bench",
		OccurredAt:  time.Unix(1700000002, 0).UTC(),
	}
}
