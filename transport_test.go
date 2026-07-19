package shardpilot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTransportDoesNotRetryClientErrors(t *testing.T) {
	var requests atomic.Int64
	var logs strings.Builder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":"validation_error"}}`))
	}))
	defer server.Close()

	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "secret-token-value",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		Logger:        testLogger{out: &logs},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	err = client.Track(context.Background(), Event{
		ID:   "evt-bad-request",
		Name: "server_event",
	})
	if err == nil {
		t.Fatal("expected Track error")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected typed HTTP status error, got %v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("expected one request, got %d", requests.Load())
	}

	stats := client.Snapshot()
	if stats.FailedBatches != 1 {
		t.Fatalf("expected failed batch count 1, got %d", stats.FailedBatches)
	}
	if strings.Contains(stats.LastError, "secret-token-value") ||
		strings.Contains(logs.String(), "secret-token-value") {
		t.Fatal("token leaked into error or log output")
	}
	if strings.Contains(logs.String(), "server_event") {
		t.Fatal("event payload leaked into log output")
	}
}

type testLogger struct {
	out *strings.Builder
}

func (l testLogger) Printf(format string, args ...any) {
	l.out.WriteString(format)
	for _, arg := range args {
		l.out.WriteString(" ")
		l.out.WriteString(fmt.Sprint(arg))
	}
}

// recordingRoundTripper proves requests ride an injected client by recording
// every request path it carries before delegating to the default transport.
type recordingRoundTripper struct {
	mu    sync.Mutex
	paths []string
}

func (r *recordingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.paths = append(r.paths, request.URL.Path)
	r.mu.Unlock()
	return http.DefaultTransport.RoundTrip(request)
}

func (r *recordingRoundTripper) sawPath(path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, seen := range r.paths {
		if seen == path {
			return true
		}
	}
	return false
}

func TestInjectedHTTPClientCarriesAllRequests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/events:batch":
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprint(w, `{"accepted":1,"rejected":0,"duplicates":0}`)
		case "/v1/consent":
			fmt.Fprint(w, `{"recorded":true,"replayed":false}`)
		default:
			fmt.Fprint(w, `{"values":{"k":"v"}}`)
		}
	}))
	defer server.Close()

	recorder := &recordingRoundTripper{}
	client, err := NewClient(Config{
		IngestURL:       server.URL,
		Token:           "test-token",
		WorkspaceID:     "workspace-test",
		AppID:           "app-test",
		EnvironmentID:   "develop",
		Source:          SourceBackend,
		AnonymousID:     "anon-inject-1",
		APIKey:          "test-rc-key",
		RemoteConfigURL: server.URL,
		HTTPClient:      &http.Client{Transport: recorder},
		FlushInterval:   time.Hour,
		HTTPTimeout:     time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if err := client.Track(context.Background(), Event{Name: "injected"}); err != nil {
		t.Fatalf("Track: %v", err)
	}
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("FetchRemoteConfig: %v", err)
	}
	client.SetConsent(true)
	// Close waits (bounded) for the consent sender, so the consent POST has
	// happened by the time it returns.
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for _, path := range []string{"/v1/events:batch", "/v1/consent"} {
		if !recorder.sawPath(path) {
			t.Fatalf("expected %s carried by the injected client, saw %v", path, recorder.paths)
		}
	}
	// The remote-config GET is the only non-ingest path.
	sawRC := false
	recorder.mu.Lock()
	for _, seen := range recorder.paths {
		if seen != "/v1/events:batch" && seen != "/v1/consent" {
			sawRC = true
		}
	}
	recorder.mu.Unlock()
	if !sawRC {
		t.Fatalf("expected the remote-config fetch carried by the injected client, saw %v", recorder.paths)
	}
}

func TestInjectedHTTPClientRemoteConfigStillRefusesRedirects(t *testing.T) {
	var followed atomic.Int64
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/elsewhere" {
			followed.Add(1)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"values":{"k":"redirected"}}`)
			return
		}
		w.Header().Set("Location", server.URL+"/elsewhere")
		w.WriteHeader(http.StatusFound)
	}))
	defer server.Close()

	// A plain injected client WOULD follow the 302; the SDK must derive its
	// remote-config client with the redirect refusal pinned — the 3xx is the
	// contract's permanent http_3xx outcome, never the target's body.
	client, err := NewClient(Config{
		IngestURL:       server.URL,
		Token:           "test-token",
		WorkspaceID:     "workspace-test",
		AppID:           "app-test",
		EnvironmentID:   "develop",
		Source:          SourceBackend,
		AnonymousID:     "anon-inject-2",
		APIKey:          "test-rc-key",
		RemoteConfigURL: server.URL,
		HTTPClient:      &http.Client{},
		FlushInterval:   time.Hour,
		HTTPTimeout:     time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	_, err = client.FetchRemoteConfig(context.Background())
	if err == nil || !strings.Contains(err.Error(), "http_302") {
		t.Fatalf("expected the redirect classified as permanent http_302 under an injected client, got %v", err)
	}
	if followed.Load() != 0 {
		t.Fatalf("expected the redirect target never requested, got %d requests", followed.Load())
	}
}
