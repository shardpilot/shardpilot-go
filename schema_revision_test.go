package shardpilot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// capturedRevisionHeader records, for one ingest request, which route it hit
// and whether the schema-revision request header was present (and its value).
type capturedRevisionHeader struct {
	route   string
	present bool
	value   string
}

// newSchemaRevisionTestServer serves both ingest routes, records each
// request's X-ShardPilot-Schema-Revision header into headers, and answers
// with a minimal valid success body per route.
func newSchemaRevisionTestServer(t *testing.T, headers chan capturedRevisionHeader) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values(schemaRevisionHeader)
		captured := capturedRevisionHeader{route: r.URL.Path, present: len(values) > 0}
		if captured.present {
			captured.value = values[0]
		}
		headers <- captured
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/events:batch":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
		case "/v1/consent":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"recorded":true,"replayed":false}`))
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func newSchemaRevisionTestClient(t *testing.T, ingestURL string, mutate func(*Config)) *Client {
	t.Helper()
	cfg := Config{
		IngestURL:     ingestURL,
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		BatchSize:     10,
		BufferSize:    16,
		FlushInterval: time.Hour,
		HTTPTimeout:   time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func waitForRevisionHeader(t *testing.T, headers chan capturedRevisionHeader) capturedRevisionHeader {
	t.Helper()
	select {
	case captured := <-headers:
		return captured
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for an ingest request")
		return capturedRevisionHeader{}
	}
}

func TestBatchPublishDeclaresDefaultSchemaRevision(t *testing.T) {
	// Pin the compiled-in constant byte for byte: it is the digest of the
	// ingest service's embedded schema set this SDK release was coordinated
	// against, and an accidental edit must fail loudly.
	const pinned = "sha256:e1ba01d4b76b9e73444e2edd5639281929fd89496cadc1dcc79eb68208c6a0a0"
	if DefaultSchemaRevision != pinned {
		t.Fatalf("DefaultSchemaRevision = %q, want the pinned coordination digest %q", DefaultSchemaRevision, pinned)
	}

	headers := make(chan capturedRevisionHeader, 4)
	server := newSchemaRevisionTestServer(t, headers)
	defer server.Close()

	client := newSchemaRevisionTestClient(t, server.URL, nil)
	defer client.Close(context.Background())

	if err := client.Track(context.Background(), Event{Name: "server_event"}); err != nil {
		t.Fatalf("Track: %v", err)
	}
	captured := waitForRevisionHeader(t, headers)
	if captured.route != "/v1/events:batch" {
		t.Fatalf("expected a batch request, got %q", captured.route)
	}
	if !captured.present {
		t.Fatal("batch request carried no schema-revision header")
	}
	if captured.value != DefaultSchemaRevision {
		t.Fatalf("batch schema-revision header = %q, want %q", captured.value, DefaultSchemaRevision)
	}
}

func TestConsentRouteNeverCarriesSchemaRevision(t *testing.T) {
	headers := make(chan capturedRevisionHeader, 4)
	server := newSchemaRevisionTestServer(t, headers)
	defer server.Close()

	client := newSchemaRevisionTestClient(t, server.URL, func(cfg *Config) {
		cfg.UserID = "user-actor"
	})
	defer client.Close(context.Background())

	// Same client instance, both routes: the batch publish must declare the
	// revision while the consent publish must not — the header is defined for
	// the events:batch endpoint only, and postJSON is shared across routes.
	if err := client.Track(context.Background(), Event{Name: "server_event"}); err != nil {
		t.Fatalf("Track: %v", err)
	}
	batch := waitForRevisionHeader(t, headers)
	if batch.route != "/v1/events:batch" || !batch.present {
		t.Fatalf("expected the batch request to declare the revision, got %+v", batch)
	}

	client.SetConsent(true)
	consent := waitForRevisionHeader(t, headers)
	if consent.route != "/v1/consent" {
		t.Fatalf("expected a consent request, got %q", consent.route)
	}
	if consent.present {
		t.Fatalf("consent request must not carry %s, got %q", schemaRevisionHeader, consent.value)
	}
}

func TestSchemaRevisionOverrideIsDeclared(t *testing.T) {
	// Runtime-assembled override value; sha256-shaped like a real revision.
	custom := "sha256:" + strings.Repeat("42", 32)

	headers := make(chan capturedRevisionHeader, 4)
	server := newSchemaRevisionTestServer(t, headers)
	defer server.Close()

	client := newSchemaRevisionTestClient(t, server.URL, func(cfg *Config) {
		cfg.SchemaRevision = custom
	})
	defer client.Close(context.Background())

	if err := client.Track(context.Background(), Event{Name: "server_event"}); err != nil {
		t.Fatalf("Track: %v", err)
	}
	captured := waitForRevisionHeader(t, headers)
	if !captured.present || captured.value != custom {
		t.Fatalf("batch schema-revision header = %+v, want override %q", captured, custom)
	}
}

func TestDisableSchemaRevisionOmitsHeader(t *testing.T) {
	headers := make(chan capturedRevisionHeader, 4)
	server := newSchemaRevisionTestServer(t, headers)
	defer server.Close()

	// Disable wins even over an explicit override: "stop declaring" must be
	// absolute, matching the server's undeclared-always-passes escape hatch.
	client := newSchemaRevisionTestClient(t, server.URL, func(cfg *Config) {
		cfg.SchemaRevision = "sha256:" + strings.Repeat("42", 32)
		cfg.DisableSchemaRevision = true
	})
	defer client.Close(context.Background())

	if err := client.Track(context.Background(), Event{Name: "server_event"}); err != nil {
		t.Fatalf("Track: %v", err)
	}
	captured := waitForRevisionHeader(t, headers)
	if captured.route != "/v1/events:batch" {
		t.Fatalf("expected a batch request, got %q", captured.route)
	}
	if captured.present {
		t.Fatalf("disabled client must not declare a schema revision, got %q", captured.value)
	}
}

// schemaRevisionMismatchBody mirrors the enforce-mode 409 envelope the ingest
// service sends when the declared revision does not match the served one.
const schemaRevisionMismatchBody = `{"error":{"code":"schema_revision_mismatch",` +
	`"message":"the declared schema revision does not match the schema revision this ingest-api serves",` +
	`"details":[{"field":"X-ShardPilot-Schema-Revision","code":"schema_revision_mismatch",` +
	`"message":"redeploy the writer against the current schema set or stop declaring a revision"}]}}`

func TestSchemaRevisionMismatch409IsTerminalAndLogged(t *testing.T) {
	var requests atomic.Int64
	var logs strings.Builder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(schemaRevisionMismatchBody))
	}))
	defer server.Close()

	client := newSchemaRevisionTestClient(t, server.URL, func(cfg *Config) {
		cfg.Logger = testLogger{out: &logs}
	})
	defer client.Close(context.Background())

	err := client.Track(context.Background(), Event{Name: "server_event"})
	if err == nil {
		t.Fatal("expected Track error")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusConflict ||
		statusErr.ErrorCode != schemaRevisionMismatchCode {
		t.Fatalf("expected a 409 %s error, got %v", schemaRevisionMismatchCode, err)
	}
	if !isSchemaRevisionMismatch(err) {
		t.Fatalf("expected the error to classify as a schema-revision mismatch: %v", err)
	}
	// Terminal for the batch: the worker's drop decision routes through
	// isPermanentPublishError, so the mismatch must classify as permanent —
	// dropped, never retried or split.
	if !isPermanentPublishError(err) {
		t.Fatalf("expected a schema-revision mismatch to be a permanent publish error: %v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("expected exactly one request (no retry), got %d", requests.Load())
	}
	if stats := client.Snapshot(); stats.FailedBatches != 1 {
		t.Fatalf("expected failed batch count 1, got %d", stats.FailedBatches)
	}
	logged := logs.String()
	if !strings.Contains(logged, "schema revision mismatch") ||
		!strings.Contains(logged, "dropped as terminal") {
		t.Fatalf("expected a dedicated schema-revision mismatch log line, got %q", logged)
	}
	if !strings.Contains(logged, DefaultSchemaRevision) {
		t.Fatalf("expected the log line to name the declared revision, got %q", logged)
	}
}

func TestOther409IsNotClassifiedAsSchemaRevisionMismatch(t *testing.T) {
	var logs strings.Builder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"code":"workspace_override_conflict","message":"conflicting workspace override"}}`))
	}))
	defer server.Close()

	client := newSchemaRevisionTestClient(t, server.URL, func(cfg *Config) {
		cfg.Logger = testLogger{out: &logs}
	})
	defer client.Close(context.Background())

	err := client.Track(context.Background(), Event{Name: "server_event"})
	if err == nil {
		t.Fatal("expected Track error")
	}
	// 409 is a shared status: a different conflict code must not classify as
	// the handshake mismatch (still permanent today via the generic
	// non-retryable branch, but eligible for any future 409-specific
	// handling; the mismatch never is).
	if isSchemaRevisionMismatch(err) {
		t.Fatalf("workspace_override_conflict must not classify as a schema-revision mismatch: %v", err)
	}
	if !isPermanentPublishError(err) {
		t.Fatalf("expected a 409 to remain a permanent publish error: %v", err)
	}
	logged := logs.String()
	if strings.Contains(logged, "schema revision mismatch") {
		t.Fatalf("expected the generic failure log line for other 409 codes, got %q", logged)
	}
	if !strings.Contains(logged, "batch publish failed") {
		t.Fatalf("expected the generic failure log line, got %q", logged)
	}
}

func TestIsSchemaRevisionMismatchClassification(t *testing.T) {
	mismatch := &HTTPStatusError{StatusCode: http.StatusConflict, ErrorCode: schemaRevisionMismatchCode}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil error", err: nil, want: false},
		{name: "plain error", err: errors.New("boom"), want: false},
		{name: "409 with mismatch code", err: mismatch, want: true},
		{name: "wrapped 409 with mismatch code", err: fmt.Errorf("publish: %w", mismatch), want: true},
		{name: "409 with other conflict code", err: &HTTPStatusError{StatusCode: http.StatusConflict, ErrorCode: "static_token_workspace_conflict"}, want: false},
		{name: "409 without envelope code", err: &HTTPStatusError{StatusCode: http.StatusConflict}, want: false},
		{name: "non-409 with mismatch code", err: &HTTPStatusError{StatusCode: http.StatusBadRequest, ErrorCode: schemaRevisionMismatchCode}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSchemaRevisionMismatch(tc.err); got != tc.want {
				t.Fatalf("isSchemaRevisionMismatch(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
