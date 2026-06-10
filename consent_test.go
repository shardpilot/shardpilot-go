package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
)

type capturedConsent struct {
	authHeader  string
	contentType string
	body        map[string]any
}

func newConsentTestServer(t *testing.T, eventCount *atomic.Int64, consents chan capturedConsent) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/events:batch":
			var request batchRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode batch request: %v", err)
			}
			eventCount.Add(int64(len(request.Events)))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"accepted":0,"rejected":0,"duplicates":0}`))
		case "/v1/consent":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode consent request: %v", err)
			}
			if consents != nil {
				consents <- capturedConsent{
					authHeader:  r.Header.Get("Authorization"),
					contentType: r.Header.Get("Content-Type"),
					body:        body,
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"recorded":true,"replayed":false}`))
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func newConsentTestClient(t *testing.T, ingestURL string, userID, anonymousID string) *Client {
	t.Helper()
	client, err := NewClient(Config{
		IngestURL:     ingestURL,
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		UserID:        userID,
		AnonymousID:   anonymousID,
		BatchSize:     10,
		BufferSize:    16,
		FlushInterval: time.Hour,
		HTTPTimeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func waitForConsent(t *testing.T, consents chan capturedConsent) capturedConsent {
	t.Helper()
	select {
	case captured := <-consents:
		return captured
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for consent POST")
		return capturedConsent{}
	}
}

func TestConsentTriStateGatingAndQueueClear(t *testing.T) {
	var eventCount atomic.Int64
	consents := make(chan capturedConsent, 4)
	server := newConsentTestServer(t, &eventCount, consents)
	defer server.Close()

	client := newConsentTestClient(t, server.URL, "", "anon-actor")
	defer client.Close(context.Background())

	if state := client.Consent(); state != ConsentUnknown {
		t.Fatalf("expected initial consent state unknown, got %q", state)
	}

	// Unknown state is fully open.
	for _, name := range []string{"purchase", "economy_tx", "match_end"} {
		if err := client.Enqueue(Event{Name: name}); err != nil {
			t.Fatalf("Enqueue under unknown consent returned error: %v", err)
		}
	}

	client.SetConsent(false)
	waitForConsent(t, consents)

	if state := client.Consent(); state != ConsentDenied {
		t.Fatalf("expected consent state denied, got %q", state)
	}
	if err := client.Enqueue(Event{Name: "purchase"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected ErrConsentDenied from Enqueue, got %v", err)
	}
	if err := client.Track(context.Background(), Event{Name: "purchase"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected ErrConsentDenied from Track, got %v", err)
	}

	// Flush must publish nothing: the pending queue (and any worker-held
	// batch) is cleared on denial.
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if got := eventCount.Load(); got != 0 {
		t.Fatalf("expected no events published after denial, got %d", got)
	}
	stats := client.Snapshot()
	if stats.Dropped != 5 {
		t.Fatalf("expected 3 cleared + 2 rejected = 5 dropped events, got %d", stats.Dropped)
	}

	client.SetConsent(true)
	waitForConsent(t, consents)

	if state := client.Consent(); state != ConsentGranted {
		t.Fatalf("expected consent state granted, got %q", state)
	}
	if err := client.Enqueue(Event{Name: "purchase"}); err != nil {
		t.Fatalf("Enqueue after grant returned error: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after grant returned error: %v", err)
	}
	if got := eventCount.Load(); got != 1 {
		t.Fatalf("expected 1 event published after grant, got %d", got)
	}
}

func TestSetConsentPostsConsentDecisionShape(t *testing.T) {
	var eventCount atomic.Int64
	consents := make(chan capturedConsent, 4)
	server := newConsentTestServer(t, &eventCount, consents)
	defer server.Close()

	client := newConsentTestClient(t, server.URL, "user-actor", "anon-actor")
	defer client.Close(context.Background())

	before := time.Now().UTC().Add(-time.Minute)
	client.SetConsent(true)
	granted := waitForConsent(t, consents)

	if granted.authHeader != "Bearer test-token" {
		t.Fatalf("unexpected consent auth header %q", granted.authHeader)
	}
	if granted.contentType != "application/json" {
		t.Fatalf("unexpected consent content type %q", granted.contentType)
	}
	for field, want := range map[string]string{
		"workspace_id":   "workspace-test",
		"app_id":         "app-test",
		"environment_id": "develop",
		// user_id is preferred over anonymous_id.
		"actor_identifier": "user-actor",
	} {
		if got := granted.body[field]; got != want {
			t.Fatalf("consent body %s = %v, want %q", field, got, want)
		}
	}
	categories, ok := granted.body["categories"].(map[string]any)
	if !ok || categories["analytics"] != true {
		t.Fatalf("expected categories.analytics true, got %v", granted.body["categories"])
	}
	decidedAtRaw, _ := granted.body["decided_at"].(string)
	decidedAt, err := time.Parse(time.RFC3339, decidedAtRaw)
	if err != nil {
		t.Fatalf("decided_at %q is not RFC3339: %v", decidedAtRaw, err)
	}
	if decidedAt.Before(before) {
		t.Fatalf("decided_at %v is implausibly old", decidedAt)
	}
	grantedKey, _ := granted.body["idempotency_key"].(string)
	if !uuidv7.IsValid(grantedKey) {
		t.Fatalf("idempotency_key %q is not a UUIDv7", grantedKey)
	}
	for _, forbidden := range []string{"event_id", "event_name", "props", "source"} {
		if _, exists := granted.body[forbidden]; exists {
			t.Fatalf("consent body must not carry event envelope field %q", forbidden)
		}
	}

	client.SetConsent(false)
	denied := waitForConsent(t, consents)
	categories, ok = denied.body["categories"].(map[string]any)
	if !ok || categories["analytics"] != false {
		t.Fatalf("expected categories.analytics false, got %v", denied.body["categories"])
	}
	deniedKey, _ := denied.body["idempotency_key"].(string)
	if !uuidv7.IsValid(deniedKey) || deniedKey == grantedKey {
		t.Fatalf("expected a fresh UUIDv7 idempotency key, got %q (previous %q)", deniedKey, grantedKey)
	}
}

func TestSetConsentFallsBackToAnonymousActorAndSkipsPostWithoutIdentity(t *testing.T) {
	var eventCount atomic.Int64
	consents := make(chan capturedConsent, 4)
	server := newConsentTestServer(t, &eventCount, consents)
	defer server.Close()

	anonClient := newConsentTestClient(t, server.URL, "", "anon-actor")
	defer anonClient.Close(context.Background())
	anonClient.SetConsent(true)
	captured := waitForConsent(t, consents)
	if captured.body["actor_identifier"] != "anon-actor" {
		t.Fatalf("expected anonymous actor fallback, got %v", captured.body["actor_identifier"])
	}

	noIdentityClient := newConsentTestClient(t, server.URL, "", "")
	defer noIdentityClient.Close(context.Background())
	noIdentityClient.SetConsent(false)
	if state := noIdentityClient.Consent(); state != ConsentDenied {
		t.Fatalf("expected local denial without identity, got %q", state)
	}
	if err := noIdentityClient.Enqueue(Event{Name: "purchase"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected local gating without identity, got %v", err)
	}
	select {
	case unexpected := <-consents:
		t.Fatalf("expected no consent POST without actor identity, got %v", unexpected.body)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestSetConsentPublishFailureIsQuietAndKeepsLocalState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	logs := make(chan string, 8)
	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		AnonymousID:   "anon-actor",
		FlushInterval: time.Hour,
		HTTPTimeout:   200 * time.Millisecond,
		Logger:        chanLogger{logs: logs},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	client.SetConsent(false)
	select {
	case line := <-logs:
		if !strings.Contains(line, "consent publish failed") {
			t.Fatalf("unexpected consent failure log %q", line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for quiet consent failure log")
	}
	if state := client.Consent(); state != ConsentDenied {
		t.Fatalf("expected local state to survive publish failure, got %q", state)
	}
	if err := client.Enqueue(Event{Name: "purchase"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected gating to survive publish failure, got %v", err)
	}
}

type chanLogger struct {
	logs chan string
}

func (l chanLogger) Printf(format string, args ...any) {
	select {
	case l.logs <- fmt.Sprintf(format, args...):
	default:
	}
}
