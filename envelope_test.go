package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTrackSendsAppFirstEnvelope(t *testing.T) {
	var requestPath string
	var authHeader string
	var body string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath = r.URL.Path
		authHeader = r.Header.Get("Authorization")
		var raw map[string]any
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		encoded, err := json.Marshal(raw)
		if err != nil {
			t.Fatalf("re-encode request: %v", err)
		}
		body = string(encoded)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	defer client.Close(context.Background())

	err := client.Track(context.Background(), Event{
		ID:              "evt-test-1",
		Name:            "match_end",
		Timestamp:       time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		AnonymousID:     "anonymous-example",
		SessionID:       "session-example",
		SessionSequence: 1,
		Props: map[string]any{
			"surface":  "test",
			"match_id": "match-example",
		},
	})
	if err != nil {
		t.Fatalf("Track returned error: %v", err)
	}

	if requestPath != "/v1/events:batch" {
		t.Fatalf("unexpected path %q", requestPath)
	}
	if authHeader != "Bearer test-token" {
		t.Fatalf("unexpected auth header %q", authHeader)
	}

	for _, field := range []string{
		`"workspace_id":"workspace-test"`,
		`"app_id":"app-test"`,
		`"environment_id":"develop"`,
		`"event_ts":"2026-05-14T12:00:00Z"`,
		`"session_sequence":1`,
		`"app_version":"0.1.0"`,
		`"app_build":"100"`,
		`"match_id":"match-example"`,
	} {
		if !strings.Contains(body, field) {
			t.Fatalf("payload missing %s in %s", field, body)
		}
	}

	for _, legacy := range []string{
		`"project_id"`,
		`"game_id"`,
		`"env"`,
		`"event_ts_server"`,
		`"event_seq_session"`,
		`"build_version"`,
	} {
		if strings.Contains(body, legacy) {
			t.Fatalf("payload contains legacy field %s in %s", legacy, body)
		}
	}
}

func TestConfigValidationAndDefaults(t *testing.T) {
	if _, err := NewClient(Config{}); err == nil {
		t.Fatal("expected empty config error")
	}

	_, err := NewClient(Config{
		IngestURL:     "http://example.com",
		Token:         "token",
		WorkspaceID:   "workspace",
		AppID:         "app",
		EnvironmentID: "develop",
		Source:        SourceBackend,
	})
	if err == nil {
		t.Fatal("expected non-local http URL to be rejected")
	}

	client, err := NewClient(Config{
		IngestURL:     "http://localhost:8080/",
		Token:         "token",
		WorkspaceID:   "workspace",
		AppID:         "app",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		BatchSize:     500,
	})
	if err != nil {
		t.Fatalf("expected localhost URL to be allowed: %v", err)
	}
	defer client.Close(context.Background())

	_, err = NewClient(Config{
		IngestURL:     "http://10.0.0.10:8080",
		Token:         "token",
		WorkspaceID:   "workspace",
		AppID:         "app",
		EnvironmentID: "develop",
		Source:        SourceBackend,
	})
	if err == nil {
		t.Fatal("expected private network http URL to require explicit opt-in")
	}

	privateClient, err := NewClient(Config{
		IngestURL:                   "http://10.0.0.10:8080",
		Token:                       "token",
		WorkspaceID:                 "workspace",
		AppID:                       "app",
		EnvironmentID:               "develop",
		Source:                      SourceBackend,
		AllowInsecurePrivateNetwork: true,
	})
	if err != nil {
		t.Fatalf("expected private network http URL to be allowed with explicit opt-in: %v", err)
	}
	defer privateClient.Close(context.Background())

	for name, ingestURL := range map[string]string{
		"user info":     "https://token@example.com",
		"query":         "https://example.com?token=value",
		"fragment":      "https://example.com#fragment",
		"non-root path": "https://example.com/ingest",
	} {
		t.Run("rejects "+name, func(t *testing.T) {
			_, err := NewClient(Config{
				IngestURL:     ingestURL,
				Token:         "token",
				WorkspaceID:   "workspace",
				AppID:         "app",
				EnvironmentID: "develop",
				Source:        SourceBackend,
			})
			if err == nil {
				t.Fatalf("expected ingest URL %q to be rejected", ingestURL)
			}
		})
	}

	if client.cfg.IngestURL != "http://localhost:8080" {
		t.Fatalf("unexpected normalized URL %q", client.cfg.IngestURL)
	}
	if client.cfg.BatchSize != maxBatchSize {
		t.Fatalf("expected batch size cap %d, got %d", maxBatchSize, client.cfg.BatchSize)
	}
	if client.cfg.BufferSize != defaultBufferSize {
		t.Fatalf("expected default buffer size %d, got %d", defaultBufferSize, client.cfg.BufferSize)
	}
}

func newTestClient(t *testing.T, ingestURL string) *Client {
	t.Helper()
	client, err := NewClient(Config{
		IngestURL:     ingestURL,
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		AppVersion:    "0.1.0",
		AppBuild:      "100",
		Platform:      "linux",
		BatchSize:     2,
		BufferSize:    4,
		FlushInterval: time.Hour,
		HTTPTimeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func TestBuildBatchIsolatingAttributesPoisonMembers(t *testing.T) {
	client := &Client{
		cfg: Config{
			WorkspaceID:   "workspace-test",
			AppID:         "app-test",
			EnvironmentID: "develop",
			Source:        SourceBackend,
		},
		clock: realClock{},
	}
	now := time.Now()
	ok1 := Event{ID: "evt-iso-1", Name: "e1", Timestamp: now}
	poison := Event{ID: "evt-iso-poison", Name: "e2", Timestamp: now, Props: map[string]any{"bad": func() {}}}
	ok2 := Event{ID: "evt-iso-3", Name: "e3", Timestamp: now}

	// Mixed batch, nothing retained: the poison member is attributed by id
	// with the EncodeError class, and the request/kept pair carries exactly
	// its batchmates, aligned.
	request, kept, poisoned := client.buildBatchIsolating([]Event{ok1, poison, ok2}, batchRequest{})
	if len(request.Events) != 2 || request.Events[0].EventID != "evt-iso-1" || request.Events[1].EventID != "evt-iso-3" {
		t.Fatalf("expected the two serializable members built, got %+v", request.Events)
	}
	if len(request.rawEvents) != 2 {
		t.Fatalf("expected raw bytes aligned with the built envelopes, got %d", len(request.rawEvents))
	}
	if len(kept) != 2 || kept[0].ID != "evt-iso-1" || kept[1].ID != "evt-iso-3" {
		t.Fatalf("expected kept aligned with the request, got %+v", kept)
	}
	if len(poisoned) != 1 || poisoned[0].id != "evt-iso-poison" {
		t.Fatalf("expected the poison member attributed by id, got %+v", poisoned)
	}
	var encodeErr *EncodeError
	if !errors.As(poisoned[0].err, &encodeErr) {
		t.Fatalf("expected the EncodeError class attributed, got %v", poisoned[0].err)
	}

	// A retained prefix rides verbatim: the reused member's bytes are the
	// retained bytes, never a re-marshal, even with a poison member behind it.
	retained, err := client.buildBatch([]Event{ok1})
	if err != nil {
		t.Fatalf("buildBatch: %v", err)
	}
	request, kept, poisoned = client.buildBatchIsolating([]Event{ok1, poison, ok2}, retained)
	if len(request.rawEvents) != 2 || string(request.rawEvents[0]) != string(retained.rawEvents[0]) {
		t.Fatalf("expected the retained member's bytes reused verbatim")
	}
	if len(kept) != 2 || len(poisoned) != 1 {
		t.Fatalf("expected the poison member isolated behind the reused prefix, got kept=%d poisoned=%d", len(kept), len(poisoned))
	}

	// Every member poisoned: an empty request, an empty kept, all attributed.
	request, kept, poisoned = client.buildBatchIsolating([]Event{poison}, batchRequest{})
	if len(request.Events) != 0 || len(kept) != 0 || len(poisoned) != 1 {
		t.Fatalf("expected the all-poison batch fully attributed, got request=%d kept=%d poisoned=%d",
			len(request.Events), len(kept), len(poisoned))
	}

	// Nothing poisoned: kept is the input, untouched, and no attribution.
	events := []Event{ok1, ok2}
	request, kept, poisoned = client.buildBatchIsolating(events, batchRequest{})
	if len(request.Events) != 2 || len(kept) != 2 || poisoned != nil {
		t.Fatalf("expected the clean batch built whole, got request=%d kept=%d poisoned=%v",
			len(request.Events), len(kept), poisoned)
	}
}
