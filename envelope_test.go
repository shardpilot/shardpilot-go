package shardpilot

import (
	"context"
	"encoding/json"
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
		Name:            "session_start",
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
