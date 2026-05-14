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
