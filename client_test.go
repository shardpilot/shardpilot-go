package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFlushPublishesQueuedEvents(t *testing.T) {
	var received atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request batchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		received.Add(int64(len(request.Events)))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":2,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	defer client.Close(context.Background())

	if err := client.Enqueue(Event{ID: "evt-1", Name: "queued_one"}); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if err := client.Enqueue(Event{ID: "evt-2", Name: "queued_two"}); err != nil {
		t.Fatalf("enqueue second: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	if received.Load() != 2 {
		t.Fatalf("expected 2 events, got %d", received.Load())
	}
	stats := client.Snapshot()
	if stats.Enqueued != 2 {
		t.Fatalf("expected enqueued 2, got %d", stats.Enqueued)
	}
	if stats.Accepted != 2 {
		t.Fatalf("expected accepted 2, got %d", stats.Accepted)
	}
}

func TestCloseRejectsNewEvents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":0,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := client.Enqueue(Event{Name: "after_close"}); err != ErrClosed {
		t.Fatalf("expected ErrClosed after Close, got %v", err)
	}
	if stats := client.Snapshot(); stats.Enqueued != 0 {
		t.Fatalf("expected closed enqueue not to increment Enqueued, got %d", stats.Enqueued)
	}
}

func TestCloseRacingWithEnqueueRejectsPostCloseEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":0,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	client.lifecycleMu.Lock()

	closeStarted := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeDone <- client.Close(context.Background())
	}()
	<-closeStarted

	enqueueDone := make(chan error, 1)
	go func() {
		enqueueDone <- client.Enqueue(Event{Name: "racing_event"})
	}()

	client.lifecycleMu.Unlock()

	if err := <-enqueueDone; err != ErrClosed {
		t.Fatalf("expected racing enqueue after close start to return ErrClosed, got %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if stats := client.Snapshot(); stats.Enqueued != 0 {
		t.Fatalf("expected racing closed enqueue not to increment Enqueued, got %d", stats.Enqueued)
	}
}

func TestFlushAvailableAttemptsAllQueuedBatchesAfterFirstError(t *testing.T) {
	firstErr := errors.New("first batch failed")
	transport := &sequenceTransport{firstErr: firstErr}
	client := &Client{
		cfg: Config{
			WorkspaceID:   "workspace-test",
			AppID:         "app-test",
			EnvironmentID: "develop",
			Source:        SourceBackend,
			BatchSize:     1,
			HTTPTimeout:   time.Second,
		},
		clock:     realClock{},
		queue:     newBoundedQueue(2),
		transport: transport,
	}

	if !client.queue.enqueue(Event{ID: "evt-1", Name: "first"}) {
		t.Fatal("expected first enqueue to succeed")
	}
	if !client.queue.enqueue(Event{ID: "evt-2", Name: "second"}) {
		t.Fatal("expected second enqueue to succeed")
	}

	_, err := client.flushAvailable(context.Background(), nil)
	if !errors.Is(err, firstErr) {
		t.Fatalf("expected first error, got %v", err)
	}
	if transport.calls != 2 {
		t.Fatalf("expected 2 publish attempts, got %d", transport.calls)
	}
	if stats := client.Snapshot(); stats.FailedBatches != 1 || stats.Published != 1 || stats.Accepted != 1 {
		t.Fatalf("unexpected stats after partial flush: %+v", stats)
	}
}

func TestSourceCompatibilityBaselineAndCIMatrix(t *testing.T) {
	goMod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	if !strings.Contains(string(goMod), "\ngo 1.23\n") {
		t.Fatalf("go.mod must keep Go 1.23 source-compatibility baseline:\n%s", string(goMod))
	}

	workflow, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("read CI workflow: %v", err)
	}
	for _, version := range []string{"'1.23.x'", "'1.26.3'"} {
		if !strings.Contains(string(workflow), version) {
			t.Fatalf("CI workflow missing Go matrix version %s", version)
		}
	}
}

type sequenceTransport struct {
	firstErr error
	calls    int
}

func (t *sequenceTransport) Publish(ctx context.Context, request batchRequest) (batchResult, error) {
	t.calls++
	if t.calls == 1 {
		return batchResult{}, t.firstErr
	}
	return batchResult{Accepted: len(request.Events)}, nil
}
