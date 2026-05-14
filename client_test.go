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
	if err := client.Track(context.Background(), Event{Name: "track_after_close"}); err != ErrClosed {
		t.Fatalf("expected ErrClosed from Track after Close, got %v", err)
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
	waitForClientClosed(t, client)

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

func TestCloseRacingWithTrackRejectsPostCloseEvent(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
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
	waitForClientClosed(t, client)

	trackDone := make(chan error, 1)
	go func() {
		trackDone <- client.Track(context.Background(), Event{Name: "racing_track"})
	}()

	client.lifecycleMu.Unlock()

	if err := <-trackDone; err != ErrClosed {
		t.Fatalf("expected racing Track after close start to return ErrClosed, got %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("expected no Track publish after close start, got %d requests", got)
	}
}

func TestCloseWaitsForInFlightTrack(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-releaseRequest
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)

	trackDone := make(chan error, 1)
	go func() {
		trackDone <- client.Track(context.Background(), Event{Name: "in_flight_track"})
	}()
	<-requestStarted

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- client.Close(context.Background())
	}()
	waitForClientClosed(t, client)

	select {
	case err := <-closeDone:
		t.Fatalf("Close completed before in-flight Track finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseRequest)

	if err := <-trackDone; err != nil {
		t.Fatalf("Track returned error: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

func TestEnqueueSnapshotsMutableEventMaps(t *testing.T) {
	requests := make(chan batchRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request batchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		requests <- request
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	defer client.Close(context.Background())

	props := map[string]any{"level": "before", "score": 10}
	eventContext := map[string]any{"surface": "menu", "online": true}
	if err := client.Enqueue(Event{
		ID:      "evt-mutable",
		Name:    "mutable_event",
		Props:   props,
		Context: eventContext,
	}); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	props["level"] = "after"
	props["score"] = 99
	props["new"] = "post-enqueue"
	eventContext["surface"] = "gameplay"
	eventContext["online"] = false
	eventContext["new"] = "post-enqueue"

	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	request := <-requests
	if len(request.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(request.Events))
	}
	event := request.Events[0]
	if event.Props["level"] != "before" || event.Props["score"] != float64(10) {
		t.Fatalf("props were not snapshotted before caller mutation: %+v", event.Props)
	}
	if _, exists := event.Props["new"]; exists {
		t.Fatalf("props include post-enqueue mutation: %+v", event.Props)
	}
	if event.Context["surface"] != "menu" || event.Context["online"] != true {
		t.Fatalf("context was not snapshotted before caller mutation: %+v", event.Context)
	}
	if _, exists := event.Context["new"]; exists {
		t.Fatalf("context includes post-enqueue mutation: %+v", event.Context)
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

func waitForClientClosed(t *testing.T, client *Client) {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		if client.closed.Load() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for client Close to start")
		case <-ticker.C:
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
