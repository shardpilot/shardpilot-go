package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		_, _ = w.Write([]byte(fmt.Sprintf(`{"accepted":%d,"rejected":0,"duplicates":0}`, len(request.Events))))
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

func TestEnqueueRejectsInvalidEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":0,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	defer client.Close(context.Background())

	if err := client.Enqueue(Event{Name: "   "}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
	if stats := client.Snapshot(); stats.Enqueued != 0 {
		t.Fatalf("expected invalid enqueue not to increment Enqueued, got %d", stats.Enqueued)
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

func TestEnqueueIsNotBlockedBySlowTrackPublish(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var started atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if started.CompareAndSwap(false, true) {
			close(requestStarted)
			<-releaseRequest
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	defer client.Close(context.Background())

	trackDone := make(chan error, 1)
	go func() {
		trackDone <- client.Track(context.Background(), Event{Name: "slow_track"})
	}()
	<-requestStarted

	enqueueDone := make(chan error, 1)
	go func() {
		enqueueDone <- client.Enqueue(Event{Name: "queued_while_track_publishes"})
	}()

	select {
	case err := <-enqueueDone:
		if err != nil {
			t.Fatalf("Enqueue returned error: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Enqueue blocked behind slow Track network publish")
	}

	close(releaseRequest)
	if err := <-trackDone; err != nil {
		t.Fatalf("Track returned error: %v", err)
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

func TestCloseWithCanceledContextReturnsWhileTrackInFlight(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := client.Close(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error while Track is in-flight, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("Close waited too long for in-flight Track after context deadline: %s", elapsed)
	}
	if err := client.Enqueue(Event{Name: "after_close_start"}); err != ErrClosed {
		t.Fatalf("expected Enqueue after Close start to return ErrClosed, got %v", err)
	}

	close(releaseRequest)
	if err := <-trackDone; err != nil {
		t.Fatalf("Track returned error: %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("second Close with valid context returned error: %v", err)
	}
}

func TestCloseWithCanceledContextDoesNotPublishAfterDeadline(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	if err := client.Enqueue(Event{Name: "queued_before_expired_close"}); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled Close error, got %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("expected no publish after Close context cancellation, got %d requests", got)
	}
}

func TestRepeatedCloseReturnsFirstFlushFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`temporary failure body that must not leak`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	if err := client.Enqueue(Event{Name: "queued_before_failed_close"}); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	firstErr := client.Close(context.Background())
	if firstErr == nil {
		t.Fatal("expected first Close to return flush failure")
	}
	secondErr := client.Close(context.Background())
	if secondErr == nil {
		t.Fatal("expected repeated Close to return first failure")
	}
	if secondErr.Error() != firstErr.Error() {
		t.Fatalf("expected repeated Close error %q, got %q", firstErr.Error(), secondErr.Error())
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

	var request batchRequest
	select {
	case request = <-requests:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for later valid event publish")
	}
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

func TestInvalidQueuedBuildErrorDoesNotBlockLaterValidEvents(t *testing.T) {
	transport := &sequenceTransport{}
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

	if !client.queue.enqueue(Event{ID: "evt-invalid", Name: " "}) {
		t.Fatal("expected invalid internal enqueue to succeed")
	}
	if !client.queue.enqueue(Event{ID: "evt-valid", Name: "valid"}) {
		t.Fatal("expected valid internal enqueue to succeed")
	}

	batch, err := client.flushAvailable(context.Background(), nil)
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent, got %v", err)
	}
	if len(batch) != 0 {
		t.Fatalf("expected invalid batch to be discarded, got %+v", batch)
	}
	if transport.calls != 1 {
		t.Fatalf("expected later valid event to publish, got %d publish calls", transport.calls)
	}
	if got := transport.requestEventNames(); strings.Join(got, ",") != "valid" {
		t.Fatalf("unexpected published events %v", got)
	}
	if stats := client.Snapshot(); stats.FailedBatches != 1 || stats.Published != 1 || stats.Accepted != 1 {
		t.Fatalf("unexpected stats after invalid and valid flush: %+v", stats)
	}
}

func TestPermanentHTTPStatusDoesNotBlockLaterValidEvents(t *testing.T) {
	for _, statusCode := range []int{http.StatusBadRequest, http.StatusForbidden} {
		t.Run(fmt.Sprintf("status_%d", statusCode), func(t *testing.T) {
			transport := &sequenceTransport{firstErr: &HTTPStatusError{StatusCode: statusCode}}
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

			if !client.queue.enqueue(Event{ID: "evt-permanent", Name: "permanent_status"}) {
				t.Fatal("expected permanent-status enqueue to succeed")
			}
			if !client.queue.enqueue(Event{ID: "evt-valid", Name: "valid_after_permanent_status"}) {
				t.Fatal("expected valid enqueue to succeed")
			}

			batch, err := client.flushAvailable(context.Background(), nil)
			var statusErr *HTTPStatusError
			if !errors.As(err, &statusErr) || statusErr.StatusCode != statusCode {
				t.Fatalf("expected HTTPStatusError %d, got %v", statusCode, err)
			}
			if len(batch) != 0 {
				t.Fatalf("expected permanent status batch to be discarded, got %+v", batch)
			}
			if transport.calls != 2 {
				t.Fatalf("expected later valid event to publish after permanent status, got %d calls", transport.calls)
			}
			if got := transport.requestEventNames(); strings.Join(got, ",") != "permanent_status,valid_after_permanent_status" {
				t.Fatalf("unexpected published events %v", got)
			}
			if stats := client.Snapshot(); stats.FailedBatches != 1 || stats.Published != 1 || stats.Accepted != 1 || stats.Dropped != 1 {
				t.Fatalf("unexpected stats after permanent status and valid flush: %+v", stats)
			}
		})
	}
}

func TestPermanentEncodeErrorDoesNotBlockLaterValidEvents(t *testing.T) {
	requests := make(chan batchRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request batchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		requests <- request
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"accepted":%d,"rejected":0,"duplicates":0}`, len(request.Events))))
	}))
	defer server.Close()

	cfg := Config{
		IngestURL:     server.URL,
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		AppVersion:    "0.1.0",
		AppBuild:      "100",
		Platform:      "linux",
		BatchSize:     1,
		BufferSize:    4,
		FlushInterval: time.Hour,
		HTTPTimeout:   time.Second,
	}
	client := &Client{
		cfg:       cfg,
		clock:     realClock{},
		queue:     newBoundedQueue(4),
		transport: newHTTPTransport(cfg),
	}

	if err := client.Enqueue(Event{
		ID:    "evt-encode-failure",
		Name:  "encode_failure",
		Props: map[string]any{"bad": func() {}},
	}); err != nil {
		t.Fatalf("Enqueue invalid JSON event returned error: %v", err)
	}
	if err := client.Enqueue(Event{ID: "evt-valid", Name: "valid_after_encode_failure"}); err != nil {
		t.Fatalf("Enqueue valid event returned error: %v", err)
	}

	batch, err := client.flushAvailable(context.Background(), nil)
	var encodeErr *EncodeError
	if !errors.As(err, &encodeErr) {
		t.Fatalf("expected EncodeError, got %v", err)
	}
	if len(batch) != 0 {
		t.Fatalf("expected permanent encode failure not to retain a batch, got %+v", batch)
	}

	var request batchRequest
	select {
	case request = <-requests:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for later valid event publish")
	}
	if len(request.Events) != 1 || request.Events[0].EventName != "valid_after_encode_failure" {
		t.Fatalf("expected later valid event to publish, got %+v", request.Events)
	}
	if stats := client.Snapshot(); stats.FailedBatches != 1 || stats.Dropped != 1 || stats.Published != 1 || stats.Accepted != 1 {
		t.Fatalf("unexpected stats after encode failure and valid flush: %+v", stats)
	}
}

func TestFlushAvailableRetainsFailedBatchAndRetries(t *testing.T) {
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

	batch, err := client.flushAvailable(context.Background(), nil)
	if !errors.Is(err, firstErr) {
		t.Fatalf("expected first error, got %v", err)
	}
	if transport.calls != 1 {
		t.Fatalf("expected one publish attempt before retaining failed batch, got %d", transport.calls)
	}
	if len(batch) != 1 || batch[0].ID != "evt-1" {
		t.Fatalf("expected failed batch to be retained, got %+v", batch)
	}
	if len(client.queue.ch) != 1 {
		t.Fatalf("expected later queued event to remain queued, got queue size %d", len(client.queue.ch))
	}
	if stats := client.Snapshot(); stats.FailedBatches != 1 || stats.Published != 0 || stats.Accepted != 0 {
		t.Fatalf("unexpected stats after failed flush: %+v", stats)
	}

	batch, err = client.flushAvailable(context.Background(), batch)
	if err != nil {
		t.Fatalf("retry flush returned error: %v", err)
	}
	if len(batch) != 0 {
		t.Fatalf("expected retained batch to be cleared after retry, got %+v", batch)
	}
	if transport.calls != 3 {
		t.Fatalf("expected retained and queued batches to publish after retry, got %d calls", transport.calls)
	}
	if got := transport.requestEventNames(); strings.Join(got, ",") != "first,first,second" {
		t.Fatalf("unexpected publish order %v", got)
	}
	if stats := client.Snapshot(); stats.FailedBatches != 1 || stats.Published != 2 || stats.Accepted != 2 {
		t.Fatalf("unexpected stats after retry flush: %+v", stats)
	}
}

func TestRetryableHTTPStatusRetainsFailedBatch(t *testing.T) {
	for _, statusCode := range []int{http.StatusInternalServerError, http.StatusTooManyRequests} {
		t.Run(fmt.Sprintf("status_%d", statusCode), func(t *testing.T) {
			transport := &sequenceTransport{firstErr: &HTTPStatusError{StatusCode: statusCode}}
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
				queue:     newBoundedQueue(1),
				transport: transport,
			}

			if !client.queue.enqueue(Event{ID: "evt-retryable", Name: "retryable_status"}) {
				t.Fatal("expected retryable-status enqueue to succeed")
			}
			batch, err := client.flushAvailable(context.Background(), nil)
			var statusErr *HTTPStatusError
			if !errors.As(err, &statusErr) || statusErr.StatusCode != statusCode {
				t.Fatalf("expected HTTPStatusError %d, got %v", statusCode, err)
			}
			if len(batch) != 1 || batch[0].ID != "evt-retryable" {
				t.Fatalf("expected retryable status batch retained, got %+v", batch)
			}

			batch, err = client.flushAvailable(context.Background(), batch)
			if err != nil {
				t.Fatalf("retry flush returned error: %v", err)
			}
			if len(batch) != 0 {
				t.Fatalf("expected retryable status batch to clear after success, got %+v", batch)
			}
			if stats := client.Snapshot(); stats.FailedBatches != 1 || stats.Published != 1 || stats.Accepted != 1 {
				t.Fatalf("unexpected stats after retryable status retry: %+v", stats)
			}
		})
	}
}

func TestPublishWorkerBatchDropsPermanentEncodeFailure(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	cfg := Config{
		IngestURL:     server.URL,
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		BatchSize:     2,
		HTTPTimeout:   time.Second,
	}
	client := &Client{
		cfg:       cfg,
		clock:     realClock{},
		queue:     newBoundedQueue(1),
		transport: newHTTPTransport(cfg),
	}

	retained := client.publishWorkerBatch([]Event{{
		ID:      "evt-encode-failure",
		Name:    "encode_failure",
		Context: map[string]any{"bad": func() {}},
	}})
	if len(retained) != 0 {
		t.Fatalf("expected worker to drop permanent encode failure, got %+v", retained)
	}
	if stats := client.Snapshot(); stats.FailedBatches != 1 || stats.Dropped != 1 {
		t.Fatalf("unexpected stats after worker encode failure: %+v", stats)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("expected no HTTP request for encode failure, got %d", got)
	}
}

func TestPublishWorkerBatchRetainsFailedBatch(t *testing.T) {
	firstErr := errors.New("first batch failed")
	transport := &sequenceTransport{firstErr: firstErr}
	client := &Client{
		cfg: Config{
			WorkspaceID:   "workspace-test",
			AppID:         "app-test",
			EnvironmentID: "develop",
			Source:        SourceBackend,
			BatchSize:     2,
			HTTPTimeout:   time.Second,
		},
		clock:     realClock{},
		queue:     newBoundedQueue(2),
		transport: transport,
	}

	batch := []Event{{ID: "evt-1", Name: "first"}}
	retained := client.publishWorkerBatch(batch)
	if len(retained) != 1 || retained[0].ID != "evt-1" {
		t.Fatalf("expected worker to retain failed batch, got %+v", retained)
	}

	retained = client.publishWorkerBatch(retained)
	if len(retained) != 0 {
		t.Fatalf("expected successful retry to clear batch, got %+v", retained)
	}
}

func TestSourceCompatibilityBaselineAndCIMatrix(t *testing.T) {
	goMod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	if !strings.Contains(string(goMod), "\ngo 1.24\n") {
		t.Fatalf("go.mod must keep Go 1.24 source-compatibility baseline:\n%s", string(goMod))
	}

	workflow, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("read CI workflow: %v", err)
	}
	for _, version := range []string{"'1.24.x'", "'1.26.4'"} {
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
	requests []batchRequest
}

func (t *sequenceTransport) Publish(ctx context.Context, request batchRequest) (batchResult, error) {
	t.calls++
	t.requests = append(t.requests, request)
	if t.calls == 1 && t.firstErr != nil {
		return batchResult{}, t.firstErr
	}
	return batchResult{Accepted: len(request.Events)}, nil
}

func (t *sequenceTransport) requestEventNames() []string {
	names := make([]string, 0)
	for _, request := range t.requests {
		for _, event := range request.Events {
			names = append(names, event.EventName)
		}
	}
	return names
}
