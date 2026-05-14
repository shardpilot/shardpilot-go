package shardpilot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
}
