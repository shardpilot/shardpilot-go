package shardpilot

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// statusForEventName maps a test event name to the per-event outcome the fake
// ingest server reports for it, so tests can assert outcomes by event_id
// regardless of batch ordering.
func statusForEventName(name string) (status, code, message string) {
	switch {
	case strings.Contains(name, "observed"):
		return "observed", "event_not_registered", ""
	case strings.Contains(name, "duplicate"):
		return "duplicate", "duplicate_event_id", ""
	case strings.Contains(name, "suppressed_ad"):
		return "suppressed_ad_revenue_consent", "", ""
	case strings.Contains(name, "suppressed"):
		return "suppressed_no_consent", "", ""
	case strings.Contains(name, "rejected"):
		return "rejected", "validation_error", "props.amount must be a number"
	case strings.Contains(name, "unknown"):
		return "quarantined", "held_for_review", "" // not a known EventStatus
	default:
		return "accepted", "", ""
	}
}

// perEventStatusServer is an httptest server that decodes the batch request
// and returns a 202 with one BatchEventStatus per event (status derived from
// the event name) plus matching aggregate counts.
func perEventStatusServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request batchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		type wireEvent struct {
			EventID string `json:"event_id"`
			Status  string `json:"status"`
			Code    string `json:"code,omitempty"`
			Message string `json:"message,omitempty"`
		}
		var (
			events                         []wireEvent
			accepted, rejected, duplicates int
		)
		for _, envelope := range request.Events {
			status, code, message := statusForEventName(envelope.EventName)
			events = append(events, wireEvent{EventID: envelope.EventID, Status: status, Code: code, Message: message})
			switch status {
			case "accepted":
				accepted++
			case "rejected":
				rejected++
			case "duplicate":
				duplicates++
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"accepted":   accepted,
			"rejected":   rejected,
			"duplicates": duplicates,
			"events":     events,
		})
	}))
}

type batchResultRecorder struct {
	mu      sync.Mutex
	results []BatchResult
}

func (r *batchResultRecorder) record(result BatchResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, result)
}

func (r *batchResultRecorder) all() []BatchResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]BatchResult, len(r.results))
	copy(out, r.results)
	return out
}

func newBatchResultClient(t *testing.T, ingestURL string, onResult func(BatchResult)) *Client {
	t.Helper()
	client, err := NewClient(Config{
		IngestURL:     ingestURL,
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		BatchSize:     10,
		BufferSize:    32,
		FlushInterval: time.Hour,
		HTTPTimeout:   time.Second,
		OnBatchResult: onResult,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func TestOnBatchResultSurfacesPerEventStatuses(t *testing.T) {
	server := perEventStatusServer(t)
	defer server.Close()

	recorder := &batchResultRecorder{}
	client := newBatchResultClient(t, server.URL, recorder.record)
	defer client.Close(context.Background())

	names := []string{"ev_accepted", "ev_observed", "ev_duplicate", "ev_suppressed", "ev_suppressed_ad", "ev_rejected"}
	for i, name := range names {
		if err := client.Enqueue(Event{ID: "id-" + name, Name: name}); err != nil {
			t.Fatalf("enqueue %d (%s): %v", i, name, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Flush may publish in more than one batch (the worker can pre-pull some
	// events into its local batch before the flush drains the rest), so merge
	// the per-event statuses across every OnBatchResult call. The invariant is
	// that the union covers all events exactly once.
	results := recorder.all()
	if len(results) == 0 {
		t.Fatal("expected at least one OnBatchResult call")
	}
	byID := make(map[string]BatchEventStatus)
	for _, result := range results {
		for _, event := range result.Events {
			if _, dup := byID[event.EventID]; dup {
				t.Fatalf("event %s reported in more than one batch result", event.EventID)
			}
			byID[event.EventID] = event
		}
	}
	if len(byID) != len(names) {
		t.Fatalf("expected %d per-event statuses across all calls, got %d", len(names), len(byID))
	}

	want := map[string]struct {
		status EventStatus
		code   string
	}{
		"id-ev_accepted":      {EventStatusAccepted, ""},
		"id-ev_observed":      {EventStatusObserved, "event_not_registered"},
		"id-ev_duplicate":     {EventStatusDuplicate, "duplicate_event_id"},
		"id-ev_suppressed":    {EventStatusSuppressedNoConsent, ""},
		"id-ev_suppressed_ad": {EventStatusSuppressedAdRevenueConsent, ""},
		"id-ev_rejected":      {EventStatusRejected, "validation_error"},
	}
	for id, expect := range want {
		got, ok := byID[id]
		if !ok {
			t.Fatalf("missing per-event status for %s", id)
		}
		if got.Status != expect.status {
			t.Fatalf("%s: expected status %q, got %q", id, expect.status, got.Status)
		}
		if got.Code != expect.code {
			t.Fatalf("%s: expected code %q, got %q", id, expect.code, got.Code)
		}
	}
	if rejected := byID["id-ev_rejected"]; rejected.Message == "" {
		t.Fatal("expected rejected event to carry a message")
	}

	stats := client.Snapshot()
	if stats.Accepted != 1 || stats.Rejected != 1 || stats.Duplicates != 1 {
		t.Fatalf("unexpected aggregate stats: accepted=%d rejected=%d duplicates=%d",
			stats.Accepted, stats.Rejected, stats.Duplicates)
	}
	wantByStatus := map[EventStatus]uint64{
		EventStatusAccepted:                   1,
		EventStatusObserved:                   1,
		EventStatusDuplicate:                  1,
		EventStatusSuppressedNoConsent:        1,
		EventStatusSuppressedAdRevenueConsent: 1,
		EventStatusRejected:                   1,
	}
	for status, count := range wantByStatus {
		if got := stats.ByStatus[status]; got != count {
			t.Fatalf("ByStatus[%s]: expected %d, got %d", status, count, got)
		}
	}
}

func TestOnBatchResultOnSynchronousTrack(t *testing.T) {
	server := perEventStatusServer(t)
	defer server.Close()

	recorder := &batchResultRecorder{}
	client := newBatchResultClient(t, server.URL, recorder.record)
	defer client.Close(context.Background())

	if err := client.Track(context.Background(), Event{ID: "id-ev_suppressed", Name: "ev_suppressed"}); err != nil {
		t.Fatalf("Track: %v", err)
	}

	results := recorder.all()
	if len(results) != 1 {
		t.Fatalf("expected one OnBatchResult call from Track, got %d", len(results))
	}
	if len(results[0].Events) != 1 {
		t.Fatalf("expected one per-event status, got %d", len(results[0].Events))
	}
	if status := results[0].Events[0].Status; status != EventStatusSuppressedNoConsent {
		t.Fatalf("expected suppressed_no_consent, got %q", status)
	}
	if got := client.Snapshot().ByStatus[EventStatusSuppressedNoConsent]; got != 1 {
		t.Fatalf("expected ByStatus suppressed_no_consent 1, got %d", got)
	}
}

func TestByStatusFoldsWithoutCallback(t *testing.T) {
	server := perEventStatusServer(t)
	defer server.Close()

	client := newBatchResultClient(t, server.URL, nil)
	defer client.Close(context.Background())

	for _, name := range []string{"ev_observed", "ev_observed", "ev_accepted"} {
		if err := client.Enqueue(Event{Name: name}); err != nil {
			t.Fatalf("enqueue %s: %v", name, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	stats := client.Snapshot()
	if got := stats.ByStatus[EventStatusObserved]; got != 2 {
		t.Fatalf("expected ByStatus observed 2, got %d", got)
	}
	if got := stats.ByStatus[EventStatusAccepted]; got != 1 {
		t.Fatalf("expected ByStatus accepted 1, got %d", got)
	}
}

func TestUnknownEventStatusCarriedThrough(t *testing.T) {
	server := perEventStatusServer(t)
	defer server.Close()

	recorder := &batchResultRecorder{}
	client := newBatchResultClient(t, server.URL, recorder.record)
	defer client.Close(context.Background())

	if err := client.Track(context.Background(), Event{ID: "id-unknown", Name: "ev_unknown_status"}); err != nil {
		t.Fatalf("Track: %v", err)
	}

	results := recorder.all()
	if len(results) != 1 || len(results[0].Events) != 1 {
		t.Fatalf("expected one event in one result, got %#v", results)
	}
	got := results[0].Events[0]
	if got.Status != EventStatus("quarantined") {
		t.Fatalf("expected unknown status carried through as %q, got %q", "quarantined", got.Status)
	}
	if got := client.Snapshot().ByStatus[EventStatus("quarantined")]; got != 1 {
		t.Fatalf("expected ByStatus[quarantined] 1, got %d", got)
	}
}

func TestOnBatchResultPanicDoesNotStopDelivery(t *testing.T) {
	var received int
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request batchRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		mu.Lock()
		received += len(request.Events)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0,"events":[{"event_id":"x","status":"accepted"}]}`))
	}))
	defer server.Close()

	client := newBatchResultClient(t, server.URL, func(BatchResult) {
		panic("user callback blew up")
	})
	defer client.Close(context.Background())

	// Track publishes inline on the caller's goroutine; the second call
	// proves the panic in the first callback did not abort that path.
	if err := client.Track(context.Background(), Event{Name: "first"}); err != nil {
		t.Fatalf("first Track: %v", err)
	}
	if err := client.Track(context.Background(), Event{Name: "second"}); err != nil {
		t.Fatalf("second Track: %v", err)
	}

	mu.Lock()
	got := received
	mu.Unlock()
	if got != 2 {
		t.Fatalf("expected both events delivered despite callback panic, got %d", got)
	}
	if accepted := client.Snapshot().Accepted; accepted != 2 {
		t.Fatalf("expected accepted 2 after panic-guarded callbacks, got %d", accepted)
	}
}

func TestOnBatchResultPanicViaWorkerDoesNotStopDelivery(t *testing.T) {
	var received int
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request batchRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		mu.Lock()
		received += len(request.Events)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0,"events":[{"event_id":"x","status":"accepted"}]}`))
	}))
	defer server.Close()

	client := newBatchResultClient(t, server.URL, func(BatchResult) {
		panic("user callback blew up on the worker goroutine")
	})
	defer client.Close(context.Background())

	// Enqueue+Flush routes the publish (and the panicking callback) through the
	// background flush worker goroutine, which has no recover of its own. If the
	// panic were not recovered in notifyBatchResult the worker would die and the
	// SECOND Flush would observe a closed client (ErrClosed).
	if err := client.Enqueue(Event{Name: "worker_first"}); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
	defer cancel1()
	if err := client.Flush(ctx1); err != nil {
		t.Fatalf("first Flush: %v", err)
	}

	if err := client.Enqueue(Event{Name: "worker_second"}); err != nil {
		t.Fatalf("second Enqueue (worker should still be alive): %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := client.Flush(ctx2); err != nil {
		t.Fatalf("second Flush (worker should still be alive): %v", err)
	}

	mu.Lock()
	got := received
	mu.Unlock()
	if got != 2 {
		t.Fatalf("expected the worker to deliver both events despite a callback panic, got %d", got)
	}
}

func TestBatchResultWithoutPerEventList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request batchRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		// No "events" key: an older or minimal server returns aggregates only.
		_, _ = w.Write([]byte(fmt.Sprintf(`{"accepted":%d,"rejected":0,"duplicates":0}`, len(request.Events))))
	}))
	defer server.Close()

	recorder := &batchResultRecorder{}
	client := newBatchResultClient(t, server.URL, recorder.record)
	defer client.Close(context.Background())

	// ByStatus is nil until a response carrying a per-event list is recorded.
	if got := client.Snapshot().ByStatus; got != nil {
		t.Fatalf("expected ByStatus nil before any batch, got %#v", got)
	}

	for i := 0; i < 2; i++ {
		if err := client.Enqueue(Event{Name: "no_status_list"}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	for _, result := range recorder.all() {
		if result.Events != nil {
			t.Fatalf("expected nil Events when the response omits events[], got %#v", result.Events)
		}
	}
	stats := client.Snapshot()
	if stats.Accepted != 2 {
		t.Fatalf("expected aggregate accepted 2, got %d", stats.Accepted)
	}
	if stats.ByStatus != nil {
		t.Fatalf("expected ByStatus to stay nil when no per-event list is returned, got %#v", stats.ByStatus)
	}
}

func TestTransportDecodesPerEventStatuses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":1,"events":[` +
			`{"event_id":"a","status":"accepted"},` +
			`{"event_id":"b","status":"duplicate","code":"duplicate_event_id","message":"seen before"}` +
			`]}`))
	}))
	defer server.Close()

	tr := newHTTPTransport(Config{IngestURL: server.URL, Token: "test-token", HTTPTimeout: time.Second})
	result, err := tr.Publish(context.Background(), batchRequest{})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(result.Events) != 2 {
		t.Fatalf("expected 2 decoded events, got %d", len(result.Events))
	}
	if result.Events[1].EventID != "b" || result.Events[1].Status != "duplicate" ||
		result.Events[1].Code != "duplicate_event_id" || result.Events[1].Message != "seen before" {
		t.Fatalf("unexpected decoded event: %#v", result.Events[1])
	}

	public := result.toPublic()
	if len(public.Events) != 2 || public.Events[1].Status != EventStatusDuplicate {
		t.Fatalf("toPublic did not preserve per-event statuses: %#v", public.Events)
	}
}
