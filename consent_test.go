package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

func TestConsentDenyThenGrantDropsWorkerHeldBatch(t *testing.T) {
	var eventCount atomic.Int64
	consents := make(chan capturedConsent, 4)
	server := newConsentTestServer(t, &eventCount, consents)
	defer server.Close()

	// BatchSize 10 + 1h FlushInterval: the worker pulls enqueued events
	// into its local batch and then holds them without publishing.
	client := newConsentTestClient(t, server.URL, "", "anon-actor")
	defer client.Close(context.Background())

	for _, name := range []string{"purchase", "economy_tx", "match_end"} {
		if err := client.Enqueue(Event{Name: name}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// Wait until the worker has moved every event out of the shared queue
	// into its worker-local batch, so the denial below cannot reach them
	// via the queue drain.
	deadline := time.Now().Add(3 * time.Second)
	for len(client.queue.ch) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the worker to pull events into its local batch")
		}
		time.Sleep(time.Millisecond)
	}

	client.SetConsent(false)
	waitForConsent(t, consents)
	client.SetConsent(true)
	waitForConsent(t, consents)

	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := eventCount.Load(); got != 0 {
		t.Fatalf("worker-held events from the denied period must not publish after re-grant, got %d published", got)
	}
	if got := client.Snapshot().Dropped; got != 3 {
		t.Fatalf("expected 3 dropped worker-held events, got %d", got)
	}

	// The pipeline is open again: post-grant events publish normally.
	if err := client.Enqueue(Event{Name: "purchase"}); err != nil {
		t.Fatalf("Enqueue after grant: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after grant: %v", err)
	}
	if got := eventCount.Load(); got != 1 {
		t.Fatalf("expected exactly the post-grant event to publish, got %d", got)
	}
}

func TestCloseWaitsForPendingConsentPublish(t *testing.T) {
	consentStarted := make(chan struct{})
	releaseConsent := make(chan struct{})
	var consentStartedOnce, releaseOnce sync.Once
	var consentPosts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/consent":
			consentStartedOnce.Do(func() { close(consentStarted) })
			<-releaseConsent
			consentPosts.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"recorded":true,"replayed":false}`))
		case "/v1/events:batch":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"accepted":0,"rejected":0,"duplicates":0}`))
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	// Unblock the handler before server.Close (defers run LIFO) so a test
	// failure cannot deadlock the server shutdown.
	defer releaseOnce.Do(func() { close(releaseConsent) })

	client := newConsentTestClient(t, server.URL, "user-actor", "")

	client.SetConsent(true)
	select {
	case <-consentStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the consent POST to start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()

	// While the consent POST is held open, Close must keep waiting on the
	// sender instead of returning with the decision untransmitted.
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned (err=%v) while the recorded consent decision was still untransmitted", err)
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(releaseConsent) })
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Close after releasing the consent POST")
	}
	if got := consentPosts.Load(); got != 1 {
		t.Fatalf("expected the consent POST to complete before Close returned, got %d", got)
	}
}

func TestConsentDenialCancelsInFlightPublish(t *testing.T) {
	const batchSize = 3

	publishStarted := make(chan struct{})
	publishResolved := make(chan error, 1)
	var publishStartedOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/events:batch":
			// Drain the body first: the server only watches for a client
			// disconnect (cancelling r.Context()) once the request body is
			// consumed and its background read is active.
			_, _ = io.Copy(io.Discard, r.Body)
			publishStartedOnce.Do(func() { close(publishStarted) })
			// Hold the publish open until the SDK aborts the request (the
			// denial cancellation, surfacing as r.Context().Done()) or the
			// failure-mode timeout proves no abort happened. Both arms stay
			// well under the client's HTTPTimeout so a timeout cannot
			// masquerade as the denial abort.
			select {
			case <-r.Context().Done():
				publishResolved <- r.Context().Err()
			case <-time.After(3 * time.Second):
				publishResolved <- nil
			}
		case "/v1/consent":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"recorded":true,"replayed":false}`))
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		AnonymousID:   "anon-actor",
		BatchSize:     batchSize,
		BufferSize:    8,
		FlushInterval: time.Hour,
		HTTPTimeout:   10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	// Filling a whole batch makes the worker start a publish immediately;
	// the server then holds that publish in flight.
	for _, name := range []string{"purchase", "economy_tx", "match_end"} {
		if err := client.Enqueue(Event{Name: name}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	select {
	case <-publishStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the worker publish to reach the server")
	}

	client.SetConsent(false)

	select {
	case cause := <-publishResolved:
		if cause == nil {
			t.Fatal("consent denial did not abort the in-flight publish")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the in-flight publish to resolve")
	}

	// The aborted batch must be counted as Dropped (the worker reconciles
	// the denial epoch right after the failed publish), never as Published.
	deadline := time.Now().Add(3 * time.Second)
	for {
		stats := client.Snapshot()
		if stats.Dropped == batchSize {
			if stats.Published != 0 {
				t.Fatalf("expected no events published after mid-flight denial, got %d", stats.Published)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d dropped events after mid-flight denial, got %+v", batchSize, stats)
		}
		time.Sleep(time.Millisecond)
	}
}

// blockingCancelTransport blocks Publish until its publish context is
// cancelled AND the test releases it, then returns the wrapped cancellation
// error the real HTTP transport surfaces when a request is aborted. The
// two-phase block lets a test deterministically interleave consent flips
// between the cancellation and the transport's return.
type blockingCancelTransport struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingCancelTransport() *blockingCancelTransport {
	return &blockingCancelTransport{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (t *blockingCancelTransport) Publish(ctx context.Context, request batchRequest) (batchResult, error) {
	t.once.Do(func() { close(t.started) })
	<-ctx.Done()
	<-t.release
	return batchResult{}, fmt.Errorf("send shardpilot ingest request: %w", ctx.Err())
}

func (t *blockingCancelTransport) PublishConsent(ctx context.Context, request consentRequest) (consentResult, error) {
	return consentResult{Recorded: true}, nil
}

// newBlockingPublishClient builds a workerless client around a
// blockingCancelTransport, suitable for driving Track directly. No identity
// is configured, so SetConsent applies locally only and never spawns the
// consent sender.
func newBlockingPublishClient(transport *blockingCancelTransport) *Client {
	client := &Client{
		cfg: Config{
			WorkspaceID:   "workspace-test",
			AppID:         "app-test",
			EnvironmentID: "develop",
			Source:        SourceBackend,
			BatchSize:     1,
			HTTPTimeout:   10 * time.Second,
		},
		clock:     realClock{},
		queue:     newBoundedQueue(8),
		transport: transport,
	}
	client.consentGate.Store(newConsentGateState())
	return client
}

func TestConsentGateCancellationMapsToDeniedAfterQuickRegrant(t *testing.T) {
	transport := newBlockingCancelTransport()
	client := newBlockingPublishClient(transport)

	trackErr := make(chan error, 1)
	go func() {
		trackErr <- client.Track(context.Background(), Event{Name: "purchase"})
	}()
	select {
	case <-transport.started:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the publish to enter the transport")
	}

	// Deny while the publish is inside the transport (cancelling its gate),
	// then re-grant BEFORE the transport returns: consentDenied() is false by
	// the time the cancellation error surfaces, so only the held gate can
	// identify the abort as a denial.
	client.SetConsent(false)
	client.SetConsent(true)
	if state := client.Consent(); state != ConsentGranted {
		t.Fatalf("expected consent re-granted before the transport returned, got %q", state)
	}
	close(transport.release)

	select {
	case err := <-trackErr:
		if !errors.Is(err, ErrConsentDenied) {
			t.Fatalf("expected the gate-cancelled publish to surface ErrConsentDenied despite the re-grant, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Track to return")
	}

	// The aborted batch counts as Dropped — never as a failed batch and
	// never as Published (README: in-flight denial aborts count as Dropped).
	stats := client.Snapshot()
	if stats.Dropped != 1 {
		t.Fatalf("expected the aborted event to count as dropped, got %+v", stats)
	}
	if stats.FailedBatches != 0 {
		t.Fatalf("expected no failed batches for a denial abort, got %+v", stats)
	}
	if stats.Published != 0 {
		t.Fatalf("expected no published events after a mid-flight denial, got %+v", stats)
	}
}

func TestCallerContextCancellationIsNotMappedToConsentDenied(t *testing.T) {
	transport := newBlockingCancelTransport()
	client := newBlockingPublishClient(transport)

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	defer cancelCaller()
	trackErr := make(chan error, 1)
	go func() {
		trackErr <- client.Track(callerCtx, Event{Name: "purchase"})
	}()
	select {
	case <-transport.started:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the publish to enter the transport")
	}

	// The CALLER aborts the publish; the consent gate stays intact, so the
	// cancellation must surface as-is, not be reclassified as a denial.
	cancelCaller()
	close(transport.release)

	select {
	case err := <-trackErr:
		if errors.Is(err, ErrConsentDenied) {
			t.Fatalf("caller-context cancellation was misclassified as ErrConsentDenied: %v", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected the caller cancellation to surface, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Track to return")
	}

	stats := client.Snapshot()
	if stats.Dropped != 0 {
		t.Fatalf("expected no dropped events for a caller cancellation, got %+v", stats)
	}
	if stats.FailedBatches != 1 {
		t.Fatalf("expected the caller cancellation to count as a failed batch, got %+v", stats)
	}
}

func TestWorkerMidFlightDenialQuickRegrantCountsDroppedNotFailed(t *testing.T) {
	const batchSize = 3

	client, err := NewClient(Config{
		IngestURL:     "http://127.0.0.1:9", // never dialed: the transport is replaced below
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		BatchSize:     batchSize,
		BufferSize:    8,
		FlushInterval: time.Hour,
		HTTPTimeout:   10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	// Swap in the blocking transport before any event flows; the enqueue
	// channel hand-off orders this write before the worker's first publish.
	transport := newBlockingCancelTransport()
	client.transport = transport

	// Filling a whole batch makes the worker start a publish immediately;
	// the transport then holds that publish in flight.
	for _, name := range []string{"purchase", "economy_tx", "match_end"} {
		if err := client.Enqueue(Event{Name: name}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	select {
	case <-transport.started:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the worker publish to enter the transport")
	}

	// Deny (aborting the in-flight publish) and re-grant before the
	// transport returns the cancellation error.
	client.SetConsent(false)
	client.SetConsent(true)
	close(transport.release)

	// The aborted batch must be counted as Dropped immediately by the worker
	// (ErrConsentDenied is permanent), never as a failed batch.
	deadline := time.Now().Add(3 * time.Second)
	for {
		stats := client.Snapshot()
		if stats.Dropped == batchSize {
			if stats.FailedBatches != 0 {
				t.Fatalf("expected no failed batches for a denial abort under quick re-grant, got %+v", stats)
			}
			if stats.Published != 0 {
				t.Fatalf("expected no events published after a mid-flight denial, got %+v", stats)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d dropped events after the mid-flight denial, got %+v", batchSize, stats)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSetConsentDecisionsArriveInCallOrder(t *testing.T) {
	const flips = 12

	var mu sync.Mutex
	var arrived []bool
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/consent" {
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var body consentRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode consent request: %v", err)
		}
		// Hold every request briefly: concurrent unordered senders would
		// interleave here, while the single serialized sender stays in
		// strict call order.
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		arrived = append(arrived, body.Categories["analytics"])
		full := len(arrived) == flips
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"recorded":true,"replayed":false}`))
		if full {
			close(done)
		}
	}))
	defer server.Close()

	client := newConsentTestClient(t, server.URL, "user-actor", "")
	defer client.Close(context.Background())

	want := make([]bool, 0, flips)
	for i := 0; i < flips; i++ {
		granted := i%2 != 0 // deny, grant, deny, grant, ...
		client.SetConsent(granted)
		want = append(want, granted)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		mu.Lock()
		got := len(arrived)
		mu.Unlock()
		t.Fatalf("timed out waiting for %d consent POSTs, got %d", flips, got)
	}

	mu.Lock()
	defer mu.Unlock()
	for i, granted := range want {
		if arrived[i] != granted {
			t.Fatalf("consent decisions arrived out of call order: position %d got analytics=%v, want %v (full order %v, want %v)", i, arrived[i], granted, arrived, want)
		}
	}
}
