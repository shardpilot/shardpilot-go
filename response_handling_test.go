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
	"time"
)

func TestTransportParsesErrorEnvelopeAndRetryAfterHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"ingest rate limit exceeded",` +
			`"details":[{"field":"events","code":"events_rate_limited","message":"event rate limit exceeded for the current window"}]}}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	defer client.Close(context.Background())

	err := client.Track(context.Background(), Event{Name: "rate_limited_event"})
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %v", err)
	}
	if statusErr.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected status 429, got %d", statusErr.StatusCode)
	}
	if statusErr.ErrorCode != "rate_limited" {
		t.Fatalf("expected error code rate_limited, got %q", statusErr.ErrorCode)
	}
	if statusErr.ErrorMessage != "ingest rate limit exceeded" {
		t.Fatalf("unexpected error message %q", statusErr.ErrorMessage)
	}
	if len(statusErr.Details) != 1 || statusErr.Details[0].Field != "events" || statusErr.Details[0].Code != "events_rate_limited" {
		t.Fatalf("unexpected details %+v", statusErr.Details)
	}
	if statusErr.RetryAfter != 7*time.Second {
		t.Fatalf("expected RetryAfter 7s, got %v", statusErr.RetryAfter)
	}
	if !statusErr.Retryable() {
		t.Fatal("expected a 429 to stay retryable")
	}
	message := statusErr.Error()
	if !strings.Contains(message, "status 429") ||
		!strings.Contains(message, "(rate_limited)") ||
		!strings.Contains(message, "events:events_rate_limited") {
		t.Fatalf("expected enriched error message, got %q", message)
	}
}

func TestTransportDegradesToBareStatusOnMalformedErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("not json at all"))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	defer client.Close(context.Background())

	err := client.Track(context.Background(), Event{Name: "malformed_error_body"})
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %v", err)
	}
	if statusErr.ErrorCode != "" || statusErr.Details != nil || statusErr.RetryAfter != 0 {
		t.Fatalf("expected bare status error, got %+v", statusErr)
	}
	if statusErr.Error() != "shardpilot ingest returned status 400" {
		t.Fatalf("unexpected message %q", statusErr.Error())
	}
}

func TestErrorMessageCapsDetailCodes(t *testing.T) {
	details := make([]ErrorDetail, 0, maxErrorDetailCodes+2)
	for i := 0; i < maxErrorDetailCodes+2; i++ {
		details = append(details, ErrorDetail{Field: fmt.Sprintf("events[%d].event_id", i), Code: "required"})
	}
	statusErr := &HTTPStatusError{StatusCode: 400, ErrorCode: "validation_error", Details: details}
	message := statusErr.Error()
	if !strings.HasSuffix(message, ",...]") {
		t.Fatalf("expected capped detail list, got %q", message)
	}
	if got := strings.Count(message, "required"); got != maxErrorDetailCodes {
		t.Fatalf("expected %d folded detail codes, got %d in %q", maxErrorDetailCodes, got, message)
	}
}

func TestParseRetryAfter(t *testing.T) {
	cases := []struct {
		header string
		want   time.Duration
	}{
		{"", 0},
		{"7", 7 * time.Second},
		{" 10 ", 10 * time.Second},
		{"0", 0},
		{"-3", 0},
		{"abc", 0},
		{"Wed, 21 Oct 2026 07:28:00 GMT", 0},
		{"999999", maxRetryAfter},
		// Parseable but beyond the int64 nanosecond range: the clamp must
		// compare raw seconds, or the duration conversion would overflow.
		{"99999999999", maxRetryAfter},
		{"9223372036854775807", maxRetryAfter},
	}
	for _, c := range cases {
		if got := parseRetryAfter(c.header); got != c.want {
			t.Fatalf("parseRetryAfter(%q) = %v, want %v", c.header, got, c.want)
		}
	}
}

func TestApplyRetryAfterArmsAndClearsDeadline(t *testing.T) {
	clock := &stubClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	client := &Client{clock: clock}

	var deferUntil time.Time
	client.applyRetryAfter(&HTTPStatusError{StatusCode: 429, RetryAfter: 7 * time.Second}, &deferUntil)
	if want := clock.now.Add(7 * time.Second); !deferUntil.Equal(want) {
		t.Fatalf("expected deadline %v, got %v", want, deferUntil)
	}
	if !client.publishDeferred(deferUntil) {
		t.Fatal("expected publishes to be deferred before the deadline")
	}

	// A shorter hint never shortens an already later deadline.
	client.applyRetryAfter(&HTTPStatusError{StatusCode: 429, RetryAfter: time.Second}, &deferUntil)
	if want := clock.now.Add(7 * time.Second); !deferUntil.Equal(want) {
		t.Fatalf("expected deadline to stay %v, got %v", want, deferUntil)
	}

	// A non-retryable status never arms the deferral.
	var fresh time.Time
	client.applyRetryAfter(&HTTPStatusError{StatusCode: 400, RetryAfter: 7 * time.Second}, &fresh)
	if !fresh.IsZero() {
		t.Fatalf("expected 400 to leave the deadline unset, got %v", fresh)
	}

	// Success clears it.
	client.applyRetryAfter(nil, &deferUntil)
	if !deferUntil.IsZero() {
		t.Fatalf("expected success to clear the deadline, got %v", deferUntil)
	}

	clock.now = clock.now.Add(time.Minute)
	if client.publishDeferred(deferUntil) {
		t.Fatal("expected a cleared deadline to never defer")
	}
}

func TestRetriesReuseEventIDAndTimestamp(t *testing.T) {
	transport := &sequenceTransport{firstErr: &HTTPStatusError{StatusCode: http.StatusInternalServerError}}
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

	// No caller-supplied ID or timestamp: both are stamped once at intake so
	// the retry re-sends the identical envelope and the ingest service can
	// fold it as a duplicate of the first attempt.
	if err := client.Enqueue(Event{Name: "retry_reuses_identity"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var consentEpoch uint64
	batch, err := client.flushAvailable(context.Background(), nil, &consentEpoch)
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected transient 500 on the first attempt, got %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected the batch to stay retained after a transient failure, got %d events", len(batch))
	}

	batch, err = client.flushAvailable(context.Background(), batch, &consentEpoch)
	if err != nil || len(batch) != 0 {
		t.Fatalf("expected the retry to succeed and clear the batch, got err=%v len=%d", err, len(batch))
	}

	if len(transport.requests) != 2 || len(transport.requests[0].Events) != 1 || len(transport.requests[1].Events) != 1 {
		t.Fatalf("expected two single-event attempts, got %+v", transport.requests)
	}
	first := transport.requests[0].Events[0]
	second := transport.requests[1].Events[0]
	if first.EventID == "" || first.EventID != second.EventID {
		t.Fatalf("expected the retry to reuse the event id, got %q then %q", first.EventID, second.EventID)
	}
	if first.EventTS == "" || first.EventTS != second.EventTS {
		t.Fatalf("expected the retry to reuse the event timestamp, got %q then %q", first.EventTS, second.EventTS)
	}
}

func TestWorkerHoldsAutomaticPublishesUntilRetryAfter(t *testing.T) {
	var calls atomic.Int64
	var firstAttempt, secondAttempt atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if call == 1 {
			firstAttempt.Store(time.Now().UnixMilli())
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"ingest rate limit exceeded"}}`))
			return
		}
		if call == 2 {
			secondAttempt.Store(time.Now().UnixMilli())
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "token-value",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		FlushInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	if err := client.Enqueue(Event{Name: "deferred_event"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	waitFor(t, 2*time.Second, "first publish attempt", func() bool { return calls.Load() >= 1 })

	// Well inside the 1s Retry-After window the 20ms flush ticks must not
	// have produced another attempt.
	time.Sleep(300 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("expected automatic publishes to hold during Retry-After, got %d attempts", calls.Load())
	}

	waitFor(t, 3*time.Second, "post-deferral retry", func() bool { return calls.Load() >= 2 })
	if gap := secondAttempt.Load() - firstAttempt.Load(); gap < 800 {
		t.Fatalf("expected the retry to wait out the Retry-After hint, got %dms", gap)
	}
}

func TestExplicitFlushBypassesRetryAfterDeferral(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"ingest rate limit exceeded"}}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "token-value",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		FlushInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	if err := client.Enqueue(Event{Name: "flush_bypasses_deferral"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	waitFor(t, 2*time.Second, "first publish attempt", func() bool { return calls.Load() >= 1 })

	// The worker is parked behind a 60s Retry-After; an explicit Flush
	// carries caller intent and publishes now.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("explicit flush: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected the explicit flush to publish immediately, got %d attempts", calls.Load())
	}
}

type stubClock struct {
	now time.Time
}

func (c *stubClock) Now() time.Time {
	return c.now
}

func waitFor(t *testing.T, timeout time.Duration, what string, done func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if done() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-ticker.C:
		}
	}
}
