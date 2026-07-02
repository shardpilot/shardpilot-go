package shardpilot

import (
	"context"
	"encoding/json"
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
		header      string
		want        time.Duration
		wantPresent bool
	}{
		{"", 0, false},
		{"7", 7 * time.Second, true},
		{" 10 ", 10 * time.Second, true},
		// An explicit zero is a REAL hint ("retry now"), distinct from a
		// missing header; same for an already-elapsed HTTP-date.
		{"0", 0, true},
		{"Wed, 21 Oct 2015 07:28:00 GMT", 0, true},
		{"-3", 0, false},
		{"abc", 0, false},
		{"999999", maxRetryAfter, true},
		// Parseable but beyond the int64 nanosecond range: the clamp must
		// compare raw seconds, or the duration conversion would overflow.
		{"99999999999", maxRetryAfter, true},
		{"9223372036854775807", maxRetryAfter, true},
		// Too large even for int64 still means "wait a long time": clamp,
		// don't ignore. A hugely negative value stays malformed.
		{"999999999999999999999", maxRetryAfter, true},
		{"-999999999999999999999", 0, false},
	}
	for _, c := range cases {
		got, present := parseRetryAfter(c.header)
		if got != c.want || present != c.wantPresent {
			t.Fatalf("parseRetryAfter(%q) = (%v, %v), want (%v, %v)", c.header, got, present, c.want, c.wantPresent)
		}
	}
}

func TestParseRetryAfterHTTPDateForm(t *testing.T) {
	// A future HTTP-date defers by the distance from now (both standard
	// header forms are honored, like the crash client).
	when := time.Now().Add(10 * time.Minute).UTC().Format(http.TimeFormat)
	got, present := parseRetryAfter(when)
	if !present || got < 9*time.Minute || got > 10*time.Minute+time.Second {
		t.Fatalf("parseRetryAfter(%q) = (%v, %v), want ~10m present", when, got, present)
	}

	// A far-future date clamps to the 24h maximum.
	farFuture := time.Now().Add(90 * 24 * time.Hour).UTC().Format(http.TimeFormat)
	if got, present := parseRetryAfter(farFuture); !present || got != maxRetryAfter {
		t.Fatalf("parseRetryAfter(far future) = (%v, %v), want (%v, true)", got, present, maxRetryAfter)
	}
}

func TestDeferralWakeRetriesAtDeadlineNotNextTick(t *testing.T) {
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

	// The flush interval is far LONGER than the 1s Retry-After: the retry
	// must fire at the backpressure deadline (the dedicated wake), not at
	// the next flush tick minutes later.
	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "token-value",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		BatchSize:     1,
		FlushInterval: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	// BatchSize 1: the enqueue itself triggers the first automatic publish.
	if err := client.Enqueue(Event{Name: "deadline_wake"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	waitFor(t, 2*time.Second, "first publish attempt", func() bool { return calls.Load() >= 1 })
	waitFor(t, 5*time.Second, "the deadline-wake retry", func() bool { return calls.Load() >= 2 })
	gap := secondAttempt.Load() - firstAttempt.Load()
	if gap < 800 {
		t.Fatalf("expected the retry to wait out the Retry-After hint, got %dms", gap)
	}
	if gap > 4000 {
		t.Fatalf("expected the retry at the deadline, not the next flush tick, got %dms", gap)
	}
}

func TestApplyRetryAfterArmsAndClearsDeadline(t *testing.T) {
	clock := &stubClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	client := &Client{clock: clock}

	var deferUntil time.Time
	client.applyRetryAfter(&HTTPStatusError{StatusCode: 429, RetryAfter: 7 * time.Second, retryAfterPresent: true}, &deferUntil)
	if want := clock.now.Add(7 * time.Second); !deferUntil.Equal(want) {
		t.Fatalf("expected deadline %v, got %v", want, deferUntil)
	}
	if !client.publishDeferred(deferUntil) {
		t.Fatal("expected publishes to be deferred before the deadline")
	}

	// The server's LATEST word wins: a fresh shorter hint replaces an
	// earlier longer deadline.
	client.applyRetryAfter(&HTTPStatusError{StatusCode: 429, RetryAfter: time.Second, retryAfterPresent: true}, &deferUntil)
	if want := clock.now.Add(time.Second); !deferUntil.Equal(want) {
		t.Fatalf("expected the fresh hint to replace the deadline with %v, got %v", want, deferUntil)
	}

	// An explicit zero ("retry now") arms only the tiny anti-hot-loop floor.
	client.applyRetryAfter(&HTTPStatusError{StatusCode: 429, RetryAfter: 0, retryAfterPresent: true}, &deferUntil)
	if want := clock.now.Add(minRetryNowSpacing); !deferUntil.Equal(want) {
		t.Fatalf("expected a retry-now hint to arm the %v floor, got %v", minRetryNowSpacing, deferUntil)
	}

	// A retryable failure WITHOUT a usable header leaves the deadline alone.
	before := deferUntil
	client.applyRetryAfter(&HTTPStatusError{StatusCode: 500}, &deferUntil)
	if !deferUntil.Equal(before) {
		t.Fatalf("expected a hintless failure to leave the deadline at %v, got %v", before, deferUntil)
	}

	// A non-retryable status never arms the deferral.
	var fresh time.Time
	client.applyRetryAfter(&HTTPStatusError{StatusCode: 400, RetryAfter: 7 * time.Second, retryAfterPresent: true}, &fresh)
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

func TestRetryAfterZeroRetriesPromptly(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"ingest rate limit exceeded"}}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	// With a 10-minute flush interval, only an honored retry-now hint can
	// produce a prompt second attempt.
	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "token-value",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		BatchSize:     1,
		FlushInterval: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	if err := client.Enqueue(Event{Name: "retry_now"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 3*time.Second, "the retry-now attempt", func() bool { return calls.Load() >= 2 })
}

func TestFlushDroppingTheBatchClearsStaleDeferral(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			// Arm a long server deferral.
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"ingest rate limit exceeded"}}`))
		case 2:
			// The explicit flush turns the batch into a permanent drop.
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"code":"validation_error","message":"request validation failed"}}`))
		default:
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "token-value",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		BatchSize:     1,
		FlushInterval: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	if err := client.Enqueue(Event{Name: "deferred_then_dropped"}); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	waitFor(t, 2*time.Second, "the first (rate-limited) attempt", func() bool { return calls.Load() >= 1 })

	// The explicit flush bypasses the 1h deferral; the server now rejects
	// the batch permanently, so the flush drops it.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err == nil {
		t.Fatal("expected the flush to surface the permanent rejection")
	}

	// The batch the deferral protected is gone: a fresh event must publish
	// on the normal cadence, not be held behind the stale 1h deadline.
	if err := client.Enqueue(Event{Name: "after_drop"}); err != nil {
		t.Fatalf("enqueue second: %v", err)
	}
	waitFor(t, 3*time.Second, "the post-drop publish", func() bool { return calls.Load() >= 3 })
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

func TestEventArrivingDuringDeferralKeepsTheDeadlineRetry(t *testing.T) {
	var calls atomic.Int64
	var firstAttempt, secondAttempt atomic.Int64
	var secondAttemptEvents atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if call == 1 {
			firstAttempt.Store(time.Now().UnixMilli())
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"ingest rate limit exceeded"}}`))
			return
		}
		var request batchRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		if call == 2 {
			secondAttempt.Store(time.Now().UnixMilli())
			secondAttemptEvents.Store(int64(len(request.Events)))
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"accepted":%d,"rejected":0,"duplicates":0}`, len(request.Events))))
	}))
	defer server.Close()

	// A long flush interval: if the deadline wake were lost (e.g. to a queue
	// event racing the timer), the retry would not happen for 10 minutes.
	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "token-value",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		BatchSize:     2,
		FlushInterval: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	if err := client.Enqueue(Event{Name: "deferred_first"}); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	// The explicit flush publishes the under-sized batch and hits the 429;
	// the worker retains it on a 1s server hint.
	flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Flush(flushCtx); err == nil {
		t.Fatal("expected the first flush to surface the rate-limited failure")
	}

	// A fresh event arrives mid-deferral and fills the batch; the retry must
	// still fire at the backpressure deadline, carrying both events.
	if err := client.Enqueue(Event{Name: "deferred_second"}); err != nil {
		t.Fatalf("enqueue second: %v", err)
	}

	waitFor(t, 5*time.Second, "the deadline retry", func() bool { return calls.Load() >= 2 })
	gap := secondAttempt.Load() - firstAttempt.Load()
	if gap < 800 {
		t.Fatalf("expected the retry to wait out the Retry-After hint, got %dms", gap)
	}
	if gap > 4000 {
		t.Fatalf("expected the retry at the deadline, not the next flush tick, got %dms", gap)
	}
	if got := secondAttemptEvents.Load(); got != 2 {
		t.Fatalf("expected the deadline retry to carry both events, got %d", got)
	}
}

func TestDeadlineRetryFailureWithoutHintFallsBackToTickCadence(t *testing.T) {
	var calls atomic.Int64
	var attempts [3]atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if call <= 3 {
			attempts[call-1].Store(time.Now().UnixMilli())
		}
		switch call {
		case 1:
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"ingest rate limit exceeded"}}`))
		case 2:
			// Retryable, but NO fresh hint: the stale, already-elapsed
			// deadline must not trigger an immediate back-to-back retry.
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"code":"internal_error","message":"internal server error"}}`))
		default:
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "token-value",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		BatchSize:     1,
		FlushInterval: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	if err := client.Enqueue(Event{Name: "cadence_after_deadline"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	waitFor(t, 10*time.Second, "the third attempt", func() bool { return calls.Load() >= 3 })
	gap12 := attempts[1].Load() - attempts[0].Load()
	gap23 := attempts[2].Load() - attempts[1].Load()
	if gap12 < 800 {
		t.Fatalf("expected the second attempt at the Retry-After deadline, got %dms", gap12)
	}
	if gap23 < 150 {
		t.Fatalf("expected the post-deadline failure to fall back to the tick cadence, got a %dms back-to-back retry", gap23)
	}
	if gap23 > 3000 {
		t.Fatalf("expected the tick-cadence retry within a flush interval or two, got %dms", gap23)
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
