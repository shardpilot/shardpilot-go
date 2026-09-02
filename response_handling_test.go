package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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

func TestApplyRetryPacingArmsAndClearsDeadline(t *testing.T) {
	clock := &stubClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	client := &Client{clock: clock}

	var deferUntil time.Time
	attempt := 0
	client.applyRetryPacing(&HTTPStatusError{StatusCode: 429, RetryAfter: 7 * time.Second, retryAfterPresent: true}, &deferUntil, &attempt)
	if want := clock.now.Add(7 * time.Second); !deferUntil.Equal(want) {
		t.Fatalf("expected deadline %v, got %v", want, deferUntil)
	}
	if !client.publishDeferred(deferUntil) {
		t.Fatal("expected publishes to be deferred before the deadline")
	}

	// The server's LATEST word wins: a fresh shorter hint replaces an
	// earlier longer deadline.
	client.applyRetryPacing(&HTTPStatusError{StatusCode: 429, RetryAfter: time.Second, retryAfterPresent: true}, &deferUntil, &attempt)
	if want := clock.now.Add(time.Second); !deferUntil.Equal(want) {
		t.Fatalf("expected the fresh hint to replace the deadline with %v, got %v", want, deferUntil)
	}

	// An explicit zero ("retry now") arms only the tiny anti-hot-loop floor.
	client.applyRetryPacing(&HTTPStatusError{StatusCode: 429, RetryAfter: 0, retryAfterPresent: true}, &deferUntil, &attempt)
	if want := clock.now.Add(minRetryNowSpacing); !deferUntil.Equal(want) {
		t.Fatalf("expected a retry-now hint to arm the %v floor, got %v", minRetryNowSpacing, deferUntil)
	}

	// A server hint never advances the client-side backoff progression.
	if attempt != 0 {
		t.Fatalf("expected hinted failures to leave the backoff attempt at 0, got %d", attempt)
	}

	// The FIRST hint-less retryable failure advances the backoff count and
	// REPLACES the stale hint deadline with its own — the latest failure's
	// schedule wins, so an old hint from a previous attempt cannot park the
	// batch past its slot.
	//
	// It used to clear the deadline outright, which handed the retry to the
	// flush tick. That read as "retry promptly" only while the flush interval
	// was one second; it is a fifteen-second stall at the current default.
	// The first failure now paces on the RETRY clock like every other.
	client.applyRetryPacing(&HTTPStatusError{StatusCode: 500}, &deferUntil, &attempt)
	if want := clock.now.Add(publishBackoffBase); !deferUntil.Equal(want) {
		t.Fatalf("expected the first hintless failure to arm its own %v deadline, got %v", publishBackoffBase, deferUntil)
	}
	if attempt != 1 {
		t.Fatalf("expected the hintless failure to advance the backoff attempt to 1, got %d", attempt)
	}

	// A non-retryable status never arms the deferral.
	var fresh time.Time
	freshAttempt := 0
	client.applyRetryPacing(&HTTPStatusError{StatusCode: 400, RetryAfter: 7 * time.Second, retryAfterPresent: true}, &fresh, &freshAttempt)
	if !fresh.IsZero() {
		t.Fatalf("expected 400 to leave the deadline unset, got %v", fresh)
	}
	if freshAttempt != 0 {
		t.Fatalf("expected 400 to leave the backoff attempt at 0, got %d", freshAttempt)
	}

	// Re-arm from a fresh hint; a success then clears the deadline and
	// resets the backoff progression.
	client.applyRetryPacing(&HTTPStatusError{StatusCode: 429, RetryAfter: 5 * time.Second, retryAfterPresent: true}, &deferUntil, &attempt)
	if want := clock.now.Add(5 * time.Second); !deferUntil.Equal(want) {
		t.Fatalf("expected the fresh hint to arm %v, got %v", want, deferUntil)
	}
	client.applyRetryPacing(nil, &deferUntil, &attempt)
	if !deferUntil.IsZero() {
		t.Fatalf("expected success to clear the deadline, got %v", deferUntil)
	}
	if attempt != 0 {
		t.Fatalf("expected success to reset the backoff attempt, got %d", attempt)
	}

	clock.now = clock.now.Add(time.Minute)
	if client.publishDeferred(deferUntil) {
		t.Fatal("expected a cleared deadline to never defer")
	}
}

func TestBackoffCeilingGrowthAndCap(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 0},
		// The first failure shares the second's window. It used to be 0 —
		// "no deferral", which meant the flush tick decided, and that tick is
		// now fifteen seconds by default.
		{1, time.Second},
		{2, time.Second},
		{3, 2 * time.Second},
		{4, 4 * time.Second},
		{5, 8 * time.Second},
		{6, 16 * time.Second},
		{7, 32 * time.Second},
		{8, publishBackoffCap}, // 64s ceiling clamps to the 60s cap
		{9, publishBackoffCap},
		{50, publishBackoffCap},
		{1 << 30, publishBackoffCap}, // exponent clamp: huge counts cannot overflow
	}
	for _, c := range cases {
		if got := backoffCeiling(c.attempt); got != c.want {
			t.Fatalf("backoffCeiling(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestBackoffDelayJitterBounds(t *testing.T) {
	client := &Client{}

	// Jitter pinned at the bottom of the window: the delay is exactly the
	// base for every attempt, never less.
	client.jitter = func() float64 { return 0 }
	for _, attempt := range []int{2, 5, 9} {
		if got := client.backoffDelay(attempt); got != publishBackoffBase {
			t.Fatalf("backoffDelay(%d) with zero jitter = %v, want %v", attempt, got, publishBackoffBase)
		}
	}

	// Jitter pinned at the top: the delay stays strictly under the ceiling.
	client.jitter = func() float64 { return math.Nextafter(1, 0) }
	for _, attempt := range []int{3, 5, 20} {
		got := client.backoffDelay(attempt)
		ceiling := backoffCeiling(attempt)
		if got < publishBackoffBase || got >= ceiling {
			t.Fatalf("backoffDelay(%d) with max jitter = %v, want in [%v, %v)", attempt, got, publishBackoffBase, ceiling)
		}
	}

	// The first failure defers by exactly the base regardless of jitter: its
	// window has zero width (ceiling == base), so there is nothing to jitter
	// over — and, deliberately, nothing that can make the first retry LATER
	// than the ~1s the old flush-tick path actually delivered.
	for _, jitter := range []float64{0, 0.5, math.Nextafter(1, 0)} {
		client.jitter = func() float64 { return jitter }
		if got := client.backoffDelay(1); got != publishBackoffBase {
			t.Fatalf("backoffDelay(1) with jitter %v = %v, want %v", jitter, got, publishBackoffBase)
		}
	}

	// The default (real) jitter source stays within the window and actually
	// varies — the whole point is that clients do not retry in lockstep.
	client.jitter = nil
	seen := make(map[time.Duration]bool)
	for i := 0; i < 200; i++ {
		got := client.backoffDelay(6)
		if got < publishBackoffBase || got > 16*time.Second {
			t.Fatalf("backoffDelay(6) sample %v outside [%v, %v]", got, publishBackoffBase, 16*time.Second)
		}
		seen[got] = true
	}
	if len(seen) < 2 {
		t.Fatal("expected jittered delays to vary across samples")
	}
}

func TestApplyRetryPacingBacksOffWithoutHint(t *testing.T) {
	clock := &stubClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	client := &Client{clock: clock}
	client.jitter = func() float64 { return 0 } // pin: delay == window floor

	var deferUntil time.Time
	attempt := 0

	// Failure 1 (5xx, no header): arms the 1s floor on the RETRY clock. It
	// used to arm nothing and let the flush tick decide, which is a
	// fifteen-second stall at the current default.
	client.applyRetryPacing(&HTTPStatusError{StatusCode: 500}, &deferUntil, &attempt)
	if want := clock.now.Add(publishBackoffBase); attempt != 1 || !deferUntil.Equal(want) {
		t.Fatalf("after failure 1: attempt=%d deferUntil=%v, want 1 and %v", attempt, deferUntil, want)
	}

	// Failure 2: same 1s floor — the window does not start growing until the
	// third (window [1s, 1s]).
	client.applyRetryPacing(&HTTPStatusError{StatusCode: 503}, &deferUntil, &attempt)
	if want := clock.now.Add(publishBackoffBase); attempt != 2 || !deferUntil.Equal(want) {
		t.Fatalf("after failure 2: attempt=%d deferUntil=%v, want 2 and %v", attempt, deferUntil, want)
	}

	// Failure 3, a transport error (no HTTP status at all — the server is
	// unreachable), with jitter pinned high: the delay lands inside the
	// grown window [1s, 2s].
	client.jitter = func() float64 { return math.Nextafter(1, 0) }
	client.applyRetryPacing(errors.New("dial tcp: connection refused"), &deferUntil, &attempt)
	if attempt != 3 {
		t.Fatalf("after failure 3: attempt=%d, want 3", attempt)
	}
	floor := clock.now.Add(publishBackoffBase)
	ceiling := clock.now.Add(2 * time.Second)
	if deferUntil.Before(floor) || !deferUntil.Before(ceiling) {
		t.Fatalf("after failure 3: deferUntil=%v, want in [%v, %v)", deferUntil, floor, ceiling)
	}

	// A fresh Retry-After hint mid-outage: the server's word wins the
	// deadline, and the backoff progression is left where it was.
	client.applyRetryPacing(&HTTPStatusError{StatusCode: 429, RetryAfter: 7 * time.Second, retryAfterPresent: true}, &deferUntil, &attempt)
	if want := clock.now.Add(7 * time.Second); attempt != 3 || !deferUntil.Equal(want) {
		t.Fatalf("after hinted failure: attempt=%d deferUntil=%v, want 3 and %v", attempt, deferUntil, want)
	}

	// A client-side permanent failure (encode) says nothing about the
	// endpoint: pacing is fully untouched.
	before := deferUntil
	client.applyRetryPacing(&EncodeError{Err: errors.New("bad payload")}, &deferUntil, &attempt)
	if attempt != 3 || !deferUntil.Equal(before) {
		t.Fatalf("after client-side permanent failure: attempt=%d deferUntil=%v, want 3 and %v", attempt, deferUntil, before)
	}

	// A permanent HTTP response proves the endpoint answered — the outage
	// is over — so the streak resets; the deadline is never armed by a
	// permanent outcome and stays where it was.
	client.applyRetryPacing(&HTTPStatusError{StatusCode: 400}, &deferUntil, &attempt)
	if attempt != 0 || !deferUntil.Equal(before) {
		t.Fatalf("after permanent HTTP failure: attempt=%d deferUntil=%v, want 0 and %v", attempt, deferUntil, before)
	}

	// Success clears the deadline; the next hint-less failure starts the
	// schedule over at the 1s floor rather than deep in the grown window.
	client.applyRetryPacing(nil, &deferUntil, &attempt)
	if !deferUntil.IsZero() {
		t.Fatalf("after success: deferUntil=%v, want zero", deferUntil)
	}
	client.applyRetryPacing(&HTTPStatusError{StatusCode: 500}, &deferUntil, &attempt)
	if want := clock.now.Add(publishBackoffBase); attempt != 1 || !deferUntil.Equal(want) {
		t.Fatalf("after reset + failure: attempt=%d deferUntil=%v, want 1 and %v", attempt, deferUntil, want)
	}
}

func TestCallerAbandonedFlushDoesNotPace(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	// A failure that IS the caller's cancellation (possibly wrapped) is not
	// a backpressure signal: pacing is skipped entirely.
	if !callerAbandonedFlush(canceled, context.Canceled) {
		t.Fatal("expected a context.Canceled failure under a canceled caller context to be abandoned")
	}
	if !callerAbandonedFlush(canceled, fmt.Errorf("publish: %w", context.Canceled)) {
		t.Fatal("expected a wrapped cancellation to be abandoned")
	}
	expired, expire := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expire()
	if !callerAbandonedFlush(expired, fmt.Errorf("publish: %w", context.DeadlineExceeded)) {
		t.Fatal("expected a caller-deadline failure to be abandoned")
	}

	// A REAL endpoint outcome racing the caller's deadline is still
	// endpoint feedback and must pace, even though ctx.Err() is non-nil.
	if callerAbandonedFlush(canceled, &HTTPStatusError{StatusCode: 503}) {
		t.Fatal("expected an HTTP status failure to pace even under a canceled caller context")
	}
	if callerAbandonedFlush(canceled, errors.New("dial tcp: connection refused")) {
		t.Fatal("expected a transport failure to pace even under a canceled caller context")
	}

	// The error must match the caller context's OWN state: a transport
	// timeout (DeadlineExceeded) under a merely CANCELED caller context is
	// the endpoint's doing, not the caller's, and must pace.
	if callerAbandonedFlush(canceled, fmt.Errorf("publish: %w", context.DeadlineExceeded)) {
		t.Fatal("expected a transport deadline under a canceled caller context to pace")
	}

	// A cancellation-shaped failure with the caller context still live is
	// the transport's own doing (HTTP timeout) and must pace.
	if callerAbandonedFlush(context.Background(), context.DeadlineExceeded) {
		t.Fatal("expected a failure under a live context to apply pacing")
	}

	// A flush that finished successfully paces (resets) even if the caller
	// context was canceled just after — success is success.
	if callerAbandonedFlush(canceled, nil) {
		t.Fatal("expected a successful flush to apply pacing regardless of the caller context")
	}

	// A nil context can never be the cause of the failure.
	if callerAbandonedFlush(nil, context.Canceled) {
		t.Fatal("expected a nil caller context to never mark the flush abandoned")
	}
}

func TestConsentDenialDiscardResetsBackoffStreak(t *testing.T) {
	clock := &stubClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	client := &Client{clock: clock}
	client.jitter = func() float64 { return 0 }

	// A denial surfacing as ErrConsentDenied from the publish itself: the
	// discarded batch takes the streak with it, and no deferral is armed.
	var deferUntil time.Time
	attempt := 3
	client.applyRetryPacing(ErrConsentDenied, &deferUntil, &attempt)
	if attempt != 0 || !deferUntil.IsZero() {
		t.Fatalf("after mid-publish denial: attempt=%d deferUntil=%v, want 0 and zero", attempt, deferUntil)
	}

	// The epoch-based discard of a held batch resets the streak too.
	client.consentEpoch.Add(1)
	seen := uint64(0)
	attempt = 4
	batch := client.dropBatchOnConsentEpoch([]Event{{ID: "evt-1", Name: "denied"}}, &seen, &attempt)
	if len(batch) != 0 {
		t.Fatalf("expected the epoch discard to clear the batch, got %+v", batch)
	}
	if attempt != 0 {
		t.Fatalf("expected the epoch discard to reset the streak, got %d", attempt)
	}

	// An epoch move with NOTHING held resets nothing — there was no batch
	// for the streak to belong to.
	client.consentEpoch.Add(1)
	attempt = 2
	if got := client.dropBatchOnConsentEpoch(nil, &seen, &attempt); len(got) != 0 || attempt != 2 {
		t.Fatalf("expected an empty-batch epoch move to leave the streak, got batch=%v attempt=%d", got, attempt)
	}
}

func TestTrackSuccessClearsWorkerBackoff(t *testing.T) {
	var calls atomic.Int64
	var stamps [3]atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if call <= 3 {
			stamps[call-1].Store(time.Now().UnixMilli())
		}
		if call == 1 {
			// A hint-less 5xx: the batch is retained; with a 10-minute
			// flush interval the next automatic attempt is far away.
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"code":"internal_error","message":"broker unavailable"}}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	// The flush interval is deliberately enormous: only the success-signal
	// nudge can produce a prompt retry of the retained batch.
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

	if err := client.Enqueue(Event{Name: "outage_victim"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 5*time.Second, "the failing attempt", func() bool { return calls.Load() >= 1 })

	// A synchronous Track succeeds (call 2): the endpoint is healthy, so
	// the nudge must wake the parked worker and retry the retained batch
	// (call 3) promptly — NOT at the flush tick minutes away.
	if err := client.Track(context.Background(), Event{Name: "recovery_probe"}); err != nil {
		t.Fatalf("track: %v", err)
	}
	waitFor(t, 3*time.Second, "the retained batch retry after recovery", func() bool { return calls.Load() >= 3 })
	gap := stamps[2].Load() - stamps[1].Load()
	if gap >= 2000 {
		t.Fatalf("expected the Track success to nudge a prompt retry, got %dms", gap)
	}
}

func TestTrackSuccessDoesNotFlushHealthyPartialBatches(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	// Healthy pipeline: no failures, no armed deadline, no streak — a Track
	// success must NOT flush the partial async batch ahead of its
	// BatchSize/FlushInterval schedule.
	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "token-value",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		BatchSize:     10,
		FlushInterval: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if err := client.Enqueue(Event{Name: "partial_batch_event"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := client.Track(context.Background(), Event{Name: "healthy_track"}); err != nil {
		t.Fatalf("track: %v", err)
	}

	// Only the synchronous Track single may have reached the server; give a
	// wrongly-nudged worker ample time to misbehave before asserting.
	time.Sleep(1200 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected only the Track publish on a healthy pipeline, got %d calls", got)
	}

	// The partial batch is still intact and delivers on close.
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected the partial batch to deliver on close, got %d calls", got)
	}
}

func TestFlushSwallowedPermanentHTTPResetsStreak(t *testing.T) {
	// The flush loop swallows a permanent HTTP rejection (drops the batch,
	// keeps draining) and then returns a LATER batch's retryable failure.
	// The swallowed 4xx proved the endpoint answered, so the pre-flush
	// streak must end there and the returned 5xx paces as a first failure.
	transport := &scriptedTransport{errs: []error{&HTTPStatusError{StatusCode: 400}, &HTTPStatusError{StatusCode: 503}}}
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
	if !client.queue.enqueue(Event{ID: "evt-2", Name: "second"}) {
		t.Fatal("enqueue evt-2")
	}

	batch := []Event{{ID: "evt-1", Name: "first"}}
	var consentEpoch uint64
	backoffAttempt := 5 // a pre-flush failure streak
	retained, err := client.flushAvailable(context.Background(), batch, &consentEpoch, &backoffAttempt)
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected the later 503 to be returned, got %v", err)
	}
	if len(retained) != 1 || retained[0].ID != "evt-2" {
		t.Fatalf("expected the second batch retained, got %+v", retained)
	}
	if backoffAttempt != 0 {
		t.Fatalf("expected the swallowed permanent HTTP response to reset the streak, got %d", backoffAttempt)
	}

	// The reset is what this pins, and the DEADLINE is the sharper witness of
	// it than the counter: a streak that had not reset would arm a window
	// grown to attempt 6, not the 1s floor.
	var deferUntil time.Time
	before := client.clock.Now()
	client.applyRetryPacing(err, &deferUntil, &backoffAttempt)
	// A real clock, so the window is a bound rather than an equality — but a
	// wide one is still decisive here: attempt 6 would arm 32 seconds.
	if delay := deferUntil.Sub(before); backoffAttempt != 1 || delay < publishBackoffBase || delay > publishBackoffBase+time.Second {
		t.Fatalf("expected the post-flush failure to pace as a fresh attempt 1 at the %v floor, got attempt=%d deferUntil=+%v", publishBackoffBase, backoffAttempt, deferUntil.Sub(before))
	}
}

// scriptedTransport fails call i with errs[i] (nil = success) and accepts
// every call past the script's end.
type scriptedTransport struct {
	unusedRemoteConfigTransport
	errs  []error
	calls int
}

func (t *scriptedTransport) Publish(ctx context.Context, request batchRequest) (batchResult, error) {
	i := t.calls
	t.calls++
	if i < len(t.errs) && t.errs[i] != nil {
		return batchResult{}, t.errs[i]
	}
	return batchResult{Accepted: len(request.Events)}, nil
}

func (t *scriptedTransport) PublishConsent(ctx context.Context, request consentRequest) (consentResult, error) {
	return consentResult{Recorded: true}, nil
}

func TestFlushMidSuccessResetsBackoffStreak(t *testing.T) {
	// The retained batch delivers, then a LATER queued batch fails
	// hint-less: the mid-flush success must break the failure streak, so
	// the returned error paces as a FIRST failure (cadence slot), not as
	// a continuation of the pre-flush streak.
	transport := &scriptedTransport{errs: []error{nil, &HTTPStatusError{StatusCode: 503}}}
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
	if !client.queue.enqueue(Event{ID: "evt-2", Name: "second"}) {
		t.Fatal("enqueue evt-2")
	}

	batch := []Event{{ID: "evt-1", Name: "first"}}
	var consentEpoch uint64
	backoffAttempt := 5 // a pre-flush failure streak
	retained, err := client.flushAvailable(context.Background(), batch, &consentEpoch, &backoffAttempt)
	if err == nil || len(retained) != 1 || retained[0].ID != "evt-2" {
		t.Fatalf("expected the second batch to fail retryably and be retained, got err=%v retained=%+v", err, retained)
	}
	if backoffAttempt != 0 {
		t.Fatalf("expected the mid-flush success to reset the streak, got %d", backoffAttempt)
	}

	// The worker's pacing call then treats the returned failure as the
	// streak's first.
	// The reset is what this pins, and the DEADLINE is the sharper witness of
	// it than the counter: a streak that had not reset would arm a window
	// grown to attempt 6, not the 1s floor.
	var deferUntil time.Time
	before := client.clock.Now()
	client.applyRetryPacing(err, &deferUntil, &backoffAttempt)
	// A real clock, so the window is a bound rather than an equality — but a
	// wide one is still decisive here: attempt 6 would arm 32 seconds.
	if delay := deferUntil.Sub(before); backoffAttempt != 1 || delay < publishBackoffBase || delay > publishBackoffBase+time.Second {
		t.Fatalf("expected the post-flush failure to pace as a fresh attempt 1 at the %v floor, got attempt=%d deferUntil=+%v", publishBackoffBase, backoffAttempt, deferUntil.Sub(before))
	}
}

func TestHintlessFailureBacksOffEndToEnd(t *testing.T) {
	var calls atomic.Int64
	var stamps [3]atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if call <= 3 {
			stamps[call-1].Store(time.Now().UnixMilli())
		}
		if call <= 2 {
			// A 5xx WITHOUT a Retry-After header: pacing is entirely the
			// client's responsibility.
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"code":"internal_error","message":"broker unavailable"}}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	// A DELIBERATELY tiny flush interval — 50ms, far below the 1s backoff
	// floor. It is the control: if retry pacing were still riding the flush
	// ticker, every retry would land at ~50ms. Both retries holding at ~1s
	// is what proves the two clocks are separate.
	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "token-value",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		BatchSize:     1,
		FlushInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	if err := client.Enqueue(Event{Name: "hintless_backoff"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	waitFor(t, 5*time.Second, "three publish attempts", func() bool { return calls.Load() >= 3 })
	firstGap := stamps[1].Load() - stamps[0].Load()
	secondGap := stamps[2].Load() - stamps[1].Load()
	// The FIRST failure now paces on the retry clock too. It used to fall
	// through to the flush tick, which was indistinguishable from prompt only
	// because that tick was a second; the same path is a fifteen-second stall
	// at the current default.
	if firstGap < 800 {
		t.Fatalf("expected the FIRST retry to wait out the 1s backoff floor rather than riding the 50ms flush tick, got %dms", firstGap)
	}
	if secondGap < 800 {
		t.Fatalf("expected the second retry to wait out the 1s backoff floor, got %dms", secondGap)
	}
	if firstGap > 4000 || secondGap > 4000 {
		t.Fatalf("expected both retries at the backoff deadline, got %dms and %dms", firstGap, secondGap)
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
	backoffAttempt := 0
	batch, err := client.flushAvailable(context.Background(), nil, &consentEpoch, &backoffAttempt)
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected transient 500 on the first attempt, got %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected the batch to stay retained after a transient failure, got %d events", len(batch))
	}

	batch, err = client.flushAvailable(context.Background(), batch, &consentEpoch, &backoffAttempt)
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
		_ = json.NewDecoder(ingestRequestBody(t, r)).Decode(&request)
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

	// Poll the recorded attempt time itself, not the call counter: the
	// handler bumps calls BEFORE storing secondAttempt, so a counter-based
	// wait can read the timestamp mid-handler as zero.
	waitFor(t, 5*time.Second, "the deadline retry", func() bool { return secondAttempt.Load() != 0 })
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

func TestConsentDenialClearsStaleDeferral(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "3600")
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
		BatchSize:     1,
		FlushInterval: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	if err := client.Enqueue(Event{Name: "deferred_then_denied"}); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	waitFor(t, 2*time.Second, "the rate-limited attempt", func() bool { return calls.Load() >= 1 })

	// The denial discards the retained batch; the re-grant admits new
	// events. The stale 1h deadline must not hold them.
	client.SetConsent(false)
	client.SetConsent(true)
	if err := client.Enqueue(Event{Name: "after_regrant"}); err != nil {
		t.Fatalf("enqueue second: %v", err)
	}
	waitFor(t, 3*time.Second, "the post-regrant publish", func() bool { return calls.Load() >= 2 })
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

// TestRetryPacingIsIndependentOfTheFlushInterval is A6's separation
// acceptance for this SDK, and it is deliberately taken at the SHIPPED
// default rather than at a test-friendly interval.
//
// The regression it pins is specific. `defaultFlushInterval` moved from 1s to
// 15s to stop an idle process issuing a request per second. That change is
// correct on its own terms, but the first retryable publish failure used to
// arm no deadline at all and let the flush ticker decide when to retry — so
// raising the idle cadence silently multiplied the recovery delay by fifteen,
// on a path where the worker has stopped reading the queue (`queueEvents` is
// nil at BatchSize) and enqueues are being refused with ErrQueueFull.
//
// A test that sets its own FlushInterval cannot catch this: it measures the
// interval it chose. This one takes the default and asserts the retry lands
// nowhere near it.
func TestRetryPacingIsIndependentOfTheFlushInterval(t *testing.T) {
	var calls atomic.Int64
	var stamps [2]atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if call <= 2 {
			stamps[call-1].Store(time.Now().UnixMilli())
		}
		if call == 1 {
			// Retryable, and NO Retry-After: pacing is entirely the client's.
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"code":"internal_error","message":"broker unavailable"}}`))
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
		BatchSize:     1,
		// FlushInterval DELIBERATELY unset: this exercises the shipped
		// default, which is the whole subject of the test.
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	if err := client.Enqueue(Event{Name: "independent_pacing"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Wait on the TIMESTAMP, not the call counter. The handler increments
	// calls before storing the stamp, so a scheduler preemption between those
	// two atomics lets a counter-only wait read stamps[1] as zero and compute
	// a huge negative gap — failing the lower bound below while retry pacing
	// was in fact correct (Codex on #48).
	waitFor(t, defaultFlushInterval+10*time.Second, "the retry", func() bool { return stamps[1].Load() != 0 })
	gap := time.Duration(stamps[1].Load()-stamps[0].Load()) * time.Millisecond

	// Comfortably below the flush interval: this is the assertion that fails
	// if retry pacing is ever handed back to the ticker.
	if ceiling := defaultFlushInterval / 3; gap > ceiling {
		t.Fatalf("the retry waited %v — with a %v flush interval that is the ticker pacing the retry, not the backoff schedule (want under %v)",
			gap, defaultFlushInterval, ceiling)
	}
	// And not a hot loop either: the 1s floor still applies.
	if gap < publishBackoffBase-200*time.Millisecond {
		t.Fatalf("the retry came back in %v, under the %v backoff floor", gap, publishBackoffBase)
	}
}

// TestRetainedBatchRetryDoesNotStarveTheQueue is the second clause of A6's
// Go retry-decoupling gate, beside the sibling above that pins the first:
// while a retained FULL batch waits out a hint-less failure on the retry
// clock, the bounded queue keeps absorbing a sustained enqueue rate and drains
// at the retry — no ErrQueueFull. It is taken at the SHIPPED FlushInterval
// default for the same reason: with the retry handed back to the ticker, the
// worker stops reading the queue (queueEvents is nil at BatchSize) for the
// whole interval, and a queue of BufferSize fills at any rate above
// BufferSize per interval. Here the queue is twenty deep and thirty events
// arrive over three seconds — the fifteen-second tick would overflow it in
// the second second; the one-second retry clock never lets it pass ten.
//
// The two clauses are distinct, and this scene exists because the sibling
// cannot see this one: a first wait of three seconds passes the sibling's
// timing bound and still refuses ten of these events.
func TestRetainedBatchRetryDoesNotStarveTheQueue(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			// Retryable, and NO Retry-After: the wait is the client's own.
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"code":"internal_error","message":"broker unavailable"}}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	const queueDepth = 20
	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "token-value",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		BatchSize:     1,
		BufferSize:    queueDepth,
		// FlushInterval DELIBERATELY unset: the shipped default is the subject.
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	// One event fills the batch, fails once, and is retained: from here the
	// worker reads nothing from the queue until the retry lands.
	if err := client.Enqueue(Event{Name: "retained"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 3*time.Second, "the first attempt", func() bool { return calls.Load() >= 1 })

	// A sustained rate for three seconds: 1.5x the queue's depth in total,
	// so the outcome is decided by when the worker resumes reading.
	const sustained = 30
	refused := 0
	for i := 0; i < sustained; i++ {
		switch err := client.Enqueue(Event{Name: "sustained"}); {
		case errors.Is(err, ErrQueueFull):
			refused++
		case err != nil:
			t.Fatalf("enqueue %d: %v", i, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if refused > 0 {
		t.Fatalf("%d of %d enqueues were refused with ErrQueueFull: the retained batch held the queue closed on the flush cadence instead of retrying on the backoff clock", refused, sustained)
	}

	// And nothing was lost on the way out: every event reached the server,
	// the retained one twice.
	waitFor(t, defaultFlushInterval+10*time.Second, "delivery of the sustained events", func() bool {
		return calls.Load() >= int64(1+1+sustained)
	})
}
