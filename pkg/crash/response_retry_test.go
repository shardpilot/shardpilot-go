package crash

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// The server's per-crash response (suppressed / warnings) must be surfaced through OnResult
// instead of being discarded.
func TestEmitSurfacesSuppressedResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"suppressed":true}`))
	}))
	defer server.Close()

	var got Result
	var calls int32
	client, err := NewClient(ClientOptions{
		IngestURL: server.URL,
		APIKey:    "workspace-api-key-test",
		Sampler:   alwaysSampler{},
		OnResult:  func(r Result) { got = r; atomic.AddInt32(&calls, 1) },
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.Emit(context.Background(), validEvent(t)); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("OnResult call count = %d, want 1", calls)
	}
	if !got.Suppressed {
		t.Errorf("Result.Suppressed = false, want true (consent-withheld crash)")
	}
}

func TestEmitSurfacesCrashIDFingerprintAndWarnings(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"crash_id":"c-1","fingerprint":"fp-1","warnings":["truncated breadcrumbs"]}`))
	}))
	defer server.Close()

	var got Result
	client, err := NewClient(ClientOptions{
		IngestURL: server.URL, APIKey: "workspace-api-key-test", Sampler: alwaysSampler{},
		OnResult: func(r Result) { got = r },
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.Emit(context.Background(), validEvent(t)); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if got.CrashID != "c-1" || got.Fingerprint != "fp-1" {
		t.Errorf("Result = %+v, want crash_id=c-1 fingerprint=fp-1", got)
	}
	if got.Suppressed {
		t.Errorf("Result.Suppressed = true, want false")
	}
	if len(got.Warnings) != 1 || got.Warnings[0] != "truncated breadcrumbs" {
		t.Errorf("Result.Warnings = %v, want [truncated breadcrumbs]", got.Warnings)
	}
}

// A 2xx with an unparseable body must NOT turn into an error — the crash was accepted.
func TestEmitTreatsGarbageResponseBodyAsAcceptedNoError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`<<not json>>`))
	}))
	defer server.Close()

	var called bool
	client, err := NewClient(ClientOptions{
		IngestURL: server.URL, APIKey: "workspace-api-key-test", Sampler: alwaysSampler{},
		OnResult: func(Result) { called = true },
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.Emit(context.Background(), validEvent(t)); err != nil {
		t.Fatalf("Emit must succeed on a 2xx with an unparseable body, got: %v", err)
	}
	if !called {
		t.Error("OnResult should still fire on an accepted crash")
	}
}

// A panic inside OnResult must not escape into the caller (or, on auto-capture, re-enter
// crash handling).
func TestOnResultPanicIsContained(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client, err := NewClient(ClientOptions{
		IngestURL: server.URL, APIKey: "workspace-api-key-test", Sampler: alwaysSampler{},
		OnResult: func(Result) { panic("boom") },
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.Emit(context.Background(), validEvent(t)); err != nil {
		t.Fatalf("Emit: %v", err)
	}
}

func TestParseRetryAfter(t *testing.T) {
	check := func(in string, wantD time.Duration, wantOK bool) {
		t.Helper()
		if d, ok := parseRetryAfter(in); d != wantD || ok != wantOK {
			t.Errorf("parseRetryAfter(%q) = (%v, %v), want (%v, %v)", in, d, ok, wantD, wantOK)
		}
	}
	check("3", 3*time.Second, true)
	check("  5 ", 5*time.Second, true)
	check("", 0, false)
	check("not-a-header", 0, false)
	check("-5", 0, false) // malformed negative → absent
	check("0", 0, true)   // explicit "retry now" is present, distinct from absent
	check("100000", maxRetryAfter, true)
	// Overflow guard: a value that would overflow time.Duration when multiplied by a second
	// must clamp to the cap, NOT wrap to a wrong/zero/negative duration.
	check("99999999999", maxRetryAfter, true)
	// A value too large to even fit an int (Atoi range error) still clamps to the cap;
	// a hugely negative one is malformed.
	check("99999999999999999999", maxRetryAfter, true)
	check("-99999999999999999999", 0, false)

	// An HTTP-date in the past is present but clamps to 0 (retry now).
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	if d, ok := parseRetryAfter(past); d != 0 || !ok {
		t.Errorf("past date: got (%v, %v), want (0, true)", d, ok)
	}
	// An HTTP-date in the near future yields a positive, capped duration.
	future := time.Now().Add(2 * time.Second).UTC().Format(http.TimeFormat)
	if d, ok := parseRetryAfter(future); d <= 0 || d > maxRetryAfter || !ok {
		t.Errorf("future date: got (%v, %v), want positive<=%v and true", d, ok, maxRetryAfter)
	}
}

// An explicit Retry-After: 0 means "retry now" and must override the (here huge) fixed
// backoff, not be mistaken for an absent header that falls back to it.
func TestRetryAfterZeroRetriesImmediately(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&hits, 1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client, err := NewClient(ClientOptions{
		IngestURL:    server.URL,
		APIKey:       "workspace-api-key-test",
		Sampler:      alwaysSampler{},
		MaxAttempts:  2,
		RetryBackoff: time.Hour, // would hang if the explicit 0 were treated as "absent"
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	start := time.Now()
	if err := client.Emit(context.Background(), validEvent(t)); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("Retry-After: 0 was not honored as retry-now (elapsed %v)", elapsed)
	}
	if atomic.LoadInt32(&hits) != 2 {
		t.Errorf("server hits = %d, want 2", hits)
	}
}

// The Retry-After header is parsed and carried on the returned status error.
func TestRetryAfterCarriedOnStatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	client, err := NewClient(ClientOptions{
		IngestURL: server.URL, APIKey: "workspace-api-key-test", Sampler: alwaysSampler{},
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	err = client.Emit(context.Background(), validEvent(t))
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("want *HTTPStatusError, got %v", err)
	}
	if statusErr.RetryAfter != 5*time.Second {
		t.Errorf("RetryAfter = %v, want 5s", statusErr.RetryAfter)
	}
}

// The retry loop must wait the server-supplied Retry-After, NOT the (here much larger) fixed
// backoff: a 429+Retry-After:1 then 202 completes in ~1s, far under the 1h fixed backoff.
func TestRetryAfterHonoredOverFixedBackoff(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&hits, 1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client, err := NewClient(ClientOptions{
		IngestURL:    server.URL,
		APIKey:       "workspace-api-key-test",
		Sampler:      alwaysSampler{},
		MaxAttempts:  2,
		RetryBackoff: time.Hour, // would hang the test if used instead of Retry-After
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	start := time.Now()
	if err := client.Emit(context.Background(), validEvent(t)); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("retry used the fixed backoff, not Retry-After (elapsed %v)", elapsed)
	}
	if atomic.LoadInt32(&hits) != 2 {
		t.Errorf("server hits = %d, want 2 (one 429 then one 202)", hits)
	}
}
