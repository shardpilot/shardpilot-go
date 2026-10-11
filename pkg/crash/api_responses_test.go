package crash

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type responseLogger struct{ calls atomic.Int32 }

func (l *responseLogger) Printf(string, ...any) { l.calls.Add(1) }

func TestAPIResponseSuccessDiagnostics(t *testing.T) {
	for i, body := range []string{"", `not json`, `null`, `{"crash_id":"partial","suppressed":"invalid"}`, `{} trailing`, `truncated`, `{"future":true}`, `{}` + strings.Repeat(" ", maxResponseBodyBytes+1)} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if body == "truncated" {
					w.Header().Set("Content-Length", "100")
					w.WriteHeader(202)
					fmt.Fprint(w, `{"crash_id":"partial"}`)
					return
				}
				w.WriteHeader(202)
				fmt.Fprint(w, body)
			}))
			defer server.Close()
			logger := &responseLogger{}
			var result Result
			client, err := NewClient(ClientOptions{CrashURL: server.URL, CrashKey: "synthetic-fixture", Sampler: alwaysSampler{}, Logger: logger, OnResult: func(r Result) { result = r }})
			if err != nil {
				t.Fatal(err)
			}
			if err = client.Emit(context.Background(), validEvent(t)); err != nil {
				t.Fatal(err)
			}
			method := reflect.ValueOf(client).MethodByName("UnparsedResponses")
			var count uint64
			if method.IsValid() {
				count = method.Call(nil)[0].Uint()
			}
			want := uint64(1)
			if body == `{"future":true}` {
				want = 0
			}
			if count != want || (want == 1 && logger.calls.Load() == 0) {
				t.Errorf("unparsed=%d want=%d diagnostics=%d", count, want, logger.calls.Load())
			}
			if result.CrashID != "" || result.Suppressed {
				t.Errorf("partial result escaped: %+v", result)
			}
			if calls.Load() != 1 {
				t.Errorf("settled crash replayed: %d", calls.Load())
			}
		})
	}
}

func TestAPIResponseErrorAndRedirect(t *testing.T) {
	for _, status := range []int{307, 400, 408, 429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var targetCalls atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls.Add(1); w.WriteHeader(202) }))
			defer target.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", target.URL)
				w.Header().Set("Retry-After", "3600")
				w.WriteHeader(status)
				fmt.Fprint(w, `{"error":{"code":"field_renamed","message":"translated detail","future":true}}`)
			}))
			defer server.Close()
			injected := &http.Client{}
			client, err := NewClient(ClientOptions{CrashURL: server.URL, CrashKey: "synthetic-fixture", Sampler: alwaysSampler{}, MaxAttempts: 1, HTTPClient: injected})
			if err != nil {
				t.Fatal(err)
			}
			err = client.Emit(context.Background(), validEvent(t))
			var httpErr *HTTPStatusError
			if !errors.As(err, &httpErr) || httpErr.StatusCode != status || targetCalls.Load() != 0 {
				t.Fatalf("status=%d err=%v redirect requests=%d", status, err, targetCalls.Load())
			}
			if !strings.Contains(httpErr.Error(), "field_renamed") {
				t.Error("missing stable code diagnostic")
			}
			if httpErr.Retryable() != (status == 408 || status == 429 || status >= 500) {
				t.Error("retry class")
			}
			if (status == 429 || status >= 500) && httpErr.RetryAfter != time.Hour {
				t.Errorf("Retry-After was not preserved: %v", httpErr.RetryAfter)
			}
			if status < 429 && httpErr.RetryAfter != 0 {
				t.Errorf("out-of-class retry hint: %v", httpErr.RetryAfter)
			}
			if injected.CheckRedirect != nil {
				t.Error("mutated injected client")
			}
		})
	}
}

func TestAPIResponseExponentialBackoff(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 3 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(202)
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	client, err := NewClient(ClientOptions{CrashURL: server.URL, CrashKey: "synthetic-fixture", Sampler: alwaysSampler{}, MaxAttempts: 4, RetryBaseDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var delays []time.Duration
	client.retryJitter = func() float64 { return 0.5 }
	client.retryWait = func(ctx context.Context, d time.Duration) error { delays = append(delays, d); return ctx.Err() }
	if err := client.Emit(context.Background(), validEvent(t)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 4 {
		t.Fatalf("requests=%d", calls.Load())
	}
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}
	if !reflect.DeepEqual(delays, want) {
		t.Fatalf("actual HTTP retry waits=%v want exponential jitter %v", delays, want)
	}
}

func TestAPIResponseBoundedCrashEnvelope(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		t.Run(fmt.Sprint(overflow), func(t *testing.T) {
			body := `{"error":{"code":"field_renamed"}}`
			if overflow {
				body += strings.Repeat(" ", maxResponseBodyBytes+1)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(400); fmt.Fprint(w, body) }))
			defer server.Close()
			client, err := NewClient(ClientOptions{CrashURL: server.URL, CrashKey: "synthetic-fixture", Sampler: alwaysSampler{}})
			if err != nil {
				t.Fatal(err)
			}
			err = client.Emit(context.Background(), validEvent(t))
			if err == nil {
				t.Fatal("terminal error missing")
			}
			if strings.Contains(err.Error(), "field_renamed") == overflow {
				t.Fatalf("overflow=%v error=%v", overflow, err)
			}
		})
	}
}

// A transport-owned timeout leaves the caller alive and is retryable. A caller
// cancellation stops the operation even if there are attempts left.
func TestAPIResponseCrashTimeoutAndCancellation(t *testing.T) {
	for _, cancelCaller := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelCaller), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if calls.Add(1) == 1 {
					<-r.Context().Done()
					return
				}
				w.WriteHeader(202)
				fmt.Fprint(w, `{}`)
			}))
			defer server.Close()
			ctx := context.Background()
			if cancelCaller {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			}
			client, err := NewClient(ClientOptions{CrashURL: server.URL, CrashKey: "synthetic-fixture", Sampler: alwaysSampler{}, MaxAttempts: 2, RetryBaseDelay: time.Nanosecond, HTTPClient: &http.Client{Timeout: 100 * time.Millisecond}})
			if err != nil {
				t.Fatal(err)
			}
			err = client.Emit(ctx, validEvent(t))
			if cancelCaller {
				if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
					t.Fatalf("caller cancellation err=%v requests=%d", err, calls.Load())
				}
			} else if err != nil || calls.Load() != 2 {
				t.Fatalf("live caller timeout err=%v requests=%d", err, calls.Load())
			}
		})
	}
}

func TestAPIResponseDefaultDiagnostic(t *testing.T) {
	for _, body := range []string{`not json`, `{}`} {
		t.Run(body, func(t *testing.T) {
			var output bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
			defer slog.SetDefault(previous)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(202); fmt.Fprint(w, body) }))
			defer server.Close()
			client, err := NewClient(ClientOptions{CrashURL: server.URL, CrashKey: "synthetic-fixture", Sampler: alwaysSampler{}})
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Emit(context.Background(), validEvent(t)); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(output.String(), "unparseable response") != (body == "not json") {
				t.Fatalf("default diagnostic: %q", output.String())
			}
		})
	}
}

func TestAPIResponseRetryPacingControls(t *testing.T) {
	date := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	for _, header := range []string{"", "garbage", "0", "5", "100000", date.Format(http.TimeFormat)} {
		t.Run(header, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.Header().Set("Retry-After", header)
					w.WriteHeader(503)
					return
				}
				w.WriteHeader(202)
				fmt.Fprint(w, `{}`)
			}))
			defer server.Close()
			client, err := NewClient(ClientOptions{CrashURL: server.URL, CrashKey: "synthetic-fixture", Sampler: alwaysSampler{}, MaxAttempts: 2, RetryBaseDelay: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			client.retryJitter = func() float64 { return 0.5 }
			var waited time.Duration
			client.retryWait = func(ctx context.Context, d time.Duration) error { waited = d; return ctx.Err() }
			if err := client.Emit(context.Background(), validEvent(t)); err != nil {
				t.Fatal(err)
			}
			want := 500 * time.Millisecond
			switch header {
			case "0":
				want = 0
			case "5":
				want = 5 * time.Second
			case "100000":
				want = 24 * time.Hour
			case date.Format(http.TimeFormat):
				if waited < time.Until(date) || waited > time.Hour {
					t.Fatalf("date floor violated: %v", waited)
				}
				want = waited
			}
			if waited != want || calls.Load() != 2 {
				t.Fatalf("wait=%v want=%v requests=%d", waited, want, calls.Load())
			}
		})
	}
	client := &Client{retryBackoff: time.Duration(1<<63 - 1), retryJitter: func() float64 { return 0.5 }}
	if delay := client.retryDelay(10000); delay != 30*time.Second {
		t.Fatalf("overflow/cap: %v", delay)
	}
}

func TestAPIResponseRetryWaitCancellation(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(429)
	}))
	defer server.Close()
	client, err := NewClient(ClientOptions{CrashURL: server.URL, CrashKey: "synthetic-fixture", Sampler: alwaysSampler{}, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = client.Emit(ctx, validEvent(t))
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Fatalf("cancellation err=%v requests=%d", err, calls.Load())
	}
}
