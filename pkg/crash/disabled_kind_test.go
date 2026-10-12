package crash

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCrashDisabledKindStatusAndCode(t *testing.T) {
	for _, status := range []int{307, 400, 401, 403, 404, 408, 409, 422, 429, 500, 503} {
		for _, answer := range []struct {
			name, body string
			disabled   bool
		}{
			{"exact", `{"error":{"code":"kind_not_enabled","message":"translated","future":true}}`, true},
			{"unknown", `{"error":{"code":"unknown_kind"}}`, false},
			{"case", `{"error":{"code":"Kind_not_enabled"}}`, false},
			{"suffix", `{"error":{"code":"kind_not_enabled_extra"}}`, false},
			{"message-only", `{"error":{"code":"invalid_request","message":"kind_not_enabled"}}`, false},
			{"wrong-shape", `{"error":"kind_not_enabled"}`, false},
			{"malformed", `{"error":{"code":"kind_not_enabled"}`, false},
			{"empty", ``, false},
			{"oversized", `{"error":{"code":"kind_not_enabled"}}` + strings.Repeat(" ", maxResponseBodyBytes), false},
		} {
			t.Run(fmt.Sprintf("%d/%s", status, answer.name), func(t *testing.T) {
				requests := make(chan []byte, 2)
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					requests <- body
					if calls.Add(1) == 1 {
						w.Header().Set("Retry-After", "0")
						w.WriteHeader(status)
						fmt.Fprint(w, answer.body)
						return
					}
					w.WriteHeader(202)
					fmt.Fprint(w, `{"crash_id":"settled"}`)
				}))
				defer server.Close()
				var results []Result
				client, err := NewClient(ClientOptions{CrashURL: server.URL, CrashKey: "synthetic-fixture", MaxAttempts: 2, RetryBaseDelay: time.Nanosecond, OnResult: func(r Result) { results = append(results, r) }})
				if err != nil {
					t.Fatal(err)
				}
				event := validEvent(t)
				err = client.EmitFatal(context.Background(), event)
				retry := status == 408 || status == 429 || status >= 500 || (answer.disabled && (status == 400 || status == 404))
				if retry {
					if err != nil || calls.Load() != 2 || len(results) != 1 || results[0].CrashID != "settled" {
						t.Fatalf("retry err=%v calls=%d results=%v", err, calls.Load(), results)
					}
					first, second := <-requests, <-requests
					if !bytes.Equal(first, second) {
						t.Fatal("retry changed the encoded report")
					}
					var sent Event
					if err := json.Unmarshal(first, &sent); err != nil || sent.CrashID != event.CrashID || !sent.OccurredAt.Equal(event.OccurredAt) {
						t.Fatalf("original identity/clocks: %s, err=%v", first, err)
					}
				} else {
					var statusErr *HTTPStatusError
					if !errors.As(err, &statusErr) || statusErr.StatusCode != status || calls.Load() != 1 || len(results) != 0 {
						t.Fatalf("terminal err=%v calls=%d results=%v", err, calls.Load(), results)
					}
				}
			})
		}
	}
}

func TestCrashDisabledKindRetryDelays(t *testing.T) {
	date := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	for _, status := range []int{400, 404} {
		for _, header := range []string{"", "invalid", "-1", "0", "3", date.Format(http.TimeFormat), "999999", "Mon, 01 Jan 2001 00:00:00 GMT"} {
			t.Run(fmt.Sprintf("%d/%s", status, header), func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.Header().Set("Retry-After", header)
					w.WriteHeader(status)
					fmt.Fprint(w, `{"error":{"code":"kind_not_enabled"}}`)
				}))
				defer server.Close()
				client, err := NewClient(ClientOptions{CrashURL: server.URL, CrashKey: "synthetic-fixture", MaxAttempts: 3, RetryBaseDelay: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				var waits []time.Duration
				client.retryJitter = func() float64 { return 0.5 }
				client.retryWait = func(ctx context.Context, delay time.Duration) error { waits = append(waits, delay); return ctx.Err() }
				err = client.EmitFatal(context.Background(), validEvent(t))
				var statusErr *HTTPStatusError
				if !errors.As(err, &statusErr) || statusErr.StatusCode != status || !statusErr.Retryable() || calls.Load() != 3 || len(waits) != 2 {
					t.Fatalf("budget err=%v calls=%d waits=%v", err, calls.Load(), waits)
				}
				want := []time.Duration{500 * time.Millisecond, time.Second}
				switch header {
				case "0", "Mon, 01 Jan 2001 00:00:00 GMT":
					want = []time.Duration{0, 0}
				case "3":
					want = []time.Duration{3 * time.Second, 3 * time.Second}
				case "999999":
					want = []time.Duration{24 * time.Hour, 24 * time.Hour}
				case date.Format(http.TimeFormat):
					for _, wait := range waits {
						if wait < time.Until(date) || wait > time.Hour {
							t.Fatalf("date floor/cap: %v", waits)
						}
					}
					want = waits
				}
				if !reflect.DeepEqual(waits, want) {
					t.Fatalf("waits=%v want=%v", waits, want)
				}
			})
		}
	}
}

func TestCrashDisabledKindCancellation(t *testing.T) {
	for _, status := range []int{400, 404} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Retry-After", "3600")
				w.WriteHeader(status)
				fmt.Fprint(w, `{"error":{"code":"kind_not_enabled"}}`)
			}))
			defer server.Close()
			client, err := NewClient(ClientOptions{CrashURL: server.URL, CrashKey: "synthetic-fixture", MaxAttempts: 3})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			err = client.EmitFatal(ctx, validEvent(t))
			if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
				t.Fatalf("cancellation err=%v calls=%d", err, calls.Load())
			}
		})
	}
}
