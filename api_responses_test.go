package shardpilot

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

func TestAPIResponseBatchSuccessSettles(t *testing.T) {
	for _, body := range []string{"", `not json`, `{"accepted":1,"events":"invalid"}`, `{"accepted":0,"future":{"nested":true},"events":[{"event_id":"future","status":"future_status","future":true}]}`} {
		t.Run(body, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(202)
				fmt.Fprint(w, body)
			}))
			defer server.Close()
			client := newTestClient(t, server.URL)
			if err := client.Track(context.Background(), Event{Name: "settled_event"}); err != nil {
				t.Errorf("2xx must settle: %v", err)
			}
			if err := client.Close(context.Background()); err != nil {
				t.Errorf("Close: %v", err)
			}
			if calls.Load() != 1 {
				t.Errorf("2xx replayed: %d requests", calls.Load())
			}
			if client.Snapshot().Published != 1 || client.Snapshot().Accepted != 0 {
				t.Errorf("not counted settled: %+v", client.Snapshot())
			}
			if strings.Contains(body, "future_status") {
				stats := client.Snapshot()
				if stats.Accepted != 0 || stats.ByStatus[EventStatus("future_status")] != 1 {
					t.Errorf("unknown status was not preserved as non-accepted: %+v", stats)
				}
			}
		})
	}
}

func TestAPIResponseTimeoutRetryable(t *testing.T) {
	if !(&HTTPStatusError{StatusCode: 408}).Retryable() {
		t.Fatal("408 must retry")
	}
	if (&HTTPStatusError{StatusCode: 400}).Retryable() {
		t.Fatal("ordinary 400 is terminal")
	}
}

func TestAPIResponseNoRedirects(t *testing.T) {
	for _, consent := range []bool{false, true} {
		t.Run(fmt.Sprint(consent), func(t *testing.T) {
			var targetCalls atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls.Add(1); fmt.Fprint(w, `{}`) }))
			defer target.Close()
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", target.URL)
				w.WriteHeader(307)
			}))
			defer source.Close()
			injected := &http.Client{}
			transport := newHTTPTransport(Config{IngestURL: source.URL, HTTPClient: injected, HTTPTimeout: time.Second})
			var err error
			if consent {
				_, err = transport.PublishConsent(context.Background(), consentRequest{})
			} else {
				_, err = transport.Publish(context.Background(), batchRequest{})
			}
			var status *HTTPStatusError
			if !errors.As(err, &status) || status.StatusCode != 307 || targetCalls.Load() != 0 {
				t.Fatalf("redirect err=%v target requests=%d", err, targetCalls.Load())
			}
			if injected.CheckRedirect != nil {
				t.Fatal("mutated injected client")
			}
		})
	}
}

func TestAPIResponseBoundedEnvelope(t *testing.T) {
	envelope := `{"error":{"code":"validation_error","details":[{"code":"invalid_gzip"}]}}`
	for _, extra := range []int{0, maxErrorBodyBytes + 1} {
		body := envelope + strings.Repeat(" ", extra)
		err := newHTTPStatusError(&http.Response{StatusCode: 400, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))})
		if (err.ErrorCode != "") != (extra == 0) {
			t.Fatalf("padding=%d code=%q", extra, err.ErrorCode)
		}
	}
}

type apiResponseLogger struct{ calls atomic.Int32 }

func (l *apiResponseLogger) Printf(string, ...any) { l.calls.Add(1) }

func TestAPIResponseWriteDiagnostics(t *testing.T) {
	for _, consent := range []bool{false, true} {
		for i, body := range []string{"", `not json`, `null`, `{} trailing`, `truncated`, `{"future":true}`, `{}` + strings.Repeat(" ", rcMaxBodyBytes+1)} {
			t.Run(fmt.Sprintf("consent=%t/case=%d", consent, i), func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if body == "truncated" {
						w.Header().Set("Content-Length", "100")
						w.WriteHeader(202)
						fmt.Fprint(w, `{"accepted":1}`)
						return
					}
					w.WriteHeader(202)
					fmt.Fprint(w, body)
				}))
				defer server.Close()
				client := newTestClient(t, server.URL)
				logger := &apiResponseLogger{}
				client.cfg.Logger = logger
				field := "UnparsedBatchResponses"
				if consent {
					client.publishConsent(consentRequest{})
					field = "UnparsedConsentResponses"
				} else if err := client.Track(context.Background(), Event{Name: "settled_event"}); err != nil {
					t.Fatal(err)
				}
				if err := client.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				if client.Snapshot().Accepted != 0 {
					t.Fatal("partial accepted count escaped")
				}
				count := reflect.ValueOf(client.Snapshot()).FieldByName(field)
				want := uint64(1)
				if body == `{"future":true}` {
					want = 0
				}
				got := uint64(0)
				if count.IsValid() {
					got = count.Uint()
				}
				if got != want {
					t.Errorf("%s count invalid or differs from %d", field, want)
				}
				if want == 1 && logger.calls.Load() == 0 {
					t.Error("unparsed success was not diagnosed")
				}
				if calls.Load() != 1 {
					t.Errorf("settled write replayed: %d", calls.Load())
				}
			})
		}
	}
}

func TestAPIResponseRetryAfterStatusScope(t *testing.T) {
	for _, status := range []int{400, 408, 429, 503} {
		response := &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{"120"}}, Body: io.NopCloser(strings.NewReader(`{}`))}
		err := newHTTPStatusError(response)
		if err.retryAfterPresent != (status == 429 || status >= 500) {
			t.Errorf("status %d honors an out-of-class Retry-After", status)
		}
	}
}

func TestAPIResponseOutboxSuccessDiagnostics(t *testing.T) {
	for _, body := range []string{"", `{"recorded":true,"replayed":"invalid"}`, `{"future":true}`} {
		t.Run(body, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(202)
				fmt.Fprint(w, body)
			}))
			defer server.Close()
			logger := &apiResponseLogger{}
			client := newFloorTestClient(t, server.URL, "", func(cfg *Config) { cfg.Logger = logger })
			if _, err := client.SetConsent(true); err != nil {
				t.Fatal(err)
			}
			if err := client.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			snapshot := client.Snapshot()
			count := reflect.ValueOf(snapshot).FieldByName("UnparsedConsentResponses")
			got := uint64(0)
			if count.IsValid() {
				got = count.Uint()
			}
			want := uint64(1)
			if body == `{"future":true}` {
				want = 0
			}
			if got != want || snapshot.ConsentRecorded != 1 || calls.Load() != 1 {
				t.Fatalf("unparsed=%d want=%d recorded=%d requests=%d", got, want, snapshot.ConsentRecorded, calls.Load())
			}
			if want == 1 && logger.calls.Load() == 0 {
				t.Fatal("outbox success lacked diagnostic")
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
			client := newTestClient(t, server.URL)
			client.cfg.Logger = nil
			client.publishConsent(consentRequest{})
			if err := client.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(output.String(), "unreadable response") != (body == "not json") {
				t.Fatalf("default diagnostic: %q", output.String())
			}
		})
	}
}

func TestAPIResponseSpoolSettlementDiagnostics(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		t.Run(fmt.Sprintf("automatic=%t", automatic), func(t *testing.T) {
			for _, tc := range []struct {
				name, body string
				malformed  bool
				resent     uint64
				drop       SpoolDropReason
			}{
				{name: "empty", malformed: true},
				{name: "invalid", body: "not json", malformed: true},
				{name: "null", body: "null", malformed: true},
				{name: "trailing", body: "{} trailing", malformed: true},
				{name: "truncated", body: `{"accepted":1}`, malformed: true},
				{name: "oversized", body: `{}` + strings.Repeat(" ", rcMaxBodyBytes+1), malformed: true},
				{name: "partial", body: `{"accepted":1,"events":"invalid"}`, malformed: true},
				{name: "accepted", body: `{"events":[{"event_id":"restart-response","status":"accepted"}]}`, resent: 1},
				{name: "duplicate", body: `{"events":[{"event_id":"restart-response","status":"duplicate"}]}`, resent: 1},
				{name: "rejected", body: `{"events":[{"event_id":"restart-response","status":"rejected"}]}`, drop: SpoolDropTerminal},
				{name: "suppressed", body: `{"events":[{"event_id":"restart-response","status":"suppressed_no_consent"}]}`, drop: SpoolDropConsent},
				{name: "unknown", body: `{"events":[{"event_id":"restart-response","status":"future_status"}]}`, resent: 1},
				{name: "missing", body: `{}`, resent: 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var requests atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						requests.Add(1)
						if r.URL.Path != "/v1/events:batch" || len(wireEventBytes(t, ingestRequestBytes(t, r))) != 1 {
							t.Error("expected the real restart-loaded event batch")
						}
						if tc.name == "truncated" {
							w.Header().Set("Content-Length", "100")
						}
						w.WriteHeader(http.StatusAccepted)
						fmt.Fprint(w, tc.body)
					}))
					defer server.Close()
					dir := t.TempDir()
					writeConsentRecordFile(t, dir, "granted")
					deadline := int64(0)
					if !automatic {
						// Keep the automatic worker parked so Flush owns this case.
						deadline = time.Now().Add(time.Hour).UnixMilli()
					}
					writeSpoolRecordFile(t, dir, deadline, spoolTestEnvelope(t, "restart-response", time.Now()))
					recorder := &spoolDeadLetterRecorder{}
					settled := make(chan struct{}, 2)
					client := newSpoolTestClient(t, server.URL, dir, recorder, func(cfg *Config) {
						cfg.OnBatchResult = func(BatchResult) { settled <- struct{}{} }
					})
					defer client.Close(context.Background())
					if !automatic {
						if err := client.Flush(context.Background()); err != nil {
							t.Fatal(err)
						}
					}
					select {
					case <-settled:
					case <-time.After(10 * time.Second):
						t.Fatal("restart-loaded spool did not settle")
					}
					if err := client.Close(context.Background()); err != nil {
						t.Fatal(err)
					}
					if requests.Load() != 1 || len(readSpoolRecordFile(t, dir).Events) != 0 {
						t.Fatal("settled chunk must leave the durable spool without replay")
					}
					stats := client.Snapshot()
					if stats.SpoolResent != tc.resent {
						t.Errorf("SpoolResent=%d, want %d; unreadable success is settlement, not delivery evidence", stats.SpoolResent, tc.resent)
					}
					wantUnparsed := uint64(0)
					if tc.malformed {
						wantUnparsed = 1
					}
					if stats.UnparsedBatchResponses != wantUnparsed {
						t.Errorf("unparsed=%d, want %d", stats.UnparsedBatchResponses, wantUnparsed)
					}
					if tc.drop == "" && recorder.count() != 0 || tc.drop != "" && len(recorder.byReason(tc.drop)) != 1 {
						t.Errorf("unexpected dead letters: %+v", recorder.letters)
					}
				})
			}
		})
	}
}
