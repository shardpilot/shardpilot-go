package shardpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// These records carry local retry metadata beside the original wire bytes.
// Restart and dispatch use the public constructor and Flush API.
func TestSpoolRestartRetryWindow(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		intakeAge, ambiguousAge, hostAge time.Duration
		delivered                        bool
	}{
		{"offline_two_hours", 2 * time.Hour, 0, 2 * time.Hour, true},
		{"ambiguous_sixteen_minutes", 2 * time.Hour, 16 * time.Minute, -time.Hour, false},
		{"ambiguous_at_fifteen_minutes", 2 * time.Hour, 15 * time.Minute, -time.Hour, false},
		{"ambiguous_fourteen_minutes", 2 * time.Hour, 14 * time.Minute, -time.Hour, true},
		{"outer_age_with_future_host_time", 8 * 24 * time.Hour, 0, -time.Hour, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, server := newSpoolTestServer(t)
			defer server.Close()
			state.setOutcome(http.StatusAccepted, "", "")
			dir := t.TempDir()
			now := time.Now()
			const subjectID = "48ec99d5-81da-4fdc-aebd-c1c4d991a9ef"
			const freshID = "6abf7f19-0265-497a-9f49-cc00798416c0"
			subject := spoolTestEnvelope(t, subjectID, now.Add(-tc.hostAge))
			fresh := spoolTestEnvelope(t, freshID, now.Add(-time.Minute))
			ambiguous := int64(0)
			if tc.ambiguousAge > 0 {
				ambiguous = now.Add(-tc.ambiguousAge).UnixMilli()
			}
			data, err := json.Marshal(map[string]any{"version": 2, "events": []any{
				map[string]any{"raw": subject, "intake_unix_ms": now.Add(-tc.intakeAge).UnixMilli(), "ambiguous_unix_ms": ambiguous},
				map[string]any{"raw": fresh, "intake_unix_ms": now.Add(-time.Minute).UnixMilli()},
			}})
			if err != nil {
				t.Fatal(err)
			}
			writeConsentRecordFile(t, dir, "granted")
			if err := os.WriteFile(filepath.Join(dir, spoolFileName), data, 0600); err != nil {
				t.Fatal(err)
			}
			recorder := &spoolDeadLetterRecorder{}
			client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
			defer client.Close(context.Background())
			if err := client.Flush(context.Background()); err != nil {
				t.Fatalf("flush: %v", err)
			}
			var sawSubject, sawFresh bool
			for _, batch := range state.allArrivals() {
				for _, id := range batch {
					sawSubject = sawSubject || id == subjectID
					sawFresh = sawFresh || id == freshID
				}
			}
			if sawSubject != tc.delivered || !sawFresh {
				t.Fatalf("retry window: subject reached wire=%t (want %t), fresh control reached wire=%t", sawSubject, tc.delivered, sawFresh)
			}
			wantExpired := uint64(0)
			if !tc.delivered {
				wantExpired = 1
			}
			if got := client.Snapshot().SpoolExpired; got != wantExpired {
				t.Fatalf("expiry count=%d want %d", got, wantExpired)
			}
			if !tc.delivered {
				letters := recorder.byReason(SpoolDropExpired)
				if len(letters) != 1 || len(letters[0].Envelopes) != 1 || !containsEventID(t, letters[0].Envelopes, subjectID) {
					t.Fatalf("original envelope must be reported once: %+v", letters)
				}
			}
		})
	}
}

// The injected HTTP client is a supported host API. It reads the actual
// serialized request before simulating a lost response or rejection.
type retryWindowHTTP struct {
	mu     sync.Mutex
	status int
	err    error
	bodies [][]byte
	trace  func(*httptrace.ClientTrace)
}

func (r *retryWindowHTTP) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Path != "/v1/events:batch" {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bodies = append(r.bodies, body)
	if r.trace != nil {
		r.trace(httptrace.ContextClientTrace(req.Context()))
	}
	if r.err != nil {
		return nil, r.err
	}
	return &http.Response{StatusCode: r.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
}

func TestRetryWindowTransportClassification(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		status    int
		ambiguous bool
		trace     func(*httptrace.ClientTrace)
	}{
		{name: "dns", err: &net.DNSError{Err: "offline", Name: "ingest.invalid"}, ambiguous: true},
		{name: "connect_refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, ambiguous: true},
		{name: "headers_written", err: io.ErrUnexpectedEOF, ambiguous: true, trace: func(trace *httptrace.ClientTrace) {
			trace.GetConn("ingest.invalid:443")
			if trace.WroteHeaders != nil {
				trace.WroteHeaders()
			}
		}},
		{name: "request_write_failed", err: io.ErrUnexpectedEOF, ambiguous: true, trace: func(trace *httptrace.ClientTrace) {
			trace.GetConn("ingest.invalid:443")
			if trace.WroteRequest != nil {
				trace.WroteRequest(httptrace.WroteRequestInfo{Err: io.ErrUnexpectedEOF})
			}
		}},
		{name: "response_lost_after_write", err: io.ErrUnexpectedEOF, ambiguous: true},
		{name: "server_failure", status: 503, ambiguous: true},
		{name: "backpressure", status: 429},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			rt := &retryWindowHTTP{status: tc.status, err: tc.err, trace: tc.trace}
			client := newSpoolTestClient(t, "https://ingest.invalid", dir, nil, func(cfg *Config) {
				cfg.HTTPClient = &http.Client{Transport: rt}
				cfg.DisableRequestCompression = true
			})
			defer client.Close(context.Background())
			client.SetConsent(true)
			const id = "a7f6be37-084d-4d76-a28d-3aac94c329f4"
			before := time.Now().UnixMilli()
			if err := client.Enqueue(Event{ID: id, Name: "retry_clock", Timestamp: time.Now().Add(24 * time.Hour)}); err != nil {
				t.Fatal(err)
			}
			if err := client.Flush(context.Background()); err == nil {
				t.Fatal("expected failed attempt")
			}
			record := readSpoolRecordFile(t, dir)
			if len(record.Events) != 1 {
				t.Fatalf("spool members=%d", len(record.Events))
			}
			entry := record.Events[0]
			if entry.IntakeUnixMS < before || entry.IntakeUnixMS > time.Now().UnixMilli() {
				t.Fatalf("intake clock came from host timestamp: %d", entry.IntakeUnixMS)
			}
			if (entry.AmbiguousUnixMS != 0) != tc.ambiguous {
				t.Fatalf("ambiguous clock=%d, want present=%v", entry.AmbiguousUnixMS, tc.ambiguous)
			}
			if tc.ambiguous {
				// A later definite rejection cannot erase the previous uncertainty.
				rt.mu.Lock()
				rt.err = nil
				rt.status = 429
				rt.mu.Unlock()
				if err := client.Flush(context.Background()); err == nil {
					t.Fatal("expected rejection")
				}
				again := readSpoolRecordFile(t, dir).Events[0]
				if again.AmbiguousUnixMS != entry.AmbiguousUnixMS {
					t.Fatalf("retry clock changed: %d -> %d", entry.AmbiguousUnixMS, again.AmbiguousUnixMS)
				}
				if !bytes.Equal(again.Raw, entry.Raw) {
					t.Fatal("retry changed envelope bytes")
				}
				rt.mu.Lock()
				defer rt.mu.Unlock()
				if len(rt.bodies) < 2 || !bytes.Equal(rt.bodies[0], rt.bodies[1]) {
					t.Fatal("live retry did not preserve exact request bytes")
				}
			}
		})
	}
}

type retryWindowForwarder func(*http.Request) (*http.Response, error)

func (f retryWindowForwarder) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestRetryWindowForwardedTypedErrorExpires(t *testing.T) {
	for _, typedError := range []error{
		&net.DNSError{Err: "proxy response lookup failed", Name: "proxy.invalid"},
		&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
	} {
		for _, traced := range []bool{false, true} {
			t.Run(fmt.Sprintf("%T/traced_%v", typedError, traced), func(t *testing.T) {
				state, server := newSpoolTestServer(t)
				defer server.Close()
				base := http.DefaultTransport.(*http.Transport).Clone()
				defer base.CloseIdleConnections()
				rt := retryWindowForwarder(func(req *http.Request) (*http.Response, error) {
					forwarded := req
					if !traced {
						// A supported custom transport may perform its own request
						// without propagating the SDK's trace context.
						ctx, cancel := context.WithTimeout(context.Background(), time.Second)
						defer cancel()
						forwarded = req.Clone(ctx)
					}
					resp, err := base.RoundTrip(forwarded)
					if err != nil || req.URL.Path != "/v1/events:batch" {
						return resp, err
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					return nil, typedError
				})
				dir := t.TempDir()
				client := newSpoolTestClient(t, server.URL, dir, nil, func(cfg *Config) {
					cfg.HTTPClient = &http.Client{Transport: rt}
				})
				defer client.Close(context.Background())
				client.SetConsent(true)
				const id = "aa5b99d4-720b-48ac-82d7-e14ce815558a"
				if err := client.Enqueue(Event{ID: id, Name: "forwarded_then_error"}); err != nil {
					t.Fatal(err)
				}
				if err := client.Flush(context.Background()); err == nil {
					t.Fatal("expected the forwarded request's typed error")
				}
				arrivals := state.allArrivals()
				if len(arrivals) != 1 || len(arrivals[0]) != 1 || arrivals[0][0] != id {
					t.Fatalf("receiver did not observe original event: %v", arrivals)
				}
				record := readSpoolRecordFile(t, dir)
				if len(record.Events) != 1 {
					t.Fatalf("spool members=%d", len(record.Events))
				}
				if record.Events[0].AmbiguousUnixMS == 0 {
					t.Error("forwarded typed error erased the ambiguous clock")
				}
				// Advance only the loader's time. The saved envelope and both
				// clocks are the real attempt's unchanged durable output. Use
				// a copy so the loader cannot modify the live client's store.
				data, err := os.ReadFile(filepath.Join(dir, spoolFileName))
				if err != nil {
					t.Fatal(err)
				}
				restartConfig := client.cfg
				restartConfig.SpoolDir = t.TempDir()
				if err := os.WriteFile(filepath.Join(restartConfig.SpoolDir, spoolFileName), data, 0600); err != nil {
					t.Fatal(err)
				}
				restarted := newDiskSpool(restartConfig)
				outcome := restarted.load(time.Now().Add(16 * time.Minute))
				if len(outcome.expired) != 1 || restarted.hasResendWork() {
					t.Fatal("forwarded event remained retryable after sixteen minutes")
				}
			})
		}
	}
}

func TestRetryWindowStandardTransportDialFailureDoesNotStartClock(t *testing.T) {
	for _, dialError := range []error{
		&net.DNSError{Err: "offline", Name: "ingest.invalid"},
		&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
	} {
		t.Run(fmt.Sprintf("%T", dialError), func(t *testing.T) {
			var dialCalls atomic.Int64
			rt := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
				dialCalls.Add(1)
				return nil, dialError
			}}
			defer rt.CloseIdleConnections()
			dir := t.TempDir()
			client := newSpoolTestClient(t, "https://ingest.invalid", dir, nil, func(cfg *Config) {
				cfg.HTTPClient = &http.Client{Transport: rt}
			})
			defer client.Close(context.Background())
			client.SetConsent(true)
			if err := client.Enqueue(Event{Name: "pre_connection_failure"}); err != nil {
				t.Fatal(err)
			}
			if err := client.Flush(context.Background()); err == nil {
				t.Fatal("expected pre-connection failure")
			}
			record := readSpoolRecordFile(t, dir)
			if dialCalls.Load() == 0 || len(record.Events) != 1 || record.Events[0].AmbiguousUnixMS != 0 {
				t.Fatalf("traced dial failure started a clock: dials=%d, record=%+v", dialCalls.Load(), record)
			}
		})
	}
}

func TestRetryWindowTLSFailureDoesNotStartClock(t *testing.T) {
	var arrivals atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { arrivals.Add(1) }))
	defer server.Close()
	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	defer client.Close(context.Background())
	client.SetConsent(true)
	if err := client.Enqueue(Event{Name: "tls_offline"}); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatal("expected untrusted TLS failure")
	}
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 1 || record.Events[0].AmbiguousUnixMS != 0 || arrivals.Load() != 0 {
		t.Fatalf("TLS pre-send failure started clock or reached HTTP: %+v, arrivals=%d", record, arrivals.Load())
	}
}

func TestRetryWindowLiveExpiryKeepsFreshBatchmate(t *testing.T) {
	now := time.Now()
	clock := &stubClock{now: now}
	recorder := &spoolDeadLetterRecorder{}
	c := &Client{clock: clock, cfg: Config{OnSpoolDeadLetter: recorder.record}}
	expired, _ := c.prepareEvent(Event{ID: "expired", Name: "expired", Timestamp: now.Add(time.Hour)})
	fresh, _ := c.prepareEvent(Event{ID: "fresh", Name: "fresh"})
	expired.delivery.ambiguousMS.Store(now.UnixMilli())
	clock.now = now.Add(16 * time.Minute)
	request, err := c.buildBatch([]Event{expired, fresh})
	if err != nil {
		t.Fatal(err)
	}
	kept, events := c.expireBuiltBatch(request, []Event{expired, fresh})
	if len(events) != 1 || events[0].ID != "fresh" || len(kept.rawEvents) != 1 || !bytes.Equal(kept.rawEvents[0], request.rawEvents[1]) {
		t.Fatal("expiry did not isolate the ambiguous member")
	}
	if c.Snapshot().SpoolExpired != 1 || len(recorder.byReason(SpoolDropExpired)) != 1 {
		t.Fatal("live expiry was not counted and reported once")
	}
}

func TestRetryWindowClockFenceBeforeResend(t *testing.T) {
	now := time.Now()
	dir := t.TempDir()
	s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: 10, SpoolMaxBytes: 1 << 20})
	entry := spoolEntryFromEnvelope(t, spoolTestEnvelope(t, "clock-fence", now))
	_, _, _, _, failed := s.append([]spoolEntry{entry}, 0, false, now, func() bool { return true })
	if failed {
		t.Fatal("seed write failed")
	}
	rt := &retryWindowHTTP{status: 503}
	c := &Client{clock: &stubClock{now: now}, spool: s}
	transport := &httpTransport{client: &http.Client{Transport: rt}, attemptTimeout: time.Second, compressionOptOut: true, endpoint: "https://ingest.invalid/v1/events:batch"}
	restore := breakSpoolWrites(s, errors.New("clock fence refused"))
	ctx, cancel := c.analyticsAttemptContext(context.Background(), spoolChunkRequest([]spoolEntry{entry}))
	_, err := transport.Publish(ctx, spoolChunkRequest([]spoolEntry{entry}))
	cancel()
	rt.mu.Lock()
	calls := len(rt.bodies)
	rt.mu.Unlock()
	if calls != 0 {
		t.Fatal("request escaped failed durable clock fence")
	}
	if err == nil || !strings.Contains(err.Error(), "clock fence refused") {
		t.Fatalf("expected clock fence error, got %v", err)
	}

	restore()
	ctx, cancel = c.analyticsAttemptContext(context.Background(), spoolChunkRequest([]spoolEntry{entry}))
	_, err = transport.Publish(ctx, spoolChunkRequest([]spoolEntry{entry}))
	cancel()
	if err == nil {
		t.Fatal("expected server failure")
	}
	saved := readSpoolRecordFile(t, dir).Events[0]
	if saved.AmbiguousUnixMS != now.UnixMilli() {
		t.Fatalf("clock not durable: %+v", saved)
	}
	// Model abrupt exit after a sent request: no completion/ack rewrite.
	restarted := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: 10, SpoolMaxBytes: 1 << 20})
	outcome := restarted.load(now.Add(16 * time.Minute))
	if len(outcome.expired) != 1 || restarted.hasResendWork() {
		t.Fatal("restart lost the durable retry fence")
	}
}

func TestRetryWindowResendHandoff(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		t.Run(fmt.Sprintf("automatic_%v", automatic), func(t *testing.T) {
			now := time.Now()
			clock := &stubClock{now: now}
			dir := t.TempDir()
			s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: 10, SpoolMaxBytes: 1 << 20})
			old := spoolEntryFromEnvelope(t, spoolTestEnvelope(t, "handoff-old", now))
			fresh := spoolEntryFromEnvelope(t, spoolTestEnvelope(t, "handoff-fresh", now))
			old.delivery.ambiguousMS.Store(now.UnixMilli())
			_, _, _, _, failed := s.append([]spoolEntry{old, fresh}, 0, false, now, func() bool { return true })
			if failed {
				t.Fatal("seed write failed")
			}
			s.resend = []spoolEntry{old, fresh}
			rt := &retryWindowHTTP{status: 202}
			c := &Client{clock: clock, spool: s, cfg: Config{BatchSize: 4}, transport: &httpTransport{client: &http.Client{Transport: rt}, attemptTimeout: time.Second, compressionOptOut: true, endpoint: "https://ingest.invalid/v1/events:batch"}}
			c.spoolResendHandoffSeam = func([]spoolEntry) { clock.now = now.Add(16 * time.Minute) }
			backoff := 0
			if automatic {
				var deadline time.Time
				if !c.resendSpooledChunks(&deadline, &backoff) {
					t.Fatal("handoff expiry deferred fresh member")
				}
			} else if err := c.flushSpooledChunks(context.Background(), &backoff); err != nil {
				t.Fatal(err)
			}
			rt.mu.Lock()
			defer rt.mu.Unlock()
			if len(rt.bodies) != 1 || bytes.Contains(rt.bodies[0], []byte("handoff-old")) || !bytes.Contains(rt.bodies[0], []byte("handoff-fresh")) {
				t.Fatalf("handoff sent wrong population: %s", rt.bodies)
			}
			if c.Snapshot().SpoolExpired != 1 {
				t.Fatal("handoff expiry was not counted once")
			}
		})
	}
}

func TestRetryWindowMissingMetadataExpires(t *testing.T) {
	now := time.Now()
	dir := t.TempDir()
	raw := spoolTestEnvelope(t, "unverifiable", now)
	legacy := spoolRecordWire{Version: 2, Events: []spoolEventWire{{Raw: raw}}}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, spoolFileName), data, 0600); err != nil {
		t.Fatal(err)
	}
	s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: 10, SpoolMaxBytes: 1 << 20})
	outcome := s.load(now)
	if len(outcome.expired) != 1 || s.hasResendWork() {
		t.Fatal("legacy record acquired a fresh retry clock")
	}
}
