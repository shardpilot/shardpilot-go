package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	shardpilot "github.com/shardpilot/shardpilot-go"
	"github.com/shardpilot/shardpilot-go/internal/redact"
	"github.com/shardpilot/shardpilot-go/pkg/crash"
)

func main() { os.Exit(run(os.Getenv, os.Stdout, http.DefaultTransport)) }

const bodyLimit = 64 << 10

type evidenceWriter struct {
	io.Writer
	err error
}

func (w *evidenceWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.Writer.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}

// Each case owns a client and a single request budget. SDK retries cannot turn
// an evidence probe into an unbounded publisher. Redirects are also refused.
type witness struct {
	mu              sync.Mutex
	base            http.RoundTripper
	out             io.Writer
	redact          *redact.Redactor
	expectedActor   string
	name            string
	unauthenticated bool
	attempted       bool
	records         []exchange
	done            chan struct{}
}

type exchange struct {
	Case         string  `json:"case"`
	Method       string  `json:"method"`
	Path         string  `json:"path"`
	RequestBody  string  `json:"request_body"`
	RequestBytes int     `json:"request_bytes"`
	Status       int     `json:"status"`
	ResponseBody string  `json:"response_body"`
	RequestID    string  `json:"request_id"`
	LatencyMS    float64 `json:"latency_ms"`
	Error        string  `json:"error,omitempty"`
}

func (w *witness) RoundTrip(req *http.Request) (*http.Response, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.attempted {
		return nil, fmt.Errorf("case request budget exhausted; no retry sent")
	}
	w.attempted = true
	defer close(w.done)
	body, err := io.ReadAll(io.LimitReader(req.Body, bodyLimit+1))
	if err != nil || len(body) > bodyLimit {
		return nil, fmt.Errorf("cannot record bounded request")
	}
	_ = req.Body.Close()
	req = req.Clone(req.Context())
	req.Body = io.NopCloser(bytes.NewReader(body))
	if w.unauthenticated {
		req.Header.Del("Authorization")
	}
	e := exchange{Case: w.name, Method: req.Method, Path: req.URL.Path, RequestBody: string(body), RequestBytes: len(body)}
	start := time.Now()
	resp, err := w.base.RoundTrip(req)
	if resp != nil {
		e.Status, e.RequestID = resp.StatusCode, resp.Header.Get("X-Request-Id")
		if resp.Body == nil {
			err = fmt.Errorf("response has no body")
		} else {
			var data []byte
			data, err = io.ReadAll(io.LimitReader(resp.Body, bodyLimit+1))
			_ = resp.Body.Close()
			if len(data) > bodyLimit {
				data = data[:bodyLimit]
				err = fmt.Errorf("response exceeds evidence limit; body truncated")
			}
			e.ResponseBody = string(data)
			resp.Body = io.NopCloser(bytes.NewReader(data))
		}
	}
	e.LatencyMS = float64(time.Since(start).Microseconds()) / 1000
	if err != nil {
		e.Error = err.Error()
	}
	w.records = append(w.records, e)
	// Keep the exact wire bytes for evaluation; redact only the printed copy.
	e.RequestBody = w.redact.Replace(e.RequestBody)
	e.ResponseBody = w.redact.Replace(e.ResponseBody)
	e.RequestID = w.redact.Replace(e.RequestID)
	e.Error = w.redact.Replace(e.Error)
	if writeErr := json.NewEncoder(w.out).Encode(e); writeErr != nil {
		return nil, fmt.Errorf("write evidence: %w", writeErr)
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (w *witness) outcome(kind string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.records) != 1 {
		return fmt.Errorf("expected one observed request, got %d", len(w.records))
	}
	e := w.records[0]
	if e.Error != "" {
		return fmt.Errorf("incomplete HTTP exchange")
	}
	if kind == "unauthenticated" {
		if e.Status == 401 || e.Status == 403 {
			return nil
		}
		return fmt.Errorf("unauthenticated request was not refused with401/403")
	}
	if e.Status < 200 || e.Status >= 300 {
		return fmt.Errorf("unexpected HTTP status%d", e.Status)
	}
	if kind == "crash" {
		var sent struct {
			ID          string `json:"crash_id"`
			AnonymousID string `json:"anonymous_id"`
		}
		var got struct {
			ID          string `json:"crash_id"`
			Fingerprint string `json:"fingerprint"`
			Suppressed  bool   `json:"suppressed"`
		}
		if json.Unmarshal([]byte(e.RequestBody), &sent) != nil || json.Unmarshal([]byte(e.ResponseBody), &got) != nil || sent.ID == "" || got.ID != sent.ID || strings.TrimSpace(got.Fingerprint) == "" || got.Suppressed {
			return fmt.Errorf("crash acknowledgement missing, mismatched or suppressed")
		}
		if w.expectedActor == "" || sent.AnonymousID != w.expectedActor {
			return fmt.Errorf("crash request lost or changed the configured actor")
		}
		return nil
	}
	if e.Status != 202 {
		return fmt.Errorf("expected batch HTTP202")
	}
	var sent struct {
		Events []json.RawMessage `json:"events"`
	}
	var got struct {
		Events []struct {
			ID     string `json:"event_id"`
			Status string `json:"status"`
			Code   string `json:"code"`
		} `json:"events"`
	}
	if json.Unmarshal([]byte(e.RequestBody), &sent) != nil || json.Unmarshal([]byte(e.ResponseBody), &got) != nil || len(sent.Events) == 0 || len(got.Events) != len(sent.Events) {
		return fmt.Errorf("missing complete per-event verdicts")
	}
	count := 1
	if w.name == "realistic-batch" {
		count = 4
	}
	if kind == "mixed" {
		count = 2
	}
	if len(sent.Events) != count {
		return fmt.Errorf("case sent %d events, expected %d", len(sent.Events), count)
	}
	expected := make(map[string]bool)
	large := 0
	for _, raw := range sent.Events {
		var event struct {
			ID string `json:"event_id"`
		}
		if json.Unmarshal(raw, &event) != nil || event.ID == "" {
			return fmt.Errorf("invalid sent event identity")
		}
		oversize := len(raw) > 2048
		if oversize {
			large++
		}
		expected[event.ID] = oversize
	}
	if kind == "mixed" && (len(sent.Events) != 2 || large != 1) {
		return fmt.Errorf("mixed scene must contain one >2048-byte event and one small event")
	}
	for _, event := range got.Events {
		oversize, exists := expected[event.ID]
		if !exists {
			return fmt.Errorf("unknown or repeated response event id")
		}
		delete(expected, event.ID)
		if oversize && kind == "mixed" {
			if event.Status != "rejected" || event.Code != "event_too_large" {
				return fmt.Errorf("oversize event lacks event_too_large rejection")
			}
		} else if event.Status != "accepted" {
			return fmt.Errorf("event not accepted (status=%s)", event.Status)
		}
	}
	return nil
}

func run(getenv func(string) string, out io.Writer, transport http.RoundTripper) int {
	log := &evidenceWriter{Writer: out}
	out = log
	names := []string{"SHARDPILOT_INGEST_URL", "SHARDPILOT_TOKEN", "SHARDPILOT_WORKSPACE_ID", "SHARDPILOT_APP_ID", "SHARDPILOT_ENVIRONMENT_ID", "SHARDPILOT_ANONYMOUS_ID", "SHARDPILOT_CRASH_INGEST_URL", "SHARDPILOT_API_KEY"}
	values := make(map[string]string)
	for _, name := range names {
		values[name] = strings.TrimSpace(getenv(name))
		if values[name] == "" {
			fmt.Fprintf(out, "configuration: %s is required; exit2\n", name)
			return 2
		}
	}
	for _, name := range []string{"SHARDPILOT_INGEST_URL", "SHARDPILOT_CRASH_INGEST_URL"} {
		u, err := url.Parse(values[name])
		if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "https" && u.Scheme != "http") {
			fmt.Fprintf(out, "configuration: %s must be an HTTP(S) origin without credentials, path, query or fragment; exit2\n", name)
			return 2
		}
		ip := net.ParseIP(u.Hostname())
		if u.Scheme == "http" && !strings.EqualFold(u.Hostname(), "localhost") && (ip == nil || !ip.IsLoopback()) {
			fmt.Fprintf(out, "configuration: %s requires HTTPS outside loopback; exit2\n", name)
			return 2
		}
	}
	actor, err := crash.SanitizeEvent(crash.Event{AnonymousID: values["SHARDPILOT_ANONYMOUS_ID"]})
	if err != nil || actor.AnonymousID != values["SHARDPILOT_ANONYMOUS_ID"] {
		fmt.Fprintln(out, "configuration: SHARDPILOT_ANONYMOUS_ID must survive the crash SDK actor sanitizer unchanged; exit2")
		return 2
	}
	redact := redact.New(values["SHARDPILOT_TOKEN"], values["SHARDPILOT_API_KEY"])
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		fmt.Fprintln(out, "cannot generate synthetic run id; exit2")
		return 2
	}
	runID := "sdk-evidence-" + hex.EncodeToString(random[:])
	fmt.Fprintf(out, "run_id=%s; synthetic telemetry only; HTTP acceptance does not prove downstream visibility\n", runID)
	failed := false
	caseRun := func(name, kind string, action func(*http.Client) error) {
		w := &witness{base: transport, out: out, redact: redact, expectedActor: values["SHARDPILOT_ANONYMOUS_ID"], name: name, unauthenticated: kind == "unauthenticated", done: make(chan struct{})}
		hc := &http.Client{Transport: w, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		err := action(hc)
		check := w.outcome(kind)
		if err != nil && kind != "unauthenticated" {
			failed = true
		}
		if check != nil {
			failed = true
		}
		fmt.Fprintf(out, "case=%s sdk_error=%s expectation_error=%s\n", name, redact.Replace(fmt.Sprint(err)), redact.Replace(fmt.Sprint(check)))
	}
	event := func(id, name, session string, sequence int64, props map[string]any) shardpilot.Event {
		return shardpilot.Event{ID: runID + "-" + id, Name: name, SessionID: runID + "-" + session, SessionSequence: sequence, Props: props}
	}
	analytics := func(name, kind string, events []shardpilot.Event, synchronous bool) {
		caseRun(name, kind, func(hc *http.Client) error {
			c, err := shardpilot.NewClient(shardpilot.Config{IngestURL: values["SHARDPILOT_INGEST_URL"], Token: values["SHARDPILOT_TOKEN"], WorkspaceID: values["SHARDPILOT_WORKSPACE_ID"], AppID: values["SHARDPILOT_APP_ID"], EnvironmentID: values["SHARDPILOT_ENVIRONMENT_ID"], AnonymousID: values["SHARDPILOT_ANONYMOUS_ID"], Source: shardpilot.SourceClient, BatchSize: len(events), FlushInterval: time.Hour, HTTPTimeout: 15 * time.Second, HTTPClient: hc, DisableRequestCompression: true})
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if synchronous {
				err = c.Track(ctx, events[0])
			} else {
				for _, e := range events {
					if err = c.Enqueue(e); err != nil {
						break
					}
				}
				if err == nil {
					select {
					case <-hc.Transport.(*witness).done:
					case <-ctx.Done():
						err = ctx.Err()
					}
				}
			}
			closeErr := c.Close(ctx)
			if err != nil {
				return err
			}
			return closeErr
		})
	}
	analytics("single", "accepted", []shardpilot.Event{event("single", "app.session_started", "single", 1, nil)}, true)
	analytics("realistic-batch", "accepted", []shardpilot.Event{
		event("batch-start-a", "app.session_started", "batch-a", 1, map[string]any{"entry_point": "menu"}),
		event("batch-end-a", "app.session_ended", "batch-a", 2, map[string]any{"duration_ms": 12000, "reason": "completed"}),
		event("batch-start-b", "app.session_started", "batch-b", 1, map[string]any{"entry_point": "resume"}),
		event("batch-end-b", "app.session_ended", "batch-b", 2, map[string]any{"duration_ms": 8000, "reason": "background"}),
	}, false)
	analytics("mixed-size", "mixed", []shardpilot.Event{event("large", "app.session_started", "large", 1, map[string]any{"synthetic_padding": strings.Repeat("x", 3072)}), event("small", "app.session_started", "small", 1, nil)}, false)
	analytics("unauthenticated", "unauthenticated", []shardpilot.Event{event("unauth", "app.session_started", "unauth", 1, nil)}, true)
	for _, kind := range []string{"go-panic", "native-json", "raw-text"} {
		caseRun(kind, "crash", func(hc *http.Client) error {
			c, err := crash.NewClient(crash.ClientOptions{IngestURL: values["SHARDPILOT_CRASH_INGEST_URL"], APIKey: values["SHARDPILOT_API_KEY"], App: crash.AppInfo{ID: values["SHARDPILOT_APP_ID"], Version: "synthetic", BuildID: "sdk-evidence"}, Source: "sdk-evidence", AnonymousID: values["SHARDPILOT_ANONYMOUS_ID"], SessionID: runID, HTTPClient: hc, MaxAttempts: 1})
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			c.RecordBreadcrumb("synthetic-test-started")
			if kind == "go-panic" {
				func() { defer func() { c.CapturePanic(ctx, recover()) }(); panic("synthetic evidence panic") }()
				return nil // CapturePanic has no error return; the witness requires a real acknowledgement.
			}
			e := crash.Event{Platform: "linux", OS: crash.OSInfo{Name: "linux"}, Exception: crash.ExceptionInfo{Type: "synthetic_fault", Reason: "SDK evidence fixture"}}
			if kind == "native-json" {
				e.Modules = []crash.Module{{ID: "fixture", Name: "synthetic-module", DebugID: "AABBCCDDEEFF00112233445566778899", LoadAddress: "0x400000"}}
				e.Threads = []crash.Thread{{ID: "main", Crashed: true, Frames: []crash.Frame{{ModuleID: "fixture", InstructionAddress: "0x401000"}}}}
			} else {
				e.RawText = "synthetic_fault\n  at synthetic_work (fixture.go:12)"
			}
			return c.EmitFatal(ctx, e)
		})
	}
	if failed || log.err != nil {
		fmt.Fprintln(out, "one or more evidence expectations failed; exit1")
		return 1
	}
	fmt.Fprintln(out, "all seven request expectations met; downstream visibility and symbolication still require readback; exit0")
	if log.err != nil {
		return 1
	}
	return 0
}
