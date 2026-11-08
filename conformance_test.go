package shardpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shardpilot/shardpilot-go/internal/consentpolicy"
	"github.com/shardpilot/shardpilot-go/pkg/crash"
)

// Decode fixture expectations independently of SDK response wire types.
type conformanceReply struct {
	Accepted, Rejected, Duplicates int
	Events                         []struct {
		EventID               string `json:"event_id"`
		Status, Code, Message string
	}
	Recorded, Replayed bool
	Error              struct {
		Code, Message string
		Details       []struct{ Field, Code, Message string }
	}
}

type conformanceCrashReply struct {
	CrashID     string `json:"crash_id"`
	Fingerprint string
	Suppressed  bool
	Warnings    []string
}

type conformanceHTTP struct {
	t      *testing.T
	c      conformanceCase
	files  map[string][]byte
	mu     sync.Mutex
	seen   []*http.Request
	bodies [][]byte
	seed   bool
}

func (w *conformanceHTTP) RoundTrip(r *http.Request) (*http.Response, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	body := []byte(nil)
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
	}
	if r.URL.Host != "conformance.example.invalid" {
		return nil, fmt.Errorf("unexpected origin %s", r.URL.Host)
	}
	w.seen = append(w.seen, r.Clone(r.Context()))
	w.bodies = append(w.bodies, body)
	side := w.c.Response
	if w.seed {
		side = conformanceSide{Status: 200, Headers: map[string]string{"ETag": w.c.Request.Headers["If-None-Match"]}, File: "bodies/rc.two-key-200.response"}
	}
	headers := http.Header{}
	for k, v := range side.Headers {
		headers.Set(k, v)
	}
	return &http.Response{StatusCode: side.Status, Header: headers, Body: io.NopCloser(bytes.NewReader(w.files["v1/"+side.File])), Request: r}, nil
}

type conformanceConsentResult struct {
	result consentResult
	err    error
}
type conformanceConsentProbe struct {
	transport
	done chan conformanceConsentResult
}

func (p conformanceConsentProbe) PublishConsent(ctx context.Context, request consentRequest) (consentResult, error) {
	r, err := p.transport.PublishConsent(ctx, request)
	p.done <- conformanceConsentResult{r, err}
	return r, err
}

func conformanceEqual(t *testing.T, label string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: got %#v, want %#v", label, got, want)
	}
}

func conformanceMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func conformanceError(t *testing.T, err error, c conformanceCase, raw []byte) {
	t.Helper()
	if c.Response.Status < 300 {
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	var got *HTTPStatusError
	if !errors.As(err, &got) {
		t.Fatalf("expected typed status %d, got %v", c.Response.Status, err)
	}
	var want conformanceReply
	if e := json.Unmarshal(raw, &want); e != nil {
		t.Fatal(e)
	}
	conformanceEqual(t, "status", got.StatusCode, c.Response.Status)
	conformanceEqual(t, "error code", got.ErrorCode, want.Error.Code)
	conformanceEqual(t, "error message", got.ErrorMessage, want.Error.Message)
	var retryAfter time.Duration
	if c.Response.Status == http.StatusTooManyRequests || c.Response.Status >= 500 {
		seconds, err := strconv.ParseUint(strings.TrimSpace(c.Response.Headers["Retry-After"]), 10, 64)
		if err == nil {
			retryAfter = time.Duration(min(seconds, 24*60*60)) * time.Second
		}
	}
	conformanceEqual(t, "retry after", got.RetryAfter, retryAfter)
	conformanceEqual(t, "detail count", len(got.Details), len(want.Error.Details))
	for i, d := range want.Error.Details {
		conformanceEqual(t, "error detail", got.Details[i], ErrorDetail{d.Field, d.Code, d.Message})
	}
}

// PASS is a fixed case-ID set, so a request regression cannot become a new unsupported case.
func conformanceRequest(t *testing.T, w *conformanceHTTP, credential string) bool {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	conformanceEqual(t, "actual delivery count", len(w.seen), 1)
	r, body, c := w.seen[0], w.bodies[0], w.c
	conformanceEqual(t, "method", r.Method, c.Request.Method)
	conformanceEqual(t, "complete target", r.URL.RequestURI(), c.Request.Path)
	values := func(name string) []string {
		var all []string
		for key, list := range r.Header {
			if strings.EqualFold(key, name) {
				all = append(all, list...)
			}
		}
		return all
	}
	conformanceEqual(t, "credential", values("Authorization"), []string{"Bearer " + credential})
	for _, name := range []string{"Content-Type", "Cache-Control", "ETag", "If-None-Match", "Retry-After", "Vary", "X-ShardPilot-Anonymous-Id"} {
		var want []string
		if v, exists := c.Request.Headers[name]; exists {
			want = []string{v}
		}
		conformanceEqual(t, "approved header "+name, values(name), want)
	}
	exact := conformanceRequestDispositions()[c.ID] == "PASS"
	if exact {
		conformanceEqual(t, "raw body", string(body), string(w.files["v1/"+c.Request.File]))
	}
	if c.Request.Method == "POST" && bytes.Equal(body, w.files["v1/"+c.Request.File]) {
		t.Fatal("request capability changed: review the explicit unsupported disposition")
	}
	return exact
}

func conformanceBatch(t *testing.T, c *Client, w *conformanceHTTP, captured, reply []byte) {
	t.Helper()
	var request struct{ Events []map[string]any }
	if json.Unmarshal(captured, &request) != nil {
		request.Events = []map[string]any{{"event_id": "6f1c2a52-8f4e-4c7a-9d10-2b3c4d5e6f70", "event_name": "conformance_probe", "event_ts": "2000-01-01T00:00:00Z"}}
	}
	text := func(m map[string]any, k string) string { v, _ := m[k].(string); return v }
	var got BatchResult
	calls := 0
	c.cfg.OnBatchResult = func(r BatchResult) { got = r; calls++ }
	events := make([]Event, 0, len(request.Events))
	for _, e := range request.Events {
		// The public API reserves app.* names; replay responses with a valid probe.
		e["event_name"] = "conformance_probe"
		stamp, err := time.Parse(time.RFC3339, text(e, "event_ts"))
		if err != nil {
			t.Fatal(err)
		}
		event := Event{ID: text(e, "event_id"), Name: text(e, "event_name"), Timestamp: stamp, UserID: text(e, "user_id"), AnonymousID: text(e, "anonymous_id"), SessionID: text(e, "session_id")}
		if n, ok := e["session_sequence"].(float64); ok {
			event.SessionSequence = int64(n)
		}
		event.Props, _ = e["props"].(map[string]any)
		events = append(events, event)
	}
	err := c.publishBatchWithContext(context.Background(), events)
	conformanceError(t, err, w.c, reply)
	if w.c.Response.Status < 300 {
		var expected conformanceReply
		if err := json.Unmarshal(reply, &expected); err != nil {
			t.Fatal(err)
		}
		conformanceEqual(t, "batch callback count", calls, 1)
		want := BatchResult{Accepted: expected.Accepted, Rejected: expected.Rejected, Duplicates: expected.Duplicates}
		for _, e := range expected.Events {
			want.Events = append(want.Events, BatchEventStatus{e.EventID, EventStatus(e.Status), e.Code, e.Message})
		}
		conformanceEqual(t, "complete batch outcome", got, want)
		stats := c.Snapshot()
		conformanceEqual(t, "accepted", stats.Accepted, uint64(expected.Accepted))
		conformanceEqual(t, "rejected", stats.Rejected, uint64(expected.Rejected))
		conformanceEqual(t, "duplicates", stats.Duplicates, uint64(expected.Duplicates))
		counts := map[EventStatus]uint64{}
		for _, e := range expected.Events {
			counts[EventStatus(e.Status)]++
		}
		for status, n := range counts {
			conformanceEqual(t, "status "+string(status), stats.ByStatus[status], n)
		}
	} else {
		conformanceEqual(t, "refused callback count", calls, 0)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.bodies) != 1 {
		t.Fatalf("batch deliveries %d", len(w.bodies))
	}
	actual := conformanceMap(t, w.bodies[0])["events"].([]any)
	conformanceEqual(t, "event count", len(actual), len(request.Events))
	for i, e := range request.Events {
		item := actual[i].(map[string]any)
		for _, k := range []string{"event_id", "event_name", "event_ts", "user_id", "anonymous_id", "session_id", "session_sequence", "props"} {
			conformanceEqual(t, "event "+k, item[k], e[k])
		}
		for k, v := range map[string]string{"workspace_key": c.cfg.WorkspaceKey, "app_key": c.cfg.AppKey, "environment_key": c.cfg.EnvironmentKey, "source": string(c.cfg.Source)} {
			conformanceEqual(t, "envelope "+k, item[k], v)
		}
		if _, exists := item["app_id"]; exists {
			t.Fatal("retired field emitted")
		}
	}
}

func conformanceConsent(t *testing.T, c *Client, w *conformanceHTTP, captured, reply []byte) {
	t.Helper()
	input := conformanceMap(t, captured)
	analytics := input["categories"].(map[string]any)["analytics"].(bool)
	p := conformanceConsentProbe{c.transport, make(chan conformanceConsentResult, 1)}
	c.transport = p
	var notices []ConsentNotice
	if n, ok := input["notice_version"].(string); ok {
		notices = append(notices, ConsentNotice{NoticeVersion: n, NoticeLocale: input["notice_locale"].(string), PolicyVersion: input["policy_version"].(string)})
	}
	if _, err := c.SetConsent(analytics, notices...); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-p.done:
		conformanceError(t, got.err, w.c, reply)
		if got.err == nil {
			var want conformanceReply
			if err := json.Unmarshal(reply, &want); err != nil {
				t.Fatal(err)
			}
			conformanceEqual(t, "parsed consent receipt", got.result, consentResult{Recorded: want.Recorded, Replayed: want.Replayed})
		}
	case <-time.After(5 * time.Second):
		t.Fatal("consent not delivered and settled")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.bodies) != 1 {
		t.Fatalf("consent deliveries %d", len(w.bodies))
	}
	got := conformanceMap(t, w.bodies[0])
	for _, k := range []string{"workspace_key", "app_key", "environment_key", "categories", "notice_version", "notice_locale", "policy_version"} {
		conformanceEqual(t, "consent "+k, got[k], input[k])
	}
	conformanceEqual(t, "consent actor", got["actor_identifier"], c.cfg.AnonymousID)
	conformanceEqual(t, "consent kind", got["kind"], "anon")
	if stamp, ok := got["decided_at"].(string); !ok {
		t.Fatal("no decided_at")
	} else if _, err := time.Parse(time.RFC3339Nano, stamp); err != nil {
		t.Fatal(err)
	}
	if key, ok := got["idempotency_key"].(string); !ok || key == "" {
		t.Fatal("no receipt key")
	}
	if _, exists := got["app_id"]; exists {
		t.Fatal("retired consent field emitted")
	}
}

func conformanceRemoteConfig(t *testing.T, c *Client, w *conformanceHTTP) {
	t.Helper()
	if w.c.Request.Headers["X-ShardPilot-Anonymous-Id"] != "" {
		c.consent.Store(consentStateGranted)
	}
	if w.c.Request.Headers["If-None-Match"] != "" {
		w.seed = true
		if _, err := c.FetchRemoteConfig(context.Background()); err != nil {
			t.Fatal(err)
		}
		w.mu.Lock()
		conformanceEqual(t, "seed delivery", len(w.seen), 1)
		w.seen = nil
		w.bodies = nil
		w.seed = false
		w.mu.Unlock()
	}
	result, err := c.FetchRemoteConfig(context.Background())
	s := w.c.Response.Status
	if s == 200 || s == 304 {
		if err != nil {
			t.Fatal(err)
		}
		want := RemoteConfigResult{
			FromCache:  s == 304,
			Reason:     "",
			Values:     map[string]any{"feature.matchmaking.enabled": true},
			Version:    1,
			HasVersion: true,
		}
		conformanceEqual(t, "config result", result, want)
		conformanceEqual(t, "config getter", c.RemoteConfigBool("feature.matchmaking.enabled", false), true)
	} else {
		reason := map[int]string{401: "unauthorized", 400: "http_400", 404: "remote_config_not_published", 429: "transient_429", 503: "transient_503"}[s]
		if err == nil || err.Error() != "shardpilot remote config fetch failed: "+reason {
			t.Fatalf("expected %s, got %v", reason, err)
		}
		conformanceEqual(t, "refused config result", result, RemoteConfigResult{})
		conformanceEqual(t, "refused config getter", c.RemoteConfigBool("feature.matchmaking.enabled", false), false)
		if raw := w.c.Response.Headers["Retry-After"]; raw != "" {
			n, e := strconv.Atoi(raw)
			if e != nil {
				t.Fatal(e)
			}
			c.rc.mu.Lock()
			window := c.rc.retryWindows[c.rc.anonymousScope]
			c.rc.mu.Unlock()
			left := time.Until(window.until)
			if left <= time.Duration(n-1)*time.Second || left > time.Duration(n)*time.Second {
				t.Fatalf("Retry-After %s produced %s", raw, left)
			}
		}
	}
}

func conformanceCrash(t *testing.T, w *conformanceHTTP, captured, reply []byte, httpClient *http.Client) {
	t.Helper()
	var event crash.Event
	if err := json.Unmarshal(captured, &event); err != nil {
		t.Fatal(err)
	}
	// Captured synthetic IDs and base-only modules cannot pass this SDK's intake.
	event.CrashID = "0190a2b4-7c5d-7e8f-9a0b-1c2d3e4f5a6b"
	for i := range event.Modules {
		event.Modules[i].LoadAddress = event.Modules[i].BaseAddress
	}
	var got crash.Result
	calls := 0
	c, err := crash.NewClient(crash.ClientOptions{CrashURL: "https://conformance.example.invalid", CrashKey: "conformance_crash", HTTPClient: httpClient, MaxAttempts: 1, OnResult: func(r crash.Result) { got = r; calls++ }})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Emit(context.Background(), event)
	if w.c.Response.Status >= 300 {
		var e *crash.HTTPStatusError
		if !errors.As(err, &e) {
			t.Fatalf("expected crash refusal, got %v", err)
		}
		conformanceEqual(t, "crash status", e.StatusCode, w.c.Response.Status)
		conformanceEqual(t, "crash error", e.ErrorCode, conformanceMap(t, reply)["error"].(map[string]any)["code"])
		conformanceEqual(t, "crash callback", calls, 0)
	} else {
		if err != nil {
			t.Fatal(err)
		}
		var expected conformanceCrashReply
		if err := json.Unmarshal(reply, &expected); err != nil {
			t.Fatal(err)
		}
		want := crash.Result{CrashID: expected.CrashID, Fingerprint: expected.Fingerprint, Suppressed: expected.Suppressed, Warnings: expected.Warnings}
		conformanceEqual(t, "crash callback", calls, 1)
		conformanceEqual(t, "crash result", got, want)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.bodies) != 1 {
		t.Fatalf("crash deliveries %d", len(w.bodies))
	}
	actual := conformanceMap(t, w.bodies[0])
	conformanceEqual(t, "crash anonymous identity", actual["anonymous_id"], conformanceMap(t, captured)["anonymous_id"])
	if _, exists := actual["user_id"]; exists {
		t.Fatal("claimed user identity emitted")
	}
	if _, exists := actual["app_id"]; exists {
		t.Fatal("retired crash field emitted")
	}
}

func TestConformance(t *testing.T) {
	files := conformanceFiles(t)
	m, err := conformanceCorpus(files)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range m.Cases {
		t.Run(c.ID, func(t *testing.T) {
			mode := m.Applicability[c.Surface]["go"]
			captured, reply := files["v1/"+c.Request.File], files["v1/"+c.Response.File]
			if mode.Value == "unsupported" {
				t.Logf("request N/A; response N/A: %s; delivery 0", mode.Reason)
				return
			}
			if mode.Value == "parse_only" {
				plan, err := consentpolicy.ParsePlan(reply)
				if err != nil {
					t.Fatal(err)
				}
				conformanceEqual(t, "parsed complete plan", conformanceMap(t, mustConformanceJSON(t, plan)), conformanceMap(t, reply))
				if c.Response.Status == 400 {
					d := consentpolicy.Prepare(context.Background(), consentpolicy.VerifiedPlayerPolicy{Plan: reply, Scope: consentpolicy.Scope{WorkspaceKey: "ws_1", AppKey: "app_1", EnvironmentKey: "env_1"}})
					conformanceEqual(t, "resolver refusal", d.Reason, consentpolicy.ReasonResolverRefused)
					conformanceEqual(t, "no policy admission", d.PlanUsed(), false)
				}
				t.Logf("request N/A: %s; response PASS (parser only); delivery N/A: no resolver HTTP API", mode.Reason)
				return
			}
			w := &conformanceHTTP{t: t, c: c, files: files}
			hc := &http.Client{Transport: w}
			credential := "sp_ingest_test"
			cfg := Config{IngestURL: "https://conformance.example.invalid", ServiceToken: credential, Source: SourceBackend, WorkspaceKey: "synthetic-ws-01", AppKey: "synthetic-app-001", EnvironmentKey: "develop", BatchSize: 1000, FlushInterval: time.Hour, HTTPClient: hc, DisableRequestCompression: true}
			if c.Operation == "recordConsent" {
				cfg.AnonymousID = conformanceMap(t, captured)["actor_identifier"].(string)
				if c.Request.Credential == "publishable_key" {
					credential = "sp_pub_conformance"
					cfg.Source = SourceClient
					cfg.ServiceToken = ""
					cfg.PublishableKey = credential
				}
			}
			if c.Operation == "fetchRemoteConfigSDKConfigByKeys" {
				parts := strings.Split(c.Request.Path, "/")
				cfg.WorkspaceKey = parts[3]
				cfg.EnvironmentKey = parts[4]
				credential = "sp_pub_conformance"
				cfg.PublishableKey = credential
				cfg.AppURL = cfg.IngestURL
				cfg.RemoteConfigEnabled = true
				cfg.AnonymousID = "synthetic-anon-1"
			}
			if c.Operation == "ingestCrash" {
				conformanceCrash(t, w, captured, reply, hc)
				credential = "conformance_crash"
			} else {
				client, err := NewClient(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := client.Close(context.Background()); err != nil {
						t.Errorf("close: %v", err)
					}
				})
				switch c.Operation {
				case "batchIngestEvents":
					conformanceBatch(t, client, w, captured, reply)
				case "recordConsent":
					conformanceConsent(t, client, w, captured, reply)
				case "fetchRemoteConfigSDKConfigByKeys":
					conformanceRemoteConfig(t, client, w)
				default:
					t.Fatalf("unhandled required operation %s", c.Operation)
				}
			}
			if conformanceRequest(t, w, credential) {
				t.Log("request PASS; response PASS; delivery 1 (seed excluded)")
			} else {
				reason := conformanceRequestDispositions()[c.ID]
				t.Logf("request UNSUPPORTED: %s; response PASS; delivery 1", reason)
			}
		})
	}
}

func mustConformanceJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestConformanceIngestErrorRetryAfter(t *testing.T) {
	files := conformanceFiles(t)
	manifest, err := conformanceCorpus(files)
	if err != nil {
		t.Fatal(err)
	}
	var captured conformanceCase
	for _, c := range manifest.Cases {
		if c.ID == "batch-retired-scope-member" {
			captured = c
		}
	}
	if captured.ID == "" {
		t.Fatal("missing captured ingest refusal")
	}
	for _, control := range []struct {
		name   string
		status int
		header string
	}{
		{"absent", http.StatusServiceUnavailable, ""},
		{"explicit-zero", http.StatusServiceUnavailable, "0"},
		{"positive", http.StatusTooManyRequests, "7"},
		{"positive-server-error", http.StatusServiceUnavailable, "7"},
		{"invalid", http.StatusServiceUnavailable, "invalid"},
		{"ignored", http.StatusBadRequest, "7"},
	} {
		t.Run(control.name, func(t *testing.T) {
			c := captured
			c.Response.Status = control.status
			c.Response.Headers = map[string]string{"Content-Type": captured.Response.Headers["Content-Type"]}
			if control.header != "" {
				c.Response.Headers["Retry-After"] = control.header
			}
			w := &conformanceHTTP{t: t, c: c, files: files}
			client, err := NewClient(Config{
				IngestURL: "https://conformance.example.invalid", ServiceToken: "sp_ingest_test",
				Source: SourceBackend, WorkspaceKey: "synthetic-ws-01", AppKey: "synthetic-app-001", EnvironmentKey: "develop",
				BatchSize: 1000, FlushInterval: time.Hour, DisableRequestCompression: true,
				HTTPClient: &http.Client{Transport: w},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := client.Close(context.Background()); err != nil {
					t.Errorf("close: %v", err)
				}
			})
			conformanceBatch(t, client, w, files["v1/"+c.Request.File], files["v1/"+c.Response.File])
			t.Log("derived header control; response PASS; delivery 1")
		})
	}
}

func TestConformanceUnreviewedRequest(t *testing.T) {
	files := conformanceFiles(t)
	m, err := conformanceCorpus(files)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range m.Cases {
		if c.ID != "rc.two-key-200" {
			continue
		}
		c.ID = "rc.unreviewed-200"
		r, err := http.NewRequest(c.Request.Method, "https://conformance.example.invalid"+c.Request.Path, nil)
		if err != nil {
			t.Fatal(err)
		}
		for key, value := range c.Request.Headers {
			r.Header.Set(key, value)
		}
		r.Header.Set("Authorization", "Bearer fixture")
		w := &conformanceHTTP{c: c, files: files, seen: []*http.Request{r}, bodies: [][]byte{nil}}
		if conformanceRequest(t, w, "fixture") {
			t.Fatal("unreviewed request inherited PASS")
		}
		return
	}
	t.Fatal("positive Remote Config control missing")
}
