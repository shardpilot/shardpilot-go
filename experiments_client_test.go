package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// expScript is a programmable assignment endpoint: each request pops the
// next scripted response (the last one repeats), and every request is
// recorded for assertions.
type expScript struct {
	mu        sync.Mutex
	responses []expScriptResponse
	requests  []*http.Request
	gate      chan struct{}         // when non-nil, each handler run waits on it
	gates     map[int]chan struct{} // per-arrival-index gates (override gate)
}

type expScriptResponse struct {
	status     int
	body       string
	retryAfter string
}

func (s *expScript) push(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, expScriptResponse{status: status, body: body})
}

func (s *expScript) pushRetryAfter(status int, body, retryAfter string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, expScriptResponse{status: status, body: body, retryAfter: retryAfter})
}

func (s *expScript) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != expAssignmentRoute {
			// The ingest and remote-config routes share the test server;
			// answer them minimally.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
			return
		}
		s.mu.Lock()
		gate := s.gate
		clone := r.Clone(context.Background())
		index := len(s.requests)
		s.requests = append(s.requests, clone)
		if perIndex, ok := s.gates[index]; ok {
			gate = perIndex
		}
		var response expScriptResponse
		switch {
		case len(s.responses) == 0:
			response = expScriptResponse{status: 200, body: expAssignedBody("1")}
		case len(s.responses) == 1:
			response = s.responses[0]
		default:
			response = s.responses[0]
			s.responses = s.responses[1:]
		}
		s.mu.Unlock()
		if gate != nil {
			<-gate
		}
		if response.retryAfter != "" {
			w.Header().Set("Retry-After", response.retryAfter)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.status)
		_, _ = w.Write([]byte(response.body))
	}
}

func (s *expScript) requestCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

func (s *expScript) request(i int) *http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[i]
}

// expFakeClock is a settable clock for cadence tests.
type expFakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *expFakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *expFakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// expWireCapture records every envelope the ingest route receives and
// answers with a programmable status (202 by default). With status 500 and
// BatchSize=1 the worker parks holding a retained batch — the staging seam
// the owed-exposure tests use to make ErrQueueFull deterministic.
type expWireCapture struct {
	mu        sync.Mutex
	status    int
	hits      int
	envelopes []map[string]any
}

func (w *expWireCapture) handler() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		var batch struct {
			Events []map[string]any `json:"events"`
		}
		_ = json.NewDecoder(r.Body).Decode(&batch)
		w.mu.Lock()
		w.hits++
		status := w.status
		if status == 0 {
			status = http.StatusAccepted
		}
		if status == http.StatusAccepted {
			w.envelopes = append(w.envelopes, batch.Events...)
		}
		count := len(batch.Events)
		w.mu.Unlock()
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(status)
		if status == http.StatusAccepted {
			_, _ = rw.Write([]byte(fmt.Sprintf(`{"accepted":%d,"rejected":0,"duplicates":0}`, count)))
		}
	}
}

func (w *expWireCapture) setStatus(status int) {
	w.mu.Lock()
	w.status = status
	w.mu.Unlock()
}

func (w *expWireCapture) hitCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.hits
}

// exposures returns the delivered experiment_exposure envelopes.
func (w *expWireCapture) exposures() []map[string]any {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []map[string]any
	for _, envelope := range w.envelopes {
		if envelope["event_name"] == experimentExposureName {
			out = append(out, envelope)
		}
	}
	return out
}

// newExperimentServer builds the shared test server: the assignment route
// answers from the script, everything else (the ingest batch route) from
// the capture.
func newExperimentServer(t *testing.T, script *expScript, capture *expWireCapture) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc(expAssignmentRoute, script.handler(t))
	mux.HandleFunc("/v1/consent", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{}`))
	})
	mux.HandleFunc("/", capture.handler())
	return httptest.NewServer(mux)
}

// parkWorkerWithFullQueue stages the deterministic owed-exposure setup: the
// ingest answers 500, so the worker (BatchSize=1) pulls one filler, fails
// retryably, and parks holding it — then a second filler fills the
// BufferSize=1 queue for good. Every emission until the queue is drained or
// the ingest recovers fails ErrQueueFull.
func parkWorkerWithFullQueue(t *testing.T, client *Client, capture *expWireCapture) {
	t.Helper()
	capture.setStatus(http.StatusInternalServerError)
	if err := client.Enqueue(Event{Name: "filler_parked"}); err != nil {
		t.Fatalf("filler 1: %v", err)
	}
	waitFor(t, 5*time.Second, "the worker parks on the failing ingest", func() bool { return capture.hitCount() >= 1 })
	if err := client.Enqueue(Event{Name: "filler_queued"}); err != nil {
		t.Fatalf("filler 2: %v", err)
	}
}

func newExperimentClient(t *testing.T, serverURL string, mutate func(*Config)) *Client {
	t.Helper()
	cfg := Config{
		IngestURL:          serverURL,
		Token:              "test-token",
		WorkspaceID:        "workspace-test",
		AppID:              "app-test",
		EnvironmentID:      "develop",
		Source:             SourceBackend,
		AnonymousID:        "anon-test",
		APIKey:             "test-exp-key",
		RemoteConfigURL:    serverURL,
		ExperimentsEnabled: true,
		BatchSize:          8,
		BufferSize:         16,
		FlushInterval:      time.Hour,
		HTTPTimeout:        time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if client.exp != nil {
		// Park the background lane so tests drive experimentCycle
		// deterministically.
		client.exp.mu.Lock()
		client.exp.laneParkedForTests = true
		client.exp.mu.Unlock()
	}
	return client
}

func drainQueuedEvents(c *Client) []Event {
	return c.queue.drainInto(nil, 1024)
}

func fetchAssignment(t *testing.T, c *Client, key string) ExperimentAssignmentResult {
	t.Helper()
	result, err := c.FetchExperimentAssignment(context.Background(), key, nil)
	if err != nil {
		t.Fatalf("FetchExperimentAssignment(%q): %v", key, err)
	}
	return result
}

// ── dark default ────────────────────────────────────────────────────────────

func TestExperimentsDarkDefault(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":0,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.ExperimentsEnabled = false
		cfg.SpoolDir = spoolDir
	})
	defer client.Close(context.Background())

	if client.exp != nil || client.expLaneDone != nil {
		t.Fatalf("the default must construct no experiment machinery")
	}
	if _, err := client.FetchExperimentAssignment(context.Background(), "exp", nil); !errors.Is(err, ErrExperimentsNotConfigured) {
		t.Fatalf("expected ErrExperimentsNotConfigured, got %v", err)
	}
	if err := client.TrackExperimentExposure("exp"); !errors.Is(err, ErrExperimentsNotConfigured) {
		t.Fatalf("expected ErrExperimentsNotConfigured, got %v", err)
	}
	if err := client.TrackExperimentOutcome("exp", "score", 1); !errors.Is(err, ErrExperimentsNotConfigured) {
		t.Fatalf("expected ErrExperimentsNotConfigured, got %v", err)
	}
	if v := client.ExperimentVariant("exp"); v != "" {
		t.Fatalf("expected no variant, got %q", v)
	}
	if p := client.ExperimentVariantPayload("exp"); p != nil {
		t.Fatalf("expected no payload, got %v", p)
	}
	// Zero new persistence keys: the state directory holds no experiment
	// files.
	for _, name := range []string{expSubjectFileName, expCacheFileName} {
		if _, err := os.Stat(filepath.Join(spoolDir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("dark default must not create %s", name)
		}
	}
	if requests != 0 {
		t.Fatalf("dark default must produce zero experiment traffic, saw %d requests", requests)
	}
}

func TestExperimentsConfigRequiresRemoteConfigURL(t *testing.T) {
	_, err := NewClient(Config{
		IngestURL:          "https://ingest.example",
		Token:              "test-token",
		WorkspaceID:        "ws",
		AppID:              "app",
		EnvironmentID:      "dev",
		Source:             SourceBackend,
		ExperimentsEnabled: true,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

// ── fetch basics ────────────────────────────────────────────────────────────

func TestFetchExperimentAssignmentRequestShape(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, map[string]string{
		"geo":         " DE ",
		"invented":    "dropped",
		"app_version": "2.0.0",
	})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !result.Assigned || result.VariantKey != "treatment" || result.FromCache {
		t.Fatalf("unexpected result %+v", result)
	}
	if result.Boundary["assignment_unit"] != "client_id" {
		t.Fatalf("boundary must ride the fresh result, got %v", result.Boundary)
	}

	request := script.request(0)
	if request.Method != http.MethodGet {
		t.Fatalf("expected GET, got %s", request.Method)
	}
	if request.Header.Get("Authorization") != "Bearer test-exp-key" {
		t.Fatalf("expected the publishable APIKey bearer, got %q", request.Header.Get("Authorization"))
	}
	if request.Header.Get("If-None-Match") != "" {
		t.Fatalf("the assignment fetch has no conditional-request contract")
	}
	query := request.URL.Query()
	if query.Get("app_key") != "app-test" || query.Get("environment_key") != "develop" ||
		query.Get("experiment_key") != expTestScopeKey {
		t.Fatalf("routing params wrong: %v", query)
	}
	subject := query.Get("subject_key")
	if !validExperimentSubjectID(subject) || !strings.HasPrefix(subject, "spcid_") || len(subject) != 38 {
		t.Fatalf("expected a freshly minted 32-hex subject, got %q", subject)
	}
	if query.Get("geo") != "DE" || query.Get("app_version") != "2.0.0" {
		t.Fatalf("allowlisted attributes must ride trimmed: %v", query)
	}
	if _, ok := query["invented"]; ok {
		t.Fatalf("out-of-vocabulary attributes must never be sent")
	}
}

func TestFetchServesCacheOverTransientAndPersists(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(503, `{"error":"kill switch state unavailable"}`)
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	result := fetchAssignment(t, client, expTestScopeKey)
	if !result.FromCache || result.Code != "transient_503" || result.VariantKey != "treatment" {
		t.Fatalf("expected cache serve over the 503, got %+v", result)
	}
	if v := client.ExperimentVariant(expTestScopeKey); v != "treatment" {
		t.Fatalf("getter must serve the cached variant, got %q", v)
	}

	// The durable record landed, scope-stamped.
	data, err := os.ReadFile(filepath.Join(spoolDir, expCacheFileName))
	if err != nil {
		t.Fatalf("read cache file: %v", err)
	}
	var record expDurableRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode cache file: %v", err)
	}
	if record.Scope == "" || len(record.Entries) != 1 {
		t.Fatalf("expected one scope-stamped entry, got %+v", record)
	}
}

func TestSubjectPersistsAndFullGrammarLoadsStick(t *testing.T) {
	script := &expScript{}
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()

	client1 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	fetchAssignment(t, client1, expTestScopeKey)
	subject1 := script.request(0).URL.Query().Get("subject_key")
	if err := client1.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	client2 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	fetchAssignment(t, client2, expTestScopeKey)
	subject2 := script.request(1).URL.Query().Get("subject_key")
	_ = client2.Close(context.Background())
	if subject1 != subject2 {
		t.Fatalf("the persisted subject must stick across restarts: %q vs %q", subject1, subject2)
	}

	// A stored id that is wire-valid but NOT this SDK's mint shape stays
	// sticky (re-minting re-buckets).
	foreign := "spcid_FOREIGN-build_" + strings.Repeat("x", 12)
	if !validExperimentSubjectID(foreign) {
		t.Fatalf("test id must be wire-valid")
	}
	if err := os.WriteFile(filepath.Join(spoolDir, expSubjectFileName), []byte(foreign+"\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	client3 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	fetchAssignment(t, client3, expTestScopeKey)
	if got := script.request(2).URL.Query().Get("subject_key"); got != foreign {
		t.Fatalf("a wire-valid stored id must never be re-minted: got %q", got)
	}
	_ = client3.Close(context.Background())

	// A corrupt stored id re-mints (fresh-install semantics) and persists
	// the replacement.
	if err := os.WriteFile(filepath.Join(spoolDir, expSubjectFileName), []byte("not-a-subject\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	client4 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client4.Close(context.Background())
	fetchAssignment(t, client4, expTestScopeKey)
	reminted := script.request(3).URL.Query().Get("subject_key")
	if !validExperimentSubjectID(reminted) || reminted == foreign {
		t.Fatalf("a corrupt stored id must re-mint, got %q", reminted)
	}
	stored, err := os.ReadFile(filepath.Join(spoolDir, expSubjectFileName))
	if err != nil || strings.TrimSpace(string(stored)) != reminted {
		t.Fatalf("the re-mint must persist: %q err=%v", stored, err)
	}
}

// ── consent gating ──────────────────────────────────────────────────────────

func TestGrantedOnlyGatesWithConsentFloor(t *testing.T) {
	script := &expScript{}
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.ConsentFloor = &ConsentFloorConfig{}
	})
	defer client.Close(context.Background())

	// Floor-on, undecided: the plane refuses distinctly and no request
	// leaves the process; no subject id is minted.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("expected ErrConsentUnknown, got %v", err)
	}
	if err := client.TrackExperimentExposure(expTestScopeKey); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("expected ErrConsentUnknown from the producer, got %v", err)
	}
	if script.requestCount() != 0 {
		t.Fatalf("an undecided floor session must produce zero assignment traffic")
	}
	client.exp.mu.Lock()
	subjectChecked := client.exp.subjectID
	client.exp.mu.Unlock()
	if subjectChecked != "" {
		t.Fatalf("no subject id may be minted while consent refuses")
	}

	// Forced-minor: the consumer is fully OFF (AC-8) — zero experiment
	// traffic on both planes.
	if err := client.SetConsentDecision(ConsentDecisionDeniedForcedMinor); err != nil {
		t.Fatalf("SetConsentDecision: %v", err)
	}
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected ErrConsentDenied under forced-minor, got %v", err)
	}
	if err := client.TrackExperimentOutcome(expTestScopeKey, "score", 1); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected ErrConsentDenied from the producer, got %v", err)
	}
	client.experimentCycle(context.Background())
	if script.requestCount() != 0 {
		t.Fatalf("forced-minor must produce zero experiment traffic, saw %d", script.requestCount())
	}

	// Granted: the plane opens.
	if err := client.SetConsentDecision(ConsentDecisionGranted); err != nil {
		t.Fatalf("SetConsentDecision: %v", err)
	}
	fetchAssignment(t, client, expTestScopeKey)
	if script.requestCount() != 1 {
		t.Fatalf("expected the granted fetch to reach the server")
	}
}

func TestFloorOffUnknownAdmitsAndDenialCloses(t *testing.T) {
	script := &expScript{}
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	// Floor-off, unknown: this SDK's documented open-under-unknown posture
	// applies to the plane (the same effective consent state the analytics
	// path uses).
	fetchAssignment(t, client, expTestScopeKey)
	if script.requestCount() != 1 {
		t.Fatalf("floor-off unknown must admit the fetch")
	}

	// Denial closes the plane and stops serving; the cached record is
	// retained, and a re-grant serves it again without a fetch.
	client.SetConsent(false)
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected ErrConsentDenied, got %v", err)
	}
	if v := client.ExperimentVariant(expTestScopeKey); v != "" {
		t.Fatalf("a denied session must serve no variants, got %q", v)
	}
	client.SetConsent(true)
	if v := client.ExperimentVariant(expTestScopeKey); v != "treatment" {
		t.Fatalf("a re-grant must serve the retained cache, got %q", v)
	}
	if script.requestCount() != 1 {
		t.Fatalf("the retained cache serves without a new fetch")
	}
}

// ── failure taxonomy end to end ─────────────────────────────────────────────

func TestUnauthorizedLatchesAndHostFetchUnlatches(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(401, `{"error":"invalid runtime token"}`)
	script.push(200, expAssignedBody("2"))
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("expected unauthorized, got %v", err)
	}
	// The latch stops serving and halts revalidation; the durable record is
	// retained.
	if v := client.ExperimentVariant(expTestScopeKey); v != "" {
		t.Fatalf("the latch must stop serving, got %q", v)
	}
	if _, err := os.Stat(filepath.Join(spoolDir, expCacheFileName)); err != nil {
		t.Fatalf("an ordinary 401 must retain the durable record: %v", err)
	}
	before := script.requestCount()
	client.exp.mu.Lock()
	client.exp.revalidateAtMS = 1 // long expired
	client.exp.mu.Unlock()
	client.experimentCycle(context.Background())
	if script.requestCount() != before {
		t.Fatalf("a latched lane must not revalidate")
	}
	// A host-triggered fetch stays allowed; its authorized success
	// unlatches and serves.
	result := fetchAssignment(t, client, expTestScopeKey)
	if !result.Assigned || result.Version != 2 {
		t.Fatalf("expected the fresh verdict, got %+v", result)
	}
	if v := client.ExperimentVariant(expTestScopeKey); v != "treatment" {
		t.Fatalf("the unlatched plane serves again, got %q", v)
	}
}

func TestRealSubjectsSentinelDropsDurablyAndDiscardsOwed(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(403, `{"error":"experiment real-subject assignment is disabled"}`)
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	if _, err := os.Stat(filepath.Join(spoolDir, expCacheFileName)); err != nil {
		t.Fatalf("precondition: cache persisted: %v", err)
	}
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("expected unauthorized, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(spoolDir, expCacheFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the sentinel must drop the durable record, got %v", err)
	}
	client.exp.mu.Lock()
	owed := len(client.exp.pendingExposure)
	client.exp.mu.Unlock()
	if owed != 0 {
		t.Fatalf("the sentinel discards owed exposure snapshots, %d remain", owed)
	}
}

func TestNotFoundDropsAndReturnsFirstClassMiss(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(404, `{"error":"published experiment not found"}`)
	script.push(503, ``)
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
	if err != nil || result.Assigned || result.Code != "not_found" {
		t.Fatalf("expected the not_found miss, got %+v / %v", result, err)
	}
	if v := client.ExperimentVariant(expTestScopeKey); v != "" {
		t.Fatalf("404 drops the cached assignment, got %q", v)
	}
	// Nothing is served stale for this experiment afterwards.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil || !strings.Contains(err.Error(), "transient_503") {
		t.Fatalf("expected a bare transient failure (no stale serve), got %v", err)
	}
}

func TestPermanent400DropsCachedEntry(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(400, `{"error":"experiment key is required"}`)
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil || !strings.Contains(err.Error(), "bad_request") {
		t.Fatalf("expected bad_request, got %v", err)
	}
	if v := client.ExperimentVariant(expTestScopeKey); v != "" {
		t.Fatalf("a permanent 400 drops the cached entry (no stale-forever), got %q", v)
	}
	data, _ := os.ReadFile(filepath.Join(spoolDir, expCacheFileName))
	var record expDurableRecord
	_ = json.Unmarshal(data, &record)
	if len(record.Entries) != 0 {
		t.Fatalf("the 400 drop must land durably, got %+v", record)
	}
}

func TestGrammar400RemintsOncePreservingAttributesAndOwedExposures(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(400, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`)
	script.push(200, expAssignedBody("2"))
	script.push(400, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	// First fetch with attributes: assigned (version 1). A consent
	// denial/re-grant purges its queued exposure fact and re-arms it as an
	// OWED snapshot of the version-1 treatment.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, map[string]string{"geo": "DE"}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	subjectBefore := script.request(0).URL.Query().Get("subject_key")
	client.SetConsent(false)
	client.SetConsent(true)
	client.exp.mu.Lock()
	owedBefore := len(client.exp.pendingExposure[expTestScopeKey])
	client.exp.mu.Unlock()
	if owedBefore == 0 {
		t.Fatalf("precondition: the purge re-armed an owed exposure")
	}

	// Second fetch hits the grammar sentinel: ONE re-mint, and the retry
	// carries the EXACT normalized attribute set of the rejected request.
	result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, map[string]string{"geo": "FR", "invented": "x"})
	if err != nil || !result.Assigned || result.Version != 2 {
		t.Fatalf("expected the reminted retry to succeed, got %+v / %v", result, err)
	}
	if script.requestCount() != 3 {
		t.Fatalf("expected exactly one retry, got %d requests", script.requestCount())
	}
	rejected := script.request(1).URL.Query()
	retried := script.request(2).URL.Query()
	if rejected.Get("geo") != "FR" || retried.Get("geo") != "FR" {
		t.Fatalf("the retry must carry the rejected request's normalized attributes: %v vs %v", rejected, retried)
	}
	subjectRetry := retried.Get("subject_key")
	if subjectRetry == subjectBefore || !validExperimentSubjectID(subjectRetry) {
		t.Fatalf("the retry must ride a freshly minted subject, got %q", subjectRetry)
	}

	// The owed exposure of the PAST (version-1) treatment survived the
	// re-mint: the resolution sweep emitted it alongside the fresh
	// version-2 application's fact.
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	versions := map[float64]int{}
	for _, fact := range capture.exposures() {
		versions[fact["props"].(map[string]any)["experiment_version"].(float64)]++
	}
	if versions[1] != 1 || versions[2] != 1 {
		t.Fatalf("expected the past treatment's owed fact AND the fresh one, got %v", versions)
	}

	// The one-shot budget: a second grammar 400 fails permanently, no loop.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil || !strings.Contains(err.Error(), "bad_request") {
		t.Fatalf("expected the second grammar 400 to fail without re-minting, got %v", err)
	}
	if script.requestCount() != 4 {
		t.Fatalf("no retry loop: expected 4 requests, got %d", script.requestCount())
	}
}

// ── fences ──────────────────────────────────────────────────────────────────

func TestStaleAuthoritativeResponseServesSettledState(t *testing.T) {
	script := &expScript{}
	slowGate := make(chan struct{})
	script.gates = map[int]chan struct{}{0: slowGate}
	script.push(401, `{"error":"invalid runtime token"}`) // request 0: the SLOW, stale refusal
	script.push(200, expAssignedBody("2"))                // request 1: the fast, newer verdict
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	type fetchOutcome struct {
		result ExperimentAssignmentResult
		err    error
	}
	slow := make(chan fetchOutcome, 1)
	go func() {
		result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
		slow <- fetchOutcome{result, err}
	}()
	// The slow request reaches the server and parks on its gate; the fast
	// fetch then dispatches, completes, and settles the fresh verdict.
	waitFor(t, 5*time.Second, "slow request reaches the server", func() bool { return script.requestCount() == 1 })
	fast := fetchAssignment(t, client, expTestScopeKey)
	if !fast.Assigned || fast.Version != 2 {
		t.Fatalf("the fast fetch must settle the fresh verdict, got %+v", fast)
	}
	// Release the stale 401 only now: it lost the per-key fence to the
	// newer settled success — it must NOT latch the plane, NOT touch the
	// cache, and its caller must receive the SETTLED state, never the
	// discarded refusal.
	close(slowGate)
	outcome := <-slow
	if outcome.err != nil {
		t.Fatalf("the fenced-out refusal must serve the settled state, got %v", outcome.err)
	}
	if !outcome.result.FromCache || outcome.result.Code != "superseded" || outcome.result.VariantKey != "treatment" {
		t.Fatalf("expected the superseded serve of the settled state, got %+v", outcome.result)
	}
	client.exp.mu.Lock()
	latched := client.exp.authBlocked
	client.exp.mu.Unlock()
	if latched {
		t.Fatalf("a stale refusal fenced out by a newer settled success must not latch the plane")
	}
	if v := client.ExperimentVariant(expTestScopeKey); v != "treatment" {
		t.Fatalf("the settled assignment keeps serving, got %q", v)
	}
}

// ── revalidation ────────────────────────────────────────────────────────────

func TestRevalidationCycleReusesAttributesAndDropsOnKill(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, `{"assigned":false,"reason":"kill_switch","version":1}`)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	clock := &expFakeClock{now: time.Now()}
	client := newExperimentClient(t, server.URL, nil)
	client.clock = clock
	defer client.Close(context.Background())

	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, map[string]string{"geo": "DE"}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(capture.exposures()) != 1 {
		t.Fatalf("precondition: the application's exposure emitted")
	}

	// The install armed the cadence; before the deadline a cycle is a
	// no-op.
	client.experimentCycle(context.Background())
	if script.requestCount() != 1 {
		t.Fatalf("no revalidation before the deadline")
	}
	// Past the deadline (300s ± 10%): the cycle re-fetches the cached key
	// with the remembered attributes; the kill answer drops the entry and
	// emits no exposure.
	clock.advance(331 * time.Second)
	client.experimentCycle(context.Background())
	if script.requestCount() != 2 {
		t.Fatalf("expected the revalidation fetch, got %d requests", script.requestCount())
	}
	if got := script.request(1).URL.Query().Get("geo"); got != "DE" {
		t.Fatalf("revalidation must re-send the last host-supplied attributes, got %q", got)
	}
	if v := client.ExperimentVariant(expTestScopeKey); v != "" {
		t.Fatalf("the kill must drop the served assignment, got %q", v)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if facts := capture.exposures(); len(facts) != 1 {
		t.Fatalf("a kill emits no exposure, got %d facts", len(facts))
	}
	// With nothing cached the cycle stops asking.
	clock.advance(400 * time.Second)
	client.experimentCycle(context.Background())
	if script.requestCount() != 2 {
		t.Fatalf("an empty cache must not revalidate")
	}
}

func TestRetryAfterPacesTheCadenceOnly(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.pushRetryAfter(429, ``, "120")
	script.push(200, expAssignedBody("1"))
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	clock := &expFakeClock{now: time.Now()}
	client := newExperimentClient(t, server.URL, nil)
	client.clock = clock
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	// A host fetch eats the 429 (served from cache) and arms the pacing.
	result := fetchAssignment(t, client, expTestScopeKey)
	if !result.FromCache || result.Code != "transient_429" {
		t.Fatalf("expected the paced cache serve, got %+v", result)
	}
	// Inside the Retry-After window the cadence stays parked even with the
	// revalidation deadline long past...
	clock.advance(60 * time.Second)
	client.exp.mu.Lock()
	client.exp.revalidateAtMS = clock.Now().UnixMilli() - 1
	client.exp.mu.Unlock()
	requestsBefore := script.requestCount()
	client.experimentCycle(context.Background())
	if script.requestCount() != requestsBefore {
		t.Fatalf("the Retry-After window must park the cadence")
	}
	// ...while an explicit host fetch stays allowed (RC parity: one GET,
	// per-fetch classification).
	fetchAssignment(t, client, expTestScopeKey)
	if script.requestCount() != requestsBefore+1 {
		t.Fatalf("an explicit fetch must not be blocked by the pacing")
	}
	// Past the window the cadence resumes.
	clock.advance(120 * time.Second)
	client.exp.mu.Lock()
	client.exp.revalidateAtMS = clock.Now().UnixMilli() - 1
	client.exp.mu.Unlock()
	client.experimentCycle(context.Background())
	if script.requestCount() != requestsBefore+2 {
		t.Fatalf("an expired Retry-After window must release the cadence")
	}
}

// ── durable owed-state machinery ────────────────────────────────────────────

// breakExperimentStorage makes the cache file unwritable by replacing it
// with a directory of the same name (writePrivateFileAtomic's rename fails).
func breakExperimentStorage(t *testing.T, c *Client) {
	t.Helper()
	c.exp.mu.Lock()
	c.exp.failDurableWritesForTests = true
	c.exp.mu.Unlock()
}

func restoreExperimentStorage(t *testing.T, c *Client) {
	t.Helper()
	c.exp.mu.Lock()
	c.exp.failDurableWritesForTests = false
	c.exp.mu.Unlock()
}

func TestOwedDurableDropRetriesUntilStorageRecovers(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, `{"assigned":false,"reason":"kill_switch"}`)
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	// Storage breaks; the kill's durable drop cannot land and is owed.
	breakExperimentStorage(t, client)
	result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
	if err != nil || result.Assigned {
		t.Fatalf("expected the kill miss, got %+v / %v", result, err)
	}
	client.exp.mu.Lock()
	var pending expOwedSync
	owed := false
	for _, candidate := range client.exp.durablePending {
		if candidate.experimentKey == expTestScopeKey {
			pending, owed = candidate, true
		}
	}
	client.exp.mu.Unlock()
	if !owed || !pending.drop {
		t.Fatalf("the failed durable drop must be owed, got %+v (owed=%v)", pending, owed)
	}
	// The killed variant is still reload truth on disk until the retry
	// lands — that is exactly what the owed sync exists to fix.
	if record := readExperimentRecord(t, spoolDir); record == nil || len(record.Entries) != 1 {
		t.Fatalf("precondition: the stale record is still on disk")
	}
	// Storage recovers; the next cycle converges the disk.
	restoreExperimentStorage(t, client)
	client.experimentCycle(context.Background())
	client.exp.mu.Lock()
	owedCount := len(client.exp.durablePending)
	client.exp.mu.Unlock()
	if owedCount != 0 {
		t.Fatalf("the owed drop must settle once storage recovers, %d remain", owedCount)
	}
	record := readExperimentRecord(t, spoolDir)
	if record != nil && len(record.Entries) != 0 {
		t.Fatalf("the killed entry must not survive on disk: %+v", record)
	}
}

func readExperimentRecord(t *testing.T, spoolDir string) *expDurableRecord {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(spoolDir, expCacheFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	var record expDurableRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode record: %v", err)
	}
	return &record
}

func TestOrdinaryLatchCancelsOwedWritesKeepsOwedDrops(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))                // exp assigned
	script.push(200, `{"assigned":false}`)                // traffic-gate drop — owed under broken storage
	script.push(200, expAssignedBody("2"))                // re-assigned — owed WRITE under broken storage
	script.push(401, `{"error":"invalid runtime token"}`) // the ordinary latch
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	breakExperimentStorage(t, client)
	if result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err != nil || result.Assigned {
		t.Fatalf("expected the traffic-gate miss, got %+v / %v", result, err)
	}
	fetchAssignment(t, client, expTestScopeKey) // v2 assigned; its durable write is owed
	client.exp.mu.Lock()
	writeOwed := false
	for _, pending := range client.exp.durablePending {
		if !pending.drop {
			writeOwed = true
		}
	}
	client.exp.mu.Unlock()
	if !writeOwed {
		t.Fatalf("precondition: an owed WRITE exists")
	}
	// The ordinary 401 cancels owed writes (their in-memory source is
	// latched away) but keeps owed authoritative drops.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil {
		t.Fatalf("expected the latch")
	}
	client.exp.mu.Lock()
	for key, pending := range client.exp.durablePending {
		if !pending.drop {
			t.Fatalf("an owed write survived the latch: %s -> %+v", key, pending)
		}
	}
	client.exp.mu.Unlock()
}

func TestOwedWholeRecordClearDemotesOnFreshInstall(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(403, `{"error":"experiment real-subject assignment is disabled"}`)
	script.push(200, expAssignedBody("2"))
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	// Storage breaks: the sentinel's whole-record clear cannot land and is
	// owed, stamped by its resolution.
	breakExperimentStorage(t, client)
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err == nil {
		t.Fatalf("expected the sentinel refusal")
	}
	client.exp.mu.Lock()
	clearOwed := client.exp.durableClearPending
	client.exp.mu.Unlock()
	if !clearOwed {
		t.Fatalf("the failed whole-record clear must be owed")
	}
	// A fresh authorized install demotes the owed clear immediately: the
	// stale clear must never wipe state written after it.
	restoreExperimentStorage(t, client)
	fetchAssignment(t, client, expTestScopeKey)
	client.exp.mu.Lock()
	clearOwed = client.exp.durableClearPending
	client.exp.mu.Unlock()
	if clearOwed {
		t.Fatalf("a fresh install must demote the owed whole-record clear")
	}
	// The demoted per-key drops (for keys the clear still covered) and the
	// fresh install converge the disk to exactly the new state.
	client.experimentCycle(context.Background())
	record := readExperimentRecord(t, spoolDir)
	if record == nil || len(record.Entries) != 1 || record.Entries[expTestScopeKey].Version != 2 {
		t.Fatalf("the disk must converge to the fresh install, got %+v", record)
	}
}

func TestClockRollbackCannotReviveSupersededVariant(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, strings.Replace(expAssignedBody("2"), `"variant_key":"treatment"`, `"variant_key":"treatment-b"`, 1))
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	spoolDir := t.TempDir()
	clock := &expFakeClock{now: time.Now()}
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	client.clock = clock
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	// The wall clock rolls BACK before the refresh: the superseding write
	// must still order above the stored record.
	clock.advance(-time.Hour)
	fetchAssignment(t, client, expTestScopeKey)
	record := readExperimentRecord(t, spoolDir)
	if record == nil || len(record.Entries) != 1 {
		t.Fatalf("expected one stored entry, got %+v", record)
	}
	entry := record.Entries[expTestScopeKey]
	if entry.VariantKey != "treatment-b" || entry.Version != 2 {
		t.Fatalf("the rollback must not leave the superseded variant as reload truth: %+v", entry)
	}
}

// ── exposure lane ───────────────────────────────────────────────────────────

func queuedExposures(events []Event) []Event {
	var out []Event
	for _, event := range events {
		if event.Name == experimentExposureName {
			out = append(out, event)
		}
	}
	return out
}

func TestExposureAutoEmitsOnceWithDeterministicID(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(503, ``)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	facts := capture.exposures()
	if len(facts) != 1 {
		t.Fatalf("expected exactly one exposure fact, got %d", len(facts))
	}
	fact := facts[0]
	client.exp.mu.Lock()
	marker := client.exp.sessionMarker
	subject := client.exp.entries[expTestScopeKey].SubjectKey
	client.exp.mu.Unlock()
	wantID := experimentExposureEventID(marker, subject, expTestScopeKey, 1, 0)
	if fact["event_id"] != wantID {
		t.Fatalf("expected the deterministic id %q, got %v", wantID, fact["event_id"])
	}
	props := fact["props"].(map[string]any)
	if props["assignment_key"] != "sfk1_"+strings.Repeat("a", 64) {
		t.Fatalf("the fact must carry the server-minted subject-fact key verbatim, got %v", props["assignment_key"])
	}
	if props["experiment_version"] != float64(1) || props["variant_key"] != "treatment" ||
		props["assignment_unit"] != "client_id" || props["experiment_key"] != expTestScopeKey {
		t.Fatalf("props mismatch: %v", props)
	}
	if len(props) != 5 {
		t.Fatalf("the props allowlist is exactly five keys, got %v", props)
	}
	for _, value := range props {
		if s, ok := value.(string); ok && strings.HasPrefix(s, "spcid_") {
			t.Fatalf("the raw subject id must never ride props: %v", props)
		}
	}

	// A cache-served refetch does not re-emit within the session.
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err != nil {
		t.Fatalf("refetch: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if extra := capture.exposures(); len(extra) != 1 {
		t.Fatalf("once per (experiment, version, subject) per session: got %d", len(extra))
	}
}

func TestConsentPurgeReArmsExposureWithSameID(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	facts := capture.exposures()
	if len(facts) != 1 {
		t.Fatalf("precondition: one fact, got %d", len(facts))
	}
	firstID := facts[0]["event_id"]
	// The denial purges queued-but-unpublished facts and re-arms; the
	// re-grant re-emits with the SAME deterministic id, so a fact that HAD
	// published collapses server-side as a duplicate.
	client.SetConsent(false)
	client.SetConsent(true)
	// Let the worker observe the denial epoch with an empty batch before
	// the sweep re-emits: an event received by a PARKED worker as the
	// first wake after a deny lands in its batch ahead of the loop-top
	// epoch check and is dropped as pre-denial — a pre-existing epoch-
	// observation race in the worker, not an experiments behavior (see
	// the PR notes; it hits ordinary Enqueue the same way).
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("sync flush: %v", err)
	}
	client.experimentCycle(context.Background())
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	facts = capture.exposures()
	if len(facts) != 2 {
		t.Fatalf("expected the re-armed emission, got %d", len(facts))
	}
	if facts[1]["event_id"] != firstID {
		t.Fatalf("the re-emission must derive the SAME deterministic id: %v vs %v", facts[1]["event_id"], firstID)
	}
}

func TestExplicitReArmWhileAutoOwedEmitsBothDistinctIDs(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.BatchSize = 1
		cfg.BufferSize = 1
	})
	defer client.Close(context.Background())

	// Park the worker and fill the queue: the automatic arm-0 emission at
	// fetch resolution fails ErrQueueFull and stays OWED.
	parkWorkerWithFullQueue(t, client, capture)
	fetchAssignment(t, client, expTestScopeKey)
	client.exp.mu.Lock()
	owed := len(client.exp.pendingExposure[expTestScopeKey])
	client.exp.mu.Unlock()
	if owed != 1 {
		t.Fatalf("precondition: the auto emission is owed, got %d", owed)
	}
	// Free the queue (the worker stays parked on its retained batch), then
	// explicitly re-arm while arm 0 is still owed: the re-arm buys the
	// EXTRA fact (arm 1), never the owed one's slot.
	client.queue.drainAll()
	if err := client.TrackExperimentExposure(expTestScopeKey); err != nil {
		t.Fatalf("TrackExperimentExposure: %v", err)
	}
	// The parked worker is not pulling (its batch is retained at
	// BatchSize), so the BufferSize=1 queue is a stable observation point:
	// drain the explicit fact, sweep the owed arm 0, drain it too.
	var facts []Event
	for _, event := range client.queue.drainInto(nil, 16) {
		if event.Name == experimentExposureName {
			facts = append(facts, event)
		}
	}
	client.experimentCycle(context.Background()) // sweeps the owed arm 0
	for _, event := range client.queue.drainInto(nil, 16) {
		if event.Name == experimentExposureName {
			facts = append(facts, event)
		}
	}
	if len(facts) != 2 {
		t.Fatalf("expected BOTH facts (explicit arm 1 + owed arm 0), got %d", len(facts))
	}
	client.exp.mu.Lock()
	marker := client.exp.sessionMarker
	subject := client.exp.entries[expTestScopeKey].SubjectKey
	client.exp.mu.Unlock()
	wantArm0 := experimentExposureEventID(marker, subject, expTestScopeKey, 1, 0)
	wantArm1 := experimentExposureEventID(marker, subject, expTestScopeKey, 1, 1)
	got := map[string]bool{facts[0].ID: true, facts[1].ID: true}
	if !got[wantArm0] || !got[wantArm1] {
		t.Fatalf("expected arm 0 and arm 1 ids, got %v (want %q, %q)", got, wantArm0, wantArm1)
	}
	capture.setStatus(http.StatusAccepted)
}

func TestExposureRequiresSubjectFactKey(t *testing.T) {
	script := &expScript{}
	noFactKey := strings.Replace(expAssignedBody("1"),
		`"subject_fact_key":"sfk1_`+strings.Repeat("a", 64)+`",`, ``, 1)
	script.push(200, noFactKey)
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	if facts := queuedExposures(drainQueuedEvents(client)); len(facts) != 0 {
		t.Fatalf("no subject-fact key ⇒ no fact (the raw subject never egresses), got %d", len(facts))
	}
	if err := client.TrackExperimentExposure(expTestScopeKey); !errors.Is(err, ErrExperimentFactUnavailable) {
		t.Fatalf("expected ErrExperimentFactUnavailable, got %v", err)
	}
	if err := client.TrackExperimentOutcome(expTestScopeKey, "score", 2); !errors.Is(err, ErrExperimentFactUnavailable) {
		t.Fatalf("expected ErrExperimentFactUnavailable, got %v", err)
	}
}

func TestOwedExposureFIFOBounded(t *testing.T) {
	script := &expScript{}
	for version := 1; version <= expMaxOwedExposures+2; version++ {
		script.push(200, strings.Replace(expAssignedBody("1"), `"version":1`, fmt.Sprintf(`"version":%d`, version), 1))
	}
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.BatchSize = 1
		cfg.BufferSize = 1
	})
	defer client.Close(context.Background())

	// Park the worker and fill the queue so every application's emission
	// stays owed; ten distinct applications (version bumps) arm ten
	// snapshots into the bounded FIFO of eight.
	parkWorkerWithFullQueue(t, client, capture)
	for i := 0; i < expMaxOwedExposures+2; i++ {
		fetchAssignment(t, client, expTestScopeKey)
	}
	client.exp.mu.Lock()
	list := client.exp.pendingExposure[expTestScopeKey]
	count := len(list)
	oldestVersion := int64(0)
	if count > 0 {
		oldestVersion = list[0].entry.Version
	}
	client.exp.mu.Unlock()
	if count != expMaxOwedExposures {
		t.Fatalf("the owed FIFO is bounded at %d, got %d", expMaxOwedExposures, count)
	}
	if oldestVersion != 3 {
		t.Fatalf("overflow drops the OLDEST snapshots: expected the queue to start at version 3, got %d", oldestVersion)
	}
	capture.setStatus(http.StatusAccepted)
}

func TestRestoreFromDiskServesAndReArmsExposure(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()

	client1 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	fetchAssignment(t, client1, expTestScopeKey)
	if err := client1.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	facts1 := capture.exposures()
	if len(facts1) != 1 {
		t.Fatalf("precondition: one fact in the first instance, got %d", len(facts1))
	}
	if err := client1.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	client2 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client2.Close(context.Background())
	// The restored assignment serves without a fetch...
	if v := client2.ExperimentVariant(expTestScopeKey); v != "treatment" {
		t.Fatalf("expected the restored variant, got %q", v)
	}
	if script.requestCount() != 1 {
		t.Fatalf("the restore must not fetch")
	}
	// ...and its exposure re-arms for the NEW session (a fresh instance is
	// a fresh session): the sweep emits one fact with the new session's
	// deterministic id.
	client2.experimentCycle(context.Background())
	if err := client2.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	facts2 := capture.exposures()
	if len(facts2) != 2 {
		t.Fatalf("expected the restored application's fact, got %d total", len(facts2))
	}
	if facts2[1]["event_id"] == facts1[0]["event_id"] {
		t.Fatalf("a new session derives its own deterministic id")
	}
}

// ── producers ───────────────────────────────────────────────────────────────

func TestExperimentFactWireEnvelope(t *testing.T) {
	type captured struct {
		Events []json.RawMessage `json:"events"`
	}
	var mu sync.Mutex
	var envelopes []map[string]any
	mux := http.NewServeMux()
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	mux.HandleFunc(expAssignmentRoute, script.handler(t))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var batch captured
		_ = json.NewDecoder(r.Body).Decode(&batch)
		mu.Lock()
		for _, raw := range batch.Events {
			var envelope map[string]any
			_ = json.Unmarshal(raw, &envelope)
			envelopes = append(envelopes, envelope)
		}
		count := len(batch.Events)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"accepted":%d,"rejected":0,"duplicates":0}`, count)))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.UserID = "user-42" // MUST NOT ride the facts
		cfg.Source = SourceBackend
	})
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	if err := client.TrackExperimentOutcome(expTestScopeKey, "score", 3.5); err != nil {
		t.Fatalf("outcome: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(envelopes) != 2 {
		t.Fatalf("expected the exposure and the outcome on the wire, got %d", len(envelopes))
	}
	for _, envelope := range envelopes {
		name := envelope["event_name"]
		if name != experimentExposureName && name != experimentOutcomeName {
			t.Fatalf("unexpected event %v", name)
		}
		if envelope["source"] != "client" {
			t.Fatalf("%v: experiment facts ride source=client, got %v", name, envelope["source"])
		}
		if _, hasUser := envelope["user_id"]; hasUser {
			t.Fatalf("%v: user_id must be OMITTED on experiment facts", name)
		}
		if envelope["anonymous_id"] != "anon-test" {
			t.Fatalf("%v: anonymous_id must carry the configured client identity, got %v", name, envelope["anonymous_id"])
		}
		props := envelope["props"].(map[string]any)
		if props["assignment_key"] != "sfk1_"+strings.Repeat("a", 64) {
			t.Fatalf("%v: assignment_key must be the sfk1 fact key, got %v", name, props["assignment_key"])
		}
		if name == experimentOutcomeName {
			if props["outcome_key"] != "score" || props["outcome_value"] != 3.5 {
				t.Fatalf("outcome pair mismatch: %v", props)
			}
			if len(props) != 7 {
				t.Fatalf("outcome props are exactly seven keys, got %v", props)
			}
		} else if len(props) != 5 {
			t.Fatalf("exposure props are exactly five keys, got %v", props)
		}
	}
}

func TestOutcomeValidation(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	if err := client.TrackExperimentOutcome("missing", "score", 1); !errors.Is(err, ErrExperimentNoAssignment) {
		t.Fatalf("expected ErrExperimentNoAssignment, got %v", err)
	}
	if err := client.TrackExperimentExposure("missing"); !errors.Is(err, ErrExperimentNoAssignment) {
		t.Fatalf("expected ErrExperimentNoAssignment, got %v", err)
	}
	fetchAssignment(t, client, expTestScopeKey)
	for _, tc := range []struct {
		key   string
		value float64
	}{
		{"", 1},
		{"score", nan()},
		{"score", inf()},
	} {
		if err := client.TrackExperimentOutcome(expTestScopeKey, tc.key, tc.value); !errors.Is(err, ErrInvalidExperimentFact) {
			t.Fatalf("expected ErrInvalidExperimentFact for %+v, got %v", tc, err)
		}
	}
}

func nan() float64  { return float64(0) / zero() }
func inf() float64  { return float64(1) / zero() }
func zero() float64 { return 0 }

// ── spool interaction ───────────────────────────────────────────────────────

func TestExperimentFactsGateOnAnonymousActorUnderFloor(t *testing.T) {
	// The fleet actor rule (review round 7): under a USER-scoped floor
	// (UserID configured) the recorded grant covers the user actor, but an
	// experiment fact rides the ANONYMOUS identity alone on the wire — the
	// actor whose id actually ships has no grant, so the fact REFUSES at
	// intake (ErrConsentActorMismatch, retryably: owed snapshots stay
	// armed) and nothing reaches the wire or the spool. An
	// ANONYMOUS-scoped floor (no UserID) grants the exact actor the fact
	// carries, and the fact passes.
	failures := 0
	var mu sync.Mutex
	mux := http.NewServeMux()
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	mux.HandleFunc(expAssignmentRoute, script.handler(t))
	mux.HandleFunc("/v1/consent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{}`))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		failures++
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	spoolDir := t.TempDir()
	var deadLetters []SpoolDeadLetter
	var dlMu sync.Mutex
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.SpoolDir = spoolDir
		cfg.UserID = "user-42"
		cfg.ConsentFloor = &ConsentFloorConfig{}
		cfg.OnSpoolDeadLetter = func(letter SpoolDeadLetter) {
			dlMu.Lock()
			deadLetters = append(deadLetters, letter)
			dlMu.Unlock()
		}
	})
	defer client.Close(context.Background())
	if err := client.SetConsentDecision(ConsentDecisionGranted); err != nil {
		t.Fatalf("grant: %v", err)
	}

	fetchAssignment(t, client, expTestScopeKey) // the PLANE is unaffected
	if err := client.TrackExperimentOutcome(expTestScopeKey, "score", 1); !errors.Is(err, ErrConsentActorMismatch) {
		t.Fatalf("a user-scoped floor must refuse the anonymous-actor fact, got %v", err)
	}
	if err := client.TrackExperimentExposure(expTestScopeKey); !errors.Is(err, ErrConsentActorMismatch) {
		t.Fatalf("the explicit re-arm must refuse the same way, got %v", err)
	}
	if owed := client.owedExperimentExposureCount(); owed == 0 {
		t.Fatalf("the automatic exposure must stay owed (retryable), not lost")
	}
	_ = client.Flush(context.Background())
	if _, err := os.Stat(filepath.Join(spoolDir, "spool.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("no fact reached the pipeline, so nothing may spool, got %v", err)
	}
	_ = failures
	dlMu.Lock()
	letters := len(deadLetters)
	dlMu.Unlock()
	if letters != 0 {
		t.Fatalf("nothing reached the spool, so nothing may dead-letter, got %d", letters)
	}

	// The ANONYMOUS-scoped floor grants the exact actor the fact carries:
	// the fact passes end-to-end.
	script.push(200, expAssignedBody("1"))
	capture2 := &expWireCapture{}
	server2 := newExperimentServer(t, script, capture2)
	defer server2.Close()
	client2 := newExperimentClient(t, server2.URL, func(cfg *Config) {
		cfg.SpoolDir = t.TempDir()
		cfg.ConsentFloor = &ConsentFloorConfig{}
	})
	defer client2.Close(context.Background())
	if err := client2.SetConsentDecision(ConsentDecisionGranted); err != nil {
		t.Fatalf("grant: %v", err)
	}
	fetchAssignment(t, client2, expTestScopeKey)
	if err := client2.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := capture2.exposures(); len(got) == 0 {
		t.Fatalf("an anonymous-scoped floor must pass the fact")
	}
}

// ── close housekeeping ──────────────────────────────────────────────────────

func TestCloseSweepsOwedExposureAndRetriesOwedDurableSync(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, `{"assigned":false,"reason":"kill_switch"}`)
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) {
		cfg.SpoolDir = spoolDir
		cfg.BatchSize = 1
		cfg.BufferSize = 1
	})

	// Park the worker and fill the queue: the application's exposure stays
	// OWED through the fetch.
	parkWorkerWithFullQueue(t, client, capture)
	fetchAssignment(t, client, expTestScopeKey)
	client.exp.mu.Lock()
	owed := len(client.exp.pendingExposure[expTestScopeKey])
	client.exp.mu.Unlock()
	if owed != 1 {
		t.Fatalf("precondition: the exposure is owed, got %d", owed)
	}
	// Break storage and take the kill: the durable drop is owed too.
	breakExperimentStorage(t, client)
	if result, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err != nil || result.Assigned {
		t.Fatalf("kill fetch: %+v / %v", result, err)
	}
	restoreExperimentStorage(t, client)
	capture.setStatus(http.StatusAccepted)

	// Close: the flush frees the room, the owed exposure sweeps in and the
	// second flush delivers it, and the owed durable drop converges before
	// teardown.
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if facts := capture.exposures(); len(facts) != 1 {
		t.Fatalf("a treatment applied under a full queue must not exit without its fact, got %d", len(facts))
	}
	record := readExperimentRecord(t, spoolDir)
	if record != nil && len(record.Entries) != 0 {
		t.Fatalf("the owed durable drop must land before teardown, got %+v", record)
	}
}

func TestCloseStopsLaneAndRejectsLateFetches(t *testing.T) {
	script := &expScript{}
	server := httptest.NewServer(script.handler(t))
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case <-client.expLaneDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("the lane goroutine must exit with Close")
	}
	if _, err := client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if err := client.TrackExperimentExposure(expTestScopeKey); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}
