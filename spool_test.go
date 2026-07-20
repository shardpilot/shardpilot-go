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
	"sync/atomic"
	"testing"
	"time"
)

// spoolDeadLetterRecorder captures OnSpoolDeadLetter invocations.
type spoolDeadLetterRecorder struct {
	mu      sync.Mutex
	letters []SpoolDeadLetter
}

func (r *spoolDeadLetterRecorder) record(letter SpoolDeadLetter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.letters = append(r.letters, letter)
}

func (r *spoolDeadLetterRecorder) byReason(reason SpoolDropReason) []SpoolDeadLetter {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []SpoolDeadLetter
	for _, letter := range r.letters {
		if letter.Reason == reason {
			out = append(out, letter)
		}
	}
	return out
}

func (r *spoolDeadLetterRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.letters)
}

// spoolTestServer routes the batch and consent endpoints with a switchable
// batch outcome and captures raw batch bodies plus per-batch event id
// arrival order.
type spoolTestServer struct {
	t *testing.T

	mu           sync.Mutex
	status       int
	errorCode    string
	retryAfter   string
	responseBody string
	bodies       [][]byte
	arrivals     [][]string
	consents     []capturedConsent
}

func newSpoolTestServer(t *testing.T) (*spoolTestServer, *httptest.Server) {
	t.Helper()
	state := &spoolTestServer{t: t, status: http.StatusAccepted}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/events:batch":
			body := make([]byte, 0, 1024)
			buffer := make([]byte, 4096)
			for {
				n, err := r.Body.Read(buffer)
				body = append(body, buffer[:n]...)
				if err != nil {
					break
				}
			}
			var request struct {
				Events []struct {
					EventID string `json:"event_id"`
				} `json:"events"`
			}
			if err := json.Unmarshal(body, &request); err != nil {
				t.Errorf("decode batch request: %v", err)
			}
			ids := make([]string, 0, len(request.Events))
			for _, event := range request.Events {
				ids = append(ids, event.EventID)
			}
			state.mu.Lock()
			state.bodies = append(state.bodies, body)
			state.arrivals = append(state.arrivals, ids)
			status := state.status
			errorCode := state.errorCode
			retryAfter := state.retryAfter
			responseBody := state.responseBody
			state.mu.Unlock()

			w.Header().Set("Content-Type", "application/json")
			if retryAfter != "" {
				w.Header().Set("Retry-After", retryAfter)
			}
			if status != http.StatusAccepted {
				w.WriteHeader(status)
				if errorCode != "" {
					fmt.Fprintf(w, `{"error":{"code":%q,"message":"test"}}`, errorCode)
				}
				return
			}
			w.WriteHeader(http.StatusAccepted)
			if responseBody != "" {
				_, _ = w.Write([]byte(responseBody))
				return
			}
			fmt.Fprintf(w, `{"accepted":%d,"rejected":0,"duplicates":0}`, len(ids))
		case "/v1/consent":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode consent request: %v", err)
			}
			state.mu.Lock()
			state.consents = append(state.consents, capturedConsent{body: body})
			state.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"recorded":true,"replayed":false}`))
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	return state, server
}

func (s *spoolTestServer) setOutcome(status int, errorCode, retryAfter string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
	s.errorCode = errorCode
	s.retryAfter = retryAfter
}

func (s *spoolTestServer) setAcceptedBody(body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = http.StatusAccepted
	s.errorCode = ""
	s.retryAfter = ""
	s.responseBody = body
}

func (s *spoolTestServer) batchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.bodies)
}

func (s *spoolTestServer) allBodies() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.bodies))
	copy(out, s.bodies)
	return out
}

func (s *spoolTestServer) allArrivals() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]string, len(s.arrivals))
	copy(out, s.arrivals)
	return out
}

func (s *spoolTestServer) consentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.consents)
}

func (s *spoolTestServer) consentAt(i int) capturedConsent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consents[i]
}

func newSpoolTestClient(t *testing.T, serverURL, spoolDir string, recorder *spoolDeadLetterRecorder, mutate func(*Config)) *Client {
	t.Helper()
	cfg := Config{
		IngestURL:     serverURL,
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		AnonymousID:   "anon-spool-1",
		SpoolDir:      spoolDir,
		BatchSize:     4,
		BufferSize:    16,
		FlushInterval: time.Hour,
		HTTPTimeout:   time.Second,
	}
	if recorder != nil {
		cfg.OnSpoolDeadLetter = recorder.record
	}
	if mutate != nil {
		mutate(&cfg)
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func readSpoolRecordFile(t *testing.T, dir string) spoolRecordWire {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, spoolFileName))
	if err != nil {
		t.Fatalf("read spool record: %v", err)
	}
	var record spoolRecordWire
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("unmarshal spool record: %v", err)
	}
	return record
}

func spoolFileExists(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, spoolFileName))
	return err == nil
}

// spoolTestActorDigest is the actor digest of the identity newSpoolTestClient
// configures, so pre-seeded consent records read as THAT client's decision.
func spoolTestActorDigest() string {
	return consentActorDigest(Config{
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		AnonymousID:   "anon-spool-1",
	})
}

func writeConsentRecordFile(t *testing.T, dir, decision string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	payload := []byte(fmt.Sprintf(`{"consent_analytics":%q,"actor_digest":%q}`, decision, spoolTestActorDigest()))
	if err := os.WriteFile(consentRecordPath(dir), payload, 0o600); err != nil {
		t.Fatalf("write consent record: %v", err)
	}
}

func spoolTestEnvelope(t *testing.T, id string, ts time.Time) json.RawMessage {
	t.Helper()
	envelope := eventEnvelope{
		EventID:       id,
		SchemaVersion: 1,
		EventName:     "spooled_event",
		Source:        SourceBackend,
		EventTS:       ts.UTC().Format(time.RFC3339Nano),
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal test envelope: %v", err)
	}
	return raw
}

func writeSpoolRecordFile(t *testing.T, dir string, deadlineMS int64, envelopes ...json.RawMessage) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	record := spoolRecordWire{Version: spoolRecordVersion, Events: envelopes, RetryAfterUntilMS: deadlineMS}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal spool record: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, spoolFileName), payload, 0o600); err != nil {
		t.Fatalf("write spool record: %v", err)
	}
}

func wireEventBytes(t *testing.T, body []byte) []string {
	t.Helper()
	var request struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatalf("decode wire body: %v", err)
	}
	out := make([]string, len(request.Events))
	for i, raw := range request.Events {
		out[i] = string(raw)
	}
	return out
}

// spoolRecordEventCount reads the record's event count without failing on an
// absent file.
func spoolRecordEventCount(dir string) (int, bool) {
	data, err := os.ReadFile(filepath.Join(dir, spoolFileName))
	if err != nil {
		return 0, false
	}
	var record spoolRecordWire
	if json.Unmarshal(data, &record) != nil {
		return 0, false
	}
	return len(record.Events), true
}

// flushUntilSpooled flushes (tolerating the expected failures) until the
// spool record holds want events. The flush worker drains the queue into its
// held batch asynchronously, so how many events each failed attempt carries
// is not deterministic — what IS deterministic is that repeated flushes spool
// everything undelivered.
func flushUntilSpooled(t *testing.T, client *Client, dir string, want int) {
	t.Helper()
	waitFor(t, 5*time.Second, "events spooled", func() bool {
		_ = client.Flush(context.Background())
		count, ok := spoolRecordEventCount(dir)
		return ok && count >= want
	})
}

func TestSpoolConfigDefaults(t *testing.T) {
	cfg, err := normalizeConfig(Config{
		IngestURL:     "http://localhost:8080",
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		SpoolDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("normalizeConfig: %v", err)
	}
	if cfg.SpoolMaxEvents != 2000 {
		t.Fatalf("expected the canonical 2000-event cap, got %d", cfg.SpoolMaxEvents)
	}
	if cfg.SpoolMaxBytes != 1<<20 {
		t.Fatalf("expected the canonical 1 MiB byte cap, got %d", cfg.SpoolMaxBytes)
	}

	// Without SpoolDir the caps stay unset — no spool exists to bound.
	cfg, err = normalizeConfig(Config{
		IngestURL:     "http://localhost:8080",
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
	})
	if err != nil {
		t.Fatalf("normalizeConfig: %v", err)
	}
	if cfg.SpoolMaxEvents != 0 || cfg.SpoolMaxBytes != 0 {
		t.Fatalf("expected no spool caps without SpoolDir, got %d/%d", cfg.SpoolMaxEvents, cfg.SpoolMaxBytes)
	}
}

func TestSpoolDisabledWithoutSpoolDir(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, "", recorder, nil)

	client.SetConsent(true)
	if err := client.Enqueue(Event{Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	_ = client.Close(context.Background())

	if recorder.count() != 0 {
		t.Fatalf("OnSpoolDeadLetter must never fire without SpoolDir, got %d", recorder.count())
	}
	stats := client.Snapshot()
	if stats.Spooled != 0 || stats.SpoolResent != 0 || stats.SpoolEvicted != 0 || stats.SpoolExpired != 0 || stats.SpoolPersistFailed != 0 {
		t.Fatalf("expected zero spool counters without SpoolDir: %+v", stats)
	}
}

func TestSpoolUnknownConsentRefusesDiskAndDeadLetters(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)

	// Live state stays ConsentUnknown: the pipeline is open, so the publish
	// is attempted — but disk participation is grant-only.
	if err := client.Enqueue(Event{ID: "evt-unknown-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}

	if spoolFileExists(dir) {
		t.Fatalf("nothing may be written to disk under unknown consent")
	}
	letters := recorder.byReason(SpoolDropConsent)
	if len(letters) == 0 {
		t.Fatalf("expected the would-have-spooled batch dead-lettered as consent")
	}
	if len(letters[0].Envelopes) != 1 || !containsEventID(t, letters[0].Envelopes, "evt-unknown-1") {
		t.Fatalf("expected the refused envelope in the dead letter, got %+v", letters[0])
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = client.Close(context.Background())
}

func containsEventID(t *testing.T, envelopes []json.RawMessage, id string) bool {
	t.Helper()
	for _, raw := range envelopes {
		var envelope spoolEnvelopeWire
		if err := json.Unmarshal(raw, &envelope); err != nil {
			t.Fatalf("dead-letter envelope is not valid JSON: %v", err)
		}
		if envelope.EventID == id {
			return true
		}
	}
	return false
}

func TestSpoolGrantedRetriableFailureSpoolsExactWireBytes(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	client.SetConsent(true)

	state.setOutcome(http.StatusServiceUnavailable, "internal_error", "")
	if err := client.Enqueue(Event{ID: "evt-wire-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Enqueue(Event{ID: "evt-wire-2", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	flushUntilSpooled(t, client, dir, 2)

	record := readSpoolRecordFile(t, dir)
	if record.Version != 1 {
		t.Fatalf("expected record version 1, got %d", record.Version)
	}
	// Every spooled envelope is the EXACT bytes some failed publish attempt
	// put on the wire.
	sent := make(map[string]bool)
	for _, body := range state.allBodies() {
		for _, envelope := range wireEventBytes(t, body) {
			sent[envelope] = true
		}
	}
	if len(record.Events) != 2 {
		t.Fatalf("expected 2 spooled envelopes, got %d", len(record.Events))
	}
	for i, raw := range record.Events {
		if !sent[string(raw)] {
			t.Fatalf("spooled envelope %d is not byte-identical to any wire attempt:\n%s", i, raw)
		}
	}
	if stats := client.Snapshot(); stats.Spooled != 2 {
		t.Fatalf("expected Spooled=2, got %+v", stats)
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = client.Close(context.Background())
}

func TestSpoolTerminalOutcomesNeverSpool(t *testing.T) {
	for _, tc := range []struct {
		status    int
		errorCode string
	}{
		{400, "validation_error"},
		{401, "unauthorized"},
		{409, "schema_revision_mismatch"},
		{413, "payload_too_large"},
	} {
		state, server := newSpoolTestServer(t)
		state.setOutcome(tc.status, tc.errorCode, "")

		dir := t.TempDir()
		client := newSpoolTestClient(t, server.URL, dir, nil, nil)
		client.SetConsent(true)

		if err := client.Enqueue(Event{Name: "e1"}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if err := client.Flush(context.Background()); err == nil {
			t.Fatalf("%d: expected the terminal failure surfaced", tc.status)
		}
		// Nothing was ever spooled, so no record file exists at all (the
		// grant alone creates no spool.json).
		if count, ok := spoolRecordEventCount(dir); ok && count != 0 {
			t.Fatalf("%d (%s) must never spool, got %d events", tc.status, tc.errorCode, count)
		}
		_ = client.Close(context.Background())
		server.Close()
	}
}

func TestSpool202SettlesSpooledEventsByID(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	client.SetConsent(true)

	state.setOutcome(http.StatusInternalServerError, "internal_error", "")
	if err := client.Enqueue(Event{ID: "evt-settle-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Enqueue(Event{ID: "evt-settle-2", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	flushUntilSpooled(t, client, dir, 2)

	// The retry lands a 202 whose per-event verdicts are terminal server
	// outcomes (rejected, duplicate, event_too_large): ack-removal settles
	// ALL of the batch's events out of the spool.
	state.setAcceptedBody(`{"accepted":0,"rejected":1,"duplicates":1,"events":[` +
		`{"event_id":"evt-settle-1","status":"rejected","code":"event_too_large"},` +
		`{"event_id":"evt-settle-2","status":"duplicate","code":"duplicate_event_id"}]}`)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("retry flush: %v", err)
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 0 {
		t.Fatalf("expected the 202 to settle every spooled event, got %d left", got)
	}
	_ = client.Close(context.Background())
}

func TestSpoolResendBeforeFreshAndByteIdenticalAcrossRestart(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	client.SetConsent(true)

	state.setOutcome(http.StatusInternalServerError, "internal_error", "")
	if err := client.Enqueue(Event{ID: "evt-restart-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Enqueue(Event{ID: "evt-restart-2", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	flushUntilSpooled(t, client, dir, 2)
	_ = client.Close(context.Background())

	// The record as the dead process left it: what a restart must resend,
	// byte for byte.
	spooledBytes := readSpoolRecordFile(t, dir).Events

	// A new process over the same state dir: the persisted grant loads the
	// spool, and the spooled chunk resends BEFORE freshly enqueued events —
	// byte-identical to the record (and therefore to the original failed
	// attempts).
	state.setOutcome(http.StatusAccepted, "", "")
	countBefore := state.batchCount()
	restarted := newSpoolTestClient(t, server.URL, dir, nil, nil)
	if err := restarted.Enqueue(Event{ID: "evt-restart-3", Name: "fresh"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := restarted.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	arrivals := state.allArrivals()[countBefore:]
	if len(arrivals) != 2 {
		t.Fatalf("expected the spooled chunk and the fresh batch as two requests, got %v", arrivals)
	}
	if arrivals[0][0] != "evt-restart-1" || arrivals[0][1] != "evt-restart-2" || arrivals[1][0] != "evt-restart-3" {
		t.Fatalf("expected spooled-before-fresh ordering, got %v", arrivals)
	}
	resendWire := wireEventBytes(t, state.allBodies()[countBefore])
	if len(resendWire) != len(spooledBytes) {
		t.Fatalf("expected %d resent envelopes, got %d", len(spooledBytes), len(resendWire))
	}
	for i := range spooledBytes {
		if resendWire[i] != string(spooledBytes[i]) {
			t.Fatalf("resent envelope %d is not byte-identical to the record:\n%s\n%s", i, spooledBytes[i], resendWire[i])
		}
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 0 {
		t.Fatalf("expected the resent events acked out of the record, got %d", got)
	}
	if stats := restarted.Snapshot(); stats.SpoolResent != 2 {
		t.Fatalf("expected SpoolResent=2, got %+v", stats)
	}
	_ = restarted.Close(context.Background())
}

func TestSpoolOldestDropAtCountAndByteCaps(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, func(cfg *Config) {
		cfg.SpoolMaxEvents = 3
	})
	client.SetConsent(true)

	// Drive the append directly with one four-event batch, so the count-cap
	// eviction provably reaches into the batch being appended.
	events := make([]Event, 0, 4)
	for i := 1; i <= 4; i++ {
		events = append(events, Event{ID: fmt.Sprintf("evt-cap-%d", i), Name: "e"})
	}
	request, err := client.buildBatch(events)
	if err != nil {
		t.Fatalf("buildBatch: %v", err)
	}
	client.spoolFailedBatch(request, nil, false)

	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 3 {
		t.Fatalf("expected the count cap to keep 3 events, got %d", len(record.Events))
	}
	if containsEventID(t, record.Events, "evt-cap-1") {
		t.Fatalf("expected the OLDEST event evicted first")
	}
	capacity := recorder.byReason(SpoolDropCapacity)
	if len(capacity) != 1 || !containsEventID(t, capacity[0].Envelopes, "evt-cap-1") {
		t.Fatalf("expected the evicted oldest event dead-lettered as capacity, got %+v", capacity)
	}
	stats := client.Snapshot()
	if stats.SpoolEvicted != 1 {
		t.Fatalf("expected SpoolEvicted=1, got %+v", stats)
	}
	// Partial durability: only the 3 survivors count as spooled.
	if stats.Spooled != 3 {
		t.Fatalf("expected Spooled=3 (survivors only), got %+v", stats)
	}
	_ = client.Close(context.Background())

	// Byte cap, exercised at load: a record larger than SpoolMaxBytes keeps
	// only the newest events that fit.
	byteDir := t.TempDir()
	now := time.Now()
	envelopes := []json.RawMessage{
		spoolTestEnvelope(t, "evt-bytes-1", now),
		spoolTestEnvelope(t, "evt-bytes-2", now),
		spoolTestEnvelope(t, "evt-bytes-3", now),
	}
	writeConsentRecordFile(t, byteDir, "granted")
	writeSpoolRecordFile(t, byteDir, 0, envelopes...)
	byteRecorder := &spoolDeadLetterRecorder{}
	byteClient := newSpoolTestClient(t, server.URL, byteDir, byteRecorder, func(cfg *Config) {
		cfg.SpoolMaxBytes = len(envelopes[1]) + len(envelopes[2])
	})
	record = readSpoolRecordFile(t, byteDir)
	if len(record.Events) != 2 || containsEventID(t, record.Events, "evt-bytes-1") {
		t.Fatalf("expected the byte cap to evict the oldest at load, got %d events", len(record.Events))
	}
	if letters := byteRecorder.byReason(SpoolDropCapacity); len(letters) != 1 || !containsEventID(t, letters[0].Envelopes, "evt-bytes-1") {
		t.Fatalf("expected the byte-cap eviction dead-lettered as capacity, got %+v", letters)
	}
	_ = byteClient.Close(context.Background())
}

func TestSpoolCountCapReappliedAtLoad(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	now := time.Now()
	var envelopes []json.RawMessage
	for i := 1; i <= 5; i++ {
		envelopes = append(envelopes, spoolTestEnvelope(t, fmt.Sprintf("evt-load-%d", i), now))
	}
	writeConsentRecordFile(t, dir, "granted")
	writeSpoolRecordFile(t, dir, 0, envelopes...)

	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, func(cfg *Config) {
		cfg.SpoolMaxEvents = 3
	})
	defer client.Close(context.Background())

	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 3 {
		t.Fatalf("expected the caps re-applied at load, got %d events", len(record.Events))
	}
	if containsEventID(t, record.Events, "evt-load-1") || containsEventID(t, record.Events, "evt-load-2") {
		t.Fatalf("expected the two oldest evicted at load")
	}
	if stats := client.Snapshot(); stats.SpoolEvicted != 2 {
		t.Fatalf("expected SpoolEvicted=2, got %+v", stats)
	}
}

func TestSpoolRetryAgeCapAtLoad(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	now := time.Now()
	fresh := spoolTestEnvelope(t, "evt-age-fresh", now.Add(-time.Hour))
	old := spoolTestEnvelope(t, "evt-age-old", now.Add(-8*24*time.Hour))
	future := spoolTestEnvelope(t, "evt-age-future", now.Add(2*time.Hour))
	undatable := json.RawMessage(`{"event_id":"evt-age-undatable","event_ts":"not-a-time"}`)
	noID := json.RawMessage(`{"event_ts":"` + now.UTC().Format(time.RFC3339Nano) + `"}`)
	writeConsentRecordFile(t, dir, "granted")
	writeSpoolRecordFile(t, dir, 0, old, future, undatable, noID, fresh)

	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	defer client.Close(context.Background())

	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 1 || !containsEventID(t, record.Events, "evt-age-fresh") {
		t.Fatalf("expected only the fresh-enough event to survive, got %s", mustJSON(t, record.Events))
	}
	expired := recorder.byReason(SpoolDropExpired)
	if len(expired) != 1 || len(expired[0].Envelopes) != 4 {
		t.Fatalf("expected the 4 unprovable/expired events dead-lettered, got %+v", expired)
	}
	if stats := client.Snapshot(); stats.SpoolExpired != 4 {
		t.Fatalf("expected SpoolExpired=4, got %+v", stats)
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(data)
}

func TestSpoolRetryAfterDeadlinePersisted(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusTooManyRequests, "rate_limited", "60")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	client.SetConsent(true)

	before := time.Now()
	if err := client.Enqueue(Event{ID: "evt-ra-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the 429 surfaced")
	}
	record := readSpoolRecordFile(t, dir)
	low := before.Add(59 * time.Second).UnixMilli()
	high := time.Now().Add(61 * time.Second).UnixMilli()
	if record.RetryAfterUntilMS < low || record.RetryAfterUntilMS > high {
		t.Fatalf("expected retry_after_until_ms about now+60s, got %d (want %d..%d)", record.RetryAfterUntilMS, low, high)
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = client.Close(context.Background())
}

func TestSpoolRestoredDeferralHonorsRemainingWindow(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	now := time.Now()
	writeConsentRecordFile(t, dir, "granted")
	writeSpoolRecordFile(t, dir, now.Add(600*time.Millisecond).UnixMilli(), spoolTestEnvelope(t, "evt-defer-1", now))

	client := newSpoolTestClient(t, server.URL, dir, nil, func(cfg *Config) {
		cfg.FlushInterval = 30 * time.Millisecond
	})
	defer client.Close(context.Background())

	if client.initialDeferUntil.IsZero() {
		t.Fatalf("expected the persisted deadline to seed the deferral")
	}
	// Inside the restored window automatic resends hold off despite the
	// short flush cadence...
	time.Sleep(250 * time.Millisecond)
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected no resend inside the restored Retry-After window, got %d requests", got)
	}
	// ...and the resend goes out once the window passes.
	waitFor(t, 5*time.Second, "spooled resend after the restored deadline", func() bool {
		return state.batchCount() > 0
	})
}

func TestSpoolRestoredDeferralClampAndExpiry(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	// A deadline absurdly far in the future seeds at most the 24h clamp.
	clampDir := t.TempDir()
	now := time.Now()
	writeConsentRecordFile(t, clampDir, "granted")
	writeSpoolRecordFile(t, clampDir, now.Add(48*time.Hour).UnixMilli(), spoolTestEnvelope(t, "evt-clamp-1", now))
	clampClient := newSpoolTestClient(t, server.URL, clampDir, nil, nil)
	if max := time.Now().Add(24*time.Hour + time.Minute); clampClient.initialDeferUntil.After(max) {
		t.Fatalf("expected the restored deferral clamped to 24h, got %v", clampClient.initialDeferUntil)
	}
	if clampClient.initialDeferUntil.Before(now.Add(23 * time.Hour)) {
		t.Fatalf("expected a substantial restored deferral, got %v", clampClient.initialDeferUntil)
	}
	_ = clampClient.Close(context.Background())

	// An expired deadline is dropped by the init rewrite and defers nothing.
	expiredDir := t.TempDir()
	writeConsentRecordFile(t, expiredDir, "granted")
	writeSpoolRecordFile(t, expiredDir, now.Add(-time.Second).UnixMilli(), spoolTestEnvelope(t, "evt-expired-deadline", now))
	expiredClient := newSpoolTestClient(t, server.URL, expiredDir, nil, nil)
	if !expiredClient.initialDeferUntil.IsZero() {
		t.Fatalf("expected an expired deadline to seed no deferral, got %v", expiredClient.initialDeferUntil)
	}
	if record := readSpoolRecordFile(t, expiredDir); record.RetryAfterUntilMS != 0 {
		t.Fatalf("expected the expired deadline dropped by the init rewrite, got %d", record.RetryAfterUntilMS)
	}
	_ = expiredClient.Close(context.Background())
}

func TestSpoolDenialPurgesAndConsentSenderStillPosts(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	now := time.Now()
	writeConsentRecordFile(t, dir, "granted")
	writeSpoolRecordFile(t, dir, 0,
		spoolTestEnvelope(t, "evt-deny-1", now),
		spoolTestEnvelope(t, "evt-deny-2", now))

	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)

	// Denial: the spool purges, the denied record persists, and the denial
	// receipt still rides the ordered /v1/consent sender.
	client.SetConsent(false)
	if spoolFileExists(dir) {
		t.Fatalf("expected the denial to purge spool.json")
	}
	if state2, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || state2 != ConsentDenied {
		t.Fatalf("expected the denied record persisted, got %v %v", state2, ok)
	}
	letters := recorder.byReason(SpoolDropConsent)
	if len(letters) != 1 || len(letters[0].Envelopes) != 2 {
		t.Fatalf("expected both purged events dead-lettered as consent, got %+v", letters)
	}
	waitFor(t, 3*time.Second, "denial receipt", func() bool { return state.consentCount() >= 1 })
	if categories, ok := state.consentAt(0).body["categories"].(map[string]any); !ok || categories["analytics"] != false {
		t.Fatalf("expected the denial receipt posted, got %+v", state.consentAt(0).body)
	}

	// Nothing spooled survives into delivery: a flush after the denial
	// resends nothing.
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected no batch requests after the denial purge, got %d", got)
	}

	// Re-grant: the granted record persists and exactly one grant receipt
	// posts.
	client.SetConsent(true)
	if state2, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || state2 != ConsentGranted {
		t.Fatalf("expected the granted record persisted, got %v %v", state2, ok)
	}
	waitFor(t, 3*time.Second, "grant receipt", func() bool { return state.consentCount() >= 2 })
	if categories, ok := state.consentAt(1).body["categories"].(map[string]any); !ok || categories["analytics"] != true {
		t.Fatalf("expected the grant receipt posted, got %+v", state.consentAt(1).body)
	}
	if state.consentCount() != 2 {
		t.Fatalf("expected exactly two receipts, got %d", state.consentCount())
	}
	_ = client.Close(context.Background())
}

func TestSpoolInFlightResendAbortedByDenialIsDropped(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	batchCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/events:batch":
			mu.Lock()
			batchCount++
			mu.Unlock()
			once.Do(func() { close(started) })
			<-release
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"accepted":0,"rejected":0,"duplicates":0}`))
		case "/v1/consent":
			_, _ = w.Write([]byte(`{"recorded":true,"replayed":false}`))
		}
	}))
	defer server.Close()
	defer close(release)

	dir := t.TempDir()
	now := time.Now()
	writeConsentRecordFile(t, dir, "granted")
	writeSpoolRecordFile(t, dir, 0, spoolTestEnvelope(t, "evt-abort-1", now))

	client := newSpoolTestClient(t, server.URL, dir, nil, func(cfg *Config) {
		cfg.HTTPTimeout = 5 * time.Second
	})

	flushDone := make(chan error, 1)
	go func() { flushDone <- client.Flush(context.Background()) }()
	<-started
	// The resend is on the wire; the denial aborts it and purges the spool.
	client.SetConsent(false)
	if err := <-flushDone; err != nil {
		t.Fatalf("expected the aborted flush to settle as a denial (nil), got %v", err)
	}
	if spoolFileExists(dir) {
		t.Fatalf("expected the spool purged by the denial")
	}
	// The aborted chunk is dropped, never re-spooled or re-sent.
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	mu.Lock()
	finalCount := batchCount
	mu.Unlock()
	if finalCount != 1 {
		t.Fatalf("expected the aborted chunk never re-sent, got %d batch requests", finalCount)
	}
	_ = client.Close(context.Background())
}

func TestSpoolPurgeFailureOwesWipeFailClosed(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	client.SetConsent(true)

	// Spool one batch under grant.
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")
	if err := client.Enqueue(Event{ID: "evt-owed-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}

	// Injected purge failure: the wipe is owed, durably marked, and the
	// spool fails closed.
	injectedErr := errors.New("injected remove failure")
	client.spool.removeFn = func(string) error { return injectedErr }
	client.SetConsent(false)
	if !wipeOwedMarkerExists(dir) {
		t.Fatalf("expected the spool-wipe-owed marker created")
	}
	if !spoolFileExists(dir) {
		t.Fatalf("the failed purge leaves the record file in place")
	}
	if state2, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || state2 != ConsentDenied {
		t.Fatalf("expected the denied record written despite the failed purge, got %v %v", state2, ok)
	}
	if stats := client.Snapshot(); stats.LastError != "spool_purge_failed" {
		t.Fatalf("expected spool_purge_failed surfaced, got %q", stats.LastError)
	}

	// Grant while the wipe still fails: the wipe is retried FIRST, the
	// persisted decision stays denied, and appends stay refused.
	client.SetConsent(true)
	if state2, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || state2 != ConsentDenied {
		t.Fatalf("expected the persisted decision to stay denied while the wipe is owed, got %v %v", state2, ok)
	}
	if stats := client.Snapshot(); stats.LastError != "spool_purge_failed" {
		t.Fatalf("expected spool_purge_failed surfaced on the failed re-grant, got %q", stats.LastError)
	}
	// Let the worker observe the denial epoch and discard its condemned held
	// batch before fresh post-grant events are enqueued (a fresh event merged
	// into a still-condemned held batch is dropped with it by design).
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("epoch-settling flush: %v", err)
	}
	if err := client.Enqueue(Event{ID: "evt-owed-2", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	if letters := recorder.byReason(SpoolDropConsent); len(letters) == 0 {
		t.Fatalf("expected the owed-wipe append refusal dead-lettered as consent")
	}

	// The failure clears: the next grant settles the wipe and re-opens.
	client.spool.removeFn = os.Remove
	client.SetConsent(true)
	if wipeOwedMarkerExists(dir) {
		t.Fatalf("expected the marker removed once the wipe succeeded")
	}
	if spoolFileExists(dir) {
		t.Fatalf("expected the condemned record wiped")
	}
	if state2, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || state2 != ConsentGranted {
		t.Fatalf("expected the granted record written after the wipe, got %v %v", state2, ok)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 1 {
		t.Fatalf("expected the spool re-opened after the wipe, got %d events", got)
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = client.Close(context.Background())
}

func TestSpoolOwedWipeAtStartFailsClosedInMemory(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	now := time.Now()
	writeConsentRecordFile(t, dir, "granted")
	writeSpoolRecordFile(t, dir, 0, spoolTestEnvelope(t, "evt-start-owed-1", now))
	if err := createWipeOwedMarker(dir); err != nil {
		t.Fatalf("create marker: %v", err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	defer func() { _ = os.Chmod(dir, 0o700) }()
	if probe, err := os.CreateTemp(dir, "probe-*"); err == nil {
		_ = probe.Close()
		_ = os.Remove(probe.Name())
		t.Skip("directory permissions are not enforced in this environment")
	}

	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	// The owed wipe could not be settled (the dir is unwritable), so the
	// spool fails closed: nothing loads, nothing resends.
	if stats := client.Snapshot(); stats.LastError != "spool_purge_failed" {
		t.Fatalf("expected spool_purge_failed at start, got %q", stats.LastError)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected no resend while the wipe is owed, got %d", got)
	}

	// Once the wipe can succeed, a grant settles it and the condemned
	// record is gone.
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	client.SetConsent(true)
	if wipeOwedMarkerExists(dir) || spoolFileExists(dir) {
		t.Fatalf("expected the owed wipe settled on grant")
	}
	_ = client.Close(context.Background())
}

func TestSpoolGrantPersistFailureKeepsSpoolClosed(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)

	// The grant's record write fails: the LIVE pipeline opens as documented,
	// but disk stays closed — writes require a PERSISTED grant.
	injectedErr := errors.New("injected rename failure")
	client.spool.renameFn = func(string, string) error { return injectedErr }
	client.SetConsent(true)
	if _, ok := loadConsentRecord(dir, spoolTestActorDigest()); ok {
		t.Fatalf("expected no consent record after the failed persist")
	}
	if stats := client.Snapshot(); stats.LastError != "consent_record_persist_failed" {
		t.Fatalf("expected consent_record_persist_failed surfaced, got %q", stats.LastError)
	}

	// Live sends are unaffected...
	state.setOutcome(http.StatusAccepted, "", "")
	if err := client.Track(context.Background(), Event{Name: "live"}); err != nil {
		t.Fatalf("Track: %v", err)
	}
	// ...but a retriable failure is refused disk and dead-lettered.
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")
	if err := client.Enqueue(Event{ID: "evt-unpersisted-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	if spoolFileExists(dir) {
		t.Fatalf("expected no spool file while the grant record is unpersisted")
	}
	if letters := recorder.byReason(SpoolDropConsent); len(letters) == 0 {
		t.Fatalf("expected the refused batch dead-lettered as consent")
	}

	// A later successful persist opens the spool.
	client.spool.renameFn = os.Rename
	client.SetConsent(true)
	if state2, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || state2 != ConsentGranted {
		t.Fatalf("expected the granted record persisted on retry, got %v %v", state2, ok)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 1 {
		t.Fatalf("expected the spool opened after the persisted grant, got %d events", got)
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = client.Close(context.Background())
}

func TestSpoolPersistFailureCountsAndMirrorStaysAuthoritative(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	client.SetConsent(true)

	injectedErr := errors.New("injected rename failure")
	client.spool.renameFn = func(string, string) error { return injectedErr }
	if err := client.Enqueue(Event{ID: "evt-persist-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	stats := client.Snapshot()
	if stats.SpoolPersistFailed == 0 {
		t.Fatalf("expected SpoolPersistFailed counted, got %+v", stats)
	}
	// Spooled counts DURABLY spooled events only: nothing landed on disk,
	// so nothing may be reported as spooled.
	if stats.Spooled != 0 {
		t.Fatalf("expected Spooled=0 while the record write fails, got %+v", stats)
	}
	if spoolFileExists(dir) {
		t.Fatalf("expected no record file while the write fails")
	}

	// The write path recovers and the flush-cadence retryPersist lands the
	// mirror: the entries that just became durable count into Spooled now —
	// exactly once.
	client.spool.renameFn = os.Rename
	client.spoolMaintain()
	if got := len(readSpoolRecordFile(t, dir).Events); got != 1 {
		t.Fatalf("expected the retried write to land the mirror, got %d events", got)
	}
	if stats := client.Snapshot(); stats.Spooled != 1 {
		t.Fatalf("expected Spooled=1 at the successful retry, got %+v", stats)
	}
	client.spoolMaintain()
	if stats := client.Snapshot(); stats.Spooled != 1 {
		t.Fatalf("expected the retry count to move exactly once, got %+v", stats)
	}

	// The mirror stayed authoritative throughout: a later append persists
	// alongside the earlier event and counts only itself.
	if err := client.Enqueue(Event{ID: "evt-persist-2", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 2 || !containsEventID(t, record.Events, "evt-persist-1") || !containsEventID(t, record.Events, "evt-persist-2") {
		t.Fatalf("expected both events persisted, got %s", mustJSON(t, record.Events))
	}
	if stats := client.Snapshot(); stats.Spooled != 2 {
		t.Fatalf("expected Spooled=2 after the second durable append, got %+v", stats)
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = client.Close(context.Background())
}

func TestSpoolGrantLoadMatrixAtStart(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, dir string)
		loads   bool
	}{
		{"absent record purges", func(t *testing.T, dir string) {}, false},
		{"denied record purges", func(t *testing.T, dir string) { writeConsentRecordFile(t, dir, "denied") }, false},
		{"unreadable record purges", func(t *testing.T, dir string) {
			if err := os.WriteFile(consentRecordPath(dir), []byte("not json"), 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}
		}, false},
		{"granted record loads", func(t *testing.T, dir string) { writeConsentRecordFile(t, dir, "granted") }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeSpoolRecordFile(t, dir, 0, spoolTestEnvelope(t, "evt-matrix-1", time.Now()))
			tc.prepare(t, dir)

			recorder := &spoolDeadLetterRecorder{}
			client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
			defer client.Close(context.Background())

			if tc.loads {
				if got := len(readSpoolRecordFile(t, dir).Events); got != 1 {
					t.Fatalf("expected the record loaded and kept, got %d events", got)
				}
				if recorder.count() != 0 {
					t.Fatalf("expected no dead letters on a granted load, got %+v", recorder.letters)
				}
				return
			}
			if spoolFileExists(dir) {
				t.Fatalf("expected the record purged at init")
			}
			letters := recorder.byReason(SpoolDropConsent)
			if len(letters) != 1 || !containsEventID(t, letters[0].Envelopes, "evt-matrix-1") {
				t.Fatalf("expected the purged record dead-lettered as consent, got %+v", letters)
			}
		})
	}
}

func TestSpoolCloseSpoolsUndeliveredRemnant(t *testing.T) {
	state, server := newSpoolTestServer(t)
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	client.SetConsent(true)

	// The endpoint dies before anything is delivered.
	server.Close()
	if err := client.Enqueue(Event{ID: "evt-remnant-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Enqueue(Event{ID: "evt-remnant-2", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	_ = client.Close(context.Background())

	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 2 || !containsEventID(t, record.Events, "evt-remnant-1") || !containsEventID(t, record.Events, "evt-remnant-2") {
		t.Fatalf("expected the undelivered remnant spooled at Close, got %s", mustJSON(t, record.Events))
	}

	// A healthy restart delivers it.
	state2, server2 := newSpoolTestServer(t)
	defer server2.Close()
	state2.setOutcome(http.StatusAccepted, "", "")
	restarted := newSpoolTestClient(t, server2.URL, dir, nil, nil)
	if err := restarted.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	arrivals := state2.allArrivals()
	if len(arrivals) != 1 || len(arrivals[0]) != 2 || arrivals[0][0] != "evt-remnant-1" {
		t.Fatalf("expected the remnant resent after restart, got %v", arrivals)
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 0 {
		t.Fatalf("expected the resent remnant acked, got %d events", got)
	}
	_ = restarted.Close(context.Background())
}

func TestSpoolTerminalOnSpooledEventsDeadLetters(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	client.SetConsent(true)

	state.setOutcome(http.StatusInternalServerError, "internal_error", "")
	if err := client.Enqueue(Event{ID: "evt-poison-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 1 {
		t.Fatalf("expected the batch spooled, got %d", got)
	}

	// The retry answers terminally: the spooled copy is settled out and
	// dead-lettered — a poison batch cannot re-fail every launch.
	state.setOutcome(http.StatusBadRequest, "validation_error", "")
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the terminal failure surfaced")
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 0 {
		t.Fatalf("expected the terminal outcome to settle the spooled copy, got %d", got)
	}
	letters := recorder.byReason(SpoolDropTerminal)
	if len(letters) != 1 || !containsEventID(t, letters[0].Envelopes, "evt-poison-1") {
		t.Fatalf("expected the settled event dead-lettered as terminal, got %+v", letters)
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = client.Close(context.Background())
}

func TestSpoolRetryAfterClearedOnSuccessfulPublish(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	client.SetConsent(true)

	// Spool a batch under a live 429 Retry-After window.
	state.setOutcome(http.StatusTooManyRequests, "rate_limited", "60")
	if err := client.Enqueue(Event{ID: "evt-clear-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the 429 surfaced")
	}
	if readSpoolRecordFile(t, dir).RetryAfterUntilMS == 0 {
		t.Fatalf("expected the Retry-After deadline persisted with the spooled batch")
	}

	// An explicit Flush delivers before the window expires: the success
	// proves the backpressure over, so the saved record must not carry the
	// stale deadline.
	state.setOutcome(http.StatusAccepted, "", "")
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("delivering flush: %v", err)
	}
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 0 {
		t.Fatalf("expected the delivered batch acked, got %d events", len(record.Events))
	}
	if record.RetryAfterUntilMS != 0 {
		t.Fatalf("expected the stale deadline cleared on delivery, got %d", record.RetryAfterUntilMS)
	}
	_ = client.Close(context.Background())

	// A new process over the same dir starts publishing immediately.
	restarted := newSpoolTestClient(t, server.URL, dir, nil, nil)
	if !restarted.initialDeferUntil.IsZero() {
		t.Fatalf("expected no restored deferral after the cleared deadline, got %v", restarted.initialDeferUntil)
	}
	countBefore := state.batchCount()
	if err := restarted.Enqueue(Event{ID: "evt-clear-2", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := restarted.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if state.batchCount() != countBefore+1 {
		t.Fatalf("expected the restarted client to publish immediately")
	}
	_ = restarted.Close(context.Background())
}

func TestSpoolResendRetryAfterWrittenThrough(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	now := time.Now()
	writeConsentRecordFile(t, dir, "granted")
	writeSpoolRecordFile(t, dir, 0, spoolTestEnvelope(t, "evt-through-1", now))

	// The startup-loaded resend hits a 429 with a fresh Retry-After: the new
	// deadline is written through to the record, not just held in memory.
	state.setOutcome(http.StatusTooManyRequests, "rate_limited", "120")
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the 429 surfaced")
	}
	record := readSpoolRecordFile(t, dir)
	low := now.Add(118 * time.Second).UnixMilli()
	high := time.Now().Add(121 * time.Second).UnixMilli()
	if record.RetryAfterUntilMS < low || record.RetryAfterUntilMS > high {
		t.Fatalf("expected the resend 429's deadline written through (~now+120s), got %d (want %d..%d)", record.RetryAfterUntilMS, low, high)
	}
	if len(record.Events) != 1 {
		t.Fatalf("expected the chunk still spooled, got %d events", len(record.Events))
	}
	_ = client.Close(context.Background())

	// An immediate process "restart" honors the remaining window.
	restarted := newSpoolTestClient(t, server.URL, dir, nil, nil)
	if restarted.initialDeferUntil.IsZero() {
		t.Fatalf("expected the written-through deadline to seed the restarted deferral")
	}
	if until := restarted.initialDeferUntil.UnixMilli(); until < low || until > high {
		t.Fatalf("expected the restored deferral inside the remaining window, got %d (want %d..%d)", until, low, high)
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = restarted.Close(context.Background())
}

func TestSpoolSharedDirMergePreservesSiblingRecords(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		SpoolDir:       dir,
		SpoolMaxEvents: 100,
		SpoolMaxBytes:  1 << 20,
		WorkspaceID:    "workspace-test",
		EnvironmentID:  "develop",
		AnonymousID:    "anon-spool-1",
	}
	spoolA := newDiskSpool(cfg)
	spoolB := newDiskSpool(cfg)
	var foreignB int
	spoolB.countForeign = func(n int) { foreignB += n }
	allowed := func() bool { return true }
	now := time.Now()

	e1 := spoolEntry{id: "evt-shared-1", ts: now.UTC().Format(time.RFC3339Nano), raw: spoolTestEnvelope(t, "evt-shared-1", now)}
	e2 := spoolEntry{id: "evt-shared-2", ts: now.UTC().Format(time.RFC3339Nano), raw: spoolTestEnvelope(t, "evt-shared-2", now)}

	// Interleaved appends from two instances: each save reloads and merges,
	// so neither writer's records are silently dropped by the other's
	// mirror-only view.
	if refused, _, _, _, persistFailed := spoolA.append([]spoolEntry{e1}, 0, false, now, allowed); refused || persistFailed {
		t.Fatalf("append A: refused=%v persistFailed=%v", refused, persistFailed)
	}
	if refused, _, _, _, persistFailed := spoolB.append([]spoolEntry{e2}, 0, false, now, allowed); refused || persistFailed {
		t.Fatalf("append B: refused=%v persistFailed=%v", refused, persistFailed)
	}
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 2 || !containsEventID(t, record.Events, "evt-shared-1") || !containsEventID(t, record.Events, "evt-shared-2") {
		t.Fatalf("expected both writers' records to survive interleaved saves, got %s", mustJSON(t, record.Events))
	}
	if foreignB == 0 {
		t.Fatalf("expected B's merging save to count A's record as a foreign mutation")
	}

	// An ack in A settles ONLY A's event: B's still-undelivered record
	// survives A's rewrite, and A's settled id is not resurrected from the
	// stale disk copy.
	removed, persistFailed := spoolA.ack([]string{"evt-shared-1"})
	if len(removed) != 1 || persistFailed {
		t.Fatalf("ack A: removed=%d persistFailed=%v", len(removed), persistFailed)
	}
	record = readSpoolRecordFile(t, dir)
	if len(record.Events) != 1 || !containsEventID(t, record.Events, "evt-shared-2") {
		t.Fatalf("expected only B's record to remain after A's ack, got %s", mustJSON(t, record.Events))
	}
	if containsEventID(t, record.Events, "evt-shared-1") {
		t.Fatalf("A's acked event must not be resurrected by a later save")
	}
}

func TestSpoolConsentRecordActorScoped(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	clientA := newSpoolTestClient(t, server.URL, dir, nil, nil)
	clientA.SetConsent(true)
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")
	if err := clientA.Enqueue(Event{ID: "evt-actor-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := clientA.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	// The endpoint stays down through Close, so the batch stays spooled.
	_ = clientA.Close(context.Background())
	state.setOutcome(http.StatusAccepted, "", "")
	if got := len(readSpoolRecordFile(t, dir).Events); got == 0 {
		t.Fatalf("expected actor A's spool populated")
	}
	countAfterA := state.batchCount()

	// A different actor over the SAME state dir: A's persisted grant covers
	// A's tuple only, so B sees no usable decision — the spool is not
	// loaded, and the purge-on-non-grant path condemns it.
	recorder := &spoolDeadLetterRecorder{}
	clientB := newSpoolTestClient(t, server.URL, dir, recorder, func(cfg *Config) {
		cfg.AnonymousID = "anon-spool-2"
	})
	if spoolFileExists(dir) {
		t.Fatalf("expected another actor's spool purged, never loaded")
	}
	letters := recorder.byReason(SpoolDropConsent)
	if len(letters) != 1 || !containsEventID(t, letters[0].Envelopes, "evt-actor-1") {
		t.Fatalf("expected the purged records dead-lettered as consent, got %+v", letters)
	}
	if err := clientB.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := state.batchCount(); got != countAfterA {
		t.Fatalf("expected none of actor A's events resent by actor B, got %d extra batch requests", got-countAfterA)
	}
	_ = clientB.Close(context.Background())

	// The same actor coming back finds its own record intact and loads
	// normally (a fresh spool this time — the purge condemned the data).
	clientA2 := newSpoolTestClient(t, server.URL, dir, nil, nil)
	if state2, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || state2 != ConsentGranted {
		t.Fatalf("expected actor A's grant record still usable, got %v %v", state2, ok)
	}
	_ = clientA2.Close(context.Background())
}

func TestSpoolEmptyLoadDropsStaleDeadline(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	// The record carries a still-future deadline but ONLY expired events:
	// nothing survives the load, so nothing is left for the window to
	// protect — fresh events must not be gated on it.
	dir := t.TempDir()
	now := time.Now()
	writeConsentRecordFile(t, dir, "granted")
	writeSpoolRecordFile(t, dir, now.Add(time.Hour).UnixMilli(),
		spoolTestEnvelope(t, "evt-stale-1", now.Add(-8*24*time.Hour)),
		spoolTestEnvelope(t, "evt-stale-2", now.Add(-9*24*time.Hour)))

	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	defer client.Close(context.Background())

	if !client.initialDeferUntil.IsZero() {
		t.Fatalf("an all-discarded load must not seed the deferral, got %v", client.initialDeferUntil)
	}
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 0 || record.RetryAfterUntilMS != 0 {
		t.Fatalf("expected an empty record without the stale deadline, got %d events, deadline %d", len(record.Events), record.RetryAfterUntilMS)
	}
	if letters := recorder.byReason(SpoolDropExpired); len(letters) != 1 || len(letters[0].Envelopes) != 2 {
		t.Fatalf("expected the discarded events dead-lettered as expired, got %+v", letters)
	}
	// Brand-new events publish immediately.
	if err := client.Enqueue(Event{ID: "evt-fresh-after-stale", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := state.batchCount(); got != 1 {
		t.Fatalf("expected the fresh event published immediately, got %d requests", got)
	}
}

func TestSpoolActorScopedPerEnvelope(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	// A grant persisted for the CONFIGURED actor covers only envelopes whose
	// effective actor IS that tuple: a per-event override rides the batch
	// live, but never that grant onto disk.
	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	client.SetConsent(true)

	request, err := client.buildBatch([]Event{
		{ID: "evt-own-actor", Name: "e1"},
		{ID: "evt-other-actor", Name: "e2", AnonymousID: "anon-somebody-else"},
	})
	if err != nil {
		t.Fatalf("buildBatch: %v", err)
	}
	client.spoolFailedBatch(request, nil, false)

	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 1 || !containsEventID(t, record.Events, "evt-own-actor") {
		t.Fatalf("expected only the configured actor's envelope spooled, got %s", mustJSON(t, record.Events))
	}
	letters := recorder.byReason(SpoolDropConsent)
	if len(letters) != 1 || len(letters[0].Envelopes) != 1 || !containsEventID(t, letters[0].Envelopes, "evt-other-actor") {
		t.Fatalf("expected the override envelope dead-lettered as consent, got %+v", letters)
	}
	if stats := client.Snapshot(); stats.Spooled != 1 {
		t.Fatalf("expected Spooled=1 (the covered envelope only), got %+v", stats)
	}
	_ = client.Close(context.Background())

	// With NO configured actor, the grant covers the empty tuple only:
	// events carrying explicit identities never spool under it.
	bareDir := t.TempDir()
	bareRecorder := &spoolDeadLetterRecorder{}
	bare := newSpoolTestClient(t, server.URL, bareDir, bareRecorder, func(cfg *Config) {
		cfg.AnonymousID = ""
	})
	bare.SetConsent(true)
	request, err = bare.buildBatch([]Event{{ID: "evt-explicit-id", Name: "e1", UserID: "user-explicit"}})
	if err != nil {
		t.Fatalf("buildBatch: %v", err)
	}
	bare.spoolFailedBatch(request, nil, false)
	if spoolFileExists(bareDir) {
		t.Fatalf("an explicit-identity envelope must not spool under a no-actor grant")
	}
	if letters := bareRecorder.byReason(SpoolDropConsent); len(letters) != 1 || !containsEventID(t, letters[0].Envelopes, "evt-explicit-id") {
		t.Fatalf("expected the explicit-identity envelope dead-lettered as consent, got %+v", letters)
	}
	_ = bare.Close(context.Background())
}

func TestSpoolMergeCapEvictionSettlesMirror(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		SpoolDir:       dir,
		SpoolMaxEvents: 3,
		SpoolMaxBytes:  1 << 20,
		WorkspaceID:    "workspace-test",
		EnvironmentID:  "develop",
		AnonymousID:    "anon-spool-1",
	}
	spoolA := newDiskSpool(cfg)
	spoolB := newDiskSpool(cfg)
	allowed := func() bool { return true }
	now := time.Now()
	entry := func(id string) spoolEntry {
		return spoolEntry{id: id, ts: now.UTC().Format(time.RFC3339Nano), raw: spoolTestEnvelope(t, id, now)}
	}

	// A persists two, B adds a third (the shared cap is 3), then A appends a
	// fourth: A's merged view is [a1 a2 b1] + [a3] — over the cap — and the
	// oldest entry dropped is A's OWN a1, which A still mirrors.
	if refused, _, _, _, persistFailed := spoolA.append([]spoolEntry{entry("evt-mc-a1"), entry("evt-mc-a2")}, 0, false, now, allowed); refused || persistFailed {
		t.Fatalf("append A: refused=%v persistFailed=%v", refused, persistFailed)
	}
	if refused, _, _, _, persistFailed := spoolB.append([]spoolEntry{entry("evt-mc-b1")}, 0, false, now, allowed); refused || persistFailed {
		t.Fatalf("append B: refused=%v persistFailed=%v", refused, persistFailed)
	}
	refused, added, _, evicted, persistFailed := spoolA.append([]spoolEntry{entry("evt-mc-a3")}, 0, false, now, allowed)
	if refused || persistFailed || len(evicted) != 0 {
		t.Fatalf("append A2: refused=%v persistFailed=%v evicted=%d", refused, persistFailed, len(evicted))
	}
	if len(added) != 1 {
		t.Fatalf("expected a3 accepted, got %d", len(added))
	}

	// The merge-stage cap drop settled LOCAL state: a1 left the mirror, is
	// reported for the capacity dead-letter, and cannot resurrect.
	drops := spoolA.takeCapacityDrops()
	if len(drops) != 1 || drops[0].id != "evt-mc-a1" {
		t.Fatalf("expected a1 reported as a local capacity drop, got %+v", drops)
	}
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 3 || containsEventID(t, record.Events, "evt-mc-a1") {
		t.Fatalf("expected the record to hold [a2 b1 a3], got %s", mustJSON(t, record.Events))
	}
	spoolA.mu.Lock()
	mirrorIDs := make([]string, 0, len(spoolA.entries))
	for _, held := range spoolA.entries {
		mirrorIDs = append(mirrorIDs, held.id)
	}
	total := spoolA.totalBytes
	spoolA.mu.Unlock()
	if len(mirrorIDs) != 2 || mirrorIDs[0] != "evt-mc-a2" || mirrorIDs[1] != "evt-mc-a3" {
		t.Fatalf("expected A's mirror to match the written record's local subset, got %v", mirrorIDs)
	}
	wantBytes := 0
	for _, held := range []string{"evt-mc-a2", "evt-mc-a3"} {
		wantBytes += len(spoolTestEnvelope(t, held, now))
	}
	if total != wantBytes {
		t.Fatalf("expected the byte counter to follow the mirror, got %d want %d", total, wantBytes)
	}
	// A later save (an ack of a2) must not resurrect a1.
	if removed, persistFailed := spoolA.ack([]string{"evt-mc-a2"}); len(removed) != 1 || persistFailed {
		t.Fatalf("ack: removed=%d persistFailed=%v", len(removed), persistFailed)
	}
	record = readSpoolRecordFile(t, dir)
	if containsEventID(t, record.Events, "evt-mc-a1") {
		t.Fatalf("a merge-cap-dropped entry must never resurrect, got %s", mustJSON(t, record.Events))
	}
}

func TestSpoolAppendExpiresPreAgedEnvelopes(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	client.SetConsent(true)

	// The event's caller-supplied timestamp is already older than the 7-day
	// retry cap when the batch fails: the retention bound is enforced at
	// APPEND — the envelope dead-letters as expired and never lands on disk.
	request, err := client.buildBatch([]Event{
		{ID: "evt-preaged-1", Name: "e1", Timestamp: time.Now().Add(-8 * 24 * time.Hour)},
	})
	if err != nil {
		t.Fatalf("buildBatch: %v", err)
	}
	client.spoolFailedBatch(request, nil, false)

	if spoolFileExists(dir) {
		t.Fatalf("a pre-expired envelope must never land in spool.json")
	}
	letters := recorder.byReason(SpoolDropExpired)
	if len(letters) != 1 || !containsEventID(t, letters[0].Envelopes, "evt-preaged-1") {
		t.Fatalf("expected the pre-expired envelope dead-lettered as expired, got %+v", letters)
	}
	stats := client.Snapshot()
	if stats.SpoolExpired != 1 || stats.Spooled != 0 {
		t.Fatalf("expected SpoolExpired=1 and Spooled=0, got %+v", stats)
	}

	// A future-dated envelope beyond the skew tolerance fails the same way,
	// while a fresh one in the same batch still spools.
	request, err = client.buildBatch([]Event{
		{ID: "evt-future-1", Name: "e2", Timestamp: time.Now().Add(2 * time.Hour)},
		{ID: "evt-fresh-1", Name: "e3"},
	})
	if err != nil {
		t.Fatalf("buildBatch: %v", err)
	}
	client.spoolFailedBatch(request, nil, false)
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 1 || !containsEventID(t, record.Events, "evt-fresh-1") {
		t.Fatalf("expected only the fresh envelope spooled, got %s", mustJSON(t, record.Events))
	}
	if stats := client.Snapshot(); stats.SpoolExpired != 2 || stats.Spooled != 1 {
		t.Fatalf("expected SpoolExpired=2 and Spooled=1, got %+v", stats)
	}
	_ = client.Close(context.Background())
}

func TestSpoolPreExistingStateDirTightenedToPrivate(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	// The app pre-created the state dir with loose permissions: os.MkdirAll
	// alone would keep them (its mode applies only to dirs it creates), so
	// the first private write must TIGHTEN the dir to the documented 0700.
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	defer client.Close(context.Background())
	client.SetConsent(true)

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("expected the pre-existing state dir tightened to 0700, got %v", info.Mode().Perm())
	}
	if _, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok {
		t.Fatalf("expected the grant record persisted alongside the tighten")
	}
}

func TestSpoolChmodRefusedFailsClosedAndDeadLetters(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)

	// The dir's privacy cannot be established (chmod refused): the write is a
	// persist failure — never a silent proceed — so the grant record fails,
	// the spool stays closed, and would-have-spooled batches dead-letter.
	client.spool.chmodFn = func(string, os.FileMode) error {
		return errors.New("chmod refused")
	}
	client.SetConsent(true)
	if got := client.Snapshot().LastError; got != "consent_record_persist_failed" {
		t.Fatalf("expected the refused tighten surfaced as a record persist failure, got %q", got)
	}

	if err := client.Enqueue(Event{ID: "evt-chmod-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	if spoolFileExists(dir) {
		t.Fatalf("nothing may be written through a dir whose privacy could not be established")
	}
	letters := recorder.byReason(SpoolDropConsent)
	if len(letters) == 0 || !containsEventID(t, letters[0].Envelopes, "evt-chmod-1") {
		t.Fatalf("expected the refused batch dead-lettered, got %+v", letters)
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = client.Close(context.Background())
}

func TestSpoolOversizedRecordDiscardedWithoutFullLoad(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	writeConsentRecordFile(t, dir, "granted")
	// A VALID record far beyond SpoolMaxBytes plus the framing allowance —
	// e.g. left by a version with different caps, or tampered. Loading it
	// whole and then evicting would defeat the bounded-spool guarantee, so
	// the bounded read treats it as corrupt: discarded as a clean start, with
	// NO eviction cascade (which would dead-letter every entry).
	now := time.Now()
	pad := strings.Repeat("x", 4096)
	envelopes := make([]json.RawMessage, 0, 20)
	for i := 0; i < 20; i++ {
		envelopes = append(envelopes, json.RawMessage(fmt.Sprintf(
			`{"event_id":"evt-oversized-%d","event_ts":%q,"pad":%q}`, i, now.UTC().Format(time.RFC3339Nano), pad)))
	}
	writeSpoolRecordFile(t, dir, 0, envelopes...)

	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, func(cfg *Config) {
		cfg.SpoolMaxBytes = 4096
	})
	if spoolFileExists(dir) {
		t.Fatalf("expected the over-limit record discarded at load")
	}
	if recorder.count() != 0 {
		t.Fatalf("an over-limit record is a clean start, not an eviction cascade; got %d letters", recorder.count())
	}
	if stats := client.Snapshot(); stats.SpoolEvicted != 0 || stats.SpoolExpired != 0 {
		t.Fatalf("expected no drop counters from the discarded record, got %+v", stats)
	}
	if !client.initialDeferUntil.IsZero() {
		t.Fatalf("expected no deferral seeded from a discarded record")
	}

	// The client starts clean and functions normally.
	if err := client.Enqueue(Event{ID: "evt-after-oversize-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	arrivals := state.allArrivals()
	if len(arrivals) != 1 || len(arrivals[0]) != 1 || arrivals[0][0] != "evt-after-oversize-1" {
		t.Fatalf("expected only the fresh event published (no resends from the discarded record), got %v", arrivals)
	}
	_ = client.Close(context.Background())
}

func TestSpoolResendPerEventTerminalVerdictsDeadLetter(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	now := time.Now()
	writeConsentRecordFile(t, dir, "granted")
	writeSpoolRecordFile(t, dir, 0,
		spoolTestEnvelope(t, "evt-verdict-ok-1", now),
		spoolTestEnvelope(t, "evt-verdict-rej-1", now),
		spoolTestEnvelope(t, "evt-verdict-sup-1", now),
	)
	// The 2xx carries per-event verdicts: one delivered, one rejected, one
	// consent-suppressed — and the batch contract says suppressed is NOT
	// delivery confirmation.
	state.setAcceptedBody(`{"accepted":1,"rejected":1,"duplicates":0,"events":[` +
		`{"event_id":"evt-verdict-ok-1","status":"accepted"},` +
		`{"event_id":"evt-verdict-rej-1","status":"rejected","code":"invalid_event"},` +
		`{"event_id":"evt-verdict-sup-1","status":"suppressed_no_consent"}]}`)

	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Every event is settled on the server (all removed from the record)...
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 0 {
		t.Fatalf("expected every verdicted event settled out of the record, got %s", mustJSON(t, record.Events))
	}
	// ...but only the confirmed delivery counts as resent; the per-event
	// terminal verdicts dead-letter with their matching classes.
	terminal := recorder.byReason(SpoolDropTerminal)
	if len(terminal) != 1 || len(terminal[0].Envelopes) != 1 || !containsEventID(t, terminal[0].Envelopes, "evt-verdict-rej-1") {
		t.Fatalf("expected exactly the rejected event dead-lettered terminal, got %+v", terminal)
	}
	consentLetters := recorder.byReason(SpoolDropConsent)
	if len(consentLetters) != 1 || len(consentLetters[0].Envelopes) != 1 || !containsEventID(t, consentLetters[0].Envelopes, "evt-verdict-sup-1") {
		t.Fatalf("expected exactly the suppressed event dead-lettered as consent, got %+v", consentLetters)
	}
	if recorder.count() != 2 {
		t.Fatalf("the delivered event must not dead-letter, got %d letters", recorder.count())
	}
	if stats := client.Snapshot(); stats.SpoolResent != 1 {
		t.Fatalf("expected SpoolResent to count only the confirmed delivery, got %+v", stats)
	}
	_ = client.Close(context.Background())
}

func TestSpoolRecoveryWakeResendsSpoolOnlyWork(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	writeConsentRecordFile(t, dir, "granted")
	writeSpoolRecordFile(t, dir, 0, spoolTestEnvelope(t, "evt-wake-1", time.Now()))

	// The startup-loaded chunk fails retriably WITH a Retry-After hint: the
	// chunk is requeued and the worker parks behind the deadline, with an
	// empty held batch — the only pending work is spool-only.
	state.setOutcome(http.StatusInternalServerError, "internal_error", "60")
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable resend failure surfaced")
	}

	// A synchronous Track success proves the endpoint healthy again: the
	// recovery wake must kick the requeued spool chunk NOW — FlushInterval is
	// an hour, so idling until the next tick would strand it.
	state.setOutcome(http.StatusAccepted, "", "")
	if err := client.Track(context.Background(), Event{ID: "evt-wake-live-1", Name: "live"}); err != nil {
		t.Fatalf("Track: %v", err)
	}
	waitFor(t, 5*time.Second, "the spooled chunk resent on the recovery wake", func() bool {
		return client.Snapshot().SpoolResent == 1
	})
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 0 {
		t.Fatalf("expected the resent chunk acked out of the record, got %s", mustJSON(t, record.Events))
	}
	// The chunk arrived exactly twice: the failed startup attempt and the one
	// recovery-wake resend — no extra retries.
	chunkArrivals := 0
	for _, arrival := range state.allArrivals() {
		if len(arrival) == 1 && arrival[0] == "evt-wake-1" {
			chunkArrivals++
		}
	}
	if chunkArrivals != 2 {
		t.Fatalf("expected the failed attempt plus exactly one recovery-wake resend, got %d chunk arrivals (%v)", chunkArrivals, state.allArrivals())
	}
	_ = client.Close(context.Background())
}

// A canceled explicit Flush whose only pending work is a startup-loaded
// spool chunk is caller abandonment, not endpoint feedback: it must leave
// the armed retry deadline untouched. The requeued chunk is exactly the
// work that deadline still protects, so the worker's empty-batch deadline
// clear must not run for an abandoned flush — or the chunk would retry at
// the next tick inside the server's window.
func TestSpoolCanceledFlushPreservesArmedDeadline(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	// Any leaked retry keeps failing retriably; the discriminator is whether
	// a request arrives at all while the deadline is armed.
	state.setOutcome(http.StatusInternalServerError, "internal_error", "3600")

	dir := t.TempDir()
	now := time.Now()
	writeConsentRecordFile(t, dir, "granted")
	writeSpoolRecordFile(t, dir, now.Add(time.Hour).UnixMilli(), spoolTestEnvelope(t, "evt-cancel-armed-1", now))

	// The short cadence makes a leaked deadline clear observable: the next
	// tick would retry the requeued chunk immediately.
	client := newSpoolTestClient(t, server.URL, dir, nil, func(cfg *Config) {
		cfg.FlushInterval = 40 * time.Millisecond
	})
	defer client.Close(context.Background())

	if client.initialDeferUntil.IsZero() {
		t.Fatalf("expected the persisted deadline to seed the deferral")
	}
	// Baseline: inside the restored window automatic resends hold off.
	time.Sleep(150 * time.Millisecond)
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected no resend inside the armed window before the flush, got %d requests", got)
	}

	// The canceled flush is handed straight to the worker so it cannot
	// short-circuit in Flush's own pre-send context check. The publish
	// attempt fails with the caller's context error before reaching the
	// server; the chunk is requeued and the held batch stays empty.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	reply := make(chan error, 1)
	select {
	case client.flushRequests <- flushRequest{ctx: canceledCtx, reply: reply}:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out handing the canceled flush to the worker")
	}
	select {
	case err := <-reply:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected the canceled flush to surface the caller's context error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the canceled flush reply")
	}

	// Zero pacing side-effects: the deadline stays armed, so the requeued
	// chunk must NOT retry at the following ticks.
	time.Sleep(400 * time.Millisecond)
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected the armed deadline to keep gating the requeued chunk after the canceled flush, got %d requests", got)
	}

	// Control: a live-context flush behaves as before — explicit intent
	// bypasses the deferral, delivers the chunk, and the successful outcome
	// clears pacing so a fresh event publishes on the normal cadence.
	state.setOutcome(http.StatusAccepted, "", "")
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("live-context flush: %v", err)
	}
	if got := state.batchCount(); got != 1 {
		t.Fatalf("expected the live flush to deliver the requeued chunk, got %d requests", got)
	}
	if record := readSpoolRecordFile(t, dir); len(record.Events) != 0 {
		t.Fatalf("expected the delivered chunk acked out of the record, got %s", mustJSON(t, record.Events))
	}
	if err := client.Enqueue(Event{ID: "evt-cancel-armed-fresh-1", Name: "fresh"}); err != nil {
		t.Fatalf("enqueue fresh event: %v", err)
	}
	waitFor(t, 5*time.Second, "the fresh event published on the flush cadence", func() bool {
		for _, arrival := range state.allArrivals() {
			for _, id := range arrival {
				if id == "evt-cancel-armed-fresh-1" {
					return true
				}
			}
		}
		return false
	})
}

// The abandonment rule extends to PERSISTED pacing state: a canceled flush
// whose spool-resend attempt fails with the caller's own context error must
// not run the hintless deadline withdrawal — nothing was learned from the
// endpoint, so retry_after_until_ms survives and a restart still defers
// inside the server's window.
func TestSpoolCanceledFlushKeepsPersistedDeadline(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	// A real retriable outcome (WITH a hint) for the eventual Close attempt;
	// the canceled flush itself must never reach the server.
	state.setOutcome(http.StatusInternalServerError, "internal_error", "3600")

	dir := t.TempDir()
	now := time.Now()
	deadlineMS := now.Add(time.Hour).UnixMilli()
	writeConsentRecordFile(t, dir, "granted")
	writeSpoolRecordFile(t, dir, deadlineMS, spoolTestEnvelope(t, "evt-keep-deadline-1", now))

	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	if client.initialDeferUntil.IsZero() {
		t.Fatalf("expected the persisted deadline to seed the deferral")
	}

	// Canceled flush straight to the worker: the resend attempt fails with
	// the caller's context error before reaching the server.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	reply := make(chan error, 1)
	select {
	case client.flushRequests <- flushRequest{ctx: canceledCtx, reply: reply}:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out handing the canceled flush to the worker")
	}
	select {
	case err := <-reply:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected the canceled flush to surface the caller's context error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the canceled flush reply")
	}

	// Zero persisted-pacing mutations: the record still carries the seeded
	// deadline and the requeued chunk stays spooled.
	record := readSpoolRecordFile(t, dir)
	if record.RetryAfterUntilMS != deadlineMS {
		t.Fatalf("expected the persisted deadline %d preserved after the canceled flush, got %d", deadlineMS, record.RetryAfterUntilMS)
	}
	if len(record.Events) != 1 {
		t.Fatalf("expected the requeued chunk still spooled, got %d events", len(record.Events))
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected the canceled flush to never reach the server, got %d requests", got)
	}
	// Close's own flush is live-context work: its retriable failure carries
	// the server's fresh Retry-After hint, refreshing the persisted window.
	_ = client.Close(context.Background())

	// A restart seeds its deferral from the preserved window and does not
	// resend inside it despite a hot flush cadence.
	restarted := newSpoolTestClient(t, server.URL, dir, nil, func(cfg *Config) {
		cfg.FlushInterval = 30 * time.Millisecond
	})
	if restarted.initialDeferUntil.IsZero() {
		t.Fatalf("expected the restart to seed the deferral from the preserved deadline")
	}
	before := state.batchCount()
	time.Sleep(250 * time.Millisecond)
	if got := state.batchCount(); got != before {
		t.Fatalf("expected no resend inside the preserved window after restart, got %d requests after %d", got, before)
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = restarted.Close(context.Background())
}

// The same persisted-state abandonment rule on the batch APPEND path: a
// retained batch retried under a canceled caller context re-appends (crash
// insurance), but the hintless context error must not withdraw the window a
// real 429 persisted alongside the spooled copy.
func TestSpoolCanceledFlushBatchAppendKeepsPersistedDeadline(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	// The live flush fails 429 WITH Retry-After: the batch spools and the
	// server window persists alongside it.
	state.setOutcome(http.StatusTooManyRequests, "rate_limited", "3600")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	client.SetConsent(true)

	if err := client.Enqueue(Event{ID: "evt-append-keep-1", Name: "append_keep"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the 429 surfaced")
	}
	armed := readSpoolRecordFile(t, dir)
	if armed.RetryAfterUntilMS == 0 {
		t.Fatalf("expected the 429 hint persisted alongside the spooled batch, got %+v", armed)
	}
	if len(armed.Events) != 1 {
		t.Fatalf("expected the failed batch spooled, got %d events", len(armed.Events))
	}

	// The retained batch retried under a canceled caller context: the
	// context error is hintless, but abandonment must not withdraw the
	// persisted window.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	reply := make(chan error, 1)
	select {
	case client.flushRequests <- flushRequest{ctx: canceledCtx, reply: reply}:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out handing the canceled flush to the worker")
	}
	select {
	case err := <-reply:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected the canceled flush to surface the caller's context error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the canceled flush reply")
	}

	after := readSpoolRecordFile(t, dir)
	if after.RetryAfterUntilMS != armed.RetryAfterUntilMS {
		t.Fatalf("expected the persisted deadline %d preserved after the canceled retry, got %d", armed.RetryAfterUntilMS, after.RetryAfterUntilMS)
	}
	if len(after.Events) != 1 {
		t.Fatalf("expected the spooled batch intact, got %d events", len(after.Events))
	}
	if got := state.batchCount(); got != 1 {
		t.Fatalf("expected only the live 429 attempt to reach the server, got %d requests", got)
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = client.Close(context.Background())
}

// An OnBatchResult callback observing a resend 202 runs only AFTER the spool
// settled the delivered chunk: a consent flip inside the callback purges the
// remainder only — the delivered entries are already acked off disk and must
// never be dead-lettered as consent drops.
func TestSpoolResendConsentFlipInCallbackSettlesDeliveredFirst(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	now := time.Now()
	writeConsentRecordFile(t, dir, "granted")
	writeSpoolRecordFile(t, dir, 0,
		spoolTestEnvelope(t, "evt-flip-1", now),
		spoolTestEnvelope(t, "evt-flip-2", now))

	recorder := &spoolDeadLetterRecorder{}
	// The callback closes over the client variable assigned below; the first
	// publish (and therefore the first callback) cannot fire before the
	// explicit Flush, so the assignment is ordered before every read.
	var client *Client
	var flipped atomic.Bool
	client = newSpoolTestClient(t, server.URL, dir, recorder, func(cfg *Config) {
		cfg.BatchSize = 1 // one spooled envelope per resend chunk
		cfg.OnBatchResult = func(BatchResult) {
			if flipped.CompareAndSwap(false, true) {
				client.SetConsent(false)
			}
		}
	})

	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// The purge applies to the undelivered remainder only.
	consentLetters := recorder.byReason(SpoolDropConsent)
	if len(consentLetters) != 1 || len(consentLetters[0].Envelopes) != 1 || !containsEventID(t, consentLetters[0].Envelopes, "evt-flip-2") {
		t.Fatalf("expected exactly the undelivered remainder dead-lettered as consent, got %+v", consentLetters)
	}
	for _, reason := range []SpoolDropReason{SpoolDropConsent, SpoolDropTerminal, SpoolDropCapacity, SpoolDropExpired} {
		for _, letter := range recorder.byReason(reason) {
			if containsEventID(t, letter.Envelopes, "evt-flip-1") {
				t.Fatalf("delivered event evt-flip-1 must never dead-letter, found in a %q letter", reason)
			}
		}
	}
	stats := client.Snapshot()
	if stats.SpoolResent != 1 {
		t.Fatalf("expected SpoolResent=1 for the delivered chunk, got %+v", stats)
	}
	// Exactly one batch reached the server: the second chunk was purged by
	// the denial, never attempted.
	if got := state.batchCount(); got != 1 {
		t.Fatalf("expected exactly the delivered chunk on the wire, got %d requests", got)
	}
	if spoolFileExists(dir) {
		t.Fatalf("expected the denial purge to remove spool.json")
	}
	_ = client.Close(context.Background())
}

func TestSpoolInProcessRetryReusesRetainedBytes(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	client.SetConsent(true)

	// Intake clones Props one level deep: the NESTED map stays shared with
	// the caller, so mutating it after Enqueue would change a re-marshal.
	nested := map[string]any{"k": "v1"}
	if err := client.Enqueue(Event{ID: "evt-bytes-1", Name: "e1", Props: map[string]any{"nested": nested}}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	spooled := readSpoolRecordFile(t, dir)
	if len(spooled.Events) != 1 {
		t.Fatalf("expected the failed batch spooled, got %d events", len(spooled.Events))
	}

	// The caller mutates the nested value AFTER the failure. The in-process
	// retry must resend the RETAINED bytes — the ones just spooled — not a
	// fresh marshal that would drift under the same event_id.
	nested["k"] = "v2"
	state.setOutcome(http.StatusAccepted, "", "")
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}

	bodies := state.allBodies()
	if len(bodies) != 2 {
		t.Fatalf("expected the failed attempt and its retry, got %d bodies", len(bodies))
	}
	firstWire := wireEventBytes(t, bodies[0])
	retryWire := wireEventBytes(t, bodies[1])
	if len(firstWire) != 1 || len(retryWire) != 1 {
		t.Fatalf("expected single-event bodies, got %d/%d", len(firstWire), len(retryWire))
	}
	if retryWire[0] != firstWire[0] {
		t.Fatalf("retry bytes drifted from the first attempt:\n first: %s\n retry: %s", firstWire[0], retryWire[0])
	}
	if retryWire[0] != string(spooled.Events[0]) {
		t.Fatalf("retry bytes drifted from the spooled record:\n spool: %s\n retry: %s", spooled.Events[0], retryWire[0])
	}
	if !strings.Contains(retryWire[0], `"v1"`) || strings.Contains(retryWire[0], `"v2"`) {
		t.Fatalf("expected the retry to carry the retained pre-mutation encoding, got %s", retryWire[0])
	}
	if record := readSpoolRecordFile(t, dir); len(record.Events) != 0 {
		t.Fatalf("expected the delivered batch acked out of the spool, got %s", mustJSON(t, record.Events))
	}
	_ = client.Close(context.Background())
}

func TestSpoolRetryReusesRetainedBytesForPaddedID(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	client.SetConsent(true)

	// The caller pads the id with whitespace; buildEnvelope trims it into
	// the wire event_id, so the retained-request comparison must match in
	// that same canonical form — a raw comparison would rebuild and drift.
	nested := map[string]any{"k": "v1"}
	if err := client.Enqueue(Event{ID: "  evt-pad-1  ", Name: "e1", Props: map[string]any{"nested": nested}}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	spooled := readSpoolRecordFile(t, dir)
	if len(spooled.Events) != 1 || !containsEventID(t, spooled.Events, "evt-pad-1") {
		t.Fatalf("expected the failed batch spooled under the trimmed id, got %s", mustJSON(t, spooled.Events))
	}

	nested["k"] = "v2"
	state.setOutcome(http.StatusAccepted, "", "")
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}

	bodies := state.allBodies()
	if len(bodies) != 2 {
		t.Fatalf("expected the failed attempt and its retry, got %d bodies", len(bodies))
	}
	firstWire := wireEventBytes(t, bodies[0])
	retryWire := wireEventBytes(t, bodies[1])
	if len(firstWire) != 1 || len(retryWire) != 1 {
		t.Fatalf("expected single-event bodies, got %d/%d", len(firstWire), len(retryWire))
	}
	if retryWire[0] != firstWire[0] || retryWire[0] != string(spooled.Events[0]) {
		t.Fatalf("padded-id retry drifted from the retained/spooled encoding:\n first: %s\n retry: %s\n spool: %s", firstWire[0], retryWire[0], spooled.Events[0])
	}
	if !strings.Contains(retryWire[0], `"v1"`) || strings.Contains(retryWire[0], `"v2"`) {
		t.Fatalf("expected the retry to carry the retained pre-mutation encoding, got %s", retryWire[0])
	}
	_ = client.Close(context.Background())
}

func TestSpoolCapacityDeadLetterDeferredUntilEvictionDurable(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, func(cfg *Config) {
		cfg.SpoolMaxEvents = 1
	})
	client.SetConsent(true)

	// evt-defer-1 spools durably first.
	if err := client.Enqueue(Event{ID: "evt-defer-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 1 {
		t.Fatalf("expected the first event durably spooled, got %d", got)
	}

	// The next append evicts it over the 1-event cap — but the removing
	// rewrite FAILS, so the old record (still carrying evt-defer-1) stays on
	// disk. A crash here would reload and resend it: the capacity
	// dead-letter must therefore NOT fire yet. The over-cap append drives the
	// diskSpool directly (the same call spoolFailedBatch makes) instead of
	// racing an Enqueue+Flush against the worker's queue consumption: whether
	// the worker had absorbed the second event before a flush request decided
	// what to retry is a scheduler coin flip, and losing it retried only the
	// retained first event with no eviction attempted at all.
	injectedErr := errors.New("injected rename failure")
	client.spool.renameFn = func(string, string) error { return injectedErr }
	now := time.Now()
	_, added, _, evicted, persistFailed := client.spool.append([]spoolEntry{{
		id:  "evt-defer-2",
		ts:  now.UTC().Format(time.RFC3339Nano),
		raw: spoolTestEnvelope(t, "evt-defer-2", now),
	}}, 0, false, now, func() bool { return true })
	if len(added) != 1 || !persistFailed {
		t.Fatalf("expected the over-cap append accepted with a failed rewrite, got added=%d persistFailed=%v", len(added), persistFailed)
	}
	if len(evicted) != 0 {
		t.Fatalf("a capacity eviction whose rewrite failed must be deferred, not returned, got %d", len(evicted))
	}
	// A full client-layer drain pass (the flush-cadence upkeep) while the
	// rewrite still fails must not release the deferred letter either.
	client.spoolMaintain()
	if letters := recorder.byReason(SpoolDropCapacity); len(letters) != 0 {
		t.Fatalf("a capacity drop whose rewrite failed must not dead-letter yet, got %+v", letters)
	}
	if stats := client.Snapshot(); stats.SpoolEvicted != 0 {
		t.Fatalf("expected SpoolEvicted deferred with the letter, got %+v", stats)
	}
	if record := readSpoolRecordFile(t, dir); !containsEventID(t, record.Events, "evt-defer-1") {
		t.Fatalf("expected the old record still on disk while the rewrite fails, got %s", mustJSON(t, record.Events))
	}

	// The write path recovers: the flush-cadence retry lands the record
	// WITHOUT the evicted entry — the eviction is now final, so the deferred
	// capacity dead-letter fires exactly once.
	client.spool.renameFn = os.Rename
	client.spoolMaintain()
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 1 || !containsEventID(t, record.Events, "evt-defer-2") {
		t.Fatalf("expected the landed record to carry only the survivor, got %s", mustJSON(t, record.Events))
	}
	letters := recorder.byReason(SpoolDropCapacity)
	if len(letters) != 1 || len(letters[0].Envelopes) != 1 || !containsEventID(t, letters[0].Envelopes, "evt-defer-1") {
		t.Fatalf("expected exactly the evicted event dead-lettered once the eviction landed, got %+v", letters)
	}
	if stats := client.Snapshot(); stats.SpoolEvicted != 1 {
		t.Fatalf("expected SpoolEvicted counted at the durable eviction, got %+v", stats)
	}
	client.spoolMaintain()
	if got := recorder.byReason(SpoolDropCapacity); len(got) != 1 {
		t.Fatalf("expected the deferred letter released exactly once, got %+v", got)
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = client.Close(context.Background())
}

func TestSpoolFlushPoisonMemberIsolatedSurvivorsDeliver(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	client.SetConsent(true)

	// evt-rebuild-1 fails retriably: spooled durably, retained by the worker.
	if err := client.Enqueue(Event{ID: "evt-rebuild-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 1 {
		t.Fatalf("expected the failed batch spooled, got %d", got)
	}

	// A later batchmate whose Props cannot serialize joins the retained
	// batch; wait for the worker to absorb it so the flush retry rebuilds ONE
	// batch of both events and attributes the build failure to the poison
	// member alone.
	if err := client.Enqueue(Event{ID: "evt-rebuild-2", Name: "e2", Props: map[string]any{"bad": func() {}}}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(client.queue.ch) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the worker to pull the poison event into its batch")
		}
		time.Sleep(time.Millisecond)
	}

	state.setOutcome(http.StatusAccepted, "", "")
	err := client.Flush(context.Background())
	// The poison member's build error folds into the flush's first-error
	// (like a terminal chunk failure) — but its batchmate DELIVERS in the
	// same flush instead of dying with it.
	if err == nil || !strings.Contains(err.Error(), "encode shardpilot batch") {
		t.Fatalf("expected the poison member's encode failure surfaced, got %v", err)
	}
	arrivals := state.allArrivals()
	last := arrivals[len(arrivals)-1]
	if len(last) != 1 || last[0] != "evt-rebuild-1" {
		t.Fatalf("expected the surviving batchmate delivered, got %v", last)
	}
	// Delivery settled the survivor's spooled copy; the poison member was
	// never on disk (its bytes never marshaled), so nothing dead-letters.
	if record := readSpoolRecordFile(t, dir); len(record.Events) != 0 {
		t.Fatalf("expected the delivered survivor settled out of the record, got %s", mustJSON(t, record.Events))
	}
	if got := recorder.byReason(SpoolDropTerminal); len(got) != 0 {
		t.Fatalf("expected no terminal dead-letters (the survivor delivered, the poison member never spooled), got %+v", got)
	}
	if stats := client.Snapshot(); stats.Dropped != 1 {
		t.Fatalf("expected only the poison member counted dropped, got %+v", stats)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A restart delivers NOTHING: the survivor was acked off the record and
	// the poison member never reached it.
	sent := state.batchCount()
	restarted := newSpoolTestClient(t, server.URL, dir, nil, nil)
	if err := restarted.Flush(context.Background()); err != nil {
		t.Fatalf("restart Flush: %v", err)
	}
	if got := state.batchCount(); got != sent {
		t.Fatalf("expected no redelivery after restart, got %d batches (was %d)", got, sent)
	}
	_ = restarted.Close(context.Background())
}

func TestSpoolWorkerPoisonMemberIsolatedSurvivorsDeliver(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, func(cfg *Config) {
		cfg.BatchSize = 2
	})
	client.SetConsent(true)

	if err := client.Enqueue(Event{ID: "evt-wrebuild-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 1 {
		t.Fatalf("expected the failed batch spooled, got %d", got)
	}

	// The poison batchmate fills the batch to BatchSize, so the WORKER path
	// (publishWorkerBatch) rebuilds on its own: the poison member is dropped
	// attributed, and the retained batchmate delivers in the same attempt
	// instead of being condemned with it.
	state.setOutcome(http.StatusAccepted, "", "")
	if err := client.Enqueue(Event{ID: "evt-wrebuild-2", Name: "e2", Props: map[string]any{"bad": func() {}}}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitFor(t, 3*time.Second, "the worker to deliver the surviving batchmate", func() bool {
		arrivals := state.allArrivals()
		if len(arrivals) == 0 {
			return false
		}
		last := arrivals[len(arrivals)-1]
		return len(last) == 1 && last[0] == "evt-wrebuild-1"
	})
	waitFor(t, 3*time.Second, "the delivered survivor settled off the record", func() bool {
		return len(readSpoolRecordFile(t, dir).Events) == 0
	})
	if got := recorder.byReason(SpoolDropTerminal); len(got) != 0 {
		t.Fatalf("expected no terminal dead-letters (the survivor delivered, the poison member never spooled), got %+v", got)
	}
	if stats := client.Snapshot(); stats.Dropped != 1 {
		t.Fatalf("expected only the poison member counted dropped, got %+v", stats)
	}
	_ = client.Close(context.Background())
}

func TestSpoolCloseRemnantPoisonMemberIsolated(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		WorkspaceID:    "workspace-test",
		AppID:          "app-test",
		EnvironmentID:  "develop",
		Source:         SourceBackend,
		AnonymousID:    "anon-spool-1",
		SpoolDir:       dir,
		SpoolMaxEvents: defaultSpoolMaxEvents,
		SpoolMaxBytes:  defaultSpoolMaxBytes,
	}
	client := &Client{cfg: cfg, clock: realClock{}, queue: newBoundedQueue(4), spool: newDiskSpool(cfg)}
	client.consent.Store(consentStateGranted)
	client.spool.grantPersisted = true

	now := time.Now()
	batch := []Event{
		{ID: "evt-remnant-ok", Name: "e1", Timestamp: now},
		{ID: "evt-remnant-poison", Name: "e2", Timestamp: now, Props: map[string]any{"bad": func() {}}},
	}
	client.spoolCloseRemnant(batch)

	// The poison member is dropped attributed; the rest of the remnant still
	// spools instead of dying with it (the old whole-remnant drop).
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 1 || !containsEventID(t, record.Events, "evt-remnant-ok") {
		t.Fatalf("expected exactly the serializable remnant member spooled, got %s", mustJSON(t, record.Events))
	}
	if stats := client.Snapshot(); stats.Dropped != 1 {
		t.Fatalf("expected the poison member counted dropped, got %+v", stats)
	}
}

func TestSpoolCorruptRecordDiscardedAtLoad(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	writeConsentRecordFile(t, dir, "granted")
	// An unparseable spool.json — truncated by a crash mid-tamper, or plain
	// garbage — proves nothing about what was retained: the load must be a
	// clean start (file removed, no dead-letters, no counters), never a
	// crash or an eviction cascade.
	if err := os.WriteFile(filepath.Join(dir, spoolFileName), []byte(`{"version":1,"events":[{`), 0o600); err != nil {
		t.Fatalf("write corrupt record: %v", err)
	}

	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	if spoolFileExists(dir) {
		t.Fatalf("expected the unparseable record discarded at load")
	}
	if recorder.count() != 0 {
		t.Fatalf("an unparseable record is a clean start, not a drop report; got %d letters", recorder.count())
	}
	if stats := client.Snapshot(); stats.SpoolEvicted != 0 || stats.SpoolExpired != 0 {
		t.Fatalf("expected no drop counters from the discarded record, got %+v", stats)
	}
	if !client.initialDeferUntil.IsZero() {
		t.Fatalf("expected no deferral seeded from a discarded record")
	}

	// The client starts clean and functions normally.
	if err := client.Enqueue(Event{ID: "evt-after-corrupt-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	arrivals := state.allArrivals()
	if len(arrivals) != 1 || len(arrivals[0]) != 1 || arrivals[0][0] != "evt-after-corrupt-1" {
		t.Fatalf("expected only the fresh event published, got %v", arrivals)
	}
	_ = client.Close(context.Background())
}

func TestSpoolWrongVersionRecordDiscardedAtLoad(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	writeConsentRecordFile(t, dir, "granted")
	// A well-formed record whose version this build does not write: its
	// event bytes and its persisted deadline belong to an incompatible
	// layout and must not be trusted — not resent, not seeding the deferral.
	// Discarded as a clean start exactly like an unparseable record.
	record := spoolRecordWire{
		Version:           spoolRecordVersion + 1,
		Events:            []json.RawMessage{spoolTestEnvelope(t, "evt-wrong-version-1", time.Now())},
		RetryAfterUntilMS: time.Now().Add(time.Hour).UnixMilli(),
	}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal wrong-version record: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, spoolFileName), payload, 0o600); err != nil {
		t.Fatalf("write wrong-version record: %v", err)
	}

	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	if spoolFileExists(dir) {
		t.Fatalf("expected the wrong-version record discarded at load")
	}
	if recorder.count() != 0 {
		t.Fatalf("a wrong-version record is a clean start, not a drop report; got %d letters", recorder.count())
	}
	if !client.initialDeferUntil.IsZero() {
		t.Fatalf("expected no deferral seeded from an incompatible record's deadline")
	}

	// The client starts clean: only fresh events publish, never the
	// incompatible record's.
	if err := client.Enqueue(Event{ID: "evt-after-wrong-version-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	arrivals := state.allArrivals()
	if len(arrivals) != 1 || len(arrivals[0]) != 1 || arrivals[0][0] != "evt-after-wrong-version-1" {
		t.Fatalf("expected only the fresh event published (no resends from the discarded record), got %v", arrivals)
	}
	_ = client.Close(context.Background())
}

func TestSetConsentDiskStallDoesNotBlockIntake(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)

	// Stall the consent-record rename mid-decision: SetConsent's disk side
	// must not hold the lock Track/Enqueue take, so intake proceeds while
	// the write is stuck (a slow SpoolDir must never stall the pipeline).
	stalled := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	client.spool.renameFn = func(oldpath, newpath string) error {
		once.Do(func() {
			close(stalled)
			<-release
		})
		return os.Rename(oldpath, newpath)
	}

	decisionDone := make(chan struct{})
	go func() {
		defer close(decisionDone)
		client.SetConsent(true)
	}()
	<-stalled

	enqueued := make(chan error, 1)
	go func() { enqueued <- client.Enqueue(Event{ID: "evt-during-stall-1", Name: "e1"}) }()
	select {
	case err := <-enqueued:
		if err != nil {
			t.Fatalf("Enqueue during the stalled consent write: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Enqueue blocked behind the consent decision's disk write")
	}
	if got := client.Consent(); got != ConsentGranted {
		t.Fatalf("expected the in-memory decision already applied, got %v", got)
	}

	close(release)
	<-decisionDone
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDenialAppliesToIntakeWhileEarlierDecisionDiskStalls(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)

	// Stall the FIRST consent-record rename (the grant's). The denial issued
	// while it is stuck must take effect on intake IMMEDIATELY — the
	// in-memory flip must never queue behind an earlier decision's disk
	// write — while its own record write waits its turn, so the persisted
	// record still lands in decision order (denied last).
	stalled := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	client.spool.renameFn = func(oldpath, newpath string) error {
		once.Do(func() {
			close(stalled)
			<-release
		})
		return os.Rename(oldpath, newpath)
	}

	grantDone := make(chan struct{})
	go func() {
		defer close(grantDone)
		client.SetConsent(true)
	}()
	<-stalled

	denyDone := make(chan struct{})
	go func() {
		defer close(denyDone)
		client.SetConsent(false)
	}()
	waitFor(t, 3*time.Second, "the denial visible to intake while the grant's write is stalled", func() bool {
		return client.Consent() == ConsentDenied
	})
	if err := client.Enqueue(Event{ID: "evt-under-denial-1", Name: "e1"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected intake rejecting under the denial, got %v", err)
	}
	select {
	case <-grantDone:
		t.Fatal("the grant decision finished while its rename was supposed to be stalled")
	default:
	}

	close(release)
	<-grantDone
	<-denyDone

	// Disk order equals decision order: the denial's record is the one on
	// disk, even though it was ISSUED mid-stall.
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the denied record persisted last, got (%v, %v)", recorded, ok)
	}
	// Transmission order equals decision order too.
	waitFor(t, 5*time.Second, "both decisions transmitted", func() bool {
		return state.consentCount() == 2
	})
	first, second := state.consentAt(0), state.consentAt(1)
	if got := consentBodyAnalytics(t, first.body); got != true {
		t.Fatalf("expected the grant transmitted first, got %v", first.body)
	}
	if got := consentBodyAnalytics(t, second.body); got != false {
		t.Fatalf("expected the denial transmitted second, got %v", second.body)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// consentBodyAnalytics extracts categories.analytics from a captured consent
// request body.
func consentBodyAnalytics(t *testing.T, body map[string]any) bool {
	t.Helper()
	categories, ok := body["categories"].(map[string]any)
	if !ok {
		t.Fatalf("consent body carries no categories: %v", body)
	}
	analytics, ok := categories["analytics"].(bool)
	if !ok {
		t.Fatalf("consent body carries no boolean analytics category: %v", body)
	}
	return analytics
}

func TestCloseWaitsForStalledGrantDecision(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)

	stalled := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	client.spool.renameFn = func(oldpath, newpath string) error {
		once.Do(func() {
			close(stalled)
			<-release
		})
		return os.Rename(oldpath, newpath)
	}

	decisionDone := make(chan struct{})
	go func() {
		defer close(decisionDone)
		client.SetConsent(true)
	}()
	<-stalled

	// Close must fence behind the in-flight decision: without the fence it
	// would stop and drain the consent sender BEFORE the stalled decision's
	// handoff, and the grant would silently never transmit.
	closeErr := make(chan error, 1)
	go func() { closeErr <- client.Close(context.Background()) }()
	select {
	case err := <-closeErr:
		t.Fatalf("Close returned (%v) while a pre-Close decision was still persisting", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	<-decisionDone
	if err := <-closeErr; err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The fenced Close drained the sender AFTER the handoff: the decision
	// reached the server, and its record reached disk.
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected the pre-Close grant transmitted before Close returned, got %d consent posts", got)
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("expected the granted record persisted, got (%v, %v)", recorded, ok)
	}
}

func TestCloseWaitsForStalledDenialDecision(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	// A completed grant first, so an abandoned denial would leave a STALE
	// GRANTED record on disk — the exact hazard the Close fence prevents.
	client.SetConsent(true)

	stalled := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	client.spool.removeFn = func(path string) error {
		once.Do(func() {
			close(stalled)
			<-release
		})
		return os.Remove(path)
	}

	decisionDone := make(chan struct{})
	go func() {
		defer close(decisionDone)
		client.SetConsent(false)
	}()
	<-stalled

	closeErr := make(chan error, 1)
	go func() { closeErr <- client.Close(context.Background()) }()
	select {
	case err := <-closeErr:
		t.Fatalf("Close returned (%v) while the denial's purge was still in flight", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	<-decisionDone
	if err := <-closeErr; err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The denial completed before teardown: denied record on disk (never a
	// stale grant), and both decisions transmitted in order.
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the denied record persisted before Close returned, got (%v, %v)", recorded, ok)
	}
	if got := state.consentCount(); got != 2 {
		t.Fatalf("expected both decisions transmitted before Close returned, got %d consent posts", got)
	}
	if got := consentBodyAnalytics(t, state.consentAt(1).body); got != false {
		t.Fatalf("expected the denial transmitted last, got %v", state.consentAt(1).body)
	}
}

func TestSpoolDuplicateAppendRetriesDirtyWrite(t *testing.T) {
	dir := t.TempDir()
	s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: 8, SpoolMaxBytes: 1 << 20})
	now := time.Now()
	entry := spoolEntry{id: "evt-dup-1", ts: now.UTC().Format(time.RFC3339Nano), raw: spoolTestEnvelope(t, "evt-dup-1", now)}

	injectedErr := errors.New("injected rename failure")
	s.renameFn = func(string, string) error { return injectedErr }
	refused, added, _, _, persistFailed := s.append([]spoolEntry{entry}, 0, false, now, func() bool { return true })
	if refused || len(added) != 1 || !persistFailed {
		t.Fatalf("expected the first append accepted with a failed write, got refused=%v added=%d persistFailed=%v", refused, len(added), persistFailed)
	}
	if spoolFileExists(dir) {
		t.Fatalf("expected no record on disk while the write fails")
	}

	// The disk error clears, and the SAME batch re-appends — exactly what the
	// retained batch's next in-process retry does. Every entry is a duplicate
	// and no deadline rides along: before the fix this early-returned without
	// ever retrying the dirty write.
	s.renameFn = os.Rename
	refused, added, _, _, persistFailed = s.append([]spoolEntry{entry}, 0, false, now, func() bool { return true })
	if refused || len(added) != 0 || persistFailed {
		t.Fatalf("expected the duplicate append to retry and land the write, got refused=%v added=%d persistFailed=%v", refused, len(added), persistFailed)
	}
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 1 || !containsEventID(t, record.Events, "evt-dup-1") {
		t.Fatalf("expected the recovered write durable, got %s", mustJSON(t, record.Events))
	}
	if s.dirty {
		t.Fatalf("expected the dirty flag cleared by the retried write")
	}
	if got := s.takeBecameDurable(); got != 1 {
		t.Fatalf("expected the entry counted durable exactly once the retried write landed, got %d", got)
	}
}

func TestSpoolCloseRetriesDirtyWrite(t *testing.T) {
	_, server := newSpoolTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)

	// An append accepts the entry into the mirror but the record write fails.
	injectedErr := errors.New("injected rename failure")
	client.spool.renameFn = func(string, string) error { return injectedErr }
	now := time.Now()
	_, added, _, _, persistFailed := client.spool.append([]spoolEntry{{
		id:  "evt-close-1",
		ts:  now.UTC().Format(time.RFC3339Nano),
		raw: spoolTestEnvelope(t, "evt-close-1", now),
	}}, 0, false, now, func() bool { return true })
	if len(added) != 1 || !persistFailed {
		t.Fatalf("expected the append accepted with a failed write, got added=%d persistFailed=%v", len(added), persistFailed)
	}
	if spoolFileExists(dir) {
		t.Fatalf("expected no record on disk while the write fails")
	}

	// The disk error clears before shutdown. Close's final settle must retry
	// the dirty write even though nothing else touches the spool (empty
	// remnant, no flush-cadence tick left) — exiting without it would lose
	// the event despite the disk having recovered.
	client.spool.renameFn = os.Rename
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 1 || !containsEventID(t, record.Events, "evt-close-1") {
		t.Fatalf("expected Close to land the recovered write durably, got %s", mustJSON(t, record.Events))
	}
}

func TestSpoolLoadRefusedWhenDirCannotBePrivate(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	writeConsentRecordFile(t, dir, "granted")
	now := time.Now()
	writeSpoolRecordFile(t, dir, 0, spoolTestEnvelope(t, "evt-dirref-1", now))

	cfg := Config{
		WorkspaceID:    "workspace-test",
		AppID:          "app-test",
		EnvironmentID:  "develop",
		AnonymousID:    "anon-spool-1",
		SpoolDir:       dir,
		SpoolMaxEvents: 8,
		SpoolMaxBytes:  1 << 20,
	}
	client := &Client{cfg: cfg, clock: realClock{}, spool: newDiskSpool(cfg)}
	// The dir pre-exists looser than 0700 and the tighten is refused: the
	// persisted grant and record must NOT be trusted — no load, no resend
	// seeding — the same fail-closed posture as the refused-tighten write
	// path.
	client.spool.chmodFn = func(string, os.FileMode) error {
		return errors.New("chmod refused")
	}
	if letters := client.initSpool(); len(letters) != 0 {
		t.Fatalf("a refused dir tighten must not produce init dead-letters, got %+v", letters)
	}
	if client.spool.hasResendWork() {
		t.Fatalf("a record from an untightenable dir must not seed resend work")
	}
	if got := client.Snapshot().LastError; got != "spool_dir_private_failed" {
		t.Fatalf("expected spool_dir_private_failed surfaced, got %q", got)
	}
	if !spoolFileExists(dir) {
		t.Fatalf("the untrusted record must be left in place for a later run with fixed permissions")
	}
}

func TestSpoolDeadLetterEnvelopesAreCopies(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	var mutated atomic.Bool
	client := newSpoolTestClient(t, server.URL, dir, nil, func(cfg *Config) {
		cfg.SpoolMaxEvents = 1
		cfg.OnSpoolDeadLetter = func(letter SpoolDeadLetter) {
			// A worst-case integrator: shred every envelope byte slice the
			// callback hands over. The retained/mirror bytes must be
			// unaffected — the payload is the integrator's copy.
			for _, envelope := range letter.Envelopes {
				for i := range envelope {
					envelope[i] = 'X'
				}
			}
			mutated.Store(true)
		}
	})
	client.SetConsent(true)

	if err := client.Enqueue(Event{ID: "evt-mut-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	// evt-mut-2 evicts evt-mut-1 over the 1-event cap: the capacity
	// dead-letter fires with evt-mut-1's envelope — whose spooled bytes came
	// from the same marshal as the worker's retained request — and the
	// callback shreds it.
	if err := client.Enqueue(Event{ID: "evt-mut-2", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(client.queue.ch) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the worker to pull the second event into its batch")
		}
		time.Sleep(time.Millisecond)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	if !mutated.Load() {
		t.Fatalf("expected the capacity dead-letter callback to have run")
	}

	// The retained batch redelivers on recovery: its wire bytes must be the
	// ORIGINAL encoding, not the callback-mutated one.
	state.setOutcome(http.StatusAccepted, "", "")
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("recovery Flush: %v", err)
	}
	bodies := state.allBodies()
	firstWire := wireEventBytes(t, bodies[0])
	lastWire := wireEventBytes(t, bodies[len(bodies)-1])
	if len(firstWire) != 1 || len(lastWire) != 2 {
		t.Fatalf("expected 1-event first body and 2-event final body, got %d/%d", len(firstWire), len(lastWire))
	}
	if lastWire[0] != firstWire[0] || strings.Contains(lastWire[0], "XX") {
		t.Fatalf("callback mutation leaked into the retry bytes:\n first: %s\n retry: %s", firstWire[0], lastWire[0])
	}
	_ = client.Close(context.Background())
}

func TestSpoolPrivateDirTightensCwdSpoolDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Chdir(dir)

	// SpoolDir "." resolves record paths to bare filenames (filepath.Dir
	// "."): the 0700 state-directory guarantee must hold for the actual
	// directory all the same.
	s := newDiskSpool(Config{SpoolDir: ".", SpoolMaxEvents: 8, SpoolMaxBytes: 1 << 20})
	now := time.Now()
	refused, added, _, _, persistFailed := s.append([]spoolEntry{spoolEntryFromEnvelope(t, spoolTestEnvelope(t, "evt-cwd-1", now))}, 0, false, now, func() bool { return true })
	if refused || len(added) != 1 || persistFailed {
		t.Fatalf("append: refused=%v added=%d persistFailed=%v", refused, len(added), persistFailed)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("a cwd spool dir must be tightened to 0700 like any explicit path, got %v", info.Mode().Perm())
	}
	record := readSpoolRecordFile(t, ".")
	if len(record.Events) != 1 || !containsEventID(t, record.Events, "evt-cwd-1") {
		t.Fatalf("expected the record written through the tightened cwd, got %s", mustJSON(t, record.Events))
	}
}

func TestSpoolOversizedConsentRecordTreatedAsNoDecision(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	// An oversized consent.json — stale, corrupt, or locally planted — must
	// be read only through the bound and treated as no usable decision:
	// never loaded whole, never trusted as a grant. The fixture PARSES as a
	// granted record for this actor (a padded-but-valid JSON object), so
	// only the size bound stands between it and being trusted.
	oversized := fmt.Sprintf(`{"consent_analytics":"granted","actor_digest":%q,"pad":%q}`,
		spoolTestActorDigest(), strings.Repeat("j", consentRecordReadLimit+4096))
	if err := os.WriteFile(consentRecordPath(dir), []byte(oversized), 0o600); err != nil {
		t.Fatalf("write oversized consent record: %v", err)
	}
	if _, ok := loadConsentRecord(dir, spoolTestActorDigest()); ok {
		t.Fatalf("an over-limit consent record must be no usable decision")
	}
	now := time.Now()
	writeSpoolRecordFile(t, dir, 0, spoolTestEnvelope(t, "evt-bigconsent-1", now))

	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	// No usable decision: the record purges at init (consent drop) and the
	// client starts normally.
	letters := recorder.byReason(SpoolDropConsent)
	if len(letters) != 1 || !containsEventID(t, letters[0].Envelopes, "evt-bigconsent-1") {
		t.Fatalf("expected the record purged as a consent drop, got %+v", letters)
	}
	if spoolFileExists(dir) {
		t.Fatalf("expected the spool record purged from disk")
	}
	_ = client.Close(context.Background())
}

func TestSpoolHintlessRetryClearsPersistedDeadline(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusTooManyRequests, "rate_limited", "120")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	client.SetConsent(true)

	if err := client.Enqueue(Event{ID: "evt-hintless-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the 429 surfaced")
	}
	if got := readSpoolRecordFile(t, dir).RetryAfterUntilMS; got <= time.Now().UnixMilli() {
		t.Fatalf("expected a live persisted deadline from the 429, got %d", got)
	}

	// The next retriable failure carries NO Retry-After: the persisted
	// window is withdrawn — the server's latest word governs the disk
	// exactly as it governs live pacing.
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the 500 surfaced")
	}
	record := readSpoolRecordFile(t, dir)
	if record.RetryAfterUntilMS != 0 {
		t.Fatalf("expected the hintless failure to clear the persisted deadline, got %d", record.RetryAfterUntilMS)
	}
	if len(record.Events) != 1 {
		t.Fatalf("expected the event still spooled, got %d", len(record.Events))
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = client.Close(context.Background())

	// A restart must not defer: no window is being asserted anymore.
	restarted := newSpoolTestClient(t, server.URL, dir, nil, nil)
	if !restarted.initialDeferUntil.IsZero() {
		t.Fatalf("expected no re-seeded deferral after the hintless clear, got %v", restarted.initialDeferUntil)
	}
	_ = restarted.Close(context.Background())
}

func TestSpoolResendHintlessFailureClearsPersistedDeadline(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	writeConsentRecordFile(t, dir, "granted")
	now := time.Now()
	// A record persisted under a live Retry-After window by a previous
	// process.
	writeSpoolRecordFile(t, dir, now.Add(time.Hour).UnixMilli(), spoolTestEnvelope(t, "evt-resend-clear-1", now))

	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	if client.initialDeferUntil.IsZero() {
		t.Fatalf("expected the persisted deadline to seed the deferral")
	}
	// The explicit-Flush resend fails WITHOUT a Retry-After: the persisted
	// window is withdrawn while the event stays spooled for retry.
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the resend failure surfaced")
	}
	record := readSpoolRecordFile(t, dir)
	if record.RetryAfterUntilMS != 0 {
		t.Fatalf("expected the hintless resend failure to clear the persisted deadline, got %d", record.RetryAfterUntilMS)
	}
	if len(record.Events) != 1 || !containsEventID(t, record.Events, "evt-resend-clear-1") {
		t.Fatalf("expected the event still spooled for retry, got %s", mustJSON(t, record.Events))
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = client.Close(context.Background())
}

func TestSpoolDuplicateIDBatchCountsSpooledOnce(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	client.SetConsent(true)

	// The caller supplies the same Event.ID twice in one batch (legal — the
	// server de-duplicates by event_id): the spool stores ONE envelope for
	// that id and must count one.
	if err := client.Enqueue(Event{ID: "evt-dupid-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Enqueue(Event{ID: "evt-dupid-1", Name: "e1b"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Enqueue(Event{ID: "evt-dupid-2", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(client.queue.ch) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the worker to pull the batch")
		}
		time.Sleep(time.Millisecond)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}

	if got := len(readSpoolRecordFile(t, dir).Events); got != 2 {
		t.Fatalf("expected two unique envelopes spooled, got %d", got)
	}
	if stats := client.Snapshot(); stats.Spooled != 2 {
		t.Fatalf("expected Spooled to count unique ids (2), got Spooled=%d", stats.Spooled)
	}
	state.setOutcome(http.StatusAccepted, "", "")
	_ = client.Close(context.Background())
}

func TestSpoolLoadClampsPersistedRetryDeadline(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	writeConsentRecordFile(t, dir, "granted")
	now := time.Now()
	// A far-future persisted deadline (72h — a forward clock at write time,
	// or tampering): the load must clamp not only the seeded deferral but
	// the RE-PERSISTED value, or every restart would park a fresh 24h.
	writeSpoolRecordFile(t, dir, now.Add(72*time.Hour).UnixMilli(), spoolTestEnvelope(t, "evt-clamp-1", now))

	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	if got := client.initialDeferUntil; got.After(now.Add(spoolMaxDeferralSeed + time.Minute)) {
		t.Fatalf("expected the seeded deferral clamped to 24h, got %v", got)
	}
	record := readSpoolRecordFile(t, dir)
	if max := now.Add(spoolMaxDeferralSeed + time.Minute).UnixMilli(); record.RetryAfterUntilMS > max {
		t.Fatalf("expected the RE-PERSISTED deadline clamped (<= now+24h), got %d > %d", record.RetryAfterUntilMS, max)
	}
	if record.RetryAfterUntilMS <= now.UnixMilli() {
		t.Fatalf("expected a live clamped deadline persisted, got %d", record.RetryAfterUntilMS)
	}
	_ = client.Close(context.Background())
}

func TestSpoolLiveRetryAckHonorsPerEventVerdicts(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	client.SetConsent(true)

	if err := client.Enqueue(Event{ID: "evt-live-rej-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 1 {
		t.Fatalf("expected the failed batch spooled, got %d", got)
	}

	// The in-process retry's 202 carries a per-event rejected verdict: the
	// spooled copy must dead-letter terminal, exactly like a restart-loaded
	// resend would — never vanish as if delivered.
	state.setAcceptedBody(`{"accepted":0,"rejected":1,"duplicates":0,"events":[` +
		`{"event_id":"evt-live-rej-1","status":"rejected","code":"invalid_event"}]}`)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 0 {
		t.Fatalf("expected the verdicted event settled out of the record, got %d", got)
	}
	terminal := recorder.byReason(SpoolDropTerminal)
	if len(terminal) != 1 || len(terminal[0].Envelopes) != 1 || !containsEventID(t, terminal[0].Envelopes, "evt-live-rej-1") {
		t.Fatalf("expected exactly the rejected event dead-lettered terminal, got %+v", terminal)
	}
	if stats := client.Snapshot(); stats.SpoolResent != 0 {
		t.Fatalf("a live-path ack must not count SpoolResent, got %+v", stats)
	}
	_ = client.Close(context.Background())
}

func TestSpoolDenialDropClearsRetainedBytes(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	client.SetConsent(true)

	if err := client.Enqueue(Event{ID: "evt-stale-1", Name: "e1", Props: map[string]any{"v": "old"}}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	// The denial-drained flush discards the held batch — and must discard
	// the retained wire bytes with it.
	client.SetConsent(false)
	_ = client.Flush(context.Background())
	client.SetConsent(true)

	// A NEW event reusing the same explicit id must publish ITS bytes, not
	// the denied batch's stale retained encoding.
	state.setOutcome(http.StatusAccepted, "", "")
	if err := client.Enqueue(Event{ID: "evt-stale-1", Name: "e1", Props: map[string]any{"v": "new"}}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bodies := state.allBodies()
	last := string(bodies[len(bodies)-1])
	if !strings.Contains(last, `"new"`) || strings.Contains(last, `"old"`) {
		t.Fatalf("expected the post-grant publish to carry the fresh encoding, got %s", last)
	}
	_ = client.Close(context.Background())
}

func TestSpoolOwedWipeStaysClosedUntilMarkerRemoved(t *testing.T) {
	dir := t.TempDir()
	writeSpoolRecordFile(t, dir, 0, spoolTestEnvelope(t, "evt-owed-old-1", time.Now()))
	if err := createWipeOwedMarker(dir); err != nil {
		t.Fatalf("marker: %v", err)
	}

	// The record wipe succeeds but the MARKER cannot come off: the spool
	// must stay fail-closed — a stale marker would re-condemn (wipe at the
	// next start) anything spooled after this settle.
	s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: 10, SpoolMaxBytes: 1 << 20})
	if !s.owed {
		t.Fatalf("expected the marker to open the spool owed")
	}
	markerPath := spoolWipeOwedPath(dir)
	s.removeFn = func(path string) error {
		if path == markerPath {
			return errors.New("marker removal refused")
		}
		return os.Remove(path)
	}
	if s.settleOwedWipe() {
		t.Fatalf("a wipe whose marker cannot come off must not settle")
	}
	if spoolFileExists(dir) {
		t.Fatalf("expected the record itself wiped")
	}
	refused, _, _, _, _ := s.append([]spoolEntry{{id: "evt-owed-new-1", ts: time.Now().UTC().Format(time.RFC3339Nano), raw: json.RawMessage(`{}`)}}, 0, false, time.Now(), func() bool { return true })
	if !refused {
		t.Fatalf("an append must stay refused while the stale marker persists")
	}

	// Marker removal recovers: the wipe settles and the spool reopens.
	s.removeFn = os.Remove
	if !s.settleOwedWipe() {
		t.Fatalf("expected the retried settle to land")
	}
	if s.owedWipe() {
		t.Fatalf("expected the spool reopened")
	}
	if wipeOwedMarkerExists(dir) {
		t.Fatalf("expected the marker gone")
	}
	refused, added, _, _, _ := s.append([]spoolEntry{spoolEntryFromEnvelope(t, spoolTestEnvelope(t, "evt-owed-new-2", time.Now()))}, 0, false, time.Now(), func() bool { return true })
	if refused || len(added) != 1 {
		t.Fatalf("expected the reopened spool to accept appends, got refused=%v added=%d", refused, len(added))
	}
}

func spoolEntryFromEnvelope(t *testing.T, raw json.RawMessage) spoolEntry {
	t.Helper()
	var envelope spoolEnvelopeWire
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	return spoolEntry{id: envelope.EventID, ts: envelope.EventTS, raw: raw}
}

func TestSpoolReadLimitScalesWithEventCap(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	const n = 80_000
	envelopes := make([]json.RawMessage, 0, n)
	rawTotal := 0
	for i := 0; i < n; i++ {
		raw := json.RawMessage(fmt.Sprintf(`{"event_id":"e%06d","event_ts":%q}`, i, now))
		rawTotal += len(raw)
		envelopes = append(envelopes, raw)
	}
	writeSpoolRecordFile(t, dir, 0, envelopes...)

	// A cap-full self-written record: raw bytes equal the byte cap, and the
	// ~80k array separators alone exceed the fixed 64 KiB framing allowance
	// — the read limit must scale with the event cap or the SDK discards
	// its own record at load.
	s := newDiskSpool(Config{SpoolDir: dir, SpoolMaxEvents: 100_000, SpoolMaxBytes: rawTotal})
	outcome := s.load(time.Now())
	if len(s.entries) != n {
		t.Fatalf("expected the cap-full record loaded whole, got %d of %d entries (evicted %d, expired %d)",
			len(s.entries), n, len(outcome.evicted), len(outcome.expired))
	}
}
