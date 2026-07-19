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
	"sync"
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
	client.spoolFailedBatch(request, nil)

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
	if stats := client.Snapshot(); stats.SpoolPersistFailed == 0 {
		t.Fatalf("expected SpoolPersistFailed counted, got %+v", stats)
	}
	if spoolFileExists(dir) {
		t.Fatalf("expected no record file while the write fails")
	}

	// The mirror stayed authoritative: once writes work again, the next
	// append persists the earlier event too.
	client.spool.renameFn = os.Rename
	if err := client.Enqueue(Event{ID: "evt-persist-2", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	record := readSpoolRecordFile(t, dir)
	if len(record.Events) != 2 || !containsEventID(t, record.Events, "evt-persist-1") || !containsEventID(t, record.Events, "evt-persist-2") {
		t.Fatalf("expected the recovered write to persist the whole mirror, got %s", mustJSON(t, record.Events))
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
	if refused, _, _, persistFailed := spoolA.append([]spoolEntry{e1}, 0, allowed); refused || persistFailed {
		t.Fatalf("append A: refused=%v persistFailed=%v", refused, persistFailed)
	}
	if refused, _, _, persistFailed := spoolB.append([]spoolEntry{e2}, 0, allowed); refused || persistFailed {
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
