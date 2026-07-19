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

// floorTestServer records EVERY analytics-plane arrival in one global order
// (consent posts and event batches interleaved), so tests can pin
// receipts-before-batches and AC-8's exactly-one-request shape.
type floorTestServer struct {
	t *testing.T

	mu                sync.Mutex
	order             []string
	consents          []map[string]any
	batches           [][]string
	consentStatus     int
	consentRetryAfter string
}

func newFloorTestServer(t *testing.T) (*floorTestServer, *httptest.Server) {
	t.Helper()
	state := &floorTestServer{t: t, consentStatus: http.StatusOK}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/events:batch":
			var request struct {
				Events []struct {
					EventID string `json:"event_id"`
				} `json:"events"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode batch request: %v", err)
			}
			ids := make([]string, 0, len(request.Events))
			for _, event := range request.Events {
				ids = append(ids, event.EventID)
			}
			state.mu.Lock()
			state.order = append(state.order, "batch")
			state.batches = append(state.batches, ids)
			state.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprintf(w, `{"accepted":%d,"rejected":0,"duplicates":0}`, len(ids))
		case "/v1/consent":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode consent request: %v", err)
			}
			state.mu.Lock()
			state.order = append(state.order, "consent")
			state.consents = append(state.consents, body)
			status := state.consentStatus
			retryAfter := state.consentRetryAfter
			state.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			if retryAfter != "" {
				w.Header().Set("Retry-After", retryAfter)
			}
			if status == http.StatusNoContent {
				// An empty-body acknowledgement: any 2xx acknowledges.
				w.WriteHeader(status)
				return
			}
			if status < 200 || status >= 300 {
				w.WriteHeader(status)
				fmt.Fprint(w, `{"error":{"code":"test","message":"test"}}`)
				return
			}
			fmt.Fprint(w, `{"recorded":true,"replayed":false}`)
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	return state, server
}

func (s *floorTestServer) setConsentOutcome(status int, retryAfter string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consentStatus = status
	s.consentRetryAfter = retryAfter
}

func (s *floorTestServer) snapshotOrder() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.order...)
}

func (s *floorTestServer) consentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.consents)
}

func (s *floorTestServer) consentAt(i int) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consents[i]
}

func (s *floorTestServer) batchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.batches)
}

func newFloorTestClient(t *testing.T, serverURL, spoolDir string, mutate func(*Config)) *Client {
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
		ConsentFloor:  &ConsentFloorConfig{},
		BatchSize:     4,
		BufferSize:    16,
		FlushInterval: time.Hour,
		HTTPTimeout:   time.Second,
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

// clearConsentDeferral releases an armed consent-plane deferral so a test
// can proceed past a parked window without waiting it out.
func clearConsentDeferral(client *Client) {
	client.consentOutbox.mu.Lock()
	client.consentOutbox.deferUntil = time.Time{}
	client.consentOutbox.mu.Unlock()
}

func consentBoolCategory(t *testing.T, body map[string]any) bool {
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

func TestConsentFloorDefaultOffBehaviorUnchanged(t *testing.T) {
	// The equivalence proof for the default: with ConsentFloor ABSENT the
	// client keeps the documented server-side posture byte-for-byte — the
	// pipeline is OPEN under unknown consent, SetConsent posts its receipt
	// fire-and-forget, and no consent-outbox file is ever created. (The
	// rest of this package's suite runs entirely without ConsentFloor and
	// is the broader half of the same proof.)
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		cfg.ConsentFloor = nil
	})

	// Unknown consent, floor off: intake and publishing are open.
	if err := client.Enqueue(Event{ID: "evt-off-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue under unknown consent must stay open with the floor off: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := state.batchCount(); got != 1 {
		t.Fatalf("expected the batch published under unknown consent, got %d", got)
	}
	// SetConsent stays fire-and-forget: one post, no outbox file.
	client.SetConsent(true)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected the fire-and-forget consent post, got %d", got)
	}
	if _, err := os.Stat(filepath.Join(dir, consentOutboxFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected no consent outbox file with the floor off, got %v", err)
	}
	if stats := client.Snapshot(); stats.ConsentRecorded != 0 || stats.ConsentFailed != 0 ||
		stats.ConsentOutboxEvicted != 0 || stats.ConsentOutboxPersistFailed != 0 || stats.LastConsentError != "" {
		t.Fatalf("expected zeroed consent-floor counters with the floor off, got %+v", stats)
	}
}

func TestConsentFloorUnknownRefusesIntakeAndStaysDark(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)

	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("expected ErrConsentUnknown from Track, got %v", err)
	}
	if err := client.Enqueue(Event{Name: "e2"}); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("expected ErrConsentUnknown from Enqueue, got %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("an empty flush must succeed: %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A consent-first install with no decision transmits NOTHING.
	if got := state.snapshotOrder(); len(got) != 0 {
		t.Fatalf("expected a fully dark undecided session, got %v", got)
	}
	if stats := client.Snapshot(); stats.Dropped != 2 {
		t.Fatalf("expected both refused events counted dropped, got %+v", stats)
	}
}

func TestConsentFloorGrantReceiptPrecedesBatch(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)

	client.SetConsent(true)
	if err := client.Enqueue(Event{ID: "evt-floor-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue after the grant: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	order := state.snapshotOrder()
	if len(order) < 2 || order[0] != "consent" {
		t.Fatalf("expected the grant receipt on the wire before the batch, got %v", order)
	}
	if got := state.batchCount(); got != 1 {
		t.Fatalf("expected the batch delivered after the receipt, got %d", got)
	}
	body := state.consentAt(0)
	if !consentBoolCategory(t, body) {
		t.Fatalf("expected an analytics grant receipt, got %v", body)
	}
	for _, field := range []string{"idempotency_key", "decided_at", "actor_identifier", "workspace_id", "app_id", "environment_id"} {
		if value, ok := body[field].(string); !ok || value == "" {
			t.Fatalf("expected receipt field %q present, got %v", field, body)
		}
	}
	// The anonymous-id retention snapshot NEVER rides the wire.
	if _, present := body["anonymous_id"]; present {
		t.Fatalf("anonymous_id must never reach the wire, got %v", body)
	}
	if _, present := body["reason"]; present {
		t.Fatalf("a plain grant carries no reason, got %v", body)
	}
	if stats := client.Snapshot(); stats.ConsentRecorded != 1 {
		t.Fatalf("expected the acknowledged receipt counted, got %+v", stats)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorParkedGrantGatesEventLegs(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()
	// The receipt parks: a retryable 503 carrying Retry-After (the 5xx
	// pass-through is load-bearing — the strict-consent lane answers 503).
	state.setConsentOutcome(http.StatusServiceUnavailable, "30")

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)

	client.SetConsent(true)
	// The decision-time dispatch (worker wake) hands the receipt once and
	// parks the plane behind the server's window.
	waitFor(t, 3*time.Second, "the first receipt attempt", func() bool {
		return state.consentCount() >= 1
	})

	// Intake stays OPEN under the grant; only the event LEGS hold.
	if err := client.Enqueue(Event{ID: "evt-gated-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue must not be gated: %v", err)
	}
	if err := client.Flush(context.Background()); !errors.Is(err, ErrConsentReceiptPending) {
		t.Fatalf("expected the flush gated on the parked grant, got %v", err)
	}
	if err := client.Track(context.Background(), Event{Name: "e2"}); !errors.Is(err, ErrConsentReceiptPending) {
		t.Fatalf("expected Track gated on the parked grant, got %v", err)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected no batch on the wire while the grant is parked, got %d", got)
	}

	// The server heals and the window ends: the receipt delivers, the gate
	// releases, and the held events follow.
	state.setConsentOutcome(http.StatusOK, "")
	clearConsentDeferral(client)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after the gate released: %v", err)
	}
	order := state.snapshotOrder()
	if state.batchCount() != 1 || order[len(order)-1] != "batch" {
		t.Fatalf("expected the held batch delivered after the receipt, got %v", order)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorReceiptRetainedAcrossRestartAndResentVerbatim(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the failed decision-time dispatch", func() bool {
		return state.consentCount() >= 1
	})
	minted := state.consentAt(0)
	// The receipt is durably retained, so teardown COMPLETES — durability
	// is the outbox's whole point.
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("expected Close to complete over the durable outbox, got %v", err)
	}

	// Relaunch: the persisted decision is the live state, the retained
	// grant re-sends VERBATIM (same idempotency key, same decided_at)
	// before any batch, and the reloaded grant itself is the gate.
	state.setConsentOutcome(http.StatusOK, "")
	restarted := newFloorTestClient(t, server.URL, dir, nil)
	if got := restarted.Consent(); got != ConsentGranted {
		t.Fatalf("expected the persisted grant as the live state, got %v", got)
	}
	if err := restarted.Enqueue(Event{ID: "evt-restart-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := restarted.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := state.consentCount(); got < 2 {
		t.Fatalf("expected the retained receipt re-sent after restart, got %d posts", got)
	}
	resent := state.consentAt(1)
	if resent["idempotency_key"] != minted["idempotency_key"] || resent["decided_at"] != minted["decided_at"] {
		t.Fatalf("expected the retained receipt re-sent verbatim, minted %v resent %v", minted, resent)
	}
	order := state.snapshotOrder()
	batchIndex, consentIndex := -1, -1
	for i, kind := range order {
		if kind == "batch" && batchIndex == -1 {
			batchIndex = i
		}
		if i > 0 && kind == "consent" && consentIndex == -1 {
			consentIndex = i
		}
	}
	if consentIndex == -1 || batchIndex == -1 || consentIndex > batchIndex {
		t.Fatalf("expected the reloaded receipt dispatched before the first post-restart batch, got %v", order)
	}
	if err := restarted.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorDecisionTrailDeliversInOrder(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()
	// Park delivery so BOTH decisions are retained before anything sends:
	// the trail is append-only — a later denial never withdraws the
	// appended grant receipt; both deliver, in decision order.
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	client.SetConsent(false)
	if got := client.Consent(); got != ConsentDenied {
		t.Fatalf("expected the denial as the live state, got %v", got)
	}
	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected Track refused under the denial, got %v", err)
	}

	state.setConsentOutcome(http.StatusOK, "")
	clearConsentDeferral(client)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	waitFor(t, 3*time.Second, "both receipts delivered", func() bool {
		granted, denied := 0, 0
		for i := 0; i < state.consentCount(); i++ {
			if consentBoolCategory(t, state.consentAt(i)) {
				granted++
			} else {
				denied++
			}
		}
		return granted >= 1 && denied >= 1
	})
	// Decision order on the wire: the grant's SUCCESSFUL delivery precedes
	// the denial's (parked attempts may precede both).
	grantIndex, denyIndex := -1, -1
	for i := 0; i < state.consentCount(); i++ {
		if consentBoolCategory(t, state.consentAt(i)) {
			grantIndex = i
		} else if denyIndex == -1 {
			denyIndex = i
		}
	}
	if grantIndex == -1 || denyIndex == -1 || grantIndex > denyIndex {
		t.Fatalf("expected grant-then-deny on the wire, got grant at %d deny at %d", grantIndex, denyIndex)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorForcedMinorAC8WholeSession(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)

	if err := client.SetConsentDecision(ConsentDecisionDeniedForcedMinor); err != nil {
		t.Fatalf("SetConsentDecision: %v", err)
	}
	if got := client.Consent(); got != ConsentDeniedForcedMinor {
		t.Fatalf("expected the forced-minor state, got %v", got)
	}
	// Gameplay-shaped usage: everything analytics refuses with the SAME
	// error a plain denial produces.
	if err := client.Track(context.Background(), Event{Name: "level_up"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected Track refused consent_denied, got %v", err)
	}
	if err := client.Enqueue(Event{Name: "session_start"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected Enqueue refused consent_denied, got %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// AC-8: EXACTLY one analytics-plane request across the whole session —
	// the forced-minor receipt — with the pinned body shape.
	order := state.snapshotOrder()
	if len(order) != 1 || order[0] != "consent" {
		t.Fatalf("expected exactly the one consent POST on the wire, got %v", order)
	}
	body := state.consentAt(0)
	if consentBoolCategory(t, body) {
		t.Fatalf("expected categories.analytics false, got %v", body)
	}
	if reason, ok := body["reason"].(string); !ok || reason != "denied_forced_minor" {
		t.Fatalf("expected reason denied_forced_minor, got %v", body)
	}
	for _, field := range []string{"actor_identifier", "idempotency_key", "decided_at"} {
		if value, ok := body[field].(string); !ok || value == "" {
			t.Fatalf("expected receipt field %q present, got %v", field, body)
		}
	}
	if _, present := body["anonymous_id"]; present {
		t.Fatalf("anonymous_id must never reach the wire, got %v", body)
	}

	// A forced-minor RELAUNCH with an empty outbox transmits nothing, and
	// the state reloads as itself.
	relaunched := newFloorTestClient(t, server.URL, dir, nil)
	if got := relaunched.Consent(); got != ConsentDeniedForcedMinor {
		t.Fatalf("expected the forced-minor state reloaded, got %v", got)
	}
	if err := relaunched.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := relaunched.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.snapshotOrder(); len(got) != 1 {
		t.Fatalf("expected the relaunch to transmit nothing, got %v", got)
	}
}

func TestConsentFloorForcedMinorSupersededByGrant(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	if err := client.SetConsentDecision(ConsentDecisionDeniedForcedMinor); err != nil {
		t.Fatalf("SetConsentDecision: %v", err)
	}
	// A later explicit decision supersedes normally; the new receipt
	// carries NO reason.
	if err := client.SetConsentDecision(ConsentDecisionGranted); err != nil {
		t.Fatalf("SetConsentDecision: %v", err)
	}
	if got := client.Consent(); got != ConsentGranted {
		t.Fatalf("expected the grant to supersede, got %v", got)
	}
	if err := client.Enqueue(Event{ID: "evt-band-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue after the superseding grant: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitFor(t, 3*time.Second, "both receipts delivered", func() bool {
		return state.consentCount() >= 2
	})
	first, second := state.consentAt(0), state.consentAt(1)
	if consentBoolCategory(t, first) || first["reason"] != "denied_forced_minor" {
		t.Fatalf("expected the forced-minor receipt first, got %v", first)
	}
	if !consentBoolCategory(t, second) {
		t.Fatalf("expected the superseding grant receipt second, got %v", second)
	}
	if _, present := second["reason"]; present {
		t.Fatalf("a superseding grant carries no reason, got %v", second)
	}
}

func TestSetConsentDecisionRejectsUnknownValues(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	client := newFloorTestClient(t, server.URL, t.TempDir(), nil)
	defer client.Close(context.Background())

	if err := client.SetConsentDecision(ConsentDecision("denied ")); !errors.Is(err, ErrInvalidConsentDecision) {
		t.Fatalf("expected ErrInvalidConsentDecision, got %v", err)
	}
	if err := client.SetConsentDecision(ConsentDecision("maybe")); !errors.Is(err, ErrInvalidConsentDecision) {
		t.Fatalf("expected ErrInvalidConsentDecision, got %v", err)
	}
	// NOTHING was applied: the state is still undecided and nothing went to
	// the wire.
	if got := client.Consent(); got != ConsentUnknown {
		t.Fatalf("expected an invalid decision to apply nothing, got %v", got)
	}
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected nothing transmitted for invalid decisions, got %d", got)
	}
}

func TestSetConsentDecisionForcedMinorWithoutFloor(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		cfg.ConsentFloor = nil
	})
	if err := client.SetConsentDecision(ConsentDecisionDeniedForcedMinor); err != nil {
		t.Fatalf("SetConsentDecision: %v", err)
	}
	if got := client.Consent(); got != ConsentDeniedForcedMinor {
		t.Fatalf("expected the forced-minor state, got %v", got)
	}
	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected the full denial semantics, got %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The legacy fire-and-forget receipt carries the reason.
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected the fire-and-forget receipt, got %d", got)
	}
	body := state.consentAt(0)
	if consentBoolCategory(t, body) || body["reason"] != "denied_forced_minor" {
		t.Fatalf("expected a reasoned denial receipt, got %v", body)
	}
}

// testConsentReceipt builds a well-formed stored receipt for outbox unit
// tests.
func testConsentReceipt(key string, analytics bool) consentReceipt {
	receipt := consentReceipt{
		IdempotencyKey:  key,
		WorkspaceID:     "workspace-test",
		AppID:           "app-test",
		EnvironmentID:   "develop",
		ActorIdentifier: "anon-spool-1",
		DecidedAt:       "2026-07-19T00:00:00Z",
	}
	receipt.Categories.Analytics = &analytics
	return receipt
}

func TestConsentOutboxCapEvictsOldestOnSave(t *testing.T) {
	outbox := newConsentOutbox(t.TempDir())
	for i := 0; i < maxConsentOutboxEntries+8; i++ {
		if outbox.append(testConsentReceipt(fmt.Sprintf("key-%02d", i), false)) {
			t.Fatalf("append %d: unexpected persist failure", i)
		}
	}
	outbox.mu.Lock()
	kept := len(outbox.receipts)
	oldest := outbox.receipts[0].IdempotencyKey
	newest := outbox.receipts[kept-1].IdempotencyKey
	outbox.mu.Unlock()
	if kept != maxConsentOutboxEntries {
		t.Fatalf("expected the cap applied, got %d receipts", kept)
	}
	if oldest != "key-08" || newest != fmt.Sprintf("key-%02d", maxConsentOutboxEntries+7) {
		t.Fatalf("expected oldest-first eviction (newest decisions operative), got %s..%s", oldest, newest)
	}
	if got := outbox.takeEvicted(); got != 8 {
		t.Fatalf("expected 8 evictions counted, got %d", got)
	}
}

func TestConsentOutboxFailedWriteNeverEvicts(t *testing.T) {
	dir := t.TempDir()
	outbox := newConsentOutbox(dir)
	writeErr := errors.New("disk full")
	failing := true
	outbox.renameFn = func(oldpath, newpath string) error {
		if failing {
			return writeErr
		}
		return os.Rename(oldpath, newpath)
	}
	if !outbox.append(testConsentReceipt("key-durable-1", false)) {
		t.Fatalf("expected the append's durable write to fail")
	}
	// The failed write evicted NOTHING and partially succeeded at nothing:
	// the mirror keeps the receipt, the write is owed, and no record landed.
	if head, ok := outbox.head(); !ok || head.IdempotencyKey != "key-durable-1" {
		t.Fatalf("expected the receipt retained in the mirror, got (%v, %v)", head, ok)
	}
	if !outbox.pending() {
		t.Fatalf("expected pending work (retained receipt + owed write)")
	}
	if _, err := os.Stat(filepath.Join(dir, consentOutboxFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected no partial record on disk, got %v", err)
	}
	// The heal lands the owed write.
	failing = false
	if attempted, failed := outbox.retryPersist(); !attempted || failed {
		t.Fatalf("expected the owed write retried and landed, got (%v, %v)", attempted, failed)
	}
	reloaded := newConsentOutbox(dir)
	reloaded.load()
	if head, ok := reloaded.head(); !ok || head.IdempotencyKey != "key-durable-1" {
		t.Fatalf("expected the healed record readable, got (%v, %v)", head, ok)
	}
}

func TestConsentOutboxSanitizerDropsMalformedAndOversized(t *testing.T) {
	dir := t.TempDir()
	boundary := strings.Repeat("a", maxConsentIdentifierBytes)
	oversized := strings.Repeat("b", maxConsentIdentifierBytes+1)
	record := fmt.Sprintf(`{"version":1,"receipts":[
		{"idempotency_key":"key-ok","workspace_id":"w","app_id":"a","environment_id":"e","actor_identifier":%q,"categories":{"analytics":true},"decided_at":"2026-07-19T00:00:00Z","stray_field":"dropped"},
		{"idempotency_key":"","workspace_id":"w","app_id":"a","environment_id":"e","actor_identifier":"x","categories":{"analytics":true},"decided_at":"2026-07-19T00:00:00Z"},
		{"idempotency_key":"key-oversized","workspace_id":"w","app_id":"a","environment_id":"e","actor_identifier":%q,"categories":{"analytics":false},"decided_at":"2026-07-19T00:00:00Z"},
		{"idempotency_key":"key-truncated","workspace_id":"w"}
	]}`, boundary, oversized)
	if err := os.WriteFile(filepath.Join(dir, consentOutboxFileName), []byte(record), 0o600); err != nil {
		t.Fatalf("write outbox record: %v", err)
	}
	outbox := newConsentOutbox(dir)
	outbox.load()
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	if len(outbox.receipts) != 1 {
		t.Fatalf("expected only the valid boundary entry to survive, got %d", len(outbox.receipts))
	}
	if outbox.receipts[0].IdempotencyKey != "key-ok" || outbox.receipts[0].ActorIdentifier != boundary {
		t.Fatalf("expected the 512-byte boundary identifier kept verbatim, got %+v", outbox.receipts[0])
	}
}

func TestConsentOutboxGarbledRecordLoadsEmpty(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, consentOutboxFileName), []byte(`{"version":1,"receipts":[{`), 0o600); err != nil {
		t.Fatalf("write garbled record: %v", err)
	}
	client := newFloorTestClient(t, server.URL, dir, nil)
	// A wholly garbled record loads as empty — never a crash, never a
	// transmission of garbage — and the client functions.
	client.SetConsent(true)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected only the fresh decision's receipt, got %d", got)
	}
}

func TestConsentFloorIdentifierClampOnReceiptPath(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// An oversized UserID is REJECTED (never truncated) and the receipt
	// actor falls back to the valid AnonymousID.
	oversized := strings.Repeat("u", maxConsentIdentifierBytes+1)
	client := newFloorTestClient(t, server.URL, t.TempDir(), func(cfg *Config) {
		cfg.UserID = oversized
	})
	client.SetConsent(true)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected the receipt delivered, got %d", got)
	}
	if actor := state.consentAt(0)["actor_identifier"]; actor != "anon-spool-1" {
		t.Fatalf("expected the actor to fall back to the valid anonymous id, got %v", actor)
	}

	// With NO valid identifier the decision applies locally only: no
	// receipt is minted, nothing reaches the wire, teardown completes.
	dark := newFloorTestClient(t, server.URL, t.TempDir(), func(cfg *Config) {
		cfg.UserID = oversized
		cfg.AnonymousID = strings.Repeat("v", maxConsentIdentifierBytes+1)
	})
	dark.SetConsent(false)
	if got := dark.Consent(); got != ConsentDenied {
		t.Fatalf("expected the decision applied locally, got %v", got)
	}
	if err := dark.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected no receipt for an invalid-identity decision, got %d", got)
	}
}

func TestConsentFloorCloseConsentPendingWithoutDurableBackend(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")

	// No SpoolDir: the outbox has no durable backend, so an undeliverable
	// receipt must DECLINE completion — exiting would silently lose the
	// decision's receipt.
	client := newFloorTestClient(t, server.URL, "", nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the failed decision-time dispatch", func() bool {
		return state.consentCount() >= 1
	})
	if err := client.Close(context.Background()); !errors.Is(err, ErrConsentPending) {
		t.Fatalf("expected ErrConsentPending with no durable backend, got %v", err)
	}

	// Close stays RETRYABLE: once the endpoint heals, a repeated Close
	// delivers the receipt and completes.
	state.setConsentOutcome(http.StatusOK, "")
	clearConsentDeferral(client)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("expected the retried Close to deliver and complete, got %v", err)
	}
	last := state.consentAt(state.consentCount() - 1)
	if !consentBoolCategory(t, last) {
		t.Fatalf("expected the delivered grant receipt, got %v", last)
	}
}

func TestConsentFloorEmptyPipelineNeverGated(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the failed decision-time dispatch", func() bool {
		return state.consentCount() >= 1
	})
	// The grant is parked and retained — but with NOTHING to publish the
	// gate never fires: flushes succeed and teardown completes over the
	// durable outbox (the receipt re-sends at the next launch).
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("an empty pipeline must never be gated, got %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("expected teardown completed over the durable outbox, got %v", err)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected no batches, got %d", got)
	}
}

func TestConsentFloorUnauthorizedIsTerminalAndChainsNext(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()
	// The bearer is static for the client's lifetime — no re-mint seam — so
	// a 401 is TERMINAL on the receipt path: the head drops (diagnosed) and
	// the next retained receipt still delivers.
	state.setConsentOutcome(http.StatusUnauthorized, "")

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the terminal drop", func() bool {
		return state.consentCount() >= 1
	})
	state.setConsentOutcome(http.StatusOK, "")
	client.SetConsent(false)
	waitFor(t, 3*time.Second, "the next receipt chained and delivered", func() bool {
		count := state.consentCount()
		return count >= 2 && !consentBoolCategory(t, state.consentAt(count-1))
	})
	stats := client.Snapshot()
	if stats.ConsentFailed == 0 || stats.LastConsentError == "" {
		t.Fatalf("expected the terminal failure surfaced, got %+v", stats)
	}
	if stats.ConsentRecorded == 0 {
		t.Fatalf("expected the chained receipt acknowledged, got %+v", stats)
	}
	// The dropped grant released the gate vacuously: the pipeline flows.
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorCloseAfterGatedFlushIsRetryablePending(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")

	// Memory-only floor + a parked grant + queued events: Close's final
	// flush comes back GATED (ErrConsentReceiptPending), and the consent
	// drain must still run — Close lands on the RETRYABLE durability
	// verdict, never freezing the transient gate error while the receipt
	// quietly evaporates with the process.
	client := newFloorTestClient(t, server.URL, "", nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the failed decision-time dispatch", func() bool {
		return state.consentCount() >= 1
	})
	if err := client.Enqueue(Event{ID: "evt-gatedclose-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	err := client.Close(context.Background())
	if !errors.Is(err, ErrConsentPending) {
		t.Fatalf("expected the retryable ErrConsentPending verdict, got %v", err)
	}
	if errors.Is(err, ErrConsentReceiptPending) {
		t.Fatalf("the transient gate error must not be Close's frozen verdict, got %v", err)
	}

	state.setConsentOutcome(http.StatusOK, "")
	clearConsentDeferral(client)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("expected the retried Close to deliver and complete, got %v", err)
	}
	last := state.consentAt(state.consentCount() - 1)
	if !consentBoolCategory(t, last) {
		t.Fatalf("expected the grant receipt delivered on the retried Close, got %v", last)
	}
}

func TestConsentFloorRefusedTightenStartsFailClosed(t *testing.T) {
	dir := t.TempDir()
	// A planted/stale granted decision and outbox receipt in a directory
	// whose privacy cannot be established: NEITHER may be trusted — the
	// floor must start undecided with an empty outbox (fail-closed), the
	// same gate initSpool applies before trusting spool.json.
	writeConsentRecordFile(t, dir, "granted")
	planted := newConsentOutbox(dir)
	if planted.append(testConsentReceipt("key-planted-1", true)) {
		t.Fatalf("seeding the outbox record failed")
	}
	// Loosen the dir AFTER seeding (the seeding writes tightened it), so
	// the client's init actually reaches the refused tighten.
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	cfg := Config{
		WorkspaceID:   "workspace-test",
		EnvironmentID: "develop",
		AnonymousID:   "anon-spool-1",
		SpoolDir:      dir,
		ConsentFloor:  &ConsentFloorConfig{},
	}
	client := &Client{cfg: cfg, clock: realClock{}}
	client.initConsentFloor(func(string, os.FileMode) error {
		return errors.New("chmod refused")
	})
	if got := client.Consent(); got != ConsentUnknown {
		t.Fatalf("a persisted decision from an untightenable dir must not become the live state, got %v", got)
	}
	if client.consentOutbox.pending() {
		t.Fatalf("a receipt from an untightenable dir must not be loaded")
	}
	if got := client.Snapshot().LastError; got != "spool_dir_private_failed" {
		t.Fatalf("expected spool_dir_private_failed surfaced, got %q", got)
	}
	// The untrusted files are left in place for a later run with fixed
	// permissions.
	if _, err := os.Stat(filepath.Join(dir, consentOutboxFileName)); err != nil {
		t.Fatalf("expected the outbox record left in place, got %v", err)
	}
	if _, err := os.Stat(consentRecordPath(dir)); err != nil {
		t.Fatalf("expected the consent record left in place, got %v", err)
	}
}

func TestConsentFloorPostCloseDecisionAppliedLocallyOnly(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A decision recorded AFTER Close applies locally — and ONLY locally:
	// no receipt is minted, retained, or persisted, so nothing transmits
	// now or at the next launch.
	client.SetConsent(true)
	if got := client.Consent(); got != ConsentGranted {
		t.Fatalf("expected the post-Close decision applied locally, got %v", got)
	}
	if client.consentOutbox.pending() {
		t.Fatalf("a post-Close decision must not retain a receipt")
	}
	if _, err := os.Stat(filepath.Join(dir, consentOutboxFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a post-Close decision must not persist a durable receipt, got %v", err)
	}
	if got := state.consentCount(); got != 0 {
		t.Fatalf("a post-Close decision must not transmit, got %d posts", got)
	}

	// The next launch transmits nothing either.
	relaunched := newFloorTestClient(t, server.URL, dir, nil)
	if err := relaunched.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := relaunched.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected no receipt transmitted at the next launch, got %d posts", got)
	}
}

func TestConsentFloorDispatchBoundedByCallerDeadline(t *testing.T) {
	hang := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/consent" {
			<-hang
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"recorded":true,"replayed":false}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprint(w, `{"accepted":0,"rejected":0,"duplicates":0}`)
	}))
	releaseHang := sync.OnceFunc(func() { close(hang) })
	defer server.Close()
	defer releaseHang()

	client := newFloorTestClient(t, server.URL, t.TempDir(), func(cfg *Config) {
		cfg.HTTPTimeout = 5 * time.Second
	})
	// Seed a retained grant WITHOUT waking the worker, and a granted live
	// state, so the caller-driven Track is the pass that meets the hung
	// endpoint.
	client.consent.Store(consentStateGranted)
	if client.consentOutbox.append(testConsentReceipt("key-hang-1", true)) {
		t.Fatalf("seeding the outbox failed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := client.Track(ctx, Event{Name: "e1"})
	elapsed := time.Since(start)
	// The caller's deadline bounded the consent POST — not HTTPTimeout
	// (5s). The receipt WAS handed to the transport, so the gate released
	// for this cycle (release-on-dispatch) and Track proceeded to the event
	// leg, which failed on the same expired caller context — the caller's
	// own error, exactly like any deadline-bounded Track.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the caller's deadline surfaced, got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("the caller deadline did not bound the consent dispatch: %v", elapsed)
	}
	// The abort is NO outcome: nothing counted, no deferral armed, and the
	// receipt stays retained at the head for the next dispatch point.
	client.consentOutbox.mu.Lock()
	deferUntil := client.consentOutbox.deferUntil
	client.consentOutbox.mu.Unlock()
	if !deferUntil.IsZero() {
		t.Fatalf("a caller abort must not arm the consent deferral, got %v", deferUntil)
	}
	if stats := client.Snapshot(); stats.ConsentFailed != 0 {
		t.Fatalf("a caller abort is no outcome, got %+v", stats)
	}
	if head, ok := client.consentOutbox.head(); !ok || head.IdempotencyKey != "key-hang-1" {
		t.Fatalf("expected the aborted receipt retained at the head, got (%v, %v)", head, ok)
	}

	// Unblock the endpoint: the retained receipt delivers and Close
	// completes.
	releaseHang()
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentOutboxSanitizerRejectsAbsentCategory(t *testing.T) {
	dir := t.TempDir()
	record := `{"version":1,"receipts":[
		{"idempotency_key":"key-no-categories","workspace_id":"w","app_id":"a","environment_id":"e","actor_identifier":"x","decided_at":"2026-07-19T00:00:00Z"},
		{"idempotency_key":"key-empty-categories","workspace_id":"w","app_id":"a","environment_id":"e","actor_identifier":"x","categories":{},"decided_at":"2026-07-19T00:00:00Z"},
		{"idempotency_key":"key-explicit-denial","workspace_id":"w","app_id":"a","environment_id":"e","actor_identifier":"x","categories":{"analytics":false},"decided_at":"2026-07-19T00:00:00Z"}
	]}`
	if err := os.WriteFile(filepath.Join(dir, consentOutboxFileName), []byte(record), 0o600); err != nil {
		t.Fatalf("write outbox record: %v", err)
	}
	outbox := newConsentOutbox(dir)
	outbox.load()
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	// An entry with the analytics category ABSENT is malformed, never an
	// implicit denial: resending it as {"analytics":false} could overwrite
	// a previously granted actor server-side. Only the explicit denial
	// survives.
	if len(outbox.receipts) != 1 || outbox.receipts[0].IdempotencyKey != "key-explicit-denial" {
		t.Fatalf("expected only the explicit-category entry to survive, got %+v", outbox.receipts)
	}
	if outbox.receipts[0].analyticsGranted() {
		t.Fatalf("expected the surviving entry's explicit denial preserved")
	}
}

func TestGateRefusalLeavesEventPacingUntouched(t *testing.T) {
	client := &Client{}
	var deferUntil time.Time
	attempt := 0
	// The gate held the batch leg — it was never attempted, so nothing was
	// learned about the endpoint: no backoff advance, no deferral.
	client.applyRetryPacing(ErrConsentReceiptPending, &deferUntil, &attempt)
	client.applyRetryPacing(ErrConsentReceiptPending, &deferUntil, &attempt)
	if attempt != 0 || !deferUntil.IsZero() {
		t.Fatalf("consent gating must not arm event retry pacing, got attempt=%d deferUntil=%v", attempt, deferUntil)
	}
}

func TestConsentOutboxOverCapLoadKeepsNewest(t *testing.T) {
	dir := t.TempDir()
	entries := make([]string, 0, maxConsentOutboxEntries+8)
	for i := 0; i < maxConsentOutboxEntries+8; i++ {
		entries = append(entries, fmt.Sprintf(
			`{"idempotency_key":"key-%02d","workspace_id":"w","app_id":"a","environment_id":"e","actor_identifier":"x","categories":{"analytics":false},"decided_at":"2026-07-19T00:00:00Z"}`, i))
	}
	record := fmt.Sprintf(`{"version":1,"receipts":[%s]}`, strings.Join(entries, ","))
	if err := os.WriteFile(filepath.Join(dir, consentOutboxFileName), []byte(record), 0o600); err != nil {
		t.Fatalf("write outbox record: %v", err)
	}
	outbox := newConsentOutbox(dir)
	outbox.load()
	outbox.mu.Lock()
	kept := len(outbox.receipts)
	oldest := outbox.receipts[0].IdempotencyKey
	newest := outbox.receipts[kept-1].IdempotencyKey
	outbox.mu.Unlock()
	// The load-time cap trims OLDEST first, exactly like the save-time cap:
	// an over-cap legacy record keeps its NEWEST receipts — the operative
	// decisions — never the stalest history.
	if kept != maxConsentOutboxEntries || oldest != "key-08" || newest != fmt.Sprintf("key-%02d", maxConsentOutboxEntries+7) {
		t.Fatalf("expected the newest %d receipts kept (oldest evicted), got %d spanning %s..%s",
			maxConsentOutboxEntries, kept, oldest, newest)
	}
	if got := outbox.takeEvicted(); got != 8 {
		t.Fatalf("expected 8 load-time evictions counted, got %d", got)
	}
}

func TestConsentFloorEmptyBody2xxAcknowledges(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()
	// 204 No Content: the status IS the acknowledgement; the body is
	// optional. Treating the decode EOF as retryable would retain the
	// accepted receipt forever — gating events and holding Close.
	state.setConsentOutcome(http.StatusNoContent, "")

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the empty-body acknowledgement settled", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	if client.consentOutbox.pending() {
		t.Fatalf("expected the acknowledged receipt pruned")
	}
	// The gate released: events flow.
	if err := client.Enqueue(Event{ID: "evt-204-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := state.batchCount(); got != 1 {
		t.Fatalf("expected the batch delivered after the empty-body ack, got %d", got)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
