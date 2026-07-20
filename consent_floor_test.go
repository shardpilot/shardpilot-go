package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	consentRawBody    string
	batchStatus       int
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
			batchStatus := state.batchStatus
			state.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			if batchStatus != 0 && batchStatus != http.StatusAccepted {
				w.WriteHeader(batchStatus)
				fmt.Fprint(w, `{"error":{"code":"test","message":"test"}}`)
				return
			}
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
			rawBody := state.consentRawBody
			state.mu.Unlock()
			if rawBody != "" {
				// A 2xx with a NON-JSON body (an intermediary's plain-text
				// answer): the status is still the acknowledgement.
				w.Header().Set("Content-Type", "text/plain")
				fmt.Fprint(w, rawBody)
				return
			}
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

func (s *floorTestServer) setBatchOutcome(status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchStatus = status
}

func (s *floorTestServer) setConsentRawBody(body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consentRawBody = body
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

func TestConsentFloorRejectsOutOfContractIdentity(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// Go's EVENT path stamps configured identifiers verbatim (deliberately
	// unclamped), so a receipt minted for a substitute actor would
	// authorize a DIFFERENT actor than events carry. The floor therefore
	// REJECTS a decision whole when a configured identifier is out of
	// contract — reject, never truncate, never silently mint for a
	// fallback actor.
	oversized := strings.Repeat("u", maxConsentIdentifierBytes+1)
	client := newFloorTestClient(t, server.URL, t.TempDir(), func(cfg *Config) {
		cfg.UserID = oversized
	})
	if err := client.SetConsentDecision(ConsentDecisionGranted); !errors.Is(err, ErrInvalidConsentIdentity) {
		t.Fatalf("expected ErrInvalidConsentIdentity, got %v", err)
	}
	// NOTHING was applied — not the state, not a receipt, not a wire post —
	// and the void SetConsent surface rejects identically.
	client.SetConsent(true)
	if got := client.Consent(); got != ConsentUnknown {
		t.Fatalf("expected the rejected decision to apply nothing, got %v", got)
	}
	if client.consentOutbox.pending() {
		t.Fatalf("expected no receipt minted for a rejected decision")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected nothing transmitted for rejected decisions, got %d", got)
	}

	// The 512-byte boundary is IN contract: accepted verbatim as the
	// receipt actor — exactly what events carry.
	boundary := strings.Repeat("b", maxConsentIdentifierBytes)
	boundaryClient := newFloorTestClient(t, server.URL, t.TempDir(), func(cfg *Config) {
		cfg.UserID = boundary
	})
	if err := boundaryClient.SetConsentDecision(ConsentDecisionGranted); err != nil {
		t.Fatalf("SetConsentDecision at the boundary: %v", err)
	}
	if err := boundaryClient.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected the boundary receipt delivered, got %d", got)
	}
	if actor := state.consentAt(0)["actor_identifier"]; actor != boundary {
		t.Fatalf("expected the boundary identifier as the receipt actor, verbatim")
	}

	// With NO configured identifier at all the decision applies locally
	// only (no receipt) — the documented no-actor path, not a rejection.
	dark := newFloorTestClient(t, server.URL, t.TempDir(), func(cfg *Config) {
		cfg.AnonymousID = ""
	})
	if err := dark.SetConsentDecision(ConsentDecisionDenied); err != nil {
		t.Fatalf("SetConsentDecision with no identity: %v", err)
	}
	if got := dark.Consent(); got != ConsentDenied {
		t.Fatalf("expected the decision applied locally, got %v", got)
	}
	if err := dark.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected no receipt for a no-identity decision, got %d", got)
	}

	// Floor OFF: no identity gate — the legacy fire-and-forget posture is
	// unchanged (the clamp is floor-scoped; go's general event/identity
	// path deliberately keeps today's behavior).
	legacy := newFloorTestClient(t, server.URL, t.TempDir(), func(cfg *Config) {
		cfg.ConsentFloor = nil
		cfg.UserID = oversized
	})
	if err := legacy.SetConsentDecision(ConsentDecisionGranted); err != nil {
		t.Fatalf("expected no identity gate with the floor off, got %v", err)
	}
	if err := legacy.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.consentCount(); got != 2 {
		t.Fatalf("expected the legacy post despite the oversized identity, got %d", got)
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
	// The gated event had no spool to survive in: its discard is part of
	// the verdict, and Stats counts it.
	if !errors.Is(err, ErrEventsDiscarded) {
		t.Fatalf("expected the discarded gated event reported, got %v", err)
	}
	if got := client.Snapshot().Dropped; got == 0 {
		t.Fatalf("expected the discarded event counted dropped, got %+v", client.Snapshot())
	}

	state.setConsentOutcome(http.StatusOK, "")
	clearConsentDeferral(client)
	retried := client.Close(context.Background())
	// The retried Close delivers the receipt — the PENDING state clears —
	// but the event loss is permanent history and keeps being reported:
	// never a clean teardown over silently discarded events.
	if errors.Is(retried, ErrConsentPending) {
		t.Fatalf("expected the receipt delivered on the retried Close, got %v", retried)
	}
	if !errors.Is(retried, ErrEventsDiscarded) {
		t.Fatalf("expected the discard still reported on the retried Close, got %v", retried)
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
		AppID:         "app-test",
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
	// (5s) — and because the abort observed NO HTTP outcome, the receipt
	// counts as UNHANDED: the gate stays armed and the event leg refuses
	// rather than risk being the server's first-seen request ahead of the
	// grant.
	if !errors.Is(err, ErrConsentReceiptPending) {
		t.Fatalf("expected the gate armed after the no-response abort, got %v", err)
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

func TestConsentFloorCloseRunsDrainDespiteEventError(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the first grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})

	// Manufacture PENDING consent work that does NOT arm the gate: the
	// second grant delivers and prunes, but the prune's rewrite fails —
	// dirty with an EMPTY mirror, so the outbox is pending (the on-disk
	// record is stale) while no retained grant holds the event legs. The
	// flag is atomic: the worker's dispatch pass reads it concurrently.
	writeErr := errors.New("disk full")
	var failing atomic.Bool
	failing.Store(true)
	// Assigned under the outbox mutex: the worker's dispatch pass reads the
	// seam under the same lock while it settles the first decision.
	client.consentOutbox.mu.Lock()
	client.consentOutbox.renameFn = func(oldpath, newpath string) error {
		if failing.Load() {
			return writeErr
		}
		return os.Rename(oldpath, newpath)
	}
	client.consentOutbox.mu.Unlock()
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the second grant acknowledged with its prune owed", func() bool {
		return client.Snapshot().ConsentRecorded == 2 && client.consentOutbox.pending()
	})

	// A REAL, non-gate event-plane error at Close: the batch endpoint
	// answers a terminal 400 for the queued event.
	state.setBatchOutcome(http.StatusBadRequest)
	if err := client.Enqueue(Event{ID: "evt-terminal-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	err := client.Close(context.Background())
	// The consent drain ran DESPITE the event error, and its retryable
	// verdict rides the fold — the cached terminal event error must not
	// mask the pending state, or repeated Close calls could never retry
	// the drain and the owed rewrite would be lost with the process.
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected the terminal event outcome surfaced, got %v", err)
	}
	if !errors.Is(err, ErrConsentPending) {
		t.Fatalf("expected the retryable consent verdict folded in, got %v", err)
	}

	// The retry path stays alive: heal the disk and a repeated Close lands
	// the owed rewrite and completes.
	failing.Store(false)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("expected the retried Close to settle the owed write and complete, got %v", err)
	}
	_ = state
}

// eofOnConsentTransport fails consent POSTs at the SEND (a connection
// closed before any status arrives — a bare io.EOF in the chain), while
// delegating everything else to the real transport.
type eofOnConsentTransport struct {
	mu      sync.Mutex
	failing bool
}

func (t *eofOnConsentTransport) setFailing(failing bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failing = failing
}

func (t *eofOnConsentTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.mu.Lock()
	failing := t.failing
	t.mu.Unlock()
	if failing && strings.HasSuffix(request.URL.Path, "/v1/consent") {
		return nil, io.EOF
	}
	return http.DefaultTransport.RoundTrip(request)
}

func TestConsentFloorSendPathEOFStaysRetryable(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	transport := &eofOnConsentTransport{failing: true}
	client := newFloorTestClient(t, server.URL, t.TempDir(), func(cfg *Config) {
		cfg.HTTPClient = &http.Client{Transport: transport}
	})
	client.SetConsent(true)
	// A send-path EOF observed NO status: it must classify retryable —
	// never as an empty-body acknowledgement — so the receipt stays
	// retained and nothing counts recorded.
	waitFor(t, 3*time.Second, "the send failure counted", func() bool {
		return client.Snapshot().ConsentFailed >= 1
	})
	if got := client.Snapshot().ConsentRecorded; got != 0 {
		t.Fatalf("a send-path EOF must never acknowledge, got %d recorded", got)
	}
	if head, ok := client.consentOutbox.head(); !ok || !head.analyticsGranted() {
		t.Fatalf("expected the grant receipt retained at the head, got (%v, %v)", head, ok)
	}

	// The endpoint heals: the retained receipt delivers.
	transport.setFailing(false)
	clearConsentDeferral(client)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected the retained receipt delivered after the heal, got %d", got)
	}
}

func TestConsentFloorFlushDispatchesReceiptsUnderDenial(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	client.SetConsent(false)
	if got := client.Consent(); got != ConsentDenied {
		t.Fatalf("expected the denied state, got %v", got)
	}

	// Heal, drain any pending worker nudge so the EXPLICIT Flush is the
	// dispatch point under test, and release the parked window.
	state.setConsentOutcome(http.StatusOK, "")
	select {
	case <-client.consentWake:
	default:
	}
	clearConsentDeferral(client)

	// Receipt delivery is permitted — required — while consent is denied:
	// the flush dispatches the retained trail even though the event legs
	// refuse, synchronously in this call.
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	granted, denied := 0, 0
	for i := 0; i < state.consentCount(); i++ {
		if consentBoolCategory(t, state.consentAt(i)) {
			granted++
		} else {
			denied++
		}
	}
	if granted < 1 || denied < 1 {
		t.Fatalf("expected the full trail delivered through the denied flush, got %d grants %d denials", granted, denied)
	}
	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected the event legs still refused under the denial, got %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorReloadedReceiptDispatchesWithoutCallerOps(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the parked decision-time dispatch", func() bool {
		return state.consentCount() >= 1
	})
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close over the durable outbox: %v", err)
	}

	// Relaunch with the endpoint healed and NO caller operations at all:
	// construction is a dispatch point — the reloaded receipt must re-send
	// via the construction wake, not idle until a flush tick (an hour
	// away) or a caller op.
	state.setConsentOutcome(http.StatusOK, "")
	relaunched := newFloorTestClient(t, server.URL, dir, nil)
	waitFor(t, 3*time.Second, "the reloaded receipt re-sent by construction alone", func() bool {
		return state.consentCount() >= 2
	})
	if err := relaunched.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorPostCloseDecisionLeavesNoDurableState(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A post-Close decision under the floor is memory-only IN FULL: no
	// receipt AND no persisted decision — the next launch must not
	// resurrect a decision whose receipt was never sent.
	if err := client.SetConsentDecision(ConsentDecisionDenied); err != nil {
		t.Fatalf("SetConsentDecision: %v", err)
	}
	if got := client.Consent(); got != ConsentDenied {
		t.Fatalf("expected the post-Close decision applied in memory, got %v", got)
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("expected the pre-Close granted record untouched, got (%v, %v)", recorded, ok)
	}

	// The next launch runs on the pre-Close state and transmits nothing
	// new.
	posts := state.consentCount()
	relaunched := newFloorTestClient(t, server.URL, dir, nil)
	if got := relaunched.Consent(); got != ConsentGranted {
		t.Fatalf("expected the pre-Close persisted state as the live state, got %v", got)
	}
	if err := relaunched.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.consentCount(); got != posts {
		t.Fatalf("expected no phantom transmissions at the next launch, got %d (was %d)", got, posts)
	}
}

func TestPostCloseDecisionStillPersistsRecordWithoutFloor(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		cfg.ConsentFloor = nil
	})
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Floor OFF keeps today's behavior byte-for-byte: a post-Close
	// decision still writes the local record (it only ever gates the next
	// launch's spool there, never the live state).
	client.SetConsent(false)
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the post-Close record written with the floor off, got (%v, %v)", recorded, ok)
	}
	_ = state
}

func TestConsentFloorTrailTailOverridesStaleRecord(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})

	// The DENIAL: its receipt appends durably, but the decision-record
	// write fails transiently — consent.json keeps saying GRANTED while
	// the durable trail's tail says denied.
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")
	client.spool.renameFn = func(oldpath, newpath string) error {
		return errors.New("disk full")
	}
	client.SetConsent(false)
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("test setup: expected the record write to have failed (stale grant), got (%v, %v)", recorded, ok)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close over the durable outbox: %v", err)
	}

	// Reload: trusting the stale record would reopen the pipeline for an
	// actor whose LAST decision was a denial (and a pending DENY receipt
	// arms no gate). The trail tail is the newer truth: the floor starts
	// DENIED, and the stale record heals to denied on disk.
	relaunched := newFloorTestClient(t, server.URL, dir, nil)
	if got := relaunched.Consent(); got != ConsentDenied {
		t.Fatalf("expected the trail tail to override the stale granted record, got %v", got)
	}
	if err := relaunched.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected the pipeline closed under the trail-derived denial, got %v", err)
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the stale record healed from the trail, got (%v, %v)", recorded, ok)
	}
	// The retained deny receipt still delivers once the endpoint heals.
	state.setConsentOutcome(http.StatusOK, "")
	clearConsentDeferral(relaunched)
	if err := relaunched.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	last := state.consentAt(state.consentCount() - 1)
	if consentBoolCategory(t, last) {
		t.Fatalf("expected the retained deny receipt delivered, got %v", last)
	}
	if err := relaunched.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorGrantNotObservableBeforeReceiptArmed(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)

	// Stall an EARLIER decision's slow half (the deny's record write), so
	// the following grant's fast half applies — live state granted — while
	// its receipt append queues behind the stalled ticket turn.
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
	denyDone := make(chan struct{})
	go func() {
		defer close(denyDone)
		client.SetConsent(false)
	}()
	<-stalled
	grantDone := make(chan struct{})
	go func() {
		defer close(grantDone)
		client.SetConsent(true)
	}()
	waitFor(t, 3*time.Second, "the grant observable in the live state", func() bool {
		return client.Consent() == ConsentGranted
	})

	// The grant is OBSERVABLE but its receipt does not exist yet: the
	// arming window must hold the event legs — a batch shipped now would
	// precede the grant receipt on the wire.
	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentReceiptPending) {
		t.Fatalf("expected the event leg held while the grant receipt is mid-append, got %v", err)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected no batch before the grant receipt exists, got %d", got)
	}

	close(release)
	<-denyDone
	<-grantDone
	// Both receipts now exist; the trail drains and events follow.
	if err := client.Enqueue(Event{ID: "evt-armed-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	order := state.snapshotOrder()
	if state.batchCount() != 1 || order[len(order)-1] != "batch" {
		t.Fatalf("expected the receipts on the wire before the batch, got %v", order)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorNonJSON2xxAcknowledges(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()
	// A 2xx whose body is not JSON at all (an intermediary's plain-text
	// answer): the status IS the acknowledgement — the consent route never
	// requires a decodable body.
	state.setConsentRawBody("OK\n")

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the non-JSON 2xx acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	if client.consentOutbox.pending() {
		t.Fatalf("expected the acknowledged receipt pruned")
	}
	if err := client.Enqueue(Event{ID: "evt-raw-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := state.batchCount(); got != 1 {
		t.Fatalf("expected the batch delivered after the raw-body ack, got %d", got)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorReloadRefusesOutOfContractIdentity(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	oversized := strings.Repeat("u", maxConsentIdentifierBytes+1)
	cfgTuple := Config{
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		UserID:        oversized,
		AnonymousID:   "anon-spool-1",
	}
	// A persisted grant for the out-of-contract tuple (legacy, seeded, or
	// written by a floor-off build, which has no identity gate).
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	payload := []byte(fmt.Sprintf(`{"consent_analytics":"granted","actor_digest":%q}`, consentActorDigest(cfgTuple)))
	if err := os.WriteFile(consentRecordPath(dir), payload, 0o600); err != nil {
		t.Fatalf("write consent record: %v", err)
	}

	client := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		cfg.UserID = oversized
	})
	// The identity contract holds at reload exactly as at decision time:
	// the persisted grant is NOT loaded — the floor starts undecided,
	// distinctly diagnosed — so out-of-contract identifiers never publish
	// past the decision-time gate via a persisted state.
	if got := client.Consent(); got != ConsentUnknown {
		t.Fatalf("expected the out-of-contract reload refused, got %v", got)
	}
	if got := client.Snapshot().LastError; got != "consent_identity_invalid" {
		t.Fatalf("expected the distinct identity diagnostic, got %q", got)
	}
	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("expected the pipeline closed under the refused reload, got %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.snapshotOrder(); len(got) != 0 {
		t.Fatalf("expected a fully dark session under the refused reload, got %v", got)
	}
}

func TestConsentFloorNoResponseFailureKeepsGateArmed(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	transport := &eofOnConsentTransport{failing: true}
	client := newFloorTestClient(t, server.URL, t.TempDir(), func(cfg *Config) {
		cfg.HTTPClient = &http.Client{Transport: transport}
	})
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the no-response failure counted", func() bool {
		return client.Snapshot().ConsentFailed >= 1
	})

	// NO HTTP outcome was observed for the grant: the server may never
	// have seen it, so the receipt counts as UNHANDED and the gate keeps
	// holding the event legs — a batch shipped now could be the server's
	// first-seen request, overtaking the grant.
	if err := client.Enqueue(Event{ID: "evt-noresp-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); !errors.Is(err, ErrConsentReceiptPending) {
		t.Fatalf("expected the gate armed after a no-response dispatch failure, got %v", err)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected no batch while the grant is unobserved, got %d", got)
	}

	// The endpoint heals: the receipt delivers with an OBSERVED outcome and
	// the held events follow.
	transport.setFailing(false)
	clearConsentDeferral(client)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after the heal: %v", err)
	}
	order := state.snapshotOrder()
	if state.batchCount() != 1 || order[len(order)-1] != "batch" {
		t.Fatalf("expected the receipt observed before the batch, got %v", order)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorStaleGrantNeverResendsSpooledEvents(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// Session 1: a granted client spools an event on a retryable batch
	// failure, so spool.json holds a pre-denial chunk.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	state.setBatchOutcome(http.StatusServiceUnavailable)
	if err := client.Enqueue(Event{ID: "evt-predenial-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the batch failure that spools the event")
	}
	waitFor(t, 3*time.Second, "the event spooled durably", func() bool {
		return client.Snapshot().Spooled == 1
	})
	_ = client.Close(context.Background()) // the 503 event error is expected
	if _, err := os.Stat(filepath.Join(dir, spoolFileName)); err != nil {
		t.Fatalf("test setup: expected the spooled chunk on disk, got %v", err)
	}

	// The crash window: a DENY receipt became durable while the decision
	// record write never landed — consent.json still says granted, and the
	// pre-denial chunk is still on disk.
	planted := newConsentOutbox(dir)
	deny := testConsentReceipt("key-crash-deny-1", false)
	if planted.append(deny) {
		t.Fatalf("seeding the deny receipt failed")
	}
	batchesBefore := state.batchCount()

	// Relaunch: the floor resolves FIRST (trail tail overrides the stale
	// grant, heals the record), and the spool loads only under a grant the
	// resolved state confirms — the pre-denial chunk is purged, never
	// resent.
	state.setBatchOutcome(0)
	relaunched := newFloorTestClient(t, server.URL, dir, nil)
	if got := relaunched.Consent(); got != ConsentDenied {
		t.Fatalf("expected the trail-tail denial resolved before the spool, got %v", got)
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the stale record healed to denied, got (%v, %v)", recorded, ok)
	}
	if _, err := os.Stat(filepath.Join(dir, spoolFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected the pre-denial spool purged at init, got %v", err)
	}
	// The retained deny receipt still delivers; no batch ever follows.
	waitFor(t, 3*time.Second, "the deny receipt delivered", func() bool {
		return state.consentCount() >= 1 && !consentBoolCategory(t, state.consentAt(state.consentCount()-1))
	})
	if err := relaunched.Track(context.Background(), Event{Name: "e2"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected the pipeline closed under the resolved denial, got %v", err)
	}
	if err := relaunched.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := state.batchCount(); got != batchesBefore {
		t.Fatalf("expected NO pre-denial event on the wire after the relaunch, got %d batches (was %d)", got, batchesBefore)
	}
	if err := relaunched.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorUnconfirmedGrantPurgesSpoolAtInit(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// Session 1 seeds a durably spooled chunk under a real grant.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	state.setBatchOutcome(http.StatusServiceUnavailable)
	if err := client.Enqueue(Event{ID: "evt-unconfirmed-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the batch failure that spools the event")
	}
	waitFor(t, 3*time.Second, "the event spooled durably", func() bool {
		return client.Snapshot().Spooled == 1
	})
	_ = client.Close(context.Background())

	// A GRANTED record matching the RELAUNCH tuple's digest, whose identity
	// the floor refuses (out of contract): the resolved live state stays
	// undecided, so the persisted grant is NOT confirmed and the spool must
	// purge instead of seeding resend work for a session that must stay
	// dark.
	oversized := strings.Repeat("u", maxConsentIdentifierBytes+1)
	invalidTuple := Config{
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		UserID:        oversized,
		AnonymousID:   "anon-spool-1",
	}
	payload := []byte(fmt.Sprintf(`{"consent_analytics":"granted","actor_digest":%q}`, consentActorDigest(invalidTuple)))
	if err := os.WriteFile(consentRecordPath(dir), payload, 0o600); err != nil {
		t.Fatalf("write consent record: %v", err)
	}
	batchesBefore := state.batchCount()
	state.setBatchOutcome(0)

	relaunched := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		cfg.UserID = oversized
	})
	if got := relaunched.Consent(); got != ConsentUnknown {
		t.Fatalf("expected the refused reload undecided, got %v", got)
	}
	if _, err := os.Stat(filepath.Join(dir, spoolFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected the unconfirmed grant's spool purged at init, got %v", err)
	}
	if err := relaunched.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := state.batchCount(); got != batchesBefore {
		t.Fatalf("expected the dark session to resend nothing, got %d batches (was %d)", got, batchesBefore)
	}
	if err := relaunched.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorGrantRecordWithheldWhenReceiptNotDurable(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// The receipt cannot be written durably; the consent endpoint is down
	// too, so the receipt cannot even deliver in-session.
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.consentOutbox.mu.Lock()
	client.consentOutbox.renameFn = func(oldpath, newpath string) error {
		return errors.New("disk full")
	}
	client.consentOutbox.mu.Unlock()

	// GRANT ordering is receipt-first: with the receipt trail NOT safely
	// down, the granted record write is WITHHELD — the reachable crash
	// state is "no grant record, no durable receipt", never "granted
	// record, empty outbox" (which would flow events receipt-less at the
	// next launch).
	client.SetConsent(true)
	if got := client.Consent(); got != ConsentGranted {
		t.Fatalf("expected the live grant applied in memory, got %v", got)
	}
	if got := client.Snapshot().ConsentOutboxPersistFailed; got == 0 {
		t.Fatalf("expected the failed receipt write counted")
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); ok {
		t.Fatalf("expected the granted record WITHHELD while the receipt write is owed, got %v", recorded)
	}
	// Neither delivered nor durable: Close declines with the retryable
	// pending verdict.
	if err := client.Close(context.Background()); !errors.Is(err, ErrConsentPending) {
		t.Fatalf("expected ErrConsentPending for the undurable undelivered receipt, got %v", err)
	}

	// The next launch restores the PRIOR state — fail-closed undecided —
	// not a receipt-less grant.
	relaunched := newFloorTestClient(t, server.URL, dir, nil)
	if got := relaunched.Consent(); got != ConsentUnknown {
		t.Fatalf("expected the relaunch fail-closed undecided, got %v", got)
	}
	if err := relaunched.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("expected the pipeline dark after the withheld grant, got %v", err)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected no batch anywhere, got %d", got)
	}
	if err := relaunched.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorGrantReceiptTailRestoresGrantAndHealsRecord(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// The OTHER half of the grant's crash window: the receipt landed
	// durably, the record write did not. The trail tail restores the grant,
	// heals the record, and the retained receipt still precedes any batch.
	dir := t.TempDir()
	planted := newConsentOutbox(dir)
	if planted.append(testConsentReceipt("key-crash-grant-1", true)) {
		t.Fatalf("seeding the grant receipt failed")
	}

	client := newFloorTestClient(t, server.URL, dir, nil)
	if got := client.Consent(); got != ConsentGranted {
		t.Fatalf("expected the grant restored from the trail tail, got %v", got)
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("expected the missing record healed to granted, got (%v, %v)", recorded, ok)
	}
	if err := client.Enqueue(Event{ID: "evt-tailgrant-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	order := state.snapshotOrder()
	if len(order) < 2 || order[0] != "consent" || order[len(order)-1] != "batch" {
		t.Fatalf("expected the reloaded receipt on the wire before the batch, got %v", order)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorCrossScopeTailNeverFlipsState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*consentReceipt)
	}{
		{"workspace", func(r *consentReceipt) { r.WorkspaceID = "workspace-other" }},
		{"app", func(r *consentReceipt) { r.AppID = "app-other" }},
		{"environment", func(r *consentReceipt) { r.EnvironmentID = "production" }},
		{"actor", func(r *consentReceipt) { r.ActorIdentifier = "anon-other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, server := newFloorTestServer(t)
			defer server.Close()

			// This client's own scoped record says DENIED; a reused SpoolDir
			// retains a newer GRANT receipt from another scope. The foreign
			// tail must neither flip the live state nor heal consent.json
			// for a digest its decision never covered.
			dir := t.TempDir()
			writeConsentRecordFile(t, dir, "denied")
			foreign := testConsentReceipt("key-foreign-grant-1", true)
			tc.mutate(&foreign)
			planted := newConsentOutbox(dir)
			if planted.append(foreign) {
				t.Fatalf("seeding the foreign receipt failed")
			}

			client := newFloorTestClient(t, server.URL, dir, nil)
			if got := client.Consent(); got != ConsentDenied {
				t.Fatalf("expected the correctly-scoped denial to rule, got %v", got)
			}
			if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
				t.Fatalf("expected the scoped record untouched by the foreign tail, got (%v, %v)", recorded, ok)
			}
			if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentDenied) {
				t.Fatalf("expected the pipeline closed under the scoped denial, got %v", err)
			}
			// The foreign receipt still delivers verbatim for its own
			// historic scope — retention is per-directory, state is
			// per-scope.
			waitFor(t, 3*time.Second, "the foreign receipt delivered", func() bool {
				return state.consentCount() >= 1
			})
			if err := client.Close(context.Background()); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

func TestConsentFloorRefusesPerEventActorOverride(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	client := newFloorTestClient(t, server.URL, "", nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})

	// The floor's decision covers the CONFIGURED actor (anon-spool-1): an
	// override resolving to a DIFFERENT effective actor has no local
	// decision and no receipt — refused, distinctly, at both intakes.
	if err := client.Track(context.Background(), Event{Name: "e1", UserID: "intruder-1"}); !errors.Is(err, ErrConsentActorMismatch) {
		t.Fatalf("expected the user-id override actor refused, got %v", err)
	}
	if err := client.Enqueue(Event{Name: "e1", AnonymousID: "anon-other"}); !errors.Is(err, ErrConsentActorMismatch) {
		t.Fatalf("expected the anonymous-id override actor refused, got %v", err)
	}
	if got := client.Snapshot().Dropped; got != 2 {
		t.Fatalf("expected both override refusals counted dropped, got %d", got)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected no override event on the wire, got %d batches", got)
	}
	// An override resolving to the SAME effective actor passes through.
	if err := client.Track(context.Background(), Event{ID: "evt-same-actor-1", Name: "e1", AnonymousID: "anon-spool-1"}); err != nil {
		t.Fatalf("expected the same-actor override accepted, got %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// With a configured UserID the effective actor stays the user id even
	// when the event overrides only the anonymous id — allowed.
	userClient := newFloorTestClient(t, server.URL, "", func(cfg *Config) {
		cfg.UserID = "user-1"
	})
	userClient.SetConsent(true)
	waitFor(t, 3*time.Second, "the user grant acknowledged", func() bool {
		return userClient.Snapshot().ConsentRecorded == 1
	})
	if err := userClient.Track(context.Background(), Event{ID: "evt-secondary-1", Name: "e1", AnonymousID: "anon-secondary"}); err != nil {
		t.Fatalf("expected the secondary-identifier override accepted (effective actor unchanged), got %v", err)
	}
	if err := userClient.Track(context.Background(), Event{Name: "e1", UserID: "user-2"}); !errors.Is(err, ErrConsentActorMismatch) {
		t.Fatalf("expected the user-id override to another actor refused, got %v", err)
	}
	if err := userClient.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Floor OFF: per-event actor overrides pass through unchanged (the
	// server-side posture the default documents).
	offClient := newFloorTestClient(t, server.URL, "", func(cfg *Config) {
		cfg.ConsentFloor = nil
	})
	if err := offClient.Track(context.Background(), Event{ID: "evt-off-override-1", Name: "e1", UserID: "someone-else"}); err != nil {
		t.Fatalf("expected the floor-off override accepted, got %v", err)
	}
	if err := offClient.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorGatedCloseReportsUnspooledRemnant(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A granted spooled client with its grant receipt PARKED retryably (the
	// gate re-arms every cycle) and one queued event.
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the parked grant attempted", func() bool {
		return client.Snapshot().ConsentFailed >= 1
	})
	if err := client.Enqueue(Event{ID: "evt-remnant-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The remnant spool write fails and stays failed through the final
	// settle retry: the held event is neither delivered (the gated flush
	// held it) nor durable (the persist failed) when the process exits.
	client.spool.mu.Lock()
	client.spool.renameFn = func(oldpath, newpath string) error {
		return errors.New("disk full")
	}
	client.spool.mu.Unlock()

	// Close: the final flush is GATED (parked grant), the consent drain
	// succeeds durably (the receipt IS safely on disk) — but the lost
	// remnant must keep the verdict non-nil: ErrEventsDiscarded, folded
	// permanently.
	err := client.Close(context.Background())
	if !errors.Is(err, ErrEventsDiscarded) {
		t.Fatalf("expected the unspooled gated remnant reported on Close, got %v", err)
	}
	if errors.Is(err, ErrConsentPending) {
		t.Fatalf("expected the durable receipt NOT pending, got %v", err)
	}
	if got := client.Snapshot().Dropped; got == 0 {
		t.Fatalf("expected the lost remnant counted dropped")
	}
	// Permanent history: a repeated Close keeps reporting the loss.
	if err := client.Close(context.Background()); !errors.Is(err, ErrEventsDiscarded) {
		t.Fatalf("expected the discard permanent on repeated Close, got %v", err)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected the held event never published, got %d batches", got)
	}
}

func TestConsentFloorForeignTailDoesNotHideInScopeProof(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A stale GRANTED record, this client's durable DENY receipt, and a
	// FOREIGN receipt retained after it (another scope sharing the dir).
	// The absolute tail is foreign — the override must still find the
	// latest IN-SCOPE receipt and resolve the denial.
	dir := t.TempDir()
	writeConsentRecordFile(t, dir, "granted")
	planted := newConsentOutbox(dir)
	if planted.append(testConsentReceipt("key-inscope-deny-1", false)) {
		t.Fatalf("seeding the in-scope deny receipt failed")
	}
	foreign := testConsentReceipt("key-foreign-tail-1", true)
	foreign.WorkspaceID = "workspace-other"
	if planted.append(foreign) {
		t.Fatalf("seeding the foreign tail receipt failed")
	}

	client := newFloorTestClient(t, server.URL, dir, nil)
	if got := client.Consent(); got != ConsentDenied {
		t.Fatalf("expected the latest IN-SCOPE receipt to override the stale grant, got %v", got)
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the stale record healed from the in-scope proof, got (%v, %v)", recorded, ok)
	}
	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected the pipeline closed under the in-scope denial, got %v", err)
	}
	// Both receipts still deliver, in retention order (deny first), each
	// verbatim for its own scope; no batch ever.
	waitFor(t, 3*time.Second, "both receipts delivered", func() bool {
		return state.consentCount() >= 2
	})
	if consentBoolCategory(t, state.consentAt(0)) {
		t.Fatalf("expected the in-scope deny delivered first, got %v", state.consentAt(0))
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected a dark denied session, got %d batches", got)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorDenyProofHeldUntilRecordDurable(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})

	// The denial's record write fails (the granted record stays on disk),
	// while the deny receipt appends durably. Delivering that receipt now
	// would prune the trail's ONLY durable evidence of the denial: it must
	// stay HELD until the denied record lands.
	client.spool.mu.Lock()
	client.spool.renameFn = func(oldpath, newpath string) error {
		return errors.New("disk full")
	}
	client.spool.mu.Unlock()
	client.SetConsent(false)
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("test setup: expected the denied record write to have failed, got (%v, %v)", recorded, ok)
	}
	_ = client.Flush(context.Background()) // a dispatch point; the proof must hold
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected the deny proof HELD while its record write is owed, got %d consent posts", got)
	}

	// The disk heals: the owed record retry lands the denied record at the
	// next dispatch point, which releases the proof for delivery.
	client.spool.mu.Lock()
	client.spool.renameFn = os.Rename
	client.spool.mu.Unlock()
	if err := client.Flush(context.Background()); err != nil && !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("Flush after the heal: %v", err)
	}
	waitFor(t, 3*time.Second, "the deny receipt delivered after the record healed", func() bool {
		return state.consentCount() == 2
	})
	if consentBoolCategory(t, state.consentAt(1)) {
		t.Fatalf("expected the deny receipt delivered, got %v", state.consentAt(1))
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the denied record durable before the proof delivered, got (%v, %v)", recorded, ok)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorHeldDenyProofRestoresDenialAcrossRestart(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// The crash half of the held proof: the record write NEVER heals in
	// session one — Close completes over the durable retained proof — and
	// the relaunch must restore the denial from it, not the stale grant.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	client.spool.mu.Lock()
	client.spool.renameFn = func(oldpath, newpath string) error {
		return errors.New("disk full")
	}
	client.spool.mu.Unlock()
	client.SetConsent(false)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close over the durable held proof: %v", err)
	}
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected the held proof undelivered at exit, got %d consent posts", got)
	}

	relaunched := newFloorTestClient(t, server.URL, dir, nil)
	if got := relaunched.Consent(); got != ConsentDenied {
		t.Fatalf("expected the held proof to restore the denial, got %v", got)
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the stale record healed at reload, got (%v, %v)", recorded, ok)
	}
	// With the record healed the proof is releasable and delivers.
	waitFor(t, 3*time.Second, "the deny receipt delivered after the relaunch heal", func() bool {
		return state.consentCount() == 2
	})
	if err := relaunched.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorWithheldGrantRecordCompletesOnRetry(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// The grant's receipt write fails, so its record is WITHHELD
	// (receipt-first); the endpoint is down too, so nothing delivers yet.
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.consentOutbox.mu.Lock()
	client.consentOutbox.renameFn = func(oldpath, newpath string) error {
		return errors.New("disk full")
	}
	client.consentOutbox.mu.Unlock()
	client.SetConsent(true)
	if _, ok := loadConsentRecord(dir, spoolTestActorDigest()); ok {
		t.Fatalf("test setup: expected the granted record withheld")
	}

	// The disk heals: the SAME dispatch pass that recovers the outbox write
	// completes the receipt-first pair — the granted record lands BEFORE
	// the receipt can dispatch, be acknowledged, and prune away.
	client.consentOutbox.mu.Lock()
	client.consentOutbox.renameFn = os.Rename
	client.consentOutbox.mu.Unlock()
	state.setConsentOutcome(http.StatusOK, "")
	clearConsentDeferral(client)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("expected the withheld record completed on the outbox retry, got (%v, %v)", recorded, ok)
	}
	waitFor(t, 3*time.Second, "the grant receipt delivered", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The relaunch restores the grant from the RECORD — the receipt is long
	// pruned, and without the completed pair this would start unknown.
	relaunched := newFloorTestClient(t, server.URL, dir, nil)
	if got := relaunched.Consent(); got != ConsentGranted {
		t.Fatalf("expected the completed record to restore the grant, got %v", got)
	}
	if err := relaunched.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorDirtyDuplicateRemnantCountsDiscarded(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})

	// The batch fails retryably and its spool append lands in the MIRROR
	// only (the save fails): the event is already tracked as unpersisted
	// when Close's remnant re-appends it as a DUPLICATE.
	state.setBatchOutcome(http.StatusServiceUnavailable)
	client.spool.mu.Lock()
	client.spool.renameFn = func(oldpath, newpath string) error {
		return errors.New("disk full")
	}
	client.spool.mu.Unlock()
	if err := client.Enqueue(Event{ID: "evt-dupremnant-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retryable batch failure")
	}

	// Close: the remnant append de-duplicates (nothing newly added), the
	// final settle retry still fails — the event is neither delivered nor
	// durable, and the loss must fold into the verdict.
	err := client.Close(context.Background())
	if !errors.Is(err, ErrEventsDiscarded) {
		t.Fatalf("expected the dirty duplicate remnant reported on Close, got %v", err)
	}
	if got := client.Snapshot().Dropped; got == 0 {
		t.Fatalf("expected the lost remnant counted dropped")
	}
	if got := state.batchCount(); got == 0 {
		t.Fatalf("test shape: expected the failing batch attempts on the wire")
	}
}

func TestConsentFloorRecordScopedToApp(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A granted record persisted by ANOTHER APP in the same
	// workspace/environment/actor (its receipt long delivered and pruned).
	// This app's floor must not adopt it as live state.
	dir := t.TempDir()
	otherApp := Config{
		WorkspaceID:   "workspace-test",
		AppID:         "app-other",
		EnvironmentID: "develop",
		AnonymousID:   "anon-spool-1",
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	payload := []byte(fmt.Sprintf(`{"consent_analytics":"granted","actor_digest":%q}`, consentActorDigest(otherApp)))
	if err := os.WriteFile(consentRecordPath(dir), payload, 0o600); err != nil {
		t.Fatalf("write consent record: %v", err)
	}

	client := newFloorTestClient(t, server.URL, dir, nil)
	if got := client.Consent(); got != ConsentUnknown {
		t.Fatalf("expected the app-foreign record refused (undecided), got %v", got)
	}
	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("expected the pipeline dark under the refused record, got %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.snapshotOrder(); len(got) != 0 {
		t.Fatalf("expected a fully dark session, got %v", got)
	}
	// The foreign record is left in place for its own app.
	if _, err := os.Stat(consentRecordPath(dir)); err != nil {
		t.Fatalf("expected the app-foreign record left in place, got %v", err)
	}
}

func TestConsentFloorForeignParkedGrantDoesNotGateEvents(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	client := newFloorTestClient(t, server.URL, t.TempDir(), nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})

	// A FOREIGN grant receipt (another scope sharing the dir) parks behind
	// a retryable failure. It is unrelated to this client's actor: it must
	// keep re-sending for its own scope, but never hold this pipeline.
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")
	foreign := testConsentReceipt("key-foreign-parked-1", true)
	foreign.WorkspaceID = "workspace-other"
	if client.consentOutbox.append(foreign) {
		t.Fatalf("seeding the foreign receipt failed")
	}
	// First event leg: the pass attempts the foreign receipt (503 — an
	// observed outcome) and parks the plane; the batch must still flow.
	if err := client.Track(context.Background(), Event{ID: "evt-foreign-gate-1", Name: "e1"}); err != nil {
		t.Fatalf("expected the batch to flow despite the parked foreign grant, got %v", err)
	}
	// Second event leg: the plane is parked, nothing is handed this cycle —
	// the retained foreign grant alone must not arm the gate.
	if err := client.Track(context.Background(), Event{ID: "evt-foreign-gate-2", Name: "e1"}); err != nil {
		t.Fatalf("expected the batch to flow while the foreign grant stays parked, got %v", err)
	}
	if got := state.batchCount(); got != 2 {
		t.Fatalf("expected both batches on the wire, got %d", got)
	}
	// The foreign receipt still delivers for its own scope once the
	// endpoint heals.
	state.setConsentOutcome(http.StatusOK, "")
	clearConsentDeferral(client)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
