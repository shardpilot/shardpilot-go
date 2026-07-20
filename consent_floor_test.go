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

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
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

// batchIDsSince flattens every event id the server has seen in batch posts
// from index `from` onward, in arrival order — so a test can scope the scan
// past setup-era attempts (the server records a post even when it answers
// it with an error status).
func (s *floorTestServer) batchIDsSince(from int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for i := from; i < len(s.batches); i++ {
		ids = append(ids, s.batches[i]...)
	}
	return ids
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

	// Heal and drain. A parked 503 attempt can still be IN FLIGHT here and
	// re-arm the deferral after a one-shot clear, so the poll clears and
	// flushes each round until the trail drains — the assertion is about
	// ORDER, not about which cycle delivers.
	state.setConsentOutcome(http.StatusOK, "")
	waitFor(t, 5*time.Second, "both receipts delivered", func() bool {
		clearConsentDeferral(client)
		_ = client.Flush(context.Background())
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
		// The retention metadata a real mint stamps for this config: the
		// in-scope rule matches BOTH actor components, so a planted receipt
		// must carry the anonymous id it was "minted" under.
		AnonymousID: "anon-spool-1",
		DecidedAt:   "2026-07-19T00:00:00Z",
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
	reloaded.load(nil)
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
	outbox.load(nil)
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
	client.initConsentFloor(os.Rename, func(string, os.FileMode) error {
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
	outbox.load(nil)
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
	outbox.load(nil)
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
	// append itself must land durably first (a grant with its own append
	// owed is HELD from dispatch by design), so the second grant is parked
	// behind a 503 while its receipt persists, and only then does the disk
	// fail — the delivery's prune rewrite is the single failing write. The
	// flag is atomic: the worker's dispatch pass reads it concurrently.
	writeErr := errors.New("disk full")
	var failing atomic.Bool
	// Assigned under the outbox mutex: the worker's dispatch pass reads the
	// seam under the same lock.
	client.consentOutbox.mu.Lock()
	client.consentOutbox.renameFn = func(oldpath, newpath string) error {
		if failing.Load() {
			return writeErr
		}
		return os.Rename(oldpath, newpath)
	}
	client.consentOutbox.mu.Unlock()
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the second grant parked with its receipt durable", func() bool {
		return client.Snapshot().ConsentFailed >= 1
	})
	failing.Store(true)
	state.setConsentOutcome(http.StatusOK, "")
	clearConsentDeferral(client)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush delivering the parked grant: %v", err)
	}
	if client.Snapshot().ConsentRecorded != 2 || !client.consentOutbox.pending() {
		t.Fatalf("test shape: expected the second grant acknowledged with its prune rewrite owed")
	}

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

	// Hold the serial-dispatch claim BEFORE the decisions — a deterministic
	// in-flight pass. The worker's decision wakes lose the claim and skip,
	// so the trail stays parked with NO attempt ever started: unlike the
	// earlier 503-parked shape, no late-settling in-flight failure can
	// re-arm a deferral under the flush.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	if !client.consentOutbox.claimDispatch() {
		t.Fatalf("test shape: claiming the dispatch lock failed")
	}
	client.SetConsent(true)
	client.SetConsent(false)
	if got := client.Consent(); got != ConsentDenied {
		t.Fatalf("expected the denied state, got %v", got)
	}
	if got := state.consentCount(); got != 0 {
		t.Fatalf("test shape: the trail must be parked before the flush, got %d posts", got)
	}

	// Receipt delivery is permitted — required — while consent is denied:
	// the flush dispatches the retained trail even though the event legs
	// refuse. It JOINS behind the held claim (observable as a dispatch
	// waiter) and, once released, the trail is fully drained BY THE TIME
	// the flush returns — the join guarantee.
	flushErr := make(chan error, 1)
	go func() {
		flushErr <- client.Flush(context.Background())
	}()
	waitFor(t, 3*time.Second, "the flush parked as a dispatch waiter", func() bool {
		client.consentOutbox.mu.Lock()
		defer client.consentOutbox.mu.Unlock()
		return len(client.consentOutbox.dispatchWaiters) > 0
	})
	client.consentOutbox.releaseDispatch()
	if err := <-flushErr; err != nil {
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
	if granted != 1 || denied != 1 {
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
	// pre-denial chunk is still on disk. The denial was decided AFTER the
	// grant, so its receipt carries a stamp newer than the record's (the
	// strictly-newer override rule orders them).
	planted := newConsentOutbox(dir)
	deny := testConsentReceipt("key-crash-deny-1", false)
	deny.DecidedAt = time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
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
			// The foreign receipt is never dispatched with this client's
			// scoped bearer — a terminal 401/403 here would prune another
			// scope's consent receipt — so it stays retained on disk for a
			// correctly scoped client (retention is per-directory, delivery
			// is per-scope).
			if err := client.Flush(context.Background()); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if got := state.consentCount(); got != 0 {
				t.Fatalf("expected the foreign receipt never posted with this bearer, got %d posts", got)
			}
			if !client.consentOutbox.pending() {
				t.Fatalf("expected the foreign receipt still retained")
			}
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
	// Only the IN-SCOPE deny delivers — the foreign receipt is never
	// dispatched with this client's scoped bearer (it stays retained for a
	// correctly scoped client); no batch ever.
	waitFor(t, 3*time.Second, "the in-scope deny delivered", func() bool {
		return state.consentCount() >= 1
	})
	if consentBoolCategory(t, state.consentAt(0)) {
		t.Fatalf("expected the in-scope deny delivered, got %v", state.consentAt(0))
	}
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected ONLY the in-scope receipt on the wire, got %d posts", got)
	}
	if !client.consentOutbox.pending() {
		t.Fatalf("expected the foreign receipt still retained")
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

	// A FOREIGN grant receipt (another scope sharing the dir) sits retained.
	// It is unrelated to this client's actor: it is never dispatched with
	// this client's bearer and must never hold this pipeline.
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")
	foreign := testConsentReceipt("key-foreign-parked-1", true)
	foreign.WorkspaceID = "workspace-other"
	if client.consentOutbox.append(foreign) {
		t.Fatalf("seeding the foreign receipt failed")
	}
	// Event legs: the dispatch pass SKIPS the foreign receipt (nothing
	// in-scope to send, nothing handed), and the retained foreign grant
	// alone must not arm the gate — both batches flow.
	if err := client.Track(context.Background(), Event{ID: "evt-foreign-gate-1", Name: "e1"}); err != nil {
		t.Fatalf("expected the batch to flow despite the retained foreign grant, got %v", err)
	}
	if err := client.Track(context.Background(), Event{ID: "evt-foreign-gate-2", Name: "e1"}); err != nil {
		t.Fatalf("expected the batch to flow while the foreign grant stays retained, got %v", err)
	}
	if got := state.batchCount(); got != 2 {
		t.Fatalf("expected both batches on the wire, got %d", got)
	}
	// Only this client's own grant ever rode the consent route; the foreign
	// receipt stays retained on disk for a correctly scoped client.
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected only the own grant on the consent route, got %d posts", got)
	}
	if !client.consentOutbox.pending() {
		t.Fatalf("expected the foreign receipt still retained")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorStaleAckedReceiptCannotOverrideNewerDenial(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)

	// A grant whose receipt is acknowledged but whose PRUNE rewrite fails:
	// the acked receipt stays on disk in consent-outbox.json. (The append
	// lands durably while the grant is parked; only the prune's rewrite
	// hits the failing disk.)
	writeErr := errors.New("disk full")
	var failing atomic.Bool
	client.consentOutbox.mu.Lock()
	client.consentOutbox.renameFn = func(oldpath, newpath string) error {
		if failing.Load() {
			return writeErr
		}
		return os.Rename(oldpath, newpath)
	}
	client.consentOutbox.mu.Unlock()
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant parked with its receipt durable", func() bool {
		return client.Snapshot().ConsentFailed >= 1
	})
	failing.Store(true)
	state.setConsentOutcome(http.StatusOK, "")
	clearConsentDeferral(client)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush delivering the parked grant: %v", err)
	}

	// The user then DENIES: the denied record persists (a NEWER decision),
	// while the deny receipt's append hits the failing disk — after the
	// crash, the disk holds the STALE acked grant receipt and the newer
	// denied record.
	client.SetConsent(false)
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("test shape: expected the denied record persisted, got (%v, %v)", recorded, ok)
	}
	_ = client.Close(context.Background()) // the owed outbox rewrite keeps Close pending; the crash half is the point

	// Relaunch: the stale acked grant receipt on disk is OLDER than the
	// record's decision — it must NOT flip the state back to granted.
	relaunched := newFloorTestClient(t, server.URL, dir, nil)
	if got := relaunched.Consent(); got != ConsentDenied {
		t.Fatalf("expected the newer denied record to rule over the stale acked receipt, got %v", got)
	}
	if err := relaunched.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected the pipeline closed under the denial, got %v", err)
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the denied record untouched, got (%v, %v)", recorded, ok)
	}
	if err := relaunched.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorLegacyGrantRecordUnproven(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A floor-OFF-era SpoolDir: a LEGACY granted consent.json (no floor
	// provenance, no stamp — the fire-and-forget era, whose POST may have
	// failed; no receipt exists) plus spooled events.
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	legacy := []byte(fmt.Sprintf(`{"consent_analytics":"granted","actor_digest":%q}`, spoolTestActorDigest()))
	if err := os.WriteFile(consentRecordPath(dir), legacy, 0o600); err != nil {
		t.Fatalf("write legacy record: %v", err)
	}
	writeSpoolRecordFile(t, dir, 0, spoolTestEnvelope(t, "evt-legacy-1", time.Now()))

	// Enabling the floor over that directory must NOT promote the unproven
	// grant: the floor starts undecided (distinctly diagnosed), the spool
	// purges under the unconfirmed grant, and the session stays dark.
	client := newFloorTestClient(t, server.URL, dir, nil)
	if got := client.Consent(); got != ConsentUnknown {
		t.Fatalf("expected the unproven legacy grant refused, got %v", got)
	}
	if got := client.Snapshot().LastError; got != "consent_record_unproven" {
		t.Fatalf("expected the provenance diagnostic, got %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, spoolFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected the legacy spool purged under the unproven grant, got %v", err)
	}
	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("expected the pipeline dark, got %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.snapshotOrder(); len(got) != 0 {
		t.Fatalf("expected a fully dark session, got %v", got)
	}

	// A legacy DENIAL is honored regardless of provenance — honoring a
	// denial is the fail-closed direction.
	deniedDir := t.TempDir()
	if err := os.MkdirAll(deniedDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	legacyDenied := []byte(fmt.Sprintf(`{"consent_analytics":"denied","actor_digest":%q}`, spoolTestActorDigest()))
	if err := os.WriteFile(consentRecordPath(deniedDir), legacyDenied, 0o600); err != nil {
		t.Fatalf("write legacy denied record: %v", err)
	}
	deniedClient := newFloorTestClient(t, server.URL, deniedDir, nil)
	if got := deniedClient.Consent(); got != ConsentDenied {
		t.Fatalf("expected the legacy denial honored, got %v", got)
	}
	if err := deniedClient.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorFailedHealRegistersOwedDenial(t *testing.T) {
	// The reload derives a denial from the in-scope proof, but the HEALING
	// record write fails: the failed heal must register as an OWED denial
	// record before the worker could ever dispatch — otherwise the proof
	// would not be held, could deliver and prune, and a crash would restore
	// the stale grant.
	dir := t.TempDir()
	writeConsentRecordFile(t, dir, "granted") // stale, floor-marked, old stamp
	planted := newConsentOutbox(dir)
	deny := testConsentReceipt("key-heal-deny-1", false) // newer than the record's stamp
	if planted.append(deny) {
		t.Fatalf("seeding the deny receipt failed")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
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
	client.initConsentFloor(func(oldpath, newpath string) error {
		if strings.Contains(newpath, consentRecordFileName) {
			return errors.New("disk full")
		}
		return os.Rename(oldpath, newpath)
	}, os.Chmod)

	if got := client.Consent(); got != ConsentDenied {
		t.Fatalf("expected the trail-derived denial applied in memory, got %v", got)
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("test shape: expected the heal to have failed (stale grant on disk), got (%v, %v)", recorded, ok)
	}
	owed := client.consentRecordOwedSnapshot()
	if owed == nil || owed.decision != ConsentDecisionDenied || owed.decidedAt != deny.DecidedAt {
		t.Fatalf("expected the failed heal registered as an owed denial record, got %+v", owed)
	}
	if !client.consentDenyProofHeld(deny) {
		t.Fatalf("expected the denial's proof receipt held from dispatch while the heal is owed")
	}
}

func TestConsentFloorRestartReopensSpoolWriteGate(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// Session 1 persists the floor grant.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The relaunch trusts that grant as live state — and the spool write
	// gate must reopen with it: a retriable failure after restart spools
	// durably instead of dead-lettering until a fresh SetConsent(true).
	relaunched := newFloorTestClient(t, server.URL, dir, nil)
	if got := relaunched.Consent(); got != ConsentGranted {
		t.Fatalf("expected the persisted grant live after restart, got %v", got)
	}
	state.setBatchOutcome(http.StatusServiceUnavailable)
	if err := relaunched.Enqueue(Event{ID: "evt-postrestart-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := relaunched.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retryable batch failure")
	}
	waitFor(t, 3*time.Second, "the post-restart event spooled durably", func() bool {
		return relaunched.Snapshot().Spooled == 1
	})
	_ = relaunched.Close(context.Background()) // the 503 event error is expected
	if _, err := os.Stat(filepath.Join(dir, spoolFileName)); err != nil {
		t.Fatalf("expected the post-restart event durably on disk, got %v", err)
	}
}

func TestConsentFloorGrantHeldWhileOwnAppendOwed(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// The grant receipt's own append fails: the receipt is retained in the
	// MIRROR only. Dispatching it now and losing the process right after
	// the acknowledgement would leave neither a durable receipt nor a
	// granted record — the grant must not ride the wire until its own
	// write lands.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	writeErr := errors.New("disk full")
	var failing atomic.Bool
	failing.Store(true)
	client.consentOutbox.mu.Lock()
	client.consentOutbox.renameFn = func(oldpath, newpath string) error {
		if failing.Load() {
			return writeErr
		}
		return os.Rename(oldpath, newpath)
	}
	client.consentOutbox.mu.Unlock()
	client.SetConsent(true)
	_ = client.Flush(context.Background()) // a dispatch point; the grant must hold
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected the grant HELD while its own append is owed, got %d consent posts", got)
	}
	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentReceiptPending) {
		t.Fatalf("expected the event legs gated behind the held grant, got %v", err)
	}

	// The disk heals: the same pass that recovers the outbox write
	// completes the withheld record and releases the grant for dispatch.
	failing.Store(false)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after the heal: %v", err)
	}
	waitFor(t, 3*time.Second, "the grant delivered after its write landed", func() bool {
		return state.consentCount() == 1
	})
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("expected the withheld record completed before delivery, got (%v, %v)", recorded, ok)
	}
	if err := client.Track(context.Background(), Event{ID: "evt-held-grant-1", Name: "e1"}); err != nil {
		t.Fatalf("expected the pipeline open after the release, got %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorSecondaryOverrideSpoolsUnderFloor(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A configured UserID with an event overriding only AnonymousID: the
	// floor admits it (the effective actor is unchanged), so the spool must
	// retain it too — accepted-then-dead-lettered would contradict the
	// intake's override semantics.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		cfg.UserID = "user-1"
	})
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	state.setBatchOutcome(http.StatusServiceUnavailable)
	if err := client.Enqueue(Event{ID: "evt-secondary-spool-1", Name: "e1", AnonymousID: "anon-secondary"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retryable batch failure")
	}
	waitFor(t, 3*time.Second, "the admitted override event spooled durably", func() bool {
		return client.Snapshot().Spooled == 1
	})
	_ = client.Close(context.Background()) // the 503 event error is expected
}

func TestConsentFloorGrantReceiptHeldWhileRecordOwed(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// The grant's receipt appends durably, but the granted RECORD write
	// fails: the pair is incomplete, and the receipt must be held from
	// dispatch — an acknowledgement would prune the only durable half, and
	// a crash would leave NEITHER on disk.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	writeErr := errors.New("disk full")
	var failing atomic.Bool
	failing.Store(true)
	client.spool.mu.Lock()
	client.spool.renameFn = func(oldpath, newpath string) error {
		if failing.Load() {
			return writeErr
		}
		return os.Rename(oldpath, newpath)
	}
	client.spool.mu.Unlock()
	client.SetConsent(true)
	_ = client.Flush(context.Background()) // a dispatch point; the grant must hold
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected the grant receipt HELD while its record is owed, got %d consent posts", got)
	}
	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentReceiptPending) {
		t.Fatalf("expected the event legs gated behind the held pair, got %v", err)
	}

	// Close must not read as clean over the incomplete pair: the owed
	// granted record pends the verdict, retryably.
	if err := client.Close(context.Background()); !errors.Is(err, ErrConsentPending) {
		t.Fatalf("expected ErrConsentPending while the granted record is owed, got %v", err)
	}

	// The disk heals: the retried Close completes the record, releases the
	// receipt, delivers it, and finishes clean.
	failing.Store(false)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("expected the retried Close to complete the pair, got %v", err)
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("expected the granted record completed before delivery, got (%v, %v)", recorded, ok)
	}
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected the receipt delivered once the pair completed, got %d posts", got)
	}
}

func TestConsentDecisionStampsMonotonicSameTick(t *testing.T) {
	// A frozen clock mints the SAME instant twice: the stamps must still
	// order strictly, or the reload's strictly-newer override would miss
	// the newest decision after a crash.
	frozen := &stubClock{now: time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)}
	client := &Client{cfg: Config{}, clock: frozen}
	first := client.consentDecisionStamp()
	second := client.consentDecisionStamp()
	third := client.consentDecisionStamp()
	if first == second || second == third {
		t.Fatalf("expected distinct stamps from a frozen clock, got %q %q %q", first, second, third)
	}
	if !consentReceiptNewerThanRecord(second, first) || !consentReceiptNewerThanRecord(third, second) {
		t.Fatalf("expected strictly increasing stamps, got %q %q %q", first, second, third)
	}
	// A clock that moved BACKWARD still cannot regress the order.
	frozen.now = frozen.now.Add(-time.Hour)
	fourth := client.consentDecisionStamp()
	if !consentReceiptNewerThanRecord(fourth, third) {
		t.Fatalf("expected monotonicity across a backward clock step, got %q then %q", third, fourth)
	}
}

func TestConsentFloorForeignReceiptNeverDispatched(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A foreign receipt retained in a reused SpoolDir must never ride this
	// client's scoped bearer — a terminal 401/403 answer would prune
	// ANOTHER scope's consent receipt. It stays retained; only in-scope
	// receipts dispatch.
	dir := t.TempDir()
	foreign := testConsentReceipt("key-foreign-keep-1", true)
	foreign.WorkspaceID = "workspace-other"
	planted := newConsentOutbox(dir)
	if planted.append(foreign) {
		t.Fatalf("seeding the foreign receipt failed")
	}

	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the own grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected only the in-scope receipt on the wire, got %d posts", got)
	}
	if got := state.consentAt(0)["workspace_id"]; got != "workspace-test" {
		t.Fatalf("expected the posted receipt to carry this client's scope, got %v", got)
	}
	if !client.consentOutbox.pending() {
		t.Fatalf("expected the foreign receipt still retained")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close over the durably retained foreign receipt: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, consentOutboxFileName))
	if err != nil || !strings.Contains(string(data), "key-foreign-keep-1") {
		t.Fatalf("expected the foreign receipt durably retained for a correctly scoped client, got %q (%v)", data, err)
	}
}

func TestConsentFloorLateDiscardFoldsIntoCachedClose(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	if err := client.Enqueue(Event{ID: "evt-latediscard-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The worker STALLS in a spool write (the failed flush's crash-insurance
	// append, then the stop path's remnant) while Close's context expires:
	// Close caches its verdict BEFORE the stop path counts the discarded
	// remnant.
	state.setBatchOutcome(http.StatusServiceUnavailable)
	stalled := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	writeErr := errors.New("disk full")
	client.spool.mu.Lock()
	client.spool.renameFn = func(oldpath, newpath string) error {
		once.Do(func() { close(stalled) })
		<-release
		return writeErr
	}
	client.spool.mu.Unlock()

	expiring, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	err := client.Close(expiring)
	if err == nil || errors.Is(err, ErrEventsDiscarded) {
		t.Fatalf("test shape: expected the cached verdict WITHOUT the not-yet-counted discard, got %v", err)
	}

	// The worker finishes discarding AFTER the verdict was cached: the
	// remnant write keeps failing, so the events are neither delivered nor
	// durable at exit.
	<-stalled
	close(release)
	waitFor(t, 3*time.Second, "the late discard counted", func() bool {
		return client.closeDiscardedEvents.Load() > 0
	})

	// Every later Close must fold the late discard into the cached verdict.
	if err := client.Close(context.Background()); !errors.Is(err, ErrEventsDiscarded) {
		t.Fatalf("expected the late discard reported on the cached close, got %v", err)
	}
	_ = state
}

func TestConsentFloorReloadSeedsStampsPastPersistedState(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// Persisted state carries a stamp AHEAD of this process's clock (the
	// behind-clock restart): a proven granted record decided "in the
	// future". New decisions must still out-order it, or their receipts
	// could never override the stale record after a crash.
	dir := t.TempDir()
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := saveConsentRecord(dir, ConsentDecisionGranted, spoolTestActorDigest(), future, true, os.Rename, os.Chmod); err != nil {
		t.Fatalf("seed record: %v", err)
	}

	client := newFloorTestClient(t, server.URL, dir, nil)
	if got := client.Consent(); got != ConsentGranted {
		t.Fatalf("expected the proven grant loaded, got %v", got)
	}

	// The denial's record write fails — the deny receipt is the only
	// evidence, and it must carry a stamp STRICTLY newer than the future-
	// stamped record for the reload override to see it.
	writeErr := errors.New("disk full")
	client.spool.mu.Lock()
	client.spool.renameFn = func(oldpath, newpath string) error {
		return writeErr
	}
	client.spool.mu.Unlock()
	client.SetConsent(false)
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("test shape: expected the denied record write to have failed, got (%v, %v)", recorded, ok)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close over the durable held proof: %v", err)
	}

	// Without the seeded stamp floor the deny receipt would read OLDER
	// than the record and the stale grant would rule the relaunch.
	relaunched := newFloorTestClient(t, server.URL, dir, nil)
	if got := relaunched.Consent(); got != ConsentDenied {
		t.Fatalf("expected the seeded-stamp denial to out-order the future-stamped record, got %v", got)
	}
	if err := relaunched.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_ = state
}

func TestConsentFloorOwnerlessOwedDenialPendsClose(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A LOCAL-ONLY floor client (no configured identifiers): decisions mint
	// no receipts, so an owed denial has no durable proof — Close must not
	// read clean while the stale granted record is the only durable state.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		cfg.UserID = ""
		cfg.AnonymousID = ""
	})
	client.SetConsent(true) // granted record persists; no receipt (local-only)
	digest := consentActorDigest(client.cfg)
	if recorded, ok := loadConsentRecord(dir, digest); !ok || recorded != ConsentGranted {
		t.Fatalf("test shape: expected the local-only granted record persisted, got (%v, %v)", recorded, ok)
	}

	writeErr := errors.New("disk full")
	var failing atomic.Bool
	failing.Store(true)
	client.spool.mu.Lock()
	client.spool.renameFn = func(oldpath, newpath string) error {
		if failing.Load() {
			return writeErr
		}
		return os.Rename(oldpath, newpath)
	}
	client.spool.mu.Unlock()
	client.SetConsent(false)
	if recorded, ok := loadConsentRecord(dir, digest); !ok || recorded != ConsentGranted {
		t.Fatalf("test shape: expected the denied record write to have failed, got (%v, %v)", recorded, ok)
	}

	// No proof receipt exists: the owed denial must pend Close, retryably.
	if err := client.Close(context.Background()); !errors.Is(err, ErrConsentPending) {
		t.Fatalf("expected the ownerless owed denial to pend Close, got %v", err)
	}
	failing.Store(false)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("expected the retried Close to land the denied record, got %v", err)
	}
	if recorded, ok := loadConsentRecord(dir, digest); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the denied record durable after the retry, got (%v, %v)", recorded, ok)
	}
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected no consent posts on the local-only path, got %d", got)
	}
}

func TestConsentFloorProofReceiptPromotesUnprovenGrantRecord(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// An UNPROVEN granted record (floor-off era, stamped but unmarked) plus
	// a STRICTLY NEWER in-scope grant receipt (a floor grant whose record
	// write never landed before the crash): the same-state pair must heal
	// from the proof — discarding the grant despite its durable receipt
	// would force the user to re-decide.
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	legacy := []byte(fmt.Sprintf(`{"consent_analytics":"granted","actor_digest":%q,"decided_at":"2026-07-18T00:00:00Z"}`, spoolTestActorDigest()))
	if err := os.WriteFile(consentRecordPath(dir), legacy, 0o600); err != nil {
		t.Fatalf("write unproven record: %v", err)
	}
	planted := newConsentOutbox(dir)
	if planted.append(testConsentReceipt("key-proof-grant-1", true)) { // stamped 2026-07-19, newer
		t.Fatalf("seeding the proof receipt failed")
	}

	client := newFloorTestClient(t, server.URL, dir, nil)
	if got := client.Consent(); got != ConsentGranted {
		t.Fatalf("expected the proof receipt to promote the same-state grant, got %v", got)
	}
	info, ok := loadConsentRecordInfo(dir, spoolTestActorDigest())
	if !ok || !info.floor || info.decidedAt != "2026-07-19T00:00:00Z" || info.state != ConsentGranted {
		t.Fatalf("expected the record healed floor-marked with the receipt's stamp, got %+v %v", info, ok)
	}
	// The retained proof still dispatches ahead of any batch.
	if err := client.Enqueue(Event{ID: "evt-promoted-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	order := state.snapshotOrder()
	if len(order) < 2 || order[0] != "consent" || order[len(order)-1] != "batch" {
		t.Fatalf("expected the proof on the wire before the batch, got %v", order)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentOutboxMalformedStampDroppedAndNeverReloadTruth(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// The sanitizer drops a receipt with an unparsable decided_at.
	malformed := testConsentReceipt("key-malformed-1", true)
	malformed.DecidedAt = "not-a-time"
	if _, ok := sanitizeConsentReceipt(malformed); ok {
		t.Fatalf("expected the malformed stamp dropped by the sanitizer")
	}

	// Reload: the newest on-disk entry has the malformed stamp; the tail
	// pick must skip it (dropped at load) and land on the valid in-scope
	// deny — the corrupt entry can never become reload truth.
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	valid := testConsentReceipt("key-valid-deny-1", false)
	payload, err := json.Marshal(consentOutboxWire{Version: consentOutboxRecordVersion, Receipts: []consentReceipt{valid, malformed}})
	if err != nil {
		t.Fatalf("marshal outbox: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, consentOutboxFileName), payload, 0o600); err != nil {
		t.Fatalf("write outbox: %v", err)
	}

	client := newFloorTestClient(t, server.URL, dir, nil)
	if got := client.Consent(); got != ConsentDenied {
		t.Fatalf("expected the VALID deny as reload truth (malformed grant dropped), got %v", got)
	}
	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected the pipeline closed under the valid denial, got %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected a dark session, got %d batches", got)
	}
}

func TestConsentFloorOwedGrantHoldSurvivesNewerOwedDenialOverwrite(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A grant whose record write fails leaves its receipt durable with the
	// record OWED; a NEWER denial whose record write ALSO fails overwrites
	// the single owed slot. The retained grant's pair-incomplete hold must
	// survive that overwrite (it is tracked PER RECEIPT): the grant
	// delivering and pruning now — while the deny proof is held — would
	// make a grant the server's last word against a local denial.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	recordErr := errors.New("disk full")
	var failing atomic.Bool
	failing.Store(true)
	client.spool.renameFn = func(oldpath, newpath string) error {
		if failing.Load() && strings.Contains(newpath, consentRecordFileName) {
			return recordErr
		}
		return os.Rename(oldpath, newpath)
	}

	client.SetConsent(true)  // receipt durable; the granted record write fails (owed)
	client.SetConsent(false) // deny receipt durable; the denied record write fails — the slot OVERWRITES
	if owed := client.consentRecordOwedSnapshot(); owed == nil || owed.decision != ConsentDecisionDenied {
		t.Fatalf("test shape: expected the owed slot overwritten by the denial, got %+v", owed)
	}
	_ = client.Flush(context.Background()) // a dispatch point; NOTHING may deliver
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected the retained grant HELD despite the slot overwrite, got %d consent posts", got)
	}

	// Crash half: teardown completes over the durable proof (both receipts
	// safely on disk, the denial last), and the relaunch — the record never
	// landed — restores the denial from the trail, heals the record, and
	// re-sends the WHOLE trail in decision order, denial as the last word.
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close over the durable held trail: %v", err)
	}
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected the trail still held through Close, got %d consent posts", got)
	}
	relaunched := newFloorTestClient(t, server.URL, dir, nil)
	if got := relaunched.Consent(); got != ConsentDenied {
		t.Fatalf("expected the trail-derived denial live after relaunch, got %v", got)
	}
	waitFor(t, 3*time.Second, "the retained trail re-sent in order", func() bool {
		return state.consentCount() == 2
	})
	if !consentBoolCategory(t, state.consentAt(0)) || consentBoolCategory(t, state.consentAt(1)) {
		t.Fatalf("expected grant-then-denial in decision order, got %v then %v", state.consentAt(0), state.consentAt(1))
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the healed denied record after relaunch, got (%v, %v)", recorded, ok)
	}
	if err := relaunched.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentOutboxMergingSavePreservesSiblingReceipts(t *testing.T) {
	// Two floor clients share a SpoolDir (different scopes). Outbox rewrites
	// must reload-and-merge like the disk spool's saves, never blindly
	// serialize this process's mirror: a sibling's receipt appended after
	// this process loaded would otherwise be clobbered by the next rewrite —
	// and, symmetrically, a receipt the sibling pruned must not resurrect
	// from this process's stale mirror copy.
	dir := t.TempDir()
	fileKeys := func() string {
		t.Helper()
		probe := newConsentOutbox(dir)
		keys := make([]string, 0, 4)
		for _, entry := range probe.readRecordReceipts() {
			keys = append(keys, entry.IdempotencyKey)
		}
		return strings.Join(keys, ",")
	}

	clientA := newConsentOutbox(dir)
	clientA.load(nil)
	if clientA.append(testConsentReceipt("key-a-1", true)) {
		t.Fatalf("append a1: unexpected persist failure")
	}

	clientB := newConsentOutbox(dir)
	clientB.load(func(entry consentReceipt) bool {
		return strings.HasPrefix(entry.IdempotencyKey, "key-b-")
	})
	siblingReceipt := testConsentReceipt("key-b-1", false)
	siblingReceipt.ActorIdentifier = "anon-sibling-2" // another scope's decision
	if clientB.append(siblingReceipt) {
		t.Fatalf("append b1: unexpected persist failure")
	}

	// A's next rewrite runs with a mirror that predates B's append: the
	// merge (fresh disk view first) must preserve b1 while adding A's new
	// receipt after it.
	if clientA.append(testConsentReceipt("key-a-2", true)) {
		t.Fatalf("append a2: unexpected persist failure")
	}
	if got := fileKeys(); got != "key-a-1,key-b-1,key-a-2" {
		t.Fatalf("expected the sibling receipt preserved through the merge, got %q", got)
	}

	// Each client prunes its OWN delivered receipt: settled and gone — and
	// NOT resurrected by the other's next save, even though that other
	// client's stale mirror still carries a copy.
	if clientA.prune("key-a-1") {
		t.Fatalf("prune a1: unexpected persist failure")
	}
	if clientB.prune("key-b-1") {
		t.Fatalf("prune b1: unexpected persist failure")
	}
	if got := fileKeys(); got != "key-a-2" {
		t.Fatalf("expected both prunes honored across the shared file, got %q", got)
	}
}

func TestConsentFloorMintFailureWithholdsAndRetries(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// GRANT: a failed idempotency-key mint for a CONFIGURED actor must NOT
	// take the actorless local-only path: the receipt is OWED (retried at
	// every dispatch point), the granted record is withheld — a restart
	// meanwhile must not flow events with no receipt ever possible — and
	// the batch legs hold exactly as behind a failed append.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	mintErr := errors.New("entropy exhausted")
	var failing atomic.Bool
	failing.Store(true)
	client.consentOwedMu.Lock()
	client.consentMintIDFn = func() (string, error) {
		if failing.Load() {
			return "", mintErr
		}
		return uuidv7.New()
	}
	client.consentOwedMu.Unlock()

	client.SetConsent(true)
	if got := client.Consent(); got != ConsentGranted {
		t.Fatalf("expected the decision applied locally, got %v", got)
	}
	if _, ok := loadConsentRecord(dir, spoolTestActorDigest()); ok {
		t.Fatalf("expected the granted record WITHHELD while the receipt is owed to the failed mint")
	}
	if client.consentOutbox.pending() {
		t.Fatalf("test shape: no receipt may exist while the mint is owed")
	}
	if got := client.Snapshot().LastConsentError; got != "consent_receipt_mint_failed" {
		t.Fatalf("expected the mint failure diagnosed, got %q", got)
	}
	if err := client.Enqueue(Event{ID: "evt-mint-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); !errors.Is(err, ErrConsentReceiptPending) {
		t.Fatalf("expected the batch legs held behind the owed mint, got %v", err)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected no batch before the receipt exists, got %d", got)
	}

	// The mint heals: the next dispatch point mints the owed receipt with
	// the ORIGINAL decision's stamp, appends it, completes the withheld
	// record, and the receipt precedes the batch on the wire.
	failing.Store(false)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after the heal: %v", err)
	}
	waitFor(t, 3*time.Second, "receipt then batch delivered", func() bool {
		return state.consentCount() == 1 && state.batchCount() == 1
	})
	if order := state.snapshotOrder(); len(order) < 2 || order[0] != "consent" || order[1] != "batch" {
		t.Fatalf("expected the healed receipt to precede the batch, got %v", order)
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("expected the withheld record completed after the mint healed, got (%v, %v)", recorded, ok)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// DENIAL: record-first still writes the denied record, but the deny
	// receipt is owed to the failed mint and Close must PEND (retryable)
	// rather than read clean over a receipt that is coming.
	deniedDir := t.TempDir()
	deniedClient := newFloorTestClient(t, server.URL, deniedDir, nil)
	var denyFailing atomic.Bool
	denyFailing.Store(true)
	deniedClient.consentOwedMu.Lock()
	deniedClient.consentMintIDFn = func() (string, error) {
		if denyFailing.Load() {
			return "", mintErr
		}
		return uuidv7.New()
	}
	deniedClient.consentOwedMu.Unlock()
	deniedClient.SetConsent(false)
	if recorded, ok := loadConsentRecord(deniedDir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the denied record written FIRST regardless, got (%v, %v)", recorded, ok)
	}
	if err := deniedClient.Close(context.Background()); !errors.Is(err, ErrConsentPending) {
		t.Fatalf("expected Close pending on the owed mint, got %v", err)
	}
	denyFailing.Store(false)
	if err := deniedClient.Close(context.Background()); err != nil {
		t.Fatalf("expected the retried Close to mint and deliver, got %v", err)
	}
	waitFor(t, 3*time.Second, "the owed deny receipt delivered", func() bool {
		return state.consentCount() == 2
	})
	if consentBoolCategory(t, state.consentAt(1)) {
		t.Fatalf("expected the denial receipt on the wire, got %v", state.consentAt(1))
	}
}

func TestConsentFloorCorruptStampFloorRecordReadsAbsent(t *testing.T) {
	// A floor-marked record with an empty or garbled decided_at cannot be
	// ordered against the receipt trail: it must read as ABSENT
	// (fail-closed), never as an unorderable decision — a stale grant would
	// otherwise beat a durable NEWER deny receipt purely because the file
	// was damaged. Legacy (unmarked) records keep loading stampless: their
	// grants are already vetted by provenance and their denials honored.
	writeFloorRecord := func(t *testing.T, dir, stamp string) {
		t.Helper()
		payload := []byte(fmt.Sprintf(`{"consent_analytics":"granted","actor_digest":%q,"decided_at":%q,"floor":true}`, spoolTestActorDigest(), stamp))
		if err := os.WriteFile(consentRecordPath(dir), payload, 0o600); err != nil {
			t.Fatalf("write consent record: %v", err)
		}
	}

	// Unit shape: both corrupt-stamp flavors read as no usable decision,
	// while a legacy stampless record still loads.
	unitDir := t.TempDir()
	for _, stamp := range []string{"", "not-a-time"} {
		writeFloorRecord(t, unitDir, stamp)
		if _, ok := loadConsentRecordInfo(unitDir, spoolTestActorDigest()); ok {
			t.Fatalf("expected the floor-marked record with stamp %q unusable (absent)", stamp)
		}
	}
	legacy := []byte(fmt.Sprintf(`{"consent_analytics":"denied","actor_digest":%q}`, spoolTestActorDigest()))
	if err := os.WriteFile(consentRecordPath(unitDir), legacy, 0o600); err != nil {
		t.Fatalf("write legacy record: %v", err)
	}
	if info, ok := loadConsentRecordInfo(unitDir, spoolTestActorDigest()); !ok || info.state != ConsentDenied || info.floor {
		t.Fatalf("expected the legacy stampless denial still loading, got (%+v, %v)", info, ok)
	}

	state, server := newFloorTestServer(t)
	defer server.Close()

	// Reload shape 1: corrupt floor-marked grant, no trail → undecided (a
	// dark session), never a live grant.
	bareDir := t.TempDir()
	writeFloorRecord(t, bareDir, "not-a-time")
	bare := newFloorTestClient(t, server.URL, bareDir, nil)
	if got := bare.Consent(); got != ConsentUnknown {
		t.Fatalf("expected the corrupt-stamp record to start the floor undecided, got %v", got)
	}
	if err := bare.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("expected the undecided refusal, got %v", err)
	}
	if err := bare.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reload shape 2: corrupt floor-marked grant PLUS a durable in-scope
	// deny receipt → the record reads absent, so the proof applies
	// unconditionally and heals a proper floor-marked denied record with
	// the receipt's stamp.
	healDir := t.TempDir()
	writeFloorRecord(t, healDir, "not-a-time")
	planted := newConsentOutbox(healDir)
	deny := testConsentReceipt("key-corrupt-deny-1", false)
	if planted.append(deny) {
		t.Fatalf("seeding the deny receipt failed")
	}
	healed := newFloorTestClient(t, server.URL, healDir, nil)
	if got := healed.Consent(); got != ConsentDenied {
		t.Fatalf("expected the durable deny receipt to beat the corrupt grant record, got %v", got)
	}
	info, ok := loadConsentRecordInfo(healDir, spoolTestActorDigest())
	if !ok || info.state != ConsentDenied || !info.floor || info.decidedAt != deny.DecidedAt {
		t.Fatalf("expected the record healed from the proof (floor-marked, receipt stamp), got (%+v, %v)", info, ok)
	}
	waitFor(t, 3*time.Second, "the retained deny receipt delivered", func() bool {
		return state.consentCount() == 1
	})
	if err := healed.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorFlushJoinsInFlightDispatchPass(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A caller-driven drain must never silently skip when a concurrent pass
	// holds the serial-dispatch claim: pre-fix, a Flush losing the claim on
	// the denied path returned SUCCESS with the denial receipt undrained.
	// The test holds the claim itself — a deterministic in-flight pass — so
	// the decision's worker wake cannot deliver either.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	if !client.consentOutbox.claimDispatch() {
		t.Fatalf("test shape: claiming the dispatch lock failed")
	}
	client.SetConsent(false) // the deny receipt is retained; every pass loses the claim

	// A bounded caller joins up to its own deadline, then reports THAT
	// bound — never nil success over the undrained trail.
	shortCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	err := client.Flush(shortCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the cut join to surface the caller's bound over the undrained trail, got %v", err)
	}
	if got := state.consentCount(); got != 0 {
		t.Fatalf("test shape: nothing may deliver while the claim is held, got %d", got)
	}

	// An unbounded caller JOINS: the flush parks as a dispatch waiter, the
	// in-flight pass releases, and the flush then drains the trail ITSELF
	// before returning success.
	flushErr := make(chan error, 1)
	go func() {
		flushErr <- client.Flush(context.Background())
	}()
	waitFor(t, 3*time.Second, "the flush parked as a dispatch waiter", func() bool {
		client.consentOutbox.mu.Lock()
		defer client.consentOutbox.mu.Unlock()
		return len(client.consentOutbox.dispatchWaiters) > 0
	})
	client.consentOutbox.releaseDispatch()
	if err := <-flushErr; err != nil {
		t.Fatalf("expected the joined flush to drain and succeed, got %v", err)
	}
	if got := state.consentCount(); got != 1 {
		t.Fatalf("expected the deny receipt drained BY the joined flush before it returned, got %d", got)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorCloseRemnantCapacityEvictionFoldsDiscarded(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A close remnant larger than the spool cap evicts at the STOP-path
	// append. That eviction lands durably gone at exit — there is no later
	// resend, unlike a mid-session eviction — so it must fold into the
	// Close verdict like every other close-remnant loss. Pre-fix: the fold
	// counted only gate refusals and still-unpersisted mirror entries, the
	// evicted event was silently lost, and Close read nil.
	dir := t.TempDir()
	transport := &eofOnConsentTransport{}
	client := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		cfg.SpoolMaxEvents = 1
		cfg.HTTPClient = &http.Client{Transport: transport}
	})
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})

	// Park a superseding grant receipt with NO observed outcome (send-path
	// EOF): unhanded, it keeps the gate armed through Close's final flush,
	// so the queued events reach the spool only through the stop path's
	// close-remnant append — where the cap evicts the older one.
	transport.setFailing(true)
	client.SetConsent(true)
	if err := client.Enqueue(Event{ID: "evt-cap-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Enqueue(Event{ID: "evt-cap-2", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	closeErr := client.Close(context.Background())
	if !errors.Is(closeErr, ErrEventsDiscarded) {
		t.Fatalf("expected the capacity-evicted remnant folded into the Close verdict, got %v", closeErr)
	}
	if errors.Is(closeErr, ErrConsentPending) {
		t.Fatalf("the parked receipt is durably on disk; only the discard may report: %v", closeErr)
	}
	stats := client.Snapshot()
	if stats.SpoolEvicted != 1 {
		t.Fatalf("expected exactly the one cap eviction, got %d", stats.SpoolEvicted)
	}
	if stats.Dropped == 0 {
		t.Fatalf("expected the exit-time eviction counted Dropped")
	}
	if stats.Spooled != 1 {
		t.Fatalf("expected the surviving remnant event durably spooled, got %d", stats.Spooled)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected the gated close to publish nothing, got %d batches", got)
	}
	// The fold is permanent: a repeated Close still reports the loss.
	if err := client.Close(context.Background()); !errors.Is(err, ErrEventsDiscarded) {
		t.Fatalf("expected the discard permanent on repeated Close, got %v", err)
	}
}

func TestConsentFloorRetryableCloseSurfacesCallerBound(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()
	state.setConsentOutcome(http.StatusServiceUnavailable, "3600")

	// MEMORY-ONLY floor with a parked receipt: the first Close correctly
	// declines with the retryable ErrConsentPending. A RETRIED Close whose
	// own deadline expires mid-drain (here: waiting out a held dispatch
	// claim) must fold the caller's context error into the verdict — bare
	// ErrConsentPending would hide that this Close never ran its delivery
	// attempt to completion within the caller's bound.
	client := newFloorTestClient(t, server.URL, "", nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the parked receipt counted", func() bool {
		return client.Snapshot().ConsentFailed >= 1
	})
	if err := client.Close(context.Background()); !errors.Is(err, ErrConsentPending) {
		t.Fatalf("expected the memory-only parked receipt to pend Close, got %v", err)
	}

	if !client.consentOutbox.claimDispatch() {
		t.Fatalf("test shape: claiming the dispatch lock failed")
	}
	shortCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	err := client.Close(shortCtx)
	cancel()
	if !errors.Is(err, ErrConsentPending) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the retried Close to carry BOTH the pending state and the caller's bound, got %v", err)
	}
	client.consentOutbox.releaseDispatch()

	// Released, healed, deferral cleared: the next retried Close drains and
	// completes clean.
	state.setConsentOutcome(http.StatusOK, "")
	clearConsentDeferral(client)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("expected the healed retried Close to complete, got %v", err)
	}
	// Exactly two arrivals: the decision-time attempt the server answered
	// 503 (an observed outcome — it reached the server), then the final
	// Close's successful delivery of the retained receipt.
	if got := state.consentCount(); got != 2 {
		t.Fatalf("expected the 503-answered attempt plus the final delivery, got %d", got)
	}
}

func TestConsentFloorPoisonedCloseRemnantFoldsDiscarded(t *testing.T) {
	_, server := newFloorTestServer(t)
	defer server.Close()

	// A close-remnant member that cannot serialize (poisoned Props) settles
	// at the stop path — a teardown loss: it must fold into the Close
	// verdict (its Dropped count already happened at the settle) instead of
	// letting Close read nil over it.
	dir := t.TempDir()
	transport := &eofOnConsentTransport{}
	client := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		cfg.HTTPClient = &http.Client{Transport: transport}
	})
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	transport.setFailing(true)
	client.SetConsent(true) // parked with no observed outcome: gates the final flush
	if err := client.Enqueue(Event{ID: "evt-poison-1", Name: "e1", Props: map[string]any{"bad": func() {}}}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Enqueue(Event{ID: "evt-poison-2", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	closeErr := client.Close(context.Background())
	if !errors.Is(closeErr, ErrEventsDiscarded) {
		t.Fatalf("expected the poisoned remnant member folded into the Close verdict, got %v", closeErr)
	}
	if errors.Is(closeErr, ErrConsentPending) {
		t.Fatalf("the parked receipt is durably on disk; only the discard may report: %v", closeErr)
	}
	stats := client.Snapshot()
	if stats.Dropped == 0 {
		t.Fatalf("expected the poisoned member counted Dropped by its settle")
	}
	if stats.Spooled != 1 {
		t.Fatalf("expected the healthy remnant member durably spooled, got %d", stats.Spooled)
	}
}

func TestConsentFloorExpiredCloseRemnantFoldsDiscarded(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A close-remnant member already past the retry-age cap never lands on
	// disk (the append refuses it as too old to ever resend) — at teardown
	// that is a permanent loss, folded into the Close verdict exactly like
	// gate refusals and capacity evictions.
	dir := t.TempDir()
	transport := &eofOnConsentTransport{}
	client := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		cfg.HTTPClient = &http.Client{Transport: transport}
	})
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	transport.setFailing(true)
	client.SetConsent(true) // parked with no observed outcome: gates the final flush
	if err := client.Enqueue(Event{ID: "evt-expired-1", Name: "e1", Timestamp: time.Now().Add(-8 * 24 * time.Hour)}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Enqueue(Event{ID: "evt-fresh-1", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	closeErr := client.Close(context.Background())
	if !errors.Is(closeErr, ErrEventsDiscarded) {
		t.Fatalf("expected the expired remnant member folded into the Close verdict, got %v", closeErr)
	}
	stats := client.Snapshot()
	if stats.SpoolExpired != 1 {
		t.Fatalf("expected exactly the one retry-age expiry, got %d", stats.SpoolExpired)
	}
	if stats.Dropped == 0 {
		t.Fatalf("expected the exit-time expiry counted Dropped")
	}
	if stats.Spooled != 1 {
		t.Fatalf("expected the fresh remnant member durably spooled, got %d", stats.Spooled)
	}
	if got := state.batchCount(); got != 0 {
		t.Fatalf("expected the gated close to publish nothing, got %d batches", got)
	}
}

func TestConsentOutboxOverCapLoadTrimLandsDurably(t *testing.T) {
	// The load-time cap trim is a MEMORY change: without marking the write
	// owed, a process that never saves before exiting leaves the over-cap
	// file behind, and every restart re-evicts and re-counts the same
	// entries. The trim must owe the rewrite; the owed-write machinery
	// lands it at the first retry.
	dir := t.TempDir()
	over := maxConsentOutboxEntries + 8
	record := consentOutboxWire{Version: consentOutboxRecordVersion}
	for i := 0; i < over; i++ {
		record.Receipts = append(record.Receipts, testConsentReceipt(fmt.Sprintf("key-trim-%02d", i), false))
	}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal seed record: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, consentOutboxFileName), payload, 0o600); err != nil {
		t.Fatalf("write seed record: %v", err)
	}

	outbox := newConsentOutbox(dir)
	outbox.load(nil)
	if got := outbox.takeEvicted(); got != 8 {
		t.Fatalf("expected the 8 load-time evictions counted, got %d", got)
	}
	if !outbox.writeOwed() {
		t.Fatalf("expected the load-time trim to OWE the durable rewrite")
	}
	if attempted, failed := outbox.retryPersist(); !attempted || failed {
		t.Fatalf("expected the owed rewrite attempted and landed, got (%v, %v)", attempted, failed)
	}
	if got := len(outbox.readRecordReceipts()); got != maxConsentOutboxEntries {
		t.Fatalf("expected the trimmed record durable on disk, got %d entries", got)
	}

	// A restart sees a within-cap record: no re-eviction, no re-count, no
	// owed write.
	reloaded := newConsentOutbox(dir)
	reloaded.load(nil)
	if got := reloaded.takeEvicted(); got != 0 {
		t.Fatalf("expected no re-eviction after the durable trim, got %d", got)
	}
	if reloaded.writeOwed() {
		t.Fatalf("expected no owed write on a within-cap record")
	}
}

func TestConsentFloorCorruptStampFloorDenialStaysDenied(t *testing.T) {
	// A floor-marked DENIED record with a garbled stamp must PRESERVE the
	// denial (denied-with-unknown-stamp) — read as absent, a stale retained
	// grant receipt would apply unconditionally and reopen the floor
	// against a durable denial. Only corrupt-stamped GRANTS read as absent:
	// the two flavors fail closed in opposite directions.
	writeRecord := func(t *testing.T, dir, state, stamp string) {
		t.Helper()
		payload := []byte(fmt.Sprintf(`{"consent_analytics":%q,"actor_digest":%q,"decided_at":%q,"floor":true}`, state, spoolTestActorDigest(), stamp))
		if err := os.WriteFile(consentRecordPath(dir), payload, 0o600); err != nil {
			t.Fatalf("write consent record: %v", err)
		}
	}

	// Unit shape: corrupt-stamped denials load as denied with the stamp
	// cleared (both flavors); the corrupt-stamped grant stays absent.
	unitDir := t.TempDir()
	writeRecord(t, unitDir, "denied", "not-a-time")
	if info, ok := loadConsentRecordInfo(unitDir, spoolTestActorDigest()); !ok || info.state != ConsentDenied || info.decidedAt != "" || !info.floor {
		t.Fatalf("expected the corrupt-stamped denial preserved stampless, got (%+v, %v)", info, ok)
	}
	writeRecord(t, unitDir, "denied_forced_minor", "")
	if info, ok := loadConsentRecordInfo(unitDir, spoolTestActorDigest()); !ok || info.state != ConsentDeniedForcedMinor || info.decidedAt != "" {
		t.Fatalf("expected the corrupt-stamped forced-minor denial preserved, got (%+v, %v)", info, ok)
	}
	writeRecord(t, unitDir, "granted", "not-a-time")
	if _, ok := loadConsentRecordInfo(unitDir, spoolTestActorDigest()); ok {
		t.Fatalf("expected the corrupt-stamped grant absent (fail-closed)")
	}

	// Reload shape: corrupt-stamped denial PLUS a stale retained in-scope
	// GRANT receipt — the floor-marked stampless record is never superseded
	// by comparison, so the session stays denied, and the stale grant stays
	// RETAINED rather than posting: under a durably denied live state with
	// no newer denial receipt behind it, a grant must not become the wire's
	// last word (it is durable on disk; if it ever posts it is a replay the
	// server de-duplicates by key).
	state, server := newFloorTestServer(t)
	defer server.Close()
	dir := t.TempDir()
	writeRecord(t, dir, "denied", "not-a-time")
	planted := newConsentOutbox(dir)
	if planted.append(testConsentReceipt("key-stale-grant-1", true)) {
		t.Fatalf("seeding the stale grant receipt failed")
	}
	client := newFloorTestClient(t, server.URL, dir, nil)
	if got := client.Consent(); got != ConsentDenied {
		t.Fatalf("expected the corrupt-stamped denial to rule the reload, got %v", got)
	}
	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("expected the denial live, got %v", err)
	}
	_ = client.Flush(context.Background()) // a full dispatch pass
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected the stale grant RETAINED under the denied state, got %d posts", got)
	}
	if got := client.Consent(); got != ConsentDenied {
		t.Fatalf("expected the denial to survive the pass, got %v", got)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close over the durably retained receipt: %v", err)
	}
	if probe := newConsentOutbox(dir); len(probe.readRecordReceipts()) != 1 {
		t.Fatalf("expected the stale grant durably retained after Close")
	}
}

func TestConsentFloorLegacyRecordSupersededByStampedProof(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A LEGACY stampless record predates the stamping build: any
	// validly-stamped in-scope proof supersedes it in BOTH directions.
	writeLegacyRecord := func(t *testing.T, dir, decision string) {
		t.Helper()
		payload := []byte(fmt.Sprintf(`{"consent_analytics":%q,"actor_digest":%q}`, decision, spoolTestActorDigest()))
		if err := os.WriteFile(consentRecordPath(dir), payload, 0o600); err != nil {
			t.Fatalf("write consent record: %v", err)
		}
	}

	// Legacy GRANT + durable denial proof: pre-fix the compare blocked the
	// override and provenance stranded the floor UNDECIDED — losing the
	// denial. The proof must heal denied.
	deniedDir := t.TempDir()
	writeLegacyRecord(t, deniedDir, "granted")
	planted := newConsentOutbox(deniedDir)
	if planted.append(testConsentReceipt("key-legacy-deny-1", false)) {
		t.Fatalf("seeding the deny receipt failed")
	}
	deniedClient := newFloorTestClient(t, server.URL, deniedDir, nil)
	if got := deniedClient.Consent(); got != ConsentDenied {
		t.Fatalf("expected the stamped denial proof to supersede the legacy grant, got %v", got)
	}
	info, ok := loadConsentRecordInfo(deniedDir, spoolTestActorDigest())
	if !ok || info.state != ConsentDenied || !info.floor || info.decidedAt != "2026-07-19T00:00:00Z" {
		t.Fatalf("expected the record healed denied from the proof, got (%+v, %v)", info, ok)
	}
	if err := deniedClient.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Legacy DENIAL + durable later grant proof: the user's newest decision
	// is the grant — the proof supersedes the legacy denial and heals a
	// floor-marked grant.
	grantedDir := t.TempDir()
	writeLegacyRecord(t, grantedDir, "denied")
	planted = newConsentOutbox(grantedDir)
	if planted.append(testConsentReceipt("key-legacy-grant-1", true)) {
		t.Fatalf("seeding the grant receipt failed")
	}
	grantedClient := newFloorTestClient(t, server.URL, grantedDir, nil)
	if got := grantedClient.Consent(); got != ConsentGranted {
		t.Fatalf("expected the stamped grant proof to supersede the legacy denial, got %v", got)
	}
	info, ok = loadConsentRecordInfo(grantedDir, spoolTestActorDigest())
	if !ok || info.state != ConsentGranted || !info.floor {
		t.Fatalf("expected the record healed to a floor-marked grant, got (%+v, %v)", info, ok)
	}
	waitFor(t, 3*time.Second, "the retained proofs delivered", func() bool {
		return state.consentCount() == 2
	})
	if err := grantedClient.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentOutboxDuplicateKeysKeepFirstAtLoad(t *testing.T) {
	// Duplicate idempotency keys never come from this SDK; a corrupt or
	// hand-edited record can carry them with DIFFERENT decision bodies. The
	// load keeps the FIRST occurrence — the server-consistent choice: the
	// ingest service de-duplicates by key and honors the first body it saw,
	// so a later conflicting body could never take effect server-side.
	dir := t.TempDir()
	first := testConsentReceipt("key-dup-1", true)
	conflicting := testConsentReceipt("key-dup-1", false)
	conflicting.DecidedAt = "2026-07-19T01:00:00Z"
	other := testConsentReceipt("key-uniq-1", false)
	record := consentOutboxWire{Version: consentOutboxRecordVersion, Receipts: []consentReceipt{first, conflicting, other}}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal seed record: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, consentOutboxFileName), payload, 0o600); err != nil {
		t.Fatalf("write seed record: %v", err)
	}

	outbox := newConsentOutbox(dir)
	outbox.load(nil)
	outbox.mu.Lock()
	keys := make([]string, 0, len(outbox.receipts))
	var firstKept *bool
	for _, entry := range outbox.receipts {
		keys = append(keys, entry.IdempotencyKey)
		if entry.IdempotencyKey == "key-dup-1" {
			firstKept = entry.Categories.Analytics
		}
	}
	outbox.mu.Unlock()
	if len(keys) != 2 || keys[0] != "key-dup-1" || keys[1] != "key-uniq-1" {
		t.Fatalf("expected the duplicate dropped keep-first, got %v", keys)
	}
	if firstKept == nil || !*firstKept {
		t.Fatalf("expected the FIRST body (the grant) kept for the duplicated key")
	}
}

func TestConsentOutboxSanitizerRejectsInvalidReason(t *testing.T) {
	// The only reason this SDK mints is denied_forced_minor, and only on
	// denials: a grant carrying it is a self-contradictory statement, and
	// an unknown reason is not a receipt this SDK could have written — both
	// drop fail-safe rather than re-send.
	grantWithReason := testConsentReceipt("key-reason-1", true)
	grantWithReason.Reason = consentDecisionReason
	if _, ok := sanitizeConsentReceipt(grantWithReason); ok {
		t.Fatalf("expected a GRANT with the forced-minor reason dropped")
	}
	unknownReason := testConsentReceipt("key-reason-2", false)
	unknownReason.Reason = "because"
	if _, ok := sanitizeConsentReceipt(unknownReason); ok {
		t.Fatalf("expected an unknown reason dropped")
	}
	forcedMinor := testConsentReceipt("key-reason-3", false)
	forcedMinor.Reason = consentDecisionReason
	if sanitized, ok := sanitizeConsentReceipt(forcedMinor); !ok || sanitized.Reason != consentDecisionReason {
		t.Fatalf("expected the reasoned denial kept, got (%+v, %v)", sanitized, ok)
	}
	plainGrant := testConsentReceipt("key-reason-4", true)
	if _, ok := sanitizeConsentReceipt(plainGrant); !ok {
		t.Fatalf("expected the reasonless grant kept")
	}
	plainDenial := testConsentReceipt("key-reason-5", false)
	if _, ok := sanitizeConsentReceipt(plainDenial); !ok {
		t.Fatalf("expected the reasonless denial kept")
	}
}

func TestConsentFloorGrantHeldBehindParkedNewerDenial(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// Third variant of the stale-grant family: the retained grant's OWN
	// pair is complete (receipt durable, record persisted), and the newer
	// denial is minted and durably appended but HELD — its record write
	// owed, the deny proof parked pending the heal. The grant must not
	// dispatch and prune past it: the server's last word would be granted
	// against the local denial for as long as the heal keeps failing.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	if !client.consentOutbox.claimDispatch() {
		t.Fatalf("test shape: claiming the dispatch lock failed")
	}
	client.SetConsent(true) // pair completes cleanly; receipt retained (claim held)
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("test shape: expected the granted record persisted, got (%v, %v)", recorded, ok)
	}
	recordErr := errors.New("disk full")
	var failing atomic.Bool
	failing.Store(true)
	client.spool.renameFn = func(oldpath, newpath string) error {
		if failing.Load() && strings.Contains(newpath, consentRecordFileName) {
			return recordErr
		}
		return os.Rename(oldpath, newpath)
	}
	client.SetConsent(false) // denied record write fails: deny receipt durable but HELD
	client.consentOutbox.releaseDispatch()

	_ = client.Flush(context.Background()) // a full dispatch pass; NOTHING may deliver
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected the grant HELD behind the parked newer denial, got %d consent posts", got)
	}

	// The record heals: the same pass lands the denied record, releases the
	// trail, and delivers grant-then-denial in decision order.
	failing.Store(false)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after the heal: %v", err)
	}
	waitFor(t, 3*time.Second, "the trail delivered in order", func() bool {
		return state.consentCount() == 2
	})
	if !consentBoolCategory(t, state.consentAt(0)) || consentBoolCategory(t, state.consentAt(1)) {
		t.Fatalf("expected grant-then-denial in decision order, got %v then %v", state.consentAt(0), state.consentAt(1))
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the healed denied record, got (%v, %v)", recorded, ok)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorFailedHealPreservesResolvedGrantSpool(t *testing.T) {
	// A grant-tail reload whose consent.json heal FAILED: the live state
	// resolves GRANTED from the durable proof while the on-disk record
	// still reads absent (here: a corrupt-stamped grant). The spool
	// decision must consult the RESOLVED truth — keep and load spool.json,
	// gated (grantPersisted stays false until the owed heal lands) — never
	// purge and dead-letter events the resolved grant plainly covers.
	dir := t.TempDir()
	payload := []byte(fmt.Sprintf(`{"consent_analytics":"granted","actor_digest":%q,"decided_at":"not-a-time","floor":true}`, spoolTestActorDigest()))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(consentRecordPath(dir), payload, 0o600); err != nil {
		t.Fatalf("write corrupt record: %v", err)
	}
	planted := newConsentOutbox(dir)
	if planted.append(testConsentReceipt("key-heal-grant-1", true)) {
		t.Fatalf("seeding the grant proof failed")
	}
	writeSpoolRecordFile(t, dir, 0, spoolTestEnvelope(t, "evt-preserve-1", time.Now()))

	cfg := Config{
		WorkspaceID:    "workspace-test",
		AppID:          "app-test",
		EnvironmentID:  "develop",
		AnonymousID:    "anon-spool-1",
		SpoolDir:       dir,
		SpoolMaxEvents: 100,
		SpoolMaxBytes:  1 << 20,
		ConsentFloor:   &ConsentFloorConfig{},
	}
	client := &Client{cfg: cfg, clock: realClock{}}
	client.initConsentFloor(func(oldpath, newpath string) error {
		if strings.Contains(newpath, consentRecordFileName) {
			return errors.New("disk full")
		}
		return os.Rename(oldpath, newpath)
	}, os.Chmod)
	if got := client.Consent(); got != ConsentGranted {
		t.Fatalf("test shape: expected the proof-resolved grant, got %v", got)
	}
	if owed := client.consentRecordOwedSnapshot(); owed == nil || owed.decision != ConsentDecisionGranted {
		t.Fatalf("test shape: expected the failed heal owed, got %+v", owed)
	}

	client.spool = newDiskSpool(cfg)
	letters := client.initSpool()
	if len(letters) != 0 {
		t.Fatalf("expected NO dead-letters for a spool the resolved grant covers, got %v", letters)
	}
	if _, err := os.Stat(filepath.Join(dir, spoolFileName)); err != nil {
		t.Fatalf("expected spool.json preserved under the resolved grant, got %v", err)
	}
	client.spool.mu.Lock()
	loaded := len(client.spool.entries)
	gateOpen := client.spool.grantPersisted
	client.spool.mu.Unlock()
	if loaded != 1 {
		t.Fatalf("expected the spooled event loaded for resend, got %d entries", loaded)
	}
	if gateOpen {
		t.Fatalf("expected the write gate CLOSED while the heal is owed (grantPersisted false)")
	}
}

func TestConsentFloorRetriedCloseWaitsForWorkerStop(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// An earlier Close that timed out BEFORE workerDone leaves the worker's
	// stop path unfinished; the retried Close (taken because consent was
	// pending) must not return nil without waiting for it — the caller
	// would exit before the close remnant is spooled or counted.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	if !client.consentOutbox.claimDispatch() {
		t.Fatalf("test shape: claiming the dispatch lock failed")
	}
	client.SetConsent(true) // pair completes; receipt retained under the held claim
	outboxErr := errors.New("disk full")
	var outboxFailing atomic.Bool
	client.consentOutbox.mu.Lock()
	client.consentOutbox.renameFn = func(oldpath, newpath string) error {
		if outboxFailing.Load() {
			return outboxErr
		}
		return os.Rename(oldpath, newpath)
	}
	client.consentOutbox.mu.Unlock()
	outboxFailing.Store(true)
	client.consentOutbox.releaseDispatch()
	// The delivery acks but the prune REWRITE fails: consent stays pending
	// (dirty outbox, nothing retained) while the spool write gate is OPEN
	// (the granted record persisted cleanly).
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := client.Snapshot().ConsentRecorded; got != 1 {
		t.Fatalf("test shape: expected the grant delivered, got %d", got)
	}
	if !client.consentOutbox.writeOwed() {
		t.Fatalf("test shape: expected the prune rewrite owed")
	}

	// Stall the worker's NEXT spool.json write (the 503-failed batch's
	// crash-insurance append) so Close #1 times out with the worker stuck.
	stallRelease := make(chan struct{})
	var stallOnce sync.Once
	client.spool.renameFn = func(oldpath, newpath string) error {
		if strings.Contains(newpath, spoolFileName) {
			stallOnce.Do(func() { <-stallRelease })
		}
		return os.Rename(oldpath, newpath)
	}
	state.setBatchOutcome(http.StatusServiceUnavailable)
	if err := client.Enqueue(Event{ID: "evt-retry-close-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	shortCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	err := client.Close(shortCtx)
	cancel()
	if !errors.Is(err, ErrConsentPending) {
		t.Fatalf("test shape: expected the first Close pending on the owed outbox write, got %v", err)
	}

	// The outbox heals, but the WORKER is still stalled mid-write: the
	// retried Close settles the consent plane yet must NOT return nil —
	// bounded by its own context, it reports the bound instead of letting
	// the caller exit over an unspooled remnant.
	outboxFailing.Store(false)
	retryCtx, cancelRetry := context.WithTimeout(context.Background(), 2*time.Second)
	retryErr := client.Close(retryCtx)
	cancelRetry()
	if retryErr == nil {
		t.Fatalf("expected the retried Close to wait for the worker stop path, got nil with the remnant unspooled")
	}

	// Release the stall: the worker finishes, and the remnant lands
	// durably.
	close(stallRelease)
	waitFor(t, 3*time.Second, "the remnant spooled durably", func() bool {
		return client.Snapshot().Spooled == 1
	})
	data, err := os.ReadFile(filepath.Join(dir, spoolFileName))
	if err != nil || !strings.Contains(string(data), "evt-retry-close-1") {
		t.Fatalf("expected the remnant event durable in spool.json, got (%v, %q)", err, string(data))
	}
}

func TestConsentFloorDenialRecordLandsBeforePurge(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// With a durable prior grant, the denial's RECORD must land before the
	// purge destroys the spool: purge-first opens a crash window where the
	// spool is gone but no durable evidence of the denial exists yet — a
	// relaunch would promote the stale granted record over a destroyed
	// spool. Phase 1 pins the ORDER; phase 2 pins the deferred purge when
	// the record write fails.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})
	state.setBatchOutcome(http.StatusServiceUnavailable)
	if err := client.Enqueue(Event{ID: "evt-order-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retryable batch failure")
	}
	waitFor(t, 3*time.Second, "the event spooled durably", func() bool {
		return client.Snapshot().Spooled == 1
	})

	var opsMu sync.Mutex
	var ops []string
	client.spool.renameFn = func(oldpath, newpath string) error {
		if strings.Contains(newpath, consentRecordFileName) {
			opsMu.Lock()
			ops = append(ops, "record-write")
			opsMu.Unlock()
		}
		return os.Rename(oldpath, newpath)
	}
	client.spool.removeFn = func(path string) error {
		if strings.Contains(path, spoolFileName) {
			opsMu.Lock()
			ops = append(ops, "spool-purge")
			opsMu.Unlock()
		}
		return os.Remove(path)
	}
	client.SetConsent(false)
	opsMu.Lock()
	order := append([]string(nil), ops...)
	opsMu.Unlock()
	if len(order) < 2 || order[0] != "record-write" || order[1] != "spool-purge" {
		t.Fatalf("expected the denied record written BEFORE the spool purge, got %v", order)
	}
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentDenied {
		t.Fatalf("expected the denied record durable, got (%v, %v)", recorded, ok)
	}
	_ = client.Close(context.Background())

	// Phase 2: the record write FAILS — the purge is deferred with it, and
	// the owed-record retry completes both in the pass the record lands.
	dir2 := t.TempDir()
	client2 := newFloorTestClient(t, server.URL, dir2, nil)
	client2.SetConsent(true)
	waitFor(t, 3*time.Second, "the second grant acknowledged", func() bool {
		return client2.Snapshot().ConsentRecorded == 1
	})
	if err := client2.Enqueue(Event{ID: "evt-order-2", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client2.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retryable batch failure")
	}
	waitFor(t, 3*time.Second, "the second event spooled durably", func() bool {
		return client2.Snapshot().Spooled == 1
	})
	recordErr := errors.New("disk full")
	var failing atomic.Bool
	failing.Store(true)
	var purged atomic.Bool
	client2.spool.renameFn = func(oldpath, newpath string) error {
		if failing.Load() && strings.Contains(newpath, consentRecordFileName) {
			return recordErr
		}
		return os.Rename(oldpath, newpath)
	}
	client2.spool.removeFn = func(path string) error {
		if strings.Contains(path, spoolFileName) {
			purged.Store(true)
		}
		return os.Remove(path)
	}
	client2.SetConsent(false)
	if purged.Load() {
		t.Fatalf("expected the purge DEFERRED while the denied record write is owed")
	}
	if _, err := os.Stat(filepath.Join(dir2, spoolFileName)); err != nil {
		t.Fatalf("expected spool.json intact while the record is owed, got %v", err)
	}
	failing.Store(false)
	_ = client2.Flush(context.Background()) // a dispatch point: the owed record retries
	waitFor(t, 3*time.Second, "the deferred purge completed with the record", func() bool {
		recorded, ok := loadConsentRecord(dir2, spoolTestActorDigest())
		return ok && recorded == ConsentDenied && purged.Load()
	})
	if _, err := os.Stat(filepath.Join(dir2, spoolFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected the spool purged once the record landed, got %v", err)
	}
}

func TestConsentFloorChangedAnonymousIDReceiptOutOfScope(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A reused SpoolDir with the SAME UserID but a DIFFERENT configured
	// AnonymousID: the old identity's receipt must be FOREIGN — both actor
	// components are part of the scope, exactly like the record digest —
	// never overriding or healing the new digest's state.
	dir := t.TempDir()
	oldReceipt := testConsentReceipt("key-old-anon-1", true)
	oldReceipt.ActorIdentifier = "user-1"
	oldReceipt.AnonymousID = "anon-A"
	planted := newConsentOutbox(dir)
	if planted.append(oldReceipt) {
		t.Fatalf("seeding the old-identity receipt failed")
	}
	client := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		cfg.UserID = "user-1"
		cfg.AnonymousID = "anon-B"
	})
	if got := client.Consent(); got != ConsentUnknown {
		t.Fatalf("expected the old identity's receipt out of scope (undecided floor), got %v", got)
	}
	if err := client.Track(context.Background(), Event{Name: "e1"}); !errors.Is(err, ErrConsentUnknown) {
		t.Fatalf("expected the undecided refusal, got %v", err)
	}
	newDigest := consentActorDigest(Config{
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		UserID:        "user-1",
		AnonymousID:   "anon-B",
	})
	if _, ok := loadConsentRecordInfo(dir, newDigest); ok {
		t.Fatalf("expected NO healed record for the new digest")
	}
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected the foreign receipt never dispatched with this scope's bearer, got %d", got)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if probe := newConsentOutbox(dir); len(probe.readRecordReceipts()) != 1 {
		t.Fatalf("expected the foreign receipt retained for its own identity")
	}

	// Control: the SAME both-component identity stays in scope and the
	// proof still restores the grant.
	sameDir := t.TempDir()
	sameReceipt := testConsentReceipt("key-same-anon-1", true)
	sameReceipt.ActorIdentifier = "user-1"
	sameReceipt.AnonymousID = "anon-B"
	planted = newConsentOutbox(sameDir)
	if planted.append(sameReceipt) {
		t.Fatalf("seeding the same-identity receipt failed")
	}
	sameClient := newFloorTestClient(t, server.URL, sameDir, func(cfg *Config) {
		cfg.UserID = "user-1"
		cfg.AnonymousID = "anon-B"
	})
	if got := sameClient.Consent(); got != ConsentGranted {
		t.Fatalf("expected the matching identity's proof to restore the grant, got %v", got)
	}
	_ = sameClient.Close(context.Background())
}

func TestConsentFloorGrantHandoffRechecksUnderDecisionLock(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// TOCTOU at the handoff: a decision mid-flight (the record-apply lock
	// held) may be appending a held denial AFTER the pass's hold checks ran.
	// The grant must re-take the decision serialization point before the
	// transport call — and PARK when it cannot (a decision IS mid-flight),
	// never posting past a denial that may be landing right now.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	if !client.consentOutbox.claimDispatch() {
		t.Fatalf("test shape: claiming the dispatch lock failed")
	}
	client.SetConsent(true) // pair completes; receipt retained under the held claim
	client.consentOutbox.releaseDispatch()

	client.consentRecordApplyMu.Lock() // a decision mid-flight, deterministically
	_ = client.Flush(context.Background())
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected the grant PARKED while a decision holds the apply lock, got %d posts", got)
	}
	client.consentRecordApplyMu.Unlock()

	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after the release: %v", err)
	}
	waitFor(t, 3*time.Second, "the grant delivered after the lock released", func() bool {
		return state.consentCount() == 1
	})
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorMintOwedGrantRecordWaits(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A mint-owed GRANT has NO receipt anywhere — the outbox is clean only
	// because there is nothing in it to write, so the owed-record retry's
	// writeOwed guard alone cannot protect the pair. A dispatch point
	// passed while the mint still fails must NOT write the granted
	// consent.json: "granted record, no receipt ever minted" would be
	// durable, and a crash would promote the grant receipt-less at the
	// next launch — exactly what receipt-first forbids.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	mintErr := errors.New("entropy exhausted")
	var failing atomic.Bool
	failing.Store(true)
	client.consentOwedMu.Lock()
	client.consentMintIDFn = func() (string, error) {
		if failing.Load() {
			return "", mintErr
		}
		return uuidv7.New()
	}
	client.consentOwedMu.Unlock()

	client.SetConsent(true)
	if _, ok := loadConsentRecord(dir, spoolTestActorDigest()); ok {
		t.Fatalf("test setup: expected the granted record withheld at decision time while the mint is owed")
	}

	// Dispatch points pass with the mint STILL failing: the mint retry
	// fails again, and the record retry must keep waiting on the owed mint
	// — the record may land only AFTER the retried mint appends the
	// receipt, never before it.
	if err := client.Enqueue(Event{ID: "evt-mintwait-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); !errors.Is(err, ErrConsentReceiptPending) {
		t.Fatalf("expected the batch legs held behind the owed mint, got %v", err)
	}
	if _, ok := loadConsentRecord(dir, spoolTestActorDigest()); ok {
		t.Fatalf("expected the granted record STILL withheld after a dispatch point with the mint owed (the pair must complete receipt-first)")
	}
	_ = client.Flush(context.Background())
	if _, ok := loadConsentRecord(dir, spoolTestActorDigest()); ok {
		t.Fatalf("expected the granted record withheld across repeated dispatch points while the mint is owed")
	}
	if client.consentOutbox.pending() {
		t.Fatalf("test shape: no receipt may exist while the mint is owed")
	}
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected no receipt on the wire while the mint is owed, got %d", got)
	}

	// The mint heals: the same dispatch sequence mints the owed receipt,
	// appends it, and only then completes the withheld record — pair in
	// order, receipt before batch on the wire.
	failing.Store(false)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after the heal: %v", err)
	}
	waitFor(t, 3*time.Second, "receipt then batch delivered", func() bool {
		return state.consentCount() == 1 && state.batchCount() == 1
	})
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("expected the withheld record completed after the healed mint appended the receipt, got (%v, %v)", recorded, ok)
	}
	if order := state.snapshotOrder(); len(order) < 2 || order[0] != "consent" || order[1] != "batch" {
		t.Fatalf("expected the healed receipt to precede the batch, got %v", order)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsentFloorSupersedingGrantSettlesPurgeDebt(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// A denial whose record write fails DEFERS its spool purge — but the
	// purge is a DEBT the denial leaves behind, carried independently of
	// the single owed-record slot. A SUPERSEDING grant must settle that
	// debt BEFORE its own record can reopen the spool: the events the
	// denial condemned must never resend, whatever decision follows.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)
	client.SetConsent(true)
	waitFor(t, 3*time.Second, "the grant acknowledged", func() bool {
		return client.Snapshot().ConsentRecorded == 1
	})

	// A retryable batch failure spools the condemned-to-be event durably.
	state.setBatchOutcome(http.StatusServiceUnavailable)
	if err := client.Enqueue(Event{ID: "evt-condemned-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retryable batch failure that spools the event")
	}
	waitFor(t, 3*time.Second, "the event spooled durably", func() bool {
		return client.Snapshot().Spooled == 1
	})
	if _, err := os.Stat(filepath.Join(dir, spoolFileName)); err != nil {
		t.Fatalf("test setup: expected the spooled chunk on disk, got %v", err)
	}

	// Every consent-record write fails from here (denial AND the
	// superseding grant): the denial's purge defers into the durable debt,
	// and the debt must not depend on the grant's record landing either.
	recordWriteErr := errors.New("disk full")
	var failRecordWrites atomic.Bool
	failRecordWrites.Store(true)
	client.spool.mu.Lock()
	client.spool.renameFn = func(oldpath, newpath string) error {
		if failRecordWrites.Load() && strings.HasSuffix(newpath, consentRecordFileName) {
			return recordWriteErr
		}
		return os.Rename(oldpath, newpath)
	}
	client.spool.mu.Unlock()

	client.SetConsent(false)
	if recorded, ok := loadConsentRecord(dir, spoolTestActorDigest()); !ok || recorded != ConsentGranted {
		t.Fatalf("test setup: expected the denied record write to have failed (stale grant on disk), got (%v, %v)", recorded, ok)
	}
	if !wipeOwedMarkerExists(dir) {
		t.Fatalf("expected the deferred purge carried as a DURABLE debt (wipe-owed marker) independent of the owed-record slot")
	}
	if _, err := os.Stat(filepath.Join(dir, spoolFileName)); err != nil {
		t.Fatalf("expected the spool file removal deferred with the debt, got %v", err)
	}
	client.spool.mu.Lock()
	heldEntries, heldResend := len(client.spool.entries), len(client.spool.resend)
	client.spool.mu.Unlock()
	if heldEntries != 0 || heldResend != 0 {
		t.Fatalf("expected the condemned entries dead-lettered from MEMORY at denial time (mirror and resend queue cleared), got %d entries, %d resend chunks", heldEntries, heldResend)
	}

	// The SUPERSEDING grant (its own record write still failing): the owed
	// wipe must settle FIRST — spool file and marker consumed — before
	// anything could reopen the spool for this grant.
	client.SetConsent(true)
	if _, err := os.Stat(filepath.Join(dir, spoolFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected the superseding grant to settle the owed wipe BEFORE reopening the spool (spool file must be gone), got %v", err)
	}
	if wipeOwedMarkerExists(dir) {
		t.Fatalf("expected the settled debt's marker consumed by the superseding grant")
	}

	// Everything heals: the owed GRANT record completes at the next
	// dispatch point, the pipeline reopens, and a fresh event flows — but
	// the condemned event never reappears on the wire. Scope the scan past
	// the setup era: the server recorded the original 503-answered post of
	// the condemned event too, and only what follows the heal may be
	// judged a resend.
	batchesBefore := state.batchCount()
	failRecordWrites.Store(false)
	state.setBatchOutcome(0)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after the heal: %v", err)
	}
	waitFor(t, 3*time.Second, "the healed grant record completed", func() bool {
		recorded, ok := loadConsentRecord(dir, spoolTestActorDigest())
		return ok && recorded == ConsentGranted
	})
	if err := client.Enqueue(Event{ID: "evt-fresh-1", Name: "e2"}); err != nil {
		t.Fatalf("Enqueue after the heal: %v", err)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush of the fresh event: %v", err)
	}
	waitFor(t, 3*time.Second, "the fresh event delivered", func() bool {
		for _, id := range state.batchIDsSince(batchesBefore) {
			if id == "evt-fresh-1" {
				return true
			}
		}
		return false
	})
	for _, id := range state.batchIDsSince(batchesBefore) {
		if id == "evt-condemned-1" {
			t.Fatalf("the denial-condemned event RESENT under the superseding grant: %v", state.batchIDsSince(batchesBefore))
		}
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, id := range state.batchIDsSince(batchesBefore) {
		if id == "evt-condemned-1" {
			t.Fatalf("the denial-condemned event resent at Close: %v", state.batchIDsSince(batchesBefore))
		}
	}
}

func TestConsentFloorFastHalfDenialParksGrantHandoff(t *testing.T) {
	state, server := newFloorTestServer(t)
	defer server.Close()

	// The fast-half window: SetConsent(false) has flipped the LIVE state
	// (fast half, under lifecycleMu) but its slow half has not yet reached
	// the record-apply lock — TryLock at the grant handoff succeeds and no
	// denial receipt or hold is visible in the trail. Handing a retained
	// older grant to the transport in this window could make it the
	// server's last word with the denial's receipt landing right behind.
	// The handoff must consult the LIVE state too and park the grant.
	dir := t.TempDir()
	client := newFloorTestClient(t, server.URL, dir, nil)

	// Retain a completed grant receipt: the dispatch claim is held across
	// the decision — and KEPT held until the denial's fast-half window is
	// open — so neither the decision-time wake nor the grant-arming
	// re-wake can hand the receipt to the transport before the window
	// under test exists (a stray worker pass released early would
	// legitimately deliver the grant pre-denial and void the pin).
	if !client.consentOutbox.claimDispatch() {
		t.Fatalf("test shape: claiming the dispatch lock failed")
	}
	client.SetConsent(true)
	if !client.consentOutbox.pending() {
		t.Fatalf("test shape: expected the grant receipt retained")
	}

	// Hold the NEXT decision open between its halves.
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	client.consentSlowHalfGate = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	denyDone := make(chan struct{})
	go func() {
		defer close(denyDone)
		client.SetConsent(false)
	}()
	<-entered
	// The window is open (live state denied): release the claim only now —
	// a stray worker pass from the earlier wakes lands INSIDE the window
	// and must park exactly like the drain below.
	client.consentOutbox.releaseDispatch()

	// Mid-window drain: live state is denied, the trail holds only the
	// grant, the record-apply lock is free. The retained grant must PARK —
	// zero posts.
	_ = client.Flush(context.Background())
	if got := state.consentCount(); got != 0 {
		t.Fatalf("expected the retained grant PARKED during the denial's fast-half window, got %d posts", got)
	}

	// The denial's slow half completes: its receipt now exists in the
	// trail BEHIND the grant, and both deliver in decision order.
	close(release)
	<-denyDone
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush after the denial completed: %v", err)
	}
	waitFor(t, 3*time.Second, "both receipts delivered in order", func() bool {
		return state.consentCount() == 2
	})
	if !consentBoolCategory(t, state.consentAt(0)) {
		t.Fatalf("expected the GRANT delivered first, got %v", state.consentAt(0))
	}
	if consentBoolCategory(t, state.consentAt(1)) {
		t.Fatalf("expected the DENIAL delivered second, got %v", state.consentAt(1))
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
