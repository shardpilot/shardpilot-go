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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	kindSceneUser = "synthetic-user"
	kindSceneAnon = "synthetic-anon"
)

// consentKindCapture records every consent body the scene server receives.
// failConsent answers consent writes with 503 until it is cleared.
type consentKindCapture struct {
	mu          sync.Mutex
	consents    []map[string]any
	failConsent bool
}

func (c *consentKindCapture) bodies() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.consents)
}

func (c *consentKindCapture) setFailConsent(fail bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failConsent = fail
}

func newConsentKindServer(t *testing.T) (*httptest.Server, *consentKindCapture) {
	t.Helper()
	capture := &consentKindCapture{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/v1/consent" {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"accepted":0,"rejected":0,"duplicates":0}`))
			return
		}
		var body map[string]any
		if err := json.NewDecoder(ingestRequestBody(t, r)).Decode(&body); err != nil {
			t.Errorf("decode consent request: %v", err)
		}
		capture.mu.Lock()
		capture.consents = append(capture.consents, body)
		fail := capture.failConsent
		capture.mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"recorded":true,"replayed":false}`))
	}))
	t.Cleanup(server.Close)
	return server, capture
}

func consentKindConfig(serverURL string, source Source, userID, anonymousID string, floor bool, spoolDir string) Config {
	cfg := Config{
		IngestURL:     serverURL,
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        source,
		UserID:        userID,
		AnonymousID:   anonymousID,
		BatchSize:     4,
		BufferSize:    16,
		FlushInterval: time.Hour,
		HTTPTimeout:   time.Second,
	}
	if source == SourceClient {
		cfg.PublishableKey = "sp_pub_fixture"
	} else {
		cfg.ServiceToken = "service-fixture"
	}
	if floor {
		cfg.ConsentFloor = &ConsentFloorConfig{}
		cfg.SpoolDir = spoolDir
	}
	return cfg
}

func closeConsentKindClient(t *testing.T, client *Client) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return client.Close(ctx)
}

// TestConsentActorKindMatrix drives the real NewClient → SetConsent(false) →
// Close path for every source, configured identity and floor posture, and
// reads the consent actor and its kind off the wire. A client source
// authenticates with a publishable key: its UserID is self-asserted, so the
// consent actor is the anonymous ID (kind anon), and a client with only a
// UserID has no admissible actor (local denial, no receipt). A server or
// backend source vouches for its user (kind user_verified) and falls back to
// the anonymous ID.
func TestConsentActorKindMatrix(t *testing.T) {
	identities := []struct{ name, user, anon string }{
		{"anonymous", "", kindSceneAnon},
		{"user", kindSceneUser, ""},
		{"both", kindSceneUser, kindSceneAnon},
	}
	for _, source := range []Source{SourceClient, SourceServer, SourceBackend} {
		for _, identity := range identities {
			for _, floor := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/floor=%t", source, identity.name, floor), func(t *testing.T) {
					wantActor, wantKind := kindSceneAnon, "anon"
					refused := false
					switch {
					case source == SourceClient && identity.anon == "":
						refused = true
					case source != SourceClient && identity.user != "":
						wantActor, wantKind = kindSceneUser, "user_verified"
					}
					server, capture := newConsentKindServer(t)
					client, err := NewClient(consentKindConfig(server.URL, source, identity.user, identity.anon, floor, t.TempDir()))
					if err != nil {
						t.Fatalf("NewClient: %v", err)
					}
					result, err := client.SetConsent(false)
					if err != nil {
						t.Fatalf("SetConsent(false): %v", err)
					}
					if err := closeConsentKindClient(t, client); err != nil {
						t.Fatalf("Close: %v", err)
					}
					if state := client.ConsentState(); state != ConsentDenied {
						t.Fatalf("state = %q, want the denial applied locally", state)
					}
					bodies := capture.bodies()
					if refused {
						if len(bodies) != 0 {
							t.Fatalf("a client source without an AnonymousID posted %d consent receipts: %v", len(bodies), bodies)
						}
						if !slices.Contains(result.Warnings, "consent_actor_invalid") {
							t.Fatalf("warnings = %v, want consent_actor_invalid", result.Warnings)
						}
						return
					}
					if len(bodies) != 1 {
						t.Fatalf("posted %d consent receipts, want 1: %v", len(bodies), bodies)
					}
					if got := bodies[0]["actor_identifier"]; got != wantActor {
						t.Errorf("actor_identifier = %v, want %q", got, wantActor)
					}
					if got := bodies[0]["kind"]; got != wantKind {
						t.Errorf("kind = %v, want %q", got, wantKind)
					}
				})
			}
		}
	}
}

// TestConsentActorKindClientUserOnlyGrantIsRefused: a client source with a
// UserID and no AnonymousID cannot grant. Nothing is applied or posted.
func TestConsentActorKindClientUserOnlyGrantIsRefused(t *testing.T) {
	for _, floor := range []bool{false, true} {
		t.Run(fmt.Sprintf("floor=%t", floor), func(t *testing.T) {
			server, capture := newConsentKindServer(t)
			client, err := NewClient(consentKindConfig(server.URL, SourceClient, kindSceneUser, "", floor, t.TempDir()))
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			if _, err := client.SetConsent(true); !errors.Is(err, ErrInvalidConsentIdentity) {
				t.Fatalf("SetConsent(true) = %v, want ErrInvalidConsentIdentity", err)
			}
			if state := client.ConsentState(); state != ConsentUnknown {
				t.Fatalf("state = %q after a refused grant, want unknown", state)
			}
			if err := closeConsentKindClient(t, client); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if bodies := capture.bodies(); len(bodies) != 0 {
				t.Fatalf("a refused grant posted %d consent receipts", len(bodies))
			}
		})
	}
}

// TestConsentActorKindSurvivesRetryAndRestart: the kind is fixed when the
// floor mints the receipt. It is persisted with the receipt and sent
// unchanged on every attempt, including after a restart on the same SpoolDir.
func TestConsentActorKindSurvivesRetryAndRestart(t *testing.T) {
	dir := t.TempDir()
	server, capture := newConsentKindServer(t)
	capture.setFailConsent(true)
	cfg := consentKindConfig(server.URL, SourceBackend, kindSceneUser, kindSceneAnon, true, dir)
	first, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, err := first.SetConsent(true); err != nil {
		t.Fatalf("SetConsent(true): %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(capture.bodies()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_ = first.Close(ctx)
	cancel()
	raw, err := os.ReadFile(filepath.Join(dir, consentOutboxFileName))
	if err != nil {
		t.Fatalf("read the retained outbox: %v", err)
	}
	if !strings.Contains(string(raw), `"kind":"user_verified"`) {
		t.Fatalf("the retained receipt does not carry its kind: %s", raw)
	}

	capture.setFailConsent(false)
	second, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient after restart: %v", err)
	}
	if err := closeConsentKindClient(t, second); err != nil {
		t.Fatalf("Close after restart: %v", err)
	}
	bodies := capture.bodies()
	if len(bodies) < 2 {
		t.Fatalf("the retained receipt was not retried after the restart: %d attempts", len(bodies))
	}
	key := bodies[0]["idempotency_key"]
	for i, body := range bodies {
		if body["idempotency_key"] != key || body["kind"] != "user_verified" || body["actor_identifier"] != kindSceneUser {
			t.Fatalf("attempt %d = %v, want the same receipt with kind user_verified", i, body)
		}
	}
}

// TestConsentReceiptKindIsRequiredAtLoad: a stored receipt keeps its kind
// only when it is one this SDK mints. A receipt without a kind, or with any
// other value, is malformed.
func TestConsentReceiptKindIsRequiredAtLoad(t *testing.T) {
	for kind, keep := range map[string]bool{
		`"kind":"anon",`:            true,
		`"kind":"user_verified",`:   true,
		``:                          false,
		`"kind":"",`:                false,
		`"kind":"user_unverified",`: false,
		`"kind":"ANON",`:            false,
	} {
		raw := `{` + kind + `"idempotency_key":"key","workspace_id":"w","app_id":"a","environment_id":"e","actor_identifier":"x","categories":{"analytics":true},"decided_at":"2026-07-19T00:00:00Z"}`
		var entry consentReceipt
		if err := json.Unmarshal([]byte(raw), &entry); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
		sanitized, ok := sanitizeConsentReceipt(entry)
		if ok != keep {
			t.Errorf("%s: kept=%t, want %t", raw, ok, keep)
			continue
		}
		if !ok {
			continue
		}
		encoded, err := json.Marshal(sanitized)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(encoded), strings.TrimSuffix(kind, ",")) {
			t.Errorf("sanitized receipt lost its kind: %s", encoded)
		}
	}
}

// TestConsentReceiptScopeIncludesKind: a retained receipt rules this
// client's floor only when it was minted for the same actor AND kind.
func TestConsentReceiptScopeIncludesKind(t *testing.T) {
	client := &Client{cfg: Config{
		WorkspaceID: "w", AppID: "a", EnvironmentID: "e",
		Source: SourceBackend, UserID: kindSceneUser, AnonymousID: kindSceneAnon,
	}}
	for kind, want := range map[string]bool{"user_verified": true, "anon": false} {
		raw := fmt.Sprintf(`{"kind":%q,"idempotency_key":"key","workspace_id":"w","app_id":"a","environment_id":"e","actor_identifier":%q,"anonymous_id":%q,"categories":{"analytics":true},"decided_at":"2026-07-19T00:00:00Z"}`, kind, kindSceneUser, kindSceneAnon)
		var receipt consentReceipt
		if err := json.Unmarshal([]byte(raw), &receipt); err != nil {
			t.Fatal(err)
		}
		if got := client.consentReceiptInScope(receipt); got != want {
			t.Errorf("kind %q: in scope = %t, want %t", kind, got, want)
		}
	}
}

// TestConsentRecordDigestFollowsTheSelectedActor: the persisted decision is
// scoped to the actor and kind it covers. The same configured IDs under a
// client source (the anonymous actor) and a backend source (the verified
// user) are different decisions; server and backend select the same actor.
func TestConsentRecordDigestFollowsTheSelectedActor(t *testing.T) {
	cfg := Config{WorkspaceID: "w", AppID: "a", EnvironmentID: "e", UserID: kindSceneUser, AnonymousID: kindSceneAnon}
	digest := func(source Source) string {
		scoped := cfg
		scoped.Source = source
		return consentActorDigest(scoped)
	}
	if digest(SourceClient) == digest(SourceBackend) {
		t.Error("a client-source record would rule the backend's verified-user scope")
	}
	if digest(SourceServer) != digest(SourceBackend) {
		t.Error("server and backend select the same actor but digest differently")
	}
}

// TestConsentFloorActorPairForClientSource: under the floor a client
// source's decision covers its anonymous ID. Overriding only the
// self-asserted UserID keeps the same consent actor; overriding the
// anonymous ID does not. A backend's decision covers its verified user.
func TestConsentFloorActorPairForClientSource(t *testing.T) {
	floor := &ConsentFloorConfig{}
	clientCfg := Config{Source: SourceClient, UserID: kindSceneUser, AnonymousID: kindSceneAnon, ConsentFloor: floor}
	backendCfg := Config{Source: SourceBackend, UserID: kindSceneUser, AnonymousID: kindSceneAnon, ConsentFloor: floor}
	cases := []struct {
		name     string
		cfg      Config
		event    Event
		mismatch bool
	}{
		{"client user override", clientCfg, Event{UserID: "another-user"}, false},
		{"client anonymous override", clientCfg, Event{AnonymousID: "another-anon"}, true},
		{"backend user override", backendCfg, Event{UserID: "another-user"}, true},
		{"backend anonymous override", backendCfg, Event{AnonymousID: "another-anon"}, false},
	}
	for _, tc := range cases {
		client := &Client{cfg: tc.cfg}
		if got := client.consentFloorActorMismatch(tc.event); got != tc.mismatch {
			t.Errorf("%s: mismatch = %t, want %t", tc.name, got, tc.mismatch)
		}
		envelope := eventEnvelope{
			UserID:      firstNonEmpty(tc.event.UserID, tc.cfg.UserID),
			AnonymousID: firstNonEmpty(tc.event.AnonymousID, tc.cfg.AnonymousID),
		}
		if got := client.spoolActorEligible(envelope); got == tc.mismatch {
			t.Errorf("%s: spool eligible = %t, want %t", tc.name, got, !tc.mismatch)
		}
	}
	// The SDK's own experiment facts ride the anonymous ID alone: a client
	// source's anonymous-scoped decision covers them, a backend's
	// user-scoped decision does not.
	fact := eventEnvelope{AnonymousID: kindSceneAnon, internalIdentityFact: true}
	if !(&Client{cfg: clientCfg}).spoolActorEligible(fact) {
		t.Error("a client source's experiment fact was refused retention under its own anonymous-scoped decision")
	}
	if (&Client{cfg: backendCfg}).spoolActorEligible(fact) {
		t.Error("a backend's experiment fact was retained under a user-scoped decision")
	}
}

// TestConsentFloorExperimentFactIntakeFollowsTheActor: the fact intake
// refuses an anonymous-only fact only when the floor's decision covers a
// verified user.
func TestConsentFloorExperimentFactIntakeFollowsTheActor(t *testing.T) {
	for _, tc := range []struct {
		source   Source
		mismatch bool
	}{{SourceClient, false}, {SourceBackend, true}} {
		t.Run(string(tc.source), func(t *testing.T) {
			server, _ := newConsentKindServer(t)
			client, err := NewClient(consentKindConfig(server.URL, tc.source, kindSceneUser, kindSceneAnon, true, t.TempDir()))
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			defer func() { _ = closeConsentKindClient(t, client) }()
			if _, err := client.SetConsent(true); err != nil {
				t.Fatalf("SetConsent(true): %v", err)
			}
			_, err = client.enqueueExperimentFact(Event{Name: "experiment_exposure", omitUserID: true}, false)
			if got := errors.Is(err, ErrConsentActorMismatch); got != tc.mismatch {
				t.Fatalf("fact intake error = %v, want mismatch=%t", err, tc.mismatch)
			}
		})
	}
}
