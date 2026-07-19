package shardpilot

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConsentRecordRoundTrip(t *testing.T) {
	dir := t.TempDir()
	digest := spoolTestActorDigest()

	if _, ok := loadConsentRecord(dir, digest); ok {
		t.Fatalf("expected no decision from an absent record")
	}
	if err := saveConsentRecord(dir, ConsentDecisionGranted, digest, os.Rename, os.Chmod); err != nil {
		t.Fatalf("save granted: %v", err)
	}
	if state, ok := loadConsentRecord(dir, digest); !ok || state != ConsentGranted {
		t.Fatalf("expected a granted record, got %v %v", state, ok)
	}
	if err := saveConsentRecord(dir, ConsentDecisionDenied, digest, os.Rename, os.Chmod); err != nil {
		t.Fatalf("save denied: %v", err)
	}
	if state, ok := loadConsentRecord(dir, digest); !ok || state != ConsentDenied {
		t.Fatalf("expected a denied record, got %v %v", state, ok)
	}
	if err := saveConsentRecord(dir, ConsentDecisionDeniedForcedMinor, digest, os.Rename, os.Chmod); err != nil {
		t.Fatalf("save forced-minor: %v", err)
	}
	if state, ok := loadConsentRecord(dir, digest); !ok || state != ConsentDeniedForcedMinor {
		t.Fatalf("expected a forced-minor record, got %v %v", state, ok)
	}

	// A record written for a DIFFERENT actor/scope tuple is no decision for
	// this one: a reused state dir never lets one actor's decision authorize
	// another's disk participation.
	otherDigest := consentActorDigest(Config{
		WorkspaceID:   "workspace-test",
		EnvironmentID: "develop",
		AnonymousID:   "anon-other",
	})
	if _, ok := loadConsentRecord(dir, otherDigest); ok {
		t.Fatalf("expected no decision for a different actor digest")
	}

	// Unreadable and unknown-valued records are no decision at all — the
	// spool fails toward purging, never toward loading.
	if err := os.WriteFile(consentRecordPath(dir), []byte("not json"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, ok := loadConsentRecord(dir, digest); ok {
		t.Fatalf("expected no decision from an unreadable record")
	}
	if err := os.WriteFile(consentRecordPath(dir), []byte(`{"consent_analytics":"maybe","actor_digest":"`+digest+`"}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, ok := loadConsentRecord(dir, digest); ok {
		t.Fatalf("expected no decision from an unknown value")
	}
}

func TestConsentActorDigestInjective(t *testing.T) {
	base := Config{WorkspaceID: "ws", EnvironmentID: "env", UserID: "u", AnonymousID: "a"}
	digest := consentActorDigest(base)
	if len(digest) != 64 {
		t.Fatalf("expected a sha256 hex digest, got %q", digest)
	}
	// Every tuple field participates, and shifting bytes across field
	// boundaries changes the digest (length-prefixed hashing).
	variants := []Config{
		{WorkspaceID: "wsX", EnvironmentID: "env", UserID: "u", AnonymousID: "a"},
		{WorkspaceID: "ws", EnvironmentID: "envX", UserID: "u", AnonymousID: "a"},
		{WorkspaceID: "ws", EnvironmentID: "env", UserID: "uX", AnonymousID: "a"},
		{WorkspaceID: "ws", EnvironmentID: "env", UserID: "u", AnonymousID: "aX"},
		{WorkspaceID: "wse", EnvironmentID: "nv", UserID: "u", AnonymousID: "a"},
	}
	for _, variant := range variants {
		if consentActorDigest(variant) == digest {
			t.Fatalf("expected a distinct digest for %+v", variant)
		}
	}
	if consentActorDigest(base) != digest {
		t.Fatalf("expected a deterministic digest")
	}
}

func TestConsentRecordWrittenAtomicallyWithPrivateModes(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusAccepted, "", "")

	dir := t.TempDir()
	client := newSpoolTestClient(t, server.URL, dir, nil, nil)
	defer client.Close(context.Background())

	client.SetConsent(true)
	path := consentRecordPath(dir)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("expected consent.json written on SetConsent: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected a 0600 record, got %v", info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := `{"consent_analytics":"granted","actor_digest":"` + spoolTestActorDigest() + `"}`
	if string(data) != want {
		t.Fatalf("unexpected record content %q (want %q)", data, want)
	}
	// The record carries a fixed-size digest, never the verbatim identity.
	if strings.Contains(string(data), "anon-spool-1") {
		t.Fatalf("the consent record must not contain plaintext identifiers: %q", data)
	}
	// No stray temp files linger from the atomic write.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, entry := range entries {
		if entry.Name() != consentRecordFileName {
			t.Fatalf("unexpected file %q in the state dir", entry.Name())
		}
	}
}

func TestWipeOwedMarkerHelpers(t *testing.T) {
	dir := t.TempDir()
	if wipeOwedMarkerExists(dir) {
		t.Fatalf("expected no marker in a fresh dir")
	}
	if err := createWipeOwedMarker(dir); err != nil {
		t.Fatalf("create: %v", err)
	}
	if !wipeOwedMarkerExists(dir) {
		t.Fatalf("expected the marker to exist")
	}
	// Creation is idempotent.
	if err := createWipeOwedMarker(dir); err != nil {
		t.Fatalf("re-create: %v", err)
	}
	if err := removeWipeOwedMarker(dir); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if wipeOwedMarkerExists(dir) {
		t.Fatalf("expected the marker removed")
	}
	// Removing an absent marker is success.
	if err := removeWipeOwedMarker(dir); err != nil {
		t.Fatalf("remove absent: %v", err)
	}
}

func TestRemoteConfigCachePathAloneNeverPersistsConsent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/consent":
			_, _ = w.Write([]byte(`{"recorded":true,"replayed":false}`))
		default:
			_, _ = w.Write([]byte(`{"values":{}}`))
		}
	}))
	defer server.Close()

	dir := t.TempDir()
	client, err := NewClient(Config{
		IngestURL:             server.URL,
		Token:                 "test-token",
		WorkspaceID:           "workspace-test",
		AppID:                 "app-test",
		EnvironmentID:         "develop",
		Source:                SourceBackend,
		AnonymousID:           "anon-rc-1",
		APIKey:                "test-rc-key",
		RemoteConfigURL:       server.URL,
		RemoteConfigCachePath: filepath.Join(dir, "rc-cache.json"),
		FlushInterval:         time.Hour,
		HTTPTimeout:           time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	client.SetConsent(true)
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("FetchRemoteConfig: %v", err)
	}

	// The cache path enables ONLY the remote-config cache: no consent
	// record, no spool, no marker — consent persistence is SpoolDir's.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, entry := range entries {
		if entry.Name() != "rc-cache.json" {
			t.Fatalf("unexpected file %q — RemoteConfigCachePath must not enable consent persistence", entry.Name())
		}
	}
}
