package shardpilot

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// A self-contained mirror of the ShardPilot Mode-B verifier's
// hard-reject contract. It exists ONLY in the test so the round-trip proves a
// minted token would actually pass the server's 16-check contract, without the
// SDK taking a dependency on the server or a JWT library. Keep these defaults
// in sync with internal/config DefaultIngestClientJWT* and the verifier in
// internal/ingestauth/modeb_jwt.go.
// ---------------------------------------------------------------------------

const (
	verifierMaxIatAge   = 5 * time.Minute
	verifierMaxLifetime = 15 * time.Minute
	verifierClockSkew   = 5 * time.Second
)

type parsedToken struct {
	header  map[string]any
	payload map[string]any
}

// hs256Verify parses a compact JWS, asserts the alg-allowlist and a valid
// HMAC-SHA256 signature under the given secret, and returns the decoded header
// and payload. It is the test's stand-in for the server's signed parse.
func hs256Verify(t *testing.T, token string, secret []byte) parsedToken {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token must have 3 dot-separated parts, got %d", len(parts))
	}

	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	var header map[string]any
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		t.Fatalf("unmarshal header: %v", err)
	}

	// alg-allowlist: HS256 is the ONLY accepted alg.
	if alg, _ := header["alg"].(string); alg != "HS256" {
		t.Fatalf("alg must be HS256, got %q", header["alg"])
	}

	// Verify the signature (HMAC-SHA256 over "header.payload").
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		t.Fatalf("signature does not verify under the provided secret")
	}

	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	return parsedToken{header: header, payload: payload}
}

// assertVerifierContract mirrors the modeb_jwt.go hard-reject checks (steps 4
// and 5) so a token that passes here would pass the real server.
func assertVerifierContract(t *testing.T, pt parsedToken, now time.Time, issuer, audience, subject string) {
	t.Helper()

	// kid header present and non-empty.
	if kid, _ := pt.header["kid"].(string); strings.TrimSpace(kid) == "" {
		t.Fatalf("kid header must be present and non-empty")
	}

	// exp present + not expired; iat present + not in the future.
	expF, okExp := pt.payload["exp"].(float64)
	iatF, okIat := pt.payload["iat"].(float64)
	if !okExp {
		t.Fatalf("exp claim required")
	}
	if !okIat {
		t.Fatalf("iat claim required")
	}
	exp := time.Unix(int64(expF), 0).UTC()
	iat := time.Unix(int64(iatF), 0).UTC()

	if !now.Before(exp.Add(verifierClockSkew)) {
		t.Fatalf("token already expired: exp=%s now=%s", exp, now)
	}
	if iat.After(now.Add(verifierClockSkew)) {
		t.Fatalf("iat is in the future: iat=%s now=%s", iat, now)
	}
	// iat-age window: now - iat <= MaxIatAge (+skew).
	if now.Sub(iat) > verifierMaxIatAge+verifierClockSkew {
		t.Fatalf("iat too old: now-iat=%s > MaxIatAge=%s", now.Sub(iat), verifierMaxIatAge)
	}
	// max-lifetime cap: exp - iat <= MaxLifetime.
	if exp.Sub(iat) > verifierMaxLifetime {
		t.Fatalf("lifetime exceeds MaxLifetime: exp-iat=%s > %s", exp.Sub(iat), verifierMaxLifetime)
	}

	// iss / aud must match config.
	if got, _ := pt.payload["iss"].(string); got != issuer {
		t.Fatalf("iss=%q, want %q", got, issuer)
	}
	if got, _ := pt.payload["aud"].(string); got != audience {
		t.Fatalf("aud=%q, want %q", got, audience)
	}

	// scope must be EXACTLY analytics:ingest.
	if got, _ := pt.payload["scope"].(string); got != "analytics:ingest" {
		t.Fatalf("scope=%q, want analytics:ingest", got)
	}

	// sub non-empty, within IngestSubjectMaxLength, and equal to the expected.
	sub, _ := pt.payload["sub"].(string)
	if strings.TrimSpace(sub) == "" {
		t.Fatalf("sub must be non-empty")
	}
	if len(sub) > IngestSubjectMaxLength {
		t.Fatalf("sub exceeds %d bytes", IngestSubjectMaxLength)
	}
	if sub != subject {
		t.Fatalf("sub=%q, want %q", sub, subject)
	}

	// workspace_id / app_id / environment_id non-empty.
	for _, key := range []string{"workspace_id", "app_id", "environment_id"} {
		if v, _ := pt.payload[key].(string); strings.TrimSpace(v) == "" {
			t.Fatalf("%s must be non-empty", key)
		}
	}
}

// fixedClock returns a Clock pinned to t.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// ---------------------------------------------------------------------------
// Round-trip: mint -> hand-verify -> assert every claim + the server contract.
// ---------------------------------------------------------------------------

func TestSignIngestJWT_RoundTrip(t *testing.T) {
	secret := []byte("per-tenant-secret-bytes-32-bytes-long!!")
	key := SigningKey{KID: "kid-2026-06.v1", Secret: secret}
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)

	claims := IngestJWTClaims{
		Subject:       "user-abc-123",
		WorkspaceID:   "ws-1",
		AppID:         "app-9",
		EnvironmentID: "env-develop",
		BindAnon:      "anon-device-777",
	}

	token, err := SignIngestJWT(key, claims, WithIngestClock(fixedClock{now}))
	if err != nil {
		t.Fatalf("SignIngestJWT: %v", err)
	}

	pt := hs256Verify(t, token, secret)

	// Header: alg + typ + kid.
	if pt.header["alg"] != "HS256" {
		t.Errorf("alg=%v, want HS256", pt.header["alg"])
	}
	if pt.header["typ"] != "JWT" {
		t.Errorf("typ=%v, want JWT", pt.header["typ"])
	}
	if pt.header["kid"] != key.KID {
		t.Errorf("kid=%v, want %q", pt.header["kid"], key.KID)
	}

	// Every claim.
	if pt.payload["sub"] != claims.Subject {
		t.Errorf("sub=%v, want %q", pt.payload["sub"], claims.Subject)
	}
	if pt.payload["workspace_id"] != claims.WorkspaceID {
		t.Errorf("workspace_id=%v", pt.payload["workspace_id"])
	}
	if pt.payload["app_id"] != claims.AppID {
		t.Errorf("app_id=%v", pt.payload["app_id"])
	}
	if pt.payload["environment_id"] != claims.EnvironmentID {
		t.Errorf("environment_id=%v", pt.payload["environment_id"])
	}
	if pt.payload["scope"] != "analytics:ingest" {
		t.Errorf("scope=%v, want analytics:ingest", pt.payload["scope"])
	}
	if pt.payload["iss"] != DefaultIngestIssuer {
		t.Errorf("iss=%v, want %q", pt.payload["iss"], DefaultIngestIssuer)
	}
	if pt.payload["aud"] != DefaultIngestAudience {
		t.Errorf("aud=%v, want %q", pt.payload["aud"], DefaultIngestAudience)
	}
	if pt.payload["bind_anon"] != claims.BindAnon {
		t.Errorf("bind_anon=%v, want %q", pt.payload["bind_anon"], claims.BindAnon)
	}

	// exp - iat is the default 5m (= the server MaxIatAge window), within MaxLifetime.
	iat := int64(pt.payload["iat"].(float64))
	exp := int64(pt.payload["exp"].(float64))
	if iat != now.Unix() {
		t.Errorf("iat=%d, want %d", iat, now.Unix())
	}
	if got := time.Duration(exp-iat) * time.Second; got != DefaultIngestLifetime {
		t.Errorf("lifetime=%s, want %s", got, DefaultIngestLifetime)
	}

	// The full server contract: verified "now" is the mint instant.
	assertVerifierContract(t, pt, now, DefaultIngestIssuer, DefaultIngestAudience, claims.Subject)
}

func TestSignIngestJWT_BindAnonOmittedWhenEmpty(t *testing.T) {
	secret := []byte("another-tenant-secret-value-here-1234567")
	key := SigningKey{KID: "kidB", Secret: secret}
	now := time.Date(2026, 6, 18, 9, 30, 0, 0, time.UTC)

	token, err := SignIngestJWT(key, IngestJWTClaims{
		Subject:       "u1",
		WorkspaceID:   "ws",
		AppID:         "app",
		EnvironmentID: "env",
		// BindAnon intentionally empty.
	}, WithIngestNow(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("SignIngestJWT: %v", err)
	}

	pt := hs256Verify(t, token, secret)
	if _, present := pt.payload["bind_anon"]; present {
		t.Errorf("bind_anon must be omitted when empty, got %v", pt.payload["bind_anon"])
	}
	assertVerifierContract(t, pt, now, DefaultIngestIssuer, DefaultIngestAudience, "u1")
}

func TestSignIngestJWT_CustomIssuerAudienceLifetime(t *testing.T) {
	secret := []byte("custom-issuer-secret-bytes-aaaaaaaaaaaa")
	key := SigningKey{KID: "kidC", Secret: secret}
	now := time.Date(2026, 6, 18, 0, 0, 0, 0, time.UTC)

	token, err := SignIngestJWT(key, IngestJWTClaims{
		Subject:       "u2",
		WorkspaceID:   "ws2",
		AppID:         "app2",
		EnvironmentID: "env2",
	},
		WithIngestIssuer("my-game-backend"),
		WithIngestAudience("analytics-service"),
		WithIngestLifetime(5*time.Minute),
		WithIngestNow(func() time.Time { return now }),
	)
	if err != nil {
		t.Fatalf("SignIngestJWT: %v", err)
	}

	pt := hs256Verify(t, token, secret)
	if pt.payload["iss"] != "my-game-backend" {
		t.Errorf("iss=%v", pt.payload["iss"])
	}
	iat := int64(pt.payload["iat"].(float64))
	exp := int64(pt.payload["exp"].(float64))
	if got := time.Duration(exp-iat) * time.Second; got != 5*time.Minute {
		t.Errorf("lifetime=%s, want 5m", got)
	}
	assertVerifierContract(t, pt, now, "my-game-backend", "analytics-service", "u2")
}

// ---------------------------------------------------------------------------
// Negatives.
// ---------------------------------------------------------------------------

func TestSignIngestJWT_Negatives(t *testing.T) {
	goodKey := SigningKey{KID: "kid1", Secret: []byte("secret-secret-secret-secret-secret-12")}
	goodClaims := IngestJWTClaims{
		Subject:       "u",
		WorkspaceID:   "ws",
		AppID:         "app",
		EnvironmentID: "env",
	}
	longID := strings.Repeat("x", IngestSubjectMaxLength+1)

	cases := []struct {
		name    string
		key     SigningKey
		claims  IngestJWTClaims
		opts    []MintOption
		wantErr error
	}{
		{
			name:    "empty secret",
			key:     SigningKey{KID: "kid1", Secret: nil},
			claims:  goodClaims,
			wantErr: ErrInvalidSigningKey,
		},
		{
			name:    "all-zero secret",
			key:     SigningKey{KID: "kid1", Secret: make([]byte, 32)},
			claims:  goodClaims,
			wantErr: ErrInvalidSigningKey,
		},
		{
			name:    "empty kid",
			key:     SigningKey{KID: "   ", Secret: goodKey.Secret},
			claims:  goodClaims,
			wantErr: ErrInvalidSigningKey,
		},
		{
			name:    "kid bad charset",
			key:     SigningKey{KID: "kid/with/slash", Secret: goodKey.Secret},
			claims:  goodClaims,
			wantErr: ErrInvalidSigningKey,
		},
		{
			name:    "kid too long",
			key:     SigningKey{KID: strings.Repeat("k", ingestKidMaxLength+1), Secret: goodKey.Secret},
			claims:  goodClaims,
			wantErr: ErrInvalidSigningKey,
		},
		{
			name:    "empty subject",
			key:     goodKey,
			claims:  IngestJWTClaims{Subject: "  ", WorkspaceID: "ws", AppID: "app", EnvironmentID: "env"},
			wantErr: ErrInvalidIngestClaims,
		},
		{
			name:    "subject too long",
			key:     goodKey,
			claims:  IngestJWTClaims{Subject: longID, WorkspaceID: "ws", AppID: "app", EnvironmentID: "env"},
			wantErr: ErrInvalidIngestClaims,
		},
		{
			name:    "empty workspace",
			key:     goodKey,
			claims:  IngestJWTClaims{Subject: "u", WorkspaceID: "", AppID: "app", EnvironmentID: "env"},
			wantErr: ErrInvalidIngestClaims,
		},
		{
			name:    "empty app",
			key:     goodKey,
			claims:  IngestJWTClaims{Subject: "u", WorkspaceID: "ws", AppID: "", EnvironmentID: "env"},
			wantErr: ErrInvalidIngestClaims,
		},
		{
			name:    "empty environment",
			key:     goodKey,
			claims:  IngestJWTClaims{Subject: "u", WorkspaceID: "ws", AppID: "app", EnvironmentID: ""},
			wantErr: ErrInvalidIngestClaims,
		},
		{
			name:    "bind_anon too long",
			key:     goodKey,
			claims:  IngestJWTClaims{Subject: "u", WorkspaceID: "ws", AppID: "app", EnvironmentID: "env", BindAnon: longID},
			wantErr: ErrInvalidIngestClaims,
		},
		{
			name:    "lifetime zero",
			key:     goodKey,
			claims:  goodClaims,
			opts:    []MintOption{WithIngestLifetime(0)},
			wantErr: ErrInvalidMintOption,
		},
		{
			name:    "lifetime negative",
			key:     goodKey,
			claims:  goodClaims,
			opts:    []MintOption{WithIngestLifetime(-time.Second)},
			wantErr: ErrInvalidMintOption,
		},
		{
			name:    "lifetime over server max",
			key:     goodKey,
			claims:  goodClaims,
			opts:    []MintOption{WithIngestLifetime(maxIngestLifetime + time.Second)},
			wantErr: ErrInvalidMintOption,
		},
		{
			name:    "empty issuer override",
			key:     goodKey,
			claims:  goodClaims,
			opts:    []MintOption{WithIngestIssuer("  ")},
			wantErr: ErrInvalidMintOption,
		},
		{
			name:    "empty audience override",
			key:     goodKey,
			claims:  goodClaims,
			opts:    []MintOption{WithIngestAudience("")},
			wantErr: ErrInvalidMintOption,
		},
		{
			name:    "nil now",
			key:     goodKey,
			claims:  goodClaims,
			opts:    []MintOption{WithIngestNow(nil)},
			wantErr: ErrInvalidMintOption,
		},
		{
			name:    "nil clock",
			key:     goodKey,
			claims:  goodClaims,
			opts:    []MintOption{WithIngestClock(nil)},
			wantErr: ErrInvalidMintOption,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			token, err := SignIngestJWT(tc.key, tc.claims, tc.opts...)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err=%v, want errors.Is(%v)", err, tc.wantErr)
			}
			if token != "" {
				t.Errorf("token must be empty on error, got %q", token)
			}
		})
	}
}

// Subject and bind_anon must be signed RAW (byte-for-byte) so they match the
// raw user_id/anonymous_id the event path sends; tenant-scope ids are trimmed.
func TestSignIngestJWT_SubjectAndBindAnonSignedRaw(t *testing.T) {
	secret := []byte("raw-preserve-secret-value-0987654321ab")
	key := SigningKey{KID: "kid", Secret: secret}
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	claims := IngestJWTClaims{
		Subject:       " user-with-spaces ",
		WorkspaceID:   "  ws  ",
		AppID:         "app",
		EnvironmentID: "env",
		BindAnon:      " anon-padded ",
	}
	token, err := SignIngestJWT(key, claims, WithIngestNow(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("SignIngestJWT: %v", err)
	}
	pt := hs256Verify(t, token, secret)
	if pt.payload["sub"] != " user-with-spaces " {
		t.Errorf("sub=%q, want raw %q (trimming would authorize a different identity)", pt.payload["sub"], " user-with-spaces ")
	}
	if pt.payload["bind_anon"] != " anon-padded " {
		t.Errorf("bind_anon=%q, want raw %q (trimming would stitch a different anon)", pt.payload["bind_anon"], " anon-padded ")
	}
	// Tenant-scope identifiers ARE canonicalized (trimmed) — they match the
	// resolved whitespace-free scope, not a per-event byte value.
	if pt.payload["workspace_id"] != "ws" {
		t.Errorf("workspace_id=%q, want trimmed %q", pt.payload["workspace_id"], "ws")
	}
}

// ZeroSecret must leave the key unable to mint: the original (detached) fails
// the empty-secret guard, and a by-value copy taken before wiping (whose backing
// array is now all-zero) fails the all-zero guard.
func TestSignIngestJWT_WipedKeyCannotMint(t *testing.T) {
	claims := IngestJWTClaims{Subject: "u", WorkspaceID: "ws", AppID: "app", EnvironmentID: "env"}
	key := SigningKey{KID: "kid", Secret: []byte("a-real-tenant-secret-value-1234567890")}
	copyOfKey := key // shares the same backing array

	key.ZeroSecret()

	if key.Secret != nil {
		t.Errorf("ZeroSecret must detach the slice, got len=%d", len(key.Secret))
	}
	if _, err := SignIngestJWT(key, claims); !errors.Is(err, ErrInvalidSigningKey) {
		t.Fatalf("wiped key must not mint, err=%v", err)
	}
	for i, b := range copyOfKey.Secret {
		if b != 0 {
			t.Fatalf("ZeroSecret must scrub the shared backing array, byte %d = %d", i, b)
		}
	}
	if _, err := SignIngestJWT(copyOfKey, claims); !errors.Is(err, ErrInvalidSigningKey) {
		t.Fatalf("a copy of a wiped key (all-zero backing) must not mint, err=%v", err)
	}
}

// A SigningKey must redact its secret when formatted, so a stray %v/%s/%#v log
// line cannot leak the per-tenant HMAC secret.
func TestSigningKeyRedactsSecretWhenFormatted(t *testing.T) {
	secret := []byte("super-secret-tenant-hmac-value-abcdef")
	key := SigningKey{KID: "kid-redact", Secret: secret}
	for _, verb := range []string{"%v", "%+v", "%s", "%#v"} {
		out := fmt.Sprintf(verb, key)
		if strings.Contains(out, string(secret)) {
			t.Errorf("%s leaked the secret: %q", verb, out)
		}
		if !strings.Contains(out, "kid-redact") {
			t.Errorf("%s should still show the kid: %q", verb, out)
		}
		if !strings.Contains(out, "REDACTED") {
			t.Errorf("%s should mark the secret redacted: %q", verb, out)
		}
	}
	// A pointer to the key formats the same way (value receiver is in *T's set).
	if strings.Contains(fmt.Sprintf("%v", &key), string(secret)) {
		t.Error("formatting a *SigningKey leaked the secret")
	}
}

// The default lifetime must stay within the server's iat-age window, or cached
// tokens would fail ingest before their advertised exp (independent of exp).
func TestDefaultIngestLifetimeWithinIatAgeWindow(t *testing.T) {
	if DefaultIngestLifetime > verifierMaxIatAge {
		t.Fatalf("DefaultIngestLifetime=%s exceeds the verifier MaxIatAge=%s; cached tokens would fail ingest before exp", DefaultIngestLifetime, verifierMaxIatAge)
	}
}

// lifetime exactly at the server cap is accepted.
func TestSignIngestJWT_LifetimeAtCapAccepted(t *testing.T) {
	key := SigningKey{KID: "kid", Secret: []byte("secret-value-secret-value-secret-valu")}
	_, err := SignIngestJWT(key, IngestJWTClaims{
		Subject: "u", WorkspaceID: "ws", AppID: "app", EnvironmentID: "env",
	}, WithIngestLifetime(maxIngestLifetime))
	if err != nil {
		t.Fatalf("lifetime exactly at the cap must be accepted, got %v", err)
	}
}

// A token signed with secret A must NOT verify under secret B (tamper/secret
// confusion guard).
func TestSignIngestJWT_WrongSecretFailsVerify(t *testing.T) {
	secretA := []byte("secret-A-secret-A-secret-A-secret-A-12")
	secretB := []byte("secret-B-secret-B-secret-B-secret-B-99")
	token, err := SignIngestJWT(SigningKey{KID: "kid", Secret: secretA}, IngestJWTClaims{
		Subject: "u", WorkspaceID: "ws", AppID: "app", EnvironmentID: "env",
	})
	if err != nil {
		t.Fatalf("SignIngestJWT: %v", err)
	}
	parts := strings.Split(token, ".")
	sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
	mac := hmac.New(sha256.New, secretB)
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	if hmac.Equal(sig, mac.Sum(nil)) {
		t.Fatalf("token must not verify under a different secret")
	}
}

// ZeroSecret wipes the secret bytes in place.
func TestSigningKey_ZeroSecret(t *testing.T) {
	secret := []byte{1, 2, 3, 4, 5}
	key := SigningKey{KID: "kid", Secret: secret}
	key.ZeroSecret()
	for i, b := range secret {
		if b != 0 {
			t.Fatalf("byte %d not zeroed: %d", i, b)
		}
	}
	// Safe on a zero-value key.
	(&SigningKey{}).ZeroSecret()
}

// iat is stamped to "now", so the server's MaxIatAge window starts fresh
// (now - iat == 0).
func TestSignIngestJWT_FreshIat(t *testing.T) {
	key := SigningKey{KID: "kid", Secret: []byte("freshness-secret-freshness-secret-1234")}
	now := time.Date(2026, 6, 18, 6, 0, 0, 0, time.UTC)
	token, err := SignIngestJWT(key, IngestJWTClaims{
		Subject: "u", WorkspaceID: "ws", AppID: "app", EnvironmentID: "env",
	}, WithIngestNow(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("SignIngestJWT: %v", err)
	}
	pt := hs256Verify(t, token, key.Secret)
	if iat := int64(pt.payload["iat"].(float64)); iat != now.Unix() {
		t.Fatalf("iat=%d, want %d (fresh)", iat, now.Unix())
	}
}
