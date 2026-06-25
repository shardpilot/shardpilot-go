package shardpilot

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ===========================================================================
// Mode-B per-tenant ingest JWT mint helper.
//
// SECURITY — BACKEND-ONLY. THIS HELPER HOLDS THE PER-TENANT SIGNING SECRET.
//
// This is the ONE legitimate holder of the per-tenant HS256 ingest signing
// secret, and it is server-side only. It belongs in a studio's trusted Go
// game-backend, NEVER in a shipped client binary. A client SDK (Unity, Unreal,
// Defold) consumes a minted token through a token_provider callback and never
// sees the secret; this helper is the inverse — it MINTS the token the client
// then fetches over the studio's own authenticated channel.
//
// The per-tenant HS256 secret is minted, rotated, and served by ShardPilot.
// The backend obtains it out-of-band (e.g. over ShardPilot's machine-to-machine
// serve channel) together with its key id (kid). This helper does not fetch, store,
// or rotate the secret — it only signs a conformant short-lived JWT with it.
//
// NEVER ship the secret in a client. NEVER log the secret or a minted token.
// ===========================================================================

// IngestSubjectMaxLength is the maximum byte length of the JWT subject (the
// verified user_id) and of the optional bound anonymous_id. It mirrors the
// ShardPilot ingest API's canonical identifier upper bound (512
// bytes): the server back-fills the verified subject onto events that omit a
// user_id, and stamps a verified alias edge from bind_anon, both AFTER its own
// per-event length guard runs, so a subject/bind_anon longer than this is
// rejected at verify (and would later be un-erasable). Rejecting it here at the
// mint source means a token this helper emits can never be rejected for an
// over-long subject or anon downstream.
const IngestSubjectMaxLength = 512

// ingestKidMaxLength mirrors the ShardPilot ingest API's max kid length
// (256): a kid is a short opaque id. A kid longer than this — or one carrying a
// character outside the resolver charset [A-Za-z0-9_.-] — is rejected by the
// Mode-B verifier before it ever resolves the secret, so reject it here too.
const ingestKidMaxLength = 256

// ingestScope is the fixed, non-overridable scope of every token this helper
// mints. The Mode-B verifier requires the scope claim to be EXACTLY this value;
// the helper never lets a caller change it.
const ingestScope = "analytics:ingest"

const (
	// DefaultIngestIssuer is the issuer the ShardPilot Mode-B verifier
	// expects by default. Override per
	// deployment with WithIngestIssuer when the server is configured otherwise.
	DefaultIngestIssuer = "shardpilot"

	// DefaultIngestAudience is the audience the ShardPilot Mode-B verifier
	// expects by default. Override with
	// WithIngestAudience when the server is configured otherwise.
	DefaultIngestAudience = "shardpilot-ingest"

	// DefaultIngestLifetime is the default token validity window (exp - iat). It
	// equals the server's 5m MaxIatAge window so the advertised exp is actually
	// reachable. The verifier enforces iat-age independently of exp: it rejects
	// any token more than MaxIatAge past its iat REGARDLESS of exp, so a longer
	// default (e.g. 10m) would advertise validity the server will not honour —
	// cached/retried tokens would start failing ingest at ~5m even though they
	// are not yet "expired". 5m sits well under the 15m MaxLifetime cap, leaving
	// the iat-age window as the binding bound and exp honest about it.
	DefaultIngestLifetime = 5 * time.Minute

	// maxIngestLifetime is the hard cap on a minted token's lifetime. It mirrors
	// the ShardPilot server's max client-JWT lifetime (15m): a token whose
	// exp - iat exceeds this is rejected at verify even though it is not yet
	// expired. The helper rejects an over-long WithIngestLifetime at the mint
	// source so a token it emits is never rejected downstream for lifetime.
	maxIngestLifetime = 15 * time.Minute
)

// kidCharset reports whether the kid uses only the resolver-permitted charset
// [A-Za-z0-9_.-]. It mirrors the ShardPilot ingest API's kid pattern. A
// hand-written charset check keeps this file dependency-free.
func validIngestKid(kid string) bool {
	if kid == "" || len(kid) > ingestKidMaxLength {
		return false
	}
	for i := 0; i < len(kid); i++ {
		c := kid[i]
		switch {
		case c >= 'A' && c <= 'Z':
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '_' || c == '.' || c == '-':
		default:
			return false
		}
	}
	return true
}

// ErrInvalidSigningKey is returned when the supplied SigningKey is unusable
// (empty kid, kid charset/length violation, or an empty secret).
var ErrInvalidSigningKey = errors.New("invalid shardpilot signing key")

// ErrInvalidIngestClaims is returned when the supplied IngestJWTClaims are
// invalid for minting (empty or over-long subject/bind_anon, or an empty
// required tenant-scope field).
var ErrInvalidIngestClaims = errors.New("invalid shardpilot ingest claims")

// ErrInvalidMintOption is returned when a MintOption carries an unusable value
// (e.g. a non-positive lifetime, a lifetime over the server cap, or an empty
// issuer/audience override).
var ErrInvalidMintOption = errors.New("invalid shardpilot mint option")

// SigningKey is a per-tenant HS256 ingest signing key obtained out-of-band from
// ShardPilot. KID is the key's stable opaque id, stamped into the JWT header
// so the verifier can resolve the matching secret. Secret holds the RAW secret
// bytes (already base64url-decoded if the caller received it base64url-encoded
// on the wire) and is HELD ONLY in a trusted server-side process.
//
// SigningKey implements a redacting String/GoString, so logging a key (or a
// struct that embeds one) with %v/%+v/%s/%#v never emits the secret — only the
// kid and the secret's byte length. The Secret field itself MUST still never be
// logged directly (e.g. fmt.Sprintf("%s", key.Secret) prints the raw bytes). Use
// ZeroSecret to wipe a copy once a key is no longer needed.
type SigningKey struct {
	KID    string
	Secret []byte
}

// String redacts the secret so a SigningKey formatted with %v/%+v/%s never emits
// the raw HMAC bytes; only the kid and the secret's length are shown. A value
// receiver puts String in the method set of both SigningKey and *SigningKey, so
// fmt uses it whether a value or a pointer is logged.
func (k SigningKey) String() string {
	return fmt.Sprintf("SigningKey{KID:%q, Secret:[REDACTED %d bytes]}", k.KID, len(k.Secret))
}

// GoString redacts the secret for the %#v verb as well.
func (k SigningKey) GoString() string {
	return k.String()
}

// ZeroSecret overwrites the key's secret bytes in place with zeros, so a
// long-lived process can wipe a secret it no longer needs rather than wait for
// the garbage collector. After calling it the key can no longer mint tokens:
// the backing bytes are scrubbed AND the slice is detached (length 0), so a
// later SignIngestJWT on this key fails the non-empty-secret guard rather than
// silently signing with all-zero bytes. It is safe to call on a zero-value key.
func (k *SigningKey) ZeroSecret() {
	for i := range k.Secret {
		k.Secret[i] = 0
	}
	k.Secret = nil
}

// allZeroBytes reports whether b is non-empty and every byte is zero — the state
// the backing array of a SigningKey is left in by ZeroSecret (and the state a
// by-value copy of a wiped key is in, since only the original's slice header is
// detached). Such a key cannot mint a token the verifier will accept, so
// SignIngestJWT rejects it.
func allZeroBytes(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return len(b) > 0
}

// IngestJWTClaims are the per-mint claims that bind a token to a verified user
// and a tenant ingest scope. All fields except BindAnon are required.
//
//   - Subject is the verified user_id (the JWT `sub`). It becomes the server's
//     verified principal user_id. Must be non-empty and at most
//     IngestSubjectMaxLength bytes.
//   - WorkspaceID, AppID, EnvironmentID identify the tenant ingest scope the
//     token authorizes. The server binds the ingest scope against these. All
//     three must be non-empty.
//   - BindAnon is OPTIONAL: the device's persistent anonymous_id. When set, the
//     server admits a stitched anon equal to it alongside the verified subject
//     (a verified anonymous alias). Empty means the token vouches for no anon. When set
//     it must be at most IngestSubjectMaxLength bytes.
type IngestJWTClaims struct {
	Subject       string
	WorkspaceID   string
	AppID         string
	EnvironmentID string
	BindAnon      string
}

// mintOptions are the resolved, defaulted mint parameters.
type mintOptions struct {
	issuer   string
	audience string
	lifetime time.Duration
	now      func() time.Time
}

// MintOption customizes a SignIngestJWT call.
type MintOption func(*mintOptions) error

// WithIngestIssuer overrides the `iss` claim (default DefaultIngestIssuer). It
// must match the issuer the target ShardPilot ingest API is configured with.
func WithIngestIssuer(issuer string) MintOption {
	return func(o *mintOptions) error {
		issuer = strings.TrimSpace(issuer)
		if issuer == "" {
			return fmt.Errorf("%w: issuer must be non-empty", ErrInvalidMintOption)
		}
		o.issuer = issuer
		return nil
	}
}

// WithIngestAudience overrides the `aud` claim (default DefaultIngestAudience).
// It must match the audience the target ShardPilot ingest API is configured with.
func WithIngestAudience(audience string) MintOption {
	return func(o *mintOptions) error {
		audience = strings.TrimSpace(audience)
		if audience == "" {
			return fmt.Errorf("%w: audience must be non-empty", ErrInvalidMintOption)
		}
		o.audience = audience
		return nil
	}
}

// WithIngestLifetime overrides the token validity window (exp - iat). The
// default is DefaultIngestLifetime (5m). It must be greater than zero and at
// most the server's 15m MaxLifetime cap; an over-long lifetime is rejected here
// at the mint source so the token is never rejected for lifetime at verify.
//
// NOTE: the server ALSO enforces a 5m iat-age window independent of exp, so a
// lifetime longer than ~5m advertises validity the server will not honour — the
// token becomes unusable ~5m after iat regardless of its exp. Prefer the default
// unless the deployment's MaxIatAge is configured higher.
func WithIngestLifetime(d time.Duration) MintOption {
	return func(o *mintOptions) error {
		if d <= 0 {
			return fmt.Errorf("%w: lifetime must be greater than zero", ErrInvalidMintOption)
		}
		if d > maxIngestLifetime {
			return fmt.Errorf("%w: lifetime %s exceeds the server maximum of %s", ErrInvalidMintOption, d, maxIngestLifetime)
		}
		o.lifetime = d
		return nil
	}
}

// WithIngestNow overrides the clock used to stamp iat/exp. It is intended for
// tests; production callers should leave it unset to use the real UTC clock.
// The supplied function must return a non-zero time.
func WithIngestNow(now func() time.Time) MintOption {
	return func(o *mintOptions) error {
		if now == nil {
			return fmt.Errorf("%w: now function must be non-nil", ErrInvalidMintOption)
		}
		o.now = now
		return nil
	}
}

// WithIngestClock overrides the clock used to stamp iat/exp from a Clock (the
// same interface the rest of the SDK uses). It is a convenience over
// WithIngestNow for callers that already hold a Clock. Intended for tests.
func WithIngestClock(clock Clock) MintOption {
	return func(o *mintOptions) error {
		if clock == nil {
			return fmt.Errorf("%w: clock must be non-nil", ErrInvalidMintOption)
		}
		o.now = clock.Now
		return nil
	}
}

// jwtHeader is the fixed JWS protected header: HS256, type JWT, plus the
// per-tenant kid. alg is hard-coded to HS256 and never taken from input, so
// this helper can only ever emit an HS256 token — algorithm confusion is
// impossible by construction.
type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
	Kid string `json:"kid"`
}

// ingestJWTPayload is the registered + custom claim set the Mode-B verifier
// requires. Times are Unix seconds (RFC 7519 NumericDate).
type ingestJWTPayload struct {
	Issuer        string `json:"iss"`
	Audience      string `json:"aud"`
	Subject       string `json:"sub"`
	IssuedAt      int64  `json:"iat"`
	ExpiresAt     int64  `json:"exp"`
	Scope         string `json:"scope"`
	WorkspaceID   string `json:"workspace_id"`
	AppID         string `json:"app_id"`
	EnvironmentID string `json:"environment_id"`
	BindAnon      string `json:"bind_anon,omitempty"`
}

// SignIngestJWT mints a short-lived Mode-B per-tenant ingest JWT,
// signed HS256 with the supplied per-tenant secret, that the ShardPilot
// Mode-B verifier accepts. The returned string is a compact JWS
// (header.payload.signature, all base64url, no padding).
//
// SECURITY: call this ONLY in a trusted server-side process. The returned token
// authorizes ingest for the bound user and tenant — treat it as a bearer
// credential, hand it to exactly one client over an authenticated channel, and
// NEVER log it. The signing secret it uses is held only here and must never
// ship in a client binary.
//
// It validates every input so a token it returns can never be rejected at
// verify for a malformed claim, an over-long subject/anon, or an over-long
// lifetime: an over-long lifetime, an empty/over-long subject, an empty tenant
// field, or an empty/invalid key all return an error and no token. The scope is
// fixed to analytics:ingest; iat is stamped to now (so the server's 5m MaxIatAge
// freshness window starts fresh) and exp to now+lifetime.
func SignIngestJWT(key SigningKey, claims IngestJWTClaims, opts ...MintOption) (string, error) {
	// --- Resolve options + defaults ---
	o := mintOptions{
		issuer:   DefaultIngestIssuer,
		audience: DefaultIngestAudience,
		lifetime: DefaultIngestLifetime,
		now:      func() time.Time { return time.Now().UTC() },
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&o); err != nil {
			return "", err
		}
	}

	// --- Validate the signing key ---
	kid := strings.TrimSpace(key.KID)
	if !validIngestKid(kid) {
		return "", fmt.Errorf("%w: kid must be non-empty, at most %d bytes, and use only [A-Za-z0-9_.-]", ErrInvalidSigningKey, ingestKidMaxLength)
	}
	if len(key.Secret) == 0 {
		return "", fmt.Errorf("%w: secret must be non-empty", ErrInvalidSigningKey)
	}
	if allZeroBytes(key.Secret) {
		// A wiped (ZeroSecret) or misconfigured all-zero secret has non-zero
		// length but cannot produce a token the verifier — which signs with the
		// real tenant secret — will accept. Reject it at the mint source.
		return "", fmt.Errorf("%w: secret is wiped or all-zero", ErrInvalidSigningKey)
	}

	// --- Validate the claims ---
	// Subject (sub) and BindAnon are SIGNED RAW (untrimmed). The server compares
	// the verified sub/bind_anon byte-for-byte against each event's user_id /
	// anonymous_id, which the event path (buildEnvelope) preserves verbatim;
	// trimming here would sign "user" for a caller's " user" and the token would
	// authorize/stitch a different identity than the events carry. Validate
	// against a trimmed copy (subject must be non-blank) and against the RAW
	// length the server will guard, but never alter the signed bytes.
	//
	// WorkspaceID/AppID/EnvironmentID, by contrast, are tenant-scope identifiers
	// the server matches against the resolved canonical scope (which is
	// whitespace-free), so they ARE trimmed to the canonical form.
	if strings.TrimSpace(claims.Subject) == "" {
		return "", fmt.Errorf("%w: subject (user_id) is required", ErrInvalidIngestClaims)
	}
	if len(claims.Subject) > IngestSubjectMaxLength {
		return "", fmt.Errorf("%w: subject exceeds the maximum of %d bytes", ErrInvalidIngestClaims, IngestSubjectMaxLength)
	}

	workspaceID := strings.TrimSpace(claims.WorkspaceID)
	appID := strings.TrimSpace(claims.AppID)
	environmentID := strings.TrimSpace(claims.EnvironmentID)
	if workspaceID == "" {
		return "", fmt.Errorf("%w: workspace_id is required", ErrInvalidIngestClaims)
	}
	if appID == "" {
		return "", fmt.Errorf("%w: app_id is required", ErrInvalidIngestClaims)
	}
	if environmentID == "" {
		return "", fmt.Errorf("%w: environment_id is required", ErrInvalidIngestClaims)
	}

	if len(claims.BindAnon) > IngestSubjectMaxLength {
		return "", fmt.Errorf("%w: bind_anon exceeds the maximum of %d bytes", ErrInvalidIngestClaims, IngestSubjectMaxLength)
	}

	// --- Stamp times ---
	now := o.now().UTC()
	if now.IsZero() {
		return "", fmt.Errorf("%w: clock returned a zero time", ErrInvalidMintOption)
	}
	iat := now.Unix()
	exp := now.Add(o.lifetime).Unix()

	header := jwtHeader{Alg: "HS256", Typ: "JWT", Kid: kid}
	payload := ingestJWTPayload{
		Issuer:        o.issuer,
		Audience:      o.audience,
		Subject:       claims.Subject,
		IssuedAt:      iat,
		ExpiresAt:     exp,
		Scope:         ingestScope,
		WorkspaceID:   workspaceID,
		AppID:         appID,
		EnvironmentID: environmentID,
		BindAnon:      claims.BindAnon,
	}

	return signHS256(header, payload, key.Secret)
}

// signHS256 encodes the header and payload as base64url JSON, computes the
// HMAC-SHA256 over "header.payload" with the secret, and returns the compact
// JWS. The secret is used only as the HMAC key and is never copied into the
// returned string or any error.
func signHS256(header jwtHeader, payload ingestJWTPayload, secret []byte) (string, error) {
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("encode shardpilot jwt header: %w", err)
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode shardpilot jwt payload: %w", err)
	}

	signingInput := base64URL(headerJSON) + "." + base64URL(payloadJSON)

	mac := hmac.New(sha256.New, secret)
	// hash.Hash.Write never returns an error; the signing input is the only
	// thing hashed and the secret is never written to the hash output.
	_, _ = mac.Write([]byte(signingInput))
	signature := mac.Sum(nil)

	return signingInput + "." + base64URL(signature), nil
}

// base64URL returns the RawURLEncoding (unpadded, JWT-canonical) base64 of b.
func base64URL(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}
