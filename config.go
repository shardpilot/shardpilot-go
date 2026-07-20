package shardpilot

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Source string

const (
	SourceClient  Source = "client"
	SourceServer  Source = "server"
	SourceBackend Source = "backend"
)

type Logger interface {
	Printf(format string, args ...any)
}

type Config struct {
	IngestURL     string
	Token         string
	WorkspaceID   string
	AppID         string
	EnvironmentID string
	Source        Source
	AppVersion    string
	AppBuild      string
	Platform      string

	// UserID and AnonymousID are optional default actor identity values.
	// When set, they are used as envelope defaults for events that do not
	// set their own UserID/AnonymousID, and as the consent actor identifier
	// for SetConsent (UserID preferred, else AnonymousID). AnonymousID can
	// be sourced from the opt-in LoadOrCreateAnonymousID helper.
	UserID      string
	AnonymousID string

	BatchSize                   int
	BufferSize                  int
	FlushInterval               time.Duration
	HTTPTimeout                 time.Duration
	Logger                      Logger
	AllowInsecurePrivateNetwork bool

	// HTTPClient, when set, is the *http.Client every request this SDK makes
	// goes through — event-batch publishes, consent posts, and remote-config
	// fetches — so an integrator can supply a pooled transport, a proxy, mTLS,
	// or instrumentation. Nil — the default — keeps the SDK's internal
	// clients, exactly as before the field existed. Two SDK contracts survive
	// injection unchanged: every attempt is still bounded by the SOONER of
	// HTTPTimeout and the caller's own context deadline through per-request
	// contexts, whether or not the injected client carries a Timeout of its
	// own (a caller deadline longer than HTTPTimeout never stretches an
	// attempt past HTTPTimeout); and
	// remote-config fetches still refuse to follow redirects — the SDK derives
	// its remote-config client from HTTPClient with CheckRedirect pinned
	// (sharing the Transport and Jar), because silently following a 3xx would
	// surface the redirect target's body for an endpoint that authoritatively
	// is not here (see the rcClient rationale in transport.go).
	HTTPClient *http.Client

	// SchemaRevision overrides the ingest envelope schema-set revision this
	// client declares on events:batch publishes (the
	// X-ShardPilot-Schema-Revision request header). Empty — the default —
	// declares DefaultSchemaRevision, the revision this SDK release was
	// coordinated against. Set it when running against an ingest deployment
	// whose schema set differs from that pin (the server reports its own
	// revision in the same response header once its handshake is armed).
	SchemaRevision string

	// DisableSchemaRevision stops the client from declaring a schema-set
	// revision at all: no X-ShardPilot-Schema-Revision header on any request.
	// An undeclared revision always passes the server's handshake in every
	// mode, so this is the no-rebuild escape hatch if an armed enforce-mode
	// handshake starts rejecting this build's declared revision as stale.
	DisableSchemaRevision bool

	// APIKey is the publishable client ingest key (`sp_ingest_...`) that
	// authenticates remote-config fetches: the GET carries
	// `Authorization: Bearer <APIKey>`, never Token (a Mode B ingest JWT
	// cannot authenticate the remote-config endpoint). Required when
	// RemoteConfigURL is set; unused otherwise. Held in memory; never logged.
	APIKey string

	// RemoteConfigURL is the remote-config base URL (a dedicated config
	// origin — never the ingest URL). Empty — the default — disables the
	// remote-config client entirely. Validated like IngestURL: absolute,
	// HTTPS outside loopback (or private nets with
	// AllowInsecurePrivateNetwork), no path/query/fragment/user info.
	RemoteConfigURL string

	// RemoteConfigCachePath, when set, is the file the remote-config client
	// persists its durable last-known-good cache record to, so a restart or
	// an offline start still serves the previously fetched configuration.
	// Empty keeps the cache in memory only (getters still serve the last
	// served snapshot within the process). Independent of SpoolDir — setting
	// it never enables consent persistence or the disk spool.
	RemoteConfigCachePath string

	// RemoteConfigRevalidateInterval opts this client into PERIODIC
	// remote-config revalidation: a background timer that re-runs the same
	// conditional GET FetchRemoteConfig performs (If-None-Match with the
	// cached ETag), so an already-running client converges on a
	// server-side change — a kill switch flip included — within one
	// interval instead of at its next explicit fetch. Zero — the default —
	// keeps the documented explicit-fetch-only behavior byte-for-byte: no
	// timer, no background fetch, nothing new on the wire. Ignored unless
	// RemoteConfigURL is set.
	//
	// The schedule anchors to the SERVER's cadence: each cycle waits
	// max(this interval, the last observed Cache-Control max-age — 300s
	// before one is observed, and restored to 300s when a usable success
	// stops advertising one — floored at 60s), so a positive value here
	// can slow the timer down but never drive it faster than the server's
	// advertised cadence or the 60s floor. The cadence is observed on
	// USABLE fenced outcomes only (a fresh 200 or a 304), never from a
	// transient or refused response's incidental header, and a fetch that
	// observes a shorter cadence pulls a pending tick in. A tick inside an armed 429 Retry-After
	// cooldown performs no network call (the cooldown's cache-served
	// transient_429 outcome, exactly like an explicit fetch) and does not
	// reschedule tighter. Timer fetches classify exactly like explicit
	// fetches with ONE addition: after the TIMER receives an authoritative
	// 401/403 it halts until the client is re-initialized — an unattended
	// loop must not keep re-asking an endpoint that authoritatively
	// refused it. Only a refusal that IS the newest settled word for its
	// scope halts — a stale overlapping refusal that lost the per-scope
	// fence to a newer success halts nothing. Explicit FetchRemoteConfig
	// calls keep classifying per-fetch and never resume the halted timer.
	RemoteConfigRevalidateInterval time.Duration

	// ExperimentsURL opts this client into the experiment assignment
	// consumer: the control-plane experiments base URL INCLUDING the API
	// prefix the deployment routes (production/public
	// `https://<control-plane>/api/cp/v1`; in-cluster deployments may use
	// `/api/v1`). The SDK appends `/runtime/experiments/assignment`.
	// Unlike IngestURL/RemoteConfigURL a path prefix is expected here;
	// query, fragment, and user info are still rejected, and HTTPS is
	// required outside loopback (or private nets with
	// AllowInsecurePrivateNetwork). Empty — the default — disables the
	// experiment consumer entirely (fetches fail
	// experiments_not_configured, producers refuse
	// ErrExperimentsNotConfigured; nothing new on the wire). Requires
	// APIKey: the assignment fetch authenticates with the publishable
	// client key, never Token.
	//
	// Dark-phase note: the platform's experimentation flags are OFF in
	// every environment today, so a configured consumer receives 403
	// (`experimentation runtime is disabled` / `experiment assignment
	// fetch is disabled`) on every call until enablement — the expected
	// fail-closed posture. Until the control-plane auth leg grants
	// publishable keys the `experiment_assignment_read` scope, only an
	// explicitly scoped runtime token authenticates (401 otherwise);
	// integration tests need such a token during the dark phase.
	ExperimentsURL string

	// ExperimentSubjectKey is this installation's persisted experiment
	// subject key — the `subject_key` every assignment fetch sends —
	// conforming to `^spcid_[A-Za-z0-9_-]{20,64}$`. Source it from the
	// opt-in LoadOrCreateExperimentSubjectKey helper. It is a DEDICATED
	// identifier: never derive it from, and never replace, AnonymousID
	// (analytics identity continuity) — the raw value authenticates
	// nothing, and it never rides experiment analytics facts (the server
	// returns a derived subject_fact_key for that). A non-empty value that
	// does not match the grammar is rejected at construction; empty makes
	// assignment fetches fail subject_key_unavailable before any network
	// use.
	ExperimentSubjectKey string

	// ExperimentAssignmentCachePath, when set, is the file the experiment
	// consumer persists its cached assignment records to (one record per
	// experiment scope), so a restart serves last-known-good assignments
	// over transient failures. File 0600; directories the SDK creates are
	// 0700; a pre-existing parent's permissions are never changed. The
	// file is read back through a hard size limit (an over-limit file is
	// discarded as corrupt, never loaded whole) and holds ONE client's
	// scope set: records for another (workspace, app, environment,
	// subject, URL) tuple are misses and are dropped by this client's next persisted
	// change — one client per cache path is the supported topology, like
	// RemoteConfigCachePath. Empty keeps assignment records in memory
	// only. Independent of SpoolDir; never enables consent persistence.
	ExperimentAssignmentCachePath string

	// ExperimentAssignmentRevalidateInterval opts this client into the
	// AUTOMATIC assignment revalidation lane: a background timer that
	// re-fetches every cached assignment (the experiment keys this client
	// has fetched) so a running client converges on an operator kill or a
	// variant change without a host call. Zero — the default — means NO
	// automatic lane: assignments refresh only by explicit
	// FetchExperimentAssignment calls. Ignored unless ExperimentsURL is
	// set. Each cycle waits max(this interval, 60s).
	//
	// Halt contract (ADR-0259 Amendment 2): after the automatic lane
	// receives ANY authoritative 401/403 — the real-subjects sentinel
	// included — that is the newest settled word for its scope (a stale
	// overlapping refusal halts nothing), it stops scheduling assignment
	// fetches until the client is re-initialized. Host-triggered fetches keep classifying
	// per-fetch, and a later host-fetch success does NOT resume the lane.
	ExperimentAssignmentRevalidateInterval time.Duration

	// SpoolDir, when set, is the opt-in state directory for the bounded disk
	// spool and the persisted consent decision (`spool.json`, `consent.json`,
	// and the `spool-wipe-owed` marker; directory 0700, files 0600). Empty —
	// the default — keeps today's memory-only queue behavior: nothing is
	// ever written to disk. Disk participation is strictly grant-only; see
	// SetConsent. With ConsentFloor set it additionally holds the durable
	// consent-receipt outbox (`consent-outbox.json`).
	SpoolDir string

	// ConsentFloor opts this client into the CLIENT-SIDE consent floor: the
	// consent-first posture the ShardPilot engine SDKs (Defold/Unity/Unreal)
	// ship, adapted to this SDK's server posture. Nil — the default — keeps
	// the documented server-side-responsibility posture byte-for-byte:
	// unknown consent leaves the pipeline open, SetConsent posts its receipt
	// fire-and-forget, and nothing below applies.
	//
	// With the floor enabled:
	//   - Tri-state gating: Track/Enqueue refuse ErrConsentUnknown until an
	//     explicit decision is recorded, and ErrConsentDenied under a denial
	//     (including the forced-minor denial; see SetConsentDecision). With
	//     SpoolDir set, the persisted decision reloads as the LIVE state at
	//     construction, so a decision survives restarts — with the durable
	//     receipt trail's newest IN-SCOPE receipt overriding (and healing) a
	//     stale record when its decision is STRICTLY NEWER than the
	//     record's, and spooled events reloading only under a grant the
	//     resolved state confirms (which also reopens the spool write
	//     gate). The persisted record is scoped to the configured
	//     workspace/app/environment/actor tuple and carries floor
	//     provenance: a granted record written without the floor (the
	//     fire-and-forget era, receipt-less) is never promoted to live
	//     state — the floor starts undecided; record a fresh decision.
	//   - Consent receipts ride a durable outbox (32 receipts, FIFO,
	//     oldest-evicted, retried until acknowledged, delivered strictly in
	//     decision order — grant-then-deny can never settle reversed; server
	//     Retry-After honored on 429 and 5xx, jittered backoff otherwise).
	//     A receipt documents the decision itself, so delivery is permitted
	//     — required — while consent is denied or unknown. With SpoolDir the
	//     outbox survives process death; without it receipts are retained in
	//     memory only and Close reports ErrConsentPending while any remain
	//     undelivered.
	//   - The grant-receipt dispatch gate: event batches hold (Track/Flush
	//     return ErrConsentReceiptPending) while an analytics-grant receipt
	//     is retained undispatched, so post-grant events can never overtake
	//     the grant on the wire on a strict-consent workspace. Released on
	//     an OBSERVED HTTP outcome for the receipt (success or a status
	//     error) — never gated on its acknowledgement, while a no-response
	//     failure keeps holding — and an empty pipeline is never gated.
	//   - The floor covers the CONFIGURED identity: an event whose per-event
	//     UserID/AnonymousID override resolves to a DIFFERENT effective
	//     actor is refused (ErrConsentActorMismatch) — its actor has no
	//     local decision and no receipt. Per-actor decisions beyond the
	//     configured identity belong to the server-side consent path.
	//   - The floor requires IN-CONTRACT identifiers: a non-empty
	//     UserID/AnonymousID over 512 BYTES rejects a consent decision
	//     whole (ErrInvalidConsentIdentity — reject, never truncate), and
	//     refuses a persisted decision at reload the same way.
	//     Events stamp configured identifiers verbatim, so a receipt minted
	//     for a substitute actor would authorize a different actor than the
	//     events carry; the floor refuses to create that divergence.
	//     Oversized entries in a persisted outbox are dropped fail-safe by
	//     the sanitizer.
	//   - Decisions recorded after Close are memory-only in full (no
	//     receipt, no persisted record): record decisions before Close.
	ConsentFloor *ConsentFloorConfig

	// SpoolMaxEvents caps how many undelivered events the disk spool retains;
	// past it the OLDEST events are dropped (through OnSpoolDeadLetter).
	// Zero defaults to 2000 when SpoolDir is set.
	SpoolMaxEvents int

	// SpoolMaxBytes caps the total serialized envelope bytes the disk spool
	// retains, with the same oldest-first eviction. Zero defaults to 1 MiB
	// (1,048,576 bytes) when SpoolDir is set.
	SpoolMaxBytes int

	// OnSpoolDeadLetter, when set, is called with every event the disk spool
	// drops undelivered: capacity eviction, retry-age expiry, a terminal
	// ingest outcome on previously spooled events, a consent purge, and
	// batches refused disk under a non-grant (or owed-wipe) state. It runs on
	// the SDK's worker/consent paths; keep it fast and non-blocking. A panic
	// inside it is recovered, like OnBatchResult. Never called when SpoolDir
	// is empty.
	OnSpoolDeadLetter func(SpoolDeadLetter)

	// OnBatchResult, when set, is called after each successful batch publish
	// (HTTP 202) with the ingest outcome: the accepted/rejected/duplicate
	// aggregate plus the per-event status list the endpoint reports. It is the
	// only way to learn which individual events the server rejected, folded as
	// duplicates, observed (event_name not registered), or suppressed for
	// withheld consent — for suppressed events the 202 is not delivery
	// confirmation. The same per-event statuses are also folded into the
	// Snapshot().ByStatus aggregate.
	//
	// It runs synchronously on the SDK's publish path and may be called
	// concurrently: the background flush worker and synchronous Track publishes
	// share it. Keep it fast, non-blocking, and safe for concurrent use. It is
	// never called when the publish fails at the transport, and a panic inside
	// it is recovered and ignored so a buggy callback cannot stop delivery.
	// With a disk spool configured, the callback fires only AFTER the spool
	// has settled the delivered batch (previously spooled copies acked off
	// disk), so state changes made inside it — e.g. SetConsent(false) —
	// apply to the remaining record only, never to events this 202 already
	// delivered.
	//
	// Wiring it is the per-event way to notice strict-consent suppression:
	// on a workspace whose strict consent mode is enforced, events published
	// for an actor with no explicit consent recorded server-side (this SDK's
	// ConsentUnknown default keeps the pipeline open) come back
	// suppressed_no_consent in the 202 — a successful publish that delivered
	// nothing. Integrations without the callback can poll the
	// Snapshot().ByStatus breakdown for the same statuses. See
	// ConsentUnknown and SetConsent.
	OnBatchResult func(BatchResult)
}

// ConsentFloorConfig configures the opt-in client-side consent floor. The
// zero value enables the full documented contract; the struct exists so
// future floor tuning has a home without another Config field. See
// Config.ConsentFloor.
type ConsentFloorConfig struct{}

const (
	defaultBatchSize     = 25
	maxBatchSize         = 100
	defaultBufferSize    = 1000
	defaultFlushInterval = time.Second
	defaultHTTPTimeout   = 2 * time.Second

	// Disk-spool caps: the cross-SDK canonical bound of 2000 events / 1 MiB
	// of serialized envelopes (the Defold reference's smaller 500/256KiB is a
	// documented save-file adaptation, not the contract).
	defaultSpoolMaxEvents = 2000
	defaultSpoolMaxBytes  = 1 << 20
)

func normalizeConfig(cfg Config) (Config, error) {
	cfg.IngestURL = strings.TrimSpace(cfg.IngestURL)
	cfg.Token = strings.TrimSpace(cfg.Token)
	cfg.WorkspaceID = strings.TrimSpace(cfg.WorkspaceID)
	cfg.AppID = strings.TrimSpace(cfg.AppID)
	cfg.EnvironmentID = strings.TrimSpace(cfg.EnvironmentID)
	cfg.UserID = strings.TrimSpace(cfg.UserID)
	cfg.AnonymousID = strings.TrimSpace(cfg.AnonymousID)
	// The server trims the declared revision before comparing; trimming here
	// keeps a whitespace-padded override equal to its trimmed form (and a
	// whitespace-only override falls back to the default — use
	// DisableSchemaRevision to stop declaring).
	cfg.SchemaRevision = strings.TrimSpace(cfg.SchemaRevision)
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	cfg.RemoteConfigURL = strings.TrimSpace(cfg.RemoteConfigURL)
	cfg.RemoteConfigCachePath = strings.TrimSpace(cfg.RemoteConfigCachePath)
	cfg.ExperimentsURL = strings.TrimSpace(cfg.ExperimentsURL)
	cfg.ExperimentSubjectKey = strings.TrimSpace(cfg.ExperimentSubjectKey)
	cfg.ExperimentAssignmentCachePath = strings.TrimSpace(cfg.ExperimentAssignmentCachePath)
	cfg.SpoolDir = strings.TrimSpace(cfg.SpoolDir)

	if cfg.IngestURL == "" {
		return Config{}, fmt.Errorf("%w: ingest URL is required", ErrInvalidConfig)
	}
	if cfg.Token == "" {
		return Config{}, fmt.Errorf("%w: token is required", ErrInvalidConfig)
	}
	if cfg.WorkspaceID == "" {
		return Config{}, fmt.Errorf("%w: workspace ID is required", ErrInvalidConfig)
	}
	if cfg.AppID == "" {
		return Config{}, fmt.Errorf("%w: app ID is required", ErrInvalidConfig)
	}
	if cfg.EnvironmentID == "" {
		return Config{}, fmt.Errorf("%w: environment ID is required", ErrInvalidConfig)
	}
	if !validSource(cfg.Source) {
		return Config{}, fmt.Errorf("%w: source must be client, server, or backend", ErrInvalidConfig)
	}

	normalizedIngest, err := normalizeBaseURL(cfg.IngestURL, "ingest URL", cfg.AllowInsecurePrivateNetwork)
	if err != nil {
		return Config{}, err
	}
	cfg.IngestURL = normalizedIngest

	if cfg.RemoteConfigURL != "" {
		// The remote-config route authenticates with the publishable API key
		// only — a Mode B ingest JWT never does — so configuring the URL
		// without the key could never produce a working fetch.
		if cfg.APIKey == "" {
			return Config{}, fmt.Errorf("%w: remote config requires APIKey", ErrInvalidConfig)
		}
		normalizedRC, err := normalizeBaseURL(cfg.RemoteConfigURL, "remote config URL", cfg.AllowInsecurePrivateNetwork)
		if err != nil {
			return Config{}, err
		}
		cfg.RemoteConfigURL = normalizedRC
	}

	if cfg.ExperimentsURL != "" {
		// Same credential contract as remote config: the assignment fetch
		// authenticates with the publishable API key only, never Token.
		if cfg.APIKey == "" {
			return Config{}, fmt.Errorf("%w: experiments requires APIKey", ErrInvalidConfig)
		}
		normalizedExp, err := normalizePrefixedBaseURL(cfg.ExperimentsURL, "experiments URL", cfg.AllowInsecurePrivateNetwork)
		if err != nil {
			return Config{}, err
		}
		cfg.ExperimentsURL = normalizedExp
	}
	if cfg.ExperimentSubjectKey != "" && !validExperimentSubjectKey(cfg.ExperimentSubjectKey) {
		// The server enforces the spcid grammar with a permanent 400; a
		// value that can never fetch is a caller bug caught at
		// construction. (Notably a UUID AnonymousID wired here by mistake
		// fails this check — the subject key is a dedicated identifier.)
		return Config{}, fmt.Errorf("%w: ExperimentSubjectKey must match %s (see LoadOrCreateExperimentSubjectKey)", ErrInvalidConfig, experimentSubjectKeyGrammar)
	}

	if cfg.SpoolDir != "" {
		if cfg.SpoolMaxEvents <= 0 {
			cfg.SpoolMaxEvents = defaultSpoolMaxEvents
		}
		if cfg.SpoolMaxBytes <= 0 {
			cfg.SpoolMaxBytes = defaultSpoolMaxBytes
		}
	}

	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.BatchSize > maxBatchSize {
		cfg.BatchSize = maxBatchSize
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = defaultBufferSize
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultFlushInterval
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = defaultHTTPTimeout
	}

	return cfg, nil
}

// normalizeBaseURL validates and normalizes a base endpoint URL (ingest or
// remote-config): absolute, HTTPS outside loopback (or private networks when
// explicitly allowed), and bare — no user info, query, fragment, or path —
// with any trailing slash trimmed so equivalent spellings normalize
// identically. label names the field in the returned error.
func normalizeBaseURL(raw, label string, allowPrivate bool) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("%w: %s must be absolute", ErrInvalidConfig, label)
	}
	if parsed.Scheme != "https" && !allowInsecureURL(parsed, allowPrivate) {
		return "", fmt.Errorf("%w: %s must use https outside localhost, loopback, or explicitly allowed private networks", ErrInvalidConfig, label)
	}
	if parsed.User != nil {
		return "", fmt.Errorf("%w: %s must not include user info", ErrInvalidConfig, label)
	}
	if parsed.RawQuery != "" {
		return "", fmt.Errorf("%w: %s must not include query parameters", ErrInvalidConfig, label)
	}
	if parsed.Fragment != "" {
		return "", fmt.Errorf("%w: %s must not include a fragment", ErrInvalidConfig, label)
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", fmt.Errorf("%w: %s must not include a path", ErrInvalidConfig, label)
	}
	parsed.Path = ""
	parsed.RawPath = ""
	parsed.ForceQuery = false
	return strings.TrimRight(parsed.String(), "/"), nil
}

// normalizePrefixedBaseURL validates and normalizes a base endpoint URL that
// MAY carry a path prefix (the experiments base URL: control-plane routes
// mount under a deployment-chosen prefix such as /api/cp/v1, so the prefix is
// configuration, not something the SDK can hardcode). Same rules as
// normalizeBaseURL otherwise: absolute, HTTPS outside loopback (or private
// networks when explicitly allowed), no user info, query, or fragment; any
// trailing slash is trimmed so equivalent spellings normalize identically.
func normalizePrefixedBaseURL(raw, label string, allowPrivate bool) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("%w: %s must be absolute", ErrInvalidConfig, label)
	}
	if parsed.Scheme != "https" && !allowInsecureURL(parsed, allowPrivate) {
		return "", fmt.Errorf("%w: %s must use https outside localhost, loopback, or explicitly allowed private networks", ErrInvalidConfig, label)
	}
	if parsed.User != nil {
		return "", fmt.Errorf("%w: %s must not include user info", ErrInvalidConfig, label)
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return "", fmt.Errorf("%w: %s must not include query parameters", ErrInvalidConfig, label)
	}
	if parsed.Fragment != "" {
		return "", fmt.Errorf("%w: %s must not include a fragment", ErrInvalidConfig, label)
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func validSource(source Source) bool {
	switch source {
	case SourceClient, SourceServer, SourceBackend:
		return true
	default:
		return false
	}
}

func allowInsecureURL(parsed *url.URL, allowPrivate bool) bool {
	if parsed.Scheme != "http" {
		return false
	}
	host := parsed.Hostname()
	if isLoopbackHost(host) {
		return true
	}
	if allowPrivate && isPrivateIP(host) {
		return true
	}
	return false
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func isPrivateIP(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && ip.IsPrivate()
}
