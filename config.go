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

	// SpoolDir, when set, is the opt-in state directory for the bounded disk
	// spool and the persisted consent decision (`spool.json`, `consent.json`,
	// and the `spool-wipe-owed` marker; directory 0700, files 0600). Empty —
	// the default — keeps today's memory-only queue behavior: nothing is
	// ever written to disk. Disk participation is strictly grant-only; see
	// SetConsent.
	SpoolDir string

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
