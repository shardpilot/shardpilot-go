package shardpilot

import (
	"fmt"
	"net"
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
	OnBatchResult func(BatchResult)
}

const (
	defaultBatchSize     = 25
	maxBatchSize         = 100
	defaultBufferSize    = 1000
	defaultFlushInterval = time.Second
	defaultHTTPTimeout   = 2 * time.Second
)

func normalizeConfig(cfg Config) (Config, error) {
	cfg.IngestURL = strings.TrimSpace(cfg.IngestURL)
	cfg.Token = strings.TrimSpace(cfg.Token)
	cfg.WorkspaceID = strings.TrimSpace(cfg.WorkspaceID)
	cfg.AppID = strings.TrimSpace(cfg.AppID)
	cfg.EnvironmentID = strings.TrimSpace(cfg.EnvironmentID)
	cfg.UserID = strings.TrimSpace(cfg.UserID)
	cfg.AnonymousID = strings.TrimSpace(cfg.AnonymousID)

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

	parsed, err := url.Parse(cfg.IngestURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return Config{}, fmt.Errorf("%w: ingest URL must be absolute", ErrInvalidConfig)
	}
	if parsed.Scheme != "https" && !allowInsecureURL(parsed, cfg.AllowInsecurePrivateNetwork) {
		return Config{}, fmt.Errorf("%w: ingest URL must use https outside localhost, loopback, or explicitly allowed private networks", ErrInvalidConfig)
	}
	if parsed.User != nil {
		return Config{}, fmt.Errorf("%w: ingest URL must not include user info", ErrInvalidConfig)
	}
	if parsed.RawQuery != "" {
		return Config{}, fmt.Errorf("%w: ingest URL must not include query parameters", ErrInvalidConfig)
	}
	if parsed.Fragment != "" {
		return Config{}, fmt.Errorf("%w: ingest URL must not include a fragment", ErrInvalidConfig)
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return Config{}, fmt.Errorf("%w: ingest URL must not include a path", ErrInvalidConfig)
	}
	parsed.Path = ""
	parsed.RawPath = ""
	parsed.ForceQuery = false
	cfg.IngestURL = strings.TrimRight(parsed.String(), "/")

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
