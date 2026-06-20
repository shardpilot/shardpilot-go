package crash

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

const defaultHTTPTimeout = 30 * time.Second
const defaultMaxAttempts = 2
const defaultRetryBackoff = 50 * time.Millisecond

type Client struct {
	ingestURL    string
	apiKey       string
	app          AppInfo
	source       string
	httpClient   *http.Client
	logger       Logger
	sampler      Sampler
	breadcrumbs  *breadcrumbRing
	maxAttempts  int
	retryBackoff time.Duration
}

type ClientOptions struct {
	IngestURL string
	APIKey    string
	// App identifies the project this client reports for. Its fields default onto every
	// event that leaves them empty (per-event values win). REQUIRED for auto-capture
	// (Recover/CapturePanic), which builds events with no App of their own; the producer
	// rejects an empty app.id and one that mismatches the API key's app scope. App.ID
	// must equal the API key's app.
	App AppInfo
	// Source is the component slug (ADR-0223) stamped on every event that doesn't set
	// its own: which repo/service in a multi-component product this crash came from
	// (e.g. main-server, game-server). Optional.
	Source       string
	HTTPClient   *http.Client
	Logger       Logger
	Sampler      Sampler
	MaxAttempts  int
	RetryBackoff time.Duration
}

type Logger interface {
	Printf(format string, args ...any)
}

type Sampler interface {
	ShouldEmit(e Event) bool
}

type HTTPStatusError struct {
	StatusCode int
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("shardpilot crash ingest returned status %d", e.StatusCode)
}

func (e *HTTPStatusError) Retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

func NewClient(opts ClientOptions) (*Client, error) {
	ingestURL, err := normalizeIngestURL(opts.IngestURL)
	if err != nil {
		return nil, err
	}
	apiKey := strings.TrimSpace(opts.APIKey)
	if apiKey == "" {
		return nil, fmt.Errorf("%w: api key is required", ErrInvalidConfig)
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPTimeout}
	}
	sampler := opts.Sampler
	if sampler == nil {
		sampler = newDefaultSampler()
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}
	retryBackoff := opts.RetryBackoff
	if retryBackoff <= 0 {
		retryBackoff = defaultRetryBackoff
	}

	return &Client{
		ingestURL: ingestURL,
		apiKey:    apiKey,
		app: AppInfo{
			ID:      strings.TrimSpace(opts.App.ID),
			Version: strings.TrimSpace(opts.App.Version),
			BuildID: strings.TrimSpace(opts.App.BuildID),
		},
		source:       strings.TrimSpace(opts.Source),
		httpClient:   httpClient,
		logger:       opts.Logger,
		sampler:      sampler,
		breadcrumbs:  newBreadcrumbRing(),
		maxAttempts:  maxAttempts,
		retryBackoff: retryBackoff,
	}, nil
}

func (c *Client) Emit(ctx context.Context, event Event) error {
	return c.emit(ctx, event, false, false)
}

func (c *Client) EmitFatal(ctx context.Context, event Event) error {
	return c.emit(ctx, event, true, false)
}

// emitCapturedFatal is the auto-capture path's fatal emit. Its frame functions come from
// the Go runtime symbol table (trusted code symbols, never caller-supplied), so they get
// the symbol-aware scrub that preserves package-qualified names; a manual Emit/EmitFatal
// caller's frame functions stay under the full content scrubber.
func (c *Client) emitCapturedFatal(ctx context.Context, event Event) error {
	return c.emit(ctx, event, true, true)
}

func (c *Client) RecordBreadcrumb(name string) {
	if c == nil || c.breadcrumbs == nil {
		return
	}
	c.breadcrumbs.Record(name)
}

func (c *Client) emit(ctx context.Context, event Event, fatal, trustedFrameFunctions bool) error {
	if c == nil {
		return fmt.Errorf("%w: client is nil", ErrInvalidConfig)
	}
	if err := c.validateReady(); err != nil {
		return err
	}
	prepared, err := c.prepareEvent(event, trustedFrameFunctions)
	if err != nil {
		return err
	}
	if !fatal && c.sampler != nil && !c.sampler.ShouldEmit(prepared) {
		return nil
	}
	return c.post(ctx, prepared)
}

func (c *Client) prepareEvent(event Event, trustedFrameFunctions bool) (Event, error) {
	event = cloneEvent(event)
	event.CrashID = strings.TrimSpace(event.CrashID)
	// Default app identity and source from client config; a per-event value always wins.
	if strings.TrimSpace(event.App.ID) == "" {
		event.App.ID = c.app.ID
	}
	if strings.TrimSpace(event.App.Version) == "" {
		event.App.Version = c.app.Version
	}
	if strings.TrimSpace(event.App.BuildID) == "" {
		event.App.BuildID = c.app.BuildID
	}
	if strings.TrimSpace(event.Source) == "" {
		event.Source = c.source
	}
	if event.CrashID == "" {
		id, err := newCrashID()
		if err != nil {
			return Event{}, fmt.Errorf("%w: generate crash_id: %v", ErrInvalidEvent, err)
		}
		event.CrashID = id
	}

	if len(event.Breadcrumbs) == 0 && c.breadcrumbs != nil {
		event.Breadcrumbs = c.breadcrumbs.Snapshot()
	} else {
		event.Breadcrumbs = capBreadcrumbs(event.Breadcrumbs)
	}
	event = normalizeEventTimes(event, time.Now().UTC())
	event = normalizeEventShape(event)

	sanitized, err := sanitizeEvent(event, trustedFrameFunctions)
	if err != nil {
		return Event{}, err
	}
	if err := validateEvent(sanitized); err != nil {
		return Event{}, err
	}
	return sanitized, nil
}

func (c *Client) post(ctx context.Context, event Event) error {
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		err := c.postOnce(ctx, event)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt >= c.maxAttempts || !retryableError(err) {
			return err
		}
		if waitErr := sleepContext(ctx, c.retryBackoff*time.Duration(attempt)); waitErr != nil {
			return waitErr
		}
	}
	return lastErr
}

func (c *Client) postOnce(ctx context.Context, event Event) error {
	if err := c.validateReady(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode shardpilot crash event: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.ingestURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create shardpilot crash ingest request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.apiKey)

	response, err := c.httpClient.Do(request)
	if err != nil {
		c.logf("shardpilot crash ingest request failed: %v", err)
		return fmt.Errorf("send shardpilot crash ingest request: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		err := &HTTPStatusError{StatusCode: response.StatusCode}
		c.logf("shardpilot crash ingest failed: %v", err)
		return err
	}
	return nil
}

func (c *Client) validateReady() error {
	if c == nil {
		return fmt.Errorf("%w: client is nil", ErrInvalidConfig)
	}
	if strings.TrimSpace(c.ingestURL) == "" {
		return fmt.Errorf("%w: ingest url is required", ErrInvalidConfig)
	}
	if strings.TrimSpace(c.apiKey) == "" {
		return fmt.Errorf("%w: api key is required", ErrInvalidConfig)
	}
	if c.httpClient == nil {
		return fmt.Errorf("%w: http client is required", ErrInvalidConfig)
	}
	if c.maxAttempts <= 0 {
		return fmt.Errorf("%w: max attempts must be positive", ErrInvalidConfig)
	}
	if c.retryBackoff <= 0 {
		return fmt.Errorf("%w: retry backoff must be positive", ErrInvalidConfig)
	}
	return nil
}

func (c *Client) logf(format string, args ...any) {
	if c.logger != nil {
		c.logger.Printf(format, args...)
	}
}

// safeLogf logs without ever letting a misbehaving Logger's panic escape. It is used on
// the crash-capture path, where an escaping panic would replace (mask) the original
// crash value that Recover is about to re-panic.
func (c *Client) safeLogf(format string, args ...any) {
	defer func() { _ = recover() }()
	c.logf(format, args...)
}

func normalizeIngestURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("%w: ingest url is required", ErrInvalidConfig)
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("%w: ingest url must be absolute", ErrInvalidConfig)
	}
	if parsed.Scheme != "https" && !allowInsecureURL(parsed) {
		return "", fmt.Errorf("%w: ingest url must use https outside localhost or loopback", ErrInvalidConfig)
	}
	if parsed.User != nil {
		return "", fmt.Errorf("%w: ingest url must not include user info", ErrInvalidConfig)
	}
	if parsed.RawQuery != "" {
		return "", fmt.Errorf("%w: ingest url must not include query parameters", ErrInvalidConfig)
	}
	if parsed.Fragment != "" {
		return "", fmt.Errorf("%w: ingest url must not include a fragment", ErrInvalidConfig)
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", fmt.Errorf("%w: ingest url must be the crash-symbolicator base URL", ErrInvalidConfig)
	}
	parsed.Path = ""
	parsed.RawPath = ""
	parsed.ForceQuery = false
	return strings.TrimRight(parsed.String(), "/") + "/api/v1/crashes/ingest", nil
}

func allowInsecureURL(parsed *url.URL) bool {
	if parsed.Scheme != "http" {
		return false
	}
	return isLoopbackHost(parsed.Hostname())
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type rateSampler struct {
	every   uint64
	counter atomic.Uint64
}

func newDefaultSampler() *rateSampler {
	return &rateSampler{every: 10}
}

func (s *rateSampler) ShouldEmit(Event) bool {
	if s == nil || s.every <= 1 {
		return true
	}
	return s.counter.Add(1)%s.every == 0
}

func retryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	type retryable interface {
		Retryable() bool
	}
	var statusErr retryable
	if errors.As(err, &statusErr) {
		return statusErr.Retryable()
	}
	return true
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
