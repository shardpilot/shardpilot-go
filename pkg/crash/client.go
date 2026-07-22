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
	"strconv"
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
	onResult     func(Result)
	// selfModule is the running binary's identity, resolved ONCE at NewClient when
	// DebugIDFillEnabled is on (nil when the flag is off OR the binary is
	// unreadable), so the panic path does no file I/O.
	selfModule *Module
	// allGoroutines mirrors AllGoroutineCaptureEnabled.
	allGoroutines bool
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
	// Source is the component slug stamped on every event that doesn't set
	// its own: which repo/service in a multi-component product this crash came from
	// (e.g. main-server, game-server). Optional.
	Source string
	// DebugIDFillEnabled opts auto-capture (Recover/CapturePanic) into attaching
	// the RUNNING BINARY's identity as the event's single modules[] entry: the
	// executable's base name plus a debug_id read from the binary itself — the ELF
	// GNU build-id as lowercase hex (the identity `dump_syms` emits for ELF, so
	// the crash joins symbols uploaded under that id), falling back to the
	// lowercase-hex SHA-256 of the Go build id (`go tool buildid <binary> |
	// sha256sum`) when no GNU note is present (the default Go linker emits none).
	// The identity is resolved ONCE at NewClient; on a non-ELF platform or an
	// unreadable binary the fill is skipped and capture proceeds unchanged.
	// Manual Emit/EmitFatal events are never touched — their modules stay
	// caller-owned.
	//
	// Default false — DARK (ADR-0297 §7d): while off, zero self-read code paths
	// execute and the auto-captured wire shape stays byte-identical to the
	// pre-fill SDK. Phase-D arming order (§12): enable only after the SDK's
	// client-side consent gate and durable spool have landed — new capture detail
	// must not ship ahead of consent parity.
	DebugIDFillEnabled bool
	// AllGoroutineCaptureEnabled opts auto-capture into snapshotting EVERY
	// goroutine at panic time (runtime.Stack all): the other goroutines ship as
	// additional pre-symbolicated threads[] beside the precise crashed thread,
	// each named by its goroutine id with the scheduler state (e.g. "chan
	// receive") as the thread name. Bounded by the event's caps — 64 threads and
	// 256 total frames, in dump order, at most 16 frames per non-crashing
	// goroutine.
	//
	// Default false — DARK (ADR-0297 §7d): while off, the dump is never taken and
	// the auto-captured wire shape stays byte-identical. Same Phase-D arming
	// order as DebugIDFillEnabled (§12): consent gate + durable spool first.
	AllGoroutineCaptureEnabled bool
	HTTPClient                 *http.Client
	Logger                     Logger
	Sampler                    Sampler
	MaxAttempts                int
	RetryBackoff               time.Duration
	// OnResult, when set, is called after each successful ingest with the server's
	// Result (whether the crash was suppressed, plus any warnings). It runs on the
	// calling goroutine — including the auto-capture path during a panic — so keep it
	// fast and non-blocking; a panic inside it is recovered and ignored.
	OnResult func(Result)
}

// Result is the outcome of a crash ingest, parsed from the server's response.
type Result struct {
	// CrashID echoes the id of the accepted crash.
	CrashID string
	// Fingerprint is the server-assigned grouping fingerprint (empty when suppressed).
	Fingerprint string
	// Suppressed is true when the server accepted the request (2xx) but did NOT store the
	// crash because the actor withheld consent. Callers that need delivery confirmation
	// must check this — the HTTP status alone is 2xx in this case.
	Suppressed bool
	// Warnings are non-fatal processing notices the server attached to the response.
	Warnings []string
}

type Logger interface {
	Printf(format string, args ...any)
}

type Sampler interface {
	ShouldEmit(e Event) bool
}

type HTTPStatusError struct {
	StatusCode int
	// RetryAfter is the delay the server asked the client to wait before retrying,
	// parsed from the Retry-After response header. Zero when absent, unparseable, or an
	// explicit "retry now" (0); use a present value via the retry loop, which distinguishes
	// the two internally.
	RetryAfter time.Duration
	// retryAfterPresent reports whether the server actually sent a Retry-After (even "0").
	// The retry loop honors a present value over the fixed backoff; an absent one falls back.
	retryAfterPresent bool
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
	var selfModule *Module
	if opts.DebugIDFillEnabled {
		if m, ok := readSelfModule(); ok {
			selfModule = &m
		} else if opts.Logger != nil {
			// Fail open: an unreadable binary must not fail client construction or
			// capture — the event just ships without a self-module, as before.
			opts.Logger.Printf("shardpilot crash: debug-id fill enabled but the running binary's identity is unreadable; capturing without a self-module")
		}
	}

	return &Client{
		ingestURL: ingestURL,
		apiKey:    apiKey,
		app: AppInfo{
			ID:      strings.TrimSpace(opts.App.ID),
			Version: strings.TrimSpace(opts.App.Version),
			BuildID: strings.TrimSpace(opts.App.BuildID),
		},
		source:        strings.TrimSpace(opts.Source),
		httpClient:    httpClient,
		logger:        opts.Logger,
		sampler:       sampler,
		breadcrumbs:   newBreadcrumbRing(),
		maxAttempts:   maxAttempts,
		retryBackoff:  retryBackoff,
		onResult:      opts.OnResult,
		selfModule:    selfModule,
		allGoroutines: opts.AllGoroutineCaptureEnabled,
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
	result, err := c.post(ctx, prepared)
	if err != nil {
		return err
	}
	c.notifyResult(result)
	return nil
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

func (c *Client) post(ctx context.Context, event Event) (Result, error) {
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		result, err := c.postOnce(ctx, event)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if attempt >= c.maxAttempts || !retryableError(err) {
			return Result{}, err
		}
		backoff := c.retryBackoff * time.Duration(attempt)
		// Honor a server-supplied Retry-After (e.g. a gateway 429) over the fixed backoff,
		// including an explicit "retry now" (0); fall back to the fixed backoff only when
		// the server sent no Retry-After.
		if retryAfter, ok := retryAfterFromError(err); ok {
			backoff = retryAfter
		}
		if waitErr := sleepContext(ctx, backoff); waitErr != nil {
			return Result{}, waitErr
		}
	}
	return Result{}, lastErr
}

func (c *Client) postOnce(ctx context.Context, event Event) (Result, error) {
	if err := c.validateReady(); err != nil {
		return Result{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return Result{}, fmt.Errorf("encode shardpilot crash event: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.ingestURL, bytes.NewReader(payload))
	if err != nil {
		return Result{}, fmt.Errorf("create shardpilot crash ingest request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.apiKey)

	response, err := c.httpClient.Do(request)
	if err != nil {
		c.logf("shardpilot crash ingest request failed: %v", err)
		return Result{}, fmt.Errorf("send shardpilot crash ingest request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// Drain a bounded amount for connection reuse without buffering the unused
		// error body into a retained slice.
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxResponseBodyBytes))
		retryAfter, present := parseRetryAfter(response.Header.Get("Retry-After"))
		statusErr := &HTTPStatusError{
			StatusCode:        response.StatusCode,
			RetryAfter:        retryAfter,
			retryAfterPresent: present,
		}
		c.logf("shardpilot crash ingest failed: %v", statusErr)
		return Result{}, statusErr
	}

	body, _ := io.ReadAll(io.LimitReader(response.Body, maxResponseBodyBytes))
	result := parseResult(body)
	if result.Suppressed {
		c.logf("shardpilot crash ingest suppressed: actor consent withheld, crash not stored")
	}
	for _, warning := range result.Warnings {
		c.logf("shardpilot crash ingest warning: %s", warning)
	}
	return result, nil
}

const (
	// maxResponseBodyBytes caps how much of the ingest response is read for parsing.
	maxResponseBodyBytes = 1 << 20
	// maxRetryAfter caps a server-supplied Retry-After so a hostile or buggy value cannot
	// stall a reporting goroutine — which, on the auto-capture path, runs during crash
	// handling — for an unbounded time.
	maxRetryAfter = 2 * time.Minute
	// maxRetryAfterSeconds is maxRetryAfter in whole seconds; the seconds form is clamped
	// against it BEFORE multiplying so a huge value cannot overflow time.Duration.
	maxRetryAfterSeconds = int(maxRetryAfter / time.Second)
)

// parseResult best-effort decodes the crash ingest response. A malformed or empty body
// yields the zero Result and never turns a 2xx into a failure.
func parseResult(body []byte) Result {
	var payload struct {
		CrashID     string   `json:"crash_id"`
		Fingerprint string   `json:"fingerprint"`
		Suppressed  bool     `json:"suppressed"`
		Warnings    []string `json:"warnings"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return Result{}
	}
	return Result{
		CrashID:     payload.CrashID,
		Fingerprint: payload.Fingerprint,
		Suppressed:  payload.Suppressed,
		Warnings:    payload.Warnings,
	}
}

// parseRetryAfter parses a Retry-After header (delta-seconds or an HTTP-date). The bool
// reports whether a valid value was present: an explicit "0" (or an already-elapsed date)
// returns (0, true) — retry now — distinct from an absent or malformed header (0, false),
// which leaves the caller on its fixed backoff. The duration is clamped to [0, maxRetryAfter].
func parseRetryAfter(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		switch {
		case seconds < 0:
			return 0, false
		case seconds == 0:
			return 0, true
		case seconds > maxRetryAfterSeconds:
			// Clamp before multiplying so a huge value cannot overflow time.Duration.
			return maxRetryAfter, true
		default:
			return time.Duration(seconds) * time.Second, true
		}
	} else if errors.Is(err, strconv.ErrRange) {
		// A value too large to fit an int (e.g. a 20-digit header) still means "wait a
		// long time" → clamp to the cap; a hugely negative one is malformed.
		if strings.HasPrefix(value, "-") {
			return 0, false
		}
		return maxRetryAfter, true
	}
	if when, err := http.ParseTime(value); err == nil {
		return clampRetryAfter(time.Until(when)), true
	}
	return 0, false
}

func clampRetryAfter(d time.Duration) time.Duration {
	switch {
	case d <= 0:
		return 0
	case d > maxRetryAfter:
		return maxRetryAfter
	default:
		return d
	}
}

// retryAfterFromError returns the server-requested retry delay carried by an
// *HTTPStatusError and whether one was present (an explicit "0" is present, retry now).
func retryAfterFromError(err error) (time.Duration, bool) {
	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) {
		return statusErr.RetryAfter, statusErr.retryAfterPresent
	}
	return 0, false
}

// notifyResult invokes the optional OnResult callback, guarding against a panic in user
// code — which, on the auto-capture path, would re-enter crash handling.
func (c *Client) notifyResult(result Result) {
	if c == nil || c.onResult == nil {
		return
	}
	defer func() { _ = recover() }()
	c.onResult(result)
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
		return "", fmt.Errorf("%w: ingest url must be the crash ingest base URL", ErrInvalidConfig)
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
