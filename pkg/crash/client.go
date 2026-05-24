package crash

import (
	"bytes"
	"context"
	"encoding/json"
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

type Client struct {
	ingestURL   string
	apiKey      string
	httpClient  *http.Client
	logger      Logger
	sampler     Sampler
	breadcrumbs *breadcrumbRing
}

type ClientOptions struct {
	IngestURL  string
	APIKey     string
	HTTPClient *http.Client
	Logger     Logger
	Sampler    Sampler
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

	return &Client{
		ingestURL:   ingestURL,
		apiKey:      apiKey,
		httpClient:  httpClient,
		logger:      opts.Logger,
		sampler:     sampler,
		breadcrumbs: newBreadcrumbRing(),
	}, nil
}

func (c *Client) Emit(ctx context.Context, event Event) error {
	return c.emit(ctx, event, false)
}

func (c *Client) EmitFatal(ctx context.Context, event Event) error {
	return c.emit(ctx, event, true)
}

func (c *Client) RecordBreadcrumb(name string) {
	if c == nil || c.breadcrumbs == nil {
		return
	}
	c.breadcrumbs.Record(name)
}

func (c *Client) emit(ctx context.Context, event Event, fatal bool) error {
	if c == nil {
		return fmt.Errorf("%w: client is nil", ErrInvalidConfig)
	}
	if err := c.validateReady(); err != nil {
		return err
	}
	prepared, err := c.prepareEvent(event)
	if err != nil {
		return err
	}
	if !fatal && c.sampler != nil && !c.sampler.ShouldEmit(prepared) {
		return nil
	}
	return c.post(ctx, prepared)
}

func (c *Client) prepareEvent(event Event) (Event, error) {
	event = cloneEvent(event)
	event.CrashID = strings.TrimSpace(event.CrashID)
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

	sanitized, err := sanitize.Event(event)
	if err != nil {
		return Event{}, err
	}
	if err := validateEvent(sanitized); err != nil {
		return Event{}, err
	}
	return sanitized, nil
}

func (c *Client) post(ctx context.Context, event Event) error {
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
	return nil
}

func (c *Client) logf(format string, args ...any) {
	if c.logger != nil {
		c.logger.Printf(format, args...)
	}
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
	return strings.TrimRight(parsed.String(), "/"), nil
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
