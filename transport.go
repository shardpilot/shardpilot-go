package shardpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type transport interface {
	Publish(ctx context.Context, request batchRequest) (batchResult, error)
	PublishConsent(ctx context.Context, request consentRequest) (consentResult, error)
	FetchRemoteConfig(ctx context.Context, request remoteConfigRequest) (remoteConfigResponse, error)
}

// remoteConfigRequest is one remote-config GET: the fully built fetch URL,
// the publishable API key for the Bearer header (never the ingest token),
// and the cached ETag for If-None-Match revalidation (empty sends none).
type remoteConfigRequest struct {
	url         string
	bearer      string
	ifNoneMatch string
}

// remoteConfigResponse is the raw remote-config outcome the decision core
// (applyRemoteConfig) classifies. body is capped at rcMaxBodyBytes+1 so an
// over-cap payload is detectable without unbounded reads; bodyIncomplete
// marks a body whose read failed mid-stream (a truncated response) — the
// status and headers already arrived and are real information, so a
// truncated response still classifies BY STATUS (a truncated 401 fails
// closed exactly like a whole one; only a 200 needs its body at all).
// retryAfterSeconds carries a 429's Retry-After parsed digits-only
// (HTTP-date is NOT accepted on this route — the server contract sends
// integer seconds >= 1); retryAfterPresent distinguishes a parsed header
// from an absent or malformed one.
type remoteConfigResponse struct {
	status            int
	body              []byte
	bodyIncomplete    bool
	etag              string
	retryAfterSeconds int
	retryAfterPresent bool
}

// batchResult is the wire decode of the events:batch response (HTTP 202).
// It is mapped to the public BatchResult before it leaves the SDK; see
// batch_result.go.
type batchResult struct {
	Accepted   int                    `json:"accepted"`
	Rejected   int                    `json:"rejected"`
	Duplicates int                    `json:"duplicates"`
	Events     []batchEventStatusWire `json:"events"`
}

// batchEventStatusWire is the wire decode of one per-event outcome in the
// batch response.
type batchEventStatusWire struct {
	EventID string `json:"event_id"`
	Status  string `json:"status"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// ErrorDetail is one entry of the ingest error envelope's details list: a
// per-field machine-readable reason for a request-level rejection (for
// example field "events[0].event_id", code "required").
type ErrorDetail struct {
	Field   string
	Code    string
	Message string
}

// HTTPStatusError is a non-2xx ingest response. Beyond the bare HTTP status
// it carries the server's error envelope ({"error":{code,message,details}})
// when the body held one, and the whole-seconds Retry-After header as a
// duration when the server attached one (429 backpressure), so callers see
// the real cause — e.g. rate_limited with detail monthly_quota_exceeded —
// instead of a silent status number.
type HTTPStatusError struct {
	StatusCode int

	// ErrorCode is the envelope's top-level error code (validation_error,
	// unauthorized, forbidden, rate_limited, internal_error, ...); empty when
	// the response body carried no parseable envelope.
	ErrorCode string

	// ErrorMessage is the envelope's human-readable message; empty when absent.
	ErrorMessage string

	// Details are the envelope's per-field reasons; nil when absent.
	Details []ErrorDetail

	// RetryAfter is the server's Retry-After response header — delta-seconds
	// or HTTP-date form, clamped to 24h — or zero when the header was absent
	// or unparseable. The background flush worker defers its next automatic
	// publish attempt by at least this long; synchronous Track callers
	// receive it here and decide for themselves.
	RetryAfter time.Duration

	// retryAfterPresent distinguishes an explicit "Retry-After: 0" (or an
	// already-elapsed HTTP-date) — the server saying "retry now" — from a
	// missing/unparseable header. The worker honors an explicit zero by
	// retrying immediately instead of waiting out the flush interval.
	retryAfterPresent bool
}

// maxErrorDetailCodes bounds how many detail codes Error() folds into the
// message so a hostile/verbose body cannot bloat logs; Details keeps them all.
const maxErrorDetailCodes = 5

func (e *HTTPStatusError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "shardpilot ingest returned status %d", e.StatusCode)
	if e.ErrorCode != "" {
		fmt.Fprintf(&b, " (%s)", e.ErrorCode)
	}
	if len(e.Details) > 0 {
		b.WriteString(" [")
		for i, detail := range e.Details {
			if i >= maxErrorDetailCodes {
				b.WriteString(",...")
				break
			}
			if i > 0 {
				b.WriteString(",")
			}
			if detail.Field != "" {
				b.WriteString(detail.Field)
				b.WriteString(":")
			}
			b.WriteString(detail.Code)
		}
		b.WriteString("]")
	}
	return b.String()
}

func (e *HTTPStatusError) Retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

type EncodeError struct {
	Err error
}

func (e *EncodeError) Error() string {
	return fmt.Sprintf("encode shardpilot batch: %v", e.Err)
}

func (e *EncodeError) Unwrap() error {
	return e.Err
}

type httpTransport struct {
	endpoint        string
	consentEndpoint string
	token           string
	// schemaRevision is declared via the X-ShardPilot-Schema-Revision request
	// header on events:batch publishes ONLY — the header is not defined for
	// the consent route. Empty means declare nothing (header omitted).
	schemaRevision string
	client         *http.Client
	// rcClient serves remote-config fetches only. Unlike the ingest client it
	// does NOT follow redirects: the remote-config contract classifies a 3xx
	// as a permanent http_3xx failure, and silently following it would
	// instead surface the redirect target's body (typically HTML) as a
	// transient malformed_response — the wrong class, and one that serves the
	// cache for an endpoint that authoritatively is not here.
	rcClient *http.Client
}

func newHTTPTransport(cfg Config) *httpTransport {
	base := strings.TrimRight(cfg.IngestURL, "/")
	refuseRedirects := func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	client := cfg.HTTPClient
	rcClient := &http.Client{
		Timeout:       cfg.HTTPTimeout,
		CheckRedirect: refuseRedirects,
	}
	if client == nil {
		client = &http.Client{
			Timeout: cfg.HTTPTimeout,
		}
	} else {
		// The remote-config client derives from the injected one — same
		// Transport, Jar, and Timeout — with only CheckRedirect pinned: the
		// no-redirect classification is an SDK contract (a 3xx is the
		// permanent http_3xx outcome), not an integrator knob.
		derived := *client
		derived.CheckRedirect = refuseRedirects
		rcClient = &derived
	}
	return &httpTransport{
		endpoint:        base + "/v1/events:batch",
		consentEndpoint: base + "/v1/consent",
		token:           cfg.Token,
		schemaRevision:  effectiveSchemaRevision(cfg),
		client:          client,
		rcClient:        rcClient,
	}
}

func (t *httpTransport) Publish(ctx context.Context, request batchRequest) (batchResult, error) {
	var result batchResult
	if err := t.postJSON(ctx, t.endpoint, request, &result, t.schemaRevision); err != nil {
		return batchResult{}, err
	}
	return result, nil
}

func (t *httpTransport) PublishConsent(ctx context.Context, request consentRequest) (consentResult, error) {
	var result consentResult
	// The schema-revision header is a batch-endpoint contract; the consent
	// route must never carry it.
	if err := t.postJSON(ctx, t.consentEndpoint, request, &result, ""); err != nil {
		if errors.Is(err, io.EOF) {
			// A 2xx with an EMPTY body (a 204, or an empty 200): the decode
			// hit clean EOF, but the consent contract is that ANY 2xx
			// acknowledges the write — the status is the acknowledgement,
			// the body is optional. Treating it as retryable would retain
			// an already-accepted receipt forever: a grant would keep
			// gating events and Close would keep reporting pending work.
			// (postJSON decodes only after the 2xx check, so a decode EOF
			// implies the status already passed; a TRUNCATED body —
			// ErrUnexpectedEOF — stays retryable, and the server
			// de-duplicates the re-send by idempotency key.)
			return consentResult{}, nil
		}
		return consentResult{}, err
	}
	return result, nil
}

// FetchRemoteConfig GETs the remote-config resource. It reports transport-
// level failures before a status arrives (no connection, header-phase
// timeout) as a bare error — classified as the transient `http_0` outcome
// upstream — and every HTTP response, whatever its status, as a
// remoteConfigResponse for applyRemoteConfig to classify; a response whose
// body read was ended by the request context is returned WITH the read error
// so the caller can discriminate a caller abort from an SDK-internal
// deadline (see the body-read comment below).
// Redirects are NOT followed (see rcClient): a 3xx is returned as-is so the
// decision core can classify it as the contract's permanent http_3xx. The
// request authenticates with the publishable API key only, and never carries
// the schema-revision header (a batch-route contract).
func (t *httpTransport) FetchRemoteConfig(ctx context.Context, request remoteConfigRequest) (remoteConfigResponse, error) {
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, request.url, nil)
	if err != nil {
		return remoteConfigResponse{}, fmt.Errorf("create shardpilot remote config request: %w", err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+request.bearer)
	if request.ifNoneMatch != "" {
		httpRequest.Header.Set("If-None-Match", request.ifNoneMatch)
	}

	response, err := t.rcClient.Do(httpRequest)
	if err != nil {
		return remoteConfigResponse{}, fmt.Errorf("send shardpilot remote config request: %w", err)
	}
	defer response.Body.Close()

	// Read one byte past the contract's 1MB cap: an over-cap body arrives
	// truncated to rcMaxBodyBytes+1 bytes, which applyRemoteConfig classifies
	// as malformed without this ever reading an unbounded response. A body
	// read cut short — by the server, or by the request context expiring
	// mid-body — is not a transport failure: the status line and headers
	// already arrived and are real information, so the response is passed
	// through with bodyIncomplete set (a stalled or truncated 401/403 must
	// fail closed by status, not degrade into a cache-served http_0). When
	// the CONTEXT ended the read, the error is returned alongside the
	// response: the caller discriminates a caller-context abort (the caller's
	// own error wins, response discarded) from an SDK-internal deadline (the
	// response classifies by status).
	body, readErr := io.ReadAll(io.LimitReader(response.Body, rcMaxBodyBytes+1))
	seconds, present := parseRemoteConfigRetryAfter(response.Header.Get("Retry-After"))
	result := remoteConfigResponse{
		status:            response.StatusCode,
		body:              body,
		bodyIncomplete:    readErr != nil,
		etag:              response.Header.Get("Etag"),
		retryAfterSeconds: seconds,
		retryAfterPresent: present,
	}
	if readErr != nil && ctx.Err() != nil {
		return result, fmt.Errorf("read shardpilot remote config response: %w", readErr)
	}
	return result, nil
}

// parseRemoteConfigRetryAfter reads a remote-config 429's Retry-After header
// in digits-only delta-seconds form. Unlike the batch route's parseRetryAfter
// it deliberately does NOT accept the HTTP-date form: the remote-config
// server contract sends integer seconds >= 1, and the SDK-wide parse rule
// for this route is digits-only. Anything else — absent, negative, a date, a
// fraction — reports (0, false), which the cooldown treats as the 1s floor.
// A digits-only value too large for int64 still means "wait a long time" and
// saturates at the cooldown clamp instead of being ignored.
func parseRemoteConfigRetryAfter(header string) (int, bool) {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0, false
	}
	for i := 0; i < len(header); i++ {
		if header[i] < '0' || header[i] > '9' {
			return 0, false
		}
	}
	seconds, err := strconv.ParseInt(header, 10, 64)
	if err != nil || seconds > rcCooldownClampSeconds {
		return rcCooldownClampSeconds, true
	}
	return int(seconds), true
}

// postJSON posts one JSON request to an ingest endpoint. schemaRevision, when
// non-empty, is declared via the X-ShardPilot-Schema-Revision request header;
// it is passed per call because this helper is shared across routes and only
// the events:batch route defines the header.
func (t *httpTransport) postJSON(ctx context.Context, endpoint string, request any, result any, schemaRevision string) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return &EncodeError{Err: err}
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create shardpilot ingest request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+t.token)
	if schemaRevision != "" {
		httpRequest.Header.Set(schemaRevisionHeader, schemaRevision)
	}

	response, err := t.client.Do(httpRequest)
	if err != nil {
		return fmt.Errorf("send shardpilot ingest request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return newHTTPStatusError(response)
	}

	if err := json.NewDecoder(response.Body).Decode(result); err != nil {
		return fmt.Errorf("decode shardpilot ingest response: %w", err)
	}
	return nil
}

// maxErrorBodyBytes bounds how much of a non-2xx response body is read while
// looking for the error envelope; a real envelope is tiny.
const maxErrorBodyBytes = 64 << 10

// maxRetryAfter caps the honored Retry-After at one day (the largest delay
// the ingest service advertises), so a hostile or garbled header can never
// park the client longer.
const maxRetryAfter = 24 * time.Hour

// errorResponseWire mirrors the ingest error envelope
// {"error":{"code","message","details":[{"field","code","message"}]}}.
type errorResponseWire struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details []struct {
			Field   string `json:"field"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"details"`
	} `json:"error"`
}

// newHTTPStatusError builds the typed error for a non-2xx response: the
// error envelope is parsed best-effort from a bounded read of the body (a
// malformed body degrades to the bare status), and the whole-seconds
// Retry-After header is carried as a duration when present.
func newHTTPStatusError(response *http.Response) *HTTPStatusError {
	retryAfter, retryAfterPresent := parseRetryAfter(response.Header.Get("Retry-After"))
	statusErr := &HTTPStatusError{
		StatusCode:        response.StatusCode,
		RetryAfter:        retryAfter,
		retryAfterPresent: retryAfterPresent,
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxErrorBodyBytes))
	if err != nil || len(body) == 0 {
		return statusErr
	}
	var wire errorResponseWire
	if json.Unmarshal(body, &wire) != nil || wire.Error.Code == "" {
		return statusErr
	}
	statusErr.ErrorCode = wire.Error.Code
	statusErr.ErrorMessage = wire.Error.Message
	if len(wire.Error.Details) > 0 {
		statusErr.Details = make([]ErrorDetail, 0, len(wire.Error.Details))
		for _, detail := range wire.Error.Details {
			statusErr.Details = append(statusErr.Details, ErrorDetail(detail))
		}
	}
	return statusErr
}

// parseRetryAfter reads a Retry-After header in either standard form —
// delta-seconds or HTTP-date — mirroring the crash client's handling, and
// clamps the result to maxRetryAfter. The second return reports whether the
// header carried a usable value at all: an explicit "0" (or an
// already-elapsed HTTP-date) is a real server hint meaning "retry now" and
// returns (0, true), while an absent, negative, or malformed header returns
// (0, false). The delta-seconds clamp compares raw seconds BEFORE the
// duration conversion: a huge-but-parseable value (above ~9.2e9 seconds)
// would overflow the nanosecond multiplication and slip past a
// duration-level clamp as garbage; a value too large even for int64 still
// means "wait a long time" and clamps rather than being ignored.
func parseRetryAfter(header string) (time.Duration, bool) {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(header, 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		if seconds > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter, true
		}
		return time.Duration(seconds) * time.Second, true
	} else if errors.Is(err, strconv.ErrRange) && !strings.HasPrefix(header, "-") {
		return maxRetryAfter, true
	}
	if when, err := http.ParseTime(header); err == nil {
		retryAfter := time.Until(when)
		if retryAfter <= 0 {
			return 0, true
		}
		if retryAfter > maxRetryAfter {
			return maxRetryAfter, true
		}
		return retryAfter, true
	}
	return 0, false
}

// contextWithDefaultTimeout bounds one attempt by the SDK timeout,
// UNCONDITIONALLY: context.WithTimeout never extends a parent's earlier
// deadline, so the derived context carries min(caller deadline, now+timeout).
// It used to leave a parent that already had a deadline untouched, relying on
// the internal http.Client's Timeout as the cap — which an injected
// Config.HTTPClient with a zero Timeout does not have, letting a caller
// deadline LONGER than HTTPTimeout stretch an attempt to the caller's
// deadline against the documented injection contract. Caller-abort
// discrimination is unaffected: the callers that need it (FetchRemoteConfig,
// callerAbandonedFlush) test the CALLER's original context, which an SDK-cap
// expiry never marks.
func contextWithDefaultTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}
