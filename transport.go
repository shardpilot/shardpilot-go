package shardpilot

import (
	"bytes"
	"context"
	"encoding/json"
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

	// RetryAfter is the server's Retry-After response header (whole seconds,
	// clamped to 24h), or zero when the header was absent or unparseable. The
	// background flush worker defers its next automatic publish attempt by at
	// least this long; synchronous Track callers receive it here and decide
	// for themselves.
	RetryAfter time.Duration
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
	client          *http.Client
}

func newHTTPTransport(cfg Config) *httpTransport {
	base := strings.TrimRight(cfg.IngestURL, "/")
	return &httpTransport{
		endpoint:        base + "/v1/events:batch",
		consentEndpoint: base + "/v1/consent",
		token:           cfg.Token,
		client: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
	}
}

func (t *httpTransport) Publish(ctx context.Context, request batchRequest) (batchResult, error) {
	var result batchResult
	if err := t.postJSON(ctx, t.endpoint, request, &result); err != nil {
		return batchResult{}, err
	}
	return result, nil
}

func (t *httpTransport) PublishConsent(ctx context.Context, request consentRequest) (consentResult, error) {
	var result consentResult
	if err := t.postJSON(ctx, t.consentEndpoint, request, &result); err != nil {
		return consentResult{}, err
	}
	return result, nil
}

func (t *httpTransport) postJSON(ctx context.Context, endpoint string, request any, result any) error {
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
	statusErr := &HTTPStatusError{
		StatusCode: response.StatusCode,
		RetryAfter: parseRetryAfter(response.Header.Get("Retry-After")),
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

// parseRetryAfter reads a whole-seconds Retry-After header value. Only the
// delta-seconds form is honored — the HTTP-date form returns zero so the
// client falls back to its own cadence — and the result is clamped to
// maxRetryAfter. Absent, negative, or malformed values yield zero.
func parseRetryAfter(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}
	seconds, err := strconv.ParseInt(header, 10, 64)
	if err != nil || seconds <= 0 {
		return 0
	}
	retryAfter := time.Duration(seconds) * time.Second
	if retryAfter > maxRetryAfter {
		return maxRetryAfter
	}
	return retryAfter
}

func contextWithDefaultTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if _, ok := parent.Deadline(); ok {
		return parent, func() {}
	}
	return context.WithTimeout(parent, timeout)
}
