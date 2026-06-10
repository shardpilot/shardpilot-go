package shardpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type transport interface {
	Publish(ctx context.Context, request batchRequest) (batchResult, error)
	PublishConsent(ctx context.Context, request consentRequest) (consentResult, error)
}

type batchResult struct {
	Accepted   int `json:"accepted"`
	Rejected   int `json:"rejected"`
	Duplicates int `json:"duplicates"`
}

type HTTPStatusError struct {
	StatusCode int
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("shardpilot ingest returned status %d", e.StatusCode)
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
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return &HTTPStatusError{StatusCode: response.StatusCode}
	}

	if err := json.NewDecoder(response.Body).Decode(result); err != nil {
		return fmt.Errorf("decode shardpilot ingest response: %w", err)
	}
	return nil
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
