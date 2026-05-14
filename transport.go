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
}

type batchResult struct {
	Accepted   int `json:"accepted"`
	Rejected   int `json:"rejected"`
	Duplicates int `json:"duplicates"`
}

type httpTransport struct {
	endpoint string
	token    string
	client   *http.Client
}

func newHTTPTransport(cfg Config) *httpTransport {
	return &httpTransport{
		endpoint: strings.TrimRight(cfg.IngestURL, "/") + "/v1/events:batch",
		token:    cfg.Token,
		client: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
	}
}

func (t *httpTransport) Publish(ctx context.Context, request batchRequest) (batchResult, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return batchResult{}, fmt.Errorf("encode shardpilot batch: %w", err)
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(payload))
	if err != nil {
		return batchResult{}, fmt.Errorf("create shardpilot ingest request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+t.token)

	response, err := t.client.Do(httpRequest)
	if err != nil {
		return batchResult{}, fmt.Errorf("send shardpilot ingest request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return batchResult{}, fmt.Errorf("shardpilot ingest returned status %d", response.StatusCode)
	}

	var result batchResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return batchResult{}, fmt.Errorf("decode shardpilot ingest response: %w", err)
	}
	return result, nil
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
