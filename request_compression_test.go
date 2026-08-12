package shardpilot

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// capturedRequest is what the fake ingest server saw: the header, the bytes on
// the wire, and the bytes after decoding. Every assertion below is made
// against the WIRE, because the point of the change is what leaves the
// process, not what the SDK believes it sent.
type capturedRequest struct {
	contentEncoding string
	wireBytes       int
	decodedBytes    int
	decoded         []byte
}

// compressionCapturingServer decodes gzip request bodies the way the ingest
// server does and records what it saw.
func compressionCapturingServer(t *testing.T, respond func(w http.ResponseWriter, r *http.Request, decoded []byte)) (*httptest.Server, func() []capturedRequest) {
	t.Helper()

	var mu sync.Mutex
	var captured []capturedRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wire, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}

		record := capturedRequest{contentEncoding: r.Header.Get("Content-Encoding"), wireBytes: len(wire)}
		decoded := wire
		if record.contentEncoding == "gzip" {
			reader, err := gzip.NewReader(strings.NewReader(string(wire)))
			if err != nil {
				t.Errorf("body declared gzip but is not a gzip stream: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			decoded, err = io.ReadAll(reader)
			if err != nil {
				t.Errorf("decode gzip body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if err := reader.Close(); err != nil {
				t.Errorf("gzip trailer: %v", err)
			}
		}
		record.decodedBytes = len(decoded)
		record.decoded = decoded

		mu.Lock()
		captured = append(captured, record)
		mu.Unlock()

		respond(w, r, decoded)
	}))

	return server, func() []capturedRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]capturedRequest(nil), captured...)
	}
}

// eventsIn counts the events carried by every captured request. The flush
// worker drains the queue opportunistically, so the number of PUBLISHES a
// batch of N events becomes is not deterministic — the assertions below are on
// what every request carried and on the total delivered, never on the count.
func eventsIn(t *testing.T, requests []capturedRequest) int {
	t.Helper()

	total := 0
	for i, request := range requests {
		var decoded batchRequest
		if err := json.Unmarshal(request.decoded, &decoded); err != nil {
			t.Fatalf("request %d body is not a batch: %v", i, err)
		}
		total += len(decoded.Events)
	}

	return total
}

func acceptBatch(w http.ResponseWriter, _ *http.Request, decoded []byte) {
	var request batchRequest
	if err := json.Unmarshal(decoded, &request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(fmt.Sprintf(`{"accepted":%d,"rejected":0,"duplicates":0}`, len(request.Events))))
}

func newCompressionTestClient(t *testing.T, ingestURL string, mutate func(*Config)) *Client {
	t.Helper()

	cfg := Config{
		IngestURL:     ingestURL,
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		AppVersion:    "0.1.0",
		AppBuild:      "100",
		Platform:      "linux",
		BatchSize:     200,
		BufferSize:    400,
		FlushInterval: time.Hour,
		HTTPTimeout:   5 * time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

// enqueueBatch queues count events carrying enough props to look like real
// telemetry rather than a bare envelope.
func enqueueBatch(t *testing.T, client *Client, count int) {
	t.Helper()

	for i := range count {
		err := client.Enqueue(Event{
			ID:   fmt.Sprintf("evt-compression-%d", i),
			Name: "level_complete",
			Props: map[string]any{
				"level_id":     fmt.Sprintf("world-3-stage-%d", i%12),
				"duration_ms":  41200 + i,
				"attempts":     1 + i%4,
				"score":        18400 + i*7,
				"difficulty":   "normal",
				"input_device": "gamepad",
			},
		})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
}

// TestPublishCompressesBatchBodies is A6's SDK-side positive: a realistic
// batch leaves the process gzipped, the server reads it, and the byte-count
// delta is the acceptance artifact.
func TestPublishCompressesBatchBodies(t *testing.T) {
	server, captured := compressionCapturingServer(t, acceptBatch)
	defer server.Close()

	client := newCompressionTestClient(t, server.URL, nil)
	defer client.Close(context.Background())

	enqueueBatch(t, client, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	requests := captured()
	if len(requests) == 0 {
		t.Fatal("no requests reached the server")
	}

	wire, plain, compressedRequests := 0, 0, 0
	for i, request := range requests {
		// The contract is "compress iff the body is worth compressing", and
		// the flush worker decides how many events each publish carries, so
		// the assertion is per-request against the threshold rather than
		// against a publish count the worker is free to change.
		if request.decodedBytes >= compressionMinimumBytes {
			if request.contentEncoding != "gzip" {
				t.Fatalf("request %d carried %d bytes with Content-Encoding %q, want gzip",
					i, request.decodedBytes, request.contentEncoding)
			}
			if request.wireBytes >= request.decodedBytes {
				t.Fatalf("request %d: compression did not shrink the body: %d on the wire vs %d decoded",
					i, request.wireBytes, request.decodedBytes)
			}
			compressedRequests++
		} else if request.contentEncoding != "" {
			t.Fatalf("request %d carried only %d bytes but was compressed (%q); the threshold is %d",
				i, request.decodedBytes, request.contentEncoding, compressionMinimumBytes)
		}
		wire += request.wireBytes
		plain += request.decodedBytes
	}
	if compressedRequests == 0 {
		t.Fatalf("no publish crossed the %d-byte threshold, so compression was never exercised",
			compressionMinimumBytes)
	}
	t.Logf("byte-count delta over %d publish(es): %d decoded -> %d on the wire (%.1f%%, %.1fx smaller)",
		len(requests), plain, wire, 100*float64(wire)/float64(plain), float64(plain)/float64(wire))

	// The decoded bytes must be the batch the SDK meant to send — a
	// compression lane that truncated would still shrink the body.
	if got := eventsIn(t, requests); got != 100 {
		t.Fatalf("expected 100 events through the compressed bodies, got %d", got)
	}

	stats := client.Snapshot()
	if stats.Accepted != 100 {
		t.Fatalf("expected 100 accepted, got %d", stats.Accepted)
	}
}

// TestPublishSendsSmallBodiesUncompressed pins the threshold. gzip costs 18
// bytes of framing before deflate's own overhead, so compressing a
// single-event batch spends CPU to make the request bigger.
func TestPublishSendsSmallBodiesUncompressed(t *testing.T) {
	server, captured := compressionCapturingServer(t, acceptBatch)
	defer server.Close()

	client := newCompressionTestClient(t, server.URL, nil)
	defer client.Close(context.Background())

	if err := client.Enqueue(Event{ID: "evt-tiny", Name: "session_start"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	requests := captured()
	if len(requests) != 1 {
		t.Fatalf("a single-event flush must be one publish, got %d", len(requests))
	}
	if requests[0].contentEncoding != "" {
		t.Fatalf("a %d-byte body was compressed; the threshold is %d bytes",
			requests[0].decodedBytes, compressionMinimumBytes)
	}
	if requests[0].decodedBytes >= compressionMinimumBytes {
		t.Fatalf("fixture is %d bytes, must sit under the %d threshold for this test to mean anything",
			requests[0].decodedBytes, compressionMinimumBytes)
	}
}

// TestDisableRequestCompressionOptsOut pins the escape hatch: with it set, no
// request carries a Content-Encoding regardless of size.
func TestDisableRequestCompressionOptsOut(t *testing.T) {
	server, captured := compressionCapturingServer(t, acceptBatch)
	defer server.Close()

	client := newCompressionTestClient(t, server.URL, func(cfg *Config) {
		cfg.DisableRequestCompression = true
	})
	defer client.Close(context.Background())

	enqueueBatch(t, client, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	requests := captured()
	if len(requests) == 0 {
		t.Fatal("no requests reached the server")
	}
	overThreshold := false
	for i, request := range requests {
		if request.contentEncoding != "" {
			t.Fatalf("request %d: DisableRequestCompression did not stop compression: Content-Encoding %q",
				i, request.contentEncoding)
		}
		if request.wireBytes >= compressionMinimumBytes {
			overThreshold = true
		}
	}
	if !overThreshold {
		t.Fatalf("every request sat under the %d-byte threshold, so the opt-out was never exercised",
			compressionMinimumBytes)
	}
}

// newCompressionTestTransport builds the transport directly. The fallback
// tests drive it rather than the Client because the flush worker decides how
// many events each publish carries, and these tests are about the SEQUENCE of
// attempts — which body was compressed, which was retried — not about
// batching.
func newCompressionTestTransport(t *testing.T, ingestURL string) *httpTransport {
	t.Helper()

	return newHTTPTransport(Config{
		IngestURL:   ingestURL,
		Token:       "test-token",
		HTTPTimeout: 5 * time.Second,
	})
}

// compressionTestBatch is a batch body comfortably over the threshold.
func compressionTestBatch(count int) batchRequest {
	events := make([]eventEnvelope, 0, count)
	for i := range count {
		events = append(events, eventEnvelope{
			EventID:       fmt.Sprintf("evt-fallback-%d", i),
			SchemaVersion: 1,
			EventName:     "level_complete",
			Source:        "backend",
			EventTS:       "2026-08-12T10:00:00Z",
			WorkspaceID:   "workspace-test",
			AppID:         "app-test",
			EnvironmentID: "develop",
		})
	}

	return batchRequest{Events: events}
}

// TestPublishFallsBackWhenServerRefusesCompression is the ordering safety net,
// and the reason compression can be on by default in a PUBLIC SDK.
//
// Customers point this client at their own ingest deployment. An SDK upgrade
// aimed at a server that predates gzip acceptance would otherwise drop every
// batch forever — a transport detail turned into total data loss. Instead the
// first rejection latches compression off for the process and the SAME batch
// is re-sent uncompressed, so the events land.
func TestPublishFallsBackWhenServerRefusesCompression(t *testing.T) {
	var refusals atomic.Int64

	server, captured := compressionCapturingServer(t, func(w http.ResponseWriter, r *http.Request, decoded []byte) {
		if r.Header.Get("Content-Encoding") == "gzip" {
			refusals.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"code":"validation_error","message":"request validation failed",` +
				`"details":[{"field":"content-encoding","code":"unsupported_content_encoding",` +
				`"message":"content encoding gzip is not supported"}]}}`))
			return
		}
		acceptBatch(w, r, decoded)
	})
	defer server.Close()

	transport := newCompressionTestTransport(t, server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := transport.Publish(ctx, compressionTestBatch(100))
	if err != nil {
		t.Fatalf("the fallback must deliver the batch, not surface the refusal: %v", err)
	}
	if result.Accepted != 100 {
		t.Fatalf("expected 100 accepted through the fallback, got %d", result.Accepted)
	}

	requests := captured()
	if len(requests) != 2 {
		t.Fatalf("expected the refused compressed attempt plus one uncompressed retry, got %d requests", len(requests))
	}
	if requests[0].contentEncoding != "gzip" {
		t.Fatalf("first attempt should have been compressed, got %q", requests[0].contentEncoding)
	}
	if requests[1].contentEncoding != "" {
		t.Fatalf("retry should have been uncompressed, got %q", requests[1].contentEncoding)
	}
	if requests[0].decodedBytes != requests[1].decodedBytes {
		t.Fatalf("the retry sent a DIFFERENT body: %d bytes vs %d — the fallback must re-send the same batch",
			requests[1].decodedBytes, requests[0].decodedBytes)
	}

	// The latch: a SECOND publish must not pay the refused round-trip again.
	if _, err := transport.Publish(ctx, compressionTestBatch(100)); err != nil {
		t.Fatalf("second publish: %v", err)
	}

	requests = captured()
	if len(requests) != 3 {
		t.Fatalf("the second publish should have cost exactly one request, got %d total", len(requests))
	}
	if requests[2].contentEncoding != "" {
		t.Fatalf("compression was not latched off: third request carried %q", requests[2].contentEncoding)
	}
	if refusals.Load() != 1 {
		t.Fatalf("the server should have refused exactly once, got %d", refusals.Load())
	}
}

// TestOrdinaryValidationFailureDoesNotDisableCompression is the CONTROL for
// the fallback above, and the reason it matches on detail codes rather than on
// the bare 400.
//
// A status-only match would let any ordinary validation failure — a bad event
// name, a scope mismatch — silently switch compression off for the rest of the
// process. That is a transport change nobody asked for, made in response to an
// unrelated defect, and it would hide the real one behind it.
func TestOrdinaryValidationFailureDoesNotDisableCompression(t *testing.T) {
	var rejectNext atomic.Bool
	rejectNext.Store(true)

	server, captured := compressionCapturingServer(t, func(w http.ResponseWriter, r *http.Request, decoded []byte) {
		if rejectNext.Swap(false) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"code":"validation_error","message":"request validation failed",` +
				`"details":[{"field":"events.0.event_name","code":"unknown_event",` +
				`"message":"event name is not in the tracking plan"}]}}`))
			return
		}
		acceptBatch(w, r, decoded)
	})
	defer server.Close()

	transport := newCompressionTestTransport(t, server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := transport.Publish(ctx, compressionTestBatch(100)); err == nil {
		t.Fatal("the rejected publish should have surfaced its validation error")
	}

	// The next publish must still be compressed — and there must be no
	// uncompressed retry of the first, since the failure was not about
	// encoding.
	if _, err := transport.Publish(ctx, compressionTestBatch(100)); err != nil {
		t.Fatalf("second publish: %v", err)
	}

	requests := captured()
	if len(requests) != 2 {
		t.Fatalf("expected exactly the rejected publish and the next one — an unrelated 400 must not trigger an uncompressed retry; got %d requests", len(requests))
	}
	for i, request := range requests {
		if request.contentEncoding != "gzip" {
			t.Fatalf("request %d was sent uncompressed (%q): an unrelated validation failure disabled compression",
				i, request.contentEncoding)
		}
	}
}

// TestConsentWritesAreCompressedWhenLargeEnough pins that the lane covers the
// consent route too and is driven by size rather than by route. A single
// consent decision is a few hundred bytes and stays uncompressed; the same
// helper compresses it once it is worth compressing.
func TestConsentWritesAreCompressedWhenLargeEnough(t *testing.T) {
	small := []byte(`{"workspace_id":"workspace-test","categories":{"analytics":true}}`)
	if _, ok := compressRequestBody(small); ok {
		t.Fatalf("a %d-byte consent decision must not be compressed", len(small))
	}

	large := []byte(`{"workspace_id":"workspace-test","reason":"` + strings.Repeat("consent-", 400) + `"}`)
	encoded, ok := compressRequestBody(large)
	if !ok {
		t.Fatalf("a %d-byte body must be compressed", len(large))
	}
	if len(encoded) >= len(large) {
		t.Fatalf("compression did not shrink the body: %d vs %d", len(encoded), len(large))
	}
}

// TestCompressRequestBodyRefusesToGrowABody pins the second half of the
// threshold rule: a body over the size threshold that gzip cannot shrink is
// sent uncompressed. High-entropy payloads come back LARGER, and sending them
// would spend CPU to make the request worse.
func TestCompressRequestBodyRefusesToGrowABody(t *testing.T) {
	incompressible := make([]byte, 4<<10)
	state := uint32(0x9e3779b9)
	for i := range incompressible {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		incompressible[i] = byte(state)
	}

	encoded, ok := compressRequestBody(incompressible)
	if ok {
		t.Fatalf("an incompressible %d-byte body was compressed to %d bytes and sent anyway",
			len(incompressible), len(encoded))
	}
}

// TestCompressedBodyIsAReadableGzipStream pins the framing itself. The server
// rejects a body that declares gzip and is not one, so a writer that is not
// closed — leaving the trailer unwritten — turns every publish into a
// permanent 400. It is the single easiest way to get this wrong, and the
// pooled writer makes it easier still.
func TestCompressedBodyIsAReadableGzipStream(t *testing.T) {
	payload := []byte(`{"events":[` + strings.Repeat(`{"event_name":"level_complete","score":1840},`, 60) + `{}]}`)

	for round := range 5 {
		encoded, ok := compressRequestBody(payload)
		if !ok {
			t.Fatalf("round %d: body was not compressed", round)
		}

		reader, err := gzip.NewReader(strings.NewReader(string(encoded)))
		if err != nil {
			t.Fatalf("round %d: not a gzip stream: %v", round, err)
		}
		decoded, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("round %d: decode: %v", round, err)
		}
		// Close verifies the CRC and length trailer — the part a reused
		// writer would corrupt.
		if err := reader.Close(); err != nil {
			t.Fatalf("round %d: gzip trailer: %v", round, err)
		}
		if string(decoded) != string(payload) {
			t.Fatalf("round %d: round-trip changed the body: %d bytes out, %d in",
				round, len(decoded), len(payload))
		}
	}
}

// TestConcurrentCompressionIsSafe pins the pooled writer under the concurrency
// the SDK actually has: the flush worker, synchronous Track callers and the
// consent outbox all publish at once. A writer handed to two goroutines
// produces interleaved garbage that decodes to nothing recognisable.
func TestConcurrentCompressionIsSafe(t *testing.T) {
	payload := []byte(`{"events":[` + strings.Repeat(`{"event_name":"level_complete","score":1840},`, 60) + `{}]}`)

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			encoded, ok := compressRequestBody(payload)
			if !ok {
				errs <- fmt.Errorf("body was not compressed")
				return
			}
			reader, err := gzip.NewReader(strings.NewReader(string(encoded)))
			if err != nil {
				errs <- fmt.Errorf("not a gzip stream: %w", err)
				return
			}
			decoded, err := io.ReadAll(reader)
			if err != nil {
				errs <- fmt.Errorf("decode: %w", err)
				return
			}
			if err := reader.Close(); err != nil {
				errs <- fmt.Errorf("trailer: %w", err)
				return
			}
			if string(decoded) != string(payload) {
				errs <- fmt.Errorf("round-trip changed the body")
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent compression: %v", err)
	}
}

// TestServerRefusedCompressionMatchesOnlyEncodingCodes pins the fallback
// predicate directly, including the shapes that must NOT trip it.
func TestServerRefusedCompressionMatchesOnlyEncodingCodes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "unsupported coding",
			err:  &HTTPStatusError{StatusCode: 400, Details: []ErrorDetail{{Code: "unsupported_content_encoding"}}},
			want: true,
		},
		{
			name: "unreadable stream",
			err:  &HTTPStatusError{StatusCode: 400, Details: []ErrorDetail{{Code: "invalid_content_encoding"}}},
			want: true,
		},
		{
			name: "encoding code among others",
			err: &HTTPStatusError{StatusCode: 400, Details: []ErrorDetail{
				{Code: "unknown_field"}, {Code: "unsupported_content_encoding"},
			}},
			want: true,
		},
		{
			name: "ordinary validation failure",
			err:  &HTTPStatusError{StatusCode: 400, Details: []ErrorDetail{{Code: "unknown_event"}}},
			want: false,
		},
		{
			name: "bare 400 with no details",
			err:  &HTTPStatusError{StatusCode: 400},
			want: false,
		},
		{
			name: "over-cap body",
			err:  &HTTPStatusError{StatusCode: 400, Details: []ErrorDetail{{Code: "request_too_large"}}},
			want: false,
		},
		{name: "no error", err: nil, want: false},
		{name: "transport error", err: fmt.Errorf("connection reset"), want: false},
	}

	for _, testCase := range cases {
		if got := serverRefusedCompression(testCase.err); got != testCase.want {
			t.Fatalf("%s: serverRefusedCompression = %v, want %v", testCase.name, got, testCase.want)
		}
	}
}

// BenchmarkCompressBatchBody reports what the compression costs per publish,
// which is the number that decides whether a game SDK may do this by default.
func BenchmarkCompressBatchBody(b *testing.B) {
	events := make([]map[string]any, 0, 100)
	for i := range 100 {
		events = append(events, map[string]any{
			"event_id": fmt.Sprintf("evt-bench-%d", i),
			"event_ts": "2026-08-12T10:00:00Z",
			"props": map[string]any{
				"level_id":    fmt.Sprintf("world-3-stage-%d", i%12),
				"duration_ms": 41200 + i,
				"score":       18400 + i*7,
			},
		})
	}
	payload, err := json.Marshal(map[string]any{"events": events})
	if err != nil {
		b.Fatalf("marshal: %v", err)
	}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		encoded, ok := compressRequestBody(payload)
		if !ok {
			b.Fatal("body was not compressed")
		}
		_ = encoded
	}
}

// TestTheUncompressedRetryGetsItsOwnAttemptTimeout pins that the fallback is
// a real fallback and not a formality.
//
// The refusal is the response of a server that does NOT understand the coding,
// and there is no reason such a server answers quickly — an old deployment
// behind a proxy can spend most of the request budget deciding. The attempt
// timeout used to be applied to the whole publish by the CALLER, so whatever
// the refusal spent came out of the retry: the retry then timed out, the
// synchronous Track failed, and the advertised compatibility path did not
// deliver on the one occasion it exists for.
//
// It is driven through a real Client rather than the transport directly,
// because the caller is where the old budget was applied — a transport-level
// test passes a fresh context and cannot see the defect at all.
//
// The server burns 80% of HTTPTimeout before refusing, so a retry sharing that
// budget cannot finish.
func TestTheUncompressedRetryGetsItsOwnAttemptTimeout(t *testing.T) {
	const attemptTimeout = 600 * time.Millisecond

	var uncompressed atomic.Int64
	server, _ := compressionCapturingServer(t, func(w http.ResponseWriter, r *http.Request, decoded []byte) {
		if r.Header.Get("Content-Encoding") == "gzip" {
			time.Sleep(attemptTimeout * 8 / 10)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"code":"validation_error","message":"request validation failed",` +
				`"details":[{"field":"content-encoding","code":"unsupported_content_encoding",` +
				`"message":"content encoding gzip is not supported"}]}}`))
			return
		}
		// The retry takes half the attempt budget: comfortably inside a FRESH
		// one, and comfortably OUTSIDE the fifth the refusal left over. Both
		// numbers matter — a retry that answers instantly would succeed on the
		// leftovers too, and the test would pass against the defect.
		time.Sleep(attemptTimeout / 2)
		uncompressed.Add(1)
		acceptBatch(w, r, decoded)
	})
	defer server.Close()

	client, err := NewClient(Config{
		IngestURL:     server.URL,
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
		FlushInterval: time.Hour,
		HTTPTimeout:   attemptTimeout,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = client.Close(context.Background()) }()

	// A synchronous Track publishes on the CALLER's goroutine, under the
	// caller's context — which is where the shared budget used to be applied.
	// The props push the single-event body over the compression threshold.
	if err := client.Track(context.Background(), Event{
		ID:    "evt-slow-refusal-1",
		Name:  "level_complete",
		Props: map[string]any{"blob": strings.Repeat("compressible-", 200)},
	}); err != nil {
		t.Fatalf("the uncompressed retry must have its own attempt budget: %v", err)
	}
	if got := uncompressed.Load(); got != 1 {
		t.Fatalf("expected exactly one uncompressed retry to land, got %d", got)
	}
}

// TestACallerDeadlineStillBoundsBothAttempts is the other half: giving each
// attempt its own budget must not let the transport outlive the caller's own
// deadline. context.WithTimeout never EXTENDS an earlier deadline, and this is
// the assertion that keeps it that way.
func TestACallerDeadlineStillBoundsBothAttempts(t *testing.T) {
	server, _ := compressionCapturingServer(t, func(w http.ResponseWriter, r *http.Request, decoded []byte) {
		time.Sleep(2 * time.Second)
		acceptBatch(w, r, decoded)
	})
	defer server.Close()

	transport := newHTTPTransport(Config{
		IngestURL:   server.URL,
		Token:       "test-token",
		HTTPTimeout: time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	started := time.Now()
	if _, err := transport.Publish(ctx, compressionTestBatch(100)); err == nil {
		t.Fatal("expected the caller deadline to abort the publish")
	}
	// The deadline is 200ms and the server answers at 2s, so anything near a
	// second means the transport waited for the server rather than the caller.
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("the caller deadline did not bound the attempt: took %v", elapsed)
	}
}
