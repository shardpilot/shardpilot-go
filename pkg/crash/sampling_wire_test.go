package crash

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientSamplingWire(t *testing.T) {
	admittingRate := func(every uint64) *rateSampler {
		s := &rateSampler{every: every}
		if every > 1 {
			s.counter.Store(every - 1)
		}
		return s
	}
	for _, tc := range []struct {
		name      string
		sampler   Sampler
		fatal     bool
		calls     int
		wantPosts int
		wantRate  uint64
	}{
		{"default", nil, false, 10, 1, 10},
		{"fatal-default-no-rate", nil, true, 1, 1, 0},
		{"fatal-bypasses-sampler", neverSampler{}, true, 1, 1, 0},
		{"custom-rate-unknown", alwaysSampler{}, false, 1, 1, 0},
		{"sampled-out", neverSampler{}, false, 1, 0, 0},
		{"minimum-known-rate", admittingRate(1), false, 1, 1, 1},
		{"maximum-known-rate", admittingRate(1000000), false, 1, 1, 1000000},
		{"zero-rate-unknown", admittingRate(0), false, 1, 1, 0},
		{"out-of-range-rate-unknown", admittingRate(1000001), false, 1, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bodies := make(chan []byte, tc.calls+1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				bodies <- body
				w.WriteHeader(http.StatusAccepted)
			}))
			defer server.Close()
			client, err := NewClient(ClientOptions{IngestURL: server.URL, APIKey: "workspace-api-key-test", Sampler: tc.sampler})
			if err != nil {
				t.Fatal(err)
			}
			event := validEvent(t)
			// Caller data with these names remains nested data, never a stamp.
			event.Metadata = map[string]string{"fatal": "true", "non_fatal_sample_one_in": "123"}
			for i := 0; i < tc.calls; i++ {
				if tc.fatal {
					err = client.EmitFatal(context.Background(), event)
				} else {
					err = client.Emit(context.Background(), event)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(bodies) != tc.wantPosts {
				t.Fatalf("posts = %d, want %d", len(bodies), tc.wantPosts)
			}
			for len(bodies) > 0 {
				assertSamplingWire(t, <-bodies, tc.fatal, tc.wantRate)
			}
		})
	}
}

func assertSamplingWire(t *testing.T, body []byte, fatal bool, rate uint64) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatal(err)
	}
	wantFatal, _ := json.Marshal(fatal)
	if !bytes.Equal(fields["fatal"], wantFatal) {
		t.Fatalf("fatal stamp = %s, want %s", fields["fatal"], wantFatal)
	}
	got, present := fields["non_fatal_sample_one_in"]
	if rate == 0 {
		if present {
			t.Fatalf("unknown/fatal rate must be absent, got %s", got)
		}
	} else {
		want, _ := json.Marshal(rate)
		if !bytes.Equal(got, want) {
			t.Fatalf("sample rate = %s, want %s", got, want)
		}
	}
}

func TestEventSamplingFieldsStaySDKOwned(t *testing.T) {
	event := validEvent(t)
	original, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	// A caller can decode arbitrary JSON into Event, but cannot add wire stamps
	// to this public value. Legacy Event encodings remain byte-identical.
	var injected map[string]json.RawMessage
	if err := json.Unmarshal(original, &injected); err != nil {
		t.Fatal(err)
	}
	injected["fatal"] = json.RawMessage(`true`)
	injected["non_fatal_sample_one_in"] = json.RawMessage(`123`)
	input, _ := json.Marshal(injected)
	if err := json.Unmarshal(input, &event); err != nil {
		t.Fatal(err)
	}
	after, err := json.Marshal(event)
	if err != nil || !bytes.Equal(original, after) {
		t.Fatalf("caller changed legacy Event encoding with sampling fields: %v", err)
	}
}

func TestClientSamplingRetryKeepsCaptureBytes(t *testing.T) {
	var bodies [][]byte
	var client *Client
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		bodies = append(bodies, body)
		status := http.StatusAccepted
		if len(bodies) == 1 {
			// Synchronous transport: change the sampler after capture, before
			// retry, without a timing race or a new public sampler API.
			client.sampler.(*rateSampler).every = 50
			status = http.StatusServiceUnavailable
		}
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})
	var err error
	client, err = NewClient(ClientOptions{IngestURL: "https://ingest.example.invalid", APIKey: "workspace-api-key-test",
		HTTPClient: &http.Client{Transport: transport}, MaxAttempts: 2, RetryBackoff: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := client.Emit(context.Background(), validEvent(t)); err != nil {
			t.Fatal(err)
		}
	}
	if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) {
		t.Fatal("retry did not preserve the captured wire bytes")
	}
	assertSamplingWire(t, bodies[0], false, 10)
}

type retainingSampler struct{ event Event }

func (s *retainingSampler) ShouldEmit(event Event) bool {
	s.event = event
	return true
}

func TestClientRetryDoesNotReencodeSamplerEvent(t *testing.T) {
	sampler := &retainingSampler{}
	var bodies [][]byte
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		bodies = append(bodies, body)
		status := http.StatusAccepted
		if len(bodies) == 1 {
			sampler.event.Metadata["capture"] = "after"
			status = http.StatusServiceUnavailable
		}
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})
	client, err := NewClient(ClientOptions{IngestURL: "https://ingest.example.invalid", APIKey: "workspace-api-key-test",
		Sampler: sampler, HTTPClient: &http.Client{Transport: transport}, MaxAttempts: 2, RetryBackoff: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	event := validEvent(t)
	event.Metadata = map[string]string{"capture": "before"}
	if err := client.Emit(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || !bytes.Contains(bodies[0], []byte(`"capture":"before"`)) || !bytes.Equal(bodies[0], bodies[1]) {
		t.Fatal("retry reencoded the sampler's retained Event instead of the captured body")
	}
	assertSamplingWire(t, bodies[0], false, 0)
}
