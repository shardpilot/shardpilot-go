package shardpilot

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/shardpilot/shardpilot-go/internal/sdkversion"
)

// Every request this SDK makes carries X-ShardPilot-SDK: go/<Version>.

func TestIngestAndConsentRequestsCarryTheSDKHeader(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = r.Header.Get("X-ShardPilot-SDK")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/events:batch":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
		case "/v1/consent":
			_, _ = w.Write([]byte(`{"recorded":true,"replayed":false}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client := newSchemaRevisionTestClient(t, server.URL, func(cfg *Config) { cfg.UserID = "user-actor" })
	if err := client.Track(context.Background(), Event{Name: "server_event"}); err != nil {
		t.Fatalf("Track: %v", err)
	}
	client.SetConsent(true)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, route := range []string{"/v1/events:batch", "/v1/consent"} {
		if got := seen[route]; got != sdkversion.HeaderValue {
			t.Fatalf("%s: X-ShardPilot-SDK = %q, want %q", route, got, sdkversion.HeaderValue)
		}
	}
}

func TestRemoteConfigRequestCarriesTheSDKHeader(t *testing.T) {
	seen := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/config/v1/") {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		seen <- r.Header.Get("X-ShardPilot-SDK")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":1,"values":{"k":"v"}}`))
	}))
	defer server.Close()
	client := newRemoteConfigClient(t, server.URL, "", "anon-1")
	defer client.Close(context.Background())
	if _, err := client.SetConsent(true); err != nil {
		t.Fatal(err)
	}
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("FetchRemoteConfig: %v", err)
	}
	if got := <-seen; got != sdkversion.HeaderValue {
		t.Fatalf("X-ShardPilot-SDK = %q, want %q", got, sdkversion.HeaderValue)
	}
}

func TestExperimentRequestsCarryTheSDKHeader(t *testing.T) {
	rig := newExpHopRig(t, nil)
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.cycle()
	if got := rig.script.request(0).Header.Get("X-ShardPilot-SDK"); got != sdkversion.HeaderValue {
		t.Fatalf("assignment: X-ShardPilot-SDK = %q, want %q", got, sdkversion.HeaderValue)
	}
	requests := rig.script.apply.requestsSoFar()
	if len(requests) != 1 {
		t.Fatalf("apply requests = %d, want 1", len(requests))
	}
	if got := requests[0].header.Get("X-ShardPilot-SDK"); got != sdkversion.HeaderValue {
		t.Fatalf("exposure apply: X-ShardPilot-SDK = %q, want %q", got, sdkversion.HeaderValue)
	}
}
