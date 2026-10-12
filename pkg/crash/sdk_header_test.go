package crash

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shardpilot/shardpilot-go/internal/sdkversion"
)

func TestCrashIngestCarriesTheSDKHeader(t *testing.T) {
	seen := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get("X-ShardPilot-SDK")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	client, err := NewClient(ClientOptions{CrashURL: server.URL, CrashKey: "workspace-api-key-test", Sampler: alwaysSampler{}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.Emit(context.Background(), validEvent(t)); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if got := <-seen; got != sdkversion.HeaderValue {
		t.Fatalf("X-ShardPilot-SDK = %q, want %q", got, sdkversion.HeaderValue)
	}
}
