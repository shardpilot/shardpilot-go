package shardpilot

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const consentConfigPath = "/config/v1/workspace-test/develop"

func newConsentConfigClient(t *testing.T, serverURL, cachePath string, configure ...func(*Config)) *Client {
	t.Helper()
	cfg := Config{
		IngestURL: serverURL, RemoteConfigURL: serverURL,
		Token: "test-token", APIKey: "test-rc-key",
		WorkspaceID: "workspace-test", AppID: "app-test", EnvironmentID: "develop",
		Source: SourceBackend, AnonymousID: "config-anonymous", UserID: "config-host-user",
		RemoteConfigCachePath: cachePath, RemoteConfigAttributesEnabled: true,
		FlushInterval: time.Hour, HTTPTimeout: 5 * time.Second,
	}
	for _, apply := range configure {
		apply(&cfg)
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestRemoteConfigConsentRequestIdentity(t *testing.T) {
	for _, state := range []ConsentState{ConsentUnknown, ConsentGranted, ConsentDenied, ConsentDeniedForcedMinor} {
		t.Run(string(state), func(t *testing.T) {
			type capturedRequest struct {
				url, method, body string
				headers           http.Header
			}
			seen := make(chan capturedRequest, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasPrefix(r.URL.Path, "/config/v1/") {
					w.WriteHeader(http.StatusAccepted)
					return
				}
				body, _ := io.ReadAll(r.Body)
				seen <- capturedRequest{"http://" + r.Host + r.RequestURI, r.Method, string(body), r.Header.Clone()}
				_, _ = w.Write([]byte(`{"values":{"available":true}}`))
			}))
			defer server.Close()
			client := newConsentConfigClient(t, server.URL, "")
			defer client.Close(context.Background())
			if state != ConsentUnknown {
				if _, err := client.SetConsentDecision(ConsentDecision(state)); err != nil {
					t.Fatal(err)
				}
			}
			if got := client.ConsentState(); got != state {
				t.Fatalf("state %q, want %q", got, state)
			}
			client.SetRemoteConfigAttributes(map[string]string{"geo": "US"})
			if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
				t.Fatal(err)
			}
			request := <-seen
			want := server.URL + consentConfigPath
			if state == ConsentGranted {
				want += "/config-anonymous?geo=US"
			}
			if request.url != want {
				t.Errorf("complete request URL = %q, want %q", request.url, want)
			}
			if request.method != http.MethodGet || request.body != "" {
				t.Errorf("method/body = %q/%q", request.method, request.body)
			}
			if request.headers.Get("Authorization") != "Bearer test-rc-key" {
				t.Error("publishable authorization missing")
			}
			headers, err := json.Marshal(request.headers)
			if err != nil {
				t.Fatal(err)
			}
			wire := request.url + string(headers) + request.body
			if strings.Contains(wire, "config-host-user") {
				t.Error("host user ID reached config request")
			}
			if state != ConsentGranted && strings.Contains(wire, "config-anonymous") {
				t.Error("anonymous ID reached non-granted config request")
			}
			if !client.RemoteConfigBool("available", false) {
				t.Error("configuration unavailable")
			}
		})
	}
}

func TestRemoteConfigConsentCacheRestartAndRegrant(t *testing.T) {
	type capturedRequest struct{ path, etag string }
	seen := make(chan capturedRequest, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/config/v1/") {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		seen <- capturedRequest{r.URL.Path, r.Header.Get("If-None-Match")}
		etag, body := `"anonymous"`, `{"values":{"kind":"anonymous"}}`
		if strings.HasSuffix(r.URL.Path, "/config-anonymous") {
			etag, body = `"identified"`, `{"values":{"kind":"identified"}}`
		}
		w.Header().Set("ETag", etag)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	cachePath := filepath.Join(t.TempDir(), "config.json")
	fetch := func(client *Client) (RemoteConfigResult, capturedRequest) {
		t.Helper()
		result, err := client.FetchRemoteConfig(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return result, <-seen
	}
	client := newConsentConfigClient(t, server.URL, cachePath)
	if _, err := client.SetConsent(true); err != nil {
		t.Fatal(err)
	}
	fetch(client)
	result, request := fetch(client)
	if !result.FromCache || request.etag != `"identified"` {
		t.Fatal("identified validator positive control failed")
	}
	if _, err := client.SetConsent(false); err != nil {
		t.Fatal(err)
	}
	result, request = fetch(client)
	if request.path != consentConfigPath || request.etag != "" || result.FromCache {
		t.Errorf("denied fetch reused identified scope: %+v %+v", request, result)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Without the optional consent floor, ordinary decisions are reapplied by
	// the host. A restarted client begins Unknown and selects anonymous config.
	restarted := newConsentConfigClient(t, server.URL, cachePath)
	if restarted.ConsentState() != ConsentUnknown {
		t.Fatal("restart state is not unknown")
	}
	if got := restarted.RemoteConfigString("kind", "missing"); got != "anonymous" {
		t.Errorf("anonymous startup cache = %q", got)
	}
	result, request = fetch(restarted)
	if !result.FromCache || request.etag != `"anonymous"` {
		t.Error("anonymous 304 validator was not retained")
	}
	if _, err := restarted.SetConsent(true); err != nil {
		t.Fatal(err)
	}
	result, request = fetch(restarted)
	if request.path != consentConfigPath+"/config-anonymous" || request.etag != "" || result.FromCache {
		t.Errorf("regrant reused anonymous scope: %+v %+v", request, result)
	}
	if err := restarted.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The durable identified record must not preload into a new Unknown client.
	unknown := newConsentConfigClient(t, server.URL, cachePath)
	defer unknown.Close(context.Background())
	if unknown.RemoteConfigValues() != nil {
		t.Error("unknown startup loaded identified cache")
	}
}

func TestRemoteConfigConsentResponseKeepsDispatchScope(t *testing.T) {
	seen := make(chan string, 1)
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/config/v1/") {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		seen <- r.URL.Path
		<-resume
		w.Header().Set("ETag", `"anonymous-late"`)
		_, _ = w.Write([]byte(`{"values":{"kind":"anonymous-late"}}`))
	}))
	defer server.Close()
	cachePath := filepath.Join(t.TempDir(), "config.json")
	client := newConsentConfigClient(t, server.URL, cachePath)
	defer client.Close(context.Background())
	defer release()
	done := make(chan error, 1)
	go func() { _, err := client.FetchRemoteConfig(context.Background()); done <- err }()
	if path := <-seen; path != consentConfigPath {
		t.Errorf("anonymous dispatch path = %q", path)
	}
	if _, err := client.SetConsent(true); err != nil {
		t.Fatal(err)
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := client.RemoteConfigString("kind", "missing"); got != "anonymous-late" {
		t.Errorf("late anonymous response did not install: %q", got)
	}
	record := readRemoteConfigCacheFile(t, cachePath)
	if want := buildRemoteConfigScope("workspace-test", "develop", "", server.URL); record.Scope != want {
		t.Errorf("response cache lost dispatch scope: %q, want %q", record.Scope, want)
	}
}

func TestRemoteConfigConsentRestoredStateSelectsCache(t *testing.T) {
	for _, state := range []ConsentState{ConsentGranted, ConsentDenied, ConsentDeniedForcedMinor} {
		t.Run(string(state), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasPrefix(r.URL.Path, "/config/v1/") {
					w.WriteHeader(http.StatusAccepted)
					return
				}
				_, _ = w.Write([]byte(`{"values":{"kind":"restored"}}`))
			}))
			defer server.Close()
			directory := t.TempDir()
			cachePath := filepath.Join(directory, "config.json")
			configure := func(cfg *Config) {
				cfg.SpoolDir = filepath.Join(directory, "spool")
				cfg.ConsentFloor = &ConsentFloorConfig{}
			}
			client := newConsentConfigClient(t, server.URL, cachePath, configure)
			if _, err := client.SetConsentDecision(ConsentDecision(state)); err != nil {
				t.Fatal(err)
			}
			if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := client.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			restarted := newConsentConfigClient(t, server.URL, cachePath, configure)
			defer restarted.Close(context.Background())
			if got := restarted.ConsentState(); got != state {
				t.Fatalf("restored consent = %q, want %q", got, state)
			}
			if got := restarted.RemoteConfigString("kind", "missing"); got != "restored" {
				t.Errorf("matching restored consent cache = %q", got)
			}
		})
	}
}
