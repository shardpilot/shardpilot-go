package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// unusedRemoteConfigTransport satisfies the transport interface's remote-
// config method for fakes that never fetch configuration.
type unusedRemoteConfigTransport struct{}

func (unusedRemoteConfigTransport) FetchRemoteConfig(context.Context, remoteConfigRequest) (remoteConfigResponse, error) {
	return remoteConfigResponse{}, errors.New("remote config fetch not faked")
}

func newRemoteConfigClient(t *testing.T, serverURL, cachePath, anonymousID string) *Client {
	t.Helper()
	client, err := NewClient(Config{
		IngestURL:             serverURL,
		Token:                 "test-token",
		WorkspaceID:           "workspace-test",
		AppID:                 "app-test",
		EnvironmentID:         "develop",
		Source:                SourceBackend,
		AnonymousID:           anonymousID,
		APIKey:                "test-rc-key",
		RemoteConfigURL:       serverURL,
		RemoteConfigCachePath: cachePath,
		BatchSize:             2,
		BufferSize:            4,
		FlushInterval:         time.Hour,
		HTTPTimeout:           time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func testRemoteConfigScope(serverURL string) string {
	return buildRemoteConfigScope("workspace-test", "develop", "anon-rc-1", serverURL)
}

func writeRemoteConfigCacheFile(t *testing.T, path string, record rcCache) {
	t.Helper()
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal cache record: %v", err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write cache record: %v", err)
	}
}

func readRemoteConfigCacheFile(t *testing.T, path string) rcCache {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cache record: %v", err)
	}
	var record rcCache
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("unmarshal cache record: %v", err)
	}
	return record
}

func TestRemoteConfigRequestShape(t *testing.T) {
	type seenRequest struct {
		method      string
		escapedPath string
		auth        string
		ifNoneMatch string
		revision    string
	}
	seen := make(chan seenRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- seenRequest{
			method:      r.Method,
			escapedPath: r.URL.EscapedPath(),
			auth:        r.Header.Get("Authorization"),
			ifNoneMatch: r.Header.Get("If-None-Match"),
			revision:    r.Header.Get("X-ShardPilot-Schema-Revision"),
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":1,"values":{"k":"v"}}`))
	}))
	defer server.Close()

	// The identifier carries a space, a slash, and a percent sign: each must
	// arrive as exactly one escaped path segment byte sequence.
	client := newRemoteConfigClient(t, server.URL, "", "anon id/1%x")
	defer client.Close(context.Background())

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("FetchRemoteConfig: %v", err)
	}
	request := <-seen
	if request.method != http.MethodGet {
		t.Fatalf("expected GET, got %s", request.method)
	}
	if want := "/config/v1/workspace-test/develop/anon%20id%2F1%25x"; request.escapedPath != want {
		t.Fatalf("expected path %q, got %q", want, request.escapedPath)
	}
	if request.auth != "Bearer test-rc-key" {
		t.Fatalf("expected the publishable APIKey bearer, got %q", request.auth)
	}
	if request.ifNoneMatch != "" {
		t.Fatalf("expected no If-None-Match without a cache, got %q", request.ifNoneMatch)
	}
	if request.revision != "" {
		t.Fatalf("remote config request must never carry the schema-revision header, got %q", request.revision)
	}
}

func TestRemoteConfigConfigValidation(t *testing.T) {
	base := Config{
		IngestURL:     "http://localhost:8080",
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
	}

	missingKey := base
	missingKey.RemoteConfigURL = "https://config.example.test"
	if _, err := NewClient(missingKey); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig for RemoteConfigURL without APIKey, got %v", err)
	}

	for _, badURL := range []string{
		"config.example.test",              // not absolute
		"http://config.example.test",       // http outside loopback
		"https://config.example.test/path", // path
		"https://config.example.test?x=1",  // query
		"https://config.example.test#frag", // fragment
		"https://user@config.example.test", // user info
		"http://192.168.1.10",              // private without opt-in
	} {
		cfg := base
		cfg.APIKey = "test-rc-key"
		cfg.RemoteConfigURL = badURL
		if _, err := NewClient(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("expected remote config URL %q to be rejected, got %v", badURL, err)
		}
	}

	private := base
	private.APIKey = "test-rc-key"
	private.RemoteConfigURL = "http://192.168.1.10"
	private.AllowInsecurePrivateNetwork = true
	client, err := NewClient(private)
	if err != nil {
		t.Fatalf("expected private remote config URL to be allowed with the opt-in, got %v", err)
	}
	_ = client.Close(context.Background())

	// APIKey alone (no RemoteConfigURL) is valid — there is no both-set
	// credential conflict in this SDK.
	keyOnly := base
	keyOnly.APIKey = "test-rc-key"
	client, err = NewClient(keyOnly)
	if err != nil {
		t.Fatalf("expected APIKey without RemoteConfigURL to be valid, got %v", err)
	}
	if _, err := client.FetchRemoteConfig(context.Background()); err == nil || !strings.Contains(err.Error(), "remote_config_not_configured") {
		t.Fatalf("expected remote_config_not_configured, got %v", err)
	}
	_ = client.Close(context.Background())
}

func TestRemoteConfigClientIDUnavailable(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "")
	defer client.Close(context.Background())

	if _, err := client.FetchRemoteConfig(context.Background()); err == nil || !strings.Contains(err.Error(), "client_id_unavailable") {
		t.Fatalf("expected client_id_unavailable, got %v", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("expected the failure before any network use, got %d requests", requests.Load())
	}
}

func TestRemoteConfigFresh200ServesAndPersists(t *testing.T) {
	body := `{"version":3,"values":{"speed":2.5,"title":"go","dark":false}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"v3"`)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())
	clock := &stubClock{now: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)}
	client.clock = clock

	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil {
		t.Fatalf("FetchRemoteConfig: %v", err)
	}
	if result.FromCache || result.Reason != "" {
		t.Fatalf("expected a fresh result, got %+v", result)
	}
	if !result.HasVersion || result.Version != 3 {
		t.Fatalf("expected wrapper version 3, got %+v", result)
	}
	if result.Values["speed"] != 2.5 || result.Values["title"] != "go" || result.Values["dark"] != false {
		t.Fatalf("unexpected values %+v", result.Values)
	}
	if _, present := result.Values["version"]; present {
		t.Fatalf("wrapper version must never appear as a configuration value")
	}

	record := readRemoteConfigCacheFile(t, cachePath)
	if record.Scope != testRemoteConfigScope(server.URL) {
		t.Fatalf("unexpected persisted scope %q", record.Scope)
	}
	if record.ETag != `"v3"` {
		t.Fatalf("expected the response ETag persisted, got %q", record.ETag)
	}
	if record.Body != body {
		t.Fatalf("expected the raw response text persisted, got %q", record.Body)
	}
	if record.FetchedAtMS != clock.now.UnixMilli() {
		t.Fatalf("expected fetched_at_ms %d, got %d", clock.now.UnixMilli(), record.FetchedAtMS)
	}
	if info, err := os.Stat(cachePath); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("expected a 0600 cache file, got %v %v", info.Mode(), err)
	}
}

func TestRemoteConfig304RenewsStampAndServesCache(t *testing.T) {
	var ifNoneMatch atomic.Value
	var mode atomic.Int64 // 0 = 200, 1 = 304
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode.Load() == 1 {
			ifNoneMatch.Store(r.Header.Get("If-None-Match"))
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte(`{"version":1,"values":{"k":"v"}}`))
	}))
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())
	clock := &stubClock{now: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)}
	client.clock = clock

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("fresh fetch: %v", err)
	}
	firstStamp := readRemoteConfigCacheFile(t, cachePath).FetchedAtMS

	mode.Store(1)
	clock.now = clock.now.Add(10 * time.Second)
	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil {
		t.Fatalf("revalidation fetch: %v", err)
	}
	if !result.FromCache || result.Reason != "" {
		t.Fatalf("expected a clean cache-served revalidation, got %+v", result)
	}
	if result.Values["k"] != "v" || !result.HasVersion || result.Version != 1 {
		t.Fatalf("unexpected revalidated result %+v", result)
	}
	if got := ifNoneMatch.Load(); got != `"v1"` {
		t.Fatalf("expected If-None-Match %q, got %q", `"v1"`, got)
	}
	record := readRemoteConfigCacheFile(t, cachePath)
	if record.FetchedAtMS != firstStamp+10_000 {
		t.Fatalf("expected the freshness stamp renewed to %d, got %d", firstStamp+10_000, record.FetchedAtMS)
	}
	if record.ETag != `"v1"` {
		t.Fatalf("expected the ETag kept, got %q", record.ETag)
	}
}

// remoteConfigScriptServer answers each request from the front of script;
// the last entry repeats.
type rcScriptStep struct {
	status  int
	body    string
	headers map[string]string
	drop    bool // close the connection without responding (transport error)
}

func newRCScriptServer(t *testing.T, requests *atomic.Int64, script *atomic.Value) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests != nil {
			requests.Add(1)
		}
		step := script.Load().(rcScriptStep)
		if step.drop {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		for key, value := range step.headers {
			w.Header().Set(key, value)
		}
		if step.status != http.StatusOK {
			w.WriteHeader(step.status)
		}
		_, _ = w.Write([]byte(step.body))
	}))
}

func TestRemoteConfigTransientOutcomesServeCacheThenFail(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"version":1,"values":{"k":"v"}}`, headers: map[string]string{"ETag": `"v1"`}})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	cacheBefore, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}

	transients := []struct {
		step   rcScriptStep
		reason string
	}{
		{rcScriptStep{status: 408, body: `{}`}, "transient_408"},
		{rcScriptStep{status: 500, body: `oops`}, "transient_500"},
		{rcScriptStep{status: 503, body: ``}, "transient_503"},
		{rcScriptStep{drop: true}, "http_0"},
		{rcScriptStep{status: 200, body: `not json`}, "malformed_response"},
		{rcScriptStep{status: 200, body: `[]`}, "malformed_response"},
		{rcScriptStep{status: 200, body: `{"pad":"` + strings.Repeat("a", rcMaxBodyBytes) + `"}`}, "malformed_response"},
	}
	for _, tc := range transients {
		script.Store(tc.step)
		result, err := client.FetchRemoteConfig(context.Background())
		if err != nil {
			t.Fatalf("%s: expected the cache served, got error %v", tc.reason, err)
		}
		if !result.FromCache || result.Reason != tc.reason {
			t.Fatalf("expected cache-served %s, got %+v", tc.reason, result)
		}
		if result.Values["k"] != "v" {
			t.Fatalf("%s: unexpected values %+v", tc.reason, result.Values)
		}
	}

	cacheAfter, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	if string(cacheBefore) != string(cacheAfter) {
		t.Fatalf("transient outcomes must never disturb the cache record")
	}

	// With no usable cache the same outcomes hard-fail with the same codes.
	bare := newRemoteConfigClient(t, server.URL, "", "anon-rc-2")
	defer bare.Close(context.Background())
	script.Store(rcScriptStep{status: 503, body: ``})
	if _, err := bare.FetchRemoteConfig(context.Background()); err == nil || !strings.Contains(err.Error(), "transient_503") {
		t.Fatalf("expected transient_503 failure without a cache, got %v", err)
	}
}

func TestRemoteConfigUnauthorizedFailsClosed(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"v"}}`, headers: map[string]string{"ETag": `"v1"`}})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	cacheBefore, _ := os.ReadFile(cachePath)

	for _, status := range []int{401, 403} {
		script.Store(rcScriptStep{status: status, body: `{"error":{"code":"unauthorized"}}`})
		_, err := client.FetchRemoteConfig(context.Background())
		if err == nil || !strings.Contains(err.Error(), "unauthorized") {
			t.Fatalf("expected unauthorized for %d, got %v", status, err)
		}
		// Fail closed means refuse to serve — not delete: the cache file is
		// byte-identical and the getter snapshot still serves.
		cacheAfter, _ := os.ReadFile(cachePath)
		if string(cacheBefore) != string(cacheAfter) {
			t.Fatalf("%d must not disturb the cache file", status)
		}
		if got := client.RemoteConfigString("k", "fallback"); got != "v" {
			t.Fatalf("%d must leave the getter snapshot intact, got %q", status, got)
		}
	}

	// A later 200 with a valid credential resumes over the same cache file.
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"v2"}}`})
	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil || result.Values["k"] != "v2" {
		t.Fatalf("expected recovery after unauthorized, got %+v %v", result, err)
	}
}

func TestRemoteConfigPermanentFailuresNeverServeCache(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"v"}}`})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	cacheBefore, _ := os.ReadFile(cachePath)

	for _, status := range []int{404, 413} {
		script.Store(rcScriptStep{status: status, body: ``})
		_, err := client.FetchRemoteConfig(context.Background())
		want := "http_" + map[int]string{404: "404", 413: "413"}[status]
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("expected %s, got %v", want, err)
		}
		cacheAfter, _ := os.ReadFile(cachePath)
		if string(cacheBefore) != string(cacheAfter) {
			t.Fatalf("%d must not disturb the cache file", status)
		}
		if got := client.RemoteConfigString("k", "fallback"); got != "v" {
			t.Fatalf("%d must leave the getter snapshot intact, got %q", status, got)
		}
	}
}

func TestRemoteConfigMalformedValuesMemberNeverFallsBack(t *testing.T) {
	var script atomic.Value
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())

	// A wrapper whose values member is not an object must be malformed —
	// never served as an unwrapped payload (that would expose wrapper fields
	// as configuration).
	for _, body := range []string{
		`{"version":1,"values":null}`,
		`{"version":1,"values":[]}`,
		`{"version":1,"values":"nope"}`,
		`{"version":1,"values":5}`,
	} {
		script.Store(rcScriptStep{status: 200, body: body})
		if _, err := client.FetchRemoteConfig(context.Background()); err == nil || !strings.Contains(err.Error(), "malformed_response") {
			t.Fatalf("expected malformed_response for %s, got %v", body, err)
		}
	}
}

func TestRemoteConfigUnwrappedPayload(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"version":7,"speed":1.5}`})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())

	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil {
		t.Fatalf("FetchRemoteConfig: %v", err)
	}
	if result.HasVersion {
		t.Fatalf("an unwrapped payload carries no wrapper version, got %+v", result)
	}
	// In an unwrapped payload a "version" KEY is configuration.
	if result.Values["version"] != float64(7) || result.Values["speed"] != 1.5 {
		t.Fatalf("unexpected unwrapped values %+v", result.Values)
	}
	if got := client.RemoteConfigNumber("version", 0); got != 7 {
		t.Fatalf("expected the unwrapped version key served as configuration, got %v", got)
	}
	if _, has := client.RemoteConfigVersion(); has {
		t.Fatalf("RemoteConfigVersion must report absent for an unwrapped payload")
	}
}

func TestRemoteConfigTypedGetters(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"values":{"s":"text","n":4,"flag":false,"nested":{"a":1},"list":[1,2]}}`})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())

	// Before any served snapshot: defaults and nil Values.
	if got := client.RemoteConfigString("s", "default"); got != "default" {
		t.Fatalf("expected the default pre-fetch, got %q", got)
	}
	if client.RemoteConfigValues() != nil {
		t.Fatalf("expected nil Values before a served snapshot")
	}

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("FetchRemoteConfig: %v", err)
	}

	if got := client.RemoteConfigString("s", "default"); got != "text" {
		t.Fatalf("string getter: got %q", got)
	}
	if got := client.RemoteConfigNumber("n", 0); got != 4 {
		t.Fatalf("number getter: got %v", got)
	}
	// A stored false must be servable — not collapsed into the default.
	if got := client.RemoteConfigBool("flag", true); got != false {
		t.Fatalf("bool getter must serve a stored false")
	}
	// Missing key AND type mismatch both serve the default.
	if got := client.RemoteConfigString("missing", "default"); got != "default" {
		t.Fatalf("missing key: got %q", got)
	}
	if got := client.RemoteConfigString("n", "default"); got != "default" {
		t.Fatalf("type mismatch: got %q", got)
	}
	if got := client.RemoteConfigNumber("s", 9); got != 9 {
		t.Fatalf("type mismatch: got %v", got)
	}
	if got := client.RemoteConfigBool("s", true); got != true {
		t.Fatalf("type mismatch: got %v", got)
	}

	// Values and nested values are defensive copies: mutating them must not
	// corrupt what later getters read.
	values := client.RemoteConfigValues()
	values["s"] = "mutated"
	nested, _ := client.RemoteConfigValue("nested")
	nested.(map[string]any)["a"] = 99.0
	if got := client.RemoteConfigString("s", ""); got != "text" {
		t.Fatalf("snapshot corrupted through Values copy: %q", got)
	}
	fresh, _ := client.RemoteConfigValue("nested")
	if fresh.(map[string]any)["a"] != float64(1) {
		t.Fatalf("snapshot corrupted through nested value copy: %+v", fresh)
	}
}

func TestRemoteConfigGettersServeFromDiskPreFetch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("no fetch should happen in this test")
	}))
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	writeRemoteConfigCacheFile(t, cachePath, rcCache{
		Scope:       testRemoteConfigScope(server.URL),
		ETag:        `"v1"`,
		Body:        `{"version":2,"values":{"k":"from-disk"}}`,
		FetchedAtMS: 1000,
	})

	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())

	if got := client.RemoteConfigString("k", "default"); got != "from-disk" {
		t.Fatalf("expected the persisted snapshot served pre-fetch, got %q", got)
	}
	if version, has := client.RemoteConfigVersion(); !has || version != 2 {
		t.Fatalf("expected the persisted wrapper version, got %v %v", version, has)
	}
}

func TestRemoteConfigOtherScopeCacheIsMissAndOverwritten(t *testing.T) {
	var sawIfNoneMatch atomic.Value
	sawIfNoneMatch.Store("")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawIfNoneMatch.Store(r.Header.Get("If-None-Match"))
		_, _ = w.Write([]byte(`{"values":{"k":"fresh"}}`))
	}))
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	writeRemoteConfigCacheFile(t, cachePath, rcCache{
		Scope:       buildRemoteConfigScope("other-workspace", "develop", "anon-rc-1", server.URL),
		ETag:        `"stale"`,
		Body:        `{"values":{"k":"other-scope"}}`,
		FetchedAtMS: 1000,
	})

	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())

	// The other-scope record is a miss: never served, its ETag never sent.
	if got := client.RemoteConfigString("k", "default"); got != "default" {
		t.Fatalf("another scope's values must never be served, got %q", got)
	}
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("FetchRemoteConfig: %v", err)
	}
	if got := sawIfNoneMatch.Load(); got != "" {
		t.Fatalf("another scope's ETag must never be sent, got %q", got)
	}
	record := readRemoteConfigCacheFile(t, cachePath)
	if record.Scope != testRemoteConfigScope(server.URL) {
		t.Fatalf("expected the next successful fetch to overwrite the record, got scope %q", record.Scope)
	}
}

func TestRemoteConfigCorruptCacheFileIsCleanStart(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"values":{"k":"recovered"}}`))
	}))
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	if err := os.WriteFile(cachePath, []byte("not json at all"), 0o600); err != nil {
		t.Fatalf("write corrupt cache: %v", err)
	}

	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())

	if got := client.RemoteConfigString("k", "default"); got != "default" {
		t.Fatalf("a corrupt cache must be a miss, got %q", got)
	}
	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil || result.Values["k"] != "recovered" {
		t.Fatalf("expected a clean recovery fetch, got %+v %v", result, err)
	}
	record := readRemoteConfigCacheFile(t, cachePath)
	if record.Body != `{"values":{"k":"recovered"}}` {
		t.Fatalf("expected the corrupt record overwritten, got %q", record.Body)
	}
}

func TestRemoteConfigConcurrentFetchFenceRefusesLateOverwrite(t *testing.T) {
	firstArrived := make(chan struct{})
	releaseFirst := make(chan struct{})
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			close(firstArrived)
			<-releaseFirst
			_, _ = w.Write([]byte(`{"values":{"k":"old"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"values":{"k":"new"}}`))
	}))
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())

	firstResult := make(chan RemoteConfigResult, 1)
	go func() {
		result, err := client.FetchRemoteConfig(context.Background())
		if err != nil {
			t.Errorf("first fetch: %v", err)
		}
		firstResult <- result
	}()
	// The first fetch holds a lower sequence number: its request reached the
	// server (so its seq was assigned) before the second fetch dispatches.
	<-firstArrived

	second, err := client.FetchRemoteConfig(context.Background())
	if err != nil || second.Values["k"] != "new" {
		t.Fatalf("second fetch: %+v %v", second, err)
	}

	close(releaseFirst)
	first := <-firstResult
	// The late first response still answers ITS caller...
	if first.FromCache || first.Values["k"] != "old" {
		t.Fatalf("the late fetch must still receive its own response, got %+v", first)
	}
	// ...but must not roll the snapshot back over the newer settled outcome.
	if got := client.RemoteConfigString("k", "default"); got != "new" {
		t.Fatalf("a late older response must not overwrite a newer settled one, got %q", got)
	}
}

func TestRemoteConfigBackwardClockStampMonotonic(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"first"}}`})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())
	clock := &stubClock{now: time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)}
	client.clock = clock

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	firstStamp := readRemoteConfigCacheFile(t, cachePath).FetchedAtMS

	// The wall clock jumps BACK an hour; the record installed now must still
	// rank above the record it supersedes: superseded stamp + 1ms.
	clock.now = clock.now.Add(-time.Hour)
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"second"}}`})
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	record := readRemoteConfigCacheFile(t, cachePath)
	if record.FetchedAtMS != firstStamp+1 {
		t.Fatalf("expected the stamp raised to superseded+1 (%d), got %d", firstStamp+1, record.FetchedAtMS)
	}
	if record.Body != `{"values":{"k":"second"}}` {
		t.Fatalf("expected the newer body persisted, got %q", record.Body)
	}
}

func TestRemoteConfigFailedCacheWriteKeepsHeldSnapshot(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"served"}}`})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	dir := t.TempDir()
	cachePath := filepath.Join(dir, "rc-cache.json")
	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	defer func() { _ = os.Chmod(dir, 0o700) }()
	if probe, err := os.CreateTemp(dir, "probe-*"); err == nil {
		// Running with privileges that ignore file modes (root): the failed
		// write cannot be provoked this way.
		_ = probe.Close()
		_ = os.Remove(probe.Name())
		t.Skip("directory permissions are not enforced in this environment")
	}

	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil {
		t.Fatalf("the fetch itself must succeed despite the failed persist: %v", err)
	}
	if result.Values["k"] != "served" {
		t.Fatalf("unexpected result %+v", result)
	}
	// The held in-memory record backs the getters and later offline fetches.
	if got := client.RemoteConfigString("k", "default"); got != "served" {
		t.Fatalf("expected the held snapshot to serve, got %q", got)
	}
	if stats := client.Snapshot(); stats.LastError != "remote_config_cache_persist_failed" {
		t.Fatalf("expected remote_config_cache_persist_failed surfaced, got %q", stats.LastError)
	}
	script.Store(rcScriptStep{status: 503})
	offline, err := client.FetchRemoteConfig(context.Background())
	if err != nil || !offline.FromCache || offline.Values["k"] != "served" {
		t.Fatalf("expected the held record served offline, got %+v %v", offline, err)
	}
}

func TestRemoteConfigNotConsentGated(t *testing.T) {
	var requests atomic.Int64
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"v"}}`})
	server := newRCScriptServer(t, &requests, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	cacheBefore, _ := os.ReadFile(cachePath)

	client.SetConsent(false)
	// The denial itself must not clear the cache record.
	cacheAfterDenial, _ := os.ReadFile(cachePath)
	if string(cacheBefore) != string(cacheAfterDenial) {
		t.Fatalf("denied consent must not clear the remote-config cache")
	}
	// And the fetch is not consent-gated: it still reaches the network and
	// serves (and refreshes) as usual.
	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil || result.Values["k"] != "v" {
		t.Fatalf("expected the fetch to work under denied consent, got %+v %v", result, err)
	}
	if requests.Load() < 2 {
		t.Fatalf("expected the denied-consent fetch to reach the network, got %d requests", requests.Load())
	}
	if record := readRemoteConfigCacheFile(t, cachePath); record.Body != `{"values":{"k":"v"}}` {
		t.Fatalf("expected the cache still healthy after the denied-consent fetch, got %q", record.Body)
	}
}

func TestRemoteConfigCooldownShortCircuitsInsideWindow(t *testing.T) {
	var requests atomic.Int64
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"v"}}`})
	server := newRCScriptServer(t, &requests, &script)
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())
	clock := &stubClock{now: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)}
	client.clock = clock

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}

	script.Store(rcScriptStep{status: 429, headers: map[string]string{"Retry-After": "7"}})
	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil || !result.FromCache || result.Reason != "transient_429" {
		t.Fatalf("expected the live 429 cache-served, got %+v %v", result, err)
	}
	sent := requests.Load()

	// Inside the 7s window: zero HTTP requests, same cache-served outcome —
	// indistinguishable from a live 429 to the caller.
	clock.now = clock.now.Add(6 * time.Second)
	result, err = client.FetchRemoteConfig(context.Background())
	if err != nil || !result.FromCache || result.Reason != "transient_429" {
		t.Fatalf("expected the in-window fetch cache-served as transient_429, got %+v %v", result, err)
	}
	if requests.Load() != sent {
		t.Fatalf("an in-window fetch must not touch the network: %d -> %d requests", sent, requests.Load())
	}

	// Past the deadline the next fetch is real again.
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"after"}}`})
	clock.now = clock.now.Add(2 * time.Second)
	result, err = client.FetchRemoteConfig(context.Background())
	if err != nil || result.FromCache || result.Values["k"] != "after" {
		t.Fatalf("expected a real fetch after expiry, got %+v %v", result, err)
	}
	if requests.Load() != sent+1 {
		t.Fatalf("expected exactly one more request after expiry, got %d", requests.Load()-sent)
	}
}

func TestRemoteConfigCooldownWithoutCacheFails(t *testing.T) {
	var requests atomic.Int64
	var script atomic.Value
	script.Store(rcScriptStep{status: 429, headers: map[string]string{"Retry-After": "30"}})
	server := newRCScriptServer(t, &requests, &script)
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())
	clock := &stubClock{now: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)}
	client.clock = clock

	if _, err := client.FetchRemoteConfig(context.Background()); err == nil || !strings.Contains(err.Error(), "transient_429") {
		t.Fatalf("expected transient_429 without a cache, got %v", err)
	}
	sent := requests.Load()
	clock.now = clock.now.Add(10 * time.Second)
	if _, err := client.FetchRemoteConfig(context.Background()); err == nil || !strings.Contains(err.Error(), "transient_429") {
		t.Fatalf("expected the in-window fetch to fail transient_429, got %v", err)
	}
	if requests.Load() != sent {
		t.Fatalf("an in-window fetch must not touch the network")
	}
}

func TestRemoteConfigCooldownFloorAndExpiry(t *testing.T) {
	for _, header := range []string{"", "0", "soon", "-5", "1.5"} {
		var requests atomic.Int64
		var script atomic.Value
		headers := map[string]string{}
		if header != "" {
			headers["Retry-After"] = header
		}
		script.Store(rcScriptStep{status: 429, headers: headers})
		server := newRCScriptServer(t, &requests, &script)

		client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
		clock := &stubClock{now: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)}
		client.clock = clock

		_, _ = client.FetchRemoteConfig(context.Background())
		sent := requests.Load()

		// An absent or malformed header floors at 1s: still in-window at
		// +0.5s, expired at +1s.
		clock.now = clock.now.Add(500 * time.Millisecond)
		_, _ = client.FetchRemoteConfig(context.Background())
		if requests.Load() != sent {
			t.Fatalf("header %q: expected the +0.5s fetch inside the 1s floor window", header)
		}
		clock.now = clock.now.Add(500 * time.Millisecond)
		_, _ = client.FetchRemoteConfig(context.Background())
		if requests.Load() != sent+1 {
			t.Fatalf("header %q: expected the +1s fetch to go out", header)
		}

		_ = client.Close(context.Background())
		server.Close()
	}
}

func TestRemoteConfigCooldownClamp(t *testing.T) {
	var requests atomic.Int64
	var script atomic.Value
	script.Store(rcScriptStep{status: 429, headers: map[string]string{"Retry-After": "999999999"}})
	server := newRCScriptServer(t, &requests, &script)
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())
	clock := &stubClock{now: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)}
	client.clock = clock

	_, _ = client.FetchRemoteConfig(context.Background())
	sent := requests.Load()

	clock.now = clock.now.Add(24*time.Hour - time.Second)
	_, _ = client.FetchRemoteConfig(context.Background())
	if requests.Load() != sent {
		t.Fatalf("expected the fetch just under 24h still inside the clamped window")
	}
	clock.now = clock.now.Add(2 * time.Second)
	_, _ = client.FetchRemoteConfig(context.Background())
	if requests.Load() != sent+1 {
		t.Fatalf("expected the fetch past the 24h clamp to go out")
	}
}

func TestRemoteConfigCooldownMonotonicMax(t *testing.T) {
	rc := &remoteConfigState{}
	now := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	rc.armCooldownLocked(now, remoteConfigResponse{retryAfterSeconds: 100, retryAfterPresent: true})
	// A later, shorter Retry-After from a straggling concurrent response
	// must never LOWER the armed deadline.
	rc.armCooldownLocked(now, remoteConfigResponse{retryAfterSeconds: 5, retryAfterPresent: true})
	if want := now.Add(100 * time.Second); !rc.cooldownUntil.Equal(want) {
		t.Fatalf("expected the deadline kept at %v, got %v", want, rc.cooldownUntil)
	}
	rc.armCooldownLocked(now, remoteConfigResponse{retryAfterSeconds: 200, retryAfterPresent: true})
	if want := now.Add(200 * time.Second); !rc.cooldownUntil.Equal(want) {
		t.Fatalf("expected the deadline extended to %v, got %v", want, rc.cooldownUntil)
	}
}

func TestRemoteConfigRetryAfterParseDigitsOnly(t *testing.T) {
	cases := []struct {
		header  string
		seconds int
		present bool
	}{
		{"", 0, false},
		{"7", 7, true},
		{" 7 ", 7, true},
		{"0", 0, true},
		{"-1", 0, false},
		{"1.5", 0, false},
		{"soon", 0, false},
		// HTTP-date is deliberately NOT accepted on this route.
		{"Wed, 01 Jul 2026 00:00:07 GMT", 0, false},
		{"99999999999999999999", rcCooldownClampSeconds, true},
		{"999999999", rcCooldownClampSeconds, true},
	}
	for _, tc := range cases {
		seconds, present := parseRemoteConfigRetryAfter(tc.header)
		if seconds != tc.seconds || present != tc.present {
			t.Fatalf("header %q: got (%d, %v), want (%d, %v)", tc.header, seconds, present, tc.seconds, tc.present)
		}
	}
}

func TestRemoteConfigRedirectClassifiedPermanent(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"v"}}`})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	cacheBefore, _ := os.ReadFile(cachePath)

	// The redirect is NOT followed: the 3xx itself is the outcome, and the
	// contract classifies it as an authoritative permanent failure — never a
	// transient malformed_response built from the redirect target's HTML.
	script.Store(rcScriptStep{status: 302, headers: map[string]string{"Location": server.URL + "/elsewhere"}})
	_, err := client.FetchRemoteConfig(context.Background())
	if err == nil || !strings.Contains(err.Error(), "http_302") {
		t.Fatalf("expected the redirect classified as permanent http_302, got %v", err)
	}
	cacheAfter, _ := os.ReadFile(cachePath)
	if string(cacheBefore) != string(cacheAfter) {
		t.Fatalf("a redirect must not disturb the cache file")
	}
	if got := client.RemoteConfigString("k", "fallback"); got != "v" {
		t.Fatalf("a redirect must leave the getter snapshot intact, got %q", got)
	}
}

func TestRemoteConfigCallerCancellationReturnsContextError(t *testing.T) {
	var requests atomic.Int64
	arrived := make(chan struct{}, 4)
	release := make(chan struct{})
	var blocking atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if blocking.Load() {
			arrived <- struct{}{}
			<-release
		}
		_, _ = w.Write([]byte(`{"values":{"k":"v"}}`))
	}))
	defer server.Close()
	defer close(release)

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	cacheBefore, _ := os.ReadFile(cachePath)

	// The CALLER cancels mid-request: the fetch returns the caller's context
	// error — never a cache-served "success" the caller cannot distinguish
	// from a healthy outcome.
	blocking.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	fetchDone := make(chan error, 1)
	go func() {
		_, fetchErr := client.FetchRemoteConfig(ctx)
		fetchDone <- fetchErr
	}()
	<-arrived
	cancel()
	err := <-fetchDone
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the caller's context.Canceled, got %v", err)
	}
	if strings.Contains(err.Error(), "http_0") {
		t.Fatalf("caller cancellation must not classify as a transport outcome, got %v", err)
	}
	cacheAfter, _ := os.ReadFile(cachePath)
	if string(cacheBefore) != string(cacheAfter) {
		t.Fatalf("caller cancellation must not disturb the cache file")
	}
	if got := client.RemoteConfigString("k", "fallback"); got != "v" {
		t.Fatalf("caller cancellation must leave the getter snapshot intact, got %q", got)
	}

	// No cooldown or fence side effects: the very next fetch reaches the
	// network normally.
	blocking.Store(false)
	sent := requests.Load()
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("post-cancel fetch: %v", err)
	}
	if requests.Load() != sent+1 {
		t.Fatalf("expected the post-cancel fetch on the network")
	}
}

func TestRemoteConfigInternalTimeoutStaysTransient(t *testing.T) {
	var slow atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if slow.Load() {
			time.Sleep(500 * time.Millisecond)
		}
		_, _ = w.Write([]byte(`{"values":{"k":"v"}}`))
	}))
	defer server.Close()

	client, err := NewClient(Config{
		IngestURL:       server.URL,
		Token:           "test-token",
		WorkspaceID:     "workspace-test",
		AppID:           "app-test",
		EnvironmentID:   "develop",
		Source:          SourceBackend,
		AnonymousID:     "anon-rc-1",
		APIKey:          "test-rc-key",
		RemoteConfigURL: server.URL,
		FlushInterval:   time.Hour,
		HTTPTimeout:     100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}

	// The SDK-internal HTTPTimeout fires while the CALLER's context (with no
	// deadline of its own) is still live: this is an endpoint outcome, and
	// the transient class serves the cache.
	slow.Store(true)
	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil {
		t.Fatalf("expected the internal timeout served from cache, got %v", err)
	}
	if !result.FromCache || result.Reason != "http_0" || result.Values["k"] != "v" {
		t.Fatalf("expected the cache-served http_0 outcome, got %+v", result)
	}
}

func TestRemoteConfigStalledBodyPreservesStatus(t *testing.T) {
	var stall atomic.Bool
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stall.Load() {
			// An authoritative 401 whose BODY stalls past the SDK-internal
			// timeout: status and headers flush immediately, the body never
			// completes.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":"unauthorized"`))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-release
			return
		}
		_, _ = w.Write([]byte(`{"values":{"k":"v"}}`))
	}))
	defer server.Close()
	defer close(release)

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	client, err := NewClient(Config{
		IngestURL:             server.URL,
		Token:                 "test-token",
		WorkspaceID:           "workspace-test",
		AppID:                 "app-test",
		EnvironmentID:         "develop",
		Source:                SourceBackend,
		AnonymousID:           "anon-rc-1",
		APIKey:                "test-rc-key",
		RemoteConfigURL:       server.URL,
		RemoteConfigCachePath: cachePath,
		FlushInterval:         time.Hour,
		HTTPTimeout:           100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	cacheBefore, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read primed cache: %v", err)
	}

	// The key is revoked: the 401 status arrives, then the body stalls until
	// the SDK-internal deadline ends the read. The received status must
	// classify — unauthorized fails closed — never degrade into a
	// cache-served http_0 that keeps a revoked key supplied with
	// configuration.
	stall.Store(true)
	result, err := client.FetchRemoteConfig(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("expected the stalled 401 to fail closed as unauthorized, got result=%+v err=%v", result, err)
	}
	if result.FromCache {
		t.Fatalf("an unauthorized outcome must never serve the cache, got %+v", result)
	}
	cacheAfter, _ := os.ReadFile(cachePath)
	if string(cacheBefore) != string(cacheAfter) {
		t.Fatalf("an unauthorized outcome must leave the cache file untouched")
	}
	if got := client.RemoteConfigString("k", "fallback"); got != "v" {
		t.Fatalf("an unauthorized outcome must leave the getter snapshot intact, got %q", got)
	}
}

func TestRemoteConfigInFlight200DoesNotRollBackFresherDurable(t *testing.T) {
	var requests atomic.Int64
	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 2 {
			arrived <- struct{}{}
			<-release
			_, _ = w.Write([]byte(`{"values":{"k":"B"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"values":{"k":"A"}}`))
	}))
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	primed := readRemoteConfigCacheFile(t, cachePath)

	fetchDone := make(chan RemoteConfigResult, 1)
	go func() {
		result, fetchErr := client.FetchRemoteConfig(context.Background())
		if fetchErr != nil {
			t.Errorf("in-flight fetch: %v", fetchErr)
		}
		fetchDone <- result
	}()
	<-arrived
	// Another same-app process refreshes the durable record while this
	// fetch's response is still in flight.
	foreign := rcCache{
		Scope:       primed.Scope,
		ETag:        `"foreign"`,
		Body:        `{"values":{"k":"C"}}`,
		FetchedAtMS: primed.FetchedAtMS + 10_000,
	}
	writeRemoteConfigCacheFile(t, cachePath, foreign)
	close(release)

	result := <-fetchDone
	// The fetch's caller still receives ITS response...
	if result.FromCache || result.Values["k"] != "B" {
		t.Fatalf("expected the in-flight fetch to serve its own response, got %+v", result)
	}
	// ...but the fresher durable record is not rolled back...
	record := readRemoteConfigCacheFile(t, cachePath)
	if record.Body != foreign.Body || record.FetchedAtMS != foreign.FetchedAtMS {
		t.Fatalf("expected the fresher durable record kept, got %+v", record)
	}
	// ...and the getters never regress (they keep the last installed
	// snapshot; the next load converges on the freshest record).
	if got := client.RemoteConfigString("k", "fallback"); got != "A" {
		t.Fatalf("expected the getter snapshot unchanged, got %q", got)
	}
}

func TestRemoteConfigFetchAfterCloseReturnsErrClosed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"values":{}}`))
	}))
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := client.FetchRemoteConfig(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestRemoteConfigTruncatedResponsesClassifyByStatus(t *testing.T) {
	// The handler advertises a longer body than it writes, so the client's
	// body read fails mid-stream while the status and headers already
	// arrived.
	var status atomic.Int64
	status.Store(200)
	var truncate atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code := int(status.Load())
		if truncate.Load() {
			w.Header().Set("Content-Length", "4096")
			w.WriteHeader(code)
			_, _ = w.Write([]byte(`{"values":{"k":`))
			return
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"values":{"k":"v"}}`))
	}))
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	cacheBefore, _ := os.ReadFile(cachePath)

	// A truncated 401 keeps its authority: fail closed, refuse to serve the
	// cache — never the cache-served http_0 a discarded status produced.
	truncate.Store(true)
	status.Store(401)
	_, err := client.FetchRemoteConfig(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("expected a truncated 401 to fail closed as unauthorized, got %v", err)
	}
	cacheAfter, _ := os.ReadFile(cachePath)
	if string(cacheBefore) != string(cacheAfter) {
		t.Fatalf("a truncated 401 must not disturb the cache file")
	}
	if got := client.RemoteConfigString("k", "fallback"); got != "v" {
		t.Fatalf("a truncated 401 must leave the getter snapshot intact, got %q", got)
	}

	// A truncated 200 is the one outcome that NEEDED its body: transient,
	// cache-served.
	status.Store(200)
	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil {
		t.Fatalf("expected the truncated 200 served from cache, got %v", err)
	}
	if !result.FromCache || result.Reason != "malformed_response" || result.Values["k"] != "v" {
		t.Fatalf("expected the cache-served malformed_response outcome, got %+v", result)
	}
}

func TestRemoteConfigCooldownHonorsCanceledContext(t *testing.T) {
	var requests atomic.Int64
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"v"}}`})
	server := newRCScriptServer(t, &requests, &script)
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())
	clock := &stubClock{now: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)}
	client.clock = clock

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	script.Store(rcScriptStep{status: 429, headers: map[string]string{"Retry-After": "60"}})
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("cooldown-arming fetch: %v", err)
	}
	sent := requests.Load()
	snapshotBefore := client.RemoteConfigString("k", "fallback")

	// Inside the cooldown window an already-canceled caller gets ITS context
	// error — never a cache "success" — and nothing installs.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	clock.now = clock.now.Add(10 * time.Second)
	_, err := client.FetchRemoteConfig(canceled)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the caller's context.Canceled inside the cooldown, got %v", err)
	}
	if requests.Load() != sent {
		t.Fatalf("an in-window fetch must not touch the network")
	}
	if got := client.RemoteConfigString("k", "fallback"); got != snapshotBefore {
		t.Fatalf("a canceled in-window fetch must not touch the snapshot, got %q", got)
	}
	// A live caller inside the same window still gets the cache-served
	// transient_429.
	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil || !result.FromCache || result.Reason != "transient_429" {
		t.Fatalf("expected the live in-window fetch cache-served, got %+v %v", result, err)
	}
}

// blockingFetchTransport holds a remote-config fetch inside the transport
// until the test releases it, then answers a fresh 200 — for proving Close
// fences in-flight fetches.
type blockingFetchTransport struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (t *blockingFetchTransport) Publish(context.Context, batchRequest) (batchResult, error) {
	return batchResult{}, nil
}

func (t *blockingFetchTransport) PublishConsent(context.Context, consentRequest) (consentResult, error) {
	return consentResult{Recorded: true}, nil
}

func (t *blockingFetchTransport) FetchRemoteConfig(context.Context, remoteConfigRequest) (remoteConfigResponse, error) {
	t.once.Do(func() { close(t.started) })
	<-t.release
	return remoteConfigResponse{status: 200, body: []byte(`{"values":{"fence":"held"}}`), etag: `"rc-fence-1"`}, nil
}

func TestRemoteConfigCloseFencesInFlightFetch(t *testing.T) {
	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	client, err := NewClient(Config{
		IngestURL:             "http://127.0.0.1:9", // never dialed: the transport is replaced below
		Token:                 "test-token",
		WorkspaceID:           "workspace-test",
		AppID:                 "app-test",
		EnvironmentID:         "develop",
		Source:                SourceBackend,
		AnonymousID:           "anon-rc-1",
		APIKey:                "test-rc-key",
		RemoteConfigURL:       "http://127.0.0.1:9",
		RemoteConfigCachePath: cachePath,
		BatchSize:             2,
		BufferSize:            4,
		FlushInterval:         time.Hour,
		HTTPTimeout:           time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	transport := &blockingFetchTransport{started: make(chan struct{}), release: make(chan struct{})}
	client.transport = transport

	fetchDone := make(chan error, 1)
	go func() {
		_, fetchErr := client.FetchRemoteConfig(context.Background())
		fetchDone <- fetchErr
	}()
	select {
	case <-transport.started:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the fetch to enter the transport")
	}

	closeDone := make(chan struct{})
	go func() {
		_ = client.Close(context.Background())
		close(closeDone)
	}()
	// Close must WAIT for the in-flight fetch — the same lifecycle fence as a
	// synchronous Track publish — never return around live fetch I/O.
	select {
	case <-closeDone:
		t.Fatal("Close returned while a remote-config fetch was still in flight")
	case <-time.After(150 * time.Millisecond):
	}

	close(transport.release)
	select {
	case fetchErr := <-fetchDone:
		if fetchErr != nil {
			t.Fatalf("FetchRemoteConfig: %v", fetchErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the released fetch to return")
	}
	select {
	case <-closeDone:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Close to return after the fetch settled")
	}

	// The fetch settled — including its durable cache write — BEFORE Close
	// returned; nothing runs after it.
	record := readRemoteConfigCacheFile(t, cachePath)
	if record.Body != `{"values":{"fence":"held"}}` {
		t.Fatalf("expected the in-flight fetch's cache write completed before Close returned, got %q", record.Body)
	}
	// A fetch that begins after Close is rejected outright.
	if _, err := client.FetchRemoteConfig(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed for a post-close fetch, got %v", err)
	}
}

func TestRemoteConfigCacheWriteDoesNotChmodSharedParent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"values":{"k":"v"}}`))
	}))
	defer server.Close()

	// RemoteConfigCachePath names a FILE in a directory the caller chose —
	// here a shared 0755 one (think /tmp or an XDG cache dir). A cache write
	// must not re-mode it: the tighten-to-0700 side effect is reserved for
	// the dedicated SpoolDir state directory the SDK owns.
	shared := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(shared, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Chmod(shared, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	cachePath := filepath.Join(shared, "rc-cache.json")

	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("FetchRemoteConfig: %v", err)
	}

	info, err := os.Stat(shared)
	if err != nil {
		t.Fatalf("stat shared dir: %v", err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("expected the pre-existing shared parent left at 0755, got %v", info.Mode().Perm())
	}
	fileInfo, err := os.Stat(cachePath)
	if err != nil {
		t.Fatalf("stat cache file: %v", err)
	}
	if fileInfo.Mode().Perm() != 0o600 {
		t.Fatalf("expected the cache file itself private 0600, got %v", fileInfo.Mode().Perm())
	}
	if record := readRemoteConfigCacheFile(t, cachePath); record.Body != `{"values":{"k":"v"}}` {
		t.Fatalf("expected the record written through the untouched parent, got %+v", record)
	}
}

func TestRemoteConfigSkippedInstallDoesNotAdoptOrMoveGetters(t *testing.T) {
	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		<-release
		_, _ = w.Write([]byte(`{"values":{"k":"C"}}`))
	}))
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	// Constructed with NO cache file: rc.held stays nil (nothing preloads),
	// which is exactly the corner where the adoption branch used to run for
	// a guard-skipped fresh 200.
	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())
	scope := testRemoteConfigScope(server.URL)

	// Another process writes record A after construction; this fetch
	// dispatches against it...
	base := time.Now().UnixMilli()
	writeRemoteConfigCacheFile(t, cachePath, rcCache{
		Scope: scope, ETag: `"a"`, Body: `{"values":{"k":"A"}}`, FetchedAtMS: base,
	})
	fetchDone := make(chan RemoteConfigResult, 1)
	go func() {
		result, fetchErr := client.FetchRemoteConfig(context.Background())
		if fetchErr != nil {
			t.Errorf("in-flight fetch: %v", fetchErr)
		}
		fetchDone <- result
	}()
	<-arrived
	// ...and refreshes it to a FRESHER record B (different body) while the
	// 200 response is still in flight, so the install guard skips.
	fresher := rcCache{
		Scope: scope, ETag: `"b"`, Body: `{"values":{"k":"B"}}`, FetchedAtMS: base + 10_000,
	}
	writeRemoteConfigCacheFile(t, cachePath, fresher)
	close(release)

	result := <-fetchDone
	// The fetch's caller still receives ITS response...
	if result.FromCache || result.Values["k"] != "C" {
		t.Fatalf("expected the in-flight fetch to serve its own response, got %+v", result)
	}
	// ...the fresher durable record stays...
	if record := readRemoteConfigCacheFile(t, cachePath); record.Body != fresher.Body || record.FetchedAtMS != fresher.FetchedAtMS {
		t.Fatalf("expected the fresher durable record kept, got %+v", record)
	}
	// ...and the skip installs NOTHING: no adoption of the at-dispatch
	// record, no skipped-200 values in the getters — the snapshot a skipped
	// install would have mismatched stays exactly where it was (empty here).
	if got := client.RemoteConfigString("k", "fallback"); got != "fallback" {
		t.Fatalf("expected the getter snapshot untouched by the skipped install, got %q", got)
	}
}

func TestRemoteConfigStaleLate429DoesNotArmCooldown(t *testing.T) {
	var requests atomic.Int64
	firstArrived := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			firstArrived <- struct{}{}
			<-releaseFirst
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"values":{"k":"fresh"}}`))
	}))
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, filepath.Join(t.TempDir(), "rc-cache.json"), "anon-rc-1")
	defer client.Close(context.Background())

	// Fetch 1 dispatches and stalls inside the server...
	firstDone := make(chan error, 1)
	go func() {
		_, fetchErr := client.FetchRemoteConfig(context.Background())
		firstDone <- fetchErr
	}()
	<-firstArrived
	// ...fetch 2 dispatches later and settles a fresh 200 FIRST...
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("newer fetch: %v", err)
	}
	// ...then the stalled 429 lands: an outdated backpressure instruction
	// from before the endpoint provably served fresh configuration.
	close(releaseFirst)
	if err := <-firstDone; err == nil || !strings.Contains(err.Error(), "transient_429") {
		t.Fatalf("expected the stale fetch's own transient_429 outcome (it dispatched with no cache), got %v", err)
	}

	// The stale 429 must NOT have armed the cooldown: a third fetch hits the
	// network instead of being cache-served inside a phantom window.
	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil {
		t.Fatalf("post-stale fetch: %v", err)
	}
	if result.FromCache || result.Values["k"] != "fresh" {
		t.Fatalf("expected a live network fetch, not a cooldown cache serve, got %+v", result)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("expected the third fetch on the wire (3 requests), got %d", got)
	}
}

func TestRemoteConfigOversizedCacheFileIgnoredWithoutFullUse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"values":{"k":"fresh"}}`))
	}))
	defer server.Close()

	// A cache file over the bounded read limit — a previous buggy version,
	// tampering, or simply not this SDK's record. It must be treated as
	// corrupt (clean start) without being loaded whole.
	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	huge := make([]byte, rcCacheReadLimit+1)
	for i := range huge {
		huge[i] = 'x'
	}
	if err := os.WriteFile(cachePath, huge, 0o600); err != nil {
		t.Fatalf("write oversized cache: %v", err)
	}

	client := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer client.Close(context.Background())
	if got := client.RemoteConfigString("k", "fallback"); got != "fallback" {
		t.Fatalf("expected no preload from an over-limit cache file, got %q", got)
	}

	// A fresh fetch works over it and overwrites it with a bounded record.
	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil || result.FromCache || result.Values["k"] != "fresh" {
		t.Fatalf("expected a clean live fetch over the discarded cache, got %+v %v", result, err)
	}
	if record := readRemoteConfigCacheFile(t, cachePath); record.Body != `{"values":{"k":"fresh"}}` {
		t.Fatalf("expected the oversized file overwritten by the fresh record, got %d body bytes", len(record.Body))
	}
}

func TestRemoteConfigCacheRoundTripsHTMLDenseBody(t *testing.T) {
	// A valid sub-cap body dense in `<`: default JSON marshaling would
	// HTML-escape each to 6 bytes and push the self-written record past the
	// bounded read limit, losing it to the corrupt-cache path on reload.
	body := `{"values":{"k":"` + strings.Repeat("<", 700_000) + `"}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "rc-cache.json")
	// The priming fetch moves a ~700KB body; the shared helper's 1s
	// HTTPTimeout flaked under loaded -race soaks (deadline mid-body), so
	// this fixture gives the fetch comfortable headroom — the test is about
	// the cache round trip, not timeout behavior.
	first, err := NewClient(Config{
		IngestURL:             server.URL,
		Token:                 "test-token",
		WorkspaceID:           "workspace-test",
		AppID:                 "app-test",
		EnvironmentID:         "develop",
		Source:                SourceBackend,
		AnonymousID:           "anon-rc-1",
		APIKey:                "test-rc-key",
		RemoteConfigURL:       server.URL,
		RemoteConfigCachePath: cachePath,
		BatchSize:             2,
		BufferSize:            4,
		FlushInterval:         time.Hour,
		HTTPTimeout:           10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, err := first.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("FetchRemoteConfig: %v", err)
	}
	_ = first.Close(context.Background())
	info, err := os.Stat(cachePath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() > rcCacheReadLimit {
		t.Fatalf("self-written record exceeds the bounded read limit: %d > %d", info.Size(), rcCacheReadLimit)
	}

	// A restart preloads and serves it — the record must not read as corrupt.
	second := newRemoteConfigClient(t, server.URL, cachePath, "anon-rc-1")
	defer second.Close(context.Background())
	if got := second.RemoteConfigString("k", "fallback"); got != strings.Repeat("<", 700_000) {
		t.Fatalf("expected the HTML-dense record preloaded, got %d-byte value (fallback? %v)", len(got), got == "fallback")
	}
}
