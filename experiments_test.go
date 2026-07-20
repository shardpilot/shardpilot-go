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
	"sync/atomic"
	"testing"
	"time"
)

const testExperimentSubjectKey = "spcid_test-subject-0123456789"

// testSubjectFactKey is a grammar-valid sfk1_ subject fact key (64 hex).
var testSubjectFactKey = "sfk1_" + strings.Repeat("0123456789abcdef", 4)

// testAssignedBody is a full client_id-unit assigned verdict.
var testAssignedBody = `{"app_key":"app-test","environment_key":"develop","experiment_key":"exp-checkout",` +
	`"version":3,"assigned":true,"assignment_key":"asgn_0123456789abcdef0123456789abcdef",` +
	`"variant_key":"variant_b","variant_payload":{"speed":2},` +
	`"subject_fact_key":"` + testSubjectFactKey + `",` +
	`"boundary":{"assignment_unit":"client_id","subject_key_kind":"client_id_pseudonymous",` +
	`"runtime_token_scope":"experiment_assignment_read","persistence":"not_persisted",` +
	`"analytics_fact_ownership":"analytics-service","production_rollout":"flag_gated_dark",` +
	`"assignment_hash_version":"sha256-basis-points-v1","traffic_allocation_basis":10000,` +
	`"variant_allocation_basis":10000,"subject_identifier_policy":"pseudonymous client id"}}`

func newExperimentsClient(t *testing.T, serverURL, cachePath string, mutate func(*Config)) *Client {
	t.Helper()
	cfg := Config{
		IngestURL:                     serverURL,
		Token:                         "test-token",
		WorkspaceID:                   "workspace-test",
		AppID:                         "app-test",
		EnvironmentID:                 "develop",
		Source:                        SourceBackend,
		AnonymousID:                   "anon-exp-1",
		APIKey:                        "test-exp-key",
		ExperimentsURL:                serverURL + "/api/cp/v1",
		ExperimentSubjectKey:          testExperimentSubjectKey,
		ExperimentAssignmentCachePath: cachePath,
		BatchSize:                     2,
		BufferSize:                    4,
		FlushInterval:                 time.Hour,
		HTTPTimeout:                   time.Second,
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

func readExperimentCacheFile(t *testing.T, path string) expCacheFile {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read experiment cache file: %v", err)
	}
	var decoded expCacheFile
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal experiment cache file: %v", err)
	}
	return decoded
}

func TestExperimentAssignmentRequestShape(t *testing.T) {
	type seenRequest struct {
		method      string
		path        string
		query       map[string][]string
		auth        string
		ifNoneMatch string
		revision    string
	}
	seen := make(chan seenRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- seenRequest{
			method:      r.Method,
			path:        r.URL.Path,
			query:       r.URL.Query(),
			auth:        r.Header.Get("Authorization"),
			ifNoneMatch: r.Header.Get("If-None-Match"),
			revision:    r.Header.Get("X-ShardPilot-Schema-Revision"),
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(testAssignedBody))
	}))
	defer server.Close()

	client := newExperimentsClient(t, server.URL, "", nil)
	defer client.Close(context.Background())

	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err != nil {
		t.Fatalf("FetchExperimentAssignment: %v", err)
	}
	request := <-seen
	if request.method != http.MethodGet {
		t.Fatalf("expected GET, got %s", request.method)
	}
	if want := "/api/cp/v1/runtime/experiments/assignment"; request.path != want {
		t.Fatalf("expected the prefixed assignment path %q, got %q", want, request.path)
	}
	wantQuery := map[string]string{
		"app_key":         "app-test",
		"environment_key": "develop",
		"experiment_key":  "exp-checkout",
		"subject_key":     testExperimentSubjectKey,
	}
	if len(request.query) != len(wantQuery) {
		t.Fatalf("expected exactly the four routing params, got %v", request.query)
	}
	for param, want := range wantQuery {
		if got := request.query[param]; len(got) != 1 || got[0] != want {
			t.Fatalf("expected %s=%q, got %v", param, want, got)
		}
	}
	if request.auth != "Bearer test-exp-key" {
		t.Fatalf("expected the publishable APIKey bearer, got %q", request.auth)
	}
	if request.ifNoneMatch != "" {
		t.Fatalf("the assignment endpoint has no ETag contract; got If-None-Match %q", request.ifNoneMatch)
	}
	if request.revision != "" {
		t.Fatalf("assignment requests must never carry the schema-revision header, got %q", request.revision)
	}
}

func TestExperimentsConfigValidation(t *testing.T) {
	base := Config{
		IngestURL:     "http://localhost:8080",
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        SourceBackend,
	}

	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"experiments URL requires APIKey", func(cfg *Config) {
			cfg.ExperimentsURL = "http://localhost:9000/api/cp/v1"
		}, true},
		{"experiments URL with path prefix is accepted", func(cfg *Config) {
			cfg.APIKey = "k"
			cfg.ExperimentsURL = "http://localhost:9000/api/cp/v1"
		}, false},
		{"experiments URL trailing slash normalizes", func(cfg *Config) {
			cfg.APIKey = "k"
			cfg.ExperimentsURL = "http://localhost:9000/api/cp/v1/"
		}, false},
		{"experiments URL rejects query", func(cfg *Config) {
			cfg.APIKey = "k"
			cfg.ExperimentsURL = "http://localhost:9000/api/cp/v1?x=1"
		}, true},
		{"experiments URL rejects fragment", func(cfg *Config) {
			cfg.APIKey = "k"
			cfg.ExperimentsURL = "http://localhost:9000/api/cp/v1#frag"
		}, true},
		{"experiments URL rejects user info", func(cfg *Config) {
			cfg.APIKey = "k"
			cfg.ExperimentsURL = "http://user@localhost:9000/api/cp/v1"
		}, true},
		{"experiments URL requires https outside loopback", func(cfg *Config) {
			cfg.APIKey = "k"
			cfg.ExperimentsURL = "http://cp.example.com/api/cp/v1"
		}, true},
		{"subject key grammar is enforced when set", func(cfg *Config) {
			cfg.APIKey = "k"
			cfg.ExperimentsURL = "http://localhost:9000/api/cp/v1"
			cfg.ExperimentSubjectKey = "spcid_short"
		}, true},
		{"a UUID anonymous id is not a subject key", func(cfg *Config) {
			cfg.APIKey = "k"
			cfg.ExperimentsURL = "http://localhost:9000/api/cp/v1"
			cfg.ExperimentSubjectKey = "0192f3a1-7b1c-7def-8123-4567890abcde"
		}, true},
		{"valid subject key is accepted", func(cfg *Config) {
			cfg.APIKey = "k"
			cfg.ExperimentsURL = "http://localhost:9000/api/cp/v1"
			cfg.ExperimentSubjectKey = testExperimentSubjectKey
		}, false},
		{"subject key without experiments URL is validated too", func(cfg *Config) {
			cfg.ExperimentSubjectKey = "not-an-spcid"
		}, true},
	}
	for _, tc := range cases {
		cfg := base
		tc.mutate(&cfg)
		_, err := normalizeConfig(cfg)
		if tc.wantErr && !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%s: expected ErrInvalidConfig, got %v", tc.name, err)
		}
		if !tc.wantErr && err != nil {
			t.Fatalf("%s: unexpected error %v", tc.name, err)
		}
	}
}

func TestExperimentAssignmentFreshVerdictShapes(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "exp-cache.json")
	client := newExperimentsClient(t, server.URL, cachePath, nil)
	defer client.Close(context.Background())

	result, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout")
	if err != nil {
		t.Fatalf("FetchExperimentAssignment: %v", err)
	}
	if result.FromCache || result.Reason != "" {
		t.Fatalf("expected a fresh verdict, got %+v", result)
	}
	assignment := result.Assignment
	if !assignment.Assigned || assignment.ExperimentKey != "exp-checkout" || assignment.Version != 3 {
		t.Fatalf("unexpected assignment %+v", assignment)
	}
	if assignment.VariantKey != "variant_b" || assignment.VariantPayload["speed"] != float64(2) {
		t.Fatalf("unexpected variant %+v", assignment)
	}
	if assignment.AssignmentKey != "asgn_0123456789abcdef0123456789abcdef" {
		t.Fatalf("unexpected assignment key %q", assignment.AssignmentKey)
	}
	// The subject_fact_key MUST be retained with the verdict: it is the only
	// permitted analytics fact subject for a client_id-unit assignment.
	if assignment.SubjectFactKey != testSubjectFactKey {
		t.Fatalf("expected the subject_fact_key retained, got %q", assignment.SubjectFactKey)
	}
	if assignment.Boundary.AssignmentUnit != "client_id" || assignment.Boundary.TrafficAllocationBasis != 10000 {
		t.Fatalf("unexpected boundary %+v", assignment.Boundary)
	}
	record := readExperimentCacheFile(t, cachePath)
	if len(record.Records) != 1 || record.Records[0].ExperimentKey != "exp-checkout" ||
		record.Records[0].Body != testAssignedBody {
		t.Fatalf("expected the verdict persisted, got %+v", record)
	}

	// All three not-assigned shapes are valid 200s, distinguished by the
	// assignment's own reason: absent (traffic gate), kill_switch,
	// targeting_unmatched.
	notAssigned := []struct {
		body       string
		wantReason string
	}{
		{`{"experiment_key":"exp-checkout","version":3,"assigned":false,"assignment_key":"asgn_00","boundary":{"assignment_unit":"client_id"}}`, ""},
		{`{"experiment_key":"exp-checkout","version":3,"assigned":false,"assignment_key":"asgn_00","reason":"kill_switch","boundary":{"assignment_unit":"client_id"}}`, "kill_switch"},
		{`{"experiment_key":"exp-checkout","version":3,"assigned":false,"assignment_key":"asgn_00","reason":"targeting_unmatched","boundary":{"assignment_unit":"client_id"}}`, "targeting_unmatched"},
	}
	for _, tc := range notAssigned {
		script.Store(rcScriptStep{status: 200, body: tc.body})
		result, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout")
		if err != nil {
			t.Fatalf("not-assigned shape (%q): %v", tc.wantReason, err)
		}
		if result.Assignment.Assigned || result.Assignment.Reason != tc.wantReason {
			t.Fatalf("expected not-assigned with reason %q, got %+v", tc.wantReason, result.Assignment)
		}
		if result.Assignment.VariantKey != "" {
			t.Fatalf("a not-assigned verdict carries no variant, got %+v", result.Assignment)
		}
	}
}

func TestExperimentAssignmentIncompleteVerdictsAreMalformed(t *testing.T) {
	// Syntactically valid JSON that is INCOMPLETE for its verdict shape —
	// or carries the wrong TYPE for a member the SDK relies on — must
	// classify as the transient malformed_response and keep serving the
	// last-known-good record, never install as a fresh verdict.
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "exp-cache.json")
	client := newExperimentsClient(t, server.URL, cachePath, nil)
	defer client.Close(context.Background())
	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	cacheBefore, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}

	incomplete := []struct {
		name string
		body string
	}{
		{"bare experiment_key only", `{"experiment_key":"exp-checkout"}`},
		{"assigned without assignment_key", `{"experiment_key":"exp-checkout","assigned":true,"variant_key":"v","boundary":{"assignment_unit":"synthetic_subject_key"}}`},
		{"assigned without variant_key", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","assigned":true,"boundary":{"assignment_unit":"synthetic_subject_key"}}`},
		{"assigned without boundary", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","assigned":true,"variant_key":"v"}`},
		{"assigned with unknown unit", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","assigned":true,"variant_key":"v","boundary":{"assignment_unit":"device_id"}}`},
		{"assigned client_id without subject_fact_key", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","assigned":true,"variant_key":"v","boundary":{"assignment_unit":"client_id"}}`},
		{"assigned client_id with a raw spcid subject", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","assigned":true,"variant_key":"v","subject_fact_key":"` + testExperimentSubjectKey + `","boundary":{"assignment_unit":"client_id"}}`},
		{"not-assigned with unknown reason", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","assigned":false,"reason":"paused"}`},
		{"fractional version", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","version":3.5}`},
		{"fractional-form integer version", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","version":3.0}`},
		{"negative version", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","version":-1}`},
		{"overflowing version", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","version":9223372036854775808}`},
		{"string version", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","version":"3"}`},
		{"array variant_payload", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","assigned":true,"variant_key":"v","variant_payload":[1,2],"boundary":{"assignment_unit":"synthetic_subject_key"}}`},
		{"string variant_payload", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","assigned":true,"variant_key":"v","variant_payload":"fast","boundary":{"assignment_unit":"synthetic_subject_key"}}`},
		{"number variant_payload", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","assigned":true,"variant_key":"v","variant_payload":7,"boundary":{"assignment_unit":"synthetic_subject_key"}}`},
		{"non-object boundary", `{"experiment_key":"exp-checkout","assignment_key":"asgn_00","assigned":true,"variant_key":"v","boundary":"client_id"}`},
	}
	for _, tc := range incomplete {
		script.Store(rcScriptStep{status: 200, body: tc.body})
		result, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout")
		if err != nil {
			t.Fatalf("%s: expected the LKG cache served over the malformed body, got error %v", tc.name, err)
		}
		if !result.FromCache || result.Reason != "malformed_response" {
			t.Fatalf("%s: expected cache-served malformed_response, got %+v", tc.name, result)
		}
		if !result.Assignment.Assigned || result.Assignment.SubjectFactKey != testSubjectFactKey {
			t.Fatalf("%s: expected the intact LKG verdict served, got %+v", tc.name, result.Assignment)
		}
	}

	cacheAfter, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	if string(cacheBefore) != string(cacheAfter) {
		t.Fatalf("incomplete verdict bodies must never disturb the cache record")
	}
}

func TestExperimentAssignmentTransientsServeCacheThenFail(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "exp-cache.json")
	client := newExperimentsClient(t, server.URL, cachePath, nil)
	defer client.Close(context.Background())

	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err != nil {
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
		{rcScriptStep{status: 429, body: `{}`}, "transient_429"},
		{rcScriptStep{status: 500, body: `oops`}, "transient_500"},
		{rcScriptStep{status: 503, body: `{"error":"kill switch state unavailable"}`}, "transient_503"},
		{rcScriptStep{drop: true}, "http_0"},
		{rcScriptStep{status: 200, body: `not json`}, "malformed_response"},
		{rcScriptStep{status: 200, body: `{"assigned":true}`}, "malformed_response"}, // no experiment_key
		{rcScriptStep{status: 200, body: `{"pad":"` + strings.Repeat("a", expMaxBodyBytes) + `"}`}, "malformed_response"},
	}
	for _, tc := range transients {
		script.Store(tc.step)
		result, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout")
		if err != nil {
			t.Fatalf("%s: expected the cache served, got error %v", tc.reason, err)
		}
		if !result.FromCache || result.Reason != tc.reason {
			t.Fatalf("expected cache-served %s, got %+v", tc.reason, result)
		}
		if !result.Assignment.Assigned || result.Assignment.SubjectFactKey != testSubjectFactKey {
			t.Fatalf("%s: unexpected cached assignment %+v", tc.reason, result.Assignment)
		}
	}

	cacheAfter, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	if string(cacheBefore) != string(cacheAfter) {
		t.Fatalf("transient outcomes must never disturb the cache file")
	}

	// With no usable cache the same outcomes hard-fail with the same codes.
	bare := newExperimentsClient(t, server.URL, "", func(cfg *Config) {
		cfg.AnonymousID = "anon-exp-2"
	})
	defer bare.Close(context.Background())
	script.Store(rcScriptStep{status: 503, body: ``})
	if _, err := bare.FetchExperimentAssignment(context.Background(), "exp-checkout"); err == nil || !strings.Contains(err.Error(), "transient_503") {
		t.Fatalf("expected transient_503 failure without a cache, got %v", err)
	}
}

func TestExperimentAssignmentUnauthorizedFailsClosedNoLatch(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "exp-cache.json")
	client := newExperimentsClient(t, server.URL, cachePath, nil)
	defer client.Close(context.Background())

	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	cacheBefore, _ := os.ReadFile(cachePath)

	// 401, and BOTH generic flag-off 403 wire truths: fail closed per fetch,
	// never a cache drop — only the exact sentinel body (separate test)
	// drops.
	refusals := []rcScriptStep{
		{status: 401, body: `{"error":"invalid runtime token"}`},
		{status: 403, body: `{"error":"experimentation runtime is disabled"}`},
		{status: 403, body: `{"error":"experiment assignment fetch is disabled"}`},
		{status: 403, body: `{"error":"workspace suspended"}`},
	}
	for _, step := range refusals {
		script.Store(step)
		_, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout")
		if err == nil || !strings.Contains(err.Error(), "unauthorized") {
			t.Fatalf("expected unauthorized for %d %q, got %v", step.status, step.body, err)
		}
		cacheAfter, _ := os.ReadFile(cachePath)
		if string(cacheBefore) != string(cacheAfter) {
			t.Fatalf("%d %q must not disturb the cache file", step.status, step.body)
		}
	}

	// No latch: a later 200 under a valid credential resumes, and a later
	// transient still serves the (untouched) cache.
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	if result, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err != nil || result.FromCache {
		t.Fatalf("expected recovery after unauthorized, got %+v %v", result, err)
	}
	script.Store(rcScriptStep{status: 500, body: ``})
	if result, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err != nil || !result.FromCache {
		t.Fatalf("expected the cache served after the unauthorized episode, got %+v %v", result, err)
	}
}

func TestExperimentAssignmentSentinelDropsCacheExactMatchOnly(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "exp-cache.json")
	client := newExperimentsClient(t, server.URL, cachePath, nil)
	defer client.Close(context.Background())

	prime := func() {
		script.Store(rcScriptStep{status: 200, body: testAssignedBody})
		if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err != nil {
			t.Fatalf("priming fetch: %v", err)
		}
	}
	prime()

	// NEAR MISSES: 403 bodies that are NOT the exact sentinel drop nothing —
	// string equality on the parsed `error` member, nothing looser.
	nearMisses := []string{
		`{"error":"Experiment real-subject assignment is disabled"}`,              // case differs
		`{"error":"experiment real-subject assignment is disabled."}`,             // trailing punctuation
		`{"error":"note: experiment real-subject assignment is disabled"}`,        // substring only
		`{"error":{"code":"experiment real-subject assignment is disabled"}}`,     // nested, not a string member
		`experiment real-subject assignment is disabled`,                          // unparseable body
		`{"message":"experiment real-subject assignment is disabled"}`,            // wrong member
		`{"error":"experiment real-subject assignment is disabled","extra":"ok"}`, // extra members are FINE — still exact on error
	}
	for i, body := range nearMisses[:len(nearMisses)-1] {
		script.Store(rcScriptStep{status: 403, body: body})
		if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err == nil || !strings.Contains(err.Error(), "unauthorized") {
			t.Fatalf("near miss %d: expected unauthorized, got %v", i, err)
		}
		script.Store(rcScriptStep{status: 500, body: ``})
		if result, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err != nil || !result.FromCache {
			t.Fatalf("near miss %d must not drop the cache, got %+v %v", i, result, err)
		}
	}

	// THE SENTINEL: exact `error` equality — extra sibling members do not
	// break it — drops the cached record and its subject_fact_key.
	script.Store(rcScriptStep{status: 403, body: nearMisses[len(nearMisses)-1]})
	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err == nil || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("expected the sentinel to classify unauthorized, got %v", err)
	}
	if records := readExperimentCacheFile(t, cachePath); len(records.Records) != 0 {
		t.Fatalf("expected the sentinel to drop the durable record, got %+v", records)
	}
	script.Store(rcScriptStep{status: 500, body: ``})
	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err == nil || !strings.Contains(err.Error(), "transient_500") {
		t.Fatalf("expected a hard transient failure after the sentinel drop (no cache left), got %v", err)
	}

	// And the plain sentinel body drops too (the wire truth's exact shape).
	prime()
	script.Store(rcScriptStep{status: 403, body: `{"error":"experiment real-subject assignment is disabled"}`})
	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err == nil || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("expected the sentinel to classify unauthorized, got %v", err)
	}
	script.Store(rcScriptStep{status: 500, body: ``})
	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err == nil || !strings.Contains(err.Error(), "transient_500") {
		t.Fatalf("expected no cache after the plain sentinel drop, got %v", err)
	}
}

func TestExperimentAssignmentPermanentFailuresNeverServeCache(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "exp-cache.json")
	client := newExperimentsClient(t, server.URL, cachePath, nil)
	defer client.Close(context.Background())
	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	cacheBefore, _ := os.ReadFile(cachePath)

	for _, status := range []int{404, 413, 302} {
		script.Store(rcScriptStep{status: status, body: `{"error":"published experiment not found"}`})
		_, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout")
		want := map[int]string{404: "http_404", 413: "http_413", 302: "http_302"}[status]
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("expected %s, got %v", want, err)
		}
		cacheAfter, _ := os.ReadFile(cachePath)
		if string(cacheBefore) != string(cacheAfter) {
			t.Fatalf("%d must not disturb the cache file", status)
		}
	}
}

func TestExperimentAssignmentPreNetworkFailures(t *testing.T) {
	var requests atomic.Int64
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	server := newRCScriptServer(t, &requests, &script)
	defer server.Close()

	// Not opted in: the consumer is entirely dark.
	unconfigured := newExperimentsClient(t, server.URL, "", func(cfg *Config) {
		cfg.ExperimentsURL = ""
		cfg.ExperimentSubjectKey = ""
	})
	defer unconfigured.Close(context.Background())
	if _, err := unconfigured.FetchExperimentAssignment(context.Background(), "exp-checkout"); err == nil || !strings.Contains(err.Error(), "experiments_not_configured") {
		t.Fatalf("expected experiments_not_configured, got %v", err)
	}

	// No subject key: nothing coherent to fetch.
	noSubject := newExperimentsClient(t, server.URL, "", func(cfg *Config) {
		cfg.ExperimentSubjectKey = ""
	})
	defer noSubject.Close(context.Background())
	if _, err := noSubject.FetchExperimentAssignment(context.Background(), "exp-checkout"); err == nil || !strings.Contains(err.Error(), "subject_key_unavailable") {
		t.Fatalf("expected subject_key_unavailable, got %v", err)
	}

	// Empty experiment key: a caller bug, decided before any network use.
	client := newExperimentsClient(t, server.URL, "", nil)
	defer client.Close(context.Background())
	if _, err := client.FetchExperimentAssignment(context.Background(), "  "); err == nil || !strings.Contains(err.Error(), "experiment_key_required") {
		t.Fatalf("expected experiment_key_required, got %v", err)
	}

	if got := requests.Load(); got != 0 {
		t.Fatalf("pre-network failures must not touch the network, saw %d requests", got)
	}
}

func TestExperimentAssignmentClosedAndCallerContext(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	client := newExperimentsClient(t, server.URL, "", nil)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.FetchExperimentAssignment(canceled, "exp-checkout"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the caller's context error, got %v", err)
	}

	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed after Close, got %v", err)
	}
}

func TestExperimentAssignmentStaleOutcomesAreFenced(t *testing.T) {
	// Unit-level fence proof on installLocked: an outcome older than the
	// newest settled one for its scope installs NOTHING — a stale sentinel
	// drops nothing and a stale 200 rolls nothing back.
	exp := newExperimentsState(Config{
		ExperimentsURL:       "https://cp.example.com/api/cp/v1",
		APIKey:               "k",
		AppID:                "app-test",
		EnvironmentID:        "develop",
		ExperimentSubjectKey: testExperimentSubjectKey,
	})
	scope := exp.scopeFor("exp-checkout")

	// Seq 2 settles a fresh verdict.
	if dropped, persistFailed := exp.installLocked(2, scope, "exp-checkout", &expCache{Body: testAssignedBody, FetchedAtMS: 100}, true, false); dropped || persistFailed {
		t.Fatalf("unexpected install outcome: dropped=%v persistFailed=%v", dropped, persistFailed)
	}
	// A STALE sentinel (seq 1) must not drop the newer install.
	if dropped, _ := exp.installLocked(1, scope, "exp-checkout", nil, true, true); dropped {
		t.Fatal("a stale sentinel must not drop a newer settled record")
	}
	if exp.held[scope] == nil || exp.held[scope].Body != testAssignedBody {
		t.Fatalf("expected the newer record kept, got %+v", exp.held[scope])
	}
	// A STALE 200 (seq 2 again — not newer) must not roll the record back.
	if _, _ = exp.installLocked(2, scope, "exp-checkout", &expCache{Body: `{"experiment_key":"exp-checkout"}`, FetchedAtMS: 50}, true, false); exp.held[scope].Body != testAssignedBody {
		t.Fatalf("a stale 200 must not overwrite a newer settled record, got %+v", exp.held[scope])
	}
	// A NEWER sentinel (seq 3) drops.
	if dropped, _ := exp.installLocked(3, scope, "exp-checkout", nil, true, true); !dropped {
		t.Fatal("expected the newer sentinel to drop the record")
	}
	if exp.held[scope] != nil {
		t.Fatalf("expected the record dropped, got %+v", exp.held[scope])
	}
}

func TestExperimentAssignmentDurableCacheRestartAndScopeMiss(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "exp-cache.json")
	first := newExperimentsClient(t, server.URL, cachePath, nil)
	if _, err := first.FetchExperimentAssignment(context.Background(), "exp-checkout"); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A restart (same scope) preloads the durable record and serves it over
	// a transient failure.
	script.Store(rcScriptStep{status: 503, body: ``})
	restarted := newExperimentsClient(t, server.URL, cachePath, nil)
	defer restarted.Close(context.Background())
	result, err := restarted.FetchExperimentAssignment(context.Background(), "exp-checkout")
	if err != nil || !result.FromCache || result.Reason != "transient_503" {
		t.Fatalf("expected the durable record served across restart, got %+v %v", result, err)
	}
	if result.Assignment.SubjectFactKey != testSubjectFactKey {
		t.Fatalf("expected the subject_fact_key preserved across restart, got %q", result.Assignment.SubjectFactKey)
	}

	// A different subject (a different scope) is a MISS: the record is
	// never served for it.
	otherSubject := newExperimentsClient(t, server.URL, cachePath, func(cfg *Config) {
		cfg.ExperimentSubjectKey = "spcid_other-subject-9876543210"
	})
	defer otherSubject.Close(context.Background())
	if _, err := otherSubject.FetchExperimentAssignment(context.Background(), "exp-checkout"); err == nil || !strings.Contains(err.Error(), "transient_503") {
		t.Fatalf("expected a scope miss to hard-fail the transient, got %v", err)
	}
}

func TestExperimentAssignmentAutoLaneRevalidatesThenHaltsUntilReinit(t *testing.T) {
	var requests atomic.Int64
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	server := newRCScriptServer(t, &requests, &script)
	defer server.Close()

	client := newExperimentsClient(t, server.URL, "", nil)
	defer client.Close(context.Background())

	// Prime one cached key, then start the production lane manually with a
	// shrunk cycle delay (the same seam pattern as the injected jitter).
	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	primed := requests.Load()
	client.exp.revalidateDelayFn = func() time.Duration { return time.Millisecond }
	client.expRevalidateDone = make(chan struct{})
	go client.runExperimentAssignmentRevalidation()

	// The lane revalidates the cached key on its own schedule.
	waitFor(t, 5*time.Second, "automatic assignment revalidation", func() bool {
		return requests.Load() >= primed+2
	})

	// An authoritative 401 received by the LANE halts it until re-init.
	script.Store(rcScriptStep{status: 401, body: `{"error":"invalid runtime token"}`})
	select {
	case <-client.expRevalidateDone:
	case <-time.After(5 * time.Second):
		t.Fatal("expected the automatic lane to halt after an authoritative 401")
	}
	if !client.exp.autoLaneHalted() {
		t.Fatal("expected the halt flag set")
	}
	halted := requests.Load()
	time.Sleep(30 * time.Millisecond)
	if got := requests.Load(); got != halted {
		t.Fatalf("the halted lane must schedule no further fetches, saw %d then %d", halted, got)
	}

	// Host-triggered fetches keep classifying per-fetch — and their success
	// does NOT resume the lane.
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	if result, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err != nil || result.FromCache {
		t.Fatalf("expected the host fetch to classify per-fetch after the halt, got %+v %v", result, err)
	}
	afterHost := requests.Load()
	time.Sleep(30 * time.Millisecond)
	if got := requests.Load(); got != afterHost {
		t.Fatalf("a host-fetch success must not resume the halted lane, saw %d then %d", afterHost, got)
	}
}

func TestExperimentAssignmentAutoLaneSentinelHaltsAndDrops(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "exp-cache.json")
	client := newExperimentsClient(t, server.URL, cachePath, nil)
	defer client.Close(context.Background())
	if _, err := client.FetchExperimentAssignment(context.Background(), "exp-checkout"); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}

	// One automatic cycle against the sentinel: Extra 1 (cache + sfk drop)
	// and Extra 2 (lane halt) both apply.
	script.Store(rcScriptStep{status: 403, body: `{"error":"experiment real-subject assignment is disabled"}`})
	if exit := client.revalidateExperimentAssignmentsOnce(); !exit {
		t.Fatal("expected the lane cycle to report exit on the sentinel 403")
	}
	if !client.exp.autoLaneHalted() {
		t.Fatal("expected the halt flag set by the sentinel 403")
	}
	if records := readExperimentCacheFile(t, cachePath); len(records.Records) != 0 {
		t.Fatalf("expected the sentinel to drop the durable record, got %+v", records)
	}
}

func TestExperimentAssignmentAutoLaneDefaultOffAndCloseStops(t *testing.T) {
	var requests atomic.Int64
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: testAssignedBody})
	server := newRCScriptServer(t, &requests, &script)
	defer server.Close()

	// Default: no interval, no lane goroutine, nothing scheduled — the
	// consumer is host-triggered only (the dark default).
	client := newExperimentsClient(t, server.URL, "", nil)
	if client.expRevalidateDone != nil {
		t.Fatal("expected no automatic lane without the opt-in interval")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Opted in: the lane goroutine exists (idle without cached keys — no
	// fetches) and Close stops it promptly.
	optedIn := newExperimentsClient(t, server.URL, "", func(cfg *Config) {
		cfg.ExperimentAssignmentRevalidateInterval = time.Hour
	})
	if optedIn.expRevalidateDone == nil {
		t.Fatal("expected the automatic lane goroutine with the opt-in interval")
	}
	if err := optedIn.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-optedIn.expRevalidateDone:
	case <-time.After(5 * time.Second):
		t.Fatal("expected Close to stop the automatic lane")
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("an idle lane (no cached keys) must fetch nothing, saw %d requests", got)
	}
}

func TestExperimentAssignmentRevalidateDelayFloorsAt60s(t *testing.T) {
	exp := newExperimentsState(Config{
		ExperimentsURL: "https://cp.example.com/api/cp/v1",
		APIKey:         "k",
	})
	if got := exp.revalidateDelay(0); got != expRevalidateFloor {
		t.Fatalf("expected the 60s floor, got %v", got)
	}
	if got := exp.revalidateDelay(time.Second); got != expRevalidateFloor {
		t.Fatalf("expected the 60s floor over a faster interval, got %v", got)
	}
	if got := exp.revalidateDelay(10 * time.Minute); got != 10*time.Minute {
		t.Fatalf("expected the slower configured interval honored, got %v", got)
	}
}
