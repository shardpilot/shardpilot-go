package shardpilot

import (
	"strings"
	"testing"
)

func TestExperimentSubjectIDMintShapeAndGrammar(t *testing.T) {
	minted, err := mintExperimentSubjectID()
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if len(minted) != len("spcid_")+32 {
		t.Fatalf("expected spcid_ + 32 chars, got %q (len %d)", minted, len(minted))
	}
	if !strings.HasPrefix(minted, "spcid_") {
		t.Fatalf("expected spcid_ prefix, got %q", minted)
	}
	body := minted[len("spcid_"):]
	for i := 0; i < len(body); i++ {
		ch := body[i]
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			t.Fatalf("expected 32 lowercase hex chars, got %q", minted)
		}
	}
	if !validExperimentSubjectID(minted) {
		t.Fatalf("minted id must satisfy the wire grammar: %q", minted)
	}
	second, err := mintExperimentSubjectID()
	if err != nil {
		t.Fatalf("mint second: %v", err)
	}
	if second == minted {
		t.Fatalf("two mints must differ")
	}
}

func TestExperimentSubjectIDGrammarAcceptsFullWireRange(t *testing.T) {
	// Loads accept the FULL wire grammar (stickiness across SDK builds),
	// not just this SDK's own 32-hex mint shape.
	valid := []string{
		"spcid_" + strings.Repeat("a", 20),
		"spcid_" + strings.Repeat("Z", 64),
		"spcid_ABCdef012345678_-abc",
	}
	for _, value := range valid {
		if !validExperimentSubjectID(value) {
			t.Fatalf("expected %q to be grammar-valid", value)
		}
	}
	invalid := []string{
		"",
		"spcid_" + strings.Repeat("a", 19),       // body too short
		"spcid_" + strings.Repeat("a", 65),       // body too long
		"spc1d_" + strings.Repeat("a", 32),       // wrong prefix
		"SPCID_" + strings.Repeat("a", 32),       // prefix case-sensitive
		"spcid_" + strings.Repeat("a", 31) + "!", // charset
		"spcid_" + strings.Repeat("a", 20) + " ", // trailing space
		"user@example.com",
	}
	for _, value := range invalid {
		if validExperimentSubjectID(value) {
			t.Fatalf("expected %q to be rejected", value)
		}
	}
}

func TestBuildExperimentAssignmentURLEscapesAndOrders(t *testing.T) {
	url := buildExperimentAssignmentURL(
		"https://cp.example/",
		"app 1", "dev&env", "exp=key", "spcid_"+strings.Repeat("a", 32),
		[]expAttribute{
			{Name: "app_version", Value: "1.2.3"},
			{Name: "custom_attribute_tier", Value: "gold#1"},
		},
	)
	want := "https://cp.example/api/v1/runtime/experiments/assignment?" +
		"app_key=app%201&environment_key=dev%26env&experiment_key=exp%3Dkey" +
		"&subject_key=spcid_" + strings.Repeat("a", 32) +
		"&app_version=1.2.3&custom_attribute_tier=gold%231"
	if url != want {
		t.Fatalf("url mismatch:\n got %q\nwant %q", url, want)
	}
}

func TestNormalizeExperimentAttributes(t *testing.T) {
	pairs, dropped := normalizeExperimentAttributes(map[string]string{
		"geo":                   "  DE ",
		"app_version":           "1.0.0",
		"device_type":           "",                       // empty after trim: dropped
		"install_date":          strings.Repeat("v", 513), // oversized: dropped
		"user_segment":          "whale",
		"custom_attribute_tier": "gold",
		"custom_attribute_":     "no-suffix", // suffix 0: dropped
		"custom_attribute_" + strings.Repeat("n", 65): "suffix-too-long", // dropped
		"invented_name": "nope", // outside vocabulary: dropped
	})
	if dropped != 5 {
		// device_type (empty), install_date (oversized), custom_attribute_
		// (empty suffix), the 65-char custom suffix, and the invented name.
		t.Fatalf("expected 5 dropped, got %d (pairs %v)", dropped, pairs)
	}
	wantOrder := []string{"app_version", "custom_attribute_tier", "geo", "user_segment"}
	if len(pairs) != len(wantOrder) {
		t.Fatalf("expected %d pairs, got %v", len(wantOrder), pairs)
	}
	for i, name := range wantOrder {
		if pairs[i].Name != name {
			t.Fatalf("expected sorted pair %d to be %q, got %q", i, name, pairs[i].Name)
		}
	}
	if pairs[2].Value != "DE" {
		t.Fatalf("expected trimmed geo value, got %q", pairs[2].Value)
	}

	// The 64-attribute cap drops beyond the cap in sorted order.
	many := make(map[string]string, 70)
	for i := 0; i < 70; i++ {
		many["custom_attribute_k"+padTestIndex(i)] = "v"
	}
	pairs, dropped = normalizeExperimentAttributes(many)
	if len(pairs) != expMaxAttributes || dropped != 6 {
		t.Fatalf("expected 64 kept / 6 dropped, got %d / %d", len(pairs), dropped)
	}

	if pairs, dropped := normalizeExperimentAttributes(nil); pairs != nil || dropped != 0 {
		t.Fatalf("nil attributes must normalize to none")
	}
}

func padTestIndex(i int) string {
	return string([]byte{'0' + byte(i/10), '0' + byte(i%10)})
}

func TestBuildExperimentScopeDimensions(t *testing.T) {
	base := buildExperimentScope("ws", "app", "env", "spcid_"+strings.Repeat("a", 32), "https://cp.example", "fp1")
	variants := []string{
		buildExperimentScope("ws2", "app", "env", "spcid_"+strings.Repeat("a", 32), "https://cp.example", "fp1"),
		buildExperimentScope("ws", "app2", "env", "spcid_"+strings.Repeat("a", 32), "https://cp.example", "fp1"),
		buildExperimentScope("ws", "app", "env2", "spcid_"+strings.Repeat("a", 32), "https://cp.example", "fp1"),
		buildExperimentScope("ws", "app", "env", "spcid_"+strings.Repeat("b", 32), "https://cp.example", "fp1"),
		buildExperimentScope("ws", "app", "env", "spcid_"+strings.Repeat("a", 32), "https://other.example", "fp1"),
		// The credential dimension: an in-place API-key swap must scope-miss.
		buildExperimentScope("ws", "app", "env", "spcid_"+strings.Repeat("a", 32), "https://cp.example", "fp2"),
	}
	for i, variant := range variants {
		if variant == base {
			t.Fatalf("scope dimension %d did not change the scope", i)
		}
	}
	// Injectivity: shifting a separator-adjacent character across components
	// must not collide (components are escaped; the separator cannot appear
	// escaped).
	a := buildExperimentScope("w", "sapp", "env", "spcid_"+strings.Repeat("a", 32), "u", "f")
	b := buildExperimentScope("ws", "app", "env", "spcid_"+strings.Repeat("a", 32), "u", "f")
	if a == b {
		t.Fatalf("scope join must be injective")
	}
	if experimentAPIKeyFingerprint("key-a") == experimentAPIKeyFingerprint("key-b") {
		t.Fatalf("distinct keys must fingerprint distinctly")
	}
	if fp := experimentAPIKeyFingerprint("key-a"); strings.Contains(fp, "key-a") || len(fp) != 16 {
		t.Fatalf("fingerprint must be a short digest, got %q", fp)
	}
}

func expTestResponse(status int, body string) remoteConfigResponse {
	return remoteConfigResponse{status: status, body: []byte(body)}
}

const expTestScopeKey = "exp-checkout"

func expTestRequestScope() expRequestScope {
	return expRequestScope{appKey: "app-test", envKey: "develop", experimentKey: expTestScopeKey}
}

func expAssignedBody(version string) string {
	return `{"app_key":"app-test","environment_key":"develop","experiment_key":"` + expTestScopeKey + `",` +
		`"version":` + version + `,"assigned":true,"assignment_key":"asgn_abc","variant_key":"treatment",` +
		`"variant_payload":{"speed":2},"subject_fact_key":"sfk1_` + strings.Repeat("a", 64) + `",` +
		`"boundary":{"assignment_unit":"client_id"}}`
}

func TestParseExperimentVerdictShapes(t *testing.T) {
	scope := expTestRequestScope()

	result, outcome, ok := parseExperimentVerdict(expTestResponse(200, expAssignedBody("3")), scope, 42)
	if !ok || !result.Assigned || result.VariantKey != "treatment" || result.Version != 3 {
		t.Fatalf("expected assigned verdict, got ok=%v result=%+v", ok, result)
	}
	if outcome.newEntry == nil || outcome.newEntry.SubjectFactKey == "" ||
		outcome.newEntry.AssignmentUnit != "client_id" || outcome.newEntry.FetchedAtMS != 42 {
		t.Fatalf("expected complete cache entry, got %+v", outcome.newEntry)
	}
	if !outcome.authoritative {
		t.Fatalf("a fresh verdict is authoritative")
	}

	malformed := []struct {
		name string
		body string
	}{
		{"not json", `nope`},
		{"array", `[]`},
		{"missing assigned", `{"experiment_key":"` + expTestScopeKey + `"}`},
		{"assigned missing version", `{"assigned":true,"assignment_key":"a","variant_key":"v","boundary":{"assignment_unit":"client_id"}}`},
		{"assigned zero version", strings.Replace(expAssignedBody("3"), `"version":3`, `"version":0`, 1)},
		{"assigned negative version", strings.Replace(expAssignedBody("3"), `"version":3`, `"version":-1`, 1)},
		{"assigned fractional version", strings.Replace(expAssignedBody("3"), `"version":3`, `"version":3.5`, 1)},
		{"assigned missing assignment_key", strings.Replace(expAssignedBody("3"), `"assignment_key":"asgn_abc"`, `"assignment_key":""`, 1)},
		{"assigned missing variant_key", strings.Replace(expAssignedBody("3"), `"variant_key":"treatment"`, `"variant_key":" "`, 1)},
		{"assigned missing unit", strings.Replace(expAssignedBody("3"), `{"assignment_unit":"client_id"}`, `{}`, 1)},
		{"assigned array payload", strings.Replace(expAssignedBody("3"), `{"speed":2}`, `[1,2]`, 1)},
		{"unknown reason", `{"assigned":false,"reason":"experiment_paused"}`},
		{"echo app mismatch", strings.Replace(expAssignedBody("3"), `"app_key":"app-test"`, `"app_key":"other-app"`, 1)},
		{"echo env mismatch", strings.Replace(expAssignedBody("3"), `"environment_key":"develop"`, `"environment_key":"prod"`, 1)},
		{"echo experiment mismatch", strings.Replace(expAssignedBody("3"), `"experiment_key":"`+expTestScopeKey+`"`, `"experiment_key":"exp-other"`, 1)},
	}
	for _, tc := range malformed {
		if _, _, ok := parseExperimentVerdict(expTestResponse(200, tc.body), scope, 42); ok {
			t.Fatalf("%s: expected malformed", tc.name)
		}
	}

	// The three not-assigned shapes are valid verdicts that drop the entry.
	notAssigned := []struct {
		body       string
		wantReason string
	}{
		{`{"assigned":false}`, ""},
		{`{"assigned":false,"reason":"kill_switch","version":3}`, "kill_switch"},
		{`{"assigned":false,"reason":"targeting_unmatched"}`, "targeting_unmatched"},
	}
	for _, tc := range notAssigned {
		result, outcome, ok := parseExperimentVerdict(expTestResponse(200, tc.body), scope, 42)
		if !ok || result.Assigned || result.Reason != tc.wantReason {
			t.Fatalf("body %q: expected not-assigned reason %q, got ok=%v %+v", tc.body, tc.wantReason, ok, result)
		}
		if !outcome.authoritative || !outcome.dropEntry {
			t.Fatalf("body %q: a not-assigned verdict drops the cached entry", tc.body)
		}
	}

	// Absent echo fields are tolerated (the parser validates presence-and-
	// different only).
	if _, _, ok := parseExperimentVerdict(expTestResponse(200, `{"assigned":false}`), scope, 42); !ok {
		t.Fatalf("absent echo fields must be tolerated")
	}

	// An incomplete (stalled) 200 body is never a verdict.
	stalled := expTestResponse(200, expAssignedBody("3"))
	stalled.bodyIncomplete = true
	if _, _, ok := parseExperimentVerdict(stalled, scope, 42); ok {
		t.Fatalf("a stalled 200 body must classify malformed")
	}
}

func TestApplyExperimentAssignmentClassification(t *testing.T) {
	scope := expTestRequestScope()
	cached := &expEntry{
		AssignmentKey:  "asgn_cached",
		VariantKey:     "cached-variant",
		Version:        2,
		AssignmentUnit: "client_id",
		SubjectFactKey: "sfk1_" + strings.Repeat("b", 64),
		SubjectKey:     "spcid_" + strings.Repeat("a", 32),
		FetchedAtMS:    10,
	}

	t.Run("fresh assigned", func(t *testing.T) {
		result, outcome, failure := applyExperimentAssignment(cached, expTestResponse(200, expAssignedBody("3")), scope, 42)
		if failure != "" || !result.Assigned || result.FromCache || outcome.newEntry == nil {
			t.Fatalf("unexpected: result=%+v outcome=%+v failure=%q", result, outcome, failure)
		}
	})

	t.Run("malformed serves cache", func(t *testing.T) {
		result, outcome, failure := applyExperimentAssignment(cached, expTestResponse(200, `{"broken":`), scope, 42)
		if failure != "" || !result.FromCache || result.Code != "malformed_response" || result.VariantKey != "cached-variant" {
			t.Fatalf("expected cache serve, got result=%+v failure=%q", result, failure)
		}
		if outcome.authoritative || !outcome.transient {
			t.Fatalf("malformed is transient, got %+v", outcome)
		}
	})

	t.Run("malformed without cache fails", func(t *testing.T) {
		_, _, failure := applyExperimentAssignment(nil, expTestResponse(200, `{}`), scope, 42)
		if failure != "malformed_response" {
			t.Fatalf("expected malformed_response, got %q", failure)
		}
	})

	t.Run("401 fails closed cache untouched", func(t *testing.T) {
		result, outcome, failure := applyExperimentAssignment(cached, expTestResponse(401, `{"error":"invalid runtime token"}`), scope, 42)
		if failure != "unauthorized" || result.FromCache {
			t.Fatalf("expected fail-closed, got result=%+v failure=%q", result, failure)
		}
		if !outcome.authoritative || !outcome.authBlocked || outcome.dropAll || outcome.dropEntry {
			t.Fatalf("401 latches without dropping, got %+v", outcome)
		}
	})

	t.Run("403 sentinel drops all; near misses stay generic", func(t *testing.T) {
		_, outcome, _ := applyExperimentAssignment(cached, expTestResponse(403, `{"error":"experiment real-subject assignment is disabled"}`), scope, 42)
		if !outcome.dropAll || !outcome.authBlocked {
			t.Fatalf("expected sentinel drop_all, got %+v", outcome)
		}
		nearMisses := []remoteConfigResponse{
			expTestResponse(403, `{"error":"experimentation runtime is disabled"}`),
			expTestResponse(403, `{"error":"experiment real-subject assignment is disabled "}`),
			expTestResponse(403, `{"error":"EXPERIMENT REAL-SUBJECT ASSIGNMENT IS DISABLED"}`),
			expTestResponse(403, `experiment real-subject assignment is disabled`),
			expTestResponse(403, `{"message":"experiment real-subject assignment is disabled"}`),
			expTestResponse(403, `{"error":"workspace suspended"}`),
			// A 401 carrying the sentinel body is NOT the sentinel (status-gated).
			expTestResponse(401, `{"error":"experiment real-subject assignment is disabled"}`),
		}
		for i, resp := range nearMisses {
			_, outcome, failure := applyExperimentAssignment(cached, resp, scope, 42)
			if outcome.dropAll {
				t.Fatalf("near miss %d must not drop the record", i)
			}
			if failure != "unauthorized" || !outcome.authBlocked {
				t.Fatalf("near miss %d still fails closed, got outcome=%+v failure=%q", i, outcome, failure)
			}
		}
		// A truncated sentinel body cannot be trusted for equality.
		truncated := expTestResponse(403, `{"error":"experiment real-subject assignment is disabled"}`)
		truncated.bodyIncomplete = true
		_, outcome, _ = applyExperimentAssignment(cached, truncated, scope, 42)
		if outcome.dropAll {
			t.Fatalf("a truncated body is never a sentinel")
		}
	})

	t.Run("404 permanent drop, first-class not-assigned", func(t *testing.T) {
		result, outcome, failure := applyExperimentAssignment(cached, expTestResponse(404, `{"error":"published experiment not found"}`), scope, 42)
		if failure != "" || result.Assigned || result.Code != "not_found" || result.FromCache {
			t.Fatalf("expected not_found result, got %+v failure=%q", result, failure)
		}
		if !outcome.authoritative || !outcome.dropEntry {
			t.Fatalf("404 drops the entry, got %+v", outcome)
		}
	})

	t.Run("400 grammar sentinel remints; other 400 drops", func(t *testing.T) {
		_, outcome, failure := applyExperimentAssignment(cached, expTestResponse(400, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`), scope, 42)
		if failure != "bad_request" || !outcome.remint || outcome.dropEntry {
			t.Fatalf("expected remint outcome, got %+v failure=%q", outcome, failure)
		}
		_, outcome, failure = applyExperimentAssignment(cached, expTestResponse(400, `{"error":"subject_key is required"}`), scope, 42)
		if failure != "bad_request" || outcome.remint || !outcome.dropEntry || !outcome.authoritative {
			t.Fatalf("expected permanent 400 drop, got %+v failure=%q", outcome, failure)
		}
		truncated := expTestResponse(400, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`)
		truncated.bodyIncomplete = true
		_, outcome, _ = applyExperimentAssignment(cached, truncated, scope, 42)
		if outcome.remint || !outcome.dropEntry {
			t.Fatalf("a truncated grammar sentinel stays a generic 400 drop, got %+v", outcome)
		}
	})

	t.Run("transient bucket serves cache and paces", func(t *testing.T) {
		for _, status := range []int{0, 408, 429, 500, 503} {
			result, outcome, failure := applyExperimentAssignment(cached, expTestResponse(status, ``), scope, 42)
			if failure != "" || !result.FromCache {
				t.Fatalf("status %d: expected cache serve, got %+v failure=%q", status, result, failure)
			}
			if outcome.authoritative || !outcome.transient {
				t.Fatalf("status %d: expected transient, got %+v", status, outcome)
			}
		}
		resp := expTestResponse(429, ``)
		resp.retryAfterRaw = "120"
		_, outcome, _ := applyExperimentAssignment(cached, resp, scope, 42)
		if !outcome.retryAfterPresent || outcome.retryAfterSeconds != 120 {
			t.Fatalf("Retry-After must ride the transient outcome, got %+v", outcome)
		}
		resp.status = 503
		_, outcome, _ = applyExperimentAssignment(cached, resp, scope, 42)
		if !outcome.retryAfterPresent {
			t.Fatalf("Retry-After honored on 5xx too")
		}
		// 408 carries no Retry-After contract.
		resp = expTestResponse(408, ``)
		resp.retryAfterRaw = "120"
		_, outcome, _ = applyExperimentAssignment(cached, resp, scope, 42)
		if outcome.retryAfterPresent {
			t.Fatalf("408 must not arm Retry-After pacing")
		}
	})

	t.Run("stalled 401 fails closed; stalled 200 is transient", func(t *testing.T) {
		stalled := expTestResponse(401, ``)
		stalled.bodyIncomplete = true
		_, outcome, failure := applyExperimentAssignment(cached, stalled, scope, 42)
		if failure != "unauthorized" || !outcome.authBlocked {
			t.Fatalf("a stalled 401 still fails closed, got %+v failure=%q", outcome, failure)
		}
		stalled = expTestResponse(200, expAssignedBody("3"))
		stalled.bodyIncomplete = true
		result, outcome, failure := applyExperimentAssignment(cached, stalled, scope, 42)
		if failure != "" || !result.FromCache || outcome.authoritative {
			t.Fatalf("a stalled 200 serves cache transiently, got %+v failure=%q", result, failure)
		}
	})

	t.Run("out-of-contract statuses are transient serve-stale", func(t *testing.T) {
		// The full status table: every status this contract never sends
		// lands in the EXPLICIT transient bucket — serve the last-known-good
		// and let the cadence retry. None may claim an authoritative
		// no-serve while the getters keep serving the cached entry (the
		// forbidden half-state), and none may drop or latch anything.
		for _, status := range []int{301, 302, 303, 304, 307, 402, 405, 409, 410, 413, 418, 422, 451} {
			result, outcome, failure := applyExperimentAssignment(cached, expTestResponse(status, ``), scope, 42)
			if failure != "" || !result.FromCache || result.Code != "http_"+itoa(status) {
				t.Fatalf("status %d: expected transient serve-stale, got %+v failure=%q", status, result, failure)
			}
			if !outcome.transient || outcome.authoritative || outcome.dropEntry || outcome.dropAll || outcome.authBlocked {
				t.Fatalf("status %d: expected the transient bucket with cache untouched, got %+v", status, outcome)
			}
			// With NO cached entry the same status is the closed transient
			// failure — code intact, still nothing dropped or latched.
			bare, bareOutcome, bareFailure := applyExperimentAssignment(nil, expTestResponse(status, ``), scope, 42)
			if bareFailure != "http_"+itoa(status) || bare.Assigned || !bareOutcome.transient || bareOutcome.authoritative {
				t.Fatalf("status %d without cache: expected closed transient failure, got %+v failure=%q", status, bare, bareFailure)
			}
		}
	})
}

func itoa(v int) string {
	digits := ""
	for v > 0 {
		digits = string([]byte{'0' + byte(v%10)}) + digits
		v /= 10
	}
	return digits
}

func TestExperimentExposureEventIDDeterminism(t *testing.T) {
	base := experimentExposureEventID("marker", "spcid_a", "exp", 3, 0)
	if base != experimentExposureEventID("marker", "spcid_a", "exp", 3, 0) {
		t.Fatalf("same tuple must derive the same id")
	}
	if len(base) != 36 || base[8] != '-' || base[13] != '-' || base[14] != '4' {
		t.Fatalf("expected uuid-shaped deterministic id, got %q", base)
	}
	variants := []string{
		experimentExposureEventID("marker2", "spcid_a", "exp", 3, 0),
		experimentExposureEventID("marker", "spcid_b", "exp", 3, 0),
		experimentExposureEventID("marker", "spcid_a", "exp2", 3, 0),
		experimentExposureEventID("marker", "spcid_a", "exp", 4, 0),
		experimentExposureEventID("marker", "spcid_a", "exp", 3, 1),
	}
	for i, variant := range variants {
		if variant == base {
			t.Fatalf("dimension %d must change the id", i)
		}
	}
}

func TestDeepCopyJSONMapIsolatesAndBounds(t *testing.T) {
	source := map[string]any{
		"nested": map[string]any{"list": []any{1.0, map[string]any{"deep": true}}},
		"scalar": "v",
	}
	copied := deepCopyJSONMap(source, 0)
	copied["scalar"] = "mutated"
	copied["nested"].(map[string]any)["list"].([]any)[0] = 99.0
	if source["scalar"] != "v" || source["nested"].(map[string]any)["list"].([]any)[0] != 1.0 {
		t.Fatalf("mutating the copy must not corrupt the source")
	}
	// Depth cap: a 20-deep chain truncates instead of walking forever.
	deep := map[string]any{}
	cursor := deep
	for i := 0; i < 20; i++ {
		next := map[string]any{}
		cursor["d"] = next
		cursor = next
	}
	if deepCopyJSONMap(deep, 0) == nil {
		t.Fatalf("the top level must copy")
	}
}

func TestSanitizeExperimentEntriesDropsMalformed(t *testing.T) {
	entries := map[string]expEntry{
		"good":         {AssignmentKey: "a", VariantKey: "v", Version: 1, AssignmentUnit: "client_id", FetchedAtMS: 5},
		"no-key":       {VariantKey: "v", Version: 1, AssignmentUnit: "client_id"},
		"no-variant":   {AssignmentKey: "a", Version: 1, AssignmentUnit: "client_id"},
		"zero-version": {AssignmentKey: "a", VariantKey: "v", Version: 0, AssignmentUnit: "client_id"},
		"no-unit":      {AssignmentKey: "a", VariantKey: "v", Version: 1},
	}
	out := sanitizeExperimentEntries(entries)
	if len(out) != 1 {
		t.Fatalf("expected only the good entry, got %v", out)
	}
	if _, ok := out["good"]; !ok {
		t.Fatalf("the good entry must survive")
	}
}
