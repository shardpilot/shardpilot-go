package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/shardpilot/shardpilot-go/internal/redact"
)

func TestCredentialPercentEncodings(t *testing.T) {
	key := "synthetic/slash key+suffix"
	canonical := url.QueryEscape(key)
	allBytes := ""
	for _, b := range []byte(key) {
		allBytes += fmt.Sprintf("%%%02x", b)
	}
	for name, echo := range map[string]string{
		"raw":           key,
		"canonical":     canonical,
		"lowercase":     strings.ReplaceAll(strings.ReplaceAll(canonical, "%2F", "%2f"), "%2B", "%2b"),
		"percent-space": strings.ReplaceAll(canonical, "+", "%20"),
		"mixed":         "synthetic%2fslash%20key%2Bsuffix",
		"every-byte":    allBytes,
	} {
		t.Run(name, func(t *testing.T) {
			get := func(name string) string {
				if name == "SHARDPILOT_TOKEN" {
					return key
				}
				return environment(name)
			}
			var output bytes.Buffer
			code := run(get, &output, transportFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 400,
					Header: http.Header{"X-Request-Id": {echo}},
					Body:   io.NopCloser(strings.NewReader("unrelated%2f+evidence " + echo)), Request: req}, nil
			}))
			if code != 1 {
				t.Fatalf("unexpected status for invalid acknowledgements: %d", code)
			}
			if strings.Contains(output.String(), echo) {
				t.Fatal("equivalently encoded configured credential survived in printed evidence")
			}
			if !strings.Contains(output.String(), "unrelated%2f+evidence") {
				t.Fatal("redaction changed unrelated percent-encoded evidence")
			}
		})
	}
}

func TestUnlinkableActorRefusedBeforeRequests(t *testing.T) {
	for name, actor := range map[string]string{
		"user-prefix": "user_4242",
		"email":       "actor@example.invalid",
		"too-long":    strings.Repeat("a", 513),
		"ip":          "198.51.100.23",
	} {
		t.Run(name, func(t *testing.T) {
			var output bytes.Buffer
			calls := 0
			get := func(name string) string {
				if name == "SHARDPILOT_ANONYMOUS_ID" {
					return actor
				}
				return environment(name)
			}
			code := run(get, &output, transportFunc(func(*http.Request) (*http.Response, error) {
				calls++
				return nil, fmt.Errorf("unexpected synthetic request")
			}))
			if code != 2 || calls != 0 {
				t.Fatalf("unsafe actor: exit=%d requests=%d, want exit=2 requests=0", code, calls)
			}
		})
	}
}

func TestAcceptedSingleCannotPassOversizeRejection(t *testing.T) {
	event, err := json.Marshal(map[string]string{"event_id": "single", "workspace_id": strings.Repeat("w", 3000)})
	if err != nil || len(event) <= 2048 {
		t.Fatal("oversized accepted fixture was not constructed")
	}
	body, _ := json.Marshal(map[string]any{"events": []json.RawMessage{event}})
	for _, status := range []string{"accepted", "rejected"} {
		t.Run(status, func(t *testing.T) {
			response, _ := json.Marshal(map[string]any{"events": []map[string]string{{
				"event_id": "single", "status": status, "code": "event_too_large",
			}}})
			w := witness{name: "single", records: []exchange{{Status: 202, RequestBody: string(body), ResponseBody: string(response)}}}
			err := w.outcome("accepted")
			if (err == nil) != (status == "accepted") {
				t.Fatalf("accepted single with %s verdict: error=%v", status, err)
			}
		})
	}
}

func TestCrashActorMustMatch(t *testing.T) {
	for _, actor := range []string{"synthetic-actor", "", "different-actor"} {
		request, _ := json.Marshal(map[string]string{"crash_id": "synthetic-crash", "anonymous_id": actor})
		response := `{"crash_id":"synthetic-crash","fingerprint":"synthetic-group"}`
		w := witness{expectedActor: "synthetic-actor", records: []exchange{{Status: 202,
			RequestBody: string(request), ResponseBody: response}}}
		err := w.outcome("crash")
		if (err == nil) != (actor == "synthetic-actor") {
			t.Fatalf("crash actor %q: error=%v", actor, err)
		}
	}
}

func TestRedactionPreservesLiteralEncodingsAndUnicode(t *testing.T) {
	for _, key := range []string{"literal%2Fkey", "synthetic-é+suffix", `synthetic"quoted\key`} {
		r := redact.New(key)
		encoded, _ := json.Marshal(key)
		for _, form := range []string{key, url.QueryEscape(key), string(encoded[1 : len(encoded)-1])} {
			if got := r.Replace("before %zz " + form + " after"); got != "before %zz [REDACTED] after" {
				t.Fatalf("credential form was not redacted without changing surrounding evidence: %q", got)
			}
		}
	}
}

func TestCredentialJSONEscapes(t *testing.T) {
	for _, tc := range []struct{ name, key, echo string }{
		{"unicode", "secret", `\u0073\u0065\u0063\u0072\u0065\u0074`},
		{"mixed-unicode", "secret", `sec\u0072et`},
		{"escaped-slash", "synthetic/slash", `synthetic\/slash`},
		{"standard-escapes", "synthetic\"quoted\\key", `synthetic\"quoted\\key`},
		{"surrogate-pair", "synthetic-\U0001f600", `synthetic-\uD83d\uDe00`},
		{"json-then-percent", "secret", `\u002573%65cret`},
		{"percent-then-json", "secret", `%5Cu0073ecret`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"echo":"` + tc.echo + `","other":"unchanged\u002f+evidence"}`
			if !json.Valid([]byte(body)) {
				t.Fatal("invalid JSON echo fixture")
			}
			get := func(name string) string {
				if name == "SHARDPILOT_TOKEN" {
					return tc.key
				}
				return environment(name)
			}
			var output bytes.Buffer
			code := run(get, &output, transportFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 400, Header: http.Header{"X-Request-Id": {tc.echo}},
					Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
			}))
			if code != 1 {
				t.Fatalf("invalid acknowledgements returned exit %d", code)
			}
			seen := 0
			for _, line := range strings.Split(output.String(), "\n") {
				if !strings.HasPrefix(line, "{") {
					continue
				}
				var e exchange
				var payload map[string]string
				if json.Unmarshal([]byte(line), &e) != nil || json.Unmarshal([]byte(e.ResponseBody), &payload) != nil {
					t.Fatal("printed exchange/response is not valid JSON")
				}
				if payload["echo"] != "[REDACTED]" || e.RequestID != "[REDACTED]" {
					t.Fatal("JSON-escaped credential survived in printed evidence")
				}
				if !strings.Contains(e.ResponseBody, `"other":"unchanged\u002f+evidence"`) {
					t.Fatal("unrelated JSON encoding was changed")
				}
				seen++
			}
			if seen != 7 {
				t.Fatalf("observed %d printed exchanges, want seven", seen)
			}
		})
	}
}

func TestCrashFingerprintMustContainNonWhitespace(t *testing.T) {
	for _, fingerprint := range []string{"", "   ", "\t\r\n", " synthetic-group "} {
		response, _ := json.Marshal(map[string]string{"crash_id": "synthetic-crash", "fingerprint": fingerprint})
		w := witness{expectedActor: "synthetic-actor", records: []exchange{{Status: 202,
			RequestBody:  `{"crash_id":"synthetic-crash","anonymous_id":"synthetic-actor"}`,
			ResponseBody: string(response)}}}
		err := w.outcome("crash")
		if (err == nil) != (strings.TrimSpace(fingerprint) != "") {
			t.Fatalf("blank/nonblank crash fingerprint expectation failed: error=%v", err)
		}
	}
}
