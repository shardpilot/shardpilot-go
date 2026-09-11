package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func environment(name string) string {
	return map[string]string{
		"SHARDPILOT_INGEST_URL":       "https://collector.invalid",
		"SHARDPILOT_TOKEN":            "synthetic-analytics-token",
		"SHARDPILOT_WORKSPACE_ID":     "synthetic-workspace",
		"SHARDPILOT_APP_ID":           "synthetic-app",
		"SHARDPILOT_ENVIRONMENT_ID":   "test",
		"SHARDPILOT_ANONYMOUS_ID":     "synthetic-actor",
		"SHARDPILOT_CRASH_INGEST_URL": "https://crashes.invalid",
		"SHARDPILOT_API_KEY":          "synthetic-crash-token",
	}[name]
}

func TestSenderEvidence(t *testing.T) {
	for _, defect := range []string{"", "accept_oversize", "accept_unauthenticated", "suppress", "missing_verdict", "wrong_id", "crash_failure", "echo_key", "write_failure"} {
		t.Run(defect, func(t *testing.T) {
			var output bytes.Buffer
			calls, analytics, crashes, mixed := 0, 0, 0, 0
			transport := transportFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				body, err := io.ReadAll(req.Body)
				if err != nil {
					t.Fatal(err)
				}
				status := http.StatusAccepted
				var result any
				if req.URL.Path == "/v1/events:batch" {
					analytics++
					var batch struct {
						Events []json.RawMessage `json:"events"`
					}
					if err := json.Unmarshal(body, &batch); err != nil {
						t.Fatal(err)
					}
					var verdicts []map[string]string
					large, small := 0, 0
					for _, raw := range batch.Events {
						var event struct {
							ID      string `json:"event_id"`
							Name    string `json:"event_name"`
							Source  string `json:"source"`
							Session string `json:"session_id"`
						}
						if err := json.Unmarshal(raw, &event); err != nil {
							t.Fatal(err)
						}
						if event.ID == "" || event.Session == "" || event.Source != "client" || !strings.HasPrefix(event.Name, "app.session_") {
							t.Fatalf("invalid fixture envelope: %s", raw)
						}
						v := map[string]string{"event_id": event.ID, "status": "accepted"}
						if len(raw) > 2048 {
							large++
							if defect != "accept_oversize" {
								v["status"], v["code"] = "rejected", "event_too_large"
							}
						} else {
							small++
						}
						if defect == "suppress" {
							v["status"] = "suppressed_no_consent"
						}
						if defect == "wrong_id" {
							v["event_id"] = "not-sent"
						}
						verdicts = append(verdicts, v)
					}
					if large > 0 && small > 0 {
						mixed++
					}
					if defect == "missing_verdict" {
						verdicts = nil
					}
					result = map[string]any{"events": verdicts}
					if req.Header.Get("Authorization") == "" && defect != "accept_unauthenticated" {
						status = http.StatusUnauthorized
						result = map[string]any{"error": map[string]string{"code": "unauthorized"}}
					}
				} else if req.URL.Path == "/api/v1/crashes/ingest" {
					crashes++
					var event struct {
						ID string `json:"crash_id"`
					}
					if err := json.Unmarshal(body, &event); err != nil {
						t.Fatal(err)
					}
					result = map[string]any{"crash_id": event.ID, "fingerprint": "synthetic-group", "suppressed": defect == "suppress"}
					if defect == "crash_failure" {
						status = http.StatusServiceUnavailable
					}
				} else {
					t.Fatalf("unexpected route: %s", req.URL.Path)
				}
				if defect == "echo_key" {
					result = map[string]string{"error": environment("SHARDPILOT_TOKEN") + environment("SHARDPILOT_API_KEY")}
				}
				encoded, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				return &http.Response{StatusCode: status, Header: http.Header{"X-Request-Id": {"synthetic-request"}}, Body: io.NopCloser(bytes.NewReader(encoded)), Request: req}, nil
			})
			var sink io.Writer = &output
			if defect == "write_failure" {
				sink = rejectPanicWriter{&output}
			}
			code := run(environment, sink, transport)
			if defect == "" {
				if code != 0 || analytics != 4 || crashes != 3 || mixed != 1 {
					t.Fatalf("code=%d analytics=%d crashes=%d mixed=%d\n%s", code, analytics, crashes, mixed, &output)
				}
			} else if code == 0 {
				t.Fatalf("false success for %s\n%s", defect, &output)
			}
			if calls == 0 {
				t.Fatal("no SDK request was exercised")
			}
			for _, secret := range []string{environment("SHARDPILOT_TOKEN"), environment("SHARDPILOT_API_KEY")} {
				if strings.Contains(output.String(), secret) {
					t.Fatal("output contains configured credential")
				}
			}
			for _, field := range []string{"status", "response_body", "request_body", "request_id", "latency_ms"} {
				if !strings.Contains(output.String(), field) {
					t.Errorf("missing evidence field %s", field)
				}
			}
		})
	}
}

func TestConfigurationRefusesBeforeNetwork(t *testing.T) {
	for _, name := range []string{"SHARDPILOT_TOKEN", "SHARDPILOT_INGEST_URL", "SHARDPILOT_API_KEY", "SHARDPILOT_ANONYMOUS_ID"} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			get := func(key string) string {
				if key == name {
					return ""
				}
				return environment(key)
			}
			code := run(get, &out, transportFunc(func(*http.Request) (*http.Response, error) {
				t.Fatal("configuration failure reached transport")
				return nil, fmt.Errorf("unreachable")
			}))
			if code != 2 {
				t.Fatalf("exit=%d, want configuration exit2", code)
			}
		})
	}
}

// A write failure only during CapturePanic must not be swallowed by its no-error API.
type rejectPanicWriter struct{ io.Writer }

func (w rejectPanicWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte(`"case":"go-panic"`)) {
		return 0, errors.New("synthetic output failure")
	}
	return w.Writer.Write(p)
}

func TestOversizeRecordingFailureCannotReopenAttempt(t *testing.T) {
	w := &witness{done: make(chan struct{}), base: transportFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("unrecordable request reached network")
		return nil, nil
	})}
	for i := 0; i < 2; i++ {
		req, err := http.NewRequest(http.MethodPost, "https://collector.invalid/v1/events:batch", strings.NewReader(strings.Repeat("x", bodyLimit+1)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.RoundTrip(req); err == nil {
			t.Fatal("unrecordable request accepted")
		}
	}
}
