package crash

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestCrashComponentWireGolden(t *testing.T) {
	for _, tc := range []struct {
		name, configSource, configComponent, eventSource, component string
		fatal                                                       bool
	}{
		{"legacy_default", "main-server", "", "", "main-server", false},
		{"event_override", "main-server", "", "game-server", "game-server", false},
		{"fatal_legacy_default", "main-server", "", "", "main-server", true},
		{"bare_app", "", "", "", "", false},
		{"scrubbed_component", "", "", "sample@example.invalid", "", false},
		{"preferred_default", "", "main-server", "", "main-server", false},
		{"preferred_wins", "old-server", "main-server", "", "main-server", false},
		{"preferred_event_override", "old-server", "main-server", "game-server", "game-server", false},
		{"blank_preferred_falls_back", "old-server", "  ", "", "old-server", false},
		{"trimmed_preferred", "old-server", " main-server ", "", "main-server", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body []byte
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ = io.ReadAll(r.Body)
				w.WriteHeader(http.StatusAccepted)
			}))
			defer server.Close()
			client, err := NewClient(ClientOptions{IngestURL: server.URL, APIKey: "synthetic-api-key", Source: tc.configSource, CrashComponent: tc.configComponent, Sampler: alwaysSampler{}})
			if err != nil {
				t.Fatal(err)
			}
			event := Event{
				CrashID: "018bcfe5-5680-7cc8-a7b8-7f6b0a5969de", OccurredAt: time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC),
				App: AppInfo{ID: "synthetic-app"}, Source: tc.eventSource, Platform: "linux", OS: OSInfo{Name: "linux"},
				Exception: ExceptionInfo{Type: "SyntheticCrash"}, RawText: "synthetic crash",
			}
			if tc.fatal {
				err = client.EmitFatal(context.Background(), event)
			} else {
				err = client.Emit(context.Background(), event)
			}
			if err != nil {
				t.Fatal(err)
			}
			var wire map[string]json.RawMessage
			if err := json.Unmarshal(body, &wire); err != nil {
				t.Fatal(err)
			}
			if _, old := wire["source"]; old {
				t.Errorf("crash body contains retired source member: %s", body)
			}
			keys := make([]string, 0, len(wire))
			for key := range wire {
				keys = append(keys, key)
			}
			want := []string{"app", "crash_id", "exception", "fatal", "modules", "occurred_at", "os", "platform", "raw_text"}
			if tc.component != "" {
				want = append(want, "component")
				if string(wire["component"]) != `"`+tc.component+`"` {
					t.Errorf("component = %s, want %q", wire["component"], tc.component)
				}
			}
			sort.Strings(keys)
			sort.Strings(want)
			if !reflect.DeepEqual(keys, want) {
				t.Errorf("golden keys = %v, want %v", keys, want)
			}
			if string(wire["fatal"]) != map[bool]string{true: "true", false: "false"}[tc.fatal] {
				t.Errorf("fatal = %s, want %v", wire["fatal"], tc.fatal)
			}
		})
	}
}
