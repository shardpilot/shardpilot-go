package shardpilot

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestEnvelopePlatformCapturedVocabulary(t *testing.T) {
	var capture []struct {
		Name      string `json:"name"`
		Platform  string `json:"platform"`
		Source    string `json:"source"`
		Code      string `json:"code"`
		Published []struct {
			Platform string `json:"platform"`
		} `json:"published"`
	}
	raw, err := os.ReadFile("testdata/analytics-vocabulary.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &capture); err != nil {
		t.Fatal(err)
	}
	other := ""
	for _, row := range capture {
		if row.Name == "canonical/other" {
			other = row.Published[0].Platform
		}
	}
	if other == "" {
		t.Fatal("missing captured fallback")
	}
	for _, row := range capture {
		if !strings.HasPrefix(row.Name, "canonical/") && !strings.HasPrefix(row.Name, "input/") {
			continue
		}
		if len(row.Published) != 1 {
			t.Fatalf("capture never published %s", row.Name)
		}
		want := row.Published[0].Platform
		if want == "" {
			want = other
		}
		for _, override := range []bool{false, true} {
			name := row.Name + "/config"
			if override {
				name = row.Name + "/event"
			}
			t.Run(name, func(t *testing.T) {
				c, _, _ := newPlatformTestClient(t, row.Platform)
				e := Event{Name: "app.session_started"}
				if override {
					e.Platform = row.Platform
					c.cfg.Platform = ""
				}
				env, err := c.buildEnvelope(e)
				if err != nil {
					t.Fatal(err)
				}
				encoded, err := json.Marshal(env)
				if err != nil {
					t.Fatal(err)
				}
				var wire map[string]any
				if err = json.Unmarshal(encoded, &wire); err != nil {
					t.Fatal(err)
				}
				if got := wire["platform"]; got != want {
					t.Errorf("wire platform=%v, captured value=%q", got, want)
				}
				if _, exists := wire["country"]; exists {
					t.Error("country must stay absent")
				}
			})
		}
	}
	for _, row := range capture {
		if !strings.HasPrefix(row.Name, "source/") {
			continue
		}
		t.Run(row.Name, func(t *testing.T) {
			source := Source(row.Source)
			want := row.Code != "source_invalid"
			if got := validSource(source); got != want {
				t.Errorf("validSource=%v want=%v", got, want)
			}
			if want {
				c, _, _ := newPlatformTestClient(t, "windows")
				c.cfg.Source = source
				env, err := c.buildEnvelope(Event{Name: "app.session_started"})
				if err != nil {
					t.Fatal(err)
				}
				if env.Source != source {
					t.Errorf("source=%q want=%q", env.Source, source)
				}
			}
		})
	}
}

func TestEnvelopePlatformEventOverridesConfig(t *testing.T) {
	for _, tc := range []struct{ event, want string }{{"ps5", "ps5"}, {"unknown-device", "other"}, {"   ", "other"}, {"", "windows"}} {
		t.Run(tc.event, func(t *testing.T) {
			c, _, _ := newPlatformTestClient(t, "windows")
			env, err := c.buildEnvelope(Event{Name: "app.session_started", Platform: tc.event})
			if err != nil {
				t.Fatal(err)
			}
			if env.Platform != tc.want {
				t.Errorf("platform=%q want=%q", env.Platform, tc.want)
			}
		})
	}
}
