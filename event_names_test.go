package shardpilot

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestHostEventNameAdmission(t *testing.T) {
	var scenes []struct {
		name string
		code string
	}
	for _, name := range []string{
		"experiment_exposure", "experiment_outcome", "governed_action_result",
		"app_runtime_ping", "monitor_evaluation_recorded", "llm_usage",
		"support_assistant_feedback", "ad_impression_revenue", "session_started", "session_ended",
	} {
		for _, spelling := range []string{name, "app." + name, " \t\n\v\f\rapp." + name + "\r\n\v\f\t "} {
			scenes = append(scenes, struct{ name, code string }{spelling, "reserved_event_name"})
		}
	}
	for _, name := range []string{"", " ", "\t\r\n\v\f"} {
		scenes = append(scenes, struct{ name, code string }{name, "event_name_required"})
	}
	// Exact, case-sensitive matching after one prefix: no extra grammar and
	// no Unicode whitespace folding. These controls must reach the wire intact.
	for _, name := range []string{
		"ordinary_control", "app.screen_view", "level_start", "level_complete", "level_fail",
		"perf_summary", "network_summary", "purchase", "economy_tx",
		"Session_started", "APP.session_started", "app.app.session_started",
		"app. session_started", "session_started_suffix", "\u00a0session_started\u00a0",
	} {
		scenes = append(scenes, struct{ name, code string }{name, ""})
	}
	for _, method := range []string{"Track", "Enqueue"} {
		for i, scene := range scenes {
			t.Run(fmt.Sprintf("%s/%02d/%s", method, i, scene.name), func(t *testing.T) {
				server, envelopes, requests := newPurchaseCaptureServer(t)
				client := newSourceTestClient(t, server.URL, SourceBackend)
				before := client.Snapshot()
				event := Event{Name: scene.name}
				var err error
				if method == "Track" {
					err = client.Track(context.Background(), event)
				} else {
					err = client.Enqueue(event)
				}
				if flushErr := client.Flush(context.Background()); flushErr != nil {
					t.Fatalf("flush admitted events: %v", flushErr)
				}
				after := client.Snapshot()
				if scene.code != "" {
					if !errors.Is(err, ErrInvalidEvent) || !strings.Contains(err.Error(), scene.code) {
						t.Errorf("name refusal = %v, want ErrInvalidEvent with %s", err, scene.code)
					}
					wantError := ErrReservedEventName
					if scene.code == "event_name_required" {
						wantError = ErrEventNameRequired
					}
					if !errors.Is(err, wantError) || !strings.Contains(after.LastError, scene.code) {
						t.Errorf("sentinel/diagnostic = %v / %q, want %v", err, after.LastError, wantError)
					}
					if after.FailedBatches != before.FailedBatches {
						t.Error("an intake refusal must not count as a failed batch")
					}
					if got := after.Dropped - before.Dropped; got != 1 {
						t.Errorf("dropped delta = %d, want exactly 1", got)
					}
					if after.Enqueued != before.Enqueued || requests.Load() != 0 || len(envelopes) != 0 {
						t.Errorf("refused event entered delivery: enqueued=%d requests=%d envelopes=%d", after.Enqueued-before.Enqueued, requests.Load(), len(envelopes))
					}
					return
				}
				if err != nil {
					t.Fatalf("ordinary event refused: %v", err)
				}
				if requests.Load() != 1 || len(envelopes) != 1 || after.Dropped != before.Dropped {
					t.Fatalf("ordinary event delivery: requests=%d envelopes=%d dropped=%d", requests.Load(), len(envelopes), after.Dropped-before.Dropped)
				}
				if got := (<-envelopes)["event_name"]; got != scene.name {
					t.Errorf("wire name = %q, want unchanged %q", got, scene.name)
				}
			})
		}
	}
}
