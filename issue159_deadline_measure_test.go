package shardpilot

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRestoredDeadlineAfterWithdrawalMeasurement(t *testing.T) {
	for _, tc := range []struct {
		name       string
		deadline   bool
		withdrawal string
		wantAuto   bool
	}{
		{"deadline_age_refusal", true, "age", true},
		{"deadline_sentinel", true, "sentinel", true},
		{"retained_deadline_control", true, "none", false},
		{"no_deadline_age_control", false, "age", true},
		{"no_deadline_sentinel_control", false, "sentinel", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spool := t.TempDir()
			bodies := []string{ageGolden(t, "adult")}
			if tc.withdrawal == "age" {
				bodies = append(bodies, ageRefusalBody(t, "age_ineligible"))
			}
			rig := newAgeWithdrawRig(t, spool, nil, bodies...)
			if tc.withdrawal == "sentinel" {
				rig.script.push(http.StatusForbidden, `{"error":{"code":"real_subjects_disabled"}}`)
			}
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)
			rig.capture.setStatus(http.StatusInternalServerError)
			rig.client.experimentCycle(context.Background())
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := rig.client.Flush(ctx); err == nil {
				t.Fatal("the actual first-launch ingest must refuse the facts")
			}
			_ = rig.client.Close(ctx)
			select {
			case <-rig.client.workerDone:
			default:
				t.Fatal("the prior worker must stop before editing its record")
			}
			record := readSpoolRecordFile(t, spool)
			if len(record.Events) != 2 {
				t.Fatalf("the real first launch must leave two sealed facts, got %d", len(record.Events))
			}
			record.RetryAfterUntilMS = 0
			if tc.deadline {
				record.RetryAfterUntilMS = time.Now().Add(10 * time.Minute).UnixMilli()
			}
			payload, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(spool, spoolFileName), payload, 0600); err != nil {
				t.Fatal(err)
			}
			rig.capture.setStatus(http.StatusAccepted)
			restarted := rig.launch(t, func(cfg *Config) { cfg.FlushInterval = 30 * time.Millisecond })
			if got := spooledExperimentFacts(restarted, ageGoldenExperiment); got != 2 {
				t.Fatalf("the restart must actually load both facts, got %d", got)
			}
			if tc.deadline != restarted.initialDeferUntil.After(time.Now().Add(9*time.Minute)) {
				t.Fatalf("the worker deadline must actually be seeded: %v", restarted.initialDeferUntil)
			}
			switch tc.withdrawal {
			case "age":
				result, err := restarted.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
				if err != nil || result.Reason != "age_ineligible" {
					t.Fatalf("the actual age refusal must land: %+v %v", result, err)
				}
			case "sentinel":
				if _, err := restarted.FetchExperimentAssignment(context.Background(), ageGoldenExperiment, nil); err == nil {
					t.Fatal("the actual sentinel must refuse")
				}
			case "none":
				// The pacing control has a current-launch adult decision, so
				// startup age quarantine cannot explain its delay.
				fetchAdultAssignment(t, restarted, ageGoldenExperiment)
			}
			stored := readSpoolRecordFile(t, spool)
			wantStored := 0
			if tc.withdrawal == "none" {
				wantStored = 2
			}
			if len(stored.Events) != wantStored || (stored.RetryAfterUntilMS != 0) != (wantStored > 0 && tc.deadline) {
				t.Fatalf("withdrawal/control must reach the real stored state: events=%d deadline=%d", len(stored.Events), stored.RetryAfterUntilMS)
			}
			if tc.withdrawal != "none" && restarted.spoolHasResendWork() {
				t.Fatal("the withdrawal must leave no restored resend work")
			}
			if err := restarted.Enqueue(Event{Name: "host.after"}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(1500 * time.Millisecond)
			automatic := len(rig.capture.byName("host.after")) > 0
			flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer flushCancel()
			flushErr := restarted.Flush(flushCtx)
			afterFlush := flushErr == nil && len(rig.capture.byName("host.after")) > 0
			t.Logf("stored_events=%d stored_deadline=%t automatic_within_1.5s=%t after_explicit_flush=%t", len(stored.Events), stored.RetryAfterUntilMS != 0, automatic, afterFlush)
			if !afterFlush {
				t.Fatalf("explicit Flush must deliver the host event: %v", flushErr)
			}
			if automatic != tc.wantAuto {
				t.Errorf("automatic publication after the withdrawal: got %t, want %t", automatic, tc.wantAuto)
			}
		})
	}
}
