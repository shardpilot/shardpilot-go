package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRestoredDeadlineAfterWithdrawal(t *testing.T) {
	for _, tc := range []struct {
		name       string
		deadline   bool
		withdrawal string
		retained   string
		wantAuto   bool
	}{
		{"deadline_age_refusal", true, "age", "", true},
		{"deadline_sentinel", true, "sentinel", "", true},
		{"batch_full_age_refusal", true, "age", "", true},
		{"batch_full_sentinel", true, "sentinel", "", true},
		{"queued_age_refusal", true, "age", "fresh", true},
		{"queued_sentinel", true, "sentinel", "fresh", true},
		{"retained_deadline_control", true, "none", "", false},
		{"no_deadline_age_control", false, "age", "", true},
		{"no_deadline_sentinel_control", false, "sentinel", "", true},
		{"remaining_resend_control", true, "age", "spool", false},
		{"held_worker_control", true, "age", "worker", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spool := t.TempDir()
			bodies := []string{ageGolden(t, "adult")}
			if tc.retained == "worker" {
				bodies = append(bodies, ageGolden(t, "adult"))
			}
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
			if tc.retained == "spool" {
				if err := rig.client.Enqueue(Event{Name: "retained.before"}); err != nil {
					t.Fatal(err)
				}
			}
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
			wantInitial := 2
			if tc.retained == "spool" {
				wantInitial++
			}
			if len(record.Events) != wantInitial {
				t.Fatalf("the real first launch must leave %d events, got %d", wantInitial, len(record.Events))
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
			restarted := rig.launch(t, func(cfg *Config) {
				cfg.FlushInterval = 30 * time.Millisecond
				if tc.name == "batch_full_age_refusal" || tc.name == "batch_full_sentinel" {
					cfg.BatchSize = 1
					cfg.FlushInterval = time.Hour
				}
			})
			if got := spooledExperimentFacts(restarted, ageGoldenExperiment); got != 2 {
				t.Fatalf("the restart must actually load both facts, got %d", got)
			}
			if tc.deadline != restarted.initialDeferUntil.After(time.Now().Add(9*time.Minute)) {
				t.Fatalf("the worker deadline must actually be seeded: %v", restarted.initialDeferUntil)
			}
			// The worker has started its loop before withdrawal. In the
			// batch-full rows no periodic tick can reconcile the later purge
			// before the fresh queue receive: that receive must do it itself.
			waitFor(t, 5*time.Second, "the restored worker starts", func() bool {
				return restarted.expPipelineLow.Load() > 0
			})
			if tc.retained == "fresh" {
				if err := restarted.Enqueue(Event{Name: "host.before"}); err != nil {
					t.Fatal(err)
				}
				waitFor(t, 5*time.Second, "fresh event received before withdrawal", func() bool { return len(restarted.queue.ch) == 0 })
				if len(rig.capture.byName("host.before")) != 0 {
					t.Fatal("the fresh event must wait behind the restored deadline before withdrawal")
				}
			}
			if tc.retained == "worker" {
				fetchAdultAssignment(t, restarted, ageGoldenExperiment)
				if err := restarted.Enqueue(Event{Name: "held.before"}); err != nil {
					t.Fatal(err)
				}
				waitFor(t, 5*time.Second, "ordinary event received by the worker", func() bool { return len(restarted.queue.ch) == 0 })
				rig.capture.mu.Lock()
				rig.capture.retryAfter = "600"
				rig.capture.mu.Unlock()
				rig.capture.setStatus(http.StatusServiceUnavailable)
				flushCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
				err := restarted.Flush(flushCtx)
				stop()
				var status *HTTPStatusError
				if !errors.As(err, &status) || status.RetryAfter != 10*time.Minute {
					t.Fatalf("the actual resend must arm the held worker's deadline: %v", err)
				}
				// The worker received held.before before this Flush. Its
				// resend failure returned without consuming that held batch.
				rig.capture.setStatus(http.StatusAccepted)
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
			if tc.retained == "spool" && len(rig.capture.byName("retained.before")) > 0 {
				t.Fatal("remaining restored work published inside its retained Retry-After window")
			}
			wantStored := 0
			if tc.withdrawal == "none" {
				wantStored = 2
			} else if tc.retained == "spool" {
				wantStored = 1
			}
			if len(stored.Events) != wantStored || (stored.RetryAfterUntilMS != 0) != (wantStored > 0 && tc.deadline) {
				t.Fatalf("withdrawal/control must reach the real stored state: events=%d deadline=%d", len(stored.Events), stored.RetryAfterUntilMS)
			}
			if tc.withdrawal != "none" && tc.retained != "spool" && restarted.spoolHasResendWork() {
				t.Fatal("the withdrawal must leave no restored resend work")
			}
			if err := restarted.Enqueue(Event{Name: "host.after"}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(1500 * time.Millisecond)
			automatic := len(rig.capture.byName("host.after")) > 0
			if tc.retained == "fresh" && len(rig.capture.byName("host.before")) == 0 {
				t.Error("the never-attempted batch must publish automatically after withdrawal")
			}
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
