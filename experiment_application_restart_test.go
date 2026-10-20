package shardpilot

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

func TestRecordedSubjectApplicationsSurviveRemintRestart(t *testing.T) {
	for _, remint := range []bool{false, true} {
		name := "current_subject_control"
		if remint {
			name = "retired_subject"
		}
		t.Run(name, func(t *testing.T) {
			rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
			if remint {
				rig.script.push(http.StatusBadRequest, `{"error":{"code":"subject_id_invalid","message":"synthetic subject grammar rejection"}}`)
				rig.script.push(http.StatusOK, ageGolden(t, "adult"))
			}
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)
			e := rig.client.exp
			e.mu.Lock()
			oldSubject := e.currentSubjectIDLocked()
			var want []string
			for _, record := range e.pendingExposure[ageGoldenExperiment] {
				want = append(want, applicationID(record))
			}
			e.mu.Unlock()
			if len(want) != 2 || storedApplicationCount(t, rig.spool) != 2 {
				t.Fatal("the actual exposure and outcome must both be durable and unsealed")
			}
			if remint {
				fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
				e.mu.Lock()
				current := e.currentSubjectIDLocked()
				e.mu.Unlock()
				if current == oldSubject || rig.script.requestCount() != 3 {
					t.Fatal("the real grammar rejection must rotate the canonical subject")
				}
			}
			image := copyApplicationImage(t, rig.spool)
			rig.client.SetConsent(false)
			_ = rig.client.Close(context.Background())
			restarted := rig.launch(t, func(cfg *Config) { cfg.SpoolDir = image })
			restarted.SetConsent(true)
			restarted.experimentCycle(context.Background())
			if len(rig.script.apply.requestsSoFar()) != 0 || len(rig.script.outcome.requestsSoFar()) != 0 {
				t.Fatal("restored applications must still wait for the new host age decision")
			}
			fetchAdultAssignment(t, restarted, ageGoldenExperiment)
			restarted.experimentCycle(context.Background())
			flushOrFail(t, restarted)
			var got []string
			for _, request := range rig.script.apply.requestsSoFar() {
				got = append(got, "eap1_e_"+request.body["exposure_id"].(string))
				if request.body["subject_key"] != oldSubject {
					t.Error("the exposure was rewritten to the new subject")
				}
			}
			for _, request := range rig.script.outcome.requestsSoFar() {
				got = append(got, "eap1_o_"+request.body["outcome_id"].(string))
				if request.body["subject_key"] != oldSubject {
					t.Error("the outcome was rewritten to the new subject")
				}
			}
			sort.Strings(got)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) || len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)) != 2 {
				t.Errorf("cold restart stranded the recorded subject's applications: got=%v want=%v facts=%d", got, want, len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)))
			}
		})
	}
}

func rewriteRestartApplications(t *testing.T, dir string, mutate func(*spoolEventWire, *expStoredApplication)) {
	t.Helper()
	path := filepath.Join(dir, spoolFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	doc := parseSpoolDocument(data)
	if !doc.ok {
		t.Fatal("the actual SDK spool must parse before the offline change")
	}
	var record spoolRecordWire
	for _, entry := range doc.live() {
		wire := entry.wire()
		application := parseStoredApplication(wire.Application)
		if application == nil {
			t.Fatal("the restart image must contain actual valid applications")
		}
		mutate(&wire, application)
		wire.Application, err = json.Marshal(application)
		if err != nil {
			t.Fatal(err)
		}
		record.Events = append(record.Events, wire)
	}
	writeSpoolDocument(t, path, spoolRecordVersionLines, record)
}

func TestRestoredApplicationRejectsEveryForeignScopeComponent(t *testing.T) {
	for field := 0; field < 5; field++ {
		t.Run([]string{"workspace", "app", "environment", "endpoint", "key_fingerprint"}[field], func(t *testing.T) {
			rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)
			image := copyApplicationImage(t, rig.spool)
			e := rig.client.exp
			e.mu.Lock()
			parts := []string{e.workspaceID, e.appKey, e.envKey, e.baseURL, e.keyPrint}
			e.mu.Unlock()
			parts[field] = "synthetic-foreign-scope"
			rewriteRestartApplications(t, image, func(_ *spoolEventWire, a *expStoredApplication) {
				a.Scope = buildExperimentScope(parts[0], parts[1], parts[2], a.Entry.SubjectKey, parts[3], parts[4])
			})
			rig.client.SetConsent(false)
			_ = rig.client.Close(context.Background())
			restarted := rig.launch(t, func(cfg *Config) { cfg.SpoolDir = image })
			restarted.SetConsent(true)
			fetchAdultAssignment(t, restarted, ageGoldenExperiment)
			restarted.experimentCycle(context.Background())
			if restarted.owedExperimentExposureCount() != 0 || len(rig.script.apply.requestsSoFar()) != 0 || len(rig.script.outcome.requestsSoFar()) != 0 {
				t.Error("a foreign construction was restored or transmitted")
			}
		})
	}
}

func TestLoadTimeApplicationRetentionIsAttributedOnce(t *testing.T) {
	for _, mode := range []string{"unexpired_control", "expiry", "event_capacity", "byte_capacity"} {
		t.Run(mode, func(t *testing.T) {
			rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)
			image := copyApplicationImage(t, rig.spool)
			if mode == "expiry" {
				rewriteRestartApplications(t, image, func(wire *spoolEventWire, _ *expStoredApplication) {
					wire.IntakeUnixMS = time.Now().Add(-8 * 24 * time.Hour).UnixMilli()
				})
			}
			rig.client.SetConsent(false)
			_ = rig.client.Close(context.Background())
			var letters atomic.Int64
			restarted := rig.launch(t, func(cfg *Config) {
				cfg.SpoolDir = image
				if mode == "event_capacity" {
					cfg.SpoolMaxEvents = 1
				}
				if mode == "byte_capacity" {
					cfg.SpoolMaxBytes = 1
				}
				cfg.OnSpoolDeadLetter = func(letter SpoolDeadLetter) { letters.Add(int64(len(letter.Envelopes))) }
			})
			restarted.SetConsent(true)
			wantExposure, wantOutcome := uint64(1), uint64(1)
			reason := expDropSpoolCapacity
			if mode == "unexpired_control" {
				wantExposure, wantOutcome = 0, 0
			}
			if mode == "event_capacity" {
				wantOutcome = 0
			}
			if mode == "expiry" {
				reason = expDropRetryAgeExpired
			}
			for cycle := 0; cycle < 3; cycle++ {
				stats := restarted.Snapshot()
				generic := stats.SpoolEvicted
				if mode == "expiry" {
					generic = stats.SpoolExpired
				}
				if generic != wantExposure+wantOutcome || uint64(letters.Load()) != generic {
					t.Fatalf("the actual load must report the expected generic loss exactly once: stats=%d letters=%d", generic, letters.Load())
				}
				if stats.ExperimentExposureDrops[reason] != wantExposure || stats.ExperimentOutcomeDrops[reason] != wantOutcome {
					t.Errorf("load loss missed kind attribution: exposure=%d/%d outcome=%d/%d", stats.ExperimentExposureDrops[reason], wantExposure, stats.ExperimentOutcomeDrops[reason], wantOutcome)
				}
				restarted.experimentCycle(context.Background())
			}
		})
	}
}
