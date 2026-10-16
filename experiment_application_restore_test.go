package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
)

func TestApplicationDurabilityWaitsForItsCanonicalSubject(t *testing.T) {
	for _, recoverWrite := range []bool{false, true} {
		name := "continuing_failure"
		if recoverWrite {
			name = "recovered_write_and_cold_image"
		}
		t.Run(name, func(t *testing.T) {
			rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
			var blocked atomic.Bool
			blocked.Store(true)
			e := rig.client.exp
			e.mu.Lock()
			e.writeSubjectFileForTests = func(path string, payload []byte, initial bool) error {
				if blocked.Load() {
					return errors.New("synthetic canonical subject write failure")
				}
				rename := os.Rename
				if initial {
					rename = func(old, next string) error { return os.Link(old, next) }
				}
				return writePrivateFileAtomic(path, payload, rename, os.Chmod)
			}
			e.mu.Unlock()
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			if recoverWrite {
				if variant, _ := rig.client.ApplyExperimentVariant(ageGoldenExperiment); variant != "control" {
					t.Fatal("the actual host application must be served")
				}
			} else {
				rig.applyAndMeasure(t, ageGoldenExperiment)
			}
			e.mu.Lock()
			subject := e.currentSubjectIDLocked()
			var want []string
			for _, record := range e.pendingExposure[ageGoldenExperiment] {
				want = append(want, applicationID(record))
			}
			e.mu.Unlock()
			sort.Strings(want)
			wantCount := 2
			if recoverWrite {
				wantCount = 1
			}
			if len(want) != wantCount || subject == "" {
				t.Fatal("the actual pinned applications must exist under the minted subject")
			}
			ordinary, err := rig.client.prepareEvent(Event{Name: "ordinary.subject.write.control"})
			if err != nil {
				t.Fatal(err)
			}
			rig.capture.setStatus(http.StatusInternalServerError)
			if err := rig.client.Enqueue(ordinary); err != nil {
				t.Fatal(err)
			}
			if err := rig.client.Flush(context.Background()); err == nil || !applicationTestHasSpoolID(rig.client, ordinary.ID) {
				t.Fatal("the actual ordinary failure must prove that spool writes still succeed")
			}
			if _, err := os.Stat(filepath.Join(rig.spool, expSubjectFileName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("the canonical-write fault must leave the subject file absent")
			}
			if storedApplicationCount(t, rig.spool) != 0 {
				t.Error("applications were called durable without a durable canonical subject")
			}
			if !recoverWrite {
				rig.script.apply.push(http.StatusServiceUnavailable, `{}`)
				rig.script.outcome.push(http.StatusServiceUnavailable, `{}`)
				_ = rig.client.Close(context.Background())
				stats := rig.client.Snapshot()
				if stats.ExperimentExposureDrops[expDropUnsealedAtClose] != 1 || stats.ExperimentOutcomeDrops[expDropUnsealedAtClose] != 1 {
					t.Error("Close claimed subject-less application state was replayable")
				}
				if len(rig.script.apply.requestsSoFar()) != 0 || len(rig.script.outcome.requestsSoFar()) != 0 {
					t.Error("a failed canonical prerequisite did not hold the application wire")
				}
				return
			}
			blocked.Store(false)
			rig.client.sweepAllExperimentExposuresMode(context.Background(), false, false)
			if storedApplicationCount(t, rig.spool) != wantCount {
				t.Error("recovered canonical storage did not persist its original application")
			}
			image := copyApplicationImage(t, rig.spool)
			_ = rig.client.Close(context.Background())
			applyBefore := len(rig.script.apply.requestsSoFar())
			outcomeBefore := len(rig.script.outcome.requestsSoFar())
			rig.capture.setStatus(http.StatusAccepted)
			restarted := rig.launch(t, func(cfg *Config) { cfg.SpoolDir = image })
			restarted.SetConsent(true)
			restarted.exp.mu.Lock()
			gotSubject := restarted.exp.currentSubjectIDLocked()
			restarted.exp.mu.Unlock()
			if gotSubject != subject {
				t.Error("cold recovery lost the canonical subject after its write healed")
			}
			fetchAdultAssignment(t, restarted, ageGoldenExperiment)
			restarted.experimentCycle(context.Background())
			flushOrFail(t, restarted)
			var got []string
			for _, request := range rig.script.apply.requestsSoFar()[applyBefore:] {
				got = append(got, "eap1_e_"+request.body["exposure_id"].(string))
			}
			for _, request := range rig.script.outcome.requestsSoFar()[outcomeBefore:] {
				got = append(got, "eap1_o_"+request.body["outcome_id"].(string))
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("cold recovery stranded or reminted applications: got=%v want=%v", got, want)
			}
		})
	}
}

func TestStoredApplicationKeepsTheWholeValidatedEntry(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	rig.client.spool.mu.Lock()
	original := append([]byte(nil), rig.client.spool.entries[0].raw...)
	rig.client.spool.mu.Unlock()
	for _, field := range []string{"attributes", "served_time", "subject_fact_key"} {
		t.Run(field, func(t *testing.T) {
			var application expStoredApplication
			if err := json.Unmarshal(original, &application); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "attributes":
				application.Entry.Attributes = []expAttribute{{Name: expCustomAttributePrefix + "bucket", Value: "kept"}, {Name: "not_allowlisted", Value: "removed"}}
			case "served_time":
				application.Entry.Served.At = "2026-01-02T03:04:05+03:00"
			case "subject_fact_key":
				application.Entry.SubjectFactKey = "corrupted-key"
			}
			raw, err := json.Marshal(application)
			if err != nil {
				t.Fatal(err)
			}
			restored := parseStoredApplication(raw)
			if restored == nil {
				t.Fatal("repairable corruption must retain an application")
			}
			switch field {
			case "attributes":
				if !reflect.DeepEqual(restored.Entry.Attributes, []expAttribute{{Name: expCustomAttributePrefix + "bucket", Value: "kept"}}) {
					t.Error("restoration returned raw attributes instead of the validated entry")
				}
			case "served_time":
				if restored.Entry.Served.At != "2026-01-02T00:04:05Z" {
					t.Error("restoration returned the raw served timestamp")
				}
			case "subject_fact_key":
				if restored.Entry.SubjectFactKey != "" {
					t.Error("restoration returned the malformed subject fact key")
				}
			}
		})
	}
	// Persist actual corrupt bytes and observe the cold client's real apply
	// request, independently of the parser-only assertions above.
	rig.client.spool.mu.Lock()
	var application expStoredApplication
	if err := json.Unmarshal(original, &application); err != nil {
		rig.client.spool.mu.Unlock()
		t.Fatal(err)
	}
	application.Entry.Attributes = []expAttribute{{Name: expCustomAttributePrefix + "bucket", Value: "kept"}, {Name: "not_allowlisted", Value: "removed"}, {Name: expCustomAttributePrefix + "oversize", Value: strings.Repeat("x", expMaxAttributeValueBytes+1)}}
	application.Entry.Served.At = "2026-01-02T03:04:05+03:00"
	raw, err := json.Marshal(application)
	if err != nil {
		rig.client.spool.mu.Unlock()
		t.Fatal(err)
	}
	rig.client.spool.totalBytes += len(raw) - len(rig.client.spool.entries[0].raw)
	rig.client.spool.entries[0].raw = raw
	err = rig.client.spool.saveLocked()
	rig.client.spool.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	image := copyApplicationImage(t, rig.spool)
	rig.client.SetConsent(false)
	_ = rig.client.Close(context.Background())
	before := len(rig.script.apply.requestsSoFar())
	restarted := rig.launch(t, func(cfg *Config) { cfg.SpoolDir = image })
	restarted.SetConsent(true)
	fetchAdultAssignment(t, restarted, ageGoldenExperiment)
	restarted.experimentCycle(context.Background())
	flushOrFail(t, restarted)
	requests := rig.script.apply.requestsSoFar()[before:]
	if len(requests) != 1 {
		t.Fatalf("the actual restored application must reach its apply endpoint once: %d", len(requests))
	}
	attributes, _ := requests[0].body["attributes"].(map[string]any)
	if !reflect.DeepEqual(attributes, map[string]any{expCustomAttributePrefix + "bucket": "kept"}) || requests[0].body["served_at"] != "2026-01-02T00:04:05Z" {
		t.Errorf("cold apply sent the unsanitized stored construction: attributes=%v served_at=%v", attributes, requests[0].body["served_at"])
	}
}

func TestVisibleSubjectNeedsItsWriteAcknowledgement(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	e := rig.client.exp
	e.mu.Lock()
	e.writeSubjectFileForTests = func(path string, payload []byte, initial bool) error {
		rename := os.Rename
		if initial {
			rename = func(old, next string) error { return os.Link(old, next) }
		}
		return writePrivateFileAtomic(path, payload, func(old, next string) error {
			if err := rename(old, next); err != nil {
				return err
			}
			return errors.New("synthetic failure after canonical publication")
		}, os.Chmod)
	}
	e.mu.Unlock()
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	e.mu.Lock()
	visible, current := e.readSubjectFileLocked(), e.currentSubjectIDLocked()
	e.mu.Unlock()
	if visible != current || !validExperimentSubjectID(visible) {
		t.Fatal("the failed write must actually publish the same visible subject")
	}
	if variant, _ := rig.client.ApplyExperimentVariant(ageGoldenExperiment); variant != "control" {
		t.Fatal("the variant must still serve")
	}
	if storedApplicationCount(t, rig.spool) != 0 {
		t.Error("a visible but unacknowledged subject was treated as durable")
	}
	rig.client.experimentCycle(context.Background())
	if len(rig.script.apply.requestsSoFar()) != 0 {
		t.Error("the canonical write debt did not hold the apply wire")
	}
}

func TestCanonicalRetryDoesNotOverwriteAnotherSubject(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	e := rig.client.exp
	e.mu.Lock()
	e.writeSubjectFileForTests = func(string, []byte, bool) error { return errors.New("synthetic initial subject write failure") }
	e.mu.Unlock()
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	if err := os.WriteFile(filepath.Join(rig.spool, expSubjectFileName), []byte(expSubjectA+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.writeSubjectFileForTests = nil
	e.mu.Unlock()
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	rig.client.experimentCycle(context.Background())
	e.mu.Lock()
	stored := e.readSubjectFileLocked()
	e.mu.Unlock()
	if stored != expSubjectA {
		t.Error("a canonical retry overwrote a valid different publication")
	}
	if storedApplicationCount(t, rig.spool) != 0 || len(rig.script.apply.requestsSoFar()) != 0 {
		t.Error("a mismatching canonical witness admitted an application")
	}
}
