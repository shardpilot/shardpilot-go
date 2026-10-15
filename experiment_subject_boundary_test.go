package shardpilot

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestHandlePlaneRefusalsBypassThePerKeyFence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"unauthorized", 401, `{}`},
		{"forbidden", 403, `{}`},
		{"disabled", 403, `{"error":{"code":"real_subjects_disabled"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newAgeWithdrawRig(t, "", nil, ageGolden(t, "adult"))
			a := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
			b := newSubjectOrFail(t, rig.client, expSubjectB, "player-b")
			if _, err := a.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := b.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err != nil {
				t.Fatal(err)
			}
			a.Apply(ageGoldenExperiment)
			if err := a.TrackOutcome(ageGoldenExperiment, "score", 1); err != nil {
				t.Fatal(err)
			}
			rig.script.mu.Lock()
			rig.script.responses = nil
			rig.script.mu.Unlock()
			rig.script.push(tc.status, tc.body)
			rig.script.push(http.StatusOK, ageGolden(t, "adult"))
			gate, release := releaseOnCleanup(t)
			rig.script.gates = map[int]chan struct{}{2: gate}
			done := make(chan error, 1)
			go func() {
				_, err := a.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil)
				done <- err
			}()
			waitFor(t, 5*time.Second, "older handle request dispatch", func() bool { return rig.script.requestCount() == 3 })
			if _, err := a.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err != nil {
				t.Fatal(err)
			}
			if b.Variant(ageGoldenExperiment) == "" {
				t.Fatal("sibling handle must serve before the refusal")
			}
			release()
			if err := <-done; err == nil {
				t.Error("a plane-wide refusal returned a healthy caller result")
			}
			if tc.name == "disabled" {
				stats := rig.client.Snapshot()
				if stats.ExperimentExposureDrops[expDropRealSubjectsDisabled] != 1 || stats.ExperimentOutcomeDrops[expDropRealSubjectsDisabled] != 1 {
					t.Error("a fenced sentinel did not withdraw the caller's owed facts")
				}
			}
			if b.Variant(ageGoldenExperiment) != "" {
				t.Error("a fenced request's plane refusal left a sibling handle serving")
			}
			if tc.name == "disabled" {
				if _, err := a.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err != nil {
					t.Fatal(err)
				}
				if b.Variant(ageGoldenExperiment) != "" {
					t.Error("re-enabling one handle resurrected another handle's withdrawn assignment")
				}
			}
		})
	}
}

func TestHandleAdultCallFencesAnOlderNonAdultAnswer(t *testing.T) {
	rig := newAgeWithdrawRig(t, "", nil, ageGolden(t, "adult"), ageGolden(t, "under_threshold"), ageGolden(t, "adult"))
	s := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
	if _, err := s.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err != nil {
		t.Fatal(err)
	}
	s.Apply(ageGoldenExperiment)
	if err := s.TrackOutcome(ageGoldenExperiment, "score", 1); err != nil {
		t.Fatal(err)
	}
	oldGate, releaseOld := releaseOnCleanup(t)
	newGate, releaseNew := releaseOnCleanup(t)
	rig.script.gates = map[int]chan struct{}{1: oldGate, 2: newGate}
	oldDone := make(chan struct{})
	go func() {
		_, _ = s.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
		close(oldDone)
	}()
	waitFor(t, 5*time.Second, "older non-adult handle dispatch", func() bool { return rig.script.requestCount() == 2 })
	newDone := make(chan struct{})
	go func() {
		_, _ = s.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil)
		close(newDone)
	}()
	waitFor(t, 5*time.Second, "newer adult handle dispatch", func() bool { return rig.script.requestCount() == 3 })
	releaseOld()
	<-oldDone
	s.mu.Lock()
	owed := len(s.owed)
	s.mu.Unlock()
	if owed != 2 {
		t.Errorf("earlier non-adult answer withdrew the handle's applications: owed=%d", owed)
	}
	releaseNew()
	<-newDone
}

func TestHandleQueuedFactsWaitForThePendingAgeAnswer(t *testing.T) {
	for _, reason := range []string{"age_ineligible", "kill_switch"} {
		t.Run(reason, func(t *testing.T) {
			rig := newAgeWithdrawRig(t, "", nil, ageGolden(t, "adult"), ageRefusalBody(t, reason))
			s := newSubjectOrFail(t, rig.client, expSubjectA, "player-a")
			if _, err := s.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); err != nil {
				t.Fatal(err)
			}
			s.Apply(ageGoldenExperiment)
			if err := s.TrackOutcome(ageGoldenExperiment, "score", 1); err != nil {
				t.Fatal(err)
			}
			rig.client.experimentCycle(context.Background())
			gate, release := releaseOnCleanup(t)
			rig.script.gates = map[int]chan struct{}{1: gate}
			done := make(chan struct{})
			go func() {
				_, _ = s.FetchWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
				close(done)
			}()
			waitFor(t, 5*time.Second, "pending handle age request", func() bool { return rig.script.requestCount() == 2 })
			if err := rig.client.Enqueue(Event{Name: "host.allowed"}); err != nil {
				t.Fatal(err)
			}
			_ = rig.client.Flush(context.Background())
			if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != 0 {
				t.Errorf("delivered %d handle facts while age was pending", got)
			}
			if got := len(rig.capture.byName("host.allowed")); got != 1 {
				t.Errorf("ordinary event held with handle facts: %d", got)
			}
			release()
			<-done
			flushOrFail(t, rig.client)
			want := 0
			if reason == "kill_switch" {
				want = 2
			}
			if got := len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)); got != want {
				t.Errorf("after %s delivered=%d want=%d", reason, got, want)
			}
		})
	}
}
