package shardpilot

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestEvictedApplicationsCannotPoisonLaterSpoolWork(t *testing.T) {
	var capacity atomic.Int64
	rig := newAgeWithdrawRig(t, t.TempDir(), func(cfg *Config) {
		cfg.SpoolMaxBytes = 2048
		cfg.OnSpoolDeadLetter = func(letter SpoolDeadLetter) {
			if letter.Reason == SpoolDropCapacity {
				capacity.Add(int64(len(letter.Envelopes)))
			}
		}
	}, ageGolden(t, "adult"))
	attrs := make(map[string]string)
	for _, key := range []string{"size_a", "size_b", "size_c", "size_d"} {
		attrs[expCustomAttributePrefix+key] = strings.Repeat("x", expMaxAttributeValueBytes)
	}
	if result, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, attrs); err != nil || !result.Assigned {
		t.Fatalf("actual assignment: %+v %v", result, err)
	}
	rig.applyAndMeasure(t, ageGoldenExperiment)
	rig.client.drainDeferredSpoolLetters()
	first := rig.client.Snapshot()
	if rig.client.owedExperimentExposureCount() != 0 {
		t.Error("capacity-evicted applications remain owed after their first disposition")
	}
	if first.SpoolEvicted != 2 || capacity.Load() != 2 {
		t.Errorf("each of the two actual applications must report capacity once: stats=%d letters=%d", first.SpoolEvicted, capacity.Load())
	}
	if first.ExperimentExposureDrops["spool_capacity"] != 1 || first.ExperimentOutcomeDrops["spool_capacity"] != 1 {
		t.Error("terminal capacity loss must be attributed once to each application kind")
	}
	ordinary, err := rig.client.prepareEvent(Event{Name: "ordinary.retention.control"})
	if err != nil {
		t.Fatal(err)
	}
	rig.capture.setStatus(http.StatusInternalServerError)
	if err := rig.client.Enqueue(ordinary); err != nil {
		t.Fatal(err)
	}
	if err := rig.client.Flush(context.Background()); err == nil {
		t.Fatal("the actual ordinary publish must fail retriably")
	}
	if !applicationTestHasSpoolID(rig.client, ordinary.ID) {
		t.Fatal("the actual failed ordinary publish must be spooled")
	}
	for cycle := 0; cycle < 2; cycle++ {
		rig.client.experimentCycle(context.Background())
		rig.client.drainDeferredSpoolLetters()
	}
	after := rig.client.Snapshot()
	t.Logf("capacity reports first=%d after=%d letters=%d owed=%d ordinary retained=%v", first.SpoolEvicted, after.SpoolEvicted, capacity.Load(), rig.client.owedExperimentExposureCount(), applicationTestHasSpoolID(rig.client, ordinary.ID))
	if after.SpoolEvicted != first.SpoolEvicted || capacity.Load() != 2 {
		t.Error("later cycles re-reported terminal capacity evictions")
	}
	if !applicationTestHasSpoolID(rig.client, ordinary.ID) {
		t.Error("re-admitting an impossible application evicted later ordinary work")
	}
	if len(rig.script.apply.requestsSoFar()) != 0 || len(rig.script.outcome.requestsSoFar()) != 0 {
		t.Error("a terminally evicted application reached the apply wire")
	}
	if rig.client.owedExperimentExposureCount() != 0 || storedApplicationCount(t, rig.spool) != 0 {
		t.Error("terminally evicted application state survived")
	}
}

func TestExpiredApplicationsSettleBeforeASecondSweep(t *testing.T) {
	var expired atomic.Int64
	rig := newAgeWithdrawRig(t, t.TempDir(), func(cfg *Config) {
		cfg.OnSpoolDeadLetter = func(letter SpoolDeadLetter) {
			if letter.Reason == SpoolDropExpired {
				expired.Add(int64(len(letter.Envelopes)))
			}
		}
	}, ageGolden(t, "adult"))
	clock := &expFakeClock{now: time.Now()}
	rig.client.clock = clock
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.script.apply.push(http.StatusServiceUnavailable, `{}`)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	rig.client.experimentCycle(context.Background())
	if len(rig.script.apply.requestsSoFar()) != 1 || storedApplicationCount(t, rig.spool) != 2 {
		t.Fatal("the actual apply refusal must leave two durable unsealed applications")
	}
	rig.client.exp.mu.Lock()
	records := append([]*expOwedExposure(nil), rig.client.exp.pendingExposure[ageGoldenExperiment]...)
	rig.client.exp.mu.Unlock()
	clock.advance(8 * 24 * time.Hour)
	rig.client.sweepAllExperimentExposures(context.Background())
	first := rig.client.Snapshot()
	if first.SpoolExpired != 2 || expired.Load() != 2 {
		t.Errorf("the first sweep must settle and report both expired applications: stats=%d letters=%d", first.SpoolExpired, expired.Load())
	}
	if rig.client.owedExperimentExposureCount() != 0 || storedApplicationCount(t, rig.spool) != 0 {
		t.Error("the first expiry left stored or owed application state")
	}
	for _, record := range records {
		if !record.intentDelivery.expiredReported.Load() {
			t.Error("application expiry bypassed its reporting receipt")
		}
	}
	rig.client.sweepAllExperimentExposures(context.Background())
	after := rig.client.Snapshot()
	t.Logf("expiry reports first=%d after=%d letters=%d owed=%d stored=%d", first.SpoolExpired, after.SpoolExpired, expired.Load(), rig.client.owedExperimentExposureCount(), storedApplicationCount(t, rig.spool))
	if after.SpoolExpired != first.SpoolExpired || expired.Load() != 2 {
		t.Error("a second sweep re-reported the same expired application")
	}
	if after.ExperimentExposureDrops["retry_age_expired"] != 1 || after.ExperimentOutcomeDrops["retry_age_expired"] != 1 {
		t.Error("terminal expiry must be attributed once to each application kind")
	}
	if len(rig.script.apply.requestsSoFar()) != 1 || len(rig.script.outcome.requestsSoFar()) != 0 {
		t.Error("an expired application retried its apply hop")
	}
}

func TestAnotherSpoolWriteCannotRearmAnEvictedApplication(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), func(cfg *Config) { cfg.SpoolMaxBytes = 2048 }, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	if storedApplicationCount(t, rig.spool) != 1 {
		t.Fatal("the application must initially fit and be durable")
	}
	rig.client.spool.mu.Lock()
	appBytes := len(rig.client.spool.entries[0].raw)
	rig.client.spool.mu.Unlock()
	ordinary, err := rig.client.prepareEvent(Event{Name: "ordinary.capacity.control", Props: map[string]any{"pad": strings.Repeat("o", 1000)}})
	if err != nil {
		t.Fatal(err)
	}
	request, err := rig.client.buildBatch([]Event{ordinary})
	if err != nil || len(request.rawEvents) != 1 {
		t.Fatalf("ordinary control: %v", err)
	}
	if n := len(request.rawEvents[0]); n > 2048 || n+appBytes <= 2048 {
		t.Fatalf("the actual ordinary event must fit alone and evict the older application: ordinary=%d app=%d", n, appBytes)
	}
	rig.capture.setStatus(http.StatusInternalServerError)
	if err := rig.client.Enqueue(ordinary); err != nil {
		t.Fatal(err)
	}
	if err := rig.client.Flush(context.Background()); err == nil {
		t.Fatal("the actual ordinary publish must fail retriably")
	}
	if storedApplicationCount(t, rig.spool) != 0 || !applicationTestHasSpoolID(rig.client, ordinary.ID) {
		t.Fatal("the ordinary write must actually evict the older application")
	}
	for cycle := 0; cycle < 2; cycle++ {
		rig.client.experimentCycle(context.Background())
	}
	if rig.client.owedExperimentExposureCount() != 0 || !applicationTestHasSpoolID(rig.client, ordinary.ID) {
		t.Error("another writer's application eviction was re-admitted instead of settled")
	}
	if len(rig.script.apply.requestsSoFar()) != 0 {
		t.Error("an application already evicted by ordinary work was sealed again")
	}
}

func applicationTestHasSpoolID(c *Client, id string) bool {
	c.spool.mu.Lock()
	defer c.spool.mu.Unlock()
	_, present := c.spool.ids[id]
	return present
}

// Keep the gap between the spool's leaf mutation and its off-lock observer
// open explicitly. The integration scene above covers the ordinary worker.
func TestApplicationAdmissionCannotRaceCapacityDisposition(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), func(cfg *Config) { cfg.SpoolMaxBytes = 2048 }, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	event, err := rig.client.prepareEvent(Event{Name: "ordinary.disposition.control", Props: map[string]any{"pad": strings.Repeat("o", 1000)}})
	if err != nil {
		t.Fatal(err)
	}
	request, err := rig.client.buildBatch([]Event{event})
	if err != nil {
		t.Fatal(err)
	}
	ordinary := spoolEntriesFromRequest(request)[0]
	e := rig.client.exp
	e.mu.Lock()
	record := e.pendingExposure[ageGoldenExperiment][0]
	_, _, _, evicted, failed := rig.client.spool.append([]spoolEntry{ordinary}, 0, false, rig.client.clock.Now(), func() bool { return true })
	if failed || len(evicted) != 1 || !evicted[0].application {
		e.mu.Unlock()
		t.Fatalf("the leaf write must evict the real stored application: failed=%v evicted=%d", failed, len(evicted))
	}
	retained := rig.client.persistExperimentApplication(ageGoldenExperiment, record)
	owed := len(e.pendingExposure[ageGoldenExperiment])
	e.mu.Unlock()
	rig.client.notifySpoolDeadLetter(SpoolDropCapacity, evicted)
	if retained || owed != 0 || !applicationTestHasSpoolID(rig.client, ordinary.id) {
		t.Errorf("admission raced the pending capacity disposition: retained=%v owed=%d ordinary=%v", retained, owed, applicationTestHasSpoolID(rig.client, ordinary.id))
	}
}

func TestApplicationHandoffCannotRecreateRetentionVictim(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), func(cfg *Config) { cfg.SpoolMaxBytes = 2048 }, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.client.ApplyExperimentVariant(ageGoldenExperiment)
	event, err := rig.client.prepareEvent(Event{Name: "ordinary.handoff.control", Props: map[string]any{"pad": strings.Repeat("o", 1000)}})
	if err != nil {
		t.Fatal(err)
	}
	request, err := rig.client.buildBatch([]Event{event})
	if err != nil {
		t.Fatal(err)
	}
	ordinary := spoolEntriesFromRequest(request)[0]
	var evicted []spoolEntry
	var failed bool
	var fired atomic.Bool
	e := rig.client.exp
	e.mu.Lock()
	e.consentRaceSeam = func(stage string) {
		if stage == "exposure_build" && fired.CompareAndSwap(false, true) {
			_, _, _, evicted, failed = rig.client.spool.append([]spoolEntry{ordinary}, 0, false, rig.client.clock.Now(), func() bool { return true })
		}
	}
	e.mu.Unlock()
	rig.client.experimentCycle(context.Background())
	e.mu.Lock()
	e.consentRaceSeam = nil
	e.mu.Unlock()
	if !fired.Load() || failed || len(evicted) != 1 || !evicted[0].application || len(rig.script.apply.requestsSoFar()) != 1 {
		t.Fatal("the actual seal must cross an actual application eviction before its observer runs")
	}
	if spooledExperimentFacts(rig.client, ageGoldenExperiment) != 0 || !applicationTestHasSpoolID(rig.client, ordinary.id) {
		t.Error("handoff recreated a terminal retention victim and displaced ordinary work")
	}
	rig.client.notifySpoolDeadLetter(SpoolDropCapacity, evicted)
	rig.client.sweepAllExperimentExposures(context.Background())
	if rig.client.owedExperimentExposureCount() != 0 {
		t.Error("the delayed capacity disposition did not settle the application")
	}
}
