package shardpilot

// A non-adult age declaration the server has not confirmed (#138 R2). The
// host declares an age other than adult for an experiment: from that moment
// the experiment is not served or recorded and the revalidation lane does
// not re-send the earlier declaration. The server's answer then governs as
// before; when the fetch ends without one, the declaration alone makes the
// subject ineligible, so the experiment is withdrawn exactly as for an
// age_ineligible refusal, and no answer to a fetch sent before the
// declaration brings it back. Plus the transport scenes #138 R3 lists: the
// forced-minor floor with an adult declaration, and an adult response or
// revalidation in flight losing to a later refusal. Synthetic data only: the
// captured age goldens and the in-process apply stubs.

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

const ageTransientBody = `{"error":"unavailable"}`

// declaresAdultAfter names every assignment request for the declared
// experiment, from index from on, that declared an adult age under either
// spelling.
func declaresAdultAfter(script *expScript, from int) []int {
	var adult []int
	for i := from; i < script.requestCount(); i++ {
		query := script.request(i).URL.Query()
		if query.Get("experiment_key") != ageGoldenExperiment {
			continue
		}
		if query.Get("age_band") == "adult" || query.Get("custom_attribute_age_band") == "adult" {
			adult = append(adult, i)
		}
	}
	return adult
}

// assertExperimentWithdrawn is the rule's full effect on one experiment: not
// served by any getter, no application or outcome accepted, nothing owed,
// the withdrawn applications counted age_ineligible, no durable copy, and
// after the lane's cycle and a flush no further apply request sent, no
// assignment request from fromRequest on declaring adult, and no fact
// delivered.
func assertExperimentWithdrawn(t *testing.T, rig *ageWithdrawRig, spool string, owedBefore int, fromRequest int) {
	t.Helper()
	exposuresBefore, outcomesBefore := rig.applyRequests()
	if variant := rig.client.ExperimentVariant(ageGoldenExperiment); variant != "" {
		t.Errorf("stop serving: ExperimentVariant still serves %q after the non-adult declaration", variant)
	}
	if payload := rig.client.ExperimentVariantPayload(ageGoldenExperiment); payload != nil {
		t.Errorf("stop serving: ExperimentVariantPayload still serves %v", payload)
	}
	if variant, _ := rig.client.ApplyExperimentVariant(ageGoldenExperiment); variant != "" {
		t.Errorf("stop serving: ApplyExperimentVariant still serves and records %q", variant)
	}
	if err := rig.client.TrackExperimentOutcome(ageGoldenExperiment, "score", 2); !errors.Is(err, ErrExperimentNoAssignment) {
		t.Errorf("stop serving: a new outcome must be refused with ErrExperimentNoAssignment, got %v", err)
	}
	if owed := rig.client.owedExperimentExposureCount(); owed != 0 {
		t.Errorf("withdraw: %d owed application(s) survive the non-adult declaration", owed)
	}
	if owedBefore > 0 {
		if exposures, outcomes := rig.drops("age_ineligible"); exposures+outcomes != uint64(owedBefore) {
			t.Errorf("withdraw: %d withdrawn application(s) counted age_ineligible (exposure=%d outcome=%d), want %d", exposures+outcomes, exposures, outcomes, owedBefore)
		}
	}
	if durableRecordHolds(t, spool, ageGoldenExperiment) {
		t.Errorf("withdraw: the adult assignment is still in the durable record")
	}
	rig.capture.setStatus(http.StatusAccepted)
	rig.client.experimentCycle(context.Background())
	if exposures, outcomes := rig.applyRequests(); exposures != exposuresBefore || outcomes != outcomesBefore {
		t.Errorf("withdraw: the lane sent %d exposure and %d outcome apply request(s) after the non-adult declaration", exposures-exposuresBefore, outcomes-outcomesBefore)
	}
	if adult := declaresAdultAfter(rig.script, fromRequest); len(adult) != 0 {
		t.Errorf("no stale re-send: request(s) %v declared age_band=adult after the host declared under_threshold", adult)
	}
	flushOrFail(t, rig.client)
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 0 {
		t.Errorf("withdraw: %d fact(s) of the experiment delivered after the non-adult declaration", len(delivered))
	}
}

// #138 R2, the scene itself: an adult assignment is applied and measured,
// then the host declares under_threshold and that fetch fails transiently.
// The server never confirms the refusal, yet nothing is served, nothing
// owed survives, the revalidation lane declares nothing adult, and nothing
// of the experiment is sent or delivered.
func TestANonAdultDeclarationWhoseFetchFailsWithdrawsTheExperiment(t *testing.T) {
	for _, band := range []ExperimentAgeBand{ExperimentAgeBandUnderThreshold, ExperimentAgeBandUnknown} {
		t.Run(string(band), func(t *testing.T) {
			spool := t.TempDir()
			rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"))
			rig.script.push(http.StatusServiceUnavailable, ageTransientBody)
			clock := &expFakeClock{now: time.Now()}
			rig.client.clock = clock
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)
			if owed := rig.client.owedExperimentExposureCount(); owed != 2 {
				t.Fatalf("setup: %d owed application(s), want the exposure and the outcome", owed)
			}

			result, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, band, nil)
			if err == nil || result.Assigned || result.FromCache {
				t.Errorf("the %s fetch across a 503 must fail closed, got %+v err=%v", band, result, err)
			}
			if rig.script.requestCount() != 2 {
				t.Fatalf("setup: %d assignment request(s), want the adult fetch and the failed %s fetch", rig.script.requestCount(), band)
			}
			clock.advance(10 * time.Minute)
			assertExperimentWithdrawn(t, rig, spool, 2, 2)
		})
	}
}

// The same scene with the applications already sealed and spooled (their
// delivery failing) before the declaration: the spooled facts are withdrawn
// at once, not delivered when the ingest recovers.
func TestANonAdultDeclarationWhoseFetchFailsWithdrawsSpooledFacts(t *testing.T) {
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"))
	rig.script.push(http.StatusServiceUnavailable, ageTransientBody)
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	rig.capture.setStatus(http.StatusInternalServerError)
	rig.client.experimentCycle(context.Background())
	_ = rig.client.Flush(context.Background())
	if spooled := spooledExperimentFacts(rig.client, ageGoldenExperiment); spooled != 2 {
		t.Fatalf("setup: %d fact(s) spooled, want the sealed exposure and outcome", spooled)
	}

	if _, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil); err == nil {
		t.Fatalf("setup: the under_threshold fetch must fail across the 503")
	}
	if spooled := spooledExperimentFacts(rig.client, ageGoldenExperiment); spooled != 0 {
		t.Errorf("withdraw: %d spooled fact(s) survive the non-adult declaration", spooled)
	}
	assertExperimentWithdrawn(t, rig, spool, 0, 2)
}

// The declaration withdraws one experiment: another experiment the server
// assigned under the adult declaration keeps serving and delivering.
func TestANonAdultDeclarationLeavesOtherExperimentsServing(t *testing.T) {
	const other = "exposure-banner-2"
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageGoldenForKey(t, "adult", other))
	rig.script.push(http.StatusServiceUnavailable, ageTransientBody)
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	fetchAdultAssignment(t, rig.client, other)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	rig.applyAndMeasure(t, other)

	if _, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil); err == nil {
		t.Fatalf("setup: the under_threshold fetch must fail across the 503")
	}
	if variant := rig.client.ExperimentVariant(other); variant != "control" {
		t.Errorf("the other experiment must keep serving, got %q", variant)
	}
	if owed := rig.client.owedExperimentExposureCount(); owed != 2 {
		t.Errorf("%d owed application(s), want the other experiment's two", owed)
	}
	rig.client.experimentCycle(context.Background())
	flushOrFail(t, rig.client)
	if delivered := deliveredExperimentFacts(rig.capture, other); len(delivered) != 2 {
		t.Errorf("the other experiment delivered %d fact(s), want its exposure and outcome", len(delivered))
	}
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 0 {
		t.Errorf("the declared experiment delivered %d fact(s)", len(delivered))
	}
}

// An adult declaration is not a withdrawal: re-declaring adult across a 503
// keeps serving the cached adult assignment and delivering what is owed.
func TestAnAdultDeclarationAcrossAFailureKeepsServing(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	rig.script.push(http.StatusServiceUnavailable, ageTransientBody)
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)

	result, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil)
	if err != nil || !result.Assigned || !result.FromCache {
		t.Errorf("an adult declaration across a 503 must serve the cached adult assignment, got %+v err=%v", result, err)
	}
	if variant := rig.client.ExperimentVariant(ageGoldenExperiment); variant != "control" {
		t.Errorf("the adult assignment must keep serving, got %q", variant)
	}
	if owed := rig.client.owedExperimentExposureCount(); owed != 2 {
		t.Errorf("%d owed application(s), want both kept", owed)
	}
	rig.client.experimentCycle(context.Background())
	flushOrFail(t, rig.client)
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 2 {
		t.Errorf("%d fact(s) delivered, want the exposure and the outcome", len(delivered))
	}
}

// While the declaring fetch is in flight nothing is served or recorded, and
// the owed applications wait for the server: its answer, when it comes,
// governs. An age_ineligible answer withdraws them, each counted once; a
// kill_switch answer keeps them, exactly as without the declaration.
func TestANonAdultDeclarationStopsServingUntilTheServerAnswers(t *testing.T) {
	for _, tc := range []struct {
		reason    string
		withdrawn bool
	}{{"age_ineligible", true}, {"kill_switch", false}} {
		t.Run(tc.reason, func(t *testing.T) {
			rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, tc.reason))
			release := make(chan struct{})
			rig.script.gates = map[int]chan struct{}{1: release}
			fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
			rig.applyAndMeasure(t, ageGoldenExperiment)

			done := make(chan error, 1)
			go func() {
				_, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
				done <- err
			}()
			waitFor(t, 5*time.Second, "the under_threshold fetch is in flight", func() bool { return rig.script.requestCount() == 2 })
			variant := rig.client.ExperimentVariant(ageGoldenExperiment)
			applied, _ := rig.client.ApplyExperimentVariant(ageGoldenExperiment)
			outcomeErr := rig.client.TrackExperimentOutcome(ageGoldenExperiment, "score", 3)
			// The lane ticks while the declaration is unanswered: it sends
			// none of the experiment's owed applications.
			rig.client.experimentCycle(context.Background())
			exposuresInFlight, outcomesInFlight := rig.applyRequests()
			owedInFlight := rig.client.owedExperimentExposureCount()
			close(release)
			if err := <-done; err != nil {
				t.Fatalf("the server's answer: %v", err)
			}
			if variant != "" || applied != "" {
				t.Errorf("stop serving: %q served and %q applied while the under_threshold fetch was in flight", variant, applied)
			}
			if !errors.Is(outcomeErr, ErrExperimentNoAssignment) {
				t.Errorf("stop serving: an outcome recorded while the under_threshold fetch was in flight: %v", outcomeErr)
			}
			if exposuresInFlight+outcomesInFlight != 0 {
				t.Errorf("withdraw: the lane sent %d exposure and %d outcome apply request(s) while the under_threshold fetch was in flight", exposuresInFlight, outcomesInFlight)
			}
			if owedInFlight != 2 {
				t.Errorf("%d owed application(s) while the fetch was in flight, want both waiting for the answer", owedInFlight)
			}
			exposures, outcomes := rig.drops("age_ineligible")
			owed := rig.client.owedExperimentExposureCount()
			if tc.withdrawn {
				if owed != 0 || exposures != 1 || outcomes != 1 {
					t.Errorf("the age_ineligible answer must withdraw each application once: owed=%d exposure=%d outcome=%d", owed, exposures, outcomes)
				}
				return
			}
			if owed != 2 || exposures+outcomes != 0 {
				t.Errorf("the kill_switch answer governs: owed=%d age_ineligible drops exposure=%d outcome=%d, want both kept and none counted", owed, exposures, outcomes)
			}
			rig.client.experimentCycle(context.Background())
			flushOrFail(t, rig.client)
			if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 2 {
				t.Errorf("after the kill_switch answer %d fact(s) delivered, want the exposure and the outcome", len(delivered))
			}
		})
	}
}

// While the declaring fetch is in flight the revalidation lane does not
// re-send the entry's remembered adult declaration.
func TestTheLaneDoesNotRevalidateWhileANonAdultDeclarationIsInFlight(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
	release := make(chan struct{})
	rig.script.gates = map[int]chan struct{}{1: release}
	clock := &expFakeClock{now: time.Now()}
	rig.client.clock = clock
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)

	done := make(chan error, 1)
	go func() {
		_, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
		done <- err
	}()
	waitFor(t, 5*time.Second, "the under_threshold fetch is in flight", func() bool { return rig.script.requestCount() == 2 })
	clock.advance(10 * time.Minute)
	rig.client.experimentCycle(context.Background())
	requests := rig.script.requestCount()
	close(release)
	if err := <-done; err != nil {
		t.Errorf("the server's answer to the declaration: %v", err)
	}
	if requests != 2 {
		t.Errorf("no stale re-send: the lane sent %d request(s) while the under_threshold fetch was in flight", requests-2)
	}
	if adult := declaresAdultAfter(rig.script, 1); len(adult) != 0 {
		t.Errorf("no stale re-send: request(s) %v declared age_band=adult after the host declared under_threshold", adult)
	}
}

// An auth latch is not the server's answer to the declaration: the latch
// retains the assignment for a later unlatch, so the declaration withdraws
// it, and nothing re-serves it when a later fetch unlatches.
func TestANonAdultDeclarationAnsweredByAnAuthLatchWithdraws(t *testing.T) {
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"))
	rig.script.push(http.StatusUnauthorized, `{"error":"unauthorized"}`)
	rig.script.push(http.StatusOK, ageGoldenForKey(t, "adult", "exposure-banner-2"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)

	if _, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil); err == nil {
		t.Fatalf("setup: the under_threshold fetch must be refused by the 401")
	}
	// Another experiment's authorized answer unlatches and restores the
	// latch-retained assignments.
	if result := fetchAdultAssignment(t, rig.client, "exposure-banner-2"); !result.Assigned {
		t.Fatalf("setup: the unlatching fetch must be assigned, got %+v", result)
	}
	assertExperimentWithdrawn(t, rig, spool, 2, 2)
}

// An adult fetch that left before the host declared under_threshold, and
// whose 200 lands while that declaration's fetch is still in flight, hands
// its caller no variant; and when the declaration's fetch then fails, the
// adult answer it installed is withdrawn with the rest.
func TestAnAdultAnswerLandingWhileANonAdultDeclarationIsPendingIsNotServed(t *testing.T) {
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"), ageGolden(t, "adult"))
	rig.script.push(http.StatusServiceUnavailable, ageTransientBody)
	adultGate := make(chan struct{})
	declarationGate := make(chan struct{})
	rig.script.gates = map[int]chan struct{}{1: adultGate, 2: declarationGate}
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)

	type answer struct {
		result ExperimentAssignmentResult
		err    error
	}
	adultDone := make(chan answer, 1)
	go func() {
		result, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil)
		adultDone <- answer{result, err}
	}()
	waitFor(t, 5*time.Second, "the adult fetch is in flight", func() bool { return rig.script.requestCount() == 2 })
	declarationDone := make(chan error, 1)
	go func() {
		_, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
		declarationDone <- err
	}()
	waitFor(t, 5*time.Second, "the under_threshold fetch is in flight", func() bool { return rig.script.requestCount() == 3 })
	close(adultGate)
	late := <-adultDone
	if late.err == nil && late.result.Assigned {
		t.Errorf("stop serving: the adult caller received %q while the under_threshold declaration was pending", late.result.VariantKey)
	}
	if variant := rig.client.ExperimentVariant(ageGoldenExperiment); variant != "" {
		t.Errorf("stop serving: %q served while the under_threshold declaration was pending", variant)
	}
	close(declarationGate)
	if err := <-declarationDone; err == nil {
		t.Fatalf("setup: the under_threshold fetch must fail across the 503")
	}
	assertExperimentWithdrawn(t, rig, spool, 2, 3)
}

// No stale re-send: a revalidation that left with the adult declaration
// before the host declared under_threshold, and whose adult 200 arrives
// after that declaration's fetch failed, installs nothing, persists
// nothing, and leaves nothing for a later cycle to re-send as adult.
func TestAnAdultRevalidationInFlightLosesToAnUnconfirmedNonAdultDeclaration(t *testing.T) {
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"), ageGolden(t, "adult"))
	rig.script.push(http.StatusServiceUnavailable, ageTransientBody)
	lane := make(chan struct{})
	rig.script.gates = map[int]chan struct{}{1: lane}
	clock := &expFakeClock{now: time.Now()}
	rig.client.clock = clock
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)

	clock.advance(10 * time.Minute)
	cycled := make(chan struct{})
	go func() {
		rig.client.experimentCycle(context.Background())
		close(cycled)
	}()
	waitFor(t, 5*time.Second, "the adult revalidation is in flight", func() bool { return rig.script.requestCount() == 2 })
	if got := rig.script.request(1).URL.Query().Get("age_band"); got != "adult" {
		t.Fatalf("setup: the revalidation declared age_band=%q", got)
	}
	if _, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil); err == nil {
		t.Fatalf("setup: the under_threshold fetch must fail across the 503")
	}
	close(lane)
	<-cycled

	if variant := rig.client.ExperimentVariant(ageGoldenExperiment); variant != "" {
		t.Errorf("no stale re-send: the in-flight adult revalidation reinstalled %q", variant)
	}
	if durableRecordHolds(t, spool, ageGoldenExperiment) {
		t.Errorf("no stale re-send: the in-flight adult revalidation persisted the assignment")
	}
	clock.advance(10 * time.Minute)
	rig.client.experimentCycle(context.Background())
	if adult := declaresAdultAfter(rig.script, 3); len(adult) != 0 {
		t.Errorf("no stale re-send: request(s) %v declared age_band=adult after the host declared under_threshold", adult)
	}
}

// A later adult declaration decides afresh: the next fetch is sent, and the
// re-admission is a new application with a fresh exposure id, delivered.
func TestAnAdultDeclarationAfterAnUnconfirmedNonAdultOneIsANewApplication(t *testing.T) {
	rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
	rig.script.push(http.StatusServiceUnavailable, ageTransientBody)
	rig.script.push(http.StatusOK, ageGolden(t, "adult"))
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	if variant, _ := rig.client.ApplyExperimentVariant(ageGoldenExperiment); variant != "control" {
		t.Fatalf("setup: apply served %q", variant)
	}
	rig.client.exp.mu.Lock()
	var withdrawnID string
	if owed := rig.client.exp.pendingExposure[ageGoldenExperiment]; len(owed) == 1 {
		withdrawnID = owed[0].app.exposureID
	}
	rig.client.exp.mu.Unlock()
	if withdrawnID == "" {
		t.Fatalf("setup: the first application must be owed")
	}
	if _, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil); err == nil {
		t.Fatalf("setup: the under_threshold fetch must fail across the 503")
	}

	if result := fetchAdultAssignment(t, rig.client, ageGoldenExperiment); !result.Assigned || result.FromCache {
		t.Fatalf("the adult re-declaration must be decided by a new fetch, got %+v", result)
	}
	if rig.script.requestCount() != 3 {
		t.Errorf("%d assignment request(s), want the re-declaration sent", rig.script.requestCount())
	}
	if variant, _ := rig.client.ApplyExperimentVariant(ageGoldenExperiment); variant != "control" {
		t.Fatalf("the re-admitted assignment must be applied, got %q", variant)
	}
	rig.client.experimentCycle(context.Background())
	applies := rig.script.apply.requestsSoFar()
	if len(applies) != 1 {
		t.Fatalf("%d exposure apply request(s), want the re-admission's one", len(applies))
	}
	if id, _ := applies[0].body["exposure_id"].(string); id == "" || id == withdrawnID {
		t.Errorf("the re-admission must be a new application with a fresh id, got exposure_id=%q (withdrawn %q)", id, withdrawnID)
	}
	flushOrFail(t, rig.client)
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 1 {
		t.Errorf("%d fact(s) delivered, want the re-admission's exposure", len(delivered))
	}
}

// ── #138 R3: the missing transport scenes ───────────────────────────────────

// The forced-minor floor stays closed with an adult declaration: cold (no
// traffic, no subject minted) and with a cached, applied assignment (nothing
// served, applied or recorded, no apply request).
func TestTheForcedMinorFloorStaysClosedForAnAdultDeclaration(t *testing.T) {
	t.Run("cold", func(t *testing.T) {
		script := &expScript{}
		script.push(http.StatusOK, ageGolden(t, "adult"))
		server := newExperimentServer(t, script, &expWireCapture{})
		defer server.Close()
		client := ageClient(t, server.URL, t.TempDir())
		defer client.Close(context.Background())
		if _, err := client.SetConsentDecision(ConsentDecisionDeniedForcedMinor); err != nil {
			t.Fatal(err)
		}
		if _, err := client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); !errors.Is(err, ErrConsentDenied) {
			t.Errorf("an adult declaration under the forced-minor floor must be refused with ErrConsentDenied, got %v", err)
		}
		client.experimentCycle(context.Background())
		client.exp.mu.Lock()
		subject := client.exp.subjectID
		client.exp.mu.Unlock()
		if script.requestCount() != 0 || subject != "" {
			t.Errorf("forced minor + adult declaration produced %d request(s) and subject %q", script.requestCount(), subject)
		}
	})
	t.Run("cached_and_applied", func(t *testing.T) {
		rig := newAgeWithdrawRig(t, t.TempDir(), nil, ageGolden(t, "adult"))
		fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
		if variant, _ := rig.client.ApplyExperimentVariant(ageGoldenExperiment); variant != "control" {
			t.Fatalf("setup: apply served %q", variant)
		}
		if _, err := rig.client.SetConsentDecision(ConsentDecisionDeniedForcedMinor); err != nil {
			t.Fatal(err)
		}
		if _, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil); !errors.Is(err, ErrConsentDenied) {
			t.Errorf("an adult declaration under the forced-minor floor must be refused with ErrConsentDenied, got %v", err)
		}
		if variant, _ := rig.client.ApplyExperimentVariant(ageGoldenExperiment); variant != "" {
			t.Errorf("the forced-minor floor served %q", variant)
		}
		if err := rig.client.TrackExperimentOutcome(ageGoldenExperiment, "score", 1); !errors.Is(err, ErrConsentDenied) {
			t.Errorf("an outcome under the forced-minor floor must be refused with ErrConsentDenied, got %v", err)
		}
		rig.client.experimentCycle(context.Background())
		if exposures, outcomes := rig.applyRequests(); rig.script.requestCount() != 1 || exposures+outcomes != 0 {
			t.Errorf("forced-minor traffic: fetches=%d exposure applies=%d outcome applies=%d", rig.script.requestCount(), exposures, outcomes)
		}
	})
}

// An adult 200 in flight when a later refusal settles installs nothing,
// hands its caller no variant, and persists nothing.
func TestAnAdultResponseInFlightLosesToALaterRefusal(t *testing.T) {
	script := &expScript{}
	script.push(http.StatusOK, ageGolden(t, "adult"))
	script.push(http.StatusOK, ageGolden(t, "under_threshold"))
	slow := make(chan struct{})
	script.gates = map[int]chan struct{}{0: slow}
	server := newExperimentServer(t, script, &expWireCapture{})
	defer server.Close()
	spool := t.TempDir()
	client := ageClient(t, server.URL, spool)
	defer client.Close(context.Background())
	client.SetConsent(true)

	type answer struct {
		result ExperimentAssignmentResult
		err    error
	}
	done := make(chan answer, 1)
	go func() {
		result, err := client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, nil)
		done <- answer{result, err}
	}()
	waitFor(t, 5*time.Second, "the adult fetch is in flight", func() bool { return script.requestCount() == 1 })
	if result, err := client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil); err != nil || result.Reason != "age_ineligible" {
		t.Fatalf("setup: the refusal must land, got %+v err=%v", result, err)
	}
	close(slow)
	late := <-done
	if late.result.Assigned {
		t.Errorf("the superseded adult caller received a variant: %+v", late.result)
	}
	if variant := client.ExperimentVariant(ageGoldenExperiment); variant != "" {
		t.Errorf("the late adult response reinstalled %q", variant)
	}
	_ = client.Close(context.Background())
	restarted := ageClient(t, server.URL, spool)
	defer restarted.Close(context.Background())
	if variant := restarted.ExperimentVariant(ageGoldenExperiment); variant != "" {
		t.Errorf("the late adult response persisted %q", variant)
	}
}

// A revalidation in flight (declaring adult) when a later refusal settles
// installs nothing and persists nothing.
func TestARevalidationInFlightLosesToALaterRefusal(t *testing.T) {
	script := &expScript{}
	script.push(http.StatusOK, ageGolden(t, "adult"))
	script.push(http.StatusOK, ageGolden(t, "adult"))
	script.push(http.StatusOK, ageGolden(t, "under_threshold"))
	lane := make(chan struct{})
	script.gates = map[int]chan struct{}{1: lane}
	server := newExperimentServer(t, script, &expWireCapture{})
	defer server.Close()
	spool := t.TempDir()
	client := ageClient(t, server.URL, spool)
	defer client.Close(context.Background())
	client.SetConsent(true)
	clock := &expFakeClock{now: time.Now()}
	client.clock = clock

	fetchAdultAssignment(t, client, ageGoldenExperiment)
	clock.advance(10 * time.Minute)
	cycled := make(chan struct{})
	go func() {
		client.experimentCycle(context.Background())
		close(cycled)
	}()
	waitFor(t, 5*time.Second, "the revalidation is in flight", func() bool { return script.requestCount() == 2 })
	if got := script.request(1).URL.Query().Get("age_band"); got != "adult" {
		t.Fatalf("setup: the revalidation declared age_band=%q", got)
	}
	if result, err := client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil); err != nil || result.Reason != "age_ineligible" {
		t.Fatalf("setup: the refusal must land, got %+v err=%v", result, err)
	}
	close(lane)
	<-cycled
	if variant := client.ExperimentVariant(ageGoldenExperiment); variant != "" {
		t.Errorf("the in-flight revalidation reinstalled %q", variant)
	}
	_ = client.Close(context.Background())
	restarted := ageClient(t, server.URL, spool)
	defer restarted.Close(context.Background())
	if variant := restarted.ExperimentVariant(ageGoldenExperiment); variant != "" {
		t.Errorf("the in-flight revalidation persisted %q", variant)
	}
}

// ── scope: client-id assignments only ───────────────────────────────────────

// expSyntheticAssignedBody is an assigned body for a synthetic-subject
// assignment, which has no age gate: the platform assigns it whatever age
// the host declares.
func expSyntheticAssignedBody() string {
	return strings.Replace(expAssignedBody("1"), `"assignment_unit":"client_id"`, `"assignment_unit":"synthetic_subject_key"`, 1)
}

// A synthetic-subject assignment is outside the age rule: a non-adult
// declaration neither stops it serving while its fetch is in flight nor
// withdraws it when that fetch fails. The transient serves the cached
// assignment, as it always has.
func TestANonAdultDeclarationLeavesASyntheticSubjectAssignmentServing(t *testing.T) {
	script := &expScript{}
	script.push(http.StatusOK, expSyntheticAssignedBody())
	script.push(http.StatusServiceUnavailable, ageTransientBody)
	release := make(chan struct{})
	script.gates = map[int]chan struct{}{1: release}
	server := newExperimentServer(t, script, &expWireCapture{})
	defer server.Close()
	spool := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spool })
	defer client.Close(context.Background())
	client.SetConsent(true)

	if result, err := client.FetchExperimentAssignmentWithAgeBand(context.Background(), expTestScopeKey, ExperimentAgeBandUnderThreshold, nil); err != nil || !result.Assigned {
		t.Fatalf("setup: the synthetic-subject assignment must be assigned, got %+v err=%v", result, err)
	}
	type answer struct {
		result ExperimentAssignmentResult
		err    error
	}
	done := make(chan answer, 1)
	go func() {
		result, err := client.FetchExperimentAssignmentWithAgeBand(context.Background(), expTestScopeKey, ExperimentAgeBandUnderThreshold, nil)
		done <- answer{result, err}
	}()
	waitFor(t, 5*time.Second, "the second under_threshold fetch is in flight", func() bool { return script.requestCount() == 2 })
	inFlight := client.ExperimentVariant(expTestScopeKey)
	close(release)
	late := <-done
	if inFlight != "treatment" {
		t.Errorf("a synthetic-subject assignment must keep serving while the declaration is in flight, got %q", inFlight)
	}
	if late.err != nil || !late.result.Assigned || !late.result.FromCache {
		t.Errorf("the transient must serve the cached synthetic-subject assignment, got %+v err=%v", late.result, late.err)
	}
	if variant := client.ExperimentVariant(expTestScopeKey); variant != "treatment" {
		t.Errorf("a synthetic-subject assignment must not be withdrawn by an unanswered declaration, got %q", variant)
	}
	if !durableRecordHolds(t, spool, expTestScopeKey) {
		t.Errorf("a synthetic-subject assignment must stay in the durable record")
	}
}

// The declaring fetch itself hands back no client-id assignment it is about
// to withdraw: a cached assignment whose remembered attributes match the
// non-adult request is not served from cache when the fetch fails.
func TestTheDeclaringFetchDoesNotServeTheAssignmentItWithdraws(t *testing.T) {
	spool := t.TempDir()
	// The stub answers the under_threshold request with an assignment, so the
	// cached client-id entry remembers under_threshold.
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"))
	rig.script.push(http.StatusServiceUnavailable, ageTransientBody)
	if result, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil); err != nil || !result.Assigned {
		t.Fatalf("setup: the stub's assignment must install, got %+v err=%v", result, err)
	}
	rig.applyAndMeasure(t, ageGoldenExperiment)

	result, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
	if err == nil || result.Assigned || result.FromCache {
		t.Errorf("the declaring fetch served the assignment its declaration withdraws: %+v err=%v", result, err)
	}
	assertExperimentWithdrawn(t, rig, spool, 2, 2)
}

// No stale re-send through a re-mint: an adult revalidation in flight when
// the host declares under_threshold, answered with the subject-grammar
// reject, does not retry with its remembered adult declaration while the
// declaration is pending.
func TestARevalidationRemintRetryDoesNotResendAdultWhileADeclarationIsPending(t *testing.T) {
	spool := t.TempDir()
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"))
	rig.script.push(http.StatusBadRequest, `{"error":"experiment metadata must use synthetic local-safe identifiers only"}`)
	rig.script.push(http.StatusServiceUnavailable, ageTransientBody)
	rig.script.push(http.StatusOK, ageGolden(t, "adult"))
	lane := make(chan struct{})
	declarationGate := make(chan struct{})
	rig.script.gates = map[int]chan struct{}{1: lane, 2: declarationGate}
	clock := &expFakeClock{now: time.Now()}
	rig.client.clock = clock
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)

	clock.advance(10 * time.Minute)
	cycled := make(chan struct{})
	go func() {
		rig.client.experimentCycle(context.Background())
		close(cycled)
	}()
	waitFor(t, 5*time.Second, "the adult revalidation is in flight", func() bool { return rig.script.requestCount() == 2 })
	declared := make(chan error, 1)
	go func() {
		_, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
		declared <- err
	}()
	waitFor(t, 5*time.Second, "the under_threshold fetch is in flight", func() bool { return rig.script.requestCount() == 3 })
	close(lane)
	<-cycled
	close(declarationGate)
	if err := <-declared; err == nil {
		t.Fatalf("setup: the under_threshold fetch must fail across the 503")
	}

	if adult := declaresAdultAfter(rig.script, 2); len(adult) != 0 {
		t.Errorf("no stale re-send: request(s) %v declared age_band=adult after the host declared under_threshold", adult)
	}
	if variant := rig.client.ExperimentVariant(ageGoldenExperiment); variant != "" {
		t.Errorf("the experiment is served after the unanswered non-adult declaration: %q", variant)
	}
}

// A synthetic-subject assignment is kept by an unanswered declaration, but
// the fence still rises: an adult fetch dispatched before the declaration,
// answered with a client-id assignment after it ended, installs nothing.
func TestAnUnansweredDeclarationKeepingASyntheticAssignmentStillFencesEarlierFetches(t *testing.T) {
	script := &expScript{}
	script.push(http.StatusOK, expSyntheticAssignedBody())
	script.push(http.StatusOK, strings.Replace(expAssignedBody("2"), `"variant_key":"treatment"`, `"variant_key":"client_id_variant"`, 1))
	script.push(http.StatusServiceUnavailable, ageTransientBody)
	adultGate := make(chan struct{})
	declarationGate := make(chan struct{})
	script.gates = map[int]chan struct{}{1: adultGate, 2: declarationGate}
	server := newExperimentServer(t, script, &expWireCapture{})
	defer server.Close()
	spool := t.TempDir()
	client := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spool })
	defer client.Close(context.Background())
	client.SetConsent(true)

	if result, err := client.FetchExperimentAssignmentWithAgeBand(context.Background(), expTestScopeKey, ExperimentAgeBandUnderThreshold, nil); err != nil || !result.Assigned {
		t.Fatalf("setup: the synthetic-subject assignment must be assigned, got %+v err=%v", result, err)
	}
	adultDone := make(chan struct{})
	go func() {
		_, _ = client.FetchExperimentAssignmentWithAgeBand(context.Background(), expTestScopeKey, ExperimentAgeBandAdult, nil)
		close(adultDone)
	}()
	waitFor(t, 5*time.Second, "the adult fetch is in flight", func() bool { return script.requestCount() == 2 })
	declared := make(chan error, 1)
	go func() {
		_, err := client.FetchExperimentAssignmentWithAgeBand(context.Background(), expTestScopeKey, ExperimentAgeBandUnderThreshold, nil)
		declared <- err
	}()
	waitFor(t, 5*time.Second, "the under_threshold fetch is in flight", func() bool { return script.requestCount() == 3 })
	close(declarationGate)
	<-declared
	close(adultGate)
	<-adultDone

	if variant := client.ExperimentVariant(expTestScopeKey); variant == "client_id_variant" {
		t.Errorf("the adult fetch dispatched before the unanswered non-adult declaration installed its client-id assignment")
	}
}

// An experiment now served as a synthetic-subject assignment still owes the
// client-id applications made before it: a pending non-adult declaration
// records no outcome against them, and an unanswered one withdraws them
// while it keeps the synthetic assignment.
func TestAnUnansweredDeclarationWithdrawsClientIDApplicationsBeneathASyntheticAssignment(t *testing.T) {
	spool := t.TempDir()
	synthetic := strings.Replace(ageGolden(t, "adult"), `"assignment_unit":"client_id"`, `"assignment_unit":"synthetic_subject_key"`, 1)
	if synthetic == ageGolden(t, "adult") {
		t.Fatalf("setup: the adult golden must name its assignment unit")
	}
	rig := newAgeWithdrawRig(t, spool, nil, ageGolden(t, "adult"), synthetic)
	rig.script.push(http.StatusServiceUnavailable, ageTransientBody)
	declarationGate := make(chan struct{})
	rig.script.gates = map[int]chan struct{}{2: declarationGate}
	fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
	rig.applyAndMeasure(t, ageGoldenExperiment)
	if result := fetchAdultAssignment(t, rig.client, ageGoldenExperiment); !result.Assigned {
		t.Fatalf("setup: the synthetic-subject assignment must install, got %+v", result)
	}

	declared := make(chan error, 1)
	go func() {
		_, err := rig.client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandUnderThreshold, nil)
		declared <- err
	}()
	waitFor(t, 5*time.Second, "the under_threshold fetch is in flight", func() bool { return rig.script.requestCount() == 3 })
	outcomeErr := rig.client.TrackExperimentOutcome(ageGoldenExperiment, "score", 3)
	close(declarationGate)
	if err := <-declared; err == nil {
		t.Fatalf("setup: the under_threshold fetch must fail across the 503")
	}
	if !errors.Is(outcomeErr, ErrExperimentNoAssignment) {
		t.Errorf("an outcome was recorded against the client-id application while the declaration was pending: %v", outcomeErr)
	}
	if owed := rig.client.owedExperimentExposureCount(); owed != 0 {
		t.Errorf("withdraw: %d owed client-id application(s) survive beneath the synthetic assignment", owed)
	}
	rig.client.experimentCycle(context.Background())
	flushOrFail(t, rig.client)
	if delivered := deliveredExperimentFacts(rig.capture, ageGoldenExperiment); len(delivered) != 0 {
		t.Errorf("withdraw: %d client-id fact(s) delivered after the unanswered non-adult declaration", len(delivered))
	}
	if variant := rig.client.ExperimentVariant(ageGoldenExperiment); variant != "control" {
		t.Errorf("the synthetic-subject assignment must keep serving, got %q", variant)
	}
}
