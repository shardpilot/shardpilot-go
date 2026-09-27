package shardpilot

import (
	"context"
	"strings"
	"testing"
)

// An exposure is recorded by the host's explicit application of a variant,
// ApplyExperimentVariant, and by nothing else: a getter read, a fetch, a
// revalidation and a cache restore record nothing. The application records
// once per (experiment, version, subject, session).

const expSecondKey = "exp-second"

func expAssignedBodyForKey(key string) string {
	return strings.Replace(expAssignedBody("1"), `"experiment_key":"`+expTestScopeKey+`"`, `"experiment_key":"`+key+`"`, 1)
}

func flushOrFail(t *testing.T, c *Client) {
	t.Helper()
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func TestAFetchAndAGetterReadRecordNoExposure(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	for i := 0; i < 3; i++ {
		if client.ExperimentVariant(expTestScopeKey) != "treatment" || client.ExperimentVariantPayload(expTestScopeKey) == nil {
			t.Fatal("control: the getters must serve the cached assignment")
		}
	}
	client.experimentCycle(context.Background())
	flushOrFail(t, client)
	if got := len(capture.exposures()); got != 0 {
		t.Fatalf("a fetch, three getter reads and a lane cycle must record nothing, got %d exposure fact(s)", got)
	}
}

func TestApplyExperimentVariantRecordsOncePerSession(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	if got, _ := client.ApplyExperimentVariant(expTestScopeKey); got != "treatment" {
		t.Fatalf("ApplyExperimentVariant must return the served variant, got %q", got)
	}
	client.experimentCycle(context.Background())
	flushOrFail(t, client)
	if got := len(capture.exposures()); got != 1 {
		t.Fatalf("the application must record one exposure, got %d", got)
	}
	// Applying again, refetching and applying once more is the same
	// application in the same session. (A new session, and a restored
	// assignment, are TestRestoreFromDiskServesAndRecordsOnlyWhenApplied.)
	client.ApplyExperimentVariant(expTestScopeKey)
	fetchAssignment(t, client, expTestScopeKey)
	client.ApplyExperimentVariant(expTestScopeKey)
	client.experimentCycle(context.Background())
	flushOrFail(t, client)
	if got := len(capture.exposures()); got != 1 {
		t.Fatalf("once per (experiment, version, subject, session): got %d", got)
	}
}

// ApplyExperimentVariant serves exactly what ExperimentVariant serves, and
// records nothing where it serves nothing.
func TestApplyExperimentVariantServesWhatTheGetterServes(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	if got, _ := client.ApplyExperimentVariant(expTestScopeKey); got != "" {
		t.Fatalf("nothing cached: want \"\", got %q", got)
	}
	fetchAssignment(t, client, expTestScopeKey)
	if got, _ := client.ApplyExperimentVariant("  "); got != "" {
		t.Fatalf("an empty key: want \"\", got %q", got)
	}
	client.SetConsent(false)
	if got, _ := client.ApplyExperimentVariant(expTestScopeKey); got != "" || client.ExperimentVariant(expTestScopeKey) != "" {
		t.Fatalf("a refused plane serves nothing and records nothing, got %q", got)
	}
	client.SetConsent(true)
	client.experimentCycle(context.Background())
	flushOrFail(t, client)
	if got := len(capture.exposures()); got != 0 {
		t.Fatalf("no application was served, so nothing may be recorded; got %d", got)
	}

	dark := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.ExperimentsEnabled = false })
	defer dark.Close(context.Background())
	if got, _ := dark.ApplyExperimentVariant(expTestScopeKey); got != "" {
		t.Fatalf("experiments not configured: want \"\", got %q", got)
	}
}

// A consent purge re-arms the session's APPLIED tuples only: an assignment
// that was fetched and never applied stays unrecorded after the re-grant.
func TestConsentPurgeReArmsOnlyAppliedAssignments(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, expAssignedBodyForKey(expSecondKey))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	fetchAssignment(t, client, expSecondKey)
	client.ApplyExperimentVariant(expTestScopeKey)
	client.experimentCycle(context.Background())
	flushOrFail(t, client)
	if got := len(capture.exposures()); got != 1 {
		t.Fatalf("precondition: only the applied experiment records, got %d", got)
	}
	client.SetConsent(false)
	client.SetConsent(true)
	client.experimentCycle(context.Background())
	flushOrFail(t, client)
	facts := capture.exposures()
	if len(facts) != 2 {
		t.Fatalf("the purge must re-arm the applied tuple and only it, got %d fact(s)", len(facts))
	}
	for _, fact := range facts {
		if props := fact["props"].(map[string]any); props["experiment_key"] != expTestScopeKey {
			t.Fatalf("an experiment that was never applied was recorded: %v", props)
		}
	}
}

// fetchAndApply is the host's fetch followed by its application of the
// variant and one cycle of the background lane, which seals the application
// and hands the fact to the queue — what a fetch alone once recorded.
func fetchAndApply(t *testing.T, c *Client, key string) ExperimentAssignmentResult {
	t.Helper()
	result := fetchAssignment(t, c, key)
	c.ApplyExperimentVariant(key)
	c.experimentCycle(context.Background())
	return result
}

// Review round 1 of #125.

// F1: a denial landing between ApplyExperimentVariant's consent check and
// its lock must not see the variant served or an exposure armed — the same
// commit-point re-check the fetch makes.
func TestADenialRacingApplyServesAndRecordsNothing(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())
	client.SetConsent(true)
	fetchAssignment(t, client, expTestScopeKey)

	raced := false
	client.exp.mu.Lock()
	client.exp.consentRaceSeam = func(stage string) {
		if stage == "apply_serve" && !raced {
			raced = true
			client.SetConsent(false)
		}
	}
	client.exp.mu.Unlock()
	if got, _ := client.ApplyExperimentVariant(expTestScopeKey); got != "" {
		t.Fatalf("a denial that landed before the lock must see nothing served, got %q", got)
	}
	if !raced {
		t.Fatal("control: the seam must have fired")
	}
	client.SetConsent(true)
	client.experimentCycle(context.Background())
	flushOrFail(t, client)
	if got := len(capture.exposures()); got != 0 {
		t.Fatalf("an application refused by consent must record nothing after the re-grant, got %d", got)
	}
}

// F2: the payload comes from the same entry the exposure is recorded for,
// and is the host's own copy.
func TestApplyReturnsThePayloadOfTheRecordedAssignment(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	script.push(200, strings.Replace(expAssignedBody("2"), `{"speed":2}`, `{"speed":3}`, 1))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())

	fetchAssignment(t, client, expTestScopeKey)
	variant, payload := client.ApplyExperimentVariant(expTestScopeKey)
	client.experimentCycle(context.Background())
	if variant != "treatment" || payload["speed"] != float64(2) {
		t.Fatalf("version 1: got %q %v", variant, payload)
	}
	payload["speed"] = 99
	if cached := client.ExperimentVariantPayload(expTestScopeKey); cached["speed"] != float64(2) {
		t.Fatalf("the returned payload must be the host's copy, the cache now reads %v", cached)
	}
	fetchAssignment(t, client, expTestScopeKey)
	variant, payload = client.ApplyExperimentVariant(expTestScopeKey)
	if variant != "treatment" || payload["speed"] != float64(3) {
		t.Fatalf("version 2: got %q %v", variant, payload)
	}
	client.experimentCycle(context.Background())
	flushOrFail(t, client)
	versions := map[float64]bool{}
	for _, fact := range capture.exposures() {
		versions[fact["props"].(map[string]any)["experiment_version"].(float64)] = true
	}
	if len(versions) != 2 || !versions[1] || !versions[2] {
		t.Fatalf("each returned payload's version must be the one recorded, got versions %v", versions)
	}
}

// F4: a first application made through TrackExperimentExposure, whose fact
// a denial drains between its enqueue and its bookkeeping, is delivered again
// after the re-grant: the purge saw it still owed and re-armed it, with the
// same application identity.
func TestAFirstExplicitApplicationSurvivesARacedPurge(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	client := newExperimentClient(t, server.URL, nil)
	defer client.Close(context.Background())
	client.SetConsent(true)
	fetchAssignment(t, client, expTestScopeKey)

	raced := false
	client.exp.mu.Lock()
	client.exp.consentRaceSeam = func(stage string) {
		if stage == "exposure_enqueued" && !raced {
			raced = true
			client.SetConsent(false)
			client.SetConsent(true)
		}
	}
	client.exp.mu.Unlock()
	if err := client.TrackExperimentExposure(expTestScopeKey); err != nil {
		t.Fatalf("TrackExperimentExposure: %v", err)
	}
	client.experimentCycle(context.Background())
	if !raced {
		t.Fatal("control: the seam must have fired")
	}
	client.experimentCycle(context.Background())
	flushOrFail(t, client)
	ids := expSealedIDs(script)
	if len(ids) < 2 || ids[1] != ids[0] {
		t.Fatalf("the re-armed application must be re-sent with the same identity, got %v", ids)
	}
	seen := 0
	for _, fact := range capture.exposures() {
		if fact["event_id"] == ids[0] {
			seen++
		}
	}
	if seen != 1 {
		t.Fatalf("the application's only exposure was lost to the raced purge (delivered %d with its id, %d facts in all)", seen, len(capture.exposures()))
	}
}
