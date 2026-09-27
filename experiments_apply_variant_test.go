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
	if got := client.ApplyExperimentVariant(expTestScopeKey); got != "treatment" {
		t.Fatalf("ApplyExperimentVariant must return the served variant, got %q", got)
	}
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

	if got := client.ApplyExperimentVariant(expTestScopeKey); got != "" {
		t.Fatalf("nothing cached: want \"\", got %q", got)
	}
	fetchAssignment(t, client, expTestScopeKey)
	if got := client.ApplyExperimentVariant("  "); got != "" {
		t.Fatalf("an empty key: want \"\", got %q", got)
	}
	client.SetConsent(false)
	if got := client.ApplyExperimentVariant(expTestScopeKey); got != "" || client.ExperimentVariant(expTestScopeKey) != "" {
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
	if got := dark.ApplyExperimentVariant(expTestScopeKey); got != "" {
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
// variant — what a fetch alone recorded before ApplyExperimentVariant.
func fetchAndApply(t *testing.T, c *Client, key string) ExperimentAssignmentResult {
	t.Helper()
	result := fetchAssignment(t, c, key)
	c.ApplyExperimentVariant(key)
	return result
}
