package shardpilot

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// fetchAdultAssignment is the host's fetch with an adult age declaration:
// the declaration a client-id assignment needs to be admitted, and to be
// restored after a restart.
func fetchAdultAssignment(t *testing.T, c *Client, key string) ExperimentAssignmentResult {
	t.Helper()
	result, err := c.FetchExperimentAssignmentWithAgeBand(context.Background(), key, ExperimentAgeBandAdult, nil)
	if err != nil {
		t.Fatalf("FetchExperimentAssignmentWithAgeBand(%q): %v", key, err)
	}
	return result
}

// A client-id assignment stored without an age declaration, which is every
// entry a release without age declarations persisted, is not served after a
// restart: client-id admission requires a declaration, so the experiment
// waits for the next fetch, which carries the host's current one. An entry
// declared adult, and a synthetic-subject entry (no age gate), restore as
// before.
func TestARestoredClientIDEntryWithoutAnAgeDeclarationIsNotServed(t *testing.T) {
	const (
		undeclared = expTestScopeKey
		declared   = "exp-declared-adult"
		synthetic  = "exp-synthetic-subject"
	)
	script := &expScript{}
	script.push(200, expAssignedBody("1"))
	capture := &expWireCapture{}
	server := newExperimentServer(t, script, capture)
	defer server.Close()
	spoolDir := t.TempDir()

	client1 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	fetchAssignment(t, client1, undeclared)
	if err := client1.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	// The record as a release without age declarations wrote it, with two
	// controls beside it.
	recordPath := filepath.Join(spoolDir, expCacheFileName)
	data, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var record expDurableRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode: %v", err)
	}
	stored := record.Entries[undeclared]
	stored.Attributes = nil
	record.Entries[undeclared] = stored
	adult := stored
	adult.Attributes = []expAttribute{{Name: "age_band", Value: "adult"}}
	record.Entries[declared] = adult
	subject := stored
	subject.AssignmentUnit = experimentAssignmentUnitSynthetic
	record.Entries[synthetic] = subject
	raw, _ := json.Marshal(record)
	if err := os.WriteFile(recordPath, raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	client2 := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = spoolDir })
	defer client2.Close(context.Background())
	if v := client2.ExperimentVariant(undeclared); v != "" {
		t.Fatalf("an undeclared client-id assignment must not be served after a restart, got %q", v)
	}
	if v := client2.ExperimentVariant(declared); v != "treatment" {
		t.Fatalf("control: an assignment declared adult must restore, got %q", v)
	}
	if v := client2.ExperimentVariant(synthetic); v != "treatment" {
		t.Fatalf("control: a synthetic-subject assignment has no age gate and must restore, got %q", v)
	}
	if script.requestCount() != 1 {
		t.Fatalf("the restore must not fetch, got %d requests", script.requestCount())
	}

	// With nothing served, an application records nothing.
	client2.ApplyExperimentVariant(undeclared)
	client2.experimentCycle(context.Background())
	if err := client2.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := len(capture.exposures()); got != 0 {
		t.Fatalf("an undeclared restored assignment must record no exposure, got %d", got)
	}

	// The next fetch carries the host's current declaration and decides.
	script.push(200, expAssignedBody("1"))
	if result := fetchAdultAssignment(t, client2, undeclared); !result.Assigned || result.FromCache {
		t.Fatalf("the declared fetch must be decided by the server, got %+v", result)
	}
	if got := script.request(1).URL.Query().Get("age_band"); got != "adult" {
		t.Fatalf("the next fetch must carry the current declaration, got age_band=%q", got)
	}
	if v := client2.ExperimentVariant(undeclared); v != "treatment" {
		t.Fatalf("the declared fetch's assignment must serve, got %q", v)
	}
}
