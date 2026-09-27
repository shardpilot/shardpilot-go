package shardpilot

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// The serving state an assignment was served from — served_revision,
// served_kill_gate and served_at — is pinned with the cached assignment, so
// that an application of it can later be recorded as of that state. The
// trio keys no serving decision: a missing or ill-typed trio leaves the
// assignment served and unpinned, never refused.

const expServedTrio = `"served_revision":7,"served_kill_gate":true,"served_at":"2026-09-27T03:00:00.123456789+02:00",`

func expAssignedServedBody(trio string) string {
	return strings.Replace(expAssignedBodyUnserved("3"), `"assigned":true,`, `"assigned":true,`+trio, 1)
}

func TestParseExperimentVerdictPinsTheServedState(t *testing.T) {
	result, outcome, ok := parseExperimentVerdict(expTestResponse(200, expAssignedServedBody(expServedTrio)), expTestRequestScope(), 42)
	if !ok || !result.Assigned || outcome.newEntry == nil {
		t.Fatalf("an assigned body carrying the served state must install, got ok=%v result=%+v", ok, result)
	}
	want := expServedState{Revision: 7, KillGate: true, At: "2026-09-27T01:00:00.123456789Z"}
	if outcome.newEntry.Served == nil || *outcome.newEntry.Served != want {
		t.Fatalf("pinned served state = %+v, want %+v (served_at normalized to UTC)", outcome.newEntry.Served, want)
	}
	zero := strings.Replace(expServedTrio, `"served_revision":7,"served_kill_gate":true`, `"served_revision":0,"served_kill_gate":false`, 1)
	_, outcome, ok = parseExperimentVerdict(expTestResponse(200, expAssignedServedBody(zero)), expTestRequestScope(), 42)
	if !ok || outcome.newEntry.Served == nil || outcome.newEntry.Served.Revision != 0 || outcome.newEntry.Served.KillGate {
		t.Fatalf("revision 0 with the kill gate off is a valid served state, got %+v", outcome.newEntry.Served)
	}
}

func TestParseExperimentVerdictLeavesAnIncompleteServedStateUnpinned(t *testing.T) {
	for name, trio := range map[string]string{
		"absent (a server without the served state)": ``,
		"revision only":        `"served_revision":7,`,
		"no served_at":         `"served_revision":7,"served_kill_gate":true,`,
		"revision a string":    strings.Replace(expServedTrio, `"served_revision":7`, `"served_revision":"7"`, 1),
		"revision negative":    strings.Replace(expServedTrio, `"served_revision":7`, `"served_revision":-1`, 1),
		"revision fractional":  strings.Replace(expServedTrio, `"served_revision":7`, `"served_revision":7.5`, 1),
		"revision null":        strings.Replace(expServedTrio, `"served_revision":7`, `"served_revision":null`, 1),
		"kill gate a string":   strings.Replace(expServedTrio, `"served_kill_gate":true`, `"served_kill_gate":"true"`, 1),
		"kill gate null":       strings.Replace(expServedTrio, `"served_kill_gate":true`, `"served_kill_gate":null`, 1),
		"served_at not a time": strings.Replace(expServedTrio, `"2026-09-27T03:00:00.123456789+02:00"`, `"yesterday"`, 1),
		"served_at a number":   strings.Replace(expServedTrio, `"2026-09-27T03:00:00.123456789+02:00"`, `1790000000`, 1),
	} {
		result, outcome, ok := parseExperimentVerdict(expTestResponse(200, expAssignedServedBody(trio)), expTestRequestScope(), 42)
		if !ok || !result.Assigned || outcome.newEntry == nil || outcome.newEntry.VariantKey != "treatment" {
			t.Fatalf("%s: the assignment must still install and serve, got ok=%v result=%+v", name, ok, result)
		}
		if outcome.newEntry.Served != nil {
			t.Fatalf("%s: an incomplete served state must leave the assignment unpinned, got %+v", name, outcome.newEntry.Served)
		}
	}
}

// A stored pin is re-validated on load like every other stored field: a
// corrupt one is dropped and the assignment keeps serving, unpinned.
func TestSanitizedEntriesKeepOnlyAValidServedState(t *testing.T) {
	_, outcome, ok := parseExperimentVerdict(expTestResponse(200, expAssignedBody("3")), expTestRequestScope(), 42)
	if !ok {
		t.Fatal("control: the assigned body must parse")
	}
	valid := *outcome.newEntry
	pin := expServedState{Revision: 7, KillGate: true, At: "2026-09-27T01:00:00.123456789Z"}
	valid.Served = &pin
	stored, err := json.Marshal(expDurableRecord{Scope: "scope", Entries: map[string]expEntry{"valid": valid}})
	if err != nil {
		t.Fatal(err)
	}
	var record expDurableRecord
	if err := json.Unmarshal(stored, &record); err != nil {
		t.Fatal(err)
	}
	corruptRevision, corruptAt := record.Entries["valid"], record.Entries["valid"]
	corruptRevision.Served = &expServedState{Revision: -3, At: pin.At}
	corruptAt.Served = &expServedState{Revision: 7, At: "not-a-time"}
	record.Entries["negative revision"], record.Entries["unparsable served_at"] = corruptRevision, corruptAt
	sanitized := sanitizeExperimentEntries(record.Entries)
	if got := sanitized["valid"].Served; got == nil || *got != pin {
		t.Fatalf("a valid pin must survive the durable round trip, got %+v", got)
	}
	for _, name := range []string{"negative revision", "unparsable served_at"} {
		entry, kept := sanitized[name]
		if !kept || entry.VariantKey != "treatment" || entry.Served != nil {
			t.Fatalf("%s: the entry must stay, served and unpinned; kept=%v entry=%+v", name, kept, entry)
		}
	}
}

// End to end: a fetch pins the served state, and a restart restores it from
// the durable cache.
func TestAFetchedServedStateSurvivesARestart(t *testing.T) {
	script := &expScript{}
	script.push(200, expAssignedServedBody(expServedTrio))
	server := newExperimentServer(t, script, &expWireCapture{})
	defer server.Close()
	dir := t.TempDir()
	first := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = dir })
	fetchAssignment(t, first, expTestScopeKey)
	want := expServedState{Revision: 7, KillGate: true, At: "2026-09-27T01:00:00.123456789Z"}
	first.exp.mu.Lock()
	pinned := first.exp.entries[expTestScopeKey]
	first.exp.mu.Unlock()
	if pinned == nil || pinned.Served == nil || *pinned.Served != want {
		t.Fatalf("the installed entry must carry the served state, got %+v", pinned)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	second := newExperimentClient(t, server.URL, func(cfg *Config) { cfg.SpoolDir = dir })
	defer second.Close(context.Background())
	if second.ExperimentVariant(expTestScopeKey) != "treatment" {
		t.Fatal("control: the restarted client must serve the cached assignment")
	}
	second.exp.mu.Lock()
	restored := second.exp.entries[expTestScopeKey]
	second.exp.mu.Unlock()
	if restored == nil || restored.Served == nil || *restored.Served != want {
		t.Fatalf("the restored entry must carry the same served state, got %+v", restored)
	}
}
