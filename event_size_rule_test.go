package shardpilot

import (
	"strings"
	"testing"
)

// TestEventSizeRuleMatchesIngest pins the ingest predicate: the element's
// byte length with surrounding whitespace trimmed, over 2048, except the
// retired experiment major (an experiment exposure or outcome at schema
// version 1, name trimmed).
func TestEventSizeRuleMatchesIngest(t *testing.T) {
	element := func(size int) []byte { return []byte(`"` + strings.Repeat("a", size-2) + `"`) }
	padded := func(size int) []byte { return []byte(" \n\t" + string(element(size)) + "\r\n ") }
	for _, tc := range []struct {
		name    string
		raw     []byte
		event   string
		version int
		over    bool
	}{
		{"2048 bytes", element(2048), "host_event", 1, false},
		{"2049 bytes", element(2049), "host_event", 1, true},
		{"2048 bytes inside whitespace", padded(2048), "host_event", 1, false},
		{"2049 bytes inside whitespace", padded(2049), "host_event", 1, true},
		{"retired exposure", element(4096), experimentExposureName, 1, false},
		{"retired outcome, padded name", element(4096), " " + experimentOutcomeName + " ", 1, false},
		{"current exposure", element(4096), experimentExposureName, 2, true},
		{"current outcome", element(4096), experimentOutcomeName, 2, true},
		{"other event at version 1", element(4096), "experiment_assignment", 1, true},
	} {
		if got := eventExceedsSizeLimit(tc.raw, tc.event, tc.version); got != tc.over {
			t.Errorf("%s: over = %t, want %t", tc.name, got, tc.over)
		}
	}
}
