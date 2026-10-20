package shardpilot

import (
	"strings"
	"testing"
)

// TestEventSizeRuleMatchesIngest pins the ingest predicate: the element's
// byte length with surrounding whitespace trimmed, over 2048.
func TestEventSizeRuleMatchesIngest(t *testing.T) {
	element := func(size int) []byte { return []byte(`"` + strings.Repeat("a", size-2) + `"`) }
	padded := func(size int) []byte { return []byte(" \n\t" + string(element(size)) + "\r\n ") }
	for _, tc := range []struct {
		name string
		raw  []byte
		over bool
	}{
		{"2048 bytes", element(2048), false},
		{"2049 bytes", element(2049), true},
		{"2048 bytes inside whitespace", padded(2048), false},
		{"2049 bytes inside whitespace", padded(2049), true},
		{"4096 bytes", element(4096), true},
	} {
		if got := eventExceedsSizeLimit(tc.raw); got != tc.over {
			t.Errorf("%s: over = %t, want %t", tc.name, got, tc.over)
		}
	}
}
