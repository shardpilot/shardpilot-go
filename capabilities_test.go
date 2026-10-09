package shardpilot_test

import (
	"strings"
	"testing"

	shardpilot "github.com/shardpilot/shardpilot-go"
)

func TestSupportsBeforeInitialization(t *testing.T) {
	keys := []string{"consent_receipt_outbox", "consent_state_denied_forced_minor", "schema_revision_declaration", "experiments_assignment", "experiments_age_band"}
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			if !shardpilot.Supports(key) {
				t.Fatalf("shipped capability %q is missing", key)
			}
			for _, unknown := range []string{strings.ToUpper(key), " " + key, key + " ", key + "_future"} {
				if shardpilot.Supports(unknown) {
					t.Fatalf("unsupported spelling %q is advertised", unknown)
				}
			}
		})
	}
	for _, key := range []string{"", "unknown", "*", "schema_revision", "experiments", "crash_reporting"} {
		if shardpilot.Supports(key) {
			t.Errorf("unknown capability %q is advertised", key)
		}
	}
}
