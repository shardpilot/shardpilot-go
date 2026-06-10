package uuidv7

import (
	"strings"
	"testing"
	"time"
)

func TestNewProducesValidDistinctIDs(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id, err := New()
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if !IsValid(id) {
			t.Fatalf("generated id is not a valid UUIDv7: %q", id)
		}
		if seen[id] {
			t.Fatalf("generated duplicate id %q", id)
		}
		seen[id] = true
	}
}

func TestNewAtEncodesTimestampPrefix(t *testing.T) {
	at := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	first, err := NewAt(at)
	if err != nil {
		t.Fatalf("NewAt: %v", err)
	}
	second, err := NewAt(at)
	if err != nil {
		t.Fatalf("NewAt: %v", err)
	}
	// The first 48 bits (12 hex chars across the first two groups) encode
	// the millisecond timestamp and must match for an identical time.
	prefix := func(id string) string { return strings.ReplaceAll(id, "-", "")[:12] }
	if prefix(first) != prefix(second) {
		t.Fatalf("expected identical timestamp prefixes, got %q and %q", first, second)
	}
}

func TestIsValidRejectsMalformedValues(t *testing.T) {
	for _, candidate := range []string{
		"",
		"not-a-uuid",
		"00000000-0000-4000-8000-000000000000", // version 4, not 7
		"00000000-0000-7000-c000-000000000000", // bad variant
		"00000000000070008000000000000000",     // missing dashes
	} {
		if IsValid(candidate) {
			t.Fatalf("expected %q to be rejected", candidate)
		}
	}
}
