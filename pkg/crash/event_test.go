package crash

import (
	"errors"
	"testing"
	"time"
)

func TestUUIDv7Validation(t *testing.T) {
	id, err := newCrashIDAt(time.Unix(1700000000, 0).UTC())
	if err != nil {
		t.Fatalf("newCrashIDAt: %v", err)
	}
	if !isUUIDv7(id) {
		t.Fatalf("generated id is not UUIDv7: %s", id)
	}

	invalid := []string{
		"",
		"not-a-uuid",
		"018bcfe5-5680-4cc8-a7b8-7f6b0a5969de",
		"018bcfe5-5680-7cc8-27b8-7f6b0a5969de",
		"018bcfe5-5680-7cc8-c7b8-7f6b0a5969de",
		"018bcfe5-5680-7cc8-a7b8-7f6b0a5969dz",
	}
	for _, candidate := range invalid {
		if isUUIDv7(candidate) {
			t.Fatalf("expected invalid UUIDv7 candidate %q to fail validation", candidate)
		}
	}
}

func TestValidateEventCapsStackFrames(t *testing.T) {
	event := validEvent(t)
	event.StackFrames = make([]Frame, maxStackFrames+1)

	if err := validateEvent(event); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent for too many stack frames, got %v", err)
	}
}

func TestNormalizeEventTimesUsesUTC(t *testing.T) {
	id, err := newCrashIDAt(time.Unix(1700000000, 0).UTC())
	if err != nil {
		t.Fatalf("newCrashIDAt: %v", err)
	}
	local := time.FixedZone("synthetic-zone", 3*60*60)
	event := Event{
		CrashID:     id,
		DeviceClass: DeviceClassDesktop,
		ThreadState: ThreadStateMain,
		OccurredAt:  time.Date(2026, 5, 24, 12, 0, 0, 0, local),
		Breadcrumbs: []Breadcrumb{{Name: "screen_open", Timestamp: time.Date(2026, 5, 24, 12, 0, 1, 0, local)}},
	}

	normalized := normalizeEventTimes(event, time.Unix(1700000000, 0).UTC())
	if normalized.OccurredAt.Location() != time.UTC {
		t.Fatalf("expected occurred_at location UTC, got %v", normalized.OccurredAt.Location())
	}
	if normalized.Breadcrumbs[0].Timestamp.Location() != time.UTC {
		t.Fatalf("expected breadcrumb timestamp location UTC, got %v", normalized.Breadcrumbs[0].Timestamp.Location())
	}
	if err := validateEvent(normalized); err != nil {
		t.Fatalf("normalized event should validate: %v", err)
	}
}

func TestValidateEventEnums(t *testing.T) {
	event := validEvent(t)
	event.DeviceClass = "watch"
	if err := validateEvent(event); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected invalid device class error, got %v", err)
	}

	event = validEvent(t)
	event.ThreadState = "worker"
	if err := validateEvent(event); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected invalid thread state error, got %v", err)
	}
}

func validEvent(t *testing.T) Event {
	t.Helper()
	id, err := newCrashIDAt(time.Unix(1700000000, 0).UTC())
	if err != nil {
		t.Fatalf("newCrashIDAt: %v", err)
	}
	return Event{
		CrashID:     id,
		AppVersion:  "0.2.0-alpha-test",
		BuildID:     "build-test",
		OS:          OSInfo{Name: "linux", Version: "test"},
		DeviceClass: DeviceClassDesktop,
		StackFrames: []Frame{{Function: "main.run", File: "main.go", Line: 42, Module: "synthetic-module"}},
		Breadcrumbs: []Breadcrumb{{Name: "screen_open", Timestamp: time.Unix(1700000001, 0).UTC()}},
		ThreadState: ThreadStateMain,
		SessionID:   "sha256-session-hash-test",
		OccurredAt:  time.Unix(1700000002, 0).UTC(),
	}
}
