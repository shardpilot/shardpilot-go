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
	event.Threads[0].Frames = make([]Frame, maxStackFrames+1)
	for i := range event.Threads[0].Frames {
		event.Threads[0].Frames[i] = Frame{ModuleID: "synthetic", InstructionAddress: "0x401015"}
	}

	if err := validateEvent(event); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected ErrInvalidEvent for too many stack frames, got %v", err)
	}
}

func TestNormalizeEventTimesUsesUTC(t *testing.T) {
	local := time.FixedZone("synthetic-zone", 3*60*60)
	event := validEvent(t)
	event.OccurredAt = time.Date(2026, 5, 24, 12, 0, 0, 0, local)
	event.Breadcrumbs = []Breadcrumb{{Name: "screen_open", Timestamp: time.Date(2026, 5, 24, 12, 0, 1, 0, local)}}

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

func TestNormalizeEventShapeDefaultsCrashedThread(t *testing.T) {
	event := validEvent(t)
	event.Exception.CrashedThreadID = ""
	event.Threads[0].Crashed = false
	normalized := normalizeEventShape(event)
	if normalized.Exception.CrashedThreadID != "main" || !normalized.Threads[0].Crashed {
		t.Fatalf("crashed thread not defaulted: %+v", normalized)
	}
}

func TestValidateEventEnums(t *testing.T) {
	event := validEvent(t)
	event.Device["class"] = "watch"
	if err := validateEvent(event); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected invalid device class error, got %v", err)
	}
}

func validEvent(t *testing.T) Event {
	t.Helper()
	id, err := newCrashIDAt(time.Unix(1700000000, 0).UTC())
	if err != nil {
		t.Fatalf("newCrashIDAt: %v", err)
	}
	return Event{
		CrashID:    id,
		OccurredAt: time.Unix(1700000002, 0).UTC(),
		App:        AppInfo{ID: "app_test_001", Version: "0.2.0-alpha-test", BuildID: "build-test"},
		Platform:   "linux",
		OS:         OSInfo{Name: "linux", Version: "test"},
		Device:     map[string]string{"class": DeviceClassDesktop, "arch": "x86_64"},
		Context:    map[string]string{"session_id": "sha256-session-hash-test"},
		Exception:  ExceptionInfo{Type: "SIGSEGV", Reason: "synthetic fault", CrashedThreadID: "main"},
		Modules: []Module{{
			ID:          "synthetic",
			Name:        "synthetic-module",
			DebugID:     "AABBCCDDEEFF00112233445566778899",
			LoadAddress: "0x400000",
		}},
		Threads: []Thread{{
			ID:      "main",
			Crashed: true,
			Frames: []Frame{{
				ModuleID:           "synthetic",
				InstructionAddress: "0x401015",
				Function:           "main.run",
				File:               "main.go",
				Line:               42,
			}},
		}},
		Breadcrumbs: []Breadcrumb{{Name: "screen_open", Timestamp: time.Unix(1700000001, 0).UTC()}},
	}
}

func TestValidateEventPreSymbolicatedFrameNeedsNoModule(t *testing.T) {
	e := validEvent(t)
	e.Modules = nil // pre-symbolicated Go crash: zero native modules
	e.Threads[0].Frames = []Frame{{Function: "main.run", File: "main.go", Line: 10}}
	if err := validateEvent(e); err != nil {
		t.Fatalf("pre-symbolicated function-only frame with 0 modules must validate, got %v", err)
	}
}

func TestValidateEventNativeAddressFrameRequiresModule(t *testing.T) {
	e := validEvent(t)
	e.Modules = nil // an address with no module map is unresolvable
	e.Threads[0].Frames = []Frame{{InstructionAddress: "0x401015"}}
	if err := validateEvent(e); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("addressed frame with 0 modules must be rejected, got %v", err)
	}
}

func TestValidateEventAddressedFrameMultiModuleRequiresModuleID(t *testing.T) {
	e := validEvent(t)
	e.Modules = []Module{
		{Name: "a", DebugID: "AABBCCDDEEFF00112233445566778899", LoadAddress: "0x1000"},
		{Name: "b", DebugID: "99887766554433221100FFEEDDCCBBAA", LoadAddress: "0x2000"},
	}
	// a frame with a function AND an address, multi-module, no module_id => reject
	// (the address still needs a module to disambiguate, despite the function).
	e.Threads[0].Frames = []Frame{{Function: "main.run", InstructionAddress: "0x1010"}}
	if err := validateEvent(e); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("addressed multi-module frame without module_id must be rejected, got %v", err)
	}
	// With a module_id it validates.
	e.Threads[0].Frames[0].ModuleID = "a"
	if err := validateEvent(e); err != nil {
		t.Fatalf("addressed frame with module_id must validate, got %v", err)
	}
}
