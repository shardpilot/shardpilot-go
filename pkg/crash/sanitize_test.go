package crash

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSanitizeEventStripsDisallowedOptionalFields(t *testing.T) {
	event := validEvent(t)
	event.AppVersion = "sample@example.invalid"
	event.BuildID = "header.eyJzdWIiOiJ0ZXN0In0.signature"
	event.OS.Name = "desktop 198.51.100.23"
	event.OS.Version = "2001:db8::1"
	event.StackFrames = []Frame{{
		Function: "player_raw_identifier",
		File:     "safe/file.go",
		Line:     -12,
		Module:   "synthetic-module",
	}}
	event.Breadcrumbs = []Breadcrumb{
		{Name: "screen_open", Timestamp: time.Unix(1700000003, 0)},
		{Name: "sample@example.invalid", Timestamp: time.Unix(1700000004, 0).UTC()},
		{Name: "device_raw_identifier", Timestamp: time.Unix(1700000005, 0).UTC()},
		{Name: "{\"payload\":true}", Timestamp: time.Unix(1700000006, 0).UTC()},
	}

	sanitized, err := SanitizeEvent(event)
	if err != nil {
		t.Fatalf("SanitizeEvent returned error: %v", err)
	}

	if sanitized.AppVersion != "" {
		t.Fatalf("expected email-like app version to be stripped, got %q", sanitized.AppVersion)
	}
	if sanitized.BuildID != "" {
		t.Fatalf("expected JWT-like build id to be stripped, got %q", sanitized.BuildID)
	}
	if sanitized.OS.Name != "" {
		t.Fatalf("expected IPv4-bearing OS name to be stripped, got %q", sanitized.OS.Name)
	}
	if sanitized.OS.Version != "" {
		t.Fatalf("expected IPv6-bearing OS version to be stripped, got %q", sanitized.OS.Version)
	}
	if sanitized.StackFrames[0].Function != "" {
		t.Fatalf("expected raw identifier prefix to be stripped, got %q", sanitized.StackFrames[0].Function)
	}
	if sanitized.StackFrames[0].File != "safe/file.go" {
		t.Fatalf("expected safe frame file to remain, got %q", sanitized.StackFrames[0].File)
	}
	if sanitized.StackFrames[0].Line != 0 {
		t.Fatalf("expected negative frame line to be clamped to 0, got %d", sanitized.StackFrames[0].Line)
	}
	if len(sanitized.Breadcrumbs) != 1 || sanitized.Breadcrumbs[0].Name != "screen_open" {
		t.Fatalf("expected only safe breadcrumb to remain, got %#v", sanitized.Breadcrumbs)
	}
	if sanitized.Breadcrumbs[0].Timestamp.Location() != time.UTC {
		t.Fatalf("expected breadcrumb timestamp to be normalized to UTC")
	}
	assertEventHasNoDisallowedStrings(t, sanitized)
}

func TestSanitizeEventRejectsUnsafeSessionID(t *testing.T) {
	for _, sessionID := range []string{
		"player_session_hash",
		"user_session_hash",
		"customer_session_hash",
		"device_session_hash",
		"sample@example.invalid",
		"198.51.100.24",
		"2001:db8::2",
		"header.eyJzdWIiOiJ0ZXN0In0.signature",
	} {
		event := validEvent(t)
		event.SessionID = sessionID
		if _, err := SanitizeEvent(event); !errors.Is(err, ErrInvalidEvent) {
			t.Fatalf("expected ErrInvalidEvent for session %q, got %v", sessionID, err)
		}
	}
}

func TestSanitizeBreadcrumbNameShape(t *testing.T) {
	valid := []string{"screen_open", "match.round-start", "Shop:Purchase", "level_2"}
	for _, name := range valid {
		got, ok := sanitizeBreadcrumbName(name)
		if !ok || got != name {
			t.Fatalf("expected breadcrumb %q to be accepted, got %q ok=%v", name, got, ok)
		}
	}

	invalid := []string{
		"",
		" screen open ",
		"1_screen_open",
		"screen open",
		"screen_open=1",
		"screen_open@sample.invalid",
		"user_signup",
		"event_198.51.100.25",
		"event_2001:db8::3",
		"header.eyJzdWIiOiJ0ZXN0In0.signature",
	}
	for _, name := range invalid {
		if got, ok := sanitizeBreadcrumbName(name); ok {
			t.Fatalf("expected breadcrumb %q to be rejected, got %q", name, got)
		}
	}
}

func TestSanitizeEventCapsBreadcrumbs(t *testing.T) {
	event := validEvent(t)
	event.Breadcrumbs = make([]Breadcrumb, maxBreadcrumbs+10)
	for i := range event.Breadcrumbs {
		event.Breadcrumbs[i] = Breadcrumb{Name: "screen_open", Timestamp: time.Unix(int64(1700000000+i), 0).UTC()}
	}

	sanitized, err := SanitizeEvent(event)
	if err != nil {
		t.Fatalf("SanitizeEvent returned error: %v", err)
	}
	if len(sanitized.Breadcrumbs) != maxBreadcrumbs {
		t.Fatalf("expected %d breadcrumbs, got %d", maxBreadcrumbs, len(sanitized.Breadcrumbs))
	}
	if got := sanitized.Breadcrumbs[0].Timestamp.Unix(); got != 1700000010 {
		t.Fatalf("expected oldest retained breadcrumb to be 1700000010, got %d", got)
	}
}

func assertEventHasNoDisallowedStrings(t *testing.T, event Event) {
	t.Helper()
	values := []string{
		event.AppVersion,
		event.BuildID,
		event.OS.Name,
		event.OS.Version,
		event.DeviceClass,
		event.ThreadState,
		event.SessionID,
	}
	for _, frame := range event.StackFrames {
		values = append(values, frame.Function, frame.File, frame.Module)
	}
	for _, breadcrumb := range event.Breadcrumbs {
		values = append(values, breadcrumb.Name)
	}
	for _, value := range values {
		if containsDisallowedContent(value) {
			t.Fatalf("found disallowed content in sanitized string %q", value)
		}
		if strings.Contains(value, "{") || strings.Contains(value, "}") {
			t.Fatalf("found payload-shaped string in sanitized event: %q", value)
		}
	}
}
