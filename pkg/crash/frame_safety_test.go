package crash

import (
	"strings"
	"testing"
	"time"
)

func fatalSafetyEvent(frames []Frame, threadID string) Event {
	return Event{
		CrashID:    "01890000-0000-7000-8000-000000000000",
		OccurredAt: time.Now().UTC(),
		Platform:   "linux",
		App:        AppInfo{ID: "fortress-fury"},
		Exception:  ExceptionInfo{Type: "panic"},
		Threads:    []Thread{{ID: threadID, Crashed: true, Frames: frames}},
	}
}

// A 4-deep nested closure renders as foo.func1.1.1.1; the "1.1.1.1" tail matches the IPv4
// scrub pattern. Blanking the whole symbol made the address-less auto-capture frame
// identity-less and dropped the WHOLE fatal crash. The function must now be redacted in
// place (frame kept, file:line intact) so the crash survives.
func TestNestedClosureFrameDoesNotDropFatalCrash(t *testing.T) {
	ev := fatalSafetyEvent([]Frame{
		{Index: 0, Function: "main.handler.func1.1.1.1", File: "handler/h.go", Line: 42},
		{Index: 1, Function: "main.main", File: "main.go", Line: 9},
	}, "1")

	sanitized, err := sanitizeEvent(ev, true) // auto-capture path: trusted runtime symbols
	if err != nil {
		t.Fatalf("sanitizeEvent: %v", err)
	}
	if err := validateEvent(sanitized); err != nil {
		t.Fatalf("nested-closure crash must not be dropped: %v", err)
	}
	frames := sanitized.Threads[0].Frames
	if len(frames) != 2 {
		t.Fatalf("want both frames kept, got %d: %+v", len(frames), frames)
	}
	if frames[0].Function == "" {
		t.Error("origin frame function blanked; want redacted-in-place non-empty")
	}
	if strings.Contains(frames[0].Function, "1.1.1.1") {
		t.Errorf("origin frame still carries the IPv4-shaped run: %q", frames[0].Function)
	}
	if frames[0].File != "handler/h.go" || frames[0].Line != 42 {
		t.Errorf("origin frame lost file:line: %q:%d", frames[0].File, frames[0].Line)
	}
}

// On the manual path a frame whose function is pure PII is blanked by the full scrubber.
// That single identity-less frame must be DROPPED, not used to reject the whole crash.
func TestManualPIIFrameDroppedNotWholeCrash(t *testing.T) {
	ev := fatalSafetyEvent([]Frame{
		{Index: 0, Function: "alice@example.com"},
		{Index: 1, Function: "app.run", File: "main.go", Line: 3},
	}, "1")

	sanitized, err := sanitizeEvent(ev, false) // manual path: full content scrub
	if err != nil {
		t.Fatalf("sanitizeEvent: %v", err)
	}
	if err := validateEvent(sanitized); err != nil {
		t.Fatalf("crash dropped over one PII frame: %v", err)
	}
	frames := sanitized.Threads[0].Frames
	if len(frames) != 1 || frames[0].Function != "app.run" {
		t.Errorf("want only the clean frame kept, got %+v", frames)
	}
}

// A real embedded IP in a frame function is stripped before the wire even though the frame
// is preserved (defense: redact-in-place must not leak PII).
func TestSymbolRedactionDoesNotLeakIP(t *testing.T) {
	ev := fatalSafetyEvent([]Frame{
		{Index: 0, Function: "net.dial.198.51.100.23.retry", File: "net/d.go", Line: 7},
	}, "1")

	sanitized, err := sanitizeEvent(ev, true)
	if err != nil {
		t.Fatalf("sanitizeEvent: %v", err)
	}
	if err := validateEvent(sanitized); err != nil {
		t.Fatalf("crash dropped: %v", err)
	}
	got := sanitized.Threads[0].Frames[0].Function
	if strings.Contains(got, "198.51.100.23") {
		t.Errorf("frame function leaked an IP address: %q", got)
	}
	if got == "" {
		t.Error("frame function fully blanked; want redacted-in-place non-empty")
	}
}

// A symbol carrying a dot-bounded IPv6 (foo.2001:db8::1.bar) is not a real Go symbol: it is
// blanked and its frame dropped, but the crash survives and never carries the address.
func TestSymbolWithIPv6BlankedCrashKept(t *testing.T) {
	ev := fatalSafetyEvent([]Frame{
		{Index: 0, Function: "pkg.handler.2001:db8::1.tail", File: "p/h.go", Line: 5},
		{Index: 1, Function: "main.main", File: "main.go", Line: 9},
	}, "1")
	sanitized, err := sanitizeEvent(ev, true)
	if err != nil {
		t.Fatalf("sanitizeEvent: %v", err)
	}
	if err := validateEvent(sanitized); err != nil {
		t.Fatalf("crash dropped over an IPv6-bearing frame: %v", err)
	}
	for _, f := range sanitized.Threads[0].Frames {
		if strings.Contains(f.Function, "2001:db8::1") {
			t.Errorf("frame leaked a dot-bounded IPv6: %q", f.Function)
		}
	}
	if len(sanitized.Threads[0].Frames) != 1 || sanitized.Threads[0].Frames[0].Function != "main.main" {
		t.Errorf("want only the clean frame kept, got %+v", sanitized.Threads[0].Frames)
	}
}

// Two IPv4 addresses sharing a single separator must both be redacted, not just the first.
func TestSymbolRedactionDoesNotLeakAdjacentIPv4(t *testing.T) {
	ev := fatalSafetyEvent([]Frame{
		{Index: 0, Function: "pkg.handler.8.8.8.8 9.9.9.9", File: "p/h.go", Line: 6},
	}, "1")
	sanitized, err := sanitizeEvent(ev, true)
	if err != nil {
		t.Fatalf("sanitizeEvent: %v", err)
	}
	got := sanitized.Threads[0].Frames[0].Function
	for _, ip := range []string{"8.8.8.8", "9.9.9.9"} {
		if strings.Contains(got, ip) {
			t.Errorf("frame function leaked adjacent IPv4 %q: %q", ip, got)
		}
	}
}

// A symbol carrying an email is not a real Go symbol: it is blanked (no partial local-part
// leaks, even with a dotted local part like alice.smith) and its frame is dropped, while the
// crash survives via its other frames.
func TestSymbolWithEmailBlankedCrashKept(t *testing.T) {
	ev := fatalSafetyEvent([]Frame{
		{Index: 0, Function: "pkg.handler.alice.smith@example.com.retry", File: "p/h.go", Line: 5},
		{Index: 1, Function: "main.main", File: "main.go", Line: 9},
	}, "1")
	sanitized, err := sanitizeEvent(ev, true)
	if err != nil {
		t.Fatalf("sanitizeEvent: %v", err)
	}
	if err := validateEvent(sanitized); err != nil {
		t.Fatalf("crash dropped over an email-bearing frame: %v", err)
	}
	for _, f := range sanitized.Threads[0].Frames {
		for _, leak := range []string{"@", "alice", "smith", "example.com"} {
			if strings.Contains(f.Function, leak) {
				t.Errorf("frame leaked email material %q: %q", leak, f.Function)
			}
		}
	}
	// No partial leak: a dotted local part must not leave "alice." behind.
	if got := sanitizeSymbol("pkg.handler.alice.smith@example.com.retry"); got != "" {
		t.Errorf("email-shaped symbol must blank, got %q", got)
	}
}

// An IPv4-mapped IPv6 literal makes the symbol non-real; blanking it keeps neither the IPv6
// prefix nor the IPv4 tail on the wire.
func TestIPv4MappedIPv6SymbolBlanked(t *testing.T) {
	if got := sanitizeSymbol("net.dial.2001:db8:85a3::8a2e:370:192.0.2.128.retry"); got != "" {
		t.Errorf("IPv4-mapped IPv6 symbol must blank, got %q", got)
	}
}

// The detector must catch an IPv6 in any field (free text), not only the redactor — including
// when the address abuts a dotted neighbour whose leading chars are hex (.cache/.func), which
// the greedy candidate run swallows.
func TestContainsIPv6CatchesDotBounded(t *testing.T) {
	leaky := []string{
		"addr.2001:db8::1.suffix",
		"host.2001:db8::1.cache", // .cac is hex → glued into the candidate
		"node.2001:db8::1.func1", // trailing hex+digits
		"a.fe80::1.b",
	}
	for _, v := range leaky {
		if !containsIPv6(v) {
			t.Errorf("containsIPv6 missed an IPv6 in %q", v)
		}
		if sanitizeString(v) != "" {
			t.Errorf("sanitizeString must blank a free-text value carrying an IPv6: %q", v)
		}
	}
	// A symbol carrying IPv6 material (even two glued by a dot) is blanked — no address survives.
	if got := redactSymbolPII("x.2001:db8::1.2001:db8::2.y"); got != "" {
		t.Errorf("symbol with IPv6 material must blank, got %q", got)
	}
	// No false positives on hex-but-not-address runs (no colon) or IPv4-mapped staying intact.
	if containsIPv6("revision.deadbeef.cafe") {
		t.Error("containsIPv6 false-positive on a hex-but-not-address run")
	}
	if !containsIPv6("mapped.::ffff:1.2.3.4.tail") {
		t.Error("containsIPv6 missed an IPv4-mapped IPv6")
	}
}
