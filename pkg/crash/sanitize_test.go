package crash

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSanitizeEventStripsDisallowedOptionalFields(t *testing.T) {
	event := validEvent(t)
	event.App.Version = "sample@example.invalid"
	event.App.BuildID = "header.eyJzdWIiOiJ0ZXN0In0.signature"
	event.OS.Name = "desktop 198.51.100.23"
	event.OS.Version = "2001:db8::1"
	event.Threads[0].Frames[0].Function = "handler.report@example.invalid"
	event.Threads[0].Frames[0].File = "safe/file.go"
	event.Threads[0].Frames[0].Line = -12
	event.Threads[0].Frames[0].ModuleName = "synthetic-module"
	event.Device["model"] = "device_raw_identifier"
	event.Metadata = map[string]string{"safe_key": "safe_value", "unsafe": "sample@example.invalid"}
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

	if sanitized.App.Version != "" {
		t.Fatalf("expected email-like app version to be stripped, got %q", sanitized.App.Version)
	}
	if sanitized.App.BuildID != "" {
		t.Fatalf("expected JWT-like build id to be stripped, got %q", sanitized.App.BuildID)
	}
	if sanitized.OS.Name != "" {
		t.Fatalf("expected IPv4-bearing OS name to be stripped, got %q", sanitized.OS.Name)
	}
	if sanitized.OS.Version != "" {
		t.Fatalf("expected IPv6-bearing OS version to be stripped, got %q", sanitized.OS.Version)
	}
	frame := sanitized.Threads[0].Frames[0]
	if frame.Function != "" {
		t.Fatalf("expected email-bearing frame function to be stripped, got %q", frame.Function)
	}
	if frame.File != "safe/file.go" {
		t.Fatalf("expected safe frame file to remain, got %q", frame.File)
	}
	if frame.Line != 0 {
		t.Fatalf("expected negative frame line to be clamped to 0, got %d", frame.Line)
	}
	if _, ok := sanitized.Device["model"]; ok {
		t.Fatalf("expected unsafe device model to be dropped, got %#v", sanitized.Device)
	}
	if got := sanitized.Metadata["safe_key"]; got != "safe_value" {
		t.Fatalf("expected safe metadata to remain, got %#v", sanitized.Metadata)
	}
	if _, ok := sanitized.Metadata["unsafe"]; ok {
		t.Fatalf("expected unsafe metadata to be dropped, got %#v", sanitized.Metadata)
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
		event.Context["session_id"] = sessionID
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
		event.App.ID,
		event.App.Version,
		event.App.BuildID,
		event.Platform,
		event.OS.Name,
		event.OS.Version,
		event.Exception.Type,
		event.Exception.Reason,
		event.Exception.CrashedThreadID,
		event.RawText,
	}
	for _, module := range event.Modules {
		values = append(values, module.ID, module.Name, module.Platform, module.DebugID, module.BuildID, module.LoadAddress, module.BaseAddress, module.EndAddress, module.Size)
	}
	for _, thread := range event.Threads {
		values = append(values, thread.ID, thread.Name)
		for _, frame := range thread.Frames {
			values = append(values, frame.ModuleID, frame.Module, frame.ModuleName, frame.InstructionAddress, frame.Address, frame.RelativeAddress, frame.Function, frame.File)
		}
	}
	for _, breadcrumb := range event.Breadcrumbs {
		values = append(values, breadcrumb.Name, breadcrumb.Type, breadcrumb.Category, breadcrumb.Level, breadcrumb.Message)
	}
	for _, value := range event.Device {
		values = append(values, value)
	}
	for _, value := range event.Context {
		values = append(values, value)
	}
	for _, value := range event.Metadata {
		values = append(values, value)
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

func TestSanitizeSymbolKeepsGoSymbolsButStripsPII(t *testing.T) {
	// Package-qualified Go symbols (value-receiver methods, closures) are dotted
	// 3-segment strings that the JWT heuristic would falsely blank; sanitizeSymbol must
	// keep them, or the frame loses its function and the whole crash is dropped.
	keep := []string{
		"crash_test.boomForCaptureTest",
		"main.Server.Handle",
		"pkg.(*Type).Method",
		"pkg.worker.process.func1",
		"crash_test.TestRecoverCapturesPanicOriginAndRepanics.func2",
		// Packages legitimately named with a raw-identifier prefix must survive — blanking
		// them would drop the whole crash (the symbol is the address-less frame's identity).
		"player_state.Tick",
		"user_session.(*Manager).Close",
		"device_registry.lookup.func1",
		"customer_billing.Charge",
	}
	for _, s := range keep {
		if got := sanitizeSymbol(s); got != s {
			t.Errorf("sanitizeSymbol(%q) = %q, want it preserved", s, got)
		}
	}
	// Only an embedded email or IP blanks a symbol — content that never appears in a
	// legitimate Go symbol.
	strip := []string{
		"pkg.handler@example.invalid",
		"node.198.51.100.23.handler",
		"handler_2001:db8::1",
	}
	for _, s := range strip {
		if got := sanitizeSymbol(s); got != "" {
			t.Errorf("sanitizeSymbol(%q) = %q, want blanked", s, got)
		}
	}
}

func TestShortFuncNameTrimsImportPath(t *testing.T) {
	cases := map[string]string{
		"github.com/org/repo/pkg.(*Type).Method": "pkg.(*Type).Method",
		"github.com/org/repo/internal/foo.Bar":   "foo.Bar",
		"main.main":                              "main.main",
		"runtime.gopanic":                        "runtime.gopanic",
		"":                                       "",
	}
	for in, want := range cases {
		if got := shortFuncName(in); got != want {
			t.Errorf("shortFuncName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSanitizeEventScrubsSource(t *testing.T) {
	e := validEvent(t)
	e.Source = "report@example.invalid" // PII in the source slug
	s, err := SanitizeEvent(e)
	if err != nil {
		t.Fatalf("SanitizeEvent: %v", err)
	}
	if s.Source != "" {
		t.Fatalf("PII-bearing source must be blanked, got %q", s.Source)
	}
	e.Source = "main-server" // a valid component slug survives
	s, err = SanitizeEvent(e)
	if err != nil {
		t.Fatalf("SanitizeEvent: %v", err)
	}
	if s.Source != "main-server" {
		t.Fatalf("valid source slug must survive, got %q", s.Source)
	}
}

func TestSafeTypeNameSurvivesScrubber(t *testing.T) {
	// safeTypeName keeps the auto-capture Go type from being blanked by the FULL exception
	// type scrubber while never producing an empty (crash-dropping) value.
	cases := map[string]string{
		"string":               "string",
		"*errors.errorString":  "*errors.errorString",
		"runtime.Error":        "runtime.Error",
		"*user_session.Fault":  "*user_session.Fault", // pointer: prefix rule does not fire
		"user_session.Fault":   "Fault",               // value type in a user_ pkg → bare type
		"player_state.ErrTick": "ErrTick",
	}
	for in, want := range cases {
		got := safeTypeName(in)
		if got != want {
			t.Errorf("safeTypeName(%q) = %q, want %q", in, got, want)
		}
		if sanitizeString(got) == "" {
			t.Errorf("safeTypeName(%q) = %q must survive the wire scrubber, got blanked", in, got)
		}
	}
}

func TestSanitizeEventScrubsManualExceptionType(t *testing.T) {
	// A MANUAL caller's exception.type stays under the full scrubber (defense for misuse):
	// a token-like value is blanked (and the event then fails validation, dropping the
	// misuse rather than leaking it).
	e := validEvent(t)
	e.Exception.Type = "header.eyJzdWIiOiJ0ZXN0In0.signature"
	s, err := SanitizeEvent(e)
	if err != nil {
		t.Fatalf("SanitizeEvent: %v", err)
	}
	if s.Exception.Type != "" {
		t.Fatalf("token-like manual exception type must be blanked, got %q", s.Exception.Type)
	}
}

func TestSanitizeEventModulesNeverNull(t *testing.T) {
	e := validEvent(t)
	e.Modules = nil // zero-module pre-symbolicated crash
	e.Threads[0].Frames = []Frame{{Function: "main.run", File: "main.go", Line: 1}}
	s, err := SanitizeEvent(e)
	if err != nil {
		t.Fatalf("SanitizeEvent: %v", err)
	}
	if s.Modules == nil {
		t.Fatalf("Modules must be a non-nil empty slice so it marshals as [] not null")
	}
}

func TestTrimBuildPath(t *testing.T) {
	cases := map[string]string{
		"/home/alice/work/proj/internal/foo/bar.go":           "foo/bar.go",
		"/Users/oleg/shardpilot/shardpilot-go/pkg/crash/c.go": "crash/c.go",
		"foo/bar.go": "foo/bar.go",
		"bar.go":     "bar.go",
		"":           "",
	}
	for in, want := range cases {
		if got := trimBuildPath(in); got != want {
			t.Errorf("trimBuildPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSanitizeFunctionNameTrustedVsUntrusted(t *testing.T) {
	jwtShaped := "crash_test.TestFoo.func2"         // legit Go symbol, but JWT-shaped
	prefixed := "player_state.Tick"                 // legit symbol in a raw-id-prefixed pkg
	token := "header.eyJzdWIiOiJ0ZXN0In0.signature" // a real token in a function field
	// Trusted (SDK-captured): legit symbols survive.
	for _, s := range []string{jwtShaped, prefixed, "main.run"} {
		if got := sanitizeFunctionName(s, true); got != s {
			t.Errorf("trusted sanitizeFunctionName(%q) = %q, want preserved", s, got)
		}
	}
	// Untrusted (manual caller): the full scrubber blanks token/raw-id-prefixed values.
	for _, s := range []string{jwtShaped, prefixed, token} {
		if got := sanitizeFunctionName(s, false); got != "" {
			t.Errorf("untrusted sanitizeFunctionName(%q) = %q, want blanked", s, got)
		}
	}
	// A plain symbol survives even untrusted.
	if got := sanitizeFunctionName("main.run", false); got != "main.run" {
		t.Errorf("untrusted plain symbol must survive, got %q", got)
	}
}

func TestSanitizeEventBlanksTokenInManualFrameFunction(t *testing.T) {
	e := validEvent(t) // frame already carries an instruction_addr + module, so it stays valid
	e.Threads[0].Frames[0].Function = "header.eyJzdWIiOiJ0ZXN0In0.signature"
	s, err := SanitizeEvent(e) // manual path → full scrub
	if err != nil {
		t.Fatalf("SanitizeEvent: %v", err)
	}
	if got := s.Threads[0].Frames[0].Function; got != "" {
		t.Fatalf("token-like manual frame function must be blanked on the wire, got %q", got)
	}
}
