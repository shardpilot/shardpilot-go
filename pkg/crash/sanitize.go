package crash

import (
	"fmt"
	"net"
	"regexp"
	"strings"
)

var (
	analyticsEventNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.:-]{0,127}$`)
	mapKeyPattern             = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.:-]{0,63}$`)
	ipv4Pattern               = regexp.MustCompile(`(?:^|[^0-9])((?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\.(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\.(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\.(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9]))(?:$|[^0-9])`)
	ipv6CandidatePattern      = regexp.MustCompile(`[0-9A-Fa-f:.]{3,}`)
	jwtPattern                = regexp.MustCompile(`(?:^|[^A-Za-z0-9_-])([A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,})(?:$|[^A-Za-z0-9_-])`)
)

var disallowedPrefixes = [...]string{
	"player_",
	"user_",
	"customer_",
	"device_",
}

type sanitizer struct{}

var sanitize sanitizer

func (sanitizer) Event(event Event) (Event, error) {
	return SanitizeEvent(event)
}

func SanitizeEvent(event Event) (Event, error) {
	event = cloneEvent(event)
	event.CrashID = strings.TrimSpace(event.CrashID)
	event.App.ID = sanitizeString(event.App.ID)
	event.App.Version = sanitizeString(event.App.Version)
	event.App.BuildID = sanitizeString(event.App.BuildID)
	// Source is an operator-set component slug (ADR-0223) on the wire; scrub it like the
	// other identifiers so a misconfigured value carrying PII never leaves the process.
	event.Source = sanitizeString(event.Source)
	event.Platform = sanitizeString(event.Platform)
	event.OS.Name = sanitizeString(event.OS.Name)
	event.OS.Version = sanitizeString(event.OS.Version)
	// exception.type is a CODE SYMBOL (a Go panic value's type, e.g. user_session.Fault, or
	// a native signal name) — scrub it as a symbol so a legit package-prefixed type name is
	// not blanked as a raw identifier (which would drop the crash for an empty exception.type).
	event.Exception.Type = sanitizeSymbol(event.Exception.Type)
	event.Exception.Reason = sanitizeString(event.Exception.Reason)
	event.Exception.CrashedThreadID = sanitizeString(event.Exception.CrashedThreadID)
	event.RawText = sanitizeString(event.RawText)
	if sessionID := event.Context["session_id"]; containsDisallowedContent(sessionID) {
		return Event{}, fmt.Errorf("%w: context.session_id contains disallowed identifier material", ErrInvalidEvent)
	}
	event.Device = sanitizeStringMap(event.Device)
	event.Context = sanitizeStringMap(event.Context)
	event.Metadata = sanitizeStringMap(event.Metadata)

	for i := range event.Modules {
		event.Modules[i].ID = sanitizeString(event.Modules[i].ID)
		event.Modules[i].Name = sanitizeString(event.Modules[i].Name)
		event.Modules[i].Platform = sanitizeString(event.Modules[i].Platform)
		event.Modules[i].DebugID = sanitizeString(event.Modules[i].DebugID)
		event.Modules[i].BuildID = sanitizeString(event.Modules[i].BuildID)
		event.Modules[i].LoadAddress = sanitizeString(event.Modules[i].LoadAddress)
		event.Modules[i].BaseAddress = sanitizeString(event.Modules[i].BaseAddress)
		event.Modules[i].EndAddress = sanitizeString(event.Modules[i].EndAddress)
		event.Modules[i].Size = sanitizeString(event.Modules[i].Size)
	}

	for i := range event.Threads {
		event.Threads[i].ID = sanitizeString(event.Threads[i].ID)
		event.Threads[i].Name = sanitizeString(event.Threads[i].Name)
		for j := range event.Threads[i].Frames {
			frame := &event.Threads[i].Frames[j]
			frame.ModuleID = sanitizeString(frame.ModuleID)
			frame.Module = sanitizeString(frame.Module)
			frame.ModuleName = sanitizeString(frame.ModuleName)
			frame.InstructionAddress = sanitizeString(frame.InstructionAddress)
			frame.Address = sanitizeString(frame.Address)
			frame.RelativeAddress = sanitizeString(frame.RelativeAddress)
			frame.Function = sanitizeSymbol(frame.Function)
			frame.File = sanitizeString(frame.File)
			if frame.Line < 0 {
				frame.Line = 0
			}
		}
	}

	sourceBreadcrumbs := capBreadcrumbs(event.Breadcrumbs)
	breadcrumbs := make([]Breadcrumb, 0, len(sourceBreadcrumbs))
	for _, breadcrumb := range sourceBreadcrumbs {
		name, ok := sanitizeBreadcrumbName(breadcrumb.Name)
		if !ok {
			continue
		}
		breadcrumb.Name = name
		breadcrumb.Type = sanitizeString(breadcrumb.Type)
		breadcrumb.Category = sanitizeString(breadcrumb.Category)
		breadcrumb.Level = sanitizeString(breadcrumb.Level)
		breadcrumb.Message = sanitizeString(breadcrumb.Message)
		breadcrumb.Timestamp = breadcrumb.Timestamp.UTC()
		breadcrumbs = append(breadcrumbs, breadcrumb)
	}
	event.Breadcrumbs = breadcrumbs
	event.FingerprintComponents = sanitizeStringSlice(event.FingerprintComponents)
	event.OccurredAt = event.OccurredAt.UTC()

	// The required `modules` field must marshal as [] (an empty list), not null, for a
	// pre-symbolicated crash with zero native modules — a strict producer schema validating
	// `type: array` would reject null and drop every auto-captured Go panic.
	if event.Modules == nil {
		event.Modules = []Module{}
	}

	return event, nil
}

func sanitizeString(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || containsDisallowedContent(value) {
		return ""
	}
	return value
}

func sanitizeStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := map[string]string{}
	for key, value := range in {
		key = strings.TrimSpace(key)
		if !mapKeyPattern.MatchString(key) || containsDisallowedContent(key) {
			continue
		}
		value = sanitizeString(value)
		if value == "" {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func sanitizeStringSlice(in []string) []string {
	out := make([]string, 0, len(in))
	for _, value := range in {
		value = sanitizeString(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func sanitizeBreadcrumbName(name string) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", false
	}
	if !analyticsEventNamePattern.MatchString(name) {
		return "", false
	}
	if containsDisallowedContent(name) {
		return "", false
	}
	return name, true
}

func containsDisallowedContent(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if containsDisallowedIdentity(value) {
		return true
	}
	return jwtPattern.MatchString(value)
}

// containsDisallowedIdentity reports the non-token PII signals: emails, the
// player_/user_/customer_/device_ raw-identifier prefixes, and IP addresses. It is
// the part of the disallowed-content check that applies even to code symbols.
func containsDisallowedIdentity(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if strings.Contains(value, "@") {
		return true
	}
	if startsWithDisallowedPrefix(value) {
		return true
	}
	if ipv4Pattern.MatchString(value) {
		return true
	}
	return containsIPv6(value)
}

// sanitizeSymbol scrubs a code symbol (a stack-frame function name). It applies ONLY the
// signals that never legitimately appear in a Go symbol — an embedded email or IP — and
// deliberately omits both the JWT/dotted-token heuristic (a package-qualified symbol like
// pkg.Type.Method is three dotted segments) AND the raw-identifier PREFIX heuristic (a
// package may legitimately be named player_*, user_*, customer_*, device_*, e.g.
// player_state.Tick). Blanking a valid symbol would strip the frame's only identity and,
// for a pre-symbolicated frame with no address, drop the WHOLE crash; the crash-symbolicator
// re-scrubs Function server-side (full pattern set) as defense in depth.
func sanitizeSymbol(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || symbolHasDisallowedContent(value) {
		return ""
	}
	return value
}

// symbolHasDisallowedContent flags an embedded email or IP address — the only PII signals
// that cannot appear in a legitimate Go code symbol.
func symbolHasDisallowedContent(value string) bool {
	if strings.Contains(value, "@") {
		return true
	}
	if ipv4Pattern.MatchString(value) {
		return true
	}
	return containsIPv6(value)
}

func startsWithDisallowedPrefix(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	for _, prefix := range disallowedPrefixes {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func containsIPv6(value string) bool {
	for _, candidate := range ipv6CandidatePattern.FindAllString(value, -1) {
		if !strings.Contains(candidate, ":") {
			continue
		}
		candidate = strings.Trim(candidate, "[](){}<>,;")
		if strings.Contains(candidate, ":") && net.ParseIP(candidate) != nil {
			return true
		}
	}
	return false
}
