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

// SanitizeEvent scrubs an event for the wire with the FULL content rules on every
// caller-populated string, including frame functions. The auto-capture path uses the
// internal sanitizeEvent(event, true) instead, which treats runtime-derived frame
// functions as trusted code symbols.
func SanitizeEvent(event Event) (Event, error) {
	return sanitizeEvent(event, false)
}

func sanitizeEvent(event Event, trustedFrameFunctions bool) (Event, error) {
	event = cloneEvent(event)
	event.CrashID = strings.TrimSpace(event.CrashID)
	event.App.ID = sanitizeString(event.App.ID)
	event.App.Version = sanitizeString(event.App.Version)
	event.App.BuildID = sanitizeString(event.App.BuildID)
	// Source is an operator-set component slug on the wire; scrub it like the
	// other identifiers so a misconfigured value carrying PII never leaves the process.
	event.Source = sanitizeString(event.Source)
	event.Platform = sanitizeString(event.Platform)
	event.OS.Name = sanitizeString(event.OS.Name)
	event.OS.Version = sanitizeString(event.OS.Version)
	// exception.type stays under the FULL scrubber: it is caller-populated free text for
	// manual Emit/EmitFatal, so a token-like or raw-identifier value must still be stripped.
	// The auto-capture path keeps its Go type safe at the source via panicType/safeTypeName
	// so a legit package-prefixed type is not blanked here.
	event.Exception.Type = sanitizeString(event.Exception.Type)
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
			frame.Function = sanitizeFunctionName(frame.Function, trustedFrameFunctions)
			frame.File = sanitizeString(frame.File)
			if frame.Line < 0 {
				frame.Line = 0
			}
		}
	}

	// Drop any frame left with neither a function nor an address: it carries no identity,
	// fails validateEvent, and would DROP THE WHOLE CRASH. A frame reaches this state when
	// the scrubber blanks its function entirely (a manual Emit frame whose function was
	// PII, or a pathological all-PII symbol). Dropping the single dead frame keeps the
	// fatal crash; the validate() "at least one frame or raw_text" floor still applies if a
	// thread ends up empty.
	for i := range event.Threads {
		frames := event.Threads[i].Frames
		kept := frames[:0]
		for _, frame := range frames {
			hasFunction := strings.TrimSpace(frame.Function) != ""
			hasAddress := strings.TrimSpace(firstNonEmptyString(frame.InstructionAddress, frame.Address)) != ""
			if !hasFunction && !hasAddress {
				continue
			}
			kept = append(kept, frame)
		}
		event.Threads[i].Frames = kept
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

// sanitizeSymbol scrubs a code symbol (a stack-frame function name). It strips the signals
// that never legitimately appear in a Go symbol — an embedded email or IP — and deliberately
// omits both the JWT/dotted-token heuristic (a package-qualified symbol like pkg.Type.Method
// is three dotted segments) AND the raw-identifier PREFIX heuristic (a package may legitimately
// be named player_*, user_*, customer_*, device_*, e.g. player_state.Tick).
//
// The IPv4 run is redacted IN PLACE rather than blanking the whole symbol: a blanked function
// on a pre-symbolicated frame (no address) makes the frame identity-less, which fails
// validation and would drop the WHOLE crash. This matters for a false positive — a deeply
// nested closure renders as foo.func1.1.1.1, whose "1.1.1.1" tail looks like an IPv4 address —
// where redacting only that run keeps the frame and its file:line. An '@' or ':' marks a
// value that is not a real symbol and is blanked (see redactSymbolPII); the frame-drop
// backstop in sanitizeEvent keeps the crash. ShardPilot re-scrubs Function server-side (full
// pattern set) as defense in depth.
func sanitizeSymbol(value string) string {
	return strings.TrimSpace(redactSymbolPII(strings.TrimSpace(value)))
}

// sanitizeFunctionName scrubs a stack-frame function. A TRUSTED function comes from the
// Go runtime symbol table (the auto-capture path) and is scrubbed as a code symbol so a
// legitimate package-qualified name (which is JWT-shaped / raw-id-prefixed) survives; an
// UNTRUSTED function is caller-populated (manual Emit/EmitFatal) and gets the full content
// scrubber, preserving the SDK's no-tokens-on-the-wire guarantee for that public field.
func sanitizeFunctionName(value string, trusted bool) string {
	if trusted {
		return sanitizeSymbol(value)
	}
	return sanitizeString(value)
}

// redactSymbolPII scrubs a code symbol so the stack frame keeps its identity where possible.
//
// An email ('@') or an IPv6 literal (':') never appears in a legitimate Go symbol — method
// names use '.', generics use '[]'. So if either is present the value is not a real code
// symbol and is blanked; the frame-drop backstop in sanitizeEvent then keeps the crash. These
// cannot be redacted in place cleanly: a dotted email local part or a dotted IPv6 neighbour
// has no separator marking where it begins/ends within the surrounding dotted text, so an
// in-place attempt either leaks part of the address or eats the whole symbol.
//
// The IPv4 shape is different — it DOES have a legitimate false positive: a deeply nested
// closure renders as foo.func1.1.1.1, whose "1.1.1.1" tail looks like an address. Blanking
// that would drop a real frame (and a single-frame fatal crash), so the IPv4 run is redacted
// IN PLACE, keeping the rest of the symbol and its file:line.
func redactSymbolPII(value string) string {
	if strings.Contains(value, "@") || containsIPv6(value) {
		return ""
	}
	return redactIPv4(value)
}

// redactIPv4 removes every dotted-quad address while preserving the surrounding boundary
// characters that ipv4Pattern consumes (the leading/trailing non-digit), so adjacent symbol
// text is not eaten. ipv4Pattern consumes a boundary char on each side and ReplaceAllStringFunc
// is non-overlapping, so two quads sharing one separator (8.8.8.8 9.9.9.9) would leave the
// second behind in a single pass; re-run until the string stops shrinking. Each pass only
// removes digits (never adds), so this terminates.
func redactIPv4(value string) string {
	for {
		next := ipv4Pattern.ReplaceAllStringFunc(value, func(match string) string {
			loc := ipv4Pattern.FindStringSubmatchIndex(match)
			if loc == nil || loc[2] < 0 {
				return match
			}
			return match[:loc[2]] + match[loc[3]:]
		})
		if next == value {
			return value
		}
		value = next
	}
}

// ipv6Address finds a real IPv6 address inside a [0-9A-Fa-f:.] candidate run and returns it.
// ipv6CandidatePattern greedily glues a dotted/hex neighbour onto the address
// (2001:db8::1.cache -> "2001:db8::1.cac"), so when the whole run does not parse, the longest
// dot-delimited substring that does is returned. Without this a real IPv6 would escape both
// the detector (containsIPv6/sanitizeString) and the redactor. A candidate with no ':' cannot
// be IPv6 and is rejected up front.
func ipv6Address(candidate string) (string, bool) {
	candidate = strings.Trim(candidate, "[](){}<>,;. \t")
	if !strings.Contains(candidate, ":") {
		return "", false
	}
	segments := strings.Split(candidate, ".")
	// Widest window first so an IPv4-mapped form (::ffff:1.2.3.4) is matched whole before
	// any shorter prefix; leftmost at each width for determinism.
	for size := len(segments); size >= 1; size-- {
		for start := 0; start+size <= len(segments); start++ {
			sub := strings.Join(segments[start:start+size], ".")
			if strings.Contains(sub, ":") && net.ParseIP(sub) != nil {
				return sub, true
			}
		}
	}
	return "", false
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
		if _, ok := ipv6Address(candidate); ok {
			return true
		}
	}
	return false
}
