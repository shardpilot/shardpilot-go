package crash

import (
	"fmt"
	"net"
	"regexp"
	"strings"
)

var (
	analyticsEventNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.:-]{0,127}$`)
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

	sessionID := strings.TrimSpace(event.SessionID)
	if containsDisallowedContent(sessionID) {
		return Event{}, fmt.Errorf("%w: session_id contains disallowed identifier material", ErrInvalidEvent)
	}
	event.SessionID = sessionID

	event.AppVersion = sanitizeString(event.AppVersion)
	event.BuildID = sanitizeString(event.BuildID)
	event.OS.Name = sanitizeString(event.OS.Name)
	event.OS.Version = sanitizeString(event.OS.Version)
	event.DeviceClass = sanitizeString(event.DeviceClass)
	event.ThreadState = sanitizeString(event.ThreadState)

	for i := range event.StackFrames {
		event.StackFrames[i].Function = sanitizeString(event.StackFrames[i].Function)
		event.StackFrames[i].File = sanitizeString(event.StackFrames[i].File)
		event.StackFrames[i].Module = sanitizeString(event.StackFrames[i].Module)
		if event.StackFrames[i].Line < 0 {
			event.StackFrames[i].Line = 0
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
		breadcrumb.Timestamp = breadcrumb.Timestamp.UTC()
		breadcrumbs = append(breadcrumbs, breadcrumb)
	}
	event.Breadcrumbs = breadcrumbs
	event.OccurredAt = event.OccurredAt.UTC()
	return event, nil
}

func sanitizeString(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || containsDisallowedContent(value) {
		return ""
	}
	return value
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
	if strings.Contains(value, "@") {
		return true
	}
	if startsWithDisallowedPrefix(value) {
		return true
	}
	if ipv4Pattern.MatchString(value) {
		return true
	}
	if containsIPv6(value) {
		return true
	}
	return jwtPattern.MatchString(value)
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
