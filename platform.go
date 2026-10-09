package shardpilot

import "strings"

// Analytics platform values and supported host aliases.
var envelopePlatformVocabulary = map[string]string{
	"android":   "android",
	"browser":   "web",
	"darwin":    "macos",
	"html5":     "web",
	"ios":       "ios",
	"ipad":      "ios",
	"ipados":    "ios",
	"iphone":    "ios",
	"linux":     "linux",
	"mac":       "macos",
	"macos":     "macos",
	"macosx":    "macos",
	"osx":       "macos",
	"other":     "other",
	"ps4":       "ps4",
	"ps5":       "ps5",
	"switch":    "switch",
	"tvos":      "tvos",
	"steamdeck": "linux",
	"web":       "web",
	"win":       "windows",
	"win32":     "windows",
	"win64":     "windows",
	"windows":   "windows",
	"xbox":      "xbox",
}

// maxWarnedPlatforms caps the reported-value set. A correct caller produces a
// handful; a caller interpolating a version or a device id into `platform`
// produces one per event, and that must cost a bounded amount of memory.
const maxWarnedPlatforms = 32

// normalizeEnvelopePlatform folds host input to a canonical analytics value.
func normalizeEnvelopePlatform(value string) string {
	if platform := envelopePlatformVocabulary[strings.ToLower(strings.TrimSpace(value))]; platform != "" {
		return platform
	}
	return "other"
}

// warnUnmappedPlatform reports each configured fallback once, with bounded state.
func (c *Client) warnUnmappedPlatform(raw string) {
	// NOTHING TO SAY MEANS NOTHING TO REMEMBER. `logf` is a no-op without a
	// logger (consent.go:641), so caching here would retain unbounded host
	// input to suppress a message that is never printed.
	if c.cfg.Logger == nil {
		return
	}

	c.warnedMu.Lock()
	switch {
	case c.warnedPlatforms[raw]:
		c.warnedMu.Unlock()
		return
	case len(c.warnedPlatforms) >= maxWarnedPlatforms:
		announce := !c.warnedPlatformsFull
		c.warnedPlatformsFull = true
		c.warnedMu.Unlock()
		// SAY THAT IT STOPPED. Going quiet at the ceiling would leave a reader
		// of the log believing the values after it were understood, which is
		// the exact silence this diagnostic exists to break.
		if announce {
			c.logf("shardpilot platform: more than %d distinct unrecognised "+
				"platform values have been configured; each is sent as other, "+
				"but they will no longer be logged "+
				"individually", maxWarnedPlatforms)
		}
		return
	}
	if c.warnedPlatforms == nil {
		c.warnedPlatforms = make(map[string]bool, maxWarnedPlatforms)
	}
	c.warnedPlatforms[raw] = true
	c.warnedMu.Unlock()

	// Logged OUTSIDE the lock: Printf is host code and may block or re-enter.
	c.logf("shardpilot platform: configured platform %q is unrecognised; "+
		"the event envelope uses other", raw)
}
