package platform

import "strings"

// Canonical event/crash values and supported host aliases.
var vocabulary = map[string]string{
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

// Normalize folds a host or runtime spelling into the closed wire vocabulary.
// Empty and unmapped values use other.
func Normalize(value string) string {
	if platform := vocabulary[strings.ToLower(strings.TrimSpace(value))]; platform != "" {
		return platform
	}
	return "other"
}

// NormalizeRuntime also recognizes Go's JavaScript target. Analytics host input
// keeps its existing alias contract; automatic capture uses this runtime mapper.
func NormalizeRuntime(value string) string {
	if strings.ToLower(strings.TrimSpace(value)) == "js" {
		return "web"
	}
	return Normalize(value)
}
