package shardpilot

import "strings"

// The analytics envelope's `platform` is folded to the vocabulary the ingest
// door accepts. It was NOT folded before: `Config.Platform` reached the wire as
// the host typed it, and the ingest vocabulary is closed -- one out-of-vocabulary
// value fails the WHOLE batch, every event in it, not just the offending one.
//
// THE TABLE IS NOT AUTHORED HERE. It is tier 1 of the shared conformance corpus
// at `docs/engineering/platform-fold-corpus/`, which was generated from the two
// SDKs that already fold: shardpilot-unreal's NormalizeEnvelopePlatform
// (compiled and executed) and shardpilot-godot's real vocabulary. Corpus
// association digest: 1bd5acee111988d8 -- `platform_conformance_test.go` pins it, so this
// table and the corpus cannot drift apart quietly.
//
// TIER 2 (Unreal's suffix strip: "WindowsNoEditor" -> "windows") IS DELIBERATELY
// NOT IMPLEMENTED, and the corpus permits that. Unreal has it because
// FPlatformProperties::PlatformName() PRODUCES those spellings -- a closed source
// with an enumerable set. Here the value is typed by a human ("Windows 11",
// "win-x64", "PC"), so nine suffixes would not approach completeness; they would
// manufacture the APPEARANCE of it, and the next uncovered spelling would read as
// a gap in the rule rather than as a property of an open input. A reader finding
// this narrower than Unreal is seeing a decision, not an omission.
//
// THE CRASH PLATFORM IS NOT TOUCHED. There the value is required, may be any
// lowercase token, and rides the group fingerprint; folding it would refingerprint
// existing crash groups (ShardPilotCore.cpp:116-120). `pkg/crash` keeps sending
// the honest name.
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
	"steamdeck": "linux",
	"web":       "web",
	"win":       "windows",
	"win32":     "windows",
	"win64":     "windows",
	"windows":   "windows",
}

// normalizeEnvelopePlatform folds a host-supplied platform to the accepted
// vocabulary, or returns "" when it maps to nothing. An empty answer means the
// key is OMITTED from the envelope -- `platform` is optional at the door, so an
// omitted key is accepted while a wrong one fails the batch.
func normalizeEnvelopePlatform(value string) string {
	return envelopePlatformVocabulary[strings.ToLower(strings.TrimSpace(value))]
}

// warnUnmappedPlatform reports a host-supplied platform that folded to nothing,
// once per distinct value.
//
// Tier 1 of the corpus loses such a value silently: it becomes "" and the key is
// omitted, so the host who typed something meaningful never learns it was not
// understood. Enumerating spellings cannot close that -- the input is open, and a
// human may type anything -- but saying "I did not understand what you set" can,
// and always.
//
// Only what the host SET is reported. An unset platform is the ordinary default
// path, and warning there would fire on every correctly-configured client.
func (c *Client) warnUnmappedPlatform(raw string) {
	if _, seen := c.warnedPlatforms.LoadOrStore(raw, struct{}{}); seen {
		return
	}
	c.logf("shardpilot platform: configured platform %q is not one of the "+
		"accepted values (web, ios, android, windows, macos, linux); it is "+
		"omitted from the event envelope rather than sent, which would fail "+
		"the whole batch", raw)
}
