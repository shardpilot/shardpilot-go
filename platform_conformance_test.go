package shardpilot

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// Supported host aliases remain stable as the canonical set grows.
type foldVector struct {
	in_ string
	out string
}

var corpusCore = []foldVector{
	{in_: "android", out: "android"},
	{in_: "browser", out: "web"},
	{in_: "darwin", out: "macos"},
	{in_: "html5", out: "web"},
	{in_: "ios", out: "ios"},
	{in_: "ipad", out: "ios"},
	{in_: "ipados", out: "ios"},
	{in_: "iphone", out: "ios"},
	{in_: "linux", out: "linux"},
	{in_: "mac", out: "macos"},
	{in_: "macos", out: "macos"},
	{in_: "macosx", out: "macos"},
	{in_: "osx", out: "macos"},
	{in_: "steamdeck", out: "linux"},
	{in_: "web", out: "web"},
	{in_: "win", out: "windows"},
	{in_: "win32", out: "windows"},
	{in_: "win64", out: "windows"},
	{in_: "windows", out: "windows"},
}

func TestEnvelopePlatformLegacyAliases(t *testing.T) {
	for _, v := range corpusCore {
		for _, in := range []string{v.in_, " " + strings.ToUpper(v.in_) + " "} {
			if got := normalizeEnvelopePlatform(in); got != v.out {
				t.Errorf("alias %q: got %q, want %q", in, got, v.out)
			}
		}
	}
}

func TestSuffixSpellingsUseFallback(t *testing.T) {
	for _, in := range []string{"WindowsNoEditor", "LinuxArm64", "MacEditor", "WindowsClient"} {
		if got := normalizeEnvelopePlatform(in); got != "other" {
			t.Errorf("platform %q: got %q, want other", in, got)
		}
	}
}

type collectLogger struct {
	mu   *sync.Mutex
	msgs *[]string
}

func (l collectLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	*l.msgs = append(*l.msgs, format)
}

func newPlatformTestClient(t *testing.T, platform string) (*Client, *[]string, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	msgs := []string{}
	c := &Client{
		cfg:   Config{Platform: platform, Logger: collectLogger{mu: &mu, msgs: &msgs}},
		clock: realClock{},
	}
	return c, &msgs, &mu
}

// Repeated configured fallbacks produce one warning per distinct value.
func TestUnmappedPlatformIsReportedOncePerValue(t *testing.T) {
	c, msgs, mu := newPlatformTestClient(t, "Win64_Shipping")
	c.warnUnmappedPlatform("Win64_Shipping")
	c.warnUnmappedPlatform("Win64_Shipping")
	c.warnUnmappedPlatform("PC")
	mu.Lock()
	defer mu.Unlock()
	if len(*msgs) != 2 {
		t.Fatalf("want one warning per DISTINCT value (2), got %d: %v", len(*msgs), *msgs)
	}
}

func TestSetAndUnmappedPlatformWarnsWhileUnsetIsSilent(t *testing.T) {
	for _, tc := range []struct {
		name         string
		platform     string
		wantWarn     bool
		wantPlatform string
	}{
		{"set and unmapped is reported", "Windows 11", true, "other"},
		{"unset stays silent", "", false, "other"},
		{"canonical other stays silent", " OtHeR ", false, "other"},
		{"new canonical stays silent", "ps5", false, "ps5"},
		{"set and mapped is silent", "win", false, "windows"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, msgs, mu := newPlatformTestClient(t, tc.platform)
			env, err := c.buildEnvelope(Event{Name: "purchase"})
			if err != nil {
				t.Fatalf("buildEnvelope: %v", err)
			}
			if env.Platform != tc.wantPlatform {
				t.Errorf("envelope platform %q, want %q", env.Platform, tc.wantPlatform)
			}
			mu.Lock()
			defer mu.Unlock()
			if got := len(*msgs) > 0; got != tc.wantWarn {
				t.Errorf("warned=%v, want %v (messages: %v)", got, tc.wantWarn, *msgs)
			}
		})
	}
}

// THE CACHE IS BOUNDED, AND IT SAYS WHEN IT STOPS.
//
// `Event.Platform` is per-event host input, so the reported-value set is keyed
// on a cardinality the caller chooses, not one we do. A caller interpolating a
// build id into `platform` produces a distinct value per event; retaining all of
// them for the client's lifetime is unbounded growth driven from outside.
func TestWarnedPlatformCacheIsBounded(t *testing.T) {
	c, msgs, mu := newPlatformTestClient(t, "")
	for i := 0; i < maxWarnedPlatforms*8; i++ {
		c.warnUnmappedPlatform(fmt.Sprintf("build-%d", i))
	}

	c.warnedMu.Lock()
	held := len(c.warnedPlatforms)
	c.warnedMu.Unlock()
	if held > maxWarnedPlatforms {
		t.Errorf("retained %d distinct values, cap is %d", held, maxWarnedPlatforms)
	}

	mu.Lock()
	defer mu.Unlock()
	// One line per remembered value, plus exactly one ceiling notice -- not zero
	// (going quiet reads as "understood") and not one per event.
	ceilings := 0
	for _, m := range *msgs {
		if strings.Contains(m, "no longer be logged") {
			ceilings++
		}
	}
	if ceilings != 1 {
		t.Errorf("ceiling announced %d time(s), want exactly 1", ceilings)
	}
	if len(*msgs) > maxWarnedPlatforms+1 {
		t.Errorf("emitted %d messages for %d events; the cap did not hold",
			len(*msgs), maxWarnedPlatforms*8)
	}
}

// Without a logger there is nothing to suppress, so there is nothing to retain.
func TestNoLoggerRetainsNothing(t *testing.T) {
	c := &Client{cfg: Config{}, clock: realClock{}}
	for i := 0; i < maxWarnedPlatforms*4; i++ {
		c.warnUnmappedPlatform(fmt.Sprintf("build-%d", i))
	}
	c.warnedMu.Lock()
	defer c.warnedMu.Unlock()
	if n := len(c.warnedPlatforms); n != 0 {
		t.Errorf("retained %d values with no logger configured; logf is a no-op "+
			"there, so the cache buys nothing and costs host-controlled memory", n)
	}
}
