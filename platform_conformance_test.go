package shardpilot

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"testing"
)

// TIER 1 OF THE SHARED CONFORMANCE CORPUS, copied in verbatim from
// docs/engineering/platform-fold-corpus/vectors.json.
//
// These vectors are NOT authored here. They were generated from the two SDKs
// that already fold -- shardpilot-unreal's NormalizeEnvelopePlatform, compiled
// and executed, and shardpilot-godot's real vocabulary -- and shardpilot-defold
// and shardpilot-unity are measured against the same set. Three implementations
// agreeing with three private expectations is three implementations diverging at
// the edges, quietly, each passing its own suite.
//
// The copy is pinned by the corpus's own association digest below. Editing a
// vector here breaks that check and names itself; it does not quietly disagree
// with the other two SDKs.
const corpusAssociationDigest = "1bd5acee111988d8"

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

var corpusRejections = []foldVector{
	{in_: "tvos", out: ""},
	{in_: "steam", out: ""},
	{in_: "other", out: ""},
	{in_: "unknown", out: ""},
	{in_: "ps5", out: ""},
	{in_: "ps4", out: ""},
	{in_: "xbox", out: ""},
	{in_: "xsx", out: ""},
	{in_: "switch", out: ""},
	{in_: "nintendo", out: ""},
	{in_: "freebsd", out: ""},
	{in_: "openbsd", out: ""},
	{in_: "netbsd", out: ""},
	{in_: "solaris", out: ""},
	{in_: "illumos", out: ""},
	{in_: "js", out: ""},
	{in_: "", out: ""},
	{in_: "   ", out: ""},
	{in_: "machine-42", out: ""},
	{in_: "studios-pc", out: ""},
	{in_: "Windows 11", out: ""},
	{in_: "win-x64", out: ""},
	{in_: "PC", out: ""},
	{in_: "Win64_Shipping", out: ""},
	{in_: "notaplatformx64", out: ""},
	{in_: "editor", out: ""},
	{in_: "client", out: ""},
	{in_: "x64", out: ""},
}

// TestCorpusCopyMatchesItsDigest is the guard on the copy itself. Without it the
// vectors above are a snapshot nobody compares to anything, and a hand edit reads
// as a legitimate expectation.
func TestCorpusCopyMatchesItsDigest(t *testing.T) {
	var b strings.Builder
	for i, v := range append(append([]foldVector{}, corpusCore...), corpusRejections...) {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(v.in_)
		b.WriteString("\t")
		b.WriteString(v.out)
	}
	sum := sha256.Sum256([]byte(b.String()))
	if got := hex.EncodeToString(sum[:])[:16]; got != corpusAssociationDigest {
		t.Fatalf("the embedded corpus copy has drifted from the corpus: computed %s, corpus records %s -- "+
			"re-copy from docs/engineering/platform-fold-corpus/vectors.json rather than editing a vector here",
			got, corpusAssociationDigest)
	}
}

func TestEnvelopePlatformFoldMatchesCorpus(t *testing.T) {
	for _, v := range corpusCore {
		if got := normalizeEnvelopePlatform(v.in_); got != v.out {
			t.Errorf("core vector %q: got %q, corpus requires %q", v.in_, got, v.out)
		}
	}
	for _, v := range corpusRejections {
		if got := normalizeEnvelopePlatform(v.in_); got != "" {
			t.Errorf("rejection vector %q: got %q, corpus requires the empty answer", v.in_, got)
		}
	}
}

// TIER 2 IS NOT IMPLEMENTED, AND THAT IS THE DECISION. The corpus permits the
// suffix strip and never requires it; here the value is typed by a human, so
// nine suffixes would manufacture the appearance of completeness against an open
// input. This pins the choice so a later reader sees a decision, not an omission.
func TestSuffixSpellingsAreNotFolded(t *testing.T) {
	for _, in := range []string{"WindowsNoEditor", "LinuxArm64", "MacEditor", "WindowsClient"} {
		if got := normalizeEnvelopePlatform(in); got != "" {
			t.Errorf("tier 2 is deliberately unimplemented, but %q folded to %q", in, got)
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

// TIER 3, BOTH HALVES. A value the host SET that folds to nothing is reported;
// nothing set stays silent. The silent half is the load-bearing one: warning on
// the default path fires on every correctly-configured client, which is how a
// diagnostic gets switched off before the day it matters.
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
	// THROUGH buildEnvelope, NOT a re-statement of its condition.
	//
	// An earlier version of this test re-implemented the rule inline --
	// `if folded == "" && raw != "" { warn }` -- and asserted on that. It
	// passed whatever envelope.go actually did: a mutant relaxing the real
	// condition to `if platform == ""` SURVIVED, because the subject was never
	// executed. A fixture that models its subject certifies the model.
	for _, tc := range []struct {
		name        string
		platform    string
		wantWarn    bool
		wantOmitted bool
	}{
		{"set and unmapped is reported", "Windows 11", true, true},
		{"unset stays silent", "", false, true},
		{"set and mapped is silent", "win", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, msgs, mu := newPlatformTestClient(t, tc.platform)
			env, err := c.buildEnvelope(Event{Name: "purchase"})
			if err != nil {
				t.Fatalf("buildEnvelope: %v", err)
			}
			if got := env.Platform == ""; got != tc.wantOmitted {
				t.Errorf("envelope platform %q: omitted=%v, want omitted=%v", env.Platform, got, tc.wantOmitted)
			}
			mu.Lock()
			defer mu.Unlock()
			if got := len(*msgs) > 0; got != tc.wantWarn {
				t.Errorf("warned=%v, want %v (messages: %v)", got, tc.wantWarn, *msgs)
			}
		})
	}
}
