package sdkversion

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// The header value grammar: "go/" + a SemVer 2.0 version without build
// metadata, at most 64 bytes of visible ASCII.
var headerValuePattern = regexp.MustCompile(`^go/(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`)

func TestHeaderValueGrammar(t *testing.T) {
	if !headerValuePattern.MatchString(HeaderValue) {
		t.Fatalf("HeaderValue %q is not go/<semver without build metadata>", HeaderValue)
	}
	if len(HeaderValue) > 64 {
		t.Fatalf("HeaderValue is %d bytes, over 64", len(HeaderValue))
	}
	for _, b := range []byte(HeaderValue) {
		if b < 0x21 || b > 0x7e {
			t.Fatalf("HeaderValue %q has a byte outside visible ASCII", HeaderValue)
		}
	}
}

// TestVersionIsTheReleaseTag: the constant is the README's latest-tag claim
// without its leading "v" (the README is tied to the CHANGELOG and the
// integration skill by scripts/check_release_consistency.sh), so the release
// PR cannot bump the tag and leave the header naming the old version.
func TestVersionIsTheReleaseTag(t *testing.T) {
	readme, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	claims := regexp.MustCompile("`(v[0-9]+\\.[0-9]+\\.[0-9]+(?:[.-][0-9A-Za-z.-]+)?)` is the latest tag").FindAllSubmatch(readme, -1)
	if len(claims) != 1 {
		t.Fatalf("README.md has %d latest-tag claims, want exactly 1", len(claims))
	}
	if tag := string(claims[0][1]); "v"+Version != tag {
		t.Fatalf("Version is %q but the README's latest tag is %s: bump the constant in the release PR (no leading v, no build metadata)", Version, tag)
	}
}
