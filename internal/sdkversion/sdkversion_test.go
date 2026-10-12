package sdkversion

import (
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
