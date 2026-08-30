package main

import (
	"strings"
	"testing"
)

// TestEveryComponentIsMeasuredAsReceived is the second sweep this stack earned.
//
// ⚠ THE LENGTH IS EVIDENCE, AND TWO LENGTHS FOR ONE VALUE CONTRADICT EACH OTHER.
// Every component of a URI or a Set-Cookie is replaced by the number of characters
// it HELD — not by the width of its wire spelling, and not by the width of the
// recorder's own reversible escape. Five components have each been corrected for
// this separately: the query value, the path segment, the userinfo, the cookie
// value, the attribute value — and then the zone, the opaque payload and the
// cookie name arrived as new code inheriting none of it
// (shardpilot/shardpilot-go#85 review).
//
// So the population is a table, and a component added later is a row rather than a
// ninth instance of one finding.
func TestEveryComponentIsMeasuredAsReceived(t *testing.T) {
	// `%C3%A9` is one character; `\x00` escaped by the recorder is one byte.
	cases := []struct {
		what, line, want string
	}{
		{"query value", "Location: /cb?a=%C3%A9", "redacted-1-chars"},
		{"path segment", "Location: /%C3%A9/x", "redacted-1-chars"},
		{"userinfo", "Location: https://%C3%A9@e.example/cb", "redacted-1-chars"},
		{"opaque payload", "Location: https:%C3%A9", "redacted-1-chars"},
		// ⚠ THE ZONE ROW USES A FORM GO ACTUALLY PRODUCES. `%C3%A9` inside a
		// bracketed host is refused by `url.Parse` itself ("invalid URL escape"),
		// measured — so a row written that way asks for a case the parser will not
		// hand this program, and fails on a tree that is correct. `%30` is accepted
		// and decodes to `0`, so `eth%30` is four characters.
		{"IPv6 zone", "Location: https://[fe80::1%25eth%30]/cb", "redacted-4-chars"},
	}
	for _, c := range cases {
		t.Run(c.what, func(t *testing.T) {
			suppliedValues = nil
			got := stripMarks(redactTarget(c.line))
			if !strings.Contains(got, c.want) {
				t.Fatalf("%s was measured in its wire spelling: %q", c.what, got)
			}
		})
	}
	// ⚠ THE REASON PHRASE IS A COMPONENT TOO, and the sweep did not have a row for
	// it — which is why the review found that one and not this table
	// (shardpilot/shardpilot-go#85 review). A sweep's own population needs deriving
	// as much as the thing it sweeps.
	t.Run("reason phrase", func(t *testing.T) {
		suppliedValues = nil
		got := stripMarks(redactUnlessVerbatim("HTTP/1.1 599 " + escapeMarks(capturedMark)))
		if !strings.Contains(got, "<redacted, 1 chars>") {
			t.Fatalf("a reason phrase was measured in its escaped spelling: %q", got)
		}
	})

	for _, c := range []struct{ what, line, want string }{
		{"cookie name", "Set-Cookie: " + escapeMarks(capturedMark) + "=x", "redacted-1-chars"},
		{"cookie value", "Set-Cookie: sid=" + escapeMarks(capturedMark), "redacted-1-chars"},
		{"attribute value", "Set-Cookie: sid=x; Path=" + escapeMarks(capturedMark), "Path=redacted-1-chars"},
	} {
		t.Run(c.what, func(t *testing.T) {
			suppliedValues = nil
			got := stripMarks(redactSetCookie(c.line))
			if !strings.Contains(got, c.want) {
				t.Fatalf("%s was measured in its escaped spelling: %q", c.what, got)
			}
		})
	}
}
