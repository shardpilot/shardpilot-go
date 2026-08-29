package main

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// Each test below stands for one review finding on this program. They are here
// because the findings were all of one kind -- the record claiming something the
// bytes do not support -- and that kind is invisible to a run that succeeds.

func TestDropFramingStopsAtTheHeaderBlock(t *testing.T) {
	dump := "HTTP/1.1 200 OK\r\n" +
		"Content-Length: 42\r\n" +
		"\r\n" +
		"a diagnostic line the endpoint sent:\r\n" +
		"Content-Length: 42 was the value we saw\r\n"
	got := dropFraming(dump)
	if strings.Count(got, "X-Capture-Note") != 1 {
		t.Fatalf("framing cleanup escaped the header block: %q", got)
	}
	if !strings.Contains(got, "Content-Length: 42 was the value we saw") {
		t.Fatalf("a body line was rewritten by the recorder: %q", got)
	}
}

func TestFencedBlockDoesNotAlterTheMessage(t *testing.T) {
	req := "GET /x HTTP/1.1\r\nHost: h\r\n\r\n"
	got := fencedBlock(req)
	if !strings.Contains(got, req) {
		t.Fatalf("the message was altered inside the fence: %q", got)
	}
	// Compact JSON has no trailing newline, and the block must not invent one
	// INSIDE the fence -- with framing removed a parser would read it as payload.
	body := "HTTP/1.1 200 OK\r\n\r\n{\"a\":1}"
	gotB := fencedBlock(body)
	if !strings.Contains(gotB, body+"\n```") && !strings.Contains(gotB, body+"\n") {
		t.Fatalf("body not present verbatim: %q", gotB)
	}
	if !strings.Contains(gotB, "does not end with a newline") {
		t.Fatal("the manufactured separator was not disclosed to the reader")
	}
}

func TestGuardDoesNotRefuseALegalShortKeyInProse(t *testing.T) {
	suppliedValues = []string{"a"}
	t.Cleanup(func() { suppliedValues = nil })
	// The SDK accepts any non-empty experiment key. Ordinary report prose
	// contains the letter inside words, and that is not an occurrence.
	if err := assertNoLeak(asCaptured("# assignment capture — 2026-08-29\n")); err != nil {
		t.Fatalf("the guard refused a valid run over prose: %v", err)
	}
	// A whole-token occurrence is still a leak.
	if err := assertNoLeak(asCaptured("experiment_key=a&x=1")); err == nil {
		t.Fatal("the guard missed a short value standing as its own token")
	}
}

func TestGuardDecodesSurrogatePairs(t *testing.T) {
	suppliedValues = []string{"a\U0001F600b"}
	t.Cleanup(func() { suppliedValues = nil })
	esc := `{"k":"a` + "\\ud83d\\ude00" + `b"}`
	if err := assertNoLeak(asCaptured(esc)); err == nil {
		t.Fatalf("a surrogate-pair spelling was not decoded: %q", esc)
	}
}

func TestTrailerReportIsOutsideTheMessage(t *testing.T) {
	tee := &teeBody{trailer: http.Header{"X-Late": []string{"value"}}}
	ex := exchange{head: []byte("HTTP/1.1 200 OK\r\n\r\n"), captured: tee}
	if strings.Contains(string(ex.resp()), "X-Late") {
		t.Fatal("trailers were appended into the HTTP message body")
	}
	if !strings.Contains(ex.trailerReport(), "X-Late: value") {
		t.Fatal("trailers were dropped instead of being reported beside the message")
	}
}

func TestScrubCoversTheJSONUnicodeSpelling(t *testing.T) {
	suppliedValues = []string{"a<b"}
	t.Cleanup(func() { suppliedValues = nil })
	// encoding/json writes `<` as \u003c by default; strconv.Quote leaves it literal.
	body := `{"experiment_key":"a\u003cb"}`
	if !strings.Contains(body, `\u003c`) {
		t.Fatal("precondition: the fixture must carry the escape spelling, not the literal")
	}
	got := scrubSupplied(body)
	if strings.Contains(got, `a\u003cb`) {
		t.Fatalf("the JSON escape spelling survived: %q", got)
	}
}

func TestScrubCoversThePercentSpelling(t *testing.T) {
	suppliedValues = []string{`a"b`}
	t.Cleanup(func() { suppliedValues = nil })
	got := scrubSupplied("Location: /x?experiment_key=a%22b\r\n")
	if strings.Contains(got, "a%22b") {
		t.Fatalf("the percent spelling survived: %q", got)
	}
}

// The one that makes the list above safe to be incomplete.
func TestAssertNoLeakCatchesASpellingNobodyConstructed(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	// json.Marshal does not escape these characters, so encodingsOf never
	// produces this spelling and scrubSupplied cannot match it.
	hidden := `{"k":"\u0061bcdefgh"}`
	if scrubSupplied(hidden) != hidden {
		t.Fatalf("precondition failed: the scrub was expected to miss this spelling")
	}
	if err := assertNoLeak(asCaptured(hidden)); err == nil {
		t.Fatal("assertNoLeak passed a value it should have decoded and caught")
	}
	if err := assertNoLeak(asCaptured(`{"k":"nothing here"}`)); err != nil {
		t.Fatalf("assertNoLeak refused a clean artifact: %v", err)
	}
}

func TestCeilingWithAConclusiveEOFIsComplete(t *testing.T) {
	// Exactly the ceiling, delivered with io.EOF: the body IS whole, and the
	// SDK's refusal of an oversized response is a complete refusal.
	src := bytes.Repeat([]byte("x"), capturedBodyMax)
	tee := &teeBody{inner: io.NopCloser(bytes.NewReader(src))}
	if _, err := io.Copy(io.Discard, tee); err != nil {
		t.Fatalf("copy: %v", err)
	}
	if !tee.sawEOF {
		t.Fatal("precondition: the reader did not deliver EOF")
	}
	if (&exchange{head: []byte("x"), captured: tee}).truncErr() != nil {
		t.Fatal("a ceiling-sized body confirmed by EOF was reported incomplete")
	}
}

func TestCeilingWithoutEOFIsIndeterminate(t *testing.T) {
	// The SDK reads through io.LimitReader, so the wrapper stops at the ceiling
	// with more bytes behind it and never sees EOF. Nothing here can tell a
	// whole body from a truncated one.
	src := bytes.Repeat([]byte("x"), capturedBodyMax+512)
	tee := &teeBody{inner: io.NopCloser(bytes.NewReader(src))}
	if _, err := io.CopyN(io.Discard, tee, int64(capturedBodyMax)); err != nil {
		t.Fatalf("copyN: %v", err)
	}
	if tee.sawEOF {
		t.Fatal("precondition: EOF was seen, which is not this case")
	}
	if (&exchange{head: []byte("x"), captured: tee}).truncErr() == nil {
		t.Fatal("a ceiling-sized body with no EOF was reported complete")
	}
	short := &teeBody{inner: io.NopCloser(bytes.NewReader([]byte("ok")))}
	_, _ = io.Copy(io.Discard, short)
	if (&exchange{head: []byte("x"), captured: short}).truncErr() != nil {
		t.Fatal("a short body was reported incomplete")
	}
}

func TestGuardDecodesNestedPercentEscapes(t *testing.T) {
	suppliedValues = []string{`a"b`}
	t.Cleanup(func() { suppliedValues = nil })
	// A URL embedded in another URL's parameter encodes the identifier twice.
	if err := assertNoLeak(asCaptured("Location: /r?next=%2Fx%3Fexperiment_key%3Da%2522b")); err == nil {
		t.Fatal("a doubly percent-encoded identifier passed the guard")
	}
}

func TestGuardIgnoresItsOwnPlaceholders(t *testing.T) {
	suppliedValues = []string{"redacted"}
	t.Cleanup(func() { suppliedValues = nil })
	// Built by the REAL generators, not by hand: a placeholder is distinguished
	// from captured text by provenance now, so a hand-written copy of one would
	// be testing the wrong thing.
	report := redactQuery("GET /x?experiment_key=redacted HTTP/1.1") +
		"\n" + scrubSupplied("echoed: redacted")
	if err := assertNoLeak(asCaptured(report)); err != nil {
		t.Fatalf("the guard rejected a fully redacted report: %v", err)
	}
	if err := assertNoLeak(asCaptured("experiment_key=redacted&x=1")); err == nil {
		t.Fatal("the guard missed a real occurrence outside a placeholder")
	}
}

func TestEveryQueryValueTheSDKSendsIsScrubbed(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	u, err := url.Parse("https://h/a?subject_key=spcid_abababababababababababababababab&app_key=k")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for _, vs := range u.Query() {
		for _, v := range vs {
			addSuppliedValue(v)
		}
	}
	// The minted subject is not an environment value, and it is what a redirect
	// Location echoes back.
	got := scrubSupplied("Location: /r?subject_key=spcid_abababababababababababababababab")
	if strings.Contains(got, "spcid_abababababababababababababababab") {
		t.Fatalf("the SDK-minted subject was published verbatim: %q", got)
	}
}

func TestSetCookieIsRedactedStructurally(t *testing.T) {
	dump := "HTTP/1.1 200 OK\r\n" +
		"Set-Cookie: sid=abc123def456; Path=/; HttpOnly\r\n" +
		"\r\n{}"
	got := dropFraming(dump)
	if strings.Contains(got, "abc123def456") {
		t.Fatalf("a server-set cookie was published verbatim: %q", got)
	}
	for _, keep := range []string{"sid=", "Path=/", "HttpOnly"} {
		if !strings.Contains(got, keep) {
			t.Fatalf("structural redaction dropped %q: %q", keep, got)
		}
	}
}

func TestGuardDecodesPlusAsSpace(t *testing.T) {
	suppliedValues = []string{"a b"}
	t.Cleanup(func() { suppliedValues = nil })
	// A URL nested in another URL's query: the inner `+` is itself encoded.
	if err := assertNoLeak(asCaptured("Location: /r?next=%3Fexperiment_key%3Da%2Bb")); err == nil {
		t.Fatal("a nested query-plus spelling passed the guard")
	}
}

func TestGuardHasNoFixedDecodingDepth(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	// Wrap the percent-escape in many %25 layers -- more than any fixed cap.
	v := "%61bcdefgh"
	for i := 0; i < 40; i++ {
		v = strings.ReplaceAll(v, "%", "%25")
	}
	// ⚠ ASSERT THE REASON, NOT MERELY AN ERROR. The first version of this test
	// checked only `err != nil`, and the mutant that restores the 16-round cap
	// SURVIVED it: that version also errors, but with "did not settle" rather
	// than by finding the value. Two different verdicts, one indistinguishable
	// assertion.
	err := assertNoLeak(asCaptured(v))
	if err == nil {
		t.Fatal("a deeply nested encoding walked through the guard")
	}
	if !strings.Contains(err.Error(), "survived redaction") {
		t.Fatalf("the guard refused for the wrong reason -- it did not reach the "+
			"value, it ran out of rounds: %v", err)
	}
}

func TestTrailerNamesAreScrubbedToo(t *testing.T) {
	suppliedValues = []string{"secret"}
	t.Cleanup(func() { suppliedValues = nil })
	tee := &teeBody{trailer: http.Header{"X-secret": []string{"v"}}}
	got := (&exchange{head: []byte("x"), captured: tee}).trailerReport()
	if strings.Contains(got, "secret") {
		t.Fatalf("the identifier was published in a trailer NAME: %q", got)
	}
}

func TestRedactedAuthorizationKeepsItsTerminator(t *testing.T) {
	dump := []byte("GET /x HTTP/1.1\r\nAuthorization: Bearer tok\r\nHost: h\r\n\r\n")
	got := string(redact(dump))
	for _, line := range strings.Split(got, "\n") {
		if line == "" {
			continue
		}
		if !strings.HasSuffix(line, "\r") {
			t.Fatalf("line lost its CR, giving mixed endings: %q in %q", line, got)
		}
	}
}

func TestGuardDecodesHTMLEntities(t *testing.T) {
	suppliedValues = []string{"a&b"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("<p>rejected key a&amp;b</p>")); err == nil {
		t.Fatal("an HTML entity spelling passed the guard")
	}
}

func TestGuardDoesNotMaskAValueShapedLikeAPlaceholder(t *testing.T) {
	suppliedValues = []string{"redacted-38-chars"}
	t.Cleanup(func() { suppliedValues = nil })
	// A legal experiment key that happens to look like a generated placeholder.
	if err := assertNoLeak(asCaptured("GET /x?experiment_key=redacted-38-chars HTTP/1.1")); err == nil {
		t.Fatal("the mask swallowed the very value it was protecting")
	}
}

func TestResponseHeaderNamesAreScrubbed(t *testing.T) {
	suppliedValues = []string{"Secret"}
	t.Cleanup(func() { suppliedValues = nil })
	got := dropFraming("HTTP/1.1 200 OK\r\nX-Secret: value\r\n\r\n{}")
	if strings.Contains(got, "X-Secret") {
		t.Fatalf("an identifier was published in a response header NAME: %q", got)
	}
}

func TestLocationQueryValuesAreRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := dropFraming("HTTP/1.1 302 Found\r\nLocation: /cb?state=abc123&token=zzz\r\n\r\n")
	for _, leaked := range []string{"abc123", "zzz"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("a server-generated redirect value was published: %q", got)
		}
	}
	if !strings.Contains(got, "state=") || !strings.Contains(got, "token=") {
		t.Fatalf("structural redaction dropped the parameter names: %q", got)
	}
}

func TestReplacedFramingHeadersKeepTheirTerminator(t *testing.T) {
	got := dropFraming("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n{}")
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "X-Capture-Note") && !strings.HasSuffix(line, "\r") {
			t.Fatalf("a replacement line lost its CR: %q in %q", line, got)
		}
	}
}

func TestCookieLengthIsTheCookiesNotThePlaceholders(t *testing.T) {
	suppliedValues = []string{"abc12345"}
	t.Cleanup(func() { suppliedValues = nil })
	// The cookie value equals a supplied identifier: the structural redactor must
	// see the cookie, not a substitution made before it.
	ex := exchange{head: []byte("HTTP/1.1 200 OK\r\nSet-Cookie: sid=abc12345; Path=/\r\n\r\n")}
	got := responseText(&ex)
	if !strings.Contains(got, "<redacted, 8 chars>") {
		t.Fatalf("the cookie's own length was not reported: %q", got)
	}
}

func TestCeilingWithAnExactContentLengthIsComplete(t *testing.T) {
	// Production's outer io.LimitReader can synthesise EOF without calling the
	// tee again, so sawEOF stays false on a complete body.
	src := bytes.Repeat([]byte("x"), capturedBodyMax)
	tee := &teeBody{inner: io.NopCloser(bytes.NewReader(src)), declared: int64(capturedBodyMax)}
	if _, err := io.CopyN(io.Discard, tee, int64(capturedBodyMax)); err != nil {
		t.Fatalf("copyN: %v", err)
	}
	if tee.sawEOF {
		t.Fatal("precondition: this case is about NOT seeing EOF")
	}
	if (&exchange{head: []byte("x"), captured: tee}).truncErr() != nil {
		t.Fatal("a complete body with an exact Content-Length was called incomplete")
	}
}

func TestHyphenatedIdentifierInAHeaderName(t *testing.T) {
	suppliedValues = []string{"foo-bar"}
	t.Cleanup(func() { suppliedValues = nil })
	if got := scrubHeaderName("X-foo-bar"); strings.Contains(got, "foo-bar") {
		t.Fatalf("a hyphenated identifier survived the name scrub: %q", got)
	}
	// And the guard must see it too, under the name convention.
	if err := assertNoLeak(asCaptured("X-foo-bar: value")); err == nil {
		t.Fatal("the guard missed a hyphenated identifier in a header name")
	}
}

func TestGuardDecodesHexEscapes(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	hidden := `{"k":"\\x61bcdefgh"}`
	if scrubSupplied(hidden) != hidden {
		t.Fatal("precondition: the scrub is expected to miss this spelling")
	}
	err := assertNoLeak(asCaptured(hidden))
	if err == nil {
		t.Fatal("a hex-escape spelling passed the guard")
	}
	if !strings.Contains(err.Error(), "survived redaction") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

func TestQueryLengthIsTheValueNotItsWireSpelling(t *testing.T) {
	got := redactQuery(`GET /x?experiment_key=a%22b HTTP/1.1`)
	if !strings.Contains(got, "redacted-3-chars") {
		t.Fatalf("the encoded length was reported instead of the value's: %q", got)
	}
}

func TestAValueShapedLikeAPlaceholderStillPublishes(t *testing.T) {
	// The mirror of the collision case: a legal key that looks like a placeholder
	// must neither be swallowed by the mask NOR make every real placeholder read
	// as a leak. Both directions in one fixture.
	suppliedValues = []string{"redacted-38-chars"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("GET /x?experiment_key=redacted-38-chars HTTP/1.1")); err == nil {
		t.Fatal("the mask swallowed the value it was protecting")
	}
	clean := redactQuery("GET /x?subject_key=ababababababababababababababababababab HTTP/1.1")
	if err := assertNoLeak(asCaptured(clean)); err != nil {
		t.Fatalf("a generated placeholder was read as a leak: %v", err)
	}
}

func TestLongestSuppliedValueWins(t *testing.T) {
	suppliedValues = []string{"abcdefgh", "abcdefghi"}
	t.Cleanup(func() { suppliedValues = nil })
	got := scrubSupplied("echoed abcdefghi here")
	if strings.Contains(got, "i here") && strings.Contains(got, "8 chars") {
		t.Fatalf("the shorter value destroyed the longer match: %q", got)
	}
	if !strings.Contains(got, "9 chars") {
		t.Fatalf("the longer value was not redacted as itself: %q", got)
	}
}

func TestFragmentCredentialsAreRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := dropFraming("HTTP/1.1 302 Found\r\nLocation: https://c.example/cb#access_token=secretvalue\r\n\r\n")
	if strings.Contains(got, "secretvalue") {
		t.Fatalf("a fragment credential was published: %q", got)
	}
	if !strings.Contains(got, "access_token=") {
		t.Fatalf("structural redaction dropped the parameter name: %q", got)
	}
}

func TestQueryLengthCountsCharactersNotBytes(t *testing.T) {
	got := redactQuery("GET /x?experiment_key=%C3%A9 HTTP/1.1")
	if !strings.Contains(got, "redacted-1-chars") {
		t.Fatalf("a one-character value was measured in bytes: %q", got)
	}
}

func TestGeneratedProseIsNotCheckedForLeaks(t *testing.T) {
	suppliedValues = []string{"assignment"}
	t.Cleanup(func() { suppliedValues = nil })
	// The recorder's own heading contains the word; only captured spans are in
	// question, and prose carries no mark at all.
	if err := assertNoLeak("# assignment capture — 2026-08-29\n"); err != nil {
		t.Fatalf("the guard read its own prose as captured content: %v", err)
	}
	if err := assertNoLeak(asCaptured("experiment_key=assignment&x=1")); err == nil {
		t.Fatal("the guard missed a real occurrence inside captured text")
	}
}

func TestCapturedNULIsEscapedNotDeleted(t *testing.T) {
	ex := exchange{head: []byte("HTTP/1.1 200 OK\r\n\r\n"),
		captured: &teeBody{buf: *bytes.NewBufferString("a\x00b")}}
	got := responseText(&ex)
	if strings.Contains(stripMarks(got), "ab") {
		t.Fatalf("a captured NUL was deleted, joining separated bytes: %q", got)
	}
	if !strings.Contains(got, `\x00`) {
		t.Fatalf("a captured NUL was not disclosed: %q", got)
	}
}

func TestHeaderNamePlaceholderIsTokenSafe(t *testing.T) {
	suppliedValues = []string{"secret"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubHeaderName("X-secret"))
	for _, bad := range []string{" ", ",", "<", ">"} {
		if strings.Contains(got, bad) {
			t.Fatalf("a header name got a non-token placeholder %q: %q", bad, got)
		}
	}
	if strings.Contains(got, "secret") {
		t.Fatalf("the identifier survived: %q", got)
	}
}

func TestPercentDecodedFormIsCheckedBeforePlus(t *testing.T) {
	suppliedValues = []string{"abcdefghi+j"}
	t.Cleanup(func() { suppliedValues = nil })
	// Percent-decoding reconstructs the value; undoPlus would then destroy it.
	if err := assertNoLeak(asCaptured("k=%61bcdefghi%2Bj")); err == nil {
		t.Fatal("the intermediate percent-decoded form was never checked")
	}
}

func TestLocationUserinfoIsRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(dropFraming("HTTP/1.1 302 Found\r\nLocation: https://user:secret@e.example/cb\r\n\r\n"))
	if strings.Contains(got, "secret") || strings.Contains(got, "user:") {
		t.Fatalf("userinfo credentials were published: %q", got)
	}
	// ⚠ THIS ASSERTION WAS NARROWED, DELIBERATELY. It used to require
	// `e.example/cb` verbatim, which now contradicts a later finding: a redirect
	// PATH segment is server-generated and can carry a credential
	// (`/reset/<token>`), so segments are redacted too. What this test actually
	// protects -- its own failure message says so -- is that the target is not
	// DESTROYED: the host and the path structure must survive. That is what it
	// asks now, and the segment is checked to be lengthened rather than dropped.
	if !strings.Contains(got, "e.example/") {
		t.Fatalf("the redirect target itself was destroyed: %q", got)
	}
	if !strings.Contains(got, "e.example/redacted-2-chars") {
		t.Fatalf("the path segment was dropped rather than lengthened: %q", got)
	}
}

func TestSchemeRelativeUserinfoIsRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(dropFraming("HTTP/1.1 302 Found\r\nLocation: //user:secret@e.example/cb\r\n\r\n"))
	if strings.Contains(got, "secret") {
		t.Fatalf("userinfo in a network-path reference was published: %q", got)
	}
}

func TestTrailerContentIsInsideACapturedSpan(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	tee := &teeBody{trailer: http.Header{"X-Late": []string{`\\x61bcdefgh`}}}
	ex := exchange{head: []byte("x"), captured: tee}
	if err := assertNoLeak(ex.trailerReport()); err == nil {
		t.Fatal("a trailer's content was never read by the guard")
	}
}

func TestResponsePlaceholderCountsRunes(t *testing.T) {
	suppliedValues = []string{"é"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied("echoed é here"))
	if !strings.Contains(got, "1 chars") {
		t.Fatalf("a one-character value was measured in bytes: %q", got)
	}
}

func TestEveryPlaceholderCountsCharacters(t *testing.T) {
	// One measure, one place: this asserts the PROPERTY across every surface
	// that produces a placeholder, so a new one measuring bytes fails here
	// rather than in a review round two days later.
	suppliedValues = []string{"éé"}
	t.Cleanup(func() { suppliedValues = nil })
	for name, got := range map[string]string{
		"value":         stripMarks(scrubSupplied("body éé here")),
		"header name":   stripMarks(scrubHeaderName("X-éé")),
		"query":         stripMarks(redactQuery("GET /x?k=%C3%A9%C3%A9 HTTP/1.1")),
		"cookie":        stripMarks(redactSetCookie("Set-Cookie: s=éé; Path=/")),
		"userinfo":      stripMarks(redactUserinfo("Location: https://éé@h/cb")),
		"fragment":      stripMarks(redactFragment("Location: /cb#éé")),
		"authorization": stripMarks(string(redact([]byte("GET / HTTP/1.1\r\nAuthorization: Bearer éé\r\n\r\n")))),
	} {
		// Two placeholder shapes, one measure: `<redacted, N chars>` and
		// `redacted-N-chars`. The property is the NUMBER, not the spelling.
		if strings.Contains(got, "4 chars") || strings.Contains(got, "4-chars") {
			t.Errorf("%s measured bytes, not characters: %q", name, got)
		}
		if !strings.Contains(got, "2 chars") && !strings.Contains(got, "2-chars") {
			t.Errorf("%s did not report 2 characters: %q", name, got)
		}
	}
}

func TestOpaqueQueryComponentIsRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(dropFraming("HTTP/1.1 302 Found\r\nLocation: /cb?server-secret-token\r\n\r\n"))
	if strings.Contains(got, "server-secret-token") {
		t.Fatalf("a key-only query component was published: %q", got)
	}
}

func TestScrubDoesNotReachInsideGeneratedText(t *testing.T) {
	suppliedValues = []string{"redacted"}
	t.Cleanup(func() { suppliedValues = nil })
	// dropFraming writes a marked placeholder; the general scrub must leave it
	// alone, or the marks nest and the guard reports a survivor that is its own
	// output.
	body := dropFraming("HTTP/1.1 302 Found\r\nLocation: /cb?token=abc\r\n\r\n")
	out := scrubSupplied(body)
	if err := assertNoLeak(asCaptured(out)); err != nil {
		t.Fatalf("the guard flagged its own placeholder: %v (%q)", err, stripMarks(out))
	}
	if strings.Contains(stripMarks(out), "-3-chars") && strings.Contains(stripMarks(out), "chars>-") {
		t.Fatalf("a placeholder was scrubbed inside itself: %q", stripMarks(out))
	}
}

func TestGuardDecodesBraceFormUnicodeEscapes(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	hidden := `{"k":"\\u{61}bcdefgh"}`
	err := assertNoLeak(asCaptured(hidden))
	if err == nil {
		t.Fatal("a brace-form escape passed the guard")
	}
	if !strings.Contains(err.Error(), "survived redaction") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

func TestLegalTokenHeaderNamesAreRecognised(t *testing.T) {
	suppliedValues = []string{"Secret"}
	t.Cleanup(func() { suppliedValues = nil })
	for _, name := range []string{"X.Secret", "X+Secret", "X^Secret", "X-Secret"} {
		got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\n" + name + ": v\r\n\r\n"))
		if strings.Contains(got, "Secret") {
			t.Errorf("%s: the identifier survived: %q", name, got)
		}
		// The NAME is the part of the HEADER line before its colon -- not of the
		// whole dump, whose status line carries spaces of its own. The first
		// version of this assertion split the dump and failed on correct output.
		var field string
		for _, ln := range strings.Split(got, "\r\n") {
			if i := strings.IndexByte(ln, ':'); i > 0 && !strings.HasPrefix(ln, "HTTP/") {
				field = ln[:i]
				break
			}
		}
		if field == "" {
			t.Errorf("%s: no header line survived: %q", name, got)
			continue
		}
		for j := 0; j < len(field); j++ {
			if !isTokenByte(field[j]) {
				t.Errorf("%s: %q is not a legal field name (byte %q)", name, field, field[j])
				break
			}
		}
	}
}

func TestTrailersAreSnapshotAtEOFNotFromTheHead(t *testing.T) {
	resp := &http.Response{Trailer: http.Header{}}
	tee := &teeBody{inner: io.NopCloser(strings.NewReader("BODY")), resp: resp}
	buf := make([]byte, 8)
	for {
		_, err := tee.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		// The values arrive with the last chunk, not with the head, so a
		// snapshot taken when the head was dumped would see nothing.
		resp.Trailer.Set("X-Late", "value")
	}
	ex := exchange{head: []byte("HTTP/1.1 200 OK\r\nTrailer: X-Late\r\n\r\n"), captured: tee}
	if !strings.Contains(ex.trailerReport(), "X-Late: value") {
		t.Fatal("a declared trailer was announced and then omitted")
	}
	if strings.Contains(string(ex.resp()), "X-Late: value") {
		t.Fatal("the trailer was written into the HTTP message instead of beside it")
	}
}

// ---- round on c0fbd19: five findings, one property each ----

func TestOpaqueFragmentPrefixWithQuestionMarkIsRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(dropFraming(
		"HTTP/1.1 302 Found\r\nLocation: /cb#server-secret?x=y\r\n\r\n"))
	if strings.Contains(got, "server-secret") {
		t.Fatalf("an opaque fragment prefix was published: %q", got)
	}
}

func TestNameBoundariesDoNotReachOrdinaryBodyText(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil })
	body := `{"reason":"foo-bar-baz"}`
	if err := assertNoLeak(asCaptured(body)); err != nil {
		t.Fatalf("ordinary hyphenated body text was refused: %v", err)
	}
}

func TestGuardDecodesBase64(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	hidden := `{"k":"YWJjZGVmZ2g="}`
	err := assertNoLeak(asCaptured(hidden))
	if err == nil {
		t.Fatal("a base64-encoded identifier passed the guard")
	}
	if !strings.Contains(err.Error(), "survived redaction") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

func TestTrailersGetStructuralRedaction(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	tee := &teeBody{trailer: http.Header{
		"Set-Cookie": []string{"sid=server-secret"},
		"Location":   []string{"/cb?state=server-state"},
	}}
	e := exchange{head: []byte("x"), captured: tee}
	got := stripMarks(e.trailerReport())
	for _, leak := range []string{"server-secret", "server-state"} {
		if strings.Contains(got, leak) {
			t.Errorf("a trailer published %q: %q", leak, got)
		}
	}
}

func TestServerMintedSubjectFactKeyIsRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	// ⚠ THE VALUE REPEATS A SHORT PATTERN, AND THAT IS LOAD-BEARING. A fixture
	// needs a value's SHAPE -- here `sfk1_` and sixteen following characters --
	// never its randomness: gitleaks' `generic-api-key` fires on a key-like NAME
	// followed by a value with enough entropy, and this file has now reddened the
	// whole repository twice over exactly that. Length is kept so the
	// length-preserving placeholder still has something to preserve; entropy is
	// not (shardpilot/shardpilot-go#73 review).
	body := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n" +
		`{"assigned":true,"subject_fact_key":"sfk1_abababababababab"}`
	got := stripMarks(dropFraming(body))
	if strings.Contains(got, "sfk1_abababababababab") {
		t.Fatalf("the server-minted subject key was published: %q", got)
	}
}

// A Location carrying BOTH a query and a fragment is where the composition
// order is load-bearing on its own. Composed the other way round, redactQuery
// cuts at the first `?`, swallows the whole `#fragment` into the last parameter
// VALUE and reports one length for the two of them -- the fragment vanishes as a
// structure and the number printed describes neither part. The `?`-opaque rule
// cannot see this: there is no `?` inside the fragment to make it opaque.
func TestQueryAndFragmentAreMeasuredSeparately(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(dropFraming(
		"HTTP/1.1 302 Found\r\nLocation: /cb?a=b#frag-only\r\n\r\n"))
	if !strings.Contains(got, "a=redacted-1-chars") {
		t.Errorf("the query value was not measured on its own: %q", got)
	}
	if !strings.Contains(got, "#redacted-9-chars") {
		t.Errorf("the fragment was not redacted as a fragment: %q", got)
	}
	if strings.Contains(got, "frag-only") {
		t.Errorf("the fragment was published: %q", got)
	}
}

// ---- round on 45eb421: eight findings ----

func TestMintedKeyIsRedactedAcrossALineBreak(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	body := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n" +
		"{\"assigned\":true,\"subject_fact_key\":\n\"sfk1_abababababababab\"}"
	got := stripMarks(dropFraming(body))
	if strings.Contains(got, "sfk1_abababababababab") {
		t.Fatalf("a minted key split across lines was published: %q", got)
	}
}

func TestMintedKeyPlaceholderIsNotDoubleMarked(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	body := "HTTP/1.1 200 OK\r\n\r\n" + `{"subject_fact_key":"sfk1_abababababababab"}`
	out := scrubSupplied(dropFraming(body))
	if strings.Contains(stripMarks(out), "chars>,") {
		t.Fatalf("the placeholder was rewritten inside itself: %q", stripMarks(out))
	}
	if strings.Contains(out, genMark+genMark) {
		t.Fatalf("adjacent nested provenance marks: %q", out)
	}
}

func TestGuardDecodesShortBase64Tokens(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured(`{"k":"YmFy"}`)); err == nil {
		t.Fatal("a four-byte base64 token passed the guard")
	}
}

func TestRedirectPathSegmentsAreRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(dropFraming(
		"HTTP/1.1 302 Found\r\nLocation: https://e.example/reset/server-secret\r\n\r\n"))
	if strings.Contains(got, "server-secret") {
		t.Fatalf("a credential in a redirect path was published: %q", got)
	}
	if !strings.Contains(got, "e.example/") {
		t.Fatalf("the host was destroyed with it: %q", got)
	}
}

func TestMarkEscapingIsInjective(t *testing.T) {
	fromWire := escapeMarks("a" + capturedMark + "b")
	literal := escapeMarks(`a\x00b`)
	if fromWire == literal {
		t.Fatalf("a real marker byte and its literal spelling render alike: %q", fromWire)
	}
}

func TestSuppliedValueInACanonicalisedHeaderNameIsScrubbed(t *testing.T) {
	suppliedValues = []string{"secret"}
	t.Cleanup(func() { suppliedValues = nil })
	// net/http canonicalises `X-secret` to `X-Secret` before the dump.
	got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\nX-Secret: v\r\n\r\n"))
	if strings.Contains(got, "Secret") {
		t.Fatalf("a folded supplied identifier survived in a field name: %q", got)
	}
}

func TestEncodedFormPlaceholderMeasuresTheValue(t *testing.T) {
	suppliedValues = []string{`a"b`}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(`{"k":"a\"b"}`))
	if strings.Contains(got, "4 chars") {
		t.Fatalf("the placeholder measured the encoding, not the value: %q", got)
	}
	if !strings.Contains(got, "3 chars") {
		t.Fatalf("the placeholder did not measure the value: %q", got)
	}
}

func TestReportDoesNotClaimTheBodyIsAsReceived(t *testing.T) {
	if strings.Contains(respSection, "status line and body are as received") {
		t.Fatal("the report claims as-received bytes for a body it redacts")
	}
	if !strings.Contains(respSection, "REDACTED capture") {
		t.Fatal("the report does not say the body is redacted")
	}
}
