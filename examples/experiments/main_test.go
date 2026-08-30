package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
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
	// ⚠ THE VALUE IS A LENGTH NOW, NOT `value`. A trailer field is not in the
	// verbatim vocabulary, so its value is lengthened exactly as a header's would
	// be -- a trailer is a header that arrived late. What this test protects is
	// that the FIELD is reported beside the message rather than dropped.
	// ⚠ THE NAME IS LENGTHENED TOO NOW. `X-Late` is not in the field-name
	// registry, so it is endpoint-chosen text like any other -- the criterion
	// reaches trailer names as it reaches header names. What this protects is
	// that the field is REPORTED beside the message rather than dropped.
	if !strings.Contains(ex.trailerReport(), ": ") {
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
	got := string(redact(dump, http.Header{"Host": nil}))
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

func TestReplacedFramingHeadersKeepTheirTerminator(t *testing.T) {
	got := dropFraming("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n{}")
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "X-Capture-Note") && !strings.HasSuffix(line, "\r") {
			t.Fatalf("a replacement line lost its CR: %q in %q", line, got)
		}
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
	// ⚠ THIS ASSERTION WAS INVERTED BY A LATER CHANGE, AND THAT IS AN IMPROVEMENT
	// RATHER THAN A LOSS. It used to require that the GUARD catch an encoded
	// supplied value in a trailer, which is a statement about the last line of
	// defence. Trailer values are now lengthened before the guard ever sees them,
	// so there is nothing left for it to catch -- the property is enforced
	// earlier and more strongly. What the test asks now is the property itself:
	// no spelling of a supplied value reaches the trailer report at all.
	got := stripMarks(ex.trailerReport())
	if strings.Contains(got, "bcdefgh") {
		t.Fatalf("a supplied value survived into the trailer report: %q", got)
	}
	if err := assertNoLeak(ex.trailerReport()); err != nil {
		t.Fatalf("the guard flagged an already-lengthened trailer: %v", err)
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
	// The value is lengthened; what must not happen is the field being announced
	// in the head and then absent from the report.
	if !strings.Contains(ex.trailerReport(), ": ") {
		t.Fatal("a declared trailer was announced and then omitted")
	}
	if strings.Contains(string(ex.resp()), "X-Late: value") {
		t.Fatal("the trailer was written into the HTTP message instead of beside it")
	}
}

// ---- round on c0fbd19: five findings, one property each ----

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

func TestGuardDecodesShortBase64Tokens(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured(`{"k":"YmFy"}`)); err == nil {
		t.Fatal("a four-byte base64 token passed the guard")
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

// ---- round on 0dc23aa: six findings ----

func TestEscapingSeparatesABackslashNextToAMarker(t *testing.T) {
	adjacent := escapeMarks(`\` + capturedMark) // a backslash then a REAL marker byte
	literal := escapeMarks(`\` + `\x00`)        // a backslash then the wire spelling
	if adjacent == literal {
		t.Fatalf("a backslash beside a marker collides with its literal form: %q", adjacent)
	}
	if bare := escapeMarks(capturedMark); bare == adjacent || bare == literal {
		t.Fatalf("a bare marker collides with an escaped form: %q", bare)
	}
	// And an ordinary escape the guard's decoders rely on is untouched.
	if got := escapeMarks(`a`); got != `a` {
		t.Fatalf("an unrelated escape was rewritten: %q", got)
	}
}

func TestGuardDecodesBareHexadecimal(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured(`{"k":"6162636465666768"}`)); err == nil {
		t.Fatal("a bare hexadecimal identifier passed the guard")
	}
}

func TestDecodeBudgetChargesEveryStage(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	// ~1 MiB of filler plus an escape nested deeply enough that the rounds
	// needed, times the FIVE decoders each round runs, exceeds the budget --
	// while one charge per round would stay well inside it.
	// ⚠ NESTED, NOT REPEATED. `%25%25%25…` all peels in ONE pass, because
	// undoPercent decodes each escape independently; the nesting that costs a
	// round each is in the encoding of the percent SIGN -- `%2525` -> `%25` -> `%`.
	// And the payload decodes to something harmless, so only the budget can fire.
	nested := "%" + strings.Repeat("25", 20) + "41"
	body := nested + strings.Repeat("f", 1<<20)
	err := assertNoLeak(asCaptured(body))
	if err == nil || !strings.Contains(err.Error(), "work budget") {
		t.Fatalf("the work budget did not fire: %v", err)
	}
}

func TestStatusLineIsNotRewrittenByTheValueScrub(t *testing.T) {
	suppliedValues = []string{"200"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied("HTTP/1.1 200 OK\r\n\r\nbody 200 here"))
	if !strings.HasPrefix(got, "HTTP/1.1 200 OK") {
		t.Fatalf("the status line was rewritten into something unparsable: %q", got)
	}
	if strings.Contains(got, "body 200 here") {
		t.Fatalf("the BODY occurrence was left unredacted: %q", got)
	}
}

// ---- round on 51eeaaf: three findings ----

func TestReportDoesNotClaimTheStatusLineIsReceived(t *testing.T) {
	if strings.Contains(respSection, "status line is as received") {
		t.Fatal("the status line is synthesised by Response.Write; the report claims otherwise")
	}
	if !strings.Contains(respSection, "CANONICAL") {
		t.Fatal("the report does not label the status line as a representation")
	}
}

// ---- what this half REFUSES, which is its half of the contract ----

func TestProtocolSyntaxIsNotReadAsCapturedData(t *testing.T) {
	for _, v := range []string{"GET", "200"} {
		suppliedValues = []string{v}
		dump := asCaptured("GET /v1/assign HTTP/1.1\r\nHost: e.example\r\n") +
			asCaptured("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nbody\r\n")
		if err := assertNoLeak(dump); err != nil {
			t.Errorf("supplied %q: protocol syntax was read as a leak: %v", v, err)
		}
		suppliedValues = nil
	}
	// But the same token in the request TARGET is data, and must still be caught.
	suppliedValues = []string{"200"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("GET /v1/200 HTTP/1.1\r\n")); err == nil {
		t.Fatal("the request target was dropped along with the syntax")
	}
}

func TestABodyLineShapedLikeAHeaderIsNotAFieldName(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil })
	dump := asCaptured("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nX-foo-bar: explanation\r\n")
	if err := assertNoLeak(dump); err != nil {
		t.Fatalf("a header-shaped BODY line was read as a field name: %v", err)
	}
	// And a real header name still is one.
	if err := assertNoLeak(asCaptured("HTTP/1.1 200 OK\r\nX-foo-bar: v\r\n\r\n")); err == nil {
		t.Fatal("a genuine field name stopped being checked under the name rule")
	}
}

// ---- round on 6b28540 ----

func TestFieldNamesAreDecodedBeforeTheNameBoundaryApplies(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("HTTP/1.1 200 OK\r\nX-%62ar: v\r\n\r\n")); err == nil {
		t.Fatal("an escaped field name hid a supplied value from the name rule")
	}
}

func TestTheReasonPhraseIsCheckedLikeData(t *testing.T) {
	suppliedValues = []string{"secret99"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("HTTP/1.1 400 secret99\r\n\r\n")); err == nil {
		t.Fatal("a custom reason phrase was exempted along with the status syntax")
	}
	// The version and code are still syntax, not data.
	suppliedValues = []string{"400"}
	if err := assertNoLeak(asCaptured("HTTP/1.1 400 Bad Request\r\n\r\n")); err != nil {
		t.Fatalf("the numeric code was read as data: %v", err)
	}
}

func TestGuardDecodesEightDigitUnicodeEscapes(t *testing.T) {
	suppliedValues = []string{"a\U0001F600b"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured(`{"k":"a\U0001F600b"}`)); err == nil {
		t.Fatal("an eight-digit escape passed the guard")
	}
}

func TestGuardDecodesMimeWrappedBase64(t *testing.T) {
	suppliedValues = []string{"abcdefghijklmnopqrstuvwxyz012345678901234567890123456789"}
	t.Cleanup(func() { suppliedValues = nil })
	enc := base64.StdEncoding.EncodeToString([]byte(suppliedValues[0]))
	wrapped := enc[:40] + "\r\n" + enc[40:]
	if err := assertNoLeak(asCaptured("body:\r\n" + wrapped + "\r\n")); err == nil {
		t.Fatal("a MIME-wrapped base64 value passed the guard")
	}
}

func TestTrailersAreSnapshotOnCloseToo(t *testing.T) {
	resp := &http.Response{Trailer: http.Header{"X-Late": []string{"v"}}}
	tee := &teeBody{inner: io.NopCloser(strings.NewReader("")), resp: resp}
	// No Read at all: the SDK's own limiter can synthesise EOF above us.
	if err := tee.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if len(tee.trailer) == 0 {
		t.Fatal("trailers were never snapshot, so the record would omit them")
	}
}

func TestTheVerdictEscapesMarkerBytesBeforeStripping(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := verdictValue("a" + capturedMark + "b")
	if got == "ab" {
		t.Fatal("a marker byte in the SDK verdict was deleted rather than shown")
	}
}

func TestOffRouteTrafficIsNotRecorded(t *testing.T) {
	sent := false
	r := &recorder{inner: rtFunc(func(*http.Request) (*http.Response, error) {
		sent = true
		return &http.Response{StatusCode: 204, Body: io.NopCloser(strings.NewReader("")),
			Header: http.Header{}, Proto: "HTTP/1.1"}, nil
	})}
	req, _ := http.NewRequest("POST", "https://e.example/api/v1/ingest", strings.NewReader("{}"))
	resp, err := r.RoundTrip(req)
	if err != nil {
		t.Fatalf("off-route request errored instead of being absorbed: %v", err)
	}
	if resp.StatusCode != 204 {
		t.Fatalf("off-route request was not absorbed: %d", resp.StatusCode)
	}
	if sent {
		t.Fatal("off-route request was FORWARDED — the harness emitted analytics")
	}
	if len(r.exchanges) != 0 {
		t.Fatalf("ingest traffic was recorded as an assignment attempt: %d", len(r.exchanges))
	}
	if r.offRoute != 1 {
		t.Fatalf("off-route traffic was not counted: %d", r.offRoute)
	}
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// ---- round on 7854bd8 ----

func TestATransportErrorDoesNotDeadlockTheRecorder(t *testing.T) {
	r := &recorder{inner: rtFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial: no route")
	})}
	req, _ := http.NewRequest("GET", "https://e.example"+assignmentRoute+"?a=b", nil)
	done := make(chan struct{})
	go func() { _, _ = r.RoundTrip(req); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the transport-error path did not return: the recorder deadlocked on its own mutex")
	}
	if len(r.exchanges) != 1 {
		t.Fatalf("the failed attempt was not recorded: %d", len(r.exchanges))
	}
}

func TestNormalisedBase64CandidateIsDecoded(t *testing.T) {
	suppliedValues = []string{"abcdefghijklmnopqrstuvwxyz012345678901234567890123456789"}
	t.Cleanup(func() { suppliedValues = nil })
	enc := base64.StdEncoding.EncodeToString([]byte(suppliedValues[0]))
	// Wrapped at 41 — NOT a quartet boundary, so no fragment decodes to anything.
	wrapped := enc[:41] + "\r\n" + enc[41:]
	if err := assertNoLeak(asCaptured("body:\r\n" + wrapped + "\r\n")); err == nil {
		t.Fatal("a MIME wrap off the quartet boundary passed the guard")
	}
}

func TestTheProtocolExemptionAppliesToTheFirstLineOnly(t *testing.T) {
	suppliedValues = []string{"200"}
	t.Cleanup(func() { suppliedValues = nil })
	dump := asCaptured("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n" +
		"HTTP/1.1 %32%30%30 OK\r\n")
	if err := assertNoLeak(dump); err == nil {
		t.Fatal("a status-shaped BODY line was exempted and hid a supplied value")
	}
}

func TestTrailersAreSnapshotAfterCloseAsWell(t *testing.T) {
	resp := &http.Response{Trailer: http.Header{}}
	tee := &teeBody{inner: closerFunc(func() error {
		// Values become visible during close, which is legal for HTTP/2.
		resp.Trailer.Set("X-Late", "v")
		return nil
	}), resp: resp}
	if err := tee.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if len(tee.trailer) == 0 {
		t.Fatal("trailers that appeared during close were never recorded")
	}
}

type closerFunc func() error

func (f closerFunc) Close() error             { return f() }
func (f closerFunc) Read([]byte) (int, error) { return 0, io.EOF }

// ---- round on 706ab2c ----

func TestMimeNormalisationStaysInsideTheEncodedRun(t *testing.T) {
	suppliedValues = []string{"abcdefghijklmnopqrstuvwxyz012345678901234567890123456789"}
	t.Cleanup(func() { suppliedValues = nil })
	enc := base64.StdEncoding.EncodeToString([]byte(suppliedValues[0]))
	// A header value ending in base64 letters, then the wrapped body. Joining the
	// whole span would merge `abc` into the token and decode the wrong thing.
	dump := "X-Test: abc\r\n\r\n" + enc[:41] + "\r\n" + enc[41:] + "\r\n"
	if err := assertNoLeak(asCaptured(dump)); err == nil {
		t.Fatal("normalisation crossed the field boundary and lost the value")
	}
}

func TestTrailerHeaderListedNamesAreScrubbed(t *testing.T) {
	suppliedValues = []string{"Bar"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\nTrailer: X-Bar\r\n\r\n"))
	if strings.Contains(got, "Bar") {
		t.Fatalf("a name listed by the Trailer header kept a supplied value: %q", got)
	}
}

func TestTheSDKRouteIsNotReadAsCapturedData(t *testing.T) {
	suppliedValues = []string{"assignment"}
	t.Cleanup(func() { suppliedValues = nil })
	req := asCaptured("GET " + assignmentRoute + " HTTP/1.1\r\nHost: e.example\r\n")
	if err := assertNoLeak(req); err != nil {
		t.Fatalf("the SDK's own fixed route was reported as a leak: %v", err)
	}
}

func TestGeneratedCaptureNotesAreMarked(t *testing.T) {
	suppliedValues = []string{"Capture-Note"}
	t.Cleanup(func() { suppliedValues = nil })
	// ⚠ THROUGH THE SCRUB, NOT dropFraming ALONE. The note is written by
	// dropFraming and scrubbed by a LATER pass, so a fixture that stops at the
	// first says nothing about the second -- the mutant reverting the mark
	// survived exactly that.
	got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")))
	if strings.Contains(got, "<redacted") {
		t.Fatalf("a generated capture note was scrubbed into an unparsable name: %q", got)
	}
	if !strings.Contains(got, "X-Capture-Note:") {
		t.Fatalf("the generated note was destroyed: %q", got)
	}
}

// ---- round on 0cdc4ee / 03cba8b ----

func TestDecodedFieldNamesAreFoldedBeforeApproval(t *testing.T) {
	suppliedValues = []string{"secret"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("HTTP/1.1 200 OK\r\nX-%53ecret: v\r\n\r\n")); err == nil {
		t.Fatal("an encoded name that decodes to a case variant passed the guard")
	}
}

func TestNormalisedBase64GoesBackThroughEveryDecoder(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	inner := base64.StdEncoding.EncodeToString([]byte("%61bcdefgh"))
	wrapped := inner[:5] + "\r\n" + inner[5:]
	if err := assertNoLeak(asCaptured("body:\r\n" + wrapped + "\r\n")); err == nil {
		t.Fatal("an encoding nested inside MIME-wrapped base64 passed the guard")
	}
}

func TestProtocolTokensDoNotRefuseTheCapture(t *testing.T) {
	// ⚠ THROUGH `redact`, NOT A HAND-BUILT DUMP. My first version composed the
	// marked line itself and so could not see the call site at all -- the third
	// fixture in this branch to do that. What `redact` produces is what the guard
	// reads, so that is what the test reads.
	for _, v := range []string{"Bearer", "Authorization", "Host", "User-Agent"} {
		suppliedValues = []string{v}
		raw := "GET /p HTTP/1.1\r\nHost: e.example\r\nAuthorization: Bearer abcdefgh\r\nUser-Agent: sp/1\r\n\r\n"
		if err := assertNoLeak(asCaptured(string(redact([]byte(escapeMarks(raw)), http.Header{"Host": nil})))); err != nil {
			t.Errorf("supplied %q: fixed request syntax was read as a leak: %v", v, err)
		}
		suppliedValues = nil
	}
}

func TestAJSONLiteralIsNotRewritten(t *testing.T) {
	suppliedValues = []string{"false"}
	t.Cleanup(func() { suppliedValues = nil })
	// ⚠ THROUGH `dropFraming`, WHERE THE GRAMMAR IS MARKED. The exemption is a
	// POSITION now, not a value, so a fixture calling the scrub on a bare string
	// exercises neither half of it.
	body := "HTTP/1.1 200 OK\r\n\r\n" + `{"assigned":false}`
	got := stripMarks(scrubSupplied(dropFraming(body)))
	if !strings.Contains(got, `{"assigned":false}`) {
		t.Fatalf("a JSON literal was rewritten into invalid JSON: %q", got)
	}
	// And the SAME word inside a string is data, not grammar.
	quoted := "HTTP/1.1 200 OK\r\n\r\n" + `{"experiment_key":"false"}`
	got = stripMarks(scrubSupplied(dropFraming(quoted)))
	if strings.Contains(got, `"false"`) {
		t.Fatalf("the key echoed as a JSON string was published: %q", got)
	}
}

func TestANestedMintedNameIsNotTheVerdictsField(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	dropFraming("HTTP/1.1 200 OK\r\n\r\n" + `{"assigned":true,"variant_payload":{"subject_fact_key":"label"}}`)
	if len(structuralSurfaces) != 0 {
		t.Fatalf("a payload member refused a publishable capture: %v", structuralSurfaces)
	}
	// ⚠ AND IN THIS HALF THE TOP-LEVEL ONE IS REDACTED, NOT REFUSED -- that is the
	// whole difference between the two changes. The guard half refuses what it
	// cannot redact; this half redacts it, so the property to assert here is that
	// the minted value does not reach the artifact.
	structuralSurfaces = nil
	got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + `{"subject_fact_key":"sfk1_abab"}`))
	if strings.Contains(got, "sfk1_abab") {
		t.Fatalf("the top-level minted value was published: %q", got)
	}
}

func TestTheWithheldQueryMarkerIsOneToken(t *testing.T) {
	// ⚠ THROUGH THE REAL REDACTOR. The guard half withholds the query whole with
	// a stand-in; this half redacts it structurally, so the property -- a request
	// line is three components -- is asserted against what this half produces.
	got := stripMarks(redactQuery("GET /v1/assign?a=b HTTP/1.1\r"))
	line := strings.TrimSuffix(got, "\r")
	if n := len(strings.Fields(line)); n != 3 {
		t.Fatalf("the request line has %d components, not 3: %q", n, line)
	}
}

// ---- round on 3d334a3 ----

func TestNormalisedCandidateReachesAFixedPoint(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	// TWO layers of percent encoding inside MIME-wrapped base64.
	inner := base64.StdEncoding.EncodeToString([]byte("%2561bcdefgh"))
	wrapped := inner[:5] + "\r\n" + inner[5:]
	if err := assertNoLeak(asCaptured("body:\r\n" + wrapped + "\r\n")); err == nil {
		t.Fatal("a doubly-encoded value inside wrapped base64 passed the guard")
	}
}

func TestTheGuardHonoursTheJSONLiteralExemption(t *testing.T) {
	suppliedValues = []string{"false"}
	t.Cleanup(func() { suppliedValues = nil })
	body := "HTTP/1.1 200 OK\r\n\r\n" + `{"assigned":false}`
	if err := assertNoLeak(asCaptured(scrubSupplied(dropFraming(body)))); err != nil {
		t.Fatalf("a JSON grammar literal refused a publishable capture: %v", err)
	}
	// And a quoted occurrence is still checked: the scrub above replaces it, so
	// what reaches the guard is a placeholder, not the word.
	quoted := "HTTP/1.1 200 OK\r\n\r\n" + `{"experiment_key":"false"}`
	if err := assertNoLeak(asCaptured(scrubSupplied(dropFraming(quoted)))); err != nil {
		t.Fatalf("the guard flagged its own placeholder: %v", err)
	}
}

func TestSerialiserWrittenHeaderValuesAreGenerated(t *testing.T) {
	suppliedValues = []string{"gzip"}
	t.Cleanup(func() { suppliedValues = nil })
	raw := "GET /p HTTP/1.1\r\nHost: e.example\r\nAccept-Encoding: gzip\r\n\r\n"
	if err := assertNoLeak(asCaptured(string(redact([]byte(escapeMarks(raw)), http.Header{"Host": nil})))); err != nil {
		t.Fatalf("a value net/http wrote itself was read as a leak: %v", err)
	}
}

func TestAnUndecodableContentCodingIsRefused(t *testing.T) {
	for _, c := range []struct {
		enc     string
		refused bool
	}{{"deflate", true}, {"br", true}, {"identity", false}} {
		structuralSurfaces = nil
		suppliedValues = nil
		dropFraming("HTTP/1.1 200 OK\r\nContent-Encoding: " + c.enc + "\r\n\r\nbody")
		if got := len(structuralSurfaces) > 0; got != c.refused {
			t.Errorf("%s: refused=%v, want %v", c.enc, got, c.refused)
		}
		structuralSurfaces = nil
	}
}

func TestABodyReadErrorIsReadByTheGuard(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	enc := base64.StdEncoding.EncodeToString([]byte("abcdefgh"))
	tee := &teeBody{err: errors.New("malformed trailer \"X-Bad " + enc + "\"")}
	ex := exchange{head: []byte("x"), captured: tee}
	if e := assertNoLeak(incompleteBodyLine(&ex)); e == nil {
		t.Fatal("endpoint bytes carried by a body-read error were never decoded")
	}
}

func TestTransportErrorTextIsReadByTheGuard(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	// Go's parser puts the offending line into the error it returns.
	enc := base64.StdEncoding.EncodeToString([]byte("abcdefgh"))
	err := errors.New("malformed HTTP response \"X-Bad " + enc + "\"")
	if e := assertNoLeak(transportErrorLine(err)); e == nil {
		t.Fatal("endpoint bytes carried by a transport error were never decoded")
	}
}

func TestARefusalDoesNotPrintWhatItRefusesOver(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = []string{"deflate"}
	t.Cleanup(func() { structuralSurfaces = nil; suppliedValues = nil })
	dropFraming("HTTP/1.1 200 OK\r\nContent-Encoding: deflate\r\n\r\nbody")
	if len(structuralSurfaces) == 0 {
		t.Fatal("an undecodable coding stopped being refused")
	}
	for _, w := range structuralSurfaces {
		if strings.Contains(w, "deflate") {
			t.Fatalf("the refusal message carries the value it refuses over: %q", w)
		}
	}
}

func TestTheSerialiserUserAgentIsGenerated(t *testing.T) {
	suppliedValues = []string{"Go-http-client/1.1"}
	t.Cleanup(func() { suppliedValues = nil })
	raw := "GET /p HTTP/1.1\r\nHost: e.example\r\nUser-Agent: Go-http-client/1.1\r\n\r\n"
	if err := assertNoLeak(asCaptured(string(redact([]byte(escapeMarks(raw)), http.Header{"Host": nil})))); err != nil {
		t.Fatalf("the serialiser's own User-Agent was read as a leak: %v", err)
	}
}

// ---- round on 4b472c8 ----

func TestGrammarLiteralsAreFoundByParsing(t *testing.T) {
	suppliedValues = []string{"false"}
	t.Cleanup(func() { suppliedValues = nil })
	for _, c := range []struct {
		name, body string
		published  bool
	}{
		{"a literal node", `{"assigned":false}`, true},
		{"inside a string", `{"message":"saw false value"}`, false},
		{"a plain-text body", `error: false`, false},
	} {
		got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + c.body)))
		has := strings.Contains(got, "false")
		if has != c.published {
			t.Errorf("%s: `false` present=%v, want %v: %q", c.name, has, c.published, got)
		}
	}
}

func TestFieldNameComponentsDecodeIndependently(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("HTTP/1.1 200 OK\r\nX-YmFy: v\r\n\r\n")); err == nil {
		t.Fatal("a base64 component of a field name hid a supplied value")
	}
}

func TestTheSynthesisedHTTP2ReasonIsNotCapturedData(t *testing.T) {
	suppliedValues = []string{"OK"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("HTTP/2.0 200 OK\r\n\r\n")); err != nil {
		t.Fatalf("HTTP/2's synthesised phrase was read as a leak: %v", err)
	}
	// An HTTP/1 phrase the endpoint really sent is still data.
	suppliedValues = []string{"secret99"}
	if err := assertNoLeak(asCaptured("HTTP/1.1 400 secret99\r\n\r\n")); err == nil {
		t.Fatal("an HTTP/1 reason phrase stopped being checked")
	}
}

func TestTheSerialiserConnectionHeaderIsGenerated(t *testing.T) {
	suppliedValues = []string{"close"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming("HTTP/2.0 200 OK\r\nConnection: close\r\n\r\n")))
	if strings.Contains(got, "<redacted") {
		t.Fatalf("a serialiser-added Connection header was scrubbed into an invalid response: %q", got)
	}
}

// ---- round on d9a8bb1 ----

func TestOnlySynthesisedConnectionIsGenerated(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil; receivedConnection = false })
	// ⚠ TWO RESTATEMENTS OF ONE SCENE, MERGED. The guard half replaced the protocol
	// premise with the flag the recorder sets from `resp.Header`, because Go
	// synthesises `Connection: close` for HTTP/1.1 too; this half replaced the
	// guard-errors MECHANISM with the property, because it redacts the value
	// structurally and nothing reaches the guard. Both corrections are real and
	// they are about different halves of the same sentence
	// (shardpilot/shardpilot-go#84, #85 review).
	receivedConnection = true
	got1 := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nConnection: YmFy\r\n\r\n")))
	if strings.Contains(got1, "YmFy") {
		t.Fatalf("a received Connection value was exempted as generated and published: %q", got1)
	}
	if !strings.Contains(got1, "<redacted") {
		t.Fatalf("a received Connection value was neither published nor accounted for: %q", got1)
	}
	// Not received: the serialiser wrote it, whatever the protocol says.
	receivedConnection = false
	suppliedValues = []string{"close"}
	got := stripMarks(scrubSupplied(dropFraming("HTTP/2.0 200 OK\r\nConnection: close\r\n\r\n")))
	if strings.Contains(got, "<redacted") {
		t.Fatalf("the synthesised HTTP/2 Connection line was scrubbed: %q", got)
	}
	// The case the protocol rule could not see: HTTP/1.1 with a synthesised line,
	// which is what Go writes when the length is unknown.
	got = stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n")))
	if strings.Contains(got, "<redacted") {
		t.Fatalf("a synthesised HTTP/1 Connection line was scrubbed: %q", got)
	}
}

func TestRefusalLabelsCarryNoEndpointText(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	accountedSurfaces = nil
	t.Cleanup(func() { accountedSurfaces = nil })
	dropFraming("HTTP/1.1 200 OK\r\n\r\n" + "{\"ſubject_fact_key\":\"x\"}")
	// ⚠ THE ACCOUNTING LEDGER, NOT THE REFUSAL ONE. The guard half refused such a
	// capture, so detection and refusal were the same list. This half redacts and
	// PUBLISHES it, so asserting on the refusal ledger would demand the program
	// refuse every fact response (shardpilot/shardpilot-go#85, stack seam).
	if len(accountedSurfaces) == 0 {
		t.Fatal("a folded minted name stopped being detected")
	}
	// The label rule holds for BOTH ledgers: every one of them is printed.
	for _, w := range append(append([]string{}, structuralSurfaces...), accountedSurfaces...) {
		if strings.Contains(w, "ſ") {
			t.Fatalf("the refusal label carries the endpoint's own spelling: %q", w)
		}
	}
}

func TestErrorTextCannotInjectProvenanceBytes(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	enc := base64.StdEncoding.EncodeToString([]byte("abcdefgh"))
	// The endpoint puts the guard's own reserved byte around its payload.
	err := errors.New("malformed response \"" + genMark + enc + genMark + "\"")
	if e := assertNoLeak(transportErrorLine(err)); e == nil {
		t.Fatal("injected provenance bytes made the guard blank endpoint text")
	}
}

func TestMimeWhitespaceInsideARunIsIgnored(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	enc := base64.StdEncoding.EncodeToString([]byte("abcdefgh"))
	wrapped := enc[:5] + " \r\n" + enc[5:]
	if err := assertNoLeak(asCaptured("body:\r\n" + wrapped + "\r\n")); err == nil {
		t.Fatal("horizontal whitespace inside a MIME run defeated the guard")
	}
}

func TestBinaryBase64DecodesAreChecked(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	tok := base64.StdEncoding.EncodeToString(append([]byte{0xff}, []byte("abcdefgh")...))
	if err := assertNoLeak(asCaptured(`{"k":"` + tok + `"}`)); err == nil {
		t.Fatal("a decode whose bytes are not valid UTF-8 was discarded")
	}
}

// TestAnOrdinaryFactResponseStaysPublishable is the scene the promise sweep found
// missing: the doc claims a value whose EXTENT cannot be determined is refused
// with exit 4, and nothing exercised the publish/refuse decision at all. Under
// that gap I recorded an ordinary successful redaction in the REFUSAL ledger, and
// every fact response — the captures this change exists to publish — would have
// exited 4 with the whole suite green.
func TestAnOrdinaryFactResponseStaysPublishable(t *testing.T) {
	structuralSurfaces = nil
	accountedSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil; accountedSurfaces = nil })
	dropFraming("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n" +
		`{"subject_fact_key":"sfk1_aaaaaaaaaaaaaaaa","assigned":true}`)
	if got := refusalLedger(); len(got) != 0 {
		t.Fatalf("an ordinary fact response was made unpublishable: %q", got)
	}
	if len(accountedSurfaces) == 0 {
		t.Fatal("the minted field was rewritten and not accounted for")
	}
}

// TestASurfaceTheRulesCannotDescribeRefuses is the other half of the promise the
// sweep found uncovered. The scene above asserts the refusal ledger is EMPTY for
// an ordinary response; nothing asserted it FILLS for a shape the rules cannot
// describe, so `refusalLedger` could have returned nil unconditionally with the
// whole suite green — a negative-only assertion is satisfied by a gate that never
// fires (shardpilot/shardpilot-go#85, promise sweep).
func TestASurfaceTheRulesCannotDescribeRefuses(t *testing.T) {
	structuralSurfaces = nil
	accountedSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil; accountedSurfaces = nil })
	dropFraming("HTTP/1.1 200 OK\r\nSet-Cookie: \r\n\r\n{}")
	if len(refusalLedger()) == 0 {
		t.Fatal("a Set-Cookie with no name=value pair left the capture publishable")
	}
}

// TestABinaryHexDecodeIsRetained is the reviewer's own example: a bare hex token
// decoding to invalid UTF-8 that CONTAINS a supplied value. `undoHex` drops such
// a decode because substituting binary destroys the text around it, and nothing
// retained the bytes -- so the guard approved a value a standard hex decoder
// reconstructs in one step (shardpilot/shardpilot-go#84 review).
func TestABinaryHexDecodeIsRetained(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("payload ff6162636465666768 end")); err == nil {
		t.Fatal("a binary hex decode carrying a supplied value was approved")
	}
}

// TestABinaryCandidateIsDecodedAgain is the other half of the same class: a
// binary candidate is an INPUT to the decoder chain, not an answer from it.
// `/yU2MWJjZGVmZ2g=` is base64 of `0xff%61bcdefgh`; the binary decode was kept
// as-is and never percent-decoded, and the ordinary chain cannot reach it
// because it never un-wraps the base64 (shardpilot/shardpilot-go#84 review).
func TestABinaryCandidateIsDecodedAgain(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("body /yU2MWJjZGVmZ2g= end")); err == nil {
		t.Fatal("a binary base64 candidate was never fed back through the decoders")
	}
}

// TestAMalformedBodyWithAMintedFieldRefuses is the reviewer's own example: a body
// that ends mid-value. `topLevelMembers` returns nothing for it, and "no top-level
// names" was read as "no minted field here" — so the identifier was published and
// the leak guard could not help, because a server-minted value is not supplied
// (shardpilot/shardpilot-go#84 review).
func TestAMalformedBodyWithAMintedFieldRefuses(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	accountedSurfaces = nil
	t.Cleanup(func() { accountedSurfaces = nil })
	// ⚠ THE PROPERTY, NOT THE MECHANISM THAT PROVED IT — third time in this stack.
	// The guard half could only REFUSE a malformed body carrying a minted field,
	// so its scene asserted refusal. This half redacts the value, which is the
	// stronger outcome: the identifier never reaches the artifact. What it owes is
	// the record, and that is what is asserted (shardpilot/shardpilot-go#84 review,
	// ported across the stack seam).
	got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\n\r\n" +
		`{"assigned":true,"subject_fact_key":"sfk1_xxxxxxxxxxxx"`))
	if strings.Contains(got, "sfk1_xxxxxxxxxxxx") {
		t.Fatalf("a minted value in a malformed body was published: %q", got)
	}
	if len(structuralSurfaces)+len(accountedSurfaces) == 0 {
		t.Fatalf("a malformed body was rewritten and not accounted for: %q", got)
	}
}

// TestAShortValueInsideANonASCIIWordSurvives is the reviewer's own example: the
// short-value rule exists to avoid corrupting unrelated words, and a byte-based
// boundary check made every non-ASCII byte a separator — so `é` was rewritten
// inside the unrelated endpoint word `αéβ` (shardpilot/shardpilot-go#84 review).
func TestAShortValueInsideANonASCIIWordSurvives(t *testing.T) {
	suppliedValues = []string{"é"}
	t.Cleanup(func() { suppliedValues = nil })
	if got := scrubSupplied("word αéβ here"); !strings.Contains(got, "αéβ") {
		t.Fatalf("an unrelated non-ASCII word was corrupted: %q", got)
	}
	// ⚠ AND THE RULE STILL FIRES. A boundary check made permissive enough to stop
	// corrupting words stops finding values too, which is the dangerous direction:
	// this half of the scene fails if the repair simply disabled the rule.
	if got := scrubSupplied("word é here"); strings.Contains(got, "é") {
		t.Fatalf("a standalone short supplied value was published: %q", got)
	}
	// Non-ASCII PUNCTUATION is still a boundary, or the same miss returns wearing
	// different bytes.
	if got := scrubSupplied("word «é» here"); strings.Contains(got, "é") {
		t.Fatalf("a value between non-ASCII punctuation was published: %q", got)
	}
}

// TestASanitizerCreatedMarkIsNotEscaped: `dropQuery` inserts a GENERATED
// `query-withheld` token, and the outer escape read those freshly generated bytes
// as endpoint bytes — so the report claimed a transport error contained a mark
// pair nothing produced (shardpilot/shardpilot-go#84 review).
func TestASanitizerCreatedMarkIsNotEscaped(t *testing.T) {
	suppliedValues = nil
	got := stripMarks(sanitizeCaptured(&url.Error{
		Op: "Get", URL: "https://example.invalid/v1/assign?subject_key=zzz", Err: errors.New("timeout"),
	}))
	if strings.Contains(got, "subject_key") {
		t.Fatalf("the error's query was published: %q", got)
	}
	// The guard half replaced the query with a generated `query-withheld` token;
	// this half redacts it structurally, so asserting that token would demand the
	// weaker mechanism back. What both must satisfy is that the query's VALUES do
	// not appear.
	if strings.Contains(got, "zzz") {
		t.Fatalf("a query value from the error was published: %q", got)
	}
	for _, m := range []string{capturedMark, genMark} {
		if strings.Contains(got, m) {
			t.Fatalf("a provenance mark reached the report text: %q", got)
		}
	}
	// ⚠ THE ESCAPED SPELLING IS THE SYMPTOM, and the first version of this scene
	// missed it: it asserted the token survived and that no raw mark appeared,
	// both of which the DEFECT also satisfies — escaping renders the marks as
	// different bytes, so neither check could tell the two orders apart. The
	// mutant survived and said so. What the defect prints is the literal escape.
	for _, lit := range []string{`\x00`, `\x01`} {
		if strings.Contains(got, lit) {
			t.Fatalf("a mark this program itself generated was escaped as endpoint bytes: %q", got)
		}
	}
}

// ---- round on b7d4c7a ----

// TestIPvFutureNeedsItsWholeGrammar asks `isIPvFuture` directly, NOT through
// `parsesAsURI`.
//
// ⚠ THE FINDING'S PREMISE DOES NOT HOLD ON THIS GO VERSION, measured: `url.Parse`
// refuses EVERY bracketed IPvFuture form, the well-formed `[v7.abc]` included
// ("invalid host: ParseAddr: unexpected character"), so the exemption the finding
// describes is unreachable behind a parse that already failed. This file has
// recorded the same thing once before, about the bracket check beside it.
//
// The grammar stays and is stated here rather than inherited, for the reason the
// rest of this file stopped borrowing predicates: a future net/url that starts
// accepting IPvFuture must not silently widen what this program publishes. So the
// scene exercises the predicate, which is the thing that would then be
// load-bearing (shardpilot/shardpilot-go#85 review).
func TestIPvFutureNeedsItsWholeGrammar(t *testing.T) {
	for _, bad := range []string{"vSERVER-SECRET", "v", "v.abc", "vg7.abc", "v7.", "v7abc"} {
		if isIPvFuture(bad) {
			t.Fatalf("a bracketed authority was exempted on its first letter alone: %q", bad)
		}
	}
	// And a well-formed one is still admitted, or the repair is just a refusal.
	for _, ok := range []string{"v7.abc", "V1f.host:name", "v0.a"} {
		if !isIPvFuture(ok) {
			t.Fatalf("a well-formed IPvFuture literal was refused: %q", ok)
		}
	}
}

func TestAScopedIPv6AuthorityIsAccepted(t *testing.T) {
	if !parsesAsURI("https://[fe80::1%25eth0]/cb") {
		t.Fatal("a scoped IPv6 redirect Go itself accepts forced a refusal")
	}
	if parsesAsURI("https://[fe80::1%25]/cb") {
		t.Fatal("an empty zone was accepted")
	}
}

func TestAValuelessStandardAttributeIsMarked(t *testing.T) {
	suppliedValues = []string{"Secure"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(redactSetCookie("Set-Cookie: sid=x; Secure")))
	if !strings.Contains(got, "Secure") {
		t.Fatalf("a standard cookie flag was scrubbed into something no parser accepts: %q", got)
	}
}

func TestStandardAttributeValuesKeepTheirVocabulary(t *testing.T) {
	suppliedValues = nil
	got := stripMarks(redactSetCookie("Set-Cookie: sid=x; SameSite=Lax; Max-Age=10"))
	for _, want := range []string{"SameSite=Lax", "Max-Age=10"} {
		if !strings.Contains(got, want) {
			t.Fatalf("a fixed-vocabulary attribute value the criterion admits was lengthened: %q", got)
		}
	}
}

func TestAVouchedCookieNameIsMarked(t *testing.T) {
	suppliedValues = []string{"experiment_key"}
	// `nameIsOurs` reads the names the harness actually put on the wire, so the
	// scene has to put one there: the first version of this test asserted about a
	// registry it had left empty, and measured the branch it was not aiming at.
	requestNames = map[string]bool{"experiment_key": true}
	t.Cleanup(func() { suppliedValues = nil; requestNames = map[string]bool{} })
	got := stripMarks(scrubSupplied(redactSetCookie("Set-Cookie: experiment_key=x")))
	if !strings.Contains(got, "experiment_key=") {
		t.Fatalf("a cookie name this program vouched for was scrubbed: %q", got)
	}
}

func TestAnUnparsableBodyRefusesAnUnfamiliarMember(t *testing.T) {
	structuralSurfaces = nil
	accountedSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil; accountedSurfaces = nil })
	redactMintedBody(`{"server_secret_identifier":"x`)
	if len(refusalLedger()) == 0 {
		t.Fatal("an unfamiliar member in a body that does not parse stayed publishable")
	}
	// ⚠ AND A COMPLETE, ORDINARY BODY STILL PUBLISHES. Failing closed on a parse
	// failure is one line away from failing closed on everything.
	structuralSurfaces = nil
	redactMintedBody(`{"assigned":true,"variant_key":"blue"}`)
	if len(refusalLedger()) != 0 {
		t.Fatalf("an ordinary complete body was refused: %q", refusalLedger())
	}
}

func TestCoveredSpansAreWalkedInOrder(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	// A minted member AFTER many covered ones: a cursor that ran past it would
	// report the body clean, which is exactly how this optimisation can go wrong.
	var b strings.Builder
	b.WriteString(`{`)
	for i := 0; i < 200; i++ {
		b.WriteString(`"filler_` + strconv.Itoa(i) + `":"v",`)
	}
	b.WriteString(`"subject_fact_key":"sfk1_xxxxxxxxxxxx"}`)
	if got := redactMintedBody(b.String()); strings.Contains(got, "sfk1_xxxxxxxxxxxx") {
		t.Fatal("a minted value after many covered spans was published")
	}
}

// TestDepthIsWalkedForwardNotRescanned pins what the cursor must still get right.
// The rescan it replaces was correct and quadratic; a cursor is fast and can be
// wrong in exactly one way — running ahead of the byte being asked about — so the
// scene puts a minted name NESTED after many members (must stay skipped) and one
// at top level after them (must still be caught).
//
// ⚠ MEASURED LIMIT, recorded rather than fixtured around: a cursor that overshoots
// by a SMALL fixed amount survives this scene. Depth changes only at braces, and
// an overshoot alters an answer only when a brace falls between the queried byte
// and the overshot one — which no member name in a realistic body does. What the
// scene does kill is a cursor that never advances (depth stuck at 0, nothing
// top-level) and one that reports a constant depth (everything top-level), both
// verified. A defect in the narrow band between those is not covered here.
func TestDepthIsWalkedForwardNotRescanned(t *testing.T) {
	structuralSurfaces = nil
	accountedSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil; accountedSurfaces = nil })

	var b strings.Builder
	b.WriteString(`{"variant_payload":{`)
	for i := 0; i < 300; i++ {
		b.WriteString(`"n` + strconv.Itoa(i) + `":` + strconv.Itoa(i) + `,`)
	}
	b.WriteString(`"subject_fact_key":1},"assigned":true}`)
	if got := redactMintedBody(b.String()); strings.Contains(got, "withheld") {
		t.Fatalf("a nested member the endpoint merely named that way refused a capture: %q", got)
	}

	var c strings.Builder
	c.WriteString(`{"variant_payload":{`)
	for i := 0; i < 300; i++ {
		c.WriteString(`"n` + strconv.Itoa(i) + `":` + strconv.Itoa(i) + `,`)
	}
	c.WriteString(`"x":1},"subject_fact_key":1}`)
	structuralSurfaces = nil
	if got := redactMintedBody(c.String()); !strings.Contains(got, "withheld") {
		t.Fatalf("a top-level minted member after many nested ones was published: %q", got)
	}
}

// ---- round on 9721c2d ----

// TestATrailingJSONValueIsNotGrammar is the reviewer's own example. `json.Decoder`
// reads a SEQUENCE of top-level values, so `{"x":1} false` walked as valid JSON
// and the trailing literal was marked as grammar — and a marked span is skipped by
// both the scrub and the guard, while `json.Unmarshal` rejects that body as a
// verdict outright (shardpilot/shardpilot-go#84 review).
func TestATrailingJSONValueIsNotGrammar(t *testing.T) {
	suppliedValues = []string{"false"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(markBareJSONLiterals(`{"x":1} false`)))
	if strings.Contains(got, "false") {
		t.Fatalf("a supplied value was marked as grammar in a multi-value body: %q", got)
	}
	// ⚠ AND A REAL VERDICT BODY STILL HAS ITS GRAMMAR PROTECTED, or the repair is
	// just a refusal to mark anything.
	got = stripMarks(scrubSupplied(markBareJSONLiterals(`{"assigned":false}`)))
	if !strings.Contains(got, `"assigned":false`) {
		t.Fatalf("the literal node of an ordinary verdict body was scrubbed: %q", got)
	}
}

// TestASeparatorArrivingMidChainIsSplit is the other reviewer example: a legal
// field name percent-encoding the separator before a base64 component. The
// one-time split saw no hyphen, and when the percent stage produced `X-YmFy` the
// name went on being one url-base64 token, so `bar` was never reconstructed
// (shardpilot/shardpilot-go#84 review).
func TestASeparatorArrivingMidChainIsSplit(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("HTTP/1.1 200 OK\r\nX%2dYmFy: v\r\n\r\n")); err == nil {
		t.Fatal("a field name whose separator arrived mid-chain was published")
	}
}

// ---- round on 73ae245 ----

// TestLeadingTextIsNotJSON is the third side of one rule. Earlier rounds found
// this function accepting a body that merely CONTAINS a value and a body followed
// by more values; this one is a body PRECEDED by text (shardpilot/shardpilot-go#84
// review). The rule is one value with only JSON whitespace around it.
func TestLeadingTextIsNotJSON(t *testing.T) {
	suppliedValues = []string{"false"}
	t.Cleanup(func() { suppliedValues = nil })
	for _, body := range []string{
		`error: {"assigned":false}`,
		`{"assigned":false} trailing`,
		"\ufeff" + `{"assigned":false}`,
	} {
		got := stripMarks(scrubSupplied(markBareJSONLiterals(body)))
		if strings.Contains(got, "false") {
			t.Fatalf("a supplied value was marked as grammar in %q: %q", body, got)
		}
	}
	// ⚠ AND AN ORDINARY BODY, WITH THE WHITESPACE A SERVER ACTUALLY SENDS, still
	// has its grammar protected — otherwise the repair is a refusal to mark.
	got := stripMarks(scrubSupplied(markBareJSONLiterals("\n  " + `{"assigned":false}` + "\n")))
	if !strings.Contains(got, `"assigned":false`) {
		t.Fatalf("the literal node of an ordinary verdict body was scrubbed: %q", got)
	}
}

// TestABlankLineInsideABase64RunIsWhitespace: a standard base64 decoder ignores
// every CR and LF, so `YWJjZ\r\n\r\nGVmZ2g=` reconstructs the identifier in one
// step, while ending the run at the empty line left neither fragment decoding to
// anything (shardpilot/shardpilot-go#84 review).
func TestABlankLineInsideABase64RunIsWhitespace(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("head\r\nYWJjZ\r\n\r\nGVmZ2g=\r\ntail\r\n")); err == nil {
		t.Fatal("a base64 run split by a blank line published a reconstructable value")
	}
}

// TestValidNonObjectJSONIsNotUnclassifiable: `[{"subject_fact_key":…}]` is
// syntactically valid JSON whose structure PROVES the member is nested. Reading
// `topLevelMembers(body) == nil` as "cannot be classified" refused it
// (shardpilot/shardpilot-go#84 review).
func TestValidNonObjectJSONIsNotUnclassifiable(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	// This half redacts where the guard half refuses, so the scene asks the
	// function that does the work here.
	redactMintedBody(`[{"subject_fact_key":"ordinary payload"}]`)
	if len(structuralSurfaces) != 0 {
		t.Fatalf("valid non-object JSON was refused as unclassifiable: %q", structuralSurfaces)
	}
	// ⚠ AND A BODY THAT GENUINELY DOES NOT PARSE STILL FAILS CLOSED.
	structuralSurfaces = nil
	redactMintedBody(`{"assigned":true,"subject_fact_key":"x`)
	if len(structuralSurfaces) == 0 {
		t.Fatal("a body that does not parse stopped failing closed")
	}
}

// TestAnAcceptedIdentityCodingIsGrammar: the branch accepts `identity` as the
// no-op coding, then left the token as captured text — so a legal experiment key
// of `identity` turned it into a declaration of an unknown coding
// (shardpilot/shardpilot-go#84 review).
func TestAnAcceptedIdentityCodingIsGrammar(t *testing.T) {
	suppliedValues = []string{"identity"}
	receivedConnection = true
	t.Cleanup(func() { suppliedValues = nil; receivedConnection = false })
	got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\nContent-Encoding: identity\r\n\r\n")))
	if !strings.Contains(got, "Content-Encoding: identity") {
		t.Fatalf("an accepted no-op coding was rewritten into an invalid one: %q", got)
	}
}

// ---- round on c72e65d ----

// TestAnOpaqueURIPayloadIsRedacted: `https:SERVER_SECRET` is a valid absolute URI
// whose remainder has no slash, so the segment-based path redaction left it
// untouched and the endpoint's text reached the artifact verbatim
// (shardpilot/shardpilot-go#85 review).
func TestAnOpaqueURIPayloadIsRedacted(t *testing.T) {
	suppliedValues = nil
	got := stripMarks(redactTarget("Location: https:SERVER_SECRET"))
	if strings.Contains(got, "SERVER_SECRET") {
		t.Fatalf("an opaque URI payload was published: %q", got)
	}
	// ⚠ AND THE SHAPES THAT MUST STILL WORK, or the repair is a refusal: a scheme
	// with an absolute path, and an ordinary authority target.
	if got := stripMarks(redactTarget("Location: https:/cb")); !strings.Contains(got, "https:/") {
		t.Fatalf("a scheme with an absolute path was mangled: %q", got)
	}
	if got := stripMarks(redactTarget("Location: https://e.example/cb")); !strings.Contains(got, "e.example") {
		t.Fatalf("an ordinary authority target was mangled: %q", got)
	}
}

// TestACodingAnnouncedInATrailerRefuses: for a chunked HTTP/1 response declaring
// `Trailer: Content-Encoding`, Go leaves the initial field empty and the coding
// arrives late, with the raw compressed bytes in the body. The header path refuses
// that; this one accepted it (shardpilot/shardpilot-go#85 review).
func TestACodingAnnouncedInATrailerRefuses(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	e := &exchange{captured: &teeBody{trailer: map[string][]string{"Content-Encoding": {"gzip"}}}}
	e.trailerReport()
	if len(structuralSurfaces) == 0 {
		t.Fatal("a content coding announced in a trailer left the capture publishable")
	}
	// A no-op coding in a trailer is still a no-op.
	structuralSurfaces = nil
	e2 := &exchange{captured: &teeBody{trailer: map[string][]string{"Content-Encoding": {"identity"}}}}
	e2.trailerReport()
	if len(structuralSurfaces) != 0 {
		t.Fatalf("an identity coding in a trailer refused a publishable capture: %q", structuralSurfaces)
	}
}

// TestACookieAttributeIsMeasuredAsReceived: `responseText` expands a marker-like
// spelling before the attribute is measured, so a four-character value was
// reported as seven (shardpilot/shardpilot-go#85 review).
func TestACookieAttributeIsMeasuredAsReceived(t *testing.T) {
	suppliedValues = nil
	got := stripMarks(redactSetCookie("Set-Cookie: sid=x; Path=" + escapeMarks(capturedMark)))
	// ⚠ ANCHORED TO THE ATTRIBUTE. The first version asserted `"1 chars"` anywhere
	// in the line — and the cookie's OWN value is one character, so the assertion
	// matched something other than its subject and the mutant survived.
	if !strings.Contains(got, "Path=<redacted, 1 chars>") {
		t.Fatalf("an attribute value was measured in its escaped spelling: %q", got)
	}
}

// ---- round on ac5f3a0 ----

// TestTheAuthorityIsNotAParameterName: registering the URL authority in
// `requestNames` made `nameIsOurs` vouch for it wherever a NAME is expected, so
// with host and experiment key both `control` the identifier was marked
// harness-generated and both the scrub and the guard skipped it
// (shardpilot/shardpilot-go#85 review).
func TestTheAuthorityIsNotAParameterName(t *testing.T) {
	suppliedValues = []string{"control"}
	requestNames = map[string]bool{}
	t.Cleanup(func() { suppliedValues = nil; requestNames = map[string]bool{} })
	got := stripMarks(scrubSupplied(redactTarget("Location: /cb?control=x")))
	if strings.Contains(got, "control") {
		t.Fatalf("a supplied identifier equal to the authority was published: %q", got)
	}
	// ⚠ AND A NAME THE HARNESS REALLY SENT IS STILL VOUCHED FOR.
	requestNames = map[string]bool{"control": true}
	got = stripMarks(scrubSupplied(redactTarget("Location: /cb?control=x")))
	if !strings.Contains(got, "control=") {
		t.Fatalf("a parameter name the harness sent was scrubbed: %q", got)
	}
}

// TestAnIPv6ZoneIsNotExempt: the host exemption rests on "publicly resolvable and
// constrained by its grammar", and a zone identifier is an arbitrary local string
// (shardpilot/shardpilot-go#85 review).
func TestAnIPv6ZoneIsNotExempt(t *testing.T) {
	// ⚠ THE PROPERTY IS ABOUT THE OUTPUT, NOT THE PREDICATE. My first version
	// asserted `parsesAsURI` refuses it — and `SERVER_SECRET` satisfies RFC 6874
	// exactly, so the scene failed on the fix and was right to: a grammar check
	// cannot express "this is endpoint text". The zone is redacted instead.
	suppliedValues = nil
	got := stripMarks(redactTarget("Location: https://[fe80::1%25SERVER_SECRET]/cb"))
	if strings.Contains(got, "SERVER_SECRET") {
		t.Fatalf("an arbitrary IPv6 zone reached the capture verbatim: %q", got)
	}
	if !strings.Contains(got, "fe80::1") {
		t.Fatalf("the address itself was lost with the zone: %q", got)
	}
	// A real scoped address still is one, or the earlier fix is undone.
	if !parsesAsURI("https://[fe80::1%25eth0]/cb") {
		t.Fatal("a legitimate scoped IPv6 authority was refused")
	}
	for _, bad := range []string{"", "a b", "a/b", "a%b"} {
		if isZoneID(bad) {
			t.Fatalf("a zone identifier outside the grammar was accepted: %q", bad)
		}
	}
}

// ---- round on f258c43 ----

// TestConnectionProvenanceIsPerExchange: the SDK retries, and every attempt is
// rendered. A single global held the LAST attempt's answer, so a first response
// that really sent the field had it marked serialiser-generated when the final
// response carried none (shardpilot/shardpilot-go#84 review).
func TestConnectionProvenanceIsPerExchange(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil; receivedConnection = false })
	first := &exchange{proto: "HTTP/1.1", recvConn: true,
		head: []byte("HTTP/1.1 200 OK\r\nConnection: YmFy\r\n\r\n")}
	last := &exchange{proto: "HTTP/1.1", recvConn: false,
		head: []byte("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n")}
	// Render the LAST one first, as a run that retried would, then the first: the
	// global must not carry the last attempt's answer into the earlier one.
	_ = responseText(last)
	// ⚠ THE ASSERTION IS ABOUT THE GUARD, because in this half a RECEIVED value is
	// kept verbatim on purpose — captured data the guard must read. Marking it
	// serialiser-generated is what makes the guard skip it, so that is what the
	// scene detects. My first version asserted the value was absent from the text,
	// which is true only of the half that redacts structurally.
	// ⚠ THE PROPERTY, NOT THE GUARD-ERRORS MECHANISM. The guard half keeps a
	// RECEIVED value verbatim and relies on `assertNoLeak` to catch it; this half
	// redacts it structurally, so nothing reaches the guard and a nil error is the
	// stricter answer. What both halves must satisfy is that the first attempt's
	// received value is not treated as serialiser syntax — which shows as the
	// value surviving into the text (shardpilot/shardpilot-go#84, #85 review).
	got := stripMarks(responseText(first))
	if strings.Contains(got, "YmFy") {
		t.Fatalf("a received Connection value was published as serialiser syntax: %q", got)
	}
	if !strings.Contains(got, "<redacted") {
		t.Fatalf("a received Connection value was neither published nor accounted for: %q", got)
	}
	// And the synthesised one is still exempt, or the repair refuses every capture.
	receivedConnection = false
	suppliedValues = []string{"close"}
	if got := stripMarks(responseText(last)); strings.Contains(got, "<redacted") {
		t.Fatalf("a synthesised Connection line was redacted: %q", got)
	}
}

// fakeTransport returns one canned response, so a scene can exercise the RECORDER
// rather than restating what `net/http` does.
type fakeTransport struct{ resp *http.Response }

func (f *fakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	f.resp.Request = req
	return f.resp, nil
}

// TestConnectionPresenceIsNotItsFirstValue exercises the recorder, not `http.Header`.
//
// ⚠ MY FIRST VERSION ASSERTED THE STDLIB'S SEMANTICS — that `Get` returns the
// first value and map membership does not — and the mutant restoring `Get` at the
// call site SURVIVED it, because the scene never reached that call site. This
// file already records the same defect about `responseText`: a test of the
// composition is not a test of the call site (shardpilot/shardpilot-go#84 review).
func TestConnectionPresenceIsNotItsFirstValue(t *testing.T) {
	h := http.Header{}
	h.Add("Connection", "")
	h.Add("Connection", "YmFy")
	if h.Get("Connection") != "" {
		t.Fatal("the probe's premise no longer holds: Get returned a non-empty first value")
	}
	rec := &recorder{inner: &fakeTransport{resp: &http.Response{
		StatusCode: 200, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Status: "200 OK", Header: h, ContentLength: -1,
		Body: io.NopCloser(strings.NewReader("")),
	}}}
	req, err := http.NewRequest("GET", "https://e.example"+assignmentRoute, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rec.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	got := rec.exchanges
	rec.mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("the recorder kept %d exchanges", len(got))
	}
	if !got[0].recvConn {
		t.Fatal("a Connection field whose FIRST value is empty was recorded as absent")
	}
}
