package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"
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

func TestAServerGeneratedHeaderMakesTheCaptureUnpublishable(t *testing.T) {
	for _, c := range []struct{ dump, want string }{
		{"HTTP/1.1 302 Found\r\nLocation: /cb?state=server-state\r\n\r\n", "server-state"},
		{"HTTP/1.1 200 OK\r\nSet-Cookie: sid=server-secret\r\n\r\n", "server-secret"},
	} {
		structuralSurfaces = nil
		got := stripMarks(dropFraming(c.dump))
		if len(structuralSurfaces) == 0 {
			t.Errorf("a server-generated surface was not noted: %q", got)
		}
		if strings.Contains(got, c.want) {
			t.Errorf("the value was published rather than withheld: %q", got)
		}
		structuralSurfaces = nil
	}
}

func TestAMintedSubjectKeyMakesTheCaptureUnpublishable(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	body := "HTTP/1.1 200 OK\r\n\r\n" + `{"assigned":true,"subject_fact_key":"sfk1_abababababababab"}`
	dropFraming(body)
	if len(structuralSurfaces) == 0 {
		t.Fatal("a server-minted subject key did not make the capture unpublishable")
	}
}

func TestAServerGeneratedTrailerMakesTheCaptureUnpublishable(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	tee := &teeBody{trailer: http.Header{"Set-Cookie": []string{"sid=server-secret"}}}
	e := exchange{head: []byte("x"), captured: tee}
	got := stripMarks(e.trailerReport())
	if len(structuralSurfaces) == 0 {
		t.Fatal("a Set-Cookie trailer did not make the capture unpublishable")
	}
	if strings.Contains(got, "server-secret") {
		t.Fatalf("the trailer value was published rather than withheld: %q", got)
	}
}

func TestARequestQueryIsWithheldWhole(t *testing.T) {
	// This half cannot say how long each value was, and a length it cannot
	// compute is not a length it may guess.
	got := stripMarks(dropQuery("/v1/assign?subject_key=abc&x=y"))
	if strings.Contains(got, "abc") || strings.Contains(got, "x=y") {
		t.Fatalf("a query value survived: %q", got)
	}
	if !strings.HasPrefix(got, "/v1/assign") {
		t.Fatalf("the path was destroyed with the query: %q", got)
	}
}

// ---- round on bfac48f ----

func TestWithholdingTheQueryKeepsTheRequestLine(t *testing.T) {
	got := stripMarks(dropQuery("GET /v1/assign?subject_key=abc HTTP/1.1\r"))
	if strings.Contains(got, "abc") {
		t.Fatalf("a query value survived: %q", got)
	}
	if !strings.HasSuffix(got, " HTTP/1.1\r") {
		t.Fatalf("the version and terminator were cut with the query: %q", got)
	}
	// A bare URL has no space and must still lose its query.
	if bare := stripMarks(dropQuery("https://e.example/cb?t=1")); strings.Contains(bare, "t=1") {
		t.Fatalf("a bare URL kept its query: %q", bare)
	}
}

func TestAnEscapedMemberNameStillMakesTheCaptureUnpublishable(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	// ⚠ THE NAME IS SPELLED WITH AN ESCAPE. `\u0066` is `f`, so this is the same
	// field to encoding/json and to the endpoint. Written with the PLAIN name --
	// which is what I did first, twice in one day, in both halves -- this fixture
	// asserts only what already passed, and the mutant said so both times.
	dropFraming("HTTP/1.1 200 OK\r\n\r\n" + `{"subject_\u0066act_key":"sfk1_abababababab"}`)
	if len(structuralSurfaces) == 0 {
		t.Fatal("an escaped member name hid a minted key from the refusal")
	}
}

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

func TestMintedNamesAreDetectedCaseInsensitively(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	dropFraming("HTTP/1.1 200 OK\r\n\r\n" + `{"SUBJECT_FACT_KEY":"sfk1_abababababab"}`)
	if len(structuralSurfaces) == 0 {
		t.Fatal("a case variant of a minted member name was not detected")
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

func TestMintedNamesFoldTheWayEncodingJSONDoes(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	dropFraming("HTTP/1.1 200 OK\r\n\r\n" + "{\"ſubject_fact_key\":\"sfk1_abababababab\"}")
	if len(structuralSurfaces) == 0 {
		t.Fatal("a Unicode case-fold spelling of a minted name was not detected")
	}
}

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
		if err := assertNoLeak(asCaptured(string(redact([]byte(escapeMarks(raw)))))); err != nil {
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
	// And the real one still does.
	structuralSurfaces = nil
	dropFraming("HTTP/1.1 200 OK\r\n\r\n" + `{"subject_fact_key":"sfk1_abab"}`)
	if len(structuralSurfaces) == 0 {
		t.Fatal("the top-level minted field stopped being detected")
	}
}

func TestTheWithheldQueryMarkerIsOneToken(t *testing.T) {
	got := stripMarks(dropQuery("GET /v1/assign?a=b HTTP/1.1\r"))
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
	if err := assertNoLeak(asCaptured(string(redact([]byte(escapeMarks(raw)))))); err != nil {
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
	if err := assertNoLeak(asCaptured(string(redact([]byte(escapeMarks(raw)))))); err != nil {
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
	// ⚠ THE FIXTURE NOW STATES ITS PREMISE INSTEAD OF ENCODING IT AS A PROTOCOL.
	// This scene always meant "the endpoint really sent it", and said so by
	// writing HTTP/1.1 — which was the rule the code used and was WRONG: Go
	// synthesises `Connection: close` for HTTP/1.1 too, whenever the length is
	// unknown (shardpilot/shardpilot-go#84 review). The premise is now the flag
	// the recorder sets from `resp.Header`, so the scene says what it assumes.
	receivedConnection = true
	if err := assertNoLeak(asCaptured(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nConnection: YmFy\r\n\r\n")))); err == nil {
		t.Fatal("a received Connection value was marked generated and skipped")
	}
	// Not received: the serialiser wrote it, whatever the protocol says.
	receivedConnection = false
	suppliedValues = []string{"close"}
	got := stripMarks(scrubSupplied(dropFraming("HTTP/2.0 200 OK\r\nConnection: close\r\n\r\n")))
	if strings.Contains(got, "<redacted") {
		t.Fatalf("the synthesised HTTP/2 Connection line was scrubbed: %q", got)
	}
	// ⚠ AND THE CASE THE PROTOCOL RULE COULD NOT SEE: HTTP/1.1 with a synthesised
	// line, which is what Go writes when the length is unknown. A legal experiment
	// key of `close` used to come back as `Connection: <redacted, 5 chars>`.
	got = stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n")))
	if strings.Contains(got, "<redacted") {
		t.Fatalf("a synthesised HTTP/1 Connection line was scrubbed: %q", got)
	}
}

func TestRefusalLabelsCarryNoEndpointText(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	dropFraming("HTTP/1.1 200 OK\r\n\r\n" + "{\"ſubject_fact_key\":\"x\"}")
	if len(structuralSurfaces) == 0 {
		t.Fatal("a folded minted name stopped being detected")
	}
	for _, w := range structuralSurfaces {
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
	dropFraming("HTTP/1.1 200 OK\r\n\r\n" + `{"assigned":true,"subject_fact_key":"x"`)
	if len(structuralSurfaces) == 0 {
		t.Fatal("a malformed body carrying a minted field stayed publishable")
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
	if !strings.Contains(got, "query-withheld") {
		t.Fatalf("the generated withheld token did not survive: %q", got)
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

// ---- round on 9721c2d ----

// TestATrailingJSONValueIsNotGrammar is the reviewer's own example. `json.Decoder`
// reads a SEQUENCE of top-level values, so `{"x":1} false` walked as valid JSON
// and the trailing literal was marked as grammar — and a marked span is skipped by
// both the scrub and the guard, while `json.Unmarshal` rejects that body as a
// verdict outright (shardpilot/shardpilot-go#84 review).
func TestATrailingJSONValueIsNotGrammar(t *testing.T) {
	suppliedValues = []string{"false"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(markBareJSONLiterals(`{"x":1} false`, assignmentTopLevel)))
	if strings.Contains(got, "false") {
		t.Fatalf("a supplied value was marked as grammar in a multi-value body: %q", got)
	}
	// ⚠ AND A REAL VERDICT BODY STILL HAS ITS GRAMMAR PROTECTED, or the repair is
	// just a refusal to mark anything.
	got = stripMarks(scrubSupplied(markBareJSONLiterals(`{"assigned":false}`, assignmentTopLevel)))
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
		got := stripMarks(scrubSupplied(markBareJSONLiterals(body, assignmentTopLevel)))
		if strings.Contains(got, "false") {
			t.Fatalf("a supplied value was marked as grammar in %q: %q", body, got)
		}
	}
	// ⚠ AND AN ORDINARY BODY, WITH THE WHITESPACE A SERVER ACTUALLY SENDS, still
	// has its grammar protected — otherwise the repair is a refusal to mark.
	got := stripMarks(scrubSupplied(markBareJSONLiterals("\n  "+`{"assigned":false}`+"\n", assignmentTopLevel)))
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
	noteMinted(`[{"subject_fact_key":"ordinary payload"}]`)
	if len(structuralSurfaces) != 0 {
		t.Fatalf("valid non-object JSON was refused as unclassifiable: %q", structuralSurfaces)
	}
	// ⚠ AND A BODY THAT GENUINELY DOES NOT PARSE STILL FAILS CLOSED.
	structuralSurfaces = nil
	noteMinted(`{"assigned":true,"subject_fact_key":"x`)
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
	if err := assertNoLeak(responseText(first)); err == nil {
		t.Fatal("a received Connection value was treated as serialiser syntax and skipped")
	}
	// And the synthesised one is still exempt, or the repair refuses every capture.
	receivedConnection = false
	suppliedValues = []string{"close"}
	if err := assertNoLeak(responseText(last)); err != nil {
		t.Fatalf("a synthesised Connection line was reported as a leak: %v", err)
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

// TestARecognisedMediaTypeIsGrammar: with a legal experiment key of `json`, the
// ordinary `Content-Type: application/json` came back as
// `application/<redacted, 4 chars>` — the recorded response no longer declaring
// its own media type, approved because the placeholder is generated
// (shardpilot/shardpilot-go#84 review).
func TestARecognisedMediaTypeIsGrammar(t *testing.T) {
	suppliedValues = []string{"json"}
	receivedConnection = true
	t.Cleanup(func() { suppliedValues = nil; receivedConnection = false })
	got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n")))
	if !strings.Contains(got, "Content-Type: application/json") {
		t.Fatalf("a registered media type was scrubbed into an undeclarable one: %q", got)
	}
	// ⚠ THE PARAMETERS ARE STILL THE ENDPOINT'S. Only the type/subtype is fixed by
	// the registry; a boundary or charset value is a string the origin chose.
	suppliedValues = []string{"utf-8"}
	got = stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\n\r\n")))
	if strings.Contains(got, "charset=utf-8") {
		t.Fatalf("a media-type PARAMETER was vouched for: %q", got)
	}
	// ⚠ AND AN UNREGISTERED TYPE IS NOT VOUCHED FOR, or the fix publishes whatever
	// the endpoint puts in that position.
	suppliedValues = []string{"server-secret"}
	got = stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\nContent-Type: application/server-secret\r\n\r\n")))
	if strings.Contains(got, "server-secret") {
		t.Fatalf("an unregistered media type was published: %q", got)
	}
}

// ---- round on b86853d ----

// TestTheQuerySeparatorSurvives: concatenating the marker onto the path published
// `/api/…/assignmentquery-withheld` — a route the SDK never requested, on every
// successful report, since every assignment request carries a query
// (shardpilot/shardpilot-go#84 review).
func TestTheQuerySeparatorSurvives(t *testing.T) {
	suppliedValues = nil
	got := stripMarks(dropQuery("GET /api/v1/runtime/experiments/assignment?a=1 HTTP/1.1\r"))
	if !strings.Contains(got, "assignment?") {
		t.Fatalf("the query separator was consumed with the query: %q", got)
	}
	if !strings.Contains(got, "HTTP/1.1") {
		t.Fatalf("the request line lost its version: %q", got)
	}
	// A fragment separator is syntax in the same way.
	got = stripMarks(dropQuery("Location: /cb#tok"))
	if !strings.Contains(got, "/cb#") {
		t.Fatalf("the fragment separator was consumed: %q", got)
	}
}

// TestBinaryCandidatesSeeTheNameForms: a field name whose component decodes to
// invalid UTF-8 — `X-_2Jhcg`, where `_2Jhcg` is url-base64 for `0xffbar` — was
// examined as one unsplit token and no candidate ever held the value
// (shardpilot/shardpilot-go#84 review).
func TestBinaryCandidatesSeeTheNameForms(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("HTTP/1.1 200 OK\r\nX-_2Jhcg: v\r\n\r\n")); err == nil {
		t.Fatal("a binary decode of a header-name component was never examined")
	}
}

// ---- round on a2457d6 ----

// TestATrailerIsNotAMessageStart: `trailerReport` wraps each trailer in its own
// captured span, so a trailer named `X-YmFy` carrying the legal value `HTTP/1.1`
// was handed to `dataOf` as though it were a request line — the value dropped, the
// header name never collected, and `YmFy` decoded as one token
// (shardpilot/shardpilot-go#84 review).
func TestATrailerIsNotAMessageStart(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("X-YmFy: HTTP/1.1")); err == nil {
		t.Fatal("a trailer line was treated as a message start and skipped")
	}
	// ⚠ AND A REAL MESSAGE START IS STILL EXEMPT, or the repair refuses every
	// capture: the version and code of a status line are syntax.
	suppliedValues = []string{"200"}
	if err := assertNoLeak(asCaptured("HTTP/1.1 200 OK\r\nX-A: b\r\n")); err != nil {
		t.Fatalf("a status line's own code was reported as a leak: %v", err)
	}
	suppliedValues = []string{"GET"}
	if err := assertNoLeak(asCaptured("GET /cb HTTP/1.1\r\nX-A: b\r\n")); err != nil {
		t.Fatalf("a request line's method was reported as a leak: %v", err)
	}
}

// TestTheConfiguredHostIsOurs: with `SP_REMOTE_CONFIG_URL` of
// `https://app.shardpilot.com` and a legal experiment key of `app`, the guard found
// `app` inside the host this program itself configured and refused every otherwise
// valid capture (shardpilot/shardpilot-go#84 review).
func TestTheConfiguredHostIsOurs(t *testing.T) {
	suppliedValues = []string{"app"}
	configuredHost = "app.shardpilot.com"
	t.Cleanup(func() { suppliedValues = nil; configuredHost = "" })
	// ⚠ ASSERTED ON THE SCRUBBED OUTPUT. My first version asserted on `redact`'s
	// own result, which does not scrub — so it passed with and without the fix, and
	// the mutant survived. And the symptom is not the one the finding names:
	// measured, the guard does NOT refuse, because `scrubSupplied` has already
	// replaced the value. What is published is `Host: <redacted, 3 chars>.
	// shardpilot.com` — an authority no parser accepts, approved because the
	// placeholder is generated. Same class, different consequence.
	got := stripMarks(scrubSupplied(string(redact(
		[]byte("GET /x HTTP/1.1\r\nHost: app.shardpilot.com\r\n\r\n")))))
	if !strings.Contains(got, "Host: app.shardpilot.com") {
		t.Fatalf("the configured authority was rewritten: %q", got)
	}
	// ⚠ AND A DIFFERENT HOST IS STILL ENDPOINT TEXT, or the exemption covers
	// whatever stands in that position.
	suppliedValues = []string{"elsewhere"}
	got = stripMarks(scrubSupplied(string(redact(
		[]byte("GET /x HTTP/1.1\r\nHost: elsewhere.invalid\r\n\r\n")))))
	if strings.Contains(got, "Host: elsewhere.invalid") {
		t.Fatalf("an unconfigured authority was vouched for: %q", got)
	}
}

// TestATransportErrorGoesThroughTheStructuralQuestion: Go rejects a malformed
// response before returning one and puts the complete bad header into the error, so
// a server-set cookie reached the report through the error diagnostic where only
// the supplied-value scrub ran (shardpilot/shardpilot-go#84 review).
func TestATransportErrorGoesThroughTheStructuralQuestion(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	_ = sanitizeCaptured(errors.New(
		"malformed HTTP response \"Set-Cookie: session=abcdefghijkl\""))
	if len(structuralSurfaces) == 0 {
		t.Fatal("a server-set cookie inside a transport error left the capture publishable")
	}
	// ⚠ AND AN ORDINARY ERROR STILL PUBLISHES, or every transport failure becomes
	// unreportable — which is the case this artifact most needs to report.
	structuralSurfaces = nil
	_ = sanitizeCaptured(errors.New("dial tcp: i/o timeout"))
	if len(structuralSurfaces) != 0 {
		t.Fatalf("an ordinary transport error was made unpublishable: %q", structuralSurfaces)
	}
}

// ---- round on 967f7c1 ----

// TestAnEmbeddedBase64SuffixIsTried: the token scan is MAXIMAL, so
// `prefix/YWJjZGVmZ2g=` is one token that does not decode, and the final path
// component — which a standard decoder reconstructs directly — was never tried
// (shardpilot/shardpilot-go#84 review).
func TestAnEmbeddedBase64SuffixIsTried(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("path prefix/YWJjZGVmZ2g= end")); err == nil {
		t.Fatal("an embedded base64 component was never decoded")
	}
}

// TestAMintedFieldInATransportErrorRefuses: an endpoint can make its malformed
// first line a JSON object carrying the subject key, and Go puts that whole line in
// the error — where no supplied value can match it and no body scan runs
// (shardpilot/shardpilot-go#84 review).
func TestAMintedFieldInATransportErrorRefuses(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	_ = sanitizeCaptured(errors.New(
		`malformed HTTP response "{\"subject_fact_key\":\"sfk1_xxxxxxxxxxxx\"}"`))
	if len(structuralSurfaces) == 0 {
		t.Fatal("a minted field inside a transport error left the capture publishable")
	}
}

// TestAnAuthChallengeRefusesOnTheSuccessPathToo: the transport-error path
// classifies these as unpublishable, and that check ran only when PARSING FAILED —
// so an ordinary 401 published the endpoint-minted challenge
// (shardpilot/shardpilot-go#84 review).
func TestAnAuthChallengeRefusesOnTheSuccessPathToo(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = nil
	receivedConnection = true
	t.Cleanup(func() { structuralSurfaces = nil; receivedConnection = false })
	dropFraming("HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Digest nonce=\"abcdefgh\"\r\n\r\n")
	if len(structuralSurfaces) == 0 {
		t.Fatal("an authentication challenge on a valid response stayed publishable")
	}
	// ⚠ AND AN ORDINARY RESPONSE STILL PUBLISHES.
	structuralSurfaces = nil
	dropFraming("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{}")
	if len(structuralSurfaces) != 0 {
		t.Fatalf("an ordinary response was made unpublishable: %q", structuralSurfaces)
	}
}

// ---- round on f639447 ----

// TestTheSDKTaxonomyIsVouchedFor: with a legal experiment key of `not_found` and a
// 404, the SDK deliberately produces `Code == "not_found"` and the generic scrub
// rewrote the verdict to `<redacted, 9 chars>` — the capture losing the
// first-class classification the report exists to record
// (shardpilot/shardpilot-go#84 review).
func TestTheSDKTaxonomyIsVouchedFor(t *testing.T) {
	for _, v := range []string{"not_found", "targeting_unmatched", "kill_switch", "superseded"} {
		suppliedValues = []string{v}
		got := stripMarks(scrubSupplied(vouchTaxonomy(v)))
		suppliedValues = nil
		if got != v {
			t.Fatalf("an SDK classification was rewritten: %q became %q", v, got)
		}
	}
	// ⚠ AND A VALUE THIS SDK DOES NOT PRODUCE IS STILL ENDPOINT TEXT.
	suppliedValues = []string{"server_secret"}
	t.Cleanup(func() { suppliedValues = nil })
	if got := stripMarks(scrubSupplied(vouchTaxonomy("server_secret"))); got == "server_secret" {
		t.Fatalf("an unrecognised classification was vouched for: %q", got)
	}
}

// TestAnEscapedMintedNameInATransportErrorRefuses: a malformed first line may
// spell the member as `subject_fact_key`; splitting on non-identifier bytes
// cuts that into `subject_` and `u0066act_key`, neither of which is the name
// (shardpilot/shardpilot-go#84 review).
func TestAnEscapedMintedNameInATransportErrorRefuses(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	// ⚠ THE ESCAPED SPELLING, which is the whole point: the plain one was already
	// caught by the previous round's fix, so a scene using it measures nothing new.
	_ = sanitizeCaptured(errors.New(
		`malformed HTTP response "{\"subject_\u0066act_key\":\"sfk1_xxxxxxxxxxxx\"}"`))
	if len(structuralSurfaces) == 0 {
		t.Fatal("an escaped minted name inside a transport error stayed publishable")
	}
}

// ⚠ THE POPULATION IS THE MAP, NOT A LIST I RECALLED. Three review rounds asked
// the same question -- which fields carry a value the ENDPOINT mints -- and each
// answered it on one path with a name added by hand, so the header block and the
// trailer report disagreed about `WWW-Authenticate`
// (shardpilot/shardpilot-go#84 review). This draws its rows from
// `serverMintedFields` itself, so a name added there is measured on BOTH paths
// without anyone remembering to come back here.
func TestEveryServerMintedFieldIsRefusedOnBothPaths(t *testing.T) {
	if len(serverMintedFields) == 0 {
		t.Fatal("the registry this sweep derives its population from is empty")
	}
	for name := range serverMintedFields {
		t.Run("header/"+name, func(t *testing.T) {
			structuralSurfaces = nil
			t.Cleanup(func() { structuralSurfaces = nil })
			got := dropFraming("HTTP/1.1 401 Unauthorized\r\n" +
				canonicalFieldName(name) + ": Digest nonce=\"server-secret\"\r\n\r\n")
			if len(structuralSurfaces) == 0 {
				t.Fatalf("a server-minted field stayed publishable as a header: %q", got)
			}
			if strings.Contains(got, "server-secret") {
				t.Fatalf("the endpoint-minted value was printed: %q", got)
			}
		})
		t.Run("transport-error/"+name, func(t *testing.T) {
			structuralSurfaces = nil
			t.Cleanup(func() { structuralSurfaces = nil })
			noteStructuralInText("malformed HTTP response from \"e.example\": " +
				canonicalFieldName(name) + ": Digest nonce=\"server-secret\"")
			if len(structuralSurfaces) == 0 {
				t.Fatal("a server-minted field stayed publishable inside a transport error")
			}
		})
		t.Run("trailer/"+name, func(t *testing.T) {
			structuralSurfaces = nil
			t.Cleanup(func() { structuralSurfaces = nil })
			tee := &teeBody{trailer: http.Header{
				canonicalFieldName(name): []string{`Digest nonce="server-secret"`},
			}}
			got := (&exchange{head: []byte("x"), captured: tee}).trailerReport()
			if len(structuralSurfaces) == 0 {
				t.Fatalf("a server-minted field stayed publishable as a trailer: %q", got)
			}
			if strings.Contains(got, "server-secret") {
				t.Fatalf("the endpoint-minted value was printed: %q", got)
			}
		})
	}
}

// A refusal must not print the thing it refuses: the trailer note composed its
// text out of the field's ARRIVED spelling.
func TestAStructuralNoteDoesNotCarryTheArrivedSpelling(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	tee := &teeBody{trailer: http.Header{"WWW-AuThEnTiCaTe": []string{"Digest x"}}}
	(&exchange{head: []byte("x"), captured: tee}).trailerReport()
	for _, s := range structuralSurfaces {
		if strings.Contains(s, "AuThEnTiCaTe") {
			t.Fatalf("the refusal carried the endpoint's spelling: %q", s)
		}
	}
}

// A close-delimited body carries no brace and does not parse; the minted-field
// scan exists for exactly that shape and was skipped on it.
func TestAMintedFieldIsFoundInABodyWithNoBrace(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	noteMinted(`"subject_fact_key":"sfk1_server_secret"`)
	if len(structuralSurfaces) == 0 {
		t.Fatal("a minted field in a brace-less malformed body stayed publishable")
	}
}

// A decoded suffix must be reachable as its own candidate: spliced back behind
// its separator, the short-value matcher reads it as embedded.
func TestADecodedBase64SuffixIsItsOwnCandidate(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil })
	for _, sep := range []string{"-", "_"} {
		if err := assertNoLeak(asCaptured("x" + sep + "YmFy")); err == nil {
			t.Fatalf("a value reconstructable from a %q-separated suffix passed the guard", sep)
		}
	}
}

// The suffix probes are the quadratic term, and the budget did not count them.
//
// ⚠ MEASURED ON THE PROBES, NOT ON THE VERDICT. The first version of this scene
// fed a 20,000-byte run of separators to `assertNoLeak` and asserted a work-budget
// refusal -- which arrives either way, in 0.08s, because the candidates those
// probes produce blow the budget in the seed loop regardless of whether the
// probing itself was ever charged. It survived both mutants. What the fix changes
// is how much work happens BEFORE anything is counted, so that is what this reads.
func TestSeparatorProbesStopAtTheBudget(t *testing.T) {
	decodeWork = 0
	t.Cleanup(func() { decodeWork = 0 })
	const n = 20000
	tok := strings.Repeat("/", n)
	starts := separatorStarts(tok, 4)
	if len(starts) >= n-8 {
		t.Fatalf("every tail was named: %d probes over a %d-byte run, which is the quadratic scan", len(starts), n)
	}
	if decodeWork <= decodeWorkMax {
		t.Fatalf("the probes stopped for some reason other than the budget: %d bytes charged", decodeWork)
	}
}

// The decode budget is per record: an expensive one must not spend the next
// one's allowance.
func TestTheProbeBudgetDoesNotCarryBetweenRecords(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured(strings.Repeat("/", 20000))); err == nil {
		t.Fatal("the expensive record this scene needs was not refused, so it measures nothing")
	}
	if err := assertNoLeak(asCaptured("HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false}")); err != nil {
		t.Fatalf("an ordinary record was refused on the previous record's budget: %v", err)
	}
}

// The transport error carries a QUOTED line: `net/http` doubles the backslash, so
// one decoding pass is one layer short of the name.
func TestAQuotedTransportEscapeIsDecodedToAFixedPoint(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	noteStructuralInText(`malformed HTTP response from "e.example": "{\\"subject_\\u0066act_key\":\"sfk1_server_secret\"}"`)
	if len(structuralSurfaces) == 0 {
		t.Fatal("a minted name behind Go's own quoting stayed publishable")
	}
}

// A wrapped base64 run may begin after other text on its line.
func TestAWrappedBase64RunSharingItsLineIsJoined(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("prefix: YWJj\r\nZGVmZ2g=")); err == nil {
		t.Fatal("a value a standard decoder reconstructs across the line break passed the guard")
	}
}

// A legal short supplied value travels as bare hex below undoHex's floor.
func TestAShortBareHexValueIsDecoded(t *testing.T) {
	suppliedValues = []string{"ab"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("k=6162")); err == nil {
		t.Fatal("a two-character value spelled in bare hex passed the guard")
	}
}

// A control character inside a supplied value has a JSON spelling, and an outer
// encoding can hide it.
func TestAJSONControlEscapeIsDecoded(t *testing.T) {
	suppliedValues = []string{"a\nb"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("k=%61%5Cnb")); err == nil {
		t.Fatal("a value whose JSON spelling hides a control character passed the guard")
	}
}

// A legal experiment key may be a NUMBER, and an assignment may be at that
// version. The response block redacts the JSON number; this printer did not.
func TestTheVerdictVersionGoesThroughTheScrub(t *testing.T) {
	suppliedValues = []string{"123"}
	t.Cleanup(func() { suppliedValues = nil })
	if got := verdictVersion(123); strings.Contains(got, "123") {
		t.Fatalf("a supplied identifier was reintroduced by the verdict line: %q", got)
	}
	// ⚠ AND THE CALL SITE, WHICH IS OTHERWISE UNREACHABLE. The verdict block is
	// printed inside `main()`, so no fixture can run it -- and a test that calls
	// `verdictVersion` alone passes while the report goes on printing `%d`
	// directly, which is exactly what the mutant showed. The same gap covers the
	// three sibling verdict lines; this binds the one this round is about, and the
	// rest is named rather than implied.
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("the scene cannot read its own subject: %v", err)
	}
	if !strings.Contains(string(src), `verdictVersion(result.Version)`) {
		t.Fatal("the verdict block prints the version without the scrub")
	}
}

// MIME ignores a blank line inside a run, including a run that shares its first
// line with other text.
func TestAWrappedRunSurvivesABlankLine(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("prefix: YWJj\r\n\r\nZGVmZ2g=")); err == nil {
		t.Fatal("a value reconstructable across a blank line passed the guard")
	}
}

// Provenance marks exist for the guard; stderr gets a message, not SOH bytes.
func TestTheStderrSanitizerStripsProvenanceMarks(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	got := sanitize(errors.New("dial tcp: abcdefgh refused"))
	if strings.Contains(got, genMark) || strings.Contains(got, capturedMark) {
		t.Fatalf("a provenance marker reached stderr: %q", got)
	}
	if strings.Contains(got, "abcdefgh") {
		t.Fatalf("stripping the marks published the identifier: %q", got)
	}
}

// A one- or two-character key travels as UNPADDED base64, below the rewrite floor.
func TestAShortRawBase64ValueIsDecoded(t *testing.T) {
	suppliedValues = []string{"ab"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("k=YWI")); err == nil {
		t.Fatal("a two-character value in unpadded base64 passed the guard")
	}
}

// The splitter's alphabet must be as wide as the predicate's fold.
func TestAUnicodeFoldedMintedNameIsTokenised(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	if !isMintedName("\u017fubject_fact_key") {
		t.Skip("the fold this scene depends on is not what isMintedName does")
	}
	noteStructuralInText(`malformed HTTP response from "e.example": {"\u017fubject_fact_key":"sfk1_x"}`)
	if len(structuralSurfaces) == 0 {
		t.Fatal("a minted name behind a Unicode fold stayed publishable")
	}
}

// The schema's member names are grammar; a supplied key equal to one must not
// rewrite the schema -- and a NON-canonical spelling must not be vouched.
func TestASchemaMemberNameIsGrammar(t *testing.T) {
	suppliedValues = []string{"assigned"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n{\"assigned\":true}")))
	if !strings.Contains(got, `"assigned"`) {
		t.Fatalf("the response schema was rewritten by the scrub: %q", got)
	}
	suppliedValues = []string{"ASSIGNED"}
	got = stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n{\"ASSIGNED\":true}")))
	if strings.Contains(got, "ASSIGNED") {
		t.Fatalf("a non-canonical spelling was vouched as grammar: %q", got)
	}
}

// TestTheClaimNamesExactlyTheDecodersThatRun holds the claim's enumeration and
// the decoding chain together.
//
// ⚠ A CLAIM THAT NAMES A LIST CAN DRIFT FROM IT SILENTLY, and prose has no
// compiler. The first clause of this program's claim was narrowed from "every
// form the decoders reach" -- an unenumerable set -- to the decoders themselves,
// which is only an improvement while the two agree. This reads both out of the
// source: the documented names, and the stage list the chain actually runs.
func TestTheClaimNamesExactlyTheDecodersThatRun(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("the scene cannot read its own subject: %v", err)
	}
	text := string(src)

	stage := regexp.MustCompile(`range \[\]func\(string\) string\{([^}]*)\}`)
	ms := stage.FindAllStringSubmatch(text, -1)
	if len(ms) == 0 {
		t.Fatal("the decoding chain's stage list was not found, so this scene measures nothing")
	}
	inCode := map[string]bool{}
	for _, m := range ms {
		for _, n := range strings.Split(m[1], ",") {
			if n = strings.TrimSpace(n); n != "" {
				inCode[n] = true
			}
		}
	}

	claim := text[:strings.Index(text, "\npackage ")]
	for n := range inCode {
		if !strings.Contains(claim, n) {
			t.Errorf("the chain runs %s and the claim does not name it", n)
		}
	}
	// ⚠ THE PATTERN NEEDS AN UPPERCASE LETTER AND MUST NOT STOP AT A DIGIT. The
	// first version was `undo[A-Za-z]+`, which reported the claim as naming
	// `undoBase` (cut before the 64) and `undocumented` (a word in the prose) --
	// two failures on a correct claim, from the instrument rather than the subject.
	named := regexp.MustCompile(`\bundo[A-Z][A-Za-z0-9]*\b`).FindAllString(claim, -1)
	for _, n := range named {
		if !inCode[n] {
			t.Errorf("the claim names %s and the chain does not run it", n)
		}
	}
	if len(named) == 0 {
		t.Fatal("the claim names no decoder, so this scene cannot discriminate")
	}
}

// A registry's SCOPE is part of what it says: `benignTopLevel` describes the
// top-level schema, and a nested member of the same name is the endpoint's.
func TestASchemaNameIsGrammarOnlyAtTheRoot(t *testing.T) {
	suppliedValues = []string{"assigned"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n{\"assigned\":true,\"variant_payload\":{\"assigned\":\"x\"}}")))
	if strings.Count(got, "assigned") != 1 {
		t.Fatalf("the nested endpoint-controlled name was exempted too: %q", got)
	}
}

// A container VALUE consumes its parent's turn; the member after it is a key.
func TestTheMemberAfterAContainerValueIsStillAKey(t *testing.T) {
	suppliedValues = []string{"version"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n{\"variant_payload\":{},\"version\":1}")))
	if !strings.Contains(got, `"version"`) {
		t.Fatalf("a fixed schema member after a container value was rewritten: %q", got)
	}
}

// Two rules that each cover half of a case do not cover the case.
func TestAShortBase64SuffixAfterASeparatorIsDecoded(t *testing.T) {
	suppliedValues = []string{"a"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("prefix/YQ")); err == nil {
		t.Fatal("a one-character value after path punctuation passed the guard")
	}
}

// MIME ignores the whitespace a line-based reading treats as structure.
func TestAWrappedRunSurvivesAHorizontalWhitespaceLine(t *testing.T) {
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("prefix: YWJj\r\n \t\r\nZGVmZ2g=")); err == nil {
		t.Fatal("a value reconstructable across a whitespace-only line passed the guard")
	}
}

// Candidate assembly is LINEAR, which is a claim about cost and therefore about
// time.
//
// ⚠ THE FIRST VERSION OF THIS SCENE ASSERTED THE BUDGET FIRED, and it does not
// have to: once the assembly uses a builder the work IS linear -- 160 KB for this
// input -- so the ceiling is never reached and the scene failed on the fixed code.
// The subject is the quadratic, and the only honest instrument for "not
// quadratic" here is elapsed time. The bound is set FROM A MEASUREMENT of both
// forms rather than guessed: on this input the builder takes 0.00s and the `+=`
// form takes 1.55s, so the first bound I wrote -- two seconds -- did not separate
// them and the mutant passed. 500ms sits three times above the linear form and
// three times below the quadratic one.
func TestWrappedCandidateAssemblyIsLinear(t *testing.T) {
	decodeWork = 0
	t.Cleanup(func() { decodeWork = 0 })
	var b strings.Builder
	b.WriteString("prefix: YWJj\r\n")
	for i := 0; i < 40000; i++ {
		b.WriteString("YWJj\r\n")
	}
	b.WriteString("x ZGVmZ2g=")
	in := b.String()

	start := time.Now()
	got := wrappedBase64Candidates(in)
	elapsed := time.Since(start)

	if len(got) == 0 {
		t.Fatal("no candidate was assembled, so the scene measured an empty loop")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("assembling one candidate over %d lines took %v: that is the quadratic copy", 40000, elapsed)
	}
	if decodeWork == 0 {
		t.Fatal("the assembly charged nothing to the decode budget")
	}
}

// Go's quoting alphabet is not JSON's, and `encodingsOf` emits both.
func TestGoControlEscapesAreDecoded(t *testing.T) {
	// BOTH escapes Go writes and JSON does not: a scene covering one of them leaves
	// the other unmeasured, and the mutant that removed `\a` survived the first
	// version of this.
	for _, c := range []struct{ val, wire string }{
		{"a\vb", "k=%61%5Cvb"},
		{"a\ab", "k=%61%5Cab"},
	} {
		suppliedValues = []string{c.val}
		err := assertNoLeak(asCaptured(c.wire))
		suppliedValues = nil
		if err == nil {
			t.Fatalf("a value whose Go spelling hides a control byte passed the guard: %q", c.wire)
		}
	}
}

// The SDK's classification reaches the error path too.
func TestTheTaxonomyInsideAFetchErrorIsVouched(t *testing.T) {
	suppliedValues = []string{"not_found"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(sanitizeCaptured(
		errors.New("shardpilot experiment assignment fetch failed: not_found")))
	if !strings.Contains(got, "not_found") {
		t.Fatalf("the SDK's own classification was rewritten inside the error: %q", got)
	}
}

// "Short" means characters everywhere else; this threshold counted bytes.
func TestTheShortValueThresholdCountsCharacters(t *testing.T) {
	suppliedValues = []string{"éééé"}
	t.Cleanup(func() { suppliedValues = nil })
	// ⚠ THE TEXT MUST BE UNCHANGED, not merely still bracketed by its neighbours.
	// The first version asserted `α` and `β` survive -- they survive the
	// unconditional replacement too, so it matched something other than its subject.
	// `éééé` here is EMBEDDED, and the boundary rule that applies to short values is
	// exactly what the byte-length threshold was skipping.
	if got := stripMarks(scrubSupplied(asCaptured("αééééβ"))); !strings.Contains(got, "αééééβ") {
		t.Fatalf("an embedded short value was replaced past its boundary rule: %q", got)
	}
}

// The identity branch is an early return, and owes what the common path does.
func TestTheIdentityBranchAdmitsItsFieldNameAndSpelling(t *testing.T) {
	suppliedValues = []string{"IDENTITY"}
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nContent-Encoding: IDENTITY\r\n\r\n{\"assigned\":false}")))
	suppliedValues = nil
	if strings.Contains(got, "IDENTITY") {
		t.Fatalf("a non-canonical coding spelling was vouched: %q", got)
	}
	suppliedValues = []string{"Content-Encoding"}
	t.Cleanup(func() { suppliedValues = nil })
	got = stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nContent-Encoding: identity\r\n\r\n{\"assigned\":false}")))
	// ⚠ THE FIELD NAME IS THE SUBJECT, AND THE PROPERTY IS THAT IT IS STILL A FIELD
	// NAME. Two earlier versions of this assertion were wrong: the first checked
	// that `identity` survives, which it does either way since the branch marks the
	// value; the second checked the name survives VERBATIM, which it must not -- the
	// name equals a supplied identifier and is redacted. What the early return
	// skipped is the TOKEN-SAFE spelling, so the header stayed parsable.
	name := strings.SplitN(strings.SplitN(got, "\r\n", 2)[1], ":", 2)[0]
	if strings.ContainsAny(name, " <>,") || name == "" {
		t.Fatalf("the early return left a field name no parser accepts: %q", got)
	}
	if strings.Contains(name, "Content-Encoding") {
		t.Fatalf("a supplied identifier survived in the field name: %q", got)
	}
}

// A media type is recognised by folding and vouched by spelling.
func TestOnlyACanonicalMediaTypeSpellingIsVouched(t *testing.T) {
	suppliedValues = []string{"APPLICATION"}
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nContent-Type: APPLICATION/JSON\r\n\r\n{\"assigned\":false}")))
	suppliedValues = nil
	if strings.Contains(got, "APPLICATION") {
		t.Fatalf("a non-canonical media-type spelling was vouched: %q", got)
	}
	suppliedValues = []string{"application/json"}
	t.Cleanup(func() { suppliedValues = nil })
	if v := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"assigned\":false}"))); !strings.Contains(v, "application/json") {
		t.Fatalf("the canonical spelling lost its vouching: %q", v)
	}
}

// One notion, one threshold: the scrub and the guard must agree about "short".
func TestTheGuardsThresholdMatchesTheScrubs(t *testing.T) {
	suppliedValues = []string{"éééé"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("αééééβ")); err != nil {
		t.Fatalf("the guard refused text the scrub deliberately left alone: %v", err)
	}
}

// The undecodable-coding refusal is about what a body could hide.
func TestACodingWithNoBodyIsNotRefused(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	dropFraming("HTTP/1.1 204 No Content\r\nContent-Encoding: br\r\n\r\n")
	if len(structuralSurfaces) != 0 {
		t.Fatalf("a coding on an empty body was refused: %v", structuralSurfaces)
	}
	// AND A CODING WITH A BODY STILL IS.
	structuralSurfaces = nil
	dropFraming("HTTP/1.1 200 OK\r\nContent-Encoding: br\r\n\r\n\x1f\x8b\x08")
	if len(structuralSurfaces) == 0 {
		t.Fatal("an undecodable coding over a real body stopped being refused")
	}
}

// Inside a field NAME, every token punctuation separates -- including `_`.
func TestAnUnderscoreSeparatesInsideAFieldName(t *testing.T) {
	suppliedValues = []string{"bar"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nX_bar: v\r\n\r\n{\"assigned\":false}")))
	if strings.Contains(got, "bar") {
		t.Fatalf("a supplied identifier survived in a field name across an underscore: %q", got)
	}
}

// An undecodable coding is about BYTES, not about content worth reading.
//
// ⚠ THE POPULATION IS "A BODY", AND `TrimSpace` NAMED ONLY THE NON-BLANK HALF OF
// IT. A response with an unsupported coding and a body of ` \t` was published:
// the trimmed body looked empty, the structural refusal was skipped, and a
// decoder for that coding may reconstruct a supplied value from exactly those
// bytes (shardpilot/shardpilot-go#84 review). The round before wrote the trim to
// spare a 204 its diagnostic; a 204 has no bytes, so length answers both.
//
// Rows are the product of the whitespace bytes HTTP can leave in a body with the
// empty and non-empty cases, so the rule is read as "any bytes at all" rather
// than as the two spellings that happened to be shown.
func TestAWhitespaceOnlyEncodedBodyIsStillABody(t *testing.T) {
	for _, c := range []struct {
		body    string
		refused bool
	}{
		{"", false}, {" ", true}, {"\t", true}, {"\n", true}, {"\r\n", true},
		{" \t", true}, {"\r\n\r\n", true}, {"x", true}, {" x ", true},
	} {
		structuralSurfaces = nil
		dropFraming("HTTP/1.1 200 OK\r\nContent-Encoding: x-private\r\n\r\n" + c.body)
		if got := len(structuralSurfaces) > 0; got != c.refused {
			t.Errorf("body %q under an unsupported coding: refused=%v, want %v", c.body, got, c.refused)
		}
	}
	structuralSurfaces = nil
}

// The exemption sets are the wire shapes' members, read out of the SDK source.
//
// ⚠ AND THERE IS NO ALLOWLIST BESIDE THE DERIVATION. The first version of this
// scene derived the assignment members with `go/ast` and then wrote
// `errorBody := {"error","code"}` by hand as "a different wire shape" -- and
// `code` is a member of no top-level shape at all, so the derivation was right and
// missed exactly the name that had been lifted out of it
// (shardpilot/shardpilot-go#85 review). A list beside a derived check is where the
// assumption the check just closed goes to live. Both shapes are read from the
// source now.
func TestTheTopLevelExemptionsAreExactlyTheWireMembers(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "../../experiments.go", nil, 0)
	if err != nil {
		t.Fatalf("the SDK source is the oracle and it could not be read: %v", err)
	}
	tags := func(st *ast.StructType) map[string]bool {
		out := map[string]bool{}
		for _, fld := range st.Fields.List {
			if fld.Tag == nil {
				continue
			}
			tag, err := strconv.Unquote(fld.Tag.Value)
			if err != nil {
				continue
			}
			if name, _, _ := strings.Cut(reflect.StructTag(tag).Get("json"), ","); name != "" && name != "-" {
				out[name] = true
			}
		}
		return out
	}
	wire, errWire := map[string]bool{}, map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		if ts, ok := n.(*ast.TypeSpec); ok && ts.Name.Name == "expAssignmentWire" {
			if st, ok := ts.Type.(*ast.StructType); ok {
				wire = tags(st)
			}
			return false
		}
		// The error body is an anonymous struct inside `experimentBodyErrorText`.
		if fd, ok := n.(*ast.FuncDecl); ok && fd.Name.Name == "experimentBodyErrorText" {
			ast.Inspect(fd, func(m ast.Node) bool {
				if st, ok := m.(*ast.StructType); ok {
					for k := range tags(st) {
						errWire[k] = true
					}
				}
				return true
			})
			return false
		}
		return true
	})
	if len(wire) == 0 || len(errWire) == 0 {
		t.Fatalf("the oracle read %d assignment members and %d error members: finding nothing is not agreement",
			len(wire), len(errWire))
	}
	for name := range wire {
		if !assignmentTopLevel[name] && !mintedNames[name] {
			t.Errorf("%q is a member of expAssignmentWire and is exempted nowhere: an ordinary capture loses it", name)
		}
	}
	for name := range assignmentTopLevel {
		if !wire[name] {
			t.Errorf("%q is exempted in an assignment body and is not a member of expAssignmentWire", name)
		}
	}
	for name := range errWire {
		if !errorTopLevel[name] {
			t.Errorf("%q is a member of the error body and is exempted nowhere", name)
		}
	}
	for name := range errorTopLevel {
		if !errWire[name] {
			t.Errorf("%q is exempted in an error body and the SDK's error shape has no such member", name)
		}
	}
}

// A name fixed in one shape is endpoint text in the other.
func TestExemptionsFollowTheResponseShape(t *testing.T) {
	for _, c := range []struct{ supplied, head, body string }{
		{"error", "HTTP/1.1 200 OK", `{"assigned":false,"error":"x"}`},
		{"assigned", "HTTP/1.1 401 Unauthorized", `{"error":"unauthorized","assigned":"x"}`},
		{"variant_key", "HTTP/1.1 500 Internal Server Error", `{"error":"boom","variant_key":"x"}`},
	} {
		suppliedValues = []string{c.supplied}
		structuralSurfaces = nil
		got := stripMarks(scrubSupplied(dropFraming(c.head + "\r\n\r\n" + c.body)))
		if strings.Contains(got, `"`+c.supplied+`"`) {
			t.Errorf("%s with %q supplied: the name was marked as grammar and published: %q",
				c.head, c.supplied, got)
		}
		suppliedValues = nil
		structuralSurfaces = nil
	}
	// And the shape's OWN names still pass, or every ordinary capture loses them.
	for _, c := range []struct{ head, body, keep string }{
		{"HTTP/1.1 200 OK", `{"assigned":false,"variant_key":"v"}`, "variant_key"},
		{"HTTP/1.1 401 Unauthorized", `{"error":"unauthorized"}`, "error"},
	} {
		suppliedValues = nil
		structuralSurfaces = nil
		got := stripMarks(scrubSupplied(dropFraming(c.head + "\r\n\r\n" + c.body)))
		if !strings.Contains(got, `"`+c.keep+`"`) {
			t.Errorf("%s: the shape's own member %q was rewritten: %q", c.head, c.keep, got)
		}
	}
}

// The taxonomy is what the SDK writes, and the SDK enumerates it in its own
// source.
//
// ⚠ PART OF IT IS GENERATED, SO A LIST CANNOT HOLD IT. `"http_" + Itoa(status)`
// and `"transient_" + Itoa(status)` are unbounded families, and the registry held
// four static entries -- so every `http_503` or `transient_408` verdict lost its
// classification to a placeholder on an ORDINARY capture
// (shardpilot/shardpilot-go#84 review).
//
// The population is parsed out of the doc comment where the SDK states the
// taxonomy, so a code added there turns this red. A template token is exercised
// with a concrete status rather than skipped, because the templates are exactly
// the part a list cannot express.
func TestTheTaxonomyCoversTheSDKsOwnEnumeration(t *testing.T) {
	src, err := os.ReadFile("../../experiments.go")
	if err != nil {
		t.Fatalf("the SDK source is the oracle and it could not be read: %v", err)
	}
	lines := strings.Split(string(src), "\n")
	anchor := -1
	for i, l := range lines {
		if strings.Contains(l, "the taxonomy code in the error text") {
			anchor = i
			break
		}
	}
	if anchor < 0 {
		t.Fatal("the enumeration this scene reads is gone from the SDK source: an oracle that finds nothing is not agreement")
	}
	lo, hi := anchor, anchor
	for lo > 0 && strings.HasPrefix(strings.TrimSpace(lines[lo-1]), "//") {
		lo--
	}
	for hi < len(lines)-1 && strings.HasPrefix(strings.TrimSpace(lines[hi+1]), "//") {
		hi++
	}
	quoted := regexp.MustCompile(`"([a-z0-9_<>.]+)"`)
	var seen int
	for _, m := range quoted.FindAllStringSubmatch(strings.Join(lines[lo:hi+1], "\n"), -1) {
		tok := m[1]
		// A template stands for the generated family; exercise it with a status
		// the SDK would actually render.
		probe := tok
		switch {
		case strings.Contains(tok, "<status>"):
			probe = strings.ReplaceAll(tok, "<status>", "503")
		case strings.HasSuffix(tok, "..."):
			probe = strings.TrimSuffix(tok, "...") + "503"
		}
		seen++
		if !isSDKTaxonomy(probe) {
			t.Errorf("the SDK names %q as a taxonomy code and this build does not vouch %q: an ordinary verdict loses its classification", tok, probe)
		}
	}
	if seen < 5 {
		t.Fatalf("only %d codes were read from the enumeration: the oracle is not reading what it thinks it is", seen)
	}
	// And the canonical spelling only: an endpoint may echo a shape the SDK never
	// writes, and recognising a shape is not having written it.
	for _, no := range []string{"http_007", "http_+7", "http_ 7", "http_", "http_5o3", "transient_-1", "HTTP_503"} {
		if isSDKTaxonomy(no) {
			t.Errorf("%q is vouched as this SDK's taxonomy and the SDK never writes it", no)
		}
	}
}

// The rendered-text vouch has to reach the generated families too, and it must
// still refuse a near-miss.
func TestGeneratedTaxonomyIsVouchedInRenderedText(t *testing.T) {
	for _, c := range []struct {
		tok     string
		vouched bool
	}{
		{"http_503", true}, {"transient_408", true}, {"http_0", true},
		{"kill_switch", true},
		{"http_007", false}, {"http_+7", false}, {"http_5o3", false},
		{"transient_-1", false}, {"HTTP_503", false},
	} {
		got := vouchTaxonomyIn("shardpilot experiment assignment fetch failed: " + c.tok + " end")
		if marked := strings.Contains(got, genMark); marked != c.vouched {
			t.Errorf("%q vouched=%v, want %v: %q", c.tok, marked, c.vouched, got)
		}
	}
}

// A combining mark continues the word it is attached to.
//
// ⚠ THE POPULATION IS "UNICODE MARK", NOT U+0301. `IsLetter || IsDigit` put every
// mark in the boundary class, so a supplied short value adjacent to a decomposed
// grapheme was replaced while the mark stayed behind -- evidence altered into
// something the endpoint never sent, and approved, because the placeholder is
// generated (shardpilot/shardpilot-go#84 review).
//
// The rows are the product of the three mark categories with both sides of the
// value, generated from `unicode.Mn/Mc/Me` rather than from the one code point
// the review named.
func TestACombiningMarkDoesNotDetachFromItsLetter(t *testing.T) {
	marks := []rune{'́', 'ٔ', 'ः', '⃝'} // Mn, Mn, Mc, Me
	for _, m := range marks {
		if !unicode.IsMark(m) {
			t.Fatalf("the scene's own population is wrong: %U is not a mark", m)
		}
		for _, side := range []string{"after", "before"} {
			text := "a" + string(m)
			if side == "before" {
				text = string(m) + "a"
			}
			got := replaceTokenWith(text, "a", "<X>", isWordByte)
			if got != text {
				t.Errorf("%s %U: %q became %q -- the mark was left detached from its letter",
					side, m, text, got)
			}
		}
	}
}

// MIME ignores horizontal whitespace, so the guard must too — wherever it sits.
//
// ⚠ THE POPULATION IS "WHERE A SPACE CAN SIT IN A WRAPPED RUN", NOT THE ONE
// PLACEMENT SHOWN. The whole-line producer normalised spaces and tabs and the
// shared-line producer did not, so a run carrying whitespace produced no
// candidate and `assertNoLeak` approved a spelling a standard decoder turns
// straight back into the identifier (shardpilot/shardpilot-go#84 review).
// Measured before fixing: three of the four placements broke it.
//
// The rows are the product of the whitespace bytes with the placements and with
// both ways a run can end, including ending WITH THE TEXT — which produced no
// candidate at all and was found by measuring rather than by being told.
func TestWrappedBase64SurvivesWhitespaceWhereverItSits(t *testing.T) {
	// "abcdefgh" is YWJjZGVmZ2g=, wrapped after four characters.
	const head, tail = "YWJj", "ZGVmZ2g="
	suppliedValues = []string{"abcdefgh"}
	t.Cleanup(func() { suppliedValues = nil; decodeWork = 0 })
	for _, ws := range []string{" ", "\t", "  ", " \t"} {
		placements := map[string]string{
			"after the head":          "prefix: " + head + ws + "\r\n" + tail,
			"before the tail":         "prefix: " + head + "\r\n" + ws + tail,
			"inside the tail":         "prefix: " + head + "\r\n" + tail[:4] + ws + tail[4:],
			"inside the head":         "prefix: " + head[:2] + ws + head[2:] + "\r\n" + tail,
			"on both sides of a fold": "prefix: " + head + ws + "\r\n" + ws + tail,
		}
		for where, run := range placements {
			for _, ending := range []string{"", "\r\nend."} {
				decodeWork = 0
				if err := assertNoLeak(asCaptured(run + ending)); err == nil {
					t.Errorf("%q %s, ending %q: the guard approved a run a MIME decoder reconstructs",
						ws, where, ending)
				}
			}
		}
	}
}

// A field name is a token, and the stand-in for a generated span has to be one.
//
// ⚠ THE VALUE SIDE AND THE NAME SIDE WANT DIFFERENT STAND-INS. Blanking a
// generated span stops two unrelated value fragments reading as one token, and it
// makes a field NAME syntactically invalid -- so `headerNameEnd` failed, the
// name-specific decoded boundary check never ran, and with `bar` and `qux`
// supplied the capture `X-redacted-3-chars-%71ux` was approved, from which the
// supported percent decoder reconstructs `qux` (shardpilot/shardpilot-go#84
// review).
//
// The rows are the product of where the encoded value sits in the name with which
// of its bytes are encoded, because the defect is about the name PARSING at all
// and any of these spellings exercises it.
func TestAGeneratedSpanLeavesTheFieldNameParsable(t *testing.T) {
	spellings := []string{"%71ux", "q%75x", "qu%78", "%71%75%78"}
	for _, enc := range spellings {
		for _, name := range []string{"X-bar-" + enc, "X-" + enc + "-bar", "bar-" + enc} {
			suppliedValues = []string{"bar", "qux"}
			structuralSurfaces = nil
			got := scrubSupplied(dropFraming("GET / HTTP/1.1\r\n" + name + ": v\r\n\r\n"))
			// ⚠ THE PROPERTY IS THE DISJUNCTION, AND IT HAS TO BE, BECAUSE THE TWO
			// BRANCHES OF THE STACK PUBLISH DIFFERENT THINGS. This branch replaces the
			// whole field name, so the encoded spelling never reaches the output and
			// there is nothing to refuse; the parent leaves `X-<gen>-%71ux` standing and
			// must refuse it. Asserting "the guard refuses" holds on one side only, and
			// a scene that holds on one side of a seam is a scene that will be edited
			// at the merge rather than read.
			if strings.Contains(stripMarks(got), enc) && assertNoLeak(asCaptured(got)) == nil {
				t.Errorf("%q: the guard approved %q, which still carries %q and the percent decoder turns that back into a supplied value",
					name, stripMarks(got), enc)
			}
		}
	}
	suppliedValues = nil
	structuralSurfaces = nil
}

// The transport consumes `Connection: close`, and the dump writes it back.
//
// ⚠ MEMBERSHIP IN `resp.Header` IS NOT "THE ENDPOINT SENT IT". Go removes
// `Connection: close` from the header map and represents it as `resp.Close`,
// while `DumpResponse` reconstructs the line -- so this recorded "not received"
// for a line the endpoint DID send, `dropFraming` marked the whole line as
// serializer-generated, and a supplied `close` was published because the guard
// skips generated spans (shardpilot/shardpilot-go#84 review).
//
// Fourth round on one line, and the rows are the product of the two ways the
// signal can arrive -- in the map, or consumed into `resp.Close` -- with the case
// where it did not arrive at all.
func TestAConsumedCloseSignalIsStillReceived(t *testing.T) {
	// ⚠ AND `resp.Close` HAS TWO CAUSES. Go sets it when the transport CONSUMED
	// `Connection: close`, and also when an HTTP/1.1 response is close-delimited --
	// no length, no chunked framing, body ends at EOF. `DumpResponse` synthesises
	// the line either way, so the two are separable only by the FRAMING
	// (shardpilot/shardpilot-go#84 review). The round before read both as received
	// and this table had no framing column at all.
	//
	// The rows are the product of the header's presence with `resp.Close` and with
	// the three framings a response can have.
	// ⚠ AND THE PROTOCOL IS A THIRD AXIS, because BELOW HTTP/1.1 closure is the
	// default: `net/http` sets `Close` on a 1.0 response carrying an explicit
	// `Content-Length` and no `Connection` field at all -- measured with
	// `http.ReadResponse`, which gives `Close=true` there and false for the same
	// response as 1.1. So framing separated the two provenances only above 1.0, and
	// the row that proves it was missing rather than wrong
	// (shardpilot/shardpilot-go#84 review). The expectation carries `ambiguous` too:
	// a scene that reads only `recvConn` cannot tell "the endpoint did not send it"
	// from "we cannot tell", and those are the two answers this branch chooses
	// between.
	for _, c := range []struct {
		name      string
		minor     int
		header    http.Header
		close     bool
		length    int64
		chunked   bool
		received  bool
		ambiguous bool
	}{
		{"in the map", 1, http.Header{"Connection": []string{"close"}}, false, -1, false, true, false},
		{"consumed, explicit length", 1, http.Header{}, true, 0, false, true, false},
		{"consumed, chunked", 1, http.Header{}, true, -1, true, true, false},
		{"close-delimited, no header", 1, http.Header{}, true, -1, false, false, true},
		{"both", 1, http.Header{"Connection": []string{"close"}}, true, -1, false, true, false},
		{"absent", 1, http.Header{}, false, -1, false, false, false},
		{"1.0, explicit length", 0, http.Header{}, true, 0, false, false, true},
		{"1.0, chunked", 0, http.Header{}, true, -1, true, false, true},
		{"1.0, close-delimited", 0, http.Header{}, true, -1, false, false, true},
		{"1.0, in the map", 0, http.Header{"Connection": []string{"close"}}, false, -1, false, true, false},
	} {
		resp := &http.Response{
			StatusCode: 200, Proto: "HTTP/1." + strconv.Itoa(c.minor),
			ProtoMajor: 1, ProtoMinor: c.minor,
			Status: "200 OK", Header: c.header, ContentLength: c.length, Close: c.close,
			Body: io.NopCloser(strings.NewReader("")),
		}
		if c.chunked {
			resp.TransferEncoding = []string{"chunked"}
		}
		rec := &recorder{inner: &fakeTransport{resp: resp}}
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
			t.Fatalf("%s: the recorder kept %d exchanges", c.name, len(got))
		}
		if got[0].recvConn != c.received {
			t.Errorf("%s: recvConn=%v, want %v", c.name, got[0].recvConn, c.received)
		}
		if got[0].closeAmbiguous != c.ambiguous {
			t.Errorf("%s: closeAmbiguous=%v, want %v", c.name, got[0].closeAmbiguous, c.ambiguous)
		}
	}
}

// A taxonomy word is this SDK's only where this SDK WROTE it.
//
// ⚠ THIS PRINTER RECEIVES ARBITRARY `net/http` DIAGNOSTICS, NOT ONLY THE SDK'S
// WRAPPER. Marking a taxonomy word wherever it appeared was right for
// `shardpilot experiment assignment fetch failed: <code>` and wrong for
// everything else: with an experiment key of `unauthorized`, `malformed HTTP
// response "unauthorized"` had the word marked as generated, the guard ignored
// it, and `stripMarks` published it verbatim (shardpilot/shardpilot-go#84
// review). Recognition is not authorship, and here the position is what carries
// the authorship.
//
// The rows are the product of the taxonomy words with the two contexts.
func TestTaxonomyIsVouchedOnlyWhereTheSDKWroteIt(t *testing.T) {
	for _, code := range []string{"unauthorized", "kill_switch", "http_503", "transient_408"} {
		for _, c := range []struct {
			name     string
			text     string
			sdkWrote bool
		}{
			{"the SDK's own wrapper", "shardpilot experiment assignment fetch failed: " + code, true},
			{"the remote-config wrapper", "shardpilot remote config fetch failed: " + code, true},
			{"an arbitrary transport diagnostic", `malformed HTTP response "` + code + `"`, false},
			{"prose that merely contains it", "retrying after " + code + " from upstream", false},
		} {
			suppliedValues = []string{code}
			structuralSurfaces = nil
			got := stripMarks(sanitizeCaptured(errors.New(c.text)))
			printed := strings.Contains(got, code)
			// ⚠ THE HALF THAT HOLDS ON BOTH SIDES OF THE SEAM IS THE NEGATIVE ONE.
			// Where the SDK did NOT write the token, it must not be published --
			// that is the finding, and it is true in both branches. Where the SDK
			// DID, this branch publishes it and the branch stacked above withholds
			// the whole diagnostic, because there the text is never taken from the
			// error at all; so the positive half is "published, or refused", which
			// is exactly what each branch does (shardpilot/shardpilot-go#84 review).
			if !c.sdkWrote && printed {
				t.Errorf("%s / %s: a taxonomy word this SDK did not write was published: %q",
					code, c.name, got)
			}
			if c.sdkWrote && !printed && len(structuralSurfaces) == 0 {
				t.Errorf("%s / %s: the SDK's own classification was neither published nor refused: %q",
					code, c.name, got)
			}
			suppliedValues = nil
			structuralSurfaces = nil
		}
	}
}

// The prefixes are the ones the SDK writes, read off its source.
func TestTheSDKErrorPrefixesAreTheOnesTheSDKWrites(t *testing.T) {
	found := map[string]bool{}
	for _, f := range []string{"../../experiments.go", "../../remote_config.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("the SDK source is the oracle and %s could not be read: %v", f, err)
		}
		for _, m := range regexp.MustCompile(`"(shardpilot [a-z ]+failed): %s"`).FindAllStringSubmatch(string(src), -1) {
			found[m[1]+": "] = true
		}
	}
	if len(found) == 0 {
		t.Fatal("no wrapper was read from the SDK source: an oracle that finds nothing is not agreement")
	}
	for p := range found {
		if !slices.Contains(sdkErrorPrefixes, p) {
			t.Errorf("the SDK writes %q before its classification and this build does not vouch there", p)
		}
	}
	for _, p := range sdkErrorPrefixes {
		if !found[p] {
			t.Errorf("%q is vouched as an SDK position and the SDK writes no such prefix", p)
		}
	}
}

// A wrapped run may BEGIN on a whole line and END before prose.
//
// ⚠ TWO PRODUCERS EACH DECLINING ON THE BELIEF THAT THE OTHER COVERS IT. The
// shared-line producer skipped a first line that is entirely base64, deferring to
// `joinBase64Runs`, which joins only lines that are ENTIRELY base64 -- so
// `USF6\r\nJDdwQA== end!` was assembled by neither and the guard approved text a
// decoder turns straight back into the identifier
// (shardpilot/shardpilot-go#84 review).
//
// And the padding: with horizontal whitespace dropped, `JDdwQA== end!` became
// `JDdwQA==end!`, and a scan that treats `=` as one more admissible byte ran PAST
// the padding into the prose, producing a candidate no decoder accepts.
func TestAWrappedRunMayBeginOnAWholeLine(t *testing.T) {
	// Q!z$7p@ is USF6JDdwQA==, wrapped after four characters.
	const head, tail = "USF6", "JDdwQA=="
	suppliedValues = []string{"Q!z$7p@"}
	t.Cleanup(func() { suppliedValues = nil; decodeWork = 0 })
	for _, ending := range []string{" end!", "end!", " ", "", "\r\nmore prose"} {
		for _, lead := range []string{"", "prefix: "} {
			decodeWork = 0
			text := lead + head + "\r\n" + tail + ending
			if err := assertNoLeak(asCaptured(text)); err == nil {
				t.Errorf("%q: the guard approved a run a decoder reconstructs", text)
			}
		}
	}
}

// The section must not claim a request it does not have.
//
// ⚠ `DumpRequestOut` REFUSES REQUESTS AN OPERATOR CAN BUILD. An API key with an
// embedded newline passes the nonempty and config-normalisation checks and makes
// an invalid `Authorization` value; the serialiser's error was discarded, `req`
// stayed empty, and the report rendered an empty canonical block under a heading
// saying the request was formed (shardpilot/shardpilot-go#84 review). The
// artifact exists to carry that evidence, so its absence is the one thing it must
// not be silent about.
func TestASectionDoesNotClaimARequestItDoesNotHave(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	var report strings.Builder
	renderExchanges(&report, []exchange{{
		reqDumpErr: errors.New("net/http: invalid header field value"),
		status:     200, proto: "HTTP/1.1",
	}})
	got := report.String()
	if strings.Contains(got, "the request's canonical form") {
		t.Errorf("the report claimed a canonical request it does not have: %q", got)
	}
	if !strings.Contains(got, "NOT CAPTURED") {
		t.Errorf("the report is silent about the missing request evidence: %q", got)
	}
	if len(structuralSurfaces) == 0 {
		t.Error("a record with no request evidence was left publishable")
	}
}

// The section must not call a decoded payload the received bytes.
//
// ⚠ `http.Transport` ADDS `Accept-Encoding: gzip` ON ITS OWN. A gzip response is
// decompressed before `RoundTrip` returns and the coding headers are removed, so
// `teeBody` wraps the DECODED payload -- while the section's prose called it "the
// received bytes" and the header block is missing a `Content-Encoding` the
// endpoint did send (shardpilot/shardpilot-go#84 review). A capture that
// misdescribes its own provenance is worse than one that captures less.
func TestADecodedBodyIsNotDescribedAsReceived(t *testing.T) {
	for _, uncompressed := range []bool{true, false} {
		var report strings.Builder
		renderExchanges(&report, []exchange{{
			head: []byte("HTTP/1.1 200 OK\r\n\r\n"), status: 200, proto: "HTTP/1.1",
			uncompressed: uncompressed,
		}})
		got := report.String()
		said := strings.Contains(got, "DECODED BY THE TRANSPORT")
		if said != uncompressed {
			t.Errorf("uncompressed=%v: the report says decoded=%v", uncompressed, said)
		}
	}
}

// And the recorder carries the serialiser's refusal off the request it built.
//
// The rows are the shapes `DumpRequestOut` rejects -- an invalid value byte and an
// invalid name byte -- because an operator reaches them through configuration: an
// `SP_API_KEY` with an embedded newline passes the nonempty and normalisation
// checks and lands in `Authorization`.
func TestTheRecorderKeepsTheSerialisersRefusal(t *testing.T) {
	for _, c := range []struct{ name, k, v string }{
		{"newline in a value", "Authorization", "Bearer a\nb"},
		{"carriage return in a value", "Authorization", "Bearer a\rb"},
		{"NUL in a value", "Authorization", "Bearer a\x00b"},
		{"newline in a name", "Auth\norization", "x"},
		{"a request it accepts", "Authorization", "Bearer ok"},
	} {
		rec := &recorder{inner: &fakeTransport{resp: &http.Response{
			StatusCode: 200, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Status: "200 OK", Header: http.Header{}, ContentLength: -1,
			Body: io.NopCloser(strings.NewReader("")),
		}}}
		req, err := http.NewRequest("GET", "https://e.example"+assignmentRoute, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header[c.k] = []string{c.v}
		if _, err := rec.RoundTrip(req); err != nil {
			t.Fatal(err)
		}
		rec.mu.Lock()
		got := rec.exchanges
		rec.mu.Unlock()
		if len(got) != 1 {
			t.Fatalf("%s: the recorder kept %d exchanges", c.name, len(got))
		}
		refused := c.name != "a request it accepts"
		if (got[0].reqDumpErr != nil) != refused {
			t.Errorf("%s: reqDumpErr=%v, want refused=%v -- a discarded serialiser error leaves the section claiming a request it does not have",
				c.name, got[0].reqDumpErr, refused)
		}
		if refused && len(got[0].req) != 0 {
			t.Errorf("%s: a refused serialisation left %d bytes of request evidence", c.name, len(got[0].req))
		}
	}
}

// And the recorder carries both facts off the response it observed.
func TestTheRecorderCarriesSerialiserAndCodingFacts(t *testing.T) {
	for _, uncompressed := range []bool{true, false} {
		rec := &recorder{inner: &fakeTransport{resp: &http.Response{
			StatusCode: 200, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Status: "200 OK", Header: http.Header{}, ContentLength: -1,
			Uncompressed: uncompressed,
			Body:         io.NopCloser(strings.NewReader("")),
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
		if len(got) != 1 || got[0].uncompressed != uncompressed {
			t.Errorf("uncompressed=%v was not carried off the response", uncompressed)
		}
	}
}

// An interim response the transport consumed is still something the endpoint sent.
//
// ⚠ GO DELIVERS 1xx ONLY THROUGH `httptrace`. A bare `RoundTrip` returns the final
// response alone, so a `103 Early Hints` — its status and its headers — vanished
// from a report that went on to present a complete captured pair
// (shardpilot/shardpilot-go#84 review). This harness records rather than
// summarises, and an omission it does not mention is the one kind it cannot make.
//
// The fake transport fires the trace the way the real one does, so the scene
// exercises the installation and not a hand-built field.
func TestAnInterimResponseIsRecordedAndPrinted(t *testing.T) {
	inner := &traceFiringTransport{codes: []int{103, 100}, resp: &http.Response{
		StatusCode: 200, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Status: "200 OK", Header: http.Header{}, ContentLength: -1,
		Body: io.NopCloser(strings.NewReader("")),
	}}
	rec := &recorder{inner: inner}
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
	if len(got[0].infos) != 2 {
		t.Fatalf("the recorder kept %d interim responses, want 2 -- a status the endpoint sent was dropped", len(got[0].infos))
	}
	var report strings.Builder
	renderExchanges(&report, got)
	// ⚠ WITHOUT THE MARKS. The response pipeline marks the reason phrase and the
	// field name as generated, so `103 Early Hints` and `Link` are split by
	// provenance bytes in the raw builder output. A scene that reads the marked
	// text is reading a spelling, not the content.
	out := stripMarks(report.String())
	for _, want := range []string{"Informational", "103 Early Hints", "Link"} {
		if !strings.Contains(out, want) {
			t.Errorf("the report omits %q from an interim response: %q", want, out)
		}
	}
}

// traceFiringTransport calls Got1xxResponse the way net/http does before
// returning the final response.
type traceFiringTransport struct {
	codes []int
	hdr   textproto.MIMEHeader
	resp  *http.Response
}

func (t *traceFiringTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if tr := httptrace.ContextClientTrace(req.Context()); tr != nil && tr.Got1xxResponse != nil {
		for _, c := range t.codes {
			h := t.hdr
			if h == nil {
				h = textproto.MIMEHeader{"Link": []string{"</s.css>; rel=preload"}}
			}
			if err := tr.Got1xxResponse(c, h); err != nil {
				return nil, err
			}
		}
	}
	r := *t.resp
	return &r, nil
}

// Provenance is being written by the SDK, not containing what it writes.
//
// ⚠ `strings.Index` FOUND THE WRAPPER ANYWHERE. An endpoint that puts the wrapper
// text INSIDE its own diagnostic got its own bytes vouched as this SDK's
// classification (shardpilot/shardpilot-go#84 review). `fmt.Errorf("shardpilot …
// failed: %s", code)` puts the wrapper at the START of the message and nowhere
// else, so that is the only position that establishes provenance — finding a
// prefix is not being written by the thing that writes it, one level up from the
// token.
func TestOnlyAnSDKAuthoredErrorVouchesItsClassification(t *testing.T) {
	for _, code := range []string{"unauthorized", "kill_switch", "http_503"} {
		for _, c := range []struct {
			name     string
			text     string
			sdkWrote bool
		}{
			{"the SDK's own error", "shardpilot experiment assignment fetch failed: " + code, true},
			{"remote config", "shardpilot remote config fetch failed: " + code, true},
			{"the wrapper quoted inside a diagnostic",
				`malformed HTTP response "shardpilot experiment assignment fetch failed: ` + code + `"`, false},
			{"the wrapper after other prose",
				"retrying: shardpilot remote config fetch failed: " + code, false},
		} {
			suppliedValues = []string{code}
			structuralSurfaces = nil
			got := stripMarks(sanitizeCaptured(errors.New(c.text)))
			printed := strings.Contains(got, code)
			// ⚠ THE HALF THAT HOLDS ON BOTH SIDES OF THE SEAM IS THE NEGATIVE ONE.
			// Where the SDK did not author the message, the token must not be
			// published -- that is the finding, and it is true in both branches. Where
			// it did, this branch publishes and the branch above withholds the whole
			// diagnostic, because there the text is never taken from the error at all.
			if !c.sdkWrote && printed {
				t.Errorf("%s / %s: a token the SDK did not author was published: %q", code, c.name, got)
			}
			if c.sdkWrote && !printed && len(structuralSurfaces) == 0 {
				t.Errorf("%s / %s: the SDK's own classification was neither published nor refused: %q",
					code, c.name, got)
			}
			suppliedValues = nil
			structuralSurfaces = nil
		}
	}
}

// Every producer over every view, and a seed re-enters the producing half too.
//
// ⚠ TWO OF SIX PRODUCER/VIEW PAIRS WERE MISSING, AND THE CHAIN RERAN ONLY ITS
// DESTRUCTIVE HALF. `binaryCandidates` and `base64SuffixCandidates` were given the
// NORMALISED form and the hex, short-base64 and wrapped producers were not, so a
// one-character value emitted as wrapped unpadded base64 — `a` as `Y\r\nQ` — was
// approved: the fragments are each too short to decode and nothing scanned the
// joined form. And a binary candidate carrying another wrapped run could not be
// reached at all, because the rounds rerun the six decoders and no producer
// (shardpilot/shardpilot-go#84 review).
//
// The rows are the review's two cases plus the shapes around them, and the fix is
// stated as a product rather than as pairs listed by hand — which is how two of
// six went missing.
func TestTheCandidateChainReachesWrappedAndReWrappedValues(t *testing.T) {
	// ⚠ A LIMIT, NAMED WHERE A READER SEES IT. A wrapped run whose continuation is
	// ADJACENT to base64-shaped prose cannot be delimited: in `prefix: Y\r\nQ end`
	// the bytes `Qend` are a legal continuation and a legal word, and separating
	// them means trying every prefix of the run -- the quadratic scan the work
	// budget exists to prevent. That case is a MISS, and a declared limit excuses a
	// miss; it does not excuse a false refusal, so nothing here refuses on it.
	for _, c := range []struct{ sup, text string }{
		{"a", "Y\r\nQ"},
		{"ab", "Y\r\nWI"},
		{"secret99", "/2MyVmoNCmNtVjBPVGs9"},
		{"secret99", "x /2MyVmoNCmNtVjBPVGs9 y"},
	} {
		suppliedValues = []string{c.sup}
		decodeWork = 0
		if err := assertNoLeak(asCaptured(c.text)); err == nil {
			t.Errorf("%q with %q supplied: the guard approved text the chain reconstructs", c.text, c.sup)
		}
		suppliedValues = nil
		decodeWork = 0
	}
}

// An interim response is a response.
//
// ⚠ THE REQUEST REDACTOR ASKS A QUESTION ABOUT THE REQUEST. `redact` consults
// `serialiserWritten` — whether net/http wrote a field for the OUTGOING request —
// so every endpoint field of an interim response was marked generated, the guard
// skipped the span, and `stripMarks` published it verbatim
// (shardpilot/shardpilot-go#84 review). My own capture from the previous round
// introduced this: handing a response to the request's redactor is the same
// category error as reading a trailer with the header path's rules, which this
// file has now made five times.
//
// The rows are the shapes an interim response carries: an endpoint-only field, a
// field the request also has, and a server-minted surface.
func TestAnInterimResponseGoesThroughTheResponsePipeline(t *testing.T) {
	for _, c := range []struct {
		name string
		hdr  textproto.MIMEHeader
	}{
		{"an endpoint-only field", textproto.MIMEHeader{"Link": []string{"</reset/kill_switch>"}}},
		{"a field the request also has", textproto.MIMEHeader{"User-Agent": []string{"kill_switch"}}},
		{"a server-minted surface", textproto.MIMEHeader{"Set-Cookie": []string{"sid=kill_switch"}}},
	} {
		suppliedValues = []string{"kill_switch"}
		structuralSurfaces = nil
		rec := &recorder{inner: &traceFiringTransport{codes: []int{103}, hdr: c.hdr, resp: &http.Response{
			StatusCode: 200, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Status: "200 OK", Header: http.Header{}, ContentLength: -1, Body: http.NoBody,
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
		var b strings.Builder
		renderExchanges(&b, got)
		// ⚠ THE PROPERTY, NOT THIS BRANCH'S ANSWER TO IT. Whether a server-minted
		// surface in an interim response is REFUSED or structurally REDACTED differs
		// across the stack seam -- refusing is this branch's parent's answer and
		// redacting is this one's -- so asserting a refusal holds on one side only.
		// What both owe is that the identifier is not published.
		if out := stripMarks(b.String()); strings.Contains(out, "kill_switch") {
			t.Errorf("%s: a supplied identifier in an interim response was published: %q", c.name, out)
		}
		suppliedValues = nil
		structuralSurfaces = nil
	}
}

// A refusal about the REQUEST does not withdraw the response.
//
// ⚠ `continue` MADE THE SECTION'S OWN SENTENCE FALSE. The NOT CAPTURED block says
// the transport's outcome is reported below, and then skipped the informational
// blocks, the response, and the recorded transport error
// (shardpilot/shardpilot-go#84 review). The missing evidence is the request;
// everything else was recorded and is still owed.
func TestASerialiserRefusalStillRendersTheResponse(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	var report strings.Builder
	renderExchanges(&report, []exchange{{
		reqDumpErr: errors.New("net/http: invalid header field value"),
		transErr:   errors.New("dial tcp: connection refused"),
		proto:      "HTTP/1.1",
	}})
	got := report.String()
	if !strings.Contains(got, "NOT CAPTURED") {
		t.Errorf("the request section is silent about the missing evidence: %q", got)
	}
	if !strings.Contains(got, "## Response") {
		t.Errorf("the section promised the transport outcome below and printed none: %q", got)
	}
}

// A budget that does not count the work it bounds is a number in a message.
//
// ⚠ THE CANDIDATE PRODUCERS SCANNED `cur`, `norm` AND THE NAME STREAM ONCE PER
// DECODER STAGE AND PER ROUND, AND NONE OF IT WAS CHARGED. The `len(cur)` charge
// after the block accounted for one pass and `takeDecodeWork` covered only the
// suffix tails, so post-processing could examine far more than the advertised
// ceiling (shardpilot/shardpilot-go#84 review).
//
// I could not find an input that flips the budget's VERDICT: on every shape I
// built, the other charges dominate and the refusal arrives either way. So this
// asserts the accounting itself, which is the thing that was wrong — the same
// seam `decodeWork` already provides for the suffix probes. The lower bound is
// arithmetic rather than a guess: one round charges `joinBase64Runs` once over
// `cur` and `binaryCandidates` twice, so at least three passes.
func TestTheProducingScansAreCharged(t *testing.T) {
	suppliedValues = []string{"nothingmatches"}
	t.Cleanup(func() { suppliedValues = nil; decodeWork = 0; producerWork = 0 })
	body := strings.Repeat("YWJjZGVm", 500) // 4000 bytes, no supplied value in it
	decodeWork, producerWork = 0, 0
	_ = assertNoLeak(asCaptured(body))
	if producerWork == 0 {
		t.Fatal("the candidate producers were charged nothing, so the ceiling does not bound them")
	}
	if min := 3 * len(body); producerWork < min {
		t.Errorf("the producers were charged %d bytes for a %d-byte record; one round alone scans it at least %d",
			producerWork, len(body), min)
	}
}

// Every producer re-enters, not the two I happened to name.
//
// ⚠ THE ROUND BEFORE FIXED AN ENUMERATION BY WRITING ANOTHER ONE. Seeds were made
// to re-enter the PRODUCING half of the chain, and only the wrapped and
// short-Base64 producers were enqueued — so a seed carrying a form another
// producer handles stayed unreachable: `/zYx` decodes to `0xff61`, which `undoHex`
// deliberately ignores as a two-byte token and only `hexCandidates` splits
// (shardpilot/shardpilot-go#84 review).
//
// The rows are one input per producer that was missing, so the assertion is about
// the SET of producers rather than about the case shown.
func TestEveryProducerReEntersForASeed(t *testing.T) {
	for _, c := range []struct{ sup, text, why string }{
		{"a", "/zYx", "a hex token only hexCandidates splits"},
		{"secret99", "/2MyVmoNCmNtVjBPVGs9", "a wrapped run inside a binary seed"},
		// ⚠ THE VECTOR IS COMPUTED, NOT WRITTEN OUT. My first spelling of this row
		// was `/2JhY2RlZmdo`, which decodes to `0xffbacdefgh` -- one transposed byte,
		// and the row failed as though it had found a gap in the chain rather than in
		// my arithmetic.
		{"abcdefgh", "/2FiY2RlZmdo", "a plain base64 payload behind a binary byte"},
	} {
		suppliedValues = []string{c.sup}
		decodeWork = 0
		if err := assertNoLeak(asCaptured(c.text)); err == nil {
			t.Errorf("%q with %q supplied: the guard approved text the chain reconstructs (%s)",
				c.text, c.sup, c.why)
		}
		suppliedValues = nil
		decodeWork = 0
	}
}

// The interim section is a canonical reconstruction and must say so.
//
// ⚠ `Got1xxResponse` HANDS OVER A CODE AND HEADERS, NOT BYTES. The status line is
// built here from `http.StatusText`, so a custom reason phrase the endpoint sent
// is replaced by the registered one — and on HTTP/2 no textual status line was
// received at all, while the section presented the synthesized bytes as a status
// the endpoint sent (shardpilot/shardpilot-go#84 review). The final-response
// section has carried that label since #73; this one was added without it.
func TestTheInterimSectionIsLabelledCanonical(t *testing.T) {
	var report strings.Builder
	renderExchanges(&report, []exchange{{
		infos:  []string{"HTTP/1.1 103 Early Hints\r\nLink: </s.css>\r\n\r\n"},
		status: 200, proto: "HTTP/2.0",
		head: []byte("HTTP/2.0 200 OK\r\n\r\n"),
	}})
	got := report.String()
	if !strings.Contains(got, "CANONICAL RECONSTRUCTION") {
		t.Errorf("the interim section presents built bytes as received: %q", got)
	}
	if !strings.Contains(got, "http.StatusText") {
		t.Errorf("the section does not say where its status line came from: %q", got)
	}
}

// An interim block's Connection provenance is its own.
//
// ⚠ THE FINAL RESPONSE'S ANSWER SAYS NOTHING ABOUT AN INTERIM ONE.
// `Got1xxResponse` hands over the headers IT delivered, so whether an interim
// carried a `Connection` field is a fact about that block — and inheriting the
// final response's `recvConn=false` marked a received interim line as
// serialiser-generated, which the guard skips and `stripMarks` publishes
// (shardpilot/shardpilot-go#84 review). Per exchange was the fix two rounds ago;
// per interim BLOCK is the same sentence one surface along.
func TestInterimConnectionProvenanceIsPerBlock(t *testing.T) {
	suppliedValues = []string{"secret"}
	t.Cleanup(func() { suppliedValues = nil; structuralSurfaces = nil })
	rec := &recorder{inner: &traceFiringTransport{codes: []int{103},
		hdr: textproto.MIMEHeader{"Connection": []string{base64.StdEncoding.EncodeToString([]byte("secret"))}},
		resp: &http.Response{
			StatusCode: 200, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Status: "200 OK", Header: http.Header{}, ContentLength: -1, Body: http.NoBody,
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
	if len(got) != 1 || len(got[0].interimConn) != 1 || !got[0].interimConn[0] {
		t.Fatalf("the interim block's own Connection provenance was not recorded: %+v", got)
	}
	// ⚠ THE GUARD IS WHAT CATCHES AN ENCODED VALUE, NOT THE SCRUB. `secret` is
	// short, so the scrub matches it only as a whole token and never inside
	// `c2VjcmV0`; what the provenance decides is whether the guard READS the span at
	// all. Marked generated, it is skipped.
	//
	// ⚠ AND THROUGH `renderExchanges`, WHICH IS WHERE THE PROVENANCE IS APPLIED. My
	// first version set `receivedConnection` itself and called `dropFraming`, so it
	// asserted the pipeline and never the CALL SITE -- and the mutant that restores
	// the final response's answer survived it. A test of the composition is not a
	// test of the call site, which this file already records about `responseText`.
	var b strings.Builder
	renderExchanges(&b, got)
	span := b.String()
	if i := strings.Index(span, "## Informational"); i >= 0 {
		if j := strings.Index(span[i:], "## Response"); j > 0 {
			span = span[i : i+j]
		}
	}
	// ⚠ THE PROPERTY, NOT THIS BRANCH'S ANSWER TO IT. Whether a received interim
	// `Connection` value is REFUSED by the guard or structurally REDACTED before it
	// gets there differs across the stack seam. What both owe is that the value does
	// not reach the artifact.
	enc := base64.StdEncoding.EncodeToString([]byte("secret"))
	if strings.Contains(stripMarks(span), enc) && assertNoLeak(span) == nil {
		t.Errorf("a received interim Connection value was published and the guard skipped it: %q",
			stripMarks(span))
	}
}

// The interim capture is bounded, and says so when it drops.
//
// ⚠ INSTALLING `Got1xxResponse` REMOVES THE ONLY BOUND THERE WAS. `net/http`
// resets its response-header allowance after every interim response it delivers,
// leaving the callback responsible for limiting them — so the capture I added in a
// previous round could grow for the whole deadline
// (shardpilot/shardpilot-go#84 review).
func TestTheInterimCaptureIsBounded(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil; suppliedValues = nil })
	codes := make([]int, maxInterimResponses+8)
	for i := range codes {
		codes[i] = 103
	}
	rec := &recorder{inner: &traceFiringTransport{codes: codes, resp: &http.Response{
		StatusCode: 200, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Status: "200 OK", Header: http.Header{}, ContentLength: -1, Body: http.NoBody,
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
	if len(got[0].infos) > maxInterimResponses {
		t.Errorf("the capture retained %d interim responses, past its cap of %d",
			len(got[0].infos), maxInterimResponses)
	}
	if !got[0].infoOverflow {
		t.Error("interim responses were dropped without the overflow being recorded")
	}
	var b strings.Builder
	renderExchanges(&b, got)
	if !strings.Contains(b.String(), "INCOMPLETE") {
		t.Errorf("the section is silent about the blocks it dropped: %q", b.String())
	}
	if len(structuralSurfaces) == 0 {
		t.Error("a record missing interim blocks was left publishable")
	}
}

// When provenance cannot be established, the capture refuses.
//
// ⚠ THE AMBIGUOUS STATE COVERS BOTH ANSWERS. Without explicit framing, `resp.Close`
// means either a CONSUMED `Connection: close` or close-delimited framing, and
// `net/http` leaves the same evidence for each — so calling it received lets the
// scrub rewrite a line net/http invented, and calling it generated lets the guard
// skip a line the endpoint sent (shardpilot/shardpilot-go#84 review). Two rounds
// each picked one default; the third fact is that neither is knowable here.
func TestAnUndecidableConnectionProvenanceRefuses(t *testing.T) {
	for _, c := range []struct {
		name    string
		header  http.Header
		close   bool
		length  int64
		refuses bool
	}{
		{"ambiguous: close, no framing, no header", http.Header{}, true, -1, true},
		{"consumed with a length", http.Header{}, true, 0, false},
		{"in the map", http.Header{"Connection": []string{"close"}}, false, -1, false},
		{"absent", http.Header{}, false, -1, false},
	} {
		structuralSurfaces = nil
		rec := &recorder{inner: &fakeTransport{resp: &http.Response{
			StatusCode: 200, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Status: "200 OK", Header: c.header, ContentLength: c.length, Close: c.close,
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
		var b strings.Builder
		renderExchanges(&b, got)
		if refused := len(structuralSurfaces) > 0; refused != c.refuses {
			t.Errorf("%s: refused=%v, want %v (%v)", c.name, refused, c.refuses, structuralSurfaces)
		}
		structuralSurfaces = nil
	}
}

// A limit tested after the work it bounds is a number in a message.
//
// ⚠ THE PRODUCERS CAN OVERSHOOT THE CAP BEFORE THE WORKLIST EXISTS. A 900 KB body
// of `61 ` repeated yields roughly 300,000 hex seeds and about 234 MB of
// allocations, and the loop checked the cap only after processing every initial
// entry (shardpilot/shardpilot-go#84 review) — the same sentence this file had just
// applied to the decode budget, one collection along.
func TestTheSeedCapIsAppliedWhileCollecting(t *testing.T) {
	suppliedValues = []string{"nothingmatches"}
	t.Cleanup(func() { suppliedValues = nil; decodeWork = 0 })
	decodeWork = 0
	// ⚠ MEASURED IN ALLOCATIONS, WHICH IS WHAT THE FINDING IS ABOUT. Wall time does
	// not separate the two -- 115ms against 202ms -- because the per-seed work is
	// charged either way; what the cap changes is how much is BUILT first. Measured
	// on this machine for a 900 KB body of `61 ` repeated: 56 MiB with the cap
	// applied while collecting, 234 MiB without. The bound is 120 MiB, between them.
	got := allocatedMiB(func() { _ = assertNoLeak(asCaptured(strings.Repeat("61 ", 300000))) })
	if got > 120 {
		t.Errorf("collecting the seeds allocated %d MiB; with the cap applied while "+
			"collecting it is 56 and without it 234", got)
	}
}

// The transport-error decode is charged and bounded.
//
// ⚠ EVERY INTERMEDIATE FORM WAS RETAINED. `%252525…61` removes one layer per pass
// and a full copy of the line was kept for each, all of it BEFORE `assertNoLeak`
// resets and enforces its budget — so a malformed status line of tens of kilobytes
// could spend the process after the network deadline has already ended
// (shardpilot/shardpilot-go#84 review).
func TestTheTransportErrorDecodeIsBounded(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil; suppliedValues = nil })
	tok := "61"
	for i := 0; i < 20000; i++ {
		tok = "25" + tok
	}
	line := "malformed HTTP response %" + tok
	// ⚠ ALLOCATIONS AGAIN, AND THE SAME REASON: wall time is 45ms either way on a
	// small line, and the cost is the retained forms. Measured on this machine for
	// this 40 KB line: 516 MiB charged and bounded, 3394 MiB retaining every form.
	// The bound is 1024 MiB, between them.
	got := allocatedMiB(func() { _ = sanitizeCaptured(errors.New(line)) })
	if got > 1024 {
		t.Errorf("scanning a %d-byte diagnostic allocated %d MiB; bounded it is 516 and "+
			"unbounded 3394", len(line), got)
	}
}

// allocatedMiB reports what f allocated, in MiB. A memory claim is measured with a
// memory instrument; this file has twice used wall time for one and found it does
// not separate the cases.
func allocatedMiB(f func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / (1 << 20)
}

// TestALegalEmptyFieldIsNotRefused: the refusal is about what a value could
// hide, so a field with no value bytes has nothing for it to be about. A legal
// empty `Location:` was refused on the strength of its NAME and the capture cost
// an operator the record and exit 4 (shardpilot/shardpilot-go#84 review) — the
// same shape as the unsupported-coding check, which already asks whether there is
// a body to decode.
func TestALegalEmptyFieldIsNotRefused(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })
	for _, dump := range []string{
		"HTTP/1.1 302 Found\r\nLocation:\r\n\r\n",
		"HTTP/1.1 302 Found\r\nLocation: \t\r\n\r\n",
	} {
		structuralSurfaces = nil
		got := stripMarks(dropFraming(dump))
		if len(structuralSurfaces) != 0 {
			t.Errorf("a field with no value bytes cost the capture: %v (%q)", structuralSurfaces, got)
		}
		if !strings.Contains(got, "Location:") {
			t.Errorf("the empty field was not printed in this program's spelling: %q", got)
		}
	}
	// ⚠ AND A FIELD THAT CARRIES VALUE BYTES IS STILL REFUSED, or the repair has
	// traded a withheld capture for a published redirect target.
	structuralSurfaces = nil
	got := stripMarks(dropFraming("HTTP/1.1 302 Found\r\nLocation: /cb?state=x\r\n\r\n"))
	if len(structuralSurfaces) == 0 {
		t.Fatalf("a Location carrying endpoint-minted bytes was not refused: %q", got)
	}
}

// TestAnEncodedFieldNameIsRefusedOnEveryForm: this scan decodes to a fixed point
// precisely because a name can be spelled to defeat a reader — and then the
// field-name sweep ran once, over the UNDECODED line. `%53et-Cookie:` was decoded,
// handed to `isMintedName` (which answers about minted identifiers, not field
// names) and nothing was recorded, so the diagnostic was published and the
// supported percent decoder reconstructs the cookie from it
// (shardpilot/shardpilot-go#84 review).
func TestAnEncodedFieldNameIsRefusedOnEveryForm(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })
	for _, spelling := range []string{
		`%53et-Cookie: session=secret`,
		`%2553et-Cookie: session=secret`,
		`Set-%43ookie: session=secret`,
		`Set-Cookie: session=secret`,
	} {
		structuralSurfaces = nil
		noteStructuralInText(`malformed HTTP response "` + spelling + `"`)
		if len(structuralSurfaces) == 0 {
			t.Errorf("a server-minted field spelled %q was not refused", spelling)
		}
	}
	// ⚠ AND AN ORDINARY DIAGNOSTIC IS NOT REFUSED, or the sweep has become a
	// refusal of every transport error.
	structuralSurfaces = nil
	noteStructuralInText(`malformed HTTP response "X-Trace: 1"`)
	if len(structuralSurfaces) != 0 {
		t.Fatalf("a diagnostic naming no minted field was refused: %v", structuralSurfaces)
	}
}

// TestALoneCRFoldsBase64LikeLF: `encoding/base64` ignores CR and LF alike, so a
// run folded with a lone CR reconstructs the value — while splitting only on LF
// left the CR inside one line, `allBase64` rejected the run, and the ordinary
// token decoder saw two fragments that reconstruct nothing
// (shardpilot/shardpilot-go#84 review).
func TestALoneCRFoldsBase64LikeLF(t *testing.T) {
	// ⚠ THE BUDGET IS SHARED AND THIS SCENE IS NOT FIRST. Assembly stops charging
	// against `decodeWork`, so a scene that does not reset it passes alone and fails
	// in the suite -- which is what the first draft of this one did, for all three
	// folds including the LF that has always worked.
	decodeWork = 0
	t.Cleanup(func() { decodeWork = 0 })
	for _, fold := range []string{"\n", "\r\n", "\r"} {
		var found bool
		for _, c := range wrappedBase64Candidates("prefix: YWJj" + fold + "ZGVmZ2g=") {
			if strings.Contains(c, "YWJjZGVmZ2g=") {
				found = true
			}
		}
		if !found {
			t.Errorf("a run folded with %q produced no assembled candidate", fold)
		}
	}
}

// TestAShortBinaryBase64DecodeIsRetained: the text answer retained only a
// valid-UTF-8 decode and the binary producer's floor is four bytes, so `/2E` —
// which raw-decodes to 0xff 0x61 — fell between the two producers and a
// one-character supplied value was approved, even though the configured decoder
// reconstructs it directly (shardpilot/shardpilot-go#84 review).
func TestAShortBinaryBase64DecodeIsRetained(t *testing.T) {
	var found bool
	for _, c := range shortBase64Candidates("/2E") {
		if strings.Contains(c, "a") {
			found = true
		}
	}
	if !found {
		t.Fatalf("the binary decode of a short token was discarded: %q", shortBase64Candidates("/2E"))
	}
	// ⚠ AND THE TEXT ANSWER IS UNCHANGED, or one decode answering two questions has
	// taken the first one with it.
	found = false
	for _, c := range shortBase64Candidates("YWI") {
		if c == "ab" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the textual decode of a short token was lost: %q", shortBase64Candidates("YWI"))
	}
}
