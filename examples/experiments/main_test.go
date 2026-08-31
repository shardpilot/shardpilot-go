package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
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
	"syscall"
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
	// ⚠ THE VALUE IS A LENGTH NOW, NOT `value`. A trailer field is not in the
	// verbatim vocabulary, so its value is lengthened exactly as a header's would
	// be -- a trailer is a header that arrived late. What this test protects is
	// that the FIELD is reported beside the message rather than dropped.
	// ⚠ THE NAME IS LENGTHENED TOO NOW. `X-Late` is not in the field-name
	// registry, so it is endpoint-chosen text like any other -- the criterion
	// reaches trailer names as it reaches header names. What this protects is
	// that the field is REPORTED beside the message rather than dropped.
	// Compared with the provenance marks STRIPPED: the delimiter is marked as
	// syntax now, so the raw report reads `X-Late\x01:\x01 value`. The mark is
	// invisible in the published artifact and the property here is unchanged.
	if !strings.Contains(stripMarks(ex.trailerReport()), ": ") {
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
	got := string(redact(dump, http.Header{"Host": nil}, false))
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
	// Compared with the provenance marks STRIPPED: the delimiter is marked as
	// syntax now, so the raw report reads `X-Late\x01:\x01 value`. The mark is
	// invisible in the published artifact and the property here is unchanged.
	if !strings.Contains(stripMarks(ex.trailerReport()), ": ") {
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
		if err := assertNoLeak(asCaptured(string(redact([]byte(escapeMarks(raw)), http.Header{"Host": nil}, false)))); err != nil {
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
	if err := assertNoLeak(asCaptured(string(redact([]byte(escapeMarks(raw)), http.Header{"Host": nil}, false)))); err != nil {
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
		// The body is JSON because the SUBJECT here is the coding, not the body: a
		// body this build cannot describe is a refusal of its own now, and a
		// fixture that carries one would report that refusal as this rule's.
		dropFraming("HTTP/1.1 200 OK\r\nContent-Encoding: " + c.enc + "\r\n\r\n{\"assigned\":false}")
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
	// ⚠ THE PAYLOAD SITS OUTSIDE THE QUOTES ON PURPOSE. The quoted extent of a
	// transport diagnostic is now replaced by its length before the guard sees it,
	// so a value hidden THERE cannot reach the guard -- and cannot reach the
	// artifact either. What this scene is for is the other half: the guard must go
	// on decoding the endpoint text that is NOT quoted, which is the fallback for
	// any diagnostic shape the redaction does not cover
	// (shardpilot/shardpilot-go#85 review).
	tee := &teeBody{err: errors.New("malformed trailer X-Bad " + enc)}
	ex := exchange{head: []byte("x"), captured: tee}
	// ⚠ THE PROPERTY GOT STRONGER, SO THE ASSERTION DID. This used to say the guard
	// must DECODE the endpoint bytes a diagnostic carries outside its quotes -- a
	// fallback for shapes the extent redaction did not cover. The diagnostic is now
	// built from the error VALUE and takes nothing from the message, so those bytes
	// never reach the artifact and there is nothing left to decode. What must hold
	// is that they are absent, in both spellings.
	got := stripMarks(incompleteBodyLine(&ex))
	if strings.Contains(got, enc) || strings.Contains(got, "abcdefgh") {
		t.Fatalf("endpoint bytes carried by a body-read error reached the artifact: %q", got)
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
	if err := assertNoLeak(asCaptured(string(redact([]byte(escapeMarks(raw)), http.Header{"Host": nil}, false)))); err != nil {
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
	// ⚠ THE PROPERTY CHANGED, AND THIS ASSERTION CHANGED WITH IT. A later finding
	// showed that vouching a cookie name because it appears in `requestNames`
	// publishes a supplied identifier: a name we sent in a QUERY is not a name we
	// authored in a `Set-Cookie` the ENDPOINT wrote
	// (shardpilot/shardpilot-go#85 review). Cookie names are always lengthened now.
	// What this scene still holds is the reason it was written: the name must not
	// reach the generic scrub, which would rewrite it into PROSE and produce
	// something no cookie parser accepts. The placeholder is token-safe and
	// generated, so it survives the scrub intact.
	got := stripMarks(scrubSupplied(redactSetCookie("Set-Cookie: experiment_key=x")))
	if strings.Contains(got, "experiment_key") {
		t.Fatalf("a cookie name was vouched from the query-name registry: %q", got)
	}
	// The NAME is what follows `Set-Cookie: `, not the whole prefix -- the first
	// version of this check included the field name and its space and failed on a
	// correct program.
	nm := strings.SplitN(strings.TrimPrefix(got, "Set-Cookie: "), "=", 2)[0]
	if strings.ContainsAny(nm, " <>,") {
		t.Fatalf("the cookie name was rewritten into prose: %q", got)
	}
}

func TestAnUnparsableBodyRefusesAnUnfamiliarMember(t *testing.T) {
	structuralSurfaces = nil
	accountedSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil; accountedSurfaces = nil })
	redactMintedBody(`{"server_secret_identifier":"x`, assignmentTopLevel)
	if len(refusalLedger()) == 0 {
		t.Fatal("an unfamiliar member in a body that does not parse stayed publishable")
	}
	// ⚠ AND A COMPLETE, ORDINARY BODY STILL PUBLISHES. Failing closed on a parse
	// failure is one line away from failing closed on everything.
	structuralSurfaces = nil
	redactMintedBody(`{"assigned":true,"variant_key":"blue"}`, assignmentTopLevel)
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
	if got := redactMintedBody(b.String(), assignmentTopLevel); strings.Contains(got, "sfk1_xxxxxxxxxxxx") {
		t.Fatal("a minted value after many covered spans was published")
	}
}

// TestDepthIsWalkedForwardNotRescanned pins what the cursor must still get right.
// The rescan it replaces was correct and quadratic; a cursor is fast and can be
// wrong in exactly one way — running ahead of the byte being asked about — so the
// scene puts a minted name NESTED after many members (must stay skipped) and one
// at top level after them (must still be caught).
//
// ⚠ MEASURED SCOPE, recorded rather than fixtured around. What this scene kills,
// verified by mutation: a cursor that never advances (depth stuck at 0, nothing
// top-level) and one that reports a constant depth (everything top-level). What it
// does NOT distinguish is an overshoot by a small fixed amount — depth changes only
// at braces, and an overshoot alters an answer only when a brace falls between the
// queried byte and the overshot one, which no member name in a realistic body does.
// Stated as the scene's reach, because a sentence about where checking stops is
// exactly the class the public-surface gate refuses — and it refused this comment's
// first wording, correctly.
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
	if got := redactMintedBody(b.String(), assignmentTopLevel); strings.Contains(got, "withheld") {
		t.Fatalf("a nested member the endpoint merely named that way refused a capture: %q", got)
	}

	var c strings.Builder
	c.WriteString(`{"variant_payload":{`)
	for i := 0; i < 300; i++ {
		c.WriteString(`"n` + strconv.Itoa(i) + `":` + strconv.Itoa(i) + `,`)
	}
	c.WriteString(`"x":1},"subject_fact_key":1}`)
	structuralSurfaces = nil
	if got := redactMintedBody(c.String(), assignmentTopLevel); !strings.Contains(got, "withheld") {
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
	// This half redacts where the guard half refuses, so the scene asks the
	// function that does the work here.
	redactMintedBody(`[{"subject_fact_key":"ordinary payload"}]`, assignmentTopLevel)
	if len(structuralSurfaces) != 0 {
		t.Fatalf("valid non-object JSON was refused as unclassifiable: %q", structuralSurfaces)
	}
	// ⚠ AND A BODY THAT GENUINELY DOES NOT PARSE STILL FAILS CLOSED.
	structuralSurfaces = nil
	redactMintedBody(`{"assigned":true,"subject_fact_key":"x`, assignmentTopLevel)
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
	// ⚠ WITH A BODY. The refusal is about what an undecodable body could HIDE, so a
	// trailer coding on a zero-length response has no subject -- the header path
	// already asks that and this scene had not (shardpilot/shardpilot-go#85 review).
	tee := &teeBody{trailer: map[string][]string{"Content-Encoding": {"gzip"}}}
	tee.buf.WriteString("x")
	e := &exchange{captured: tee}
	e.trailerReport()
	if len(structuralSurfaces) == 0 {
		t.Fatal("a content coding announced in a trailer left the capture publishable")
	}
	// A no-op coding in a trailer is still a no-op.
	structuralSurfaces = nil
	tee2 := &teeBody{trailer: map[string][]string{"Content-Encoding": {"identity"}}}
	tee2.buf.WriteString("x")
	e2 := &exchange{captured: tee2}
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
	if !strings.Contains(got, "Path=redacted-1-chars") {
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
	// ⚠ AND A NAME THE HARNESS REALLY SENT IS STILL VOUCHED FOR **IN ITS OWN
	// REQUEST**. The registry answers for what we sent, so it does not reach a
	// `Location` (shardpilot/shardpilot-go#85 review).
	requestNames = map[string]bool{"control": true}
	got = stripMarks(scrubSupplied(redactQuery("GET /cb?control=x HTTP/1.1")))
	if !strings.Contains(got, "control=") {
		t.Fatalf("a parameter name the harness sent was scrubbed from its own request: %q", got)
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

// ---- round on 819d04c ----

// TestVouchingIsTopLevelOnly: a key inside `variant_payload` is endpoint-controlled
// payload, not SDK wire syntax, so vouching at every depth let a supplied
// identifier of `assigned` ride out inside the nested object — skipped by the
// scrub AND the guard (shardpilot/shardpilot-go#85 review).
func TestVouchingIsTopLevelOnly(t *testing.T) {
	suppliedValues = []string{"assigned"}
	structuralSurfaces = nil
	accountedSurfaces = nil
	t.Cleanup(func() { suppliedValues = nil; structuralSurfaces = nil; accountedSurfaces = nil })
	body := `{"assigned":true,"variant_payload":{"assigned":"x"}}`
	got := stripMarks(scrubSupplied(redactMintedBody(body, assignmentTopLevel)))
	// The top-level one is vouched for and survives; the nested one does not.
	if strings.Count(got, "assigned") != 1 {
		t.Fatalf("vouching did not stop at the top level: %q", got)
	}
	if !strings.Contains(got, `"assigned":true`) {
		t.Fatalf("the top-level member the SDK binds was scrubbed: %q", got)
	}
}

// TestValidNonObjectJSONIsNotWithheld is the second site of one conflation: a
// complete `[{"subject_fact_key":1}]` was labelled unparsable, its nested member
// treated as possibly top-level, and the body withheld with exit 4 — while its
// depth is fully determined (shardpilot/shardpilot-go#85 review).
func TestValidNonObjectJSONIsNotWithheld(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	got := redactMintedBody(`[{"subject_fact_key":1}]`, assignmentTopLevel)
	if strings.Contains(got, "withheld") {
		t.Fatalf("a complete valid body was withheld: %q", stripMarks(got))
	}
	if len(refusalLedger()) != 0 {
		t.Fatalf("a complete valid body was made unpublishable: %q", refusalLedger())
	}
	// A body that genuinely does not parse is still withheld.
	structuralSurfaces = nil
	if got := redactMintedBody(`{"subject_fact_key":`, assignmentTopLevel); !strings.Contains(got, "withheld") &&
		len(refusalLedger()) == 0 {
		t.Fatalf("a body that does not parse stayed publishable: %q", stripMarks(got))
	}
}

// TestAZoneIsMeasuredDecoded: `eth%30` is `eth0`, four characters, and measuring
// the wire spelling put two lengths for one value in one capture
// (shardpilot/shardpilot-go#85 review).
func TestAZoneIsMeasuredDecoded(t *testing.T) {
	suppliedValues = nil
	got := stripMarks(redactTarget("Location: https://[fe80::1%25eth%30]/cb"))
	if !strings.Contains(got, "redacted-4-chars") {
		t.Fatalf("a zone was measured in its wire spelling: %q", got)
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

// TestAQueryNameIsComparedExactly: `%20experiment_key%20` decodes to
// ` experiment_key `, which the HTTP-whitespace trim turned into the harness-owned
// name — so the entire endpoint spelling was marked generated and both the scrub
// and the guard skipped it (shardpilot/shardpilot-go#85 review).
func TestAQueryNameIsComparedExactly(t *testing.T) {
	suppliedValues = []string{"experiment_key"}
	requestNames = map[string]bool{"experiment_key": true}
	t.Cleanup(func() { suppliedValues = nil; requestNames = map[string]bool{} })
	got := stripMarks(scrubSupplied(redactTarget("Location: /cb?%20experiment_key%20=x")))
	if strings.Contains(got, "experiment_key") {
		t.Fatalf("a padded endpoint spelling was vouched for and published: %q", got)
	}
	// ⚠ AND THE EXACT NAME IS STILL VOUCHED FOR **IN OUR OWN REQUEST**, or the
	// repair scrubs the SDK's wire contract. The registry records the names the
	// harness SENT, so it answers for the request line and not for a `Location` the
	// endpoint chose -- this half used to be asserted on the endpoint's URL, which
	// is the defect the sibling thread names (shardpilot/shardpilot-go#85 review).
	got = stripMarks(scrubSupplied(redactQuery("GET /cb?experiment_key=x HTTP/1.1")))
	if !strings.Contains(got, "experiment_key") {
		t.Fatalf("a name the harness sent was scrubbed from its own request: %q", got)
	}
	// And the same name in the ENDPOINT's target is not ours.
	got = stripMarks(scrubSupplied(redactTarget("Location: /cb?experiment_key=x")))
	if strings.Contains(got, "?experiment_key=") {
		t.Fatalf("an endpoint's query name was vouched as ours: %q", got)
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
	t.Cleanup(func() { suppliedValues = nil; configuredHost, configuredHostWire = "", "" })
	// ⚠ ASSERTED ON THE SCRUBBED OUTPUT. My first version asserted on `redact`'s
	// own result, which does not scrub — so it passed with and without the fix, and
	// the mutant survived. And the symptom is not the one the finding names:
	// measured, the guard does NOT refuse, because `scrubSupplied` has already
	// replaced the value. What is published is `Host: <redacted, 3 chars>.
	// shardpilot.com` — an authority no parser accepts, approved because the
	// placeholder is generated. Same class, different consequence.
	// ⚠ THIS HALF'S `redact` TAKES THE OWNED-HEADER SET. The inherited scene called
	// the parent's one-argument form; the fact under test is the same.
	req, _ := http.NewRequest("GET", "https://app.shardpilot.com/x", nil)
	got := stripMarks(scrubSupplied(string(redact(
		[]byte("GET /x HTTP/1.1\r\nHost: app.shardpilot.com\r\n\r\n"), requestOwnedHeaders(req), false))))
	if !strings.Contains(got, "Host: app.shardpilot.com") {
		t.Fatalf("the configured authority was rewritten: %q", got)
	}
	// ⚠ AND THE SERIALISED SPELLING OF THE SAME AUTHORITY. `DumpRequestOut` writes
	// an internationalised name as PUNYCODE, so the pre-serialisation spelling never
	// matched and this program's own authority was rewritten into
	// `xn--9ca.<redacted, 7 chars>` -- an authority no parser accepts, approved
	// because the placeholder is generated (shardpilot/shardpilot-go#84 review).
	suppliedValues = []string{"example"}
	configuredHost, configuredHostWire = "é.example", "xn--9ca.example"
	got = stripMarks(scrubSupplied(string(redact(
		[]byte("GET /x HTTP/1.1\r\nHost: xn--9ca.example\r\n\r\n"), requestOwnedHeaders(req), false))))
	if !strings.Contains(got, "Host: xn--9ca.example") {
		t.Fatalf("the serialised form of the configured authority was rewritten: %q", got)
	}
	configuredHost, configuredHostWire = "app.shardpilot.com", ""
	// ⚠ AND A DIFFERENT HOST IS STILL ENDPOINT TEXT, or the exemption covers
	// whatever stands in that position.
	suppliedValues = []string{"elsewhere"}
	req2, _ := http.NewRequest("GET", "https://app.shardpilot.com/x", nil)
	got = stripMarks(scrubSupplied(string(redact(
		[]byte("GET /x HTTP/1.1\r\nHost: elsewhere.invalid\r\n\r\n"), requestOwnedHeaders(req2), false))))
	if strings.Contains(got, "Host: elsewhere.invalid") {
		t.Fatalf("an unconfigured authority was vouched for: %q", got)
	}
}

// TestATransportErrorGoesThroughTheStructuralQuestion: Go rejects a malformed
// response before returning one and puts the complete bad header into the error, so
// a server-set cookie reached the report through the error diagnostic where only
// the supplied-value scrub ran (shardpilot/shardpilot-go#84 review).
func TestATransportErrorGoesThroughTheStructuralQuestion(t *testing.T) {
	structuralSurfaces, accountedSurfaces = nil, nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces, accountedSurfaces = nil, nil })
	// ⚠ A TYPE THIS BUILD CANNOT DESCRIBE IS A REFUSAL, AND THAT IS THE POINT. The
	// scene used to hand `errors.New(...)` to both halves, which worked while the
	// message was published and redacted. Now the message is never read, so a bare
	// `*errors.errorString` carries nothing this program can account for and the
	// capture is withheld by NAME.
	_ = sanitizeCaptured(errors.New("malformed HTTP response \"Set-Cookie: session=abcdefghijkl\""))
	if len(structuralSurfaces) == 0 {
		t.Fatal("an undescribable transport error left the capture publishable")
	}
	// ⚠ AND A REAL TRANSPORT FAILURE STILL PUBLISHES, or every transport failure
	// becomes unreportable -- which is the case this artifact most needs to report.
	// Built from the typed values Go actually produces, measured: `Get` on a closed
	// port yields exactly this chain.
	structuralSurfaces, accountedSurfaces = nil, nil
	out := stripMarks(sanitizeCaptured(&url.Error{
		Op:  "Get",
		URL: "https://e.example/x",
		Err: &net.OpError{Op: "dial", Net: "tcp",
			Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}},
	}))
	if len(structuralSurfaces) != 0 {
		t.Fatalf("an ordinary transport failure was made unpublishable: %q", structuralSurfaces)
	}
	for _, want := range []string{"op=Get", "op=dial", "net=tcp", "syscall=connect"} {
		if !strings.Contains(out, want) {
			t.Errorf("the diagnostic lost %q, so it is less useful than the text it replaced: %q", want, out)
		}
	}
}

// ---- round on 0d5d3c4 ----

// TestALocationLineKeepsItsColon: `splitField` returns the colon in NEITHER piece,
// so the finishing pass dropped it from every Location line it touched —
// `Location /cb`, which is not an HTTP header at all
// (shardpilot/shardpilot-go#85 review). My scenes called `redactTarget` directly
// and never went through the header path, so nothing saw it.
func TestALocationLineKeepsItsColon(t *testing.T) {
	suppliedValues = nil
	receivedConnection = true
	t.Cleanup(func() { receivedConnection = false })
	got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\nLocation: /cb?x=y\r\n\r\n"))
	if !strings.Contains(got, "Location: ") {
		t.Fatalf("a Location line lost its colon: %q", got)
	}
}

// TestAuthorityBodyIsNotSyntax: `[v1.=]` is a valid IPvFuture authority whose `=`
// is DATA, and the finishing pass marked it — so a supplied `=` rode out past both
// the scrub and the guard (shardpilot/shardpilot-go#85 review). "Everything left is
// structure by construction" was my argument for that pass, and it is false of the
// one component whose body has its own grammar.
func TestAuthorityBodyIsNotSyntax(t *testing.T) {
	suppliedValues = []string{"="}
	receivedConnection = true
	t.Cleanup(func() { suppliedValues = nil; receivedConnection = false })
	// ⚠ MEASURED UNREACHABLE, AND SAID SO RATHER THAN ASSERTED AROUND. `url.Parse`
	// refuses `https://[v1.=]/cb` on this Go version, so the target is withheld
	// before the finishing pass runs — which is why this line asserts the WITHHOLDING
	// and not the marking, and why a mutant removing the in-authority guard survives
	// the suite. The guard stays as a statement of the grammar; the scene says what
	// it can see.
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nLocation: https://[v1.=]/cb\r\n\r\n")))
	if strings.Contains(got, "[v1.=]") {
		t.Fatalf("a malformed authority reached the capture: %q", got)
	}
	// ⚠ AND A REAL SEPARATOR OUTSIDE ONE IS STILL SYNTAX.
	got = stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nLocation: /a?x=1\r\n\r\n")))
	if !strings.Contains(got, "?") || !strings.Contains(got, "=") {
		t.Fatalf("a query separator outside an authority was scrubbed: %q", got)
	}
}

// TestALocationTrailerGetsTheFinishingPasses: a `Location` arriving as a trailer
// was rendered `<redacted, 5 chars>://…` for a supplied `https`, because the
// trailer path never applied the passes the response-header path does
// (shardpilot/shardpilot-go#85 review).
func TestALocationTrailerGetsTheFinishingPasses(t *testing.T) {
	suppliedValues = []string{"https"}
	t.Cleanup(func() { suppliedValues = nil })
	e := &exchange{captured: &teeBody{trailer: map[string][]string{
		"Location": {"https://e.example/cb"},
	}}}
	got := stripMarks(e.trailerReport())
	if !strings.Contains(got, "https://") {
		t.Fatalf("an admitted scheme in a trailer was scrubbed: %q", got)
	}
}

// TestTheRequestQuerySeparatorIsSyntax: the request dump is NOT passed through
// `scrubSupplied` afterwards, so an unmarked `&` was reported by the guard as a
// surviving supplied value and every such run exited 4
// (shardpilot/shardpilot-go#85 review). The sweep probes the response Location and
// could not see this: one function, two callers, and only one of them post-scrubs.
func TestTheRequestQuerySeparatorIsSyntax(t *testing.T) {
	suppliedValues = []string{"&"}
	requestNames = map[string]bool{"a": true, "b": true}
	t.Cleanup(func() { suppliedValues = nil; requestNames = map[string]bool{} })
	req, _ := http.NewRequest("GET", "https://e.example/x?a=1&b=2", nil)
	got := redact([]byte("GET /x?a=1&b=2 HTTP/1.1\r\nHost: e.example\r\n\r\n"),
		requestOwnedHeaders(req), false)
	if err := assertNoLeak(asCaptured(string(got))); err != nil {
		t.Fatalf("the request query separator was reported as a leak: %v", err)
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

// ---- round on d2cd70d ----

// TestACookieNameIsComparedExactly is the SECOND site of one conflation: I fixed
// the query-name lookup and left this one, so `Set-Cookie: experiment_key =x` had
// the trailing space trimmed for the provenance match and the whole endpoint
// spelling was marked generated (shardpilot/shardpilot-go#85 review).
func TestACookieNameIsComparedExactly(t *testing.T) {
	suppliedValues = []string{"experiment_key"}
	requestNames = map[string]bool{"experiment_key": true}
	t.Cleanup(func() { suppliedValues = nil; requestNames = map[string]bool{} })
	got := stripMarks(scrubSupplied(redactSetCookie("Set-Cookie: experiment_key =x")))
	if strings.Contains(got, "experiment_key") {
		t.Fatalf("a padded endpoint spelling was vouched for and published: %q", got)
	}
	// The exact name is lengthened too, and for the same reason: see
	// TestAVouchedCookieNameIsMarked. What this scene is about is that the PADDED
	// spelling is not treated as the harness's -- and that holds whether or not the
	// exact one is vouched.
	got = stripMarks(scrubSupplied(redactSetCookie("Set-Cookie: experiment_key=x")))
	if strings.Contains(got, "experiment_key") {
		t.Fatalf("a cookie name was vouched from the query-name registry: %q", got)
	}
}

// TestAnOpaqueTargetHasItsSchemeVouched: with no `://` the fallback took the FIRST
// colon in the line — the header name's — so `Location: https:abc` never had its
// scheme vouched (shardpilot/shardpilot-go#85 review).
func TestAnOpaqueTargetHasItsSchemeVouched(t *testing.T) {
	suppliedValues = []string{"https"}
	receivedConnection = true
	t.Cleanup(func() { suppliedValues = nil; receivedConnection = false })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nLocation: https:abc\r\n\r\n")))
	if !strings.Contains(got, "https:") {
		t.Fatalf("an approved scheme on an opaque target was scrubbed: %q", got)
	}
}

// TestVouchingParsesAMarkFreeView: the minted replacement inserts provenance bytes
// INSIDE a JSON string, so an ordinary body carrying a minted value stopped parsing
// and returned no names — and a supplied `assigned` took the recognised SDK field
// with it (shardpilot/shardpilot-go#85 review).
func TestVouchingParsesAMarkFreeView(t *testing.T) {
	suppliedValues = []string{"assigned"}
	structuralSurfaces = nil
	accountedSurfaces = nil
	t.Cleanup(func() { suppliedValues = nil; structuralSurfaces = nil; accountedSurfaces = nil })
	got := stripMarks(scrubSupplied(redactMintedBody(
		`{"assigned":true,"subject_fact_key":"sfk1_xxxxxxxxxxxx"}`, assignmentTopLevel)))
	if !strings.Contains(got, `"assigned":true`) {
		t.Fatalf("a recognised SDK field was scrubbed out of an ordinary body: %q", got)
	}
}

// ---- round on a212a1c ----

// TestVouchingRequiresTheRecognisedSpelling: the predicates normalise case, so
// `Content-Type: application/JSON` is admitted — and marking the RAW span vouched
// for a spelling the registry never saw (shardpilot/shardpilot-go#85 review).
func TestVouchingRequiresTheRecognisedSpelling(t *testing.T) {
	suppliedValues = []string{"JSON"}
	receivedConnection = true
	t.Cleanup(func() { suppliedValues = nil; receivedConnection = false })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nContent-Type: application/JSON\r\n\r\n")))
	if strings.Contains(got, "application/JSON") {
		t.Fatalf("a non-canonical spelling was vouched for: %q", got)
	}
	// And the canonical one still prints.
	suppliedValues = []string{"json"}
	got = stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n")))
	if !strings.Contains(got, "application/json") {
		t.Fatalf("the canonical spelling was scrubbed: %q", got)
	}
}

// TestAnEscapedMemberNameIsNotVouched: recognition here is SEMANTIC —
// `{"assigned":false}` decodes to a name this program knows — and marking the raw
// span vouched for the endpoint's escape (shardpilot/shardpilot-go#85 review).
func TestAnEscapedMemberNameIsNotVouched(t *testing.T) {
	suppliedValues = []string{"bound"}
	t.Cleanup(func() { suppliedValues = nil })
	// ⚠ OBSERVED THROUGH THE GUARD, AND ON A VALUE THE GUARD CAN SEE. Two earlier
	// versions of this scene could not tell the fix from its absence: the first
	// asserted the scrub removed the supplied value, the second asserted the guard
	// refused -- but both supplied a value sitting MID-WORD inside the escape, and
	// both the scrub and the guard require a word boundary, so neither would have
	// acted whether or not the span was vouched. `bound` here is bounded by `"` and
	// `\`, so the only thing standing between it and a refusal is the vouching.
	body := redactMintedBody(`{"bound\u0061ry":1}`, assignmentTopLevel)
	if err := assertNoLeak(asCaptured(body)); err == nil {
		t.Fatalf("an endpoint escape spelling was vouched for, so the guard passed it: %q", body)
	}
	// AND THE RECOGNISED SPELLING IS STILL VOUCHED: the rule must forbid the
	// arrived spelling without forbidding the one this program writes.
	if plain := redactMintedBody(`{"boundary":1}`, assignmentTopLevel); !strings.Contains(plain, genMark) {
		t.Fatalf("the recognised spelling lost its vouching: %q", plain)
	}
}

// TestAnEmptyPortOnABracketedAuthorityIsAccepted: `https://[::1]:/cb` leaves the
// trailing colon in `host`, so the bracket test refused an authority Go accepts —
// while the registered-name form with an empty port is admitted
// (shardpilot/shardpilot-go#85 review).
func TestAnEmptyPortOnABracketedAuthorityIsAccepted(t *testing.T) {
	if !parsesAsURI("https://[::1]:/cb") {
		t.Fatal("a bracketed authority with an empty port was refused")
	}
	if !parsesAsURI("https://e.example:/cb") {
		t.Fatal("the registered-name form stopped being accepted")
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
	// ⚠ THE CALL SITE MOVED ACROSS THE STACK SEAM. The guard half's `noteMinted`
	// is gone in this branch; the same scan lives in `redactMintedBody`, and the
	// brace prerequisite had been carried over with it.
	structuralSurfaces, accountedSurfaces = nil, nil
	t.Cleanup(func() { structuralSurfaces, accountedSurfaces = nil, nil })
	got := redactMintedBody(`"subject_fact_key":"sfk1_server_secret"`, assignmentTopLevel)
	if len(structuralSurfaces)+len(accountedSurfaces) == 0 {
		t.Fatal("a minted field in a brace-less malformed body passed unrecognised")
	}
	if strings.Contains(got, "sfk1_server_secret") {
		t.Fatalf("the minted value was printed: %q", got)
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

// The schema's member names are grammar; a supplied key equal to one must not
// rewrite the schema -- and a NON-canonical spelling must not be vouched.
func TestASchemaMemberNameIsGrammar(t *testing.T) {
	suppliedValues = []string{"assigned"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n{\"assigned\":true,\"version\":1,\"assignment_key\":\"a\",\"variant_key\":\"v\",\"boundary\":{\"assignment_unit\":\"client_id\"}}")))
	if !strings.Contains(got, `"assigned"`) {
		t.Fatalf("the response schema was rewritten by the scrub: %q", got)
	}
	suppliedValues = []string{"ASSIGNED"}
	got = stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n{\"ASSIGNED\":true}")))
	if strings.Contains(got, "ASSIGNED") {
		t.Fatalf("a non-canonical spelling was vouched as grammar: %q", got)
	}
}

// An extent that does not close cannot be measured, and the clause says that
// case is a refusal rather than a capture.
func TestAnUnterminatedQuotedExtentIsRefused(t *testing.T) {
	structuralSurfaces, accountedSurfaces = nil, nil
	t.Cleanup(func() { structuralSurfaces, accountedSurfaces = nil, nil })
	sanitizeCaptured(errors.New(`malformed HTTP response "server-secret-token`))
	if len(structuralSurfaces) == 0 {
		t.Fatal("a diagnostic whose quoted extent does not close was captured anyway")
	}
}

// The clause covers the BODY too: a shape this build does not describe is a
// refusal, not a publication.
func TestAnUndescribedBodyIsRefused(t *testing.T) {
	structuralSurfaces, accountedSurfaces = nil, nil
	t.Cleanup(func() { structuralSurfaces, accountedSurfaces = nil, nil })
	got := dropFraming("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nserver-secret-token")
	if len(structuralSurfaces) == 0 {
		t.Fatalf("an endpoint body in an undescribed shape was published: %q", got)
	}
	// AND AN ORDINARY VERDICT IS STILL PUBLISHABLE, which is what the refusal must
	// not cost.
	structuralSurfaces, accountedSurfaces = nil, nil
	if got := dropFraming("HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false}"); len(structuralSurfaces) != 0 {
		t.Fatalf("an ordinary fact response became unpublishable: %q / %v", got, structuralSurfaces)
	}
}

// The identity branch is an early return, and an early return is a promise to
// have done everything the common path does.
func TestTheIdentityBranchVouchesOnlyTheCanonicalSpelling(t *testing.T) {
	suppliedValues = []string{"IDENTITY"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nContent-Encoding: IDENTITY\r\n\r\n{\"assigned\":false}")))
	if strings.Contains(got, "IDENTITY") {
		t.Fatalf("a non-canonical coding spelling was vouched: %q", got)
	}
	// AND THE RESPONSE IS STILL A RESPONSE: the first version of this fix left the
	// line loop instead of the branch and truncated everything after the status.
	if !strings.Contains(got, `{"assigned":false}`) {
		t.Fatalf("the rest of the response was dropped: %q", got)
	}
}

func TestTheIdentityBranchAdmitsItsFieldName(t *testing.T) {
	suppliedValues = []string{"Content-Encoding"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nContent-Encoding: identity\r\n\r\n{\"assigned\":false}")))
	if !strings.Contains(got, "Content-Encoding: identity") {
		t.Fatalf("the early return skipped the generic name path: %q", got)
	}
}

// A cookie flag is vouched only in its canonical spelling, and only if it is
// actually a flag.
func TestOnlyCanonicalValuelessCookieFlagsAreVouched(t *testing.T) {
	for _, c := range []struct{ supplied, line string }{
		{"SECURE", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; SECURE\r\n\r\n{\"assigned\":false}"},
		{"Path", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; Path\r\n\r\n{\"assigned\":false}"},
	} {
		suppliedValues = []string{c.supplied}
		got := stripMarks(scrubSupplied(dropFraming(c.line)))
		suppliedValues = nil
		if strings.Contains(got, c.supplied) {
			t.Fatalf("%q was vouched as a cookie flag: %q", c.supplied, got)
		}
	}
	// AND A REAL FLAG IN ITS OWN SPELLING STILL SURVIVES.
	suppliedValues = []string{"Secure"}
	t.Cleanup(func() { suppliedValues = nil })
	if got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; Secure\r\n\r\n{\"assigned\":false}"))); !strings.Contains(got, "Secure") {
		t.Fatalf("the canonical flag lost its vouching: %q", got)
	}
}

// The late trailer renderer takes the same delimiter marking as a header line.
func TestTheTrailerRendererMarksItsDelimiter(t *testing.T) {
	suppliedValues = []string{":"}
	t.Cleanup(func() { suppliedValues = nil })
	tee := &teeBody{trailer: http.Header{"Date": []string{"Sun, 06 Nov 1994 08:49:37 GMT"}}}
	got := stripMarks(scrubSupplied((&exchange{head: []byte("x"), captured: tee}).trailerReport()))
	if !strings.Contains(got, "Date: ") {
		t.Fatalf("the trailer delimiter was rewritten into prose: %q", got)
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

	// ⚠ THE LIST IS DECLARED ONCE NOW, so this scene reads the declaration rather
	// than each `range` over an inline literal. It kept refusing when the literals
	// were replaced by a name -- which is the behaviour a derivation should have, and
	// is how this scene reported the refactor instead of passing through it.
	stage := regexp.MustCompile(`supportedDecoders = \[\]func\(string\) string\{([^}]*)\}`)
	ms := stage.FindAllStringSubmatch(text, -1)
	if len(ms) == 0 {
		t.Fatal("the decoding chain's stage list was not found, so this scene measures nothing")
	}
	// ⚠ AND NO SITE MAY STILL CARRY ITS OWN COPY, or the single list is one list
	// beside several others.
	if regexp.MustCompile(`range \[\]func\(string\) string\{`).MatchString(text) {
		t.Errorf("a decoder chain is still written inline somewhere; the declaration is not the only one")
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

// Parsing is not accounting: an admitted member NAME says nothing about its
// endpoint-chosen VALUE.
func TestAnUnaccountedValueInAParsedBodyIsRedacted(t *testing.T) {
	structuralSurfaces, accountedSurfaces = nil, nil
	t.Cleanup(func() { structuralSurfaces, accountedSurfaces = nil, nil })
	got := stripMarks(dropFraming("HTTP/1.1 401 Unauthorized\r\n\r\n{\"error\":\"server-secret-token\"}"))
	if strings.Contains(got, "server-secret-token") {
		t.Fatalf("an endpoint-minted value in a parsed body was published: %q", got)
	}
	if !strings.Contains(got, `"error"`) {
		t.Fatalf("the member name was redacted along with its value: %q", got)
	}
	if len(accountedSurfaces) == 0 {
		t.Fatalf("the redaction was not accounted for: %q", got)
	}
	// AND A VALUE THIS SDK ITSELF PRODUCES SURVIVES, which is what the verdict
	// block reads.
	structuralSurfaces, accountedSurfaces = nil, nil
	// ⚠ A VALUE THE SDK ACCEPTS **AT `reason`**, which is narrower than the taxonomy:
	// the not-assigned branch takes only `{absent, kill_switch,
	// targeting_unmatched}` (shardpilot/shardpilot-go#85 review). This scene used
	// `not_found`, which the SDK writes as a Code and refuses at this member.
	if v := stripMarks(dropFraming("HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false,\"reason\":\"targeting_unmatched\"}")); !strings.Contains(v, "targeting_unmatched") {
		t.Fatalf("the SDK's own taxonomy was lengthened: %q", v)
	}
}

// A registry's SCOPE is part of what it says: `benignTopLevel` describes the
// top-level schema, and a nested member of the same name is the endpoint's.
func TestASchemaNameIsGrammarOnlyAtTheRoot(t *testing.T) {
	suppliedValues = []string{"assigned"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n{\"assigned\":true,\"version\":1,\"assignment_key\":\"a\",\"variant_key\":\"v\",\"boundary\":{\"assignment_unit\":\"client_id\"},\"variant_payload\":{\"assigned\":\"x\"}}")))
	if strings.Count(got, "assigned") != 1 {
		t.Fatalf("the nested endpoint-controlled name was exempted too: %q", got)
	}
}

// A container VALUE consumes its parent's turn; the member after it is a key.
func TestTheMemberAfterAContainerValueIsStillAKey(t *testing.T) {
	suppliedValues = []string{"version"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		// ⚠ THE BODY CARRIES `assigned`, because a body without it is not a verdict at
		// all and takes no schema exemptions -- this scene is about KEY DETECTION after a
		// container value, not about eligibility (shardpilot/shardpilot-go#84 review).
		"HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false,\"variant_payload\":{},\"version\":1}")))
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
	// ⚠ THIS HALF IS GONE ON THIS BRANCH, AND THE REASON IS A REAL DIFFERENCE, not a
	// relaxation. On the parent the name goes through `scrubHeaderName`, which knows
	// only what the harness supplied, so a supplied `Content-Encoding` is redacted
	// and the assertion held. Here it goes through `admitFieldName`, which VOUCHES a
	// registered name in its canonical spelling -- and this branch's own sweep
	// requires exactly that of every registered name. Keeping the parent's half
	// would assert the opposite of `TestEveryVouchedTokenSurvivesTheScrub`, and one
	// of the two would have to be wrong. The property that survives the seam is the
	// one above: whatever stands there is still a legal field name.
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

// A pattern over member-colon-string is not a traversal.
func TestValuesNestedInArraysAreRedacted(t *testing.T) {
	structuralSurfaces, accountedSurfaces = nil, nil
	t.Cleanup(func() { structuralSurfaces, accountedSurfaces = nil, nil })
	for _, body := range []string{
		`{"variant_payload":["server-secret-token"]}`,
		`{"variant_payload":{"note":"server-secret-token"}}`,
		`{"variant_payload":[{"deep":["server-secret-token"]}]}`,
	} {
		got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + body))
		if strings.Contains(got, "server-secret-token") {
			t.Fatalf("an endpoint value survived at depth: %q", got)
		}
	}
}

// A value is not evidence of its author; the POSITION is.
func TestTaxonomyIsVouchedOnlyInAVerdictField(t *testing.T) {
	suppliedValues = []string{"kill_switch"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n{\"variant_payload\":{\"note\":\"kill_switch\"}}")))
	if strings.Contains(got, "kill_switch") {
		t.Fatalf("a taxonomy-shaped payload value was vouched as this SDK's: %q", got)
	}
	// AND IN ITS OWN FIELD IT STILL IS: the verdict block reads that value.
	suppliedValues = nil
	if v := stripMarks(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false,\"reason\":\"kill_switch\"}")); !strings.Contains(v, "kill_switch") {
		t.Fatalf("the SDK's own classification was lengthened in its own field: %q", v)
	}
}

// Integer syntax constrains the alphabet, not the author.
func TestAnAdmittedNumericValueIsNotVouchedWhenSupplied(t *testing.T) {
	suppliedValues = nil
	if got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\nAge: 42\r\n\r\n{\"assigned\":false}")); !strings.Contains(got, "Age: 42") {
		t.Fatalf("an admitted numeric field stopped being published: %q", got)
	}
	suppliedValues = []string{"123456"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nAge: 123456\r\n\r\n{\"assigned\":false}")))
	if strings.Contains(got, "123456") {
		t.Fatalf("a supplied identifier was vouched because it looked like a number: %q", got)
	}
	if !strings.Contains(got, "Age: ") {
		t.Fatalf("the field framing was lost along with the value: %q", got)
	}
}

// A number is grammar like the literals beside it.
func TestJSONNumbersAreGrammar(t *testing.T) {
	// ⚠ THE SUPPLIED VALUE IS NOT THE NUMBER IN THE BODY. The first version supplied
	// `1` against `{"version":1}` -- which is the COLLISION a later finding names as
	// a leak, so this scene was asserting the wrong half of the rule
	// (shardpilot/shardpilot-go#85 review). What this one holds is that an ordinary
	// number is grammar and survives a scrub aimed at something else; the collision
	// is TestACollidingJSONNumberIsRefused.
	suppliedValues = []string{"zzz"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n{\"version\":1}")))
	if !strings.Contains(got, `"version":1`) {
		t.Fatalf("the grammar's own number was rewritten: %q", got)
	}
}

// The escape spelling must be the one this program would have sent.
func TestOnlyOurOwnQueryEscapingIsVouched(t *testing.T) {
	// ⚠ THE REGISTRY HAS TO CONTAIN THE NAME, or the branch under test never runs
	// and the scene passes on a program that does not have the fix. The first
	// version omitted this and its mutant survived.
	noteRequestName("experiment_key")
	suppliedValues = []string{"5F"}
	t.Cleanup(func() {
		suppliedValues = nil
		requestNames = map[string]bool{}
	})
	// ⚠ ON OUR OWN REQUEST LINE. The registry answers for what the harness SENT, so
	// the "printed in our spelling" half belongs to the request dump; asserting it on
	// a `Location` was the provenance confusion the sibling thread names
	// (shardpilot/shardpilot-go#85 review).
	got := stripMarks(scrubSupplied(redactQuery("GET /cb?experiment%5Fkey=x HTTP/1.1")))
	if !strings.Contains(got, "experiment_key") {
		t.Fatalf("a name this program owns was lost instead of being printed in our spelling: %q", got)
	}
	if strings.Contains(got, "5F") {
		t.Fatalf("an escape spelling this program never writes was vouched: %q", got)
	}
}

// A known truncation SUPPRESSES the structural refusal; it does not replace the
// report.
//
// ⚠ BOUND BY READING THE SOURCE, because the exit path lives in `main()` and no
// fixture can run it. Two orderings are checkable there and both matter: the
// refusal must be conditioned on the truncation, and the truncation's own exit
// must come after the report is written -- my first version exited before the
// only `io.WriteString`, discarding the incomplete capture the loop had just
// assembled.
func TestTruncationSuppressesTheRefusalWithoutDiscardingTheReport(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("the scene cannot read its own subject: %v", err)
	}
	text := string(src)
	// ⚠ THE CONDITION MOVED FROM "the last attempt" TO "attributed to a truncated
	// attempt", because the ledger is global and the old form let a later truncated
	// retry excuse a COMPLETE earlier attempt's refusal
	// (shardpilot/shardpilot-go#85 review). What is checkable is that the refusal
	// reads the ATTRIBUTED set rather than the whole ledger.
	guarded := strings.Index(text, "if len(unexcused) > 0 {")
	if guarded < 0 {
		t.Fatal("the refusal reads the whole ledger rather than the refusals it may excuse")
	}
	// The attribution itself is a FUNCTION now and is measured directly by
	// TestATruncatedRetryDoesNotExcuseAnEarlierCompleteAttempt; what remains
	// checkable only from the source is that `main` uses it rather than the raw
	// ledger.
	if !strings.Contains(text, "unexcusedRefusals(refusalLedger(), perExchange)") {
		t.Fatal("main does not attribute refusals to the attempt that raised them")
	}
	write := strings.Index(text, "io.WriteString(os.Stdout, stripMarks(report.String()))")
	exit3 := strings.Index(text, "case last.truncErr() != nil:")
	if write < 0 || exit3 < 0 {
		t.Fatalf("one of the two anchors was not found (write=%d exit3=%d), so this scene measures nothing", write, exit3)
	}
	if exit3 < write {
		t.Fatal("the truncation exit runs before the report is written, so the capture is discarded")
	}
	if guarded > write {
		t.Fatal("the refusal runs after the write, which is not where it can suppress anything")
	}
}

// Numeric syntax constrains the representation, not the author.
func TestACollidingJSONNumberIsRefused(t *testing.T) {
	suppliedValues = []string{"123456"}
	structuralSurfaces, accountedSurfaces = nil, nil
	t.Cleanup(func() {
		suppliedValues = nil
		structuralSurfaces, accountedSurfaces = nil, nil
	})
	dropFraming("HTTP/1.1 200 OK\r\n\r\n{\"version\":123456}")
	if len(structuralSurfaces) == 0 {
		t.Fatal("a number equal to a supplied identifier was vouched as grammar")
	}
	// AND AN ORDINARY NUMBER IS STILL GRAMMAR.
	suppliedValues = []string{"1"}
	structuralSurfaces, accountedSurfaces = nil, nil
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n{\"version\":7}"))); !strings.Contains(got, `"version":7`) {
		t.Fatalf("an ordinary number stopped being grammar: %q", got)
	}
}

// A container in verdict position is not a scalar the SDK classified.
func TestTaxonomyIsNotVouchedInsideAContainer(t *testing.T) {
	suppliedValues = []string{"kill_switch"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false,\"reason\":[\"kill_switch\"]}")))
	if strings.Contains(got, "kill_switch") {
		t.Fatalf("an array element in verdict position was vouched as SDK taxonomy: %q", got)
	}
}

// An endpoint-chosen member NAME is a string the endpoint chose.
func TestNonSchemaMemberNamesAreLengthened(t *testing.T) {
	structuralSurfaces, accountedSurfaces = nil, nil
	t.Cleanup(func() { structuralSurfaces, accountedSurfaces = nil, nil })
	got := stripMarks(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n{\"variant_payload\":{\"server-secret-token\":1}}"))
	if strings.Contains(got, "server-secret-token") {
		t.Fatalf("a nested endpoint-chosen member name was published: %q", got)
	}
	if !strings.Contains(got, `"variant_payload"`) {
		t.Fatalf("the schema's own member name was lengthened too: %q", got)
	}
}

// A minted-field placeholder is generated text; a later pass must not remeasure it.
func TestAMintedPlaceholderSurvivesTheValuePass(t *testing.T) {
	structuralSurfaces, accountedSurfaces = nil, nil
	t.Cleanup(func() { structuralSurfaces, accountedSurfaces = nil, nil })
	got := stripMarks(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n{\"subject_fact_key\":\"sfk1_abcdefghij\"}"))
	if !strings.Contains(got, "15 chars") && !strings.Contains(got, "redacted-15-chars") {
		t.Fatalf("the minted identifier's length was replaced by the placeholder's: %q", got)
	}
}

// A cookie attribute admitted by SHAPE is not vouched over a supplied value.
func TestACollidingCookieAttributeValueIsNotVouched(t *testing.T) {
	suppliedValues = []string{"123456"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; Max-Age=123456\r\n\r\n{\"assigned\":false}")))
	if strings.Contains(got, "123456") {
		t.Fatalf("a supplied identifier was vouched as a cookie attribute value: %q", got)
	}
}

// TestEveryLedgerSiteDeclaresOneOfTheEnumeratedForms holds the claim's
// enumeration against the code.
//
// ⚠ THE QUESTION IS CONSTRUCTIVE, NOT LEXICAL. Every ledger call takes the form
// as a PARAMETER, so this reads which form each site DECLARED rather than
// guessing from its prose. The first version matched keywords in the reason text
// and reported three false rejections on correct code -- `a redirect target`
// contains no word from any form's list -- which is what a lexical criterion does
// when both sides are English.
//
// A fifth form fails to compile, since `captureForm` is a closed set of
// constants; what this scene adds is that the four constants are the four the
// claim states, and that every site passes one of them rather than a fabricated
// value.
func TestEveryLedgerSiteDeclaresOneOfTheEnumeratedForms(t *testing.T) {
	claim := ""
	sites := 0
	for _, f := range []string{"main.go", "redact.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("the scene cannot read %s: %v", f, err)
		}
		text := string(src)
		if f == "main.go" {
			claim = text[:strings.Index(text, "\npackage ")]
		}
		re := regexp.MustCompile(`note(?:Structural|Accounted)\((form[A-Za-z]+)`)
		for _, m := range re.FindAllStringSubmatch(text, -1) {
			sites++
			known := false
			for _, c := range captureForms {
				if m[1] == formConstName(c) {
					known = true
					break
				}
			}
			if !known {
				t.Errorf("a ledger site declares %s, which is not one of the enumerated forms", m[1])
			}
		}
	}
	if sites < 10 {
		t.Fatalf("only %d ledger sites were found, so this scene measures almost nothing", sites)
	}
	for _, c := range captureForms {
		if !strings.Contains(claim, string(c)) {
			t.Errorf("the claim does not name the form %q that the code declares", string(c))
		}
	}
}

// formConstName maps a form to the identifier its call sites use.
func formConstName(c captureForm) string {
	switch c {
	case formBody:
		return "formBody"
	case formField:
		return "formField"
	case formRequest:
		return "formRequest"
	case formDiagnostic:
		return "formDiagnostic"
	}
	return ""
}

// The verdict taxonomy keeps its JSON quotes: without them the body no longer
// parses, and the body rule then refuses every ordinary verdict.
func TestAVouchedTaxonomyValueKeepsItsQuotes(t *testing.T) {
	structuralSurfaces, accountedSurfaces = nil, nil
	t.Cleanup(func() { structuralSurfaces, accountedSurfaces = nil, nil })
	got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false,\"reason\":\"kill_switch\"}"))
	if !strings.Contains(got, `"kill_switch"`) {
		t.Fatalf("the quotes around a vouched verdict value were dropped: %q", got)
	}
	if len(structuralSurfaces) != 0 {
		t.Fatalf("an ordinary verdict capture was refused: %v", structuralSurfaces)
	}
}

// A schema key is exempt by its SPELLING, not by what it decodes to.
func TestAnEscapedSchemaKeyIsNotExempt(t *testing.T) {
	suppliedValues = []string{"0061"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n{\"\\u0061ssigned\":false}")))
	if strings.Contains(got, "0061") {
		t.Fatalf("an escape spelling of a schema key carried a supplied value out: %q", got)
	}
}

// A non-canonical field name that collides is replaced token-safely, not by prose.
func TestANonCanonicalFieldNameStaysAName(t *testing.T) {
	suppliedValues = []string{"DATE"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\nDATE: Sun, 06 Nov 1994 08:49:37 GMT\r\n\r\n{\"assigned\":false}")))
	nm := strings.SplitN(strings.SplitN(got, "\r\n", 2)[1], ":", 2)[0]
	if strings.ContainsAny(nm, " <>,") || nm == "" {
		t.Fatalf("a field name became prose: %q", got)
	}
	if strings.Contains(nm, "DATE") {
		t.Fatalf("the supplied identifier survived in the field name: %q", got)
	}
}

// "This capture is incomplete" excuses what an incomplete BODY produces, from the
// attempts that were incomplete -- and nothing else.
func TestTruncationExcusesOnlyItsOwnBodyRefusals(t *testing.T) {
	bodyShape := "a response body in a shape this build cannot describe"
	coding := "a body in a content coding this build cannot decode"
	cookie := "a Set-Cookie header with no name=value pair"
	restore := structuralAt
	t.Cleanup(func() { structuralAt = restore })

	// ⚠ AND ONLY WHERE TRUNCATION EXPLAINS IT. The rows carry `jsonTruncated`
	// because being incomplete is not by itself a reason a body failed to parse
	// (shardpilot/shardpilot-go#85 review).
	//
	// A truncated attempt's OWN body refusal is excused.
	structuralAt = map[string][]int{bodyShape: {0}}
	if got := unexcusedRefusals([]string{bodyShape}, []exchangeRefusals{{truncated: true, jsonTruncated: true}}); len(got) != 0 {
		t.Fatalf("a truncated attempt's own body refusal was not excused: %v", got)
	}

	// ⚠ A COMPLETE ATTEMPT RAISING THE SAME REASON KEEPS IT. The de-duplication
	// used to be by reason alone, so the complete attempt never recorded its own
	// entry and the sole one read as the truncated attempt's.
	structuralAt = map[string][]int{bodyShape: {0, 1}}
	if got := unexcusedRefusals([]string{bodyShape}, []exchangeRefusals{{truncated: false}, {truncated: true, jsonTruncated: true}}); len(got) != 1 {
		t.Fatalf("a complete attempt's body refusal was excused by a truncated one: %v", got)
	}

	// ⚠ AND A REASON THE TRUNCATION DOES NOT EXPLAIN IS NEVER EXCUSED. An
	// undecodable coding on a truncated response still hides whatever arrived.
	structuralAt = map[string][]int{coding: {0}, cookie: {0}}
	got := unexcusedRefusals([]string{coding, cookie}, []exchangeRefusals{{truncated: true, jsonTruncated: true}})
	if len(got) != 2 {
		t.Fatalf("refusals unrelated to the truncation were excused by it: %v", got)
	}
}

// Body redaction is linear in the number of values.
//
// ⚠ MEASURED, AND THE BOUND COMES FROM THE MEASUREMENT -- after the first bound
// failed to. Splicing per span takes 2.04s on this input and the single pass takes
// 0.01s, so a 2s bound sat ON the boundary and the mutant passed once and failed
// once. 500ms is fifty times the linear form and four times under the quadratic
// one; both numbers are here so the choice can be audited rather than trusted.
func TestBodyRedactionIsLinear(t *testing.T) {
	structuralSurfaces, accountedSurfaces = nil, nil
	t.Cleanup(func() { structuralSurfaces, accountedSurfaces = nil, nil })
	var b strings.Builder
	b.WriteString(`{"variant_payload":[`)
	for i := 0; i < 30000; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`"abcd"`)
	}
	b.WriteString("]}")
	body := b.String()

	start := time.Now()
	got := redactUnaccountedJSONValues(body, assignmentTopLevel, "HTTP/1.1 200 OK")
	elapsed := time.Since(start)

	if !strings.Contains(got, "redacted-4-chars") {
		t.Fatalf("nothing was redacted, so the scene measured an empty walk: %.60q", got)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("redacting %d values took %v: that is the per-span rebuild", 30000, elapsed)
	}
}

// The ledger records WHICH attempt raised each reason, so a later attempt is not
// de-duplicated away by an earlier one.
func TestTheLedgerRecordsTheAttemptThatRaisedAReason(t *testing.T) {
	structuralSurfaces, structuralAt = nil, map[string][]int{}
	saved := currentExchange
	t.Cleanup(func() {
		structuralSurfaces, structuralAt = nil, map[string][]int{}
		currentExchange = saved
	})
	reason := "a response body in a shape this build cannot describe"
	currentExchange = 0
	noteStructural(formBody, reason)
	currentExchange = 1
	noteStructural(formBody, reason)
	if len(structuralSurfaces) != 1 {
		t.Fatalf("the reason was recorded twice in the ledger: %v", structuralSurfaces)
	}
	if len(structuralAt[reason]) != 2 {
		t.Fatalf("the second attempt's instance was lost: %v", structuralAt[reason])
	}
}

// A scalar is a whole JSON document, and the body rule accepts it as one.
//
// ⚠ THE POPULATION IS "A JSON ROOT", AND THE GATE NAMED TWO OF ITS SEVEN FORMS.
// `markBareJSONLiterals` began at the first `{` or `[`, so a body that is exactly
// `null`, `true`, `false`, a number or a string returned unexamined -- neither
// marked as grammar nor noted as a collision -- and the scrub downstream replaced
// the whole document with a bare `<redacted, N chars>`, which is not JSON, while
// the refusal ledger stayed EMPTY (shardpilot/shardpilot-go#85 review).
//
// The property asserted is the one that was broken, not the fix: whatever comes
// out is either a JSON document or a refusal. Published-and-invalid is the state
// that must not exist, and the empty ledger is what made it publishable.
//
// The rows are the product of the root forms with a supplied value that collides
// with each, so a later form added to the grammar is covered by adding it here
// rather than by remembering this scene exists.
func TestEveryJSONRootFormIsEitherGrammarOrRefused(t *testing.T) {
	roots := []string{`null`, `true`, `false`, `123456`, `9876543210987654`, `12.5`,
		`"9876543210987654"`, `{"member":9876543210987654}`, `[9876543210987654]`}
	for _, body := range roots {
		supplied := strings.Trim(body, `"`)
		suppliedValues = []string{supplied}
		structuralSurfaces, accountedSurfaces = nil, nil
		got := scrubSupplied(redactUnaccountedBody(markBareJSONLiterals(
			redactUnaccountedJSONValues(redactMintedBody(body, assignmentTopLevel), assignmentTopLevel, "HTTP/1.1 200 OK"), assignmentTopLevel)))
		clean := strings.NewReplacer(capturedMark, "", genMark, "").Replace(got)
		var any interface{}
		valid := json.Unmarshal([]byte(clean), &any) == nil
		refused := len(structuralSurfaces) > 0
		if !valid && !refused {
			t.Errorf("root %q was published as %q, which is not JSON, with an empty ledger", body, clean)
		}
	}
	suppliedValues = nil
	structuralSurfaces, accountedSurfaces = nil, nil
}

// Recognising a taxonomy token is not having emitted it, and the position has to
// be the EFFECTIVE one.
//
// ⚠ THE ORACLE IS `encoding/json`, NOT A LIST I WROTE. JSON permits duplicate
// members and the decoder keeps the LAST, so the SDK's classification is whatever
// the decoder returns; the traversal vouched every recognised `code` or `reason`
// value, which published a supplied `kill_switch` as though this program had
// written it while the SDK actually classified `not_found`
// (shardpilot/shardpilot-go#85 review). The scene decodes each body and asks the
// decoder which occurrence counts, so it cannot drift from the rule it pins.
//
// Rows are the product of the two verdict fields with both orderings, because the
// defect is invisible in one of them: when the supplied value happens to come
// last it IS the effective one and vouching it is correct.
func TestOnlyTheEffectiveVerdictOccurrenceIsVouched(t *testing.T) {
	const supplied = "kill_switch"
	other := "not_found"
	for field := range sdkVerdictFields {
		for _, order := range [][2]string{{supplied, other}, {other, supplied}} {
			// ⚠ A COMPLETE NOT-ASSIGNED SHAPE. The SDK rejects a body with no `assigned`
			// member before it reads `reason` at all, so a duplicate-member scene built
			// without it exercises a body whose `reason` is never the SDK's
			// (shardpilot/shardpilot-go#85 review).
			body := `{"assigned":false,"` + field + `":"` + order[0] + `","` + field + `":"` + order[1] + `"}`
			// The oracle decodes only the member under test; `assigned` is a bool and
			// a `map[string]string` cannot hold it.
			var decoded map[string]json.RawMessage
			if err := json.Unmarshal([]byte(body), &decoded); err != nil {
				t.Fatalf("the oracle could not read %q: %v", body, err)
			}
			var effective string
			_ = json.Unmarshal(decoded[field], &effective)
			suppliedValues = []string{supplied}
			structuralSurfaces, accountedSurfaces = nil, nil
			got := scrubSupplied(redactUnaccountedBody(markBareJSONLiterals(
				redactUnaccountedJSONValues(redactMintedBody(body, assignmentTopLevel), assignmentTopLevel, "HTTP/1.1 200 OK"), assignmentTopLevel)))
			clean := strings.NewReplacer(capturedMark, "", genMark, "").Replace(got)
			printed := strings.Count(clean, supplied)
			want := 0
			if effective == supplied {
				want = 1 // the SDK's own classification, vouched by denotation
			}
			if printed != want {
				t.Errorf("%s: supplied value printed %d times, want %d (decoder classifies %q): %q",
					body, printed, want, effective, clean)
			}
		}
	}
	suppliedValues = nil
	structuralSurfaces, accountedSurfaces = nil, nil
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

// A verdict position is a member of the wire contract, not a word from the same
// vocabulary.
//
// ⚠ `code` LOOKED LIKE ONE AND IS A MEMBER OF NO TOP-LEVEL SHAPE. The assignment
// struct has no such member, the ingest error envelope spells it `error.code` --
// nested, which this root-only rule never reaches -- and the SDK synthesizes its
// own `Code` from the HTTP outcome. Vouching it marked endpoint-controlled text as
// this SDK's classification and published a supplied identifier from
// `{"assigned":false,"code":"kill_switch"}` (shardpilot/shardpilot-go#85 review).
// Four fixtures asserted `code` WAS a verdict position and pinned the wrong half
// of the rule until the contract was read.
//
// The population is the struct, read at test time out of the SDK source.
func TestVerdictPositionsAreWireMembers(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "../../experiments.go", nil, 0)
	if err != nil {
		t.Fatalf("the SDK source is the oracle and it could not be read: %v", err)
	}
	wire := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "expAssignmentWire" {
			return true
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok {
			return false
		}
		for _, fld := range st.Fields.List {
			if fld.Tag == nil {
				continue
			}
			tag, err := strconv.Unquote(fld.Tag.Value)
			if err != nil {
				continue
			}
			name, _, _ := strings.Cut(reflect.StructTag(tag).Get("json"), ",")
			if name != "" && name != "-" {
				wire[name] = true
			}
		}
		return false
	})
	if len(wire) == 0 {
		t.Fatal("no members were read: the oracle found nothing, which is not agreement")
	}
	for name := range sdkVerdictFields {
		if !wire[name] {
			t.Errorf("%q is vouched as a verdict position and is not a top-level member of expAssignmentWire", name)
		}
	}
	// And the behaviour the rule exists for.
	for _, body := range []string{
		`{"assigned":false,"code":"kill_switch"}`,
		`{"code":"kill_switch"}`,
		`{"variant_payload":{"code":"kill_switch"}}`,
	} {
		suppliedValues = []string{"kill_switch"}
		structuralSurfaces, accountedSurfaces = nil, nil
		got := scrubSupplied(redactUnaccountedBody(markBareJSONLiterals(
			redactUnaccountedJSONValues(redactMintedBody(body, assignmentTopLevel), assignmentTopLevel, "HTTP/1.1 200 OK"), assignmentTopLevel)))
		if clean := strings.NewReplacer(capturedMark, "", genMark, "").Replace(got); strings.Contains(clean, "kill_switch") {
			t.Errorf("%s: a supplied identifier at a non-verdict position was published: %q", body, clean)
		}
	}
	suppliedValues = nil
	structuralSurfaces, accountedSurfaces = nil, nil
}

// A capture that declines to vouch a token must still leave the grammar intact.
//
// ⚠ THE PROSE SCRUB IS NOT A SAFE FALLBACK IN A STRUCTURED POSITION. Restricting a
// vouch to the canonical spelling is right and leaves the non-canonical one for
// `scrubSupplied`, which writes `<redacted, N chars>` -- spaces, a comma and angle
// brackets. In a URI scheme or a cookie attribute name those are not admissible
// bytes, so the "structure-preserving" capture was malformed and the guard
// approved it, because a placeholder is generated
// (shardpilot/shardpilot-go#85 review). The valueless-cookie-flag branch already
// answered this; the answer was applied where it was shown.
//
// The rows are the product of the structural positions with the three cases a
// spelling can be in -- canonical, non-canonical and colliding, non-canonical and
// not -- and the assertion is on the GRAMMAR of what came out, not on a spelling.
func TestDecliningToVouchStillKeepsTheGrammar(t *testing.T) {
	schemeOK := regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.\-]*$`)
	tokenOK := regexp.MustCompile(`^[!#$%&'*+.^_` + "`" + `|~0-9A-Za-z-]+$`)
	for _, supplied := range []string{"HTTPS", "https", "PATH", "Path", "unrelated"} {
		suppliedValues = []string{supplied}
		structuralSurfaces, accountedSurfaces = nil, nil

		loc := stripMarks(scrubSupplied(dropFraming(
			"HTTP/1.1 302 Found\r\nLocation: HTTPS://e.example/cb\r\n\r\n")))
		for _, ln := range strings.Split(loc, "\r\n") {
			v, ok := strings.CutPrefix(ln, "Location: ")
			if !ok {
				continue
			}
			sch, _, found := strings.Cut(v, "://")
			if !found || !schemeOK.MatchString(sch) {
				t.Errorf("supplied %q: published scheme %q is not a scheme", supplied, sch)
			}
		}

		ck := stripMarks(scrubSupplied(dropFraming(
			"HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; PATH=/; Secure\r\n\r\n")))
		for _, ln := range strings.Split(ck, "\r\n") {
			v, ok := strings.CutPrefix(ln, "Set-Cookie: ")
			if !ok {
				continue
			}
			for _, attr := range strings.Split(v, ";")[1:] {
				name, _, _ := strings.Cut(strings.TrimSpace(attr), "=")
				if !tokenOK.MatchString(name) {
					t.Errorf("supplied %q: published cookie attribute name %q is not a token (%q)",
						supplied, name, v)
				}
			}
		}
	}
	suppliedValues = nil
	structuralSurfaces, accountedSurfaces = nil, nil
}

// The finding's own case: certificate-controlled names are not taken.
//
// ⚠ THE DECISIVE FACT IS THAT `x509.HostnameError` HAS NO FIELD HOLDING THAT
// LIST. `Error()` builds it from `Certificate.DNSNames`, so a rule about which
// EXTENTS of the rendered message to redact was maintaining an enumeration against
// someone else's prose -- and `x509: certificate is valid for
// server-secret-token.internal, not configured.example` carries the names with no
// quotes, so the quoted-extent rule left them standing with an empty ledger
// (shardpilot/shardpilot-go#85 review). Working from the value, the names are not
// redacted: they are never taken.
//
// The rows are the product of the SAN spellings with the two things a name can be
// -- listed and not -- so the assertion is that NO certificate-controlled name
// reaches the artifact, in any of them.
func TestCertificateNamesAreNeverTaken(t *testing.T) {
	sans := [][]string{
		{"server-secret-token.internal"},
		{"a.internal", "server-secret-token.internal", "b.internal"},
		{"configured.example"},
		nil,
	}
	// ⚠ BOTH SHAPES, AND THE VALUE IS THE ONE THAT OCCURS. This scene built only
	// `&x509.HostnameError{…}`; `crypto/x509` returns the error BY VALUE, so every
	// row here exercised a shape the verifier never produces and the branch that
	// handles the real one was unreachable and unmeasured -- deleting it broke no
	// scene (shardpilot/shardpilot-go#85 review). A fixture that constructs the
	// subject itself can construct one that does not occur.
	for _, names := range sans {
		for _, byValue := range []bool{false, true} {
			structuralSurfaces, accountedSurfaces = nil, nil
			suppliedValues = nil
			var inner error = &x509.HostnameError{
				Certificate: &x509.Certificate{DNSNames: names},
				Host:        "configured.example",
			}
			if byValue {
				inner = x509.HostnameError{
					Certificate: &x509.Certificate{DNSNames: names},
					Host:        "configured.example",
				}
			}
			err := &url.Error{Op: "Get", URL: "https://configured.example/x",
				Err: &tls.CertificateVerificationError{Err: inner}}
			out := stripMarks(sanitizeCaptured(err))
			for _, n := range names {
				if strings.Contains(out, n) {
					t.Errorf("a certificate-controlled name %q reached the artifact (byValue=%v): %q", n, byValue, out)
				}
			}
			if !strings.Contains(out, "names="+strconv.Itoa(len(names))) {
				t.Errorf("the count an operator needs is missing (byValue=%v): %q", byValue, out)
			}
			// And stderr, which the guard never reads and which used to keep printing
			// the rendered message after the report stopped.
			errOut := sanitize(err)
			for _, n := range names {
				if strings.Contains(errOut, n) {
					t.Errorf("a certificate-controlled name %q reached stderr (byValue=%v): %q", n, byValue, errOut)
				}
			}
		}
	}
	structuralSurfaces, accountedSurfaces = nil, nil
}

// Coverage, measured rather than asserted: the transport failures this harness
// actually meets are described, and anything else refuses by name.
//
// The five rows are the chains produced by real failures against real endpoints,
// read off `errors.Unwrap` rather than recalled: a closed port, a name that does
// not resolve, an untrusted certificate, a client deadline and a cancelled
// context. A type outside them is a refusal, which is the honest edge of a set
// that cannot be closed -- `OpError.Err` is an `error`.
func TestTheMeasuredTransportFailuresAreDescribable(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
	}{
		{"connection refused", &url.Error{Op: "Get", Err: &net.OpError{Op: "dial", Net: "tcp",
			Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}}},
		{"name not found", &url.Error{Op: "Get", Err: &net.OpError{Op: "dial", Net: "tcp",
			Err: &net.DNSError{IsNotFound: true}}}},
		{"untrusted certificate", &url.Error{Op: "Get",
			Err: &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}}},
		{"client deadline", &url.Error{Op: "Get", Err: context.DeadlineExceeded}},
		{"cancelled", &url.Error{Op: "Get", Err: context.Canceled}},
	} {
		if _, ok := describeTransportError(c.err, 0); !ok {
			t.Errorf("%s: a transport failure this harness meets is not describable, so it refuses", c.name)
		}
	}
	if _, ok := describeTransportError(errors.New("anything"), 0); ok {
		t.Error("an unrecognised type was described, so the edge of the set is not where it is stated")
	}
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

// The decoder folds member names, and the occurrence counting folds with it.
//
// ⚠ EXACT-STRING COUNTING PUT ONE MEMBER UNDER TWO KEYS. `encoding/json` matches
// `Reason` and `REASON` to the same field, so
// `{"reason":"kill_switch","Reason":"anything"}` is classified `anything` -- while
// the ordinal counted the two occurrences separately, left the first vouched, and
// published the supplied identifier (shardpilot/shardpilot-go#85 review). The
// ordinal answers "which occurrence does the decoder keep", so it has to group
// members exactly as the decoder groups them.
//
// This is the one place in this file where folding is CORRECT: it models the
// decoder. Vouching the VALUE still requires the canonical spelling, because that
// question is about what this program wrote.
//
// The rows are the product of the case variants with both orderings, and the
// oracle is the decoder itself.
func TestVerdictOccurrencesAreCountedAsTheDecoderGroupsThem(t *testing.T) {
	const supplied = "kill_switch"
	other := "anything"
	variants := []string{"reason", "Reason", "REASON", "ReAsOn"}
	for _, a := range variants {
		for _, b := range variants {
			for _, order := range [][2]string{{supplied, other}, {other, supplied}} {
				body := `{"assigned":false,"` + a + `":"` + order[0] + `","` + b + `":"` + order[1] + `"}`
				var decoded struct{ Reason string }
				if err := json.Unmarshal([]byte(body), &decoded); err != nil {
					t.Fatalf("the oracle could not read %q: %v", body, err)
				}
				suppliedValues = []string{supplied}
				structuralSurfaces, accountedSurfaces = nil, nil
				got := scrubSupplied(redactUnaccountedBody(markBareJSONLiterals(
					redactUnaccountedJSONValues(redactMintedBody(body, assignmentTopLevel), assignmentTopLevel, "HTTP/1.1 200 OK"), assignmentTopLevel)))
				clean := strings.NewReplacer(capturedMark, "", genMark, "").Replace(got)
				printed := strings.Count(clean, supplied)
				want := 0
				if decoded.Reason == supplied {
					want = 1 // the SDK's own classification, vouched by denotation
				}
				if printed != want {
					t.Errorf("%s: printed %d times, want %d (decoder classifies %q): %q",
						body, printed, want, decoded.Reason, clean)
				}
			}
		}
	}
	suppliedValues = nil
	structuralSurfaces, accountedSurfaces = nil, nil
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

// The IPvFuture body is endpoint text inside an exempt component.
//
// ⚠ THIS SCENE IS UNIT-LEVEL ON PURPOSE, AND SAYS SO. Measured on this Go
// version, `url.Parse` rejects every IPvFuture authority — `ParseAddr("v1.abc"):
// unexpected character` — so the host exemption cannot be reached with one and no
// end-to-end input publishes such a body. The redaction is insurance against
// `net/url` accepting the grammar RFC 3986 defines, so the only honest scene calls
// the pass directly (shardpilot/shardpilot-go#85 review, premise measured).
//
// The rows are the product of the shapes the grammar admits: a hex version of one
// and several digits, and a body carrying unreserved, sub-delim and colon bytes.
func TestTheIPvFutureBodyIsRedacted(t *testing.T) {
	for _, ver := range []string{"v1", "vA", "v1f"} {
		for _, body := range []string{"server-secret-token", "a.b:c", "x~y!z", "s"} {
			line := "Location: https://[" + ver + "." + body + "]/cb"
			got := stripMarks(redactIPvFutureBody(line))
			if strings.Contains(got, body) && len(body) > 1 {
				t.Errorf("%q: the IPvFuture body survived: %q", line, got)
			}
			if !strings.Contains(got, "["+ver+".") {
				t.Errorf("%q: the grammar that makes it an authority was lost: %q", line, got)
			}
		}
	}
	// And an authority that is NOT IPvFuture is untouched by this pass.
	for _, line := range []string{
		"Location: https://[fe80::1]/cb",
		"Location: https://e.example/cb",
		"Location: https://[v1]/cb",
	} {
		if got := redactIPvFutureBody(line); got != line {
			t.Errorf("%q was rewritten by the IPvFuture pass: %q", line, got)
		}
	}
}

// An empty cookie name has no length to report.
//
// ⚠ `strings.Cut` REPORTS A PAIR FOR `=secret`. The name is empty, `hasValue` is
// true, and `tokenPlaceholder("")` rendered `redacted-0-chars=redacted-6-chars` --
// an invalid cookie presented as a syntactically valid one this recorder invented,
// hiding the very response defect the artifact exists to preserve
// (shardpilot/shardpilot-go#85 review).
//
// Narrowed to EMPTY rather than to "not a cookie token", which the review asked
// for: a name of one NUL byte is not a token either, and this file deliberately
// MEASURES that one — the wider predicate turned
// `TestEveryComponentIsMeasuredAsReceived` red.
func TestAnEmptyCookieNameIsWithheld(t *testing.T) {
	for _, c := range []struct {
		line     string
		withheld bool
	}{
		{"Set-Cookie: =secret", true},
		{"Set-Cookie:  =secret", true},
		{"Set-Cookie: =", true},
		{"Set-Cookie: sid=x", false},
		{"Set-Cookie: " + escapeMarks(capturedMark) + "=x", false},
	} {
		suppliedValues = nil
		structuralSurfaces, accountedSurfaces = nil, nil
		got := stripMarks(redactSetCookie(c.line))
		if w := strings.Contains(got, "<withheld>"); w != c.withheld {
			t.Errorf("%q: withheld=%v, want %v: %q", c.line, w, c.withheld, got)
		}
		if c.withheld && len(structuralSurfaces) == 0 {
			t.Errorf("%q was withheld without a refusal in the ledger", c.line)
		}
		if strings.Contains(got, "redacted-0-chars") {
			t.Errorf("%q produced a zero-length placeholder, which is a cookie this recorder invented: %q", c.line, got)
		}
	}
	structuralSurfaces, accountedSurfaces = nil, nil
}

// A trailer coding refusal is about bytes, and an empty body has none.
//
// ⚠ THE HEADER PATH ASKS `hasBody` AND THE TRAILER PATH DID NOT. A zero-length
// response announcing `Content-Encoding: br` in its trailer had an otherwise
// publishable capture withheld with exit 4 over bytes that do not exist
// (shardpilot/shardpilot-go#85 review) — the fifth time a rule written on the
// header path had to be carried to the trailer path.
func TestATrailerCodingOnAnEmptyBodyDoesNotRefuse(t *testing.T) {
	for _, c := range []struct {
		body    string
		refused bool
	}{
		{"", false}, {" ", true}, {"x", true},
	} {
		structuralSurfaces, accountedSurfaces = nil, nil
		suppliedValues = nil
		tee := &teeBody{trailer: map[string][]string{"Content-Encoding": {"br"}}}
		tee.buf.WriteString(c.body)
		(&exchange{captured: tee}).trailerReport()
		if got := len(structuralSurfaces) > 0; got != c.refused {
			t.Errorf("body %q: refused=%v, want %v", c.body, got, c.refused)
		}
	}
	structuralSurfaces, accountedSurfaces = nil, nil
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

// A position is the SDK's only where the SDK reads it.
//
// ⚠ AND READING IT IS CONDITIONAL ON THE REST OF THE DOCUMENT. The assigned branch
// RETURNS before `reason` is read, so in a valid assigned response the member is
// endpoint-chosen text the SDK never looks at — and vouching it published a
// supplied identifier out of an ordinary success (shardpilot/shardpilot-go#85
// review). The rounds before moved this question from the TOKEN to the POSITION
// and then to the EFFECTIVE position; this is the third axis, and it is about the
// document rather than about the member.
//
// The allowlist narrows with it: the not-assigned branch takes `{absent,
// kill_switch, targeting_unmatched}` and refuses the body for anything else, so
// the wider taxonomy would vouch a value the SDK itself rejects.
func TestReasonIsVouchedOnlyWhereTheSDKReadsIt(t *testing.T) {
	for _, c := range []struct {
		body    string
		vouched bool
	}{
		{`{"assigned":true,"assignment_key":"a","variant_key":"v","version":1,"reason":"kill_switch"}`, false},
		{`{"assigned":false,"reason":"kill_switch"}`, true},
		// ⚠ NO `assigned` MEMBER: the SDK rejects the body at `wire.Assigned == nil`
		// before it reads `reason` at all, so the value is never its classification
		// (shardpilot/shardpilot-go#85 review). This row asserted the opposite while
		// the condition was only "is assigned true".
		{`{"reason":"kill_switch"}`, false},
		{`{"assigned":false,"reason":"targeting_unmatched"}`, true},
		{`{"assigned":false,"reason":"not_found"}`, false},
		// ⚠ AND THE ECHO. `expEchoMatches` requires a PRESENT echoed member to equal
		// the request's own value, which this pass can only check as "is it a value
		// this harness supplied" -- so an echo carrying something else stops the
		// vouch, because the SDK would reject that body and the value would not be
		// its classification (shardpilot/shardpilot-go#85 review).
		{`{"assigned":false,"reason":"kill_switch","app_key":"ak"}`, false},
		{`{"assigned":false,"reason":"kill_switch","experiment_key":"ek"}`, false},

		{`{"assigned":false,"reason":"http_503"}`, false},
	} {
		want := "kill_switch"
		if strings.Contains(c.body, `"app_key"`) || strings.Contains(c.body, `"experiment_key"`) {
			want = "kill_switch"
		} else if strings.Contains(c.body, "targeting_unmatched") {
			want = "targeting_unmatched"
		} else if strings.Contains(c.body, "not_found") {
			want = "not_found"
		} else if strings.Contains(c.body, "http_503") {
			want = "http_503"
		}
		suppliedValues = []string{want}
		structuralSurfaces, accountedSurfaces = nil, nil
		got := scrubSupplied(redactUnaccountedBody(markBareJSONLiterals(
			redactUnaccountedJSONValues(redactMintedBody(c.body, assignmentTopLevel), assignmentTopLevel, "HTTP/1.1 200 OK"), assignmentTopLevel)))
		clean := strings.NewReplacer(capturedMark, "", genMark, "").Replace(got)
		if printed := strings.Contains(clean, want); printed != c.vouched {
			t.Errorf("%s: printed=%v, want %v: %q", c.body, printed, c.vouched, clean)
		}
		suppliedValues = nil
		structuralSurfaces, accountedSurfaces = nil, nil
	}
}

// The values accepted at `reason` are the SDK's own, read from its source.
func TestTheReasonValuesAreTheSDKsOwn(t *testing.T) {
	src, err := os.ReadFile("../../experiments.go")
	if err != nil {
		t.Fatalf("the SDK source is the oracle and it could not be read: %v", err)
	}
	found := map[string]bool{}
	for _, m := range regexp.MustCompile(`experimentReason\w+\s*=\s*"([a-z_]+)"`).FindAllStringSubmatch(string(src), -1) {
		found[m[1]] = true
	}
	if len(found) == 0 {
		t.Fatal("no reason constants were read: an oracle that finds nothing is not agreement")
	}
	for v := range found {
		if !sdkReasonValues[v] {
			t.Errorf("the SDK accepts %q at `reason` and this build does not vouch it", v)
		}
	}
	for v := range sdkReasonValues {
		if !found[v] {
			t.Errorf("%q is vouched at `reason` and the SDK has no such constant", v)
		}
	}
}

// Marking the vouched names is one pass, not one rebuild per name.
//
// ⚠ THE SECOND HALF OF A FIX APPLIED TO THE HALF IT WAS SHOWN. Two rounds ago the
// VALUE-span assembly was rewritten from a right-to-left splice into a builder;
// the NAME-span pass standing beside it kept the splice, and a valid object may
// hold many vouched names (shardpilot/shardpilot-go#85 review).
//
// Measured on this machine at 30,000 duplicate canonical members in a ~510 KB
// body — inside the accepted ~1 MB response limit, and after the network deadline
// has stopped bounding anything: 2.694s splicing, 137ms with the builder. The
// bound below is 500ms, which is a measurement with room rather than a guess.
func TestVouchedNamesAreMarkedInOnePass(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	const n = 30000
	body := "{" + strings.TrimSuffix(strings.Repeat(`"assigned":false,`, n), ",") + "}"
	start := time.Now()
	got := redactMintedBody(body, assignmentTopLevel)
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("marking %d vouched names took %v; the splice it replaced took 2.694s and the builder 137ms", n, d)
	}
	if strings.Count(got, genMark) < 2*n {
		t.Errorf("the names were not all marked: %d mark bytes for %d names", strings.Count(got, genMark), n)
	}
}

// A component is a POSITION in a grammar, not a spelling that occurs somewhere.
//
// ⚠ `strings.Replace` EDITED THE FIRST OCCURRENCE, WHICH IS NOT THE AUTHORITY.
// With a supplied `http`, `http://http/cb` had its SCHEME replaced, and the
// remaining passes emitted `redacted-19-chars//redacted-4-chars/...` — a malformed
// target with no structural refusal recorded (shardpilot/shardpilot-go#85 review).
//
// The rows are the product of the components a supplied value can collide with by
// spelling — the scheme, the host, a path segment — and the assertion is on the
// GRAMMAR of what came out: a scheme, `://`, and an authority.
func TestTheAuthorityIsReplacedAtItsOffset(t *testing.T) {
	schemeOK := regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.\-]*$`)
	for _, c := range []struct{ sup, target string }{
		{"http", "http://http/cb"},
		{"https", "https://https/cb"},
		{"cb", "https://cb/cb"},
		{"example", "https://e.example/cb"},
		{"e", "https://e/e"},
	} {
		suppliedValues = []string{c.sup}
		structuralSurfaces, accountedSurfaces = nil, nil
		got := stripMarks(scrubSupplied(dropFraming(
			"HTTP/1.1 302 Found\r\nLocation: " + c.target + "\r\n\r\n")))
		for _, ln := range strings.Split(got, "\r\n") {
			v, ok := strings.CutPrefix(ln, "Location: ")
			if !ok {
				continue
			}
			sch, rest, found := strings.Cut(v, "://")
			if !found || !schemeOK.MatchString(sch) {
				t.Errorf("%q with %q supplied published %q, which has no scheme", c.target, c.sup, v)
				continue
			}
			if rest == "" || strings.HasPrefix(rest, "/") {
				t.Errorf("%q with %q supplied published %q, which has no authority", c.target, c.sup, v)
			}
		}
		suppliedValues = nil
		structuralSurfaces, accountedSurfaces = nil, nil
	}
}

// Incompleteness is not by itself a reason a body failed to parse.
//
// ⚠ A `text/plain` PAYLOAD THAT WAS ALSO CUT SHORT IS AN UNSUPPORTED SHAPE EITHER
// WAY. The excuse fired on truncation alone, so a partial body carrying
// endpoint-chosen text was printed with its refusal removed — and the guard cannot
// see a value this harness never supplied (shardpilot/shardpilot-go#85 review).
//
// The classification is asked of the DECODER by sentinel: `Decode` returns
// `io.ErrUnexpectedEOF` for a JSON prefix and a syntax error for something that is
// not JSON, so the two are distinguishable without reading an error's text.
func TestTruncationExcusesOnlyWhatTruncationExplains(t *testing.T) {
	for _, c := range []struct {
		body    string
		excuses bool
	}{
		{`{"assigned":false`, true},
		{`{"a":`, true},
		{`[1,2`, true},
		{`{"assigned":false}`, true},
		// ⚠ AND A CUT *INSIDE* A VALUE IS NOT EXCUSED, however good a JSON prefix it
		// is. Neither body redactor can traverse a malformed document, so the extent
		// the cut opened is published unmeasured and the supplied-value scrub cannot
		// see it -- the token is the endpoint's (shardpilot/shardpilot-go#85 review).
		// The row for `"abc` used to say true; a truncated bare string is exactly this
		// case at the root.
		{`"abc`, false},
		{`{"assigned":false,"variant_payload":{"token":"server-secret-tok`, false},
		// ⚠ LIMIT, MEASURED: a cut inside a NUMBER is invisible here. `Token` emits a
		// trailing `12` as a complete number -- it cannot know more digits were coming
		// -- so `{"a":12` is excused and `[1,2` above is too. The decoder is this
		// program's oracle for "is this a JSON prefix", and it does not answer this
		// question; asserting `false` here would be asserting something no instrument
		// in this file can produce.
		{`{"a":12`, true},
		// A tail that OPENS a value extent is refused even when the extent is empty:
		// this file does not reason about how many bytes the cut exposed.
		{`{"a":"`, false},
		{"server-secret-token", false},
		{"<html><body>oops", false},
		{"", false},
	} {
		if got := truncationCausedTheFailure([]byte(c.body)); got != c.excuses {
			t.Errorf("%q: truncation explains the failure=%v, want %v", c.body, got, c.excuses)
		}
	}
	// And the excuse follows it.
	restore := structuralAt
	t.Cleanup(func() { structuralAt = restore })
	shape := "a response body in a shape this build cannot describe"
	structuralAt = map[string][]int{shape: {0}}
	if got := unexcusedRefusals([]string{shape},
		[]exchangeRefusals{{truncated: true, jsonTruncated: false}}); len(got) != 1 {
		t.Errorf("a truncated NON-JSON body had its refusal excused: %v", got)
	}
}

// The assignment shape is exempt at 200 and nowhere else.
//
// ⚠ "NOT AN ERROR" IS NOT "THE ASSIGNMENT SHAPE".
// `applyExperimentAssignment` parses `expAssignmentWire` for status 200 and for
// nothing else, so on a `201` the SDK never decodes that shape — and exempting its
// member names there marked endpoint-chosen text as generated grammar
// (shardpilot/shardpilot-go#85 review). The registry has now been wrong about its
// depth, its membership, its shape, and its STATUS.
func TestAssignmentExemptionsApplyAtTwoHundredOnly(t *testing.T) {
	for _, c := range []struct {
		head    string
		exempts bool
	}{
		{"HTTP/1.1 200 OK", true},
		{"HTTP/1.1 201 Created", false},
		{"HTTP/1.1 204 No Content", false},
		{"HTTP/1.1 302 Found", false},
		{"not a status line", false},
	} {
		suppliedValues = []string{"assigned"}
		structuralSurfaces, accountedSurfaces = nil, nil
		got := stripMarks(scrubSupplied(dropFraming(c.head + "\r\n\r\n" + `{"assigned":false}`)))
		if printed := strings.Contains(got, `"assigned"`); printed != c.exempts {
			t.Errorf("%s: the member name printed=%v, want %v: %q", c.head, printed, c.exempts, got)
		}
		suppliedValues = nil
		structuralSurfaces, accountedSurfaces = nil, nil
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

// An admitted value that collides keeps its field's grammar.
//
// ⚠ THIRD SITE OF ONE SHAPE ON THIS BRANCH. The redirect scheme, the cookie
// attribute name, and now an admitted header value: each time the vouch was
// correctly narrowed to the canonical spelling, and each time the non-canonical
// one was left to a prose scrub that does not know the field's grammar. With a
// supplied `JSON`, `Content-Type: application/JSON` became
// `application/<redacted, 4 chars>` — which `mime.ParseMediaType` rejects even
// though what arrived was a valid media type, with an empty ledger
// (shardpilot/shardpilot-go#85 review).
//
// The assertion is on the GRAMMAR of what came out, over the product of the
// spellings with a colliding and a non-colliding supplied value.
func TestAnAdmittedValueKeepsItsGrammarWhenItCollides(t *testing.T) {
	for _, arrived := range []string{"application/JSON", "application/json", "text/PLAIN"} {
		for _, sup := range []string{"JSON", "json", "PLAIN", "unrelated"} {
			suppliedValues = []string{sup}
			structuralSurfaces, accountedSurfaces = nil, nil
			got := stripMarks(scrubSupplied(dropFraming(
				"HTTP/1.1 200 OK\r\nContent-Type: " + arrived + "\r\n\r\n{}")))
			v := ""
			for _, ln := range strings.Split(got, "\r\n") {
				if x, ok := strings.CutPrefix(ln, "Content-Type: "); ok {
					v = x
				}
			}
			if v == "" {
				continue
			}
			if _, _, err := mime.ParseMediaType(v); err != nil {
				t.Errorf("%q with %q supplied published %q, which is not a media type: %v",
					arrived, sup, v, err)
			}
			if strings.Contains(v, "/") == false {
				t.Errorf("%q with %q supplied published %q, which lost its type", arrived, sup, v)
			}
			suppliedValues = nil
			structuralSurfaces, accountedSurfaces = nil, nil
		}
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

// An open grammar admits by a property a placeholder cannot have.
//
// ⚠ FOURTH SITE OF ONE SHAPE, AND THE FIRST WHERE THE ANSWER IS NOT A TOKEN-SAFE
// SPELLING. `Age: 123456` is admitted because it IS an integer, so with `123456`
// supplied there is nothing to substitute that stays one — the prose scrub emitted
// `Age: <redacted, 6 chars>` with an empty ledger, and a token placeholder would be
// no better (shardpilot/shardpilot-go#85 review). The line is withheld and the
// refusal recorded.
//
// A REGISTRY value is different in kind: it has a canonical spelling this program
// can write, so it is vouched rather than withheld — which is why the rows carry
// both, and why `TestARecognisedMediaTypeIsGrammar` still holds.
func TestAnOpenGrammarCollisionIsRefusedRatherThanMangled(t *testing.T) {
	for _, c := range []struct {
		line, sup string
		withheld  bool
	}{
		{"Age: 123456", "123456", true},
		{"Age: 123456", "unrelated", false},
		{"Content-Type: application/json", "json", false},
		{"Cache-Control: no-store", "no-store", false},
	} {
		suppliedValues = []string{c.sup}
		structuralSurfaces, accountedSurfaces = nil, nil
		got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n" + c.line + "\r\n\r\n")))
		if w := strings.Contains(got, "<withheld>"); w != c.withheld {
			t.Errorf("%q with %q supplied: withheld=%v, want %v: %q", c.line, c.sup, w, c.withheld, got)
		}
		if c.withheld && len(structuralSurfaces) == 0 {
			t.Errorf("%q was withheld without a refusal in the ledger", c.line)
		}
		if strings.Contains(got, "<redacted,") && strings.HasPrefix(c.line, "Age:") {
			t.Errorf("%q became a non-integer Age: %q", c.line, got)
		}
		suppliedValues = nil
		structuralSurfaces, accountedSurfaces = nil, nil
	}
}

// A name we sent in a query proves nothing about a name in someone else's fragment.
//
// ⚠ SECOND TIME THIS REGISTRY HAS BEEN READ AS SAYING MORE THAN IT DOES. The round
// before took it out of the COOKIE path for the same reason, and the fragment stood
// one caller away: with an experiment key of `experiment_key`, an endpoint's
// `Location: /cb#experiment_key=x` had that member name marked harness-authored, so
// the scrub and the guard both skipped it (shardpilot/shardpilot-go#85 review).
func TestFragmentNamesAreNotInferredFromTheRequestQuery(t *testing.T) {
	restore := requestNames
	t.Cleanup(func() { requestNames = restore; suppliedValues = nil })
	requestNames = map[string]bool{"experiment_key": true}
	for _, sup := range []string{"experiment_key", "unrelated"} {
		suppliedValues = []string{sup}
		structuralSurfaces, accountedSurfaces = nil, nil
		got := stripMarks(scrubSupplied(dropFraming(
			"HTTP/1.1 302 Found\r\nLocation: /cb#experiment_key=x\r\n\r\n")))
		if strings.Contains(got, "#experiment_key=") {
			t.Errorf("supplied %q: an endpoint's fragment name was vouched as ours: %q", sup, got)
		}
	}
}

// The not-assigned shape includes `version`, and trailing data is not truncation.
//
// ⚠ FIFTH AXIS OF ONE QUESTION. `version` is presence-aware: absent is tolerated,
// a present one must decode to a number and be at least 1, and an explicit null is
// PRESENT and rejected — so `{"assigned":false,"version":null,"reason":"kill_switch"}`
// is a body the SDK refuses, and `reason` there is not its classification
// (shardpilot/shardpilot-go#85 review).
func TestTheNotAssignedShapeIncludesVersion(t *testing.T) {
	for _, c := range []struct {
		body    string
		vouched bool
	}{
		{`{"assigned":false,"reason":"kill_switch"}`, true},
		{`{"assigned":false,"version":1,"reason":"kill_switch"}`, true},
		{`{"assigned":false,"version":null,"reason":"kill_switch"}`, false},
		{`{"assigned":false,"version":0,"reason":"kill_switch"}`, false},
		{`{"assigned":false,"version":-1,"reason":"kill_switch"}`, false},
		{`{"assigned":false,"version":"1","reason":"kill_switch"}`, false},
	} {
		suppliedValues = []string{"kill_switch"}
		structuralSurfaces, accountedSurfaces = nil, nil
		got := scrubSupplied(redactUnaccountedBody(markBareJSONLiterals(
			redactUnaccountedJSONValues(redactMintedBody(c.body, assignmentTopLevel), assignmentTopLevel, "HTTP/1.1 200 OK"), assignmentTopLevel)))
		clean := strings.NewReplacer(capturedMark, "", genMark, "").Replace(got)
		if printed := strings.Contains(clean, "kill_switch"); printed != c.vouched {
			t.Errorf("%s: printed=%v, want %v: %q", c.body, printed, c.vouched, clean)
		}
		suppliedValues = nil
		structuralSurfaces, accountedSurfaces = nil, nil
	}
}

// A successful first decode proves one value, not the whole prefix.
//
// ⚠ TRAILING DATA VIOLATES THE SINGLE-DOCUMENT SHAPE WHETHER OR NOT ANYTHING WAS
// TRUNCATED. `{}` in `{}server-secret-token` decodes cleanly and leaves endpoint
// text behind, so the excuse removed a refusal the trailing bytes had earned
// (shardpilot/shardpilot-go#85 review) — the same sentence `markBareJSONLiterals`
// already applies to a value STREAM, one pass along.
func TestTrailingDataIsNotExcusedByTruncation(t *testing.T) {
	for _, c := range []struct {
		body    string
		excuses bool
	}{
		{`{"a":1}`, true},
		{`{"a":1}   `, true},
		{`{"a":1`, true},
		{`{}server-secret-token`, false},
		{`{} {"b":2}`, false},
		{`"abc" trailing`, false},
	} {
		if got := truncationCausedTheFailure([]byte(c.body)); got != c.excuses {
			t.Errorf("%q: truncation explains the failure=%v, want %v", c.body, got, c.excuses)
		}
	}
}

// The error envelope is read at 400 and 403, not at every 4xx.
//
// ⚠ FIFTH AXIS THIS REGISTRY HAS BEEN WRONG ABOUT: depth, membership, shape,
// status, and now WHICH statuses. `applyExperimentAssignment` calls
// `experimentBodyErrorText` only for the subject-grammar sentinel at 400 and the
// real-subjects sentinel at 403; every other status is classified by the status
// alone (shardpilot/shardpilot-go#85 review).
func TestErrorExemptionsApplyOnlyWhereTheEnvelopeIsRead(t *testing.T) {
	for _, c := range []struct {
		head    string
		exempts bool
	}{
		{"HTTP/1.1 400 Bad Request", true},
		{"HTTP/1.1 403 Forbidden", true},
		{"HTTP/1.1 401 Unauthorized", false},
		{"HTTP/1.1 404 Not Found", false},
		{"HTTP/1.1 500 Internal Server Error", false},
	} {
		suppliedValues = []string{"error"}
		structuralSurfaces, accountedSurfaces = nil, nil
		got := stripMarks(scrubSupplied(dropFraming(c.head + "\r\n\r\n" + `{"error":"anything"}`)))
		if printed := strings.Contains(got, `"error"`); printed != c.exempts {
			t.Errorf("%s: the member name printed=%v, want %v: %q", c.head, printed, c.exempts, got)
		}
		suppliedValues = nil
		structuralSurfaces, accountedSurfaces = nil, nil
	}
}

// TestUnicodeSpaceIsNotJSONWhitespaceAtTheRoot is the THIRD site of the question
// the other two fixes in this round answer, and the only one no reviewer pointed
// at: `markBareJSONLiterals` still asks it with `strings.TrimSpace`, which eats
// Unicode's whole space class. MEASURED, NOT ASSUMED, AND THE MEASUREMENT SAID NO:
// deleting that early refusal republishes the STREAM case (`{"x":1} false` -- and
// TestATrailingJSONValueIsNotGrammar kills that mutant) while U+00A0 stays
// redacted, because the refusal for it is `encoding/json`'s own and not this
// program's. So the production spelling is left alone -- there is no defect here
// to fix -- and this scene exists to keep it that way: it fails the day a tolerant
// pre-filter becomes the answer at the root (shardpilot/shardpilot-go#85 review).
func TestUnicodeSpaceIsNotJSONWhitespaceAtTheRoot(t *testing.T) {
	suppliedValues = []string{"false"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(markBareJSONLiterals(`{"assigned":false}`+"\u00a0", assignmentTopLevel)))
	if strings.Contains(got, ":false") {
		t.Fatalf("a body encoding/json rejects was published as grammar: %q", got)
	}
	// ⚠ AND JSON'S OWN FOUR BYTES ARE STILL ADMITTED, or the rule has become a
	// refusal to mark anything with trailing space at all.
	got = stripMarks(scrubSupplied(markBareJSONLiterals(`{"assigned":false}`+" \t\r\n", assignmentTopLevel)))
	if !strings.Contains(got, `"assigned":false`) {
		t.Fatalf("a document followed by JSON whitespace was refused: %q", got)
	}
}

// TestAUnicodeSpaceDoesNotMakeABodyParse is the other half of the whitespace
// substitution, and the half that had no scene: `jsonParses` gates
// `redactUnaccountedBody`, which passes an endpoint body through UNREFUSED when it
// believes the body is one JSON document. With `TrimSpace` there, `{"x":1}`
// followed by U+00A0 was believed -- so endpoint text in a shape this build does
// not cover was carried into the artifact with an EMPTY refusal ledger, while
// `encoding/json` rejects that body (shardpilot/shardpilot-go#85 review).
//
// ⚠ THE OBSERVABLE IS THE LEDGER, NOT THE STRING. This function refuses without
// rewriting -- both paths return the body unchanged -- so a scene that compares
// the returned text cannot tell the two apart, and the first draft of this one
// could not.
func TestAUnicodeSpaceDoesNotMakeABodyParse(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })

	structuralSurfaces = nil
	redactUnaccountedBody(`{"x":1}` + "\u00a0")
	if len(structuralSurfaces) == 0 {
		t.Fatalf("a body encoding/json rejects was accepted as a document, ledger empty")
	}
	// ⚠ AND JSON'S OWN FOUR BYTES STILL MAKE A DOCUMENT, or the rule has become a
	// refusal of every body with trailing space.
	structuralSurfaces = nil
	redactUnaccountedBody(`{"x":1}` + " \t\r\n")
	if len(structuralSurfaces) != 0 {
		t.Fatalf("a document followed by JSON whitespace was refused: %v", structuralSurfaces)
	}
}

// TestAVouchNeedsTheWholeWireShape: the vouch claims the SDK would call this body
// its own verdict, and the SDK makes that claim with `json.Unmarshal` into
// `expAssignmentWire` -- whose TYPED members are part of the answer.
// `variant_payload` must be an object, so `{"assigned":false,"variant_payload":1,
// "reason":"kill_switch"}` is rejected outright; the reduced mirror carrying only
// the members this pass reads decoded it happily and vouched `reason`, and a
// supplied `kill_switch` was skipped by both the scrub and the guard and published
// with an empty ledger (shardpilot/shardpilot-go#85 review).
func TestAVouchNeedsTheWholeWireShape(t *testing.T) {
	suppliedValues = []string{"kill_switch"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false,\"variant_payload\":1,\"reason\":\"kill_switch\"}")))
	if strings.Contains(got, "kill_switch") {
		t.Fatalf("a body the SDK rejects vouched its `reason`: %q", got)
	}
	// ⚠ AND A BODY THE SDK ACCEPTS STILL VOUCHES, or the repair is a refusal to
	// vouch anything that carries a payload at all.
	got = stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false,\"variant_payload\":{\"a\":1},\"reason\":\"kill_switch\"}")))
	if !strings.Contains(got, "kill_switch") {
		t.Fatalf("the SDK's own classification was lengthened in a body it accepts: %q", got)
	}
}

// TestTheMirroredWireShapeMatchesTheSDKs is the drift guard the mirror needs. A
// copy of someone else's grammar has as many edges as that grammar has versions,
// and five rounds have narrowed this predicate one member at a time. Both structs
// are read out of the source: the day the SDK gains a member, or retypes one, this
// fails instead of the vouch quietly answering a weaker question
// (shardpilot/shardpilot-go#85 review).
func TestTheMirroredWireShapeMatchesTheSDKs(t *testing.T) {
	fset := token.NewFileSet()
	members := func(path, name string) map[string]string {
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("the source is the oracle and %s could not be read: %v", path, err)
		}
		out, found := map[string]string{}, false
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || ts.Name.Name != name {
				return true
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				return true
			}
			found = true
			for _, fld := range st.Fields.List {
				if fld.Tag == nil {
					continue
				}
				tag := reflect.StructTag(strings.Trim(fld.Tag.Value, "`")).Get("json")
				var b strings.Builder
				if err := printer.Fprint(&b, fset, fld.Type); err != nil {
					t.Fatalf("the member type of %q could not be rendered: %v", tag, err)
				}
				out[tag] = b.String()
			}
			return false
		})
		if !found {
			t.Fatalf("the type %s is not in %s -- the derivation, not the shape, is what broke", name, path)
		}
		return out
	}
	sdk := members("../../experiments.go", "expAssignmentWire")
	mine := members("redact.go", "sdkAssignmentWire")
	if len(sdk) == 0 {
		t.Fatalf("the oracle came back empty, so agreeing with it proves nothing")
	}
	for tag, typ := range sdk {
		got, ok := mine[tag]
		if !ok {
			t.Errorf("the SDK member %q is absent from the mirror: the vouch answers a weaker question than the SDK asks", tag)
			continue
		}
		if got != typ {
			t.Errorf("member %q is %s in the SDK and %s in the mirror; the typing IS the grammar", tag, typ, got)
		}
	}
	for tag := range mine {
		if _, ok := sdk[tag]; !ok {
			t.Errorf("the mirror carries %q, which the SDK shape does not: it would refuse bodies the SDK accepts", tag)
		}
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
	// ⚠ THE BOUND IS RE-DERIVED FROM A MEASUREMENT, NOT INHERITED. 120 was chosen
	// when this collection allocated 56 MiB; the short producer now answers a second
	// question about the same tokens -- the binary decode of a token whose text
	// decode is not UTF-8 -- and every one of these 300000 tokens has one. Measured
	// on this toolchain: 104 MiB with the cap applied, and 1521 MiB with the cap
	// raised out of reach -- so what this scene separates is not a narrow band.
	//
	// ⚠ AND THE MARGIN IS FOR THE TOOLCHAIN, WHICH IS ALSO MEASURED. The same code
	// allocated 117 MiB here and 124 in CI on Go 1.25 -- so this scene was green
	// locally and red there, on a bound with 3 MiB of headroom
	// (shardpilot/shardpilot-go#84 CI). A bound one toolchain passes is not a
	// statement about the toolchains this repository builds on; the spread is about
	// 6%, and what this scene must separate is 104 from 234.
	if got > 160 {
		t.Errorf("collecting the seeds allocated %d MiB; with the cap applied while "+
			"collecting it is 104 here and 1521 with the cap raised out of reach", got)
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
	// ⚠ AND A FIELD THAT CARRIES VALUE BYTES IS STILL PROTECTED. The half this
	// scene comes from WITHHELD such a field, having no renderer for it; this half
	// RENDERS it -- measured, `/cb?state=x` is published as
	// `/redacted-2-chars?redacted-5-chars=redacted-1-chars` and recorded on the
	// ACCOUNTING ledger as "a redirect target". Both keep the same property: no
	// byte the endpoint chose appears in the artifact. Asserting the REFUSAL
	// asserted the other half's mechanism, which this one replaces; the bytes are
	// what the scene is about.
	structuralSurfaces, accountedSurfaces = nil, nil
	got := stripMarks(dropFraming("HTTP/1.1 302 Found\r\nLocation: /cb?state=x\r\n\r\n"))
	for _, endpointByte := range []string{"/cb", "state", "=x"} {
		if strings.Contains(got, endpointByte) {
			t.Fatalf("a Location's endpoint-minted %q was published: %q", endpointByte, got)
		}
	}
	if len(structuralSurfaces) == 0 && len(accountedSurfaces) == 0 {
		t.Fatalf("a Location carrying endpoint-minted bytes reached neither ledger: %q", got)
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

// TestExemptionsFollowTheSDKsOwnGates: the exemption registry says "these member
// names are the SCHEMA's, not the endpoint's" — a claim that is only true when the
// SDK would actually parse the body under that schema. It asks by STATUS and by
// SIZE, and this selection asked by neither exactly: every status below 400 took
// the assignment set, so `201 Created` with `{"assigned":"x"}` marked an
// endpoint-controlled name as generated and a supplied `assigned` was published;
// and an over-limit body took it too, though `parseExperimentVerdict` refuses
// before decoding (shardpilot/shardpilot-go#84 and #85 reviews).
func TestExemptionsFollowTheSDKsOwnGates(t *testing.T) {
	t.Cleanup(func() { suppliedValues = nil })
	for _, c := range []struct {
		name, dump string
		exempt     bool
	}{
		{"200 is the assignment shape", "HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false}", true},
		{"201 is not", "HTTP/1.1 201 Created\r\n\r\n{\"assigned\":\"x\"}", false},
		{"304 is not", "HTTP/1.1 304 Not Modified\r\n\r\n{\"assigned\":\"x\"}", false},
		{"an unreadable code is not", "HTTP/1.1 wat\r\n\r\n{\"assigned\":\"x\"}", false},
		// ⚠ AND A FIRST LINE THAT IS NOT A STATUS LINE AT ALL reaches a DIFFERENT
		// branch: the first version of this table had only the row above, and the
		// mutant that made an unparsable head take the assignment set survived it.
		{"a head with no version is not", "garbage\r\n\r\n{\"assigned\":\"x\"}", false},
		{"an empty head is not", "\r\n\r\n{\"assigned\":\"x\"}", false},
	} {
		suppliedValues = []string{"assigned"}
		got := stripMarks(scrubSupplied(dropFraming(c.dump)))
		if strings.Contains(got, "assigned") != c.exempt {
			t.Errorf("%s: exempt=%v, want %v: %q", c.name, !c.exempt, c.exempt, got)
		}
	}
	// ⚠ AND THE SIZE GATE, WHICH THE STATUS CANNOT SEE. A body one byte past the
	// SDK's ceiling is never parsed as an assignment, so its member names are the
	// endpoint's whatever the status says.
	suppliedValues = []string{"assigned"}
	over := `{"assigned":false}` + strings.Repeat(" ", sdkMaxBodyBytes)
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + over))); strings.Contains(got, "assigned") {
		t.Errorf("an over-limit body kept the assignment exemptions: %q", got[:120])
	}
	// ⚠ AND THE LAST ACCEPTED SIZE IS STILL EXEMPT. The separator line is framing,
	// not payload, and measuring it made the two largest legal bodies lose their
	// exemptions (shardpilot/shardpilot-go#84 review).
	suppliedValues = []string{"assigned"}
	head := `{"assigned":false}`
	exact := head + strings.Repeat(" ", sdkMaxBodyBytes-len(head))
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + exact))); !strings.Contains(got, "assigned") {
		t.Errorf("a body at exactly the SDK's ceiling lost its exemptions: %q", got[:120])
	}
	// ⚠ AND A BODY THE SDK *WOULD* PARSE IS STILL EXEMPT, or the gate has become a
	// refusal to exempt anything with a body worth reading.
	under := `{"assigned":false}` + strings.Repeat(" ", 4096)
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + under))); !strings.Contains(got, "assigned") {
		t.Errorf("a body well within the SDK's ceiling lost its exemptions: %q", got[:120])
	}
}

// TestTheMirroredBodyLimitMatchesTheSDKs is the drift guard for the one number
// this file mirrors. The SDK's constant is read out of the source and evaluated by
// `go/constant`, and the relation between the two ceilings — the recorder's is one
// byte above the SDK's, deliberately — is asserted rather than remembered.
func TestTheMirroredBodyLimitMatchesTheSDKs(t *testing.T) {
	fset := token.NewFileSet()
	var eval func(ast.Expr) (int64, bool)
	eval = func(e ast.Expr) (int64, bool) {
		switch v := e.(type) {
		case *ast.BasicLit:
			n, ok := constant.Int64Val(constant.MakeFromLiteral(v.Value, v.Kind, 0))
			return n, ok
		case *ast.ParenExpr:
			return eval(v.X)
		case *ast.BinaryExpr:
			x, okx := eval(v.X)
			y, oky := eval(v.Y)
			if !okx || !oky {
				return 0, false
			}
			switch v.Op {
			case token.SHL:
				return x << uint(y), true
			case token.ADD:
				return x + y, true
			case token.SUB:
				return x - y, true
			case token.MUL:
				return x * y, true
			}
		}
		return 0, false
	}
	spec := func(path, name string) ast.Expr {
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("the SDK source is the oracle and %s could not be read: %v", path, err)
		}
		var out ast.Expr
		ast.Inspect(f, func(n ast.Node) bool {
			vs, ok := n.(*ast.ValueSpec)
			if !ok || len(vs.Names) != 1 || vs.Names[0].Name != name || len(vs.Values) != 1 {
				return true
			}
			out = vs.Values[0]
			return false
		})
		if out == nil {
			t.Fatalf("%s is not declared in %s -- the derivation, not the value, is what broke", name, path)
		}
		return out
	}
	rc, ok := eval(spec("../../remote_config.go", "rcMaxBodyBytes"))
	if !ok {
		t.Fatalf("rcMaxBodyBytes is no longer an expression this scene can evaluate; it must not pass by default")
	}
	if rc != sdkMaxBodyBytes {
		t.Errorf("the SDK reads at most %d bytes and this file mirrors %d", rc, sdkMaxBodyBytes)
	}
	if id, isIdent := spec("../../experiments.go", "expMaxBodyBytes").(*ast.Ident); !isIdent || id.Name != "rcMaxBodyBytes" {
		t.Errorf("expMaxBodyBytes no longer aliases rcMaxBodyBytes, so the mirrored number answers for the wrong gate")
	}
	if capturedBodyMax != sdkMaxBodyBytes+1 {
		t.Errorf("the recorder's ceiling is %d and the SDK's is %d; a capture AT the ceiling is only indeterminate while they differ by one",
			capturedBodyMax, sdkMaxBodyBytes)
	}
}

// TestAWhitespaceOnlyBodyIsNotEmpty: a body of a single space — or of U+00A0 — is
// neither empty nor a JSON document, and `jsonParses` rejects both; `TrimSpace`
// made each look empty, so the structural refusal was skipped and endpoint bytes
// were published outside the four documented capture forms with an empty ledger
// (shardpilot/shardpilot-go#85 review).
func TestAWhitespaceOnlyBodyIsNotEmpty(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })
	for _, c := range []struct {
		name, body string
		refused    bool
	}{
		{"a single space", " ", true},
		{"a non-breaking space", " ", true},
		{"genuinely empty", "", false},
		// ⚠ CR AND LF STILL COUNT AS EMPTY: a body of only line breaks cannot be told
		// apart from the framing this dump adds, and that limit is the framing's.
		{"framing only", "\r\n", false},
	} {
		structuralSurfaces = nil
		redactUnaccountedBody(c.body)
		if (len(structuralSurfaces) != 0) != c.refused {
			t.Errorf("%s: refused=%v, want %v", c.name, len(structuralSurfaces) != 0, c.refused)
		}
	}
}

// TestAScanProducesEverySpellingTheGuardReconstructs is one scene for one class:
// a scan that looks for a forbidden shape has to produce the forms the publication
// guard will reconstruct, or it answers about a smaller world than the guard
// searches — and the difference is exactly what an endpoint spells its payload in.
// The transport diagnostic ran two decoders while the guard runs six, and the
// malformed-body member scan ran none (shardpilot/shardpilot-go#84 review).
func TestAScanProducesEverySpellingTheGuardReconstructs(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })
	for _, c := range []struct{ name, text string }{
		{"arrived", `malformed HTTP response "Set-Cookie: session=secret"`},
		{"percent", `malformed HTTP response "%53et-Cookie: session=secret"`},
		{"base64", `malformed HTTP response "U2V0LUNvb2tpZTogc2Vzc2lvbj1zZWNyZXQ="`},
		// ⚠ AND TWO HOPS, because an endpoint chooses how many stages to use. The first
		// version of the producer applied each decoder ONCE, so a value behind two
		// already-supported stages reached only its middle spelling and neither scan
		// recorded anything (shardpilot/shardpilot-go#84 review).
		{"base64 of base64", `malformed HTTP response "VTJWMExVTnZiMnRwWlRvZ2MyVnpjMmx2YmoxelpXTnlaWFE9"`},
		{"double percent", `malformed HTTP response "%2553et-Cookie: session=secret"`},
	} {
		structuralSurfaces = nil
		noteStructuralInText(c.text)
		if len(structuralSurfaces) == 0 {
			t.Errorf("a server-minted field spelled in %s was not refused", c.name)
		}
	}
	// ⚠ AND A FORM LIST THAT COULD NOT BE FINISHED IS A REFUSAL, not a clean scan.
	// A truncated enumeration is a scan that stopped early and reported nothing, which
	// is the failure mode this file keeps naming.
	//
	// ⚠ ASSERTED ON THE REASON, NOT ON "SOMETHING WAS RECORDED". The work budget
	// refuses the same input from the other side, so a scene that only counts the
	// ledger passes with this refusal removed -- the mutant said so.
	structuralSurfaces = nil
	noteStructuralInText(`malformed HTTP response "` + strings.Repeat("%41", 90000) + `"`)
	var sawEnumeration bool
	for _, r := range structuralSurfaces {
		if strings.Contains(r, "decoded forms could not be enumerated") {
			sawEnumeration = true
		}
	}
	if !sawEnumeration {
		t.Errorf("a diagnostic whose forms could not be enumerated was not refused for that: %v", structuralSurfaces)
	}
	// ⚠ AND AN ORDINARY DIAGNOSTIC IS STILL NOT REFUSED, or scanning wider has
	// become refusing everything.
	structuralSurfaces = nil
	noteStructuralInText(`malformed HTTP response "X-Trace: 1"`)
	if len(structuralSurfaces) != 0 {
		t.Errorf("a diagnostic naming no minted field was refused: %v", structuralSurfaces)
	}

	// The same rule on the body path: a malformed body that percent-encodes the
	// member. The guard's own decoder reconstructs it, so this scan must produce it.
	for _, spelling := range []string{
		`%22subject_fact_key%22:%22sfk_secret%22`,
		`%2522subject_fact_key%2522:%2522sfk_secret%2522`,
		// ⚠ AND A FORM ONLY A PRODUCER REACHES. `undoBase64` leaves a token whose
		// decode is not valid UTF-8 exactly as it found it, so this spelling was in no
		// form the walk produced while `assertNoLeak` builds that very candidate
		// downstream and checks it only against SUPPLIED values
		// (shardpilot/shardpilot-go#84 review).
		`/yJzdWJqZWN0X2ZhY3Rfa2V5Ijoic2ZrX3NlY3JldCI=`,
	} {
		structuralSurfaces = nil
		noteMinted(spelling)
		if len(structuralSurfaces) == 0 {
			t.Errorf("an encoded minted member in an unparsable body was not refused: %s", spelling)
		}
	}
	structuralSurfaces = nil
	noteMinted(`{"ordinary":"value"}`)
	if len(structuralSurfaces) != 0 {
		t.Errorf("an ordinary body was refused: %v", structuralSurfaces)
	}
}

// TestASynthesisedInterimStatusLineIsOurs: `Got1xxResponse` hands this program a
// code and headers, never a status line — the version, the code and the reason
// phrase are all read out of `net/http`'s own table. Leaving that line CAPTURED
// made `dataOf` treat the reason phrase as endpoint data, so a legal supplied
// value equal to a standard phrase — experiment key `Hints` against a
// `103 Early Hints` — refused an otherwise safe capture
// (shardpilot/shardpilot-go#84 review).
func TestASynthesisedInterimStatusLineIsOurs(t *testing.T) {
	suppliedValues = []string{"Hints"}
	t.Cleanup(func() { suppliedValues = nil })
	inner := &traceFiringTransport{codes: []int{103}, resp: &http.Response{
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
	var report strings.Builder
	renderExchanges(&report, got)
	if err := assertNoLeak(report.String()); err != nil {
		t.Fatalf("a reason phrase this program wrote was read as endpoint data: %v", err)
	}
	// ⚠ AND THE CODE IS STILL PRINTED, or the repair is a line nobody can read.
	// The PHRASE is separately replaced by the supplied-value scrub, which runs on
	// `ex.infos` before this line is marked -- a different consequence of the same
	// confusion, and not one this scene claims to have fixed.
	if !strings.Contains(stripMarks(report.String()), "103 Early") {
		t.Fatalf("the interim status line was lost: %q", stripMarks(report.String()))
	}
}

// TestAShortBinarySuffixIsRetained: fixing the standalone short token left the
// same token unmeasured one position along. `decodeBase64` keeps only a valid-UTF-8
// decode and `binaryCandidates` starts at four bytes, so `/2E` — 0xff 0x61 — was
// retained when it stood alone and lost when it followed a separator
// (shardpilot/shardpilot-go#84 review).
func TestAShortBinarySuffixIsRetained(t *testing.T) {
	suppliedValues = []string{"a"}
	t.Cleanup(func() { suppliedValues = nil; decodeWork = 0 })
	decodeWork = 0
	if err := assertNoLeak(asCaptured("zz//2E")); err == nil {
		t.Fatalf("a supplied value reachable through a short base64 SUFFIX was approved")
	}
	// ⚠ AND ORDINARY PROSE IS STILL APPROVED, or the producer has become a refusal.
	decodeWork = 0
	suppliedValues = []string{"nothingmatchesthis"}
	if err := assertNoLeak(asCaptured("zz//2E")); err != nil {
		t.Fatalf("an unrelated capture was refused: %v", err)
	}
}

// TestAProducerRunsBeforeAStageDestroysTheForm: a destructive stage can remove what
// a later producer would have found. With `dada` supplied, `/zY0NjE2NDYx` decodes
// to a binary seed the hex producer reconstructs from — and `undoBase64` rewrites
// that run before the producers ever see it, so the value was approved
// (shardpilot/shardpilot-go#84 review).
func TestAProducerRunsBeforeAStageDestroysTheForm(t *testing.T) {
	suppliedValues = []string{"dada"}
	t.Cleanup(func() { suppliedValues = nil; decodeWork = 0 })
	decodeWork = 0
	if err := assertNoLeak(asCaptured("/zY0NjE2NDYx")); err == nil {
		t.Fatalf("a value reachable only from an intermediate form was approved")
	}
}

// TestTheSeedCapBoundsTheAppend: one seed can carry thousands of short encoded
// tokens, so a single round appended tens of thousands of entries and the loop
// processed every one before the check at the top refused — a limit tested after
// the overshoot is a report, not a limit (shardpilot/shardpilot-go#84 review).
func TestTheSeedCapBoundsTheAppend(t *testing.T) {
	suppliedValues = []string{"nothingmatches"}
	t.Cleanup(func() { suppliedValues = nil; decodeWork = 0 })
	decodeWork = 0
	// ⚠ ASSERTED ON THE NUMBER THE REFUSAL REPORTS. Allocations do not discriminate
	// here -- the work budget bounds the same population from the other side, and the
	// mutant that removes the cap from the collector survived an allocation bound.
	// What differs is how far past the cap the worklist ran.
	// ⚠ THE EXPANSION, NOT THE INITIAL LIST. A capture that already carries 40000
	// short tokens is refused by the check BEFORE the loop -- a different guard, and
	// one that was never in question. The finding is about ONE seed that DECODES into
	// thousands, so the tokens are hidden inside a single base64 run.
	wide := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("61 ", 20000)))
	err := assertNoLeak(asCaptured(wide))
	if err == nil {
		t.Fatalf("a worklist that cannot settle was approved")
	}
	if !strings.Contains(err.Error(), "reached 4096 seeds") {
		t.Errorf("the worklist ran past its cap before refusing: %v", err)
	}
}

// TestAContentCodingIsAList: `Content-Encoding: identity, identity` is the same
// response as two separate `identity` lines, which this loop already accepts — so
// the classification depended only on how an intermediary chose to combine the
// fields, and the combined spelling withheld a readable capture with exit 4
// (shardpilot/shardpilot-go#84 review).
func TestAContentCodingIsAList(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })
	for _, c := range []struct {
		name, value string
		refused     bool
	}{
		{"one identity", "identity", false},
		{"a list of identity", "identity, identity", false},
		{"folded case is still nothing applied", "IDENTITY, identity", false},
		{"a real coding in the list", "identity, gzip", true},
		{"an empty element is malformed", "identity, ", true},
		{"a real coding alone", "gzip", true},
	} {
		structuralSurfaces = nil
		dropFraming("HTTP/1.1 200 OK\r\nContent-Encoding: " + c.value + "\r\n\r\n{\"assigned\":false}")
		if (len(structuralSurfaces) != 0) != c.refused {
			t.Errorf("%s: refused=%v, want %v (%v)", c.name, len(structuralSurfaces) != 0, c.refused, structuralSurfaces)
		}
	}
}

// TestTheOffRouteClaimIsVerdictDependent reads the SOURCE, because the sentence is
// assembled in `main()` where no fixture can run it. What it pins is that the claim
// is no longer unconditional: applying an assignment ARMS an `experiment_exposure`
// that `Close` flushes through this same transport, so on the served path at least
// one off-route request is the SDK working — and a line that calls that unexpected
// teaches an operator to ignore it (shardpilot/shardpilot-go#84 review).
//
// ⚠ A SOURCE-READING SCENE IS WEAKER THAN A RUN, and this one says so. It cannot
// see what the report prints; it can only see that the claim is conditioned on the
// verdict at all. The alternative — extracting the whole report assembly — is a
// larger change than this finding asked for, and the limit is recorded here rather
// than left to look like coverage.
func TestTheOffRouteClaimIsVerdictDependent(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("the scene cannot read its own subject: %v", err)
	}
	text := string(src)
	if strings.Contains(text, `"The ingest leg shares this transport; zero is the expected answer`) {
		t.Errorf("the off-route claim is still unconditional")
	}
	// ⚠ THE CONDITION, NOT THE WORDS. A first version checked that `result.Assigned`
	// appears in the file at all -- it appears in the verdict switch too, so replacing
	// the condition here with `false` left the scene green.
	// ⚠ RETARGETED, BECAUSE THE CLAIM THIS PINNED WAS ALSO WRONG. I replaced "zero is
	// always expected" with "the served path expects one" -- and the exposure never
	// reaches the transport at all, because this harness sets no `AnonymousID`. Two
	// false claims in a row, in opposite directions, both written without measuring
	// the path (shardpilot/shardpilot-go#84 review). What the line must carry now is
	// the REASON, so an operator can check it rather than trust it.
	if strings.Contains(text, "if result.Assigned {\n\t\toffRouteExpected") {
		t.Errorf("the expected count is still conditioned on the verdict")
	}
	// The COMPARISON must be unconditional too, not only the wording: a mutant that
	// re-conditions it on the verdict leaves both strings in place.
	if !strings.Contains(text, "offRouteAgrees := \"matches\"\n\tif offRoute != 0 {") {
		t.Errorf("the off-route comparison is conditioned on the verdict again")
	}
	if !strings.Contains(text, "exposure is skipped before it reaches the transport") {
		t.Errorf("the claim does not say WHY zero is expected, so an operator cannot check it")
	}
}

// TestTheSecondRoundOfSchemaGates covers the three preconditions the exemption
// registry has to carry, each of which arrived as its own finding: the body length
// must be the one the SDK READ (not the one this program's `escapeMarks` produced),
// and an incomplete read disqualifies a body whatever its length
// (shardpilot/shardpilot-go#84 review).
func TestTheSecondRoundOfSchemaGates(t *testing.T) {
	t.Cleanup(func() {
		suppliedValues = nil
		capturedIncomplete, capturedBodyBytes = false, -1
	})
	body := `{"assigned":false}`

	// ⚠ THE LENGTH THE SDK READ. `escapeMarks` expands a literal `\x00` spelling, so
	// a body at the ceiling arrives here longer than the SDK ever saw.
	suppliedValues, capturedIncomplete = []string{"assigned"}, false
	capturedBodyBytes = sdkMaxBodyBytes
	// The text this pass sees is LONGER than the ceiling; the captured length is not.
	// It stays one JSON document, so only the gate's choice of number decides.
	inflated := `{"assigned":false,"pad":"` + strings.Repeat("a", sdkMaxBodyBytes/2) + `"}`
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + inflated))); !strings.Contains(got, "assigned") {
		t.Errorf("a body the SDK accepted lost its exemptions to this program's own expansion")
	}
	// ...and a body the SDK really did refuse still loses them.
	capturedBodyBytes = sdkMaxBodyBytes + 1
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + body))); strings.Contains(got, "assigned") {
		t.Errorf("an over-limit body kept its exemptions: %q", got)
	}
	// ⚠ AND INCOMPLETENESS, WHICH IS NOT A LENGTH.
	capturedBodyBytes, capturedIncomplete = len(body), true
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + body))); strings.Contains(got, "assigned") {
		t.Errorf("an incomplete body kept its exemptions: %q", got)
	}
	// ⚠ AND A BODY THE SDK WOULD PARSE STILL KEEPS THEM.
	capturedIncomplete = false
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + body))); !strings.Contains(got, "assigned") {
		t.Errorf("a complete, in-limit body lost its exemptions: %q", got)
	}
}

// TestASynthesisedCacheControlIsAmbiguous: `http.ReadResponse` ADDS
// `Cache-Control: no-cache` when the response carries `Pragma: no-cache` and no
// cache directive of its own — measured — so with both present the field's
// provenance cannot be established (shardpilot/shardpilot-go#84 review).
func TestASynthesisedCacheControlIsAmbiguous(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })
	structuralSurfaces = nil
	dropFraming("HTTP/1.1 200 OK\r\nPragma: no-cache\r\nCache-Control: no-cache\r\n\r\n")
	if len(structuralSurfaces) == 0 {
		t.Errorf("a field that may be the parser's was treated as the endpoint's")
	}
	// ⚠ AND EITHER ALONE IS UNAMBIGUOUS, or the refusal covers ordinary responses.
	for _, dump := range []string{
		"HTTP/1.1 200 OK\r\nCache-Control: no-cache\r\n\r\n",
		"HTTP/1.1 200 OK\r\nPragma: no-cache\r\n\r\n",
		"HTTP/1.1 200 OK\r\nPragma: no-cache\r\nCache-Control: max-age=0\r\n\r\n",
	} {
		structuralSurfaces = nil
		dropFraming(dump)
		for _, r := range structuralSurfaces {
			if strings.Contains(r, "provenance this build cannot establish") {
				t.Errorf("an unambiguous response was refused: %q", dump)
			}
		}
	}
}

// TestAWholeLineFoldIsInEveryWalk: `wrappedBase64Candidates` deliberately SKIPS a
// line that is entirely base64, because `joinBase64Runs` is supposed to have joined
// it — and that normalisation ran on the outer text only. So a member wrapped across
// whole base64 lines was in no form either walk produced, though three ordinary
// decodes that ignore CR/LF reconstruct it (shardpilot/shardpilot-go#84 review).
// A producer that assumes another pass ran first is only correct where that pass runs.
func TestAWholeLineFoldIsInEveryWalk(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil; suppliedValues = nil; decodeWork = 0 })
	structuralSurfaces, decodeWork = nil, 0
	noteMinted("InN1\r\nYmplY3RfZmFjdF9rZXkiOiJzZmtfc2VjcmV0Ig==")
	if len(structuralSurfaces) == 0 {
		t.Errorf("a minted member wrapped across whole base64 lines was not refused")
	}
	// ...and the same fold inside a decoded SEED, which is the other walk.
	suppliedValues, decodeWork = []string{"secret99"}, 0
	if err := assertNoLeak(asCaptured("WXpK\r\nV2FnMEtZMjFXTUU5VWF6MD0=")); err == nil {
		t.Errorf("a supplied value behind a folded seed was approved")
	}
}

// TestTheStructuralWalkIsBoundedByWhatItWalks: a budget consulted after the
// expensive call is a report, not a limit. Measured: a 108 KB run costs about 240
// MiB across this walk — six decoders and a tokenising scan per form, sixty-four
// forms — and charging each producer's pass count afterwards changed that by 2 MiB
// (shardpilot/shardpilot-go#84 review). The bound is on what is WALKED, and an
// unfinished form list is a refusal rather than a silent clean scan.
func TestTheStructuralWalkIsBoundedByWhatItWalks(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil; decodeWork = 0 })
	structuralSurfaces, decodeWork = nil, 0
	big := strings.Repeat("YWJjZGVm/", 12000)
	got := allocatedMiB(func() { noteStructuralInText(big) })
	if got > 32 {
		t.Errorf("scanning a %d-byte run allocated %d MiB; bounded it is about 1 and "+
			"unbounded about 240", len(big), got)
	}
	var sawEnumeration bool
	for _, r := range structuralSurfaces {
		if strings.Contains(r, "decoded forms could not be enumerated") {
			sawEnumeration = true
		}
	}
	if !sawEnumeration {
		t.Errorf("a run too large to enumerate was scanned as clean: %v", structuralSurfaces)
	}
	// ⚠ AND AN ORDINARY DIAGNOSTIC IS STILL WALKED, or the bound has become a refusal
	// of everything: the two-hop base64 spelling must still be found.
	structuralSurfaces, decodeWork = nil, 0
	noteStructuralInText(`malformed HTTP response "VTJWMExVTnZiMnRwWlRvZ2MyVnpjMmx2YmoxelpXTnlaWFE9"`)
	if len(structuralSurfaces) == 0 {
		t.Errorf("a diagnostic within the bound was no longer scanned")
	}
}

// ⚠ AND A BODY THAT MENTIONS THOSE FIELDS IS NOT A HEADER BLOCK. My own repair
// scanned the whole dump, so an error page quoting `Pragma: no-cache` refused an
// otherwise fine capture (shardpilot/shardpilot-go#84 review).
func TestTheCacheProvenanceCheckStopsAtTheHeaderBlock(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })
	structuralSurfaces = nil
	dropFraming("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n" +
		"Pragma: no-cache\nCache-Control: no-cache\n")
	for _, r := range structuralSurfaces {
		if strings.Contains(r, "provenance this build cannot establish") {
			t.Fatalf("body prose was read as a header block: %v", structuralSurfaces)
		}
	}
	// ...and the real header pair is still refused.
	structuralSurfaces = nil
	dropFraming("HTTP/1.1 200 OK\r\nPragma: no-cache\r\nCache-Control: no-cache\r\n\r\n")
	if len(structuralSurfaces) == 0 {
		t.Fatalf("the ambiguous header pair was no longer refused")
	}
}

// TestExemptionsNeedTheAssignmentSHAPE: status and size say whether the SDK would
// LOOK at a body; the typed decode says whether it finds a verdict. A complete,
// under-limit 200 may be valid JSON that is not an assignment — `{"assigned":"x"}`
// is rejected as `malformed_response` — and exempting its member names marked a
// supplied identifier as generated (shardpilot/shardpilot-go#84 review).
func TestExemptionsNeedTheAssignmentSHAPE(t *testing.T) {
	t.Cleanup(func() { suppliedValues = nil })
	for _, c := range []struct {
		name, body string
		exempt     bool
	}{
		{"a verdict", `{"assigned":false}`, true},
		{"assigned of the wrong type", `{"assigned":"x"}`, false},
		{"no assigned member at all", `{"version":1}`, false},
		{"not an object", `["assigned"]`, false},
		{"a typed member the SDK validates", `{"assigned":false,"variant_payload":1}`, false},
		// ⚠ AND THE SEMANTIC GATES AFTER THE DECODE. `parseExperimentVerdict` keeps
		// validating: on the assigned branch a version of at least 1, non-empty
		// assignment and variant keys, and an assignment unit from a closed set; on the
		// unassigned branch a reason from a closed set
		// (shardpilot/shardpilot-go#84 review).
		{"assigned with nothing else", `{"assigned":true}`, false},
		// Each gate gets a row that isolates IT: a table where one row trips several
		// gates cannot tell which one is load-bearing, and the mutant for the version
		// check survived until this row existed.
		{"assigned without a version",
			`{"assigned":true,"assignment_key":"a","variant_key":"v","boundary":{"assignment_unit":"client_id"}}`, false},
		{"assigned with version 0",
			`{"assigned":true,"version":0,"assignment_key":"a","variant_key":"v","boundary":{"assignment_unit":"client_id"}}`, false},
		{"assigned without a variant key",
			`{"assigned":true,"version":1,"assignment_key":"a","boundary":{"assignment_unit":"client_id"}}`, false},
		{"assigned without an assignment unit",
			`{"assigned":true,"version":1,"assignment_key":"a","variant_key":"v"}`, false},
		{"assigned with an unknown unit",
			`{"assigned":true,"version":1,"assignment_key":"a","variant_key":"v","boundary":{"assignment_unit":"made_up"}}`, false},
		{"a complete assigned verdict",
			`{"assigned":true,"version":1,"assignment_key":"a","variant_key":"v","boundary":{"assignment_unit":"client_id"}}`, true},
		{"an unassigned reason outside the set",
			`{"assigned":false,"reason":"because"}`, false},
		{"an unassigned reason inside it",
			`{"assigned":false,"reason":"kill_switch"}`, true},
	} {
		suppliedValues = []string{"assigned"}
		got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + c.body)))
		if strings.Contains(got, "assigned") != c.exempt {
			t.Errorf("%s: exempt=%v, want %v: %q", c.name, !c.exempt, c.exempt, got)
		}
	}
}

// TestTheInterimLineKeepsItsMarks: escaping AFTER the marks turns the provenance
// bytes into literal `\x01` text — the line stops being recognised as generated, or
// as a status line at all (shardpilot/shardpilot-go#84 review).
func TestTheInterimLineKeepsItsMarks(t *testing.T) {
	suppliedValues = []string{"Hints"}
	t.Cleanup(func() { suppliedValues = nil })
	inner := &traceFiringTransport{codes: []int{103}, resp: &http.Response{
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
	var report strings.Builder
	renderExchanges(&report, got)
	out := report.String()
	if strings.Contains(out, `\x01HTTP/1.1 103`) {
		t.Fatalf("the provenance marks were escaped into literal text: %q", stripMarks(out))
	}
	if !strings.Contains(stripMarks(out), "103 Early Hints") {
		t.Fatalf("the reason phrase this program wrote was rewritten: %q", stripMarks(out))
	}
}

// TestTheCacheProvenanceMirrorsTheParsersCondition: `fixPragmaCacheControl`
// synthesises the directive only when the FIRST parsed `Pragma` value is exactly
// `no-cache`, lowercase. My substring test refused genuine pairs
// (shardpilot/shardpilot-go#84 review).
func TestTheCacheProvenanceMirrorsTheParsersCondition(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })
	for _, c := range []struct {
		name, head string
		refused    bool
	}{
		{"the parser's own condition", "Pragma: no-cache\r\nCache-Control: no-cache", true},
		{"a different pragma value", "Pragma: x-no-cache\r\nCache-Control: no-cache", false},
		{"no-cache not first", "Pragma: foo, no-cache\r\nCache-Control: no-cache", false},
		{"uppercase spelling", "Pragma: NO-CACHE\r\nCache-Control: no-cache", false},
		{"a real directive", "Pragma: no-cache\r\nCache-Control: max-age=0", false},
	} {
		structuralSurfaces = nil
		dropFraming("HTTP/1.1 200 OK\r\n" + c.head + "\r\n\r\n")
		var saw bool
		for _, r := range structuralSurfaces {
			if strings.Contains(r, "provenance this build cannot establish") {
				saw = true
			}
		}
		if saw != c.refused {
			t.Errorf("%s: refused=%v, want %v", c.name, saw, c.refused)
		}
	}
}

// TestTheAmbiguityNeedsTheAbsenceOfOtherDirectives: Go synthesises the cache
// directive only when the `Cache-Control` map key is ENTIRELY absent, so any other
// `Cache-Control` field in the block proves the parser wrote none of them
// (shardpilot/shardpilot-go#84 review).
func TestTheAmbiguityNeedsTheAbsenceOfOtherDirectives(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })
	for _, c := range []struct {
		name, head string
		refused    bool
	}{
		{"one directive, the parser's shape", "Pragma: no-cache\r\nCache-Control: no-cache", true},
		{"a second directive proves receipt", "Pragma: no-cache\r\nCache-Control: max-age=0\r\nCache-Control: no-cache", false},
	} {
		structuralSurfaces = nil
		dropFraming("HTTP/1.1 200 OK\r\n" + c.head + "\r\n\r\n")
		var saw bool
		for _, r := range structuralSurfaces {
			if strings.Contains(r, "provenance this build cannot establish") {
				saw = true
			}
		}
		if saw != c.refused {
			t.Errorf("%s: refused=%v, want %v", c.name, saw, c.refused)
		}
	}
}

// TestTheInterimSectionSaysWhatItCannotShow: `net/http` deletes `Connection` from
// an interim's headers before the callback, so the reconstruction omits a field the
// endpoint sent — the section's own prose has to say so rather than present the
// block as the interim's headers (shardpilot/shardpilot-go#84 review).
func TestTheInterimSectionSaysWhatItCannotShow(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("the scene cannot read its own subject: %v", err)
	}
	for _, want := range []string{"PARSER CONSUMED", "headers THE CALLBACK WAS GIVEN"} {
		if !strings.Contains(string(src), want) {
			t.Errorf("the interim section does not qualify its claim: %q missing", want)
		}
	}
}

// TestTheLastRoundOfExemptionGatesAndSpans covers the four narrow findings of this
// round together, each with the negative half (shardpilot/shardpilot-go#84 review).
func TestTheLastRoundOfExemptionGatesAndSpans(t *testing.T) {
	t.Cleanup(func() {
		suppliedValues = nil
		requestedAppKey, requestedEnvKey, requestedExpKey = "", "", ""
		structuralSurfaces = nil
	})
	// ⚠ AN ECHO IS COMPARED WITH THE REQUEST'S OWN VALUE. A mismatch, or an explicit
	// null, unmarshals fine into a bare string and passed before.
	requestedExpKey = "exp1"
	for _, c := range []struct {
		name, body string
		exempt     bool
	}{
		{"the echo this request sent", `{"assigned":false,"experiment_key":"exp1"}`, true},
		{"a mismatched echo", `{"assigned":false,"experiment_key":"other"}`, false},
		{"an explicit null echo", `{"assigned":false,"experiment_key":null}`, false},
		{"an absent echo", `{"assigned":false}`, true},
	} {
		suppliedValues = []string{"assigned"}
		got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + c.body)))
		if strings.Contains(got, "assigned") != c.exempt {
			t.Errorf("%s: exempt=%v, want %v", c.name, !c.exempt, c.exempt)
		}
	}
	// ⚠ AND THE ERROR ENVELOPE IS TYPED TOO.
	for _, c := range []struct {
		name, body string
		exempt     bool
	}{
		{"a typed envelope", `{"error":"nope"}`, true},
		{"a boolean where the string goes", `{"error":false}`, false},
	} {
		suppliedValues = []string{"error"}
		got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 400 Bad Request\r\n\r\n" + c.body)))
		if strings.Contains(got, "error") != c.exempt {
			t.Errorf("%s: exempt=%v, want %v: %q", c.name, !c.exempt, c.exempt, got)
		}
	}
	// ⚠ A QUESTION MARK OUTSIDE A URL IS NOT A QUERY.
	suppliedValues = nil
	if got := sanitizeText(`malformed HTTP response "BOGUS?detail"`); strings.Contains(got, "query-withheld") {
		t.Errorf("a question mark outside a URL was rewritten: %q", got)
	}
	if got := sanitizeText(`Get "https://e.example/x?k=v": timeout`); !strings.Contains(got, "query-withheld") {
		t.Errorf("a real URL query survived: %q", got)
	}
	// ⚠ AND THE FIRST Pragma VALUE IS NOT SPLIT ON COMMAS.
	structuralSurfaces = nil
	dropFraming("HTTP/1.1 200 OK\r\nPragma: no-cache, extension\r\nCache-Control: no-cache\r\n\r\n")
	for _, r := range structuralSurfaces {
		if strings.Contains(r, "provenance this build cannot establish") {
			t.Errorf("a Pragma value the parser would not match forced a refusal")
		}
	}
}

// TestTheShapeIsCheckedOnCapturedBytes: `escapeMarks` runs before `dropFraming`, so
// an echo carrying a literal marker spelling reached the shape check with extra
// backslashes and the equality failed -- the exemptions were disabled and the scrub
// rewrote the schema member, producing altered JSON the marks make the guard
// approve. I had named that as a limit that "fails closed"; it does not
// (shardpilot/shardpilot-go#84 review).
func TestTheShapeIsCheckedOnCapturedBytes(t *testing.T) {
	t.Cleanup(func() {
		suppliedValues, capturedBodyRaw = nil, ""
		capturedBodyBytes = -1
		requestedAppKey = ""
	})
	// The LITERAL four-character spelling, which is what `escapeMarks` expands -- a
	// raw NUL is not legal inside a JSON string and would fail the decode for an
	// unrelated reason.
	mark := `\x00`
	body := `{"assigned":false,"app_key":"\\x00"}`
	requestedAppKey = mark
	suppliedValues = []string{"assigned"}
	capturedBodyRaw, capturedBodyBytes = body, len(body)
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + escapeMarks(body)))); !strings.Contains(got, "assigned") {
		t.Errorf("the schema member was rewritten because the check saw escaped bytes: %q", got)
	}
	// ⚠ AND A BODY THE SDK REJECTS STILL LOSES THEM, or the fix has become "always
	// exempt": the captured bytes are checked, not skipped.
	bad := `{"assigned":"x"}`
	capturedBodyRaw, capturedBodyBytes = bad, len(bad)
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + bad))); strings.Contains(got, "assigned") {
		t.Errorf("a body the SDK rejects kept its exemptions: %q", got)
	}
}

// TestAnEmptyMintedMemberIsNotConcealment: an explicitly empty
// `"subject_fact_key":""` is accepted by the SDK and conceals nothing -- the same
// defect as the empty `Location:` header one surface along
// (shardpilot/shardpilot-go#84 review).
func TestAnEmptyMintedMemberIsNotConcealment(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })
	for _, c := range []struct {
		name, body string
		refused    bool
	}{
		{"an empty minted member", `{"assigned":false,"subject_fact_key":""}`, false},
		{"one with bytes", `{"assigned":false,"subject_fact_key":"sfk1_x"}`, true},
		{"a null one", `{"assigned":false,"subject_fact_key":null}`, true},
	} {
		structuralSurfaces = nil
		noteMinted(c.body)
		if (len(structuralSurfaces) != 0) != c.refused {
			t.Errorf("%s: refused=%v, want %v", c.name, len(structuralSurfaces) != 0, c.refused)
		}
	}
}

// TestTheFinalSectionSaysWhatItCannotShow: a `Connection` option naming another
// header makes net/http remove BOTH, so the reconstruction omits a field the
// endpoint sent and the section's prose has to say so
// (shardpilot/shardpilot-go#84 review).
func TestTheFinalSectionSaysWhatItCannotShow(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("the scene cannot read its own subject: %v", err)
	}
	for _, want := range []string{"A FIELD THE PARSER CONSUMED IS NOT HERE", "header set THIS PROGRAM WAS GIVEN"} {
		if !strings.Contains(string(src), want) {
			t.Errorf("the response section does not qualify its claim: %q missing", want)
		}
	}
}

// TestAFieldNameIsScannedInEverySpelling: `%` is legal in an HTTP field name, so
// `%53et-Cookie:` passed a raw lookup while the publication guard's own percent
// decoder rebuilds `Set-Cookie:` from it -- and `assertNoLeak` checks only SUPPLIED
// values, so the endpoint-minted credential was published
// (shardpilot/shardpilot-go#84 review).
//
// ⚠ THIS IS THE THIRD SITE OF ONE DEFECT, and the population was greped this time:
// the transport diagnostic and the unparsable body were fixed in earlier rounds, the
// response header path is the one reported, and the TRAILER block had it too and was
// not reported. Both remaining sites are covered here.
func TestAFieldNameIsScannedInEverySpelling(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })
	for _, c := range []struct {
		name, dump string
		refused    bool
	}{
		{"the arrived spelling", "HTTP/1.1 200 OK\r\nSet-Cookie: session=secret\r\n\r\n", true},
		{"percent-encoded", "HTTP/1.1 200 OK\r\n%53et-Cookie: session=secret\r\n\r\n", true},
		{"doubly encoded", "HTTP/1.1 200 OK\r\n%2553et-Cookie: session=secret\r\n\r\n", true},
		{"an ordinary field", "HTTP/1.1 200 OK\r\nX-Trace: 1\r\n\r\n", false},
	} {
		// ⚠ EITHER LEDGER ANSWERS. A spelling this half can RENDER is recorded as
		// accounted rather than refused -- the arrived `Set-Cookie` is published as
		// `redacted-7-chars=redacted-6-chars`, not withheld. What the scene is about
		// is that every spelling is RECOGNISED, not which ledger it lands on.
		structuralSurfaces, accountedSurfaces = nil, nil
		dropFraming(c.dump)
		if (len(structuralSurfaces) != 0 || len(accountedSurfaces) != 0) != c.refused {
			t.Errorf("%s: refused=%v, want %v", c.name, len(structuralSurfaces) != 0, c.refused)
		}
	}
	// ⚠ AND THE TRAILER BLOCK, which had the same raw lookup and was not reported.
	structuralSurfaces = nil
	if note, minted, _ := mintedFieldIn("%53et-Cookie"); !minted || note == "" {
		t.Errorf("an encoded trailer field name was not recognised")
	}
	if _, minted, _ := mintedFieldIn("X-Trace"); minted {
		t.Errorf("an ordinary trailer field name was treated as minted")
	}
}

// TestTheWalkIsBoundedByCostNotLength: the suffix producers enumerate one candidate
// per SEPARATOR POSITION and decode each, so the work is about (separators x length)
// and not length. Measured: 8192 bytes of `/` cost 251 MiB while sitting exactly
// under an 8 KiB byte bound I had chosen one round earlier without measuring the
// worst case AT it (shardpilot/shardpilot-go#84 review) -- the same mistake as a
// threshold that outlived the subject it was computed from.
//
// The preflight is one linear pass over the form and allocates nothing.
func TestTheWalkIsBoundedByCostNotLength(t *testing.T) {
	worst := strings.Repeat("/", 8192)
	if got := allocatedMiB(func() { _, _ = decodedForms(worst) }); got > 8 {
		t.Errorf("a separator-dense form under the byte bound allocated %d MiB; bounded it is about 0 and unbounded about 251", got)
	}
	if forms, whole := decodedForms(worst); whole || len(forms) != 1 {
		t.Errorf("a form too expensive to walk was not reported as unfinished: forms=%d whole=%v", len(forms), whole)
	}
	// ⚠ AND ORDINARY INPUT IS STILL WALKED TO A FIXED POINT, or the preflight has
	// become a refusal of everything: both of these are scanned elsewhere in this file
	// and must keep producing their forms.
	for _, c := range []struct{ name, text string }{
		{"a base64 diagnostic", `malformed HTTP response "U2V0LUNvb2tpZTogc2Vzc2lvbj1zZWNyZXQ="`},
		{"a doubly percent-encoded member", `%2522subject_fact_key%2522:%2522sfk_secret%2522`},
	} {
		if forms, whole := decodedForms(c.text); !whole || len(forms) < 2 {
			t.Errorf("%s: forms=%d whole=%v -- the preflight rejected ordinary input", c.name, len(forms), whole)
		}
	}
}

// TestTheFourthRoundOfNamesAndSpans covers this round's four findings, three of
// which are consequences of repairs from the two rounds before it
// (shardpilot/shardpilot-go#84 review).
func TestTheFourthRoundOfNamesAndSpans(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces = nil, nil })

	// ⚠ ALLOWING AN EMPTY VALUE SAID NOTHING ABOUT THE NAME. Marking the line
	// generated is right about the bytes and wrong when a supplied value equals that
	// spelling: a generated span is skipped by both the scrub and the guard.
	suppliedValues, structuralSurfaces = []string{"Location"}, nil
	got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 302 Found\r\nLocation:\r\n\r\n")))
	if strings.Contains(got, "Location:") || len(structuralSurfaces) == 0 {
		t.Errorf("a supplied value equal to a protected field name was published: %q %v", got, structuralSurfaces)
	}
	// ...and an empty protected field that collides with nothing is still not refused.
	suppliedValues, structuralSurfaces = []string{"unrelated"}, nil
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 302 Found\r\nLocation:\r\n\r\n"))); len(structuralSurfaces) != 0 {
		t.Errorf("an ordinary empty protected field was refused: %q %v", got, structuralSurfaces)
	}

	// ⚠ A MINTED NAME IS ONLY GRAMMAR WHERE THE SDK READS ONE.
	suppliedValues, structuralSurfaces = []string{"subject_fact_key"}, nil
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 404 Not Found\r\n\r\n{\"subject_fact_key\":\"\"}"))); strings.Contains(got, "subject_fact_key") {
		t.Errorf("a minted member name was exempted on a non-assignment response: %q", got)
	}
	// ...and on a body the SDK accepts it is still grammar.
	suppliedValues = []string{"subject_fact_key"}
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false,\"subject_fact_key\":\"\"}"))); !strings.Contains(got, "subject_fact_key") {
		t.Errorf("a minted member name lost its exemption on an accepted verdict: %q", got)
	}

	// ⚠ A URL ENDS BEFORE THE PUNCTUATION THAT ENCLOSES IT.
	suppliedValues = nil
	if got := sanitizeText(`failed (https://e/p?q=s): detail`); !strings.Contains(got, "): detail") {
		t.Errorf("punctuation after a URL was swallowed into the query: %q", got)
	}
	if got := sanitizeText(`failed (https://e/p?q=s): detail`); strings.Contains(got, "q=s") {
		t.Errorf("the query itself survived: %q", got)
	}
}

// TestATrailerCodingIsStillACoding: `Trailer: Content-Encoding` with a final
// `Content-Encoding: gzip` is accepted by Go and leaves the body encoded, and the
// head-only check never sees it -- a gzip-compressed supplied value passed
// `assertNoLeak`, which has no gzip decoder, while this report carried the coding
// needed to rebuild it (shardpilot/shardpilot-go#84 review).
func TestATrailerCodingIsStillACoding(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })
	for _, c := range []struct {
		name, value string
		refused     bool
	}{
		{"an unsupported coding in a trailer", "gzip", true},
		{"identity in a trailer", "identity", false},
		{"a list of identity", "identity, identity", false},
	} {
		structuralSurfaces = nil
		// ⚠ AND THE FIXTURE CARRIES A BODY, because the refusal is about what an
		// undecodable body could HIDE. This half exempts a capture with no body bytes
		// -- an absent body is not the refusal's subject -- and a trailer arrives only
		// after one, so a fixture without bytes was asking the question in a shape the
		// protocol does not produce.
		tb := &teeBody{trailer: http.Header{"Content-Encoding": []string{c.value}}}
		tb.buf.WriteString("compressed-bytes")
		ex := &exchange{captured: tb}
		_ = ex.trailerReport()
		if (len(structuralSurfaces) != 0) != c.refused {
			t.Errorf("%s: refused=%v, want %v", c.name, len(structuralSurfaces) != 0, c.refused)
		}
	}
}

// TestTheSDKGateIsMeasuredOnTheCapturedBytes: the SDK refuses a body over
// `expMaxBodyBytes` BEFORE decoding it, so a reason phrase in an oversized body is
// the endpoint's word and not this program's grammar.
//
// ⚠ THE PASS THAT ASKS RUNS AFTER THE PASS THAT SHORTENS. `redactMintedBody`
// replaces a minted string with a placeholder, so a 1 MiB+1 response whose bulk is
// a minted `assignment_key` reached the gate under the limit, the gate read as
// satisfied, and a supplied `kill_switch` was vouched -- through both the scrub and
// the guard, with an empty refusal ledger (shardpilot/shardpilot-go#85 review).
// `capturedBodyBytes` and `capturedBodyRaw` are what the SDK read; the exemption
// registry was moved onto them a round earlier and this site was not.
func TestTheSDKGateIsMeasuredOnTheCapturedBytes(t *testing.T) {
	t.Cleanup(func() {
		suppliedValues, structuralSurfaces = nil, nil
		capturedBodyBytes, capturedBodyRaw = -1, ""
	})
	oversized := `{"assigned":false,"assignment_key":"` +
		strings.Repeat("k", sdkMaxBodyBytes) + `","reason":"kill_switch"}`
	suppliedValues = []string{"kill_switch"}
	capturedBodyBytes, capturedBodyRaw = len(oversized), oversized
	got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + oversized)))
	if strings.Contains(got, "kill_switch") {
		t.Errorf("a reason phrase in a body the SDK refuses before decoding was vouched: %q",
			got[len(got)-70:])
	}
	// ⚠ AND A BODY THE SDK DOES READ STILL VOUCHES ITS REASON, or the gate has
	// stopped answering about the SIZE and started refusing every verdict.
	small := `{"assigned":false,"reason":"kill_switch"}`
	capturedBodyBytes, capturedBodyRaw = len(small), small
	got = stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + small)))
	if !strings.Contains(got, "kill_switch") {
		t.Errorf("a reason phrase the SDK reads lost its exemption: %q", got)
	}
}

// TestASignedMaxAgeKeepsItsGrammar: `Max-Age=-1` is how a deletion cookie is
// spelled and `net/http` reads it as `MaxAge == -1`, so replacing it with a length
// costs the capture the attribute's meaning.
//
// ⚠ AND THE PREDICATE WAS WRONG IN BOTH DIRECTIONS. `isDigits` refused `-1`, which
// the parser accepts (shardpilot/shardpilot-go#85 review), and admitted `007`,
// which the parser leaves in `Unparsed` and does NOT read as a Max-Age -- so this
// program vouched endpoint-chosen bytes as grammar the specification fixes. The
// second half was not in the finding and is the same defect. Both sites that asked
// it now call the parser instead of restating its grammar.
func TestASignedMaxAgeKeepsItsGrammar(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces, accountedSurfaces = nil, nil })
	for _, c := range []struct {
		value string
		kept  bool
	}{
		{"-1", true}, {"0", true}, {"60", true}, {"+1", true},
		{"007", false}, {"1x", false}, {"", false},
	} {
		structuralSurfaces, accountedSurfaces = nil, nil
		got := stripMarks(redactSetCookie("Set-Cookie: sid=x; Max-Age=" + c.value))
		if kept := strings.Contains(got, "Max-Age="+c.value) && c.value != ""; kept != c.kept {
			t.Errorf("Max-Age=%q kept=%v, want %v (%q)", c.value, kept, c.kept, got)
		}
	}
}

// TestANonBracketedAuthorityIsMeasuredAgainstTheHostGrammar: the host exemption
// rests on "publicly resolvable and constrained by its grammar", and the predicate
// that decides it validated the BRACKETED authority thoroughly and then returned
// true for everything else.
//
// ⚠ `url.Parse` IS PERMISSIVE ABOUT REGISTERED NAMES. It accepts `se_cret`, `a;b`,
// `host$tok` and `..`, all of which rode into the capture VERBATIM with nothing
// recorded (shardpilot/shardpilot-go#85 review). RFC 3986 `reg-name` is not the
// test either -- it admits the sub-delims `!$&'()*+,;=`, so it would accept exactly
// the spellings that carried the text through. What the exemption is about is LDH.
func TestANonBracketedAuthorityIsMeasuredAgainstTheHostGrammar(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces, accountedSurfaces = nil, nil })
	for _, c := range []struct {
		authority string
		isHost    bool
	}{
		// ⚠ THE ALLOWED HALF IS THE POINT OF THIS SCENE. A guard that replaced these
		// would cost every capture its redirect target.
		{"ok.example", true}, {"ok.example.", true}, {"a.b.c:8443", true},
		{"xn--9ca.example", true}, {"127.0.0.1", true}, {"[2001:db8::1]", true},
		{"localhost", true}, {"user:pass@ok.example", true},
		// ⚠ THE PORT IS ONE QUESTION FOR BOTH FORMS. Asked twice, the bracketed
		// branch skipped port validation entirely because the authority held a `]`,
		// so `[::1]:99999999` kept an unusable port while the identical
		// `example.com:99999999` was replaced; and an EMPTY port, which RFC 3986
		// allows as `port = *DIGIT` and `parsesAsURI` already accepts, was refused
		// and cost the capture its host (shardpilot/shardpilot-go#85 review). Two
		// findings on ADJACENT lines of one function in one round is the signal that
		// the repair was finer than the defect.
		{"example.com:", true}, {"[::1]:443", true}, {"[::1]", true},
		{"[::1]:99999999", false},
		// ⚠ AND THE SIZES, which are the same property as the character set and were
		// left unstated: a 64-octet label and a 319-octet name were both called
		// publicly resolvable and published verbatim (shardpilot/shardpilot-go#85
		// review). DNS fixes a label at 1..63 octets and a name at 253.
		{strings.Repeat("a", 63) + ".example", true},
		{strings.Repeat("a", 64) + ".example", false},
		{strings.TrimSuffix(strings.Repeat(strings.Repeat("b", 63)+".", 5), "."), false},
		{"se_cret", false}, {"a;b", false}, {"host$tok", false}, {"..", false},
		{"-lead", false}, {"trail-", false}, {"sec.ret:99999999", false},
	} {
		structuralSurfaces, accountedSurfaces = nil, nil
		got := stripMarks(dropFraming("HTTP/1.1 302 Found\r\nLocation: https://" + c.authority + "/cb\r\n\r\n"))
		host := c.authority
		if i := strings.LastIndexByte(host, '@'); i >= 0 {
			host = host[i+1:]
		}
		if kept := strings.Contains(got, "://"+c.authority+"/") ||
			strings.Contains(got, "@"+host+"/"); kept != c.isHost {
			t.Errorf("authority %q kept=%v, want %v (%q)", c.authority, kept, c.isHost, got)
		}
		if !c.isHost && !slices.Contains(accountedSurfaces, "a redirect authority that is not a host") {
			t.Errorf("authority %q was replaced without reaching a ledger: %v", c.authority, accountedSurfaces)
		}
	}
}

// TestAnEmptyCookieAttributeNameIsRefused: `sid=x; =secret` is transport-valid and
// its extension has no name. The placeholder path rendered it
// `redacted-0-chars=redacted-6-chars` -- INVENTING a syntactically valid attribute
// where the endpoint sent a malformed one, and recording nothing, so the capture
// concealed the response defect (shardpilot/shardpilot-go#85 review). The PRIMARY
// cookie name asks exactly this question a hundred lines up and withholds.
func TestAnEmptyCookieAttributeNameIsRefused(t *testing.T) {
	t.Cleanup(func() { structuralSurfaces = nil })
	structuralSurfaces = nil
	got := stripMarks(redactSetCookie("Set-Cookie: sid=x; =secret"))
	if !strings.Contains(got, "<withheld>") || len(structuralSurfaces) == 0 {
		t.Errorf("an attribute with an empty name was rendered as one: %q %v", got, structuralSurfaces)
	}
	// ⚠ AND ORDINARY ATTRIBUTES AND FLAGS ARE UNTOUCHED, or the check has turned
	// every cookie into a withheld one.
	for _, ck := range []string{"sid=x; Path=/", "sid=x; Secure", "sid=x; Max-Age=-1"} {
		structuralSurfaces = nil
		if got := stripMarks(redactSetCookie("Set-Cookie: " + ck)); strings.Contains(got, "<withheld>") {
			t.Errorf("%q was withheld: %q %v", ck, got, structuralSurfaces)
		}
	}
}

// TestADirectiveArgumentBelongsToItsField: `walkDirectives` admits `name=<digits>`
// when a field's grammar has arguments, and every registered field was given that
// permission -- so `Allow: GET=123456`, `Content-Encoding: gzip=123456` and
// `Vary: accept=123456` all passed and the whole value was vouched, publishing
// endpoint-selected numeric text that both the scrub and the guard then skip if it
// is a supplied identifier (shardpilot/shardpilot-go#85 review). Those three
// grammars have no arguments at all.
func TestADirectiveArgumentBelongsToItsField(t *testing.T) {
	for _, c := range []struct {
		line string
		kept bool
	}{
		{"Allow: GET=123456", false},
		{"Content-Encoding: gzip=123456", false},
		{"Vary: accept=123456", false},
		// ⚠ AND A REGISTERED TOKEN IS NOT A LICENCE EITHER. Disabling NUMERIC
		// arguments described the kind of the example that had arrived; the next one
		// was `Allow: GET=POST`, admitted by the branch that accepted any registered
		// token as an argument, so a supplied `POST` was published with an empty
		// refusal ledger (shardpilot/shardpilot-go#85 review). A third condition
		// would have described the third example. The rule is the DIRECTIVE's.
		{"Allow: GET=POST", false},
		{"Cache-Control: no-cache=POST", false},
		{"Cache-Control: max-age=abc", false},
		// ⚠ AND THE FIELD THAT DOES HAVE ARGUMENTS KEEPS THEM, or the repair has
		// traded a published identifier for a capture that cannot show a max-age.
		{"Cache-Control: max-age=60", true},
		{"Allow: GET, POST", true},
		{"Content-Encoding: gzip", true},
		{"Vary: accept", true},
	} {
		if got := stripMarks(redactUnlessVerbatim(c.line)); (got == c.line) != c.kept {
			t.Errorf("%q kept=%v, want %v (%q)", c.line, got == c.line, c.kept, got)
		}
	}
}

// TestANonCanonicalCodingKeepsItsMeaning: with a supplied `IDENTITY`,
// `Content-Encoding: IDENTITY` -- which this path accepts as readable -- was
// rewritten to `Content-Encoding: redacted-8-chars` while the captured body stayed
// plain, so the published field declared a coding no consumer can apply and the
// capture contradicted itself (shardpilot/shardpilot-go#85 review). The canonical
// spelling is THIS program's text and means what the arrived spelling meant.
func TestANonCanonicalCodingKeepsItsMeaning(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces = nil, nil })
	suppliedValues, structuralSurfaces = []string{"IDENTITY"}, nil
	got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\nContent-Encoding: IDENTITY\r\n\r\nbody")))
	if !strings.Contains(got, "Content-Encoding: identity") {
		t.Errorf("a no-op coding lost its meaning: %q", got)
	}
	// ⚠ AND NOT WHERE THE CANONICAL SPELLING IS ITSELF SUPPLIED. Measured: with
	// `identity` supplied the guard reports it as a survivor and nothing is
	// publishable, so substituting it would trade a misleading capture for no
	// capture at all. That spelling stays CAPTURED rather than vouched.
	suppliedValues, structuralSurfaces = []string{"identity"}, nil
	raw := dropFraming("HTTP/1.1 200 OK\r\nContent-Encoding: IDENTITY\r\n\r\nbody")
	if err := assertNoLeak(asCaptured(raw)); err != nil {
		t.Errorf("the capture became unpublishable: %v", err)
	}
	if strings.Contains(stripMarks(scrubSupplied(raw)), "Content-Encoding: identity") {
		t.Errorf("the supplied canonical spelling was published: %q", stripMarks(scrubSupplied(raw)))
	}
}

// ---- round on 50d9a50 ----

// TestARedirectFollowUpIsForwardedNotAbsorbed is the P1 of this round. The
// recorder classified by PATH, so the follow-up `http.Client` sends after a 302
// -- which is not the assignment route -- was answered with the synthetic 204
// meant for the SDK's background ingest leg. The target was never contacted, the
// SDK acted on a response no server sent, and the report paired its verdict with
// an exchange it had not acted on.
//
// The scene fails on the old code with "the redirect target was never
// contacted": `sent` stays false and `offRoute` reaches 1.
func TestARedirectFollowUpIsForwardedNotAbsorbed(t *testing.T) {
	sent := false
	r := &recorder{inner: rtFunc(func(req *http.Request) (*http.Response, error) {
		sent = true
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}")),
			Header: http.Header{}, Proto: "HTTP/1.1", Request: req}, nil
	})}
	req, _ := http.NewRequest("GET", "https://e.example/cb", nil)
	// What net/http itself sets on a redirect follow-up, and the only thing that
	// distinguishes one from background ingest traffic at this layer.
	req.Response = &http.Response{StatusCode: 302}
	resp, err := r.RoundTrip(req)
	if err != nil {
		t.Fatalf("the redirect follow-up errored: %v", err)
	}
	if !sent {
		t.Fatal("the redirect target was never contacted — the recorder answered for it")
	}
	if resp.StatusCode != 200 {
		t.Fatalf("the SDK was handed an invented response: %d", resp.StatusCode)
	}
	if r.offRoute != 0 {
		t.Fatalf("a redirect leg was counted as off-route traffic: %d", r.offRoute)
	}
	if len(r.exchanges) != 1 || !r.exchanges[0].redirectLeg {
		t.Fatalf("the redirect leg was not recorded as one: %+v", r.exchanges)
	}
}

// TestBackgroundIngestIsStillAbsorbed is the other edge of the same predicate,
// written in the same movement. Widening "what may pass through" is exactly how a
// guard stops guarding, and the side effect this harness must not have -- an
// automatic exposure delivered from a run whose only purpose is to observe -- is
// held out by nothing else.
func TestBackgroundIngestIsStillAbsorbed(t *testing.T) {
	sent := false
	r := &recorder{inner: rtFunc(func(*http.Request) (*http.Response, error) {
		sent = true
		return &http.Response{StatusCode: 204, Body: io.NopCloser(strings.NewReader("")),
			Header: http.Header{}, Proto: "HTTP/1.1"}, nil
	})}
	req, _ := http.NewRequest("POST", "https://e.example/api/v1/ingest", strings.NewReader("{}"))
	if _, err := r.RoundTrip(req); err != nil {
		t.Fatalf("ingest request errored instead of being absorbed: %v", err)
	}
	if sent {
		t.Fatal("ingest traffic was FORWARDED — the harness emitted analytics")
	}
	if r.offRoute != 1 {
		t.Fatalf("absorbed ingest traffic was not counted: %d", r.offRoute)
	}
}

// TestTheCeilingSentinelIsDescribedNotRefused: `errOversizedForCapture` is this
// program's own text, but it reached the generic describer as an
// `*errors.errorString` with no matching case, so `sanitizeCaptured` recorded an
// unexcused structural refusal and a large-but-valid body exited 4 instead of
// producing the documented incomplete capture at exit 3.
//
// The scene fails on the old code with a non-empty refusal ledger.
func TestTheCeilingSentinelIsDescribedNotRefused(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	got := stripMarks(sanitizeCaptured(errOversizedForCapture))
	if len(refusalLedger()) != 0 {
		t.Fatalf("the recorder's own diagnostic was refused as an endpoint error: %q", refusalLedger())
	}
	if !strings.Contains(got, "read ceiling") {
		t.Fatalf("the sentinel was withheld rather than described: %q", got)
	}
}

// TestAnUnknownErrorTypeIsStillRefused is this predicate's other edge. The case
// added above is a hole in a refusal that exists to keep an ENDPOINT's message
// out of the report, and a type switch that answers "described" too readily is
// how that refusal stops firing.
func TestAnUnknownErrorTypeIsStillRefused(t *testing.T) {
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	got := stripMarks(sanitizeCaptured(errors.New("server-secret-token")))
	if len(refusalLedger()) == 0 {
		t.Fatal("an undescribable transport error left the capture publishable")
	}
	if strings.Contains(got, "server-secret-token") {
		t.Fatalf("an endpoint message was published: %q", got)
	}
}

// TestARegisteredMediaTypeCollisionStaysPublishable is the finding this round's
// structural change answers. `registryOnlyValue` walked the DIRECTIVE registries
// only, so `Content-Type` -- a registry field in the other three classifiers --
// could not satisfy it: an ordinary `application/json` against a supplied `json`
// fell past the vouch, took a structural refusal, and exited 4, while
// `markMediaType` vouched the same bytes one pass later and the stale refusal was
// never withdrawn.
//
// The scene fails on the old code with a non-empty refusal ledger.
func TestARegisteredMediaTypeCollisionStaysPublishable(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil })
	suppliedValues, structuralSurfaces, accountedSurfaces = []string{"json"}, nil, nil
	got := dropFraming("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{}")
	if len(refusalLedger()) != 0 {
		t.Fatalf("an ordinary media type colliding with a supplied value was refused: %q", refusalLedger())
	}
	if !strings.Contains(stripMarks(scrubSupplied(got)), "Content-Type: application/json") {
		t.Fatalf("a registered media type was not published as itself: %q", stripMarks(scrubSupplied(got)))
	}
}

// TestAShapeCollisionStillRefuses is the other edge, and the reason the fix is a
// per-field ROW rather than "registryOnlyValue also asks about media types".
// Integer syntax constrains the alphabet and says nothing about who chose the
// number, so a supplied identifier colliding with `Age` must NOT be waved through
// by the same widening -- `redacted-6-chars` is not an integer, and no placeholder
// is a legal argument here.
func TestAShapeCollisionStillRefuses(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil })
	suppliedValues, structuralSurfaces, accountedSurfaces = []string{"123456"}, nil, nil
	dropFraming("HTTP/1.1 200 OK\r\nAge: 123456\r\n\r\n{}")
	if len(refusalLedger()) == 0 {
		t.Fatal("a supplied identifier colliding with a shape-admitted field was published")
	}
}

// TestEveryRegistryFieldCanAnswerBothRegistryQuestions is the property the four
// hand-written classifiers could not hold. A field admitted BECAUSE A REGISTRY
// NAMES IT must also be able to say which of its values are registry words end to
// end -- that pair is what makes a collision answerable. `content-type` satisfied
// the first and not the second for four rounds, and no scene could see it because
// the two answers lived in different functions.
//
// This is the invariant, not an instance: a field added later cannot land in
// three of the four answers, because there is now one row and this reads it.
func TestEveryRegistryFieldCanAnswerBothRegistryQuestions(t *testing.T) {
	for name, adm := range verbatimHeaders {
		if adm.vocabulary && adm.registryOnly == nil {
			t.Errorf("%q is admitted by a registry but has no registry-only rule, "+
				"so a collision on it can only be refused", name)
		}
		if !adm.vocabulary && adm.registryOnly != nil {
			t.Errorf("%q is admitted by a SHAPE but claims registry-only values, "+
				"so an endpoint-chosen value could be vouched", name)
		}
		if !adm.vocabulary && adm.folds {
			t.Errorf("%q has no registry, so it has no canonical spelling to fold to", name)
		}
	}
}

// TestAJSONStringIsMeasuredAsItArrived is the finding at the body traversal.
// `responseText` runs `escapeMarks` over the whole response before redaction, and
// that escape LENGTHENS a backslash run standing before the literal `x00`/`x01`.
// The traversal then decoded and measured the expanded spelling, so the wire's
// four characters were published as `redacted-6-chars` -- while a header carrying
// the same value reported four, because every measured site outside this
// traversal unescapes first.
//
// Both halves are asserted: the member NAME and the member VALUE, because the
// finding names both and one site was fixed once before while its neighbour was
// not.
//
// The scene fails on the old code with `redacted-6-chars`.
func TestAJSONStringIsMeasuredAsItArrived(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil })
	suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil
	raw := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n" + `{"message":"\\x00"}`
	got := stripMarks(dropFraming(escapeMarks(raw)))
	if strings.Contains(got, "redacted-6-chars") {
		t.Fatalf("the value was measured after the recorder expanded it: %q", got)
	}
	if !strings.Contains(got, "redacted-4-chars") {
		t.Fatalf("the four characters the endpoint sent were not measured as four: %q", got)
	}

	// The same value in the member NAME position, which the finding names as the
	// second site.
	structuralSurfaces, accountedSurfaces = nil, nil
	rawKey := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n" + `{"\\x00":"v"}`
	gotKey := stripMarks(dropFraming(escapeMarks(rawKey)))
	if strings.Contains(gotKey, "redacted-6-chars") {
		t.Fatalf("the member name was measured after the recorder expanded it: %q", gotKey)
	}
	if !strings.Contains(gotKey, "redacted-4-chars") {
		t.Fatalf("the member name was not measured as it arrived: %q", gotKey)
	}
}

// TestAnOrdinaryJSONValueIsStillMeasuredWhole is the other edge: undoing the
// escape must not shorten a value that never carried the escape's trigger. A
// backslash run before anything else, and a plain value, are measured as
// themselves.
func TestAnOrdinaryJSONValueIsStillMeasuredWhole(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil })
	suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil
	raw := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n" + `{"m":"abcde"}`
	if got := stripMarks(dropFraming(escapeMarks(raw))); !strings.Contains(got, "redacted-5-chars") {
		t.Fatalf("a plain five-character value was not measured as five: %q", got)
	}
	structuralSurfaces, accountedSurfaces = nil, nil
	// `\\` decodes to one backslash and is not followed by the escape's trigger.
	raw2 := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n" + `{"m":"\\y00"}`
	if got := stripMarks(dropFraming(escapeMarks(raw2))); !strings.Contains(got, "redacted-4-chars") {
		t.Fatalf("a backslash run not before the escape's trigger was mismeasured: %q", got)
	}
}

// ---- round on adbb037 ----

// TestARedirectLegPublishesNoEndpointTarget is the P1 of this round, and it is a
// consequence of the previous one. Forwarding redirect follow-ups was right; what
// it broke is an assumption the REQUEST redactor had been able to make while they
// were absorbed — that every byte of a request dump is this program's. It is not:
// on a redirect leg the SDK issues a target the endpoint chose, and `http.Client`
// generates a `Referer` from the previous endpoint-selected URL.
//
// Driven as a real two-redirect chain rather than a composed dump, because the
// finding is about what net/http DOES, and a fixture that composes the headers
// would assert my belief about that instead of the fact.
//
// On the pre-fix code this fails on the request line first: `GET
// /server-secret-token?x=y` is published whole.
func TestARedirectLegPublishesNoEndpointTarget(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil })
	suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, assignmentRoute):
			http.Redirect(w, r, "/server-secret-token?x=y", http.StatusFound)
		case r.URL.Path == "/server-secret-token":
			http.Redirect(w, r, "/second-endpoint-choice", http.StatusFound)
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"assigned":false,"reason":"absent"}`))
		}
	}))
	defer srv.Close()

	rec := &recorder{inner: http.DefaultTransport}
	req, _ := http.NewRequest("GET", srv.URL+assignmentRoute, nil)
	resp, err := (&http.Client{Transport: rec}).Do(req)
	if err != nil {
		t.Fatalf("the redirect chain failed: %v", err)
	}
	_ = resp.Body.Close()

	if len(rec.exchanges) != 3 {
		t.Fatalf("expected the assignment and two followed legs, got %d", len(rec.exchanges))
	}
	// Every byte the endpoint chose, in every recorded request.
	for i := range rec.exchanges {
		got := stripMarks(scrubSupplied(string(rec.exchanges[i].req)))
		for _, endpointChosen := range []string{"server-secret-token", "second-endpoint-choice"} {
			if strings.Contains(got, endpointChosen) {
				t.Errorf("leg %d published the endpoint's own %q:\n%s", i, endpointChosen, got)
			}
		}
	}
	// And the Referer's QUERY, which no request-side rule read before: nothing on
	// the request side treated a header value as a URI at all.
	last := stripMarks(scrubSupplied(string(rec.exchanges[2].req)))
	if strings.Contains(last, "x=y") {
		t.Errorf("the generated Referer published its query verbatim:\n%s", last)
	}
}

// TestACrossHostRedirectPublishesNoEndpointAuthority is this round's P1, and it is
// the case I ARGUED OUT of the previous round's population. I wrote that a redirect
// leg's `Host` is not a leak because the response side exempts a host as
// structurally constrained -- reasoning about the exemption's RULE and not its
// PREMISE. The premise is "publicly resolvable and constrained by its grammar", and
// `redactTarget` enforces it: measured, the response side prints
// `http://redacted-13-chars/redacted-2-chars` for `http://server_secret/cb` while
// the request leg published `Host: server_secret` with an EMPTY ledger.
//
// The dial fails -- that host does not resolve -- but `DumpRequestOut` runs inside
// `RoundTrip`, so the leg is recorded and published either way.
func TestACrossHostRedirectPublishesNoEndpointAuthority(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil })
	suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://server_secret/cb", http.StatusFound)
	}))
	defer srv.Close()

	rec := &recorder{inner: http.DefaultTransport}
	req, _ := http.NewRequest("GET", srv.URL+assignmentRoute, nil)
	if resp, err := (&http.Client{Transport: rec}).Do(req); err == nil {
		_ = resp.Body.Close()
	}
	if len(rec.exchanges) != 2 {
		t.Fatalf("expected the assignment and the followed leg, got %d", len(rec.exchanges))
	}
	for i := range rec.exchanges {
		got := stripMarks(scrubSupplied(string(rec.exchanges[i].req)))
		if strings.Contains(got, "server_secret") {
			t.Errorf("leg %d published the endpoint's own authority:\n%s", i, got)
		}
	}
	// And the response side's answer for the same authority, so the two halves are
	// pinned as AGREEING rather than each being right on its own.
	structuralSurfaces, accountedSurfaces = nil, nil
	if resp := stripMarks(redactTarget("Location: http://server_secret/cb")); strings.Contains(resp, "server_secret") {
		t.Errorf("the response side stopped enforcing the exemption's premise: %q", resp)
	}
}

// TestARedirectLegMarksItsFieldNames is the second half of the same repair. The
// first version of the `Referer` branch returned `redactTarget`'s line as it
// stands, leaving the field NAME bare inside the captured span -- so a legal
// experiment key of `Referer` or `Host` would be reported by the guard as a
// survivor and refuse every run, which is what `Host` and `User-Agent` already cost
// this file once.
func TestARedirectLegMarksItsFieldNames(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil })
	for _, key := range []string{"Referer", "Host"} {
		t.Run(key, func(t *testing.T) {
			suppliedValues, structuralSurfaces, accountedSurfaces = []string{key}, nil, nil
			dump := []byte(escapeMarks("GET /cb HTTP/1.1\r\nHost: other.example\r\n" +
				"Referer: http://e.example/prev\r\n\r\n"))
			got := string(redact(dump, http.Header{"Host": nil, "Referer": nil}, true))
			if err := assertNoLeak(asCaptured(got)); err != nil {
				t.Fatalf("the %s field name was reported as a surviving supplied value: %v", key, err)
			}
		})
	}
}

// TestAHarnessOriginatedRequestIsUnchanged is the other edge. The repair above
// hands two lines of a request dump to the RESPONSE side's redactor, and applying
// that to the leg this program itself composed would lengthen the parameter names
// it authored — the readable-artifact property three scenes pin.
func TestAHarnessOriginatedRequestIsUnchanged(t *testing.T) {
	t.Cleanup(func() {
		suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil
		requestNames = map[string]bool{}
	})
	suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil
	// The names this program put on the wire, which is what makes them ours.
	requestNames = map[string]bool{"a": true, "b": true}
	dump := []byte("GET /x?a=1&b=2 HTTP/1.1\r\nHost: e.example\r\nReferer: http://e.example/prev\r\n\r\n")
	got := stripMarks(string(redact(dump, http.Header{"Host": nil, "Referer": nil}, false)))
	if !strings.Contains(got, "a=") || !strings.Contains(got, "b=") {
		t.Errorf("a harness-authored parameter name was lengthened: %q", got)
	}
	if !strings.Contains(got, "GET /x?") {
		t.Errorf("a harness-authored request path was redacted: %q", got)
	}
	if !strings.Contains(got, "Referer: http://e.example/prev") {
		t.Errorf("a harness-authored Referer was rewritten: %q", got)
	}
}

// TestAMalformedCodingIsNotReadAsTheNoOpOne is the round's coding finding.
// `strings.TrimSpace` removes Unicode whitespace — U+00A0 among it — which
// net/http preserves in a field value. So `Content-Encoding: identity\u00a0` was
// trimmed to the well-known token, the undecodable-coding refusal was skipped, and
// the non-token byte was published while the body was called readable. `ows` —
// space and tab, what HTTP calls optional whitespace — already existed in this
// file with a comment naming this exact defect.
//
// ⚠ WHAT THIS SCENE DOES AND DOES NOT COVER, because the change is wider than it.
// The same `TrimSpace` stood at seven other endpoint-facing trims (the field name,
// the media type, the cache directive, the minted-field lookup), and all of them
// moved to `ows` — that is the file's own stated rule and leaving them would leave
// the trap. But three of those were MEASURED before and after and produce the same
// bytes: a later rule already redacts those values, so they are defence in depth,
// not fixed leaks. Only this one is demonstrated, and a scene per site would have
// been three that cannot fail.
func TestAMalformedCodingIsNotReadAsTheNoOpOne(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil })
	// U+00A0, written as an escape so the byte survives an edit that reflows this file.
	const nbsp = "\u00a0"
	structuralSurfaces, accountedSurfaces, suppliedValues = nil, nil, nil
	got := stripMarks(dropFraming(escapeMarks(
		"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Encoding: identity" +
			nbsp + "\r\n\r\n{\"a\":1}")))
	if strings.Contains(got, "identity"+nbsp) {
		t.Errorf("a coding carrying non-HTTP whitespace was published as recognised: %q", got)
	}
	found := false
	for _, r := range refusalLedger() {
		if strings.Contains(r, "content coding this build cannot decode") {
			found = true
		}
	}
	if !found {
		t.Fatalf("a malformed coding was classified as the no-op one: %q", refusalLedger())
	}
}

// TestAnOrdinaryIdentityCodingStillPublishes is the other edge. Narrowing the trim
// to OWS must not stop the well-formed value being recognised — a classifier
// repaired into refusing everything protects nothing and gets switched off.
func TestAnOrdinaryIdentityCodingStillPublishes(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil })
	structuralSurfaces, accountedSurfaces, suppliedValues = nil, nil, nil
	got := stripMarks(dropFraming(escapeMarks(
		"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Encoding: identity\r\n\r\n{\"a\":1}")))
	if !strings.Contains(got, "Content-Encoding: identity") {
		t.Errorf("an ordinary no-op coding stopped being recognised: %q", got)
	}
	if !strings.Contains(got, "Content-Type: application/json") {
		t.Errorf("an ordinary media type stopped being recognised: %q", got)
	}
	for _, r := range refusalLedger() {
		if strings.Contains(r, "content coding this build cannot decode") {
			t.Errorf("an ordinary identity coding was refused: %q", refusalLedger())
		}
	}
}

// TestTheSANCountNamesItsOwnFamily: crypto/x509 builds a hostname-mismatch
// diagnostic from `IPAddresses` when the host is an IP literal and from `DNSNames`
// otherwise. This counted DNS names always, so an IP endpoint whose certificate
// carries DNS SANs and no IP SAN reported a count of an unrelated population —
// misleading precisely where an operator is diagnosing an IP.
//
// The family is asserted as well as the count, because a count whose population is
// not named is not a measurement.
func TestTheSANCountNamesItsOwnFamily(t *testing.T) {
	ipCert := &x509.Certificate{
		DNSNames:    []string{"a.example", "b.example"},
		IPAddresses: nil,
	}
	out := stripMarks(sanitizeCaptured(&tls.CertificateVerificationError{
		Err: x509.HostnameError{Certificate: ipCert, Host: "203.0.113.7"}}))
	if !strings.Contains(out, "san=ip") {
		t.Errorf("an IP host was answered from the DNS population: %q", out)
	}
	if strings.Contains(out, "names=2") {
		t.Errorf("the count came from the two DNS SANs, which cannot match an IP host: %q", out)
	}
	if !strings.Contains(out, "names=0 configured-host-listed=false") {
		t.Errorf("the applicable population is not reported as empty: %q", out)
	}

	// The DNS side keeps working, and reports its own family.
	dnsOut := stripMarks(sanitizeCaptured(&tls.CertificateVerificationError{
		Err: x509.HostnameError{Certificate: ipCert, Host: "a.example"}}))
	if !strings.Contains(dnsOut, "san=dns") || !strings.Contains(dnsOut, "names=2") ||
		!strings.Contains(dnsOut, "configured-host-listed=true") {
		t.Errorf("the DNS answer changed: %q", dnsOut)
	}
	// And no SAN is taken, on either path.
	for _, n := range []string{"a.example", "b.example"} {
		if strings.Contains(out, n) {
			t.Errorf("a certificate-controlled name %q reached the artifact: %q", n, out)
		}
	}
}

// TestAnOpaqueTargetKeepsTheReceivedFieldLayout: `head` already carries the colon
// and the OWS, and this return appended the gap a second time — so an ordinary
// one-space header came back with two, and the evidence stopped preserving the
// layout it received while only the endpoint's value was meant to change.
func TestAnOpaqueTargetKeepsTheReceivedFieldLayout(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil })
	structuralSurfaces, accountedSurfaces, suppliedValues = nil, nil, nil
	got := stripMarks(redactTarget("Location: https:secret"))
	if strings.Contains(got, "Location:  ") {
		t.Errorf("the received field layout gained a space: %q", got)
	}
	if !strings.HasPrefix(got, "Location: https:") {
		t.Errorf("the scheme or the layout was lost: %q", got)
	}
	if strings.Contains(got, "secret") {
		t.Errorf("the opaque endpoint value survived: %q", got)
	}
	// A header written with NO space after the colon keeps having none.
	structuralSurfaces, accountedSurfaces = nil, nil
	tight := stripMarks(redactTarget("Location:https:secret"))
	if !strings.HasPrefix(tight, "Location:https:") {
		t.Errorf("a header with no OWS gained some: %q", tight)
	}
}

// TestANonCanonicalCollisionKeepsItsGrammar is this round's finding and the
// sibling no review named. Declining to VOUCH a non-canonical spelling was right;
// leaving it to a scrub that does not know the field's grammar was not, and the
// two are the same sentence one step apart.
//
// Both tokens fold, so the canonical spelling means what arrived and is this
// program's own text — the answer the admitted header value already gives.
func TestANonCanonicalCollisionKeepsItsGrammar(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil })
	for _, c := range []struct{ name, supplied, raw, want, reject string }{
		{"the redirect scheme", "HTTPS",
			"HTTP/1.1 302 Found\r\nLocation: HTTPS://e.example/cb\r\n\r\n",
			"Location: https://", "redacted-5-chars://"},
		{"an enumerated cookie attribute", "LAX",
			"HTTP/1.1 200 OK\r\nSet-Cookie: a=b; SameSite=LAX\r\n\r\n",
			"SameSite=Lax", "SameSite=redacted-3-chars"},
	} {
		t.Run(c.name, func(t *testing.T) {
			suppliedValues, structuralSurfaces, accountedSurfaces = []string{c.supplied}, nil, nil
			got := stripMarks(scrubSupplied(dropFraming(escapeMarks(c.raw))))
			if strings.Contains(got, c.reject) {
				t.Errorf("a placeholder its own grammar rejects was published: %q", got)
			}
			if !strings.Contains(got, c.want) {
				t.Errorf("the canonical spelling was not substituted: %q", got)
			}
			if len(refusalLedger()) != 0 {
				t.Errorf("a repairable collision was refused: %q", refusalLedger())
			}
		})
	}
}

// TestACollisionWithNoSafeSpellingRefuses is the other edge, and the reason the
// repair above is not just "always lower-case it". When the canonical spelling is
// ITSELF supplied, nothing semantics-preserving is left — and a placeholder with an
// empty ledger is exactly the shape this whole change exists to stop.
func TestACollisionWithNoSafeSpellingRefuses(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil })
	for _, c := range []struct {
		name, raw string
		supplied  []string
	}{
		{"the redirect scheme", "HTTP/1.1 302 Found\r\nLocation: HTTPS://e.example/cb\r\n\r\n",
			[]string{"HTTPS", "https"}},
		{"an enumerated cookie attribute", "HTTP/1.1 200 OK\r\nSet-Cookie: a=b; SameSite=LAX\r\n\r\n",
			[]string{"LAX", "Lax"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			suppliedValues, structuralSurfaces, accountedSurfaces = c.supplied, nil, nil
			dropFraming(escapeMarks(c.raw))
			if len(refusalLedger()) == 0 {
				t.Fatal("no spelling was left and the capture was published anyway")
			}
		})
	}
}

// TestACanonicalSpellingIsStillPublished is the third edge: an ordinary value that
// collides with nothing must be untouched, or the repair has been achieved by
// redacting everything.
func TestACanonicalSpellingIsStillPublished(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil })
	suppliedValues, structuralSurfaces, accountedSurfaces = nil, nil, nil
	got := stripMarks(scrubSupplied(dropFraming(escapeMarks(
		"HTTP/1.1 302 Found\r\nLocation: https://e.example/cb\r\n\r\n"))))
	if !strings.Contains(got, "Location: https://e.example/") {
		t.Errorf("an ordinary redirect stopped being published as itself: %q", got)
	}
	structuralSurfaces, accountedSurfaces = nil, nil
	ck := stripMarks(scrubSupplied(dropFraming(escapeMarks(
		"HTTP/1.1 200 OK\r\nSet-Cookie: a=b; SameSite=Lax\r\n\r\n"))))
	if !strings.Contains(ck, "SameSite=Lax") {
		t.Errorf("an ordinary SameSite stopped being published as itself: %q", ck)
	}
}
