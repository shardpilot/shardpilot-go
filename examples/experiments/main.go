// Command experiments records one experiment-assignment exchange made by this
// SDK against a live endpoint: the request as the SDK built it, and the
// response as it came back.
//
// WHY THIS EXISTS, AND WHY curl DOES NOT. Reaching the endpoint with curl shows
// that the endpoint answers. It does not show that THIS SDK reaches it: the
// route it builds, the query it assembles, the Authorization header it sets and
// the transport it uses are the SDK's, and a fault in any of them is invisible
// to a hand-made request. So this program changes nothing in the SDK -- it
// supplies an http.Client whose RoundTripper records what the SDK actually sent
// and what came back, which makes the result an artifact rather than a claim
// that something worked.
//
// The Authorization header is redacted in the output. The key is read from the
// environment and never from a command line, because a command line is visible
// to every process on the host and lands in shell history.
//
//	SP_REMOTE_CONFIG_URL=https://app.shardpilot.com \
//	SP_API_KEY=sp_ingest_... \
//	SP_WORKSPACE_ID=... SP_APP_ID=... SP_ENVIRONMENT_ID=... \
//	SP_EXPERIMENT_KEY=... \
//	go run ./examples/experiments
//
// Exit codes, and they are distinct because a consumer of this program reads
// them rather than the prose:
//
//	0  a complete pair was captured and the assignment was served
//	1  a complete pair was captured and the endpoint refused it
//	2  no request was made at all
//	3  a request was made and NO COMPLETE PAIR came back -- transport failure,
//	   a truncated body, or a deadline. Distinct from 1 on purpose: 1 says the
//	   endpoint answered and the answer is in the record, and an incomplete run
//	   reported as 1 would be read as a refusal that was never observed.
//	4  THE RECORD WAS NOT PUBLISHED. Either a supplied value survived redaction
//	   in some encoding and the report was withheld, or the report could not be
//	   written whole. Nothing is claimed about the exchange: it may have been a
//	   perfect 200. This is a failure OF THE RECORDER, kept distinct from 3 --
//	   which is a failure of the exchange -- because the remedies differ and a
//	   consumer reads these codes rather than the prose. Added when the leak
//	   guard landed, which had been returning an undocumented status
//	   (shardpilot/shardpilot-go#73 review).
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"maps"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	shardpilot "github.com/shardpilot/shardpilot-go"
)

// recorder captures the one exchange the SDK performs. It is deliberately not
// a general proxy: a capture that summarises is a capture that can omit the
// thing under test.
// exchange is one attempt. The SDK may make several -- a refusal can send it
// round again with a fresh subject -- and a recorder that keeps only the last
// one reports a multi-attempt sequence as a single request that never happened
// that way.
type exchange struct {
	req      []byte
	head     []byte
	status   int
	proto    string // the protocol actually negotiated, which the request dump is not
	transErr error  // set when no response arrived at all
	captured *teeBody
}

// body is what the SDK actually read, and truncErr is the SDK's own read
// failure if there was one -- observed rather than caused.
func (e *exchange) body() []byte {
	if e.captured == nil {
		return nil
	}
	return e.captured.buf.Bytes()
}

func (e *exchange) truncErr() error {
	if e.captured == nil {
		return nil
	}
	// ⚠ AN EOF SEEN AT THE CEILING IS CONCLUSIVE. A fixed-Content-Length body can
	// deliver its last bytes together with io.EOF, so a response that is exactly
	// capturedBodyMax long is COMPLETE and the SDK's own refusal of it is a
	// complete refusal -- exit 1, not exit 3. Treating every ceiling-sized body
	// as indeterminate discarded the one witness that settles it
	// (shardpilot/shardpilot-go#73 review).
	if e.captured.err == nil && e.captured.overflowed {
		return errOversizedForCapture
	}
	// ⚠ EOF CAN BE SYNTHESISED ABOVE THIS WRAPPER. The SDK reads through
	// io.LimitReader, and when the limit is reached exactly, the limiter returns
	// EOF itself WITHOUT calling teeBody.Read again -- so `sawEOF` stays false on
	// a body that is complete, and a complete refusal reported exit 3 instead of
	// 1. The earlier test read the tee directly and performed the extra read that
	// production suppresses, which is why it passed
	// (shardpilot/shardpilot-go#73 review). An exact Content-Length is a signal
	// visible ABOVE the limiter and settles the case without one.
	if e.captured.err == nil && e.captured.atCeiling && !e.captured.sawEOF &&
		e.captured.declared != int64(e.captured.buf.Len()) {
		return errOversizedForCapture
	}
	return e.captured.err
}

// errOversizedForCapture is not the SDK's failure -- it is THIS RECORD's. The
// SDK may have handled the oversized body perfectly; what is incomplete is the
// copy, and the run is reported as an incomplete capture rather than as a
// complete one that happens to be short.
var errOversizedForCapture = errors.New(
	"the body reached the read ceiling this record shares with the SDK, so whether " +
		"more followed is not knowable from here; the capture is reported incomplete " +
		"rather than guessed, and the SDK's own verdict above is unaffected")

func (e *exchange) resp() []byte {
	if e.head == nil {
		return nil
	}
	return append(append([]byte{}, e.head...), e.body()...)
}

// trailerReport renders captured trailers as REPORT METADATA, outside the HTTP
// message entirely.
//
// ⚠ THE FIRST FIX APPENDED THEM TO THE BODY. With the framing headers removed,
// a parser reads a close-delimited body, so the capture note and the trailer
// lines became payload: the artifact both altered the body the SDK received and
// still did not represent trailers semantically -- and HTTP/2 trailers are a
// separate header block in any case (shardpilot/shardpilot-go#73 review). The
// fenced block is now exactly the bytes; everything the report adds lives
// outside it.
func (e *exchange) trailerReport() string {
	if e.captured == nil || len(e.captured.trailer) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("Trailer fields, received after the body and recorded here rather than\n" +
		"inside the message above, because the framing that carries them is not in\n" +
		"the decoded bytes:\n\n")
	for _, k := range slices.Sorted(maps.Keys(e.captured.trailer)) {
		for _, v := range e.captured.trailer[k] {
			// THE NAME IS PRINTED TOO, so it is scrubbed too: a trailer legally
			// named `X-<key>` published the identifier in the name while only the
			// value was cleaned, and the guard's short-value rule counts `-` as a
			// word byte so the boundary check did not catch it either
			// (shardpilot/shardpilot-go#73 review).
			// ⚠ CAPTURED, SO MARKED AS SUCH. Trailer fields come off the wire, and
			// outside a captured span the leak check does not read them at all --
			// so an unanticipated encoding of a supplied value in a trailer was
			// published unexamined (shardpilot/shardpilot-go#73 review).
			// ⚠ AND THE STRUCTURAL RULES, NOT ONLY THE SUPPLIED-VALUE SCRUB. A
			// `Set-Cookie` or `Location` arriving as a trailer -- both legal, and
			// net/http accepts them -- carries a SERVER-generated value, which no
			// list of values this program supplied can reach, so it was published
			// verbatim (shardpilot/shardpilot-go#73 review). Same dispatch as the
			// header block, same order: structural first, supplied-value second.
			line := structuralRedact(scrubHeaderName(escapeMarks(k)) + ": " + escapeMarks(v))
			fmt.Fprintf(&b, "    %s\n", asCaptured(scrubSupplied(line)))
		}
	}
	b.WriteString("\n")
	return b.String()
}

// dropFraming removes Content-Length from a recorded response.
//
// Redaction replaces identifiers with longer placeholders, so the recorded
// length stops describing the recorded body: a parser honouring it would stop
// inside the redacted text or read the remainder as trailing data, and the
// artifact would not be a valid HTTP response at all -- which defeats the point
// of keeping one (shardpilot/shardpilot-go#73 review).
//
// REMOVED rather than recomputed. A recomputed length would describe the
// redacted body and quietly assert that this is what arrived; the header is
// dropped and its absence says the body was altered, which is the true thing.
// fenceFor returns a backtick run longer than any run inside the content, so a
// captured body cannot close its own container.
//
// A non-JSON error body carrying a line of three backticks ended the fence early
// and let the remainder appear as report prose -- forged verdict sections in an
// artifact whose whole purpose is to be published
// (shardpilot/shardpilot-go#73 review).
// forFence is GONE as a content transform. See fencedBlock: the newline a
// Markdown fence needs is emitted by the printer, outside the content, and the
// report says so when the content did not end with one.
//
// The retired comment, kept because the defect it records is easy to reintroduce:
// forFence guaranteed a trailing newline so a fence closes on its own line, and
// changed nothing else.
//
// ⚠ IT REPLACES A TrimRight THAT DELETED THE HTTP MESSAGE TERMINATOR. Every
// bodyless GET dump ends with the required CRLF CRLF; trimming "\r\n" removed
// the whole empty line, and the single newline printed before the closing fence
// did not recreate it -- so a strict parser reached unexpected EOF after the
// last header, and legitimate trailing newlines were stripped from response
// bodies too (shardpilot/shardpilot-go#73 review).
// fencedBlock renders content inside a Markdown fence WITHOUT altering it.
//
// ⚠ ITS PREDECESSOR APPENDED A NEWLINE WHEN THE CONTENT LACKED ONE -- the normal
// shape for compact JSON. With the framing headers removed, a parser reads the
// recorded response as close-delimited, so that manufactured byte became payload
// and the artifact no longer held the body the SDK received
// (shardpilot/shardpilot-go#73 review). The newline a fence needs is the
// PRINTER'S, and when it is not part of the content the report says so on the
// line after the block.
func fencedBlock(content string) string {
	f := fenceFor(content)
	if strings.HasSuffix(content, "\n") {
		return f + "\n" + content + f + "\n"
	}
	return f + "\n" + content + "\n" + f + "\n" +
		"*(The content above does not end with a newline; the one before the closing\n" +
		"fence is Markdown's, not the message's.)*\n"
}

func fenceFor(content string) string {
	longest := 0
	run := 0
	for i := 0; i < len(content); i++ {
		if content[i] == '`' {
			run++
			if run > longest {
				longest = run
			}
		} else {
			run = 0
		}
	}
	n := longest + 1
	if n < 3 {
		n = 3
	}
	return strings.Repeat("`", n)
}

// ⚠ HEADER BLOCK ONLY. The first version walked every line, so a plain-text or
// multiline body containing a line beginning `Content-Length:` had that line
// REPLACED by a capture note -- the recorder editing endpoint-provided evidence
// that had nothing to do with redaction (shardpilot/shardpilot-go#73 review).
// redactSetCookie keeps `Set-Cookie: <name>=` and every attribute after the
// first `;`, and replaces only the value.
//
// ⚠ IT MUST RUN BEFORE THE SUPPLIED-VALUE SUBSTITUTION, which is why the call
// sites read `scrubSupplied(dropFraming(...))` and not the reverse. With the
// old order, a cookie whose value equalled a supplied identifier was already
// `<redacted, N chars>` by the time this function measured it, so the header
// reported the PLACEHOLDER's length -- an 8-byte cookie printed as
// `<redacted, 19 chars>`, a second wrong number where this function promises
// the real one (shardpilot/shardpilot-go#73 review).
// headerNameEnd returns the index just past a header line's name, and whether
// the line looks like a header at all. The status line and the body have no
// name to scrub.
// isTokenByte is RFC 7230's `tchar`: the characters a field name may legally
// contain.
//
// ⚠ `isWordByte` IS NOT THAT GRAMMAR. It admits letters, digits, `_` and `-`,
// so a legal name like `X.Secret` failed the header test, the line fell through
// to the generic scrub, and the NAME got the prose placeholder
// `X.<redacted, 6 chars>` -- spaces and angle brackets inside a field name, an
// unparsable response, produced by the very code that exists to keep it
// parsable (shardpilot/shardpilot-go#73 review). The token-safe fix was there;
// these names never reached it.
func isTokenByte(c byte) bool {
	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func headerNameEnd(line string) (int, bool) {
	i := strings.IndexByte(line, ':')
	if i <= 0 || strings.HasPrefix(line, "HTTP/") {
		return 0, false
	}
	for j := 0; j < i; j++ {
		if !isTokenByte(line[j]) {
			return 0, false
		}
	}
	return i, true
}

// responseText is the ONE place the response pipeline is composed, and the
// order inside it is load-bearing: structural redaction first, supplied-value
// substitution second. See redactSetCookie for why.
//
// ⚠ IT EXISTS BECAUSE A TEST OF THE COMPOSITION IS NOT A TEST OF THE CALL SITE.
// The first fixture for that ordering called `scrubSupplied(dropFraming(...))`
// itself, so a mutant that restored the wrong order at the two call sites
// SURVIVED it -- the test was checking its own copy of the pipeline
// (shardpilot/shardpilot-go#73 review, found by mutating rather than by
// reading). Naming it once removes the copy.
func responseText(ex *exchange) string {
	return asCaptured(scrubSupplied(dropFraming(escapeMarks(string(ex.resp())))))
}

// redactUserinfo removes `user:password@` from a redirect target.
//
// ⚠ A THIRD PLACE A URL CARRIES A CREDENTIAL, after the query and the fragment,
// and the one that is a credential BY DEFINITION rather than by convention:
// `Location: https://user:secret@example.com/…` is standard URI syntax
// (shardpilot/shardpilot-go#73 review). Kept as a marker rather than dropped,
// so the artifact still shows that userinfo was present.
func redactUserinfo(line string) string {
	// A network-path reference -- `//user:secret@host/cb` -- is a legal Location
	// and carries userinfo without a scheme (shardpilot/shardpilot-go#73 review).
	i := strings.Index(line, "://")
	skip := 3
	if i < 0 {
		if j := strings.Index(line, "//"); j >= 0 {
			i, skip = j, 2
		} else {
			return line
		}
	}
	rest := line[i+skip:]
	at := strings.IndexByte(rest, '@')
	if at < 0 {
		return line
	}
	for _, c := range rest[:at] {
		if c == '/' || c == ' ' || c == '?' || c == '#' {
			return line
		}
	}
	return line[:i+skip] + tokenPlaceholder(rest[:at]) + rest[at:]
}

// redactFragment applies the query treatment to a URL fragment: parameter names
// kept, values replaced by their length. A fragment with no `=` is replaced
// whole, because an opaque fragment is not a name and cannot be shown to be
// harmless.
func redactFragment(line string) string {
	i := strings.IndexByte(line, '#')
	if i < 0 {
		return line
	}
	head, frag := line[:i+1], line[i+1:]
	tail := ""
	if j := strings.IndexByte(frag, ' '); j >= 0 {
		frag, tail = frag[:j], frag[j:]
	}
	if frag == "" {
		return line
	}
	if !strings.Contains(frag, "=") {
		return head + tokenPlaceholder(frag) + tail
	}
	// ⚠ `?` IS ORDINARY FRAGMENT DATA, NOT A QUERY INTRODUCER. This delegated to
	// redactQuery, which cut at the first `?` and kept everything before it as a
	// parameter NAME -- so `#server-secret?x=y` published `server-secret`
	// verbatim while reporting the fragment redacted
	// (shardpilot/shardpilot-go#73 review). A fragment component whose name side
	// carries a `?` is not a name/value pair; it is opaque.
	return head + redactPairs(frag, "?") + tail
}

// redactTarget redacts a header line carrying a URL.
//
// ⚠ THE FRAGMENT IS CUT BEFORE ANY QUERY IS INTERPRETED. Composed the other way
// round -- redactFragment(redactQuery(line)) -- redactQuery saw the `?` INSIDE
// the fragment, split there, and by the time redactFragment ran the fragment
// contained a generated `x=` component and no longer looked opaque
// (shardpilot/shardpilot-go#73 review). A `#` always ends the query, so cutting
// there first is what the grammar says.
func redactTarget(line string) string {
	if i := strings.IndexByte(line, '#'); i >= 0 {
		return redactPath(redactUserinfo(redactQuery(line[:i]))) + redactFragment(line[i:])
	}
	return redactPath(redactUserinfo(redactQuery(line)))
}

// redactPath replaces every non-empty path segment of a redirect target with its
// length, keeping the separators.
//
// ⚠ THE PATH CARRIES CREDENTIALS AS READILY AS THE QUERY. `/reset/<token>` is
// an ordinary shape, the segment is SERVER-generated, and so no list of values
// this program supplied can reach it -- userinfo, query and fragment were all
// covered and the path was published whole (shardpilot/shardpilot-go#73 review).
// Every segment goes, not a chosen few: picking which ones "look opaque" is the
// entropy guess this file has already been burnt by, and a redirect path is
// server-generated in its entirety.
func redactPath(line string) string {
	head, url, ok := strings.Cut(line, ": ")
	if !ok {
		return line
	}
	tail := ""
	if j := strings.IndexByte(url, ' '); j >= 0 {
		url, tail = url[:j], url[j:]
	}
	// ⚠ THE PATH ENDS AT `?`. Running after redactQuery, this first version took
	// the whole redacted query as one more path segment and replaced it with a
	// single length -- destroying the parameter NAMES that structural redaction
	// exists to keep. Caught by the fixture that pins them.
	if k := strings.IndexByte(url, '?'); k >= 0 {
		url, tail = url[:k], url[k:]+tail
	}
	start := 0
	if i := strings.Index(url, "://"); i >= 0 {
		start = i + 3
	} else if strings.HasPrefix(url, "//") {
		start = 2
	}
	if start > 0 {
		k := strings.IndexByte(url[start:], '/')
		if k < 0 {
			return line
		}
		start += k
	}
	segs := strings.Split(url[start:], "/")
	for i, seg := range segs {
		if seg != "" {
			segs[i] = tokenPlaceholder(seg)
		}
	}
	return head + ": " + url[:start] + strings.Join(segs, "/") + tail
}

// structuralRedact applies the rules a field NAME selects, for fields whose
// values are server-generated and therefore unreachable by any scrub built from
// values THIS program supplied.
//
// ⚠ IT IS A FUNCTION BECAUSE IT HAS TWO CALL SITES. The header block had these
// rules and the trailer block did not, so the same `Set-Cookie` was redacted in
// one position and published in the other (shardpilot/shardpilot-go#73 review).
// A trailer is a header that arrived late; it is not a different kind of secret.
func structuralRedact(line string) string {
	low := strings.ToLower(line)
	switch {
	case strings.HasPrefix(low, "set-cookie:"):
		return redactSetCookie(line)
	case strings.HasPrefix(low, "location:"):
		return redactTarget(line)
	}
	return line
}

func redactSetCookie(line string) string {
	cr := ""
	body := line
	if strings.HasSuffix(body, "\r") {
		cr, body = "\r", strings.TrimSuffix(body, "\r")
	}
	head, rest, ok := strings.Cut(body, ":")
	if !ok {
		return line
	}
	pair, attrs, hasAttrs := strings.Cut(rest, ";")
	name, value, hasValue := strings.Cut(strings.TrimSpace(pair), "=")
	if !hasValue {
		return line
	}
	out := head + ": " + name + "=" + placeholder(value)
	if hasAttrs {
		out += ";" + attrs
	}
	return out + cr
}

// mintedBodyKeys are JSON fields the SERVER mints. They are the fact lane's
// subject identifier and its privacy boundary, defined as such in
// experiments.go -- and being server-minted, they are exactly what a scrub
// built from supplied values cannot see (shardpilot/shardpilot-go#73 review).
var mintedBodyKeys = regexp.MustCompile(
	`"(subject_fact_key|subject_key_hash)"(\s*:\s*)"([^"]*)"`)

// redactMintedBody replaces server-minted identifiers in a JSON body with a
// length-preserving placeholder, structurally -- by FIELD NAME, the only handle
// that exists when the value itself is unknown to this program.
func redactMintedBody(body string) string {
	return mintedBodyKeys.ReplaceAllStringFunc(body, func(m string) string {
		g := mintedBodyKeys.FindStringSubmatch(m)
		// ⚠ NO OUTER `marked`: placeholder ALREADY returns one. Wrapping it made
		// adjacent nested provenance marks, `overCaptured` then read the
		// placeholder itself as captured text and rewrote it, and the output was
		// `<<redacted, 8 chars>, 21 chars>` (shardpilot/shardpilot-go#73 review).
		return `"` + g[1] + `"` + g[2] + `"` + placeholder(g[3]) + `"`
	})
}

// respSection is the response section's prose, named rather than inlined so the
// fixture that checks what it CLAIMS reads the same bytes the report prints. A
// test carrying its own copy of the sentence would pass while the report says
// something else (shardpilot/shardpilot-go#73 review).
const respSection = "## Response%s — header block re-serialised by " +
	"`httputil.DumpResponse`\n\nThe status line is as received. The BODY is " +
	"the received bytes with supplied values and server-minted subject keys " +
	"replaced by their lengths, and the two reserved marker bytes written as " +
	"`\\x00` and `\\x01` — a pre-existing spelling of either carries one extra " +
	"backslash, so the substitution stays reversible. What is below is " +
	"therefore a REDACTED capture, not a transcript; saying otherwise while " +
	"printing placeholders is the artifact contradicting itself. The " +
	"HEADER block is written back out by `net/http`, which can add what it " +
	"would send rather than what arrived — `Connection: close` appears on a " +
	"bodyless dump and is forbidden in HTTP/2, so a header here is not " +
	"evidence that it was received.\n\n%s\n%s\n"

func dropFraming(dump string) string {
	lines := strings.Split(dump, "\n")
	out := make([]string, 0, len(lines))
	inHeaders := true
	bodyStart := -1
	for _, l := range lines {
		if inHeaders && strings.TrimRight(l, "\r") == "" {
			inHeaders = false
		}
		if !inHeaders {
			if bodyStart < 0 {
				bodyStart = len(out)
			}
			out = append(out, l)
			continue
		}
		low := strings.ToLower(l)
		cr := ""
		if strings.HasSuffix(l, "\r") {
			cr = "\r"
		}
		if strings.HasPrefix(low, "content-length:") {
			out = append(out, "X-Capture-Note: Content-Length removed — the body below is redacted"+cr)
			continue
		}
		// ⚠ A REDIRECT TARGET IS A CREDENTIAL SURFACE. `Location` query values are
		// server-generated -- `state`, signed tokens, one-time callbacks -- so no
		// list of values THIS program supplied can reach them, exactly as with
		// Set-Cookie (shardpilot/shardpilot-go#73 review). Redacted structurally,
		// by the same function the request line uses: names kept, values lengthed.
		if strings.HasPrefix(low, "location:") {
			// ⚠ AND THE FRAGMENT, NOT ONLY THE QUERY. An OAuth-style redirect
			// carries its credential after `#` -- `#access_token=…` never reaches
			// the server and is exactly the value a capture must not publish, and
			// `redactQuery` saw only `?` (shardpilot/shardpilot-go#73 review).
			out = append(out, redactTarget(strings.TrimSuffix(l, "\r"))+cr)
			continue
		}
		// ⚠ AND A HEADER NAME CAN CARRY THE IDENTIFIER. `X-<key>: v` published it
		// in the name, and the guard's short-value rule counts `-` as a word byte
		// so the boundary check waved it through. The trailer path already did
		// this; the response's own header block did not -- fixed where it was
		// found and not where the question is asked, one more time.
		// net/http hands us the DECODED body, so a recorded `Transfer-Encoding:
		// chunked` describes framing the bytes below do not carry: no chunk sizes,
		// no terminator. Removing Content-Length alone left this common shape
		// unparsable (shardpilot/shardpilot-go#73 review).
		// A COOKIE THE SERVER SET IS A CREDENTIAL THIS PROGRAM NEVER SUPPLIED,
		// so no list of supplied values can reach it. Session, affinity and
		// bot-management cookies were published verbatim in an artifact whose
		// whole purpose is to be published (shardpilot/shardpilot-go#73 review).
		// Redacted STRUCTURALLY, like the query string: the cookie's name and
		// its attributes stay, the value becomes a length.
		if strings.HasPrefix(low, "set-cookie:") {
			out = append(out, redactSetCookie(l))
			continue
		}
		if strings.HasPrefix(low, "transfer-encoding:") {
			out = append(out, "X-Capture-Note: Transfer-Encoding removed — the body below is decoded"+cr)
			continue
		}
		// ⚠ AND A HEADER NAME CAN CARRY THE IDENTIFIER, so this comes LAST: the
		// specific rules above own their lines, and everything else keeps its
		// value untouched while its NAME is scrubbed. `X-<key>: v` published the
		// identifier in the name, and the guard's short-value rule counts `-` as
		// a word byte so the boundary check waved it through. The trailer path
		// already did this; the response's own header block did not -- fixed
		// where it was found and not where the question is asked, once more
		// (shardpilot/shardpilot-go#73 review).
		if i, ok := headerNameEnd(l); ok {
			out = append(out, scrubHeaderName(l[:i])+l[i:])
			continue
		}
		out = append(out, l)
	}
	// ⚠ THE WHOLE BODY, NOT ONE LINE AT A TIME. JSON may put a newline between a
	// field's colon and its value, so `"subject_fact_key":\n"sfk1_..."` matched
	// nothing while the pattern that permits the whitespace sat right there --
	// and the value is server-minted, so the guard behind this cannot see it
	// either (shardpilot/shardpilot-go#73 review).
	if bodyStart < 0 {
		return strings.Join(out, "\n")
	}
	return strings.Join(out[:bodyStart], "\n") + "\n" +
		redactMintedBody(strings.Join(out[bodyStart:], "\n"))
}

type recorder struct {
	inner     http.RoundTripper
	exchanges []exchange
}

// last is the attempt whose verdict the program reports. It is the last one
// BECAUSE that is the one the SDK acted on, not because the others did not
// happen -- every one of them is printed.
func (r *recorder) last() *exchange {
	if len(r.exchanges) == 0 {
		return nil
	}
	return &r.exchanges[len(r.exchanges)-1]
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	ex := exchange{}
	// EVERY QUERY VALUE THE SDK SENDS JOINS THE SCRUB SET, not only the values
	// this program supplied from the environment.
	//
	// ⚠ THE SUBJECT KEY IS MINTED INSIDE THE SDK and was in neither list, so a
	// redirect Location or a diagnostic body echoing the request URL published
	// `subject_key=spcid_...` verbatim -- past the response scrub AND past the
	// publication gate, both of which read the same incomplete list
	// (shardpilot/shardpilot-go#73 review). The request redactor already treats
	// every query value as identifying; this makes the RESPONSE side agree,
	// which is the property, not a longer list of names.
	for _, vs := range req.URL.Query() {
		for _, v := range vs {
			addSuppliedValue(v)
		}
	}
	if dump, err := httputil.DumpRequestOut(req, true); err == nil {
		ex.req = redact([]byte(escapeMarks(string(dump))))
	}

	resp, err := r.inner.RoundTrip(req)
	if err != nil {
		// DNS, TLS, connection setup: the request was formed but nothing came
		// back. Recorded as an attempt WITH NO RESPONSE rather than dropped,
		// because a recorder that keeps the request and loses the failure lets
		// the program print a pair whose second half never existed.
		ex.transErr = err
		r.exchanges = append(r.exchanges, ex)
		return nil, err
	}

	// TEE, DO NOT PRE-READ. Draining the body here and handing back a copy
	// changed the subject twice over: returning `nil, readErr` on a truncated
	// body made net/http report a PRE-response transport failure, so the SDK
	// saw status 0 and could classify a truncated 401 as transient rather than
	// fail-closed -- the recorder altering the verdict it claims to observe.
	// And an unbounded ReadAll drained past the SDK's own io.LimitReader, so a
	// body larger than its 1 MiB contract was buffered here instead of being
	// refused there. The SDK now reads its own response, under its own bound,
	// and this records what passes through.
	captured := &teeBody{inner: resp.Body, resp: resp, declared: resp.ContentLength}
	resp.Body = captured
	ex.status = resp.StatusCode
	ex.proto = resp.Proto
	if d, derr := httputil.DumpResponse(resp, false); derr == nil {
		ex.head = d
	}
	ex.captured = captured
	r.exchanges = append(r.exchanges, ex)
	return resp, nil
}

// teeBody hands every byte to the SDK unchanged and keeps a bounded copy. The
// bound is this recorder's own, not the SDK's: a capture is a record, and a
// record that can be made to allocate without limit by the thing it observes is
// a denial of service wearing evidence.
type teeBody struct {
	inner      io.ReadCloser
	buf        bytes.Buffer
	err        error
	overflowed bool
	atCeiling  bool
	declared   int64          // Content-Length, or -1 when unknown
	resp       *http.Response // for the trailer snapshot at EOF
	trailer    http.Header
	sawEOF     bool
}

// The SDK's own read ceiling, matched exactly.
//
// ⚠ AND THAT IS WHY COUNTING CANNOT DETECT OVERFLOW. An earlier round set this
// one byte above 1 MiB so `room < n` could witness a body larger than the copy.
// It cannot: the SDK reads through io.LimitReader(body, ceiling), so this
// wrapper is never called again once the ceiling is reached, `room < n` never
// becomes true, and a response the SDK itself truncated was labelled complete
// (shardpilot/shardpilot-go#73 review).
//
// A recorder downstream of a limit cannot see past it. So the honest signal is
// not "overflowed" but "AT THE CEILING, therefore INDETERMINATE": the body may
// be whole and exactly this long, or the SDK's read may have stopped short.
// Both are reported as an incomplete capture, because a record cannot tell them
// apart and guessing is the defect.
const capturedBodyMax = (1 << 20) + 1

func (t *teeBody) Read(p []byte) (int, error) {
	n, err := t.inner.Read(p)
	if n > 0 {
		room := capturedBodyMax - t.buf.Len()
		if room > n {
			room = n
		}
		if room > 0 {
			t.buf.Write(p[:room])
		}
		if room < n {
			t.overflowed = true
		}
		if t.buf.Len() >= capturedBodyMax {
			// Reached the ceiling the SDK also reads to. Whether anything
			// followed is UNKNOWABLE from here -- see capturedBodyMax.
			t.atCeiling = true
		}
	}
	if err == io.EOF && !t.sawEOF {
		t.sawEOF = true
		// TRAILERS ARRIVE WITH THE LAST CHUNK, NOT WITH THE HEAD. The response
		// head was dumped before the SDK read anything, when resp.Trailer held
		// only DECLARED keys and no values; a record built from that head
		// announced `Trailer: X` and then omitted X entirely, while still
		// calling the pair complete (shardpilot/shardpilot-go#73 review).
		if t.resp != nil && len(t.resp.Trailer) > 0 {
			t.trailer = t.resp.Trailer.Clone()
		}
	}
	if err != nil && err != io.EOF {
		t.err = err
	}
	return n, err
}

func (t *teeBody) Close() error { return t.inner.Close() }

// redact removes identifying VALUES from the recorded request while keeping
// every NAME and every length.
//
// WHY BY PROPERTY AND NOT BY A LIST OF FIELDS. The first version of this
// program redacted the bearer here and left the query string whole, and the
// identifying query parameters were then shortened BY HAND when the pair was
// pasted into a record -- three of four. The fourth, `subject_key`, was missed,
// and a 38-character subject identifier was published.
//
// A hand-maintained list of fields to shorten does not survive a fifth
// parameter, and neither would a list inside this function. So the rule is a
// PROPERTY: every query-parameter value is treated as identifying and replaced
// by its length. Names and lengths survive, because what this capture proves is
// that the SDK built the right route with the right parameters -- never what any
// particular subject was.
func redactQuery(line string) string {
	i := strings.IndexByte(line, '?')
	if i < 0 {
		return line
	}
	head, rest := line[:i+1], line[i+1:]
	tail := ""
	if j := strings.IndexByte(rest, ' '); j >= 0 {
		rest, tail = rest[:j], rest[j:]
	}
	return head + redactPairs(rest, "") + tail
}

// redactPairs redacts a form-encoded component list: names kept, values replaced
// by their length. `opaqueNameBytes` names bytes that CANNOT appear in a name in
// this context -- a component carrying one is not a name/value pair at all, so it
// is replaced whole.
func redactPairs(rest, opaqueNameBytes string) string {
	parts := strings.Split(rest, "&")
	for k, p := range parts {
		eq := strings.IndexByte(p, '=')
		opaque := eq < 0
		if !opaque && opaqueNameBytes != "" {
			opaque = strings.ContainsAny(p[:eq], opaqueNameBytes)
		}
		if opaque {
			// ⚠ NO `=` MEANS NO NAME TO KEEP. `?server-secret-token` is a legal
			// query component and a perfectly good place for a server-generated
			// credential, which no list of supplied values can reach -- and this
			// branch let it through untouched (shardpilot/shardpilot-go#73
			// review). Replaced whole, as redactFragment already does for the
			// same shape.
			if p != "" {
				parts[k] = tokenPlaceholder(p)
			}
			continue
		}
		// URL-SAFE, no spaces. A request line is space-delimited, so the readable
		// `<redacted, N chars>` form turned the recorded request into something no
		// HTTP parser accepts -- and being parseable is the reason this artifact is
		// kept (shardpilot/shardpilot-go#73 review).
		// ⚠ MEASURE THE VALUE, NOT ITS WIRE SPELLING. A percent-encoded parameter
		// is longer than the identifier it carries -- `a"b` travels as `a%22b` --
		// so measuring the raw segment printed `redacted-5-chars` for a
		// three-character key, while the same key in an echoed body printed
		// `<redacted, 3 chars>`. Two lengths for one value in one capture is
		// evidence that contradicts itself (shardpilot/shardpilot-go#73 review).
		raw := p[eq+1:]
		n := utf8.RuneCountInString(raw)
		if dec, err := url.QueryUnescape(raw); err == nil {
			// COUNT CHARACTERS: the placeholder says "chars", and `len` says
			// bytes -- `%C3%A9` decoded to one character reported as two
			// (shardpilot/shardpilot-go#73 review).
			n = utf8.RuneCountInString(dec)
		}
		parts[k] = p[:eq+1] + marked(fmt.Sprintf("redacted-%d-chars", n))
	}
	return strings.Join(parts, "&")
}

func redact(dump []byte) []byte {
	out := make([]string, 0, 32)
	for _, line := range strings.Split(string(dump), "\n") {
		if strings.HasPrefix(line, "GET ") || strings.HasPrefix(line, "POST ") {
			line = redactQuery(line)
		}
		// The header's PRESENCE is kept -- an absent Authorization header is
		// itself a client-profile defect this capture exists to detect, so
		// replacing the line entirely would hide the failure it is meant to show.
		if strings.HasPrefix(strings.ToLower(line), "authorization:") {
			field := strings.SplitN(line, " ", 2)
			scheme := "<redacted>"
			if len(field) == 2 {
				if parts := strings.SplitN(strings.TrimSpace(field[1]), " ", 2); len(parts) == 2 {
					scheme = parts[0] + " " + placeholder(parts[1])
				}
			}
			// KEEP THE LINE'S OWN TERMINATOR. Splitting on "\n" leaves the
			// "\r" on every line; rebuilding this one without it emitted a lone
			// LF after Authorization while its neighbours stayed CRLF, so the
			// block advertised as a canonical HTTP/1.1 request had mixed line
			// endings (shardpilot/shardpilot-go#73 review).
			cr := ""
			if strings.HasSuffix(line, "\r") {
				cr = "\r"
			}
			line = "Authorization: " + scheme + cr
		}
		out = append(out, line)
	}
	return []byte(strings.Join(out, "\n"))
}

// captureDeadline bounds the SDK, its HTTP client and this program's context
// alike, so a run cannot end on a limit none of them was given.
const captureDeadline = 30 * time.Second

// sanitize redacts any request URL an error carries. `url.Error` wraps the FULL
// url, so printing a deadline or transport failure verbatim republished the
// unredacted subject_key and every targeting value -- the same leak the request
// dump had already been fixed for, arriving by a second road
// (shardpilot/shardpilot-go#73 review).
func sanitize(err error) string {
	if err == nil {
		return ""
	}
	return scrubSupplied(sanitizeText(err.Error()))
}

// sanitizeText redacts every query string in a piece of text, scanning FORWARD
// past each replacement.
//
// The first version restarted from the beginning after each substitution, and
// the `?` it had just processed was still there -- so it selected the same
// marker again, and because the inserted text contains a space the next segment
// was shorter each time while the string grew. On the very case this exists for,
// a url.Error carrying a query, it never terminated
// (shardpilot/shardpilot-go#73 review).
func sanitizeText(out string) string {
	var b strings.Builder
	for {
		i := strings.Index(out, "?")
		if i < 0 {
			b.WriteString(out)
			return b.String()
		}
		seg := out[i:]
		if end := strings.IndexAny(seg, " \""); end >= 0 {
			seg = seg[:end]
		}
		red := strings.TrimPrefix(redactQuery("X "+seg), "X ")
		b.WriteString(out[:i])
		b.WriteString(red)
		out = out[i+len(seg):]
	}
}

func env(name string) string { return strings.TrimSpace(os.Getenv(name)) }

// suppliedValues are the identifying values this program handed the SDK. The
// response echoes them -- an assignment body carries app_key, environment_key
// and experiment_key -- so a 200 capture would republish through the RESPONSE
// what the request dump redacts.
//
// Stated as a PROPERTY rather than as a list of echo fields: a value this
// program supplied is never printed back, wherever it appears. A list of fields
// to scrub is the same mistake as a list of query parameters to shorten, one
// surface over, and this is the fourth road the same values have taken --
// request line, error text, and now response body.
var suppliedValues []string

func longestFirst(vs []string) []string {
	out := append([]string(nil), vs...)
	sort.SliceStable(out, func(i, j int) bool { return len(out[i]) > len(out[j]) })
	return out
}

func addSuppliedValue(v string) {
	if v == "" {
		return
	}
	if slices.Contains(suppliedValues, v) {
		return
	}
	suppliedValues = append(suppliedValues, v)
}

// scrubSupplied replaces every supplied value, and NEVER reaches inside text
// this program generated.
//
// ⚠ IT USED TO. `dropFraming` writes a marked `redacted-3-chars`, and a supplied
// identifier of `redacted` then had its own substring replaced INSIDE that
// placeholder -- producing nested marks that `genSpan` pairs wrongly, so
// `assertNoLeak` reported a survivor and every such capture exited 4 without
// publishing (shardpilot/shardpilot-go#73 review). The marks already record
// which text is ours; the scrub simply was not consulting them.
func scrubSupplied(text string) string {
	return overCaptured(text, scrubSuppliedRaw)
}

// overCaptured applies `f` to the parts of `text` this program did NOT generate,
// leaving marked spans exactly as they are.
func overCaptured(text string, f func(string) string) string {
	var b strings.Builder
	rest := text
	for {
		i := strings.Index(rest, genMark)
		if i < 0 {
			b.WriteString(f(rest))
			return b.String()
		}
		j := strings.Index(rest[i+len(genMark):], genMark)
		if j < 0 {
			b.WriteString(f(rest))
			return b.String()
		}
		end := i + len(genMark) + j + len(genMark)
		b.WriteString(f(rest[:i]))
		b.WriteString(rest[i:end])
		rest = rest[end:]
	}
}

func scrubSuppliedRaw(text string) string {
	// ⚠ LONGEST FIRST. With `abcdefgh` supplied before `abcdefghi`, the shorter
	// value replaced its own prefix inside the longer one, leaving
	// `<redacted, 8 chars>i` -- a published suffix AND a wrong length, and the
	// guard no longer recognised what was left (shardpilot/shardpilot-go#73
	// review). Order is not a detail here: a substitution that destroys a longer
	// match is unrecoverable by any later pass.
	for _, v := range longestFirst(suppliedValues) {
		if v == "" {
			continue
		}
		text = replaceValue(text, v)
		// AND ITS JSON-ESCAPED FORM. A response serialises `a"b` as `a\"b`, so a
		// search for the literal value finds nothing and the identifier is
		// printed reconstructably in the body this program calls publishable.
		// Same for backslashes and \uXXXX forms -- strconv.Quote produces what
		// encoding/json would write (shardpilot/shardpilot-go#73 review).
		// ⚠ MATCH ON THE SPELLING, MEASURE THE VALUE. Passing the encoded form to
		// replaceValue made the placeholder describe the ENCODING: `a"b` is
		// serialised as `a\"b` and was reported as `<redacted, 4 chars>` for a
		// three-character identifier. The same defect as the request-query wire
		// length, arriving in the response path (shardpilot/shardpilot-go#73
		// review). One value has one length wherever it is printed.
		for _, enc := range encodingsOf(v) {
			text = replaceTokenWith(text, enc, placeholder(v), isWordByte)
		}
	}
	return text
}

// encodingsOf returns the spellings of v this program can CONSTRUCT. It is
// deliberately not presented as complete -- see assertNoLeak, which is what
// makes the incompleteness safe.
//
// The Go-quoted form alone was not enough: a comment here claimed strconv.Quote
// "produces what encoding/json would write", and it does not. encoding/json
// escapes `<`, `>` and `&` as \u003c, \u003e, \u0026 by default, so an
// experiment key `a<b` survived both passes and was printed reconstructably in
// the body this program calls publishable. A response echoing the request URL
// in a Location header carries a third spelling again, percent-encoded
// (shardpilot/shardpilot-go#73 review).
func encodingsOf(v string) []string {
	var out []string
	add := func(e string) {
		if e != "" && e != v {
			out = append(out, e)
		}
	}
	if q := strconv.Quote(v); len(q) >= 2 {
		add(q[1 : len(q)-1])
	}
	if j, err := json.Marshal(v); err == nil && len(j) >= 2 {
		add(string(j[1 : len(j)-1]))
	}
	add(url.QueryEscape(v))
	add(url.PathEscape(v))
	return out
}

// assertNoLeak is the reason the list above does not have to be complete.
//
// Every previous round closed ONE more spelling: the literal, then the Go
// escape, then the JSON \uXXXX form, then percent-encoding. A list that gains
// an entry per round is the wrong shape of defence for a publishable artifact,
// because the round that misses one publishes it silently. So the artifact is
// checked before it is printed: the text is DECODED -- percent-escapes and
// \uXXXX escapes undone -- and every supplied value must be absent from every
// decoded form. An encoding nobody anticipated does not leak; it makes this
// program refuse to emit the record at all.
// mark wraps text this program GENERATED in a byte captured data cannot carry.
//
// ⚠ TELLING GENERATED FROM CAPTURED IS THE WHOLE PROBLEM, and two rounds tried
// to solve it by SHAPE. First the leak check masked everything matching a
// placeholder pattern -- and swallowed a supplied value that legally looked like
// one. Then masking was disabled whenever any supplied value looked like a
// placeholder -- so every generated placeholder became a "leak" and a run with
// `SP_EXPERIMENT_KEY=redacted-38-chars` exited 4 without publishing anything
// (shardpilot/shardpilot-go#73 review, once in each direction).
//
// Shape cannot answer it: a placeholder and a value that resembles one are the
// same string. PROVENANCE can. Every placeholder this program writes is wrapped
// in NUL, every NUL is stripped from captured text before redaction begins, and
// the marks are removed at the moment of printing. A NUL in the finished report
// is therefore ours by construction, and the check ignores exactly what we wrote
// and nothing else.
// TWO marks, because the report has three kinds of text and the check concerns
// exactly one of them.
//
// ⚠ ONE MARK WAS NOT ENOUGH, and its two failures were both mine. Marking only
// GENERATED PLACEHOLDERS left the recorder's own PROSE unmarked and
// indistinguishable from captured content, so `SP_EXPERIMENT_KEY=assignment`
// made the heading `# assignment capture` read as a leak and refused every run.
// And reserving NUL forced me to DELETE it from captured bodies, so a response
// legitimately containing one was published altered -- a recorder changing the
// bytes it exists to record (shardpilot/shardpilot-go#73 review).
//
// So: captured text is wrapped in `capturedMark`, placeholders inside it in
// `genMark`, and the check reads captured spans minus generated ones. Prose
// carries no mark and is therefore outside the question by construction rather
// than by a rule. Captured bytes are ESCAPED, not dropped.
const capturedMark = "\x00"
const genMark = "\x01"

func marked(s string) string { return genMark + s + genMark }

// ⚠ ONE MEASURE, ONE PLACE. Seven sites produced a "chars" placeholder and each
// counted for itself; TWO of them were still counting bytes when the label said
// characters, and the same defect was fixed twice in two different functions
// before it was seen as a set (shardpilot/shardpilot-go#73 review). A quantity
// with several independent implementations diverges by construction -- the
// question is only which copy is found next.
//
// `placeholder` is the prose form and `tokenPlaceholder` the one legal inside an
// HTTP field name. Both mark themselves as generated, and both count runes,
// because "chars" is what they say.
func chars(v string) int { return utf8.RuneCountInString(v) }

func placeholder(v string) string {
	return marked(fmt.Sprintf("<redacted, %d chars>", chars(v)))
}

func tokenPlaceholder(v string) string {
	return marked(fmt.Sprintf("redacted-%d-chars", chars(v)))
}

// asCaptured delimits text that came from the wire. ⚠ IT DOES NOT ESCAPE:
// escaping belongs on the RAW bytes, before redaction inserts its own marks --
// doing it here escaped those too, so the guard read its own placeholders as
// captured content and refused every run. The order is the whole of it:
// escapeMarks -> redact -> asCaptured.
func asCaptured(s string) string { return capturedMark + s + capturedMark }

var capturedSpan = regexp.MustCompile(capturedMark + "[^" + capturedMark + "]*" + capturedMark)
var genSpan = regexp.MustCompile(genMark + "[^" + genMark + "]*" + genMark)

// escapeMarks keeps captured bytes rather than deleting them: a body may legally
// contain either marker byte, and the artifact must still hold what arrived. The
// escape is visible and reversible; deletion was neither.
// escapeMarks replaces the two reserved marker bytes with readable text.
//
// ⚠ IT MUST BE INJECTIVE, AND IT WAS NOT. A response containing an actual NUL
// and a response containing the four literal bytes `\x00` both rendered as
// `\x00`, so the artifact could not say which the SDK received -- and this
// function's whole claim is that the substitution is reversible
// (shardpilot/shardpilot-go#73 review). Pre-existing spellings are lengthened by
// one backslash FIRST, so `\x00` from the wire becomes `\\x00` and only a real
// NUL produces the single-backslash form.
//
// ⚠ ONLY THE RESERVED SPELLINGS ARE TOUCHED. Escaping every backslash would
// rewrite `\uXXXX` and `\xNN` as well, and the guard's decoders would then fail
// to reconstruct an identifier they currently catch -- an injective escape that
// blinds the leak check is a worse trade than the ambiguity it fixes.
func escapeMarks(s string) string {
	s = strings.ReplaceAll(s, `\x00`, `\\x00`)
	s = strings.ReplaceAll(s, `\x01`, `\\x01`)
	s = strings.ReplaceAll(s, capturedMark, `\x00`)
	return strings.ReplaceAll(s, genMark, `\x01`)
}

func stripMarks(s string) string {
	s = strings.ReplaceAll(s, capturedMark, "")
	return strings.ReplaceAll(s, genMark, "")
}

func assertNoLeak(text string) error {
	// ⚠ MASK ONLY WHAT CANNOT BE A VALUE. Blanking every placeholder shape hid a
	// supplied value that HAPPENS to look like one: `SP_EXPERIMENT_KEY=redacted-38-chars`
	// is a legal key, it is printed verbatim in the canonical request, and the
	// mask erased it before the check (shardpilot/shardpilot-go#73 review). A
	// mask that can swallow the thing it is protecting is worse than no mask.
	// Only what was CAPTURED is in question; prose and placeholders are ours.
	var captured strings.Builder
	for _, span := range capturedSpan.FindAllString(text, -1) {
		captured.WriteString(genSpan.ReplaceAllString(span, " "))
		captured.WriteString("\n")
	}
	text = captured.String()
	// DECODE TO A FIXED POINT. One pass left `a%2522b` -- the ordinary shape when
	// a URL is embedded in another URL's parameter -- decoding only to `a%22b`,
	// which matches no supplied value, so a doubly-encoded identifier walked
	// through both the scrub and this gate (shardpilot/shardpilot-go#73 review).
	// Nesting has no fixed depth, so neither does the decoder: it iterates until
	// nothing changes, with a bound so a crafted body cannot spin it.
	// ⚠ THE BOUND IS THE STRING, NOT A MAGIC NUMBER. This capped at 16 rounds
	// while its own comment promised a fixed point, so wrapping a value in
	// seventeen `%25` layers walked straight through the gate
	// (shardpilot/shardpilot-go#73 review). Each round either shrinks the text
	// -- `%XX` becomes one byte, `\uXXXX` at most four -- or rewrites `+` as a
	// space, which cannot be undone, so it cannot cycle and cannot run longer
	// than the input. len(text)+1 is therefore a real bound rather than a guess,
	// and reaching it is a defect worth failing on rather than passing quietly.
	// ⚠ AND THE BUDGET IS WORK, NOT DEPTH. `len(text)+1` bounds the rounds but
	// not the cost: a near-limit body whose escape is nested `%25` deep peels one
	// two-byte layer per pass while rescanning and re-allocating almost the whole
	// report, which is quadratic and lets a crafted response hang this program
	// instead of being refused by it (shardpilot/shardpilot-go#73 review). The
	// budget counts bytes examined and fails CLOSED.
	const decodeWorkMax = 64 << 20
	work := 0
	forms := []string{text}
	cur := text
	settled := false
	for i := 0; i <= len(text); i++ {
		work += len(cur)
		if work > decodeWorkMax {
			return fmt.Errorf(
				"decoding exceeded its work budget (%d bytes examined); the record "+
					"is NOT publishable and was not printed", work)
		}
		// ⚠ EACH STAGE, NOT ONLY THE ROUND. Composing the four decoders meant the
		// intermediate forms never existed to be checked: `%61bcdefghi%2Bj`
		// percent-decodes to the supplied `abcdefghi+j` and `undoPlus` turned it
		// into `abcdefghi j` inside the same expression, so the one form that
		// matched was never retained (shardpilot/shardpilot-go#73 review).
		for _, stage := range []func(string) string{undoPercent, undoUnicodeEscapes, undoBase64, undoPlus, undoEntities} {
			cur = stage(cur)
			forms = append(forms, cur)
		}
		next := cur
		// ⚠ THE INDEX IS THE STAGE COUNT PLUS ONE, and it is a stage count, not a
		// constant: it must name the form this round STARTED from. Adding a
		// decoder without moving it would compare against a mid-round form and
		// settle early.
		if next == forms[len(forms)-6] {
			settled = true
			break
		}
		forms = append(forms, next)
		cur = next
	}
	if !settled {
		return fmt.Errorf(
			"decoding did not settle within %d rounds; the record is NOT "+
				"publishable and was not printed", len(text)+1)
	}
	for _, v := range suppliedValues {
		if v == "" {
			continue
		}
		for _, f := range forms {
			if containsValue(f, v) {
				return fmt.Errorf(
					"a supplied value of %d characters survived redaction in some "+
						"encoding; the record is NOT publishable and was not printed",
					chars(v))
			}
		}
	}
	return nil
}

// undoPercent decodes percent-escapes leniently -- url.QueryUnescape refuses a
// whole string for one malformed escape, and a partial decode is exactly what a
// leak check wants.
// undoPlus applies query-string semantics: inside a query, a space is spelled
// `+`. Nesting one URL inside another's parameter turns a supplied `a b` into
// `a%2Bb`, which percent-decoding alone reduces to `a+b` and no further, so the
// value was never reconstructed and both the scrub and the gate missed it
// (shardpilot/shardpilot-go#73 review).
// undoEntities decodes HTML entity spellings. A gateway or validation endpoint
// answering with an HTML diagnostic writes `a&amp;b` for `a&b`, which no
// percent, unicode or plus decoding reconstructs -- so the value reached the
// artifact while the guard reported it absent
// (shardpilot/shardpilot-go#73 review).
// undoBase64 decodes bounded base64 tokens. A diagnostic body that
// base64-encodes a supplied identifier reconstructs it for anyone who reads the
// artifact, while percent, unicode, plus and entity decoding all leave it
// untouched and the gate reported it absent (shardpilot/shardpilot-go#73
// review).
//
// ⚠ IT RUNS BEFORE undoPlus, WHICH DESTROYS ITS ALPHABET. `+` is a base64 byte
// and undoPlus rewrites it as a space, so ordered the other way this decoder
// would be handed text whose tokens no longer decode.
//
// ⚠ AND IT IS DESTRUCTIVE ON PURPOSE, WHICH IS ONLY SAFE BECAUSE EVERY
// INTERMEDIATE FORM IS KEPT. A supplied value can itself be legal base64 --
// `abcdefgh` is -- so this stage replaces the plain occurrence with the bytes it
// decodes to. The guard checks every retained form, and the form before this
// stage still carries the plain text, so nothing is lost by rewriting it here.
// Garbage produced from a non-base64 run can only ever ADD a match, which fails
// closed.
func undoBase64(text string) string {
	// ⚠ FOUR, NOT EIGHT. A three-character key is legal and `bar` travels as the
	// four-byte `YmFy`, which an eight-byte floor skipped entirely -- the guard
	// settled and approved a reconstructable identifier
	// (shardpilot/shardpilot-go#73 review). Four is the smallest token that
	// encodes anything; below it there is nothing to decode.
	const minToken = 4
	var b strings.Builder
	for i := 0; i < len(text); {
		j := i
		for j < len(text) && isBase64Byte(text[j]) {
			j++
		}
		for j < len(text) && text[j] == '=' {
			j++
		}
		tok := text[i:j]
		if len(tok) < minToken {
			if j == i {
				b.WriteByte(text[i])
				i++
				continue
			}
			b.WriteString(tok)
			i = j
			continue
		}
		if dec, ok := decodeBase64(tok); ok {
			b.WriteString(dec)
		} else {
			b.WriteString(tok)
		}
		i = j
	}
	return b.String()
}

func isBase64Byte(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') || c == '+' || c == '/' || c == '-' || c == '_'
}

// decodeBase64 tries the standard and URL alphabets, padded and unpadded. It
// reports failure rather than a partial decode: half a token tells the guard
// nothing and would only add noise to every later round.
func decodeBase64(tok string) (string, bool) {
	for _, enc := range []*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding,
		base64.URLEncoding, base64.RawURLEncoding,
	} {
		if raw, err := enc.DecodeString(tok); err == nil && utf8.Valid(raw) {
			return string(raw), true
		}
	}
	return "", false
}

func undoEntities(text string) string {
	return html.UnescapeString(text)
}

func undoPlus(text string) string {
	return strings.ReplaceAll(text, "+", " ")
}

func undoPercent(text string) string {
	var b strings.Builder
	for i := 0; i < len(text); i++ {
		if text[i] == '%' && i+2 < len(text) {
			if h, err := strconv.ParseUint(text[i+1:i+3], 16, 8); err == nil {
				b.WriteByte(byte(h))
				i += 2
				continue
			}
		}
		b.WriteByte(text[i])
	}
	return b.String()
}

// undoUnicodeEscapes decodes \uXXXX sequences wherever they appear, without
// requiring the surrounding text to be valid JSON -- a header or a log line may
// carry one outside any JSON document.
func undoUnicodeEscapes(text string) string {
	var b strings.Builder
	for i := 0; i < len(text); i++ {
		if text[i] == '\\' && i+5 < len(text) && (text[i+1] == 'u' || text[i+1] == 'U') {
			if r, err := strconv.ParseUint(text[i+2:i+6], 16, 32); err == nil {
				// A NON-BMP CHARACTER IS SPELLED AS A SURROGATE PAIR, and writing
				// the halves separately yields two replacement runes instead of the
				// character -- so an identifier containing one was invisible to this
				// decoder and to the guard behind it
				// (shardpilot/shardpilot-go#73 review).
				if utf16.IsSurrogate(rune(r)) && i+11 < len(text) &&
					text[i+6] == '\\' && (text[i+7] == 'u' || text[i+7] == 'U') {
					if lo, err2 := strconv.ParseUint(text[i+8:i+12], 16, 32); err2 == nil {
						if dec := utf16.DecodeRune(rune(r), rune(lo)); dec != 0xFFFD {
							b.WriteRune(dec)
							i += 11
							continue
						}
					}
				}
				b.WriteRune(rune(r))
				i += 5
				continue
			}
		}
		// `\u{XXXX}` is the code-point form of the same escape, and a decoder
		// accepting only exactly four hex digits never sees it
		// (shardpilot/shardpilot-go#73 review).
		if text[i] == '\\' && i+3 < len(text) &&
			(text[i+1] == 'u' || text[i+1] == 'U') && text[i+2] == '{' {
			if close := strings.IndexByte(text[i+3:], '}'); close > 0 && close <= 6 {
				if r, err := strconv.ParseUint(text[i+3:i+3+close], 16, 32); err == nil {
					b.WriteRune(rune(r))
					i += 3 + close
					continue
				}
			}
		}
		if text[i] == '\\' && i+3 < len(text) && (text[i+1] == 'x' || text[i+1] == 'X') {
			// A plain-text or JavaScript-style diagnostic spells a byte as \xNN,
			// which no percent, unicode, plus or entity decoding reconstructs -- so
			// `\x61bcdefgh` reached the artifact with every decoding round already
			// settled (shardpilot/shardpilot-go#73 review).
			if v8, err := strconv.ParseUint(text[i+2:i+4], 16, 8); err == nil {
				b.WriteByte(byte(v8))
				i += 3
				continue
			}
		}
		if text[i] == '\\' && i+1 < len(text) {
			switch text[i+1] {
			case '"', '\\', '/':
				b.WriteByte(text[i+1])
				i++
				continue
			}
		}
		b.WriteByte(text[i])
	}
	return b.String()
}

// scrubHeaderName scrubs a header or trailer NAME, where `-` separates words
// rather than belonging to one.
//
// ⚠ scrubSupplied ALONE DOES NOT REACH IT. The short-value rule matches only at
// token boundaries and `isWordByte` counts `-` as a word byte -- correct for a
// VALUE, since an experiment key may legally contain a hyphen, and wrong for a
// NAME, where HTTP uses `-` structurally. So a trailer legally called
// `X-<key>` published the identifier in its name and the boundary check waved
// it through (shardpilot/shardpilot-go#73 review). The rule is not loosened for
// values; the name is split on its own separator first.
// nameSafe is the placeholder used inside a header NAME. `<redacted, 6 chars>`
// carries spaces, a comma and angle brackets, none of which are legal in an HTTP
// field name -- so the scrub that exists to keep a name publishable made the
// message unparsable in exactly the case it handles
// (shardpilot/shardpilot-go#73 review). This is letters, digits and hyphens.
func nameSafe(v string) string {
	return tokenPlaceholder(v)
}

func scrubHeaderName(name string) string {
	// ⚠ SPLITTING FIRST CANNOT MATCH A HYPHENATED VALUE. The previous version cut
	// the name on `-` and scrubbed each piece, so a legal key `foo-bar` matched no
	// component and `X-foo-bar` was published whole
	// (shardpilot/shardpilot-go#73 review). The value is matched against the
	// COMPLETE name under a boundary rule where `-` separates -- which is what
	// the split was reaching for and could not express.
	for _, v := range longestFirst(suppliedValues) {
		if v != "" {
			// ⚠ FOLDED, BECAUSE A FIELD NAME IS CASE-INSENSITIVE AND net/http
			// CANONICALISES IT. A wire header `X-secret` reaches this function as
			// `X-Secret`, so a case-sensitive search missed the supplied `secret`
			// and published it in the name (shardpilot/shardpilot-go#73 review).
			// Folding is applied HERE and not to values, whose case is data.
			name = replaceTokenFold(name, v, nameSafe(v), isNameByte)
		}
	}
	return name
}

// isNameByte is isWordByte without `-`: in a header NAME the hyphen separates
// words, while in a VALUE it may belong to one.
// isNameByte: inside a field NAME, every token punctuation separates words --
// `-` was only the one I had met.
func isNameByte(c byte) bool {
	return isTokenByte(c) && (c == '_' || (c >= '0' && c <= '9') ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))
}

// replaceValue redacts every occurrence of v. A long value is replaced outright;
// a SHORT one only where it stands as a whole token, because the SDK validates
// these fields as non-empty and nothing more -- an experiment key may legally be
// `ab`, and the first version skipped anything under four characters, printing
// exactly those verbatim in the response it calls publishable
// (shardpilot/shardpilot-go#73 review). Blind substring replacement of `ab`
// would instead corrupt unrelated words, so short values are matched at
// boundaries and long ones are not.
// containsValue asks the question replaceValue answers, under the SAME rule:
// a long value counts anywhere, a short one only where it stands as a whole
// token.
//
// ⚠ THE GUARD USED strings.Contains AND THAT MADE IT REFUSE VALID RUNS. The SDK
// accepts any non-empty experiment key, so `SP_EXPERIMENT_KEY=a` made the letter
// `a` in ordinary report prose -- "assignment capture" -- look like a leak, and
// every otherwise good run exited 4 without printing. A guard whose matching is
// stricter than the redaction it checks does not find leaks; it finds itself
// (shardpilot/shardpilot-go#73 review).
func containsValue(text, v string) bool {
	if v == "" {
		return false
	}
	if len(v) >= 8 {
		return strings.Contains(text, v)
	}
	// ⚠ EITHER BOUNDARY CONVENTION COUNTS. A hyphenated value inside a header
	// name -- `foo-bar` in `X-foo-bar` -- has a `-` before it, which the VALUE
	// rule reads as a word byte and so as "not a whole token". The guard must be
	// at least as permissive as every redaction it checks, so it asks under both
	// rules and a hit under either is a hit.
	// ⚠ AND ONLY WHERE NAMES ARE. Asked of the WHOLE report, this convention
	// refused ordinary captured body text: with experiment key `bar`, the JSON
	// `{"reason":"foo-bar-baz"}` needs no redaction at all, but isNameByte reads
	// both hyphens as boundaries and the capture exited 4
	// (shardpilot/shardpilot-go#73 review). The permissive rule exists for field
	// NAMES, where `-` is structural, so it is asked of the field names.
	if containsValueWith(headerNames(text), v, isNameByte) {
		return true
	}
	return containsValueWith(text, v, isWordByte)
}

// headerNames returns just the field NAMES in text, one per line -- the region
// where the hyphen is structural rather than part of a word. Leading whitespace
// is trimmed because the trailer block indents its lines.
func headerNames(text string) string {
	var b strings.Builder
	for _, ln := range strings.Split(text, "\n") {
		// ⚠ STRIP THE MARKS FIRST. The guard reads CAPTURED SPANS, and a span
		// arrives with its provenance marks still attached -- so every line began
		// with a byte that is not a token byte and no line was ever recognised as
		// a header. The fixture that caught it is the one this convention exists
		// for (shardpilot/shardpilot-go#73 review).
		ln = strings.TrimSuffix(strings.TrimSpace(stripMarks(ln)), "\r")
		if i, ok := headerNameEnd(ln); ok {
			b.WriteString(ln[:i])
			b.WriteByte('\n')
		}
	}
	return b.String()
}

func containsValueWith(text, v string, isWord func(byte) bool) bool {
	for i := 0; ; {
		j := strings.Index(text[i:], v)
		if j < 0 {
			return false
		}
		j += i
		startOK := j == 0 || !isWord(text[j-1])
		endOK := j+len(v) >= len(text) || !isWord(text[j+len(v)])
		if startOK && endOK {
			return true
		}
		i = j + 1
	}
}

func replaceValue(text, v string) string {
	return replaceValueWith(text, v, isWordByte)
}

func replaceValueWith(text, v string, isWord func(byte) bool) string {
	// COUNT CHARACTERS: this said "chars" and measured bytes, so a non-ASCII
	// identifier was reported longer than it is -- the same defect the query
	// placeholder had, in the other function (shardpilot/shardpilot-go#73 review).
	return replaceTokenWith(text, v,
		placeholder(v), isWord)
}

// replaceTokenFold is replaceTokenWith under ASCII case folding, for field NAMES
// only. Non-ASCII falls back to the exact form: folding can change a string's
// LENGTH outside ASCII, and an index computed on the folded copy would then cut
// the original in the wrong place -- a redaction that corrupts is worse than one
// that misses, because the miss is still caught by the guard.
func replaceTokenFold(text, v, red string, isWord func(byte) bool) string {
	if !isASCII(text) || !isASCII(v) {
		return replaceTokenWith(text, v, red, isWord)
	}
	lt, lv := strings.ToLower(text), strings.ToLower(v)
	var b strings.Builder
	for {
		i := strings.Index(lt, lv)
		if i < 0 {
			b.WriteString(text)
			return b.String()
		}
		startOK := i == 0 || !isWord(lt[i-1])
		endOK := i+len(lv) >= len(lt) || !isWord(lt[i+len(lv)])
		b.WriteString(text[:i])
		if startOK && endOK {
			b.WriteString(red)
		} else {
			b.WriteString(text[i : i+len(v)])
		}
		text, lt = text[i+len(v):], lt[i+len(lv):]
	}
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

func replaceTokenWith(text, v, red string, isWord func(byte) bool) string {
	if len(v) >= 8 {
		return strings.ReplaceAll(text, v, red)
	}
	var b strings.Builder
	for {
		i := strings.Index(text, v)
		if i < 0 {
			b.WriteString(text)
			return b.String()
		}
		endOK := i+len(v) >= len(text) || !isWord(text[i+len(v)])
		startOK := i == 0 || !isWord(text[i-1])
		b.WriteString(text[:i])
		if startOK && endOK {
			b.WriteString(red)
		} else {
			b.WriteString(v)
		}
		text = text[i+len(v):]
	}
}

func isWordByte(c byte) bool {
	return c == '_' || c == '-' ||
		(c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func main() {
	required := []string{
		"SP_REMOTE_CONFIG_URL", "SP_API_KEY", "SP_WORKSPACE_ID",
		"SP_APP_ID", "SP_ENVIRONMENT_ID", "SP_EXPERIMENT_KEY",
	}
	var missing []string
	for _, name := range required {
		if env(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		fmt.Fprintf(os.Stderr,
			"no request made: %s unset.\nThis harness needs a publishable client "+
				"ingest key (sp_ingest_...) scoped for experiment-assignment reads; "+
				"minting one is an owner action, not this program's.\n",
			strings.Join(missing, ", "))
		os.Exit(2)
	}

	suppliedValues = []string{
		env("SP_API_KEY"), env("SP_WORKSPACE_ID"), env("SP_APP_ID"),
		env("SP_ENVIRONMENT_ID"), env("SP_EXPERIMENT_KEY"),
	}

	rec := &recorder{inner: http.DefaultTransport}
	cfg := shardpilot.Config{
		// The ingest leg is required by the constructor and is NOT exercised
		// here: nothing is tracked and nothing is flushed. It points at the
		// same origin so a misconfiguration cannot silently send events
		// somewhere else.
		IngestURL:          env("SP_REMOTE_CONFIG_URL"),
		Token:              env("SP_API_KEY"),
		WorkspaceID:        env("SP_WORKSPACE_ID"),
		AppID:              env("SP_APP_ID"),
		EnvironmentID:      env("SP_ENVIRONMENT_ID"),
		Source:             shardpilot.SourceClient,
		APIKey:             env("SP_API_KEY"),
		RemoteConfigURL:    env("SP_REMOTE_CONFIG_URL"),
		ExperimentsEnabled: true,
		// Matched to the capture deadline on purpose: leaving the SDK's own
		// timeout at its default lets the two disagree, and a run that ends
		// on whichever fires first records a deadline nobody chose.
		HTTPTimeout: captureDeadline,
		HTTPClient:  &http.Client{Transport: rec, Timeout: captureDeadline},
	}

	client, err := shardpilot.NewClient(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "no request made: %v\n", err)
		os.Exit(2)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = client.Close(closeCtx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), captureDeadline)
	defer cancel()

	result, fetchErr := client.FetchExperimentAssignment(ctx, env("SP_EXPERIMENT_KEY"), nil)

	if len(rec.exchanges) == 0 {
		fmt.Fprintf(os.Stderr,
			"no request made: the SDK returned %v without issuing one, so this "+
				"run says nothing about the endpoint\n", sanitize(fetchErr))
		os.Exit(2)
	}

	var report strings.Builder
	fmt.Fprintf(&report, "# assignment capture — %s\n\n", time.Now().UTC().Format(time.RFC3339))
	if len(rec.exchanges) > 1 {
		fmt.Fprintf(&report, "The SDK made **%d attempts**. All are below; the verdict is the "+
			"last, because that is the one it acted on.\n\n", len(rec.exchanges))
	}
	for i, ex := range rec.exchanges {
		label := ""
		if len(rec.exchanges) > 1 {
			label = fmt.Sprintf(" %d", i+1)
		}
		reqText := asCaptured(string(ex.req))
		// ⚠ NOT "as the SDK sent it". DumpRequestOut serialises the request as
		// HTTP/1.1 through a separate fake transport, BEFORE the real one
		// negotiates a protocol. On an HTTP/2 connection the report paired a
		// fabricated `HTTP/1.1` request line with a genuine `HTTP/2.0` status
		// line and called the two a single wire exchange
		// (shardpilot/shardpilot-go#73 review). The negotiated protocol is
		// reported beside it, from the response, which is measured rather than
		// serialised.
		wire := ex.proto
		if wire == "" {
			wire = "not established — no response arrived"
		}
		fmt.Fprintf(&report,
			"## Request%s — canonical HTTP/1.1 representation\n\n"+
				"Serialised by `httputil.DumpRequestOut`, which always writes HTTP/1.1. "+
				"The connection negotiated **%s**, so this is the request's canonical form "+
				"and its header set, not the bytes on the wire.\n\n%s\n",
			label, wire, fencedBlock(reqText))
		switch {
		case ex.transErr != nil:
			fmt.Fprintf(&report, "## Response%s\n\nNONE — the request was formed and no "+
				"response arrived: %s\n\n", label, sanitize(ex.transErr))
		case ex.truncErr() != nil:
			body := responseText(&ex)
			fmt.Fprintf(&report, "## Response%s — INCOMPLETE, and the SDK was told so\n\n"+
				"The body is not established as whole (%v). What arrived is below; it "+
				"is NOT a complete response.\n\n%s\n%s\n",
				label, sanitize(ex.truncErr()), fencedBlock(body), ex.trailerReport())
		default:
			respText := responseText(&ex)
			fmt.Fprintf(&report, respSection, label,
				fencedBlock(respText), ex.trailerReport())
		}
	}

	last := rec.last()
	fmt.Fprintf(&report, "## SDK verdict\n\n")
	fmt.Fprintf(&report, "    attempts: %d\n", len(rec.exchanges))
	fmt.Fprintf(&report, "    status:   %d\n", last.status)
	fmt.Fprintf(&report, "    assigned: %t\n", result.Assigned)
	fmt.Fprintf(&report, "    protocol: %q\n", last.proto)
	// Scrubbed like everything else: a variant key may legally equal a supplied
	// identifier -- an experiment and a variant both named `control` is a valid
	// response -- and the property is that a supplied value is never printed
	// back WHEREVER it appears, not only in the body.
	fmt.Fprintf(&report, "    variant:  %q\n", stripMarks(scrubSupplied(result.VariantKey)))
	fmt.Fprintf(&report, "    reason:   %q\n", stripMarks(scrubSupplied(result.Reason)))
	// The SDK's own classification. A 404 returns a usable result with
	// Code "not_found", Assigned false and a NIL error, so omitting this showed
	// only zero-valued fields and then called the run generically not-served --
	// losing the first-class verdict this program exists to report.
	fmt.Fprintf(&report, "    code:     %q\n", stripMarks(scrubSupplied(result.Code)))
	fmt.Fprintf(&report, "    version:  %d\n", result.Version)
	if fetchErr != nil {
		fmt.Fprintf(&report, "    error:    %s\n", sanitize(fetchErr))
	}

	// THE ARTIFACT IS CHECKED BEFORE IT IS PUBLISHED, not as it is assembled.
	// One gate over the finished text, so a value that slipped through any one
	// of the scrub passes stops the record instead of riding out in it.
	if err := assertNoLeak(report.String()); err != nil {
		fmt.Fprintf(os.Stderr, "REFUSING TO PRINT: %v\n", err)
		os.Exit(4)
	}
	// A CAPTURE NOBODY RECEIVED IS NOT A CAPTURE. An ignored write error let a
	// report truncated by a full filesystem -- or never written at all -- be
	// followed by "SERVED" and exit 0 (shardpilot/shardpilot-go#73 review).
	if _, werr := io.WriteString(os.Stdout, stripMarks(report.String())); werr != nil {
		fmt.Fprintf(os.Stderr, "REFUSING: the capture could not be written whole: %v\n", werr)
		os.Exit(4)
	}

	switch {
	case last.transErr != nil:
		fmt.Printf("\nNO RESPONSE (exit 3). The SDK formed the request and nothing " +
			"came back, so this run says nothing about what the endpoint would " +
			"have answered.\n")
		os.Exit(3)
	case last.truncErr() != nil:
		fmt.Printf("\nRESPONSE TRUNCATED (exit 3). The SDK read its own response and " +
			"the body did not arrive whole; what it did read is above, and it is not " +
			"a complete answer.\n")
		os.Exit(3)
	case last.status == http.StatusOK && fetchErr == nil && result.Assigned:
		fmt.Printf("\nSERVED. The pair above is the capture.\n")
		os.Exit(0)
	case last.status == http.StatusOK && fetchErr == nil:
		// A supported 200 that assigns nothing: a traffic-gate miss, a targeting
		// mismatch, a kill switch. The exchange is complete and the endpoint
		// refused to assign, which is exit 1 -- exit 0 says an assignment was
		// SERVED (shardpilot/shardpilot-go#73 review).
		fmt.Printf("\nCOMPLETE BUT NOT ASSIGNED (exit 1). The endpoint answered 200 "+
			"and assigned nothing; reason %q, code %q.\n",
			stripMarks(scrubSupplied(result.Reason)), stripMarks(scrubSupplied(result.Code)))
		os.Exit(1)
	case fetchErr != nil && errors.Is(fetchErr, context.DeadlineExceeded):
		fmt.Printf("\nNOT captured (exit 3) — the request timed out.\n")
		os.Exit(3)
	default:
		fmt.Printf("\nNOT served. The SDK reached the endpoint and it answered "+
			"%d; the pair above is what it answered, and it is not a served "+
			"assignment.\n", last.status)
		os.Exit(1)
	}
}
