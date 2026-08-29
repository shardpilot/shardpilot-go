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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"

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
	if e.captured.err == nil && e.captured.atCeiling && !e.captured.sawEOF {
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
			fmt.Fprintf(&b, "    %s: %s\n", k, scrubSupplied(v))
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
func dropFraming(dump string) string {
	lines := strings.Split(dump, "\n")
	out := make([]string, 0, len(lines))
	inHeaders := true
	for _, l := range lines {
		if inHeaders && strings.TrimRight(l, "\r") == "" {
			inHeaders = false
		}
		if !inHeaders {
			out = append(out, l)
			continue
		}
		low := strings.ToLower(l)
		if strings.HasPrefix(low, "content-length:") {
			out = append(out, "X-Capture-Note: Content-Length removed — the body below is redacted")
			continue
		}
		// net/http hands us the DECODED body, so a recorded `Transfer-Encoding:
		// chunked` describes framing the bytes below do not carry: no chunk sizes,
		// no terminator. Removing Content-Length alone left this common shape
		// unparsable (shardpilot/shardpilot-go#73 review).
		if strings.HasPrefix(low, "transfer-encoding:") {
			out = append(out, "X-Capture-Note: Transfer-Encoding removed — the body below is decoded")
			continue
		}
		out = append(out, l)
	}
	return strings.Join(out, "\n")
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
		ex.req = redact(dump)
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
	captured := &teeBody{inner: resp.Body, resp: resp}
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
	parts := strings.Split(rest, "&")
	for k, p := range parts {
		eq := strings.IndexByte(p, '=')
		if eq < 0 {
			continue
		}
		// URL-SAFE, no spaces. A request line is space-delimited, so the readable
		// `<redacted, N chars>` form turned the recorded request into something no
		// HTTP parser accepts -- and being parseable is the reason this artifact is
		// kept (shardpilot/shardpilot-go#73 review).
		parts[k] = p[:eq+1] + fmt.Sprintf("redacted-%d-chars", len(p[eq+1:]))
	}
	return head + strings.Join(parts, "&") + tail
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
					scheme = parts[0] + " <redacted, " + fmt.Sprint(len(parts[1])) + " chars>"
				}
			}
			line = "Authorization: " + scheme
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

func addSuppliedValue(v string) {
	if v == "" {
		return
	}
	if slices.Contains(suppliedValues, v) {
		return
	}
	suppliedValues = append(suppliedValues, v)
}

func scrubSupplied(text string) string {
	for _, v := range suppliedValues {
		if v == "" {
			continue
		}
		text = replaceValue(text, v)
		// AND ITS JSON-ESCAPED FORM. A response serialises `a"b` as `a\"b`, so a
		// search for the literal value finds nothing and the identifier is
		// printed reconstructably in the body this program calls publishable.
		// Same for backslashes and \uXXXX forms -- strconv.Quote produces what
		// encoding/json would write (shardpilot/shardpilot-go#73 review).
		for _, enc := range encodingsOf(v) {
			text = replaceValue(text, enc)
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
// placeholderPattern matches the strings THIS PROGRAM writes where a value used
// to be. They are masked before the leak check, because they are the recorder's
// prose rather than captured data.
//
// ⚠ WITHOUT THIS THE GUARD REJECTED FULLY REDACTED REPORTS. A supplied value of
// `redacted` is replaced in the request line by `redacted-8-chars`, and the
// long-value branch then found `redacted` inside the recorder's OWN placeholder
// and exited 4 without publishing anything -- the same self-detection as the
// short-key case, arriving through the other branch
// (shardpilot/shardpilot-go#73 review).
var placeholderPattern = regexp.MustCompile(`redacted-[0-9]+-chars|<redacted, [0-9]+ chars>`)

func assertNoLeak(text string) error {
	text = placeholderPattern.ReplaceAllString(text, "\x00")
	// DECODE TO A FIXED POINT. One pass left `a%2522b` -- the ordinary shape when
	// a URL is embedded in another URL's parameter -- decoding only to `a%22b`,
	// which matches no supplied value, so a doubly-encoded identifier walked
	// through both the scrub and this gate (shardpilot/shardpilot-go#73 review).
	// Nesting has no fixed depth, so neither does the decoder: it iterates until
	// nothing changes, with a bound so a crafted body cannot spin it.
	forms := []string{text}
	cur := text
	for i := 0; i < 16; i++ {
		next := undoUnicodeEscapes(undoPercent(cur))
		if next == cur {
			break
		}
		forms = append(forms, next)
		cur = next
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
					len(v))
			}
		}
	}
	return nil
}

// undoPercent decodes percent-escapes leniently -- url.QueryUnescape refuses a
// whole string for one malformed escape, and a partial decode is exactly what a
// leak check wants.
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
	for i := 0; ; {
		j := strings.Index(text[i:], v)
		if j < 0 {
			return false
		}
		j += i
		startOK := j == 0 || !isWordByte(text[j-1])
		endOK := j+len(v) >= len(text) || !isWordByte(text[j+len(v)])
		if startOK && endOK {
			return true
		}
		i = j + 1
	}
}

func replaceValue(text, v string) string {
	red := fmt.Sprintf("<redacted, %d chars>", len(v))
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
		endOK := i+len(v) >= len(text) || !isWordByte(text[i+len(v)])
		startOK := i == 0 || !isWordByte(text[i-1])
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
		reqText := string(ex.req)
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
			body := dropFraming(scrubSupplied(string(ex.resp())))
			fmt.Fprintf(&report, "## Response%s — INCOMPLETE, and the SDK was told so\n\n"+
				"The body is not established as whole (%v). What arrived is below; it "+
				"is NOT a complete response.\n\n%s\n%s\n",
				label, sanitize(ex.truncErr()), fencedBlock(body), ex.trailerReport())
		default:
			respText := dropFraming(scrubSupplied(string(ex.resp())))
			fmt.Fprintf(&report, "## Response%s\n\n%s\n%s\n", label,
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
	fmt.Fprintf(&report, "    variant:  %q\n", scrubSupplied(result.VariantKey))
	fmt.Fprintf(&report, "    reason:   %q\n", scrubSupplied(result.Reason))
	// The SDK's own classification. A 404 returns a usable result with
	// Code "not_found", Assigned false and a NIL error, so omitting this showed
	// only zero-valued fields and then called the run generically not-served --
	// losing the first-class verdict this program exists to report.
	fmt.Fprintf(&report, "    code:     %q\n", scrubSupplied(result.Code))
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
	if _, werr := io.WriteString(os.Stdout, report.String()); werr != nil {
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
			scrubSupplied(result.Reason), scrubSupplied(result.Code))
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
