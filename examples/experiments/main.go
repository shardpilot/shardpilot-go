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
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"os"
	"strconv"
	"strings"
	"time"

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
	transErr error // set when no response arrived at all
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
	if e.captured.err == nil && e.captured.overflowed {
		return errOversizedForCapture
	}
	return e.captured.err
}

// errOversizedForCapture is not the SDK's failure -- it is THIS RECORD's. The
// SDK may have handled the oversized body perfectly; what is incomplete is the
// copy, and the run is reported as an incomplete capture rather than as a
// complete one that happens to be short.
var errOversizedForCapture = errors.New(
	"more body reached the SDK than this record holds; the capture is incomplete, " +
		"and the SDK's own verdict above is unaffected")

func (e *exchange) resp() []byte {
	if e.head == nil {
		return nil
	}
	return append(append([]byte{}, e.head...), e.body()...)
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

func dropFraming(dump string) string {
	lines := strings.Split(dump, "\n")
	out := make([]string, 0, len(lines))
	for _, l := range lines {
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
	captured := &teeBody{inner: resp.Body}
	resp.Body = captured
	ex.status = resp.StatusCode
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
}

// One byte MORE than the SDK's own limit, deliberately. The SDK reads
// rcMaxBodyBytes+1 precisely so it can tell "at the limit" from "over it"; a
// copy capped one byte lower cannot represent the case it is meant to witness,
// and the record would show a shortened body as a whole one
// (shardpilot/shardpilot-go#73 review).
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
			// Bytes reached the SDK that this record does not hold. Saying so
			// is the point: an incomplete copy presented as complete is the
			// defect, not the incompleteness.
			t.overflowed = true
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
		if q := strconv.Quote(v); len(q) >= 2 {
			if inner := q[1 : len(q)-1]; inner != v {
				text = replaceValue(text, inner)
			}
		}
	}
	return text
}

// replaceValue redacts every occurrence of v. A long value is replaced outright;
// a SHORT one only where it stands as a whole token, because the SDK validates
// these fields as non-empty and nothing more -- an experiment key may legally be
// `ab`, and the first version skipped anything under four characters, printing
// exactly those verbatim in the response it calls publishable
// (shardpilot/shardpilot-go#73 review). Blind substring replacement of `ab`
// would instead corrupt unrelated words, so short values are matched at
// boundaries and long ones are not.
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

	fmt.Printf("# assignment capture — %s\n\n", time.Now().UTC().Format(time.RFC3339))
	if len(rec.exchanges) > 1 {
		fmt.Printf("The SDK made **%d attempts**. All are below; the verdict is the "+
			"last, because that is the one it acted on.\n\n", len(rec.exchanges))
	}
	for i, ex := range rec.exchanges {
		label := ""
		if len(rec.exchanges) > 1 {
			label = fmt.Sprintf(" %d", i+1)
		}
		reqText := strings.TrimRight(string(ex.req), "\r\n")
		f := fenceFor(reqText)
		fmt.Printf("## Request%s, as the SDK sent it\n\n%s\n%s\n%s\n\n",
			label, f, reqText, f)
		switch {
		case ex.transErr != nil:
			fmt.Printf("## Response%s\n\nNONE — the request was formed and no "+
				"response arrived: %s\n\n", label, sanitize(ex.transErr))
		case ex.truncErr() != nil:
			fmt.Printf("## Response%s — TRUNCATED, and the SDK was told so\n\n"+
				"The body could not be read whole (%v). What arrived is below; it "+
				"is NOT a complete response.\n\n%s\n%s\n%s\n\n",
				label, sanitize(ex.truncErr()),
				fenceFor(dropFraming(scrubSupplied(strings.TrimRight(string(ex.resp()), "\r\n")))),
				dropFraming(scrubSupplied(strings.TrimRight(string(ex.resp()), "\r\n"))),
				fenceFor(dropFraming(scrubSupplied(strings.TrimRight(string(ex.resp()), "\r\n")))))
		default:
			respText := dropFraming(scrubSupplied(strings.TrimRight(string(ex.resp()), "\r\n")))
			rf := fenceFor(respText)
			fmt.Printf("## Response%s\n\n%s\n%s\n%s\n\n", label, rf, respText, rf)
		}
	}

	last := rec.last()
	fmt.Printf("## SDK verdict\n\n")
	fmt.Printf("    attempts: %d\n", len(rec.exchanges))
	fmt.Printf("    status:   %d\n", last.status)
	fmt.Printf("    assigned: %t\n", result.Assigned)
	// Scrubbed like everything else: a variant key may legally equal a supplied
	// identifier -- an experiment and a variant both named `control` is a valid
	// response -- and the property is that a supplied value is never printed
	// back WHEREVER it appears, not only in the body.
	fmt.Printf("    variant:  %q\n", scrubSupplied(result.VariantKey))
	fmt.Printf("    reason:   %q\n", scrubSupplied(result.Reason))
	// The SDK's own classification. A 404 returns a usable result with
	// Code "not_found", Assigned false and a NIL error, so omitting this showed
	// only zero-valued fields and then called the run generically not-served --
	// losing the first-class verdict this program exists to report.
	fmt.Printf("    code:     %q\n", scrubSupplied(result.Code))
	fmt.Printf("    version:  %d\n", result.Version)
	if fetchErr != nil {
		fmt.Printf("    error:    %s\n", sanitize(fetchErr))
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
