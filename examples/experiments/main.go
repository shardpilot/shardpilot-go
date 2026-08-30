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
// ⚠ WHAT THIS PROGRAM CLAIMS, AND WHY THE CLAIM IS NARROW ON PURPOSE.
//
// It does NOT claim "nothing leaks". That is an absolute statement about a
// hostile input and has no fixed point: for every form a reader imagines, the
// program grows a defence, and the next form is imagined next. A list of probes
// against imagination is a list against an infinity, and a list is complete only
// from the inside. Review rounds on this file demonstrated exactly that, each
// finding real defects in the previous round's fixes, because there was no
// specification to converge against.
//
// It claims something checkable instead:
//
//	NOTHING IS PRINTED THAT THIS PROGRAM CANNOT ACCOUNT FOR.
//
//	  a supplied value      absent from every form THE DECODERS BELOW produce, or
//	                        the record is not printed at all. The decoders are a
//	                        CLOSED LIST and the claim is relative to it:
//	                        undoPercent, undoUnicodeEscapes, undoBase64, undoHex,
//	                        undoPlus, undoEntities -- run to a fixed point, over
//	                        the text AND the field names, and over the candidates
//	                        the base64, binary and hex producers add.
//	  a server-generated    Set-Cookie, Location, a top-level minted subject key,
//	    surface             or a body in a coding this build cannot decode: the
//	                        record is NOT printed, because this half can detect
//	                        them and cannot redact them
//	  a request query       withheld whole — a length this half cannot compute is
//	                        not a length it may guess
//	  protocol syntax       marked as generated, and therefore not read as
//	                        captured data: the method, the version, the status
//	                        line, request header names, the auth scheme, values
//	                        `net/http` writes into the dump, the SDK's fixed
//	                        route, and the three JSON grammar literals
//	  everything else       printed as received
//
// ⚠ AND THE FIRST CLAUSE IS NARROWED ON PURPOSE. It used to read "provably absent
// from every form the decoders reach", which is the absolute statement this
// comment opens by refusing: "the forms the decoders reach" is not a set anyone
// can enumerate, so the clause invited exactly the question the paragraph above
// says has no answer -- did you think of every encoding. Fourteen review rounds
// answered it fourteen times, and the last four found 4, 4, 6 and 9 defects, more
// than half of them in the previous round's fixes.
//
// Naming the decoders makes the clause checkable in the way the others are: the
// claim speaks about what THOSE decoders reconstruct, and an encoding outside the
// list is outside the claim. Extending it is a decoder, a line in the list above,
// and a scene -- a bounded change with a visible cost, instead of a promise that
// quietly grows.
// TestTheClaimNamesExactlyTheDecodersThatRun holds the list and the code
// together, so the sentence cannot drift from the chain it describes.
//
// ⚠ THE LAST CLAUSE IS THE WEAK ONE, AND SAYING SO IS THE POINT. "Everything
// else printed as received" covers an `ETag`, a `Server` banner, an unregistered
// field name -- values the endpoint chose and this half neither refuses nor
// redacts. That is the clause the structural-redaction change replaces, with a
// test each of those must pass; until it lands, this program prints them and
// this comment says so rather than leaving a reader to discover it.
//
// The difference the claim makes to whoever reviews this: "nothing leaks" can
// only be attacked by inventing a new shape. The clauses above can be CHECKED --
// take any byte in the artifact and ask which one admitted it. A clause that
// admits what it should not is one defect in the criterion; a place that prints
// without asking is a defect in that place. Both questions have answers. "Did
// you think of every encoding" does not.
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
//	4  THE RECORD IS NOT A CAPTURE. Either a supplied value survived redaction in
//	   some encoding and the report was withheld — nothing at all was written —
//	   or the write failed partway, in which case a PREFIX may already be on
//	   stdout and the byte count is printed on stderr. A stream cannot promise
//	   all-or-nothing, and the earlier wording ("was not published") claimed it
//	   did (shardpilot/shardpilot-go#84 review). Nothing is claimed about the
//	   exchange: it may have been a
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
	"encoding/hex"
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
	"sync"
	"time"
	"unicode"
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
	req    []byte
	head   []byte
	status int
	proto  string // the protocol actually negotiated, which the request dump is not
	// recvConn records whether THIS response carried a `Connection` field, as
	// opposed to the serialiser adding one. Per exchange, because the SDK retries:
	// a grammar-remint attempt renders every attempt, and a single global held the
	// LAST one -- so a first response that really sent `Connection: <value>` had it
	// marked serialiser-generated when the final response carried none, and the
	// guard then ignored and published it (shardpilot/shardpilot-go#84 review).
	// A fact about a response stored once per RUN is a fact about the last response.
	recvConn bool
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
			// ⚠ DISPATCH BEFORE SCRUBBING THE NAME, which is the documented
			// structural-first order this composition had reversed. With
			// `cookie` supplied, scrubHeaderName turned `Set-Cookie` into
			// `Set-redacted-6-chars` FIRST, structuralRedact no longer recognised
			// the field, and the server-generated cookie value was published --
			// the same bypass for a supplied `location`
			// (shardpilot/shardpilot-go#73 review).
			// ⚠ THE SAME TABLE THE HEADER BLOCK READS. This list said `set-cookie`
			// and `location`, and a `WWW-Authenticate` arriving as a trailer -- legal,
			// and net/http accepts it into `Response.Trailer` -- was published
			// (shardpilot/shardpilot-go#84 review). See serverMintedFields.
			// ⚠ AND THE NOTE IS A CONSTANT. It read `"a " + k + " trailer"`, carrying
			// the endpoint's own spelling of the field name to stderr -- a refusal
			// printing the thing it refuses, which this file records twice already.
			low := strings.ToLower(k)
			if note, minted := serverMintedFields[low]; minted {
				noteStructural(note)
				fmt.Fprintf(&b, "    %s\n", canonicalFieldName(low)+": "+marked("<withheld>"))
				continue
			}
			fmt.Fprintf(&b, "    %s\n",
				asCaptured(scrubHeaderName(escapeMarks(k))+": "+scrubSupplied(escapeMarks(v))))
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

// sdkTaxonomy are the classification strings THIS SDK produces for a verdict.
//
// ⚠ TRANSCRIBED FROM experiments.go, NOT RECALLED, and the constants are
// unexported so this cannot be derived in-process: `experimentReasonKillSwitch`,
// `experimentReasonTargeting` and the `Code: "not_found"` return are the source.
// A value here is not endpoint data — the SDK writes it — so leaving it captured
// let a legal experiment key of `not_found` rewrite the verdict to
// `<redacted, 9 chars>`, and the capture lost the first-class classification the
// report exists to record (shardpilot/shardpilot-go#84 review). Eleventh site of
// one rule: what this program put there is not the endpoint's choice.
//
// The failure direction of a MISSING entry is safe: an unrecognised value is
// scrubbed, which costs a label rather than publishing endpoint text. That is why
// this list may be a list, unlike the ones whose gaps publish.
var sdkTaxonomy = set(
	"kill_switch",
	"targeting_unmatched",
	"not_found",
	// Named as a Code in the same doc comment as `not_found`, found by grepping
	// the SDK source rather than by remembering the taxonomy.
	"superseded",
)

// vouchTaxonomy marks a verdict field this SDK generated.
func vouchTaxonomy(v string) string {
	if sdkTaxonomy[v] {
		return marked(v)
	}
	return v
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
	// The per-exchange fact is put where `dropFraming` reads it, for THIS exchange,
	// immediately before it is rendered.
	receivedConnection = ex.recvConn
	return asCaptured(scrubSupplied(dropFraming(escapeMarks(string(ex.resp())))))
}

// respSection is the response section's prose, named rather than inlined so the
// fixture that checks what it CLAIMS reads the same bytes the report prints. A
// test carrying its own copy of the sentence would pass while the report says
// something else (shardpilot/shardpilot-go#73 review).
const respSection = "## Response%s — header block re-serialised by " +
	"`httputil.DumpResponse`\n\nThe status line is a CANONICAL " +
	"REPRESENTATION, not received bytes: `DumpResponse` calls " +
	"`http.Response.Write`, which re-serialises the protocol, code, spacing and " +
	"reason from parsed fields — and on HTTP/2, which is what production " +
	"negotiates, no textual status line is received at all. It is labelled the " +
	"same way the request line is, for the same reason. The BODY is " +
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

// ── what this half cannot publish ────────────────────────────────────────────
//
// This change carries the publication GUARD: it proves that no value this
// program supplied survives into the record, in any encoding it can construct
// or decode. It carries no STRUCTURAL redaction, and that is the whole of the
// difference: a `Set-Cookie` value, a redirect target, a server-minted subject
// key are generated by the ENDPOINT, so no list of supplied values can reach
// them and the guard behind this cannot see them either.
//
// So they are not published. They are detected, and their presence makes the
// record unpublishable -- exit 4, the same exit a surviving leak produces,
// because it is the same fact: this program cannot show the bytes are safe.
// The change that adds structural redaction turns each of these refusals into a
// capture; until then a refusal is the honest answer, and printing the bytes
// with a note attached would not be.
// receivedConnection records whether the ENDPOINT sent a `Connection` field, as
// opposed to the serialiser adding one. See where it is set.
// configuredHost is the authority THIS PROGRAM was pointed at. The `Host:` line
// carries it, and it is not endpoint text.
var configuredHost string

var receivedConnection bool

var structuralSurfaces []string

func noteStructural(what string) {
	if !slices.Contains(structuralSurfaces, what) {
		structuralSurfaces = append(structuralSurfaces, what)
	}
}

// mintedNames are the fields the SERVER mints -- the fact lane's subject and its
// privacy boundary, defined as such in experiments.go.
// serverMintedFields names the response fields whose VALUE the ENDPOINT mints.
// No list of values THIS program supplied can reach such a value, and the
// decoders behind the guard cannot either, so the value is withheld and the
// record is not publishable.
//
// It is a map and not two conditions because the question was being asked in two
// places with two answers. The note is stored WITH the name so no caller composes
// one out of the field's arrived spelling.
// decodeWorkMax bounds the bytes the decoding chain may examine for one record.
// The budget counts bytes examined and fails CLOSED: past it the record is not
// publishable and is not printed.
const decodeWorkMax = 64 << 20

// decodeWork is what the suffix probes below have spent since the caller last
// collected it. The chain's budget lived entirely in the caller, charging
// `len(cur)` per stage -- and the suffix scans inside `undoBase64` and
// `binaryCandidates` are QUADRATIC in the length of a maximal run, so a body of
// thousands of separator bytes did work the budget never saw: 20,000 `/` bytes
// alone cost about 2.5 seconds against an accepted body ceiling near 1 MiB, so
// an endpoint could hold post-processing far past the capture deadline
// (shardpilot/shardpilot-go#84 review). A resource limit that does not count the
// dominant term is not a limit.
//
// ⚠ AND STOPPING EARLY IS ONLY SAFE BECAUSE THE CALLER REFUSES. These scans give
// up once the accumulator passes the ceiling, which examines LESS -- that would
// be fail-open on its own. It is fail-closed here because the caller collects
// this and refuses to publish the record at all; a short scan must never be able
// to become a clean verdict.
var decodeWork int

// takeDecodeWork returns what the suffix probes have spent and resets it.
func takeDecodeWork() int {
	n := decodeWork
	decodeWork = 0
	return n
}

// separatorStarts names each offset in tok that follows a plausible separator
// and leaves at least minLen bytes -- the starts a standard decoder would
// reconstruct a value from, given that this tokeniser is MAXIMAL and `/`, `+`,
// `-`, `_` and `.` all appear inside legal runs. The tails it names are charged
// to decodeWork before they are handed to any decoder.
func separatorStarts(tok string, minLen int) []int {
	var out []int
	for k := 0; k < len(tok); k++ {
		if strings.IndexByte("/+-_.", tok[k]) < 0 || len(tok)-k-1 < minLen {
			continue
		}
		decodeWork += len(tok) - k - 1
		if decodeWork > decodeWorkMax {
			return out
		}
		out = append(out, k+1)
	}
	return out
}

// base64SuffixCandidates retains each successfully decoded suffix as its OWN
// candidate.
//
// ⚠ SPLICED BACK IN, THE DECODE IS UNREACHABLE. `undoBase64` writes the prefix
// INCLUDING the separator and then the decoded bytes, so a legal short value
// `bar` arriving as `-YmFy` became `-bar` -- and the short-value matcher counts
// `-` and `_` as word bytes, so it read the occurrence as embedded and rejected
// it, and the guard approved a directly reconstructable value
// (shardpilot/shardpilot-go#84 review). The splice answers "what does this text
// say"; the candidate answers "what can be reconstructed from it", and only the
// second question has a boundary the matcher can see.
func base64SuffixCandidates(text string) []string {
	const minToken = 4
	var out []string
	for i := 0; i < len(text); {
		j := i
		for j < len(text) && isBase64Byte(text[j]) {
			j++
		}
		if j == i {
			i++
			continue
		}
		tok := text[i:j]
		i = j
		if len(tok) < minToken {
			continue
		}
		// ⚠ AND THE SHORT SUFFIXES. The suffix scan required four bytes and the short
		// scan required a whole token, so `prefix/YQ` -- a one-character key after path
		// punctuation -- fell between two fixes that each covered half of it
		// (shardpilot/shardpilot-go#84 review). The combination of two rules is a third
		// rule, and neither of them stated it.
		for _, st := range separatorStarts(tok, 2) {
			if dec, ok := decodeBase64(tok[st:]); ok {
				out = append(out, dec)
			}
		}
	}
	return out
}

var serverMintedFields = map[string]string{
	"set-cookie":         "a Set-Cookie field",
	"location":           "a Location field",
	"www-authenticate":   "an authentication challenge this build cannot describe",
	"proxy-authenticate": "an authentication challenge this build cannot describe",
}

// canonicalFieldName renders a field name from THIS program's spelling, never
// the one that arrived: printing the arrived spelling is how a refusal publishes
// what it refused.
func canonicalFieldName(low string) string {
	parts := strings.Split(low, "-")
	for i, p := range parts {
		if p == "" {
			continue
		}
		if strings.EqualFold(p, "www") {
			parts[i] = "WWW"
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, "-")
}

// fieldNameOf returns the lower-cased field name of a `name: value` line.
func fieldNameOf(low string) (string, bool) {
	i, ok := headerNameEnd(low)
	if !ok {
		return "", false
	}
	return low[:i], true
}

// benignTopLevel names the members of the SDK's own response schema.
//
// ⚠ THE SAME NAME AND THE SAME CONTENTS AS THE STRUCTURAL-REDACTION BRANCH USES.
// Two registries answering one question drift -- this file records that happening
// three rounds running to `isMinted` -- so the branch stacked on this one finds
// the identical declaration and the two cannot diverge across the seam.
var benignTopLevel = map[string]bool{
	"assigned": true, "variant_key": true, "variant_payload": true,
	"version": true, "reason": true, "boundary": true, "code": true,
	"assignment_unit": true, "attributes": true, "experiment_key": true,
	// AND THE ONES THE SDK ITSELF ACCEPTS: a `401` body is
	// `{"error":"unauthorized"}` and an echoed assignment carries `app_key` and
	// `environment_key`.
	"error": true, "app_key": true, "environment_key": true,
}

var mintedNames = map[string]bool{
	"subject_fact_key": true,
	"subject_key_hash": true,
}

// ⚠ THE MARKER IS SPACE-FREE. `<query withheld>` carries a space, and after the
// provenance marks are stripped that space becomes an extra component of the
// request line -- so every published request block was rejected by a strict
// parser while the report called it a canonical HTTP/1.1 representation
// (shardpilot/shardpilot-go#84 review). A request target is one token.
//
// dropQuery removes a URL's query and fragment entirely, keeping the path. This
// half cannot say how long each value was without the structural redactor, and a
// length it cannot compute is not a length it may guess.
func dropQuery(line string) string {
	cut := len(line)
	for _, c := range []byte{'?', '#'} {
		if i := strings.IndexByte(line, c); i >= 0 && i < cut {
			cut = i
		}
	}
	if cut == len(line) {
		return line
	}
	// ⚠ ONLY THE REQUEST-TARGET, NOT THE REST OF THE LINE. A request line is
	// space-delimited -- `GET /p?x=y HTTP/1.1\r` -- and every assignment URL this
	// program builds carries a query, so cutting from `?` to end of string
	// removed the HTTP version and the terminator from EVERY published request
	// block, while the report went on calling it a canonical HTTP/1.1
	// representation (shardpilot/shardpilot-go#84 review). A bare URL has no
	// space and keeps the old behaviour.
	tail := ""
	if j := strings.IndexByte(line[cut:], ' '); j >= 0 {
		tail = line[cut+j:]
	}
	// ⚠ THE SEPARATOR IS SYNTAX AND MUST SURVIVE. Concatenating the marker onto
	// the path published `/api/…/assignmentquery-withheld` -- a route the SDK never
	// requested, on EVERY successful report, since every assignment request carries
	// a query (shardpilot/shardpilot-go#84 review). The primary evidence of this
	// artifact is which route was called, and the redaction was rewriting it.
	return line[:cut] + string(line[cut]) + marked("query-withheld") + tail
}

// noteMinted records a server-minted field's presence and returns the body
// unchanged -- the caller does not publish a body this reports on.
// ⚠ MEMBER NAMES ARE DECODED, NOT MATCHED LITERALLY. `"subject_\u0066act_key"`
// is the same field to `encoding/json` and to the endpoint, and a substring
// check on the raw spelling did not see it -- so the capture was PUBLISHED with
// the minted key intact instead of refused, and the leak guard cannot help
// because a server-minted value is not in suppliedValues
// (shardpilot/shardpilot-go#84 review). This is the same defect the redaction
// half had in its own pattern, reintroduced here by writing a second, simpler
// detector for the same question.
var jsonMemberName = regexp.MustCompile(
	`"((?:[^"\\]|\\.)*)"(\s*:\s*)`)

// jsonString decodes a JSON string body -- the bytes BETWEEN the quotes -- to
// what it denotes, using the same decoder that produced the response.
func jsonString(raw string) (string, bool) {
	var out string
	if err := json.Unmarshal([]byte(`"`+raw+`"`), &out); err != nil {
		return "", false
	}
	return out, true
}

// isMinted reports whether a raw JSON member name denotes a server-minted field,
// under the decoding AND the folding `encoding/json` itself applies.
//
// ⚠ THIS IS THE ONE PLACE THAT ANSWERS THE QUESTION, and that is the fix for a
// class rather than a case. The change that adds structural redaction needs the
// same test in order to REDACT what this half REFUSES, and it used to answer it
// again in its own file. The two copies then drifted three rounds running --
// literal substring, ASCII case, Unicode fold -- each time the copy that had
// already been corrected failing to correct the other
// (shardpilot/shardpilot-go#84, #85 review). A stand-in cannot inherit a history
// of fixes; shared code does not need to.
func isMinted(raw string) bool {
	name, ok := jsonString(raw)
	if !ok {
		return false
	}
	return isMintedName(name)
}

// isMintedName is isMinted for a name already decoded.
func isMintedName(name string) bool {
	for n := range mintedNames {
		if strings.EqualFold(name, n) {
			return true
		}
	}
	return false
}

// jsonParses reports whether the body is exactly one JSON value with nothing but
// JSON whitespace around it.
//
// ⚠ IT IS A DIFFERENT QUESTION FROM `topLevelMembers(body) == nil`, and reading
// one as the other made a valid response unpublishable: `[{"subject_fact_key":…}]`
// is syntactically valid JSON whose structure PROVES that member is nested, and
// the nil return -- which also means "parsed, but not an object" -- was read as
// "cannot be classified", so the capture was refused
// (shardpilot/shardpilot-go#84 review). One name answering two questions turns
// the first into the second; this is the second time in this stack.
func jsonParses(body string) bool {
	d := json.NewDecoder(strings.NewReader(body))
	var v json.RawMessage
	if err := d.Decode(&v); err != nil {
		return false
	}
	return strings.TrimSpace(body[int(d.InputOffset()):]) == ""
}

// noteMinted records a server-minted field's presence and returns the body
// unchanged -- the caller does not publish a body this reports on.
// ⚠ TOP-LEVEL ONLY. `encoding/json` binds the SDK's field from the top-level
// object; a member of the same name inside `variant_payload` is ordinary payload
// the endpoint chose to call that, and refusing on it made a perfectly
// publishable assignment unpublishable (shardpilot/shardpilot-go#84 review). The
// question is "is this THE minted field", not "does this name appear".
func topLevelMembers(body string) []string {
	var raw map[string]json.RawMessage
	i := strings.IndexByte(body, '{')
	if i < 0 || json.Unmarshal([]byte(body[i:]), &raw) != nil {
		return nil
	}
	out := make([]string, 0, len(raw))
	for k := range raw {
		out = append(out, k)
	}
	return out
}

func noteMinted(body string) string {
	// ⚠ A BODY THAT WILL NOT PARSE IS NOT AN ANSWER ABOUT DEPTH. `topLevelMembers`
	// returns nothing for `{"assigned":true,"subject_fact_key":"…` with no closing
	// brace, and "no top-level names" was then read as "there is no minted field
	// here" -- so a malformed verdict body was PUBLISHED with the identifier
	// intact, and the leak guard cannot help because a server-minted value is not
	// in suppliedValues (shardpilot/shardpilot-go#84 review). The SDK calls such a
	// response malformed; the harness printed it anyway.
	//
	// Fail closed, and only here: when the body does not parse, nothing can show a
	// minted name is nested, so it is treated as top-level. A body that parses
	// keeps the depth rule exactly as it was -- a payload member the endpoint
	// merely named that way must still not refuse a good capture.
	// ⚠ `!jsonParses`, NOT `topLevelMembers == nil`: see jsonParses.
	// ⚠ AND NO `{` PREREQUISITE. A CLOSE-DELIMITED body -- `"subject_fact_key":
	// "sfk1_…"` with no object around it at all -- is malformed to the SDK and
	// carries no brace, so the scan was skipped on exactly the shape it exists
	// for, and the value went out (shardpilot/shardpilot-go#84 review). The brace
	// was a guess about how a malformed body looks, standing in front of a rule
	// about what it CONTAINS; the scan below already answers that itself, and on a
	// body with no minted member it finds nothing and costs nothing.
	if !jsonParses(body) {
		for _, m := range jsonMemberName.FindAllStringSubmatch(body, -1) {
			if isMinted(m[1]) {
				noteStructural("a server-minted subject identifier in a body that does not parse")
			}
		}
	}
	for _, name := range topLevelMembers(body) {
		if isMintedName(name) {
			// ⚠ A CONSTANT, NOT THE NAME. Third site of this defect in one session: with a
			// Unicode-folded spelling the endpoint chose, `isMintedName` accepts it and
			// the label carried that spelling to stderr -- refusing to print the
			// response while printing the identifier (shardpilot/shardpilot-go#84
			// review). Every message a guard prints is an output channel.
			noteStructural("a server-minted subject identifier")
		}
	}
	return body
}

// verdictVersion renders the assignment version for the verdict block.
//
// ⚠ IT IS A FUNCTION SO THE FIXTURE READS THE CALL SITE, for the reason written
// on verdictValue directly below: a test that re-assembles the same calls in its
// own body passes while the report does something else.
func verdictVersion(v int64) string {
	return stripMarks(scrubSupplied(fmt.Sprintf("%d", v)))
}

// verdictValue renders a value the SDK decoded, for the verdict block.
//
// ⚠ IT IS A FUNCTION SO THE FIXTURE READS THE CALL SITE. Written inline, the
// test could only re-assemble the same three calls in its own body and would
// have passed while the report did something else -- which is exactly what
// happened: the mutant that reverted the call site survived a fixture composing
// the pipeline itself (shardpilot/shardpilot-go#84 review).
//
// ⚠ AND THE ORDER IS escape, scrub, strip. A variant key containing a reserved
// marker byte was DELETED by stripMarks -- `a<NUL>b` reported as `ab` -- while
// the response block, which escapes first, kept it, so the artifact misstated
// the assignment the SDK served.
// transportErrorLine renders a transport failure for the REPORT.
//
// ⚠ IT IS A FUNCTION SO THE FIXTURE READS THE CALL SITE. Written inline, a test
// could only re-assemble the same call in its own body and would pass while the
// report used the unmarked form -- which is exactly what happened, for the third
// time in this branch (shardpilot/shardpilot-go#84 review). Go's parser puts the
// offending response line into the error, so this is a CHANNEL from the endpoint
// into report prose, and everything on it is captured input.
// incompleteBodyLine renders the read failure that ended a body, for the report.
//
// ⚠ NAMED FOR THE FIFTH TIME IN THESE TWO CHANGES, and for the fifth time for
// the same reason: a fixture that composes the call cannot see the call site, so
// reverting the site survives. Errors raised AFTER the response head arrive on
// this path rather than the transport one, and they carry endpoint text just the
// same (shardpilot/shardpilot-go#84 review).
func incompleteBodyLine(ex *exchange) string {
	return transportErrorLine(ex.truncErr())
}

func transportErrorLine(err error) string {
	return sanitizeCaptured(err)
}

func verdictValue(v string) string {
	return stripMarks(scrubSupplied(escapeMarks(v)))
}

// set builds a membership map from a list, so a registry reads as a registry.
func set(v ...string) map[string]bool {
	m := make(map[string]bool, len(v))
	for _, x := range v {
		m[x] = true
	}
	return m
}

// ⚠ TRANSCRIBED FROM A NAMED REGISTRY, NOT RECALLED. Three rounds running a
// reviewer has named one more type this list was missing, because it was written
// from memory -- and a list written from memory answers the length of the
// author's memory, not the question. Its entries are the IANA Media Types
// registry's common `application/*`, `text/*`, `image/*`, `audio/*`, `video/*`
// and `font/*` registrations; regenerate from
// https://www.iana.org/assignments/media-types/media-types.xhtml rather than by
// adding whatever the next round names.
//
// It is still a list, and it will still miss something. What the provenance buys
// is that the NEXT person can complete it in one operation instead of one entry
// per review round -- and that a miss costs readability, never safety: an
// unlisted type is lengthened, not published.
var registeredMediaTypes = set(
	"application/json", "application/problem+json", "application/ld+json",
	"application/xml", "application/xhtml+xml", "application/atom+xml",
	"application/octet-stream", "application/x-www-form-urlencoded",
	"application/javascript", "application/ecmascript", "application/pdf",
	"application/zip", "application/gzip", "application/cbor",
	"application/msgpack", "application/wasm", "application/graphql-response+json",
	"application/vnd.api+json", "application/jose", "application/jwt",
	"application/manifest+json", "application/rss+xml", "application/sql",
	"application/yaml", "application/toml",
	"text/plain", "text/html", "text/css", "text/csv", "text/xml",
	"text/javascript", "text/markdown", "text/event-stream", "text/calendar",
	"image/png", "image/jpeg", "image/gif", "image/svg+xml", "image/webp",
	"image/avif", "image/bmp", "image/tiff", "image/x-icon",
	"audio/mpeg", "audio/ogg", "audio/wav", "audio/webm",
	"video/mp4", "video/ogg", "video/webm",
	"font/woff", "font/woff2", "font/ttf", "font/otf",
)

// ⚠ A RECOGNISED MEDIA TYPE IS GRAMMAR, AND MUST BE MARKED AS SUCH. With a legal
// experiment key of `json`, the ordinary `Content-Type: application/json` came back
// as `Content-Type: application/<redacted, 4 chars>`: the recorded response no
// longer declares its own media type, and the guard approved it because the
// placeholder is generated (shardpilot/shardpilot-go#84 review). Ninth site of one
// rule in this stack -- vouching for a token and leaving it captured is not
// vouching -- and the registry lives HERE, in the shared machinery, so the half
// stacked on this one drops its copy rather than keeping a second.
//
// The PARAMETERS are not vouched for: `boundary=` and `charset=` values are the
// endpoint's, and only the type/subtype is fixed by the registry.
func markMediaType(line string) string {
	i, ok := headerNameEnd(line)
	if !ok || !strings.EqualFold(strings.TrimSpace(line[:i]), "content-type") {
		return line
	}
	rest := line[i+1:]
	cr := ""
	if strings.HasSuffix(rest, "\r") {
		cr, rest = "\r", strings.TrimSuffix(rest, "\r")
	}
	mt, params, _ := strings.Cut(rest, ";")
	lead := mt[:len(mt)-len(strings.TrimLeft(mt, " \t"))]
	bare := strings.TrimSpace(mt)
	if !registeredMediaTypes[strings.ToLower(bare)] {
		return line
	}
	out := line[:i+1] + lead + marked(bare)
	if params != "" {
		out += ";" + params
	}
	return out + cr
}

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
		// ⚠ THE NOTES BELOW ARE GENERATED, AND ARE MARKED AS SUCH. Left unmarked,
		// a supplied identifier equal to `Capture-Note` reached them through the
		// generic scrub and produced `X-<redacted, 12 chars>` -- spaces and angle
		// brackets inside a field name, an unparsable response block, which the
		// guard then approved because the placeholder carries generated marks
		// (shardpilot/shardpilot-go#84 review). Text this program wrote is not
		// captured text.
		low := strings.ToLower(l)
		cr := ""
		if strings.HasSuffix(l, "\r") {
			cr = "\r"
		}
		// ⚠ `DumpResponse` ADDS `Connection: close` when the length is unknown,
		// and HTTP/2 forbids that field -- so it is generated syntax on a
		// response that never carried it, and a key of `close` produced
		// `Connection: <redacted, 5 chars>`: an invalid canonical response the
		// guard then approved because the placeholder is generated
		// (shardpilot/shardpilot-go#84 review). Same treatment as the framing
		// headers directly below.
		// ⚠ ONLY THE SYNTHESISED ONE. HTTP/2 forbids `Connection`, so its presence
		// in an HTTP/2 dump is proof the serialiser wrote it -- but on HTTP/1 the
		// transport keeps what the endpoint sent, and marking that line generated
		// let a value base64-decoding to the key through both the scrub and the
		// guard (shardpilot/shardpilot-go#84 review). The exemption was written
		// for one protocol and applied to both.
		// ⚠ THE PROTOCOL IS NOT THE QUESTION; whether it was RECEIVED is. See
		// receivedConnection.
		if strings.HasPrefix(low, "connection:") && !receivedConnection {
			out = append(out, marked(strings.TrimSuffix(l, "\r"))+cr)
			continue
		}
		if strings.HasPrefix(low, "content-length:") {
			out = append(out, marked("X-Capture-Note: Content-Length removed — the body below is redacted")+cr)
			continue
		}
		// ⚠ A REDIRECT TARGET IS A CREDENTIAL SURFACE. `Location` query values are
		// server-generated -- `state`, signed tokens, one-time callbacks -- so no
		// list of values THIS program supplied can reach them, exactly as with
		// Set-Cookie (shardpilot/shardpilot-go#73 review). Redacted structurally,
		// by the same function the request line uses: names kept, values lengthed.
		// ⚠ A CODING THE TRANSPORT DID NOT UNDO LEAVES THE BODY OPAQUE. Go
		// decompresses gzip it requested itself; anything still declared here --
		// `deflate`, `br`, `zstd` -- means the bytes below are compressed, and
		// neither the scrub nor the guard's decoders can see a supplied value
		// inside them, while a reader need only apply the declared coding
		// (shardpilot/shardpilot-go#84 review). This half cannot decode it, so it
		// does not publish it.
		// ⚠ ONE QUESTION, ONE SITE, AND BOTH PATHS ASK IT. This chain and the
		// trailer report each decided for themselves which fields carry a value the
		// ENDPOINT mints, and the trailer's answer named two of them -- so a chunked
		// response declaring `Trailer: WWW-Authenticate` published the challenge
		// while `structuralSurfaces` stayed empty (shardpilot/shardpilot-go#84
		// review). That is the third time the same question was answered on one
		// path and not its twin, and each time the repair was another NAME. The
		// population lives in `serverMintedFields` now, and adding a name to it
		// reaches every path that asks; the sweep over that map is what makes the
		// two paths' agreement a measured thing rather than a habit.
		if name, ok := fieldNameOf(low); ok {
			if note, minted := serverMintedFields[name]; minted {
				noteStructural(note)
				out = append(out, canonicalFieldName(name)+": "+marked("<withheld>")+cr)
				continue
			}
		}
		if strings.HasPrefix(low, "content-encoding:") {
			if v := strings.TrimSpace(strings.TrimSuffix(l[len("content-encoding:"):], "\r")); v != "" &&
				!strings.EqualFold(v, "identity") {
				// ⚠ A CLASSIFICATION, NOT THE VALUE. With a supplied key of
				// `deflate` the scrub hid the header and this diagnostic printed
				// it verbatim to stderr -- the refusal publishing what the refusal
				// was for (shardpilot/shardpilot-go#84 review).
				noteStructural("a body in a content coding this build cannot decode")
			} else if strings.EqualFold(v, "identity") {
				// ⚠ AN ACCEPTED CODING IS GRAMMAR, AND MUST BE MARKED AS SUCH. This
				// branch deliberately accepts `identity` as the no-op coding that
				// accompanies a readable body -- and then left the token as captured
				// text, so with a legal experiment key of `identity` the generic scrub
				// rewrote it to `Content-Encoding: <redacted, 8 chars>`: a declaration
				// of an unknown, syntactically invalid coding, approved by the guard
				// because the placeholder is generated
				// (shardpilot/shardpilot-go#84 review). Same rule as the cookie
				// attributes and the query parameter names: vouching for a token and
				// leaving it captured is not vouching.
				out = append(out, l[:len("content-encoding:")]+
					strings.Replace(l[len("content-encoding:"):], v, marked(v), 1))
				continue
			}
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
		if strings.HasPrefix(low, "transfer-encoding:") {
			out = append(out, marked("X-Capture-Note: Transfer-Encoding removed — the body below is decoded")+cr)
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
		// ⚠ `Trailer:` LISTS FIELD NAMES IN ITS VALUE. Scrubbing only the field
		// name `Trailer` left `Trailer: X-Bar` carrying a supplied `Bar`, which
		// ordinary value matching misses because the hyphen is a word byte and
		// the guard's name-aware check extracts only `Trailer` -- while the
		// trailer report itself scrubs the real name, so the two disagreed about
		// the same string (shardpilot/shardpilot-go#84 review).
		if strings.HasPrefix(low, "trailer:") {
			if i, ok := headerNameEnd(l); ok {
				names := strings.Split(l[i+1:], ",")
				for k, n := range names {
					names[k] = scrubHeaderName(n)
				}
				out = append(out, scrubHeaderName(l[:i])+":"+strings.Join(names, ","))
				continue
			}
		}
		if i, ok := headerNameEnd(l); ok {
			l = markMediaType(l)
			if i, ok = headerNameEnd(l); !ok {
				out = append(out, l)
				continue
			}
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
		markBareJSONLiterals(noteMinted(strings.Join(out[bodyStart:], "\n")))
}

type recorder struct {
	// ⚠ GUARDED, because this transport is shared with the SDK's ingest worker,
	// which flushes on its own timer from another goroutine. Even with the
	// off-route filter above returning early, the counter and the slice are
	// touched concurrently (shardpilot/shardpilot-go#84 review).
	mu        sync.Mutex
	inner     http.RoundTripper
	exchanges []exchange
	// offRoute counts requests this recorder deliberately did not record. It is
	// PRINTED: "the ingest leg is not exercised" then stops being a comment and
	// becomes a number the run either shows as zero or does not.
	offRoute int
}

// last is the attempt whose verdict the program reports. It is the last one
// BECAUSE that is the one the SDK acted on, not because the others did not
// happen -- every one of them is printed.
func (r *recorder) last() *exchange {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.exchanges) == 0 {
		return nil
	}
	return &r.exchanges[len(r.exchanges)-1]
}

// assignmentRoute is the ONLY path this recorder records.
//
// ⚠ THE SDK HAS A SECOND LEG, AND IT SHARES THIS TRANSPORT. Applying an
// assignment enqueues an automatic `experiment_exposure`, and the ingest worker
// flushes on its own timer through the same `HTTPClient` -- so on a run that
// settles near the tick, an ingest POST was recorded as another supposed
// assignment attempt, could change which exchange `last()` returns, and raced
// the report's own reads of `exchanges`. The configuration comment claiming
// the ingest leg is "NOT exercised" was an intention, not a fact
// (shardpilot/shardpilot-go#84 review).
//
// Filtering here rather than trusting a config knob makes it a fact: anything
// that is not an assignment passes through unrecorded, and is COUNTED, so the
// claim is checked on every run instead of asserted once in a comment.
const assignmentRoute = "/api/v1/runtime/experiments/assignment"

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL == nil || !strings.HasSuffix(req.URL.Path, assignmentRoute) {
		r.mu.Lock()
		r.offRoute++
		r.mu.Unlock()
		// ⚠ ABSORBED, NOT FORWARDED. Filtering it out of the RECORD while still
		// sending it left the side effect this harness must not have: an
		// automatic exposure delivered to the ingest endpoint from a run whose
		// only purpose is to observe one assignment
		// (shardpilot/shardpilot-go#84 review). A capture tool that emits
		// analytics is not observing, it is participating.
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return &http.Response{
			Status: "204 No Content", StatusCode: http.StatusNoContent,
			Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")),
			ContentLength: 0, Request: req,
		}, nil
	}
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
		r.mu.Lock()
		r.exchanges = append(r.exchanges, ex)
		r.mu.Unlock()
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
	// ⚠ WHETHER THE RESPONSE CARRIED `Connection` IS A FACT ABOUT THE RESPONSE,
	// and only here is it knowable. `DumpResponse` SYNTHESISES `Connection: close`
	// for HTTP/1.1 whenever the length is unknown, so the dump cannot be asked --
	// and the rule "HTTP/1 means the endpoint sent it" published
	// `Connection: <redacted, 5 chars>` for a legal experiment key of `close`: not
	// a connection-option, and never received (shardpilot/shardpilot-go#84
	// review). The earlier fix distinguished the two PROTOCOLS, which was the
	// right distinction for the case it was shown and not the question.
	// ⚠ PRESENCE, NOT THE FIRST VALUE. `Header.Get` returns the FIRST value, so a
	// response sending `Connection:` and then `Connection: YmFy` reported the
	// field as absent -- both lines were marked serialiser-generated and the guard
	// skipped a directly decodable identifier (shardpilot/shardpilot-go#84
	// review). Map membership answers the question that was asked; `Get` answers a
	// question about a value.
	//
	// Third round on this one line, and each fix was correct for the case it was
	// shown: the protocol was an approximation to the question, the global was the
	// wrong place to keep the answer, and this was the wrong way to ask it.
	_, ex.recvConn = resp.Header["Connection"]
	if d, derr := httputil.DumpResponse(resp, false); derr == nil {
		ex.head = d
	}
	ex.captured = captured
	r.mu.Lock()
	r.exchanges = append(r.exchanges, ex)
	r.mu.Unlock()
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

// snapTrailers copies the trailer block as it stands now.
//
// ⚠ IT IS CALLED ON CLOSE AS WELL AS AT EOF. When a body is EXACTLY the read
// ceiling and also carries trailers -- a legal HTTP/2 shape -- the SDK's own
// io.LimitReader synthesises EOF without calling Read again, so this wrapper
// never saw one. The record then announced a `Trailer` field and omitted its
// contents while calling the pair complete (shardpilot/shardpilot-go#84 review).
func (t *teeBody) snapTrailers() {
	if t.resp != nil && len(t.resp.Trailer) > 0 {
		t.trailer = t.resp.Trailer.Clone()
	}
}

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
		t.snapTrailers()
		// TRAILERS ARRIVE WITH THE LAST CHUNK, NOT WITH THE HEAD. The response
		// head was dumped before the SDK read anything, when resp.Trailer held
		// only DECLARED keys and no values; a record built from that head
		// announced `Trailer: X` and then omitted X entirely, while still
		// calling the pair complete (shardpilot/shardpilot-go#73 review).
	}
	if err != nil && err != io.EOF {
		t.err = err
	}
	return n, err
}

func (t *teeBody) Close() error {
	// The last chance to see trailers: an exact-ceiling body reaches EOF inside
	// the SDK's own limiter and never calls Read again.
	t.snapTrailers()
	err := t.inner.Close()
	// ⚠ AND AGAIN AFTER. Trailer values can become visible DURING close, and a
	// snapshot taken only before it left the report announcing a Trailer field,
	// omitting its contents, and calling the pair complete
	// (shardpilot/shardpilot-go#84 review).
	t.snapTrailers()
	return err
}

// serialiserWritten are request headers `net/http` adds while dumping, whose
// values are as invariant as their names.
// ⚠ BOTH OF THEM. `DumpRequestOut` supplies a default `User-Agent` as well as
// `Accept-Encoding`, and a key equal to `Go-http-client/1.1` was found in this
// program's own output and refused every run (shardpilot/shardpilot-go#84
// review). The property is "written by the serialiser, not chosen by anyone", and
// net/http writes exactly these two.
var serialiserWritten = map[string]bool{"accept-encoding": true, "user-agent": true}

func redact(dump []byte) []byte {
	out := make([]string, 0, 32)
	for _, line := range strings.Split(string(dump), "\n") {
		if strings.HasPrefix(line, "GET ") || strings.HasPrefix(line, "POST ") {
			line = dropQuery(line)
		}
		// The header's PRESENCE is kept -- an absent Authorization header is
		// itself a client-profile defect this capture exists to detect, so
		// replacing the line entirely would hide the failure it is meant to show.
		// ⚠ THE CONFIGURED AUTHORITY IS OURS, VALUE AND ALL. The generic branch below
		// marks only the field NAME, so with `SP_REMOTE_CONFIG_URL` of
		// `https://app.shardpilot.com` and a legal experiment key of `app`, the guard
		// found `app` inside the host this program itself configured and refused
		// EVERY otherwise valid capture (shardpilot/shardpilot-go#84 review). The
		// same rule as the fixed route and the serialiser-written headers: what this
		// program put on the wire is not the endpoint's choice.
		if configuredHost != "" && strings.HasPrefix(strings.ToLower(line), "host:") {
			if i := strings.IndexByte(line, ':'); i > 0 {
				v := strings.TrimSuffix(line[i+1:], "\r")
				cr := ""
				if strings.HasSuffix(line, "\r") {
					cr = "\r"
				}
				if strings.TrimSpace(v) == configuredHost {
					out = append(out, marked(line[:i+1]+v)+cr)
					continue
				}
			}
		}
		if strings.HasPrefix(strings.ToLower(line), "authorization:") {
			field := strings.SplitN(line, " ", 2)
			scheme := "<redacted>"
			if len(field) == 2 {
				if parts := strings.SplitN(strings.TrimSpace(field[1]), " ", 2); len(parts) == 2 {
					// ⚠ THE SCHEME IS SYNTAX TOO. Only the credential was marked, so a
					// legal experiment key of `Bearer` was found by the guard in
					// this program's own rebuilt line and refused every run
					// (shardpilot/shardpilot-go#84 review).
					scheme = marked(parts[0]+" ") + placeholder(parts[1])
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
			line = marked("Authorization: ") + scheme + cr
		}
		// ⚠ EVERY REQUEST HEADER NAME IS SYNTAX THIS PROGRAM DID NOT CHOOSE. A
		// legal experiment key of `Host` or `User-Agent` was reported by the guard
		// as a surviving value, because the canonical name sat unmarked inside the
		// captured span -- so those keys could never produce a capture
		// (shardpilot/shardpilot-go#84 review). Marking the NAME leaves the value
		// under the scrub, where it belongs.
		if i, ok := headerNameEnd(strings.TrimSuffix(line, "\r")); ok &&
			!strings.HasPrefix(line, genMark) {
			// ⚠ AND THE VALUES THE SERIALISER ITSELF WRITES. `DumpRequestOut` adds
			// `Accept-Encoding: gzip`, which this program did not choose and the
			// endpoint did not send -- so a legal experiment key of `gzip` was
			// found there and refused every run, exactly as the header NAMES and
			// the auth scheme did before it (shardpilot/shardpilot-go#84 review).
			if serialiserWritten[strings.ToLower(strings.TrimSpace(line[:i]))] {
				line = marked(strings.TrimSuffix(line, "\r")) + strings.TrimPrefix(line[len(strings.TrimSuffix(line, "\r")):], "")
			} else {
				line = marked(line[:i+1]) + line[i+1:]
			}
		}
		out = append(out, line)
	}
	return []byte(strings.Join(out, "\n"))
}

// captureDeadline bounds the SDK, its HTTP client and this program's context
// alike, so a run cannot end on a limit none of them was given.
const captureDeadline = 30 * time.Second

// noteStructuralInText applies the response path's REFUSAL question to text that
// never became a response.
//
// ⚠ A TRANSPORT ERROR CARRIES THE OFFENDING LINE. Go rejects a malformed response
// before returning one, and puts the complete bad header into the error — so
// `Set-Cookie: session=<server value>` reached the report through the error
// diagnostic, where only the supplied-value scrub ran: `dropFraming` never saw it,
// nothing was added to structuralSurfaces, and the guard has no supplied value to
// match a server-minted cookie against (shardpilot/shardpilot-go#84 review).
//
// The fields are the ones the response path already treats as server-generated.
// This half REFUSES them; the change stacked on it redacts them, and inherits this
// question rather than restating it.
func noteStructuralInText(text string) {
	for _, ln := range strings.Split(text, "\n") {
		low := strings.ToLower(strings.TrimSpace(stripMarks(ln)))
		// ⚠ CONTAINS, NOT HasPrefix. Go wraps the offending line in prose and quotes
		// -- `malformed HTTP response "Set-Cookie: …"` -- so a prefix test never
		// matched the shape the finding is about. Over-refusing an error diagnostic
		// that merely mentions one of these fields is the safe direction, and the
		// scene guards the other: an ordinary transport failure must stay reportable.
		// ⚠ AND THE MINTED-FIELD QUESTION. An endpoint can make its malformed first
		// line a JSON object carrying the subject key, and Go puts that whole line in
		// the error -- so the value reached the report through the diagnostic, where
		// no supplied value can match it and no body scan runs
		// (shardpilot/shardpilot-go#84 review). The question is about TEXT, not about
		// a body: asking it only where a body exists is asking it about the parse.
		// ⚠ THE NAMES ARE ESCAPED HERE. Go quotes the offending line, so the member
		// names arrive as `\"subject_fact_key\"` and the JSON member pattern -- which
		// expects a bare quote -- matched nothing. The check runs over identifier
		// tokens instead, which is the shape that survives any quoting.
		// ⚠ THE ESCAPES SURVIVE GO'S QUOTING AND NOT THIS SCAN. A malformed first
		// line may spell the member as `subject_\u0066act_key`, and splitting on
		// non-identifier bytes cuts that into `subject_` and `u0066act_key` --
		// neither of which is the name (shardpilot/shardpilot-go#84 review). My
		// previous fix here moved from the JSON pattern to identifier tokens
		// because the QUOTING defeated the pattern, and then met the escaping the
		// rest of this program already decodes everywhere else. The line is decoded
		// first, and both spellings are scanned: a decode can also join two names.
		// ⚠ TO A FIXED POINT, NOT ONE PASS EACH. `net/http` QUOTES the offending line
		// into the error it returns, so a member spelled `subject_\u0066act_key` on
		// the wire arrives here with TWO backslashes -- one pass reduces that to
		// `\u0066` and stops, and no scanned token equals the name
		// (shardpilot/shardpilot-go#84 review). Quoting adds a layer; a decoder
		// applied once removes one layer. The bound is the length, because each pass
		// that changes anything removes at least one byte.
		forms := []string{ln}
		cur := ln
		for k := 0; k <= len(ln); k++ {
			next := undoPercent(undoUnicodeEscapes(cur))
			if next == cur {
				break
			}
			cur = next
			forms = append(forms, cur)
		}
		for _, form := range forms {
			// ⚠ THE TOKENISER'S ALPHABET MUST BE AS WIDE AS THE PREDICATE'S.
			// `isMintedName` folds the way `encoding/json` does, so it MATCHES
			// `ſubject_fact_key` -- and this splitter, being ASCII-only, cut the name
			// at `ſ` and never handed it that token (shardpilot/shardpilot-go#84
			// review). A candidate the predicate would accept but the splitter cannot
			// produce is a predicate that is never asked.
			for _, tok := range strings.FieldsFunc(form, func(r rune) bool {
				return !(r == '_' || r == '-' || unicode.IsLetter(r) || unicode.IsDigit(r))
			}) {
				if isMintedName(tok) {
					noteStructural("a server-minted field inside a transport error")
				}
			}
		}
		// ⚠ THE THIRD SITE ASKING THE SAME QUESTION, and it had its own answer too.
		// This one listed all four names while the trailer report listed two, which
		// is how the drift stayed invisible: whichever site you read, it looked
		// complete (shardpilot/shardpilot-go#84 review). All three read
		// `serverMintedFields` now. Sorted, so a line naming two fields notes them
		// in a fixed order rather than in map order.
		// A `switch` would stop at the first match; an error line is free-form text
		// and can carry more than one field, so every match is noted.
		for _, name := range slices.Sorted(maps.Keys(serverMintedFields)) {
			if strings.Contains(low, name+":") {
				noteStructural(serverMintedFields[name] + " inside a transport error")
			}
		}
	}
}

// sanitize redacts any request URL an error carries. `url.Error` wraps the FULL
// url, so printing a deadline or transport failure verbatim republished the
// unredacted subject_key and every targeting value -- the same leak the request
// dump had already been fixed for, arriving by a second road
// (shardpilot/shardpilot-go#73 review).
// ⚠ AN ERROR FROM THE TRANSPORT CARRIES ENDPOINT BYTES. Go's parser puts the
// offending line into the error it returns, so a malformed response can place an
// encoded supplied value there -- and the result was written into report PROSE,
// outside any captured span, where the guard's decoders never look
// (shardpilot/shardpilot-go#84 review). It is marked as captured, so the same
// fixed-point check runs over it.
// sanitizeCaptured is for the REPORT, where the guard reads it. sanitize is for
// stderr, which the guard never sees and where a marker byte would print as a
// raw control character.
func sanitizeCaptured(err error) string {
	// ⚠ ESCAPE FIRST. Go puts the offending line into the error verbatim, so an
	// endpoint can put the guard's own reserved bytes there -- and an
	// attacker-controlled `\x01…\x01` pair reads as a GENERATED span, which the
	// guard blanks and `stripMarks` then publishes
	// (shardpilot/shardpilot-go#84 review). Provenance marks in captured text are
	// escaped everywhere else in this program; this path had been added without
	// them.
	// ⚠ ESCAPE THE RAW ERROR, BEFORE THE STAGE THAT CREATES MARKS OF ITS OWN.
	// `sanitize` runs `dropQuery`, which inserts a GENERATED `query-withheld`
	// token -- and this outer escape then read those freshly generated bytes as
	// endpoint bytes and rendered them literally, so after `stripMarks` the report
	// said the transport error contained a mark pair neither the transport nor the
	// endpoint ever produced (shardpilot/shardpilot-go#84 review). The order was
	// right for the byte it was written against and wrong for the byte the stage
	// beneath it adds.
	if err == nil {
		return asCaptured("")
	}
	noteStructuralInText(err.Error())
	return asCaptured(scrubSupplied(sanitizeText(escapeMarks(err.Error()))))
}

// sanitize renders an error for STDERR. The marks are stripped: they exist so the
// publication guard can tell captured bytes from generated ones, and a terminal
// gets raw SOH control bytes instead of a message
// (shardpilot/shardpilot-go#84 review). `sanitizeCaptured` keeps them, because
// that output is what the guard reads.
func sanitize(err error) string {
	return stripMarks(sanitizeRaw(err))
}

func sanitizeRaw(err error) string {
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
		red := dropQuery(seg)
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
// ⚠ THE STATUS LINE IS PROTOCOL SYNTAX, NOT DATA. A legal supplied identifier can
// equal a status token -- `SP_EXPERIMENT_KEY=200` -- and the generic scrub then
// rewrote `HTTP/1.1 200 OK` into `HTTP/1.1 <redacted, 3 chars> OK`, an unparsable
// response the guard nonetheless approved, because the replacement carries
// generated marks (shardpilot/shardpilot-go#73 review). There is no value a
// scrub could legitimately find there.
func scrubSupplied(text string) string {
	if strings.HasPrefix(text, "HTTP/") {
		if i := strings.IndexByte(text, '\n'); i >= 0 {
			return text[:i+1] + overCaptured(text[i+1:], scrubSuppliedRaw)
		}
		return text
	}
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

// nameComponents emits each field name AND each of its `-`-separated components
// on its own line, so a decoder stage can see a component boundary the wire
// spelling hid.
//
// ⚠ SPLITTING ONCE, BEFORE THE DECODERS, IS SPLITTING THE WRONG STRING. A legal
// field name may percent-encode the separator -- `X%2dYmFy` -- so the one-time
// split saw no hyphen, and when the percent stage later produced `X-YmFy` the
// name went on being treated as ONE url-base64 token: `YmFy` was never decoded,
// and the guard published a name from which the identifier is reconstructed by
// two supported decoders (shardpilot/shardpilot-go#84 review). The round before
// taught that base64's alphabet swallows the boundary; this one adds that the
// boundary can ARRIVE mid-chain.
func nameComponents(names string) string {
	var b strings.Builder
	// ⚠ DEDUPED, because this form FEEDS the next stage rather than only being
	// recorded -- and re-emitting every line plus its components each round grows
	// the text the decoders walk, which is charged to the same work budget.
	seen := map[string]bool{}
	emit := func(v string) {
		if v == "" || seen[v] {
			return
		}
		seen[v] = true
		b.WriteString(v)
		b.WriteByte('\n')
	}
	for _, ln := range strings.Split(names, "\n") {
		if ln == "" {
			continue
		}
		emit(ln)
		if strings.IndexByte(ln, '-') < 0 {
			continue
		}
		for _, part := range strings.Split(ln, "-") {
			emit(part)
		}
	}
	return b.String()
}

// markBareJSONLiterals wraps `true`, `false` and `null` in generated-provenance
// marks WHERE THEY STAND AS GRAMMAR -- after `:` `,` `[` and before `,` `}` `]`
// or the end.
//
// ⚠ THE EXEMPTION IS A POSITION, NOT A VALUE. Skipping the value outright let a
// response echo the key back as a STRING -- `"experiment_key":"false"` -- and
// neither the scrub nor the guard would touch it, so the supplied value was
// published (shardpilot/shardpilot-go#84 review). Marking the grammar instead
// costs nothing to maintain: `overCaptured` already leaves generated spans alone
// and the guard already blanks them, so both rules follow from one mark rather
// than from two copies of a list.
func markBareJSONLiterals(text string) string {
	// ⚠ PARSED, NOT GUESSED. The first version tested the bytes around the token,
	// which marks `{"message":"saw false value"}` and `error: false` as grammar --
	// and a marked span is skipped by BOTH the scrub and the guard, so the
	// supplied value was published (shardpilot/shardpilot-go#84 review). A
	// heuristic about where a token sits is not a statement about the grammar it
	// sits in. `encoding/json` knows which of them is a literal NODE; nothing
	// else does.
	//
	// A body that does not parse is left alone: there is no JSON grammar in it to
	// protect, so the ordinary scrub applies and the value is redacted.
	// ⚠ THE WHOLE BODY MODULO WHITESPACE, ON BOTH SIDES. Three rounds have now
	// found this function accepting something that is not one JSON document: a
	// body that merely CONTAINS a value, a body followed by more values, and now
	// a body PRECEDED by text -- `error: {"assigned":false}` was read as JSON
	// because the scan began at the first brace (shardpilot/shardpilot-go#84
	// review). Each round fixed the side it was shown. The rule is one value with
	// nothing but JSON whitespace around it, stated once, so there is no third
	// side left to be shown.
	start := 0
	for start < len(text) && (text[start] == ' ' || text[start] == '\t' || text[start] == '\n' || text[start] == '\r') {
		start++
	}
	if start >= len(text) || (text[start] != '{' && text[start] != '[') {
		return text
	}
	// ⚠ ONE VALUE, NOT A STREAM. `json.Decoder` reads a SEQUENCE of top-level
	// values, so `{"x":1} false` walked as valid and the trailing literal was
	// marked as grammar -- and a marked span is skipped by BOTH the scrub and the
	// guard, so a supplied value of `false` was published, while `json.Unmarshal`
	// rejects that body as a verdict outright (shardpilot/shardpilot-go#84
	// review). The comment above already said this function protects a GRAMMAR;
	// a stream of values is not the grammar this program's responses have.
	{
		v := json.NewDecoder(strings.NewReader(text[start:]))
		var one json.RawMessage
		if err := v.Decode(&one); err != nil {
			return text
		}
		if strings.TrimSpace(text[start+int(v.InputOffset()):]) != "" {
			return text
		}
	}
	dec := json.NewDecoder(strings.NewReader(text[start:]))
	type span struct{ a, b int }
	var spans []span
	// ⚠ THE SCHEMA'S MEMBER NAMES ARE GRAMMAR TOO, not only its literals. A legal
	// supplied key may equal a response member -- `assigned`, `experiment_key` --
	// and the generic scrub rewrote the NAME, so a successful report no longer
	// carried the verdict schema the endpoint sent, while the generated
	// provenance made the guard approve it (shardpilot/shardpilot-go#84 review).
	// The literals were marked here for exactly this reason and the names beside
	// them were not.
	//
	// ⚠ AND ONLY THE CANONICAL SPELLING. `isBenignName` folds, so vouching on
	// recognition would publish a supplied `ASSIGNED`; the registry's own spelling
	// is what this program writes, and the fold is only how it recognises. That is
	// the class the child branch spent three rounds on -- carried here rather than
	// repeated (shardpilot/shardpilot-go#85 review).
	//
	// Depth and turn are tracked because `Token()` returns a string for a key and
	// for a value alike: only position tells them apart.
	// 0 = array, 1 = object expecting a KEY, 2 = object expecting a VALUE.
	//
	// ⚠ TWO DEFECTS CAME OUT OF THE `[]bool` THIS REPLACES, both mine from the round
	// before. A bare "expecting a key" flag cannot say "this is an array", so the
	// toggle ran on array elements too; and closing a container popped the child
	// without advancing the PARENT's turn, so the member after a `{}` value was not
	// seen as a key -- `{"variant_payload":{},"version":1}` left `version`
	// unrecognised, and a supplied `version` rewrote a fixed schema member
	// (shardpilot/shardpilot-go#84 review).
	var objDepth []int8
	for {
		tok, err := dec.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			return text // malformed: no grammar to protect
		}
		advance := func() {
			// A container or a scalar in VALUE position consumes the parent's turn.
			if n := len(objDepth); n > 0 && objDepth[n-1] == 2 {
				objDepth[n-1] = 1
			}
		}
		if d, ok := tok.(json.Delim); ok {
			switch d {
			case '{':
				advance()
				objDepth = append(objDepth, 1)
			case '[':
				advance()
				objDepth = append(objDepth, 0)
			case '}', ']':
				if len(objDepth) > 0 {
					objDepth = objDepth[:len(objDepth)-1]
				}
			}
			continue
		}
		isKey, atRoot := false, false
		if n := len(objDepth); n > 0 && objDepth[n-1] == 1 {
			isKey, atRoot = true, n == 1
			objDepth[n-1] = 2
		} else {
			advance()
		}
		// ⚠ AT THE ROOT ONLY. `benignTopLevel` describes the SDK's TOP-LEVEL schema,
		// and marking those names at every depth exempted an endpoint-controlled
		// nested member of the same name -- `variant_payload` may carry
		// `{"assigned":"x"}`, whose `assigned` the endpoint chose
		// (shardpilot/shardpilot-go#84 review). A registry's scope is part of what it
		// says.
		if name, ok := tok.(string); ok && isKey && atRoot {
			if benignTopLevel[name] || mintedNames[name] {
				end := start + int(dec.InputOffset())
				quoted := `"` + name + `"`
				if end-len(quoted) >= 0 && text[end-len(quoted):end] == quoted {
					spans = append(spans, span{end - len(quoted), end})
				}
			}
			continue
		}
		switch tok.(type) {
		case bool, nil:
			end := int(dec.InputOffset())
			lit := "null"
			if b, ok := tok.(bool); ok {
				if b {
					lit = "true"
				} else {
					lit = "false"
				}
			}
			if end-len(lit) >= 0 &&
				text[start+end-len(lit):start+end] == lit {
				spans = append(spans, span{start + end - len(lit), start + end})
			}
		}
	}
	for k := len(spans) - 1; k >= 0; k-- {
		sp := spans[k]
		text = text[:sp.a] + marked(text[sp.a:sp.b]) + text[sp.b:]
	}
	return text
}

// jsonLiterals are the three bare tokens of JSON grammar. A supplied value equal
// to one of them cannot be distinguished from the grammar itself, and replacing
// it turned every not-assigned body into `{"assigned":<redacted, 5 chars>}` --
// invalid JSON that no longer states the endpoint's verdict
// (shardpilot/shardpilot-go#84 review). The guard still reads the body; what it
// would find there is the word `false`, which the endpoint did not learn from us.
var jsonLiterals = map[string]bool{"true": true, "false": true, "null": true}

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
// escapeMarks replaces the two reserved marker bytes with readable text, and the
// substitution must be REVERSIBLE, because the report claims it is.
//
// ⚠ PRE-ESCAPING THE COMPLETE SPELLINGS WAS NOT ENOUGH. That first fix separated
// a real NUL from the four wire bytes `\x00`, and still collided on a backslash
// sitting NEXT to a real marker: `\` + NUL and the wire bytes `\` + `\x00` both
// rendered as `\\x00` (shardpilot/shardpilot-go#73 review). A backslash run is
// therefore lengthened whenever what FOLLOWS it could be read as the escape --
// a marker byte, or the literal `x00`/`x01` -- so decoding a run of k
// backslashes before `x00` is unambiguous: k=1 is a real NUL, k>1 is k-1
// backslashes and the literal text.
//
// ⚠ AND ONLY THOSE RUNS ARE TOUCHED. Escaping every backslash would rewrite
// `\uXXXX` and `\xNN` as well, and assertNoLeak's decoders would stop
// reconstructing identifiers they currently catch. An injective escape that
// blinds the leak check is a worse trade than the ambiguity it fixes.
func escapeMarks(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] == '\\' {
			j := i
			for j < len(s) && s[j] == '\\' {
				j++
			}
			// ⚠ PARITY SEPARATES THE TWO FAMILIES. Adding ONE backslash was not
			// enough: the marker's own substitution contributes a backslash too, so
			// `\`+NUL and `\\x00` both landed on three. A run of k before a REAL
			// marker becomes 2k (the marker then adds its own, giving 2k+1 -- odd);
			// a run of k before the literal text becomes 2k+2 -- even. Decoding
			// reads the parity: odd is a marker with (m-1)/2 backslashes, even is
			// literal text with (m-2)/2 (shardpilot/shardpilot-go#73 review).
			n := j - i
			rest := s[j:]
			switch {
			case strings.HasPrefix(rest, capturedMark) || strings.HasPrefix(rest, genMark):
				n = 2 * n
			case strings.HasPrefix(rest, "x00") || strings.HasPrefix(rest, "x01"):
				n = 2*n + 2
			}
			b.WriteString(strings.Repeat(`\`, n))
			i = j
			continue
		}
		switch s[i : i+1] {
		case capturedMark:
			b.WriteString(`\x00`)
		case genMark:
			b.WriteString(`\x01`)
		default:
			b.WriteByte(s[i])
		}
		i++
	}
	return b.String()
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
	// ⚠ AND PROTOCOL SYNTAX IS NOT CAPTURED DATA. A legal supplied value can be
	// `GET` or `200`; the scrub deliberately leaves the request and status lines
	// alone, because rewriting them produces an unparsable message -- and this
	// collected them anyway, so the guard reported the method or the status code
	// as a surviving value and those keys could never produce a capture at all
	// (shardpilot/shardpilot-go#84 review). The exemption has to be the same on
	// both sides or the scrub's deliberate silence becomes the guard's false
	// alarm.
	//
	// The NAMES the boundary rule reads are collected per span and stop at the
	// blank line that ends a header block: a body line shaped like `X-foo-bar:
	// explanation` is prose, and reading it as a field name refused a completely
	// safe capture (same review).
	var captured, names strings.Builder
	for _, span := range capturedSpan.FindAllString(text, -1) {
		span = genSpan.ReplaceAllString(span, " ")
		inHead := true
		first := true
		for _, ln := range strings.Split(span, "\n") {
			bare := strings.TrimSuffix(strings.TrimSpace(stripMarks(ln)), "\r")
			if inHead && bare == "" {
				inHead = false
			}
			// ⚠ THE EXEMPTION IS POSITIONAL, NOT SHAPED. Applied to every line, a
			// plain-text BODY line that merely looks like a status line had its
			// first two tokens discarded before decoding -- so a percent-spelled
			// supplied value inside it was never checked and was published
			// (shardpilot/shardpilot-go#84 review). Only the message's own first
			// line is protocol syntax.
			// ⚠ AND THE SPAN MUST BE AN HTTP MESSAGE, not merely have a first line. The
			// trailer report wraps EACH trailer in its own captured span, so a trailer
			// named `X-YmFy` carrying the legal value `HTTP/1.1` was handed to `dataOf`
			// as though it were a request line: the value was dropped, the header name
			// was never collected, and `YmFy` was decoded as one url-base64 token
			// instead of splitting out `bar` (shardpilot/shardpilot-go#84 review).
			// Positional was the right correction one round ago and it was not the
			// whole one: a position inside something that is not a message means
			// nothing.
			keep, ok := bare, true
			if first {
				if isMessageStart(bare) {
					keep, ok = dataOf(bare)
				}
				first = false
			}
			if !ok {
				continue
			}
			if keep != bare {
				captured.WriteString(keep)
				captured.WriteString("\n")
				continue
			}
			captured.WriteString(ln)
			captured.WriteString("\n")
			if inHead {
				if i, ok := headerNameEnd(bare); ok {
					// One splitter for both sites: the extraction and every decode stage ask
					// the same question, and two copies of it drifted apart once already.
					// ⚠ REDUNDANT HERE, AND A MUTANT SURVIVES IT -- measured, and recorded
					// rather than fixtured around, like the bracket check elsewhere in this
					// program. Once the split feeds the decode stream, the first round splits
					// whatever this would have split, so removing this call fails nothing. It
					// stays because `nameForms[0]` is the pre-stage form a reader will assume
					// is complete, and because the redundancy costs one pass over a header
					// block.
					names.WriteString(nameComponents(bare[:i]))
				}
			}
		}
	}
	capturedNames := names.String()
	curNames := capturedNames
	nameForms := []string{capturedNames}
	var extra []string
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
	work := 0
	// ⚠ THE PROBE ACCUMULATOR IS RESET HERE, because the budget is PER RECORD.
	// Today it happens to reach zero on its own -- the collection points below are
	// exhaustive -- but "the four places that drain it are all of them" is a claim
	// that quietly stops being true when a fifth caller appears, and what it would
	// then cost is one record's probes making the NEXT record unpublishable.
	decodeWork = 0
	forms := []string{text}
	cur := text
	settled := false
	for i := 0; i <= len(text); i++ {
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
		// ⚠ EVERY STAGE IS CHARGED, NOT ONE PER ROUND. The budget counted
		// `len(cur)` once while FIVE full-text decoders ran, so a near-limit body
		// could do hundreds of MiB of scanning -- and retain that many full-size
		// intermediate forms -- before a nominal 64 MiB bound fired. A resource
		// limit that undercounts by the number of stages is not fail-closed
		// (shardpilot/shardpilot-go#73 review).
		for _, stage := range []func(string) string{undoPercent, undoUnicodeEscapes, undoBase64, undoHex, undoPlus, undoEntities} {
			// ⚠ THE NAMES DECODE TOO, IN LOCKSTEP. `capturedNames` was extracted
			// once from the RAW span, so a field name spelling a short supplied
			// value in an escape -- `X-%62ar` for `bar` -- decoded in the text but
			// never in the names, and the name-boundary rule that exists for
			// exactly that case never saw it (shardpilot/shardpilot-go#84 review).
			// ⚠ RE-SPLIT INTO THE STREAM, not beside it. Recording the split form and
			// decoding the unsplit one leaves the new component undecoded by every
			// later stage -- which is the defect, one indirection along: the form that
			// can reconstruct the value has to be the form the chain continues from.
			curNames = nameComponents(stage(curNames))
			nameForms = append(nameForms, curNames)
			// ⚠ THE EXTRA CANDIDATE GOES IN ITS OWN SLICE. base64 is MIME-WRAPPED at
			// column 76 and the scanner reads each line as its own token, so every
			// fragment decoded separately and no retained form held the value a
			// standard decoder reconstructs directly (shardpilot/shardpilot-go#84
			// review). It must NOT join `forms`: that slice's length is the
			// fixed-point comparison's arithmetic, and adding to it per stage made
			// the loop compare against a mid-round form and settle early -- three
			// existing decoder fixtures said so immediately.
			// ⚠ AND IT IS DECODED. Storing the normalised form alone checked the
			// wrapped SPELLING, which no supplied value ever equals -- the whole
			// point is that a MIME decoder reconstructs the value FROM it
			// (shardpilot/shardpilot-go#84 review). Both forms are kept: the
			// normalisation, and what base64 makes of it.
			// ⚠ WITHIN THE RUN, NOT ACROSS THE SPAN. Joining every field of the
			// whole captured text removed the header/body separator too, so a
			// header value ending in base64 letters merged into the encoded body
			// and the combined token decoded to something else -- while the
			// per-line candidates still could not reconstruct the value
			// (shardpilot/shardpilot-go#84 review). Only runs of CONSECUTIVE
			// lines that are entirely base64 alphabet are joined.
			// ⚠ AND BACK THROUGH EVERY DECODER. base64 can carry another supported
			// encoding -- base64 of `%61bcdefgh` -- and the decoded candidate was
			// checked as-is, so nothing ever percent-decoded it
			// (shardpilot/shardpilot-go#84 review). A candidate is an input to the
			// chain, not an answer from it.
			// ⚠ TO A FIXED POINT, NOT ONE PASS. base64 can carry TWO layers of
			// another encoding -- `%2561bcdefgh` -- and a single sweep of the
			// decoders left `%61bcdefgh` undecoded, while the ordinary chain
			// cannot reach it because it never un-wraps the base64
			// (shardpilot/shardpilot-go#84 review). The candidate joins the same
			// work budget, so a crafted body cannot spin it.
			norm := joinBase64Runs(cur)
			dec := undoBase64(norm)
			extra = append(extra, norm, dec)
			// ⚠ AND EVERY BINARY CANDIDATE RE-ENTERS THE CHAIN. These were appended
			// as-is and never decoded again, so `/yU2MWJjZGVmZ2g=` -- base64 of
			// `0xff%61bcdefgh` -- was retained in a form nothing percent-decoded,
			// while the ordinary chain cannot reach it because it never un-wraps
			// the base64 (shardpilot/shardpilot-go#84 review). A candidate is an
			// input to the chain, not an answer from it -- which this file already
			// said about the base64 decode, and then did not do for the binary one
			// standing beside it. Same budget, so a crafted body cannot spin it.
			// ⚠ AND THE NAME FORMS. These looked only at the captured TEXT, so a field
			// name whose component decodes to invalid UTF-8 -- `X-_2Jhcg`, where
			// `_2Jhcg` is url-base64 for `0xffbar` -- was examined as one unsplit token
			// and no candidate ever held `bar` (shardpilot/shardpilot-go#84 review). The
			// names decode in lockstep with the text everywhere else; the binary path was
			// added later and inherited none of that.
			bins := append(binaryCandidates(cur), binaryCandidates(norm)...)
			bins = append(bins, binaryCandidates(curNames)...)
			extra = append(extra, bins...)
			// ⚠ AND THE SUFFIX DECODES, AS CANDIDATES IN THEIR OWN RIGHT. See
			// base64SuffixCandidates: spliced back behind their separator they are
			// unreachable to the short-value matcher. They are SEEDS like the rest --
			// a candidate is an input to the chain, not an answer from it.
			sufs := append(base64SuffixCandidates(cur), base64SuffixCandidates(norm)...)
			sufs = append(sufs, base64SuffixCandidates(curNames)...)
			// AND the short hex forms, and the wrapped runs that share a line with
			// other text. Seeds like the rest: a candidate is an input to the chain.
			sufs = append(sufs, hexCandidates(cur)...)
			sufs = append(sufs, hexCandidates(curNames)...)
			sufs = append(sufs, shortBase64Candidates(cur)...)
			sufs = append(sufs, shortBase64Candidates(curNames)...)
			for _, w := range wrappedBase64Candidates(cur) {
				sufs = append(sufs, w)
				if d, ok := decodeBase64(w); ok {
					sufs = append(sufs, d)
				}
			}
			extra = append(extra, sufs...)
			// The suffix scans above are the quadratic term; collect what they spent
			// before the round's own check reads the budget.
			work += takeDecodeWork()
			if work > decodeWorkMax {
				return fmt.Errorf(
					"decoding exceeded its work budget (%d bytes examined); the record "+
						"is NOT publishable and was not printed", work)
			}
			for _, seed := range append(append([]string{dec}, bins...), sufs...) {
				d := seed
				for round := 0; round <= len(d) && work <= decodeWorkMax; round++ {
					before := d
					for _, st := range []func(string) string{undoPercent, undoUnicodeEscapes, undoBase64, undoHex, undoPlus, undoEntities} {
						work += len(d)
						d = st(d)
						work += takeDecodeWork()
						extra = append(extra, d)
					}
					if d == before {
						break
					}
				}
			}
			work += len(cur) + takeDecodeWork()
			if work > decodeWorkMax {
				return fmt.Errorf(
					"decoding exceeded its work budget (%d bytes examined); the record "+
						"is NOT publishable and was not printed", work)
			}
			cur = stage(cur)
			work += takeDecodeWork()
			forms = append(forms, cur)
		}
		next := cur
		// ⚠ THE INDEX IS THE STAGE COUNT PLUS ONE, and it is a stage count, not a
		// constant: it must name the form this round STARTED from. Adding a
		// decoder without moving it would compare against a mid-round form and
		// settle early.
		if next == forms[len(forms)-7] {
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
		// ⚠ THE SAME EXEMPTION AS THE SCRUB. `jsonLiterals` was consulted by
		// `scrubSuppliedRaw` alone, so a key of `false` stopped corrupting the
		// body and went on refusing every capture instead -- the fix moved the
		// defect rather than removing it (shardpilot/shardpilot-go#84 review). An
		// exemption honoured by one of two rules is a disagreement, not an
		// exemption.
		if v == "" {
			continue
		}
		for _, f := range append(append([]string{}, forms...), extra...) {
			if containsValue(f, strings.Join(nameForms, "\n"), v) {
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
// shortBase64Candidates retains what a SHORT unpadded base64 token decodes to.
//
// ⚠ THE REWRITE'S FLOOR IS NOT A STATEMENT ABOUT LEGAL VALUES -- the second time
// today, after the bare-hex floor. `undoBase64` rewrites in place, so its
// four-byte floor bounds the garbage a destructive pass may produce; but
// `decodeBase64` accepts `RawStdEncoding`, a one-character key travels as `YQ`
// and a two-character one as `YWI`, and no binary or suffix path took tokens
// below that floor either (shardpilot/shardpilot-go#84 review). Nothing is
// rewritten here, so the floor keeps protecting what it was chosen for.
func shortBase64Candidates(text string) []string {
	var out []string
	for i := 0; i < len(text); {
		j := i
		for j < len(text) && isBase64Byte(text[j]) {
			j++
		}
		if j == i {
			i++
			continue
		}
		tok := text[i:j]
		i = j
		if len(tok) < 2 || len(tok) > 3 {
			continue
		}
		if dec, ok := decodeBase64(tok); ok {
			out = append(out, dec)
		}
	}
	return out
}

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
		// ⚠ AND ITS SUFFIXES, because this tokeniser is MAXIMAL and `/` is in the
		// standard alphabet: `prefix/YWJjZGVmZ2g=` is ONE token that does not
		// decode, so the final path component -- which a standard decoder
		// reconstructs directly -- was never tried
		// (shardpilot/shardpilot-go#84 review). The binary-candidate path needed the
		// same correction; this one is where a decode that is valid UTF-8 lands.
		if dec, ok := decodeBase64(tok); ok {
			b.WriteString(dec)
		} else {
			wrote := false
			for _, st := range separatorStarts(tok, minToken) {
				if dec, ok := decodeBase64(tok[st:]); ok {
					b.WriteString(tok[:st])
					b.WriteString(dec)
					wrote = true
					break
				}
			}
			if !wrote {
				b.WriteString(tok)
			}
		}
		i = j
	}
	return b.String()
}

// undoHex decodes bare even-length hexadecimal tokens. `\x61` is covered by the
// escape decoder; `6162636465666768` is the same identifier with no syntax at
// all, and nothing in the chain touched it (shardpilot/shardpilot-go#73 review).
// Six is the floor -- three bytes, matching the shortest value undoBase64 can
// reach -- and, like that stage, it is destructive on purpose: every earlier form
// is retained and checked, so rewriting a token here cannot lose the plain one.
// hexCandidates retains what a bare-hex token decodes to, for tokens SHORTER than
// undoHex's destructive floor.
//
// ⚠ A FLOOR CHOSEN FOR ONE STAGE IS NOT A STATEMENT ABOUT LEGAL VALUES. `undoHex`
// rewrites in place, so its six-character floor is about how much garbage a
// destructive rewrite may produce -- while an experiment key of `ab` is legal and
// travels as `6162`, four characters, which that floor excluded from BOTH the
// rewrite and the binary candidates. The ordinary matcher then saw no literal
// `ab` and the guard approved a value a standard hex decoder reconstructs
// directly (shardpilot/shardpilot-go#84 review).
//
// These are CANDIDATES, not a rewrite: nothing is replaced, so the floor that
// protects the rewrite is left where it is and the short forms are covered anyway.
func hexCandidates(text string) []string {
	var out []string
	for i := 0; i < len(text); {
		j := i
		for j < len(text) && isHexByte(text[j]) {
			j++
		}
		if j == i {
			i++
			continue
		}
		tok := text[i:j]
		i = j
		if len(tok) < 2 || len(tok)%2 != 0 {
			continue
		}
		raw := make([]byte, 0, len(tok)/2)
		ok := true
		for k := 0; k+1 < len(tok); k += 2 {
			v, err := strconv.ParseUint(tok[k:k+2], 16, 8)
			if err != nil {
				ok = false
				break
			}
			raw = append(raw, byte(v))
		}
		if ok {
			out = append(out, string(raw))
		}
	}
	return out
}

func undoHex(text string) string {
	const minHex = 6
	var b strings.Builder
	for i := 0; i < len(text); {
		j := i
		for j < len(text) && isHexByte(text[j]) {
			j++
		}
		tok := text[i:j]
		if len(tok) < minHex || len(tok)%2 != 0 {
			if j == i {
				b.WriteByte(text[i])
				i++
				continue
			}
			b.WriteString(tok)
			i = j
			continue
		}
		if raw, err := hex.DecodeString(tok); err == nil && utf8.Valid(raw) {
			b.Write(raw)
		} else {
			b.WriteString(tok)
		}
		i = j
	}
	return b.String()
}

var binaryDecoders = []struct {
	isByte func(byte) bool
	pad    byte
	minLen int
	decode func(string) [][]byte
}{
	{isBase64Byte, '=', 4, func(tok string) [][]byte {
		var out [][]byte
		for _, enc := range []*base64.Encoding{
			base64.StdEncoding, base64.RawStdEncoding,
			base64.URLEncoding, base64.RawURLEncoding,
		} {
			if raw, err := enc.DecodeString(tok); err == nil {
				out = append(out, raw)
			}
		}
		return out
	}},
	{isHexByte, 0, 6, func(tok string) [][]byte {
		if len(tok)%2 != 0 {
			return nil
		}
		if raw, err := hex.DecodeString(tok); err == nil {
			return [][]byte{raw}
		}
		return nil
	}},
}

// binaryCandidates returns the raw decode of every token any binaryDecoders
// entry recognises, where that decode is NOT valid UTF-8 -- exactly the decodes
// the textual stages drop.
func binaryCandidates(text string) []string {
	var out []string
	for _, d := range binaryDecoders {
		for i := 0; i < len(text); {
			j := i
			for j < len(text) && d.isByte(text[j]) {
				j++
			}
			if d.pad != 0 {
				for j < len(text) && text[j] == d.pad {
					j++
				}
			}
			// ⚠ AND ITS SUFFIXES AT PLAUSIBLE SEPARATORS. This scan is MAXIMAL, so
			// `prefix/YWJjZGVmZ2g=` is one token that does not decode, and the final
			// path component -- which a standard decoder reconstructs directly -- was
			// never tried (shardpilot/shardpilot-go#84 review). A maximal tokeniser
			// answers about the longest run; the question is about every run a decoder
			// would accept.
			if tok := text[i:j]; len(tok) >= d.minLen {
				cands := []string{tok}
				for _, st := range separatorStarts(tok, d.minLen) {
					cands = append(cands, tok[st:])
				}
				for _, c := range cands {
					for _, raw := range d.decode(c) {
						if !utf8.Valid(raw) {
							out = append(out, string(raw))
						}
					}
				}
			}
			if j == i {
				i++
			} else {
				i = j
			}
		}
	}
	return out
}

func isHexByte(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// joinBase64Runs removes the wrapping from MIME base64 without touching anything
// else: consecutive lines made entirely of base64 alphabet are joined, and every
// other line is left where it is, separator included.
// wrappedBase64Candidates joins a base64 run that BEGINS after other text on its
// line, and ends before other text on the last one.
//
// ⚠ A WHOLE-LINE PREDICATE CANNOT SEE A RUN THAT SHARES ITS LINE. `joinBase64Runs`
// asks whether a line is entirely base64, so `prefix: YWJj\r\nZGVmZ2g=` was two
// separate tokens and neither decoded to the supplied value -- while a standard
// decoder applied to the substring ignores the CRLF and reconstructs it directly
// (shardpilot/shardpilot-go#84 review). MIME wrapping is about where a line BREAK
// falls, not about what else is on the line.
func wrappedBase64Candidates(text string) []string {
	lines := strings.Split(text, "\n")
	for i, ln := range lines {
		lines[i] = strings.TrimSuffix(ln, "\r")
	}
	suffix := func(s string) string {
		i := len(s)
		for i > 0 && isBase64Byte(s[i-1]) {
			i--
		}
		return s[i:]
	}
	prefix := func(s string) string {
		i := 0
		for i < len(s) && (isBase64Byte(s[i]) || s[i] == '=') {
			i++
		}
		return s[:i]
	}
	var out []string
	for i := 0; i < len(lines); i++ {
		head := suffix(lines[i])
		if head == "" || allBase64(lines[i]) {
			// A whole-base64 line is already joined by joinBase64Runs; this producer
			// exists for the runs that share a line with something else.
			continue
		}
		// ⚠ A BUILDER, NOT `+=`. Each `+=` copies the whole prefix accumulated so far,
		// so assembling a candidate over many short base64 lines is quadratic -- and it
		// happens BEFORE the decode budget charges anything, which is the same gap the
		// suffix probes had (shardpilot/shardpilot-go#84 review).
		var jb strings.Builder
		jb.WriteString(head)
		for j := i + 1; j < len(lines); j++ {
			// ⚠ A BLANK LINE IS WHITESPACE, NOT A BOUNDARY -- the same sentence this
			// file has now had to apply three times: to horizontal space inside a
			// line, to a blank line inside a whole-line run, and now to a blank line
			// inside a run that SHARES its first line. A standard decoder ignores
			// every CR and LF, so `prefix: YWJj\r\n\r\nZGVmZ2g=` reconstructs the
			// identifier in one step (shardpilot/shardpilot-go#84 review). Each time
			// the producer was new and the rule was not.
			if strings.TrimLeft(lines[j], " \t") == "" {
				// ⚠ HORIZONTAL-WHITESPACE-ONLY COUNTS AS BLANK HERE TOO. `joinBase64Runs`
				// normalises spaces and tabs before judging a line; this producer
				// recognised only the empty string, so a run crossing ` \t` terminated
				// early (shardpilot/shardpilot-go#84 review). Fourth time this file has
				// been told MIME ignores whitespace a line-based reading treats as
				// structure, and the third producer told separately.
				continue
			}
			if allBase64(lines[j]) {
				decodeWork += len(lines[j])
				if decodeWork > decodeWorkMax {
					break
				}
				jb.WriteString(lines[j])
				continue
			}
			if p := prefix(lines[j]); p != "" {
				out = append(out, jb.String()+p)
			}
			break
		}
	}
	return out
}

func joinBase64Runs(text string) string {
	lines := strings.Split(text, "\n")
	var b strings.Builder
	run := false
	for i, ln := range lines {
		bare := strings.TrimSuffix(ln, "\r")
		// ⚠ MIME IGNORES WHITESPACE INSIDE AN ENCODED LINE TOO, not only the CRLF
		// between them. A line ending in a space was rejected as a run, so
		// `YWJjZ \r\nGVmZ2g=` never rejoined while a MIME decoder reconstructs it
		// directly (shardpilot/shardpilot-go#84 review). Horizontal whitespace is
		// dropped before the line is judged and before it is joined.
		bare = strings.Map(func(r rune) rune {
			if r == ' ' || r == '\t' {
				return -1
			}
			return r
		}, bare)
		// ⚠ AND A BLANK LINE INSIDE A RUN IS WHITESPACE, NOT A BOUNDARY. A standard
		// base64 decoder ignores every CR and LF, so `YWJjZ\r\n\r\nGVmZ2g=`
		// reconstructs the identifier in one step -- while this ended the run at the
		// empty line and neither fragment decoded to anything
		// (shardpilot/shardpilot-go#84 review). Second time this function has been
		// told that MIME ignores whitespace the line-based reading treats as
		// structure; the first was horizontal space INSIDE a line.
		isRun := bare != "" && allBase64(bare)
		if run && bare == "" {
			continue
		}
		if isRun {
			b.WriteString(bare)
			run = true
			continue
		}
		if run {
			b.WriteByte('\n')
			run = false
		}
		b.WriteString(ln)
		if i < len(lines)-1 {
			b.WriteByte('\n')
		}
	}
	return b.String()
}

func allBase64(s string) bool {
	for i := 0; i < len(s); i++ {
		if !isBase64Byte(s[i]) && s[i] != '=' {
			return false
		}
	}
	return true
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
		// ⚠ `\U` IS EIGHT DIGITS, `\u` IS FOUR. Treating them alike consumed only
		// the first four of `\U0001F600`, yielding U+0001 and leaving `F600`
		// behind, so a non-BMP character in a supplied value was never
		// reconstructed (shardpilot/shardpilot-go#84 review).
		if text[i] == '\\' && i+9 < len(text) && text[i+1] == 'U' {
			if r, err := strconv.ParseUint(text[i+2:i+10], 16, 32); err == nil && r <= 0x10FFFF {
				b.WriteRune(rune(r))
				i += 9
				continue
			}
		}
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
			// ⚠ THE WHOLE JSON ESCAPE ALPHABET, not the three that pass through
			// unchanged. A supplied value may contain a control character -- `a\nb` is
			// a legal identifier -- and JSON spells it `a\\nb`, which an outer encoding
			// can hide as `%61%5Cnb`. The percent stage reconstructed the JSON
			// spelling and this switch left it there, so no retained form held the
			// value while a reader applying the same two decoders reconstructs it
			// (shardpilot/shardpilot-go#84 review). Listing the escapes that are
			// IDENTITY and omitting the ones that DENOTE is the whole defect: those
			// are exactly the ones a decode changes.
			switch text[i+1] {
			case '"', '\\', '/':
				b.WriteByte(text[i+1])
				i++
				continue
			case 'n':
				b.WriteByte('\n')
				i++
				continue
			case 'r':
				b.WriteByte('\r')
				i++
				continue
			case 't':
				b.WriteByte('\t')
				i++
				continue
			case 'b':
				b.WriteByte('\b')
				i++
				continue
			case 'f':
				b.WriteByte('\f')
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
func containsValue(text, names, v string) bool {
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
	// ⚠ FOLDED, LIKE THE SCRUB THAT PRODUCED THESE NAMES. `X-%53ecret` decodes to
	// `X-Secret`, and this test was case-sensitive while `scrubHeaderName` folds
	// -- so an encoded byte became a CASE variant only after decoding and slipped
	// between the two rules (shardpilot/shardpilot-go#84 review). Two rules about
	// the same names must agree about case as well as about spelling.
	if containsValueWith(names, v, isNameByte) ||
		containsValueWith(strings.ToLower(names), strings.ToLower(v), isNameByte) {
		return true
	}
	return containsValueWith(text, v, isWordByte)
}

// wordBefore and wordAt answer the boundary question about the RUNE at an edge,
// not about one byte of it.
//
// ⚠ EVERY NON-ASCII BYTE WAS A SEPARATOR. `isWordByte` is byte-based, so with a
// legal short key of `é` the unrelated endpoint text `αéβ` looked like a word
// boundary on both sides -- and the short-value rule, which exists precisely to
// avoid corrupting unrelated words, rewrote the middle of one and published
// altered endpoint evidence (shardpilot/shardpilot-go#84 review).
//
// The naive repair -- "treat every byte >= 0x80 as a word byte" -- moves the
// error the DANGEROUS way: non-ASCII punctuation would stop being a boundary,
// fewer matches would be found, and a value that should be scrubbed would
// survive. So the rune is decoded and classified: letters and digits are word
// characters, everything else is a boundary, and ASCII keeps exactly the
// behaviour its own predicate gives it, hyphen rules included.
func wordBefore(text string, i int, isWord func(byte) bool) bool {
	if i <= 0 {
		return false
	}
	r, _ := utf8.DecodeLastRuneInString(text[:i])
	if r < utf8.RuneSelf {
		return isWord(byte(r))
	}
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

func wordAt(text string, i int, isWord func(byte) bool) bool {
	if i >= len(text) {
		return false
	}
	r, _ := utf8.DecodeRuneInString(text[i:])
	if r < utf8.RuneSelf {
		return isWord(byte(r))
	}
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

func containsValueWith(text, v string, isWord func(byte) bool) bool {
	for i := 0; ; {
		j := strings.Index(text[i:], v)
		if j < 0 {
			return false
		}
		j += i
		startOK := !wordBefore(text, j, isWord)
		endOK := !wordAt(text, j+len(v), isWord)
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

// isMessageStart reports whether a line can BEGIN an HTTP message: a status line,
// or a request line ending in a version token. A trailer, a body line or a report
// fragment can look like either in its middle and never at its start.
func isMessageStart(bare string) bool {
	if strings.HasPrefix(bare, "HTTP/") {
		return true
	}
	i := strings.LastIndex(bare, " HTTP/")
	if i <= 0 {
		return false
	}
	// A request line is `<method> <target> HTTP/x.y`, and a method is a token with
	// no colon — which is what separates it from `X-Name: HTTP/1.1`.
	m, rest, ok := strings.Cut(bare[:i], " ")
	if !ok || m == "" || rest == "" {
		return false
	}
	for k := 0; k < len(m); k++ {
		c := m[k]
		if !(c >= 'A' && c <= 'Z') && !(c >= 'a' && c <= 'z') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

// dataOf returns the part of a line the guard must read, dropping canonical
// protocol SYNTAX this program re-serialises.
//
// ⚠ THE SYNTAX, NOT THE LINE. Dropping a whole request line also drops the
// request TARGET, which is the query -- the most value-bearing bytes in the
// capture -- and the fixture that pins "a mask must not swallow what it
// protects" said so immediately. A status line carries no data; a request line
// carries exactly one field of it.
func dataOf(bare string) (string, bool) {
	if strings.HasPrefix(bare, "HTTP/") {
		// ⚠ THE REASON PHRASE IS NOT SYNTAX. `Response.Write` re-serialises the
		// version and the numeric code, but it carries the PARSED reason through
		// -- so `HTTP/1.1 400 secret99` from an endpoint or proxy is server text,
		// and dropping the whole line published it while the scrub was
		// deliberately leaving the line alone (shardpilot/shardpilot-go#84
		// review). Version and code go; whatever follows them stays.
		f := strings.SplitN(bare, " ", 3)
		if len(f) == 3 {
			// ⚠ ON HTTP/2 THE PHRASE IS SYNTHESISED. The protocol carries only
			// `:status`; Go builds `resp.Status` as "200 OK" and DumpResponse
			// writes it, so returning it as captured data refused every HTTP/2
			// response whose key happened to be a standard phrase
			// (shardpilot/shardpilot-go#84 review). An HTTP/1 reason phrase is
			// still endpoint text and is still checked.
			if strings.HasPrefix(bare, "HTTP/2") {
				if code, err := strconv.Atoi(f[1]); err == nil && f[2] == http.StatusText(code) {
					return "", false
				}
			}
			return f[2], true
		}
		return "", false
	}
	i := strings.LastIndex(bare, " HTTP/")
	if i <= 0 {
		return bare, true
	}
	target := bare[:i]
	if j := strings.IndexByte(target, ' '); j >= 0 {
		target = target[j+1:]
	}
	// ⚠ THE ROUTE IS SDK SYNTAX, NOT CAPTURED DATA. It is a constant this program
	// did not choose and the endpoint did not send, so with a legal experiment
	// key of `assignment` the guard reported the SDK's own path as a survivor and
	// exited 4 on every run (shardpilot/shardpilot-go#84 review). Only the
	// variable part of the target is data.
	return strings.ReplaceAll(target, assignmentRoute, "/"), true
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
		endOK := !wordAt(text, i+len(v), isWord)
		startOK := !wordBefore(text, i, isWord)
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

	// The authority this program was pointed at, recorded before anything is sent:
	// the `Host:` line carries it, and it is not endpoint text. See configuredHost.
	if u, uerr := url.Parse(env("SP_REMOTE_CONFIG_URL")); uerr == nil {
		configuredHost = u.Host
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
	// Kept for the ordinary return path. It does NOT settle the worker before the
	// counter is read -- `os.Exit` runs no defers -- which is why the close is
	// also performed explicitly below.
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = client.Close(closeCtx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), captureDeadline)
	defer cancel()

	result, fetchErr := client.FetchExperimentAssignment(ctx, env("SP_EXPERIMENT_KEY"), nil)

	// ⚠ STOP THE TRAFFIC BEFORE COUNTING IT. An armed automatic exposure can have
	// its worker issue an ingest request AFTER the fetch returns, so the snapshot
	// below recorded zero while the recorder went on absorbing and counting that
	// request as the report was assembled -- and the printed claim, already
	// copied, said zero (shardpilot/shardpilot-go#84 review). The deferred Close
	// cannot settle it either: every path below calls `os.Exit`, which runs no
	// defers at all. A count that is a claim about the RUN cannot be taken at an
	// instant in the middle of it.
	// ⚠ AND THE CLOSE HAS TO SUCCEED, NOT MERELY BE CALLED. Discarding its error
	// left the same race one step further along: when the five-second context
	// expires, `Close` returns before the worker lanes are done, so background
	// requests still reach the recorder after the snapshot and the report prints
	// an off-route count that was already stale, or pairs the copied exchanges
	// with a later verdict (shardpilot/shardpilot-go#84 review). A capture whose
	// own accounting cannot be trusted is not a capture, so it refuses.
	func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if cerr := client.Close(closeCtx); cerr != nil {
			fmt.Fprintf(os.Stderr,
				"REFUSING TO PRINT: the client could not be stopped before the counters "+
					"were read (%v), so background traffic may have arrived after them and "+
					"the report would state a count it cannot stand behind.\n", sanitize(cerr))
			os.Exit(4)
		}
	}()

	rec.mu.Lock()
	exchanges := append([]exchange{}, rec.exchanges...)
	offRoute := rec.offRoute
	rec.mu.Unlock()
	if len(exchanges) == 0 {
		fmt.Fprintf(os.Stderr,
			"no request made: the SDK returned %v without issuing one, so this "+
				"run says nothing about the endpoint\n", sanitize(fetchErr))
		os.Exit(2)
	}

	var report strings.Builder
	fmt.Fprintf(&report, "# assignment capture — %s\n\n", time.Now().UTC().Format(time.RFC3339))
	// ⚠ PRINTED, SO THE CLAIM IS CHECKED. The configuration says the ingest leg
	// is not exercised; this is the run's own answer to that, and a non-zero
	// value means the SDK's exposure worker used the same transport while the
	// capture was being taken (shardpilot/shardpilot-go#84 review). A counter
	// nothing reads would be worse than no counter.
	fmt.Fprintf(&report, "Requests seen on other routes and NOT recorded: **%d**. "+
		"The ingest leg shares this transport; zero is the expected answer and the "+
		"reason it is printed rather than assumed.\n\n", offRoute)
	if len(exchanges) > 1 {
		fmt.Fprintf(&report, "The SDK made **%d attempts**. All are below; the verdict is the "+
			"last, because that is the one it acted on.\n\n", len(exchanges))
	}
	for i, ex := range exchanges {
		label := ""
		if len(exchanges) > 1 {
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
				"response arrived: %s\n\n", label, transportErrorLine(ex.transErr))
		case ex.truncErr() != nil:
			body := responseText(&ex)
			fmt.Fprintf(&report, "## Response%s — INCOMPLETE, and the SDK was told so\n\n"+
				"The body is not established as whole (%v). What arrived is below; it "+
				"is NOT a complete response.\n\n%s\n%s\n",
				label, incompleteBodyLine(&ex), fencedBlock(body), ex.trailerReport())
		default:
			respText := responseText(&ex)
			fmt.Fprintf(&report, respSection, label,
				fencedBlock(respText), ex.trailerReport())
		}
	}

	last := rec.last()
	fmt.Fprintf(&report, "## SDK verdict\n\n")
	fmt.Fprintf(&report, "    attempts: %d\n", len(exchanges))
	fmt.Fprintf(&report, "    status:   %d\n", last.status)
	fmt.Fprintf(&report, "    assigned: %t\n", result.Assigned)
	fmt.Fprintf(&report, "    protocol: %q\n", last.proto)
	// Scrubbed like everything else: a variant key may legally equal a supplied
	// identifier -- an experiment and a variant both named `control` is a valid
	// response -- and the property is that a supplied value is never printed
	// back WHEREVER it appears, not only in the body.
	// ⚠ ESCAPED FIRST. A variant key containing a reserved marker byte was DELETED
	// from the verdict by stripMarks -- `a<NUL>b` reported as `ab` -- while the
	// response block, which escapes before stripping, kept it. The artifact then
	// misstated the assignment the SDK served (shardpilot/shardpilot-go#84 review).
	fmt.Fprintf(&report, "    variant:  %q\n", verdictValue(result.VariantKey))
	fmt.Fprintf(&report, "    reason:   %q\n", stripMarks(scrubSupplied(vouchTaxonomy(result.Reason))))
	// The SDK's own classification. A 404 returns a usable result with
	// Code "not_found", Assigned false and a NIL error, so omitting this showed
	// only zero-valued fields and then called the run generically not-served --
	// losing the first-class verdict this program exists to report.
	fmt.Fprintf(&report, "    code:     %q\n", stripMarks(scrubSupplied(vouchTaxonomy(result.Code))))
	// ⚠ THROUGH THE SCRUB, LIKE EVERY OTHER VERDICT FIELD. A legal experiment key
	// is `123`, an assignment can be at version 123, and this line reintroduced it
	// verbatim AFTER the response block had redacted the matching JSON number --
	// and the verdict lines carry no captured provenance, so `assertNoLeak` does
	// not read them (shardpilot/shardpilot-go#84 review). "Wherever it appears" is
	// a claim about every printer, and this one had been left out because a number
	// did not look like text.
	fmt.Fprintf(&report, "    version:  %s\n", verdictVersion(result.Version))
	if fetchErr != nil {
		fmt.Fprintf(&report, "    error:    %s\n", sanitizeCaptured(fetchErr))
	}

	// THE ARTIFACT IS CHECKED BEFORE IT IS PUBLISHED, not as it is assembled.
	// One gate over the finished text, so a value that slipped through any one
	// of the scrub passes stops the record instead of riding out in it.
	if err := assertNoLeak(report.String()); err != nil {
		fmt.Fprintf(os.Stderr, "REFUSING TO PRINT: %v\n", err)
		os.Exit(4)
	}
	// ⚠ AND A SURFACE THIS HALF CANNOT REDACT IS THE SAME FACT AS A SURVIVING
	// LEAK: the program cannot show the bytes are safe to publish. It exits the
	// same way, and says which surface, so the operator knows what to look at
	// rather than that "something" was wrong.
	if len(structuralSurfaces) > 0 {
		fmt.Fprintf(os.Stderr,
			"REFUSING TO PRINT: the response carries %d server-generated surface(s) "+
				"this build cannot redact, so the capture is NOT publishable:\n",
			len(structuralSurfaces))
		for _, w := range structuralSurfaces {
			fmt.Fprintf(os.Stderr, "  - %s\n", w)
		}
		fmt.Fprintf(os.Stderr,
			"  These values are minted by the ENDPOINT, so no list of values this\n"+
				"  program supplied can reach them and the leak guard above cannot see\n"+
				"  them. Structural redaction is a separate change; until it lands, a\n"+
				"  refusal is the only honest answer.\n")
		os.Exit(4)
	}
	// A CAPTURE NOBODY RECEIVED IS NOT A CAPTURE. An ignored write error let a
	// report truncated by a full filesystem -- or never written at all -- be
	// followed by "SERVED" and exit 0 (shardpilot/shardpilot-go#73 review).
	// ⚠ A STREAM CANNOT PROMISE ALL-OR-NOTHING, so the claim says what is true.
	// `io.WriteString` may return a positive count WITH an error, so a prefix of
	// the report can already be on the pipe before this refusal runs -- and the
	// documented exit-4 clause said a report that could not be written whole was
	// not published (shardpilot/shardpilot-go#84 review). It cannot be unwritten.
	// What this program CAN do is say how many bytes escaped, so a consumer that
	// captured them independently of the exit status knows the artifact is a
	// fragment rather than a capture.
	if wn, werr := io.WriteString(os.Stdout, stripMarks(report.String())); werr != nil {
		fmt.Fprintf(os.Stderr,
			"REFUSING: the capture could not be written whole: %v\n"+
				"  %d byte(s) of it reached stdout before the failure and CANNOT be\n"+
				"  recalled; treat anything captured from this run as a fragment, not a\n"+
				"  capture.\n", werr, wn)
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
			stripMarks(scrubSupplied(vouchTaxonomy(result.Reason))), stripMarks(scrubSupplied(vouchTaxonomy(result.Code))))
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
