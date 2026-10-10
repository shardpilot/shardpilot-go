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
// The recording guarantee is bounded by the decoders, field registries
// and data shapes listed below. Encodings outside that decoder list are
// outside the supplied-value claim.
//
// It claims something checkable instead:
//
//	NOTHING IS PRINTED THAT THIS PROGRAM CANNOT ACCOUNT FOR
//
//	  a supplied value      absent from every form THE DECODERS BELOW produce, or
//	                        the record is not printed at all. The decoders are a
//	                        CLOSED LIST and the claim is relative to it:
//	                        undoPercent, undoUnicodeEscapes, undoBase64, undoHex,
//	                        undoPlus, undoEntities -- run to a fixed point, over
//	                        the text AND the field names, and over the candidates
//	                        the base64, binary and hex producers add.
//	  a header VALUE        every token drawn from a vocabulary the specification
//	                        fixes -- an HTTP-date in GMT, an integer, a registered
//	                        directive, a registered media type, the registered
//	                        reason phrase for the status code
//	  a field NAME          it is in the field-name registry, header and trailer
//	                        alike, including the names a `Trailer:` announces
//	  a parameter NAME      this program itself put it on the wire
//	  a cookie ATTRIBUTE    the specification names it, and its value comes from
//	                        that attribute's own fixed vocabulary
//	  a URI authority       Go's own parser accepts it; the host is exempt because
//	                        it is structurally constrained, the SCHEME is not
//	  protocol syntax       marked as generated, and so not read as captured data:
//	                        the method, the version, the status line, request
//	                        header names, the auth scheme, values `net/http`
//	                        writes into the dump, the SDK's fixed route, and the
//	                        three JSON grammar literals
//	  everything else       IN THE FORMS BELOW, replaced by its length. Outside
//	                        them the capture is REFUSED, not redacted.
//
// The structural-redaction guarantee covers the four forms below.
// Other forms cause the capture to be refused.
//
// The forms this program redacts:
//
//	a response BODY        that is exactly one JSON document. Its string values,
//	                       its member names outside the SDK's top-level schema,
//	                       and its numbers are covered; its grammar is marked.
//	                       A body that is not one JSON document is REFUSED.
//	a response FIELD       header or trailer, name and value alike, by the
//	                       registries and vocabularies named above.
//	the REQUEST            its route, its query and its own headers, which this
//	                       program itself put on the wire.
//	a transport DIAGNOSTIC its QUOTED extent, which is what Go marks as the
//	                       endpoint's.
//
// Anything else -- a body in another syntax, a diagnostic carrying UNQUOTED
// endpoint data, a coding this build cannot decode -- is a refusal. Saying which
// four shapes are covered turns "did you think of X" into "is X one of these
// four", which is a question with an answer.
//
// TestTheClaimNamesEveryLedgerReason reads every `noteStructural` and
// `noteAccounted` reason from the source and requires this claim to name it.
//
// Redaction accounts for recognized shapes; the guard refuses a capture
// containing a shape that none of these rules can describe.
//
// The supplied-value check covers the outputs of the named decoders.
// It makes no claim about every possible encoding.
//
// Naming the decoders makes the clause checkable in the way the others are: the
// claim speaks about what THOSE decoders reconstruct, and an encoding outside the
// list is outside the claim. Extending it is a decoder, a line in the list above,
// and a scene -- a bounded change with a visible cost, instead of a promise that
// quietly grows.
// TestTheClaimNamesExactlyTheDecodersThatRun holds the list and the code
// together, so the sentence cannot drift from the chain it describes.
//
// Each printed byte must be admitted by a clause above. A byte that no
// clause accounts for makes the capture unpublishable.
//
// The criterion is applied in redact.go and pinned by the sweep in
// sweep_test.go, which has three parts: forms that must not be published,
// clauses that must still admit what they admit, and the cases the sweep cannot
// structurally express, which are fixtured beside it.
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
//	4  the record could not be safely emitted: redaction left an unaccounted
//	   value or shape, or the output write failed. Withheld records write
//	   nothing; a write failure may leave a prefix on stdout and reports
//	   its byte count on stderr. This is a recorder failure and says
//	   nothing about whether the endpoint accepted the exchange.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/http/httputil"
	"net/textproto"
	"net/url"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
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
	// guard then ignored and published it.
	// A fact about a response stored once per RUN is a fact about the last response.
	recvConn bool
	// redirectLeg records that this exchange is a REDIRECT FOLLOW-UP rather than
	// an attempt of the SDK's own. Both are recorded and both are printed, but
	// they are different facts: retries say the SDK asked again, redirect legs say
	// the endpoint sent it elsewhere. Counting them together would report "2
	// attempts" for a single assignment that happened to be redirected once.
	redirectLeg bool
	transErr    error // set when no response arrived at all
	// reqDumpErr is the SERIALISER's failure, which is not the transport's.
	// `DumpRequestOut` rejects a request an operator can build -- an API key with
	// an embedded newline makes an invalid `Authorization` value -- and the error
	// was discarded, leaving `req` empty while the report went on to render an
	// empty canonical request block and say the request was formed. A record that omits the very evidence
	// it exists to carry must say so rather than print a blank.
	reqDumpErr error
	// uncompressed records that the TRANSPORT decoded the body. With
	// `http.DefaultTransport` adding `Accept-Encoding: gzip`, a gzip response is
	// decompressed before `RoundTrip` returns and the coding headers are removed,
	// so `teeBody` wraps the DECODED payload -- and the section's prose called
	// those "the received bytes".
	uncompressed bool
	// infos are the INFORMATIONAL responses, kept RAW and redacted by the RESPONSE
	// path at render time. Go's
	// transport consumes a `103 Early Hints` and exposes it only through
	// `httptrace.ClientTrace.Got1xxResponse`, so a bare `RoundTrip` returns the
	// final response alone -- and the report then claimed a complete captured pair
	// while silently dropping a status and headers the endpoint had sent. This harness records rather than
	// summarises; what it cannot show it must at least not omit in silence.
	infos []string
	// infoOverflow records that interim responses were DROPPED because the cap was
	// reached. A capture that silently omits them is the failure this section was
	// added to prevent.
	infoOverflow bool
	// closeAmbiguous records that `resp.Close` was set with no explicit framing and
	// no header-map entry, so the reconstructed `Connection: close` line cannot be
	// attributed to either side.
	closeAmbiguous bool
	// interimConn records, per interim block, whether IT carried a `Connection`
	// field. The final response's provenance says nothing about an interim one.
	interimConn []bool
	captured    *teeBody
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
	// as indeterminate discarded the one witness that settles it.
	if e.captured.err == nil && e.captured.overflowed {
		return errOversizedForCapture
	}
	// ⚠ EOF CAN BE SYNTHESISED ABOVE THIS WRAPPER. The SDK reads through
	// io.LimitReader, and when the limit is reached exactly, the limiter returns
	// EOF itself WITHOUT calling teeBody.Read again -- so `sawEOF` stays false on
	// a body that is complete, and a complete refusal reported exit 3 instead of
	// 1. The earlier test read the tee directly and performed the extra read that
	// production suppresses, which is why it passed. An exact Content-Length is a signal
	// visible ABOVE the limiter and settles the case without one.
	if e.captured.err == nil && e.captured.atCeiling && !e.captured.sawEOF &&
		e.captured.declared != int64(e.captured.buf.Len()) {
		return errOversizedForCapture
	}
	return e.captured.err
}

// recorderDiag is a diagnostic THIS PROGRAM wrote, travelling on the same
// `error` as the transport's own failures.
//
// ⚠ IT IS A TYPE, NOT A SENTINEL, BECAUSE THE QUESTION IS A PROPERTY. The
// describer below covers the transport's error types and refuses what it does not
// recognise -- correctly, because an endpoint's message is captured text. But
// `errOversizedForCapture` is an `*errors.errorString` this program constructed,
// so it fell through that refusal and `sanitizeCaptured` recorded an unexcused
// structural refusal: a large but perfectly valid JSON body exited 4 and withheld
// the report instead of publishing the documented incomplete capture at exit 3.
//
// Written as `errors.Is(err, errOversizedForCapture)` this would be the same
// enumeration one floor down, and the next program-owned diagnostic would
// reintroduce the defect. The property is "this program wrote every byte of it",
// and a type states it once for all of them.
type recorderDiag struct{ msg string }

func (e recorderDiag) Error() string { return e.msg }

// errOversizedForCapture is not the SDK's failure -- it is THIS RECORD's. The
// SDK may have handled the oversized body perfectly; what is incomplete is the
// copy, and the run is reported as an incomplete capture rather than as a
// complete one that happens to be short.
var errOversizedForCapture error = recorderDiag{
	"the body reached the read ceiling this record shares with the SDK, so whether " +
		"more followed is not knowable from here; the capture is reported incomplete " +
		"rather than guessed, and the SDK's own verdict above is unaffected"}

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
// separate header block in any case. The
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
			// Trailer names and values are captured data. Apply structural rules
			// before scrubbing a name, since the original name selects the rule.
			// Use the same field registry as the response headers; generic value
			// redaction is a fallback only when no structural rule applies.
			// A nonempty body with an unsupported trailer Content-Encoding is
			// refused. With no body bytes, there is nothing to decode.
			// Refusal notes are constants, never composed from a received name.
			low := strings.ToLower(k)
			// ⚠ A CODING CAN ARRIVE AS A TRAILER, AND THE HEAD-ONLY CHECK NEVER SEES IT.
			// `Trailer: Content-Encoding` with a final `Content-Encoding: gzip` is accepted
			// by Go and leaves the body encoded -- so a gzip-compressed supplied value
			// passed `assertNoLeak`, which has no gzip decoder, and was published as
			// opaque bytes while this very report carried the coding needed to rebuild it. The refusal is about what an
			// undecodable body could HIDE, and where the coding was declared does not
			// change what it hides.
			// ⚠ AND THE EMPTY-BODY EXEMPTION SURVIVES THE LIST-VALUED CHECK. Both halves
			// repaired this line: one taught it that a coding list is not one token, the
			// other that an absent body is not the subject of the refusal. Either alone
			// loses what the other learned.
			if low == "content-encoding" && len(e.body()) > 0 && !allIdentityCodings(v, true) {
				noteStructural(formField, "a body in a content coding this build cannot decode")
			}
			red, handled := structuralRedact(escapeMarks(k) + ": " + escapeMarks(v))
			// Apply the same finishing passes as response headers, preserving the
			// scheme and delimiters of a Location received as a trailer.
			if handled {
				red = vouchTargetSyntax(vouchScheme(red))
			}
			if !handled {
				// ⚠ MEMBERSHIP IS ONE QUESTION, HAVING A RENDERING IS ANOTHER. Asked
				// first, this table answered for `Set-Cookie` and `Location` and withheld
				// them, replacing the structural renderings above with a bare
				// `<withheld>` -- the rule the header path states, carried to its twin.
				if note, minted, whole := mintedFieldIn(k); minted || !whole {
					if !minted {
						noteStructural(formField, "a trailer field name whose decoded forms could not be enumerated")
						continue
					}
					noteStructural(formField, note)
					fmt.Fprintf(&b, "    %s\n", canonicalFieldName(low)+": "+marked("<withheld>"))
					continue
				}
				red = redactUnlessVerbatim(red)
			}
			// Admit trailer field names through the same registry as response headers.
			// scrubHeaderName alone cannot identify endpoint-chosen names absent from
			// the supplied-value set.
			if i := strings.IndexByte(red, ':'); i > 0 {
				red = admitFieldName(red[:i]) + red[i:]
			}
			// Mark the field delimiter as syntax too. A supplied colon must not
			// replace the separator between a vouched name and value.
			fmt.Fprintf(&b, "    %s\n", asCaptured(scrubSupplied(emitField(red))))
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
// of keeping one.
//
// REMOVED rather than recomputed. A recomputed length would describe the
// redacted body and quietly assert that this is what arrived; the header is
// dropped and its absence says the body was altered, which is the true thing.
// fenceFor returns a backtick run longer than any run inside the content, so a
// captured body cannot close its own container.
//
// A non-JSON error body carrying a line of three backticks ended the fence early
// and let the remainder appear as report prose -- forged verdict sections in an
// artifact whose whole purpose is to be published.
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
// bodies too.
// fencedBlock renders content inside a Markdown fence WITHOUT altering it.
//
// ⚠ ITS PREDECESSOR APPENDED A NEWLINE WHEN THE CONTENT LACKED ONE -- the normal
// shape for compact JSON. With the framing headers removed, a parser reads the
// recorded response as close-delimited, so that manufactured byte became payload
// and the artifact no longer held the body the SDK received. The newline a fence needs is the
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
// that had nothing to do with redaction.
// redactSetCookie keeps `Set-Cookie: <name>=` and every attribute after the
// first `;`, and replaces only the value.
//
// ⚠ IT MUST RUN BEFORE THE SUPPLIED-VALUE SUBSTITUTION, which is why the call
// sites read `scrubSupplied(dropFraming(...))` and not the reverse. With the
// old order, a cookie whose value equalled a supplied identifier was already
// `<redacted, N chars>` by the time this function measured it, so the header
// reported the PLACEHOLDER's length -- an 8-byte cookie printed as
// `<redacted, 19 chars>`, a second wrong number where this function promises
// the real one.
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
// parsable. The token-safe fix was there;
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
// report exists to record. Eleventh site of
// one rule: what this program put there is not the endpoint's choice.
//
// The failure direction of a MISSING entry is safe: an unrecognised value is
// scrubbed, which costs a label rather than publishing endpoint text. That is why
// this list may be a list, unlike the ones whose gaps publish.
//
// ⚠ AND PART OF THE TAXONOMY IS GENERATED, WHICH NO LIST CAN HOLD. The SDK builds
// `"http_" + strconv.Itoa(status)` and `"transient_" + strconv.Itoa(status)`, so
// the families are unbounded and the four entries here covered none of them --
// every `http_503` verdict lost its classification to a placeholder. The fixed codes below are read off the
// SDK's own enumeration; `isSDKTaxonomy` covers the generated ones.
//
// The failure direction of a MISSING entry is safe: an unrecognised value is
// scrubbed, which costs a label rather than publishing endpoint text. That is why
// this may be a list at all -- but a label lost on every transient failure is a
// cost paid on ordinary captures, not on strange ones.
var sdkTaxonomy = set(
	"kill_switch",
	"targeting_unmatched",
	"age_ineligible",
	"not_found",
	// The rest are read off the doc comment that enumerates the taxonomy in the
	// SDK source, not remembered; `TestTheTaxonomyCoversTheSDKsOwnEnumeration`
	// parses that comment and compares.
	"superseded",
	"unauthorized",
	"bad_request",
	"malformed_response",
	"stale_subject",
	"experiment_key_required",
	"subject_unavailable",
)

// isSDKTaxonomy answers whether a string is one this SDK writes as a
// classification.
//
// ⚠ BY EQUALITY WITH THE CANONICAL SPELLING, INCLUDING FOR THE GENERATED FAMILIES.
// A prefix test would admit `http_007` and `http_+7`, which this SDK never writes
// and an endpoint may well echo -- so the suffix is parsed and RE-RENDERED, and
// only an exact match vouches. Recognising a token is not having written it; the
// canonical spelling is what this program would have produced.
func isSDKTaxonomy(s string) bool {
	if sdkTaxonomy[s] {
		return true
	}
	for _, prefix := range []string{"http_", "transient_"} {
		rest, ok := strings.CutPrefix(s, prefix)
		if !ok {
			continue
		}
		n, err := strconv.Atoi(rest)
		if err == nil && n >= 0 && prefix+strconv.Itoa(n) == s {
			return true
		}
	}
	return false
}

// vouchTaxonomyIn marks this SDK's own classification wherever it appears inside
// a rendered error.
//
// ⚠ THE TAXONOMY REACHES THE ERROR PATH TOO. `result.Code` and `result.Reason`
// are vouched, and the SDK also puts the same classification INSIDE the error it
// returns -- `…fetch failed: http_0`. With that string as a legal experiment key
// the generic scrub rewrote it to a placeholder, and the report's exit status
// depends on precisely that token. Same
// vocabulary, second printer; vouching it in one place is vouching it in one
// place.
// sdkErrorPrefixes are the exact prefixes this SDK writes before its own
// classification. Read off the SDK source by
// `TestTheSDKErrorPrefixesAreTheOnesTheSDKWrites`, not recalled.
var sdkErrorPrefixes = []string{
	"shardpilot experiment assignment fetch failed: ",
	"shardpilot remote config fetch failed: ",
}

func vouchTaxonomyIn(text string) string {
	// ⚠ THE POSITION, NOT THE TOKEN, AND THIS TEXT IS NOT ALL OURS. Marking a
	// taxonomy word WHEREVER it appears was right for the SDK's own wrapper and
	// wrong for everything else that reaches this printer: an arbitrary `net/http`
	// diagnostic such as `malformed HTTP response "unauthorized"` is endpoint text
	// end to end, and with an experiment key of `unauthorized` the word was marked
	// as generated, ignored by the guard, and published verbatim. The SDK makes a token its own by
	// WRITING it at a known position; matching that token elsewhere is recognition,
	// which is not authorship.
	for _, prefix := range sdkErrorPrefixes {
		// ⚠ THE ERROR IS THE WRAPPER, NOT AN ERROR CONTAINING IT. `strings.Index`
		// found the prefix anywhere, so an endpoint that puts the wrapper text INSIDE
		// its own diagnostic -- `malformed HTTP response "shardpilot experiment
		// assignment fetch failed: unauthorized"` -- got its own bytes vouched as this
		// SDK's classification. Finding a prefix
		// is not being written by the thing that writes it, which is the same
		// recognition-is-not-authorship mistake one level up from the token.
		//
		// `fmt.Errorf("shardpilot ... failed: %s", code)` puts the wrapper at the
		// START of the message and nowhere else, so that is the only position that
		// establishes provenance.
		if !strings.HasPrefix(text, prefix) {
			continue
		}
		i := 0
		rest := text[len(prefix):]
		j := 0
		for j < len(rest) && isWordByte(rest[j]) {
			j++
		}
		if tok := rest[:j]; tok != "" && isSDKTaxonomy(tok) {
			text = text[:i+len(prefix)] + marked(tok) + rest[j:]
		}
	}
	return text
}

// vouchTaxonomy marks a verdict field this SDK generated.
func vouchTaxonomy(v string) string {
	if isSDKTaxonomy(v) {
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
// SURVIVED it -- the test was checking its own copy of the pipeline. Naming it once removes the copy.
func responseText(ex *exchange) string {
	// The per-exchange fact is put where `dropFraming` reads it, for THIS exchange,
	// immediately before it is rendered.
	receivedConnection = ex.recvConn
	// ⚠ THE INCOMPLETENESS TRAVELS WITH THE BODY. The SDK sets `bodyIncomplete` and
	// refuses the verdict BEFORE decoding, and neither the exemption registry nor the
	// `reason` vouch could see it: both asked only about SIZE, and a body that is
	// UNDER the limit but ended short -- a declared `Content-Length` larger than what
	// arrived -- is complete JSON to them. A
	// size test answers a different question than the SDK's gate does.
	//
	// Keep the configured authority available to the shared registry function.
	prev := capturedIncomplete
	capturedIncomplete = ex.truncErr() != nil
	defer func() { capturedIncomplete = prev }()
	prevLen, prevRaw := capturedBodyBytes, capturedBodyRaw
	capturedBodyBytes = len(ex.body())
	// Compare the bytes as well as their count. escapeMarks runs before
	// dropFraming, so a literal \x00 spelling gains backslashes. The shape check
	// must account for that escaping to preserve schema-member exemptions;
	// losing them lets the scrub replace assigned and alter the JSON body.
	capturedBodyRaw = string(ex.body())
	defer func() { capturedBodyBytes, capturedBodyRaw = prevLen, prevRaw }()
	return asCaptured(scrubSupplied(dropFraming(escapeMarks(string(ex.resp())))))
}

// renderExchanges writes the per-exchange sections.
//
// Keep report rendering separate so tests can inspect the headings,
// request and response sections, and per-exchange refusal records.
func renderExchanges(report *strings.Builder, exchanges []exchange) []exchangeRefusals {
	var perExchange []exchangeRefusals
	for i, ex := range exchanges {
		currentExchange = i
		beforeRender := len(structuralSurfaces)
		label := ""
		if len(exchanges) > 1 {
			label = fmt.Sprintf(" %d", i+1)
		}
		if ex.reqDumpErr != nil {
			// ⚠ NAMED, NOT BLANK, AND THE REST OF THE SECTION STILL RENDERS. The
			// serialiser refused, so there is no request
			// evidence -- and a section that prints an empty block under a heading
			// saying the request was formed is the artifact asserting what it does not
			// have.
			noteStructural(formRequest, "a request this build could not serialise for the record")
			fmt.Fprintf(report, "## Request%s — NOT CAPTURED\n\n"+
				"`httputil.DumpRequestOut` refused this request, so no canonical "+
				"serialisation exists to publish. The transport's own outcome is "+
				"reported below; this section carries no evidence and does not "+
				"pretend to.\n\n", label)
			// ⚠ AND `continue` MADE THAT SENTENCE FALSE. Skipping the rest of the
			// iteration dropped the informational blocks, the response, and the
			// recorded transport error -- so the artifact promised the outcome below
			// and printed nothing. The missing
			// evidence is the REQUEST; everything else was recorded and is still owed.
		} else {
			reqText := asCaptured(string(ex.req))
			// ⚠ NOT "as the SDK sent it". DumpRequestOut serialises the request as
			// HTTP/1.1 through a separate fake transport, BEFORE the real one
			// negotiates a protocol. On an HTTP/2 connection the report paired a
			// fabricated `HTTP/1.1` request line with a genuine `HTTP/2.0` status
			// line and called the two a single wire exchange. The negotiated protocol is
			// reported beside it, from the response, which is measured rather than
			// serialised.
			wire := ex.proto
			if wire == "" {
				wire = "not established — no response arrived"
			}
			fmt.Fprintf(report,
				"## Request%s — canonical HTTP/1.1 representation\n\n"+
					"Serialised by `httputil.DumpRequestOut`, which always writes HTTP/1.1. "+
					"The connection negotiated **%s**, so this is the request's canonical form "+
					"and its header set, not the bytes on the wire.\n\n%s\n",
				label, wire, fencedBlock(reqText))
		}
		if ex.closeAmbiguous {
			noteStructural(formField, "a reconstructed Connection line whose provenance cannot be established")
		}
		if ex.infoOverflow {
			noteStructural(formDiagnostic, "interim responses beyond this build's cap, which were not captured")
			fmt.Fprintf(report, "## Informational%s — INCOMPLETE\n\nThe endpoint sent "+
				"more interim responses than this build retains (%d blocks or %d bytes), "+
				"so what follows is a prefix and the record is not publishable.\n\n",
				label, maxInterimResponses, maxInterimBytes)
		}
		for infoNo, info := range ex.infos {
			// ⚠ THIS BLOCK'S OWN PROVENANCE. The final response's answer says nothing
			// about an interim one.
			receivedConnection = false
			if infoNo < len(ex.interimConn) {
				receivedConnection = ex.interimConn[infoNo]
			}
			// ⚠ PRINTED, NOT SUMMARISED. These arrived from the endpoint and the final
			// response does not contain them; a pair rendered without them is a pair
			// that omits what it saw.
			fmt.Fprintf(report, "## Informational%s — an interim response the "+
				"transport consumed — CANONICAL RECONSTRUCTION\n\nGo delivers these "+
				"only through `httptrace`, which hands over the numeric code and the "+
				"headers and nothing else — so the status line below is BUILT here from "+
				"`http.StatusText`, not received: a custom reason phrase the endpoint "+
				"sent is replaced by the registered one, and on HTTP/2 no textual status "+
				"line was received at all. The header block is likewise re-serialised "+
				"from the parsed fields, and a field the PARSER CONSUMED is not among them: "+
				"transfer processing runs before this callback and removes `Connection`, "+
				"so an interim that carried it is reconstructed without it. What this "+
				"section is evidence of is the CODE and the headers THE CALLBACK WAS GIVEN "+
				"; the bytes are ours, exactly as in the response section "+
				"below. A report without it would omit a status and headers the endpoint "+
				"did send.\n\n%s\n", label, fencedBlock(asCaptured(scrubSupplied(dropFraming(info)))))
		}
		switch {
		case ex.transErr != nil:
			fmt.Fprintf(report, "## Response%s\n\nNONE — the request was formed and no "+
				"response arrived: %s\n\n", label, transportErrorLine(ex.transErr))
		case ex.truncErr() != nil:
			body := responseText(&ex)
			fmt.Fprintf(report, "## Response%s — INCOMPLETE, and the SDK was told so\n\n"+
				"The body is not established as whole (%v). What arrived is below; it "+
				"is NOT a complete response.\n\n%s\n%s\n",
				label, incompleteBodyLine(&ex), fencedBlock(body), ex.trailerReport())
		default:
			respText := responseText(&ex)
			if ex.uncompressed {
				// ⚠ THE TRANSPORT DECODED IT, SO THE PROSE MUST NOT SAY OTHERWISE.
				fmt.Fprintf(report, "\n**The body below was DECODED BY THE TRANSPORT.** "+
					"`http.Transport` added `Accept-Encoding: gzip` on its own, "+
					"decompressed the response and removed the coding headers before this "+
					"recorder saw it, so what follows is the payload delivered to the SDK, "+
					"not the bytes on the wire.\n")
			}
			fmt.Fprintf(report, respSection, label,
				fencedBlock(respText), ex.trailerReport())
		}
		// Everything this attempt added to the ledger, and only it. See the comment
		// above the loop: the excuse belongs to the attempt that earned it.
		//
		// `noteStructural` de-duplicates by reason. If a truncated attempt
		// records a reason before a complete attempt would record the same
		// reason, the latter instance can be excused too. This limitation can
		// permit publication; avoiding it requires an exchange-specific ledger.
		perExchange = append(perExchange, exchangeRefusals{
			truncated:     ex.truncErr() != nil,
			jsonTruncated: truncationCausedTheFailure(ex.body()),
			added:         append([]string{}, structuralSurfaces[beforeRender:]...),
		})
	}
	return perExchange
}

// respSection is the response section's prose, named rather than inlined so the
// fixture that checks what it CLAIMS reads the same bytes the report prints. A
// test carrying its own copy of the sentence would pass while the report says
// something else.
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
	"evidence that it was received. AND A FIELD THE PARSER CONSUMED IS NOT HERE " +
	"AT ALL: a `Connection` option naming another header makes net/http remove " +
	"BOTH before this recorder sees them, so a response that sent " +
	"`Connection: close, X-Secret` is reconstructed without `X-Secret`. This " +
	"block is the header set THIS PROGRAM WAS GIVEN.\n\n%s\n%s\n"

// receivedConnection records whether the ENDPOINT sent a `Connection` field, as
// opposed to the serialiser adding one. See where it is set.
// configuredHost is the authority THIS PROGRAM was pointed at. The `Host:` line
// carries it, and it is not endpoint text.
var configuredHost string

var receivedConnection bool

// ── the minted-field test, shared with the half this change builds on ────────
//
// The guard and redactor share the same minted-field classification.
// configuredHostWire is configuredHost as the serializer writes it:
// DumpRequestOut emits internationalized host names as punycode, so
// comparisons must use that wire spelling.
//
// ⚠ ASKED OF THE SERIALISER, NOT COMPUTED. Deriving punycode here would be a
// second implementation of someone else's grammar, and this module carries no
// external dependency to borrow the first. The spelling is read out of a dump of a
// throwaway request to the configured URL -- produced by the very function that
// will produce the real one, offline, before anything is sent.
var configuredHostWire string

// capturedBodyBytes is the length of the body AS THE SDK READ IT, or -1 when this
// pass is run directly by a scene. The report path sets it, because by the time
// `dropFraming` runs the text has been through `escapeMarks`.
var capturedBodyBytes = -1

// capturedBodyRaw is the body AS THE SDK READ IT, before this program's own mark
// escaping. The schema checks decode it rather than the escaped text.
var capturedBodyRaw string

// mintedNames are the fields the SERVER mints -- the fact lane's subject and its
// privacy boundary, defined as such in experiments.go.
// serverMintedFields names the response fields whose VALUE the ENDPOINT mints.
// No list of values THIS program supplied can reach such a value, and the
// decoders behind the guard cannot either, so the value is withheld and the
// record is not publishable.
//
// Store each refusal note with its field name so callers do not compose
// notes from received spelling. decodeWorkMax bounds bytes examined by
// the decoding chain; exceeding the budget withholds the record.
// producerWork counts candidate-producing work since the caller's reset.
var producerWork int

// maxInterimResponses and maxInterimBytes bound what one exchange retains from
// `Got1xxResponse`. Installing that callback makes `net/http` reset its
// response-header allowance after every delivered interim response, so the
// per-response ceiling stops bounding the aggregate.
const (
	maxInterimResponses = 32
	maxInterimBytes     = 1 << 20
)

const decodeWorkMax = 64 << 20

// decodeWork is what the suffix probes below have spent since the caller last
// collected it. The chain's budget lived entirely in the caller, charging
// `len(cur)` per stage -- and the suffix scans inside `undoBase64` and
// `binaryCandidates` are QUADRATIC in the length of a maximal run, so a body of
// thousands of separator bytes did work the budget never saw: 20,000 `/` bytes
// alone cost about 2.5 seconds against an accepted body ceiling near 1 MiB, so
// an endpoint could hold post-processing far past the capture deadline. A resource limit that does not count the
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
// it, and the guard approved a directly reconstructable value. The splice answers "what does this text
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
		// punctuation -- fell between two fixes that each covered half of it. The combination of two rules is a third
		// rule, and neither of them stated it.
		for _, st := range separatorStarts(tok, 2) {
			// ⚠ THE SAME TWO QUESTIONS AS A STANDALONE SHORT TOKEN. `decodeBase64` keeps
			// only a valid-UTF-8 decode and `binaryCandidates` starts at four bytes, so
			// `/2E` -- 0xff 0x61 -- was retained when it stood alone and LOST when it
			// followed a separator: `zz//2E` passed with `a` supplied. Fixing the standalone case left the
			// same token unmeasured one position along.
			text, haveText, bin, haveBin := base64Answers(tok[st:])
			switch {
			case haveText:
				out = capSeeds(out, text)
			case haveBin:
				out = capSeeds(out, bin)
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
// The structural-redaction path uses this same registry declaration.
//
// ⚠ AND IT IS THE WIRE STRUCT'S MEMBERS, NOT NAMES FROM THE SAME VOCABULARY.
// `attributes` and `assignment_unit` were in here and are not top-level members
// of `expAssignmentWire` at all -- `assignment_unit` lives inside `boundary` --
// so a legal supplied value of `attributes` was marked as generated grammar in a
// 200 response and published. Belonging to
// the same protocol is not being a member of this object, and `assignment_key`
// was missing for the same reason the other two were present: the list was
// recalled rather than read off the struct.
//
// TestTheTopLevelExemptionsAreExactlyTheWireMembers compares this
// registry with the SDK's unexported wire structs in both directions.
// assignmentTopLevel applies to assignment responses and errorTopLevel
// to error responses: a fixed member in one shape can be endpoint-chosen
// text in the other. Depth and response shape both constrain exemptions.
var assignmentTopLevel = map[string]bool{
	"assigned": true, "variant_key": true, "variant_payload": true,
	"version": true, "reason": true, "boundary": true,
	"experiment_key": true, "assignment_key": true,
	"app_key": true, "environment_key": true,
	"served_revision": true, "served_kill_gate": true, "served_at": true,
}

// ⚠ `code` IS NOT AMONG THEM. It is a member of no top-level shape: the ingest
// error envelope spells it `error.code`, nested, and the assignment struct has no
// such member -- the SDK synthesizes its `Code` from the HTTP outcome.
var errorTopLevel = map[string]bool{"error": true}

// benignTopLevel answers a DIFFERENT question: is this member name one this build
// knows at all. That question is asked to REFUSE an unfamiliar member, so its
// answer must be the union -- narrowing it there would refuse a legitimate name
// from the other shape. Marking asks whether the name is grammar HERE, and takes
// the shape-specific set. One name for two questions is how the first became the
// second.
var benignTopLevel = func() map[string]bool {
	m := map[string]bool{}
	for k := range assignmentTopLevel {
		m[k] = true
	}
	for k := range errorTopLevel {
		m[k] = true
	}
	return m
}()

// sdkWouldParseAssignment reports whether a body decodes into the SDK's assignment
// wire shape at all. A complete, under-limit 200 may still be valid JSON that is
// NOT an assignment -- `{"assigned":"x"}` is rejected by the typed decode as
// `malformed_response` -- and exempting its member names marked a supplied
// identifier as generated. Status and size say
// whether the SDK would LOOK; this says whether it would find a verdict.
//
// ⚠ LIMIT: the text this pass holds has been through `escapeMarks`, so a body
// carrying literal backslash spellings may fail to decode here though the SDK
// accepted the captured bytes. That fails CLOSED -- no exemptions -- which is the
// direction this file refuses in.
// sdkWouldReadErrorText mirrors `experimentBodyErrorText`: the envelope's `error`
// member decodes into a STRING or the SDK reads nothing from it.
func sdkWouldReadErrorText(body string) bool {
	var wire struct {
		Error string `json:"error"`
	}
	return json.Unmarshal([]byte(body), &wire) == nil
}

func sdkWouldParseAssignment(body string) bool {
	var wire sdkAssignmentWire
	if json.Unmarshal([]byte(body), &wire) != nil || wire.Assigned == nil {
		return false
	}
	// ⚠ AND EVERY SEMANTIC GATE AFTER THE UNMARSHAL, not just the typed decode. The
	// SDK keeps validating: echoed identity, a version that decodes to a number, and
	// on the assigned branch a version of at least 1, non-empty assignment and variant
	// keys, an assignment unit from a closed set, and a subject-fact key matching its
	// pattern; on the unassigned branch a reason from a closed set. `{"assigned":true}`
	// alone is type-correct and is NOT a verdict -- exempting its members marked a
	// supplied `assigned` as generated.
	//
	// Mirrored from `parseExperimentVerdict`, and the drift guard for the SHAPE is
	// TestTheMirroredWireShapeMatchesTheSDKs; these gates are prose in that function
	// and have no such derivation, which is stated here rather than implied.
	// ⚠ EQUAL TO THE REQUEST'S VALUE, NOT MERELY A STRING. `expEchoMatches` tolerates
	// an ABSENT member and requires a PRESENT one to be a non-null string EQUAL to
	// what this request carried in that slot -- so a mismatched echo, or an explicit
	// `null` which unmarshals fine into a bare string, passed here while the SDK
	// rejects the body.
	echoed := func(raw json.RawMessage, want string) bool {
		if raw == nil {
			return true
		}
		var v *string
		if json.Unmarshal(raw, &v) != nil || v == nil {
			return false
		}
		// An unrecorded request value cannot confirm an echo.
		return want != "" && *v == want
	}
	if !echoed(wire.AppKey, requestedAppKey) ||
		!echoed(wire.EnvironmentKey, requestedEnvKey) ||
		!echoed(wire.ExperimentKey, requestedExpKey) {
		return false
	}
	var version *int64
	if wire.Version != nil {
		if json.Unmarshal(wire.Version, &version) != nil || version == nil {
			return false
		}
	}
	if *wire.Assigned {
		if version == nil || *version < 1 {
			return false
		}
		if strings.TrimSpace(wire.AssignmentKey) == "" || strings.TrimSpace(wire.VariantKey) == "" {
			return false
		}
		unit, _ := wire.Boundary["assignment_unit"].(string)
		switch unit {
		case "synthetic_subject_key", "client_id":
		default:
			return false
		}
		if sfk := strings.TrimSpace(wire.SubjectFactKey); sfk != "" &&
			!regexp.MustCompile(`^sfk1_[0-9a-f]{64}$`).MatchString(sfk) {
			return false
		}
		return true
	}
	reason := ""
	if wire.Reason != nil {
		var decoded *string
		if json.Unmarshal(wire.Reason, &decoded) != nil || decoded == nil {
			return false
		}
		reason = *decoded
	}
	switch reason {
	case "", "kill_switch", "targeting_unmatched", "age_ineligible":
	default:
		return false
	}
	return version == nil || *version >= 1
}

// topLevelExemptions picks the registry a body of this status is described by.
//
// ⚠ THE SDK'S OWN PRECONDITIONS COME FIRST. Both SDK paths refuse a body before
// its schema means anything: `parseExperimentVerdict` and `experimentBodyErrorText`
// each return early when the body is incomplete or longer than `expMaxBodyBytes`.
// A status-200 body one byte over that limit is therefore never parsed as an
// assignment -- and exempting its member names marked an endpoint-controlled name
// as generated, so a supplied `assigned` was published with an empty refusal
// ledger.
//
// Size is the whole test, because this recorder's ceiling sits exactly one byte
// ABOVE the SDK's: a capture at the ceiling is indeterminate -- it may be whole or
// truncated -- and already fails this comparison. That is what `capturedBodyMax`
// is written for, and why incompleteness needs no separate signal here.
//
// An unreadable status grants no schema exemptions. The SDK classifies
// responses by status, so neither assignment nor error names are fixed
// when that classification cannot be established.
// Use the captured body length for the SDK's size gate, since escapeMarks
// can expand the text before this pass reads it.
func topLevelExemptions(statusLine string, bodyLen int, shapeOK, envelopeOK bool) map[string]bool {
	none := map[string]bool{}
	// The SDK requires a complete body read before applying its schema.
	// A syntactically complete JSON prefix with a read error does not qualify
	// its member names as SDK-defined grammar.
	if capturedIncomplete || bodyLen > sdkMaxBodyBytes {
		return none
	}
	f := strings.Fields(strings.TrimSuffix(statusLine, "\r"))
	if len(f) < 2 || !strings.HasPrefix(f[0], "HTTP/") {
		return none
	}
	n, err := strconv.Atoi(f[1])
	if err != nil {
		return none
	}
	switch {
	case n == 200:
		if !shapeOK {
			return none
		}
		return assignmentTopLevel
	// ⚠ AND THE ERROR ENVELOPE IS READ AT 400 AND 403, NOT AT EVERY 4xx.
	// `applyExperimentAssignment` calls `experimentBodyErrorText` only for those
	// two -- the subject-grammar sentinel and the real-subjects sentinel -- and
	// classifies every other status by the status alone. Exempting `error` at a
	// 404 marked an endpoint-selected member name as SDK grammar and published a
	// supplied identifier. Fifth axis this
	// registry has been wrong about: depth, membership, shape, status, and now
	// WHICH statuses.
	case n == 400 || n == 403:
		// ⚠ AND THE ERROR ENVELOPE IS TYPED TOO. `experimentBodyErrorText` unmarshals
		// `error` into a STRING, so `{"error":false}` is valid JSON that the SDK does
		// not recognise as an envelope -- and exempting its member marked a supplied
		// `error` as generated. The assignment
		// branch had already learned this; the error branch was left asking only the
		// status, which is the same defect one case along.
		if !envelopeOK {
			return none
		}
		return errorTopLevel
	}
	return none
}

var mintedNames = map[string]bool{
	"subject_fact_key": true,
	"subject_key_hash": true,
	// ⚠ AND THE ASSIGNMENT KEY. A normal assigned response carries both, and this
	// set named one -- so once `subject_fact_key` was redacted the response became
	// publishable and carried the other out with it. A change that turns a refusal
	// into a capture must cover EVERYTHING that refusal stood in front of.
	"assignment_key": true,
}

// ⚠ THE MARKER IS SPACE-FREE. `<query withheld>` carries a space, and after the
// provenance marks are stripped that space becomes an extra component of the
// request line -- so every published request block was rejected by a strict
// parser while the report called it a canonical HTTP/1.1 representation. A request target is one token.
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
	// representation. A bare URL has no
	// space and keeps the old behaviour.
	tail := ""
	if j := strings.IndexByte(line[cut:], ' '); j >= 0 {
		tail = line[cut+j:]
	}
	// ⚠ THE SEPARATOR IS SYNTAX AND MUST SURVIVE. Concatenating the marker onto
	// the path published `/api/…/assignmentquery-withheld` -- a route the SDK never
	// requested, on EVERY successful report, since every assignment request carries
	// a query. The primary evidence of this
	// artifact is which route was called, and the redaction was rewriting it.
	return line[:cut] + string(line[cut]) + marked("query-withheld") + tail
}

// noteMinted records a server-minted field's presence and returns the body
// unchanged -- the caller does not publish a body this reports on.
// jsonString decodes a JSON string literal, so a member is matched by what it
// DENOTES rather than by one spelling of it.
// ⚠ MEMBER NAMES ARE DECODED, NOT MATCHED LITERALLY. `"subject_\u0066act_key"`
// is the same field to `encoding/json` and to the endpoint, and a substring
// check on the raw spelling did not see it -- so the capture was PUBLISHED with
// the minted key intact instead of refused, and the leak guard cannot help
// because a server-minted value is not in suppliedValues. This is the same defect the redaction
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
// Share this minted-name predicate between the guard and redactor so
// both apply the same matching rules.
func isMinted(raw string) bool {
	name, ok := jsonString(raw)
	if !ok {
		return false
	}
	return isMintedName(name)
}

// jsonDepthAt reports the object nesting depth of the byte at index i, counting
// braces outside strings. Depth 1 is a member of the top-level object.
//
// ⚠ NAME MEMBERSHIP IS NOT DEPTH. A top-level minted string beside a nested
// member of the SAME name made every nested occurrence look top-level, and the
// whole response was withheld -- while the comment two lines above the check said
// such payload is ordinary. The code and its own comment disagreed.
// newDepthWalker answers "what JSON depth is byte i at" for a SEQUENCE of
// ascending positions, in ONE forward pass over the body.
//
// ⚠ `jsonDepthAt` RESCANNED FROM BYTE ZERO PER MATCH. A valid assignment whose
// `variant_payload` carries many non-string members puts every one of them
// outside `covered`, so each asked its depth and each answer walked the whole
// prefix again -- tens of billions of byte inspections on a response near the
// capture limit, AFTER the bounded HTTP operation had finished. Endpoint-
// controlled input that hangs the capture is not a slow path; it is the gate
// failing open by never arriving.
//
// The caller consumes matches in order. A cursor avoids rescanning for
// each match; assert that ordering so the cursor cannot answer a depth
// question about an earlier byte.
func newDepthWalker(body string) func(int) int {
	k, depth, inStr, esc := 0, 0, false, false
	last := -1
	return func(i int) int {
		if i < last {
			panic("newDepthWalker called out of order: the cursor's premise does not hold")
		}
		last = i
		for ; k < i && k < len(body); k++ {
			c := body[k]
			switch {
			case esc:
				esc = false
			case inStr && c == '\\':
				esc = true
			case c == '"':
				inStr = !inStr
			case inStr:
			case c == '{':
				depth++
			case c == '}':
				depth--
			}
		}
		return depth
	}
}

// jsonParses reports whether the body is exactly one JSON value surrounded
// only by JSON whitespace. A nil topLevelMembers result also covers parsed
// non-object values, so it cannot answer this question.
func jsonWhitespaceOnly(s string) bool {
	return strings.Trim(s, " \t\r\n") == ""
}

func jsonParses(body string) bool {
	d := json.NewDecoder(strings.NewReader(body))
	var v json.RawMessage
	if err := d.Decode(&v); err != nil {
		return false
	}
	// Skip only JSON whitespace. Unicode TrimSpace also removes characters
	// such as U+00A0 that encoding/json rejects as trailing data.
	return jsonWhitespaceOnly(body[int(d.InputOffset()):])
}

// topLevelMembers returns the names bound from the top-level object, which is
// the only depth at which `encoding/json` binds the SDK's fields.
// ⚠ TOP-LEVEL ONLY. `encoding/json` binds the SDK's field from the top-level
// object; a member of the same name inside `variant_payload` is ordinary payload
// the endpoint chose to call that, and refusing on it made a perfectly
// publishable assignment unpublishable. The
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

// structuralSurfaces is the ledger of shapes the structural rules cannot
// account for. Both the guard and redactor share it.
// Keep requested identifiers per slot: suppliedValues answers whether a
// string was supplied, while expEchoMatches needs the value requested
// for a particular field.
var requestedAppKey, requestedEnvKey, requestedExpKey string

// capturedIncomplete is true while the body of the exchange being rendered is one
// the recorder could not show whole. Both vouching decisions read it: a body the
// SDK would refuse before decoding carries no schema this program may vouch for.
var capturedIncomplete bool

var structuralSurfaces []string

// accountedSurfaces records successful structural rewrites separately
// from the refusal ledger. An ordinary redaction must not itself make
// structuralSurfaces nonempty and force exit 4.
//
// The two answer different questions. "What did this program rewrite" is
// accounting. "What defeated it" is a refusal. One name for both turns the first
// into the second, and no test saw it because the publish/refuse decision had no
// scene -- see TestAnOrdinaryFactResponseStaysPublishable, which exists now.
var accountedSurfaces []string

// captureForm names one of the four shapes the claim enumerates. It is a
// PARAMETER of every ledger call rather than a word in the reason text, so the
// test that holds the claim against the code asks a constructive question -- which
// form did this site declare -- instead of guessing from prose. The first version
// of that test matched keywords and reported three false rejections on correct
// code (`a redirect target` reads as no form at all), which is what a lexical
// criterion does when the two sides are both English.
type captureForm string

const (
	formBody       captureForm = "a response BODY"
	formField      captureForm = "a response FIELD"
	formRequest    captureForm = "the REQUEST"
	formDiagnostic captureForm = "a transport DIAGNOSTIC"
)

// captureForms is the enumeration the claim states, and the test reads it from
// here rather than from the prose.
var captureForms = []captureForm{formBody, formField, formRequest, formDiagnostic}

func noteAccounted(form captureForm, what string) {
	_ = form
	if !slices.Contains(accountedSurfaces, what) {
		accountedSurfaces = append(accountedSurfaces, what)
	}
}

// refusalLedger is what main consults to decide whether the capture may be
// printed. It exists so a test can ask that question without running main.
func refusalLedger() []string { return structuralSurfaces }

// exchangeRefusals records what one rendered exchange added to the refusal ledger
// and whether that exchange was incomplete.
type exchangeRefusals struct {
	truncated bool
	// jsonTruncated is whether the received prefix would have PARSED had it
	// arrived whole. Being incomplete is not by itself a reason a body failed to
	// parse: a `text/plain` payload that was also cut short is an unsupported
	// shape either way.
	jsonTruncated bool
	added         []string
}

// truncationCausedTheFailure answers whether incompleteness ITSELF prevented an
// otherwise supported JSON document from parsing.
//
// ⚠ ASKED OF THE DECODER, BY SENTINEL. `Decode` returns `io.ErrUnexpectedEOF` for
// a JSON prefix and a syntax error for something that is not JSON at all, so the
// two are distinguishable without reading an error's text -- which is what this
// file requires of every other error it classifies.
// jsonTailIsStructuralOnly reports whether bytes the decoder did not consume are
// nothing but JSON whitespace and structure, so a cut there left no partial value
// behind.
//
// ⚠ THE ALPHABET, NOT A PARSE. This is not a second JSON reader: it asks whether a
// tail could contain the beginning of a VALUE, and every value in this grammar
// starts with a byte outside this set. Over-refusing here costs a capture; the
// direction it refuses in is the one that publishes.
func jsonTailIsStructuralOnly(tail string) bool {
	for i := 0; i < len(tail); i++ {
		switch tail[i] {
		case ' ', '\t', '\r', '\n', ',', ':', '{', '}', '[', ']':
		default:
			return false
		}
	}
	return true
}

func truncationCausedTheFailure(body []byte) bool {
	d := json.NewDecoder(bytes.NewReader(body))
	var v json.RawMessage
	err := d.Decode(&v)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		// ⚠ AND A PREFIX MAY BE CUT *INSIDE* A VALUE. `{"assigned":false,
		// "variant_payload":{"token":"server-secret-tok` is a JSON prefix by this
		// sentinel, and neither body redactor can traverse a malformed document -- so
		// both returned the token unchanged, this excuse removed every refusal, and the
		// exit-3 report published it. The
		// supplied-value scrub cannot see it either: the token is the endpoint's.
		//
		// The decoder is asked HOW FAR IT GOT, and the excuse holds only when what it
		// did not consume is structure -- whitespace and punctuation. A tail carrying
		// the start of a value is a value extent nothing measured.
		w := json.NewDecoder(bytes.NewReader(body))
		off := int64(0)
		for {
			if _, terr := w.Token(); terr != nil {
				break
			}
			off = w.InputOffset()
		}
		return jsonTailIsStructuralOnly(string(body[off:]))
	}
	if err != nil {
		return false
	}
	// ⚠ A SUCCESSFUL FIRST DECODE PROVES ONE VALUE, NOT THE WHOLE PREFIX. `{}` in
	// `{}server-secret-token` decodes cleanly and leaves endpoint text behind, so
	// the excuse removed a refusal the trailing bytes had earned. Trailing data violates the
	// single-document shape whether or not anything was truncated -- which is the
	// same sentence `markBareJSONLiterals` already applies to a value STREAM, one
	// pass along.
	// ⚠ `Buffered()` IS THE READ-AHEAD, NOT THE REMAINDER. It exposes only what the
	// decoder happened to pull past the value, so `{}` followed by 510 spaces and
	// then endpoint text read as "nothing but whitespace left". `InputOffset` says where the value
	// ended, and the input is right here -- the remainder is the rest of the slice.
	//
	// ⚠ AND JSON'S WHITESPACE IS FOUR BYTES. `strings.TrimSpace` also eats U+00A0
	// and the rest of Unicode's space class, which `encoding/json` rejects -- so a
	// body followed by a non-breaking space read as one document when the decoder
	// says it is not.
	return strings.Trim(string(body[d.InputOffset():]), " \t\r\n") == ""
}

// unexcusedRefusals returns the ledger entries that no TRUNCATED attempt accounts
// for.
//
// ⚠ IT IS A FUNCTION SO A FIXTURE CAN REACH IT. The attribution lived inline in
// `main()`, where nothing can run it -- and the mutant that suppressed the whole
// ledger passed, because the only scene there read the SOURCE. The rule this
// encodes: "this capture is incomplete" excuses the shapes THAT attempt produced
// and nothing else, so a later truncated retry cannot excuse a complete earlier
// attempt's refusal.
// truncationExcusable names the reasons an INCOMPLETE body itself produces.
//
// ⚠ NOT EVERY REASON A TRUNCATED ATTEMPT RAISES. The first version excused all of
// them, so a truncated response declaring `Content-Encoding: br` had its
// undecodable-coding refusal removed and the opaque partial bytes published --
// a reader with a Brotli decoder recovers whatever arrived before the truncation. The excuse is "this body did not arrive
// whole", which explains a body that does not parse and explains nothing about
// the coding it was sent in.
var truncationExcusable = map[string]bool{
	"a response body in a shape this build cannot describe":                          true,
	"a member of a body that does not parse, in a shape this program has not judged": true,
}

func unexcusedRefusals(ledger []string, per []exchangeRefusals) []string {
	truncated := map[int]bool{}
	for i, e := range per {
		// ⚠ AND THE TRUNCATION MUST BE THE REASON. Excusing on incompleteness alone
		// printed a partial `text/plain` body carrying endpoint text: the reason was
		// raised because the shape is unsupported, not because bytes were missing,
		// and the guard cannot see a value this harness never supplied.
		if e.truncated && e.jsonTruncated {
			truncated[i] = true
		}
	}
	out := []string{}
	for _, w := range ledger {
		if !truncationExcusable[w] {
			out = append(out, w)
			continue
		}
		// Excused only if EVERY attempt that raised it was incomplete. One complete
		// attempt raising the same reason is enough to keep it.
		raisers := structuralAt[w]
		allTruncated := len(raisers) > 0
		for _, r := range raisers {
			if !truncated[r] {
				allTruncated = false
				break
			}
		}
		if !allTruncated {
			out = append(out, w)
		}
	}
	return out
}

// currentExchange identifies the attempt being rendered. Include it in the
// de-duplication key so a reason from a truncated attempt cannot suppress
// the same reason from a later complete response.
var currentExchange = -1

// structuralAt records which attempts raised each reason.
var structuralAt = map[string][]int{}

func noteStructural(form captureForm, what string) {
	_ = form
	if !slices.Contains(structuralSurfaces, what) {
		structuralSurfaces = append(structuralSurfaces, what)
	}
	if !slices.Contains(structuralAt[what], currentExchange) {
		structuralAt[what] = append(structuralAt[what], currentExchange)
	}
}

// isBenignName is benignTopLevel under the SAME folding `encoding/json` applies,
// which is what `isMintedName` already used. `{"aſſigned":true}` populates the
// SDK's field and must not be reported as unfamiliar.
func isBenignName(name string) bool {
	for n := range benignTopLevel {
		if strings.EqualFold(name, n) {
			return true
		}
	}
	return false
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

func noteMinted(body string) string {
	// ⚠ A BODY THAT WILL NOT PARSE IS NOT AN ANSWER ABOUT DEPTH. `topLevelMembers`
	// returns nothing for `{"assigned":true,"subject_fact_key":"…` with no closing
	// brace, and "no top-level names" was then read as "there is no minted field
	// here" -- so a malformed verdict body was PUBLISHED with the identifier
	// intact, and the leak guard cannot help because a server-minted value is not
	// in suppliedValues. The SDK calls such a
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
	// for, and the value went out. The brace
	// was a guess about how a malformed body looks, standing in front of a rule
	// about what it CONTAINS; the scan below already answers that itself, and on a
	// body with no minted member it finds nothing and costs nothing.
	if !jsonParses(body) {
		// ⚠ AND IN EVERY SUPPORTED SPELLING OF IT. A malformed body that
		// percent-encodes the member -- `%22subject_fact_key%22:%22sfk_secret%22` --
		// left this ledger empty while the guard's own decoder reconstructs it, so the
		// report published text the guard would refuse if it could see it. Same rule as the transport diagnostic
		// one file-section along, and the same list answers both.
		forms, whole := decodedForms(body)
		for _, form := range forms {
			for _, m := range jsonMemberName.FindAllStringSubmatch(form, -1) {
				if isMinted(m[1]) {
					noteStructural(formBody, "a server-minted subject identifier in a body that does not parse")
				}
			}
		}
		if !whole {
			noteStructural(formBody, "an unparsable body whose decoded forms could not be enumerated")
		}
	}
	// ⚠ AND ONLY WHERE THE MEMBER HAS VALUE BYTES. An explicitly empty
	// `"subject_fact_key":""` is accepted by the SDK and conceals nothing, yet a
	// name-only check refused the capture and forced exit 4 -- the same defect as the
	// empty `Location:` header one surface along, and the same repair. The refusal is about what a VALUE could
	// hide.
	var top map[string]json.RawMessage
	decoded := json.Unmarshal([]byte(body), &top) == nil
	for _, name := range topLevelMembers(body) {
		if !isMintedName(name) {
			continue
		}
		if decoded {
			var v *string
			if raw, ok := top[name]; ok && json.Unmarshal(raw, &v) == nil && v != nil && *v == "" {
				continue
			}
		}
		// ⚠ A CONSTANT, NOT THE NAME. With a Unicode-folded spelling the endpoint
		// chose, `isMintedName` accepts it and the label would carry that spelling to
		// stderr -- refusing to print the response while printing the identifier. Every message a guard prints is an
		// output channel.
		noteStructural(formBody, "a server-minted subject identifier")
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

// incompleteBodyLine renders a body-read failure through the same captured-input
// sanitizer as transportErrorLine. Both error paths can contain endpoint text.
// The helper lets tests exercise the report's call site directly.
func incompleteBodyLine(ex *exchange) string {
	return transportErrorLine(ex.truncErr())
}

// transportErrorLine sanitizes a transport failure before report rendering.
func transportErrorLine(err error) string {
	return sanitizeCaptured(err)
}

// verdictValue escapes marker bytes before scrubbing supplied text and stripping
// provenance. Stripping first would remove literal marker bytes from the value.
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

// The entries come from the IANA Media Types registry's common
// application/*, text/*, image/*, audio/*, video/* and font/* registrations.
// Regenerate from https://www.iana.org/assignments/media-types/media-types.xhtml.
//
// This is a bounded registry snapshot. An unlisted media type is replaced
// by its length, reducing readability without admitting its raw value.
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

// Mark a recognized media type as grammar so supplied-value scrubbing
// does not replace part of application/json with a prose placeholder.
// The guard and redactor share this registry.
//
// The PARAMETERS are not vouched for: `boundary=` and `charset=` values are the
// endpoint's, and only the type/subtype is fixed by the registry.
func markMediaType(line string) string {
	// Split on the colon even when the field name already contains provenance
	// marks. A field name cannot itself contain a colon.
	i := strings.IndexByte(line, ':')
	if i <= 0 || !strings.EqualFold(stripMarks(ows(line[:i])), "content-type") {
		return line
	}
	rest := line[i+1:]
	cr := ""
	if strings.HasSuffix(rest, "\r") {
		cr, rest = "\r", strings.TrimSuffix(rest, "\r")
	}
	mt, params, _ := strings.Cut(rest, ";")
	lead := mt[:len(mt)-len(strings.TrimLeft(mt, " \t"))]
	bare := ows(strings.TrimSuffix(mt, "\r"))
	// ⚠ THE REGISTRY LOOKUP FOLDS CASE; VOUCHING MUST NOT. `application/JSON` is
	// the same media type to the registry and a different string on the wire, and
	// marking the raw span vouched for a spelling the registry never saw -- so a
	// supplied `JSON` was skipped by both the scrub and the guard. Recognition is about what a value
	// DENOTES; vouching is about the bytes.
	// ⚠ NOT IF THE CRITERION HAS ALREADY VOUCHED. In the half that carries
	// `verbatimHeaders`, an admitted media type is marked before this runs, and
	// marking it again produced NESTED marks -- which read as captured text, so the
	// value was scrubbed after all. The guard half has no such criterion, which is
	// why this function still exists; here it must stand down.
	if strings.Contains(bare, genMark) {
		return line
	}
	plainBare := stripMarks(bare)
	if !registeredMediaTypes[strings.ToLower(plainBare)] || plainBare != strings.ToLower(plainBare) {
		return line
	}
	out := line[:i+1] + lead + marked(bare)
	if params != "" {
		out += ";" + params
	}
	return out + cr
}

// scrubStructuralName applies the token-safe name scrub to a line the structural
// redactors produced, so a supplied value equal to a field NAME cannot reach the
// generic prose placeholder and make the field unparsable.
// ⚠ IT ADMITS THROUGH THE REGISTRY, LIKE EVERY OTHER NAME POSITION. `scrubHeaderName`
// knows only values the HARNESS supplied, so with a legal experiment key of
// `Location` an ordinary redirect was published as `redacted-8-chars: /…`: the
// capture lost which standard field arrived, and the generated token passed the
// guard. The trailer path already used
// `admitFieldName`; the three structural paths here did not, which is the same
// shape as every other "one site was shown and the others were not" in this file.
func scrubStructuralName(line string) string {
	if i := strings.IndexByte(line, ':'); i > 0 {
		// ⚠ AND A SUPPLIED VALUE EQUAL TO A PROTECTED NAME, WHERE THERE ARE NO VALUE
		// BYTES. Vouching is right about the BYTES -- they are this program's
		// canonical spelling -- and wrong on an EMPTY protected field, where the whole
		// line is this program's own text: a supplied `Location` against a legal
		// `Location:` was published through it, a generated span being skipped by both
		// the scrub and the guard. The half that
		// found this asked inside the gate that WITHHELD such fields; this half renders
		// them, so the gate never reaches them, and every structural name passes here.
		//
		// This condition applies only to an empty field. With value bytes,
		// redaction preserves the field name so the HTTP message remains parseable.
		if strings.Trim(line[i+1:], " \t\r") == "" {
			name := ows(line[:i])
			if _, protected := serverMintedFields[strings.ToLower(name)]; protected {
				for _, sv := range suppliedValues {
					if sv != "" && strings.EqualFold(sv, name) {
						noteStructural(formField, "a supplied value equal to a protected field name")
						return line
					}
				}
			}
		}
		return admitFieldName(line[:i]) + line[i:]
	}
	return line
}

// markFieldColon marks the delimiter between a field name and its value.
//
// ⚠ APPLIED LAST, like the media type and the target delimiters: the passes before
// it re-split the line, and a mark inserted earlier does not survive them.
// emitField is the ONE exit every header line takes, so the finishing passes
// cannot be forgotten on a branch that returns early.
//
// Mark the field delimiter structurally for every header path, including
// redirect targets, cookies and announced trailers. Otherwise a supplied
// colon can replace the message's punctuation with a prose placeholder.
func emitField(line string) string { return markFieldColon(line) }

func markFieldColon(line string) string {
	i := strings.IndexByte(line, ':')
	if i <= 0 {
		return line
	}
	// ⚠ THE QUESTION IS WHETHER THIS BYTE IS INSIDE A MARK, NOT WHAT FOLLOWS IT.
	// The idempotence guard tested `line[i+1]`, so on a line whose field NAME is
	// vouched -- `\x01Trailer\x01:` -- the byte after the colon opens the NEXT
	// marked span and the guard read that as "already marked", leaving the
	// delimiter captured on exactly the paths that admit their names. Adjacency is not containment; parity is.
	inMark := false
	for k := 0; k < i; k++ {
		if string(line[k]) == genMark {
			inMark = !inMark
		}
	}
	if inMark {
		return line
	}
	return line[:i] + marked(":") + line[i+1:]
}

// statusLineOf returns the dump's first line, or "" when there is none.
func statusLineOf(out []string) string {
	if len(out) > 0 {
		return out[0]
	}
	return ""
}

// allIdentityCodings reports whether every coding in a `Content-Encoding` value is
// `identity` -- the only one this build treats as "nothing was applied". An empty
// element is not a coding and makes the list malformed rather than transparent.
// ⚠ TWO COMPARISONS, DELIBERATELY. The REFUSAL folds case -- `IDENTITY` is still
// "nothing was applied", so refusing it would withhold a readable capture. The VOUCH
// does not: it marks the received bytes as this program's grammar, and that is only
// true of the canonical spelling. A scene pins both, and collapsing them into one
// fold vouched `IDENTITY` immediately.
func allIdentityCodings(v string, fold bool) bool {
	if ows(v) == "" {
		return false
	}
	for _, part := range strings.Split(v, ",") {
		p := ows(part)
		if fold {
			if !strings.EqualFold(p, "identity") {
				return false
			}
			continue
		}
		if p != "identity" {
			return false
		}
	}
	return true
}

func dropFraming(dump string) string {
	lines := strings.Split(dump, "\n")
	// ⚠ `Cache-Control: no-cache` MAY BE THE PARSER'S, NOT THE ENDPOINT'S.
	// `http.ReadResponse` ADDS it when the response carries `Pragma: no-cache` and no
	// cache directive of its own -- measured. So with both present its provenance
	// cannot be established: the endpoint may have sent it, or net/http may have
	// written it. Treated as captured, a supplied `Cache-Control` made the generic
	// scrub rewrite the field NAME to `redacted-13-chars`, and the generated
	// placeholder made the guard approve a canonical response carrying a header that
	// never arrived.
	//
	// Refuse ambiguous provenance: treating received text as generated skips
	// the guard, while treating generated text as received permits corruption.
	// Inspect only the header block, since body text can resemble a header.
	// This guard distinguishes serializer-written fields from received fields;
	// registered-name grammar is a separate question.
	//
	// A broader change to name marking would also need to preserve the
	// registered field-name grammar and the existing rendering behavior.
	//
	// Mirror fixPragmaCacheControl's exact condition: only the first parsed
	// Pragma value equal to lowercase no-cache can trigger synthesis, and
	// only when the Cache-Control map key is entirely absent. Another
	// Cache-Control field therefore establishes that synthesis did not run.
	pragmaNoCache, cacheNoCache, sawPragma := false, false, false
	cacheCount := 0
	for _, l := range lines {
		if strings.TrimSuffix(l, "\r") == "" {
			break
		}
		low := strings.ToLower(strings.TrimSuffix(l, "\r"))
		if !sawPragma && strings.HasPrefix(low, "pragma:") {
			sawPragma = true
			// Compare the complete first Pragma value with no-cache, as
			// fixPragmaCacheControl does. Splitting it at a comma would broaden
			// the parser's condition and misclassify a received Cache-Control.
			first := strings.TrimSuffix(l, "\r")[len("Pragma:"):]
			if strings.Trim(first, " \t") == "no-cache" {
				pragmaNoCache = true
			}
		}
		if strings.HasPrefix(low, "cache-control:") {
			cacheCount++
			if ows(strings.TrimSuffix(strings.TrimPrefix(low, "cache-control:"), "\r")) == "no-cache" {
				cacheNoCache = true
			}
		}
	}
	if pragmaNoCache && cacheNoCache && cacheCount == 1 {
		noteStructural(formField, "a Cache-Control field whose provenance this build cannot establish")
	}
	out := make([]string, 0, len(lines))
	inHeaders := true
	bodyStart := -1
	// Whether there are bytes after the header separator at all. Computed here
	// because the header rules run before the body is reached, and one of them
	// asks about it: an undecodable coding matters only if something is encoded.
	hasBody := false
	for _, sep := range []string{"\r\n\r\n", "\n\n"} {
		if k := strings.Index(dump, sep); k >= 0 {
			// Check byte length, not trimmed content. Whitespace under an unsupported
			// coding is still opaque encoded input. A body with zero bytes needs
			// no decoding.
			hasBody = len(dump[k+len(sep):]) > 0
			break
		}
	}
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
		// guard then approved because the placeholder carries generated marks. Text this program wrote is not
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
		// guard then approved because the placeholder is generated. Same treatment as the framing
		// headers directly below.
		// ⚠ ONLY THE SYNTHESISED ONE. HTTP/2 forbids `Connection`, so its presence
		// in an HTTP/2 dump is proof the serialiser wrote it -- but on HTTP/1 the
		// transport keeps what the endpoint sent, and marking that line generated
		// let a value base64-decoding to the key through both the scrub and the
		// guard. The exemption was written
		// for one protocol and applied to both.
		// ⚠ THE PROTOCOL IS NOT THE QUESTION; whether it was RECEIVED is. See
		// receivedConnection.
		if strings.HasPrefix(low, "connection:") && !receivedConnection {
			out = append(out, emitField(marked(strings.TrimSuffix(l, "\r"))+cr))
			continue
		}
		if strings.HasPrefix(low, "content-length:") {
			out = append(out, emitField(marked("X-Capture-Note: Content-Length removed — the body below is redacted")+cr))
			continue
		}
		// ⚠ A REDIRECT TARGET IS A CREDENTIAL SURFACE. `Location` query values are
		// server-generated -- `state`, signed tokens, one-time callbacks -- so no
		// list of values THIS program supplied can reach them, exactly as with
		// Set-Cookie. Redacted structurally,
		// by the same function the request line uses: names kept, values lengthed.
		// ⚠ A CODING THE TRANSPORT DID NOT UNDO LEAVES THE BODY OPAQUE. Go
		// decompresses gzip it requested itself; anything still declared here --
		// `deflate`, `br`, `zstd` -- means the bytes below are compressed, and
		// neither the scrub nor the guard's decoders can see a supplied value
		// inside them, while a reader need only apply the declared coding. This half cannot decode it, so it
		// does not publish it.
		if strings.HasPrefix(low, "content-encoding:") {
			// ⚠ AND ONLY IF THERE IS A BODY TO DECODE. A 204, or any zero-length
			// response, may still declare a coding -- and refusing there withholds a
			// diagnostic over bytes that do not exist. The refusal is about what an undecodable body could HIDE, so an
			// absent body is not its subject.
			// ⚠ A CONTENT CODING IS A LIST. `Content-Encoding: identity, identity` is the
			// same response as two separate `identity` lines, which this loop already
			// accepts -- so the classification depended only on how an intermediary chose
			// to combine the fields, and the combined spelling withheld a readable capture
			// with exit 4. A whole field value
			// compared against a single token is a list read as a scalar.
			if v := ows(strings.TrimSuffix(l[len("content-encoding:"):], "\r")); v != "" &&
				!allIdentityCodings(v, true) && hasBody {
				// ⚠ A CLASSIFICATION, NOT THE VALUE. With a supplied key of
				// `deflate` the scrub hid the header and this diagnostic printed
				// it verbatim to stderr -- the refusal publishing what the refusal
				// was for.
				noteStructural(formField, "a body in a content coding this build cannot decode")
			} else if allIdentityCodings(v, false) {
				// ⚠ THE CANONICAL SPELLING, AND THE GENERIC NAME PATH. Two things this
				// early return skipped: `EqualFold` admitted `IDENTITY` and the branch
				// vouched the RECEIVED spelling, and returning here bypassed the header
				// name handling, so a supplied `Content-Encoding` was scrubbed out of the
				// field NAME. An early return is a
				// promise to have done everything the common path does.
				// ⚠ THE CANONICAL SPELLING, EXACTLY. `EqualFold` here admitted
				// `IDENTITY` and this branch then vouched the RECEIVED spelling, so a
				// supplied `IDENTITY` was published. A non-canonical spelling is not refused: it falls through to
				// the generic path and is redacted like any other value.
				//
				// Condition this branch without ending the line loop. Admit identity as
				// the no-op content coding using its canonical spelling, and apply the
				// common field-name admission path before emitting the line.
				out = append(out, emitField(admitFieldName(l[:len("content-encoding:")-1])+":"+
					strings.Replace(l[len("content-encoding:"):], v, marked(v), 1)))
				continue
			}
		}
		if strings.HasPrefix(low, "location:") {
			// ⚠ AND THE FRAGMENT, NOT ONLY THE QUERY. An OAuth-style redirect
			// carries its credential after `#` -- `#access_token=…` never reaches
			// the server and is exactly the value a capture must not publish, and
			// `redactQuery` saw only `?`.
			// ⚠ THE NAME TOO, THROUGH THE TOKEN-SAFE SCRUB. This branch handed the
			// line straight on, so a supplied value equal to the field NAME fell
			// to the generic scrub and became `<redacted, 8 chars>` -- spaces and
			// angle brackets inside a field name, an unparsable response. The
			// trailer path already did this.
			out = append(out, emitField(vouchTargetSyntax(vouchScheme(scrubStructuralName(redactTarget(strings.TrimSuffix(l, "\r")))))+cr))
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
		// unparsable.
		// A COOKIE THE SERVER SET IS A CREDENTIAL THIS PROGRAM NEVER SUPPLIED,
		// so no list of supplied values can reach it. Session, affinity and
		// bot-management cookies were published verbatim in an artifact whose
		// whole purpose is to be published.
		// Redacted STRUCTURALLY, like the query string: the cookie's name and
		// its attributes stay, the value becomes a length.
		if strings.HasPrefix(low, "set-cookie:") {
			out = append(out, emitField(scrubStructuralName(redactSetCookie(l))))
			continue
		}
		// Check minted-field membership after the branches that provide structural
		// renderings for Set-Cookie and Location. Those renderings preserve useful
		// syntax instead of withholding the entire field. Response headers and
		// trailers share serverMintedFields so they classify the same population.
		if name, ok := fieldNameOf(low); ok {
			if note, minted, whole := mintedFieldIn(name); !whole && !minted {
				noteStructural(formField, "a response field name whose decoded forms could not be enumerated")
			} else if minted {
				// ⚠ AND ONLY WHERE THERE ARE VALUE BYTES TO CONCEAL. A legal empty
				// field -- `Location:`, or one whose value is nothing but OWS -- was
				// refused on the strength of its NAME, so a capture with no
				// endpoint-minted bytes in it at all cost an operator the record and
				// exit 4. The refusal is about what
				// the value could hide, exactly as the unsupported-coding check is about
				// what an undecodable body could hide.
				//
				// The name printed is still THIS program's spelling, so the empty field
				// is vouched for rather than echoed.
				if _, v, _ := strings.Cut(strings.TrimSuffix(l, "\r"), ":"); strings.Trim(v, " \t") == "" {
					// ⚠ THE NAME STILL HAS TO SURVIVE THE COLLISION TEST. Marking the whole
					// line generated is right about the BYTES -- they are this program's
					// canonical spelling -- and wrong when a supplied value equals that
					// spelling: `SP_EXPERIMENT_KEY=Location` against a legal empty
					// `Location:` published the identifier, because a generated span is
					// skipped by both the scrub and the guard. Allowing the empty VALUE was
					// correct; it said nothing about the NAME.
					collides := false
					for _, sv := range suppliedValues {
						if sv != "" && strings.EqualFold(sv, canonicalFieldName(name)) {
							collides = true
						}
					}
					if collides {
						noteStructural(formField, "a supplied value equal to a protected field name")
						out = append(out, canonicalFieldName(name)+":"+cr)
						continue
					}
					out = append(out, marked(canonicalFieldName(name)+":")+cr)
					continue
				}
				noteStructural(formField, note)
				out = append(out, emitField(canonicalFieldName(name)+": "+marked("<withheld>")+cr))
				continue
			}
		}
		if strings.HasPrefix(low, "transfer-encoding:") {
			out = append(out, emitField(marked("X-Capture-Note: Transfer-Encoding removed — the body below is decoded")+cr))
			continue
		}
		// ⚠ AND A HEADER NAME CAN CARRY THE IDENTIFIER, so this comes LAST: the
		// specific rules above own their lines, and everything else keeps its
		// value untouched while its NAME is scrubbed. `X-<key>: v` published the
		// identifier in the name, and the guard's short-value rule counts `-` as
		// a word byte so the boundary check waved it through. The trailer path
		// already did this; the response's own header block did not -- fixed
		// where it was found and not where the question is asked, once more.
		// ⚠ `Trailer:` LISTS FIELD NAMES IN ITS VALUE. Scrubbing only the field
		// name `Trailer` left `Trailer: X-Bar` carrying a supplied `Bar`, which
		// ordinary value matching misses because the hyphen is a word byte and
		// the guard's name-aware check extracts only `Trailer` -- while the
		// trailer report itself scrubs the real name, so the two disagreed about
		// the same string.
		if strings.HasPrefix(low, "trailer:") {
			if i, ok := headerNameEnd(l); ok {
				// ⚠ STRIP THE TERMINATOR FIRST. The last element of a CRLF
				// declaration is ` Location\r`, and OWS does not include CR, so
				// the registered name was misclassified and lengthened -- on the
				// last announced name of EVERY canonical trailer declaration.
				names := strings.Split(strings.TrimSuffix(l[i+1:], "\r"), ",")
				for k, n := range names {
					// Trailer announcements also use the field-name registry. Redacting
					// a trailer value later does not account for an endpoint-chosen name
					// in the earlier announcement.
					names[k] = admitFieldName(n)
				}
				// Admit and vouch for the Trailer field name just like every other
				// registered name. Mark the generated comma separator as syntax so a
				// supplied comma cannot alter the announced field list.
				out = append(out, emitField(admitFieldName(l[:i])+":"+strings.Join(names, marked(","))+cr))
				continue
			}
		}
		// ⚠ AND THE VALUE GOES THROUGH THE CRITERION, not only the name. The
		// sentence above said "everything else keeps its value untouched", and
		// that was the hole: `Content-Location`, `Refresh`, `Link`,
		// `WWW-Authenticate`, `Set-Cookie2`, an `ETag`, a `Server` banner, a
		// `Content-Type` boundary -- each carries a string the ORIGIN chose, none
		// is in `suppliedValues`, and the guard behind this cannot see any of
		// them.
		// Unknown fails closed now: see verbatimHeaders for the criterion.
		if i, ok := headerNameEnd(l); ok {
			// Emit each field once. Classify its received name against the IANA
			// registry before supplied-value scrubbing; mark media-type grammar
			// only after that classification. Preserve the field delimiter as
			// message punctuation.
			out = append(out, markFieldColon(markMediaType(admitFieldName(l[:i])+redactUnlessVerbatim(l)[i:])))
			continue
		}
		out = append(out, scrubStructuralName(redactUnlessVerbatim(l)))
	}
	// ⚠ THE WHOLE BODY, NOT ONE LINE AT A TIME. JSON may put a newline between a
	// field's colon and its value, so `"subject_fact_key":\n"sfk1_..."` matched
	// nothing while the pattern that permits the whitespace sat right there --
	// and the value is server-minted, so the guard behind this cannot see it
	// either.
	// The PAYLOAD as this pass sees it: the exemption question is partly about its
	// SIZE, so the registry cannot be chosen from the status line alone.
	//
	// ⚠ AND `bodyStart` NAMES THE SEPARATOR, NOT THE PAYLOAD. It is set on the blank
	// line that ends the header block, and that line is appended too -- so the size
	// this gate measured was one or two bytes more than the SDK reads, and a body at
	// either of the last two accepted sizes lost its exemptions: the scrub then
	// rewrote the fixed `"assigned"` member name and published altered JSON for a
	// body the SDK parsed. A gate that measures
	// the framing measures the wrong thing.
	exemptBody := ""
	if bodyStart >= 0 && bodyStart+1 < len(out) {
		// ⚠ MEASURED ON THE CAPTURED BYTES, NOT THE ESCAPED ONES. `escapeMarks` expands
		// a literal `\x00` spelling before this pass sees it, so a valid body the SDK
		// accepted at 1048575 bytes reached here as about 1.89 MiB and lost every schema
		// exemption -- the fixed `"assigned"` member was then rewritten and the generated
		// marks made the guard approve altered, invalid JSON. The SDK's gate is about the bytes it
		// read; a length taken after this program's own expansion answers a different
		// question.
		exemptBody = strings.Join(out[bodyStart+1:], "\n")
	}
	exemptions := map[string]bool{}
	if len(out) > 0 {
		exemptBodyLen := len(exemptBody)
		if capturedBodyBytes >= 0 {
			exemptBodyLen = capturedBodyBytes
		}
		// The SDK decoded the captured bytes; so does this.
		shapeBody := exemptBody
		if capturedBodyRaw != "" {
			shapeBody = capturedBodyRaw
		}
		exemptions = topLevelExemptions(out[0], exemptBodyLen,
			sdkWouldParseAssignment(shapeBody), sdkWouldReadErrorText(shapeBody))
	}
	if bodyStart < 0 {
		return strings.Join(out, "\n")
	}
	return strings.Join(out[:bodyStart], "\n") + "\n" +
		redactUnaccountedBody(markBareJSONLiterals(redactUnaccountedJSONValues(redactMintedBody(strings.Join(out[bodyStart:], "\n"), exemptions), exemptions, statusLineOf(out)), exemptions))
}

type recorder struct {
	// ⚠ GUARDED, because this transport is shared with the SDK's ingest worker,
	// which flushes on its own timer from another goroutine. Even with the
	// off-route filter above returning early, the counter and the slice are
	// touched concurrently.
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
// ⚠ THE SDK HAS OTHER LEGS, AND THEY SHARE THIS TRANSPORT. Applying a variant
// (`ApplyExperimentVariant`, which this harness never calls; before it existed,
// the fetch itself) has the lane POST the application to the exposure apply
// route and enqueue the sealed `experiment_exposure`, and the ingest worker
// flushes on its own timer through the same `HTTPClient` -- so on a run that
// settles near the tick, an ingest POST was recorded as another supposed
// assignment attempt, could change which exchange `last()` returns, and raced
// the report's own reads of `exchanges`. The configuration comment claiming
// the ingest leg is "NOT exercised" was an intention, not a fact.
//
// Filtering here rather than trusting a config knob makes it a fact: anything
// that is not an assignment passes through unrecorded, and is COUNTED, so the
// claim is checked on every run instead of asserted once in a comment.
const assignmentRoute = "/api/v1/runtime/experiments/assignment"

// observedConversation reports whether this request belongs to the exchange the
// harness exists to observe, as opposed to background traffic it must not emit.
//
// ⚠ THE SUBJECT IS THE CONVERSATION, NOT THE PATH. Stated as a path suffix, this
// absorbed the SDK's REDIRECT FOLLOW-UPS: `http.Client` sends a 302's follow-up
// through this same RoundTripper, `/cb` is not the assignment route, and the
// synthetic 204 below was returned without the target ever being contacted -- so
// the SDK acted on a response no server sent and the report paired its verdict
// with an exchange it did not act on. A
// recorder that answers on the endpoint's behalf is not observing the experiment,
// it is conducting a different one.
//
// `Request.Response` is net/http's OWN answer to "is this a redirect follow-up" --
// the field is populated only during a client redirect follow. Asking the client
// beats reconstructing redirect chains here, for the same reason the pre-push hook
// reads the push's refspec instead of modelling git's ref resolution.
//
// One bit is enough because it can only be set on a response THIS recorder
// forwarded: the absorbed leg returns 204, and no client follows a 204. So a
// non-nil Response means the previous leg was a real redirect from a real
// assignment-conversation request, and its follow-up belongs to that conversation.
func observedConversation(req *http.Request) bool {
	if req.URL == nil {
		return false
	}
	return strings.HasSuffix(req.URL.Path, assignmentRoute) || req.Response != nil
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	if !observedConversation(req) {
		r.mu.Lock()
		r.offRoute++
		r.mu.Unlock()
		// ⚠ ABSORBED, NOT FORWARDED. Filtering it out of the RECORD while still
		// sending it left the side effect this harness must not have: an
		// exposure delivered to the ingest endpoint from a run whose
		// only purpose is to observe one assignment. A capture tool that emits
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
	ex := exchange{redirectLeg: req.Response != nil}
	// EVERY QUERY VALUE THE SDK SENDS JOINS THE SCRUB SET, not only the values
	// this program supplied from the environment.
	//
	// ⚠ THE SUBJECT KEY IS MINTED INSIDE THE SDK and was in neither list, so a
	// redirect Location or a diagnostic body echoing the request URL published
	// `subject_key=spcid_...` verbatim -- past the response scrub AND past the
	// publication gate, both of which read the same incomplete list. The request redactor already treats
	// every query value as identifying; this makes the RESPONSE side agree,
	// which is the property, not a longer list of names.
	for k, vs := range req.URL.Query() {
		// The NAMES this program put on the wire are names it can vouch for; the
		// response echoing one back is structure, not an endpoint's choice.
		noteRequestName(k)
		for _, v := range vs {
			addSuppliedValue(v)
		}
	}
	// ⚠ THE AUTHORITY IS NOT A PARAMETER NAME. It was registered here, so
	// `nameIsOurs` vouched for it wherever a NAME is expected -- and with host and
	// experiment key both `control`, a `Location: /cb?control=x` or a cookie of
	// that name was marked harness-generated, which both the scrub and the guard
	// then skipped: the supplied identifier published. The `Host:` LINE is vouched for by
	// `requestOwnedHeaders`, which is the mechanism written for it. The suite was
	// run with this line removed before removing it, and stayed green: the
	// registration was measured to be unnecessary rather than judged so.
	if dump, err := httputil.DumpRequestOut(req, true); err == nil {
		// ⚠ THE AUTHORITY IS OURS AND IS NOT IN `req.Header`. Go stores it in
		// `Request.Host`/`URL.Host`, so a derivation asking only the header map
		// called every `Host:` line serialiser-written -- and a supplied value
		// equal to the configured host was then skipped by the scrub AND the
		// guard, and printed. Deriving from
		// one of two places the data lives in is not deriving.
		ex.req = redact([]byte(escapeMarks(string(dump))), requestOwnedHeaders(req), ex.redirectLeg)
	} else {
		ex.reqDumpErr = err
	}

	// ⚠ A COPY, AND ONLY FOR THE INNER CALL. A RoundTripper must not modify the
	// request it is given; `WithContext` yields a shallow copy, and an existing
	// trace is COMPOSED with rather than replaced, so a caller's own hooks keep
	// firing.
	var infoMu sync.Mutex
	var infos []string
	var interimConn []bool
	infoBytes := 0
	infoOverflow := false
	traced := req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		Got1xxResponse: func(code int, h textproto.MIMEHeader) error {
			var b strings.Builder
			// ⚠ MARKED, BECAUSE THIS PROGRAM WROTE IT. The version, the code and the
			// reason phrase all come from `net/http`'s own table, not from the wire --
			// `Got1xxResponse` hands over a code and headers, never a status line. Leaving
			// it CAPTURED made `dataOf` read the reason phrase as endpoint data, so a
			// legal supplied value equal to a standard phrase -- experiment key `Hints`
			// against a `103 Early Hints` -- refused an otherwise safe capture. `scrubSupplied` already skips status
			// lines; the guard reads provenance, and the provenance was wrong.
			// ⚠ THE CAPTURED BYTES ARE ESCAPED FIRST, THEN THE GENERATED LINE IS ADDED.
			// Escaping AFTER the marks turns the provenance bytes themselves into literal
			// `\x01` text: the line stops being recognised as generated -- or as a status
			// line at all -- the scrub rewrites a reason phrase this program wrote, and
			// the report calls the result canonical.
			// `escapeMarks` exists to make ARRIVED bytes unambiguous; running it over our
			// own marks is asking it about the wrong text.
			names := make([]string, 0, len(h))
			for k := range h {
				names = append(names, k)
			}
			sort.Strings(names)
			for _, k := range names {
				for _, v := range h[k] {
					fmt.Fprintf(&b, "%s: %s\r\n", k, v)
				}
			}
			b.WriteString("\r\n")
			infoMu.Lock()
			// Record interim responses raw and apply the response redactor at render
			// time. Request-header provenance cannot classify an interim response.
			// The callback bounds accumulated interim headers because net/http resets
			// its header allowance after each interim block. Record overflow explicitly.
			//
			// Connection provenance belongs to this interim header block. Do not
			// inherit it from the final response or another interim block.
			line := marked(fmt.Sprintf("HTTP/1.1 %d %s", code, http.StatusText(code))) +
				"\r\n" + escapeMarks(b.String())
			// ⚠ `net/http` DELETES `Connection` FROM AN INTERIM'S HEADERS BEFORE THIS
			// CALLBACK. Transfer processing runs first, so a 1xx that carried
			// `Connection: close` reaches here without it: this lookup says false and the
			// reconstructed block silently omits a field the endpoint sent, while the
			// report presents the block as evidence of the interim headers.
			//
			// The field cannot be recovered from here, so the block is QUALIFIED rather
			// than claimed complete: a note is emitted with it, and the section's prose
			// says the same. Refusing every interim instead would cost the record for a
			// field that is usually absent.
			_, hadConn := h["Connection"]
			if len(infos) >= maxInterimResponses || infoBytes+len(line) > maxInterimBytes {
				infoOverflow = true
				infoMu.Unlock()
				return nil
			}
			infoBytes += len(line)
			infos = append(infos, line)
			interimConn = append(interimConn, hadConn)
			infoMu.Unlock()
			return nil
		},
	}))
	resp, err := r.inner.RoundTrip(traced)
	if err != nil {
		// DNS, TLS, connection setup: the request was formed but nothing came
		// back. Recorded as an attempt WITH NO RESPONSE rather than dropped,
		// because a recorder that keeps the request and loses the failure lets
		// the program print a pair whose second half never existed.
		ex.transErr = err
		infoMu.Lock()
		ex.infos = infos
		ex.interimConn = interimConn
		ex.infoOverflow = infoOverflow
		infoMu.Unlock()
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
	// a connection-option, and never received. The earlier fix distinguished the two PROTOCOLS, which was the
	// right distinction for the case it was shown and not the question.
	// ⚠ PRESENCE, NOT THE FIRST VALUE. `Header.Get` returns the FIRST value, so a
	// response sending `Connection:` and then `Connection: YmFy` reported the
	// field as absent -- both lines were marked serialiser-generated and the guard
	// skipped a directly decodable identifier. Map membership answers the question that was asked; `Get` answers a
	// question about a value.
	//
	// Go can consume Connection: close into resp.Close and reconstruct the
	// field in DumpResponse. Header membership alone therefore cannot
	// establish whether the field arrived from the endpoint.
	//
	// `resp.Close` is also true for an HTTP/1.0 response with no keep-alive, where
	// the endpoint sent no such line -- so this errs toward CAPTURED, which is the
	// direction that keeps the guard looking.
	_, ex.recvConn = resp.Header["Connection"]
	// resp.Close can represent a consumed Connection: close or an HTTP/1.1
	// response delimited by EOF. DumpResponse emits the field in both cases,
	// so the boolean alone does not establish received provenance.
	//
	// The two are separable by the FRAMING. A response with an explicit length or a
	// chunked encoding does not need `Close` for delimitation, so `Close` there came
	// from the field; without either, it is ambiguous and the line keeps its
	// serialiser provenance -- which is the direction that does not rewrite bytes
	// this program invented.
	if resp.Close && !ex.recvConn {
		explicitFraming := resp.ContentLength >= 0
		for _, te := range resp.TransferEncoding {
			if strings.EqualFold(te, "chunked") {
				explicitFraming = true
			}
		}
		// ⚠ AND FRAMING SEPARATES THE TWO ONLY ABOVE HTTP/1.0. Below it, closure is
		// the PROTOCOL DEFAULT: net/http sets `Close` on a 1.0 response carrying an
		// explicit `Content-Length` and no `Connection` field at all -- measured,
		// `HTTP/1.0 200 OK\r\nContent-Length: 2` gives `Close=true` while the same
		// response as 1.1 gives false. So the framing test answered "the field was
		// received" about a field that was never sent, and with a supplied `close`
		// the scrub rewrote the synthesised line into `Connection: <redacted, 5
		// chars>` and called the result canonical. `ProtoAtLeast` is the standard library's own spelling of the
		// question; arithmetic on the version numbers would be a second grammar.
		if !resp.ProtoAtLeast(1, 1) {
			explicitFraming = false
		}
		// Without explicit framing, Close cannot distinguish a consumed field
		// from EOF framing. Refuse that ambiguity instead of declaring the
		// reconstructed field received or generated.
		//
		// So the capture REFUSES. That is the standing trade in this file: a
		// withheld record costs an operator a minute, and either default costs the
		// subject.
		ex.recvConn = explicitFraming
		ex.closeAmbiguous = !explicitFraming
	}
	if d, derr := httputil.DumpResponse(resp, false); derr == nil {
		ex.head = d
	}
	infoMu.Lock()
	ex.infos = infos
	ex.interimConn = interimConn
	ex.infoOverflow = infoOverflow
	infoMu.Unlock()
	ex.uncompressed = resp.Uncompressed
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
// Counting cannot detect overflow past the SDK's read ceiling: its
// io.LimitReader stops calling this wrapper once that ceiling is reached.
// A larger recorder buffer alone cannot prove the response was complete.
//
// A recorder downstream of a limit cannot see past it. So the honest signal is
// not "overflowed" but "AT THE CEILING, therefore INDETERMINATE": the body may
// be whole and exactly this long, or the SDK's read may have stopped short.
// Both are reported as an incomplete capture, because a record cannot tell them
// apart and guessing is the defect.
const capturedBodyMax = (1 << 20) + 1

// sdkMaxBodyBytes mirrors `expMaxBodyBytes`, which aliases `rcMaxBodyBytes` --
// the size past which BOTH SDK read paths refuse a body before its schema means
// anything.
//
// ⚠ ONE LITERAL, AND THE RELATION STATED. Writing `1 << 20` again would be a
// second place for the same number to drift from; the ceiling above is defined as
// one byte more than this on purpose, so the two move together by construction.
// TestTheMirroredBodyLimitMatchesTheSDKs reads the SDK's own constant out of the
// source and fails if either relation stops holding.
const sdkMaxBodyBytes = capturedBodyMax - 1

// snapTrailers copies the trailer block as it stands now.
//
// ⚠ IT IS CALLED ON CLOSE AS WELL AS AT EOF. When a body is EXACTLY the read
// ceiling and also carries trailers -- a legal HTTP/2 shape -- the SDK's own
// io.LimitReader synthesises EOF without calling Read again, so this wrapper
// never saw one. The record then announced a `Trailer` field and omitted its
// contents while calling the pair complete.
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
		// calling the pair complete.
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
	// omitting its contents, and calling the pair complete.
	t.snapTrailers()
	return err
}

// requestOwnedHeaders identifies the fields supplied by the SDK or this
// program. Comparing this set with the dump identifies fields written by
// DumpRequestOut, including its default User-Agent and Accept-Encoding.
// Keeping this calculation in a function lets tests exercise the same
// call site as the recorder.
func requestOwnedHeaders(req *http.Request) http.Header {
	ours := http.Header{}
	if req != nil {
		ours = req.Header.Clone()
		if ours == nil {
			ours = http.Header{}
		}
	}
	// ⚠ THE AUTHORITY IS OURS AND IS NOT IN `req.Header`. Go stores it in
	// `Request.Host`/`URL.Host`, so asking only the header map called every
	// `Host:` line serialiser-written -- and a supplied value equal to the
	// configured host was then skipped by the scrub AND the guard, and printed.
	// Deriving from one of the two places the data lives in is not deriving.
	ours.Set("Host", "")
	return ours
}

func writtenBySerialiser(name string, ours http.Header) bool {
	if ours == nil {
		return false
	}
	_, set := ours[http.CanonicalHeaderKey(strings.TrimSpace(name))]
	return !set
}

// redact renders a request dump. `fromRedirect` says the SDK issued this leg
// because the ENDPOINT sent it there, which changes who authored two of the
// lines below -- see endpointChosenTarget.
func redact(dump []byte, ours http.Header, fromRedirect bool) []byte {
	out := make([]string, 0, 32)
	for _, line := range strings.Split(string(dump), "\n") {
		if strings.HasPrefix(line, "GET ") || strings.HasPrefix(line, "POST ") {
			if fromRedirect {
				// ⚠ THE TARGET OF A REDIRECT LEG IS THE ENDPOINT'S, PATH AND ALL.
				// `redactQuery` redacts the query and KEEPS harness-authored parameter
				// names, which is right for a request this program composed and wrong
				// here twice over: the names are the endpoint's too, and the PATH was
				// never touched at all. Measured on a two-redirect chain, a
				// `Location: /server-secret-token?x=y` was reissued as the next
				// request line and published whole.
				line = redirectRequestLine(line)
			} else {
				line = redactQuery(line)
			}
		}
		// ⚠ AND `Referer` IS GENERATED BY THE CLIENT FROM THE PREVIOUS TARGET. On a
		// chain of two or more redirects `http.Client` puts the previous
		// endpoint-selected URL here, so it reaches this dump as an ordinary request
		// header -- present in `req.Header`, therefore not serialiser-written,
		// therefore name-marked with its value left to a scrub that cannot see an
		// endpoint's bytes. It carried its query
		// values verbatim: nothing on the request side reads a header value as a URI.
		if fromRedirect && strings.HasPrefix(strings.ToLower(line), "referer:") {
			cr := ""
			body := line
			if strings.HasSuffix(body, "\r") {
				cr, body = "\r", strings.TrimSuffix(body, "\r")
			}
			// ⚠ AND THE NAME IS MARKED, like every other request header name. The
			// first version returned `redactTarget`'s line as it stands, leaving
			// `Referer` bare inside the captured span -- so a legal experiment key of
			// `Referer` would be reported by the guard as a survivor and refuse every
			// run, exactly as `Host` and `User-Agent` did before them.
			if name, gap, _, ok := splitField(body); ok {
				out = append(out, marked(name+":")+gap+
					strings.TrimPrefix(redactTarget(body), name+":"+gap)+cr)
				continue
			}
			out = append(out, redactTarget(body)+cr)
			continue
		}
		// The header's PRESENCE is kept -- an absent Authorization header is
		// itself a client-profile defect this capture exists to detect, so
		// replacing the line entirely would hide the failure it is meant to show.
		// ⚠ THE CONFIGURED AUTHORITY IS OURS, VALUE AND ALL. The generic branch below
		// marks only the field NAME, so with `SP_REMOTE_CONFIG_URL` of
		// `https://app.shardpilot.com` and a legal experiment key of `app`, the guard
		// found `app` inside the host this program itself configured and refused
		// EVERY otherwise valid capture. The
		// same rule as the fixed route and the serialiser-written headers: what this
		// program put on the wire is not the endpoint's choice.
		// ⚠ AND THE COMPARISON CANNOT BE WITH THE PRE-SERIALISATION SPELLING. A
		// request dump's `Host` is written by the client from the URL this program
		// configured, and `DumpRequestOut` serialises an internationalised name as
		// PUNYCODE -- `https://é.example` arrives here as `Host: xn--9ca.example`, the
		// equality failed, and the generic scrub rewrote this program's own authority
		// into `xn--9ca.<redacted, 7 chars>`: a corrupted canonical request the guard
		// approves because a placeholder is generated.
		//
		// A REQUEST dump carries no endpoint bytes in this field by construction, so
		// the line is vouched for on that ground rather than on a spelling match. The
		// `configuredHost` test is kept as the positive control it always was: when it
		// holds, nothing changed; when it does not, this is a serialisation of the same
		// authority and the mark says so either way.
		if (configuredHost != "" || configuredHostWire != "") && strings.HasPrefix(strings.ToLower(line), "host:") {
			if i := strings.IndexByte(line, ':'); i > 0 {
				v := strings.TrimSuffix(line[i+1:], "\r")
				cr := ""
				if strings.HasSuffix(line, "\r") {
					cr = "\r"
				}
				if hv := strings.TrimSpace(v); hv == configuredHost || (configuredHostWire != "" && hv == configuredHostWire) {
					out = append(out, marked(line[:i+1]+v)+cr)
					continue
				}
			}
		}
		// ⚠ A CROSS-HOST REDIRECT PUTS THE ENDPOINT'S AUTHORITY HERE. The branch
		// above vouches `Host` only when it IS the configured one; anything else on a
		// followed leg was chosen by the endpoint, and the generic branch below marks
		// the NAME and leaves the value to a scrub that cannot see it. See
		// endpointChosenAuthority for why "a host is exempt" does not cover this.
		if fromRedirect && strings.HasPrefix(strings.ToLower(line), "host:") &&
			!strings.HasPrefix(line, genMark) {
			cr := ""
			body := line
			if strings.HasSuffix(body, "\r") {
				cr, body = "\r", strings.TrimSuffix(body, "\r")
			}
			if name, gap, value, ok := splitField(body); ok && value != "" {
				out = append(out, marked(name+":")+gap+endpointChosenAuthority(value)+cr)
				continue
			}
		}
		if strings.HasPrefix(strings.ToLower(line), "authorization:") {
			field := strings.SplitN(line, " ", 2)
			scheme := "<redacted>"
			if len(field) == 2 {
				if parts := strings.SplitN(strings.TrimSpace(field[1]), " ", 2); len(parts) == 2 {
					// ⚠ THE SCHEME IS SYNTAX TOO. Only the credential was marked, so a
					// legal experiment key of `Bearer` was found by the guard in
					// this program's own rebuilt line and refused every run.
					scheme = marked(parts[0]+" ") + placeholder(parts[1])
				}
			}
			// KEEP THE LINE'S OWN TERMINATOR. Splitting on "\n" leaves the
			// "\r" on every line; rebuilding this one without it emitted a lone
			// LF after Authorization while its neighbours stayed CRLF, so the
			// block advertised as a canonical HTTP/1.1 request had mixed line
			// endings.
			cr := ""
			if strings.HasSuffix(line, "\r") {
				cr = "\r"
			}
			line = marked("Authorization: ") + scheme + cr
		}
		// ⚠ EVERY REQUEST HEADER NAME IS SYNTAX THIS PROGRAM DID NOT CHOOSE. A
		// legal experiment key of `Host` or `User-Agent` was reported by the guard
		// as a surviving value, because the canonical name sat unmarked inside the
		// captured span -- so those keys could never produce a capture. Marking the NAME leaves the value
		// under the scrub, where it belongs.
		if i, ok := headerNameEnd(strings.TrimSuffix(line, "\r")); ok &&
			!strings.HasPrefix(line, genMark) {
			// ⚠ AND THE VALUES THE SERIALISER ITSELF WRITES. `DumpRequestOut` adds
			// `Accept-Encoding: gzip`, which this program did not choose and the
			// endpoint did not send -- so a legal experiment key of `gzip` was
			// found there and refused every run, exactly as the header NAMES and
			// the auth scheme did before it.
			if writtenBySerialiser(line[:i], ours) {
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
// match a server-minted cookie against.
//
// The guard and redactor share the response's server-generated field
// classification. supportedDecoders is the single six-decoder chain;
// structural scans and the supplied-value guard use the same forms.
//
// Use the shared decoder list for structural names too. Encoded minted
// field names in a diagnostic or malformed body must be interpreted
// consistently with the supplied-value guard.
var supportedDecoders = []func(string) string{
	undoPercent, undoUnicodeEscapes, undoBase64, undoHex, undoPlus, undoEntities,
}

// decodedForms returns every form reachable from `text` by applying the supported
// decoders REPEATEDLY, to a fixed point, plus `text` itself. The second return says
// whether the walk finished; a caller that scans for a forbidden shape must refuse
// when it did not, because a truncated form list is a scan that stopped early and
// reported clean.
//
// ⚠ ONE HOP IS NOT THE CHAIN. The first version applied each decoder ONCE, so a
// value behind two already-supported stages -- `%2522subject_fact_key%2522`, or
// base64-of-base64 around a `Set-Cookie:` -- reached only its middle spelling and
// neither structural scan recorded anything, while `assertNoLeak` reconstructs the
// content downstream and checks only SUPPLIED values, so the endpoint-minted secret
// was published. The guard reconstructs to a
// fixed point; a producer that stops at one hop answers about a smaller world by
// exactly the number of stages an endpoint chooses to use.
//
// The walk is bounded twice -- by the form count and by its OWN byte budget -- and
// says so rather than truncating silently.
//
// ⚠ ITS OWN, NOT THE SHARED ONE. Charging this enumeration to `decodeWork` made
// the RESULT depend on what had already been decoded elsewhere in the run: two
// scenes passed alone and failed in the suite, and in production the first large
// response would have turned every later scan into a refusal. A bound whose
// answer depends on call order is not a bound on this call.
const (
	decodedFormsMax  = 64
	decodedFormsWork = 1 << 20
	// The largest form the candidate producers are given. Above it the walk refuses
	// rather than spending superlinear work on one string.
	decodedFormsFormMax = 8 << 10
)

// mintedFieldIn reports the note for a protected field name found in ANY supported
// decoding of `name`, and whether the form list could be finished.
//
// ⚠ THE NAME IS SCANNED IN EVERY SPELLING THE GUARD RECONSTRUCTS. `%` is legal in an
// HTTP field name, so `%53et-Cookie: session=secret` passed a raw lookup while the
// publication guard's own percent decoder rebuilds `Set-Cookie:` from it -- and
// `assertNoLeak` checks only SUPPLIED values, so an endpoint-minted credential was
// published.
//
// Headers, trailers, transport diagnostics and unparsable bodies use
// this shared minted-field classifier.
func mintedFieldIn(name string) (string, bool, bool) {
	forms, whole := decodedForms(name)
	for _, f := range forms {
		if note, ok := serverMintedFields[strings.ToLower(ows(f))]; ok {
			return note, true, whole
		}
	}
	return "", false, whole
}

func decodedForms(text string) ([]string, bool) {
	// ⚠ THE BOUND HAS TO BE ON WHAT IS WALKED, NOT ON WHAT IS CHARGED AFTERWARDS.
	// Measured: a 108 KB run costs about 240 MiB across this walk -- six decoders and a
	// tokenising scan per form, sixty-four forms -- and charging each producer's pass
	// count afterwards changed that by 2 MiB. A budget consulted after the expensive
	// call is a report, not a limit.
	//
	// Refused rather than truncated: the caller treats an unfinished form list as a
	// structural refusal, so a diagnostic too large to enumerate costs a capture
	// instead of a silent clean scan.
	if len(text) > decodedFormsFormMax {
		return []string{text}, false
	}
	// Suffix producers decode a candidate for each separator position, so
	// work depends on separators times length. Charge candidate-producing
	// work as well as the input length.
	//
	// Preflighted from the cost model rather than materialised and then regretted:
	// counting the separator bytes is one linear pass and needs no allocation.
	seps := 0
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '/', '+', '-', '_', '=', '.', ':':
			seps++
		}
	}
	if seps*len(text) > decodedFormsWork {
		return []string{text}, false
	}
	seen := map[string]bool{text: true}
	out := []string{text}
	spent := 0
	add := func(f string) bool {
		if f == "" || seen[f] {
			return true
		}
		if len(out) >= decodedFormsMax {
			return false
		}
		seen[f] = true
		out = append(out, f)
		return true
	}
	for i := 0; i < len(out); i++ {
		for _, d := range supportedDecoders {
			spent += len(out[i])
			if spent > decodedFormsWork {
				return out, false
			}
			if f := d(out[i]); f != out[i] {
				if !add(f) {
					return out, false
				}
			}
		}
		// ⚠ AND THE CANDIDATE PRODUCERS, NOT ONLY THE REWRITING DECODERS. `undoBase64`
		// leaves a token whose decode is not valid UTF-8 exactly as it found it -- so a
		// minted field inside `/yJzdWJqZWN0X2ZhY3Rfa2V5Ijoic2ZrX3NlY3JldCI=` was in no
		// form this walk produced, `noteMinted` recorded nothing, and `assertNoLeak`
		// later built that very candidate and checked it only against SUPPLIED values. The guard's reconstruction includes the
		// producers; a scan that omits them is narrower than the guard by exactly the
		// encodings a decoder cannot rewrite in place.
		spent += len(out[i])
		if spent > decodedFormsWork {
			return out, false
		}
		// ⚠ AND THE WHOLE-LINE FOLD, WHICH NO PRODUCER COVERS. `wrappedBase64Candidates`
		// deliberately SKIPS a line that is entirely base64, because `joinBase64Runs`
		// is supposed to have joined it -- and that normalisation ran on the outer text
		// only. So `InN1\r\nYmplY3RfZmFjdF9rZXkiOiJzZmtfc2VjcmV0Ig==` was in no form this
		// walk produced, though three ordinary decodes that ignore CR/LF reconstruct the
		// member. A producer that assumes another
		// pass ran first is only correct where that pass runs.
		//
		// ⚠ AND EACH PRODUCER IS CHARGED FOR ITSELF. One `len(out[i])` before five of
		// them said nothing about what they cost: `binaryCandidates` makes two full
		// scans and the suffix producers decode many overlapping tails, so a 100 KB run
		// full of separators could retain tens of MiB before the form cap was reached,
		// under a ceiling advertised as 1 MiB. The pass count is per
		// producer and stated at the call, as it is in the seed loop.
		if j := joinBase64Runs(out[i]); j != out[i] {
			spent += len(out[i])
			if !add(j) {
				return out, false
			}
		}
		// ⚠ AND A CHARGE AFTER THE CALL CANNOT BOUND THE CALL. Measured: a 108 KB run
		// full of separators costs 242 MiB inside `binaryCandidates` alone, and charging
		// its pass count afterwards changes that by nothing -- the suffix enumeration is
		// superlinear in the run, so the only bound that binds is on what the producers
		// are HANDED. Skipping them silently would
		// be a scan that stopped early and reported clean, so the walk reports itself
		// incomplete and the callers refuse.
		for _, prod := range []struct {
			passes int
			fn     func(string) []string
		}{
			{2, binaryCandidates}, {1, shortBase64Candidates}, {1, hexCandidates},
			{1, base64SuffixCandidates}, {1, wrappedBase64Candidates},
		} {
			spent += prod.passes * len(out[i])
			if spent > decodedFormsWork {
				return out, false
			}
			for _, f := range prod.fn(out[i]) {
				if !add(f) {
					return out, false
				}
			}
			spent += takeDecodeWork()
			if spent > decodedFormsWork {
				return out, false
			}
		}
	}
	return out, true
}

func noteStructuralInText(text string) {
	for _, ln := range strings.Split(text, "\n") {
		// Search the whole diagnostic for minted-field shapes: Go can wrap an
		// endpoint line in quoted prose. Decode escapes to a fixed point before
		// scanning identifier tokens, checking each produced spelling because
		// decoding can also join names. Charge and scan intermediate forms as
		// they are produced rather than retaining all of them.
		//
		// The budget is the SHARED one, charged before each pass, and exceeding it
		// refuses rather than truncating: a scan that stopped early and said nothing
		// reports a clean line it did not finish reading.
		scanForm := func(form string) {
			// ⚠ THE TOKENISER'S ALPHABET MUST BE AS WIDE AS THE PREDICATE'S.
			// `isMintedName` folds the way `encoding/json` does, so it MATCHES
			// `ſubject_fact_key` -- and this splitter, being ASCII-only, cut the name
			// at `ſ` and never handed it that token. A candidate the predicate would accept but the splitter cannot
			// produce is a predicate that is never asked.
			for _, tok := range strings.FieldsFunc(form, func(r rune) bool {
				return !(r == '_' || r == '-' || isWordRune(r))
			}) {
				if isMintedName(tok) {
					noteStructural(formDiagnostic, "a server-minted field inside a transport error")
				}
			}
			// ⚠ AND THE FIELD NAMES ON EVERY FORM, NOT ONLY THE ARRIVED ONE. This scan
			// decodes to a fixed point precisely because a name can be spelled to defeat
			// a reader, and then the `serverMintedFields` sweep ran ONCE, over the
			// undecoded line: `%53et-Cookie: session=secret` was decoded here, handed to
			// `isMintedName` -- which answers about minted IDENTIFIERS, not field names
			// -- and nothing was recorded, so the diagnostic was published and the
			// supported percent decoder reconstructs the cookie from it. One scan asked about every form and
			// its twin asked about one; the forms are the population for both.
			// Normalised exactly as the single pass it replaces was: marks stripped and
			// the line trimmed, so the first form answers identically and every later
			// one is asked the same question.
			lowForm := strings.ToLower(strings.TrimSpace(stripMarks(form)))
			for _, name := range slices.Sorted(maps.Keys(serverMintedFields)) {
				if strings.Contains(lowForm, name+":") {
					noteStructural(formDiagnostic, serverMintedFields[name]+" inside a transport error")
				}
			}
		}
		// ⚠ ONE WALK, NOT A WALK PER HOP. This loop used to advance `cur` through the
		// nesting itself and scan each step; now that `decodedForms` iterates to a fixed
		// point, calling it once per step made the scan QUADRATIC in the nesting -- 1119
		// MiB against a 1024 MiB bound on a 40 KB diagnostic, which the budget scene
		// reported immediately. The advancing and the enumeration were two spellings of
		// the same fixed point; one of them had to go.
		spent := 0
		forms, whole := decodedForms(ln)
		for _, form := range forms {
			spent += len(form)
			scanForm(form)
			if spent > decodeWorkMax {
				noteStructural(formDiagnostic, "a transport diagnostic whose decoding exceeded this build's work budget")
				break
			}
		}
		if !whole {
			noteStructural(formDiagnostic, "a transport diagnostic whose decoded forms could not be enumerated")
		}
		decodeWork += spent
	}
}

// sanitize redacts any request URL an error carries. `url.Error` wraps the FULL
// url, so printing a deadline or transport failure verbatim republished the
// unredacted subject_key and every targeting value -- the same leak the request
// dump had already been fixed for, arriving by a second road.
// ⚠ AN ERROR FROM THE TRANSPORT CARRIES ENDPOINT BYTES. Go's parser puts the
// offending line into the error it returns, so a malformed response can place an
// encoded supplied value there -- and the result was written into report PROSE,
// outside any captured span, where the guard's decoders never look. It is marked as captured, so the same
// fixed-point check runs over it.
// sanitizeCaptured is for the REPORT, where the guard reads it. sanitize is for
// stderr, which the guard never sees and where a marker byte would print as a
// raw control character.
// stdErrToken admits a token the STANDARD LIBRARY chose, and refuses anything
// else. `Op`, `Net` and `Syscall` are set by Go from fixed strings, and this says
// so as a check rather than as a belief: if one of them ever carries something
// else, the describer refuses instead of publishing it.
func stdErrToken(s string) (string, bool) {
	if s == "" || len(s) > 32 {
		return "", false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') &&
			!(c >= '0' && c <= '9') && c != '-' && c != '_' {
			return "", false
		}
	}
	return s, true
}

// describeTransportError renders a transport failure from the error VALUE, never
// from its rendered text.
//
// ⚠ `err.Error()` IS WHERE ENDPOINT DATA IS FUSED INTO PROSE, AND THE VALUE DOES
// NOT FUSE IT. The redaction this replaces asked which EXTENTS of a rendered
// message to remove, and that is an enumeration maintained by hand against a
// moving surface: `x509: certificate is valid for server-secret-token.internal,
// not configured.example` carries certificate-controlled names with no quotes
// around them, so a rule about quoted extents left them standing and the ledger
// was empty.
//
// The decisive fact is that `x509.HostnameError` HAS NO FIELD holding that list:
// `Error()` builds it from `Certificate.DNSNames`. Working from the value, the
// names are not redacted -- they are never taken. What is published is their
// COUNT and whether the configured host is among them, which is what an operator
// needs and is ours by construction.
//
// The set of types is not closed either, and that is stated rather than hidden:
// `OpError.Err` is an `error` and may be anything. So an unrecognised type is a
// REFUSAL naming the type, and no branch of this function can reach `Error()`.
// The difference from a rule about quotes is that the refusal now fires on an
// unknown TYPE, which is a property of this program's coverage, instead of on the
// absence of a punctuation mark, which is a property of someone else's prose.
// describeHostnameError renders a hostname mismatch from the value's fields. The
// SAN list is NOT a field: `Error()` builds it from `Certificate.DNSNames`, so
// working from the value means the names are never taken.
func describeHostnameError(e x509.HostnameError) string {
	// ⚠ THE FAMILY THAT ANSWERS THE QUESTION IS THE ONE THE HOST IS IN. crypto/x509
	// builds its own diagnostic from `IPAddresses` when the host is an IP literal
	// and from `DNSNames` otherwise; this counted DNS names always, so an IP
	// endpoint whose certificate carries two DNS SANs and no IP SAN reported
	// `names=2` -- an unrelated number, printed exactly where an operator is
	// diagnosing an IP.
	//
	// The family is PRINTED, because a count whose population is not named is not a
	// measurement. Both are this program's own text: no SAN is taken.
	family, names, listed := "dns", 0, false
	if ip := net.ParseIP(e.Host); ip != nil {
		family = "ip"
		if e.Certificate != nil {
			names = len(e.Certificate.IPAddresses)
			for _, a := range e.Certificate.IPAddresses {
				if a.Equal(ip) {
					listed = true
				}
			}
		}
	} else if e.Certificate != nil {
		names = len(e.Certificate.DNSNames)
		for _, d := range e.Certificate.DNSNames {
			if d == e.Host {
				listed = true
			}
		}
	}
	return "x509=hostname-mismatch san=" + family + " names=" + strconv.Itoa(names) +
		" configured-host-listed=" + strconv.FormatBool(listed)
}

func describeTransportError(err error, depth int) (string, bool) {
	if err == nil || depth > 8 {
		return "", false
	}
	switch e := err.(type) {
	case recorderDiag:
		// This program's own words, so there is nothing here to withhold. It is
		// described rather than refused for exactly the reason the refusal below
		// exists: that one guards against publishing an ENDPOINT's message, and this
		// error never carried one.
		return e.msg, true
	case *url.Error:
		op, ok := stdErrToken(e.Op)
		inner, innerOK := describeTransportError(e.Err, depth+1)
		if !ok || !innerOK {
			return "", false
		}
		return "op=" + op + " " + inner, true
	case *net.OpError:
		op, ok := stdErrToken(e.Op)
		if !ok {
			return "", false
		}
		out := "op=" + op
		if e.Net != "" {
			n, ok := stdErrToken(e.Net)
			if !ok {
				return "", false
			}
			out += " net=" + n
		}
		// ⚠ NEITHER `Addr` NOR `Source` IS PUBLISHED. The destination is already in
		// the report as the CONFIGURED target; the address in the error is what
		// resolution produced, which is infrastructure this program was not asked to
		// disclose.
		inner, innerOK := describeTransportError(e.Err, depth+1)
		if !innerOK {
			return "", false
		}
		return out + " " + inner, true
	case *os.SyscallError:
		call, ok := stdErrToken(e.Syscall)
		inner, innerOK := describeTransportError(e.Err, depth+1)
		if !ok || !innerOK {
			return "", false
		}
		return "syscall=" + call + " " + inner, true
	case syscall.Errno:
		return "errno=" + strconv.Itoa(int(e)), true
	case *net.DNSError:
		// ⚠ THE BOOLEANS, NOT `Err`, `Name` OR `Server`. `Err` is a message string,
		// `Name` is the host this program configured and already publishes as the
		// target, and `Server` is the resolver -- infrastructure nobody asked this
		// program to disclose. What an operator needs is which KIND of lookup failure
		// it was, and the three flags say exactly that.
		return "dns=lookup not-found=" + strconv.FormatBool(e.IsNotFound) +
			" timeout=" + strconv.FormatBool(e.IsTimeout) +
			" temporary=" + strconv.FormatBool(e.IsTemporary), true
	case *tls.CertificateVerificationError:
		inner, ok := describeTransportError(e.Err, depth+1)
		if !ok {
			return "", false
		}
		return "tls=verification " + inner, true
	case x509.HostnameError:
		// ⚠ THE VERIFIER RETURNS THIS BY VALUE. `crypto/x509` constructs
		// `HostnameError{…}` and `tls.CertificateVerificationError` carries it as
		// such, so a case naming only the POINTER never matched -- the ordinary
		// hostname mismatch fell through to "unrecognised type", the whole capture
		// refused with exit 4, and the summary this branch exists to emit was
		// unreachable.
		//
		// Use the HostnameError value shape returned by the verifier, including
		// when checking certificate subject-alternative-name redaction.
		return describeHostnameError(e), true
	case *x509.HostnameError:
		return describeHostnameError(*e), true
	case x509.UnknownAuthorityError:
		return "x509=unknown-authority", true
	case x509.CertificateInvalidError:
		return "x509=certificate-invalid reason=" + strconv.Itoa(int(e.Reason)), true
	}
	switch {
	case errors.Is(err, io.EOF):
		return "eof", true
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected-eof", true
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline-exceeded", true
	case errors.Is(err, context.Canceled):
		return "canceled", true
	}
	return "", false
}

func sanitizeCaptured(err error) string {
	// ⚠ ESCAPE FIRST. Go puts the offending line into the error verbatim, so an
	// endpoint can put the guard's own reserved bytes there -- and an
	// attacker-controlled `\x01…\x01` pair reads as a GENERATED span, which the
	// guard blanks and `stripMarks` then publishes. Provenance marks in captured text are
	// escaped everywhere else in this program; this path had been added without
	// them.
	// ⚠ ESCAPE THE RAW ERROR, BEFORE THE STAGE THAT CREATES MARKS OF ITS OWN.
	// `sanitize` runs `dropQuery`, which inserts a GENERATED `query-withheld`
	// token -- and this outer escape then read those freshly generated bytes as
	// endpoint bytes and rendered them literally, so after `stripMarks` the report
	// said the transport error contained a mark pair neither the transport nor the
	// endpoint ever produced. The order was
	// right for the byte it was written against and wrong for the byte the stage
	// beneath it adds.
	if err == nil {
		return asCaptured("")
	}
	if d, ok := describeTransportError(err, 0); ok {
		// Every byte of this was written by this program, so it is generated, and the
		// fixed-point check over the captured span still runs.
		return asCaptured(marked(d))
	}
	// ⚠ THE REFUSAL NAMES THE TYPE, AND NOTHING ELSE FROM THE ERROR. The set of
	// error types is not closed -- `OpError.Err` is an `error` -- so this is where
	// the coverage of the describer above ends, said out loud. A type name is
	// compiled into this program; the message is not.
	noteStructural(formDiagnostic, "a transport failure of a type this build cannot describe: "+fmt.Sprintf("%T", err))
	return asCaptured(marked("<transport failure withheld: unrecognised error type>"))
}

// sanitize renders an error for STDERR. The marks are stripped: they exist so the
// publication guard can tell captured bytes from generated ones, and a terminal
// gets raw SOH control bytes instead of a message. `sanitizeCaptured` keeps them, because
// that output is what the guard reads.
func sanitize(err error) string {
	return stripMarks(sanitizeRaw(err))
}

func sanitizeRaw(err error) string {
	if err == nil {
		return ""
	}
	// ⚠ STDERR IS A PUBLICATION TOO. The guard never reads this line, which is why
	// it kept rendering `err.Error()` after the report stopped -- and an operator
	// reading it, or a CI log keeping it, sees exactly what the report refused to
	// print. The same describer answers both, so there is one place where endpoint
	// text could enter and it takes nothing from the message.
	if d, ok := describeTransportError(err, 0); ok {
		return d
	}
	return "transport failure withheld: unrecognised error type " + fmt.Sprintf("%T", err)
}

// sanitizeText redacts every query string in a piece of text, scanning FORWARD
// past each replacement.
//
// The first version restarted from the beginning after each substitution, and
// the `?` it had just processed was still there -- so it selected the same
// marker again, and because the inserted text contains a space the next segment
// was shorter each time while the string grew. On the very case this exists for,
// a url.Error carrying a query, it never terminated.
// sanitizeText removes the query from every URL a transport diagnostic carries.
//
// ⚠ A QUESTION MARK IS NOT A QUERY UNLESS IT IS IN A URL. This scanned for `?`
// anywhere and rewrote the rest of the token, so Go's own
// `malformed HTTP response "BOGUS?detail"` came out as `BOGUS?query-withheld` --
// the report altering the very line it exists to preserve as evidence of the
// failure. The span is found by the scheme
// separator now, and only inside it does a `?` mean anything.
func sanitizeText(out string) string {
	var b strings.Builder
	rest := out
	for {
		i := strings.Index(rest, "://")
		if i < 0 {
			b.WriteString(rest)
			return b.String()
		}
		start := strings.LastIndexAny(rest[:i], " \"") + 1
		seg := rest[start:]
		if end := strings.IndexAny(seg, " \""); end >= 0 {
			seg = seg[:end]
		}
		// A URL ends before its enclosing punctuation. Preserve the closing
		// parenthesis and colon in a diagnostic such as failed (https://e/p?q=s):
		// when replacing its query.
		seg = strings.TrimRight(seg, ")]}>,;:.'")
		if seg == "" {
			b.WriteString(rest[:start])
			rest = rest[start:]
			continue
		}
		b.WriteString(rest[:start])
		b.WriteString(dropQuery(seg))
		rest = rest[start+len(seg):]
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
// publishing. The marks already record
// which text is ours; the scrub simply was not consulting them.
// ⚠ THE STATUS LINE IS PROTOCOL SYNTAX, NOT DATA. A legal supplied identifier can
// equal a status token -- `SP_EXPERIMENT_KEY=200` -- and the generic scrub then
// rewrote `HTTP/1.1 200 OK` into `HTTP/1.1 <redacted, 3 chars> OK`, an unparsable
// response the guard nonetheless approved, because the replacement carries
// generated marks. There is no value a
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
// Split name components after decoding stages too. A percent-decoded
// separator can introduce a component boundary that was absent from
// the original field name.
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
// published. Marking the grammar instead
// costs nothing to maintain: `overCaptured` already leaves generated spans alone
// and the guard already blanks them, so both rules follow from one mark rather
// than from two copies of a list.
// redactUnaccountedBody replaces a body this program cannot account for with its
// length.
//
// ⚠ THE CLAUSE SAYS "EVERYTHING ELSE", AND THE BODY WAS AN EXCEPTION NOBODY HAD
// WRITTEN DOWN. Every rule above it addresses JSON: the minted-member redaction,
// the schema names, the grammar literals. A `text/plain` body of
// `server-secret-token` met none of them, the refusal ledger stayed empty, and
// the guard is blind to it because the value was never supplied by this program
// -- so the response was published verbatim. "Everything else replaced by its length"
// is either true of the body too or it is not the claim.
//
// A body that PARSES as one JSON value has already been through those rules and
// is left alone; an empty body has nothing to describe. What remains is endpoint
// text in a shape this build does not cover, and the record is refused.
func redactUnaccountedBody(body string) string {
	// ⚠ EMPTY MEANS ZERO BYTES, NOT "TRIMS TO NOTHING". A body of a single space --
	// or of U+00A0 -- is neither empty nor a JSON document, and `jsonParses` rejects
	// both; `TrimSpace` made each look empty here, so the structural refusal was
	// skipped and endpoint bytes were published outside the four documented capture
	// forms with an empty ledger. The whole
	// question this function answers is what a body that describes nothing may
	// contain, and "nothing" has a length.
	// ⚠ LIMIT, AND IT IS THE FRAMING'S. A body of only CR/LF cannot be told apart
	// from the framing this dump adds, so those two bytes still count as empty; a
	// SPACE or a U+00A0 cannot come from the framing and is endpoint bytes.
	if strings.Trim(stripMarks(body), "\r\n") == "" {
		return body
	}
	if jsonParses(stripMarks(body)) {
		return body
	}
	// Refuse this body shape rather than replace it with a length. That keeps
	// the recorder's body-line preservation and captured-NUL disclosure rules
	// intact, at the cost of withholding a capture it cannot describe.
	noteStructural(formBody, "a response body in a shape this build cannot describe")
	return body
}

func markBareJSONLiterals(text string, exempt map[string]bool) string {
	// ⚠ PARSED, NOT GUESSED. The first version tested the bytes around the token,
	// which marks `{"message":"saw false value"}` and `error: false` as grammar --
	// and a marked span is skipped by BOTH the scrub and the guard, so the
	// supplied value was published. A
	// heuristic about where a token sits is not a statement about the grammar it
	// sits in. `encoding/json` knows which of them is a literal NODE; nothing
	// else does.
	//
	// A body outside the JSON grammar is left to the ordinary scrub.
	// Accept exactly one JSON value surrounded only by JSON whitespace.
	// Parse a mark-free view, then edit the original: earlier provenance
	// marks inside strings are not part of the JSON document.
	//
	// Swapping the two passes does not fix it: BOTH need a parsable document and
	// BOTH insert marks, so whichever runs second is handed the other's bytes --
	// measured, 18 tests said so. The dependency is removed rather than reordered:
	// the parse runs over a view with the marks taken out, and every offset is
	// mapped back to the text that is actually edited.
	plain := make([]byte, 0, len(text))
	back := make([]int, 0, len(text))
	// ⚠ AND WHETHER EACH BYTE WAS ALREADY INSIDE A MARK. Marking a quote that
	// bounds an existing placeholder produced NESTED marks, which read as captured
	// text -- the fixture for exactly that said so on the first run. Containment is
	// tracked while the view is built, because after it is built the information is
	// gone.
	inMark := make([]bool, 0, len(text))
	depth := 0
	for i := 0; i < len(text); i++ {
		if text[i] == capturedMark[0] || text[i] == genMark[0] {
			depth++
			continue
		}
		plain = append(plain, text[i])
		back = append(back, i)
		inMark = append(inMark, depth%2 == 1)
	}
	view := string(plain)
	back = append(back, len(text))

	start := 0
	for start < len(view) && (view[start] == ' ' || view[start] == '\t' || view[start] == '\n' || view[start] == '\r') {
		start++
	}
	// ⚠ AND A SCALAR IS A WHOLE JSON DOCUMENT. Beginning at `{` or `[` named two
	// of the grammar's root forms and left the other five: a body that is exactly
	// `null`, `true`, `false`, a number or a string returned here unexamined, so
	// this pass neither marked it as grammar nor noted the number collision --
	// and the scrub downstream replaced the entire document with a bare
	// `<redacted, 4 chars>`, which is not JSON, with an EMPTY refusal ledger. Measured against the nested forms this
	// file already handles: `{"assigned":true}` is marked and passes untouched,
	// `[9876543210987654]` is refused as a colliding number -- at the root, both
	// were published instead.
	//
	// The gate was a cheap pre-filter for "is this JSON at all", and the one-value
	// check below answers that properly: a body that is not one JSON document with
	// only whitespace around it still returns unchanged.
	if start >= len(view) {
		return text
	}
	// ⚠ ONE VALUE, NOT A STREAM. `json.Decoder` reads a SEQUENCE of top-level
	// values, so `{"x":1} false` walked as valid and the trailing literal was
	// marked as grammar -- and a marked span is skipped by BOTH the scrub and the
	// guard, so a supplied value of `false` was published, while `json.Unmarshal`
	// rejects that body as a verdict outright. The comment above already said this function protects a GRAMMAR;
	// a stream of values is not the grammar this program's responses have.
	{
		v := json.NewDecoder(strings.NewReader(view[start:]))
		var one json.RawMessage
		if err := v.Decode(&one); err != nil {
			return text
		}
		if strings.TrimSpace(view[start+int(v.InputOffset()):]) != "" {
			return text
		}
	}
	dec := json.NewDecoder(strings.NewReader(view[start:]))
	dec.UseNumber()
	type span struct{ a, b int }
	var spans []span
	// ⚠ THE SCHEMA'S MEMBER NAMES ARE GRAMMAR TOO, not only its literals. A legal
	// supplied key may equal a response member -- `assigned`, `experiment_key` --
	// and the generic scrub rewrote the NAME, so a successful report no longer
	// carried the verdict schema the endpoint sent, while the generated
	// provenance made the guard approve it.
	// The literals were marked here for exactly this reason and the names beside
	// them were not.
	//
	// Recognition can fold case; provenance can vouch only the canonical
	// spelling that this program itself would write.
	//
	// Depth and turn are tracked because `Token()` returns a string for a key and
	// for a value alike: only position tells them apart.
	// 0 = array, 1 = object expecting a KEY, 2 = object expecting a VALUE.
	//
	// Track container kind and key/value position separately. Array elements
	// are not object keys, and closing a child container advances the parent's
	// position before the next member is read.
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
		// Continue into the delimiter-marking switch after updating depth and
		// key/value position. An early continue here would leave braces unmarked.
		isKey, atRoot := false, false
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
		} else if n := len(objDepth); n > 0 && objDepth[n-1] == 1 {
			isKey, atRoot = true, n == 1
			objDepth[n-1] = 2
		} else {
			advance()
		}
		// ⚠ AT THE ROOT ONLY, and only the CANONICAL spelling. `benignTopLevel`
		// describes the SDK's TOP-LEVEL schema, so marking those names at every depth
		// exempts an endpoint-controlled nested member of the same name; and the
		// predicates fold, so vouching on recognition publishes a supplied `ASSIGNED`.
		if name, ok := tok.(string); ok && isKey && atRoot {
			// A minted name is grammar only in a response shape where the SDK reads
			// it. Apply the same status and body-shape conditions as the other
			// schema-name registry.
			if exempt[name] || (mintedNames[name] && len(exempt) > 0) {
				end := start + int(dec.InputOffset())
				quoted := `"` + name + `"`
				if start+end-len(quoted) >= 0 && start+end <= len(view) &&
					view[start+end-len(quoted):start+end] == quoted {
					spans = append(spans, span{back[start+end-len(quoted)], back[start+end-1] + 1})
				}
			}
			continue
		}
		switch tok.(type) {
		case json.Delim:
			// ⚠ THE DELIMITERS ARE GRAMMAR TOO, and the parser is what identifies them.
			// A supplied identifier may legally BE `{`: with that experiment key an
			// ordinary body came back as `<redacted, 1 chars>"assigned":false}`, which
			// the guard approved because the placeholder is generated -- published JSON
			// that no longer parses. Same walk that
			// finds the bare literals; a delimiter is one byte at the offset the decoder
			// has just passed.
			end := int(dec.InputOffset())
			if end-1 >= 0 && end-1 < len(view) {
				spans = append(spans, span{back[start+end-1], back[start+end-1] + 1})
			}
		case json.Number:
			// ⚠ A NUMBER IS GRAMMAR TOO. A supplied identifier may legally BE `1`, and
			// an ordinary `{"version":1}` was scrubbed into
			// `{"version":<redacted, 1 chars>}` -- published JSON that no longer parses,
			// approved because the placeholder is generated. The literals, the delimiters and the
			// punctuation were each given this rule in turn; the numbers are the fourth
			// kind of token in the same grammar.
			end := int(dec.InputOffset())
			lit := string(tok.(json.Number))
			// ⚠ THE LEXEME IS NOT THE SYNTAX. Marking every number vouched the
			// ENDPOINT'S choice of value: with `123456` supplied, `{"version":123456}`
			// published the exact identifier because a marked span is skipped by both
			// the scrub and the guard. Numeric
			// syntax constrains the representation and says nothing about who chose it
			// -- the same sentence the header values and the cookie attributes carry.
			//
			// A colliding number is REFUSED rather than replaced. A numeric sentinel
			// would keep the document parsing and say `"version":0`, which a reader
			// takes for the endpoint's answer: a redaction that corrupts is worse than
			// one that refuses, and this file states that trade elsewhere.
			if scrubSuppliedRaw(lit) != lit {
				noteStructural(formBody, "a JSON number that collides with a supplied value")
			} else if end-len(lit) >= 0 && view[start+end-len(lit):start+end] == lit {
				spans = append(spans, span{back[start+end-len(lit)], back[start+end-1] + 1})
			}
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
				view[start+end-len(lit):start+end] == lit {
				// Offsets are mapped back to the text that is actually edited.
				spans = append(spans, span{back[start+end-len(lit)], back[start+end-1] + 1})
			}
		}
	}
	// ⚠ `json.Delim` REPORTS BRACES AND BRACKETS ONLY. Commas, colons and the
	// quotes that bound strings are grammar too, and a supplied identifier may
	// legally BE one: with `,` the ordinary `{"assigned":false,"code":1}` was
	// published as invalid JSON. The
	// population is the punctuation of the grammar, not the subset one API
	// happens to name.
	//
	// Walked over the same mark-free view, tracking string state, so a `,` INSIDE
	// a string stays captured text -- which is the whole reason this cannot be a
	// byte scan.
	{
		inStr, esc := false, false
		for k := start; k < len(view); k++ {
			c := view[k]
			switch {
			case esc:
				esc = false
			case inStr && c == '\\':
				esc = true
			case c == '"':
				inStr = !inStr
				if !inMark[k] {
					spans = append(spans, span{back[k], back[k] + 1})
				}
			case inStr:
			case c == ',' || c == ':':
				if !inMark[k] {
					spans = append(spans, span{back[k], back[k] + 1})
				}
			}
		}
		sort.Slice(spans, func(a, b int) bool { return spans[a].a < spans[b].a })
		// ⚠ ADJACENT SPANS ARE MERGED. Marking each punctuation byte on its own put
		// `\x01{\x01\x01"\x01` in the output -- two marked spans touching, which reads
		// as a nested pair and is exactly what the double-mark fixture forbids. One
		// run of grammar is one span.
		merged := spans[:0]
		for _, sp := range spans {
			if n := len(merged); n > 0 && sp.a <= merged[n-1].b {
				if sp.b > merged[n-1].b {
					merged[n-1].b = sp.b
				}
			} else {
				merged = append(merged, sp)
			}
		}
		spans = merged
	}
	// ⚠ APPLIED AS ONE REGION MAP, NOT SPAN BY SPAN. Inserting a mark pair per span
	// put two marked regions side by side wherever a grammar byte abuts a value the
	// earlier passes had already marked -- and two touching pairs read as a nested
	// one, which the double-mark fixture forbids and the guard mis-parses. The
	// generated bytes are collected into a boolean map, the old markers are dropped,
	// and markers are re-emitted only at the boundaries of the merged regions.
	gen := make([]bool, len(text))
	{
		d := 0
		for i := 0; i < len(text); i++ {
			if text[i] == genMark[0] {
				d++
				continue
			}
			gen[i] = d%2 == 1
		}
	}
	for _, sp := range spans {
		for i := sp.a; i < sp.b && i < len(text); i++ {
			gen[i] = true
		}
	}
	var out strings.Builder
	open := false
	for i := 0; i < len(text); i++ {
		if text[i] == genMark[0] {
			continue
		}
		if gen[i] && !open {
			out.WriteString(genMark)
			open = true
		} else if !gen[i] && open {
			out.WriteString(genMark)
			open = false
		}
		out.WriteByte(text[i])
	}
	if open {
		out.WriteString(genMark)
	}
	return out.String()
}

// jsonLiterals are the three bare tokens of JSON grammar. A supplied value equal
// to one of them cannot be distinguished from the grammar itself, and replacing
// it turned every not-assigned body into `{"assigned":<redacted, 5 chars>}` --
// invalid JSON that no longer states the endpoint's verdict. The guard still reads the body; what it
// would find there is the word `false`, which the endpoint did not learn from us.
var jsonLiterals = map[string]bool{"true": true, "false": true, "null": true}

func scrubSuppliedRaw(text string) string {
	// ⚠ LONGEST FIRST. With `abcdefgh` supplied before `abcdefghi`, the shorter
	// value replaced its own prefix inside the longer one, leaving
	// `<redacted, 8 chars>i` -- a published suffix AND a wrong length, and the
	// guard no longer recognised what was left. Order is not a detail here: a substitution that destroys a longer
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
		// encoding/json would write.
		// ⚠ MATCH ON THE SPELLING, MEASURE THE VALUE. Passing the encoded form to
		// replaceValue made the placeholder describe the ENCODING: `a"b` is
		// serialised as `a\"b` and was reported as `<redacted, 4 chars>` for a
		// three-character identifier. The same defect as the request-query wire
		// length, arriving in the response path. One value has one length wherever it is printed.
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
// in a Location header carries a third spelling again, percent-encoded.
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
// Before printing, check supplied values against the forms produced by
// the named decoding chain. The guarantee is bounded by that chain.
// mark wraps generated text in a byte captured data cannot carry.
//
// Use provenance marks to distinguish generated placeholders from
// captured text. A placeholder-shaped supplied value must not create
// an exemption, and generated placeholders must not be treated as input.
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
// bytes it exists to record.
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
// before it was seen as a set. A quantity
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
// rendered as `\\x00`. A backslash run is
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
			// literal text with (m-2)/2.
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
	// mask erased it before the check. A
	// mask that can swallow the thing it is protecting is worse than no mask.
	// Only what was CAPTURED is in question; prose and placeholders are ours.
	// ⚠ AND PROTOCOL SYNTAX IS NOT CAPTURED DATA. A legal supplied value can be
	// `GET` or `200`; the scrub deliberately leaves the request and status lines
	// alone, because rewriting them produces an unparsable message -- and this
	// collected them anyway, so the guard reported the method or the status code
	// as a surviving value and those keys could never produce a capture at all. The exemption has to be the same on
	// both sides or the scrub's deliberate silence becomes the guard's false
	// alarm.
	//
	// The NAMES the boundary rule reads are collected per span and stop at the
	// blank line that ends a header block: a body line shaped like `X-foo-bar:
	// explanation` is prose, and reading it as a field name refused a completely
	// safe capture.
	var captured, names strings.Builder
	for _, rawSpan := range capturedSpan.FindAllString(text, -1) {
		span := genSpan.ReplaceAllString(rawSpan, " ")
		// ⚠ A SPACE IS NOT A TOKEN BYTE, AND A FIELD NAME IS A TOKEN. Blanking a
		// generated span keeps generated text from being read as endpoint text, which
		// is right for the VALUE side -- and it makes the NAME syntactically invalid,
		// so `headerNameEnd` fails and the name-specific decoded boundary check never
		// runs. With `bar` and `qux` supplied, `X-bar-%71ux: v` was published as
		// `X-redacted-3-chars-%71ux`, the generic value rule read the surrounding
		// hyphen as a word byte, and the guard approved a name the supported percent
		// decoder turns back into `qux`.
		//
		// The name view REMOVES the generated span instead of blanking it: `X-` and
		// `-%71ux` rejoin as `X--%71ux`, which is a token, so the name parses and its
		// components are decoded. Keeping the placeholder text instead was tried and
		// is wrong in the other direction -- a fully generated name such as a scrubbed
		// `Host` then entered the collected names and the guard refused a capture with
		// `Host` supplied, which `TestProtocolTokensDoNotRefuseTheCapture` says
		// directly. Removal keeps the syntax and contributes no generated text.
		//
		// The value side is untouched: blanking there is what stops two unrelated
		// fragments reading as one token.
		spanLines := strings.Split(span, "\n")
		nameLines := strings.Split(stripMarks(genSpan.ReplaceAllString(rawSpan, "")), "\n")
		inHead := true
		first := true
		for lineNo, ln := range spanLines {
			bare := strings.TrimSuffix(strings.TrimSpace(stripMarks(ln)), "\r")
			if inHead && bare == "" {
				inHead = false
			}
			// Only an HTTP message's first line is protocol syntax. A body line
			// that resembles a status line remains captured data, and an individual
			// trailer span is a field rather than a complete HTTP message.
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
				// A generated span may in principle carry a newline, which would put the
				// two views out of step; the parallel line is used only when they agree.
				nameSrc := bare
				if len(nameLines) == len(spanLines) {
					nameSrc = strings.TrimSuffix(strings.TrimSpace(nameLines[lineNo]), "\r")
				}
				if i, ok := headerNameEnd(nameSrc); ok {
					// Extraction and decoding share one name splitter. The decoding chain
					// also splits these components; this earlier pass keeps nameForms[0]
					// complete for its callers at the cost of one header-block pass.
					names.WriteString(nameComponents(nameSrc[:i]))
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
	// through both the scrub and this gate.
	// Nesting has no fixed depth, so neither does the decoder: it iterates until
	// nothing changes, with a bound so a crafted body cannot spin it.
	// ⚠ THE BOUND IS THE STRING, NOT A MAGIC NUMBER. This capped at 16 rounds
	// while its own comment promised a fixed point, so wrapping a value in
	// seventeen `%25` layers walked straight through the gate. Each round either shrinks the text
	// -- `%XX` becomes one byte, `\uXXXX` at most four -- or rewrites `+` as a
	// space, which cannot be undone, so it cannot cycle and cannot run longer
	// than the input. len(text)+1 is therefore a real bound rather than a guess,
	// and reaching it is a defect worth failing on rather than passing quietly.
	// ⚠ AND THE BUDGET IS WORK, NOT DEPTH. `len(text)+1` bounds the rounds but
	// not the cost: a near-limit body whose escape is nested `%25` deep peels one
	// two-byte layer per pass while rescanning and re-allocating almost the whole
	// report, which is quadratic and lets a crafted response hang this program
	// instead of being refused by it. The
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
		// matched was never retained.
		// ⚠ EVERY STAGE IS CHARGED, NOT ONE PER ROUND. The budget counted
		// `len(cur)` once while FIVE full-text decoders ran, so a near-limit body
		// could do hundreds of MiB of scanning -- and retain that many full-size
		// intermediate forms -- before a nominal 64 MiB bound fired. A resource
		// limit that undercounts by the number of stages is not fail-closed.
		for _, stage := range supportedDecoders {
			// ⚠ THE NAMES DECODE TOO, IN LOCKSTEP. `capturedNames` was extracted
			// once from the RAW span, so a field name spelling a short supplied
			// value in an escape -- `X-%62ar` for `bar` -- decoded in the text but
			// never in the names, and the name-boundary rule that exists for
			// exactly that case never saw it.
			// ⚠ RE-SPLIT INTO THE STREAM, not beside it. Recording the split form and
			// decoding the unsplit one leaves the new component undecoded by every
			// later stage -- which is the defect, one indirection along: the form that
			// can reconstruct the value has to be the form the chain continues from.
			curNames = nameComponents(stage(curNames))
			nameForms = append(nameForms, curNames)
			// ⚠ THE EXTRA CANDIDATE GOES IN ITS OWN SLICE. base64 is MIME-WRAPPED at
			// column 76 and the scanner reads each line as its own token, so every
			// fragment decoded separately and no retained form held the value a
			// standard decoder reconstructs directly. It must NOT join `forms`: that slice's length is the
			// fixed-point comparison's arithmetic, and adding to it per stage made
			// the loop compare against a mid-round form and settle early -- three
			// existing decoder fixtures said so immediately.
			// ⚠ AND IT IS DECODED. Storing the normalised form alone checked the
			// wrapped SPELLING, which no supplied value ever equals -- the whole
			// point is that a MIME decoder reconstructs the value FROM it. Both forms are kept: the
			// normalisation, and what base64 makes of it.
			// ⚠ WITHIN THE RUN, NOT ACROSS THE SPAN. Joining every field of the
			// whole captured text removed the header/body separator too, so a
			// header value ending in base64 letters merged into the encoded body
			// and the combined token decoded to something else -- while the
			// per-line candidates still could not reconstruct the value. Only runs of CONSECUTIVE
			// lines that are entirely base64 alphabet are joined.
			// ⚠ AND BACK THROUGH EVERY DECODER. base64 can carry another supported
			// encoding -- base64 of `%61bcdefgh` -- and the decoded candidate was
			// checked as-is, so nothing ever percent-decoded it. A candidate is an input to the
			// chain, not an answer from it.
			// ⚠ TO A FIXED POINT, NOT ONE PASS. base64 can carry TWO layers of
			// another encoding -- `%2561bcdefgh` -- and a single sweep of the
			// decoders left `%61bcdefgh` undecoded, while the ordinary chain
			// cannot reach it because it never un-wraps the base64. The candidate joins the same
			// work budget, so a crafted body cannot spin it.
			// ⚠ EVERY PRODUCING SCAN IS CHARGED BEFORE IT RUNS. The block below scans
			// `cur`, `norm` and the name stream once per DECODER STAGE and per ROUND,
			// and none of those linear passes was added to `work`: the `len(cur)`
			// charge after it accounts for one pass and `takeDecodeWork` covers only
			// the suffix tails, so a crafted response could make post-processing
			// examine hundreds of MiB under an advertised 64 MiB ceiling. A budget that does not count the
			// work it is meant to bound is a number in a message.
			//
			// The pass count is per producer and stated at the call: `binaryCandidates`
			// tokenises twice, the rest walk their input once.
			charge := func(passes int, x string) string {
				work += passes * len(x)
				producerWork += passes * len(x)
				return x
			}
			norm := joinBase64Runs(charge(1, cur))
			dec := undoBase64(charge(1, norm))
			extra = append(extra, norm, dec)
			// ⚠ AND EVERY BINARY CANDIDATE RE-ENTERS THE CHAIN. These were appended
			// as-is and never decoded again, so `/yU2MWJjZGVmZ2g=` -- base64 of
			// `0xff%61bcdefgh` -- was retained in a form nothing percent-decoded,
			// while the ordinary chain cannot reach it because it never un-wraps
			// the base64. A candidate is an
			// input to the chain, not an answer from it -- which this file already
			// said about the base64 decode, and then did not do for the binary one
			// standing beside it. Same budget, so a crafted body cannot spin it.
			// ⚠ AND THE NAME FORMS. These looked only at the captured TEXT, so a field
			// name whose component decodes to invalid UTF-8 -- `X-_2Jhcg`, where
			// `_2Jhcg` is url-base64 for `0xffbar` -- was examined as one unsplit token
			// and no candidate ever held `bar`. The
			// names decode in lockstep with the text everywhere else; the binary path was
			// added later and inherited none of that.
			bins := append(binaryCandidates(charge(2, cur)), binaryCandidates(charge(2, norm))...)
			bins = append(bins, binaryCandidates(charge(2, curNames))...)
			extra = append(extra, bins...)
			// ⚠ AND THE SUFFIX DECODES, AS CANDIDATES IN THEIR OWN RIGHT. See
			// base64SuffixCandidates: spliced back behind their separator they are
			// unreachable to the short-value matcher. They are SEEDS like the rest --
			// a candidate is an input to the chain, not an answer from it.
			sufs := append(base64SuffixCandidates(charge(1, cur)), base64SuffixCandidates(charge(1, norm))...)
			sufs = append(sufs, base64SuffixCandidates(charge(1, curNames))...)
			// The normalized wrapped form is already covered here. Avoid producing
			// duplicate candidate views that consume the shared work budget.
			for _, view := range []string{cur, curNames} {
				sufs = append(sufs, hexCandidates(charge(1, view))...)
				sufs = append(sufs, shortBase64Candidates(charge(1, view))...)
				for _, w := range wrappedBase64Candidates(charge(1, view)) {
					sufs = append(sufs, w)
					if d, ok := decodeBase64(w); ok {
						sufs = append(sufs, d)
					}
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
			// A seed re-enters candidate production as well as all six decoders.
			// A decoded binary candidate can itself contain a wrapped encoded run,
			// which needs another producer pass before the decoders can read it.
			//
			// The worklist is bounded twice over -- by the shared work budget and by a
			// seed cap -- so a crafted body cannot spin it, and what the cap drops is
			// named rather than silently truncated.
			// ⚠ THE CAP IS APPLIED WHILE COLLECTING, NOT AFTER. `bins` and `sufs` can
			// already hold far more than the cap before the worklist exists -- a 900 KB
			// body of `61 ` repeated makes roughly 300,000 hex seeds and about 234 MB of
			// allocations -- and the loop then processed every initial entry and checked
			// the cap only afterwards. A limit
			// tested after the work it bounds is a number in a message, which is the
			// same sentence this file just applied to the decode budget.
			initial := append(append([]string{dec}, bins...), sufs...)
			if len(initial) > seedMax {
				return fmt.Errorf(
					"the candidate producers yielded %d seeds, past the cap of %d, so the "+
						"decoding chain did not settle; the record is NOT publishable and was "+
						"not printed", len(initial), seedMax)
			}
			seeds := initial
			// ⚠ THE CAP BOUNDS THE APPEND, NOT THE NEXT ITERATION. One seed can carry
			// thousands of short encoded tokens, so a single round appended tens of
			// thousands of entries and the loop then PROCESSED every one before the check
			// at the top refused -- the cap bounded neither memory nor CPU. A limit tested after the overshoot is
			// a report, not a limit.
			seedOverflow := false
			addSeed := func(v string) {
				if len(seeds) >= seedMax {
					seedOverflow = true
					return
				}
				seeds = append(seeds, v)
			}
			for si := 0; si < len(seeds) && work <= decodeWorkMax && !seedOverflow; si++ {
				d := seeds[si]
				// ⚠ NORMALISED PER FORM, NOT ONCE ON THE OUTER TEXT. `joinBase64Runs` ran on
				// `cur` only, and `wrappedBase64Candidates` skips a whole-base64 line because
				// it assumes that join already happened -- so a decoded seed carrying a MIME
				// fold reached neither: `WXpK\r\nV2FnMEtZMjFXTUU5VWF6MD0=` passed with
				// `secret99` supplied, though three ordinary decodes reconstruct it. An assumption about what ran before
				// travels only as far as the place that made it.
				if jd := joinBase64Runs(charge(1, d)); jd != d {
					addSeed(jd)
				}
				forms := []string{d}
				for round := 0; round <= len(d) && work <= decodeWorkMax; round++ {
					before := d
					for _, st := range supportedDecoders {
						work += len(d)
						d = st(d)
						work += takeDecodeWork()
						extra = append(extra, d)
						forms = append(forms, d)
					}
					if d == before {
						break
					}
				}
				// Apply every producer to each retained form. A decoding stage can erase
				// a representation that another producer needs: undoBase64 can rewrite
				// a binary seed before the hex producer reconstructs it. The guard already
				// retains these forms, so producers inspect each one.
				for _, f := range forms {
					for _, w := range wrappedBase64Candidates(charge(1, f)) {
						addSeed(w)
						if dd, ok := decodeBase64(w); ok {
							addSeed(dd)
						}
					}
					for _, v := range shortBase64Candidates(charge(1, f)) {
						addSeed(v)
					}
					for _, v := range hexCandidates(charge(1, f)) {
						addSeed(v)
					}
					for _, v := range base64SuffixCandidates(charge(1, f)) {
						addSeed(v)
					}
					for _, v := range binaryCandidates(charge(2, f)) {
						addSeed(v)
					}
					if seedOverflow {
						break
					}
				}

				work += takeDecodeWork()
			}
			// ⚠ THE NUMBER REACHED, NOT THE CONSTANT. Printing `seedMax` says what the
			// limit is and nothing about what happened, so a worklist that overshot it
			// five times over read identically to one that touched it -- and the mutant
			// that removes the cap from the collector survived, because nothing this
			// program prints could tell the two apart. A cap that cannot report the overshoot is a cap only in the
			// comment.
			if seedOverflow || len(seeds) >= seedMax {
				return fmt.Errorf(
					"the candidate worklist reached %d seeds against a cap of %d, so the "+
						"decoding chain did not settle; the record is NOT publishable and was "+
						"not printed", len(seeds), seedMax)
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
	// ⚠ JOINED ONCE, NOT PER CANDIDATE PER VALUE. This rebuilt the whole name-forms
	// string inside two nested loops and outside the decode-work accounting: measured,
	// a 100 KB field name with ten short base64 tokens and the six supplied values
	// cost about 31 GiB of cumulative allocations and 4.7 seconds AFTER the network
	// deadline, under a budget advertised as 64 MiB. The value does not change inside the loops, so building it there was
	// never anything but cost.
	joinedNameForms := strings.Join(nameForms, "\n")
	for _, v := range suppliedValues {
		// ⚠ THE SAME EXEMPTION AS THE SCRUB. `jsonLiterals` was consulted by
		// `scrubSuppliedRaw` alone, so a key of `false` stopped corrupting the
		// body and went on refusing every capture instead -- the fix moved the
		// defect rather than removing it. An
		// exemption honoured by one of two rules is a disagreement, not an
		// exemption.
		if v == "" {
			continue
		}
		for _, f := range append(append([]string{}, forms...), extra...) {
			if containsValue(f, joinedNameForms, v) {
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
// value was never reconstructed and both the scrub and the gate missed it.
// undoEntities decodes HTML entity spellings. A gateway or validation endpoint
// answering with an HTML diagnostic writes `a&amp;b` for `a&b`, which no
// percent, unicode or plus decoding reconstructs -- so the value reached the
// artifact while the guard reported it absent.
// undoBase64 decodes bounded base64 tokens. A diagnostic body that
// base64-encodes a supplied identifier reconstructs it for anyone who reads the
// artifact, while percent, unicode, plus and entity decoding all leave it
// untouched and the gate reported it absent.
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
// The destructive rewrite floor is not a minimum legal value length.
// `undoBase64` rewrites in place, so its
// four-byte floor bounds the garbage a destructive pass may produce; but
// `decodeBase64` accepts `RawStdEncoding`, a one-character key travels as `YQ`
// and a two-character one as `YWI`, and no binary or suffix path took tokens
// below that floor either. Nothing is
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
		// ⚠ ONE DECODE, TWO QUESTIONS. The text answer retained only a VALID-UTF-8
		// decode and the binary producer's floor is four bytes, so `/2E` -- which
		// raw-decodes to 0xff 0x61 -- fell BETWEEN the two producers and a
		// one-character supplied value was approved even though the configured decoder
		// reconstructs it directly.
		//
		// Asked here rather than by lowering the binary floor: these tokens are
		// already walked, so nothing new is enumerated. Both alternatives were
		// MEASURED against the seed-cap scene, which bounds this collection in
		// allocations: against a 56 MiB baseline, lowering the floor cost 295 MiB
		// (every long token then yielding suffixes), asking a SECOND decode after the
		// first cost 156 MiB, and asking one decode two questions costs what the
		// single question cost.
		text, haveText, bin, haveBin := base64Answers(tok)
		switch {
		case haveText:
			out = capSeeds(out, text)
		case haveBin:
			out = capSeeds(out, bin)
		}
	}
	return out
}

func undoBase64(text string) string {
	// ⚠ FOUR, NOT EIGHT. A three-character key is legal and `bar` travels as the
	// four-byte `YmFy`, which an eight-byte floor skipped entirely -- the guard
	// settled and approved a reconstructable identifier. Four is the smallest token that
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
		// reconstructs directly -- was never tried. The binary-candidate path needed the
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
// all, and nothing in the chain touched it.
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
// directly.
//
// These are CANDIDATES, not a rewrite: nothing is replaced, so the floor that
// protects the rewrite is left where it is and the short forms are covered anyway.
// seedMax bounds the candidate SET handed to the decoding chain.
//
// Apply the candidate cap while producing candidates, before appending
// them. Checking only the assembled slice would allow allocation and
// processing to exceed the advertised limit.
//
// Truncating a producer cannot weaken the leak scan, and that is by construction
// rather than by care: any producer that reaches the cap makes the assembled set
// exceed it, so the run refuses and `extra` is never read.
const seedMax = 4096

// capSeeds appends while there is room under the cap, so nothing past it is ever
// materialised.
func capSeeds(out []string, v ...string) []string {
	for _, s := range v {
		if len(out) >= seedMax {
			return out
		}
		out = append(out, s)
	}
	return out
}

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
			out = capSeeds(out, string(raw))
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

// base64Encodings is the alphabet this program accepts, written once: a second
// spelling of a decoder is a second grammar, and the two would answer differently
// the day one of them is widened.
var base64Encodings = []*base64.Encoding{
	base64.StdEncoding, base64.RawStdEncoding,
	base64.URLEncoding, base64.RawURLEncoding,
}

// base64Decodes is every raw byte string those encodings read out of one token.
func base64Decodes(tok string) [][]byte {
	var out [][]byte
	for _, enc := range base64Encodings {
		if raw, err := enc.DecodeString(tok); err == nil {
			out = append(out, raw)
		}
	}
	return out
}

// base64Answers walks that alphabet ONCE and keeps the first decode of each kind.
//
// Avoid materializing every decoded byte slice for every token. Walk each
// alphabet until the required text and binary answers have been found.
//
// Nothing downstream needs the later decodes, so nothing holds them, and the walk
// stops as soon as both questions are answered.
func base64Answers(tok string) (text string, haveText bool, bin string, haveBin bool) {
	for _, enc := range base64Encodings {
		raw, err := enc.DecodeString(tok)
		if err != nil {
			continue
		}
		if utf8.Valid(raw) {
			if !haveText {
				text, haveText = string(raw), true
			}
		} else if !haveBin {
			bin, haveBin = string(raw), true
		}
		if haveText && haveBin {
			break
		}
	}
	return text, haveText, bin, haveBin
}

var binaryDecoders = []struct {
	isByte func(byte) bool
	pad    byte
	minLen int
	decode func(string) [][]byte
}{
	{isBase64Byte, '=', 4, base64Decodes},
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
			// never tried. A maximal tokeniser
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
							out = capSeeds(out, string(raw))
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
// decoder applied to the substring ignores the CRLF and reconstructs it directly. MIME wrapping is about where a line BREAK
// falls, not about what else is on the line.
func wrappedBase64Candidates(text string) []string {
	// ⚠ AND A LONE CR IS A FOLD TOO. `encoding/base64` ignores CR and LF alike, so
	// `YWJj\rZGVmZ2g=` reconstructs the value -- while splitting only on LF left the
	// CR INSIDE one line, `allBase64` rejected the run, and the ordinary token
	// decoder saw two independent fragments that reconstruct nothing, so the guard
	// approved a spelling a standard decoder reads straight through. All three spellings of a fold are one
	// separator here, normalised before the split rather than trimmed after it.
	norm := strings.ReplaceAll(text, "\r\n", "\n")
	norm = strings.ReplaceAll(norm, "\r", "\n")
	lines := strings.Split(norm, "\n")
	for i, ln := range lines {
		// ⚠ NORMALISED THE SAME WAY THE WHOLE-LINE PRODUCER NORMALISES. Judging
		// unnormalised lines here made a space anywhere in the run terminate it:
		// `prefix: YWJj \r\nZGVmZ2g=` produced no candidate at all, while a standard
		// decoder reconstructs the identifier from that substring directly, and the
		// guard therefore approved a spelling it should have refused. Measured: three of the four places a
		// space can sit in such a run broke it -- after the head, before the
		// continuation, and inside the final fragment.
		lines[i] = dropHorizontalWhitespace(strings.TrimSuffix(ln, "\r"))
	}
	suffix := func(s string) string {
		i := len(s)
		for i > 0 && isBase64Byte(s[i-1]) {
			i--
		}
		return s[i:]
	}
	prefix := func(s string) string {
		// ⚠ PADDING ENDS THE ENCODING, SO THE SCAN ENDS WITH IT. Treating `=` as one
		// more admissible byte let the scan run PAST the padding into the prose behind
		// it -- and dropping horizontal whitespace, which is right, removed the space
		// that used to stop it: `JDdwQA== end!` became `JDdwQA==end!` and yielded
		// `JDdwQA==end`, which no decoder accepts. Data bytes, then padding, then stop.
		i := 0
		for i < len(s) && isBase64Byte(s[i]) {
			i++
		}
		for i < len(s) && s[i] == '=' {
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
			//
			// Skip duplicate candidates here. Wrapped-token coverage depends on
			// the padding rule below; adding duplicates only consumes work budget.
			continue
		}
		// ⚠ A BUILDER, NOT `+=`. Each `+=` copies the whole prefix accumulated so far,
		// so assembling a candidate over many short base64 lines is quadratic -- and it
		// happens BEFORE the decode budget charges anything, which is the same gap the
		// suffix probes had.
		var jb strings.Builder
		jb.WriteString(head)
		ended := ""
		for j := i + 1; j < len(lines); j++ {
			// A blank line remains whitespace within a base64 run, including one that
			// shares its first line with a prefix. Standard decoding ignores CR and LF
			// and can reconstruct an identifier across those lines.
			if strings.TrimLeft(lines[j], " \t") == "" {
				// Treat spaces-and-tabs-only lines as blank, matching joinBase64Runs.
				// Such whitespace does not terminate a MIME base64 run.
				continue
			}
			if allBase64(lines[j]) {
				decodeWork += len(lines[j])
				if decodeWork > decodeWorkMax {
					ended = "budget"
					break
				}
				jb.WriteString(lines[j])
				// Padding ends the encoding here too: whatever follows is prose, and a
				// candidate carrying it decodes to nothing.
				if strings.HasSuffix(lines[j], "=") {
					out = capSeeds(out, jb.String())
					ended = "padding"
					break
				}
				continue
			}
			if p := prefix(lines[j]); p != "" {
				out = capSeeds(out, jb.String()+p)
			}
			ended = "text"
			break
		}
		// ⚠ AND A RUN THAT ENDS WITH THE TEXT IS STILL A RUN. Emitting only where a
		// following line TERMINATED the run was survivable while lines were judged
		// unnormalised; once whitespace is dropped, a final fragment like `ZGVm Z2g=`
		// becomes a whole-base64 line, the run reaches the end of the text, and the
		// candidate was never emitted -- `TestWrappedCandidateAssemblyIsLinear` says
		// so directly. The two halves are one fix. A run cut short by the WORK BUDGET is still not emitted: a
		// truncated candidate is a spelling nothing decodes.
		if ended == "" && jb.Len() > len(head) {
			out = capSeeds(out, jb.String())
		}
	}
	return out
}

// dropHorizontalWhitespace removes the bytes a MIME decoder ignores inside an
// encoded line.
//
// ⚠ ONE COPY, BECAUSE TWO PRODUCERS ANSWERING THE SAME QUESTION DRIFT. This file
// has now been told four times that MIME ignores whitespace a line-based reading
// treats as structure, and the fourth was the SHARED-LINE producer still judging
// unnormalised lines while the whole-line producer normalised. The sentence was right each time; it was
// applied at one site each time.
func dropHorizontalWhitespace(s string) string {
	return strings.Map(func(r rune) rune {
		if r == ' ' || r == '\t' {
			return -1
		}
		return r
	}, s)
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
		// directly. Horizontal whitespace is
		// dropped before the line is judged and before it is joined.
		bare = dropHorizontalWhitespace(bare)
		// Continue across blank lines within a base64 run. Standard decoding
		// ignores CR and LF, so splitting at an empty line can hide a value that
		// the complete run reconstructs.
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
	text, haveText, _, _ := base64Answers(tok)
	return text, haveText
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
		// reconstructed.
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
				// decoder and to the guard behind it.
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
		// accepting only exactly four hex digits never sees it.
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
			// settled.
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
			// value while a reader applying the same two decoders reconstructs it. Listing the escapes that are
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
			case 'a':
				// ⚠ GO'S OWN QUOTING ALPHABET TOO, not only JSON's. `encodingsOf`
				// explicitly produces `strconv.Quote` spellings, and that quoting
				// writes `\a` and `\v` where JSON does not -- so a supplied value
				// containing a bell or a vertical tab had a spelling no retained form
				// reconstructed. A decoder that
				// covers one of two alphabets its own producer emits is covering half
				// of what it was written for.
				b.WriteByte(7)
				i++
				continue
			case 'v':
				b.WriteByte(11)
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
// it through. The rule is not loosened for
// values; the name is split on its own separator first.
// nameSafe is the placeholder used inside a header NAME. `<redacted, 6 chars>`
// carries spaces, a comma and angle brackets, none of which are legal in an HTTP
// field name -- so the scrub that exists to keep a name publishable made the
// message unparsable in exactly the case it handles. This is letters, digits and hyphens.
func nameSafe(v string) string {
	return tokenPlaceholder(v)
}

func scrubHeaderName(name string) string {
	// ⚠ SPLITTING FIRST CANNOT MATCH A HYPHENATED VALUE. The previous version cut
	// the name on `-` and scrubbed each piece, so a legal key `foo-bar` matched no
	// component and `X-foo-bar` was published whole. The value is matched against the
	// COMPLETE name under a boundary rule where `-` separates -- which is what
	// the split was reaching for and could not express.
	for _, v := range longestFirst(suppliedValues) {
		if v != "" {
			// ⚠ FOLDED, BECAUSE A FIELD NAME IS CASE-INSENSITIVE AND net/http
			// CANONICALISES IT. A wire header `X-secret` reaches this function as
			// `X-Secret`, so a case-sensitive search missed the supplied `secret`
			// and published it in the name.
			// Folding is applied HERE and not to values, whose case is data.
			name = replaceTokenFold(name, v, nameSafe(v), isNameByte)
		}
	}
	return name
}

// isNameByte treats only letters and digits as word bytes in a field name.
// Token punctuation, including hyphens and underscores, separates words.
func isNameByte(c byte) bool {
	// ⚠ `_` SEPARATES HERE TOO. The comment above says every token punctuation
	// separates words inside a NAME and then listed `_` as a word byte, so a legal
	// `X_bar: v` kept a supplied `bar` -- and the guard applies the same rule to
	// collected names, so it approved the capture as well. A sentence that states the rule and an
	// expression that contradicts it is the expression's defect, not the sentence's.
	return isTokenByte(c) && ((c >= '0' && c <= '9') ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))
}

// replaceValue redacts every occurrence of v. A long value is replaced outright;
// a SHORT one only where it stands as a whole token, because the SDK validates
// these fields as non-empty and nothing more -- an experiment key may legally be
// `ab`, and the first version skipped anything under four characters, printing
// exactly those verbatim in the response it calls publishable. Blind substring replacement of `ab`
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
// stricter than the redaction it checks does not find leaks; it finds itself.
func containsValue(text, names, v string) bool {
	if v == "" {
		return false
	}
	// ⚠ CHARACTERS, THE SAME THRESHOLD THE SCRUB USES. Moving the scrub's threshold
	// to characters left this one counting bytes, so a four-character non-ASCII
	// value was SHORT to the scrub and LONG to the guard: the scrub correctly left
	// `αééééβ` alone and the guard then refused the capture for containing it. One notion, two thresholds, and fixing
	// one of them is what made the disagreement reachable.
	if chars(v) >= 8 {
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
	// both hyphens as boundaries and the capture exited 4. The permissive rule exists for field
	// NAMES, where `-` is structural, so it is asked of the field names.
	// ⚠ FOLDED, LIKE THE SCRUB THAT PRODUCED THESE NAMES. `X-%53ecret` decodes to
	// `X-Secret`, and this test was case-sensitive while `scrubHeaderName` folds
	// -- so an encoded byte became a CASE variant only after decoding and slipped
	// between the two rules. Two rules about
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
// altered endpoint evidence.
//
// The naive repair -- "treat every byte >= 0x80 as a word byte" -- moves the
// error the DANGEROUS way: non-ASCII punctuation would stop being a boundary,
// fewer matches would be found, and a value that should be scrubbed would
// survive. So the rune is decoded and classified: letters and digits are word
// characters, everything else is a boundary, and ASCII keeps exactly the
// behaviour its own predicate gives it, hyphen rules included.
//
// A combining mark continues its word. Share the boundary classification
// between matching and tokenization so replacing a base letter cannot
// leave its accent behind as if it were unrelated punctuation.
func isWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsMark(r)
}

func wordBefore(text string, i int, isWord func(byte) bool) bool {
	if i <= 0 {
		return false
	}
	r, _ := utf8.DecodeLastRuneInString(text[:i])
	if r < utf8.RuneSelf {
		return isWord(byte(r))
	}
	return isWordRune(r)
}

func wordAt(text string, i int, isWord func(byte) bool) bool {
	if i >= len(text) {
		return false
	}
	r, _ := utf8.DecodeRuneInString(text[i:])
	if r < utf8.RuneSelf {
		return isWord(byte(r))
	}
	return isWordRune(r)
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
	// placeholder had, in the other function.
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
		// deliberately leaving the line alone. Version and code go; whatever follows them stays.
		f := strings.SplitN(bare, " ", 3)
		if len(f) == 3 {
			// ⚠ ON HTTP/2 THE PHRASE IS SYNTHESISED. The protocol carries only
			// `:status`; Go builds `resp.Status` as "200 OK" and DumpResponse
			// writes it, so returning it as captured data refused every HTTP/2
			// response whose key happened to be a standard phrase. An HTTP/1 reason phrase is
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
	// exited 4 on every run. Only the
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
	// ⚠ CHARACTERS, LIKE EVERY OTHER PLACE THAT SAYS "SHORT". `len` is bytes, so a
	// four-character non-ASCII key such as `éééé` is eight bytes and took the
	// unconditional branch -- rewriting ordinary endpoint text `αééééβ` into
	// `α<redacted, 4 chars>β`, evidence the guard then approved because the
	// placeholder is generated. The
	// placeholder counts characters and so does the notion of a short identifier;
	// only this threshold counted bytes.
	if chars(v) >= 8 {
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
	// ...and BY NAME as well, because one question needs the mapping the flat list
	// throws away: whether an echoed member carries the value THIS request put in
	// that slot. See the echo check in redactUnaccountedJSONValues.
	requestedAppKey = env("SP_APP_ID")
	requestedEnvKey = env("SP_ENVIRONMENT_ID")
	requestedExpKey = env("SP_EXPERIMENT_KEY")

	// The authority this program was pointed at, recorded before anything is sent:
	// the `Host:` line carries it, and it is not endpoint text. See configuredHost.
	if u, uerr := url.Parse(env("SP_REMOTE_CONFIG_URL")); uerr == nil {
		configuredHost = u.Host
	}
	if r, rerr := http.NewRequest("GET", env("SP_REMOTE_CONFIG_URL"), nil); rerr == nil {
		if d, derr := httputil.DumpRequestOut(r, false); derr == nil {
			for _, l := range strings.Split(string(d), "\r\n") {
				if strings.HasPrefix(strings.ToLower(l), "host:") {
					configuredHostWire = strings.TrimSpace(l[len("host:"):])
				}
			}
		}
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

	// ⚠ STOP THE TRAFFIC BEFORE COUNTING IT. An armed exposure (a fetch armed one
	// before ApplyExperimentVariant existed) can have
	// its worker issue an ingest request AFTER the fetch returns, so the snapshot
	// below recorded zero while the recorder went on absorbing and counting that
	// request as the report was assembled -- and the printed claim, already
	// copied, said zero. The deferred Close
	// cannot settle it either: every path below calls `os.Exit`, which runs no
	// defers at all. A count that is a claim about the RUN cannot be taken at an
	// instant in the middle of it.
	// ⚠ AND THE CLOSE HAS TO SUCCEED, NOT MERELY BE CALLED. Discarding its error
	// left the same race one step further along: when the five-second context
	// expires, `Close` returns before the worker lanes are done, so background
	// requests still reach the recorder after the snapshot and the report prints
	// an off-route count that was already stale, or pairs the copied exchanges
	// with a later verdict. A capture whose
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
	// Report the off-route request count. This example supplies no AnonymousID,
	// so buildExperimentFactEvent drops an armed exposure with
	// exposure_no_anonymous_id before it reaches the transport. The expected
	// off-route count is zero for every verdict.
	offRouteExpected := "zero on every verdict: this harness sets no `AnonymousID`, " +
		"so an armed exposure is skipped before it reaches the transport"
	offRouteAgrees := "matches"
	if offRoute != 0 {
		offRouteAgrees = "does NOT match"
	}
	fmt.Fprintf(&report, "Requests seen on other routes and NOT recorded: **%d**. "+
		"The ingest leg shares this transport; for this verdict the expected answer is "+
		"%s, and the count %s it. It is printed rather than assumed.\n\n",
		offRoute, offRouteExpected, offRouteAgrees)
	// ⚠ A REDIRECT LEG IS NOT AN ATTEMPT. Every recorded exchange used to be
	// counted as one, so a single assignment the endpoint redirected once was
	// reported as two attempts by the SDK -- a claim about the SDK's behaviour made
	// out of the endpoint's. Both are printed;
	// only the count is separated, because only the count is a claim.
	attempts, legs := 0, 0
	for i := range exchanges {
		if exchanges[i].redirectLeg {
			legs++
		} else {
			attempts++
		}
	}
	if legs > 0 {
		fmt.Fprintf(&report, "The endpoint redirected **%d time(s)**; those legs were "+
			"followed and recorded, not answered here.\n\n", legs)
	}
	if len(exchanges) > 1 {
		fmt.Fprintf(&report, "The SDK made **%d attempts** over %d exchange(s). All are "+
			"below; the verdict is the last, because that is the one it acted on.\n\n",
			attempts, len(exchanges))
	}
	// ⚠ REFUSALS ARE ATTRIBUTED TO THE EXCHANGE THAT RAISED THEM. The ledger is
	// global and the truncation suppression asked only about the LAST attempt, so a
	// COMPLETE earlier attempt carrying an undescribed body had its refusal
	// suppressed by a later truncated retry -- and the guard cannot see a value the
	// harness never supplied, so the unsafe earlier response was published. "This capture is incomplete" excuses the
	// shapes THAT attempt produced and nothing else.
	perExchange := renderExchanges(&report, exchanges)

	last := rec.last()
	fmt.Fprintf(&report, "## SDK verdict\n\n")
	fmt.Fprintf(&report, "    attempts: %d\n", attempts)
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
	// misstated the assignment the SDK served.
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
	// not read them. "Wherever it appears" is
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
	// Refuse a surface that structural rules cannot describe. For a known
	// truncated body, use exit 3 rather than the structural-refusal exit 4:
	// incompleteness is the reason it cannot be parsed. Keep the assembled
	// request and partial-response report so that classification does not
	// discard the evidence.
	//
	// The refusal is skipped because a fragment is undescribable BECAUSE it is
	// incomplete, which is the truncation, not a shape the rules failed on. The
	// report is written and the classification below returns exit 3.
	unexcused := unexcusedRefusals(refusalLedger(), perExchange)
	if len(unexcused) > 0 {
		fmt.Fprintf(os.Stderr,
			"REFUSING TO PRINT: the response carries %d server-generated surface(s) "+
				"in a shape the structural rules do not describe, so the capture is "+
				"NOT publishable:\n", len(unexcused))
		for _, w := range unexcused {
			fmt.Fprintf(os.Stderr, "  - %s\n", w)
		}
		os.Exit(4)
	}
	// A CAPTURE NOBODY RECEIVED IS NOT A CAPTURE. An ignored write error let a
	// report truncated by a full filesystem -- or never written at all -- be
	// followed by "SERVED" and exit 0.
	// ⚠ A STREAM CANNOT PROMISE ALL-OR-NOTHING, so the claim says what is true.
	// `io.WriteString` may return a positive count WITH an error, so a prefix of
	// the report can already be on the pipe before this refusal runs -- and the
	// documented exit-4 clause said a report that could not be written whole was
	// not published. It cannot be unwritten.
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
		// SERVED.
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
