package main

// Structural redaction applies field-specific rules to endpoint-generated
// values that are absent from the supplied-value list, such as cookie values,
// redirect targets and minted subject keys. It preserves parseable message
// structure while replacing values according to the recording contract.
// Classify the original field name before scrubbing it; the name selects
// the structural rule.

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// jsonMemberValue matches a JSON member together with its string VALUE. The
// guard half already carries `jsonMember`, which matches a name and its colon;
// this needs the value too, in order to replace it.
// Structural rules redact recognized shapes and refuse forms they cannot
// describe, such as a malformed cookie, an unsupported minted-value shape
// or an invalid redirect URI. A successful redaction is accounted for
// separately from a refusal.

var jsonMemberValue = regexp.MustCompile(
	`"((?:[^"\\]|\\.)*)"(\s*:\s*)"((?:[^"\\]|\\.)*)"`)

// redactResponseQuery redacts a target chosen by the endpoint. The outgoing
// request's query-name registry does not establish the provenance of names
// in a response Location; those names remain endpoint-controlled input.
func redactResponseQuery(line string) string { return redactQueryWith(line, false) }

func redactQuery(line string) string { return redactQueryWith(line, true) }

func redactQueryWith(line string, oursAuthored bool) string {
	i := strings.IndexByte(line, '?')
	if i < 0 {
		return line
	}
	head, rest := line[:i+1], line[i+1:]
	tail := ""
	if j := strings.IndexByte(rest, ' '); j >= 0 {
		rest, tail = rest[:j], rest[j:]
	}
	return head + redactPairs(rest, "", oursAuthored) + tail
}

// redactPairs redacts a form-encoded component list: names kept, values replaced
// by their length. `opaqueNameBytes` names bytes that CANNOT appear in a name in
// this context -- a component carrying one is not a name/value pair at all, so it
// is replaced whole.
// requestNames are the parameter names the HARNESS put on the wire.
//
// ⚠ THE NAME SIDE IS SPLIT BY PROVENANCE, NOT COVERED OR ABANDONED. An
// identifier can sit in a parameter NAME as easily as in its value -- the design
// said "names kept, values lengthened", and `?server-secret=x` published it. Lengthening every name closes that and
// produces `/cb?redacted-5-chars=redacted-6-chars`, an artifact nobody can read
// -- and a rule nobody can work with is relaxed by whoever next needs to read a
// capture. A strict version that gets switched off protects less than a workable
// one that runs.
//
// So the SAME provenance test the value side already uses is applied to names: a
// name this program itself sent is one it can vouch for and is printed; a name
// that came back and is not in that set was chosen by the endpoint and is
// lengthened.
//
//	/cb?state=redacted-6-chars     state was sent by us -- readable
//	/cb?redacted-6-chars=…         the endpoint invented this name
//
// ⚠ THE REDIRECT HOST IS DELIBERATELY NOT COVERED. It is structurally
// constrained, publicly resolvable, and the first thing a reader looks for;
// hiding it hides the subject of the capture. That is a decision, recorded here
// so it is not mistaken for an oversight.
var requestNames = map[string]bool{}

func noteRequestName(n string) { requestNames[ows(n)] = true }

func nameIsOurs(n string) bool { return requestNames[ows(n)] }

// nameIsOursExactly is nameIsOurs WITHOUT the HTTP-whitespace trim, for positions
// where a space is data rather than layout.
//
// ⚠ OWS IS A HEADER RULE, AND A QUERY NAME IS NOT A HEADER. `%20experiment_key%20`
// decodes to ` experiment_key `, which `ows` trimmed to the harness-owned name --
// so the ENTIRE endpoint spelling was marked generated, and with a supplied
// experiment key of `experiment_key` both the scrub and the guard skipped it and
// published a reconstructable identifier.
// Borrowing a normalisation from the wrong grammar is how a vouching rule vouches
// for something nobody sent.
func nameIsOursExactly(n string) bool { return requestNames[n] }

// syntax marks a delimiter THIS PROGRAM emitted while reconstructing a redacted
// line, so the supplied-value scrub cannot replace it.
//
// ⚠ A SUPPLIED IDENTIFIER MAY LEGALLY BE A DELIMITER. An experiment key of `;`,
// `?`, `&` or `..` is a string like any other, and the redactors deliberately keep
// those bytes — then handed them to the generic scrub, which turned them into
// prose placeholders the guard approves: JSON that no longer parses, a cookie
// whose attribute is no longer separated, a Location no longer parent-relative. Same rule as every admitted token, arriving
// on characters: what this program emits as STRUCTURE must be marked as structure.
func syntax(s string) string { return marked(s) }

// ⚠ `nameProvenance` SAYS WHETHER THIS PROGRAM AUTHORED THESE NAMES. The registry
// records the names of the OUTGOING request's query, and reusing it for a
// fragment the ENDPOINT chose let `Location: /cb#experiment_key=x` mark that
// member name as harness-authored -- so with an experiment key of
// `experiment_key` the scrub and the guard both skipped it and it was published. A name we sent in a query proves nothing
// about a name in someone else's fragment.
//
// The outgoing request's parameter-name registry does not authorize
// names chosen by the endpoint in a fragment.
func redactPairs(rest, opaqueNameBytes string, oursAuthored bool) string {
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
			// branch let it through untouched. Replaced whole, as redactFragment already does for the
			// same shape.
			if p != "" {
				// MEASURED DECODED, like every other branch here: `%C3%A9` is one
				// character, and reporting six put two lengths for one value in
				// one capture.
				parts[k] = tokenPlaceholder(queryDecoded(p))
			}
			continue
		}
		// URL-SAFE, no spaces. A request line is space-delimited, so the readable
		// `<redacted, N chars>` form turned the recorded request into something no
		// HTTP parser accepts -- and being parseable is the reason this artifact is
		// kept.
		// ⚠ MEASURE THE VALUE, NOT ITS WIRE SPELLING. A percent-encoded parameter
		// is longer than the identifier it carries -- `a"b` travels as `a%22b` --
		// so measuring the raw segment printed `redacted-5-chars` for a
		// three-character key, while the same key in an echoed body printed
		// `<redacted, 3 chars>`. Two lengths for one value in one capture is
		// evidence that contradicts itself.
		// COUNT CHARACTERS OF THE DECODED VALUE: the placeholder says "chars",
		// and `len` says bytes -- `%C3%A9` decoded to one character reported as
		// two.
		name := p[:eq]
		// ⚠ COMPARED DECODED. `req.URL.Query()` records the DECODED name, while the
		// dump carries its percent spelling, so a harness-owned non-ASCII
		// attribute was classified as endpoint-chosen and its structure lost. The lookup decodes; the OUTPUT
		// keeps the wire spelling, since that is what was on the wire.
		// ⚠ A NAME WE VOUCH FOR MUST BE MARKED AS SUCH. Preserving it as captured
		// text left it to the supplied-value pass, which rewrote it to the prose
		// placeholder -- so an experiment key equal to an SDK query name made the
		// URI invalid, and the guard refused every capture besides. Vouching for a name and then
		// letting another rule redact it is not vouching.
		// ⚠ AND THE SPELLING THAT ARRIVED MUST BE THE ONE WE SEND. The lookup DECODES,
		// so `experiment%5Fkey` denotes a name this program owns -- and marking the
		// raw span vouched an escape spelling this program never writes, so a supplied
		// `5F` was skipped by both the scrub and the guard. Knowing what a spelling means is not
		// knowing that we wrote it; the same rule the JSON member names carry.
		// ⚠ COMPARED WITH THE SPELLING THIS PROGRAM WOULD HAVE PRODUCED, not with the
		// decoded name. `%C3%A9` is the REQUIRED encoding of `é` in a name the harness
		// sent, and refusing it would classify our own parameter as endpoint-chosen --
		// the scene for that exists. `%5F` is an escape of `_`, which needs none, and
		// vouching it published a supplied `5F`.
		// The question is not "is any escaping present" but "is this OUR escaping".
		dn := queryDecoded(name)
		switch {
		case oursAuthored && nameIsOursExactly(dn) && name == url.QueryEscape(dn):
			name = marked(name)
		case oursAuthored && nameIsOursExactly(dn):
			// ⚠ OURS BY WHAT IT DENOTES, SO PRINTED IN OUR SPELLING. `experiment%5Fkey`
			// decodes to a name this program owns: vouching the raw span published a
			// supplied `5F`, and calling it endpoint-chosen and lengthening it loses a
			// name we sent.
			//
			// Leaving the encoded component captured is insufficient when its
			// boundary prevents both the scrub and guard from matching the value.
			//
			// What is printed is the spelling this program WOULD have sent, marked as
			// generated, which is exactly what it then is. The artifact says which
			// parameter it was; it no longer says which escape the endpoint chose, and
			// that spelling is the one thing here nobody can account for.
			name = marked(url.QueryEscape(dn))
		default:
			// ⚠ MARKED, NOT STRIPPED. Stripping the provenance marks to keep the
			// name looking like a name let the supplied-value scrub reach INSIDE
			// the placeholder it had just generated -- `<redacted, 8 chars>-5-chars`
			// (caught by the fixture that exists for exactly that). A placeholder
			// is generated text wherever it sits.
			name = tokenPlaceholder(queryDecoded(name))
		}
		parts[k] = name + syntax("=") + tokenPlaceholder(queryDecoded(p[eq+1:]))
	}
	// The separator is syntax like the `=` above; the request dump is not scrubbed
	// afterwards, so an unmarked `&` was reported by the guard as a surviving
	// supplied value and every such run exited 4.
	return strings.Join(parts, syntax("&"))
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
		return head + tokenPlaceholder(queryDecoded(frag)) + tail
	}
	// ⚠ `?` IS ORDINARY FRAGMENT DATA, NOT A QUERY INTRODUCER. This delegated to
	// redactQuery, which cut at the first `?` and kept everything before it as a
	// parameter NAME -- so `#server-secret?x=y` published `server-secret`
	// verbatim while reporting the fragment redacted. A fragment component whose name side
	// carries a `?` is not a name/value pair; it is opaque.
	return head + redactPairs(frag, "?", false) + tail
}

// redactTarget redacts a header line carrying a URL.
//
// ⚠ THE FRAGMENT IS CUT BEFORE ANY QUERY IS INTERPRETED. Composed the other way
// round -- redactFragment(redactQuery(line)) -- redactQuery saw the `?` INSIDE
// the fragment, split there, and by the time redactFragment ran the fragment
// contained a generated `x=` component and no longer looked opaque. A `#` always ends the query, so cutting
// there first is what the grammar says.
// parsesAsURI reports whether a redirect target is a URI Go itself would accept,
// which is the only thing that makes the host exemption true.
// isSchemeName reports a URI scheme: a letter followed by letters, digits, `+`,
// `-` or `.`, and nothing else -- which a path segment cannot be.
func isSchemeName(v string) bool {
	if v == "" || !((v[0] >= 'a' && v[0] <= 'z') || (v[0] >= 'A' && v[0] <= 'Z')) {
		return false
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.'
		if !ok {
			return false
		}
	}
	return true
}

func parsesAsURI(target string) bool {
	u, err := url.Parse(ows(target))
	if err != nil {
		return false
	}
	if u.Host == "" {
		return true // a relative target has no authority to exempt
	}
	// ⚠ AND A BRACKETED IPv6 AUTHORITY IS ORDINARY. `[2001:db8::1]` has a Host
	// that differs from its Hostname and no Port, so the first version of this
	// predicate called every such redirect malformed and refused the capture. A test written from two accessors
	// rather than from the grammar rejects what the grammar allows.
	// ⚠ THE GRAMMAR, NOT THE ACCESSORS. Every version of this predicate so far has
	// been assembled from what `net/url` exposes, and each time the accessors
	// agreed for the common case and disagreed somewhere real: they rejected a
	// bracketed IPv6 authority, then accepted `[server-secret]` merely because it
	// closed its bracket, then rejected the legal empty port
	// `https://example.com:/cb`. A bracketed host is an IP literal or it is not a host; an empty
	// port is explicitly permitted; anything else is a registered name.
	host := u.Host
	if p := u.Port(); p != "" {
		host = strings.TrimSuffix(host, ":"+p)
	}
	// An unbracketed host with an empty port follows the registered-name
	// case below. The bracket check also states the IP-literal grammar
	// explicitly, even where url.Parse already rejects an invalid literal.
	if strings.HasPrefix(host, "[") {
		// ⚠ AN EMPTY PORT IS LEGAL AND `Port()` REPORTS NOTHING. `https://[::1]:/cb`
		// leaves the trailing colon in `host`, so this refused an authority Go
		// accepts -- while the registered-name form with an empty port is admitted a
		// few lines above. Two spellings of one
		// thing, treated differently because only one was in front of me.
		host = strings.TrimSuffix(host, ":")
		if !strings.HasSuffix(host, "]") {
			return false
		}
		lit := host[1 : len(host)-1]
		if strings.HasPrefix(lit, "v") || strings.HasPrefix(lit, "V") {
			// ⚠ THE PREFIX IS NOT THE GRAMMAR. `v` alone admitted
			// `https://[vSERVER-SECRET]/cb`: Go parses it, the prefix check said
			// IPvFuture, and the authority was exempted and preserved VERBATIM, so
			// endpoint-chosen text reached the capture with no structural refusal. `whose body this program does
			// not judge` was true of the body and became an excuse for not judging
			// the shape either. IPvFuture is `v 1*HEXDIG . 1*(unreserved /
			// sub-delims / :)`, and that is now what is checked.
			return isIPvFuture(lit)
		}
		// ⚠ AND A ZONE IS PART OF A VALID IPv6 AUTHORITY. `[fe80::1%25eth0]` is
		// what RFC 6874 puts on the wire; `url.Parse` accepts it and hands back the
		// host with `%25` decoded, and `net.ParseIP` then refused the zone suffix --
		// so a redirect Go itself accepts forced exit 4. Address and zone are validated
		// separately, which is the only way either can be judged at all.
		addr, zone, hasZone := strings.Cut(lit, "%")
		// ⚠ A ZONE IS NOT STRUCTURALLY CONSTRAINED. The host exemption rests on
		// `publicly resolvable and constrained by its grammar`; a zone identifier is
		// an arbitrary LOCAL string, so `[fe80::1%25SERVER_SECRET]` carried endpoint
		// text straight into the capture while this checked only that it was
		// non-empty. The host exemption does not cover an arbitrary zone
		// identifier beside the address.
		//
		// The grammar is checked here -- a zone outside it is not an authority Go
		// would have produced -- but grammar is NOT the property: `SERVER_SECRET` is
		// a perfectly valid zone-id, which is why the zone is REDACTED in the emitted
		// target rather than merely admitted here. See redactZone.
		if hasZone && !isZoneID(zone) {
			return false
		}
		return net.ParseIP(addr) != nil && strings.Contains(addr, ":")
	}
	return true
}

// isZoneID accepts an RFC 6874 zone identifier: unreserved characters only, which
// is what an interface name is. Anything else is a string the endpoint chose.
func isZoneID(z string) bool {
	if z == "" {
		return false
	}
	// RFC 6874 defines zone-id as 1*( unreserved / pct-encoded ).
	// Accept valid percent-encoded bytes as well as unreserved characters.
	//
	// Measured while fixing it: `url.Parse` refuses some escapes inside a bracketed
	// host on its own — `%C3` comes back as "invalid URL escape" — so this branch
	// is reached for fewer spellings than the grammar admits. It states the
	// grammar anyway, for the same reason the rest of this file stopped borrowing
	// predicates from whatever net/url happens to accept today.
	for i := 0; i < len(z); i++ {
		c := z[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '-' || c == '.' || c == '_' || c == '~':
		case c == '%':
			if i+2 >= len(z) || !isHexDigit(z[i+1]) || !isHexDigit(z[i+2]) {
				return false
			}
			i += 2
		default:
			return false
		}
	}
	return true
}

// isIPvFuture is the grammar RFC 3986 gives for the bracketed authority form
// this program does not otherwise judge: "v" 1*HEXDIG "." 1*( unreserved /
// sub-delims / ":" ). Written out because a prefix test admitted anything
// beginning with the letter.
func isIPvFuture(lit string) bool {
	if len(lit) < 4 || (lit[0] != 'v' && lit[0] != 'V') {
		return false
	}
	i := 1
	for i < len(lit) && isHexDigit(lit[i]) {
		i++
	}
	if i == 1 || i >= len(lit) || lit[i] != '.' {
		return false
	}
	body := lit[i+1:]
	if body == "" {
		return false
	}
	for j := 0; j < len(body); j++ {
		if !isIPvFutureByte(body[j]) {
			return false
		}
	}
	return true
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func isIPvFutureByte(c byte) bool {
	if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
		return true
	}
	return strings.IndexByte("-._~!$&'()*+,;=:", c) >= 0
}

// vouchScheme marks an approved URI scheme in a finished target line.
// Mark after redaction: inserting provenance markers during redactTarget
// would invalidate offsets used by its later stages.
func vouchScheme(line string) string {
	// ⚠ AFTER THE FIELD DELIMITER. With no `://` the fallback took the FIRST colon
	// in the line -- which is the header name's -- so an opaque `Location: https:abc`
	// never had its scheme vouched, and a supplied `https` published
	// `<redacted, 5 chars>:redacted-3-chars`.
	// The target starts where `splitField` says it starts.
	_, _, url0, ok0 := splitField(line)
	if !ok0 {
		return line
	}
	off := len(line) - len(url0)
	i := strings.Index(url0, "://")
	if i < 0 {
		if i = strings.IndexByte(url0, ':'); i < 0 {
			return line
		}
	}
	i += off
	j := strings.LastIndexAny(line[:i], " 	")
	sch := line[j+1 : i]
	// ⚠ THE CANONICAL SPELLING, NOT THE RECEIVED ONE. Recognition folds because the
	// grammar does; vouching must not. `HTTPS` is an approved scheme AND a legal
	// supplied identifier, and marking it published it (see canonicalSpelling).
	bare := stripMarks(sch)
	switch strings.ToLower(bare) {
	case "http", "https":
		if !canonicalSpelling(bare, strings.ToLower(bare)) {
			// ⚠ DECLINING TO VOUCH IS NOT THE SAME AS LEAVING IT TO THE PROSE SCRUB.
			// A non-canonical spelling that also equals a supplied identifier reached
			// `scrubSupplied` and became `<redacted, 5 chars>://e.example/...` -- spaces,
			// a comma and angle brackets, which no URI grammar admits -- while the guard
			// approved it because a placeholder is generated. The valueless-cookie-flag branch in
			// this file already answers this, and the answer was applied where it was
			// shown rather than to the shape.
			//
			// Only where it COLLIDES: a non-canonical scheme that is nobody's identifier
			// is endpoint text and is published as received.
			if scrubSuppliedRaw(bare) != bare {
				// ⚠ AND A TOKEN PLACEHOLDER IS NOT A SCHEME EITHER. `HTTPS` colliding
				// produced `redacted-5-chars://e.example/...` with an EMPTY ledger --
				// structurally shaped, and naming a transport no client has. Declining to vouch was fixed
				// here; declining to keep the MEANING was not, and the two are the same
				// sentence one step apart.
				//
				// Schemes fold, so the canonical spelling means what the received one
				// meant and is THIS program's text -- the same answer the admitted
				// header value already gives. Refused only where that spelling is
				// supplied too, because then nothing semantics-preserving is left.
				return line[:j+1] + foldedReplacement(bare, strings.ToLower(bare), "redirect scheme") + line[i:]
			}
			return line
		}
		return line[:j+1] + vouched(sch) + line[i:]
	}
	return line
}

// vouchTargetSyntax marks delimiters in a finished target line. Redaction
// can re-split the line and replace entire path segments, so inserting
// markers earlier would not preserve them. Mark the delimiters only after
// the surrounding captured values have been replaced.
func vouchTargetSyntax(line string) string {
	head, gap, url, ok := splitField(line)
	if !ok {
		return line
	}
	// Do not mark separators inside a bracketed authority. IPvFuture has its
	// own grammar: the equals sign in [v1.=] belongs to its data.
	//
	// ⚠ AND IT IS UNREACHABLE ON THIS GO VERSION, measured: `url.Parse` refuses
	// `https://[v1.=]/cb` outright, so the target is withheld before this pass
	// runs, and a mutant removing this guard survives the suite. It stays for the
	// same reason as the bracket check and `isIPvFuture` beside it: the grammar is
	// stated here rather than inherited from whatever net/url accepts today.
	var b strings.Builder
	inAuthority := false
	// Track marked separators explicitly. Neighboring provenance marks do
	// not prove containment: a separator between two placeholders can lie
	// outside both. Avoid nested marks when redactPairs already marked it.
	inMark := false
	for i := 0; i < len(url); i++ {
		switch {
		case string(url[i]) == genMark:
			inMark = !inMark
			b.WriteByte(url[i])
		case inMark:
			b.WriteByte(url[i])
		case url[i] == '[':
			inAuthority = true
			b.WriteByte(url[i])
		case url[i] == ']':
			inAuthority = false
			b.WriteByte(url[i])
		// The scheme's colon is syntax just like its slashes. Mark all three
		// delimiters so a supplied colon cannot alter the reconstructed URI.
		case !inAuthority && strings.IndexByte("?&=/#:", url[i]) >= 0:
			b.WriteString(syntax(string(url[i])))
		default:
			b.WriteByte(url[i])
		}
	}
	// Restore the field colon explicitly: splitField returns it in neither
	// piece, and a Location line requires that delimiter.
	return head + ":" + gap + b.String()
}

func redactTarget(line string) string {
	// ⚠ THE ACCOUNT IS ATTACHED TO THE ACT. A rewrite recorded by its CALLERS is
	// recorded by however many of them remember to: `redactTarget` rewrote this on the
	// header path and on the trailer path and neither noted it, so a field this
	// program had redacted appeared in neither ledger -- the same gap #85 closed for
	// the JSON body, in its second instance. "Nothing is printed that this program
	// cannot account for" is a claim about what it DID, so the accounting lives
	// where the doing is.
	noteAccounted(formField, "a redirect target")
	// ⚠ A TARGET THAT IS NOT A URI IS NOT PARSED, IT IS WITHHELD. A raw space is
	// illegal in a request target but transport-valid in a header, and net/http
	// keeps the whole opaque value -- so the redactors below treated everything
	// after the space as request-line syntax and appended it unexamined.
	if head, gap, url, ok := splitField(line); ok {
		// ⚠ THE HOST EXEMPTION'S PREMISE IS NOW ENFORCED, NOT ASSUMED. It rests on
		// "a host is structurally constrained" -- and a host is constrained only
		// if something checks: `https://e.example\server-secret/cb` is refused by
		// Go's parser and was preserved verbatim here, because the only test was
		// for whitespace. This is the
		// criterion being WRONG rather than unapplied: an exemption whose
		// condition is never verified is not an exemption, it is a hole with a
		// comment over it.
		if !parsesAsURI(strings.TrimSuffix(url, "\r")) {
			noteStructural(formField, "a Location header whose target is not a valid URI")
			cr := ""
			if strings.HasSuffix(line, "\r") {
				cr = "\r"
			}
			return head + ":" + gap + marked("<withheld: malformed target>") + cr
		}
		// A structurally exempt authority can still collide with supplied text.
		// If the supplied-value scrub would change it, use a URI-safe replacement.
		// Do not replace it merely because it contains punctuation or other text
		// that the scrub would leave unchanged.
		if a := authorityOf(strings.TrimSuffix(url, "\r")); a != "" {
			{
				if !authorityIsHostShaped(a) {
					// ⚠ THE EXEMPTION'S PREMISE IS `publicly resolvable and constrained by
					// its grammar`, and a non-bracketed authority was never measured against
					// it -- `parsesAsURI` validates the BRACKETED form thoroughly and then
					// returns true for everything else. `url.Parse` accepts `se_cret`,
					// `a;b`, `host$tok` and `..`, all of which rode into the capture
					// VERBATIM with nothing recorded.
					//
					// ⚠ AND THE AUTHORITY IS REPLACED, NOT THE TARGET WITHHELD. Failing
					// `parsesAsURI` costs the whole redirect target; the defect is one
					// component, so it is answered where the colliding-value branch beside
					// it is answered -- the target stays readable and the endpoint's token
					// does not ride through. The named limit: an underscore host, which
					// exists in service records and is not a publicly resolvable name, is
					// replaced here rather than kept.
					noteAccounted(formField, "a redirect authority that is not a host")
					if lo, hi, ok := authorityRange(strings.TrimSuffix(url, "\r")); ok {
						url = url[:lo] + tokenPlaceholder(a) + url[hi:]
						line = head + ":" + gap + url
					}
				} else if lo, hi, ok := hostPortRange(strings.TrimSuffix(url, "\r")); ok &&
					scrubSuppliedRaw(url[lo:hi]) != url[lo:hi] {
					// ⚠ REPLACED AND THEN CARRIED ON. Returning here skipped the path,
					// query, fragment and userinfo redactors, so
					// `https://e.example/cb?state=<token>` kept an endpoint token the
					// guard cannot see. Fixing the
					// authority is not finishing the target: this branch answered one
					// question and left the pipeline that answers the rest.
					//
					// ⚠ AND THE HOST/PORT, NOT THE WHOLE AUTHORITY -- see hostPortRange.
					// Userinfo colliding is answered by `redactUserinfo` further down the
					// same pipeline, which keeps the host this exemption exists to keep.
					noteAccounted(formField, "a redirect authority colliding with a supplied value")
					// `url` carries its own terminator, so rebuilding the line from it
					// keeps the CR without a separate branch.
					url = url[:lo] + tokenPlaceholder(url[lo:hi]) + url[hi:]
					line = head + ":" + gap + url
				}
			}
		}
		if strings.ContainsAny(strings.TrimSuffix(url, "\r"), " \t") {
			noteStructural(formField, "a Location header whose target is not a valid URI")
			cr := ""
			if strings.HasSuffix(line, "\r") {
				cr = "\r"
			}
			return head + ":" + gap + marked("<withheld: malformed target>") + cr
		}
	}
	if i := strings.IndexByte(line, '#'); i >= 0 {
		return redactIPvFutureBody(redactZone(redactPath(redactUserinfo(redactResponseQuery(line[:i]))))) + redactFragment(line[i:])
	}
	return redactIPvFutureBody(redactZone(redactPath(redactUserinfo(redactResponseQuery(line)))))
}

// endpointChosenAuthority redacts an authority chosen by the endpoint by
// passing its network-path spelling through the target pipeline. Host
// admission requires a publicly resolvable name constrained by its grammar;
// redactTarget enforces that requirement for redirect request legs too.
// Endpoint-chosen names absent from suppliedValues still need redaction.
func endpointChosenAuthority(value string) string {
	const synth = "X-Redirect-Authority"
	return strings.TrimPrefix(redactTarget(synth+": //"+value), synth+": //")
}

// redirectRequestLine redacts the target of a request line the endpoint chose.
// The line is `METHOD SP target SP HTTP/1.1`; only the middle field is data.
func redirectRequestLine(line string) string {
	cr := ""
	body := line
	if strings.HasSuffix(body, "\r") {
		cr, body = "\r", strings.TrimSuffix(body, "\r")
	}
	method, rest, ok := strings.Cut(body, " ")
	if !ok {
		return line
	}
	target, tail := rest, ""
	if j := strings.IndexByte(rest, ' '); j >= 0 {
		target, tail = rest[:j], rest[j:]
	}
	return method + " " + endpointChosenTarget(target) + tail + cr
}

// endpointChosenTarget redacts a bare URI the ENDPOINT chose, through the
// RESPONSE side's own target pipeline rather than a second copy of it.
//
// ⚠ A REDIRECT LEG'S REQUEST IS NOT THIS PROGRAM'S TEXT. The request redactor
// treats every byte of a dump as harness-authored -- true only while the recorder
// ABSORBED redirect follow-ups. Forwarding them  made the SDK issue requests whose target and `Referer` the endpoint
// chose, and both were published verbatim: measured on a two-redirect chain,
// `GET /server-secret-token?x=y` and `Referer: http://host/server-secret-token?x=y`,
// the latter with its query value untouched because no request-side rule reads it.
//
// The pipeline is written against a `name: value` line, so a bare target is given
// one and it is taken off again. Calling it beats restating it: the path, query,
// fragment, userinfo, zone and IPvFuture rules are one grammar, and a second
// spelling of a grammar has as many edges as it has versions.
func endpointChosenTarget(target string) string {
	const synth = "X-Redirect-Target"
	return strings.TrimPrefix(redactTarget(synth+": "+target), synth+": ")
}

// splitField cuts a header line into its name, the whitespace after the colon,
// and its value.
//
// ⚠ THE SPACE AFTER THE COLON IS OPTIONAL. Cutting on ": " meant `Location:/cb`
// -- transport-valid, OWS is what the grammar calls it -- matched nothing and was
// returned unredacted. One splitter, so the
// callers cannot disagree about where a value begins.
func splitField(line string) (name, gap, value string, ok bool) {
	i := strings.IndexByte(line, ':')
	if i <= 0 {
		return "", "", "", false
	}
	rest := line[i+1:]
	j := 0
	for j < len(rest) && (rest[j] == ' ' || rest[j] == '\t') {
		j++
	}
	return line[:i], rest[:j], rest[j:], true
}

func redactPath(line string) string {
	head, gap, url, ok := splitField(line)
	if !ok {
		return line
	}
	head += ":" + gap
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
	// ⚠ A SCHEME DOES NOT REQUIRE AN AUTHORITY. `https:/cb` is a valid absolute
	// URI with no `//`, and treating `https:` as a path segment rewrote the target
	// into a relative reference that resolves somewhere else.
	if c := strings.IndexByte(url, ':'); c > 0 && isSchemeName(url[:c]) &&
		!strings.HasPrefix(url[c:], "://") {
		switch strings.ToLower(url[:c]) {
		case "http", "https":
			// ⚠ AN OPAQUE URI HAS NO AUTHORITY, SO NOTHING HERE IS EXEMPT.
			// `https:SERVER_SECRET` is a valid absolute URI whose remainder contains
			// no slash, so the path redaction below -- which works on segments --
			// left it untouched and the endpoint's text reached the artifact verbatim. The host exemption is written for
			// a component that is not present in this shape, and an exemption cannot
			// be inherited by whatever stands where its subject would have been.
			if rest := url[c+1:]; rest != "" && !strings.ContainsAny(rest, "/") {
				// MEASURED DECODED, like every other component here.
				// ⚠ AND `head` ALREADY CARRIES THE COLON AND THE OWS. Appending `gap`
				// again turned an ordinary one-space header into
				// `Location:  https:redacted-6-chars`, so the evidence stopped
				// preserving the field layout it received while only the endpoint's
				// value was meant to change. The
				// refusal branch below this one had it right; this return did not.
				return head + url[:c+1] + tokenPlaceholder(queryDecoded(rest)) + tail
			}
			start = c + 1
		default:
			noteStructural(formField, "a Location header with an unapproved URI scheme")
			return head + marked("<withheld: unapproved scheme>") + tail
		}
	}
	// ⚠ ANCHORED. An unrestricted search read the `://` inside `/cb/http://x` as
	// introducing a scheme called `/cb/http`, refused the capture, and exited 4 on
	// a target Go parses as an ordinary relative path.
	if i := strings.Index(url, "://"); i >= 0 && isSchemeName(url[:i]) {
		// ⚠ THE SCHEME IS ENDPOINT-CHOSEN, AND THE HOST EXEMPTION DOES NOT COVER
		// IT. A host is exempt deliberately -- structurally constrained, publicly
		// resolvable. A scheme is neither: `server-secret://e.example/cb` is a
		// valid URI and carried the identifier past everything. An exemption written for one
		// component must not be read as covering whatever sits beside it.
		switch strings.ToLower(url[:i]) {
		case "http", "https":
		default:
			noteStructural(formField, "a Location header with an unapproved URI scheme")
			return head + marked("<withheld: unapproved scheme>") + tail
		}
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
		// ⚠ `.` AND `..` ARE NAVIGATION, NOT DATA. Replacing them with
		// placeholders made the recorded target resolve somewhere else and
		// erased whether the endpoint redirected relative to this path or its
		// parent -- which is exactly the structure this capture exists to show.
		if seg == "." || seg == ".." {
			// Navigation segments are URI syntax this function preserves on purpose, so
			// they are marked as such: with a supplied `..`, `../cb` became
			// `<redacted, 2 chars>/…` -- no longer parent-relative, and approved.
			segs[i] = syntax(seg)
			continue
		}
		if seg != "" {
			// ⚠ MEASURE THE VALUE, NOT ITS WIRE SPELLING -- the same rule the query
			// path already follows. `%C3%A9` is the single character `é` and was
			// reported as six. PathUnescape,
			// not QueryUnescape: `+` is a literal plus in a path.
			segs[i] = tokenPlaceholder(pathDecoded(seg))
		}
	}
	return head + url[:start] + strings.Join(segs, "/") + tail
}

// authorityOf returns the authority of a URI reference, or "" if it has none.
func authorityOf(url string) string {
	a, b, ok := authorityRange(url)
	if !ok {
		return ""
	}
	return url[a:b]
}

// authorityRange returns the authority's byte range in the target.
//
// ⚠ THE OFFSET, BECAUSE THE TEXT REPEATS. Replacing the authority by its first
// textual occurrence edited the wrong component whenever the same text appears
// earlier: with a supplied `http`, `http://http/cb` had its SCHEME replaced, and
// the remaining passes then emitted `redacted-19-chars//redacted-4-chars/...` --
// a malformed target, with no structural refusal recorded. A component is a POSITION in a grammar;
// finding its spelling somewhere is not finding it.
func authorityRange(url string) (int, int, bool) {
	i := strings.Index(url, "//")
	if i < 0 {
		return 0, 0, false
	}
	if i > 0 && !isSchemeName(strings.TrimSuffix(url[:i], ":")) {
		return 0, 0, false
	}
	a := i + 2
	b := len(url)
	if j := strings.IndexAny(url[a:], "/?#"); j >= 0 {
		b = a + j
	}
	return a, b, true
}

func redactSetCookie(line string) string {
	// ⚠ THE ACCOUNT IS ATTACHED TO THE ACT. A rewrite recorded by its CALLERS is
	// recorded by however many of them remember to: `redactSetCookie` rewrote this on the
	// header path and on the trailer path and neither noted it, so a field this
	// program had redacted appeared in neither ledger -- the same gap #85 closed for
	// the JSON body, in its second instance. "Nothing is printed that this program
	// cannot account for" is a claim about what it DID, so the accounting lives
	// where the doing is.
	noteAccounted(formField, "a Set-Cookie value")
	cr := ""
	body := line
	if strings.HasSuffix(body, "\r") {
		cr, body = "\r", strings.TrimSuffix(body, "\r")
	}
	head, rest, ok := strings.Cut(body, ":")
	if !ok {
		noteStructural(formField, "an unparseable Set-Cookie header")
		return marked("<withheld: unparseable Set-Cookie>")
	}
	pair, attrs, hasAttrs := strings.Cut(rest, ";")
	name, value, hasValue := strings.Cut(ows(pair), "=")
	// An empty cookie name has nothing to measure. Refuse it rather than
	// inventing a valid-looking name with a zero-length placeholder.
	// This is specifically an empty-name check; nonempty malformed names
	// are measured by the existing component-rendering rule.
	if hasValue && ows(name) == "" {
		noteStructural(formField, "a Set-Cookie header with an empty cookie name")
		return head + ": " + marked("<withheld>") + cr
	}
	if !hasValue {
		// ⚠ AN EMPTY FIELD HAS NO VALUE BYTES TO CONCEAL. `Set-Cookie:` is preserved
		// and serialised by net/http, and it went through the refusal written for
		// `Set-Cookie: server-secret` -- a field carrying an endpoint token. There is
		// no token here, so the refusal is about something that is not present, and an
		// otherwise safe capture exited 4.
		//
		// An empty value contains no bytes to redact and remains empty.
		if ows(rest) == "" {
			return head + ":" + rest + cr
		}
		// `Set-Cookie: server-secret` is transport-valid and net/http keeps it.
		// Returning it unchanged published a server-generated value the guard
		// cannot see.
		noteStructural(formField, "a Set-Cookie header with no name=value pair")
		return head + ": " + marked("<withheld>") + cr
	}
	// ⚠ THE QUOTES ARE DELIMITERS, NOT VALUE. A quoted cookie `sid="abc"` was
	// reported as five characters for a three-character value -- the same
	// measure-the-spelling defect the query path has been fixed for twice.
	measured := value
	if len(measured) >= 2 && measured[0] == '"' && measured[len(measured)-1] == '"' {
		measured = measured[1 : len(measured)-1]
	}
	// ⚠ A COOKIE NAME IS ALWAYS LENGTHENED, and the query-name registry says nothing
	// about it. This used to vouch a cookie name that appears in `requestNames` -- so
	// with `experiment_key` both supplied and set as a cookie by the endpoint, the
	// name was marked generated and skipped by the scrub and the guard. A name we sent in a QUERY is not a name
	// we authored in a `Set-Cookie` the ENDPOINT wrote: membership in one namespace
	// is not provenance in another.
	//
	// MEASURED AS RECEIVED. `responseText` expands a marker-like spelling on the way
	// in, so a cookie name containing the literal `\x00` was reported three
	// characters longer than it arrived.
	//
	// The placeholder is GENERATED, which also keeps the earlier defect closed: the
	// supplied-value scrub cannot rewrite it into prose the way it rewrote a name
	// left captured.
	name = tokenPlaceholder(unescapeMarks(name))
	// Measured before `escapeMarks` lengthened it, exactly as redactUnlessVerbatim
	// does -- the structural path had kept the older behaviour.
	// ⚠ A COOKIE-SAFE PLACEHOLDER. `<redacted, N chars>` carries a space and a
	// comma, and neither is a `cookie-octet`: a strict RFC 6265 consumer rejects or
	// re-reads the line this change promises to keep structural. `tokenPlaceholder` is the spelling the
	// cookie NAME and the URI components already use, and it is safe in all three.
	out := head + ": " + name + syntax("=") + tokenPlaceholder(unescapeMarks(measured))
	if hasAttrs {
		// ⚠ ATTRIBUTE VALUES ARE SERVER-GENERATED TOO. `; Path=/reset/<token>`,
		// or an extension attribute carrying a nonce, went through unchanged
		// while the cookie's own value was lengthened -- the same bytes treated
		// two ways in one line. Attribute
		// NAMES are a closed set in the specification and are kept; their values
		// are not, and are lengthened.
		parts := strings.Split(attrs, ";")
		for i, a := range parts {
			if ows(a) == "" {
				// A trailing `;` produces an empty component. Replacing it invented
				// an attribute the response did not carry.
				continue
			}
			an, av, has := strings.Cut(a, "=")
			if !has {
				// Only standard valueless flags may be admitted without a value.
				// Extension attributes are endpoint-chosen text, and standard attributes
				// that require a value do not become flags when one is absent.
				// Emit admitted flags with their canonical spelling.
				if can, ok := canonicalCookieFlag(ows(an)); ok && ows(an) == can {
					// ⚠ MARKED, NOT MERELY KEPT. Left as captured text, a flag whose
					// name equals a supplied identifier -- `Secure` -- reached
					// `scrubSupplied` and became `<redacted, 6 chars>`: no longer a
					// cookie flag, and approved because a placeholder is generated.
					parts[i] = markAttrName(an)
					continue
				}
				// ⚠ AND A FLAG REPLACED BY ITS LENGTH IS NO LONGER THAT FLAG. Supplied
				// `SECURE` against `; SECURE` became `; redacted-6-chars` with an empty
				// ledger, so the published cookie is not Secure -- the capture stating
				// something about the response that the response did not say. `HttpOnly` and `Partitioned`
				// behave the same way. Flag names fold, so the canonical spelling is the
				// same flag.
				if canFlag, okFlag := canonicalCookieFlag(ows(an)); okFlag &&
					scrubSuppliedRaw(ows(an)) != ows(an) {
					parts[i] = " " + foldedReplacement(ows(an), canFlag, "cookie flag")
					continue
				}
				parts[i] = " " + tokenPlaceholder(ows(an))
				continue
			}
			// Extension attribute names can carry identifying text too. Admit only
			// specification-defined names; replace other nonempty names safely.
			// Refuse an empty attribute name rather than inventing a valid-looking
			// attribute with a zero-length placeholder.
			if ows(an) == "" {
				noteStructural(formField, "a Set-Cookie attribute with an empty name")
				return head + ": " + marked("<withheld>") + cr
			}
			if !standardCookieAttr(an) {
				// Keep provenance marks on generated placeholders. Stripping them here
				// would let the later scrub turn a grammar-safe cookie token into prose.
				parts[i] = " " + tokenPlaceholder(ows(an)) +
					syntax("=") + tokenPlaceholder(unescapeMarks(ows(av)))
				continue
			}
			// Classify the received attribute name before marking it. Provenance
			// marks are not part of the vocabulary that selects Max-Age, Expires
			// or SameSite value rules.
			verbatim := cookieAttrVerbatim(an, ows(av))
			// ⚠ COMPUTED HERE, BESIDE THE PREDICATE, for the reason written directly
			// above: `an` is about to be MARKED, and a lookup on the marked spelling
			// answers about a name no registry contains.
			canAttr, canKnown := cookieAttrCanonical(an, ows(av))
			// Look up the attribute before marking its name. Provenance markers are
			// not part of the registry spelling; looking up a marked name would make
			// an ordinary SameSite=Lax value appear unregistered.
			enumerated := cookieAttrEnumerates(ows(an))
			// Emit valued standard attribute names in their canonical spelling,
			// as for valueless standard flags.
			if can, ok := canonicalCookieAttr(ows(an)); ok && ows(an) == can {
				// Marked for the same reason a vouched-for parameter name is: the
				// value scrub would otherwise rewrite `Path` into a prose
				// placeholder and produce an attribute no cookie parser accepts.
				an = markAttrName(an)
			} else if can, ok := canonicalCookieAttr(ows(an)); ok && scrubSuppliedRaw(ows(an)) != ows(an) {
				// The same rule as the flag beside it, and the same reason: an attribute
				// name replaced by its length is a different attribute. Found by asking
				// the population when the flag was shown, not by being told.
				an = strings.Replace(an, ows(an), foldedReplacement(ows(an), can, "cookie attribute name"), 1)
			} else if scrubSuppliedRaw(ows(an)) != ows(an) {
				// ⚠ AND THE NON-CANONICAL SPELLING NEEDS THE SAME PROTECTION. Restricting
				// the vouch to the canonical case was right and left `PATH=/` with a
				// supplied `PATH` for the prose scrub, which produced
				// `<redacted, 4 chars>=` -- spaces, a comma and angle brackets are not
				// cookie attribute-name bytes, so the structure-preserving capture was
				// malformed and approved. Declining
				// to vouch is not the same as declining to keep the syntax.
				an = strings.Replace(an, ows(an), tokenPlaceholder(ows(an)), 1)
			}
			// ⚠ AND ONLY IF THE ARRIVED SPELLING IS THE CANONICAL ONE. The predicate
			// folds -- `SameSite=LAX` is a legal cookie -- so vouching the received
			// spelling published a supplied `LAX` (see canonicalSpelling). A
			// non-canonical spelling is not refused, it is simply left CAPTURED, which
			// is what the value is.
			// ⚠ THE COLLISION CHECK IS FOR THE NON-ENUMERATED ATTRIBUTES ONLY. `Max-Age`
			// is an integer and `Expires` an HTTP-date: admitted by SHAPE, and shape
			// says nothing about who chose the value, so `Max-Age=123456` vouched a
			// supplied numeric key exactly as the header path did. `SameSite=Lax` is different in kind
			// -- the specification ENUMERATES its values, so `Lax` is the grammar's own
			// token whoever else also chose that string, and three sweep rows say it
			// must survive.
			if verbatim && canKnown && canonicalSpelling(ows(av), canAttr) &&
				(enumerated || scrubSuppliedRaw(ows(av)) == ows(av)) {
				// The VALUE is vouched for by the same criterion that admitted it; see
				// redactUnlessVerbatim for why admitting without marking is not admitting.
				parts[i] = an + syntax("=") + strings.Replace(av, ows(av), vouched(ows(av)), 1)
				continue
			}
			// ⚠ MEASURED AS RECEIVED, NOT AS ESCAPED. `responseText` expands a
			// marker-like spelling before this runs, so `Path=\x00` -- four characters
			// on the wire -- was reported as seven. The cookie's own value already
			// unescaped before measuring; the attributes did not.
			// ⚠ SHAPE-ADMITTED AND COLLIDING MEANS NO SPELLING IS LEFT. `Max-Age` is
			// an integer and `Expires` an HTTP-date, so with a supplied `123456`,
			// `Max-Age=123456` reached this fallback and was answered with a token
			// placeholder that is not an integer -- and nothing was recorded, so the
			// guard approved a cookie no parser accepts. Same rule as the admitted header
			// value: a registry token survives a collision, a shape does not, and where
			// no grammar-preserving replacement exists the capture is refused.
			if verbatim && canKnown && !enumerated && canonicalSpelling(ows(av), canAttr) &&
				scrubSuppliedRaw(ows(av)) != ows(av) {
				noteStructural(formField, "a shape-admitted cookie attribute whose colliding value has no grammar-preserving spelling")
			}
			// Enumerated cookie values need grammar-safe output too. Recognition
			// folds case, so write the canonical enumeration spelling rather than
			// passing a non-canonical value to a generic length placeholder.
			if verbatim && canKnown && enumerated && !canonicalSpelling(ows(av), canAttr) &&
				scrubSuppliedRaw(ows(av)) != ows(av) {
				rep := foldedReplacement(ows(av), canAttr, "cookie attribute value")
				parts[i] = an + syntax("=") + strings.Replace(av, ows(av), rep, 1)
				continue
			}
			parts[i] = an + syntax("=") + tokenPlaceholder(unescapeMarks(ows(av)))
		}
		out += syntax(";") + strings.Join(parts, syntax(";"))
	}
	return out + cr
}

// markAttrName marks a cookie attribute NAME as generated while keeping the
// whitespace around it: the surrounding OWS is captured layout, and the name is
// what this program vouches for.
//
// Mark every vouched cookie name as generated, including standard flags,
// valued attributes and an admitted cookie name. Otherwise the later
// supplied-value scrub can replace a valid name with prose.
func markAttrName(an string) string {
	name := ows(an)
	if name == "" {
		return an
	}
	return strings.Replace(an, name, marked(name), 1)
}

// redactIPvFutureBody replaces an IPvFuture authority's data portion with
// its length, preserving the v<hex>. prefix. RFC 3986 permits arbitrary
// data after the dot; satisfying that grammar does not establish provenance.
//
// The current net/url parser rejects IPvFuture authorities, so this helper
// is exercised directly by tests and is not an end-to-end path guarantee.
// It handles the syntax if a future parser begins accepting it.
func redactIPvFutureBody(line string) string {
	b := strings.IndexByte(line, '[')
	if b < 0 {
		return line
	}
	e := strings.IndexByte(line[b:], ']')
	if e < 0 {
		return line
	}
	e += b
	lit := line[b+1 : e]
	if !isIPvFuture(lit) {
		return line
	}
	dot := strings.IndexByte(lit, '.')
	if dot < 0 || dot+1 >= len(lit) {
		return line
	}
	return line[:b+1] + lit[:dot+1] + tokenPlaceholder(lit[dot+1:]) + line[e:]
}

func redactZone(line string) string {
	b := strings.IndexByte(line, '[')
	if b < 0 {
		return line
	}
	e := strings.IndexByte(line[b:], ']')
	if e < 0 {
		return line
	}
	e += b
	lit := line[b+1 : e]
	z := strings.Index(lit, "%25")
	sep := 3
	if z < 0 {
		if z = strings.IndexByte(lit, '%'); z < 0 {
			return line
		}
		sep = 1
	}
	zone := lit[z+sep:]
	if zone == "" {
		return line
	}
	// MEASURED DECODED, like every other component here: `eth%30` is `eth0`, four
	// characters, and measuring the wire spelling put two lengths for one value in
	// one capture.
	return line[:b+1] + lit[:z+sep] + tokenPlaceholder(queryDecoded(zone)) + line[e:]
}

// redactUserinfo removes `user:password@` from a redirect target.
//
// ⚠ A THIRD PLACE A URL CARRIES A CREDENTIAL, after the query and the fragment,
// and the one that is a credential BY DEFINITION rather than by convention:
// `Location: https://user:secret@example.com/…` is standard URI syntax. Kept as a marker rather than dropped,
// so the artifact still shows that userinfo was present.
func redactUserinfo(line string) string {
	// A network-path reference -- `//user:secret@host/cb` -- is a legal Location
	// and carries userinfo without a scheme.
	// ⚠ AT THE START. An unrestricted search read the `http://` inside
	// `/cb/http://foo@bar/x` as an authority and redacted `foo` as userinfo, after
	// which the generated token was measured again and the seven-character segment
	// was reported as twenty-two. The
	// network-path branch was already anchored; this one was not.
	i := -1
	if _, _, u, ok := splitField(line); ok {
		if j := strings.Index(u, "://"); j >= 0 && isSchemeName(u[:j]) {
			i = len(line) - len(u) + j
		}
	}
	skip := 3
	if i < 0 {
		// ⚠ AT THE START OF THE TARGET. An unrestricted search read `//foo@bar`
		// inside `/a//foo@bar/b` as an authority and redacted `foo` as userinfo,
		// so the segment was reported at the wrong length.
		// ⚠ A NETWORK-PATH REFERENCE IS AT THE START OF THE TARGET. A mutant
		// loosening this to `Contains` survives, and that is honest: the
		// authority bound below already stops an interior `//`, so two guards
		// cover the case and only one of them is load-bearing today. The prefix
		// test states the grammar, and the grammar is what the next reader needs.
		if _, gap, url, ok := splitField(line); ok && strings.HasPrefix(url, "//") {
			i, skip = len(line)-len(url), 2
			_ = gap
		} else {
			return line
		}
	}
	rest := line[i+skip:]
	// ⚠ THE LAST `@` IN THE AUTHORITY, NOT THE FIRST. Go reads everything before
	// the final separator as userinfo, so `//user@server-secret@host/cb` had only
	// `user` redacted and the endpoint-generated middle was preserved as part of
	// the authority.
	authEnd := len(rest)
	if j := strings.IndexAny(rest, "/?#"); j >= 0 {
		authEnd = j
	}
	at := strings.LastIndexByte(rest[:authEnd], '@')
	if at < 0 {
		return line
	}
	for _, c := range rest[:at] {
		if c == '/' || c == ' ' || c == '?' || c == '#' {
			return line
		}
	}
	// MEASURED DECODED, like every other component here: `us%C3%A9r:p` is six
	// characters, not eleven.
	// ⚠ AND THE SEPARATOR THIS RECONSTRUCTS IS SYNTAX. The userinfo becomes a
	// placeholder and the `@` was handed on as captured text, so a supplied
	// identifier SPANNING the boundary -- `@e` in `https://u@e.example/cb` -- was
	// replaced across it and the authority stopped being an authority. A supplied value need not BE the
	// punctuation to cross it, which is why the sweep behind this asks about
	// spellings and not characters.
	return line[:i+skip] + tokenPlaceholder(queryDecoded(rest[:at])) + syntax("@") + rest[at+1:]
}

// structuralRedact applies the rules a field NAME selects, for fields whose
// values are server-generated and therefore unreachable by any scrub built from
// values THIS program supplied.
//
// ⚠ IT IS A FUNCTION BECAUSE IT HAS TWO CALL SITES. The header block had these
// rules and the trailer block did not, so the same `Set-Cookie` was redacted in
// one position and published in the other.
// A trailer is a header that arrived late; it is not a different kind of secret.
func structuralRedact(line string) (string, bool) {
	low := strings.ToLower(line)
	switch {
	case strings.HasPrefix(low, "set-cookie:"):
		return redactSetCookie(line), true
	case strings.HasPrefix(low, "location:"):
		return redactTarget(line), true
	}
	// ⚠ AND EVERY OTHER FIELD WHOSE VALUE THE ENDPOINT MINTS. The two cases above
	// are the ones this build can DESCRIBE -- it renders the cookie's shape and the
	// target's syntax and accounts for them. Membership in `serverMintedFields` is a
	// different question from having a rendering, and answering only the first two
	// left a `WWW-Authenticate` arriving as a trailer published, because the trailer
	// path asks its structural question here and nowhere else. No
	// rendering means no account, so it is withheld and the record is refused.
	if name, ok := fieldNameOf(low); ok {
		if note, minted := serverMintedFields[name]; minted {
			noteStructural(formField, note)
			return canonicalFieldName(name) + ": " + marked("<withheld>"), true
		}
	}
	return line, false
}

// redactMintedBody replaces server-minted identifiers in a JSON body with a
// length-preserving placeholder, structurally -- by FIELD NAME, the only handle
// that exists when the value itself is unknown to this program.
// redactUnaccountedJSONValues replaces every string VALUE in a parsed body that
// this program cannot account for with its length.
//
// ⚠ PARSING IS NOT ACCOUNTING. The body rule refused a body that does not parse
// and let a parsed one through whole -- so `{"error":"server-secret-token"}` was
// published verbatim: `error` is an admitted MEMBER, its value is endpoint-minted,
// and the guard is blind because the value was never supplied by this program. The admitted list says which member NAMES
// this program recognises. It says nothing about their values.
//
// What is accounted for: a value this SDK itself produces (the verdict taxonomy),
// and the grammar's own literals, which are not strings. Everything else is
// endpoint text and becomes a length -- which is what the clause says.
// keySpan finds the span of the string token that ends at `end` in `view`.
func keySpan(view string, end int) (int, int) {
	k := end - 2
	for k >= 0 {
		if view[k] == '"' {
			bs := 0
			for m := k - 1; m >= 0 && view[m] == '\\'; m-- {
				bs++
			}
			if bs%2 == 0 {
				return k, end
			}
		}
		k--
	}
	return -1, end
}

func anyMarked(inMark []bool, a, b int) bool {
	for m := a; m < b && m < len(inMark); m++ {
		if inMark[m] {
			return true
		}
	}
	return false
}

// sdkVerdictFields are the TOP-LEVEL members whose value this SDK writes as its
// own classification. Named once, so a scene cannot assert a position the code
// does not vouch at -- four fixtures asserted `code` was one and pinned the wrong
// half of the rule until the wire contract was read.
var sdkVerdictFields = map[string]bool{"reason": true}

// sdkReasonValues are the classifications the SDK accepts AT the `reason` member.
//
// ⚠ NARROWER THAN THE TAXONOMY, BECAUSE THE POSITION IS NARROWER. `sdkTaxonomy`
// is every string this SDK writes as a classification anywhere; the not-assigned
// branch allows only `{absent, kill_switch, targeting_unmatched, age_ineligible}` and refuses the
// body outright for anything else. Vouching the whole taxonomy here says the SDK
// wrote a value it would have rejected.
// `TestTheReasonValuesAreTheSDKsOwn` reads the constants out of the SDK source.
var sdkReasonValues = map[string]bool{
	"kill_switch":         true,
	"targeting_unmatched": true,
	"age_ineligible":      true,
}

// verdictKey groups a member name the way `encoding/json` groups it.
//
// ⚠ THE DECODER FOLDS, AND THE COUNTING HAS TO FOLD WITH IT. `encoding/json`
// matches `Reason` and `REASON` to the same field, so
// `{"reason":"kill_switch","Reason":"anything"}` is classified `anything` -- while
// exact-string counting put the two occurrences under separate keys, left the
// first vouched, and published the supplied identifier. The ordinal answers "which occurrence does
// the decoder keep", so it must group members exactly as the decoder does.
//
// This is the one place in this file where folding is CORRECT: it models the
// decoder's own matching. Vouching the VALUE still requires the canonical
// spelling, because that question is about what this program wrote.
func verdictKey(name string) string {
	for n := range sdkVerdictFields {
		if strings.EqualFold(name, n) {
			return n
		}
	}
	return name
}

// sdkAssignmentWire MIRRORS `expAssignmentWire` in experiments.go MEMBER FOR
// MEMBER -- every tag, and every Go type. The vouch below asks whether the SDK
// would accept a body as an assignment verdict, and the SDK asks that with
// `json.Unmarshal` into that struct: THE TYPING IS THE GRAMMAR. A reduced copy
// carrying only the members this pass reads answers a weaker question, and
// `{"assigned":false,"variant_payload":1,"reason":"kill_switch"}` is the
// difference -- the SDK rejects that body because `variant_payload` is not a map,
// while the reduced copy decoded it and vouched `reason`, so a supplied
// `kill_switch` was skipped by BOTH the scrub and the guard and published with an
// empty refusal ledger.
//
// TestTheMirroredWireShapeMatchesTheSDKs compares both mirrored structs
// with the SDK source. The SDK's decoding type is unexported, so this
// example keeps a checked mirror without changing the API it observes.
type sdkAssignmentWire struct {
	AppKey         json.RawMessage `json:"app_key"`
	EnvironmentKey json.RawMessage `json:"environment_key"`
	ExperimentKey  json.RawMessage `json:"experiment_key"`
	Version        json.RawMessage `json:"version"`
	Assigned       *bool           `json:"assigned"`
	AssignmentKey  string          `json:"assignment_key"`
	VariantKey     string          `json:"variant_key"`
	VariantPayload map[string]any  `json:"variant_payload"`
	Reason         json.RawMessage `json:"reason"`
	SubjectFactKey string          `json:"subject_fact_key"`
	Boundary       map[string]any  `json:"boundary"`
	ServedRevision json.RawMessage `json:"served_revision"`
	ServedKillGate json.RawMessage `json:"served_kill_gate"`
	ServedAt       json.RawMessage `json:"served_at"`
}

// statusCodeOf reads a status line's code. Extracted so the exemption registry and
// the `reason` vouch ask it the same way; two spellings of "what status is this"
// are how the two came to disagree.
func statusCodeOf(statusLine string) (int, bool) {
	f := strings.Fields(strings.TrimSuffix(statusLine, "\r"))
	if len(f) < 2 || !strings.HasPrefix(f[0], "HTTP/") {
		return 0, false
	}
	n, err := strconv.Atoi(f[1])
	if err != nil {
		return 0, false
	}
	return n, true
}

func redactUnaccountedJSONValues(body string, exempt map[string]bool, statusLine string) string {
	// ⚠ A PATTERN OVER MEMBER-COLON-STRING IS NOT A TRAVERSAL. The first version
	// matched only a string immediately after a member's colon, so
	// `{"variant_payload":["server-secret-token"]}` kept its array element verbatim
	// -- the body parses, so the body rule accepts it, and the guard is blind
	// because the value was never supplied by this program. Array elements, nested members and a
	// top-level string are values too.
	//
	// Traverse the decoder's token stream and use token-end offsets to recover
	// string-value spans. Key/value tracking excludes keys. Parse a mark-free
	// view with an index map back to the original text; leave already marked
	// spans untouched.
	plain := make([]byte, 0, len(body))
	back := make([]int, 0, len(body))
	inMark := make([]bool, 0, len(body))
	md := 0
	for i := 0; i < len(body); i++ {
		if body[i] == capturedMark[0] || body[i] == genMark[0] {
			md++
			continue
		}
		plain = append(plain, body[i])
		back = append(back, i)
		inMark = append(inMark, md%2 == 1)
	}
	view := string(plain)
	dec := json.NewDecoder(strings.NewReader(view))
	dec.UseNumber()
	type span struct {
		a, b int
		val  string
		// Set only where a value was VOUCHED as this SDK's taxonomy, so the
		// ineffective duplicates can be demoted once the whole document is known.
		vouchedField string
		raw          string
		// ⚠ AND THE OCCURRENCE'S ORDINAL, COUNTED OVER EVERY VALUE AT THAT MEMBER.
		// The first version of the demotion recorded only VOUCHED spans, so a final
		// duplicate whose value is not taxonomy never entered the map and the earlier
		// vouch survived as the last one it knew about:
		// `{"code":"kill_switch","code":"anything"}` published the identifier while
		// the decoder classifies `anything`. What
		// decides is WHICH occurrence the decoder uses, and that is a fact about the
		// document, not about the values this program happens to recognise.
		verdictOrd int
	}
	var spans []span
	var depth []int8 // 0 = array, 1 = object expecting KEY, 2 = object expecting VALUE
	// ⚠ WHETHER THE SDK READS `reason` AT ALL, WHICH IS A FACT ABOUT THE WHOLE BODY.
	//
	// `parseExperimentVerdict` rejects the body outright when `assigned` is ABSENT,
	// returns from the assigned branch before touching `reason` when it is TRUE, and
	// rejects again when a PRESENT echoed member is not a string or does not equal
	// the request's own value. Only after all of that is `reason` this SDK's
	// classification.
	//
	// The first version of this asked only "is assigned true", so an absent
	// `assigned` -- a body the SDK refuses entirely -- still vouched, and
	// `{"reason":"kill_switch"}` published a supplied identifier with an empty
	// ledger.
	//
	// ⚠ EACH ECHO AGAINST ITS OWN REQUEST VALUE. `expEchoMatches` requires a present
	// member to equal the value THIS request carried in that slot, and a flat list of
	// supplied values cannot say which value belongs in which member -- so an
	// endpoint returning the app key in `experiment_key` passed a membership test
	// that the SDK rejects, and `reason` was vouched for a body the SDK never
	// classifies.
	//
	// Pass the configured identifiers through explicitly so this pass can
	// apply their supplied-value checks.
	reasonIsSDKs := false
	// A non-200 response is classified by status, not as an assignment body.
	// Its body can contain taxonomy-shaped text without that text being the
	// SDK's verdict. Apply this condition before vouching for a reason.
	code, haveStatus := statusCodeOf(statusLine)
	// Apply the SDK's size gate before decoding a verdict. Use captured body
	// bytes, since earlier redaction may shorten a response that originally
	// exceeded expMaxBodyBytes. The exemption registry uses the same inputs.
	sdkLen, sdkView := len(body), view
	if capturedBodyBytes >= 0 {
		sdkLen = capturedBodyBytes
	}
	if capturedBodyRaw != "" {
		sdkView = capturedBodyRaw
	}
	if haveStatus && code == 200 && !capturedIncomplete && sdkLen <= sdkMaxBodyBytes {
		var shape sdkAssignmentWire
		// Presence-aware, exactly as `expEchoMatches` states it: absent is tolerated,
		// while a present member must be a JSON STRING and must be one this harness
		// sent. A present non-string -- an explicit null included -- is not this
		// contract's shape.
		echoed := func(raw json.RawMessage, want string) bool {
			if raw == nil {
				return true
			}
			// ⚠ AN UNRECORDED REQUEST VALUE CANNOT CONFIRM AN ECHO. If this harness did
			// not record what it asked for in that slot, a present member cannot be shown
			// to be this request's -- and an unconfirmable echo is not a vouch.
			if want == "" {
				return false
			}
			var v string
			if json.Unmarshal(raw, &v) != nil {
				return false
			}
			return v == want
		}
		// Check version after decoding. Absence is tolerated in the traffic-gate
		// shape; a present value must be numeric and, in the not-assigned branch,
		// at least 1. Null or a non-positive value cannot authorize a verdict.
		if json.Unmarshal([]byte(sdkView), &shape) == nil &&
			shape.Assigned != nil && !*shape.Assigned &&
			echoed(shape.AppKey, requestedAppKey) &&
			echoed(shape.EnvironmentKey, requestedEnvKey) &&
			echoed(shape.ExperimentKey, requestedExpKey) {
			versionOK := true
			if shape.Version != nil {
				var v *int64
				if json.Unmarshal(shape.Version, &v) != nil || v == nil || *v < 1 {
					versionOK = false
				}
			}
			reasonIsSDKs = versionOK
		}
	}
	verdictField := ""
	verdictSeen := map[string]int{}
	verdictOrd := 0
	for {
		off := int(dec.InputOffset())
		_ = off
		tok, err := dec.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			return body // not a shape this pass can traverse; the body rule answers
		}
		advance := func() {
			if n := len(depth); n > 0 && depth[n-1] == 2 {
				depth[n-1] = 1
			}
		}
		isKey := false
		atRoot := false
		if d, ok := tok.(json.Delim); ok {
			switch d {
			case '{', '[':
				// ⚠ A CONTAINER IN VALUE POSITION ENDS THE VERDICT POSITION. Leaving
				// `verdictField` set across `[` marked the first element of
				// `{"code":["kill_switch"]}` as this SDK's taxonomy. The SDK classifies a SCALAR
				// there; anything else in that position is the endpoint's shape.
				verdictField = ""
			}
			switch d {
			case '{':
				advance()
				depth = append(depth, 1)
			case '[':
				advance()
				depth = append(depth, 0)
			case '}', ']':
				if len(depth) > 0 {
					depth = depth[:len(depth)-1]
				}
			}
			continue
		}
		if n := len(depth); n > 0 && depth[n-1] == 1 {
			isKey, atRoot = true, n == 1
			depth[n-1] = 2
		} else {
			advance()
		}
		if isKey {
			name, isStr := tok.(string)
			if isStr && atRoot {
				verdictField = name
				// Counted for EVERY root member, whatever its value turns out to be --
				// including a value this traversal produces no span for, such as a
				// number, which is still the occurrence the decoder keeps.
				verdictSeen[verdictKey(name)]++
				verdictOrd = verdictSeen[verdictKey(name)]
			} else {
				verdictField = ""
			}
			// ⚠ AND A KEY OUTSIDE THE SCHEMA IS ENDPOINT TEXT LIKE ANY VALUE. This
			// traversal skipped every key, and only canonical TOP-LEVEL names are
			// accounted for elsewhere -- so `{"variant_payload":{"server-secret-token":1}}`
			// published the nested member NAME verbatim: the top-level check accepts
			// `variant_payload`, the number is grammar, and nothing knows the token. A name the endpoint chose is a
			// string the endpoint chose.
			// ⚠ EXEMPTED BY THE RAW SPELLING, NOT BY WHAT IT DECODES TO. `name` here is
			// the DECODER's output, so `{"\u0061ssigned":false}` exempted the key as a
			// schema name -- while the vouching pass correctly declined to mark it, and
			// the scrub and guard then failed to match a supplied `0061` inside
			// `u0061ssigned`. Two passes asking
			// the same question, one about the spelling and one about the meaning, and
			// a token can satisfy neither's answer to the other.
			kq, ke := keySpan(view, int(dec.InputOffset()))
			canonicalKey := kq >= 0 && view[kq:ke] == `"`+name+`"`
			if isStr && !(atRoot && canonicalKey && (benignTopLevel[name] || mintedNames[name])) {
				k, end := kq, ke
				if k >= 0 && !anyMarked(inMark, k, end) {
					noteAccounted(formBody, "an endpoint-chosen member name in a parsed response body")
					spans = append(spans, span{back[k], back[end-1] + 1,
						`"` + tokenPlaceholder(asArrived(view[k:end], name)) + `"`, "", name, 0})
				}
			}
			continue
		}
		str, ok := tok.(string)
		if !ok {
			verdictField = ""
			continue
		}
		end := int(dec.InputOffset())
		// Walk back to the opening quote of this string, skipping escaped quotes.
		k := end - 2
		for k >= 0 {
			if view[k] == '"' {
				bs := 0
				for m := k - 1; m >= 0 && view[m] == '\\'; m-- {
					bs++
				}
				if bs%2 == 0 {
					break
				}
			}
			k--
		}
		// ⚠ ANY BYTE OF THE SPAN, NOT ONLY ITS OPENING QUOTE. `redactMintedBody` marks
		// the placeholder's CONTENTS and leaves the JSON quotes around them unmarked,
		// so testing `inMark[k]` at the quote read a generated placeholder as endpoint
		// data and replaced it again -- reporting the placeholder's length instead of
		// the identifier's. A span that contains
		// generated bytes is not endpoint text.
		if k < 0 || end > len(view) {
			verdictField = ""
			continue
		}
		already := false
		for m := k; m < end; m++ {
			if inMark[m] {
				already = true
				break
			}
		}
		if already {
			verdictField = ""
			continue
		}
		// Vouch taxonomy only at a position where the SDK reads it. Payload text
		// and a root code member are endpoint-chosen: code is not a member of
		// expAssignmentWire. The assigned branch returns before reading reason,
		// so reason is not a verdict in that branch either.
		//
		// The allowlist is the narrower one too: the not-assigned branch takes
		// `{absent, kill_switch, targeting_unmatched, age_ineligible}` and refuses the body for
		// anything else, so the wider taxonomy would vouch a value the SDK rejects.
		if sdkReasonValues[str] && sdkVerdictFields[verdictKey(verdictField)] && reasonIsSDKs {
			// ⚠ THE SPAN INCLUDES THE QUOTES, SO THE REPLACEMENT MUST TOO. Emitting
			// only `marked(str)` produced `{"code":kill_switch}` after the marks are
			// stripped -- invalid JSON, which the body rule then refused, so EVERY
			// ordinary verdict capture was withheld with exit 4. The neighbouring branch got this
			// right and this one did not, in the same expression.
			spans = append(spans, span{back[k], back[end-1] + 1, `"` + marked(str) + `"`, verdictField, str, verdictOrd})
		} else {
			noteAccounted(formBody, "an endpoint-chosen value in a parsed response body")
			spans = append(spans, span{back[k], back[end-1] + 1,
				`"` + tokenPlaceholder(asArrived(view[k:end], str)) + `"`, "", str, verdictOrd})
		}
		verdictField = ""
	}
	// Assemble value replacements in one pass. Rebuilding the body for each
	// span makes work and allocation quadratic in the number of strings,
	// even for a body within the response-size limit.
	//
	// ⚠ ONLY THE OCCURRENCE THE DECODER USES IS THIS SDK'S CLASSIFICATION.
	//
	// JSON permits duplicate members and `encoding/json` keeps the LAST, so
	// `{"code":"kill_switch","code":"not_found"}` is classified `not_found` --
	// while this traversal vouched EVERY `code` value it recognised, marking the
	// first as taxonomy. A marked span is skipped by both the scrub and the guard,
	// so a supplied `kill_switch` was published as though this program had written
	// it. Recognising a taxonomy token is not
	// having emitted it, and here even the POSITION was not enough: the position
	// has to be the EFFECTIVE one.
	//
	// Demote the ineffective occurrence to endpoint-chosen text rather than
	// refusing the body. The SDK accepts this shape, and ordinary redaction
	// can account for the occurrence.
	for i := range spans {
		if f := spans[i].vouchedField; f != "" && spans[i].verdictOrd != verdictSeen[verdictKey(f)] {
			noteAccounted(formBody, "an endpoint-chosen value in a parsed response body")
			spans[i].val = `"` + tokenPlaceholder(spans[i].raw) + `"`
		}
	}
	// The spans are produced in ascending order by the traversal; the builder walks
	// them once and copies each byte at most once.
	var out strings.Builder
	out.Grow(len(body))
	prev := 0
	for _, sp := range spans {
		if sp.a < prev || sp.b > len(body) {
			continue
		}
		out.WriteString(body[prev:sp.a])
		out.WriteString(sp.val)
		prev = sp.b
	}
	out.WriteString(body[prev:])
	return out.String()
}

func redactMintedBody(body string, exempt map[string]bool) string {
	// ⚠ MEMBER NAMES ARE MATCHED BY WHAT THEY DENOTE, NOT BY ONE SPELLING, and
	// ASCII case does not distinguish them: `encoding/json` matches a field
	// case-insensitively, so `SUBJECT_FACT_KEY` is the same field to the SDK and
	// to the endpoint.
	//
	// ⚠ NO OUTER `marked`: placeholder ALREADY returns one. Wrapping it made
	// adjacent nested provenance marks, `overCaptured` then read the placeholder
	// itself as captured text and rewrote it.
	out := jsonMemberValue.ReplaceAllStringFunc(body, func(m string) string {
		g := jsonMemberValue.FindStringSubmatch(m)
		if !isMinted(g[1]) {
			return m
		}
		val, ok := jsonString(g[3])
		if !ok {
			val = g[3]
		}
		return `"` + g[1] + `"` + g[2] + `"` + placeholder(val) + `"`
	})
	// ⚠ AND A MINTED NAME THIS PATTERN COULD NOT DESCRIBE IS REFUSED, NOT
	// PUBLISHED. Anything left carrying a minted member name after the pass above
	// held a value shape these rules do not cover.
	// ⚠ SCAN WHAT WAS NOT REDACTED, NOT WHAT WAS. Run over `out`, this found the
	// member name the replacement above had just left in place, refused the body,
	// and made EVERY ordinary fact response unpublishable -- the exact responses
	// this change exists to publish. The
	// scan runs on the ORIGINAL body and skips the spans the value pattern
	// covered, so it reports only shapes that pattern could not describe.
	covered := jsonMemberValue.FindAllStringIndex(body, -1)
	// ⚠ ONE CURSOR, NOT A RESCAN PER MATCH. Both lists come from
	// `FindAllStringIndex` and are therefore ASCENDING, and this walked `covered`
	// from the beginning for every member name -- quadratic on a body with many
	// members even when no minted field exists, so a valid response near the
	// capture limit spent many seconds here AFTER the bounded HTTP operation had
	// finished. The callers below consume
	// their matches in order, which is what makes a cursor correct rather than
	// merely faster; it is asserted rather than assumed.
	cur, last := 0, -1
	inCovered := func(i int) bool {
		if i < last {
			panic("inCovered called out of order: the cursor's premise does not hold")
		}
		last = i
		for cur < len(covered) && covered[cur][1] <= i {
			cur++
		}
		return cur < len(covered) && i >= covered[cur][0] && i < covered[cur][1]
	}
	// ⚠ AND THE REFUSAL IS TOP-LEVEL ONLY, as the guard half's detection is.
	// `encoding/json` binds the SDK's field from the top-level object; a member of
	// that name inside `variant_payload` is ordinary payload the endpoint chose to
	// call that, and refusing on it made a publishable assignment unpublishable. The REDACTION above stays depth-blind
	// on purpose -- lengthening a payload member costs a reader a label, while
	// publishing a minted key costs the subject.
	// ⚠ AND A BODY THAT WILL NOT PARSE FAILS CLOSED. `topLevelMembers` returns
	// nothing for a truncated body -- `{"subject_fact_key":"sfk1_…` with no
	// closing quote -- and "no top-level names" was then read as "this occurrence
	// is nested", so a partial credential reached the artifact by the path that
	// exists to publish what arrived. A
	// parse that fails is not an answer about depth.
	// Refuse unfamiliar top-level members here, as noteMinted does in the guard.
	// ⚠ AND AN UNPARSABLE BODY IS ACCOUNTED FOR TOO. `topLevelMembers` returns
	// nothing for `{"assigned":true,"subject_fact_key":"…` with no closing brace,
	// so this loop did not run: the value WAS redacted -- the identifier never
	// reached the artifact -- and neither ledger recorded that anything had
	// happened. Safe and silent is still a hole in "nothing is printed that this
	// program cannot account for" (the guard refuses this shape; redaction must account for its contents).
	// ⚠ `!jsonParses`, NOT `topLevelMembers == nil` -- the same conflation the
	// guard half was shown, living here too: the nil return ALSO means "parsed,
	// but not an object", so `[{"subject_fact_key":…}]` was treated as
	// unclassifiable although its structure proves the member is nested.
	// ⚠ AND NO `{` PREREQUISITE. A CLOSE-DELIMITED body -- `"subject_fact_key":
	// "sfk1_…"` with no object around it at all -- is malformed to the SDK and
	// carries no brace, so the scan was skipped on exactly the shape it exists for. The brace
	// was a guess about how a malformed body LOOKS, standing in front of a rule
	// about what one CONTAINS.
	if !jsonParses(body) {
		// An incomplete parse makes every affected member indeterminate,
		// including unfamiliar names as well as minted names. Do not publish
		// an unparsed name or value merely because its traversal did not run.
		//
		// Fail closed BEFORE the benign-name allowance, because that allowance
		// rests on knowing the member is top-level, which is exactly what a failed
		// parse cannot tell us.
		for _, m := range jsonMemberName.FindAllStringSubmatch(body, -1) {
			dec, ok := jsonString(m[1])
			if !ok {
				dec = m[1]
			}
			switch {
			case isMinted(m[1]):
				noteAccounted(formBody, "a server-minted subject identifier in a body that does not parse")
			case !isBenignName(dec):
				noteStructural(formBody, "a member of a body that does not parse, in a shape this program has not judged")
			}
		}
	}
	for _, n := range topLevelMembers(body) {
		// Record ordinary successful redaction as well as structural refusals.
		// Top-level minted-field accounting uses the same depth condition as its
		// detection; an arbitrary payload member with that name does not qualify.
		if isMintedName(n) {
			noteAccounted(formBody, "a server-minted subject identifier")
		}
		if !isMintedName(n) && !isBenignName(n) {
			noteStructural(formBody, "a top-level member this program has not judged")
		}
	}
	parsed := topLevelMembers(body)
	// A nil topLevelMembers set does not imply a parse failure. A valid array
	// can contain nested members without any top-level object members; its
	// depth is still known and must not be treated as an unreadable body.
	unparsable := !jsonParses(body) && strings.Contains(body, "{")
	top := map[string]bool{}
	for _, n := range parsed {
		top[n] = true
	}
	depthAt := newDepthWalker(body)
	for _, loc := range jsonMemberName.FindAllStringSubmatchIndex(body, -1) {
		if inCovered(loc[0]) {
			continue
		}
		name := body[loc[2]:loc[3]]
		dec, _ := jsonString(name)
		if !unparsable && (depthAt(loc[0]) != 1 || !top[dec]) {
			continue
		}
		if isMinted(name) {
			// ⚠ NOTED AND WITHHELD, not noted alone. The capture is refused at
			// publication either way, but the rule a reader has to hold is
			// simpler if it has no exceptions: a surface these rules cannot
			// describe does not appear in what this function returns. The body
			// goes whole, because the shape that defeated the pattern is exactly
			// the shape whose extent cannot be determined.
			noteStructural(formBody, "a server-minted field in a value shape these rules do not describe")
			return marked("<withheld: a minted field in an undescribed shape>")
		}
	}
	// ⚠ LAST, AFTER EVERY PARSE. Placed at the top, this inserted provenance marks
	// inside JSON strings before this function's own `jsonParses`, `topLevelMembers`
	// and depth walk read the body -- eighteen tests said so at once. The rule is
	// the same one the grammar pass had to learn: analysis runs on a document,
	// marking runs on the result.
	//
	// Mark admitted member names as grammar so the supplied-value scrub
	// preserves the wire contract. Arbitrary payload names remain captured;
	// only a name admitted by the applicable registry is vouched.
	var vouchAt [][2]int
	// Vouch only for top-level schema members. A member inside variant_payload
	// is endpoint-controlled even when its spelling matches an SDK field.
	// Parse a mark-free view because replacement can insert provenance bytes
	// inside JSON strings. Only the name set is needed here, so offsets need
	// not be mapped back to the marked input.
	topNames := map[string]bool{}
	for _, n := range topLevelMembers(stripMarks(out)) {
		topNames[n] = true
	}
	vouchWalk := newDepthWalker(out)
	for _, loc := range jsonMemberName.FindAllStringSubmatchIndex(out, -1) {
		raw := out[loc[2]:loc[3]]
		dec, ok := jsonString(raw)
		if !ok {
			dec = raw
		}
		if vouchWalk(loc[0]) != 1 || !topNames[dec] {
			continue
		}
		// ⚠ ONLY IF THE WIRE SPELLING IS THE NAME. Recognition here is SEMANTIC --
		// `{"ass\u0069gned":false}` decodes to a name this program knows -- and marking
		// the raw span then vouched for `ass\u0069gned`, so a supplied `u0069` was
		// skipped by both the scrub and the guard. Knowing what a spelling MEANS is not
		// knowing that this program wrote it.
		// ⚠ AND THE CANONICAL CASE, not only the unescaped spelling. `raw == dec`
		// catches an escape and says nothing about case, while both predicates fold
		// -- so `{"ASSIGNED":false}` with a supplied `ASSIGNED` was vouched. The question was never about escapes;
		// it is whether THIS program would have written that spelling.
		can, known := benignCanonicalIn(exempt, dec)
		// A minted member name is grammar only where the SDK reads that member.
		// Apply exempt before consulting the minted registry, including in this
		// fallback; an error response can carry the same name as arbitrary input.
		if !known && len(exempt) > 0 {
			can, known = mintedCanonical(dec)
		}
		if known && raw == dec && canonicalSpelling(dec, can) {
			vouchAt = append(vouchAt, [2]int{loc[2], loc[3]})
		}
	}
	// Assemble vouched-name replacements in one pass. Rebuilding the whole
	// document per span creates quadratic allocations for bodies containing
	// many members, despite the response-size limit.
	//
	// The spans are produced in ascending order by the scan; the builder walks them
	// once and copies each byte at most once.
	if len(vouchAt) == 0 {
		return out
	}
	var b strings.Builder
	b.Grow(len(out))
	prev := 0
	for _, sp := range vouchAt {
		if sp[0] < prev || sp[1] > len(out) {
			continue
		}
		b.WriteString(out[prev:sp[0]])
		b.WriteString(vouched(out[sp[0]:sp[1]]))
		prev = sp[1]
	}
	b.WriteString(out[prev:])
	return b.String()
}

// redactPath replaces every non-empty path segment of a redirect target with its
// length, keeping the separators.
//
// ⚠ THE PATH CARRIES CREDENTIALS AS READILY AS THE QUERY. `/reset/<token>` is
// an ordinary shape, the segment is SERVER-generated, and so no list of values
// this program supplied can reach it -- userinfo, query and fragment were all
// covered and the path was published whole.
// Every segment goes, not a chosen few: picking which ones "look opaque" is the
// entropy guess this file has already been burnt by, and a redirect path is
// server-generated in its entirety.
// pathDecoded is a path segment's decoded spelling when it decodes, and itself
// otherwise. queryDecoded is the same for a query value, where `+` is a space.
func pathDecoded(s string) string {
	if dec, err := url.PathUnescape(s); err == nil {
		return dec
	}
	return s
}

func queryDecoded(s string) string {
	if dec, err := url.QueryUnescape(s); err == nil {
		return dec
	}
	return s
}

// ── which header values may be printed as received ───────────────────────────
//
// ⚠ THIS IS AN INVERSION, AND THE INVERSION IS THE POINT. The dispatch above
// names the fields it redacts -- `Set-Cookie`, `Location` -- which makes it an
// ENUMERATION, complete only from the inside. A sweep of ten unrecognised forms
// found seven published: `Content-Location`, `Refresh`, `Link`,
// `WWW-Authenticate`, `Set-Cookie2` each carry a server-generated URL or token
// and each went straight through, because the only other tool is a scrub built
// from values the HARNESS supplied, which by construction cannot see them.
//
// Adding five names would confirm the enumeration. So the question is turned
// round: a value is printed as received only if it PASSES a test, and everything
// else is replaced by its length. Unknown fails closed.
//
// ⚠ THE CRITERION, WRITTEN OUT SO THE NEXT FIELD IS JUDGED BY IT RATHER THAN BY
// MEMORY OF WHY THESE WERE CHOSEN:
//
//	A header value may be published verbatim only if every token in it is drawn
//	from a vocabulary the SPECIFICATION fixes -- an HTTP-date, an integer, or a
//	registered keyword -- so that the origin had no opportunity to choose a
//	string. Any free-form token, quoted-string, URI or parameter fails,
//	whatever the field is called.
//
// Content-Type is admitted only without parameters. Parameters such as
// a multipart boundary contain endpoint-chosen text outside the fixed
// media-type vocabulary.
//
// A field that fails is not refused; its VALUE is replaced by its length. A
// length is enough to show the shape of the exchange, and refusing every capture
// carrying a `Server:` banner would make the harness useless. Refusal is
// reserved for the surfaces above, where a value's EXTENT cannot be determined.
//
// The criterion above is the SECTION's; the rows that apply it are below.

// admission is everything this program knows about one response field's value,
// in one row.
//
// ⚠ FOUR ANSWERS TO ONE QUESTION, AND ANY TWO COULD DISAGREE. "Is this field's
// value the specification's word or the endpoint's?" was answered in four places
// by hand -- this map, `admittedByRegistry`'s switch, `canonicalAdmitted`'s
// switch, and `registryOnlyValue` walking the DIRECTIVE registries. Three of them
// named `content-type`; the fourth could not, because media types are not
// directives. So an ordinary `Content-Type: application/json` against a supplied
// `json` took a structural refusal and exited 4 -- while `markMediaType` vouched
// that very value one pass later and the refusal was never withdrawn.
//
// Adding media types to that fourth answer is the same enumeration one floor
// down, which is the failure this file has now paid for four times. These are
// facts about a FIELD, so they are stated per field, once: a row cannot disagree
// with itself, and a field added later cannot be added to three lists out of
// four.
type admission struct {
	// ok admits a value as well-formed for this field.
	ok func(string) bool
	// vocabulary says `ok` is a REGISTRY -- the specification fixed the word --
	// rather than a SHAPE, which constrains the alphabet and says nothing about who
	// chose the content. Only a registry value has a canonical spelling this
	// program can write in place of the endpoint's bytes.
	vocabulary bool
	// registryOnly admits a value EVERY part of which is a registry word: no free
	// number, no endpoint text. Such a value is the grammar's own spelling whoever
	// else also chose that string, so colliding with a supplied value does not make
	// it the endpoint's. It is a SEPARATE rule from `ok`, not a synonym:
	// `Cache-Control: max-age=123456` is admitted and its argument is free, which
	// is why the walk behind it forbids arguments where `ok`'s allows them.
	registryOnly func(string) bool
	// folds says the registry is case-INSENSITIVE, so lowering a value yields the
	// registry's own spelling. HTTP method names are case-sensitive, so `allow`
	// does not fold -- and that fact is read from `directiveFolds` rather than
	// re-typed here, for the reason this whole type exists.
	folds bool
}

// directiveAdmission builds the row for a field whose value is a comma list of
// registered directives.
//
// ⚠ AND THE ARGUMENT RULE IS THE FIELD'S, NOT THE REGISTRY'S. Every registered
// field was once given `numericArg`, so `Allow: GET=123456`, `Content-Encoding:
// gzip=123456` and `Vary: accept=123456` all passed and the whole value was vouched
// -- publishing endpoint-selected numeric text, which both the scrub and the guard
// then skip if it is a supplied identifier.
// Those three grammars have no arguments at all; `Cache-Control` is the one here
// that does (`max-age=60`), so it is the one that gets them, in `directiveArgs`.
func directiveAdmission(field string) admission {
	return admission{
		ok:           func(v string) bool { return walkDirectives(field, v, true) },
		vocabulary:   true,
		registryOnly: func(v string) bool { return walkDirectives(field, v, false) },
		folds:        directiveFolds(field),
	}
}

var verbatimHeaders = map[string]admission{
	// Shapes: the alphabet is fixed and the content is the endpoint's, so there is
	// no registry spelling to fall back on and a collision cannot be waved through.
	"date":           {ok: isHTTPDate},
	"expires":        {ok: isHTTPDate},
	"last-modified":  {ok: isHTTPDate},
	"content-length": {ok: isDigits},
	"age":            {ok: isDigits},
	// A registered media type carries no free part -- `isMediaTypeWithoutParameters`
	// rejects parameters -- so the rule that admits it is also the rule that says
	// every part of it is the registry's.
	"content-type": {
		ok:           isMediaTypeWithoutParameters,
		vocabulary:   true,
		registryOnly: isMediaTypeWithoutParameters,
		folds:        true,
	},
	"content-encoding":  directiveAdmission("content-encoding"),
	"transfer-encoding": directiveAdmission("transfer-encoding"),
	"connection":        directiveAdmission("connection"),
	"vary":              directiveAdmission("vary"),
	"accept-ranges":     directiveAdmission("accept-ranges"),
	"allow":             directiveAdmission("allow"),
	"cache-control":     directiveAdmission("cache-control"),
}

// ows trims exactly what HTTP calls optional whitespace: space and tab.
//
// ⚠ `strings.TrimSpace` IS WIDER THAN THE GRAMMAR. It removes non-breaking space
// and other Unicode whitespace, which net/http preserves in the value -- so a
// validator that trimmed with it approved bytes the endpoint chose and the
// publisher then printed verbatim. Trimming
// more than the grammar allows is the same class as parsing more leniently than
// it: the check stops describing the thing it guards.
func ows(v string) string { return strings.Trim(v, " \t") }

func isHTTPDate(v string) bool {
	v = ows(v)
	// ⚠ THE ZONE IS FIXED TO `GMT`, AND Go's LAYOUT IS NOT. `MST` accepts any
	// three-letter zone, so `Monday, 02-Jan-06 15:04:05 XYZ` parsed and the
	// endpoint-chosen token was published through a field the criterion had
	// declared safe. ANSIC carries no zone
	// at all and is checked as itself.
	for _, f := range []string{http.TimeFormat, time.RFC850} {
		if _, err := time.Parse(f, v); err == nil && strings.HasSuffix(v, "GMT") {
			return true
		}
	}
	_, err := time.Parse(time.ANSIC, v)
	return err == nil
}

func isDigits(v string) bool {
	v = ows(v)
	if v == "" {
		return false
	}
	for i := 0; i < len(v); i++ {
		if v[i] < '0' || v[i] > '9' {
			return false
		}
	}
	return true
}

// sourceProvenance names where every transcribed list in this program came from,
// in one place a fixture can read. A list whose source is only in a comment above
// it is a list whose source is lost the first time someone reformats.
const sourceProvenance = `
media types            https://www.iana.org/assignments/media-types/media-types.xhtml
response field names   https://www.iana.org/assignments/http-fields/http-fields.xhtml
cache directives       https://www.iana.org/assignments/http-cache-directives/
content codings        https://www.iana.org/assignments/http-parameters/
request methods        https://www.iana.org/assignments/http-methods/
benign response members  expAssignmentWire in experiments.go (json tags)
`

func isMediaTypeWithoutParameters(v string) bool {
	// ⚠ NO SEPARATE PARAMETER CHECK. An explicit `ContainsAny(v, ";\"")` guard
	// stood here and a mutant removing it survived -- correctly, because it was
	// EQUIVALENT: neither `;` nor `"` nor a space is a token byte, so a value
	// carrying parameters already fails `isTokenOnly`. Code that looks
	// load-bearing and is not teaches the next reader a rule the program does
	// not have.
	t, sub, ok := strings.Cut(ows(v), "/")
	if !ok || !isTokenOnly(t) || !isTokenOnly(sub) {
		return false
	}
	// Require a registered media type. A well-formed token such as
	// application/server-secret can still contain endpoint-chosen text.
	return registeredMediaTypes[strings.ToLower(ows(v))]
}

// registeredDirectives are the closed vocabularies the specification fixes, per
// field.
//
// ⚠ THE CRITERION SAID "REGISTERED KEYWORD" AND THE CODE CHECKED "IS A TOKEN".
// Those are not the same rule, and the gap is exactly where an endpoint puts
// things: `Cache-Control: x-debug=server-secret` is syntactically a directive
// list, every part a legal token, and it published a secret through a field the
// criterion had declared safe.
//
// Validate each directive against its field's vocabulary; an unrecognized
// directive fails the field. Registry sources:
// https://www.iana.org/assignments/http-cache-directives/
// https://www.iana.org/assignments/http-parameters/ (Content Coding)
// https://www.iana.org/assignments/http-methods/
// Regenerate these vocabularies from their corresponding registries.
var registeredDirectives = map[string]map[string]bool{
	"cache-control": set("no-cache", "no-store", "no-transform", "public", "private",
		"must-revalidate", "proxy-revalidate", "max-age", "s-maxage", "immutable",
		"must-understand", "stale-while-revalidate", "stale-if-error", "only-if-cached",
		"max-stale", "min-fresh"),
	"content-encoding":  set("gzip", "deflate", "br", "zstd", "compress", "identity", "x-gzip", "x-compress"),
	"transfer-encoding": set("chunked", "compress", "deflate", "gzip", "identity"),
	"connection":        set("close", "keep-alive", "upgrade", "te"),
	"accept-ranges":     set("bytes", "none"),
	// ⚠ IN THE REGISTRY'S OWN SPELLING, because this field does not fold. Method
	// names are case-sensitive, so the members are written as the registry writes
	// them -- lowercase entries plus a folding lookup admitted `get` as `GET`. The set and the lookup are one decision:
	// storing a folded spelling and then not folding loses the real methods.
	"allow": set("GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS",
		"TRACE", "PATCH", "QUERY"),
	"vary": set("*", "accept", "accept-encoding", "accept-language", "accept-charset",
		"accept-datetime", "origin", "user-agent", "cookie", "authorization", "referer",
		"access-control-request-method", "access-control-request-headers",
		"sec-fetch-dest", "sec-fetch-mode", "sec-fetch-site", "prefer", "range"),
}

// isDirectiveList accepts a comma-separated list in which every directive NAME
// is registered for this field, and every argument is an integer or another
// registered name. A quoted-string fails outright: that is where a free-form
// value lives.
// walkDirectives answers two questions with one parser, because they are the same
// walk with one clause different: `numericArg` decides whether a NUMBER is an
// acceptable argument. Admission says yes -- `max-age=0` is a valid directive.
// Vouching says no: a number constrains the ALPHABET and says nothing about who
// chose it, which is the distinction the collision rule below turns on. Two
// parsers would have been two grammars.
// directiveFolds says whether a field's registry is case-INSENSITIVE. Cache
// directives, codings and connection options are; `Allow` carries METHOD NAMES,
// which are case-sensitive, so `get` is not the registered `GET`. Folding is a property of each registry,
// not a convenience shared by the list -- and it lives in the MEMBERSHIP test,
// which is why fixing only the canonical-spelling function changed nothing.
func directiveFolds(field string) bool { return field != "allow" }

func walkDirectives(field, v string, allowArgs bool) bool {
	known := registeredDirectives[field]
	key := func(x string) string {
		if directiveFolds(field) {
			return strings.ToLower(x)
		}
		return x
	}
	if known == nil || strings.Contains(v, `"`) {
		return false
	}
	for _, part := range strings.Split(v, ",") {
		part = ows(part)
		if part == "" {
			return false
		}
		name, arg, hasArg := strings.Cut(part, "=")
		if !known[key(ows(name))] {
			return false
		}
		if hasArg {
			// A registered token does not by itself authorize an argument. Validate
			// whether this field's directive grammar permits the argument before
			// admitting its value.
			//
			// A field whose grammar has no arguments accepts NONE, and a directive
			// that defines no argument accepts none either. That is a property of the
			// grammar, so it is stated once, per directive, in `directiveArgs` -- not
			// as a list of argument kinds to forbid one at a time.
			if !allowArgs {
				return false
			}
			rule := directiveArgs[field][key(ows(name))]
			if rule == nil || !rule(ows(arg)) {
				return false
			}
		}
	}
	return true
}

// registryOnlyValue reports whether every token in an admitted value is a member
// of that field's REGISTRY -- nothing in it is a free number or endpoint text.
// Such a value is the grammar's own spelling whoever else also chose that string,
// exactly as a registered media type is, so a collision with a supplied value does
// not make it the endpoint's.
func registryOnlyValue(name, v string) bool {
	a := verbatimHeaders[strings.ToLower(ows(name))]
	return a.registryOnly != nil && a.registryOnly(v)
}

func isTokenOnly(v string) bool {
	if v == "" {
		return false
	}
	for i := 0; i < len(v); i++ {
		if !isTokenByte(v[i]) {
			return false
		}
	}
	return true
}

// registeredFieldNames contains specification-defined response fields.
// Other names are endpoint-chosen text. The entries come from permanent
// response field names and commonly deployed provisional names in
// https://www.iana.org/assignments/http-fields/http-fields.xhtml.
// Regenerate from that registry; a missing entry reduces readability
// by replacing the name rather than admitting it.
var registeredFieldNames = set(
	"accept-ch", "accept-encoding", "accept-patch", "accept-post",
	"accept-ranges", "access-control-allow-credentials",
	"access-control-allow-headers", "access-control-allow-methods",
	"access-control-allow-origin", "access-control-expose-headers",
	"access-control-max-age", "age", "allow", "alt-svc", "alt-used",
	"cache-control", "cache-status", "clear-site-data", "connection",
	"content-digest", "content-disposition", "content-encoding",
	"content-language", "content-length", "content-location", "content-range",
	"content-security-policy", "content-security-policy-report-only",
	"content-type", "cross-origin-embedder-policy", "cross-origin-opener-policy",
	"cross-origin-resource-policy", "date", "deprecation", "etag", "expires",
	"idempotency-key", "keep-alive", "last-modified", "link", "location",
	"nel", "origin-agent-cluster", "permissions-policy", "pragma", "prefer",
	"preference-applied", "proxy-authenticate", "referrer-policy",
	"refresh", "repr-digest", "report-to", "reporting-endpoints", "retry-after",
	"sunset", "server", "server-timing", "set-cookie", "sourcemap",
	"strict-transport-security", "timing-allow-origin", "tk", "trailer",
	"transfer-encoding", "upgrade", "vary", "via", "want-content-digest",
	"want-repr-digest", "warning", "www-authenticate", "x-content-type-options",
	"x-frame-options", "x-xss-protection",
	// written by this program itself
	"x-capture-note",
)

// admitFieldName classifies the received name before scrubbing it.
// The registry asks what arrived; the scrub asks what was supplied.
// Mark admitted grammar so subsequent scrubbing preserves field names,
// attributes, content codings, JSON members and reason phrases.
//
// `vouched` is that rule with a name. Every admit site calls it.
func vouched(tok string) string { return marked(tok) }

func admitFieldName(name string) string {
	if !registeredFieldNames[strings.ToLower(ows(name))] {
		return tokenPlaceholder(ows(name))
	}
	// ⚠ THE CANONICAL SPELLING, AND HERE IT IS GO'S. HTTP field names are
	// case-insensitive, so the registry folds -- and vouching the RECEIVED spelling
	// marked an endpoint's `DATE` as this program's own syntax, which both the scrub
	// and the guard then skipped. The sweep
	// named 76 rows of this at once, which is the whole registry: the defect was in
	// the predicate, not at any of the sites.
	//
	// Both Go's CanonicalMIMEHeaderKey spelling and HTTP/2's lowercase
	// spelling are protocol-fixed. Other case variants remain captured text
	// and receive the normal supplied-value checks.
	n := ows(name)
	if n != textproto.CanonicalMIMEHeaderKey(n) && n != strings.ToLower(n) {
		// ⚠ AND IF THE GENERIC SCRUB WOULD REACH IT, REPLACE IT SAFELY HERE. Left
		// captured, a non-canonical spelling that equals a supplied identifier was
		// rewritten by the prose scrub into `<redacted, 4 chars>` -- spaces and angle
		// brackets, which a field name may not contain, approved by the guard because
		// the placeholder is generated. Not
		// vouching it was right; leaving it where a prose rule could reach it was not.
		if scrubSuppliedRaw(n) != n {
			// ⚠ AND THE PLACEHOLDER CHANGES THE FIELD'S IDENTITY. `CONTENT-TYPE`
			// colliding was published as `redacted-12-chars:`, so a consumer can no
			// longer see which standard header arrived -- token-safe, and a different
			// field. Field names fold, so the
			// canonical spelling is the same field.
			//
			// Canonicalize only registry-defined names. An endpoint-invented name
			// has no registered identity to preserve; its token placeholder already
			// has the required grammar.
			if registeredFieldNames[strings.ToLower(n)] {
				return foldedReplacement(n, textproto.CanonicalMIMEHeaderKey(n), "response field name")
			}
			return tokenPlaceholder(n)
		}
		return name
	}
	return vouched(name)
}

func redactFieldName(name string) string {
	if registeredFieldNames[strings.ToLower(ows(name))] {
		return name
	}
	return tokenPlaceholder(ows(name))
}

// redactUnlessVerbatim lengthens a header value unless its field passes the
// criterion above. The NAME is kept: it is what makes the artifact readable, and
// it is scrubbed separately for supplied values.
// cookieAttrVerbatim applies the SAME criterion to a cookie attribute: kept only
// if the specification fixes the vocabulary. `Max-Age` is an integer, `Expires`
// an HTTP-date, `SameSite` three keywords. `Path` and `Domain` are strings the
// origin invents, and are lengthened.
// standardCookieAttr reports a cookie attribute name the specification fixes.
// canonicalCookieFlag returns the specification's spelling of a VALUELESS cookie
// flag. `Path`, `Domain`, `Max-Age`, `Expires` and `SameSite` are not here: they
// carry values, and a bare one is not a flag but an attribute the endpoint
// truncated.
// cookieAttr is everything this program knows about ONE cookie attribute, in one
// row.
//
// ⚠ SIX HAND-WRITTEN LISTS ANSWERED FIVE QUESTIONS ABOUT NINE ATTRIBUTES, AND THEY
// COULD DISAGREE. `Priority` was in the NAME list and in `standardCookieAttr`, and
// in neither value list -- so an ordinary `Set-Cookie: sid=x; Priority=High` was
// published as `Priority=redacted-4-chars` with an empty ledger, erasing the
// cookie's priority while the analogous `SameSite` vocabulary was kept.
//
// TestEveryCookieAttributeHasExactlyOneRule checks that each cookie
// attribute has one rule shared by its consumers.
type cookieAttr struct {
	// canonical is the specification's spelling of the NAME.
	canonical string
	// flag: the attribute carries no value at all.
	flag bool
	// shape admits a value by its FORM -- an integer, an HTTP-date. A shape says
	// nothing about who chose the content, which is why a collision on one cannot
	// be answered with a canonical spelling.
	shape func(string) bool
	// values is the vocabulary the specification ENUMERATES, in its own spelling.
	values []string
	// free: the value is opaque by design and this program has no rule for it.
	// Stated rather than left as an absence, so "no rule" cannot be a typo.
	free bool
}

var cookieAttrs = map[string]cookieAttr{
	"secure":      {canonical: "Secure", flag: true},
	"httponly":    {canonical: "HttpOnly", flag: true},
	"partitioned": {canonical: "Partitioned", flag: true},
	"path":        {canonical: "Path", free: true},
	"domain":      {canonical: "Domain", free: true},
	"expires":     {canonical: "Expires", shape: isHTTPDate},
	"max-age":     {canonical: "Max-Age", shape: maxAgeReadByTheParser},
	"samesite":    {canonical: "SameSite", values: []string{"Lax", "Strict", "None"}},
	"priority":    {canonical: "Priority", values: []string{"Low", "Medium", "High"}},
}

func cookieAttrRow(name string) (cookieAttr, bool) {
	a, ok := cookieAttrs[strings.ToLower(ows(name))]
	return a, ok
}

func canonicalCookieFlag(name string) (string, bool) {
	if a, ok := cookieAttrRow(name); ok && a.flag {
		return a.canonical, true
	}
	return "", false
}

// canonicalCookieAttr returns the specification's spelling of a cookie attribute
// name, valued or not.
// cookieAttrCanonicalEnumerated reports whether an admitted attribute VALUE comes
// from a vocabulary the specification enumerates, as `SameSite` does -- as opposed
// to one admitted by SHAPE, like an integer `Max-Age` or an HTTP-date `Expires`.
func cookieAttrEnumerates(name string) bool {
	a, ok := cookieAttrRow(name)
	return ok && len(a.values) > 0
}

func canonicalCookieAttr(name string) (string, bool) {
	if a, ok := cookieAttrRow(name); ok {
		return a.canonical, true
	}
	return "", false
}

func standardCookieAttr(name string) bool {
	_, ok := cookieAttrRow(name)
	return ok
}

// canonicalSpelling answers, for a token some predicate RECOGNISED under folding,
// what spelling this program itself would have written for it -- and reports
// whether the arrived one IS that spelling.
//
// Admission can fold case, but provenance must reflect the spelling
// this program writes. A non-canonical received token cannot be vouched
// merely because it matches a registered value ignoring case.
//
// Recognising a token and having written it are different facts. A predicate that
// folds answers the first. Only equality with the canonical spelling answers the
// second.
// foldedReplacement answers ONE question for every token in this file whose
// grammar folds: the arrived spelling collides with a supplied value, so what is
// emitted in its place?
//
// Preserve grammar when declining to vouch for a received spelling.
// This applies to redirect schemes, cookie names and values, field names,
// admitted header values and registered media types.
//
// The answer never varied: a length placeholder is not a member of any of these
// grammars, and the canonical spelling is -- it means what arrived, because the
// grammar folds, and it is THIS program's text rather than the endpoint's. So it is
// substituted where it does not itself collide, and where it does, nothing
// semantics-preserving is left and the capture is refused rather than published
// with an empty ledger.
//
// `what` names the token in the refusal, so the ledger still says which grammar
// could not be preserved.
func foldedReplacement(arrived, canonical, what string) string {
	if scrubSuppliedRaw(canonical) == canonical {
		return vouched(canonical)
	}
	noteStructural(formField, "a "+what+" whose colliding spelling has no grammar-preserving replacement")
	return tokenPlaceholder(arrived)
}

func canonicalSpelling(arrived, canonical string) bool { return arrived == canonical }

// benignCanonical and mintedCanonical return the registry's OWN spelling of a
// name, which is the one this program writes.
// ⚠ THE SHAPE'S OWN MEMBERS, NOT EVERY KNOWN NAME. `error` is grammar in a 401
// body and endpoint-chosen text in a 200 assignment, so vouching every known
// top-level name in every object published a supplied `error` out of
// `{"assigned":false,"error":"x"}`. The caller passes the set its status selected.
func benignCanonicalIn(exempt map[string]bool, name string) (string, bool) {
	for n := range exempt {
		if strings.EqualFold(name, n) {
			return n, true
		}
	}
	return "", false
}

func benignCanonical(name string) (string, bool) {
	for n := range benignTopLevel {
		if strings.EqualFold(name, n) {
			return n, true
		}
	}
	return "", false
}

func mintedCanonical(name string) (string, bool) {
	for n := range mintedNames {
		if strings.EqualFold(name, n) {
			return n, true
		}
	}
	return "", false
}

// cookieAttrCanonical returns the canonical spelling of an admitted attribute
// VALUE. A numeric or date value is its own canonical form -- there is no
// registry spelling to compare against -- and the enumerated ones are lower-case.
func cookieAttrCanonical(name, value string) (string, bool) {
	// Use the same Max-Age rule as cookieAttrVerbatim so both callers agree
	// on accepted numeric spellings, including -1.
	a, ok := cookieAttrRow(name)
	if !ok || a.flag || a.free {
		return "", false
	}
	// A SHAPE has no canonical spelling: its value is the endpoint's, admitted for
	// its form, and returning it says only that the form was accepted.
	if a.shape != nil {
		if a.shape(value) {
			return value, true
		}
		return "", false
	}
	// ⚠ THE SPELLING THE SPECIFICATION WRITES, not the fold target. An earlier
	// version returned `strings.ToLower(value)` and so declared `lax` canonical --
	// which lengthened the ORDINARY `SameSite=Lax`, the exact capture this vouching
	// exists to keep readable, and two fixtures said so at once. The canonical form
	// is a fact about the grammar; folding is only how a predicate recognises it.
	//
	// A legal but non-canonical spelling is not refused: it is left CAPTURED and
	// redacted like any other value, which is the safe direction and the true
	// statement about who wrote it.
	for _, c := range a.values {
		if strings.EqualFold(value, c) {
			return c, true
		}
	}
	return "", false
}

// maxAgeReadByTheParser reports whether `net/http` reads `value` AS a Max-Age.
//
// Ask net/http whether a Max-Age spelling is accepted. A digits-only
// check rejects the valid -1 deletion value while accepting 007, which
// net/http leaves unparsed rather than interpreting as Max-Age.
//
// `+1`, `-0` and `0` are accepted by the parser too, and `007`, ` 1`, `1x` and
// `--1` are not; writing that rule out here would be a second implementation of
// someone else's grammar, and the edges belong to whoever owns it.
//
// ⚠ AND THE VALUE IS FENCED FIRST. It comes from the endpoint, and a `;` in it
// would end the attribute in the probe string -- `1; HttpOnly` would parse as a
// clean Max-Age plus a flag, and this would vouch bytes the real header never had
// in that slot. The caller has already split on `;`, so this only refuses what
// cannot legally be here.
func maxAgeReadByTheParser(value string) bool {
	if value == "" || strings.ContainsAny(value, ";,\r\n\t \"") {
		return false
	}
	resp := http.Response{Header: http.Header{"Set-Cookie": []string{"sp=x; Max-Age=" + value}}}
	cookies := resp.Cookies()
	if len(cookies) != 1 {
		return false
	}
	for _, u := range cookies[0].Unparsed {
		if strings.HasPrefix(strings.ToLower(u), "max-age=") {
			return false
		}
	}
	return true
}

func cookieAttrVerbatim(name, value string) bool {
	a, ok := cookieAttrRow(name)
	if !ok || a.flag || a.free {
		return false
	}
	if a.shape != nil {
		return a.shape(value)
	}
	for _, v := range a.values {
		if strings.EqualFold(value, v) {
			return true
		}
	}
	return false
}

// unescapeMarks reverses escapeMarks for MEASUREMENT only -- never for output.
// The escape is injective by parity: an odd run of backslashes before `x00` was
// a real marker byte, an even run was literal text.
func unescapeMarks(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			i++
			continue
		}
		j := i
		for j < len(s) && s[j] == '\\' {
			j++
		}
		n := j - i
		rest := s[j:]
		if strings.HasPrefix(rest, "x00") || strings.HasPrefix(rest, "x01") {
			if n%2 == 1 {
				b.WriteString(strings.Repeat(`\`, (n-1)/2))
				b.WriteByte(0)
				i = j + 3
				continue
			}
			b.WriteString(strings.Repeat(`\`, (n-2)/2))
			b.WriteString(rest[:3])
			i = j + 3
			continue
		}
		b.WriteString(strings.Repeat(`\`, n))
		i = j
	}
	return b.String()
}

// asArrived returns a JSON string literal's value as it arrived ON THE WIRE,
// undoing the recorder's own mark escape first.
//
// ⚠ THE BODY IS MEASURED AFTER `escapeMarks` HAS EXPANDED IT. `responseText`
// escapes the whole response before any redaction, and that escape LENGTHENS a
// backslash run standing before the literal text `x00`/`x01` -- so the traversal
// decoded `{"message":"\\x00"}` to a six-character value and published
// `redacted-6-chars` for the four characters the endpoint actually sent. Every
// other measured site -- the header value, the status phrase, the cookie name and
// value -- calls `unescapeMarks` first, so the same value was reported at two
// different lengths depending on where it appeared.
//
// ⚠ AND THE ESCAPE IS UNDONE ON THE SOURCE, NOT ON THE DECODED VALUE. JSON
// decoding has already halved the run by the time the value exists, and the
// parity `unescapeMarks` reads is a property of the SOURCE: on the decoded text
// it reads an even run as odd and reconstructs a marker byte that was never
// there. The expanded spelling is still what the span offsets and the emitted
// text are built from; only the LENGTH is a claim about the wire.
//
// A literal that does not decode after unescaping is measured as it was decoded.
// The escaped document parsed, and unescaping only shortens backslash runs, so
// this is unreachable for anything the traversal reaches -- it is here because a
// silent wrong number is worse than a conservative one.
func asArrived(literal, decoded string) string {
	var s string
	if err := json.Unmarshal([]byte(unescapeMarks(literal)), &s); err != nil {
		return decoded
	}
	return s
}

// canonicalAdmitted is the spelling a field's own vocabulary uses. When the value
// that arrived differs from it, the difference is the endpoint's choice and is not
// vouched for — the value still prints, because the criterion admitted it, but the
// supplied-value scrub is left able to reach it.
// admittedByRegistry says whether this field's value is admitted because a
// REGISTRY names it, as opposed to because its SHAPE fits. The difference decides
// what a collision can be answered with: a registry value has a canonical spelling
// this program can write, and an open grammar has only the endpoint's bytes.
func admittedByRegistry(name string) bool {
	return verbatimHeaders[strings.ToLower(ows(name))].vocabulary
}

// canonicalAdmitted returns the spelling the field's REGISTRY uses for a value the
// predicates admitted, or the value unchanged where the registry does not fold.
//
// ⚠ AND `Allow` DOES NOT FOLD. HTTP method names are case-sensitive, so `get` is
// not the registered `GET` -- lowering it made an endpoint-selected token read as
// the registry's own spelling, and with `get` also supplied it was marked generated
// and passed both the scrub and the guard with no refusal. Folding is a property of each registry,
// not a convenience shared by the list.
func canonicalAdmitted(name, v string) string {
	if verbatimHeaders[strings.ToLower(ows(name))].folds {
		return strings.ToLower(v)
	}
	return v
}

func redactUnlessVerbatim(line string) string {
	cr := ""
	body := line
	if strings.HasSuffix(body, "\r") {
		cr, body = "\r", strings.TrimSuffix(body, "\r")
	}
	// ⚠ A STATUS LINE HAS NO COLON, AND ITS REASON IS ENDPOINT TEXT. Returning it
	// unchanged published `HTTP/1.1 200 server-secret` -- a phrase `DumpResponse`
	// preserves and no supplied-value scrub can see. Version and code are syntax; whatever
	// follows them is not.
	if strings.HasPrefix(body, "HTTP/") {
		f := strings.SplitN(body, " ", 3)
		if len(f) == 3 && f[2] != "" {
			// ⚠ THE REGISTERED PHRASE FOR THAT CODE IS THE VOCABULARY, and Go
			// already carries it. `OK` for 200 is specification-fixed; anything
			// else in that position was written by the endpoint. Lengthening the
			// registered phrase too would have made every capture unreadable and
			// was caught by the fixture for the generated capture notes.
			code, err := strconv.Atoi(f[1])
			if err == nil && f[2] == http.StatusText(code) {
				// ⚠ VOUCHED, NOT MERELY RETURNED. The scrub deliberately skips the
				// status line, but `assertNoLeak` reads an HTTP/1 reason phrase as
				// captured data -- so with a legal experiment key of `OK` an ordinary
				// `HTTP/1.1 200 OK` could NEVER be captured: the guard reported the
				// phrase as a survivor and every run exited 4. HTTP/2 already exempts its
				// synthesised phrase; an exemption honoured by one of two rules is a
				// disagreement, not an exemption.
				return f[0] + " " + f[1] + " " + vouched(f[2]) + cr
			}
			// MEASURED AS RECEIVED, like the generic header, the cookie name and the
			// cookie value: `responseText` expands a marker-like spelling on the way in.
			return f[0] + " " + f[1] + " " + placeholder(unescapeMarks(f[2])) + cr
		}
		return line
	}
	name, value, ok := strings.Cut(body, ":")
	if !ok || !isTokenOnly(ows(name)) {
		return line
	}
	adm, known := verbatimHeaders[strings.ToLower(ows(name))]
	if known && adm.ok(ows(value)) {
		// ⚠ ADMITTED IS VOUCHED FOR. Returning the line unmarked left the value to
		// `scrubSupplied`, so an experiment key equal to an admitted value --
		// `application/json`, or a `Content-Length` of `12` -- came back as a prose
		// placeholder in a field whose grammar does not allow one, approved because
		// the placeholder is generated. The
		// criterion says WHY it prints; marking says who vouches for it.
		// ⚠ VOUCH THE RECOGNISED FORM, NOT THE ARRIVED ONE. The predicates normalise
		// case, so `Content-Type: application/JSON` is admitted -- and marking the RAW
		// span then vouched for a spelling the registry never saw: with a supplied
		// `JSON` both the scrub and the guard skipped the exact identifier and
		// published it. Recognition is about what
		// the value DENOTES; vouching is about the bytes, and the two are only the
		// same when the spelling is canonical.
		// ⚠ AND NOT WHEN THE VALUE COLLIDES WITH A SUPPLIED ONE. Integer syntax
		// constrains the ALPHABET and says nothing about who chose the number, so
		// `Age: 123456` vouched a supplied numeric experiment key and both the scrub
		// and the guard skipped it.
		//
		// Keep the numeric-field exemption, while refusing a collision that
		// cannot preserve numeric grammar. Age: 42 remains an admitted value.
		// A canonical registry token remains grammar even when it equals supplied
		// text; do not replace a registered no-store directive with prose.
		if raw := ows(value); raw == canonicalAdmitted(name, raw) &&
			(scrubSuppliedRaw(raw) == raw || registryOnlyValue(name, raw)) {
			return name + ":" + strings.Replace(value, raw, vouched(raw), 1) + cr
		}
		// ⚠ AND WHERE THE COLLISION FALLS ON A SHAPE, THERE IS NOTHING TO PRINT.
		// `Cache-Control: max-age=123456` against a supplied `123456` is canonical, so
		// the substitution branch below does not fire, and no placeholder is a legal
		// argument for `max-age`: the field's grammar admits DIGITS. That is the
		// analogous header case to the cookie one, and the answer is the file's own --
		// refuse the capture rather than publish a value no parser accepts with an
		// empty ledger.
		if raw := ows(value); raw != "" && raw == canonicalAdmitted(name, raw) &&
			scrubSuppliedRaw(raw) != raw {
			noteStructural(formField, "an admitted header value whose colliding part has no grammar-preserving spelling")
		}
		// ⚠ DECLINING TO VOUCH IS NOT DECLINING TO KEEP THE GRAMMAR. A valid
		// NON-CANONICAL spelling that also collides fell through to the prose scrub:
		// with a supplied `JSON`, `Content-Type: application/JSON` became
		// `application/<redacted, 4 chars>`, which `mime.ParseMediaType` rejects
		// even though what arrived was a valid media type -- an empty ledger and a
		// capture the guard approves.
		//
		// Use token-safe substitution for a non-canonical admitted value that
		// collides with supplied text. Preserve canonical registered media types:
		// TestARecognisedMediaTypeIsGrammar checks that application/json survives
		// a supplied json.
		if raw := ows(value); raw != "" && raw != canonicalAdmitted(name, raw) &&
			scrubSuppliedRaw(raw) != raw {
			// A numeric directive argument must remain numeric regardless of the
			// directive's casing; do not replace it with a length token. Apply this
			// rule only to fields with a directive registry, not to media types.
			if registeredDirectives[strings.ToLower(ows(name))] != nil &&
				!registryOnlyValue(name, strings.ToLower(raw)) {
				noteStructural(formField, "an admitted header value whose colliding part has no grammar-preserving spelling")
				return line
			}
			noteAccounted(formField, "an admitted header value colliding with a supplied value")
			// ⚠ THE COLLIDING TOKEN, NOT THE WHOLE VALUE. Replacing the value outright
			// keeps it parseable and throws away the type: an operator reading
			// `redacted-16-chars` cannot tell a media type from a cache directive.
			// Each token is replaced where it stands, so `application/JSON` becomes
			// `application/redacted-4-chars` -- still a media type, and the part the
			// endpoint did not choose survives.
			// ⚠ AND AN ADMITTED VALUE HAS A SEMANTICS-PRESERVING REPLACEMENT; A LENGTH
			// IS NOT IT. With a supplied `IDENTITY`, `Content-Encoding: IDENTITY` --
			// which this path accepts as readable -- became
			// `Content-Encoding: redacted-8-chars` while the captured body stayed
			// plain, so the published field declared a coding no consumer can apply
			// and the capture contradicted itself. The canonical spelling is THIS program's text, not the
			// endpoint's, and it means what the arrived spelling meant.
			//
			// ⚠ UNLESS THE CANONICAL SPELLING IS ITSELF SUPPLIED. Measured: with
			// `IDENTITY` supplied the guard passes `identity`, and with `identity`
			// supplied it reports a survivor and nothing is publishable -- so the
			// substitution is taken only where the scrub would leave it standing.
			if can := canonicalAdmitted(name, raw); can != raw && scrubSuppliedRaw(can) == can {
				return name + ":" + strings.Replace(value, raw, marked(can), 1) + cr
			}
			out := raw
			for _, v := range suppliedValues {
				out = replaceTokenWith(out, v, tokenPlaceholder(v), isWordByte)
			}
			if out == raw {
				// A long value the token rule does not reach: the whole thing goes.
				out = tokenPlaceholder(raw)
			}
			return name + ":" + strings.Replace(value, raw, out, 1) + cr
		}
		// A numeric field has no prose or token-placeholder replacement that
		// preserves its grammar. If an admitted Age value matches supplied text,
		// withhold the line and record the refusal instead of changing its type.
		if raw := ows(value); raw != "" && !admittedByRegistry(name) && scrubSuppliedRaw(raw) != raw {
			noteStructural(formField, "an admitted header value that collides with a supplied value and has no safe spelling")
			return name + ": " + marked("<withheld>") + cr
		}
		return line
	}
	// ⚠ AN EMPTY VALUE HAS NOTHING TO REDACT, AND A PLACEHOLDER INVENTS ONE.
	// `Content-Encoding:` is transport-valid and declares no coding token; routed
	// through the generic prose rule it was published as
	// `Content-Encoding: <redacted, 0 chars>` with an empty ledger -- an artifact
	// declaring a coding no consumer can apply, about a response that declared none,
	// while the captured body was plain JSON.
	// Redaction exists to replace endpoint bytes; where there are none, the honest
	// rendering is the one that arrived.
	if ows(value) == "" {
		return name + ":" + value + cr
	}
	// ⚠ MEASURED BEFORE OUR OWN ESCAPE. `escapeMarks` lengthens a backslash run
	// on the way in, so a header carrying the literal `\x00` was reported four
	// characters longer than it arrived -- the length describing the recorder's
	// reversible escape rather than the endpoint's bytes.
	return name + ": " + placeholder(unescapeMarks(ows(value))) + cr
}

// authorityIsHostShaped reports whether an authority is the kind of name the host
// exemption's premise is about: one that is publicly resolvable and constrained by
// its grammar. A bracketed literal has already been judged by `parsesAsURI`, which
// checks the IPv6, IPvFuture and zone grammars; this is the non-bracketed form,
// which that predicate accepted unconditionally.
//
// ⚠ RFC 3986 `reg-name` IS NOT THE TEST, though it is the obvious one to reach for.
// It admits the sub-delims `!$&'()*+,;=`, so `host$tok` and `a;b` are legal
// reg-names -- and they are exactly the spellings that carried endpoint text
// through. The exemption does not rest on "is this a legal reg-name"; it rests on
// "is this a name the world resolves", which is LDH: letters, digits and hyphens in
// dot-separated labels, no label beginning or ending in a hyphen. An
// internationalised host reaches this program as punycode, which is LDH.
// splitAuthority separates an authority into its host and its port, for BOTH the
// bracketed and the non-bracketed form.
//
// Validate the port for both bracketed and registered-name authorities.
// A closing bracket does not establish that the following port is valid.
func splitAuthority(a string) (string, string, bool) {
	if i := strings.LastIndexByte(a, ']'); i >= 0 {
		if i+1 < len(a) && a[i+1] == ':' {
			return a[:i+1], a[i+2:], true
		}
		return a, "", false
	}
	if i := strings.LastIndexByte(a, ':'); i >= 0 {
		return a[:i], a[i+1:], true
	}
	return a, "", false
}

// portIsDialable reports whether a port suffix is one a stack could use.
//
// ⚠ AN EMPTY PORT IS LEGAL. RFC 3986 gives `port = *DIGIT`, and `example.com:/cb`
// is a target `net/url` accepts and `parsesAsURI` already calls valid -- rejecting
// it here replaced the whole authority with a length and cost the capture the host
// AND the explicit empty port.
func portIsDialable(port string) bool {
	if port == "" {
		return true
	}
	if !isDigits(port) {
		return false
	}
	n := 0
	for i := 0; i < len(port); i++ {
		n = n*10 + int(port[i]-'0')
		if n > 65535 {
			return false
		}
	}
	return true
}

// authorityIsHostShaped reports whether an authority is the kind of name the host
// exemption's premise is about: one that is publicly resolvable and constrained by
// its grammar.
//
// ⚠ RFC 3986 `reg-name` IS NOT THE TEST, though it is the obvious one to reach for.
// It admits the sub-delims `!$&'()*+,;=`, so `host$tok` and `a;b` are legal
// reg-names -- and they are exactly the spellings that carried endpoint text
// through. The exemption does not rest on "is this a legal reg-name"; it rests on
// "is this a name the world resolves", which is LDH: letters, digits and hyphens in
// dot-separated labels, no label beginning or ending in a hyphen. An
// internationalised host reaches this program as punycode, which is LDH.
//
// ⚠ USERINFO IS NOT THE HOST, and it has its own redaction one pass along. Asking
// the whole authority failed `user:pass@host.example` on the `@` and replaced the
// HOST with it, destroying the target; two scenes said so on the first run.
// afterUserinfo returns the offset in an authority at which the host begins.
//
// The last @ separates userinfo from the host, matching Go's parser and
// redactUserinfo. All authority consumers use this same boundary.
func afterUserinfo(a string) int {
	if i := strings.LastIndexByte(a, '@'); i >= 0 {
		return i + 1
	}
	return 0
}

// hostPortRange narrows an authority's byte range to the host and port.
//
// ⚠ THE EXEMPTION IS THE HOST'S, SO THE COLLISION TEST IS THE HOST'S. Asked of
// the whole authority, a supplied value matching only the USERINFO replaced the
// host along with it: with `user` supplied, `Location: https://user@example.com/cb`
// became `https://redacted-16-chars/redacted-2-chars`, losing the deliberately
// exempt `example.com` AND the fact that userinfo was present at all. Userinfo has its own redactor, which
// produces a placeholder while keeping the host; this range is what the
// authority-level rules may speak about.
func hostPortRange(url string) (int, int, bool) {
	lo, hi, ok := authorityRange(url)
	if !ok {
		return 0, 0, false
	}
	return lo + afterUserinfo(url[lo:hi]), hi, true
}

func authorityIsHostShaped(a string) bool {
	a = a[afterUserinfo(a):]
	host, port, hasPort := splitAuthority(a)
	if hasPort && !portIsDialable(port) {
		return false
	}
	// The bracketed grammars -- IPv6, IPvFuture and the zone -- are parsesAsURI's
	// subject and are judged there; only the port needed asking here.
	if strings.HasPrefix(host, "[") {
		return true
	}
	host = strings.TrimSuffix(host, ".") // a root-anchored FQDN
	// DNS bounds both label and name lengths: each label is 1..63 octets,
	// and a name is at most 253 octets excluding the root label (RFC 1035
	// section 2.3.4). Internationalized names arrive as punycode and use
	// the same byte-length rules.
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
				(c >= '0' && c <= '9') || c == '-') {
				return false
			}
		}
	}
	return true
}

// directiveArgs states, per field, which of ITS directives take an argument and what
// the argument must be. A field absent from this map has an argument-free grammar,
// and a directive absent from its field's entry takes no argument.
//
// ⚠ THIS IS THE SUBJECT, AND THE PREVIOUS TWO ANSWERS WERE NOT. "Numeric arguments
// are off for these fields" describes a KIND of argument; the grammar's rule is
// about the DIRECTIVE. `Cache-Control` is the only field among those admitted here
// whose directives take arguments, and only these take them, each a delta-seconds
// (RFC 9111). `no-cache` and `private` take a quoted field list, which never reaches
// this walk -- a value containing `"` is rejected above.
var directiveArgs = map[string]map[string]func(string) bool{
	"cache-control": {
		"max-age":                isDigits,
		"s-maxage":               isDigits,
		"stale-while-revalidate": isDigits,
		"stale-if-error":         isDigits,
		"min-fresh":              isDigits,
		"max-stale":              isDigits,
	},
}
