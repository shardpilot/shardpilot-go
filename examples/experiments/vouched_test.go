package main

import (
	"maps"
	"slices"
	"strings"
	"testing"
)

// TestEveryVouchedTokenSurvivesTheScrub is the sweep this class earned.
//
// ⚠ SIX ROUNDS HAVE NOW PRODUCED ONE DEFECT: this program admits a token because
// it RECOGNISES it — a registered field name, a standard cookie attribute, a
// no-op content coding, a benign JSON member, a registered reason phrase — and
// then returns it as CAPTURED text, so the supplied-value scrub rewrites it into
// a placeholder and publishes a response no parser accepts, which the guard
// approves because a placeholder is generated. Each round I fixed the site I was
// shown; the list of sites is the thing that kept being incomplete.
//
// So the population is not a list here. It is drawn from the program's own
// registries, which means a token admitted by a rule added later is covered by
// this scene the day it is added, without anyone remembering to extend it.
//
// The property: if an admitted token is ALSO a supplied value, the published
// response must still contain it.
func TestEveryVouchedTokenSurvivesTheScrub(t *testing.T) {
	type probe struct {
		what string
		line string
		// tok is SUPPLIED; want is what must still be there afterwards. They are
		// the same string for a token, and different for punctuation: a lone `:`
		// survives inside a field name's own marked span whatever this program
		// does with the delimiter, so a probe that supplies `:` and looks for `:`
		// cannot fail. The delimiter is only observable in its context -- `Age:`
		// (shardpilot/shardpilot-go#85 review).
		tok  string
		want string
	}
	pv := func(what, line, tok string) probe { return probe{what, line, tok, tok} }
	var probes []probe

	// ⚠ ALL OF THEM, IN A FIXED ORDER, AND NOT ONE ARBITRARY ELEMENT. This drew a
	// single name and `break`, so WHICH name it measured depended on Go's map
	// iteration order -- a population of one, chosen freshly each run. It went
	// unnoticed until the order handed it `proxy-authenticate`, a registered name
	// this program also WITHHOLDS, and the row failed on a run that differed from
	// the last only by chance (shardpilot/shardpilot-go#84/#85, stack seam).
	//
	// A withheld field keeps its NAME -- that is the vouching this row is about --
	// but rendered in THIS program's spelling rather than the arrived one, so the
	// name rows compare case-insensitively. The value rows do not.
	for _, name := range slices.Sorted(maps.Keys(registeredFieldNames)) {
		probes = append(probes, pv("registered field name", "HTTP/1.1 200 OK\r\n"+name+": 1\r\n\r\n", name))
	}
	// ⚠ VALUELESS ONLY WHERE VALUELESS IS LEGAL. These rows rendered every
	// standard attribute BARE -- including `Path`, `Max-Age` and `SameSite`, which
	// carry values by specification -- and so asserted that a truncated attribute
	// is vouched, which is the very shape a later finding named as a bypass
	// (shardpilot/shardpilot-go#85 review). A sweep row is an assertion about what
	// SHOULD hold; this one held the defect in place.
	for _, a := range []string{"Secure", "HttpOnly"} {
		if _, ok := canonicalCookieFlag(a); !ok {
			t.Fatalf("the probe list drifted from canonicalCookieFlag: %q", a)
		}
		probes = append(probes, pv("cookie flag", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; "+a+"\r\n\r\n", a))
	}
	for _, av := range [][2]string{{"Path", "/"}, {"Max-Age", "10"}, {"SameSite", "Lax"}} {
		if !standardCookieAttr(av[0]) {
			t.Fatalf("the probe list drifted from standardCookieAttr: %q", av[0])
		}
		probes = append(probes, pv("cookie attribute",
			"HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; "+av[0]+"="+av[1]+"\r\n\r\n", av[0]))
	}
	probes = append(probes, pv("no-op content coding",
		"HTTP/1.1 200 OK\r\nContent-Encoding: identity\r\n\r\n", "identity"))
	for name := range benignTopLevel {
		probes = append(probes, pv("benign JSON member",
			"HTTP/1.1 200 OK\r\n\r\n{\""+name+"\":1}", name))
	}
	probes = append(probes, pv("HTTP/1 reason phrase", "HTTP/1.1 200 OK\r\n\r\n", "OK"))

	// ⚠ AND THE PREDICATES THAT ADMIT VALUES, not only the registries of NAMES.
	// The first version of this sweep drew from name registries alone, so four more
	// sites of the same rule reached the review: a header value `verbatimHeaders`
	// admits, a cookie attribute value `cookieAttrVerbatim` admits, an approved URI
	// scheme, and the field names the structural paths dispatch on. The population
	// is "everything this program admits BECAUSE it recognises it", and half of that
	// is values (shardpilot/shardpilot-go#85 review).
	probes = append(probes,
		pv("admitted header value", "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n", "application/json"),
		// ⚠ NOT `Content-Length`: `dropFraming` REMOVES that field and puts a capture
		// note in its place, so the probe never reached the criterion it was aiming
		// at — it measured a header that does not survive to be measured. `Age` is
		// admitted by the same numeric predicate and does survive.
		pv("admitted header value", "HTTP/1.1 200 OK\r\nAge: 12\r\n\r\n", "12"),
		pv("admitted cookie attribute value", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; SameSite=Lax\r\n\r\n", "Lax"),
		pv("admitted cookie attribute value", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; Max-Age=10\r\n\r\n", "10"),
		pv("approved URI scheme", "HTTP/1.1 200 OK\r\nLocation: https://e.example/cb\r\n\r\n", "https"),
		pv("structural field name", "HTTP/1.1 200 OK\r\nLocation: /cb\r\n\r\n", "Location"),
		pv("structural field name", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x\r\n\r\n", "Set-Cookie"),
	)

	// ⚠ AND THE PUNCTUATION THIS PROGRAM PRESERVES AS SYNTAX. A supplied identifier
	// may legally BE a structural character: an experiment key of `{`, `;` or `..`
	// is a string like any other, and the redactors deliberately keep those bytes —
	// then handed them to the generic scrub, which replaced them with prose
	// placeholders the guard approves, publishing JSON that no longer parses, a
	// cookie whose attribute is no longer separated, and a Location that is no
	// longer parent-relative (shardpilot/shardpilot-go#85 review).
	//
	// Same rule as every row above, arriving on characters instead of tokens: what
	// this program emits as STRUCTURE must be marked as structure.
	probes = append(probes,
		pv("JSON structure", "HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false}", "{"),
		pv("JSON structure", "HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false}", "}"),
		pv("JSON structure", "HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false,\"code\":1}", ","),
		pv("cookie separator", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; Secure\r\n\r\n", ";"),
		pv("URI dot segment", "HTTP/1.1 200 OK\r\nLocation: ../cb\r\n\r\n", ".."),
		pv("URI separators", "HTTP/1.1 200 OK\r\nLocation: /a/b?x=1&y=2\r\n\r\n", "?"),
		pv("URI separators", "HTTP/1.1 200 OK\r\nLocation: /a/b?x=1&y=2\r\n\r\n", "&"),
		// ⚠ THE COLONS, WHICH THE FIRST VERSION OF THIS ROW SET DID NOT NAME. A
		// syntax set's population is the grammar's punctuation, and I had listed the
		// characters I happened to think of (shardpilot/shardpilot-go#85 review).
		pv("JSON structure", "HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false,\"code\":1}", ","),
		pv("JSON structure", "HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false}", ":"),
		pv("JSON structure", "HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false}", "\""),
		// ⚠ SUPPLIES `:` AND LOOKS FOR `Age:`. Two earlier versions of this row could
		// not fail. The first supplied `":"` and looked for `":"`, and an HTTP-date
		// is full of colons the value's own mark protects. The second looked for
		// `"Date:"` -- but the field NAME is marked, so that string is split across a
		// marked span and the scrub never matched it whether the delimiter was
		// vouched or not. Only the pair "supply the character, look for the framing"
		// reaches the delimiter.
		probe{"field delimiter", "HTTP/1.1 200 OK\r\nAge: 12\r\n\r\n", ":", "Age:"},
		pv("scheme delimiter", "HTTP/1.1 200 OK\r\nLocation: https://e.example/cb\r\n\r\n", ":"),
	)

	for _, p := range probes {
		t.Run(p.what+"/"+p.tok, func(t *testing.T) {
			structuralSurfaces = nil
			accountedSurfaces = nil
			receivedConnection = true
			suppliedValues = []string{p.tok}
			t.Cleanup(func() {
				suppliedValues = nil
				structuralSurfaces = nil
				accountedSurfaces = nil
				receivedConnection = false
			})
			red := scrubSupplied(dropFraming(p.line))
			got := stripMarks(red)
			hay, needle := got, p.want
			if p.what == "registered field name" {
				hay, needle = strings.ToLower(got), strings.ToLower(p.want)
			}
			if !strings.Contains(hay, needle) {
				t.Fatalf("a token this program vouched for was rewritten: %q became %q", p.want, got)
			}
			// ⚠ AND THE GUARD MUST AGREE. Surviving the scrub is half the property:
			// a token the scrub deliberately skips can still be reported by
			// `assertNoLeak` as a survivor, and then NOTHING is publishable — which
			// is how a registered reason phrase made an ordinary 200 uncapturable
			// (shardpilot/shardpilot-go#85 review). The scrub and the guard have to
			// hold the same exemption, or the exemption is a disagreement.
			if err := assertNoLeak(asCaptured(red)); err != nil {
				t.Fatalf("the guard reported a vouched-for token as a survivor: %v", err)
			}
		})
	}
}

// TestNoAdmissionVouchesANonCanonicalSpelling is the sweep the CASE half of this
// class earned.
//
// ⚠ THREE ROUNDS, FIVE SITES, ONE QUESTION. Every admission predicate in this
// program folds case, because the protocols do. Every vouching site then marked
// the spelling that ARRIVED — so a supplied identifier differing from an admitted
// token only in case (`LAX`, `HTTPS`, `ASSIGNED`) was marked as this program's own
// syntax and skipped by BOTH the scrub and the guard. The round before repaired
// two sites by comparing raw against DECODED, which catches an escape and says
// nothing about case: the question was never about escapes
// (shardpilot/shardpilot-go#85 review).
//
// The population is drawn from the registries themselves, so a name added to any
// of them is measured here without anyone remembering to come back.
func TestNoAdmissionVouchesANonCanonicalSpelling(t *testing.T) {
	// `look` is what must be ABSENT afterwards, and it is not always `tok`: with
	// `HTTP` supplied, the status line `HTTP/1.1 302 Found` contains that token as
	// protocol syntax this program deliberately vouches, so a bare `Contains(tok)`
	// matched something other than its subject and the row failed on a correct
	// program. The scheme rows look for the token IN SCHEME POSITION.
	type row struct{ what, line, tok, look string }
	var rows []row
	add := func(what, line, tok string) { rows = append(rows, row{what, line, tok, tok}) }
	addAt := func(what, line, tok, look string) { rows = append(rows, row{what, line, tok, look}) }

	for _, n := range slices.Sorted(maps.Keys(benignTopLevel)) {
		u := strings.ToUpper(n)
		if u == n {
			continue
		}
		add("benign JSON member", "HTTP/1.1 200 OK\r\n\r\n{\""+u+"\":1}", u)
	}
	for _, n := range slices.Sorted(maps.Keys(mintedNames)) {
		u := strings.ToUpper(n)
		if u == n {
			continue
		}
		add("minted JSON member", "HTTP/1.1 200 OK\r\n\r\n{\""+u+"\":\"v\"}", u)
	}
	for _, c := range []string{"Lax", "Strict", "None"} {
		if !cookieAttrVerbatim("SameSite", c) {
			t.Fatalf("the probe list drifted from cookieAttrVerbatim: %q", c)
		}
		u := strings.ToUpper(c)
		add("cookie attribute value", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; SameSite="+u+"\r\n\r\n", u)
	}
	for _, sc := range []string{"http", "https"} {
		u := strings.ToUpper(sc)
		addAt("URI scheme", "HTTP/1.1 302 Found\r\nLocation: "+u+"://e.example/cb\r\n\r\n", u, u+"://")
	}

	for _, r := range rows {
		t.Run(r.what+"/"+r.tok, func(t *testing.T) {
			structuralSurfaces, accountedSurfaces = nil, nil
			receivedConnection = true
			suppliedValues = []string{r.tok}
			t.Cleanup(func() {
				suppliedValues = nil
				structuralSurfaces, accountedSurfaces = nil, nil
				receivedConnection = false
			})
			red := scrubSupplied(dropFraming(r.line))
			if got := stripMarks(red); strings.Contains(got, r.look) {
				t.Fatalf("a non-canonical spelling was vouched, so the supplied value survived: %q", got)
			}
			// AND THE GUARD AGREES: whatever the scrub left must not be a survivor.
			if err := assertNoLeak(asCaptured(red)); err != nil {
				t.Fatalf("the guard still found it: %v", err)
			}
		})
	}
}

// TestSuppliedPunctuationDoesNotAlterStructure is the sweep the PUNCTUATION half
// of this class earned, and it is a PRODUCT rather than a list.
//
// ⚠ NINE SITES OVER FOUR ROUNDS, EACH FOUND BY A REVIEWER AND EACH FIXED ALONE.
// The rule has been stated since the first: what this program emits as STRUCTURE
// must be marked as structure, or the supplied-value scrub rewrites it into prose
// the guard approves. Every previous sweep drew its rows from a list of probes I
// wrote, so it could only ever find the sites I had thought of -- the `@` a
// userinfo redaction reconstructs, the colon on `Set-Cookie`'s own early return,
// the comma joining an announced trailer list were all missed the same way
// (shardpilot/shardpilot-go#85 review).
//
// The population here is a PRODUCT: the grammar's punctuation, crossed with the
// structural paths this program renders. The invariant needs no per-row
// expectation and cannot be written too narrowly -- supplying a character must
// not change the rendering AT ALL.
func TestSuppliedPunctuationDoesNotAlterStructure(t *testing.T) {
	paths := []struct{ what, line string }{
		{"target", "HTTP/1.1 302 Found\r\nLocation: https://u@e.example/a/b?x=1&y=2#f\r\n\r\n"},
		{"network-path target", "HTTP/1.1 302 Found\r\nLocation: //u@e.example/a/../b\r\n\r\n"},
		{"cookie", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; Path=/; Secure; SameSite=Lax\r\n\r\n"},
		{"trailer list", "HTTP/1.1 200 OK\r\nTrailer: Date, ETag\r\n\r\n"},
		{"json body", "HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false,\"code\":1}"},
		{"media type", "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n"},
		{"status line", "HTTP/1.1 404 Not Found\r\n\r\n"},
	}
	// ⚠ AND TOKENS THAT SPAN A BOUNDARY, not only single characters. A supplied
	// identifier need not BE the punctuation: `@e` straddles the userinfo
	// separator this program RECONSTRUCTS, and a row of single characters cannot
	// express that (shardpilot/shardpilot-go#85 review). The scrub matches on
	// spellings, so the population is spellings that cross a mark boundary.
	marks := []string{"{", "}", "[", "]", "\"", ",", ":", ";", "=", "&", "?", "#", "/", "@", ".", "..", "-", "//",
		"@e", "://", "=x", "; ", ", ", "/a", "1&", "x=", ":s"}

	render := func(line string, supplied []string) string {
		structuralSurfaces, accountedSurfaces = nil, nil
		receivedConnection = true
		suppliedValues = supplied
		defer func() {
			suppliedValues = nil
			structuralSurfaces, accountedSurfaces = nil, nil
			receivedConnection = false
		}()
		return stripMarks(scrubSupplied(dropFraming(line)))
	}

	for _, p := range paths {
		base := render(p.line, nil)
		for _, m := range marks {
			t.Run(p.what+"/"+m, func(t *testing.T) {
				got := render(p.line, []string{m})
				if got != base {
					t.Fatalf("a supplied %q changed the structure this program emits:\n  without: %q\n  with:    %q", m, base, got)
				}
			})
		}
	}
}
