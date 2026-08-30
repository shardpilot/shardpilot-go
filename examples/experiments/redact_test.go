package main

// Fixtures for structural redaction. They pin the rules a field NAME selects,
// which is the whole of what this change adds; the publication guard's own
// fixtures live beside them in main_test.go and are untouched here.

import (
	"net/http"
	"strings"
	"testing"
)

func TestGuardIgnoresItsOwnPlaceholders(t *testing.T) {
	suppliedValues = []string{"redacted"}
	// The harness sent this parameter, so its NAME is printed; that is what makes
	// the placeholder distinguishable from an endpoint-chosen name at all.
	noteRequestName("experiment_key")
	t.Cleanup(func() { suppliedValues = nil; requestNames = map[string]bool{} })
	// Built by the REAL generators, not by hand: a placeholder is distinguished
	// from captured text by provenance now, so a hand-written copy of one would
	// be testing the wrong thing.
	report := redactQuery("GET /x?experiment_key=redacted HTTP/1.1") +
		"\n" + scrubSupplied("echoed: redacted")
	if err := assertNoLeak(asCaptured(report)); err != nil {
		t.Fatalf("the guard rejected a fully redacted report: %v", err)
	}
	if err := assertNoLeak(asCaptured("experiment_key=redacted&x=1")); err == nil {
		t.Fatal("the guard missed a real occurrence outside a placeholder")
	}
}

func TestQueryLengthIsTheValueNotItsWireSpelling(t *testing.T) {
	got := redactQuery(`GET /x?experiment_key=a%22b HTTP/1.1`)
	if !strings.Contains(got, "redacted-3-chars") {
		t.Fatalf("the encoded length was reported instead of the value's: %q", got)
	}
}

func TestAValueShapedLikeAPlaceholderStillPublishes(t *testing.T) {
	// The mirror of the collision case: a legal key that looks like a placeholder
	// must neither be swallowed by the mask NOR make every real placeholder read
	// as a leak. Both directions in one fixture.
	suppliedValues = []string{"redacted-38-chars"}
	t.Cleanup(func() { suppliedValues = nil })
	if err := assertNoLeak(asCaptured("GET /x?experiment_key=redacted-38-chars HTTP/1.1")); err == nil {
		t.Fatal("the mask swallowed the value it was protecting")
	}
	clean := redactQuery("GET /x?subject_key=ababababababababababababababababababab HTTP/1.1")
	if err := assertNoLeak(asCaptured(clean)); err != nil {
		t.Fatalf("a generated placeholder was read as a leak: %v", err)
	}
}

func TestQueryLengthCountsCharactersNotBytes(t *testing.T) {
	got := redactQuery("GET /x?experiment_key=%C3%A9 HTTP/1.1")
	if !strings.Contains(got, "redacted-1-chars") {
		t.Fatalf("a one-character value was measured in bytes: %q", got)
	}
}

func TestEveryPlaceholderCountsCharacters(t *testing.T) {
	// One measure, one place: this asserts the PROPERTY across every surface
	// that produces a placeholder, so a new one measuring bytes fails here
	// rather than in a review round two days later.
	suppliedValues = []string{"éé"}
	t.Cleanup(func() { suppliedValues = nil })
	for name, got := range map[string]string{
		"value":         stripMarks(scrubSupplied("body éé here")),
		"header name":   stripMarks(scrubHeaderName("X-éé")),
		"query":         stripMarks(redactQuery("GET /x?k=%C3%A9%C3%A9 HTTP/1.1")),
		"cookie":        stripMarks(redactSetCookie("Set-Cookie: s=éé; Path=/")),
		"userinfo":      stripMarks(redactUserinfo("Location: https://éé@h/cb")),
		"fragment":      stripMarks(redactFragment("Location: /cb#éé")),
		"authorization": stripMarks(string(redact([]byte("GET / HTTP/1.1\r\nAuthorization: Bearer éé\r\n\r\n"), nil))),
	} {
		// Two placeholder shapes, one measure: `<redacted, N chars>` and
		// `redacted-N-chars`. The property is the NUMBER, not the spelling.
		if strings.Contains(got, "4 chars") || strings.Contains(got, "4-chars") {
			t.Errorf("%s measured bytes, not characters: %q", name, got)
		}
		if !strings.Contains(got, "2 chars") && !strings.Contains(got, "2-chars") {
			t.Errorf("%s did not report 2 characters: %q", name, got)
		}
	}
}

func TestSetCookieIsRedactedStructurally(t *testing.T) {
	dump := "HTTP/1.1 200 OK\r\n" +
		"Set-Cookie: sid=abc123def456; Path=/; HttpOnly\r\n" +
		"\r\n{}"
	// ⚠ COMPARED STRIPPED, because that is what the report prints. Names this
	// program vouches for are MARKED now, so a raw comparison sees `Path\x01=`
	// and reports a loss that the artifact does not have.
	got := stripMarks(dropFraming(dump))
	if strings.Contains(got, "abc123def456") {
		t.Fatalf("a server-set cookie was published verbatim: %q", got)
	}
	// ⚠ `Path=/` WAS NARROWED TO `Path=`. A cookie path is chosen by the origin
	// (`Path=/reset/<token>` is the shape the finding named), so its value is
	// lengthened like any other free-form string. What this test protects is the
	// attribute STRUCTURE: names kept, valueless attributes untouched.
	for _, keep := range []string{"=", "Path=", "HttpOnly"} {
		if !strings.Contains(got, keep) {
			t.Fatalf("structural redaction dropped %q: %q", keep, got)
		}
	}
}

// The registry answers for the query WE sent. A `Location` is the endpoint's own
// URL, so a name there matching one we sent proves nothing about who chose it --
// and an endpoint is exactly who would choose our name to carry a value past the
// scrub. Both halves in one scene: the same spelling, vouched for on our request
// line and lengthened in the response target.
func TestARequestNameDoesNotVouchForARedirectTarget(t *testing.T) {
	noteRequestName("state")
	t.Cleanup(func() { requestNames = map[string]bool{} })

	ours := stripMarks(redactQuery("GET /cb?state=abc123 HTTP/1.1"))
	if !strings.Contains(ours, "state=") {
		t.Fatalf("the name we sent was not vouched for on our own request line: %q", ours)
	}
	theirs := stripMarks(dropFraming("HTTP/1.1 302 Found\r\nLocation: /cb?state=abc123\r\n\r\n"))
	if strings.Contains(theirs, "state=") {
		t.Fatalf("a request name vouched for an endpoint-chosen name in a target: %q", theirs)
	}
	if strings.Contains(theirs, "abc123") {
		t.Fatalf("the value in the redirect target was published: %q", theirs)
	}
}

func TestLocationQueryValuesAreRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	// ⚠ THE NAMES IN A `Location` ARE THE ENDPOINT'S, WHATEVER WE SENT. This scene
	// registered both as request names and asserted they were printed -- which is
	// the provenance confusion the sibling thread names: the registry answers for
	// the OUTGOING request, and a redirect target is someone else's URL
	// (shardpilot/shardpilot-go#85 review). Both names are lengthened now, and what
	// this scene is really about -- the VALUES not being published -- is unchanged.
	t.Cleanup(func() { requestNames = map[string]bool{} })
	got := stripMarks(dropFraming("HTTP/1.1 302 Found\r\nLocation: /cb?state=abc123&token=zzz\r\n\r\n"))
	for _, leaked := range []string{"abc123", "zzz"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("a server-generated redirect value was published: %q", got)
		}
	}
	if strings.Contains(got, "state=") || strings.Contains(got, "token=") {
		t.Fatalf("an endpoint-chosen query name was published verbatim: %q", got)
	}
}

func TestCookieLengthIsTheCookiesNotThePlaceholders(t *testing.T) {
	suppliedValues = []string{"abc12345"}
	t.Cleanup(func() { suppliedValues = nil })
	// The cookie value equals a supplied identifier: the structural redactor must
	// see the cookie, not a substitution made before it.
	ex := exchange{head: []byte("HTTP/1.1 200 OK\r\nSet-Cookie: sid=abc12345; Path=/\r\n\r\n")}
	got := responseText(&ex)
	if !strings.Contains(got, "redacted-8-chars") {
		t.Fatalf("the cookie's own length was not reported: %q", got)
	}
}

func TestFragmentCredentialsAreRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := dropFraming("HTTP/1.1 302 Found\r\nLocation: https://c.example/cb#access_token=secretvalue\r\n\r\n")
	if strings.Contains(got, "secretvalue") {
		t.Fatalf("a fragment credential was published: %q", got)
	}
	// ⚠ THE NAME IS LENGTHENED HERE, AND THAT IS THE RULE WORKING. `access_token`
	// is a name the ENDPOINT chose -- the harness never sent it -- so provenance
	// puts it on the redacted side. A name the harness DID send stays readable,
	// which TestLocationQueryValuesAreRedacted pins with `state=` and `token=`.
	// The pair of them is the whole rule: origin decides, not the spelling.
	if !strings.Contains(got, "=") || strings.Contains(got, "access_token") {
		t.Fatalf("the fragment lost its structure, or kept an endpoint-chosen name: %q", got)
	}
}

func TestLocationUserinfoIsRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(dropFraming("HTTP/1.1 302 Found\r\nLocation: https://user:secret@e.example/cb\r\n\r\n"))
	if strings.Contains(got, "secret") || strings.Contains(got, "user:") {
		t.Fatalf("userinfo credentials were published: %q", got)
	}
	// ⚠ THIS ASSERTION WAS NARROWED, DELIBERATELY. It used to require
	// `e.example/cb` verbatim, which now contradicts a later finding: a redirect
	// PATH segment is server-generated and can carry a credential
	// (`/reset/<token>`), so segments are redacted too. What this test actually
	// protects -- its own failure message says so -- is that the target is not
	// DESTROYED: the host and the path structure must survive. That is what it
	// asks now, and the segment is checked to be lengthened rather than dropped.
	if !strings.Contains(got, "e.example/") {
		t.Fatalf("the redirect target itself was destroyed: %q", got)
	}
	if !strings.Contains(got, "e.example/redacted-2-chars") {
		t.Fatalf("the path segment was dropped rather than lengthened: %q", got)
	}
}

func TestOpaqueQueryComponentIsRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(dropFraming("HTTP/1.1 302 Found\r\nLocation: /cb?server-secret-token\r\n\r\n"))
	if strings.Contains(got, "server-secret-token") {
		t.Fatalf("a key-only query component was published: %q", got)
	}
}

func TestScrubDoesNotReachInsideGeneratedText(t *testing.T) {
	suppliedValues = []string{"redacted"}
	t.Cleanup(func() { suppliedValues = nil })
	// dropFraming writes a marked placeholder; the general scrub must leave it
	// alone, or the marks nest and the guard reports a survivor that is its own
	// output.
	body := dropFraming("HTTP/1.1 302 Found\r\nLocation: /cb?token=abc\r\n\r\n")
	out := scrubSupplied(body)
	if err := assertNoLeak(asCaptured(out)); err != nil {
		t.Fatalf("the guard flagged its own placeholder: %v (%q)", err, stripMarks(out))
	}
	if strings.Contains(stripMarks(out), "-3-chars") && strings.Contains(stripMarks(out), "chars>-") {
		t.Fatalf("a placeholder was scrubbed inside itself: %q", stripMarks(out))
	}
}

func TestOpaqueFragmentPrefixWithQuestionMarkIsRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(dropFraming(
		"HTTP/1.1 302 Found\r\nLocation: /cb#server-secret?x=y\r\n\r\n"))
	if strings.Contains(got, "server-secret") {
		t.Fatalf("an opaque fragment prefix was published: %q", got)
	}
}

func TestTrailersGetStructuralRedaction(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	tee := &teeBody{trailer: http.Header{
		"Set-Cookie": []string{"sid=server-secret"},
		"Location":   []string{"/cb?state=server-state"},
	}}
	e := exchange{head: []byte("x"), captured: tee}
	got := stripMarks(e.trailerReport())
	for _, leak := range []string{"server-secret", "server-state"} {
		if strings.Contains(got, leak) {
			t.Errorf("a trailer published %q: %q", leak, got)
		}
	}
}

func TestServerMintedSubjectFactKeyIsRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	// ⚠ THE VALUE REPEATS A SHORT PATTERN, AND THAT IS LOAD-BEARING. A fixture
	// needs a value's SHAPE -- here `sfk1_` and sixteen following characters --
	// never its randomness: gitleaks' `generic-api-key` fires on a key-like NAME
	// followed by a value with enough entropy, and this file has now reddened the
	// whole repository twice over exactly that. Length is kept so the
	// length-preserving placeholder still has something to preserve; entropy is
	// not (shardpilot/shardpilot-go#73 review).
	body := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n" +
		`{"assigned":true,"subject_fact_key":"sfk1_abababababababab"}`
	got := stripMarks(dropFraming(body))
	if strings.Contains(got, "sfk1_abababababababab") {
		t.Fatalf("the server-minted subject key was published: %q", got)
	}
}

// A Location carrying BOTH a query and a fragment is where the composition
// order is load-bearing on its own. Composed the other way round, redactQuery
// cuts at the first `?`, swallows the whole `#fragment` into the last parameter
// VALUE and reports one length for the two of them -- the fragment vanishes as a
// structure and the number printed describes neither part. The `?`-opaque rule
// cannot see this: there is no `?` inside the fragment to make it opaque.
func TestQueryAndFragmentAreMeasuredSeparately(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	noteRequestName("a")
	t.Cleanup(func() { requestNames = map[string]bool{} })
	got := stripMarks(dropFraming(
		"HTTP/1.1 302 Found\r\nLocation: /cb?a=b#frag-only\r\n\r\n"))
	// The NAME is the endpoint's here and is lengthened with the value; what this
	// scene pins is that the query value and the fragment are measured SEPARATELY,
	// which the two placeholders still show.
	if !strings.Contains(got, "redacted-1-chars=redacted-1-chars") {
		t.Errorf("the query value was not measured on its own: %q", got)
	}
	if !strings.Contains(got, "#redacted-9-chars") {
		t.Errorf("the fragment was not redacted as a fragment: %q", got)
	}
	if strings.Contains(got, "frag-only") {
		t.Errorf("the fragment was published: %q", got)
	}
}

// ---- round on 45eb421: eight findings ----

func TestMintedKeyIsRedactedAcrossALineBreak(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	body := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n" +
		"{\"assigned\":true,\"subject_fact_key\":\n\"sfk1_abababababababab\"}"
	got := stripMarks(dropFraming(body))
	if strings.Contains(got, "sfk1_abababababababab") {
		t.Fatalf("a minted key split across lines was published: %q", got)
	}
}

func TestMintedKeyPlaceholderIsNotDoubleMarked(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	body := "HTTP/1.1 200 OK\r\n\r\n" + `{"subject_fact_key":"sfk1_abababababababab"}`
	out := scrubSupplied(dropFraming(body))
	if strings.Contains(stripMarks(out), "chars>,") {
		t.Fatalf("the placeholder was rewritten inside itself: %q", stripMarks(out))
	}
	if strings.Contains(out, genMark+genMark) {
		t.Fatalf("adjacent nested provenance marks: %q", out)
	}
}

func TestRedirectPathSegmentsAreRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(dropFraming(
		"HTTP/1.1 302 Found\r\nLocation: https://e.example/reset/server-secret\r\n\r\n"))
	if strings.Contains(got, "server-secret") {
		t.Fatalf("a credential in a redirect path was published: %q", got)
	}
	if !strings.Contains(got, "e.example/") {
		t.Fatalf("the host was destroyed with it: %q", got)
	}
}

func TestRedirectPathSegmentIsMeasuredDecoded(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(dropFraming(
		"HTTP/1.1 302 Found\r\nLocation: https://e.example/%C3%A9\r\n\r\n"))
	if !strings.Contains(got, "redacted-1-chars") {
		t.Fatalf("a percent-encoded path segment was measured on the wire: %q", got)
	}
}

func TestMintedKeyMatchStopsAtAnUnescapedQuote(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	body := "HTTP/1.1 200 OK\r\n\r\n" + `{"subject_fact_key":"abc\"server-secret"}`
	got := stripMarks(dropFraming(body))
	if strings.Contains(got, "server-secret") {
		t.Fatalf("an escaped quote ended the match early and published the rest: %q", got)
	}
	if strings.Count(got, `"`)%2 != 0 {
		t.Fatalf("the recorded JSON was left unbalanced: %q", got)
	}
}

func TestMintedKeyIsFoundThroughAnEscapedMemberName(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	// `\u005f` IS `_`: the same field, a different spelling of its NAME. The
	// first version of this fixture wrote the plain name and therefore tested
	// what was already passing -- the mutant survived and said so.
	body := "HTTP/1.1 200 OK\r\n\r\n" + `{"subject\u005ffact_key":"sfk1_abababababababab"}`
	got := stripMarks(dropFraming(body))
	if strings.Contains(got, "sfk1_abababababababab") {
		t.Fatalf("an escaped member name hid a minted key: %q", got)
	}
}

func TestTrailerDispatchesOnTheOriginalFieldName(t *testing.T) {
	for _, v := range []string{"cookie", "location"} {
		suppliedValues = []string{v}
		tee := &teeBody{trailer: http.Header{
			"Set-Cookie": []string{"sid=server-secret"},
			"Location":   []string{"/cb?state=server-state"},
		}}
		e := exchange{head: []byte("x"), captured: tee}
		got := stripMarks(e.trailerReport())
		for _, leak := range []string{"server-secret", "server-state"} {
			if strings.Contains(got, leak) {
				t.Errorf("supplied %q made the name scrub bypass structural redaction, publishing %q: %q", v, leak, got)
			}
		}
		suppliedValues = nil
	}
}

// ---- the seam: what these rules do not recognise must still be refused ----

func TestUnrecognisedStructuralShapesAreRefusedNotPublished(t *testing.T) {
	for _, c := range []struct{ name, dump, leak string }{
		{"opaque Set-Cookie",
			"HTTP/1.1 200 OK\r\nSet-Cookie: server-secret\r\n\r\n", "server-secret"},
		{"malformed Location target",
			"HTTP/1.1 302 Found\r\nLocation: /cb?state=x server-secret\r\n\r\n", "server-secret"},
		{"minted field with a non-string value",
			"HTTP/1.1 200 OK\r\n\r\n" + `{"subject_fact_key":{"token":"server-secret"}}`, "server-secret"},
	} {
		structuralSurfaces = nil
		suppliedValues = nil
		got := stripMarks(dropFraming(c.dump))
		if len(structuralSurfaces) == 0 {
			t.Errorf("%s: a shape these rules do not cover was accepted: %q", c.name, got)
		}
		if strings.Contains(got, c.leak) {
			t.Errorf("%s: the server-generated value was published: %q", c.name, got)
		}
		structuralSurfaces = nil
	}
}

func TestMintedMemberNamesAreMatchedCaseInsensitively(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	got := stripMarks(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n" + `{"SUBJECT_FACT_KEY":"sfk1_abababababab"}`))
	if strings.Contains(got, "sfk1_abababababab") {
		t.Fatalf("a case variant of a minted name was published: %q", got)
	}
}

func TestQuotedCookieValueIsMeasuredWithoutItsDelimiters(t *testing.T) {
	suppliedValues = nil
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\nSet-Cookie: sid=\"abc\"\r\n\r\n"))
	if !strings.Contains(got, "redacted-3-chars") {
		t.Fatalf("the quotes were counted as value: %q", got)
	}
}

func TestOpaqueQueryComponentIsMeasuredDecoded(t *testing.T) {
	suppliedValues = nil
	structuralSurfaces = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	got := stripMarks(dropFraming("HTTP/1.1 302 Found\r\nLocation: /cb?%C3%A9\r\n\r\n"))
	if !strings.Contains(got, "redacted-1-chars") {
		t.Fatalf("an opaque component was measured on its wire spelling: %q", got)
	}
}

// The host is exempt from REDACTION; that never meant the generic scrub may
// corrupt it.
func TestASuppliedValueDoesNotCorruptTheExemptAuthority(t *testing.T) {
	suppliedValues = []string{"example"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 302 Found\r\nLocation: https://e.example/cb\r\n\r\n{\"assigned\":false}")))
	if strings.Contains(got, "example") {
		t.Fatalf("the supplied value survived in the authority: %q", got)
	}
	if strings.ContainsAny(strings.SplitN(strings.SplitN(got, "Location: ", 2)[1], "\r", 2)[0], " <>,") {
		t.Fatalf("the authority was rewritten into prose: %q", got)
	}
	// AND AN AUTHORITY THAT COLLIDES WITH NOTHING IS UNTOUCHED.
	suppliedValues = nil
	if plain := stripMarks(dropFraming(
		"HTTP/1.1 302 Found\r\nLocation: https://e.example/cb\r\n\r\n{\"assigned\":false}")); !strings.Contains(plain, "e.example") {
		t.Fatalf("an ordinary authority was replaced: %q", plain)
	}
}

// Replacing the authority is not finishing the target.
func TestTheTargetPipelineContinuesAfterTheAuthorityIsReplaced(t *testing.T) {
	suppliedValues = []string{"example"}
	t.Cleanup(func() { suppliedValues = nil })
	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 302 Found\r\nLocation: https://e.example/cb?state=server-secret-token\r\n\r\n{\"assigned\":false}")))
	if strings.Contains(got, "server-secret-token") {
		t.Fatalf("the query was left unredacted after the authority was replaced: %q", got)
	}
	if strings.Contains(got, "example") {
		t.Fatalf("the supplied value survived in the authority: %q", got)
	}
}

// TestARegistryTokenSurvivesACollision: recognition is about what a value
// DENOTES, and a directive list denotes members of a closed registry. A supplied
// value that happens to equal one does not make the endpoint the author of it --
// the registered media type already had this ruling, and the directive lists did
// not: `Cache-Control: no-store` came back as `<redacted, 8 chars>`, which is not
// a cache-directive token, with an EMPTY refusal ledger
// (shardpilot/shardpilot-go#85 review).
func TestARegistryTokenSurvivesACollision(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces = nil, nil })

	suppliedValues, structuralSurfaces = []string{"no-store"}, nil
	got := stripMarks(scrubSupplied(redactUnlessVerbatim("Cache-Control: no-store")))
	if !strings.Contains(got, "no-store") {
		t.Fatalf("a registry token was lost to a collision with a supplied value: %q", got)
	}
	if len(structuralSurfaces) != 0 {
		t.Fatalf("a registry token was refused rather than vouched: %v", structuralSurfaces)
	}
	// ⚠ AND A NUMBER IS NOT A REGISTRY TOKEN. `max-age` admits DIGITS, so the
	// alphabet says nothing about who chose them and no placeholder is a legal
	// argument -- the capture is refused rather than published malformed.
	suppliedValues, structuralSurfaces = []string{"123456"}, nil
	got = stripMarks(scrubSupplied(redactUnlessVerbatim("Cache-Control: max-age=123456")))
	if strings.Contains(got, "max-age=123456") {
		t.Fatalf("a supplied number was vouched by the shape that admitted it: %q", got)
	}
	if len(structuralSurfaces) == 0 {
		t.Fatalf("a value no parser accepts was published with an empty ledger: %q", got)
	}
}

// TestAShapeAdmittedCookieAttributeIsRefusedOnCollision is the cookie half of the
// same rule. `Max-Age` is an integer and `Expires` an HTTP-date: admitted by
// SHAPE, and a shape says nothing about who chose the value. With a supplied
// `123456` the fallback answered `Max-Age=redacted-6-chars`, which is not an
// integer, and recorded nothing -- so the guard approved a cookie no parser
// accepts (shardpilot/shardpilot-go#85 review).
func TestAShapeAdmittedCookieAttributeIsRefusedOnCollision(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces = nil, nil })

	suppliedValues, structuralSurfaces = []string{"123456"}, nil
	got := stripMarks(scrubSupplied(redactSetCookie("Set-Cookie: sid=x; Max-Age=123456")))
	if len(structuralSurfaces) == 0 {
		t.Fatalf("a cookie attribute no parser accepts was published with an empty ledger: %q", got)
	}
	// ⚠ AND AN ENUMERATED ATTRIBUTE IS DIFFERENT IN KIND. The specification LISTS
	// `SameSite`'s values, so `Lax` is the grammar's own token whoever else also
	// chose that string -- refusing it too would be the guard forbidding what it
	// should allow, and three sweep rows say so.
	suppliedValues, structuralSurfaces = []string{"Lax"}, nil
	got = stripMarks(scrubSupplied(redactSetCookie("Set-Cookie: sid=x; SameSite=Lax")))
	if !strings.Contains(got, "SameSite=Lax") {
		t.Fatalf("an enumerated attribute value was lost to a collision: %q", got)
	}
	if len(structuralSurfaces) != 0 {
		t.Fatalf("an enumerated attribute value was refused: %v", structuralSurfaces)
	}
}

// TestAnEchoIsComparedWithItsOwnRequestValue: `expEchoMatches` asks whether a
// present echoed member carries the value THIS request put in that slot. A flat
// membership test cannot ask that, so an endpoint returning the app key in
// `experiment_key` passed it — a body the SDK rejects — and `reason` was vouched
// as this SDK's taxonomy, publishing a supplied identifier with an empty ledger
// (shardpilot/shardpilot-go#85 review).
func TestAnEchoIsComparedWithItsOwnRequestValue(t *testing.T) {
	t.Cleanup(func() {
		suppliedValues = nil
		requestedAppKey, requestedEnvKey, requestedExpKey = "", "", ""
	})
	requestedAppKey, requestedExpKey = "app", "kill_switch"
	suppliedValues = []string{"app", "kill_switch"}

	got := stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false,\"experiment_key\":\"app\",\"reason\":\"kill_switch\"}")))
	if strings.Contains(got, "kill_switch") {
		t.Fatalf("a cross-slot echo vouched a body the SDK rejects: %q", got)
	}
	// ⚠ AND THE RIGHT VALUE IN THE RIGHT SLOT STILL VOUCHES, or the repair is a
	// refusal to vouch any body that echoes anything.
	got = stripMarks(scrubSupplied(dropFraming(
		"HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false,\"experiment_key\":\"kill_switch\",\"reason\":\"kill_switch\"}")))
	if !strings.Contains(got, "kill_switch") {
		t.Fatalf("a correctly echoed member lost the SDK's own classification: %q", got)
	}
}

// TestAnOverLimitBodyIsNotThisSDKsVerdict: `parseExperimentVerdict` refuses a body
// over `expMaxBodyBytes` BEFORE decoding, so a well-formed verdict past the ceiling
// is not this SDK's classification at all — while the decode here succeeded and
// vouched `reason` (shardpilot/shardpilot-go#85 review).
func TestAnOverLimitBodyIsNotThisSDKsVerdict(t *testing.T) {
	t.Cleanup(func() { suppliedValues = nil })
	suppliedValues = []string{"kill_switch"}
	verdict := `{"assigned":false,"reason":"kill_switch"}`

	over := verdict + strings.Repeat(" ", sdkMaxBodyBytes)
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + over))); strings.Contains(got, "kill_switch") {
		t.Fatalf("an over-limit body vouched its reason: %q", got[:160])
	}
	// ⚠ AND A BODY THE SDK WOULD READ STILL VOUCHES.
	under := verdict + strings.Repeat(" ", 4096)
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + under))); !strings.Contains(got, "kill_switch") {
		t.Fatalf("a body within the SDK's ceiling lost its classification: %q", got[:160])
	}
}

// TestVouchingCarriesEverySDKPrecondition: the SDK refuses a response before
// decoding for THREE reasons — the status, the size, and incompleteness — and a
// vouch claims it would have parsed. Each of the three was missing from one of the
// two vouching decisions at some point in this round
// (shardpilot/shardpilot-go#85 review).
func TestVouchingCarriesEverySDKPrecondition(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces = nil, nil; capturedIncomplete = false })
	verdict := "{\"assigned\":false,\"reason\":\"kill_switch\"}"

	// ⚠ THE STATUS. A non-200 is classified from its status alone; there is no
	// assignment verdict in the body to have a taxonomy.
	suppliedValues, capturedIncomplete = []string{"kill_switch"}, false
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 404 Not Found\r\n\r\n" + verdict))); strings.Contains(got, "kill_switch") {
		t.Errorf("a 404 body vouched its reason: %q", got)
	}
	// ⚠ INCOMPLETENESS, WHICH IS NOT A LENGTH. A body under the ceiling whose read
	// ended short is refused before decoding.
	suppliedValues, capturedIncomplete = []string{"kill_switch"}, true
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + verdict))); strings.Contains(got, "kill_switch") {
		t.Errorf("an incomplete body vouched its reason: %q", got)
	}
	// ...and the same fact reaches the exemption registry.
	suppliedValues, capturedIncomplete = []string{"assigned"}, true
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n{\"assigned\":false}"))); strings.Contains(got, "assigned") {
		t.Errorf("an incomplete body kept its schema exemptions: %q", got)
	}
	// ⚠ AND A COMPLETE 200 STILL VOUCHES, or the preconditions have become a refusal
	// to vouch anything at all.
	suppliedValues, capturedIncomplete = []string{"kill_switch"}, false
	if got := stripMarks(scrubSupplied(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + verdict))); !strings.Contains(got, "kill_switch") {
		t.Errorf("a complete 200 lost the SDK's own classification: %q", got)
	}
}

// TestAllowIsNotFolded: HTTP method names are case-sensitive, so `get` is not the
// registered `GET`. Folding it made an endpoint-selected token read as the
// registry's own spelling (shardpilot/shardpilot-go#85 review).
func TestAllowIsNotFolded(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces = nil, nil })
	suppliedValues, structuralSurfaces = []string{"get"}, nil
	if got := stripMarks(scrubSupplied(redactUnlessVerbatim("Allow: get"))); strings.Contains(got, "Allow: get") {
		t.Errorf("a lowercase method was vouched as the registered one: %q", got)
	}
	// ⚠ AND THE REGISTERED SPELLING STILL SURVIVES A COLLISION.
	suppliedValues, structuralSurfaces = []string{"GET"}, nil
	if got := stripMarks(scrubSupplied(redactUnlessVerbatim("Allow: GET"))); !strings.Contains(got, "Allow: GET") {
		t.Errorf("the registered method was lost: %q", got)
	}
}

// TestANumericArgumentIsAShapeWhateverTheCasing: this branch fires on a
// NON-canonical spelling, and `MAX-AGE=123456` is non-canonical — so a colliding
// number reached the token substitution and came back as `MAX-AGE=redacted-6-chars`,
// which is not an integer, with the ledger recording an accounted rewrite rather
// than a refusal (shardpilot/shardpilot-go#85 review).
func TestANumericArgumentIsAShapeWhateverTheCasing(t *testing.T) {
	t.Cleanup(func() { suppliedValues, structuralSurfaces = nil, nil })
	suppliedValues, structuralSurfaces = []string{"123456"}, nil
	got := stripMarks(scrubSupplied(redactUnlessVerbatim("Cache-Control: MAX-AGE=123456")))
	if len(structuralSurfaces) == 0 {
		t.Errorf("a directive no parser accepts was published with an empty ledger: %q", got)
	}
	// ⚠ AND A NON-CANONICAL SPELLING WHOSE TOKENS ARE ALL REGISTRY MEMBERS still
	// takes the substitution rather than a refusal.
	suppliedValues, structuralSurfaces = []string{"STORE"}, nil
	if got := stripMarks(scrubSupplied(redactUnlessVerbatim("Cache-Control: NO-STORE"))); len(structuralSurfaces) != 0 {
		t.Errorf("a registry token in a non-canonical spelling was refused: %q %v", got, structuralSurfaces)
	}
}
