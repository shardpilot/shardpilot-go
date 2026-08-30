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

func TestLocationQueryValuesAreRedacted(t *testing.T) {
	suppliedValues = nil
	t.Cleanup(func() { suppliedValues = nil })
	// The harness sent both names, so both are printed; a name it did NOT send is
	// lengthened, which the sweep fixture pins separately.
	noteRequestName("state")
	noteRequestName("token")
	t.Cleanup(func() { requestNames = map[string]bool{} })
	got := stripMarks(dropFraming("HTTP/1.1 302 Found\r\nLocation: /cb?state=abc123&token=zzz\r\n\r\n"))
	for _, leaked := range []string{"abc123", "zzz"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("a server-generated redirect value was published: %q", got)
		}
	}
	if !strings.Contains(got, "state=") || !strings.Contains(got, "token=") {
		t.Fatalf("structural redaction dropped the parameter names: %q", got)
	}
}

func TestCookieLengthIsTheCookiesNotThePlaceholders(t *testing.T) {
	suppliedValues = []string{"abc12345"}
	t.Cleanup(func() { suppliedValues = nil })
	// The cookie value equals a supplied identifier: the structural redactor must
	// see the cookie, not a substitution made before it.
	ex := exchange{head: []byte("HTTP/1.1 200 OK\r\nSet-Cookie: sid=abc12345; Path=/\r\n\r\n")}
	got := responseText(&ex)
	if !strings.Contains(got, "<redacted, 8 chars>") {
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
	if !strings.Contains(got, "a=redacted-1-chars") {
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
	if !strings.Contains(got, "3 chars") {
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
