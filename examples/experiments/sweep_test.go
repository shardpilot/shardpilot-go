package main

import (
	"net/http"
	"strings"
	"testing"
)

// The sweep. For every structural surface these rules RECOGNISE, what happens to
// a form they do NOT recognise?
//
// ⚠ IT IS A FIXTURE, NOT A REPORT, AND THAT IS THE WHOLE POINT. Written as a
// survey that logs its findings, it was a placebo: it told me seven of ten forms
// were published and let the suite pass. The rule it enforces — a header value
// is printed as received only if it passes the criterion in redact.go — is prose
// everywhere else, and prose composes silently. A field added next month that
// the criterion does not cover fails HERE, in the run that adds it, instead of
// in a review round some weeks later.
//
// Each case carries SRVGEN, standing for a value the ENDPOINT generated. It is
// absent from suppliedValues by construction, so the publication guard cannot
// see it: if it reaches the artifact, nothing downstream will catch it.
func TestNoUnrecognisedFormPublishesServerGeneratedText(t *testing.T) {
	for _, c := range []struct{ name, dump string }{
		{"Location with no space after the colon", "HTTP/1.1 302 Found\r\nLocation:/cb/SRVGEN\r\n\r\n"},
		{"userinfo containing a slash", "HTTP/1.1 302 Found\r\nLocation: https://a/SRVGEN@e.example/cb\r\n\r\n"},
		{"Set-Cookie attribute value", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; Domain=SRVGEN\r\n\r\n"},
		{"Content-Location", "HTTP/1.1 200 OK\r\nContent-Location: /r/SRVGEN\r\n\r\n"},
		{"Refresh", "HTTP/1.1 200 OK\r\nRefresh: 0; url=/cb?t=SRVGEN\r\n\r\n"},
		{"Link", "HTTP/1.1 200 OK\r\nLink: </r/SRVGEN>; rel=next\r\n\r\n"},
		{"WWW-Authenticate", "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer realm=\"SRVGEN\"\r\n\r\n"},
		{"Set-Cookie2", "HTTP/1.1 200 OK\r\nSet-Cookie2: sid=SRVGEN\r\n\r\n"},
		{"a second Location header", "HTTP/1.1 302 Found\r\nLocation: /a\r\nLocation: /b/SRVGEN\r\n\r\n"},
		{"minted key nested in an array", "HTTP/1.1 200 OK\r\n\r\n{\"a\":[{\"subject_fact_key\":\"SRVGEN\"}]}"},
		{"Content-Type boundary parameter", "HTTP/1.1 200 OK\r\nContent-Type: multipart/form-data; boundary=SRVGEN\r\n\r\n"},
		{"Server banner", "HTTP/1.1 200 OK\r\nServer: nginx/SRVGEN\r\n\r\n"},
		{"ETag", "HTTP/1.1 200 OK\r\nETag: \"SRVGEN\"\r\n\r\n"},
		{"an unregistered X- field", "HTTP/1.1 200 OK\r\nX-Request-Id: SRVGEN\r\n\r\n"},
		{"an extension directive in a known field", "HTTP/1.1 200 OK\r\nCache-Control: x-debug=SRVGEN\r\n\r\n"},
		// ⚠ REGISTERED NAME, FREE-FORM ARGUMENT. The case above has an unknown
		// directive NAME and is caught by that check alone -- a mutant removing
		// the ARGUMENT check survived it. This one names a real directive and
		// puts the string in its argument, which is the only thing that check
		// stands between.
		{"a free-form argument to a registered directive", "HTTP/1.1 200 OK\r\nCache-Control: max-age=SRVGEN\r\n\r\n"},
		{"an unregistered Vary field name", "HTTP/1.1 200 OK\r\nVary: X-SRVGEN\r\n\r\n"},
		{"an unregistered content coding", "HTTP/1.1 200 OK\r\nContent-Encoding: SRVGEN\r\n\r\n"},
		{"a valueless cookie extension attribute", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; SRVGEN\r\n\r\n"},
		{"an unapproved redirect scheme", "HTTP/1.1 302 Found\r\nLocation: SRVGEN://e.example/cb\r\n\r\n"},
		{"an authority Go's parser rejects", "HTTP/1.1 302 Found\r\nLocation: https://e.example\\SRVGEN/cb\r\n\r\n"},
		{"a cookie extension attribute NAME", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; SRVGEN=y\r\n\r\n"},
		{"an unregistered media type", "HTTP/1.1 200 OK\r\nContent-Type: application/SRVGEN\r\n\r\n"},
		{"an unregistered response field NAME", "HTTP/1.1 200 OK\r\nSRVGEN-Field: x\r\n\r\n"},
		{"a custom status reason phrase", "HTTP/1.1 200 SRVGEN\r\nSet-Cookie: a=b\r\n\r\n"},
		{"userinfo before a second at-sign", "HTTP/1.1 302 Found\r\nLocation: https://user@SRVGEN@host/cb\r\n\r\n"},
		{"an unregistered trailer NAME", "HTTP/1.1 200 OK\r\nTrailer: SRVGEN-Late\r\n\r\n"},
		{"a server-minted assignment key", "HTTP/1.1 200 OK\r\n\r\n{\"assignment_key\":\"SRVGEN\"}"},
		{"a minted member in a truncated body", "HTTP/1.1 200 OK\r\n\r\n{\"subject_fact_key\":\"SRVGEN"},

		{"identifier as a query parameter NAME", "HTTP/1.1 302 Found\r\nLocation: /cb?SRVGEN=x\r\n\r\n"},
		{"identifier as a fragment parameter NAME", "HTTP/1.1 302 Found\r\nLocation: /cb#SRVGEN=x\r\n\r\n"},
		{"identifier as the cookie NAME", "HTTP/1.1 200 OK\r\nSet-Cookie: SRVGEN=x\r\n\r\n"},
		// ⚠ ONE CASE IS DELIBERATELY ABSENT, and its absence is a decision rather
		// than an oversight: an identifier in the redirect HOST. The host is
		// structurally constrained, publicly resolvable, and the first thing a
		// reader looks for -- hiding it hides the subject of the capture. The
		// name-side rule covers parameter and cookie names because those are
		// unbounded strings the endpoint invents; a host is not.
	} {
		structuralSurfaces = nil
		suppliedValues = nil
		got := stripMarks(dropFraming(c.dump))
		if strings.Contains(got, "SRVGEN") && len(structuralSurfaces) == 0 {
			t.Errorf("%s: server-generated text was published and not refused: %q", c.name, got)
		}
		structuralSurfaces = nil
	}
}

// The other half of the criterion: it must still let through what the
// specification's own vocabulary fixes, or the artifact stops being readable and
// the rule gets relaxed by whoever needs to read one.
func TestCriterionStillPublishesFixedVocabularyFields(t *testing.T) {
	for _, c := range []string{
		"Date: Mon, 02 Jan 2006 15:04:05 GMT",
		"Content-Type: application/json",
		"Cache-Control: no-cache, max-age=600",
		"Vary: Accept-Encoding",
		"Age: 42",
	} {
		suppliedValues = nil
		got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\n" + c + "\r\n\r\n"))
		if !strings.Contains(got, c) {
			t.Errorf("the criterion lengthened a fixed-vocabulary field: %q -> %q", c, got)
		}
	}
}

// The sweep, extended to trailers: a trailer is a header that arrived late, and
// the criterion must reach it.
func TestTrailersObeyTheSameCriterion(t *testing.T) {
	for _, k := range []string{"X-Request-Id", "Set-Cookie", "Location", "Server"} {
		structuralSurfaces = nil
		suppliedValues = nil
		tee := &teeBody{trailer: http.Header{k: []string{"SRVGEN"}}}
		e := exchange{head: []byte("x"), captured: tee}
		got := stripMarks(e.trailerReport())
		if strings.Contains(got, "SRVGEN") && len(structuralSurfaces) == 0 {
			t.Errorf("%s trailer published server-generated text: %q", k, got)
		}
		structuralSurfaces = nil
	}
}

// And a minted member name spelled with a character encoding/json folds but
// strings.ToLower does not.
func TestMintedNamesFoldTheWayEncodingJSONDoes(t *testing.T) {
	structuralSurfaces = nil
	suppliedValues = nil
	t.Cleanup(func() { structuralSurfaces = nil })
	got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\n\r\n" + "{\"ſubject_fact_key\":\"SRVGEN\"}"))
	if strings.Contains(got, "SRVGEN") && len(structuralSurfaces) == 0 {
		t.Fatalf("a Unicode-folded minted name was published: %q", got)
	}
}

// The other side of the provenance split, and of the structural dispatch: what
// must NOT be lost. Written beside the sweep because a rule that only ever
// tightens ends as a rule nobody runs.
func TestStructureSurvivesWhereItIsVouchedFor(t *testing.T) {
	t.Run("a structural trailer keeps its shape", func(t *testing.T) {
		suppliedValues = nil
		structuralSurfaces = nil
		t.Cleanup(func() { structuralSurfaces = nil })
		tee := &teeBody{trailer: http.Header{"Set-Cookie": []string{"sid=abcdefgh; Path=/x"}}}
		e := exchange{head: []byte("x"), captured: tee}
		got := stripMarks(e.trailerReport())
		if !strings.Contains(got, "Path=") {
			t.Fatalf("the generic fallback flattened a structurally redacted trailer: %q", got)
		}
	})
	t.Run("a name the harness sent survives its wire spelling", func(t *testing.T) {
		suppliedValues = nil
		noteRequestName("custom_attribute_é")
		t.Cleanup(func() { requestNames = map[string]bool{} })
		got := stripMarks(dropFraming(
			"HTTP/1.1 302 Found\r\nLocation: /cb?custom_attribute_%C3%A9=v\r\n\r\n"))
		if !strings.Contains(got, "custom_attribute_%C3%A9=") {
			t.Fatalf("a harness-owned name was classified as endpoint-chosen: %q", got)
		}
	})
}

// More of what must NOT be lost. Every clause of the criterion that ADMITS
// something needs a case here, or the next tightening removes it silently.
func TestTheCriterionStillAdmitsWhatItShould(t *testing.T) {
	for _, c := range []struct{ name, dump, keep string }{
		{"a registered reason phrase", "HTTP/1.1 200 OK\r\n\r\n", "200 OK"},
		{"a bracketed IPv6 authority", "HTTP/1.1 302 Found\r\nLocation: https://[2001:db8::1]/cb\r\n\r\n", "[2001:db8::1]"},
		{"a relative path containing ://", "HTTP/1.1 302 Found\r\nLocation: /cb/http://e/x\r\n\r\n", "/"},
		{"an interior at-sign in a path", "HTTP/1.1 302 Found\r\nLocation: /a//foo@bar/b\r\n\r\n", "/"},
		{"a GMT HTTP-date", "HTTP/1.1 200 OK\r\nDate: Mon, 02 Jan 2006 15:04:05 GMT\r\n\r\n", "15:04:05 GMT"},
	} {
		suppliedValues = nil
		structuralSurfaces = nil
		got := stripMarks(dropFraming(c.dump))
		if !strings.Contains(got, c.keep) {
			t.Errorf("%s: the criterion refused what it admits: %q", c.name, got)
		}
		if len(structuralSurfaces) != 0 {
			t.Errorf("%s: a valid capture was made unpublishable: %v", c.name, structuralSurfaces)
		}
		structuralSurfaces = nil
	}
}

// Three clauses cannot carry the SRVGEN marker in the position that matters — a
// date zone is three letters, a trailer NAME is checked by a different path, and
// the attribute-name collision needs a supplied value to trigger. They get their
// own cases rather than being left to a sweep that structurally cannot reach
// them; a probe that cannot express the case is not covering it.
func TestClausesTheSweepCannotExpress(t *testing.T) {
	t.Run("only HTTP OWS is trimmed when validating", func(t *testing.T) {
		// The sweep cannot express this: what leaks is a non-breaking space, and
		// a marker cannot be spelled with one. `strings.TrimSpace` removes it and
		// the date then parses, so the endpoint's byte is published verbatim.
		if isHTTPDate("Mon, 02 Jan 2006 15:04:05 GMT\u00a0") {
			t.Fatal("obs-text around a date was trimmed away and the value approved")
		}
		if !isHTTPDate(" Mon, 02 Jan 2006 15:04:05 GMT\t") {
			t.Fatal("real HTTP OWS was rejected")
		}
	})
	t.Run("an obsolete date's zone is fixed to GMT", func(t *testing.T) {
		if isHTTPDate("Monday, 02-Jan-06 15:04:05 XYZ") {
			t.Fatal("an arbitrary three-letter zone was accepted as an HTTP-date")
		}
		if !isHTTPDate("Monday, 02-Jan-06 15:04:05 GMT") {
			t.Fatal("a legal obsolete HTTP-date was rejected")
		}
	})
	t.Run("an unregistered trailer name is redacted", func(t *testing.T) {
		suppliedValues = nil
		structuralSurfaces = nil
		t.Cleanup(func() { structuralSurfaces = nil })
		tee := &teeBody{trailer: http.Header{"Server-Secret": []string{"x"}}}
		e := exchange{head: []byte("x"), captured: tee}
		got := stripMarks(e.trailerReport())
		if strings.Contains(got, "Server-Secret") {
			t.Fatalf("an endpoint-chosen trailer name was published: %q", got)
		}
	})
	t.Run("a generated attribute name survives the value scrub", func(t *testing.T) {
		suppliedValues = []string{"redacted"}
		structuralSurfaces = nil
		t.Cleanup(func() { suppliedValues = nil; structuralSurfaces = nil })
		got := stripMarks(scrubSupplied(dropFraming(
			"HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; server-secret\r\n\r\n")))
		// ⚠ THE SHAPE, NOT THE SUBSTRING. `<redacted, 1 chars>` is the legitimate
		// placeholder for the cookie's own value; what must not appear is the
		// prose form spliced INTO a generated token, which is what stripping the
		// marks produced.
		if strings.Contains(got, "chars>-") {
			t.Fatalf("the scrub rewrote a generated attribute name: %q", got)
		}
	})
}

// Structure that must survive, for the clauses added this round.
func TestStructureSurvivesTheNewerClauses(t *testing.T) {
	for _, c := range []struct{ name, dump, keep string }{
		{"a registered CORS field name", "HTTP/1.1 200 OK\r\nAccess-Control-Allow-Origin: *\r\n\r\n", "Access-Control-Allow-Origin"},
		{"a scheme with no authority", "HTTP/1.1 302 Found\r\nLocation: https:/cb\r\n\r\n", "https:"},
		// ⚠ THE LENGTH, NOT MERELY THE SURVIVAL OF A SLASH. Reading the interior
		// `http://` as an authority redacts `foo` as userinfo and then measures
		// the GENERATED token, so the seven-character segment is reported as
		// twenty-two. Asserting only that a `/` survives cannot see that, and a
		// mutant proved it.
		{"an interior scheme in a path", "HTTP/1.1 302 Found\r\nLocation: /cb/http://foo@bar/x\r\n\r\n", "redacted-7-chars"},
		{"a trailing semicolon", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x;\r\n\r\n", ";"},
	} {
		suppliedValues = nil
		structuralSurfaces = nil
		got := stripMarks(dropFraming(c.dump))
		if !strings.Contains(got, c.keep) {
			t.Errorf("%s: the criterion lost what it admits: %q", c.name, got)
		}
		if strings.Contains(got, "redacted-0-chars") {
			t.Errorf("%s: an attribute that was not there was invented: %q", c.name, got)
		}
		structuralSurfaces = nil
	}
	t.Run("a nested minted member beside a top-level one", func(t *testing.T) {
		suppliedValues = nil
		structuralSurfaces = nil
		t.Cleanup(func() { structuralSurfaces = nil })
		body := "HTTP/1.1 200 OK\r\n\r\n" +
			`{"subject_fact_key":"top","variant_payload":{"subject_fact_key":{"value":"payload"}}}`
		got := stripMarks(dropFraming(body))
		if strings.Contains(got, "withheld") {
			t.Fatalf("a payload member withheld the whole response: %q", got)
		}
	})
	t.Run("a header value is measured before our own escape", func(t *testing.T) {
		suppliedValues = nil
		got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\nServer: " + escapeMarks(`\x00`) + "\r\n\r\n"))
		if !strings.Contains(got, "4 chars") {
			t.Fatalf("the length described the recorder's escape, not the value: %q", got)
		}
	})
	t.Run("a name we sent survives a registered-name scrub", func(t *testing.T) {
		suppliedValues = []string{"content"}
		t.Cleanup(func() { suppliedValues = nil })
		got := stripMarks(dropFraming("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n"))
		if !strings.Contains(got, "Type") {
			t.Fatalf("a registered field name was destroyed by the value scrub: %q", got)
		}
	})
}
