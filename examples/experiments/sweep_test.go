package main

import (
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
		// ⚠ FOUR CASES ARE DELIBERATELY ABSENT, and their absence is the honest
		// record of an open decision rather than an oversight: an identifier in
		// the redirect HOST, in a query or fragment parameter NAME, or as the
		// cookie NAME. Each is a real hole, reported on shardpilot-go#85 and NOT
		// closed here. Closing them means lengthening the name side too, and the
		// measured artifact then reads
		// `Location: /redacted-2-chars?redacted-5-chars=redacted-6-chars`, which
		// four existing fixtures pin against on purpose. That is a trade between
		// coverage and a readable artifact, and it belongs to whoever owns the
		// tool, not to the change that noticed it.
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
