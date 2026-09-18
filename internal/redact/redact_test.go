package redact

import (
	"net/url"
	"strings"
	"testing"
)

// backslash keeps the JSON escape fixtures readable without fighting the
// surrounding quoting: a JSON body echoing a credential spells it /.
const backslash = `\`

// The decoded views are the point of this package, so they are the scenes: a
// credential that comes back percent-encoded — in either hex case — or as JSON
// \u escapes is the same credential, and the canonical-only implementation
// these senders started with let exactly those spellings through.
func TestDecodedViewsOfTheCredentialAreMasked(t *testing.T) {
	const key = "crash/write-key-test"
	r := New(key)
	for _, form := range []string{
		key,
		url.QueryEscape(key),                // crash%2Fwrite-key-test
		strings.ReplaceAll(key, "/", "%2f"), // the lower-case hex spelling
		strings.ReplaceAll(key, "/", "%2F"), // and the upper-case one
		strings.ReplaceAll(key, "/", backslash+"u002f"),                  // a JSON string body's escape
		strings.ReplaceAll(key, "c", backslash+"u0063"),                  // an escape away from the metacharacter
		strings.ReplaceAll(url.QueryEscape(key), "%", backslash+"u0025"), // JSON over percent
	} {
		t.Run(form, func(t *testing.T) {
			got := r.Replace("before %zz " + form + " after")
			if got != "before %zz [REDACTED] after" {
				t.Fatalf("the credential survived as %q: %q", form, got)
			}
		})
	}
}

func TestUnrelatedEvidenceIsPreserved(t *testing.T) {
	r := New("crash/write-key-test")
	for _, text := range []string{
		`{"other":"unchanged` + backslash + `u002f+evidence"}`,
		"a %2F that belongs to something else",
		"crash/write-key-tes", // one byte short of the credential
		"",
	} {
		if got := r.Replace(text); got != text {
			t.Fatalf("evidence that carries no credential was rewritten: %q -> %q", text, got)
		}
	}
	if got := New("").Replace("nothing configured"); got != "nothing configured" {
		t.Fatalf("an empty pattern masked evidence: %q", got)
	}
}

// The mutant this package exists to kill: the canonical-only replacer both
// senders started with, which passes every scene that echoes the credential
// verbatim and leaks it the moment a body comes back encoded.
func TestACanonicalOnlyReplacerWouldLeakTheEncodedForms(t *testing.T) {
	const key = "crash/write-key-test"
	canonical := strings.NewReplacer(key, "[REDACTED]")
	for _, form := range []string{
		url.QueryEscape(key),
		strings.ReplaceAll(key, "/", "%2f"),
		strings.ReplaceAll(key, "/", backslash+"u002f"),
	} {
		text := "echo " + form
		if canonical.Replace(text) != text {
			t.Fatalf("the canonical-only replacer unexpectedly caught %q; this scene no longer proves anything", form)
		}
		if got := New(key).Replace(text); got != "echo [REDACTED]" {
			t.Fatalf("this package did not catch %q: %q", form, got)
		}
	}
}
