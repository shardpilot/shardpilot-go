package main

import "testing"

// TestEveryAdmissionPredicateCanRefuse checks the CLAIMS, not the membership.
//
// ⚠ DERIVING A POPULATION REMOVES THE AUTHOR'S MEMORY FROM MEMBERSHIP; EVERY
// STATEMENT ABOUT THE MEMBERS IS STILL AUTHORED. `registeredFieldNames` and
// `registeredMediaTypes` are transcribed from a named registry, so WHICH names
// they hold is checkable. But `verbatimHeaders` attaches a PREDICATE to each name
// — "an HTTP-date in GMT", "an integer", "a registered directive" — and every one
// of those is a claim I wrote about that field's value grammar. A derived
// membership with an invented claim about each member reads as derived and is not.
//
// The direction that costs something is a predicate too PERMISSIVE: it admits an
// endpoint-chosen string, which is then printed verbatim. So the property is that
// each predicate can say no — a predicate that cannot refuse is not a criterion,
// it is a hole with a name (shardpilot/shardpilot-go#85, raised by a peer lane
// after finding the same shape in a document: the list of storage locations was
// right and the property attributed to them was wrong).
func TestEveryAdmissionPredicateCanRefuse(t *testing.T) {
	// Strings no field's fixed vocabulary admits: an endpoint-chosen token, a
	// token with punctuation, and one with a space.
	refusable := []string{"server-secret-token", "x_secret!value", "two words"}
	for name, adm := range verbatimHeaders {
		t.Run(name, func(t *testing.T) {
			for _, v := range refusable {
				if adm.ok(v) {
					t.Fatalf("the predicate for %q admits %q, so it is not a criterion", name, v)
				}
			}
		})
	}
	// The same question for the cookie attributes, whose values have fixed
	// vocabularies of their own.
	for _, a := range []string{"SameSite", "Max-Age", "Expires", "Path", "Domain"} {
		t.Run("cookie/"+a, func(t *testing.T) {
			for _, v := range refusable {
				if cookieAttrVerbatim(a, v) {
					t.Fatalf("the predicate for %q admits %q, so it is not a criterion", a, v)
				}
			}
		})
	}
	// And each must still ADMIT what it exists to admit, or "cannot refuse" has
	// been fixed by refusing everything.
	for _, ok := range []struct{ name, value string }{
		{"content-type", "application/json"},
		{"age", "12"},
		{"content-length", "0"},
	} {
		if adm, known := verbatimHeaders[ok.name]; !known || !adm.ok(ok.value) {
			t.Fatalf("the predicate for %q no longer admits %q", ok.name, ok.value)
		}
	}
	if !cookieAttrVerbatim("SameSite", "Lax") || !cookieAttrVerbatim("Max-Age", "10") {
		t.Fatal("a cookie attribute predicate no longer admits its own vocabulary")
	}
}
