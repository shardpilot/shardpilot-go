package main

import (
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
		tok  string
	}
	var probes []probe

	for name := range registeredFieldNames {
		probes = append(probes, probe{"registered field name", "HTTP/1.1 200 OK\r\n" + name + ": 1\r\n\r\n", name})
		break
	}
	for _, a := range []string{"Secure", "Path", "Max-Age", "SameSite", "HttpOnly"} {
		if !standardCookieAttr(a) {
			t.Fatalf("the probe list drifted from standardCookieAttr: %q", a)
		}
		probes = append(probes, probe{"cookie attribute", "HTTP/1.1 200 OK\r\nSet-Cookie: sid=x; " + a + "\r\n\r\n", a})
	}
	probes = append(probes, probe{"no-op content coding",
		"HTTP/1.1 200 OK\r\nContent-Encoding: identity\r\n\r\n", "identity"})
	for name := range benignTopLevel {
		probes = append(probes, probe{"benign JSON member",
			"HTTP/1.1 200 OK\r\n\r\n{\"" + name + "\":1}", name})
	}
	probes = append(probes, probe{"HTTP/1 reason phrase", "HTTP/1.1 200 OK\r\n\r\n", "OK"})

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
			if !strings.Contains(got, p.tok) {
				t.Fatalf("a token this program vouched for was rewritten: %q became %q", p.tok, got)
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
