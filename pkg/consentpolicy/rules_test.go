package consentpolicy

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// ⚠ RULE (a): PER ACTOR, NEVER PER PROCESS. A verdict cached across calls would
// authorize a different player than the events carry — and ADR §5 names the
// shared-client case explicitly, so this is the realistic arrangement rather
// than a contrived one.
func TestAVerdictIsNeverReusedForAnotherActor(t *testing.T) {
	first := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(nil), Scope: callerScope(), Now: fixedClock(),
	})
	if !first.PlanUsed {
		t.Fatalf("the control must use its plan: %+v", first)
	}
	// The SECOND actor in the same process hands in NO plan. If anything were
	// memoised, this would inherit the first actor's verdict.
	second := Prepare(context.Background(), VerifiedPlayerPolicy{Scope: callerScope(), Now: fixedClock()})
	if second.PlanUsed || second.Reason != ReasonPlanAbsent {
		t.Fatalf("a second actor inherited a verdict: %+v", second)
	}
	if !second.OptionalProcessingClosed {
		t.Fatal("the second actor must be closed, whatever the first one got")
	}
	// And the reverse order, so the scene cannot pass because the cache only
	// fills on the second call.
	third := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) { m["regime"] = string(Unknown) }), Scope: callerScope(), Now: fixedClock(),
	})
	if third.Regime != Unknown {
		t.Fatalf("the third actor got %q, not its own plan's regime", third.Regime)
	}
}

// ⚠ RULE (b): A PLAN IS NOT A GRANT. ADR §4.3: a SOFT non-objection needs a
// new, distinct admission-basis representation — "do not call today's
// SetConsent(true) or set an analytics consent boolean to pretend the player
// opted in". So this package must expose no route from a verdict to a consent
// state, and must not reach into the telemetry client at all.
func TestTheDecisionCannotBecomeAConsentGrant(t *testing.T) {
	// The package's own source is the evidence: it imports nothing from the
	// telemetry root package, so there is no SetConsent to call.
	// ⚠ ASSERTED AGAINST CODE, NOT PROSE, and the first cut got this wrong:
	// it searched for the bare word "SetConsent" and fired on doc.go's own
	// comment EXPLAINING that the package must never call it. A check that
	// fails on the sentence forbidding the thing is a check about text. So:
	// imports are read from the import block, and the call is matched as a
	// call.
	for _, file := range packageSources(t) {
		for _, line := range strings.Split(file.body, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if strings.Contains(trimmed, "shardpilot-go\"") || strings.Contains(trimmed, "shardpilot-go/pkg/") {
				t.Errorf("%s imports the telemetry client: %q — policy selection is not processing admission",
					file.name, trimmed)
			}
			for _, forbidden := range []string{".SetConsent(", ".SetConsentDecision(", ".Consent()"} {
				if strings.Contains(trimmed, forbidden) {
					t.Errorf("%s calls %s: %q — a plan must never be convertible to a consent grant",
						file.name, forbidden, trimmed)
				}
			}
		}
	}
	// And a SOFT plan — the one a careless edit would map to "granted" — still
	// reports only a regime.
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) { m["regime"] = string(SoftOptOut) }), Scope: callerScope(), Now: fixedClock(),
	})
	if decision.Regime != SoftOptOut {
		t.Fatalf("the fixture should parse as SOFT: %+v", decision)
	}
	// ⚠ FALSE IS NOT PERMISSION, and the contract says so: SOFT leaves the
	// regime's own door open and still waits for the final notice barrier and
	// backend admission, neither of which this package knows about.
	if decision.OptionalProcessingClosed {
		t.Fatal("SOFT is not closed BY THE REGIME; the barrier is elsewhere")
	}
}

// ⚠ RULE (c): NO TRUSTED CLOCK → STRICT. "Assume fresh" is the tempting
// shortcut and the wrong one: a verifier that cannot tell whether the plan
// expired has not verified it.
func TestNoTrustedClockIsStrict(t *testing.T) {
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{Plan: validPlan(nil), Scope: callerScope()})
	if decision.Reason != ReasonNoTrustedClock || decision.Regime != StrictOptIn {
		t.Fatalf("%+v", decision)
	}
	if decision.PlanUsed {
		t.Fatal("a plan whose freshness could not be checked was reported as used")
	}
}

// ⚠ RULE (d): AN UNVERIFIABLE SIGNATURE IS STRICT. The resolver's initial
// release reserves the field and sends none; when one arrives, a build that
// cannot check it must not treat its arrival as a downgrade by ignoring it.
func TestAnUnverifiableSignatureIsStrict(t *testing.T) {
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan:  validPlan(func(m map[string]any) { m["signature"] = "ed25519:synthetic" }),
		Scope: callerScope(), Now: fixedClock(),
	})
	if decision.Reason != ReasonSignatureUnverified || decision.Regime != StrictOptIn {
		t.Fatalf("%+v", decision)
	}
	// The control: the same plan WITHOUT a signature is used, so the scene is
	// about the signature rather than about the fixture.
	if !Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(nil), Scope: callerScope(), Now: fixedClock(),
	}).PlanUsed {
		t.Fatal("the unsigned control must be used")
	}
}

// The crash lane does not inherit the analytics answer in either direction —
// ADR §2.1 gives it its own rule, and ODR-0008 D1 makes it off by default.
func TestTheCrashLaneIsItsOwnDecision(t *testing.T) {
	strictWithCrashOn := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan:  validPlan(func(m map[string]any) { m["crash_profile"] = string(CrashMinimal) }),
		Scope: callerScope(), Now: fixedClock(),
	})
	if !strictWithCrashOn.AnalyticsClosed() {
		t.Fatal("analytics is still closed under STRICT")
	}
	if strictWithCrashOn.CrashClosed() {
		t.Fatal("a permitted crash profile is not closed by analytics being off")
	}
	softWithCrashOff := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) {
			m["regime"] = string(SoftOptOut)
			m["crash_profile"] = string(CrashOff)
		}),
		Scope: callerScope(), Now: fixedClock(),
	})
	if !softWithCrashOff.CrashClosed() {
		t.Fatal("an OFF crash profile is not opened by the analytics regime")
	}
}

// ODR-0008 D2: the objection route is manual and there is no in-game toggle, so
// the SDK reports a basis and a requirement and can neither record nor satisfy
// it. A fallback is DENIED with the requirement standing.
func TestServerAnalyticsIsABasisNotAToggle(t *testing.T) {
	eligible := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) {
			m["server_analytics"] = string(ServerAnalyticsEligible)
			m["server_analytics_objection_required"] = true
		}),
		Scope: callerScope(), Now: fixedClock(),
	})
	state, objection := eligible.ServerAnalyticsBasis()
	if state != ServerAnalyticsEligible || !objection {
		t.Fatalf("state=%q objection=%v", state, objection)
	}
	state, objection = Prepare(context.Background(), VerifiedPlayerPolicy{Scope: callerScope(), Now: fixedClock()}).ServerAnalyticsBasis()
	if state != ServerAnalyticsDenied || !objection {
		t.Fatalf("a fallback must be DENIED with the requirement standing, got %q/%v", state, objection)
	}
}

// The lists are copied out, so a caller cannot mutate the verdict it was given.
func TestTheVerdictsListsAreCopies(t *testing.T) {
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) {
			m["operation_blocks"] = []any{"transfer_review"}
			m["prohibited_purposes"] = []any{"advertising"}
		}),
		Scope: callerScope(), Now: fixedClock(),
	})
	blocks := decision.OperationBlocks()
	if len(blocks) != 1 {
		t.Fatalf("blocks=%v", blocks)
	}
	blocks[0] = "removed"
	if decision.OperationBlocks()[0] != "transfer_review" {
		t.Fatal("a caller mutated the verdict's own list")
	}
}

type sourceFile struct {
	name string
	body string
}

// packageSources reads this package's non-test Go files, so a rule about what
// the package may not name is checked against the source rather than trusted.
func packageSources(t *testing.T) []sourceFile {
	t.Helper()
	names := []string{"doc.go", "plan.go", "verify.go", "purpose.go"}
	files := make([]sourceFile, 0, len(names))
	for _, name := range names {
		body, err := readFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		files = append(files, sourceFile{name: name, body: body})
	}
	return files
}

// A plan the parser accepts must round-trip the fields the caller reads, or the
// verdicts above are reporting the fixture rather than the plan.
func TestParsedFieldsReachTheVerdict(t *testing.T) {
	raw := validPlan(func(m map[string]any) {
		m["presented_language"] = "en-GB"
		m["consent_text_version"] = "ff-v1.4"
	})
	var asMap map[string]any
	if err := json.Unmarshal(raw, &asMap); err != nil {
		t.Fatal(err)
	}
	plan, err := ParsePlan(raw)
	if err != nil {
		t.Fatalf("the fixture must parse: %v", err)
	}
	if plan.PresentedLanguage != "en-GB" || plan.ConsentTextVersion != "ff-v1.4" {
		t.Fatalf("%+v", plan)
	}
	if _, err := plan.expiry(); err != nil {
		t.Fatalf("the fixture's expiry must parse: %v", err)
	}
	if plan.ExpiresAt == "" || !strings.Contains(plan.ExpiresAt, "T") {
		t.Fatalf("expires_at=%q", plan.ExpiresAt)
	}
	_ = time.Now
}
