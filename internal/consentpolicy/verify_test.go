package consentpolicy

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// goldenWire returns the resolver's own bytes for one of the recorded
// responses. See testdata/golden/README.md for where they came from.
func goldenWire(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "golden", name+".json"))
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	return raw
}

// validPlan is THE RESOLVER'S OWN RESPONSE, mutated.
//
// ⚠ IT USED TO BE A HAND-WRITTEN MAP, AND THAT IS THE WHOLE DEFECT THIS
// CHANGE REPAIRS. The fixture was built from the same belief as the code, so
// the tests and the parser agreed with each other about a shape the server has
// never sent: a FLAT plan carrying three fields the resolver does not emit. A
// thousand lines of green proved only that this package was self-consistent.
// Deriving the fixture from the recorded bytes means a scene can no longer
// pass against a plan the resolver could not have produced — and a contract
// change now breaks the tests here, which is where it should break.
//
// `expires_at` is refreshed, and it is the ONLY field touched by default: the
// golden is recorded at a fixed clock, so its expiry is a fixed instant in the
// past and every scene wanting a LIVE plan would otherwise be asserting
// against the calendar. A scene that cares about expiry overrides it.
func validPlan(mutate func(map[string]any)) []byte {
	raw, err := os.ReadFile(filepath.Join("testdata", "golden", "consent-policy-resolved.json"))
	if err != nil {
		panic(err)
	}
	var plan map[string]any
	if err := json.Unmarshal(raw, &plan); err != nil {
		panic(err)
	}
	plan["expires_at"] = time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339)
	if mutate != nil {
		mutate(plan)
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		panic(err)
	}
	return encoded
}

// withFlag mutates a flag INSIDE the nested object, because that is where the
// flags live. Reaching for m["crash_profile"] is how the old fixture agreed
// with the old parser, so the helper exists to make the nesting the only
// spelling available to a scene.
func withFlag(key string, value any) func(map[string]any) {
	return func(m map[string]any) {
		flags, ok := m["flags"].(map[string]any)
		if !ok {
			panic("the fixture has no nested flags object")
		}
		flags[key] = value
	}
}

// callerScope is THE SCOPE THE GOLDEN WAS ISSUED FOR. It used to be three
// "-synthetic" strings invented here, which meant every scene that fed the
// real bytes would have been refused for scope before reaching what it was
// testing.
func callerScope() Scope {
	return Scope{WorkspaceID: "ws_1", AppID: "app_1", EnvironmentID: "env_1"}
}

func fixedClock() func() time.Time {
	return func() time.Time { return time.Now().UTC() }
}

// ⚠ IN THIS RELEASE, NOTHING IS EVER USED — INCLUDING A PERFECT PLAN. With no
// verification key, an unauthenticated plan is not evidence, so Prepare
// refuses every input. Asked over a table rather than one fixture, so a future
// edit cannot re-open a single path quietly.
func TestNoPlanIsUsedWhileThereIsNoVerificationKey(t *testing.T) {
	cases := []struct {
		name string
		plan []byte
		// The reason each one is refused FOR, named per row rather than shared.
		// A stripped required key is caught earlier, by the key walk, and
		// asserting one reason for the whole table would have meant either
		// loosening the check to "some refusal" or moving that row out of the
		// scene — both of which lose the fact that it is refused EARLIER, not
		// more weakly.
		want Reason
	}{
		{"the resolver's own recorded response", validPlan(nil), ReasonPlanUnsigned},
		{"an UNKNOWN plan", validPlan(func(m map[string]any) { m["regime"] = string(Unknown) }), ReasonPlanUnsigned},
		{"a SOFT plan", validPlan(func(m map[string]any) { m["regime"] = string(SoftOptOut) }), ReasonPlanUnsigned},
		{"a permitted crash profile", validPlan(withFlag("crash_profile", string(CrashMinimalDiagnosticsForMinors))), ReasonPlanUnsigned},
		// ⚠ THE CASE THE PREVIOUS CUT LET THROUGH. Every enum is conservative
		// and the restriction list has been STRIPPED. It used to be accepted
		// and reported as carrying no operation blocks at all.
		{"conservative enums with the restrictions removed", validPlan(func(m map[string]any) {
			delete(m["flags"].(map[string]any), "operation_blocks")
		}), ReasonPlanUnreadable},
		{"conservative enums with the restrictions emptied", validPlan(withFlag("operation_blocks", []any{})), ReasonPlanUnsigned},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			decision := Prepare(context.Background(), VerifiedPlayerPolicy{
				Plan: testCase.plan, Scope: callerScope(), Now: fixedClock(),
			})
			if decision.PlanUsed() {
				t.Fatalf("an unauthenticated plan was used: %+v", decision)
			}
			if decision.Reason != testCase.want {
				t.Fatalf("reason=%q detail=%q, want %q", decision.Reason, decision.Detail, testCase.want)
			}
			assertClosedOnEveryAxis(t, decision)
		})
	}
}

// assertClosedOnEveryAxis is the "fail closed" contract in one place: not the
// three enums, every axis a caller can ask about.
func assertClosedOnEveryAxis(t *testing.T, decision Decision) {
	t.Helper()
	if decision.Regime() != StrictOptIn {
		t.Fatalf("regime=%q, an unused verdict is STRICT_OPT_IN", decision.Regime())
	}
	// ⚠ THE CLOSED AXIS IS THE GRANT, NOT THE QUESTION. A verdict that used no
	// plan requires an explicit grant and pre-sets the choice OFF; it does not
	// say the host never asks. The methods that used to claim that are gone —
	// see purpose.go — and this is the assertion that replaced them.
	if !decision.ExplicitGrantRequired() {
		t.Fatal("an unused verdict must require an explicit grant")
	}
	if decision.AnalyticsChoiceDefault() != ChoiceDefaultOff {
		t.Fatalf("choice default=%q, an unused verdict defaults OFF", decision.AnalyticsChoiceDefault())
	}
	if profile, known := decision.CrashProfileOffered(); known || profile != CrashOff {
		t.Fatalf("an unused verdict offers no crash profile, got %q/%v", profile, known)
	}
	if state := decision.ServerAnalytics(); state != ServerAnalyticsDenied {
		t.Fatalf("server analytics must be denied, got %q", state)
	}
	if !decision.ChildRulesApplied() {
		t.Fatal("an unused verdict applies child rules")
	}
	// ⚠ AND EVERY OPERATION BLOCKED. A verdict that established no
	// restrictions is not a verdict that found none. Asked with names nothing
	// could have enumerated, because "any operation" is the claim.
	for _, name := range []string{"transfer_review", "age_capacity", "localisation", "safety", "", "a-name-nobody-registered"} {
		if !decision.OperationBlocked(name) {
			t.Fatalf("operation %q was not blocked by an unused verdict", name)
		}
	}
	if blocks, known := decision.OperationBlocks(); known || blocks != nil {
		t.Fatalf("an unused verdict must report its block list as not known, got %v/%v", blocks, known)
	}
	if _, ok := decision.Plan(); ok {
		t.Fatal("an unused verdict must hand out no plan")
	}
}

// The release-2 mapping, exercised for what it is. The gate is a compile-time
// constant, so no test can open it; decisionFromPlan is the function Prepare
// calls once it is open, called here directly rather than re-implemented.
func TestTheVerifiedMappingIsTheReleaseTwoPath(t *testing.T) {
	plan, err := ParsePlan(validPlan(nil))
	if err != nil {
		t.Fatalf("the fixture must parse: %v", err)
	}
	decision := decisionFromPlan(plan)
	if !decision.PlanUsed() || decision.Regime() != StrictOptIn || decision.Reason != ReasonNone {
		t.Fatalf("%+v", decision)
	}
	// STRICT asks, with the choice defaulted OFF and the optional lane started
	// only on an explicit grant. It does NOT mean nothing is asked.
	if decision.AnalyticsChoiceDefault() != ChoiceDefaultOff || !decision.ExplicitGrantRequired() {
		t.Fatal("STRICT_OPT_IN defaults the choice OFF and requires an explicit grant")
	}
	// And the flags arrive from the NESTED object, which is the whole contract
	// repair: a top-level read returns the zero value for all four.
	if profile, known := decision.CrashProfileOffered(); !known || profile != CrashOff {
		t.Fatalf("crash profile %q/%v did not come from flags", profile, known)
	}
	if !decision.ChildRulesApplied() {
		t.Fatal("flags.child_rules did not reach the verdict")
	}
	if blocks, known := decision.OperationBlocks(); !known || blocks == nil || len(blocks) != 0 {
		t.Fatalf("flags.operation_blocks must arrive as an empty, known list, got %v/%v", blocks, known)
	}
}

// ⚠ EVERY FAILURE IS STRICT WITH A NAMED REASON. This is the table the whole
// package exists for: a partial permissive result anywhere here would be the
// defect.
func TestEveryFailureFallsBackStrict(t *testing.T) {
	cases := []struct {
		name   string
		policy VerifiedPlayerPolicy
		want   Reason
	}{
		{"no plan at all", VerifiedPlayerPolicy{Scope: callerScope(), Now: fixedClock()}, ReasonPlanAbsent},
		{"not JSON", VerifiedPlayerPolicy{Plan: []byte("regime=STRICT"), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"trailing content", VerifiedPlayerPolicy{Plan: append(validPlan(nil), '{'), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"unknown field", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) { m["extra"] = 1 }), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"unknown field inside flags", VerifiedPlayerPolicy{Plan: validPlan(withFlag("extra", 1)), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"unknown regime", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) { m["regime"] = "PERMISSIVE" }), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"unknown crash profile", VerifiedPlayerPolicy{Plan: validPlan(withFlag("crash_profile", "everything")), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		// ⚠ THE UPPER-CASE SPELLING OF A FLAG IS NOT THE CONTRACT. This is the
		// exact value the old struct carried as its constant, so this row is
		// the one that would have passed before the vocabularies moved.
		{"a flag in the old upper-case vocabulary", VerifiedPlayerPolicy{Plan: validPlan(withFlag("crash_profile", "OFF")), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"unknown server-analytics state", VerifiedPlayerPolicy{Plan: validPlan(withFlag("server_analytics", "eligible")), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"unknown child-rules value", VerifiedPlayerPolicy{Plan: validPlan(withFlag("child_rules", "standard")), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"unknown basis character", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			m["basis"].(map[string]any)["character"] = "legal_advice"
		}), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"unknown table provenance", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			m["basis"].(map[string]any)["table_provenance"] = "guessed"
		}), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"an empty notice", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			m["basis"].(map[string]any)["notice"] = ""
		}), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"a notice over its bound", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			m["basis"].(map[string]any)["notice"] = strings.Repeat("n", maxNoticeBytes+1)
		}), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"unavailable signal with no reason", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			m["signals_used"] = []any{map[string]any{"name": "server_country", "available": false}}
		}), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"available signal carrying a reason", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			m["signals_used"] = []any{map[string]any{"name": "server_country", "available": true, "reason": string(SourceUnavailable)}}
		}), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"no expiry", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) { delete(m, "expires_at") }), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"expiry is not RFC 3339", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) { m["expires_at"] = "soon" }), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"version outside its character set", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) { m["policy_version"] = "2026 09 12" }), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"version over its bound", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) { m["policy_version"] = strings.Repeat("v", 65) }), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"incomplete scope in the plan", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			delete(m["scope"].(map[string]any), "environment_id")
		}), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"plan scoped to another app", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			m["scope"].(map[string]any)["app_id"] = "another-app"
		}), Scope: callerScope(), Now: fixedClock()}, ReasonScopeMismatch},
		{"caller names no scope", VerifiedPlayerPolicy{Plan: validPlan(nil), Now: fixedClock()}, ReasonScopeMismatch},
		{"expired", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			m["expires_at"] = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
		}), Scope: callerScope(), Now: fixedClock()}, ReasonPlanExpired},
		{"no trusted clock", VerifiedPlayerPolicy{Plan: validPlan(nil), Scope: callerScope()}, ReasonNoTrustedClock},
		{"signature this build cannot verify", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			m["signature"] = "ed25519:synthetic"
		}), Scope: callerScope(), Now: fixedClock()}, ReasonSignatureUnverified},
		// ⚠ THE REFUSAL BODY, AND IT IS READ AS A REFUSAL RATHER THAN AS A
		// SCOPE MISMATCH. Its scope is three empty strings, so answering it
		// after the scope comparison reported the wrong cause for every
		// refusal the resolver has ever sent. See the ordering note in
		// verify.go.
		{"the resolver's own recorded refusal", VerifiedPlayerPolicy{
			Plan: goldenRefusalWithLiveExpiry(), Scope: callerScope(), Now: fixedClock(),
		}, ReasonResolverRefused},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			decision := Prepare(context.Background(), testCase.policy)
			if decision.Reason != testCase.want {
				t.Fatalf("reason=%q detail=%q, want %q", decision.Reason, decision.Detail, testCase.want)
			}
			if decision.PlanUsed() {
				t.Fatal("a fallback must not report a used plan")
			}
			assertClosedOnEveryAxis(t, decision)
		})
	}
}

// goldenRefusalWithLiveExpiry is the recorded refusal with its expiry
// refreshed, for the same reason validPlan refreshes one: the recorded instant
// is fixed. Nothing else is touched — in particular the three empty scope
// strings stay, because they are what makes the refusal a refusal rather than
// a plan.
func goldenRefusalWithLiveExpiry() []byte {
	raw, err := os.ReadFile(filepath.Join("testdata", "golden", "consent-policy-refusal.json"))
	if err != nil {
		panic(err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		panic(err)
	}
	body["expires_at"] = time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339)
	encoded, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return encoded
}

// ⚠ A REFUSAL IS ANSWERED AS A REFUSAL WHATEVER ITS SCOPE, and the reason it
// carries reaches the operator. This is the scene the ordering bug in
// verify.go would fail: with the check after the scope comparison, a refusal
// came back `scope_mismatch` and `resolver_refused` was unreachable.
func TestTheResolversRefusalIsReadAsARefusal(t *testing.T) {
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: goldenRefusalWithLiveExpiry(), Scope: callerScope(), Now: fixedClock(),
	})
	if decision.Reason != ReasonResolverRefused {
		t.Fatalf("reason=%q detail=%q, want %q", decision.Reason, decision.Detail, ReasonResolverRefused)
	}
	// The resolver's own reason reaches the log, or an operator is told a
	// refusal happened without being told which one.
	if !strings.Contains(decision.Detail, "invalid_scope") {
		t.Fatalf("the refusal's own reason must reach the detail: %q", decision.Detail)
	}
	assertClosedOnEveryAxis(t, decision)

	// ⚠ AND THE CONTROL THAT MAKES THE SCENE MEAN SOMETHING: the SAME body
	// with the reason removed and a real scope is refused for a DIFFERENT
	// reason. Without this, a package that answered `resolver_refused` to
	// everything would pass.
	asPlan := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(nil), Scope: callerScope(), Now: fixedClock(),
	})
	if asPlan.Reason == ReasonResolverRefused {
		t.Fatal("a body carrying no reason must not be read as a refusal")
	}

	// And a refusal is never usable as a plan, however permissive its contents
	// are forged to look.
	forged := goldenRefusalWithLiveExpiry()
	var body map[string]any
	if err := json.Unmarshal(forged, &body); err != nil {
		t.Fatal(err)
	}
	body["regime"] = string(SoftOptOut)
	body["scope"] = map[string]any{"workspace_id": "ws_1", "app_id": "app_1", "environment_id": "env_1"}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	permissive := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: encoded, Scope: callerScope(), Now: fixedClock(),
	})
	if permissive.Reason != ReasonResolverRefused || permissive.Regime() != StrictOptIn {
		t.Fatalf("a forged-permissive refusal must still refuse: %+v", permissive)
	}
}

// ⚠ AND THE FALLBACK NEVER PRODUCES SOFT. Asked directly, because "every
// fallback is strict" is the one property a future edit is most likely to
// weaken by accident.
func TestNoInputYieldsSoftFromAFallback(t *testing.T) {
	for _, plan := range [][]byte{nil, []byte("{"), goldenRefusalWithLiveExpiry(),
		validPlan(func(m map[string]any) { m["regime"] = "SOFT_OPT_OUT"; m["expires_at"] = "nonsense" })} {
		decision := Prepare(context.Background(), VerifiedPlayerPolicy{Plan: plan, Scope: callerScope(), Now: fixedClock()})
		if decision.Regime() == SoftOptOut {
			t.Fatal("a fallback produced SOFT_OPT_OUT")
		}
	}
}

// UNKNOWN is a VERIFIED plan that still defaults the choice off and requires
// an explicit grant: the resolver said it could not classify, and that is not
// permission.
func TestUnknownRegimeStillRequiresAnExplicitGrant(t *testing.T) {
	plan, err := ParsePlan(validPlan(func(m map[string]any) { m["regime"] = string(Unknown) }))
	if err != nil {
		t.Fatalf("the fixture must parse: %v", err)
	}
	decision := decisionFromPlan(plan)
	if !decision.PlanUsed() || decision.Regime() != Unknown {
		t.Fatalf("the plan should have been used: %+v", decision)
	}
	if decision.AnalyticsChoiceDefault() != ChoiceDefaultOff || !decision.ExplicitGrantRequired() {
		t.Fatal("UNKNOWN defaults the choice OFF and requires an explicit grant")
	}
}
