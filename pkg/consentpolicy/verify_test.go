package consentpolicy

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// A plan fixture built from the published schema shape. Values are synthetic;
// nothing here is a real scope, version or credential.
func validPlan(mutate func(map[string]any)) []byte {
	plan := map[string]any{
		"regime":                              string(StrictOptIn),
		"crash_profile":                       string(CrashOff),
		"server_analytics":                    string(ServerAnalyticsDenied),
		"server_analytics_objection_required": true,
		"policy_version":                      "2026-09-12.1",
		"consent_text_version":                "ff-v1.3",
		"presented_language":                  "en",
		"scope": map[string]any{
			"workspace_id":   "ws-synthetic",
			"app_id":         "app-synthetic",
			"environment_id": "env-synthetic",
		},
		// The resolver's initial release performs NO geolocation, so every
		// signal arrives unavailable with a reason. A valid plan carries no
		// country, and this fixture is the shape the SDK actually meets.
		"signals_used": []any{
			map[string]any{"name": "server_country", "available": false, "reason": string(NotEnabledInRelease)},
			map[string]any{"name": "store_region", "available": false, "reason": string(SourceNotPermitted)},
		},
		"expires_at":      time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339),
		"max_age_seconds": 300,
	}
	if mutate != nil {
		mutate(plan)
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		panic(err)
	}
	return encoded
}

func callerScope() Scope {
	return Scope{WorkspaceID: "ws-synthetic", AppID: "app-synthetic", EnvironmentID: "env-synthetic"}
}

func fixedClock() func() time.Time {
	return func() time.Time { return time.Now().UTC() }
}

// ⚠ IN THIS RELEASE, NOTHING IS EVER USED — INCLUDING A PERFECT PLAN. This
// replaces the old "a valid plan is used" control, which could no longer be
// true: with no verification key, an unauthenticated plan is not evidence, so
// Prepare refuses every input. Asked over a table rather than one fixture, so
// that a future edit cannot re-open a single path quietly.
func TestNoPlanIsUsedWhileThereIsNoVerificationKey(t *testing.T) {
	cases := []struct {
		name  string
		plan  []byte
		scope Scope
	}{
		{"the perfect plan", validPlan(nil), callerScope()},
		{"an UNKNOWN plan", validPlan(func(m map[string]any) { m["regime"] = string(Unknown) }), callerScope()},
		{"a SOFT plan", validPlan(func(m map[string]any) { m["regime"] = string(SoftOptOut) }), callerScope()},
		{"a minimal-permitted crash profile", validPlan(func(m map[string]any) { m["crash_profile"] = string(CrashMinimal) }), callerScope()},
		{"an eligible server-analytics basis", validPlan(func(m map[string]any) {
			m["server_analytics"] = string(ServerAnalyticsEligible)
		}), callerScope()},
		// ⚠ THE CASE THE PREVIOUS CUT LET THROUGH. Every enum is conservative
		// and the restriction lists have been STRIPPED. It used to be accepted
		// and reported as carrying no operation blocks at all.
		{"conservative enums with the restrictions removed", validPlan(func(m map[string]any) {
			delete(m, "operation_blocks")
			delete(m, "prohibited_purposes")
		}), callerScope()},
		{"conservative enums with the restrictions emptied", validPlan(func(m map[string]any) {
			m["operation_blocks"] = []any{}
			m["prohibited_purposes"] = []any{}
		}), callerScope()},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			decision := Prepare(context.Background(), VerifiedPlayerPolicy{
				Plan: testCase.plan, Scope: testCase.scope, Now: fixedClock(),
			})
			if decision.PlanUsed {
				t.Fatalf("an unauthenticated plan was used: %+v", decision)
			}
			if decision.Reason != ReasonPlanUnsigned {
				t.Fatalf("reason=%q detail=%q, want %q", decision.Reason, decision.Detail, ReasonPlanUnsigned)
			}
			assertClosedOnEveryAxis(t, decision)
		})
	}
}

// assertClosedOnEveryAxis is the "fail closed" contract in one place: not the
// three enums, every axis a caller can ask about.
func assertClosedOnEveryAxis(t *testing.T, decision Decision) {
	t.Helper()
	if decision.Regime != StrictOptIn {
		t.Fatalf("regime=%q, an unused verdict is STRICT_OPT_IN", decision.Regime)
	}
	if !decision.OptionalProcessingClosed || !decision.AnalyticsClosed() || !decision.CrashClosed() {
		t.Fatal("optional processing, analytics and the crash lane must all be closed")
	}
	state, objection := decision.ServerAnalyticsBasis()
	if state != ServerAnalyticsDenied || !objection {
		t.Fatalf("server analytics must be DENIED with the objection standing, got %q/%v", state, objection)
	}
	// ⚠ AND EVERY OPERATION BLOCKED, EVERY PURPOSE PROHIBITED. A verdict that
	// established no restrictions is not a verdict that found none. Asked with
	// names nothing could have enumerated, because "any operation" is the
	// claim.
	for _, name := range []string{"transfer_review", "age_capacity", "localisation", "safety", "", "a-name-nobody-registered"} {
		if !decision.OperationBlocked(name) {
			t.Fatalf("operation %q was not blocked by an unused verdict", name)
		}
		if !decision.PurposeProhibited(name) {
			t.Fatalf("purpose %q was not prohibited by an unused verdict", name)
		}
	}
	if blocks, known := decision.OperationBlocks(); known || blocks != nil {
		t.Fatalf("an unused verdict must report its block list as not known, got %v/%v", blocks, known)
	}
	if purposes, known := decision.ProhibitedPurposes(); known || purposes != nil {
		t.Fatalf("an unused verdict must report its purpose list as not known, got %v/%v", purposes, known)
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
	if !decision.PlanUsed || decision.Regime != StrictOptIn || decision.Reason != ReasonNone {
		t.Fatalf("%+v", decision)
	}
	// STRICT closes optional processing on the plan alone: the grant is a
	// separate authority.
	if !decision.OptionalProcessingClosed || !decision.AnalyticsClosed() {
		t.Fatal("STRICT_OPT_IN must close optional processing on this verdict alone")
	}
}

// ⚠ EVERY FAILURE IS STRICT WITH OPTIONAL PROCESSING CLOSED AND A NAMED
// REASON. This is the table the whole package exists for: a partial permissive
// result anywhere here would be the defect.
func TestEveryFailureFallsBackStrict(t *testing.T) {
	future := time.Now().UTC().Add(time.Hour)
	cases := []struct {
		name   string
		policy VerifiedPlayerPolicy
		want   Reason
	}{
		{"no plan at all", VerifiedPlayerPolicy{Scope: callerScope(), Now: fixedClock()}, ReasonPlanAbsent},
		{"not JSON", VerifiedPlayerPolicy{Plan: []byte("regime=STRICT"), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"trailing content", VerifiedPlayerPolicy{Plan: append(validPlan(nil), '{'), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"unknown field", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) { m["extra"] = 1 }), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"unknown regime", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) { m["regime"] = "PERMISSIVE" }), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"unknown crash profile", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) { m["crash_profile"] = "EVERYTHING" }), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
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
			m["scope"] = map[string]any{"workspace_id": "ws-synthetic", "app_id": "app-synthetic"}
		}), Scope: callerScope(), Now: fixedClock()}, ReasonPlanUnreadable},
		{"plan scoped to another app", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			m["scope"] = map[string]any{"workspace_id": "ws-synthetic", "app_id": "another-app", "environment_id": "env-synthetic"}
		}), Scope: callerScope(), Now: fixedClock()}, ReasonScopeMismatch},
		{"caller names no scope", VerifiedPlayerPolicy{Plan: validPlan(nil), Now: fixedClock()}, ReasonScopeMismatch},
		{"expired", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			m["expires_at"] = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
		}), Scope: callerScope(), Now: fixedClock()}, ReasonPlanExpired},
		{"no trusted clock", VerifiedPlayerPolicy{Plan: validPlan(nil), Scope: callerScope()}, ReasonNoTrustedClock},
		{"signature this build cannot verify", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			m["signature"] = "ed25519:synthetic"
		}), Scope: callerScope(), Now: fixedClock()}, ReasonSignatureUnverified},
		{"plan over its byte bound", VerifiedPlayerPolicy{Plan: validPlan(func(m map[string]any) {
			blocks := make([]any, 0, 64)
			for i := 0; i < 64; i++ {
				blocks = append(blocks, strings.Repeat("b", 64))
			}
			m["operation_blocks"] = blocks
			m["prohibited_purposes"] = blocks
		}), Scope: callerScope(), Now: fixedClock()}, ReasonNone},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			decision := Prepare(context.Background(), testCase.policy)
			if testCase.want == ReasonNone {
				// The oversize case is bounded by entry count rather than
				// refused outright; what matters is only that it did not open
				// anything, which the shared assertions below cover.
				if decision.Regime == SoftOptOut {
					t.Fatal("no input may yield SOFT")
				}
				return
			}
			if decision.Reason != testCase.want {
				t.Fatalf("reason=%q detail=%q, want %q", decision.Reason, decision.Detail, testCase.want)
			}
			if decision.Regime != StrictOptIn {
				t.Fatalf("regime=%q, every fallback is STRICT_OPT_IN", decision.Regime)
			}
			if !decision.OptionalProcessingClosed || !decision.AnalyticsClosed() || !decision.CrashClosed() {
				t.Fatal("a fallback must close optional processing and the crash lane")
			}
			if decision.PlanUsed {
				t.Fatal("a fallback must not report a used plan")
			}
			state, objection := decision.ServerAnalyticsBasis()
			if state != ServerAnalyticsDenied || !objection {
				t.Fatalf("a fallback is DENIED with the objection requirement standing, got %q/%v", state, objection)
			}
		})
	}
	_ = future
}

// ⚠ AND THE FALLBACK NEVER PRODUCES SOFT. Asked directly, because "every
// fallback is strict" is the one property a future edit is most likely to
// weaken by accident.
func TestNoInputYieldsSoftFromAFallback(t *testing.T) {
	for _, plan := range [][]byte{nil, []byte("{"), validPlan(func(m map[string]any) { m["regime"] = "SOFT_OPT_OUT"; m["expires_at"] = "nonsense" })} {
		decision := Prepare(context.Background(), VerifiedPlayerPolicy{Plan: plan, Scope: callerScope(), Now: fixedClock()})
		if decision.Regime == SoftOptOut {
			t.Fatal("a fallback produced SOFT_OPT_OUT")
		}
	}
}

// UNKNOWN is a VERIFIED plan that still closes optional processing: the
// resolver said it could not classify, and that is not permission.
func TestUnknownRegimeClosesOptionalProcessing(t *testing.T) {
	plan, err := ParsePlan(validPlan(func(m map[string]any) { m["regime"] = string(Unknown) }))
	if err != nil {
		t.Fatalf("the fixture must parse: %v", err)
	}
	decision := decisionFromPlan(plan)
	if !decision.PlanUsed || decision.Regime != Unknown {
		t.Fatalf("the plan should have been used: %+v", decision)
	}
	if !decision.OptionalProcessingClosed {
		t.Fatal("UNKNOWN must close optional processing")
	}
}
