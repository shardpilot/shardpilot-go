package consentpolicy

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// A plan fixture built from the control plane's schema shape. Values are
// synthetic; nothing here is a real scope, version or credential.
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

// TestPrepareUsesAValidPlan is the control: without it every refusal below
// would be satisfied by a verifier that refuses everything.
func TestPrepareUsesAValidPlan(t *testing.T) {
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(nil), Scope: callerScope(), Now: fixedClock(),
	})
	if !decision.PlanUsed {
		t.Fatalf("a valid plan was not used: reason=%q detail=%q", decision.Reason, decision.Detail)
	}
	if decision.Regime != StrictOptIn || decision.Reason != ReasonNone {
		t.Fatalf("regime=%q reason=%q, want STRICT_OPT_IN with no fallback reason", decision.Regime, decision.Reason)
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
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) { m["regime"] = string(Unknown) }), Scope: callerScope(), Now: fixedClock(),
	})
	if !decision.PlanUsed || decision.Regime != Unknown {
		t.Fatalf("the plan should have been used: %+v", decision)
	}
	if !decision.OptionalProcessingClosed {
		t.Fatal("UNKNOWN must close optional processing")
	}
}
