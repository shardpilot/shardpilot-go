package consentpolicy

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// VerifiedPlayerPolicy is what the trusted game flow hands in for ONE scoped
// player operation: the plan it holds, the scope it is admitting for, and the
// clock it trusts.
//
// ⚠ IT CARRIES NO IDENTITY AND NO CONNECTION. The plan's signed country is not
// evidence about the admitting connection, and the game server's own IP is
// never the player's source, so neither appears here. Whether THIS
// actor may be admitted is the caller's decision, made with its own retained
// floors, age evidence, choices and objections; this package answers only what
// the policy says.
type VerifiedPlayerPolicy struct {
	// Plan is the raw plan bytes as delivered. Empty means no plan.
	Plan []byte

	// Scope is the caller's own scope for this operation. A plan issued for
	// another app does not admit here, however valid it is elsewhere.
	Scope Scope

	// Now is the caller's trusted clock. LEAVING IT NIL IS NOT "ASSUME FRESH":
	// a verifier with no trusted time cannot tell whether the plan expired, and
	// the answer to a question it cannot ask is strict.
	Now func() time.Time
}

// Reason is the closed top-level vocabulary for why a verdict fell back. It is
// separate from SignalReason, which describes one signal inside a valid plan.
type Reason string

const (
	ReasonNone                Reason = ""
	ReasonPlanAbsent          Reason = "plan_absent"
	ReasonPlanUnreadable      Reason = "plan_unreadable"
	ReasonScopeMismatch       Reason = "scope_mismatch"
	ReasonPlanExpired         Reason = "plan_expired"
	ReasonNoTrustedClock      Reason = "no_trusted_clock"
	ReasonSignatureUnverified Reason = "signature_unverified"
	// ReasonUnsignedPermissive: the plan is unsigned AND carries a permissive
	// field. Distinct from signature_unverified, which is a plan that carries a
	// signature this build cannot check.
	ReasonUnsignedPermissive Reason = "unsigned_permissive"
	ReasonCallerCancelled    Reason = "caller_cancelled"
)

// Decision is the verdict for one actor's operation.
//
// It is deliberately NOT convertible to a consent state, and this package
// exposes nothing that would do it: a plan is policy selection, and processing
// admission is a different question with a different authority.
type Decision struct {
	// Regime is the effective class AFTER the fallback. A fallback never
	// yields SOFT_OPT_OUT.
	Regime Regime
	// OptionalProcessingClosed says the REGIME ITSELF closes optional
	// processing. True for every fallback, for STRICT_OPT_IN (which waits for
	// a valid purpose-specific explicit grant) and for UNKNOWN (the resolver
	// could not classify, which is not permission).
	//
	// ⚠ FALSE IS NOT PERMISSION. It means only that the regime is not what
	// closed the door. SOFT still waits for the final notice barrier and for
	// successful backend admission bound to the scoped player session, purpose,
	// version and lease — none of which this package knows about. A caller that
	// treats false as "admit" has skipped the authority that actually decides.
	OptionalProcessingClosed bool
	// Reason names why a fallback happened; empty when the plan was used.
	Reason Reason
	// Detail carries the parser's own message when there is one. It is for an
	// operator reading a log, never for branching.
	Detail string
	// ⚠ THE PLAN IS UNEXPORTED, AND THE SLICES ARE COPIED IN. An exported Plan
	// hands the caller the same backing arrays the accessors read from, so a
	// COPY of a Decision could mutate what the original's getters return —
	// values are copied, the arrays behind them are not. A verdict another
	// holder can edit is not a verdict. Read it through Plan() and the purpose
	// helpers, which copy on the way out.
	plan Plan
	// PlanUsed says whether Plan is meaningful. A caller that branches on the
	// regime alone cannot tell a verified STRICT from a fallback STRICT, and
	// the difference matters in a receipt.
	PlanUsed bool
}

// Plan returns a COPY of the verified plan, or the zero value when none was
// used. Every slice in it is copied too, so a caller cannot reach back into
// the verdict through one.
func (d Decision) Plan() (Plan, bool) {
	if !d.PlanUsed {
		return Plan{}, false
	}
	return clonePlan(d.plan), true
}

// clonePlan deep-copies the parts a caller could otherwise mutate: the two
// string slices, the optional band and the optional required-scalar pointer.
func clonePlan(p Plan) Plan {
	out := p
	out.ProhibitedPurposes = append([]string(nil), p.ProhibitedPurposes...)
	out.OperationBlocks = append([]string(nil), p.OperationBlocks...)
	out.SignalsUsed = append([]Signal(nil), p.SignalsUsed...)
	if p.AgeBand != nil {
		band := *p.AgeBand
		out.AgeBand = &band
	}
	if p.ObjectionRequired != nil {
		required := *p.ObjectionRequired
		out.ObjectionRequired = &required
	}
	return out
}

// unsignedPlanIsPermissive names the first permissive field an unsigned plan
// carries, or "" when the plan is the conservative tuple this release accepts
// without a signature: a strict-or-unknown regime, a crash profile that is not
// minimal-permitted, and a server-analytics basis that is not eligible.
func unsignedPlanIsPermissive(plan Plan) string {
	switch {
	case plan.Regime != StrictOptIn && plan.Regime != Unknown:
		return "an unsigned plan carries regime " + string(plan.Regime) +
			"; only STRICT_OPT_IN or UNKNOWN is honoured without a verified signature"
	case plan.CrashProfile != CrashOff:
		return "an unsigned plan carries crash_profile " + string(plan.CrashProfile) +
			"; only OFF is honoured without a verified signature"
	case plan.ServerAnalytics != ServerAnalyticsDenied:
		return "an unsigned plan carries server_analytics " + string(plan.ServerAnalytics) +
			"; only DENIED is honoured without a verified signature"
	}
	return ""
}

// strictFallback is the one place a fallback verdict is built, so no path can
// invent a partial permissive one.
func strictFallback(reason Reason, detail string) Decision {
	return Decision{
		Regime:                   StrictOptIn,
		OptionalProcessingClosed: true,
		Reason:                   reason,
		Detail:                   detail,
	}
}

// Prepare verifies the plan for ONE scoped player operation.
//
// ⚠ ONE ACTOR, ONE CALL, NO MEMO. The package holds no state between calls on
// purpose: a cached verdict served to a second actor would authorize a
// different player than the events carry, and a process that shares one client
// across actors is exactly the case this contract names.
//
// Every failure resolves to STRICT with optional processing closed and a named
// reason. There is no error return, because an error a caller might ignore is
// the wrong shape for a decision that must fail closed.
func Prepare(ctx context.Context, policy VerifiedPlayerPolicy) Decision {
	if ctx == nil {
		return strictFallback(ReasonCallerCancelled, "no context supplied")
	}
	if err := ctx.Err(); err != nil {
		return strictFallback(ReasonCallerCancelled, err.Error())
	}
	if !policy.Scope.complete() {
		return strictFallback(ReasonScopeMismatch, "the caller named no complete scope tuple")
	}

	plan, err := ParsePlan(policy.Plan)
	if err != nil {
		if errors.Is(err, ErrPlanAbsent) {
			return strictFallback(ReasonPlanAbsent, err.Error())
		}
		return strictFallback(ReasonPlanUnreadable, err.Error())
	}
	if !plan.Scope.equal(policy.Scope) {
		// Said without echoing the plan's scope: a mismatch is an operator
		// fact, and reflecting another tenant's ids into this caller's log is
		// not this package's business.
		return strictFallback(ReasonScopeMismatch, "the plan is scoped to another app, environment or workspace")
	}
	// The signature is reserved and empty in the resolver's initial release. If
	// one is PRESENT, this SDK cannot verify it yet — and an unverifiable
	// signature is exactly the case that must not admit. Silently ignoring it
	// would make the field's arrival a downgrade.
	if plan.Signature != "" {
		return strictFallback(ReasonSignatureUnverified,
			"the plan carries a signature this build cannot verify")
	}
	// ⚠ AN UNSIGNED PLAN IS NOT EVIDENCE OF A PERMISSION, AND THE FIRST CUT'S
	// SAFETY ARGUMENT WAS WRONG. It said a forged plan "can only tighten" —
	// true of a forged STRICT plan, and irrelevant, because nothing stopped a
	// forged plan from being PERMISSIVE. Every field was trusted individually:
	// an attacker who could put bytes in front of this verifier could hand it
	// SOFT_OPT_OUT, a minimal-permitted crash profile or an eligible
	// server-analytics basis, and each was honoured on its own.
	//
	// Until signature verification exists, an unsigned plan may therefore carry
	// ONLY the fully conservative tuple. Any permissive field in one takes the
	// same path as an unreadable plan — not used, everything closed — so the
	// argument becomes true rather than merely comforting: a forged unsigned
	// plan can only tighten, BECAUSE a permissive unsigned field is never
	// honoured.
	if why := unsignedPlanIsPermissive(plan); why != "" {
		return strictFallback(ReasonUnsignedPermissive, why)
	}
	if policy.Now == nil {
		return strictFallback(ReasonNoTrustedClock, "the caller supplied no trusted clock")
	}
	expiry, err := plan.expiry()
	if err != nil {
		return strictFallback(ReasonPlanUnreadable, err.Error())
	}
	if !policy.Now().UTC().Before(expiry) {
		return strictFallback(ReasonPlanExpired,
			fmt.Sprintf("the plan expired at %s", expiry.Format(time.RFC3339)))
	}

	decision := Decision{
		Regime:   plan.Regime,
		Reason:   ReasonNone,
		plan:     clonePlan(plan),
		PlanUsed: true,
	}
	// STRICT waits for an explicit grant and UNKNOWN was never classified, so
	// both close it here. SOFT leaves it open in the REGIME's sense only — see
	// the field's contract; it is not an admission.
	decision.OptionalProcessingClosed = plan.Regime != SoftOptOut
	return decision
}
