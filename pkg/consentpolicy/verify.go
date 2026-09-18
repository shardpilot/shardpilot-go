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
	// ReasonPlanUnsigned: the plan carries no signature, and this build has no
	// key to have verified one with. Distinct from signature_unverified, which
	// is a plan that DOES carry a signature this build cannot check — the two
	// say different things to an operator reading a log, and both refuse.
	ReasonPlanUnsigned    Reason = "plan_unsigned"
	ReasonCallerCancelled Reason = "caller_cancelled"
)

// Decision is the verdict for one actor's operation.
//
// It is deliberately NOT convertible to a consent state, and this package
// exposes nothing that would do it: a plan is policy selection, and processing
// admission is a different question with a different authority.
type Decision struct {
	// ⚠ EVERY FIELD A CALLER COULD BRANCH ON IS UNEXPORTED, not only the
	// validity marker. Unexporting planUsed alone would have left the two
	// fields the contract actually talks about — the regime and whether the
	// door is closed — settable on a struct anyone can build, and readable
	// straight off a JSON round-trip that had already dropped the plan. That
	// is the same defect one field over.
	//
	// regime is the effective class AFTER the fallback. A fallback never
	// yields SOFT_OPT_OUT. Read it through Regime().
	regime Regime
	// optionalProcessingClosed: see OptionalProcessingClosed().
	optionalProcessingClosed bool
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
	// ⚠ THE VALIDITY MARKER IS UNEXPORTED, AND THAT IS THE POINT. As an
	// exported field it was the one piece of the verdict a caller could FORGE:
	// Decision{PlanUsed: true} reads as "a plan was verified" while plan stays
	// the zero value, so every helper below read that empty plan as permitting
	// everything. A JSON round-trip did it without anyone meaning to —
	// encoding/json cannot see the unexported plan, so it drops it and keeps
	// the flag, turning a logged verdict back into a permissive one.
	//
	// Only Prepare sets it. Read it through PlanUsed().
	planUsed bool
}

// PlanUsed reports whether a plan was verified and used. A caller that
// branches on the regime alone cannot tell a verified STRICT from a fallback
// STRICT, and the difference matters in a receipt.
func (d Decision) PlanUsed() bool { return d.planUsed }

// Regime reports the effective class. The zero Decision reports STRICT_OPT_IN,
// because a verdict nobody made is not a permissive one.
func (d Decision) Regime() Regime {
	if d.regime == "" {
		return StrictOptIn
	}
	return d.regime
}

// OptionalProcessingClosed reports whether the REGIME ITSELF closes optional
// processing, and it is true for every fallback and for the zero Decision.
//
// ⚠ FALSE IS NOT PERMISSION. It means only that the regime is not what closed
// the door. SOFT still waits for the final notice barrier and for successful
// backend admission bound to the scoped player session, purpose, version and
// lease — none of which this package knows about. A caller that treats false
// as "admit" has skipped the authority that actually decides.
func (d Decision) OptionalProcessingClosed() bool {
	if !d.planUsed {
		return true
	}
	return d.optionalProcessingClosed
}

// Plan returns a COPY of the verified plan, or the zero value when none was
// used. Every slice in it is copied too, so a caller cannot reach back into
// the verdict through one.
func (d Decision) Plan() (Plan, bool) {
	if !d.planUsed {
		return Plan{}, false
	}
	return clonePlan(d.plan), true
}

// clonePlan deep-copies the parts a caller could otherwise mutate: the two
// string slices, the signal entries AND THE POINTER INSIDE EACH ONE, the
// optional band and the optional required-scalar pointer.
//
// ⚠ COPYING A []Signal COPIES THE STRUCTS, NOT WHAT THEIR POINTERS POINT AT.
// append() gives a fresh backing array whose entries still address the same
// bool, so a caller writing through Signal.Available would reach into the
// verdict. Every pointer in the plan is copied by value, not by address.
func clonePlan(p Plan) Plan {
	out := p
	out.ProhibitedPurposes = append([]string(nil), p.ProhibitedPurposes...)
	out.OperationBlocks = append([]string(nil), p.OperationBlocks...)
	out.SignalsUsed = append([]Signal(nil), p.SignalsUsed...)
	for i := range out.SignalsUsed {
		if out.SignalsUsed[i].Available != nil {
			available := *out.SignalsUsed[i].Available
			out.SignalsUsed[i].Available = &available
		}
	}
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

// verificationKeyAvailable reports whether this build can AUTHENTICATE a plan.
//
// ⚠ IT IS false, AND NOTHING OUTSIDE THIS PACKAGE CAN MAKE IT true: no
// configuration field, no environment variable, no build tag, no exported
// setter. A verifier that can be talked into trusting an unauthenticated plan
// is not a verifier, and the talking is the whole attack.
//
// While it is false, Prepare NEVER USES A PLAN — for any input, including a
// perfectly well-formed one. That is this release's posture, not a stub: the
// resolver's initial release emits unsigned plans, so there is nothing to
// authenticate them against, and the honest answer to "is this plan genuine"
// is no rather than probably.
const verificationKeyAvailable = false

// strictFallback is the one place a fallback verdict is built, so no path can
// invent a partial permissive one.
func strictFallback(reason Reason, detail string) Decision {
	return Decision{
		regime:                   StrictOptIn,
		optionalProcessingClosed: true,
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

	// ⚠ THE AUTHENTICATION GATE, AND IN THIS RELEASE IT IS THE LAST WORD.
	//
	// The checks above still run, and they run FIRST, because "your handoff is
	// malformed" and "your handoff is for another app" are worth telling an
	// operator precisely. But none of them authenticates anything. The second
	// cut of this file accepted an unsigned plan whose regime, crash profile
	// and server-analytics basis were all conservative, and that was wrong on
	// an axis those three enums do not cover: prohibited_purposes and
	// operation_blocks were never checked at all, so an attacker who could not
	// make the plan permissive could STRIP its restrictions instead. Those
	// blocks govern transfer, age/capacity, localisation and safety
	// independently of any analytics choice — no consent setting lifts one, so
	// removing one is permissive however strict the enums look.
	//
	// There is no field-by-field repair for that. An unsigned plan is not
	// evidence, so this release uses none, and the fallback below closes EVERY
	// axis rather than the three that happen to be enums: see the purpose
	// helpers, where an unused plan reports every operation blocked and every
	// purpose prohibited.
	if !verificationKeyAvailable {
		return strictFallback(ReasonPlanUnsigned,
			"this build has no key to verify a plan with; an unauthenticated plan is not used")
	}

	// The release-2 path, unreachable while the gate above is closed.
	return decisionFromPlan(plan)
}

// decisionFromPlan is the mapping from a VERIFIED plan to a verdict. It is the
// one thing release 2 turns on, and it is a named function rather than a block
// inside Prepare so that it is exercised for what it is: no end-to-end test can
// reach it while the gate is shut, and a test that re-implemented the mapping
// in order to look end-to-end would be testing itself.
func decisionFromPlan(plan Plan) Decision {
	decision := Decision{
		regime:   plan.Regime,
		Reason:   ReasonNone,
		plan:     clonePlan(plan),
		planUsed: true,
	}
	// STRICT waits for an explicit grant and UNKNOWN was never classified, so
	// both close it here. SOFT leaves it open in the REGIME's sense only — see
	// the field's contract; it is not an admission.
	decision.optionalProcessingClosed = plan.Regime != SoftOptOut
	return decision
}
