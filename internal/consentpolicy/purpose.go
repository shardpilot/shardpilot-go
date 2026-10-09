package consentpolicy

// The per-purpose questions a caller actually asks. They are separate methods
// rather than one boolean because the strictest setting is taken PER PURPOSE —
// a single winner would let one lane's permission speak for another's, which is
// precisely what the orthogonal flags exist to prevent.

// ChoiceDefault is which way the optional-analytics choice is pre-set when the
// host asks. It is not whether the host asks.
type ChoiceDefault string

const (
	ChoiceDefaultOff ChoiceDefault = "off"
	ChoiceDefaultOn  ChoiceDefault = "on"
)

// ⚠ WHAT THE REGIME DECIDES IS THE DEFAULT OF THE CHOICE, NOT WHETHER A CHOICE
// EXISTS — and this package said the opposite.
//
// It reported `OptionalProcessingClosed`, and `AnalyticsClosed` beside it, with
// documentation telling a host that a closed lane meant nothing was asked. The
// resolver answers STRICT_OPT_IN to every request in this release, so a host
// following that reading would never ask anyone and never start analytics, for
// every customer in every country. That is not the strict regime; it is no
// product. Both methods are gone rather than renamed, because a caller who kept
// compiling against the old name would keep the old meaning.
//
// STRICT means: ASK, with the choice defaulted OFF, and start the optional lane
// only on an explicit grant. SOFT means a prominent purpose notice, the choice
// defaulted ON, and one-tap off on the same screen. The difference is the
// default and the basis it is recorded under — and a grant given under a
// fallback is a valid grant, not a void one.

// AnalyticsChoiceDefault reports how the optional device/client analytics
// choice is pre-set. A verdict that used no plan defaults OFF, like STRICT.
func (d Decision) AnalyticsChoiceDefault() ChoiceDefault {
	if !d.planUsed || d.plan.Regime != SoftOptOut {
		return ChoiceDefaultOff
	}
	return ChoiceDefaultOn
}

// ExplicitGrantRequired reports whether the optional lane may start only on an
// explicit grant. True for every fallback and for the zero Decision.
//
// ⚠ FALSE IS NOT PERMISSION. It means only that the regime does not itself
// require the grant. The final notice barrier and backend admission bound to
// the scoped session, purpose, version and lease all still apply, and this
// package knows about none of them.
func (d Decision) ExplicitGrantRequired() bool {
	if !d.planUsed {
		return true
	}
	return d.plan.Regime != SoftOptOut
}

// CrashProfileOffered reports which crash profile, if any, the plan approves.
//
// ⚠ `off` MEANS NO APPROVED CRASH PROFILE IS OFFERED. It does not amend a
// host's separately reviewed crash gate: a host whose own review permits crash
// reporting is not overridden by this, and a host without one does not acquire
// permission from it. A fallback offers nothing.
func (d Decision) CrashProfileOffered() (CrashProfile, bool) {
	if !d.planUsed {
		return CrashOff, false
	}
	return d.plan.Flags.CrashProfile, true
}

// ServerAnalytics reports the backend lane's state. A fallback is denied.
//
// The objection requirement that used to ride beside this value is gone with
// the field it read: `server_analytics_objection_required` is not on the wire
// in this release, and a getter for a field the server never sends is a
// promise this package cannot keep. It returns with the field.
func (d Decision) ServerAnalytics() ServerAnalyticsState {
	if !d.planUsed {
		return ServerAnalyticsDenied
	}
	return d.plan.Flags.ServerAnalytics
}

// ChildRulesApplied reports whether minimised child handling applies.
// A fallback applies it.
func (d Decision) ChildRulesApplied() bool {
	if !d.planUsed {
		return true
	}
	return d.plan.Flags.ChildRules == ChildRulesMinimised
}

// ⚠ THE LIST GETTER RETURNS A SECOND VALUE, AND THAT IS THE WHOLE POINT.
//
// It used to return a bare []string, nil on a fallback, with a comment asking
// the caller not to read an empty list as "nothing is blocked". A comment is
// not a type. `len(d.OperationBlocks()) == 0` compiled, read naturally, and
// meant the opposite of the truth: a verdict that knows nothing blocks
// EVERYTHING, not nothing. The second value does not compile away — a caller
// cannot call len() on a two-value expression — so the case has to be handled
// rather than remembered.
//
// known == false means: this verdict used no plan, so the list is empty
// BECAUSE NOTHING IS KNOWN, and every operation is blocked. Use
// OperationBlocked for the per-operation question, which answers that
// correctly without the caller having to.
func (d Decision) OperationBlocks() (blocks []string, known bool) {
	if !d.planUsed {
		return nil, false
	}
	// ⚠ A KNOWN LIST IS NEVER nil, EVEN WHEN IT IS EMPTY, and
	// append([]string(nil)) returns nil for an empty source — so the resolver's
	// `[]` came back as the same nil this method returns for "nothing is
	// known". The second value still told them apart, but a caller who checked
	// `blocks == nil` instead of reading it got the two opposite meanings
	// collapsed into one. The distinction belongs in the value as well as in
	// the flag.
	out := make([]string, len(d.plan.Flags.OperationBlocks))
	copy(out, d.plan.Flags.OperationBlocks)
	return out, true
}

// OperationBlocked answers the per-operation question, and answers it closed
// when nothing is known.
func (d Decision) OperationBlocked(operation string) bool {
	if !d.planUsed {
		return true
	}
	for _, blocked := range d.plan.Flags.OperationBlocks {
		if blocked == operation {
			return true
		}
	}
	return false
}

// ProhibitedPurposes and PurposeProhibited are GONE, with the field they read.
// `prohibited_purposes` is not on the wire in this release; a getter over it
// answered from a field the resolver never sends, which is worse than not
// answering. They return in the release that adds the field to the contract,
// in the server and both SDKs at once.
