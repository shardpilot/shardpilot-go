package consentpolicy

// The per-purpose questions a caller actually asks. They are separate methods
// rather than one boolean because the strictest setting is taken PER PURPOSE —
// a single winner would let one lane's permission speak for another's, which is
// precisely what the orthogonal flags exist to prevent.

// AnalyticsClosed reports whether optional device/client analytics is closed by
// this verdict. It is the same question OptionalProcessingClosed answers, named
// for the purpose so a caller reading one lane does not have to know that.
// ⚠ AND IT FAILS CLOSED ON A ZERO VALUE, which the first cut did not. A
// `var d Decision` — a caller's uninitialised struct, a map miss, a decoded
// nothing — has OptionalProcessingClosed false, so this reported analytics as
// OPEN for a decision nobody ever made. The crash and server-analytics helpers
// already keyed off PlanUsed; this one did not, and the one that did not was
// the analytics lane.
func (d Decision) AnalyticsClosed() bool {
	if !d.planUsed {
		return true
	}
	return d.optionalProcessingClosed
}

// CrashClosed reports whether the crash lane is closed.
//
// ⚠ IT DOES NOT INHERIT THE ANALYTICS ANSWER, IN EITHER DIRECTION. The crash
// profile has its own initial on/off rule; the platform decision is that crash
// reports for minors are off, with under-threshold clients not initialising the
// crash reporter at all — so this is read BEFORE a crash client exists.
// A fallback closes it; so does an absent or OFF profile. Device analytics
// being off does not by itself close a permitted crash lane, and analytics
// being open does not open this one.
func (d Decision) CrashClosed() bool {
	if !d.planUsed {
		return true
	}
	return d.plan.CrashProfile != CrashMinimal
}

// ServerAnalyticsBasis reports the backend lane's state and whether an
// objection requirement applies.
//
// ⚠ THERE IS NO TOGGLE HERE, AND THAT IS AN OWNER DECISION, NOT AN OMISSION.
// The in-game privacy rows for it were withdrawn: the objection route is manual
// — the rights page form or the privacy address — answered and executed within
// one month. So a caller honours the requirement out of band; nothing in this
// SDK can record or satisfy it.
//
// A fallback is DENIED with the requirement standing: missing lookup evidence
// is not a non-consent authorization.
func (d Decision) ServerAnalyticsBasis() (ServerAnalyticsState, bool) {
	if !d.planUsed || d.plan.ObjectionRequired == nil {
		return ServerAnalyticsDenied, true
	}
	return d.plan.ServerAnalytics, *d.plan.ObjectionRequired
}

// ⚠ THE TWO LIST GETTERS RETURN A SECOND VALUE, AND THAT IS THE WHOLE POINT.
//
// They used to return a bare []string, nil on a fallback, with a comment
// asking the caller not to read an empty list as "nothing is prohibited". A
// comment is not a type. `len(d.ProhibitedPurposes()) == 0` compiled, read
// naturally, and meant the opposite of the truth: a verdict that knows nothing
// restricts EVERYTHING, not nothing. The second value does not compile away —
// a caller cannot call len() on a two-value expression — so the case has to be
// handled rather than remembered.
//
// known == false means: this verdict used no plan, so the list is empty
// BECAUSE NOTHING IS KNOWN, and every purpose is prohibited and every
// operation blocked. Use PurposeProhibited and OperationBlocked for the
// question a caller actually has; these two exist for a receipt or a log.

// ProhibitedPurposes returns the union the verified plan carries, and whether
// that list is knowledge at all.
func (d Decision) ProhibitedPurposes() (purposes []string, known bool) {
	if !d.planUsed {
		return nil, false
	}
	return append([]string(nil), d.plan.ProhibitedPurposes...), true
}

// OperationBlocks returns the restrictions that are independent of analytics
// consent — unresolved localisation, transfer, age/capacity and safety
// requirements — and whether that list is knowledge at all. A consent toggle
// cannot remove one of these, and neither can a later grant.
func (d Decision) OperationBlocks() (blocks []string, known bool) {
	if !d.planUsed {
		return nil, false
	}
	return append([]string(nil), d.plan.OperationBlocks...), true
}

// PurposeProhibited reports whether this verdict prohibits one named purpose.
//
// ⚠ ON A FALLBACK IT IS TRUE FOR EVERY PURPOSE, including one nobody has
// heard of. A verdict that used no plan is not a verdict that found no
// restrictions; it is one that established none, and the conservative reading
// of "I do not know whether this purpose is prohibited" is that it is.
func (d Decision) PurposeProhibited(purpose string) bool {
	if !d.planUsed {
		return true
	}
	return contains(d.plan.ProhibitedPurposes, purpose)
}

// OperationBlocked reports whether this verdict blocks one named operation.
//
// ⚠ ON A FALLBACK IT IS TRUE FOR EVERY OPERATION, for the same reason, and it
// matters more here than anywhere else in this file: these blocks carry the
// transfer, age/capacity, localisation and safety restrictions that no consent
// choice can lift. An unauthenticated plan with its operation_blocks stripped
// would otherwise have read as "nothing is blocked" — which is precisely the
// forgery this package now refuses to act on at all.
func (d Decision) OperationBlocked(operation string) bool {
	if !d.planUsed {
		return true
	}
	return contains(d.plan.OperationBlocks, operation)
}

func contains(entries []string, want string) bool {
	for _, entry := range entries {
		if entry == want {
			return true
		}
	}
	return false
}
