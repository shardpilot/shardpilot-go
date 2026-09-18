package consentpolicy

// The per-purpose questions a caller actually asks. They are separate methods
// rather than one boolean because the strictest setting is taken PER PURPOSE —
// a single winner would let one lane's permission speak for another's, which is
// precisely what the orthogonal flags exist to prevent.

// AnalyticsClosed reports whether optional device/client analytics is closed by
// this verdict. It is the same question OptionalProcessingClosed answers, named
// for the purpose so a caller reading one lane does not have to know that.
func (d Decision) AnalyticsClosed() bool { return d.OptionalProcessingClosed }

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
	if !d.PlanUsed {
		return true
	}
	return d.Plan.CrashProfile != CrashMinimal
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
	if !d.PlanUsed {
		return ServerAnalyticsDenied, true
	}
	return d.Plan.ServerAnalytics, d.Plan.ObjectionRequired
}

// ProhibitedPurposes returns the union the plan carries. A fallback returns
// none — not because nothing is prohibited, but because a fallback knows
// nothing; the closed lanes above are what protect that case, and a caller must
// not read an empty list as "everything is permitted".
func (d Decision) ProhibitedPurposes() []string {
	if !d.PlanUsed {
		return nil
	}
	return append([]string(nil), d.Plan.ProhibitedPurposes...)
}

// OperationBlocks returns the restrictions that are independent of analytics
// consent — unresolved localisation, transfer, age/capacity and safety
// requirements. A consent toggle cannot remove one, and neither can a later
// grant.
func (d Decision) OperationBlocks() []string {
	if !d.PlanUsed {
		return nil
	}
	return append([]string(nil), d.Plan.OperationBlocks...)
}
