package shardpilot

import "strings"

// buildExperimentFactEvent is a scene fixture: it builds a fact-class event
// — an SDK-authored experiment fact carrying a server-minted sfk1_ subject
// fact key as assignment_key, stamped with a purge epoch — so scenes can
// feed the pipeline's withdrawal filters directly. The SDK itself no longer
// builds facts: every experiment fact it posts is one the platform sealed
// (see sealedExposureEvent). Kept verbatim from the builder it replaced, so
// the scenes still exercise the fact class they always did.
func (c *Client) buildExperimentFactEvent(name, experimentKey string, entry *expEntry, eventID, sessionID string, factEpoch uint64) (Event, string) {
	factKey := strings.TrimSpace(entry.SubjectFactKey)
	if !expSubjectFactKeyPattern.MatchString(factKey) {
		// The privacy boundary of the fact lane: ONLY a grammar-valid
		// server-minted sfk1_ key may ride assignment_key. Absent AND
		// malformed values (a raw spcid_ echo included) alike mean this
		// assignment produces no fact.
		return Event{}, "exposure_no_subject_fact_key"
	}
	if c.cfg.AnonymousID == "" {
		// The ingest contract requires the SDK client identity as
		// anonymous_id on experiment facts (erasure reachability): with
		// none configured the fact cannot be built in-contract.
		return Event{}, "exposure_no_anonymous_id"
	}
	props := map[string]any{
		"experiment_key":     experimentKey,
		"experiment_version": entry.Version,
		"assignment_key":     factKey,
		"variant_key":        entry.VariantKey,
		"assignment_unit":    entry.AssignmentUnit,
	}
	return Event{
		ID:          eventID,
		Name:        name,
		AnonymousID: c.cfg.AnonymousID,
		// The arm-time session identity. The source override below makes
		// these facts publish as "client" whatever tier the configuration
		// is, and the ingest contract requires session_id on every
		// non-backend event — an unstamped fact would be REJECTED once the
		// producer lane accepts these names. This SDK's session is the
		// client instance (one marker per construction); an owed snapshot
		// passes the marker of the session its application belonged to.
		// Host events under a backend-source configuration are untouched:
		// they keep publishing as "backend" with the contract's session_id
		// carve-out.
		SessionID: sessionID,
		Props:     props,
		// Envelope contract for experiment facts: source "client" and no
		// user_id, whatever the client configuration would default.
		omitUserID:     true,
		sourceOverride: SourceClient,
		// The purge generation this fact belongs to: a later sentinel
		// withdraws only facts built BEFORE it. The stamp is the caller's
		// snapshot-time observation (see the function comment), one atomic
		// (entry, epoch) pair.
		expFactEpoch: factEpoch,
	}, ""
}
