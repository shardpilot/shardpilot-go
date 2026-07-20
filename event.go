package shardpilot

import "time"

type Event struct {
	ID              string
	Name            string
	Timestamp       time.Time
	UserID          string
	AnonymousID     string
	SessionID       string
	SessionSequence int64
	Platform        string
	AppVersion      string
	AppBuild        string
	Props           map[string]any
	Context         map[string]any

	// omitUserID, when set, forces the wire envelope's user_id to be
	// OMITTED even when Config.UserID would default it. Unexported: only
	// the SDK's own experiment fact producers set it — the ingest contract
	// rejects experiment events that carry any user_id (identity rides
	// anonymous_id for erasure reachability). Not an actor change: the
	// envelope still carries the configured client identity.
	omitUserID bool

	// sourceOverride, when non-empty, replaces Config.Source on the wire
	// envelope for this event. Unexported: only the SDK's own experiment
	// fact producers set it — the ingest contract admits experiment events
	// with source "client" only, whatever tier the publishing credential
	// is.
	sourceOverride Source

	// expFactEpoch is the real-subjects purge generation this experiment
	// fact was BUILT under (stamped by buildExperimentFactEvent, zero for
	// everything else). The sentinel's batch filter withdraws only facts
	// whose stamp predates the current purge epoch — a FRESH post-purge
	// fact (a new authorized assignment after re-enable) must never be
	// dropped for a worker's epoch lag.
	expFactEpoch uint64
}
