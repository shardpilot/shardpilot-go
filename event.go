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
	// fact was BUILT under (stamped when a sealed fact becomes an event,
	// zero for everything else). The sentinel's batch filter withdraws only facts
	// whose stamp predates the current purge epoch — a FRESH post-purge
	// fact (a new authorized assignment after re-enable) must never be
	// dropped for a worker's epoch lag.
	expFactEpoch uint64

	// expKeyWithdrawEpoch is the per-experiment withdrawal generation
	// (Client.expKeyWithdrawEpoch) this experiment fact was BUILT under,
	// read under the experiment lock together with the owed record it came
	// from (zero for everything else). An age refusal withdraws its
	// experiment's facts built before it, and only those: a fact built
	// after it is a new application and is never withdrawn by it.
	expKeyWithdrawEpoch uint64

	// expFactSeq is the pipeline sequence an experiment fact was handed to
	// the analytics queue under (Client.expFactSeq; zero for everything
	// else). The fact-key prune reads it against the worker's low-water
	// mark to tell whether the fact may still be queued or held.
	expFactSeq uint64

	// intakeConsentEpoch is the consent denial generation this event was
	// ADMITTED under, stamped at the queue boundary. The worker joins a
	// received event to its held batch only when this stamp matches the
	// epoch it just settled: a denial's queue drain and the worker's
	// receive consume the same channel, so a pre-denial event the worker
	// steals from the drain is recognized (stale stamp) and dropped at
	// admission instead of riding — or condemning — a later epoch's batch.
	intakeConsentEpoch uint64

	// rawEventTS and attestationSeal carry a SEALED experiment fact
	// (unexported: only the SDK's apply hop sets them). The envelope then
	// writes rawEventTS as event_ts verbatim — the seal covers that exact
	// string — and attestationSeal as attestation_seal.
	rawEventTS      string
	attestationSeal string
}
