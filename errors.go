package shardpilot

import "errors"

var (
	ErrClosed        = errors.New("shardpilot client is closed")
	ErrInvalidConfig = errors.New("invalid shardpilot config")
	ErrInvalidEvent  = errors.New("invalid shardpilot event")
	ErrQueueFull     = errors.New("shardpilot queue is full")
	ErrConsentDenied = errors.New("shardpilot analytics consent is denied")

	// ErrConsentUnknown is returned by Track/Enqueue under the opt-in
	// consent floor (Config.ConsentFloor) while no explicit consent decision
	// has been recorded: the floor's consent-first posture transmits nothing
	// for an undecided actor. Never returned when the floor is off — the
	// default posture keeps the pipeline open under unknown consent.
	ErrConsentUnknown = errors.New("shardpilot analytics consent is not decided")

	// ErrConsentReceiptPending is returned by Track (and explicit Flush)
	// under the consent floor while an analytics-grant consent receipt is
	// retained undispatched: events sent meanwhile would overtake the grant
	// on the wire, and on a strict-consent workspace be terminally
	// suppressed. Transient — the receipt dispatches on the worker cadence
	// (or the next Track/Flush attempt) and the pipeline reopens.
	ErrConsentReceiptPending = errors.New("shardpilot consent receipt awaits dispatch")

	// ErrConsentPending is returned by Close under the consent floor when
	// undelivered consent receipts could not be made durable: with no
	// Config.SpoolDir there is no durable outbox, and with one a failed
	// outbox write is still owed. The receipts would be lost if the process
	// exited, so Close declines to complete; deliver or persist them by
	// calling Close again (it retries both), or accept the loss by exiting
	// anyway.
	ErrConsentPending = errors.New("shardpilot consent receipts are pending durable delivery")

	// ErrInvalidConsentDecision is returned by SetConsentDecision for a
	// decision value that is not ConsentDecisionGranted,
	// ConsentDecisionDenied, or ConsentDecisionDeniedForcedMinor.
	ErrInvalidConsentDecision = errors.New("invalid shardpilot consent decision")

	// ErrInvalidConsentIdentity rejects a consent decision under the opt-in
	// consent floor when a configured identifier (Config.UserID or
	// Config.AnonymousID) is non-empty but over the 512-byte receipt
	// clamp: the floor requires in-contract identifiers, because events
	// stamp the configured identifiers verbatim and a receipt minted for a
	// substitute actor would authorize a DIFFERENT actor than the events
	// carry. The decision is rejected whole — reject, never truncate —
	// and NOTHING is applied. Never returned when the floor is off.
	ErrInvalidConsentIdentity = errors.New("shardpilot consent identity exceeds the identifier clamp")

	// ErrConsentActorMismatch is returned by Track/Enqueue under the opt-in
	// consent floor for an event whose per-event UserID/AnonymousID override
	// resolves to a DIFFERENT effective actor than the configured identity
	// the floor's decision covers. The floor holds one client-side decision
	// for one actor: transmitting an override actor would publish events no
	// local decision (and no dispatched receipt) covers. Overrides that
	// resolve to the SAME effective actor pass through; per-actor decisions
	// beyond the configured identity belong to the server-side consent path
	// (the default posture). Never returned when the floor is off.
	ErrConsentActorMismatch = errors.New("shardpilot event actor differs from the consent-floor actor")

	// ErrEventsDiscarded reports — folded into Close's returned error under
	// the opt-in consent floor — that undelivered events were DISCARDED at
	// teardown: neither delivered nor durably retained. Without
	// Config.SpoolDir there is nothing to retain the close remnant in; with
	// one, the remnant's spool write can itself fail closed (an unpersisted
	// grant record refusing the write gate, or a persist failure — e.g.
	// disk full — the final retry could not recover). Typically these are
	// events the grant-receipt dispatch gate held through the final flush.
	// The discard is permanent history: every Close call keeps reporting
	// it, so silent event loss can never read as a clean teardown (the
	// discarded events are also counted in Stats.Dropped). Never returned
	// when the floor is off.
	ErrEventsDiscarded = errors.New("shardpilot events were discarded at close (no durable spool)")

	// ErrExperimentsNotConfigured is returned by the experiment exposure/
	// outcome producers when the experiment consumer is not opted in
	// (Config.ExperimentsURL unset): with no assignment plane configured
	// there is no assignment these facts could describe. The default
	// posture — no experiments configured — emits nothing.
	ErrExperimentsNotConfigured = errors.New("shardpilot experiments are not configured")

	// ErrExperimentNotAssigned is returned by the experiment exposure/
	// outcome producers for an assignment whose Assigned is false (a
	// traffic-gate miss, a kill switch, or a targeting mismatch): the
	// producer contract emits facts only for assignments the app can act
	// on — never for a not-assigned verdict.
	ErrExperimentNotAssigned = errors.New("shardpilot experiment assignment is not assigned")

	// ErrInvalidExperimentFact is returned by the experiment exposure/
	// outcome producers when a fact cannot be built within the ingest
	// contract: a malformed assignment (missing keys, an unknown
	// assignment unit, a client_id-unit assignment without a valid
	// sfk1_ subject_fact_key — the raw spcid_ subject never rides
	// experiment props), a missing Config.AnonymousID (experiment facts
	// must carry the SDK client identity for erasure reachability), or an
	// outcome value that is not a finite number or a boolean. The fact is
	// refused whole; nothing is queued.
	ErrInvalidExperimentFact = errors.New("invalid shardpilot experiment fact")

	// ErrExperimentScopeMismatch is returned by the experiment exposure/
	// outcome producers for an assignment whose echoed app_key/
	// environment_key does not match THIS client's configured AppID/
	// EnvironmentID: a verdict fetched by another client, app, or
	// environment must never build facts under this client's envelope
	// scope — the fact would attribute another scope's experiment keys to
	// this app's identity. Nothing is queued.
	ErrExperimentScopeMismatch = errors.New("shardpilot experiment assignment is for a different app/environment scope")
)
