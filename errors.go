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

	// ErrEventsDiscarded reports — folded into Close's returned error under
	// the opt-in consent floor — that undelivered events were DISCARDED at
	// teardown because the client has no durable spool to retain them
	// (Config.SpoolDir empty); typically events the grant-receipt dispatch
	// gate held through the final flush. The discard is permanent history:
	// every Close call keeps reporting it, so silent event loss can never
	// read as a clean teardown (the discarded events are also counted in
	// Stats.Dropped). Configure SpoolDir to retain undelivered events
	// across restarts instead. Never returned when the floor is off.
	ErrEventsDiscarded = errors.New("shardpilot events were discarded at close (no durable spool)")
)
