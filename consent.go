package shardpilot

import (
	"context"
	"sync"
	"time"

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
)

// ConsentState is the analytics consent state of the configured actor.
//
// With SpoolDir, forced-minor denial survives a restart in either mode. The
// consent floor additionally restores other persisted decisions. Without the
// floor, integrators store and reapply other consent states at startup.
type ConsentState string

const (
	// ConsentUnknown is the initial state: no decision has been recorded,
	// and the event pipeline is fully open.
	//
	// This is the opposite default posture from the consent-first client
	// SDKs (Defold/Unity/Unreal), which transmit nothing while consent is
	// unknown. Caveat for strict-consent workspaces: on a workspace whose
	// effective strict consent mode is enforce, the server fails closed and
	// terminally suppresses every event whose actor has no explicit
	// analytics consent recorded server-side — per event, as
	// suppressed_no_consent inside the 202 envelope, never as an error — so
	// publishing under ConsentUnknown "succeeds" while delivering nothing;
	// the suppressions surface only through Config.OnBatchResult or the
	// Snapshot().ByStatus breakdown. Make sure consent is recorded
	// server-side for actors who have consented before publishing their
	// events — see SetConsent for what that requires.
	ConsentUnknown ConsentState = "unknown"
	// ConsentGranted means analytics consent was explicitly granted.
	ConsentGranted ConsentState = "granted"
	// ConsentDenied means analytics consent was explicitly denied: events
	// are dropped at enqueue and the pending queue has been cleared.
	ConsentDenied ConsentState = "denied"
	// ConsentDeniedForcedMinor is the forced-minor denial recorded through
	// SetConsentDecision(ConsentDecisionDeniedForcedMinor): analytics-wise
	// IDENTICAL to ConsentDenied everywhere — every gate treats both as the
	// same denied state and Track/Enqueue refuse with the same
	// ErrConsentDenied — but the receipt carries reason
	// "denied_forced_minor" so the backend can tell a band-forced denial
	// from a chosen one. With SpoolDir it persists and reloads in either mode.
	// Ordinary denial is a no-op; ordinary grant cannot reverse it.
	ConsentDeniedForcedMinor ConsentState = "denied_forced_minor"
)

// ConsentDecision is an explicit consent decision for SetConsentDecision.
// Exactly three values are accepted; anything else is
// ErrInvalidConsentDecision.
type ConsentDecision string

const (
	ConsentDecisionGranted           ConsentDecision = "granted"
	ConsentDecisionDenied            ConsentDecision = "denied"
	ConsentDecisionDeniedForcedMinor ConsentDecision = "denied_forced_minor"
)

// ConsentResult describes an applied local decision. Warnings report this
// call's unfinished persistence, receipt or purge work, or omitted invalid notice
// metadata; they do not certify
// server-side acceptance. A refused decision returns an error instead.
type ConsentResult struct {
	Warnings []string
}

func (r *ConsentResult) warn(code string) {
	for _, existing := range r.Warnings {
		if existing == code {
			return
		}
	}
	r.Warnings = append(r.Warnings, code)
}

// consentDecisionReason is the only reason value a receipt ever carries,
// riding forced-minor decisions on the stored entry and the wire body.
const consentDecisionReason = "denied_forced_minor"

const (
	consentStateUnknown int32 = iota
	consentStateGranted
	consentStateDenied
	consentStateDeniedForcedMinor
)

// consentSendBuffer bounds the pending consent decisions awaiting the
// single ordered sender. When it overflows, the oldest pending decision is
// discarded: the newest decision supersedes it under the server's
// last-writer-wins semantics, and SetConsent never blocks on the network.
const consentSendBuffer = 16

// consentGateState is one denial generation of the in-flight publish gate:
// ctx is cancelled when consent is denied, aborting event publishes started
// under an earlier granted/unknown state. Each denial installs a fresh gate
// so publishes after a later re-grant are not affected by past denials.
type consentGateState struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newConsentGateState() *consentGateState {
	ctx, cancel := context.WithCancel(context.Background())
	return &consentGateState{ctx: ctx, cancel: cancel}
}

type consentRequest struct {
	ConsentNotice
	WorkspaceID     string          `json:"workspace_id"`
	AppID           string          `json:"app_id"`
	EnvironmentID   string          `json:"environment_id"`
	ActorIdentifier string          `json:"actor_identifier"`
	Categories      map[string]bool `json:"categories"`
	DecidedAt       string          `json:"decided_at"`
	IdempotencyKey  string          `json:"idempotency_key"`
	// Reason rides forced-minor denials only ("denied_forced_minor");
	// absent on every other decision.
	Reason string `json:"reason,omitempty"`
}

type consentResult struct {
	Recorded bool `json:"recorded"`
	Replayed bool `json:"replayed"`
}

// SetConsent records an explicit analytics consent decision.
//
// Locally it is synchronous: denied consent immediately starts rejecting
// Track/Enqueue with ErrConsentDenied, clears the pending queue (cleared
// events count as Dropped), and aborts any event batch publish already in
// flight on the network (the aborted events count as Dropped, never as
// Published). Grants require settled purge debt and no forced-minor restriction.
// Nil error means the local decision applied. ConsentResult.Warnings reports
// unfinished durability/purge work; a refused decision changes no state.
// An invalid actor permits local denial with consent_actor_invalid and no
// receipt, while its grant is refused. Ordinary denial after forced-minor
// succeeds without changing ConsentDeniedForcedMinor or minting a receipt.
//
// An optional notice supplies all three provenance identifiers for this call.
// Invalid or multiple notices refuse grants with ErrInvalidConsentNotice;
// denials still apply with consent_notice_invalid and omit the whole tuple.
// Omission preserves existing behavior. A forced-minor-preserving no-op never
// replaces the original receipt or its notice.
//
// Remotely it is fire-and-forget for the caller: the decision is handed to
// a single per-client sender goroutine that posts to
// POST {ingest}/v1/consent with the batch transport credentials, using
// Config.UserID (preferred) or Config.AnonymousID as the actor identifier.
// SetConsent never blocks on the network, and decisions are transmitted in
// call order (a deny-then-grant cannot arrive at the server reversed).
// Failures are logged quietly through Config.Logger and never affect the
// local state. If neither identity field is configured, the decision is
// applied locally only. Close waits (bounded by its context) for decisions
// recorded before it was called to finish transmitting; decisions recorded
// after Close are refused with ErrConsentShutdown. Consent
// never rides the event envelope.
//
// On a strict-consent (enforce) workspace an explicit grant is what admits
// the actor's events: without a consent decision recorded server-side the
// ingest endpoint terminally suppresses each event as suppressed_no_consent
// inside the 202 — see ConsentUnknown. Because the receipt posts
// fire-and-forget in the background, calling SetConsent(true) immediately
// before publishing does NOT synchronize the grant: events flushed before
// the /v1/consent write lands are still suppressed, and the SDK exposes no
// per-receipt success signal (failures are only logged). When admission
// must be guaranteed from the first event, record the grant out-of-band
// through a consent-write-capable service credential before publishing, and
// watch Config.OnBatchResult or the Snapshot().ByStatus breakdown for
// suppressed_no_consent to detect the race. The receipt also covers only
// the configured actor (Config.UserID, else Config.AnonymousID); events
// that override the actor per event (Event.UserID or Event.AnonymousID)
// need consent recorded for each such actor through a service path. Grants
// are recorded server-side only through a consent-write-capable service
// credential; a publishable Mode A client key may record denials only.
//
// See ConsentState for restart behavior. When Config.SpoolDir is set, an
// applied decision is additionally persisted
// (consent.json) and the disk spool follows it: denial purges the spool (a
// failed purge owes a wipe and fails the spool closed until it succeeds),
// and spool writes open only under a granted live state whose record was
// successfully persisted. A failed purge also refuses a subsequent live grant.
func (c *Client) SetConsent(analyticsGranted bool, notice ...ConsentNotice) (ConsentResult, error) {
	decision := ConsentDecisionGranted
	if !analyticsGranted {
		decision = ConsentDecisionDenied
	}
	return c.applyConsentDecision(decision, notice...)
}

// SetConsentDecision records an explicit consent decision in its typed
// form. ConsentDecisionGranted and ConsentDecisionDenied behave exactly
// like SetConsent(true)/SetConsent(false). ConsentDecisionDeniedForcedMinor
// is the forced-minor denial: analytics-wise identical to a denial — the
// full denial path runs and every gate treats the state as denied — with
// the receipt carrying reason "denied_forced_minor" so the backend can tell
// a band-forced denial from a chosen one. On an active client, any other
// value is rejected with ErrInvalidConsentDecision and NOTHING is applied.
//
// Delivery of the decision follows the client's mode: under the opt-in
// consent floor (Config.ConsentFloor) the receipt rides the durable outbox
// — retained, retried until acknowledged, delivered in decision order, with
// durability failures surfaced in the returned ConsentResult.Warnings as
// well as Stats and Close's ErrConsentPending backstop; without the
// floor it posts fire-and-forget exactly like SetConsent.
func (c *Client) SetConsentDecision(decision ConsentDecision, notice ...ConsentNotice) (ConsentResult, error) {
	return c.applyConsentDecision(decision, notice...)
}

func (c *Client) applyConsentDecision(decision ConsentDecision, notices ...ConsentNotice) (ConsentResult, error) {
	if c == nil || c.queue == nil {
		return ConsentResult{}, ErrNotInitialized
	}
	// FAST HALF, under lifecycleMu: a denial takes effect on intake
	// IMMEDIATELY — before any disk work, this call's or an earlier
	// decision's. A denial issued while a predecessor's record write stalls
	// on a slow SpoolDir must reject Track/Enqueue from this moment, so the
	// in-memory flip never queues behind disk. The ticket taken here fixes
	// this decision's place in the total decision order; the slow half below
	// runs strictly in ticket order. Admission for the Close fence is
	// decided here too: closed is stored under this same mutex, so "admitted
	// before Close" is exact (see consentDecisionsWG).
	c.lifecycleMu.Lock()
	if c.closed.Load() {
		c.lifecycleMu.Unlock()
		return ConsentResult{}, ErrConsentShutdown
	}
	// A terminal client cannot be repaired by changing its input. Admit
	// lifecycle state before validating either setter's decision or actor.
	switch decision {
	case ConsentDecisionGranted, ConsentDecisionDenied, ConsentDecisionDeniedForcedMinor:
	default:
		c.lifecycleMu.Unlock()
		return ConsentResult{}, ErrInvalidConsentDecision
	}
	var result ConsentResult
	notice, validNotice := snapshotConsentNotice(notices)
	if !validNotice {
		if decision == ConsentDecisionGranted {
			c.lifecycleMu.Unlock()
			return result, ErrInvalidConsentNotice
		}
		// Optional metadata cannot block a denial. Keep every other warning.
		result.warn("consent_notice_invalid")
	}
	invalidActor := c.validateConsentFloorIdentity() != nil
	if invalidActor && decision == ConsentDecisionGranted {
		c.lifecycleMu.Unlock()
		return result, ErrInvalidConsentIdentity
	}
	if invalidActor {
		result.warn("consent_actor_invalid")
	}
	analyticsGranted := decision == ConsentDecisionGranted
	state := consentStateGranted
	switch decision {
	case ConsentDecisionDenied:
		state = consentStateDenied
	case ConsentDecisionDeniedForcedMinor:
		state = consentStateDeniedForcedMinor
	}

	actor := firstNonEmpty(c.cfg.UserID, c.cfg.AnonymousID)

	if c.consent.Load() == consentStateDeniedForcedMinor {
		if analyticsGranted {
			c.lifecycleMu.Unlock()
			return ConsentResult{}, ErrConsentForcedMinor
		}
		if decision == ConsentDecisionDenied {
			// This denial is already effective. Keep its provenance and
			// existing durability work intact; a reasonless receipt would
			// erase the server's forced-minor reason.
			c.lifecycleMu.Unlock()
			return result, nil
		}
	}
	ticket := c.consentTicketNext
	c.consentTicketNext++
	denialEpoch := c.consentEpoch.Load()
	c.consentDecisionsWG.Add(1)
	grantArming := c.consentFloorEnabled() && analyticsGranted
	if grantArming {
		// Arm the dispatch gate BEFORE the granted state becomes visible:
		// the receipt appends in the ticket-ordered slow half, and a
		// concurrent event leg must not slip a batch out in the window
		// between the observable grant and the receipt's existence (see
		// consentGrantArming).
		c.consentGrantArming.Add(1)
	}
	if !analyticsGranted {
		c.consent.Store(state)
		// Bump the denial epoch BEFORE draining the shared queue: events the
		// worker already pulled into its local batch are invisible to
		// drainAll, and the worker drops them (counting them as Dropped)
		// when it next observes the moved epoch. Events enqueued before this
		// denial therefore never survive into a later granted period.
		c.consentEpoch.Add(1)
		// Abort any event publish already in flight: cancel the current gate
		// and install a fresh one for publishes after a later re-grant. The
		// denied state was stored above, so a publisher that misses this
		// cancellation (it loaded the fresh gate) instead sees the denial on
		// its post-load re-check.
		if gate := c.consentGate.Swap(newConsentGateState()); gate != nil {
			gate.cancel()
		}
		if dropped := c.queue.drainAll(); dropped > 0 {
			c.stats.dropped.Add(uint64(dropped))
		}
		if c.exp != nil {
			// The drain above discarded any queued-but-unpublished
			// experiment exposure facts: re-arm this session's emissions so
			// a later re-grant of a retained assignment emits its exposure
			// again (already-published — or wire-ambiguous mid-flight —
			// facts collapse server-side on their deterministic event ids)
			// instead of under-counting real treatment.
			c.exp.onAnalyticsPurge()
		}
	}
	c.lifecycleMu.Unlock()

	if gate := c.consentSlowHalfGate; gate != nil {
		// Test seam: the fast half is published (live state flipped, epoch
		// bumped, gate swapped) but the slow half has not started — no
		// receipt exists yet and the record-apply lock is free. This is the
		// window the grant handoff's fast-half check parks against.
		gate()
	}

	// SLOW HALF, in ticket order: disk persistence, then the sender handoff.
	// The wait keeps overlapping decisions' disk writes and transmissions in
	// the decision order (the LAST decision's record lands last, and the
	// server receives decisions in call order), while intake above never
	// waits — only later DECISIONS queue behind a stalled write, exactly as
	// they did when one mutex covered everything.
	c.consentTurnMu.Lock()
	for c.consentTicketServing != ticket {
		c.consentTurnCondLocked().Wait()
	}
	c.consentTurnMu.Unlock()

	// The floor retry must not reapply an older denied record between the
	// purge check and this decision's durable pair. Intake never takes this
	// lock: a later denial can still close analytics immediately.
	if c.consentFloorEnabled() {
		c.consentRecordApplyMu.Lock()
	}
	finish := func() {
		if c.consentFloorEnabled() {
			c.consentRecordApplyMu.Unlock()
		}
		if grantArming {
			c.consentGrantArming.Add(-1)
			c.wakeConsentDispatch()
		}
		c.consentTurnMu.Lock()
		c.consentTicketServing++
		if c.consentTurnCond != nil {
			c.consentTurnCond.Broadcast()
		}
		c.consentTurnMu.Unlock()
		c.consentDecisionsWG.Done()
	}
	if analyticsGranted {
		// Earlier decisions have finished their disk half. Settle their
		// purge debt BEFORE applying this grant, minting its receipt, or
		// replacing their denied record.
		if c.spool != nil && !c.spool.settleOwedWipe() {
			finish()
			return ConsentResult{}, ErrSpoolPurgeFailed
		}
		c.lifecycleMu.Lock()
		// A newer denial may already have taken effect while this ticket
		// waited. Preserve it; its durable half follows this one in order.
		if c.consentEpoch.Load() == denialEpoch {
			c.consent.Store(state)
		}
		c.lifecycleMu.Unlock()
	}
	applyRecord := func(decision ConsentDecision, stamp string) ([]SpoolDeadLetter, bool) {
		letters, persisted, purged := c.applySpoolConsent(decision, stamp)
		if !persisted {
			result.warn("consent_persist_failed")
		}
		if !purged {
			result.warn("spool_purge_failed")
		}
		return letters, persisted
	}
	mintReceipt := func(granted bool, reason, stamp string) (consentReceipt, bool, error) {
		if invalidActor {
			// Denial is local even with an invalid actor. Never mint a
			// receipt for a truncated or replacement identity.
			return consentReceipt{}, false, nil
		}
		return c.mintConsentReceipt(granted, reason, stamp, notice)
	}
	var deadLetters []SpoolDeadLetter
	var keyErr error
	if c.consentFloorEnabled() {
		// Consent-floor delivery: exactly one receipt per explicit decision
		// rides the durable outbox — appended while still holding the turn so
		// the outbox order matches the decision order — and the worker is
		// nudged to dispatch promptly. Receipts are an append-only decision
		// trail: a later decision never withdraws an earlier receipt (a
		// grant-then-deny delivers BOTH, in order), so after a denial no
		// stale grant is ever the server's last word. Post-Close decisions
		// are refused before this path; invalid-actor denials stay local.
		//
		// DURABLE ORDERING per decision flavor (the engine SDKs' shared
		// rule: grants receipt-first, denials record-first). A crash can
		// land between the receipt append and the record write, and the
		// next launch restores whatever the disk says — so the pair must
		// be ordered so that every reachable intermediate state fails
		// CLOSED. GRANT: the receipt rides the durable outbox FIRST, and
		// the granted record is written only once the receipt trail is
		// safely down (or provably never coming — no configured actor,
		// the documented local-only path; a failed idempotency-key MINT
		// is NOT that path: the receipt is OWED and retried at every
		// dispatch point, the record withheld exactly like a failed
		// append). Record-first
		// would leave "granted record, empty outbox" reachable — a
		// relaunch flowing events with no receipt ever sent. When the
		// receipt write itself fails, the record write is WITHHELD: the
		// live grant applies in memory, the receipt write stays owed, and
		// the next launch restores the old persisted state — or, once the
		// owed receipt landed, the grant from the trail tail (healing the
		// record at reload). DENIAL: the record — and the spool purge it
		// condemns — stays FIRST (a crash after it restores denied,
		// fail-closed); the deny receipt appends after it.
		{
			reason := ""
			if decision == ConsentDecisionDeniedForcedMinor {
				reason = consentDecisionReason
			}
			// One stamp per decision, shared by the receipt AND the record:
			// the reload orders retained receipts against the record by this
			// instant (only a strictly-newer receipt may override), so both
			// artifacts of one decision must carry the same moment.
			decidedAt := c.consentDecisionStamp()
			if analyticsGranted {
				// The whole grant side runs under the record-apply lock so
				// the owed-mint slot, the owed-record slot, and the
				// per-receipt pair marks move together — an opportunistic
				// retry (TryLock) can never interleave between them.
				receiptTrailSafe := true
				receipt, minted, mintErr := mintReceipt(true, reason, decidedAt)
				switch {
				case mintErr != nil:
					result.warn("consent_outbox_persist_failed")
					// The receipt could not even be minted for a CONFIGURED
					// actor: it is OWED — retried at every dispatch point —
					// and the trail is unsafe exactly like a failed append,
					// so the granted record is withheld below. Only the
					// actorless local-only path may persist receipt-less.
					receiptTrailSafe = false
					c.setConsentMintOwed(&consentOwedMint{decision: decision, analyticsGranted: true, reason: reason, decidedAt: decidedAt, notice: notice})
					c.stats.setLastConsentError("consent_receipt_mint_failed")
					c.logf("shardpilot consent floor: minting the grant receipt's idempotency key failed; the receipt is owed (retried at every dispatch point) and the granted record is withheld until it lands: %v", mintErr)
				case minted:
					// A successful mint supersedes any older owed mint (the
					// slot holds the newest decision only; appending the
					// older receipt later would break trail order).
					c.setConsentMintOwed(nil)
					if c.consentOutbox.append(receipt) {
						c.recordConsentOutboxPersistFailure()
						result.warn("consent_outbox_persist_failed")
						receiptTrailSafe = false
					}
					c.drainConsentOutboxEvictions()
					c.wakeConsentDispatch()
				default:
					// No configured actor: the documented local-only path —
					// a receipt is provably never coming, and the record may
					// persist without one. Supersedes any older owed mint.
					c.setConsentMintOwed(nil)
				}
				if receiptTrailSafe {
					var recordPersisted bool
					deadLetters, recordPersisted = applyRecord(decision, decidedAt)
					c.setConsentRecordOwed(decision, decidedAt, recordPersisted)
					if minted && !recordPersisted {
						// The pair-incomplete hold is PER RECEIPT (the single
						// owed slot tracks only the newest decision — a later
						// decision's failure must not release this one).
						c.consentOutbox.markRecordOwed(receipt.IdempotencyKey)
					}
				} else {
					// The record write is WITHHELD (receipt-first) and OWED:
					// the retry at every dispatch point completes the pair
					// the moment the outbox write (or the owed mint) lands —
					// an acknowledged receipt must never prune away leaving
					// no durable grant.
					result.warn("consent_persist_failed")
					c.setConsentRecordOwed(decision, decidedAt, false)
					if minted {
						c.consentOutbox.markRecordOwed(receipt.IdempotencyKey)
					}
					c.logf("shardpilot consent floor: the grant receipt could not be written durably; the granted record is withheld (owed — completed when the receipt write lands; a restart meanwhile restores the prior state, or the grant from the trail tail once the owed receipt landed)")
				}
			} else {
				// The denial side holds the record-apply lock across the
				// record write AND the receipt mint/append for the same
				// reason as the grant side: the owed slots and the
				// per-receipt marks must move together.
				var recordPersisted bool
				deadLetters, recordPersisted = applyRecord(decision, decidedAt)
				// A failed denied-record write is OWED: retried at every
				// dispatch point, and until it lands the denial's in-scope
				// proof receipt is HELD from dispatch (consentDenyProofHeld
				// plus the per-receipt mark) so the trail's only durable
				// evidence cannot prune away while the stale pre-denial
				// record would rule a restart.
				c.setConsentRecordOwed(decision, decidedAt, recordPersisted)
				receipt, minted, mintErr := mintReceipt(false, reason, decidedAt)
				switch {
				case mintErr != nil:
					result.warn("consent_outbox_persist_failed")
					// The deny receipt is OWED to the failed mint (retried at
					// every dispatch point; Close pends until it lands). The
					// record was already written FIRST — fail-closed exactly
					// as a failed append would leave it.
					c.setConsentMintOwed(&consentOwedMint{decision: decision, analyticsGranted: false, reason: reason, decidedAt: decidedAt, notice: notice})
					c.stats.setLastConsentError("consent_receipt_mint_failed")
					c.logf("shardpilot consent floor: minting the denial receipt's idempotency key failed; the receipt is owed and retried at every dispatch point (the denied record was written first): %v", mintErr)
				case minted:
					c.setConsentMintOwed(nil)
					if c.consentOutbox.append(receipt) {
						c.recordConsentOutboxPersistFailure()
						result.warn("consent_outbox_persist_failed")
					}
					c.drainConsentOutboxEvictions()
					if !recordPersisted {
						c.consentOutbox.markRecordOwed(receipt.IdempotencyKey)
					}
					c.wakeConsentDispatch()
				default:
					c.setConsentMintOwed(nil)
				}
			}
		}

	} else {
		// Floor-off: the record/spool side applies before the fire-and-forget
		// post. Persistence failures are warnings; only the restrictive
		// forced-minor marker restores live denial at the next launch.
		// The record carries this decision's stamp and NO floor provenance:
		// a later floor enablement must not promote a fire-and-forget-era
		// grant (its POST may have failed; no receipt exists) to live state.
		deadLetters, _ = applyRecord(decision, c.consentDecisionStamp())
		if actor != "" && !invalidActor {
			idempotencyKey, err := uuidv7.New()
			if err != nil {
				keyErr = err
				result.warn("consent_outbox_persist_failed")
			} else {
				// Hand off while still holding the turn so the transmission
				// order matches the decision order across concurrent
				// SetConsent calls (the turn is the single producer on
				// consentSends).
				request := consentRequest{
					ConsentNotice:   notice,
					WorkspaceID:     c.cfg.WorkspaceID,
					AppID:           c.cfg.AppID,
					EnvironmentID:   c.cfg.EnvironmentID,
					ActorIdentifier: actor,
					Categories:      map[string]bool{"analytics": analyticsGranted},
					DecidedAt:       c.clock.Now().UTC().Format(time.RFC3339),
					IdempotencyKey:  idempotencyKey,
				}
				if decision == ConsentDecisionDeniedForcedMinor {
					// Without the floor the forced-minor decision still
					// applies its full denial semantics; the reason rides
					// the fire-and-forget receipt (best-effort, like every
					// legacy consent post).
					request.Reason = consentDecisionReason
				}
				c.enqueueConsentPublish(request)
			}
		}
	}

	// Release every decision lock before callbacks may re-enter a setter.
	finish()

	c.emitSpoolDeadLetters(deadLetters)
	if !c.consentFloorEnabled() && actor == "" {
		c.logf("shardpilot consent: no actor identity configured (Config.UserID or Config.AnonymousID); decision applied locally only")
	} else if keyErr != nil {
		c.logf("shardpilot consent: generate idempotency key failed: %v", keyErr)
	}
	return result, nil
}

// consentTurnCondLocked returns the turn condition variable, materializing
// it on first need. Must be called with consentTurnMu held. NewClient
// initializes the cond eagerly; the lazy path exists for bare Clients
// constructed by tests, which never run NewClient.
func (c *Client) consentTurnCondLocked() *sync.Cond {
	if c.consentTurnCond == nil {
		c.consentTurnCond = sync.NewCond(&c.consentTurnMu)
	}
	return c.consentTurnCond
}

// startConsentSender starts the single consent sender goroutine exactly
// once. Close also calls it (after closing c.stop) so consentSenderDone is
// guaranteed to close even when no decision was ever recorded.
func (c *Client) startConsentSender() {
	c.consentSenderOnce.Do(func() {
		go c.consentSender()
	})
}

// enqueueConsentPublish hands a decision to the single ordered consent
// sender, starting it lazily on first use. Must be called while holding the
// consent ticket turn (SetConsent's slow half) — the turn is the only
// producer on consentSends, which keeps the drop-oldest overflow handling
// race-free on the producer side.
func (c *Client) enqueueConsentPublish(request consentRequest) {
	c.startConsentSender()
	for {
		select {
		case c.consentSends <- request:
			return
		default:
		}
		// The backlog is full: discard the oldest pending decision. The
		// newer decisions supersede it server-side (last-writer-wins), and
		// the caller must never block on the network.
		select {
		case stale := <-c.consentSends:
			c.logf("shardpilot consent: publish backlog full; dropped pending decision (idempotency key %s)", stale.IdempotencyKey)
		default:
		}
	}
}

// consentSender is the single goroutine that transmits consent decisions in
// the order they were recorded. It exits once the client stops, after
// flushing any decisions still pending at that point; consentSenderDone is
// closed on exit so Close can wait for that flush.
func (c *Client) consentSender() {
	defer close(c.consentSenderDone)
	for {
		select {
		case request := <-c.consentSends:
			c.publishConsent(request)
		case <-c.stop:
			for {
				select {
				case request := <-c.consentSends:
					c.publishConsent(request)
				default:
					return
				}
			}
		}
	}
}

func (c *Client) publishConsent(request consentRequest) {
	// Unbounded here on purpose: the transport bounds each HTTP attempt by
	// HTTPTimeout, so a gzip-refusal fallback gets its own budget instead of
	// the refusal's leftovers.
	if _, err := c.transport.PublishConsent(context.Background(), request); err != nil {
		c.logf("shardpilot consent publish failed: %v", err)
	}
}

// ConsentState returns the current in-memory consent state.
// A zero-value Client reports ConsentUnknown. Reading the state has no side effects.
func (c *Client) ConsentState() ConsentState {
	switch c.consent.Load() {
	case consentStateGranted:
		return ConsentGranted
	case consentStateDenied:
		return ConsentDenied
	case consentStateDeniedForcedMinor:
		return ConsentDeniedForcedMinor
	default:
		return ConsentUnknown
	}
}

// consentDenied treats both denial flavors identically: the forced-minor
// state gates analytics exactly like an ordinary denial.
func (c *Client) consentDenied() bool {
	switch c.consent.Load() {
	case consentStateDenied, consentStateDeniedForcedMinor:
		return true
	default:
		return false
	}
}

// consentUndecided reports the unknown state, which the opt-in consent
// floor treats as closed (ErrConsentUnknown at intake).
func (c *Client) consentUndecided() bool {
	return c.consent.Load() == consentStateUnknown
}

func (c *Client) logf(format string, args ...any) {
	if c.cfg.Logger != nil {
		c.cfg.Logger.Printf(format, args...)
	}
}
