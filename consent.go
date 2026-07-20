package shardpilot

import (
	"context"
	"sync"
	"time"

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
)

// ConsentState is the tri-state analytics consent of the configured actor.
//
// The state lives in client memory only; the SDK does not persist it. An
// integrator that needs consent to survive process restarts reads Consent
// after SetConsent, stores it, and re-applies it with SetConsent on startup.
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
	// from a chosen one. Under the consent floor with SpoolDir it persists
	// as its own state and reloads as the same state.
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
// Published). Granting re-opens the pipeline.
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
// after Close are applied locally but are no longer transmitted. Consent
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
// The live state is held in memory only; see ConsentState for persistence
// notes. When Config.SpoolDir is set, the DECISION is additionally persisted
// (consent.json) and the disk spool follows it: denial purges the spool (a
// failed purge owes a wipe and fails the spool closed until it succeeds),
// and spool writes open only under a granted live state whose record was
// successfully persisted — a strictly grant-only disk posture that leaves
// the live pipeline's documented behavior untouched.
func (c *Client) SetConsent(analyticsGranted bool) {
	decision := ConsentDecisionGranted
	if !analyticsGranted {
		decision = ConsentDecisionDenied
	}
	if err := c.applyConsentDecision(decision); err != nil {
		// Only the floor's identity gate can reject here (the decision
		// value is always valid), and this void legacy surface has nowhere
		// to return it: surface loudly instead of applying half a decision.
		c.logf("shardpilot consent: decision rejected, nothing applied: %v", err)
	}
}

// SetConsentDecision records an explicit consent decision in its typed
// form. ConsentDecisionGranted and ConsentDecisionDenied behave exactly
// like SetConsent(true)/SetConsent(false). ConsentDecisionDeniedForcedMinor
// is the forced-minor denial: analytics-wise identical to a denial — the
// full denial path runs and every gate treats the state as denied — with
// the receipt carrying reason "denied_forced_minor" so the backend can tell
// a band-forced denial from a chosen one. Any other value is rejected with
// ErrInvalidConsentDecision and NOTHING is applied.
//
// Delivery of the decision follows the client's mode: under the opt-in
// consent floor (Config.ConsentFloor) the receipt rides the durable outbox
// — retained, retried until acknowledged, delivered in decision order, with
// durability failures surfaced through Stats (ConsentOutboxPersistFailed,
// LastConsentError) and Close's ErrConsentPending backstop; without the
// floor it posts fire-and-forget exactly like SetConsent.
func (c *Client) SetConsentDecision(decision ConsentDecision) error {
	switch decision {
	case ConsentDecisionGranted, ConsentDecisionDenied, ConsentDecisionDeniedForcedMinor:
	default:
		return ErrInvalidConsentDecision
	}
	return c.applyConsentDecision(decision)
}

func (c *Client) applyConsentDecision(decision ConsentDecision) error {
	if c.consentFloorEnabled() {
		// The floor requires in-contract identifiers BEFORE anything
		// applies: a configured identifier over the receipt clamp would
		// force the receipt onto a different actor than events carry (go's
		// event path stamps identifiers verbatim, deliberately unclamped),
		// so the decision is rejected whole — reject, never truncate,
		// never silently mint for a substitute actor.
		if err := c.validateConsentFloorIdentity(); err != nil {
			return err
		}
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

	// FAST HALF, under lifecycleMu: the decision takes effect on intake
	// IMMEDIATELY — before any disk work, this call's or an earlier
	// decision's. A denial issued while a predecessor's record write stalls
	// on a slow SpoolDir must reject Track/Enqueue from this moment, so the
	// in-memory flip never queues behind disk. The ticket taken here fixes
	// this decision's place in the total decision order; the slow half below
	// runs strictly in ticket order. Admission for the Close fence is
	// decided here too: closed is stored under this same mutex, so "admitted
	// before Close" is exact (see consentDecisionsWG).
	c.lifecycleMu.Lock()
	ticket := c.consentTicketNext
	c.consentTicketNext++
	admitted := !c.closed.Load()
	if admitted {
		c.consentDecisionsWG.Add(1)
	}
	grantArming := c.consentFloorEnabled() && admitted && analyticsGranted
	if grantArming {
		// Arm the dispatch gate BEFORE the granted state becomes visible:
		// the receipt appends in the ticket-ordered slow half, and a
		// concurrent event leg must not slip a batch out in the window
		// between the observable grant and the receipt's existence (see
		// consentGrantArming).
		c.consentGrantArming.Add(1)
	}
	c.consent.Store(state)
	if !analyticsGranted {
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
	}
	c.lifecycleMu.Unlock()

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

	// Disk side of the decision (no-op without SpoolDir), deliberately
	// outside lifecycleMu: it fsyncs files, and event intake must not wait
	// out a disk stall. The spool's own append gate re-checks the already
	// stored live state under its lock, so a batch racing this section can
	// never re-create a record the purge below condemns. Under the FLOOR a
	// post-Close decision is memory-only in full: with the persisted
	// decision feeding the next launch's LIVE state, writing it here would
	// resurrect a decision whose receipt was never sent (and never will
	// be) — the floor's applied-locally-only means exactly the in-memory
	// state, nothing durable. Floor-off keeps writing, unchanged: there the
	// record only ever gates the next launch's spool, never the live state.
	var deadLetters []SpoolDeadLetter
	var keyErr error
	if c.consentFloorEnabled() {
		// Consent-floor delivery: exactly one receipt per explicit decision
		// rides the durable outbox — appended while still holding the turn so
		// the outbox order matches the decision order — and the worker is
		// nudged to dispatch promptly. Receipts are an append-only decision
		// trail: a later decision never withdraws an earlier receipt (a
		// grant-then-deny delivers BOTH, in order), so after a denial no
		// stale grant is ever the server's last word. A decision recorded
		// AFTER Close keeps the documented applied-locally-only posture:
		// no receipt is minted, retained, or persisted — a durable
		// post-Close receipt would transmit at the NEXT launch, which
		// "no longer transmitted" promises not to do.
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
		if admitted {
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
				c.consentRecordApplyMu.Lock()
				receiptTrailSafe := true
				receipt, minted, mintErr := c.mintConsentReceipt(true, reason, decidedAt)
				switch {
				case mintErr != nil:
					// The receipt could not even be minted for a CONFIGURED
					// actor: it is OWED — retried at every dispatch point —
					// and the trail is unsafe exactly like a failed append,
					// so the granted record is withheld below. Only the
					// actorless local-only path may persist receipt-less.
					receiptTrailSafe = false
					c.setConsentMintOwed(&consentOwedMint{decision: decision, analyticsGranted: true, reason: reason, decidedAt: decidedAt})
					c.stats.setLastConsentError("consent_receipt_mint_failed")
					c.logf("shardpilot consent floor: minting the grant receipt's idempotency key failed; the receipt is owed (retried at every dispatch point) and the granted record is withheld until it lands: %v", mintErr)
				case minted:
					// A successful mint supersedes any older owed mint (the
					// slot holds the newest decision only; appending the
					// older receipt later would break trail order).
					c.setConsentMintOwed(nil)
					if c.consentOutbox.append(receipt) {
						c.recordConsentOutboxPersistFailure()
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
					deadLetters, recordPersisted = c.applySpoolConsent(decision, decidedAt)
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
					c.setConsentRecordOwed(decision, decidedAt, false)
					if minted {
						c.consentOutbox.markRecordOwed(receipt.IdempotencyKey)
					}
					c.logf("shardpilot consent floor: the grant receipt could not be written durably; the granted record is withheld (owed — completed when the receipt write lands; a restart meanwhile restores the prior state, or the grant from the trail tail once the owed receipt landed)")
				}
				c.consentRecordApplyMu.Unlock()
			} else {
				// The denial side holds the record-apply lock across the
				// record write AND the receipt mint/append for the same
				// reason as the grant side: the owed slots and the
				// per-receipt marks must move together.
				c.consentRecordApplyMu.Lock()
				var recordPersisted bool
				deadLetters, recordPersisted = c.applySpoolConsent(decision, decidedAt)
				// A failed denied-record write is OWED: retried at every
				// dispatch point, and until it lands the denial's in-scope
				// proof receipt is HELD from dispatch (consentDenyProofHeld
				// plus the per-receipt mark) so the trail's only durable
				// evidence cannot prune away while the stale pre-denial
				// record would rule a restart.
				c.setConsentRecordOwed(decision, decidedAt, recordPersisted)
				receipt, minted, mintErr := c.mintConsentReceipt(false, reason, decidedAt)
				switch {
				case mintErr != nil:
					// The deny receipt is OWED to the failed mint (retried at
					// every dispatch point; Close pends until it lands). The
					// record was already written FIRST — fail-closed exactly
					// as a failed append would leave it.
					c.setConsentMintOwed(&consentOwedMint{decision: decision, analyticsGranted: false, reason: reason, decidedAt: decidedAt})
					c.stats.setLastConsentError("consent_receipt_mint_failed")
					c.logf("shardpilot consent floor: minting the denial receipt's idempotency key failed; the receipt is owed and retried at every dispatch point (the denied record was written first): %v", mintErr)
				case minted:
					c.setConsentMintOwed(nil)
					if c.consentOutbox.append(receipt) {
						c.recordConsentOutboxPersistFailure()
					}
					c.drainConsentOutboxEvictions()
					if !recordPersisted {
						c.consentOutbox.markRecordOwed(receipt.IdempotencyKey)
					}
					c.wakeConsentDispatch()
				default:
					c.setConsentMintOwed(nil)
				}
				c.consentRecordApplyMu.Unlock()
			}
		}
		if grantArming {
			// The receipt now exists in the outbox, is owed to a failed
			// mint (the owed-mint gate holds the batch legs until the
			// retried mint appends it), or provably never will exist (no
			// configured actor): the outbox/owed-mint predicates take over
			// from the arming window either way, and the gate must not
			// stay stuck for a receipt that cannot come. Re-wake the
			// dispatcher: a pass that ran during
			// the window HELD the grant (consentGrantPairIncomplete) and
			// returned without arming any deferral, so without this nudge
			// the receipt would idle until the next tick or caller op.
			c.consentGrantArming.Add(-1)
			c.wakeConsentDispatch()
		}
	} else {
		// Floor-off: the record/spool side applies unconditionally — even
		// post-Close, where the record only ever gates the NEXT launch's
		// spool, never any live state — and the legacy fire-and-forget
		// post follows. A failed record write stays log-only here (no owed
		// machinery: without the floor the record never feeds live state).
		// The record carries this decision's stamp and NO floor provenance:
		// a later floor enablement must not promote a fire-and-forget-era
		// grant (its POST may have failed; no receipt exists) to live state.
		deadLetters, _ = c.applySpoolConsent(decision, c.consentDecisionStamp())
		if actor != "" {
			idempotencyKey, err := uuidv7.New()
			if err != nil {
				keyErr = err
			} else {
				// Hand off while still holding the turn so the transmission
				// order matches the decision order across concurrent
				// SetConsent calls (the turn is the single producer on
				// consentSends).
				request := consentRequest{
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

	// Release the turn BEFORE the dead-letter callback runs: the callback is
	// integrator code and may call back into the client — including
	// SetConsent itself, which must be able to take the next ticket. The
	// cond is read under the same mutex as the serving counter, so a waiter
	// that materializes it concurrently is either already released by the
	// increment (its loop re-check) or found here and woken.
	c.consentTurnMu.Lock()
	c.consentTicketServing++
	turnCond := c.consentTurnCond
	c.consentTurnMu.Unlock()
	if turnCond != nil {
		turnCond.Broadcast()
	}
	if admitted {
		// The decision is fully settled — record written (or its failure
		// logged), spool side applied, transmission handed off — so the
		// Close fence may pass. The dead-letter callback below is not
		// fenced, matching its existing after-the-locks posture.
		c.consentDecisionsWG.Done()
	}

	c.emitSpoolDeadLetters(deadLetters)
	if !c.consentFloorEnabled() && actor == "" {
		c.logf("shardpilot consent: no actor identity configured (Config.UserID or Config.AnonymousID); decision applied locally only")
	} else if keyErr != nil {
		c.logf("shardpilot consent: generate idempotency key failed: %v", keyErr)
	}
	return nil
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
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.HTTPTimeout)
	defer cancel()
	if _, err := c.transport.PublishConsent(ctx, request); err != nil {
		c.logf("shardpilot consent publish failed: %v", err)
	}
}

// Consent returns the current in-memory consent state.
func (c *Client) Consent() ConsentState {
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
