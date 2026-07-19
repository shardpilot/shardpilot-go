package shardpilot

import (
	"context"
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
)

const (
	consentStateUnknown int32 = iota
	consentStateGranted
	consentStateDenied
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
	state := consentStateGranted
	if !analyticsGranted {
		state = consentStateDenied
	}

	actor := firstNonEmpty(c.cfg.UserID, c.cfg.AnonymousID)

	// consentMu serializes the whole decision; lifecycleMu guards only the
	// in-memory flip below and is released BEFORE the disk side runs, so a
	// slow SpoolDir write never stalls Track/Enqueue intake behind a consent
	// decision (see the field comment for the lock order).
	c.consentMu.Lock()
	c.lifecycleMu.Lock()
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
	// Disk side of the decision (no-op without SpoolDir), deliberately
	// outside lifecycleMu: it fsyncs files, and event intake must not wait
	// out a disk stall. The spool's own append gate re-checks the already
	// stored live state under its lock, so a batch racing this section can
	// never re-create a record the purge below condemns. Dead-letters are
	// collected here and emitted only after consentMu is released — the
	// callback is integrator code and may call back into the client
	// (including SetConsent itself).
	deadLetters := c.applySpoolConsent(analyticsGranted)

	if actor == "" {
		c.consentMu.Unlock()
		c.emitSpoolDeadLetters(deadLetters)
		c.logf("shardpilot consent: no actor identity configured (Config.UserID or Config.AnonymousID); decision applied locally only")
		return
	}

	idempotencyKey, err := uuidv7.New()
	if err != nil {
		c.consentMu.Unlock()
		c.emitSpoolDeadLetters(deadLetters)
		c.logf("shardpilot consent: generate idempotency key failed: %v", err)
		return
	}

	request := consentRequest{
		WorkspaceID:     c.cfg.WorkspaceID,
		AppID:           c.cfg.AppID,
		EnvironmentID:   c.cfg.EnvironmentID,
		ActorIdentifier: actor,
		Categories:      map[string]bool{"analytics": analyticsGranted},
		DecidedAt:       c.clock.Now().UTC().Format(time.RFC3339),
		IdempotencyKey:  idempotencyKey,
	}

	// Enqueue while still holding consentMu so the transmission order
	// matches the local state order across concurrent SetConsent calls.
	c.enqueueConsentPublish(request)
	c.consentMu.Unlock()
	c.emitSpoolDeadLetters(deadLetters)
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
// sender, starting it lazily on first use. Must be called with consentMu
// held (it is the only producer on consentSends, which keeps the
// drop-oldest overflow handling race-free on the producer side).
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
	default:
		return ConsentUnknown
	}
}

func (c *Client) consentDenied() bool {
	return c.consent.Load() == consentStateDenied
}

func (c *Client) logf(format string, args ...any) {
	if c.cfg.Logger != nil {
		c.cfg.Logger.Printf(format, args...)
	}
}
