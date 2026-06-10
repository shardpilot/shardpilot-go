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
// Track/Enqueue with ErrConsentDenied and clears the pending queue (cleared
// events count as Dropped). Granting re-opens the pipeline.
//
// Remotely it is fire-and-forget for the caller: the decision is handed to
// a single per-client sender goroutine that posts to
// POST {ingest}/v1/consent with the batch transport credentials, using
// Config.UserID (preferred) or Config.AnonymousID as the actor identifier.
// SetConsent never blocks on the network, and decisions are transmitted in
// call order (a deny-then-grant cannot arrive at the server reversed).
// Failures are logged quietly through Config.Logger and never affect the
// local state. If neither identity field is configured, the decision is
// applied locally only. Decisions recorded after Close are applied locally
// but are no longer transmitted. Consent never rides the event envelope.
//
// The state is held in memory only; see ConsentState for persistence notes.
func (c *Client) SetConsent(analyticsGranted bool) {
	state := consentStateGranted
	if !analyticsGranted {
		state = consentStateDenied
	}

	actor := firstNonEmpty(c.cfg.UserID, c.cfg.AnonymousID)

	c.lifecycleMu.Lock()
	c.consent.Store(state)
	if !analyticsGranted {
		// Bump the denial epoch BEFORE draining the shared queue: events the
		// worker already pulled into its local batch are invisible to
		// drainAll, and the worker drops them (counting them as Dropped)
		// when it next observes the moved epoch. Events enqueued before this
		// denial therefore never survive into a later granted period.
		c.consentEpoch.Add(1)
		if dropped := c.queue.drainAll(); dropped > 0 {
			c.stats.dropped.Add(uint64(dropped))
		}
	}

	if actor == "" {
		c.lifecycleMu.Unlock()
		c.logf("shardpilot consent: no actor identity configured (Config.UserID or Config.AnonymousID); decision applied locally only")
		return
	}

	idempotencyKey, err := uuidv7.New()
	if err != nil {
		c.lifecycleMu.Unlock()
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

	// Enqueue while still holding lifecycleMu so the transmission order
	// matches the local state order across concurrent SetConsent calls.
	c.enqueueConsentPublish(request)
	c.lifecycleMu.Unlock()
}

// enqueueConsentPublish hands a decision to the single ordered consent
// sender, starting it lazily on first use. Must be called with lifecycleMu
// held (it is the only producer on consentSends, which keeps the
// drop-oldest overflow handling race-free on the producer side).
func (c *Client) enqueueConsentPublish(request consentRequest) {
	c.consentSenderOnce.Do(func() {
		go c.consentSender()
	})
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
// flushing any decisions still pending at that point.
func (c *Client) consentSender() {
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
