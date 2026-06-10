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
// Remotely it is fire-and-forget: the decision is posted once to
// POST {ingest}/v1/consent with the batch transport credentials, using
// Config.UserID (preferred) or Config.AnonymousID as the actor identifier.
// Failures are logged quietly through Config.Logger and never affect the
// local state. If neither identity field is configured, the decision is
// applied locally only. Consent never rides the event envelope.
//
// The state is held in memory only; see ConsentState for persistence notes.
func (c *Client) SetConsent(analyticsGranted bool) {
	state := consentStateGranted
	if !analyticsGranted {
		state = consentStateDenied
	}

	c.lifecycleMu.Lock()
	c.consent.Store(state)
	if !analyticsGranted {
		if dropped := c.queue.drainAll(); dropped > 0 {
			c.stats.dropped.Add(uint64(dropped))
		}
	}
	c.lifecycleMu.Unlock()

	actor := firstNonEmpty(c.cfg.UserID, c.cfg.AnonymousID)
	if actor == "" {
		c.logf("shardpilot consent: no actor identity configured (Config.UserID or Config.AnonymousID); decision applied locally only")
		return
	}

	idempotencyKey, err := uuidv7.New()
	if err != nil {
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

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.HTTPTimeout)
		defer cancel()
		if _, err := c.transport.PublishConsent(ctx, request); err != nil {
			c.logf("shardpilot consent publish failed: %v", err)
		}
	}()
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
