package shardpilot

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// economyTxEventName is the canonical event name the typed resource verb
// emits (analytics.economy_tx.v1).
const economyTxEventName = "economy_tx"

// matchScopedReasons are the reason codes for which the canonical schema
// (analytics.economy_tx.v1) requires props.match_id, and the only ones for
// which it allows it. The list is the schema's, embedded here against the
// revision this SDK is built for: a transaction the schema would reject is
// refused before it is published instead of being dropped by the ingest
// inside an accepted batch.
var matchScopedReasons = []string{"match_reward", "tower_upgrade"}

func isMatchScopedReason(reason string) bool {
	for _, candidate := range matchScopedReasons {
		if candidate == reason {
			return true
		}
	}
	return false
}

// EconomyDirection is the side of the ledger an EconomyTx moves currency on.
type EconomyDirection string

const (
	// EconomySource is currency granted to the player.
	EconomySource EconomyDirection = "source"
	// EconomySink is currency spent by the player.
	EconomySink EconomyDirection = "sink"
)

// EconomyTx is the typed resource verb's input: one in-game currency ledger
// transaction reported by the backend that keeps the ledger. TrackEconomyTx
// and EnqueueEconomyTx build the canonical `economy_tx` event from it. That
// event's schema pins source to "backend" and requires props.direction,
// props.currency_type, props.reason and props.amount; match_id is
// conditional (see the field).
//
// The verb does not relax the backend pin. A client configured with any
// other Source is refused with ErrBackendSourceRequired rather than having
// its source rewritten for the event: a client asserting its own economy is
// exactly what the backend lane exists to make impossible.
type EconomyTx struct {
	// UserID and AnonymousID override the configured actor identity for
	// this event, exactly as Event.UserID and Event.AnonymousID do.
	UserID      string
	AnonymousID string

	// EventID is the event's idempotency key, forwarded to Event.ID; empty
	// means a fresh id per call, exactly as for Event.ID. The fact layer
	// collapses rows that share an event_id and nothing else, so a ledger
	// that can report one transaction twice — a redelivery, a retry after an
	// ambiguous timeout, a restart — must supply the same EventID on every
	// attempt, or each attempt is counted as a transaction of its own.
	EventID string

	// Direction is EconomySource (currency granted) or EconomySink
	// (currency spent). Required; any other value is refused.
	Direction EconomyDirection
	// CurrencyType names the in-game currency, e.g. "gold" or "gems"
	// (required).
	CurrencyType string
	// Reason is the ledger reason code, e.g. "match_reward" or
	// "shop_purchase" (required).
	Reason string
	// Amount is the quantity moved, a strictly positive integer: the
	// schema says a ledger transaction moves a non-zero amount and
	// Direction carries the sign. Required; zero or negative is refused.
	Amount int64

	// MatchID scopes the transaction to a match. The schema requires it for
	// the match-scoped reasons (match_reward, tower_upgrade) and forbids it
	// for every other reason; the verb enforces both sides and refuses a
	// mismatch with ErrInvalidEconomyTx before anything is published. A
	// match_id supplied through Props while MatchID is empty is judged by
	// the same rule, as the value that reaches the wire.
	MatchID string

	// Timestamp is the event time; zero means the client clock at the call,
	// exactly as for Event.Timestamp.
	Timestamp time.Time

	// Props carries additional properties, which the schema allows. The
	// keys this verb sets are written AFTER Props and therefore win: a
	// required field cannot be contradicted through the untyped map. An
	// optional field left empty writes nothing, so a value for it supplied
	// in Props is kept.
	Props map[string]any
}

// buildEconomyTxEvent validates an EconomyTx and shapes it into the
// canonical event. It is the whole of the typed verb; TrackEconomyTx and
// EnqueueEconomyTx only choose the delivery path, so every refusal, consent
// gate and queue rule of Track and Enqueue applies unchanged.
func (c *Client) buildEconomyTxEvent(tx EconomyTx) (Event, error) {
	if c.cfg.Source != SourceBackend {
		return Event{}, fmt.Errorf("%w: economy_tx is pinned to source %q and this client is configured with source %q",
			ErrBackendSourceRequired, SourceBackend, c.cfg.Source)
	}
	switch tx.Direction {
	case EconomySource, EconomySink:
	default:
		return Event{}, fmt.Errorf("%w: direction must be %q or %q, got %q", ErrInvalidEconomyTx, EconomySource, EconomySink, tx.Direction)
	}
	currencyType := strings.TrimSpace(tx.CurrencyType)
	if currencyType == "" {
		return Event{}, fmt.Errorf("%w: currency_type is required", ErrInvalidEconomyTx)
	}
	reason := strings.TrimSpace(tx.Reason)
	if reason == "" {
		return Event{}, fmt.Errorf("%w: reason is required", ErrInvalidEconomyTx)
	}
	if tx.Amount < 1 {
		return Event{}, fmt.Errorf("%w: amount must be a positive integer, got %d", ErrInvalidEconomyTx, tx.Amount)
	}
	// The effective match id: the typed field, or — when it is empty — a
	// match_id supplied through Props (an optional field left empty keeps
	// the Props value), so the rule below judges the value that would
	// actually reach the wire.
	matchID := strings.TrimSpace(tx.MatchID)
	if matchID == "" {
		if raw, present := tx.Props["match_id"]; present {
			supplied, ok := raw.(string)
			if !ok {
				return Event{}, fmt.Errorf("%w: props match_id must be a string, got %T", ErrInvalidEconomyTx, raw)
			}
			matchID = strings.TrimSpace(supplied)
		}
	}
	if isMatchScopedReason(reason) {
		if matchID == "" {
			return Event{}, fmt.Errorf("%w: match_id is required for reason %q", ErrInvalidEconomyTx, reason)
		}
	} else if matchID != "" {
		return Event{}, fmt.Errorf("%w: match_id is only allowed for a match-scoped reason (%s), got reason %q",
			ErrInvalidEconomyTx, strings.Join(matchScopedReasons, ", "), reason)
	}

	props := make(map[string]any, len(tx.Props)+5)
	for key, value := range tx.Props {
		props[key] = value
	}
	props["direction"] = string(tx.Direction)
	props["currency_type"] = currencyType
	props["reason"] = reason
	props["amount"] = tx.Amount
	if matchID != "" {
		props["match_id"] = matchID
	} else {
		delete(props, "match_id")
	}

	return Event{
		ID:          tx.EventID,
		Name:        economyTxEventName,
		UserID:      tx.UserID,
		AnonymousID: tx.AnonymousID,
		Timestamp:   tx.Timestamp,
		Props:       props,
	}, nil
}

// TrackEconomyTx publishes one canonical economy_tx event through Track: the
// same synchronous delivery and the same refusals — ErrClosed, the consent
// gate and publish errors — plus ErrInvalidEconomyTx for a missing or
// invalid required field and ErrBackendSourceRequired when Config.Source is
// not SourceBackend. A refused transaction never reaches the queue or the
// wire.
func (c *Client) TrackEconomyTx(ctx context.Context, tx EconomyTx) error {
	event, err := c.buildEconomyTxEvent(tx)
	if err != nil {
		return err
	}
	return c.Track(ctx, event)
}

// EnqueueEconomyTx queues one canonical economy_tx event for batched
// delivery through Enqueue, with Enqueue's refusals plus ErrInvalidEconomyTx
// and ErrBackendSourceRequired as for TrackEconomyTx.
func (c *Client) EnqueueEconomyTx(tx EconomyTx) error {
	event, err := c.buildEconomyTxEvent(tx)
	if err != nil {
		return err
	}
	return c.Enqueue(event)
}
