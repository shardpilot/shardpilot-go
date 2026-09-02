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

	// MatchID links a match-scoped ledger entry to its match. The schema
	// REQUIRES it, non-empty, when Reason is a match-scoped code (today
	// match_reward and tower_upgrade) and FORBIDS it for every other
	// reason. This verb does not encode that list — the reason codes are
	// the game's, the rule is the schema's, and the ingest enforces it —
	// so a MatchID left empty is omitted from the wire and one supplied is
	// carried as given. A mismatch with the rule is rejected by the ingest
	// per event, inside an accepted batch: watch Config.OnBatchResult.
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

	props := make(map[string]any, len(tx.Props)+5)
	for key, value := range tx.Props {
		props[key] = value
	}
	props["direction"] = string(tx.Direction)
	props["currency_type"] = currencyType
	props["reason"] = reason
	props["amount"] = tx.Amount
	if matchID := strings.TrimSpace(tx.MatchID); matchID != "" {
		props["match_id"] = matchID
	}

	return Event{
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
