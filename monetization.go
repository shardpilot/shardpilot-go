package shardpilot

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"
)

// purchaseEventName is the canonical event name the typed monetization verb
// emits (analytics.purchase.v1).
const purchaseEventName = "purchase"

// Purchase is the typed monetization verb's input: a server-validated,
// real-money purchase reported by a backend after receipt or store
// validation. TrackPurchase and EnqueuePurchase build the canonical
// `purchase` event from it. That event's schema pins source to "backend" and
// requires props.amount, props.currency and props.product; sku and quantity
// are optional.
//
// The verb does not relax the backend pin. A client configured with any
// other Source is refused with ErrBackendSourceRequired rather than having
// its source rewritten for the event: client-asserted revenue is
// structurally impossible on this pipeline, and that is a property of the
// design rather than a gap in it.
type Purchase struct {
	// UserID and AnonymousID override the configured actor identity for
	// this event, exactly as Event.UserID and Event.AnonymousID do.
	UserID      string
	AnonymousID string

	// Product identifies what was bought (required).
	Product string
	// Amount is what was paid, denominated in Currency (required; must be
	// a finite number). The schema declares a number and does not
	// constrain its sign, and this verb does not either: a refund reported
	// as a negative purchase reaches the wire and the schema admits it.
	Amount float64
	// Currency denominates Amount (required). The pipeline expects an ISO
	// 4217 code; this verb checks presence only, and the ingest validates
	// the event against the schema.
	Currency string

	// SKU is the store-specific stock-keeping unit (optional).
	SKU string
	// Quantity is the number of units bought (optional). Zero is treated as
	// unset and omitted from the wire rather than sent as 0.
	Quantity int

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

// buildPurchaseEvent validates a Purchase and shapes it into the canonical
// event. It is the whole of the typed verb; TrackPurchase and
// EnqueuePurchase only choose the delivery path, so every refusal,
// consent gate and queue rule of Track and Enqueue applies unchanged.
func (c *Client) buildPurchaseEvent(purchase Purchase) (Event, error) {
	if c.cfg.Source != SourceBackend {
		return Event{}, fmt.Errorf("%w: purchase is pinned to source %q and this client is configured with source %q",
			ErrBackendSourceRequired, SourceBackend, c.cfg.Source)
	}
	product := strings.TrimSpace(purchase.Product)
	if product == "" {
		return Event{}, fmt.Errorf("%w: product is required", ErrInvalidPurchase)
	}
	currency := strings.TrimSpace(purchase.Currency)
	if currency == "" {
		return Event{}, fmt.Errorf("%w: currency is required", ErrInvalidPurchase)
	}
	if math.IsNaN(purchase.Amount) || math.IsInf(purchase.Amount, 0) {
		return Event{}, fmt.Errorf("%w: amount must be a finite number", ErrInvalidPurchase)
	}

	props := make(map[string]any, len(purchase.Props)+5)
	for key, value := range purchase.Props {
		props[key] = value
	}
	props["amount"] = purchase.Amount
	props["currency"] = currency
	props["product"] = product
	if sku := strings.TrimSpace(purchase.SKU); sku != "" {
		props["sku"] = sku
	}
	if purchase.Quantity != 0 {
		props["quantity"] = purchase.Quantity
	}

	return Event{
		Name:        purchaseEventName,
		UserID:      purchase.UserID,
		AnonymousID: purchase.AnonymousID,
		Timestamp:   purchase.Timestamp,
		Props:       props,
	}, nil
}

// TrackPurchase publishes one canonical purchase event through Track: the
// same synchronous delivery, and the same refusals — ErrClosed, the consent
// gate (ErrConsentDenied, ErrConsentUnknown, ErrConsentActorMismatch,
// ErrConsentReceiptPending) and publish errors — plus ErrInvalidPurchase for
// a missing required field or a non-finite amount and
// ErrBackendSourceRequired when Config.Source is not SourceBackend. A
// refused purchase never reaches the queue or the wire.
func (c *Client) TrackPurchase(ctx context.Context, purchase Purchase) error {
	event, err := c.buildPurchaseEvent(purchase)
	if err != nil {
		return err
	}
	return c.Track(ctx, event)
}

// EnqueuePurchase queues one canonical purchase event for batched delivery
// through Enqueue, with Enqueue's refusals (ErrClosed, the consent gate,
// ErrQueueFull) plus ErrInvalidPurchase and ErrBackendSourceRequired as for
// TrackPurchase.
func (c *Client) EnqueuePurchase(purchase Purchase) error {
	event, err := c.buildPurchaseEvent(purchase)
	if err != nil {
		return err
	}
	return c.Enqueue(event)
}
