package shardpilot

import (
	"context"
	"errors"
	"testing"
)

func validEconomyTx() EconomyTx {
	return EconomyTx{UserID: "user-77", Direction: EconomySink, CurrencyType: "gems", Reason: "shop_purchase", Amount: 120}
}

func TestTrackEconomyTxProducesABackendLegalEnvelope(t *testing.T) {
	server, envelopes, _ := newPurchaseCaptureServer(t)
	client := newTestClient(t, server.URL)

	tx := validEconomyTx()
	tx.Direction = EconomySource
	tx.CurrencyType = " gold "
	tx.Reason = "match_reward"
	tx.Amount = 250
	tx.MatchID = "match-9001"
	tx.Props = map[string]any{
		"tier":   "ranked",
		"amount": "not-the-amount", // a typed field cannot be contradicted through Props
	}
	if err := client.TrackEconomyTx(context.Background(), tx); err != nil {
		t.Fatalf("TrackEconomyTx: %v", err)
	}

	envelope := receiveEnvelope(t, envelopes)
	if envelope["event_name"] != economyTxEventName || envelope["source"] != string(SourceBackend) || envelope["user_id"] != "user-77" {
		t.Fatalf("envelope = %v, want economy_tx from backend for user-77", envelope)
	}
	props := envelope["props"].(map[string]any)
	if props["direction"] != "source" || props["currency_type"] != "gold" || props["reason"] != "match_reward" {
		t.Fatalf("required props = %v, want source / gold (trimmed) / match_reward", props)
	}
	if props["amount"] != float64(250) {
		t.Fatalf("props.amount = %v, want 250 from the typed field, not the Props override", props["amount"])
	}
	if props["match_id"] != "match-9001" || props["tier"] != "ranked" {
		t.Fatalf("optional and extra props = match_id %v / tier %v", props["match_id"], props["tier"])
	}
}

func TestTrackEconomyTxOmitsMatchIDLeftEmpty(t *testing.T) {
	server, envelopes, _ := newPurchaseCaptureServer(t)
	client := newTestClient(t, server.URL)

	if err := client.TrackEconomyTx(context.Background(), validEconomyTx()); err != nil {
		t.Fatalf("TrackEconomyTx: %v", err)
	}
	props := receiveEnvelope(t, envelopes)["props"].(map[string]any)
	if _, present := props["match_id"]; present {
		t.Fatalf("props.match_id is present (%v); an empty MatchID must write nothing", props["match_id"])
	}
	if len(props) != 4 {
		t.Fatalf("props = %v, want exactly the four required keys", props)
	}
}

func TestTrackEconomyTxRefusesANonBackendSource(t *testing.T) {
	for _, source := range []Source{SourceClient, SourceServer} {
		t.Run(string(source), func(t *testing.T) {
			server, _, requests := newPurchaseCaptureServer(t)
			client := newSourceTestClient(t, server.URL, source)

			if err := client.TrackEconomyTx(context.Background(), validEconomyTx()); !errors.Is(err, ErrBackendSourceRequired) {
				t.Fatalf("TrackEconomyTx under source %q: err = %v, want ErrBackendSourceRequired", source, err)
			}
			if err := client.EnqueueEconomyTx(validEconomyTx()); !errors.Is(err, ErrBackendSourceRequired) {
				t.Fatalf("EnqueueEconomyTx under source %q: err = %v, want ErrBackendSourceRequired", source, err)
			}
			assertNothingReachedTheWire(t, client, requests)
		})
	}
}

func TestTrackEconomyTxRefusesAnInvalidTransaction(t *testing.T) {
	mutate := map[string]func(*EconomyTx){
		"unknown direction":   func(tx *EconomyTx) { tx.Direction = "transfer" },
		"empty direction":     func(tx *EconomyTx) { tx.Direction = "" },
		"empty currency_type": func(tx *EconomyTx) { tx.CurrencyType = "  " },
		"empty reason":        func(tx *EconomyTx) { tx.Reason = "" },
		"zero amount":         func(tx *EconomyTx) { tx.Amount = 0 },
		"negative amount":     func(tx *EconomyTx) { tx.Amount = -5 },
	}
	for name, apply := range mutate {
		t.Run(name, func(t *testing.T) {
			server, _, requests := newPurchaseCaptureServer(t)
			client := newTestClient(t, server.URL)
			tx := validEconomyTx()
			apply(&tx)

			if err := client.TrackEconomyTx(context.Background(), tx); !errors.Is(err, ErrInvalidEconomyTx) {
				t.Fatalf("TrackEconomyTx: err = %v, want ErrInvalidEconomyTx", err)
			}
			if err := client.EnqueueEconomyTx(tx); !errors.Is(err, ErrInvalidEconomyTx) {
				t.Fatalf("EnqueueEconomyTx: err = %v, want ErrInvalidEconomyTx", err)
			}
			assertNothingReachedTheWire(t, client, requests)
		})
	}
}

func TestTrackEconomyTxInheritsTheConsentGate(t *testing.T) {
	server, _, requests := newPurchaseCaptureServer(t)
	client := newTestClient(t, server.URL)
	client.SetConsent(false)

	if err := client.TrackEconomyTx(context.Background(), validEconomyTx()); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("TrackEconomyTx under denied consent: err = %v, want ErrConsentDenied", err)
	}
	if err := client.EnqueueEconomyTx(validEconomyTx()); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("EnqueueEconomyTx under denied consent: err = %v, want ErrConsentDenied", err)
	}
	assertNothingReachedTheWire(t, client, requests)
}

func TestEnqueueEconomyTxIsDeliveredOnFlush(t *testing.T) {
	server, envelopes, requests := newPurchaseCaptureServer(t)
	client := newTestClient(t, server.URL)

	if err := client.EnqueueEconomyTx(validEconomyTx()); err != nil {
		t.Fatalf("EnqueueEconomyTx: %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("EnqueueEconomyTx published immediately (%d requests); it must queue", got)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	envelope := receiveEnvelope(t, envelopes)
	if envelope["event_name"] != economyTxEventName || envelope["props"].(map[string]any)["amount"] != float64(120) {
		t.Fatalf("flushed envelope = %v, want the queued economy_tx with amount 120", envelope)
	}
}

// TestTrackEconomyTxCarriesTheCallerSuppliedEventID: the fact layer collapses
// rows that share an event_id, so a ledger that can report one transaction
// twice must be able to repeat the id — and, as the precondition shows,
// cannot without the field.
func TestTrackEconomyTxCarriesTheCallerSuppliedEventID(t *testing.T) {
	server, envelopes, _ := newPurchaseCaptureServer(t)
	client := newTestClient(t, server.URL)

	tx := validEconomyTx()
	for i := 0; i < 2; i++ {
		if err := client.TrackEconomyTx(context.Background(), tx); err != nil {
			t.Fatalf("TrackEconomyTx without EventID: %v", err)
		}
	}
	first := receiveEnvelope(t, envelopes)["event_id"]
	second := receiveEnvelope(t, envelopes)["event_id"]
	if first == "" || second == "" || first == second {
		t.Fatalf("two calls without EventID produced event_ids %v and %v; want two distinct generated ids", first, second)
	}

	tx.EventID = "ledger-5c19e2"
	for i := 0; i < 2; i++ {
		if err := client.TrackEconomyTx(context.Background(), tx); err != nil {
			t.Fatalf("TrackEconomyTx with EventID: %v", err)
		}
		if got := receiveEnvelope(t, envelopes)["event_id"]; got != "ledger-5c19e2" {
			t.Fatalf("event_id = %v, want the caller-supplied ledger-5c19e2", got)
		}
	}
}

// TestTrackEconomyTxEnforcesTheMatchScopedRule: the schema requires match_id
// for match_reward and tower_upgrade and forbids it for every other reason;
// either mismatch is refused before publishing, and both legal shapes reach
// the wire.
func TestTrackEconomyTxEnforcesTheMatchScopedRule(t *testing.T) {
	server, envelopes, requests := newPurchaseCaptureServer(t)
	client := newTestClient(t, server.URL)

	for _, reason := range []string{"match_reward", "tower_upgrade"} {
		tx := validEconomyTx()
		tx.Reason = reason
		tx.MatchID = "   "
		if err := client.TrackEconomyTx(context.Background(), tx); !errors.Is(err, ErrInvalidEconomyTx) {
			t.Fatalf("%s without match_id: err = %v, want ErrInvalidEconomyTx", reason, err)
		}
	}
	tx := validEconomyTx()
	tx.Reason = "shop_purchase"
	tx.MatchID = "match-1"
	if err := client.TrackEconomyTx(context.Background(), tx); !errors.Is(err, ErrInvalidEconomyTx) {
		t.Fatalf("shop_purchase with match_id: err = %v, want ErrInvalidEconomyTx", err)
	}
	assertNothingReachedTheWire(t, client, requests)

	tx = validEconomyTx()
	tx.Reason = "tower_upgrade"
	tx.MatchID = " match-1 "
	if err := client.TrackEconomyTx(context.Background(), tx); err != nil {
		t.Fatalf("tower_upgrade with match_id: %v", err)
	}
	if got := receiveEnvelope(t, envelopes)["props"].(map[string]any)["match_id"]; got != "match-1" {
		t.Fatalf("props.match_id = %v, want match-1", got)
	}

	tx = validEconomyTx()
	tx.Reason = "shop_purchase"
	tx.MatchID = ""
	if err := client.TrackEconomyTx(context.Background(), tx); err != nil {
		t.Fatalf("shop_purchase without match_id: %v", err)
	}
	if _, present := receiveEnvelope(t, envelopes)["props"].(map[string]any)["match_id"]; present {
		t.Fatal("props.match_id present on a non-match reason")
	}
}

// TestTrackEconomyTxJudgesAMatchIDSuppliedThroughProps: the rule judges the
// value that reaches the wire, so a match_id carried in Props while MatchID
// is empty is refused for a non-match reason, refused when it is not a
// string, and delivered (trimmed) for a match-scoped reason.
func TestTrackEconomyTxJudgesAMatchIDSuppliedThroughProps(t *testing.T) {
	server, envelopes, requests := newPurchaseCaptureServer(t)
	client := newTestClient(t, server.URL)

	tx := validEconomyTx()
	tx.Reason = "shop_purchase"
	tx.Props = map[string]any{"match_id": "match-1"}
	if err := client.TrackEconomyTx(context.Background(), tx); !errors.Is(err, ErrInvalidEconomyTx) {
		t.Fatalf("shop_purchase with a props match_id: err = %v, want ErrInvalidEconomyTx", err)
	}
	tx = validEconomyTx()
	tx.Reason = "match_reward"
	tx.Props = map[string]any{"match_id": 42}
	if err := client.TrackEconomyTx(context.Background(), tx); !errors.Is(err, ErrInvalidEconomyTx) {
		t.Fatalf("match_reward with a non-string props match_id: err = %v, want ErrInvalidEconomyTx", err)
	}
	assertNothingReachedTheWire(t, client, requests)

	tx = validEconomyTx()
	tx.Reason = "match_reward"
	tx.Props = map[string]any{"match_id": " match-1 "}
	if err := client.TrackEconomyTx(context.Background(), tx); err != nil {
		t.Fatalf("match_reward with the match id in Props: %v", err)
	}
	if got := receiveEnvelope(t, envelopes)["props"].(map[string]any)["match_id"]; got != "match-1" {
		t.Fatalf("props.match_id = %v, want match-1 (trimmed, from Props)", got)
	}
}
