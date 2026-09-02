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
