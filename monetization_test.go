package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// newPurchaseCaptureServer accepts ingest batches, counts the requests it
// saw and hands every envelope of every batch to the returned channel, so a
// scene can assert both what reached the wire and that nothing did.
func newPurchaseCaptureServer(t *testing.T) (*httptest.Server, chan map[string]any, *atomic.Int64) {
	t.Helper()
	envelopes := make(chan map[string]any, 8)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var raw struct {
			Events []map[string]any `json:"events"`
		}
		if err := json.NewDecoder(ingestRequestBody(t, r)).Decode(&raw); err != nil {
			t.Errorf("decode request: %v", err)
		}
		for _, event := range raw.Events {
			envelopes <- event
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":` + strconv.Itoa(len(raw.Events)) + `,"rejected":0,"duplicates":0}`))
	}))
	t.Cleanup(server.Close)
	return server, envelopes, &requests
}

// newSourceTestClient is newTestClient with the configured Source chosen by
// the scene: the typed backend-lane verbs decide on Config.Source.
func newSourceTestClient(t *testing.T, ingestURL string, source Source) *Client {
	t.Helper()
	client, err := NewClient(Config{
		IngestURL:     ingestURL,
		Token:         "test-token",
		WorkspaceID:   "workspace-test",
		AppID:         "app-test",
		EnvironmentID: "develop",
		Source:        source,
		AppVersion:    "0.1.0",
		AppBuild:      "100",
		Platform:      "linux",
		BatchSize:     2,
		BufferSize:    4,
		FlushInterval: time.Hour,
		HTTPTimeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

func receiveEnvelope(t *testing.T, envelopes chan map[string]any) map[string]any {
	t.Helper()
	select {
	case envelope := <-envelopes:
		return envelope
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the purchase envelope")
		return nil
	}
}

func assertNothingReachedTheWire(t *testing.T, client *Client, requests *atomic.Int64) {
	t.Helper()
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("expected no ingest request, the server saw %d", got)
	}
}

func TestTrackPurchaseProducesABackendLegalEnvelope(t *testing.T) {
	server, envelopes, _ := newPurchaseCaptureServer(t)
	client := newTestClient(t, server.URL)

	err := client.TrackPurchase(context.Background(), Purchase{
		UserID:   "user-1042",
		Product:  " starter_pack ",
		Amount:   9.99,
		Currency: "USD",
		SKU:      "com.example.starter",
		Quantity: 1,
		Props: map[string]any{
			"store":  "play",
			"amount": "not-the-amount", // a typed field cannot be contradicted through Props
		},
	})
	if err != nil {
		t.Fatalf("TrackPurchase: %v", err)
	}

	envelope := receiveEnvelope(t, envelopes)
	if envelope["event_name"] != purchaseEventName {
		t.Fatalf("event_name = %v, want %q", envelope["event_name"], purchaseEventName)
	}
	if envelope["source"] != string(SourceBackend) {
		t.Fatalf("source = %v, want backend: the schema pins it", envelope["source"])
	}
	if envelope["user_id"] != "user-1042" {
		t.Fatalf("user_id = %v, want the per-event override", envelope["user_id"])
	}
	props, ok := envelope["props"].(map[string]any)
	if !ok {
		t.Fatalf("props = %v, want an object", envelope["props"])
	}
	if props["amount"] != 9.99 {
		t.Fatalf("props.amount = %v, want 9.99 from the typed field, not the Props override", props["amount"])
	}
	if props["currency"] != "USD" || props["product"] != "starter_pack" {
		t.Fatalf("required props = currency %v / product %v (trimmed), want USD / starter_pack", props["currency"], props["product"])
	}
	if props["sku"] != "com.example.starter" || props["quantity"] != float64(1) {
		t.Fatalf("optional props = sku %v / quantity %v, want com.example.starter / 1", props["sku"], props["quantity"])
	}
	if props["store"] != "play" {
		t.Fatalf("props.store = %v, want the extra property carried through", props["store"])
	}
}

func TestTrackPurchaseOmitsOptionalFieldsLeftEmpty(t *testing.T) {
	server, envelopes, _ := newPurchaseCaptureServer(t)
	client := newTestClient(t, server.URL)

	if err := client.TrackPurchase(context.Background(), Purchase{Product: "gems_100", Amount: 0.99, Currency: "EUR"}); err != nil {
		t.Fatalf("TrackPurchase: %v", err)
	}
	props := receiveEnvelope(t, envelopes)["props"].(map[string]any)
	if _, present := props["sku"]; present {
		t.Fatalf("props.sku is present (%v); an empty SKU must write nothing", props["sku"])
	}
	if _, present := props["quantity"]; present {
		t.Fatalf("props.quantity is present (%v); a zero Quantity must write nothing, not 0", props["quantity"])
	}
	if len(props) != 3 {
		t.Fatalf("props = %v, want exactly the three required keys", props)
	}
}

func TestTrackPurchaseRefusesANonBackendSource(t *testing.T) {
	for _, source := range []Source{SourceClient, SourceServer} {
		t.Run(string(source), func(t *testing.T) {
			server, _, requests := newPurchaseCaptureServer(t)
			client := newSourceTestClient(t, server.URL, source)

			err := client.TrackPurchase(context.Background(), Purchase{Product: "starter_pack", Amount: 9.99, Currency: "USD"})
			if !errors.Is(err, ErrBackendSourceRequired) {
				t.Fatalf("TrackPurchase under source %q: err = %v, want ErrBackendSourceRequired", source, err)
			}
			if err := client.EnqueuePurchase(Purchase{Product: "starter_pack", Amount: 9.99, Currency: "USD"}); !errors.Is(err, ErrBackendSourceRequired) {
				t.Fatalf("EnqueuePurchase under source %q: err = %v, want ErrBackendSourceRequired", source, err)
			}
			assertNothingReachedTheWire(t, client, requests)
		})
	}
}

func TestTrackPurchaseRefusesAnInvalidPurchase(t *testing.T) {
	cases := map[string]Purchase{
		"empty product":     {Product: "  ", Amount: 1, Currency: "USD"},
		"empty currency":    {Product: "p", Amount: 1, Currency: ""},
		"NaN amount":        {Product: "p", Amount: math.NaN(), Currency: "USD"},
		"+Inf amount":       {Product: "p", Amount: math.Inf(1), Currency: "USD"},
		"-Inf amount":       {Product: "p", Amount: math.Inf(-1), Currency: "USD"},
		"everything absent": {},
	}
	for name, purchase := range cases {
		t.Run(name, func(t *testing.T) {
			server, _, requests := newPurchaseCaptureServer(t)
			client := newTestClient(t, server.URL)

			if err := client.TrackPurchase(context.Background(), purchase); !errors.Is(err, ErrInvalidPurchase) {
				t.Fatalf("TrackPurchase: err = %v, want ErrInvalidPurchase", err)
			}
			if err := client.EnqueuePurchase(purchase); !errors.Is(err, ErrInvalidPurchase) {
				t.Fatalf("EnqueuePurchase: err = %v, want ErrInvalidPurchase", err)
			}
			assertNothingReachedTheWire(t, client, requests)
		})
	}
}

func TestTrackPurchaseInheritsTheConsentGate(t *testing.T) {
	server, _, requests := newPurchaseCaptureServer(t)
	client := newTestClient(t, server.URL)
	client.SetConsent(false)

	valid := Purchase{Product: "starter_pack", Amount: 9.99, Currency: "USD"}
	if err := client.TrackPurchase(context.Background(), valid); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("TrackPurchase under denied consent: err = %v, want ErrConsentDenied", err)
	}
	if err := client.EnqueuePurchase(valid); !errors.Is(err, ErrConsentDenied) {
		t.Fatalf("EnqueuePurchase under denied consent: err = %v, want ErrConsentDenied", err)
	}
	assertNothingReachedTheWire(t, client, requests)
}

func TestEnqueuePurchaseIsDeliveredOnFlush(t *testing.T) {
	server, envelopes, requests := newPurchaseCaptureServer(t)
	client := newTestClient(t, server.URL)

	if err := client.EnqueuePurchase(Purchase{Product: "starter_pack", Amount: 9.99, Currency: "USD", Quantity: 2}); err != nil {
		t.Fatalf("EnqueuePurchase: %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("EnqueuePurchase published immediately (%d requests); it must queue", got)
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	envelope := receiveEnvelope(t, envelopes)
	if envelope["event_name"] != purchaseEventName || envelope["props"].(map[string]any)["quantity"] != float64(2) {
		t.Fatalf("flushed envelope = %v, want the queued purchase with quantity 2", envelope)
	}
}

// TestTrackPurchaseCarriesTheCallerSuppliedEventID: the fact layer collapses
// rows that share an event_id, so a handler that runs twice for one purchase
// must be able to repeat the id — and, as the precondition shows, cannot
// without the field.
func TestTrackPurchaseCarriesTheCallerSuppliedEventID(t *testing.T) {
	server, envelopes, _ := newPurchaseCaptureServer(t)
	client := newTestClient(t, server.URL)

	purchase := Purchase{UserID: "user-1042", Product: "starter_pack", Amount: 9.99, Currency: "USD"}

	// Precondition, and the double count the key exists to prevent: two
	// calls for the same purchase without an EventID are two events.
	for i := 0; i < 2; i++ {
		if err := client.TrackPurchase(context.Background(), purchase); err != nil {
			t.Fatalf("TrackPurchase without EventID: %v", err)
		}
	}
	first := receiveEnvelope(t, envelopes)["event_id"]
	second := receiveEnvelope(t, envelopes)["event_id"]
	if first == "" || second == "" || first == second {
		t.Fatalf("two calls without EventID produced event_ids %v and %v; want two distinct generated ids", first, second)
	}

	// With the key, a redelivery repeats the id the fact layer collapses on.
	purchase.EventID = "receipt-7f3a2c"
	for i := 0; i < 2; i++ {
		if err := client.TrackPurchase(context.Background(), purchase); err != nil {
			t.Fatalf("TrackPurchase with EventID: %v", err)
		}
		if got := receiveEnvelope(t, envelopes)["event_id"]; got != "receipt-7f3a2c" {
			t.Fatalf("event_id = %v, want the caller-supplied receipt-7f3a2c", got)
		}
	}
}

// TestTrackPurchaseUpperCasesTheCurrency: a provider-native lowercase code
// lands in the same currency group as its canonical form.
func TestTrackPurchaseUpperCasesTheCurrency(t *testing.T) {
	server, envelopes, _ := newPurchaseCaptureServer(t)
	client := newTestClient(t, server.URL)

	err := client.TrackPurchase(context.Background(), Purchase{
		UserID:   "user-1042",
		Product:  "starter_pack",
		Amount:   9.99,
		Currency: " usd ",
	})
	if err != nil {
		t.Fatalf("TrackPurchase: %v", err)
	}
	props := receiveEnvelope(t, envelopes)["props"].(map[string]any)
	if got := props["currency"]; got != "USD" {
		t.Fatalf("props.currency = %v, want the provider-native usd upper-cased to USD", got)
	}
}
