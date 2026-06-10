package shardpilot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestQuickstartPurchaseEventIsBackendLegal sends the exact event the README
// quickstart demonstrates and asserts the produced wire envelope satisfies
// the canonical analytics.purchase.v1 contract: event_name const "purchase",
// source const "backend", and the required props amount/currency/product.
func TestQuickstartPurchaseEventIsBackendLegal(t *testing.T) {
	envelopes := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw struct {
			Events []map[string]any `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if len(raw.Events) == 1 {
			envelopes <- raw.Events[0]
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"duplicates":0}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	defer client.Close(context.Background())

	// Keep this event in sync with the README quickstart and examples/basic.
	err := client.Track(context.Background(), Event{
		Name:   "purchase",
		UserID: "user-1042",
		Props: map[string]any{
			"amount":   9.99,
			"currency": "USD",
			"product":  "starter_pack",
			"quantity": 1,
		},
	})
	if err != nil {
		t.Fatalf("Track quickstart purchase returned error: %v", err)
	}

	var envelope map[string]any
	select {
	case envelope = <-envelopes:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for quickstart event publish")
	}

	if envelope["event_name"] != "purchase" {
		t.Fatalf("expected event_name const purchase, got %v", envelope["event_name"])
	}
	if envelope["source"] != "backend" {
		t.Fatalf("quickstart event must be backend-legal (source const backend), got %v", envelope["source"])
	}
	if envelope["schema_version"] != float64(1) {
		t.Fatalf("expected schema_version 1, got %v", envelope["schema_version"])
	}
	for _, required := range []string{"event_id", "event_ts", "workspace_id", "app_id", "environment_id"} {
		value, _ := envelope[required].(string)
		if value == "" {
			t.Fatalf("expected required envelope field %s to be non-empty", required)
		}
	}
	if _, err := time.Parse(time.RFC3339Nano, envelope["event_ts"].(string)); err != nil {
		t.Fatalf("event_ts is not a date-time: %v", err)
	}

	props, ok := envelope["props"].(map[string]any)
	if !ok {
		t.Fatalf("expected props object, got %v", envelope["props"])
	}
	if amount, ok := props["amount"].(float64); !ok || amount < 0 {
		t.Fatalf("expected required props.amount >= 0, got %v", props["amount"])
	}
	if currency, ok := props["currency"].(string); !ok || len(currency) != 3 {
		t.Fatalf("expected required ISO 4217 props.currency, got %v", props["currency"])
	}
	if product, ok := props["product"].(string); !ok || product == "" {
		t.Fatalf("expected required props.product, got %v", props["product"])
	}
}

// TestConfigIdentityDefaultsApplyToEnvelope covers the optional
// Config.UserID/Config.AnonymousID defaults: events that do not set their
// own identity inherit them; per-event values win.
func TestConfigIdentityDefaultsApplyToEnvelope(t *testing.T) {
	client := &Client{
		cfg: Config{
			WorkspaceID:   "workspace-test",
			AppID:         "app-test",
			EnvironmentID: "develop",
			Source:        SourceBackend,
			UserID:        "config-user",
			AnonymousID:   "config-anon",
		},
		clock: realClock{},
	}

	defaulted, err := client.buildEnvelope(Event{Name: "purchase"})
	if err != nil {
		t.Fatalf("buildEnvelope: %v", err)
	}
	if defaulted.UserID != "config-user" || defaulted.AnonymousID != "config-anon" {
		t.Fatalf("expected config identity defaults, got user %q anon %q", defaulted.UserID, defaulted.AnonymousID)
	}

	overridden, err := client.buildEnvelope(Event{
		Name:        "purchase",
		UserID:      "event-user",
		AnonymousID: "event-anon",
	})
	if err != nil {
		t.Fatalf("buildEnvelope override: %v", err)
	}
	if overridden.UserID != "event-user" || overridden.AnonymousID != "event-anon" {
		t.Fatalf("expected per-event identity to win, got user %q anon %q", overridden.UserID, overridden.AnonymousID)
	}
}
