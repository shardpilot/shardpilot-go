package shardpilot_test

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shardpilot/shardpilot-go"
)

type consentStateReader interface {
	ConsentState() shardpilot.ConsentState
}

type getterNoNetwork struct{ calls atomic.Int64 }

func (p *getterNoNetwork) RoundTrip(*http.Request) (*http.Response, error) {
	p.calls.Add(1)
	return nil, fmt.Errorf("unexpected consent getter network request")
}

func TestConsentGetterPublicVocabulary(t *testing.T) {
	t.Run("before-initialization", func(t *testing.T) {
		client := new(shardpilot.Client)
		reader, ok := any(client).(consentStateReader)
		if !ok {
			t.Fatal("required ConsentState getter is absent")
		}
		if got := reader.ConsentState(); got != shardpilot.ConsentUnknown {
			t.Fatalf("zero client state: got %q, want unknown", got)
		}
	})
	t.Run("retired-name-is-absent", func(t *testing.T) {
		if _, ok := any(new(shardpilot.Client)).(interface {
			Consent() shardpilot.ConsentState
		}); ok {
			t.Fatal("retired Consent getter remains available")
		}
	})
	for _, floor := range []bool{false, true} {
		for _, state := range []shardpilot.ConsentState{
			shardpilot.ConsentUnknown, shardpilot.ConsentGranted,
			shardpilot.ConsentDenied, shardpilot.ConsentDeniedForcedMinor,
		} {
			t.Run(fmt.Sprintf("floor-%t/%s", floor, state), func(t *testing.T) {
				probe := new(getterNoNetwork)
				cfg := shardpilot.Config{
					IngestURL: "https://sdk.synthetic.invalid", Token: "test-token",
					WorkspaceID: "workspace-test", AppID: "app-test", EnvironmentID: "develop",
					Source: shardpilot.SourceBackend, HTTPClient: &http.Client{Transport: probe},
					FlushInterval: time.Hour,
				}
				if floor {
					cfg.ConsentFloor = &shardpilot.ConsentFloorConfig{}
				}
				client, err := shardpilot.NewClient(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := client.Close(context.Background()); err != nil {
						t.Error(err)
					}
					if got := probe.calls.Load(); got != 0 {
						t.Errorf("actorless getter control made %d requests", got)
					}
				})
				if state != shardpilot.ConsentUnknown {
					if _, err := client.SetConsentDecision(shardpilot.ConsentDecision(state)); err != nil {
						t.Fatalf("real decision failed: %v", err)
					}
				}
				reader, ok := any(client).(consentStateReader)
				if !ok {
					t.Fatal("required ConsentState getter is absent")
				}
				for i := 0; i < 3; i++ {
					if got := reader.ConsentState(); got != state {
						t.Fatalf("read %d: got %q, want %q", i, got, state)
					}
				}
			})
		}
	}
}
