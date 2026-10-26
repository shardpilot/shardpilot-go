package shardpilot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/shardpilot/shardpilot-go/pkg/crash"
)

func TestAPIConfigVocabulary(t *testing.T) {
	typ := reflect.TypeOf(Config{})
	for _, name := range []string{"PublishableKey", "ServiceToken", "AppURL", "RemoteConfigEnabled"} {
		if _, ok := typ.FieldByName(name); !ok {
			t.Errorf("missing setting %s", name)
		}
	}
	crashType := reflect.TypeOf(crash.ClientOptions{})
	for _, name := range []string{"CrashURL", "CrashKey", "CrashComponent", "RetryBaseDelay"} {
		if _, ok := crashType.FieldByName(name); !ok {
			t.Errorf("missing crash setting %s", name)
		}
	}
	for _, name := range []string{"APIKey", "IngestURL", "Source", "RetryBackoff"} {
		if _, ok := crashType.FieldByName(name); ok {
			t.Errorf("retired crash alias %s remains", name)
		}
	}
	for _, name := range []string{"APIKey", "Token", "RemoteConfigURL"} {
		if _, ok := typ.FieldByName(name); ok {
			t.Errorf("retired alias %s is still exported", name)
		}
	}
}

func TestAPIConfigCredentialAndEnableRules(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		source                     Source
		service, publishable, app  string
		remote, experiments, valid bool
	}{
		{"backend service", SourceBackend, "service-fixture", "", "", false, false, true},
		{"server service", SourceServer, "service-fixture", "", "", false, false, true},
		{"client publishable", SourceClient, "", "sp_pub_fixture", "", false, false, true},
		{"client missing key", SourceClient, "", "", "", false, false, false},
		{"client static JWT refused", SourceClient, "", "eyJhbGciOiJIUzI1NiJ9.e30.signature", "", false, false, false},
		{"client service refused", SourceClient, "service-fixture", "sp_pub_fixture", "", false, false, false},
		{"backend server-tier ingest token", SourceBackend, "sp_ingest_fixture", "", "", false, false, true},
		{"server server-tier ingest token", SourceServer, "sp_ingest_fixture", "", "", false, false, true},
		{"backend sp_pub_ key is not service", SourceBackend, "sp_pub_fixture", "", "", false, false, false},
		{"static jwt refused", SourceBackend, "eyJhbGciOiJIUzI1NiJ9.e30.signature", "", "", false, false, false},
		{"missing service", SourceServer, "", "sp_pub_fixture", "", false, false, false},
		{"remote missing app", SourceBackend, "service-fixture", "sp_pub_fixture", "", true, false, false},
		{"remote missing key", SourceBackend, "service-fixture", "", "https://app.example", true, false, false},
		{"address only", SourceBackend, "service-fixture", "", "https://app.example", false, false, true},
		{"remote enabled", SourceBackend, "service-fixture", "sp_pub_fixture", "https://app.example", true, false, true},
		{"experiments without remote", SourceBackend, "service-fixture", "sp_pub_fixture", "https://app.example", false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{IngestURL: "https://ingest.example", WorkspaceKey: "w", AppKey: "a", EnvironmentKey: "e", Source: tc.source}
			setConfigSceneFields(&cfg, map[string]any{"ServiceToken": tc.service, "PublishableKey": tc.publishable, "AppURL": tc.app, "RemoteConfigEnabled": tc.remote, "ExperimentsEnabled": tc.experiments})
			_, err := normalizeConfig(cfg)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
		})
	}
}

// The same scene can execute the previous configuration API to demonstrate
// behavioral failures, instead of stopping at a missing-field compile error.
func setConfigSceneFields(cfg *Config, values map[string]any) {
	v := reflect.ValueOf(cfg).Elem()
	oldNames := map[string]string{"ServiceToken": "Token", "PublishableKey": "APIKey", "AppURL": "RemoteConfigURL"}
	for name, value := range values {
		field := v.FieldByName(name)
		if !field.IsValid() {
			field = v.FieldByName(oldNames[name])
		}
		if field.IsValid() {
			field.Set(reflect.ValueOf(value))
		}
	}
}

func TestAPIConfigRoutesCredentialsAndSeparatesEnablement(t *testing.T) {
	for _, source := range []Source{SourceClient, SourceServer, SourceBackend} {
		t.Run(string(source), func(t *testing.T) {
			ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				want := "Bearer service-fixture"
				if source == SourceClient {
					want = "Bearer sp_pub_fixture"
				}
				if got := r.Header.Get("Authorization"); got != want {
					t.Errorf("ingest bearer=%q want %q", got, want)
				}
				fmt.Fprint(w, `{"accepted":1}`)
			}))
			defer ingest.Close()
			app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Authorization"); got != "Bearer sp_pub_fixture" {
					t.Errorf("app bearer=%q", got)
				}
				fmt.Fprint(w, `{"version":1,"values":{"example":true}}`)
			}))
			defer app.Close()
			cfg := Config{IngestURL: ingest.URL, WorkspaceKey: "w", AppKey: "a", EnvironmentKey: "e", Source: source, AnonymousID: "synthetic-actor", FlushInterval: time.Hour}
			service := "service-fixture"
			if source == SourceClient {
				service = ""
			}
			setConfigSceneFields(&cfg, map[string]any{"ServiceToken": service, "PublishableKey": "sp_pub_fixture", "AppURL": app.URL})
			disabled, err := NewClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if disabled.rc != nil {
				t.Error("address alone enabled remote config")
			}
			if err := disabled.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			setConfigSceneFields(&cfg, map[string]any{"RemoteConfigEnabled": true})
			client, err := NewClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close(context.Background())
			if err := client.Track(context.Background(), Event{Name: "example"}); err != nil {
				t.Fatal(err)
			}
			if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAPIConfigAttributePrerequisite(t *testing.T) {
	for _, remote := range []bool{false, true} {
		for _, attributes := range []bool{false, true} {
			for _, experiments := range []bool{false, true} {
				t.Run(fmt.Sprintf("remote=%t/attributes=%t/experiments=%t", remote, attributes, experiments), func(t *testing.T) {
					app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if got := r.URL.Query().Get("geo"); r.URL.Path != "/v1/consent" && (got == "CA") != attributes {
							t.Errorf("attribute query=%q, enabled=%t", got, attributes)
						}
						fmt.Fprint(w, `{"version":1,"values":{"example":true}}`)
					}))
					defer app.Close()
					client, err := NewClient(Config{IngestURL: app.URL, ServiceToken: "service-fixture", PublishableKey: "sp_pub_fixture", AppURL: app.URL,
						WorkspaceKey: "w", AppKey: "a", EnvironmentKey: "e", Source: SourceBackend, AnonymousID: "synthetic-actor",
						RemoteConfigEnabled: remote, RemoteConfigAttributesEnabled: attributes, ExperimentsEnabled: experiments, FlushInterval: time.Hour})
					if client != nil {
						defer client.Close(context.Background())
					}
					if attributes && !remote {
						if !errors.Is(err, ErrInvalidConfig) || client != nil {
							t.Fatalf("attributes without remote config: client=%t error=%v", client != nil, err)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if (client.rc != nil) != remote {
						t.Fatal("remote-config enablement changed")
					}
					if remote {
						if _, err := client.SetConsent(true); err != nil {
							t.Fatal(err)
						}
						client.SetRemoteConfigAttributes(map[string]string{"geo": "CA"})
						if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
		}
	}
}
