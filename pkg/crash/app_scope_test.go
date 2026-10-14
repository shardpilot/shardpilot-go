package crash

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
)

func TestCrashAppKeyWireGolden(t *testing.T) {
	fixture, err := os.ReadFile("testdata/app-scope/request.json")
	if err != nil {
		t.Fatal(err)
	}
	response, err := os.ReadFile("testdata/app-scope/response.json")
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]json.RawMessage
	if err = json.Unmarshal(fixture, &want); err != nil {
		t.Fatal(err)
	}
	var expected map[string]any
	if err = json.Unmarshal(want["app"], &expected); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"event", "client-default", "event-override"} {
		t.Run(mode, func(t *testing.T) {
			event := validEvent(t)
			event.App.Key = expected["key"].(string)
			event.App.Version = expected["version"].(string)
			event.App.BuildID = ""
			defaults := AppInfo{}
			if mode == "client-default" {
				defaults = event.App
				defaults.Key = " " + defaults.Key + " "
				event.App = AppInfo{}
			}
			if mode == "event-override" {
				defaults = AppInfo{Key: "different-default", Version: "different-version"}
			}
			originalID := event.CrashID
			originalDebugID := event.Modules[0].DebugID
			calls := 0
			var captured map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "POST" || r.URL.Path != "/api/v1/crashes/ingest" {
					t.Errorf("wrong method/path")
				}
				body, readErr := io.ReadAll(r.Body)
				if readErr != nil {
					t.Error(readErr)
				}
				if readErr = json.Unmarshal(body, &captured); readErr != nil {
					t.Error(readErr)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write(response)
			}))
			defer server.Close()
			results := 0
			client, err := NewClient(ClientOptions{CrashURL: server.URL, CrashKey: "synthetic-fixture", App: defaults, OnResult: func(result Result) {
				results++
				if result.CrashID != "s31-golden" {
					t.Errorf("wrong pinned response crash id")
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err = client.EmitFatal(context.Background(), event); err != nil {
				t.Fatal(err)
			}
			if calls != 1 || results != 1 {
				t.Fatalf("real sender/result counts %d/%d", calls, results)
			}
			if captured["crash_id"] != originalID {
				t.Error("intrinsic crash id changed")
			}
			if captured["modules"].([]any)[0].(map[string]any)["debug_id"] != originalDebugID {
				t.Error("intrinsic debug id changed")
			}
			for _, key := range []string{"workspace_id", "workspace_key", "environment_id", "environment_key"} {
				if _, ok := captured[key]; ok {
					t.Errorf("new authority root %s", key)
				}
			}
			if !reflect.DeepEqual(captured["app"], expected) {
				t.Errorf("actual sender app selector %v; accepted handler golden app %v", captured["app"], expected)
			}
			t.Log("real POST/result and intrinsic-ID/authority controls ran")
		})
	}
}

func TestCrashAppKeyFixturePins(t *testing.T) {
	for file, want := range map[string]string{"request.json": "64b8b73d69579eff096006fb1400f5d0f62f8b429fa9fd8252addf6676592969", "response.json": "3a81a7608ff0212496dad3f0735bff493fa40d9ce68042ccdc319735d5eac18d"} {
		body, err := os.ReadFile("testdata/app-scope/" + file)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(body)); got != want {
			t.Errorf("%s golden digest %s, want %s", file, got, want)
		}
	}
}

func TestAppInfoCanonicalScopeKeyAPI(t *testing.T) {
	typ := reflect.TypeOf(AppInfo{})
	if field, ok := typ.FieldByName("Key"); !ok || field.Type.Kind() != reflect.String || field.Tag.Get("json") != "key" {
		t.Error("AppInfo must expose the canonical Key string with the key wire tag")
	}
	if _, ok := typ.FieldByName("ID"); ok {
		t.Error("retired AppInfo.ID compatibility alias must be absent")
	}
}
