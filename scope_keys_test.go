package shardpilot

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
)

func TestScopeKeyConfiguration(t *testing.T) {
	typ := reflect.TypeOf(Config{})
	for _, axis := range []string{"Workspace", "App", "Environment"} {
		if field, ok := typ.FieldByName(axis + "Key"); !ok || field.Type.Kind() != reflect.String {
			t.Errorf("%sKey must be a public string setting", axis)
		}
		if _, ok := typ.FieldByName(axis + "ID"); ok {
			t.Errorf("retired %sID setting remains", axis)
		}
	}
}

// Reflection lets the wire scene run against either configuration vocabulary.
func setScopeKeySettings(t *testing.T, cfg *Config) {
	t.Helper()
	v := reflect.ValueOf(cfg).Elem()
	for axis, value := range map[string]string{"Workspace": "scope_ws", "App": "scope_app", "Environment": "scope_env"} {
		field := v.FieldByName(axis + "Key")
		if !field.IsValid() {
			field = v.FieldByName(axis + "ID")
		}
		if !field.IsValid() {
			t.Fatalf("missing %s scope setting", axis)
		}
		field.SetString(value)
	}
}

func TestOrdinaryEventScopeKeys(t *testing.T) {
	var mu sync.Mutex
	var received []map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Events []map[string]json.RawMessage `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = body.Events
		mu.Unlock()
		for _, event := range body.Events {
			for _, axis := range []string{"workspace", "app", "environment"} {
				if _, retired := event[axis+"_id"]; retired {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"error":{"code":"field_renamed"}}`)
					return
				}
			}
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"accepted":1,"rejected":0,"duplicates":0}`)
	}))
	defer server.Close()
	client := newSchemaRevisionTestClient(t, server.URL, func(cfg *Config) {
		setScopeKeySettings(t, cfg)
		cfg.DisableRequestCompression = true
	})
	defer client.Close(context.Background())
	err := client.Track(context.Background(), Event{Name: "scope_event"})
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("received %d events, want one actual SDK event; Track error: %v", len(received), err)
	}
	for axis, want := range map[string]string{"workspace": "scope_ws", "app": "scope_app", "environment": "scope_env"} {
		var got string
		if decodeErr := json.Unmarshal(received[0][axis+"_key"], &got); decodeErr != nil || got != want {
			t.Errorf("%s_key = %q (%v), want %q", axis, got, decodeErr, want)
		}
		if _, retired := received[0][axis+"_id"]; retired {
			t.Errorf("retired %s_id reached the wire", axis)
		}
	}
	if string(received[0]["schema_version"]) != "1" || string(received[0]["source"]) != `"backend"` {
		t.Error("ordinary scope naming changed the event major or source")
	}
	if err != nil {
		t.Fatalf("Track: %v", err)
	}
}
