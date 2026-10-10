package consentpolicy

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestScopeKeyWire(t *testing.T) {
	for _, pair := range goldenPairs {
		t.Run(pair.name, func(t *testing.T) {
			raw := string(goldenWire(t, pair.wire))
			t.Run("canonical", func(t *testing.T) {
				plan, err := ParsePlan([]byte(raw))
				if err != nil {
					t.Fatal(err)
				}
				want := map[string]string{"workspace_key": "ws_1", "app_key": "app_1", "environment_key": "env_1"}
				if pair.refusal {
					for key := range want {
						want[key] = ""
					}
				}
				encoded, err := json.Marshal(plan.Scope)
				if err != nil {
					t.Fatal(err)
				}
				var got map[string]string
				if err := json.Unmarshal(encoded, &got); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("scope=%v want=%v", got, want)
				}
			})
			legacy := raw
			for _, axis := range []string{"workspace", "app", "environment"} {
				key, retired := `"`+axis+`_key":`, `"`+axis+`_id":`
				if strings.Count(raw, key) != 1 {
					t.Fatalf("fixture must carry exactly one %s", key)
				}
				legacy = strings.Replace(legacy, key, retired, 1)
				for name, replacement := range map[string]string{
					"replaced": retired,
					"mixed":    retired + `"retired",` + key,
				} {
					t.Run(axis+"/"+name, func(t *testing.T) {
						if _, err := ParsePlan([]byte(strings.Replace(raw, key, replacement, 1))); err == nil {
							t.Fatal("retired scope member was accepted")
						}
					})
				}
			}
			t.Run("all-retired", func(t *testing.T) {
				if _, err := ParsePlan([]byte(legacy)); err == nil {
					t.Fatal("retired scope was accepted")
				}
			})
		})
	}
}
