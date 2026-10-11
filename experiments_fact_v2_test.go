package shardpilot

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func factV2Golden(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("testdata/experiment-fact-v2/fact_scope_keys_client_attested.json")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != "d8056298713460b9ad9e8255fdfcb06eeb974d66f7938b9ede08bcfb1c49c021" {
		t.Fatal("server golden bytes changed; update the source receipt explicitly")
	}
	var fact map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&fact); err != nil {
		t.Fatal(err)
	}
	return fact
}

func TestExperimentV2ServerGoldenEnvelope(t *testing.T) {
	for _, name := range []string{experimentExposureName, experimentOutcomeName} {
		t.Run(name, func(t *testing.T) {
			fact := factV2Golden(t)
			fact["event_name"] = name
			if name == experimentOutcomeName {
				props := fact["props"].(map[string]any)
				props["outcome_key"], props["outcome_value"] = "completed", json.Number("42")
				props["experiment_version"] = json.Number("9007199254740993")
			}
			raw, err := json.Marshal(fact)
			if err != nil {
				t.Fatal(err)
			}
			client := &Client{clock: realClock{}, cfg: Config{
				WorkspaceID: fact["workspace_key"].(string), AppID: fact["app_key"].(string),
				EnvironmentID: fact["environment_key"].(string), AnonymousID: "anon-test", Source: SourceBackend,
			}}
			sealed := &expSealedFact{fact: raw, seal: expStubSeal}
			event, ok := client.sealedExposureEvent("session-test", sealed, 0)
			if !ok {
				t.Fatal("the server golden's literal key scope must be admitted")
			}
			batch, err := client.buildBatch([]Event{event, {Name: "ordinary.control"}})
			if err != nil {
				t.Fatal(err)
			}
			var wire map[string]any
			decoder := json.NewDecoder(bytes.NewReader(batch.rawEvents[0]))
			decoder.UseNumber()
			if err := decoder.Decode(&wire); err != nil {
				t.Fatal(err)
			}
			if wire["schema_version"] != json.Number("2") || wire["attestation_seal"] != expStubSeal {
				t.Fatal("the fact must use schema major 2 and preserve its opaque seal")
			}
			for _, member := range []string{"workspace_id", "app_id", "environment_id"} {
				if _, exists := wire[member]; exists {
					t.Fatalf("retired scope member %s must be absent", member)
				}
			}
			for member, value := range fact {
				if !reflect.DeepEqual(wire[member], value) {
					t.Fatalf("sealed member %s changed", member)
				}
			}
			var ordinary map[string]any
			if err := json.Unmarshal(batch.rawEvents[1], &ordinary); err != nil {
				t.Fatal(err)
			}
			if ordinary["schema_version"] != float64(1) || ordinary["workspace_id"] != client.cfg.WorkspaceID || ordinary["workspace_key"] != nil {
				t.Fatal("ordinary events must retain their original major and scope fields")
			}
			for _, axis := range []string{"workspace_key", "app_key", "environment_key"} {
				original := fact[axis]
				fact[axis] = " " + original.(string)
				foreign, _ := json.Marshal(fact)
				if _, ok := client.sealedExposureEvent("session-test", &expSealedFact{fact: foreign, seal: expStubSeal}, 0); ok {
					t.Fatalf("foreign literal %s must not be trimmed into the configured scope", axis)
				}
				fact[axis] = original
			}
		})
	}
}
