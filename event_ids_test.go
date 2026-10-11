package shardpilot

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
)

func TestHostEventIDAdmission(t *testing.T) {
	const v4 = "01234567-89ab-4cde-8fab-0123456789ab"
	const v7 = "01922222-3333-7abc-bdef-0123456789ab"
	scenes := []struct {
		name, id, want string
		invalid        bool
	}{
		{"arbitrary", "receipt-123", "", true},
		{"short", v4[:35], "", true},
		{"nonhex", "g" + v4[1:], "", true},
		{"separator", v4[:8] + "_" + v4[9:], "", true},
		{"braces", "{" + v4 + "}", "", true},
		{"urn", "urn:uuid:" + v4, "", true},
		{"compact", strings.ReplaceAll(v4, "-", ""), "", true},
		{"v4", v4, v4, false},
		{"v7", v7, v7, false},
		{"uppercase", strings.ToUpper(v4), v4, false},
		{"mixedcase", "01922222-3333-7aBc-BdEf-0123456789aB", v7, false},
		{"generated", "", "", false},
	}

	for version := byte('1'); version <= '8'; version++ {
		id := v4[:14] + string(version) + v4[15:]
		scenes = append(scenes, struct {
			name, id, want string
			invalid        bool
		}{"version_" + string(version), id, id, version != '4' && version != '5' && version != '7'})
	}
	for _, variant := range []byte{'8', '9', 'a', 'b'} {
		id := v4[:19] + string(variant) + v4[20:]
		scenes = append(scenes, struct {
			name, id, want string
			invalid        bool
		}{"variant_" + string(variant), strings.ToUpper(id), id, false})
	}
	for _, version := range []byte{'0', '9', 'f'} {
		id := v4[:14] + string(version) + v4[15:]
		scenes = append(scenes, struct {
			name, id, want string
			invalid        bool
		}{"bad_version_" + string(version), id, "", true})
	}
	for _, variant := range []byte{'0', '7', 'c', 'f'} {
		id := v4[:19] + string(variant) + v4[20:]
		scenes = append(scenes, struct {
			name, id, want string
			invalid        bool
		}{"bad_variant_" + string(variant), id, "", true})
	}
	for _, method := range []string{"Track", "Enqueue"} {
		for _, scene := range scenes {
			t.Run(method+"/"+scene.name, func(t *testing.T) {
				server, envelopes, requests := newPurchaseCaptureServer(t)
				client := newSourceTestClient(t, server.URL, SourceBackend)
				before := client.Snapshot()
				event := Event{ID: scene.id, Name: "ordinary_control"}
				var err error
				if method == "Track" {
					err = client.Track(context.Background(), event)
				} else {
					err = client.Enqueue(event)
				}
				if flushErr := client.Flush(context.Background()); flushErr != nil {
					t.Fatal(flushErr)
				}
				after := client.Snapshot()
				if scene.invalid {
					if !errors.Is(err, ErrInvalidEvent) || !strings.Contains(fmt.Sprint(err), "invalid_event_id") {
						t.Errorf("ID refusal = %v; want typed invalid_event_id", err)
					}
					if after.Dropped-before.Dropped != 1 || !strings.Contains(after.LastError, "invalid_event_id") {
						t.Errorf("refusal accounting = dropped %d, diagnostic %q", after.Dropped-before.Dropped, after.LastError)
					}
					if after.Enqueued != before.Enqueued || after.FailedBatches != before.FailedBatches || requests.Load() != 0 || len(envelopes) != 0 {
						t.Error("refused ID entered delivery or batch accounting")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if requests.Load() != 1 || len(envelopes) != 1 || after.Dropped != before.Dropped {
					t.Fatal("accepted ID did not reach transport exactly once")
				}
				got := (<-envelopes)["event_id"].(string)
				if scene.want != "" && got != scene.want {
					t.Errorf("wire ID = %q, want %q", got, scene.want)
				}
				if scene.id == "" && (!uuidv7.IsValid(got) || got != strings.ToLower(got)) {
					t.Errorf("generated ID = %q, want lowercase UUIDv7", got)
				}
				if event.ID != scene.id {
					t.Fatal("caller value changed")
				}
			})
		}
	}
}

func TestTypedEventIDAdmission(t *testing.T) {
	const canonical = "01234567-89ab-4cde-8fab-0123456789ab"
	for _, method := range []string{"TrackPurchase", "EnqueuePurchase", "TrackEconomyTx", "EnqueueEconomyTx"} {
		for _, id := range []string{"receipt-123", strings.ToUpper(canonical), ""} {
			t.Run(method+"/"+id, func(t *testing.T) {
				server, envelopes, requests := newPurchaseCaptureServer(t)
				client := newSourceTestClient(t, server.URL, SourceBackend)
				purchase := Purchase{EventID: id, Product: "product", Currency: "USD", Amount: 1}
				economy := EconomyTx{EventID: id, Direction: EconomySource, CurrencyType: "gold", Reason: "daily_bonus", Amount: 1}
				var err error
				switch method {
				case "TrackPurchase":
					err = client.TrackPurchase(context.Background(), purchase)
				case "EnqueuePurchase":
					err = client.EnqueuePurchase(purchase)
				case "TrackEconomyTx":
					err = client.TrackEconomyTx(context.Background(), economy)
				case "EnqueueEconomyTx":
					err = client.EnqueueEconomyTx(economy)
				}
				if flushErr := client.Flush(context.Background()); flushErr != nil {
					t.Fatal(flushErr)
				}
				if id == "receipt-123" {
					if !errors.Is(err, ErrInvalidEvent) || !strings.Contains(fmt.Sprint(err), "invalid_event_id") || requests.Load() != 0 {
						t.Fatalf("typed ID refusal = %v; requests %d", err, requests.Load())
					}
					return
				}
				if err != nil || requests.Load() != 1 || len(envelopes) != 1 {
					t.Fatalf("typed ID delivery = %v; requests %d", err, requests.Load())
				}
				got := (<-envelopes)["event_id"].(string)
				if id == "" {
					if !uuidv7.IsValid(got) {
						t.Fatalf("typed generated ID = %q", got)
					}
				} else if got != canonical {
					t.Fatalf("typed wire ID = %q", got)
				}
			})
		}
	}
}
