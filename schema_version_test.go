package shardpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func schemaAdmit(c *Client, path string, e Event) error {
	if path == "Track" {
		return c.Track(context.Background(), e)
	}
	return c.Enqueue(e)
}
func schemaWireVersion(t *testing.T, raw []byte) int {
	t.Helper()
	var e struct {
		Version int `json:"schema_version"`
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatal(err)
	}
	return e.Version
}

func TestEventSchemaVersionAdmission(t *testing.T) {
	for _, path := range []string{"Track", "Enqueue"} {
		t.Run(path+"/mixed", func(t *testing.T) {
			c, scene := newSizeSceneClient(t)
			one, two, max := 1, 2, 65535
			for _, v := range []*int{nil, &one, &two, &max} {
				if err := schemaAdmit(c, path, Event{Name: "schema_mixed", SchemaVersion: v}); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			got := scene.received()
			if len(got) != 4 {
				t.Fatalf("wire elements=%d, want 4", len(got))
			}
			for i, want := range []int{1, 1, 2, 65535} {
				if schemaWireVersion(t, got[i]) != want {
					t.Fatalf("element %d version=%d, want %d", i, schemaWireVersion(t, got[i]), want)
				}
			}
		})
		for _, value := range []int{0, -1, 65536, int(^uint(0) >> 1)} {
			t.Run(fmt.Sprintf("%s/invalid_%d", path, value), func(t *testing.T) {
				c, scene := newSizeSceneClient(t)
				calls := new(atomic.Int32)
				v := value
				before := c.Snapshot()
				err := schemaAdmit(c, path, Event{Name: "schema_invalid", SchemaVersion: &v, Props: map[string]any{"probe": schemaProbe{calls: calls}}})
				after := c.Snapshot()
				if !errors.Is(err, ErrSchemaVersionInvalid) || !errors.Is(err, ErrInvalidEvent) || !strings.Contains(err.Error(), "schema_version_invalid") {
					t.Fatalf("typed refusal=%v", err)
				}
				if after.LastError != "schema_version_invalid" || after.Dropped-before.Dropped != 1 || after.Enqueued != before.Enqueued || after.FailedBatches != before.FailedBatches || len(scene.received()) != 0 || calls.Load() != 0 {
					t.Fatalf("refusal produced effects: %+v, marshal calls=%d", after, calls.Load())
				}
			})
		}
	}
}

type schemaProbe struct {
	calls     *atomic.Int32
	failFirst bool
}

func (p schemaProbe) MarshalJSON() ([]byte, error) {
	if p.calls.Add(1) == 1 && p.failFirst {
		return nil, errors.New("synthetic intake failure")
	}
	return []byte(`"ok"`), nil
}

type schemaClock struct{ calls atomic.Int32 }

func (c *schemaClock) Now() time.Time {
	c.calls.Add(1)
	return time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
}
func TestEventSchemaVersionStagesInvalidWithoutEffects(t *testing.T) {
	clock := &schemaClock{}
	c := &Client{clock: clock}
	v := 0
	calls := new(atomic.Int32)
	e, err := c.stageEvent(Event{Name: "schema_invalid", SchemaVersion: &v, Props: map[string]any{"probe": schemaProbe{calls: calls}}})
	if err != nil || e.ID != "" || !e.Timestamp.IsZero() || e.delivery != nil || clock.calls.Load() != 0 || calls.Load() != 0 {
		t.Fatalf("staging invalid option prepared it: id=%q timestamp=%v delivery=%v clock=%d marshal=%d error=%v", e.ID, e.Timestamp, e.delivery, clock.calls.Load(), calls.Load(), err)
	}
}

func TestEventSchemaVersionRefusalPrecedence(t *testing.T) {
	for _, path := range []string{"Track", "Enqueue"} {
		for _, value := range []int{0, 65535} {
			for _, kind := range []string{"closed", "denied", "unknown", "actor", "name", "id"} {
				t.Run(fmt.Sprintf("%s/%d/%s", path, value, kind), func(t *testing.T) {
					state, server := newSpoolTestServer(t)
					defer server.Close()
					c := newSpoolTestClient(t, server.URL, "", nil, func(cfg *Config) {
						if kind == "unknown" || kind == "actor" {
							cfg.ConsentFloor = &ConsentFloorConfig{}
						}
					})
					defer c.Close(context.Background())
					v := value
					e := Event{Name: "schema_precedence", SchemaVersion: &v}
					want := ErrClosed
					switch kind {
					case "closed":
						c.closed.Store(true)
					case "denied":
						c.consent.Store(consentStateDenied)
						want = ErrConsentDenied
					case "unknown":
						want = ErrConsentUnknown
					case "actor":
						c.consent.Store(consentStateGranted)
						e.AnonymousID = "synthetic-other"
						want = ErrConsentActorMismatch
					case "name":
						e.Name = "app.experiment_exposure"
						want = ErrReservedEventName
					case "id":
						e.ID = "invalid-id"
						want = ErrInvalidEventID
					}
					if err := schemaAdmit(c, path, e); !errors.Is(err, want) {
						t.Fatalf("precedence=%v, want %v", err, want)
					}
					if c.Snapshot().Enqueued != 0 || state.batchCount() != 0 {
						t.Fatal("refused option entered delivery")
					}
				})
			}
		}
	}
}

func TestEventSchemaVersionSnapshot(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("fallback_%v", fallback), func(t *testing.T) {
			c, scene := newSizeSceneClient(t)
			v := 65535
			calls := new(atomic.Int32)
			if err := c.Enqueue(Event{Name: "schema_snapshot", SchemaVersion: &v, Props: map[string]any{"probe": schemaProbe{calls: calls, failFirst: fallback}}}); err != nil {
				t.Fatal(err)
			}
			v = 2
			if err := c.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			got := scene.received()
			if len(got) != 1 || schemaWireVersion(t, got[0]) != 65535 {
				t.Fatalf("caller mutation changed admitted version: %s", got)
			}
			if fallback && calls.Load() < 2 {
				t.Fatal("deferred encoding did not run")
			}
		})
	}
}

func TestEventSchemaVersionWireSize(t *testing.T) {
	for _, path := range []string{"Track", "Enqueue"} {
		for _, size := range []int{2047, 2048, 2049} {
			t.Run(fmt.Sprintf("%s/%d", path, size), func(t *testing.T) {
				c, scene := newSizeSceneClient(t)
				v := 65535
				// The default fixture is four bytes shorter than the selected integer.
				e := sizedEvent(t, c, "0199a0b0-0000-7000-8000-000000000001", size-4)
				e.SchemaVersion = &v
				err := schemaAdmit(c, path, e)
				if size > 2048 {
					if !errors.Is(err, ErrEventTooLarge) || c.Snapshot().Dropped != 1 || len(scene.received()) != 0 {
						t.Fatalf("selected width did not refuse %d bytes: %v", size, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if err = c.Flush(context.Background()); err != nil {
					t.Fatal(err)
				}
				got := scene.received()
				if len(got) != 1 || len(bytes.TrimSpace(got[0])) != size || schemaWireVersion(t, got[0]) != 65535 {
					t.Fatalf("measured wire changed: %s", got)
				}
			})
		}
	}
}

func TestEventSchemaVersionRetryAndNewSpool(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	dir := t.TempDir()
	c := newSpoolTestClient(t, server.URL, dir, nil, nil)
	c.SetConsent(true)
	state.setOutcome(http.StatusServiceUnavailable, "internal_error", "")
	v := 65535
	if err := c.Enqueue(Event{Name: "schema_spool", SchemaVersion: &v}); err != nil {
		t.Fatal(err)
	}
	v = 0
	flushUntilSpooled(t, c, dir, 1)
	record := readSpoolRecordFile(t, dir).Events
	if len(record) != 1 || schemaWireVersion(t, record[0].Raw) != 65535 {
		t.Fatal("new spool lost admitted version")
	}
	_ = c.Flush(context.Background())
	_ = c.Close(context.Background())
	bodies := state.allBodies()
	if len(bodies) < 2 {
		t.Fatal("retry did not run")
	}
	for _, body := range bodies {
		raw := wireEventBytes(t, body)
		if len(raw) != 1 || raw[0] != string(record[0].Raw) {
			t.Fatal("retry/spool bytes changed")
		}
	}
	state.setOutcome(http.StatusAccepted, "", "")
	before := state.batchCount()
	restarted := newSpoolTestClient(t, server.URL, dir, nil, nil)
	defer restarted.Close(context.Background())
	if err := restarted.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	resent := state.allBodies()[before:]
	if len(resent) != 1 {
		t.Fatalf("restart batches=%d, want 1", len(resent))
	}
	if got := wireEventBytes(t, resent[0]); len(got) != 1 || got[0] != string(record[0].Raw) {
		t.Fatal("restart bytes changed")
	}
	if len(readSpoolRecordFile(t, dir).Events) != 0 || restarted.Snapshot().SpoolResent != 1 {
		t.Fatal("replay did not settle")
	}
}

func TestEventSchemaVersionOwnedProducers(t *testing.T) {
	c := &Client{clock: realClock{}, cfg: Config{Source: SourceBackend}}
	for _, name := range []string{experimentExposureName, experimentOutcomeName} {
		v := 65535
		e, err := c.buildEnvelope(Event{Name: name, omitUserID: true, SchemaVersion: &v})
		if err != nil || e.SchemaVersion != 2 {
			t.Fatalf("owned version overridden: %d, %v", e.SchemaVersion, err)
		}
	}
}

func TestEventSchemaVersionRevisionIndependent(t *testing.T) {
	for _, revision := range []string{"", "sha256:" + strings.Repeat("42", 32)} {
		t.Run(fmt.Sprintf("declared_%v", revision != ""), func(t *testing.T) {
			headers := make(chan capturedRevisionHeader, 4)
			server := newSchemaRevisionTestServer(t, headers)
			defer server.Close()
			c := newSchemaRevisionTestClient(t, server.URL, func(cfg *Config) { cfg.SchemaRevision = revision })
			defer c.Close(context.Background())
			v := 65535
			if err := c.Track(context.Background(), Event{Name: "schema_revision", SchemaVersion: &v}); err != nil {
				t.Fatal(err)
			}
			got := waitForRevisionHeader(t, headers)
			if got.present != (revision != "") || got.value != revision || !Supports("schema_revision_declaration") {
				t.Fatalf("version changed revision declaration: %+v", got)
			}
		})
	}
}

func TestEventSchemaVersionCatalogueVerdict(t *testing.T) {
	for _, code := range []string{"schema_version_rejected", "schema_inactive"} {
		t.Run(code, func(t *testing.T) {
			c, fake := rejectionClient(t, nil)
			fake.transform = func(result *batchResult) {
				result.Rejected = 1
				result.Accepted = 0
				for i := range result.Events {
					result.Events[i].Status = "rejected"
					result.Events[i].Code = code
				}
			}
			v := 65535
			if err := c.Enqueue(Event{Name: "schema_catalogue", SchemaVersion: &v}); err != nil {
				t.Fatal(err)
			}
			if err := c.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			entries := rejectionEntries(t, c)
			if len(entries) != 1 || entries[0].Code != code || !strings.Contains(c.Snapshot().LastError, code) {
				t.Fatalf("catalogue diagnostic changed: %+v", entries)
			}
			if err := c.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			if fake.count() != 1 || len(fake.batches[0]) != 1 || fake.batches[0][0].SchemaVersion != 65535 {
				t.Fatal("catalogue verdict caused version fallback")
			}
		})
	}
}
