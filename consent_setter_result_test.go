package shardpilot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type setterObservation struct {
	warnings []string
	err      error
}

// Reflect the public result so the same scene can run against the old setters.
func observeSetter(t *testing.T, client *Client, decision ConsentDecision, boolean bool) (got setterObservation) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			got.err = fmt.Errorf("setter panicked: %v", p)
			t.Errorf("setter must return a refusal instead of panicking: %v", p)
		}
	}()
	name, arg := "SetConsentDecision", reflect.ValueOf(decision)
	if boolean {
		name, arg = "SetConsent", reflect.ValueOf(decision == ConsentDecisionGranted)
	}
	values := reflect.ValueOf(client).MethodByName(name).Call([]reflect.Value{arg})
	if len(values) != 2 || values[0].Type().Name() != "ConsentResult" {
		t.Errorf("%s lacks the required (ConsentResult, error) carrier", name)
	} else {
		field := values[0].FieldByName("Warnings")
		if !field.IsValid() || field.Type() != reflect.TypeFor[[]string]() {
			t.Errorf("%s lacks Warnings []string", name)
		} else {
			got.warnings = append([]string(nil), field.Interface().([]string)...)
		}
	}
	if len(values) > 0 {
		last := values[len(values)-1]
		if last.Type().Implements(reflect.TypeFor[error]()) && !last.IsNil() {
			got.err = last.Interface().(error)
		}
	}
	return got
}

func checkSetterResult(t *testing.T, got setterObservation, errorCode, warning string) {
	t.Helper()
	if errorCode == "" {
		if got.err != nil {
			t.Errorf("applied decision returned an error: %v", got.err)
		}
	} else if got.err == nil || !strings.Contains(got.err.Error(), errorCode) {
		t.Errorf("refusal = %v, want %s", got.err, errorCode)
	}
	if warning != "" && !slices.Contains(got.warnings, warning) {
		t.Errorf("warnings = %v, want %s", got.warnings, warning)
	}
	if warning == "" && len(got.warnings) != 0 {
		t.Errorf("unexpected warnings: %v", got.warnings)
	}
}

func setterClient(t *testing.T, floor bool, dir, actor string) (*Client, *floorTestServer) {
	t.Helper()
	state, server := newFloorTestServer(t)
	t.Cleanup(server.Close)
	client := newFloorTestClient(t, server.URL, dir, func(cfg *Config) {
		if !floor {
			cfg.ConsentFloor = nil
		}
		cfg.AnonymousID = actor
	})
	t.Cleanup(func() {
		state.setBatchOutcome(http.StatusAccepted)
		if client.spool != nil {
			client.spool.mu.Lock()
			client.spool.removeFn = os.Remove
			client.spool.renameFn = os.Rename
			client.spool.mu.Unlock()
		}
		if client.consentOutbox != nil {
			client.consentOutbox.mu.Lock()
			client.consentOutbox.renameFn = os.Rename
			client.consentOutbox.mu.Unlock()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Close(ctx); err != nil {
			t.Errorf("cleanup Close: %v", err)
		}
	})
	return client, state
}

func TestConsentSetterResultHealthyAndRefused(t *testing.T) {
	for _, floor := range []bool{false, true} {
		for _, boolean := range []bool{false, true} {
			t.Run(fmt.Sprintf("floor-%t/bool-%t", floor, boolean), func(t *testing.T) {
				client, _ := setterClient(t, floor, "", "")
				checkSetterResult(t, observeSetter(t, client, ConsentDecisionGranted, boolean), "", "")
				checkSetterResult(t, observeSetter(t, client, ConsentDecisionDenied, boolean), "", "")
				if got := client.ConsentState(); got != ConsentDenied {
					t.Errorf("healthy denial state = %s", got)
				}
				checkSetterResult(t, observeSetter(t, client, "invalid", false), "invalid_consent", "")
				if got := client.ConsentState(); got != ConsentDenied {
					t.Errorf("invalid input changed state to %s", got)
				}
				if err := client.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				checkSetterResult(t, observeSetter(t, client, ConsentDecisionGranted, boolean), "shutdown", "")
				if got := client.ConsentState(); got != ConsentDenied {
					t.Errorf("post-shutdown setter changed state to %s", got)
				}
			})
		}
	}
	for _, boolean := range []bool{false, true} {
		t.Run(fmt.Sprintf("not-initialized/bool-%t", boolean), func(t *testing.T) {
			checkSetterResult(t, observeSetter(t, new(Client), ConsentDecisionDenied, boolean), "not_initialized", "")
			checkSetterResult(t, observeSetter(t, nil, ConsentDecisionDenied, boolean), "not_initialized", "")
		})
	}
}

func TestConsentSetterForcedMinorCannotBeReopened(t *testing.T) {
	for _, floor := range []bool{false, true} {
		for _, boolean := range []bool{false, true} {
			t.Run(fmt.Sprintf("floor-%t/bool-%t", floor, boolean), func(t *testing.T) {
				client, _ := setterClient(t, floor, "", "")
				checkSetterResult(t, observeSetter(t, client, ConsentDecisionDeniedForcedMinor, false), "", "")
				checkSetterResult(t, observeSetter(t, client, ConsentDecisionGranted, boolean), "consent_forced_minor", "")
				checkSetterResult(t, observeSetter(t, client, ConsentDecisionDenied, boolean), "", "")
				if got := client.ConsentState(); got != ConsentDeniedForcedMinor {
					t.Errorf("ordinary denial erased forced-minor state: %s", got)
				}
				checkSetterResult(t, observeSetter(t, client, ConsentDecisionGranted, boolean), "consent_forced_minor", "")
				if got := client.ConsentState(); got != ConsentDeniedForcedMinor {
					t.Errorf("ordinary grant changed restricted denial: %s", got)
				}
				if err := client.Enqueue(Event{Name: "screen_view"}); !errors.Is(err, ErrConsentDenied) {
					t.Errorf("forced-minor intake = %v, want denied", err)
				}
			})
		}
	}
}

func TestConsentSetterShutdownPrecedesValidation(t *testing.T) {
	for _, floor := range []bool{false, true} {
		for _, boolean := range []bool{false, true} {
			for _, decision := range []ConsentDecision{ConsentDecisionGranted, ConsentDecisionDenied} {
				t.Run(fmt.Sprintf("floor-%t/bool-%t/%s", floor, boolean, decision), func(t *testing.T) {
					client, wire := setterClient(t, floor, "", strings.Repeat("x", 513))
					checkSetterResult(t, observeSetter(t, client, ConsentDecisionGranted, boolean), "consent_actor_invalid", "")
					checkSetterResult(t, observeSetter(t, client, ConsentDecisionDenied, boolean), "", "consent_actor_invalid")
					if err := client.Close(context.Background()); err != nil {
						t.Fatal(err)
					}
					got := observeSetter(t, client, decision, boolean)
					checkSetterResult(t, got, "shutdown", "")
					if !errors.Is(got.err, ErrConsentShutdown) || !errors.Is(got.err, ErrClosed) {
						t.Errorf("terminal client returned %v, want shutdown matching ErrClosed", got.err)
					}
					if client.ConsentState() != ConsentDenied || wire.consentCount() != 0 {
						t.Errorf("closed setter changed state or emitted receipt: %s / %v", client.ConsentState(), wire.snapshotOrder())
					}
				})
			}
		}
		t.Run(fmt.Sprintf("floor-%t/invalid-decision", floor), func(t *testing.T) {
			client, _ := setterClient(t, floor, "", "")
			checkSetterResult(t, observeSetter(t, client, "invalid", false), "invalid_consent", "")
			prior := client.ConsentState()
			if err := client.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			got := observeSetter(t, client, "invalid", false)
			checkSetterResult(t, got, "shutdown", "")
			if !errors.Is(got.err, ErrClosed) || client.ConsentState() != prior {
				t.Errorf("closed invalid decision returned %v or changed state", got.err)
			}
		})
	}
}

func TestConsentSetterPurgeResult(t *testing.T) {
	for _, floor := range []bool{false, true} {
		for _, denial := range []ConsentDecision{ConsentDecisionDenied, ConsentDecisionDeniedForcedMinor} {
			t.Run(fmt.Sprintf("floor-%t/%s", floor, denial), func(t *testing.T) {
				dir := t.TempDir()
				client, state := setterClient(t, floor, dir, "actor-test")
				checkSetterResult(t, observeSetter(t, client, ConsentDecisionGranted, false), "", "")
				waitFor(t, 3*time.Second, "setup grant receipt", func() bool { return state.consentCount() == 1 && (!floor || !client.consentOutbox.pending()) })
				state.setBatchOutcome(http.StatusServiceUnavailable)
				if err := client.Enqueue(Event{ID: "retained-event", Name: "screen_view"}); err != nil {
					t.Fatal(err)
				}
				if err := client.Flush(context.Background()); err == nil || !spoolFileExists(dir) {
					t.Fatalf("setup did not retain the real failed batch: %v", err)
				}
				client.spool.mu.Lock()
				client.spool.removeFn = func(string) error { return errors.New("synthetic purge failure") }
				client.spool.mu.Unlock()
				checkSetterResult(t, observeSetter(t, client, denial, false), "", "spool_purge_failed")
				if got := client.ConsentState(); got != ConsentState(denial) || !client.spool.owedWipe() {
					t.Errorf("failed denial purge state=%s owed=%t", got, client.spool.owedWipe())
				}
				count := state.batchCount()
				refusal := "spool_purge_failed"
				if denial == ConsentDecisionDeniedForcedMinor {
					refusal = "consent_forced_minor"
				}
				checkSetterResult(t, observeSetter(t, client, ConsentDecisionGranted, false), refusal, "")
				if got := client.ConsentState(); got != ConsentState(denial) {
					t.Errorf("refused grant changed denial to %s", got)
				}
				if err := client.Enqueue(Event{Name: "screen_view"}); !errors.Is(err, ErrConsentDenied) {
					t.Errorf("purge-debt intake = %v, want denied", err)
				}
				_ = client.Flush(context.Background())
				if got := state.batchCount(); got != count {
					t.Errorf("analytics dispatched while purge debt remained: %d -> %d", count, got)
				}
				client.spool.mu.Lock()
				client.spool.removeFn = os.Remove
				client.spool.mu.Unlock()
				if denial == ConsentDecisionDeniedForcedMinor {
					checkSetterResult(t, observeSetter(t, client, ConsentDecisionGranted, true), "consent_forced_minor", "")
					if !client.spool.settleOwedWipe() {
						t.Fatal("healthy purge retry did not settle")
					}
					checkSetterResult(t, observeSetter(t, client, ConsentDecisionGranted, false), "consent_forced_minor", "")
					if client.ConsentState() != ConsentDeniedForcedMinor {
						t.Error("purge recovery lifted forced-minor denial")
					}
				} else {
					checkSetterResult(t, observeSetter(t, client, ConsentDecisionGranted, true), "", "")
					if client.ConsentState() != ConsentGranted || client.spool.owedWipe() || spoolFileExists(dir) {
						t.Error("healthy grant failed to settle purge before opening")
					}
				}
			})
		}
	}
}

func TestConsentSetterInvalidActorDenial(t *testing.T) {
	for _, floor := range []bool{false, true} {
		for _, denial := range []ConsentDecision{ConsentDecisionDenied, ConsentDecisionDeniedForcedMinor} {
			t.Run(fmt.Sprintf("floor-%t/%s", floor, denial), func(t *testing.T) {
				client, state := setterClient(t, floor, "", strings.Repeat("x", 513))
				var mints atomic.Int32
				client.consentOwedMu.Lock()
				client.consentMintIDFn = func() (string, error) { mints.Add(1); return "synthetic-invalid-actor-mint", nil }
				client.consentOwedMu.Unlock()
				checkSetterResult(t, observeSetter(t, client, denial, false), "", "consent_actor_invalid")
				if got := client.ConsentState(); got != ConsentState(denial) {
					t.Errorf("invalid-actor denial was not applied: %s", got)
				}
				checkSetterResult(t, observeSetter(t, client, ConsentDecisionGranted, false), "consent_actor_invalid", "")
				if client.ConsentState() != ConsentState(denial) {
					t.Error("invalid-actor grant changed the denial")
				}
				if err := client.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				if mints.Load() != 0 {
					t.Errorf("invalid actor minted %d receipts", mints.Load())
				}
				if state.consentCount() != 0 || state.batchCount() != 0 {
					t.Errorf("invalid actor reached the wire: %v", state.snapshotOrder())
				}
			})
		}
	}
}

func TestConsentSetterPersistenceWarnings(t *testing.T) {
	for _, floor := range []bool{false, true} {
		for _, decision := range []ConsentDecision{ConsentDecisionGranted, ConsentDecisionDenied, ConsentDecisionDeniedForcedMinor} {
			t.Run(fmt.Sprintf("record/floor-%t/%s", floor, decision), func(t *testing.T) {
				client, _ := setterClient(t, floor, t.TempDir(), "synthetic-setter-actor")
				client.spool.mu.Lock()
				client.spool.renameFn = func(string, string) error { return errors.New("synthetic record failure") }
				client.spool.mu.Unlock()
				result := observeSetter(t, client, decision, false)
				checkSetterResult(t, result, "", "consent_persist_failed")
				if got := client.ConsentState(); got != ConsentState(decision) {
					t.Fatalf("record failure lost applied decision: %s", got)
				}
				client.spool.mu.Lock()
				client.spool.renameFn = os.Rename
				client.spool.mu.Unlock()
				checkSetterResult(t, observeSetter(t, client, decision, false), "", "")
				checkSetterResult(t, result, "", "consent_persist_failed")
			})
		}
	}
	for _, decision := range []ConsentDecision{ConsentDecisionGranted, ConsentDecisionDenied, ConsentDecisionDeniedForcedMinor} {
		t.Run("outbox/"+string(decision), func(t *testing.T) {
			client, _ := setterClient(t, true, t.TempDir(), "synthetic-warning-actor")
			client.consentOutbox.mu.Lock()
			client.consentOutbox.renameFn = func(string, string) error { return errors.New("synthetic outbox failure") }
			client.consentOutbox.mu.Unlock()
			result := observeSetter(t, client, decision, false)
			checkSetterResult(t, result, "", "consent_outbox_persist_failed")
			if got := client.ConsentState(); got != ConsentState(decision) {
				t.Fatalf("outbox failure lost applied decision: %s", got)
			}
			client.consentOutbox.mu.Lock()
			client.consentOutbox.renameFn = os.Rename
			client.consentOutbox.mu.Unlock()
			checkSetterResult(t, observeSetter(t, client, decision, false), "", "")
			checkSetterResult(t, result, "", "consent_outbox_persist_failed")
		})
		t.Run("mint/"+string(decision), func(t *testing.T) {
			client, _ := setterClient(t, true, t.TempDir(), "synthetic-warning-actor")
			client.consentOwedMu.Lock()
			client.consentMintIDFn = func() (string, error) { return "", errors.New("synthetic mint failure") }
			client.consentOwedMu.Unlock()
			result := observeSetter(t, client, decision, false)
			client.consentOwedMu.Lock()
			client.consentMintIDFn = nil
			client.consentOwedMu.Unlock()
			checkSetterResult(t, result, "", "consent_outbox_persist_failed")
			if got := client.ConsentState(); got != ConsentState(decision) {
				t.Fatalf("mint failure lost applied decision: %s", got)
			}
			checkSetterResult(t, observeSetter(t, client, decision, false), "", "")
		})
	}
}

func TestConsentSetterOverlappingDenials(t *testing.T) {
	for _, floor := range []bool{false, true} {
		t.Run(fmt.Sprintf("floor-%t", floor), func(t *testing.T) {
			client, _ := setterClient(t, floor, t.TempDir(), "synthetic-setter-actor")
			entered := make(chan struct{})
			release := make(chan struct{})
			var calls atomic.Int32
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			client.consentSlowHalfGate = func() {
				if calls.Add(1) == 1 {
					close(entered)
					<-release
				}
			}
			done := make(chan struct{}, 3)
			start := func(decision ConsentDecision) {
				go func() {
					observeSetter(t, client, decision, false)
					done <- struct{}{}
				}()
			}
			start(ConsentDecisionDenied)
			<-entered
			start(ConsentDecisionGranted)
			waitFor(t, time.Second, "grant admitted behind denial", func() bool { return calls.Load() >= 2 })
			if got := client.ConsentState(); got != ConsentDenied {
				t.Errorf("grant bypassed earlier unfinished denial: %s", got)
			}
			start(ConsentDecisionDeniedForcedMinor)
			waitFor(t, time.Second, "later denial applied immediately", func() bool { return calls.Load() == 3 })
			if got := client.ConsentState(); got != ConsentDeniedForcedMinor {
				t.Errorf("later forced-minor denial did not apply: %s", got)
			}
			unblock()
			for range 3 {
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("overlapping setters did not finish")
				}
			}
			if got := client.ConsentState(); got != ConsentDeniedForcedMinor {
				t.Errorf("older grant overwrote newer denial: %s", got)
			}
			if got, ok := loadConsentRecord(client.spool.dir, client.spool.actorDigest); !ok || got != ConsentDeniedForcedMinor {
				t.Errorf("durable order differs from live denial: %s, %t", got, ok)
			}
		})
	}
}

func TestConsentSetterBoolWarning(t *testing.T) {
	for _, floor := range []bool{false, true} {
		t.Run(fmt.Sprintf("floor-%t", floor), func(t *testing.T) {
			client, _ := setterClient(t, floor, t.TempDir(), "synthetic-bool-warning")
			client.spool.mu.Lock()
			client.spool.renameFn = func(string, string) error { return errors.New("synthetic record failure") }
			client.spool.mu.Unlock()
			checkSetterResult(t, observeSetter(t, client, ConsentDecisionDenied, true), "", "consent_persist_failed")
		})
	}
}

func TestConsentSetterForcedMinorReload(t *testing.T) {
	for _, floor := range []bool{false, true} {
		t.Run(fmt.Sprintf("floor-%t", floor), func(t *testing.T) {
			dir := t.TempDir()
			client, wire := setterClient(t, floor, dir, "synthetic-restart-actor")
			checkSetterResult(t, observeSetter(t, client, ConsentDecisionDeniedForcedMinor, false), "", "")
			if err := client.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			waitFor(t, time.Second, "forced-minor receipt delivered", func() bool { return wire.consentCount() == 1 })
			before, err := os.ReadFile(consentRecordPath(dir))
			if err != nil {
				t.Fatal(err)
			}
			checkSetterResult(t, observeSetter(t, client, ConsentDecisionDenied, true), "", "")
			checkSetterResult(t, observeSetter(t, client, ConsentDecisionDenied, false), "", "")
			if got := client.ConsentState(); got != ConsentDeniedForcedMinor {
				t.Errorf("ordinary denial erased forced-minor state: %s", got)
			}
			checkSetterResult(t, observeSetter(t, client, ConsentDecisionGranted, false), "consent_forced_minor", "")
			if err := client.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(consentRecordPath(dir))
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != string(before) {
				t.Errorf("no-op denial rewrote the forced-minor record: %s", after)
			}
			if wire.consentCount() != 1 {
				t.Errorf("no-op denial sent a new receipt: %v", wire.snapshotOrder())
			}
			restarted, _ := setterClient(t, floor, dir, "synthetic-restart-actor")
			if got := restarted.ConsentState(); got != ConsentDeniedForcedMinor {
				t.Errorf("restart lost forced-minor state: %s", got)
			}
			checkSetterResult(t, observeSetter(t, restarted, ConsentDecisionGranted, true), "consent_forced_minor", "")
			if err := restarted.Enqueue(Event{Name: "screen_view"}); !errors.Is(err, ErrConsentDenied) {
				t.Errorf("restart reopened analytics: %v", err)
			}
		})
	}
}

func TestConsentSetterNoopPreservesOwedReceipt(t *testing.T) {
	client, wire := setterClient(t, true, t.TempDir(), "synthetic-owed-actor")
	client.consentOwedMu.Lock()
	client.consentMintIDFn = func() (string, error) { return "", errors.New("synthetic mint failure") }
	client.consentOwedMu.Unlock()
	checkSetterResult(t, observeSetter(t, client, ConsentDecisionDeniedForcedMinor, false), "", "consent_outbox_persist_failed")
	before := client.consentMintOwedSnapshot()
	checkSetterResult(t, observeSetter(t, client, ConsentDecisionDenied, true), "", "")
	after := client.consentMintOwedSnapshot()
	if before == nil || after == nil || after.decision != ConsentDecisionDeniedForcedMinor || before.decidedAt != after.decidedAt {
		t.Errorf("no-op denial replaced or lost the owed forced-minor receipt: before=%+v after=%+v", before, after)
	}
	client.consentOwedMu.Lock()
	client.consentMintIDFn = nil
	client.consentOwedMu.Unlock()
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if wire.consentCount() != 1 || wire.consentAt(0)["reason"] != "denied_forced_minor" {
		t.Errorf("recovered receipt lost forced-minor provenance: %v", wire.snapshotOrder())
	}
}

func TestConsentSetterRestrictionScopeAndOrdinaryControl(t *testing.T) {
	for _, floor := range []bool{false, true} {
		for _, restricted := range []bool{false, true} {
			t.Run(fmt.Sprintf("floor-%t/restricted-%t", floor, restricted), func(t *testing.T) {
				dir := t.TempDir()
				client, _ := setterClient(t, floor, dir, "synthetic-scope-a")
				decision := ConsentDecisionDenied
				if restricted {
					decision = ConsentDecisionDeniedForcedMinor
				}
				checkSetterResult(t, observeSetter(t, client, decision, false), "", "")
				if err := client.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				actor := "synthetic-scope-a"
				if restricted {
					actor = "synthetic-scope-b"
				}
				restarted, _ := setterClient(t, floor, dir, actor)
				checkSetterResult(t, observeSetter(t, restarted, ConsentDecisionGranted, false), "", "")
				if got := restarted.ConsentState(); got != ConsentGranted {
					t.Errorf("ordinary/foreign-marker control refused grant: %s", got)
				}
			})
		}
	}
}

func TestConsentSetterForcedMinorReceiptTail(t *testing.T) {
	dir := t.TempDir()
	client, _ := setterClient(t, true, dir, "anon-spool-1")
	checkSetterResult(t, observeSetter(t, client, ConsentDecisionDeniedForcedMinor, false), "", "")
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	grant := testConsentReceipt("synthetic-old-client-grant", true)
	grant.DecidedAt = "2030-01-01T00:00:00Z"
	outbox := newConsentOutbox(dir)
	if outbox.append(grant) {
		t.Fatal("could not seed retained receipt")
	}
	restarted, wire := setterClient(t, true, dir, "anon-spool-1")
	if got := restarted.ConsentState(); got != ConsentDeniedForcedMinor {
		t.Errorf("receipt tail lifted forced-minor state: %s", got)
	}
	checkSetterResult(t, observeSetter(t, restarted, ConsentDecisionGranted, true), "consent_forced_minor", "")
	if err := restarted.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if wire.consentCount() != 0 {
		t.Errorf("withheld grant receipt reached wire: %v", wire.snapshotOrder())
	}
}
