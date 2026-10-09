package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

var noticeExample = [3]string{"nv_2026_10_1", "en", "pv_2026_10_1"}

// Invoke the actual public setter on both sides of the API change. On the old
// API the decision still runs; missing provenance is an assertion, not a build failure.
func observeNotice(t *testing.T, c *Client, decision ConsentDecision, boolean bool, notices ...[3]string) (setterObservation, func()) {
	t.Helper()
	name, arg := "SetConsentDecision", reflect.ValueOf(decision)
	if boolean {
		name, arg = "SetConsent", reflect.ValueOf(decision == ConsentDecisionGranted)
	}
	method := reflect.ValueOf(c).MethodByName(name)
	var values []reflect.Value
	mutate := func() {}
	if method.Type().IsVariadic() && method.Type().NumIn() == 2 {
		typ := method.Type().In(1)
		if typ.Elem().Name() != "ConsentNotice" || typ.Elem().NumField() != 3 {
			t.Fatalf("unexpected carrier: %s", typ)
		}
		carrier := reflect.MakeSlice(typ, len(notices), len(notices))
		for i, notice := range notices {
			for j, name := range []string{"NoticeVersion", "NoticeLocale", "PolicyVersion"} {
				field := carrier.Index(i).FieldByName(name)
				if !field.IsValid() || field.Kind() != reflect.String {
					t.Fatalf("missing string %s", name)
				}
				field.SetString(notice[j])
			}
		}
		values = method.CallSlice([]reflect.Value{arg, carrier})
		mutate = func() {
			for i := range notices {
				for _, name := range []string{"NoticeVersion", "NoticeLocale", "PolicyVersion"} {
					carrier.Index(i).FieldByName(name).SetString("changed")
				}
			}
		}
	} else {
		values = method.Call([]reflect.Value{arg})
		if len(notices) != 0 {
			t.Errorf("%s ran but has no per-call notice carrier", name)
		}
	}
	result := setterObservation{warnings: values[0].Interface().(ConsentResult).Warnings}
	if !values[1].IsNil() {
		result.err = values[1].Interface().(error)
	}
	return result, mutate
}

func checkNoticeBody(t *testing.T, body map[string]any, want *[3]string) {
	t.Helper()
	for i, key := range []string{"notice_version", "notice_locale", "policy_version"} {
		got, exists := body[key]
		if want == nil {
			if exists {
				t.Errorf("omitted notice contains %s=%v", key, got)
			}
		} else if !exists || got != want[i] {
			t.Errorf("%s = %v, want %q", key, got, want[i])
		}
	}
	if _, exists := body["notice_text"]; exists {
		t.Error("notice text reached wire")
	}
	if body["actor_identifier"] != "synthetic-notice-actor" {
		t.Errorf("actor control: %v", body)
	}
	if body["idempotency_key"] == "" || body["decided_at"] == "" {
		t.Errorf("receipt control: %v", body)
	}
}

func TestConsentNoticeWire(t *testing.T) {
	for _, floor := range []bool{false, true} {
		for _, boolean := range []bool{false, true} {
			t.Run(fmt.Sprintf("floor-%t/bool-%t", floor, boolean), func(t *testing.T) {
				c, wire := setterClient(t, floor, "", "synthetic-notice-actor")
				for _, decision := range []ConsentDecision{ConsentDecisionGranted, ConsentDecisionDenied} {
					got, mutate := observeNotice(t, c, decision, boolean, noticeExample)
					checkSetterResult(t, got, "", "")
					mutate()
				}
				got, _ := observeNotice(t, c, ConsentDecisionDenied, boolean)
				checkSetterResult(t, got, "", "")
				if err := c.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				if wire.consentCount() != 3 {
					t.Fatalf("real POST count = %d, want 3", wire.consentCount())
				}
				for i := 0; i < 3; i++ {
					var want *[3]string
					if i < 2 {
						want = &noticeExample
					}
					body := wire.consentAt(i)
					checkNoticeBody(t, body, want)
					if consentBoolCategory(t, body) != (i == 0) {
						t.Errorf("decision control for receipt %d: %v", i, body)
					}
				}
			})
		}
	}
}

func TestConsentNoticeInvalidOutcomes(t *testing.T) {
	cases := map[string][][3]string{
		"empty":                       {{"", "", ""}},
		"multiple":                    {noticeExample, noticeExample},
		"missing-version":             {{"", "en", "p"}},
		"missing-locale":              {{"v", "", "p"}},
		"missing-policy":              {{"v", "en", ""}},
		"long-version":                {{strings.Repeat("v", 65), "en", "p"}},
		"long-policy":                 {{"v", "en", strings.Repeat("p", 65)}},
		"version-space":               {{"v 1", "en", "p"}},
		"policy-colon":                {{"v", "en", "p:1"}},
		"version-non-ascii":           {{"v\u00e9", "en", "p"}},
		"locale-underscore":           {{"v", "en_US", "p"}},
		"locale-incomplete-extension": {{"v", "en-a", "p"}},
	}
	for name, notices := range cases {
		for _, floor := range []bool{false, true} {
			for _, boolean := range []bool{false, true} {
				for _, grant := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/floor-%t/bool-%t/grant-%t", name, floor, boolean, grant), func(t *testing.T) {
						c, wire := setterClient(t, floor, "", "synthetic-notice-actor")
						decision := ConsentDecisionDenied
						if grant {
							decision = ConsentDecisionGranted
						}
						got, _ := observeNotice(t, c, decision, boolean, notices...)
						if grant {
							checkSetterResult(t, got, "consent_notice_invalid", "")
							if c.ConsentState() != ConsentUnknown {
								t.Errorf("refused grant changed state to %s", c.ConsentState())
							}
						} else {
							checkSetterResult(t, got, "", "consent_notice_invalid")
							if c.ConsentState() != ConsentDenied {
								t.Errorf("invalid notice lost denial: %s", c.ConsentState())
							}
						}
						if err := c.Close(context.Background()); err != nil {
							t.Fatal(err)
						}
						if grant {
							if wire.consentCount() != 0 {
								t.Errorf("refused grant sent %d receipts", wire.consentCount())
							}
						} else {
							if wire.consentCount() != 1 {
								t.Fatalf("denial POST count = %d", wire.consentCount())
							}
							checkNoticeBody(t, wire.consentAt(0), nil)
							if consentBoolCategory(t, wire.consentAt(0)) {
								t.Error("denial became wire grant")
							}
						}
					})
				}
			}
		}
	}
}

func TestConsentNoticeLocaleSyntax(t *testing.T) {
	valid := []string{"en", "EN-us", "zh-cmn-Hans-CN", "es-419", "sl-rozaj-biske", "de-CH-1901", "en-a-abc-b-def-x-a", "x-a", "en-GB-oed", "I-KLINGON", "sgn-BE-FR", "zz", "abcd", "abcdefgh", "en-abc-def-ghi", "en-x-12345678-12345678-12345678-123"}
	invalid := []string{"", "e", "en-", "-en", "en--US", "en_US", "en US", "12", "abcdefghi", "en-12", "en-1234a5678", "en-abcd-abcd", "en-US-Latn", "en-a", "en-x", "x", "a-abc", "i-unknown", "en-\u00e9", "en-\u212aR", "en-\u017fA", "en-x-123456789", "en-x-12345678-12345678-12345678-1234"}
	for _, group := range []struct {
		tags  []string
		valid bool
	}{{valid, true}, {invalid, false}} {
		for _, tag := range group.tags {
			t.Run(fmt.Sprintf("valid-%t/%s", group.valid, tag), func(t *testing.T) {
				c, wire := setterClient(t, false, "", "synthetic-notice-actor")
				notice := [3]string{strings.Repeat("V", 64), tag, strings.Repeat("p", 64)}
				got, _ := observeNotice(t, c, ConsentDecisionDenied, false, notice)
				if group.valid {
					checkSetterResult(t, got, "", "")
				} else {
					checkSetterResult(t, got, "", "consent_notice_invalid")
				}
				if err := c.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				if wire.consentCount() != 1 {
					t.Fatalf("real POST count=%d", wire.consentCount())
				}
				var want *[3]string
				if group.valid {
					want = &notice
				}
				checkNoticeBody(t, wire.consentAt(0), want)
			})
		}
	}
}

func TestConsentNoticeRetryReload(t *testing.T) {
	dir := t.TempDir()
	c, wire := setterClient(t, true, dir, "synthetic-notice-actor")
	wire.setConsentOutcome(http.StatusServiceUnavailable, "3600")
	got, mutate := observeNotice(t, c, ConsentDecisionDenied, false, noticeExample)
	checkSetterResult(t, got, "", "")
	mutate()
	waitFor(t, time.Second, "first notice POST", func() bool { return wire.consentCount() == 1 })
	original := wire.consentAt(0)
	checkNoticeBody(t, original, &noticeExample)
	next := [3]string{"next-v", "fr-CA", "next-p"}
	got, _ = observeNotice(t, c, ConsentDecisionGranted, true, next)
	checkSetterResult(t, got, "", "")
	clearConsentDeferral(c)
	c.dispatchConsentReceipts(context.Background(), true)
	if wire.consentCount() != 2 {
		t.Fatalf("retry count=%d", wire.consentCount())
	}
	if !reflect.DeepEqual(original, wire.consentAt(1)) {
		t.Errorf("retry changed receipt: %v", wire.consentAt(1))
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, consentOutboxFileName))
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		Receipts []map[string]any `json:"receipts"`
	}
	if err = json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	if len(record.Receipts) != 2 {
		t.Fatalf("durable receipt count=%d", len(record.Receipts))
	}
	checkNoticeBody(t, record.Receipts[0], &noticeExample)
	checkNoticeBody(t, record.Receipts[1], &next)
	wire.setConsentOutcome(http.StatusOK, "")
	restored, err := NewClient(c.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = restored.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if wire.consentCount() != 4 {
		t.Fatalf("restart POST count=%d", wire.consentCount())
	}
	if !reflect.DeepEqual(original, wire.consentAt(2)) {
		t.Errorf("reload changed original receipt: %v", wire.consentAt(2))
	}
	checkNoticeBody(t, wire.consentAt(3), &next)
}

func TestConsentNoticeOwedMint(t *testing.T) {
	for _, decision := range []ConsentDecision{ConsentDecisionGranted, ConsentDecisionDeniedForcedMinor} {
		t.Run(string(decision), func(t *testing.T) {
			c, wire := setterClient(t, true, t.TempDir(), "synthetic-notice-actor")
			c.consentOwedMu.Lock()
			c.consentMintIDFn = func() (string, error) { return "", errors.New("synthetic mint failure") }
			c.consentOwedMu.Unlock()
			got, mutate := observeNotice(t, c, decision, false, noticeExample)
			checkSetterResult(t, got, "", "consent_outbox_persist_failed")
			mutate()
			c.consentOwedMu.Lock()
			c.consentMintIDFn = nil
			c.consentOwedMu.Unlock()
			if err := c.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if wire.consentCount() != 1 {
				t.Fatalf("recovered mint POST count=%d", wire.consentCount())
			}
			checkNoticeBody(t, wire.consentAt(0), &noticeExample)
			if decision == ConsentDecisionDeniedForcedMinor && wire.consentAt(0)["reason"] != consentDecisionReason {
				t.Error("forced-minor reason lost")
			}
		})
	}
}

func TestConsentNoticeCombinedWarnings(t *testing.T) {
	for _, floor := range []bool{false, true} {
		t.Run(fmt.Sprintf("floor-%t", floor), func(t *testing.T) {
			c, wire := setterClient(t, floor, t.TempDir(), "synthetic-notice-actor")
			c.spool.mu.Lock()
			c.spool.renameFn = func(string, string) error { return errors.New("synthetic record failure") }
			c.spool.removeFn = func(string) error { return errors.New("synthetic purge failure") }
			c.spool.mu.Unlock()
			if floor {
				c.consentOutbox.mu.Lock()
				c.consentOutbox.renameFn = func(string, string) error { return errors.New("synthetic receipt failure") }
				c.consentOutbox.mu.Unlock()
			}
			got, _ := observeNotice(t, c, ConsentDecisionDenied, true, [3]string{})
			for _, warning := range []string{"consent_notice_invalid", "consent_persist_failed", "spool_purge_failed"} {
				if !slices.Contains(got.warnings, warning) {
					t.Errorf("warnings %v lack %s", got.warnings, warning)
				}
			}
			if floor && !slices.Contains(got.warnings, "consent_outbox_persist_failed") {
				t.Errorf("outbox warning lost: %v", got.warnings)
			}
			if got.err != nil || c.ConsentState() != ConsentDenied {
				t.Errorf("denial lost: %+v / %s", got, c.ConsentState())
			}
			c.spool.mu.Lock()
			c.spool.renameFn = os.Rename
			c.spool.removeFn = os.Remove
			c.spool.mu.Unlock()
			if floor {
				c.consentOutbox.mu.Lock()
				c.consentOutbox.renameFn = os.Rename
				c.consentOutbox.mu.Unlock()
			}
			got, _ = observeNotice(t, c, ConsentDecisionDenied, true, [3]string{})
			checkSetterResult(t, got, "", "consent_notice_invalid")
			if err := c.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < wire.consentCount(); i++ {
				checkNoticeBody(t, wire.consentAt(i), nil)
			}
		})
	}
}

func TestConsentNoticeForcedMinorAndTerminal(t *testing.T) {
	for _, floor := range []bool{false, true} {
		t.Run(fmt.Sprintf("floor-%t", floor), func(t *testing.T) {
			c, wire := setterClient(t, floor, t.TempDir(), "synthetic-notice-actor")
			got, _ := observeNotice(t, c, ConsentDecisionDeniedForcedMinor, false, noticeExample)
			checkSetterResult(t, got, "", "")
			got, _ = observeNotice(t, c, ConsentDecisionDenied, true, [3]string{})
			checkSetterResult(t, got, "", "consent_notice_invalid")
			if c.ConsentState() != ConsentDeniedForcedMinor {
				t.Error("notice input erased forced-minor restriction")
			}
			if err := c.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if wire.consentCount() != 1 {
				t.Fatalf("forced-minor no-op minted receipt: %d", wire.consentCount())
			}
			checkNoticeBody(t, wire.consentAt(0), &noticeExample)
			got, _ = observeNotice(t, c, ConsentDecisionGranted, true, [3]string{})
			checkSetterResult(t, got, "shutdown", "")
			got, _ = observeNotice(t, nil, ConsentDecisionGranted, true, [3]string{})
			checkSetterResult(t, got, "not_initialized", "")
		})
	}
}

func TestConsentNoticeGolden(t *testing.T) {
	request, err := os.ReadFile("testdata/consent-notice/request.json")
	if err != nil {
		t.Fatal(err)
	}
	response, err := os.ReadFile("testdata/consent-notice/response.json")
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]any
	if err = json.Unmarshal(request, &want); err != nil {
		t.Fatal(err)
	}
	for _, floor := range []bool{false, true} {
		t.Run(fmt.Sprintf("floor-%t", floor), func(t *testing.T) {
			wire, server := newFloorTestServer(t)
			defer server.Close()
			wire.setConsentRawBody(string(response))
			c := newFloorTestClient(t, server.URL, "", func(cfg *Config) {
				cfg.WorkspaceID = want["workspace_id"].(string)
				cfg.AppID = want["app_id"].(string)
				cfg.AnonymousID = want["actor_identifier"].(string)
				if !floor {
					cfg.ConsentFloor = nil
				}
			})
			notice := [3]string{want["notice_version"].(string), want["notice_locale"].(string), want["policy_version"].(string)}
			got, _ := observeNotice(t, c, ConsentDecisionDenied, false, notice)
			checkSetterResult(t, got, "", "")
			if err := c.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if wire.consentCount() != 1 {
				t.Fatalf("POST count=%d", wire.consentCount())
			}
			actual := wire.consentAt(0)
			if _, err := time.Parse(time.RFC3339Nano, actual["decided_at"].(string)); err != nil {
				t.Fatal(err)
			}
			if actual["idempotency_key"] == "" {
				t.Fatal("missing real minted key")
			}
			actual["decided_at"] = want["decided_at"]
			actual["idempotency_key"] = want["idempotency_key"]
			if !reflect.DeepEqual(actual, want) {
				t.Errorf("serialized request = %v, want %v", actual, want)
			}
			if floor && c.Snapshot().ConsentRecorded != 1 {
				t.Errorf("fixture response not acknowledged: %+v", c.Snapshot())
			}
		})
	}
}

func TestConsentNoticeStoredValidation(t *testing.T) {
	for _, grant := range []bool{false, true} {
		for name, extra := range map[string]string{
			"legacy": "", "valid": `,"notice_version":"v","notice_locale":"en","policy_version":"p"`,
			"partial": `,"notice_version":"v"`, "malformed": `,"notice_version":"v","notice_locale":"en_US","policy_version":"p"`,
		} {
			t.Run(fmt.Sprintf("grant-%t/%s", grant, name), func(t *testing.T) {
				raw := fmt.Sprintf(`{"idempotency_key":"synthetic-key","workspace_id":"w","app_id":"a","environment_id":"e","actor_identifier":"synthetic-notice-actor","categories":{"analytics":%t},"decided_at":"2026-06-10T12:00:00Z"%s}`, grant, extra)
				var entry consentReceipt
				if err := json.Unmarshal([]byte(raw), &entry); err != nil {
					t.Fatal(err)
				}
				sanitized, ok := sanitizeConsentReceipt(entry)
				want := name == "legacy" || name == "valid"
				if ok != want {
					t.Fatalf("stored receipt accepted=%t, want %t", ok, want)
				}
				if !ok {
					return
				}
				encoded, err := json.Marshal(consentReceiptWire(sanitized))
				if err != nil {
					t.Fatal(err)
				}
				var body map[string]any
				if err = json.Unmarshal(encoded, &body); err != nil {
					t.Fatal(err)
				}
				var notice *[3]string
				if name == "valid" {
					notice = &[3]string{"v", "en", "p"}
				}
				checkNoticeBody(t, body, notice)
			})
		}
	}
}

func TestConsentNoticeVersionPunctuation(t *testing.T) {
	for _, floor := range []bool{false, true} {
		for _, boolean := range []bool{false, true} {
			t.Run(fmt.Sprintf("floor-%t/bool-%t", floor, boolean), func(t *testing.T) {
				c, wire := setterClient(t, floor, "", "synthetic-notice-actor")
				notice := [3]string{"strict-fallback/1", "en", "policy+build/1"}
				got, _ := observeNotice(t, c, ConsentDecisionGranted, boolean, notice)
				checkSetterResult(t, got, "", "")
				if c.ConsentState() != ConsentGranted {
					t.Error("permitted punctuation refused grant")
				}
				if err := c.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				if wire.consentCount() != 1 {
					t.Fatalf("POST count=%d", wire.consentCount())
				}
				checkNoticeBody(t, wire.consentAt(0), &notice)
			})
		}
	}
}

func TestConsentNoticeInvalidActorWarnings(t *testing.T) {
	for _, floor := range []bool{false, true} {
		t.Run(fmt.Sprintf("floor-%t", floor), func(t *testing.T) {
			c, wire := setterClient(t, floor, "", strings.Repeat("x", 513))
			got, _ := observeNotice(t, c, ConsentDecisionDenied, false, [3]string{})
			if got.err != nil || c.ConsentState() != ConsentDenied {
				t.Fatalf("local denial lost: %+v", got)
			}
			for _, warning := range []string{"consent_actor_invalid", "consent_notice_invalid"} {
				if !slices.Contains(got.warnings, warning) {
					t.Errorf("combined warnings %v lack %s", got.warnings, warning)
				}
			}
			if err := c.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if wire.consentCount() != 0 {
				t.Errorf("invalid actor sent %d receipts", wire.consentCount())
			}
		})
	}
}
