package shardpilot

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAPIResponseConfigBackoff(t *testing.T) {
	for _, status := range []int{0, 204, 408, 429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			var healthy atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if healthy.Load() {
					fmt.Fprint(w, `{"values":{"healthy":true}}`)
				} else if status == 0 {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err == nil {
						_ = conn.Close()
					}
				} else {
					w.WriteHeader(status)
				}
			}))
			defer server.Close()
			client := newRemoteConfigClient(t, server.URL, "", "synthetic")
			defer client.Close(context.Background())
			clock := &stubClock{now: time.Now()}
			client.clock = clock
			client.jitter = func() float64 { return 0.5 }
			for attempt, delay := range []time.Duration{time.Second, time.Second, 1500 * time.Millisecond, 2500 * time.Millisecond} {
				_, _ = client.FetchRemoteConfig(context.Background())
				client.rc.mu.Lock()
				until := client.rc.cooldownUntil
				client.rc.mu.Unlock()
				if got := until.Sub(clock.now); got != delay {
					t.Errorf("attempt %d: backoff=%v want %v", attempt+1, got, delay)
				}
				before := calls.Load()
				_, _ = client.FetchRemoteConfig(context.Background())
				if calls.Load() != before {
					t.Error("another request dispatched inside backoff")
				}
				clock.now = clock.now.Add(2 * time.Minute)
			}
			healthy.Store(true)
			if result, err := client.FetchRemoteConfig(context.Background()); err != nil || result.FromCache {
				t.Fatalf("recovery: %+v %v", result, err)
			}
			healthy.Store(false)
			_, _ = client.FetchRemoteConfig(context.Background())
			client.rc.mu.Lock()
			delay := client.rc.cooldownUntil.Sub(clock.now)
			client.rc.mu.Unlock()
			if delay != time.Second {
				t.Errorf("recovery did not reset the streak: %v", delay)
			}
		})
	}
}

func TestAPIResponseConfigPublicationWithdrawal(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		clears     bool
	}{
		{"absent", `{"error":{"code":"remote_config_not_published","message":"translated"}}`, true},
		{"case", `{"error":{"code":"REMOTE_CONFIG_NOT_PUBLISHED"}}`, false},
		{"message-only", `{"error":{"code":"unknown","message":"remote_config_not_published"}}`, false},
		{"legacy", `{"error":"published remote config not found"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var status atomic.Int32
			status.Store(200)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				code := int(status.Load())
				w.WriteHeader(code)
				if code == 200 {
					fmt.Fprint(w, `{"version":1,"values":{"example":true}}`)
				} else {
					fmt.Fprint(w, tc.body)
				}
			}))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "config.json")
			client := newRemoteConfigClient(t, server.URL, path, "synthetic")
			defer client.Close(context.Background())
			if _, err := client.FetchRemoteConfig(context.Background()); err != nil || !client.RemoteConfigBool("example", false) {
				t.Fatalf("initial control: %v", err)
			}
			status.Store(404)
			if _, err := client.FetchRemoteConfig(context.Background()); err == nil {
				t.Fatal("404 must fail closed")
			}
			if client.RemoteConfigBool("example", false) {
				t.Error("terminal getter stayed open")
			}
			restarted := newRemoteConfigClient(t, server.URL, path, "synthetic")
			defer restarted.Close(context.Background())
			if got := restarted.RemoteConfigBool("example", false); got == tc.clears {
				t.Errorf("restart served=%v, cache cleared=%v", got, tc.clears)
			}
			status.Store(503)
			result, err := client.FetchRemoteConfig(context.Background())
			if (err != nil) != tc.clears || result.FromCache == tc.clears {
				t.Errorf("explicit refetch: %+v err=%v; cleared=%v", result, err, tc.clears)
			}
		})
	}
}

func TestAPIResponseReadSuccessClasses(t *testing.T) {
	for _, status := range []int{200, 201, 299} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			rc, _, _, _, failure := applyRemoteConfig(nil, expTestResponse(status, `{"values":{"example":true}}`), 42)
			if failure != "" || rc.Values["example"] != true {
				t.Errorf("config: %+v %s", rc, failure)
			}
			exp, _, failure := applyExperimentAssignment(nil, expTestResponse(status, expAssignedBody("1")), expTestRequestScope(), 42)
			if failure != "" || !exp.Assigned {
				t.Errorf("assignment: %+v %s", exp, failure)
			}
			_, out, failure := applyExperimentAssignment(&expEntry{VariantKey: "control", Version: 1}, expTestResponse(status, `{`), expTestRequestScope(), 42)
			if !out.transient || failure != "" {
				t.Errorf("malformed read did not serve stale: %+v %s", out, failure)
			}
		})
	}
}

func TestAPIResponseApplySuccessClasses(t *testing.T) {
	for _, status := range []int{200, 201, 299} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			rig := newExpHopRig(t, func(cfg *Config) {
				cfg.HTTPClient = &http.Client{Transport: apiDecisionForwarder(func(r *http.Request) (*http.Response, error) {
					resp, err := http.DefaultTransport.RoundTrip(r)
					if err == nil && r.Method == "POST" && strings.Contains(r.URL.Path, "/runtime/experiments/") && resp.StatusCode == 200 {
						resp.StatusCode = status
					}
					return resp, err
				})}
			}, expAssignedBody("1"))
			fetchAssignment(t, rig.client, expTestScopeKey)
			rig.client.ApplyExperimentVariant(expTestScopeKey)
			rig.cycle()
			flushOrFail(t, rig.client)
			if rig.owed() != 0 || len(rig.capture.byName(experimentExposureName)) != 1 {
				t.Fatal("valid sealed exposure did not settle")
			}
			if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 42); err != nil {
				t.Fatal(err)
			}
			rig.cycle()
			flushOrFail(t, rig.client)
			if len(rig.capture.byName(experimentOutcomeName)) != 1 {
				t.Fatal("valid sealed outcome did not settle")
			}
		})
	}
}

func TestAPIResponsePublicationDurableFences(t *testing.T) {
	for _, mode := range []string{"same", "other-scope", "newer", "cold-newer", "write-failed"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			client := newRemoteConfigClient(t, "http://127.0.0.1:1", path, "synthetic")
			defer client.Close(context.Background())
			rc := client.rc
			scope := rc.anonymousScope
			_, served, _, _, _ := applyRemoteConfig(nil, expTestResponse(200, `{"version":1,"values":{"old":true}}`), 10)
			served.Scope = scope
			durable := *served
			if mode == "other-scope" {
				durable.Scope = rc.scope
			}
			if mode == "newer" || mode == "cold-newer" {
				durable.FetchedAtMS = 20
			}
			if err := rc.saveDurable(&durable); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			rc.held = served
			if mode == "cold-newer" {
				served = nil
				rc.held = nil
			}
			if mode == "write-failed" {
				if err := os.Chmod(filepath.Dir(path), 0500); err != nil {
					t.Fatal(err)
				}
				defer os.Chmod(filepath.Dir(path), 0700)
			}
			failed := rc.clearPublicationLocked(scope, served)
			if failed != (mode == "write-failed") {
				t.Fatalf("clear failure=%v for %s", failed, mode)
			}
			if rc.loadCacheLocked(scope) != nil {
				t.Fatal("withdrawn publication revived in memory")
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "same" {
				if string(after) != "{}" {
					t.Fatal("matching publication was not cleared")
				}
			} else if string(after) != string(before) {
				t.Fatal("unrelated/newer or unwritable durable record was changed")
			}
			if mode == "write-failed" {
				if err := os.Chmod(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				result, record, authoritative, renewed, failure := applyRemoteConfig(nil, expTestResponse(200, `{"version":2,"values":{"fresh":true}}`), 30)
				if failure != "" || rc.installLocked(1, scope, result, true, record, authoritative, nil, renewed) {
					t.Fatal("fresh recovery could not persist")
				}
				if !client.RemoteConfigBool("fresh", false) {
					t.Fatal("fresh recovery stayed closed after a failed clear")
				}
			}
		})
	}
}
