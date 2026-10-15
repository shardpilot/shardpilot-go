package shardpilot

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type rcPolicyTransport struct {
	sequenceTransport
	fetch func(context.Context, remoteConfigRequest) (remoteConfigResponse, error)
}

func (p *rcPolicyTransport) FetchRemoteConfig(ctx context.Context, request remoteConfigRequest) (remoteConfigResponse, error) {
	return p.fetch(ctx, request)
}

func rcPolicyClient() (*Client, *expFakeClock, *rcPolicyTransport) {
	cfg := Config{AppURL: "https://config.example.invalid", WorkspaceKey: "workspace-test", EnvironmentKey: "develop", AnonymousID: "anon-rc-1", HTTPTimeout: time.Second}
	clock := &expFakeClock{now: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)}
	transport := &rcPolicyTransport{}
	client := &Client{cfg: cfg, rc: newRemoteConfigState(cfg), clock: clock, transport: transport}
	client.rc.held = &rcCache{Scope: client.rc.anonymousScope, ETag: "cached", Body: `{"value":"cached"}`}
	client.rc.snapshot = map[string]any{"value": "cached"}
	return client, clock, transport
}

func TestRemoteConfigPolicyNoLocalDelay(t *testing.T) {
	for _, status := range []int{0, 200, 408, 429, 503} {
		for _, hint := range []int{-1, 0, 10} {
			if hint > 0 && (status == 429 || status == 503) {
				continue // Positive server hints have their own scope/expiry scene.
			}
			t.Run(fmt.Sprintf("status_%d_hint_%d", status, hint), func(t *testing.T) {
				client, _, transport := rcPolicyClient()
				calls := 0
				transport.fetch = func(context.Context, remoteConfigRequest) (remoteConfigResponse, error) {
					calls++
					return remoteConfigResponse{status: status, body: []byte(`invalid`), retryAfterPresent: hint >= 0, retryAfterSeconds: hint}, nil
				}
				_, _ = client.FetchRemoteConfig(context.Background())
				_, _ = client.FetchRemoteConfig(context.Background())
				if calls != 2 {
					t.Fatalf("a missing/non-positive/inapplicable hint must not suppress an explicit fetch: %d calls", calls)
				}
			})
		}
	}
}

func TestRemoteConfigPolicyWindowsFollowScope(t *testing.T) {
	for _, start := range []int32{consentStateUnknown, consentStateGranted} {
		t.Run(fmt.Sprint(start), func(t *testing.T) {
			client, clock, transport := rcPolicyClient()
			client.consent.Store(start)
			calls := 0
			transport.fetch = func(context.Context, remoteConfigRequest) (remoteConfigResponse, error) {
				calls++
				if calls == 1 {
					return remoteConfigResponse{status: 429, retryAfterPresent: true, retryAfterSeconds: 10}, nil
				}
				return remoteConfigResponse{status: 200, body: []byte(`{"value":"fresh"}`)}, nil
			}
			_, _ = client.FetchRemoteConfig(context.Background())
			client.consent.Store(consentStateGranted - start)
			if _, err := client.FetchRemoteConfig(context.Background()); err != nil || calls != 2 {
				t.Fatalf("another consent scope must dispatch: calls=%d error=%v", calls, err)
			}
			client.consent.Store(start)
			_, _ = client.FetchRemoteConfig(context.Background())
			if calls != 2 {
				t.Fatal("returning to the original scope must retain its window")
			}
			clock.advance(10 * time.Second)
			_, _ = client.FetchRemoteConfig(context.Background())
			if calls != 3 {
				t.Fatal("the original window must expire at its deadline")
			}
		})
	}
}

func TestRemoteConfigPolicyTerminalRefetchStaysClosed(t *testing.T) {
	for _, terminal := range []int{301, 401, 403, 404, 413} {
		for _, fallback := range []int{0, 200, 408, 429, 503} {
			t.Run(fmt.Sprintf("terminal_%d_fallback_%d", terminal, fallback), func(t *testing.T) {
				client, clock, transport := rcPolicyClient()
				response := remoteConfigResponse{status: terminal}
				calls := 0
				transport.fetch = func(context.Context, remoteConfigRequest) (remoteConfigResponse, error) {
					calls++
					return response, nil
				}
				if _, err := client.FetchRemoteConfig(context.Background()); err == nil {
					t.Fatal("terminal response must fail")
				}
				response = remoteConfigResponse{status: fallback, body: []byte(`invalid`), retryAfterPresent: true, retryAfterSeconds: 10}
				for attempt := 0; attempt < 2; attempt++ {
					result, err := client.FetchRemoteConfig(context.Background())
					if err == nil || result.Values != nil || client.RemoteConfigString("value", "default") != "default" || client.RemoteConfigValues() != nil {
						t.Errorf("explicit/cached refetch reopened a terminal scope: result=%+v error=%v", result, err)
					}
				}
				if (fallback == 429 || fallback == 503) && calls != 2 {
					t.Errorf("closed scope must still honor its positive server window: %d calls", calls)
				}
				clock.advance(10 * time.Second)
				response = remoteConfigResponse{status: 304}
				if result, err := client.FetchRemoteConfig(context.Background()); err != nil || !result.FromCache || client.RemoteConfigString("value", "default") != "cached" {
					t.Fatalf("winning 304 must reopen retained valid cache: %+v %v", result, err)
				}
			})
		}
	}
}

func TestRemoteConfigPolicyStaleSuccessCannotReopen(t *testing.T) {
	for _, status := range []int{200, 304} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			client, _, transport := rcPolicyClient()
			started, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			transport.fetch = func(context.Context, remoteConfigRequest) (remoteConfigResponse, error) {
				switch calls.Add(1) {
				case 1:
					close(started)
					<-release
					return remoteConfigResponse{status: status, body: []byte(`{"value":"stale"}`)}, nil
				case 2:
					return remoteConfigResponse{status: 403}, nil
				default:
					return remoteConfigResponse{status: 200, body: []byte(`{"value":"fresh"}`)}, nil
				}
			}
			done := make(chan error, 1)
			go func() { _, err := client.FetchRemoteConfig(context.Background()); done <- err }()
			<-started
			_, _ = client.FetchRemoteConfig(context.Background())
			close(release)
			if err := <-done; err == nil || client.RemoteConfigString("value", "default") != "default" {
				t.Fatal("a fenced success must not return values or reopen a closed scope")
			}
			if _, err := client.FetchRemoteConfig(context.Background()); err != nil || client.RemoteConfigString("value", "default") != "fresh" {
				t.Fatal("a winning valid 200 must reopen the scope")
			}
		})
	}
}

func TestRemoteConfigPolicyClosureFollowsScope(t *testing.T) {
	client, _, transport := rcPolicyClient()
	response := remoteConfigResponse{status: 403}
	transport.fetch = func(context.Context, remoteConfigRequest) (remoteConfigResponse, error) { return response, nil }
	_, _ = client.FetchRemoteConfig(context.Background())
	client.consent.Store(consentStateGranted)
	response = remoteConfigResponse{status: 200, body: []byte(`{"version":7,"values":{"value":"identified"}}`)}
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatal(err)
	}
	client.consent.Store(consentStateUnknown)
	if value, ok := client.RemoteConfigValue("value"); ok || value != nil || client.RemoteConfigValues() != nil {
		t.Fatal("another scope's success must not reopen this closed scope's getters")
	}
	if _, ok := client.RemoteConfigVersion(); ok {
		t.Fatal("a closed scope must not expose a version")
	}
}

func TestRemoteConfigPolicyLateForeignScopeCannotSettle(t *testing.T) {
	for _, status := range []int{200, 304, 403, 429} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			client, _, transport := rcPolicyClient()
			started, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			transport.fetch = func(context.Context, remoteConfigRequest) (remoteConfigResponse, error) {
				if calls.Add(1) == 1 {
					close(started)
					<-release
					return remoteConfigResponse{status: status, body: []byte(`{"value":"old-scope"}`), retryAfterPresent: true, retryAfterSeconds: 10}, nil
				}
				return remoteConfigResponse{status: 200, body: []byte(`{"value":"current-scope"}`)}, nil
			}
			done := make(chan struct{})
			go func() { _, _ = client.FetchRemoteConfig(context.Background()); close(done) }()
			<-started
			client.consent.Store(consentStateGranted)
			_, _ = client.FetchRemoteConfig(context.Background())
			close(release)
			<-done
			if client.RemoteConfigString("value", "default") != "current-scope" {
				t.Fatal("a response for a departed scope changed current getters")
			}
			client.consent.Store(consentStateUnknown)
			_, _ = client.FetchRemoteConfig(context.Background())
			if calls.Load() != 3 {
				t.Fatal("a late response for a departed scope armed a retry window")
			}
		})
	}
}

func TestRemoteConfigPolicyRejectedInstallStaysClosed(t *testing.T) {
	client, _, transport := rcPolicyClient()
	client.rc.cachePath = t.TempDir() + "/config.json"
	transport.fetch = func(context.Context, remoteConfigRequest) (remoteConfigResponse, error) {
		return remoteConfigResponse{status: 403}, nil
	}
	_, _ = client.FetchRemoteConfig(context.Background())
	transport.fetch = func(context.Context, remoteConfigRequest) (remoteConfigResponse, error) {
		newer := &rcCache{Scope: client.rc.anonymousScope, ETag: "newer", Body: `{"value":"durable"}`, FetchedAtMS: 20}
		if err := client.rc.saveDurable(newer); err != nil {
			t.Fatal(err)
		}
		return remoteConfigResponse{status: 200, body: []byte(`{"value":"overtaken"}`)}, nil
	}
	if result, err := client.FetchRemoteConfig(context.Background()); err == nil || result.Values != nil || client.RemoteConfigValues() != nil {
		t.Fatal("a durable-fenced install must not reopen the scope or return values")
	}
	transport.fetch = func(_ context.Context, request remoteConfigRequest) (remoteConfigResponse, error) {
		if request.ifNoneMatch != "newer" {
			t.Fatal("next fetch did not select the newer durable validator")
		}
		return remoteConfigResponse{status: 304}, nil
	}
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil || client.RemoteConfigString("value", "default") != "durable" {
		t.Fatal("the newer durable record must recover through a winning 304")
	}
}

type rcPolicyClockFunc func() time.Time

func (f rcPolicyClockFunc) Now() time.Time { return f() }

func TestRemoteConfigPolicyWindowExpiresDuringLockContention(t *testing.T) {
	client, clock, transport := rcPolicyClient()
	client.rc.retryWindows = map[string]rcRetryWindow{client.rc.anonymousScope: {
		until: clock.Now().Add(time.Second), reason: "transient_503",
	}}
	var sampled atomic.Bool
	client.clock = rcPolicyClockFunc(func() time.Time {
		if sampled.CompareAndSwap(false, true) {
			if client.rc.mu.TryLock() {
				client.rc.mu.Unlock()
				// An outside-lock sample ages while another holder keeps the
				// mutex. Barriers make that elapsed time deterministic.
				held, advance, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
				go func() {
					client.rc.mu.Lock()
					close(held)
					<-advance
					clock.advance(2 * time.Second)
					client.rc.mu.Unlock()
					close(finished)
				}()
				<-held
				before := clock.Now()
				close(advance)
				<-finished
				return before
			}
			clock.advance(2 * time.Second)
		}
		return clock.Now()
	})
	calls := 0
	transport.fetch = func(context.Context, remoteConfigRequest) (remoteConfigResponse, error) {
		calls++
		return remoteConfigResponse{status: 200, body: []byte(`{"value":"fresh"}`)}, nil
	}
	result, err := client.FetchRemoteConfig(context.Background())
	if err != nil || calls != 1 || result.FromCache || result.Values["value"] != "fresh" {
		t.Fatalf("expired retry window suppressed dispatch: calls=%d result=%+v error=%v", calls, result, err)
	}
}
