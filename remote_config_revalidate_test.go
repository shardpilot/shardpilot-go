package shardpilot

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseCacheControlMaxAge(t *testing.T) {
	cases := []struct {
		header  string
		seconds int
		present bool
	}{
		{"private, max-age=300", 300, true},
		{"max-age=0", 0, true},
		{"MAX-AGE=60", 60, true},
		{"private,max-age=60,stale-while-revalidate=30", 60, true},
		{"  max-age = 45 ", 45, true},
		{"", 0, false},
		{"no-store", 0, false},
		{"private", 0, false},
		{"max-age=abc", 0, false},
		{"max-age=-1", 0, false},
		{"max-age=1.5", 0, false},
		{"max-age=", 0, false},
		{"max-age=99999999999999999999", rcCooldownClampSeconds, true},
		{"max-age=" + "9999999", rcCooldownClampSeconds, true}, // above the 24h clamp
	}
	for _, tc := range cases {
		seconds, present := parseCacheControlMaxAge(tc.header)
		if seconds != tc.seconds || present != tc.present {
			t.Fatalf("parseCacheControlMaxAge(%q) = (%d, %v), want (%d, %v)", tc.header, seconds, present, tc.seconds, tc.present)
		}
	}
}

func TestRemoteConfigRevalidateDelayAnchors(t *testing.T) {
	rc := &remoteConfigState{}

	// Before any Cache-Control is seen the anchor falls back to 300s.
	if got := rc.revalidateDelay(0); got != rcRevalidateFallback {
		t.Fatalf("expected the 300s fallback anchor, got %v", got)
	}
	// The server's advertised max-age anchors the schedule.
	rc.maxAgeSeconds, rc.maxAgePresent = 120, true
	if got := rc.revalidateDelay(0); got != 120*time.Second {
		t.Fatalf("expected the server max-age anchor, got %v", got)
	}
	// The 60s floor respects the server-side fetch rate limiter.
	rc.maxAgeSeconds = 10
	if got := rc.revalidateDelay(0); got != rcRevalidateFloor {
		t.Fatalf("expected the 60s floor, got %v", got)
	}
	// A configured interval can slow the timer down…
	rc.maxAgeSeconds = 120
	if got := rc.revalidateDelay(10 * time.Minute); got != 10*time.Minute {
		t.Fatalf("expected the slower configured interval honored, got %v", got)
	}
	// …but never drive it faster than the anchor.
	if got := rc.revalidateDelay(30 * time.Second); got != 120*time.Second {
		t.Fatalf("expected the anchor to floor a faster configured interval, got %v", got)
	}
}

func TestRemoteConfigFetchCapturesMaxAge(t *testing.T) {
	var mode atomic.Int64 // 0 = 200 with max-age 120, 1 = 304 with max-age 45
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode.Load() == 1 {
			w.Header().Set("Cache-Control", "private, max-age=45")
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Cache-Control", "private, max-age=120")
		_, _ = w.Write([]byte(`{"version":1,"values":{"k":"v"}}`))
	}))
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())

	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("fresh fetch: %v", err)
	}
	client.rc.mu.Lock()
	seconds, present := client.rc.maxAgeSeconds, client.rc.maxAgePresent
	client.rc.mu.Unlock()
	if !present || seconds != 120 {
		t.Fatalf("expected max-age 120 captured from the 200, got (%d, %v)", seconds, present)
	}

	// A 304 revalidation's Cache-Control updates the anchor too.
	mode.Store(1)
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("revalidation fetch: %v", err)
	}
	client.rc.mu.Lock()
	seconds, present = client.rc.maxAgeSeconds, client.rc.maxAgePresent
	client.rc.mu.Unlock()
	if !present || seconds != 45 {
		t.Fatalf("expected max-age 45 captured from the 304, got (%d, %v)", seconds, present)
	}
	if got := client.rc.revalidateDelay(0); got != rcRevalidateFloor {
		t.Fatalf("expected the 60s floor over the 45s max-age, got %v", got)
	}
}

func TestRemoteConfigRevalidationTickConditionalGetKeepsSchedule(t *testing.T) {
	var ifNoneMatch atomic.Value
	var mode atomic.Int64 // 0 = 200, 1 = 304, 2 = 500
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch mode.Load() {
		case 1:
			ifNoneMatch.Store(r.Header.Get("If-None-Match"))
			w.WriteHeader(http.StatusNotModified)
		case 2:
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.Header().Set("ETag", `"v7"`)
			_, _ = w.Write([]byte(`{"version":1,"values":{"k":"v"}}`))
		}
	}))
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}

	// A timer tick is the same conditional GET an explicit fetch performs:
	// the cached ETag rides If-None-Match and a 304 keeps the schedule.
	mode.Store(1)
	if exit := client.revalidateRemoteConfigOnce(); exit {
		t.Fatal("a 304 revalidation must keep the timer running")
	}
	if got := ifNoneMatch.Load(); got != `"v7"` {
		t.Fatalf("expected the tick to revalidate with If-None-Match %q, got %q", `"v7"`, got)
	}

	// A transient failure keeps the schedule too (the durable cache
	// serves; the next cycle re-asks).
	mode.Store(2)
	if exit := client.revalidateRemoteConfigOnce(); exit {
		t.Fatal("a transient failure must keep the timer running")
	}
	if client.rc.autoLaneHalted() {
		t.Fatal("a transient failure must not halt the timer")
	}
}

func TestRemoteConfigRevalidationTickRespectsCooldown(t *testing.T) {
	var requests atomic.Int64
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"version":1,"values":{"k":"v"}}`, headers: map[string]string{"ETag": `"v1"`}})
	server := newRCScriptServer(t, &requests, &script)
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}

	// A live 429 arms the cooldown…
	script.Store(rcScriptStep{status: 429, body: ``, headers: map[string]string{"Retry-After": "60"}})
	if result, err := client.FetchRemoteConfig(context.Background()); err != nil || result.Reason != "transient_429" {
		t.Fatalf("expected the cache-served transient_429, got %+v %v", result, err)
	}
	before := requests.Load()

	// …and a timer tick inside the armed window performs NO network call:
	// the cache-served transient_429 outcome, schedule unchanged.
	if exit := client.revalidateRemoteConfigOnce(); exit {
		t.Fatal("a cooldown tick must keep the timer running")
	}
	if got := requests.Load(); got != before {
		t.Fatalf("a tick inside the cooldown must not touch the network, saw %d then %d requests", before, got)
	}
	if client.rc.autoLaneHalted() {
		t.Fatal("a cooldown tick must not halt the timer")
	}
}

func TestRemoteConfigRevalidationHaltsAfterUnauthorizedUntilReinit(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"version":1,"values":{"k":"v"}}`, headers: map[string]string{"ETag": `"v1"`}})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}

	// The TIMER's own authoritative 401/403 halts it until re-init.
	for _, status := range []int{401, 403} {
		freshClient := newRemoteConfigClient(t, server.URL, "", "anon-rc-halt")
		script.Store(rcScriptStep{status: status, body: `{"error":"nope"}`})
		if exit := freshClient.revalidateRemoteConfigOnce(); !exit {
			t.Fatalf("expected the timer to exit on %d", status)
		}
		if !freshClient.rc.autoLaneHalted() {
			t.Fatalf("expected the halt flag set on %d", status)
		}
		// A halted timer stays halted: the next cycle exits immediately.
		script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"v"}}`})
		if exit := freshClient.revalidateRemoteConfigOnce(); !exit {
			t.Fatal("a halted timer must not resume on its own")
		}
		// Explicit fetches keep classifying per-fetch — the halt is the
		// TIMER's, never the host's.
		if result, err := freshClient.FetchRemoteConfig(context.Background()); err != nil || result.Values["k"] != "v" {
			t.Fatalf("expected the explicit fetch to work after the halt, got %+v %v", result, err)
		}
		if !freshClient.rc.autoLaneHalted() {
			t.Fatal("an explicit fetch success must not resume the halted timer")
		}
		if err := freshClient.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

func TestRemoteConfigRevalidationLoopRunsThenHalts(t *testing.T) {
	var requests atomic.Int64
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"version":1,"values":{"k":"v"}}`, headers: map[string]string{"ETag": `"v1"`}})
	server := newRCScriptServer(t, &requests, &script)
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	primed := requests.Load()

	// Start the production loop with a shrunk cycle delay (the same seam
	// pattern as the injected jitter).
	client.rc.revalidateDelayFn = func() time.Duration { return time.Millisecond }
	client.rcRevalidateDone = make(chan struct{})
	go client.runRemoteConfigRevalidation()

	waitFor(t, 5*time.Second, "periodic remote-config revalidation", func() bool {
		return requests.Load() >= primed+2
	})

	// An authoritative 401 received by the TIMER halts the loop.
	script.Store(rcScriptStep{status: 401, body: `{"error":"nope"}`})
	select {
	case <-client.rcRevalidateDone:
	case <-time.After(5 * time.Second):
		t.Fatal("expected the revalidation timer to halt after an authoritative 401")
	}
	halted := requests.Load()
	time.Sleep(30 * time.Millisecond)
	if got := requests.Load(); got != halted {
		t.Fatalf("the halted timer must schedule no further fetches, saw %d then %d", halted, got)
	}

	// Explicit fetches keep working; their success does not resume the
	// timer.
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"v2"}}`})
	if result, err := client.FetchRemoteConfig(context.Background()); err != nil || result.Values["k"] != "v2" {
		t.Fatalf("expected the explicit fetch to classify per-fetch after the halt, got %+v %v", result, err)
	}
	afterHost := requests.Load()
	time.Sleep(30 * time.Millisecond)
	if got := requests.Load(); got != afterHost {
		t.Fatalf("an explicit fetch success must not resume the halted timer, saw %d then %d", afterHost, got)
	}
}

func TestRemoteConfigRevalidationReArmsOnShorterCadence(t *testing.T) {
	// A pending long timer must not wait out its old schedule after a
	// fetch observes a SHORTER server cadence: the observed change nudges
	// the loop and the next tick is re-armed to the new, sooner deadline.
	var requests atomic.Int64
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"v"}}`, headers: map[string]string{"ETag": `"v1"`, "Cache-Control": "private, max-age=60"}})
	server := newRCScriptServer(t, &requests, &script)
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())

	// The seam models the anchor change: the initial arm reads a LONG
	// cycle, every re-evaluation after the nudge a short one.
	var delayCalls atomic.Int64
	client.rc.revalidateDelayFn = func() time.Duration {
		if delayCalls.Add(1) == 1 {
			return time.Hour
		}
		return 5 * time.Millisecond
	}
	client.rcRevalidateDone = make(chan struct{})
	go client.runRemoteConfigRevalidation()

	// The explicit fetch observes the server cadence (a CHANGE from
	// nothing-yet-seen) and nudges the pending hour-long timer…
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("explicit fetch: %v", err)
	}
	// …which must re-arm to the shorter deadline and tick well before the
	// old schedule.
	waitFor(t, 5*time.Second, "re-armed revalidation tick", func() bool {
		return requests.Load() >= 2
	})
}

func TestRemoteConfigCadenceChangeNudgesRecalc(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"v"}}`, headers: map[string]string{"Cache-Control": "private, max-age=120"}})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())

	// A fetch that observes a CHANGED max-age parks one recalc nudge.
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	select {
	case <-client.rc.revalidateRecalc:
	default:
		t.Fatal("expected the observed cadence change to park a recalc nudge")
	}

	// An UNCHANGED max-age nudges nothing (no recalc churn per tick).
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	select {
	case <-client.rc.revalidateRecalc:
		t.Fatal("an unchanged cadence must not nudge the recalc channel")
	default:
	}
}

func TestRemoteConfigCadenceOnlyFromUsableOutcomes(t *testing.T) {
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"v"}}`, headers: map[string]string{"ETag": `"v1"`, "Cache-Control": "private, max-age=60"}})
	server := newRCScriptServer(t, nil, &script)
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("priming fetch: %v", err)
	}
	anchored := func(want time.Duration, what string) {
		t.Helper()
		if got := client.rc.revalidateDelay(0); got != want {
			t.Fatalf("%s: expected the %v anchor, got %v", what, want, got)
		}
	}
	anchored(60*time.Second, "after the priming 200")

	// A TRANSIENT failure carrying Cache-Control must not move the anchor —
	// the cadence updates from usable outcomes only.
	script.Store(rcScriptStep{status: 500, body: `oops`, headers: map[string]string{"Cache-Control": "private, max-age=86400"}})
	if result, err := client.FetchRemoteConfig(context.Background()); err != nil || result.Reason != "transient_500" {
		t.Fatalf("expected the cache-served transient_500, got %+v %v", result, err)
	}
	anchored(60*time.Second, "after a transient with an incidental max-age")

	// An authoritative REFUSAL carrying Cache-Control moves nothing either.
	script.Store(rcScriptStep{status: 401, body: `{"error":"nope"}`, headers: map[string]string{"Cache-Control": "private, max-age=86400"}})
	if _, err := client.FetchRemoteConfig(context.Background()); err == nil || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("expected unauthorized, got %v", err)
	}
	anchored(60*time.Second, "after an unauthorized with an incidental max-age")

	// And a usable success WITHOUT the header restores the default anchor:
	// the server stopped advertising a cadence, so the stale 60s must not
	// keep governing the timer.
	script.Store(rcScriptStep{status: 304, body: ``})
	if result, err := client.FetchRemoteConfig(context.Background()); err != nil || !result.FromCache || result.Reason != "" {
		t.Fatalf("expected a clean 304 revalidation, got %+v %v", result, err)
	}
	client.rc.mu.Lock()
	present := client.rc.maxAgePresent
	client.rc.mu.Unlock()
	if present {
		t.Fatal("expected the cadence observation cleared by a usable success without Cache-Control")
	}
	anchored(rcRevalidateFallback, "after a 304 without Cache-Control")
}

func TestRemoteConfigRevalidationStaleRefusalDoesNotHalt(t *testing.T) {
	// A delayed 401 that LOST the per-scope fence to a newer settled 200 is
	// old news: it fails its own fetch closed, but the unattended timer
	// must not halt on it. A fresh (fence-winning) refusal still halts.
	var requests atomic.Int64
	firstArrived := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			firstArrived <- struct{}{}
			<-releaseFirst
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"nope"}`))
		case 2:
			_, _ = w.Write([]byte(`{"values":{"k":"fresh"}}`))
		default:
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"nope"}`))
		}
	}))
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())

	// The timer's tick dispatches and stalls inside the server…
	tickExit := make(chan bool, 1)
	go func() {
		tickExit <- client.revalidateRemoteConfigOnce()
	}()
	<-firstArrived
	// …an explicit fetch settles a fresh 200 FIRST…
	if result, err := client.FetchRemoteConfig(context.Background()); err != nil || result.Values["k"] != "fresh" {
		t.Fatalf("newer fetch: %+v %v", result, err)
	}
	// …then the stalled 401 lands, having lost the fence: no halt.
	close(releaseFirst)
	if exit := <-tickExit; exit {
		t.Fatal("a stale refusal that lost the fence must not halt the timer")
	}
	if client.rc.autoLaneHalted() {
		t.Fatal("expected the timer not halted by the stale refusal")
	}

	// The NEXT tick's refusal wins its fence and halts as designed.
	if exit := client.revalidateRemoteConfigOnce(); !exit {
		t.Fatal("expected the fence-winning refusal to halt the timer")
	}
	if !client.rc.autoLaneHalted() {
		t.Fatal("expected the halt flag set by the fence-winning refusal")
	}
}

func TestRemoteConfigStaleLateResponseDoesNotOverwriteCadence(t *testing.T) {
	// Overlapping fetches: an older response landing AFTER a newer fetch
	// settled must not overwrite the revalidation cadence — the max-age
	// store sits behind the same per-scope sequence fence as installs and
	// the 429 cooldown.
	var requests atomic.Int64
	firstArrived := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			firstArrived <- struct{}{}
			<-releaseFirst
			// The STALE cadence: an outdated 24h max-age.
			w.Header().Set("Cache-Control", "private, max-age=86400")
			_, _ = w.Write([]byte(`{"values":{"k":"stale"}}`))
			return
		}
		w.Header().Set("Cache-Control", "private, max-age=60")
		_, _ = w.Write([]byte(`{"values":{"k":"fresh"}}`))
	}))
	defer server.Close()

	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	defer client.Close(context.Background())

	// Fetch 1 dispatches and stalls inside the server...
	firstDone := make(chan error, 1)
	go func() {
		_, fetchErr := client.FetchRemoteConfig(context.Background())
		firstDone <- fetchErr
	}()
	<-firstArrived
	// ...fetch 2 dispatches later and settles a fresh 200 FIRST, with the
	// server's CURRENT 60s cadence...
	if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
		t.Fatalf("newer fetch: %v", err)
	}
	// ...then the stalled response lands with its outdated 24h max-age.
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("stale fetch: %v", err)
	}

	client.rc.mu.Lock()
	seconds, present := client.rc.maxAgeSeconds, client.rc.maxAgePresent
	client.rc.mu.Unlock()
	if !present || seconds != 60 {
		t.Fatalf("expected the settled fetch's 60s cadence kept over the stale 24h one, got (%d, %v)", seconds, present)
	}
	if got := client.rc.revalidateDelay(0); got != 60*time.Second {
		t.Fatalf("expected the revalidation schedule anchored at 60s, got %v", got)
	}
}

func TestRemoteConfigRevalidationDefaultOffAndCloseStops(t *testing.T) {
	var requests atomic.Int64
	var script atomic.Value
	script.Store(rcScriptStep{status: 200, body: `{"values":{"k":"v"}}`})
	server := newRCScriptServer(t, &requests, &script)
	defer server.Close()

	// DEFAULT OFF: no interval, no timer goroutine, no background fetches —
	// the documented explicit-fetch-only behavior, byte-for-byte.
	client := newRemoteConfigClient(t, server.URL, "", "anon-rc-1")
	if client.rcRevalidateDone != nil {
		t.Fatal("expected no revalidation timer without the opt-in interval")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("expected zero background fetches by default, saw %d", got)
	}

	// Opted in: the timer goroutine exists and Close stops it promptly
	// (first cycle delay is the 300s fallback anchor — Close must not wait
	// it out).
	optedIn, err := NewClient(Config{
		IngestURL:                      server.URL,
		Token:                          "test-token",
		WorkspaceID:                    "workspace-test",
		AppID:                          "app-test",
		EnvironmentID:                  "develop",
		Source:                         SourceBackend,
		AnonymousID:                    "anon-rc-1",
		APIKey:                         "test-rc-key",
		RemoteConfigURL:                server.URL,
		RemoteConfigRevalidateInterval: time.Hour,
		FlushInterval:                  time.Hour,
		HTTPTimeout:                    time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if optedIn.rcRevalidateDone == nil {
		t.Fatal("expected the revalidation timer goroutine with the opt-in interval")
	}
	start := time.Now()
	if err := optedIn.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Close must stop the timer promptly, took %v", elapsed)
	}
	select {
	case <-optedIn.rcRevalidateDone:
	case <-time.After(5 * time.Second):
		t.Fatal("expected Close to stop the revalidation timer")
	}
}

func TestRemoteConfigRevalidationIntervalIgnoredWithoutURL(t *testing.T) {
	// The revalidation knobs are dependent opt-ins: without their primary
	// URL they configure nothing (the same posture as
	// RemoteConfigCachePath without RemoteConfigURL).
	client, err := NewClient(Config{
		IngestURL:                              "http://localhost:8080",
		Token:                                  "test-token",
		WorkspaceID:                            "workspace-test",
		AppID:                                  "app-test",
		EnvironmentID:                          "develop",
		Source:                                 SourceBackend,
		RemoteConfigRevalidateInterval:         time.Minute,
		ExperimentAssignmentRevalidateInterval: time.Minute,
		FlushInterval:                          time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close(context.Background())
	if client.rcRevalidateDone != nil || client.expRevalidateDone != nil {
		t.Fatal("expected no revalidation goroutines without the corresponding URLs")
	}
	if _, err := client.FetchRemoteConfig(context.Background()); err == nil || !strings.Contains(err.Error(), "remote_config_not_configured") {
		t.Fatalf("expected remote_config_not_configured, got %v", err)
	}
}
