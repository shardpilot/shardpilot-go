package shardpilot

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAPIResponseCodes(t *testing.T) {
	for _, tc := range []struct {
		code            string
		status          int
		remint, dropAll bool
		failure         string
	}{
		{"real_subjects_disabled", 403, false, true, "unauthorized"},
		{"subject_id_invalid", 400, true, false, "bad_request"},
		{"subject_pii_marker", 400, true, false, "bad_request"},
		{"subject_invalid_byte", 400, false, false, "bad_request"},
		{"subject_not_synthetic", 400, false, false, "bad_request"},
		{"definition_invalid", 500, false, false, "transient_500"},
		{"runtime_disabled", 403, false, false, "runtime_disabled"},
		{"experiments_disabled", 403, false, false, "experiments_disabled"},
		{"experiment_assignment_disabled", 403, false, false, "experiment_assignment_disabled"},
		{"future_code", 400, false, false, "bad_request"},
		{"SUBJECT_ID_INVALID", 400, false, false, "bad_request"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			for _, message := range []string{"translated detail", "experiment metadata must use synthetic local-safe identifiers only"} {
				body := fmt.Sprintf(`{"future":{},"error":{"code":%q,"message":%q,"future":true}}`, tc.code, message)
				_, out, failure := applyExperimentAssignment(nil, expTestResponse(tc.status, body), expTestRequestScope(), 42)
				if tc.status == 403 {
					want := "unauthorized"
					if tc.code == "runtime_disabled" {
						want = tc.code
					}
					_, _, _, _, got := applyRemoteConfig(nil, expTestResponse(tc.status, body), 42)
					if got != want {
						t.Errorf("config code %s: got %s want %s", tc.code, got, want)
					}
				}
				if out.remint != tc.remint || out.dropAll != tc.dropAll || failure != tc.failure {
					t.Fatalf("code=%s message=%s: %+v failure=%s", tc.code, message, out, failure)
				}
			}
		})
	}
	for _, body := range []string{`{"error":"experiment metadata must use synthetic local-safe identifiers only"}`, `<html>proxy</html>`, `{"error":{"code":"unknown","message":"experiment metadata must use synthetic local-safe identifiers only"}}`} {
		_, out, _ := applyExperimentAssignment(nil, expTestResponse(400, body), expTestRequestScope(), 42)
		if out.remint || !out.dropEntry {
			t.Fatalf("status-only fallback: %+v", out)
		}
	}
}

func TestAPIResponseAgeDetailCode(t *testing.T) {
	for _, tc := range []struct {
		body string
		want bool
	}{
		{`{"error":{"code":"not_assigned","message":"anything","details":[{"field":"reason","code":"age_ineligible","message":"anything","future":true}]}}`, true},
		{`{"error":{"code":"not_assigned","details":[{"field":"reason","code":"age_ineligible_extra"}]}}`, false},
		{`{"error":{"code":"unknown","details":[{"field":"reason","code":"age_ineligible"}]}}`, false},
		{`{"error":{"code":"not_assigned","details":[{"field":"other","code":"age_ineligible"}]}}`, false},
		{`{"error":"not_assigned","reason":"age_ineligible"}`, false},
	} {
		if got := experimentApplyAgeRefusal([]byte(tc.body), false); got != tc.want {
			t.Errorf("body=%s got %v want %v", tc.body, got, tc.want)
		}
	}
}

func TestAPIResponseUnknownAssignmentValues(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"reason", `{"assigned":false,"reason":"future_reason","future":{"nested":true}}`},
		{"unit", strings.Replace(expAssignedBody("3"), `"assignment_unit":"client_id"`, `"assignment_unit":"future_unit"`, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, out, ok := parseExperimentVerdict(expTestResponse(200, tc.body), expTestRequestScope(), 42)
			if !ok || result.Assigned || result.Reason != "unknown" || !out.dropEntry || out.newEntry != nil {
				t.Fatalf("ok=%v result=%+v outcome=%+v", ok, result, out)
			}
		})
	}
	for _, unit := range []string{"client_id", "synthetic_subject_key"} {
		for _, key := range []string{`"invalid-ignored"`, `null`, `true`, `17`, `[]`, `{}`} {
			t.Run(unit+"/"+key, func(t *testing.T) {
				body := strings.Replace(expAssignedBody("3"), `"assignment_unit":"client_id"`, `"assignment_unit":"`+unit+`"`, 1)
				body = strings.Replace(body, `"sfk1_`+strings.Repeat("a", 64)+`"`, key, 1)
				_, out, ok := parseExperimentVerdict(expTestResponse(200, body), expTestRequestScope(), 42)
				if unit == "client_id" {
					if ok {
						t.Fatal("invalid client fact key accepted")
					}
					return
				}
				if !ok || out.newEntry == nil || out.newEntry.SubjectFactKey != "" {
					t.Fatalf("synthetic fact key not ignored: ok=%v out=%+v", ok, out)
				}
			})
		}
	}
}

func TestAPIResponseApplyCodeActions(t *testing.T) {
	for _, tc := range []struct {
		code          string
		status        int
		keep, refetch bool
	}{
		{"experiment_apply_disabled", 403, true, false},
		{"unauthorized", 401, false, false},
		{"workspace_suspended", 403, false, false},
		{"unknown", 302, false, false},
		{"version_mismatch", 409, false, true},
		{"variant_mismatch", 409, false, true},
		{"unit_not_recordable", 409, false, true},
		{"unknown", 409, false, false},
		{"seal_unavailable", 503, true, false},
	} {
		for _, removed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cache-removed=%t", tc.code, removed), func(t *testing.T) {
				rig := newExpHopRig(t, nil, expAssignedBody("1"), `{"assigned":false}`)
				fetchAssignment(t, rig.client, expTestScopeKey)
				rig.client.ApplyExperimentVariant(expTestScopeKey)
				if removed {
					if _, err := rig.client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err != nil {
						t.Fatal(err)
					}
				}
				rig.script.apply.push(tc.status, fmt.Sprintf(`{"error":{"code":%q,"message":"translated detail"}}`, tc.code))
				before := rig.script.requestCount()
				rig.cycle()
				if got := rig.owed() > 0; got != tc.keep {
					t.Errorf("keep=%v want %v", got, tc.keep)
				}
				rig.cycle()
				if got := rig.script.requestCount() > before; got != tc.refetch {
					t.Errorf("assignment refetched=%v want %v", got, tc.refetch)
				}
			})
		}
	}
}

func TestAPIResponseHTTPClasses(t *testing.T) {
	for _, status := range []int{0, 301, 302, 307, 400, 401, 403, 404, 405, 408, 409, 413, 429, 500, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			retry := status == 0 || status == 408 || status == 429 || status >= 500
			cached := &expEntry{VariantKey: "control", Version: 1}
			result, out, _ := applyExperimentAssignment(cached, expTestResponse(status, `{"error":{"code":"future_code"}}`), expTestRequestScope(), 42)
			if result.FromCache != retry || out.transient != retry {
				t.Fatalf("status %d: result=%+v outcome=%+v", status, result, out)
			}
		})
	}
}

func TestAPIResponseConfigTerminalClosesGetters(t *testing.T) {
	for _, status := range []int{302, 401, 403, 404, 408, 429, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					fmt.Fprint(w, `{"version":1,"values":{"example":true}}`)
					return
				}
				w.WriteHeader(status)
				fmt.Fprint(w, `{"error":{"code":"future_code"}}`)
			}))
			defer server.Close()
			client := newRemoteConfigClient(t, server.URL, "", "synthetic")
			defer client.Close(context.Background())
			if _, err := client.FetchRemoteConfig(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, _ = client.FetchRemoteConfig(context.Background())
			retry := status == 408 || status == 429 || status >= 500
			if got := client.RemoteConfigBool("example", false); got != retry {
				t.Fatalf("getter=%v retryable=%v", got, retry)
			}
		})
	}
}

func TestAPIResponseConfigRetryAfter(t *testing.T) {
	for _, status := range []int{429, 503} {
		for _, header := range []string{"120", time.Now().UTC().Add(120 * time.Second).Format(http.TimeFormat)} {
			t.Run(fmt.Sprintf("%d/%s", status, header), func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.Header().Set("Retry-After", header)
					w.WriteHeader(status)
				}))
				defer server.Close()
				client := newRemoteConfigClient(t, server.URL, "", "synthetic")
				defer client.Close(context.Background())
				_, _ = client.FetchRemoteConfig(context.Background())
				client.rc.mu.Lock()
				until := client.rc.retryWindows[client.rc.anonymousScope].until
				client.rc.mu.Unlock()
				if time.Until(until) < 110*time.Second {
					t.Errorf("Retry-After floor missing: %v", until)
				}
				_, _ = client.FetchRemoteConfig(context.Background())
				if calls.Load() != 1 {
					t.Errorf("request inside server floor: %d", calls.Load())
				}
			})
		}
	}
}

func TestAPIResponseRetryAfterFloor(t *testing.T) {
	for _, apply := range []bool{false, true} {
		t.Run(fmt.Sprint(apply), func(t *testing.T) {
			rig := newExpHopRig(t, nil, expAssignedBody("1"))
			fetchAssignment(t, rig.client, expTestScopeKey)
			deadline := time.Now().UTC().Add(120 * time.Second).Truncate(time.Second)
			if apply {
				rig.client.ApplyExperimentVariant(expTestScopeKey)
				rig.script.apply.pushRetryAfter(503, `{"error":{"code":"seal_unavailable"}}`, deadline.Format(http.TimeFormat))
				rig.cycle()
			} else {
				rig.script.mu.Lock()
				rig.script.responses = nil // replace the fixture's repeating last success
				rig.script.mu.Unlock()
				rig.script.pushRetryAfter(503, `{"error":{"code":"definition_invalid"}}`, deadline.Format(http.TimeFormat))
				result, err := rig.client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
				if err != nil || !result.FromCache {
					t.Fatalf("503 fixture did not serve stale: result=%+v err=%v", result, err)
				}
			}
			rig.client.exp.mu.Lock()
			until := rig.client.exp.retryAfterMS
			rig.client.exp.mu.Unlock()
			if until < deadline.UnixMilli() {
				t.Fatalf("retry deadline %d precedes server floor %d", until, deadline.UnixMilli())
			}
		})
	}
}

func TestAPIResponseConflictNeedsNewerFetch(t *testing.T) {
	rig := newExpHopRig(t, nil, expAssignedBody("1"), expAssignedBody("2"), expAssignedBody("3"))
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	gate := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(gate)
		}
	}()
	rig.script.mu.Lock()
	rig.script.gates = map[int]chan struct{}{1: gate}
	rig.script.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		result, err := rig.client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil)
		if err == nil && result.Version != 2 {
			err = fmt.Errorf("old fetch returned version %d, want 2", result.Version)
		}
		done <- err
	}()
	waitFor(t, 5*time.Second, "the earlier fetch reached the server", func() bool { return rig.script.requestCount() == 2 })
	rig.script.apply.push(409, `{"error":{"code":"version_mismatch"}}`)
	rig.cycle()
	rig.client.exp.mu.Lock()
	queued := rig.client.exp.refetchAfterApply[expTestScopeKey] != nil
	rig.client.exp.mu.Unlock()
	if !queued || rig.owed() != 0 {
		t.Fatal("control: conflict must drop the fact and queue its refetch")
	}
	close(gate)
	released = true
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	rig.cycle()
	if got := rig.script.requestCount(); got != 3 {
		t.Fatalf("a pre-conflict GET consumed the newer refetch: requests=%d, want 3", got)
	}
	rig.client.exp.mu.Lock()
	queued = rig.client.exp.refetchAfterApply[expTestScopeKey] != nil
	rig.client.exp.mu.Unlock()
	if queued {
		t.Fatal("a post-conflict authoritative GET must consume its ticket")
	}
}

func TestAPIResponseConflictRefetchWithdrawal(t *testing.T) {
	rig := newExpHopRig(t, nil, expAssignedBody("1"), `{"assigned":false}`, expAssignedBody("2"))
	fetchAssignment(t, rig.client, expTestScopeKey)
	rig.client.ApplyExperimentVariant(expTestScopeKey)
	rig.script.apply.push(409, `{"error":{"code":"version_mismatch"}}`)
	rig.cycle()
	rig.client.exp.mu.Lock()
	ticket := rig.client.exp.refetchAfterApply[expTestScopeKey]
	rig.client.exp.mu.Unlock()
	if ticket == nil {
		t.Fatal("conflict did not schedule a refetch")
	}
	// The host settles a newer withdrawal after the worker captured its ticket.
	if result, err := rig.client.FetchExperimentAssignment(context.Background(), expTestScopeKey, nil); err != nil || result.Assigned {
		t.Fatalf("withdrawal: result=%+v err=%v", result, err)
	}
	before := rig.script.requestCount()
	attrs := append(make([]expAttribute, 0, len(ticket.Attributes)), ticket.Attributes...)
	_, _ = rig.client.fetchExperimentAssignment(context.Background(), expTestScopeKey, nil, true, attrs, ticket)
	if got := rig.script.requestCount(); got != before {
		t.Fatalf("stale conflict refetch dispatched: before=%d after=%d", before, got)
	}
	if rig.client.ExperimentVariant(expTestScopeKey) != "" {
		t.Fatal("stale conflict refetch restored a withdrawn assignment")
	}
}
