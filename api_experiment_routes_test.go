package shardpilot

import (
	"net/http"
	"strings"
	"sync"
	"testing"
)

type apiDecisionForwarder func(*http.Request) (*http.Response, error)

func (f apiDecisionForwarder) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAPIExperimentPublicRoutes(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	rig := newAppliedOutcomeRig(t, func(cfg *Config) {
		cfg.HTTPClient = &http.Client{Transport: apiDecisionForwarder(func(r *http.Request) (*http.Response, error) {
			if strings.Contains(r.URL.Path, "/runtime/experiments/") {
				mu.Lock()
				requests = append(requests, r.Method+" "+r.URL.Path)
				mu.Unlock()
			}
			return http.DefaultTransport.RoundTrip(r)
		})}
	})
	if err := rig.client.TrackExperimentOutcome(expTestScopeKey, "purchase", 42); err != nil {
		t.Fatal(err)
	}
	rig.cycle()
	flushOrFail(t, rig.client)
	if len(rig.capture.byName(experimentOutcomeName)) != 1 {
		t.Fatal("control: the real outcome must be sealed and delivered")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 3 {
		t.Fatalf("expected all three real calls, got %v", requests)
	}
	for _, want := range []string{
		"GET /api/cp/v1/runtime/experiments/assignment",
		"POST /api/cp/v1/runtime/experiments/exposures",
		"POST /api/cp/v1/runtime/experiments/outcomes",
	} {
		t.Run(want, func(t *testing.T) {
			count := 0
			for _, got := range requests {
				if got == want {
					count++
				}
			}
			if count != 1 {
				t.Errorf("public route %q seen %d times; actual calls: %v", want, count, requests)
			}
		})
	}
}
