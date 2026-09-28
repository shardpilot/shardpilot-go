package shardpilot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const ageGoldenExperiment = "exposure-banner"

func ageGolden(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "experiment-age", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func ageClient(t *testing.T, url, spool string) *Client {
	t.Helper()
	return newExperimentClient(t, url, func(cfg *Config) {
		cfg.AppID = "exposure-app"
		cfg.SpoolDir = spool
	})
}

// Server sources: experiment_age_eligibility_routes_test.go:36 (refusals),
// experiment_fact_apply_routes_test.go:219 (adult assignment). See the fixture
// contract for the capture provenance and the public fixture projection.
func TestExperimentAgeServerGolden(t *testing.T) {
	for _, band := range []string{"undeclared", "unknown", "under_threshold"} {
		t.Run("experiment_age_eligibility_routes_test.go:36/"+band, func(t *testing.T) {
			script := &expScript{}
			script.push(200, ageGolden(t, "adult"))
			script.push(200, ageGolden(t, band))
			server := newExperimentServer(t, script, &expWireCapture{})
			defer server.Close()
			spool := t.TempDir()
			client := ageClient(t, server.URL, spool)
			defer client.Close(context.Background())
			clock := &expFakeClock{now: time.Now()}
			client.clock = clock
			result, err := client.FetchExperimentAssignment(context.Background(), ageGoldenExperiment, map[string]string{"age_band": "adult"})
			if err != nil || !result.Assigned || result.VariantKey != "control" || result.FromCache {
				t.Fatalf("adult positive control: result=%+v err=%v", result, err)
			}
			attrs := map[string]string{}
			if band != "undeclared" {
				attrs["age_band"] = band
			}
			result, err = client.FetchExperimentAssignment(context.Background(), ageGoldenExperiment, attrs)
			if band == "undeclared" && script.request(1).URL.Query().Has("age_band") {
				t.Error("legacy fetch invented an age declaration")
			}
			if err != nil || result.Assigned || result.FromCache || result.Code != "" || result.Reason != "age_ineligible" {
				t.Errorf("authoritative age refusal: result=%+v err=%v", result, err)
			}
			if got := client.ExperimentVariant(ageGoldenExperiment); got != "" {
				t.Errorf("age refusal retained stale variant %q", got)
			}
			clock.advance(10 * time.Minute)
			client.experimentCycle(context.Background())
			if got := script.requestCount(); got != 2 {
				t.Errorf("age refusal must not schedule a retry: %d requests", got)
			}
			if err := client.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			restarted := ageClient(t, server.URL, spool)
			defer restarted.Close(context.Background())
			if got := restarted.ExperimentVariant(ageGoldenExperiment); got != "" {
				t.Errorf("age refusal retained durable assignment %q", got)
			}
		})
	}
}

func TestExperimentAgeBandAPI(t *testing.T) {
	for _, band := range []ExperimentAgeBand{ExperimentAgeBandUnknown, ExperimentAgeBandUnderThreshold, ExperimentAgeBandAdult} {
		t.Run(string(band), func(t *testing.T) {
			script := &expScript{}
			script.push(200, ageGolden(t, string(band)))
			server := newExperimentServer(t, script, &expWireCapture{})
			defer server.Close()
			client := ageClient(t, server.URL, t.TempDir())
			defer client.Close(context.Background())
			attrs := map[string]string{"geo": "DE"}
			result, err := client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, band, attrs)
			if err != nil || result.Assigned != (band == ExperimentAgeBandAdult) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if band != ExperimentAgeBandAdult && result.Reason != "age_ineligible" {
				t.Fatalf("age refusal lost its reason: %+v", result)
			}
			if got := script.request(0).URL.Query().Get("age_band"); got != string(band) {
				t.Fatalf("age_band=%q, want %q", got, band)
			}
			if len(attrs) != 1 || attrs["geo"] != "DE" {
				t.Fatalf("caller attributes mutated: %v", attrs)
			}
			if band != ExperimentAgeBandAdult {
				return
			}
			// Restore the durable assignment and drive its automatic fetch.
			if err := client.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			restarted := ageClient(t, server.URL, client.cfg.SpoolDir)
			defer restarted.Close(context.Background())
			clock := &expFakeClock{now: time.Now()}
			restarted.clock = clock
			if restarted.ExperimentVariant(ageGoldenExperiment) != "control" {
				t.Fatal("positive control: durable adult assignment did not load")
			}
			restarted.experimentCycle(context.Background()) // Arm the restored cadence.
			clock.advance(10 * time.Minute)
			restarted.experimentCycle(context.Background())
			if script.requestCount() != 2 || script.request(1).URL.Query().Get("age_band") != "adult" {
				t.Fatal("durable revalidation lost the declared adult band")
			}
		})
	}
}

func TestExperimentAgeBandAPIRejectsInvalidAndConflictingDeclarations(t *testing.T) {
	script := &expScript{}
	script.push(200, ageGolden(t, "under_threshold"))
	server := newExperimentServer(t, script, &expWireCapture{})
	defer server.Close()
	client := ageClient(t, server.URL, "")
	defer client.Close(context.Background())
	for _, band := range []ExperimentAgeBand{"", "ADULT", " adult ", "minor", "child", "teen", "unknown_future_band"} {
		if _, err := client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, band, nil); !errors.Is(err, ErrInvalidExperimentAgeBand) {
			t.Errorf("invalid band %q: %v", band, err)
		}
	}
	if _, err := client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, map[string]string{"age_band": "under_threshold"}); !errors.Is(err, ErrInvalidExperimentAgeBand) {
		t.Errorf("conflicting declaration: %v", err)
	}
	if script.requestCount() != 0 {
		t.Fatalf("invalid declarations reached the server: %d", script.requestCount())
	}
	// A legacy alias still refuses admission beside a typed adult declaration.
	result, err := client.FetchExperimentAssignmentWithAgeBand(context.Background(), ageGoldenExperiment, ExperimentAgeBandAdult, map[string]string{"custom_attribute_age_band": "under_threshold"})
	if err != nil || result.Assigned || result.Reason != "age_ineligible" || script.request(0).URL.Query().Get("custom_attribute_age_band") != "under_threshold" {
		t.Fatalf("conflicting alias was promoted: result=%+v err=%v", result, err)
	}
}

func TestExperimentAgeUnknownReasonRemainsMalformed(t *testing.T) {
	script := &expScript{}
	script.push(200, ageGolden(t, "adult"))
	script.push(200, strings.Replace(ageGolden(t, "undeclared"), `"age_ineligible"`, `"future_age_reason"`, 1))
	server := newExperimentServer(t, script, &expWireCapture{})
	defer server.Close()
	client := ageClient(t, server.URL, "")
	defer client.Close(context.Background())
	attrs := map[string]string{"age_band": "adult"}
	for i := 0; i < 2; i++ {
		result, err := client.FetchExperimentAssignment(context.Background(), ageGoldenExperiment, attrs)
		if err != nil || !result.Assigned || result.FromCache != (i == 1) {
			t.Fatalf("fetch %d: result=%+v err=%v", i, result, err)
		}
		if i == 1 && result.Code != "malformed_response" {
			t.Fatalf("unknown reason must remain malformed: %+v", result)
		}
	}
}

func TestExperimentAgeAttributesPreserveEligibility(t *testing.T) {
	for _, band := range []string{"adult", "unknown", "under_threshold", "", " adult ", "ADULT", strings.Repeat("x", 513)} {
		label := fmt.Sprintf("%q", band)
		if len(band) > 512 {
			label = "oversized"
		}
		for _, name := range []string{"age_band", "custom_attribute_age_band"} {
			t.Run(name+"/"+label, func(t *testing.T) {
				script := &expScript{}
				script.push(200, ageGolden(t, "undeclared"))
				server := newExperimentServer(t, script, &expWireCapture{})
				defer server.Close()
				client := ageClient(t, server.URL, "")
				defer client.Close(context.Background())
				attrs := map[string]string{"age_band": "adult", "custom_attribute_age_band": "adult"}
				attrs[name] = band
				// An attribute flood must not discard either age declaration.
				for i := 0; i < 70; i++ {
					attrs[fmt.Sprintf("custom_attribute_0%02d", i)] = "v"
				}
				_, _ = client.FetchExperimentAssignment(context.Background(), ageGoldenExperiment, attrs)
				if script.requestCount() != 1 {
					t.Fatal("real fetch did not reach the HTTP fixture")
				}
				query := script.request(0).URL.Query()
				want := band
				if len(want) > 512 {
					want = "unknown"
				}
				if !query.Has(name) || query.Get(name) != want {
					t.Errorf("age declaration changed or dropped: %s=%q, want %q (present=%v)", name, query.Get(name), want, query.Has(name))
				}
				other := "age_band"
				if name == other {
					other = "custom_attribute_age_band"
				}
				if query.Get(other) != "adult" || len(query) != 68 {
					t.Errorf("reserve both declarations within 64 attributes: other=%q query count=%d", query.Get(other), len(query))
				}
			})
		}
	}
}
