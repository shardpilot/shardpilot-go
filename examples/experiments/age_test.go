package main

import (
	"os"
	"strings"
	"testing"
)

func TestExperimentAgeGoldenInCapture(t *testing.T) {
	app, env, exp := requestedAppKey, requestedEnvKey, requestedExpKey
	t.Cleanup(func() { requestedAppKey, requestedEnvKey, requestedExpKey = app, env, exp })
	requestedAppKey, requestedEnvKey, requestedExpKey = "exposure-app", "develop", "exposure-banner"
	for _, name := range []string{"adult", "undeclared", "unknown", "under_threshold"} {
		body, err := os.ReadFile("../../testdata/experiment-age/" + name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		if !sdkWouldParseAssignment(string(body)) {
			t.Errorf("capture rejected SDK server golden %s", name)
		}
		if name != "adult" && sdkWouldParseAssignment(strings.Replace(string(body), "age_ineligible", "future_age_reason", 1)) {
			t.Error("capture accepted an unknown reason")
		}
	}
	if !isSDKTaxonomy("age_ineligible") || !sdkReasonValues["age_ineligible"] {
		t.Error("capture taxonomy lost the authoritative age reason")
	}
}
