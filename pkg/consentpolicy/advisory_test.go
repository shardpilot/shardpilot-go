package consentpolicy

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// advisoryPlan is THE RESOLVER'S OWN ADVISORY RESPONSE, mutated, for the same
// reason validPlan is the resolved one: a fixture written here would agree
// with this parser about a shape the server may never send. Only expires_at
// is refreshed by default.
func advisoryPlan(mutate func(plan, advisory map[string]any)) []byte {
	raw, err := os.ReadFile(filepath.Join("testdata", "golden", "consent-policy-resolved-advisory.json"))
	if err != nil {
		panic(err)
	}
	var plan map[string]any
	if err := json.Unmarshal(raw, &plan); err != nil {
		panic(err)
	}
	plan["expires_at"] = time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339)
	if mutate != nil {
		advisory, _ := plan["advisory"].(map[string]any)
		mutate(plan, advisory)
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		panic(err)
	}
	return encoded
}

func mustParse(t *testing.T, raw []byte) Plan {
	t.Helper()
	plan, err := ParsePlan(raw)
	if err != nil {
		t.Fatalf("the body must parse: %v", err)
	}
	return plan
}

// The recorded advisory body parses, and EVERY member arrives with the value
// the resolver sent — not merely "an advisory is present".
func TestTheAdvisoryGoldenCarriesWhatTheResolverSent(t *testing.T) {
	for _, name := range []string{"consent-policy-resolved-advisory", "consent-policy-resolved-advisory.indented"} {
		plan := mustParse(t, goldenWire(t, name))
		a := plan.Advisory
		if a == nil {
			t.Fatalf("%s: the advisory part did not arrive", name)
		}
		if a.Jurisdiction != "GB" || a.RowID != "GB" {
			t.Fatalf("%s: jurisdiction=%q row_id=%q", name, a.Jurisdiction, a.RowID)
		}
		if a.Estimate == nil || *a.Estimate != AdvisoryEstimateSoftOptOut {
			t.Fatalf("%s: estimate=%v, want SOFT_OPT_OUT", name, a.Estimate)
		}
		if a.RowStatus != AdvisoryRowCounselPending || a.RowBasis != AdvisoryRowBasisAIDraft {
			t.Fatalf("%s: row_status=%q row_basis=%q", name, a.RowStatus, a.RowBasis)
		}
		if a.ResolvedBy != AdvisoryResolvedByServerCountry {
			t.Fatalf("%s: resolved_by=%q", name, a.ResolvedBy)
		}
		if !strings.HasPrefix(a.AdvisoryBasis, "`medium` · contested") {
			t.Fatalf("%s: the basis was not carried verbatim: %q", name, a.AdvisoryBasis)
		}
		if a.Matrix.DocsCommit != "f6b6f0f617d4e15442608fc77be8a5bb40f40e26" ||
			a.Matrix.FileSHA256 != "370b04f374d0c506b92a003d4c801f450d5e5c45aed369f14a1c9382e3d592cc" ||
			a.Matrix.Date != "2026-10-07" {
			t.Fatalf("%s: matrix=%+v", name, a.Matrix)
		}
		// The plan beside it is the strict plan, unchanged.
		if plan.Regime != StrictOptIn {
			t.Fatalf("%s: regime=%q beside a SOFT_OPT_OUT estimate", name, plan.Regime)
		}
	}
}

// ⚠ THE ADVISORY IS OPTIONAL. A plan without it — every version-1 response,
// and every version-2 response the resolver did not admit — parses exactly as
// before, with no advisory.
func TestAPlanWithoutTheAdvisoryHasNone(t *testing.T) {
	plan := mustParse(t, goldenWire(t, "consent-policy-resolved"))
	if plan.Advisory != nil {
		t.Fatalf("an advisory appeared from nowhere: %+v", plan.Advisory)
	}
	stripped := mustParse(t, advisoryPlan(func(plan, _ map[string]any) { delete(plan, "advisory") }))
	if stripped.Advisory != nil {
		t.Fatal("deleting the advisory left one behind")
	}
}

// ⚠ IT NEVER CHANGES THE REGIME, OR ANYTHING A CALLER COULD BRANCH ON.
//
// Asked of decisionFromPlan, the mapping a verified plan would go through,
// because Prepare uses no plan in this release and so cannot be the witness:
// every input there refuses, and a scene through it would pass however the
// mapping read the advisory. The same plan with and without its advisory part
// must give the same answer to every question a Decision answers.
func TestTheAdvisoryNeverChangesTheVerdict(t *testing.T) {
	for _, estimate := range []any{"SOFT_OPT_OUT", "STRICT_OPT_IN", nil} {
		withIt := mustParse(t, advisoryPlan(func(_, advisory map[string]any) { advisory["estimate"] = estimate }))
		without := mustParse(t, advisoryPlan(func(plan, _ map[string]any) { delete(plan, "advisory") }))
		a, b := decisionFromPlan(withIt), decisionFromPlan(without)
		if a.Regime() != StrictOptIn || a.Regime() != b.Regime() {
			t.Fatalf("estimate %v: regime=%q, without the advisory %q", estimate, a.Regime(), b.Regime())
		}
		if a.AnalyticsChoiceDefault() != b.AnalyticsChoiceDefault() ||
			a.ExplicitGrantRequired() != b.ExplicitGrantRequired() ||
			a.ServerAnalytics() != b.ServerAnalytics() ||
			a.ChildRulesApplied() != b.ChildRulesApplied() {
			t.Fatalf("estimate %v: the advisory changed a purpose answer", estimate)
		}
		crashA, okA := a.CrashProfileOffered()
		crashB, okB := b.CrashProfileOffered()
		if crashA != crashB || okA != okB {
			t.Fatalf("estimate %v: the advisory changed the crash profile", estimate)
		}
		if a.AnalyticsChoiceDefault() != ChoiceDefaultOff || !a.ExplicitGrantRequired() {
			t.Fatalf("estimate %v: a SOFT estimate opened the analytics choice", estimate)
		}
	}
}

// And the release posture holds: an advisory does not open the gate. The
// recorded body, live, still stops at the missing signature.
func TestTheAdvisoryDoesNotOpenTheGate(t *testing.T) {
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: advisoryPlan(nil), Scope: callerScope(), Now: fixedClock(),
	})
	if decision.Reason != ReasonPlanUnsigned || decision.PlanUsed() || decision.Regime() != StrictOptIn {
		t.Fatalf("reason=%q used=%v regime=%q detail=%q",
			decision.Reason, decision.PlanUsed(), decision.Regime(), decision.Detail)
	}
}

// Every advisory key the resolver sends is required: deleting one makes the
// plan unreadable. Enumerated from the recorded bytes, so a key added to the
// contract is covered the moment the golden is re-recorded.
func TestEveryRecordedAdvisoryKeyIsRequired(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(goldenWire(t, "consent-policy-resolved-advisory"), &body); err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, path := range goldenKeyPaths(body, "") {
		if strings.HasPrefix(path, "advisory.") {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	for _, needed := range []string{"advisory.estimate", "advisory.matrix", "advisory.matrix.date", "advisory.resolved_by"} {
		if !contains(paths, needed) {
			t.Fatalf("the enumeration missed %q; it found %v", needed, paths)
		}
	}
	if len(paths) != 11 {
		t.Fatalf("expected the 8 advisory members and 3 matrix members, found %d: %v", len(paths), paths)
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			raw := advisoryPlan(func(plan, _ map[string]any) {
				if !deleteAtPath(plan, path) {
					t.Fatalf("could not delete %q", path)
				}
			})
			if _, err := ParsePlan(raw); err == nil {
				t.Fatalf("a plan missing %q parsed", path)
			}
		})
	}
}

// ⚠ ONE ADVISORY KEY MAY BE NULL, AND THE ADVISORY ITSELF IS NOT IT. The
// resolver omits an advisory it does not serve; it never sends null for one.
// `estimate` is null where the row carries none.
func TestOnlyTheEstimateMayBeNullInTheAdvisory(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(goldenWire(t, "consent-policy-resolved-advisory"), &body); err != nil {
		t.Fatal(err)
	}
	paths := []string{"advisory"}
	for _, path := range goldenKeyPaths(body, "") {
		if strings.HasPrefix(path, "advisory.") {
			paths = append(paths, path)
		}
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			raw := advisoryPlan(func(plan, _ map[string]any) {
				if !setAtPath(plan, path, nil) {
					t.Fatalf("could not set %q", path)
				}
			})
			_, err := ParsePlan(raw)
			if path == "advisory.estimate" {
				if err != nil {
					t.Fatalf("a null estimate is the contract's own spelling: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("a null %q parsed", path)
			}
		})
	}
}

// The shapes the contract permits, beyond the recorded one, all parse.
func TestTheAdvisoryShapesTheContractPermits(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(advisory map[string]any)
	}{
		{"an unresolved connection: OTHER, no estimate, unknown", func(a map[string]any) {
			a["jurisdiction"], a["row_id"], a["estimate"], a["resolved_by"] = "OTHER", "OTHER", nil, "unknown"
		}},
		{"a located country without a row of its own: OTHER, server_country", func(a map[string]any) {
			a["jurisdiction"], a["row_id"], a["estimate"] = "OTHER", "OTHER", nil
		}},
		{"a row that carries no estimate", func(a map[string]any) {
			a["jurisdiction"], a["row_id"], a["estimate"] = "TD", "TD", nil
		}},
		{"a STRICT_OPT_IN estimate", func(a map[string]any) { a["estimate"] = "STRICT_OPT_IN" }},
		{"a basis at exactly the bound", func(a map[string]any) {
			a["advisory_basis"] = strings.Repeat("a", maxAdvisoryBasisBytes)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mustParse(t, advisoryPlan(func(_, advisory map[string]any) { c.mutate(advisory) }))
		})
	}
}

// ⚠ AND EVERY SHAPE OUTSIDE IT MAKES THE PLAN UNREADABLE. One member wrong is
// enough: a plan this package cannot fully read is a plan it cannot act on,
// and "keep the parts I understand" is how a permissive reading gets in.
func TestTheAdvisoryVocabularyIsClosed(t *testing.T) {
	set := func(key string, value any) func(map[string]any) {
		return func(a map[string]any) { a[key] = value }
	}
	setMatrix := func(key string, value any) func(map[string]any) {
		return func(a map[string]any) { a["matrix"].(map[string]any)[key] = value }
	}
	cases := []struct {
		name   string
		mutate func(advisory map[string]any)
	}{
		{"jurisdiction in lower case", set("jurisdiction", "gb")},
		{"jurisdiction of three letters", set("jurisdiction", "GBR")},
		{"jurisdiction empty", set("jurisdiction", "")},
		{"jurisdiction OTHERS", set("jurisdiction", "OTHERS")},
		{"row_id with a digit", set("row_id", "G1")},
		{"estimate outside the vocabulary", set("estimate", "UNKNOWN")},
		{"estimate in lower case", set("estimate", "soft_opt_out")},
		{"estimate as a number", set("estimate", 1)},
		{"row_status claims review", set("row_status", "REVIEWED")},
		{"row_basis claims acceptance", set("row_basis", "owner_accepted")},
		{"resolved_by outside the vocabulary", set("resolved_by", "geoip")},
		{"basis empty", set("advisory_basis", "")},
		{"basis one byte over the bound", set("advisory_basis", strings.Repeat("a", maxAdvisoryBasisBytes+1))},
		{"basis with a newline", set("advisory_basis", "estimate\nforged: everything is fine")},
		{"basis with a tab", set("advisory_basis", "a\tb")},
		{"OTHER with an estimate", func(a map[string]any) {
			a["jurisdiction"], a["row_id"], a["estimate"] = "OTHER", "OTHER", "SOFT_OPT_OUT"
		}},
		{"an unresolved connection that names a country", set("resolved_by", "unknown")},
		{"docs_commit in upper case", setMatrix("docs_commit", strings.ToUpper("f6b6f0f617d4e15442608fc77be8a5bb40f40e26"))},
		{"docs_commit one character short", setMatrix("docs_commit", "f6b6f0f617d4e15442608fc77be8a5bb40f40e2")},
		{"file_sha256 one character short", setMatrix("file_sha256", "370b04f374d0c506b92a003d4c801f450d5e5c45aed369f14a1c9382e3d592c")},
		{"date without leading zeros", setMatrix("date", "2026-10-7")},
		{"date that does not exist", setMatrix("date", "2026-02-30")},
		{"date as a timestamp", setMatrix("date", "2026-10-07T00:00:00Z")},
		{"an unknown advisory member", set("note", "x")},
		{"an unknown matrix member", setMatrix("branch", "main")},
		{"the advisory as a string", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw := advisoryPlan(func(plan, advisory map[string]any) {
				if c.mutate == nil {
					plan["advisory"] = "SOFT_OPT_OUT"
					return
				}
				c.mutate(advisory)
			})
			if _, err := ParsePlan(raw); err == nil {
				t.Fatal("parsed")
			}
		})
	}
}

// A member in another spelling, or twice, is refused at every depth: the key
// walk reaches inside the advisory as it does inside flags.
func TestTheAdvisoryKeysAreExactAndSingle(t *testing.T) {
	valid := string(advisoryPlan(nil))
	for name, body := range map[string]string{
		"a case variant of estimate": strings.Replace(valid, `"estimate":`, `"Estimate":`, 1),
		"estimate twice":             strings.Replace(valid, `"estimate":`, `"estimate":"STRICT_OPT_IN","estimate":`, 1),
		"advisory twice":             strings.Replace(valid, `"advisory":`, `"advisory":{},"advisory":`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if body == valid {
				t.Fatal("the mutation did not apply")
			}
			if _, err := ParsePlan([]byte(body)); err == nil {
				t.Fatal("parsed")
			}
		})
	}
}

// The resolver never serves the advisory part on a refusal.
func TestARefusalCarryingAnAdvisoryIsUnreadable(t *testing.T) {
	var refusal map[string]any
	if err := json.Unmarshal(goldenWire(t, "consent-policy-refusal"), &refusal); err != nil {
		t.Fatal(err)
	}
	var advisoryBody map[string]any
	if err := json.Unmarshal(goldenWire(t, "consent-policy-resolved-advisory"), &advisoryBody); err != nil {
		t.Fatal(err)
	}
	if _, err := ParsePlan(goldenWire(t, "consent-policy-refusal")); err != nil {
		t.Fatalf("the control: the refusal itself must parse: %v", err)
	}
	refusal["advisory"] = advisoryBody["advisory"]
	encoded, err := json.Marshal(refusal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParsePlan(encoded); err == nil {
		t.Fatal("a refusal carrying an advisory parsed")
	}
}

// ⚠ A COPY OF THE VERDICT CANNOT REACH BACK INTO IT THROUGH THE ADVISORY. The
// advisory is a pointer, and so is its estimate: copying the plan struct
// copies the addresses, not what they point at.
func TestTheAdvisoryIsCopiedOutOfTheVerdict(t *testing.T) {
	decision := decisionFromPlan(mustParse(t, advisoryPlan(nil)))
	first, ok := decision.Plan()
	if !ok || first.Advisory == nil || first.Advisory.Estimate == nil {
		t.Fatal("the verdict's plan carries no advisory")
	}
	*first.Advisory.Estimate = AdvisoryEstimateStrictOptIn
	first.Advisory.Jurisdiction = "FR"
	first.Advisory.Matrix.Date = "1970-01-01"
	second, _ := decision.Plan()
	if *second.Advisory.Estimate != AdvisoryEstimateSoftOptOut || second.Advisory.Jurisdiction != "GB" ||
		second.Advisory.Matrix.Date != "2026-10-07" {
		t.Fatalf("a caller wrote through a copy into the verdict: %+v", *second.Advisory)
	}
}

// ⚠ AND A CLONED ADVISORY MARSHALS BACK TO THE RESOLVER'S OWN BYTES. The
// advisory is compared as bytes, so a renamed tag, a dropped member or a
// rewritten null cannot pass by meaning the same thing.
func TestACloneMarshalsTheAdvisoryBackByteForByte(t *testing.T) {
	raw := goldenWire(t, "consent-policy-resolved-advisory")
	var recorded map[string]json.RawMessage
	if err := json.Unmarshal(raw, &recorded); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(clonePlan(mustParse(t, raw)))
	if err != nil {
		t.Fatal(err)
	}
	var round map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &round); err != nil {
		t.Fatal(err)
	}
	if string(round["advisory"]) != string(recorded["advisory"]) {
		t.Fatalf("the advisory did not marshal back to the recorded bytes\n  got:  %s\n  want: %s",
			round["advisory"], recorded["advisory"])
	}
	// A plan without one marshals without one: absent, never null.
	encoded, err = json.Marshal(clonePlan(mustParse(t, goldenWire(t, "consent-policy-resolved"))))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `"advisory"`) {
		t.Fatalf("a plan without an advisory marshalled one: %s", encoded)
	}
}
