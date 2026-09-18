package consentpolicy

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ⚠ RULE (a): PER ACTOR, NEVER PER PROCESS. A verdict cached across calls would
// authorize a different player than the events carry — and the contract names
// the shared-client case explicitly, so this is the realistic arrangement
// rather than a contrived one.
func TestAVerdictIsNeverReusedForAnotherActor(t *testing.T) {
	first := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(nil), Scope: callerScope(), Now: fixedClock(),
	})
	if !first.PlanUsed {
		t.Fatalf("the control must use its plan: %+v", first)
	}
	// The SECOND actor in the same process hands in NO plan. If anything were
	// memoised, this would inherit the first actor's verdict.
	second := Prepare(context.Background(), VerifiedPlayerPolicy{Scope: callerScope(), Now: fixedClock()})
	if second.PlanUsed || second.Reason != ReasonPlanAbsent {
		t.Fatalf("a second actor inherited a verdict: %+v", second)
	}
	if !second.OptionalProcessingClosed {
		t.Fatal("the second actor must be closed, whatever the first one got")
	}
	// And the reverse order, so the scene cannot pass because the cache only
	// fills on the second call.
	third := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) { m["regime"] = string(Unknown) }), Scope: callerScope(), Now: fixedClock(),
	})
	if third.Regime != Unknown {
		t.Fatalf("the third actor got %q, not its own plan's regime", third.Regime)
	}
}

// ⚠ RULE (b): A PLAN IS NOT A GRANT. A notice-and-objection outcome needs its
// own distinct admission-basis representation; setting an analytics consent
// boolean to stand in for one would record a grant nobody gave. So this package
// must expose no route from a verdict to a consent state, and must not reach
// into the telemetry client at all.
func TestTheDecisionCannotBecomeAConsentGrant(t *testing.T) {
	// The package's own source is the evidence: it imports nothing from the
	// telemetry root package, so there is no SetConsent to call.
	// ⚠ ASSERTED AGAINST CODE, NOT PROSE, and the first cut got this wrong:
	// it searched for the bare word "SetConsent" and fired on doc.go's own
	// comment EXPLAINING that the package must never call it. A check that
	// fails on the sentence forbidding the thing is a check about text. So:
	// imports are read from the import block, and the call is matched as a
	// call.
	for _, file := range packageSources(t) {
		for _, line := range strings.Split(file.body, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if strings.Contains(trimmed, "shardpilot-go\"") || strings.Contains(trimmed, "shardpilot-go/pkg/") {
				t.Errorf("%s imports the telemetry client: %q — policy selection is not processing admission",
					file.name, trimmed)
			}
			for _, forbidden := range []string{".SetConsent(", ".SetConsentDecision(", ".Consent()"} {
				if strings.Contains(trimmed, forbidden) {
					t.Errorf("%s calls %s: %q — a plan must never be convertible to a consent grant",
						file.name, forbidden, trimmed)
				}
			}
		}
	}
	// ⚠ AND THE SOFT PLAN THAT USED TO PROVE THIS IS NOW REFUSED OUTRIGHT,
	// which is the stronger answer: an UNSIGNED plan carrying SOFT is not
	// honoured at all in this release, so there is no verdict to map to a
	// grant in the first place.
	refused := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) { m["regime"] = string(SoftOptOut) }), Scope: callerScope(), Now: fixedClock(),
	})
	if refused.PlanUsed || refused.Reason != ReasonUnsignedPermissive {
		t.Fatalf("an unsigned SOFT plan must not be used: %+v", refused)
	}
	// The contract that "false is not permission" is asked of the TYPE, since
	// the verifier no longer produces such a verdict without a signature. A
	// decision built with a SOFT plan still reports only a regime, and its
	// analytics helper still keys off the plan being used.
	soft := Decision{Regime: SoftOptOut, OptionalProcessingClosed: false, PlanUsed: true,
		plan: Plan{Regime: SoftOptOut}}
	if soft.AnalyticsClosed() {
		t.Fatal("with a used SOFT plan the regime is not what closes the door")
	}
	zero := Decision{}
	if !zero.AnalyticsClosed() || !zero.CrashClosed() {
		t.Fatal("a zero Decision must be closed on every lane")
	}
}

// ⚠ RULE (c): NO TRUSTED CLOCK → STRICT. "Assume fresh" is the tempting
// shortcut and the wrong one: a verifier that cannot tell whether the plan
// expired has not verified it.
func TestNoTrustedClockIsStrict(t *testing.T) {
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{Plan: validPlan(nil), Scope: callerScope()})
	if decision.Reason != ReasonNoTrustedClock || decision.Regime != StrictOptIn {
		t.Fatalf("%+v", decision)
	}
	if decision.PlanUsed {
		t.Fatal("a plan whose freshness could not be checked was reported as used")
	}
}

// ⚠ RULE (d): AN UNVERIFIABLE SIGNATURE IS STRICT. The resolver's initial
// release reserves the field and sends none; when one arrives, a build that
// cannot check it must not treat its arrival as a downgrade by ignoring it.
func TestAnUnverifiableSignatureIsStrict(t *testing.T) {
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan:  validPlan(func(m map[string]any) { m["signature"] = "ed25519:synthetic" }),
		Scope: callerScope(), Now: fixedClock(),
	})
	if decision.Reason != ReasonSignatureUnverified || decision.Regime != StrictOptIn {
		t.Fatalf("%+v", decision)
	}
	// The control: the same plan WITHOUT a signature is used, so the scene is
	// about the signature rather than about the fixture.
	if !Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(nil), Scope: callerScope(), Now: fixedClock(),
	}).PlanUsed {
		t.Fatal("the unsigned control must be used")
	}
}

// The crash lane does not inherit the analytics answer in either direction —
// the crash profile has its own rule, and the platform decision makes it off
// by default.
func TestTheCrashLaneIsItsOwnDecision(t *testing.T) {
	// ⚠ AN UNSIGNED PLAN MAY NOT PERMIT THE CRASH LANE AT ALL in this release,
	// so the end-to-end question is now "is it refused?" — and it is.
	refused := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan:  validPlan(func(m map[string]any) { m["crash_profile"] = string(CrashMinimal) }),
		Scope: callerScope(), Now: fixedClock(),
	})
	if refused.PlanUsed || refused.Reason != ReasonUnsignedPermissive {
		t.Fatalf("an unsigned crash-minimal plan must not be used: %+v", refused)
	}
	if !refused.CrashClosed() || !refused.AnalyticsClosed() {
		t.Fatal("a refused plan closes both lanes")
	}

	// The INDEPENDENCE of the two lanes is a property of the helpers, and it is
	// asked of them directly — it becomes reachable end to end in the release
	// that verifies signatures, and this is where a regression would show
	// before then.
	strictWithCrashOn := Decision{Regime: StrictOptIn, OptionalProcessingClosed: true, PlanUsed: true,
		plan: Plan{Regime: StrictOptIn, CrashProfile: CrashMinimal}}
	if !strictWithCrashOn.AnalyticsClosed() {
		t.Fatal("analytics is still closed under STRICT")
	}
	if strictWithCrashOn.CrashClosed() {
		t.Fatal("a permitted crash profile is not closed by analytics being off")
	}
	softWithCrashOff := Decision{Regime: SoftOptOut, OptionalProcessingClosed: false, PlanUsed: true,
		plan: Plan{Regime: SoftOptOut, CrashProfile: CrashOff}}
	if !softWithCrashOff.CrashClosed() {
		t.Fatal("an OFF crash profile is not opened by the analytics regime")
	}
}

// The objection route is manual and there is no in-game toggle, so
// the SDK reports a basis and a requirement and can neither record nor satisfy
// it. A fallback is DENIED with the requirement standing.
func TestServerAnalyticsIsABasisNotAToggle(t *testing.T) {
	// Eligible is permissive, so an UNSIGNED plan carrying it is refused.
	refused := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) {
			m["server_analytics"] = string(ServerAnalyticsEligible)
		}),
		Scope: callerScope(), Now: fixedClock(),
	})
	if refused.PlanUsed || refused.Reason != ReasonUnsignedPermissive {
		t.Fatalf("an unsigned eligible plan must not be used: %+v", refused)
	}

	// The basis/requirement SHAPE, asked of the helper: there is no toggle, and
	// a fallback is DENIED with the requirement standing.
	required := true
	eligible := Decision{PlanUsed: true, plan: Plan{
		ServerAnalytics: ServerAnalyticsEligible, ObjectionRequired: &required}}
	state, objection := eligible.ServerAnalyticsBasis()
	if state != ServerAnalyticsEligible || !objection {
		t.Fatalf("state=%q objection=%v", state, objection)
	}
	state, objection = Prepare(context.Background(), VerifiedPlayerPolicy{Scope: callerScope(), Now: fixedClock()}).ServerAnalyticsBasis()
	if state != ServerAnalyticsDenied || !objection {
		t.Fatalf("a fallback must be DENIED with the requirement standing, got %q/%v", state, objection)
	}
	// An absent requirement is not a false one: a plan that never stated it
	// does not reach a verdict at all, and the helper is closed regardless.
	unstated := Decision{PlanUsed: true, plan: Plan{ServerAnalytics: ServerAnalyticsEligible}}
	if _, objection := unstated.ServerAnalyticsBasis(); !objection {
		t.Fatal("an unstated objection requirement must read as required")
	}
}

// The lists are copied out, so a caller cannot mutate the verdict it was given.
func TestTheVerdictsListsAreCopies(t *testing.T) {
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) {
			m["operation_blocks"] = []any{"transfer_review"}
			m["prohibited_purposes"] = []any{"advertising"}
		}),
		Scope: callerScope(), Now: fixedClock(),
	})
	blocks := decision.OperationBlocks()
	if len(blocks) != 1 {
		t.Fatalf("blocks=%v", blocks)
	}
	blocks[0] = "removed"
	if decision.OperationBlocks()[0] != "transfer_review" {
		t.Fatal("a caller mutated the verdict's own list")
	}
}

type sourceFile struct {
	name string
	body string
}

// packageSources DISCOVERS this package's non-test Go files, so a rule about
// what the package may not do is checked against the source rather than
// trusted.
//
// ⚠ IT USED TO HARD-CODE FOUR NAMES, which is the shape of guard that stops
// guarding the day somebody adds a fifth file — the new file would be the one
// place the rule did not reach, and nothing would say so. The list is read
// from the directory now, and the scene below asserts the count matches the
// listing so an empty glob cannot pass as "nothing to check".
func packageSources(t *testing.T) []sourceFile {
	t.Helper()
	matches, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list the package's files: %v", err)
	}
	files := make([]sourceFile, 0, len(matches))
	for _, name := range matches {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := readFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		files = append(files, sourceFile{name: name, body: body})
	}
	if len(files) == 0 {
		t.Fatal("no non-test source files were discovered; a guard that scans nothing passes everything")
	}
	return files
}

// The guard's own reach, asserted: the discovered set must be exactly the
// package's non-test files on disk. A glob that silently matched fewer would
// leave a file unguarded and still report success.
func TestTheConsentGuardReachesEveryPackageFile(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	onDisk := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		onDisk[name] = true
	}
	discovered := map[string]bool{}
	for _, file := range packageSources(t) {
		discovered[file.name] = true
	}
	if len(onDisk) != len(discovered) {
		t.Fatalf("the guard reads %d file(s) and the package has %d: %v vs %v",
			len(discovered), len(onDisk), discovered, onDisk)
	}
	for name := range onDisk {
		if !discovered[name] {
			t.Errorf("%s is in the package and outside the guard's reach", name)
		}
	}
}

// A plan the parser accepts must round-trip the fields the caller reads, or the
// verdicts above are reporting the fixture rather than the plan.
func TestParsedFieldsReachTheVerdict(t *testing.T) {
	raw := validPlan(func(m map[string]any) {
		m["presented_language"] = "en-GB"
		m["consent_text_version"] = "ff-v1.4"
	})
	var asMap map[string]any
	if err := json.Unmarshal(raw, &asMap); err != nil {
		t.Fatal(err)
	}
	plan, err := ParsePlan(raw)
	if err != nil {
		t.Fatalf("the fixture must parse: %v", err)
	}
	if plan.PresentedLanguage != "en-GB" || plan.ConsentTextVersion != "ff-v1.4" {
		t.Fatalf("%+v", plan)
	}
	if _, err := plan.expiry(); err != nil {
		t.Fatalf("the fixture's expiry must parse: %v", err)
	}
	if plan.ExpiresAt == "" || !strings.Contains(plan.ExpiresAt, "T") {
		t.Fatalf("expires_at=%q", plan.ExpiresAt)
	}
	_ = time.Now
}

// ⚠ AN UNSIGNED PLAN MAY CARRY ONLY THE CONSERVATIVE TUPLE, and the safety
// argument depends on it. "A forged plan can only tighten" is true of a forged
// STRICT plan and says nothing about a forged PERMISSIVE one; every field used
// to be trusted individually, so anyone who could put bytes in front of this
// verifier could hand it SOFT, a permitted crash profile or an eligible
// server-analytics basis and have each honoured on its own.
func TestAnUnsignedPlanMayNotBePermissive(t *testing.T) {
	permissive := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"regime", func(m map[string]any) { m["regime"] = string(SoftOptOut) }},
		{"crash profile", func(m map[string]any) { m["crash_profile"] = string(CrashMinimal) }},
		{"server analytics", func(m map[string]any) { m["server_analytics"] = string(ServerAnalyticsEligible) }},
		{"all three together", func(m map[string]any) {
			m["regime"] = string(SoftOptOut)
			m["crash_profile"] = string(CrashMinimal)
			m["server_analytics"] = string(ServerAnalyticsEligible)
		}},
	}
	for _, one := range permissive {
		t.Run(one.name, func(t *testing.T) {
			decision := Prepare(context.Background(), VerifiedPlayerPolicy{
				Plan: validPlan(one.mutate), Scope: callerScope(), Now: fixedClock(),
			})
			if decision.PlanUsed {
				t.Fatalf("an unsigned plan with a permissive %s was used: %+v", one.name, decision)
			}
			if decision.Reason != ReasonUnsignedPermissive || decision.Regime != StrictOptIn {
				t.Fatalf("reason=%q regime=%q", decision.Reason, decision.Regime)
			}
			// Every purpose helper closed, not merely the one that was permissive.
			if !decision.AnalyticsClosed() || !decision.CrashClosed() {
				t.Fatal("a refused plan must close every lane")
			}
			if state, objection := decision.ServerAnalyticsBasis(); state != ServerAnalyticsDenied || !objection {
				t.Fatalf("state=%q objection=%v", state, objection)
			}
			if purposes := decision.ProhibitedPurposes(); purposes != nil {
				t.Fatalf("a refused plan reported purposes: %v", purposes)
			}
		})
	}
	// UNKNOWN is conservative and IS honoured unsigned — or the rule would be
	// refusing the resolver's own output rather than a forgery.
	unknown := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) { m["regime"] = string(Unknown) }), Scope: callerScope(), Now: fixedClock(),
	})
	if !unknown.PlanUsed || unknown.Regime != Unknown {
		t.Fatalf("an unsigned UNKNOWN plan must be used: %+v", unknown)
	}
}

// ⚠ ABSENCE IS NOT false FOR A REQUIRED SCALAR. Decoded into a bool, a plan
// that simply omits the objection requirement read as "none required" — the
// permissive answer, from a field the server never sent.
func TestAnAbsentRequiredScalarFailsClosed(t *testing.T) {
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan:  validPlan(func(m map[string]any) { delete(m, "server_analytics_objection_required") }),
		Scope: callerScope(), Now: fixedClock(),
	})
	if decision.PlanUsed {
		t.Fatalf("a plan missing the required scalar was used: %+v", decision)
	}
	if decision.Reason != ReasonPlanUnreadable {
		t.Fatalf("reason=%q detail=%q", decision.Reason, decision.Detail)
	}
	// And an explicit false still parses — the rule is about ABSENCE, not about
	// the value.
	explicit := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan:  validPlan(func(m map[string]any) { m["server_analytics_objection_required"] = false }),
		Scope: callerScope(), Now: fixedClock(),
	})
	if !explicit.PlanUsed {
		t.Fatalf("an explicit false must parse: %+v", explicit)
	}
	if _, objection := explicit.ServerAnalyticsBasis(); objection {
		t.Fatal("an explicit false must read as false")
	}
}

// ⚠ THE ZERO DECISION IS CLOSED ON EVERY LANE. An uninitialised struct, a map
// miss, a decoded nothing — all of them used to report analytics as OPEN.
func TestTheZeroDecisionIsClosedEverywhere(t *testing.T) {
	var d Decision
	if !d.AnalyticsClosed() {
		t.Error("analytics must be closed on a zero Decision")
	}
	if !d.CrashClosed() {
		t.Error("the crash lane must be closed on a zero Decision")
	}
	if state, objection := d.ServerAnalyticsBasis(); state != ServerAnalyticsDenied || !objection {
		t.Errorf("state=%q objection=%v", state, objection)
	}
	if d.ProhibitedPurposes() != nil || d.OperationBlocks() != nil {
		t.Error("a zero Decision must report no lists")
	}
	if _, ok := d.Plan(); ok {
		t.Error("a zero Decision must report no plan")
	}
}

// ⚠ TRAILING CONTENT: json.Decoder.More() answers whether another element
// follows INSIDE the current array or object, so it says false at a stray "]"
// or "}" — and the garbage after a complete plan went unnoticed.
func TestTrailingContentIsRefused(t *testing.T) {
	for _, suffix := range []string{"]", "}", "]garbage", "{}", `{"regime":"SOFT_OPT_OUT"}`, "null", " \t"} {
		raw := append(validPlan(nil), []byte(suffix)...)
		decision := Prepare(context.Background(), VerifiedPlayerPolicy{
			Plan: raw, Scope: callerScope(), Now: fixedClock(),
		})
		if suffix == " \t" {
			// Trailing WHITESPACE is not content; refusing it would reject a
			// pretty-printed body.
			if !decision.PlanUsed {
				t.Errorf("trailing whitespace must be accepted: %+v", decision)
			}
			continue
		}
		if decision.PlanUsed {
			t.Errorf("a plan followed by %q was used", suffix)
		}
	}
}

// ⚠ encoding/json MATCHES KEYS CASE-INSENSITIVELY and lets a later duplicate
// win. "REGIME" is not the schema name, the schema name never appeared twice,
// so DisallowUnknownFields saw nothing wrong — and the permissive spelling
// decided the regime.
func TestKeysMustBeExactAndUnique(t *testing.T) {
	base := string(validPlan(nil))
	cases := map[string]string{
		"a different case wins the field": `{"REGIME":"SOFT_OPT_OUT",` + base[1:],
		"a duplicate exact key":           `{"regime":"STRICT_OPT_IN",` + base[1:],
		"a mixed-case duplicate":          `{"Regime":"SOFT_OPT_OUT",` + base[1:],
	}
	for name, raw := range cases {
		decision := Prepare(context.Background(), VerifiedPlayerPolicy{
			Plan: []byte(raw), Scope: callerScope(), Now: fixedClock(),
		})
		if decision.PlanUsed {
			t.Errorf("%s: the plan was used", name)
		}
		if decision.Reason != ReasonPlanUnreadable {
			t.Errorf("%s: reason=%q detail=%q", name, decision.Reason, decision.Detail)
		}
	}
	// The control: the schema's own spelling parses, or the rule refuses
	// everything.
	if !Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(nil), Scope: callerScope(), Now: fixedClock(),
	}).PlanUsed {
		t.Fatal("the exact spelling must parse")
	}
}

// ⚠ A COPIED DECISION MUST NOT BE ABLE TO EDIT THE ORIGINAL'S ANSWER. Values
// copy; the arrays behind them do not.
func TestACopiedDecisionCannotMutateTheOriginal(t *testing.T) {
	original := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) {
			m["operation_blocks"] = []any{"transfer_review"}
			m["prohibited_purposes"] = []any{"advertising"}
		}),
		Scope: callerScope(), Now: fixedClock(),
	})
	if !original.PlanUsed {
		t.Fatalf("the fixture must be used: %+v", original)
	}
	copied := original
	plan, ok := copied.Plan()
	if !ok {
		t.Fatal("the copy must report its plan")
	}
	plan.OperationBlocks[0] = "removed"
	plan.ProhibitedPurposes[0] = "removed"
	if plan.AgeBand != nil {
		plan.AgeBand.Band = "removed"
	}
	if got := original.OperationBlocks(); len(got) != 1 || got[0] != "transfer_review" {
		t.Fatalf("the original's blocks were mutated through a copy: %v", got)
	}
	if got := original.ProhibitedPurposes(); len(got) != 1 || got[0] != "advertising" {
		t.Fatalf("the original's purposes were mutated through a copy: %v", got)
	}
	// And two reads of the same verdict do not share an array either.
	first, _ := original.Plan()
	second, _ := original.Plan()
	first.OperationBlocks[0] = "removed"
	if second.OperationBlocks[0] != "transfer_review" {
		t.Fatal("two reads of one verdict share a backing array")
	}
}
