package consentpolicy

import (
	"bytes"
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
	// The first actor hands in a complete, well-formed plan. It is refused for
	// want of a key — but it is refused for ITS OWN reason, which is what makes
	// the second actor's different reason evidence of no memo.
	if first.Reason != ReasonPlanUnsigned {
		t.Fatalf("the control's own reason: %+v", first)
	}
	// The SECOND actor in the same process hands in NO plan. If anything were
	// memoised, this would inherit the first actor's verdict and its reason.
	second := Prepare(context.Background(), VerifiedPlayerPolicy{Scope: callerScope(), Now: fixedClock()})
	if second.PlanUsed() || second.Reason != ReasonPlanAbsent {
		t.Fatalf("a second actor inherited a verdict: %+v", second)
	}
	if !second.OptionalProcessingClosed() {
		t.Fatal("the second actor must be closed, whatever the first one got")
	}
	// And the reverse order, so the scene cannot pass because the cache only
	// fills on the second call. A third actor with a DIFFERENT malformation
	// gets that malformation's reason, not the one before it.
	third := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) { m["expires_at"] = "not-a-time" }), Scope: callerScope(), Now: fixedClock(),
	})
	if third.Reason != ReasonPlanUnreadable {
		t.Fatalf("the third actor got %q, not its own plan's reason", third.Reason)
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
	// which is the stronger answer: an UNAUTHENTICATED plan is not honoured at
	// all in this release, so there is no verdict to map to a grant in the
	// first place.
	refused := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(func(m map[string]any) { m["regime"] = string(SoftOptOut) }), Scope: callerScope(), Now: fixedClock(),
	})
	if refused.PlanUsed() || refused.Reason != ReasonPlanUnsigned {
		t.Fatalf("an unsigned SOFT plan must not be used: %+v", refused)
	}
	// The contract that "false is not permission" is asked of the TYPE, since
	// the verifier no longer produces such a verdict without a signature. A
	// decision built with a SOFT plan still reports only a regime, and its
	// analytics helper still keys off the plan being used.
	soft := Decision{regime: SoftOptOut, optionalProcessingClosed: false, planUsed: true,
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
	if decision.Reason != ReasonNoTrustedClock || decision.Regime() != StrictOptIn {
		t.Fatalf("%+v", decision)
	}
	if decision.PlanUsed() {
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
	if decision.Reason != ReasonSignatureUnverified || decision.Regime() != StrictOptIn {
		t.Fatalf("%+v", decision)
	}
	// The control: the same plan WITHOUT a signature is refused for a
	// DIFFERENT, named reason. Neither is used in this release, so "was it
	// used" cannot tell them apart — but the two reasons say different things
	// to an operator ("someone sent us a signature we cannot check" is not
	// "we have no key"), and that distinction is what this scene now guards.
	if got := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(nil), Scope: callerScope(), Now: fixedClock(),
	}); got.Reason != ReasonPlanUnsigned {
		t.Fatalf("the unsigned control must be refused as unsigned, not as unverifiable: %+v", got)
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
	if refused.PlanUsed() || refused.Reason != ReasonPlanUnsigned {
		t.Fatalf("an unsigned crash-minimal plan must not be used: %+v", refused)
	}
	if !refused.CrashClosed() || !refused.AnalyticsClosed() {
		t.Fatal("a refused plan closes both lanes")
	}

	// The INDEPENDENCE of the two lanes is a property of the helpers, and it is
	// asked of them directly — it becomes reachable end to end in the release
	// that verifies signatures, and this is where a regression would show
	// before then.
	strictWithCrashOn := Decision{regime: StrictOptIn, optionalProcessingClosed: true, planUsed: true,
		plan: Plan{Regime: StrictOptIn, CrashProfile: CrashMinimal}}
	if !strictWithCrashOn.AnalyticsClosed() {
		t.Fatal("analytics is still closed under STRICT")
	}
	if strictWithCrashOn.CrashClosed() {
		t.Fatal("a permitted crash profile is not closed by analytics being off")
	}
	softWithCrashOff := Decision{regime: SoftOptOut, optionalProcessingClosed: false, planUsed: true,
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
	if refused.PlanUsed() || refused.Reason != ReasonPlanUnsigned {
		t.Fatalf("an unsigned eligible plan must not be used: %+v", refused)
	}

	// The basis/requirement SHAPE, asked of the helper: there is no toggle, and
	// a fallback is DENIED with the requirement standing.
	required := true
	eligible := Decision{planUsed: true, plan: Plan{
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
	unstated := Decision{planUsed: true, plan: Plan{ServerAnalytics: ServerAnalyticsEligible}}
	if _, objection := unstated.ServerAnalyticsBasis(); !objection {
		t.Fatal("an unstated objection requirement must read as required")
	}
}

// The lists are copied out, so a caller cannot mutate the verdict it was given.
func TestTheVerdictsListsAreCopies(t *testing.T) {
	decision := verifiedVerdict(t, validPlan(func(m map[string]any) {
		m["operation_blocks"] = []any{"transfer_review"}
		m["prohibited_purposes"] = []any{"advertising"}
	}))
	blocks, known := decision.OperationBlocks()
	if !known || len(blocks) != 1 {
		t.Fatalf("blocks=%v known=%v", blocks, known)
	}
	blocks[0] = "removed"
	if again, _ := decision.OperationBlocks(); again[0] != "transfer_review" {
		t.Fatal("a caller mutated the verdict's own list")
	}
	// The same question of the SIGNAL pointers, which append() does not copy:
	// the entries are fresh structs still addressing the verdict's own bools.
	plan, ok := decision.Plan()
	if !ok || len(plan.SignalsUsed) == 0 || plan.SignalsUsed[0].Available == nil {
		t.Fatalf("the fixture must carry a stated signal: %+v", plan)
	}
	*plan.SignalsUsed[0].Available = true
	again, _ := decision.Plan()
	if *again.SignalsUsed[0].Available {
		t.Fatal("a caller mutated the verdict through a signal's availability pointer")
	}
}

// verifiedVerdict is the verdict the release that has a verification key will
// return for these bytes. The gate is a compile-time constant, so no test can
// open it; this calls the SAME mapping Prepare calls once it is open rather
// than re-implementing it.
func verifiedVerdict(t *testing.T, raw []byte) Decision {
	t.Helper()
	plan, err := ParsePlan(raw)
	if err != nil {
		t.Fatalf("the fixture must parse: %v", err)
	}
	return decisionFromPlan(plan)
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

// ⚠ THE PREVIOUS CUT OF THIS RULE WAS "AN UNSIGNED PLAN MAY CARRY ONLY THE
// CONSERVATIVE TUPLE", AND IT WAS A HOLE IN THE SHAPE OF AN ANSWER. It
// authenticated three enums and nothing else. prohibited_purposes and
// operation_blocks rode along unauthenticated — and those govern transfer,
// age/capacity, localisation and safety, which no consent choice lifts. So an
// attacker who could not make the plan permissive could STRIP its restrictions
// instead and be told, by a verdict marked USED, that nothing was blocked.
//
// This scene is what replaces it: forge whatever you like, it is not used.
func TestAnUnsignedPlanIsNeverUsedHoweverItIsForged(t *testing.T) {
	forgeries := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"a permissive regime", func(m map[string]any) { m["regime"] = string(SoftOptOut) }},
		{"a permitted crash profile", func(m map[string]any) { m["crash_profile"] = string(CrashMinimal) }},
		{"an eligible server-analytics basis", func(m map[string]any) {
			m["server_analytics"] = string(ServerAnalyticsEligible)
		}},
		// ⚠ THE FOUR THE OLD PREDICATE WAVED THROUGH. Every enum conservative,
		// the restrictions gone.
		{"operation_blocks removed", func(m map[string]any) { delete(m, "operation_blocks") }},
		{"operation_blocks emptied", func(m map[string]any) { m["operation_blocks"] = []any{} }},
		{"prohibited_purposes removed", func(m map[string]any) { delete(m, "prohibited_purposes") }},
		{"prohibited_purposes emptied", func(m map[string]any) { m["prohibited_purposes"] = []any{} }},
		{"nothing at all — the resolver's own honest output", nil},
	}
	for _, one := range forgeries {
		t.Run(one.name, func(t *testing.T) {
			decision := Prepare(context.Background(), VerifiedPlayerPolicy{
				Plan: validPlan(one.mutate), Scope: callerScope(), Now: fixedClock(),
			})
			if decision.PlanUsed() {
				t.Fatalf("an unauthenticated plan (%s) was used: %+v", one.name, decision)
			}
			if decision.Reason != ReasonPlanUnsigned || decision.Regime() != StrictOptIn {
				t.Fatalf("reason=%q regime=%q", decision.Reason, decision.Regime())
			}
			assertClosedOnEveryAxis(t, decision)
		})
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
	if decision.PlanUsed() {
		t.Fatalf("a plan missing the required scalar was used: %+v", decision)
	}
	if decision.Reason != ReasonPlanUnreadable {
		t.Fatalf("reason=%q detail=%q", decision.Reason, decision.Detail)
	}
	// And an explicit false still parses — the rule is about ABSENCE, not about
	// the value. Asked of the parser and the mapping, since Prepare refuses
	// every plan in this release and so cannot tell the two cases apart.
	explicit := verifiedVerdict(t, validPlan(func(m map[string]any) {
		m["server_analytics_objection_required"] = false
	}))
	if !explicit.PlanUsed() {
		t.Fatalf("an explicit false must parse: %+v", explicit)
	}
	if _, objection := explicit.ServerAnalyticsBasis(); objection {
		t.Fatal("an explicit false must read as false")
	}
}

// ⚠ THE VALIDITY MARKER CANNOT BE SET FROM OUTSIDE. As an exported field it
// was the one piece of a verdict a caller could forge — and a JSON round-trip
// forged it without anyone meaning to, because encoding/json cannot see the
// unexported plan, so it dropped the plan and kept the flag.
func TestTheValidityMarkerCannotBeForged(t *testing.T) {
	// The written-by-hand forgery: the old field shape, straight from JSON.
	var forged Decision
	if err := json.Unmarshal([]byte(`{"Regime":"SOFT_OPT_OUT","OptionalProcessingClosed":false,"PlanUsed":true}`),
		&forged); err != nil {
		t.Fatalf("the fixture must decode: %v", err)
	}
	if forged.PlanUsed() {
		t.Fatal("a Decision decoded from JSON reported a verified plan")
	}
	assertClosedOnEveryAxis(t, forged)

	// And the accidental one: a REAL verified verdict, logged and read back.
	encoded, err := json.Marshal(verifiedVerdict(t, validPlan(func(m map[string]any) {
		m["regime"] = string(SoftOptOut)
		m["operation_blocks"] = []any{"transfer_review"}
	})))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var restored Decision
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if restored.PlanUsed() {
		t.Fatal("a round-tripped verdict came back claiming a plan it no longer carries")
	}
	assertClosedOnEveryAxis(t, restored)
}

// ⚠ encoding/json SUBSTITUTES U+FFFD FOR INVALID UTF-8 rather than refusing
// it, so a stray byte inside a string decodes to a DIFFERENT, well-formed
// string which is then compared, bounded and returned as though the resolver
// had sent it. A replacement character is a repair, and this parser refuses
// rather than repairs.
func TestInvalidUTF8IsUnreadable(t *testing.T) {
	raw := validPlan(func(m map[string]any) { m["operation_blocks"] = []any{"transfer_review"} })
	forged := bytes.Replace(raw, []byte("transfer_review"), []byte("transfer\x80review"), 1)
	if bytes.Equal(forged, raw) {
		t.Fatal("the fixture does not carry the entry this scene rewrites")
	}
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: forged, Scope: callerScope(), Now: fixedClock(),
	})
	if decision.PlanUsed() || decision.Reason != ReasonPlanUnreadable {
		t.Fatalf("%+v detail=%q", decision, decision.Detail)
	}
	// The control: the same plan with the byte removed still parses, so the
	// rule is about the byte rather than about the entry.
	verifiedVerdict(t, raw)
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
	if purposes, known := d.ProhibitedPurposes(); known || purposes != nil {
		t.Error("a zero Decision must report its purpose list as not known")
	}
	if blocks, known := d.OperationBlocks(); known || blocks != nil {
		t.Error("a zero Decision must report its block list as not known")
	}
	// ⚠ AND "NOT KNOWN" MEANS RESTRICTED, NOT PERMITTED. The lists being empty
	// is the reading that used to be available to a caller; the predicates are
	// the reading that is true.
	if !d.OperationBlocked("transfer_review") || !d.PurposeProhibited("advertising") {
		t.Error("a zero Decision must block every operation and prohibit every purpose")
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
		// Prepare refuses every plan in this release, so "was it used" can no
		// longer separate these cases. The REASON can: trailing content is
		// unreadable, while a clean body gets as far as the authentication
		// gate and is refused there.
		if suffix == " \t" {
			// Trailing WHITESPACE is not content; refusing it would reject a
			// pretty-printed body.
			if decision.Reason != ReasonPlanUnsigned {
				t.Errorf("trailing whitespace must be accepted: %+v", decision)
			}
			continue
		}
		if decision.PlanUsed() || decision.Reason != ReasonPlanUnreadable {
			t.Errorf("a plan followed by %q: %+v", suffix, decision)
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
		if decision.PlanUsed() {
			t.Errorf("%s: the plan was used", name)
		}
		if decision.Reason != ReasonPlanUnreadable {
			t.Errorf("%s: reason=%q detail=%q", name, decision.Reason, decision.Detail)
		}
	}
	// The control: the schema's own spelling parses, or the rule refuses
	// everything. Asked of the reason, since no plan is used in this release.
	if got := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(nil), Scope: callerScope(), Now: fixedClock(),
	}); got.Reason != ReasonPlanUnsigned {
		t.Fatalf("the exact spelling must parse and reach the gate: %+v", got)
	}
}

// ⚠ AND THE KEY WALK MUST REACH EVERY DEPTH. The top-level-only version was
// not a smaller version of this rule, it was a hole: it handed each nested
// value to encoding/json untouched, so the case-insensitive, last-one-wins
// matching decided the nested field. A scope carrying "workspace_id" and
// "WORKSPACE_ID" decoded to the expected workspace and then PASSED the scope
// comparison — a plan issued for another app admitting here.
func TestNestedObjectKeysMustBeExactAndUnique(t *testing.T) {
	withBand := func(m map[string]any) {
		m["age_band"] = map[string]any{"vocabulary": "coarse.v1", "band": "adult"}
	}
	cases := []struct {
		name           string
		mutate         func(map[string]any)
		old, forged    string
		alreadyRefused bool
	}{
		// ⚠ THE ONE THAT MATTERS. The plan is issued for another tenant and
		// the case-variant spelling is what the decoder picks, so the scope
		// comparison compares the FORGERY against itself and passes.
		{"a case-variant key inside scope", nil,
			`"workspace_id":"ws-synthetic"`,
			`"workspace_id":"ws-OTHER-TENANT","WORKSPACE_ID":"ws-synthetic"`, false},
		{"a duplicate key inside scope", nil,
			`"app_id":"app-synthetic"`,
			`"app_id":"app-synthetic","app_id":"app-synthetic"`, false},
		{"a case-variant key inside age_band", withBand,
			`"band":"adult"`,
			`"BAND":"minor","band":"adult"`, false},
		{"a case-variant key inside a signal", nil,
			`"available":false`,
			`"AVAILABLE":true,"available":false`, false},
		{"a duplicate key inside a signal", nil,
			`"name":"server_country"`,
			`"name":"server_country","name":"server_country"`, false},
		// Stated for completeness, and honestly: DisallowUnknownFields already
		// applies at every depth, so an unknown NAME inside a nested object was
		// never the hole. The hole was the ambiguity between two spellings of a
		// name the schema does have, which DisallowUnknownFields cannot see.
		{"an unknown key inside scope", nil,
			`"environment_id":"env-synthetic"`,
			`"environment_id":"env-synthetic","tenant":"x"`, true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			// ⚠ THE FORGERY IS SPLICED INTO THE FIXTURE'S OWN NESTED OBJECT,
			// not prepended as a second top-level key. A second top-level
			// "scope" would be caught by the top-level walk that already
			// existed, and the scene would pass for the wrong reason — which
			// is exactly what the first draft of this test did.
			raw := string(validPlan(testCase.mutate))
			if !strings.Contains(raw, testCase.old) {
				t.Fatalf("the fixture does not contain %q", testCase.old)
			}
			forged := strings.Replace(raw, testCase.old, testCase.forged, 1)
			decision := Prepare(context.Background(), VerifiedPlayerPolicy{
				Plan: []byte(forged), Scope: callerScope(), Now: fixedClock(),
			})
			if decision.PlanUsed() || decision.Reason != ReasonPlanUnreadable {
				t.Fatalf("%+v detail=%q", decision, decision.Detail)
			}
		})
	}
	// The control: the same nested objects spelled correctly still parse, or
	// the rule would be refusing every plan that has a nested object at all.
	verifiedVerdict(t, validPlan(withBand))
}

// ⚠ A SIGNAL THAT NEVER STATES ITS AVAILABILITY IS UNREADABLE, NOT
// UNAVAILABLE. Decoded into a plain bool, the zero value invented an answer
// the resolver never gave — and it happened to be the conservative side of
// that one field, which is exactly why it went unnoticed. The provenance
// record then says the source was unavailable when what is true is that the
// plan never said.
func TestASignalMustStateItsAvailability(t *testing.T) {
	for name, signal := range map[string]any{
		"available omitted": map[string]any{"name": "server_country", "reason": string(NotEnabledInRelease)},
		"available null":    map[string]any{"name": "server_country", "available": nil, "reason": string(NotEnabledInRelease)},
	} {
		t.Run(name, func(t *testing.T) {
			decision := Prepare(context.Background(), VerifiedPlayerPolicy{
				Plan:  validPlan(func(m map[string]any) { m["signals_used"] = []any{signal} }),
				Scope: callerScope(), Now: fixedClock(),
			})
			if decision.PlanUsed() || decision.Reason != ReasonPlanUnreadable {
				t.Fatalf("%+v detail=%q", decision, decision.Detail)
			}
			if !strings.Contains(decision.Detail, "does not state whether it was available") {
				t.Fatalf("the refusal must name what was missing: %q", decision.Detail)
			}
		})
	}
	// The controls: a STATED false with a reason, and a STATED true without
	// one, both parse. Without them this rule would be satisfied by a parser
	// that refuses every signal.
	verifiedVerdict(t, validPlan(nil))
	verifiedVerdict(t, validPlan(func(m map[string]any) {
		m["signals_used"] = []any{map[string]any{"name": "server_country", "available": true}}
	}))
}

// ⚠ A COPIED DECISION MUST NOT BE ABLE TO EDIT THE ORIGINAL'S ANSWER. Values
// copy; the arrays behind them do not.
func TestACopiedDecisionCannotMutateTheOriginal(t *testing.T) {
	original := verifiedVerdict(t, validPlan(func(m map[string]any) {
		m["operation_blocks"] = []any{"transfer_review"}
		m["prohibited_purposes"] = []any{"advertising"}
	}))
	if !original.PlanUsed() {
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
	if got, known := original.OperationBlocks(); !known || len(got) != 1 || got[0] != "transfer_review" {
		t.Fatalf("the original's blocks were mutated through a copy: %v", got)
	}
	if got, known := original.ProhibitedPurposes(); !known || len(got) != 1 || got[0] != "advertising" {
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
