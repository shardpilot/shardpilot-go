package consentpolicy

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	if !second.ExplicitGrantRequired() {
		t.Fatal("the second actor must require an explicit grant, whatever the first one got")
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
	// The contract that "a default is not a grant" is asked of the TYPE, since
	// the verifier no longer produces such a verdict without a signature. A
	// SOFT plan pre-sets the choice ON and does not itself require the explicit
	// grant — and that is still not permission: the notice barrier and the
	// backend admission bound to the scoped session both stand, and this
	// package knows about neither.
	soft := Decision{regime: SoftOptOut, planUsed: true, plan: Plan{Regime: SoftOptOut}}
	if soft.AnalyticsChoiceDefault() != ChoiceDefaultOn {
		t.Fatal("a used SOFT plan pre-sets the optional choice ON")
	}
	if soft.ExplicitGrantRequired() {
		t.Fatal("SOFT does not itself require the explicit grant")
	}
	zero := Decision{}
	if zero.AnalyticsChoiceDefault() != ChoiceDefaultOff || !zero.ExplicitGrantRequired() {
		t.Fatal("a zero Decision defaults OFF and requires an explicit grant")
	}
	if _, known := zero.CrashProfileOffered(); known {
		t.Fatal("a zero Decision offers no crash profile")
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
// release reserves the field and sends null; when a value arrives, a build that
// cannot check it must not treat its arrival as a downgrade by ignoring it.
func TestAnUnverifiableSignatureIsStrict(t *testing.T) {
	decision := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan:  validPlan(func(m map[string]any) { m["signature"] = "ed25519:synthetic" }),
		Scope: callerScope(), Now: fixedClock(),
	})
	if decision.Reason != ReasonSignatureUnverified || decision.Regime() != StrictOptIn {
		t.Fatalf("%+v", decision)
	}
	// The control: the same plan with the key PRESENT AND NULL is refused for a
	// DIFFERENT, named reason. Neither is used in this release, so "was it
	// used" cannot tell them apart — but the two reasons say different things
	// to an operator ("someone sent us a signature we cannot check" is not
	// "we have no key"), and that distinction is what this scene now guards.
	if got := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(nil), Scope: callerScope(), Now: fixedClock(),
	}); got.Reason != ReasonPlanUnsigned {
		t.Fatalf("the null-signature control must be refused as unsigned, not as unverifiable: %+v", got)
	}
}

// The crash lane does not inherit the analytics answer in either direction —
// the crash profile has its own rule, and the platform decision makes it off
// by default.
func TestTheCrashLaneIsItsOwnDecision(t *testing.T) {
	// ⚠ AN UNSIGNED PLAN MAY NOT PERMIT THE CRASH LANE AT ALL in this release,
	// so the end-to-end question is now "is it refused?" — and it is.
	refused := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan:  validPlan(withFlag("crash_profile", string(CrashMinimalDiagnosticsForMinors))),
		Scope: callerScope(), Now: fixedClock(),
	})
	if refused.PlanUsed() || refused.Reason != ReasonPlanUnsigned {
		t.Fatalf("an unsigned crash-minimal plan must not be used: %+v", refused)
	}
	if profile, known := refused.CrashProfileOffered(); known || profile != CrashOff {
		t.Fatalf("a refused plan offers no crash profile, got %q/%v", profile, known)
	}
	if refused.AnalyticsChoiceDefault() != ChoiceDefaultOff {
		t.Fatal("a refused plan defaults the analytics choice OFF")
	}

	// The INDEPENDENCE of the two lanes is a property of the helpers, and it is
	// asked of them directly — it becomes reachable end to end in the release
	// that verifies signatures, and this is where a regression would show
	// before then.
	strictWithCrashOn := Decision{regime: StrictOptIn, planUsed: true, plan: Plan{
		Regime: StrictOptIn, Flags: Flags{CrashProfile: CrashMinimalDiagnosticsForMinors}}}
	if strictWithCrashOn.AnalyticsChoiceDefault() != ChoiceDefaultOff {
		t.Fatal("the analytics choice still defaults OFF under STRICT")
	}
	if profile, known := strictWithCrashOn.CrashProfileOffered(); !known || profile != CrashMinimalDiagnosticsForMinors {
		t.Fatalf("a permitted crash profile is not withdrawn by the analytics default, got %q/%v", profile, known)
	}
	softWithCrashOff := Decision{regime: SoftOptOut, planUsed: true, plan: Plan{
		Regime: SoftOptOut, Flags: Flags{CrashProfile: CrashOff}}}
	if profile, known := softWithCrashOff.CrashProfileOffered(); !known || profile != CrashOff {
		t.Fatalf("an off crash profile is not opened by the analytics regime, got %q/%v", profile, known)
	}
}

// ⚠ THE BACKEND LANE IS A STATE, NOT A TOGGLE, AND THE OBJECTION REQUIREMENT
// THAT USED TO RIDE BESIDE IT IS GONE WITH ITS FIELD.
//
// `server_analytics_objection_required` is not on the wire in this release —
// it was read at the top level, from a key the resolver has never sent, so the
// getter answered from a decoded zero value. A getter over a field the server
// does not send is a promise this package cannot keep, so it returns with the
// field rather than being renamed.
func TestServerAnalyticsIsAStateNotAToggle(t *testing.T) {
	// `denied` is the only value in this release's vocabulary, so a plan
	// carrying anything else does not parse — which is itself the guard
	// against a permissive value being invented locally.
	unreadable := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan:  validPlan(withFlag("server_analytics", "eligible")),
		Scope: callerScope(), Now: fixedClock(),
	})
	if unreadable.Reason != ReasonPlanUnreadable {
		t.Fatalf("a value outside the vocabulary must not parse: %+v", unreadable)
	}
	// A fallback is denied.
	if state := Prepare(context.Background(), VerifiedPlayerPolicy{
		Scope: callerScope(), Now: fixedClock(),
	}).ServerAnalytics(); state != ServerAnalyticsDenied {
		t.Fatalf("a fallback must be denied, got %q", state)
	}
	// And a USED plan reports the state from the nested flags rather than from
	// a top-level field that no longer exists.
	used := verifiedVerdict(t, validPlan(nil))
	if state := used.ServerAnalytics(); state != ServerAnalyticsDenied {
		t.Fatalf("the plan's own flags.server_analytics must reach the verdict, got %q", state)
	}
	// The withdrawn getters are gone from the surface, not renamed: a caller
	// who kept compiling against the old name would keep the old meaning.
	assertNoSuchMethods(t, "OptionalProcessingClosed", "AnalyticsClosed", "CrashClosed",
		"ServerAnalyticsBasis", "ProhibitedPurposes", "PurposeProhibited")
}

// assertNoSuchMethods reads the package's own source for method names that
// must not come back. A withdrawn getter returning under its old name is the
// one change that would break a caller silently rather than loudly.
func assertNoSuchMethods(t *testing.T, names ...string) {
	t.Helper()
	for _, file := range packageSources(t) {
		for _, name := range names {
			if strings.Contains(file.body, "func (d Decision) "+name+"(") {
				t.Errorf("%s declares Decision.%s, which was withdrawn with the field it read", file.name, name)
			}
		}
	}
}

// The lists are copied out, so a caller cannot mutate the verdict it was given.
func TestTheVerdictsListsAreCopies(t *testing.T) {
	decision := verifiedVerdict(t, validPlan(withFlag("operation_blocks", []any{"transfer_review"})))
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
	// ⚠ AND THE FIELDS THAT USED TO BE READ FROM THE WRONG DEPTH. A top-level
	// read leaves all four at their zero value, which is an empty string and a
	// nil slice — and a nil slice is the "nothing is blocked" reading.
	if plan.Flags.CrashProfile != CrashOff || plan.Flags.ServerAnalytics != ServerAnalyticsDenied ||
		plan.Flags.ChildRules != ChildRulesMinimised || plan.Flags.OperationBlocks == nil {
		t.Fatalf("the nested flags did not reach the plan: %+v", plan.Flags)
	}
	if plan.Basis.Character != BasisInformationalReference || plan.Basis.TableProvenance != ProvenanceAIDraft ||
		plan.Basis.Notice == "" {
		t.Fatalf("the nested basis did not reach the plan: %+v", plan.Basis)
	}
}

// ⚠ THE PREVIOUS CUT OF THIS RULE WAS "AN UNSIGNED PLAN MAY CARRY ONLY THE
// CONSERVATIVE TUPLE", AND IT WAS A HOLE IN THE SHAPE OF AN ANSWER. It
// authenticated three enums and nothing else. operation_blocks rode along
// unauthenticated — and those govern transfer, age/capacity, localisation and
// safety, which no consent choice lifts. So an attacker who could not make the
// plan permissive could STRIP its restrictions instead and be told, by a
// verdict marked USED, that nothing was blocked.
//
// This scene is what replaces it: forge whatever you like, it is not used.
func TestAnUnsignedPlanIsNeverUsedHoweverItIsForged(t *testing.T) {
	forgeries := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"a permissive regime", func(m map[string]any) { m["regime"] = string(SoftOptOut) }},
		{"a permitted crash profile", withFlag("crash_profile", string(CrashMinimalDiagnosticsForMinors))},
		// ⚠ THE TWO THE OLD PREDICATE WAVED THROUGH. Every enum conservative,
		// the restrictions gone.
		{"operation_blocks removed", func(m map[string]any) { delete(m["flags"].(map[string]any), "operation_blocks") }},
		{"operation_blocks emptied", withFlag("operation_blocks", []any{})},
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
			if decision.Regime() != StrictOptIn {
				t.Fatalf("regime=%q", decision.Regime())
			}
			// The two stripped cases are refused EARLIER, as unreadable,
			// because a required key is missing — which is a stronger refusal
			// than the gate's, not a weaker one. Both are closed, and that is
			// what the shared assertion checks.
			if decision.Reason != ReasonPlanUnsigned && decision.Reason != ReasonPlanUnreadable {
				t.Fatalf("reason=%q detail=%q", decision.Reason, decision.Detail)
			}
			assertClosedOnEveryAxis(t, decision)
		})
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
		m["flags"].(map[string]any)["operation_blocks"] = []any{"transfer_review"}
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
	raw := validPlan(withFlag("operation_blocks", []any{"transfer_review"}))
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

// ⚠ AND max_age_seconds IS A REQUIRED SCALAR THAT ABSENCE COULD FAKE. It
// decoded to 0 and passed the negative-only check, so "the resolver did not
// say" and "the resolver said zero" were the same plan.
func TestAnAbsentMaxAgeFailsClosed(t *testing.T) {
	for _, mutate := range []func(map[string]any){
		func(m map[string]any) { delete(m, "max_age_seconds") },
		func(m map[string]any) { m["max_age_seconds"] = nil },
	} {
		decision := Prepare(context.Background(), VerifiedPlayerPolicy{
			Plan: validPlan(mutate), Scope: callerScope(), Now: fixedClock(),
		})
		if decision.PlanUsed() || decision.Reason != ReasonPlanUnreadable {
			t.Fatalf("%+v detail=%q", decision, decision.Detail)
		}
	}
	// An explicit 0 is a legal value and stays distinguishable. It means "do
	// not reuse this" to the client SDKs' private caches; this package holds no
	// cache, so nothing here acts on it — it parses and is carried.
	zero := verifiedVerdict(t, validPlan(func(m map[string]any) { m["max_age_seconds"] = 0 }))
	plan, ok := zero.Plan()
	if !ok || plan.MaxAgeSeconds == nil || *plan.MaxAgeSeconds != 0 {
		t.Fatalf("an explicit 0 must parse and be carried: %+v", plan)
	}
	// And it is copied out, like every other pointer in the plan: a new
	// pointer field is a new way to reach back into the verdict.
	*plan.MaxAgeSeconds = 999
	if again, _ := zero.Plan(); *again.MaxAgeSeconds != 0 {
		t.Fatal("a caller mutated the verdict through max_age_seconds")
	}
}

// ⚠ THE ZERO DECISION IS CLOSED ON EVERY LANE. An uninitialised struct, a map
// miss, a decoded nothing — all of them used to report analytics as OPEN.
func TestTheZeroDecisionIsClosedEverywhere(t *testing.T) {
	var d Decision
	if d.AnalyticsChoiceDefault() != ChoiceDefaultOff {
		t.Error("the analytics choice must default OFF on a zero Decision")
	}
	if !d.ExplicitGrantRequired() {
		t.Error("a zero Decision must require an explicit grant")
	}
	if profile, known := d.CrashProfileOffered(); known || profile != CrashOff {
		t.Errorf("a zero Decision offers no crash profile, got %q/%v", profile, known)
	}
	if state := d.ServerAnalytics(); state != ServerAnalyticsDenied {
		t.Errorf("state=%q", state)
	}
	if !d.ChildRulesApplied() {
		t.Error("a zero Decision applies child rules")
	}
	if blocks, known := d.OperationBlocks(); known || blocks != nil {
		t.Error("a zero Decision must report its block list as not known")
	}
	// ⚠ AND "NOT KNOWN" MEANS RESTRICTED, NOT PERMITTED. The list being empty
	// is the reading that used to be available to a caller; the predicate is
	// the reading that is true.
	if !d.OperationBlocked("transfer_review") {
		t.Error("a zero Decision must block every operation")
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
			// pretty-printed body — and this package now reads one, so this is
			// no longer a hypothetical.
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
//
// ⚠ AND THE CONTRACT NOW HAS TWO MORE NESTED OBJECTS THAN IT DID. `flags` and
// `basis` arrived with this change, so they are in this table from the start
// rather than after the next incident.
func TestNestedObjectKeysMustBeExactAndUnique(t *testing.T) {
	cases := []struct {
		name        string
		old, forged string
	}{
		// ⚠ THE ONE THAT MATTERS. The plan is issued for another tenant and
		// the case-variant spelling is what the decoder picks, so the scope
		// comparison compares the FORGERY against itself and passes.
		{"a case-variant key inside scope",
			`"workspace_id":"ws_1"`,
			`"workspace_id":"ws-OTHER-TENANT","WORKSPACE_ID":"ws_1"`},
		{"a duplicate key inside scope",
			`"app_id":"app_1"`,
			`"app_id":"app_1","app_id":"app_1"`},
		// ⚠ THE SAME ATTACK ON A FLAG. A permissive spelling wins the field
		// and the conservative one is what a reader of the body sees.
		{"a case-variant key inside flags",
			`"crash_profile":"off"`,
			`"CRASH_PROFILE":"minimal_diagnostics_for_minors","crash_profile":"off"`},
		{"a duplicate key inside flags",
			`"child_rules":"minimised"`,
			`"child_rules":"minimised","child_rules":"minimised"`},
		{"a case-variant key inside basis",
			`"table_provenance":"ai_draft"`,
			`"TABLE_PROVENANCE":"owner_accepted","table_provenance":"ai_draft"`},
		{"a case-variant key inside a signal",
			`"available":false`,
			`"AVAILABLE":true,"available":false`},
		{"a duplicate key inside a signal",
			`"name":"server_country"`,
			`"name":"server_country","name":"server_country"`},
		// Stated for completeness, and honestly: DisallowUnknownFields already
		// applies at every depth, so an unknown NAME inside a nested object was
		// never the hole. The hole was the ambiguity between two spellings of a
		// name the schema does have, which DisallowUnknownFields cannot see.
		{"an unknown key inside scope",
			`"environment_id":"env_1"`,
			`"environment_id":"env_1","tenant":"x"`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			// ⚠ THE FORGERY IS SPLICED INTO THE FIXTURE'S OWN NESTED OBJECT,
			// not prepended as a second top-level key. A second top-level
			// "scope" would be caught by the top-level walk that already
			// existed, and the scene would pass for the wrong reason — which
			// is exactly what the first draft of this test did.
			raw := string(validPlan(nil))
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
	verifiedVerdict(t, validPlan(nil))
}

// ⚠ A SIGNAL THAT NEVER STATES ITS AVAILABILITY IS UNREADABLE, NOT
// UNAVAILABLE. Decoded into a plain bool, the zero value invented an answer
// the resolver never gave — and it happened to be the conservative side of
// that one field, which is exactly why it went unnoticed. The provenance
// record then says the source was unavailable when what is true is that the
// plan never said.
func TestASignalMustStateItsAvailability(t *testing.T) {
	// ⚠ OMITTED AND NULL ARE REFUSED BY DIFFERENT CHECKS, AND EACH IS ASKED
	// FOR ITS OWN MESSAGE — which is the whole reason this field is a pointer.
	// Both decode to the same nil, so only the RAW document can tell them
	// apart: the required-key walk names an absent key, and the null-class
	// walk names a present-null one. Asserting one shared sentence across both
	// would have let either check disappear silently.
	for _, testCase := range []struct {
		name   string
		signal any
		detail string
	}{
		{"available omitted", map[string]any{"name": "server_country", "reason": string(NotEnabledInRelease)},
			`omits the required key "signals_used[0].available"`},
		{"available null", map[string]any{"name": "server_country", "available": nil, "reason": string(NotEnabledInRelease)},
			`sends null for "signals_used[0].available"`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			decision := Prepare(context.Background(), VerifiedPlayerPolicy{
				Plan:  validPlan(func(m map[string]any) { m["signals_used"] = []any{testCase.signal} }),
				Scope: callerScope(), Now: fixedClock(),
			})
			if decision.PlanUsed() || decision.Reason != ReasonPlanUnreadable {
				t.Fatalf("%+v detail=%q", decision, decision.Detail)
			}
			if !strings.Contains(decision.Detail, testCase.detail) {
				t.Fatalf("the refusal must name what was missing: %q", decision.Detail)
			}
		})
	}
	// The controls: the recorded body's three STATED-false signals with their
	// reasons, and a STATED true without one, both parse. Without them this
	// rule would be satisfied by a parser that refuses every signal.
	verifiedVerdict(t, validPlan(nil))
	verifiedVerdict(t, validPlan(func(m map[string]any) {
		m["signals_used"] = []any{map[string]any{"name": "server_country", "available": true}}
	}))
}

// ⚠ A COPIED DECISION MUST NOT BE ABLE TO EDIT THE ORIGINAL'S ANSWER. Values
// copy; the arrays behind them do not.
func TestACopiedDecisionCannotMutateTheOriginal(t *testing.T) {
	original := verifiedVerdict(t, validPlan(withFlag("operation_blocks", []any{"transfer_review"})))
	if !original.PlanUsed() {
		t.Fatalf("the fixture must be used: %+v", original)
	}
	copied := original
	plan, ok := copied.Plan()
	if !ok {
		t.Fatal("the copy must report its plan")
	}
	plan.Flags.OperationBlocks[0] = "removed"
	if got, known := original.OperationBlocks(); !known || len(got) != 1 || got[0] != "transfer_review" {
		t.Fatalf("the original's blocks were mutated through a copy: %v", got)
	}
	// And two reads of the same verdict do not share an array either.
	first, _ := original.Plan()
	second, _ := original.Plan()
	first.Flags.OperationBlocks[0] = "removed"
	if second.Flags.OperationBlocks[0] != "transfer_review" {
		t.Fatal("two reads of one verdict share a backing array")
	}
}
