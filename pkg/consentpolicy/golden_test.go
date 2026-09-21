package consentpolicy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// The two recorded responses, by the names they are vendored under.
var goldenPairs = []struct {
	name    string
	wire    string
	review  string
	refusal bool
}{
	{"a resolved plan", "consent-policy-resolved", "consent-policy-resolved.indented", false},
	{"a refusal", "consent-policy-refusal", "consent-policy-refusal.indented", true},
}

// ⚠ THE REVIEW FORM COMPACTS TO THE WIRE FORM, BYTE FOR BYTE, WITH NO ROUND
// TRIP THROUGH A JSON LIBRARY.
//
// Decoding both files and comparing the results would prove only that they
// MEAN the same thing, which is the weaker claim — and the weaker claim is
// exactly the one that let this SDK and the resolver disagree for months. A
// key reordered, a number respelled, an escape rewritten or a `null` where an
// empty array belongs all survive a round trip and all fail this.
//
// The compaction is hand-rolled for the same reason: json.Compact is a JSON
// library, so using it here would be asking the question of the same code the
// scene exists to be independent of.
func TestTheReviewFormsCompactToTheWireBytes(t *testing.T) {
	for _, pair := range goldenPairs {
		t.Run(pair.name, func(t *testing.T) {
			wire := goldenWire(t, pair.wire)
			review := goldenWire(t, pair.review)
			// The scene would be vacuous if the two files were already
			// identical — then "compaction preserves them" says nothing.
			if bytes.Equal(wire, review) {
				t.Fatal("the review form is byte-identical to the wire form; this scene is asserting nothing")
			}
			compacted := compactOutsideStrings(review)
			if !bytes.Equal(compacted, wire) {
				t.Fatalf("the review form does not compact to the wire bytes\n  compacted: %d bytes\n  wire:     %d bytes\n  first difference at %d",
					len(compacted), len(wire), firstDifference(compacted, wire))
			}
		})
	}
}

// compactOutsideStrings removes insignificant whitespace and NOTHING ELSE. It
// never enters a string literal, so a space or a newline inside the notice
// survives; an escaped quote does not end the string.
func compactOutsideStrings(raw []byte) []byte {
	out := make([]byte, 0, len(raw))
	inString, escaped := false, false
	for _, b := range raw {
		if inString {
			out = append(out, b)
			switch {
			case escaped:
				escaped = false
			case b == '\\':
				escaped = true
			case b == '"':
				inString = false
			}
			continue
		}
		switch b {
		case ' ', '\t', '\n', '\r':
			continue
		case '"':
			inString = true
		}
		out = append(out, b)
	}
	return out
}

func firstDifference(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

// ⚠ THE COMPACTION MUST NOT TOUCH WHAT IS INSIDE A STRING, and that is asked
// of it directly rather than inferred from the scene above passing. A
// compactor that stripped spaces everywhere would still turn the review form
// into something — just not the wire bytes — and if the notice happened to
// have no interior spaces the difference would never show.
func TestTheCompactionLeavesStringsAlone(t *testing.T) {
	cases := []struct{ in, want string }{
		{"{ \"a\" : \"x y\" }", `{"a":"x y"}`},
		{"{\n\t\"a\": \"line\\nbreak\"\n}", `{"a":"line\nbreak"}`},
		// An escaped quote does not end the string, so the spaces after it are
		// still interior.
		{`{ "a" : "he said \" a b \" " }`, `{"a":"he said \" a b \" "}`},
		{"[ 1 , 2 ]", "[1,2]"},
	}
	for _, testCase := range cases {
		if got := string(compactOutsideStrings([]byte(testCase.in))); got != testCase.want {
			t.Errorf("compact(%q) = %q, want %q", testCase.in, got, testCase.want)
		}
	}
	// And the real notice, which is the string this actually protects: it
	// carries interior spaces, a semicolon and an escaped em dash.
	var body map[string]any
	if err := json.Unmarshal(goldenWire(t, "consent-policy-resolved"), &body); err != nil {
		t.Fatal(err)
	}
	notice, _ := body["basis"].(map[string]any)["notice"].(string)
	if !strings.Contains(notice, " ") {
		t.Fatal("the notice has no interior spaces, so the scene above cannot detect a compactor that strips them")
	}
}

// Both recorded bodies parse. Without this, every scene below could be passing
// because the parser refuses everything.
func TestTheRecordedResponsesParse(t *testing.T) {
	for _, pair := range goldenPairs {
		for _, name := range []string{pair.wire, pair.review} {
			plan, err := ParsePlan(goldenWire(t, name))
			if err != nil {
				t.Fatalf("%s must parse: %v", name, err)
			}
			// The flags arrived from the nested object rather than as four
			// zero values, which is the contract this change repairs.
			if plan.Flags.CrashProfile != CrashOff || plan.Flags.ServerAnalytics != ServerAnalyticsDenied ||
				plan.Flags.ChildRules != ChildRulesMinimised || plan.Flags.OperationBlocks == nil {
				t.Fatalf("%s: flags did not arrive from the nested object: %+v", name, plan.Flags)
			}
			// operation_blocks is `[]`, never null — an empty list that is
			// PRESENT is a different fact from an absent one.
			if len(plan.Flags.OperationBlocks) != 0 {
				t.Fatalf("%s: operation_blocks=%v", name, plan.Flags.OperationBlocks)
			}
			// signature is present and null on every response, refusals
			// included.
			if plan.Signature != nil {
				t.Fatalf("%s: signature must decode as present-null, got %q", name, *plan.Signature)
			}
			if (plan.Reason != nil) != pair.refusal {
				t.Fatalf("%s: reason=%v but refusal=%v", name, plan.Reason, pair.refusal)
			}
			// The refusal's reason is the one the resolver actually sent, from
			// the closed vocabulary — not merely "some reason is present".
			if pair.refusal && *plan.Reason != RefusalInvalidScope {
				t.Fatalf("%s: reason=%q, want %q", name, *plan.Reason, RefusalInvalidScope)
			}
		}
	}
}

// ⚠ WHITESPACE MUST NOT CHANGE THE ANSWER. This package scans the RAW TEXT for
// duplicate keys, unknown keys and container types before it decodes anything,
// and JSON permits insignificant whitespace between every pair of tokens.
// Until the review forms were vendored, every scene ran on the compact
// spelling alone, so that scanner had never met a newline or an indent — and a
// proxy that re-serialises, or a server that starts pretty-printing, arrives
// as exactly that.
//
// Compared as PARSED PLANS rather than as verdicts, because a verdict is
// dominated by the release's authentication gate: every input refuses, so two
// spellings would agree trivially while the parser disagreed about both.
func TestWhitespaceDoesNotChangeTheParse(t *testing.T) {
	for _, pair := range goldenPairs {
		t.Run(pair.name, func(t *testing.T) {
			fromWire, err := ParsePlan(goldenWire(t, pair.wire))
			if err != nil {
				t.Fatalf("the wire form must parse: %v", err)
			}
			fromReview, err := ParsePlan(goldenWire(t, pair.review))
			if err != nil {
				t.Fatalf("the review form must parse: %v", err)
			}
			if !reflect.DeepEqual(fromWire, fromReview) {
				t.Fatalf("the two spellings parsed differently\n  wire:   %+v\n  review: %+v", fromWire, fromReview)
			}
		})
	}
}

// goldenKeyPaths enumerates EVERY key in a recorded body, at every depth, as a
// dotted path with array indices. The required-key scene is driven from this
// rather than from a hand-written list, so a key added to the contract is
// covered the moment the golden is re-recorded — a list would have to be
// remembered, and the last hand-maintained list in this package is what this
// whole change is repairing.
func goldenKeyPaths(value any, prefix string) []string {
	var paths []string
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			path := key
			if prefix != "" {
				path = prefix + "." + key
			}
			paths = append(paths, path)
			paths = append(paths, goldenKeyPaths(child, path)...)
		}
	case []any:
		for i, child := range typed {
			paths = append(paths, goldenKeyPaths(child, fmt.Sprintf("%s[%d]", prefix, i))...)
		}
	}
	return paths
}

func splitPath(path string) []string { return strings.Split(path, ".") }

// deleteAtPath removes exactly one key, navigating array indices on the way.
func deleteAtPath(body map[string]any, path string) bool {
	segments := splitPath(path)
	var cursor any = body
	for _, segment := range segments[:len(segments)-1] {
		name, indices := parseSegment(segment)
		container, ok := cursor.(map[string]any)
		if !ok {
			return false
		}
		cursor, ok = container[name]
		if !ok {
			return false
		}
		for _, index := range indices {
			list, ok := cursor.([]any)
			if !ok || index >= len(list) {
				return false
			}
			cursor = list[index]
		}
	}
	name, _ := parseSegment(segments[len(segments)-1])
	container, ok := cursor.(map[string]any)
	if !ok {
		return false
	}
	if _, present := container[name]; !present {
		return false
	}
	delete(container, name)
	return true
}

func parseSegment(segment string) (string, []int) {
	open := strings.Index(segment, "[")
	if open < 0 {
		return segment, nil
	}
	name := segment[:open]
	var indices []int
	for _, part := range strings.Split(strings.Trim(segment[open:], "[]"), "][") {
		var index int
		if _, err := fmt.Sscanf(part, "%d", &index); err != nil {
			return name, indices
		}
		indices = append(indices, index)
	}
	return name, indices
}

// ⚠ EVERY KEY THE RESOLVER SENDS IS REQUIRED, AND THE LIST IS ENUMERATED FROM
// THE BYTES. Omitting any one of them is a refusal — because absence is not a
// value: decoded into a struct, a missing key becomes the zero value and the
// package reports a fact the resolver never stated. That is how
// `available: false` was invented for a signal that said nothing, and how a
// stripped `operation_blocks` read as "nothing is blocked".
//
// `reason` is the one optional key and is absent from the resolved body by
// construction, so it never appears in this enumeration — the discriminator
// cannot be required without making every plan a refusal.
func TestEveryRecordedKeyIsRequired(t *testing.T) {
	raw := goldenWire(t, "consent-policy-resolved")
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	paths := goldenKeyPaths(body, "")
	sort.Strings(paths)

	// ⚠ THE ENUMERATION'S OWN REACH, ASSERTED. A walker that only saw the top
	// level would produce a short list, every deletion would be refused, and
	// the scene would pass while covering none of the nesting this change
	// exists for — the same shape of hole as the top-level-only key walk this
	// package already fixed once.
	for _, needed := range []string{
		"flags", "flags.crash_profile", "flags.operation_blocks",
		"scope.app_id", "basis.notice", "signals_used[0].available", "signals_used[2].reason",
		"signature", "max_age_seconds",
	} {
		if !contains(paths, needed) {
			t.Fatalf("the enumeration missed %q; it found %v", needed, paths)
		}
	}
	if len(paths) < 20 {
		t.Fatalf("only %d keys enumerated, which is too few for this contract: %v", len(paths), paths)
	}
	if contains(paths, "reason") {
		t.Fatal("the resolved body carries a reason; it would not be a resolved body")
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			var mutated map[string]any
			if err := json.Unmarshal(raw, &mutated); err != nil {
				t.Fatal(err)
			}
			mutated["expires_at"] = time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339)
			if !deleteAtPath(mutated, path) {
				t.Fatalf("could not delete %q from the body", path)
			}
			encoded, err := json.Marshal(mutated)
			if err != nil {
				t.Fatal(err)
			}
			decision := Prepare(context.Background(), VerifiedPlayerPolicy{
				Plan: encoded, Scope: callerScope(), Now: fixedClock(),
			})
			if decision.Reason != ReasonPlanUnreadable {
				t.Fatalf("omitting %q gave reason=%q detail=%q, want %q",
					path, decision.Reason, decision.Detail, ReasonPlanUnreadable)
			}
			if decision.PlanUsed() {
				t.Fatalf("a plan missing %q was used", path)
			}
		})
	}

	// The control: the SAME body with nothing removed reaches the
	// authentication gate instead. Without it, a parser that refused every
	// input would pass the whole table above.
	if got := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(nil), Scope: callerScope(), Now: fixedClock(),
	}); got.Reason != ReasonPlanUnsigned {
		t.Fatalf("the complete body must reach the gate: reason=%q detail=%q", got.Reason, got.Detail)
	}
}

func contains(haystack []string, needle string) bool {
	for _, one := range haystack {
		if one == needle {
			return true
		}
	}
	return false
}

// ⚠ A KEY THE SCHEMA DOES NOT HAVE IS REFUSED WHEREVER IT SITS — including at
// the TOP LEVEL under a name the nested object does have. A package that read
// flags at the top level would accept this and quietly prefer it to the nested
// object; a package that reads the contract refuses it as unknown.
func TestTheFlagNamesAreNotTopLevelKeys(t *testing.T) {
	for _, key := range []string{"crash_profile", "server_analytics", "child_rules", "operation_blocks"} {
		t.Run(key, func(t *testing.T) {
			decision := Prepare(context.Background(), VerifiedPlayerPolicy{
				Plan:  validPlan(func(m map[string]any) { m[key] = "off" }),
				Scope: callerScope(), Now: fixedClock(),
			})
			if decision.Reason != ReasonPlanUnreadable {
				t.Fatalf("a top-level %q gave reason=%q detail=%q", key, decision.Reason, decision.Detail)
			}
		})
	}
}

// ⚠ PRESENT-AND-NULL IS THE SIGNATURE'S ONLY ADMISSIBLE STATE, and the three
// cases are three different answers. A pointer alone tells null from a value;
// it cannot tell null from ABSENT, which is what the required-key walk is for.
func TestTheSignatureKeyIsPresentNullOrRefused(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]any)
		want   Reason
	}{
		// Present and null: this is what the resolver sends today, and it must
		// reach the gate rather than being refused as malformed.
		{"present and null", func(m map[string]any) { m["signature"] = nil }, ReasonPlanUnsigned},
		// Absent: a body that never mentioned the key is not the same body.
		{"absent", func(m map[string]any) { delete(m, "signature") }, ReasonPlanUnreadable},
		// Present with a value this build cannot check: refused, and named
		// distinctly, because ignoring it would make the field's arrival a
		// downgrade.
		{"present with a value", func(m map[string]any) { m["signature"] = "ed25519:synthetic" }, ReasonSignatureUnverified},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			decision := Prepare(context.Background(), VerifiedPlayerPolicy{
				Plan: validPlan(testCase.mutate), Scope: callerScope(), Now: fixedClock(),
			})
			if decision.Reason != testCase.want {
				t.Fatalf("reason=%q detail=%q, want %q", decision.Reason, decision.Detail, testCase.want)
			}
			assertClosedOnEveryAxis(t, decision)
		})
	}
}

// ⚠ THE REQUIRED SET IS DERIVED FROM THE STRUCT TAGS, SO A NEW FIELD IS
// REQUIRED BY DEFAULT. That is the safe direction: forgetting to mark a new
// key required would otherwise let it be absent and read as a zero value. This
// asserts the derivation against the golden rather than against itself — every
// name the resolver actually sends must be in the required set, and `reason`,
// the one key carrying `omitempty`, must not be.
func TestTheRequiredSetMatchesTheRecordedTopLevelKeys(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(goldenWire(t, "consent-policy-resolved"), &body); err != nil {
		t.Fatal(err)
	}
	required, ok := requiredKeys[reflect.TypeOf(Plan{})]
	if !ok {
		t.Fatal("the Plan type has no required-key set")
	}
	for key := range body {
		if _, present := required[key]; !present {
			t.Errorf("the resolver sends %q on every response and it is not required", key)
		}
	}
	if _, present := required["reason"]; present {
		t.Error("reason is the refusal discriminator and must not be required")
	}
	// And the reverse: nothing is required that the resolver does not send, or
	// the package refuses every real response.
	for key := range required {
		if _, present := body[key]; !present {
			t.Errorf("%q is required and the resolver does not send it", key)
		}
	}
}

// ⚠ NO KEY IN THIS CONTRACT IS NULLABLE EXCEPT `signature`, AND THAT IS ASKED
// OF EVERY KEY THE RESOLVER SENDS RATHER THAN OF THE ONE THAT WAS CAUGHT.
//
// `"operation_blocks": null` decoded to a nil slice, validate() asked only for
// its length, and a STRIPPED restriction list read as "nothing is blocked" —
// the permissive answer, from a shape the resolver never sends. The same hole
// was open on every other container and scalar: encoding/json gives the same
// zero value for `null` and for absent, so neither the decoded struct nor any
// check over it can tell them apart. Only the raw document can.
//
// Driven from the golden bytes for the same reason the omission table is: a
// key added to the contract is covered the moment the golden is re-recorded.
func TestOnlyTheSignatureKeyMayBeNull(t *testing.T) {
	raw := goldenWire(t, "consent-policy-resolved")
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	paths := goldenKeyPaths(body, "")
	sort.Strings(paths)
	if len(paths) < 20 {
		t.Fatalf("only %d keys enumerated: %v", len(paths), paths)
	}

	for _, path := range paths {
		if path == "signature" {
			continue
		}
		t.Run(path, func(t *testing.T) {
			var mutated map[string]any
			if err := json.Unmarshal(raw, &mutated); err != nil {
				t.Fatal(err)
			}
			mutated["expires_at"] = time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339)
			if !setAtPath(mutated, path, nil) {
				t.Fatalf("could not set %q to null", path)
			}
			encoded, err := json.Marshal(mutated)
			if err != nil {
				t.Fatal(err)
			}
			decision := Prepare(context.Background(), VerifiedPlayerPolicy{
				Plan: encoded, Scope: callerScope(), Now: fixedClock(),
			})
			if decision.Reason != ReasonPlanUnreadable {
				t.Fatalf("null for %q gave reason=%q detail=%q, want %q",
					path, decision.Reason, decision.Detail, ReasonPlanUnreadable)
			}
			if decision.PlanUsed() {
				t.Fatalf("a plan with null at %q was used", path)
			}
			// ⚠ THE REFUSAL NAMES THE KEY. A generic "malformed plan" would
			// pass this table while telling an operator nothing, and the key
			// is what turns a refusal into something actionable.
			if !strings.Contains(decision.Detail, path) {
				t.Fatalf("the refusal for %q must name it: %q", path, decision.Detail)
			}
		})
	}

	// ⚠ THE CONTROL, AND IT IS THE WHOLE POINT OF THE EXCEPTION. `signature`
	// is null on every response the resolver sends, so a rule that refused
	// every null would refuse every real body — the opposite defect, and one
	// that would have shipped looking like rigour.
	t.Run("signature null is accepted", func(t *testing.T) {
		decision := Prepare(context.Background(), VerifiedPlayerPolicy{
			Plan: validPlan(func(m map[string]any) { m["signature"] = nil }), Scope: callerScope(), Now: fixedClock(),
		})
		if decision.Reason != ReasonPlanUnsigned {
			t.Fatalf("a null signature must reach the gate: reason=%q detail=%q", decision.Reason, decision.Detail)
		}
	})
}

// setAtPath replaces one key's value, navigating array indices on the way.
func setAtPath(body map[string]any, path string, value any) bool {
	segments := splitPath(path)
	var cursor any = body
	for _, segment := range segments[:len(segments)-1] {
		name, indices := parseSegment(segment)
		container, ok := cursor.(map[string]any)
		if !ok {
			return false
		}
		cursor, ok = container[name]
		if !ok {
			return false
		}
		for _, index := range indices {
			list, ok := cursor.([]any)
			if !ok || index >= len(list) {
				return false
			}
			cursor = list[index]
		}
	}
	name, _ := parseSegment(segments[len(segments)-1])
	container, ok := cursor.(map[string]any)
	if !ok {
		return false
	}
	if _, present := container[name]; !present {
		return false
	}
	container[name] = value
	return true
}

// ⚠ THE REFUSAL VOCABULARY IS CLOSED, AND IT IS CLOSED BECAUSE THIS VALUE
// REACHES AN OPERATOR'S LOG. It used to be any string, concatenated straight
// into Decision.Detail — so a body could write a second line into a log that
// nothing had authorised, and an empty reason was indistinguishable from no
// reason at all, which read as a PLAN rather than as a refusal.
func TestTheRefusalVocabularyIsClosed(t *testing.T) {
	// Every member of the vocabulary is honoured, and each reaches the detail
	// as itself. Enumerated so that a value dropped from the set fails here.
	for _, reason := range []RefusalReason{
		RefusalInvalidRequest, RefusalInvalidScope, RefusalStoreRegionNotAccepted,
		RefusalUnsupportedAppVersion, RefusalPolicyUnavailable,
	} {
		t.Run(string(reason), func(t *testing.T) {
			decision := Prepare(context.Background(), VerifiedPlayerPolicy{
				Plan:  validPlan(func(m map[string]any) { m["reason"] = string(reason) }),
				Scope: callerScope(), Now: fixedClock(),
			})
			if decision.Reason != ReasonResolverRefused {
				t.Fatalf("reason=%q detail=%q, want %q", decision.Reason, decision.Detail, ReasonResolverRefused)
			}
			if !strings.Contains(decision.Detail, string(reason)) {
				t.Fatalf("the detail must name the resolver's own reason: %q", decision.Detail)
			}
			assertClosedOnEveryAxis(t, decision)
		})
	}

	// And everything else is unreadable — including the two shapes that used
	// to pass: an empty string, and a string carrying a control character.
	for _, testCase := range []struct{ name, value string }{
		{"an unknown member", "teapot"},
		{"empty", ""},
		// ⚠ THE INJECTION. Decision.Detail is documented as text for a human
		// reading a log; a newline here wrote a second line into it.
		{"a newline", "bad\nforged: everything is fine"},
		{"a carriage return", "bad\rforged"},
		{"a NUL", "bad\x00forged"},
		{"an ANSI escape", "bad\x1b[2Kforged"},
		// Case is not the vocabulary either.
		{"the right member in the wrong case", "INVALID_SCOPE"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			decision := Prepare(context.Background(), VerifiedPlayerPolicy{
				Plan:  validPlan(func(m map[string]any) { m["reason"] = testCase.value }),
				Scope: callerScope(), Now: fixedClock(),
			})
			if decision.Reason != ReasonPlanUnreadable {
				t.Fatalf("reason=%q detail=%q, want %q", decision.Reason, decision.Detail, ReasonPlanUnreadable)
			}
			// ⚠ AND NOTHING FROM THE UNVALIDATED BODY REACHED THE LOG FIELD.
			// The refusal names the offending value, but a refusal is not the
			// "the resolver refused: X" line that a caller reads as a genuine
			// verdict — and the raw bytes must not appear verbatim.
			if strings.Contains(decision.Detail, "the resolver refused") {
				t.Fatalf("an invalid reason was logged as a genuine refusal: %q", decision.Detail)
			}
			for _, forbidden := range []string{"\n", "\r", "\x00", "\x1b"} {
				if strings.Contains(decision.Detail, forbidden) {
					t.Fatalf("a control character from the body reached the log field: %q", decision.Detail)
				}
			}
			assertClosedOnEveryAxis(t, decision)
		})
	}

	// The control: a body with NO reason key is a plan, not a refusal.
	if got := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: validPlan(nil), Scope: callerScope(), Now: fixedClock(),
	}); got.Reason == ReasonResolverRefused {
		t.Fatal("a body carrying no reason must not be read as a refusal")
	}
}

// ⚠ A CLONED PLAN MUST MARSHAL BACK TO WHAT THE RESOLVER SENT. clonePlan
// append()-ed onto a nil slice, and appending zero elements to nil returns
// NIL — so the resolver's own `operation_blocks: []` came back from a clone as
// `null`. A round trip through this package produced a body this package now
// REFUSES on the way in, which is the contradiction this scene exists to
// prevent.
func TestACloneMarshalsBackToTheContract(t *testing.T) {
	plan, err := ParsePlan(goldenWire(t, "consent-policy-resolved"))
	if err != nil {
		t.Fatalf("the golden must parse: %v", err)
	}
	encoded, err := json.Marshal(clonePlan(plan))
	if err != nil {
		t.Fatalf("marshal the clone: %v", err)
	}
	var round map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &round); err != nil {
		t.Fatal(err)
	}
	// The two members whose empty form the bug rewrote, compared as BYTES
	// against the contract's spelling rather than through a decode — `null`
	// and `[]` decode to the same nil, which is exactly what hid this.
	var flags map[string]json.RawMessage
	if err := json.Unmarshal(round["flags"], &flags); err != nil {
		t.Fatal(err)
	}
	if got := string(flags["operation_blocks"]); got != "[]" {
		t.Fatalf("a cloned plan marshals operation_blocks as %s, the contract says []", got)
	}

	// And the same question of an EMPTY signals_used, which the golden does
	// not carry — so it is built here rather than left untested.
	empty := clonePlan(Plan{})
	if empty.Flags.OperationBlocks == nil || empty.SignalsUsed == nil {
		t.Fatalf("a clone must not produce nil containers: blocks=%v signals=%v",
			empty.Flags.OperationBlocks, empty.SignalsUsed)
	}

	// ⚠ AND THE ROUND TRIP IS FED BACK IN. The strongest form of this claim is
	// not "the bytes look right" but "this package accepts what it produced".
	var reparsed map[string]any
	if err := json.Unmarshal(encoded, &reparsed); err != nil {
		t.Fatal(err)
	}
	reparsed["expires_at"] = time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339)
	again, err := json.Marshal(reparsed)
	if err != nil {
		t.Fatal(err)
	}
	if decision := Prepare(context.Background(), VerifiedPlayerPolicy{
		Plan: again, Scope: callerScope(), Now: fixedClock(),
	}); decision.Reason != ReasonPlanUnsigned {
		t.Fatalf("this package refused a body it produced itself: reason=%q detail=%q",
			decision.Reason, decision.Detail)
	}
}
