package consentpolicy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Regime is the effective consent class a plan carries.
type Regime string

const (
	// StrictOptIn: optional device/client analytics and its non-exempt device
	// access stay off until a valid, purpose-specific explicit grant.
	StrictOptIn Regime = "STRICT_OPT_IN"
	// SoftOptOut: the reviewed notice-and-objection structure. Parsed so a
	// future plan is readable; NOT reachable from the resolver's initial
	// release, and never produced by a fallback.
	SoftOptOut Regime = "SOFT_OPT_OUT"
	// Unknown resolves to strict with optional processing closed. Missing
	// policy, invalid signature or schema, expired evidence and lookup errors
	// all land here rather than selecting SOFT.
	Unknown Regime = "UNKNOWN"
)

func (r Regime) known() bool {
	return r == StrictOptIn || r == SoftOptOut || r == Unknown
}

// CrashProfile is the crash lane's own decision. It never inherits an analytics
// permission. The platform decision is that crash reports for minors are off
// and that an under-threshold or provisional client does not initialise the
// crash reporter at all — so this is read BEFORE a crash client is created.
type CrashProfile string

const (
	CrashOff                         CrashProfile = "off"
	CrashMinimalDiagnosticsForMinors CrashProfile = "minimal_diagnostics_for_minors"
)

func (p CrashProfile) known() bool {
	return p == CrashOff || p == CrashMinimalDiagnosticsForMinors
}

// ServerAnalyticsState is the backend lane's separate state. It is a BASIS plus
// an objection requirement, never an in-game toggle: the platform decision
// withdrew the in-game privacy rows for it and made the objection route manual
// — the rights page or the privacy address — answered within one month.
type ServerAnalyticsState string

const (
	// ONE VALUE, and the schema says one value: no path in the resolver
	// assigns another while SOFT is unreachable. A second member here would
	// be this package inventing a state the wire cannot carry.
	ServerAnalyticsDenied ServerAnalyticsState = "denied"
)

// ChildRules is the minimised-handling flag.
type ChildRules string

// One value, for the same reason as ServerAnalyticsDenied.
const ChildRulesMinimised ChildRules = "minimised"

func (c ChildRules) known() bool { return c == ChildRulesMinimised }

// BasisCharacter says what KIND of answer a plan is.
type BasisCharacter string

const BasisInformationalReference BasisCharacter = "informational_reference"

func (b BasisCharacter) known() bool { return b == BasisInformationalReference }

// TableProvenance says where the jurisdiction reading behind a plan came from.
type TableProvenance string

const (
	ProvenanceAIDraft       TableProvenance = "ai_draft"
	ProvenanceOwnerAccepted TableProvenance = "owner_accepted"
)

func (t TableProvenance) known() bool {
	return t == ProvenanceAIDraft || t == ProvenanceOwnerAccepted
}

func (s ServerAnalyticsState) known() bool {
	return s == ServerAnalyticsDenied
}

// SignalReason is the per-signal availability vocabulary. It is NOT the
// top-level error vocabulary, and conflating the two was a real mistake in an
// earlier draft of this work: source_not_permitted describes one signal inside
// signals_used, not a failed request.
type SignalReason string

const (
	SourceNotPermitted  SignalReason = "source_not_permitted"
	SourceUnavailable   SignalReason = "source_unavailable"
	NotEnabledInRelease SignalReason = "not_enabled_in_release"
)

func (s SignalReason) known() bool {
	return s == SourceNotPermitted || s == SourceUnavailable || s == NotEnabledInRelease
}

// Scope is the canonical tuple a plan is issued for. A bare workspace cannot
// select per-app overrides, so all three are required and all three are
// compared.
type Scope struct {
	WorkspaceID   string `json:"workspace_id"`
	AppID         string `json:"app_id"`
	EnvironmentID string `json:"environment_id"`
}

func (s Scope) equal(other Scope) bool {
	return s.WorkspaceID == other.WorkspaceID && s.AppID == other.AppID &&
		s.EnvironmentID == other.EnvironmentID
}

func (s Scope) complete() bool {
	return s.WorkspaceID != "" && s.AppID != "" && s.EnvironmentID != ""
}

// Signal is one entry of signals_used: what the resolver was able to read, or
// the honest reason it could not. The resolver's initial release performs no
// geolocation, so every signal arrives unavailable with a reason — a plan
// carrying no country is VALID and must not be treated as an error.
type Signal struct {
	Name string `json:"name"`
	// A POINTER FOR THE SAME REASON server_analytics_objection_required IS ONE:
	// absence is not false. Decoded into a plain bool, a signal that never
	// states `available` reads as UNAVAILABLE — an answer the resolver did not
	// give, invented by the decoder's zero value. It happens to be the
	// conservative side of that one field, which is exactly why it went
	// unnoticed: the provenance record then says the source was unavailable
	// when what is true is that the plan never said.
	Available *bool        `json:"available"`
	Reason    SignalReason `json:"reason,omitempty"`
}

// Plan is the resolver's response, as the SDK reads it.
// Flags are the plan's orthogonal restrictions.
//
// ⚠ NESTED, AND THAT IS NOT A DETAIL. This package read crash_profile and
// server_analytics at the TOP LEVEL, in upper case, beside fields the resolver
// has never sent. It could not read one real response — and it was harmless
// only because this release refuses every unsigned plan anyway, which is
// exactly how the same defect hid in the sibling SDK for fifteen rounds. The
// schema is the contract; this is the schema.
type Flags struct {
	CrashProfile    CrashProfile         `json:"crash_profile"`
	ServerAnalytics ServerAnalyticsState `json:"server_analytics"`
	ChildRules      ChildRules           `json:"child_rules"`
	// Always present and never null: `[]` when there is nothing to block.
	OperationBlocks []string `json:"operation_blocks"`
}

// Basis says what kind of answer the plan is, and carries the notice every
// carrier must show.
type Basis struct {
	Character       BasisCharacter  `json:"character"`
	TableProvenance TableProvenance `json:"table_provenance"`
	Notice          string          `json:"notice"`
}

// Plan is the resolver's response, exactly as it is sent.
//
// EVERY FIELD BELOW IS PRESENT ON EVERY RESPONSE, refusals included, except
// `reason` — which is present ONLY on a refusal and is the discriminator.
type Plan struct {
	Regime                Regime   `json:"regime"`
	Flags                 Flags    `json:"flags"`
	PolicyVersion         string   `json:"policy_version"`
	ConsentTextVersion    string   `json:"consent_text_version"`
	PresentedLanguage     string   `json:"presented_language"`
	Scope                 Scope    `json:"scope"`
	SignalsUsed           []Signal `json:"signals_used"`
	BandVocabulary        string   `json:"band_vocabulary"`
	BandVocabularyVersion string   `json:"band_vocabulary_version"`
	ExpiresAt             string   `json:"expires_at"`
	// A POINTER BECAUSE ABSENCE IS NOT ZERO. Decoded into an int, a plan that
	// omits this key reads as 0 — indistinguishable from a resolver that chose
	// 0, and the presence check below is what makes an omission a refusal.
	MaxAgeSeconds *int `json:"max_age_seconds"`
	// ⚠ THE ONE NULLABLE KEY, AND PRESENT-NULL IS THE ONLY ADMISSIBLE STATE IN
	// THIS RELEASE. The key is on every response; its value is null while no
	// signature exists. A pointer distinguishes null from a value; the
	// required-key walk distinguishes null from ABSENT, which a pointer alone
	// cannot. A non-null signature is refused here rather than honoured,
	// because this package holds no verification key — see
	// verificationKeyAvailable, and the paragraph there about the two safety
	// arguments that were tried and failed.
	Signature *string `json:"signature"`
	Basis     Basis   `json:"basis"`
	// ⚠ PRESENT ONLY ON A REFUSAL, AND IT IS THE DISCRIMINATOR — not the HTTP
	// status, which is 400 for four reasons and 200 for policy_unavailable. A
	// body carrying a reason is the strict fallback with the reason surfaced:
	// its scope is three empty strings and must not be compared, and it is
	// never usable as a permissive plan.
	Reason string `json:"reason,omitempty"`
}

// The bounds are the resolver's, mirrored here so a malformed plan is refused
// before it can reach a verdict rather than passed through.
const (
	maxVersionBytes  = 64
	maxLanguageBytes = 35
	maxBandBytes     = 32
	maxListEntries   = 64
	maxEntryBytes    = 64
	maxSignals       = 16
	maxNoticeBytes   = 2048
	maxPlanBytes     = 16 << 10
)

// ⚠ THE SOLIDUS IS IN THE SET BECAUSE THE RESOLVER SENDS IT. Every version
// on a real response is `strict-fallback/1`, and this pattern — invented here
// from the prose rather than read off a response — refused it. The SDK would
// have rejected every strict-fallback plan the resolver has ever issued as
// "characters outside the permitted set", which is the whole defect this
// change exists for, in one regular expression.
//
// It stays a closed set rather than becoming permissive: these values are
// echoed into logs and provenance records, so whitespace, quotes and control
// characters remain out.
var versionPattern = regexp.MustCompile(`^[A-Za-z0-9._+/-]+$`)

// ErrPlanAbsent is returned when there is no plan at all to verify. It is
// separated from a malformed one because the caller's remedy differs: one is a
// missing handoff, the other is a bad one.
var ErrPlanAbsent = errors.New("consentpolicy: no plan supplied")

// ParsePlan reads a plan from the bytes the trusted game flow delivered.
//
// ⚠ IT REFUSES RATHER THAN REPAIRS. Unknown fields are rejected, every bounded
// field is checked against its bound, and every enum must be a known member —
// because a plan the SDK cannot fully read is a plan it cannot act on, and
// "read the parts I understand" is how a permissive default gets in.
func ParsePlan(raw []byte) (Plan, error) {
	var plan Plan
	if len(raw) == 0 {
		return Plan{}, ErrPlanAbsent
	}
	if len(raw) > maxPlanBytes {
		return Plan{}, fmt.Errorf("consentpolicy: plan is %d bytes, over the %d-byte bound", len(raw), maxPlanBytes)
	}
	// ⚠ AND THE BYTES ARE CHECKED BEFORE THE KEYS. encoding/json does not
	// refuse invalid UTF-8 inside a string, it SUBSTITUTES U+FFFD for each bad
	// byte — so a plan whose operation_blocks entry carries a stray 0x80
	// decodes to a different, well-formed string and is then compared,
	// bounded and returned as though the resolver had sent it. A replacement
	// character is a repair, and this parser refuses rather than repairs.
	if !utf8.Valid(raw) {
		return Plan{}, errors.New("consentpolicy: the plan is not valid UTF-8")
	}
	// ⚠ THE KEYS ARE CHECKED BEFORE THE DECODE, because encoding/json is
	// case-INSENSITIVE and lets a later duplicate win. A plan carrying
	// "regime" and "REGIME" decodes to whichever came last: the schema name
	// never appeared twice, so DisallowUnknownFields sees nothing wrong, and a
	// permissive spelling silently wins. Exact case, no duplicates, or the
	// plan is unreadable.
	if err := checkObjectKeys(raw); err != nil {
		return Plan{}, err
	}
	// ⚠ AND EVERY REQUIRED KEY MUST BE PRESENT, which the decode cannot ask.
	// A missing key decodes to a zero value, and a zero value is a permissive
	// answer the resolver never sent: an absent `signature` is indistinguishable
	// from the null one this release requires, an absent `max_age_seconds` is
	// 0, an absent `flags` is every flag at its zero string. The contract says
	// every field below is on every response, so an omission is a refusal that
	// NAMES the key rather than a plan with a hole in it.
	if err := checkRequiredKeys(raw); err != nil {
		return Plan{}, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return Plan{}, fmt.Errorf("consentpolicy: unreadable plan: %w", err)
	}
	// ⚠ AND More() IS NOT A TRAILING-CONTENT CHECK. It reports whether another
	// element follows INSIDE the current array or object, so it answers false
	// at a stray "]" or "}" and garbage after a complete plan went unnoticed.
	// Decoding once more and requiring io.EOF is the question that was meant.
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Plan{}, errors.New("consentpolicy: trailing content after the plan")
	}
	if err := plan.validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

// schemaKeys maps every struct type reachable from Plan to that type's exact
// schema spellings, and to the Go type behind each name so the walk below
// knows what the value is meant to be. It is DERIVED from the struct tags
// rather than retyped, so a field added above cannot be forgotten here — and
// a NESTED object added above cannot silently stop being checked.
var schemaKeys = buildSchemaKeys(reflect.TypeOf(Plan{}))

func buildSchemaKeys(root reflect.Type) map[reflect.Type]map[string]reflect.Type {
	out := make(map[reflect.Type]map[string]reflect.Type)
	var walk func(reflect.Type)
	walk = func(t reflect.Type) {
		for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
			t = t.Elem()
		}
		// Recording the type BEFORE descending is what makes a self-referential
		// schema terminate rather than recurse forever.
		if t.Kind() != reflect.Struct || out[t] != nil {
			return
		}
		keys := make(map[string]reflect.Type)
		out[t] = keys
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name == "" || name == "-" {
				continue
			}
			keys[name] = field.Type
			walk(field.Type)
		}
	}
	walk(root)
	return out
}

// checkObjectKeys refuses any name that is not the exact schema spelling, and
// any duplicate — AT EVERY DEPTH, not only at the top.
//
// ⚠ THE TOP-LEVEL-ONLY WALK WAS NOT A SMALLER VERSION OF THIS, IT WAS A HOLE.
// It decoded each value wholesale as json.RawMessage, so encoding/json got the
// nested ambiguity untouched and its case-insensitive, last-one-wins matching
// decided it: {"scope":{"workspace_id":"other","WORKSPACE_ID":"expected"}}
// decoded to the EXPECTED workspace and then passed the scope comparison —
// a plan issued for another app admitting here, which is the one thing the
// scope comparison exists to stop.
func checkObjectKeys(raw []byte) error {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("consentpolicy: unreadable plan: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return errors.New("consentpolicy: the plan is not a JSON object")
	}
	return checkObjectBody(decoder, reflect.TypeOf(Plan{}), "the plan")
}

// checkObjectBody walks one object's keys, the opening brace already consumed.
func checkObjectBody(decoder *json.Decoder, t reflect.Type, where string) error {
	var keys map[string]reflect.Type
	if t != nil {
		keys = schemaKeys[t]
	}
	seen := make(map[string]bool)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("consentpolicy: unreadable plan: %w", err)
		}
		key, ok := token.(string)
		if !ok {
			return fmt.Errorf("consentpolicy: a key of %s is not a string", where)
		}
		if keys != nil {
			if _, allowed := keys[key]; !allowed {
				return fmt.Errorf("consentpolicy: %s carries the key %q, which is not a schema name "+
					"in its exact spelling", where, key)
			}
		}
		if seen[key] {
			return fmt.Errorf("consentpolicy: %s carries the key %q twice", where, key)
		}
		seen[key] = true
		if err := checkValueKeys(decoder, keys[key], where+"'s "+key); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil { // the closing brace
		return fmt.Errorf("consentpolicy: unreadable plan: %w", err)
	}
	return nil
}

// checkValueKeys walks ONE value of the schema type t, which may be nil when
// the schema does not describe it. A scalar carries no keys to confuse, so it
// is consumed and left to the strict decode to judge.
func checkValueKeys(decoder *json.Decoder, t reflect.Type, where string) error {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("consentpolicy: unreadable plan: %w", err)
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		return checkObjectBody(decoder, t, where)
	case '[':
		var elem reflect.Type
		if t != nil && (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) {
			elem = t.Elem()
		}
		for decoder.More() {
			if err := checkValueKeys(decoder, elem, "an entry of "+where); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil { // the closing bracket
			return fmt.Errorf("consentpolicy: unreadable plan: %w", err)
		}
		return nil
	}
	return nil
}

func (p Plan) validate() error {
	if !p.Regime.known() {
		return fmt.Errorf("consentpolicy: unknown regime %q", string(p.Regime))
	}
	if !p.Flags.CrashProfile.known() {
		return fmt.Errorf("consentpolicy: unknown flags.crash_profile %q", string(p.Flags.CrashProfile))
	}
	if !p.Flags.ServerAnalytics.known() {
		return fmt.Errorf("consentpolicy: unknown flags.server_analytics %q", string(p.Flags.ServerAnalytics))
	}
	if !p.Flags.ChildRules.known() {
		return fmt.Errorf("consentpolicy: unknown flags.child_rules %q", string(p.Flags.ChildRules))
	}
	if !p.Basis.Character.known() {
		return fmt.Errorf("consentpolicy: unknown basis.character %q", string(p.Basis.Character))
	}
	if !p.Basis.TableProvenance.known() {
		return fmt.Errorf("consentpolicy: unknown basis.table_provenance %q", string(p.Basis.TableProvenance))
	}
	if p.Basis.Notice == "" || len(p.Basis.Notice) > maxNoticeBytes {
		return fmt.Errorf("consentpolicy: basis.notice is empty or over the %d-byte bound", maxNoticeBytes)
	}
	// ⚠ A REFUSAL CARRIES THREE EMPTY SCOPE STRINGS BY CONTRACT, so
	// completeness is asked only of a body that is not one. Requiring it
	// unconditionally made every refusal the resolver sends unparseable —
	// reported as `plan_unreadable`, which says "I could not read the answer"
	// when the truth is "the answer was a refusal, and here is why". The KEYS
	// are still required on both: the required-key walk runs before this and
	// does not care what the values are.
	if p.Reason == "" && !p.Scope.complete() {
		return errors.New("consentpolicy: the plan names no complete scope tuple")
	}
	for _, field := range []struct {
		name  string
		value string
		bound int
	}{
		{"policy_version", p.PolicyVersion, maxVersionBytes},
		{"consent_text_version", p.ConsentTextVersion, maxVersionBytes},
		{"band_vocabulary", p.BandVocabulary, maxBandBytes},
		{"band_vocabulary_version", p.BandVocabularyVersion, maxBandBytes},
	} {
		if field.value == "" {
			return fmt.Errorf("consentpolicy: %s is empty", field.name)
		}
		if len(field.value) > field.bound {
			return fmt.Errorf("consentpolicy: %s is %d bytes, over the %d-byte bound",
				field.name, len(field.value), field.bound)
		}
		if !versionPattern.MatchString(field.value) {
			return fmt.Errorf("consentpolicy: %s carries characters outside the permitted set", field.name)
		}
	}
	// presented_language must NAME the actual supported text: it is not an echo
	// of the requested locale, so it is bounded but not pattern-matched to one.
	if p.PresentedLanguage == "" || len(p.PresentedLanguage) > maxLanguageBytes {
		return fmt.Errorf("consentpolicy: presented_language is empty or over the %d-byte bound", maxLanguageBytes)
	}
	if len(p.SignalsUsed) > maxSignals {
		return fmt.Errorf("consentpolicy: %d signals, over the %d bound", len(p.SignalsUsed), maxSignals)
	}
	for _, signal := range p.SignalsUsed {
		if signal.Name == "" || len(signal.Name) > maxEntryBytes {
			return errors.New("consentpolicy: a signal name is empty or over its bound")
		}
		// A signal that never STATES its availability is unreadable, not
		// unavailable. Inventing the answer here would put a fact in the
		// provenance record that the resolver never asserted.
		if signal.Available == nil {
			return fmt.Errorf("consentpolicy: signal %q does not state whether it was available", signal.Name)
		}
		// An UNAVAILABLE signal must say WHY, from the closed vocabulary. A
		// bare "not available" is the shape that hides a prohibited source.
		if !*signal.Available && !signal.Reason.known() {
			return fmt.Errorf("consentpolicy: signal %q is unavailable with no known reason", signal.Name)
		}
		if *signal.Available && signal.Reason != "" {
			return fmt.Errorf("consentpolicy: signal %q is available and carries a reason", signal.Name)
		}
	}
	for _, list := range []struct {
		name    string
		entries []string
	}{
		{"flags.operation_blocks", p.Flags.OperationBlocks},
	} {
		if len(list.entries) > maxListEntries {
			return fmt.Errorf("consentpolicy: %s has %d entries, over the %d bound",
				list.name, len(list.entries), maxListEntries)
		}
		for _, entry := range list.entries {
			if entry == "" || len(entry) > maxEntryBytes {
				return fmt.Errorf("consentpolicy: an entry of %s is empty or over its bound", list.name)
			}
		}
	}
	// ⚠ 0 IS A LEGAL VALUE HERE AND MEANS SOMETHING ELSEWHERE: it is the
	// client SDKs' "do not reuse this" for their private caches. This package
	// holds no cache — one plan, one operation, no memo — so nothing acts on
	// it; it is carried and bounded, and an ABSENT one is unreadable.
	if p.MaxAgeSeconds == nil {
		return errors.New("consentpolicy: the plan does not state max_age_seconds")
	}
	if *p.MaxAgeSeconds < 0 {
		return errors.New("consentpolicy: max_age_seconds is negative")
	}
	if _, err := p.expiry(); err != nil {
		return err
	}
	return nil
}

// expiry parses expires_at, which is an absolute RFC 3339 UTC instant. It is
// required: a plan with no expiry is a plan that never goes stale, which is not
// a thing this contract has.
func (p Plan) expiry() (time.Time, error) {
	if p.ExpiresAt == "" {
		return time.Time{}, errors.New("consentpolicy: the plan carries no expires_at")
	}
	parsed, err := time.Parse(time.RFC3339, p.ExpiresAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("consentpolicy: expires_at is not an RFC 3339 instant: %w", err)
	}
	return parsed.UTC(), nil
}

// requiredKeys maps every struct type reachable from Plan to the schema
// spellings that must be PRESENT on the wire.
//
// ⚠ DERIVED FROM THE TAGS, AND `omitempty` IS THE MARKER. Every field the
// resolver sends on every response is required; the one field that is present
// only sometimes — `reason`, the refusal discriminator — carries `omitempty`
// and is the only optional entry. So a field added to Plan is required by
// default, which is the direction that fails closed, and making one optional
// takes a deliberate tag rather than a forgotten line in a list.
var requiredKeys = buildRequiredKeys(reflect.TypeOf(Plan{}))

func buildRequiredKeys(root reflect.Type) map[reflect.Type]map[string]struct{} {
	out := make(map[reflect.Type]map[string]struct{})
	var walk func(t reflect.Type)
	walk = func(t reflect.Type) {
		for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct {
			return
		}
		if _, done := out[t]; done {
			return
		}
		names := make(map[string]struct{})
		out[t] = names
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			tag := field.Tag.Get("json")
			if tag == "" || tag == "-" {
				continue
			}
			parts := strings.Split(tag, ",")
			optional := false
			for _, opt := range parts[1:] {
				if opt == "omitempty" {
					optional = true
				}
			}
			if !optional {
				names[parts[0]] = struct{}{}
			}
			walk(field.Type)
		}
	}
	walk(root)
	return out
}

// checkRequiredKeys refuses a body that omits any required key, naming it.
func checkRequiredKeys(raw []byte) error {
	return requiredKeysIn(raw, reflect.TypeOf(Plan{}), "")
}

func requiredKeysIn(raw []byte, t reflect.Type, where string) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		var elements []json.RawMessage
		if err := json.Unmarshal(raw, &elements); err != nil {
			return nil // shape errors are the decode's to report
		}
		for i, element := range elements {
			if err := requiredKeysIn(element, t.Elem(), fmt.Sprintf("%s[%d]", where, i)); err != nil {
				return err
			}
		}
		return nil
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil
	}
	for name := range requiredKeys[t] {
		if _, present := object[name]; !present {
			return fmt.Errorf("consentpolicy: the plan omits the required key %q", strings.TrimPrefix(where+"."+name, "."))
		}
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := strings.Split(field.Tag.Get("json"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		value, present := object[tag]
		if !present || string(value) == "null" {
			continue
		}
		if err := requiredKeysIn(value, field.Type, strings.TrimPrefix(where+"."+tag, ".")); err != nil {
			return err
		}
	}
	return nil
}
