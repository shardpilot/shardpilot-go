package shardpilot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
)

// Experiment-assignment consumer (ADR-0259 SDK leg): GETs the server-evaluated
// assignment for one (app, environment, experiment, subject) tuple from the
// control-plane assignment endpoint and serves the assigned variant to the
// host, with a durable last-known-good cache, periodic revalidation (the
// SDK-side kill-switch reach), and an exposure-fact lane riding the normal
// analytics pipeline. Deliberately separate from remote_config.go (a
// different endpoint with different fail-closed rules) but mirroring its
// transport discipline: publishable-key bearer auth, injective escaping,
// scope-stamped cache with corrupt = miss, per-key sequence fencing for
// out-of-order responses, and the shared redirect-refusing bounded-read GET.
//
// DARK BY DEFAULT. This machinery is constructed only when the config sets
// `ExperimentsEnabled = true`; the flag defaults to false and while it is off
// ZERO experiment code paths execute — no subject-id mint, no assignment
// fetch, no revalidation goroutine, no exposure emit, no new persistence
// keys, no reads of previously persisted ones. The server side is equally
// dark: while the platform flags are off the endpoint answers 403, which
// this client treats fail-closed exactly like bad auth. Flipping the SDK
// flag on for real traffic is governed by the platform flag-flip registry
// preconditions, not by this module.
//
// Wire contract (assignment fetch):
//
//	GET {RemoteConfigURL}/api/v1/runtime/experiments/assignment
//	  ?app_key=&environment_key=&experiment_key=&subject_key=&<attributes>
//	Authorization: Bearer <publishable APIKey>
//
// The base URL is the configured RemoteConfigURL — the control-plane host —
// with the path swapped; no new endpoint configuration exists. The endpoint
// requires the experiment-assignment read scope on the key, granted
// server-side.
//
// Outcomes (decided by applyExperimentAssignment, pure):
//   - 200 assigned — the variant is served and cached (memory + durable).
//     Assignment stickiness is entirely the server's deterministic hash; the
//     cache is a latency/offline device, never an assignment authority, and
//     this client never re-buckets locally.
//   - 200 not-assigned — three shapes distinguished only by `reason`: absent
//     (deterministic traffic-gate miss), "targeting_unmatched" (may change
//     when attributes change), "kill_switch" (operator kill). Any OTHER
//     reason is not a verdict this SDK can represent and classifies as
//     malformed. All three shapes drop the cached assignment; a kill in
//     particular must stop applying at the next safe point and emits no
//     exposure.
//   - 401/403 — fail CLOSED: the result never serves a cached assignment,
//     in-memory serving stops (getters return nothing) and revalidation
//     halts until re-init or a later successful, authorized fetch. The
//     durable cache record itself is left untouched (remote-config parity)
//     EXCEPT for the server's real-subjects kill sentinel ("experiment
//     real-subject assignment is disabled"), which additionally drops the
//     durable record — the platform flipped the real-subjects flag back
//     off, and the cached assignments plus their subject-fact keys must not
//     outlive that.
//   - 404 — permanent for the experiment: treated as not-assigned, the
//     cached assignment is dropped and never served stale, and revalidation
//     stops asking for that key (the drop removes it from the cache).
//   - 400 — permanent for this input set. One special case: the subject-id
//     grammar sentinel with an SDK-minted subject id re-mints the id ONCE
//     per process and retries with the EXACT normalized attribute set of the
//     rejected request (a conforming mint that still 400s is a bug, surfaced
//     through diagnostics, never a retry loop). Every other 400 drops the
//     cached entry: a permanently rejected input set must not serve stale
//     forever while revalidation re-sends it.
//   - 503 / 429 / 5xx / offline / timeout / malformed — transient: the
//     cached assignment is served (FromCache=true with Code carrying the
//     reason) and the revalidation cadence backs off; Retry-After is honored
//     on 429 AND 5xx exactly like the batch transport. An offline client
//     keeps its last-known-good variant indefinitely — the documented
//     kill-latency caveat.
//
// Subject id (`spcid_`): SDK-minted and SDK-managed — "spcid_" plus the 32
// lowercase hex chars of a UUIDv7 with the dashes removed. There is NO host
// override path: no public setter and no config field is read for it. It is
// persisted in the SpoolDir state directory (in memory only, per process,
// when SpoolDir is unset — documented ephemeral re-bucketing), minted lazily
// the first time a fetch needs it, validated against the wire grammar on
// load (the FULL grammar, so an id from another SDK build stays sticky), and
// re-minted only on storage loss/corruption or the server's grammar
// sentinel. It is NOT the anonymous id, and it egresses ONLY as the
// assignment fetch's subject_key — never in analytics events, in any props,
// or as an envelope identity.
//
// Consent posture (assignment plane): the plane consumes the SAME effective
// consent state the analytics path uses — nothing separate is computed.
// While that state refuses analytics (denied, either flavor — the forced
// minor state included — or unknown under the opt-in ConsentFloor) no
// assignment request leaves the process, no subject id is minted, nothing
// is served (getters return nothing) and the revalidation lane does not
// run: a forced-minor session produces ZERO experiment traffic on both
// planes. Without the floor this SDK's documented open-under-unknown
// posture applies to this plane too: consent unknown ADMITS, denial
// refuses. This is deliberately stricter than the remote-config fetch
// (which is not consent-gated). A consent downgrade mid-session stops
// fetching and serving; the durable cache record is retained but not
// served, and a later re-grant serves it again.
//
// Exposure lane (analytics plane): `experiment_exposure` facts ride the
// normal event pipeline (queue → batch → spool → consent gates) with the
// strict server-side props allowlist. Emission timing is the ratified SDK
// convention: at most once per (experiment_key, experiment_version, subject)
// per session — this SDK has no session lifecycle, so its session is the
// client instance — emitted when the assigned variant is first applied (a
// fresh fetch resolution, or the first sweep serving a cache-restored
// assignment), with a DETERMINISTIC event id so at-least-once retries and
// same-session re-emissions collapse server-side as duplicates.
// TrackExperimentExposure is the explicit re-arm escape hatch (a re-arm
// mints a distinct deterministic id). The assignment_key prop carries the
// server-minted subject-fact key VERBATIM (the raw subject id is
// structurally rejected there); an assignment without one emits NO fact.
// NOTE: the analytics service currently rejects these event names from
// publishable client keys by design; until the platform's producer-lane
// decision lands, an emitted exposure is expected to come back as a
// per-event reject. That server-side block is load-bearing and this SDK
// deliberately relies on it staying authoritative — the lane is dark
// end-to-end.

const expAssignmentRoute = "/api/v1/runtime/experiments/assignment"

// Revalidation cadence (the SDK's contribution to the kill-switch reach):
// re-issue the assignment GET for every cached entry, batched per cycle,
// every 300 seconds with ±10% uniform jitter. The endpoint has no
// conditional requests (no ETag), so revalidation is a plain re-fetch.
const (
	expRevalidateIntervalSeconds = 300
	expRevalidateJitter          = 0.1
)

// Transient-failure backoff for the revalidation cadence (transport parity
// with the batch path): full jitter in [base, ceiling], ceiling doubling per
// consecutive failure up to the cap; a server Retry-After (clamped to one
// day) overrides the computed wait.
const (
	expBackoffBaseSeconds = 1
	expBackoffCapSeconds  = 60
	expMaxDeferSeconds    = 86400
)

// Server error sentinels this client reacts to by exact body text (the error
// contract distinguishes same-status outcomes only by the body's `error`
// string). String equality on a fully read body only; an unparseable,
// truncated, or near-miss body is the generic outcome for its status.
const (
	expSentinelRealSubjectsDisabled = "experiment real-subject assignment is disabled"
	expSentinelSubjectGrammar       = "experiment metadata must use synthetic local-safe identifiers only"
)

// The server-evaluated targeting attribute vocabulary: the fixed allowlist
// plus the custom_attribute_<name> family (suffix 1-64 chars). Names outside
// it are never sent; values are trimmed and bounded to 512 bytes, and at
// most 64 attributes ride one fetch (sorted-name order, matching the
// server's own consideration order).
var expAllowedAttributes = map[string]bool{
	"geo":          true,
	"app_version":  true,
	"device_type":  true,
	"install_date": true,
	"user_segment": true,
}

const (
	expCustomAttributePrefix   = "custom_attribute_"
	expMaxAttributeValueBytes  = 512
	expMaxAttributes           = 64
	expMaxCustomAttributeName  = 64
	expMaxOwedExposures        = 8
	expMaxBodyBytes            = rcMaxBodyBytes
	expMaxRecordBytes          = 393216 // canon parity with the defold store clamp
	expSubjectFileName         = "experiment_subject_key"
	expSubjectReadLimit        = 1024
	expTombstoneFileName       = "experiments.condemned"
	expTombstoneReadLimit      = 8192
	expCacheFileName           = "experiments.json"
	experimentExposureName     = "experiment_exposure"
	experimentOutcomeName      = "experiment_outcome"
	experimentReasonKillSwitch = "kill_switch"
	experimentReasonTargeting  = "targeting_unmatched"

	experimentAssignmentUnitSynthetic = "synthetic_subject_key"
	experimentAssignmentUnitClientID  = "client_id"
)

// expSubjectKeyPattern is the FULL wire grammar for a client subject id.
// Accepting the full grammar on load (not just this SDK's own 32-hex body)
// keeps a stored id sticky across SDK builds: re-minting re-buckets, so an
// id that is still wire-valid is never discarded.
var expSubjectKeyPattern = regexp.MustCompile(`^spcid_[A-Za-z0-9_-]{20,64}$`)

// expSubjectFactKeyPattern is the server grammar for the derived analytics
// fact subject (`sfk1_` + 64 lowercase hex). It is THE privacy boundary of
// the fact lane: only a grammar-valid subject-fact key may ever ride an
// analytics fact's assignment_key — a malformed or echoed-raw value (the
// spcid_ subject id included) is never sent.
var expSubjectFactKeyPattern = regexp.MustCompile(`^sfk1_[0-9a-f]{64}$`)

func validExperimentSubjectID(value string) bool {
	return expSubjectKeyPattern.MatchString(value)
}

// mintExperimentSubjectID mints a fresh subject id: "spcid_" + the SDK's
// UUIDv7 with dashes removed (32 lowercase hex chars, 38 chars total —
// inside the wire grammar's 20-64 body bound and charset).
func mintExperimentSubjectID() (string, error) {
	value, err := uuidv7.New()
	if err != nil {
		return "", err
	}
	return "spcid_" + strings.ReplaceAll(value, "-", ""), nil
}

// ── URL, scope, attributes ──────────────────────────────────────────────────

type expAttribute struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// buildExperimentAssignmentURL builds the assignment GET URL: the four
// required routing params in fixed order, then the normalized attributes in
// their sorted order. Names and values are percent-escaped so a value
// containing "&", "=", or "#" cannot restructure the query string.
func buildExperimentAssignmentURL(baseURL, appKey, environmentKey, experimentKey, subjectKey string, attributes []expAttribute) string {
	var b strings.Builder
	b.WriteString(strings.TrimRight(baseURL, "/"))
	b.WriteString(expAssignmentRoute)
	b.WriteByte('?')
	writePair := func(name, value string, first bool) {
		if !first {
			b.WriteByte('&')
		}
		b.WriteString(escapeExperimentQueryComponent(name))
		b.WriteByte('=')
		b.WriteString(escapeExperimentQueryComponent(value))
	}
	writePair("app_key", appKey, true)
	writePair("environment_key", environmentKey, false)
	writePair("experiment_key", experimentKey, false)
	writePair("subject_key", subjectKey, false)
	for _, attribute := range attributes {
		writePair(attribute.Name, attribute.Value, false)
	}
	return b.String()
}

// escapeExperimentQueryComponent percent-escapes everything outside the RFC
// 3986 unreserved set (injective; the shared escaping discipline with the
// remote-config URL builder — url.QueryEscape's space-as-plus would not
// round-trip through a strict decoder unambiguously).
func escapeExperimentQueryComponent(value string) string {
	return escapeRemoteConfigSegment(value)
}

// buildExperimentScope stamps a cache record with the (workspace, app,
// environment, subject, url, credential) tuple its assignments were fetched
// for. Components are escaped and joined with a separator no escaped
// component can contain, so two distinct tuples can never collide into one
// scope string. The credential rides as a non-secret fingerprint: an
// in-place API-key swap (a tenant change with everything else equal) must
// scope-miss, never serve the previous tenant's assignment. The experiment
// key is deliberately NOT part of the base scope — entries are keyed by it
// inside the record, so the full cache identity is the (scope,
// experiment_key) pair.
func buildExperimentScope(workspaceID, appID, environmentID, subjectID, baseURL, apiKeyFingerprint string) string {
	return escapeRemoteConfigSegment(workspaceID) + rcScopeSeparator +
		escapeRemoteConfigSegment(appID) + rcScopeSeparator +
		escapeRemoteConfigSegment(environmentID) + rcScopeSeparator +
		escapeRemoteConfigSegment(subjectID) + rcScopeSeparator +
		strings.TrimRight(baseURL, "/") + rcScopeSeparator +
		apiKeyFingerprint
}

// experimentAPIKeyFingerprint is a short non-secret digest of the
// publishable key for cache scoping: it identifies the credential without
// storing anything that authenticates.
func experimentAPIKeyFingerprint(apiKey string) string {
	digest := sha256.Sum256([]byte("shardpilot-exp-key|" + apiKey))
	return hex.EncodeToString(digest[:8])
}

// normalizeExperimentAttributes turns host-supplied targeting attributes
// into the ordered pairs a fetch sends: names outside the vocabulary and
// unusable values are DROPPED (counted for diagnostics), never sent — an
// invented name would be ignored server-side at best, and an oversized
// value would fail the whole fetch whenever the experiment carries a
// targeting condition. Dropping fails toward targeting_unmatched, the safe
// direction. Values are trimmed; the surviving pairs are sorted by name and
// capped at 64 (drop beyond the cap, in that same order — mirroring the
// server's sorted-key consideration).
func normalizeExperimentAttributes(attributes map[string]string) (pairs []expAttribute, dropped int) {
	if len(attributes) == 0 {
		return nil, 0
	}
	names := make([]string, 0, len(attributes))
	for name := range attributes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := strings.TrimSpace(attributes[name])
		switch {
		case !validExperimentAttributeName(name), value == "", len(value) > expMaxAttributeValueBytes:
			dropped++
		case len(pairs) >= expMaxAttributes:
			dropped++
		default:
			pairs = append(pairs, expAttribute{Name: name, Value: value})
		}
	}
	return pairs, dropped
}

func validExperimentAttributeName(name string) bool {
	if expAllowedAttributes[name] {
		return true
	}
	if strings.HasPrefix(name, expCustomAttributePrefix) {
		suffix := len(name) - len(expCustomAttributePrefix)
		return suffix >= 1 && suffix <= expMaxCustomAttributeName
	}
	return false
}

// ── verdict parsing and classification ──────────────────────────────────────

// ExperimentAssignmentResult is one fetch's usable outcome. A cache-served
// transient IS a usable outcome: the host has the last known assignment
// (FromCache=true) with Code carrying why the network could not refresh it.
type ExperimentAssignmentResult struct {
	// Assigned is the verdict. False for a traffic-gate miss (Reason
	// empty), an operator kill (Reason "kill_switch"), a targeting miss
	// (Reason "targeting_unmatched"), or an unknown experiment (Code
	// "not_found").
	Assigned bool
	// VariantKey and VariantPayload are present only when Assigned.
	VariantKey     string
	VariantPayload map[string]any
	// Version is the published experiment version the verdict was computed
	// against (0 when the server omitted it on a not-assigned shape).
	Version int64
	// Reason distinguishes the three not-assigned shapes; empty for the
	// legacy traffic-gate miss and for an assigned verdict.
	Reason string
	// Boundary is the server's machine-readable boundary block, served
	// verbatim on fresh verdicts for observability. Do not assert its
	// literal values; they change as the platform's rollout posture does.
	Boundary map[string]any
	// FromCache reports that the assignment was served from the
	// last-known-good cache over a transient failure.
	FromCache bool
	// Code is the taxonomy code when the outcome is not a fresh verdict:
	// "not_found" (unknown experiment), "superseded" (a newer outcome
	// settled while this fetch was in flight), or the transient reason of a
	// cache serve ("transient_503", "http_0", "malformed_response", ...).
	Code string
}

// expEntry is one cached assignment. Only ASSIGNED verdicts are cached;
// every not-assigned outcome drops its key.
type expEntry struct {
	AssignmentKey  string         `json:"assignment_key"`
	VariantKey     string         `json:"variant_key"`
	VariantPayload map[string]any `json:"variant_payload,omitempty"`
	Version        int64          `json:"version"`
	AssignmentUnit string         `json:"assignment_unit"`
	SubjectFactKey string         `json:"subject_fact_key,omitempty"`
	SubjectKey     string         `json:"subject_key,omitempty"`
	Attributes     []expAttribute `json:"attributes,omitempty"`
	FetchedAtMS    int64          `json:"fetched_at_ms"`
}

// expOutcome directs the stateful install after the pure classification.
type expOutcome struct {
	// authoritative settles the per-key sequence fence (a fresh 200 in any
	// shape, 401/403, 404, 400, and other permanent errors);
	// transient/cache outcomes do not, so they cannot fence off a fresh
	// response in flight.
	authoritative bool
	// newEntry — cache this assignment (exists exactly for 200 assigned).
	newEntry *expEntry
	// dropEntry / dropAll — drop the experiment's cached assignment (every
	// not-assigned shape, 404, and non-grammar 400s) / the whole durable
	// record (the real-subjects kill sentinel).
	dropEntry bool
	dropAll   bool
	// authBlocked — 401/403: stop serving, halt revalidation, fail closed.
	authBlocked bool
	// remint — the subject-grammar 400 sentinel: the caller may re-mint the
	// subject id once per process and retry.
	remint bool
	// transient — pace the revalidation backoff; retryAfterSeconds honors a
	// server Retry-After (429 and 5xx alike).
	transient         bool
	retryAfterSeconds int
	retryAfterPresent bool
	// attributes is stamped by the fetch path: the normalized set this
	// request sent, remembered on the installed entry for revalidation.
	attributes []expAttribute
}

// expAssignmentWire is the presence-aware decode shape for a 200 body.
// Wrong types anywhere in the tree — a string version, an array payload, a
// non-object boundary — fail the typed decode and classify malformed.
type expAssignmentWire struct {
	// The echoed request identity and the reason decode PRESENCE-AWARE as
	// raw JSON: an absent member is tolerated (nil), while a present member
	// of any non-string type — an explicit null included — is not this
	// contract's shape and classifies malformed (presence vs type split).
	AppKey         json.RawMessage `json:"app_key"`
	EnvironmentKey json.RawMessage `json:"environment_key"`
	ExperimentKey  json.RawMessage `json:"experiment_key"`
	// Version decodes presence-aware too: a bare *int64 receives nil for
	// BOTH an absent member and an explicit JSON null, and null must not
	// inherit absent's traffic-gate tolerance on the not-assigned shape —
	// it is present-non-number, malformed (the fleet's null-presence
	// ruling).
	Version        json.RawMessage `json:"version"`
	Assigned       *bool           `json:"assigned"`
	AssignmentKey  string          `json:"assignment_key"`
	VariantKey     string          `json:"variant_key"`
	VariantPayload map[string]any  `json:"variant_payload"`
	Reason         json.RawMessage `json:"reason"`
	SubjectFactKey string          `json:"subject_fact_key"`
	Boundary       map[string]any  `json:"boundary"`
}

// expEchoMatches validates one presence-aware echoed member: absent is
// tolerated; present must be a JSON string equal to the request's value —
// a different value OR a non-string type (null included) is malformed.
func expEchoMatches(raw json.RawMessage, want string) bool {
	if raw == nil {
		return true
	}
	// Decode through a pointer: `json.Unmarshal` of a JSON null into a bare
	// string is a silent no-op, and null is PRESENT-non-string — malformed,
	// never coerced to the absent shape.
	var value *string
	if json.Unmarshal(raw, &value) != nil || value == nil {
		return false
	}
	return *value == want
}

// expRequestScope is the request identity a 200 body must echo consistently:
// a body whose echoed app_key, environment_key, or experiment_key disagrees
// with the fetch that was just built is not this request's verdict and
// classifies malformed BEFORE install (absent echo fields are tolerated).
type expRequestScope struct {
	appKey        string
	envKey        string
	experimentKey string
}

// serveExperimentEntryOrFail serves the cached entry for a transient
// failure, or fails when none exists. A served entry is still a usable
// outcome — the host has an assignment — with FromCache=true and Code
// carrying why the network could not refresh it. Only assigned entries are
// ever cached, so a cache serve always carries a variant.
func serveExperimentEntryOrFail(entry *expEntry, code string) (ExperimentAssignmentResult, string) {
	if entry != nil {
		return ExperimentAssignmentResult{
			Assigned:       true,
			VariantKey:     entry.VariantKey,
			VariantPayload: deepCopyJSONMap(entry.VariantPayload, 0),
			Version:        entry.Version,
			FromCache:      true,
			Code:           code,
		}, ""
	}
	return ExperimentAssignmentResult{}, code
}

// experimentBodyErrorText extracts the body `error` string of a non-2xx
// answer, or "". A truncated OR over-limit body never yields a sentinel —
// the equality cannot be trusted (an over-limit read is a truncated VIEW
// even when the prefix happens to unmarshal, e.g. sentinel JSON followed by
// padding whitespace) — and reads as generic for its status, exactly the
// bound parseExperimentVerdict applies to 200 bodies.
func experimentBodyErrorText(body []byte, bodyIncomplete bool) string {
	if bodyIncomplete || len(body) > expMaxBodyBytes {
		return ""
	}
	var wire struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &wire) != nil {
		return ""
	}
	return wire.Error
}

// applyExperimentAssignment decides one fetch outcome from the transport
// response and the CURRENT cached entry for the key. Pure (no IO, no state)
// so tests can drive every branch. Returns the served result, the install
// directives, and a failure code; a non-empty failure code means the fetch
// produced no usable verdict (the result is zero). A transport-level error
// arrives here as status 0; a stalled body after an authoritative status
// arrives with the status and bodyIncomplete set, and every non-200
// classifies by STATUS alone (a truncated 401 still fails closed, a
// truncated 404 is still permanent; the sentinels are the one
// body-sensitive refinement and a truncated body is never a sentinel).
func applyExperimentAssignment(entry *expEntry, resp remoteConfigResponse, scope expRequestScope, nowMS int64) (ExperimentAssignmentResult, expOutcome, string) {
	status := resp.status

	if status == 200 {
		verdict, outcome, ok := parseExperimentVerdict(resp, scope, nowMS)
		if !ok {
			result, failure := serveExperimentEntryOrFail(entry, "malformed_response")
			return result, expOutcome{transient: true}, failureOrEmpty(result, failure)
		}
		return verdict, outcome, ""
	}

	// Unauthorized / forbidden fail CLOSED (a dark server, a revoked or
	// unscoped key, a suspended tenant — indistinguishable here, all must
	// stop serving): the cached record is never served for this outcome and
	// revalidation halts. The durable record is kept EXCEPT under the
	// real-subjects kill sentinel, which drops it — the assignments and
	// their subject-fact keys must not outlive the platform flipping that
	// flag off.
	if status == 401 || status == 403 {
		outcome := expOutcome{authoritative: true, authBlocked: true}
		if status == 403 && experimentBodyErrorText(resp.body, resp.bodyIncomplete) == expSentinelRealSubjectsDisabled {
			outcome.dropAll = true
		}
		return ExperimentAssignmentResult{}, outcome, "unauthorized"
	}

	// Unknown experiment key or nothing published in scope: permanent for
	// this key. Treated as a first-class not-assigned answer (the host's
	// actionable outcome is identical: no variant), with Code carrying the
	// diagnosis; the cached assignment is dropped, never served stale, and
	// the drop stops revalidation from re-asking.
	if status == 404 {
		return ExperimentAssignmentResult{Code: "not_found"},
			expOutcome{authoritative: true, dropEntry: true}, ""
	}

	// Bad inputs are permanent for this input set — retrying unchanged
	// cannot help. The one self-healing case: the subject-grammar sentinel
	// with an SDK-minted id means the persisted id went bad; the caller
	// re-mints once (fresh-install semantics) and retries — deliberately
	// NOT a drop: the assignment itself was never rejected, the subject id
	// was. Every OTHER 400 drops the cached entry: a cached assignment
	// whose revalidation is permanently rejected must not keep serving
	// stale forever while the cadence re-sends the same invalid input set.
	if status == 400 {
		if experimentBodyErrorText(resp.body, resp.bodyIncomplete) == expSentinelSubjectGrammar {
			return ExperimentAssignmentResult{},
				expOutcome{authoritative: true, remint: true}, "bad_request"
		}
		return ExperimentAssignmentResult{},
			expOutcome{authoritative: true, dropEntry: true}, "bad_request"
	}

	// Transient bucket: no connection, timeout, backpressure, or any server
	// error — including the 503 the endpoint answers when its kill-state
	// read fails (an explicit serve-stale-and-retry case). Retry-After is
	// honored on 429 and 5xx alike.
	transientCode := ""
	switch {
	case status == 0:
		transientCode = "http_0"
	case status == 408:
		transientCode = "transient_408"
	case status == 429 || status >= 500:
		transientCode = "transient_" + strconv.Itoa(status)
	}
	if transientCode != "" {
		outcome := expOutcome{transient: true}
		if status == 429 || status >= 500 {
			// Batch-transport parity (this plane's documented contract):
			// Retry-After parses as delta-seconds OR an HTTP-date, day
			// clamped — unlike the remote-config route's deliberately
			// digits-only parse.
			if wait, present := parseRetryAfter(resp.retryAfterRaw); present {
				outcome.retryAfterSeconds = int(wait / time.Second)
				outcome.retryAfterPresent = true
			}
		}
		result, failure := serveExperimentEntryOrFail(entry, transientCode)
		return result, outcome, failureOrEmpty(result, failure)
	}

	// Anything else (an unexpected redirect, another 4xx — statuses this
	// contract never sends) is the fleet-contract TRANSIENT class: a closed
	// transient result to the caller (serve-stale under the attribute-match
	// rule), the cache RETAINED, and the revalidation cadence kept probing —
	// a stray proxy 409 must never kill a valid cached treatment; the
	// platform speaks authoritatively only through 200 verdicts, 400s, the
	// auth statuses, and the sentinel. Two refinements distinguish the class
	// from ordinary transients: the SERVER answered, so the per-key sequence
	// fence SETTLES (authoritative for ordering — an older in-flight
	// response must not overwrite the newer observation), while the auth
	// latch never changes in EITHER direction (the install's unlatch is
	// gated on authoritative NON-transient outcomes).
	result, failure := serveExperimentEntryOrFail(entry, "http_"+strconv.Itoa(status))
	return result, expOutcome{transient: true, authoritative: true}, failureOrEmpty(result, failure)
}

func failureOrEmpty(result ExperimentAssignmentResult, failure string) string {
	if result.FromCache {
		return ""
	}
	return failure
}

// parseExperimentVerdict parses a 200 body end-to-end: complete for its
// verdict shape, echo-consistent with the request, or not a verdict at all
// (ok=false → malformed_response). Required per shape (the server contract
// always sends these):
//   - assigned: a PRESENT, POSITIVE version (published versions start at
//     1), non-empty assignment_key and variant_key, and a non-empty
//     boundary.assignment_unit;
//   - not assigned: a KNOWN reason — absent (traffic gate), kill_switch, or
//     targeting_unmatched. An unknown reason is not a shape this SDK knows.
func parseExperimentVerdict(resp remoteConfigResponse, scope expRequestScope, nowMS int64) (ExperimentAssignmentResult, expOutcome, bool) {
	if resp.bodyIncomplete || len(resp.body) > expMaxBodyBytes {
		return ExperimentAssignmentResult{}, expOutcome{}, false
	}
	var wire expAssignmentWire
	if err := json.Unmarshal(resp.body, &wire); err != nil {
		return ExperimentAssignmentResult{}, expOutcome{}, false
	}
	if wire.Assigned == nil {
		return ExperimentAssignmentResult{}, expOutcome{}, false
	}
	// Echo validation: a present-but-different — or present-but-non-string
	// — echoed identity is not this request's verdict and must not install
	// under its scope.
	if !expEchoMatches(wire.AppKey, scope.appKey) ||
		!expEchoMatches(wire.EnvironmentKey, scope.envKey) ||
		!expEchoMatches(wire.ExperimentKey, scope.experimentKey) {
		return ExperimentAssignmentResult{}, expOutcome{}, false
	}
	// Presence-aware version decode, shared by both shapes: absent is nil
	// raw; PRESENT must be a JSON integer — an explicit null (which a bare
	// *int64 decode silently collapses into the absent shape), a string, or
	// a fractional number is not a verdict this contract sends and must not
	// slip the positivity rule into the authoritative paths (null needs the
	// pointer decode: unmarshalling null into a bare int64 is a silent
	// no-op).
	var version *int64
	if wire.Version != nil {
		if json.Unmarshal(wire.Version, &version) != nil || version == nil {
			return ExperimentAssignmentResult{}, expOutcome{}, false
		}
	}

	if *wire.Assigned {
		if version == nil || *version < 1 {
			return ExperimentAssignmentResult{}, expOutcome{}, false
		}
		assignmentKey := strings.TrimSpace(wire.AssignmentKey)
		variantKey := strings.TrimSpace(wire.VariantKey)
		assignmentUnit, _ := wire.Boundary["assignment_unit"].(string)
		if assignmentKey == "" || variantKey == "" {
			return ExperimentAssignmentResult{}, expOutcome{}, false
		}
		switch assignmentUnit {
		case experimentAssignmentUnitSynthetic, experimentAssignmentUnitClientID:
		default:
			// An unknown assignment unit is not a verdict this SDK can
			// represent (it would be cached and forwarded verbatim into
			// analytics facts).
			return ExperimentAssignmentResult{}, expOutcome{}, false
		}
		subjectFactKey := strings.TrimSpace(wire.SubjectFactKey)
		if subjectFactKey != "" && !expSubjectFactKeyPattern.MatchString(subjectFactKey) {
			// A present subject-fact key MUST satisfy the sfk1_ grammar: a
			// malformed echo — the raw spcid_ subject id included — must
			// never be cached as the value the fact lane would forward
			// verbatim into analytics assignment_key props.
			return ExperimentAssignmentResult{}, expOutcome{}, false
		}
		entry := &expEntry{
			AssignmentKey:  assignmentKey,
			VariantKey:     variantKey,
			VariantPayload: deepCopyJSONMap(wire.VariantPayload, 0),
			Version:        *version,
			AssignmentUnit: assignmentUnit,
			SubjectFactKey: subjectFactKey,
			FetchedAtMS:    nowMS,
		}
		return ExperimentAssignmentResult{
			Assigned:       true,
			VariantKey:     variantKey,
			VariantPayload: deepCopyJSONMap(wire.VariantPayload, 0),
			Version:        *version,
			Boundary:       deepCopyJSONMap(wire.Boundary, 0),
		}, expOutcome{authoritative: true, newEntry: entry}, true
	}

	reason := ""
	if wire.Reason != nil {
		// Present must be a JSON string (presence vs type split: an
		// explicit null or any other type is malformed, never coerced to
		// the absent traffic-gate shape — and null needs the pointer
		// decode, since unmarshalling null into a bare string is a silent
		// no-op).
		var decoded *string
		if json.Unmarshal(wire.Reason, &decoded) != nil || decoded == nil {
			return ExperimentAssignmentResult{}, expOutcome{}, false
		}
		reason = *decoded
	}
	switch reason {
	case "", experimentReasonKillSwitch, experimentReasonTargeting:
	default:
		// Not a verdict this SDK can represent: the allowlist is {absent,
		// kill_switch, targeting_unmatched}.
		return ExperimentAssignmentResult{}, expOutcome{}, false
	}
	notAssignedVersion := int64(0)
	if version != nil {
		if *version < 1 {
			// The SAME positivity rule the assigned branch enforces
			// (published versions start at 1): a PRESENT non-positive
			// version is not a verdict this contract sends — treating
			// {"assigned":false,"reason":"kill_switch","version":-1} as an
			// authoritative not-assigned would drop the cached assignment
			// on a malformed body, instead of the malformed_response
			// transient path that retains and serve-stales the
			// last-known-good record. Absent stays tolerated (the
			// traffic-gate shape carries no version); an explicit null is
			// PRESENT and already classified malformed by the
			// presence-aware decode above, never coerced to this
			// tolerance.
			return ExperimentAssignmentResult{}, expOutcome{}, false
		}
		notAssignedVersion = *version
	}
	// Every not-assigned shape drops the cached assignment: the server just
	// said this subject has no variant NOW, and a kill in particular must
	// stop applying at the next safe point and emit no exposure.
	return ExperimentAssignmentResult{
		Assigned: false,
		Reason:   reason,
		Version:  notAssignedVersion,
		Boundary: deepCopyJSONMap(wire.Boundary, 0),
	}, expOutcome{authoritative: true, dropEntry: true}, true
}

// deepCopyJSONMap deep-copies a decoded JSON object so a map handed to host
// code can be mutated freely without corrupting the cached entry later
// reads use. Depth-bounded (decoded JSON is acyclic; the cap only bounds
// the walk — deeper subtrees are dropped, canon parity).
func deepCopyJSONMap(value map[string]any, depth int) map[string]any {
	if value == nil {
		return nil
	}
	if depth >= 16 {
		return nil
	}
	out := make(map[string]any, len(value))
	for key, child := range value {
		out[key] = deepCopyJSONValue(child, depth+1)
	}
	return out
}

func deepCopyJSONValue(value any, depth int) any {
	switch typed := value.(type) {
	case map[string]any:
		return deepCopyJSONMap(typed, depth)
	case []any:
		if depth >= 16 {
			return nil
		}
		out := make([]any, len(typed))
		for i, child := range typed {
			out[i] = deepCopyJSONValue(child, depth+1)
		}
		return out
	default:
		return typed
	}
}

// ── deterministic exposure event id ─────────────────────────────────────────

// experimentExposureEventID derives the deterministic exposure event id: a
// stable digest of (session marker, subject, experiment, version, arm) —
// exactly the de-dupe tuple plus the session marker and the re-arm counter,
// so the id-uniqueness domain and the de-dupe domain coincide. The same
// tuple always derives the same id, so an accidental double emission inside
// one session collapses server-side as a duplicate; an explicit re-arm
// bumps the counter, and a rotated subject or another client instance
// derives a distinct id. UUID-shaped so it rides the envelope like any
// other event id.
func experimentExposureEventID(sessionMarker, subjectKey, experimentKey string, version, arm int64) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		"exposure",
		sessionMarker,
		subjectKey,
		experimentKey,
		strconv.FormatInt(version, 10),
		strconv.FormatInt(arm, 10),
	}, "\x1f")))
	b := digest[:16]
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// experimentAttributesEqual reports whether two normalized attribute sets
// are identical (both are sorted by the normalizer, so pairwise equality is
// set equality). A cached verdict was computed FOR its attribute set: a
// transient failure may serve it only to a request with the SAME set — a
// geo=US cache must not answer a geo=CA request during an outage (the
// server could have targeted them differently).
func experimentAttributesEqual(a, b []expAttribute) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// exposureTupleKey is the ratified de-dupe tuple: (experiment, version,
// subject) — nothing more. The assignment key is deliberately NOT part of
// it: it is normally deterministic for this tuple anyway, and a regenerated
// key for the same tuple must not over-count exposures. The emitted fact
// still carries the current subject-fact key as a prop.
func exposureTupleKey(experimentKey string, entry *expEntry) string {
	return escapeRemoteConfigSegment(experimentKey) + rcScopeSeparator +
		strconv.FormatInt(entry.Version, 10) + rcScopeSeparator +
		escapeRemoteConfigSegment(entry.SubjectKey)
}

// ── durable store ───────────────────────────────────────────────────────────

// expDurableRecord is the durable cache file's shape: ONE scope's entry set.
// A record stamped for any other scope is dead weight and is overwritten by
// the next write; a record without a scope stamp cannot be attributed to
// any tuple and reads as no cache.
type expDurableRecord struct {
	Scope   string              `json:"scope"`
	Entries map[string]expEntry `json:"entries"`
}

// sanitizeExperimentEntries keeps only entries that are complete,
// well-formed assignment records, copied down to the known fields. Anything
// else — a corrupt file, a truncated entry, a garbled field — is dropped
// rather than served or crashed on (corrupt = miss, clean start).
func sanitizeExperimentEntries(entries map[string]expEntry) map[string]expEntry {
	out := make(map[string]expEntry, len(entries))
	for key, entry := range entries {
		if key == "" ||
			strings.TrimSpace(entry.AssignmentKey) == "" ||
			strings.TrimSpace(entry.VariantKey) == "" ||
			entry.Version < 1 {
			continue
		}
		switch entry.AssignmentUnit {
		case experimentAssignmentUnitSynthetic, experimentAssignmentUnitClientID:
		default:
			// An unknown unit could not have been installed by this build's
			// parser: corrupt or foreign — miss.
			continue
		}
		if entry.SubjectFactKey != "" && !expSubjectFactKeyPattern.MatchString(entry.SubjectFactKey) {
			// A stored fact key outside the sfk1_ grammar (older builds,
			// corruption) must never reach the fact lane: the assignment
			// still serves, degraded to fact-less.
			entry.SubjectFactKey = ""
		}
		// Stored attributes re-validate against the LIVE fetch-time
		// vocabulary and bounds: a corrupt or older-build record must not
		// feed out-of-vocabulary names, oversized values, or an over-cap
		// set into revalidation fetches.
		if len(entry.Attributes) > 0 {
			restored := make(map[string]string, len(entry.Attributes))
			for _, attribute := range entry.Attributes {
				restored[attribute.Name] = attribute.Value
			}
			entry.Attributes, _ = normalizeExperimentAttributes(restored)
		}
		entry.VariantPayload = deepCopyJSONMap(entry.VariantPayload, 0)
		out[key] = entry
	}
	return out
}

// ── the consumer state ──────────────────────────────────────────────────────

// experimentsState is the per-client experiment machinery. mu guards every
// field; emitMu serializes exposure emission and sweeps and is always taken
// OUTSIDE mu (lock order: emitMu → lifecycleMu → mu; nothing takes mu and
// then a client lock).
type experimentsState struct {
	mu     sync.Mutex
	emitMu sync.Mutex

	workspaceID string
	appKey      string
	envKey      string
	baseURL     string
	keyPrint    string
	spoolDir    string

	// entries is the in-memory serving cache: experiment key → entry. Only
	// assigned entries exist here; every not-assigned outcome drops its
	// key.
	entries map[string]*expEntry

	// latchRetained is the RETAINED-unserved assignment set: the serving
	// state an ordinary 401/403 latch cleared from entries while the
	// durable record deliberately kept it (the disk-free memory mirror of
	// that retention). The consent purge's re-arm pass reads it so
	// latch-cleared assignments count as RETAINED — their exposure intents
	// re-arm at the grant moment like any live entry's — never as drops
	// (whose snapshots the ratified purge rule kills). Keys leave when the
	// server actually withdraws them (applyEntryDropLocked), when a fresh
	// install supersedes them, and wholesale on the sentinel withdrawal
	// and on a subject rotation. Never consulted for serving.
	latchRetained map[string]*expEntry

	// pendingExposure holds applications whose exposure fact is still OWED:
	// experiment key → a bounded FIFO of {entry, session} snapshots, each
	// carrying THE SESSION THE APPLICATION BELONGS TO. Swept while consent
	// admits. The snapshot — not the serving cache — is the emission
	// source, so an owed fact for a variant that really ran survives a
	// later drop of the live entry, an ordinary auth latch, and a subject
	// re-mint (exposure facts are facts about the PAST; only the
	// real-subjects sentinel — whose fact keys must not outlive it —
	// discards them).
	pendingExposure map[string][]*expOwedExposure

	// durablePending is the durable-write convergence intent: experiment
	// key → {asOf, drop} for an OWED sync (memory changed, disk did not),
	// retried every cycle; durableClearPending is an owed whole-record
	// clear stamped by its resolution. The recorded intent lets an
	// ordinary fail-closed latch cancel owed WRITES (the 401/403 canon
	// retains the durable record) while authoritative drops still land.
	durablePending      map[string]expOwedSync
	durableClearPending bool
	durableClearAsOf    int64
	durableClearScope   string

	// tornDown is set at teardown: an in-flight response landing afterwards
	// must not install, persist, pace, or surface anything.
	tornDown bool

	// exposed is the session-scoped exposure dedup: tuple key → {arm,
	// auto}. arm is the highest arm handed out for the tuple; auto records
	// whether the AUTOMATIC arm-0 fact has emitted — an explicit re-arm may
	// run while that emission is still owed in the queue, and must not
	// consume its slot.
	exposed map[string]expExposed

	// sessionMarker is one marker per constructed consumer (= per SDK
	// session; this SDK has no session lifecycle, so its session is the
	// client instance): part of the deterministic exposure id, so each
	// instance's first application emits its own fact while duplicates
	// within the instance collapse.
	sessionMarker string

	// Per-(scope, experiment) sequence fence for out-of-order responses,
	// remote-config discipline: only an outcome newer than every settled
	// one for its key may install.
	fetchSeq uint64
	settled  map[string]uint64

	// authBlocked is the fail-closed latch: set by 401/403, cleared by
	// re-init or a later authoritative, authorized outcome of a fetch
	// STARTED AFTER the latch was set. While set, nothing is served and
	// revalidation halts; an explicit host fetch stays allowed (the
	// user-triggered path) and its success unlatches. authEpoch counts
	// latch events: every fetch captures it at dispatch and an outcome
	// whose captured epoch is stale is discarded outright — with a batch
	// of revalidations in flight, one 401 must not be undone (and revoked
	// assignments must not be reinstalled) by a sibling response that was
	// already in flight when the latch landed.
	authBlocked bool
	authEpoch   uint64

	// Revalidation cadence state (unix ms; 0 = unarmed).
	revalidateAtMS int64
	retryAfterMS   int64
	backoffAttempt int

	// reminted is the one-shot grammar re-mint guard (per process).
	reminted bool

	// purgeEpoch counts consent purges: an emission whose enqueue raced a
	// purge must not mark its tuple exposed over the purge's re-arm (the
	// queued fact may have been wiped) — the sweep re-emits it with the
	// SAME deterministic id, so a survivor collapses server-side.
	purgeEpoch uint64

	// owedExposureOverflow counts owed-exposure snapshots dropped by the
	// bounded FIFO, drained to a diagnostic by the next client-side pass.
	owedExposureOverflow int

	// subjectID is the lazily loaded/minted subject id ("" = none yet);
	// subjectChecked marks that the persisted value was read once.
	subjectID      string
	subjectChecked bool

	// memorySubjectID / memoryRecord are the SpoolDir-less fallbacks: the
	// subject id and the record last only for the process lifetime,
	// documented ephemeral.
	memorySubjectID string
	memoryRecord    *expDurableRecord

	// jitterFn is the uniform [0, 1) source for cadence jitter, wired from
	// the owning client at construction (the Client.jitter seam). nil
	// degrades to the midpoint (no jitter) — construction always sets it.
	jitterFn func() float64

	// captureOwedDropFn, when set, durably captures an entry's still-owed
	// exposure facts BEFORE the entry's durable delete lands (the fleet
	// contract: a kill/not-assigned drop must not lose the fact of real
	// treatment to a process death before the next sweep — the spool
	// replays it at the next launch, and the deterministic event id
	// collapses a double delivery server-side). Returns whether the capture
	// is DURABLE (or moot: nothing capturable, no spool, policy-refused);
	// on false it also returns the FROZEN capture payload — the exact
	// envelopes whose durability the contract demands — for the owed
	// capture+delete pair to retry via captureRetryFn: the gate releases
	// only when THOSE envelopes are durably spooled, never merely because
	// the live sweep emptied the owed snapshots into the volatile queue.
	// Called under e.mu; the implementation must take no client lock and
	// defer any integrator callbacks.
	captureOwedDropFn func(experimentKey string, owed []*expOwedExposure) (bool, []spoolEntry)

	// captureRetryFn re-attempts a frozen capture payload's durable spool
	// append. Same locking contract as captureOwedDropFn.
	captureRetryFn func(entries []spoolEntry) bool

	// laneParkedForTests suspends the background lane's automatic ticks so
	// tests can drive tick() deterministically. Never set in production.
	laneParkedForTests bool

	// consentRaceSeam, when set, fires at the consent commit points —
	// "mint_adopted" (between the lazy mint's adoption and its commit
	// re-check), "remint_adopted" (the same window at the grammar
	// re-mint's adoption), "settle_entry" (a response settle before its
	// locked section — via fireConsentRaceSeam, so a test can land a
	// denial that fully precedes the settle), "settle_locked" (the settle
	// inside the locked section, before the under-lock consent read), the
	// emission pair "exposure_enqueue" / "exposure_enqueued" (immediately
	// before the fact intake's own gate re-check, and immediately after a
	// successful enqueue before the arm bookkeeping re-locks — the raced
	// consent refusal and the raced purge-vs-high-water windows), and the
	// build pair "exposure_build" / "outcome_build" (between a fact's
	// (entry, epoch) snapshot leaving e.mu and its builder running — the
	// raced-sentinel window the snapshot-time epoch stamp closes) — so
	// tests can land a consent flip, purge, or sentinel deterministically
	// inside the race windows the commit checks close. Never set in
	// production (the consentSlowHalfGate discipline).
	consentRaceSeam func(stage string)

	// failDurableWritesForTests makes every durable save/clear report
	// failure, so the owed-sync machinery can be exercised
	// deterministically (the createAnonymousIDWith seam shape). Never set
	// in production.
	failDurableWritesForTests bool

	// syncDirFn substitutes the directory-sync primitive (nil = syncDir),
	// so tests can record and fail parent-directory syncs to pin the
	// unlink-durability sequences (the spool syncFn seam shape). Never set
	// in production.
	syncDirFn func(dir string) error

	// writeSubjectFileForTests substitutes the subject file's atomic write,
	// so tests can inject partial-durability failures — the rename/link
	// landed but the parent sync failed. Never set in production.
	writeSubjectFileForTests func(path string, payload []byte, initialMint bool) error

	// factPurgeEpochBumpFn, wired at construction, bumps the CLIENT's
	// pipeline-fact purge epoch (Client.expFactPurgeEpoch). Called UNDER
	// e.mu by the sentinel package, atomically with its decisive state
	// change, so no fact built after the sentinel can carry a pre-sentinel
	// epoch stamp — the pipeline purge's stamp-aware filters then spare
	// exactly the post-sentinel facts. nil only in bare test states.
	factPurgeEpochBumpFn func()

	// sentinelSpoolPurgeFn, wired at construction, runs the DISK-SPOOL leg
	// of the real-subjects sentinel (Client.sentinelSpoolPurgeUnderLock)
	// as part of the sentinel's durable commit. Called UNDER e.mu by the
	// sentinel package, after the fact-epoch bump and BEFORE the record
	// clear/tombstone, so no crash ordering can leave the assignment
	// withdrawal durably settled while the withdrawn facts are still armed
	// for the next launch's spool resend. Same locking contract as
	// captureOwedDropFn: leaf spool mutex only, integrator dead-letters
	// deferred. nil only in bare test states (and memory-only clients
	// no-op inside).
	sentinelSpoolPurgeFn func()

	// chmodFn is the chmod used when preload establishes the state
	// directory's privacy (nil never occurs in a constructed state:
	// newExperimentsState defaults it to os.Chmod, tests inject a failing
	// one via Config.experimentDirChmodForTests to pin the refused-tighten
	// fail-closed path).
	chmodFn func(name string, mode os.FileMode) error
}

// syncDirLocked is the state's directory-sync primitive (the syncDirFn
// seam, defaulting to syncDir).
func (e *experimentsState) syncDirLocked(dir string) error {
	if e.syncDirFn != nil {
		return e.syncDirFn(dir)
	}
	return syncDir(dir)
}

type expOwedExposure struct {
	entry   *expEntry
	session string
}

// expOwedSync is one owed durable intent, KEYED BY (scope, experiment) —
// see scopedIntentKey — and carrying both halves: a subject rotation
// retires a scope, and an owed drop must land against the RETIRED record
// (never be re-aimed at, or overwritten by, the new subject's intents for
// the same experiment key), while an owed write's in-memory source is gone
// with the rotation and cancels. Keying by experiment alone would let a
// post-rotation failed WRITE overwrite the retired scope's owed DROP,
// leaving stale assignment and fact-key data on disk forever.
type expOwedSync struct {
	asOf          int64
	drop          bool
	scope         string
	experimentKey string
	// captureFirst marks a drop whose owed-exposure durable capture has not
	// landed yet: the record delete must not outrun the replay source, so
	// the pair retries TOGETHER — the capture attempt first, the record
	// converge only once it lands. Such an intent never folds into sibling
	// saves. captureEntries is the FROZEN payload (the exact envelopes to
	// spool): the gate releases only when they are durable — a live sweep
	// emptying the owed snapshots into the volatile queue is not enough.
	captureFirst   bool
	captureEntries []spoolEntry
}

// scopedIntentKey is the durablePending map key: the (scope, experiment)
// composite, joined with the separator no escaped component contains.
func scopedIntentKey(scope, experimentKey string) string {
	return scope + rcScopeSeparator + experimentKey
}

type expExposed struct {
	arm  int64
	auto bool
}

func newExperimentsState(cfg Config) *experimentsState {
	marker, err := uuidv7.New()
	if err != nil {
		// The marker only needs uniqueness per instance; a randomness
		// failure this early degrades to a time-derived marker rather than
		// failing construction.
		marker = "session-" + strconv.FormatInt(int64(os.Getpid()), 10)
	}
	chmod := cfg.experimentDirChmodForTests
	if chmod == nil {
		chmod = os.Chmod
	}
	return &experimentsState{
		workspaceID:     cfg.WorkspaceID,
		appKey:          cfg.AppID,
		envKey:          cfg.EnvironmentID,
		baseURL:         strings.TrimRight(cfg.RemoteConfigURL, "/"),
		keyPrint:        experimentAPIKeyFingerprint(cfg.APIKey),
		spoolDir:        cfg.SpoolDir,
		entries:         make(map[string]*expEntry),
		latchRetained:   make(map[string]*expEntry),
		pendingExposure: make(map[string][]*expOwedExposure),
		durablePending:  make(map[string]expOwedSync),
		exposed:         make(map[string]expExposed),
		sessionMarker:   marker,
		settled:         make(map[string]uint64),
		chmodFn:         chmod,
	}
}

// preload serves the persisted last-known-good assignments immediately
// after a restart when a stored subject id exists and the record matches
// this exact scope; their exposures re-arm for this instance and are
// emitted by the first admitted sweep. No subject id is minted here.
// Returns whether the state directory's privacy could not be established —
// the preload then refused to read anything (the caller diagnoses it).
func (e *experimentsState) preload() (privacyRefused bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.spoolDir != "" {
		// The persisted experiment state may be READ — the subject id, the
		// assignment record, the condemnation tombstone — only through a
		// directory whose privacy is established, exactly like the spool
		// and consent-floor startup paths (initSpool's ensurePrivateDir
		// discipline): with ExperimentsEnabled and a SpoolDir but no
		// persisted grant, initSpool skips its load path and never runs the
		// tighten, so without this the preload would be the FIRST touch of
		// the directory and would seed served variants from files in a
		// loose (0755, possibly other-user-writable) directory before any
		// experiment write ran its own tighten. An existing SpoolDir that
		// cannot be tightened to 0700 fails the preload CLOSED instead:
		// nothing is read, nothing serves — the on-disk state is left for
		// a later run with the permissions fixed, matching the spool's
		// refused-tighten posture.
		if ensurePrivateDir(e.spoolDir, e.chmodFn) != nil {
			// Latch the subject as checked-and-absent: the lazy first read
			// (a later fetch's currentSubjectIDLocked) would otherwise pull
			// the subject id from the untrusted directory mid-session. A
			// lazily minted subject still rules this process memory-only —
			// its persist attempts keep failing closed through the write
			// path's own tighten.
			e.subjectChecked = true
			return true
		}
	}
	if condemned := e.condemnedScopeLocked(); condemned != "" {
		// A previous process's real-subjects sentinel clear never landed:
		// the tombstone condemns that scope's record. Refuse to serve it
		// and re-arm the owed clear so this process retries it (the
		// write-time demotion rule still applies if fresh authorized state
		// installs first).
		e.durableClearPending = true
		e.durableClearAsOf = 0
		e.durableClearScope = condemned
	}
	subject := e.currentSubjectIDLocked()
	if subject == "" {
		return
	}
	record := e.loadDurableRecordLocked()
	if record == nil || record.Scope != e.scopeForLocked(subject) {
		return
	}
	if e.durableClearPending && e.durableClearScope == record.Scope {
		// The record is condemned: nothing serves from it.
		return
	}
	for key, stored := range record.Entries {
		entry := stored
		e.entries[key] = &entry
		e.pendingExposure[key] = []*expOwedExposure{
			{entry: &entry, session: e.sessionMarker},
		}
	}
	return false
}

func (e *experimentsState) scopeForLocked(subjectID string) string {
	return buildExperimentScope(e.workspaceID, e.appKey, e.envKey, subjectID, e.baseURL, e.keyPrint)
}

// ── subject id persistence ──────────────────────────────────────────────────

func (e *experimentsState) subjectFilePath() string {
	if e.spoolDir == "" {
		return ""
	}
	return filepath.Join(e.spoolDir, expSubjectFileName)
}

// readSubjectFileLocked reads the persisted subject id through a hard cap:
// a valid file holds at most a 70-char id plus a newline, so anything past
// the cap is not a file this SDK wrote — corrupt, read as a miss ("")
// without allocating it.
func (e *experimentsState) readSubjectFileLocked() string {
	file, err := os.Open(e.subjectFilePath())
	if err != nil {
		return ""
	}
	data, readErr := io.ReadAll(io.LimitReader(file, expSubjectReadLimit+1))
	_ = file.Close()
	if readErr != nil || len(data) > expSubjectReadLimit {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// currentSubjectIDLocked returns the stored subject id when it is
// wire-valid, else "". Never mints. The persisted value is read at most
// once per process; adoption updates the cached copy.
func (e *experimentsState) currentSubjectIDLocked() string {
	if !e.subjectChecked {
		e.subjectChecked = true
		raw := e.memorySubjectID
		if e.subjectFilePath() != "" {
			raw = e.readSubjectFileLocked()
		}
		if validExperimentSubjectID(raw) {
			e.subjectID = raw
		}
	}
	return e.subjectID
}

// adoptMintedSubjectIDLocked mints and adopts a fresh subject id. A new
// subject is a new cache scope AND a new exposure subject: assignments
// fetched for the old subject must neither serve nor expose GOING FORWARD,
// and the session's exposure de-dupe resets — the de-dupe tuple is per
// (experiment, version, SUBJECT), so the new subject's first application
// must emit even where the old subject's already did. Owed exposure
// snapshots are deliberately RETAINED: a treatment that already ran under
// the old subject is a fact about the past, its fact carries the
// server-minted fact key (never the subject id), and its tuple — old
// subject included — stays distinct from anything the new subject arms. A
// failed persist is diagnosed and the minted id still rules this process,
// so one session stays self-consistent.
// adoptMintedSubjectIDLocked mints and adopts a subject id. The INITIAL
// mint (no id ruled this state yet) publishes create-only — temp file plus
// a hard link that fails when another process already published — and
// CONVERGES on the winner's id, so two clients racing a shared state dir
// end on one subject instead of two. A RE-MINT (grammar sentinel, corrupt
// load) replaces the stored value outright: rotation is the point.
// ownFile reports whether THIS mint is RESPONSIBLE for whatever exists at
// the persisted path — true from the moment this mint's write attempt
// begins, a FAILED write included: writePrivateFileAtomic can error after
// its link/rename already published the file (a parent-directory fsync
// failure), and an undo that trusted the failure report would leave the
// refused session's freshly minted spcid_ on disk for a later granted
// session to converge on. ownFile is false only when no write was attempted
// (memory-only, mint failure) or when the id converged on another process's
// winner — that pre-existing file is the winner's property and an undo must
// never remove it.
func (e *experimentsState) adoptMintedSubjectIDLocked() (subject string, persisted, ownFile bool) {
	minted, err := mintExperimentSubjectID()
	if err != nil {
		return "", false, false
	}
	initialMint := e.subjectID == ""
	e.entries = make(map[string]*expEntry)
	// The rotation retires the scope: the old subject's retained set is
	// dead weight (scope-missed on disk) and never re-arms under the new
	// subject.
	e.latchRetained = make(map[string]*expEntry)
	e.exposed = make(map[string]expExposed)
	// The rotation retires the previous scope: owed WRITES lose their
	// in-memory source and cancel; owed DROPS were authoritative server
	// decisions against the retired record and stay aimed at it (the
	// retry cycle's foreign-scope pass lands or settles them).
	for key, pending := range e.durablePending {
		if !pending.drop {
			delete(e.durablePending, key)
		}
	}
	e.subjectID = minted
	e.subjectChecked = true
	persisted = true
	if path := e.subjectFilePath(); path != "" {
		// Responsibility attaches BEFORE the write's outcome is known: a
		// write that reports failure may still have published the file
		// (rename landed, parent sync failed), and the undo must cover
		// exactly that residue.
		ownFile = true
		if err := e.writeSubjectFileLocked(path, minted, initialMint); err != nil {
			converged := false
			if initialMint && errors.Is(err, os.ErrExist) {
				if winner := e.readSubjectFileLocked(); validExperimentSubjectID(winner) {
					// Lost the initial publish race to a VALID winner:
					// converge on its id — one subject rules the shared
					// state dir. The winner's file is NOT this mint's
					// property (never removed by an undo).
					e.subjectID = winner
					return winner, true, false
				}
				// The existing file is CORRUPT (this is why the mint ran):
				// replace it outright — fresh-install semantics must not be
				// blocked by the garbage they exist to heal.
				converged = e.writeSubjectFileLocked(path, minted, false) == nil
			}
			persisted = converged
		}
	} else {
		e.memorySubjectID = minted
	}
	return minted, persisted, ownFile
}

// writeSubjectFileLocked publishes the subject file — create-only (link
// semantics, EEXIST when another process already published) for the initial
// mint, replace (rename) otherwise. The chmod hook tightens a pre-existing
// loose state dir/file (the spool's ensurePrivateDir discipline) — the
// subject id is private state whatever permissions the directory arrived
// with. writeSubjectFileForTests substitutes the whole write so tests can
// inject partial-durability failures (the rename landed, the parent sync
// did not).
func (e *experimentsState) writeSubjectFileLocked(path, minted string, initialMint bool) error {
	if e.writeSubjectFileForTests != nil {
		return e.writeSubjectFileForTests(path, []byte(minted+"\n"), initialMint)
	}
	rename := os.Rename
	if initialMint {
		rename = func(oldpath, newpath string) error { return os.Link(oldpath, newpath) }
	}
	return writePrivateFileAtomic(path, []byte(minted+"\n"), rename, os.Chmod)
}

// unadoptFreshMintLocked reverses a lazy mint whose commit-point consent
// re-check found the plane refused: the adopted id leaves memory, and the
// persisted path — when THIS mint is responsible for it (ownFile: the write
// ATTEMPT began, whatever it reported; a "failed" write can still have
// published the file when only the parent sync failed) — is cleared with an
// ENOENT-tolerant unlink plus a parent-directory sync, so nothing of the
// refused-session mint outlives the abort DURABLY: an unsynced unlink could
// resurrect the spcid_ file at the next launch for a later granted session
// to converge on. A converged-on-winner file is another process's
// pre-existing subject state, not this mint's, and is left in place (the
// invariant holds vacuously: this process created no durable state).
// Returns whether the undo could not be made durable — the caller surfaces
// that residual (the file then reads as a pre-existing id a later granted
// session converges on, exactly like any other process's publish).
func (e *experimentsState) unadoptFreshMintLocked(ownFile bool) bool {
	e.subjectID = ""
	e.memorySubjectID = ""
	removeFailed := false
	if ownFile {
		if path := e.subjectFilePath(); path != "" {
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				removeFailed = true
			} else if e.syncDirLocked(filepath.Dir(path)) != nil {
				// The unlink (this call's, or the one a failed write never
				// performed — ENOENT) is durable only once the parent
				// metadata is synced; a failed sync is a failed undo.
				removeFailed = true
			}
		}
	}
	return removeFailed
}

// ── durable record IO (all under mu) ────────────────────────────────────────

func (e *experimentsState) cacheFilePath() string {
	if e.spoolDir == "" {
		return ""
	}
	return filepath.Join(e.spoolDir, expCacheFileName)
}

func (e *experimentsState) tombstoneFilePath() string {
	if e.spoolDir == "" {
		return ""
	}
	return filepath.Join(e.spoolDir, expTombstoneFileName)
}

// writeCondemnationTombstoneLocked durably condemns the named scope's
// record after a FAILED sentinel clear: the owed-clear intent is otherwise
// memory-only, and a crash before the retry would let the next process
// preload and serve assignments the real-subjects sentinel withdrew. The
// tombstone is tiny, so it often lands where the record rewrite could not
// (the defold-canon tombstone rationale); when even it cannot be written
// the in-memory owed clear still rules this process and the residual
// crash-window is surfaced to the caller for a diagnostic. Deliberately
// NOT gated by the test write-failure seam: the tombstone is the recovery
// mechanism for exactly that failure.
func (e *experimentsState) writeCondemnationTombstoneLocked(scope string) bool {
	path := e.tombstoneFilePath()
	if path == "" {
		return true // memory-only mode: the in-memory owed clear IS the state
	}
	return writePrivateFileAtomic(path, []byte(scope+"\n"), os.Rename, os.Chmod) == nil
}

// condemnedScopeLocked returns the scope a durable tombstone condemns, or
// "". Bounded read; unreadable or over-cap reads as no tombstone (the owed
// machinery still rules in-process).
func (e *experimentsState) condemnedScopeLocked() string {
	path := e.tombstoneFilePath()
	if path == "" {
		return ""
	}
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	data, readErr := io.ReadAll(io.LimitReader(file, expTombstoneReadLimit+1))
	_ = file.Close()
	if readErr != nil || len(data) > expTombstoneReadLimit {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// clearCondemnationTombstoneLocked removes a tombstone naming scope (or any
// tombstone when scope is ""), reporting whether the spend DURABLY landed.
// The unlink is durable only once the parent directory's metadata is
// synced: a crash after the record save but before a durable unlink would
// resurrect the tombstone and condemn the FRESH record at the next launch
// (the tombstone is scope-stamped, and the replacement record usually
// carries the same scope). The MISSING-tombstone path syncs too: an absent
// name is indistinguishable from a PRIOR attempt's unlink whose directory
// sync never landed — reporting that spend durable without the sync leaves
// the same resurrection window the fresh-unlink path closes (the
// recovery-marker missing-file discipline). A failed spend fails CLOSED —
// the caller keeps the save owed and retries the pair.
func (e *experimentsState) clearCondemnationTombstoneLocked(scope string) bool {
	path := e.tombstoneFilePath()
	if path == "" {
		return true
	}
	if scope != "" && e.condemnedScopeLocked() != scope {
		return true
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false
	}
	return e.syncDirLocked(filepath.Dir(path)) == nil
}

// loadDurableRecordLocked loads the stored record, or nil when absent or
// unusable: a missing, over-limit, or unparseable file — or one without a
// scope stamp — is a clean start, decided before unbounded memory is spent
// reading it. Entries are sanitized field-by-field (corrupt = miss).
func (e *experimentsState) loadDurableRecordLocked() *expDurableRecord {
	path := e.cacheFilePath()
	if path == "" {
		if e.memoryRecord == nil {
			return nil
		}
		copied := expDurableRecord{Scope: e.memoryRecord.Scope, Entries: sanitizeExperimentEntries(e.memoryRecord.Entries)}
		return &copied
	}
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(file, expMaxRecordBytes+1))
	_ = file.Close()
	if err != nil || len(data) > expMaxRecordBytes {
		return nil
	}
	var record expDurableRecord
	if json.Unmarshal(data, &record) != nil {
		return nil
	}
	if record.Scope == "" {
		return nil
	}
	record.Entries = sanitizeExperimentEntries(record.Entries)
	return &record
}

// saveDurableRecordLocked persists the record atomically (private file, no
// JSON HTML escaping; the record clamp refuses a write that could not be
// read back whole). Returns false when the write did not land — the
// previously stored record stays untouched.
func (e *experimentsState) saveDurableRecordLocked(record *expDurableRecord) bool {
	if e.failDurableWritesForTests {
		return false
	}
	if record == nil || record.Scope == "" {
		return false
	}
	stored := expDurableRecord{Scope: record.Scope, Entries: sanitizeExperimentEntries(record.Entries)}
	path := e.cacheFilePath()
	if path == "" {
		e.memoryRecord = &stored
		return true
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(stored); err != nil {
		return false
	}
	if buf.Len() > expMaxRecordBytes {
		return false
	}
	// The chmod hook tightens a pre-existing loose state dir/file (the
	// spool's ensurePrivateDir discipline).
	if writePrivateFileAtomic(path, buf.Bytes(), os.Rename, os.Chmod) != nil {
		return false
	}
	// Fresh durable state supersedes ANY condemnation (the write-time
	// demotion rule): whatever record a tombstone condemned was just
	// REPLACED by this save, so the spend covers every scope — a stale
	// old-scope tombstone left by a failed re-mint persist included. The
	// spend is part of the durable save: a save whose tombstone unlink
	// could not be made durable is NOT reported landed — the owed-write
	// machinery retries the idempotent pair until both are on disk,
	// because a resurrected tombstone would condemn the fresh record at
	// the next launch.
	return e.clearCondemnationTombstoneLocked("")
}

// clearDurableRecordLocked drops the stored record outright (loads as "no
// cache" afterwards). Returns true when the clear landed.
func (e *experimentsState) clearDurableRecordLocked() bool {
	if e.failDurableWritesForTests {
		return false
	}
	path := e.cacheFilePath()
	if path == "" {
		e.memoryRecord = nil
		return true
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false
	}
	// The unlink is durable only once the parent directory's metadata is on
	// stable storage: a crash right after a synced-nothing Remove can
	// resurrect the withdrawn record, serving assignments the real-subjects
	// sentinel just condemned. The ErrNotExist path syncs too — an absent
	// name is indistinguishable from a PRIOR unlink (this process's failed
	// attempt, or a crashed predecessor's) whose directory sync never
	// landed, and reporting the clear landed without the sync would leave
	// that same resurrection window. A failed sync keeps the clear OWED
	// (the caller retries), matching the write path's syncDir discipline.
	if e.syncDirLocked(filepath.Dir(path)) != nil {
		return false
	}
	// The withdrawn record is gone: any condemnation tombstone is spent —
	// durably, or the clear stays owed (a resurrected tombstone would
	// needlessly condemn the NEXT record written for its scope).
	return e.clearCondemnationTombstoneLocked("")
}

// durableRecordForLocked returns the stored record for scope, or a fresh
// empty one (a record stamped for any other scope is dead weight and is
// overwritten by the next write).
func (e *experimentsState) durableRecordForLocked(scope string) *expDurableRecord {
	if record := e.loadDurableRecordLocked(); record != nil && record.Scope == scope {
		if condemned := e.condemnedScopeLocked(); condemned == record.Scope {
			// A condemned record is not a merge base: folding its entries
			// back into a fresh save would resurrect withdrawn
			// assignments. Start fresh; the successful save then
			// supersedes the tombstone.
			return &expDurableRecord{Scope: scope, Entries: make(map[string]expEntry)}
		}
		return record
	}
	return &expDurableRecord{Scope: scope, Entries: make(map[string]expEntry)}
}

// syncDurableEntryLocked converges one experiment's durable entry to the
// in-memory truth — and, in the SAME save, folds every other owed intent
// decided against the same scope (combined-save folding: a working save
// must not leave sibling intents waiting for the retry cycle). An entry
// present in memory is written, an absent one is dropped. asOf stamps the
// state change (the entry's own fetch time for a write, the resolution time
// for a drop) and drives the sibling freshness fence. isRetry marks an
// owed-sync retry: only there does the fence apply to DROPS — at decision
// time a drop resolves whatever the shared record holds, clock rollbacks
// included, and its stamp is raised ABOVE that record; a retry may instead
// run after a sibling client persisted a genuinely newer assignment the
// drop never saw, and yields to it. The same asymmetry holds for WRITES: at
// decision time a fresh authoritative write supersedes the stored record
// (raising its own stamp above it), while an owed retry yields to a
// strictly-fresher sibling write. Returns true when the disk agrees with
// memory for the primary key; false marks it owed with the raised stamp,
// the intent, and the decision scope (a later ordinary fail-closed latch
// cancels owed WRITES but must let owed authoritative DROPS land; a subject
// rotation cancels owed writes and keeps drops aimed at the retired scope).
func (e *experimentsState) syncDurableEntryLocked(scope, experimentKey string, asOf int64, isRetry bool) bool {
	intentKey := scopedIntentKey(scope, experimentKey)
	entry := e.entries[experimentKey]
	record := e.durableRecordForLocked(scope)
	stored, hasStored := record.Entries[experimentKey]
	if entry != nil {
		if hasStored && stored.FetchedAtMS > entry.FetchedAtMS {
			if isRetry {
				// A same-namespace sibling persisted a FRESHER entry after
				// this write was decided: never roll the shared record back.
				delete(e.durablePending, intentKey)
				return true
			}
			entry.FetchedAtMS = stored.FetchedAtMS + 1
		}
		if existing, ok := e.durablePending[intentKey]; ok && !isRetry && existing.asOf >= entry.FetchedAtMS {
			// A pending intent for this key (typically a capture-gated drop
			// whose record half is blocked behind its frozen payload) is the
			// key's LATEST decided outcome, and this fresh install strictly
			// supersedes it in program order — but the stored record the
			// stamp-raise above consulted predates the intent, so a
			// same-millisecond resolution would leave the install's stamp
			// EQUAL to the intent's asOf and the pair's later drop retry
			// would outrank the newer assignment (deleting it) instead of
			// settling. Rank the install above the intent it supersedes.
			entry.FetchedAtMS = existing.asOf + 1
		}
		record.Entries[experimentKey] = *entry
		asOf = entry.FetchedAtMS
	} else {
		if !hasStored {
			if existing, ok := e.durablePending[intentKey]; ok && existing.captureFirst {
				// Nothing stored to delete, but the pending pair's FROZEN
				// capture payload has not landed: the capture debt is a
				// distinct obligation (the owed exposure's only durable
				// replay source) and survives until captureRetryFn lands
				// it — its delete half then self-settles via the fences.
				return true
			}
			delete(e.durablePending, intentKey)
			return true
		}
		if isRetry && stored.FetchedAtMS > asOf {
			// The RETRY of an owed drop found an entry stamped after the
			// drop was decided: a sibling client persisted a newer
			// assignment this drop never resolved. Deleting it would lose
			// newer valid state, so the outranked drop settles instead.
			delete(e.durablePending, intentKey)
			return true
		}
		if !isRetry && stored.FetchedAtMS >= asOf {
			// At DECISION time a drop always wins over the stored record it
			// resolves: the wall clock can move backward, and fencing the
			// delete on raw stamps would let a rollback revive a killed
			// variant at the next launch. Raise the drop's effective stamp
			// above the record instead.
			asOf = stored.FetchedAtMS + 1
		}
		delete(record.Entries, experimentKey)
	}
	// Fold every owed same-scope sibling intent into this save (their own
	// retry fences honored), so one working write converges the whole
	// record instead of leaving siblings for the cycle.
	folded := e.foldOwedIntentsLocked(record, scope, experimentKey)
	if e.saveDurableRecordLocked(record) {
		if existing, ok := e.durablePending[intentKey]; ok && existing.captureFirst && !isRetry {
			// The landed save supersedes the pending pair's RECORD half
			// only (typically a fresh same-experiment install replacing a
			// capture-gated drop): the FROZEN capture payload is the old
			// owed exposure's only durable replay source and must not be
			// cleared by the shared (scope, experiment) key. The pair stays
			// armed — captureRetryFn re-attempts the payload, and once it
			// lands the pair's delete half self-settles as outranked
			// against the fresher record (the owed-drop fence).
		} else {
			delete(e.durablePending, intentKey)
		}
		for _, foldedKey := range folded {
			delete(e.durablePending, foldedKey)
		}
		return true
	}
	if entry != nil && hasStored {
		// The refresh write failed with a superseded entry still stored:
		// tombstone it best-effort (the smaller record often fits where the
		// refreshed one did not), so a relaunch starts clean rather than
		// serving the variant memory already replaced. The owed retry below
		// still converges the disk to the full entry when storage recovers.
		delete(record.Entries, experimentKey)
		e.saveDurableRecordLocked(record)
	}
	next := expOwedSync{asOf: asOf, drop: entry == nil, scope: scope, experimentKey: experimentKey}
	if existing, ok := e.durablePending[intentKey]; ok && existing.captureFirst && !isRetry {
		// The failed save's replacement intent CARRIES the unlanded capture
		// debt: overwriting the pair with a plain intent would silently
		// drop the frozen payload — the old owed exposure's only durable
		// replay source.
		next.captureFirst, next.captureEntries = true, existing.captureEntries
	}
	e.durablePending[intentKey] = next
	return false
}

// foldOwedIntentsLocked applies every owed intent decided against scope —
// except skipKey, the caller's primary — onto record, with owed-retry fence
// semantics. Returns the keys whose intents were applied (or settled by
// their fences): the caller settles them only if its save lands. Intents
// for OTHER scopes are foreign and are left for the retry cycle's
// foreign-scope pass; memory is consulted for writes only (drops resolve
// against the record alone).
func (e *experimentsState) foldOwedIntentsLocked(record *expDurableRecord, scope, skipExperimentKey string) []string {
	var folded []string
	for intentKey, pending := range e.durablePending {
		if pending.scope != scope || pending.experimentKey == skipExperimentKey {
			continue
		}
		if pending.captureFirst {
			// A drop whose owed-exposure capture has not landed: folding it
			// would delete the record entry before the replay source exists.
			// It waits for the retry cycle's capture attempt.
			continue
		}
		stored, hasStored := record.Entries[pending.experimentKey]
		if pending.drop {
			if hasStored && stored.FetchedAtMS > pending.asOf {
				// Outranked owed drop: a fresher sibling write landed after
				// the drop was decided — the drop settles without applying.
				folded = append(folded, intentKey)
				continue
			}
			delete(record.Entries, pending.experimentKey)
			folded = append(folded, intentKey)
			continue
		}
		entry := e.entries[pending.experimentKey]
		if entry == nil {
			// An owed write whose in-memory source is gone (latch or
			// rotation should have cancelled it): nothing to fold — settle
			// it rather than let it decay into a delete.
			folded = append(folded, intentKey)
			continue
		}
		if hasStored && stored.FetchedAtMS > entry.FetchedAtMS {
			// Retry semantics: yield to the strictly-fresher stored entry.
			folded = append(folded, intentKey)
			continue
		}
		record.Entries[pending.experimentKey] = *entry
		folded = append(folded, intentKey)
	}
	return folded
}

// demoteOwedClearLocked demotes an owed whole-record clear into per-key
// drops for exactly the keys the clear still covers — everything on the
// clear's OWN scope's record that memory does not hold — each stamped by
// the clear decision, raised above the covered record (the sentinel is
// decisive over the state it withdrew). A record already replaced by
// another scope's write settles the clear outright: the withdrawn record is
// gone. Runs from the retry cycle AND at fresh-install time: a fresh
// authorized assignment landing after the failed clear supersedes the
// whole-record form immediately, so a later ordinary auth latch — which
// empties memory while RETAINING the durable record — can never leave the
// stale clear armed to wipe state written after it.
func (e *experimentsState) demoteOwedClearLocked() {
	if !e.durableClearPending {
		return
	}
	clearAsOf := e.durableClearAsOf
	clearScope := e.durableClearScope
	e.durableClearPending = false
	e.durableClearAsOf = 0
	e.durableClearScope = ""
	record := e.loadDurableRecordLocked()
	if record == nil || (clearScope != "" && record.Scope != clearScope) {
		return
	}
	for key, stored := range record.Entries {
		if e.entries[key] != nil {
			continue
		}
		asOf := clearAsOf
		if stored.FetchedAtMS >= asOf {
			asOf = stored.FetchedAtMS + 1
		}
		e.durablePending[scopedIntentKey(record.Scope, key)] = expOwedSync{asOf: asOf, drop: true, scope: record.Scope, experimentKey: key}
	}
}

// retryDurableSync retries every owed durable intent so the disk converges
// as soon as storage recovers, instead of waiting for an unrelated write or
// a relaunch: the owed whole-record clear first (scope-gated — a record
// already replaced by a newer scope settles it), then ONE folded save for
// the current scope's intents, then the foreign-scope drops — intents
// decided against a RETIRED scope land against that record alone, memory
// never consulted, and settle the moment another scope's write replaced the
// file. Local disk housekeeping only: no network, no serving decisions, so
// it runs regardless of the consent state — a kill drop decided under grant
// must land durably even if consent flips.
func (e *experimentsState) retryDurableSync() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.durableClearPending {
		if len(e.entries) > 0 {
			// Newer authorized state was installed after the failed clear:
			// the clear must not wipe it — demote it to the per-key drops
			// it still covers.
			e.demoteOwedClearLocked()
		} else {
			record := e.loadDurableRecordLocked()
			switch {
			case record == nil:
				// Nothing in the NAMESPACE (or unreadable = miss) — but an
				// absent record is indistinguishable from a prior unlink
				// whose directory sync failed (exactly how a failed clear
				// leaves the disk), so absence alone must not settle the
				// clear: a crash could resurrect the withdrawn record and
				// serve it at the next launch. Route through the clear —
				// its missing-file path performs the durable-absence work
				// (parent sync, tombstone spend) — and settle only when it
				// reports landed.
				if e.clearDurableRecordLocked() {
					e.durableClearPending = false
					e.durableClearAsOf = 0
					e.durableClearScope = ""
				}
			case e.durableClearScope != "" && record.Scope != e.durableClearScope:
				// Another scope's write replaced the withdrawn record: the
				// clear's target no longer exists.
				e.durableClearPending = false
				e.durableClearAsOf = 0
				e.durableClearScope = ""
			case e.clearDurableRecordLocked():
				e.durableClearPending = false
				e.durableClearAsOf = 0
				e.durableClearScope = ""
				e.durablePending = make(map[string]expOwedSync)
			}
		}
	}
	if len(e.durablePending) == 0 {
		return
	}
	// Capture-gated drops first: re-attempt the FROZEN capture payload's
	// durable append for every pair whose capture has not landed. Only a
	// durable (or policy-moot) append releases the gate — the live sweep
	// emptying the owed snapshots into the volatile queue is NOT
	// durability, and a crash with the fact queue-resident would lose it
	// with no durable assignment left to re-arm it. Failure keeps the pair
	// owed and the record intact.
	for intentKey, pending := range e.durablePending {
		if !pending.captureFirst {
			continue
		}
		if len(pending.captureEntries) == 0 || e.captureRetryFn == nil || e.captureRetryFn(pending.captureEntries) {
			pending.captureFirst = false
			pending.captureEntries = nil
			e.durablePending[intentKey] = pending
		}
	}
	// Current-scope intents: one folded save.
	if subject := e.currentSubjectIDLocked(); subject != "" {
		scope := e.scopeForLocked(subject)
		hasCurrent := false
		for _, pending := range e.durablePending {
			if pending.scope == scope {
				hasCurrent = true
				break
			}
		}
		if hasCurrent {
			record := e.durableRecordForLocked(scope)
			folded := e.foldOwedIntentsLocked(record, scope, "")
			if len(folded) > 0 && e.saveDurableRecordLocked(record) {
				for _, key := range folded {
					delete(e.durablePending, key)
				}
			}
		}
	}
	// Foreign-scope drops: land against the retired scope's record —
	// memory never consulted — or settle when another scope's write
	// already replaced it. (Foreign WRITES cannot exist: a rotation
	// cancels owed writes; any straggler cancels here the same way.)
	foreignScopes := make(map[string][]string)
	for intentKey, pending := range e.durablePending {
		if subject := e.currentSubjectIDLocked(); subject != "" && pending.scope == e.scopeForLocked(subject) {
			continue
		}
		if !pending.drop {
			delete(e.durablePending, intentKey)
			continue
		}
		if pending.captureFirst {
			// The capture gate above could not release this pair yet: the
			// retired record keeps its entry until the replay source lands.
			continue
		}
		foreignScopes[pending.scope] = append(foreignScopes[pending.scope], intentKey)
	}
	for scope, intentKeys := range foreignScopes {
		record := e.loadDurableRecordLocked()
		if record == nil || record.Scope != scope {
			// The retired record is gone or replaced: the withdrawn state
			// cannot serve (scope-miss), the drops settle.
			for _, intentKey := range intentKeys {
				delete(e.durablePending, intentKey)
			}
			continue
		}
		sort.Strings(intentKeys)
		applied := intentKeys[:0]
		for _, intentKey := range intentKeys {
			pending := e.durablePending[intentKey]
			if stored, hasStored := record.Entries[pending.experimentKey]; hasStored && stored.FetchedAtMS > pending.asOf {
				// Outranked by a fresher foreign write: settle without
				// applying.
				delete(e.durablePending, intentKey)
				continue
			}
			delete(record.Entries, pending.experimentKey)
			applied = append(applied, intentKey)
		}
		if len(applied) > 0 && e.saveDurableRecordLocked(record) {
			for _, intentKey := range applied {
				delete(e.durablePending, intentKey)
			}
		}
	}
}

// ── install ─────────────────────────────────────────────────────────────────

// installLocked settles an authoritative fetch outcome and, when it may,
// installs it. Gates, in remote-config order: the auth epoch must still be
// current (an outcome whose fetch was already in flight when a fail-closed
// latch landed is discarded outright — it must neither unlatch nor
// reinstall revoked assignments), the scope must still be current (a
// subject re-minted while the response was in flight makes it another
// subject's assignment), then the per-key sequence fence, then the
// outcome's own directives. Returns install directives for the caller's
// off-lock work: whether an exposure sweep is owed, whether a durable
// persist failure must be surfaced, and whether the real-subjects sentinel
// drop actually landed (vs being discarded by a gate).
// applyEntryDropLocked is the authoritative single-entry drop package: the
// entry leaves memory and its durable delete converges — with the narrow
// drop-time capture first. Shared by the current-epoch install and the
// epoch-stale destructive carve-out (the fleet partition).
func (e *experimentsState) applyEntryDropLocked(scope, experimentKey string, resolvedAtMS int64) (persistFailed bool) {
	dropped := e.entries[experimentKey]
	delete(e.entries, experimentKey)
	// A server-directed withdrawal ends the key's RETAINED status too: a
	// later consent purge must not re-arm an exposure intent for an
	// assignment the server dropped (the ratified since-dropped rule).
	delete(e.latchRetained, experimentKey)
	// The OWED exposures deliberately survive the drop: an application
	// that already happened is a fact — the drop stops future serving,
	// not the record of real treatment. The durable drop converges
	// keyed on the DISK state (a latch may have cleared serving while
	// the record retains the entry) and is retried until it lands — the
	// kill rule demands the drop reach the disk. The drop's effective
	// stamp is raised above the entry it resolves: the wall clock can
	// move backward, and a drop for X must always outrank X's own
	// stamp.
	asOf := resolvedAtMS
	if dropped != nil && dropped.FetchedAtMS >= asOf {
		asOf = dropped.FetchedAtMS + 1
	}
	// The narrow drop-time capture (fleet contract): the entry's durable
	// delete is about to land while its exposure may still be owed only
	// in memory — durably capture those owed facts FIRST, so a process
	// death before the next sweep replays them from the spool instead of
	// losing real treatment. Queue-full-without-drop stays memory-only
	// by design; only the delete triggers capture. A capture that could
	// NOT be made durable (spool persist failure) must not let the
	// record delete outrun the replay source: the drop becomes an owed
	// capture+delete PAIR — serving already stopped (the entry left
	// memory above), the durable record stays intact, and the retry
	// cycle re-attempts the capture before converging the record.
	if e.captureOwedDropFn != nil {
		if owed := e.pendingExposure[experimentKey]; len(owed) > 0 {
			if ok, frozen := e.captureOwedDropFn(experimentKey, owed); !ok {
				e.durablePending[scopedIntentKey(scope, experimentKey)] = expOwedSync{
					asOf: asOf, drop: true, scope: scope,
					experimentKey: experimentKey,
					captureFirst:  true, captureEntries: frozen,
				}
				return true
			}
		}
	}
	return !e.syncDurableEntryLocked(scope, experimentKey, asOf, false)
}

// applySentinelWithdrawalLocked is the real-subjects sentinel package: the
// fail-closed latch, a fresh auth epoch, and the outright withdrawal of the
// cached assignments, their subject-fact keys, and the owed exposure
// snapshots that carry them — durably (a failed whole-record clear is owed,
// tombstoned, and retried). Shared by the current-epoch install and the
// epoch-stale destructive carve-out (the fleet partition).
func (e *experimentsState) applySentinelWithdrawalLocked(scope string, resolvedAtMS int64) (persistFailed, landed bool) {
	e.authBlocked = true
	e.authEpoch++
	e.entries = make(map[string]*expEntry)
	// The sentinel withdraws the retained set outright — nothing survives
	// as a re-arm source.
	e.latchRetained = make(map[string]*expEntry)
	// The sentinel withdraws the assignments AND their subject-fact
	// keys outright: owed exposure snapshots carry those keys and
	// go with them. The AUTO dedupe slate resets with them — a purged
	// queued-undelivered automatic exposure must not leave its tuple
	// marked emitted, or a later authorized re-fetch in this session
	// would arm a replacement and suppress it as already-sent
	// (under-counting real treatment). The blanket auto reset is safe
	// on both sides of the delivery ambiguity: a fact that HAD
	// delivered re-derives the SAME deterministic id and collapses
	// server-side. The purge kills FACTS, never the session's
	// id-domain bookkeeping: the highest arm each tuple handed out
	// stays claimed (resetExposedKeepArmsLocked), so a post-re-enable
	// explicit re-arm continues from high-water+1 with a DISTINCT
	// deterministic id instead of reusing an arm a delivered
	// pre-sentinel fact already spent (which the server would dedupe,
	// undercounting a real re-exposure). The purge epoch bump fences
	// emissions already in flight past their gates, exactly like the
	// consent purge.
	e.resetExposedKeepArmsLocked()
	e.purgeEpoch++
	e.pendingExposure = make(map[string][]*expOwedExposure)
	if e.factPurgeEpochBumpFn != nil {
		// The PIPELINE-fact purge epoch bumps here, under e.mu, atomically
		// with the sentinel's decisive state change — NOT later inside the
		// off-lock pipeline purge: a fresh authorized fetch can install and
		// enqueue an exposure in the scheduling window between this lock's
		// release and that purge taking emitMu, and with a late bump that
		// post-sentinel fact would carry the pre-sentinel epoch and be
		// swept as withdrawn. Bumped here, every fact built after this
		// moment is stamped past the purge, and the stamp-aware queue,
		// worker, and spool filters spare exactly those.
		e.factPurgeEpochBumpFn()
	}
	if e.sentinelSpoolPurgeFn != nil {
		// The DISK-SPOOL leg of the withdrawal runs HERE, under e.mu, as
		// part of the sentinel's durable commit — after the epoch bump
		// (the sweep spares current-epoch captures) and BEFORE the record
		// clear/tombstone below. Off-lock only (the pipeline purge), a
		// crash after the durable clear or tombstone but before that purge
		// left the next startup loading the old raw facts with purge epoch
		// zero — initSpool runs before the experiment preload — and
		// RESENDING subject-fact keys the durable state says were
		// withdrawn. removeMatching persists the withdrawal marker before
		// the mirror forgets anything, so this leg is individually
		// crash-durable, and the facts-first order keeps every crash point
		// fail-closed: facts withdrawn with the assignment withdrawal
		// still unsettled just re-encounter the sentinel at the next
		// server contact, while the reverse order would let a settled
		// sentinel resend withdrawn facts. The off-lock pipeline purge
		// still runs after the install settles (the queue and worker legs'
		// home); its spool pass then finds nothing new.
		e.sentinelSpoolPurgeFn()
	}
	if e.clearDurableRecordLocked() {
		e.durablePending = make(map[string]expOwedSync)
	} else {
		// The withdrawn assignments are still on disk: mark the
		// clear OWED — stamped by this resolution — and retry it
		// every cycle until it lands. The owed intent alone is
		// memory-only, so a durable condemnation tombstone is
		// written fail-closed too: a crash before the retry must
		// not let the next process preload and serve the withdrawn
		// record.
		e.durableClearPending = true
		e.durableClearAsOf = resolvedAtMS
		e.durableClearScope = scope
		// The sentinel cancels this scope's FROZEN capture debts even
		// while the record clear stays owed: a prior kill/not-assigned
		// drop's durablePending captureFirst payload holds pre-sentinel
		// exposure envelopes, and the next retryDurableSync runs its
		// capture retry regardless of the still-failing clear — it would
		// re-append those withdrawn facts into the spool AFTER the
		// under-lock sentinel sweep above purged it, and a crash/restart
		// then reloads them with no in-memory purge epoch and resends
		// withdrawn subject-fact keys. Only the frozen payloads die (they
		// carry exactly what the sentinel withdrew — the owed exposure
		// snapshots died wholesale above for the same reason, silently by
		// the same precedent); the DROP half of each pair survives, aimed
		// at the record the owed clear and the tombstone already condemn.
		for key, pending := range e.durablePending {
			if pending.scope == scope && pending.captureFirst {
				pending.captureFirst = false
				pending.captureEntries = nil
				e.durablePending[key] = pending
			}
		}
		e.writeCondemnationTombstoneLocked(scope)
		return true, true
	}
	return false, true
}

func (e *experimentsState) installLocked(seq uint64, scope, experimentKey string, outcome expOutcome, authEpoch uint64, resolvedAtMS int64) (sweepOwed, persistFailed, dropAllLanded bool) {
	subject := e.currentSubjectIDLocked()
	if subject == "" || e.scopeForLocked(subject) != scope {
		return false, false, false
	}
	fenceKey := scope + rcScopeSeparator + experimentKey
	if seq <= e.settled[fenceKey] {
		return false, false, false
	}
	if authEpoch != e.authEpoch {
		// The fleet partition (defold R22) applied to the auth-epoch gate:
		// a response that raced a fail-closed latch installs nothing,
		// unlatches nothing, settles no fence, resets no backoff — but its
		// DESTRUCTIVE half still lands. The latch retains the durable
		// record, so discarding a server-directed withdrawal here would let
		// a later unlatch or re-init serve an assignment the server already
		// killed or withdrew. The scope and sequence gates above still
		// apply: another subject's verdict, or one a newer settled outcome
		// superseded, stays discarded whole.
		if outcome.authoritative && !outcome.transient {
			if outcome.dropAll {
				pf, landed := e.applySentinelWithdrawalLocked(scope, resolvedAtMS)
				return false, pf, landed
			}
			if outcome.dropEntry {
				return false, e.applyEntryDropLocked(scope, experimentKey, resolvedAtMS), false
			}
		}
		return false, false, false
	}
	if outcome.authoritative {
		e.settled[fenceKey] = seq
	}
	if outcome.authBlocked {
		// Fail closed: stop serving every cached assignment (the getters
		// return nothing), halt revalidation, and open a new auth epoch so
		// every response still in flight is discarded — only a fetch
		// started AFTER this moment may unlatch. The durable record is
		// retained (a re-init may serve it and re-probe) unless the
		// real-subjects sentinel says the platform withdrew the
		// assignments outright.
		if outcome.dropAll {
			pf, landed := e.applySentinelWithdrawalLocked(scope, resolvedAtMS)
			return false, pf, landed
		}
		e.authBlocked = true
		e.authEpoch++
		// The latch RETAINS the assignments it stops serving (the durable
		// record keeps them; a re-init or unlatch re-serves): mirror them
		// into latchRetained so a consent purge can tell latch-cleared
		// retained state from true drops — its re-arm pass must keep those
		// assignments' exposure intents alive (grant-moment re-arms for
		// RETAINED assignments), not lose them with the emptied entries.
		for key, entry := range e.entries {
			e.latchRetained[key] = entry
		}
		e.entries = make(map[string]*expEntry)
		// An ORDINARY 401/403 retains the durable record — and the owed
		// EXPOSURE snapshots: a treatment that already ran is a fact
		// about the past, and the latch stops future serving, not the
		// reporting of what happened (the sweep keeps draining them
		// while consent admits). An owed cache WRITE whose entry this
		// latch just cleared from memory must not decay into a delete
		// at the next retry — absence in memory here is fail-closed
		// serving, not a drop decision — so owed writes are cancelled
		// (their source data is gone; the disk keeps its last-known
		// record). Owed DROPS were authoritative server decisions and
		// still land.
		for key, pending := range e.durablePending {
			if !pending.drop {
				delete(e.durablePending, key)
			}
		}
		return false, false, false
	}
	if outcome.authoritative && !outcome.transient {
		// An authoritative, authorized VERDICT of a post-latch fetch (the
		// epoch gate above guarantees that) proves the credential works
		// again: unlatch and let revalidation resume. The unexpected-status
		// class (authoritative for FENCE ordering, transient for
		// everything else) is excluded: the fleet contract pins the latch
		// invariant in both directions — a stray 409 neither latches nor
		// unlatches.
		if e.authBlocked {
			// The unlatch RESTORES the retained assignments, not just the
			// flag: the latch cleared e.entries while deliberately
			// retaining the assignments (durably, mirrored in
			// latchRetained), and an unlatch driven by ONE experiment's
			// fetch must not strand the others — unserved by the getters
			// and invisible to the revalidation lane (which iterates
			// entries) until a process restart. The mirror restores with
			// its ORIGINAL entries — stamps, attributes, and fences
			// untouched (no disk writes: the record already holds them; no
			// stamp raises: nothing was superseded) — and this fetch's own
			// outcome then applies on top: a fresh install replaces its
			// key, a drop removes it. Owed exposures were never touched by
			// the latch, so nothing re-arms here — restoring an entry is
			// not a new application (the R12 grant-moment re-arms belong
			// to the consent purge alone).
			for key, entry := range e.latchRetained {
				if e.entries[key] == nil {
					e.entries[key] = entry
				}
			}
			e.latchRetained = make(map[string]*expEntry)
		}
		e.authBlocked = false
	}
	if outcome.transient {
		return false, false, false
	}
	// A settled authoritative answer resets the consecutive-failure
	// counter. A server-set Retry-After deadline is deliberately NOT
	// cleared here: the pacing is shared by every cached entry, and one
	// admitted request for an unrelated entry does not rescind the server's
	// wait for the plane — the deadline simply expires on its own (clamped
	// to a day; the deferral setter already keeps only the LATEST
	// deadline).
	e.backoffAttempt = 0
	if outcome.dropEntry {
		return false, e.applyEntryDropLocked(scope, experimentKey, resolvedAtMS), false
	}
	if outcome.newEntry != nil {
		entry := outcome.newEntry
		entry.SubjectKey = subject
		entry.Attributes = outcome.attributes
		e.entries[experimentKey] = entry
		// The fresh install supersedes any latch-retained copy: the LIVE
		// entry is the re-arm source from here on.
		delete(e.latchRetained, experimentKey)
		// A fresh authorized assignment supersedes any owed whole-record
		// clear RIGHT NOW, not at the next cycle: waiting would leave the
		// stale clear armed across an ordinary auth latch (which empties
		// memory while retaining the durable record), and the retention
		// canon must never let that clear delete state written after it.
		e.demoteOwedClearLocked()
		persisted := e.syncDurableEntryLocked(scope, experimentKey, entry.FetchedAtMS, false)
		e.armRevalidationLocked(resolvedAtMS)
		// The variant takes effect at this resolution (a variant change on
		// republish applies here too): the application point. The snapshot
		// is ARMED behind any still-owed earlier applications (the queue
		// preserves them — a full analytics queue must not cost the
		// previous treatment its fact) and the caller's off-lock sweep
		// drains in order; a locally failed emit is retried by the cycle
		// sweep instead of being lost.
		e.armExposureLocked(experimentKey, entry)
		return true, !persisted, false
	}
	return false, false, false
}

// ── exposure arming (state side; emission lives in experiment_facts.go) ─────

// armExposureLocked arms one application snapshot for emission. A
// same-(session, tuple) snapshot already armed at the tail is refreshed in
// place (purges re-arm the same application; the de-dupe collapses
// same-tuple emissions anyway), so the queue only grows across genuinely
// distinct applications. The FIFO is bounded; overflowing drops the OLDEST
// with a diagnostic, so a new application displacing the slot cannot
// silently lose the previous application's still-unemitted fact.
func (e *experimentsState) armExposureLocked(experimentKey string, entry *expEntry) (overflowed bool) {
	list := e.pendingExposure[experimentKey]
	if tail := lastOwedExposure(list); tail != nil && tail.session == e.sessionMarker &&
		exposureTupleKey(experimentKey, tail.entry) == exposureTupleKey(experimentKey, entry) {
		tail.entry = entry
		return false
	}
	list = append(list, &expOwedExposure{entry: entry, session: e.sessionMarker})
	if len(list) > expMaxOwedExposures {
		list = list[1:]
		e.owedExposureOverflow++
		overflowed = true
	}
	e.pendingExposure[experimentKey] = list
	return overflowed
}

// drainOwedExposureOverflowLocked reports and resets the FIFO-overflow
// count for the caller's diagnostic.
func (e *experimentsState) drainOwedExposureOverflowLocked() int {
	count := e.owedExposureOverflow
	e.owedExposureOverflow = 0
	return count
}

func lastOwedExposure(list []*expOwedExposure) *expOwedExposure {
	if len(list) == 0 {
		return nil
	}
	return list[len(list)-1]
}

// owedTupleArmedLocked reports whether the queue still holds an armed
// CURRENT-SESSION snapshot for the tuple: this session's automatic arm-0
// emission is owed, not yet emitted. Prior sessions' owed snapshots are
// another session's facts and do not count against this session's arm
// accounting.
func (e *experimentsState) owedTupleArmedLocked(experimentKey, tuple string) bool {
	for _, owed := range e.pendingExposure[experimentKey] {
		if owed.session == e.sessionMarker && exposureTupleKey(experimentKey, owed.entry) == tuple {
			return true
		}
	}
	return false
}

// onAnalyticsPurge re-arms this session's emissions after a consent denial
// purged queued-but-unpublished analytics facts — exposure facts included.
// Those facts never reached the server, so a later re-grant of the retained
// assignment emits the exposure again instead of silently under-counting
// real treatment. Facts that HAD already published — or were mid-flight,
// wire-ambiguous, when the denial landed — simply collapse server-side as
// duplicates of their deterministic event ids (the re-emission derives the
// SAME id for the same session, tuple, and arm), so the blanket re-arm is
// safe on both sides of the ambiguity.
func (e *experimentsState) onAnalyticsPurge() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.purgeEpoch++
	// Owed snapshots are DISCARDED first, then every live entry re-arms:
	// after a purge the session's emission slate is exactly its still-live
	// treatments — a snapshot for a since-dropped entry must not re-emit
	// into the re-granted session (the ratified reconciliation of the
	// blanket-re-arm rule). The deterministic id domain is unchanged: a
	// re-armed live tuple derives the same id its purged fact carried, so
	// a wire-ambiguous survivor still collapses server-side.
	e.pendingExposure = make(map[string][]*expOwedExposure)
	// The purge kills FACTS, never the session's id-domain bookkeeping:
	// each tuple's highest handed-out arm survives the reset (only the
	// auto slate re-arms), so a post-re-grant TrackExperimentExposure
	// continues from high-water+1 with a DISTINCT deterministic id — a
	// reset-to-zero counter would hand out arm 1 again, colliding with the
	// pre-denial explicit fact's id, and the server's de-dupe would
	// undercount a real new re-exposure.
	e.resetExposedKeepArmsLocked()
	for key, entry := range e.entries {
		e.armExposureLocked(key, entry)
	}
	// Latch-cleared serving state is RETAINED, not dropped: an ordinary
	// 401/403 latch emptied e.entries while the durable record kept the
	// assignments (and the re-arm-from-entries pass above cannot see
	// them), so without this pass a denial landing under a latch would
	// silently lose a retained treatment's owed exposure — nothing left to
	// sweep after the re-grant. Re-arm the retained set exactly like live
	// entries (grant-moment intents for retained assignments): the auto
	// snapshot re-derives the same deterministic arm-0 id, so anything
	// that HAD published collapses server-side, and the arm high-water
	// preserved above keeps explicit re-arms distinct.
	for key, entry := range e.latchRetained {
		if e.entries[key] != nil {
			continue // the live entry already re-armed above
		}
		e.armExposureLocked(key, entry)
	}
}

// resetExposedKeepArmsLocked re-arms every tuple's AUTOMATIC emission slot
// while preserving its arm high-water: purge semantics for the dedup
// bookkeeping. The arm counter is the session's deterministic-id domain —
// arms already handed out stay spent forever (their facts may have
// published), while auto=false makes the next automatic emission (the
// purge's re-arm, a restore, a fresh install of the same tuple) emit again
// with its unchanged arm-0 id, collapsing server-side if a survivor already
// carried it.
func (e *experimentsState) resetExposedKeepArmsLocked() {
	for tuple, prior := range e.exposed {
		e.exposed[tuple] = expExposed{arm: prior.arm, auto: false}
	}
}

// teardown stops the consumer: an in-flight response landing afterwards is
// discarded outright — nothing installs, persists, paces, or surfaces.
func (e *experimentsState) teardown() {
	e.mu.Lock()
	e.tornDown = true
	e.mu.Unlock()
}

// fireConsentRaceSeam fires a consent-race test stage from OUTSIDE e.mu
// (the seam field itself is mu-guarded, so the read synchronizes with the
// test's under-lock set/clear). Production leaves the seam nil.
func (e *experimentsState) fireConsentRaceSeam(stage string) {
	e.mu.Lock()
	seam := e.consentRaceSeam
	e.mu.Unlock()
	if seam != nil {
		seam(stage)
	}
}

// ── revalidation cadence state ──────────────────────────────────────────────

// armRevalidationLocked schedules the next revalidation cycle:
// 300s ± 10% uniform jitter from now. jitter is the caller's [0, 1) source
// (the client's seeded jitter seam); nil uses the midpoint.
func (e *experimentsState) armRevalidationLocked(nowMS int64) {
	unit := 0.5
	if e.jitterFn != nil {
		unit = e.jitterFn()
	}
	factor := 1 + (unit*2-1)*expRevalidateJitter
	e.revalidateAtMS = nowMS + int64(math.Floor(expRevalidateIntervalSeconds*factor*1000))
}

// deferRevalidationLocked parks the cadence until a deadline. Monotone: only
// the LATEST deadline is kept, and a deferral never shortens an armed one.
// Clamped to one day so a hostile or bugged header cannot park the plane
// (and its kill checks) indefinitely.
func (e *experimentsState) deferRevalidationLocked(nowMS int64, seconds int) {
	if seconds <= 0 {
		return
	}
	if seconds > expMaxDeferSeconds {
		seconds = expMaxDeferSeconds
	}
	deadline := nowMS + int64(seconds)*1000
	if deadline > e.retryAfterMS {
		e.retryAfterMS = deadline
	}
	// Pacing pulls the armed cadence deadline DOWN, never out (the defold
	// canon's rule). The cycle arms the next interval BEFORE its batch
	// runs, so a transient park landing mid-batch would otherwise strand
	// the failed and the skipped keys until the FULL interval — the
	// documented Retry-After/backoff recovery, not the 300s cadence, must
	// decide when the plane probes again after a parked failure. An
	// unarmed cadence stays unarmed (nothing cached is waiting to probe).
	if e.revalidateAtMS > 0 && e.revalidateAtMS > e.retryAfterMS {
		e.revalidateAtMS = e.retryAfterMS
	}
}

// paceTransientLocked paces the revalidation cadence after a transient
// failure: a server Retry-After wins; otherwise exponential backoff with
// full jitter (transport parity — the first failure is free, then the
// ceiling doubles per consecutive failure up to the cap).
func (e *experimentsState) paceTransientLocked(nowMS int64, retryAfterSeconds int, retryAfterPresent bool) {
	if retryAfterPresent {
		// A PRESENT hint is the server's explicit pacing answer either way:
		// positive parks the cadence; ZERO (a literal 0, or an HTTP-date
		// already past) says "retry immediately" — no deferral, no backoff
		// arming (the exponential streak is the guess for a server that
		// said nothing, never an override of one that spoke), AND any
		// previously armed deferral clears: this is the LATEST fence-gated
		// server word on pacing, and staying parked to an older deadline
		// would defy it.
		if retryAfterSeconds > 0 {
			e.deferRevalidationLocked(nowMS, retryAfterSeconds)
		} else {
			e.retryAfterMS = 0
			// Present-zero says retry NOW for the cadence too: the due
			// cycle pre-armed the next interval before dispatching, and
			// leaving that deadline standing would defer the server's
			// explicit immediate-retry answer by the full interval. The
			// pull-down twin of the parked case (an unarmed cadence stays
			// unarmed: 0 is never greater than nowMS).
			if e.revalidateAtMS > nowMS {
				e.revalidateAtMS = nowMS
			}
		}
		return
	}
	e.backoffAttempt++
	if e.backoffAttempt < 2 {
		// The first transient is free for the backoff LADDER (transport
		// parity: no deferral fence arms, the streak only starts counting) —
		// but never free for the armed CADENCE: the due cycle arms the next
		// 300s interval BEFORE dispatching its batch, so returning here left
		// that pre-armed deadline standing and the FIRST outage probe waited
		// out the full interval instead of entering the documented base
		// backoff — a brief blip delayed kill-switch and republish
		// convergence by up to five minutes. Pull the armed deadline DOWN to
		// the base delay — pull-down only, the defold-canon rule: an unarmed
		// cadence stays unarmed (nothing cached is waiting to probe), and a
		// deadline already sooner stands.
		deadline := nowMS + int64(expBackoffBaseSeconds)*1000
		if e.revalidateAtMS > 0 && e.revalidateAtMS > deadline {
			e.revalidateAtMS = deadline
		}
		return
	}
	exp := e.backoffAttempt - 2
	if exp > 16 {
		exp = 16
	}
	ceiling := float64(expBackoffBaseSeconds) * math.Pow(2, float64(exp))
	if ceiling > expBackoffCapSeconds {
		ceiling = expBackoffCapSeconds
	}
	unit := 0.5
	if e.jitterFn != nil {
		unit = e.jitterFn()
	}
	wait := float64(expBackoffBaseSeconds) + unit*(ceiling-float64(expBackoffBaseSeconds))
	e.deferRevalidationLocked(nowMS, int(math.Ceil(wait)))
}

// ── the fetch (on *Client: transport, clock, lifecycle) ─────────────────────

// expFetchError formats a fetch outcome with no usable verdict; the
// taxonomy code is the machine-readable part of the message (the
// remote-config error convention).
func expFetchError(code string) error {
	return fmt.Errorf("shardpilot experiment assignment fetch failed: %s", code)
}

// FetchExperimentAssignment performs one explicit (host-triggered)
// assignment fetch for experimentKey and blocks for its outcome.
// attributes is the optional server-evaluated targeting attribute set
// (allowlisted names plus the custom_attribute_* family; out-of-vocabulary
// names and unusable values are dropped, never sent). A nil error means the
// host has a usable verdict — fresh, or served from the last-known-good
// cache over a transient failure (FromCache=true with Code set), or a
// first-class not-assigned answer (Assigned=false; Code "not_found" for an
// unknown experiment). A non-nil error means no usable verdict:
// ErrConsentDenied/ErrConsentUnknown when the plane's consent gate refuses
// (granted-only, forced-minor fully off), ErrClosed after Close, and
// otherwise the taxonomy code in the error text ("unauthorized" fails
// closed without serving the cache; "bad_request"/"http_<status>" are
// permanent; "transient_..."/"http_0"/"malformed_response" are transient
// with no usable cache; "superseded"/"stale_subject" mean a newer outcome
// settled while this fetch was in flight and no cached assignment exists;
// "experiment_key_required" and "subject_unavailable" fail before any
// network use).
//
// Concurrent fetches are legal; an older response never overwrites (or,
// for the sentinel, drops past) a newer settled outcome — a fenced-out
// authoritative response serves the SETTLED current state to its caller,
// never the discarded variant. Every fetch classifies independently: a
// 401/403 halts the automatic revalidation lane and in-memory serving, and
// a later authorized fetch started after the latch resumes both.
func (c *Client) FetchExperimentAssignment(ctx context.Context, experimentKey string, attributes map[string]string) (ExperimentAssignmentResult, error) {
	return c.fetchExperimentAssignment(ctx, experimentKey, attributes, false, nil)
}

func (c *Client) fetchExperimentAssignment(ctx context.Context, experimentKey string, attributes map[string]string, isRevalidation bool, presetAttributes []expAttribute) (ExperimentAssignmentResult, error) {
	// The same lifecycle fence as Track and FetchRemoteConfig for HOST
	// fetches: Close either sees this fetch (and waits for it) or completed
	// its closed store first (and this fetch is rejected). The automatic
	// lane's fetches deliberately do NOT join the host-operation wait
	// group: a hung revalidation GET must not hold Close for a timeout —
	// the lane's stop-cancelled context aborts it instead, and Close's
	// lane-done wait (plus the teardown gate on the settle path) covers
	// the rest.
	c.lifecycleMu.Lock()
	if c.closed.Load() {
		c.lifecycleMu.Unlock()
		return ExperimentAssignmentResult{}, ErrClosed
	}
	// The dispatch-time consent stamp, read under lifecycleMu — the same
	// lock a denial's fast half takes — so it is exact, exactly like the
	// event intake's: a fetch admitted here is provably admitted before
	// any later denial's epoch bump. The settle's commit point compares it
	// against the then-current epoch, so a deny → re-grant completing
	// while the response is in flight is caught even though the CURRENT
	// consent state admits again — a pre-revocation response must not
	// install constructively across the revoked interval (the event-intake
	// epoch idiom, applied to fetches).
	dispatchConsentEpoch := c.consentEpoch.Load()
	if !isRevalidation {
		c.trackWG.Add(1)
		defer c.trackWG.Done()
	}
	c.lifecycleMu.Unlock()

	e := c.exp
	if e == nil {
		return ExperimentAssignmentResult{}, ErrExperimentsNotConfigured
	}
	experimentKey = strings.TrimSpace(experimentKey)
	if experimentKey == "" {
		return ExperimentAssignmentResult{}, expFetchError("experiment_key_required")
	}
	// GRANTED-ONLY plane (see the module header): while the effective
	// consent state refuses analytics — the forced-minor state included —
	// no request leaves the process, nothing is minted, nothing is served.
	if err := c.experimentConsentRefusal(); err != nil {
		return ExperimentAssignmentResult{}, err
	}

	e.mu.Lock()
	if e.tornDown {
		e.mu.Unlock()
		return ExperimentAssignmentResult{}, ErrClosed
	}
	subject := e.currentSubjectIDLocked()
	if subject == "" {
		// Re-checked immediately before minting: a revocation landing
		// between the preflight refusal check above and this lazy mint must
		// not adopt — let alone persist — a NEW spcid_ for a session the
		// plane already refuses (forced-minor sessions especially create
		// ZERO experiment state). An existing subject takes no new state
		// and rides the pre-wire re-check instead.
		if err := c.experimentConsentRefusal(); err != nil {
			e.mu.Unlock()
			return ExperimentAssignmentResult{}, err
		}
		var persisted, ownFile bool
		subject, persisted, ownFile = e.adoptMintedSubjectIDLocked()
		if subject == "" {
			e.mu.Unlock()
			return ExperimentAssignmentResult{}, expFetchError("subject_unavailable")
		}
		if e.consentRaceSeam != nil {
			e.consentRaceSeam("mint_adopted")
		}
		// COMMIT-POINT double check, after the adopt/persist: a denial's
		// fast half flips the consent atomic OUTSIDE e.mu (its e.mu
		// rendezvous — onAnalyticsPurge — only queues BEHIND this section),
		// so the flip can land between the pre-mint check above and the
		// persist. Consent moved → the mint aborts and the undo leaves
		// nothing persisted: a refused session — the forced-minor state
		// above all — ends with ZERO subject state, not a freshly minted
		// spcid_ that would survive the denial indefinitely. A flip landing
		// after THIS read linearizes the mint before the denial (the
		// denial's own e.mu purge completes strictly later), which is the
		// pre-denial-mint case whose subject the denial retains by design.
		if refusal := c.experimentConsentRefusal(); refusal != nil {
			removeFailed := e.unadoptFreshMintLocked(ownFile)
			e.mu.Unlock()
			if removeFailed {
				c.stats.setLastError("experiment_subject_unmint_failed")
				c.logf("shardpilot experiments: removing the subject id minted across a consent revocation failed; the orphaned file reads as a pre-existing id at the next granted session")
			}
			return ExperimentAssignmentResult{}, refusal
		}
		if !persisted {
			c.stats.setLastError("experiment_subject_persist_failed")
			c.logf("shardpilot experiments: persisting the minted subject id failed; the id rules this process only (a restart re-buckets)")
		}
	}
	if isRevalidation && presetAttributes == nil && e.entries[experimentKey] == nil {
		// The pre-dispatch existence check and this section run under
		// separate lock acquisitions: the entry can vanish in the gap (a
		// concurrent host fetch's drop, a re-mint clearing the cache).
		// Re-check WHILE HOLDING the lock — a revalidation for a vanished
		// key must not dispatch at all, or its 200 would reinstall (with
		// no remembered attributes) an experiment the plane just removed.
		// The one legitimate entry-less revalidation dispatch is the
		// grammar-remint RETRY (presetAttributes non-nil): the rotation it
		// rides just cleared the whole cache BY DESIGN, and the retry
		// carries the rejected request's exact attribute set — aborting it
		// here would kill the lane path's one-shot self-heal.
		e.mu.Unlock()
		return ExperimentAssignmentResult{}, expFetchError("revalidation_entry_vanished")
	}
	e.fetchSeq++
	seq := e.fetchSeq
	// Capture the scope and the auth epoch ONCE per fetch: the URL, the
	// served cache, and the installed entry all describe the same subject
	// even if the id re-mints while the request is in flight, and an
	// outcome that raced a fail-closed latch is discarded by the epoch
	// gate at install time.
	scope := e.scopeForLocked(subject)
	authEpoch := e.authEpoch
	var normalizedAttributes []expAttribute
	dropped := 0
	switch {
	case presetAttributes != nil:
		// Internal grammar-remint retry only: the EXACT normalized set of
		// the rejected request rides the retry verbatim — the subject
		// rotation cleared the cached entry the cadence would have re-read
		// them from, and a targeted assignment must be retried with the
		// same input set, not un-targeted.
		normalizedAttributes = presetAttributes
	case attributes != nil:
		normalizedAttributes, dropped = normalizeExperimentAttributes(attributes)
	case isRevalidation:
		// A revalidation re-sends the attributes of the last host-supplied
		// fetch for this experiment (one targeting vocabulary, one value
		// set), so a server-evaluated condition keeps seeing the same
		// subject it matched before. Only the CADENCE reuses them: a
		// host-triggered fetch that omits attributes means what it says —
		// it sends none, and none become the entry's remembered set.
		if entry := e.entries[experimentKey]; entry != nil {
			normalizedAttributes = entry.Attributes
		}
	}
	fetchURL := buildExperimentAssignmentURL(e.baseURL, e.appKey, e.envKey, experimentKey, subject, normalizedAttributes)
	e.mu.Unlock()
	if dropped > 0 {
		c.logf("shardpilot experiments: dropped %d targeting attribute(s) outside the vocabulary or value bounds for experiment %q", dropped, experimentKey)
	}

	callerCtx := ctx
	fetchCtx, cancel := contextWithDefaultTimeout(ctx, c.cfg.HTTPTimeout)
	defer cancel()
	// Load the denial gate BEFORE the pre-wire consent re-check — the
	// events plane's publish discipline (publishRequestResult), applied to
	// the granted-only experiment plane: a denial completed AFTER the load
	// cancels the loaded gate and aborts the GET mid-flight; one completed
	// BEFORE it stored the refused state first and the re-check below
	// refuses pre-wire. Either way no assignment GET runs to completion
	// past a completed revocation — the settle-time check alone only
	// discarded the RESPONSE, after the wire traffic already happened,
	// which broke the forced-minor zero-experiment-traffic promise under
	// concurrent revocation.
	gate := c.consentGate.Load()
	if gate != nil {
		var cancelOnDenial context.CancelFunc
		fetchCtx, cancelOnDenial = context.WithCancel(fetchCtx)
		defer cancelOnDenial()
		stop := context.AfterFunc(gate.ctx, cancelOnDenial)
		defer stop()
	}
	if err := c.experimentConsentRefusal(); err != nil {
		// Refused PRE-WIRE: the claimed fence sequence settles nothing and
		// a later fetch simply outranks it.
		return ExperimentAssignmentResult{}, err
	}
	// The transport's remote-config GET is exactly this route's shape too —
	// bare authenticated GET, redirects refused, bounded body read — with
	// no If-None-Match: the assignment endpoint has no ETag/304 contract.
	resp, err := c.transport.FetchRemoteConfig(fetchCtx, remoteConfigRequest{
		url:    fetchURL,
		bearer: c.cfg.APIKey,
	})
	if err != nil {
		if gate != nil && gate.ctx.Err() != nil && errors.Is(err, context.Canceled) {
			// THIS fetch's gate was cancelled: a consent denial aborted the
			// GET mid-flight. The refusal is returned regardless of the
			// CURRENT consent state (a quick re-grant can land before the
			// transport returns) and BEFORE any classification: an aborted
			// wire exchange must not install, pace, serve stale, or count
			// as a transient endpoint outcome. A caller-context
			// cancellation is never reclassified — it leaves this gate's
			// context intact.
			if refusal := c.experimentConsentRefusal(); refusal != nil {
				return ExperimentAssignmentResult{}, refusal
			}
			return ExperimentAssignmentResult{}, ErrConsentDenied
		}
		if callerCtx != nil {
			if ctxErr := callerCtx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				// The CALLER's own context ended the fetch: an abort, not an
				// endpoint outcome — no cache fallback, no fence side
				// effects, just the caller's error back.
				return ExperimentAssignmentResult{}, err
			}
		}
		if resp.status == 0 {
			// Transport-level failure before any status arrived: the
			// transient http_0 class.
			resp = remoteConfigResponse{}
		}
		// A non-zero status rode the error through: the SDK-internal
		// deadline ended the BODY read after an authoritative status
		// arrived. Classify BY STATUS with the incomplete-body marker — a
		// stalled 401/403 fails closed (and can never be a sentinel), a
		// stalled 404 stays permanent, only a stalled 200 is transient.
	}
	return c.settleExperimentFetch(ctx, experimentKey, attributes, isRevalidation, seq, scope, authEpoch, dispatchConsentEpoch, normalizedAttributes, resp)
}

// settleExperimentFetch is the response half of a fetch: the consent
// re-check, the pure classification against the CURRENT fenced entry, the
// grammar-remint path, pacing, the install, and the caller-result guards.
// A revocation while the request was in flight is decided at the LOCKED
// commit point, never before it: an early pre-lock return would discard the
// response wholesale — destructive server verdicts included — so a denial
// landing with a kill_switch (or permanent-drop, or sentinel) response
// already in hand would leave the withdrawn assignment cached and re-served
// after a later re-grant. Every response classifies; the partition below
// strips the constructive halves and lands the destructive ones, and the
// caller still receives the refusal.
func (c *Client) settleExperimentFetch(ctx context.Context, experimentKey string, attributes map[string]string, isRevalidation bool, seq uint64, scope string, authEpoch uint64, dispatchConsentEpoch uint64, normalizedAttributes []expAttribute, resp remoteConfigResponse) (ExperimentAssignmentResult, error) {
	e := c.exp
	e.fireConsentRaceSeam("settle_entry")

	nowMS := c.clock.Now().UnixMilli()
	e.mu.Lock()
	if e.tornDown {
		// A torn-down consumer discards in-flight responses outright:
		// nothing installs, persists, or paces (the documented teardown
		// contract).
		e.mu.Unlock()
		return ExperimentAssignmentResult{}, ErrClosed
	}
	if e.consentRaceSeam != nil {
		e.consentRaceSeam("settle_locked")
	}
	// The consent check UNDER e.mu, at the commit point — the ONLY consent
	// gate on the settle path: a denial completing anywhere between this
	// fetch's admission and this read — SetConsentDecision's fast half
	// flips the atomic without e.mu — must not let a 200 install
	// memory/disk state or arm exposure debt for a refused (or
	// interrupted, below) plane; after a re-grant the client would serve
	// an assignment fetched across the revoked interval. The fleet
	// partition (defold R22) splits the refused settle instead of
	// discarding it whole: CONSTRUCTIVE halves — the install, exposure
	// arming, the grammar re-mint's subject adoption, pacing, a healthy
	// caller result — are discarded, while DESTRUCTIVE server verdicts
	// from a request dispatched under grant (an authoritative
	// not-assigned, a permanent drop, the real-subjects sentinel) still
	// apply to durable state: the cache is retained across denial by
	// design, so a discarded withdrawal would re-serve a server-killed
	// assignment at the next re-grant.
	// A flip landing after this read linearizes the settle before the
	// denial (its e.mu purge queues behind this section) — the response
	// then predates the revocation.
	consentRefusal := c.experimentConsentRefusal()
	if consentRefusal == nil && c.consentEpoch.Load() != dispatchConsentEpoch {
		// The CURRENT state admits, but the denial generation moved since
		// this fetch's admission (the epoch was captured under lifecycleMu
		// — the same lock a denial's fast half takes — so it is exact): a
		// deny → re-grant completed while the response was in flight. The
		// response belongs to a consent interval that CLOSED — installing
		// it would serve an assignment fetched across the revoked interval
		// — so the settle takes the same partition a still-standing denial
		// takes, and the caller receives the refusal the interrupting
		// denial owed it (the mid-flight gate-abort precedent: the refusal
		// is returned regardless of the current, re-granted state).
		consentRefusal = ErrConsentDenied
	}
	// Classification reads the CURRENT fenced entry, not the one captured
	// at dispatch: a kill or supersede that resolved while this request was
	// in flight must not be undone in the RESULT either — a caller must
	// never be handed a variant the fenced state no longer serves. A stale
	// epoch or scope reads as no entry.
	var entryNow *expEntry
	if authEpoch == e.authEpoch {
		if subjectNow := e.currentSubjectIDLocked(); subjectNow != "" && e.scopeForLocked(subjectNow) == scope {
			entryNow = e.entries[experimentKey]
		}
	}
	// Attribute-match gate on stale serving: the cached verdict was
	// computed for ITS normalized attribute set — a transient outcome may
	// serve it only to a request carrying the SAME set. A mismatched
	// request gets the closed transient failure instead of another
	// cohort's variant. (Revalidation re-sends the entry's own set, so the
	// lane always matches.)
	serveEntry := entryNow
	if serveEntry != nil && !experimentAttributesEqual(serveEntry.Attributes, normalizedAttributes) {
		serveEntry = nil
	}
	result, outcome, failure := applyExperimentAssignment(serveEntry, resp, expRequestScope{
		appKey:        e.appKey,
		envKey:        e.envKey,
		experimentKey: experimentKey,
	}, nowMS)
	outcome.attributes = normalizedAttributes
	// Captured BEFORE install (which may settle this very seq): true when a
	// NEWER outcome for this key already settled while this response was in
	// flight, i.e. the install below discards it.
	fenceKey := scope + rcScopeSeparator + experimentKey
	fencedOut := seq <= e.settled[fenceKey]

	if outcome.remint && consentRefusal == nil && !fencedOut && authEpoch == e.authEpoch {
		// The persisted subject id failed the wire grammar (storage
		// corruption this client could not detect locally): re-mint once
		// per process and retry as a fresh subject — from the revalidation
		// path too, or a rejected cached subject would never heal until an
		// explicit host fetch happens to run. A second grammar reject with
		// a freshly minted id is a bug, never a loop. The re-mint honors
		// the SAME fences as any other outcome — a grammar reject that is
		// fenced out, stale by epoch, or stale by scope must not rotate
		// the persisted subject, wipe entries, or consume the one-shot
		// budget: it falls through and is discarded like the stale outcome
		// it is.
		if subjectNow := e.currentSubjectIDLocked(); subjectNow != "" && e.scopeForLocked(subjectNow) == scope {
			if !e.reminted {
				e.reminted = true
				// Snapshot the rotation's in-memory casualties BEFORE the
				// adopt: the settle-time consent read above is not the
				// commit point — a denial's fast half flips the consent
				// atomic OUTSIDE e.mu (its e.mu purge only queues BEHIND
				// this section), so the flip can land between that read
				// and the persist below, and a refused/forced-minor
				// session must end with the SAME zero-new-state the
				// initial lazy mint's commit-point undo guarantees: no
				// freshly minted spcid_ in memory or on disk, no spent
				// one-shot budget, no rotation wreckage. The maps are
				// REPLACED by the adopt (the prior references stay valid
				// snapshots); durablePending is mutated in place and needs
				// a copy.
				priorSubject := e.subjectID
				priorMemorySubject := e.memorySubjectID
				priorEntries := e.entries
				priorLatchRetained := e.latchRetained
				priorExposed := e.exposed
				priorPending := make(map[string]expOwedSync, len(e.durablePending))
				for key, pending := range e.durablePending {
					priorPending[key] = pending
				}
				_, persisted, ownFile := e.adoptMintedSubjectIDLocked()
				if e.consentRaceSeam != nil {
					e.consentRaceSeam("remint_adopted")
				}
				// COMMIT-POINT double check, after the adopt/persist — the
				// r11 lazy-mint idiom at the re-mint site: consent moved →
				// the re-mint aborts whole. The undo covers the persisted
				// key (unadoptFreshMintLocked's ownFile-responsibility
				// unlink + parent sync — the minted id must not outlive
				// the abort durably), the rotation (the prior subject and
				// the maps the adopt wiped return; the denial's own
				// contract then governs them, exactly as if no re-mint had
				// run), and the one-shot budget (a later granted session
				// must still be able to heal the rejected subject). A flip
				// landing after THIS read linearizes the re-mint before
				// the denial, which is the pre-denial-rotation case whose
				// state the denial retains by design.
				if refusal := c.experimentConsentRefusal(); refusal != nil {
					removeFailed := e.unadoptFreshMintLocked(ownFile)
					e.subjectID = priorSubject
					e.memorySubjectID = priorMemorySubject
					e.entries = priorEntries
					e.latchRetained = priorLatchRetained
					e.exposed = priorExposed
					e.durablePending = priorPending
					e.reminted = false
					if removeFailed {
						// Whatever survived the failed unlink — the minted
						// id, or the OLD (server-rejected) id when the
						// replace itself never landed — must not let the
						// next launch preload and serve the old scope's
						// assignments: condemn that record exactly like
						// the non-refused failed-persist path below.
						e.writeCondemnationTombstoneLocked(scope)
					}
					e.mu.Unlock()
					if removeFailed {
						c.stats.setLastError("experiment_subject_unmint_failed")
						c.logf("shardpilot experiments: removing the subject id re-minted across a consent revocation failed; the orphaned file reads as a pre-existing id at the next granted session")
					}
					return ExperimentAssignmentResult{}, refusal
				}
				if !persisted {
					// The OLD subject file may outlive this process (the
					// replace failed): durably condemn the OLD scope's
					// record, or a crash would let the next launch preload
					// and SERVE the server-rejected subject's assignments
					// (its wire self-heal only fixes fetches, not the
					// stale serving in between). The tombstone spends when
					// any fresh save replaces the record.
					e.writeCondemnationTombstoneLocked(scope)
				}
				e.mu.Unlock()
				c.logf("shardpilot experiments: the server rejected the persisted subject id's grammar; re-minted once and retrying (the subject re-buckets, fresh-install semantics)")
				if !persisted {
					c.stats.setLastError("experiment_subject_persist_failed")
				}
				// The retry carries the SAME normalized attribute set the
				// rejected request sent — under the CALLER's own context:
				// an expired or cancelled caller bounds the whole blocking
				// call, the internal retry included.
				preset := normalizedAttributes
				if preset == nil {
					preset = []expAttribute{}
				}
				return c.fetchExperimentAssignment(ctx, experimentKey, attributes, isRevalidation, preset)
			}
			// The one-shot budget is SPENT and this grammar reject is
			// CURRENT (it passed the epoch/scope/sequence gates): a
			// permanently rejected input set must not keep serving stale —
			// it converts to the permanent-400 durable drop. Stale rejects
			// above are discarded whole (no drop, no budget effect).
			outcome.remint = false
			outcome.dropEntry = true
		}
	}
	if outcome.transient && consentRefusal == nil && authEpoch == e.authEpoch && !fencedOut {
		// Pacing is gated on scope currency AND the per-key sequence fence
		// exactly like the install and the re-mint: a transient answer for
		// a RE-MINTED-AWAY subject — or one a newer settled fetch already
		// fenced out — is discarded from state, and its Retry-After must be
		// discarded with it: a stale 429/5xx must not park the CURRENT
		// plane's revalidation (and kill checks) for up to the day clamp.
		// (A refused plane paces nothing either — the 2381 contract.)
		if subjectNow := e.currentSubjectIDLocked(); subjectNow != "" && e.scopeForLocked(subjectNow) == scope {
			e.paceTransientLocked(nowMS, outcome.retryAfterSeconds, outcome.retryAfterPresent)
		}
	}
	skipInstall := false
	if consentRefusal != nil {
		// The partition's constructive strip: nothing installs and no
		// exposure debt arms on a refused plane. Destructive halves —
		// dropEntry and the dropAll sentinel package — flow through to the
		// install below and land durably. An ORDINARY latch (authBlocked
		// without the sentinel) is protective serving state, not a
		// server-directed withdrawal: a refused settle leaves it unmoved,
		// exactly as the pre-lock refusal return always has.
		outcome.newEntry = nil
		skipInstall = outcome.authBlocked && !outcome.dropAll
	}
	var sweepOwedNow, persistFailed, dropAllLanded bool
	if !skipInstall {
		sweepOwedNow, persistFailed, dropAllLanded = e.installLocked(seq, scope, experimentKey, outcome, authEpoch, nowMS)
	}
	// The epoch re-check guards the PUBLIC result like the install: a
	// response that raced a fail-closed latch was discarded from state
	// above, and its caller must not receive a healthy assignment either —
	// it gets the closed result. (The latch-setting response itself
	// re-derives its own identical closed result here.)
	epochStale := authEpoch != e.authEpoch && !outcome.authBlocked
	// The subject-scope re-check guards the result the same way: a subject
	// re-minted while this response was in flight makes it ANOTHER
	// subject's assignment — the install discarded it, and the caller must
	// receive the miss, never the discarded variant.
	subjectNow := e.currentSubjectIDLocked()
	scopeStale := subjectNow == "" || e.scopeForLocked(subjectNow) != scope
	// And the per-key sequence fence guards it last: an older AUTHORITATIVE
	// response that a newer settled outcome already fenced out was
	// discarded by the install, and its caller must receive the SETTLED
	// current state — the cached assignment the getters serve, or the miss
	// — never the fenced-out variant. Transient results already derive
	// from the current fenced entry and pass through untouched.
	var supersededResult ExperimentAssignmentResult
	supersededFailure := ""
	if !epochStale && !scopeStale && fencedOut && outcome.authoritative {
		supersededServe := e.entries[experimentKey]
		if supersededServe != nil && !experimentAttributesEqual(supersededServe.Attributes, normalizedAttributes) {
			supersededServe = nil
		}
		supersededResult, supersededFailure = serveExperimentEntryOrFail(supersededServe, "superseded")
	}
	overflowed := e.drainOwedExposureOverflowLocked()
	e.mu.Unlock()
	// Dead-letters the locked install deferred (the drop-time owed capture
	// runs under e.mu) dispatch here, with no lock held.
	c.drainDeferredSpoolLetters()
	if overflowed > 0 {
		c.logf("shardpilot experiments: %d owed exposure snapshot(s) dropped (bounded queue overflow) — oldest first", overflowed)
	}

	if dropAllLanded {
		// The install (under e.mu) discarded the cache and the owed
		// snapshots; the PIPELINE-resident facts — queue, worker batch,
		// spool — are withdrawn here, off e.mu.
		c.purgeWithdrawnExperimentFacts()
		c.logf("shardpilot experiments: the platform disabled real-subject assignment; dropped the cached assignments and their subject fact keys")
	}
	if persistFailed {
		c.stats.setLastError("experiment_cache_persist_failed")
		c.logf("shardpilot experiments: persisting the assignment cache failed; the write is owed and retried until it lands")
	}
	if sweepOwedNow {
		// The freshly applied assignment's exposure fact (and any earlier
		// owed ones for the key) drain now, off-lock, in FIFO order.
		c.sweepExperimentExposures(experimentKey)
	}

	if consentRefusal != nil {
		// The destructive halves (if any) applied above; the caller
		// receives the refusal — never a healthy assignment fetched across
		// a consent that closed while the response settled.
		return ExperimentAssignmentResult{}, consentRefusal
	}

	switch {
	case epochStale:
		return ExperimentAssignmentResult{}, expFetchError("unauthorized")
	case scopeStale:
		return ExperimentAssignmentResult{}, expFetchError("stale_subject")
	case fencedOut && outcome.authoritative:
		if supersededFailure != "" {
			return ExperimentAssignmentResult{}, expFetchError(supersededFailure)
		}
		return supersededResult, nil
	case failure != "":
		return ExperimentAssignmentResult{}, expFetchError(failure)
	default:
		return result, nil
	}
}

// experimentJitter adapts the client's uniform [0, 1) jitter seam for the
// cadence math.
func (c *Client) experimentJitter() func() float64 {
	return c.jitterValue
}

// ── the cycle (background lane) ─────────────────────────────────────────────

// experimentCycle is one lane cycle, driven every second by the background
// goroutine (and directly by tests): owed durable writes retry FIRST and
// regardless of consent (local disk housekeeping — a kill drop decided
// under grant must land durably even if consent flipped meanwhile); then,
// while consent admits, the owed-exposure sweep (cache-restored
// applications, locally failed emissions, and applications whose facts a
// consent purge re-armed all drain here); then the revalidation cadence —
// while not auth latched, at least one assignment is cached, and no
// Retry-After deadline parks it, every cached key re-fetches once per
// armed interval. A parked revalidation never blocks anything: there is no
// pending state to drain at shutdown.
func (c *Client) experimentCycle(ctx context.Context) {
	e := c.exp
	if e == nil {
		return
	}
	e.mu.Lock()
	if e.tornDown {
		e.mu.Unlock()
		return
	}
	overflowed := e.drainOwedExposureOverflowLocked()
	e.mu.Unlock()
	if overflowed > 0 {
		c.logf("shardpilot experiments: %d owed exposure snapshot(s) dropped (bounded queue overflow) — oldest first", overflowed)
	}
	e.retryDurableSync()
	// The retry sync defers integrator dead-letters under e.mu (a capture
	// retry's policy refusal, expiry, or eviction): dispatch them THIS
	// cycle, before the consent gate's early return below — on a quiet
	// client (no fetch settling, no close) a letter deferred here would
	// otherwise sit until an unrelated settle or Close fires the next
	// off-lock drain.
	c.drainDeferredSpoolLetters()
	if c.experimentConsentRefusal() != nil {
		return
	}
	c.sweepAllExperimentExposures()
	nowMS := c.clock.Now().UnixMilli()
	var keys []string
	e.mu.Lock()
	switch {
	case e.authBlocked || len(e.entries) == 0:
	case e.retryAfterMS != 0 && nowMS < e.retryAfterMS:
	default:
		if e.retryAfterMS != 0 {
			e.retryAfterMS = 0
		}
		if e.revalidateAtMS == 0 {
			e.armRevalidationLocked(nowMS)
		} else if nowMS >= e.revalidateAtMS {
			e.armRevalidationLocked(nowMS)
			keys = make([]string, 0, len(e.entries))
			for key := range e.entries {
				keys = append(keys, key)
			}
			sort.Strings(keys)
		}
	}
	e.mu.Unlock()
	for _, key := range keys {
		// Batched per cycle: one GET per cached entry, each re-sending its
		// last host-supplied attributes. The plane's gates re-check BEFORE
		// every automatic fetch: an auth latch set by an earlier key's
		// refusal stops the batch outright (an unattended lane must not
		// keep asking a plane that just closed, and a later 200 in the same
		// batch must not clear the latch — only a host fetch or re-init
		// reopens), and a teardown, consent flip, or lane-context cancel
		// (Close) stops it the same way.
		if ctx.Err() != nil || c.experimentConsentRefusal() != nil {
			return
		}
		dispatchNowMS := c.clock.Now().UnixMilli()
		e.mu.Lock()
		blocked := e.authBlocked || e.tornDown
		// A deferral armed MID-BATCH (a 429/5xx Retry-After or the
		// transient backoff from an earlier key in this very cycle) parks
		// the whole plane: later GETs must not dispatch inside the
		// server-requested wait — the batch stops and the cadence resumes
		// after the deadline.
		parked := e.retryAfterMS != 0 && dispatchNowMS < e.retryAfterMS
		// The key must STILL be cached (and under the current subject's
		// scope) at dispatch: the batch runs from a snapshot, and a
		// concurrent host fetch can have dropped the entry (a
		// not-assigned verdict, a permanent 400, a subject re-mint
		// clearing the cache) since it was taken. A revalidation for a
		// vanished key is a no-op — dispatching it anyway could REINSTALL
		// the just-dropped experiment (with no remembered attributes),
		// resurrecting state the plane deliberately removed. (The fetch
		// re-checks under its own lock too — this gate just avoids the
		// dispatch.)
		stillCached := e.entries[key] != nil
		e.mu.Unlock()
		if blocked || parked {
			return
		}
		if !stillCached {
			continue
		}
		_, _ = c.fetchExperimentAssignment(ctx, key, nil, true, nil)
	}
}

// runExperimentsLane is the background lane goroutine, started only when
// the consumer is enabled: a one-second heartbeat driving experimentCycle
// until the client stops. The cadence state (not the heartbeat) decides
// when network work happens.
func (c *Client) runExperimentsLane() {
	defer close(c.expLaneDone)
	// The lane's fetches run under a stop-cancelled context: Close cancels
	// any in-flight revalidation GET instead of waiting it out (the lane
	// never blocks Close).
	laneCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-c.stop
		cancel()
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.exp.mu.Lock()
			parked := c.exp.laneParkedForTests
			c.exp.mu.Unlock()
			if !parked {
				c.experimentCycle(laneCtx)
			}
		}
	}
}

// ── getters ─────────────────────────────────────────────────────────────────

// ExperimentVariant returns the cached assigned variant key for an
// experiment, or "". It never touches the network and serves "" while no
// assignment is cached — or while the plane's consent gate refuses: the
// plane is granted-only, so a non-admitted session sees no variants at all
// (the cache record is retained, unserved, until a re-grant). Host code can
// ship one code path with the control experience as its default.
func (c *Client) ExperimentVariant(experimentKey string) string {
	e := c.exp
	experimentKey = strings.TrimSpace(experimentKey)
	if e == nil || experimentKey == "" {
		return ""
	}
	if c.experimentConsentRefusal() != nil {
		return ""
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.tornDown {
		return ""
	}
	if entry := e.entries[experimentKey]; entry != nil {
		return entry.VariantKey
	}
	return ""
}

// ExperimentVariantPayload returns a copy of the cached assigned variant's
// payload for an experiment, or nil — under exactly ExperimentVariant's
// serving rules.
func (c *Client) ExperimentVariantPayload(experimentKey string) map[string]any {
	e := c.exp
	experimentKey = strings.TrimSpace(experimentKey)
	if e == nil || experimentKey == "" {
		return nil
	}
	if c.experimentConsentRefusal() != nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.tornDown {
		return nil
	}
	if entry := e.entries[experimentKey]; entry != nil {
		return deepCopyJSONMap(entry.VariantPayload, 0)
	}
	return nil
}
