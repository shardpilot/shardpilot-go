package shardpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Experiment assignment consumer (GAP-017 / ADR-0259): GETs the deterministic
// experiment assignment for this installation's subject key from the
// control-plane assignment endpoint and serves the parsed verdict, with a
// per-experiment last-known-good cache so transient failures keep serving the
// assignment the app last acted on. Opt-in via Config.ExperimentsURL and
// entirely dark by default — with the platform's experimentation flags off
// (today's state everywhere) every fetch answers 403 and fails closed.
// Like remote config it authenticates with the publishable Config.APIKey only
// (never the ingest Config.Token), refuses redirects, and is NOT
// consent-gated: an assignment is configuration-plane routing, not telemetry
// (the exposure/outcome FACTS are consent-gated — see experiment_events.go).
//
// Fetch semantics (one fetch = one HTTP GET, decided by
// applyExperimentAssignment; the ratified remote-config canon applied to this
// plane, plus the two assignment-only extras of ADR-0259 Amendment 2):
//   - 200 with a parseable assignment body — the fresh verdict is served and
//     the per-experiment cache record is overwritten. ALL THREE not-assigned
//     shapes are valid 200s, distinguished by Assignment.Reason: absent =
//     the deterministic traffic gate, "kill_switch" = an operator kill,
//     "targeting_unmatched" = a targeting miss. For client_id-unit
//     assignments the response's subject_fact_key is retained with the
//     record — it is the ONLY value permitted as the analytics fact subject.
//   - A transient failure (offline, 408, 429, 5xx — the kill-switch-state
//     503 included — malformed or over-cap body) — the cached assignment is
//     served with FromCache=true and Reason carrying why; with no usable
//     cache the fetch fails. No cooldown is armed: the assignment plane has
//     no 429 Retry-After contract today, and none is invented here.
//   - 401/403 — fails CLOSED for that fetch, classified by status alone
//     ("unauthorized"): the cached assignment is not served as this fetch's
//     outcome, and NOTHING else happens — no cross-fetch latch, and the
//     cache is left untouched for later transient fallbacks… with exactly
//     ONE exception (Amendment 2 Extra 1): a 403 whose JSON body's `error`
//     field EQUALS "experiment real-subject assignment is disabled" — the
//     platform's real-subjects sentinel — additionally DROPS the cached
//     record and its subject_fact_key, so a client never keeps honoring an
//     assignment for a surface flipped back off. Any other 403 body (the
//     generic flag-off bodies included) and every 401 drop nothing.
//   - Any other status (404 for an unknown experiment, an unexpected 3xx,
//     413, other 4xx) is a PERMANENT failure (http_<status>) with the same
//     per-fetch shape.
//
// Permanent and fail-closed outcomes are authoritative for THE FETCH THAT
// RECEIVED THEM — they do not latch; every fetch classifies independently
// (the remote_config.go contract, ported not redesigned). The one carve-out
// is the AUTOMATIC revalidation lane (Amendment 2 Extra 2, opt-in via
// Config.ExperimentAssignmentRevalidateInterval): after the lane's own fetch
// receives ANY authoritative 401/403 it stops scheduling further assignment
// fetches until the client is re-initialized. Host-triggered
// FetchExperimentAssignment calls keep working and classifying per-fetch,
// and a later host-fetch success does not resume the halted lane.

// expMaxBodyBytes caps how much of an assignment response body is read;
// assignment payloads are small, and the shared transport reads at most
// rcMaxBodyBytes+1 — an over-cap body is malformed by definition, mirroring
// the remote-config contract.
const expMaxBodyBytes = rcMaxBodyBytes

// experimentRealSubjectsDisabledSentinel is the EXACT 403 body `error` value
// that — alone among all 401/403 outcomes — drops the cached assignment
// record (ADR-0259 Amendment 2 Extra 1). String equality only; any other
// body, an unparseable body, or a truncated body is a generic 403.
const experimentRealSubjectsDisabledSentinel = "experiment real-subject assignment is disabled"

// expCacheBodyBudget bounds the total cached assignment body bytes held (and
// persisted) per client: past it the oldest records are evicted. Assignment
// bodies are small; the budget exists so a pathological payload cannot grow
// the cache file unboundedly.
const expCacheBodyBudget = 1 << 20

// expCacheMaxRecords bounds how many per-experiment records are held, with
// the same oldest-first eviction.
const expCacheMaxRecords = 64

// expCacheReadLimit bounds how much of the durable cache file is ever read
// back: twice the body budget (bodies ride records as JSON strings written
// without HTML escaping, so escaping at worst doubles them — see
// marshalRCCache) plus framing headroom for record metadata. A larger file
// is not one this client could have written and is treated as corrupt
// without being loaded whole.
const expCacheReadLimit = 2*expCacheBodyBudget + 256<<10

// expRevalidateFloor is the minimum automatic-lane cycle spacing: an
// unattended loop never polls the assignment endpoint more often than once a
// minute, however small the configured interval.
const expRevalidateFloor = 60 * time.Second

// ExperimentAssignmentBoundary is the assignment response's boundary block:
// the server's machine-readable statement of the assignment's unit, scope,
// and rollout posture. Served verbatim for observability; the SDK consumes
// AssignmentUnit (it selects the analytics fact subject).
type ExperimentAssignmentBoundary struct {
	AssignmentUnit          string `json:"assignment_unit"`
	SubjectKeyKind          string `json:"subject_key_kind"`
	RuntimeTokenScope       string `json:"runtime_token_scope"`
	Persistence             string `json:"persistence"`
	AnalyticsFactOwnership  string `json:"analytics_fact_ownership"`
	ProductionRollout       string `json:"production_rollout"`
	AssignmentHashVersion   string `json:"assignment_hash_version"`
	TrafficAllocationBasis  int64  `json:"traffic_allocation_basis"`
	VariantAllocationBasis  int64  `json:"variant_allocation_basis"`
	SubjectIdentifierPolicy string `json:"subject_identifier_policy"`
}

// ExperimentAssignment is one parsed assignment verdict.
type ExperimentAssignment struct {
	AppKey         string `json:"app_key"`
	EnvironmentKey string `json:"environment_key"`
	ExperimentKey  string `json:"experiment_key"`
	// Version is the published experiment version the verdict was computed
	// against; echoed into the facts' experiment_version prop.
	Version int64 `json:"version"`
	// Assigned is the verdict: false for a traffic-gate miss (Reason
	// absent), an operator kill (Reason "kill_switch"), or a targeting miss
	// (Reason "targeting_unmatched"). The producers refuse to emit facts
	// for a not-assigned verdict.
	Assigned bool `json:"assigned"`
	// AssignmentKey is the deterministic assignment identifier; the
	// analytics fact subject for synthetic_subject_key-unit assignments and
	// the client-side exposure dedupe key for both units.
	AssignmentKey string `json:"assignment_key"`
	// VariantKey and VariantPayload are present only when Assigned.
	VariantKey     string         `json:"variant_key"`
	VariantPayload map[string]any `json:"variant_payload"`
	// Reason distinguishes the three not-assigned shapes; empty for the
	// legacy traffic-gate miss and for an assigned verdict.
	Reason string `json:"reason"`
	// SubjectFactKey (client_id unit only) is the derived
	// `sfk1_<64 hex>` subject the analytics facts MUST carry as their
	// assignment_key — the raw spcid subject key never rides props.
	SubjectFactKey string                       `json:"subject_fact_key"`
	Boundary       ExperimentAssignmentBoundary `json:"boundary"`
}

// ExperimentAssignmentResult is one explicit fetch's outcome with a usable
// verdict. A cache-served transient IS a success: the caller has the last
// known assignment (FromCache=true) with Reason carrying why the network
// could not refresh it.
type ExperimentAssignmentResult struct {
	// FromCache reports that Assignment was served from the last-known-good
	// cache over a transient failure rather than a fresh 200 body.
	FromCache bool
	// Reason is the taxonomy code when the cache was served over a
	// transient failure ("transient_429", "malformed_response", "http_0",
	// ...); empty on a fresh 200. (The assignment's own not-assigned reason
	// is Assignment.Reason.)
	Reason string
	// Assignment is the verdict served to the caller.
	Assignment ExperimentAssignment
}

// expCache is one durable last-known-good assignment record, keyed by the
// scope it was fetched for. Records survive restarts (with
// ExperimentAssignmentCachePath) and are replaced only by a fresher fetched
// verdict — or dropped by the real-subjects sentinel.
type expCache struct {
	Scope         string `json:"scope"`
	ExperimentKey string `json:"experiment_key"`
	Body          string `json:"body"`
	FetchedAtMS   int64  `json:"fetched_at_ms"`
}

// expCacheFile is the durable cache file's shape: this client's record set.
// Like the remote-config cache the file belongs to ONE client scope set:
// records outside this client's (app, environment, subject, URL) prefix are
// misses at load and are dropped by the next persisted change.
type expCacheFile struct {
	Records []expCache `json:"records"`
}

// experimentsState is the per-client experiment machinery: the held record
// map, the per-scope request fence, the automatic-lane halt flag, and the
// exposure dedupe set. All fields are guarded by mu; the HTTP fetch itself
// runs outside it, so concurrent fetches are legal.
type experimentsState struct {
	mu sync.Mutex

	baseURL    string
	apiKey     string
	cachePath  string
	appKey     string
	envKey     string
	subjectKey string

	// scopePrefix stamps every record with the (app, environment, subject,
	// url) tuple it was fetched for; the full scope appends the escaped
	// experiment key. Same escaped-join construction as the remote-config
	// scope, so two distinct tuples can never collide. INVARIANT: the
	// scope covers every input that varies the fetch URL —
	// buildExperimentAssignmentURL builds from exactly (base URL incl.
	// prefix, app_key, environment_key, experiment_key, subject_key) and
	// this SDK sends NO targeting attributes; if attribute params are ever
	// added, they must join the scope or records would be served across
	// attribute sets.
	scopePrefix string

	// held is the freshest served record per scope for this process,
	// preloaded from the durable file at construction. It is updated even
	// when the durable write fails, so a later offline fetch falls back to
	// the freshest served assignment.
	held map[string]*expCache

	// Requests are numbered per state; settled maps a scope to the highest
	// sequence whose AUTHORITATIVE outcome landed (fresh 200, 401/403,
	// permanent HTTP error) — the same out-of-order fence as remote config:
	// an older response must neither roll back a newer verdict nor, for the
	// sentinel, drop a record a newer fetch already refreshed.
	fetchSeq uint64
	settled  map[string]uint64

	// autoHalted marks the automatic revalidation lane halted after an
	// authoritative 401/403 it received (Amendment 2 Extra 2). The lane
	// goroutine exits on it; only re-initialization (a new client) resumes
	// automatic revalidation. Host-triggered fetches ignore it entirely.
	autoHalted bool

	// exposures is the client-side exposure dedupe state: one reservation
	// per assignment IDENTITY — experiment key + version + assignment key
	// (experimentExposureDedupeKey) — in this client instance (per-launch,
	// matching the producer contract's per-session dedupe). A reservation
	// is PENDING while its emitting attempt is in flight, CONVERTED once
	// an exposure was actually admitted, and removed when the attempt
	// failed — concurrent duplicates wait on the pending attempt rather
	// than reporting success for an emission that may still re-arm.
	exposures map[string]*expExposureClaim

	// revalidateDelayFn overrides the automatic lane's cycle delay; test
	// seam (like Client.jitter), nil in production. Set only before the
	// lane goroutine starts.
	revalidateDelayFn func() time.Duration
}

func newExperimentsState(cfg Config) *experimentsState {
	exp := &experimentsState{
		baseURL:    cfg.ExperimentsURL,
		apiKey:     cfg.APIKey,
		cachePath:  cfg.ExperimentAssignmentCachePath,
		appKey:     cfg.AppID,
		envKey:     cfg.EnvironmentID,
		subjectKey: cfg.ExperimentSubjectKey,
		held:       make(map[string]*expCache),
		settled:    make(map[string]uint64),
		exposures:  make(map[string]*expExposureClaim),
	}
	exp.scopePrefix = escapeRemoteConfigSegment(exp.appKey) + rcScopeSeparator +
		escapeRemoteConfigSegment(exp.envKey) + rcScopeSeparator +
		escapeRemoteConfigSegment(exp.subjectKey) + rcScopeSeparator +
		strings.TrimRight(cfg.ExperimentsURL, "/") + rcScopeSeparator
	return exp
}

func (e *experimentsState) scopeFor(experimentKey string) string {
	return e.scopePrefix + escapeRemoteConfigSegment(experimentKey)
}

// buildExperimentAssignmentURL builds the assignment GET: the configured
// prefixed base plus the fixed route and the four required routing params
// (url.Values escapes each; the server ignores unknown params and evaluates
// only allowlisted ones — this SDK sends no targeting attributes, matching
// its remote-config fetch, which exposes none either).
func buildExperimentAssignmentURL(baseURL, appKey, environmentKey, experimentKey, subjectKey string) string {
	query := url.Values{}
	query.Set("app_key", appKey)
	query.Set("environment_key", environmentKey)
	query.Set("experiment_key", experimentKey)
	query.Set("subject_key", subjectKey)
	return strings.TrimRight(baseURL, "/") + "/runtime/experiments/assignment?" + query.Encode()
}

// The two machine-readable not-assigned reasons (absent = the legacy
// traffic-gate miss). Any OTHER reason value is not a shape this SDK knows
// and classifies as malformed rather than being served as a verdict.
const (
	experimentReasonKillSwitch         = "kill_switch"
	experimentReasonTargetingUnmatched = "targeting_unmatched"
)

// parseExperimentAssignmentBody parses an assignment body end-to-end: a JSON
// object decoding into the assignment shape AND complete for its verdict
// shape — a syntactically valid but incomplete body must classify as the
// transient malformed_response (serving the last-known-good cache), never be
// installed as a fresh verdict the producers and cache would then trust.
// Required per shape (the server contract always sends these):
//   - both shapes: non-empty experiment_key and assignment_key (the
//     deterministic assignment identifier is computed before any verdict and
//     is always present; it is also part of the exposure dedupe identity)
//     and a non-negative version — the typed decode already rejects
//     fractional, exponent-form, overflowing, and non-numeric version
//     values (any of them errors json.Unmarshal into the int64 field, never
//     silently truncates), so the explicit check covers the one
//     representable-but-invalid form;
//   - assigned: non-empty variant_key and a KNOWN boundary.assignment_unit —
//     and for the client_id unit a grammar-valid subject_fact_key, the one
//     value the analytics facts rely on;
//   - not assigned: a known reason (absent = traffic gate, kill_switch,
//     targeting_unmatched); an unknown reason is not a verdict this SDK can
//     represent.
//
// Wrong TYPES anywhere in the tree — an array/string variant_payload, a
// non-object boundary — also fail the typed decode and classify malformed.
// Returns ok=false for anything unusable.
func parseExperimentAssignmentBody(body string) (ExperimentAssignment, bool) {
	var assignment ExperimentAssignment
	if err := json.Unmarshal([]byte(body), &assignment); err != nil {
		return ExperimentAssignment{}, false
	}
	if strings.TrimSpace(assignment.ExperimentKey) == "" || strings.TrimSpace(assignment.AssignmentKey) == "" {
		return ExperimentAssignment{}, false
	}
	if assignment.Version < 0 {
		return ExperimentAssignment{}, false
	}
	if assignment.Assigned {
		if strings.TrimSpace(assignment.VariantKey) == "" {
			return ExperimentAssignment{}, false
		}
		switch assignment.Boundary.AssignmentUnit {
		case experimentAssignmentUnitSynthetic:
		case experimentAssignmentUnitClientID:
			if !experimentSubjectFactKeyPattern.MatchString(assignment.SubjectFactKey) {
				return ExperimentAssignment{}, false
			}
		default:
			return ExperimentAssignment{}, false
		}
		return assignment, true
	}
	switch assignment.Reason {
	case "", experimentReasonKillSwitch, experimentReasonTargetingUnmatched:
		return assignment, true
	default:
		return ExperimentAssignment{}, false
	}
}

// serveExperimentAssignmentCache serves the cached assignment for a
// transient failure, or fails when no usable cache exists.
func serveExperimentAssignmentCache(cache *expCache, reason string) (ExperimentAssignmentResult, string) {
	if cache != nil {
		if assignment, ok := parseExperimentAssignmentBody(cache.Body); ok {
			return ExperimentAssignmentResult{
				FromCache:  true,
				Reason:     reason,
				Assignment: assignment,
			}, ""
		}
	}
	return ExperimentAssignmentResult{}, reason
}

// experimentSentinelBody reports whether a 403 body is EXACTLY the
// real-subjects sentinel: a parseable JSON object whose `error` member
// string-equals the sentinel. A truncated body is never a sentinel — the
// equality cannot be trusted — and stays a generic 403.
func experimentSentinelBody(body []byte, bodyIncomplete bool) bool {
	if bodyIncomplete {
		return false
	}
	var wire struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &wire) != nil {
		return false
	}
	return wire.Error == experimentRealSubjectsDisabledSentinel
}

// applyExperimentAssignment decides one fetch outcome from the transport
// response and the cache record captured at dispatch. Pure (no IO, no state)
// so tests can drive every branch. Returns:
//   - result — the served outcome, meaningful only when failure is empty;
//   - newCache — non-nil means "install this record"; only a fresh 200
//     produces one;
//   - authoritative — the outcomes that settle the request fence: a fresh
//     200, an unauthorized response (401/403, sentinel included), and a
//     permanent HTTP error;
//   - dropCache — the sentinel verdict: drop the cached record (and with it
//     the subject_fact_key). Fenced at install time like everything else, so
//     a stale sentinel cannot drop a record a newer fetch refreshed;
//   - failure — the taxonomy code when the fetch produced no usable verdict.
//
// A transport-level error arrives here as status 0.
func applyExperimentAssignment(cache *expCache, resp remoteConfigResponse, nowMS int64) (result ExperimentAssignmentResult, newCache *expCache, authoritative bool, dropCache bool, failure string) {
	if resp.status == 200 {
		// Only a 200 needs its body; every non-200 below classifies by
		// STATUS alone, so a truncated 401 still fails closed and a
		// truncated 404 is still permanent. (The sentinel is the one
		// body-sensitive refinement, and a truncated 403 stays generic.)
		if !resp.bodyIncomplete && len(resp.body) <= expMaxBodyBytes {
			if assignment, ok := parseExperimentAssignmentBody(string(resp.body)); ok {
				return ExperimentAssignmentResult{Assignment: assignment},
					&expCache{Body: string(resp.body), FetchedAtMS: nowMS},
					true, false, ""
			}
		}
		result, failure = serveExperimentAssignmentCache(cache, "malformed_response")
		return result, nil, false, false, failure
	}

	// An unauthorized response is an authoritative refusal for THIS fetch:
	// fail closed, no latch, cache untouched — except the real-subjects
	// sentinel, whose exact 403 body additionally condemns the cached
	// record (the consumer half of the platform's flip-off defense).
	if resp.status == 401 || resp.status == 403 {
		dropCache = resp.status == 403 && experimentSentinelBody(resp.body, resp.bodyIncomplete)
		return ExperimentAssignmentResult{}, nil, true, dropCache, "unauthorized"
	}

	// The cache fallback is reserved for failures a retry can plausibly
	// fix. The assignment 503 (`kill switch state unavailable`) is
	// transient by contract; 404 (unknown experiment) and every other
	// unexpected status is authoritative and permanent. No cooldown arms
	// on 429 — this plane has no Retry-After contract today.
	var reason string
	switch {
	case resp.status == 0:
		reason = "http_0"
	case resp.status == 408:
		reason = "transient_408"
	case resp.status == 429:
		reason = "transient_429"
	case resp.status >= 500:
		reason = fmt.Sprintf("transient_%d", resp.status)
	default:
		return ExperimentAssignmentResult{}, nil, true, false, fmt.Sprintf("http_%d", resp.status)
	}
	result, failure = serveExperimentAssignmentCache(cache, reason)
	return result, nil, false, false, failure
}

// preload seeds held from the durable cache file: records inside this
// client's scope prefix, freshest per scope, caps applied — so transient
// fallbacks and the automatic lane work across restarts.
func (e *experimentsState) preload() {
	if e.cachePath == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, record := range e.durableRecords() {
		if !strings.HasPrefix(record.Scope, e.scopePrefix) || record.ExperimentKey == "" {
			continue
		}
		if _, ok := parseExperimentAssignmentBody(record.Body); !ok {
			continue
		}
		if prior := e.held[record.Scope]; prior != nil && prior.FetchedAtMS >= record.FetchedAtMS {
			continue
		}
		e.held[record.Scope] = &record
	}
	e.enforceCapsLocked()
}

// durableRecords loads the durable cache file's record list, or nil: a
// missing, over-limit, or unparseable file is a clean start, decided before
// unbounded memory is spent reading it.
func (e *experimentsState) durableRecords() []expCache {
	file, err := os.Open(e.cachePath)
	if err != nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(file, expCacheReadLimit+1))
	_ = file.Close()
	if err != nil || int64(len(data)) > expCacheReadLimit {
		return nil
	}
	var decoded expCacheFile
	if json.Unmarshal(data, &decoded) != nil {
		return nil
	}
	return decoded.Records
}

// enforceCapsLocked evicts oldest-stamped records past the count and
// body-byte budgets.
func (e *experimentsState) enforceCapsLocked() {
	totalBodyBytes := 0
	for _, record := range e.held {
		totalBodyBytes += len(record.Body)
	}
	for len(e.held) > expCacheMaxRecords || totalBodyBytes > expCacheBodyBudget {
		oldestScope := ""
		var oldest *expCache
		for scope, record := range e.held {
			if oldest == nil || record.FetchedAtMS < oldest.FetchedAtMS ||
				(record.FetchedAtMS == oldest.FetchedAtMS && scope < oldestScope) {
				oldestScope, oldest = scope, record
			}
		}
		if oldest == nil {
			return
		}
		totalBodyBytes -= len(oldest.Body)
		delete(e.held, oldestScope)
	}
}

// saveDurableLocked persists the held record set atomically (deterministic
// order; no JSON HTML escaping — the bodies are JSON text read back only by
// this SDK, and default escaping could push a legitimate record set past the
// bounded read; see marshalRCCache). Create-only for parents, like the
// remote-config cache: the path names a file in a caller-chosen directory.
func (e *experimentsState) saveDurableLocked() error {
	records := make([]expCache, 0, len(e.held))
	for _, record := range e.held {
		records = append(records, *record)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].FetchedAtMS != records[j].FetchedAtMS {
			return records[i].FetchedAtMS < records[j].FetchedAtMS
		}
		return records[i].Scope < records[j].Scope
	})
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(expCacheFile{Records: records}); err != nil {
		return err
	}
	return writePrivateFileAtomic(e.cachePath, buf.Bytes(), os.Rename, nil)
}

// installLocked settles an authoritative fetch outcome and, when it may,
// installs it: the per-scope sequence fence first (an outcome older than the
// newest settled one for its scope installs NOTHING — a stale sentinel drops
// nothing, a stale 200 rolls nothing back), then the sentinel drop or the
// fresh-record install, then the best-effort durable write. Returns whether
// the sentinel drop happened and whether a durable write failed (the caller
// surfaces both).
func (e *experimentsState) installLocked(seq uint64, scope, experimentKey string, newCache *expCache, authoritative, dropCache bool) (dropped, persistFailed bool) {
	if seq <= e.settled[scope] {
		return false, false
	}
	if authoritative {
		e.settled[scope] = seq
	}
	switch {
	case dropCache:
		if _, exists := e.held[scope]; !exists {
			return false, false
		}
		delete(e.held, scope)
		// Best-effort durable drop: on a failed rewrite the condemned
		// record can survive on disk into the next launch, where the next
		// fetch of that experiment re-receives the sentinel and re-drops
		// it (or the surface is back on and a fresh 200 replaces it). The
		// failure is surfaced either way.
		if e.cachePath != "" && e.saveDurableLocked() != nil {
			return true, true
		}
		return true, false
	case newCache != nil:
		newCache.Scope = scope
		newCache.ExperimentKey = experimentKey
		if prior := e.held[scope]; prior != nil && newCache.FetchedAtMS <= prior.FetchedAtMS {
			// The wall clock can move backward (an NTP correction): the
			// record being installed must still order above the record it
			// supersedes, or a restart would revive the stale one.
			newCache.FetchedAtMS = prior.FetchedAtMS + 1
		}
		e.held[scope] = newCache
		e.enforceCapsLocked()
		// The in-process record is kept even when the durable write fails:
		// the freshest served assignment stays this process's fallback.
		if e.cachePath != "" && e.saveDurableLocked() != nil {
			return false, true
		}
	}
	return false, false
}

// cachedExperimentKeys is the automatic lane's work list: the experiment
// keys this client holds records for, deterministic order.
func (e *experimentsState) cachedExperimentKeys() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	seen := make(map[string]struct{}, len(e.held))
	keys := make([]string, 0, len(e.held))
	for _, record := range e.held {
		if _, dup := seen[record.ExperimentKey]; dup {
			continue
		}
		seen[record.ExperimentKey] = struct{}{}
		keys = append(keys, record.ExperimentKey)
	}
	sort.Strings(keys)
	return keys
}

// expExposureClaim is one assignment key's exposure reservation. done is
// closed when the emitting attempt settles; converted (written before the
// close, so any waiter released by done observes it) records whether the
// exposure was actually admitted.
type expExposureClaim struct {
	done      chan struct{}
	converted bool
}

// beginExposureClaim reserves the one-per-launch exposure slot for an
// assignment identity (experimentExposureDedupeKey). The second result
// reports whether THIS caller owns the emitting attempt; false hands back
// the existing reservation (pending or converted) for the caller to wait
// on. A duplicate must never report success off a reservation that has not
// CONVERTED — a pending attempt can still fail and re-arm.
func (e *experimentsState) beginExposureClaim(dedupeKey string) (*expExposureClaim, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if existing := e.exposures[dedupeKey]; existing != nil {
		return existing, false
	}
	claim := &expExposureClaim{done: make(chan struct{})}
	e.exposures[dedupeKey] = claim
	return claim, true
}

// settleExposureClaim settles the reservation the caller owns: converted
// keeps it forever (the per-launch dedupe mark); a failed attempt removes it
// so a later — or a concurrently waiting — call can emit.
func (e *experimentsState) settleExposureClaim(dedupeKey string, claim *expExposureClaim, converted bool) {
	e.mu.Lock()
	claim.converted = converted
	if !converted {
		delete(e.exposures, dedupeKey)
	}
	e.mu.Unlock()
	close(claim.done)
}

func (e *experimentsState) haltAutoLane() {
	e.mu.Lock()
	e.autoHalted = true
	e.mu.Unlock()
}

func (e *experimentsState) autoLaneHalted() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.autoHalted
}

// revalidateDelay is the automatic lane's next cycle delay:
// max(configured interval, 60s floor) — no server cadence signal exists on
// this endpoint (no Cache-Control), so the floor is the anchor.
func (e *experimentsState) revalidateDelay(configured time.Duration) time.Duration {
	if e.revalidateDelayFn != nil {
		return e.revalidateDelayFn()
	}
	if configured > expRevalidateFloor {
		return configured
	}
	return expRevalidateFloor
}

// expFetchError is a fetch outcome with no usable verdict; the taxonomy code
// is the machine-readable part of the message.
func expFetchError(code string) error {
	return fmt.Errorf("shardpilot experiment assignment fetch failed: %s", code)
}

// FetchExperimentAssignment performs one explicit (host-triggered)
// assignment fetch for experimentKey and blocks for its outcome. A nil error
// means the caller has a usable verdict — fresh, or served from the
// last-known-good cache over a transient failure (FromCache=true with
// Reason set). A non-nil error means no usable verdict: the taxonomy code is
// in the error text ("unauthorized" fails closed without serving the cache;
// "http_<status>" is permanent; "transient_..."/"http_0"/
// "malformed_response" are transient with no usable cache;
// "experiments_not_configured", "experiment_key_required", and
// "subject_key_unavailable" fail before any network use). A fetch ended by
// the CALLER's context returns that context error with no cache fallback and
// no side effects; an SDK-internal timeout that stalled the response body
// preserves the received status and classifies by it. Fetching is not
// consent-gated. Concurrent fetches are legal; an older response never
// overwrites (or, for the sentinel, drops past) a newer settled outcome.
// Fetches are fenced by the client lifecycle exactly like FetchRemoteConfig:
// after Close begins they are rejected with ErrClosed, and Close waits for
// in-flight fetches to settle.
//
// Every fetch classifies independently — a 401/403 received here never
// halts anything. Only the automatic revalidation lane halts on
// authoritative refusals, and only for itself.
func (c *Client) FetchExperimentAssignment(ctx context.Context, experimentKey string) (ExperimentAssignmentResult, error) {
	result, code, err := c.fetchExperimentAssignmentOutcome(ctx, experimentKey)
	if err != nil {
		return ExperimentAssignmentResult{}, err
	}
	if code != "" {
		return ExperimentAssignmentResult{}, expFetchError(code)
	}
	return result, nil
}

// fetchExperimentAssignmentOutcome is the shared fetch core: the public
// method formats the failure code into an error, while the automatic lane
// reads it directly (its halt trigger is the "unauthorized" class). A
// non-nil error is a lifecycle or caller-context outcome (ErrClosed, the
// caller's own context error), mutually exclusive with a failure code.
func (c *Client) fetchExperimentAssignmentOutcome(ctx context.Context, experimentKey string) (ExperimentAssignmentResult, string, error) {
	// The same lifecycle fence as Track and FetchRemoteConfig: Close either
	// sees this fetch (and waits for it) or completed its closed store
	// first (and this fetch is rejected).
	c.lifecycleMu.Lock()
	if c.closed.Load() {
		c.lifecycleMu.Unlock()
		return ExperimentAssignmentResult{}, "", ErrClosed
	}
	c.trackWG.Add(1)
	c.lifecycleMu.Unlock()
	defer c.trackWG.Done()
	exp := c.exp
	if exp == nil {
		return ExperimentAssignmentResult{}, "experiments_not_configured", nil
	}
	experimentKey = strings.TrimSpace(experimentKey)
	if experimentKey == "" {
		return ExperimentAssignmentResult{}, "experiment_key_required", nil
	}
	if exp.subjectKey == "" {
		// The fetch route and the cache scope both need the subject key;
		// with none configured there is nothing coherent to fetch. Decided
		// before any network use, mirroring client_id_unavailable.
		return ExperimentAssignmentResult{}, "subject_key_unavailable", nil
	}

	exp.mu.Lock()
	exp.fetchSeq++
	seq := exp.fetchSeq
	scope := exp.scopeFor(experimentKey)
	cache := exp.held[scope]
	fetchURL := buildExperimentAssignmentURL(exp.baseURL, exp.appKey, exp.envKey, experimentKey, exp.subjectKey)
	apiKey := exp.apiKey
	exp.mu.Unlock()

	callerCtx := ctx
	ctx, cancel := contextWithDefaultTimeout(ctx, c.cfg.HTTPTimeout)
	defer cancel()
	// The transport's remote-config GET is exactly this route's shape too —
	// bare authenticated GET, redirects refused, bounded body read — with no
	// If-None-Match: the assignment endpoint has no ETag/304 contract.
	resp, err := c.transport.FetchRemoteConfig(ctx, remoteConfigRequest{
		url:    fetchURL,
		bearer: apiKey,
	})
	if err != nil {
		if callerCtx != nil {
			if ctxErr := callerCtx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				// The CALLER's own context ended the fetch: an abort, not an
				// endpoint outcome — no cache fallback, no fence side
				// effects, just the caller's error back.
				return ExperimentAssignmentResult{}, "", err
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

	now := c.clock.Now()
	result, newCache, authoritative, dropCache, failure := applyExperimentAssignment(cache, resp, now.UnixMilli())

	exp.mu.Lock()
	dropped, persistFailed := exp.installLocked(seq, scope, experimentKey, newCache, authoritative, dropCache)
	exp.mu.Unlock()

	if dropped {
		c.logf("shardpilot experiment assignment: the platform disabled real-subject assignment; dropped the cached assignment (and its subject fact key) for experiment %q", experimentKey)
	}
	if persistFailed {
		c.stats.setLastError("experiment_assignment_cache_persist_failed")
		c.logf("shardpilot experiment assignment: persisting the cache record failed; the in-memory record remains this process's fallback")
	}
	if failure != "" {
		return ExperimentAssignmentResult{}, failure, nil
	}
	return result, "", nil
}

// runExperimentAssignmentRevalidation is the opt-in AUTOMATIC assignment
// revalidation lane (started only when
// Config.ExperimentAssignmentRevalidateInterval > 0): each cycle re-fetches
// every cached experiment key so a running client converges on an operator
// kill or a variant change within one interval. The lane exits when the
// client stops — or when it halts (Amendment 2 Extra 2): after any of ITS
// fetches receives an authoritative 401/403 it stops scheduling until
// re-init. Host-triggered fetches classify per-fetch throughout and never
// resume the lane.
func (c *Client) runExperimentAssignmentRevalidation() {
	defer close(c.expRevalidateDone)
	for {
		timer := time.NewTimer(c.exp.revalidateDelay(c.cfg.ExperimentAssignmentRevalidateInterval))
		select {
		case <-c.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
		if c.revalidateExperimentAssignmentsOnce() {
			return
		}
	}
}

// revalidateExperimentAssignmentsOnce runs one automatic-lane cycle and
// reports whether the lane must exit (halted, or the client closed).
func (c *Client) revalidateExperimentAssignmentsOnce() (exit bool) {
	if c.exp.autoLaneHalted() {
		return true
	}
	for _, experimentKey := range c.exp.cachedExperimentKeys() {
		_, code, err := c.fetchExperimentAssignmentOutcome(context.Background(), experimentKey)
		if err != nil {
			// Only lifecycle outcomes surface as errors on a background
			// context; the lane winds down with the client.
			if errors.Is(err, ErrClosed) {
				return true
			}
			continue
		}
		if code == "unauthorized" {
			// The endpoint authoritatively refused an unattended fetch:
			// halt the lane until re-init. (The sentinel is a 403 and
			// halts too — its cache drop already happened in the fetch.)
			c.exp.haltAutoLane()
			c.logf("shardpilot experiment assignment revalidation: halted after an authoritative 401/403; host-triggered fetches remain available, and the lane resumes only on re-initialization")
			return true
		}
	}
	return false
}
