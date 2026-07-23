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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Remote-config client: GETs the published configuration for this
// (workspace, environment, client) scope from the remote-config endpoint and
// serves typed values, with a durable last-known-good cache so a restart or
// an offline start still gets the previously fetched configuration. It is
// deliberately separate from the ingest transport routes: configuration is
// FETCHED (a GET of one resource, ETag-revalidated), never batched, and
// authenticates with the publishable Config.APIKey only — the ingest
// Config.Token never authenticates the remote-config endpoint. Every fetch
// is explicit (FetchRemoteConfig); there is no automatic refresh, no
// Cache-Control interpretation, no experiment assignment, and no client-side
// rule evaluation. Fetching is NOT consent-gated: denied consent neither
// blocks the fetch nor clears the cache (configuration is client-public
// tuning, not telemetry).
//
// Fetch semantics (one fetch = one HTTP GET, decided by applyRemoteConfig):
//   - 200 with a parseable JSON object body — fresh values are served and
//     the cache record is overwritten (body + response ETag).
//   - 304 Not Modified — the cached snapshot is served (FromCache=true) and
//     the record's freshness stamp is renewed: the endpoint confirmed the
//     body as current.
//   - A transient failure (offline, 408, 429, 5xx, malformed or over-cap
//     body) — the cached snapshot is served with FromCache=true and Reason
//     carrying why; with no usable cache the fetch fails.
//   - 401/403 — fails CLOSED: the cached snapshot is never served for this
//     outcome (a revoked or wrong key must not keep supplying
//     configuration), but the cache record and the getter snapshot are left
//     untouched — a later 200 under a valid credential resumes normally.
//   - Any other status (404, an unexpected 3xx, 413, other 4xx) is a
//     PERMANENT failure: the fetch fails rather than serving stale values as
//     healthy; cache and snapshot stay untouched.
//
// Permanent and fail-closed outcomes are authoritative for THE FETCH THAT
// RECEIVED THEM — they do not latch: every fetch classifies independently,
// so a later transient failure still serves the last-known-good cache. This
// matches the canonical Defold/Unity behavior (ports, not redesigns).
//
// A 429 additionally arms an in-memory cooldown from its Retry-After header
// (digits-only, floored at 1s, clamped at 24h; absent or malformed reads as
// the floor). An explicit fetch inside the window does not touch the network:
// it returns the cache-served transient_429 outcome, indistinguishable from a
// live 429. This is a deliberate delta vs the Defold/Unity reference clients
// (which ignore Retry-After on this route) — the client half of the
// control plane's server-side RemoteConfigFetchRateLimit.

// rcMaxBodyBytes caps how much of a remote-config response body is read: the
// server contract caps targeted configuration at 1MB (answering 413 above
// it), so a larger body is malformed by definition.
const rcMaxBodyBytes = 1 << 20

// rcCooldownClampSeconds bounds the honored 429 cooldown at one day,
// mirroring the ingest plane's maxRetryAfter clamp, so a hostile or garbled
// header can never park remote-config fetches longer.
const rcCooldownClampSeconds = 24 * 60 * 60

// rcScopeSeparator joins escaped scope components. No escaped component can
// contain it (0x1F is outside the unreserved set and escapes to %1F), so two
// distinct (workspace, environment, client, url) tuples can never collide
// into one scope string.
const rcScopeSeparator = "\x1f"

// RemoteConfigResult is one explicit fetch's outcome with usable values.
// A cache-served transient IS a success: the caller has usable configuration
// (FromCache=true) with Reason carrying why the network could not refresh it.
type RemoteConfigResult struct {
	// FromCache reports that Values were served from the last-known-good
	// cache — either a 304 revalidation or a transient-failure fallback —
	// rather than a fresh 200 body.
	FromCache bool
	// Reason is the taxonomy code when the cache was served over a transient
	// failure ("transient_429", "malformed_response", "http_0", ...); empty
	// on a fresh 200 and on a clean 304 revalidation.
	Reason string
	// Values is the configuration map served to the caller. The caller may
	// mutate it freely; the getter snapshot is an independent copy.
	Values map[string]any
	// Version is the published configuration version from the response
	// wrapper; meaningful only when HasVersion. An unwrapped payload carries
	// none (a "version" KEY in an unwrapped payload is configuration).
	Version    float64
	HasVersion bool
}

// rcCache is the durable last-known-good record: ONE record, keyed by the
// scope string it was fetched for, holding the raw response text and the
// freshness stamp that orders same-scope records. There is no TTL — it
// survives restarts and is served offline; only a fresher served outcome
// replaces it.
type rcCache struct {
	Scope       string `json:"scope"`
	ETag        string `json:"etag"`
	Body        string `json:"body"`
	FetchedAtMS int64  `json:"fetched_at_ms"`
	// The ADR-0310 attribute signature of the fetch that produced this
	// record ("" = attribute-less, which every pre-ADR-0310 durable record
	// unmarshals to; otherwise a NON-REVERSIBLE SHA-256 digest of the
	// normalized attribute query — never the attribute values themselves,
	// so the durable cache puts zero personal-data-shaped bytes at rest and
	// nothing about the targeting set survives a consent downgrade in
	// readable form). The ETag revalidates ONLY against a fetch carrying
	// the SAME signature: a shared publication validator must never 304 a
	// differently-targeted request into serving the previous target's body.
	// Value SERVING stays scope-keyed regardless of signature (the
	// documented v1 limit — a cached body may reflect the previously sent
	// attribute set until the next successful fetch).
	AttributeSignature string `json:"attribute_signature,omitempty"`
}

// remoteConfigState is the per-client remote-config machinery: the held
// in-process cache record (which survives a failed durable write), the
// getter snapshot, the request fence, and the 429 cooldown. All fields are
// guarded by mu; the HTTP fetch itself runs outside it, so concurrent
// explicit fetches are legal.
type remoteConfigState struct {
	mu sync.Mutex

	fetchURL  string
	apiKey    string
	cachePath string
	clientID  string
	scope     string

	// attributes is the developer-supplied targeting attribute set
	// (ADR-0310, dark behind Config.RemoteConfigAttributesEnabled). Stored
	// RAW: normalization (vocabulary allowlist, bounds, sort) happens at
	// FETCH time so it always reflects the current experiment vocabulary,
	// and the consent gate is evaluated per fetch — a downgrade after
	// SetRemoteConfigAttributes makes the very next fetch attribute-less.
	attributes map[string]string

	// held is the freshest served record for this process. It is updated
	// even when the durable write fails, so a later offline fetch falls back
	// to the freshest served configuration rather than reviving an older
	// on-disk record.
	held *rcCache

	// snapshot backs the typed getters: an independent copy of the last
	// served values (fresh or cached), last-served-wins. Nothing clears it
	// except replacement by a later served outcome.
	snapshot   map[string]any
	version    float64
	hasVersion bool

	// Requests are numbered, and settled maps a scope to the highest
	// sequence whose AUTHORITATIVE outcome has landed for it — a fresh 200,
	// a 304 revalidation, an unauthorized response, or a permanent HTTP
	// error. Only a fetch newer than every settled one for its scope may
	// install: with two fetches in flight, responses can arrive out of
	// order, and an older success must neither roll back a newer
	// configuration nor sneak values in after a newer fail-closed outcome.
	// Non-authoritative outcomes (transient failures, cache-served
	// fallbacks) never settle — they say nothing about the current
	// configuration, so they must not fence off a fresh response still in
	// flight.
	fetchSeq uint64
	settled  map[string]uint64

	// cooldownUntil is the in-memory 429 next-fetch-allowed deadline. ONE
	// deadline per client instance, not per scope — acceptable conservatism
	// (a client's scope is fixed by its config). Monotone: a new 429 can
	// only extend it, and it is never persisted; it expires only by time.
	cooldownUntil time.Time
}

func newRemoteConfigState(cfg Config) *remoteConfigState {
	rc := &remoteConfigState{
		fetchURL:  buildRemoteConfigURL(cfg.RemoteConfigURL, cfg.WorkspaceID, cfg.EnvironmentID, cfg.AnonymousID),
		apiKey:    cfg.APIKey,
		cachePath: cfg.RemoteConfigCachePath,
		clientID:  cfg.AnonymousID,
		settled:   make(map[string]uint64),
	}
	if rc.clientID != "" {
		rc.scope = buildRemoteConfigScope(cfg.WorkspaceID, cfg.EnvironmentID, rc.clientID, cfg.RemoteConfigURL)
	}
	return rc
}

// preload serves the persisted last-known-good snapshot immediately after
// construction: getters work before (and without) any fetch when a durable
// record for this exact scope exists.
func (rc *remoteConfigState) preload() {
	if rc.clientID == "" || rc.cachePath == "" {
		return
	}
	record := rc.durableRecord(rc.scope)
	if record == nil {
		return
	}
	values, version, hasVersion, ok := parseRemoteConfigBody(record.Body)
	if !ok {
		return
	}
	rc.held = record
	rc.snapshot = values
	rc.version = version
	rc.hasVersion = hasVersion
}

// escapeRemoteConfigSegment percent-escapes every byte outside the RFC 3986
// unreserved set, so an identifier containing "/", "%", or spaces cannot
// smuggle extra path segments into the fetch URL. The escaping is injective
// ("%" itself included), so two distinct raw strings can never escape to the
// same output — which is also what makes the scope join unambiguous.
func escapeRemoteConfigSegment(value string) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		ch := value[i]
		switch {
		case ch >= 'A' && ch <= 'Z', ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9',
			ch == '.', ch == '_', ch == '~', ch == '-':
			b.WriteByte(ch)
		default:
			fmt.Fprintf(&b, "%%%02X", ch)
		}
	}
	return b.String()
}

func buildRemoteConfigURL(baseURL, workspaceID, environmentID, clientID string) string {
	return strings.TrimRight(baseURL, "/") + "/config/v1/" +
		escapeRemoteConfigSegment(workspaceID) + "/" +
		escapeRemoteConfigSegment(environmentID) + "/" +
		escapeRemoteConfigSegment(clientID)
}

// appendRemoteConfigAttributes appends the ADR-0310 targeting attributes as
// query parameters — already normalized (allowlisted, bounded, SORTED) by
// normalizeExperimentAttributes, escaped with the same injective escaper the
// path segments use (the experiment assignment URL's exact discipline).
func appendRemoteConfigAttributes(fetchURL string, attributes []expAttribute) string {
	if len(attributes) == 0 {
		return fetchURL
	}
	var b strings.Builder
	b.WriteString(fetchURL)
	for i, attribute := range attributes {
		if i == 0 {
			b.WriteByte('?')
		} else {
			b.WriteByte('&')
		}
		b.WriteString(escapeRemoteConfigSegment(attribute.Name))
		b.WriteByte('=')
		b.WriteString(escapeRemoteConfigSegment(attribute.Value))
	}
	return b.String()
}

// buildRemoteConfigScope stamps a cache record with the (workspace,
// environment, client, url) tuple it was fetched for. Components are escaped
// like URL segments and joined with a separator no escaped component can
// contain; the base URL is re-trimmed so equivalent spellings of the same
// endpoint can never produce distinct scopes.
func buildRemoteConfigScope(workspaceID, environmentID, clientID, baseURL string) string {
	return escapeRemoteConfigSegment(workspaceID) + rcScopeSeparator +
		escapeRemoteConfigSegment(environmentID) + rcScopeSeparator +
		escapeRemoteConfigSegment(clientID) + rcScopeSeparator +
		strings.TrimRight(baseURL, "/")
}

// parseRemoteConfigBody parses a configuration body end-to-end: the body
// must be a JSON object. A top-level `values` member that is a JSON object
// is the configuration map, with the wrapper's numeric `version` as
// metadata; a `values` member of any other shape (null, array, scalar) is
// MALFORMED — falling back to serving the wrapper would expose wrapper
// fields as configuration. An object WITHOUT a `values` member is served as
// the map itself (unwrapped compatibility; its "version" key, if any, is
// then configuration, and no wrapper version exists). Returns ok=false for
// anything unusable.
func parseRemoteConfigBody(body string) (values map[string]any, version float64, hasVersion bool, ok bool) {
	var decoded map[string]any
	if err := json.Unmarshal([]byte(body), &decoded); err != nil || decoded == nil {
		// A non-object body (array, scalar, empty) fails to unmarshal into a
		// map; a literal JSON null "succeeds" into a nil map and is caught
		// by the nil check — accepting it would serve an empty configuration
		// for a body that carried none.
		return nil, 0, false, false
	}
	if raw, present := decoded["values"]; present {
		obj, isObject := raw.(map[string]any)
		if !isObject {
			return nil, 0, false, false
		}
		// The published version is wrapper metadata, so it is read only
		// here: in an unwrapped payload a config key named "version" is
		// configuration, not a revision marker. A non-numeric wrapper
		// version is simply absent.
		version, hasVersion = decoded["version"].(float64)
		return obj, version, hasVersion, true
	}
	return decoded, 0, false, true
}

// serveRemoteConfigCache serves the cached snapshot for a transient failure,
// or fails when no usable cache exists. A served snapshot is still a SUCCESS
// — the caller has usable configuration — with FromCache=true and Reason
// carrying why the network could not refresh it.
func serveRemoteConfigCache(cache *rcCache, reason string) (RemoteConfigResult, string) {
	if cache != nil {
		if values, version, hasVersion, ok := parseRemoteConfigBody(cache.Body); ok {
			return RemoteConfigResult{
				FromCache:  true,
				Reason:     reason,
				Values:     values,
				Version:    version,
				HasVersion: hasVersion,
			}, ""
		}
	}
	return RemoteConfigResult{}, reason
}

// applyRemoteConfig decides one fetch outcome from the transport response
// and the cache record captured at dispatch. Pure (no IO, no state) so tests
// can drive every branch. Returns:
//   - result — the served outcome, meaningful only when failure is empty;
//   - newCache — non-nil means "persist this record"; it exists only for a
//     fresh 200, so no failure and no cache-served outcome ever disturbs the
//     last-known-good record;
//   - authoritative — the outcomes that settle the request fence: a fresh
//     200, a successful 304 revalidation, an unauthorized response, and a
//     permanent HTTP error;
//   - revalidated — non-nil exactly for a successful 304 revalidation: the
//     cached record with its freshness stamp renewed (the body is unchanged;
//     the endpoint just confirmed it as current). Whether the renewal may
//     land is decided at install time;
//   - failure — the taxonomy code when the fetch produced no usable values.
//
// A transport-level error arrives here as status 0.
func applyRemoteConfig(cache *rcCache, resp remoteConfigResponse, nowMS int64) (result RemoteConfigResult, newCache *rcCache, authoritative bool, revalidated *rcCache, failure string) {
	if resp.status == 200 {
		// A 200 is the only outcome that needs its body: a mid-stream read
		// failure (bodyIncomplete) makes it unusable — the transient
		// malformed class — while every non-200 below classifies by STATUS
		// alone, so a truncated 401 still fails closed and a truncated
		// 3xx/4xx is still permanent.
		if !resp.bodyIncomplete && len(resp.body) <= rcMaxBodyBytes {
			if values, version, hasVersion, ok := parseRemoteConfigBody(string(resp.body)); ok {
				return RemoteConfigResult{
						Values:     values,
						Version:    version,
						HasVersion: hasVersion,
					}, &rcCache{
						ETag:        resp.etag,
						Body:        string(resp.body),
						FetchedAtMS: nowMS,
					}, true, nil, ""
			}
		}
		result, failure = serveRemoteConfigCache(cache, "malformed_response")
		return result, nil, false, nil, failure
	}

	if resp.status == 304 && cache != nil {
		if values, version, hasVersion, ok := parseRemoteConfigBody(cache.Body); ok {
			// A successful revalidation is authoritative: the endpoint just
			// confirmed the cached ETag as CURRENT, so an older in-flight
			// response must not overwrite it afterwards.
			return RemoteConfigResult{
					FromCache:  true,
					Values:     values,
					Version:    version,
					HasVersion: hasVersion,
				}, nil, true, &rcCache{
					ETag:        cache.ETag,
					Body:        cache.Body,
					FetchedAtMS: nowMS,
				}, ""
		}
		// The revalidated cache turned out unreadable: there is nothing left
		// to serve (a 304 carries no body), so this fails rather than
		// re-serving the very cache that just failed to decode.
		return RemoteConfigResult{}, nil, false, nil, "cache_unreadable_after_304"
	}

	// An unauthorized response is an authoritative "this key may not read
	// this configuration", not a transient outage: serving the cached
	// snapshot would keep a revoked or wrong key supplied with configuration
	// indefinitely. Fail closed; the cache record is kept untouched but is
	// never served for this outcome.
	if resp.status == 401 || resp.status == 403 {
		return RemoteConfigResult{}, nil, true, nil, "unauthorized"
	}

	// The cache fallback is reserved for failures a retry can plausibly fix:
	// no connection (status 0), a request timeout, backpressure, or a
	// server-side error. Any other status — a 404 for a removed environment,
	// an unexpected redirect, a 413, other 4xx, or a 304 with no cache to
	// revalidate — is an authoritative "this configuration is not being
	// served here", so the fetch fails instead of reporting stale values as
	// healthy.
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
		return RemoteConfigResult{}, nil, true, nil, fmt.Sprintf("http_%d", resp.status)
	}
	result, failure = serveRemoteConfigCache(cache, reason)
	return result, nil, false, nil, failure
}

// rcCacheReadLimit bounds how much of the durable cache file is ever read
// back: twice the response-body cap plus a fixed framing overhead. The body
// rides the record as a JSON string written WITHOUT HTML escaping (see
// marshalRCCache), so escaping a valid JSON text at worst doubles it (each
// `"`, `\`, and inter-token whitespace control byte escapes to two bytes;
// valid JSON text carries no raw control characters inside strings, and
// `<`/`>`/`&` pass through verbatim). A record this client could have
// written therefore fits with room to spare — a larger file is not such a
// record and is treated as corrupt without ever being loaded whole,
// mirroring the spool's bounded record read.
const rcCacheReadLimit = 2*rcMaxBodyBytes + 64<<10

// marshalRCCache encodes the cache record without JSON HTML escaping: the
// record is a private file read back only by this SDK, never embedded in
// HTML, and the default escaping would expand every `<`, `>`, and `&` in the
// body SIXFOLD (`<`) — enough to push a legitimately sub-cap body past
// rcCacheReadLimit and lose the last-known-good record to the corrupt-cache
// path on its next load.
func marshalRCCache(record *rcCache) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(record); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// durableRecord loads the usable durable record for the given scope, or nil.
// A record written for any other (workspace, environment, client, url) tuple
// is a miss: its values are never served and its ETag is never sent. So is a
// record whose body no longer parses, or a file over the bounded read limit
// — corrupt cache is a clean start, decided before unbounded memory is spent
// reading it. The next successful fetch overwrites it.
func (rc *remoteConfigState) durableRecord(scope string) *rcCache {
	if rc.cachePath == "" {
		return nil
	}
	file, err := os.Open(rc.cachePath)
	if err != nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(file, rcCacheReadLimit+1))
	_ = file.Close()
	if err != nil || int64(len(data)) > rcCacheReadLimit {
		return nil
	}
	var record rcCache
	if json.Unmarshal(data, &record) != nil || record.Scope != scope {
		return nil
	}
	if _, _, _, ok := parseRemoteConfigBody(record.Body); !ok {
		return nil
	}
	return &record
}

// loadCacheLocked returns the usable cache record for this scope, or nil —
// the FRESHEST of the in-process held record (which survives a failed
// durable write) and the durable record (which another same-app process may
// have refreshed), compared by their fetched-at stamps; the in-process
// record wins ties, being known-good and already backing the getters.
func (rc *remoteConfigState) loadCacheLocked() *rcCache {
	var held *rcCache
	if rc.held != nil && rc.held.Scope == rc.scope {
		held = rc.held
	}
	durable := rc.durableRecord(rc.scope)
	if held != nil && (durable == nil || durable.FetchedAtMS <= held.FetchedAtMS) {
		return held
	}
	return durable
}

// saveDurable persists the cache record atomically: full write to a private
// temp file in the same directory (0600), then rename over the final path —
// rename rather than the anonymous-ID helper's no-overwrite link, because
// this record must be overwritable by design. The write is create-only for
// parents (nil chmod): RemoteConfigCachePath names a FILE in a directory the
// caller chose — often shared (/tmp, an XDG cache dir) — so a pre-existing
// parent's permissions are never changed; tightening is reserved for the
// dedicated SpoolDir state directory the SDK owns.
func (rc *remoteConfigState) saveDurable(record *rcCache) error {
	payload, err := marshalRCCache(record)
	if err != nil {
		return err
	}
	return writePrivateFileAtomic(rc.cachePath, payload, os.Rename, nil)
}

// tombstoneDurable clears the durable record (best-effort) by overwriting it
// with an empty object, which no scope matches — a restart then starts from
// the caller's defaults rather than from rolled-back values. Create-only for
// parents, like saveDurable.
func (rc *remoteConfigState) tombstoneDurable() {
	_ = writePrivateFileAtomic(rc.cachePath, []byte("{}"), os.Rename, nil)
}

// ensurePrivateDir creates dir with the SDK's private mode and TIGHTENS an
// already-existing directory to it: os.MkdirAll is a mode no-op for a
// directory the app pre-created (say 0755), which would leave the documented
// "private 0700 state directory" promise silently unkept — other local users
// could enumerate it, and in a writable directory delete or replace the
// records regardless of their own 0600 modes. A stat or chmod failure is
// returned as the write's error, so the caller's persist-failure (fail-safe)
// path applies; the write never proceeds against a directory whose privacy
// could not be established. chmod is injectable so tests can exercise the
// refused-tighten path deterministically.
func ensurePrivateDir(dir string, chmod func(name string, mode os.FileMode) error) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if info.Mode().Perm() == 0o700 {
		return nil
	}
	return chmod(dir, 0o700)
}

// writePrivateFileAtomic writes payload to path with the SDK's private-file
// discipline: parent directories ensured private 0700, a fully written and
// synced 0600 temp file in the same directory, then an atomic publish via
// the given rename. A non-nil chmod additionally TIGHTENS a pre-existing
// looser parent to 0700 (see ensurePrivateDir) — for paths inside the
// dedicated SpoolDir state directory the SDK owns. A nil chmod is
// create-only: directories the write creates are 0700, but a pre-existing
// parent's permissions are never changed — for RemoteConfigCachePath, whose
// parent is an arbitrary caller-chosen (possibly shared) directory that a
// cache write must not lock other users out of.
func writePrivateFileAtomic(path string, payload []byte, rename func(oldpath, newpath string) error, chmod func(name string, mode os.FileMode) error) error {
	dir := filepath.Dir(path)
	// filepath.Dir yields "." for a bare filename — a caller-chosen state
	// dir of "." (or ""). The privacy guarantee applies to the ACTUAL
	// directory all the same: the tighten path must ensure/chmod "." like
	// any explicit path, or a cwd spool dir would silently keep whatever
	// permissions it already had. Only the create-only (nil chmod) posture
	// skips it — "." always exists, so there is nothing to create and, by
	// that posture's contract, nothing may be chmodded.
	if chmod != nil {
		if err := ensurePrivateDir(dir, chmod); err != nil {
			return err
		}
	} else if dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+"-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer func() { _ = os.Remove(tempPath) }()

	if _, err := temp.Write(payload); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := rename(tempPath, path); err != nil {
		return err
	}
	// The rename is durable only once the parent directory's metadata is on
	// stable storage: fsyncing the temp file made the BYTES durable, but a
	// crash before the directory entry lands can forget the publish (or leave
	// the old name). A failed directory sync reports as a failed write — the
	// caller's mirror-authoritative retry then rewrites and re-syncs, so
	// durability is never silently assumed.
	return syncDir(filepath.Dir(path))
}

// syncDir fsyncs a directory so a just-published entry (a rename over the
// final path, or a created marker) survives a crash. Scope: the publish
// transitions are what this guards — a lost UNLINK, by contrast, degrades to
// a state an existing recovery path re-handles idempotently (a resurrected
// spool.json re-purges at the next non-grant init; a resurrected record
// resends and the server de-duplicates by event_id). On Windows directories
// cannot be opened for syncing and NTFS journals metadata on its own, so the
// sync is skipped there rather than failing every write.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	// The handle is read-only: Close after a successful Sync cannot lose data,
	// so only the Sync outcome decides the write's fate.
	defer handle.Close()
	return handle.Sync()
}

// raiseStampAboveSuperseded orders the record being installed above every
// record it supersedes. Freshness stamps order same-scope records, and the
// wall clock can move backward (an NTP correction): stamped naively, a
// record being installed could rank BELOW the stale records it supersedes,
// and a later offline fetch would roll back to them. Only the relative order
// of stamps matters, so comparisons stay meaningful across restarts.
func (rc *remoteConfigState) raiseStampAboveSuperseded(record *rcCache, served, durable *rcCache) {
	var floor int64
	if served != nil && served.FetchedAtMS > floor {
		floor = served.FetchedAtMS
	}
	if rc.held != nil && rc.held.Scope == record.Scope && rc.held.FetchedAtMS > floor {
		floor = rc.held.FetchedAtMS
	}
	if durable != nil && durable.FetchedAtMS > floor {
		floor = durable.FetchedAtMS
	}
	if record.FetchedAtMS <= floor {
		record.FetchedAtMS = floor + 1
	}
}

// installLocked settles an authoritative fetch outcome and, when it may,
// installs it: the in-process record, the durable copy (best-effort), and
// the getter snapshot. The gates, in order: the per-scope sequence fence
// (an outcome older than the newest settled one installs nothing); only
// authoritative outcomes settle it; a fresh 200 or a 304 revalidation
// installs UNLESS a FRESHER durable record with a DIFFERENT body appeared
// mid-flight — both validate at server handling time, and responses arrive
// in no particular order, so neither can be ordered against a record another
// process persisted while this fetch was in flight; overwriting it could
// roll the durable configuration (and, for restarts and siblings, the
// getters) back. On that guard the values are still served to this fetch's
// caller, the install is skipped, and the freshest durable record stays —
// this process converges on it at the next load. A cache-served outcome
// installs only by ADOPTION, when the served record is strictly fresher
// than the held one. Returns whether the durable write failed (the caller
// surfaces it).
func (rc *remoteConfigState) installLocked(seq uint64, result RemoteConfigResult, ok bool, newCache *rcCache, authoritative bool, served, revalidated *rcCache) (persistFailed bool) {
	scope := rc.scope
	if seq <= rc.settled[scope] {
		return false
	}
	if authoritative {
		rc.settled[scope] = seq
	}
	if !ok {
		return false
	}
	record := newCache
	if record == nil {
		record = revalidated
	}
	var durable *rcCache
	if record != nil {
		durable = rc.durableRecord(scope)
	}
	if record != nil && durable != nil &&
		durable.Body != record.Body &&
		(served == nil || durable.FetchedAtMS > served.FetchedAtMS) {
		// The durable record advanced past what this fetch dispatched
		// against: a DIFFERENT body, persisted by another same-app process
		// while this response was in flight, may reflect a newer server
		// state than this response — the two cannot be ordered from here.
		// Raising this record's stamp above it and saving would roll the
		// durable configuration back, so the install is skipped ENTIRELY:
		// returning here (never falling through to the adoption branch)
		// keeps the getter snapshot untouched too — falling through would
		// adopt the at-dispatch record as held while installing the SKIPPED
		// response's values into the getters, a mismatched pair that rolls
		// getters back over the newer durable configuration. The fetched
		// values are still served to this fetch's caller; the next load
		// converges on the freshest record.
		return persistFailed
	}
	switch {
	case record != nil:
		record.Scope = scope
		rc.raiseStampAboveSuperseded(record, served, durable)
		// The in-process record is updated even when the durable write
		// fails: the freshest served configuration stays the offline
		// fallback for this process either way.
		rc.held = record
		if rc.cachePath != "" {
			if err := rc.saveDurable(record); err != nil {
				// The stale durable record this fetch captured may still be
				// on disk, and a restart would revive it OVER the
				// configuration just served. Clear it (best-effort) — but
				// only a record no fresher than the one captured at fetch
				// time: another process may have persisted a FRESHER record
				// while this response was in flight, and after a
				// revalidation a durable record carrying the SAME body needs
				// no clearing either (only its stamp is stale).
				durable = rc.durableRecord(scope)
				if durable != nil && served != nil &&
					durable.FetchedAtMS <= served.FetchedAtMS &&
					(newCache != nil || durable.Body != record.Body) {
					rc.tombstoneDurable()
				}
				persistFailed = true
			}
		}
	case served != nil && (rc.held == nil || rc.held.Scope != scope || served.FetchedAtMS > rc.held.FetchedAtMS):
		// Adoption: the served record was discovered at fetch time (written
		// by an earlier launch or another same-app process) and is strictly
		// fresher than anything this instance holds.
		rc.held = served
	default:
		return persistFailed
	}
	rc.snapshot = copyRemoteConfigMap(result.Values)
	rc.version = result.Version
	rc.hasVersion = result.HasVersion
	return persistFailed
}

// armCooldownLocked extends the 429 cooldown from a Retry-After value:
// digits-only seconds, floored at 1s (an absent or malformed header reads as
// the floor — the server contract always sends >= 1), clamped at 24h, and
// monotone (a shorter later value never lowers an armed deadline).
func (rc *remoteConfigState) armCooldownLocked(now time.Time, resp remoteConfigResponse) {
	seconds := resp.retryAfterSeconds
	if !resp.retryAfterPresent || seconds < 1 {
		seconds = 1
	}
	if seconds > rcCooldownClampSeconds {
		seconds = rcCooldownClampSeconds
	}
	deadline := now.Add(time.Duration(seconds) * time.Second)
	if deadline.After(rc.cooldownUntil) {
		rc.cooldownUntil = deadline
	}
}

// rcFetchError is a fetch outcome with no usable values; the taxonomy code
// is the machine-readable part of the message.
func rcFetchError(code string) error {
	return fmt.Errorf("shardpilot remote config fetch failed: %s", code)
}

// rcAttributeSignature is the equality token the ETag-revalidation keying
// stores for an attributed fetch: a non-reversible SHA-256 digest of the
// normalized attribute query — never the query itself, so the durable cache
// record carries zero personal-data-shaped bytes (an equality comparison is
// the signature's ONLY use). The attribute-less signature stays the empty
// string, which every pre-ADR-0310 durable record also unmarshals to.
func rcAttributeSignature(query string) string {
	if query == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(query))
	return hex.EncodeToString(sum[:])
}

// SetRemoteConfigAttributes replaces the client's targeting attribute set
// for the ADR-0310 remote-config attribute pass-through (nil or empty
// clears). Inert while Config.RemoteConfigAttributesEnabled is false: the
// call returns without retaining anything — the dark posture stores zero
// bytes of the personal-data-shaped input, not just "never sends it". With
// the opt-in on, attributes ride a fetch ONLY while consent is
// ConsentGranted — the gate is read at dispatch time, per fetch, so a
// consent downgrade makes the very next fetch attribute-less. Values are
// stored raw and normalized at fetch time against the experiment attribute
// vocabulary (out-of-vocabulary keys are dropped; bounds and ordering per
// normalizeExperimentAttributes). A no-op when the remote-config fetch is
// not configured.
func (c *Client) SetRemoteConfigAttributes(attributes map[string]string) {
	rc := c.rc
	if rc == nil || !c.cfg.RemoteConfigAttributesEnabled {
		return
	}
	var copied map[string]string
	if len(attributes) > 0 {
		copied = make(map[string]string, len(attributes))
		for name, value := range attributes {
			copied[name] = value
		}
	}
	rc.mu.Lock()
	rc.attributes = copied
	rc.mu.Unlock()
}

// FetchRemoteConfig performs one explicit remote-config fetch and blocks for
// its outcome. A nil error means the caller has usable values — fresh
// (FromCache=false), revalidated, or served from the last-known-good cache
// over a transient failure (FromCache=true with Reason set). A non-nil error
// means no usable values: the taxonomy code is in the error text
// ("unauthorized" fails closed without touching the cache or the getter
// snapshot; "http_<status>" is permanent; "transient_..."/"http_0"/
// "malformed_response" are transient with no usable cache;
// "client_id_unavailable" and "remote_config_not_configured" fail before any
// network use). A fetch ended by the CALLER's context — cancellation or the
// caller's own deadline — returns that context error with no cache fallback
// and no side effects; an SDK-internal timeout classifies as the transient
// http_0 only when it fired before a status arrived, while one that merely
// stalled the response BODY preserves the received status and classifies by
// it (a stalled 401 fails closed; only a stalled 200 stays transient). A
// successful result also updates the getter snapshot; a failed one
// leaves it untouched. Fetching is not consent-gated. Concurrent fetches are
// legal; an older response never overwrites a newer settled outcome. Fetches
// are fenced by the client lifecycle exactly like synchronous Track
// publishes: a fetch that begins after Close is rejected with ErrClosed, and
// Close waits (bounded by its own context) for in-flight fetches to settle,
// so no fetch I/O or durable cache write is still running when Close returns.
func (c *Client) FetchRemoteConfig(ctx context.Context) (RemoteConfigResult, error) {
	// The same lifecycle fence as Track: registering in trackWG under
	// lifecycleMu means Close either sees this fetch (and waits for it) or
	// completed its closed store first (and this fetch is rejected) — an
	// in-flight fetch can never keep doing network I/O or write the durable
	// cache after Close has returned.
	c.lifecycleMu.Lock()
	if c.closed.Load() {
		c.lifecycleMu.Unlock()
		return RemoteConfigResult{}, ErrClosed
	}
	c.trackWG.Add(1)
	c.lifecycleMu.Unlock()
	defer c.trackWG.Done()
	rc := c.rc
	if rc == nil {
		return RemoteConfigResult{}, rcFetchError("remote_config_not_configured")
	}
	if rc.clientID == "" {
		// The cache scope and the fetch route both need the client id; with
		// none configured there is nothing coherent to fetch. Decided before
		// any network use.
		return RemoteConfigResult{}, rcFetchError("client_id_unavailable")
	}

	rc.mu.Lock()
	rc.fetchSeq++
	seq := rc.fetchSeq
	cache := rc.loadCacheLocked()
	if c.clock.Now().Before(rc.cooldownUntil) {
		// The cooldown fast path honors the caller's context FIRST, exactly
		// like a dispatched fetch would through the transport: a caller that
		// already canceled (or whose deadline already passed) gets its
		// context error — never a cache "success" it cannot distinguish from
		// a healthy outcome — and nothing installs or adopts.
		if ctx != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				rc.mu.Unlock()
				return RemoteConfigResult{}, ctxErr
			}
		}
		// Inside the 429 cooldown an explicit fetch does not touch the
		// network: the outcome is the cache-served transient_429, by design
		// indistinguishable from a live 429.
		result, failure := serveRemoteConfigCache(cache, "transient_429")
		rc.installLocked(seq, result, failure == "", nil, false, cache, nil)
		rc.mu.Unlock()
		if failure != "" {
			return RemoteConfigResult{}, rcFetchError(failure)
		}
		return result, nil
	}
	etag := ""
	cacheSignature := ""
	if cache != nil {
		etag = cache.ETag
		cacheSignature = cache.AttributeSignature
	}
	fetchURL := rc.fetchURL
	apiKey := rc.apiKey
	var storedAttributes map[string]string
	if c.cfg.RemoteConfigAttributesEnabled && len(rc.attributes) > 0 {
		storedAttributes = make(map[string]string, len(rc.attributes))
		for name, value := range rc.attributes {
			storedAttributes[name] = value
		}
	}
	rc.mu.Unlock()

	// ADR-0310 attribute pass-through: opt-in AND ConsentGranted. The URL is
	// PREPARED here but the consent gate is read at the LAST moment before
	// dispatch (below), so a downgrade landing while the fetch is being
	// prepared still strips the attributes; one landing after dispatch
	// cannot recall a request already on the wire — the very next fetch is
	// attribute-less. Deliberately STRICTER than this SDK's
	// open-under-unknown event posture: unknown consent (and both denied
	// states) keeps the fetch byte-identical to the attribute-less URL, and
	// the fetch itself still happens (config delivery stays
	// consent-neutral).
	attributedURL := ""
	attributedSignature := ""
	if len(storedAttributes) > 0 {
		if pairs, _ := normalizeExperimentAttributes(storedAttributes); len(pairs) > 0 {
			attributedURL = appendRemoteConfigAttributes(fetchURL, pairs)
			attributedSignature = rcAttributeSignature(strings.TrimPrefix(attributedURL, fetchURL))
		}
	}

	callerCtx := ctx
	ctx, cancel := contextWithDefaultTimeout(ctx, c.cfg.HTTPTimeout)
	defer cancel()
	// The last-moment consent read: the decision and the dispatch are
	// adjacent, with no work between them a downgrade could hide behind.
	requestURL := fetchURL
	usedSignature := ""
	if attributedURL != "" && c.Consent() == ConsentGranted {
		requestURL = attributedURL
		usedSignature = attributedSignature
	}
	// The cached ETag revalidates only a SAME-SIGNATURE request: a 304
	// against a differently-attributed URL could otherwise serve the
	// previous target's body as "current" under a shared publication
	// validator. A signature change forces a full fetch instead.
	if cache != nil && cacheSignature != usedSignature {
		etag = ""
	}
	resp, err := c.transport.FetchRemoteConfig(ctx, remoteConfigRequest{
		url:         requestURL,
		bearer:      apiKey,
		ifNoneMatch: etag,
	})
	if err != nil {
		if callerCtx != nil {
			if ctxErr := callerCtx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				// The CALLER's own context ended the fetch (cancellation or
				// its deadline): that is an abort, not an endpoint outcome —
				// no cache fallback masquerading as success, no cooldown or
				// fence side effects, just the caller's error back (same
				// discrimination as callerAbandonedFlush). Any partial
				// response riding the error is discarded with it.
				return RemoteConfigResult{}, err
			}
		}
		if resp.status == 0 {
			// Transport-level failure before any status arrived (no
			// connection, or the SDK-internal timeout fired in the header
			// phase): the transient http_0 class.
			resp = remoteConfigResponse{}
		}
		// A non-zero status rode the error through: the SDK-internal deadline
		// ended the BODY read after an authoritative status had already
		// arrived. The response classifies BY STATUS below with its
		// incomplete-body marker — a stalled 401/403 fails closed, a stalled
		// 3xx stays permanent, a stalled 429 stays transient and arms the
		// cooldown; only a stalled 200 remains the transient malformed class —
		// instead of every stall degrading into a cache-served http_0 (which
		// would keep serving a snapshot for a revoked key).
	}

	now := c.clock.Now()
	result, newCache, authoritative, revalidated, failure := applyRemoteConfig(cache, resp, now.UnixMilli())
	if newCache != nil {
		// A fresh 200 record carries the signature of the fetch that
		// produced it.
		newCache.AttributeSignature = usedSignature
	}
	if revalidated != nil {
		// The 304-renewed record is REBUILT by applyRemoteConfig, so the
		// signature must be re-stamped here too — the ETag only rode a
		// same-signature request, so usedSignature is that record's own.
		// Without this a revalidation would persist the empty signature and
		// force an alternating 304/full-200 pattern on every later
		// same-signature fetch.
		revalidated.AttributeSignature = usedSignature
	}

	rc.mu.Lock()
	if resp.status == 429 && seq > rc.settled[rc.scope] {
		// The cooldown is fenced by the same per-scope sequence as installs:
		// a stale 429 that arrives AFTER a newer fetch already settled an
		// authoritative outcome (a fresh 200, say) is an outdated
		// backpressure instruction — arming it would suppress later fetches
		// for a window the server has already moved past, even though the
		// stale response's result is otherwise ignored.
		rc.armCooldownLocked(now, resp)
	}
	persistFailed := rc.installLocked(seq, result, failure == "", newCache, authoritative, cache, revalidated)
	rc.mu.Unlock()

	if persistFailed {
		c.stats.setLastError("remote_config_cache_persist_failed")
		c.logf("shardpilot remote config: persisting the cache record failed; the fetched configuration stays the in-memory fallback for this process")
	}
	if failure != "" {
		return RemoteConfigResult{}, rcFetchError(failure)
	}
	return result, nil
}

// ── typed value getters ─────────────────────────────────────────────────────
//
// Getters read the in-memory snapshot: the last served fetch (fresh or
// cached), or the durable cache loaded at construction. They never touch the
// network, never return an error, and serve the caller's default until
// configuration is available. Typed getters return the default on a missing
// key AND on a type mismatch, so callers always receive the type they asked
// for — with explicit branches, so a stored `false` is servable.

// copyRemoteConfigMap deep-copies a decoded configuration map so a value
// handed to the caller can be mutated freely without corrupting what later
// getters read. Decoded JSON is acyclic.
func copyRemoteConfigMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = copyRemoteConfigValue(value)
	}
	return out
}

func copyRemoteConfigValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return copyRemoteConfigMap(typed)
	case []any:
		out := make([]any, len(typed))
		for i, child := range typed {
			out[i] = copyRemoteConfigValue(child)
		}
		return out
	default:
		return value
	}
}

// remoteConfigLookup returns the snapshot value for a key. The second result
// distinguishes a stored JSON null (nil, true) from an absent key.
func (c *Client) remoteConfigLookup(key string) (any, bool) {
	rc := c.rc
	if rc == nil || key == "" {
		return nil, false
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.snapshot == nil {
		return nil, false
	}
	value, ok := rc.snapshot[key]
	return value, ok
}

// RemoteConfigValue returns the raw configuration value for key (any JSON
// type) and whether the key exists. Maps and arrays are returned as deep
// copies the caller may mutate freely.
func (c *Client) RemoteConfigValue(key string) (any, bool) {
	value, ok := c.remoteConfigLookup(key)
	if !ok {
		return nil, false
	}
	return copyRemoteConfigValue(value), true
}

// RemoteConfigString returns the string value for key, or def on a missing
// key or a non-string value.
func (c *Client) RemoteConfigString(key, def string) string {
	value, ok := c.remoteConfigLookup(key)
	if !ok {
		return def
	}
	if typed, isString := value.(string); isString {
		return typed
	}
	return def
}

// RemoteConfigNumber returns the numeric value for key (JSON numbers decode
// as float64), or def on a missing key or a non-numeric value.
func (c *Client) RemoteConfigNumber(key string, def float64) float64 {
	value, ok := c.remoteConfigLookup(key)
	if !ok {
		return def
	}
	if typed, isNumber := value.(float64); isNumber {
		return typed
	}
	return def
}

// RemoteConfigBool returns the boolean value for key, or def on a missing
// key or a non-boolean value. A stored false is served as false.
func (c *Client) RemoteConfigBool(key string, def bool) bool {
	value, ok := c.remoteConfigLookup(key)
	if !ok {
		return def
	}
	if typed, isBool := value.(bool); isBool {
		return typed
	}
	return def
}

// RemoteConfigValues returns a defensive copy of the whole configuration
// map, or nil when no configuration has been served yet (no fetch and no
// usable cache).
func (c *Client) RemoteConfigValues() map[string]any {
	rc := c.rc
	if rc == nil {
		return nil
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return copyRemoteConfigMap(rc.snapshot)
}

// RemoteConfigVersion returns the published configuration version from the
// last served payload, and false when unknown (no configuration yet, or an
// unwrapped payload — the version is wrapper metadata, so an unwrapped
// payload never carries one).
func (c *Client) RemoteConfigVersion() (float64, bool) {
	rc := c.rc
	if rc == nil {
		return 0, false
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.version, rc.hasVersion
}
