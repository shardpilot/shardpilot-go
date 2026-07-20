package shardpilot

import (
	"bytes"
	"context"
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

	// maxAgeSeconds/maxAgePresent hold the last seen Cache-Control max-age
	// — the server's advertised revalidation cadence. Consumed ONLY by the
	// opt-in periodic revalidation timer's schedule anchor; fetch
	// classification never reads it (the explicit-fetch contract still
	// interprets no Cache-Control).
	maxAgeSeconds int
	maxAgePresent bool

	// autoHalted marks the opt-in periodic revalidation TIMER halted after
	// an authoritative 401/403 one of ITS fetches received: an unattended
	// loop must not keep re-asking an endpoint that authoritatively refused
	// it. The timer goroutine exits on it; only re-initialization (a new
	// client) resumes automatic revalidation. Explicit fetches ignore it
	// entirely — per-fetch classification is unchanged.
	autoHalted bool

	// revalidateDelayFn overrides the revalidation timer's cycle delay;
	// test seam (like Client.jitter), nil in production. Set only before
	// the timer goroutine starts.
	revalidateDelayFn func() time.Duration
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
	result, code, err := c.fetchRemoteConfigOutcome(ctx)
	if err != nil {
		return RemoteConfigResult{}, err
	}
	if code != "" {
		return RemoteConfigResult{}, rcFetchError(code)
	}
	return result, nil
}

// fetchRemoteConfigOutcome is the shared fetch core behind FetchRemoteConfig
// (which formats the failure code into an error) and the opt-in periodic
// revalidation timer (which reads the code directly — its halt trigger is
// the "unauthorized" class). A non-nil error is a lifecycle or
// caller-context outcome (ErrClosed, the caller's own context error),
// mutually exclusive with a failure code; every path's behavior is the
// public method's documented contract, unchanged.
func (c *Client) fetchRemoteConfigOutcome(ctx context.Context) (RemoteConfigResult, string, error) {
	// The same lifecycle fence as Track: registering in trackWG under
	// lifecycleMu means Close either sees this fetch (and waits for it) or
	// completed its closed store first (and this fetch is rejected) — an
	// in-flight fetch can never keep doing network I/O or write the durable
	// cache after Close has returned.
	c.lifecycleMu.Lock()
	if c.closed.Load() {
		c.lifecycleMu.Unlock()
		return RemoteConfigResult{}, "", ErrClosed
	}
	c.trackWG.Add(1)
	c.lifecycleMu.Unlock()
	defer c.trackWG.Done()
	rc := c.rc
	if rc == nil {
		return RemoteConfigResult{}, "remote_config_not_configured", nil
	}
	if rc.clientID == "" {
		// The cache scope and the fetch route both need the client id; with
		// none configured there is nothing coherent to fetch. Decided before
		// any network use.
		return RemoteConfigResult{}, "client_id_unavailable", nil
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
				return RemoteConfigResult{}, "", ctxErr
			}
		}
		// Inside the 429 cooldown an explicit fetch does not touch the
		// network: the outcome is the cache-served transient_429, by design
		// indistinguishable from a live 429.
		result, failure := serveRemoteConfigCache(cache, "transient_429")
		rc.installLocked(seq, result, failure == "", nil, false, cache, nil)
		rc.mu.Unlock()
		if failure != "" {
			return RemoteConfigResult{}, failure, nil
		}
		return result, "", nil
	}
	etag := ""
	if cache != nil {
		etag = cache.ETag
	}
	fetchURL := rc.fetchURL
	apiKey := rc.apiKey
	rc.mu.Unlock()

	callerCtx := ctx
	ctx, cancel := contextWithDefaultTimeout(ctx, c.cfg.HTTPTimeout)
	defer cancel()
	resp, err := c.transport.FetchRemoteConfig(ctx, remoteConfigRequest{
		url:         fetchURL,
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
				return RemoteConfigResult{}, "", err
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

	rc.mu.Lock()
	if resp.maxAgePresent {
		// The server's advertised revalidation cadence, remembered for the
		// opt-in periodic revalidation timer's schedule anchor. Last write
		// wins — a cadence hint, never a classification input.
		rc.maxAgeSeconds = resp.maxAgeSeconds
		rc.maxAgePresent = true
	}
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
		return RemoteConfigResult{}, failure, nil
	}
	return result, "", nil
}

// ── periodic revalidation (opt-in) ─────────────────────────────────────────

// rcRevalidateFloor is the minimum revalidation cycle spacing, respecting
// the platform's per-(workspace, environment, client) fetch rate limiter: a
// timer never polls more often than once a minute, however small the server
// cadence or the configured interval.
const rcRevalidateFloor = 60 * time.Second

// rcRevalidateFallback is the schedule anchor before any Cache-Control
// max-age has been seen (the server's default max-age).
const rcRevalidateFallback = 300 * time.Second

// revalidateDelay is the revalidation timer's next cycle delay:
// max(configured interval, server anchor), where the anchor is the last
// seen Cache-Control max-age — 300s before one is seen — floored at 60s. A
// configured interval can slow the timer down but never drive it faster
// than the server's advertised cadence.
func (rc *remoteConfigState) revalidateDelay(configured time.Duration) time.Duration {
	if rc.revalidateDelayFn != nil {
		return rc.revalidateDelayFn()
	}
	rc.mu.Lock()
	seconds, present := rc.maxAgeSeconds, rc.maxAgePresent
	rc.mu.Unlock()
	anchor := rcRevalidateFallback
	if present {
		anchor = time.Duration(seconds) * time.Second
	}
	if anchor < rcRevalidateFloor {
		anchor = rcRevalidateFloor
	}
	if configured > anchor {
		return configured
	}
	return anchor
}

func (rc *remoteConfigState) haltAutoLane() {
	rc.mu.Lock()
	rc.autoHalted = true
	rc.mu.Unlock()
}

func (rc *remoteConfigState) autoLaneHalted() bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.autoHalted
}

// runRemoteConfigRevalidation is the opt-in periodic revalidation timer
// (started only when Config.RemoteConfigRevalidateInterval > 0): each cycle
// re-runs the same conditional GET an explicit FetchRemoteConfig performs —
// If-None-Match with the cached ETag, the full failure taxonomy, the 429
// cooldown (a tick inside an armed cooldown performs no network call and
// does not reschedule tighter) — so a running client converges on a
// server-side change within one interval. The timer exits when the client
// stops, or when it halts: after one of ITS OWN fetches receives an
// authoritative 401/403 it stops scheduling until re-initialization — an
// unattended loop must not keep re-asking an endpoint that authoritatively
// refused it. Explicit fetches keep classifying per-fetch throughout and
// never resume the halted timer.
func (c *Client) runRemoteConfigRevalidation() {
	defer close(c.rcRevalidateDone)
	for {
		timer := time.NewTimer(c.rc.revalidateDelay(c.cfg.RemoteConfigRevalidateInterval))
		select {
		case <-c.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
		if c.revalidateRemoteConfigOnce() {
			return
		}
	}
}

// revalidateRemoteConfigOnce runs one timer cycle and reports whether the
// timer must exit (halted, or the client closed). Transient and permanent
// non-auth outcomes keep the schedule: the durable cache serves, and the
// next cycle re-asks.
func (c *Client) revalidateRemoteConfigOnce() (exit bool) {
	if c.rc.autoLaneHalted() {
		return true
	}
	_, code, err := c.fetchRemoteConfigOutcome(context.Background())
	if err != nil {
		// Only lifecycle outcomes surface as errors on a background
		// context; the timer winds down with the client.
		return errors.Is(err, ErrClosed)
	}
	if code == "unauthorized" {
		c.rc.haltAutoLane()
		c.logf("shardpilot remote config revalidation: halted after an authoritative 401/403; explicit fetches remain available, and the timer resumes only on re-initialization")
		return true
	}
	return false
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
