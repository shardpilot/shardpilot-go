package shardpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Bounded disk spool: opt-in (Config.SpoolDir) at-least-once delivery across
// restarts for worker batches that failed retriably, fulfilling queue.go's
// long-standing follow-up. The spool stores each event's exact
// wire-serialized envelope bytes — stamped and marshaled once at intake/build
// (see buildBatch) — so a resend is byte-identical and the ingest service
// de-duplicates it by event_id. It stores envelope BODIES only, never
// headers: the schema-revision header stays a per-request transport concern
// picked up at request time.
//
// What spools: a worker batch that failed for a retriable reason (429, any
// 5xx, network/transport error), de-duplicated by event_id, and the
// undelivered remnant at Close (queue + retained batch). Track's synchronous
// path never spools — the caller owns that error. Terminal outcomes are
// never spooled, and a terminal outcome on previously spooled events removes
// them (ack-removal, so a poison batch cannot re-fail every launch); a 2xx
// settles all of a batch's events the same way (per-event verdicts inside
// the 202 are terminal server outcomes).
//
// At startup — only under a persisted consent grant (consent_record.go) —
// the record is loaded, re-capped, age-filtered, chunked to BatchSize, and
// resent BEFORE the fresh queue drains, through the same pacing gates as
// fresh publishes. A persisted retry_after_until_ms re-seeds the Retry-After
// deferral with the remaining window (24h clamp).
//
// Caps: SpoolMaxEvents / SpoolMaxBytes, evicting OLDEST first — an outage
// outlasting the spool drops oldest events. Retry-age cap: an event whose
// age is not provable (undatable), older than spoolRetryAgeCap, or
// future-dated beyond spoolMaxForwardClockSkew is expired — retention must
// be provable, so unprovable fails closed. Every drop, whatever the class,
// is reported through Config.OnSpoolDeadLetter.
//
// Consent: disk participation is strictly grant-only. Writes require the
// live state granted AND the grant record persisted by this process's
// SetConsent(true) (a failed record write keeps the spool closed — stricter
// than the Defold reference, deliberately). Loads require the persisted
// grant; any other persisted state purges the record at init. Any live
// transition to non-grant purges. A failed purge owes a wipe
// (spool-wipe-owed marker, or an in-memory flag when even the marker cannot
// be created) and fails closed — no append, no load, no resend — until the
// wipe succeeds; it is retried at start, every flush cycle, before any
// append, and first in SetConsent(true).

// SpoolDropReason classifies why the disk spool dropped events, reported
// through Config.OnSpoolDeadLetter.
type SpoolDropReason string

const (
	// SpoolDropCapacity: the count or byte cap evicted the oldest events.
	SpoolDropCapacity SpoolDropReason = "capacity"
	// SpoolDropExpired: the retry-age cap expired the events (older than 7
	// days, future-dated beyond the skew tolerance, or undatable).
	SpoolDropExpired SpoolDropReason = "expired"
	// SpoolDropTerminal: a permanent ingest outcome settled previously
	// spooled events — a batch-level permanent failure, or a per-event
	// `rejected` verdict inside a resent chunk's 2xx.
	SpoolDropTerminal SpoolDropReason = "terminal"
	// SpoolDropConsent: a consent purge dropped the record, a
	// would-have-spooled batch was refused disk under a non-grant or
	// owed-wipe state, or a resent event came back consent-suppressed in the
	// response's per-event verdicts.
	SpoolDropConsent SpoolDropReason = "consent"
)

// SpoolDeadLetter is one dead-letter notification: the drop reason and the
// exact spooled wire bytes of each dropped envelope.
type SpoolDeadLetter struct {
	Reason    SpoolDropReason
	Envelopes []json.RawMessage
}

const (
	spoolFileName      = "spool.json"
	spoolRecordVersion = 2

	// spoolRetryAgeCap bounds how long an undelivered event may wait in the
	// spool, mirroring the crash-plane retention contract; age derives from
	// the envelope's event_ts (stamped once at intake).
	spoolRetryAgeCap = 7 * 24 * time.Hour

	// spoolMaxForwardClockSkew bounds how far in the FUTURE an event_ts may
	// sit before the record is treated as expired: an untrusted timestamp
	// must not stretch the retention bound (worst case is the cap plus this
	// hour), while ordinary clock corrections are absorbed.
	spoolMaxForwardClockSkew = time.Hour

	// spoolMaxDeferralSeed clamps the deferral re-seeded from a persisted
	// retry_after_until_ms, matching the ingest plane's Retry-After clamp so
	// wall-clock skew in the stored deadline cannot park the client longer.
	spoolMaxDeferralSeed = 24 * time.Hour

	// spoolWithdrawnFileName is the durable withdrawal marker: event ids the
	// real-subjects sentinel purge removed from the spool whose record
	// rewrite has not landed yet. Terminal withdrawal is not an ordinary
	// at-least-once ack — a crash between the purge and a successful
	// rewrite must NOT resurrect withdrawn facts into the next process's
	// resend queue — so the ids persist first and the marker spends only
	// once a rewrite without those entries lands. Honored at load.
	spoolWithdrawnFileName = "spool.withdrawn.json"

	// spoolWithdrawnReadLimit is the FLOOR of the marker read bound: small
	// configurations keep this generous legacy bound, while larger
	// configured spools derive a capacity-proportional bound instead (see
	// withdrawnMarkerReadLimit) — a fixed cap would read a legitimately
	// large marker as absent after a crash and reload withdrawn facts for
	// resend.
	spoolWithdrawnReadLimit = 256 << 10

	// spoolWithdrawnPerIDBytes is the per-id allowance behind the derived
	// marker read bound: a UUID-shaped event id's JSON envelope (36 chars,
	// quotes, separator) is ~40 bytes; 128 leaves headroom for longer
	// deterministic ids and a second unspent generation's merge.
	spoolWithdrawnPerIDBytes = 128

	// spoolWithdrawnReadCeiling clamps the derived marker bound (overflow
	// safety for absurd SpoolMaxEvents values): past this the marker is not
	// a file this spool could legitimately have written.
	spoolWithdrawnReadCeiling = 1 << 30

	// spoolRecordReadOverhead is the fixed allowance over SpoolMaxBytes for
	// the record's own JSON framing when reading spool.json back. Every save
	// re-applies the byte cap to the EVENT bytes before writing, so a
	// legitimate record can exceed the cap only by its framing (field names,
	// separators, the deadline) — a few KB at the 2000-event cap; 64 KiB is
	// generous by orders of magnitude, and a file larger than the cap plus
	// this is not a record this spool could have written.
	spoolRecordReadOverhead = 64 << 10
)

// errSpoolRecordOversized marks a spool.json exceeding the bounded read
// limit: it is handled by the corrupt-record path (discarded, clean start)
// without ever being read whole — the bounded-spool guarantee must hold at
// load time too, not just for what this process writes.
var errSpoolRecordOversized = errors.New("spool record exceeds the bounded read limit")

// spoolRecordWire is the spool.json payload. Events hold the exact
// wire-serialized envelope bytes; retry_after_until_ms carries a live server
// Retry-After deadline captured when a batch was spooled under one.
// Version 2 wraps each event in spoolEventWire — the exact envelope bytes
// plus SDK-side metadata that must survive a restart but can never ride the
// wire envelope itself (the ingest contract is a strict allowlist): the
// SDK-authorship marker the sentinel's spool predicates require. Version 1
// records (bare raw arrays) fail the version gate at load and start clean —
// the pre-release breaking-format posture.
type spoolRecordWire struct {
	Version           int              `json:"version"`
	Events            []spoolEventWire `json:"events"`
	RetryAfterUntilMS int64            `json:"retry_after_until_ms,omitempty"`
}

// spoolEventWire is one persisted spool entry: the envelope's exact wire
// bytes (joined verbatim on resend — byte identity is the de-dupe contract)
// and the internal experiment-fact flag, so a reloaded record can tell this
// SDK's own facts from host events that merely resemble them.
type spoolEventWire struct {
	Raw          json.RawMessage `json:"raw"`
	InternalFact bool            `json:"internal_fact,omitempty"`
	// Drop names an id this run evicted. A line carrying it is a RECORDED
	// DECISION rather than an event: it has no envelope, and the reader
	// applies it instead of re-deriving that the eviction must have
	// happened. See spoolDocument.live.
	Drop string `json:"drop,omitempty"`
}

// ── v3: one record per line ─────────────────────────────────────────────────
//
// The v2 document is a single JSON object holding every envelope, so an append
// re-reads it, re-marshals it, and rewrites it — O(spool size) per event, and
// on this SDK O(2x) because the merge reads first. docs/SPOOL_APPEND_ONLY_DESIGN.md
// (shipped with the Unity SDK, the reference implementation) works through why
// splicing into the array format is unsafe and settles on a line format:
//
//	{"version":3,"max_events":2000,"max_bytes":1048576,"retry_after_until_ms":0}
//	{"raw":{...},"internal_fact":true}
//	{"raw":{...}}
//
// ⚠ THE NEWLINE IS THE RECORD BOUNDARY, AND THAT IS THE WHOLE POINT. A torn
// append leaves a final line with no terminator; the load drops exactly that
// line and keeps every complete one. The boundary is unambiguous because the
// JSON serializer escapes newlines inside strings, so a raw newline byte in the
// file can only be a terminator — where in the array format `},{` can occur inside a
// string a game chose to send, which is why a recovery scanning for it would
// silently truncate good records.
//
// ⚠ THE CAPS ARE IN THE HEADER, and that is load-bearing rather than
// decorative. Eviction is LOGICAL — an evicted entry leaves the mirror at once
// and its bytes stay in the file as a dead record — so nothing in a record says
// whether it is live, and the load rebuilds the live set by trimming
// oldest-first to the caps. That reproduces the writing run's cut exactly only
// while the caps are the same. Raise SpoolMaxEvents in a new build and records
// the previous run had already dropped would fit again, load, and resend as
// events this SDK had already reported dropped. So the file states the caps it
// was written under, the load trims under THOSE first, and only then under the
// configured ones: a config change can shrink what a file carries forward, and
// can never resurrect what was already gone.
const spoolRecordVersionLines = 3

// spoolHeaderWire is the v3 first line.
type spoolHeaderWire struct {
	Version           int   `json:"version"`
	MaxEvents         int   `json:"max_events,omitempty"`
	MaxBytes          int   `json:"max_bytes,omitempty"`
	RetryAfterUntilMS int64 `json:"retry_after_until_ms,omitempty"`
}

// spoolCompactionSlack is how far the file may exceed the live content it
// holds before a rewrite reclaims the dead records: 1.5x.
//
// ⚠ EVICTION MUST NOT REWRITE PER APPEND, which is what this constant buys.
// Eviction happens when the spool is FULL — precisely the window the bound
// measures p95 in — so a rewrite there would put an O(spool) cost back exactly
// where it was removed from. At the 2000-event default a rewrite lands roughly
// once per (live record count) appends, ~0.1% of calls, a factor of ten below
// where p99 reads.
const spoolCompactionSlack = 3

// spoolHeaderAllowance is the header line's contribution to the live-content
// yardstick, so a nearly-empty spool does not read as mostly-dead and compact
// on every append.
const spoolHeaderAllowance = 4 << 10

// spoolEnvelopeWire is the minimal per-envelope parse a loaded record needs:
// the de-duplication/ack key and the age source.
type spoolEnvelopeWire struct {
	EventID string `json:"event_id"`
	EventTS string `json:"event_ts"`
}

// spoolEntry is one spooled envelope: its id (ack/de-dup key), its event_ts
// (age source), and its exact wire bytes.
type spoolEntry struct {
	id  string
	ts  string
	raw json.RawMessage

	// expFactEpoch is the real-subjects purge generation the entry's batch
	// was BUILT under (stamped at partitionSpoolEligible from the request;
	// zero for entries loaded from disk — a previous process's facts
	// predate any purge this process runs). The sentinel's spool sweep
	// spares entries stamped AT its own post-bump epoch: a drop-time
	// capture appended under e.mu alone (no emitMu) can land mid-sweep,
	// and an epoch-blind raw predicate would withdraw that current-epoch
	// fact.
	expFactEpoch uint64

	// internalFact marks an entry whose envelope this SDK authored as one
	// of its own experiment facts (envelope.internalIdentityFact, persisted
	// through the record as spoolEventWire.InternalFact): the sentinel's
	// spool predicates require it, so a host-authored envelope that merely
	// resembles a fact — the reserved-looking name + sfk1_-shaped
	// assignment_key — is never withdrawn, class-condemned, or withheld.
	internalFact bool
}

// spoolEntryExpired applies the retry-age cap: expired when the event_ts is
// unparseable or missing (retention must be provable — undatable fails
// closed), older than the cap, or future-dated beyond the skew tolerance.
// An entry without an event_id is also unusable (it could never be acked or
// de-duplicated) and fails the same way.
func spoolEntryExpired(entry spoolEntry, now time.Time) bool {
	if entry.id == "" {
		return true
	}
	ts, err := time.Parse(time.RFC3339Nano, entry.ts)
	if err != nil {
		return true
	}
	return ts.Before(now.Add(-spoolRetryAgeCap)) || ts.After(now.Add(spoolMaxForwardClockSkew))
}

// diskSpool is the spool state machine: an in-memory mirror of spool.json
// (authoritative — a failed rewrite keeps the mirror and retries), the
// startup resend queue, and the owed-wipe fail-closed flag. All fields are
// guarded by mu. It performs no callbacks and touches no client state:
// methods return the dropped/added entries for the Client layer to count and
// dead-letter outside the lock.
type diskSpool struct {
	mu sync.Mutex

	dir       string
	maxEvents int
	maxBytes  int

	// experimentsEnabled mirrors Config.ExperimentsEnabled for the
	// load-time withdrawal-marker adjudication. The marker itself is
	// honored flag-independently — a durable purge debt outlives the
	// opt-in that created it — but a DAMAGED marker's remedy is
	// flag-scoped: an experiments-enabled client escalates to the
	// whole-record wipe rung, while a dark client fails closed WITHIN the
	// experiment-fact class only, keeping the ordinary spool alive (see
	// load).
	experimentsEnabled bool

	// withdrawnMarkerOwed reports an unspent withdrawal DEBT: a marker may
	// be live on disk (the crash protection), or its write may have failed
	// — the debt arms either way and spends (removing any marker file,
	// durably) after the next successful record rewrite, which by then
	// excludes the withdrawn entries.
	withdrawnMarkerOwed bool

	// withdrawnOwedIDs holds the UNSPENT withdrawal debt's full id set —
	// populated when a withdrawal happens (marker write SUCCESSFUL OR NOT)
	// or when a marker is honored at load, retained across merged
	// generations, cleared only when the debt spends. The reload-and-merge
	// save excludes these ids DIRECTLY: a withdrawal larger than
	// spoolSettledMemory would evict its oldest ids from the bounded
	// settled cache before the save runs, and the merge would read them
	// back from the stale on-disk record — writing a "clean" file that
	// resurrects facts the mirror already dead-lettered.
	withdrawnOwedIDs map[string]struct{}

	entries    []spoolEntry
	ids        map[string]struct{}
	totalBytes int

	// resend holds the startup-loaded entries awaiting re-publish, consumed
	// in chunks before fresh queue drains. In-process appends do NOT join it:
	// their events are still retained in the worker's in-memory batch, which
	// owns the in-process retry (the spool is crash insurance for them).
	resend []spoolEntry

	retryAfterUntilMS int64

	// dirty marks a failed record rewrite: the mirror is authoritative and
	// the write is retried on the flush cadence.
	dirty bool

	// fileAppendable: the bytes on disk are a v3 document matching the
	// committed state, so appendRecordsLocked may extend it. Set only by a
	// landed rewrite; cleared by every failed write.
	fileAppendable bool
	// fileBytes: what this instance has written to the file, LIVE AND DEAD
	// together. Logical eviction means the file holds records the committed
	// state has dropped, so this is >= a rewrite's size and is what the
	// compaction trigger reads.
	fileBytes int64
	// tornTailOwed: a load recovered an unterminated final line, or an append
	// wrote partially. Either way the torn bytes are still there and the file
	// must be rewritten before anything extends it.
	tornTailOwed bool

	// owed marks a failed purge: the spool is fail-closed (no append, no
	// load, no resend) until the wipe succeeds. Mirrored by the durable
	// spool-wipe-owed marker when that marker could be created.
	owed bool

	// grantPersisted reports that THIS process persisted the grant record in
	// SetConsent(true). Spool writes require it in addition to the live
	// granted state: a grant whose record failed to persist keeps disk
	// closed, so "disk participation requires a persisted grant" holds for
	// writes exactly as it does for loads.
	grantPersisted bool

	// actorDigest is the canonical digest of the actor/scope tuple this
	// client's consent decisions cover; the persisted consent record carries
	// and is verified against it (see consentActorDigest).
	actorDigest string

	// settledIDs remembers (bounded, FIFO) the event ids THIS process removed
	// from the spool — acked, expired, evicted, or purged — so the
	// reload-and-merge save can never resurrect them from a stale on-disk
	// copy written by another client sharing the directory.
	settledIDs  map[string]struct{}
	settledFIFO []string

	// pendingCapacityDrops holds locally-owned entries a merging save
	// cap-evicted (foreign records pushed the merged view over the caps).
	// The client layer drains them (takeCapacityDrops) after each spool
	// operation to dead-letter and count outside the lock.
	pendingCapacityDrops []spoolEntry

	// deferredCapacityDrops holds cap-evicted entries whose REMOVING rewrite
	// failed: the old record still carries them on disk, so the eviction is
	// not final yet — a crash before a successful rewrite reloads and
	// resends them. Their capacity dead-letters are deferred until the
	// eviction durably lands (a later successful save, whose merge excludes
	// them via settledIDs, or the record file's removal), at which point
	// they move to pendingCapacityDrops; the callback thereby reports only
	// FINAL undelivered outcomes, never an eviction the disk later undoes.
	deferredCapacityDrops []spoolEntry

	// uncountedIDs tracks entries accepted into the mirror under a FAILED
	// record write: they were deliberately not counted as Spooled (nothing
	// was durable). When a later save lands, the ones still mirrored have
	// just become durable — becameDurable accumulates them for the client
	// layer to fold into Stats.Spooled exactly once (takeBecameDurable).
	uncountedIDs  map[string]struct{}
	becameDurable int

	// countForeign, when set, is called (under mu; must not call back into
	// the spool or run user code) with the number of on-disk records a
	// merging save found that this process neither holds nor settled — i.e.
	// another writer's mutations detected on the shared directory.
	countForeign func(n int)

	// removeFn/renameFn/chmodFn are the file primitives, injectable so tests
	// can exercise purge, persist, and refused-dir-tighten failures
	// deterministically (the same seam discipline as createAnonymousIDWith).
	// markerFn creates the wipe-owed marker (createWipeOwedMarker),
	// injectable so tests can exercise a failed debt-marker creation.
	// syncFn fsyncs the spool directory (syncDir), injectable so tests can
	// exercise a destruction whose unlink cannot be made durable.
	//
	// Production sets them once at construction and never touches them again,
	// but tests swap one MID-FLIGHT to model a disk healing, so every read
	// outside the spool lock is a data race with that swap. It is a race the
	// race detector only sees when the timing lines up — a change elsewhere in
	// the worker's wake behaviour surfaced it, having been latent — so the
	// reads on the unlocked paths go through fileFuncs() below rather than
	// touching the fields.
	removeFn func(path string) error
	renameFn func(oldpath, newpath string) error
	// appendFn is the append path's write seam, the counterpart of renameFn
	// for the rewrite path.
	//
	// ⚠ TWO WRITE PATHS NEED TWO SEAMS, and leaving this one out was a real
	// gap rather than a test inconvenience: fault-injection tests break
	// renameFn to make "the disk write fails" true, and an append that went
	// straight to the file would have kept succeeding under exactly the
	// condition those tests establish — so the spool would have reported
	// durability it did not have, in the tests written to catch that.
	appendFn func(path string, payload []byte) error
	chmodFn  func(name string, mode os.FileMode) error
	markerFn func(dir string) error
	syncFn   func(dir string) error
}

// fileFuncs reads the injectable rename/chmod pair under the spool lock. See
// the field declarations: a test may swap them while the worker is running.
func (s *diskSpool) fileFuncs() (func(string, string) error, func(string, os.FileMode) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.renameFn, s.chmodFn
}

// spoolSettledMemory bounds the settled-id memory used to suppress
// merge-resurrection. Past it the oldest suppression is forgotten: a very
// old id could then be resurrected by a stale sibling write, which degrades
// to at-least-once delivery (the server de-dupes by event_id).
const spoolSettledMemory = 4096

func newDiskSpool(cfg Config) *diskSpool {
	s := &diskSpool{
		dir:                cfg.SpoolDir,
		experimentsEnabled: cfg.ExperimentsEnabled,
		maxEvents:          cfg.SpoolMaxEvents,
		maxBytes:           cfg.SpoolMaxBytes,
		ids:                make(map[string]struct{}),
		settledIDs:         make(map[string]struct{}),
		uncountedIDs:       make(map[string]struct{}),
		actorDigest:        consentActorDigest(cfg),
		removeFn:           os.Remove,
		renameFn:           os.Rename,
		appendFn:           appendPrivateFile,
		chmodFn:            os.Chmod,
		markerFn:           createWipeOwedMarker,
		syncFn:             syncDir,
	}
	s.owed = wipeOwedMarkerExists(s.dir)
	return s
}

func (s *diskSpool) filePath() string {
	return filepath.Join(s.dir, spoolFileName)
}

// recordSettledLocked remembers an id this process removed from the spool so
// a merging save does not resurrect it (bounded FIFO).
func (s *diskSpool) recordSettledLocked(id string) {
	if id == "" {
		return
	}
	if _, exists := s.settledIDs[id]; exists {
		return
	}
	s.settledIDs[id] = struct{}{}
	s.settledFIFO = append(s.settledFIFO, id)
	if len(s.settledFIFO) > spoolSettledMemory {
		delete(s.settledIDs, s.settledFIFO[0])
		s.settledFIFO = s.settledFIFO[1:]
	}
}

// saveLocked rewrites spool.json with RELOAD-AND-MERGE semantics (atomic
// temp+rename, 0600): the current on-disk record is re-read and unioned with
// the in-memory mirror by event_id — minus the ids this process settled —
// disk records first (they are the older, already-persisted view), then the
// caps re-applied oldest-drop on the merged list, deterministically. One
// client per SpoolDir is the supported topology; the merge is the safety net
// that keeps a concurrent writer's still-undelivered records from being
// silently dropped by a mirror-only rewrite. Cross-process races thereby
// shrink to last-writer-wins over a merged view: a record a sibling acked
// concurrently can be resurrected and resent — at-least-once, and the server
// de-duplicates by event_id. Foreign records ride the file only: they are
// never adopted into this process's mirror or resend queue (a restart loads
// them normally). A cap eviction at merge time settles LOCAL state too: an
// evicted entry this process still mirrored is removed from the mirror,
// settled, and queued in pendingCapacityDrops for the client layer to
// dead-letter and count (the file and the mirror must never disagree about
// an event's fate); an evicted foreign entry is only settled, so it does not
// resurrect through a later save. On failure the mirror stays authoritative
// and dirty marks the flush-cadence retry.
// renderDocumentLocked renders the whole v3 document: header line, then one
// record per line, each newline-terminated.
func (s *diskSpool) renderDocumentLocked(entries []spoolEntry) ([]byte, error) {
	header, err := json.Marshal(spoolHeaderWire{
		Version:           spoolRecordVersionLines,
		MaxEvents:         s.maxEvents,
		MaxBytes:          s.maxBytes,
		RetryAfterUntilMS: s.retryAfterUntilMS,
	})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Grow(len(header) + 1 + s.totalBytes + 32*len(entries))
	buf.Write(header)
	buf.WriteByte('\n')
	for _, entry := range entries {
		line, err := json.Marshal(spoolEventWire{Raw: entry.raw, InternalFact: entry.internalFact})
		if err != nil {
			return nil, err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

// liveContentBytesLocked is what a rewrite of the committed state would cost on
// disk: the yardstick the compaction trigger measures the file against.
func (s *diskSpool) liveContentBytesLocked() int64 {
	// 32 bytes per record covers the {"raw":…} wrapper, the optional
	// internal-fact flag and the terminator, with room to spare.
	return int64(s.totalBytes) + 32*int64(len(s.entries)) + spoolHeaderAllowance
}

// appendRecordsLocked writes new records to the end of the file and nothing
// else. O(record).
//
// It reports whether it could: a file that is not a known-good v3 document, or
// one owing a reconcile after a recovered tear, must take the rewrite path
// instead. The caller does not need to know which — "not appendable" is one
// answer with one remedy.
func (s *diskSpool) appendRecordsLocked(entries []spoolEntry, drops []string) (ok bool, err error) {
	if !s.fileAppendable || s.tornTailOwed || (len(entries) == 0 && len(drops) == 0) {
		return false, nil
	}
	// ⚠ A FOREIGN WRITER SENDS THIS BACK TO THE REWRITE, and the rewrite is
	// where the cross-process merge lives. One client per SpoolDir is the
	// supported topology and the merge is its safety net — appending past a
	// sibling's records would keep them (append-only does not clobber) but
	// would never apply the caps to the combined view, nor settle the local
	// state of an entry that view evicts. fileBytes is what THIS instance
	// believes it wrote, so a size that disagrees is somebody else's write.
	info, err := os.Stat(s.filePath())
	if err != nil || info.Size() != s.fileBytes {
		s.fileAppendable = false
		return false, nil
	}
	// ⚠ AND A LOOSENED MODE SENDS IT BACK TOO. O_APPEND on an existing file
	// IGNORES the mode argument, and this path never touches the directory —
	// so where the atomic rewrite re-ran the private-directory check and
	// replaced the destination with a 0600 file on EVERY write, an append
	// would happily add event payloads to a world-readable one. The rewrite
	// re-establishes both, which is why the remedy is to fall back to it
	// rather than to chmod here.
	if info.Mode().Perm() != 0o600 {
		s.fileAppendable = false
		return false, nil
	}
	if err := ensurePrivateDir(s.dir, s.chmodFn); err != nil {
		s.fileAppendable = false
		return false, nil
	}
	var buf bytes.Buffer
	for _, entry := range entries {
		line, marshalErr := json.Marshal(spoolEventWire{Raw: entry.raw, InternalFact: entry.internalFact})
		if marshalErr != nil {
			return false, nil
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	// The drops follow the entries they may refer to: a batch member evicted
	// on arrival is written and then dropped, so the log states what happened
	// rather than requiring the reader to work out that it must have.
	for _, id := range drops {
		line, marshalErr := json.Marshal(spoolEventWire{Drop: id})
		if marshalErr != nil {
			return false, nil
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	payload := buf.Bytes()
	// ⚠ THE READ BOUND IS CHECKED BEFORE THE WRITE, NOT AFTER IT. Compacting
	// afterwards cannot help a batch big enough to cross the bound in one
	// append: the records are fsynced first, so a crash between the sync and
	// the rewrite leaves a file the next startup reads as oversized, removes,
	// and loses the whole backlog for. A batch that would cross it is not
	// appended at all — it goes to the rewrite, which is bounded by the live
	// set and cannot overshoot.
	if s.fileBytes+int64(len(payload)) > s.readLimitLocked() {
		return false, nil
	}
	err = s.appendFn(s.filePath(), payload)
	written := len(payload)
	if err != nil {
		// A partial write leaves a torn tail THIS process put there. It owes
		// the same reconcile a crash-torn tail owes, and the rewrite that
		// settles it is the caller's fallback.
		s.fileAppendable = false
		if written > 0 {
			s.tornTailOwed = true
		}
		return false, err
	}
	s.fileBytes += int64(written)
	return true, nil
}

// appendPrivateFile appends to an existing 0600 spool file. It does not create
// one: appendability is established by a rewrite, which is what sets the mode.
//
// ⚠ IT SYNCS, AND THE PREVIOUS VERSION OF THIS FUNCTION DID NOT. The atomic
// rewrite it replaces syncs the temp file before the rename, so a write that
// returned meant the bytes were on the device. An append that only writes and
// closes returns as soon as the kernel has the page — so a host crash loses
// records this spool has already reported durably spooled, and the resend
// contract is exactly the promise that does not survive that. The whole
// sequencing argument for Phase 1 is "durability is exactly as strong as
// today", which was false without this line.
func appendPrivateFile(path string, payload []byte) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(payload); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// compactionOwedLocked reports whether dead records have accumulated past the
// slack — see spoolCompactionSlack for why this is not per-append.
func (s *diskSpool) compactionOwedLocked() bool {
	if !s.fileAppendable {
		return false
	}
	live := s.liveContentBytesLocked()
	if s.fileBytes*2 > live*spoolCompactionSlack {
		return true
	}
	// ⚠ AND NEVER PAST WHAT THE LOADER WILL READ. The slack trigger above is
	// relative to the LIVE content, so a spool whose live set is small while
	// its file is large is covered by it — but the two bounds are derived
	// differently and only this one is the loader's. Compacting at three
	// quarters of the read limit leaves room for the appends between this
	// check and the next.
	return s.fileBytes*4 > s.readLimitLocked()*3
}

func (s *diskSpool) saveLocked() error {
	merged := make([]spoolEntry, 0, len(s.entries))
	seen := make(map[string]struct{}, len(s.entries))
	foreign := 0
	// ⚠ THE MIRROR'S ENVELOPE WINS, AND THE DISK ONLY SUPPLIES THE POSITION.
	// A disk occurrence sharing an id with a live mirror entry is NOT proof
	// that the two are the same generation: an ack or eviction whose rewrite
	// failed leaves the settled envelope on disk, and an id reused before that
	// retry lands makes both exist at once. Emitting the disk copy there would
	// persist the OLD envelope while reporting the new one durable, and a
	// restart would resend the settled payload and lose its replacement.
	//
	// Taking the mirror's entry is right in both cases: when the disk
	// occurrence IS the live generation the two are equal, and when it is not,
	// the mirror is authoritative. This is the rewrite path, which is already
	// O(live), so indexing the mirror costs it nothing it was not paying.
	live := make(map[string]spoolEntry, len(s.entries))
	for _, entry := range s.entries {
		live[entry.id] = entry
	}
	for _, entry := range s.readRecordEntriesLocked() {
		if entry.id == "" {
			continue
		}
		mirrored, isLive := live[entry.id]
		if _, settled := s.settledIDs[entry.id]; settled {
			// Suppression is per-id but the history is per-generation: an id
			// stays settled after the generation that settled it was removed,
			// so a newer live occurrence read from the append log must not
			// inherit it — skipping it here and re-adding it from s.entries at
			// the end of the merge loses its position, and with it the order
			// the caps evict in.
			if !isLive {
				continue
			}
		}
		if _, withdrawn := s.withdrawnOwedIDs[entry.id]; withdrawn {
			// The unspent withdrawal marker's FULL id set — never the
			// bounded settled cache alone — keeps withdrawn facts out of
			// the merge (see the field comment).
			continue
		}
		if _, dup := seen[entry.id]; dup {
			continue
		}
		seen[entry.id] = struct{}{}
		if !isLive {
			foreign++
			merged = append(merged, entry)
			continue
		}
		merged = append(merged, mirrored)
	}
	for _, entry := range s.entries {
		if _, dup := seen[entry.id]; dup {
			continue
		}
		seen[entry.id] = struct{}{}
		merged = append(merged, entry)
	}
	if foreign > 0 && s.countForeign != nil {
		s.countForeign(foreign)
	}
	mergedBytes := 0
	for _, entry := range merged {
		mergedBytes += len(entry.raw)
	}
	var capDropped map[string]struct{}
	var capDrops []spoolEntry
	for len(merged) > 0 && (len(merged) > s.maxEvents || mergedBytes > s.maxBytes) {
		dropped := merged[0]
		mergedBytes -= len(dropped.raw)
		merged = merged[1:]
		s.recordSettledLocked(dropped.id)
		if _, ours := s.ids[dropped.id]; ours {
			// This process still claimed the entry: the mirror follows the
			// written record, and the drop surfaces through the standard
			// capacity dead-letter (drained by the client layer after the
			// current operation) — once the write below lands; a failed
			// write defers it (see deferredCapacityDrops).
			if capDropped == nil {
				capDropped = make(map[string]struct{})
			}
			capDropped[dropped.id] = struct{}{}
			delete(s.ids, dropped.id)
			s.totalBytes -= len(dropped.raw)
			capDrops = append(capDrops, dropped)
		}
	}
	if capDropped != nil {
		kept := s.entries[:0]
		for _, entry := range s.entries {
			if _, wasDropped := capDropped[entry.id]; !wasDropped {
				kept = append(kept, entry)
			}
		}
		s.entries = kept
	}
	if len(merged) == 0 {
		// An empty record must never park a future start behind a stale
		// Retry-After deadline.
		s.retryAfterUntilMS = 0
	}
	payload, err := s.renderDocumentLocked(merged)
	if err == nil {
		err = writePrivateFileAtomic(s.filePath(), payload, s.renameFn, s.chmodFn)
	}
	s.dirty = err != nil
	// A landed rewrite is the ONLY thing that makes the file appendable: it is
	// the moment the bytes on disk are known to be a v3 document matching the
	// committed state. A failed one clears the flag, which is also how a
	// legacy file, an absent file, and a recovered tear all take the rewrite
	// path without any of them needing a separate check.
	s.fileAppendable = err == nil
	s.tornTailOwed = s.tornTailOwed && err != nil
	if err == nil {
		s.fileBytes = int64(len(payload))
	}
	if err != nil {
		// The rewrite that would have removed these merge evictions from
		// disk did not land: their dead-letters wait for the save that does.
		s.deferredCapacityDrops = append(s.deferredCapacityDrops, capDrops...)
		return err
	}
	s.pendingCapacityDrops = append(s.pendingCapacityDrops, capDrops...)
	// This record just landed WITHOUT every previously deferred eviction
	// (settledIDs excluded them from the merge): those evictions are now
	// final, so their capacity dead-letters release.
	s.releaseDeferredCapacityDropsLocked()
	if len(s.uncountedIDs) > 0 {
		// Entries accepted under an earlier FAILED write just became
		// durable, IF they are still mirrored (one removed before any
		// successful write was never durably spooled and stays uncounted).
		for id := range s.uncountedIDs {
			if _, present := s.ids[id]; present {
				s.becameDurable++
			}
		}
		s.uncountedIDs = make(map[string]struct{})
	}
	if s.withdrawnMarkerOwed {
		if s.removeWithdrawnMarkerLocked() {
			// The record that just landed excludes the withdrawn entries
			// (the owed-id set kept them out of the merge): the marker is
			// spent.
			s.withdrawnMarkerOwed = false
			s.withdrawnOwedIDs = nil
		} else {
			// The clean record landed but the marker did not spend. A stale
			// marker honored at the next load would condemn a same-id FRESH
			// fact spooled after a re-enable, so an unspent marker is
			// unfinished save work: report the save failed and keep the
			// spool dirty so the flush-cadence retry re-attempts the spend.
			s.dirty = true
			return errSpoolMarkerSpendFailed
		}
	}
	return nil
}

// errSpoolMarkerSpendFailed reports a save whose record rewrite landed but
// whose withdrawal-marker spend did not (the marker file could not be
// removed): the save cycle is incomplete and retries on the flush cadence.
var errSpoolMarkerSpendFailed = errors.New("spool withdrawal marker spend failed")

// releaseDeferredCapacityDropsLocked moves evictions whose removal from disk
// has now durably happened — a successful rewrite that excluded them, or the
// record file's removal — into pendingCapacityDrops for the client layer to
// dead-letter and count.
func (s *diskSpool) releaseDeferredCapacityDropsLocked() {
	if len(s.deferredCapacityDrops) == 0 {
		return
	}
	s.pendingCapacityDrops = append(s.pendingCapacityDrops, s.deferredCapacityDrops...)
	s.deferredCapacityDrops = nil
}

// takeCapacityDrops drains the locally-owned entries a merging save evicted
// for capacity, for the client layer to dead-letter and count outside the
// lock.
func (s *diskSpool) takeCapacityDrops() []spoolEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	drops := s.pendingCapacityDrops
	s.pendingCapacityDrops = nil
	return drops
}

// takeBecameDurable drains the count of previously-uncounted mirror entries
// a successful save just made durable, for Stats.Spooled.
func (s *diskSpool) takeBecameDurable() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := s.becameDurable
	s.becameDurable = 0
	return count
}

// clearRetryDeadline drops the persisted Retry-After deadline after a
// successful batch publish: a success proves the backpressure window over,
// and a stale persisted deadline must not defer the next start's publishes.
// Returns whether a rewrite was needed and failed.
func (s *diskSpool) clearRetryDeadline() (persistFailed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retryAfterUntilMS == 0 {
		return false
	}
	s.retryAfterUntilMS = 0
	if s.owed {
		return false
	}
	return s.saveLocked() != nil
}

// storeRetryDeadline writes through a refreshed server Retry-After deadline
// (e.g. a 429 on a spooled resend) so a process exit before the in-memory
// retry still honors the remaining window at the next start.
func (s *diskSpool) storeRetryDeadline(deadlineMS int64) (persistFailed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if deadlineMS <= 0 || s.owed {
		return false
	}
	s.retryAfterUntilMS = deadlineMS
	return s.saveLocked() != nil
}

// removeRecordFile deletes spool.json; an already-absent file is success.
func (s *diskSpool) removeRecordFile() error {
	err := s.removeFn(s.filePath())
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// owedWipe reports whether a wipe is still owed (spool fail-closed).
func (s *diskSpool) owedWipe() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.owed
}

// settleOwedWipe retries an owed wipe. Returns true when no wipe is owed
// after the attempt (including when none was owed).
func (s *diskSpool) settleOwedWipe() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settleOwedWipeLocked()
}

func (s *diskSpool) settleOwedWipeLocked() bool {
	if !s.owed {
		return true
	}
	// Unlink ORDER: the record file first, then a DIRECTORY fsync, and
	// only then the marker (then a second sync) — the marker must OUTLIVE
	// the record it condemns. Unlinking both before one sync would let a
	// crash persist the MARKER's unlink but not the record's: no marker,
	// the condemned spool.json back on disk, a stale granted record — the
	// next launch would reload exactly what the denial condemned. With the
	// record's unlink durably synced FIRST, every crash interleaving fails
	// closed: the marker still on disk re-derives the debt and the settle
	// re-runs; the marker durably gone implies the record already was.
	// POSIX permits a crash to lose any un-synced unlink — and on the
	// purge-debt destruction rung this settle IS the only durable outcome
	// (record, marker, and record-retry all failed) — so a failed sync
	// keeps the wipe owed (fail-closed, retried) and that rung falls to
	// the surfaced posture.
	if s.removeRecordFile() != nil {
		return false
	}
	if s.syncFn(s.dir) != nil {
		return false
	}
	// The durable marker comes off only now — after the record's durable
	// destruction — and BEFORE the spool reopens: a stale marker would
	// re-condemn (wipe at the next start) anything spooled after this
	// settle, so its removal must be durable too before the wipe settles.
	// A failed removal or sync keeps the spool fail-closed (the record
	// file is already durably gone; the next settle retries just the
	// marker half). The WITHDRAWAL marker goes with it: the record's total
	// destruction subsumes any partial-withdrawal debt (a damaged marker's
	// unknowable id set included), and left behind it would condemn — or,
	// damaged, re-wipe — facts spooled fresh after this settle.
	if s.removeMarkerFile() != nil {
		return false
	}
	if err := s.removeFn(spoolWithdrawnPath(s.dir)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false
	}
	if s.syncFn(s.dir) != nil {
		return false
	}
	s.withdrawnMarkerOwed = false
	s.withdrawnOwedIDs = nil
	// The record file is durably gone: any deferred capacity evictions are
	// final.
	s.releaseDeferredCapacityDropsLocked()
	s.owed = false
	return true
}

// removeMarkerFile removes the wipe-owed marker through the injectable
// remove primitive (so tests can exercise a refused marker removal); an
// already-absent marker is success.
func (s *diskSpool) removeMarkerFile() error {
	err := s.removeFn(spoolWipeOwedPath(s.dir))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// condemnEntriesLocked clears the in-memory mirror and resend queue and
// tombstones every held entry (condemned data must not be resurrected by a
// later merging save), returning the entries for dead-lettering. This is
// the MEMORY half of a purge: condemning what the spool holds is never
// disk-dependent — only the record-file removal can fail, and only that
// removal is ever owed.
func (s *diskSpool) condemnEntriesLocked() []spoolEntry {
	dropped := s.entries
	for _, entry := range dropped {
		s.recordSettledLocked(entry.id)
	}
	s.entries = nil
	s.ids = make(map[string]struct{})
	s.uncountedIDs = make(map[string]struct{})
	s.totalBytes = 0
	s.resend = nil
	s.retryAfterUntilMS = 0
	s.dirty = false
	return dropped
}

// purge condemns the whole spool: the mirror and resend queue are cleared
// unconditionally (the entries are returned for dead-lettering — they are
// dropped from delivery either way), the record file is removed, and the
// WITHDRAWAL marker is spent with it. A failed record removal — or a
// withdrawal-marker spend that could not be made durable — owes a wipe: the
// durable wipe marker is created (best-effort; an in-memory owed flag fails
// closed regardless) and the spool refuses all disk work until a later wipe
// succeeds (the wipe's settle removes both markers, sync-fenced).
func (s *diskSpool) purge() (dropped []spoolEntry, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dropped = s.condemnEntriesLocked()
	if err = s.removeRecordFile(); err != nil {
		s.owed = true
		_ = createWipeOwedMarker(s.dir)
		return dropped, err
	}
	// The record file is gone: any deferred capacity evictions are final.
	s.releaseDeferredCapacityDropsLocked()
	// The WITHDRAWAL marker goes with the record: the purge is a
	// total-destruction rung exactly like the owed wipe, whose settle
	// already subsumes any partial-withdrawal debt — a damaged marker's
	// unknowable id set included — so spending it here is always safe. A
	// stale spool.withdrawn.json left behind would never be loaded by a
	// purge-path start (withdrawnMarkerOwed never arms, so no save spends
	// it) and a LATER granted restart would honor it against the record:
	// condemning fresh same-id facts, or — damaged — escalating that start
	// to the wipe rung. The spend follows the durable-unlink discipline
	// (ENOENT-tolerant unlink, then the parent sync) BEFORE the purge
	// reports complete: unlike the record's un-synced unlink, whose loss
	// re-purges idempotently at the next non-grant init, a resurrected
	// withdrawal marker is not idempotently healed. A failed spend fails
	// closed onto the owed-wipe rung.
	if removeErr := s.removeFn(spoolWithdrawnPath(s.dir)); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
		s.owed = true
		_ = createWipeOwedMarker(s.dir)
		return dropped, removeErr
	}
	if syncErr := s.syncFn(s.dir); syncErr != nil {
		s.owed = true
		_ = createWipeOwedMarker(s.dir)
		return dropped, syncErr
	}
	s.withdrawnMarkerOwed = false
	s.withdrawnOwedIDs = nil
	if s.owed {
		// The purge itself succeeded, but the spool reopens only once the
		// durable marker is off disk too — a stale marker would re-condemn
		// (wipe at the next start) anything spooled after this purge.
		if s.removeMarkerFile() != nil {
			return dropped, nil
		}
		s.owed = false
	}
	return dropped, nil
}

// readRecordEntries best-effort parses the on-disk record WITHOUT loading it
// into the mirror — used at init to report what a non-grant purge is about
// to drop, and by the merging save to see a sibling writer's view.
func (s *diskSpool) readRecordEntries() []spoolEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readRecordEntriesLocked()
}

// readRecordBytesLocked reads spool.json through a hard size limit derived
// from the byte cap (SpoolMaxBytes plus a fixed framing overhead), so an
// unexpectedly large record — a previous buggy version, different caps, or
// local tampering — can never make startup or a merging save allocate
// unbounded memory before the caps get a chance to apply. An over-limit file
// returns errSpoolRecordOversized for the caller's corrupt-record handling.
func (s *diskSpool) readRecordBytesLocked() ([]byte, error) {
	file, err := os.Open(s.filePath())
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// The framing allowance scales with the EVENT cap as well: past ~64k
	// retained envelopes the per-event framing alone outgrows the fixed
	// overhead, and a self-written cap-full record must never read back as
	// oversized. Forty bytes per allowed event covers the version-2
	// spoolEventWire wrapper ({"raw":...} plus the optional internal-fact
	// flag, worst case 29 bytes) and the array separator with room to
	// spare.
	limit := s.readLimitLocked()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errSpoolRecordOversized
	}
	return data, nil
}

// spoolDocument is one parsed spool file, whatever version wrote it.
// spoolRecord is one line of the append log: either an appended entry, or a
// recorded eviction naming the id it removed.
type spoolRecord struct {
	entry spoolEntry
	drop  string
}

type spoolDocument struct {
	records           []spoolRecord
	retryAfterUntilMS int64
	// tornTail is set when the final line had no terminator: its bytes are
	// still on disk and the file MUST be rewritten before anything extends
	// it — see reconcile below.
	tornTail bool
	// legacy is set for a v2 document, which is rewritten as v3 on first save.
	legacy bool
	ok     bool
}

// parseSpoolDocument reads either format.
//
// ⚠ THE TORN TAIL IS NOT MERELY DROPPED, IT IS OWED A REWRITE. Discarding the
// unterminated final line keeps the RECORDS intact, but its bytes are still at
// the end of the file. Appending onto them concatenates the new record to a
// half-written one, and the new record's terminator turns the pair into a
// COMPLETE malformed line — which the next launch reads, correctly, as damage,
// and discards the whole backlog for. A crash that should have cost one record
// would cost every record, through the very mechanism chosen to prevent that.
func parseSpoolDocument(data []byte) spoolDocument {
	var doc spoolDocument
	if len(data) == 0 {
		return doc
	}

	// v2: a single JSON object. Recognised by its version field rather than
	// by shape, so a v3 header — also a JSON object on line one — cannot be
	// mistaken for one.
	var legacy spoolRecordWire
	if json.Unmarshal(data, &legacy) == nil && legacy.Version == spoolRecordVersion {
		doc.ok = true
		doc.legacy = true
		doc.retryAfterUntilMS = legacy.RetryAfterUntilMS
		doc.records = make([]spoolRecord, 0, len(legacy.Events))
		for _, wire := range legacy.Events {
			doc.records = append(doc.records, spoolRecord{entry: spoolEntryFromWire(wire)})
		}
		return doc
	}

	lines := bytes.Split(data, []byte{'\n'})
	// A file ending in a terminator splits with a trailing empty element; one
	// that does not ends with the torn bytes.
	if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	} else if len(lines) > 1 {
		doc.tornTail = true
		lines = lines[:len(lines)-1]
	} else {
		// A single unterminated line is a header that never completed: there
		// is nothing provable to keep.
		return doc
	}

	var header spoolHeaderWire
	if json.Unmarshal(lines[0], &header) != nil || header.Version != spoolRecordVersionLines {
		return doc
	}
	doc.ok = true
	doc.retryAfterUntilMS = header.RetryAfterUntilMS
	// The parser keeps every line, in order, and interprets none of them: an
	// entry record and a drop record are both just what the writer put there.
	ordered := make([]spoolRecord, 0, len(lines))
	for _, line := range lines[1:] {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var wire spoolEventWire
		if json.Unmarshal(line, &wire) != nil {
			// A record line that does not parse is damage in the MIDDLE of the
			// file, not a tear at its end. Skipping it keeps every other
			// record rather than discarding the backlog for one bad line.
			continue
		}
		if wire.Drop != "" {
			ordered = append(ordered, spoolRecord{drop: wire.Drop})
			continue
		}
		if len(wire.Raw) == 0 {
			continue
		}
		// ⚠ AN ID-LESS RECORD IS KEPT, NOT DROPPED HERE. A syntactically valid
		// envelope with no usable event_id is UNPROVABLE, not absent: load
		// classifies it through spoolEntryExpired, counts it in SpoolExpired
		// and surfaces it on OnSpoolDeadLetter. Filtering it in the parser
		// makes it disappear with no accounting at all — a silent loss where
		// the documented behaviour is a reported fail-closed drop, and the v2
		// path has always passed it through.
		ordered = append(ordered, spoolRecord{entry: spoolEntryFromWire(wire)})
	}
	doc.records = ordered
	return doc
}

// live is the backlog the writing run held: the append log's records applied
// in order.
//
// ⚠ THIS USED TO RE-DERIVE THE WRITER'S EVICTIONS INSTEAD OF READING THEM, and
// that was the single most expensive decision in this change. Replaying the
// caps here made the reader a SECOND IMPLEMENTATION of eviction that had to
// agree with the writer's in every detail — and it did not, four separate
// times: it got the byte cap wrong by de-duplicating first, it mis-ordered
// re-appended ids, it lost the pressure of batch members evicted on arrival,
// and it cost O(n²) to run. Each was found, one review round apart, and each
// fix taught the copy one more thing the original already knew.
//
// A reader cannot re-derive a decision that depended on state the writer had
// and the file does not. So the writer now WRITES THE DECISION: an eviction
// appends a drop record naming the id. This function applies drops; it does
// not decide them. That is why it no longer consults the caps at all, and why
// there is nothing here left to disagree with.
func (doc spoolDocument) live() []spoolEntry {
	ordered := make([]spoolEntry, 0, len(doc.records))
	// ⚠ LIVENESS IS TRACKED SEPARATELY, NOT SIGNALLED BY A BLANK id. Using the
	// empty id as the tombstone conflated "this was evicted" with "this record
	// never carried an id" — and the second is data the LOAD has to see, so it
	// can dead-letter it as unprovable rather than have the parser discard it.
	alive := make([]bool, 0, len(doc.records))
	at := make(map[string]int, len(doc.records))
	kill := func(id string) {
		if idx, ok := at[id]; ok && idx >= 0 {
			alive[idx] = false
			at[id] = -1
		}
	}
	for _, rec := range doc.records {
		if rec.drop != "" {
			kill(rec.drop)
			continue
		}
		// An id-less record cannot be named by a drop and cannot collide with
		// a later generation, so it is simply carried.
		if rec.entry.id != "" {
			// A re-appended id supersedes its live copy in place. The writer
			// only appends an id the index does not hold, so this can only be
			// a generation the file records as dropped further up — but
			// applying it unconditionally keeps the two readings identical.
			kill(rec.entry.id)
			at[rec.entry.id] = len(ordered)
		}
		ordered = append(ordered, rec.entry)
		alive = append(alive, true)
	}
	out := make([]spoolEntry, 0, len(ordered))
	for i, entry := range ordered {
		if !alive[i] {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func spoolEntryFromWire(wire spoolEventWire) spoolEntry {
	var envelope spoolEnvelopeWire
	_ = json.Unmarshal(wire.Raw, &envelope)
	return spoolEntry{id: envelope.EventID, ts: envelope.EventTS, raw: wire.Raw, internalFact: wire.InternalFact}
}

// readLimitLocked bounds what the loader will read.
//
// ⚠ IT HAS TO ADMIT THE LARGEST FILE THIS SPOOL CAN LEGITIMATELY WRITE, and
// before logical eviction it did — the file was a rewrite of the live backlog,
// so live content plus framing was the whole of it. It is not any more: dead
// records ride the file until compaction, deliberately, up to the slack. A
// reader still bounded by the live size classifies a file this very process
// wrote as oversized, deletes it, and loses the entire backlog on the next
// start — the durability failure being introduced by the change that was
// supposed to be latency-only.
//
// So the bound scales with the slack, matching what the design already states:
// SpoolMaxBytes bounds the DELIVERABLE backlog, and the file holding it may
// reach about 1.5x that.
func (s *diskSpool) readLimitLocked() int64 {
	live := int64(s.maxBytes) + 40*int64(s.maxEvents)

	return live*spoolCompactionSlack/2 + spoolRecordReadOverhead
}

func (s *diskSpool) readRecordEntriesLocked() []spoolEntry {
	data, err := s.readRecordBytesLocked()
	if err != nil {
		return nil
	}
	doc := parseSpoolDocument(data)
	if !doc.ok {
		return nil
	}
	return doc.live()
}

// spoolLoadOutcome is what a startup load produced: the drops to report,
// the re-seeded deferral (zero when none), and whether the init rewrite
// failed.
type spoolLoadOutcome struct {
	expired []spoolEntry
	evicted []spoolEntry
	// condemned holds experiment-fact-class entries a DARK client dropped
	// under a damaged withdrawal marker (the class-scoped fail-closed
	// remedy; see load) — dead-lettered terminal by the caller.
	condemned     []spoolEntry
	deferUntil    time.Time
	persistFailed bool
}

// load reads spool.json into the mirror at startup (persisted-grant path
// only): every envelope is minimally parsed for its id and event_ts,
// age-filtered, de-duplicated, and re-capped; survivors seed the resend
// queue. A still-future retry_after_until_ms re-seeds the publish deferral
// with the remaining window (clamped); an expired one is dropped by the
// rewrite. A record that does not parse at all is a clean start (nothing
// provable to keep — the file is removed best-effort).
func (s *diskSpool) load(now time.Time) spoolLoadOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	var outcome spoolLoadOutcome
	data, err := s.readRecordBytesLocked()
	if err != nil {
		if errors.Is(err, errSpoolRecordOversized) {
			// Over the bounded read limit: not a record this spool could have
			// written, treated exactly like a record that does not parse.
			_ = s.removeRecordFile()
		}
		return outcome
	}
	doc := parseSpoolDocument(data)
	if !doc.ok {
		_ = s.removeRecordFile()
		return outcome
	}
	record := spoolRecordWire{Version: spoolRecordVersion, RetryAfterUntilMS: doc.retryAfterUntilMS}
	// ⚠ THE FILE'S OWN CAPS BIND FIRST. Trimming under the configured caps
	// alone would let a build that RAISED SpoolMaxEvents resurrect records the
	// writing run had already evicted — they fit again, so they load and
	// resend as events this SDK reported dropped. Trimming under the header's
	// caps reproduces the writing run's cut; the configured caps, applied by
	// the existing re-cap below, can then only shrink it further.
	for _, entry := range doc.live() {
		record.Events = append(record.Events, spoolEventWire{Raw: entry.raw, InternalFact: entry.internalFact})
	}
	// A recovered tear, or a v2 file, owes the reconcile rewrite before
	// anything may extend the file. load's own rewrite settles it.
	s.tornTailOwed = doc.tornTail
	s.fileAppendable = false
	withdrawn, markerDamaged := readWithdrawnMarker(s.dir, s.withdrawnMarkerReadLimit())
	factClassCondemned := false
	if markerDamaged {
		// A PRESENT-but-unusable withdrawal marker (unreadable, over the
		// bound, corrupt) is a withdrawal whose id set is UNKNOWABLE — it
		// must never collapse to "absent" and reload withdrawn facts for
		// resend (the terminal-withdrawal contract is privacy-side). The
		// REMEDY is scoped by the experiments opt-in:
		//   - ENABLED: fail closed by escalating to the wipe rung — total
		//     durable destruction of the record subsumes the unknowable
		//     partial withdrawal, the owed wipe blocks all disk work until
		//     it settles durably (record first, then both markers, each
		//     sync-fenced), and facts spooled after the settle are FRESH —
		//     never condemned by the stale marker, which the settle
		//     removed.
		//   - DARK: the marker is an experiment-only artifact, and only
		//     experiment facts can be the withdrawal's subject — so fail
		//     closed WITHIN the experiment-fact class instead: every
		//     fact-class entry is dropped below (a superset of whatever
		//     the unreadable id set named), the ordinary spool stays alive
		//     and serving, and the damaged marker stays ON DISK — unspent,
		//     still owed — for a future experiments-enabled session to
		//     adjudicate on the wipe rung. Escalating a dark client to the
		//     whole-record wipe would let an experiment-only persistence
		//     artifact destroy ordinary analytics durability the dark-off
		//     guarantee promises to leave untouched.
		if s.experimentsEnabled {
			s.owed = true
			_ = s.markerFn(s.dir)
			return outcome
		}
		factClassCondemned = true
	}
	withdrawnIDs := make(map[string]struct{}, len(withdrawn))
	for _, id := range withdrawn {
		withdrawnIDs[id] = struct{}{}
	}
	if len(withdrawnIDs) > 0 {
		s.withdrawnMarkerOwed = true
		// The FULL marker set backs the merge exclusion until the marker
		// spends — the bounded settled cache alone cannot (see
		// withdrawnOwedIDs).
		s.withdrawnOwedIDs = withdrawnIDs
	}
	for _, wire := range record.Events {
		var envelope spoolEnvelopeWire
		_ = json.Unmarshal(wire.Raw, &envelope)
		entry := spoolEntry{id: envelope.EventID, ts: envelope.EventTS, raw: wire.Raw, internalFact: wire.InternalFact}
		if _, isWithdrawn := withdrawnIDs[entry.id]; isWithdrawn {
			// A previous process withdrew this envelope (the real-subjects
			// sentinel) but exited before the clean rewrite landed: honor
			// the durable withdrawal — never resend. Settled, so the
			// merging save cannot resurrect it; the marker spends when the
			// rewrite below (or any later one) lands.
			s.recordSettledLocked(entry.id)
			continue
		}
		if factClassCondemned && entry.internalFact && withdrawnExperimentFactRaw(entry.raw) {
			// The dark client's class-scoped fail-closed under a damaged
			// marker: the marker testified that SOME experiment facts were
			// withdrawn, and the whole fact class is the smallest knowable
			// superset of its unreadable id set. Settled so the merging
			// save cannot resurrect them — with the unbounded owed-id set
			// backing that exclusion past the bounded settled cache — and
			// returned for the caller's terminal dead-letter; the damaged
			// marker itself is deliberately NOT spent (withdrawnMarkerOwed
			// stays unarmed, so no rewrite removes the file).
			s.recordSettledLocked(entry.id)
			if s.withdrawnOwedIDs == nil {
				s.withdrawnOwedIDs = make(map[string]struct{})
			}
			s.withdrawnOwedIDs[entry.id] = struct{}{}
			outcome.condemned = append(outcome.condemned, entry)
			continue
		}
		if spoolEntryExpired(entry, now) {
			s.recordSettledLocked(entry.id)
			outcome.expired = append(outcome.expired, entry)
			continue
		}
		if _, dup := s.ids[entry.id]; dup {
			continue
		}
		s.entries = append(s.entries, entry)
		s.ids[entry.id] = struct{}{}
		s.totalBytes += len(entry.raw)
	}
	outcome.evicted = s.evictOverCapsLocked()
	// The persisted deadline re-seeds the deferral only while loaded events
	// remain to protect: a load that discarded EVERY saved event (expired,
	// undatable, evicted by smaller caps) has nothing left behind the
	// backpressure window, and gating brand-new events on it — up to the 24h
	// clamp — would defer fresh work for a record that no longer exists. The
	// stale deadline is dropped along with the empty record (retryAfterUntilMS
	// stays zero, and the rewrite below persists that).
	if len(s.entries) > 0 && record.RetryAfterUntilMS > now.UnixMilli() {
		remaining := time.Duration(record.RetryAfterUntilMS-now.UnixMilli()) * time.Millisecond
		if remaining > spoolMaxDeferralSeed {
			remaining = spoolMaxDeferralSeed
		}
		outcome.deferUntil = now.Add(remaining)
		// The STORED deadline is clamped too, not just the seeded deferral:
		// re-persisting a far-future raw value (a far-forward clock at write
		// time, or a tampered file) would hand every subsequent restart a
		// fresh full-clamp deferral — the rewrite below persists the bounded
		// absolute instant instead, so a restart chain can never be parked
		// longer than one clamp window from this load.
		s.retryAfterUntilMS = outcome.deferUntil.UnixMilli()
	}
	s.resend = append([]spoolEntry(nil), s.entries...)
	outcome.persistFailed = s.saveLocked() != nil
	if outcome.persistFailed && len(outcome.evicted) > 0 {
		// Same deferral as append: the rewrite that would have removed the
		// load-time evictions from disk failed, so their dead-letters wait
		// for the save that durably lands.
		s.deferredCapacityDrops = append(s.deferredCapacityDrops, outcome.evicted...)
		outcome.evicted = nil
	}
	return outcome
}

// evictOverCapsLocked applies the count cap then the byte cap, evicting
// OLDEST first, and returns the evicted entries (settled — an evicted event
// was dead-lettered and must not be resurrected by a merging save).
func (s *diskSpool) evictOverCapsLocked() []spoolEntry {
	var evicted []spoolEntry
	for len(s.entries) > 0 && (len(s.entries) > s.maxEvents || s.totalBytes > s.maxBytes) {
		oldest := s.entries[0]
		s.entries = s.entries[1:]
		delete(s.ids, oldest.id)
		s.recordSettledLocked(oldest.id)
		s.totalBytes -= len(oldest.raw)
		evicted = append(evicted, oldest)
	}
	return evicted
}

// append records a failed batch's envelopes. allowed is evaluated UNDER the
// spool lock so an append can never race a consent purge into re-creating a
// condemned record (SetConsent stores the denied state before its purge
// takes this lock). The retry-age cap is enforced AT APPEND, not just at
// load: an envelope already expired (or future-dated beyond the skew
// tolerance) when it fails is returned as expired and never lands on disk —
// the documented retention bound holds without waiting for a restart.
// De-duplicated by event_id; oldest-first eviction may reach into the batch
// being appended, in which case only the survivors count as durably spooled.
// deadlineMS, when non-zero, records the live server Retry-After deadline
// this batch was spooled under; clearStaleDeadline marks a retriable FAILURE
// that carried no deadline — the server's latest word governs either way, so
// a live hint replaces the stored deadline and a hintless failure WITHDRAWS
// it (a restart must not park behind a window the server stopped asserting;
// mirrors applyRetryPacing, whose client-side backoff is never persisted).
// The Close remnant append is not a failure outcome and passes
// clearStaleDeadline=false, so a live window survives it into the next
// start. An append that changes nothing (everything expired or duplicate, no
// deadline change) skips the rewrite entirely.
func (s *diskSpool) append(batch []spoolEntry, deadlineMS int64, clearStaleDeadline bool, now time.Time, allowed func() bool) (refused bool, added, expired, evicted []spoolEntry, persistFailed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owed || !allowed() {
		return true, nil, nil, nil, false
	}
	appended := make(map[string]struct{}, len(batch))
	// ⚠ THE ENTRIES ACTUALLY INSERTED, NOT THE BATCH THEY CAME FROM. Rebuilding
	// this by id from `batch` takes the FIRST occurrence of an id, which is the
	// wrong one whenever an expired copy precedes a fresh one: the loop below
	// rejects the stale entry and stores the fresh one, but the reconstruction
	// would hand the stale envelope to the append. The call then reports the
	// fresh event durable while the file holds the copy a restart expires.
	inserted := make([]spoolEntry, 0, len(batch))
	for _, entry := range batch {
		if entry.id == "" {
			continue
		}
		if spoolEntryExpired(entry, now) {
			expired = append(expired, entry)
			continue
		}
		if _, exists := s.ids[entry.id]; exists {
			continue
		}
		s.entries = append(s.entries, entry)
		s.ids[entry.id] = struct{}{}
		s.totalBytes += len(entry.raw)
		appended[entry.id] = struct{}{}
		inserted = append(inserted, entry)
	}
	deadlineChanged := false
	if deadlineMS > 0 {
		s.retryAfterUntilMS = deadlineMS
		deadlineChanged = true
	} else if clearStaleDeadline && s.retryAfterUntilMS != 0 {
		// The latest retriable failure carried no Retry-After: the stored
		// deadline is no longer the server's word, and keeping it would park
		// a restart's publishes behind backpressure nobody is asserting.
		s.retryAfterUntilMS = 0
		deadlineChanged = true
	}
	if len(appended) == 0 && !deadlineChanged {
		// Nothing new to write — but a PREVIOUS append may have accepted
		// entries into the mirror under a failed record write (dirty). This
		// path is exactly how that batch comes back (a retained batch's
		// in-process retry, and Close's remnant settle, re-append as
		// duplicates), so it must retry the recovered write rather than skip
		// it: a transient disk error at the original append followed only by
		// duplicate re-appends would otherwise hold the events in memory all
		// the way through shutdown and lose them on exit.
		if s.dirty {
			persistFailed = s.saveLocked() != nil
		}
		return false, nil, expired, nil, persistFailed
	}
	evicted = s.evictOverCapsLocked()
	for _, entry := range inserted {
		// `inserted` already holds exactly one entry per id — the one the loop
		// above stored — so no de-duplication is needed here, and none may be
		// applied by id lest it pick a different envelope than the mirror has.
		if _, survived := s.ids[entry.id]; survived {
			added = append(added, entry)
		}
	}
	// ⚠ EVICTION MUST NOT DISQUALIFY THE FAST PATH, and gating on it was a
	// defect that measured 234x against a 2x bound. Eviction happens when the
	// spool is FULL — which is exactly the window the bound measures p95 in —
	// so sending every eviction to the rewrite puts the O(spool) cost back
	// precisely where this change removes it, while a measurement that stops
	// at the cap never sees it. That is why eviction here is LOGICAL: the
	// entry leaves the mirror at once, its bytes stay on disk as a dead
	// record, and compaction reclaims them amortised.
	//
	// The load needs no cooperation because it is TOLD: each eviction appends a
	// drop record naming the id, so a reload applies the writing run's cut
	// instead of trying to reconstruct it (see spoolDocument.live).
	//
	// What DOES disqualify it is state that only a rewrite can settle: a
	// deadline change (it lives in the header), and anything owed from an
	// earlier failure.
	fastPath := false
	if !deadlineChanged && !s.dirty && len(s.uncountedIDs) == 0 &&
		!s.withdrawnMarkerOwed && len(s.deferredCapacityDrops) == 0 {
		appendedEntries := make([]spoolEntry, 0, len(added))
		for _, entry := range added {
			appendedEntries = append(appendedEntries, entry)
		}
		droppedIDs := make([]string, 0, len(evicted))
		for _, entry := range evicted {
			droppedIDs = append(droppedIDs, entry.id)
		}
		wrote, _ := s.appendRecordsLocked(appendedEntries, droppedIDs)
		if wrote {
			fastPath = true
			s.dirty = false
			// ⚠ THE APPEND'S DURABILITY IS NOT THE COMPACTION'S. The records
			// above are fsynced before this line; a compaction rewrite that
			// then fails has not undone them. Conflating the two marked every
			// addition uncounted for a failure that did not touch it, so a
			// disk-full Close reported durable records as discarded — and
			// once an ack removed the id, nothing was left to repair the
			// count from.
			if s.compactionOwedLocked() {
				// Dead records past the slack: reclaim them now. This is the
				// amortised O(live) rewrite, not a per-append one. Its
				// failure costs only the reclaim: the records and the drops
				// are already on disk, so nothing above is less durable for
				// it and there is nothing to defer.
				_ = s.saveLocked()
			}
			// ⚠ THE EVICTIONS ARE RETURNED, SO THEY MUST NOT ALSO BE QUEUED.
			// pendingCapacityDrops carries evictions the caller never sees —
			// the ones a MERGING save makes. These the caller already has as
			// the append's return value; queuing them too made ordinary
			// overflow dead-letter and count every dropped event twice.
			//
			// Nor are they deferred: the drop records landed with the batch,
			// so a reload applies the same cut and no crash can resurrect
			// them.
		}
	}
	if !fastPath {
		persistFailed = s.saveLocked() != nil
	}
	if persistFailed {
		// Accepted into the mirror but not durable: deliberately uncounted
		// until a later save lands (see uncountedIDs).
		for _, entry := range added {
			s.uncountedIDs[entry.id] = struct{}{}
		}
		// The failed rewrite left PREVIOUSLY SAVED evictions in the on-disk
		// record: a crash now would reload and resend those, so their
		// capacity dead-letters defer until the eviction durably lands
		// rather than reporting a drop the disk could still undo. An
		// eviction with NO durable copy — a member of THIS batch, or one
		// accepted under an earlier failed write with no successful save
		// since (still in uncountedIDs) — has nothing the disk could undo:
		// it is a SETTLED loss the moment it leaves the mirror and must be
		// returned and counted NOW. Deferring it too would let a disk-full
		// Close exit with the member neither mirrored nor counted — lost
		// without ever surfacing in the discard verdict.
		var deferred, settled []spoolEntry
		for _, entry := range evicted {
			_, fromThisBatch := appended[entry.id]
			_, unsaved := s.uncountedIDs[entry.id]
			if fromThisBatch || unsaved {
				delete(s.uncountedIDs, entry.id)
				settled = append(settled, entry)
				continue
			}
			deferred = append(deferred, entry)
		}
		s.deferredCapacityDrops = append(s.deferredCapacityDrops, deferred...)
		evicted = settled
	}
	return false, added, expired, evicted, persistFailed
}

// ack removes settled events by id — a 2xx delivery or a terminal ingest
// outcome — from the mirror (and the resend queue) and rewrites the record.
// Returns the removed entries so a terminal settle can dead-letter exactly
// what was still spooled.
func (s *diskSpool) ack(ids []string) (removed []spoolEntry, persistFailed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(ids) == 0 || len(s.entries) == 0 {
		return nil, false
	}
	acked := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		acked[id] = struct{}{}
	}
	kept := s.entries[:0]
	for _, entry := range s.entries {
		if _, isAcked := acked[entry.id]; isAcked {
			removed = append(removed, entry)
			delete(s.ids, entry.id)
			s.recordSettledLocked(entry.id)
			s.totalBytes -= len(entry.raw)
			continue
		}
		kept = append(kept, entry)
	}
	s.entries = kept
	if len(removed) == 0 {
		return nil, false
	}
	keptResend := s.resend[:0]
	for _, entry := range s.resend {
		if _, isAcked := acked[entry.id]; !isAcked {
			keptResend = append(keptResend, entry)
		}
	}
	s.resend = keptResend
	persistFailed = s.saveLocked() != nil
	return removed, persistFailed
}

// removeMatching removes every spooled envelope the predicate matches (the
// predicate reads the entry's exact wire bytes), with ack's mechanics: the
// mirror, id set, byte total, and resend queue all forget the removed
// entries, the removal is persisted, and the removed entries are returned
// for the caller to dead-letter. Unlike an ack, this removal is a TERMINAL
// WITHDRAWAL: before the mirror forgets anything, the removed ids persist
// in the withdrawal marker, so a crash before a successful record rewrite
// cannot resurrect withdrawn facts into the next process's resend queue —
// the marker is honored at load and spends once a rewrite without those
// entries lands. A failed persist is reported the same way ack reports one
// — the mirror is authoritative and the next save retries.
func (s *diskSpool) removeMatching(matches func(raw json.RawMessage) bool, currentExpEpoch uint64) (removed []spoolEntry, persistFailed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return nil, false
	}
	removedIDs := make(map[string]struct{})
	for _, entry := range s.entries {
		if currentExpEpoch > 0 && entry.expFactEpoch >= currentExpEpoch {
			// Stamped AT the sweeping purge's own (post-bump) epoch: a
			// fresh post-purge fact — typically a drop-time capture that
			// landed mid-sweep from under e.mu, outside the emitMu window —
			// is never withdrawn by the purge that predates it.
			continue
		}
		if entry.internalFact && matches(entry.raw) {
			// The SDK-authorship flag joins the raw-shape match: only this
			// SDK's own facts are ever withdrawn — a host-authored envelope
			// that resembles one is never condemned by the sentinel.
			removed = append(removed, entry)
			removedIDs[entry.id] = struct{}{}
		}
	}
	if len(removed) == 0 {
		return nil, false
	}
	// Durability first: persist the withdrawal intent BEFORE the mirror
	// forgets the entries. A failed marker write proceeds regardless — the
	// save below may still land the clean record; both failing is the
	// storage-dead residual the persist flag surfaces. The debt arms and
	// the in-memory FULL id set backs the merge exclusion EITHER WAY
	// (writeWithdrawnMarkerLocked populates it before attempting the
	// write): a failed marker write must not degrade the exclusion to the
	// bounded settled cache, which a large withdrawal overflows — the save
	// would merge the oldest withdrawn ids back from the stale record and
	// report clean, resurrecting dead-lettered facts. The spend tolerates
	// a marker that never reached disk.
	ids := make([]string, 0, len(removed))
	for _, entry := range removed {
		ids = append(ids, entry.id)
	}
	_ = s.writeWithdrawnMarkerLocked(ids)
	s.withdrawnMarkerOwed = true
	kept := s.entries[:0]
	for _, entry := range s.entries {
		if _, isRemoved := removedIDs[entry.id]; isRemoved {
			delete(s.ids, entry.id)
			s.recordSettledLocked(entry.id)
			s.totalBytes -= len(entry.raw)
			continue
		}
		kept = append(kept, entry)
	}
	s.entries = kept
	keptResend := s.resend[:0]
	for _, entry := range s.resend {
		if _, isRemoved := removedIDs[entry.id]; !isRemoved {
			keptResend = append(keptResend, entry)
		}
	}
	s.resend = keptResend
	persistFailed = s.saveLocked() != nil
	return removed, persistFailed
}

// writeWithdrawnMarkerLocked arms the withdrawal debt and persists the
// withdrawn-id set, merging the still-unspent prior generation (a second
// purge before the first rewrite lands) from BOTH sources — the readable
// disk marker and the in-memory owed set. The in-memory FULL set backs the
// merge exclusion BEFORE the file write is attempted: even a FAILED marker
// write must keep withdrawn ids out of every following reload-and-merge
// save (the bounded settled cache alone lets the oldest ids of a large
// withdrawal merge back from the stale on-disk record and report a clean
// save), and the union means a damaged or unwritable disk generation never
// drops ids this process still remembers.
func (s *diskSpool) writeWithdrawnMarkerLocked(ids []string) error {
	prior, _ := readWithdrawnMarker(s.dir, s.withdrawnMarkerReadLimit())
	seen := make(map[string]struct{})
	all := make([]string, 0, len(ids))
	for priorOwed := range s.withdrawnOwedIDs {
		prior = append(prior, priorOwed)
	}
	for _, id := range append(prior, ids...) {
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		all = append(all, id)
	}
	s.withdrawnOwedIDs = seen
	payload, err := json.Marshal(all)
	if err != nil {
		return err
	}
	if err := writePrivateFileAtomic(spoolWithdrawnPath(s.dir), payload, s.renameFn, s.chmodFn); err != nil {
		return err
	}
	// The recovery-marker durability idiom (createWipeOwedMarker): the
	// marker's directory entry is fsynced before the withdrawal intent is
	// considered durably persisted — an un-synced marker could vanish in a
	// crash while the stale record survives, resurrecting withdrawn facts
	// at the next load.
	return s.syncFn(s.dir)
}

// removeWithdrawnMarkerLocked spends the marker; a missing file counts as
// removed, but the spend completes only once the directory entry's removal
// is durably synced — POSIX may lose an un-synced unlink in a crash, and a
// resurrected marker honored after the owed state cleared would condemn
// same-id FRESH facts spooled after re-enable (the settleOwedWipeLocked
// unlink discipline). The sync runs on the missing-file path too: a prior
// attempt may have unlinked and then failed exactly this sync.
func (s *diskSpool) removeWithdrawnMarkerLocked() bool {
	if err := s.removeFn(spoolWithdrawnPath(s.dir)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false
	}
	return s.syncFn(s.dir) == nil
}

func spoolWithdrawnPath(dir string) string {
	return filepath.Join(dir, spoolWithdrawnFileName)
}

// withdrawnMarkerReadLimit derives the marker read bound from the
// CONFIGURED spool capacity — never a fixed constant: a marker for a
// legitimately large configured withdrawal must read back after a crash.
// Floored at the legacy generous bound, ceiling-clamped for overflow
// safety.
func (s *diskSpool) withdrawnMarkerReadLimit() int64 {
	limit := int64(spoolWithdrawnReadLimit)
	if s.maxEvents > 0 {
		if s.maxEvents > spoolWithdrawnReadCeiling/spoolWithdrawnPerIDBytes {
			return spoolWithdrawnReadCeiling
		}
		derived := int64(s.maxEvents)*spoolWithdrawnPerIDBytes + int64(spoolRecordReadOverhead)
		if derived > limit {
			limit = derived
		}
	}
	if limit > spoolWithdrawnReadCeiling {
		limit = spoolWithdrawnReadCeiling
	}
	return limit
}

// readWithdrawnMarker reads the withdrawn-id set. damaged distinguishes a
// PRESENT-but-unusable marker — unreadable, over the bound, or corrupt —
// from a genuinely absent one: a damaged marker still testifies that a
// withdrawal happened whose id set is now unknowable, and callers must
// fail CLOSED on it (the load escalates to the wipe rung) instead of
// collapsing it to "no marker" and reloading withdrawn facts for resend.
func readWithdrawnMarker(dir string, limit int64) (ids []string, damaged bool) {
	file, err := os.Open(spoolWithdrawnPath(dir))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false
		}
		return nil, true
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, true
	}
	if json.Unmarshal(data, &ids) != nil {
		return nil, true
	}
	return ids, false
}

// pullResendChunk takes up to limit startup-loaded entries for re-publish,
// applying the retry-age cap at selection time (entries that expired while
// waiting are dropped from the mirror and returned as expired). Entries no
// longer in the mirror — acked or purged while queued — are skipped. The
// pulled chunk stays in the mirror until acked; only the resend queue
// forgets it (requeueResend puts it back on a retriable failure).
func (s *diskSpool) pullResendChunk(limit int, now time.Time) (chunk, expired []spoolEntry, persistFailed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owed {
		return nil, nil, false
	}
	for len(chunk) < limit && len(s.resend) > 0 {
		entry := s.resend[0]
		s.resend = s.resend[1:]
		if _, present := s.ids[entry.id]; !present {
			continue
		}
		if spoolEntryExpired(entry, now) {
			delete(s.ids, entry.id)
			s.recordSettledLocked(entry.id)
			s.totalBytes -= len(entry.raw)
			kept := s.entries[:0]
			for _, held := range s.entries {
				if held.id != entry.id {
					kept = append(kept, held)
				}
			}
			s.entries = kept
			expired = append(expired, entry)
			continue
		}
		chunk = append(chunk, entry)
	}
	if len(expired) > 0 {
		persistFailed = s.saveLocked() != nil
	}
	return chunk, expired, persistFailed
}

// requeueResend puts a chunk back at the FRONT of the resend queue after a
// retriable failure, preserving oldest-first order. Entries purged while the
// chunk was in flight are dropped, not re-queued.
func (s *diskSpool) requeueResend(chunk []spoolEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := make([]spoolEntry, 0, len(chunk)+len(s.resend))
	for _, entry := range chunk {
		if _, present := s.ids[entry.id]; present {
			kept = append(kept, entry)
		}
	}
	s.resend = append(kept, s.resend...)
}

// hasResendWork reports whether startup-loaded entries still await
// re-publish (never under an owed wipe: the spool is fail-closed then).
func (s *diskSpool) hasResendWork() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.owed && len(s.resend) > 0
}

// unpersistedOf counts which of the given ids are accepted into the mirror
// but NOT safely on disk: still tracked by uncountedIDs (accepted under a
// FAILED record write and not yet covered by a later successful one) and
// still mirrored (an entry evicted before any successful write was already
// reported through its capacity dead-letter). The close-remnant accounting
// asks this AFTER the final settle retry, so a recovered write — which
// clears uncountedIDs — reads as safe, and a remnant that merely
// DE-DUPLICATED against an earlier dirty append counts exactly like a
// fresh dirty add.
func (s *diskSpool) unpersistedOf(ids []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, id := range ids {
		if _, uncounted := s.uncountedIDs[id]; !uncounted {
			continue
		}
		if _, mirrored := s.ids[id]; mirrored {
			count++
		}
	}
	return count
}

// retryPersist re-attempts a failed record rewrite (flush-cadence retry).
// The first return reports whether a write was attempted at all.
func (s *diskSpool) retryPersist() (attempted, failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dirty || s.owed {
		return false, false
	}
	return true, s.saveLocked() != nil
}

// ── Client-level orchestration ──────────────────────────────────────────────

// spoolEntriesFromRequest pairs a built batch's typed envelopes with their
// retained wire bytes.
func spoolEntriesFromRequest(request batchRequest) []spoolEntry {
	if len(request.Events) == 0 || len(request.Events) != len(request.rawEvents) {
		return nil
	}
	entries := make([]spoolEntry, 0, len(request.Events))
	for i, envelope := range request.Events {
		entries = append(entries, spoolEntry{id: envelope.EventID, ts: envelope.EventTS, raw: request.rawEvents[i], internalFact: envelope.internalIdentityFact})
	}
	return entries
}

// spoolChunkRequest builds the wire request for a spooled chunk: raw bytes
// only, joined verbatim, so the resend is byte-identical to the record.
func spoolChunkRequest(chunk []spoolEntry) batchRequest {
	raws := make([]json.RawMessage, len(chunk))
	for i, entry := range chunk {
		raws[i] = entry.raw
	}
	return batchRequest{rawEvents: raws}
}

func spoolEntryIDs(chunk []spoolEntry) []string {
	ids := make([]string, len(chunk))
	for i, entry := range chunk {
		ids[i] = entry.id
	}
	return ids
}

func spoolDeadLetterFrom(reason SpoolDropReason, entries []spoolEntry) SpoolDeadLetter {
	envelopes := make([]json.RawMessage, len(entries))
	for i, entry := range entries {
		// The callback payload is the integrator's to keep or mutate: it must
		// never alias the live bytes backing the retained request or the
		// spool mirror, or a callback that redacts/annotates an envelope
		// would silently corrupt the byte-identical retry/resend contract
		// under that same event_id.
		envelopes[i] = append(json.RawMessage(nil), entry.raw...)
	}
	return SpoolDeadLetter{Reason: reason, Envelopes: envelopes}
}

// emitSpoolDeadLetters invokes the OnSpoolDeadLetter callback for each drop,
// recovering a panic per invocation like OnBatchResult. Never called with a
// lock held — the callback is integrator code.
func (c *Client) emitSpoolDeadLetters(letters []SpoolDeadLetter) {
	if c.cfg.OnSpoolDeadLetter == nil {
		return
	}
	for _, letter := range letters {
		if len(letter.Envelopes) == 0 {
			continue
		}
		func() {
			defer func() { _ = recover() }()
			c.cfg.OnSpoolDeadLetter(letter)
		}()
	}
}

func (c *Client) notifySpoolDeadLetter(reason SpoolDropReason, entries []spoolEntry) {
	if len(entries) == 0 {
		return
	}
	c.emitSpoolDeadLetters([]SpoolDeadLetter{spoolDeadLetterFrom(reason, entries)})
}

// recordSpoolExpired counts and dead-letters retry-age expiries.
func (c *Client) recordSpoolExpired(expired []spoolEntry) {
	if len(expired) == 0 {
		return
	}
	c.stats.spoolExpired.Add(uint64(len(expired)))
	c.notifySpoolDeadLetter(SpoolDropExpired, expired)
}

func (c *Client) recordSpoolPersistFailure() {
	c.stats.spoolPersistFailed.Add(1)
	c.logf("shardpilot spool: writing the spool record failed; the in-memory mirror stays authoritative and the write is retried on the flush cadence")
}

// spoolDeadlineFromError extracts the live server Retry-After deadline a
// retriable failure carried (already clamped by parseRetryAfter), for
// persisting alongside the spooled batch. Zero when the failure carried no
// usable hint — client-side backoff is not a server deadline and is never
// persisted.
func (c *Client) spoolDeadlineFromError(err error) int64 {
	var statusErr *HTTPStatusError
	if err == nil || !errors.As(err, &statusErr) {
		return 0
	}
	if !statusErr.Retryable() || !statusErr.retryAfterPresent {
		return 0
	}
	return c.clock.Now().Add(statusErr.RetryAfter).UnixMilli()
}

// drainSpoolCapacityDrops emits the locally-owned entries a merging save
// cap-evicted. Called after every client-level spool operation that can
// rewrite the record, outside the spool lock.
// drainSpoolCapacityDrops surfaces settled capacity evictions (dead-letter
// + SpoolEvicted) and returns how many it drained: the worker's stop path
// counts close-phase evictions into the discard fold — an eviction that
// lands at exit is a permanent loss with no later resend (still-DEFERRED
// evictions are excluded by takeCapacityDrops itself: their removing
// rewrite never landed, the old record still carries them, and the next
// launch reloads and resends them — the #34 deferral rule).
func (c *Client) drainSpoolCapacityDrops() int {
	if c.spool == nil {
		return 0
	}
	// Fold in entries a successful save just made durable after an earlier
	// failed write left them uncounted.
	if durable := c.spool.takeBecameDurable(); durable > 0 {
		c.stats.spooled.Add(uint64(durable))
	}
	drops := c.spool.takeCapacityDrops()
	if len(drops) == 0 {
		return 0
	}
	c.stats.spoolEvicted.Add(uint64(len(drops)))
	c.notifySpoolDeadLetter(SpoolDropCapacity, drops)
	return len(drops)
}

// partitionSpoolEligible splits a failed batch's envelopes by whether the
// persisted grant's actor scope covers them. The consent record (and its
// actor_digest) is scoped to the CONFIGURED actor tuple, but events may
// carry per-event UserID/AnonymousID overrides: an envelope whose EFFECTIVE
// actor differs from the configured tuple must never ride the configured
// actor's grant onto disk. Refuse-per-envelope rather than batch splitting:
// the in-memory batch stays whole for the live retry (the live pipeline is
// not consent-scoped per envelope), and only the DISK side filters.
func (c *Client) partitionSpoolEligible(request batchRequest) (eligible, refused []spoolEntry) {
	if len(request.Events) == 0 || len(request.Events) != len(request.rawEvents) {
		return nil, nil
	}
	for i, envelope := range request.Events {
		// The request's purge-epoch stamp rides each entry: the sentinel's
		// spool sweep spares entries born at its own epoch.
		entry := spoolEntry{id: envelope.EventID, ts: envelope.EventTS, raw: request.rawEvents[i], expFactEpoch: request.expPurgeEpoch, internalFact: envelope.internalIdentityFact}
		if c.spoolActorEligible(envelope) {
			eligible = append(eligible, entry)
		} else {
			refused = append(refused, entry)
		}
	}
	return eligible, refused
}

// spoolActorEligible reports whether the persisted grant's actor scope
// covers this envelope. UNDER THE FLOOR, eligibility mirrors the intake
// actor gate exactly (consentFloorActorMismatch): the envelope's EFFECTIVE
// actor — buildEnvelope already resolved per-event overrides over the
// configured identifiers — must equal the configured actor the decision
// covers; a secondary-identifier override (say AnonymousID under a
// configured UserID) does not change the effective actor, so an event the
// floor ADMITTED is never refused disk retention later (accepted-then-
// dead-lettered would contradict the round-4 override semantics). Floor
// OFF keeps the released strict rule — both envelope identifiers equal to
// the configured tuple — unchanged.
func (c *Client) spoolActorEligible(envelope eventEnvelope) bool {
	if envelope.internalIdentityFact {
		if c.consentFloorEnabled() && c.cfg.UserID != "" {
			// The mirror of intake's anonymous-actor gate
			// (enqueueExperimentFact → ErrConsentActorMismatch): under an
			// enabled floor with a UserID configured, the grant covers the
			// USER actor while the fact ships the anonymous identity alone
			// — the actor whose id actually rides the wire has no recorded
			// grant. Intake refuses such facts live; the drop-time capture
			// path reaches this eligibility check WITHOUT passing intake,
			// and admitting here would give an ungranted actor disk
			// retention (and a later publish) through the capture
			// side-door. Refused: the capture dead-letters as a consent
			// drop and the durable drop proceeds without it — the
			// documented consent-first posture.
			return false
		}
		// Otherwise the SDK's OWN experiment facts are the one envelope
		// shape whose wire identifiers deliberately differ from the
		// configured tuple WITHOUT an actor change: user_id is omitted by
		// the ingest contract for those event names while anonymous_id
		// carries the configured client identity. Their eligibility is the
		// same decision intake made, matched on the anonymous_id the fact
		// actually carries.
		return c.cfg.AnonymousID != "" && envelope.AnonymousID == c.cfg.AnonymousID
	}
	if c.consentFloorEnabled() {
		effective := firstNonEmpty(envelope.UserID, envelope.AnonymousID)
		return effective == firstNonEmpty(c.cfg.UserID, c.cfg.AnonymousID)
	}
	return envelope.UserID == c.cfg.UserID && envelope.AnonymousID == c.cfg.AnonymousID
}

// spoolFailedBatch appends a retriably failed worker batch to the spool.
// Under a non-grant live state, an unpersisted grant record, or an owed
// wipe, nothing touches disk and the would-have-spooled batch goes to the
// dead-letter callback instead. Eligibility is per-envelope: only envelopes
// whose effective actor the persisted grant covers may spool; the rest
// dead-letter as consent drops. abandoned marks a failure that was the
// flush caller's OWN context error (callerAbandonedFlush): the append still
// happens — the batch is undelivered work and spooling it is crash
// insurance, not pacing — but the persisted deadline stays untouched.
// Returns how many events the closed write gate refused, the ids the
// append left IN THE MIRROR — newly added or de-duplicated against an
// earlier append: a duplicate of an entry accepted under a FAILED write is
// exactly as unpersisted as a fresh add — the settled capacity evictions,
// and the retry-age expiries the append dropped, so the close-remnant path
// can tell whether the remnant is actually safe (see
// recordUnspooledCloseRemnant and diskSpool.unpersistedOf; the worker's
// mid-session calls ignore the counts — expiry and eviction are normal
// retention outcomes there, reported through their own stats).
func (c *Client) spoolFailedBatch(request batchRequest, cause error, abandoned bool) (gateRefused int, mirrored []string, capacityDropped, expiredDropped int) {
	s := c.spool
	if s == nil {
		return 0, nil, 0, 0
	}
	if c.exp != nil && request.expPurgeEpoch != c.expFactPurgeEpoch.Load() {
		// A real-subjects sentinel purge landed AFTER this batch was built
		// (it was in transport when the sentinel arrived): its withdrawn
		// experiment facts must not be written back into the pipeline the
		// purge just cleaned. Re-filter at respool time; the surviving
		// members keep their exact bytes. Deliberately NOT counted in
		// Stats.Dropped here: the same events still sit in the worker's
		// retained batch on the retriable path, and the worker-batch filter
		// counts them exactly once at its next dispatch point.
		filtered, removed := filterWithdrawnFromBatchRequest(request)
		if removed > 0 {
			request = filtered
			c.logf("shardpilot experiments: withheld %d withdrawn experiment fact(s) from a post-sentinel respool", removed)
		}
	}
	eligible, refusedActors := c.partitionSpoolEligible(request)
	c.notifySpoolDeadLetter(SpoolDropConsent, refusedActors)
	if len(eligible) == 0 {
		return 0, nil, 0, 0
	}
	// An owed wipe is retried before any append; still owed refuses disk.
	s.settleOwedWipe()
	deadlineMS := c.spoolDeadlineFromError(cause)
	// A retriable failure that carried NO usable Retry-After withdraws a
	// previously persisted deadline (the server's latest word governs). The
	// Close remnant append (cause == nil) is not a failure outcome: a live
	// persisted window must survive it into the next start. A caller-abandoned
	// flush is no failure OUTCOME either — nothing was learned from the
	// endpoint — so the abandonment rule extends to persisted pacing state
	// and the window survives untouched.
	clearStale := cause != nil && deadlineMS <= 0 && !abandoned
	refused, added, expired, evicted, persistFailed := s.append(eligible, deadlineMS, clearStale, c.clock.Now(), func() bool {
		return c.consent.Load() == consentStateGranted && s.grantPersisted
	})
	if refused {
		c.notifySpoolDeadLetter(SpoolDropConsent, eligible)
		return len(eligible), nil, 0, 0
	}
	// An envelope already past the retry-age cap when it fails never lands
	// on disk: the retention bound is enforced at append, not just at load.
	c.recordSpoolExpired(expired)
	// Spooled counts DURABLY spooled events only: a failed record rewrite
	// keeps the additions in the mirror (retried on the flush cadence) but
	// must not report them as safely on disk.
	if len(added) > 0 && !persistFailed {
		c.stats.spooled.Add(uint64(len(added)))
	}
	if len(evicted) > 0 {
		c.stats.spoolEvicted.Add(uint64(len(evicted)))
		c.notifySpoolDeadLetter(SpoolDropCapacity, evicted)
	}
	if persistFailed {
		c.recordSpoolPersistFailure()
	}
	capacityDropped = len(evicted) + c.drainSpoolCapacityDrops()
	// Everything eligible that was not dropped for retry age is in the
	// mirror now — freshly added, or a duplicate of an entry a previous
	// append already accepted (possibly under a failed write). Expiry is
	// judged per ENTRY, so remove exactly as many copies of an id as the
	// append expired (a MULTISET discount), never every copy sharing the
	// id: a batch can carry an expired stale copy AND a fresh duplicate
	// under the same event id, and filtering by bare id would drop the
	// retained fresh copy from the remnant accounting — a failed-save
	// close would then under-report it (unpersistedOf never asked).
	expiredCopies := make(map[string]int, len(expired))
	for _, entry := range expired {
		expiredCopies[entry.id]++
	}
	for _, entry := range eligible {
		if expiredCopies[entry.id] > 0 {
			expiredCopies[entry.id]--
			continue
		}
		mirrored = append(mirrored, entry.id)
	}
	return 0, mirrored, capacityDropped, len(expired)
}

// spoolAckWithVerdicts settles a delivered live batch's spooled copies from
// the response's per-event verdicts, exactly like settleResentChunk does for
// restart-loaded chunks: every event in the accepted batch is settled
// server-side and comes off the spool, but one the response marked rejected
// or consent-suppressed was DROPPED, not delivered — its previously spooled
// copy dead-letters with the matching class instead of vanishing as if
// delivered. (No SpoolResent counting here: that counter is for a previous
// process's records.)
func (c *Client) spoolAckWithVerdicts(request batchRequest, result batchResult) {
	s := c.spool
	if s == nil {
		return
	}
	entries := spoolEntriesFromRequest(request)
	if len(entries) == 0 {
		return
	}
	verdicts := make(map[string]EventStatus, len(result.Events))
	for _, event := range result.Events {
		verdicts[event.EventID] = EventStatus(event.Status)
	}
	// retry_later is not a settlement: the server stored nothing and asked for
	// the event again, so its spooled copy is the only one left. Acking it
	// here would erase the event and fire no dead-letter either — a loss
	// nothing anywhere reports. Kept in the mirror and put back on the resend
	// queue for the next pass.
	settleIDs, deferred := partitionRetryLater(entries, verdicts)
	removed, persistFailed := s.ack(settleIDs)
	if len(deferred) > 0 {
		s.requeueResend(deferred)
	}
	var terminal, consentDropped []spoolEntry
	for _, entry := range removed {
		reason, dropped := spoolVerdictDropReason(verdicts[entry.id])
		if !dropped {
			continue
		}
		if reason == SpoolDropConsent {
			consentDropped = append(consentDropped, entry)
		} else {
			terminal = append(terminal, entry)
		}
	}
	c.notifySpoolDeadLetter(SpoolDropTerminal, terminal)
	c.notifySpoolDeadLetter(SpoolDropConsent, consentDropped)
	if persistFailed {
		c.recordSpoolPersistFailure()
	}
	c.drainSpoolCapacityDrops()
}

// spoolSettleTerminal settles a terminally failed batch: previously spooled
// events are removed so a poison batch cannot re-fail every launch, and
// exactly those removed are dead-lettered as terminal.
func (c *Client) spoolSettleTerminal(request batchRequest) {
	entries := spoolEntriesFromRequest(request)
	if len(entries) == 0 {
		return
	}
	c.spoolSettleTerminalIDs(spoolEntryIDs(entries))
}

// spoolSettleTerminalIDs is spoolSettleTerminal for callers that hold event
// ids rather than a built request (the per-event poison drops, whose events
// never produced wire bytes).
func (c *Client) spoolSettleTerminalIDs(ids []string) {
	s := c.spool
	if s == nil || len(ids) == 0 {
		return
	}
	removed, persistFailed := s.ack(ids)
	if len(removed) > 0 {
		c.notifySpoolDeadLetter(SpoolDropTerminal, removed)
	}
	if persistFailed {
		c.recordSpoolPersistFailure()
	}
	c.drainSpoolCapacityDrops()
}

// spoolVerdictDropReason maps a per-event ingest verdict to the dead-letter
// class a spooled event settled by it drops with: a rejection is a terminal
// server outcome, and a consent suppression is a consent outcome. Everything
// else — accepted, observed, duplicate, an event absent from the verdict
// list, or a status this build does not recognize — reports no drop.
func spoolVerdictDropReason(status EventStatus) (SpoolDropReason, bool) {
	switch status {
	case EventStatusRejected:
		return SpoolDropTerminal, true
	case EventStatusSuppressedNoConsent, EventStatusSuppressedAdRevenueConsent:
		return SpoolDropConsent, true
	default:
		return "", false
	}
}

// settleResentChunk settles a re-published spool chunk from the response's
// per-event verdicts. Every event in an accepted batch is SETTLED on the
// server — removal from the spool is right for all of them, and retrying any
// would only re-produce the same verdict — but a 2xx is not delivery
// confirmation per event: a per-event terminal verdict (rejected, or a
// consent suppression, which the batch contract explicitly says is not
// delivery) means the spooled event was dropped, so it dead-letters with the
// matching class instead of counting as resent. An event the verdict list
// does not mention, or whose status this build cannot classify, settles as
// delivered — inventing a drop for an unrecognized status would false-alarm
// the dead-letter contract (mirroring Stats.ByStatus's carry-through
// posture).
// settleResentChunk reports whether any member was DEFERRED rather than
// settled — answered retry_later, so it stays spooled and queued for another
// pass. The caller ends this resend pass on a deferral instead of pulling the
// requeued entries straight back: the reservation they collided with lives
// for the ingest pending TTL, and re-sending inside the same loop would spin
// against the server for as long as it holds.
func (c *Client) settleResentChunk(chunk []spoolEntry, result batchResult) bool {
	verdicts := make(map[string]EventStatus, len(result.Events))
	for _, event := range result.Events {
		verdicts[event.EventID] = EventStatus(event.Status)
	}
	settleIDs, deferred := partitionRetryLater(chunk, verdicts)
	resent := 0
	for _, entry := range chunk {
		if verdicts[entry.id] == EventStatusRetryLater {
			// Not delivered: the server stored nothing for it.
			continue
		}
		if _, dropped := spoolVerdictDropReason(verdicts[entry.id]); !dropped {
			resent++
		}
	}
	if resent > 0 {
		c.stats.spoolResent.Add(uint64(resent))
	}
	removed, persistFailed := c.spool.ack(settleIDs)
	if len(deferred) > 0 {
		c.spool.requeueResend(deferred)
	}
	var terminal, consentDropped []spoolEntry
	for _, entry := range removed {
		reason, dropped := spoolVerdictDropReason(verdicts[entry.id])
		if !dropped {
			continue
		}
		if reason == SpoolDropConsent {
			consentDropped = append(consentDropped, entry)
		} else {
			terminal = append(terminal, entry)
		}
	}
	c.notifySpoolDeadLetter(SpoolDropTerminal, terminal)
	c.notifySpoolDeadLetter(SpoolDropConsent, consentDropped)
	if persistFailed {
		c.recordSpoolPersistFailure()
	}
	c.drainSpoolCapacityDrops()

	return len(deferred) > 0
}

// partitionRetryLater splits a settled batch's entries into the ids to ack and
// the entries to keep. An entry the server answered retry_later is owed, not
// delivered: nothing stored it, so it stays. Every other verdict — including
// one this build does not recognise, and one the list does not mention at all
// — settles, which keeps the carry-through posture the rest of this file has:
// inventing a deferral for an unknown status would hold events forever.
func partitionRetryLater(entries []spoolEntry, verdicts map[string]EventStatus) (settleIDs []string, deferred []spoolEntry) {
	settleIDs = make([]string, 0, len(entries))
	for _, entry := range entries {
		if verdicts[entry.id] == EventStatusRetryLater {
			deferred = append(deferred, entry)

			continue
		}
		settleIDs = append(settleIDs, entry.id)
	}

	return settleIDs, deferred
}

// spoolHasResendWork reports pending spool resend work for the worker's
// recovery wake: a success that proves the endpoint healthy again must kick
// requeued spooled chunks too, or spool-only work would idle until the next
// flush tick (potentially hours away) even though the batch that proved
// recovery already delivered.
func (c *Client) spoolHasResendWork() bool {
	return c.spool != nil && c.spool.hasResendWork()
}

// resendSpooledChunks re-publishes startup-loaded spool chunks on the
// worker's automatic path, BEFORE the fresh batch — spooled events are the
// oldest undelivered work. Outcomes pace exactly like fresh publishes
// (applyRetryPacing). Returns false when a retriable failure armed the
// pacing deadline: the fresh batch then waits behind the same gate.
func (c *Client) resendSpooledChunks(deferUntil *time.Time, backoffAttempt *int) bool {
	s := c.spool
	if s == nil {
		return true
	}
	for {
		chunk, expired, persistFailed := s.pullResendChunk(c.cfg.BatchSize, c.clock.Now())
		c.recordSpoolExpired(expired)
		if persistFailed {
			c.recordSpoolPersistFailure()
		}
		c.drainSpoolCapacityDrops()
		if len(chunk) == 0 {
			return true
		}
		if c.spoolResendHandoffSeam != nil {
			c.spoolResendHandoffSeam(chunk)
		}
		// Re-verify the pulled chunk against the current purge state at the
		// transport handoff: a real-subjects sentinel landing between the
		// pull and this point condemned members the mirror sweep cannot
		// reach (see dropWithdrawnSpoolChunkMembers).
		chunk = c.dropWithdrawnSpoolChunkMembers(chunk)
		if len(chunk) == 0 {
			continue
		}
		// Unbounded here: the transport bounds each HTTP attempt, so the
		// gzip-refusal fallback gets its own budget.
		result, err := c.publishRequestResult(context.Background(), spoolChunkRequest(chunk), len(chunk))
		c.applyRetryPacing(err, deferUntil, backoffAttempt)
		if err == nil {
			// Settle the delivered chunk off the spool BEFORE the callback:
			// a callback-driven consent flip must purge the remainder only.
			deferred := c.settleResentChunk(chunk, result)
			c.notifyBatchResult(result.toPublic())
			if deferred {
				// Some members are owed again and are back on the queue.
				// Nothing failed — the endpoint answered — so the fresh
				// batch is not held back; this pass simply stops rather
				// than re-pulling what it just put back.
				return true
			}
			continue
		}
		if errors.Is(err, ErrConsentDenied) {
			// The denial purged the spool; the aborted in-flight chunk is
			// dropped with it, never re-queued (the purge dead-lettered it).
			return true
		}
		if isPermanentPublishError(err) {
			removed, ackPersistFailed := s.ack(spoolEntryIDs(chunk))
			if len(removed) > 0 {
				c.notifySpoolDeadLetter(SpoolDropTerminal, removed)
			}
			if ackPersistFailed {
				c.recordSpoolPersistFailure()
			}
			continue
		}
		c.spoolStoreResendDeadline(err)
		s.requeueResend(chunk)
		return false
	}
}

// spoolStoreResendDeadline writes through the server Retry-After deadline a
// retriable spool-resend failure carried, so a process exit before the
// in-memory retry still honors the remaining window at the next start. A
// hintless retriable resend failure WITHDRAWS a previously persisted
// deadline instead — the same latest-word rule as the append path: the
// server stopped asserting a window, so a restart must not park behind the
// stale one.
func (c *Client) spoolStoreResendDeadline(err error) {
	deadlineMS := c.spoolDeadlineFromError(err)
	if deadlineMS <= 0 {
		if c.spool.clearRetryDeadline() {
			c.recordSpoolPersistFailure()
		}
		c.drainSpoolCapacityDrops()
		return
	}
	if c.spool.storeRetryDeadline(deadlineMS) {
		c.recordSpoolPersistFailure()
	}
	c.drainSpoolCapacityDrops()
}

// flushSpooledChunks is resendSpooledChunks for the explicit Flush path: it
// uses the flush caller's context, returns a retriable failure immediately
// (the flush reports it), and folds terminal chunk failures into the flush's
// first-error semantics. backoffAttempt mirrors the batch loop's streak
// bookkeeping: a success ends the streak, a permanent HTTP response resets
// it.
func (c *Client) flushSpooledChunks(ctx context.Context, backoffAttempt *int) error {
	s := c.spool
	if s == nil {
		return nil
	}
	var firstErr error
	for {
		chunk, expired, persistFailed := s.pullResendChunk(c.cfg.BatchSize, c.clock.Now())
		c.recordSpoolExpired(expired)
		if persistFailed {
			c.recordSpoolPersistFailure()
		}
		c.drainSpoolCapacityDrops()
		if len(chunk) == 0 {
			return firstErr
		}
		if c.spoolResendHandoffSeam != nil {
			c.spoolResendHandoffSeam(chunk)
		}
		// The same handoff re-check as the automatic resend path: a
		// sentinel racing the pulled chunk must not publish withdrawn
		// facts through the explicit flush either.
		chunk = c.dropWithdrawnSpoolChunkMembers(chunk)
		if len(chunk) == 0 {
			continue
		}
		result, err := c.publishRequestResult(ctx, spoolChunkRequest(chunk), len(chunk))
		if err == nil {
			// Settle the delivered chunk off the spool BEFORE the callback:
			// a callback-driven consent flip must purge the remainder only.
			deferred := c.settleResentChunk(chunk, result)
			c.notifyBatchResult(result.toPublic())
			*backoffAttempt = 0
			if deferred {
				// Owed again and back on the queue: this flush stops
				// draining rather than re-publishing what it just
				// requeued, for the same reason the automatic pass does.
				return firstErr
			}
			continue
		}
		if errors.Is(err, ErrConsentDenied) {
			return err
		}
		if !isPermanentPublishError(err) {
			// A caller-abandoned flush learned nothing from the endpoint, so
			// it makes no persisted-pacing mutations either: the hintless
			// withdrawal below would wipe retry_after_until_ms and let the
			// next start resend inside the server's still-live window. The
			// requeued chunk keeps the previously persisted deadline.
			if !callerAbandonedFlush(ctx, err) {
				c.spoolStoreResendDeadline(err)
			}
			s.requeueResend(chunk)
			return err
		}
		removed, ackPersistFailed := s.ack(spoolEntryIDs(chunk))
		if len(removed) > 0 {
			c.notifySpoolDeadLetter(SpoolDropTerminal, removed)
		}
		if ackPersistFailed {
			c.recordSpoolPersistFailure()
		}
		var statusErr *HTTPStatusError
		if errors.As(err, &statusErr) {
			// A permanent HTTP response is still a response: the endpoint
			// answered, so the hint-less streak ends (same rationale as the
			// batch loop's swallowed-permanent reset).
			*backoffAttempt = 0
		}
		if firstErr == nil {
			firstErr = err
		}
	}
}

// spoolMaintain runs the flush-cadence spool upkeep: retry an owed wipe and
// a failed record rewrite.
func (c *Client) spoolMaintain() (capacityDropped int) {
	s := c.spool
	if s == nil {
		return 0
	}
	s.settleOwedWipe()
	if attempted, failed := s.retryPersist(); attempted && failed {
		c.recordSpoolPersistFailure()
	}
	return c.drainSpoolCapacityDrops()
}

// spoolCloseRemnant spools the undelivered remnant when the worker stops:
// the retained batch (usually already spooled at its failure — the append
// de-duplicates) plus whatever Close's flush left in the queue. Under a
// non-grant or owed-wipe state the remnant goes to the dead-letter callback
// instead (spoolFailedBatch's gate). Returns the gate-refused count, the
// remnant ids left in the mirror, the settled capacity evictions, the
// retry-age expiries, and the members that could not serialize (poisoned —
// already counted Dropped by settlePoisonedEvents) so the floor's close
// accounting can fold every teardown loss (see
// recordUnspooledCloseRemnant).
func (c *Client) spoolCloseRemnant(batch []Event) (gateRefused int, mirrored []string, capacityDropped, expiredDropped, poisonedDropped int) {
	if c.spool == nil {
		return 0, nil, 0, 0, 0
	}
	remnant := batch
	for {
		select {
		case event := <-c.queue.ch:
			remnant = append(remnant, event)
			continue
		default:
		}
		break
	}
	// The remnant spool is a transport handoff too (the spooled envelopes
	// resend at the next launch): a fact the real-subjects sentinel
	// withdrew must not ride it. Runs on the worker's stop path, so the
	// worker-owned filter applies as at any dispatch point — but that gate
	// alone only covers the batch the worker HELD: the members just
	// drained from the queue above never passed the worker's admission,
	// and with the seen mark already advanced past the sentinel's bump the
	// gate short-circuits without inspecting them. A pre-sentinel fact
	// surviving here would be REBUILT under the current request epoch,
	// bypassing spoolFailedBatch's epoch-mismatch re-filter and landing
	// its withdrawn subject-fact key durably in the spool — a crash before
	// the sentinel's spool sweep would resurrect it at the next launch. So
	// the handoff also checks PER MEMBER below (the admitReceivedEvent
	// idiom: the fact's own build stamp against the purge state), exactly
	// like the consent-epoch boundary.
	var remnantBackoff int
	remnant = c.dropWithdrawnExperimentFacts(remnant, &remnantBackoff)
	// The consent-epoch boundary applies at this handoff too: a denial that
	// bumped the epoch while the worker held its batch — with Close's final
	// flush abandoned before the worker's next boundary observation —
	// condemned those events, and spooling them would durably resend them
	// under a later grant. The intake stamp decides per member (the remnant
	// legitimately mixes the possibly-stale held batch with post-re-grant
	// queue events); dropped members count once, exactly like the boundary
	// drop they missed, and any drop invalidates the retained bytes (they
	// described the pre-filter held batch). The withdrawn-fact stamp check
	// rides the same pass, with the same accounting.
	currentConsentEpoch := c.consentEpoch.Load()
	keptRemnant := remnant[:0]
	boundaryDropped := 0
	for _, event := range remnant {
		if event.intakeConsentEpoch < currentConsentEpoch {
			boundaryDropped++
			continue
		}
		if c.isWithdrawnExperimentFactEvent(event) {
			boundaryDropped++
			continue
		}
		keptRemnant = append(keptRemnant, event)
	}
	remnant = keptRemnant
	if boundaryDropped > 0 {
		c.stats.dropped.Add(uint64(boundaryDropped))
		c.retainedRequest = batchRequest{}
	}
	if len(remnant) == 0 {
		return 0, nil, 0, 0, 0
	}
	// The retained batch's prefix reuses its retained wire bytes (the append
	// de-duplicates by event_id against bytes already spooled at the failure,
	// so a re-encode drifting under a mutated Props value must not happen
	// here either); the queue remainder builds fresh, and a member that no
	// longer serializes is dropped alone — the rest of the remnant still
	// spools instead of dying with it.
	request, _, poisoned := c.buildBatchIsolating(remnant, c.retainedRequest)
	c.settlePoisonedEvents(poisoned)
	if len(request.Events) == 0 {
		return 0, nil, 0, 0, len(poisoned)
	}
	gateRefused, mirrored, capacityDropped, expiredDropped = c.spoolFailedBatch(request, nil, false)
	return gateRefused, mirrored, capacityDropped, expiredDropped, len(poisoned)
}

// initSpool runs the construction-time spool lifecycle: settle an owed wipe
// from a previous run, then load under a persisted grant or purge under any
// other persisted state. Returns the dead-letters to emit once construction
// finishes (the callback must not run mid-construction).
func (c *Client) initSpool() []SpoolDeadLetter {
	s := c.spool
	s.settleOwedWipe()
	if s.owedWipe() {
		c.stats.setLastError("spool_purge_failed")
		c.logf("shardpilot spool: a spool wipe is still owed from a previous run; disk spool disabled until it succeeds")
		return nil
	}
	// The RESOLVED floor truth governs the spool decision, never
	// consent.json alone: a grant-tail reload whose record HEAL failed
	// leaves the live state granted — durable in-scope proof retained, the
	// heal registered as an owed record write — while the on-disk record is
	// still stale or absent. Purging on the record read alone would
	// dead-letter a spool the resolved grant plainly covers. So the load
	// path admits a durably-recorded grant OR a floor-RESOLVED grant;
	// grantPersisted (the write gate) opens only when the RECORD itself is
	// durably the grant — under an owed heal it stays closed until the
	// retried write lands (applySpoolConsent's success path reopens it).
	recordState, recordOK := loadConsentRecord(s.dir, s.actorDigest)
	recordIsGrant := recordOK && recordState == ConsentGranted
	floorResolvedGrant := c.consentFloorEnabled() && c.consent.Load() == consentStateGranted
	if recordIsGrant || floorResolvedGrant {
		// The persisted record may be trusted — loaded, resend-seeded,
		// rewritten — only through a directory whose privacy is established.
		// An existing SpoolDir that cannot be tightened to 0700 fails the
		// spool CLOSED instead: no load, no resend, matching the
		// refused-tighten write path's posture (would-spool batches keep
		// dead-lettering through the closed write gate, which requires a
		// successful record persist through this same tighten). The on-disk
		// records are left in place for a later run with the permissions
		// fixed.
		if err := ensurePrivateDir(s.dir, s.chmodFn); err != nil {
			c.stats.setLastError("spool_dir_private_failed")
			c.logf("shardpilot spool: the spool directory could not be made private (0700); the persisted record is not loaded and the disk spool is disabled: %v", err)
			return nil
		}
		// GRANT-ONLY under the RESOLVED floor: with the consent floor opted
		// in, initConsentFloor already resolved the live truth before this
		// runs — the receipt trail's tail can override a stale record (a
		// deny receipt durable while the record write was still owed), and
		// the identity contract can refuse the reload outright. A persisted
		// grant the resolved state does NOT confirm must not seed resend
		// work: the worker re-publishes loaded chunks, so trusting the
		// stale record here would transmit pre-denial events under an
		// operative denial (or transmit at all in a session that must stay
		// dark). The unconfirmed spool falls through to the purge below,
		// condemned exactly like a persisted denial.
		if !c.consentFloorEnabled() || floorResolvedGrant {
			if c.consentFloorEnabled() && recordIsGrant {
				// The floor confirmed the persisted grant as the LIVE state,
				// and the record on disk IS that persisted grant: the spool
				// write gate reopens now. Without this, every restart would
				// refuse retriable-failure appends and close remnants
				// (grantPersisted false) until a fresh SetConsent(true) —
				// dead-lettering events the reloaded grant plainly covers.
				// Under a FAILED heal (resolved grant, record still stale or
				// absent) the gate stays closed instead: writes reopen the
				// moment the owed record write lands. Floor-off keeps the
				// documented posture: live state is memory-only, the host
				// re-applies SetConsent at startup and the gate opens there.
				s.mu.Lock()
				s.grantPersisted = true
				s.mu.Unlock()
			}
			outcome := s.load(c.clock.Now())
			var letters []SpoolDeadLetter
			if len(outcome.condemned) > 0 {
				// The dark client's damaged-marker remedy (see load): the
				// experiment-fact class fails closed alone, terminal like
				// every sentinel withdrawal; the ordinary spool loaded and
				// serves.
				c.logf("shardpilot spool: dropped %d experiment fact(s) at load — the withdrawal marker is damaged and experiments are disabled, so the unknowable withdrawal fails closed within the experiment-fact class (the marker stays owed for a future experiments-enabled session; ordinary events are untouched)", len(outcome.condemned))
				letters = append(letters, spoolDeadLetterFrom(SpoolDropTerminal, outcome.condemned))
			}
			if len(outcome.expired) > 0 {
				c.stats.spoolExpired.Add(uint64(len(outcome.expired)))
				letters = append(letters, spoolDeadLetterFrom(SpoolDropExpired, outcome.expired))
			}
			if len(outcome.evicted) > 0 {
				c.stats.spoolEvicted.Add(uint64(len(outcome.evicted)))
				letters = append(letters, spoolDeadLetterFrom(SpoolDropCapacity, outcome.evicted))
			}
			// The init rewrite is a merging save too; drops it produced are
			// folded into the deferred init letters (the callback must not
			// run mid-construction).
			if drops := s.takeCapacityDrops(); len(drops) > 0 {
				c.stats.spoolEvicted.Add(uint64(len(drops)))
				letters = append(letters, spoolDeadLetterFrom(SpoolDropCapacity, drops))
			}
			if outcome.persistFailed {
				c.recordSpoolPersistFailure()
			}
			c.initialDeferUntil = outcome.deferUntil
			return letters
		}
	}
	// No persisted grant (absent, denied, or unreadable record) — or, under
	// the consent floor, a persisted grant the RESOLVED live state does not
	// confirm: the spool record is purged, unconditionally and idempotently.
	// What it held is reported as a consent drop — condemned whether or not
	// the file removal itself succeeds (a failure owes a wipe and fails
	// closed).
	dropped := s.readRecordEntries()
	if _, err := s.purge(); err != nil {
		c.stats.setLastError("spool_purge_failed")
		c.logf("shardpilot spool: purging the spool record at init failed; a wipe is owed and the disk spool is disabled until it succeeds: %v", err)
	}
	if len(dropped) == 0 {
		return nil
	}
	return []SpoolDeadLetter{spoolDeadLetterFrom(SpoolDropConsent, dropped)}
}

// applySpoolConsent applies a SetConsent decision to the disk side: persist
// the decision record and purge or open the spool. Called while holding the
// consent ticket turn (or, for an owed-record retry, the record-apply lock
// — see retryOwedConsentRecord) — NEVER lifecycleMu, which every
// Track/Enqueue takes: this function fsyncs files, and event intake must
// not wait out a disk stall. The ticket order keeps the disk order equal to
// the decision order across concurrent SetConsent calls; the in-memory
// state was already stored before this runs, so the spool's own gates
// (append's allowed re-check, owed-wipe) see the decided state under their
// own lock. Returns the dead-letters for the caller to emit after releasing
// the turn, and whether the decision RECORD is durably persisted (true with
// no spool at all — nothing to persist; under the consent floor a false
// return owes the record write, retried at every dispatch point). Denial:
// the spool is purged (a failed purge owes a wipe and fails closed) and the
// denied record is written regardless. Grant: an owed wipe is retried FIRST
// — while it still fails, the persisted decision stays denied and the live
// in-memory grant applies per the open posture — then the granted record is
// persisted, and only a SUCCESSFUL persist opens spool writes
// (grantPersisted): a grant whose record could not be written keeps disk
// closed, so a load on the next start can always trust the record it finds.
func (c *Client) applySpoolConsent(decision ConsentDecision, decidedAt string) ([]SpoolDeadLetter, bool) {
	s := c.spool
	if s == nil {
		return nil, true
	}
	floorAuthored := c.consentFloorEnabled()
	// Read the injectable file primitives ONCE, under the lock. Every
	// saveConsentRecord below runs with the spool lock released, so touching
	// the fields directly races a test swapping one to model a healing disk.
	renameFn, chmodFn := s.fileFuncs()
	if decision != ConsentDecisionGranted {
		// Both denial flavors run the full denial path; the persisted record
		// keeps the exact decision value (a forced-minor denial reloads as
		// its own state under the consent floor, and reads as "no usable
		// decision" — fail toward purging — on builds that predate it).
		s.mu.Lock()
		s.grantPersisted = false
		s.mu.Unlock()
		// The denied RECORD lands BEFORE the purge destroys anything: with a
		// durable prior grant on disk, purging first would open a crash
		// window where the spool is gone but no durable evidence of the
		// denial exists yet (record unwritten, receipt not yet appended) — a
		// relaunch would promote the stale granted record over a destroyed
		// spool. Record-first, every crash interleaving fails CLOSED: a
		// crash after the record restores denied (and the relaunch purges);
		// a crash before it changed nothing durable. When the record write
		// FAILS the purge is DEFERRED with it: the write gate is already
		// closed (grantPersisted false) and the live denial refuses intake,
		// so nothing flows meanwhile, and the owed-record retry re-runs this
		// branch — the purge completes in the same pass the record lands.
		recordPersisted := true
		var condemned []spoolEntry
		if recordErr := saveConsentRecord(s.dir, decision, s.actorDigest, decidedAt, floorAuthored, renameFn, chmodFn); recordErr != nil {
			recordPersisted = false
			c.stats.setLastError("consent_record_persist_failed")
			c.logf("shardpilot spool: persisting the denied consent record failed (the decision still applies in memory; the spool purge is deferred to the record retry and owed durably): %v", recordErr)
			// The deferred purge is a DEBT carried independently of the
			// owed-record slot: the slot holds only the NEWEST owed decision,
			// so a SUPERSEDING grant whose record write succeeds would clear
			// it — and with it, silently forget that this denial condemned
			// the spooled events, letting them resend under the new grant.
			// The MEMORY half of the purge runs NOW regardless: condemning
			// the held entries (mirror, resend queue, tombstones against a
			// merging save) is not disk-dependent, so the entries
			// dead-letter immediately and can never resend from memory.
			// Only the record-FILE removal is deferred, and the wipe-owed
			// marker carries that debt: the grant path settles an owed wipe
			// BEFORE its record can reopen the spool, the retried denial's
			// own purge clears the marker when it runs, and a crash
			// re-derives the debt at the next start (initSpool settles the
			// marker before anything loads). Events a denial condemned must
			// never resend, whatever decision follows.
			// The marker lands under the SAME s.mu hold that sets the owed
			// flag: releasing the lock first would let a concurrent settle
			// (spoolMaintain, the append gate, a superseding grant) consume
			// the flag and complete a FULL wipe inside the window — record
			// and marker unlinked, owed cleared — after which the late
			// marker create would land on disk while memory says nothing
			// is owed: a stale marker that would wipe the events a later
			// grant spools, at the next start. Serialized, a concurrent
			// settle observes either no debt yet (a no-op) or the flag AND
			// the marker together (a legitimate full wipe).
			s.mu.Lock()
			condemned = s.condemnEntriesLocked()
			s.owed = true
			markerErr := s.markerFn(s.dir)
			s.mu.Unlock()
			if markerErr != nil {
				// The debt marker could not be made durable either. The
				// INVARIANT this branch upholds: it returns only after
				// EITHER the denied record, the wipe-owed marker, or the
				// spool file's removal is durable — with none of them, a
				// crash leaves stale granted consent.json + spool.json and
				// NO owed marker, and the next launch reloads the condemned
				// events under the old grant (in the actorless local-only
				// path no deny receipt would ever exist to override it).
				// Escalate through the remaining durable outcomes in order.
				if retryErr := saveConsentRecord(s.dir, decision, s.actorDigest, decidedAt, floorAuthored, renameFn, chmodFn); retryErr == nil {
					// The denied RECORD is durable after all: record-first
					// is restored, and every crash interleaving from here
					// fails closed (a relaunch restores denied and purges at
					// init). Fall through to the normal purge below — its
					// own failure modes are covered by the durable record.
					recordPersisted = true
					c.logf("shardpilot spool: the wipe-owed marker could not be created (%v); the retried denied record write succeeded and rules any reload", markerErr)
				} else if s.settleOwedWipe() {
					// The condemned spool FILE itself is gone (the settle
					// consumed the in-memory debt with it): nothing
					// condemned can reload, whatever the stale granted
					// record says — the debt is settled by destruction. The
					// denied record stays owed and retries at every
					// dispatch point.
					c.logf("shardpilot spool: the wipe-owed marker could not be created (%v); the condemned spool file was removed instead — nothing condemned can reload", markerErr)
					if len(condemned) == 0 {
						return nil, false
					}
					return []SpoolDeadLetter{spoolDeadLetterFrom(SpoolDropConsent, condemned)}, false
				} else {
					// NOTHING durable could be made — surface it. The live
					// denial's in-memory condemnation holds for this
					// process (intake refused, mirror and resend queue
					// cleared, disk work refused via the in-memory owed
					// flag), and the owed-record retry re-runs this whole
					// branch at every dispatch point — re-deriving the debt
					// until one of the three outcomes lands. A crash
					// meanwhile is the surfaced, diagnosed residual.
					c.stats.setLastError("spool_purge_failed")
					c.logf("shardpilot spool: neither the denied record, the wipe-owed marker, nor the spool removal could be made durable (marker: %v; record retry: %v); the denial holds in memory and the debt re-derives at every dispatch point", markerErr, retryErr)
					if len(condemned) == 0 {
						return nil, false
					}
					return []SpoolDeadLetter{spoolDeadLetterFrom(SpoolDropConsent, condemned)}, false
				}
			} else {
				if len(condemned) == 0 {
					return nil, false
				}
				return []SpoolDeadLetter{spoolDeadLetterFrom(SpoolDropConsent, condemned)}, false
			}
		}
		dropped, err := s.purge()
		if err != nil {
			c.stats.setLastError("spool_purge_failed")
			c.logf("shardpilot spool: consent purge failed; a wipe is owed and the disk spool is disabled until it succeeds: %v", err)
		}
		condemned = append(condemned, dropped...)
		if len(condemned) == 0 {
			return nil, recordPersisted
		}
		return []SpoolDeadLetter{spoolDeadLetterFrom(SpoolDropConsent, condemned)}, recordPersisted
	}
	if !s.settleOwedWipe() {
		c.stats.setLastError("spool_purge_failed")
		c.logf("shardpilot spool: a spool wipe is still owed; the persisted consent decision stays denied and the disk spool stays disabled until the wipe succeeds")
		return nil, false
	}
	if err := saveConsentRecord(s.dir, ConsentDecisionGranted, s.actorDigest, decidedAt, floorAuthored, renameFn, chmodFn); err != nil {
		s.mu.Lock()
		s.grantPersisted = false
		s.mu.Unlock()
		c.stats.setLastError("consent_record_persist_failed")
		c.logf("shardpilot spool: persisting the granted consent record failed; the live grant applies but the disk spool stays closed until a persist succeeds: %v", err)
		return nil, false
	}
	s.mu.Lock()
	s.grantPersisted = true
	s.mu.Unlock()
	return nil, true
}
