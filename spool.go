package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
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
	// spooled events.
	SpoolDropTerminal SpoolDropReason = "terminal"
	// SpoolDropConsent: a consent purge dropped the record, or a
	// would-have-spooled batch was refused disk under a non-grant or
	// owed-wipe state.
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
	spoolRecordVersion = 1

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
)

// spoolRecordWire is the spool.json payload. Events hold the exact
// wire-serialized envelope bytes; retry_after_until_ms carries a live server
// Retry-After deadline captured when a batch was spooled under one.
type spoolRecordWire struct {
	Version           int               `json:"version"`
	Events            []json.RawMessage `json:"events"`
	RetryAfterUntilMS int64             `json:"retry_after_until_ms,omitempty"`
}

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

	// removeFn/renameFn are the file primitives, injectable so tests can
	// exercise purge and persist failures deterministically (the same seam
	// discipline as createAnonymousIDWith).
	removeFn func(path string) error
	renameFn func(oldpath, newpath string) error
}

// spoolSettledMemory bounds the settled-id memory used to suppress
// merge-resurrection. Past it the oldest suppression is forgotten: a very
// old id could then be resurrected by a stale sibling write, which degrades
// to at-least-once delivery (the server de-dupes by event_id).
const spoolSettledMemory = 4096

func newDiskSpool(cfg Config) *diskSpool {
	s := &diskSpool{
		dir:          cfg.SpoolDir,
		maxEvents:    cfg.SpoolMaxEvents,
		maxBytes:     cfg.SpoolMaxBytes,
		ids:          make(map[string]struct{}),
		settledIDs:   make(map[string]struct{}),
		uncountedIDs: make(map[string]struct{}),
		actorDigest:  consentActorDigest(cfg),
		removeFn:     os.Remove,
		renameFn:     os.Rename,
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
func (s *diskSpool) saveLocked() error {
	merged := make([]spoolEntry, 0, len(s.entries))
	seen := make(map[string]struct{}, len(s.entries))
	foreign := 0
	for _, entry := range s.readRecordEntriesLocked() {
		if entry.id == "" {
			continue
		}
		if _, settled := s.settledIDs[entry.id]; settled {
			continue
		}
		if _, dup := seen[entry.id]; dup {
			continue
		}
		seen[entry.id] = struct{}{}
		if _, ours := s.ids[entry.id]; !ours {
			foreign++
		}
		merged = append(merged, entry)
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
	for len(merged) > 0 && (len(merged) > s.maxEvents || mergedBytes > s.maxBytes) {
		dropped := merged[0]
		mergedBytes -= len(dropped.raw)
		merged = merged[1:]
		s.recordSettledLocked(dropped.id)
		if _, ours := s.ids[dropped.id]; ours {
			// This process still claimed the entry: the mirror follows the
			// written record, and the drop surfaces through the standard
			// capacity dead-letter (drained by the client layer after the
			// current operation).
			if capDropped == nil {
				capDropped = make(map[string]struct{})
			}
			capDropped[dropped.id] = struct{}{}
			delete(s.ids, dropped.id)
			s.totalBytes -= len(dropped.raw)
			s.pendingCapacityDrops = append(s.pendingCapacityDrops, dropped)
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
	record := spoolRecordWire{
		Version:           spoolRecordVersion,
		Events:            make([]json.RawMessage, 0, len(merged)),
		RetryAfterUntilMS: s.retryAfterUntilMS,
	}
	for _, entry := range merged {
		record.Events = append(record.Events, entry.raw)
	}
	payload, err := json.Marshal(record)
	if err == nil {
		err = writePrivateFileAtomic(s.filePath(), payload, s.renameFn)
	}
	s.dirty = err != nil
	if err == nil && len(s.uncountedIDs) > 0 {
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
	return err
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
	if s.removeRecordFile() != nil {
		return false
	}
	s.owed = false
	// Best-effort: a marker that cannot be removed re-enters owed on the
	// next start and re-runs this (idempotent) wipe.
	_ = removeWipeOwedMarker(s.dir)
	return true
}

// purge condemns the whole spool: the mirror and resend queue are cleared
// unconditionally (the entries are returned for dead-lettering — they are
// dropped from delivery either way), and the record file is removed. A
// failed file removal owes a wipe: the durable marker is created
// (best-effort; an in-memory owed flag fails closed regardless) and the
// spool refuses all disk work until a later wipe succeeds.
func (s *diskSpool) purge() (dropped []spoolEntry, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dropped = s.entries
	for _, entry := range dropped {
		// Condemned data must not be resurrected by a later merging save.
		s.recordSettledLocked(entry.id)
	}
	s.entries = nil
	s.ids = make(map[string]struct{})
	s.uncountedIDs = make(map[string]struct{})
	s.totalBytes = 0
	s.resend = nil
	s.retryAfterUntilMS = 0
	s.dirty = false
	if err = s.removeRecordFile(); err != nil {
		s.owed = true
		_ = createWipeOwedMarker(s.dir)
		return dropped, err
	}
	if s.owed {
		s.owed = false
		_ = removeWipeOwedMarker(s.dir)
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

func (s *diskSpool) readRecordEntriesLocked() []spoolEntry {
	data, err := os.ReadFile(s.filePath())
	if err != nil {
		return nil
	}
	var record spoolRecordWire
	if json.Unmarshal(data, &record) != nil {
		return nil
	}
	entries := make([]spoolEntry, 0, len(record.Events))
	for _, raw := range record.Events {
		var envelope spoolEnvelopeWire
		_ = json.Unmarshal(raw, &envelope)
		entries = append(entries, spoolEntry{id: envelope.EventID, ts: envelope.EventTS, raw: raw})
	}
	return entries
}

// spoolLoadOutcome is what a startup load produced: the drops to report,
// the re-seeded deferral (zero when none), and whether the init rewrite
// failed.
type spoolLoadOutcome struct {
	expired       []spoolEntry
	evicted       []spoolEntry
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
	data, err := os.ReadFile(s.filePath())
	if err != nil {
		return outcome
	}
	var record spoolRecordWire
	if json.Unmarshal(data, &record) != nil || record.Version != spoolRecordVersion {
		_ = s.removeRecordFile()
		return outcome
	}
	for _, raw := range record.Events {
		var envelope spoolEnvelopeWire
		_ = json.Unmarshal(raw, &envelope)
		entry := spoolEntry{id: envelope.EventID, ts: envelope.EventTS, raw: raw}
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
		s.retryAfterUntilMS = record.RetryAfterUntilMS
	}
	s.resend = append([]spoolEntry(nil), s.entries...)
	outcome.persistFailed = s.saveLocked() != nil
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
// this batch was spooled under (the server's latest word replaces an earlier
// stored one). An append that changes nothing (everything expired or
// duplicate, no deadline) skips the rewrite entirely.
func (s *diskSpool) append(batch []spoolEntry, deadlineMS int64, now time.Time, allowed func() bool) (refused bool, added, expired, evicted []spoolEntry, persistFailed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owed || !allowed() {
		return true, nil, nil, nil, false
	}
	appended := make(map[string]struct{}, len(batch))
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
	}
	if len(appended) == 0 && deadlineMS <= 0 {
		return false, nil, expired, nil, false
	}
	evicted = s.evictOverCapsLocked()
	for _, entry := range batch {
		if _, wasAppended := appended[entry.id]; !wasAppended {
			continue
		}
		if _, survived := s.ids[entry.id]; survived {
			added = append(added, entry)
		}
	}
	if deadlineMS > 0 {
		s.retryAfterUntilMS = deadlineMS
	}
	persistFailed = s.saveLocked() != nil
	if persistFailed {
		// Accepted into the mirror but not durable: deliberately uncounted
		// until a later save lands (see uncountedIDs).
		for _, entry := range added {
			s.uncountedIDs[entry.id] = struct{}{}
		}
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
		entries = append(entries, spoolEntry{id: envelope.EventID, ts: envelope.EventTS, raw: request.rawEvents[i]})
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
		envelopes[i] = entry.raw
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
func (c *Client) drainSpoolCapacityDrops() {
	if c.spool == nil {
		return
	}
	// Fold in entries a successful save just made durable after an earlier
	// failed write left them uncounted.
	if durable := c.spool.takeBecameDurable(); durable > 0 {
		c.stats.spooled.Add(uint64(durable))
	}
	drops := c.spool.takeCapacityDrops()
	if len(drops) == 0 {
		return
	}
	c.stats.spoolEvicted.Add(uint64(len(drops)))
	c.notifySpoolDeadLetter(SpoolDropCapacity, drops)
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
		entry := spoolEntry{id: envelope.EventID, ts: envelope.EventTS, raw: request.rawEvents[i]}
		// buildEnvelope resolved the effective actor (per-event override,
		// else the configured default), so equality against the configured
		// tuple is exactly "the persisted grant covers this envelope".
		if envelope.UserID == c.cfg.UserID && envelope.AnonymousID == c.cfg.AnonymousID {
			eligible = append(eligible, entry)
		} else {
			refused = append(refused, entry)
		}
	}
	return eligible, refused
}

// spoolFailedBatch appends a retriably failed worker batch to the spool.
// Under a non-grant live state, an unpersisted grant record, or an owed
// wipe, nothing touches disk and the would-have-spooled batch goes to the
// dead-letter callback instead. Eligibility is per-envelope: only envelopes
// whose effective actor the persisted grant covers may spool; the rest
// dead-letter as consent drops.
func (c *Client) spoolFailedBatch(request batchRequest, cause error) {
	s := c.spool
	if s == nil {
		return
	}
	eligible, refusedActors := c.partitionSpoolEligible(request)
	c.notifySpoolDeadLetter(SpoolDropConsent, refusedActors)
	if len(eligible) == 0 {
		return
	}
	// An owed wipe is retried before any append; still owed refuses disk.
	s.settleOwedWipe()
	refused, added, expired, evicted, persistFailed := s.append(eligible, c.spoolDeadlineFromError(cause), c.clock.Now(), func() bool {
		return c.consent.Load() == consentStateGranted && s.grantPersisted
	})
	if refused {
		c.notifySpoolDeadLetter(SpoolDropConsent, eligible)
		return
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
	c.drainSpoolCapacityDrops()
}

// spoolAck settles a delivered batch: its events, if spooled, are removed
// (no dead-letter — they were delivered).
func (c *Client) spoolAck(request batchRequest) {
	s := c.spool
	if s == nil {
		return
	}
	entries := spoolEntriesFromRequest(request)
	if len(entries) == 0 {
		return
	}
	if _, persistFailed := s.ack(spoolEntryIDs(entries)); persistFailed {
		c.recordSpoolPersistFailure()
	}
	c.drainSpoolCapacityDrops()
}

// spoolSettleTerminal settles a terminally failed batch: previously spooled
// events are removed so a poison batch cannot re-fail every launch, and
// exactly those removed are dead-lettered as terminal.
func (c *Client) spoolSettleTerminal(request batchRequest) {
	s := c.spool
	if s == nil {
		return
	}
	entries := spoolEntriesFromRequest(request)
	if len(entries) == 0 {
		return
	}
	removed, persistFailed := s.ack(spoolEntryIDs(entries))
	if len(removed) > 0 {
		c.notifySpoolDeadLetter(SpoolDropTerminal, removed)
	}
	if persistFailed {
		c.recordSpoolPersistFailure()
	}
	c.drainSpoolCapacityDrops()
}

// settleResentChunk settles a successfully re-published spool chunk.
func (c *Client) settleResentChunk(chunk []spoolEntry) {
	c.stats.spoolResent.Add(uint64(len(chunk)))
	if _, persistFailed := c.spool.ack(spoolEntryIDs(chunk)); persistFailed {
		c.recordSpoolPersistFailure()
	}
	c.drainSpoolCapacityDrops()
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
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.HTTPTimeout)
		err := c.publishRequest(ctx, spoolChunkRequest(chunk), len(chunk))
		cancel()
		c.applyRetryPacing(err, deferUntil, backoffAttempt)
		if err == nil {
			c.settleResentChunk(chunk)
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
// in-memory retry still honors the remaining window at the next start.
func (c *Client) spoolStoreResendDeadline(err error) {
	deadlineMS := c.spoolDeadlineFromError(err)
	if deadlineMS <= 0 {
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
		err := c.publishRequest(ctx, spoolChunkRequest(chunk), len(chunk))
		if err == nil {
			c.settleResentChunk(chunk)
			*backoffAttempt = 0
			continue
		}
		if errors.Is(err, ErrConsentDenied) {
			return err
		}
		if !isPermanentPublishError(err) {
			c.spoolStoreResendDeadline(err)
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
func (c *Client) spoolMaintain() {
	s := c.spool
	if s == nil {
		return
	}
	s.settleOwedWipe()
	if attempted, failed := s.retryPersist(); attempted && failed {
		c.recordSpoolPersistFailure()
	}
	c.drainSpoolCapacityDrops()
}

// spoolCloseRemnant spools the undelivered remnant when the worker stops:
// the retained batch (usually already spooled at its failure — the append
// de-duplicates) plus whatever Close's flush left in the queue. Under a
// non-grant or owed-wipe state the remnant goes to the dead-letter callback
// instead (spoolFailedBatch's gate).
func (c *Client) spoolCloseRemnant(batch []Event) {
	if c.spool == nil {
		return
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
	if len(remnant) == 0 {
		return
	}
	request, err := c.buildBatch(remnant)
	if err != nil {
		c.logf("shardpilot spool: close remnant could not be serialized and was dropped: %v", err)
		return
	}
	c.spoolFailedBatch(request, nil)
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
	if state, ok := loadConsentRecord(s.dir, s.actorDigest); ok && state == ConsentGranted {
		outcome := s.load(c.clock.Now())
		var letters []SpoolDeadLetter
		if len(outcome.expired) > 0 {
			c.stats.spoolExpired.Add(uint64(len(outcome.expired)))
			letters = append(letters, spoolDeadLetterFrom(SpoolDropExpired, outcome.expired))
		}
		if len(outcome.evicted) > 0 {
			c.stats.spoolEvicted.Add(uint64(len(outcome.evicted)))
			letters = append(letters, spoolDeadLetterFrom(SpoolDropCapacity, outcome.evicted))
		}
		// The init rewrite is a merging save too; drops it produced are
		// folded into the deferred init letters (the callback must not run
		// mid-construction).
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
	// No persisted grant (absent, denied, or unreadable record): the spool
	// record is purged, unconditionally and idempotently. What it held is
	// reported as a consent drop — condemned whether or not the file removal
	// itself succeeds (a failure owes a wipe and fails closed).
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

// applySpoolConsentLocked applies a SetConsent decision to the disk side:
// persist the decision record and purge or open the spool. Called with
// lifecycleMu held; returns the dead-letters for the caller to emit after
// unlocking. Denial: the spool is purged (a failed purge owes a wipe and
// fails closed) and the denied record is written regardless. Grant: an owed
// wipe is retried FIRST — while it still fails, the persisted decision
// stays denied and the live in-memory grant applies per the open posture —
// then the granted record is persisted, and only a SUCCESSFUL persist opens
// spool writes (grantPersisted): a grant whose record could not be written
// keeps disk closed, so a load on the next start can always trust the
// record it finds.
func (c *Client) applySpoolConsentLocked(analyticsGranted bool) []SpoolDeadLetter {
	s := c.spool
	if s == nil {
		return nil
	}
	if !analyticsGranted {
		s.mu.Lock()
		s.grantPersisted = false
		s.mu.Unlock()
		dropped, err := s.purge()
		if err != nil {
			c.stats.setLastError("spool_purge_failed")
			c.logf("shardpilot spool: consent purge failed; a wipe is owed and the disk spool is disabled until it succeeds: %v", err)
		}
		if recordErr := saveConsentRecord(s.dir, false, s.actorDigest, s.renameFn); recordErr != nil {
			c.stats.setLastError("consent_record_persist_failed")
			c.logf("shardpilot spool: persisting the denied consent record failed (the decision still applies in memory): %v", recordErr)
		}
		if len(dropped) == 0 {
			return nil
		}
		return []SpoolDeadLetter{spoolDeadLetterFrom(SpoolDropConsent, dropped)}
	}
	if !s.settleOwedWipe() {
		c.stats.setLastError("spool_purge_failed")
		c.logf("shardpilot spool: a spool wipe is still owed; the persisted consent decision stays denied and the disk spool stays disabled until the wipe succeeds")
		return nil
	}
	if err := saveConsentRecord(s.dir, true, s.actorDigest, s.renameFn); err != nil {
		s.mu.Lock()
		s.grantPersisted = false
		s.mu.Unlock()
		c.stats.setLastError("consent_record_persist_failed")
		c.logf("shardpilot spool: persisting the granted consent record failed; the live grant applies but the disk spool stays closed until a persist succeeds: %v", err)
		return nil
	}
	s.mu.Lock()
	s.grantPersisted = true
	s.mu.Unlock()
	return nil
}
