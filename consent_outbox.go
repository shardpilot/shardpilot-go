package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
)

// Durable consent-receipt outbox for the opt-in consent floor
// (Config.ConsentFloor): every explicit consent decision mints exactly one
// receipt, retained here until the ingest service acknowledges it (any 2xx)
// or a terminal outcome drops it. The outbox is CONSENT-PLANE ONLY — it
// never carries event envelopes, is never consent-purged, and delivery is
// permitted (required) while analytics consent is denied or unknown: a
// receipt documents the decision itself, which is its legal purpose. The
// server de-duplicates re-sends on idempotency_key, so an ack lost to a
// crash re-sends harmlessly at the next launch.
//
// Bounds: 32 receipts, FIFO, evicting OLDEST first on save — the newest
// decisions are the operative ones — and deliberately no TTL: an
// undelivered receipt retries until acknowledged. A FAILED durable write
// never evicts and never partially succeeds: the save fails whole, the
// in-memory mirror stays authoritative, and the write is owed (retried at
// every dispatch point and at Close). Identifiers are bounded at
// maxConsentIdentifierBytes; the sanitizer applies the same rules on load
// AND save, dropping malformed or oversized entries fail-safe.
//
// Delivery is strictly serial and in decision order: at most one receipt in
// flight, so a grant-then-deny can never settle deny-then-grant. Dispatch
// points: client construction, every SetConsent/SetConsentDecision (via the
// worker wake), every worker cycle and explicit Flush, and Close's final
// drain; an acknowledgement immediately chains the next retained receipt.
// Retryable outcomes (transport failure, 429, any 5xx) keep the receipt at
// the head and park the CONSENT plane — independent of the events plane's
// pacing — behind the server's Retry-After (parsed on 429 AND 5xx) or the
// client-side jittered backoff otherwise. Everything else is terminal: the
// receipt drops (diagnosed through the log and Stats.LastConsentError) so
// receipts queued behind it still deliver. This SDK's bearer is static for
// the client's lifetime — there is no re-mint seam — so a 401 is terminal
// here exactly like the engine SDKs' static-key (Mode A) rule.

const (
	consentOutboxFileName      = "consent-outbox.json"
	consentOutboxRecordVersion = 1

	// maxConsentOutboxEntries is the cross-SDK canonical outbox cap.
	maxConsentOutboxEntries = 32

	// maxConsentIdentifierBytes bounds host-supplied identifiers on the
	// receipt path, in BYTES (cross-SDK contract, GAP-075): receipts persist
	// identifiers verbatim and the outbox has no byte budget of its own, so
	// the identifier clamp is what keeps the 32-receipt worst case bounded.
	// Oversized identifiers are REJECTED, never truncated — truncation could
	// collide distinct identities and mis-attribute consent decisions.
	maxConsentIdentifierBytes = 512

	// consentOutboxReadLimit bounds how much of consent-outbox.json is ever
	// read back. The 32-receipt worst case at the identifier clamp is under
	// 50 KiB; 256 KiB is generous by a factor of five, and a larger file is
	// not a record this SDK could have written — treated as corrupt without
	// ever being loaded whole, mirroring the spool's bounded record read.
	consentOutboxReadLimit = 256 << 10
)

// validConsentIdentifier reports whether a host-supplied identifier may ride
// a consent receipt: non-empty and within the byte clamp.
func validConsentIdentifier(identifier string) bool {
	return identifier != "" && len(identifier) <= maxConsentIdentifierBytes
}

// consentReceipt is one stored outbox entry — the canonical receipt fields,
// minted ONCE at decision time and re-sent verbatim (same idempotency key,
// same decided_at, never re-stamped). AnonymousID is retention-only
// metadata and NEVER rides the wire body. Categories.Analytics is a
// POINTER deliberately: a malformed or legacy entry with the category
// absent must be distinguishable from an explicit denial — JSON's zero
// value for a plain bool would silently turn "field missing" into
// "analytics: false", and a resend of that fabricated denial could
// overwrite a previously granted actor server-side. The sanitizer drops
// absent-category entries as malformed instead.
type consentReceipt struct {
	IdempotencyKey  string `json:"idempotency_key"`
	WorkspaceID     string `json:"workspace_id"`
	AppID           string `json:"app_id"`
	EnvironmentID   string `json:"environment_id"`
	ActorIdentifier string `json:"actor_identifier"`
	Categories      struct {
		Analytics *bool `json:"analytics"`
	} `json:"categories"`
	DecidedAt   string `json:"decided_at"`
	Reason      string `json:"reason,omitempty"`
	AnonymousID string `json:"anonymous_id,omitempty"`
}

// analyticsGranted reads the receipt's analytics category, false when the
// category is absent (only sanitized receipts — where it is always present
// — are ever dispatched or gated on).
func (r consentReceipt) analyticsGranted() bool {
	return r.Categories.Analytics != nil && *r.Categories.Analytics
}

// consentOutboxWire is the consent-outbox.json payload.
type consentOutboxWire struct {
	Version  int              `json:"version"`
	Receipts []consentReceipt `json:"receipts"`
}

// sanitizeConsentReceipt validates one stored entry and copies it down to
// exactly the known fields. An entry survives only with every required
// field a non-empty string, the actor identifier within the byte clamp,
// the analytics category PRESENT (an absent category is a malformed entry,
// never an implicit denial — see the type comment), and the optional
// fields absent or valid; anything else — a truncated entry, a garbled
// field, an oversized legacy identifier — is dropped fail-safe: never
// sent, never a crash, never blocking the deliverable rest.
func sanitizeConsentReceipt(entry consentReceipt) (consentReceipt, bool) {
	if entry.IdempotencyKey == "" || entry.WorkspaceID == "" || entry.AppID == "" ||
		entry.EnvironmentID == "" || entry.DecidedAt == "" {
		return consentReceipt{}, false
	}
	if entry.Categories.Analytics == nil {
		return consentReceipt{}, false
	}
	if !validConsentIdentifier(entry.ActorIdentifier) {
		return consentReceipt{}, false
	}
	if entry.AnonymousID != "" && !validConsentIdentifier(entry.AnonymousID) {
		return consentReceipt{}, false
	}
	switch entry.Reason {
	case "":
	case consentDecisionReason:
		// The only reason this SDK ever mints, and only on DENIALS: a
		// "granted" receipt claiming a forced-minor denial reason is a
		// contradiction no decision path can produce — corrupt data,
		// dropped fail-safe rather than re-sent as a self-contradictory
		// consent statement.
		if *entry.Categories.Analytics {
			return consentReceipt{}, false
		}
	default:
		// An unknown reason value is not a receipt this SDK could have
		// written; dropped fail-safe like every unknown field shape.
		return consentReceipt{}, false
	}
	if _, err := time.Parse(time.RFC3339Nano, entry.DecidedAt); err != nil {
		// Every SDK-minted receipt carries a parseable stamp: a malformed
		// decided_at is corrupt data, dropped fail-safe — persisted, it
		// could otherwise become reload truth (the tail-pick accepts an
		// in-scope receipt unconditionally when no usable record exists,
		// and only parses the stamp when comparing against one).
		return consentReceipt{}, false
	}
	sanitized := consentReceipt{
		IdempotencyKey:  entry.IdempotencyKey,
		WorkspaceID:     entry.WorkspaceID,
		AppID:           entry.AppID,
		EnvironmentID:   entry.EnvironmentID,
		ActorIdentifier: entry.ActorIdentifier,
		DecidedAt:       entry.DecidedAt,
		Reason:          entry.Reason,
		AnonymousID:     entry.AnonymousID,
	}
	// A fresh pointer, never an alias into the loaded entry: the copy-down
	// must own every byte it keeps.
	analytics := *entry.Categories.Analytics
	sanitized.Categories.Analytics = &analytics
	return sanitized, true
}

// consentOutbox is the outbox state machine: the in-memory mirror (oldest
// first, authoritative — a failed rewrite keeps the mirror and retries), the
// consent plane's deferral state, and the serial-dispatch claim. All fields
// are guarded by the client's consent ticket line plus mu; methods never
// run callbacks or network IO under mu.
type consentOutbox struct {
	// mu guards every field below. It is never held across network IO (the
	// dispatch claim serializes that); saves hold it by design, mirroring
	// the spool's mirror/disk coherence.
	mu sync.Mutex

	dir      string
	receipts []consentReceipt

	// dirty marks an owed durable write: the mirror is authoritative and
	// the write retries at every dispatch point and at Close.
	dirty bool

	// evictedSinceSave counts receipts the LAST save evicted for the cap,
	// drained by the client layer into Stats.ConsentOutboxEvicted.
	evictedSinceSave int

	// dispatching is the serial-dispatch claim: at most one dispatch pass
	// runs at a time, so at most one receipt is ever in flight and decision
	// order is preserved on the wire. dispatchWaiters are caller-driven
	// drains JOINING behind an in-flight pass (see claimDispatchWait):
	// released (closed) when the claim frees, so a Flush/Close drain waits
	// its turn instead of silently skipping receipt work.
	dispatching     bool
	dispatchWaiters []chan struct{}

	// deferUntil parks the consent plane after a retryable delivery failure
	// (server Retry-After, or jittered backoff); backoffAttempt counts the
	// consecutive-failure streak (reset by an acknowledgement). Independent
	// of the events plane's pacing.
	deferUntil     time.Time
	backoffAttempt int

	// settledKeys remembers (bounded, FIFO) the idempotency keys THIS process
	// removed from the outbox — acknowledged, terminally dropped, or
	// cap-evicted — so the reload-and-merge save can never resurrect them
	// from a stale on-disk copy written by another client sharing the
	// directory (the same suppression discipline as diskSpool.settledIDs).
	settledKeys map[string]struct{}
	settledFIFO []string

	// ownKeys marks the receipts THIS process appended (its own minted
	// decisions). The merging save writes exactly: the fresh DISK view minus
	// settled keys, plus own unsaved appends — a stale foreign copy in this
	// mirror never flows back to the file, so a receipt its owner pruned
	// concurrently stays pruned (the same no-foreign-writeback rule as the
	// disk spool's mirror). Loaded entries are deliberately NOT own: the
	// disk view refreshes them every save.
	ownKeys map[string]struct{}

	// recordOwedKeys marks retained receipts whose DECISION-RECORD write is
	// still owed — the receipt's durable PAIR is incomplete. Tracked PER
	// RECEIPT, not only in the client's single owed slot: the slot always
	// holds the NEWEST owed decision, so a newer denial's failed record write
	// would otherwise erase the fact that an earlier grant's record never
	// landed — and the unmarked grant would dispatch and prune while the deny
	// proof is held, making a grant the server's last word against a local
	// denial. A marked receipt never dispatches; a SUCCESSFUL record write
	// (always for the newest decision) clears every mark — the on-disk record
	// is then at least as new as every marked receipt's decision, which
	// settles each pair's crash story. In-memory only: after a crash the
	// reload re-derives the owed state from record-vs-trail and re-marks.
	recordOwedKeys map[string]struct{}

	// renameFn/chmodFn are the file primitives, injectable so tests can
	// exercise failed-write-never-evicts deterministically (the same seam
	// discipline as diskSpool).
	renameFn func(oldpath, newpath string) error
	chmodFn  func(name string, mode os.FileMode) error
}

func newConsentOutbox(dir string) *consentOutbox {
	return &consentOutbox{
		dir:            dir,
		settledKeys:    make(map[string]struct{}),
		ownKeys:        make(map[string]struct{}),
		recordOwedKeys: make(map[string]struct{}),
		renameFn:       os.Rename,
		chmodFn:        os.Chmod,
	}
}

// consentSettledMemory bounds the settled-key memory used to suppress
// merge-resurrection (see settledKeys). Decisions are rare — the bound is
// generous — and past it the oldest suppression is forgotten: a very old key
// could then be resurrected by a stale sibling write, which degrades to
// at-least-once delivery (the server de-duplicates by idempotency key).
const consentSettledMemory = 256

// recordSettledLocked remembers a key this process removed from the outbox
// so a merging save does not resurrect it (bounded FIFO). Must be called
// with mu held.
func (o *consentOutbox) recordSettledLocked(key string) {
	if key == "" {
		return
	}
	if _, exists := o.settledKeys[key]; exists {
		return
	}
	o.settledKeys[key] = struct{}{}
	o.settledFIFO = append(o.settledFIFO, key)
	if len(o.settledFIFO) > consentSettledMemory {
		delete(o.settledKeys, o.settledFIFO[0])
		o.settledFIFO = o.settledFIFO[1:]
	}
}

// durable reports whether the outbox has a durable backend at all (a
// configured SpoolDir). Without one, receipts live in memory only and
// Close's ErrConsentPending branch applies while any remain.
func (o *consentOutbox) durable() bool {
	return o.dir != ""
}

func (o *consentOutbox) filePath() string {
	return filepath.Join(o.dir, consentOutboxFileName)
}

// readRecordReceipts best-effort parses the current on-disk record WITHOUT
// adopting it: bounded read, sanitized entries in file order. Used by load
// at construction and by the merging save to see a sibling writer's view
// (it touches no guarded state, so it is safe with or without mu held). A
// file that is missing, unreadable, over the read limit, unparseable, or of
// an unknown version reads as EMPTY — corrupt state is a clean start, never
// a crash into the host — and a wholly garbled record is simply overwritten
// by the next save.
func (o *consentOutbox) readRecordReceipts() []consentReceipt {
	if !o.durable() {
		return nil
	}
	file, err := os.Open(o.filePath())
	if err != nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(file, consentOutboxReadLimit+1))
	_ = file.Close()
	if err != nil || len(data) > consentOutboxReadLimit {
		return nil
	}
	var record consentOutboxWire
	if json.Unmarshal(data, &record) != nil || record.Version != consentOutboxRecordVersion {
		return nil
	}
	loaded := make([]consentReceipt, 0, len(record.Receipts))
	seen := make(map[string]struct{}, len(record.Receipts))
	for _, entry := range record.Receipts {
		sanitized, ok := sanitizeConsentReceipt(entry)
		if !ok {
			continue
		}
		if _, dup := seen[sanitized.IdempotencyKey]; dup {
			// Duplicate idempotency keys never come from this SDK (keys are
			// minted once and the merging save de-duplicates); a corrupt or
			// hand-edited record can carry them, possibly with DIFFERENT
			// decision bodies. Keep the FIRST occurrence — the
			// server-consistent choice: the ingest service de-duplicates by
			// idempotency key and honors the first body it saw, answering
			// replays with `replayed`, so a later conflicting body under the
			// same key could never take effect server-side anyway.
			continue
		}
		seen[sanitized.IdempotencyKey] = struct{}{}
		loaded = append(loaded, sanitized)
	}
	return loaded
}

// load reads the durable record into the mirror at construction: sanitized,
// capped, oldest first. Entries matching own (nil: every entry) are adopted
// as THIS process's — its trail from a previous run, which its merging
// saves keep writing back even when a disk re-read transiently fails —
// while the rest stay foreign: refreshed from the fresh disk view on every
// save, never written back from this mirror, so their owner's concurrent
// prune is honored.
func (o *consentOutbox) load(own func(consentReceipt) bool) {
	if !o.durable() {
		return
	}
	loaded := o.readRecordReceipts()
	o.mu.Lock()
	evicted := 0
	for len(loaded) > maxConsentOutboxEntries {
		// The cap trims OLDEST first at load exactly as it does on save: an
		// over-cap legacy record keeps its NEWEST receipts — the newest
		// decisions are the operative ones, and dropping them instead would
		// resend only stale history. The trim is settled so the next merging
		// save does not re-adopt (and re-count) the same entries from the
		// still-over-cap file.
		o.recordSettledLocked(loaded[0].IdempotencyKey)
		loaded = loaded[1:]
		evicted++
	}
	for _, entry := range loaded {
		if own == nil || own(entry) {
			o.ownKeys[entry.IdempotencyKey] = struct{}{}
		}
	}
	o.receipts = loaded
	o.evictedSinceSave += evicted
	if evicted > 0 {
		// The trim only happened in MEMORY: left there, a process that never
		// saves before exiting leaves the over-cap file behind, and every
		// restart re-evicts and re-counts the same entries. Marking the
		// write OWED is enough — the owed-write machinery (retryPersist at
		// every dispatch point, the construction wake below, Close's retry)
		// lands the trimmed record durably, and the failed-write posture is
		// preserved: nothing more is evicted if the rewrite fails, the
		// mirror stays authoritative, and the settled trim keys keep the
		// merge from re-adopting the evicted entries meanwhile.
		o.dirty = true
	}
	o.mu.Unlock()
}

// saveLocked rewrites consent-outbox.json with RELOAD-AND-MERGE semantics
// (the diskSpool.saveLocked discipline): the current on-disk record is
// re-read and unioned with the mirror by idempotency key — disk entries
// first (the older, already-persisted view), minus the keys this process
// settled — then the cap re-applied oldest-drop on the merged list, atomic
// private write. One floor client per SpoolDir is the supported topology;
// the merge is the safety net that keeps a sibling writer's undelivered
// receipts (appended after this process loaded) from being clobbered by a
// mirror-only rewrite, honoring the preserve-foreign model at the FILE
// level too. Cross-process races thereby shrink to last-writer-wins over a
// merged view: a receipt a sibling settled concurrently can resurrect and
// re-send, which the server de-duplicates by idempotency key. On SUCCESS
// the mirror adopts the merged view (foreign entries ride exactly as a
// load would have adopted them: retained, never dispatched out of scope)
// and cap evictions settle so a stale sibling write cannot resurrect them.
// On failure nothing is evicted, settled, or adopted — the mirror stays
// authoritative past the cap, dirty marks the owed write, and the caller
// counts the failure; evict-and-retry on failure is forbidden: it could
// turn a transient failure into a "successfully written" smaller record,
// silently dropping a receipt while reporting success. Must be called with
// mu held.
func (o *consentOutbox) saveLocked() error {
	seen := make(map[string]struct{}, len(o.receipts))
	merged := make([]consentReceipt, 0, len(o.receipts))
	for _, entry := range o.readRecordReceipts() {
		if _, settled := o.settledKeys[entry.IdempotencyKey]; settled {
			continue
		}
		if _, dup := seen[entry.IdempotencyKey]; dup {
			continue
		}
		seen[entry.IdempotencyKey] = struct{}{}
		merged = append(merged, entry)
	}
	for _, entry := range o.receipts {
		sanitized, ok := sanitizeConsentReceipt(entry)
		if !ok {
			continue
		}
		if _, own := o.ownKeys[sanitized.IdempotencyKey]; !own {
			// Only entries THIS process appended flow from the mirror to the
			// file; a foreign entry rides the fresh disk view above or not at
			// all — a stale mirror copy must not resurrect a receipt its
			// owner pruned since this process loaded it.
			continue
		}
		if _, dup := seen[sanitized.IdempotencyKey]; dup {
			continue
		}
		seen[sanitized.IdempotencyKey] = struct{}{}
		merged = append(merged, sanitized)
	}
	var evictKeys []string
	for len(merged) > maxConsentOutboxEntries {
		evictKeys = append(evictKeys, merged[0].IdempotencyKey)
		merged = merged[1:]
	}
	settleEvictions := func() {
		for _, key := range evictKeys {
			o.recordSettledLocked(key)
			delete(o.recordOwedKeys, key)
			delete(o.ownKeys, key)
		}
		o.evictedSinceSave += len(evictKeys)
	}
	if !o.durable() {
		// No durable backend: the trimmed mirror is all there is (and the
		// disk view above was empty). The cap still applies — the outbox is
		// bounded in every mode — and dirty stays false: there is no write
		// to owe.
		o.receipts = merged
		settleEvictions()
		return nil
	}
	record := consentOutboxWire{Version: consentOutboxRecordVersion, Receipts: merged}
	payload, err := json.Marshal(record)
	if err == nil {
		err = writePrivateFileAtomic(o.filePath(), payload, o.renameFn, o.chmodFn)
	}
	if err != nil {
		o.dirty = true
		return err
	}
	o.receipts = merged
	settleEvictions()
	o.dirty = false
	return nil
}

// append adds a freshly minted receipt (a new decision) and persists.
// Returns whether the durable write failed (owed; retried at every dispatch
// point).
func (o *consentOutbox) append(receipt consentReceipt) (persistFailed bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	// Own BEFORE saving: the merging save writes exactly the disk view plus
	// this process's own unsaved appends, and the fresh entry is not on disk
	// yet.
	o.ownKeys[receipt.IdempotencyKey] = struct{}{}
	o.receipts = append(o.receipts, receipt)
	return o.saveLocked() != nil
}

// head returns the oldest retained receipt for dispatch, when one exists.
func (o *consentOutbox) head() (consentReceipt, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.receipts) == 0 {
		return consentReceipt{}, false
	}
	return o.receipts[0], true
}

// tail returns the newest retained receipt — the LATEST explicit decision
// still awaiting delivery — when one exists. The reload uses it as the
// newer truth over a stale persisted decision record (see initConsentFloor).
func (o *consentOutbox) tail() (consentReceipt, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.receipts) == 0 {
		return consentReceipt{}, false
	}
	return o.receipts[len(o.receipts)-1], true
}

// maxDecidedAt returns the latest parseable decided_at among ALL retained
// receipts ("" when none). The reload seeds the monotonic stamp floor from
// it: new decisions must out-order every persisted stamp, and including
// foreign receipts is harmlessly conservative (stamps merely start later).
func (o *consentOutbox) maxDecidedAt() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	best := ""
	var bestAt time.Time
	for _, entry := range o.receipts {
		at, err := time.Parse(time.RFC3339Nano, entry.DecidedAt)
		if err != nil {
			continue
		}
		if best == "" || at.After(bestAt) {
			best, bestAt = entry.DecidedAt, at
		}
	}
	return best
}

// latestMatching returns the newest retained receipt satisfying match — the
// trail tail AS SEEN BY one scope. A reused SpoolDir interleaves scopes: a
// foreign receipt newer than this client's latest in-scope receipt must not
// HIDE it (requiring the absolute tail to be in scope would skip the
// override and let a stale record rule), so the reload scans newest→oldest
// for the latest decision that actually belongs to this scope.
func (o *consentOutbox) latestMatching(match func(consentReceipt) bool) (consentReceipt, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for i := len(o.receipts) - 1; i >= 0; i-- {
		if match(o.receipts[i]) {
			return o.receipts[i], true
		}
	}
	return consentReceipt{}, false
}

// prune removes a settled receipt by idempotency key after an
// acknowledgement or a terminal drop, and rewrites the record. The settled
// receipt is the dispatch pass's in-flight one — the oldest IN-SCOPE entry
// — which need not be the absolute head in a reused SpoolDir: foreign
// entries retained before it are never dispatched (and so never pruned) by
// this client, and removing a mid-array entry preserves the relative order
// of everything else. A failed rewrite never blocks the rest of the trail:
// the mirror is already pruned, dirty marks the owed write, and if the
// process dies first the next launch re-sends the stale entry and the
// server de-duplicates.
func (o *consentOutbox) prune(idempotencyKey string) (persistFailed bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for i, entry := range o.receipts {
		if entry.IdempotencyKey != idempotencyKey {
			continue
		}
		// Settle the key FIRST: the merging save re-reads the on-disk record,
		// which still carries this receipt until the rewrite lands, and an
		// unsettled key would simply resurrect through the merge. Settling
		// also survives a failed rewrite — the next successful save excludes
		// it — and clears any stale record-owed/own mark with the entry.
		o.recordSettledLocked(idempotencyKey)
		delete(o.recordOwedKeys, idempotencyKey)
		delete(o.ownKeys, idempotencyKey)
		pruned := make([]consentReceipt, 0, len(o.receipts)-1)
		pruned = append(pruned, o.receipts[:i]...)
		pruned = append(pruned, o.receipts[i+1:]...)
		o.receipts = pruned
		return o.saveLocked() != nil
	}
	return false
}

// markRecordOwed marks one retained receipt's decision-record write as owed
// (see recordOwedKeys): the receipt holds from dispatch until a successful
// record write settles every pair.
func (o *consentOutbox) markRecordOwed(idempotencyKey string) {
	if idempotencyKey == "" {
		return
	}
	o.mu.Lock()
	o.recordOwedKeys[idempotencyKey] = struct{}{}
	o.mu.Unlock()
}

// markRecordOwedWhere marks every retained receipt matching the predicate
// (the reload uses it after a failed heal: every in-scope receipt newer than
// the stale on-disk record describes a decision that record does not cover).
func (o *consentOutbox) markRecordOwedWhere(match func(consentReceipt) bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, entry := range o.receipts {
		if match(entry) {
			o.recordOwedKeys[entry.IdempotencyKey] = struct{}{}
		}
	}
}

// clearRecordOwedMarks drops every record-owed mark: a decision-record write
// just SUCCEEDED, and the record always describes the newest decision — at
// least as new as every marked receipt's — so no retained receipt's pair is
// incomplete anymore.
func (o *consentOutbox) clearRecordOwedMarks() {
	o.mu.Lock()
	o.recordOwedKeys = make(map[string]struct{})
	o.mu.Unlock()
}

// recordOwedFor reports whether a receipt's decision-record write is still
// owed (per-receipt; see recordOwedKeys).
func (o *consentOutbox) recordOwedFor(idempotencyKey string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, owed := o.recordOwedKeys[idempotencyKey]
	return owed
}

// snapshot copies the retained trail in order (for read-only walks that
// combine per-receipt state with client-level holds without holding mu).
func (o *consentOutbox) snapshot() []consentReceipt {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]consentReceipt(nil), o.receipts...)
}

// nextDispatchable returns the oldest retained receipt IN SCOPE — the one
// this client's dispatch may put on the wire. Foreign receipts (a reused
// SpoolDir interleaves scopes) are SKIPPED, never dispatched: this client's
// bearer is scoped, so a foreign receipt could take a terminal 401/403 here
// and be pruned — losing ANOTHER scope's consent receipt before a correctly
// scoped client resends it. Foreign entries stay retained (per-directory
// retention; the cap still bounds them), and skipping them cannot reorder
// this client's own trail — in-scope receipts keep their relative order.
func (o *consentOutbox) nextDispatchable(inScope func(consentReceipt) bool) (consentReceipt, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, entry := range o.receipts {
		if inScope(entry) {
			return entry, true
		}
	}
	return consentReceipt{}, false
}

// writeOwed reports an owed durable outbox write (a failed save not yet
// recovered). The owed-record retry checks it before writing a GRANT
// record: receipt-first means the granted record may not land while the
// receipt trail's own write is still owed.
func (o *consentOutbox) writeOwed() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.dirty
}

// retryPersist re-attempts an owed durable write. The first return reports
// whether a write was attempted at all.
func (o *consentOutbox) retryPersist() (attempted, failed bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.dirty {
		return false, false
	}
	return true, o.saveLocked() != nil
}

// takeEvicted drains the cap-eviction count for Stats.ConsentOutboxEvicted.
func (o *consentOutbox) takeEvicted() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	evicted := o.evictedSinceSave
	o.evictedSinceSave = 0
	return evicted
}

// pending reports undelivered work: retained receipts, or an owed durable
// write (the dirty-with-empty-mirror case — a failed post-ack prune — still
// counts: the on-disk record is stale until the rewrite lands).
func (o *consentOutbox) pending() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.receipts) > 0 || o.dirty
}

// pendingDurability reports what Close must know: whether undelivered work
// remains at all, and whether it is safely on disk (durable backend, no
// owed write) so teardown may complete with the receipts re-sending at the
// next launch.
func (o *consentOutbox) pendingDurability() (pending, safelyOnDisk bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	pending = len(o.receipts) > 0 || o.dirty
	safelyOnDisk = o.durable() && !o.dirty
	return pending, safelyOnDisk
}

// grantPendingDispatch is the dispatch-gate predicate: an analytics-GRANT
// receipt is retained that was NOT handed to the transport during the
// current cycle's dispatch pass (handed). Such a grant — parked behind a
// Retry-After or backoff window, queued behind another receipt, or reloaded
// from disk after a relaunch — must hold event batches: events dispatched
// meanwhile would overtake it on the wire, and on a strict-consent
// workspace be terminally suppressed. A grant that WAS handed this cycle
// releases the gate for itself the moment of handoff — dispatch, never
// acknowledgement — even when its outcome was a retryable failure: its
// request preceded any batch dispatched after it in this cycle, and the
// retained copy re-arms the gate for LATER cycles instead. Only IN-SCOPE
// grants arm the gate (inScope — the same workspace/app/environment/actor
// tuple as the reload's trail matching): a FOREIGN grant retained in a
// reused SpoolDir is unrelated to whether this client's events are
// admissible, and a parked foreign grant must not hold this pipeline — it
// still re-sends verbatim for its own historic scope, it just doesn't gate.
func (o *consentOutbox) grantPendingDispatch(handed map[string]struct{}, inScope func(consentReceipt) bool) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, entry := range o.receipts {
		if !entry.analyticsGranted() || !inScope(entry) {
			continue
		}
		if _, wasHanded := handed[entry.IdempotencyKey]; !wasHanded {
			return true
		}
	}
	return false
}

// claimDispatch takes the serial-dispatch claim; false when another pass is
// already running (its receipts count as in flight; the caller's gate check
// treats everything it did not hand itself as pending).
func (o *consentOutbox) claimDispatch() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.dispatching {
		return false
	}
	o.dispatching = true
	return true
}

// claimDispatchWait takes the serial-dispatch claim, JOINING behind an
// in-flight pass instead of skipping: a caller-driven drain (Flush, Close)
// promises its caller that the receipt work ran, so losing the claim to a
// concurrent pass must wait for that pass to release — bounded by the
// caller's context — and then run its own. Returns false only when the
// context ended first (the drain did not run; the caller must not report
// success over it). A pass is always finite — each attempt is bounded by
// HTTPTimeout and the trail is capped — so the wait terminates.
func (o *consentOutbox) claimDispatchWait(ctx context.Context) bool {
	for {
		o.mu.Lock()
		if !o.dispatching {
			o.dispatching = true
			o.mu.Unlock()
			return true
		}
		wait := make(chan struct{})
		o.dispatchWaiters = append(o.dispatchWaiters, wait)
		o.mu.Unlock()
		select {
		case <-wait:
			// The claim released; loop to contend for it again.
		case <-contextDone(ctx):
			return false
		}
	}
}

func (o *consentOutbox) releaseDispatch() {
	o.mu.Lock()
	o.dispatching = false
	waiters := o.dispatchWaiters
	o.dispatchWaiters = nil
	o.mu.Unlock()
	for _, wait := range waiters {
		close(wait)
	}
}

// deferralActive reports whether the consent plane is parked (a retryable
// failure armed Retry-After or backoff) as of now.
func (o *consentOutbox) deferralActive(now time.Time) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return !o.deferUntil.IsZero() && now.Before(o.deferUntil)
}

// ── Client-level orchestration ──────────────────────────────────────────────

// consentFloorEnabled reports the opt-in.
func (c *Client) consentFloorEnabled() bool {
	return c.cfg.ConsentFloor != nil
}

// initConsentFloor runs the construction-time consent-floor lifecycle,
// before the worker starts. Persisted floor state — the receipt outbox AND
// the decision that becomes the LIVE state — may be trusted only through a
// state directory whose privacy is established, the same
// ensurePrivateDir-first gate initSpool applies before trusting spool.json:
// in a directory that cannot be tightened to 0700, a stale or planted
// grant/outbox entry could otherwise start the client live-granted or
// transmit fabricated receipts. A refused tighten starts the floor
// FAIL-CLOSED instead: undecided state, empty outbox, surfaced through
// Stats.LastError — and every later durable outbox write keeps failing
// through this same gate inside writePrivateFileAtomic, so the owed-write
// machinery and Close's ErrConsentPending backstop apply (the on-disk files
// are left in place for a later run with the permissions fixed). The
// worker's first dispatch pass re-sends reloaded receipts BEFORE any event
// batch — the retained grant itself is the dispatch gate; no deferral state
// persists across launches, and none is needed. rename and chmod are
// injectable so tests can exercise the refused-tighten gate and a failed
// heal write deterministically.
func (c *Client) initConsentFloor(rename func(oldpath, newpath string) error, chmod func(name string, mode os.FileMode) error) {
	c.consentOutbox = newConsentOutbox(c.cfg.SpoolDir)
	if c.cfg.SpoolDir == "" {
		return
	}
	if err := ensurePrivateDir(c.cfg.SpoolDir, chmod); err != nil {
		c.stats.setLastError("spool_dir_private_failed")
		c.logf("shardpilot consent floor: the state directory could not be made private (0700); persisted floor state is not loaded and the floor starts undecided with an empty outbox: %v", err)
		return
	}
	// In-scope entries load as this client's OWN trail (its previous run's
	// receipts, kept written back by every merging save); foreign entries
	// stay disk-refreshed so a sibling scope's own prunes are honored.
	c.consentOutbox.load(c.consentReceiptInScope)
	c.drainConsentOutboxEvictions()
	if err := c.validateConsentFloorIdentity(); err != nil {
		// The identity contract holds at RELOAD exactly as at decision
		// time: a persisted decision (legacy, seeded, or written under a
		// different build) must not start the floor granted for a
		// configuration whose identifiers are out of contract — events
		// would publish out-of-contract identifiers past the decision-time
		// gate. Fail closed: undecided, distinctly diagnosed. Retained
		// receipts still deliver (the sanitizer guarantees their actors are
		// in contract).
		c.stats.setLastError("consent_identity_invalid")
		c.logf("shardpilot consent floor: a configured identifier exceeds the %d-byte clamp; the persisted decision is not loaded and the floor starts undecided: %v", maxConsentIdentifierBytes, err)
	} else {
		digest := consentActorDigest(c.cfg)
		record, recordOK := loadConsentRecordInfo(c.cfg.SpoolDir, digest)
		state := record.state
		// Seed the monotonic stamp floor from PERSISTED state before any
		// new decision can mint: a restart on a system clock running behind
		// the persisted stamps would otherwise mint a decision OLDER than
		// the record or a retained receipt — the strictly-newer rule would
		// then ignore the new decision's receipt at the next reload, or let
		// a stale unpruned grant receipt out-order a durable denial.
		c.seedConsentStamp(record.decidedAt, c.consentOutbox.maxDecidedAt())
		// The trail can be newer truth than the record: when the record
		// write for the LATEST decision was still owed when the previous
		// process ended, trusting the record would resurrect the SUPERSEDED
		// decision (a stale grant reopening the pipeline for an actor whose
		// last decision was a denial). The proof is the latest IN-SCOPE
		// receipt — scanned newest→oldest, so a foreign receipt retained
		// after it (another scope sharing the SpoolDir) can never hide it —
		// and it may override ONLY when it SUPERSEDES the record
		// (consentReceiptSupersedesRecord): strictly newer than a stamped
		// record — the outbox can also hold a STALE receipt (acknowledged
		// long ago, left on disk by a failed prune rewrite), and re-reading
		// that must never flip the state back over a record persisted for a
		// NEWER decision (deny record durable, deny receipt append failed,
		// crash) — while a LEGACY stampless record is provably older than
		// any validly-stamped proof and yields in both directions, and a
		// floor-marked stampless record (the preserved corrupt denial)
		// never yields by comparison. With no record at all the proof
		// restores its decision unconditionally.
		if tail, ok := c.consentOutbox.latestMatching(c.consentReceiptInScope); ok &&
			(!recordOK || consentReceiptSupersedesRecord(tail.DecidedAt, record)) {
			tailDecision := ConsentDecisionDenied
			tailState := ConsentDenied
			switch {
			case tail.analyticsGranted():
				tailDecision, tailState = ConsentDecisionGranted, ConsentGranted
			case tail.Reason == consentDecisionReason:
				tailDecision, tailState = ConsentDecisionDeniedForcedMinor, ConsentDeniedForcedMinor
			}
			// Heal UNCONDITIONALLY inside this branch: the outer condition
			// already established the record is absent or strictly OLDER
			// than the proof, so the on-disk record is outdated even when
			// the state string matches — a same-state rewrite is what turns
			// an UNPROVEN (floor-off era) granted record into the proven,
			// receipt-stamped one, or the provenance gate below would
			// discard a grant whose durable proof is right there. The heal
			// carries the RECEIPT's stamp, so a later reload sees record
			// and receipt as the same decision (no re-override churn); the
			// trail-derived state is floor-proven by construction — its
			// receipt exists.
			staleRecord, staleOK := record, recordOK
			state, recordOK = tailState, true
			record = consentRecordInfo{state: tailState, decidedAt: tail.DecidedAt, floor: true}
			if err := saveConsentRecord(c.cfg.SpoolDir, tailDecision, digest, tail.DecidedAt, true, rename, chmod); err != nil {
				c.stats.setLastError("consent_record_persist_failed")
				c.logf("shardpilot consent floor: healing the stale decision record from the receipt trail failed (the trail-derived state still applies in memory and the write stays owed): %v", err)
				// The failed heal is an OWED record write, registered
				// BEFORE the worker starts: without it the proof receipt
				// would not be held (consentDenyProofHeld) and could
				// deliver and prune the only durable evidence of the
				// denial — a crash after that would restore the stale
				// pre-denial record. The retry at every dispatch point
				// heals it exactly like a live decision's failed write.
				// Marked PER RECEIPT too: every in-scope receipt the stale
				// on-disk record does not cover holds from dispatch until
				// the heal lands, so a later live decision's failed write
				// overwriting the single owed slot cannot release them.
				c.consentRecordApplyMu.Lock()
				c.setConsentRecordOwed(tailDecision, tail.DecidedAt, false)
				c.consentOutbox.markRecordOwedWhere(func(entry consentReceipt) bool {
					if !c.consentReceiptInScope(entry) {
						return false
					}
					return !staleOK || consentReceiptSupersedesRecord(entry.DecidedAt, staleRecord)
				})
				c.consentRecordApplyMu.Unlock()
			}
		}
		if recordOK {
			if state == ConsentGranted && !record.floor {
				// FLOOR PROVENANCE: a granted record without the floor mark
				// was authored by the floor-off fire-and-forget era — its
				// consent POST may have failed and no durable receipt
				// exists — so promoting it to live floor state would flow
				// events with no receipt ever sent (receipt-before-events
				// broken on a strict-consent workspace). Unproven grants
				// start the floor UNDECIDED, fail-closed and distinctly
				// diagnosed; the host records a fresh decision to proceed.
				// DENIALS are honored regardless of provenance — honoring a
				// denial is the fail-closed direction.
				c.stats.setLastError("consent_record_unproven")
				c.logf("shardpilot consent floor: the persisted granted record has no floor provenance (written without Config.ConsentFloor); it is not promoted to live state and the floor starts undecided — record a fresh decision")
			} else {
				switch state {
				case ConsentGranted:
					c.consent.Store(consentStateGranted)
				case ConsentDenied:
					c.consent.Store(consentStateDenied)
				case ConsentDeniedForcedMinor:
					c.consent.Store(consentStateDeniedForcedMinor)
				}
			}
		}
	}
	if c.consentOutbox.pending() {
		// Construction is a dispatch point: reloaded receipts must re-send
		// promptly, not idle until the first flush tick (potentially
		// FlushInterval away) or a caller-driven operation. The buffered
		// nudge is consumed by the worker's select as soon as it starts.
		c.wakeConsentDispatch()
	}
}

// consentReceiptInScope reports whether a retained receipt describes THIS
// client's decision scope: the configured workspace/app/environment tuple
// and the configured actor. The reload's trail-tail override may trust only
// an in-scope receipt as the operative decision — a SpoolDir reused across
// workspaces, apps, or environments retains foreign receipts (they re-send
// verbatim for their own historic scope, correctly), and a foreign grant or
// denial must not flip this client's live state or heal consent.json for a
// digest the receipt's decision never covered. Deliberately STRICTER than
// the record digest (which spans apps within a workspace/environment):
// skipping an out-of-scope override keeps the correctly-scoped record,
// while adopting a cross-scope tail would apply another scope's decision.
func (c *Client) consentReceiptInScope(receipt consentReceipt) bool {
	// BOTH actor components must match, not just the wire identifier: the
	// receipt retains the AnonymousID it was minted under precisely so a
	// reused SpoolDir with the same UserID but a DIFFERENT configured
	// AnonymousID cannot treat the old identity's receipt as this scope's —
	// the record digest already spans both components, and an
	// ActorIdentifier-only match would let the old receipt override and
	// heal the NEW digest (loading spool data the record scoping rejected).
	// The mint stamps AnonymousID from the same validated config the digest
	// hashes, so equality here mirrors the digest's actor scope exactly.
	return receipt.WorkspaceID == c.cfg.WorkspaceID &&
		receipt.AppID == c.cfg.AppID &&
		receipt.EnvironmentID == c.cfg.EnvironmentID &&
		receipt.ActorIdentifier == firstNonEmpty(c.cfg.UserID, c.cfg.AnonymousID) &&
		receipt.AnonymousID == c.cfg.AnonymousID
}

// consentFloorActorMismatch reports whether an event's EFFECTIVE actor —
// per-event overrides applied over the configured identifiers exactly as
// buildEnvelope stamps them — differs from the configured actor the floor's
// decision covers. The floor holds ONE client-side decision for ONE actor:
// an event overriding to a different effective actor would transmit an
// actor with no local decision and no dispatched receipt, so Track/Enqueue
// refuse it (ErrConsentActorMismatch). Per-actor decisions beyond the
// configured identity belong to the server-side consent path — the default
// posture, which per-event overrides were designed for; with the floor off
// they pass through unchanged.
func (c *Client) consentFloorActorMismatch(event Event) bool {
	if event.UserID == "" && event.AnonymousID == "" {
		return false
	}
	effective := firstNonEmpty(
		firstNonEmpty(event.UserID, c.cfg.UserID),
		firstNonEmpty(event.AnonymousID, c.cfg.AnonymousID),
	)
	return effective != firstNonEmpty(c.cfg.UserID, c.cfg.AnonymousID)
}

// validateConsentFloorIdentity gates a consent decision on IN-CONTRACT
// configured identifiers when the floor is enabled: any non-empty
// Config.UserID/AnonymousID over the 512-byte clamp REJECTS the decision —
// reject, never truncate, and never silently mint the receipt for a
// DIFFERENT actor than events carry. Go's EVENT path deliberately has no
// identifier clamp (the envelope stamps configured identifiers verbatim),
// so a receipt minted for a fallback actor would authorize an actor the
// events do not attribute to — on a strict-consent workspace the events
// would be suppressed while the receipt reads as covered. Both identifiers
// empty stays the documented local-only decision path.
func (c *Client) validateConsentFloorIdentity() error {
	if c.cfg.UserID != "" && !validConsentIdentifier(c.cfg.UserID) {
		return ErrInvalidConsentIdentity
	}
	if c.cfg.AnonymousID != "" && !validConsentIdentifier(c.cfg.AnonymousID) {
		return ErrInvalidConsentIdentity
	}
	return nil
}

// consentOwedRecord is an owed decision-record write: the decision and its
// original decided-at stamp (the retried record must carry the instant of
// the decision it describes, not the retry's).
type consentOwedRecord struct {
	decision  ConsentDecision
	decidedAt string
}

// setConsentRecordOwed records the outcome of a consent decision's durable
// record write: persisted clears the owed slot, a failure (or a withheld
// grant record) owes the exact decision for retry. Must be called under
// consentRecordApplyMu, right after the write (or withhold) it describes —
// the slot and the disk state move together. Owed state only ever exists
// with a spool (memory-only floors have no record at all).
func (c *Client) setConsentRecordOwed(decision ConsentDecision, decidedAt string, persisted bool) {
	c.consentOwedMu.Lock()
	// SpoolDir (not c.spool) is the durability truth: the reload registers
	// a failed HEAL as owed before the spool object exists.
	if persisted || c.cfg.SpoolDir == "" {
		c.consentRecordOwed = nil
		c.consentOwedMu.Unlock()
		if persisted && c.consentOutbox != nil {
			// The record write SUCCEEDED, and the record always describes the
			// newest decision: every retained receipt's pair is settled, so
			// the per-receipt holds release (see recordOwedKeys — the slot
			// alone cannot carry them: a newer decision's failure overwrites
			// it, which must not erase an older receipt's incomplete pair).
			c.consentOutbox.clearRecordOwedMarks()
		}
		return
	}
	c.consentRecordOwed = &consentOwedRecord{decision: decision, decidedAt: decidedAt}
	c.consentOwedMu.Unlock()
}

// consentRecordOwedSnapshot reads the owed slot (nil when nothing is owed).
func (c *Client) consentRecordOwedSnapshot() *consentOwedRecord {
	c.consentOwedMu.Lock()
	defer c.consentOwedMu.Unlock()
	return c.consentRecordOwed
}

// retryOwedConsentRecord re-applies an OWED consent-record write at a
// dispatch point — every dispatch point is a persistence retry point for
// the record exactly as it is for the outbox. Completion rules: a DENIAL
// re-applies immediately (re-purge is idempotent); a GRANT completes the
// receipt-first PAIR — it may write the granted record only once the
// receipt trail's own durable write is no longer owed (a withheld grant
// record landing right after the outbox retry succeeds, so an acknowledged
// receipt can never prune away leaving no durable grant behind). The
// record-apply lock is TRY-locked: an opportunistic retry never makes the
// dispatch path wait out a stalled decision write — the holder settles the
// slot itself, and the next dispatch point retries.
func (c *Client) retryOwedConsentRecord() {
	if !c.consentFloorEnabled() || c.spool == nil {
		return
	}
	if c.consentRecordOwedSnapshot() == nil {
		return
	}
	if !c.consentRecordApplyMu.TryLock() {
		return
	}
	var deadLetters []SpoolDeadLetter
	func() {
		defer c.consentRecordApplyMu.Unlock()
		owed := c.consentRecordOwedSnapshot()
		if owed == nil {
			return
		}
		if owed.decision == ConsentDecisionGranted {
			if c.consentOutbox.writeOwed() {
				// Receipt-first: the grant's record stays withheld while the
				// receipt trail itself is not durably down.
				return
			}
			if mo := c.consentMintOwedSnapshot(); mo != nil && mo.decidedAt == owed.decidedAt {
				// The grant's receipt has not even been MINTED yet: the
				// outbox is clean only because there is nothing in it to
				// write, so writeOwed alone cannot guard this pair. Writing
				// the granted record now would make "granted record, no
				// receipt anywhere" durable — a crash would promote the
				// grant receipt-less at the next launch, exactly what
				// receipt-first forbids. The record lands only after the
				// mint retry appends the receipt (retryOwedConsentMint runs
				// earlier in the same dispatch sequence), completing the
				// pair in order.
				return
			}
		}
		var persisted bool
		deadLetters, persisted = c.applySpoolConsent(owed.decision, owed.decidedAt)
		c.setConsentRecordOwed(owed.decision, owed.decidedAt, persisted)
	}()
	// Emit outside the lock: the callback is integrator code and may call
	// back into the client (a retried denial purge is normally empty — the
	// original purge already condemned and reported the entries).
	c.emitSpoolDeadLetters(deadLetters)
}

// consentOwedMint is a decision whose receipt could not even be MINTED (the
// idempotency-key mint failed — crypto randomness unavailable): the decision
// applied locally, but its receipt is OWED, never "provably never coming".
// Only the truly actorless local-only path (no configured UserID or
// AnonymousID) is sanctioned to persist a decision without a receipt; a
// mint failure for a CONFIGURED actor behaves like a failed append instead
// — the grant record is withheld, Close pends (ErrConsentPending), and the
// mint retries at every dispatch point. The slot holds the NEWEST decision
// only: a newer decision that mints successfully supersedes it — appending
// the older receipt after the newer one would break the trail's
// append-order = decision-order invariant, and the superseded decision was
// never durable anywhere, exactly like a failed append lost to a crash.
type consentOwedMint struct {
	decision         ConsentDecision
	analyticsGranted bool
	reason           string
	decidedAt        string
}

// setConsentMintOwed stores (or, with nil, clears) the owed-mint slot.
func (c *Client) setConsentMintOwed(owed *consentOwedMint) {
	c.consentOwedMu.Lock()
	c.consentMintOwed = owed
	c.consentOwedMu.Unlock()
}

// consentMintOwedSnapshot reads the owed-mint slot (nil when nothing is
// owed).
func (c *Client) consentMintOwedSnapshot() *consentOwedMint {
	c.consentOwedMu.Lock()
	defer c.consentOwedMu.Unlock()
	return c.consentMintOwed
}

// consentMintID mints a receipt idempotency key through the injectable seam
// (tests exercise mint failure deterministically); nil means uuidv7.
func (c *Client) consentMintID() (string, error) {
	c.consentOwedMu.Lock()
	mint := c.consentMintIDFn
	c.consentOwedMu.Unlock()
	if mint == nil {
		return uuidv7.New()
	}
	return mint()
}

// retryOwedConsentMint re-attempts an OWED receipt mint at a dispatch point
// — every dispatch point is a mint retry point exactly as it is a
// persistence retry point. On success the receipt appends (its stamp, flavor
// and reason are the ORIGINAL decision's; only the idempotency key is fresh
// — no receipt with the old key can exist anywhere, the mint never produced
// one) and, when the decision's record write is still owed (a mint-failed
// GRANT withholds its record), the fresh receipt is marked pair-incomplete
// so it holds from dispatch until the record lands. The record-apply lock is
// TRY-locked like the owed-record retry: an opportunistic retry never makes
// the dispatch path wait out a live decision.
func (c *Client) retryOwedConsentMint() {
	if !c.consentFloorEnabled() {
		return
	}
	if c.consentMintOwedSnapshot() == nil {
		return
	}
	if !c.consentRecordApplyMu.TryLock() {
		return
	}
	defer c.consentRecordApplyMu.Unlock()
	owed := c.consentMintOwedSnapshot()
	if owed == nil {
		return
	}
	receipt, minted, err := c.mintConsentReceipt(owed.analyticsGranted, owed.reason, owed.decidedAt)
	if err != nil || !minted {
		// Still failing (the configured actor cannot vanish — cfg is
		// static); the slot stays owed for the next dispatch point.
		return
	}
	c.setConsentMintOwed(nil)
	if c.consentOutbox.append(receipt) {
		c.recordConsentOutboxPersistFailure()
	}
	c.drainConsentOutboxEvictions()
	if owedRecord := c.consentRecordOwedSnapshot(); owedRecord != nil && owedRecord.decidedAt == owed.decidedAt {
		c.consentOutbox.markRecordOwed(receipt.IdempotencyKey)
	}
	c.wakeConsentDispatch()
}

// newerInScopeDenialHeld reports whether an in-scope DENIAL receipt NEWER
// than the given grant (later in the trail — trail order is decision order
// per scope) is retained AND itself held from dispatch: its own
// decision-record write is owed (the per-receipt mark), or it is the held
// deny proof (consentDenyProofHeld). Delivering the earlier grant and
// pruning it while such a denial is parked would make the grant the
// server's LAST word for as long as the denial's record write keeps
// failing — and a crash meanwhile leaves it that way against the local
// denial. The generalized rule across the stale-grant family: an in-scope
// grant never dispatches past a parked newer in-scope denial, whatever
// parked it — the slot-overwrite mark, the owed mint (checked separately:
// that receipt is not in the trail yet), or the held proof. A newer denial
// with NO holds needs no park: the same serial pass delivers the grant and
// then the denial, in order.
func (c *Client) newerInScopeDenialHeld(grant consentReceipt) bool {
	seenGrant := false
	for _, entry := range c.consentOutbox.snapshot() {
		if entry.IdempotencyKey == grant.IdempotencyKey {
			seenGrant = true
			continue
		}
		if !seenGrant || entry.analyticsGranted() || !c.consentReceiptInScope(entry) {
			continue
		}
		if c.consentOutbox.recordOwedFor(entry.IdempotencyKey) || c.consentDenyProofHeld(entry) {
			return true
		}
	}
	return false
}

// newerInScopeDenialInTrail reports whether ANY in-scope denial receipt —
// held or freely dispatchable — sits after the grant in the trail. The
// fast-half window check uses it: a live DENIED state with no denial
// receipt behind the grant means the denial's slow half has not appended
// yet (or its evidence is otherwise absent), so the grant must not post on
// the strength of the trail alone.
func (c *Client) newerInScopeDenialInTrail(grant consentReceipt) bool {
	seenGrant := false
	for _, entry := range c.consentOutbox.snapshot() {
		if entry.IdempotencyKey == grant.IdempotencyKey {
			seenGrant = true
			continue
		}
		if !seenGrant || entry.analyticsGranted() || !c.consentReceiptInScope(entry) {
			continue
		}
		return true
	}
	return false
}

// consentDenyProofHeld reports whether this receipt must stay retained
// instead of dispatching: it is the trail's PROOF — the latest in-scope
// receipt — of a DENIAL whose decision-record write is still owed. If it
// delivered and pruned now, the only durable state left would be the stale
// pre-denial record, and a crash would restore the superseded decision
// (a stale grant reopening the pipeline for a denied actor). The pass stops
// at the proof (serial order forbids skipping); the owed-record retry at
// each dispatch point releases it the moment the denied record lands.
// Grants are never held: their crash direction is fail-CLOSED, and the
// withheld-record pair completes at the retry site instead.
func (c *Client) consentDenyProofHeld(receipt consentReceipt) bool {
	owed := c.consentRecordOwedSnapshot()
	if owed == nil || owed.decision == ConsentDecisionGranted {
		return false
	}
	proof, ok := c.consentOutbox.latestMatching(c.consentReceiptInScope)
	return ok && proof.IdempotencyKey == receipt.IdempotencyKey
}

// wakeConsentDispatch nudges the worker to run a consent dispatch pass
// promptly (non-blocking; one pending nudge is enough). No-op with the
// floor off.
func (c *Client) wakeConsentDispatch() {
	if c.consentWake == nil {
		return
	}
	select {
	case c.consentWake <- struct{}{}:
	default:
	}
}

// consentReceiptWire builds the wire body for a stored receipt: the entry
// minus the anonymous-id retention metadata, verbatim — same idempotency
// key, same decided_at, on every attempt.
func consentReceiptWire(receipt consentReceipt) consentRequest {
	return consentRequest{
		WorkspaceID:     receipt.WorkspaceID,
		AppID:           receipt.AppID,
		EnvironmentID:   receipt.EnvironmentID,
		ActorIdentifier: receipt.ActorIdentifier,
		Categories:      map[string]bool{"analytics": receipt.analyticsGranted()},
		DecidedAt:       receipt.DecidedAt,
		IdempotencyKey:  receipt.IdempotencyKey,
		Reason:          receipt.Reason,
	}
}

// consentDeliveryRetryable classifies a receipt delivery failure: transport
// errors (no response), 429, and any 5xx retry with the receipt kept at the
// head; every other outcome is terminal — including 401, because this
// client's bearer is static for its lifetime (no re-mint seam), matching
// the engine SDKs' static-credential rule.
func consentDeliveryRetryable(err error) bool {
	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) {
		return statusErr.Retryable()
	}
	var encodeErr *EncodeError
	if errors.As(err, &encodeErr) {
		return false
	}
	// No HTTP response arrived: transport-level failure, retryable.
	return true
}

// dispatchConsentReceipts runs one serial dispatch pass when the floor is
// on: while the plane is not parked, hand the head receipt to the
// transport; an acknowledgement prunes and chains the next, a retryable
// failure parks the plane (server Retry-After on 429 AND 5xx, else jittered
// backoff) and stops, a terminal outcome drops the receipt (diagnosed) and
// chains. Each attempt is bounded by the SOONER of the caller's context and
// HTTPTimeout — caller-driven dispatch points (Track, Flush, Close) pass
// their own context so a caller deadline or cancellation bounds the consent
// POST too; the worker's automatic points pass background. An attempt ended
// by the CALLER's own context is no outcome: the receipt stays at the head,
// nothing is counted, no deferral is armed (the same no-outcome discipline
// as callerAbandonedFlush). Returns the idempotency keys handed to the
// transport during THIS pass — the dispatch gate releases exactly those
// (release-on-dispatch, never on acknowledgement). An owed durable write is
// retried first — every dispatch point is also a persistence retry point.
//
// join selects the claim discipline: automatic passes (worker ticks/wakes,
// Track's opportunistic dispatch) SKIP when another pass already holds the
// serial claim — the work is being served — while CALLER-DRIVEN drains
// (Flush, Close) JOIN behind the in-flight pass, bounded by the caller's
// context, and then run their own: their caller was promised the receipt
// work ran, and skipping would let a flush report success over an undrained
// trail. The second return reports whether the drain RAN TO ITS OWN STOP:
// false when the caller's context ended first — either waiting out the join
// or mid-attempt — so the caller must not report success over receipts it
// never drained.
func (c *Client) dispatchConsentReceipts(ctx context.Context, join bool) (map[string]struct{}, bool) {
	if !c.consentFloorEnabled() {
		return nil, true
	}
	o := c.consentOutbox
	// The mint retry runs FIRST: a recovered mint appends its receipt, and
	// the outbox/record retries just below then settle that append's write
	// and its withheld record in the same pass.
	c.retryOwedConsentMint()
	if attempted, failed := o.retryPersist(); attempted {
		if failed {
			c.recordConsentOutboxPersistFailure()
		}
		c.drainConsentOutboxEvictions()
	}
	// The record retry runs AFTER the outbox retry on purpose: a withheld
	// grant record completes its receipt-first pair in the same pass the
	// outbox write recovers — before the receipt below can dispatch, be
	// acknowledged, and prune away the trail's only durable evidence.
	c.retryOwedConsentRecord()
	if join {
		if !o.claimDispatchWait(ctx) {
			return nil, false
		}
	} else if !o.claimDispatch() {
		return nil, false
	}
	defer o.releaseDispatch()
	handed := make(map[string]struct{})
	for {
		if o.deferralActive(c.clock.Now()) {
			return handed, true
		}
		receipt, ok := o.nextDispatchable(c.consentReceiptInScope)
		if !ok {
			return handed, true
		}
		if o.recordOwedFor(receipt.IdempotencyKey) {
			// THIS receipt's own decision-record write is still owed — its
			// durable pair is incomplete, PER RECEIPT: the client's single
			// owed slot tracks only the newest decision, so a retained older
			// grant whose record never landed must not lose its hold when a
			// newer denial's failed record write overwrites the slot — it
			// would dispatch and prune while the deny proof is held, making
			// a grant the server's last word against a local denial. The pass
			// stops (serial order); a successful record write — always for
			// the newest decision — clears every mark and releases the trail
			// in order.
			return handed, true
		}
		if owedMint := c.consentMintOwedSnapshot(); owedMint != nil &&
			owedMint.decision != ConsentDecisionGranted && receipt.analyticsGranted() {
			// A NEWER denial's receipt is still owed to the mint: delivering
			// an earlier in-scope grant now could leave the grant as the
			// server's last word indefinitely (the denial's receipt does not
			// even exist to follow it). Earlier GRANTS park until the owed
			// denial mints; earlier denials may still deliver — same state as
			// the operative decision, the fail-safe direction.
			return handed, true
		}
		if receipt.analyticsGranted() && c.newerInScopeDenialHeld(receipt) {
			// The same rule for a denial that IS in the trail, minted and
			// durably appended, but itself HELD (its record write owed, or
			// the held deny proof): the earlier grant must not deliver and
			// prune while the newer denial is parked — the server's last
			// word would be granted against the local denial until (unless)
			// the denial's record heals. The pass stops; the heal that
			// releases the denial releases the whole trail in order.
			return handed, true
		}
		if c.consentDenyProofHeld(receipt) {
			// The next dispatchable is the trail's proof of a denial whose
			// record write is still owed: it must not deliver (and prune)
			// while the stale pre-denial record is the only other durable
			// state — a crash after the prune would restore the superseded
			// decision. The pass stops here (serial order forbids skipping
			// within the scope); the owed-record retry above releases it
			// once the record heals.
			return handed, true
		}
		if receipt.analyticsGranted() && c.consentGrantPairIncomplete() {
			// A GRANT dispatches only when its PAIR — the durable receipt
			// AND the granted record — is fully down: with the outbox write
			// owed (this receipt's own failed append may BE that write),
			// with the granted record write owed, or while a grant decision
			// is still mid-persist (the arming window), an acknowledgement
			// now could be followed by a crash before the remaining half
			// recovers — leaving neither a durable receipt nor a granted
			// record, losing the user's grant across restart even though
			// the server recorded it. The pass stops (serial order); the
			// retries above heal both halves at the next dispatch point and
			// release it. Memory-only outboxes never owe writes and are
			// unaffected.
			return handed, true
		}
		if receipt.analyticsGranted() {
			// TOCTOU guard before the handoff: a decision mid-flight can be
			// appending and marking a NEWER held denial right now — the hold
			// checks above ran before its append landed in the trail.
			// Re-take the decision serialization point OPPORTUNISTICALLY:
			// a failed TryLock means a decision IS mid-flight (possibly that
			// denial), so the grant parks for the next pass — dispatch never
			// waits out a stalled decision write — and a successful TryLock
			// proves no decision is between its append and its marks, so the
			// held-denial predicates re-run against the settled trail. A
			// denial recorded AFTER this instant follows the grant on the
			// wire in decision order (the dispatch claim serializes posts),
			// which is the legitimate sequential outcome.
			if !c.consentRecordApplyMu.TryLock() {
				return handed, true
			}
			mo := c.consentMintOwedSnapshot()
			heldBehindDenial := (mo != nil && mo.decision != ConsentDecisionGranted) ||
				c.newerInScopeDenialHeld(receipt)
			if !heldBehindDenial && c.consentDenied() && !c.newerInScopeDenialInTrail(receipt) {
				// The FAST-HALF window: a denial has already flipped the
				// LIVE state (stored under lifecycleMu, before any disk
				// work) but its slow half has not appended the receipt —
				// the apply lock was free to take and NO hold is visible in
				// the trail, yet posting the grant now could make it the
				// server's last word with the denial's receipt landing right
				// behind. Park until the denial's evidence exists: an
				// appended unheld denial then delivers in order BEHIND the
				// grant (the normal grant-then-deny trail), while a stale
				// grant reloaded under a durably denied state simply stays
				// retained — durable, and a replay the server de-duplicates
				// by key if it ever posts.
				heldBehindDenial = true
			}
			c.consentRecordApplyMu.Unlock()
			if heldBehindDenial {
				return handed, true
			}
		}
		attemptCtx, cancel := contextWithDefaultTimeout(ctx, c.cfg.HTTPTimeout)
		_, err := c.transport.PublishConsent(attemptCtx, consentReceiptWire(receipt))
		cancel()
		// The dispatch gate releases only on an OBSERVED HTTP outcome: a
		// success, or a status error — proof the request reached the server
		// and was answered, so an event batch following in this cycle
		// cannot be the server's first-seen request. A failure with NO
		// response observed (connection refused, send-path EOF, a timeout
		// before any status, a caller abort) leaves the receipt UNHANDED:
		// the server may never have seen the grant, and the gate must keep
		// holding the batch legs.
		var statusErr *HTTPStatusError
		if err == nil || errors.As(err, &statusErr) {
			handed[receipt.IdempotencyKey] = struct{}{}
		}
		if err == nil {
			c.stats.consentRecorded.Add(1)
			o.mu.Lock()
			o.deferUntil = time.Time{}
			o.backoffAttempt = 0
			o.mu.Unlock()
			if o.prune(receipt.IdempotencyKey) {
				c.recordConsentOutboxPersistFailure()
			}
			c.drainConsentOutboxEvictions()
			continue
		}
		if ctx != nil {
			if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				// The CALLER's own context ended the attempt (cancellation
				// or its deadline): an abort, not an endpoint outcome — and
				// for a caller-driven drain, an incomplete drain.
				return handed, false
			}
		}
		c.stats.consentFailed.Add(1)
		c.stats.setLastConsentError(err.Error())
		if !consentDeliveryRetryable(err) {
			c.logf("shardpilot consent floor: receipt %s dropped after a terminal delivery outcome: %v", receipt.IdempotencyKey, err)
			if o.prune(receipt.IdempotencyKey) {
				c.recordConsentOutboxPersistFailure()
			}
			c.drainConsentOutboxEvictions()
			continue
		}
		c.armConsentDeferral(err)
		return handed, true
	}
}

// armConsentDeferral parks the consent plane after a retryable delivery
// failure: the server's Retry-After (parsed on 429 AND 5xx — the
// strict-consent lane answers 503 + Retry-After) wins when present, else
// the shared jittered backoff shape (first retry free at the next dispatch
// point, then full jitter in [1s, min(2^(n-2), 60)s]). The deferral state
// is independent of the events plane's pacing: a denial clears the publish
// deferral while receipt retries must keep backing off.
func (c *Client) armConsentDeferral(err error) {
	o := c.consentOutbox
	o.mu.Lock()
	defer o.mu.Unlock()
	o.backoffAttempt++
	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) && statusErr.retryAfterPresent {
		retryAfter := statusErr.RetryAfter
		if retryAfter < minRetryNowSpacing {
			retryAfter = minRetryNowSpacing
		}
		o.deferUntil = c.clock.Now().Add(retryAfter)
		return
	}
	if delay := c.backoffDelay(o.backoffAttempt); delay > 0 {
		o.deferUntil = c.clock.Now().Add(delay)
	} else {
		o.deferUntil = time.Time{}
	}
}

// grantReceiptGateArmed evaluates the dispatch gate after a pass: handed is
// what THIS cycle put on the wire. A grant decision mid-append (its receipt
// not yet in the outbox — see consentGrantArming) arms the gate exactly
// like a retained undispatched grant: the batch legs must not reopen before
// the receipt exists to be dispatched ahead of them.
func (c *Client) grantReceiptGateArmed(handed map[string]struct{}) bool {
	if !c.consentFloorEnabled() {
		return false
	}
	if c.consentGrantArming.Load() > 0 {
		return true
	}
	if owedMint := c.consentMintOwedSnapshot(); owedMint != nil && owedMint.analyticsGranted {
		// The operative grant's receipt is OWED to a failed mint: no retained
		// receipt exists to arm the gate, but the batch legs must hold all
		// the same — events dispatched now would reach a strict-consent
		// workspace before any receipt could (the mint retries at every
		// dispatch point and the appended receipt then takes over the gate).
		// Always this client's own decision, so no foreign-scope concern.
		return true
	}
	return c.consentOutbox.grantPendingDispatch(handed, c.consentReceiptInScope)
}

func (c *Client) recordConsentOutboxPersistFailure() {
	c.stats.consentOutboxPersistFailed.Add(1)
	c.stats.setLastConsentError("consent_outbox_persist_failed")
	c.logf("shardpilot consent floor: writing the consent outbox failed; the in-memory receipts stay authoritative and the write is retried at every dispatch point")
}

func (c *Client) drainConsentOutboxEvictions() {
	if evicted := c.consentOutbox.takeEvicted(); evicted > 0 {
		c.stats.consentOutboxEvicted.Add(uint64(evicted))
		c.logf("shardpilot consent floor: the outbox cap evicted %d oldest receipt(s)", evicted)
	}
}

// mintConsentReceipt builds a receipt at decision time under the floor. The
// actor snapshot is the configured actor exactly as events carry it (UserID
// preferred, else AnonymousID) — validateConsentFloorIdentity already
// rejected out-of-contract identifiers before the decision applied, so the
// receipt's actor can never silently diverge from the events' actor. The
// two no-receipt outcomes are DISTINCT: no configured actor at all means no
// receipt with a nil error — the sanctioned local-only path, where a
// receipt is provably never coming — while a failed idempotency-key mint
// returns the error: the receipt IS owed (the caller registers it for
// retry; the decision must not persist as if the trail were safe). The
// anonymous-id retention snapshot never rides the wire.
func (c *Client) mintConsentReceipt(analyticsGranted bool, reason, decidedAt string) (consentReceipt, bool, error) {
	actor := firstNonEmpty(c.cfg.UserID, c.cfg.AnonymousID)
	if actor == "" {
		return consentReceipt{}, false, nil
	}
	idempotencyKey, err := c.consentMintID()
	if err != nil {
		return consentReceipt{}, false, err
	}
	receipt := consentReceipt{
		IdempotencyKey:  idempotencyKey,
		WorkspaceID:     c.cfg.WorkspaceID,
		AppID:           c.cfg.AppID,
		EnvironmentID:   c.cfg.EnvironmentID,
		ActorIdentifier: actor,
		// The DECISION's stamp, shared with the decision record: the reload
		// orders receipts against the record by this instant, so both sides
		// of one decision must carry the same one (see consentDecisionStamp).
		DecidedAt: decidedAt,
		Reason:    reason,
	}
	receipt.Categories.Analytics = &analyticsGranted
	if validConsentIdentifier(c.cfg.AnonymousID) {
		receipt.AnonymousID = c.cfg.AnonymousID
	}
	return receipt, true, nil
}

// consentDecisionStamp mints a decision's decided-at instant: RFC3339 with
// nanoseconds, STRICTLY MONOTONIC per client. Two decisions recorded in the
// same clock tick (a coarse or duplicated reading) would otherwise mint
// identical stamps and defeat the reload's strictly-newer override — after
// a crash the stale record would tie with, and beat, the newest retained
// receipt. When the clock has not advanced past the previously issued
// stamp, the new stamp is the previous one plus one nanosecond: a
// deterministic total order for rapid decisions that the parse/compare
// honors, and still valid RFC3339 on the wire. Mints are serialized by the
// consent ticket turn; the mutex keeps the seam safe for bare test clients
// too.
func (c *Client) consentDecisionStamp() string {
	c.consentStampMu.Lock()
	defer c.consentStampMu.Unlock()
	now := c.clock.Now().UTC()
	if c.lastConsentStamp != "" {
		if last, err := time.Parse(time.RFC3339Nano, c.lastConsentStamp); err == nil && !now.After(last) {
			now = last.Add(time.Nanosecond)
		}
	}
	stamp := now.Format(time.RFC3339Nano)
	c.lastConsentStamp = stamp
	return stamp
}

// seedConsentStamp advances the monotonic decision-stamp floor to at least
// the given persisted stamps (unparseable or empty ones are ignored): after
// a reload, consentDecisionStamp must mint strictly AFTER everything
// already on disk, whatever the system clock says.
func (c *Client) seedConsentStamp(stamps ...string) {
	c.consentStampMu.Lock()
	defer c.consentStampMu.Unlock()
	for _, stamp := range stamps {
		if stamp == "" {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil {
			continue
		}
		if c.lastConsentStamp != "" {
			if last, lerr := time.Parse(time.RFC3339Nano, c.lastConsentStamp); lerr == nil && !at.After(last) {
				continue
			}
		}
		c.lastConsentStamp = stamp
	}
}

// consentGrantPairIncomplete reports whether the current GRANT pair — the
// durable receipt trail and the granted decision record — is not yet fully
// on disk: the outbox write is owed, the granted record write is owed, or a
// grant decision is still mid-persist (the arming window between its
// receipt append and its owed-slot settlement). While incomplete, in-scope
// grant receipts are held from dispatch: an acknowledgement would prune the
// only durable half (or race the missing one), and a crash right after
// would lose the user's grant across restart even though the server
// recorded it.
func (c *Client) consentGrantPairIncomplete() bool {
	if c.consentOutbox.writeOwed() {
		return true
	}
	if c.consentGrantArming.Load() > 0 {
		return true
	}
	owed := c.consentRecordOwedSnapshot()
	return owed != nil && owed.decision == ConsentDecisionGranted
}

// consentReceiptNewerThanRecord reports whether a retained receipt's
// decision is STRICTLY newer than the persisted record's decision moment.
// Only then may the trail override the record at reload: a STALE receipt —
// acknowledged long ago but re-read from an outbox rewrite that failed to
// prune it — must never flip the state back over a record persisted for a
// NEWER decision.
func consentReceiptNewerThanRecord(receiptDecidedAt, recordDecidedAt string) bool {
	if receiptDecidedAt == "" || recordDecidedAt == "" {
		return false
	}
	receiptAt, err := time.Parse(time.RFC3339Nano, receiptDecidedAt)
	if err != nil {
		return false
	}
	recordAt, err := time.Parse(time.RFC3339Nano, recordDecidedAt)
	if err != nil {
		return false
	}
	return receiptAt.After(recordAt)
}

// consentReceiptSupersedesRecord decides whether an in-scope proof receipt
// may override (and heal) the persisted record at reload. Three record
// shapes, three rules:
//   - STAMPED record: the proof must be STRICTLY newer
//     (consentReceiptNewerThanRecord) — a stale acked-but-unpruned receipt
//     never flips back a newer decision.
//   - LEGACY stampless record (no floor mark): it predates the stamping
//     build, so it is OLDER than any validly-stamped receipt BY DEFINITION —
//     the proof supersedes it in BOTH directions (a grant proof heals a
//     floor-marked grant over a legacy denial; a denial proof heals denied
//     over a legacy grant that provenance would otherwise strand as
//     undecided, losing the denial). With no validly-stamped in-scope proof
//     retained, today's posture holds: provenance vets legacy grants,
//     legacy denials are honored.
//   - FLOOR-MARKED stampless record (the corrupt-stamped DENIAL shape
//     loadConsentRecordInfo preserves): never superseded by comparison —
//     the record is not provably old, and failing toward the denial is the
//     fail-closed direction.
func consentReceiptSupersedesRecord(receiptDecidedAt string, record consentRecordInfo) bool {
	if record.decidedAt == "" {
		if record.floor {
			return false
		}
		_, err := time.Parse(time.RFC3339Nano, receiptDecidedAt)
		return err == nil
	}
	return consentReceiptNewerThanRecord(receiptDecidedAt, record.decidedAt)
}

// recordDiscardedCloseRemnant accounts for the undelivered events a
// MEMORY-ONLY floor client discards when the worker stops: with no spool
// there is nothing to retain them in — typically the remnant a gated final
// flush left held — so they are gone. Counted into Stats.Dropped and
// remembered for Close's verdict (ErrEventsDiscarded): the documented
// memory-only contract is that undelivered events do not survive teardown,
// and the floor's addition is that the loss is REPORTED rather than read
// as a clean close. No-op with a spool configured (the remnant spooled) or
// with the floor off (there Close's own flush error is the caller's
// signal, unchanged).
func (c *Client) recordDiscardedCloseRemnant(batch []Event) {
	if c.spool != nil || !c.consentFloorEnabled() {
		return
	}
	discarded := len(batch)
	for {
		select {
		case <-c.queue.ch:
			discarded++
			continue
		default:
		}
		break
	}
	if discarded == 0 {
		return
	}
	c.stats.dropped.Add(uint64(discarded))
	c.closeDiscardedEvents.Add(uint64(discarded))
}

// recordUnspooledCloseRemnant accounts for close-remnant events a FLOOR
// client's spool could not make safe when the worker stopped: the write
// gate refused them (an unpersisted grant record or an owed wipe — e.g. a
// refused directory tighten), they sit in the mirror with their durable
// write STILL owed after the final persist retry (e.g. disk full) —
// counted per event through uncountedIDs, so a remnant that merely
// de-duplicated against an earlier dirty append is counted exactly like a
// fresh dirty add — or the close phase CAPACITY-EVICTED them
// (capacityDropped: the remnant overflowed SpoolMaxEvents/SpoolMaxBytes
// and the settled evictions — the remnant's own members or the older
// entries they displaced — landed durably gone; an eviction at exit has no
// later resend, unlike a mid-session one whose loss the caps always
// implied, while a still-DEFERRED eviction stays on disk and reloads, so
// it is deliberately NOT counted). Remnant members past the RETRY-AGE cap
// (expiredDropped — the append refused them as too old to ever resend) and
// members that could not SERIALIZE (poisonedDropped — settled and already
// counted Dropped by settlePoisonedEvents, so they join the verdict fold
// only) are teardown losses the same way. Either way the process exiting
// now loses them — neither delivered nor durable — so, exactly like the
// memory-only discard above, they are counted Dropped and folded
// permanently into every Close verdict (ErrEventsDiscarded): a successful
// consent drain must not read as a clean teardown over lost events. The
// dead-letter callback (gate refusals, capacity drops, expiries, poisoned
// members) still received its courtesy copy. Floor-off keeps today's
// posture unchanged: stats and dead-letters, no Close-verdict fold.
func (c *Client) recordUnspooledCloseRemnant(gateRefused int, mirrored []string, capacityDropped, expiredDropped, poisonedDropped int) {
	if c.spool == nil || !c.consentFloorEnabled() {
		return
	}
	// Gate refusals, close-phase capacity evictions, retry-age expiries, and
	// still-unpersisted mirror entries count Dropped here; the remnant's
	// POISONED members were already counted Dropped by settlePoisonedEvents
	// and only join the close-verdict fold — a teardown loss either way.
	lost := gateRefused + capacityDropped + expiredDropped + c.spool.unpersistedOf(mirrored)
	discarded := lost + poisonedDropped
	if discarded == 0 {
		return
	}
	if lost > 0 {
		c.stats.dropped.Add(uint64(lost))
	}
	c.closeDiscardedEvents.Add(uint64(discarded))
}

// closeDiscardVerdict folds the permanent discarded-events record into a
// Close verdict; nil-in nil-out when nothing was discarded. IDEMPOTENT: a
// verdict already carrying ErrEventsDiscarded passes through unchanged, so
// the fold is re-applied safely on every cached-close return — a Close
// whose context expired before the worker's stop path finished counting can
// cache its verdict BEFORE the discard lands, and later Close calls must
// still report the loss.
func (c *Client) closeDiscardVerdict(err error) error {
	discarded := c.closeDiscardedEvents.Load()
	if discarded == 0 || errors.Is(err, ErrEventsDiscarded) {
		return err
	}
	return errors.Join(err, fmt.Errorf("%w: %d event(s)", ErrEventsDiscarded, discarded))
}

// finalizeConsentOutbox is Close's consent-floor drain: one last dispatch
// pass (delivering what the endpoint will take, bounded by the Close
// context), then the durability verdict — teardown completes only when
// nothing undelivered remains OR every retained receipt is safely on disk
// (durable backend, no owed write), where it re-sends at the next launch.
// Otherwise ErrConsentPending: the process exiting now would lose the
// receipts; a repeated Close retries both the delivery and the owed write.
func (c *Client) finalizeConsentOutbox(ctx context.Context) error {
	if !c.consentFloorEnabled() {
		return nil
	}
	// Close's drain JOINS behind any in-flight pass (bounded by the Close
	// context) so the final delivery attempt actually runs instead of
	// skipping; the durability verdict below stays safe either way.
	_, drained := c.dispatchConsentReceipts(ctx, true)
	if attempted, failed := c.consentOutbox.retryPersist(); attempted {
		if failed {
			c.recordConsentOutboxPersistFailure()
		}
		c.drainConsentOutboxEvictions()
	}
	verdict := c.consentOutboxCloseVerdict()
	if !drained && (verdict != nil || c.consentOutbox.pending()) {
		// The caller's OWN context stopped the drain — the join was cut
		// short, or an attempt aborted mid-flight — while receipt work
		// remains. The verdict alone (ErrConsentPending, or even nil over
		// durably-retained receipts) would hide that this Close never ran
		// its delivery attempt to completion within the caller's bound, so
		// the caller's error folds in; errors.Is(·, ErrConsentPending)
		// still drives the retryable-close branch when the pending state
		// holds. A cut drain with NOTHING left (the joined pass delivered
		// everything) folds nothing — no work was skipped.
		verdict = errors.Join(verdict, contextCause(ctx))
	}
	return verdict
}

// consentOutboxCloseVerdict computes Close's consent durability verdict —
// teardown completes only when nothing undelivered remains OR every retained
// receipt is safely on disk with no owed mint or record write.
func (c *Client) consentOutboxCloseVerdict() error {
	if c.consentMintOwedSnapshot() != nil {
		// A decision's receipt is still OWED to a failed mint (the dispatch
		// pass above retried it): the receipt is neither delivered nor
		// durable ANYWHERE — unlike the actorless local-only path, it is
		// coming — so teardown must not read as clean. Retryable: a repeated
		// Close re-runs the mint retry.
		return ErrConsentPending
	}
	if owed := c.consentRecordOwedSnapshot(); owed != nil {
		if owed.decision == ConsentDecisionGranted {
			// The GRANT pair is incomplete: the granted record write is
			// still owed (the dispatch pass above retried it), so teardown
			// must not read as clean — outbox durability alone does not
			// cover the pair, and a receipt that had already delivered
			// would leave NEITHER half on disk. Retryable: a repeated
			// Close re-runs the record retry.
			return ErrConsentPending
		}
		// An owed DENIAL record may complete teardown ONLY over a DURABLE
		// proof: a held deny receipt safely on disk lets the next launch
		// restore the denial from the trail and heal the record. Without
		// one — the documented local-only path (no configured actor)
		// minted no receipt, or the outbox write is itself owed — nothing
		// durable contradicts the stale pre-denial record, and a clean
		// Close would let a restart promote it. (A receipt owed to a
		// failed MINT already pended above.) Retryable exactly like the
		// grant pair.
		proof, ok := c.consentOutbox.latestMatching(c.consentReceiptInScope)
		_, safelyOnDisk := c.consentOutbox.pendingDurability()
		if !ok || proof.analyticsGranted() || !safelyOnDisk {
			return ErrConsentPending
		}
	}
	pending, safelyOnDisk := c.consentOutbox.pendingDurability()
	if !pending || safelyOnDisk {
		return nil
	}
	return ErrConsentPending
}
