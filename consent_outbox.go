package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
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
	// order is preserved on the wire.
	dispatching bool

	// deferUntil parks the consent plane after a retryable delivery failure
	// (server Retry-After, or jittered backoff); backoffAttempt counts the
	// consecutive-failure streak (reset by an acknowledgement). Independent
	// of the events plane's pacing.
	deferUntil     time.Time
	backoffAttempt int

	// renameFn/chmodFn are the file primitives, injectable so tests can
	// exercise failed-write-never-evicts deterministically (the same seam
	// discipline as diskSpool).
	renameFn func(oldpath, newpath string) error
	chmodFn  func(name string, mode os.FileMode) error
}

func newConsentOutbox(dir string) *consentOutbox {
	return &consentOutbox{
		dir:      dir,
		renameFn: os.Rename,
		chmodFn:  os.Chmod,
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

// load reads the durable record into the mirror at construction: sanitized,
// capped, oldest first. A file that is missing, over the bounded read
// limit, unparseable, or of an unknown version loads as EMPTY — corrupt
// state is a clean start, never a crash into the host — and a wholly
// garbled record is simply overwritten by the next save.
func (o *consentOutbox) load() {
	if !o.durable() {
		return
	}
	file, err := os.Open(o.filePath())
	if err != nil {
		return
	}
	data, err := io.ReadAll(io.LimitReader(file, consentOutboxReadLimit+1))
	_ = file.Close()
	if err != nil || len(data) > consentOutboxReadLimit {
		return
	}
	var record consentOutboxWire
	if json.Unmarshal(data, &record) != nil || record.Version != consentOutboxRecordVersion {
		return
	}
	loaded := make([]consentReceipt, 0, len(record.Receipts))
	for _, entry := range record.Receipts {
		sanitized, ok := sanitizeConsentReceipt(entry)
		if !ok {
			continue
		}
		loaded = append(loaded, sanitized)
	}
	evicted := 0
	for len(loaded) > maxConsentOutboxEntries {
		// The cap trims OLDEST first at load exactly as it does on save: an
		// over-cap legacy record keeps its NEWEST receipts — the newest
		// decisions are the operative ones, and dropping them instead would
		// resend only stale history.
		loaded = loaded[1:]
		evicted++
	}
	o.mu.Lock()
	o.receipts = loaded
	o.evictedSinceSave += evicted
	o.mu.Unlock()
}

// saveLocked rewrites consent-outbox.json from the mirror: sanitize, evict
// oldest past the cap (the mirror ADOPTS the trimmed list — the newest
// decisions are the operative ones), atomic private write. On failure
// nothing is evicted and nothing partially succeeds: the mirror stays
// authoritative, dirty marks the owed write, and the caller counts the
// failure. Must be called with mu held.
func (o *consentOutbox) saveLocked() error {
	kept := make([]consentReceipt, 0, len(o.receipts))
	for _, entry := range o.receipts {
		sanitized, ok := sanitizeConsentReceipt(entry)
		if !ok {
			continue
		}
		kept = append(kept, sanitized)
	}
	evicted := 0
	for len(kept) > maxConsentOutboxEntries {
		kept = kept[1:]
		evicted++
	}
	if !o.durable() {
		// No durable backend: the trimmed mirror is all there is. The cap
		// still applies (the outbox is bounded in every mode), and dirty
		// stays false — there is no write to owe.
		o.receipts = kept
		o.evictedSinceSave += evicted
		return nil
	}
	record := consentOutboxWire{Version: consentOutboxRecordVersion, Receipts: kept}
	payload, err := json.Marshal(record)
	if err == nil {
		err = writePrivateFileAtomic(o.filePath(), payload, o.renameFn, o.chmodFn)
	}
	if err != nil {
		// Failed write: NEVER evict, never partially succeed. The mirror —
		// including entries past the cap — keeps ruling the process and the
		// write is owed. Evict-and-retry on failure is forbidden: it could
		// turn a transient failure into a "successfully written" smaller
		// record, silently dropping a receipt while reporting success.
		o.dirty = true
		return err
	}
	o.receipts = kept
	o.evictedSinceSave += evicted
	o.dirty = false
	return nil
}

// append adds a freshly minted receipt (a new decision) and persists.
// Returns whether the durable write failed (owed; retried at every dispatch
// point).
func (o *consentOutbox) append(receipt consentReceipt) (persistFailed bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
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

// prune removes the head receipt by idempotency key after an
// acknowledgement or a terminal drop, and rewrites the record. A failed
// rewrite never blocks the rest of the trail: the mirror is already pruned,
// dirty marks the owed write, and if the process dies first the next launch
// re-sends the stale entry and the server de-duplicates.
func (o *consentOutbox) prune(idempotencyKey string) (persistFailed bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.receipts) == 0 || o.receipts[0].IdempotencyKey != idempotencyKey {
		return false
	}
	o.receipts = append([]consentReceipt(nil), o.receipts[1:]...)
	return o.saveLocked() != nil
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
// retained copy re-arms the gate for LATER cycles instead.
func (o *consentOutbox) grantPendingDispatch(handed map[string]struct{}) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, entry := range o.receipts {
		if !entry.analyticsGranted() {
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

func (o *consentOutbox) releaseDispatch() {
	o.mu.Lock()
	o.dispatching = false
	o.mu.Unlock()
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
// persists across launches, and none is needed. chmod is injectable so
// tests can exercise the refused-tighten gate deterministically.
func (c *Client) initConsentFloor(chmod func(name string, mode os.FileMode) error) {
	c.consentOutbox = newConsentOutbox(c.cfg.SpoolDir)
	if c.cfg.SpoolDir == "" {
		return
	}
	if err := ensurePrivateDir(c.cfg.SpoolDir, chmod); err != nil {
		c.stats.setLastError("spool_dir_private_failed")
		c.logf("shardpilot consent floor: the state directory could not be made private (0700); persisted floor state is not loaded and the floor starts undecided with an empty outbox: %v", err)
		return
	}
	c.consentOutbox.load()
	c.drainConsentOutboxEvictions()
	if state, ok := loadConsentRecord(c.cfg.SpoolDir, consentActorDigest(c.cfg)); ok {
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
func (c *Client) dispatchConsentReceipts(ctx context.Context) map[string]struct{} {
	if !c.consentFloorEnabled() {
		return nil
	}
	o := c.consentOutbox
	if attempted, failed := o.retryPersist(); attempted {
		if failed {
			c.recordConsentOutboxPersistFailure()
		}
		c.drainConsentOutboxEvictions()
	}
	if !o.claimDispatch() {
		return nil
	}
	defer o.releaseDispatch()
	handed := make(map[string]struct{})
	for {
		if o.deferralActive(c.clock.Now()) {
			return handed
		}
		receipt, ok := o.head()
		if !ok {
			return handed
		}
		handed[receipt.IdempotencyKey] = struct{}{}
		attemptCtx, cancel := contextWithDefaultTimeout(ctx, c.cfg.HTTPTimeout)
		_, err := c.transport.PublishConsent(attemptCtx, consentReceiptWire(receipt))
		cancel()
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
				// or its deadline): an abort, not an endpoint outcome.
				return handed
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
		return handed
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
// what THIS cycle put on the wire.
func (c *Client) grantReceiptGateArmed(handed map[string]struct{}) bool {
	if !c.consentFloorEnabled() {
		return false
	}
	return c.consentOutbox.grantPendingDispatch(handed)
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
// actor snapshot is the first VALID configured identifier (UserID
// preferred; validity = non-empty and within the 512-byte clamp — rejected,
// never truncated); no valid actor means no receipt (the decision still
// applies locally). The anonymous-id retention snapshot rides only when
// valid, and never on the wire.
func (c *Client) mintConsentReceipt(analyticsGranted bool, reason string) (consentReceipt, bool) {
	actor := ""
	if validConsentIdentifier(c.cfg.UserID) {
		actor = c.cfg.UserID
	} else if validConsentIdentifier(c.cfg.AnonymousID) {
		actor = c.cfg.AnonymousID
	}
	if actor == "" {
		return consentReceipt{}, false
	}
	idempotencyKey, err := uuidv7.New()
	if err != nil {
		c.logf("shardpilot consent floor: generate idempotency key failed; the decision applies locally without a receipt: %v", err)
		return consentReceipt{}, false
	}
	receipt := consentReceipt{
		IdempotencyKey:  idempotencyKey,
		WorkspaceID:     c.cfg.WorkspaceID,
		AppID:           c.cfg.AppID,
		EnvironmentID:   c.cfg.EnvironmentID,
		ActorIdentifier: actor,
		DecidedAt:       c.clock.Now().UTC().Format(time.RFC3339),
		Reason:          reason,
	}
	receipt.Categories.Analytics = &analyticsGranted
	if validConsentIdentifier(c.cfg.AnonymousID) {
		receipt.AnonymousID = c.cfg.AnonymousID
	}
	return receipt, true
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
	c.dispatchConsentReceipts(ctx)
	if attempted, failed := c.consentOutbox.retryPersist(); attempted {
		if failed {
			c.recordConsentOutboxPersistFailure()
		}
		c.drainConsentOutboxEvictions()
	}
	pending, safelyOnDisk := c.consentOutbox.pendingDurability()
	if !pending || safelyOnDisk {
		return nil
	}
	return ErrConsentPending
}
