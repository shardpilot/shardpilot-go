package shardpilot

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	cfg       Config
	clock     Clock
	queue     *boundedQueue
	transport transport
	stats     statsCollector

	// jitter is the uniform [0, 1) source for the publish backoff's full
	// jitter; tests pin it for deterministic schedules. Only the flush
	// worker goroutine reads it. Nil falls back to the shared math/rand
	// source (bare Clients constructed in tests).
	jitter func() float64

	// publishSuccesses counts every successful batch publish on ANY path —
	// including synchronous Track publishes on caller goroutines. The worker
	// samples it (against workerSeenSuccesses) to learn that the endpoint
	// recovered while it was waiting out a retry deadline: a success that
	// happened after the deadline was armed clears the deferral and resets
	// the backoff, exactly like a success on the worker's own path.
	publishSuccesses atomic.Uint64

	// workerSeenSuccesses is the worker's last-consumed publishSuccesses
	// value. Owned by the flush worker goroutine exclusively (resynced by
	// applyRetryPacing when a publish outcome settles, so a success
	// concurrent with a failing publish is never mistaken for a later
	// recovery).
	workerSeenSuccesses uint64

	// pacingWake nudges a parked worker when publishSuccesses moves, so a
	// recovery proven by a synchronous Track retries the held batch NOW
	// instead of at the next flush tick (which can be minutes away).
	// Capacity 1 with non-blocking sends: one pending nudge is enough.
	pacingWake chan struct{}

	flushRequests chan flushRequest
	stop          chan struct{}
	workerDone    chan struct{}
	lifecycleMu   sync.Mutex

	// The consent ticket line serializes the SLOW half of SetConsent — disk
	// persistence and the sender handoff — in the exact order the fast
	// halves took effect, WITHOUT making intake or a later decision's
	// in-memory flip wait behind an earlier decision's fsync. Tickets are
	// assigned under lifecycleMu together with the in-memory flip (so the
	// ticket order IS the decision order), and each decision waits for its
	// ticket before touching disk: the persisted record and the transmitted
	// sequence always agree with the in-memory order — the last decision's
	// write lands last — while a denial issued during an earlier decision's
	// stalled write still rejects Track/Enqueue from the moment it was
	// issued. consentTicketNext is guarded by lifecycleMu;
	// consentTicketServing by consentTurnMu.
	consentTurnMu        sync.Mutex
	consentTurnCond      *sync.Cond
	consentTicketNext    uint64
	consentTicketServing uint64

	// consentDecisionsWG counts SetConsent calls admitted BEFORE Close
	// stored the closed flag (both happen under lifecycleMu, so the set is
	// exact). Close waits for them — bounded by its context — before
	// stopping and draining the consent sender: a pre-Close decision's
	// record write, spool purge, and transmission handoff must not be
	// abandoned by teardown (a stranded enqueue would silently never
	// transmit; an interrupted denial could leave a stale granted record on
	// disk). Decisions arriving after Close keep today's documented
	// applied-locally-only behavior and are not fenced.
	consentDecisionsWG sync.WaitGroup

	trackWG       sync.WaitGroup
	closeMu       sync.Mutex
	closeInFlight bool
	closeComplete bool
	closeDone     chan struct{}
	stopOnce      sync.Once
	closeErr      error
	closed        atomic.Bool
	consent       atomic.Int32

	// consentEpoch is the denial generation counter: SetConsent(false)
	// increments it so the worker discards events it has already pulled
	// into its local batch. The worker tracks the last epoch it observed
	// and drops its held batch whenever the epoch moved, which guarantees
	// events enqueued before a denial never survive into a later granted
	// period.
	consentEpoch atomic.Uint64

	// consentGate carries a context cancelled on consent denial so event
	// publishes already in flight abort instead of completing against a
	// freshly denied actor. SetConsent stores the denied state before
	// swapping in a fresh gate, and publishers load the gate before
	// re-checking denial, so every denial either cancels the gate an
	// in-flight publisher holds or is visible to that publisher's
	// re-check.
	consentGate atomic.Pointer[consentGateState]

	// consentSends feeds the single consent sender goroutine (started
	// lazily by consentSenderOnce) so consent decisions are transmitted
	// in SetConsent call order. consentSenderDone is closed when the
	// sender exits; Close waits on it (bounded by the Close context, like
	// workerDone) so decisions recorded before Close are transmitted
	// before Close returns.
	consentSends      chan consentRequest
	consentSenderOnce sync.Once
	consentSenderDone chan struct{}

	// rc is the remote-config machinery; nil when Config.RemoteConfigURL is
	// unset (fetches fail remote_config_not_configured, getters serve
	// defaults).
	rc *remoteConfigState

	// spool is the opt-in bounded disk spool; nil when Config.SpoolDir is
	// unset (today's memory-only behavior, unchanged).
	spool *diskSpool

	// consentOutbox is the consent floor's receipt outbox; nil unless
	// Config.ConsentFloor is set (see consent_outbox.go). Durable when
	// SpoolDir is also set, in-memory otherwise.
	consentOutbox *consentOutbox

	// consentWake nudges the worker to run a consent dispatch pass promptly
	// after a decision under the consent floor, instead of waiting for the
	// flush tick. Capacity 1 with non-blocking sends; nil when the floor is
	// off (a nil channel never fires in the worker's select).
	consentWake chan struct{}

	// consentGrantArming counts floor GRANT decisions whose receipt is not
	// yet appended to the outbox: the fast half flips the live state to
	// granted BEFORE the ticket-ordered slow half appends the receipt, and
	// in that window a concurrent Track/worker flush would see granted with
	// an empty outbox — an unarmed gate — and ship a batch BEFORE the grant
	// receipt exists. The counter arms the dispatch gate across the window
	// (incremented with the state flip under lifecycleMu, decremented once
	// the slow half has appended the receipt — or established that none
	// will exist), so the event path reopens only once the receipt is the
	// gate's own source of truth: receipt-armed-before-observable-grant,
	// adapted to the fast/slow lock split.
	consentGrantArming atomic.Int64

	// closeDiscardedEvents counts undelivered events a MEMORY-ONLY floor
	// client discarded when the worker stopped (no spool to retain them —
	// typically a gated final flush's remnant). Written by the worker's
	// stop path before workerDone closes; folded into Close's verdict as
	// ErrEventsDiscarded on every Close call, so the loss can never read
	// as a clean teardown.
	closeDiscardedEvents atomic.Uint64

	// consentRecordApplyMu serializes every consent-record disk write under
	// the floor: a decision's own disk section (already serial through the
	// ticket turn) BLOCK-locks it, while an owed-record retry at a dispatch
	// point TRY-locks — an opportunistic retry must never make Track or the
	// worker wait out a stalled decision write. Because the owed slot below
	// is only mutated under this lock together with the write it describes,
	// a retry that wins the lock always re-applies the CURRENT owed decision
	// (never a superseded one over a newer record).
	consentRecordApplyMu sync.Mutex

	// consentOwedMu guards consentRecordOwed for cheap reads (the deny-proof
	// dispatch hold); mutations happen under consentRecordApplyMu too.
	consentOwedMu sync.Mutex

	// consentStampMu guards lastConsentStamp: the per-client MONOTONIC
	// decision-stamp seam (consentDecisionStamp). Same-tick decisions must
	// mint strictly increasing decided_at values or the reload's
	// strictly-newer override would miss the newest decision after a crash.
	consentStampMu   sync.Mutex
	lastConsentStamp string

	// consentRecordOwed is the floor decision whose durable record write is
	// still OWED (failed, deliberately withheld while the grant's receipt
	// trail write is itself owed — receipt-first — or a failed reload
	// heal). Carries the decision's decided-at stamp so the retried record
	// keeps the ordering instant of the decision it describes. Retried at
	// every dispatch point (retryOwedConsentRecord); while a DENIAL is
	// owed, the trail's in-scope proof receipt is held from dispatch so the
	// only durable evidence of the denial cannot prune away before the
	// record heals. Nil when nothing is owed; each new decision's slow half
	// overwrites it — which is why the outbox ALSO marks incomplete pairs
	// per receipt (consentOutbox.recordOwedKeys): the slot alone would
	// forget an older receipt's owed record when a newer decision's write
	// fails too.
	consentRecordOwed *consentOwedRecord

	// consentMintOwed is the floor decision whose RECEIPT could not be
	// minted (idempotency-key generation failed): the decision applied
	// locally, its receipt is owed — retried at every dispatch point
	// (retryOwedConsentMint), pending Close until it lands. Guarded by
	// consentOwedMu; mutations happen under consentRecordApplyMu too. Nil
	// when nothing is owed; the newest decision owns the slot (a successful
	// newer mint supersedes an owed older one — see consentOwedMint).
	consentMintOwed *consentOwedMint

	// consentMintIDFn is the receipt idempotency-key mint seam, injectable
	// so tests can exercise mint failure deterministically (nil = uuidv7).
	// Guarded by consentOwedMu.
	consentMintIDFn func() (string, error)

	// initialDeferUntil seeds the flush worker's retry-pacing deadline from
	// the spool's persisted retry_after_until_ms, so server backpressure
	// captured before a restart still holds automatic publishes for the
	// remaining window. Set before the worker starts; zero when none.
	initialDeferUntil time.Time

	// retainedRequest is the built wire request a retriably failed worker
	// publish retained alongside its in-memory batch. Owned by the flush
	// worker goroutine exclusively (every path that reads or writes it —
	// publishWorkerBatch, flushAvailable, the close remnant — runs there).
	// In-process retries rebuild THROUGH it (buildBatchIsolating) so the
	// bytes a retry puts on the wire are the bytes the failure spooled, even
	// when the caller mutates nested Props/Context values after Enqueue; it
	// is cleared whenever the batch it described is delivered, dropped, or
	// discarded.
	retainedRequest batchRequest
}

type flushRequest struct {
	ctx   context.Context
	reply chan error
}

func NewClient(cfg Config) (*Client, error) {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	client := &Client{
		cfg:               normalized,
		clock:             realClock{},
		jitter:            rand.Float64,
		queue:             newBoundedQueue(normalized.BufferSize),
		transport:         newHTTPTransport(normalized),
		pacingWake:        make(chan struct{}, 1),
		flushRequests:     make(chan flushRequest),
		stop:              make(chan struct{}),
		workerDone:        make(chan struct{}),
		consentSends:      make(chan consentRequest, consentSendBuffer),
		consentSenderDone: make(chan struct{}),
	}
	client.consentTurnCond = sync.NewCond(&client.consentTurnMu)
	client.consentGate.Store(newConsentGateState())

	// Consent-floor init runs FIRST when the floor is opted in: it resolves
	// the LIVE consent truth — the outbox reload, the identity contract, the
	// receipt trail's tail overriding (and healing) a stale decision record
	// — before ANY spool data is loaded, so initSpool below trusts the
	// RESOLVED state. The worker re-publishes loaded chunks, so seeding
	// resend work under a stale grant whose operative decision was a durable
	// denial would transmit pre-denial events (see initSpool's grant-only
	// rule under the floor).
	if normalized.ConsentFloor != nil {
		client.consentWake = make(chan struct{}, 1)
		client.initConsentFloor(os.Rename, os.Chmod)
	}
	// Spool init runs before the worker starts: it seeds initialDeferUntil
	// and the resend queue the worker consumes. Dead-letters it produced are
	// emitted only after the client is fully wired.
	var initDeadLetters []SpoolDeadLetter
	if normalized.SpoolDir != "" {
		client.spool = newDiskSpool(normalized)
		client.spool.countForeign = func(n int) {
			client.stats.spoolForeignMerged.Add(uint64(n))
		}
		initDeadLetters = client.initSpool()
	}
	if normalized.RemoteConfigURL != "" {
		client.rc = newRemoteConfigState(normalized)
		client.rc.preload()
	}

	go client.run()
	client.emitSpoolDeadLetters(initDeadLetters)
	return client, nil
}

func (c *Client) Track(ctx context.Context, event Event) error {
	c.lifecycleMu.Lock()
	if c.closed.Load() {
		c.lifecycleMu.Unlock()
		return ErrClosed
	}
	if c.consentDenied() {
		c.lifecycleMu.Unlock()
		c.stats.dropped.Add(1)
		return ErrConsentDenied
	}
	if c.consentFloorEnabled() && c.consentUndecided() {
		// The consent floor's consent-first posture: an undecided actor
		// transmits nothing (distinct refusal from a denial, so the host can
		// tell "ask the user" from "the user said no").
		c.lifecycleMu.Unlock()
		c.stats.dropped.Add(1)
		return ErrConsentUnknown
	}
	if c.consentFloorEnabled() && c.consentFloorActorMismatch(event) {
		// The floor's decision covers the CONFIGURED identity only: an event
		// overriding UserID/AnonymousID to a different effective actor would
		// transmit an actor with no local decision and no receipt. Distinct
		// refusal; per-actor decisions beyond the configured one use the
		// server-side consent path.
		c.lifecycleMu.Unlock()
		c.stats.dropped.Add(1)
		return ErrConsentActorMismatch
	}
	event, err := c.prepareEvent(event)
	if err != nil {
		c.stats.recordFailure(err)
		c.lifecycleMu.Unlock()
		return err
	}
	c.trackWG.Add(1)
	c.lifecycleMu.Unlock()
	defer c.trackWG.Done()

	if c.consentFloorEnabled() {
		// Track is a consent dispatch point: retained receipts go to the
		// transport BEFORE the event leg — and while an analytics-grant
		// receipt remains undispatched (parked in a backoff/Retry-After
		// window, or claimed by a concurrent pass), the event leg refuses
		// rather than overtake the grant on the wire. Transient: the
		// receipt dispatches on the worker cadence and the pipeline
		// reopens.
		handed, _ := c.dispatchConsentReceipts(ctx, false)
		if c.grantReceiptGateArmed(handed) {
			return ErrConsentReceiptPending
		}
	}

	err = c.publish(ctx, []Event{event})
	if errors.Is(err, ErrConsentDenied) {
		// A denial landed between Track's consent check above and the
		// publish; count the event as dropped, matching the check-time
		// denial path.
		c.stats.dropped.Add(1)
	}
	return err
}

func (c *Client) Enqueue(event Event) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	if c.closed.Load() {
		return ErrClosed
	}
	if c.consentDenied() {
		c.stats.dropped.Add(1)
		return ErrConsentDenied
	}
	if c.consentFloorEnabled() && c.consentUndecided() {
		// Consent-first floor: nothing is even queued for an undecided
		// actor. The dispatch gate, by contrast, never blocks intake — a
		// parked grant receipt holds the worker's BATCH leg, not Enqueue.
		c.stats.dropped.Add(1)
		return ErrConsentUnknown
	}
	if c.consentFloorEnabled() && c.consentFloorActorMismatch(event) {
		// Same actor contract as Track: the floor's decision covers the
		// configured identity only, so an override to a different effective
		// actor is refused at intake rather than queued for transmission.
		c.stats.dropped.Add(1)
		return ErrConsentActorMismatch
	}
	event, err := c.prepareEvent(event)
	if err != nil {
		return err
	}
	if !c.queue.enqueue(event) {
		c.stats.dropped.Add(1)
		return ErrQueueFull
	}
	c.stats.enqueued.Add(1)
	return nil
}

func (c *Client) Flush(ctx context.Context) error {
	reply := make(chan error, 1)
	request := flushRequest{ctx: ctx, reply: reply}

	select {
	case c.flushRequests <- request:
	case <-c.workerDone:
		return ErrClosed
	case <-contextDone(ctx):
		return contextCause(ctx)
	}

	select {
	case err := <-reply:
		return err
	case <-c.workerDone:
		return ErrClosed
	case <-contextDone(ctx):
		return contextCause(ctx)
	}
}

func (c *Client) Close(ctx context.Context) error {
	c.lifecycleMu.Lock()
	c.closed.Store(true)
	c.lifecycleMu.Unlock()

	if err := c.waitForTracks(ctx); err != nil {
		return err
	}
	if err := c.waitForConsentDecisions(ctx); err != nil {
		return err
	}
	return c.finishClose(ctx)
}

func (c *Client) waitForTracks(ctx context.Context) error {
	waitDone := make(chan struct{})
	go func() {
		c.trackWG.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		return nil
	case <-contextDone(ctx):
		return contextCause(ctx)
	}
}

// waitForConsentDecisions waits — bounded by the Close context — for every
// SetConsent call admitted before Close stored the closed flag to finish its
// disk work and hand its transmission to the consent sender. Without this
// fence, Close could stop and drain the sender inside a decision's window
// between the in-memory flip and the handoff: the decision would enqueue
// into a channel nobody reads again (silently never transmitted), and a
// denial's spool purge or record write could be abandoned mid-flight,
// leaving a stale granted record for the next start to trust.
func (c *Client) waitForConsentDecisions(ctx context.Context) error {
	waitDone := make(chan struct{})
	go func() {
		c.consentDecisionsWG.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		return nil
	case <-contextDone(ctx):
		return contextCause(ctx)
	}
}

func (c *Client) finishClose(ctx context.Context) error {
	c.closeMu.Lock()
	if c.closeInFlight {
		done := c.closeDone
		c.closeMu.Unlock()
		select {
		case <-done:
			c.closeMu.Lock()
			err := c.closeErr
			c.closeMu.Unlock()
			// Re-fold LATE discards (idempotent): the close that cached this
			// verdict may have abandoned the worker on its own context
			// expiry, and the stop path can count discarded remnant events
			// AFTER the verdict was cached.
			return c.closeDiscardVerdict(err)
		case <-contextDone(ctx):
			return contextCause(ctx)
		}
	}
	if c.closeComplete {
		if !errors.Is(c.closeErr, ErrConsentPending) {
			err := c.closeErr
			c.closeMu.Unlock()
			// Same late-discard re-fold as the waiter path above: a cached
			// verdict must never hide a loss counted after it was stored.
			return c.closeDiscardVerdict(err)
		}
		// The previous Close left consent receipts pending (undeliverable
		// AND not durably on disk): completion was declined so the process
		// would not silently lose them, and Close stays RETRYABLE — this
		// call re-runs the consent drain alone (the worker and sender are
		// already stopped; the drain dispatches directly on this goroutine)
		// and the verdict replaces the stored one. A discarded gated-flush
		// remnant stays folded into the fresh verdict too: a successful
		// receipt drain must not retroactively report a clean teardown over
		// events that were already lost.
		c.closeInFlight = true
		done := make(chan struct{})
		c.closeDone = done
		c.closeMu.Unlock()
		err := c.closeDiscardVerdict(c.finalizeConsentOutbox(ctx))
		c.closeMu.Lock()
		c.closeErr = err
		c.closeInFlight = false
		close(done)
		c.closeMu.Unlock()
		return err
	}
	c.closeInFlight = true
	done := make(chan struct{})
	c.closeDone = done
	c.closeMu.Unlock()

	err := c.Flush(ctx)
	c.stopOnce.Do(func() {
		close(c.stop)
	})
	select {
	case <-c.workerDone:
	case <-contextDone(ctx):
		if err == nil {
			err = contextCause(ctx)
		}
	}
	// The consent sender drains decisions still pending when c.stop closes.
	// Start it if it never ran — against a closed stop it drains and exits
	// immediately, so consentSenderDone closes either way — then wait for it
	// exactly like workerDone, so a decision recorded before Close is
	// transmitted before Close returns.
	c.startConsentSender()
	select {
	case <-c.consentSenderDone:
	case <-contextDone(ctx):
		if err == nil {
			err = contextCause(ctx)
		}
	}
	// Consent-floor drain (no-op with the floor off): ALWAYS runs, whatever
	// the event-plane outcome — deliver retained receipts, retry an owed
	// outbox write, and decline completion with ErrConsentPending when
	// undelivered receipts could not be made durable. Teardown must not
	// silently lose a consent decision's receipt, and an event-plane
	// failure (a terminal batch outcome, a context expiry, a gated flush)
	// must never mask the RETRYABLE pending state: the event outcome is
	// already settled — dropped, spooled as the close remnant, or reported —
	// while pending receipts still have a path to safety through repeated
	// Close, so the drain's verdict must stay observable. The verdicts are
	// FOLDED with errors.Join: callers see both, and errors.Is(err,
	// ErrConsentPending) keeps driving the retry branch above. The one
	// substitution: a GATED final flush's ErrConsentReceiptPending is
	// dropped in favor of the drain's verdict alone — the gate error is
	// transient bookkeeping about the same receipts the drain just settled,
	// not an event-plane outcome worth freezing.
	consentErr := c.finalizeConsentOutbox(ctx)
	if errors.Is(err, ErrConsentReceiptPending) {
		err = consentErr
	} else {
		err = errors.Join(err, consentErr)
	}
	// A floor client whose close remnant was neither delivered nor made
	// durable — discarded outright (memory-only), refused by the spool's
	// write gate, or left in a mirror whose persist still failed — reports
	// the loss on every Close: see closeDiscardVerdict. In particular a
	// successful consent drain above must not turn a lost gated remnant
	// into a nil verdict.
	err = c.closeDiscardVerdict(err)

	c.closeMu.Lock()
	c.closeErr = err
	c.closeComplete = true
	c.closeInFlight = false
	close(done)
	c.closeMu.Unlock()

	return err
}

func (c *Client) Snapshot() Stats {
	return c.stats.snapshot()
}

func (c *Client) run() {
	defer close(c.workerDone)

	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]Event, 0, c.cfg.BatchSize)
	seenConsentEpoch := c.consentEpoch.Load()
	// deferUntil is the retry-pacing deadline: after a retryable failure,
	// automatic publishes (batch-full and flush-interval ticks) hold off
	// until it passes. It is armed from the server's Retry-After hint when
	// the failure carried one, else from the client-side exponential-backoff
	// schedule (backoffDelay). Events keep accumulating in the batch and the
	// queue meanwhile — the bounded queue is the backpressure surface.
	// Explicit Flush (and therefore Close) attempts are NOT gated: they
	// carry caller intent, and a renewed failure simply re-arms the deadline.
	// It starts from the deadline the spool re-seeded from a persisted
	// retry_after_until_ms, when one was live (zero otherwise).
	deferUntil := c.initialDeferUntil
	// backoffAttempt counts consecutive retryable publish failures so the
	// hint-less fallback schedule (backoffDelay) grows per failure; a
	// successful publish resets it.
	backoffAttempt := 0
	// deferTimer wakes the worker AT the backpressure deadline, so the held
	// batch retries when the server said it may — not at the next flush tick,
	// which can be much later when FlushInterval exceeds the Retry-After. It
	// is re-armed at the top of every iteration to match the current deadline.
	var deferTimer *time.Timer
	defer func() {
		if deferTimer != nil {
			deferTimer.Stop()
		}
	}()
	for {
		hadBatch := len(batch) > 0
		batch = c.dropBatchOnConsentEpoch(batch, &seenConsentEpoch, &backoffAttempt)
		if hadBatch && len(batch) == 0 {
			// A consent denial discarded the batch the deferral was
			// protecting: clear the deadline too (the drop itself reset the
			// backoff progression), or a deny→re-grant round trip would hold
			// FRESH post-grant events behind a stale Retry-After (up to the
			// 24h clamp) with nothing left to retry.
			deferUntil = time.Time{}
		}
		if successes := c.publishSuccesses.Load(); successes != c.workerSeenSuccesses {
			// A publish succeeded since the worker's pacing state last
			// settled — typically a synchronous Track on a caller goroutine
			// while the worker waited out a deadline. The endpoint is
			// healthy again: stop waiting and restart the schedule. The
			// immediate retry below fires ONLY when the success cleared an
			// actual recovery state (an armed deadline or a failure streak
			// holding a retained batch) — during normal operation a Track
			// success must not flush healthy partial batches early and
			// defeat BatchSize/FlushInterval batching. Every retained-for-
			// retry batch has such state: a hint-less failure advances the
			// streak and a hinted one arms the deadline.
			c.workerSeenSuccesses = successes
			recovered := !deferUntil.IsZero() || backoffAttempt > 0
			deferUntil = time.Time{}
			backoffAttempt = 0
			if recovered && (len(batch) > 0 || c.spoolHasResendWork()) {
				// Retry the held batch NOW rather than at the next flush
				// tick, which can be minutes away on a long FlushInterval.
				// Requeued spooled chunks are pending retryable work too:
				// when they are ALL the recovery left behind (the held batch
				// is empty), the wake must still kick publishWorkerBatch —
				// its resend-first path — or spool-only work would idle until
				// the next tick or an explicit Flush.
				batch = c.publishWorkerBatch(batch, &seenConsentEpoch, &deferUntil, &backoffAttempt)
				continue
			}
		}
		queueEvents := c.queue.ch
		if len(batch) >= c.cfg.BatchSize {
			queueEvents = nil
		}
		var deferWake <-chan time.Time
		if !deferUntil.IsZero() {
			if remaining := c.deferRemaining(deferUntil); remaining > 0 {
				if deferTimer == nil {
					deferTimer = time.NewTimer(remaining)
				} else {
					stopAndDrainTimer(deferTimer)
					deferTimer.Reset(remaining)
				}
				deferWake = deferTimer.C
			} else {
				// The backpressure deadline elapsed while the worker was busy
				// with another case — e.g. the queue case won the select in
				// the same instant the timer fired, drained its tick, and the
				// held batch stayed below BatchSize. Retry NOW instead of
				// silently disarming and waiting for the next flush tick.
				// (publishWorkerBatch consumes the elapsed deadline.)
				if deferTimer != nil {
					stopAndDrainTimer(deferTimer)
				}
				batch = c.publishWorkerBatch(batch, &seenConsentEpoch, &deferUntil, &backoffAttempt)
				continue
			}
		} else if deferTimer != nil {
			stopAndDrainTimer(deferTimer)
		}
		select {
		case event := <-queueEvents:
			batch = append(batch, event)
			if len(batch) >= c.cfg.BatchSize && !c.publishDeferred(deferUntil) {
				batch = c.publishWorkerBatch(batch, &seenConsentEpoch, &deferUntil, &backoffAttempt)
			}
		case <-ticker.C:
			// Flush-cadence spool upkeep runs even while publishes are
			// deferred: an owed wipe and a failed record rewrite are disk
			// work, not endpoint traffic.
			c.spoolMaintain()
			if c.publishDeferred(deferUntil) {
				// The CONSENT plane is independent of the events plane's
				// pacing: retained receipts still dispatch while event
				// publishes wait out their deferral.
				c.dispatchConsentReceipts(context.Background(), false)
				continue
			}
			batch = c.publishWorkerBatch(batch, &seenConsentEpoch, &deferUntil, &backoffAttempt)
		case <-deferWake:
			// The backpressure deadline passed: retry the held batch now
			// rather than waiting out the remainder of the flush interval.
			if c.publishDeferred(deferUntil) {
				continue
			}
			batch = c.publishWorkerBatch(batch, &seenConsentEpoch, &deferUntil, &backoffAttempt)
		case <-c.pacingWake:
			// A publish succeeded elsewhere (typically a synchronous Track)
			// while the worker was parked: loop back so the pacing check at
			// the top clears any stale deferral and retries the held batch.
			continue
		case <-c.consentWake:
			// A consent decision was recorded under the floor: dispatch its
			// receipt promptly — and, with the gate possibly released, give
			// the batch leg its chance — instead of waiting for the flush
			// tick. An armed events-plane deferral is respected: only the
			// consent plane dispatches then.
			if c.publishDeferred(deferUntil) {
				c.dispatchConsentReceipts(context.Background(), false)
				continue
			}
			batch = c.publishWorkerBatch(batch, &seenConsentEpoch, &deferUntil, &backoffAttempt)
		case request := <-c.flushRequests:
			var err error
			batch, err = c.flushAvailable(request.ctx, batch, &seenConsentEpoch, &backoffAttempt)
			// Caller abandonment produces ZERO pacing side-effects — the
			// empty-batch clear included: a canceled spool-only flush comes
			// back with an empty batch while the requeued chunk is exactly
			// the work the armed deadline still protects, and clearing it
			// would retry that chunk at the next tick inside the server's
			// window.
			if !callerAbandonedFlush(request.ctx, err) {
				if len(batch) == 0 {
					// The batch the deferral was protecting is gone —
					// delivered, dropped as permanent, or discarded by a
					// consent denial. A stale deadline must not gate later
					// queued events (it could hold them up to the 24h clamp);
					// a fresh failure below re-arms it when it carries its
					// own hint.
					deferUntil = time.Time{}
				}
				c.applyRetryPacing(err, &deferUntil, &backoffAttempt)
			}
			request.reply <- err
		case <-c.stop:
			// Close already flushed; whatever is still undelivered — the
			// retained batch and any queue remainder a failing endpoint left
			// behind — is the spool's remnant (grant-gated; no-op without a
			// spool). A memory-only FLOOR client instead accounts the
			// discarded remnant so Close can report the loss.
			remnantRefused, remnantMirrored, remnantCapacityDropped := c.spoolCloseRemnant(batch)
			c.recordDiscardedCloseRemnant(batch)
			// Final settle: a record write that failed earlier (dirty mirror)
			// gets one last retry before the worker exits, whatever shape the
			// remnant took — empty, refused, or unserializable, the remnant
			// append alone cannot be relied on to reach the disk retry, and
			// exiting with a recovered-but-unwritten spool would lose events
			// that a flush-cadence tick tomorrow would have saved.
			closeCapacityDropped := remnantCapacityDropped + c.spoolMaintain()
			// AFTER the final settle (a recovered write reads as safe): a
			// FLOOR client's remnant that is still neither delivered nor
			// durable is a reportable discard, exactly like the memory-only
			// case above — Close must not read as clean over it. Capacity
			// evictions the CLOSE phase settled count in too: an eviction
			// landing at exit is a permanent loss with no later resend
			// (still-deferred evictions stayed on disk and reload).
			c.recordUnspooledCloseRemnant(remnantRefused, remnantMirrored, closeCapacityDropped)
			return
		}
	}
}

// deferRemaining is the time left until the backpressure deadline, or zero
// when no deferral is active.
func (c *Client) deferRemaining(deferUntil time.Time) time.Duration {
	if deferUntil.IsZero() {
		return 0
	}
	remaining := deferUntil.Sub(c.clock.Now())
	if remaining < 0 {
		return 0
	}
	return remaining
}

// stopAndDrainTimer stops a timer and drains an already-delivered tick so a
// later Reset never wakes on a stale expiry.
func stopAndDrainTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

// publishDeferred reports whether the server-backpressure deadline is still
// in the future, i.e. automatic publishes should hold off.
func (c *Client) publishDeferred(deferUntil time.Time) bool {
	return !deferUntil.IsZero() && c.clock.Now().Before(deferUntil)
}

// applyRetryPacing re-arms or clears the worker's retry-pacing deadline from
// a publish outcome. A retryable failure that carried a Retry-After hint sets
// the deadline to now + hint — the server's LATEST word wins, so an explicit
// "Retry-After: 0" ("retry now") replaces an earlier longer deadline and the
// worker retries immediately via the elapsed-deadline path. A retryable
// failure WITHOUT a usable hint falls back to the client-side backoff
// schedule: it advances the consecutive-failure count and re-arms the
// deadline from backoffDelay — clearing it on the first failure, whose
// schedule slot is "retry at the flush cadence" (the latest failure's word
// wins here too, so a stale longer hint from a previous attempt cannot park
// the batch) — so a sustained outage is retried with exponentially growing,
// jittered spacing instead of at a fixed cadence in lockstep across the
// fleet. A fully successful outcome clears the deadline and resets the
// backoff. A permanent failure never arms a deferral; when it is an HTTP
// response (the endpoint answered — the outage, if any, is over) it also
// resets the backoff streak, while client-side permanent errors (encode,
// consent) say nothing about the endpoint and leave pacing untouched.
func (c *Client) applyRetryPacing(err error, deferUntil *time.Time, backoffAttempt *int) {
	// This outcome is the worker's newest knowledge: consume every success
	// recorded up to now, so only a success that lands strictly AFTER this
	// settles can later clear the pacing armed below (see the run() loop's
	// publishSuccesses check).
	c.workerSeenSuccesses = c.publishSuccesses.Load()
	if err == nil {
		*deferUntil = time.Time{}
		*backoffAttempt = 0
		return
	}
	if errors.Is(err, ErrConsentReceiptPending) {
		// Consent gating is not an event-publish outcome: the batch leg was
		// HELD, never attempted, so nothing was learned about the ingest
		// endpoint. Event pacing stays untouched — the consent plane has
		// its own deferral, and feeding the gate into the event backoff
		// would keep queued events waiting behind an unrelated deferral
		// after the receipt delivers.
		return
	}
	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) && statusErr.Retryable() && statusErr.retryAfterPresent {
		retryAfter := statusErr.RetryAfter
		if retryAfter < minRetryNowSpacing {
			// "Retry now" still gets a tiny spacing floor so a server that
			// keeps answering an explicit zero cannot induce a hot retry
			// loop; 100ms is immediate in product terms.
			retryAfter = minRetryNowSpacing
		}
		*deferUntil = c.clock.Now().Add(retryAfter)
		return
	}
	if errors.Is(err, ErrConsentDenied) {
		// A denial discarded the batch mid-publish: the failure streak
		// belonged to that batch and goes with it, so fresh post-re-grant
		// events never start deep in the backoff schedule (mirrors every
		// other denial-discard site).
		*backoffAttempt = 0
		return
	}
	if isPermanentPublishError(err) {
		if statusErr != nil {
			*backoffAttempt = 0
		}
		return
	}
	*backoffAttempt++
	if delay := c.backoffDelay(*backoffAttempt); delay > 0 {
		*deferUntil = c.clock.Now().Add(delay)
	} else {
		*deferUntil = time.Time{}
	}
}

// callerAbandonedFlush reports whether a failed explicit Flush was cut short
// by its own caller's context (cancellation or deadline) rather than by the
// ingest endpoint. Only a failure that IS the caller context's OWN error
// state counts (errors.Is against ctx.Err()): a real endpoint outcome — an
// HTTP status error, a connection refusal, or even a transport timeout
// (DeadlineExceeded) under a merely CANCELED caller context — is still
// endpoint feedback and must pace. Caller abandonment is not a backpressure
// signal, so it must neither advance nor re-arm retry pacing — and it must
// not reset it either, since nothing was learned about the endpoint.
func callerAbandonedFlush(ctx context.Context, err error) bool {
	if err == nil || ctx == nil {
		return false
	}
	ctxErr := ctx.Err()
	return ctxErr != nil && errors.Is(err, ctxErr)
}

// minRetryNowSpacing floors an explicit "Retry-After: 0" so honoring the
// server's retry-now hint can never degenerate into a hot loop.
const minRetryNowSpacing = 100 * time.Millisecond

// Client-side fallback pacing for retryable publish failures that carry no
// Retry-After hint (server unreachable, or a 5xx without the header):
// exponential backoff with full jitter, mirroring the shardpilot-defold
// reference semantics. The first failure retries at the normal flush cadence
// with no extra wait; sustained failures then defer by a random duration in
// [base, ceiling], with the ceiling doubling per consecutive failure up to
// the cap and the jitter de-synchronizing clients so a recovering server
// does not face the whole fleet at once.
const (
	publishBackoffBase = time.Second
	publishBackoffCap  = 60 * time.Second
)

// backoffCeiling is the deterministic upper bound of the jitter window for
// the given consecutive-failure attempt: base·2^(attempt−2), clamped to the
// cap (the exponent is clamped too, so an arbitrarily long outage cannot
// overflow the shift). Attempts before the second have no window — the first
// failure does not defer.
func backoffCeiling(attempt int) time.Duration {
	if attempt < 2 {
		return 0
	}
	exp := attempt - 2
	if exp > 16 {
		exp = 16
	}
	ceiling := publishBackoffBase << exp
	if ceiling > publishBackoffCap {
		ceiling = publishBackoffCap
	}
	return ceiling
}

// backoffDelay is the deferral for the given consecutive-failure attempt:
// zero for the first failure (retry at the next flush cadence without an
// extra wait), then full jitter in [base, ceiling] — never below the base,
// so a dead endpoint is always given breathing room before the next probe.
func (c *Client) backoffDelay(attempt int) time.Duration {
	ceiling := backoffCeiling(attempt)
	if ceiling <= 0 {
		return 0
	}
	return publishBackoffBase + time.Duration(c.jitterValue()*float64(ceiling-publishBackoffBase))
}

// jitterValue returns a uniform value in [0, 1) from the client's jitter
// source, falling back to the shared math/rand source when none was injected
// (bare Clients constructed by tests).
func (c *Client) jitterValue() float64 {
	if c.jitter != nil {
		return c.jitter()
	}
	return rand.Float64()
}

// dropBatchOnConsentEpoch discards the worker-held batch when a consent
// denial happened since the worker last looked. Events cleared here count
// as Dropped; events drained from the shared queue are counted by
// SetConsent itself, so each event is counted exactly once. The discarded
// batch takes its backoff streak with it — fresh post-re-grant events must
// never start deep in a schedule that belonged to condemned data.
func (c *Client) dropBatchOnConsentEpoch(batch []Event, seenEpoch *uint64, backoffAttempt *int) []Event {
	epoch := c.consentEpoch.Load()
	if epoch == *seenEpoch {
		return batch
	}
	*seenEpoch = epoch
	if len(batch) > 0 {
		c.stats.dropped.Add(uint64(len(batch)))
		batch = batch[:0]
		*backoffAttempt = 0
		// The retained wire bytes described the discarded batch.
		c.retainedRequest = batchRequest{}
	}
	return batch
}

func (c *Client) flushAvailable(ctx context.Context, batch []Event, seenConsentEpoch *uint64, backoffAttempt *int) ([]Event, error) {
	var firstErr error
	for {
		batch = c.dropBatchOnConsentEpoch(batch, seenConsentEpoch, backoffAttempt)
		// Consent-floor receipts dispatch first (a dispatch point of the
		// flush) — BEFORE the denied early-return below, because receipt
		// delivery is permitted (required) while consent is denied: a
		// parked grant-then-deny trail must still drain through an explicit
		// Flush in a denied session, even though the EVENT legs refuse. The
		// drain JOINS behind a concurrent pass (Track's synchronous
		// dispatch, Close's drain) instead of skipping: Flush promised its
		// caller the receipt work ran.
		handedReceipts, receiptsDrained := c.dispatchConsentReceipts(ctx, true)
		if !receiptsDrained && c.consentOutbox.pending() {
			// The caller's context ended before the drain could run (or
			// finish): receipts remain that this flush never drained, and a
			// nil return — the denied path below returns success — would
			// silently misreport them as handled. The caller's own bound is
			// the honest verdict; the retained trail re-dispatches at every
			// later dispatch point and Close's backstop still applies.
			return batch, contextCause(ctx)
		}
		if c.consentDenied() {
			dropped := len(batch) + c.queue.drainAll()
			if dropped > 0 {
				c.stats.dropped.Add(uint64(dropped))
			}
			// A denial-drained flush discards the pipeline the streak was
			// tracking along with it — including the retained wire bytes,
			// which described the discarded batch: kept, they could resend a
			// dropped event's stale encoding under a reused event_id after a
			// later grant.
			*backoffAttempt = 0
			c.retainedRequest = batchRequest{}
			return batch[:0], firstErr
		}
		// A still-undispatched analytics-grant receipt holds the event
		// legs: the flush reports ErrConsentReceiptPending instead of
		// letting queued events overtake the grant on the wire. An empty
		// pipeline is never gated.
		if c.grantReceiptGateArmed(handedReceipts) && (len(batch) > 0 || c.spoolHasResendWork() || len(c.queue.ch) > 0) {
			return batch, ErrConsentReceiptPending
		}
		// Spooled chunks flush before the fresh batch (they are the oldest
		// undelivered work), through the same error semantics: a denial or a
		// retriable failure ends the flush, a terminal chunk failure is
		// swallowed into firstErr and the flush keeps draining.
		if err := c.flushSpooledChunks(ctx, backoffAttempt); err != nil {
			if errors.Is(err, ErrConsentDenied) {
				dropped := len(batch) + c.queue.drainAll()
				if dropped > 0 {
					c.stats.dropped.Add(uint64(dropped))
				}
				*backoffAttempt = 0
				// The retained wire bytes described the discarded batch.
				c.retainedRequest = batchRequest{}
				return batch[:0], firstErr
			}
			if !isPermanentPublishError(err) {
				return batch, err
			}
			if firstErr == nil {
				firstErr = err
			}
		}
		if len(batch) > 0 {
			// Build THROUGH the retained request: a batch a previous attempt
			// already marshaled resends its retained bytes verbatim, so the
			// in-process retry matches what that failure spooled. A member
			// that no longer serializes is dropped ALONE — settled, counted,
			// and folded into the flush's first-error the way a terminal
			// chunk failure is — and its batchmates publish on.
			request, kept, poisoned := c.buildBatchIsolating(batch, c.retainedRequest)
			if len(poisoned) > 0 {
				c.settlePoisonedEvents(poisoned)
				if firstErr == nil {
					firstErr = poisoned[0].err
				}
				batch = kept
			}
			if len(batch) == 0 {
				// Every member poisoned: nothing is left to publish or
				// retain. The queue drain below still runs — later enqueued
				// events are unaffected by the condemned batch.
				c.retainedRequest = batchRequest{}
			} else if result, err := c.publishRequestResult(ctx, request, len(batch)); err != nil {
				if errors.Is(err, ErrConsentDenied) {
					// A denial landed between this iteration's consent check
					// and the publish: drop and drain exactly like the
					// denied path above, keeping Flush's nil result. The
					// streak goes with the discarded batch.
					dropped := len(batch) + c.queue.drainAll()
					c.stats.dropped.Add(uint64(dropped))
					*backoffAttempt = 0
					c.retainedRequest = batchRequest{}
					return batch[:0], firstErr
				}
				if !isPermanentPublishError(err) {
					// Retained for the in-process retry AND spooled
					// (grant-only) so a restart resends identical bytes. A
					// caller-abandoned attempt still spools (crash
					// insurance) but leaves the persisted deadline alone.
					c.retainedRequest = request
					c.spoolFailedBatch(request, err, callerAbandonedFlush(ctx, err))
					return batch, err
				}
				if firstErr == nil {
					firstErr = err
				}
				var statusErr *HTTPStatusError
				if errors.As(err, &statusErr) {
					// A swallowed permanent HTTP response is still a
					// RESPONSE: the endpoint answered, so the hint-less
					// streak ends here even though the flush keeps
					// draining — applyRetryPacing never sees this error
					// unless it is the flush's final word (same rationale
					// as its own permanent-HTTP reset).
					*backoffAttempt = 0
				}
				c.spoolSettleTerminal(request)
				c.stats.dropped.Add(uint64(len(batch)))
				batch = batch[:0]
				c.retainedRequest = batchRequest{}
			} else {
				// EVERY successful publish ends the failure streak — a flush
				// can deliver several batches and still return a later
				// batch's error, and that error must be paced as a FIRST
				// failure, not as a continuation of a streak a mid-flush
				// success already broke. Spool settled first, callback
				// second: an OnBatchResult callback may flip consent, and
				// the resulting purge must find the delivered events already
				// acked off disk — never dead-letter them as consent drops.
				c.spoolAckWithVerdicts(request, result)
				c.notifyBatchResult(result.toPublic())
				*backoffAttempt = 0
				batch = batch[:0]
				c.retainedRequest = batchRequest{}
			}
		}
		batch = c.queue.drainInto(batch, c.cfg.BatchSize)
		if len(c.queue.ch) == 0 {
			if len(batch) == 0 {
				return batch, firstErr
			}
			continue
		}
	}
}

func (c *Client) publishWorkerBatch(batch []Event, seenConsentEpoch *uint64, deferUntil *time.Time, backoffAttempt *int) []Event {
	// Every automatic caller gates on the deadline having passed, so consume
	// it here: a follow-up failure re-arms it through applyRetryPacing (from
	// its own Retry-After hint, or the backoff schedule) instead of
	// re-firing immediately off the stale, already-elapsed deadline.
	*deferUntil = time.Time{}
	batch = c.dropBatchOnConsentEpoch(batch, seenConsentEpoch, backoffAttempt)
	// Consent-floor receipts dispatch FIRST — before every event leg, spool
	// resends included: receipts deliver under denied/unknown too, and the
	// pass's handoffs are what release the dispatch gate for this cycle
	// (no-op with the floor off).
	handedReceipts, _ := c.dispatchConsentReceipts(context.Background(), false)
	if c.grantReceiptGateArmed(handedReceipts) && (len(batch) > 0 || c.spoolHasResendWork() || len(c.queue.ch) > 0) {
		// An analytics-grant receipt is retained undispatched (parked in a
		// backoff/Retry-After window, or queued behind another receipt):
		// events sent now would overtake it on the wire and be terminally
		// suppressed on a strict-consent workspace. Hold the event legs;
		// an empty pipeline is never gated, so a retained receipt alone can
		// never wedge teardown.
		return batch
	}
	// Spooled chunks (a previous process's undelivered events) resend before
	// the fresh batch; a retriable chunk failure arms the pacing deadline
	// and the fresh batch waits behind the same gate.
	if !c.resendSpooledChunks(deferUntil, backoffAttempt) {
		return batch
	}
	if len(batch) == 0 {
		return batch
	}
	if c.consentDenied() {
		c.stats.dropped.Add(uint64(len(batch)))
		// Denial-discard: the streak belonged to the dropped batch.
		*backoffAttempt = 0
		c.retainedRequest = batchRequest{}
		return batch[:0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.HTTPTimeout)
	defer cancel()
	// Build THROUGH the retained request (see buildBatchIsolating): the prefix
	// a previous failed attempt already marshaled resends its exact bytes. A
	// member that no longer serializes is dropped alone — settled and counted
	// — and its batchmates publish on.
	request, kept, poisoned := c.buildBatchIsolating(batch, c.retainedRequest)
	if len(poisoned) > 0 {
		c.settlePoisonedEvents(poisoned)
		batch = kept
		if len(batch) == 0 {
			// Every member poisoned: nothing is left to publish or retain,
			// and nothing was learned about the endpoint, so retry pacing
			// stays untouched (the same posture applyRetryPacing takes for
			// client-side permanent errors).
			c.retainedRequest = batchRequest{}
			return batch[:0]
		}
	}
	result, err := c.publishRequestResult(ctx, request, len(batch))
	c.applyRetryPacing(err, deferUntil, backoffAttempt)
	if err != nil {
		if isPermanentPublishError(err) {
			// A terminal outcome settles any previously spooled copies of
			// these events too — a poison batch must not re-fail every
			// launch.
			c.spoolSettleTerminal(request)
			c.stats.dropped.Add(uint64(len(batch)))
			c.retainedRequest = batchRequest{}
			return batch[:0]
		}
		// Retriable: the batch is retained in memory for the in-process
		// retry — together with its built wire bytes — and spooled
		// (grant-only) as crash insurance so a restart resends the identical
		// bytes. The worker publishes under its own internal context, so
		// caller abandonment never applies here.
		c.retainedRequest = request
		c.spoolFailedBatch(request, err, false)
		return batch
	}
	// Spool settled first, callback second (see flushAvailable's success
	// path for the rationale).
	c.spoolAckWithVerdicts(request, result)
	c.notifyBatchResult(result.toPublic())
	c.retainedRequest = batchRequest{}
	return batch[:0]
}

func (c *Client) publish(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	return c.publishBatchWithContext(ctx, events)
}

func (c *Client) publishBatchWithContext(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	request, err := c.buildBatch(events)
	if err != nil {
		c.recordBuildFailure(err)
		return err
	}
	return c.publishRequest(ctx, request, len(events))
}

// recordBuildFailure records a batch-build failure with the same observable
// behavior the transport-level encode path produced before envelopes were
// marshaled at build time: every failure counts a failed batch, and an
// encode failure additionally logs (a validation failure never did).
func (c *Client) recordBuildFailure(err error) {
	c.stats.recordFailure(err)
	var encodeErr *EncodeError
	if errors.As(err, &encodeErr) {
		c.logf("shardpilot batch publish failed: %v", err)
	}
}

// settlePoisonedEvents drops the members a worker-path batch build could not
// serialize, attributed per event: each is counted Dropped and logged with
// its id, and the attempt records ONE build failure — the first error — on
// the same FailedBatches/LastError surface the whole-batch drop used to
// feed. Any previously spooled copy settles off the record as terminal;
// under today's invariants a poisoned member can never HAVE one (its bytes
// never marshaled, and a member whose bytes were retained rides the reuse
// prefix), so that settle is insurance: if the invariant ever drifts,
// counted-dropped must still equal gone-from-disk.
func (c *Client) settlePoisonedEvents(poisoned []poisonedEvent) {
	if len(poisoned) == 0 {
		return
	}
	c.stats.dropped.Add(uint64(len(poisoned)))
	c.stats.recordFailure(poisoned[0].err)
	ids := make([]string, 0, len(poisoned))
	for _, poison := range poisoned {
		ids = append(ids, poison.id)
		c.logf("shardpilot batch build: event %q could not be serialized and was dropped (its batchmates were not): %v", poison.id, poison.err)
	}
	c.spoolSettleTerminalIDs(ids)
}

// publishRequest publishes one already-built batch request — fresh events
// (typed envelopes plus their retained wire bytes) or a spooled resend
// chunk (raw bytes only) — through the consent gate, transport, and stats
// plumbing shared by every publish path. Synchronous single-shot batches
// (Track) have no spooled copies to settle, so the success callback fires
// immediately here.
func (c *Client) publishRequest(ctx context.Context, request batchRequest, size int) error {
	result, err := c.publishRequestResult(ctx, request, size)
	if err == nil {
		c.notifyBatchResult(result.toPublic())
	}
	return err
}

// publishRequestResult is publishRequest returning the decoded batch response
// as well: the spool's resend settle needs the per-event verdicts to tell
// confirmed-delivered events from per-event terminal outcomes. It does NOT
// invoke OnBatchResult — each successful caller notifies AFTER settling the
// spool for the outcome, so user code observing a delivery never runs while
// the delivered events still sit in the record, where a callback-driven
// consent flip would purge (and dead-letter) entries the 202 already
// settled.
func (c *Client) publishRequestResult(ctx context.Context, request batchRequest, size int) (batchResult, error) {
	ctx, cancel := contextWithDefaultTimeout(ctx, c.cfg.HTTPTimeout)
	defer cancel()

	// Load the denial gate BEFORE re-checking consent: a denial completed
	// after the load cancels the loaded gate (aborting the in-flight HTTP
	// request mid-transfer), while a denial completed before it stored the
	// denied state first and is caught by the re-check below. Either way no
	// event publish can run to completion past a completed denial.
	gate := c.consentGate.Load()
	if gate != nil {
		var cancelOnDenial context.CancelFunc
		ctx, cancelOnDenial = context.WithCancel(ctx)
		defer cancelOnDenial()
		stop := context.AfterFunc(gate.ctx, cancelOnDenial)
		defer stop()
	}
	if c.consentDenied() {
		// Not a transport failure: callers count the batch as Dropped,
		// matching their own pre-publish denial paths.
		return batchResult{}, ErrConsentDenied
	}

	result, err := c.transport.Publish(ctx, request)
	if err != nil {
		if gate != nil && gate.ctx.Err() != nil && errors.Is(err, context.Canceled) {
			// THIS publish's gate was cancelled, so a consent denial aborted
			// the request mid-flight. Map the cancellation to ErrConsentDenied
			// regardless of the CURRENT consent state: a quick re-grant can
			// land before the transport returns, leaving consentDenied()
			// false, but the aborted batch must still count as Dropped and
			// never as a failed batch (callers treat ErrConsentDenied exactly
			// like their pre-publish denial paths). A CALLER-context
			// cancellation is never reclassified — it leaves this gate's
			// context intact, so gate.ctx.Err() stays nil unless a denial
			// actually happened during this publish.
			return batchResult{}, ErrConsentDenied
		}
		c.stats.recordFailure(err)
		if isSchemaRevisionMismatch(err) {
			// Terminal for the batch (isPermanentPublishError routes it to
			// the drop path, never a retry): this build's declared schema
			// revision no longer matches what the ingest service serves.
			c.logf("shardpilot batch publish rejected: schema revision mismatch (this build declares %q); batch dropped as terminal — rebuild against the server's current schema set, override Config.SchemaRevision, or set Config.DisableSchemaRevision to stop declaring: %v",
				effectiveSchemaRevision(c.cfg), err)
		} else {
			c.logf("shardpilot batch publish failed: %v", err)
		}
		return batchResult{}, err
	}
	c.stats.recordBatch(result, size)
	if c.spool != nil {
		// ANY successful publish proves the server's backpressure window
		// over: a persisted Retry-After deadline surviving it would defer the
		// next start's publishes for a window that already ended.
		if c.spool.clearRetryDeadline() {
			c.recordSpoolPersistFailure()
		}
		c.drainSpoolCapacityDrops()
	}
	// Signal the success to the worker's pacing state: a synchronous Track
	// (or any other path) proving the endpoint healthy must clear a stale
	// retry deadline instead of letting it hold automatic publishes. The
	// non-blocking nudge wakes a parked worker so the held batch retries
	// promptly instead of at the next flush tick.
	c.publishSuccesses.Add(1)
	select {
	case c.pacingWake <- struct{}{}:
	default:
	}
	return result, nil
}

// notifyBatchResult invokes the optional OnBatchResult callback with the
// publish outcome, guarding against a panic in user code so a buggy callback
// cannot take down the background flush worker.
func (c *Client) notifyBatchResult(result BatchResult) {
	if c.cfg.OnBatchResult == nil {
		return
	}
	defer func() { _ = recover() }()
	c.cfg.OnBatchResult(result)
}

func contextDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

func contextCause(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	return ctx.Err()
}

func cloneEvent(event Event) Event {
	event.Props = cloneMap(event.Props)
	event.Context = cloneMap(event.Context)
	return event
}

// prepareEvent validates and normalizes an event at intake. The event id and
// timestamp are stamped HERE, exactly once, so every later publish attempt of
// the same event ships the identical envelope: the ingest service
// de-duplicates re-sends by event_id, which only works when a retry reuses
// the id of the attempt it repeats (a per-attempt id would turn a
// delivered-but-timed-out batch into permanent duplicates).
func (c *Client) prepareEvent(event Event) (Event, error) {
	event = cloneEvent(event)
	if strings.TrimSpace(event.Name) == "" {
		return Event{}, fmt.Errorf("%w: event name is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(event.ID) == "" {
		id, err := newEventID()
		if err != nil {
			return Event{}, fmt.Errorf("%w: generate event id: %v", ErrInvalidEvent, err)
		}
		event.ID = id
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = c.clock.Now()
	}
	return event, nil
}

// TODO: partial-batch acceptance. A permanent 4xx (e.g. one invalid event)
// currently drops the whole batch, discarding the events the server would
// still accept. Once the ingest envelope reports which events failed on a
// 4xx (the same per-event statuses BatchResult already exposes on a 202),
// retain and re-publish the still-valid events instead of dropping them all.
func isPermanentPublishError(err error) bool {
	if errors.Is(err, ErrInvalidEvent) {
		return true
	}
	// A consent denial that raced the publish start: retrying would only
	// re-reject, and the events must be dropped (never published).
	if errors.Is(err, ErrConsentDenied) {
		return true
	}
	// An enforce-mode schema-revision-mismatch 409 is terminal by contract:
	// the server sends no Retry-After because re-sending the same batch from
	// the same build can never succeed — only a rebuild against the current
	// schema set (or stop declaring via Config.DisableSchemaRevision) clears
	// it. The generic non-retryable branch below already drops it today; this
	// explicit branch pins that routing so no future 409 handling — e.g. the
	// partial-batch/split work in the TODO above, which the two
	// workspace-conflict 409 codes could legitimately feed — ever retries or
	// splits a schema-revision mismatch.
	if isSchemaRevisionMismatch(err) {
		return true
	}
	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) {
		return !statusErr.Retryable()
	}
	var encodeErr *EncodeError
	if errors.As(err, &encodeErr) {
		return true
	}
	return false
}
