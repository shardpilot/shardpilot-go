package shardpilot

import (
	"context"
	"errors"
	"fmt"
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

	flushRequests chan flushRequest
	stop          chan struct{}
	workerDone    chan struct{}
	lifecycleMu   sync.Mutex
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
		queue:             newBoundedQueue(normalized.BufferSize),
		transport:         newHTTPTransport(normalized),
		flushRequests:     make(chan flushRequest),
		stop:              make(chan struct{}),
		workerDone:        make(chan struct{}),
		consentSends:      make(chan consentRequest, consentSendBuffer),
		consentSenderDone: make(chan struct{}),
	}
	client.consentGate.Store(newConsentGateState())

	go client.run()
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
	event, err := c.prepareEvent(event)
	if err != nil {
		c.stats.recordFailure(err)
		c.lifecycleMu.Unlock()
		return err
	}
	c.trackWG.Add(1)
	c.lifecycleMu.Unlock()
	defer c.trackWG.Done()

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

func (c *Client) finishClose(ctx context.Context) error {
	c.closeMu.Lock()
	if c.closeComplete {
		err := c.closeErr
		c.closeMu.Unlock()
		return err
	}
	if c.closeInFlight {
		done := c.closeDone
		c.closeMu.Unlock()
		select {
		case <-done:
			c.closeMu.Lock()
			err := c.closeErr
			c.closeMu.Unlock()
			return err
		case <-contextDone(ctx):
			return contextCause(ctx)
		}
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
	// deferUntil is the server-backpressure deadline: after a retryable
	// failure that carried a Retry-After hint, automatic publishes (batch-full
	// and flush-interval ticks) hold off until it passes. Events keep
	// accumulating in the batch and the queue meanwhile — the bounded queue is
	// the backpressure surface. Explicit Flush (and therefore Close) attempts
	// are NOT gated: they carry caller intent, and a renewed failure simply
	// re-arms the deadline.
	var deferUntil time.Time
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
		batch = c.dropBatchOnConsentEpoch(batch, &seenConsentEpoch)
		if hadBatch && len(batch) == 0 {
			// A consent denial discarded the batch the deferral was
			// protecting: clear the deadline too, or a deny→re-grant round
			// trip would hold FRESH post-grant events behind a stale
			// Retry-After (up to the 24h clamp) with nothing left to retry.
			deferUntil = time.Time{}
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
				batch = c.publishWorkerBatch(batch, &seenConsentEpoch, &deferUntil)
				continue
			}
		} else if deferTimer != nil {
			stopAndDrainTimer(deferTimer)
		}
		select {
		case event := <-queueEvents:
			batch = append(batch, event)
			if len(batch) >= c.cfg.BatchSize && !c.publishDeferred(deferUntil) {
				batch = c.publishWorkerBatch(batch, &seenConsentEpoch, &deferUntil)
			}
		case <-ticker.C:
			if c.publishDeferred(deferUntil) {
				continue
			}
			batch = c.publishWorkerBatch(batch, &seenConsentEpoch, &deferUntil)
		case <-deferWake:
			// The backpressure deadline passed: retry the held batch now
			// rather than waiting out the remainder of the flush interval.
			if c.publishDeferred(deferUntil) {
				continue
			}
			batch = c.publishWorkerBatch(batch, &seenConsentEpoch, &deferUntil)
		case request := <-c.flushRequests:
			var err error
			batch, err = c.flushAvailable(request.ctx, batch, &seenConsentEpoch)
			if len(batch) == 0 {
				// The batch the deferral was protecting is gone — delivered,
				// dropped as permanent, or discarded by a consent denial. A
				// stale deadline must not gate later queued events (it could
				// hold them up to the 24h clamp); a fresh failure below
				// re-arms it when it carries its own hint.
				deferUntil = time.Time{}
			}
			c.applyRetryAfter(err, &deferUntil)
			request.reply <- err
		case <-c.stop:
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

// applyRetryAfter re-arms or clears the worker's backpressure deadline from a
// publish outcome: a retryable failure that carried a Retry-After hint sets
// the deadline to now + hint — the server's LATEST word wins, so an explicit
// "Retry-After: 0" ("retry now") replaces an earlier longer deadline and the
// worker retries immediately via the elapsed-deadline path. A fully
// successful outcome clears the deadline; a failure without a usable hint
// leaves it untouched.
func (c *Client) applyRetryAfter(err error, deferUntil *time.Time) {
	if err == nil {
		*deferUntil = time.Time{}
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
	}
}

// minRetryNowSpacing floors an explicit "Retry-After: 0" so honoring the
// server's retry-now hint can never degenerate into a hot loop.
const minRetryNowSpacing = 100 * time.Millisecond

// dropBatchOnConsentEpoch discards the worker-held batch when a consent
// denial happened since the worker last looked. Events cleared here count
// as Dropped; events drained from the shared queue are counted by
// SetConsent itself, so each event is counted exactly once.
func (c *Client) dropBatchOnConsentEpoch(batch []Event, seenEpoch *uint64) []Event {
	epoch := c.consentEpoch.Load()
	if epoch == *seenEpoch {
		return batch
	}
	*seenEpoch = epoch
	if len(batch) > 0 {
		c.stats.dropped.Add(uint64(len(batch)))
		batch = batch[:0]
	}
	return batch
}

func (c *Client) flushAvailable(ctx context.Context, batch []Event, seenConsentEpoch *uint64) ([]Event, error) {
	var firstErr error
	for {
		batch = c.dropBatchOnConsentEpoch(batch, seenConsentEpoch)
		if c.consentDenied() {
			dropped := len(batch) + c.queue.drainAll()
			if dropped > 0 {
				c.stats.dropped.Add(uint64(dropped))
			}
			return batch[:0], firstErr
		}
		if len(batch) > 0 {
			if err := c.publishBatchWithContext(ctx, batch); err != nil {
				if errors.Is(err, ErrConsentDenied) {
					// A denial landed between this iteration's consent check
					// and the publish: drop and drain exactly like the
					// denied path above, keeping Flush's nil result.
					dropped := len(batch) + c.queue.drainAll()
					c.stats.dropped.Add(uint64(dropped))
					return batch[:0], firstErr
				}
				if !isPermanentPublishError(err) {
					return batch, err
				}
				if firstErr == nil {
					firstErr = err
				}
				c.stats.dropped.Add(uint64(len(batch)))
				batch = batch[:0]
			} else {
				batch = batch[:0]
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

func (c *Client) publishWorkerBatch(batch []Event, seenConsentEpoch *uint64, deferUntil *time.Time) []Event {
	// Every automatic caller gates on the deadline having passed, so consume
	// it here: a follow-up failure WITHOUT a fresh Retry-After then falls
	// back to the normal tick cadence instead of re-firing immediately off
	// the stale, already-elapsed deadline (applyRetryAfter re-arms it when
	// the new failure carries its own hint).
	*deferUntil = time.Time{}
	batch = c.dropBatchOnConsentEpoch(batch, seenConsentEpoch)
	if len(batch) == 0 {
		return batch
	}
	if c.consentDenied() {
		c.stats.dropped.Add(uint64(len(batch)))
		return batch[:0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.HTTPTimeout)
	defer cancel()
	if err := c.publishBatchWithContext(ctx, batch); err != nil {
		c.applyRetryAfter(err, deferUntil)
		if isPermanentPublishError(err) {
			c.stats.dropped.Add(uint64(len(batch)))
			return batch[:0]
		}
		return batch
	}
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
		return ErrConsentDenied
	}

	request, err := c.buildBatch(events)
	if err != nil {
		c.stats.recordFailure(err)
		return err
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
			return ErrConsentDenied
		}
		c.stats.recordFailure(err)
		if c.cfg.Logger != nil {
			c.cfg.Logger.Printf("shardpilot batch publish failed: %v", err)
		}
		return err
	}
	c.stats.recordBatch(result, len(events))
	c.notifyBatchResult(result.toPublic())
	return nil
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
