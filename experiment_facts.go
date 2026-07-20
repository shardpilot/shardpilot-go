package shardpilot

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
)

// Experiment exposure/outcome fact producers (the analytics half of the
// consumer in experiments.go): the two runtime experiment facts, emitted
// THROUGH the client's existing analytics pipeline — the bounded queue, the
// flush worker, the spool, and the consent gates — so they inherit exactly
// the consent posture the integrator configured. No new network path, no
// new queue, no consent bypass.
//
// Wire contract (analytics ingest, strict):
//   - names `experiment_exposure` / `experiment_outcome`;
//   - source is ALWAYS "client" (these are runtime client facts, whatever
//     Config.Source says otherwise);
//   - user_id is ALWAYS omitted; anonymous_id is REQUIRED and carries the
//     SDK's standard Config.AnonymousID — that identity is what makes the
//     GDPR erasure cascade reach the fact. A client with no configured
//     AnonymousID cannot build an in-contract fact and skips it terminally
//     (diagnosed);
//   - props are the exact allowlist and nothing else: experiment_key,
//     experiment_version, assignment_key, variant_key, assignment_unit
//     (plus outcome_key/outcome_value on the outcome). The assignment_key
//     prop carries the SERVER-MINTED subject-fact key VERBATIM; the
//     SDK-minted spcid subject id never rides an analytics fact — an
//     assignment without a subject-fact key (a synthetic-unit answer
//     included) emits NO fact.
//
// Emission timing (exposures): at most once per (experiment, version,
// subject) per session — this SDK's session is the client instance — with a
// DETERMINISTIC event id (experimentExposureEventID) so at-least-once
// retries and same-session re-emissions collapse server-side as duplicates.
// Owed emissions (a full queue, a consent-closed window, a cache-restored
// application) stay armed as snapshots and drain on the lane's sweep in
// FIFO order per experiment. TrackExperimentExposure is the explicit
// re-arm: it buys an EXTRA fact with a bumped arm counter — while the
// automatic arm-0 emission is still owed in the queue, the re-arm takes arm
// 1 and the owed snapshot keeps its arm 0, so BOTH facts emit with distinct
// deterministic ids.

// experimentConsentRefusal is the plane's consent gate: the SAME effective
// consent state the analytics path enforces, composed identically — denial
// (either flavor, the forced-minor state included) refuses everywhere;
// unknown refuses under the opt-in ConsentFloor and admits without it
// (this SDK's documented open-under-unknown posture). Nothing separate is
// computed. nil = admitted.
func (c *Client) experimentConsentRefusal() error {
	if c.consentDenied() {
		return ErrConsentDenied
	}
	if c.consentFloorEnabled() && c.consentUndecided() {
		return ErrConsentUnknown
	}
	return nil
}

// enqueueExperimentFact is the internal fact intake: the analytics queue
// with the same gates Enqueue applies — lifecycle, consent, preparation —
// minus the floor's actor-mismatch check, which reads per-event identity
// OVERRIDES and does not apply here: an experiment fact carries the
// configured identity by construction (anonymous_id = Config.AnonymousID;
// user_id omitted on the wire by contract, not as an actor change).
// atClose admits the fact past the closed gate: Close's last-chance sweep
// runs after the closed store, and its facts ride the final flush.
func (c *Client) enqueueExperimentFact(event Event, atClose bool) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if !atClose && c.closed.Load() {
		return ErrClosed
	}
	// Consent refusals here are RETRYABLE for the fact lane (an exposure
	// snapshot stays owed and re-emits; the caller sees the refusal), so
	// they deliberately do NOT count in Stats.Dropped — only terminal
	// outcomes drop facts, and a re-armed snapshot retried across a
	// consent-closed window must not inflate the counter per attempt.
	if c.consentDenied() {
		return ErrConsentDenied
	}
	if c.consentFloorEnabled() && c.consentUndecided() {
		return ErrConsentUnknown
	}
	event, err := c.prepareEvent(event)
	if err != nil {
		return err
	}
	if !c.queue.enqueue(event) {
		return ErrQueueFull
	}
	c.stats.enqueued.Add(1)
	return nil
}

// buildExperimentFactEvent assembles one strict-allowlist experiment fact
// from a cached entry. The subject-fact key is the fact's assignment_key —
// enforced here so the raw spcid subject id can never leave the SDK in
// experiment props. eventID, when non-empty, presets the deterministic
// exposure id; empty lets the pipeline mint a fresh one (outcomes).
func (c *Client) buildExperimentFactEvent(name, experimentKey string, entry *expEntry, eventID string) (Event, string) {
	factKey := strings.TrimSpace(entry.SubjectFactKey)
	if !expSubjectFactKeyPattern.MatchString(factKey) {
		// The privacy boundary of the fact lane: ONLY a grammar-valid
		// server-minted sfk1_ key may ride assignment_key. Absent AND
		// malformed values (a raw spcid_ echo included) alike mean this
		// assignment produces no fact.
		return Event{}, "exposure_no_subject_fact_key"
	}
	if c.cfg.AnonymousID == "" {
		// The ingest contract requires the SDK client identity as
		// anonymous_id on experiment facts (erasure reachability): with
		// none configured the fact cannot be built in-contract.
		return Event{}, "exposure_no_anonymous_id"
	}
	props := map[string]any{
		"experiment_key":     experimentKey,
		"experiment_version": entry.Version,
		"assignment_key":     factKey,
		"variant_key":        entry.VariantKey,
		"assignment_unit":    entry.AssignmentUnit,
	}
	return Event{
		ID:          eventID,
		Name:        name,
		AnonymousID: c.cfg.AnonymousID,
		Props:       props,
		// Envelope contract for experiment facts: source "client" and no
		// user_id, whatever the client configuration would default.
		omitUserID:     true,
		sourceOverride: SourceClient,
	}, ""
}

// emitEntryExposure emits one exposure fact for one applied-entry snapshot.
// Callers hold emitMu (never e.mu). Returns:
//   - ok=true                — emitted, or already emitted this session;
//   - ok=false, terminal     — terminally skipped (no server-safe fact key,
//     no anonymous id); the snapshot leaves the queue, diagnosed;
//   - ok=false, !terminal    — retryable (consent closed, queue full): an
//     armed snapshot stays for a later sweep.
//
// sessionMarker is the marker of the SESSION THE APPLICATION BELONGS TO (an
// owed snapshot carries its own; "" means the current session): the
// deterministic id derives from it, and the current session's dedup
// bookkeeping (exposed) is consulted and updated ONLY for current-session
// emissions.
//
// Arm accounting: exposed[tuple] records the highest arm handed out and
// whether the AUTOMATIC arm-0 fact has emitted. An explicit re-arm that
// runs while that automatic emission is still owed in the queue takes arm 1
// and leaves the owed snapshot its arm 0 — the re-arm buys an EXTRA fact,
// never the owed one's slot — and the later sweep emits the arm 0 exactly
// once.
func (c *Client) emitEntryExposure(experimentKey string, entry *expEntry, rearm bool, sessionMarker string, atClose bool) (ok bool, code string, terminal bool) {
	if err := c.experimentConsentRefusal(); err != nil {
		return false, consentRefusalCode(err), false
	}
	e := c.exp

	e.mu.Lock()
	purgeEpoch := e.purgeEpoch
	marker := sessionMarker
	if marker == "" {
		marker = e.sessionMarker
	}
	currentSession := marker == e.sessionMarker
	tuple := exposureTupleKey(experimentKey, entry)
	exposed, haveExposed := expExposed{}, false
	if currentSession {
		exposed, haveExposed = e.exposed[tuple]
	}
	if haveExposed && !rearm && exposed.auto {
		e.mu.Unlock()
		return true, "", false
	}
	var arm int64
	var next expExposed
	switch {
	case rearm && haveExposed:
		arm = exposed.arm + 1
		next = expExposed{arm: arm, auto: exposed.auto}
	case rearm && e.owedTupleArmedLocked(experimentKey, tuple):
		// The automatic emission is still owed in the queue: the explicit
		// re-arm counts as the EXTRA fact on top of it.
		arm = 1
		next = expExposed{arm: 1, auto: false}
	case rearm:
		next = expExposed{arm: 0, auto: true}
	default:
		// The automatic emission: arm 0 by definition. Reachable with
		// exposed already set only while that arm-0 fact was owed behind
		// explicit re-arms — emitting it completes the auto slot without
		// lowering the recorded highest arm.
		if haveExposed {
			next = expExposed{arm: exposed.arm, auto: true}
		} else {
			next = expExposed{arm: 0, auto: true}
		}
	}
	e.mu.Unlock()

	eventID := experimentExposureEventID(marker, entry.SubjectKey, experimentKey, entry.Version, arm)
	event, skipCode := c.buildExperimentFactEvent(experimentExposureName, experimentKey, entry, eventID)
	if skipCode != "" {
		c.logf("shardpilot experiments: exposure for experiment %q skipped (%s)", experimentKey, skipCode)
		return false, skipCode, true
	}
	if err := c.enqueueExperimentFact(event, atClose); err != nil {
		return false, err.Error(), false
	}
	if currentSession {
		e.mu.Lock()
		if e.purgeEpoch == purgeEpoch {
			e.exposed[tuple] = next
		}
		// A purge raced this emission: its drain may have wiped the queued
		// fact, and its re-arm must stand — the sweep re-emits the tuple
		// with the SAME deterministic id, so a fact that DID survive (or
		// had already published) collapses server-side as a duplicate.
		e.mu.Unlock()
	}
	return true, "", false
}

func consentRefusalCode(err error) string {
	if err == ErrConsentUnknown {
		return "consent_unknown"
	}
	return "consent_denied"
}

// sweepExperimentExposures drains one experiment's owed-exposure queue in
// order: emitted and terminally skipped snapshots leave the queue; a
// retryable failure (queue full, consent closed) stops the drain and keeps
// the remainder armed for a later sweep, so an older application's fact is
// never leapfrogged or lost.
func (c *Client) sweepExperimentExposures(experimentKey string) {
	c.sweepExperimentExposuresMode(experimentKey, false)
}

func (c *Client) sweepExperimentExposuresMode(experimentKey string, atClose bool) {
	e := c.exp
	e.emitMu.Lock()
	defer e.emitMu.Unlock()
	for {
		e.mu.Lock()
		list := e.pendingExposure[experimentKey]
		if len(list) == 0 {
			delete(e.pendingExposure, experimentKey)
			e.mu.Unlock()
			return
		}
		head := list[0]
		// Copy the snapshot's fields UNDER the lock: armExposureLocked
		// refreshes a same-(session, tuple) tail snapshot in place, so
		// reading head.entry/head.session after the unlock races that
		// write. The copies stay a consistent pair; a refresh that lands
		// mid-emission is tuple-identical by construction (the refresh
		// gate), so emitting the older copy derives the same deterministic
		// id and the identity-based removal below is untouched.
		headEntry, headSession := head.entry, head.session
		e.mu.Unlock()
		ok, _, terminal := c.emitEntryExposure(experimentKey, headEntry, false, headSession, atClose)
		if !ok && !terminal {
			return
		}
		e.mu.Lock()
		// Remove the settled head — by identity, not position: an arm
		// racing this sweep may have appended, never removed or reordered.
		list = e.pendingExposure[experimentKey]
		if len(list) > 0 && list[0] == head {
			e.pendingExposure[experimentKey] = list[1:]
		}
		e.mu.Unlock()
	}
}

// sweepAllExperimentExposures drains every experiment's owed exposures in
// per-experiment order. Callers gate on consent; the per-snapshot emit
// re-checks it anyway (a mid-sweep revocation stops the drain retryably).
func (c *Client) sweepAllExperimentExposures() {
	c.sweepAllExperimentExposuresMode(false)
}

func (c *Client) sweepAllExperimentExposuresMode(atClose bool) {
	e := c.exp
	e.mu.Lock()
	keys := make([]string, 0, len(e.pendingExposure))
	for key := range e.pendingExposure {
		keys = append(keys, key)
	}
	e.mu.Unlock()
	sort.Strings(keys)
	for _, key := range keys {
		c.sweepExperimentExposuresMode(key, atClose)
	}
}

// TrackExperimentExposure emits one EXTRA exposure fact for the cached
// assignment (a distinct deterministic id per re-arm), for hosts that want
// re-exposure semantics on top of the automatic once-per-session emission —
// the automatic fact emits at the assignment's application (fetch
// resolution or cache restore) without any host call. Requires the
// experiments opt-in (ErrExperimentsNotConfigured), an assignment currently
// served (ErrExperimentNoAssignment), the plane's consent admission
// (ErrConsentDenied/ErrConsentUnknown), and a server-minted subject-fact
// key on the assignment (ErrExperimentFactUnavailable — the SDK subject id
// never rides analytics facts). ErrQueueFull reports backpressure: the
// re-arm did not consume its slot and the call can be retried.
func (c *Client) TrackExperimentExposure(experimentKey string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	e := c.exp
	if e == nil {
		return ErrExperimentsNotConfigured
	}
	experimentKey = strings.TrimSpace(experimentKey)
	if experimentKey == "" {
		return fmt.Errorf("%w: experiment key is required", ErrInvalidExperimentFact)
	}
	// The consent gate comes first (canonical order): a refused plane
	// reports its refusal, not the cache state behind it.
	if err := c.experimentConsentRefusal(); err != nil {
		return err
	}
	e.emitMu.Lock()
	defer e.emitMu.Unlock()
	e.mu.Lock()
	if e.tornDown {
		e.mu.Unlock()
		return ErrClosed
	}
	// The explicit re-arm targets the LIVE assignment only.
	entry := e.entries[experimentKey]
	e.mu.Unlock()
	if entry == nil {
		return ErrExperimentNoAssignment
	}
	ok, code, terminal := c.emitEntryExposure(experimentKey, entry, true, "", false)
	if ok {
		return nil
	}
	return experimentFactError(code, terminal)
}

// TrackExperimentOutcome emits one experiment_outcome fact — the measured
// outcome for the cached assignment — through the analytics pipeline.
// outcomeValue must be a finite number. Each admitted call is a distinct
// fact (a fresh event id); outcomes are never deduplicated. The refusal
// surface matches TrackExperimentExposure, plus ErrInvalidExperimentFact
// for an empty outcome key or a non-finite value.
func (c *Client) TrackExperimentOutcome(experimentKey, outcomeKey string, outcomeValue float64) error {
	if c.closed.Load() {
		return ErrClosed
	}
	e := c.exp
	if e == nil {
		return ErrExperimentsNotConfigured
	}
	experimentKey = strings.TrimSpace(experimentKey)
	outcomeKey = strings.TrimSpace(outcomeKey)
	if experimentKey == "" {
		return fmt.Errorf("%w: experiment key is required", ErrInvalidExperimentFact)
	}
	if outcomeKey == "" {
		return fmt.Errorf("%w: outcome key is required", ErrInvalidExperimentFact)
	}
	if math.IsNaN(outcomeValue) || math.IsInf(outcomeValue, 0) {
		return fmt.Errorf("%w: outcome value must be a finite number", ErrInvalidExperimentFact)
	}
	if err := c.experimentConsentRefusal(); err != nil {
		return err
	}
	// The emit lock serializes the outcome with a real-subjects sentinel
	// purge exactly like exposures: without it this path could read a live
	// entry, lose the race to purgeWithdrawnExperimentFacts, and enqueue a
	// withdrawn subject-fact key AFTER the queue filter ran. Under the
	// lock the outcome either enqueues before the filter (and is caught)
	// or reads the already-cleared cache after it (and refuses).
	e.emitMu.Lock()
	defer e.emitMu.Unlock()
	e.mu.Lock()
	if e.tornDown {
		e.mu.Unlock()
		return ErrClosed
	}
	entry := e.entries[experimentKey]
	e.mu.Unlock()
	if entry == nil {
		return ErrExperimentNoAssignment
	}
	event, skipCode := c.buildExperimentFactEvent(experimentOutcomeName, experimentKey, entry, "")
	if skipCode != "" {
		return ErrExperimentFactUnavailable
	}
	event.Props["outcome_key"] = outcomeKey
	event.Props["outcome_value"] = outcomeValue
	return c.enqueueExperimentFact(event, false)
}

// experimentFactError maps an emit refusal code back to the public error
// surface.
func experimentFactError(code string, terminal bool) error {
	switch code {
	case "consent_denied":
		return ErrConsentDenied
	case "consent_unknown":
		return ErrConsentUnknown
	}
	if terminal {
		return ErrExperimentFactUnavailable
	}
	switch code {
	case ErrQueueFull.Error():
		return ErrQueueFull
	case ErrClosed.Error():
		return ErrClosed
	}
	return fmt.Errorf("%w: %s", ErrInvalidExperimentFact, code)
}

// isWithdrawnExperimentFactEvent recognizes one of this SDK's own
// experiment facts carrying a server-minted subject fact key — the exact
// class the real-subjects sentinel withdraws from delivery. Host events
// merely sharing a name are not matched (they cannot carry a grammar-valid
// sfk1_ assignment key they never had).
func isWithdrawnExperimentFactEvent(event Event) bool {
	if event.Name != experimentExposureName && event.Name != experimentOutcomeName {
		return false
	}
	key, _ := event.Props["assignment_key"].(string)
	return expSubjectFactKeyPattern.MatchString(key)
}

// withdrawnExperimentFactRaw is the same recognition over a spooled
// envelope's exact wire bytes.
func withdrawnExperimentFactRaw(raw json.RawMessage) bool {
	var wire struct {
		EventName string `json:"event_name"`
		Props     struct {
			AssignmentKey string `json:"assignment_key"`
		} `json:"props"`
	}
	if json.Unmarshal(raw, &wire) != nil {
		return false
	}
	if wire.EventName != experimentExposureName && wire.EventName != experimentOutcomeName {
		return false
	}
	return expSubjectFactKeyPattern.MatchString(wire.Props.AssignmentKey)
}

// purgeWithdrawnExperimentFacts runs when the real-subjects sentinel LANDS:
// the platform withdrew the assignments AND their subject fact keys, so
// experiment facts already ACCEPTED into the pipeline — the shared queue,
// the worker's held batch, the disk spool — must not ship on the next
// flush. Owed snapshots were discarded by the install under e.mu; this is
// the rest of the pipeline:
//   - the queue filters under the intake lock, with emitMu held so an emit
//     in flight either enqueued before the filter (and is caught) or reads
//     the already-cleared state after it (and emits nothing);
//   - the worker's held batch filters at its next dispatch point via the
//     purge epoch (see dropWithdrawnExperimentFacts) — before any send;
//   - the spool removes matching envelopes and dead-letters them
//     (SpoolDropTerminal: the server outcome settled them undeliverable).
//
// The residual is a fact already handed to the transport when the sentinel
// landed: indistinguishable from one already delivered — and if that same
// in-flight send fails and spools, the spool's retry-age cap bounds it.
func (c *Client) purgeWithdrawnExperimentFacts() {
	e := c.exp
	e.emitMu.Lock()
	c.lifecycleMu.Lock()
	c.expFactPurgeEpoch.Add(1)
	removedQueued := c.queue.filter(func(event Event) bool {
		return !isWithdrawnExperimentFactEvent(event)
	})
	c.lifecycleMu.Unlock()
	e.emitMu.Unlock()
	var removedSpooled []spoolEntry
	persistFailed := false
	if c.spool != nil {
		removedSpooled, persistFailed = c.spool.removeMatching(withdrawnExperimentFactRaw)
	}
	if removedQueued > 0 {
		c.stats.dropped.Add(uint64(removedQueued))
	}
	if persistFailed {
		c.recordSpoolPersistFailure()
	}
	if removedQueued > 0 || len(removedSpooled) > 0 {
		c.logf("shardpilot experiments: withdrew %d queued and %d spooled experiment fact(s) with the real-subjects sentinel (their subject fact keys must not ship)", removedQueued, len(removedSpooled))
	}
	// Dead-letters dispatch with no lock held: the callback is integrator
	// code.
	c.notifySpoolDeadLetter(SpoolDropTerminal, removedSpooled)
}

// dropWithdrawnExperimentFacts is the worker-batch leg of the sentinel
// purge: at every dispatch point (the same spot the consent-epoch drop
// runs, before any send) the worker checks whether a purge happened since
// it last looked and filters ITS held batch — events it pulled from the
// queue before the purge's filter ran are invisible to that filter, exactly
// like the consent drain. Runs only on the worker goroutine; the seen-epoch
// field is worker-owned state (the retainedRequest discipline).
func (c *Client) dropWithdrawnExperimentFacts(batch []Event, backoffAttempt *int) []Event {
	epoch := c.expFactPurgeEpoch.Load()
	if epoch == c.workerSeenExpFactPurge {
		return batch
	}
	c.workerSeenExpFactPurge = epoch
	kept := batch[:0]
	removed := 0
	for _, event := range batch {
		if isWithdrawnExperimentFactEvent(event) {
			removed++
			continue
		}
		kept = append(kept, event)
	}
	if removed > 0 {
		c.stats.dropped.Add(uint64(removed))
		// The retained wire bytes described the pre-filter batch: filter
		// them by the SAME predicate so surviving members keep their exact
		// bytes — the byte-identical retry/spool contract must hold for
		// host events that merely shared a batch with withdrawn facts (a
		// wholesale clear would remarshal them, drifting if the caller
		// mutated nested Props/Context after Enqueue). Filtering both
		// sides with one predicate keeps the pair positionally aligned for
		// the prefix-reuse builder; any residual mismatch falls back to
		// the rebuild path by clearing.
		filtered, _ := filterWithdrawnFromBatchRequest(c.retainedRequest)
		if len(filtered.Events) == len(kept) {
			c.retainedRequest = filtered
		} else {
			c.retainedRequest = batchRequest{}
		}
		if len(kept) == 0 {
			// The whole held batch was withdrawn: the discarded batch takes
			// its backoff streak with it (the consent-drop discipline) —
			// post-sentinel events must never start deep in a schedule that
			// belonged to condemned data.
			*backoffAttempt = 0
		}
	}
	return kept
}

// filterWithdrawnFromBatchRequest drops withdrawn experiment facts from a
// built batch request, preserving the surviving members' exact wire bytes
// and their envelope/raw pairing.
func filterWithdrawnFromBatchRequest(request batchRequest) (batchRequest, int) {
	if len(request.Events) == 0 || len(request.Events) != len(request.rawEvents) {
		return request, 0
	}
	removed := 0
	envelopes := make([]eventEnvelope, 0, len(request.Events))
	raws := make([]json.RawMessage, 0, len(request.rawEvents))
	for i, raw := range request.rawEvents {
		if withdrawnExperimentFactRaw(raw) {
			removed++
			continue
		}
		envelopes = append(envelopes, request.Events[i])
		raws = append(raws, raw)
	}
	if removed == 0 {
		return request, 0
	}
	request.Events = envelopes
	request.rawEvents = raws
	return request, removed
}

// captureOwedExposuresForDrop durably captures an entry's still-owed
// exposure facts into the disk spool — invoked by the install's drop branch
// UNDER e.mu, BEFORE the entry's durable delete lands (the fleet contract:
// a kill/not-assigned drop must not lose the fact of real treatment to a
// process death before the next sweep). The next launch replays the spooled
// envelope; a fact the live session ALSO delivers settles its spooled copy
// by event id, and a double delivery collapses server-side on the
// deterministic id. Queue-full-without-drop stays memory-only by design.
//
// Lock discipline: runs under e.mu, so it takes NO client lock (the fact
// and envelope builders are lock-free; the spool's mutex is a leaf) and
// DEFERS integrator dead-letter callbacks to the next off-lock drain.
func (c *Client) captureOwedExposuresForDrop(experimentKey string, owed []*expOwedExposure) {
	if c.spool == nil {
		return
	}
	events := make([]Event, 0, len(owed))
	for _, snapshot := range owed {
		// The automatic owed emission is arm 0 by definition, and the id is
		// exactly the one the live sweep would mint for it.
		eventID := experimentExposureEventID(snapshot.session, snapshot.entry.SubjectKey, experimentKey, snapshot.entry.Version, 0)
		event, skipCode := c.buildExperimentFactEvent(experimentExposureName, experimentKey, snapshot.entry, eventID)
		if skipCode != "" {
			continue // no server-safe fact exists for this snapshot
		}
		events = append(events, event)
	}
	if len(events) == 0 {
		return
	}
	request, err := c.buildBatch(events)
	if err != nil {
		return
	}
	eligible, refusedActors := c.partitionSpoolEligible(request)
	if len(refusedActors) > 0 {
		c.deferSpoolLetter(spoolDeadLetterFrom(SpoolDropConsent, refusedActors))
	}
	if len(eligible) == 0 {
		return
	}
	s := c.spool
	refused, added, expired, evicted, persistFailed := s.append(eligible, 0, false, c.clock.Now(), func() bool {
		return c.consent.Load() == consentStateGranted && s.grantPersisted
	})
	if refused {
		c.deferSpoolLetter(spoolDeadLetterFrom(SpoolDropConsent, eligible))
		return
	}
	if len(expired) > 0 {
		c.stats.spoolExpired.Add(uint64(len(expired)))
		c.deferSpoolLetter(spoolDeadLetterFrom(SpoolDropExpired, expired))
	}
	if len(added) > 0 && !persistFailed {
		c.stats.spooled.Add(uint64(len(added)))
	}
	if len(evicted) > 0 {
		c.stats.spoolEvicted.Add(uint64(len(evicted)))
		c.deferSpoolLetter(spoolDeadLetterFrom(SpoolDropCapacity, evicted))
	}
	if persistFailed {
		c.recordSpoolPersistFailure()
	}
}

// deferSpoolLetter queues a dead-letter for the next off-lock drain point —
// for spool work performed under a state lock, where invoking the
// integrator callback directly could deadlock or re-enter.
func (c *Client) deferSpoolLetter(letter SpoolDeadLetter) {
	if len(letter.Envelopes) == 0 {
		return
	}
	c.deferredLettersMu.Lock()
	c.deferredSpoolLetters = append(c.deferredSpoolLetters, letter)
	c.deferredLettersMu.Unlock()
}

// drainDeferredSpoolLetters dispatches deferred dead-letters with no lock
// held.
func (c *Client) drainDeferredSpoolLetters() {
	c.deferredLettersMu.Lock()
	letters := c.deferredSpoolLetters
	c.deferredSpoolLetters = nil
	c.deferredLettersMu.Unlock()
	c.emitSpoolDeadLetters(letters)
}

// closeExperimentPreFlush is the first half of Close's last-chance pass,
// run after the closed store (no new host calls) and BEFORE the final
// flush: owed durable syncs get one last retry (a kill/not-assigned drop —
// or a refresh write — whose cache write failed transiently must not stay
// reload truth on disk just because no cycle ran after storage recovered),
// and owed exposure facts sweep into the queue past the closed gate so the
// flush delivers them.
func (c *Client) closeExperimentPreFlush() {
	e := c.exp
	if e == nil {
		return
	}
	// Teardown FIRST: an in-flight lane response settling during the close
	// window is discarded outright (no install, no pacing, no NEW owed
	// durable intent), so the durable retry below runs against a STABLE
	// intent set — a kill drop that settled a moment later would otherwise
	// mint an owed intent after the last retry already ran and lose it at
	// exit (the discarded response re-arrives at the next launch's
	// revalidation instead). The close sweeps and the durable retry
	// deliberately keep working after teardown.
	e.teardown()
	e.retryDurableSync()
	if c.experimentConsentRefusal() == nil {
		c.sweepAllExperimentExposuresMode(true)
	}
	// Any dead-letters a locked capture deferred must not be lost at close.
	c.drainDeferredSpoolLetters()
}

// closeExperimentPostFlush is the second half: the flush freed queue room,
// so owed exposure facts that could not enqueue before it get their last
// chance (a treatment applied under a FULL queue must not exit without its
// fact — the worker's stop-path drain delivers-or-spools whatever enqueues
// here), then the consumer tears down: an assignment response still in
// flight must not install, persist, or pace from now on. Best-effort by
// design and never silent: whatever cannot be delivered is counted by the
// close path's accounting, and the durable record re-arms live assignments
// at the next launch.
// closeExperimentPostFlush drains close-time owed exposures in a LOOP —
// sweep, then flush what entered the queue, until nothing is owed or a full
// pass makes no progress (a bounded-capacity queue can admit as little as
// one fact per pass, and one sweep+flush would silently lose the rest).
// Whatever a stuck pass leaves is surfaced (logged and counted by the close
// path's delivery accounting; live assignments re-arm from the durable
// record at the next launch), then the consumer tears down.
func (c *Client) closeExperimentPostFlush(ctx context.Context) {
	e := c.exp
	if e == nil {
		return
	}
	if c.experimentConsentRefusal() == nil {
		for {
			before := c.owedExperimentExposureCount()
			if before == 0 {
				break
			}
			c.sweepAllExperimentExposuresMode(true)
			after := c.owedExperimentExposureCount()
			if after < before {
				// Deliver what the sweep enqueued so the next pass has
				// room; a failed flush leaves the facts for the worker's
				// stop-path drain (spooled or counted, never silent).
				if flushErr := c.Flush(ctx); flushErr != nil {
					c.logf("shardpilot experiments: delivering owed exposure facts at close failed (they spool or are counted with the close remnant): %v", flushErr)
					break
				}
			}
			if after == 0 || after >= before {
				break
			}
		}
	}
	// Snapshots STILL owed at teardown are lost with the process (a
	// dropped assignment's or a memory-only client's facts have nothing to
	// re-arm them; live entries re-arm from the durable record): COUNT
	// them as dropped with a distinct diagnostic — never a silent loss.
	if remaining := c.owedExperimentExposureCount(); remaining > 0 {
		c.stats.dropped.Add(uint64(remaining))
		c.stats.setLastError("experiment_exposures_discarded_at_close")
		c.logf("shardpilot experiments: %d owed exposure fact(s) discarded at close (counted in Stats.Dropped); live assignments re-arm from the durable record at the next launch", remaining)
	}
}

func (c *Client) owedExperimentExposureCount() int {
	e := c.exp
	e.mu.Lock()
	defer e.mu.Unlock()
	count := 0
	for _, list := range e.pendingExposure {
		count += len(list)
	}
	return count
}
