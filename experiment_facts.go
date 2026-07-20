package shardpilot

import (
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
	if c.consentDenied() {
		c.stats.dropped.Add(1)
		return ErrConsentDenied
	}
	if c.consentFloorEnabled() && c.consentUndecided() {
		c.stats.dropped.Add(1)
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
		e.mu.Unlock()
		ok, _, terminal := c.emitEntryExposure(experimentKey, head.entry, false, head.session, atClose)
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
	e.retryDurableSync()
	if c.experimentConsentRefusal() == nil {
		c.sweepAllExperimentExposuresMode(true)
	}
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
func (c *Client) closeExperimentPostFlush() (enqueuedAny bool) {
	e := c.exp
	if e == nil {
		return false
	}
	if c.experimentConsentRefusal() == nil {
		before := c.owedExperimentExposureCount()
		c.sweepAllExperimentExposuresMode(true)
		enqueuedAny = c.owedExperimentExposureCount() < before
	}
	e.teardown()
	return enqueuedAny
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
