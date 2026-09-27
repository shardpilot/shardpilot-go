package shardpilot

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Experiment exposure/outcome fact producers (the analytics half of the
// consumer in experiments.go): the two runtime experiment facts, delivered
// THROUGH the client's existing analytics pipeline — the bounded queue, the
// flush worker, the spool, and the consent gates — so they inherit exactly
// the consent posture the integrator configured. No consent bypass.
//
// Exposures take one hop first. An application (ApplyExperimentVariant, or
// an extra one from TrackExperimentExposure) is kept as an owed record — the
// application as it was: the assignment's version, variant, serving state,
// subject and attributes, a random exposure_id and applied_at. The
// background lane sends it to the platform's exposure apply endpoint on the
// assignment host, with the same key as the fetch; the platform re-evaluates
// the assignment as of the state it was served from and answers with a
// SEALED fact, which the record then holds. The sealed fact goes onto the
// analytics lane verbatim — id, event time, scope and props exactly as
// sealed, the seal beside them — under this client's own envelope identity
// and the application's session. The subject never enters an analytics
// event; the envelope identity and the session never reach the apply
// endpoint.
//
// Wire contract (analytics ingest, strict):
//   - names `experiment_exposure` / `experiment_outcome`;
//   - source is ALWAYS "client" (these are runtime client facts, whatever
//     Config.Source says otherwise);
//   - user_id is ALWAYS omitted; anonymous_id is REQUIRED and carries the
//     SDK's standard Config.AnonymousID — that identity is what makes the
//     GDPR erasure cascade reach the fact. A client with no configured
//     AnonymousID cannot post an in-contract fact: its applications are not
//     recordable (counted);
//   - an exposure's props are the sealed props; an outcome's are the exact
//     allowlist (experiment_key, experiment_version, assignment_key,
//     variant_key, assignment_unit, outcome_key, outcome_value), with the
//     SERVER-MINTED subject-fact key VERBATIM as assignment_key.
//
// Delivery timing (exposures): the session's own application of a tuple —
// the "automatic" slot below — goes out once per (experiment, version,
// subject) per session (the client instance); an extra application is its
// own. Idempotency rides the exposure_id: a retried or re-armed application
// re-sends it, the platform derives the same fact id, and a duplicate
// collapses server-side. Owed records (unsealed, a full queue, a
// consent-closed window, a consent purge's re-arm of an applied tuple) drain
// in FIFO order per experiment within the bounded queue.

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
	// The intake stamp, read under lifecycleMu — the same lock a denial's
	// fast half takes — so it is exact, exactly like Enqueue's: a fact
	// admitted here is fully enqueued before any later denial's bump and
	// drain start, and the stamp provably predates that denial.
	intakeEpoch := c.consentEpoch.Load()
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
	if c.consentFloorEnabled() && event.omitUserID && c.cfg.UserID != "" {
		// The floor's grant covers the configured EFFECTIVE actor — with a
		// UserID configured, the user actor — but this SDK fact rides the
		// ANONYMOUS identity alone on the wire (user_id omitted by
		// contract). The actor whose id actually ships has no recorded
		// grant, so a strict-consent ingest would suppress the fact — or
		// worse, it would persist under the wrong actor's grant. The same
		// actor rule Enqueue applies to overridden identities applies
		// here: refused, retryably (owed snapshots stay armed for a
		// session whose actor shape may change).
		return ErrConsentActorMismatch
	}
	event, err := c.prepareEvent(event)
	if err != nil {
		return err
	}
	event.intakeConsentEpoch = intakeEpoch
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
// factEpoch is the pipeline purge epoch the CALLER observed UNDER e.mu
// atomically with the entry snapshot — never loaded here: this builder runs
// after the snapshot left the lock, and a real-subjects sentinel landing in
// that gap would stamp a pre-sentinel entry with the post-sentinel epoch,
// so the purge (blocked on emitMu until the emission completes) would treat
// the withdrawn fact as fresh and leave its subject-fact key in the
// queue/spool.
func (c *Client) buildExperimentFactEvent(name, experimentKey string, entry *expEntry, eventID, sessionID string, factEpoch uint64) (Event, string) {
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
		// The arm-time session identity. The source override below makes
		// these facts publish as "client" whatever tier the configuration
		// is, and the ingest contract requires session_id on every
		// non-backend event — an unstamped fact would be REJECTED once the
		// producer lane accepts these names. This SDK's session is the
		// client instance (one marker per construction); an owed snapshot
		// passes the marker of the session its application belonged to.
		// Host events under a backend-source configuration are untouched:
		// they keep publishing as "backend" with the contract's session_id
		// carve-out.
		SessionID: sessionID,
		Props:     props,
		// Envelope contract for experiment facts: source "client" and no
		// user_id, whatever the client configuration would default.
		omitUserID:     true,
		sourceOverride: SourceClient,
		// The purge generation this fact belongs to: a later sentinel
		// withdraws only facts built BEFORE it. The stamp is the caller's
		// snapshot-time observation (see the function comment), one atomic
		// (entry, epoch) pair.
		expFactEpoch: factEpoch,
	}, ""
}

// ── exposure delivery: the apply hop, then the analytics lane ───────────────

// expOwedCopy is an owed record's fields, copied under e.mu.
type expOwedCopy struct {
	entry   *expEntry
	session string
	app     expApplication
	extra   bool
	sealed  *expSealedFact
}

func copyOwed(owed *expOwedExposure) expOwedCopy {
	return expOwedCopy{entry: owed.entry, session: owed.session, app: owed.app, extra: owed.extra, sealed: owed.sealed}
}

// emitOwedExposure delivers one owed application. An application not sealed
// yet is sent to the apply endpoint first — only when network is allowed
// (the background lane, and Close), never inside a host call — and the
// sealed fact is then handed to the analytics queue verbatim, with its seal,
// under this client's envelope identity. Callers hold emitMu (never e.mu).
// Returns:
//   - ok=true             — handed to the queue, or already delivered this
//     session (the session's own application of a tuple goes out once);
//   - ok=false, terminal  — dropped and counted; the record leaves the queue;
//   - ok=false, !terminal — kept: unsealed while the hop may not run or is
//     paused, answered transiently or 401/403, a consent refusal, or a queue
//     that could not take the fact.
//
// factEpoch is the pipeline purge epoch the caller observed UNDER e.mu
// atomically with its snapshot of the record (see buildExperimentFactEvent).
func (c *Client) emitOwedExposure(ctx context.Context, experimentKey string, record *expOwedExposure, owed expOwedCopy, network, atClose bool, factEpoch uint64) (ok bool, code string, terminal bool) {
	if err := c.experimentConsentRefusal(); err != nil {
		return false, consentRefusalCode(err), false
	}
	e := c.exp
	nowMS := c.clock.Now().UnixMilli()
	e.mu.Lock()
	purgeEpoch := e.purgeEpoch
	currentSession := owed.session == e.sessionMarker
	tuple := exposureTupleKey(experimentKey, owed.entry)
	if !owed.extra && currentSession && e.exposed[tuple].auto {
		e.mu.Unlock()
		return true, "", false
	}
	if owed.sealed == nil && (!network || e.applyBlocked || (e.retryAfterMS != 0 && nowMS < e.retryAfterMS)) {
		e.mu.Unlock()
		return false, "apply_deferred", false
	}
	e.mu.Unlock()

	sealed := owed.sealed
	if sealed == nil {
		var dropped string
		var keep bool
		sealed, dropped, keep = c.sealExperimentApplication(ctx, experimentKey, owed)
		switch {
		case dropped != "":
			return false, dropped, true
		case keep:
			return false, "apply_kept", false
		}
		e.mu.Lock()
		if e.purgeEpoch != purgeEpoch {
			// A purge discarded this record while the hop ran (and re-armed
			// the application if it was applied): drop the answer, the
			// re-armed record re-sends the same application.
			e.mu.Unlock()
			return false, "purged", false
		}
		record.sealed = sealed
		e.mu.Unlock()
	}

	// Seam: the window between the caller's (record, epoch) snapshot leaving
	// e.mu and the event build below — a sentinel landing here is exactly
	// the race the snapshot-time factEpoch stamp closes.
	e.fireConsentRaceSeam("exposure_build")
	event, scoped := c.sealedExposureEvent(owed.session, sealed, factEpoch)
	if !scoped {
		e.mu.Lock()
		e.countDropLocked(expDropForeignScope, 1)
		e.mu.Unlock()
		c.logf("shardpilot experiments: a sealed exposure for experiment %q names another workspace, app or environment than this client's; dropped (foreign_scope)", experimentKey)
		return false, expDropForeignScope, true
	}
	// Seam: the window between this emission's own consent check (above)
	// and the fact intake's gate re-check — a consent flip landing here is
	// the raced refusal the intake reports.
	e.fireConsentRaceSeam("exposure_enqueue")
	if err := c.enqueueExperimentFact(event, atClose); err != nil {
		return false, err.Error(), false
	}
	// Seam: the window between the successful enqueue and the bookkeeping
	// re-locking below — a purge landing here races it.
	e.fireConsentRaceSeam("exposure_enqueued")
	if !owed.extra && currentSession {
		e.mu.Lock()
		if e.purgeEpoch == purgeEpoch {
			e.exposed[tuple] = expExposed{auto: true, app: owed.app}
		}
		// A purge that raced this delivery saw the record still owed and
		// re-armed its application; the re-sent application derives the same
		// fact id, so a fact that survived collapses server-side.
		e.mu.Unlock()
	}
	return true, "", false
}

// sealExperimentApplication sends one application to the apply endpoint and
// classifies the answer (the SDK half of the design's disposition table):
//   - 200 with a sealed fact: sealed;
//   - 401/403: kept, and the hop pauses until an authorized fetch — except the
//     real-subjects sentinel, which drops every owed application;
//   - any other 4xx but 408/429: dropped (apply_refused), a poison record
//     never retries;
//   - 3xx, 408, 429, 5xx, no response, a 200 without a usable fact: kept,
//     paced by the plane's shared Retry-After/backoff deadline.
func (c *Client) sealExperimentApplication(ctx context.Context, experimentKey string, owed expOwedCopy) (sealed *expSealedFact, dropped string, keep bool) {
	e := c.exp
	resp, err := c.postExposureApplication(ctx, experimentKey, owed)
	if err != nil && resp.status == 0 {
		if ctx != nil && ctx.Err() != nil {
			return nil, "", true // the lane is stopping or Close ran out: kept, unpaced
		}
		resp = remoteConfigResponse{}
	}
	nowMS := c.clock.Now().UnixMilli()
	switch {
	case resp.status == 200 && !resp.bodyIncomplete:
		if parsed := parseSealedExposure(resp.body); parsed != nil {
			e.mu.Lock()
			e.backoffAttempt = 0
			e.mu.Unlock()
			return parsed, "", false
		}
	case resp.status == 401 || resp.status == 403:
		e.mu.Lock()
		defer e.mu.Unlock()
		if resp.status == 403 && experimentBodyErrorText(resp.body, resp.bodyIncomplete) == expSentinelRealSubjectsDisabled {
			for _, list := range e.pendingExposure {
				e.countDropLocked(expDropRealSubjectsDisabled, len(list))
			}
			e.pendingExposure = make(map[string][]*expOwedExposure)
			return nil, expDropRealSubjectsDisabled, false
		}
		e.applyBlocked = true
		return nil, "", true
	case resp.status >= 400 && resp.status < 500 && resp.status != 408 && resp.status != 429:
		e.mu.Lock()
		e.countDropLocked(expDropApplyRefused, 1)
		e.mu.Unlock()
		c.logf("shardpilot experiments: the platform refused an exposure for experiment %q (HTTP %d); dropped (apply_refused)", experimentKey, resp.status)
		return nil, expDropApplyRefused, false
	}
	seconds, present := 0, false
	if wait, ok := parseRetryAfter(resp.retryAfterRaw); ok && (resp.status == 429 || resp.status >= 500) {
		seconds, present = int(wait/time.Second), true
	}
	e.mu.Lock()
	e.paceTransientLocked(nowMS, seconds, present)
	e.mu.Unlock()
	return nil, "", true
}

// postExposureApplication POSTs one application — exactly the pinned
// application, no identity or session member — to the apply endpoint on the
// assignment host, with the same publishable key as the fetch. Redirects are
// not followed.
func (c *Client) postExposureApplication(ctx context.Context, experimentKey string, owed expOwedCopy) (remoteConfigResponse, error) {
	e := c.exp
	attributes := make(map[string]string, len(owed.entry.Attributes))
	for _, attribute := range owed.entry.Attributes {
		attributes[attribute.Name] = attribute.Value
	}
	body, err := json.Marshal(map[string]any{
		"app_key":            e.appKey,
		"environment_key":    e.envKey,
		"experiment_key":     experimentKey,
		"experiment_version": owed.entry.Version,
		"subject_key":        owed.entry.SubjectKey,
		"variant_key":        owed.entry.VariantKey,
		"exposure_id":        owed.app.exposureID,
		"served_revision":    owed.entry.Served.Revision,
		"served_kill_gate":   owed.entry.Served.KillGate,
		"served_at":          owed.entry.Served.At,
		"applied_at":         owed.app.appliedAt,
		"attributes":         attributes,
	})
	if err != nil {
		return remoteConfigResponse{}, err
	}
	// Bounded like an assignment fetch: an HTTPClient without a Timeout
	// must not let a silent endpoint hold the emission lock.
	ctx, cancel := contextWithDefaultTimeout(ctx, c.cfg.HTTPTimeout)
	defer cancel()
	return c.transport.FetchRemoteConfig(ctx, remoteConfigRequest{
		url:    e.baseURL + expExposureApplyRoute,
		bearer: c.cfg.APIKey,
		method: "POST",
		body:   body,
	})
}

// parseSealedExposure reads the apply endpoint's 200 body: {"fact":{...},
// "seal":"..."}, the fact an experiment_exposure with an id, an event time,
// a scope and props. Anything else is no answer (nil).
func parseSealedExposure(body []byte) *expSealedFact {
	var answer struct {
		Fact json.RawMessage `json:"fact"`
		Seal string          `json:"seal"`
	}
	if json.Unmarshal(body, &answer) != nil || strings.TrimSpace(answer.Seal) == "" {
		return nil
	}
	var fact struct {
		EventID   string          `json:"event_id"`
		EventName string          `json:"event_name"`
		EventTS   string          `json:"event_ts"`
		Props     json.RawMessage `json:"props"`
	}
	if json.Unmarshal(answer.Fact, &fact) != nil || fact.EventID == "" || fact.EventName != experimentExposureName ||
		fact.EventTS == "" || len(fact.Props) == 0 || fact.Props[0] != '{' {
		return nil
	}
	return &expSealedFact{fact: answer.Fact, seal: answer.Seal}
}

// sealedExposureEvent builds the analytics event for a sealed fact: its id,
// name, event time (the sealed string, verbatim) and props as sealed, the
// seal beside them, and this
// client's own envelope identity and the application's session. It reports
// false when the fact names another scope than the one this client's
// envelope carries: analytics would refuse it.
func (c *Client) sealedExposureEvent(sessionID string, sealed *expSealedFact, factEpoch uint64) (Event, bool) {
	var fact struct {
		EventID       string          `json:"event_id"`
		EventName     string          `json:"event_name"`
		EventTS       string          `json:"event_ts"`
		WorkspaceID   string          `json:"workspace_id"`
		AppID         string          `json:"app_id"`
		EnvironmentID string          `json:"environment_id"`
		Props         json.RawMessage `json:"props"`
	}
	if json.Unmarshal(sealed.fact, &fact) != nil {
		return Event{}, false
	}
	if fact.WorkspaceID != c.cfg.WorkspaceID || fact.AppID != c.cfg.AppID || fact.EnvironmentID != c.cfg.EnvironmentID {
		return Event{}, false
	}
	// The props travel as decoded values: analytics re-canonicalizes the
	// props it receives before verifying the seal, and every number the
	// platform seals is an integer a float64 holds exactly.
	var props map[string]any
	if json.Unmarshal(fact.Props, &props) != nil {
		return Event{}, false
	}
	return Event{
		ID:              fact.EventID,
		Name:            fact.EventName,
		AnonymousID:     c.cfg.AnonymousID,
		SessionID:       sessionID,
		Props:           props,
		omitUserID:      true,
		sourceOverride:  SourceClient,
		expFactEpoch:    factEpoch,
		rawEventTS:      fact.EventTS,
		attestationSeal: sealed.seal,
	}, true
}

func consentRefusalCode(err error) string {
	if err == ErrConsentUnknown {
		return "consent_unknown"
	}
	return "consent_denied"
}

// sweepExperimentExposures drains one experiment's owed applications that
// are already sealed, in order, without the network (a host call's path):
// an unsealed head stops the drain and waits for the lane.
func (c *Client) sweepExperimentExposures(experimentKey string) {
	c.sweepExperimentExposuresMode(nil, experimentKey, false, false)
}

// sweepExperimentExposuresMode drains one experiment's owed applications in
// order: delivered and dropped records leave the queue; a kept one stops the
// drain and keeps the remainder owed, so an older application is never
// leapfrogged or lost. network allows the apply hop for unsealed records.
func (c *Client) sweepExperimentExposuresMode(ctx context.Context, experimentKey string, atClose, network bool) {
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
		// Copy the record's fields UNDER the lock, with the purge epoch in
		// the same observation: the fact's stamp must describe the record,
		// not whatever a later sentinel left.
		owed := copyOwed(head)
		headEpoch := c.expFactPurgeEpoch.Load()
		e.mu.Unlock()
		ok, _, terminal := c.emitOwedExposure(ctx, experimentKey, head, owed, network, atClose, headEpoch)
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

// sweepAllExperimentExposures drains every experiment's owed applications,
// the apply hop allowed: the background lane's sweep.
func (c *Client) sweepAllExperimentExposures(ctx context.Context) {
	c.sweepAllExperimentExposuresMode(ctx, false, true)
}

func (c *Client) sweepAllExperimentExposuresMode(ctx context.Context, atClose, network bool) {
	e := c.exp
	e.mu.Lock()
	keys := make([]string, 0, len(e.pendingExposure))
	for key := range e.pendingExposure {
		keys = append(keys, key)
	}
	e.mu.Unlock()
	sort.Strings(keys)
	for _, key := range keys {
		c.sweepExperimentExposuresMode(ctx, key, atClose, network)
	}
}

// TrackExperimentExposure records one more application of the cached
// assignment: an EXTRA exposure, with its own exposure_id, on top of
// ApplyExperimentVariant's once-per-session one — or, called before any
// application in this session, that application itself. Like
// ApplyExperimentVariant it never touches the network: the background lane
// seals and delivers it, within about a second. Requires the experiments
// opt-in (ErrExperimentsNotConfigured), an assignment currently served
// (ErrExperimentNoAssignment), the plane's consent admission
// (ErrConsentDenied/ErrConsentUnknown), and a recordable assignment
// (ErrExperimentFactUnavailable — see ApplyExperimentVariant; counted
// not_recordable).
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
	if c.consentFloorEnabled() && c.cfg.UserID != "" {
		// The fact would ride the anonymous identity alone, which a
		// user-scoped floor's grant does not cover — the fact intake's own
		// actor rule. Reported now, as TrackExperimentOutcome reports it,
		// rather than left to fail on the lane; nothing is armed.
		return ErrConsentActorMismatch
	}
	now := c.clock.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.tornDown {
		return ErrClosed
	}
	if err := c.experimentConsentRefusal(); err != nil {
		return err
	}
	entry := e.entries[experimentKey]
	if entry == nil {
		return ErrExperimentNoAssignment
	}
	if !c.experimentEntryRecordable(entry) {
		e.countDropLocked(expDropNotRecordable, 1)
		return ErrExperimentFactUnavailable
	}
	app, err := newExperimentApplication(now)
	if err != nil {
		e.countDropLocked(expDropNotRecordable, 1)
		return ErrExperimentFactUnavailable
	}
	tuple := exposureTupleKey(experimentKey, entry)
	extra := e.exposed[tuple].auto || e.owedTupleArmedLocked(experimentKey, tuple)
	e.armExposureLocked(experimentKey, entry, app, extra)
	return nil
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
	// An outcome is measured in THIS session by definition (it has no owed
	// cross-session machinery): the current marker is its session identity.
	// The purge epoch joins the same lock hold — one atomic (entry, epoch)
	// snapshot for the fact stamp.
	marker := e.sessionMarker
	factEpoch := c.expFactPurgeEpoch.Load()
	e.mu.Unlock()
	if entry == nil {
		return ErrExperimentNoAssignment
	}
	// Seam: the window between the snapshot above leaving e.mu and the fact
	// build — a sentinel landing here is exactly the race the snapshot-time
	// factEpoch stamp closes.
	e.fireConsentRaceSeam("outcome_build")
	event, skipCode := c.buildExperimentFactEvent(experimentOutcomeName, experimentKey, entry, "", marker, factEpoch)
	if skipCode != "" {
		return ErrExperimentFactUnavailable
	}
	event.Props["outcome_key"] = outcomeKey
	event.Props["outcome_value"] = outcomeValue
	return c.enqueueExperimentFact(event, false)
}

// isExperimentFactClassEvent recognizes one of this SDK's own experiment
// facts carrying a server-minted subject fact key. The decisive test is the
// SDK-INTERNAL authorship marker (omitUserID — settable only by the fact
// builders, never through the public Event surface), so a host-authored
// event that happens to use the reserved-looking name + sfk1_-shaped
// assignment_key combination is never matched — the sentinel withdraws this
// SDK's own facts, not host data that merely resembles them. The name and
// typed-prop checks stay as the class shape on top of the marker (never
// substring matching).
func isExperimentFactClassEvent(event Event) bool {
	if !event.omitUserID {
		return false
	}
	if event.Name != experimentExposureName && event.Name != experimentOutcomeName {
		return false
	}
	key, _ := event.Props["assignment_key"].(string)
	return expSubjectFactKeyPattern.MatchString(key)
}

// isWithdrawnExperimentFactEvent recognizes a fact the CURRENT purge state
// withdraws: the fact class AND a build stamp predating the current purge
// epoch. A fresh post-purge fact (a new authorized assignment after the
// platform re-enabled real subjects) carries the current epoch and is never
// withdrawn for a worker's epoch lag.
func (c *Client) isWithdrawnExperimentFactEvent(event Event) bool {
	return isExperimentFactClassEvent(event) && event.expFactEpoch < c.expFactPurgeEpoch.Load()
}

// withdrawnExperimentFactRaw is the class-SHAPE half of the recognition
// over a spooled envelope's exact wire bytes. The wire envelope cannot
// carry the SDK-authorship marker (the ingest contract is a strict
// allowlist), so every raw-side caller pairs this check with the entry's
// persisted internalFact flag (spoolEntry.internalFact, stamped from the
// envelope at spool time and stored in the record) — the shape alone must
// never condemn a host-authored envelope that resembles a fact.
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
//   - QUEUED facts die stamp-fenced at every consumer: admitReceivedEvent
//     (the worker's receive and the flush drain), the close remnant's
//     per-member check, and the denial drain — never via a drain/re-enqueue
//     filter here, which raced the worker's channel receive and could
//     reorder unrelated keeper events. emitMu is held so an emit in flight
//     either enqueued a pre-sentinel-stamped fact before this purge (caught
//     by those stamp fences) or reads the already-cleared state after it
//     (and emits nothing);
//   - the worker's held batch filters at its next dispatch point via the
//     purge epoch (see dropWithdrawnExperimentFacts) — before any send;
//   - the spool removes matching SDK-authored envelopes and dead-letters
//     them (SpoolDropTerminal: the server outcome settled them
//     undeliverable).
//
// The residual is a fact already handed to the transport when the sentinel
// landed: indistinguishable from one already delivered — and if that same
// in-flight send fails and spools, the spool's retry-age cap bounds it.
func (c *Client) purgeWithdrawnExperimentFacts() {
	e := c.exp
	e.emitMu.Lock()
	// The QUEUE leg lives entirely on the consumer side. The epoch was
	// already bumped UNDER e.mu, atomically with the sentinel's decisive
	// state change (applySentinelWithdrawalLocked), and every path that
	// takes an event OUT of the queue is stamp-fenced per event: the
	// worker's receive and the flush drain through admitReceivedEvent (the
	// intake-epoch idiom applied to the fact stamp), the close remnant's
	// per-member check, and the denial drain which drops wholesale — each
	// withdraws exactly the facts built before the sentinel (old epoch),
	// sparing post-sentinel ones, counted exactly once at the point that
	// drops them. A drain/re-enqueue filter here (the old shape) held
	// already-drained keepers out of the channel while the worker could
	// still receive a LATER queued event, reordering unrelated host events
	// that merely shared the queue with a purge — so no queue drain runs
	// at all: withdrawn facts still queue-resident simply die at the next
	// consumer touch, and they can never egress (admission, dispatch,
	// built-batch, and spool-handoff re-checks are all stamp-aware).
	var removedSpooled []spoolEntry
	persistFailed := false
	if c.spool != nil {
		// STILL under emitMu: every fact producer holds it, so no fresh
		// post-purge fact can be BORN — let alone spooled by a worker's
		// failed publish — until this sweep completes, which makes the
		// epoch-blind raw predicate exact (everything it can reach
		// predates the purge). Pre-purge facts a concurrent respool races
		// in are re-filtered by the respool's own epoch check instead.
		// With emitMu released first (the old shape), a fresh authorized
		// fact enqueued after the bump could reach the spool ahead of this
		// sweep and be withdrawn — dead-lettered — despite carrying the
		// current epoch. emitMu → spool mutex is the established order
		// (the spool's mutex is a leaf); dead-letters still dispatch with
		// no lock held below.
		// The post-bump epoch guards the sweep on top of the emitMu window:
		// the drop-time capture path appends from under e.mu ALONE (no
		// emitMu — lock order forbids it there), so a capture born after
		// this purge's bump can land mid-sweep; its entries carry the
		// current epoch stamp and are spared, while everything older is
		// withdrawn as before.
		removedSpooled, persistFailed = c.spool.removeMatching(withdrawnExperimentFactRaw, c.expFactPurgeEpoch.Load())
	}
	e.emitMu.Unlock()
	if persistFailed {
		c.recordSpoolPersistFailure()
	}
	if len(removedSpooled) > 0 {
		c.logf("shardpilot experiments: withdrew %d spooled experiment fact(s) with the real-subjects sentinel (their subject fact keys must not ship; queue-resident facts die stamp-fenced at the next consumer touch)", len(removedSpooled))
	}
	// Dead-letters dispatch with no lock held: the callback is integrator
	// code.
	c.notifySpoolDeadLetter(SpoolDropTerminal, removedSpooled)
}

// sentinelSpoolPurgeUnderLock is the disk-spool leg of the real-subjects
// sentinel, run UNDER e.mu (via sentinelSpoolPurgeFn) as part of the
// sentinel's DURABLE commit — before the assignment record clear/tombstone
// lands. removeMatching persists the withdrawal marker BEFORE the mirror
// forgets the entries, so once the sentinel's decisive durable state is on
// disk the spool withdrawal provably is too: no crash ordering can leave
// the next launch — where initSpool runs before the experiment preload and
// reloads raw facts with purge epoch zero — resending subject-fact keys the
// durable state says were withdrawn. Lock discipline: the spool's mutex is
// a leaf (the captureOwedExposuresForDrop precedent), integrator
// dead-letters are DEFERRED to the next off-lock drain, and the
// persist-failure diagnostic follows appendCaptureEntries' under-lock
// precedent. The off-lock purgeWithdrawnExperimentFacts still owns the
// queue and worker legs; its own spool pass then finds nothing new.
func (c *Client) sentinelSpoolPurgeUnderLock() {
	if c.spool == nil {
		return
	}
	removed, persistFailed := c.spool.removeMatching(withdrawnExperimentFactRaw, c.expFactPurgeEpoch.Load())
	if persistFailed {
		c.recordSpoolPersistFailure()
	}
	c.deferSpoolLetter(spoolDeadLetterFrom(SpoolDropTerminal, removed))
}

// dropWithdrawnSpoolChunkMembers re-verifies a pulled resend chunk against
// the CURRENT purge state immediately before transport handoff. A pulled
// chunk is local raw bytes: the sentinel's spool sweep removes withdrawn
// entries from the MIRROR (and dead-letters them there), but it cannot
// reach a chunk the worker already pulled — and spooled chunks never pass
// dropWithdrawnExperimentFacts (that leg filters the worker's held QUEUE
// batch) — so without this re-check a withdrawn experiment fact pulled
// before the sentinel landed would still publish after the purge. The
// predicate is removeMatching's exactly: the SDK-authorship flag AND the
// fact class on the entry's wire bytes, stamped before the current purge
// epoch (a post-purge capture is spared; epoch zero means no purge this
// process and everything passes).
// Withheld members are NOT acked, requeued, or dead-lettered here — the
// sweep owns their mirror removal and their exactly-once dead-letter
// accounting (it has either already removed them or, when this handoff
// observes the bump before the off-lock sweep runs, is about to); this
// filter only keeps their bytes off the wire. Pre-handoff filtering is
// always allowed: wire ambiguity begins strictly at the handoff itself.
func (c *Client) dropWithdrawnSpoolChunkMembers(chunk []spoolEntry) []spoolEntry {
	currentEpoch := c.expFactPurgeEpoch.Load()
	if currentEpoch == 0 {
		return chunk
	}
	kept := chunk[:0]
	withheld := 0
	for _, entry := range chunk {
		if entry.internalFact && entry.expFactEpoch < currentEpoch && withdrawnExperimentFactRaw(entry.raw) {
			withheld++
			continue
		}
		kept = append(kept, entry)
	}
	if withheld > 0 {
		c.logf("shardpilot experiments: withheld %d pulled spool member(s) at the transport handoff (withdrawn by the real-subjects sentinel; the purge sweep settles them)", withheld)
	}
	return kept
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
		if c.isWithdrawnExperimentFactEvent(event) {
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
		// The filtered retained request stays even when the batch is LONGER
		// than the retained prefix (queued members appended after the
		// failure that retained the bytes): both filters preserve order, so
		// the surviving prefix stays aligned, and the prefix-reuse builder
		// verifies id equality per position anyway (truncating reuse at the
		// first mismatch). A wholesale clear on the length mismatch
		// re-marshaled the surviving prefix members and broke the
		// byte-identical retry/spool contract exactly when a withdrawn fact
		// shared their batch.
		filtered, _ := filterWithdrawnFromBatchRequest(c.retainedRequest)
		c.retainedRequest = filtered
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

// dropWithdrawnBuiltBatch is the BUILT batch's purge re-check, immediately
// before the transport/spool handoff on the worker's dispatch and flush
// paths: the pre-build dispatch check and buildBatchIsolating do not run
// atomically, so a real-subjects sentinel landing between them leaves
// pre-sentinel facts inside the just-built request — which the builder
// stamped with the POST-sentinel epoch (it loads the epoch at build end),
// so a failed publish would respool the withdrawn facts as fresh
// (spoolFailedBatch's epoch-mismatch re-filter sees matching epochs and
// partitionSpoolEligible stamps the entries current, past every later
// sweep), and a successful publish would send them after withdrawal.
// Filters the events and the built request BY POSITION with the
// stamp-aware event predicate — buildBatchIsolating's contract aligns them
// — so surviving members keep their exact wire bytes and the envelope/raw
// pairing; a residual misalignment falls back to the two independent
// filters (the dropWithdrawnExperimentFacts discipline: the raw predicate
// on the request, with the prefix-reuse builder's per-position id check as
// the safety net). Dropped members are counted exactly once — they passed
// the earlier dispatch-point check, and the advanced seen-epoch keeps any
// later check from recounting. Runs only on the worker goroutine (the
// seen-epoch field is worker-owned state, exactly like
// dropWithdrawnExperimentFacts).
func (c *Client) dropWithdrawnBuiltBatch(request batchRequest, batch []Event, backoffAttempt *int) (batchRequest, []Event) {
	epoch := c.expFactPurgeEpoch.Load()
	if epoch == c.workerSeenExpFactPurge {
		return request, batch
	}
	c.workerSeenExpFactPurge = epoch
	aligned := len(request.Events) == len(batch) && len(request.rawEvents) == len(batch)
	kept := batch[:0]
	var keptEnvelopes []eventEnvelope
	var keptRaws []json.RawMessage
	if aligned {
		keptEnvelopes = make([]eventEnvelope, 0, len(request.Events))
		keptRaws = make([]json.RawMessage, 0, len(request.rawEvents))
	}
	removed := 0
	for i, event := range batch {
		if isExperimentFactClassEvent(event) && event.expFactEpoch < epoch {
			removed++
			continue
		}
		kept = append(kept, event)
		if aligned {
			keptEnvelopes = append(keptEnvelopes, request.Events[i])
			keptRaws = append(keptRaws, request.rawEvents[i])
		}
	}
	if removed == 0 {
		return request, kept
	}
	c.stats.dropped.Add(uint64(removed))
	c.logf("shardpilot experiments: withheld %d built batch member(s) at the transport handoff (withdrawn by the real-subjects sentinel between the dispatch check and the build)", removed)
	if aligned {
		request.Events = keptEnvelopes
		request.rawEvents = keptRaws
	} else {
		request, _ = filterWithdrawnFromBatchRequest(request)
	}
	if len(kept) == 0 {
		// The whole built batch was withdrawn: the discarded batch takes
		// its backoff streak with it (the consent-drop discipline).
		*backoffAttempt = 0
	}
	return request, kept
}

// filterWithdrawnFromBatchRequest drops withdrawn experiment facts from a
// built batch request, preserving the surviving members' exact wire bytes
// and their envelope/raw pairing. The typed envelope rides alongside the
// raw bytes here, so the SDK-authorship marker (internalIdentityFact) joins
// the raw-shape check: a host-authored member that merely resembles a fact
// is never withdrawn.
func filterWithdrawnFromBatchRequest(request batchRequest) (batchRequest, int) {
	if len(request.Events) == 0 || len(request.Events) != len(request.rawEvents) {
		return request, 0
	}
	removed := 0
	envelopes := make([]eventEnvelope, 0, len(request.Events))
	raws := make([]json.RawMessage, 0, len(request.rawEvents))
	for i, raw := range request.rawEvents {
		if request.Events[i].internalIdentityFact && withdrawnExperimentFactRaw(raw) {
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
func (c *Client) captureOwedExposuresForDrop(experimentKey string, owed []*expOwedExposure) (bool, []spoolEntry) {
	if c.spool == nil {
		// Memory-only client: no durable capture exists to gate on — the
		// documented ephemeral posture.
		return true, nil
	}
	// Under e.mu (the caller's hold), so this observation is atomic with
	// the owed records it stamps — the same (record, epoch) pairing every
	// other build site captures at its snapshot point.
	factEpoch := c.expFactPurgeEpoch.Load()
	events := make([]Event, 0, len(owed))
	for _, snapshot := range owed {
		// Only a SEALED application has a capturable form: an unsealed one
		// cannot be sealed offline. It stays owed in memory and the lane
		// still seals it — the endpoint judges it as of the state it was
		// served from, so the drop does not invalidate it.
		if snapshot.sealed == nil {
			continue
		}
		event, scoped := c.sealedExposureEvent(snapshot.session, snapshot.sealed, factEpoch)
		if !scoped {
			continue // no deliverable fact exists for this record
		}
		events = append(events, event)
	}
	if len(events) == 0 {
		return true, nil
	}
	request, err := c.buildBatch(events)
	if err != nil {
		return true, nil // unbuildable facts have no capturable form
	}
	eligible, refusedActors := c.partitionSpoolEligible(request)
	if len(refusedActors) > 0 {
		c.deferSpoolLetter(spoolDeadLetterFrom(SpoolDropConsent, refusedActors))
	}
	if len(eligible) == 0 {
		return true, nil
	}
	if c.appendCaptureEntries(eligible) {
		return true, nil
	}
	// The capture payload is FROZEN in the owed intent: the live sweep may
	// deliver these facts into the queue and empty the owed snapshots, but
	// queue residency is not durability — the gate releases only when
	// these exact envelopes land in the spool (double delivery collapses
	// on the deterministic ids; a live publish settles the spooled copies
	// by id).
	return false, eligible
}

// appendCaptureEntries appends frozen capture envelopes to the disk spool,
// reporting whether the capture is DURABLE (or moot by policy). Runs under
// e.mu like the capture itself: leaf spool mutex only, dead-letters
// deferred.
func (c *Client) appendCaptureEntries(eligible []spoolEntry) bool {
	s := c.spool
	if s == nil || len(eligible) == 0 {
		return true
	}
	refused, added, expired, evicted, persistFailed := s.append(eligible, 0, false, c.clock.Now(), func() bool {
		return c.consent.Load() == consentStateGranted && s.grantPersisted
	})
	if refused {
		// A POLICY refusal (non-grant / owed-wipe write gate): the spool's
		// documented posture is dead-letter-instead-of-disk, and gating the
		// record delete on a state only a consent change can open would
		// hold the kill's durable side hostage indefinitely.
		c.deferSpoolLetter(spoolDeadLetterFrom(SpoolDropConsent, eligible))
		return true
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
		// MECHANICAL failure: the facts sit in the spool mirror but not
		// durably on disk — the caller keeps the record intact and retries
		// the capture+delete pair.
		c.recordSpoolPersistFailure()
		return false
	}
	return true
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
		// Sealed applications only: the one bounded apply attempt comes
		// after the flush (closeExperimentPostFlush).
		c.sweepAllExperimentExposuresMode(nil, true, false)
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
// close path's accounting. The durable record restores live assignments at
// the next launch; the host's next application there records again.
// closeExperimentPostFlush drains close-time owed exposures in a LOOP —
// sweep, then flush what entered the queue, until nothing is owed or a full
// pass makes no progress (a bounded-capacity queue can admit as little as
// one fact per pass, and one sweep+flush would silently lose the rest).
// Whatever a stuck pass leaves is surfaced (logged and counted by the close
// path's delivery accounting; the durable record restores live
// assignments at the next launch, where the host's next application records
// again), then the consumer tears down.
func (c *Client) closeExperimentPostFlush(ctx context.Context) {
	e := c.exp
	if e == nil {
		return
	}
	if c.experimentConsentRefusal() == nil {
		// The FIRST pass may use the network, bounded by Close's context:
		// it seals what it can and stops at the first refusal that keeps an
		// application owed (the plane's pacing then defers the rest). Later
		// passes only hand already-sealed facts to the queue as room frees.
		network := true
		for {
			before := c.owedExperimentExposureCount()
			if before == 0 {
				break
			}
			c.sweepAllExperimentExposuresMode(ctx, true, network)
			network = false
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
	// Applications STILL owed at teardown are lost with the process: COUNT
	// them as dropped with a distinct diagnostic — never a silent loss. Each
	// is counted by why as well: unsealed_at_close if it was never sealed,
	// undelivered_at_close if its sealed fact could not be handed to the
	// queue.
	e.mu.Lock()
	remaining, unsealed := 0, 0
	for _, list := range e.pendingExposure {
		remaining += len(list)
		for _, owed := range list {
			if owed.sealed == nil {
				unsealed++
			}
		}
	}
	e.countDropLocked(expDropUnsealedAtClose, unsealed)
	e.countDropLocked(expDropUndeliveredAtClose, remaining-unsealed)
	e.mu.Unlock()
	if remaining > 0 {
		c.stats.dropped.Add(uint64(remaining))
		c.stats.setLastError("experiment_exposures_discarded_at_close")
		c.logf("shardpilot experiments: %d owed exposure fact(s) discarded at close (counted in Stats.Dropped; %d never sealed, counted unsealed_at_close; %d sealed and not enqueued, counted undelivered_at_close)", remaining, unsealed, remaining-unsealed)
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
