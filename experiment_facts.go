package shardpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"regexp"
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
// Both take one hop first. An application (ApplyExperimentVariant, or
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
// endpoint. An outcome (TrackExperimentOutcome) is an owed record of the
// same shape — the application it follows, with no exposure_id — plus its
// own outcome_id, occurred_at, key and value, sent to the outcome apply
// endpoint and delivered the same way.
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
//   - the props are the sealed props, the server-minted subject-fact key
//     among them as assignment_key.
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

// ── exposure delivery: the apply hop, then the analytics lane ───────────────

// expOwedCopy is an owed record's fields, copied under e.mu.
type expOwedCopy struct {
	entry   *expEntry
	session string
	app     expApplication
	extra   bool
	sealed  *expSealedFact
	outcome *expOutcomeRecord
}

func copyOwed(owed *expOwedExposure) expOwedCopy {
	return expOwedCopy{entry: owed.entry, session: owed.session, app: owed.app, extra: owed.extra, sealed: owed.sealed, outcome: owed.outcome}
}

// eventName is the fact the record is sealed as.
func (owed expOwedCopy) eventName() string {
	if owed.outcome != nil {
		return experimentOutcomeName
	}
	return experimentExposureName
}

// kind names the record for diagnostics.
func (owed expOwedCopy) kind() string {
	if owed.outcome != nil {
		return "outcome"
	}
	return "exposure"
}

// countDrop reports this record not recorded, in its kind's map. Callers
// hold e.mu.
func (owed expOwedCopy) countDrop(e *experimentsState, reason string) {
	if owed.outcome != nil {
		e.countOutcomeDropLocked(reason, 1)
		return
	}
	e.countDropLocked(reason, 1)
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
// atomically with its snapshot of the record (see sealedExposureEvent).
func (c *Client) emitOwedExposure(ctx context.Context, experimentKey string, record *expOwedExposure, owed expOwedCopy, network, atClose bool, factEpoch uint64) (ok bool, code string, terminal bool) {
	if err := c.experimentConsentRefusal(); err != nil {
		return false, consentRefusalCode(err), false
	}
	e := c.exp
	nowMS := c.clock.Now().UnixMilli()
	e.mu.Lock()
	purgeEpoch := e.purgeEpoch
	if record.withdrawn {
		// An age refusal withdrew the application after the sweep took it:
		// nothing is sent (the withdrawal counted it).
		e.mu.Unlock()
		return false, expDropAgeIneligible, true
	}
	// The fact's per-experiment withdrawal stamp, read with the record
	// still owed under e.mu: a refusal landing after this point withdraws
	// the fact wherever it is in the pipeline.
	keyEpoch := c.expKeyWithdrawEpoch.Load()
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
		sealed, dropped, keep = c.sealExperimentApplication(ctx, experimentKey, record, owed)
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
		if record.withdrawn {
			// An age refusal withdrew the application while the hop ran:
			// the sealed answer is discarded, never delivered.
			e.mu.Unlock()
			return false, expDropAgeIneligible, true
		}
		keyEpoch = c.expKeyWithdrawEpoch.Load()
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
		owed.countDrop(e, expDropForeignScope)
		e.mu.Unlock()
		c.logf("shardpilot experiments: a sealed %s for experiment %q names another workspace, app or environment than this client's; dropped (foreign_scope)", owed.kind(), experimentKey)
		return false, expDropForeignScope, true
	}
	event.expKeyWithdrawEpoch = keyEpoch
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
		if e.purgeEpoch == purgeEpoch && !record.withdrawn {
			// (A withdrawn application's fact dies in the pipeline by its
			// stamp; its tuple is not marked delivered.)
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
//   - 409 not_assigned/age_ineligible: the subject's owed applications of
//     the experiment are withdrawn and its assignment stops serving
//     (age_ineligible), as for an age refusal from the assignment route;
//   - any other 4xx but 408/429: dropped (apply_refused), a poison record
//     never retries;
//   - 3xx, 408, 429, 5xx, no response, a 200 without a usable fact: kept,
//     paced by the plane's shared Retry-After/backoff deadline.
func (c *Client) sealExperimentApplication(ctx context.Context, experimentKey string, record *expOwedExposure, owed expOwedCopy) (sealed *expSealedFact, dropped string, keep bool) {
	e := c.exp
	resp, err := c.postExposureApplication(ctx, experimentKey, owed)
	if errors.Is(err, errExperimentApplyConsentRefused) {
		return nil, "", true // a denial refused or aborted it: kept, unpaced; the denial's purge settles it
	}
	if err != nil && resp.status == 0 {
		if ctx != nil && ctx.Err() != nil {
			return nil, "", true // the lane is stopping or Close ran out: kept, unpaced
		}
		resp = remoteConfigResponse{}
	}
	nowMS := c.clock.Now().UnixMilli()
	switch {
	case resp.status == 200 && !resp.bodyIncomplete:
		if parsed := parseSealedExposure(resp.body, owed.eventName()); parsed != nil {
			e.mu.Lock()
			e.backoffAttempt = 0
			e.mu.Unlock()
			return parsed, "", false
		}
	case resp.status == 401 || resp.status == 403:
		e.mu.Lock()
		if resp.status != 403 || experimentBodyErrorText(resp.body, resp.bodyIncomplete) != expSentinelRealSubjectsDisabled {
			e.applyBlocked = true
			e.mu.Unlock()
			return nil, "", true
		}
		// The assignment route's sentinel, from the apply route: the same
		// withdrawal — the latch, the cached and durable assignments, the
		// owed applications (each counted real_subjects_disabled), and the
		// pipeline's facts. The last need nothing more here: the fact purge
		// epoch the withdrawal bumps fences the queue and the worker's
		// batch at every consumer, and it sweeps the spool under e.mu. (The
		// assignment route's off-lock purge finds nothing new either; if
		// the spool sweep ever leaves the withdrawal, this route needs that
		// purge too.)
		persistFailed := false
		if scope := e.scopeForLocked(e.currentSubjectIDLocked()); scope != "" {
			persistFailed, _ = e.applySentinelWithdrawalLocked(scope, nowMS)
		} else {
			for _, list := range e.pendingExposure {
				e.countOwedDropsLocked(expDropRealSubjectsDisabled, list)
			}
			e.pendingExposure = make(map[string][]*expOwedExposure)
		}
		e.mu.Unlock()
		if persistFailed {
			c.stats.setLastError("experiment_cache_persist_failed")
		}
		c.logf("shardpilot experiments: the platform disabled real-subject assignment (apply endpoint); dropped the cached assignments, the owed applications and their subject fact keys")
		return nil, expDropRealSubjectsDisabled, false
	case resp.status == 409 && experimentApplyAgeRefusal(resp.body, resp.bodyIncomplete):
		// The platform refused the application's pinned declaration. The
		// age rule applies to the application's subject and experiment: its
		// owed applications and their facts are withdrawn, and a served
		// assignment of that subject is dropped durably. An application an
		// earlier refusal already withdrew withdraws nothing more.
		e.mu.Lock()
		withdraw := !record.withdrawn
		persistFailed := false
		if withdraw {
			persistFailed = e.applyAgeRefusedApplicationLocked(experimentKey, owed.entry, nowMS)
		}
		e.mu.Unlock()
		// (Its spool sweep's dead-letters stay deferred: this runs under
		// emitMu, and the sweep dispatches them once it is released.)
		if persistFailed {
			c.stats.setLastError("experiment_cache_persist_failed")
		}
		if withdraw {
			c.logf("shardpilot experiments: the platform refused an %s for experiment %q (HTTP 409, age_ineligible); withdrew the subject's owed applications and facts of the experiment", owed.kind(), experimentKey)
		}
		return nil, expDropAgeIneligible, false
	case resp.status >= 400 && resp.status < 500 && resp.status != 408 && resp.status != 429:
		e.mu.Lock()
		owed.countDrop(e, expDropApplyRefused)
		e.mu.Unlock()
		c.logf("shardpilot experiments: the platform refused an %s for experiment %q (HTTP %d); dropped (apply_refused)", owed.kind(), experimentKey, resp.status)
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
	// Seam: the window between the emission's consent check and the wire.
	c.exp.fireConsentRaceSeam("apply_wire")
	e := c.exp
	attributes := make(map[string]string, len(owed.entry.Attributes))
	for _, attribute := range owed.entry.Attributes {
		attributes[attribute.Name] = attribute.Value
	}
	members := map[string]any{
		"app_key":            e.appKey,
		"environment_key":    e.envKey,
		"experiment_key":     experimentKey,
		"experiment_version": owed.entry.Version,
		"subject_key":        owed.entry.SubjectKey,
		"variant_key":        owed.entry.VariantKey,
		"served_revision":    owed.entry.Served.Revision,
		"served_kill_gate":   owed.entry.Served.KillGate,
		"served_at":          owed.entry.Served.At,
		"applied_at":         owed.app.appliedAt,
		"attributes":         attributes,
	}
	route := expExposureApplyRoute
	if outcome := owed.outcome; outcome != nil {
		// The outcome request is the application it follows, without its
		// exposure_id, and the outcome's own members.
		route = expOutcomeApplyRoute
		members["outcome_id"] = outcome.outcomeID
		members["occurred_at"] = outcome.occurredAt
		members["outcome_key"] = outcome.key
		members["outcome_value"] = outcome.value
	} else {
		members["exposure_id"] = owed.app.exposureID
	}
	body, err := json.Marshal(members)
	if err != nil {
		return remoteConfigResponse{}, err
	}
	// Bounded like an assignment fetch: an HTTPClient without a Timeout
	// must not let a silent endpoint hold the emission lock.
	ctx, cancel := contextWithDefaultTimeout(ctx, c.cfg.HTTPTimeout)
	defer cancel()
	// Gated like an assignment fetch, and for the same promise — no
	// experiment traffic past a completed revocation (the request carries
	// the subject and the attributes). The gate is loaded BEFORE the
	// pre-wire re-check: a denial completing after the load cancels the
	// request mid-flight; one completing before it is refused here.
	gate := c.consentGate.Load()
	if gate != nil {
		var cancelOnDenial context.CancelFunc
		ctx, cancelOnDenial = context.WithCancel(ctx)
		defer cancelOnDenial()
		stop := context.AfterFunc(gate.ctx, cancelOnDenial)
		defer stop()
	}
	if c.experimentConsentRefusal() != nil {
		return remoteConfigResponse{}, errExperimentApplyConsentRefused
	}
	resp, err := c.transport.FetchRemoteConfig(ctx, remoteConfigRequest{
		url:    e.baseURL + route,
		bearer: c.cfg.APIKey,
		method: "POST",
		body:   body,
	})
	if err != nil && gate != nil && gate.ctx.Err() != nil && errors.Is(err, context.Canceled) {
		return remoteConfigResponse{}, errExperimentApplyConsentRefused
	}
	return resp, err
}

// errExperimentApplyConsentRefused is an apply request the consent gate
// refused before the wire or aborted on the wire.
var errExperimentApplyConsentRefused = errors.New("shardpilot experiments: apply request refused by a consent denial")

// experimentApplyAgeRefusal reports whether an apply route's answer body is
// the platform's age refusal, {"error":"not_assigned","reason":
// "age_ineligible"}. A truncated or over-limit body never is (the
// experimentBodyErrorText bound).
func experimentApplyAgeRefusal(body []byte, bodyIncomplete bool) bool {
	if bodyIncomplete || len(body) > expMaxBodyBytes {
		return false
	}
	var wire struct {
		Error  string `json:"error"`
		Reason string `json:"reason"`
	}
	if json.Unmarshal(body, &wire) != nil {
		return false
	}
	return wire.Error == "not_assigned" && wire.Reason == experimentReasonAge
}

// parseSealedExposure reads the apply endpoint's 200 body: {"fact":{...},
// "seal":"..."}, the fact an experiment_exposure with an id, an event time,
// a scope and props. Anything else is no answer (nil).
func parseSealedExposure(body []byte, eventName string) *expSealedFact {
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
	if json.Unmarshal(answer.Fact, &fact) != nil || fact.EventID == "" || fact.EventName != eventName ||
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
	// props it receives before verifying the seal. Numbers decode as
	// json.Number and marshal back as sealed: the platform seals the
	// application's int64 version, which a float64 rounds above 2^53.
	var props map[string]any
	decoder := json.NewDecoder(bytes.NewReader(fact.Props))
	decoder.UseNumber()
	if decoder.Decode(&props) != nil {
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
	c.sweepExperimentExposuresBudget(ctx, experimentKey, atClose, network, nil)
}

// sweepExperimentExposuresBudget is the sweep with an optional budget of
// apply requests: each unsealed head sent to the endpoint spends one, and a
// head met with the budget spent waits, unsent, for a later sweep. nil is
// unbounded.
func (c *Client) sweepExperimentExposuresBudget(ctx context.Context, experimentKey string, atClose, network bool, budget *int) {
	e := c.exp
	e.emitMu.Lock()
	defer func() {
		e.emitMu.Unlock()
		// Dead-letters an apply route's age refusal deferred during this
		// sweep dispatch here, with no lock held: the integrator callback
		// may re-enter an operation that sweeps, and so takes emitMu.
		c.drainDeferredSpoolLetters()
	}()
	for {
		e.mu.Lock()
		if e.declaring[experimentKey] != nil {
			// A non-adult declaration of the experiment is unanswered:
			// its owed applications wait, unsent, for the answer — kept
			// when it governs that they survive (kill_switch), withdrawn
			// when it refuses or never comes.
			e.mu.Unlock()
			return
		}
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
		headNetwork := network
		if network && budget != nil && owed.sealed == nil {
			if *budget == 0 {
				headNetwork = false
			} else {
				*budget--
			}
		}
		ok, _, terminal := c.emitOwedExposure(ctx, experimentKey, head, owed, headNetwork, atClose, headEpoch)
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

// expMaxApplyRequestsPerCycle bounds the lane's apply requests per cycle,
// so one cycle's exposure sweep stays short however many applications are
// owed; the rest wait for the next cycle.
const expMaxApplyRequestsPerCycle = expMaxOwedExposures

// sweepAllExperimentExposures drains every experiment's owed applications,
// the apply hop allowed within the cycle's budget: the background lane's
// sweep.
func (c *Client) sweepAllExperimentExposures(ctx context.Context) {
	budget := expMaxApplyRequestsPerCycle
	c.sweepAllExperimentExposuresBudget(ctx, false, true, &budget)
}

func (c *Client) sweepAllExperimentExposuresMode(ctx context.Context, atClose, network bool) {
	c.sweepAllExperimentExposuresBudget(ctx, atClose, network, nil)
}

func (c *Client) sweepAllExperimentExposuresBudget(ctx context.Context, atClose, network bool, budget *int) {
	e := c.exp
	e.mu.Lock()
	keys := make([]string, 0, len(e.pendingExposure))
	for key := range e.pendingExposure {
		keys = append(keys, key)
	}
	e.mu.Unlock()
	sort.Strings(keys)
	for _, key := range keys {
		c.sweepExperimentExposuresBudget(ctx, key, atClose, network, budget)
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
	entry := e.servedEntryLocked(experimentKey)
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
	if e.armExposureLocked(experimentKey, entry, app, extra) {
		e.lastApplied[experimentKey] = expLastApplication{entry: entry, app: app}
	}
	return nil
}

// TrackExperimentOutcome records one measured outcome of an experiment:
// outcomeKey names what was measured and outcomeValue its value. Like an
// exposure it never touches the network: the outcome is kept owed, and the
// background lane — within about a second — has the platform seal it
// through the outcome apply endpoint and hands the sealed fact to the
// analytics queue. Each accepted call is a distinct outcome, with its own
// random outcome_id; a retried outcome re-sends its id and collapses
// server-side.
//
// An outcome follows the application the host made last in this session
// for the experiment — the most recent ApplyExperimentVariant, or
// TrackExperimentExposure, that recorded. After a revalidation installs a
// newer version, an outcome still follows the older application until the
// host applies the newer one; after the assignment is dropped (a kill
// switch, a not-assigned verdict) it still follows the last application.
// The platform judges each outcome as of the state that application was
// served from. The exception is an age_ineligible refusal: it withdraws the
// subject's applications of the experiment, so no outcome follows them:
// the call is refused (ErrExperimentNoAssignment while nothing is served)
// until the host applies a new assignment.
//
// The value must be an integer of magnitude at most 2^53 — a fractional,
// larger or non-finite value is refused — and is sent as a JSON integer.
// The key, trimmed of surrounding whitespace first as the experiment key is,
// must be 1–128 of A–Z, a–z, 0–9, '.', '_', ':' and '-', and not an
// IP address. Both are refused with ErrInvalidExperimentFact, as is an
// outcome earlier than its application (a clock stepped back), so nothing
// is queued only to be refused later.
//
// Requires the experiments opt-in (ErrExperimentsNotConfigured) and the
// plane's consent admission (ErrConsentDenied/ErrConsentUnknown; under a
// user-scoped consent floor, ErrConsentActorMismatch). With no application
// in this session it reports ErrExperimentNoAssignment when nothing is
// served, ErrExperimentFactUnavailable when the served assignment cannot be
// recorded (counted not_recordable in Stats.ExperimentOutcomeDrops), and
// ErrExperimentNotApplied otherwise: call ApplyExperimentVariant first.
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
	if !validExperimentOutcomeKey(outcomeKey) {
		return fmt.Errorf("%w: outcome key must be 1-128 of A-Z a-z 0-9 . _ : - and not an IP address", ErrInvalidExperimentFact)
	}
	value, ok := experimentOutcomeInteger(outcomeValue)
	if !ok {
		return fmt.Errorf("%w: outcome value must be an integer of magnitude at most 2^53", ErrInvalidExperimentFact)
	}
	if err := c.experimentConsentRefusal(); err != nil {
		return err
	}
	if c.consentFloorEnabled() && c.cfg.UserID != "" {
		// As TrackExperimentExposure: the fact would ride the anonymous
		// identity alone, which a user-scoped floor's grant does not cover.
		return ErrConsentActorMismatch
	}
	outcomeID, err := newExperimentApplicationID()
	if err != nil {
		return ErrExperimentFactUnavailable
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
	if e.declaring[experimentKey] != nil {
		// A pending non-adult declaration records nothing under the
		// earlier one: not against the served entry, nor against the
		// client-id application an outcome follows (a synthetic-subject
		// assignment records no outcome either way).
		if applied, ok := e.lastApplied[experimentKey]; ok && applied.entry.ageGated() {
			return ErrExperimentNoAssignment
		}
		if entry := e.entries[experimentKey]; entry == nil || entry.ageGated() {
			return ErrExperimentNoAssignment
		}
	}
	applied, ok := e.lastApplied[experimentKey]
	if !ok {
		entry := e.entries[experimentKey]
		switch {
		case entry == nil:
			return ErrExperimentNoAssignment
		case !c.experimentEntryRecordable(entry):
			e.countOutcomeDropLocked(expDropNotRecordable, 1)
			return ErrExperimentFactUnavailable
		default:
			return ErrExperimentNotApplied
		}
	}
	if appliedAt, err := time.Parse(time.RFC3339Nano, applied.app.appliedAt); err != nil || now.Before(appliedAt) {
		return fmt.Errorf("%w: the outcome is earlier than the application it follows", ErrInvalidExperimentFact)
	}
	e.armOutcomeLocked(experimentKey, applied, &expOutcomeRecord{
		outcomeID:  outcomeID,
		occurredAt: now.UTC().Format(time.RFC3339Nano),
		key:        outcomeKey,
		value:      value,
	})
	return nil
}

// expOutcomeKeyPattern is the platform's outcome_key grammar.
var expOutcomeKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// validExperimentOutcomeKey reports whether the platform seals the key: in
// its grammar and not an IP address.
func validExperimentOutcomeKey(key string) bool {
	return expOutcomeKeyPattern.MatchString(key) && net.ParseIP(key) == nil
}

// experimentOutcomeInteger returns the value as the integer the platform
// seals: finite, integral, of magnitude at most 2^53 (so every such
// float64 is exact); -0 is 0.
func experimentOutcomeInteger(value float64) (int64, bool) {
	const bound = 1 << 53
	if math.IsNaN(value) || math.IsInf(value, 0) || value != math.Trunc(value) || value > bound || value < -bound {
		return 0, false
	}
	return int64(value), true
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
	// An age refusal's per-experiment withdrawal filters at the same
	// dispatch points (dropKeyWithdrawnExperimentFacts).
	batch = c.dropKeyWithdrawnExperimentFacts(batch, backoffAttempt)
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
	// The same re-check for an age refusal landing in that window
	// (dropKeyWithdrawnBuiltBatch).
	request, batch = c.dropKeyWithdrawnBuiltBatch(request, batch, backoffAttempt)
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

// ── age refusal: one experiment's facts leave the pipeline ──────────────────
//
// An age_ineligible refusal withdraws the refused subject's facts of ONE
// experiment (withdrawOwedApplicationsLocked decides which: the experiment
// key and the subject fact keys; with no key known, nothing leaves the
// pipeline). Unlike the real-subjects sentinel it is never plane-wide, so
// it has its own generation counter (Client.expKeyWithdrawEpoch) and a
// record of what each generation withdrew (Client.expKeyWithdrawals); a
// fact is withdrawn when its build stamp (Event.expKeyWithdrawEpoch)
// predates a withdrawal that matches it.
// The legs mirror the sentinel's:
//   - the disk spool is swept at once, under e.mu, before the refusal's
//     durable record delete (withdrawExperimentKeyFactsUnderLock);
//   - queued facts die at the consumer (admitReceivedEvent, the close
//     remnant's per-member check);
//   - the worker's held and retained batches are filtered at the next
//     dispatch point, and a built batch once more before its transport
//     handoff (dropKeyWithdrawnExperimentFacts, dropKeyWithdrawnBuiltBatch);
//   - a pulled spool chunk is re-checked at its handoff
//     (dropKeyWithdrawnSpoolChunkMembers), and a failed batch respools
//     without them (spoolFailedBatch).
//
// A fact already handed to the transport when the refusal lands is
// wire-ambiguous: it is not withdrawn (it may have been delivered), and if
// that send fails it is never re-sent or respooled.

// withdrawExperimentKeyFactsUnderLock records one age refusal's withdrawal
// — the experiment and the subject fact keys; an empty set matches no fact
// — under a new generation, then removes the matching facts from the disk
// spool, dead-lettering them (SpoolDropTerminal). Called UNDER e.mu (via
// ageWithdrawFactsFn): everything the spool holds then was built before the
// refusal, so the sweep needs no stamp. The generation is published in the
// same spool-lock hold as the sweep, so a resend pull either precedes both
// (its generation is stale: the chunk handoff withholds the entry) or
// follows the sweep, and a respool append likewise (spoolFailedBatch).
// Lock discipline as sentinelSpoolPurgeUnderLock: leaf locks only (spool
// lock → expKeyWithdrawMu), dead-letters deferred.
func (c *Client) withdrawExperimentKeyFactsUnderLock(experimentKey string, factKeys []string) {
	publish := func() {
		c.expKeyWithdrawMu.Lock()
		epoch := c.expKeyWithdrawEpoch.Load() + 1
		if c.expKeyWithdrawals == nil {
			c.expKeyWithdrawals = make(map[string]map[string]uint64)
		}
		byFactKey := c.expKeyWithdrawals[experimentKey]
		if byFactKey == nil {
			byFactKey = make(map[string]uint64)
			c.expKeyWithdrawals[experimentKey] = byFactKey
		}
		for _, factKey := range factKeys {
			byFactKey[factKey] = epoch
		}
		// Published after the record: a consumer that sees the new
		// generation finds its withdrawal.
		c.expKeyWithdrawEpoch.Store(epoch)
		c.expKeyWithdrawMu.Unlock()
		if c.keyWithdrawPublishedSeam != nil {
			c.keyWithdrawPublishedSeam()
		}
	}
	if c.spool == nil {
		publish()
		return
	}
	removed, persistFailed := c.spool.removeMatchingAfter(publish, func(raw json.RawMessage) bool {
		return experimentKeyFactRawMatches(raw, experimentKey, factKeys)
	}, 0)
	if persistFailed {
		c.recordSpoolPersistFailure()
	}
	c.deferSpoolLetter(spoolDeadLetterFrom(SpoolDropTerminal, removed))
}

// experimentKeyFactMatches is the withdrawal's shape: an exposure or outcome
// fact of the experiment whose assignment_key is one of factKeys. An empty
// set matches nothing: without a subject fact key, the refused subject's
// facts cannot be told apart from another subject's.
func experimentKeyFactMatches(eventName, factExperimentKey, assignmentKey, experimentKey string, factKeys []string) bool {
	if eventName != experimentExposureName && eventName != experimentOutcomeName {
		return false
	}
	if factExperimentKey == "" || factExperimentKey != experimentKey {
		return false
	}
	for _, factKey := range factKeys {
		if assignmentKey == factKey {
			return true
		}
	}
	return false
}

// experimentFactWire is the part of a fact's wire bytes a withdrawal reads.
type experimentFactWire struct {
	EventName string `json:"event_name"`
	Props     struct {
		ExperimentKey string `json:"experiment_key"`
		AssignmentKey string `json:"assignment_key"`
	} `json:"props"`
}

// experimentKeyFactRawMatches is experimentKeyFactMatches over an
// envelope's wire bytes. Callers pair it with the SDK-authorship flag.
func experimentKeyFactRawMatches(raw json.RawMessage, experimentKey string, factKeys []string) bool {
	var wire experimentFactWire
	if json.Unmarshal(raw, &wire) != nil {
		return false
	}
	return experimentKeyFactMatches(wire.EventName, wire.Props.ExperimentKey, wire.Props.AssignmentKey, experimentKey, factKeys)
}

// experimentFactKeyWithdrawn reports whether an experiment fact built at
// stamp is withdrawn by a later age refusal of its experiment and subject.
func (c *Client) experimentFactKeyWithdrawn(eventName, experimentKey, assignmentKey string, stamp uint64) bool {
	if stamp >= c.expKeyWithdrawEpoch.Load() {
		return false
	}
	if eventName != experimentExposureName && eventName != experimentOutcomeName {
		return false
	}
	c.expKeyWithdrawMu.Lock()
	defer c.expKeyWithdrawMu.Unlock()
	if assignmentKey == "" {
		return false
	}
	epoch, ok := c.expKeyWithdrawals[experimentKey][assignmentKey]
	return ok && stamp < epoch
}

// isKeyWithdrawnExperimentFactEvent: the SDK's own fact (the authorship
// marker), withdrawn by an age refusal after it was built.
func (c *Client) isKeyWithdrawnExperimentFactEvent(event Event) bool {
	if !event.omitUserID {
		return false
	}
	experimentKey, _ := event.Props["experiment_key"].(string)
	assignmentKey, _ := event.Props["assignment_key"].(string)
	return c.experimentFactKeyWithdrawn(strings.TrimSpace(event.Name), experimentKey, assignmentKey, event.expKeyWithdrawEpoch)
}

// isKeyWithdrawnEnvelope is isKeyWithdrawnExperimentFactEvent for a built
// envelope.
func (c *Client) isKeyWithdrawnEnvelope(envelope eventEnvelope) bool {
	if !envelope.internalIdentityFact {
		return false
	}
	experimentKey, _ := envelope.Props["experiment_key"].(string)
	assignmentKey, _ := envelope.Props["assignment_key"].(string)
	return c.experimentFactKeyWithdrawn(envelope.EventName, experimentKey, assignmentKey, envelope.expKeyWithdrawEpoch)
}

// dropKeyWithdrawnExperimentFacts is the worker-batch leg: at every dispatch
// point, when a refusal landed since the worker last looked, it filters the
// held batch — counted in Stats.Dropped, as queued facts a purge clears are
// — and the same members from the retained wire bytes, so the survivors
// keep their exact bytes. Runs only on the worker goroutine (the seen mark
// is worker-owned).
func (c *Client) dropKeyWithdrawnExperimentFacts(batch []Event, backoffAttempt *int) []Event {
	epoch := c.expKeyWithdrawEpoch.Load()
	if epoch == c.workerSeenKeyWithdraw {
		return batch
	}
	c.workerSeenKeyWithdraw = epoch
	kept := batch[:0]
	removedIDs := make(map[string]struct{})
	for _, event := range batch {
		if c.isKeyWithdrawnExperimentFactEvent(event) {
			removedIDs[strings.TrimSpace(event.ID)] = struct{}{}
			continue
		}
		kept = append(kept, event)
	}
	if len(removedIDs) == 0 {
		return kept
	}
	c.stats.dropped.Add(uint64(len(removedIDs)))
	c.retainedRequest = withoutEnvelopeIDs(c.retainedRequest, removedIDs)
	if len(kept) == 0 {
		// The whole held batch was withdrawn: its backoff streak goes with
		// it (the consent-drop discipline).
		*backoffAttempt = 0
	}
	return kept
}

// withoutEnvelopeIDs drops the members with the given event ids from a built
// request, keeping the survivors' exact bytes and their envelope/raw
// pairing.
func withoutEnvelopeIDs(request batchRequest, ids map[string]struct{}) batchRequest {
	if len(request.Events) == 0 || len(request.Events) != len(request.rawEvents) {
		return request
	}
	envelopes := make([]eventEnvelope, 0, len(request.Events))
	raws := make([]json.RawMessage, 0, len(request.rawEvents))
	for i, envelope := range request.Events {
		if _, removed := ids[envelope.EventID]; removed {
			continue
		}
		envelopes = append(envelopes, envelope)
		raws = append(raws, request.rawEvents[i])
	}
	request.Events = envelopes
	request.rawEvents = raws
	return request
}

// dropKeyWithdrawnBuiltBatch re-checks a BUILT batch immediately before its
// transport or spool handoff, for a refusal that landed between the
// dispatch-point check and the build (the dropWithdrawnBuiltBatch window).
// The batch and the request are filtered by position, so they stay aligned;
// a misaligned pair filters the request by its own members. Worker
// goroutine only.
func (c *Client) dropKeyWithdrawnBuiltBatch(request batchRequest, batch []Event, backoffAttempt *int) (batchRequest, []Event) {
	epoch := c.expKeyWithdrawEpoch.Load()
	if epoch == c.workerSeenKeyWithdraw {
		return request, batch
	}
	c.workerSeenKeyWithdraw = epoch
	aligned := len(request.Events) == len(batch) && len(request.rawEvents) == len(batch)
	kept := batch[:0]
	var envelopes []eventEnvelope
	var raws []json.RawMessage
	removed := 0
	for i, event := range batch {
		if c.isKeyWithdrawnExperimentFactEvent(event) {
			removed++
			continue
		}
		kept = append(kept, event)
		if aligned {
			envelopes = append(envelopes, request.Events[i])
			raws = append(raws, request.rawEvents[i])
		}
	}
	if removed == 0 {
		return request, kept
	}
	c.stats.dropped.Add(uint64(removed))
	c.logf("shardpilot experiments: withheld %d built batch member(s) at the transport handoff (withdrawn by an age refusal)", removed)
	if aligned {
		request.Events = envelopes
		request.rawEvents = raws
	} else {
		request, _ = c.filterKeyWithdrawnFromBatchRequest(request)
	}
	if len(kept) == 0 {
		*backoffAttempt = 0
	}
	return request, kept
}

// filterKeyWithdrawnFromBatchRequest drops the facts an age refusal
// withdrew from a built request, by each member's own build stamp, keeping
// the survivors' exact bytes. Returns how many it dropped.
func (c *Client) filterKeyWithdrawnFromBatchRequest(request batchRequest) (batchRequest, int) {
	if c.expKeyWithdrawEpoch.Load() == 0 || len(request.Events) == 0 || len(request.Events) != len(request.rawEvents) {
		return request, 0
	}
	envelopes := make([]eventEnvelope, 0, len(request.Events))
	raws := make([]json.RawMessage, 0, len(request.rawEvents))
	removed := 0
	for i, envelope := range request.Events {
		if c.isKeyWithdrawnEnvelope(envelope) {
			removed++
			continue
		}
		envelopes = append(envelopes, envelope)
		raws = append(raws, request.rawEvents[i])
	}
	if removed == 0 {
		return request, 0
	}
	request.Events = envelopes
	request.rawEvents = raws
	return request, removed
}

// dropKeyWithdrawnSpoolChunkMembers re-checks a pulled spool chunk at its
// transport handoff: every member was spooled before the pull, so a refusal
// that landed after the pull (pulledAt, the generation read before it)
// withdraws the members it matches. The refusal's own spool sweep has
// removed them from the mirror (and dead-lettered them); this only keeps
// their bytes off the wire, as dropWithdrawnSpoolChunkMembers does. A
// refusal whose generation the pull already read had swept before the pull
// (one spool-lock hold), so an unchanged generation needs no filter.
func (c *Client) dropKeyWithdrawnSpoolChunkMembers(chunk []spoolEntry, pulledAt uint64) []spoolEntry {
	if pulledAt == c.expKeyWithdrawEpoch.Load() {
		return chunk
	}
	kept := chunk[:0]
	withheld := 0
	for _, entry := range chunk {
		if entry.internalFact {
			var wire experimentFactWire
			if json.Unmarshal(entry.raw, &wire) == nil && c.experimentFactKeyWithdrawn(wire.EventName, wire.Props.ExperimentKey, wire.Props.AssignmentKey, pulledAt) {
				withheld++
				continue
			}
		}
		kept = append(kept, entry)
	}
	if withheld > 0 {
		c.logf("shardpilot experiments: withheld %d pulled spool member(s) at the transport handoff (withdrawn by an age refusal; its spool sweep settles them)", withheld)
	}
	return kept
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
				e.countOwedDropLocked(expDropUnsealedAtClose, owed)
			} else {
				e.countOwedDropLocked(expDropUndeliveredAtClose, owed)
			}
		}
	}
	e.mu.Unlock()
	if remaining > 0 {
		c.stats.dropped.Add(uint64(remaining))
		c.stats.setLastError("experiment_exposures_discarded_at_close")
		c.logf("shardpilot experiments: %d owed experiment fact(s) discarded at close (counted in Stats.Dropped; %d never sealed, counted unsealed_at_close; %d sealed and not enqueued, counted undelivered_at_close)", remaining, unsealed, remaining-unsealed)
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
