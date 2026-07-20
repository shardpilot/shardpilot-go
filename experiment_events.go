package shardpilot

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Experiment exposure/outcome producers (GAP-017 / ADR-0259 Decision 6c):
// the two runtime experiment facts, emitted THROUGH the client's existing
// analytics pipeline — Track/Enqueue — so they inherit exactly the consent
// posture the integrator configured: with the opt-in Config.ConsentFloor,
// unknown consent refuses (ErrConsentUnknown) and denied drops
// (ErrConsentDenied), nothing queued or spooled; with the floor nil, the
// documented server-side consent posture applies unchanged. No new network
// path, no new queue, no consent bypass.
//
// Wire contract (analytics ingest, strict):
//   - names `experiment_exposure` / `experiment_outcome`;
//   - source is ALWAYS "client" (these are runtime client facts, whatever
//     Config.Source says otherwise);
//   - user_id is ALWAYS omitted; anonymous_id is REQUIRED and carries the
//     SDK's standard Config.AnonymousID — that identity is what makes the
//     GDPR erasure cascade reach the fact;
//   - props are the exact allowlist and nothing else: experiment_key,
//     experiment_version, assignment_key, variant_key, assignment_unit
//     (plus outcome_key/outcome_value on the outcome). For a client_id-unit
//     assignment the assignment_key prop MUST be the server-derived
//     subject_fact_key (sfk1_ + 64 hex); the raw spcid_ subject key never
//     rides props. Values come verbatim from the fetched assignment.
//
// Dark-lane status: the server-side client-tier admission for these two
// event names is decision-gated and CLOSED today — a publishable (Mode A)
// client key is unconditionally rejected for them, and the analytics
// client_id-unit flag defaults off — so even an opted-in producer's facts
// are refused at the ingest edge until the producer-lane decision and flag
// pair open it. The producers additionally sit behind this SDK's own
// experiment opt-in (Config.ExperimentsURL): with it unset they refuse
// ErrExperimentsNotConfigured and nothing is emitted at all.

const (
	experimentExposureEventName = "experiment_exposure"
	experimentOutcomeEventName  = "experiment_outcome"

	experimentAssignmentUnitSynthetic = "synthetic_subject_key"
	experimentAssignmentUnitClientID  = "client_id"
)

// experimentSubjectFactKeyPattern is the server-enforced grammar for the
// client_id-unit analytics fact subject: sfk1_ + 64 lowercase hex.
var experimentSubjectFactKeyPattern = regexp.MustCompile(`^sfk1_[0-9a-f]{64}$`)

// buildExperimentFactEvent validates one assignment against the fact
// contract and assembles the strict-allowlist event. outcomeKey/outcomeValue
// apply to the outcome fact only.
func (c *Client) buildExperimentFactEvent(name string, assignment ExperimentAssignment, outcomeKey string, outcomeValue any) (Event, error) {
	if !assignment.Assigned {
		// Facts describe an assignment the app ACTED on; a not-assigned
		// verdict (traffic gate, kill switch, targeting miss) must never
		// produce one.
		return Event{}, ErrExperimentNotAssigned
	}
	if assignment.AppKey != c.cfg.AppID || assignment.EnvironmentKey != c.cfg.EnvironmentID {
		// The assignment response echoes the app/environment keys it was
		// computed for; a verdict from ANOTHER scope (another client, app,
		// or environment — or a hand-built value that names none) must not
		// build facts under this client's envelope scope.
		return Event{}, fmt.Errorf("%w: assignment is for app_key %q / environment_key %q, this client is configured for %q / %q",
			ErrExperimentScopeMismatch, assignment.AppKey, assignment.EnvironmentKey, c.cfg.AppID, c.cfg.EnvironmentID)
	}
	experimentKey := strings.TrimSpace(assignment.ExperimentKey)
	assignmentKey := strings.TrimSpace(assignment.AssignmentKey)
	variantKey := strings.TrimSpace(assignment.VariantKey)
	if experimentKey == "" || assignmentKey == "" || variantKey == "" {
		return Event{}, fmt.Errorf("%w: assignment is missing experiment_key, assignment_key, or variant_key", ErrInvalidExperimentFact)
	}
	if assignment.Version < 1 {
		// Published experiment versions start at 1 — the fetch parser
		// enforces this on wire verdicts, and a hand-built or mutated
		// assignment must meet the same bar rather than emit an
		// out-of-contract experiment_version.
		return Event{}, fmt.Errorf("%w: assignment version must be a positive integer (published versions start at 1), got %d", ErrInvalidExperimentFact, assignment.Version)
	}
	unit := assignment.Boundary.AssignmentUnit
	subject := assignmentKey
	switch unit {
	case experimentAssignmentUnitSynthetic:
		// The synthetic lane's fact subject is the assignment key itself.
	case experimentAssignmentUnitClientID:
		// The client_id lane's fact subject MUST be the server-derived
		// subject_fact_key — enforced here so a raw spcid_ (or anything
		// else) can never leave the SDK in experiment props.
		subject = strings.TrimSpace(assignment.SubjectFactKey)
		if !experimentSubjectFactKeyPattern.MatchString(subject) {
			return Event{}, fmt.Errorf("%w: a client_id-unit fact requires the assignment's subject_fact_key (sfk1_ + 64 hex); the raw subject key never rides experiment props", ErrInvalidExperimentFact)
		}
	default:
		return Event{}, fmt.Errorf("%w: unknown assignment_unit %q", ErrInvalidExperimentFact, unit)
	}
	anonymousID := c.cfg.AnonymousID
	if anonymousID == "" {
		// The ingest contract requires the SDK client identity as
		// anonymous_id on experiment facts (erasure reachability): with
		// none configured the fact cannot be built in-contract.
		return Event{}, fmt.Errorf("%w: experiment facts require Config.AnonymousID (the SDK client identity rides anonymous_id)", ErrInvalidExperimentFact)
	}

	props := map[string]any{
		"experiment_key":     experimentKey,
		"experiment_version": assignment.Version,
		"assignment_key":     subject,
		"variant_key":        variantKey,
		"assignment_unit":    unit,
	}
	if name == experimentOutcomeEventName {
		outcomeKey = strings.TrimSpace(outcomeKey)
		if outcomeKey == "" {
			return Event{}, fmt.Errorf("%w: outcome_key is required", ErrInvalidExperimentFact)
		}
		value, err := normalizeExperimentOutcomeValue(outcomeValue)
		if err != nil {
			return Event{}, err
		}
		props["outcome_key"] = outcomeKey
		props["outcome_value"] = value
	}

	return Event{
		Name:        name,
		AnonymousID: anonymousID,
		Props:       props,
		// Envelope contract for experiment facts: source "client" and no
		// user_id, whatever the client configuration would default.
		omitUserID:     true,
		sourceOverride: SourceClient,
	}, nil
}

// normalizeExperimentOutcomeValue admits the contract's outcome_value
// domain: a finite number or a boolean. Anything else — NaN, ±Inf, strings,
// maps — refuses the fact whole rather than shipping a value the server
// would 400.
func normalizeExperimentOutcomeValue(value any) (any, error) {
	switch typed := value.(type) {
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return typed, nil
	case float32:
		if math.IsNaN(float64(typed)) || math.IsInf(float64(typed), 0) {
			return nil, fmt.Errorf("%w: outcome_value must be a finite number or a boolean", ErrInvalidExperimentFact)
		}
		return typed, nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return nil, fmt.Errorf("%w: outcome_value must be a finite number or a boolean", ErrInvalidExperimentFact)
		}
		return typed, nil
	default:
		return nil, fmt.Errorf("%w: outcome_value must be a finite number or a boolean, got %T", ErrInvalidExperimentFact, value)
	}
}

// exposureProvablyUnsent reports whether a failed emission attempt PROVES
// the exposure never entered the pipeline (or was refused by an observed
// non-2xx status — for events:batch the 202 is the only acceptance, so an
// answered refusal is not an acceptance): only those outcomes re-arm the
// reservation. A failure WITHOUT an observed status — a connection error
// after the request was written, a timeout awaiting the response, a
// caller-context abort mid-flight — is AMBIGUOUS: the batch may have
// reached the wire and been 2xx-accepted, and the server does not
// de-duplicate a re-EMITTED exposure (a fresh fact gets a fresh event_id),
// so re-arming would double-count on retry. Ambiguous outcomes consume the
// reservation instead: exposure emission is at-most-once per assignment
// identity per launch, preferring an undercount to a double count.
func exposureProvablyUnsent(err error) bool {
	switch {
	case errors.Is(err, ErrConsentDenied),
		errors.Is(err, ErrConsentUnknown),
		errors.Is(err, ErrConsentActorMismatch),
		errors.Is(err, ErrConsentReceiptPending),
		errors.Is(err, ErrClosed),
		errors.Is(err, ErrQueueFull),
		errors.Is(err, ErrInvalidEvent):
		// Refused before any wire use: intake gates, lifecycle, queue
		// capacity, validation.
		return true
	}
	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) {
		// The server ANSWERED with a non-2xx: the batch was not ingested.
		return true
	}
	var encodeErr *EncodeError
	if errors.As(err, &encodeErr) {
		// The body never marshaled; nothing could have been sent.
		return true
	}
	return false
}

// experimentExposureDedupeKey is the exposure reservation identity: the
// FULL assignment identity — experiment key, version, and assignment key —
// escaped and joined injectively (the scope-join discipline), so two
// distinct assignments can never collide into one reservation. Keying on
// the assignment key alone would rely on its (server-side) derivation
// covering experiment and version; the SDK also accepts caller-held
// ExperimentAssignment values, so the identity is made explicit here.
func experimentExposureDedupeKey(assignment ExperimentAssignment) string {
	return escapeRemoteConfigSegment(strings.TrimSpace(assignment.ExperimentKey)) + rcScopeSeparator +
		strconv.FormatInt(assignment.Version, 10) + rcScopeSeparator +
		escapeRemoteConfigSegment(strings.TrimSpace(assignment.AssignmentKey))
}

// emitExperimentExposure is the shared exposure path: build the fact, then
// run the one-per-launch reservation protocol for its assignment identity
// (experiment key + version + assignment key). The FIRST caller for an
// identity owns the emitting attempt; a concurrent duplicate WAITS for that
// attempt to settle instead of reporting success for an emission that may
// still fail — nil is returned off another caller's work only once the
// reservation actually CONVERTED (the fact was admitted). If the in-flight
// attempt fails and re-arms, a waiting duplicate contends again and
// performs its own attempt. The wait observes the CALLER's context (Track's
// ctx; Enqueue has none): a cancelled or expired caller gets its context
// error back promptly instead of waiting out the owner's attempt — and,
// having claimed nothing, leaves the reservation protocol untouched.
func (c *Client) emitExperimentExposure(ctx context.Context, assignment ExperimentAssignment, send func(Event) error) error {
	exp := c.exp
	if exp == nil {
		return ErrExperimentsNotConfigured
	}
	event, err := c.buildExperimentFactEvent(experimentExposureEventName, assignment, "", nil)
	if err != nil {
		return err
	}
	dedupeKey := experimentExposureDedupeKey(assignment)
	for {
		claim, owner := exp.beginExposureClaim(dedupeKey)
		if !owner {
			// A converted reservation's done is already closed: the
			// duplicate returns nil immediately. A pending one blocks
			// until the in-flight attempt settles (bounded by that
			// attempt's own send semantics) — or until the caller's own
			// context ends the wait.
			select {
			case <-claim.done:
			case <-contextDone(ctx):
				return contextCause(ctx)
			}
			if claim.converted {
				return nil
			}
			continue
		}
		err := send(event)
		// The reservation converts on success AND on any AMBIGUOUS failure
		// (see exposureProvablyUnsent): only an outcome that proves the
		// fact never entered the pipeline — or was refused by an observed
		// status — re-arms it. Enqueue-admitted exposures stay converted
		// forever, whatever later befalls the queued event (a consent
		// purge, a spool dead-letter, a terminal batch verdict): a
		// client-side discard after admission is not "unsent", and a
		// re-grant must not double-count the exposure.
		exp.settleExposureClaim(dedupeKey, claim, err == nil || !exposureProvablyUnsent(err))
		return err
	}
}

// TrackExperimentExposure publishes ONE experiment_exposure fact for an
// assignment the app just acted on, synchronously through Track — inheriting
// Track's full posture: lifecycle (ErrClosed), the configured consent
// posture (with Config.ConsentFloor: ErrConsentUnknown/ErrConsentDenied
// refusals — consent unknown transmits nothing), and delivery feedback.
//
// Exposures are deduplicated client-side per assignment identity —
// experiment key + version + assignment key — per client instance (the
// server never deduplicates): the first admitted call emits, and later
// calls for the same identity return nil without emitting — a concurrent
// duplicate waits for the in-flight attempt (the wait honors ctx: a
// cancelled or expired caller gets its context error back promptly) and
// returns nil only if that attempt converted the reservation. A PROVABLY
// refused call — a consent gate, lifecycle, queue capacity, validation, or
// an answered non-2xx status — releases the slot so a later (or waiting)
// attempt can emit; a no-status transport failure is ambiguous (the fact
// may have been accepted) and consumes the slot instead: emission is
// at-most-once per assignment identity per launch, and a discarded-after-
// admission event (a consent purge, a spool drop) never re-arms it either.
// Requires the experiment opt-in (ErrExperimentsNotConfigured
// otherwise) and refuses not-assigned verdicts (ErrExperimentNotAssigned),
// assignments from another app/environment scope
// (ErrExperimentScopeMismatch), and out-of-contract facts
// (ErrInvalidExperimentFact).
func (c *Client) TrackExperimentExposure(ctx context.Context, assignment ExperimentAssignment) error {
	return c.emitExperimentExposure(ctx, assignment, func(event Event) error {
		return c.Track(ctx, event)
	})
}

// EnqueueExperimentExposure is TrackExperimentExposure through the
// asynchronous queue (Enqueue): same validation, same consent inheritance,
// same one-per-assignment-identity reservation; delivery rides the
// background flush worker. Enqueue carries no caller context, so a
// duplicate that finds another attempt in flight waits for it to settle
// (bounded by that attempt's own send semantics).
func (c *Client) EnqueueExperimentExposure(assignment ExperimentAssignment) error {
	return c.emitExperimentExposure(nil, assignment, c.Enqueue)
}

// TrackExperimentOutcome publishes one experiment_outcome fact — the
// measured outcome for an assignment the app acted on — synchronously
// through Track, inheriting the configured consent posture exactly like the
// exposure producer. outcomeValue must be a finite number or a boolean.
// Outcomes are NOT deduplicated: every admitted call emits one fact.
func (c *Client) TrackExperimentOutcome(ctx context.Context, assignment ExperimentAssignment, outcomeKey string, outcomeValue any) error {
	if c.exp == nil {
		return ErrExperimentsNotConfigured
	}
	event, err := c.buildExperimentFactEvent(experimentOutcomeEventName, assignment, outcomeKey, outcomeValue)
	if err != nil {
		return err
	}
	return c.Track(ctx, event)
}

// EnqueueExperimentOutcome is TrackExperimentOutcome through the
// asynchronous queue (Enqueue).
func (c *Client) EnqueueExperimentOutcome(assignment ExperimentAssignment, outcomeKey string, outcomeValue any) error {
	if c.exp == nil {
		return ErrExperimentsNotConfigured
	}
	event, err := c.buildExperimentFactEvent(experimentOutcomeEventName, assignment, outcomeKey, outcomeValue)
	if err != nil {
		return err
	}
	return c.Enqueue(event)
}
