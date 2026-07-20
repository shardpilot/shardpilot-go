package shardpilot

import (
	"context"
	"fmt"
	"math"
	"regexp"
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
	experimentKey := strings.TrimSpace(assignment.ExperimentKey)
	assignmentKey := strings.TrimSpace(assignment.AssignmentKey)
	variantKey := strings.TrimSpace(assignment.VariantKey)
	if experimentKey == "" || assignmentKey == "" || variantKey == "" {
		return Event{}, fmt.Errorf("%w: assignment is missing experiment_key, assignment_key, or variant_key", ErrInvalidExperimentFact)
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

// TrackExperimentExposure publishes ONE experiment_exposure fact for an
// assignment the app just acted on, synchronously through Track — inheriting
// Track's full posture: lifecycle (ErrClosed), the configured consent
// posture (with Config.ConsentFloor: ErrConsentUnknown/ErrConsentDenied
// refusals — consent unknown transmits nothing), and delivery feedback.
//
// Exposures are deduplicated client-side per assignment key per client
// instance (the server never deduplicates): the first admitted call emits,
// and later calls for the same assignment key return nil without emitting.
// A refused call (consent, queue, transport) releases the slot so a later
// attempt can emit. Requires the experiment opt-in (ErrExperimentsNotConfigured
// otherwise) and refuses not-assigned verdicts (ErrExperimentNotAssigned)
// and out-of-contract facts (ErrInvalidExperimentFact).
func (c *Client) TrackExperimentExposure(ctx context.Context, assignment ExperimentAssignment) error {
	exp := c.exp
	if exp == nil {
		return ErrExperimentsNotConfigured
	}
	event, err := c.buildExperimentFactEvent(experimentExposureEventName, assignment, "", nil)
	if err != nil {
		return err
	}
	if !exp.claimExposure(strings.TrimSpace(assignment.AssignmentKey)) {
		return nil
	}
	if err := c.Track(ctx, event); err != nil {
		exp.unclaimExposure(strings.TrimSpace(assignment.AssignmentKey))
		return err
	}
	return nil
}

// EnqueueExperimentExposure is TrackExperimentExposure through the
// asynchronous queue (Enqueue): same validation, same consent inheritance,
// same one-per-assignment-key dedupe; delivery rides the background flush
// worker.
func (c *Client) EnqueueExperimentExposure(assignment ExperimentAssignment) error {
	exp := c.exp
	if exp == nil {
		return ErrExperimentsNotConfigured
	}
	event, err := c.buildExperimentFactEvent(experimentExposureEventName, assignment, "", nil)
	if err != nil {
		return err
	}
	if !exp.claimExposure(strings.TrimSpace(assignment.AssignmentKey)) {
		return nil
	}
	if err := c.Enqueue(event); err != nil {
		exp.unclaimExposure(strings.TrimSpace(assignment.AssignmentKey))
		return err
	}
	return nil
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
