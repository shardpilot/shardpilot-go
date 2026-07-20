package shardpilot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type batchRequest struct {
	Events []eventEnvelope `json:"events"`

	// rawEvents, when non-nil, is the exact wire serialization of the batch's
	// envelopes, one JSON object per event. The request body is then built by
	// joining THESE bytes verbatim (see MarshalJSON), so the bytes a failed
	// batch spools to disk and the bytes a later resend puts on the wire are
	// identical — the ingest service de-duplicates re-sends by event_id, and
	// byte-identity is what makes a spooled re-send fold as a duplicate
	// instead of risking drift. Spool-origin resends carry ONLY rawEvents
	// (the typed Events are not reconstructed: a JSON round trip through Go
	// values is not byte-stable).
	rawEvents []json.RawMessage
}

// MarshalJSON emits {"events":[...]} by joining rawEvents verbatim when they
// are present, bypassing any re-encoding of the envelope bytes; a request
// without rawEvents (none is built by the SDK today) marshals normally.
func (r batchRequest) MarshalJSON() ([]byte, error) {
	if r.rawEvents == nil {
		type plainBatchRequest batchRequest
		return json.Marshal(plainBatchRequest(r))
	}
	var b bytes.Buffer
	b.WriteString(`{"events":[`)
	for i, raw := range r.rawEvents {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(raw)
	}
	b.WriteString(`]}`)
	return b.Bytes(), nil
}

type eventEnvelope struct {
	EventID         string         `json:"event_id"`
	SchemaVersion   int            `json:"schema_version"`
	EventName       string         `json:"event_name"`
	Source          Source         `json:"source"`
	EventTS         string         `json:"event_ts"`
	WorkspaceID     string         `json:"workspace_id"`
	AppID           string         `json:"app_id"`
	EnvironmentID   string         `json:"environment_id"`
	UserID          string         `json:"user_id,omitempty"`
	AnonymousID     string         `json:"anonymous_id,omitempty"`
	SessionID       string         `json:"session_id,omitempty"`
	SessionSequence int64          `json:"session_sequence,omitempty"`
	Platform        string         `json:"platform,omitempty"`
	AppVersion      string         `json:"app_version,omitempty"`
	AppBuild        string         `json:"app_build,omitempty"`
	Context         map[string]any `json:"context,omitempty"`
	Props           map[string]any `json:"props,omitempty"`

	// internalIdentityFact marks an envelope the SDK built for one of its
	// OWN experiment facts: user_id omitted BY WIRE CONTRACT (never an
	// actor override) with the CONFIGURED client identity as anonymous_id.
	// Unexported and never serialized — the wire bytes and spool records
	// are unchanged; it exists so the disk spool's actor-eligibility check
	// reaches the same verdict intake did for these envelopes (see
	// spoolActorEligible).
	internalIdentityFact bool
}

func (c *Client) buildEnvelope(event Event) (eventEnvelope, error) {
	name := strings.TrimSpace(event.Name)
	if name == "" {
		return eventEnvelope{}, fmt.Errorf("%w: event name is required", ErrInvalidEvent)
	}

	id := strings.TrimSpace(event.ID)
	if id == "" {
		generated, err := newEventID()
		if err != nil {
			return eventEnvelope{}, fmt.Errorf("%w: generate event id: %v", ErrInvalidEvent, err)
		}
		id = generated
	}

	timestamp := event.Timestamp
	if timestamp.IsZero() {
		timestamp = c.clock.Now()
	}

	props := cloneMap(event.Props)

	platform := firstNonEmpty(event.Platform, c.cfg.Platform)
	appVersion := firstNonEmpty(event.AppVersion, c.cfg.AppVersion)
	appBuild := firstNonEmpty(event.AppBuild, c.cfg.AppBuild)
	userID := firstNonEmpty(event.UserID, c.cfg.UserID)
	if event.omitUserID {
		// SDK-internal experiment facts: the ingest contract rejects these
		// event names when user_id carries ANY value, so the configured
		// default must not be stamped in.
		userID = ""
	}
	anonymousID := firstNonEmpty(event.AnonymousID, c.cfg.AnonymousID)
	source := c.cfg.Source
	if event.sourceOverride != "" {
		// SDK-internal experiment facts: admitted with source "client" only.
		source = event.sourceOverride
	}

	return eventEnvelope{
		EventID:         id,
		SchemaVersion:   1,
		EventName:       name,
		Source:          source,
		EventTS:         timestamp.UTC().Format(time.RFC3339Nano),
		WorkspaceID:     c.cfg.WorkspaceID,
		AppID:           c.cfg.AppID,
		EnvironmentID:   c.cfg.EnvironmentID,
		UserID:          userID,
		AnonymousID:     anonymousID,
		SessionID:       event.SessionID,
		SessionSequence: event.SessionSequence,
		Platform:        platform,
		AppVersion:      appVersion,
		AppBuild:        appBuild,
		Context:         cloneMap(event.Context),
		Props:           props,

		internalIdentityFact: event.omitUserID,
	}, nil
}

// buildBatch builds the wire request for a batch: each envelope is marshaled
// exactly once here and the resulting bytes are what the request body joins
// (and what the disk spool records on a retriable failure), so every publish
// attempt, spool record, and spooled resend of an event carries identical
// bytes. An envelope that cannot marshal (an unserializable Props value)
// surfaces as an EncodeError — the same permanent class the transport-level
// encode produced before.
func (c *Client) buildBatch(events []Event) (batchRequest, error) {
	envelopes := make([]eventEnvelope, 0, len(events))
	raws := make([]json.RawMessage, 0, len(events))
	for _, event := range events {
		envelope, err := c.buildEnvelope(event)
		if err != nil {
			return batchRequest{}, err
		}
		raw, err := json.Marshal(envelope)
		if err != nil {
			return batchRequest{}, &EncodeError{Err: err}
		}
		envelopes = append(envelopes, envelope)
		raws = append(raws, raw)
	}
	return batchRequest{Events: envelopes, rawEvents: raws}, nil
}

// poisonedEvent is one batch member a worker-path build could not serialize:
// the event's stamped id and its build error, attributed so the drop can
// name the event instead of condemning the whole batch.
type poisonedEvent struct {
	id  string
	err error
}

// buildBatchIsolating is buildBatch for the worker's retry paths: it reuses
// the already-marshaled envelopes a previous (retriably failed) attempt
// retained, for the longest leading prefix whose event ids still line up
// position by position, and builds only the events beyond it. Reuse is what
// keeps an in-process retry byte-identical to the bytes that failure
// spooled: rebuilding from the Event batch would re-marshal Props/Context,
// whose NESTED values the caller can mutate after Enqueue (intake clones one
// level deep), and drift bytes under the same event_id — one encoding per
// event_id everywhere: wire, disk, retry. A retained request that no longer
// corresponds (the batch was cleared, replaced, or reordered) contributes
// nothing and everything rebuilds.
//
// A member that cannot build — an unserializable Props/Context value the
// caller mutated in after Enqueue — is POISON: it is returned attributed in
// poisoned rather than failing the batch, so one bad event can never strand
// its batchmates (the previous whole-batch EncodeError settled every member,
// spooled copies included, for one event's mutation). kept is events minus
// the poisoned members, aligned with the returned request; when nothing
// poisons it is the input slice unchanged, and when something does the input
// slice's backing array is reused for the compaction — callers own the batch
// slice and must adopt kept in its place. Reused-prefix members carry bytes
// already marshaled once and can never poison.
func (c *Client) buildBatchIsolating(events []Event, retained batchRequest) (request batchRequest, kept []Event, poisoned []poisonedEvent) {
	reuse := len(retained.Events)
	if reuse > len(events) || len(retained.Events) != len(retained.rawEvents) {
		reuse = 0
	}
	for i := 0; i < reuse; i++ {
		// Compare in the envelope's canonical form: buildEnvelope TRIMS a
		// caller-supplied id before stamping it into the wire envelope, so a
		// padded Event.ID must still match the retained (trimmed) event_id —
		// a raw comparison would miss, rebuild, and drift from the bytes the
		// failure already spooled under that same event_id.
		if retained.Events[i].EventID != strings.TrimSpace(events[i].ID) {
			reuse = i
			break
		}
	}
	envelopes := make([]eventEnvelope, 0, len(events))
	raws := make([]json.RawMessage, 0, len(events))
	envelopes = append(envelopes, retained.Events[:reuse]...)
	raws = append(raws, retained.rawEvents[:reuse]...)
	kept = events
	for i, event := range events[reuse:] {
		envelope, err := c.buildEnvelope(event)
		var raw json.RawMessage
		if err == nil {
			raw, err = json.Marshal(envelope)
			if err != nil {
				err = &EncodeError{Err: err}
			}
		}
		if err != nil {
			if poisoned == nil {
				// First poison member: everything before it survives; the
				// compaction reuses the input's backing array from here on.
				kept = events[:reuse+i]
			}
			poisoned = append(poisoned, poisonedEvent{id: strings.TrimSpace(event.ID), err: err})
			continue
		}
		if poisoned != nil {
			kept = append(kept, event)
		}
		envelopes = append(envelopes, envelope)
		raws = append(raws, raw)
	}
	return batchRequest{Events: envelopes, rawEvents: raws}, kept, poisoned
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
