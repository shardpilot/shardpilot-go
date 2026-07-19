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
	anonymousID := firstNonEmpty(event.AnonymousID, c.cfg.AnonymousID)

	return eventEnvelope{
		EventID:         id,
		SchemaVersion:   1,
		EventName:       name,
		Source:          c.cfg.Source,
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
