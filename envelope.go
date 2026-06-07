package shardpilot

import (
	"fmt"
	"strings"
	"time"
)

type batchRequest struct {
	Events []eventEnvelope `json:"events"`
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

	return eventEnvelope{
		EventID:         id,
		SchemaVersion:   1,
		EventName:       name,
		Source:          c.cfg.Source,
		EventTS:         timestamp.UTC().Format(time.RFC3339Nano),
		WorkspaceID:     c.cfg.WorkspaceID,
		AppID:           c.cfg.AppID,
		EnvironmentID:   c.cfg.EnvironmentID,
		UserID:          event.UserID,
		AnonymousID:     event.AnonymousID,
		SessionID:       event.SessionID,
		SessionSequence: event.SessionSequence,
		Platform:        platform,
		AppVersion:      appVersion,
		AppBuild:        appBuild,
		Context:         cloneMap(event.Context),
		Props:           props,
	}, nil
}

func (c *Client) buildBatch(events []Event) (batchRequest, error) {
	envelopes := make([]eventEnvelope, 0, len(events))
	for _, event := range events {
		envelope, err := c.buildEnvelope(event)
		if err != nil {
			return batchRequest{}, err
		}
		envelopes = append(envelopes, envelope)
	}
	return batchRequest{Events: envelopes}, nil
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
