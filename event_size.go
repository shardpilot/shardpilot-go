package shardpilot

import (
	"bytes"
	"encoding/json"
	"strings"
)

// maxEventBytes is the ingest per-event cap. An event whose serialized JSON,
// with surrounding whitespace trimmed, is longer than this is rejected as
// event_too_large; an event of exactly this many bytes is admitted.
const maxEventBytes = 2048

// eventExceedsSizeLimit applies the ingest size rule to one wire element:
// the trimmed byte length is over the cap, unless the element is a retired
// experiment fact, which ingest leaves to its schema-version refusal.
func eventExceedsSizeLimit(raw []byte, eventName string, schemaVersion int) bool {
	return len(bytes.TrimSpace(raw)) > maxEventBytes && !retiredExperimentFact(eventName, schemaVersion)
}

// retiredExperimentFact reports an experiment exposure or outcome at schema
// version 1, the retired experiment major.
func retiredExperimentFact(eventName string, schemaVersion int) bool {
	switch strings.TrimSpace(eventName) {
	case experimentExposureName, experimentOutcomeName:
		return schemaVersion == 1
	}
	return false
}

// intakeEncoding is an event's envelope and wire bytes, built once at intake.
type intakeEncoding struct {
	envelope eventEnvelope
	raw      json.RawMessage
}

// encodeAtIntake builds and marshals a prepared event exactly as its batch
// will send it and applies the ingest size rule to those bytes. An oversize
// event is refused with ErrEventTooLarge. Otherwise the encoding is kept on
// the event, so the measured bytes are the bytes sent, spooled and resent.
// An event that cannot be built or marshaled is left unencoded: its batch
// build reports the failure for that member, as before.
func (c *Client) encodeAtIntake(event *Event) error {
	envelope, err := c.buildEnvelope(*event)
	if err != nil {
		return nil
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return nil
	}
	if eventExceedsSizeLimit(raw, envelope.EventName, envelope.SchemaVersion) {
		return ErrEventTooLarge
	}
	event.encoded = &intakeEncoding{envelope: envelope, raw: raw}
	return nil
}

// encodeEvent returns an event's wire envelope and bytes: its intake
// encoding when it has one, else a fresh build. A fresh build is measured
// like an intake one: over the cap it fails as ErrEventTooLarge, so a batch
// drops that member alone.
func (c *Client) encodeEvent(event Event) (eventEnvelope, json.RawMessage, error) {
	if event.encoded != nil {
		envelope := event.encoded.envelope
		envelope.delivery = event.delivery
		return envelope, event.encoded.raw, nil
	}
	envelope, err := c.buildEnvelope(event)
	if err != nil {
		return eventEnvelope{}, nil, err
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return eventEnvelope{}, nil, &EncodeError{Err: err}
	}
	if eventExceedsSizeLimit(raw, envelope.EventName, envelope.SchemaVersion) {
		return eventEnvelope{}, nil, &EncodeError{Err: ErrEventTooLarge}
	}
	return envelope, raw, nil
}

// refuseOversizeEvent counts an intake size refusal.
func (c *Client) refuseOversizeEvent() error {
	c.stats.dropped.Add(1)
	c.stats.setLastError(ErrEventTooLarge.Error())
	return ErrEventTooLarge
}
