package shardpilot

import (
	"encoding/json"
	"errors"
	"strings"
)

var errExperimentAgePending = errors.New("experiment facts await a current host age decision")

func (c *Client) experimentAgeHeld(key string, subject *ExperimentSubject) bool {
	if subject != nil {
		subject.mu.Lock()
		defer subject.mu.Unlock()
		return subject.declaring[key] != nil
	}
	if c.exp == nil {
		return true
	}
	c.exp.mu.Lock()
	defer c.exp.mu.Unlock()
	return c.exp.declaring[key] != nil || !c.exp.ageConfirmed[key]
}

func (c *Client) ageHeldEvent(event Event) bool {
	if event.attestationSeal == "" || !isExperimentFactClassEvent(event) {
		return false
	}
	key, _ := event.Props["experiment_key"].(string)
	return c.experimentAgeHeld(key, event.subject)
}

func (c *Client) ageHeldRaw(raw json.RawMessage, subject *ExperimentSubject) bool {
	var fact struct {
		Name  string `json:"event_name"`
		Seal  string `json:"attestation_seal"`
		Props struct {
			Key string `json:"experiment_key"`
		} `json:"props"`
	}
	if json.Unmarshal(raw, &fact) != nil || fact.Seal == "" || (fact.Name != experimentExposureName && fact.Name != experimentOutcomeName) {
		return false
	}
	return c.experimentAgeHeld(fact.Props.Key, subject)
}

// Requests retain their actual bytes. This last check also covers a
// declaration made after the worker partitioned a batch, and raw replay.
func (c *Client) ageHeldRequest(request batchRequest) bool {
	if len(request.Events) == len(request.rawEvents) {
		for _, event := range request.Events {
			if event.AttestationSeal == "" || (event.EventName != experimentExposureName && event.EventName != experimentOutcomeName) {
				continue
			}
			key, _ := event.Props["experiment_key"].(string)
			if c.experimentAgeHeld(key, event.subject) {
				return true
			}
		}
		return false
	}
	for i, raw := range request.rawEvents {
		if len(request.retryEntries) == len(request.rawEvents) && !request.retryEntries[i].internalFact {
			continue
		}
		var subject *ExperimentSubject
		if i < len(request.Events) {
			subject = request.Events[i].subject
		}
		if c.ageHeldRaw(raw, subject) {
			return true
		}
	}
	return false
}

// Only SDK-sealed facts are held. Their intake encoding is immutable, so
// rejoining them after a publish preserves the original bytes and IDs.
func (c *Client) separateAgeHeld(batch []Event, held *[]Event) []Event {
	var ready []Event
	ids := make(map[string]struct{})
	for i, event := range batch {
		if c.ageHeldEvent(event) {
			if ready == nil {
				ready = append(make([]Event, 0, len(batch)), batch[:i]...)
			}
			*held = append(*held, event)
			ids[strings.TrimSpace(event.ID)] = struct{}{}
		} else if ready != nil {
			ready = append(ready, event)
		}
	}
	if len(ids) == 0 {
		return batch
	}
	c.retainedRequest = withoutEnvelopeIDs(c.retainedRequest, ids)
	return ready
}

func (c *Client) rejoinAgeHeld(batch, held []Event) []Event {
	for _, event := range held {
		if event.intakeConsentEpoch < c.consentEpoch.Load() || c.isWithdrawnExperimentFactEvent(event) || c.isKeyWithdrawnExperimentFactEvent(event) || subjectFactWithdrawn(event.subject, event.subjectStamp, event.Props) {
			c.stats.dropped.Add(1)
			continue
		}
		batch = append(batch, event)
	}
	return batch
}

func (c *Client) separateAgeHeldSpool(chunk []spoolEntry, held *[]spoolEntry) []spoolEntry {
	ready := chunk[:0]
	for _, entry := range chunk {
		if entry.internalFact && c.ageHeldRaw(entry.raw, nil) {
			*held = append(*held, entry)
		} else {
			ready = append(ready, entry)
		}
	}
	return ready
}
