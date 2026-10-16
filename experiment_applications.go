package shardpilot

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"strings"
	"time"
)

// Applications are local spool records, never analytics envelopes. Converting
// one to a sealed envelope rewrites the same spool atomically, so neither a
// two-file handoff nor a restart can mint a fresh application identity.
type expStoredApplication struct {
	Scope         string            `json:"scope"`
	ExperimentKey string            `json:"experiment_key"`
	Entry         expEntry          `json:"entry"`
	Session       string            `json:"session"`
	ExposureID    string            `json:"exposure_id"`
	AppliedAt     string            `json:"applied_at"`
	Extra         bool              `json:"extra,omitempty"`
	Outcome       *expStoredOutcome `json:"outcome,omitempty"`
}

type expStoredOutcome struct {
	ID         string `json:"id"`
	OccurredAt string `json:"occurred_at"`
	Key        string `json:"key"`
	Value      int64  `json:"value"`
}

var errLocalExperimentApplication = errors.New("an unsealed experiment application is not an analytics envelope")
var errExperimentApplicationUnsaved = errors.New("application_handoff_unsaved")

func (a *expStoredApplication) id() string {
	if a.Outcome != nil {
		return "eap1_o_" + a.Outcome.ID
	}
	return "eap1_e_" + a.ExposureID
}

func validApplicationID(id string) bool {
	decoded, err := hex.DecodeString(id)
	return err == nil && len(decoded) == 16 && hex.EncodeToString(decoded) == id
}

func validStoredApplicationID(id string) bool {
	return (strings.HasPrefix(id, "eap1_e_") || strings.HasPrefix(id, "eap1_o_")) && validApplicationID(id[7:])
}

// A write acknowledgement can be lost after replacement. A later merge may
// retain both records; the sealed record's local link is authoritative even
// when its delivery clock has expired, so replay never resets that clock.
func sealedApplicationReplacements(wires []spoolEventWire) map[string]bool {
	replaced := make(map[string]bool)
	for _, wire := range wires {
		if len(wire.Application) != 0 || !wire.InternalFact || !validStoredApplicationID(wire.ApplicationID) {
			continue
		}
		var fact struct {
			ID   string `json:"event_id"`
			Name string `json:"event_name"`
			Seal string `json:"attestation_seal"`
		}
		if json.Unmarshal(wire.Raw, &fact) == nil && fact.ID != "" && fact.Seal != "" &&
			(fact.Name == experimentExposureName || fact.Name == experimentOutcomeName) {
			replaced[wire.ApplicationID] = true
		}
	}
	return replaced
}

func parseStoredApplication(raw json.RawMessage) *expStoredApplication {
	var a expStoredApplication
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&a) != nil || a.Scope == "" || a.ExperimentKey == "" || a.Session == "" ||
		!validApplicationID(a.ExposureID) || !validExperimentSubjectID(a.Entry.SubjectKey) ||
		a.Entry.AssignmentUnit != experimentAssignmentUnitClientID || a.Entry.Served == nil {
		return nil
	}
	if _, err := time.Parse(time.RFC3339Nano, a.AppliedAt); err != nil {
		return nil
	}
	validated := sanitizeExperimentEntries(map[string]expEntry{a.ExperimentKey: a.Entry})
	if entry, ok := validated[a.ExperimentKey]; !ok || entry.Served == nil {
		return nil
	} else {
		a.Entry = entry
	}
	if outcome := a.Outcome; outcome != nil {
		occurred, err := time.Parse(time.RFC3339Nano, outcome.OccurredAt)
		applied, _ := time.Parse(time.RFC3339Nano, a.AppliedAt)
		if !validApplicationID(outcome.ID) || !validExperimentOutcomeKey(outcome.Key) || err != nil || occurred.Before(applied) ||
			outcome.Value < -(1<<53) || outcome.Value > 1<<53 {
			return nil
		}
	}
	return &a
}

func applicationID(record *expOwedExposure) string {
	if record.outcome != nil {
		return "eap1_o_" + record.outcome.outcomeID
	}
	return "eap1_e_" + record.app.exposureID
}

// Called under e.mu; the spool mutex remains a leaf. No host callback runs.
func (c *Client) persistExperimentApplication(key string, record *expOwedExposure) bool {
	if record.intentDelivery == nil {
		record.intentDelivery = newEventDelivery(c.clock.Now())
	}
	if record.withdrawn {
		return false
	}
	if record.handoffComplete {
		return true
	}
	if !c.durableExperimentApplicationsAllowed() {
		return !record.intentDurable // a restored durable intent still needs a writable grant
	}
	if !c.exp.ensureSubjectPersistedLocked() {
		c.stats.spoolPersistFailed.Add(1) // a durable application needs its canonical identity
		return false
	}
	a := expStoredApplication{
		Scope: c.exp.scopeForLocked(record.entry.SubjectKey), ExperimentKey: key,
		Entry: *record.entry, Session: record.session, ExposureID: record.app.exposureID,
		AppliedAt: record.app.appliedAt, Extra: record.extra,
	}
	// Sealing needs the pinned construction, not the fetched variant payload.
	a.Entry.VariantPayload = nil
	if outcome := record.outcome; outcome != nil {
		a.Outcome = &expStoredOutcome{outcome.outcomeID, outcome.occurredAt, outcome.key, outcome.value}
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return false
	}
	entry := spoolEntry{id: a.id(), raw: raw, internalFact: true, application: true, delivery: record.intentDelivery}
	s := c.spool
	if spoolEntryExpired(entry, c.clock.Now()) {
		c.settleApplicationRetentionLocked(SpoolDropExpired, []spoolEntry{entry})
		if report := unreportedSpoolExpired([]spoolEntry{entry}); len(report) > 0 {
			c.stats.spoolExpired.Add(uint64(len(report)))
			c.deferSpoolLetter(spoolDeadLetterFrom(SpoolDropExpired, report))
		}
		return false
	}
	refused, added, expired, evicted, failed := s.appendExcept([]spoolEntry{entry}, 0, false, c.clock.Now(), func() bool {
		return c.consent.Load() == consentStateGranted && s.grantPersisted
	}, func() map[string]struct{} {
		// An earlier admission cannot be re-created after another writer
		// evicts it. The exclusion and append share the spool lock; a
		// pending disk failure may retry its save, never the application.
		_, present := s.ids[entry.id]
		if record.intentAdmitted && !present {
			return map[string]struct{}{entry.id: {}}
		}
		record.intentAdmitted = true
		return nil
	})
	if len(added) > 0 && !failed {
		c.stats.spooled.Add(uint64(len(added)))
	}
	c.settleApplicationRetentionLocked(SpoolDropExpired, expired)
	if report := unreportedSpoolExpired(expired); len(report) > 0 {
		c.stats.spoolExpired.Add(uint64(len(report)))
		c.deferSpoolLetter(spoolDeadLetterFrom(SpoolDropExpired, report))
	}
	c.settleApplicationRetentionLocked(SpoolDropCapacity, evicted)
	if len(evicted) > 0 {
		c.stats.spoolEvicted.Add(uint64(len(evicted)))
		c.deferSpoolLetter(spoolDeadLetterFrom(SpoolDropCapacity, evicted))
	}
	if failed {
		c.stats.spoolPersistFailed.Add(1) // under a plane lock: never invoke host logging
	}
	s.mu.Lock()
	_, present := s.ids[entry.id]
	retained := present && !s.dirty && !s.owed && !spoolEntryExpired(entry, c.clock.Now())
	settledAbsence := !refused && !failed && record.intentAdmitted && !present && !s.dirty && !s.owed
	s.mu.Unlock()
	if settledAbsence && !record.withdrawn {
		c.settleApplicationRetentionLocked(SpoolDropCapacity, []spoolEntry{entry})
	}
	if !refused && !failed && retained {
		record.intentDurable = true
	}
	return !refused && !failed && retained
}

// The same receipt follows a point application through its owed record,
// queue, worker and durable copy. Each actual consent drop can report it,
// including a handoff race, but only one can spend the receipt. Automatic
// exposures retain their existing re-arm behavior instead.
func (c *Client) countPointApplicationConsentDrop(id string, extra bool, delivery *eventDelivery) {
	if !extra || !validStoredApplicationID(id) || delivery == nil || !delivery.pointConsentReported.CompareAndSwap(false, true) {
		return
	}
	if strings.HasPrefix(id, "eap1_o_") {
		c.stats.recordExperimentOutcomeDrop(expDropConsentWithdrawn, 1)
	} else {
		c.stats.recordExperimentExposureDrop(expDropConsentWithdrawn, 1)
	}
}

func (c *Client) countPointEventConsentDrop(event Event) {
	c.countPointApplicationConsentDrop(event.expApplicationID, event.expApplicationExtra, event.delivery)
}

func (c *Client) durableExperimentApplicationsAllowed() bool {
	return c.spool != nil && c.consent.Load() == consentStateGranted &&
		!(c.consentFloorEnabled() && c.consentFloorUserScoped())
}

func (c *Client) retireExperimentApplication(_ string, record *expOwedExposure) {
	record.withdrawn = true
	if c.spool == nil {
		return
	}
	id := applicationID(record)
	c.removeStoredApplications(func(a *expStoredApplication) bool { return a.id() == id })
}

func (c *Client) withdrawExperimentApplications(key, subject string) {
	if c.spool != nil {
		c.removeStoredApplications(func(a *expStoredApplication) bool {
			return a.ExperimentKey == key && a.Entry.SubjectKey == subject
		})
	}
}

func (c *Client) removeStoredApplications(matches func(*expStoredApplication) bool) {
	c.removeStoredApplicationsReporting(matches, true)
}

func (c *Client) removeStoredApplicationsReporting(matches func(*expStoredApplication) bool, terminalLetter bool) {
	removed, failed := c.spool.removeMatching(func(raw json.RawMessage) bool {
		a := parseStoredApplication(raw)
		return a != nil && matches(a)
	}, c.expFactPurgeEpoch.Load())
	if failed {
		c.stats.spoolPersistFailed.Add(1) // under a plane lock: never invoke host logging
	}
	if terminalLetter {
		c.deferSpoolLetter(spoolDeadLetterFrom(SpoolDropTerminal, removed))
	}
}

func (c *Client) applicationDurableAtClose(record *expOwedExposure) bool {
	if !c.durableExperimentApplicationsAllowed() || record.withdrawn {
		return false
	}
	id := applicationID(record)
	if record.handoffComplete && record.intentEvent != nil {
		id = record.intentEvent.ID
	} else if !record.intentDurable {
		return false
	}
	c.spool.mu.Lock()
	defer c.spool.mu.Unlock()
	_, present := c.spool.ids[id]
	return present && !c.spool.owed && !record.intentDelivery.expired(c.clock.Now())
}

// Construction already counted generic load losses before experiments existed.
// Attribute their local application kinds once, without repeating letters or
// settling owed records: dropped applications were never restored into those.
func (c *Client) recordLoadedApplicationRetention(outcome spoolLoadOutcome) {
	e := c.exp
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, drop := range []struct {
		reason  string
		entries []spoolEntry
	}{{expDropRetryAgeExpired, outcome.expired}, {expDropSpoolCapacity, outcome.evicted}} {
		for _, entry := range drop.entries {
			if !entry.application {
				continue
			}
			a := parseStoredApplication(entry.raw)
			if a == nil || a.Scope != e.scopeForLocked(a.Entry.SubjectKey) {
				continue
			}
			if a.Outcome != nil {
				e.countOutcomeDropLocked(drop.reason, 1)
			} else {
				e.countDropLocked(drop.reason, 1)
			}
		}
	}
}

func (c *Client) restoreExperimentApplications() {
	if c.exp == nil || c.spool == nil {
		return
	}
	e, s := c.exp, c.spool
	e.mu.Lock()
	defer e.mu.Unlock()
	s.mu.Lock()
	entries := append([]spoolEntry(nil), s.entries...)
	s.mu.Unlock()
	for _, entry := range entries {
		if !entry.application {
			continue
		}
		a := parseStoredApplication(entry.raw)
		if a == nil || a.Scope != e.scopeForLocked(a.Entry.SubjectKey) {
			continue
		}
		duplicate := false
		for _, owed := range e.pendingExposure[a.ExperimentKey] {
			duplicate = duplicate || applicationID(owed) == entry.id
		}
		if duplicate {
			continue
		}
		owed := &expOwedExposure{entry: &a.Entry, session: a.Session, app: expApplication{a.ExposureID, a.AppliedAt}, extra: a.Extra, intentDelivery: entry.delivery, intentDurable: true, intentAdmitted: true}
		if outcome := a.Outcome; outcome != nil {
			owed.outcome = &expOutcomeRecord{outcome.ID, outcome.OccurredAt, outcome.Key, outcome.Value}
		}
		e.appendOwedLocked(a.ExperimentKey, owed)
	}
}

// A failed atomic conversion restores the application mirror and blocks the
// wire. The next attempt reconciles whatever the filesystem actually kept.
func (s *diskSpool) replaceApplication(id string, fact spoolEntry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owed || !s.grantPersisted {
		return false, nil
	}
	if _, present := s.ids[id]; !present {
		if _, converted := s.ids[fact.id]; !converted {
			return false, nil // a retention drop cannot become a new sealed admission
		}
	}
	oldEntries, oldResend := append([]spoolEntry(nil), s.entries...), append([]spoolEntry(nil), s.resend...)
	oldIDs, oldSettled := maps.Clone(s.ids), maps.Clone(s.settledIDs)
	oldFIFO := append([]string(nil), s.settledFIFO...)
	oldBytes, oldDeadline := s.totalBytes, s.retryAfterUntilMS
	oldDeferred := append([]spoolEntry(nil), s.deferredCapacityDrops...)
	kept := make([]spoolEntry, 0, len(s.entries)+1)
	for _, entry := range s.entries {
		if entry.id != id {
			kept = append(kept, entry)
		} else {
			s.totalBytes -= len(entry.raw)
		}
	}
	s.entries = kept
	delete(s.ids, id)
	s.recordSettledLocked(id)
	if _, exists := s.ids[fact.id]; !exists {
		s.entries = append(s.entries, fact)
		s.ids[fact.id] = struct{}{}
		s.totalBytes += len(fact.raw)
	}
	if err := s.saveLocked(); err != nil {
		s.entries, s.resend, s.ids = oldEntries, oldResend, oldIDs
		s.settledIDs, s.settledFIFO = oldSettled, oldFIFO
		s.totalBytes, s.retryAfterUntilMS, s.deferredCapacityDrops = oldBytes, oldDeadline, oldDeferred
		s.dirty, s.fileAppendable = true, false
		return false, err
	}
	_, exists := s.ids[fact.id]
	return exists, nil
}

func (c *Client) durablyHandoffApplication(key string, record *expOwedExposure, event Event) (Event, error) {
	// Withdrawal and the complete disk handoff share one lock. A consumer
	// stamp alone cannot protect a durable record whose stamp is local to
	// this process and cannot survive the next launch.
	e := c.exp
	e.mu.Lock()
	defer e.mu.Unlock()
	current := false
	for _, pending := range e.pendingExposure[key] {
		current = current || pending == record
	}
	if record.withdrawn || !current || c.experimentConsentRefusal() != nil {
		return event, errExperimentApplicationUnsaved
	}
	if record.handoffComplete && record.intentEvent != nil {
		return *record.intentEvent, nil
	}
	if !c.durableExperimentApplicationsAllowed() {
		if record.intentDurable {
			return event, errExperimentApplicationUnsaved
		}
		return event, nil
	}
	if record.intentEvent != nil {
		event = *record.intentEvent
	} else {
		var err error
		event.expApplicationID, event.expApplicationExtra = applicationID(record), record.extra
		event, err = c.prepareEvent(event)
		if err != nil {
			return event, err
		}
		event.delivery = record.intentDelivery
		if err := c.encodeAtIntake(&event); err != nil {
			return event, err
		}
		record.intentEvent = &event
	}
	request, err := c.buildBatch([]Event{event})
	if err != nil {
		return event, errExperimentApplicationUnsaved
	}
	entries := spoolEntriesFromRequest(request)
	if len(entries) != 1 {
		return event, errExperimentApplicationUnsaved
	}
	retained, persistErr := c.spool.replaceApplication(applicationID(record), entries[0])
	if persistErr != nil {
		c.stats.spoolPersistFailed.Add(1) // under a plane lock: never invoke host logging
	}
	if drops := c.spool.takeCapacityDrops(); len(drops) > 0 {
		c.settleApplicationRetentionLocked(SpoolDropCapacity, drops)
		c.stats.spoolEvicted.Add(uint64(len(drops)))
		c.deferSpoolLetter(spoolDeadLetterFrom(SpoolDropCapacity, drops))
	}
	if !retained || persistErr != nil {
		return event, errExperimentApplicationUnsaved
	}
	record.handoffComplete = true
	return event, nil
}

func (c *Client) countPointRequestConsentDrop(request batchRequest) {
	entries := request.retryEntries
	if entries == nil {
		entries = spoolEntriesFromRequest(request)
	}
	for _, entry := range entries {
		if entry.internalFact && !entry.application {
			c.countPointApplicationConsentDrop(entry.applicationID, entry.applicationExtra, entry.delivery)
		}
	}
}

// Under e.mu, settle the same local application in both retention owners.
// Ordinary spool writes call this before dispatching their off-lock callback;
// experiment writes call it directly and defer the callback. No host code runs.
func (c *Client) settleApplicationRetentionLocked(reason SpoolDropReason, entries []spoolEntry) {
	if c.exp == nil || (reason != SpoolDropCapacity && reason != SpoolDropExpired) {
		return
	}
	ids := make(map[string]bool)
	for _, entry := range entries {
		if entry.application {
			ids[entry.id] = true
		}
	}
	if len(ids) == 0 {
		return
	}
	drop := expDropSpoolCapacity
	if reason == SpoolDropExpired {
		drop = expDropRetryAgeExpired
		// append rejects an expired duplicate without removing its older
		// stored form. Withdraw that source too, without a second letter.
		c.removeStoredApplicationsReporting(func(a *expStoredApplication) bool { return ids[a.id()] }, false)
	}
	for key, list := range c.exp.pendingExposure {
		kept := list[:0]
		for _, record := range list {
			if !record.handoffComplete && ids[applicationID(record)] {
				if !record.withdrawn {
					record.withdrawn = true
					c.exp.countOwedDropLocked(drop, record)
				}
				continue
			}
			kept = append(kept, record)
		}
		if len(kept) == 0 {
			delete(c.exp.pendingExposure, key)
		} else {
			c.exp.pendingExposure[key] = kept
		}
	}
}
