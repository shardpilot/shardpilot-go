package shardpilot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sync/atomic"
	"time"
)

// analyticsDedupWindow matches the ingest service's
// internal/config.DefaultIdempotencyCommittedTTL (15 minutes). Retrying
// beyond that window can create another accepted copy of an event.
const analyticsDedupWindow = 15 * time.Minute

// eventDelivery is local metadata, never part of the event envelope. A
// shared pointer keeps the clock across retained-batch rebuilds and spool
// copies without changing the event's ID, timestamp or serialized bytes.
type eventDelivery struct {
	intakeMS        int64
	ambiguousMS     atomic.Int64
	expiredReported atomic.Bool
}

func newEventDelivery(now time.Time) *eventDelivery {
	return deliveryFromRecord(now.UnixMilli(), 0)
}

func deliveryFromRecord(intake, ambiguous int64) *eventDelivery {
	d := &eventDelivery{intakeMS: intake}
	d.ambiguousMS.Store(ambiguous)
	return d
}

func (d *eventDelivery) expired(now time.Time) bool {
	if d == nil || d.intakeMS <= 0 {
		return true
	}
	ms := now.UnixMilli()
	if d.intakeMS > ms || ms-d.intakeMS >= spoolRetryAgeCap.Milliseconds() {
		return true
	}
	first := d.ambiguousMS.Load()
	return first < 0 || (first != 0 && (first < d.intakeMS || first > ms || ms-first >= analyticsDedupWindow.Milliseconds()))
}

func (entry spoolEntry) wire() spoolEventWire {
	wire := spoolEventWire{Raw: entry.raw, InternalFact: entry.internalFact}
	if entry.delivery != nil {
		wire.IntakeUnixMS = entry.delivery.intakeMS
		wire.AmbiguousUnixMS = entry.delivery.ambiguousMS.Load()
	}
	return wire
}

// persistDelivery fences changes to clocks of already-spooled envelopes
// before transport can send them. Fresh memory-only events need no disk
// entry. A failed fence prevents the attempt; the retained batch retries.
func (s *diskSpool) persistDelivery(entries []spoolEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owed {
		return ErrConsentDenied
	}
	incoming := make(map[string]spoolEntry, len(entries))
	for _, entry := range entries {
		incoming[entry.id] = entry
	}
	matched := false
	for i, mirrored := range s.entries {
		entry, ok := incoming[mirrored.id]
		if ok && bytes.Equal(entry.raw, mirrored.raw) {
			s.entries[i].delivery = entry.delivery
			matched = true
		}
	}
	if !matched {
		return nil
	}
	data, err := s.readRecordBytesLocked()
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	doc := parseSpoolDocument(data)
	if !doc.ok {
		return s.saveLocked()
	}
	for _, saved := range doc.live() {
		entry, ok := incoming[saved.id]
		if !ok {
			continue
		}
		// Unrelated dirty work must not block a retry whose clock is already
		// durable. A memory-only entry absent from disk needs no fence either.
		if !bytes.Equal(saved.raw, entry.raw) || saved.delivery.intakeMS != entry.delivery.intakeMS || saved.delivery.ambiguousMS.Load() != entry.delivery.ambiguousMS.Load() {
			return s.saveLocked()
		}
	}
	return nil
}

// Expiry is retryable at the last HTTP handoff: the worker's next pass
// removes expired members individually while preserving fresh batchmates.
var errAnalyticsRetryExpired = errors.New("analytics retry age expired before HTTP handoff")

type analyticsAttemptKey struct{}
type analyticsAttempt func() (finish func(definitelyNotDelivered bool), err error)

func (c *Client) analyticsAttemptContext(ctx context.Context, request batchRequest) (context.Context, context.CancelFunc) {
	entries := request.retryEntries
	if entries == nil {
		entries = spoolEntriesFromRequest(request)
	}
	// Bound connection waits and response reads by the remaining window too.
	// The wall metadata survives restart; this process also gets a deadline
	// that cannot be extended by a later wall-clock correction.
	remaining := analyticsDedupWindow
	now := c.clock.Now().UnixMilli()
	for _, entry := range entries {
		if entry.delivery == nil {
			remaining = 0
			continue
		}
		outer := time.Duration(entry.delivery.intakeMS+spoolRetryAgeCap.Milliseconds()-now) * time.Millisecond
		if outer < remaining {
			remaining = outer
		}
		if first := entry.delivery.ambiguousMS.Load(); first != 0 {
			retry := time.Duration(first+analyticsDedupWindow.Milliseconds()-now) * time.Millisecond
			if retry < remaining {
				remaining = retry
			}
		}
	}
	ctx, cancel := context.WithTimeout(ctx, remaining)
	begin := analyticsAttempt(func() (func(bool), error) {
		if c.ageHeldRequest(request) {
			return nil, errExperimentAgePending
		}
		now := c.clock.Now()
		for _, entry := range entries {
			if spoolEntryExpired(entry, now) {
				return nil, errAnalyticsRetryExpired
			}
		}
		// Persist a tentative clock BEFORE send. A process exit while the
		// request is in flight leaves an ambiguous outcome. Definite offline
		// failures and definite rejections roll back only this new candidate;
		// they can never erase a previous ambiguous attempt's clock.
		started := make([]*eventDelivery, 0, len(entries))
		for _, entry := range entries {
			if entry.delivery.ambiguousMS.CompareAndSwap(0, now.UnixMilli()) {
				started = append(started, entry.delivery)
			}
		}
		rollback := func() {
			for _, d := range started {
				d.ambiguousMS.CompareAndSwap(now.UnixMilli(), 0)
			}
		}
		persist := func() error {
			if c.spool == nil {
				return nil
			}
			if err := c.spool.persistDelivery(entries); err != nil {
				c.recordSpoolPersistFailure()
				return fmt.Errorf("persist analytics retry clock: %w", err)
			}
			return nil
		}
		if err := persist(); err != nil {
			rollback()
			return nil, err
		}
		return func(definitelyNotDelivered bool) {
			if definitelyNotDelivered && len(started) != 0 {
				rollback()
				// A failed rollback retains the conservative clock on disk. The
				// mirror retries that rewrite and reports the persistence failure.
				_ = persist()
			}
		}, nil
	})
	return context.WithValue(ctx, analyticsAttemptKey{}, begin), cancel
}

func (c *Client) settleExpiredAnalytics(entries []spoolEntry) {
	if len(entries) == 0 {
		return
	}
	if c.spool != nil {
		_, failed := c.spool.ack(spoolEntryIDs(entries))
		if failed {
			c.recordSpoolPersistFailure()
		}
	}
	c.stats.dropped.Add(uint64(c.recordSpoolExpired(entries)))
}

func (c *Client) expireBuiltBatch(request batchRequest, batch []Event) (batchRequest, []Event) {
	var expired []spoolEntry
	kept := batchRequest{expPurgeEpoch: request.expPurgeEpoch}
	events := make([]Event, 0, len(batch))
	now := c.clock.Now()
	for i, entry := range spoolEntriesFromRequest(request) {
		if spoolEntryExpired(entry, now) {
			expired = append(expired, entry)
			continue
		}
		kept.Events = append(kept.Events, request.Events[i])
		kept.rawEvents = append(kept.rawEvents, request.rawEvents[i])
		events = append(events, batch[i])
	}
	if len(expired) == 0 {
		return request, batch
	}
	c.settleExpiredAnalytics(expired)
	return kept, events
}

func (c *Client) expireResendChunk(chunk []spoolEntry) []spoolEntry {
	var expired []spoolEntry
	kept := make([]spoolEntry, 0, len(chunk))
	now := c.clock.Now()
	for _, entry := range chunk {
		if spoolEntryExpired(entry, now) {
			expired = append(expired, entry)
		} else {
			kept = append(kept, entry)
		}
	}
	c.settleExpiredAnalytics(expired)
	return kept
}
