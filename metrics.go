package shardpilot

import (
	"sync"
	"sync/atomic"
)

type Stats struct {
	Enqueued      uint64
	Dropped       uint64
	Published     uint64
	FailedBatches uint64
	Accepted      uint64
	Rejected      uint64
	Duplicates    uint64
	// ByStatus is the cumulative count of per-event ingest outcomes folded
	// from the events[] list of every batch response, keyed by status. Every
	// reported status is counted, including accepted/rejected/duplicate; the
	// value of the breakdown over the three aggregate counters above is the
	// finer outcomes they cannot express on their own (such as
	// EventStatusObserved, EventStatusSuppressedNoConsent, and
	// EventStatusSuppressedAdRevenueConsent). It is forward-compatible with
	// statuses the server adds later. Each Snapshot returns a fresh copy; it
	// is nil until a batch response carrying a per-event list is recorded.
	ByStatus  map[EventStatus]uint64
	LastError string
}

type statsCollector struct {
	enqueued      atomic.Uint64
	dropped       atomic.Uint64
	published     atomic.Uint64
	failedBatches atomic.Uint64
	accepted      atomic.Uint64
	rejected      atomic.Uint64
	duplicates    atomic.Uint64

	mu        sync.Mutex
	lastError string
	byStatus  map[EventStatus]uint64
}

func (s *statsCollector) snapshot() Stats {
	s.mu.Lock()
	lastError := s.lastError
	var byStatus map[EventStatus]uint64
	if len(s.byStatus) > 0 {
		byStatus = make(map[EventStatus]uint64, len(s.byStatus))
		for status, count := range s.byStatus {
			byStatus[status] = count
		}
	}
	s.mu.Unlock()

	return Stats{
		Enqueued:      s.enqueued.Load(),
		Dropped:       s.dropped.Load(),
		Published:     s.published.Load(),
		FailedBatches: s.failedBatches.Load(),
		Accepted:      s.accepted.Load(),
		Rejected:      s.rejected.Load(),
		Duplicates:    s.duplicates.Load(),
		ByStatus:      byStatus,
		LastError:     lastError,
	}
}

func (s *statsCollector) recordBatch(result batchResult, size int) {
	s.published.Add(uint64(size))
	s.accepted.Add(uint64(result.Accepted))
	s.rejected.Add(uint64(result.Rejected))
	s.duplicates.Add(uint64(result.Duplicates))

	if len(result.Events) == 0 {
		return
	}
	s.mu.Lock()
	if s.byStatus == nil {
		s.byStatus = make(map[EventStatus]uint64, len(result.Events))
	}
	for _, event := range result.Events {
		s.byStatus[EventStatus(event.Status)]++
	}
	s.mu.Unlock()
}

func (s *statsCollector) recordFailure(err error) {
	s.failedBatches.Add(1)
	s.mu.Lock()
	s.lastError = err.Error()
	s.mu.Unlock()
}
