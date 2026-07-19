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

	// Disk-spool counters (always zero when Config.SpoolDir is unset).
	// Spooled counts events durably appended to the spool (survivors only —
	// an append the caps immediately evicted from is not counted);
	// SpoolResent counts spooled events re-published from a previous
	// process's record and confirmed delivered by the response's per-event
	// verdicts (an event the response marked rejected or consent-suppressed
	// dead-letters instead of counting); SpoolEvicted counts oldest-first cap
	// evictions; SpoolExpired counts retry-age-cap drops (older than 7 days,
	// future-dated beyond the skew tolerance, or undatable); and
	// SpoolPersistFailed counts failed spool record writes (the in-memory
	// mirror stays authoritative and the write is retried on the flush
	// cadence). Every SpoolEvicted/SpoolExpired drop is also reported through
	// Config.OnSpoolDeadLetter. SpoolForeignMerged counts on-disk records a
	// merging spool save found that this client neither holds nor settled —
	// i.e. another writer's mutations detected on a shared SpoolDir (one
	// client per SpoolDir is the supported topology; the reload-and-merge
	// save is the safety net, and this counter is how sharing shows up).
	Spooled            uint64
	SpoolResent        uint64
	SpoolEvicted       uint64
	SpoolExpired       uint64
	SpoolPersistFailed uint64
	SpoolForeignMerged uint64
}

type statsCollector struct {
	enqueued      atomic.Uint64
	dropped       atomic.Uint64
	published     atomic.Uint64
	failedBatches atomic.Uint64
	accepted      atomic.Uint64
	rejected      atomic.Uint64
	duplicates    atomic.Uint64

	spooled            atomic.Uint64
	spoolResent        atomic.Uint64
	spoolEvicted       atomic.Uint64
	spoolExpired       atomic.Uint64
	spoolPersistFailed atomic.Uint64
	spoolForeignMerged atomic.Uint64

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
		Enqueued:           s.enqueued.Load(),
		Dropped:            s.dropped.Load(),
		Published:          s.published.Load(),
		FailedBatches:      s.failedBatches.Load(),
		Accepted:           s.accepted.Load(),
		Rejected:           s.rejected.Load(),
		Duplicates:         s.duplicates.Load(),
		ByStatus:           byStatus,
		LastError:          lastError,
		Spooled:            s.spooled.Load(),
		SpoolResent:        s.spoolResent.Load(),
		SpoolEvicted:       s.spoolEvicted.Load(),
		SpoolExpired:       s.spoolExpired.Load(),
		SpoolPersistFailed: s.spoolPersistFailed.Load(),
		SpoolForeignMerged: s.spoolForeignMerged.Load(),
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

// setLastError surfaces a non-batch operational failure (a failed remote-
// config cache or spool/consent record write) in Stats.LastError without
// counting a failed batch — no batch failed.
func (s *statsCollector) setLastError(message string) {
	s.mu.Lock()
	s.lastError = message
	s.mu.Unlock()
}
