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
	LastError     string
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
}

func (s *statsCollector) snapshot() Stats {
	s.mu.Lock()
	lastError := s.lastError
	s.mu.Unlock()

	return Stats{
		Enqueued:      s.enqueued.Load(),
		Dropped:       s.dropped.Load(),
		Published:     s.published.Load(),
		FailedBatches: s.failedBatches.Load(),
		Accepted:      s.accepted.Load(),
		Rejected:      s.rejected.Load(),
		Duplicates:    s.duplicates.Load(),
		LastError:     lastError,
	}
}

func (s *statsCollector) recordBatch(result batchResult, size int) {
	s.published.Add(uint64(size))
	s.accepted.Add(uint64(result.Accepted))
	s.rejected.Add(uint64(result.Rejected))
	s.duplicates.Add(uint64(result.Duplicates))
}

func (s *statsCollector) recordFailure(err error) {
	s.failedBatches.Add(1)
	s.mu.Lock()
	s.lastError = err.Error()
	s.mu.Unlock()
}
