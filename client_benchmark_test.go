package shardpilot

import (
	"fmt"
	"testing"
	"time"
)

var (
	benchmarkBatchSink batchRequest
	benchmarkStatsSink Stats
)

func BenchmarkEnqueueMapPayload(b *testing.B) {
	client := &Client{
		queue: newBoundedQueue(1024),
	}
	event := benchmarkEvent()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Enqueue(event); err != nil {
			b.Fatalf("enqueue benchmark event: %v", err)
		}
		if len(client.queue.ch) == cap(client.queue.ch) {
			drainBenchmarkQueue(client.queue)
		}
	}
}

func BenchmarkBuildBatchMapPayloads(b *testing.B) {
	client := &Client{
		cfg: Config{
			WorkspaceID:   "workspace-bench",
			AppID:         "app-bench",
			EnvironmentID: "develop",
			Source:        SourceBackend,
		},
		clock: benchmarkClock{t: time.Unix(1700000000, 0).UTC()},
	}
	events := make([]Event, 64)
	for i := range events {
		event := benchmarkEvent()
		event.ID = fmt.Sprintf("evt-bench-%d", i)
		events[i] = event
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch, err := client.buildBatch(events)
		if err != nil {
			b.Fatalf("build benchmark batch: %v", err)
		}
		benchmarkBatchSink = batch
	}
}

func BenchmarkStatsCollectorAggregation(b *testing.B) {
	var stats statsCollector
	result := batchResult{Accepted: 23, Rejected: 1, Duplicates: 1}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stats.recordBatch(result, 25)
		if i%256 == 0 {
			benchmarkStatsSink = stats.snapshot()
		}
	}
	benchmarkStatsSink = stats.snapshot()
}

func benchmarkEvent() Event {
	return Event{
		ID:              "evt-bench",
		Name:            "bench_event",
		Timestamp:       time.Unix(1700000000, 0).UTC(),
		UserID:          "user-bench",
		AnonymousID:     "anon-bench",
		SessionID:       "session-bench",
		SessionSequence: 42,
		MatchID:         "match-bench",
		Platform:        "server",
		AppVersion:      "0.1.2-bench",
		AppBuild:        "bench-build",
		Props: map[string]any{
			"level":        12,
			"mode":         "arena",
			"score":        98233,
			"currency":     "soft",
			"upgrade_path": "ability",
			"win":          true,
		},
		Context: map[string]any{
			"surface":     "backend",
			"region":      "eu",
			"experiment":  "onboarding-a",
			"build_track": "preview",
		},
	}
}

func drainBenchmarkQueue(q *boundedQueue) {
	for {
		select {
		case <-q.ch:
		default:
			return
		}
	}
}

type benchmarkClock struct {
	t time.Time
}

func (c benchmarkClock) Now() time.Time {
	return c.t
}
