package shardpilot

// boundedQueue is an in-memory, bounded event buffer. It is memory-only by
// default: events still buffered when the process exits are lost — unless
// the opt-in bounded disk spool (Config.SpoolDir; see spool.go) is
// configured, which adds at-least-once delivery across restarts with a
// retry-age cap and a dead-letter callback for events that cannot be
// delivered before they age out.
type boundedQueue struct {
	ch chan Event
}

func newBoundedQueue(size int) *boundedQueue {
	return &boundedQueue{ch: make(chan Event, size)}
}

func (q *boundedQueue) enqueue(event Event) bool {
	select {
	case q.ch <- event:
		return true
	default:
		return false
	}
}

func (q *boundedQueue) drainAll() int {
	count := 0
	for {
		select {
		case <-q.ch:
			count++
		default:
			return count
		}
	}
}

func (q *boundedQueue) drainInto(events []Event, limit int) []Event {
	for len(events) < limit {
		select {
		case event := <-q.ch:
			events = append(events, event)
		default:
			return events
		}
	}
	return events
}
