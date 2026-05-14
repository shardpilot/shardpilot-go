package shardpilot

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
