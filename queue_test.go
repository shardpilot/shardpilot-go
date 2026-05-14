package shardpilot

import "testing"

func TestBoundedQueueDropsWhenFull(t *testing.T) {
	queue := newBoundedQueue(1)
	if !queue.enqueue(Event{Name: "first"}) {
		t.Fatal("expected first enqueue to succeed")
	}
	if queue.enqueue(Event{Name: "second"}) {
		t.Fatal("expected second enqueue to fail when queue is full")
	}
}
