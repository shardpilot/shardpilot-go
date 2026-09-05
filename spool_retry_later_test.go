package shardpilot

import (
	"context"
	"net/http"
	"testing"
)

// The ingest batch answers `retry_later` for an event whose event_id is held
// by a delivery that has not reached the broker: nothing yet proves the event
// was stored, and the sender must send it again. This file asks what this
// client does with that verdict.
//
// The failure it exists to prevent is silent: the entry leaves the spool as
// if delivered, and no dead-letter fires either, so nothing anywhere reports
// the loss. The event was on no broker; after the ack it is nowhere at all.

// TestSpoolKeepsAnEventAnsweredRetryLater is the scene. The event is spooled
// (a retriable failure puts it there), the in-process retry's 202 answers
// retry_later, and the spooled copy must survive: `retry_later` is the one
// per-event verdict that is not final.
func TestSpoolKeepsAnEventAnsweredRetryLater(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	client.SetConsent(true)

	if err := client.Enqueue(Event{ID: "evt-retry-later-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 1 {
		t.Fatalf("precondition: expected the failed batch spooled, got %d", got)
	}

	// The retry's 202 says the event_id is held by a delivery still in
	// flight. Nothing was stored: the reservation it collided with may
	// belong to a producer that died before publishing.
	state.setAcceptedBody(`{"accepted":0,"rejected":0,"duplicates":0,"suppressed":0,"retry_later":1,"events":[` +
		`{"event_id":"evt-retry-later-1","status":"retry_later","code":"reservation_in_flight"}]}`)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}

	if got := len(readSpoolRecordFile(t, dir).Events); got != 1 {
		t.Fatalf("an event answered retry_later left the spool (%d record(s) remain) — "+
			"the server stored nothing, so the only copy of this event was the spooled one", got)
	}
	if dead := recorder.count(); dead != 0 {
		t.Fatalf("retry_later is not a drop: nothing may be dead-lettered, got %d", dead)
	}
	_ = client.Close(context.Background())
}

// TestSpoolStillSettlesADuplicateVerdict is the control: the instrument must
// report the other outcome too. `duplicate` means the event is STORED, so its
// spooled copy is settled and removed — a repair that kept everything would
// pass the scene above and grow the spool without bound.
func TestSpoolStillSettlesADuplicateVerdict(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	client.SetConsent(true)

	if err := client.Enqueue(Event{ID: "evt-duplicate-1", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 1 {
		t.Fatalf("precondition: expected the failed batch spooled, got %d", got)
	}

	state.setAcceptedBody(`{"accepted":0,"rejected":0,"duplicates":1,"suppressed":0,"retry_later":0,"events":[` +
		`{"event_id":"evt-duplicate-1","status":"duplicate","code":"duplicate_event_id"}]}`)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}

	if got := len(readSpoolRecordFile(t, dir).Events); got != 0 {
		t.Fatalf("a stored event's spooled copy must settle, got %d record(s)", got)
	}
	if dead := recorder.count(); dead != 0 {
		t.Fatalf("a duplicate is delivered, not dropped: nothing may be dead-lettered, got %d", dead)
	}
	_ = client.Close(context.Background())
}

// TestSpoolResendsAnEventAnsweredRetryLaterOnTheNextPass is the other half of
// the acceptance: the client keeps the event AND sends it again. The first
// answer is retry_later, the next flush finds the event back on the resend
// queue and re-publishes it, and only the delivery settles it.
func TestSpoolResendsAnEventAnsweredRetryLaterOnTheNextPass(t *testing.T) {
	state, server := newSpoolTestServer(t)
	defer server.Close()
	state.setOutcome(http.StatusInternalServerError, "internal_error", "")

	dir := t.TempDir()
	recorder := &spoolDeadLetterRecorder{}
	client := newSpoolTestClient(t, server.URL, dir, recorder, nil)
	client.SetConsent(true)

	if err := client.Enqueue(Event{ID: "evt-retry-later-2", Name: "e1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := client.Flush(context.Background()); err == nil {
		t.Fatalf("expected the retriable failure surfaced")
	}

	state.setAcceptedBody(`{"accepted":0,"rejected":0,"duplicates":0,"suppressed":0,"retry_later":1,"events":[` +
		`{"event_id":"evt-retry-later-2","status":"retry_later","code":"reservation_in_flight"}]}`)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	deliveries := state.batchCount()

	// The reservation it collided with has since committed or expired, and
	// the event is stored on this attempt.
	state.setAcceptedBody(`{"accepted":1,"rejected":0,"duplicates":0,"suppressed":0,"retry_later":0,"events":[` +
		`{"event_id":"evt-retry-later-2","status":"accepted"}]}`)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("resend Flush: %v", err)
	}

	if got := state.batchCount(); got <= deliveries {
		t.Fatalf("the deferred event must be published again: %d request(s) before, %d after", deliveries, got)
	}
	arrivals := state.allArrivals()
	last := arrivals[len(arrivals)-1]
	if len(last) != 1 || last[0] != "evt-retry-later-2" {
		t.Fatalf("the resend must carry exactly the deferred event, got %v", last)
	}
	if got := len(readSpoolRecordFile(t, dir).Events); got != 0 {
		t.Fatalf("the delivered event must settle out of the record, got %d", got)
	}
	if dead := recorder.count(); dead != 0 {
		t.Fatalf("nothing may be dead-lettered on this path, got %d", dead)
	}
	_ = client.Close(context.Background())
}
