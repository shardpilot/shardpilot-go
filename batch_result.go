package shardpilot

// EventStatus is the per-event ingest outcome the events:batch endpoint
// reports for a single event in a published batch. The accepted/rejected/
// duplicate aggregate counts on BatchResult cannot say which event each
// outcome belongs to, nor do they expose the suppressed/observed outcomes;
// the per-event statuses do.
//
// New statuses may be added by the server over time. Callers that switch on
// EventStatus should keep a default branch: an unrecognised status is carried
// through as its raw string value rather than dropped.
type EventStatus string

const (
	// EventStatusAccepted means the event was registered and stored.
	EventStatusAccepted EventStatus = "accepted"
	// EventStatusObserved means the event was accepted but its event_name is
	// not registered, so it is observed only and not surfaced as a product
	// metric (Code is typically "event_not_registered").
	EventStatusObserved EventStatus = "observed"
	// EventStatusDuplicate means the event_id was seen before and folded away
	// as a duplicate (Code is typically "duplicate_event_id").
	EventStatusDuplicate EventStatus = "duplicate"
	// EventStatusSuppressedNoConsent means the event was dropped because the
	// actor withheld analytics consent. The 202 is not delivery confirmation.
	EventStatusSuppressedNoConsent EventStatus = "suppressed_no_consent"
	// EventStatusSuppressedAdRevenueConsent means an ad-revenue event was
	// dropped because the actor withheld the required ad-revenue consent.
	EventStatusSuppressedAdRevenueConsent EventStatus = "suppressed_ad_revenue_consent"
	// EventStatusRejected means the event was rejected (e.g. failed
	// validation); Code/Message carry the reason.
	EventStatusRejected EventStatus = "rejected"
)

// BatchEventStatus is the ingest outcome for one event in a published batch.
// EventID echoes the event_id the SDK sent (generated when the caller left
// Event.ID empty); Status is the high-level outcome; Code is a stable
// machine-readable reason and Message a human-readable detail, both optional.
type BatchEventStatus struct {
	EventID string
	Status  EventStatus
	Code    string
	Message string
}

// BatchResult is the outcome of a published event batch, surfaced to the
// OnBatchResult callback. Accepted/Rejected/Duplicates are the server's
// top-level aggregate counts; Events carries the per-event status list, which
// is the only place a caller can learn which individual events were rejected,
// suppressed for withheld consent, observed (not registered), or folded as
// duplicates. Events is nil when the response carried no per-event list.
type BatchResult struct {
	Accepted   int
	Rejected   int
	Duplicates int
	Events     []BatchEventStatus
}

// toPublic maps the wire decode to the public BatchResult. It insulates the
// public API from the wire field names and lets the typed EventStatus carry
// through unrecognised server values verbatim.
func (r batchResult) toPublic() BatchResult {
	result := BatchResult{
		Accepted:   r.Accepted,
		Rejected:   r.Rejected,
		Duplicates: r.Duplicates,
	}
	if len(r.Events) > 0 {
		result.Events = make([]BatchEventStatus, len(r.Events))
		for i, event := range r.Events {
			result.Events[i] = BatchEventStatus{
				EventID: event.EventID,
				Status:  EventStatus(event.Status),
				Code:    event.Code,
				Message: event.Message,
			}
		}
	}
	return result
}
