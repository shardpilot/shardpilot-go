package shardpilot

// Supports reports whether this SDK implements a capability. It works before
// a client exists. Keys are exact and case-sensitive; unknown keys return false.
func Supports(key string) bool {
	switch key {
	case "consent_receipt_outbox", "consent_state_denied_forced_minor",
		"schema_revision_declaration", "experiments_assignment", "experiments_age_band":
		return true
	default:
		return false
	}
}
