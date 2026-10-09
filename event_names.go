package shardpilot

import "strings"

// Event names use the same ASCII trim at admission and envelope construction.
// A later, broader trim must not turn an admitted name into a reserved one.
func normalizeEventName(name string) string {
	return strings.Trim(name, " \t\n\v\f\r")
}

// validateHostEventName is only for the public, untyped event entry points.
// Sealed experiment facts enter through their own internal producer.
func (c *Client) validateHostEventName(name string) error {
	name = normalizeEventName(name)
	var err error
	if name == "" {
		err = ErrEventNameRequired
	} else {
		switch strings.TrimPrefix(name, "app.") {
		case "experiment_exposure", "experiment_outcome", "governed_action_result",
			"app_runtime_ping", "monitor_evaluation_recorded", "llm_usage",
			"support_assistant_feedback", "ad_impression_revenue", "session_started", "session_ended":
			err = ErrReservedEventName
		}
	}
	if err != nil {
		c.stats.dropped.Add(1)
		c.stats.setLastError(err.Error())
	}
	return err
}
