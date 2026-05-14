package shardpilot

import "time"

type Event struct {
	ID              string
	Name            string
	Timestamp       time.Time
	UserID          string
	AnonymousID     string
	SessionID       string
	SessionSequence int64
	MatchID         string
	Platform        string
	AppVersion      string
	AppBuild        string
	Props           map[string]any
	Context         map[string]any
}
