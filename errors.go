package shardpilot

import "errors"

var (
	ErrClosed        = errors.New("shardpilot client is closed")
	ErrInvalidConfig = errors.New("invalid shardpilot config")
	ErrInvalidEvent  = errors.New("invalid shardpilot event")
	ErrQueueFull     = errors.New("shardpilot queue is full")
)
