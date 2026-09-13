package shardpilot

import (
	"fmt"
	"log"
	"strings"
	"sync"
)

type rejectionHistory struct {
	mu           sync.Mutex
	entries      []BatchEventStatus
	next         int
	warnedCodes  map[string]bool
	warningCount int
}

// Rejections returns oldest-first copies of this client's retained per-event
// rejections. It is safe to call concurrently, including from diagnostic hooks.
// The history survives Close for inspection but is not persisted or retried.
func (c *Client) Rejections() []BatchEventStatus {
	h := &c.rejections
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]BatchEventStatus, len(h.entries))
	n := copy(out, h.entries[h.next:])
	copy(out[n:], h.entries[:h.next])
	return out
}

// All publish paths arrive after spool settlement. Retain the entire batch
// before invoking user diagnostics, which may mutate results, panic or reenter.
func (c *Client) recordRejections(result BatchResult) {
	h := &c.rejections
	capacity := c.cfg.RejectionCapacity
	if capacity <= 0 {
		capacity = 64
	}
	defaultChannel := c.cfg.OnBatchResult == nil && c.cfg.Logger == nil
	var warnings []BatchEventStatus
	var last BatchEventStatus
	haveRejection := false
	h.mu.Lock()
	for _, entry := range result.Events {
		if entry.Status != EventStatusRejected {
			continue
		}
		if len(h.entries) < capacity {
			h.entries = append(h.entries, entry)
		} else {
			h.entries[h.next] = entry
			h.next = (h.next + 1) % capacity
		}
		last, haveRejection = entry, true
		if c.cfg.OnBatchResult != nil {
			continue
		}
		emit := !defaultChannel || h.warningCount < 10
		if defaultChannel && !h.warnedCodes[entry.Code] && len(h.warnedCodes) < 64 {
			if h.warnedCodes == nil {
				h.warnedCodes = make(map[string]bool)
			}
			h.warnedCodes[entry.Code] = true
			emit = true
		}
		if emit {
			warnings = append(warnings, entry)
			if defaultChannel {
				h.warningCount++
			}
		}
	}
	if haveRejection {
		c.stats.setLastError(c.rejectionDiagnostic(last))
	}
	h.mu.Unlock()
	for _, entry := range warnings {
		c.warnRejection(entry)
	}
}

func (c *Client) rejectionDiagnostic(entry BatchEventStatus) string {
	clean := func(value string) string {
		for _, secret := range []string{c.cfg.Token, c.cfg.APIKey} {
			if secret != "" {
				value = strings.ReplaceAll(value, secret, "[REDACTED]")
			}
		}
		return value
	}
	return fmt.Sprintf("shardpilot event rejected id=%q code=%q message=%q", clean(entry.EventID), clean(entry.Code), clean(entry.Message))
}

func (c *Client) warnRejection(entry BatchEventStatus) {
	// Logging must not turn a settled 202 into a failed or abandoned publish.
	defer func() { _ = recover() }()
	message := c.rejectionDiagnostic(entry)
	if c.cfg.Logger != nil {
		c.cfg.Logger.Printf("WARNING: %s", message)
	} else {
		log.Printf("WARNING: %s", message)
	}
}
