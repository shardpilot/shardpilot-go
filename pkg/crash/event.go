package crash

import (
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	maxStackFrames = 256
	maxBreadcrumbs = 50

	DeviceClassPhone   = "phone"
	DeviceClassTablet  = "tablet"
	DeviceClassDesktop = "desktop"
	DeviceClassConsole = "console"
	DeviceClassTV      = "tv"

	ThreadStateMain       = "main"
	ThreadStateBackground = "background"
)

var (
	ErrInvalidConfig = errors.New("invalid shardpilot crash config")
	ErrInvalidEvent  = errors.New("invalid shardpilot crash event")
)

type Event struct {
	CrashID     string       `json:"crash_id"`
	AppVersion  string       `json:"app_version"`
	BuildID     string       `json:"build_id"`
	OS          OSInfo       `json:"os"`
	DeviceClass string       `json:"device_class"`
	StackFrames []Frame      `json:"stack_frames"`
	Breadcrumbs []Breadcrumb `json:"breadcrumbs"`
	ThreadState string       `json:"thread_state"`
	SessionID   string       `json:"session_id"`
	OccurredAt  time.Time    `json:"occurred_at"`
}

type Frame struct {
	Function string `json:"function"`
	File     string `json:"file"`
	Line     int    `json:"line"`
	Module   string `json:"module"`
}

type Breadcrumb struct {
	Name      string    `json:"name"`
	Timestamp time.Time `json:"timestamp"`
}

type OSInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

func newCrashID() (string, error) {
	return newCrashIDAt(time.Now().UTC())
}

func newCrashIDAt(now time.Time) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}

	millis := uint64(now.UTC().UnixMilli())
	b[0] = byte(millis >> 40)
	b[1] = byte(millis >> 32)
	b[2] = byte(millis >> 24)
	b[3] = byte(millis >> 16)
	b[4] = byte(millis >> 8)
	b[5] = byte(millis)
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4],
		b[4:6],
		b[6:8],
		b[8:10],
		b[10:16],
	), nil
}

func validateEvent(event Event) error {
	if !isUUIDv7(strings.TrimSpace(event.CrashID)) {
		return fmt.Errorf("%w: crash_id must be UUIDv7", ErrInvalidEvent)
	}
	if len(event.StackFrames) > maxStackFrames {
		return fmt.Errorf("%w: stack_frames cannot exceed %d", ErrInvalidEvent, maxStackFrames)
	}
	if len(event.Breadcrumbs) > maxBreadcrumbs {
		return fmt.Errorf("%w: breadcrumbs cannot exceed %d", ErrInvalidEvent, maxBreadcrumbs)
	}
	if !event.OccurredAt.IsZero() && event.OccurredAt.Location() != time.UTC {
		return fmt.Errorf("%w: occurred_at must be UTC", ErrInvalidEvent)
	}
	if event.DeviceClass != "" && !validDeviceClass(event.DeviceClass) {
		return fmt.Errorf("%w: device_class must be phone, tablet, desktop, console, or tv", ErrInvalidEvent)
	}
	if event.ThreadState != "" && !validThreadState(event.ThreadState) {
		return fmt.Errorf("%w: thread_state must be main or background", ErrInvalidEvent)
	}
	for i, frame := range event.StackFrames {
		if frame.Line < 0 {
			return fmt.Errorf("%w: stack_frames[%d].line cannot be negative", ErrInvalidEvent, i)
		}
	}
	for i, breadcrumb := range event.Breadcrumbs {
		if !breadcrumb.Timestamp.IsZero() && breadcrumb.Timestamp.Location() != time.UTC {
			return fmt.Errorf("%w: breadcrumbs[%d].timestamp must be UTC", ErrInvalidEvent, i)
		}
		if strings.TrimSpace(breadcrumb.Name) == "" {
			return fmt.Errorf("%w: breadcrumbs[%d].name is required", ErrInvalidEvent, i)
		}
	}
	return nil
}

func normalizeEventTimes(event Event, now time.Time) Event {
	if event.OccurredAt.IsZero() {
		event.OccurredAt = now.UTC()
	} else {
		event.OccurredAt = event.OccurredAt.UTC()
	}
	for i := range event.Breadcrumbs {
		if event.Breadcrumbs[i].Timestamp.IsZero() {
			event.Breadcrumbs[i].Timestamp = event.OccurredAt
		} else {
			event.Breadcrumbs[i].Timestamp = event.Breadcrumbs[i].Timestamp.UTC()
		}
	}
	return event
}

func capBreadcrumbs(in []Breadcrumb) []Breadcrumb {
	if len(in) <= maxBreadcrumbs {
		return in
	}
	return in[len(in)-maxBreadcrumbs:]
}

func cloneEvent(event Event) Event {
	event.StackFrames = append([]Frame(nil), event.StackFrames...)
	event.Breadcrumbs = append([]Breadcrumb(nil), event.Breadcrumbs...)
	return event
}

func validDeviceClass(value string) bool {
	switch value {
	case DeviceClassPhone, DeviceClassTablet, DeviceClassDesktop, DeviceClassConsole, DeviceClassTV:
		return true
	default:
		return false
	}
}

func validThreadState(value string) bool {
	switch value {
	case ThreadStateMain, ThreadStateBackground:
		return true
	default:
		return false
	}
}

func isUUIDv7(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i := 0; i < len(value); i++ {
		switch i {
		case 8, 13, 18, 23:
			if value[i] != '-' {
				return false
			}
		default:
			if !isHex(value[i]) {
				return false
			}
		}
	}
	if value[14] != '7' {
		return false
	}
	switch value[19] {
	case '8', '9', 'a', 'A', 'b', 'B':
		return true
	default:
		return false
	}
}

func isHex(ch byte) bool {
	return ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f' || ch >= 'A' && ch <= 'F'
}
