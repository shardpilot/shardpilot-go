package crash

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
)

const (
	maxStackFrames = 256
	maxThreads     = 64
	maxModules     = 256
	maxBreadcrumbs = 50

	DeviceClassPhone   = "phone"
	DeviceClassTablet  = "tablet"
	DeviceClassDesktop = "desktop"
	DeviceClassConsole = "console"
	DeviceClassTV      = "tv"
)

var (
	ErrInvalidConfig = errors.New("invalid shardpilot crash config")
	ErrInvalidEvent  = errors.New("invalid shardpilot crash event")
)

type Event struct {
	CrashID    string    `json:"crash_id"`
	OccurredAt time.Time `json:"occurred_at"`
	App        AppInfo   `json:"app"`
	// Source is the component slug within the app (ADR-0223): which repo/service this
	// crash came from (e.g. main-server, game-server). Usually set once via
	// ClientOptions.Source and stamped on every event; a per-event value wins.
	Source                string            `json:"source,omitempty"`
	Platform              string            `json:"platform"`
	OS                    OSInfo            `json:"os"`
	Device                map[string]string `json:"device,omitempty"`
	Context               map[string]string `json:"context,omitempty"`
	Exception             ExceptionInfo     `json:"exception"`
	Modules               []Module          `json:"modules"`
	Threads               []Thread          `json:"threads,omitempty"`
	RawText               string            `json:"raw_text,omitempty"`
	Breadcrumbs           []Breadcrumb      `json:"breadcrumbs,omitempty"`
	FingerprintComponents []string          `json:"fingerprint_components,omitempty"`
	Metadata              map[string]string `json:"metadata,omitempty"`
}

type AppInfo struct {
	ID      string `json:"id"`
	Version string `json:"version,omitempty"`
	BuildID string `json:"build_id,omitempty"`
}

type ExceptionInfo struct {
	Type            string `json:"type"`
	Reason          string `json:"reason,omitempty"`
	CrashedThreadID string `json:"crashed_thread_id,omitempty"`
}

type Module struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name"`
	Platform    string `json:"platform,omitempty"`
	DebugID     string `json:"debug_id"`
	BuildID     string `json:"build_id,omitempty"`
	LoadAddress string `json:"load_address,omitempty"`
	BaseAddress string `json:"base_address,omitempty"`
	EndAddress  string `json:"end_address,omitempty"`
	Size        string `json:"size,omitempty"`
}

type Thread struct {
	ID      string  `json:"id"`
	Name    string  `json:"name,omitempty"`
	Crashed bool    `json:"crashed,omitempty"`
	Frames  []Frame `json:"frames"`
}

type Frame struct {
	Index              int    `json:"index,omitempty"`
	ModuleID           string `json:"module_id,omitempty"`
	Module             string `json:"module,omitempty"`
	ModuleName         string `json:"module_name,omitempty"`
	InstructionAddress string `json:"instruction_addr"`
	Address            string `json:"address,omitempty"`
	RelativeAddress    string `json:"relative_addr,omitempty"`
	Function           string `json:"function,omitempty"`
	File               string `json:"file,omitempty"`
	Line               int    `json:"line,omitempty"`
}

type Breadcrumb struct {
	Name      string    `json:"name"`
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type,omitempty"`
	Category  string    `json:"category,omitempty"`
	Level     string    `json:"level,omitempty"`
	Message   string    `json:"message,omitempty"`
}

type OSInfo struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

func newCrashID() (string, error) {
	return newCrashIDAt(time.Now().UTC())
}

func newCrashIDAt(now time.Time) (string, error) {
	return uuidv7.NewAt(now)
}

func validateEvent(event Event) error {
	if !isUUIDv7(strings.TrimSpace(event.CrashID)) {
		return fmt.Errorf("%w: crash_id must be UUIDv7", ErrInvalidEvent)
	}
	if event.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred_at is required", ErrInvalidEvent)
	}
	if event.OccurredAt.Location() != time.UTC {
		return fmt.Errorf("%w: occurred_at must be UTC", ErrInvalidEvent)
	}
	if strings.TrimSpace(event.App.ID) == "" {
		return fmt.Errorf("%w: app.id is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(event.Platform) == "" {
		return fmt.Errorf("%w: platform is required", ErrInvalidEvent)
	}
	if !validTokenish(event.Platform) {
		return fmt.Errorf("%w: platform contains unsupported characters", ErrInvalidEvent)
	}
	if strings.TrimSpace(event.Exception.Type) == "" {
		return fmt.Errorf("%w: exception.type is required", ErrInvalidEvent)
	}
	// 0 modules is allowed for a PRE-SYMBOLICATED crash (ADR-0223): a Go/server crash
	// captured from the runtime carries function/file/line frames and no native modules.
	if len(event.Modules) > maxModules {
		return fmt.Errorf("%w: modules cannot exceed %d", ErrInvalidEvent, maxModules)
	}
	if len(event.Threads) > maxThreads {
		return fmt.Errorf("%w: threads cannot exceed %d", ErrInvalidEvent, maxThreads)
	}
	if len(event.Breadcrumbs) > maxBreadcrumbs {
		return fmt.Errorf("%w: breadcrumbs cannot exceed %d", ErrInvalidEvent, maxBreadcrumbs)
	}
	for i, module := range event.Modules {
		if strings.TrimSpace(module.Name) == "" {
			return fmt.Errorf("%w: modules[%d].name is required", ErrInvalidEvent, i)
		}
		if strings.TrimSpace(module.DebugID) == "" && strings.TrimSpace(module.BuildID) == "" {
			return fmt.Errorf("%w: modules[%d].debug_id is required", ErrInvalidEvent, i)
		}
		if strings.TrimSpace(firstNonEmptyString(module.LoadAddress, module.BaseAddress)) == "" {
			return fmt.Errorf("%w: modules[%d].load_address is required", ErrInvalidEvent, i)
		}
	}
	frameCount := 0
	for i, thread := range event.Threads {
		if strings.TrimSpace(thread.ID) == "" {
			return fmt.Errorf("%w: threads[%d].id is required", ErrInvalidEvent, i)
		}
		for j, frame := range thread.Frames {
			frameCount++
			if frame.Line < 0 {
				return fmt.Errorf("%w: threads[%d].frames[%d].line cannot be negative", ErrInvalidEvent, i, j)
			}
			hasFunction := strings.TrimSpace(frame.Function) != ""
			hasAddress := strings.TrimSpace(firstNonEmptyString(frame.InstructionAddress, frame.Address)) != ""
			// Every frame must be identifiable — a PRE-SYMBOLICATED frame by its function
			// (ADR-0223, no modules/address needed) or a native frame by its address.
			if !hasFunction && !hasAddress {
				return fmt.Errorf("%w: threads[%d].frames[%d] requires instruction_addr or a function", ErrInvalidEvent, i, j)
			}
			// An instruction address is only resolvable against a module map: it requires at
			// least one module, and a module_id to disambiguate among multiple. (A frame with
			// a function and no address is fully resolved and needs neither.)
			if hasAddress {
				if len(event.Modules) == 0 {
					return fmt.Errorf("%w: threads[%d].frames[%d] has an instruction address but no module to resolve it", ErrInvalidEvent, i, j)
				}
				if len(event.Modules) > 1 && strings.TrimSpace(firstNonEmptyString(frame.ModuleID, frame.Module, frame.ModuleName)) == "" {
					return fmt.Errorf("%w: threads[%d].frames[%d].module_id is required", ErrInvalidEvent, i, j)
				}
			}
		}
	}
	if frameCount > maxStackFrames {
		return fmt.Errorf("%w: stack frames cannot exceed %d", ErrInvalidEvent, maxStackFrames)
	}
	if frameCount == 0 && strings.TrimSpace(event.RawText) == "" {
		return fmt.Errorf("%w: at least one thread frame or raw_text is required", ErrInvalidEvent)
	}
	if deviceClass := event.Device["class"]; deviceClass != "" && !validDeviceClass(deviceClass) {
		return fmt.Errorf("%w: device.class must be phone, tablet, desktop, console, or tv", ErrInvalidEvent)
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

func normalizeEventShape(event Event) Event {
	for i := range event.Threads {
		if strings.TrimSpace(event.Threads[i].ID) == "" {
			event.Threads[i].ID = fmt.Sprintf("%d", i)
		}
		for j := range event.Threads[i].Frames {
			if event.Threads[i].Frames[j].Index == 0 {
				event.Threads[i].Frames[j].Index = j
			}
		}
	}
	if strings.TrimSpace(event.Exception.CrashedThreadID) == "" {
		for _, thread := range event.Threads {
			if thread.Crashed {
				event.Exception.CrashedThreadID = thread.ID
				return event
			}
		}
		if len(event.Threads) > 0 {
			event.Threads[0].Crashed = true
			event.Exception.CrashedThreadID = event.Threads[0].ID
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
	event.Modules = append([]Module(nil), event.Modules...)
	event.Threads = cloneThreads(event.Threads)
	event.Breadcrumbs = append([]Breadcrumb(nil), event.Breadcrumbs...)
	event.FingerprintComponents = append([]string(nil), event.FingerprintComponents...)
	event.Device = cloneStringMap(event.Device)
	event.Context = cloneStringMap(event.Context)
	event.Metadata = cloneStringMap(event.Metadata)
	return event
}

func cloneThreads(threads []Thread) []Thread {
	out := append([]Thread(nil), threads...)
	for i := range out {
		out[i].Frames = append([]Frame(nil), out[i].Frames...)
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func validDeviceClass(value string) bool {
	switch value {
	case DeviceClassPhone, DeviceClassTablet, DeviceClassDesktop, DeviceClassConsole, DeviceClassTV:
		return true
	default:
		return false
	}
}

func validTokenish(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return false
	}
	return true
}

func isUUIDv7(value string) bool {
	return uuidv7.IsValid(value)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
