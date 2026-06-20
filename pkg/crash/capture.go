package crash

import (
	"context"
	"fmt"
	"runtime"
	"strings"
)

// crashPackagePrefix identifies this SDK's own frames so they can be trimmed from a
// captured stack (the panic origin, not the SDK plumbing, should be the top frame).
const crashPackagePrefix = "github.com/shardpilot/shardpilot-go/pkg/crash."

// maxCaptureFrames bounds the captured stack depth.
const maxCaptureFrames = 64

// Recover captures a panic as a FATAL crash and then RE-PANICS, preserving the
// program's normal crash behavior (the process still dies / the panic still
// propagates). Defer it at a goroutine or request-handler boundary:
//
//	func handle() {
//	    defer client.Recover(ctx)
//	    ...
//	}
//
// The report is sent synchronously (best-effort) before the re-panic, so it is not
// lost when the process exits. The caller's ctx supplies request-scoped values but its
// cancellation/deadline is detached for the send, so a panic during graceful shutdown
// or after a client disconnect (when ctx is already done) is still reported. A nil
// client is a safe no-op.
func (c *Client) Recover(ctx context.Context) {
	if r := recover(); r != nil {
		c.reportPanic(ctx, r)
		panic(r)
	}
}

// CapturePanic reports an already-recovered panic value as a FATAL crash WITHOUT
// re-panicking — for callers that intentionally recover() themselves and want to
// keep running. A nil recovered value (no panic) is a no-op.
func (c *Client) CapturePanic(ctx context.Context, recovered any) {
	if recovered == nil {
		return
	}
	c.reportPanic(ctx, recovered)
}

func (c *Client) reportPanic(ctx context.Context, recovered any) {
	if c == nil {
		return
	}
	// Capture must NEVER mask the original crash: if reporting itself panics (it
	// shouldn't — EmitFatal returns errors), swallow it so the caller's Recover still
	// re-panics with the original value rather than this secondary one. safeLogf is used
	// because even the log call must not let a misbehaving Logger panic escape here.
	defer func() {
		if rec := recover(); rec != nil {
			c.safeLogf("crash capture: panicked while reporting (suppressed): %v", rec)
		}
	}()
	// A crash report must survive a cancelled/expired caller context: a panic during
	// graceful shutdown or after a client disconnect carries an already-done ctx, and the
	// context error is non-retryable, so the report would otherwise be silently dropped.
	// Detach from the caller's cancellation/deadline (keeping its values) and bound the
	// send with a fresh timeout.
	if ctx == nil {
		ctx = context.Background()
	}
	emitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultHTTPTimeout)
	defer cancel()
	// EmitFatal is synchronous and bypasses sampling (a crash is always sent). Errors
	// are logged, never returned: capture must not block or alter the re-panic.
	if err := c.EmitFatal(emitCtx, c.panicEvent(recovered)); err != nil {
		c.logf("crash capture: emit fatal panic: %v", err)
	}
}

// panicEvent builds the crash Event for a recovered panic value. Frames are
// PRE-SYMBOLICATED from the Go runtime (function/file/line, no native modules or
// addresses — accepted by the producer per ADR-0223). Exposed unexported for tests.
func (c *Client) panicEvent(recovered any) Event {
	return Event{
		Platform: goPlatform(),
		OS:       OSInfo{Name: runtime.GOOS},
		Exception: ExceptionInfo{
			Type:            panicType(recovered),
			Reason:          fmt.Sprint(recovered),
			CrashedThreadID: "main",
		},
		Threads: []Thread{{
			ID:      "main",
			Crashed: true,
			Frames:  captureGoFrames(),
		}},
	}
}

// captureGoFrames walks the CURRENT goroutine's stack into pre-symbolicated frames.
// Called from a deferred recover the panicking stack is still live beneath it, so the
// panic origin is present; everything above the origin — the SDK's own frames, any
// caller-owned recover wrapper, and the runtime panic machinery — is trimmed so frame[0]
// is the application function that panicked.
func captureGoFrames() []Frame {
	var pcs [maxCaptureFrames]uintptr
	// skip runtime.Callers + captureGoFrames themselves.
	n := runtime.Callers(2, pcs[:])
	if n == 0 {
		return nil
	}
	it := runtime.CallersFrames(pcs[:n])
	raw := make([]runtime.Frame, 0, n)
	for {
		f, more := it.Next()
		raw = append(raw, f)
		if !more {
			break
		}
	}

	start := originFrameStart(raw)

	frames := make([]Frame, 0, len(raw)-start)
	for i := start; i < len(raw); i++ {
		f := raw[i]
		fn := shortFuncName(f.Function)
		if fn == "" {
			// The runtime could not symbolize this PC (a cgo boundary, a stripped or
			// -trimpath edge, an odd inlined wrapper). A pre-symbolicated Go frame carries
			// no address, so an empty-function frame is useless and would fail validation
			// and drop the WHOLE crash. Skip it; the application origin still has a symbol.
			continue
		}
		frames = append(frames, Frame{
			Index:    len(frames),
			Function: fn,
			File:     f.File,
			Line:     f.Line,
		})
	}
	return frames
}

// originFrameStart returns the index of the panicking application frame within a captured
// stack. Every Go panic dispatches its deferred funcs through runtime.gopanic, so the
// origin is the first NON-runtime frame beneath gopanic. Anchoring there trims, in one
// step and without enumerating every helper, three things at once: the SDK's own capture
// frames above gopanic, the caller's recover wrapper above gopanic (the CapturePanic
// case, where user code — not the SDK — invoked us), and the runtime panic helpers below
// gopanic (sigpanic / panicmem / panicdivide / panicBounds* / panicshift / goPanic* / …).
// If no panic is in flight (e.g. CapturePanic called after the panic already settled),
// it falls back to trimming only the SDK's leading frames.
func originFrameStart(raw []runtime.Frame) int {
	for i := range raw {
		if raw[i].Function != "runtime.gopanic" {
			continue
		}
		origin := i + 1
		for origin < len(raw) && strings.HasPrefix(raw[origin].Function, "runtime.") {
			origin++
		}
		if origin < len(raw) {
			return origin
		}
		// Pathological: nothing but runtime frames beneath gopanic. Keep the first frame
		// after gopanic rather than reporting an empty stack.
		return i + 1
	}
	// No panic dispatch on the stack: trim only our own leading frames.
	start := 0
	for start < len(raw) && strings.HasPrefix(raw[start].Function, crashPackagePrefix) {
		start++
	}
	if start >= len(raw) {
		return 0
	}
	return start
}

// shortFuncName trims the import-path prefix from a Go runtime function name, keeping
// the package-qualified symbol (github.com/org/repo/pkg.(*T).M → pkg.(*T).M). The full
// import path resembles a URL path and is redacted by the ingest PII scrubber; the short
// form survives intact and is the conventional stack-trace rendering.
func shortFuncName(fn string) string {
	if i := strings.LastIndexByte(fn, '/'); i >= 0 {
		return fn[i+1:]
	}
	return fn
}

// panicType derives the crash exception type from a recovered value: the concrete
// Go type for typed panics/errors (e.g. "runtime.Error", "*errors.errorString"),
// "panic" for an untyped/nil value.
func panicType(recovered any) string {
	if recovered == nil {
		return "panic"
	}
	return fmt.Sprintf("%T", recovered)
}

// goPlatform maps the Go runtime GOOS to the crash ingest platform enum.
func goPlatform() string {
	switch runtime.GOOS {
	case "darwin":
		return "macos"
	default:
		return runtime.GOOS
	}
}
