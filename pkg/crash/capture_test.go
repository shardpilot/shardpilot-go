// This test lives in the EXTERNAL crash_test package on purpose: the frame-trimming
// in captureGoFrames drops every github.com/shardpilot/shardpilot-go/pkg/crash.* frame,
// so a panic origin must live OUTSIDE that package for the "top frame is the app origin"
// assertion to be meaningful. From here the panicking helpers are crash_test.* frames,
// exactly like real application code.
package crash_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shardpilot/shardpilot-go/pkg/crash"
)

type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) { l.t.Logf("[crash-sdk] "+format, args...) }

// boomForCaptureTest is the simulated application panic origin. It must be a named
// top-level function so the assertion on the captured top frame is stable.
func boomForCaptureTest() {
	panic("captured-boom")
}

// captureServer spins up an ingest stub that decodes the single posted crash event.
func captureServer(t *testing.T, received *crash.Event) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(received); err != nil {
			t.Errorf("decode event: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
}

func newCaptureClient(t *testing.T, url, source string) *crash.Client {
	t.Helper()
	client, err := crash.NewClient(crash.ClientOptions{
		IngestURL: url,
		APIKey:    "workspace-api-key-test",
		App:       crash.AppInfo{ID: "fortress-fury"},
		Source:    source,
		Logger:    testLogger{t},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func TestRecoverCapturesPanicOriginAndRepanics(t *testing.T) {
	var received crash.Event
	server := captureServer(t, &received)
	defer server.Close()
	client := newCaptureClient(t, server.URL, "main-server")

	// Recover runs synchronously (EmitFatal blocks on the POST) BEFORE it re-panics,
	// so `received` is populated by the time the outer recover catches the re-panic.
	rePanicked := false
	var rePanicValue any
	func() {
		defer func() {
			if r := recover(); r != nil {
				rePanicked = true
				rePanicValue = r
			}
		}()
		defer client.Recover(context.Background())
		boomForCaptureTest()
	}()

	if !rePanicked {
		t.Fatalf("Recover must RE-PANIC to preserve crash semantics")
	}
	if rePanicValue != "captured-boom" {
		t.Fatalf("re-panic must carry the original value, got %v", rePanicValue)
	}

	if received.Exception.Reason != "captured-boom" {
		t.Fatalf("exception reason = %q, want captured-boom", received.Exception.Reason)
	}
	if received.Exception.Type != "string" {
		t.Fatalf("exception type = %q, want string (the concrete panic type)", received.Exception.Type)
	}
	if received.Source != "main-server" {
		t.Fatalf("source = %q, want main-server (stamped from ClientOptions)", received.Source)
	}
	if received.App.ID != "fortress-fury" {
		t.Fatalf("app.id = %q, want fortress-fury (stamped from ClientOptions)", received.App.ID)
	}
	if len(received.Modules) != 0 {
		t.Fatalf("a pre-symbolicated Go crash carries 0 native modules, got %d", len(received.Modules))
	}
	if len(received.Threads) != 1 || !received.Threads[0].Crashed {
		t.Fatalf("expected a single crashed thread, got %#v", received.Threads)
	}
	if received.Exception.CrashedThreadID != received.Threads[0].ID {
		t.Fatalf("crashed_thread_id %q must point at the crashed thread %q", received.Exception.CrashedThreadID, received.Threads[0].ID)
	}

	frames := received.Threads[0].Frames
	if len(frames) == 0 {
		t.Fatalf("captured zero frames")
	}
	top := frames[0]
	// The top frame must be the application origin, not SDK plumbing or runtime panic glue.
	if !strings.Contains(top.Function, "boomForCaptureTest") {
		t.Fatalf("top frame = %q, want the panic origin (boomForCaptureTest); trimming failed", top.Function)
	}
	for _, f := range frames {
		// Symbols are shortened to package.Symbol, so the SDK's own frames would read
		// "crash.Recover" / "crash.(*Client).reportPanic" (prefix "crash."). "crash_test."
		// is application code and must NOT match (the char after "crash" is "_", not ".").
		if strings.HasPrefix(f.Function, "crash.") {
			t.Fatalf("SDK frame leaked into the captured stack: %q", f.Function)
		}
		if f.Function == "runtime.gopanic" {
			t.Fatalf("runtime panic glue leaked into the captured stack: %q", f.Function)
		}
		// Pre-symbolicated frames carry function/file/line, never a native address.
		if f.InstructionAddress != "" || f.Address != "" {
			t.Fatalf("pre-symbolicated frame must have no address, got %#v", f)
		}
	}
	if top.File == "" || top.Line == 0 {
		t.Fatalf("top frame should be symbolicated with file:line, got %#v", top)
	}
}

func TestCapturePanicReportsWithoutRepanicking(t *testing.T) {
	var received crash.Event
	server := captureServer(t, &received)
	defer server.Close()
	client := newCaptureClient(t, server.URL, "game-server")

	// A caller that recovers itself and keeps running.
	resumed := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				client.CapturePanic(context.Background(), r)
				resumed = true // CapturePanic must NOT re-panic.
			}
		}()
		boomForCaptureTest()
	}()

	if !resumed {
		t.Fatalf("CapturePanic must not re-panic")
	}
	if received.Exception.Reason != "captured-boom" {
		t.Fatalf("exception reason = %q, want captured-boom", received.Exception.Reason)
	}
	if received.Source != "game-server" {
		t.Fatalf("source = %q, want game-server", received.Source)
	}
	if len(received.Threads) != 1 || len(received.Threads[0].Frames) == 0 {
		t.Fatalf("expected captured frames, got %#v", received.Threads)
	}
	// CapturePanic is called from the caller's OWN recover, so the panic origin is still
	// beneath it on the stack and must appear among the frames.
	foundOrigin := false
	for _, f := range received.Threads[0].Frames {
		if strings.Contains(f.Function, "boomForCaptureTest") {
			foundOrigin = true
		}
	}
	if !foundOrigin {
		t.Fatalf("panic origin missing from captured frames: %#v", received.Threads[0].Frames)
	}
}

func TestRecoverNoPanicIsNoop(t *testing.T) {
	var posts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	client := newCaptureClient(t, server.URL, "main-server")

	func() {
		defer client.Recover(context.Background()) // no panic in scope
	}()

	if posts != 0 {
		t.Fatalf("Recover with no panic must not emit, got %d posts", posts)
	}
}

func TestNilClientCaptureIsSafe(t *testing.T) {
	var client *crash.Client
	// Must not panic on a nil client (defensive: a disabled/unconfigured SDK).
	client.CapturePanic(context.Background(), "boom")

	rePanicked := false
	func() {
		defer func() {
			if recover() != nil {
				rePanicked = true
			}
		}()
		defer client.Recover(context.Background())
		boomForCaptureTest()
	}()
	if !rePanicked {
		t.Fatalf("nil-client Recover must still re-panic (it only skips reporting)")
	}
}

//go:noinline
func nineForCapture() int { return 9 }

//go:noinline
func zeroForCapture() int { return 0 }

//go:noinline
func panicIndexBoom() { s := []int{1}; _ = s[nineForCapture()] }

//go:noinline
func panicSliceBoom() { s := []int{1}; _ = s[2:nineForCapture()] }

//go:noinline
func panicNilBoom() { var p *int; _ = *p }

//go:noinline
func panicDivBoom() { _ = 1 / zeroForCapture() }

// The trim list must strip the runtime panic machinery for EVERY panic kind so the
// application origin lands at frame[0] — index/slice route through runtime.panicBounds*
// (Go 1.24+), which a stale trim list would leave on top, corrupting crash grouping.
func TestRecoverTopFrameIsOriginAcrossPanicKinds(t *testing.T) {
	cases := []struct {
		name, origin string
		boom         func()
	}{
		{"index", "panicIndexBoom", panicIndexBoom},
		{"slice", "panicSliceBoom", panicSliceBoom},
		{"nilderef", "panicNilBoom", panicNilBoom},
		{"divide", "panicDivBoom", panicDivBoom},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var received crash.Event
			server := captureServer(t, &received)
			defer server.Close()
			client := newCaptureClient(t, server.URL, "main-server")
			func() {
				defer func() { _ = recover() }() // swallow the re-panic
				defer client.Recover(context.Background())
				tc.boom()
			}()
			if len(received.Threads) == 0 || len(received.Threads[0].Frames) == 0 {
				t.Fatalf("%s: no frames captured (event likely failed validation)", tc.name)
			}
			top := received.Threads[0].Frames[0].Function
			if strings.HasPrefix(top, "runtime.") {
				t.Fatalf("%s: top frame is runtime glue %q — trim list missed this panic kind", tc.name, top)
			}
			if !strings.Contains(top, tc.origin) {
				t.Fatalf("%s: top frame = %q, want the application origin %q", tc.name, top, tc.origin)
			}
		})
	}
}

// A panic during graceful shutdown / after a client disconnect carries an already-done
// context; the crash must still be reported (the send detaches from caller cancellation).
func TestRecoverDeliversWithCancelledContext(t *testing.T) {
	var received crash.Event
	server := captureServer(t, &received)
	defer server.Close()
	client := newCaptureClient(t, server.URL, "main-server")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the panic

	func() {
		defer func() { _ = recover() }()
		defer client.Recover(ctx)
		boomForCaptureTest()
	}()

	if received.Exception.Reason != "captured-boom" {
		t.Fatalf("crash was dropped on a cancelled context: reason=%q", received.Exception.Reason)
	}
}
