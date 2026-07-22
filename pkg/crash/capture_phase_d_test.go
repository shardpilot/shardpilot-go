// External-package tests for the ADR-0297 §7d opt-ins (debug-id fill and
// all-goroutine capture): the panic origins must live outside pkg/crash, like
// real application code, for the frame assertions to be meaningful.
package crash_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/shardpilot/shardpilot-go/pkg/crash"
)

// rawCaptureServer records the exact posted body so wire-shape assertions can
// run on the bytes, not a re-marshalled struct.
func rawCaptureServer(t *testing.T, raw *[]byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		*raw = body
		w.WriteHeader(http.StatusAccepted)
	}))
}

func capturePanicWith(t *testing.T, client *crash.Client) {
	t.Helper()
	func() {
		defer func() { _ = recover() }()
		defer client.Recover(context.Background())
		boomForCaptureTest()
	}()
}

// TestRecoverPhaseDDarkDefaultsKeepWireShape pins the SHIPS-DARK posture of both
// §7d opt-ins: with neither enabled, an auto-captured event still marshals zero
// modules (no debug_id anywhere) and exactly the one precise crashed thread —
// byte-shape-identical to the pre-Phase-D SDK.
func TestRecoverPhaseDDarkDefaultsKeepWireShape(t *testing.T) {
	var raw []byte
	server := rawCaptureServer(t, &raw)
	defer server.Close()
	client := newCaptureClient(t, server.URL, "main-server")

	capturePanicWith(t, client)

	body := string(raw)
	if !strings.Contains(body, `"modules":[]`) {
		t.Fatalf("dark default must ship an empty modules list; body: %s", body)
	}
	if strings.Contains(body, `"debug_id"`) {
		t.Fatalf("dark default leaked a debug_id; body: %s", body)
	}
	if strings.Contains(body, "goroutine-") {
		t.Fatalf("dark default leaked goroutine threads; body: %s", body)
	}
	var received crash.Event
	if err := json.Unmarshal(raw, &received); err != nil {
		t.Fatalf("unmarshal captured body: %v", err)
	}
	if len(received.Threads) != 1 || received.Threads[0].ID != "main" || !received.Threads[0].Crashed {
		t.Fatalf("dark default must ship exactly the one crashed thread: %+v", received.Threads)
	}
}

func TestRecoverDebugIDFillAttachesSelfModule(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("debug-id fill reads the ELF test binary; GOOS=%s", runtime.GOOS)
	}
	var received crash.Event
	server := captureServer(t, &received)
	defer server.Close()
	client, err := crash.NewClient(crash.ClientOptions{
		IngestURL:          server.URL,
		APIKey:             "workspace-api-key-test",
		App:                crash.AppInfo{ID: "fortress-fury"},
		Source:             "main-server",
		Logger:             testLogger{t},
		DebugIDFillEnabled: true,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	capturePanicWith(t, client)

	if len(received.Modules) != 1 {
		t.Fatalf("expected the one self-module, got %+v", received.Modules)
	}
	module := received.Modules[0]
	if module.Name == "" {
		t.Fatal("self-module name is empty on the wire")
	}
	if !regexp.MustCompile(`^[0-9a-f]{40,64}$`).MatchString(module.DebugID) {
		t.Fatalf("self-module debug_id %q is not 40-64 lowercase hex chars", module.DebugID)
	}
	if module.LoadAddress != "0x0" {
		t.Fatalf("self-module load_address = %q, want 0x0", module.LoadAddress)
	}
	// Frames stay pre-symbolicated function-only: the self-module adds identity,
	// never addresses.
	for _, frame := range received.Threads[0].Frames {
		if frame.InstructionAddress != "" || frame.Address != "" {
			t.Fatalf("debug-id fill must not add frame addresses: %+v", frame)
		}
	}
}

// parkedForPhaseDTest is the distinctive park point the all-goroutine capture
// assertions look for.
func parkedForPhaseDTest(release <-chan struct{}, started *sync.WaitGroup) {
	started.Done()
	<-release
}

func TestRecoverAllGoroutineCaptureAddsParkedGoroutines(t *testing.T) {
	var received crash.Event
	server := captureServer(t, &received)
	defer server.Close()
	client, err := crash.NewClient(crash.ClientOptions{
		IngestURL:                  server.URL,
		APIKey:                     "workspace-api-key-test",
		App:                        crash.AppInfo{ID: "fortress-fury"},
		Source:                     "main-server",
		Logger:                     testLogger{t},
		AllGoroutineCaptureEnabled: true,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	release := make(chan struct{})
	var started sync.WaitGroup
	for i := 0; i < 3; i++ {
		started.Add(1)
		go parkedForPhaseDTest(release, &started)
	}
	started.Wait()
	defer close(release)

	capturePanicWith(t, client)

	if len(received.Threads) < 2 {
		t.Fatalf("all-goroutine capture shipped %d threads, want the crashed one plus extras", len(received.Threads))
	}
	if received.Threads[0].ID != "main" || !received.Threads[0].Crashed {
		t.Fatalf("thread 0 must stay the precise crashed thread: %+v", received.Threads[0])
	}
	foundParked := false
	for _, thread := range received.Threads[1:] {
		if thread.Crashed {
			t.Fatalf("extra thread %q marked crashed", thread.ID)
		}
		if !strings.HasPrefix(thread.ID, "goroutine-") {
			t.Fatalf("extra thread id %q lacks the goroutine- prefix", thread.ID)
		}
		for _, frame := range thread.Frames {
			if strings.Contains(frame.Function, "parkedForPhaseDTest") {
				foundParked = true
			}
		}
	}
	if !foundParked {
		t.Fatalf("no captured thread parks in parkedForPhaseDTest; threads: %+v", received.Threads)
	}
}

// TestRecoverAllGoroutineCaptureRespectsEventCaps floods the process with parked
// goroutines and proves the shipped event still fits the wire caps (64 threads /
// 256 total frames) — the server received it, so validation passed with the
// extras attached.
func TestRecoverAllGoroutineCaptureRespectsEventCaps(t *testing.T) {
	var received crash.Event
	server := captureServer(t, &received)
	defer server.Close()
	client, err := crash.NewClient(crash.ClientOptions{
		IngestURL:                  server.URL,
		APIKey:                     "workspace-api-key-test",
		App:                        crash.AppInfo{ID: "fortress-fury"},
		Logger:                     testLogger{t},
		AllGoroutineCaptureEnabled: true,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	release := make(chan struct{})
	var started sync.WaitGroup
	for i := 0; i < 100; i++ {
		started.Add(1)
		go parkedForPhaseDTest(release, &started)
	}
	started.Wait()
	defer close(release)

	capturePanicWith(t, client)

	if len(received.Threads) < 2 || len(received.Threads) > 64 {
		t.Fatalf("shipped %d threads, want within (1, 64]", len(received.Threads))
	}
	total := 0
	for _, thread := range received.Threads {
		total += len(thread.Frames)
	}
	if total == 0 || total > 256 {
		t.Fatalf("shipped %d total frames, want within (0, 256]", total)
	}
}
