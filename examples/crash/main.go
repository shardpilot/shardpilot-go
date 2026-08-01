package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/shardpilot/shardpilot-go/pkg/crash"
)

func main() {
	/*
		Production crash capture requires hooking the runtime panic handler.
		This example only demonstrates the client API surface with a synthetic
		stub event; it does not install a panic handler or capture a real crash.
	*/
	ingestURL := os.Getenv("SHARDPILOT_CRASH_INGEST_URL")
	apiKey := os.Getenv("SHARDPILOT_API_KEY")
	// The app id is REQUIRED, not decorative: an API key is bound to a single app, and
	// crash ingest rejects a mismatch with
	//   403 {"code":"forbidden","message":"crash app.id does not match API key app scope"}.
	// A hardcoded placeholder therefore cannot work against any real workspace, so the id
	// is read from the environment — the same shape examples/basic uses for its scope.
	appID := os.Getenv("SHARDPILOT_APP_ID")
	if ingestURL == "" || apiKey == "" || appID == "" {
		log.Fatal("SHARDPILOT_CRASH_INGEST_URL, SHARDPILOT_API_KEY and SHARDPILOT_APP_ID are required")
	}
	appVersion := os.Getenv("SHARDPILOT_APP_VERSION")
	if appVersion == "" {
		appVersion = "0.2.0-alpha"
	}
	appBuild := os.Getenv("SHARDPILOT_APP_BUILD")
	if appBuild == "" {
		appBuild = "synthetic-build"
	}

	client, err := crash.NewClient(crash.ClientOptions{
		IngestURL: ingestURL,
		APIKey:    apiKey,
	})
	if err != nil {
		log.Fatalf("create crash client: %v", err)
	}

	client.RecordBreadcrumb("app.session_started")
	client.RecordBreadcrumb("level_loaded")
	client.RecordBreadcrumb("boss_intro_seen")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.EmitFatal(ctx, crash.Event{
		OccurredAt: time.Now().UTC(),
		App:        crash.AppInfo{ID: appID, Version: appVersion, BuildID: appBuild},
		Platform:   "linux",
		OS:         crash.OSInfo{Name: "linux", Version: "synthetic"},
		Device:     map[string]string{"class": crash.DeviceClassDesktop, "arch": "x86_64"},
		Context:    map[string]string{"session_id": "sha256-session-hash-example"},
		Exception:  crash.ExceptionInfo{Type: "SIGSEGV", Reason: "synthetic fault", CrashedThreadID: "main"},
		Modules: []crash.Module{{
			ID:          "examples-crash",
			Name:        "examples-crash",
			DebugID:     "AABBCCDDEEFF00112233445566778899",
			LoadAddress: "0x400000",
		}},
		Threads: []crash.Thread{{
			ID:      "main",
			Name:    "main",
			Crashed: true,
			Frames: []crash.Frame{
				{ModuleID: "examples-crash", InstructionAddress: "0x401015", Function: "main.syntheticCrash", File: "examples/crash/main.go", Line: 42},
				{ModuleID: "examples-crash", InstructionAddress: "0x401000", Function: "main.main", File: "examples/crash/main.go", Line: 36},
			},
		}},
	})
	if err != nil {
		log.Fatalf("emit crash event: %v", err)
	}
}
