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
	ingestURL := os.Getenv("SHARDPILOT_INGEST_URL")
	apiKey := os.Getenv("SHARDPILOT_API_KEY")
	if ingestURL == "" || apiKey == "" {
		log.Fatal("SHARDPILOT_INGEST_URL and SHARDPILOT_API_KEY are required")
	}

	client, err := crash.NewClient(crash.ClientOptions{
		IngestURL: ingestURL,
		APIKey:    apiKey,
	})
	if err != nil {
		log.Fatalf("create crash client: %v", err)
	}

	client.RecordBreadcrumb("session_start")
	client.RecordBreadcrumb("level_loaded")
	client.RecordBreadcrumb("boss_intro_seen")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.EmitFatal(ctx, crash.Event{
		AppVersion:  "0.2.0-alpha",
		BuildID:     "synthetic-build",
		OS:          crash.OSInfo{Name: "linux", Version: "synthetic"},
		DeviceClass: crash.DeviceClassDesktop,
		StackFrames: []crash.Frame{
			{Function: "main.syntheticCrash", File: "examples/crash/main.go", Line: 42, Module: "examples-crash"},
			{Function: "main.main", File: "examples/crash/main.go", Line: 36, Module: "examples-crash"},
		},
		ThreadState: crash.ThreadStateMain,
		SessionID:   "sha256-session-hash-example",
		OccurredAt:  time.Now().UTC(),
	})
	if err != nil {
		log.Fatalf("emit crash event: %v", err)
	}
}
