# ShardPilot Go SDK

ShardPilot Go SDK is a v0 public-preview source SDK for sending app-first
telemetry to ShardPilot ingest. The API is pre-v1 and may change before v1.
After this PR merges and maintainers create the tag, the current alpha module
tag candidate will be `v0.2.0-alpha`.

Pin the current alpha milestone explicitly after tag creation:

```bash
go get github.com/shardpilot/shardpilot-go@v0.2.0-alpha
```

v0.2.0-alpha is an early alpha pre-release; the API is unstable and may change
before v1. v0.2.0-alpha and later require Go 1.24 or newer. v0.1.0 is
retracted in v0.1.1+ go.mod; prefer v0.2.0-alpha or later for crash reporting
or v0.1.2 for analytics-only integrations.

Floating release-style install shape:

```bash
go get github.com/shardpilot/shardpilot-go@latest
```

For development beyond the tagged alpha milestone, source checkouts or module
replacements can still be used during evaluation.

The module `go` directive is kept at Go 1.24 as the source-compatibility
baseline for SDK consumers. Current supported Go toolchains are still
recommended for production builds, and CI verifies both Go 1.24.x compatibility
and the current Go toolchain target.

## Basic Usage

```go
client, err := shardpilot.NewClient(shardpilot.Config{
    IngestURL:     os.Getenv("SHARDPILOT_INGEST_URL"),
    Token:         os.Getenv("SHARDPILOT_TOKEN"),
    WorkspaceID:   os.Getenv("SHARDPILOT_WORKSPACE_ID"),
    AppID:         os.Getenv("SHARDPILOT_APP_ID"),
    EnvironmentID: os.Getenv("SHARDPILOT_ENVIRONMENT_ID"),
    Source:        shardpilot.SourceBackend,
    AppVersion:    "0.1.0",
})
if err != nil {
    return err
}
defer client.Close(context.Background())

err = client.Track(context.Background(), shardpilot.Event{
    Name:      "session_start",
    SessionID: "session-example",
    Props: map[string]any{
        "surface": "backend",
    },
})
```

See [`examples/basic`](examples/basic) for a runnable backend/server example
using environment variables.

## Crash Reporting

The crash SDK lives in `pkg/crash` and sends the canonical crash-symbolicator
wire schema to `POST /api/v1/crashes/ingest` on the configured
crash-symbolicator base URL. The canonical schema is owned by
`shardpilot/crash-symbolicator` in `api/openapi.yaml`; the SDK mirrors that
schema with typed Go structs. It generates UUIDv7 crash IDs when needed,
records a fixed ring of analytics-event-name breadcrumbs, strips PII and raw
identifier material, and intentionally has no API surface for screenshots,
network payloads, or attachments. This is an alpha release; the API may change
before v1.0.

```go
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
    ingestURL := os.Getenv("SHARDPILOT_CRASH_SYMBOLICATOR_URL")
    apiKey := os.Getenv("SHARDPILOT_API_KEY")
    if ingestURL == "" || apiKey == "" {
        log.Fatal("SHARDPILOT_CRASH_SYMBOLICATOR_URL and SHARDPILOT_API_KEY are required")
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
        OccurredAt: time.Now().UTC(),
        App:        crash.AppInfo{ID: "app_example", Version: "0.2.0-alpha", BuildID: "synthetic-build"},
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
```

See [`examples/crash`](examples/crash) for the runnable version.

## Versioning

- `v0.1.x` is the analytics SDK only.
- `v0.2.x` adds the crash SDK under `pkg/crash`; the analytics API is unchanged.

## Wire Contract

The analytics SDK sends:

```text
POST {IngestURL}/v1/events:batch
Content-Type: application/json
Authorization: Bearer <token>
```

Event envelopes are app-first and use:

- `workspace_id`
- `app_id`
- `environment_id`
- `event_ts`
- `session_sequence`

The SDK does not expose or send legacy public SDK fields such as `project_id`,
`game_id`, `env`, `event_ts_server`, `event_seq_session`, or top-level
`build_version`. Use `app_version` or `app_build` for application version
metadata.

The `Event` envelope is universal and does not carry domain-specific fields.
Game- or vertical-specific context (for example `match_id`) goes in `Props`,
which is sent as `props` (e.g. `Props["match_id"]` is serialized to
`props.match_id`). It is not a top-level ShardPilot ingest envelope field.

The crash SDK sends:

```text
POST {SHARDPILOT_CRASH_SYMBOLICATOR_URL}/api/v1/crashes/ingest
Content-Type: application/json
Authorization: Bearer <api-key-with-crash:write>
```

Crash reports include a stable `crash_id`, `occurred_at`, `app`,
`platform`/`os`, device/context maps, exception metadata, binary images with
`debug_id` and `load_address`, per-thread raw instruction addresses, optional
pre-symbolicated frame fields, optional `raw_text`, and breadcrumbs.

## Behavior

- Batches default to 25 events and are capped at 100.
- The async queue is bounded and memory-only; there is no durable local queue
  in v0.
- The worker retains at most one failed in-memory batch for a later retry.
  There is no disk persistence and no unbounded local replay.
- Retry attempts happen through the normal worker cadence: no more frequently
  than `FlushInterval`, plus explicit `Flush` or `Close` calls.
- The SDK does not start concurrent retry storms. While one worker batch is
  retained, sustained failures can apply backpressure and new queued events may
  be dropped according to `BufferSize`.
- If the queue is full, `Enqueue` drops the event, increments `Dropped`, and
  returns `ErrQueueFull`.
- `Track` sends one event synchronously for tests and utilities.
- `Flush` drains queued events.
- `Close` marks the client closed and flushes remaining queued events until the
  context deadline.
- Event IDs are generated with crypto/rand UUIDv4-like identifiers when absent.
- Event timestamps default to `time.Now().UTC()` when absent.
- HTTP ingest URLs are allowed for localhost and loopback development. Use
  HTTPS elsewhere unless explicitly opting into private-network HTTP with
  `AllowInsecurePrivateNetwork`.

## Security And Privacy

- Do not commit tokens or real customer/player data.
- Tokens are held in memory only.
- Tokens are never logged by the SDK.
- Full event payloads are never logged by the SDK by default.
- Do not send raw provider payloads, raw player/customer payloads, diffs,
  patches, code/file/archive content, prompts, completions, secrets, or
  unsanitized stack/backtrace payloads.
- The SDK does not perform provider, model, GitHub, billing, control-plane
  write, or automatic action calls.
- Product integrations can use this SDK through normal app-first telemetry,
  but no product-specific event names or domain logic live in SDK core.

Developer docs are planned for `docs.shardpilot.com`, but that domain is not
provisioned or live in this wave.
