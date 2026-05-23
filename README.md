# ShardPilot Go SDK

ShardPilot Go SDK is a v0 public-preview source SDK for sending app-first
telemetry to ShardPilot ingest. The API is pre-1.0 and may change before v1.
After this PR merges and maintainers create the tag, the first alpha module tag
for Phase 1 will be `v0.1.0`.

Pin that Phase 1 alpha milestone explicitly after tag creation:

```bash
go get github.com/shardpilot/shardpilot-go@v0.1.0
```

v0.1.0 is a Phase 1 alpha milestone tied to ADR-0176; it is not a GA or 1.0 release.

Floating release-style install shape:

```bash
go get github.com/shardpilot/shardpilot-go@latest
```

For development beyond the tagged alpha milestone, source checkouts or module
replacements can still be used during evaluation.

The module `go` directive is kept at Go 1.23 as the source-compatibility
baseline for SDK consumers. Current supported Go toolchains are still
recommended for production builds, and CI verifies both Go 1.23.x compatibility
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

## Wire Contract

The SDK sends:

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

`MatchID` is a convenience metadata field and is placed under `props.match_id`.
It is not a top-level ShardPilot ingest envelope field.

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
- Project Tower can use this SDK through normal app-first telemetry, but no
  Project Tower-specific event names or domain logic live in SDK core.

Developer docs are planned for `docs.shardpilot.com`, but that domain is not
provisioned or live in this wave.
