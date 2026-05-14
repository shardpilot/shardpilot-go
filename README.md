# ShardPilot Go SDK

ShardPilot Go SDK is a v0 public-preview source SDK for sending app-first
telemetry to ShardPilot ingest. The API is pre-1.0 and may change before v1.
No module release, tag, or package publication has been cut in this wave.

Future release-style install shape:

```bash
go get github.com/shardpilot/shardpilot-go@latest
```

Until an explicit release wave publishes tags, use source checkouts or module
replacements during evaluation.

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
- If the queue is full, `Enqueue` drops the event, increments `Dropped`, and
  returns `ErrQueueFull`.
- `Track` sends one event synchronously for tests and utilities.
- `Flush` drains queued events.
- `Close` marks the client closed and flushes remaining queued events until the
  context deadline.
- Event IDs are generated with crypto/rand UUIDv4-like identifiers when absent.
- Event timestamps default to `time.Now().UTC()` when absent.
- Non-local HTTP ingest URLs are rejected; use HTTPS outside localhost or
  loopback development.
- v0 does not retry failed batches. Applications may call `Flush`/`Track`
  again when appropriate.

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
