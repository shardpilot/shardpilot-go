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

// purchase is a backend-source canonical event: the authoritative
// real-money purchase your backend reports AFTER receipt/store validation.
// Required props per the canonical schema: amount, currency, product.
err = client.Track(context.Background(), shardpilot.Event{
    Name:   "purchase",
    UserID: "user-1042",
    Props: map[string]any{
        "amount":   9.99,
        "currency": "USD",
        "product":  "starter_pack",
        "quantity": 1,
    },
})
```

Pick events whose canonical schema allows your configured `Source`. Session
and screen events (for example `app.session_started`, `app.screen_view`) are
client-source-only, so a backend client cannot legally send them; backend
clients send backend-source events such as `purchase` or `economy_tx`.

See [`examples/basic`](examples/basic) for a runnable backend/server example
using environment variables.

## Anonymous IDs (opt-in helper)

`LoadOrCreateAnonymousID(path)` loads a persisted anonymous identifier from a
file, or generates a fresh UUIDv7 and persists it there with 0600 permissions
(creating parent directories as needed). The ID is fully written to a private
temp file and then published to the final path atomically without
overwriting, so the file only ever appears complete: concurrent first runs
racing on the same path converge on a single identifier and never observe a
partial file. It is strictly opt-in: the SDK never calls it implicitly and
never writes files on its own, so server integrations that do not want
on-disk state simply never call it.

```go
anonymousID, err := shardpilot.LoadOrCreateAnonymousID(
    filepath.Join(configDir, "shardpilot", "anonymous_id"))
if err != nil {
    return err
}

client, err := shardpilot.NewClient(shardpilot.Config{
    // ... required fields ...
    AnonymousID: anonymousID,
})
```

`Config.UserID` and `Config.AnonymousID` are optional default actor identity
values: events that do not set their own `UserID`/`AnonymousID` inherit them,
and `SetConsent` uses them as the consent actor identifier (user ID
preferred, else anonymous ID).

## Consent

`Client.SetConsent(analyticsGranted bool)` records an explicit analytics
consent decision, with tri-state semantics:

- **unknown** (initial): no decision recorded, the event pipeline is fully
  open.
- **granted**: the pipeline is open.
- **denied**: events are dropped at enqueue — `Track` and `Enqueue` return
  `ErrConsentDenied` — the pending queue is cleared, and any event batch
  publish already in flight on the network is aborted (cleared and aborted
  events count as `Dropped` in `Snapshot`, never as `Published`).

The state is held in client memory only; the SDK does not persist it. If
consent must survive process restarts, read `Client.Consent()` after a
decision, store it yourself (for example next to the anonymous-ID file), and
re-apply it with `SetConsent` on startup.

An explicit decision is also posted to `POST {IngestURL}/v1/consent` with the
same bearer-token transport as event batches, using `Config.UserID`
(preferred) or `Config.AnonymousID` as `actor_identifier`. The post is
fire-and-forget for the caller — `SetConsent` never blocks on the network —
but decisions are transmitted by a single per-client sender in call order, so
a deny-then-grant cannot arrive at the server reversed. `Close` waits
(bounded by its context) for decisions recorded before it was called to
finish transmitting. Failures are logged quietly via `Config.Logger` and
never affect the local state. If neither identity field is configured, the
decision applies locally only and no request is sent. Consent never rides
the event envelope.

## Minting client ingest JWTs (backend-only, ADR-0222 Mode B)

> **Security: backend-only. This helper holds the per-tenant signing secret.**
> It belongs in a trusted server-side game-backend and **must never** be
> compiled into a shipped client binary. A client SDK (Unity, Unreal, Defold)
> consumes a minted token through a `token_provider` callback and never sees
> the secret. This Go SDK is a backend/service-tier SDK, so its dual-mode role
> is the inverse: it **mints** the short-lived per-user JWTs the client SDKs
> then fetch over your own authenticated channel.

`SignIngestJWT` mints a short-lived HS256 JWT that the analytics-service Mode-B
verifier accepts. The per-tenant signing secret (and its key id, `kid`) is
minted, rotated, and served by control-plane; your backend obtains it
out-of-band (e.g. over control-plane's machine-to-machine serve channel) and
passes it here. This helper does not fetch, store, or rotate the secret — it
only signs a conformant token with it. It is **additive**: it does not change
the service-tier `Config.Token` / `Authorization: Bearer` transport.

```go
// The per-tenant secret + kid, obtained out-of-band from control-plane.
// Secret is RAW bytes (base64url-decode it first if you received it encoded).
key := shardpilot.SigningKey{KID: kid, Secret: secret}

token, err := shardpilot.SignIngestJWT(key, shardpilot.IngestJWTClaims{
    Subject:       verifiedUserID, // the JWT `sub`; your authenticated user_id
    WorkspaceID:   workspaceID,
    AppID:         appID,
    EnvironmentID: environmentID,
    BindAnon:      deviceAnonymousID, // optional: the persistent anonymous_id
})
if err != nil {
    return err
}
// Hand `token` to exactly one client over an authenticated channel. NEVER log it.
```

Defaults: issuer `project-tower-main-server`, audience `analytics-service`,
lifetime 10m (well under the server's 15m max-lifetime and 5m iat-age caps).
Override per deployment with `WithIngestIssuer`, `WithIngestAudience`, and
`WithIngestLifetime` (capped at 15m); `WithIngestNow` / `WithIngestClock` exist
for tests. The scope is fixed to `analytics:ingest`. Every input is validated at
mint time — an empty/over-long subject or `bind_anon`, an empty tenant field, an
invalid `kid`, an empty secret, or an over-long lifetime all return an error and
no token — so a minted token is never rejected downstream for a malformed claim.
The secret is `[]byte` (never a logged string); call `SigningKey.ZeroSecret()` to
wipe a copy you no longer need.

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

    client.RecordBreadcrumb("app.session_started")
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
- `SignIngestJWT` (the backend-only ADR-0222 Mode-B mint helper) is a purely
  additive, backward-compatible addition, so it ships as a minor bump (intended
  `v0.3.0` at release). The git tag is applied at release time.

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

Explicit consent decisions are sent on their own endpoint (never on the
event envelope):

```text
POST {IngestURL}/v1/consent
Content-Type: application/json
Authorization: Bearer <token>
```

with body fields `workspace_id`, `app_id`, `environment_id`,
`actor_identifier`, `categories` (`{"analytics": <bool>}`), `decided_at`
(RFC3339), and a fresh UUIDv7 `idempotency_key`. The service responds
`200 {"recorded": true, "replayed": <bool>}`.

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
- While consent is denied, `Track` and `Enqueue` drop the event, increment
  `Dropped`, and return `ErrConsentDenied`; events pending at the moment of
  denial — queued, already pulled into the worker's batch, or mid-publish on
  the network (the HTTP request is aborted) — are cleared and counted as
  `Dropped`, never as `FailedBatches`, and never publish even if consent is
  granted again later — including when a re-grant lands before the aborted
  request returns.
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
