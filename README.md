# shardpilot-go

> Go client SDK for ShardPilot — sends app-first analytics events (and, optionally, crash reports) to the ShardPilot ingest plane. Zero third-party dependencies, stdlib only.

Platform context: [`../AGENTS.md`](../AGENTS.md). Governing ADRs: app-first analytics [ADR-0139](../docs/architecture/adr/0139-universal-app-analytics-core-refactor.md), crash SDK design [ADR-0191](../docs/architecture/adr/0191-crash-sdk-design.md), dual-mode ingest auth [ADR-0222](../docs/architecture/adr/0222-dual-mode-client-ingest-auth-publishable-key-and-per-tenant-jwt.md).

## Status

Real, tested, working code — **early alpha**. The API is pre-v1 and may change before v1.

- Two import paths: the root `shardpilot` package (analytics) and `pkg/crash` (crash reporting).
- Module `go` directive is **1.24** (the source-compatibility baseline for SDK consumers). CI verifies both Go 1.24.x and the current toolchain (1.26.4).
- Pre-launch: ingest endpoints are reached via the local Compose stack or a deployed environment you provide; there is no public production endpoint.

## What it does

- Builds and sends app-first event envelopes to `POST {IngestURL}/v1/events:batch` with bearer-token auth.
- Synchronous `Track`, bounded async `Enqueue`, `Flush`, and `Close`; in-memory delivery stats via `Snapshot`.
- Bounded batching (default 25 events, capped at 100) with retry of retryable HTTP responses; memory-only queue (no durable on-disk queue).
- Optional explicit analytics consent (`SetConsent` / `Consent`) with a separate `POST {IngestURL}/v1/consent` endpoint.
- Opt-in `LoadOrCreateAnonymousID(path)` helper for a persisted UUIDv7 anonymous identifier.
- Crash reporting (`pkg/crash`): sends the canonical crash-symbolicator wire schema to `POST {base}/api/v1/crashes/ingest`, with sanitized breadcrumbs, PII scrubbing, and fatal/non-fatal emit APIs.

## Installation

```bash
go get github.com/shardpilot/shardpilot-go@v0.3.0-alpha
```

`v0.3.0-alpha` is the latest tag (de-games the universal `Event` envelope). `pkg/crash` landed in `v0.2.0-alpha`. For analytics only, `v0.1.2` is available. **`v0.1.0` is retracted** in the module's `go.mod` (use `v0.1.2` or `v0.2.0-alpha` or later). `v0.1.2` and later require **Go 1.24+**.

## Quick start (analytics)

A runnable backend example lives in [`examples/basic`](examples/basic). The minimal flow:

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

// purchase is a backend-source canonical event: the server-validated,
// real-money purchase reported after receipt/store validation. The
// canonical schema requires props.amount, props.currency, and props.product.
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

Pick events whose canonical schema allows your configured `Source`. Session/screen events (e.g. `app.session_started`, `app.screen_view`) are client-source-only — a backend client cannot legally send them; backend clients send backend-source events such as `purchase` or `economy_tx`.

## Quick start (crash reporting)

A runnable example lives in [`examples/crash`](examples/crash). It demonstrates the client API surface with a synthetic stub event; it does not install a panic handler or capture a real crash.

```go
import "github.com/shardpilot/shardpilot-go/pkg/crash"

client, err := crash.NewClient(crash.ClientOptions{
    IngestURL: os.Getenv("SHARDPILOT_CRASH_SYMBOLICATOR_URL"),
    APIKey:    os.Getenv("SHARDPILOT_API_KEY"),
})
if err != nil {
    return err
}

client.RecordBreadcrumb("app.session_started")
client.RecordBreadcrumb("level_loaded")

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

err = client.EmitFatal(ctx, crash.Event{
    OccurredAt: time.Now().UTC(),
    App:        crash.AppInfo{ID: "app_example", Version: "0.2.0-alpha", BuildID: "synthetic-build"},
    Platform:   "linux",
    OS:         crash.OSInfo{Name: "linux", Version: "synthetic"},
    Device:     map[string]string{"class": crash.DeviceClassDesktop, "arch": "x86_64"},
    Exception:  crash.ExceptionInfo{Type: "SIGSEGV", Reason: "synthetic fault", CrashedThreadID: "main"},
    Modules: []crash.Module{{
        ID: "examples-crash", Name: "examples-crash",
        DebugID: "AABBCCDDEEFF00112233445566778899", LoadAddress: "0x400000",
    }},
    Threads: []crash.Thread{{
        ID: "main", Name: "main", Crashed: true,
        Frames: []crash.Frame{
            {ModuleID: "examples-crash", InstructionAddress: "0x401015", Function: "main.syntheticCrash", File: "examples/crash/main.go", Line: 42},
            {ModuleID: "examples-crash", InstructionAddress: "0x401000", Function: "main.main", File: "examples/crash/main.go", Line: 36},
        },
    }},
})
```

`EmitFatal` always sends; `Emit` (non-fatal) is subject to the client sampler (default: 1 in 10).

## Configuration

`shardpilot.Config` fields:

| Field | Required | Notes |
|---|---|---|
| `IngestURL` | yes | Absolute base URL, no path/query/fragment. HTTPS required outside localhost/loopback (or private nets with `AllowInsecurePrivateNetwork`). |
| `Token` | yes | Bearer token (Mode A `sp_ingest_` publishable key or Mode B per-tenant JWT, ADR-0222). Held in memory; never logged. |
| `WorkspaceID` / `AppID` / `EnvironmentID` | yes | App-first identity (`workspace → app → environment`). |
| `Source` | yes | `SourceClient`, `SourceServer`, or `SourceBackend`. |
| `AppVersion` / `AppBuild` / `Platform` | no | Default envelope metadata. |
| `UserID` / `AnonymousID` | no | Default actor identity; also the consent `actor_identifier` (UserID preferred). |
| `BatchSize` | no | Default 25, capped at 100. |
| `BufferSize` | no | Async queue capacity, default 1000. |
| `FlushInterval` | no | Default 1s. |
| `HTTPTimeout` | no | Default 2s. |
| `Logger` | no | `Printf`-style logger; never receives tokens or full payloads. |
| `AllowInsecurePrivateNetwork` | no | Allow plain HTTP to RFC1918 private addresses. |

The example programs read these from `SHARDPILOT_*` environment variables; the SDK itself reads no environment variables.

Crash client (`crash.ClientOptions`): `IngestURL` (crash-symbolicator base URL), `APIKey` (needs `crash:write`), plus optional `HTTPClient`, `Logger`, `Sampler`, `MaxAttempts` (default 2), `RetryBackoff` (default 50ms). Default HTTP timeout is 30s.

## Wire contract

App-first event envelope (`POST {IngestURL}/v1/events:batch`, `Authorization: Bearer <token>`). Each envelope carries `event_id`, `schema_version`, `event_name`, `source`, `event_ts`, `workspace_id`, `app_id`, `environment_id`, and optional `user_id`, `anonymous_id`, `session_id`, `session_sequence`, `platform`, `app_version`, `app_build`, `context`, `props`.

The envelope is **universal** — no domain-specific fields. Vertical context (e.g. `match_id`) goes in `Props` and serializes under `props`. **Banned legacy fields** never appear in SDK source or on the wire: `project_id`, `game_id`, `env`, `event_ts_server`, `event_seq_session`, top-level `build_version`. Use `app_version` / `app_build` for version metadata.

Consent decisions ride their own endpoint (`POST {IngestURL}/v1/consent`), never the event envelope, with body `workspace_id`, `app_id`, `environment_id`, `actor_identifier`, `categories` (`{"analytics": <bool>}`), `decided_at` (RFC3339), and a fresh UUIDv7 `idempotency_key`.

Crash reports go to `POST {base}/api/v1/crashes/ingest` (`Authorization: Bearer <api-key-with-crash:write>`) with a stable `crash_id`, `occurred_at`, app/platform/os, device & context maps, exception metadata, binary modules with `debug_id`/`load_address`, per-thread raw instruction addresses, optional pre-symbolicated frames, optional `raw_text`, and breadcrumbs. The crash structs are a **hand-maintained mirror** of crash-symbolicator's `api/openapi.yaml`.

## Privacy & consent

- `SetConsent(analyticsGranted bool)` is tri-state: **unknown** (initial, pipeline open), **granted** (open), **denied** (events dropped at enqueue with `ErrConsentDenied`; pending queue cleared and in-flight batch aborted — cleared events count as `Dropped`, never `Published`).
- Consent state is **in-memory only**; the SDK does not persist it. Read `Consent()` and re-apply with `SetConsent` to survive restarts.
- An explicit decision posts fire-and-forget to `/v1/consent`; decisions are transmitted by a single per-client sender in call order, so deny-then-grant cannot arrive reversed. `Close` waits (bounded by its context) for prior decisions to finish sending.
- Crash reports strip PII and raw identifier material; there is no API surface for screenshots, network payloads, or attachments.
- Do not commit tokens or real customer/player data. Tokens and full event payloads are never logged by default.

## Project layout

| Path | What it is |
|---|---|
| `client.go`, `queue.go`, `transport.go`, `envelope.go` | Analytics client: lifecycle, bounded queue, HTTP transport, envelope builder. |
| `config.go`, `event.go`, `consent.go`, `metrics.go`, `errors.go` | Config validation, public `Event`, consent state machine, `Stats`, sentinel errors. |
| `anonymous_id.go`, `ids.go`, `clock.go` | Opt-in anonymous-ID helper, event-ID generation, injectable clock. |
| `internal/uuidv7/` | Shared UUIDv7 generator (crash IDs, anonymous IDs, consent idempotency keys). |
| `pkg/crash/` | Crash SDK: `client.go`, `event.go` (typed wire schema), `breadcrumbs.go`, `sanitize.go`. |
| `examples/basic/`, `examples/crash/` | Runnable analytics and crash examples (env-var driven). |
| `*_test.go`, `quickstart_test.go`, `client_benchmark_test.go` | Unit, quickstart, and benchmark tests. |

## Build & test

No Makefile — standard Go tooling. CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs `gofmt` check, `go test ./...`, and `go vet ./...` on Go 1.24.x and 1.26.4.

```bash
go build ./...
go test ./...
go vet ./...
gofmt -l .
```

## Conventions & boundaries

- **Zero third-party dependencies** — stdlib only. Keep it that way.
- **No domain logic in core.** No product-specific event names or game/vertical fields in the universal envelope.
- **The SDK only sends telemetry.** It performs no provider, model, GitHub, billing, control-plane write, or automatic action calls.
- **Fail-safe HTTP.** HTTPS required outside localhost/loopback (private-network HTTP only via explicit opt-in). No durable local queue; no retry storms; the worker retains at most one failed in-memory batch.
- The `go` directive stays at **1.24** for consumer compatibility even though CI also exercises the current toolchain.

## Roadmap

Pre-v1; the API is explicitly unstable. From the changelog `[Unreleased]`:

- Consent API, `LoadOrCreateAnonymousID`, and optional default actor identity fields are landed in `[Unreleased]` (not yet tagged).
- Public developer docs are planned for `docs.shardpilot.com`; that domain is not yet provisioned.

`v0.3.0-alpha` (tagged) removed the game-flavored `MatchID` field from the universal `Event` envelope; carry that context in `Props["match_id"]` instead (wire payload unchanged).

## Related repositories

- [`../analytics-service`](../analytics-service) — ingest plane that receives the event batches and consent decisions.
- [`../crash-symbolicator`](../crash-symbolicator) — owns the canonical crash wire schema (`api/openapi.yaml`) this SDK mirrors.
- [`../control-plane`](../control-plane) — mints/introspects the ingest tokens and API keys used here.
- [`../developers`](../developers) — public docs for the ingest API and SDKs.
- Sibling SDKs: [`../shardpilot-unity`](../shardpilot-unity), [`../shardpilot-unreal`](../shardpilot-unreal), [`../shardpilot-defold`](../shardpilot-defold).

## License

See [LICENSE](LICENSE) and [NOTICE](NOTICE). Security policy: [SECURITY.md](SECURITY.md).
