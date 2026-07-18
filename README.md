# shardpilot-go

> Go client SDK for ShardPilot — sends app-first analytics events (and, optionally, crash reports) to the ShardPilot ingest API. Zero third-party dependencies, stdlib only.

## Status

Real, tested, working code — **early alpha**. The API is pre-v1 and may change before v1.

- Two import paths: the root `shardpilot` package (analytics) and `pkg/crash` (crash reporting).
- Module `go` directive is **1.24** (the source-compatibility baseline for SDK consumers). CI verifies both Go 1.24.x and the current toolchain (1.26.5).
- Pre-launch: ingest endpoints are reached via the local Compose stack or a deployed environment you provide; there is no public production endpoint.

## What it does

- Builds and sends app-first event envelopes to `POST {IngestURL}/v1/events:batch` with bearer-token auth.
- Synchronous `Track`, bounded async `Enqueue`, `Flush`, and `Close`; in-memory delivery stats via `Snapshot` (including a per-status breakdown in `Stats.ByStatus`), plus an optional `OnBatchResult` callback that surfaces the server's per-event outcomes (which events were rejected, suppressed, observed, or folded as duplicates).
- Bounded batching (default 25 events, capped at 100) with retry of retryable HTTP responses; a `429`'s `Retry-After` hint defers the next automatic publish attempt, and retryable failures without a hint (server unreachable, `5xx` without the header) fall back to client-side exponential backoff with full jitter — first failure at the flush cadence, then a random wait in [1s, ceiling] with the ceiling doubling per consecutive failure up to 60s, reset on success (explicit `Flush`/`Close` still try immediately). Memory-only queue (no durable on-disk queue). Event ids and timestamps are stamped once at intake, so a re-sent batch is de-duplicated by the service instead of double-counted.
- Non-2xx responses surface the server's error envelope on the returned error: `HTTPStatusError` carries the HTTP status plus the machine-readable `ErrorCode`, per-field `Details`, and the parsed `RetryAfter` hint — not just a bare status number.
- Optional explicit analytics consent (`SetConsent` / `Consent`) with a separate `POST {IngestURL}/v1/consent` endpoint.
- Opt-in `LoadOrCreateAnonymousID(path)` helper for a persisted UUIDv7 anonymous identifier.
- Crash reporting (`pkg/crash`): sends the canonical crash wire schema to `POST {base}/api/v1/crashes/ingest`, with sanitized breadcrumbs, PII scrubbing, and fatal/non-fatal emit APIs.

## Installation

Install the latest tagged release:

```bash
go get github.com/shardpilot/shardpilot-go@v0.4.0-alpha
```

`v0.4.0-alpha` is the latest tag. It ships the consent API (`SetConsent` / `Consent`), `LoadOrCreateAnonymousID`, the backend-only `SignIngestJWT` Mode-B mint helper, and the default actor identity fields documented in this README, on top of the de-gamed universal `Event` envelope and `pkg/crash`. To pin the earlier tag that ships only the de-gamed envelope (`v0.3.0-alpha`) and crash SDK (`v0.2.0-alpha`), use:

```bash
go get github.com/shardpilot/shardpilot-go@v0.3.0-alpha
```

For analytics only, `v0.1.2` is available. **`v0.1.0` is retracted** in the module's `go.mod` (use `v0.1.2` or `v0.2.0-alpha` or later). `v0.1.2` and later require **Go 1.24+**.

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

> **`session_id` is required for non-`backend` sources.** The ingest API requires a `session_id` on every event whose `source` is not `backend` (i.e. `SourceClient` / `SourceServer`). This is a backend/service-tier SDK and does **not** manage a session lifecycle — it passes `Event.SessionID` and `Event.SessionSequence` through verbatim. So if you configure a non-`backend` `Source`, you must set `SessionID` (and a monotonic `SessionSequence`) on each event yourself; an event with no `session_id` is rejected (`400`) and, because per-event statuses are not surfaced, the **whole batch is dropped silently**. `SourceBackend` events do not require a session. (The client SDKs — Unity, Unreal, Defold — open and number sessions automatically; this SDK is deliberately pass-through, since a backend rarely has a single per-process session.)

## Quick start (crash reporting)

A runnable example lives in [`examples/crash`](examples/crash). It demonstrates the client API surface with a synthetic stub event; it does not install a panic handler or capture a real crash.

```go
import "github.com/shardpilot/shardpilot-go/pkg/crash"

client, err := crash.NewClient(crash.ClientOptions{
    IngestURL: os.Getenv("SHARDPILOT_CRASH_INGEST_URL"),
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

`EmitFatal` always sends — fatal crashes are never sampled. `Emit` (non-fatal) is subject to the client sampler, whose default is **deterministic every-10th-call per client** (each `crash.Client` gets its own counter unless you pass a shared `Sampler`: that client's calls 10, 20, 30, … transmit; the first 9 non-fatals of each client are always dropped, so a client that emits fewer than 10 non-fatals in its lifetime reports none — and two clients emitting 5 non-fatals each report none, even though the process emitted 10). A sampled-out `Emit` returns `nil` exactly like a sent one — only the `OnResult` callback (or its absence) tells them apart. Override with the public `Sampler` option (e.g. an allow-all sampler transmits every non-fatal).

### Automatic panic capture

For Go services you can capture panics automatically instead of building `Event`s by hand. Configure the client with the app identity (and, for a multi-component product, a `Source` slug), then defer `Recover` at each goroutine / request-handler boundary:

```go
client, err := crash.NewClient(crash.ClientOptions{
    IngestURL: os.Getenv("SHARDPILOT_CRASH_INGEST_URL"),
    APIKey:    os.Getenv("SHARDPILOT_API_KEY"),
    App:       crash.AppInfo{ID: "fortress-fury", Version: "1.4.0"},
    Source:    "main-server", // which component/repo this crash came from
})

func handleRequest(ctx context.Context) {
    defer client.Recover(ctx) // reports the panic as fatal, then RE-PANICS
    // ... work that may panic ...
}
```

`Recover` recovers a panic, reports it synchronously (so the report is sent before the process exits), and then **re-panics** so the program's normal crash behaviour is preserved. Use it once per goroutine — a panic in a bare `go func(){…}()` with no deferred `Recover` is not captured. `CapturePanic(ctx, recovered)` reports an already-recovered value **without** re-panicking, for callers that intentionally recover and keep running. A nil/unconfigured client is a safe no-op (and `Recover` still re-panics).

Captured frames are **pre-symbolicated** from the Go runtime (package-qualified function, file, line — no native modules or addresses, accepted by the crash ingest API). `App` fields and `Source` are stamped onto every event that doesn't set its own; a per-event value always wins.

## Configuration

`shardpilot.Config` fields:

| Field | Required | Notes |
|---|---|---|
| `IngestURL` | yes | Absolute base URL, no path/query/fragment. HTTPS required outside localhost/loopback (or private nets with `AllowInsecurePrivateNetwork`). |
| `Token` | yes | Bearer token (Mode A `sp_ingest_` publishable key or Mode B per-tenant JWT). Held in memory; never logged. |
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
| `OnBatchResult` | no | `func(BatchResult)` called after each successful batch publish with the server's per-event outcomes. Runs on the publish path (may be called concurrently); keep it fast and non-blocking. A panic inside it is recovered. |

The example programs read these from `SHARDPILOT_*` environment variables; the SDK itself reads no environment variables.

Crash client (`crash.ClientOptions`): `IngestURL` (crash ingest base URL), `APIKey` (needs `crash:write`), plus optional `App` (`AppInfo{ID,Version,BuildID}` — defaulted onto every event; **required for automatic panic capture**, and `App.ID` must equal the API key's app scope), `Source` (component slug), `HTTPClient`, `Logger`, `Sampler`, `MaxAttempts` (default 2), `RetryBackoff` (default 50ms). Default HTTP timeout is 30s.

## Wire contract

App-first event envelope (`POST {IngestURL}/v1/events:batch`, `Authorization: Bearer <token>`). Each envelope carries `event_id`, `schema_version`, `event_name`, `source`, `event_ts`, `workspace_id`, `app_id`, `environment_id`, and optional `user_id`, `anonymous_id`, `session_id`, `session_sequence`, `platform`, `app_version`, `app_build`, `context`, `props`.

The envelope is **universal** — no domain-specific fields. Vertical context (e.g. `match_id`) goes in `Props` and serializes under `props`. **Banned legacy fields** never appear in SDK source or on the wire: `project_id`, `game_id`, `env`, `event_ts_server`, `event_seq_session`, top-level `build_version`. Use `app_version` / `app_build` for version metadata.

The `202` batch response carries four **disjoint** aggregate counters — `accepted` / `rejected` / `duplicates` / `suppressed` — plus an `events[]` list with one entry per event (`event_id`, `status`, optional `code` / `message`). Every event is counted in exactly one aggregate: `rejected` counts hard rejects only (tracking-plan and per-event schema verdicts), a benign `event_id` re-send counts only under `duplicates`, and consent suppressions only under `suppressed` — so `rejected > 0` always means events were dropped for a contract reason. `status` is one of `accepted`, `observed` (`event_name` not registered), `duplicate`, `suppressed_no_consent`, `suppressed_ad_revenue_consent`, or `rejected` — for suppressed events the `202` is **not** delivery confirmation. (`BatchResult` surfaces the `Accepted`/`Rejected`/`Duplicates` aggregates; consent suppressions are visible per event through the `events[]` list.) The `accepted` / `rejected` / `duplicates` aggregates fold into `Snapshot()` — the top-level `suppressed` aggregate is not decoded, so consent suppression shows up in `Snapshot()` only through the per-status breakdown in `Stats.ByStatus` (derived from `events[]`) — and the per-event list is surfaced through the optional `Config.OnBatchResult func(BatchResult)` callback — the only way to learn which individual events the server rejected, suppressed, observed, or folded as duplicates. The callback runs on the publish path (the background flush worker and synchronous `Track` publishes share it, so it may be called concurrently); keep it fast and non-blocking, and a panic inside it is recovered so a buggy callback cannot stop delivery.

Non-2xx responses carry an error envelope `{"error":{"code","message","details":[{"field","code","message"}]}}` (codes such as `validation_error`, `unauthorized`, `forbidden`, `rate_limited`, `internal_error`; detail codes such as `events_rate_limited` or `monthly_quota_exceeded`). The SDK parses it into the returned `*HTTPStatusError` — `ErrorCode`, `ErrorMessage`, `Details`, and the `Retry-After` header as `RetryAfter` (both standard forms, delta-seconds and HTTP-date, like the crash client; the analytics deferral clamps at 24h, while the crash client's short in-process retry loop uses its own much smaller bound) — so logs and callers see the real cause. After a rate-limited automatic publish, the background worker holds further automatic attempts until the `Retry-After` deadline passes and retries AT that deadline (a dedicated wake — not the next flush tick, which can be much later when `FlushInterval` exceeds the hint); explicit `Flush` (and `Close`) still attempt immediately because they carry caller intent, and a renewed failure re-arms the deferral. A retryable failure **without** a `Retry-After` hint paces itself: the first failure retries at the flush cadence, and each further consecutive failure defers by a full-jitter random wait in [1s, ceiling], the ceiling doubling per failure up to 60s — so an outage is probed with growing, de-synchronized spacing instead of a fixed-cadence retry storm — and a successful publish resets the schedule.

Consent decisions ride their own endpoint (`POST {IngestURL}/v1/consent`), never the event envelope, with body `workspace_id`, `app_id`, `environment_id`, `actor_identifier`, `categories` (`{"analytics": <bool>}`), `decided_at` (RFC3339), and a fresh UUIDv7 `idempotency_key`.

Crash reports go to `POST {base}/api/v1/crashes/ingest` (`Authorization: Bearer <api-key-with-crash:write>`) with a stable `crash_id`, `occurred_at`, app/platform/os, device & context maps, exception metadata, binary modules with `debug_id`/`load_address`, per-thread raw instruction addresses, optional pre-symbolicated frames, optional `raw_text`, and breadcrumbs. The crash structs are a **hand-maintained mirror** of the ShardPilot crash ingest API's OpenAPI schema.

The ingest response is surfaced through the optional `ClientOptions.OnResult func(Result)` callback (fired on both manual `Emit`/`EmitFatal` and the auto-capture path, on the calling goroutine): `Result` carries the server-assigned `CrashID`/`Fingerprint`, a `Suppressed` flag (the crash was accepted but **not stored** because the actor withheld consent — the HTTP status is still `2xx`), and any `Warnings`. Suppression and warnings are also logged. When a `429`/`5xx` carries a `Retry-After` header (delta-seconds or HTTP-date), the retry loop waits that long (clamped to a safe maximum) instead of the fixed backoff.

## Privacy & consent

- `SetConsent(analyticsGranted bool)` is tri-state: **unknown** (initial, pipeline open), **granted** (open), **denied** (events dropped at enqueue with `ErrConsentDenied`; pending queue cleared and in-flight batch aborted — cleared events count as `Dropped`, never `Published`).
- Consent state is **in-memory only**; the SDK does not persist it. Read `Consent()` and re-apply with `SetConsent` to survive restarts.
- An explicit decision posts fire-and-forget to `/v1/consent` **only when an actor identity is configured** (`Config.UserID`, else `Config.AnonymousID`); with neither set the decision is applied locally only and nothing is posted server-side. When posted, decisions are transmitted by a single per-client sender in call order, so deny-then-grant cannot arrive reversed. `Close` waits (bounded by its context) for prior decisions to finish sending.
- **Server-side acceptance depends on the credential tier.** A publishable client ingest key (Mode A) may record **denials only**: a receipt whose category values are all `false` is accepted for the key's own scope, while a **grant** receipt is rejected `403` with detail code `consent_grant_requires_verified_credential` and dropped by policy (the SDK logs it quietly; local consent state is unaffected). Grants are recorded server-side only through a consent-write-capable **service** credential (or the internal route) — and until such a grant lands, a previously recorded denial keeps suppressing that actor's events (`suppressed_no_consent`). Client-key consent writes also consume ingest budget whether accepted or rejected (deliberate anti-flood on a public credential).
- **Strict-consent (enforce) workspaces terminally suppress events published under `unknown` consent.** Unlike the consent-first client SDKs (Defold/Unity/Unreal), this SDK's initial `unknown` state keeps the event pipeline open — but on a workspace whose effective strict consent mode is `enforce`, the server fails closed: every event whose actor has no explicit analytics consent recorded server-side is suppressed per event as `suppressed_no_consent` inside the `202` (never a batch-level error), so the publish "succeeds" while delivering nothing. Ensure the grant is **recorded server-side before** the actor's first events are published: `SetConsent(true)` posts its receipt fire-and-forget in the background, so calling it immediately before publishing does not synchronize the grant — events flushed before the `/v1/consent` write lands are still suppressed — and it covers **only the configured actor** (`Config.UserID`, else `Config.AnonymousID`); events that override the actor per event (`Event.UserID`) need consent recorded for each such actor through a consent-write-capable service path (see the previous bullet — that same credential tier is what can record grants at all). Watch `Config.OnBatchResult` or the `Snapshot().ByStatus` breakdown for `suppressed_no_consent` to detect suppression.
- Crash reports strip PII and raw identifier material; there is no API surface for screenshots, network payloads, or attachments.
- Do not commit tokens or real customer/player data. Tokens and full event payloads are never logged by default.

## Minting client ingest JWTs (backend-only, Mode B)

> **Backend-only — this helper holds the per-tenant signing secret.** It belongs in a trusted server-side game backend and must **never** be compiled into a shipped client binary. Client SDKs (Unity, Unreal, Defold) consume a minted token through a `token_provider` callback and never see the secret. This Go SDK is a backend/service-tier SDK, so its dual-mode role is the inverse: it **mints** the short-lived per-user JWTs that client SDKs then fetch over your own authenticated channel.

`SignIngestJWT` mints a short-lived HS256 JWT that the ShardPilot ingest API's Mode-B verifier accepts. The per-tenant signing secret (and its key id `kid`) is minted, rotated, and served by ShardPilot; your backend obtains it out-of-band and passes it in. The helper does not fetch, store, or rotate the secret — it only signs. It is **additive** and does not change the service-tier `Config.Token` / `Authorization: Bearer` transport.

```go
// Per-tenant secret + kid, obtained out-of-band from ShardPilot.
// Secret is RAW bytes (base64url-decode first if you received it encoded).
key := shardpilot.SigningKey{KID: kid, Secret: secret}

token, err := shardpilot.SignIngestJWT(key, shardpilot.IngestJWTClaims{
    Subject:       verifiedUserID,    // JWT `sub`; your authenticated user_id
    WorkspaceID:   workspaceID,
    AppID:         appID,
    EnvironmentID: environmentID,
    BindAnon:      deviceAnonymousID, // optional: the persistent anonymous_id
})
if err != nil {
    // An empty/invalid key, malformed kid, or invalid claims fail fast here:
    // SignIngestJWT returns no token. Never hand out an empty/invalid token.
    return err
}
// Hand `token` to exactly one client over an authenticated channel. NEVER log it.
```

Defaults: issuer `shardpilot`, audience `shardpilot-ingest`, lifetime 5m (equal to the server's 5m `iat`-age window, which the verifier enforces regardless of `exp`; capped at the server's 15m max). Override with `WithIngestIssuer` / `WithIngestAudience` / `WithIngestLifetime` (plus `WithIngestNow` / `WithIngestClock` for tests). Scope is fixed to `analytics:ingest`. Every input is validated at mint time, so a returned token is never rejected downstream for a malformed claim. The secret is `[]byte` (never logged); `SigningKey.ZeroSecret()` wipes a copy.

## Project layout

| Path | What it is |
|---|---|
| `client.go`, `queue.go`, `transport.go`, `envelope.go`, `batch_result.go` | Analytics client: lifecycle, bounded queue, HTTP transport, envelope builder, public batch-response types. |
| `config.go`, `event.go`, `consent.go`, `metrics.go`, `errors.go` | Config validation, public `Event`, consent state machine, `Stats`, sentinel errors. |
| `anonymous_id.go`, `ids.go`, `clock.go` | Opt-in anonymous-ID helper, event-ID generation, injectable clock. |
| `ingest_jwt.go` | Backend-only `SignIngestJWT` Mode-B ingest-JWT mint helper. |
| `internal/uuidv7/` | Shared UUIDv7 generator (crash IDs, anonymous IDs, consent idempotency keys). |
| `pkg/crash/` | Crash SDK: `client.go`, `event.go` (typed wire schema), `capture.go` (automatic panic capture), `breadcrumbs.go`, `sanitize.go`. |
| `examples/basic/`, `examples/crash/` | Runnable analytics and crash examples (env-var driven). |
| `*_test.go`, `quickstart_test.go`, `client_benchmark_test.go` | Unit, quickstart, and benchmark tests. |

## Build & test

No Makefile — standard Go tooling. CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs `gofmt` check, `go test ./...`, and `go vet ./...` on Go 1.24.x and 1.26.5, plus a release version-consistency check (`scripts/check_release_consistency.sh`; see [`docs/release.md`](docs/release.md)).

```bash
go build ./...
go test ./...
go vet ./...
gofmt -l .
```

## Conventions & boundaries

- **Zero third-party dependencies** — stdlib only. Keep it that way.
- **No domain logic in core.** No product-specific event names or game/vertical fields in the universal envelope.
- **The SDK only sends telemetry.** It performs no provider, model, GitHub, billing, or account-management write calls, and triggers no automatic actions.
- **Fail-safe HTTP.** HTTPS required outside localhost/loopback (private-network HTTP only via explicit opt-in). No durable local queue; no retry storms; the worker retains at most one failed in-memory batch.
- The `go` directive stays at **1.24** for consumer compatibility even though CI also exercises the current toolchain.

## Roadmap

Pre-v1; the API is explicitly unstable.

- The consent API, `LoadOrCreateAnonymousID`, the backend-only `SignIngestJWT` Mode-B mint helper, and optional default actor identity fields shipped in `v0.4.0-alpha`; for changes merged since that tag, see the `Unreleased` section of [CHANGELOG.md](CHANGELOG.md).
- Public developer docs are planned for `docs.shardpilot.com`; that domain is not yet provisioned.

`v0.3.0-alpha` (tagged) removed the game-flavored `MatchID` field from the universal `Event` envelope; carry that context in `Props["match_id"]` instead (wire payload unchanged).

## Related

- The **ShardPilot platform** receives the event batches, consent decisions, and crash reports this SDK sends, and issues and introspects the ingest API keys and per-tenant signing secrets it uses (this backend SDK mints the short-lived per-tenant JWT itself via `SignIngestJWT`).
- Sibling client SDK: [`shardpilot-defold`](https://github.com/shardpilot/shardpilot-defold) — the public Defold engine SDK.
- Developer documentation for the ingest API and SDKs is planned for `docs.shardpilot.com`.

## License

See [LICENSE](LICENSE) and [NOTICE](NOTICE). Security policy: [SECURITY.md](SECURITY.md).
