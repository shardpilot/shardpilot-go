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
- Remote config client (`FetchRemoteConfig` + never-fail typed getters): explicit `GET {RemoteConfigURL}/config/v1/{workspace}/{environment}/{client_id}` fetches with ETag revalidation over a durable last-known-good cache, so a restart or an offline start still serves the previously fetched configuration. See "Remote config" below.
- Opt-in bounded disk spool (`Config.SpoolDir`): worker batches that fail retriably survive restarts as byte-identical envelope records (server-deduped by `event_id`; the failed batch retains its built wire bytes, so an in-process retry resends the exact encoding the failure spooled even when the caller mutates nested `Props`/`Context` values after handoff — one encoding per `event_id` for wire, disk, and retry) and resend before fresh events, bounded by count/byte caps (oldest dropped) and a 7-day retry-age cap, with a dead-letter callback for every drop. A resent chunk settles by the response's **per-event verdicts**: only confirmed deliveries count as resent, while a per-event `rejected` or consent-suppressed outcome dead-letters with the matching class (a `202` is not delivery confirmation per event). Disk participation is strictly consent-grant-gated; see "Privacy & consent".
- Opt-in `LoadOrCreateAnonymousID(path)` helper for a persisted UUIDv7 anonymous identifier.
- Crash reporting (`pkg/crash`): sends the canonical crash wire schema to `POST {base}/api/v1/crashes/ingest`, with sanitized breadcrumbs, PII scrubbing, and fatal/non-fatal emit APIs.

## Installation

Install the latest tagged release:

```bash
go get github.com/shardpilot/shardpilot-go@v0.5.0-alpha
```

`v0.5.0-alpha` is the latest tag. It ships the explicit-fetch remote config client (`FetchRemoteConfig` + never-fail typed getters over a durable last-known-good cache), the opt-in consent-gated bounded disk spool (`Config.SpoolDir`), the `X-ShardPilot-Schema-Revision` batch declaration, and full-jitter retry backoff documented in this README, on top of the v0.4.0-alpha consent API, `SignIngestJWT` Mode-B mint helper, and `pkg/crash`. To pin the previous tag that ships the consent API, result callbacks, and JWT mint but none of the above, use:

```bash
go get github.com/shardpilot/shardpilot-go@v0.4.0-alpha
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
| `HTTPClient` | no | Optional `*http.Client` behind every request the SDK makes (event batches, consent posts, remote-config fetches) — pooled transports, proxies, mTLS, instrumentation. Nil (default) keeps the SDK's internal clients. Every attempt stays bounded by the sooner of `HTTPTimeout` and the caller's context deadline (an injected client without its own `Timeout` cannot stretch an attempt), and remote-config fetches still refuse redirects (the SDK derives that client with `CheckRedirect` pinned). |
| `APIKey` | with `RemoteConfigURL` | Publishable `sp_ingest_` client key that authenticates remote-config fetches (`Authorization: Bearer` on the GET). Never `Token` — a Mode B ingest JWT cannot authenticate remote config. Held in memory; never logged. |
| `RemoteConfigURL` | no | Remote-config base URL (a dedicated config origin, never the ingest URL). Empty disables the remote-config client. Validated like `IngestURL`. Requires `APIKey`. |
| `RemoteConfigCachePath` | no | File for the durable last-known-good remote-config cache record (file `0600`; directories the SDK creates are `0700`, and a pre-existing parent's permissions are never changed — the cache lives in a caller-chosen, possibly shared directory. The record is read back through a hard size limit; an over-limit file is discarded as corrupt, never loaded whole). Empty keeps the cache in-memory only. Independent of `SpoolDir`; never enables consent persistence. |
| `RemoteConfigRevalidateInterval` | no | Opt-in periodic remote-config revalidation. Zero (default) keeps explicit-fetch-only behavior — no timer, nothing new on the wire. When set, a background timer re-runs the standard conditional GET at max(interval, server `Cache-Control` max-age — 300s before one is seen — floored at 60s). See "Remote config". Ignored without `RemoteConfigURL`. |
| `ExperimentsURL` | no | Opt-in experiment assignment consumer (dark today — see "Experiments"): the control-plane experiments base URL **including** the deployment's API prefix (e.g. `https://<control-plane>/api/cp/v1`). Unlike the other base URLs a path prefix is expected; query/fragment/user info are still rejected. Empty disables the consumer entirely. Requires `APIKey`. |
| `ExperimentSubjectKey` | for assignment fetches | This installation's persisted `spcid_...` experiment subject key (grammar `^spcid_[A-Za-z0-9_-]{20,64}$`, enforced at construction) — source it from `LoadOrCreateExperimentSubjectKey`. A **dedicated** identifier: never derive it from, and never replace, `AnonymousID`. Empty makes assignment fetches fail `subject_key_unavailable` before any network use. |
| `ExperimentAssignmentCachePath` | no | File for the durable per-experiment last-known-good assignment records, scoped by the (workspace, app, environment, subject, URL, experiment) tuple (same file discipline and bounded read-back as `RemoteConfigCachePath`; one client per cache path). Empty keeps assignment records in-memory only. |
| `ExperimentAssignmentRevalidateInterval` | no | Opt-in AUTOMATIC assignment revalidation lane (default OFF: assignments refresh only by explicit fetches). Each cycle re-fetches every cached assignment at max(interval, 60s). Halts after the lane receives an authoritative `401`/`403`, until the client is re-initialized. Ignored without `ExperimentsURL`. |
| `SpoolDir` | no | Opt-in state directory for the bounded disk spool and the persisted consent decision (`spool.json`, `consent.json`, `spool-wipe-owed`; dir `0700`, files `0600` — a pre-existing directory with looser permissions is tightened to `0700` at the first write, and a refused tighten is a persist failure: nothing is written through a directory whose privacy could not be established). Startup reads `spool.json` through a hard limit derived from `SpoolMaxBytes` (an over-limit file is discarded as corrupt, never loaded whole). Empty (default) keeps the queue memory-only — nothing is ever written to disk. **One client per `SpoolDir` is the supported topology**: every spool save reloads and merges the on-disk record by `event_id` as a safety net (last-writer-wins over the merged view; a record a sibling acked concurrently can be resurrected and resent — at-least-once, server-deduped), not a concurrency feature; sharing shows up in `Stats.SpoolForeignMerged`. |
| `ConsentFloor` | no | Opt-in client-side consent floor (`&ConsentFloorConfig{}`): consent-first gating (`ErrConsentUnknown` until an explicit decision), a durable consent-receipt outbox under `SpoolDir` (32-cap FIFO, retried until acknowledged, decision-order delivery), the grant-receipt dispatch gate, forced-minor denial semantics, and `Close`'s `ErrConsentPending` durability backstop. Nil (default) keeps the documented server-side posture unchanged. See "Privacy & consent". |
| `SpoolMaxEvents` | no | Spool count cap, default 2000; the oldest events are dropped past it (via `OnSpoolDeadLetter`). |
| `SpoolMaxBytes` | no | Spool byte cap over serialized envelopes, default 1 MiB (1,048,576), same oldest-first eviction. |
| `OnSpoolDeadLetter` | no | `func(SpoolDeadLetter)` called with every event the spool drops undelivered (capacity, 7-day retry-age expiry, terminal outcome, consent purge/refusal). A capacity eviction reports only once the rewrite that removed it durably lands — an eviction a failed rewrite left in the on-disk record is not final yet (a restart would reload and resend it), so its callback defers until it is. Panic-recovered like `OnBatchResult`. |
| `SchemaRevision` | no | Overrides the ingest envelope schema-set revision declared on `events:batch` publishes via the `X-ShardPilot-Schema-Revision` request header. Default: `DefaultSchemaRevision`, the revision this SDK release was coordinated against. |
| `DisableSchemaRevision` | no | Stops declaring a schema-set revision entirely (no header; undeclared always passes the server's handshake in every mode) — the no-rebuild escape hatch if an armed enforce-mode handshake rejects this build's revision as stale. |
| `OnBatchResult` | no | `func(BatchResult)` called after each successful batch publish with the server's per-event outcomes. Runs on the publish path (may be called concurrently); keep it fast and non-blocking. A panic inside it is recovered. |

The example programs read these from `SHARDPILOT_*` environment variables; the SDK itself reads no environment variables.

Crash client (`crash.ClientOptions`): `IngestURL` (crash ingest base URL), `APIKey` (needs `crash:write`), plus optional `App` (`AppInfo{ID,Version,BuildID}` — defaulted onto every event; **required for automatic panic capture**, and `App.ID` must equal the API key's app scope), `Source` (component slug), `HTTPClient`, `Logger`, `Sampler`, `MaxAttempts` (default 2), `RetryBackoff` (default 50ms). Default HTTP timeout is 30s.

## Wire contract

App-first event envelope (`POST {IngestURL}/v1/events:batch`, `Authorization: Bearer <token>`). Each envelope carries `event_id`, `schema_version`, `event_name`, `source`, `event_ts`, `workspace_id`, `app_id`, `environment_id`, and optional `user_id`, `anonymous_id`, `session_id`, `session_sequence`, `platform`, `app_version`, `app_build`, `context`, `props`.

The envelope is **universal** — no domain-specific fields. Vertical context (e.g. `match_id`) goes in `Props` and serializes under `props`. **Banned legacy fields** never appear in SDK source or on the wire: `project_id`, `game_id`, `env`, `event_ts_server`, `event_seq_session`, top-level `build_version`. Use `app_version` / `app_build` for version metadata.

The `202` batch response carries four **disjoint** aggregate counters — `accepted` / `rejected` / `duplicates` / `suppressed` — plus an `events[]` list with one entry per event (`event_id`, `status`, optional `code` / `message`). Every event is counted in exactly one aggregate: `rejected` counts hard rejects only (tracking-plan and per-event schema verdicts), a benign `event_id` re-send counts only under `duplicates`, and consent suppressions only under `suppressed` — so `rejected > 0` always means events were dropped for a contract reason. `status` is one of `accepted`, `observed` (`event_name` not registered), `duplicate`, `suppressed_no_consent`, `suppressed_ad_revenue_consent`, or `rejected` — for suppressed events the `202` is **not** delivery confirmation. (`BatchResult` surfaces the `Accepted`/`Rejected`/`Duplicates` aggregates; consent suppressions are visible per event through the `events[]` list.) The `accepted` / `rejected` / `duplicates` aggregates fold into `Snapshot()` — the top-level `suppressed` aggregate is not decoded, so consent suppression shows up in `Snapshot()` only through the per-status breakdown in `Stats.ByStatus` (derived from `events[]`) — and the per-event list is surfaced through the optional `Config.OnBatchResult func(BatchResult)` callback — the only way to learn which individual events the server rejected, suppressed, observed, or folded as duplicates. The callback runs on the publish path (the background flush worker and synchronous `Track` publishes share it, so it may be called concurrently); keep it fast and non-blocking, and a panic inside it is recovered so a buggy callback cannot stop delivery.

Non-2xx responses carry an error envelope `{"error":{"code","message","details":[{"field","code","message"}]}}` (codes such as `validation_error`, `unauthorized`, `forbidden`, `rate_limited`, `internal_error`; detail codes such as `events_rate_limited` or `monthly_quota_exceeded`). The SDK parses it into the returned `*HTTPStatusError` — `ErrorCode`, `ErrorMessage`, `Details`, and the `Retry-After` header as `RetryAfter` (both standard forms, delta-seconds and HTTP-date, like the crash client; the analytics deferral clamps at 24h, while the crash client's short in-process retry loop uses its own much smaller bound) — so logs and callers see the real cause. After a rate-limited automatic publish, the background worker holds further automatic attempts until the `Retry-After` deadline passes and retries AT that deadline (a dedicated wake — not the next flush tick, which can be much later when `FlushInterval` exceeds the hint); explicit `Flush` (and `Close`) still attempt immediately because they carry caller intent, and a renewed failure re-arms the deferral. A retryable failure **without** a `Retry-After` hint paces itself: the first failure retries at the flush cadence, and each further consecutive failure defers by a full-jitter random wait in [1s, ceiling], the ceiling doubling per failure up to 60s — so an outage is probed with growing, de-synchronized spacing instead of a fixed-cadence retry storm — and a successful publish resets the schedule. A success that ends a deferral or failure streak (typically a synchronous `Track` while the worker waits) also wakes the worker to retry its pending retryable work immediately — the held batch and any requeued spooled chunks alike — instead of leaving it for the next flush tick.

Consent decisions ride their own endpoint (`POST {IngestURL}/v1/consent`), never the event envelope, with body `workspace_id`, `app_id`, `environment_id`, `actor_identifier`, `categories` (`{"analytics": <bool>}`), `decided_at` (RFC3339), and a fresh UUIDv7 `idempotency_key`.

Remote configuration is fetched from `GET {RemoteConfigURL}/config/v1/{workspace_id}/{environment_id}/{client_id}` (`Authorization: Bearer <APIKey>`, each path segment percent-escaped, `If-None-Match` revalidation when a cached ETag exists; no query parameters, no body, and never the schema-revision header — that is a batch-route contract).

Crash reports go to `POST {base}/api/v1/crashes/ingest` (`Authorization: Bearer <api-key-with-crash:write>`) with a stable `crash_id`, `occurred_at`, app/platform/os, device & context maps, exception metadata, binary modules with `debug_id`/`load_address`, per-thread raw instruction addresses, optional pre-symbolicated frames, optional `raw_text`, and breadcrumbs. The crash structs are a **hand-maintained mirror** of the ShardPilot crash ingest API's OpenAPI schema.

The ingest response is surfaced through the optional `ClientOptions.OnResult func(Result)` callback (fired on both manual `Emit`/`EmitFatal` and the auto-capture path, on the calling goroutine): `Result` carries the server-assigned `CrashID`/`Fingerprint`, a `Suppressed` flag (the crash was accepted but **not stored** because the actor withheld consent — the HTTP status is still `2xx`), and any `Warnings`. Suppression and warnings are also logged. When a `429`/`5xx` carries a `Retry-After` header (delta-seconds or HTTP-date), the retry loop waits that long (clamped to a safe maximum) instead of the fixed backoff.

## Remote config

Configure `RemoteConfigURL` + `APIKey` (and usually `RemoteConfigCachePath` + a persisted `AnonymousID` — the anonymous ID is the fetch's `client_id`; without one a fetch fails `client_id_unavailable`), then call `FetchRemoteConfig(ctx)` whenever your service wants fresh configuration:

- **Explicit-fetch-only.** The SDK never refreshes on its own — no polling, no `Cache-Control` interpretation, no experiment assignment or client-side rule evaluation. Every fetch is one `GET`, revalidated with the cached ETag. Fetches are fenced by the client lifecycle exactly like synchronous `Track` publishes: a fetch that begins after `Close` returns `ErrClosed`, and `Close` waits (bounded by its context) for in-flight fetches, so no fetch I/O or cache write outlives it.
- **Durable last-known-good cache**, one record keyed by the (workspace, environment, client_id, base URL) tuple. A record written for any other scope is a miss (never served, its ETag never sent) and is overwritten by the next successful fetch; a corrupt record is a clean start. No TTL: the cache survives restarts and serves offline, and the typed getters serve from it before (and without) any fetch.
- **Failure taxonomy** (the Defold/Unity contract): a transient failure — offline, `408`, `429`, `5xx`, malformed or over-1MB body — serves the cached snapshot as a *success* with `FromCache=true` and `Reason` set (or fails with the same code when no usable cache exists). **`401`/`403` fail closed**: the cached snapshot is *never* served for that outcome (a revoked key must not keep supplying configuration), but the cache record and the getter snapshot are left untouched — a later `200` under a valid credential resumes over the same cache. Every other status (`404`, `3xx`, `413`, other `4xx`) is a permanent failure: the fetch errors rather than passing off stale values as healthy. **Redirects are not followed on this route** — a `302`/`307` is classified as the permanent `http_3xx` itself, never silently chased into serving the redirect target's body. A fetch ended by the **caller's own context** (cancellation or the caller's deadline) returns that context error with no cache fallback and no side effects; only SDK-internal timeouts classify as the transient `http_0`. A permanent (or fail-closed `401`/`403`) failure is authoritative **for the fetch that received it**; it does not poison later fetches — a subsequent transient failure serves the durable last-known-good cache, matching the canonical Defold/Unity behavior (every fetch classifies independently; only a fresh `200` replaces the cache).
- **Typed getters** (`RemoteConfigValue/String/Number/Bool/Values/Version`) read the in-memory snapshot only: never the network, never an error — the caller's default on a missing key *and* on a type mismatch (a stored `false` is served as `false`).
- **Not consent-gated.** Denied analytics consent neither blocks fetches nor clears the config cache — configuration is client-public tuning, not telemetry. `RemoteConfigCachePath` works without `SpoolDir`.
- **429 cooldown — a deliberate delta vs the Defold/Unity SDKs** (which ignore `Retry-After` on this route): a `429`'s `Retry-After` (digits-only seconds, floor 1s, clamp 24h; absent reads as the floor) arms an in-memory next-fetch-allowed deadline. An explicit fetch inside the window does not touch the network — it returns the cache-served `transient_429` outcome, indistinguishable from a live `429`. This is the client half of the platform's server-side remote-config fetch rate limit. The deadline is never persisted and only time expires it. A stale `429` that loses an in-flight race — landing after a newer fetch already settled an authoritative outcome — does not arm the window (the same per-scope sequence fence installs use).
- **Opt-in periodic revalidation (`RemoteConfigRevalidateInterval`) — default OFF.** The explicit-fetch-only default above is unchanged when unset. When opted in, a background timer re-runs the SAME conditional GET (`If-None-Match` with the cached ETag, full failure taxonomy) so a running client converges on a server-side change — a kill switch flip included — within one interval instead of at its next explicit fetch. The schedule anchors to the server: each cycle waits max(configured interval, last observed `Cache-Control` max-age — 300s before one is observed, and restored to 300s when a usable success stops advertising one — floored at 60s), so the knob can slow the timer down but never drive it faster than the server's advertised cadence. The cadence is observed on **usable fenced outcomes only** (a fresh `200` install or a `304` revalidation) — a transient or refused response's incidental header moves nothing. A fetch that observes a **changed** cadence re-arms a pending timer when the new anchor is shorter — the next tick pulls in instead of waiting out the old longer schedule (never pushed out; and a stale overlapping response cannot overwrite the cadence — the max-age store sits behind the same sequence fence as installs and the `429` cooldown). A tick inside an armed `429` cooldown performs **no network call** (the cooldown's cache-served outcome) and does not reschedule tighter. One addition to per-fetch classification: after the **timer's own** fetch receives an authoritative `401`/`403` that won the per-scope fence (a stale overlapping refusal that lost to a newer settled success halts nothing), the timer halts until the client is re-initialized — an unattended loop must not keep re-asking an endpoint that authoritatively refused it. Explicit fetches keep classifying per fetch and never resume the halted timer. (The schedule anchor and the timer-halt rule are pending platform ratification; these are the recommended defaults.)

## Experiments (dark, opt-in)

The experiment assignment consumer and the exposure/outcome fact producers (GAP-017 / ADR-0259). **Everything here ships dark**: with no opt-in configured the SDK's wire behavior is byte-identical to before, and the platform's experimentation flags are OFF in every environment today — an opted-in consumer simply receives `403` (`experimentation runtime is disabled` / `experiment assignment fetch is disabled`) and fails closed until enablement. During this dark phase a stock publishable key also lacks the `experiment_assignment_read` scope (`401 invalid runtime token`) until the control-plane auth leg lands; integration tests need an explicitly scoped runtime token.

- **Subject key (`spcid`).** Mint and persist a dedicated installation subject key with the opt-in `LoadOrCreateExperimentSubjectKey(path)` helper (`spcid_` + UUIDv7; the same atomic no-overwrite publish as `LoadOrCreateAnonymousID`) and wire it into `Config.ExperimentSubjectKey`. It is the assignment fetch's `subject_key` and nothing else: it never authenticates anything, never rides analytics facts, and is **never** derived from or a replacement for `AnonymousID`.
- **Assignment fetch.** Configure `ExperimentsURL` (+ `APIKey`) and call `FetchExperimentAssignment(ctx, experimentKey)`: one `GET {ExperimentsURL}/runtime/experiments/assignment?app_key&environment_key&experiment_key&subject_key` (`Authorization: Bearer <APIKey>`; `AppID`/`EnvironmentID` are sent as the app/environment **keys** — the assignment plane matches keys, not ids). The parsed `ExperimentAssignment` carries the verdict: `Assigned` with `VariantKey`/`VariantPayload`, or one of the three not-assigned shapes distinguished by `Reason` (absent = deterministic traffic gate; `kill_switch` = operator kill; `targeting_unmatched`), plus — for client_id-unit experiments — the `SubjectFactKey` (`sfk1_` + 64 hex), the only value permitted as the analytics fact subject. The body must ECHO the requested scope — a `200` naming another app, environment, or experiment classifies as malformed and is never returned or cached (preload holds durable records to the same contract), and an incomplete verdict (missing keys, an unknown unit or reason, a missing or non-positive version) is malformed too. Failure taxonomy is the remote-config canon per fetch: transients (offline, `408`, `429`, `5xx`, malformed) serve the per-experiment cached assignment as `FromCache=true`; `401`/`403` fail closed **for that fetch** with no cross-fetch latch; `404`/unexpected are permanent. Two assignment-only extras (ADR-0259 Amendment 2): a `403` whose JSON `error` EQUALS `experiment real-subject assignment is disabled` — the platform's real-subjects sentinel, exact string match only — additionally **drops** the cached record and its subject fact key; and the automatic lane below halts on authoritative refusals. No `Retry-After` cooldown exists on this plane (none is invented). `ExperimentAssignmentCachePath` makes the per-experiment records durable across restarts.
- **Automatic revalidation lane (`ExperimentAssignmentRevalidateInterval`) — default OFF.** When opted in, a background lane re-fetches every cached assignment each cycle (max(interval, 60s)). After the **lane's own** fetch receives any authoritative `401`/`403` — the sentinel included — that won the per-scope fence (a stale overlapping refusal halts nothing), it stops scheduling until the client is re-initialized; host-triggered fetches keep classifying per fetch and never resume it.
- **Exposure/outcome facts.** `TrackExperimentExposure`/`EnqueueExperimentExposure` emit ONE `experiment_exposure` per assignment identity — experiment key + version + assignment key — per client instance (client-side dedupe, at-most-once per launch; a concurrent duplicate waits for the in-flight attempt — honoring its own context — and reports success only once an exposure actually emitted; provably refused attempts re-arm, while a no-status transport failure or a client-side discard after admission consumes the slot rather than risking a double count) when the app first acts on an assignment; `TrackExperimentOutcome`/`EnqueueExperimentOutcome` emit `experiment_outcome` with `outcome_key` + `outcome_value` (finite number or boolean). Facts are built strictly to the ingest allowlist — `experiment_key`, `experiment_version`, `assignment_key`, `variant_key`, `assignment_unit` (+ the outcome pair) and nothing else; for client_id-unit assignments `assignment_key` carries the `SubjectFactKey`, never the raw `spcid_` value — with `anonymous_id` always the configured `AnonymousID` (required; it is what makes GDPR erasure reach the fact), `user_id` always omitted, and envelope source always `client`. A not-assigned verdict is refused (`ErrExperimentNotAssigned`), and so is an assignment echoing another app/environment scope (`ErrExperimentScopeMismatch`) — facts are built only from verdicts fetched for this client's configured `AppID`/`EnvironmentID`. The facts ride the EXISTING `Track`/`Enqueue` pipeline and inherit the configured consent posture: with `Config.ConsentFloor`, unknown consent ⇒ drop (`ErrConsentUnknown`) and denied ⇒ drop (`ErrConsentDenied`); with the floor nil (default), the documented server-side posture applies unchanged. **Producer-lane admission is decision-gated and CLOSED today**: the ingest edge unconditionally rejects these two event names from publishable client keys, and the analytics client_id-unit flag defaults off — the lane is dark end to end until the platform's producer decision and flag pair open it.

## Privacy & consent

- `SetConsent(analyticsGranted bool)` is tri-state: **unknown** (initial, pipeline open), **granted** (open), **denied** (events dropped at enqueue with `ErrConsentDenied`; pending queue cleared and in-flight batch aborted — cleared events count as `Dropped`, never `Published`). `SetConsentDecision` is the typed form and adds the **forced-minor denial** (`ConsentDecisionDeniedForcedMinor`): analytics-wise identical to denied everywhere, with the receipt carrying `reason: "denied_forced_minor"` so the backend can tell a band-forced denial from a chosen one.
- The live consent state is **in-memory only**; the SDK does not restore it at startup. Read `Consent()` and re-apply with `SetConsent` to survive restarts. (Exception: under the opt-in consent floor with `SpoolDir` set, the persisted decision **is** restored as the live state — see below.)
- **Opt-in client-side consent floor (`Config.ConsentFloor`) — the mode split.** The default (nil) is everything documented in this section: the server-side-responsibility posture, unchanged. Setting `ConsentFloor: &ConsentFloorConfig{}` opts this client into the consent-first floor the engine SDKs ship, for integrations that need client-side enforcement (for example user-facing adopters bound by a DPIA condition to hold events until an explicit grant): **(1) gating** — `Track`/`Enqueue` refuse `ErrConsentUnknown` until an explicit decision is recorded (distinct from `ErrConsentDenied`, so hosts can tell "ask the user" from "the user said no"), and with `SpoolDir` the persisted decision reloads as the live state at startup (the durable receipt trail's tail overrides — and heals — a stale record, and spooled events reload only under a grant the resolved state confirms); the floor covers the **configured** identity: an event whose per-event `UserID`/`AnonymousID` override resolves to a different effective actor is refused with `ErrConsentActorMismatch` (per-actor decisions beyond the configured identity stay on the server-side consent path); **(2) durable receipts** — each explicit decision mints exactly one receipt into a per-client outbox (32 receipts, FIFO oldest-evicted, no TTL, `consent-outbox.json` under `SpoolDir`; in-memory without it), retained until the server acknowledges it and re-sent **verbatim** across restarts (same `idempotency_key`, same `decided_at` — the server de-duplicates), delivered strictly in decision order with server `Retry-After` honored on `429` **and** `5xx` and jittered backoff otherwise; a failed outbox write never evicts (the write is owed and retried; `Stats.ConsentOutboxPersistFailed`); receipts deliver under denied/unknown too — a receipt documents the decision itself; **(3) the grant-receipt dispatch gate** — while an analytics-grant receipt is retained undispatched, event legs hold (`ErrConsentReceiptPending` from `Track`/`Flush`) so post-grant events can never overtake the grant on the wire and be `suppressed_no_consent` on a strict-consent workspace; released on an observed HTTP outcome for the receipt — success or a status error, never gated on its acknowledgement, while a no-response failure keeps holding — and an empty pipeline is never gated; **(4) teardown durability** — `Close` completes only when every retained receipt is durably on disk (or delivered); otherwise it returns `ErrConsentPending` and stays retryable, so a consent decision's receipt is never silently lost — and a close remnant that could be neither delivered nor durably retained is reported on every `Close` via `ErrEventsDiscarded` rather than read as a clean teardown. Receipt identifiers are bounded at 512 bytes (rejected, never truncated; the outbox sanitizer drops oversized or malformed entries fail-safe on load and save). Consent-plane observability: `Stats.ConsentRecorded/ConsentFailed/ConsentOutboxEvicted/ConsentOutboxPersistFailed/LastConsentError`. Note the credential-tier rule below still applies under the floor: a Mode A publishable key records denials only, so a grant receipt dispatched with one takes the server's terminal `403` (dropped and diagnosed; the gate releases and the trail continues).
- **Recorded design decision — live posture unchanged, disk strictly grant-only.** This SDK's open-by-default LIVE posture is deliberately unchanged: `ConsentUnknown` keeps the pipeline open, and denied stays a hard drop. DISK participation (the `SpoolDir` spool) is strictly grant-only, gated on a **persisted** grant for both writes and loads: setting `SpoolDir` persists every `SetConsent` decision to `consent.json`, and spool **writes** open only while the live state is granted *and* that grant record was successfully persisted — a grant whose record write fails keeps the spool closed (would-have-spooled batches go to `OnSpoolDeadLetter` with reason `consent`) until a later persist succeeds. The record is **scoped to the actor**: it carries a SHA-256 digest of the configured (workspace, environment, `UserID`, `AnonymousID`) tuple — a fixed-size digest, never the verbatim identifiers — and a record whose digest does not match the current configuration is no decision at all, so a grant written for one actor never authorizes disk participation for another over a reused `SpoolDir` (logout/login, tenant or workspace switch). The grant is also enforced **per envelope**: an event whose effective actor differs from the configured tuple (a per-event `Event.UserID`/`Event.AnonymousID` override) still rides the live pipeline, but never that grant onto disk — on a retriable failure such envelopes dead-letter (`consent`) instead of spooling. Spool **loads** at startup happen only from a persisted matching grant; any other persisted state (absent, denied, unreadable, other-actor) purges the record at init, and any live transition to non-grant purges it too. A failed purge owes a wipe (the `spool-wipe-owed` marker, or an in-memory flag if even the marker cannot be created) and fails the spool closed — no append, no load, no resend, surfaced as `Stats.LastError = "spool_purge_failed"` — until the wipe succeeds; while a wipe is owed, `SetConsent(true)` retries it first and does **not** write the granted record until it lands. `RemoteConfigCachePath` alone never enables consent persistence.
- An explicit decision posts fire-and-forget to `/v1/consent` **only when an actor identity is configured** (`Config.UserID`, else `Config.AnonymousID`); with neither set the decision is applied locally only and nothing is posted server-side. When posted, decisions are transmitted by a single per-client sender in call order, so deny-then-grant cannot arrive reversed. `Close` waits (bounded by its context) for prior decisions to finish sending.
- **Server-side acceptance depends on the credential tier.** A publishable client ingest key (Mode A) may record **denials only**: a receipt whose category values are all `false` is accepted for the key's own scope, while a **grant** receipt is rejected `403` with detail code `consent_grant_requires_verified_credential` and dropped by policy (the SDK logs it quietly; local consent state is unaffected). Grants are recorded server-side only through a consent-write-capable **service** credential (or the internal route) — and until such a grant lands, a previously recorded denial keeps suppressing that actor's events (`suppressed_no_consent`). Client-key consent writes also consume ingest budget whether accepted or rejected (deliberate anti-flood on a public credential).
- **Strict-consent (enforce) workspaces terminally suppress events published under `unknown` consent.** Unlike the consent-first client SDKs (Defold/Unity/Unreal), this SDK's initial `unknown` state keeps the event pipeline open — but on a workspace whose effective strict consent mode is `enforce`, the server fails closed: every event whose actor has no explicit analytics consent recorded server-side is suppressed per event as `suppressed_no_consent` inside the `202` (never a batch-level error), so the publish "succeeds" while delivering nothing. Ensure the grant is **recorded server-side before** the actor's first events are published: `SetConsent(true)` posts its receipt fire-and-forget in the background with no success signal exposed (failures are only logged), so calling it immediately before publishing does not synchronize the grant — events flushed before the `/v1/consent` write lands are still suppressed; when admission must be guaranteed from the first event, record the grant out-of-band through a consent-write-capable service credential first. The receipt also covers **only the configured actor** (`Config.UserID`, else `Config.AnonymousID`); events that override the actor per event (`Event.UserID` / `Event.AnonymousID`) need consent recorded for each such actor through that same service path (see the previous bullet — that credential tier is what can record grants at all). Watch `Config.OnBatchResult` or the `Snapshot().ByStatus` breakdown for `suppressed_no_consent` to detect suppression.
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
| `anonymous_id.go`, `spcid.go`, `ids.go`, `clock.go` | Opt-in anonymous-ID and experiment-subject-key helpers, event-ID generation, injectable clock. |
| `experiments.go`, `experiment_events.go` | Dark opt-in experiment assignment consumer and exposure/outcome fact producers (GAP-017). |
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
- **Fail-safe HTTP.** HTTPS required outside localhost/loopback (private-network HTTP only via explicit opt-in). No durable local queue unless the opt-in disk spool is configured (`SpoolDir`); no retry storms; the worker retains at most one failed in-memory batch.
- The `go` directive stays at **1.24** for consumer compatibility even though CI also exercises the current toolchain.

## Roadmap

Pre-v1; the API is explicitly unstable.

- The remote config client (`FetchRemoteConfig` + typed getters), the opt-in bounded disk spool (`Config.SpoolDir`), the schema-revision declaration on `events:batch`, and the full-jitter retry backoff shipped in `v0.5.0-alpha`; the changelog's `Unreleased` section is currently empty.
- Public developer docs are planned for `docs.shardpilot.com`; that domain is not yet provisioned.

`v0.3.0-alpha` (tagged) removed the game-flavored `MatchID` field from the universal `Event` envelope; carry that context in `Props["match_id"]` instead (wire payload unchanged).

## AI-assisted integration

Integrating with an AI coding tool (Claude Code, Cursor, etc.)? This repo ships a machine-readable integration skill at [`.claude/skills/shardpilot-go-integration/SKILL.md`](.claude/skills/shardpilot-go-integration/SKILL.md) — a source-verified brief of the pinned install tag, credential handling, this SDK's server-side consent posture, crash reporting, and a verification checklist, written so AI-generated integrations stay contract-correct. Claude Code discovers it automatically; point other tools at that file. The planned developer docs site (`docs.shardpilot.com`, not yet live) will additionally publish an `llms.txt` family of machine-readable documentation indexes once it launches.

## Related

- The **ShardPilot platform** receives the event batches, consent decisions, and crash reports this SDK sends, and issues and introspects the ingest API keys and per-tenant signing secrets it uses (this backend SDK mints the short-lived per-tenant JWT itself via `SignIngestJWT`).
- Sibling client SDK: [`shardpilot-defold`](https://github.com/shardpilot/shardpilot-defold) — the public Defold engine SDK.
- Developer documentation for the ingest API and SDKs is planned for `docs.shardpilot.com`.

## License

See [LICENSE](LICENSE) and [NOTICE](NOTICE). Security policy: [SECURITY.md](SECURITY.md).
