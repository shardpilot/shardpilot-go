# shardpilot-go

> Go client SDK for ShardPilot — sends app-first analytics events (and, optionally, crash reports) to the ShardPilot ingest API. Zero third-party dependencies, stdlib only.

## Status

Real, tested, working code — **early alpha**. The API is pre-v1 and may change before v1.

- Two supported import paths: the root `shardpilot` package (analytics) and `pkg/crash` (crash reporting).
- Module `go` directive is **1.25** (the source-compatibility baseline for SDK consumers). CI verifies both Go 1.25.x and the current toolchain (1.27.x).
- `IngestURL` (and, for crash reporting, the `pkg/crash` base URL) is the base URL of the ShardPilot ingest deployment you were given, or of a local stack you run yourself. The SDK does not discover endpoints.

## What it does

- Builds and sends app-first event envelopes to `POST {IngestURL}/v1/events:batch` with bearer-token auth.
- Synchronous `Track`, bounded async `Enqueue`, `Flush`, and `Close`; in-memory delivery stats via `Snapshot` (including a per-status breakdown in `Stats.ByStatus`), a bounded rejection history via `Rejections()`, and an optional `OnBatchResult` callback that surfaces the server's per-event outcomes (which events were rejected, suppressed, observed, or folded as duplicates).
- Bounded batching (default 25 events, capped at 100) with retry of retryable HTTP responses; a `429`'s `Retry-After` hint defers the next automatic publish attempt, and retryable failures without a hint (server unreachable, `5xx` without the header) fall back to client-side exponential backoff with full jitter on their OWN clock, independent of `FlushInterval` — every failure, the first included, waits a random duration in [1s, ceiling], the ceiling doubling from the third consecutive failure up to 60s, reset on success (explicit `Flush`/`Close` still try immediately). Memory-only queue (no durable on-disk queue). Event ids and timestamps are stamped once at intake, so a re-sent batch is de-duplicated by the service instead of double-counted.
- Non-2xx responses surface the server's error envelope on the returned error: `HTTPStatusError` carries the HTTP status plus the machine-readable `ErrorCode`, per-field `Details`, and the parsed `RetryAfter` hint — not just a bare status number.
- Optional explicit analytics consent (`SetConsent` / `ConsentState`) with a separate `POST {IngestURL}/v1/consent` endpoint.
- Remote config client (`FetchRemoteConfig` + never-fail typed getters): explicit config fetches with ETag revalidation over a durable last-known-good cache. Requests omit the client-ID path segment unless consent is granted; startup cache restoration uses the matching consent scope. See "Remote config" below.
- Opt-in bounded disk spool (`Config.SpoolDir`): worker batches that fail retriably survive restarts as byte-identical envelope records (best-effort server deduplication by `event_id`; the failed batch retains its built wire bytes, so an in-process retry resends the exact encoding the failure spooled even when the caller mutates nested `Props`/`Context` values after handoff — one encoding per `event_id` for wire, disk, and retry) and resend before fresh events, bounded by count/byte caps (oldest dropped) and a seven-day local intake-age cap plus a 15-minute window after ambiguous delivery, with a dead-letter callback for every drop. A resent chunk settles by the response's **per-event verdicts**: only confirmed deliveries count as resent, while a per-event `rejected` or consent-suppressed outcome dead-letters with the matching class (a `202` is not delivery confirmation per event). Disk participation is strictly consent-grant-gated; see "Privacy & consent".
- Opt-in `LoadOrCreateAnonymousID(path)` helper for a persisted UUIDv7 anonymous identifier.
- Crash reporting (`pkg/crash`): sends the canonical crash wire schema to `POST {base}/api/v1/crashes/ingest`, with sanitized breadcrumbs, PII scrubbing, and fatal/non-fatal emit APIs.

## Installation

Prepared install command (use after the announced release):

```bash
go get github.com/shardpilot/shardpilot-go@v0.10.0
```

`v0.10.0` is the latest tag target for this source. Install after its release announcement; the release PR prepares the source, and tagging/publication is a separate step.

**Consent getter migration:** replace `client.Consent()` with `client.ConsentState()`. The named string result retains `unknown`, `granted`, `denied` and `denied_forced_minor`; a zero-value client reports `unknown`. The old getter has no compatibility alias.

**Schema declaration migration:** remove `DisableSchemaRevision` and leave `SchemaRevision` empty unless your writer supplies its deployed schema revision. The old default digest constant is removed. See the [migration table](CHANGELOG.md#migration-from-v072-alpha).

**Breaking changes for experiment integrations:** call `ApplyExperimentVariant` when using a variant, then record outcomes against that application. Facts are sealed and delivered asynchronously. Declare the game's age band explicitly; an omitted or non-adult band is authoritatively ineligible. Follow [Migration from v0.6.4-alpha](CHANGELOG.md#migration-from-v064-alpha).

If upgrading from `v0.6.3-alpha`, also follow the [consent-policy migration](CHANGELOG.md#migration-from-v063-alpha) introduced in `v0.6.4-alpha`: the resolver contract is nested and the old closed-choice and purpose APIs are withdrawn. Strict fallback still means asking with the choice defaulted off; an explicit grant under it remains valid, subject to the host's admission rules.

This release includes the changes summarized in [CHANGELOG.md](CHANGELOG.md), including the previously unrecorded `v0.6.2-alpha` release. `v0.6.0-alpha` shipped the optional pseudonymous actor key on crash reports — `Event.AnonymousID`, the ONLY field the service keys the actor on, with `Event.SessionID` alongside it as session linkage that is never used as the actor key (both `omitempty`, so a client that does not opt in keeps a byte-identical wire shape) — the dark opt-in experiment-assignment consumer with consent-gated exposure/outcome producers, the dark opt-in remote-config targeting-attribute pass-through, the crash capture opt-ins (self-module debug-id fill and all-goroutine threads), and the opt-in client-side consent floor (`Config.ConsentFloor`), on top of the v0.5.0-alpha remote config client, disk spool, schema-revision declaration and full-jitter retry backoff documented in this README. **Do not pin an earlier tag.** Do not pin `v0.5.0-alpha` or `v0.6.0-alpha`; use `v0.6.1-alpha` or later (`v0.6.1-alpha` has no Go source difference from `v0.6.0-alpha`). Tags at `v0.4.0-alpha` and below predate most of what this README documents. If you need a release without the features listed above, wait for a later one rather than reaching back.

**`v0.1.0` through `v0.1.2` are retracted** in this release's `go.mod` as the superseded pre-alpha line; do not use them as an installation fallback. Use the release pin above after its announcement. Historical tags keep their original Go directives; the Go 1.25 baseline is not applied retroactively to them.

## Quick start (analytics)

`Track` and `Enqueue` refuse SDK-owned and runtime-only event names with
`ErrReservedEventName` (`reserved_event_name`): `experiment_exposure`,
`experiment_outcome`, `governed_action_result`, `app_runtime_ping`,
`monitor_evaluation_recorded`, `llm_usage`, `support_assistant_feedback`,
`ad_impression_revenue`, `session_started` and `session_ended`.
Matching trims ASCII whitespace, removes exactly one leading `app.`, and compares
case-sensitively. Empty or ASCII-whitespace-only names return
`ErrEventNameRequired` (`event_name_required`). Both errors match
`ErrInvalidEvent` through `errors.Is`; each refusal increments `Stats.Dropped`
once, updates `LastError`, and reaches neither the queue nor the transport.
Existing lifecycle and consent refusals take precedence. Name normalization
trims only ASCII whitespace on the wire too; Unicode whitespace is preserved.

Ordinary names such as `app.screen_view`, `purchase` and `economy_tx` remain
available. Use the experiment API for sealed experiment facts. Its internal
producer and the typed purchase/economy verbs retain their existing behavior.
This release adds no typed session or ad-revenue producer; migrate any direct
calls using reserved names before upgrading, without renaming them to bypass
the restriction. Local name admission does not establish server acceptance.

### Per-event schema version (v0.9.0)

This source adds `Event.SchemaVersion *int`; the published `v0.8.1` API predates
this option. Leave it nil to emit integer 1, or select an integer from 1 through
65535 on an ordinary event:

```go
version := 2
err := client.Enqueue(shardpilot.Event{
    Name: "app.screen_view", SchemaVersion: &version,
})
```

`Track` accepts the same field. Intake snapshots the pointed value, and actual
wire-size checks, retries and newly written spool records retain that selection.
Explicit zero, negative or over-65535 values return `ErrSchemaVersionInvalid`,
which matches `ErrInvalidEvent` through `errors.Is`, count one drop and set
`Stats.LastError` to `schema_version_invalid`. Existing lifecycle, consent,
name and ID refusals take precedence. SDK-owned producers keep their versions;
the option does not admit reserved names or change sealed experiment facts.
`Supports("event_schema_version")` reports this support before initialization.
The option is independent of `Config.SchemaRevision` and its declaration header.
The server determines whether the selected contract exists and is active;
rejection never selects another version automatically.

For a bounded analytics-and-crash evidence run, set the environment variables in
[`examples/evidence`](examples/evidence/README.md), then run
`go build -trimpath -o /tmp/shardpilot-go-evidence ./examples/evidence` followed by
`/tmp/shardpilot-go-evidence` without a pipe.

A runnable backend example lives in [`examples/basic`](examples/basic). The minimal flow:

```go
client, err := shardpilot.NewClient(shardpilot.Config{
    IngestURL:     os.Getenv("SHARDPILOT_INGEST_URL"),
    ServiceToken:  os.Getenv("SHARDPILOT_SERVICE_TOKEN"),
    WorkspaceKey:   os.Getenv("SHARDPILOT_WORKSPACE_KEY"),
    AppKey:         os.Getenv("SHARDPILOT_APP_KEY"),
    EnvironmentKey: os.Getenv("SHARDPILOT_ENVIRONMENT_KEY"),
    Source:        shardpilot.SourceBackend,
    AppVersion:    "0.1.0",
})
if err != nil {
    return err
}
defer client.Close(context.Background())

// purchase is a backend-source canonical event: the server-validated,
// real-money purchase reported after receipt/store validation. The typed
// verb builds it for you and refuses a purchase that is missing a required
// field, or a client whose Source is not backend.
err = client.TrackPurchase(context.Background(), shardpilot.Purchase{
    EventID:  "2dfb9dbf-b7fa-5dd1-a311-aa0616c0bd9f", // idempotency key: a retry of the same purchase reuses it
    UserID:   "user-1042",
    Product:  "starter_pack",
    Amount:   9.99,
    Currency: "USD",
    Quantity: 1,
})

// economy_tx is the other backend-lane event: one in-game currency ledger
// transaction, reported by the backend that keeps the ledger. EventID is
// the same kind of idempotency key.
err = client.EnqueueEconomyTx(shardpilot.EconomyTx{
    EventID:      "32e803d2-ee32-55d1-bd93-70e5cff29ed1",
    UserID:       "user-1042",
    Direction:    shardpilot.EconomySink,
    CurrencyType: "gems",
    Reason:       "shop_purchase",
    Amount:       120,
})
```

The purchase again in raw form, for events that have no typed verb yet. It is an alternative to the typed call, not a second call: every `Track` is an event of its own with its own `event_id`, and the facts layer counts each one.

```go
// The canonical schema requires props.amount, props.currency and
// props.product; ID is the same idempotency key.
err = client.Track(context.Background(), shardpilot.Event{
    ID:     "2dfb9dbf-b7fa-5dd1-a311-aa0616c0bd9f",
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

> **`session_id` is required for non-`backend` sources.** The ingest API requires a `session_id` on every event whose `source` is not `backend` (i.e. `SourceClient` / `SourceServer`). This is a backend/service-tier SDK and does **not** manage a session lifecycle — it passes `Event.SessionID` and `Event.SessionSequence` through verbatim. So if you configure a non-`backend` `Source`, you must set `SessionID` (and a monotonic `SessionSequence`) on each event yourself; an event with no `session_id` is rejected (`400`, surfaced as an `*HTTPStatusError` for the whole batch). `SourceBackend` events do not require a session. (The client SDKs — Unity, Unreal, Defold — open and number sessions automatically; this SDK is deliberately pass-through, since a backend rarely has a single per-process session.)

## Quick start (crash reporting)

**Release behavior:** emitted crash bodies carry `fatal: true` for `EmitFatal`
and `fatal: false` for `Emit`; non-fatal reports admitted by the built-in sampler
also carry `non_fatal_sample_one_in` (1 by default). Custom sampler rates remain
unknown and are omitted. The SDK stamps these fields rather than reading them
from caller data, and encodes the admitted body once for byte-identical retries.
The public `crash.Event` is unchanged. The default now keeps every non-fatal report.

A runnable example lives in [`examples/crash`](examples/crash). It demonstrates the client API surface with a synthetic stub event; it does not install a panic handler or capture a real crash.

```go
import "github.com/shardpilot/shardpilot-go/pkg/crash"

client, err := crash.NewClient(crash.ClientOptions{
    CrashURL:  os.Getenv("SHARDPILOT_CRASH_URL"),
    CrashKey:  os.Getenv("SHARDPILOT_CRASH_KEY"),
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
    App:        crash.AppInfo{Key: "app_example", Version: "0.2.0-alpha", BuildID: "synthetic-build"},
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

`EmitFatal` bypasses sampling. With the default options, `Emit` also admits every valid non-fatal report, including the first report from each client. Transport failures still return errors, and `OnResult` reports the server result after a successful request. An explicit custom `Sampler` can opt into sampling; its sampled-out `Emit` returns `nil` without a request or result callback. The built-in keep-all rate is stamped as `non_fatal_sample_one_in: 1`; custom sampler rates remain unknown and omitted.

Crash and module platforms use the same twelve values as analytics: `windows`,
`macos`, `linux`, `android`, `ios`, `tvos`, `web`, `ps4`, `ps5`, `xbox`, `switch`,
and `other`. Host values are trimmed and lowercased; supported aliases such as
`darwin` and `js` become `macos` and `web`. Empty or unmapped crash values become
`other`. An omitted module platform stays omitted and inherits the crash platform
at ingest; an explicit unmapped module value becomes `other`. Automatic capture
maps the Go runtime OS through this vocabulary. The crash runtime mapper accepts
`js` as `web`; analytics retains its existing host-alias behavior (`js` uses
`other`). `OS.Name` remains separate.
An unmapped nonempty host value produces one warning per client, through `Logger`
or the default `slog` logger; raw platform input is neither logged nor retained.
`SanitizeEvent` performs the same normalization without logging. Caller-owned
reports and module slices are unchanged. A custom sampler receives a detached
copy of the prepared report; modifying that copy cannot change the admitted body.

### Automatic panic capture

For Go services you can capture panics automatically instead of building `Event`s by hand. Configure the client with the app identity (and, for a multi-component product, a `CrashComponent` slug), then defer `Recover` at each goroutine / request-handler boundary:

```go
client, err := crash.NewClient(crash.ClientOptions{
    CrashURL:  os.Getenv("SHARDPILOT_CRASH_URL"),
    CrashKey:  os.Getenv("SHARDPILOT_CRASH_KEY"),
    App:       crash.AppInfo{Key: "fortress-fury", Version: "1.4.0"},
    CrashComponent: "main-server", // which component this crash came from
})

func handleRequest(ctx context.Context) {
    defer client.Recover(ctx) // reports the panic as fatal, then RE-PANICS
    // ... work that may panic ...
}
```

`Recover` recovers a panic, reports it synchronously (so the report is sent before the process exits), and then **re-panics** so the program's normal crash behaviour is preserved. Use it once per goroutine — a panic in a bare `go func(){…}()` with no deferred `Recover` is not captured. `CapturePanic(ctx, recovered)` reports an already-recovered value **without** re-panicking, for callers that intentionally recover and keep running. A nil/unconfigured client is a safe no-op (and `Recover` still re-panics).

Captured frames are **pre-symbolicated** from the Go runtime (package-qualified function, file, line — no native modules or addresses, accepted by the crash ingest API). `App` fields and `CrashComponent` are stamped onto every event that doesn't set its own; a per-event value always wins.

Crash bodies name the component as `component`, never `source`. `ClientOptions.CrashComponent` sets the client-wide default. The retained `Event.Source` field is a per-event override and also emits `component`. The analytics trust-tier `source` is separate.

Two **default-off** capture opt-ins extend automatic capture (both dark by default — while off, zero new code paths execute and the captured wire shape is byte-identical; server-side acceptance of these fields is gated on the SDK's client-side consent gate and durable crash spool landing first):

- `DebugIDFillEnabled` attaches the **running binary's identity** as the event's single `modules[]` entry: the executable's base name plus a `debug_id` read from the binary itself — the ELF GNU build-id as lowercase hex (the identity `dump_syms` emits, so the crash joins symbols uploaded under that id), falling back to the lowercase-hex SHA-256 of the Go build id (reproducible from the shipped binary: `printf %s "$(go tool buildid <binary>)" | sha256sum`) when no GNU note is present (the default Go linker emits none). Resolved once at `NewClient` (no file I/O on the panic path); on a non-ELF platform or an unreadable binary the fill is skipped and capture proceeds unchanged. Manual `Emit`/`EmitFatal` events are never touched. The module's `load_address` is the schema-required placeholder `0x0` — captured frames stay function-only, so nothing is ever resolved against it.
- `AllGoroutineCaptureEnabled` snapshots **every goroutine** at panic time (`runtime.Stack` all) and ships the other goroutines as additional pre-symbolicated `threads[]` beside the precise crashed thread, each named by goroutine id with the scheduler state (e.g. `chan receive, 3 minutes`) as the thread name and its leading runtime park frames trimmed. Bounded by the event caps — 64 threads and 256 total frames, in dump order, at most 16 frames per non-crashing goroutine — and everything passes the same PII scrub as every other frame.

## Configuration

`shardpilot.Config` fields:

| Field | Required | Notes |
|---|---|---|
| `IngestURL` | yes | Absolute base URL, no path/query/fragment. HTTPS required outside localhost/loopback (or private nets with `AllowInsecurePrivateNetwork`). |
| `ServiceToken` | server/backend only | Service credential for ingest. Required for `SourceServer` and `SourceBackend`; forbidden for `SourceClient`. Publishable keys and static JWTs are refused here. |
| `RemoteConfigEnabled` | no | Explicit opt-in, default false. Requires `AppURL` and `PublishableKey`. The address alone enables nothing. |
| `WorkspaceKey` / `AppKey` / `EnvironmentKey` | yes | App-first identity (`workspace → app → environment`). |
| `Source` | yes | `SourceClient`, `SourceServer`, or `SourceBackend`. |
| `AppVersion` / `AppBuild` | no | Default envelope metadata. |
| `Platform` | no | Analytics platform: `windows`, `macos`, `linux`, `android`, `ios`, `tvos`, `web`, `ps4`, `ps5`, `xbox`, `switch`, or `other`. Values are trimmed and lowercased; supported aliases such as `darwin` map to their canonical value. Empty or unmapped input becomes `other`. `Event.Platform` overrides this default when nonempty. |
| `UserID` / `AnonymousID` | no | Default actor identity; also select the consent actor. `SourceClient` uses `AnonymousID` as kind `anon` (its `UserID` is self-asserted, never a consent key). `SourceServer`/`SourceBackend` use `UserID` as kind `user_verified`, else `AnonymousID` as kind `anon`. A client source with only `UserID` cannot grant (`ErrInvalidConsentIdentity`); its denial applies locally. |
| `BatchSize` | no | Default 25, capped at 100. |
| `BufferSize` | no | Async queue capacity, default 1000. |
| `FlushInterval` | no | Default 15s (was 1s). How long a **partial** batch waits before publishing — not a heartbeat: an empty batch publishes nothing, so a process that tracks nothing makes no requests at either value. At 1s a service emitting an event every few seconds never filled a batch, so every event became its own request; 15s lets batching actually happen. A full `BatchSize` still publishes immediately and `Flush()` on demand. Retry pacing does not follow this value. Set it explicitly for a different cadence; an explicit value below the default is honoured. |
| `HTTPTimeout` | no | Default 2s. |
| `Logger` | no | `Printf`-style logger; never receives tokens or full payloads. |
| `AllowInsecurePrivateNetwork` | no | Allow plain HTTP to RFC1918 private addresses. |
| `HTTPClient` | no | Optional `*http.Client` behind every request the SDK makes (event batches, consent posts, remote-config fetches) — pooled transports, proxies, mTLS, instrumentation. Nil (default) keeps the SDK's internal clients. Every attempt stays bounded by the sooner of `HTTPTimeout` and the caller's context deadline (an injected client without its own `Timeout` cannot stretch an attempt), and remote-config fetches still refuse redirects (the SDK derives that client with `CheckRedirect` pinned). |
| `PublishableKey` | client ingest or app features | Client credential for `SourceClient` ingest and enabled app features. Never a static JWT. Held in memory and redacted from diagnostics. |
| `ExperimentsEnabled` | no | Default `false` — dark: zero experiment code paths. `true` opts into the experiment-assignment consumer (requires `AppURL`; same publishable `PublishableKey` auth; see "Experiments"). |
| `AppURL` | enabled app features | App origin for configuration and experiments, separate from `IngestURL`. The address alone enables neither feature. Validated like `IngestURL`. |
| `RemoteConfigAttributesEnabled` | no | Default `false`: requests carry no attributes; the client-ID path is consent-gated independently. `true` opts into the targeting-attribute pass-through: attributes set via `SetRemoteConfigAttributes` ride the config GET as query parameters so server-side delivery rules can target this client — but **only while consent is granted** (unknown/denied fetch identifier-free and attribute-less; see "Remote config"). Requires `RemoteConfigEnabled`, `AppURL`, and `PublishableKey`. |
| `RemoteConfigCachePath` | no | File for the durable last-known-good remote-config cache record (file `0600`; directories the SDK creates are `0700`, and a pre-existing parent's permissions are never changed — the cache lives in a caller-chosen, possibly shared directory. The record is read back through a hard size limit; an over-limit file is discarded as corrupt, never loaded whole). Empty keeps the cache in-memory only. Independent of `SpoolDir`; never enables consent persistence. |
| `SpoolDir` | no | Opt-in state directory for the bounded disk spool and the persisted consent decision (`spool.json`, `consent.json`, `spool-wipe-owed`; dir `0700`, files `0600` — a pre-existing directory with looser permissions is tightened to `0700` at the first write, and a refused tighten is a persist failure: nothing is written through a directory whose privacy could not be established). Startup reads `spool.json` through a hard limit derived from `SpoolMaxBytes` (an over-limit file is discarded as corrupt, never loaded whole). Empty (default) keeps the queue memory-only — nothing is ever written to disk. **One client per `SpoolDir` is the supported topology**: every spool save reloads and merges the on-disk record by `event_id` as a safety net (last-writer-wins over the merged view; a record a sibling acked concurrently can be resurrected and resent — at-least-once, server-deduped), not a concurrency feature; sharing shows up in `Stats.SpoolForeignMerged`. |
| `ConsentFloor` | no | Opt-in client-side consent floor (`&ConsentFloorConfig{}`): consent-first gating (`ErrConsentUnknown` until an explicit decision), a durable consent-receipt outbox under `SpoolDir` (32-cap FIFO, retried until acknowledged, decision-order delivery), the grant-receipt dispatch gate, forced-minor denial semantics, and `Close`'s `ErrConsentPending` durability backstop. Nil (default) keeps the documented server-side posture unchanged. See "Privacy & consent". |
| `SpoolMaxEvents` | no | Spool count cap, default 2000; the oldest events are dropped past it (via `OnSpoolDeadLetter`). |
| `SpoolMaxBytes` | no | Spool byte cap over serialized envelopes, default 1 MiB (1,048,576), same oldest-first eviction. |
| `OnSpoolDeadLetter` | no | `func(SpoolDeadLetter)` called with every event the spool drops undelivered (capacity, local intake/retry expiry, terminal outcome, consent purge/refusal). A capacity eviction reports only once the rewrite that removed it durably lands — an eviction a failed rewrite left in the on-disk record is not final yet (a restart would reload and resend it), so its callback defers until it is. Panic-recovered like `OnBatchResult`. |
| `DisableRequestCompression` | no | Stops gzip-compressing request bodies. Default `false` — the client **compresses**: a batch body is the same envelope keys repeated per event, so it comes down to a few percent of its size (measured: a 100-event batch, 41.7 KB -> 2.4 KB on the wire, **17x**). Bodies under 1 KiB are sent uncompressed — gzip's 18 bytes of framing make a single-event batch bigger, not smaller — as is any body compression fails to shrink. Cost is ~36 us and 2 allocations per batch. **Where that cost lands depends on which API you call**: an `Enqueue`d batch is published by the background worker, so the caller pays nothing; a synchronous `Track` publishes on the CALLER's goroutine, so a single event whose JSON exceeds the 1 KiB threshold — a large props payload — pays the compression there. Most `Track` events are a few hundred bytes and never cross it. If the server cannot read the coding it says so with a distinct detail code and the client falls back to uncompressed **for the rest of the process** after one rejected request, re-sending the same batch, so pointing a new SDK at an older ingest deployment costs one round-trip rather than dropping data. Set this to skip even that. |
| `SchemaRevision` | no | Opt-in schema-set revision for a writer released with its schema. Empty by default: no `X-ShardPilot-Schema-Revision` header. An explicit value is sent on `events:batch` only. |
| `RejectionCapacity` | no | Rejection-history entry cap; default 64 for non-positive values, oldest evicted first. See "Batch verdicts". |
| `OnBatchResult` | no | `func(BatchResult)` called after each successful batch publish with the server's per-event outcomes. Runs on the publish path (may be called concurrently); keep it fast and non-blocking. A panic inside it is recovered. |

Analytics retention uses SDK intake time, independently of `Event.Timestamp`.
An offline event can remain queued for up to seven days. DNS, connection and
pre-send TLS failures proven by the HTTP transport's trace do not start the
retry-deduplication clock. An injected transport that provides no trace leaves
delivery ambiguous, even if its returned error names DNS or dialing. A request
that may have been accepted without a definitive response (including a lost
response or server failure) starts a separate **15-minute** retry window.
Within that window retries preserve the original ID and envelope bytes; once
it expires the SDK drops the event and reports `SpoolExpired` and an `expired`
dead letter. A later connection failure or backpressure response cannot reset
that clock. This is best-effort deduplication, not an exactly-once guarantee.

Local clocks persist beside the unchanged envelope. Records without valid
clock metadata, including older spool records, expire rather than receiving a
new retry window. A backward wall-clock change that puts a stored clock in the
future also expires the entry. Before resending a stored event, the SDK must
persist its tentative attempt clock; a failed write prevents that attempt and
increments `SpoolPersistFailed`. Definite non-delivery clears only that new
clock. If clearing it cannot be persisted, the conservative clock remains on
disk until the write succeeds. An abrupt exit during an attempt is ambiguous.

The example programs read these from `SHARDPILOT_*` environment variables; the SDK itself reads no environment variables.

Crash client (`crash.ClientOptions`): `CrashURL` (crash ingest base URL), `CrashKey` (needs `crash:write`), plus optional `App` (`AppInfo{Key,Version,BuildID}` — defaulted onto every event; **required for automatic panic capture**, and `App.Key` must equal the API key's app scope), `CrashComponent` (component slug), `AnonymousID`/`SessionID` (optional actor keys — see "Actor identity on crash reports"), `DebugIDFillEnabled` (default `false` — dark; self-module debug-id fill, see "Automatic panic capture"), `AllGoroutineCaptureEnabled` (default `false` — dark; all-goroutine threads at panic time, same section), `HTTPClient`, `Logger`, `Sampler`, `MaxAttempts` (default 2), `RetryBaseDelay` (default 50ms). Default HTTP timeout is 30s.

### Actor identity on crash reports

`Event.AnonymousID` and `Event.SessionID` are optional pseudonymous keys that say *who* a crash happened to. Both are omitted from the payload entirely when unset, so a client that ignores them sends exactly the bytes it sent before.

Setting `AnonymousID` is what makes a crash:

- **countable** — a crash can be measured against an active-user denominator (the "share of players who did not crash" reliability metric), which is impossible when reports carry no actor key;
- **consent-aware** — the service can honour a diagnostics opt-out for that subject;
- **erasable** — the occurrence becomes reachable by a per-subject deletion request.

The service hashes the value server-side with a keyed digest; the raw id is not stored on the occurrence. Use the same anonymous id your analytics client uses, or the ids will not join.

A raw **account** id is deliberately not part of this struct. Crash ingest authenticates with an API key, so there is no verified bound subject: any account id the client asserts is unverified, and the service never keys the actor on it.

`ClientOptions.AnonymousID`/`SessionID` set a client-wide default stamped on every event that does not set its own (per-event wins, like `CrashComponent`). **There is no default and no host-derived fallback**, and for most Go services you should leave the client-wide value unset: one backend process serves many players, so a process-wide identity would attribute every server crash to a single synthetic actor. Set the per-event field from the request being served instead — or set the client-wide value only where the process genuinely represents one subject (a CLI tool, an agent, a single-tenant job).

A malformed value (free-form text, an email, an IP, a JWT, a raw `user_`/`player_`/`device_`-prefixed id, or anything over 512 bytes) **drops the field, never the report** — a crash is worth more than a correlation key.

## Wire contract

App-first event envelope (`POST {IngestURL}/v1/events:batch`, `Authorization: Bearer <token>`). Ordinary event envelopes carry `event_id`, `schema_version`, `event_name`, `source`, `event_ts`, `workspace_key`, `app_key`, `environment_key`, and optional `user_id`, `anonymous_id`, `session_id`, `session_sequence`, `platform`, `app_version`, `app_build`, `context`, `props`.

Every request the SDK makes (event batches, consent, remote config, the experiment assignment and apply routes, and crash ingest) carries `X-ShardPilot-SDK: go/<version>`, for example `go/0.10.0`. The version is the release the SDK was cut from, held as a constant in code; it never comes from configuration.

SDK-owned experiment exposure and outcome facts use schema major 2 and the literal `workspace_key`, `app_key`, and `environment_key` returned by the platform. The SDK forwards the signed fact values and attestation seal without renaming them to `*_id`. Existing spooled bytes remain unchanged; previously queued major-1 experiment facts are not upgraded or re-signed.

Caller `Event.ID` values (including `Purchase.EventID` and `EconomyTx.EventID`) must be UUIDs: 36 hyphenated hexadecimal characters, version 4, 5 or 7 and RFC variant `8`, `9`, `a` or `b`. Leading/trailing whitespace is trimmed and letters are sent in lowercase. Empty IDs receive a UUIDv7 once at intake, retained across retries. Other values are refused locally with `ErrInvalidEventID` (`invalid_event_id`, also matching `ErrInvalidEvent`); one drop and the diagnostic are recorded without queueing or sending a batch. Map receipt or ledger identifiers to stable UUIDs before supplying them; repeat the same UUID for a retry.

An event whose wire JSON, with surrounding whitespace trimmed, is longer than 2048 bytes is refused locally with `ErrEventTooLarge` (`event_too_large`): the ingest per-event cap, which would reject the same element. An event of exactly 2048 bytes is admitted. `Track`, `Enqueue` and the SDK's own experiment facts encode the event once at intake and measure those bytes; the batch sends, spools and retries exactly those bytes, so a nested value changed after the call never reaches the wire. A refused event records one drop and the diagnostic and is never queued or sent; an oversize experiment fact is counted instead under `event_too_large` in its kind's experiment drop map (below) and leaves the owed queue.

Request bodies over 1 KiB are sent
**gzip-compressed** with `Content-Encoding: gzip` (batch publishes and consent writes alike). The server's body limit (default 8 MiB) applies to the **uncompressed** body, so compression buys throughput and not headroom — `BatchSize` still governs how large a body gets. A deployment that cannot read the coding answers `400` with detail code `unsupported_content_encoding` (or `invalid_content_encoding` if the stream arrived unreadable); the client latches compression off for the process and re-sends the same batch uncompressed, so an SDK upgrade never outruns a server upgrade. `Config.DisableRequestCompression` opts out entirely.

The envelope is **universal** — no domain-specific fields. Vertical context (e.g. `match_id`) goes in `Props` and serializes under `props`. **Banned legacy fields** never appear in SDK source or on the wire: `project_id`, `game_id`, `env`, `event_ts_server`, `event_seq_session`, top-level `build_version`. Use `app_version` / `app_build` for version metadata.

The `202` batch response carries four **disjoint** aggregate counters — `accepted` / `rejected` / `duplicates` / `suppressed` — plus an `events[]` list with one entry per event (`event_id`, `status`, optional `code` / `message`). Every event is counted in exactly one aggregate: `rejected` counts hard rejects only (tracking-plan and per-event schema verdicts), a benign `event_id` re-send counts only under `duplicates`, and consent suppressions only under `suppressed` — so `rejected > 0` always means events were dropped for a contract reason. `status` is one of `accepted`, `duplicate`, `suppressed_no_consent`, `suppressed_ad_revenue_consent`, or `rejected` — for suppressed events the `202` is **not** delivery confirmation. (`BatchResult` surfaces the `Accepted`/`Rejected`/`Duplicates` aggregates; consent suppressions are visible per event through the `events[]` list.) The `accepted` / `rejected` / `duplicates` aggregates fold into `Snapshot()` — the top-level `suppressed` aggregate is not decoded, so consent suppression shows up in `Snapshot()` only through the per-status breakdown in `Stats.ByStatus` (derived from `events[]`) — and the per-event list is surfaced through the optional `Config.OnBatchResult func(BatchResult)` callback — the complete per-event surface for rejected, suppressed, unknown, or duplicate outcomes. The `Rejections()` accessor also retains rejected outcomes by default. The callback runs on the publish path (the background flush worker and synchronous `Track` publishes share it, so it may be called concurrently); keep it fast and non-blocking, and a panic inside it is recovered so a buggy callback cannot stop delivery.

Non-2xx responses carry an error envelope `{"error":{"code","message","details":[{"field","code","message"}]}}` (codes such as `validation_error`, `unauthorized`, `forbidden`, `rate_limited`, `internal_error`; detail codes such as `events_rate_limited` or `monthly_quota_exceeded`). The SDK parses it into the returned `*HTTPStatusError` — `ErrorCode`, `ErrorMessage`, `Details`, and the `Retry-After` header as `RetryAfter` (both standard forms, delta-seconds and HTTP-date, like the crash client; both clients clamp the server hint at 24h) — so logs and callers see the real cause. After a rate-limited automatic publish, the background worker holds further automatic attempts until the `Retry-After` deadline passes and retries AT that deadline (a dedicated wake — not the next flush tick, which can be much later when `FlushInterval` exceeds the hint); explicit `Flush` (and `Close`) still attempt immediately because they carry caller intent, and a renewed failure re-arms the deferral. A retryable failure **without** a `Retry-After` hint paces itself on its OWN clock, independent of `FlushInterval`: every failure — the first included — defers by a full-jitter random wait in [1s, ceiling], the ceiling doubling from the third consecutive failure up to 60s — so an outage is probed with growing, de-synchronized spacing instead of a fixed-cadence retry storm — and a successful publish resets the schedule. A success that ends a deferral or failure streak (typically a synchronous `Track` while the worker waits) also wakes the worker to retry its pending retryable work immediately — the held batch and any requeued spooled chunks alike — instead of leaving it for the next flush tick.

With the disk spool enabled, terminal experiment withdrawal clears a restored
retry deadline once no restored resend work remains, including when ordinary
events entered the worker batch before withdrawal. Pacing from a current-launch
publish failure still protects a held batch; remaining restored work keeps its
deadline too.

Consent decisions ride their own endpoint (`POST {IngestURL}/v1/consent`), never the event envelope, with body `workspace_key`, `app_key`, `environment_key`, `actor_identifier`, `kind` (`anon` or `user_verified`, from the consent actor rule above), `categories` (`{"analytics": <bool>}`), `decided_at` (RFC3339), and a fresh UUIDv7 `idempotency_key`.

Remote configuration is fetched from `GET {AppURL}/config/v1/{workspace_id}/{environment_id}`, appending `/{client_id}` only while consent is granted (`Authorization: Bearer <PublishableKey>`, each path segment percent-escaped, `If-None-Match` revalidation when a cached ETag exists; no body, and never the schema-revision header — that is a batch-route contract). By default the request carries **no query parameters**; under the dark `RemoteConfigAttributesEnabled` opt-in — and only while consent is granted — the targeting attributes set via `SetRemoteConfigAttributes` ride as percent-escaped query parameters in the experiment attribute vocabulary (allowlist + `custom_attribute_<name>`, ≤512-byte values, 64-attribute cap, sorted; out-of-vocabulary keys dropped client-side, never sent).

Crash reports go to `POST {base}/api/v1/crashes/ingest` (`Authorization: Bearer <api-key-with-crash:write>`) with a stable `crash_id`, `occurred_at`, app.key/platform/os, device & context maps, exception metadata, binary modules with `debug_id`/`load_address`, per-thread raw instruction addresses, optional pre-symbolicated frames, optional `raw_text`, and breadcrumbs. The crash structs are a **hand-maintained mirror** of the ShardPilot crash ingest API's OpenAPI schema.

The ingest response is surfaced through the optional `ClientOptions.OnResult func(Result)` callback (fired on both manual `Emit`/`EmitFatal` and the auto-capture path, on the calling goroutine): `Result` carries the server-assigned `CrashID`/`Fingerprint`, a `Suppressed` flag (the crash was accepted but **not stored** because the actor withheld consent — the HTTP status is still `2xx`), and any `Warnings`. Suppression and warnings are also logged. When a `429`/`5xx`, or a `400`/`404` with exact error code `kind_not_enabled`, carries a usable `Retry-After` header (delta-seconds or HTTP-date), the retry loop waits that long, capped at 24 hours. Otherwise it uses full jitter over an exponentially growing window starting at `RetryBaseDelay`, capped at one minute; the default base is 50ms. Caller cancellation interrupts a wait.

Batch, consent, and crash transports refuse redirects, including when an HTTP client is injected. A `408`, `429`, or `5xx` is retryable. Crash ingest also retries `400`/`404` with the exact structured `error.code` value `kind_not_enabled`, preserving the encoded report, ID and clocks within the existing `MaxAttempts` and caller-context bounds. Exhaustion returns the failure; no report is retained after the call. A bare 404, another code, a malformed envelope or the code appearing only in human message text stays terminal. Other non-2xx statuses are terminal. Automatic consent receipt retries belong to the opt-in `ConsentFloor` outbox; ordinary consent sends once. `Retry-After` is used on `429` and `5xx`, plus the crash-only disabled-kind exception. Error envelopes are read within a fixed bound; an incomplete or oversized body cannot supply a server code.

Every 2xx settles a batch, consent receipt, or crash, even if its body cannot be parsed. Empty, malformed, truncated, and oversized success bodies emit a diagnostic and increment `Stats.UnparsedBatchResponses`, `Stats.UnparsedConsentResponses`, or `crash.Client.UnparsedResponses()`. Partially decoded outcomes are discarded, so settlement alone does not establish accepted event counts or a stored crash. Unreadable spool successes leave the durable backlog without incrementing `Stats.SpoolResent`. Unknown object members are ignored. Unknown batch status strings are retained for diagnostics and never cause a resend; the retired `EventStatusObserved` constant is removed.

## Batch verdicts

A `202` batch response is a transport completion, not confirmation that every event was accepted. The SDK retains each rejected event's ID, status, code, and message in an in-memory rejection ring (default 64 entries, configurable, oldest evicted first), while the rejection counter remains cumulative. A configured logger or observer handles diagnostics; otherwise the default channel warns for the first ten rejected events, then once per previously unseen code, retaining at most 64 code keys to bound both memory and session output. Logging limits never suppress the ring. Terminal rejections leave the retry spool and are not resent; accepted siblings settle once. Where a flush result is available, inspect its rejected count and retained entries even when transport completion succeeded. Use the public ring accessor for automatic publishes too. The ring is diagnostic history for this client instance, not durable storage or a retry queue; restarting the client clears it.

`client.Rejections()`
returns oldest-first copies of `BatchEventStatus` values: `EventID`, `Status`,
`Code`, and `Message`. `Config.RejectionCapacity` uses 64 for non-positive
values. Reads are safe during concurrent publishes and inside diagnostics
hooks. `Close` leaves this client's history available for inspection; a new
client starts with an empty ring.

`Track` and `Flush` keep returning a nil error for a parsed `202`, including
partial rejection. Inspect `Rejections()`, cumulative `Snapshot().Rejected`,
and `Snapshot().LastError`; the last diagnostic identifies a rejected event
without incrementing `FailedBatches`. Later accepted responses do not erase
that history. `BatchResult` already carries its rejected count and full
per-event entries to `OnBatchResult`; Go does not add a flush-result object.

A configured `OnBatchResult` owns diagnostics. Otherwise, a configured
`Logger` receives every rejection; when both are unset, the standard Go logger
emits bounded warnings. Rejections and counters are recorded before spool
settlement can call `OnSpoolDeadLetter`, so that hook can read the current
batch even if it blocks or panics. Rejection warnings and `OnBatchResult`
run after settlement finishes.
Diagnostic messages redact configured `ServiceToken` and `PublishableKey` values while the
ring preserves the server's original fields. Logger and observer panics do
not turn a settled response into a failed publish. `OnSpoolDeadLetter` remains
optional and keeps its existing terminal-settlement behavior. Other per-event
statuses continue through `OnBatchResult` and `Snapshot().ByStatus`.

See the [ingest batch contract](https://docs.shardpilot.com/api/ingest/#batch-verdicts).

## Remote config

Configure `RemoteConfigEnabled: true`, `AppURL` + `PublishableKey` (and usually `RemoteConfigCachePath` + a persisted `AnonymousID` — the anonymous ID supplies `client_id` only while consent is granted; configuring it is still required, otherwise a fetch fails `client_id_unavailable`), then call `FetchRemoteConfig(ctx)` whenever your service wants fresh configuration:

- **Explicit-fetch-only.** The SDK never refreshes on its own — no polling, no `Cache-Control` interpretation, no experiment assignment or client-side rule evaluation. Every fetch is one `GET`, revalidated with the cached ETag. Fetches are fenced by the client lifecycle exactly like synchronous `Track` publishes: a fetch that begins after `Close` returns `ErrClosed`, and `Close` waits (bounded by its context) for in-flight fetches, so no fetch I/O or cache write outlives it.
- **Durable last-known-good cache**, one record keyed by the (workspace, environment, client_id, base URL) tuple. A record written for any other scope is a miss (never served, its ETag never sent) and is overwritten by the next successful fetch; a corrupt record is a clean start. The identifier-free request uses an empty client ID in its scope; identified and anonymous requests never reuse each other's record or ETag. No TTL: a matching record survives restarts and serves offline. Startup preloads only the scope allowed by restored consent. A response retains its dispatch scope even if consent changes while it is in flight. **v0.10.0:** it may install only while that scope is still current and its response wins the sequence fence.
- **Failure taxonomy:** offline, `408`, `429`, `5xx`, and malformed 2xx read bodies serve usable cached values with `FromCache=true` and a reason. Other non-success statuses close their request scope. **v0.10.0:** an explicit refetch does not reopen a closed scope through a transient cache fallback or a fenced response; only an installed valid response (fresh or 304-revalidated), or a new client/session, can reopen it. Getters for a closed current scope return defaults even after a success in the other consent scope. The cache record is retained except for exact `404` code `remote_config_not_published`, which clears the publication in memory and durably (best effort; a failed clear is diagnosed). A different scope or a durable record another process advanced during the fetch is preserved. Redirects are never followed. Caller cancellation returns the context error without a cache fallback or state changes.
- **Typed getters** (`RemoteConfigValue/String/Number/Bool/Values/Version`) read the in-memory snapshot only: never the network, never an error — the caller's default on a missing key *and* on a type mismatch (a stored `false` is served as `false`).
- **Available in every consent state.** `ConsentGranted` uses `GET /config/v1/{workspace}/{environment}/{client_id}`. Unknown, denied, and forced-minor denied use `GET /config/v1/{workspace}/{environment}`, with no client ID, host user ID, or targeting attributes in the request. Publishable-key authorization remains present. Denial does not erase the getter snapshot; the current request scope must still be open to serve it. `RemoteConfigCachePath` works without `SpoolDir`.
- **Server-side targeting attributes (dark, opt-in).** `RemoteConfigAttributesEnabled: true` (requires `RemoteConfigEnabled`, `AppURL`, and `PublishableKey`) plus `SetRemoteConfigAttributes(map[string]string)` makes fetches carry targeting attributes as query parameters so server-side delivery rules can target this client; `nil`/empty clears the set, and the setter is inert while the opt-in is off. The vocabulary and bounds are the experiment consumer's, verbatim (`geo`, `app_version`, `device_type`, `install_date`, `user_segment`, plus `custom_attribute_<name>`; out-of-vocabulary names and unusable values are dropped client-side, never sent; ≤512-byte values, 64-attribute cap, sorted). **Attributes ride ONLY while consent is granted**: under unknown consent or either denied state (forced-minor included) the fetch still happens — config delivery stays consent-neutral — but identifier-free and attribute-less, serving whatever the server publishes for an untargeted client (typically the defaults). That granted-only gate is deliberately *stricter* than this SDK's open-under-unknown analytics posture: attributes describe the actor, so "unknown = zero bytes of personal data" holds on this leg even on the Go SDK. Targeting stays 100% server-evaluated (the SDK never evaluates rules). Last-known-good caching stays scope-keyed for value serving — a cached body may reflect the previously sent attribute set until the next successful fetch (documented v1 limit) — while the cached ETag revalidates only a fetch carrying the **same** attribute signature: any signature change (opting in, changing the set, a consent downgrade) forces a full fetch instead of risking a false 304 under a shared validator. The signature is an in-memory equality token only — nothing attribute-derived is ever persisted, and an attributed record's ETag is not persisted either (a reloaded record could not prove a same-signature fetch), so the first fetch after a restart of a previously-attributed record is a full fetch on every leg. Under the opt-in `ConsentFloor`, the grant-receipt dispatch gate holds this leg exactly like the event legs: while an analytics-grant receipt is retained undispatched, fetches stay attribute-less, so attributes can never overtake the grant on the wire on a strict-consent workspace.
- **Retry pacing (v0.10.0 policy change):** only a positive `Retry-After` on `429` or `5xx` arms an in-memory window for that request scope. Both delta-seconds and HTTP-date are accepted, rounded up and capped at 24 hours. Missing, malformed, zero, negative or past-date hints add no delay, and there is no local backoff or one-second floor. Explicit fetches inside that scope's window return eligible cache and the original reason without a request; a closed scope still returns an error. Identified and identifier-free windows are independent, monotone until expiry, never persisted, and extended only by responses that win the current-scope sequence gate. `408` remains transient without arming a window. This changes the policy shipped in `v0.9.0`.

## Experiments (dark, opt-in)

`ExperimentsEnabled: true` opts this client into the experiment-assignment consumer. **The default is off — dark**: zero experiment code paths execute (no subject-id mint, no fetch, no revalidation goroutine, no fact emission, no new persistence keys), and the surface refuses `ErrExperimentsNotConfigured`. The flag requires `AppURL` (the assignment endpoint lives on the same host, path-swapped to `/api/cp/v1/runtime/experiments/assignment`) and authenticates with the same publishable `PublishableKey`.

- **Dark phase, stated plainly:** turning the flag on does not turn the feature on. Server-side enablement is separate and is not in this SDK's hands, so until it is done an enabled consumer receives `403` on every fetch — the expected fail-closed posture — and a publishable key without the `experiment_assignment_read` scope receives `401`. Exposures are dark end-to-end too: until the platform enables its exposure apply endpoint, an application cannot be sealed, stays owed within the bounded queue, and survives restart when its local application is durably retained under `SpoolDir`; `Close` counts applications without a durable replay source (`Stats.ExperimentExposureDrops["unsealed_at_close"]`); outcomes are rejected per event for publishable client keys until the lane is enabled.
- **Fetching:** `FetchExperimentAssignment(ctx, experimentKey, attributes)` issues one GET and returns the parsed verdict — assigned (variant key + payload + version + boundary), or a not-assigned verdict distinguished by `Reason`: absent = traffic-gate miss, `kill_switch` = operator kill, `targeting_unmatched` = targeting miss, `age_ineligible` = no eligible adult declaration. All four drop the cached assignment from memory and durable storage; an age refusal is authoritative, not a malformed response, and does not schedule a retry. The other three keep what the subject already applied: its owed exposures and outcomes are still sealed and delivered. An age refusal withdraws them instead — for that experiment and subject only: the owed exposures and outcomes (counted `age_ineligible`), their facts already queued or spooled, and the application a new outcome would follow, so `TrackExperimentOutcome` refuses outcomes for the experiment (`ErrExperimentNoAssignment` while nothing is served) until a new assignment is applied. A fact already on the wire when the refusal lands may have been delivered; it is not re-sent. The refusal also reaches the subject's facts built under an earlier version's subject fact key, or under a key the apply endpoint minted: the durable record keeps those keys (`fact_key_history`) while such a fact is owed, queued or spooled. Unknown reasons map to `unknown` and withdraw the cached assignment. Unknown assignment units also become not-assigned with reason `unknown`, so no exposure is recorded. A `subject_fact_key` is validated only for `client_id`; a synthetic assignment ignores it. Targeting attributes are `geo`, `app_version`, `device_type`, `install_date`, `user_segment`, and `custom_attribute_<name>`; experiment fetches also accept `age_band`. Other names and unusable targeting values are dropped. Targeting is evaluated by the server.
- **Age declaration:** prefer `FetchExperimentAssignmentWithAgeBand(ctx, key, band, attributes)` with `ExperimentAgeBandUnknown`, `ExperimentAgeBandUnderThreshold`, or `ExperimentAgeBandAdult` (wire values `unknown`, `under_threshold`, `adult`). This is the game's declaration, not age verification, and never bypasses consent or a forced-minor restriction. The method copies `attributes` and sends canonical `age_band`; any other band, including the zero value, or a conflicting `attributes["age_band"]` returns `ErrInvalidExperimentAgeBand` before sending. The legacy fetch invents no age band: an omitted declaration is ineligible for client-id experiments. The alias `custom_attribute_age_band` is also a declaration: **every present spelling must be exactly `adult`** for admission. Raw map declarations retain blanks, case and whitespace; values over 512 bytes become `unknown`. Both age spellings take priority within the 64-attribute cap, so an adult alias cannot override a non-adult declaration. Successful assignments retain their declarations for revalidation within the current process. **Every launch requires a fresh host fetch before a restored client-id assignment serves or its sealed facts replay.** A restored adult declaration is not a current decision: the previous process may have stopped during a later non-adult fetch, including when its state could not be written. This startup quarantine does not depend on a pending marker reaching disk. A successful assignment or authoritative not-assigned answer decides the experiment; an age refusal withdraws its facts, while a kill-switch answer keeps them. Synthetic assignments and ordinary analytics are unaffected. Changing age requires a new explicit fetch; it does not itself change consent.

```go
// Use the age declaration supplied by your game's age flow.
result, err := client.FetchExperimentAssignmentWithAgeBand(
    ctx, "welcome-layout", shardpilot.ExperimentAgeBandUnknown,
    map[string]string{"app_version": "1.2.0"},
)
if err != nil {
    return err
}
if !result.Assigned && result.Reason == "age_ineligible" {
    // Keep the default experience. Do not retry an authoritative refusal.
}
```

- **Failure taxonomy:** after the current launch’s host decision, offline, `408`, `429`, `5xx`, and malformed 2xx read bodies serve stale assignments and pace automatic revalidation; explicit fetches remain one GET. Other statuses stop serving the affected assignment. `401`/`403` latch the surface until an authorized explicit fetch or re-init. Stable error codes distinguish feature-disabled responses from credential faults; `real_subjects_disabled` also withdraws all cached assignments and owed facts. Only `subject_id_invalid` and `subject_pii_marker` on `400` re-mint the SDK-managed subject once, then retry with the same attributes. Error message text never drives these decisions.
- **Subject id (`spcid_`):** SDK-minted and SDK-managed — `spcid_` + 32 lowercase hex (a UUIDv7, dashes stripped). The client's own subject has no host override path: no setter, no config field. A process that serves many players uses `ExperimentSubject` handles instead (below). With `SpoolDir` it persists (`experiment_subject_key`, 0600) and sticks across restarts (any wire-valid stored id is kept verbatim — re-minting re-buckets); without `SpoolDir` it is per-process, documented ephemeral. It egresses ONLY as the `subject_key` of the assignment fetch and of the exposure and outcome apply requests — all to the assignment host, all consent-gated — never in analytics events or props.
- **One client per `SpoolDir`:** the experiments state lives in `SpoolDir` (`experiment_subject_key`, `experiments.json`, `experiments.condemned`) next to the spool, and one client coordinates it within itself only. Two clients on the same `SpoolDir`, in one process or in two, share the files but not each other's memory. They start from the same subject, but a subject-grammar `400` re-mints it in the client that received it only, while the other keeps the subject it read. An age refusal on one of them withdraws nothing the other holds in memory: the other keeps serving the experiment, and its owed and queued facts stay. One client's `fact_key_history` keeps a key only while a fact of its own carries it, so it can drop a key the other client's facts still need. A `SpoolDir` must belong to one client, across every process that opens it.
- **Serving:** `ExperimentVariant(key)` / `ExperimentVariantPayload(key)` read the in-memory assignment (restored client-id last-known-good with `SpoolDir` is available only after the post-launch host decision); they never touch the network, **record nothing**, and serve zero values while nothing is cached, while `401`/`403` latched, or while consent refuses. A pending non-adult age answer also holds the assignment and its sealed facts. `Flush` reports an error while experiment facts remain quarantined; supply the host’s current declaration through a new fetch to resolve them.
- **Applying:** `ApplyExperimentVariant(key)` returns the same variant and a copy of its payload under the same rules **and records the exposure**; both come from the entry it records, so use this payload rather than a separate `ExperimentVariantPayload` read, which a revalidation could answer from a newer version. Call it at the point your code acts on the variant. It is the only call that records one: a fetch, a revalidation and the getters create no application; a restart replays only applications the host already recorded. The SDK cannot check that you acted on the variant; the call is your statement that you did. **Recording is not synchronous:** the call never touches the network; the background lane has the platform seal the application and delivers the sealed fact, within about a second.
- **Consent (granted-only plane):** the consumer consumes the SAME effective consent state the analytics path uses — denial (either flavor, forced-minor included) closes the whole plane (no fetches, no revalidation, no serving, no facts; a forced-minor session produces ZERO experiment traffic). Under the opt-in `ConsentFloor`, unknown consent closes it too; floor-off keeps this SDK's documented open-under-unknown posture on this plane as well. A consent downgrade retains the durable cache unserved; a re-grant serves it again subject to the current launch’s host decision. Ordinary remote config remains available through its identifier-free route without granted consent.
- **Revalidation (the kill-switch reach):** after the current launch’s host decision, while assignments are cached, a background lane re-fetches each one every 300s ± 10% jitter, so a running client converges on an operator kill or a republish within one interval — an offline client keeps its variant indefinitely (the documented kill-latency caveat). Transient failures back off (transport parity), a `Retry-After` parks the cadence (clamped to one day), and the lane never blocks `Close`.
- **Exposure/outcome facts:** ride the normal analytics pipeline (queue → batch → spool → consent gates). **An exposure takes one hop first:** the lane sends the application — the assignment's version, variant and serving state, the subject, the fetch's attributes, a random `exposure_id` and `applied_at`; no identity and no session — to the platform's exposure apply endpoint on the assignment host, with the same `PublishableKey`. The platform checks it against the assignment it served, as of the state it served it from, and answers with a **sealed fact**, which the SDK posts verbatim (id, event time, scope and props exactly as sealed) with its `attestation_seal`, under its own envelope identity: `source` `client`, `user_id` omitted, `anonymous_id` the configured client identity (an application on a client without an `AnonymousID` is not recordable). The subject never enters an analytics event. One exposure is recorded per (experiment, version, subject) per client instance, when the host applies the variant with `ApplyExperimentVariant(key)`; `TrackExperimentExposure(key)` records an extra application with its own `exposure_id` (and, like `ApplyExperimentVariant`, is delivered by the lane). A retried or re-armed application re-sends its `exposure_id`, so the platform derives the same fact id and a duplicate collapses server-side. Responses from the apply endpoint: any 2xx settles only when a valid sealed fact parses, including nonblank string workspace, app and environment scope keys; otherwise the application remains owed and backs off. A well-formed fact in another scope is dropped as `foreign_scope`. Exact `403` code `experiment_apply_disabled` keeps applications until an authorized fetch; `real_subjects_disabled` withdraws cached assignments and owed facts. A `409` `not_assigned` with a `details` entry whose `field` is `reason` and `code` is `age_ineligible` withdraws that subject's assignment and facts. The conflict codes `version_mismatch`, `variant_mismatch`, and `unit_not_recordable` drop the application and require a newer assignment fetch, even if its cache entry is gone. Other `3xx`/`4xx` except `408`/`429` drop it. No response, `408`, `429`, and `5xx` retain it with backoff. The apply request carries the subject, so it is gated by consent like an assignment fetch: a denial refuses it before the wire or aborts it in flight. Owed applications live in a bounded per-experiment queue, and the lane sends at most 8 of them per cycle and rotates its starting experiment across the pending keys after spending that budget, after any revalidation that is due; `Close` makes one pass over what is still unsealed and stops at the first refusal that keeps an application owed. Every application the SDK drops before it reaches the analytics queue is counted in `Stats.ExperimentExposureDrops` by reason (once queued, a fact a consent denial clears is counted in `Stats.Dropped`; an extra exposure or outcome also increments its kind's `consent_withdrawn` counter exactly once across its owed, queue, worker and durable copies): `not_recordable`, `owed_bound_exceeded`, `apply_refused`, `real_subjects_disabled`, `foreign_scope`, `unsealed_at_close`, `undelivered_at_close`, `consent_withdrawn`, `age_ineligible`, `event_too_large`, `spool_capacity`, `retry_age_expired`. `TrackExperimentOutcome(key, outcomeKey, value)` takes the same hop through the platform's outcome apply endpoint: an outcome follows the session's most recent application of the experiment (so **call `ApplyExperimentVariant` first** — without an application it returns `ErrExperimentNotApplied`), keeps following it across a newer version the host has not applied yet and after the assignment is dropped (except by an age refusal), and one the SDK drops before it reaches the analytics queue is counted in its own `Stats.ExperimentOutcomeDrops` with the same reasons. The value must be an integer of magnitude at most 2^53 and the key (trimmed of surrounding whitespace, as the experiment key is) 1–128 of `A–Z a–z 0–9 . _ : -`, not an IP address; anything else, or an outcome earlier than its application, is refused with `ErrInvalidExperimentFact` before anything is queued. It never reports `ErrQueueFull`. A consent denial purges queued facts; a re-grant re-arms the exposures of assignments this instance applied, and only those; any other owed application is counted `consent_withdrawn`. An apply answer overtaken by withdrawal cannot change counters, shared pacing, latches or refetch state. An outcome-ID mint failure returns `ErrExperimentFactUnavailable` and increments the outcome `not_recordable` counter, as exposure-ID failures already do for exposures.

- **Application durability (`SpoolDir`):** the client's own automatic and extra exposures
  and outcomes retain their unsealed application members, IDs and times in the bounded
  spool under a matching writable consent grant. A restart preserves the original IDs and
  waits for the current launch's host age/consent decision before applying them again.
  Applications retained across a canonical-subject remint restore under their recorded
  subject and complete tenant/configuration scope, rather than the new subject.

  Sealing replaces the local application with its sealed envelope in one atomic spool
  rewrite. Recovery also recognizes a sealed drop-time capture, so an interrupted handoff
  cannot replay both forms. Handoff and restart preserve the original local intake and
  ambiguous-delivery clocks. Storage failures leave work owed and increment
  `SpoolPersistFailed`; the lane holds the apply or analytics handoff until durability is
  confirmed. Saving the canonical subject is part of that boundary: a failed save
  retries the same subject before an application can be treated as durable. The variant-serving result is unchanged.

  The same count, byte, age and private-file limits apply. Capacity, expiry, terminal and
  consent drops use the existing spool diagnostics. Capacity eviction and expiry are
  terminal for the local application and its owed record, including eviction by
  another spool write. Each reports once; subsequent sweeps cannot re-admit it or
  evict newer work. These losses also increment the application's per-kind
  `spool_capacity` or `retry_age_expired` counter. This includes losses discovered
  while loading after a stopped process; generic diagnostics and callbacks still run once. A dead-letter's entry for local
  application state is `nil`: neither the analytics wire nor a callback receives its
  subject or targeting attributes. `Close` keeps confirmed durable work replayable instead
  of counting it as `unsealed_at_close` or `undelivered_at_close`.

  Without `SpoolDir`, or without a writable matching grant, new applications remain in
  memory; a restored durable application waits for that grant. `ExperimentSubject` handles
  retain their documented memory-only state.

- **Many players in one process (`ExperimentSubject`):** a backend that serves many players opens one handle per player with `client.ExperimentSubject(shardpilot.ExperimentSubjectConfig{SubjectKey, AnonymousID, Consent})`. `SubjectKey` is the player's own `spcid_` id, as the player's client SDK minted it; it must match `^spcid_[A-Za-z0-9_-]{20,64}$`. The handle's `Fetch(ctx, key, attributes)`, `Variant`, `VariantPayload` and `Apply` work like the client's own calls, but only for that subject: the client's own `spcid_` id is never sent for a handle, never minted for one, and never replaces a handle's subject. Assignments are kept per handle, in memory only: nothing is written under `SpoolDir`, and nothing is revalidated in the background, so an assignment changes only when the host calls `Fetch`. An application records once per (experiment, version) per handle, is sealed by the background lane like the client's own, and its exposure fact carries the handle's `AnonymousID` (never `Config.AnonymousID`; `user_id` is omitted, as on every experiment fact). These facts are retried in memory and never spooled. Without an `AnonymousID` the handle's assignments are served but counted `not_recordable`. `TrackOutcome(key, outcomeKey, value)` records an outcome that follows the handle's most recent recorded `Apply` of the experiment, with `TrackExperimentOutcome`'s rules and errors; it takes the outcome apply hop and is counted in `Stats.ExperimentOutcomeDrops`. **Age:** `FetchWithAgeBand(ctx, key, band, attributes)` declares `age_band` as `FetchExperimentAssignmentWithAgeBand` does, and a non-adult declaration takes effect as on the client: an age-gated assignment stops serving and recording at once, and a fetch that ends without the server's answer withdraws what the handle owes for the experiment. An `age_ineligible` refusal, on the assignment route or an apply route, withdraws the handle's assignment of the experiment, its owed applications and outcomes (counted `age_ineligible`), the application an outcome would follow, and its facts of the experiment not yet sent (counted in `Stats.Dropped`). **Consent is per player:** a handle fetches, serves and records only while its `Consent` is `ExperimentSubjectConsentGranted` and the client's own consent admits the experiment plane; otherwise it refuses with `ErrConsentUnknown` or `ErrConsentDenied` and sends nothing. `SetConsent` changes it; anything but granted withdraws the handle's assignments and owed applications (counted `consent_withdrawn`) and drops the player's facts not yet sent, wherever they wait (counted in `Stats.Dropped`, as for queued events a consent denial purges). The client's own denial withdraws every handle's owed applications too. A subject fault the platform reports returns an error wrapping `ErrInvalidExperimentSubject` and is never retried with another subject. A `401`/`403` on any handle latches the whole plane, as on the client's own fetches, and a `Retry-After` paces it. `Close(ctx)` ends a handle after one bounded delivery pass; the client's `Close` ends every open handle the same way.

```go
player, err := client.ExperimentSubject(shardpilot.ExperimentSubjectConfig{
    SubjectKey:  request.SPCID,       // the player's spcid_ from their client
    AnonymousID: request.AnonymousID, // the player's analytics identity
    Consent:     shardpilot.ExperimentSubjectConsentGranted,
})
if err != nil {
    return err
}
defer player.Close(ctx)
if _, err := player.Fetch(ctx, "welcome-layout", map[string]string{"age_band": "adult"}); err != nil {
    return err
}
variant, payload := player.Apply("welcome-layout")
```

## Privacy & consent

- `SetConsent(analyticsGranted bool)` is tri-state: **unknown** (initial, pipeline open), **granted** (open), **denied** (events dropped at enqueue with `ErrConsentDenied`; pending queue cleared and in-flight batch aborted — cleared events count as `Dropped`, never `Published`). `SetConsentDecision` is the typed form and adds the **forced-minor denial** (`ConsentDecisionDeniedForcedMinor`): analytics-wise identical to denied everywhere, with the receipt carrying `reason: "denied_forced_minor"` so the backend can tell a band-forced denial from a chosen one.
- Both `SetConsent(bool)` and `SetConsentDecision(ConsentDecision)` return `(ConsentResult, error)`. A nil error means the local decision applied; inspect `result.Warnings` for `consent_persist_failed`, `consent_outbox_persist_failed`, or `spool_purge_failed`. These warnings report unfinished work from that call, not server acceptance. An invalid configured actor (an identifier over 512 bytes, or a client source with a `UserID` and no `AnonymousID`) still permits local denial, with warning `consent_actor_invalid` and no receipt; a grant for that actor is refused.
- Refusals leave the decision unchanged: `ErrNotInitialized` (`not_initialized`), `ErrConsentShutdown` (`shutdown`, also matching `ErrClosed`), `ErrInvalidConsentDecision` (`invalid_consent`), `ErrInvalidConsentIdentity` (`consent_actor_invalid`), `ErrConsentForcedMinor` (`consent_forced_minor`), or `ErrSpoolPurgeFailed` (`spool_purge_failed`). A pending purge must succeed before an ordinary grant opens analytics. A denied decision with a failed purge still applies and returns a warning. Decisions after `Close` begins are refused.
- With `SpoolDir`, a persisted `denied_forced_minor` restriction restores in either mode. Ordinary denial while restricted succeeds without changing the state, rewriting the record or minting another receipt; it leaves prior durability work intact. Ordinary grants cannot lift that restriction, even after purge recovery. Other live consent states are memory-only by default: read `ConsentState()` and re-apply with `SetConsent` at startup. Under the opt-in consent floor with `SpoolDir`, all persisted decisions restore as live state — see below.
- **Optional notice provenance:** either setter accepts one `ConsentNotice{NoticeVersion, NoticeLocale, PolicyVersion}` value for the notice presented by the host. Omit the argument when unknown; an explicitly empty carrier or multiple carriers is invalid. Version ids use 1–64 ASCII bytes of `[A-Za-z0-9._+/-]`; locale uses RFC 5646 syntax within 2–35 ASCII bytes and rejects case-insensitive duplicate variants or extension singletons (RFC 5646 sections 2.2.5 and 2.2.6). Repeated extension values and private-use subtags are permitted. Other locale validity requirements remain server-side; the SDK performs no language-registry lookup or case normalization. The SDK copies the tuple into that receipt; later host changes, retry and durable reload retain the original values. Notice text is never accepted by the carrier or sent.
  ```go
  result, err := client.SetConsent(false, shardpilot.ConsentNotice{
      NoticeVersion: "notice-2026.10", NoticeLocale: "en-US", PolicyVersion: "policy-3",
  })
  ```
  Malformed or partial notice metadata refuses a grant with `ErrInvalidConsentNotice` (`consent_notice_invalid`) before state or receipt changes. Denial still applies, omits all three fields and returns that warning alongside every applicable persistence/purge/actor warning. Terminal-client checks still run first; an ordinary denial under forced-minor restriction still creates no receipt. Existing calls without a notice keep their behavior; adapters holding setter method values must allow the new variadic `...ConsentNotice` parameter.
  Durable receipt reload also distinguishes absent notice fields (legacy) from present partial, empty, null-valued, ill-typed or grammatically invalid tuples. A malformed denial sends with all three fields omitted; a malformed grant is removed from the in-memory outbox and never dispatched. `Stats.LastConsentError` and the log diagnose `consent_notice_invalid`; persistence failures take precedence in the single diagnostic field. The existing rejection evidence keeps analytics Unknown rather than restoring an unprovable grant, while recorded denials remain honored. Successful maintenance removes the bad grant from disk only after its unwitnessed mark is durable; if there is no usable decision record or the mark write fails, the corrupt bytes remain as evidence until a fresh explicit decision. Re-issue a valid grant through the live setter. A last server-confirmed state cannot be reconstructed without a separate persistence contract; none is added here.
- **Opt-in client-side consent floor (`Config.ConsentFloor`) — the mode split.** The default (nil) is everything documented in this section: the server-side-responsibility posture, unchanged. Setting `ConsentFloor: &ConsentFloorConfig{}` opts this client into the consent-first floor the engine SDKs ship, for integrations that need client-side enforcement (for example user-facing adopters bound by a DPIA condition to hold events until an explicit grant): **(1) gating** — `Track`/`Enqueue` refuse `ErrConsentUnknown` until an explicit decision is recorded (distinct from `ErrConsentDenied`, so hosts can tell "ask the user" from "the user said no"), and with `SpoolDir` the persisted decision reloads as the live state at startup (the durable receipt trail's tail heals a stale record without overriding forced-minor denial, and spooled events reload only under a grant the resolved state confirms); the floor covers the **configured** consent actor: an event whose per-event `UserID`/`AnonymousID` override resolves to a different effective consent actor under the same source rule is refused with `ErrConsentActorMismatch` (on a client source only an `AnonymousID` override can change it) (per-actor decisions beyond the configured identity stay on the server-side consent path); **(2) durable receipts** — each applied decision for a valid configured actor mints exactly one receipt, except an ordinary denial that preserves an existing forced-minor restriction, into a per-client outbox (32 receipts, FIFO oldest-evicted, no TTL, `consent-outbox.json` under `SpoolDir`; in-memory without it), retained until the server acknowledges it and re-sent **verbatim** across restarts (same `idempotency_key`, same `decided_at` — the server de-duplicates), delivered strictly in decision order with server `Retry-After` honored on `429` **and** `5xx` and jittered backoff otherwise; a failed outbox write never evicts (the write is owed and retried; `Stats.ConsentOutboxPersistFailed`); receipts deliver under denied/unknown too — a receipt documents the decision itself; **(3) the grant-receipt dispatch gate** — while an analytics-grant receipt is retained undispatched, event legs hold (`ErrConsentReceiptPending` from `Track`/`Flush`) so post-grant events can never overtake the grant on the wire and be `suppressed_no_consent` on a strict-consent workspace; released on an observed HTTP outcome for the receipt — success or a status error, never gated on its acknowledgement, while a no-response failure keeps holding — and an empty pipeline is never gated; **(4) teardown durability** — `Close` completes only when every retained receipt is durably on disk (or delivered); otherwise it returns `ErrConsentPending` and stays retryable, so a consent decision's receipt is never silently lost — and a close remnant that could be neither delivered nor durably retained is reported on every `Close` via `ErrEventsDiscarded` rather than read as a clean teardown. Receipt identifiers are bounded at 512 bytes (rejected, never truncated; the outbox sanitizer drops oversized or malformed entries fail-safe on load and save). Consent-plane observability: `Stats.ConsentRecorded/ConsentFailed/ConsentOutboxEvicted/ConsentOutboxPersistFailed/LastConsentError`. Note the credential-tier rule below still applies under the floor: a Mode A publishable key records denials only, so a grant receipt dispatched with one takes the server's terminal `403` (dropped and diagnosed; the gate releases and the trail continues).
- **Recorded design decision — live posture unchanged, disk strictly grant-only.** This SDK's open-by-default LIVE posture is deliberately unchanged: `ConsentUnknown` keeps the pipeline open, and denied stays a hard drop. DISK participation (the `SpoolDir` spool) is strictly grant-only, gated on a **persisted** grant for both writes and loads: setting `SpoolDir` persists applied `SetConsent` decisions to `consent.json` (the forced-minor-preserving no-op leaves its existing record intact), and spool **writes** open only while the live state is granted *and* that grant record was successfully persisted — a grant whose record write fails keeps the spool closed (would-have-spooled batches go to `OnSpoolDeadLetter` with reason `consent`) until a later persist succeeds. The record is **scoped to the actor**: it carries a SHA-256 digest of the configured (workspace, environment, `UserID`, `AnonymousID`, consent actor kind) tuple — a fixed-size digest, never the verbatim identifiers — and a record whose digest does not match the current configuration is no decision at all, so a grant written for one actor never authorizes disk participation for another over a reused `SpoolDir` (logout/login, tenant or workspace switch). The grant is also enforced **per envelope**: an event whose effective actor differs from the configured tuple (a per-event `Event.UserID`/`Event.AnonymousID` override) still rides the live pipeline, but never that grant onto disk — on a retriable failure such envelopes dead-letter (`consent`) instead of spooling. Spool **loads** at startup happen only from a persisted matching grant; any other persisted state (absent, denied, unreadable, other-actor) purges the record at init, and any live transition to non-grant purges it too. A failed purge owes a wipe (the `spool-wipe-owed` marker, or an in-memory flag if even the marker cannot be created) and fails the spool closed — no append, no load, no resend, surfaced as `Stats.LastError = "spool_purge_failed"` — until the wipe succeeds; while a wipe is owed, `SetConsent(true)` retries it first and refuses the grant with `ErrSpoolPurgeFailed` until it lands, retaining the prior denial in memory and on disk. `RemoteConfigCachePath` alone never enables consent persistence.
- In the default mode an applied decision posts fire-and-forget to `/v1/consent` **only when an admissible consent actor is configured**: on `SourceClient`, `Config.AnonymousID` (kind `anon`); on `SourceServer`/`SourceBackend`, `Config.UserID` (kind `user_verified`), else `Config.AnonymousID` (kind `anon`). The forced-minor-preserving no-op emits no receipt; with neither identity set the decision is applied locally only and nothing is posted server-side. A client source with only `Config.UserID` refuses grants (`ErrInvalidConsentIdentity`) and applies denials locally without a receipt. When posted, decisions are transmitted by a single per-client sender in call order, so deny-then-grant cannot arrive reversed. `Close` waits (bounded by its context) for prior decisions to finish sending.
- **Server-side acceptance depends on the credential tier.** A publishable client ingest key (Mode A) may record **denials only**: a receipt whose category values are all `false` is accepted for the key's own scope, while a **grant** receipt is rejected `403` with detail code `consent_grant_requires_verified_credential` and dropped by policy (the SDK logs it quietly; local consent state is unaffected). Grants are recorded server-side only through a consent-write-capable **service** credential — and until such a grant lands, a previously recorded denial keeps suppressing that actor's events (`suppressed_no_consent`). Client-key consent writes also consume ingest budget whether accepted or rejected (deliberate anti-flood on a public credential).
- **Strict-consent (enforce) workspaces terminally suppress events published under `unknown` consent.** Unlike the consent-first client SDKs (Defold/Unity/Unreal), this SDK's initial `unknown` state keeps the event pipeline open — but on a workspace whose effective strict consent mode is `enforce`, the server fails closed: every event whose actor has no explicit analytics consent recorded server-side is suppressed per event as `suppressed_no_consent` inside the `202` (never a batch-level error), so the publish "succeeds" while delivering nothing. Ensure the grant is **recorded server-side before** the actor's first events are published: `SetConsent(true)` posts its receipt fire-and-forget in the background with no success signal exposed (failures are only logged), so calling it immediately before publishing does not synchronize the grant — events flushed before the `/v1/consent` write lands are still suppressed; when admission must be guaranteed from the first event, record the grant out-of-band through a consent-write-capable service credential first. The receipt also covers **only the configured consent actor** (selected by source, as above); an event whose per-event override changes that actor (`Event.AnonymousID` on a client source; `Event.UserID`, or `Event.AnonymousID` when no user is configured, on a server or backend source) needs consent recorded for that actor through that same service path (see the previous bullet — that credential tier is what can record grants at all). Watch `Config.OnBatchResult` or the `Snapshot().ByStatus` breakdown for `suppressed_no_consent` to detect suppression.
- **Consent-plan API migration:** the experimental `pkg/consentpolicy` package is no longer a supported public import. There is no public replacement for plan parsing or `Prepare` in this release. Remove dependencies on that experimental API before upgrading; do not replace a refusal with permission or bypass retained admission restrictions. Root analytics consent controls remain independent and unchanged.
- Crash reports strip PII and raw identifier material; there is no API surface for screenshots, network payloads, or attachments.
- Do not commit tokens or real customer/player data. Tokens and full event payloads are never logged by default.

## Minting client ingest JWTs (backend-only, Mode B)

> **Backend-only — this helper holds the per-tenant signing secret.** It belongs in a trusted server-side game backend and must **never** be compiled into a shipped client binary. Client SDKs (Unity, Unreal, Defold) consume a minted token through a `token_provider` callback and never see the secret. This Go SDK is a backend/service-tier SDK, so its dual-mode role is the inverse: it **mints** the short-lived per-user JWTs that client SDKs then fetch over your own authenticated channel.

`SignIngestJWT` mints a short-lived HS256 JWT that the ShardPilot ingest API's Mode-B verifier accepts. The per-tenant signing secret (and its key id `kid`) is minted, rotated, and served by ShardPilot; your backend obtains it out-of-band and passes it in. The helper does not fetch, store, or rotate the secret — it only signs. The helper mints tokens for clients with a token-provider integration. This Go client does not accept static JWT configuration and has no token provider.

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

## Capability discovery

`shardpilot.Supports(key)` works before a client exists. It returns true for
`consent_receipt_outbox`, `consent_state_denied_forced_minor`,
`schema_revision_declaration`, `experiments_assignment` and `experiments_age_band`.
Every other spelling, including an empty key, different case or surrounding
whitespace, returns false. A supported opt-in feature still needs its documented
configuration; the query does not enable it.

## Build & test

No Makefile — standard Go tooling. CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs `go test ./...` and `go vet ./...` on **both** Go 1.25.x (the baseline) and 1.27.x, plus a release version-consistency check (`scripts/check_release_consistency.sh`; see [`docs/release.md`](docs/release.md)).

**`gofmt` is checked once, on Go 1.27.x only**, in a separate `format` job outside the version matrix. gofmt's output changes between releases — 1.27 de-indents the continuation lines of a multi-value `return` whose operands are composite literals, which 1.25 indents — so a file cannot satisfy both formatters and running the check on every matrix leg asserts an impossibility. **Format with Go 1.27's `gofmt` even if you develop against the 1.25 baseline**: 1.25's formatter will produce a diff CI rejects. This costs nothing at runtime — formatting has no bearing on what the 1.25 baseline compiles, which is what the matrix is there to prove.

```bash
go build ./...
go test ./...
go vet ./...

# NOT a bare `gofmt -l .` and NOT `go fmt ./...` — both run whichever gofmt is on
# PATH, which on the 1.25 baseline this module targets produces a diff the 1.27
# `format` job rejects. Resolve 1.27's binary explicitly, and check the resolution
# rather than assuming it: GOROOT is tested for emptiness BEFORE /bin/gofmt is
# appended (an empty one makes that /bin/gofmt, which often exists), and the
# resolved version is compared against the requested one (a `go` older than 1.21
# ignores GOTOOLCHAIN entirely and hands back its own GOROOT, so the ask succeeds
# and the pin does nothing). The version comes from `go env GOVERSION`, not from
# $GOROOT/VERSION — that file is optional, so reading it would reject a correctly
# repackaged 1.27.0.
goroot="$(GOTOOLCHAIN=go1.27.0 go env GOROOT)" || goroot=""
[ -n "$goroot" ] || { echo "cannot resolve go1.27.0 GOROOT" >&2; exit 1; }
[ "$(GOTOOLCHAIN=go1.27.0 go env GOVERSION)" = go1.27.0 ] || { echo "resolved the wrong toolchain" >&2; exit 1; }
"$goroot"/bin/gofmt -l .   # must print nothing
"$goroot"/bin/gofmt -w .   # to fix
```

The consent-dispatch regression cases can be repeated with
`go test -run '^TestPointConsentDropsAfterBlockingDispatch$' -count=20 .`.
The spool case verifies that the blocked request carries the persisted ordinary
retry before changing consent, then checks the worker-held experiment facts'
delivery and drop accounting.

### Secret-scan coverage checks

CI runs the working-tree scan and verifies the history scan against an independently
measured Git corpus. Complete Git history is required. The history checker reports reachable commits, refs, the
expected count of commits with text patches, excluded commits and corpus digests.
It includes first-parent merge diffs and requires the scanner's count to match the
material corpus while refs and HEAD remain stable.

Run the controls with Python 3.11+ and gitleaks 8.30.1:

```bash
python3 scripts/test_secret_scan.py
python3 scripts/check_secret_scan.py .
```

Add `--container` to use the pinned scanner image through Docker, as CI does.
The controls exercise count/reader failures, changing refs, merge and empty commits,
and the synthetic literal exception in both directions. See [release checks](docs/release.md#secret-scan-corpus).

The staged publication check also refuses decorated flag identifiers whose
required separators can disappear in marker removal. It reads those spellings
directly in filenames, staged symlink targets and file contents, before the
existing raw and marker-free checks. Symlink targets are never followed.
Occurrence buckets
preserve every matched byte, including control bytes, and repeated matches
retain their multiplicity. Run `scripts/test_surface_ratchet.sh` from a clean
committed checkout to exercise the repository fixtures; see the
[publication controls](https://github.com/shardpilot/shardpilot-go/blob/main/docs/release.md#publication-shape-controls).

Baseline writes allocate a private directory on the baseline filesystem before
creating temporary files. A baseline format change must be separate from Go
source changes. CI compares a new ref with the newest published ancestor of its
immutable event, excluding the new ref from the other published heads and tags.
Missing or incomparable published ancestry refuses the comparison.
`python3 scripts/test_public_surface_base.py` runs local Git fixtures and four
whole-gate scenes, including a first publication containing several commits.

CI runs `bash scripts/public-gate.sh --test --compact` on hosted Ubuntu as a
non-root user with Bash, Python 3 and GNU coreutils. It also runs
`bash scripts/public-gate.sh --test --compact --permission-bypass channel-closes-after-json`.
That second command models ignored permission changes; it is not UID-0 execution.
The channel scene verifies the failed append before checking the refusal exit.

## Conventions & boundaries

- **Zero third-party dependencies** — stdlib only. Keep it that way.
- **No domain logic in core.** No product-specific event names or game/vertical fields in the universal envelope.
- **The SDK only sends telemetry.** It performs no provider, model, GitHub, billing, or account-management write calls, and triggers no automatic actions.
- **Fail-safe HTTP.** HTTPS required outside localhost/loopback (private-network HTTP only via explicit opt-in). No durable local queue unless the opt-in disk spool is configured (`SpoolDir`); no retry storms; the worker retains at most one failed in-memory batch.
- The `go` directive is held deliberately below the current toolchain for consumer compatibility — **1.25** today — even though CI also exercises the current toolchain. It moves only when the SDK needs something the older baseline cannot express: 1.25 was taken for `testing/synctest`, which is available to tests written against it from here on. It did **not** retire the existing suite's real-time waits, and no test currently imports it — the retry and flush-pacing tests still run on the real clock. See the `v0.6.3-alpha` entry in [`CHANGELOG.md`](CHANGELOG.md) for the measurement behind that.

## Roadmap

Pre-v1; the API is explicitly unstable.

- The crash actor key (`Event.AnonymousID`; `Event.SessionID` is session linkage, not the actor key), the experiment-assignment consumer, the remote-config targeting-attribute pass-through, the crash capture opt-ins and the client-side consent floor shipped in `v0.6.0-alpha`. The changelog's `v0.6.3-alpha` section includes the changes shipped since `v0.6.1-alpha`; read it before upgrading. It is not enumerated here — a count in a second place goes stale the next time an entry lands, and this one already had.
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
