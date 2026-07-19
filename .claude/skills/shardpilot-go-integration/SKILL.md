---
name: shardpilot-go-integration
description: Use when integrating the ShardPilot Go SDK (shardpilot-go) into a Go server or backend service — pinned install, credentials, the server-side consent posture, analytics events, crash reporting, and how to verify the integration end to end.
---

# Integrating the ShardPilot Go SDK

This skill describes the SDK exactly as shipped in the
pinned release tag `v0.4.0-alpha`. Every behavioral claim below was verified
against that tag's source. Where the SDK does not have a capability, this
skill says so — do not invent config fields, endpoints, or behaviors beyond
what is documented here.

## What the SDK does today

`shardpilot-go` is ShardPilot's **server-side** Go SDK (backend/service tier),
stdlib-only with zero third-party dependencies. It:

- builds and sends app-first analytics event batches to
  `POST {IngestURL}/v1/events:batch` with bearer-token auth;
- sends crash reports (separate `pkg/crash` client) to
  `POST {base}/api/v1/crashes/ingest`, including automatic Go panic capture;
- records explicit analytics consent decisions (`SetConsent` / `Consent`) and
  transmits them to ShardPilot in the background;
- mints short-lived Mode-B per-user ingest JWTs (`SignIngestJWT`) for client
  SDKs to consume — a backend-only helper;
- offers an opt-in persisted anonymous ID helper (`LoadOrCreateAnonymousID`).

It deliberately does **not**: manage session lifecycles (`Event.SessionID` /
`Event.SessionSequence` are passed through verbatim), fetch remote config
(see below), persist events or consent state to disk, or auto-instrument
anything. It sends telemetry only — no other write calls, no automatic
actions.

## Install

```bash
go get github.com/shardpilot/shardpilot-go@v0.4.0-alpha
```

- Requires **Go 1.24+**.
- Two import paths:
  - `github.com/shardpilot/shardpilot-go` — analytics (package `shardpilot`);
  - `github.com/shardpilot/shardpilot-go/pkg/crash` — crash reporting
    (package `crash`).
- **`v0.1.0` is retracted** in `go.mod`; never pin it. Older usable pins
  (`v0.3.0-alpha`, `v0.2.0-alpha`, `v0.1.2`) ship progressively less surface —
  prefer the pin above.
- Pre-launch: there is no public production ingest endpoint yet. `IngestURL`
  is the base URL of the ShardPilot deployment you were given (or a local
  stack). HTTPS is required outside localhost/loopback. The **analytics
  client only** can opt into private-network HTTP via
  `Config.AllowInsecurePrivateNetwork`, and only for private (RFC1918) **IP
  literals** — the SDK never resolves DNS names, so an internal hostname
  (e.g. a `.internal` alias) still requires HTTPS. The crash client has no
  such option and rejects any plain-HTTP URL outside localhost/loopback.

## Credentials

The analytics client has a **single `Config.Token` bearer field**. It holds
one of:

- **Mode A — publishable ingest key** (`sp_ingest_` prefix): the standard
  service credential for event publishing. Publishable keys are
  deliberately limited on the consent plane: they can record consent
  **denials only** — a grant receipt sent under a publishable key is
  rejected by the server (the SDK logs it quietly; local state is
  unaffected). Recording a **grant** server-side requires a
  consent-write-capable service credential provisioned outside this SDK.
- **Mode B — per-tenant ingest JWT**: a short-lived HS256 JWT minted
  **backend-side** with `SignIngestJWT(key, claims, opts...)` and placed in
  `Config.Token` (or handed to a client SDK over your own authenticated
  channel). The per-tenant `SigningKey{KID, Secret}` is obtained out-of-band
  from ShardPilot; the helper only signs — it never fetches, stores, or
  rotates the secret. Defaults: issuer `shardpilot`, audience
  `shardpilot-ingest`, lifetime 5m (server cap 15m; the server also enforces
  a 5m issued-at freshness window regardless of `exp`). Scope is fixed to
  `analytics:ingest`. Call `SigningKey.ZeroSecret()` to wipe a secret you no
  longer need. A minted JWT is **per-actor, not tenant-wide**: it binds the
  verified `Subject` (plus optional `BindAnon`) and authorizes ingest only
  for that user within the tenant scope, so a client running on a Mode-B
  token must publish only that actor's events. A long-lived backend client
  that publishes for many users belongs on a Mode A key; mint Mode-B JWTs
  primarily for individual client SDK instances to consume.

The crash client (`pkg/crash`) takes its own `ClientOptions.APIKey` — an API
key with the `crash:write` scope.

Handling rules — non-negotiable:

- **Never hardcode tokens, API keys, or signing secrets** in source, config
  files, or examples. Read them from environment variables or a secret
  manager (`os.Getenv("SHARDPILOT_TOKEN")` in the samples below).
- The Mode-B signing secret and minted JWTs are bearer credentials: never
  compile the secret into a shipped client binary, never log either
  (`SigningKey` redacts itself under `%v`/`%+v`, but `key.Secret` printed
  directly still leaks).

## Init

```go
import "github.com/shardpilot/shardpilot-go"

client, err := shardpilot.NewClient(shardpilot.Config{
    IngestURL:     os.Getenv("SHARDPILOT_INGEST_URL"), // base URL, no path
    Token:         os.Getenv("SHARDPILOT_TOKEN"),
    WorkspaceID:   os.Getenv("SHARDPILOT_WORKSPACE_ID"),
    AppID:         os.Getenv("SHARDPILOT_APP_ID"),
    EnvironmentID: os.Getenv("SHARDPILOT_ENVIRONMENT_ID"),
    Source:        shardpilot.SourceBackend, // or SourceServer / SourceClient
    AppVersion:    "1.0.0",
})
if err != nil {
    return err
}
defer func() {
    // Close flushes pending events and consent receipts, bounded by its
    // context — always give it a deadline so a degraded ingest cannot
    // stall shutdown.
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    _ = client.Close(ctx)
}()
```

Required: `IngestURL`, `Token`, `WorkspaceID`, `AppID`, `EnvironmentID`, and a
valid `Source` (`SourceClient` / `SourceServer` / `SourceBackend`).
`NewClient` rejects an `IngestURL` with a path, query, fragment, or userinfo,
and non-HTTPS URLs outside localhost/loopback (plain HTTP to a private
RFC1918 **IP literal** only, via `AllowInsecurePrivateNetwork` — hostnames
are never resolved). Optional tuning: `BatchSize` (default 25, max
100), `BufferSize` (async queue capacity, default 1000), `FlushInterval`
(default 1s), `HTTPTimeout` (default 2s), `Logger`, `UserID`/`AnonymousID`
(default actor identity), `OnBatchResult` (see verification). The SDK itself
reads no environment variables.

## Consent model — READ THIS FIRST, IT IS INVERTED

**This SDK's consent posture is deliberately the OPPOSITE of ShardPilot's
consent-first client SDKs (Defold/Unity/Unreal).** Those SDKs transmit
nothing while consent is unknown. This server-side SDK does the inverse:

- **`unknown` (initial state) = the event pipeline is fully OPEN.** The SDK
  transmits events immediately, with no consent recorded. The integrating
  server is the data controller here: **you must gate event submission
  upstream** — only hand events to this SDK for actors you are entitled to
  track. Do not port client-SDK assumptions ("unknown means nothing is
  sent") into a Go backend integration; here that assumption silently
  becomes "everything is sent".
- **`denied` = hard stop.** `SetConsent(false)` immediately makes `Track` /
  `Enqueue` return `ErrConsentDenied`, clears the pending queue (cleared
  events count as `Dropped`), and aborts any batch publish already in flight
  on the network. `SetConsent(true)` re-opens the pipeline.
- **Consent state is in-memory only.** It is NOT persisted across process
  restarts. If consent must survive restarts, store the decision yourself
  and re-apply it with `SetConsent` on startup, before publishing.
- **Consent receipts are fire-and-forget.** A `SetConsent` call also
  transmits the decision to ShardPilot in the background — but only when an
  actor identity is configured (`Config.UserID`, else `Config.AnonymousID`);
  with neither set the decision is local-only. Receipts are sent by a single
  per-client sender in call order, buffered at most **16 pending decisions**
  (overflow discards the oldest; the newest decision wins server-side).
  Failures are only logged. `Close` waits (bounded by its context) for
  pending receipt sends.
- **There is NO receipts-before-batch guarantee.** `SetConsent(true)` does
  not synchronize admission: events flushed before the background consent
  write lands on the server are still treated as consent-unknown there. On a
  workspace with strict consent enforcement, the server then terminally
  suppresses each such event as `suppressed_no_consent` **inside the HTTP
  202** — the publish "succeeds" while delivering nothing, and no error is
  returned. The only ways to see this are the `Config.OnBatchResult`
  callback and the `Snapshot().ByStatus` breakdown
  (`EventStatusSuppressedNoConsent`). When admission must be guaranteed from
  the first event, record the grant server-side out-of-band (via a
  consent-write-capable service credential) before publishing. The receipt
  covers only the configured actor; events that override the actor per event
  (`Event.UserID` / `Event.AnonymousID`) need consent recorded for each such
  actor through that same service path.
- **No forced-minor consent state.** The client SDKs' `denied_forced_minor`
  state and its associated flow do not exist in this SDK; `SetConsent` takes
  a plain bool and the states are exactly `unknown` / `granted` / `denied`
  (read via `Consent()`).

## Sending analytics events

```go
// Synchronous: publishes now, returns the transport error.
err = client.Track(ctx, shardpilot.Event{
    Name:   "purchase", // must be legal for your configured Source
    UserID: userID,
    Props: map[string]any{
        "amount":   9.99,
        "currency": "USD",
        "product":  "starter_pack",
    },
})

// Asynchronous: bounded in-memory queue, background flush worker.
err = client.Enqueue(shardpilot.Event{Name: "economy_tx", UserID: userID, Props: props})
// err is ErrQueueFull when the buffer (BufferSize) is full — the event was dropped.

_ = client.Flush(ctx) // force-drain the queue now
```

Facts that keep integrations correct:

- **Event names are schema-checked per source.** Pick canonical events whose
  schema allows your configured `Source`; e.g. `purchase` / `economy_tx` are
  backend-source events, while session/screen events are client-source-only.
  An unregistered `event_name` is accepted as status `observed` (stored for
  observation, not surfaced as a product metric).
- **`SessionID` is required for non-`backend` sources.** This SDK does not
  manage sessions; with `SourceClient`/`SourceServer` you must set
  `Event.SessionID` (and a monotonic `Event.SessionSequence`) yourself, or
  the batch is rejected.
- **Event IDs and timestamps are stamped once at intake** (`Event.ID`
  auto-generated when empty), so a retried batch is de-duplicated by the
  server instead of double-counted.
- **A permanent (non-retryable 4xx) failure drops the whole batch** — e.g.
  one invalid event takes down its batch.
- **Only queued publishes are retried.** The background flush worker retains
  a batch that failed retryably (429/5xx or transport error) and retries it,
  honoring the server's `Retry-After` hint. Synchronous `Track` does **not**
  retry: it publishes once and returns the error, so `Track` callers own
  their own retry/error policy (`HTTPStatusError.RetryAfter` carries the
  server's hint to honor).
- Non-2xx responses surface as `*shardpilot.HTTPStatusError` with the
  server's machine-readable `ErrorCode` (e.g. `unauthorized`,
  `validation_error`, `rate_limited`), per-field `Details`, and `RetryAfter`.

## Remote config

**Not available in this SDK.** The pinned release has no remote-config API —
no fetch call, no getters, no cache. Do not generate remote-config code against
this SDK; if the integration needs server-driven tuning values, source them
elsewhere.

## Crash reporting (`pkg/crash`)

Separate client, separate credential (`APIKey` with `crash:write`):

```go
import "github.com/shardpilot/shardpilot-go/pkg/crash"

crashClient, err := crash.NewClient(crash.ClientOptions{
    IngestURL: os.Getenv("SHARDPILOT_CRASH_INGEST_URL"), // base URL, no path
    APIKey:    os.Getenv("SHARDPILOT_CRASH_API_KEY"),
    App:       crash.AppInfo{ID: "<YOUR-APP-ID>", Version: "1.0.0"}, // required for auto-capture
    Source:    "<COMPONENT-SLUG>", // e.g. which service in a multi-repo product
    OnResult:  func(r crash.Result) { /* see verification */ },
})
```

- `Emit(ctx, event)` — non-fatal report. **Sampled by default: only every
  10th non-fatal per client is transmitted** (calls 10, 20, 30, …); a
  sampled-out `Emit` returns `nil` exactly like a sent one. Pass a custom
  `Sampler` to change this.
- `EmitFatal(ctx, event)` — fatal report, never sampled, always transmitted.
- `defer crashClient.Recover(ctx)` at each goroutine / request-handler
  boundary — captures a panic as a fatal crash with pre-symbolicated Go
  frames, sends it synchronously (best-effort, detached from the caller's
  cancellation), then **re-panics** so normal crash behavior is preserved.
- `CapturePanic(ctx, recovered)` — reports an already-recovered panic value
  without re-panicking.
- `RecordBreadcrumb(name)` — ring buffer attached to subsequent events.
- Events are PII-scrubbed and sanitized before send; retries default to 2
  attempts with backoff, honoring `Retry-After`.
- The crash `IngestURL` must be HTTPS outside localhost/loopback — unlike
  the analytics client, there is **no** private-network HTTP option here.

**There is no client-side consent gating in `pkg/crash`** — no opt-out
switch, no consent check before send (crash reporting operates as a
server-posture legitimate-interest plane; gate upstream if your product
needs an opt-out). Consent is enforced **server-side**: when the actor's
consent is withheld, the ingest returns 2xx but does not store the crash,
and that surfaces as `Result.Suppressed == true` in the `OnResult` callback
(also logged). A 2xx alone is not delivery confirmation.

## Offline behavior / spool

**None.** The analytics queue is in-memory only (bounded by `BufferSize`);
a disk spool is an acknowledged TODO in the source, not a feature. Concretely:

- Events still buffered when the process dies are **lost**.
- Events dropped on queue overflow (`ErrQueueFull`) are **lost**.
- There is no offline replay across restarts, for analytics or crash reports
  (a crash report that cannot be sent at capture time is lost).

Call `Flush(ctx)` at checkpoints and `Close(ctx)` on shutdown to bound the
loss window. If at-least-once delivery matters, keep your own durable record
upstream of the SDK.

## Verify your integration

Run against your dev/staging deployment credentials, then check each item:

1. **Wire the result surfaces before testing.** Set `Config.OnBatchResult`
   (analytics) and `ClientOptions.OnResult` (crash) to log what the server
   actually reported — errors alone cannot show suppression.

   ```go
   cfg.OnBatchResult = func(r shardpilot.BatchResult) {
       log.Printf("batch: accepted=%d rejected=%d duplicates=%d", r.Accepted, r.Rejected, r.Duplicates)
       for _, e := range r.Events {
           log.Printf("event %s: %s %s %s", e.EventID, e.Status, e.Code, e.Message)
       }
   }
   ```

2. **Analytics round trip.** `Track` one test event valid for your `Source`
   and confirm: the returned error is `nil`, and `OnBatchResult` reports the
   event with `Status` `shardpilot.EventStatusAccepted` (`"accepted"`) — or
   `shardpilot.EventStatusObserved` (`"observed"`) if you used an
   unregistered test name; both prove auth + connectivity + envelope shape.
   Cross-check `client.Snapshot()`: `Published` incremented and
   `Snapshot().ByStatus` counting your event's status.
3. **Consent suppression check.** If `OnBatchResult` shows
   `shardpilot.EventStatusSuppressedNoConsent` (`"suppressed_no_consent"`),
   the actor's consent is withheld server-side and the 202 was NOT
   delivery. That has two distinct causes — determine which before acting:
   either **no decision is recorded** and the workspace enforces strict
   consent, or the actor has an **explicitly recorded denial** (which
   suppresses regardless of workspace mode). If the actor genuinely
   consented, record the grant server-side (see the consent section) and
   re-test until the status is `accepted`. If the denial is real, the
   suppression is correct behavior — leave it in place; never overwrite an
   opt-out just to make a verification pass.
4. **Failure visibility.** Point `Token` at an invalid value once and
   confirm `Track` returns a `*shardpilot.HTTPStatusError` whose `ErrorCode`
   is `unauthorized`/`forbidden` — proves your error handling surfaces real
   causes. Restore the real token.
5. **Crash round trip.** Use **`EmitFatal`**, not `Emit`, for the test
   (the default sampler silently drops the first 9 non-fatal `Emit`s per
   client). Send a synthetic fatal event and confirm the error is `nil` and
   `OnResult` received a `crash.Result` with a non-empty `CrashID` and
   `Suppressed == false`. If `Suppressed` is `true`, the server accepted but
   did not store it (actor consent withheld server-side).
6. **Panic capture.** In a throwaway binary, `defer crashClient.Recover(ctx)`
   around a deliberate panic; confirm the process still crashes (re-panic)
   and the report shows up with your `App.ID`/`Source` and Go frames.
7. **Shutdown.** Confirm your service calls `Close(ctx)` with a timeout on
   shutdown, and that `Close` returns `nil` (pending events + consent
   receipts flushed within the deadline).

## Known limitations (verified 2026-07-19)

Stated plainly so integrations are designed around them, not surprised by
them:

- **No durable delivery.** In-memory queue only; process death or queue
  overflow loses events. No offline spool for analytics or crash reports.
- **Consent state does not survive restarts** (in-memory only; re-apply on
  startup yourself).
- **Consent receipts have no delivery guarantee**: fire-and-forget, 16-entry
  pending buffer with oldest-dropped overflow, failures only logged, no
  per-receipt success signal, and no ordering guarantee relative to event
  batches (`SetConsent(true)` does not gate or synchronize admission).
- **Mode-A publishable keys cannot record grants** — denials only; grants
  need a separate consent-write-capable service credential.
- **No `denied_forced_minor` / forced-minor flow** (exists in ShardPilot's
  client SDKs, not here).
- **No remote config** in this SDK.
- **Whole-batch loss on a permanent 4xx** — one invalid event drops its
  entire batch (partial-batch recovery is a known TODO).
- **Non-fatal crash sampling defaults to 1-in-10 per client**, and a
  sampled-out `Emit` is indistinguishable from a sent one except via
  `OnResult`; clients emitting fewer than 10 non-fatals report none unless
  a custom `Sampler` is set.
- **No client-side crash consent gate** — suppression happens server-side
  only (`Result.Suppressed`).
