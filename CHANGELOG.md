# Changelog

## Unreleased

- The analytics client now parses the error envelope on non-2xx ingest responses and honors
  `Retry-After`. `HTTPStatusError` carries the server's machine-readable `ErrorCode`
  (e.g. `rate_limited`, `validation_error`), `ErrorMessage`, the per-field `Details` list,
  and the `Retry-After` header as a `RetryAfter` duration (both standard forms —
  delta-seconds and HTTP-date — clamped to 24h, consistent with the crash client) —
  `Error()` folds the code and up to five `field:code` detail pairs
  into the message, so logs show `status 429 (rate_limited) [events:events_rate_limited]`
  instead of a bare status. After a rate-limited automatic publish the background flush
  worker now defers its next automatic attempt until the `Retry-After` deadline passes
  (events keep buffering in the bounded queue meanwhile) and retries AT that deadline via a
  dedicated wake — not at the next flush tick, which could be much later when
  `FlushInterval` exceeds the hint; explicit `Flush` and `Close`
  attempts are not gated — they carry caller intent — and a renewed failure re-arms the
  deferral.

- Event ids and timestamps are now stamped once when an event is accepted (`Track`/`Enqueue`)
  rather than on each publish attempt, so every retry of a batch re-sends byte-identical
  event identities and the ingest service folds re-sends as duplicates instead of storing
  them twice. Caller-supplied `Event.ID`/`Event.Timestamp` values are used unchanged, as
  before.

- The analytics client now surfaces the ingest endpoint's per-event outcomes instead of
  discarding them. The `202` batch response carries an `events[]` list (one `event_id` +
  `status` + optional `code`/`message` per event), and a new optional
  `Config.OnBatchResult func(BatchResult)` callback reports it after each successful batch
  publish — the only way to learn which individual events the server **rejected**,
  **suppressed** for withheld consent (`suppressed_no_consent` /
  `suppressed_ad_revenue_consent` — the `2xx` alone is not delivery confirmation),
  **observed** (event name not registered), or folded as **duplicates**. The callback runs
  on the publish path (the background flush worker and synchronous `Track` publishes share
  it, so it may be called concurrently); keep it fast and non-blocking, and a panic inside it
  is recovered so a buggy callback cannot stop delivery. `Snapshot()` gains a
  `Stats.ByStatus map[EventStatus]uint64` per-status breakdown folded from the same list (the
  existing `Accepted`/`Rejected`/`Duplicates` aggregate counters are unchanged). The public
  `Track`/`Enqueue`/`Flush`/`Snapshot` signatures are unchanged; this is purely additive.
  (Partial-batch acceptance on a permanent `4xx` and a bounded disk-spool remain follow-ups,
  marked with TODOs in the source.)

- `pkg/crash` now surfaces the ingest response and honors server backpressure. A new
  optional `ClientOptions.OnResult func(Result)` callback reports the server's per-crash
  `Result` — the assigned `CrashID`/`Fingerprint`, a `Suppressed` flag (the crash was
  accepted but **not stored** because the actor withheld consent, so the `2xx` alone is not
  delivery confirmation), and any `Warnings` — on both manual `Emit`/`EmitFatal` and the
  auto-capture path; suppression and warnings are also logged. The retry loop now honors a
  `Retry-After` response header (delta-seconds or HTTP-date, clamped to a safe maximum) on a
  `429`/`5xx`, falling back to the fixed backoff when absent. `Emit`/`EmitFatal` signatures
  are unchanged; the previously discarded response body is now read (best-effort — a 2xx with
  an unparseable body is still treated as accepted).

- Added automatic panic capture to `pkg/crash`: `Client.Recover(ctx)` (defer at a
  goroutine / request-handler boundary — reports the panic as a fatal crash, then
  re-panics so normal crash behaviour is preserved) and `Client.CapturePanic(ctx,
  recovered)` (report an already-recovered value without re-panicking). Captured
  frames are pre-symbolicated from the Go runtime (package-qualified function, file,
  line — no native modules or addresses, accepted by the crash ingest API). New
  `ClientOptions.App` (defaulted onto every event; required for auto-capture, and
  `App.ID` must match the API key's app scope) and `ClientOptions.Source` (component
  slug) are stamped on events that don't set their own. The report send
  detaches from the caller's context cancellation/deadline (keeping its values) so a
  panic during graceful shutdown or after a client disconnect is still delivered; a
  nil client is a safe no-op and still re-panics. The runtime panic machinery
  (`runtime.gopanic`/`sigpanic`/`panicmem`/`panicdivide`/`panicBounds*`/`goPanic*`) and
  the SDK's own frames are trimmed so the application origin is the top frame across
  panic kinds. Frame function names are scrubbed as code symbols (email/IP only), not
  free text, so legitimate package-qualified symbols (incl. `player_*`/`user_*` package
  names) survive; ShardPilot re-scrubs server-side as defense in depth.
- Added `SignIngestJWT`: an optional, backend-only helper that mints a
  short-lived Mode-B per-tenant ingest JWT (HS256) that the
  ShardPilot ingest API's Mode-B verifier accepts. A trusted Go game-backend can use
  it to mint the per-user tokens that client SDKs (Unity/Unreal/Defold) then
  fetch over the studio's own authenticated channel via their `token_provider`
  callback. The helper holds the per-tenant signing secret obtained out-of-band
  from ShardPilot (`SigningKey{KID, Secret}`, secret as raw `[]byte`), and
  signs a conformant token: header `alg=HS256` + `kid`; claims `iss`/`aud`/`sub`/
  `iat`/`exp`/`scope=analytics:ingest`/`workspace_id`/`app_id`/`environment_id`
  and optional `bind_anon`. Lifetime defaults to 5m — equal to the server's 5m
  iat-age window, which the verifier enforces regardless of `exp` (capped at the
  server's 15m max-lifetime). `iat` is stamped to now (fresh against that 5m
  window), and
  every input is validated at mint time so a token it returns is never rejected
  downstream for a malformed claim, an over-long subject/anon, or an over-long
  lifetime. The `iss`/`aud` defaults are the neutral public values `shardpilot` /
  `shardpilot-ingest` (matching the ingest verifier's defaults); override either per
  deployment with `WithIngestIssuer`/`WithIngestAudience`/`WithIngestLifetime`
  and `WithIngestNow`/`WithIngestClock` (tests). The HS256 signing is hand-rolled
  (no new dependency; the SDK stays dependency-free) and can only ever emit
  HS256, so algorithm confusion is impossible by construction. The secret is
  never logged and `SigningKey.ZeroSecret()` wipes a copy in place. This is
  additive and does not change the existing service-tier `Config.Token`
  transport. **Backend-only: the secret must never ship in a client binary.**
- Fixed the quickstart (README and `examples/basic`) to demonstrate a
  backend-legal canonical event. The previous example tracked
  `session_start` with `Source: SourceBackend`, which is doubly wrong: the
  canonical session event is named `app.session_started` AND is
  client-source-only, so a backend SDK cannot legally send it. The
  quickstart now tracks `purchase` (source const `backend`) with the
  schema-required props `amount`, `currency`, and `product`. Remaining
  stale `session_start` literals in tests, the crash example, and docs were
  updated to canonical names.
- Added `LoadOrCreateAnonymousID(path)`: an opt-in helper that loads or
  creates a UUIDv7 anonymous identifier persisted at the given file path
  (0600 permissions, parent directories created as needed). The ID is fully
  written to a private temp file and then published to the final path
  atomically without overwriting (a hard link, which fails on EEXIST instead
  of replacing the winner like a rename would), so the final path only ever
  appears complete: concurrent first runs racing on the same path converge
  on a single winner's ID, never overwrite each other, and never observe an
  empty or partially written file. A write failure (disk full and friends)
  only ever touches the temp file, which is cleaned up so later calls
  recover. The SDK never calls it implicitly and never writes files on its
  own.
- Added a minimal consent API: `Client.SetConsent(analyticsGranted bool)`
  and `Client.Consent()` with tri-state semantics {unknown, granted,
  denied}. Unknown leaves the pipeline fully open. Denied drops events at
  enqueue (`Track`/`Enqueue` return the new `ErrConsentDenied`) and clears
  every pending event — the queued backlog, any batch the worker has
  already pulled in-flight, and any batch publish already on the network
  (the HTTP request is aborted) — so events from before a denial never
  publish, even across a later re-grant (cleared and aborted events count as
  `Dropped`, never as `Published` or as failed batches, even when the
  re-grant lands before the aborted request returns). An explicit decision
  is posted to
  `POST {IngestURL}/v1/consent` with the batch transport credentials; the
  post is fire-and-forget for the caller but transmitted by a single
  per-client sender in call order, so deny-then-grant cannot arrive at the
  server reversed, and `Close` waits (bounded by its context) for decisions
  recorded before it to finish transmitting. Failures are logged quietly
  and never affect the local state. Consent state is in-memory only —
  integrators persist and re-apply it across restarts. Consent never rides
  the event envelope.
- Added optional `Config.UserID` / `Config.AnonymousID` default actor
  identity fields: used as envelope defaults for events that do not set
  their own identity, and as the consent `actor_identifier` (user ID
  preferred, else anonymous ID).
- Internal: extracted the UUIDv7 generator shared by crash IDs, anonymous
  IDs, and consent idempotency keys into `internal/uuidv7` (behavior
  unchanged).

## v0.3.0-alpha — 2026-06-07 — universal envelope

- BREAKING: Removed the game-flavored `MatchID` field from the universal
  `Event` envelope. ShardPilot is a universal multi-tenant analytics platform;
  domain-specific context does not belong on the universal envelope.
- Migration: move any `Event.MatchID` usage into the existing `Props` map as
  `Props["match_id"]` (it is serialized to `props.match_id`, exactly as before).
  No other behavior changes — the wire payload is unchanged when you set
  `Props["match_id"]`.

  ```go
  // before
  client.Track(ctx, shardpilot.Event{Name: "match_end", MatchID: "m-123"})

  // after
  client.Track(ctx, shardpilot.Event{Name: "match_end", Props: map[string]any{"match_id": "m-123"}})
  ```

- This is an early alpha pre-release. The API is unstable and may change
  before v1. Released as the `v0.3.0-alpha` git tag.

## v0.2.0-alpha — 2026-05-24 — crash SDK alpha

- Adds `pkg/crash` with typed crash event types, UUIDv7 crash IDs, sanitized
  breadcrumbs, no-PII scrubbing, fatal/non-fatal emit APIs, default non-fatal
  sampling, and a crash reporting example.
- Keeps the existing v0.1.x analytics API unchanged.
- This is an early alpha pre-release. The API is unstable and may change
  before v1.

## v0.1.2 — Go 1.24 modernization

- Bumped the `go` directive to 1.24 for Swiss Tables hash map performance and
  Go 1.24 language features such as generic type aliases. Module surface
  unchanged.
- Earlier 1.23-pinned consumers MUST upgrade their Go toolchain to 1.24+
  before pulling v0.1.2.

## v0.1.1 — 2026-05-23 — early alpha

- Documentation re-cut. CHANGELOG and README cleaned up; module surface unchanged from v0.1.0.
- v0.1.0 is retracted in this version's go.mod so consumers get a warning if they pin v0.1.0 directly.
- This is an early alpha pre-release. The API is unstable and may change before v1.

## v0.1.0 — 2026-05-23 — early alpha

- Covers app-first ingest envelopes for workspace, app, environment, event
  timestamp, and session sequence fields.
- Sends event batches to `/v1/events:batch` with bearer-token authorization.
- Supports synchronous `Track`, bounded async `Enqueue`, `Flush`, `Close`, and
  in-memory stats.
- Provides bounded batching, capped batch size, and retry handling for
  retryable HTTP responses.
- Includes a basic backend example and Go CI coverage for the compatibility
  baseline and current toolchain.
- This is an early alpha pre-release. The API is unstable and may change before v1.
