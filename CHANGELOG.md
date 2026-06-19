# Changelog

## Unreleased

- Added `SignIngestJWT`: an optional, backend-only helper that mints a
  short-lived ADR-0222 Mode-B per-tenant ingest JWT (HS256) that the
  analytics-service Mode-B verifier accepts. A trusted Go game-backend can use
  it to mint the per-user tokens that client SDKs (Unity/Unreal/Defold) then
  fetch over the studio's own authenticated channel via their `token_provider`
  callback. The helper holds the per-tenant signing secret obtained out-of-band
  from control-plane (`SigningKey{KID, Secret}`, secret as raw `[]byte`), and
  signs a conformant token: header `alg=HS256` + `kid`; claims `iss`/`aud`/`sub`/
  `iat`/`exp`/`scope=analytics:ingest`/`workspace_id`/`app_id`/`environment_id`
  and optional `bind_anon`. Lifetime defaults to 5m — equal to the server's 5m
  iat-age window, which the verifier enforces regardless of `exp` (capped at the
  server's 15m max-lifetime). `iat` is stamped to now (fresh against that 5m
  window), and
  every input is validated at mint time so a token it returns is never rejected
  downstream for a malformed claim, an over-long subject/anon, or an over-long
  lifetime. Options: `WithIngestIssuer`/`WithIngestAudience`/`WithIngestLifetime`
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

- Adds `pkg/crash` with ADR-0191 event types, UUIDv7 crash IDs, sanitized
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
