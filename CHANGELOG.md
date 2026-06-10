# Changelog

## Unreleased

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
  (0600 permissions, parent directories created as needed). The SDK never
  calls it implicitly and never writes files on its own.
- Added a minimal consent API: `Client.SetConsent(analyticsGranted bool)`
  and `Client.Consent()` with tri-state semantics {unknown, granted,
  denied}. Unknown leaves the pipeline fully open. Denied drops events at
  enqueue (`Track`/`Enqueue` return the new `ErrConsentDenied`) and clears
  the pending queue. An explicit decision is posted fire-and-forget to
  `POST {IngestURL}/v1/consent` with the batch transport credentials;
  failures are logged quietly and never affect the local state. Consent
  state is in-memory only — integrators persist and re-apply it across
  restarts. Consent never rides the event envelope.
- Added optional `Config.UserID` / `Config.AnonymousID` default actor
  identity fields: used as envelope defaults for events that do not set
  their own identity, and as the consent `actor_identifier` (user ID
  preferred, else anonymous ID).
- Internal: extracted the UUIDv7 generator shared by crash IDs, anonymous
  IDs, and consent idempotency keys into `internal/uuidv7` (behavior
  unchanged).

## v0.3.0-alpha — 2026-06-07 — universal envelope (proposed)

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
  before v1. Version bump is proposed (`v0.3.0-alpha`); the git tag is not yet
  created.

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
