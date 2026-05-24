# Changelog

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

## Unreleased
