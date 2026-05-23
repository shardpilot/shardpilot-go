# Changelog

## v0.1.1 — 2026-05-23 — early alpha

- Documentation re-cut. CHANGELOG and README cleaned up; module surface unchanged from v0.1.0.
- v0.1.0 is retracted in this version's go.mod so consumers get a warning if they pin v0.1.0 directly.
- This is an early alpha pre-release. The API is unstable and may change before v1.

## v0.1.0 — 2026-05-23 — Phase 1 alpha

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
