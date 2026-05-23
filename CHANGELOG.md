# Changelog

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
- This is a Phase 1 alpha milestone tied to ADR-0176. It is not a GA or 1.0 release.

## Unreleased

- Add v0 public-preview Go SDK source for app-first ShardPilot ingest.
- Add bounded async queue, batch transport, lifecycle, stats, tests, example,
  and CI.
