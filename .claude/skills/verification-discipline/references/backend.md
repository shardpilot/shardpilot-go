# Backend checklist — Go services (Fiber v3, HTTP-only inter-service calls)

Adapted from akovalion/paranoid-qa `references/backend.md` (MIT), curated to
Go 1.26 / Fiber v3 / Postgres 18 (pgx) / ClickHouse / Redpanda (franz-go).
Every item's verdict needs an artifact: a curl capture, a psql or
clickhouse-client row, a log line, or `make ci` output.

## API and contracts

- **Methods/status codes:** correct method per operation (GET without side
  effects); `405` on unsupported; nonexistent route → `404` JSON, not `500`
  or HTML; `400` (validation) vs `422` (semantics); `401` vs `403`; `404` vs
  `403` (do not disclose existence); no `200` with an error body; `5xx` never
  for client errors, no stacktrace/SQL/paths in bodies. House rule: 4xx for
  permanent input errors, `503` for transient/backpressure — never 5xx for
  permanent errors (retries would loop). Evidence: curl captures of each
  branch you claim.
- **Schema/body:** validate against the repo's OpenAPI spec (types, required,
  enums, patterns); ISO 8601 with offset for times; UUIDs where the contract
  says UUID (UUIDv7 for time-ordered keys); `null` vs absent vs `""`;
  int64/overflow; IDs > 2^53 as strings for JS consumers; UTF-8 incl. emoji
  and NUL-byte rejection.
- **Malformed payload:** invalid JSON → `400` not `500`; empty body / `{}`;
  wrong `Content-Type`; oversized body → `413`; deep nesting bounded.
- **Unknown/extra fields:** fixed strict-vs-lenient strategy; an extra field
  must not mass-assign (`role`, `workspace_id`, `price`, `is_admin`).
- **Backward compatibility:** no removed/renamed fields, no narrowed types,
  no enum changes without a version; adding a required request field is
  breaking. Event envelopes: never break in place — breaking change = new
  major (in-code JSON Schema validation, no registry).
- **Pagination/sorting/filter:** limit=0/negative/over-cap; offset beyond
  range → empty list, not error; stability under concurrent insert/delete;
  sort by disallowed field → `400`/ignore (fix which); filter injection.
- **Idempotency:** repeated request with the same idempotency key = same
  result, no duplicate row/effect (verify with a SELECT, not the response);
  same key + different body = conflict; scope is `(workspace_id,
  idempotency_key)` where the contract says so; retry after timeout without
  double effect. Read-only doors (exports) are run-scoped or unkeyed —
  never subject-scoped.
- **Headers:** `Content-Type`; CORS posture; no `Server`/version leaks;
  correlation ID accepted and returned, and present in the log lines you
  cite as evidence.

## Postgres 18 (pgx)

- **CRUD persistence:** written = returned = actually in the DB — always
  confirm with a `psql` SELECT, never just the API response. Update touches
  only what was specified (`updated_at` moves, `created_at` doesn't).
- **RLS/tenancy:** every tenant-scoped query runs under a transaction-local
  `SET LOCAL app.workspace_id` and is fail-closed — verify a query WITHOUT
  the setting returns nothing/errors, and that workspace A cannot read B
  (two-workspace probe with rows as evidence). Probe as the confined runtime
  role (`NOSUPERUSER`/`NOBYPASSRLS`): superusers and `BYPASSRLS` roles always
  bypass RLS, and a table owner bypasses it too unless the table sets
  `FORCE ROW LEVEL SECURITY`. Tenant comes from the token,
  never from a client-supplied parameter.
- **Integrity:** NOT NULL → error, not a silent default; UNIQUE incl.
  composite/case/trailing-space; FK rejects orphan writes; cascades don't
  mass-delete unexpectedly.
- **Transactions/concurrency:** mid-failure → full rollback (check the DB,
  not the logs); check-then-insert races → duplicates (needs
  UNIQUE + `ON CONFLICT`); concurrent decrement never goes negative;
  advisory locks (`pg_advisory_xact_lock`) actually held — and the lock/key
  material contains the bytes you think it does (Postgres rejects NUL bytes
  in text; a `\x00`-separated key built in Go must be probed live, not
  assumed).
- **Migrations (dbmigrate conventions):** forward on clean AND populated DB;
  immutable once applied — editing a merged migration is a hard error at
  apply time, at verify time, and in CI (SHA-256 checksums); compatible with
  the previous code version (expand/contract); schema drift check.
- **Money/time:** DECIMAL or integer minor units, never float; single
  rounding rule; storage in UTC `timestamptz`; DST and day-boundary probes.

## ClickHouse

- **Read-your-write is NOT immediate:** inserts land in parts; dedup/replace
  (ReplacingMergeTree) applies at merge time. A verification count uses
  `FINAL` or `GROUP BY` the business key — a raw count mismatch is not yet a
  bug, and a raw count match is not yet a pass. Poll until a condition, never
  a fixed sleep.
- **Workspace isolation:** `workspace_id` is the leading ORDER BY column and
  every query is workspace-scoped via the workspace-aware helper — probe
  cross-workspace reads with two workspaces and cite the rows.
- **Envelope → row mapping:** send a known event through ingest → Redpanda →
  worker → ClickHouse and SELECT the row; field-by-field compare against the
  envelope you sent. This is the only artifact that proves the pipeline.
- **Aggregates:** recompute one aggregate by hand from raw rows and compare
  with the served value; TZ of date columns (`event_date` is UTC).
- **Erasure/retention claims:** verify rows are actually gone with a SELECT
  per table, not by trusting the job's success status.

## Redpanda / franz-go

- **Producer:** message published after the business transaction commits
  (kill between commit and publish if you claim outbox semantics); correct
  topic/partition key (one business key → one partition if ordering is
  claimed); schema/version header present.
- **Consumer:** at-least-once means redelivery happens — force a redelivery
  and verify no duplicate row/side effect (idempotent consumer); invalid
  payload doesn't crash the consumer; unknown version → skip or DLQ, observed.
- **DLQ/poison:** an always-failing message ends in DLQ with payload +
  reason + attempt count, and does not block the partition; retries are
  bounded with backoff.
- **Lag/eventual consistency:** test by polling-until-condition with a
  deadline; record the observed convergence time in the report.

## AuthN/AuthZ

- **Token validation runs live:** expired → `401`; forged/corrupted →
  `401`; missing → `401`; token from another environment/audience rejected.
  If validation involves a JWKS fetch, prove the fetch path executes in the
  environment under test (a dev bypass/hatch that skips it makes every
  downstream auth "pass" vacuous — this exact hatch shipped once).
- **AuthZ on EVERY endpoint,** not only the gateway: probe the endpoint
  directly with a lesser role and cite the `403`; `401` (unauthenticated) vs
  `403` (unauthorized) distinct; internal `/internal/v1/*` doors demand the
  service-credential bearer and reject without it.
- **IDOR/BOLA:** another workspace's resource ID in path/query/body →
  `403`/`404`, on read AND write; nested resources checked at every level;
  a UUID is not authorization.
- **Identifier discipline:** endpoints that take an ID must reject a slug
  (and vice versa) — a lookup that "succeeds" by matching nothing can read
  as a clean apply (this exact bug shipped once); hashed player identifiers
  match `^[a-f0-9]{64}$`, raw player IDs never cross services.
- **Rate limits/enumeration:** login/reset limits per account and per IP;
  identical error + timing for existing vs missing accounts.

## Resilience (when the change touches it)

- Timeouts on all outbound HTTP calls; retries only for idempotent calls,
  with backoff — probe by injecting a slow/failing dependency, don't assert
  from config.
- Dependency down: DB / ClickHouse / Redpanda / sibling service unavailable →
  clean error or degradation, readiness reflects it; recovery reconnects
  without restart.
- Health probes: liveness independent of dependencies; readiness pulls the
  instance on degradation — observe the transition, don't read the handler.
- Graceful shutdown: SIGTERM drains in-flight requests — send one long
  request, signal, verify it completes.

## Gate

`make ci` (or the repo's aggregate; shardpilot-go has no Makefile — its gate
is `gofmt -l $(git ls-files '*.go')` printing nothing, then `go test ./...` +
`go vet ./...`, as its CI does) run locally from the worktree after
committing, output attached. Formatting via `go fmt ./...`. A green run
is the artifact; "it should pass" is not.
