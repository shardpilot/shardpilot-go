# Cross-cutting checklist — UI ↔ backend, errors, sessions, time, files

Adapted from akovalion/paranoid-qa `references/cross-cutting.md` (MIT),
curated to the ShardPilot stack (console/admin-console over Fiber v3
services, Postgres + ClickHouse behind).

## Error and network branches

- Cover every status branch the UI can receive: 400 (validation → mapped to
  fields), 401 (redirect to login with return URL), 403, 404, 409, 422, 429,
  500, 503 — clear message, no `undefined`/`[object Object]`, no stacktrace,
  no double display (toast + inline). Evidence: screenshot per branch; drive
  branches via a mock/dev server, request interception in unit specs, or by
  provoking the real service.
- Body off contract: HTML instead of JSON, truncated JSON, empty body with
  200 — UI degrades without a white screen.
- Timeout: no eternal spinner. Slow network: loading states appear.
- Retry: only retryable errors retried, with backoff, and idempotent —
  Network capture shows no duplicate effect.

## Payload inspection (rule 4 in practice)

For every user action that writes:

- Capture the actual HTTP request (method/URL/query/headers/body) from
  DevTools Network or the service's request log.
- Required fields carry real values; no `[object Object]`, no
  `"undefined"`/`"null"` strings, no empty strings where data was typed.
- Units correct: money in the contract's minor/major unit (a cents value in
  a dollars field passes every UI check); durations ms-vs-s; dates in ISO
  with the intended TZ, no ±1-day shift.
- Types per contract: `true` vs `"true"`, number vs numeric string; enums in
  the backend's casing.
- IDs vs slugs: the field named `*_id` carries the ID, not the
  human-readable slug (a slug that silently matches nothing has produced a
  false "applied" before).
- No sensitive data in query strings; masked-in-UI values sent in full
  intended format.
- One action = one request (no duplicates, no N+1 storm).

## UI ↔ backend consistency

- What the UI shows after an operation = what is actually stored: repeat the
  GET, and for write paths confirm with a psql/clickhouse-client row.
  Success labels must come from the response, not hardcoded.
- Optimistic updates roll back on 4xx/5xx.
- Counters/aggregates match a recount from raw data (mind ClickHouse
  merge-time dedup — use FINAL/GROUP BY when recounting).
- The same data on another screen agrees (no cache desync).
- Handler self-cancellation trap: a button that visibly reacts but whose
  final state ≠ its label's promise (a flag set then reset, an effect
  cancelling the first call, a branch that never runs). Verify the FINAL
  state across all layers — UI ↔ store ↔ network ↔ DB — not the mere fact of
  a reaction.

## Sessions and multi-tab

- Login sets the session per contract (cookie flags / token storage as the
  auth ADRs specify); logout invalidates server-side — replay a captured
  request after logout and cite the 401.
- Expiry mid-action: next action gets 401 → re-login flow preserves intent.
- Role change/downgrade in an active session takes effect (probe a
  now-forbidden endpoint).
- Multi-tab: logout in tab A is reflected in tab B; concurrent edit of the
  same entity does not silently lose an update.
- F5 on any page: restore or clean reset, no white screen; reload during an
  in-flight request does not duplicate a POST.

## Navigation and deep links

- Back/Forward restore state (filters/pagination/tabs); URL = content.
- Deep link to an entity/tab/filter opens directly; protected page without
  auth → login + return; nonexistent/deleted resource → 404/empty state.
- Manually injected invalid query params → defaults, not a crash.
- Workspace/app/environment context in the URL: switching context updates
  data everywhere (no stale-workspace leakage — check a data request's
  workspace scope in the payload capture).

## Time, TZ, locale

- Dates in user TZ vs server UTC; day/week boundary ("today" respects TZ);
  DST transitions; Feb 29; "N minutes ago" never in the future (clock skew).
- If the surface is localized (Transloco): switching locale changes all
  strings (no raw i18n keys on screen), date/number formats follow, long
  translations don't break layout.

## Files, export, search

- Upload: format by extension AND content; 0 bytes / at-limit / over-limit;
  odd filenames (unicode, very long); interruption; corrupted file.
- Export CSV/JSON: matches on-screen data (with active filters); UTF-8;
  CSV-injection guard (`=` `+` `-` `@` at cell start); empty and large
  datasets.
- Search/filters/sorting: empty query, 1 char, case, special characters;
  debounce + stale-request cancellation (race: slow first response must not
  overwrite the newer one — provoke it); filter combinations and reset;
  sorting stable, null values placed deliberately, persists across
  pagination.

## Feature flags and interaction

- Both flag branches exercised live — the off branch too; default when the
  flag source is unavailable is fail-secure; the flag is not the only
  authorization barrier (probe the endpoint directly with the flag off).
- New feature × adjacent features enabled simultaneously; regression pass
  over the blast radius of changed code, or an explicit deferral in the
  report's "Not covered".
