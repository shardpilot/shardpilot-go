# Common misses — final self-check before the report

Adapted from akovalion/paranoid-qa `references/common-misses.md` (MIT),
curated to the ShardPilot stack. Walk this list last, before producing any
verification report. Anything you now realize you skipped goes into
"Not covered" — honestly, not silently.

- The path you are vouching for: has it executed at least once LIVE, with an
  artifact from that run? Green unit tests around it do not count (the
  mode-b doors lesson).
- Double click / double submit / button spam during loading — one request in
  the Network capture, idempotency on the backend.
- Browser Back/Forward, F5 mid-flow, direct deep link, new tab, manual URL
  edits.
- Session expiring mid-action (next action catches 401 → re-login preserving
  intent); logout in another tab.
- Request races: latest-wins on filter change, cancellation of stale
  requests; provoke the slow-first-response case.
- Handler self-cancellation: the button reacts but the final state ≠ the
  label's promise — verify final state across UI ↔ store ↔ network ↔ DB.
- Empty list / exactly 1 item / 1000+ items.
- Emoji (ZWJ) / RTL / special characters / surrogate pairs; grapheme
  clusters in length counters; NUL bytes anywhere a string reaches Postgres
  (rejected in text — including composed lock/advisory keys).
- Whitespace-only / NBSP / zero-width in a required field (= empty after
  trim).
- Zoom 200% without horizontal scroll.
- DST / timezones / Feb 29 / day boundary / midnight UTC vs local /
  "N minutes ago" never in the future.
- IDOR: another workspace's ID in the request while the button is hidden in
  the UI; vertical escalation (admin endpoint as a plain user); mass
  assignment (`role`, `workspace_id`, `is_admin` in the body).
- Slug where an ID belongs (and vice versa) — a lookup that matches nothing
  can read as a clean apply.
- Server-side validation with the client bypassed: replay the captured
  request with an edited payload; protection that lives only in the UI is
  not protection.
- Paste (trim/format stripping); autofill not firing live validation.
- Multi-tab (change in tab A → reaction in tab B); concurrent editing (lost
  update).
- Idempotency: a repeat after timeout/retry does not create a duplicate row
  or side effect — key per action, not per attempt; verify with a SELECT.
- Numbers > 2^53 (IDs as strings for JS); money never in float; minor-unit
  vs major-unit mix-ups in payloads.
- Empty array vs `null` vs missing field in a response — the client handles
  all three; unknown enum value → fallback, not a crash.
- Auth validation hatches: does token verification (incl. any JWKS fetch)
  actually execute in the environment under test, or does a dev bypass make
  every auth check pass vacuously?
- Postgres RLS: does the check run under the same `SET LOCAL
  app.workspace_id` posture as production code, and does the no-context case
  fail closed?
- ClickHouse counts: did you account for merge-time dedup (FINAL / GROUP BY)
  before calling a number right or wrong? Did you poll-until-condition
  instead of a fixed sleep?
- Consumer redelivery: forced a duplicate delivery and verified no duplicate
  effect; poison message lands in DLQ without blocking the partition.
- Double slash / trailing slash / case in URLs; open redirect (`?next=` to
  an external host).
- Optimistic update + backend error → rollback; partial failure of a
  composite operation → all steps rolled back (check the DB).
- Offline/killed connection at the moment of submit — data not lost,
  recovery without duplicates.
- Cache keys without workspace/locale/permissions in them (leaking another
  tenant's data via shared cache); stale cache after write.
- CSV injection on export (`=` `+` `-` `@`); uploaded file content vs
  extension.
- Cron/scheduled jobs during DST (skip or double run); overlap protection
  (distributed lock) observed, not assumed.
- The report itself: environment + commit SHAs + date at the top; every
  planned check has one of Pass / Fail / Not tested / Blocked; "Not covered"
  section present; flaky results reported as flaky, not Pass.
