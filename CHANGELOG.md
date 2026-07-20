# Changelog

## Unreleased

- Opt-in client-side consent floor (`Config.ConsentFloor`), adopting the engine SDKs'
  consent-first contract for integrations that need client-side enforcement (per the
  sdk-stability-1.0 disposition: user-facing adopters bound by the DPIA condition opt in;
  the DEFAULT — `ConsentFloor` nil — keeps this SDK's documented server-side posture
  byte-for-byte, proven by an explicit equivalence test plus the whole existing suite
  running floor-off). With the floor enabled:
  - Consent-first gating: `Track`/`Enqueue` refuse the new `ErrConsentUnknown` until an
    explicit decision is recorded (distinct from `ErrConsentDenied`); with `SpoolDir` the
    persisted decision reloads as the LIVE state at startup, and an undecided session
    transmits nothing at all. Persisted floor state is trusted only through a state
    directory whose privacy is established (the spool's ensurePrivateDir-first gate): a
    refused tighten starts the floor fail-closed — undecided, empty outbox, surfaced via
    `Stats.LastError` — with the on-disk files left for a run with fixed permissions.
    Decisions recorded after `Close` are memory-only IN FULL under the floor: no receipt
    is minted, retained, or persisted, and the local decision record is not rewritten
    (the next launch runs on the pre-`Close` state; record decisions before `Close`).
    The floor requires IN-CONTRACT identifiers: a non-empty `UserID`/`AnonymousID` over
    the 512-byte clamp rejects the decision whole with the new
    `ErrInvalidConsentIdentity` (reject, never truncate, never silently mint the receipt
    for a different actor than events carry — go's event path stamps configured
    identifiers verbatim, deliberately unclamped), and the same contract holds at
    RELOAD: a persisted decision for an out-of-contract configuration is refused
    fail-closed (undecided, diagnosed `consent_identity_invalid`). At reload the
    receipt trail's TAIL is the newer truth: when the decision record disagrees with
    the newest retained receipt (the record write was still owed when the previous
    process ended), the tail's decision governs fail-closed and the stale record is
    healed — a stale grant can never reopen the pipeline for an actor whose last
    decision was a denial. The override reads the latest IN-SCOPE receipt — scanned
    newest→oldest for the configured workspace/app/environment tuple and the configured
    actor — so a foreign receipt retained in a reused `SpoolDir` can neither flip this
    client's state, nor heal the record for a digest its decision never covered, nor
    HIDE this client's own latest decision by merely being newer — and it may override
    only when its decision is STRICTLY NEWER than the record's decision moment
    (`consent.json` now carries the decision's `decided_at`, the same instant its
    receipt carries): a STALE receipt left on disk by a failed prune rewrite — already
    acknowledged long ago — can never flip the state back over a record persisted for
    a newer decision. A failed HEAL registers as an owed record write exactly like a
    live decision's failure, holding the denial's proof receipt until the record
    lands. `consent.json` also carries FLOOR PROVENANCE: a granted record written
    without `Config.ConsentFloor` (the fire-and-forget era — its POST may have failed;
    no receipt exists) is never promoted to live floor state — the floor starts
    undecided, diagnosed `consent_record_unproven` — while denials are honored
    regardless of provenance (the fail-closed direction). A floor-confirmed granted
    record also reopens the spool's write gate at reload, so post-restart retriable
    failures and close remnants spool durably instead of dead-lettering until a fresh
    `SetConsent(true)`. The scope digest that
    keys `consent.json` now includes `AppID` (a reused directory across apps in one
    workspace/environment must not let another app's record — whose receipt was
    delivered for the other app's scope — become this app's live floor state); a record
    written by an earlier build reads as "no usable decision" and fails closed, the
    same treatment as any digest mismatch (this applies with the floor off too, where
    the record gates only the next launch's spool). The floor resolves BEFORE
    any spooled events load (construction ordering), and spooled events reload only
    under a grant the RESOLVED state confirms: a stale granted record whose operative
    decision is the trail tail's denial purges the spool instead of seeding resend
    work that would transmit pre-denial events. The floor covers the CONFIGURED
    identity: an event whose per-event `UserID`/`AnonymousID` override resolves to a
    different effective actor is refused at intake with the new
    `ErrConsentActorMismatch` (that actor has no local decision and no receipt;
    per-actor decisions beyond the configured identity stay on the server-side path —
    floor-off, overrides pass through unchanged). The disk spool's actor eligibility
    mirrors the same effective-actor rule under the floor: an event the floor ADMITTED
    (say a secondary AnonymousID override under a configured UserID) is never refused
    disk retention later; floor-off keeps the released strict both-identifiers rule.
  - Durable consent-receipt outbox (`consent-outbox.json` under `SpoolDir`; in-memory
    without it): exactly one receipt per explicit decision — an append-only trail, a later
    decision never withdraws an earlier receipt — 32-cap FIFO evicting oldest on save, no
    TTL, sanitize-on-load-and-save with a bounded record read, failed-write-never-evicts
    (the write is owed and retried at every dispatch point and at `Close`), strictly serial
    decision-order delivery retried until acknowledged, re-sent VERBATIM across restarts
    (same `idempotency_key`/`decided_at`; the server de-duplicates). Durable ordering
    per decision flavor (the engine SDKs' shared rule): a GRANT appends its receipt
    FIRST and writes the granted decision record only once the receipt trail is safely
    down — a crash can never leave a restored grant with an empty outbox flowing
    events receipt-less, and a failed receipt write WITHHOLDS the record (fail-closed
    across restart, healed from the trail tail once the owed write lands) — while a
    DENIAL keeps its record (and the spool purge it condemns) first. A decision-record
    write that fails (or is withheld) is OWED and retried at every dispatch point: the
    withheld grant record completes its receipt-first PAIR in the same pass the outbox
    write recovers — before the receipt can deliver and prune away the trail's only
    durable grant evidence — and while a DENIED record write is owed, the denial's
    in-scope proof receipt is HELD from dispatch entirely, so it can never be
    acknowledged and pruned while the stale pre-denial record would rule a restart
    (Close still completes over the durable held proof; the relaunch restores the
    denial from it and heals the record). A GRANT receipt dispatches only when its
    PAIR is fully durable: while the outbox write is owed (the grant's own failed
    append included), while the granted RECORD write is owed, or while a grant
    decision is still mid-persist, the dispatch pass holds — an acknowledgement
    followed by a crash would otherwise prune the only durable half and leave neither
    a receipt nor a granted record, losing the grant across restart though the server
    recorded it; the pass that recovers the writes completes the pair before the
    receipt can be acknowledged, and an owed GRANT record also pends `Close`
    (`ErrConsentPending`, retryable) so teardown never reads clean over an incomplete
    pair — and an owed DENIAL record pends `Close` too unless a durable in-scope
    proof receipt exists (the local-only path mints none: nothing durable would
    contradict the stale pre-denial record). The pair-incomplete hold is tracked
    PER RECEIPT, not only in the newest-decision owed slot: a newer denial whose
    record write also fails cannot release an earlier grant whose record never
    landed — the retained grant stays held instead of delivering and pruning
    while the deny proof is held (a grant must never become the server's last
    word against a local denial), and a successful record write (always the
    newest decision's) releases the whole trail in order. A failed
    idempotency-key MINT for a CONFIGURED actor is never the local-only path:
    the receipt is OWED — re-minted at every dispatch point with the original
    decision's stamp — a mint-failed GRANT withholds its record and holds the
    batch legs exactly like a failed append, earlier in-scope grants park
    behind a mint-owed denial, and `Close` pends (`ErrConsentPending`) until
    the owed receipt exists; only a client with NO configured identifiers
    persists a decision receipt-less. Decision stamps are MONOTONIC per
    client AND seeded at reload from the maximum persisted stamp (record and
    retained receipts), so same-tick decisions, backward-stepping clocks, and
    behind-clock restarts all mint strictly increasing `decided_at` values — the
    reload's strictly-newer rule always sees the newest decision. The reload heals
    from the proof even when the state string matches: a strictly newer in-scope
    receipt promotes an UNPROVEN same-state grant record to the floor-marked,
    receipt-stamped one instead of discarding a grant whose durable proof exists.
    The sanitizer also drops receipts whose `decided_at` does not parse (corrupt
    data must never become reload truth), and a FLOOR-MARKED `consent.json`
    whose `decided_at` is missing or unparsable reads as ABSENT, fail-closed:
    floor-authored records always carry the stamp the ordering rule runs on, so
    an unorderable one must not let a stale grant beat a durable newer deny
    receipt (legacy unmarked records keep loading stampless — their grants are
    already vetted by provenance, their denials honored). Foreign receipts retained in
    a reused `SpoolDir` are never dispatched with this client's scoped bearer — a
    terminal 401/403 would prune ANOTHER scope's consent receipt — they stay retained
    for a correctly scoped client while this client's own trail dispatches around
    them in order, and outbox rewrites RELOAD-AND-MERGE the on-disk record
    exactly like the disk spool's saves (the fresh disk view first, minus the
    keys this process settled, plus its own unsaved appends; de-duplicated by
    idempotency key, cap re-applied on the merged view): a sibling floor
    client's receipts appended to the shared directory after this process
    loaded are never clobbered by a mirror-only rewrite, and a receipt its
    owner pruned concurrently never resurrects from this process's stale
    mirror copy. Retryable outcomes
    (transport failure, `429`, any `5xx`) keep the receipt at the head and park the consent
    plane behind the server's `Retry-After` — parsed on `429` AND `5xx` — or jittered
    backoff, independent of the events plane's pacing; every other outcome is terminal and
    chains the next receipt (including `401`: this SDK's bearer is static for the client's
    lifetime, the engine SDKs' static-credential rule). Dispatches on caller-driven
    operations (`Track`, `Flush`, `Close`) are bounded by the sooner of the caller's
    context and `HTTPTimeout`, and a caller-aborted attempt is no outcome (nothing
    counted, no deferral armed, receipt retained). Consent gating never feeds the EVENTS
    plane's retry pacing. The consent route never decodes the response body: ANY `2xx`
    status is the acknowledgement (empty `200`, `204`, even a non-JSON body), while
    send-path and no-status errors stay retryable — on the floor path and the legacy
    fire-and-forget path alike. The dispatch gate releases only on an OBSERVED HTTP
    outcome: a grant whose POST failed with no response observed (connection refused,
    send-path EOF, a timeout before any status, a caller abort) stays unhanded and keeps
    holding the event legs — a batch must never be the server's first-seen request ahead
    of the grant; and a grant decision holds the event legs from the moment it is
    OBSERVABLE, before its receipt finishes appending (the arming window). Receipt
    delivery is permitted while consent is denied, on every dispatch point including an
    explicit `Flush` in a denied session; construction is a dispatch point too (reloaded
    receipts re-send promptly, not at the first flush tick). `Close` runs the consent
    drain whatever the event-plane outcome, folding both verdicts (`errors.Join`) so a
    terminal event error never masks the retryable `ErrConsentPending` state — and a
    floor client whose close remnant was neither delivered nor made durable reports the
    loss on every `Close` (`ErrEventsDiscarded`, counted in `Stats.Dropped`) instead of
    ever reading as a clean teardown: the memory-only discard, a remnant the spool's
    write gate refused, and a remnant still unpersisted after the final settle retry
    all fold the same way — counted PER EVENT through the mirror's unpersisted-entry
    tracking, so a remnant that merely de-duplicated against an earlier append whose
    save had failed counts exactly like a fresh dirty add. The discard fold is
    re-applied (idempotently) on every cached-`Close` return: a `Close` whose context
    expired before the worker's stop path finished counting cannot hide a loss
    counted after its verdict was cached.
  - Grant-receipt dispatch gate: while an analytics-grant receipt is retained undispatched
    (parked, queued, or reloaded after a relaunch), event legs hold — `Track`/`Flush`
    return the new `ErrConsentReceiptPending`, intake stays open — so post-grant events
    can never overtake the grant on the wire and be terminally `suppressed_no_consent` on
    a strict-consent workspace. Released on an OBSERVED HTTP outcome for the receipt —
    success or a status error, never gated on its acknowledgement (a receipt the server
    answered does not hold that cycle's batch even when the answer is a retryable
    failure), while a no-response failure keeps holding — and an empty pipeline is never
    gated, so a retained receipt alone cannot wedge teardown. Only IN-SCOPE grants arm
    the gate: a foreign grant parked in a reused `SpoolDir` keeps re-sending for its own
    historic scope but never holds this client's pipeline.
  - `SetConsentDecision(ConsentDecision)` with the forced-minor denial
    (`ConsentDecisionDeniedForcedMinor`): analytics-wise identical to denied everywhere —
    same gates, same `ErrConsentDenied`, full denial path — with the receipt carrying
    `reason: "denied_forced_minor"`; persisted as its own state and reloading as itself
    under the floor; superseded normally by a later decision (whose receipt carries no
    reason). Available without the floor too, where the reason rides the legacy
    fire-and-forget post. Invalid values reject `ErrInvalidConsentDecision` and apply
    nothing. An AC-8 whole-session test pins the forced-minor session shape: exactly one
    analytics-plane request (the receipt), zero event batches.
  - Teardown durability: `Close` completes only when every retained receipt is delivered
    or durably on disk; otherwise the new `ErrConsentPending` is returned and `Close`
    stays RETRYABLE (a repeated call re-runs the delivery/persist drain), so a consent
    decision's receipt is never silently lost by process exit.
  - Receipt-path identifier clamp: `UserID`/`AnonymousID` over 512 BYTES reject the
    decision whole (`ErrInvalidConsentIdentity` — never truncated, never a substitute
    actor) at decision time AND at reload; the outbox sanitizer drops oversized or
    malformed entries fail-safe. The anonymous-id retention snapshot never rides the
    wire.
  - Consent-plane observability on `Snapshot()`: `ConsentRecorded`, `ConsentFailed`,
    `ConsentOutboxEvicted`, `ConsentOutboxPersistFailed`, `LastConsentError`.

- Fleet-audit follow-ups on the GAP-075 spool and transport machinery:
  - Poison-member isolation on the worker publish paths: a batch member whose nested
    `Props`/`Context` values no longer serialize (mutated after `Enqueue`) is now dropped
    ALONE — attributed by event id in the log, counted `Dropped`, and folded into an explicit
    `Flush`'s first-error the way a terminal spool-chunk failure already is — and its
    batchmates publish on, where the whole batch (previously spooled copies included) used to
    be condemned for one member's mutation. The Close remnant spools its serializable members
    under the same rule instead of dying whole. The synchronous `Track` path is unchanged:
    single-event, the caller owns the error.
  - `Config.HTTPClient`: optional injection of the `*http.Client` behind every request the
    SDK makes (event batches, consent posts, remote-config fetches) for pooled transports,
    proxies, mTLS, or instrumentation. Nil — the default — keeps the SDK's internal clients
    exactly as before. Injection preserves two SDK contracts: every attempt is bounded by
    the SOONER of `HTTPTimeout` and the caller's context deadline through per-request
    contexts — an injected client without a `Timeout` of its own can no longer stretch an
    attempt to a longer caller deadline — and remote-config fetches still refuse redirects
    (the SDK derives its remote-config client from the injected one with `CheckRedirect`
    pinned, sharing the transport).
  - Crash-durability of state publishes: the atomic record write (spool record, consent
    record, remote-config cache) now fsyncs the parent directory after the rename, and the
    owed-wipe marker create does the same — a crash right after a write can no longer forget
    the publish. A failed directory sync reports as a failed write, so the
    mirror-authoritative retry rewrites and re-syncs. Skipped on Windows, where directories
    cannot be opened for syncing and NTFS journals metadata on its own.
  - `SetConsent`'s disk side (consent-record persist + spool purge) moved off the lock every
    `Track`/`Enqueue` takes: a slow `SpoolDir` write no longer stalls event intake for the
    duration of a consent decision, and a decision issued while an earlier decision's write
    is stalled takes effect on intake IMMEDIATELY (a denial rejects events from the moment
    it is issued, never from when the disk frees up). Overlapping decisions' disk writes and
    transmissions still settle strictly in decision order (the last decision's record lands
    last), and `Close` now waits — bounded by its context — for decisions admitted before it,
    so teardown can no longer strand an in-flight decision's transmission or abandon a
    denial's purge half-way.
  - Tests for the spool-load discard branches that had none: an unparseable `spool.json` and
    a wrong-version record each load as a clean start — file removed, no dead-letters, no
    counters, and no deferral seeded from an incompatible record's persisted deadline.

## v0.5.0-alpha — 2026-07-19 — remote config, disk spool, schema revision

- Remote config client (GAP-075): explicit `FetchRemoteConfig` plus never-fail typed getters
  (`RemoteConfigValue/String/Number/Bool/Values/Version`) over a durable last-known-good cache
  (`Config.RemoteConfigURL`, `Config.APIKey`, `Config.RemoteConfigCachePath`), scoped by
  (workspace, environment, client_id, base URL) with ETag/`If-None-Match` revalidation. The
  fetch authenticates with the publishable `APIKey` only — never `Config.Token` — and the
  failure taxonomy ports the Defold/Unity contract: a transient outcome (offline, `408`,
  `429`, `5xx`, malformed or over-1MB body) serves the cached snapshot flagged from-cache,
  `401`/`403` fail closed (the cache is kept but never served for that outcome), and every
  other status is a permanent failure that never masquerades as healthy — redirects are not
  followed on this route, so a `3xx` is classified as its permanent `http_3xx` self instead
  of surfacing the redirect target's body, and a truncated response still classifies by its
  status (a cut-short `401` fails closed; only a `200` needs its body, so a truncated `200`
  is the transient malformed class). A fetch ended by the caller's own context
  (cancellation or the caller's deadline) returns that context error with no cache fallback;
  only SDK-internal timeouts classify as the transient `http_0`. A fresh `200`, like a `304`,
  never overwrites a durable cache record another process refreshed with different content
  while the response was in flight. Fetching is not consent-gated, and fetches are fenced
  by the client lifecycle like synchronous `Track` publishes: a fetch that begins after
  `Close` fails `ErrClosed`, and `Close` waits (bounded by its context) for in-flight
  fetches, so no fetch I/O or durable cache write outlives it. Deliberate delta vs
  Defold/Unity: a `429`'s `Retry-After` (digits-only, floor 1s, clamp 24h) arms an in-memory
  cooldown (sequence-fenced like installs: a stale `429` landing after a newer fetch already
  settled an authoritative outcome does not arm it), and an explicit fetch inside it serves
  the cache without touching the network — the client half of the server-side remote-config
  fetch rate limit.

- Bounded disk spool (GAP-075; closes the long-standing follow-up in `queue.go`): opt-in via
  `Config.SpoolDir`; 2000 events / 1 MiB caps with oldest-first eviction, and the record is
  read back through a hard limit derived from the byte cap (an over-limit `spool.json` is
  discarded as corrupt, never loaded whole); verbatim single-stamp envelope records — the
  failed batch also retains its built wire bytes, so an in-process retry resends exactly
  what the failure spooled even when the caller mutates nested `Props`/`Context` values
  after handoff — so every resend is byte-identical and the server de-dupes by `event_id`;
  ack-removal on delivery and on terminal outcomes, with a resent chunk settling by the
  response's per-event verdicts: only confirmed deliveries count as resent, and a per-event
  `rejected` or consent-suppressed outcome dead-letters with the matching class; spooled
  chunks resend before fresh events through the same pacing gates, and the recovery wake
  after a success that ends a failure streak kicks requeued spooled chunks too, not only
  the held batch. A live server `Retry-After` deadline is persisted with the record and
  re-seeds the deferral across restarts (24h clamp): a `429` on a spooled resend writes the
  refreshed deadline through, ANY successful publish clears it, an empty record never
  carries a stale deadline, and a load that discards every saved event drops the deadline
  instead of gating fresh work on it. Retriable failures (`429`/`5xx`/network) spool,
  terminal outcomes never do; a 7-day retry-age cap expires records whose age is
  unprovable, too old, or future-dated; and `Config.OnSpoolDeadLetter` fires on every drop
  (capacity, expiry, terminal, consent) — a capacity eviction reports only once the rewrite
  that removed it from disk durably lands, never for an eviction a failed rewrite left
  reloadable. `Stats.Spooled` counts only durably written events. One client per `SpoolDir`
  is the supported topology; as a safety net every save
  reloads and merges the on-disk record by `event_id` (union minus this process's settled
  ids, caps re-applied oldest-drop — a cap drop at merge time settles the local mirror and
  dead-letters locally-owned entries), so a sibling writer's undelivered records are never
  silently dropped — last-writer-wins over a merged view, at-least-once on concurrent
  acks, surfaced via `Stats.SpoolForeignMerged`. Disk participation is strictly grant-only
  and requires a PERSISTED grant for writes and loads alike: enabling the spool persists
  each `SetConsent` decision — scoped to the actor by a SHA-256 digest of the configured
  (workspace, environment, `UserID`, `AnonymousID`) tuple, never verbatim identifiers, so
  one actor's grant never authorizes another's disk participation over a reused directory,
  and enforced per envelope: an event whose effective actor differs from that tuple (a
  per-event identity override) dead-letters instead of spooling — spool writes open only
  after the granted record is durably written (stricter than the Defold reference on this
  seam, deliberately), loads happen only from a persisted matching grant, any other state
  purges the record, and a failed purge owes a wipe that fails the spool closed
  (`spool_purge_failed`) until it succeeds. The live pipeline's open-by-default
  `ConsentUnknown` posture is unchanged.

- Added a customer-facing AI integration skill
  (`.claude/skills/shardpilot-go-integration/SKILL.md`): a source-verified brief of the
  pinned install tag, credential handling, this SDK's server-side consent posture
  (open-by-default under `unknown`, inverted vs the consent-first client SDKs), crash
  reporting, offline limitations, and a verify-your-integration checklist, so AI coding
  tools integrate the SDK contract-correctly. The README points to it from a new
  "AI-assisted integration" section, and `scripts/check_release_consistency.sh` now also
  fails when the skill's single pinned install command drifts from the README's
  latest-tag claims (release runbook updated accordingly).

- Every `events:batch` publish now declares the ingest envelope schema-set revision this
  SDK build was coordinated against via the `X-ShardPilot-Schema-Revision` request header
  (`DefaultSchemaRevision` — the digest of the ingest service's embedded schema set,
  currently pinned to analytics-service `main` @ `7d118c5`). The header rides on the batch
  route ONLY — the consent route never carries it — and is provably inert while the ingest
  service's schema-revision handshake runs in its default `off` mode (the header is neither
  read nor echoed there); it arms the server-side staleness alarm for the future `log` /
  `enforce` rollout. `Config.SchemaRevision` overrides the declared value and
  `Config.DisableSchemaRevision` stops declaring entirely (an undeclared revision always
  passes the handshake in every mode). An enforce-mode rejection — HTTP `409` with error
  code `schema_revision_mismatch`, discriminated by code since `409` is shared with the
  workspace-conflict codes — is terminal for the batch: it is dropped through the permanent-
  failure path with a dedicated log line, never retried (the server sends no `Retry-After`
  for it; re-sending the same batch from the same build can never succeed).

- Docs only: documented the strict-consent caveat for this SDK's open-by-default
  `ConsentUnknown` posture (README "Privacy & consent", plus the godoc on
  `ConsentUnknown`, `SetConsent`, `EventStatusSuppressedNoConsent`, and
  `Config.OnBatchResult`). On a workspace whose effective strict consent mode is
  `enforce`, events published for actors without an explicit consent decision
  recorded server-side are terminally suppressed per event (`suppressed_no_consent`
  inside the `202`), observable only via `OnBatchResult` or `Snapshot().ByStatus`.
  The docs now spell out that the grant must be recorded server-side before
  publishing (`SetConsent` posts fire-and-forget and covers only the configured
  actor — per-event `Event.UserID` actors need a service-path consent write) and
  recommend watching `OnBatchResult`/`Snapshot().ByStatus` for suppressions. No
  behavior change.

- Retryable batch publish failures **without** a `Retry-After` hint (server unreachable,
  `5xx` without the header) now fall back to client-side exponential backoff with full
  jitter instead of retrying at the fixed flush cadence indefinitely: the first failure
  still retries at the flush cadence, each further consecutive failure defers the next
  automatic attempt by a random wait in [1s, ceiling] with the ceiling doubling per
  failure up to 60s, and a successful publish resets the schedule. A server `Retry-After`
  hint still takes precedence exactly as before, explicit `Flush`/`Close` attempts remain
  ungated, and a consent denial that discards the held batch clears the backoff along
  with the deferral. This mirrors the shardpilot-defold reference semantics and removes
  the fixed ~1s fleet-wide retry storm during ingest outages.

- This is an early alpha pre-release. The API is unstable and may change
  before v1. Released as the `v0.5.0-alpha` git tag.

## v0.4.0-alpha — 2026-07-06 — consent, result callbacks, JWT mint

- The analytics client now parses the error envelope on non-2xx ingest responses and honors
  `Retry-After`. `HTTPStatusError` carries the server's machine-readable `ErrorCode`
  (e.g. `rate_limited`, `validation_error`), `ErrorMessage`, the per-field `Details` list,
  and the `Retry-After` header as a `RetryAfter` duration (both standard forms —
  delta-seconds and HTTP-date — parsed like the crash client; the analytics deferral
  clamps at 24h, while the crash client's short in-process retry loop keeps its own
  smaller bound) —
  `Error()` folds the code and up to five `field:code` detail pairs
  into the message, so logs show `status 429 (rate_limited) [events:events_rate_limited]`
  instead of a bare status. After a rate-limited automatic publish the background flush
  worker now defers its next automatic attempt until the `Retry-After` deadline passes
  (events keep buffering in the bounded queue meanwhile) and retries AT that deadline via a
  dedicated wake — not at the next flush tick, which could be much later when
  `FlushInterval` exceeds the hint. The server's latest hint wins (a fresh shorter value
  replaces an earlier longer deadline), and an explicit `Retry-After: 0` — "retry now" — is
  honored as an immediate retry (with a tiny anti-hot-loop spacing floor). Explicit `Flush`
  and `Close` attempts are not gated — they carry caller intent — a renewed failure re-arms
  the deferral, and a flush that leaves nothing retained (delivered or permanently dropped)
  clears any stale deadline so later events are never held behind it.

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
- This is an early alpha pre-release. The API is unstable and may change
  before v1. Released as the `v0.4.0-alpha` git tag.

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
