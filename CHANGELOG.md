# Changelog

## Unreleased

- **Typed resource verb: `TrackEconomyTx` / `EnqueueEconomyTx`.** An `EconomyTx`
  value builds the canonical `economy_tx` event — `direction` (`EconomySource` or
  `EconomySink`), `currency_type`, `reason` and a strictly positive integer
  `amount` required; `match_id` carried when supplied, omitted when empty;
  further properties carried through with the typed fields winning. The verb
  refuses an invalid transaction (`ErrInvalidEconomyTx`) and refuses outright on a
  client whose `Source` is not `backend` (`ErrBackendSourceRequired`), keeping the
  schema's backend pin. The schema's match-scoped rule for `match_id` is the
  ingest's to enforce, per event, inside an accepted batch — watch
  `Config.OnBatchResult`.
  `EconomyTx.EventID` is the idempotency key, forwarded to `Event.ID`, so a
  redelivered ledger entry repeats the id the fact layer collapses on.

- **Typed monetization verb: `TrackPurchase` / `EnqueuePurchase`.** A `Purchase`
  value builds the canonical `purchase` event — `props.amount`, `props.currency`
  and `props.product` required, `sku` and `quantity` optional, further
  properties carried through with the typed fields winning. The verb refuses a
  purchase missing a required field or carrying a non-finite amount
  (`ErrInvalidPurchase`), and refuses outright on a client whose `Source` is not
  `backend` (`ErrBackendSourceRequired`) rather than rewriting the event's
  source: the schema pins `purchase` to the backend lane, and this SDK keeps
  that pin. Everything else — consent, queueing, delivery — is `Track`'s and
  `Enqueue`'s, unchanged. `Purchase.EventID` is the idempotency key,
  forwarded to `Event.ID`, so a redelivered receipt repeats the id the fact
  layer collapses on; `Currency` is upper-cased to the ISO 4217 canonical
  form. The quickstart and `examples/basic` use it.

- **Documentation-only: internal ShardPilot material removed from the published tree.**
  No API, wire format or behaviour changes.

  This repository is public, so every tracked byte is published. Two internal agent skills
  under `.claude/skills/` were tracked here and described ShardPilot's own review process and
  backend stack; they are gone, and this note does not name them because naming them here
  would publish the same thing again. Internal decision-record ids, internal
  service names, an internal commit sha and internal deployment state have been removed from
  `README.md`, this changelog, `docs/release.md` and the customer integration skill — the
  engineering content each one annotated stays, restated so a reader outside ShardPilot can
  act on it.

  Three things this deliberately does NOT claim. It does not unpublish the history: removing
  a file at HEAD leaves every commit that carried it, and this repository has been public
  throughout. It does not cover Go doc comments, which still carry internal citations; that
  is owed work in another workstream and is not touched here. And it does not prevent a
  recurrence — a check that fails on internal material in the published surface is a separate
  change, deliberately kept out of this one so that removing what is exposed today does not
  wait on it.

- **An unreadable consent outbox no longer resurrects a superseded grant
  (privacy fix).** The durable outbox is this SDK's only cross-restart
  witness to a DENIAL. Its reader treated a record it could not parse the
  same as a record that parsed and held nothing: file unreadable, over the
  read limit, corrupt, or written by an unknown version all returned "no
  receipts". When `consent.json` still held an older GRANT — the state the
  SDK produces on its own, because a denial whose spool purge fails leaves
  the record write owed while the deny receipt is already on disk — the
  denial became invisible and the stale grant was promoted to live state.
  Consent read back as granted and events published.

  The reader now distinguishes three states instead of two. A MISSING file
  is honestly empty and behaves exactly as before — a fresh install has
  expressed nothing, and reading absence as refusal would disable analytics
  for every new install. A file that EXISTS and cannot be understood is
  UNKNOWN, and a persisted grant is not promoted on the strength of a
  witness we cannot read: the floor starts undecided, which every publish
  gate already treats as closed, and the host records a fresh decision to
  proceed. This is not "denied" as a finding about the person — nothing was
  learned about them — it is "not entitled to act". Denials are honored
  regardless, as they always were.

  `Stats.ConsentOutboxUnreadable` is new and counts loads that found an
  unreadable record; it was previously impossible to see this in the field,
  because every outbox diagnostic reported a WRITE. `LastConsentError`
  surfaces `consent_outbox_unreadable` for the read itself and
  `consent_grant_unwitnessed` when a grant is held back because of it.

  "Could not be understood" includes records that parse but are not sound:
  a missing or `null` receipts list (this SDK always writes `[]`, so neither
  can come from a healthy writer), and any record with an entry the
  sanitizer rejected — a rejected entry is a hole in the trail, and a
  superseding denial is exactly what it might have been. Entries that DID
  survive are still honored, so a readable denial alongside a malformed
  entry still denies rather than falling back to undecided.

  **The consent RECORD's reader had the same defect, and it was worse.** It
  collapsed every failure — an unreadable file, an over-limit read,
  unparseable JSON, an unknown decision value — into the same answer as "no
  record", and the reload treats that as "no record at all", which makes a
  retained grant receipt apply UNCONDITIONALLY. That branch then HEALS, so
  the unreadable decision was OVERWRITTEN with a floor-proven grant: the
  denial was destroyed, not merely ignored. The record reader is now
  three-valued too, and a grant receipt is refused whenever EITHER witness is
  unreadable. A grant whose stamp alone is unorderable stays a deliberate
  discard, not an opaque failure — its decision was read, so it hides
  nothing and a readable trail may still decide.

  The conclusion is DURABLE, because withholding a grant only in memory is
  undone by ordinary housekeeping: the trail can be pruned clean underneath
  it and the next start would promote the grant this one refused. A withheld
  grant is marked `unwitnessed` on the decision RECORD. A fresh decision for
  THIS scope rewrites its record and clears the mark, which is the way back.

  The record is NOT per-scope, and an earlier draft of this entry claimed it
  was: `consent.json` is one file per `SpoolDir`, STAMPED with an actor
  digest rather than keyed by one, and overwritten whole. So a sibling
  scope's decision — a login that sets a `UserID`, a tenant switch, a second
  app — erases this scope's mark, while that sibling's own merging outbox
  save sanitizes the unreadable trail away at the same moment. Both
  witnesses disappear together, and the refused grant would be promoted and
  healed to disk. A record carrying a DIFFERENT digest is therefore treated
  as its own tombstone: it records nothing about this scope, and it cannot
  be ruled out that it replaced a mark, so a retained grant receipt is
  refused beside it. Denials are unaffected.

  Whether a grant may take effect is decided ONCE at startup, from every
  signal at, and every grant-affecting path reads that one verdict — local
  promotion, the trail-tail heal, and the dispatch worker. The same asymmetry
  was found six separate times, each because one site consulted a subset of
  the signals and a grant took effect through the door that had not been
  widened; the last of them let a marked record's grant be POSTed to the
  server while local state correctly refused it. The verdict is persisted
  whatever the record says — including over a DENIAL, where an earlier
  revision wrote nothing, so a surviving deny receipt's prune could rewrite
  the outbox clean and let the next start's grant supersede the denial. A
  fresh explicit decision clears all of it at once.

  The mark carries a STAMP — the newest decision time the trail showed when it
  was applied — and is an ordering question rather than a veto. Eight review
  rounds widened *which paths must consult the mark*; the ninth found the guard
  too STRONG, which is a different signal: receipt-first means a fresh
  decision's receipt lands durably before the record is rewritten, so a crash
  in that window leaves a marked record beside a clean, strictly newer receipt.
  An unconditional mark refuses that receipt forever and loses a decision that
  was already durable. Anything strictly newer than the stamp is provably a
  decision the mark could not have been about, and supersedes it.

  **An absent stamp reads as infinitely NEW.** Records written before this
  field existed carry none, and reading absence as infinitely OLD would let
  every retained receipt clear the mark — reopening the defect on every
  upgraded client at once. Unstamped marks therefore keep behaving exactly as
  before: they block, and only a fresh decision clears them. The field is
  additive at the SAME record version, deliberately: a version bump would make
  existing records unreadable, and unreadable means unusable, which would
  withhold every persisted grant in the fleet on upgrade. An older build
  reading a newer record ignores the unknown key and sees the boolean alone —
  strictly more blocking, so safe in the direction that matters. This is local
  SDK state on the device's own disk; it is not part of any wire contract.

  Comparing stamps across restarts uses the device clock, so a backward jump
  yields a decision that is genuinely newer but does not compare newer, and the
  mark STICKS until the host records another decision. That is the safe
  direction and it is chosen rather than overlooked.

  Two reads also stopped treating a DANGLING SYMLINK as an absent file: `Open`
  returns ENOENT for both, but a dangling link leaves a directory entry
  pointing at a witness that cannot be read, and only `Lstat` separates them.

  The mark is honoured at every point a grant is trusted, because guarding
  one path and not another guards nothing: a retained grant receipt cannot
  override — and so heal away — a marked record; the ordinary state-only
  loader refuses a marked grant, so disabling `ConsentFloor` on a later run
  cannot turn an explicit "unprovable" back into authorization for the
  floor-off spool path (the full-info loader still returns it, because the
  floor must see the mark to withhold and to recover); and if the mark itself
  cannot be written, maintenance rewrites are held so the unreadable bytes
  survive as the remaining evidence rather than being sanitized away before
  the mark lands, while a FRESH explicit decision still writes through and
  lifts the hold — it is newer than anything the unreadable bytes could have
  held, and without that carve-out the documented recovery does not exist.

  A grant recovered from an unprovable trail is also held from the WIRE, not
  just from local state: dispatching it would leave the server granted for an
  actor whose rejected entry may be the very denial that made the trail
  unusable, and the server side outlives the device. It is deferred, not
  dropped — a fresh decision releases it. Denials are never held; sending a
  denial is the fail-closed direction.

  Finally, the mark is read out of the record BEFORE any other field can
  invalidate it, and the record's shape is checked before its identity. A
  damaged timestamp, an empty object, a JSON `null` or a missing
  `actor_digest` previously read as "some other scope's record" — honestly
  absent — which let a retained grant receipt apply unconditionally and heal
  over the file. Corruption anywhere in the record is not a way to launder a
  deliberate withholding.

  KNOWN RESIDUAL, named rather than left to be discovered: a DELETED outbox
  beside a persisted grant still promotes that grant. The file is not
  distinguishable from one that was never written, and refusing that
  combination would make every seeded or legacy proven grant start undecided
  and demand a fresh decision.

  Behaviour is unchanged for every other class of state: a corrupt cache or
  telemetry spool still means "start over", and a corrupt record still never
  crashes into the host.
- **Module `go` directive moves to 1.25 (was 1.24).** The source-compatibility
  baseline for SDK consumers rises with it: the next release requires
  **Go 1.25+**. Already-published tags are unaffected — every tag from `v0.1.2`
  onward declares `go 1.24` in its own immutable `go.mod` and keeps requiring
  Go 1.24+, as it always did. CI's matrix moves to `1.25.x` (the baseline) and
  `1.27.x` (the current toolchain).

  Stated honestly, because the reason it was taken did not survive
  measurement: 1.25 was chosen for `testing/synctest`, expecting it to retire
  the SDK suite's real-time waits. It does not, on the suite as written. The
  root package spends 28.0s, of which only 6.55s is literal `time.Sleep`; the
  rest is genuine machinery — 21 test files stand up an `httptest.NewServer`
  and there are 287 `t.TempDir()` calls, and a real listener and a real
  filesystem both defeat a fake clock. The piece that would make those
  convertible, `httptest.NewTestServer`'s in-memory network, is Go **1.27**,
  above a baseline this module holds below the current toolchain on purpose.
  So `synctest` is available for tests written against it from here on, and
  converting the existing retry-pacing cluster stays a separate question that
  a 1.25 baseline does not answer.

- **Goroutine-dump parsing tolerates Go 1.27 traceback labels.** For a consumer
  built with a `go 1.27+` directive the runtime appends the goroutine's
  `runtime/pprof` labels to each header:
  `goroutine 1 [running] {route: /v1/ingest, tenant: "acme-123"}:`.

  `parseGoroutineHeader` scanned to the LAST `]` on the line, so a label value
  containing `]` produced a corrupted state such as
  `select] {route: "/v1/items[id`, and the label block could otherwise ride
  into `Thread.Name` and out with the crash report. Label contents are the
  embedding application's own text and routinely name a tenant, a route or a
  user, so this is a privacy boundary and not only a parsing bug. The state is
  now bounded by its own bracket, which keeps labels out of every field the
  parser returns — and the raw dump text has never been carried into the
  payload. The trailing `:` is no longer required either, so a future runtime
  that moves the block would not silently drop every non-crashing thread.

- **`FlushInterval` now defaults to 15s (was 1s).** BEHAVIOUR CHANGE for integrations that
  never set it: a PARTIAL batch can now wait up to 15s before it is published, instead of up
  to 1s.

  It is **not** a heartbeat, and an earlier draft of this note said it was. An idle process
  makes no requests at either value: every tick reaches `publishWorkerBatch`, which returns
  without a request when there is no pending batch, spool resend, or consent receipt. What
  `1s` actually cost was batching itself — a process emitting an event every few seconds
  never reached `BatchSize` inside the window, so every event became its own request with its
  own TLS handshake. The reduction in requests is therefore proportional to the event rate,
  not a fixed four-per-minute.

  The batch-size trigger is untouched, so a busy process still publishes the moment it has a
  full `BatchSize`, and `Flush()` still publishes on demand. An explicit `FlushInterval`
  always wins, including one below the new default; this is a default, not a floor. Set
  `FlushInterval: time.Second` to keep the old behaviour.

- **Retry pacing no longer follows `FlushInterval`.** A retryable publish failure without a
  `Retry-After` hint now waits on its own clock — every failure, the first included, a
  full-jitter random wait in [1s, ceiling], the ceiling doubling from the third consecutive
  failure up to 60s, reset on success. Previously the first hint-less failure retried at the
  flush cadence, which with the new 15s default would have stretched a one-second retry to
  fifteen. Consent-receipt retries and retained-batch retries ride the same independent
  clock, and a server-sent `Retry-After` still binds ahead of it.

  Two places that clock could still be preempted by the flush tick are closed
  here: a consent-retry deadline that elapsed while the worker was busy with
  another select case (the wake arithmetic reports zero for "already passed"
  and "no deadline" alike, so the timer was disarmed), and a deadline armed by
  a concurrent synchronous `Track` after the worker had already parked with no
  wake source. Undelivered spool entries reloaded at startup are on the same
  clock: a restarted process with no enqueue and no explicit `Flush` no longer
  waits out a flush interval before retrying them.

- **Request bodies over 1 KiB are now gzip-compressed by default**
  (`Content-Encoding: gzip`), on batch publishes and consent writes alike. A batch body is
  the same envelope keys repeated per event, so it compresses hard: measured on a real
  100-event batch, 41.7 KB becomes 2.4 KB on the wire — 17x, for ~36 microseconds and 2
  allocations.

  **This is an upgrade-visible WIRE-FORMAT change.** Anything that inspects raw request
  bodies — a custom ingest deployment, a proxy, a logging sidecar, your own test servers —
  must decode gzip or set `Config.DisableRequestCompression: true`. Two of this repo's own
  test servers had to be updated for exactly that reason.

  Bodies under 1 KiB are sent uncompressed (gzip's 18 bytes of framing make a single-event
  batch bigger, not smaller), as is any body compression fails to shrink. The ingest body cap
  applies to the **uncompressed** body, so compression buys throughput and not headroom.

  A server that cannot read the coding is handled without data loss, but only through the
  documented detail codes: a `400` carrying `unsupported_content_encoding` or
  `invalid_content_encoding` latches compression off for the process and re-sends the same
  batch uncompressed. A deployment that refuses a compressed body some OTHER way — a bare
  400 with no envelope, a connection reset, a proxy 502 — does **not** reach that
  fallback; set `DisableRequestCompression` for those.

## v0.6.1-alpha - 2026-08-20

- **Removed two internal agent skills from the published artifact.** No API,
  wire-format or behaviour change: this tag is `v0.6.0-alpha` with eight files
  deleted and no Go source difference.

  They were not merely present in this repository's history. `go get
  github.com/shardpilot/shardpilot-go@v0.6.0-alpha` DELIVERED EIGHT OF THEIR
  FILES — the module zip is cached by the module proxy, so following the
  documented install command handed them out. (The `.claude/skills/` tree in
  that zip holds nine files; the ninth is the customer-facing integration
  skill, which stays.) One described an internal review
  process; the other published the backend stack with versions, the
  tenant-isolation mechanism in operational detail with a named runtime role,
  an inventory of internal repositories with their build commands, and
  statements about where automated coverage does not reach.

  **Forward-only, and the limit is worth stating.** `v0.6.0-alpha` stays
  reachable, its zip stays cached on the module proxy and its hash stays in the
  checksum database, where nothing ShardPilot does can withdraw it — deleting
  this repository would not. This stops new installs that follow the
  documentation; it recalls nothing.

## v0.6.0-alpha — 2026-08-03 — crash actor key, experiments, remote-config targeting

- Optional pseudonymous actor keys on crash reports: `Event.AnonymousID` / `Event.SessionID`,
  with `ClientOptions.AnonymousID` / `ClientOptions.SessionID` as client-wide defaults that a
  per-event value overrides (the same rule as `Source`). Both fields are `omitempty`, so a
  client that does not opt in keeps a byte-identical wire shape.
  - Setting an anonymous id is what lets a crash be measured against an active-user
    denominator, honour a per-subject diagnostics opt-out, and be reached by a per-subject
    erasure. It is hashed server-side; the raw value is not stored on the occurrence.
  - No default and no host-derived fallback. A backend process serves many players, so a
    process-wide identity would attribute every crash to one synthetic actor; prefer the
    per-event field, set from the request being served.
  - A raw account id is deliberately **not** carried: crash ingest authenticates with an API
    key, so a client-asserted account id is unverified and is never used as the actor key.
  - A malformed value (free text, email, IP, JWT, raw `user_`/`player_`/`device_` id, or over
    512 bytes) drops the field only — never the crash report.

- Dark phase-D crash-capture opt-ins in `pkg/crash` (both default `false` —
  while off zero new code paths execute and the auto-captured wire shape is byte-identical;
  enabling is gated by the service-side arming order on the SDK's consent gate + durable
  crash spool landing first):
  - `ClientOptions.DebugIDFillEnabled` — the running binary's self-module on every
    auto-captured event: executable base name + `debug_id` self-read from the binary (ELF GNU
    build-id as lowercase hex, else lowercase-hex SHA-256 of the Go build id — both scrubber-
    safe renderings; `printf %s "$(go tool buildid <binary>)" | sha256sum` reproduces the fallback), resolved
    once at `NewClient`, fail-open on non-ELF/unreadable binaries, `load_address` pinned to the
    schema-required `0x0` placeholder (frames stay function-only).
  - `ClientOptions.AllGoroutineCaptureEnabled` — all-goroutine snapshot at panic time
    (`runtime.Stack` all) parsed into additional pre-symbolicated `threads[]` (id
    `goroutine-<n>`, scheduler state as the thread name, leading runtime park frames trimmed,
    created-by spawn site kept) beside the precise crashed thread, bounded by the event caps
    (64 threads / 256 total frames, ≤16 frames per non-crashing goroutine).
- Dark opt-in experiment-assignment consumer (`Config.ExperimentsEnabled`, default `false` —
  while off ZERO experiment code paths execute and nothing new touches the wire or the disk;
  the same semantics the other ShardPilot SDKs carry, in Go idiom):
  - `FetchExperimentAssignment(ctx, key, attributes)` against the assignment
    endpoint (`RemoteConfigURL` host, path-swapped; publishable `APIKey` bearer): full verdict
    parsing (assigned / the three not-assigned shapes distinguished by `reason` — absent,
    `kill_switch`, `targeting_unmatched`; unknown reasons are malformed), strict verdict-shape
    validation (present positive `version`, non-empty keys, known unit member, echo-consistent
    `app_key`/`environment_key`/`experiment_key`), and the server-evaluated targeting attribute
    vocabulary (allowlist + `custom_attribute_*`, ≤512-byte values, 64-attribute cap, sorted,
    out-of-vocabulary dropped never sent).
  - The ratified failure canon on this plane: transients (offline/`408`/`429`/`5xx` incl. the
    kill-state `503`, malformed bodies) serve the durable last-known-good record; `401`/generic
    `403` fail closed per fetch, halt serving and the automatic lane until a later authorized
    fetch, and leave the durable record untouched; the real-subjects sentinel `403` additionally
    drops the record and its subject-fact keys durably; `404` and non-grammar `400`s drop the
    cached entry permanently; the subject-grammar `400` sentinel re-mints the subject once per
    process and retries with the exact normalized attribute set of the rejected request.
    `Retry-After` (429 and 5xx) paces the revalidation cadence only. Per-(scope, experiment)
    sequence fences with auth-epoch gating: stale in-flight responses install nothing, pace
    nothing, re-mint nothing, and their callers receive the SETTLED state, never the discarded
    variant. Durable writes converge to memory through owed-sync intent (writes cancelled by an
    ordinary latch, authoritative drops still land, whole-record clears demote to per-key drops
    the moment fresh state installs) with clock-rollback-proof stamps.
  - SDK-managed `spcid_` subject id (UUIDv7, dashes stripped, 32 hex; full wire grammar accepted
    on load for stickiness; no host override path), persisted under `SpoolDir` when set.
    Scope-stamped durable cache (workspace, app, environment, subject, base URL, and a
    non-secret credential fingerprint; corrupt = miss, bounded read-back).
  - Granted-only consent posture from the SAME effective consent state the analytics path uses
    (`ConsentFloor` composition included): denial — the forced-minor state included — closes the
    whole plane (zero experiment traffic on both planes); floor-off keeps this SDK's documented
    open-under-unknown posture; downgrades retain the cache unserved, re-grants re-serve.
  - Automatic revalidation every 300s ± 10% jitter over the cached assignments (the SDK-side
    kill-switch reach), with transport-parity backoff, a day-clamped `Retry-After` park, and a
    lane that never blocks `Close`.
  - Consent-gated `experiment_exposure`/`experiment_outcome` producers through the existing
    analytics pipeline: strict props allowlist, `sfk1_` subject-fact key verbatim as
    `assignment_key` (the raw subject id never egresses; no fact without one), `source: client`,
    `user_id` omitted, `anonymous_id` = the configured client identity; once per (experiment,
    version, subject) per client instance with DETERMINISTIC event ids (retries and purge
    re-emissions collapse server-side); `TrackExperimentExposure` re-arms an extra fact with a
    distinct id — while the automatic arm-0 emission is still owed, both facts emit; owed
    emissions ride bounded FIFO snapshots swept by the lane and once more at `Close` after the
    flush frees room, with owed durable syncs retried before teardown. NOTE: the producer lane
    is dark end-to-end today — the ingest service rejects these event names from publishable
    client keys until the lane is enabled for the workspace.
  - `ExperimentVariant`/`ExperimentVariantPayload` cached-variant getters (consent- and
    latch-gated, deep-copied payloads). New errors: `ErrExperimentsNotConfigured`,
    `ErrExperimentNoAssignment`, `ErrExperimentFactUnavailable`, `ErrInvalidExperimentFact`.
- Dark opt-in remote-config targeting-attribute pass-through
  (`Config.RemoteConfigAttributesEnabled`, default `false` — while off the fetch URL is
  byte-identical to today's attribute-less path and `SetRemoteConfigAttributes` is inert;
  the remote-config leg):
  - `SetRemoteConfigAttributes(map[string]string)` stores the client's targeting attribute
    set (`nil`/empty clears); enabled fetches append it to the `GET /config/v1/...` request
    as sorted, percent-escaped query parameters so server-side delivery rules can target
    this client. The vocabulary and bounds are the experiment consumer's, verbatim: `geo`,
    `app_version`, `device_type`, `install_date`, `user_segment`, `custom_attribute_<name>`;
    ≤512-byte values, 64-attribute cap; out-of-vocabulary keys dropped client-side, never
    sent. Targeting stays 100% server-evaluated.
  - PRIVACY CONTRACT: attributes ride ONLY while BOTH the opt-in is true AND consent is
    granted. Unknown consent and both denied states (forced-minor included) keep the fetch
    attribute-less — the fetch itself still happens (config delivery stays consent-neutral)
    and serves the untargeted defaults. Deliberately STRICTER than this SDK's
    open-under-unknown analytics posture: "unknown = zero bytes of personal data" holds on
    this leg. A consent downgrade strips attributes from the very next fetch.
  - Last-known-good caching stays scope-keyed for value serving (a cached body may reflect
    the previously sent attribute set until the next successful fetch — documented v1
    limit), and the cached ETag revalidates ONLY a fetch carrying the SAME attribute
    signature — a signature change (opt-in, set change, consent downgrade) forces a full
    fetch so a shared publication validator can never 304 a differently-targeted request
    into the previous target's body. The signature is an IN-MEMORY-ONLY equality token
    (never persisted — even a digest of the small targeting vocabulary is
    dictionary-guessable, so zero attribute-derived bytes go to disk), and an attributed
    record persists WITHOUT its ETag — a reloaded record could not prove a
    same-signature fetch — so the first fetch after a restart of a previously-attributed
    record is a full fetch on every leg. The setter is inert at the MEMORY
    level while the opt-in is off (nothing is retained), and the consent gate is read at
    the last moment before dispatch. Under the opt-in `ConsentFloor`, the grant-receipt
    dispatch gate holds this leg exactly like the event legs: while an analytics-grant
    receipt is retained undispatched, fetches stay attribute-less so attributes can never
    overtake the grant on the wire.
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
    actor, matching BOTH actor components (the wire identifier AND the retained
    AnonymousID metadata, mirroring the record digest's actor scope), so a reused
    `SpoolDir` with the same UserID but a different configured AnonymousID treats
    the old identity's receipts as foreign — so a foreign receipt retained in a reused `SpoolDir` can neither flip this
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
    work that would transmit pre-denial events — while a grant the trail proof
    RESOLVED whose record heal FAILED preserves and loads the spool (never purged
    and dead-lettered on the stale record read alone), with the write gate closed
    until the owed record write lands. The floor covers the CONFIGURED
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
    decision never withdraws an earlier receipt — 32-cap FIFO evicting oldest on save AND
    at load (an over-cap legacy record keeps its newest receipts, and the load-time trim
    OWES the durable rewrite so the trimmed record lands at the first dispatch point
    instead of re-evicting and re-counting on every restart), no
    TTL, sanitize-on-load-and-save with a bounded record read, failed-write-never-evicts
    (the write is owed and retried at every dispatch point and at `Close`), strictly serial
    decision-order delivery retried until acknowledged, re-sent VERBATIM across restarts
    (same `idempotency_key`/`decided_at`; the server de-duplicates). Durable ordering
    per decision flavor (the engine SDKs' shared rule): a GRANT appends its receipt
    FIRST and writes the granted decision record only once the receipt trail is safely
    down — a crash can never leave a restored grant with an empty outbox flowing
    events receipt-less, and a failed receipt write WITHHOLDS the record (fail-closed
    across restart, healed from the trail tail once the owed write lands) — while a
    DENIAL keeps its record (and the spool purge it condemns) first — and WITHIN
    the denial the RECORD lands before the purge destroys anything: purge-first
    would open a crash window where the spool is gone with no durable evidence
    of the denial yet, and a relaunch would promote the stale granted record
    over a destroyed spool; a failed denied-record write DEFERS the purge with
    it (the write gate is already closed and the live denial refuses intake),
    completing both in the owed-record retry pass the record lands. The
    deferred purge is a DEBT carried independently of the single owed-record
    slot: the MEMORY half runs immediately regardless (the condemned entries
    dead-letter at denial time, cleared from the mirror and resend queue and
    tombstoned against merging saves — condemnation is never disk-dependent),
    and the deferred record-FILE removal rides the durable wipe-owed marker
    — created under the SAME spool-mutex hold that sets the owed flag, so a
    concurrent settle (maintain, the append gate, a superseding grant) can
    never consume the flag inside the window and leave a late-created
    marker orphaned on disk to wipe a later grant's events at the next
    start — which a SUPERSEDING grant settles BEFORE its own record can
    reopen the spool and which a crash re-derives at the next start — a later grant whose
    successful record write clears the owed slot can no longer silently forget
    that a denial condemned the spooled events, and events a denial condemned
    never resend, whatever decision follows. The debt's DURABILITY is itself
    an invariant: the deferring branch returns only after EITHER the denied
    record, the wipe-owed marker, or the spool file's removal is durable —
    when the marker creation fails too it escalates in order (retry the
    denied record, which restores record-first and completes the purge; else
    remove the condemned spool file itself, durable by destruction — and
    destruction counts as durable only once the DIRECTORY entry change is
    fsynced (settleOwedWipe keeps the wipe owed on a failed sync: POSIX
    permits a crash to lose an un-synced unlink, and a resurrected
    spool.json under the stale granted record with no marker would reload
    the condemned events), with the unlinks strictly ORDERED — the record
    file first, a dir-sync, only then the marker, then a second sync — so
    the marker OUTLIVES the record it condemns (both-before-one-sync would
    let a crash persist the marker's unlink but not the record's: no
    marker, condemned file back, stale grant); else
    surface the failure with the in-memory condemnation holding and the
    owed-record retry re-deriving the debt at every dispatch point), because
    a crash with none of them would leave stale granted state plus the
    condemned file and NO marker — reloading the condemned events under the
    old grant, with no deny receipt ever coming on the actorless local-only
    path. A decision-record
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
    newest decision's) releases the whole trail in order. The rule GENERALIZES
    across the stale-grant family: an in-scope grant never dispatches past a
    PARKED newer in-scope denial, whatever parked it — the per-receipt owed
    mark, the owed mint (the receipt not yet in the trail), or the held deny
    proof — while a newer denial with no holds needs no park: the same serial
    pass delivers grant then denial in decision order. The handoff RE-CHECKS
    under the decision serialization point: immediately before the transport
    call a grant re-takes the record-apply lock opportunistically — a failed
    try means a decision is mid-flight (possibly appending that held denial)
    and the grant parks for the next pass, while a successful try re-runs the
    held-denial predicates against the settled trail, closing the window
    between the pass's hold checks and the post. A successful try ALSO
    consults the LIVE consent state: a denial's fast half flips the live
    state before any disk work, so in the window before its slow half
    appends the receipt the apply lock is free and no hold is visible in the
    trail — a grant with no newer in-scope denial behind it in the trail
    parks while the live state says denied, until the denial's evidence
    exists. An appended unheld denial then delivers in order BEHIND the
    grant (the normal grant-then-deny trail), while a stale grant receipt
    reloaded under a durably DENIED state simply stays retained — durable,
    never the server's last word past the denial, and a key-de-duplicated
    replay if it ever posts. A failed
    idempotency-key MINT for a CONFIGURED actor is never the local-only path:
    the receipt is OWED — re-minted at every dispatch point with the original
    decision's stamp — a mint-failed GRANT withholds its record and holds the
    batch legs exactly like a failed append, earlier in-scope grants park
    behind a mint-owed denial, and `Close` pends (`ErrConsentPending`) until
    the owed receipt exists; only a client with NO configured identifiers
    persists a decision receipt-less. The owed-record retry WAITS on the owed
    mint too: a mint-owed grant has no receipt anywhere, so the outbox owing
    no write proves nothing — writing the granted record at a dispatch point
    would make "granted record, no receipt ever minted" durable and a crash
    would promote the grant receipt-less — the record lands only after the
    retried mint appends the receipt, completing the pair receipt-first. Decision stamps are MONOTONIC per
    client AND seeded at reload from the maximum persisted stamp (record and
    retained receipts), so same-tick decisions, backward-stepping clocks, and
    behind-clock restarts all mint strictly increasing `decided_at` values — the
    reload's strictly-newer rule always sees the newest decision. The reload heals
    from the proof even when the state string matches: a strictly newer in-scope
    receipt promotes an UNPROVEN same-state grant record to the floor-marked,
    receipt-stamped one instead of discarding a grant whose durable proof exists.
    The sanitizer also drops receipts whose `decided_at` does not parse, whose
    `reason` is not the one value this SDK mints (`denied_forced_minor`, and
    only on DENIALS — a grant claiming it is a self-contradiction), or whose
    idempotency key duplicates an earlier entry (keep-FIRST: the ingest
    service de-duplicates by key and honors the first body it saw, so a later
    conflicting body could never take effect server-side) — corrupt data must
    never become reload truth. A FLOOR-MARKED `consent.json` whose
    `decided_at` is missing or unparsable fails closed PER FLAVOR: a corrupt
    GRANT reads as ABSENT (an unorderable grant must not beat a durable newer
    deny receipt), while a corrupt DENIAL is PRESERVED as
    denied-with-unknown-stamp — read as absent, a stale retained grant
    receipt would apply unconditionally and reopen the floor against a
    durable denial — and is never superseded by comparison. LEGACY unmarked
    records keep loading stampless, with the ordering rule they predate made
    explicit: a validly-stamped in-scope proof receipt supersedes a legacy
    record in BOTH directions (a denial proof heals denied over a legacy
    grant that provenance would otherwise strand as undecided — losing the
    denial — and a grant proof heals a floor-marked grant over a legacy
    denial); with no stamped proof retained, provenance vets legacy grants
    and legacy denials stay honored. Foreign receipts retained in
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
    counted, no deferral armed, receipt retained). Caller-driven DRAINS (`Flush`,
    `Close`) JOIN behind a concurrent dispatch pass — bounded by the caller's
    context — instead of silently skipping when the serial claim is held: losing
    the claim to a concurrent pass can no longer make a denied-path `Flush`
    report success over an undrained trail, and a join the caller's context cut
    short surfaces the caller's own error while receipts remain pending
    (automatic passes keep skipping — the work is being served). Consent gating never feeds the EVENTS
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
    terminal event error never masks the retryable `ErrConsentPending` state; a drain
    the CALLER's own context stopped — the join cut short, or an attempt aborted
    mid-flight — with receipt work remaining folds the caller's context error into
    the verdict too (on the first Close and on every retried one), so a
    deadline-bounded Close never reports bare `ErrConsentPending` — or a clean nil
    over durably-retained receipts — as if its delivery attempt had actually run to
    completion — and a
    floor client whose close remnant was neither delivered nor made durable reports the
    loss on every `Close` (`ErrEventsDiscarded`, counted in `Stats.Dropped`) instead of
    ever reading as a clean teardown: the memory-only discard, a remnant the spool's
    write gate refused, a remnant still unpersisted after the final settle retry,
    the close phase's settled CAPACITY evictions (a remnant overflowing
    `SpoolMaxEvents`/`SpoolMaxBytes` at exit is a permanent loss with no later
    resend — unlike a mid-session eviction — while a still-DEFERRED eviction
    stays on disk and reloads, the durable-eviction-deferral rule, so it is
    deliberately not counted; the deferral applies only to evictions with a
    durable stale copy — an eviction under a FAILED save of an entry that was
    never durably saved, a member of the failing batch itself or one accepted
    under an earlier failed write, has nothing the disk could undo and is a
    SETTLED loss returned and counted immediately, so a disk-full Close can
    no longer exit with such a member neither mirrored nor counted), remnant
    members past the RETRY-AGE cap (refused at
    the close append as too old to ever resend), and remnant members that could
    not SERIALIZE (poisoned — already counted `Dropped` at their settle, joining
    the verdict fold only) all fold the same way — counted PER EVENT through
    the mirror's unpersisted-entry tracking, so a remnant that merely
    de-duplicated against an earlier append whose
    save had failed counts exactly like a fresh dirty add; the retry-age
    discount removes exactly the EXPIRED COPIES (a per-entry multiset,
    never every entry sharing an id), so a fresh duplicate retained under
    the same event id as an expired stale copy still reaches the
    unpersisted-mirror fold instead of silently vanishing from a
    failed-save close. The discard fold is
    re-applied (idempotently) on every cached-`Close` return: a `Close` whose context
    expired before the worker's stop path finished counting cannot hide a loss
    counted after its verdict was cached — and a RETRIED `Close` (after an
    earlier one timed out pre-workerDone with consent pending) waits for the
    worker's stop path, bounded by its own context, before it can return nil:
    the caller never exits on a clean retried verdict while the close remnant
    is still being spooled or counted.
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

- Audit follow-ups on the spool and transport machinery:
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

- Remote config client: explicit `FetchRemoteConfig` plus never-fail typed getters
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

- Bounded disk spool (closes the long-standing follow-up in `queue.go`): opt-in via
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
  pinned to the schema set this SDK build was released against). The header rides on the batch
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
