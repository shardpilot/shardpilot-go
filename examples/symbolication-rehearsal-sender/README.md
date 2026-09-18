# Symbolication rehearsal sender

Sends the controlled crashes that the symbolication rehearsal's manifest
describes, through the crash SDK's own native-address form, and prints for each
crash id the frame or status the owner reads back afterwards.

It **uploads no symbols and resolves nothing**. Run the manifest's own upload
command for a row first; an accepted crash is not a symbolicated crash, and the
resolved frame is read back from the product, never from a reply this sender
sees. Run it only against an environment you are authorized to test, with an
isolated test app and actor. It neither creates an account nor obtains
credentials.

## The manifest

`cmd/symbolication-rehearsal` in `shardpilot/crash-symbolicator` produces the
artefacts and writes the manifest. This sender reads, per row: `platform`,
`format`, `module_name`, `debug_id`, `load_address`, `instruction_address`,
`symbolication_exercised`, `expected_frame`, `note` and `negative_twins`. A
field it does not model is ignored — the producer may add one, and refusing the
manifest on run day over a new receipt field would cost the whole step. A
**twin** it does not recognise fails the run naming it, because a twin is a
crash body this sender has to be able to spell, and a silently skipped twin is a
crash the owner waits for and never receives.

## Variables

Five, from the process environment only. There are no flags and positional
arguments are refused; the credential never travels in argv.

| Variable | Required value |
|---|---|
| `SHARDPILOT_CRASH_INGEST_URL` | Crash ingest origin, with no path/query/fragment. HTTPS outside loopback. |
| `SHARDPILOT_API_KEY` | In-memory API key with `crash:write` for the test app. |
| `SHARDPILOT_APP_ID` | Canonical app key the crash key scopes. |
| `SHARDPILOT_ANONYMOUS_ID` | A dedicated synthetic actor identifier, accepted unchanged by the crash SDK sanitizer. |
| `SHARDPILOT_REHEARSAL_MANIFEST` | Path to the manifest the producer wrote. Not a credential. |

The protocol witness's other four variables (`SHARDPILOT_INGEST_URL`,
`SHARDPILOT_TOKEN`, `SHARDPILOT_WORKSPACE_ID`, `SHARDPILOT_ENVIRONMENT_ID`) are
**not** required and not read: this sender never touches the analytics door, and
demanding them would be a false precondition.

## Running it

```sh
go build -trimpath -o /tmp/shardpilot-symbolication-rehearsal ./examples/symbolication-rehearsal-sender
/tmp/shardpilot-symbolication-rehearsal
```

Per identified row it sends the positive crash — the row's module identity,
base and instruction address, one crashed thread, one native frame — and then
each negative twin the manifest promises **that the SDK can express**. Each send
gets one HTTP attempt: a per-case request budget refuses an SDK retry, so a
rehearsal probe cannot become a second crash under a new id.

A row must be whole. A row with **no** module identity is the artefact-less row
and is printed as not exercised — but only when it still names the platform and
format it stands for, since a row naming neither is a hole in the manifest and
printing it as "not exercised" would present the hole as a decision; a row carrying *some* of it — a name without a
debug id, a base without an address — fails the run naming it, because its
crash can be neither sent nor honestly skipped. A row that claims a resolved
frame must name a **complete** one (function, file and line): an empty
`expected_frame` is a promise with nothing in it, and there would be nothing to
compare on run day. A row that does not claim one never gets `resolved` as its
expected status, and a row that names a frame while claiming to exercise no
symbolication fails too: nothing would compare it. An artefact-less row may not
claim `symbolication_exercised` or name a frame at all. A twin that names no
status to read back fails the run rather than printing `expect_status: ""` — a
readback row with nothing to compare is the same silence this sender exists to
remove — and the twin the SDK refuses is only accounted for when the manifest
promises the ingest's rejection contract for it (`rejected`, `400`, not
symbolicated); any other promise means the producer and this sender have
drifted, which fails the run instead of printing a refusal that describes the
wrong thing.

The twin whose frame resolves against no declared module range **is** sent, and
the spelling matters: the SDK requires a frame carrying an address to name its
module with a NONEMPTY selector, not with one that resolves. So the twin
declares two modules with disjoint ranges, neither containing the address, and
names a module id none of them has. The server's module match then finds
nothing by id, falls through to range containment, finds nothing there either,
and has no lone module to fall back to — which is the `module_missing` the
manifest predicts. Clearing the selector instead (the spelling the rehearsal's
own handler test uses, since it posts the body directly) is refused by the SDK
before any request; a scene asserts exactly that.

**One** twin is NOT EXERCISED, and the sender proves it rather than asserting
it: it builds the event and calls `EmitFatal`, which refuses it before any
request.

| Twin | Why there is no send |
|---|---|
| the module declares no `load_address` and no `base_address` | `pkg/crash/event.go` requires one of them on every module. |

That refusal is the SDK failing closed on an input the ingest also rejects, so
**a client built on this SDK cannot produce that shape at all**; the ingest
contract for it (`400 rejected`) stays exercised by the symbolicator's own
handler test against the real route. The row with no produced artefact (the PDB
leg) is printed the same way.

## What it prints

Newline-delimited JSON, one line per attempted exchange with `case`, `crash_id`,
`method`, `route`, `status`, `response_body`, `request_id` and latency; then a
`readback` line per crash id carrying `platform`, `expect_symbolicated`,
`expect_status` and — for a positive — `expect_frame`, so the run-day comparison
against the product is mechanical; a `not-exercised` line per absent row or
refused twin, with the SDK site and its refusal error; and a final `summary`.
The configured credential is redacted from every printed body, header and error
— including an SDK or transport error that quotes what it was handed — through
the same redactor the protocol witness uses (`internal/redact`), which compares
the credential against the **decoded** views of what is printed: JSON escapes
(`\u002f`), percent-encoding in either hex case (`%2f`, `%2F`) and `+` for a
space in a query. There is one implementation of that equivalence set, not one
per sender. `Authorization` is never printed.

The evidence stream is checked as it is written, not at the end. If the banner
cannot be written, **nothing is sent**: a mutation with no receipt is the one
outcome this sender must never produce. If it breaks mid-run, the sends stop
there — the crashes already sent have receipts, the rest would not — and the
run exits 1. That includes a failed **readback** write: the crash it describes
has already happened, so the row's remaining twins are not sent on top of an
incomplete receipt.

An acknowledgement is read exactly as the protocol witness reads one: one
observed exchange, the expected status, the submitted crash id echoed back, a
fingerprint with non-whitespace content, and no suppression. A `202` proves
none of storage, symbolication or product visibility.

The summary separates what **reached** the service from what it
**acknowledged**, because the two zeros mean opposite things to whoever runs
this next: `attempted: 0` can be repeated, while requests that arrived and
failed the acknowledgement may already have stored crashes, and the run says so
in those words rather than reporting that nothing was sent.

| Exit code | Meaning |
|---|---|
| `0` | Every send was acknowledged as expected and every unsendable row was printed. |
| `1` | A failed expectation, an unrecognised twin, a row that cannot be compared, or an evidence write failure. |
| `2` | Missing or malformed configuration, or a manifest that cannot be read. |

## What this sender does not model

- **A twin whose manifest promises an HTTP rejection and which the SDK can
  otherwise express.** The SDK reports such a refusal as a typed
  `*crash.HTTPStatusError` from `EmitFatal` rather than as an acknowledged
  exchange, and this sender treats every expressible twin's send as one that
  must be acknowledged. Today's manifest contains no such twin — the only
  HTTP-rejected twin (`no load_address and no base_address`) is refused by the
  SDK before any request, and is printed as not exercised — so the gap is a
  limit of the sender, not of the current rehearsal. A future twin promising,
  say, a `413` would need this sender taught to expect the typed error.

## Offline proof

```sh
go test ./examples/symbolication-rehearsal-sender
```

The scenes use an in-memory transport: no listener, no network. They cover the
positive path, the twins on the wire (each mutation is checked in the request
body, not just in the case name), the two measured SDK refusals, the readback
rows, and the false-success replies a green run must reject — a blank
fingerprint, an absent fingerprint, an id the door did not echo, a suppressed
acknowledgement, a refusal where an acceptance was expected, an unrecognised
twin, a row that claims a frame it does not name, an artefact-less row that
promises twins, and a credential echoed back in a response body.
