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

Two of the four twins are **NOT EXERCISED**, and the sender proves it rather
than asserting it: it builds each one and calls `EmitFatal`, which refuses the
event before any request.

| Twin | Why there is no send |
|---|---|
| the module declares no `load_address` and no `base_address` | `pkg/crash/event.go` requires one of them on every module. |
| the frame's address falls in no declared module's range | `pkg/crash/event.go` requires a frame carrying an address to name its module once more than one is declared — and naming one would make the module match by id and stop being this twin. |

Both refusals are the SDK failing closed on inputs the ingest also rejects, so
**a client built on this SDK cannot produce those two shapes at all**; the
ingest contracts for them (`400 rejected`, `module_missing`) stay exercised by
crash-symbolicator's own handler test against the real route. The row with no
produced artefact (the PDB leg) is printed the same way.

## What it prints

Newline-delimited JSON, one line per attempted exchange with `case`, `crash_id`,
`method`, `route`, `status`, `response_body`, `request_id` and latency; then a
`readback` line per crash id carrying `platform`, `expect_symbolicated`,
`expect_status` and — for a positive — `expect_frame`, so the run-day comparison
against the product is mechanical; a `not-exercised` line per absent row or
refused twin, with the SDK site and its refusal error; and a final `summary`.
The configured credential is redacted from every printed body, header and error
in its raw, JSON-escaped and percent-encoded forms; `Authorization` is never
printed.

An acknowledgement is read exactly as the protocol witness reads one: one
observed exchange, the expected status, the submitted crash id echoed back, a
fingerprint with non-whitespace content, and no suppression. A `202` proves
none of storage, symbolication or product visibility.

| Exit code | Meaning |
|---|---|
| `0` | Every send was acknowledged as expected and every unsendable row was printed. |
| `1` | A failed expectation, an unrecognised twin, a row that cannot be compared, or an evidence write failure. |
| `2` | Missing or malformed configuration, or a manifest that cannot be read. |

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
