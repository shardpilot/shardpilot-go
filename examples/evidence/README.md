# SDK evidence sender

This command writes synthetic telemetry using the real analytics and crash SDKs.
Run it only against an environment you are authorized to test, with an isolated
test app and actor. It neither creates an account nor obtains credentials.

Set these environment variables through your normal secret-injection mechanism:

| Variable | Required value |
|---|---|
| `SHARDPILOT_INGEST_URL` | Analytics ingest origin, with no path/query/fragment. |
| `SHARDPILOT_TOKEN` | In-memory analytics ingest credential for the test scope. |
| `SHARDPILOT_WORKSPACE_ID` | Canonical workspace key for that credential. |
| `SHARDPILOT_APP_ID` | Canonical app key; also the app scoped by the crash key. |
| `SHARDPILOT_ENVIRONMENT_ID` | Canonical environment key for that credential. |
| `SHARDPILOT_ANONYMOUS_ID` | A dedicated synthetic actor identifier accepted unchanged by the crash SDK sanitizer. |
| `SHARDPILOT_CRASH_INGEST_URL` | Crash ingest origin, with no path/query/fragment. |
| `SHARDPILOT_API_KEY` | In-memory API key with `crash:write` for the test app. |

Do not put credentials in files, source, command arguments or shell history.
Outside loopback the SDK requires HTTPS. Both endpoints and both credentials
are required before any request is made; there is no default destination.
Record any required analytics and diagnostics consent for this test actor through
an authorized service path first. This sender does not grant consent, mint tokens
or turn on product flags. A publishable ingest key cannot record a consent grant.
The actor is checked before any request: raw identity prefixes, email/IP-shaped
values and identifiers over 512 bytes are refused. Every crash request must then
carry the exact configured actor; an omitted or changed actor fails the case.

From the repository root, compile, then run the binary without a pipe:

```sh
go build -trimpath -o /tmp/shardpilot-go-evidence ./examples/evidence
/tmp/shardpilot-go-evidence
```

The run uses seven cases, each allowing one HTTP attempt and refusing redirects:

1. One minimal `app.session_started` envelope with a synthetic session id.
2. A four-event batch: two synthetic sessions, each with a start and end, carrying
   entry-point, duration and completion/background context. Each session uses
   sequence 1 for its start and 2 for its end; standalone starts use sequence 1.
3. One event with a 3,072-byte padding property (its encoded envelope exceeds
   2,048 bytes) beside one small event **in the same batch**. The required result
   is HTTP 202, the large event `rejected` with `event_too_large`, and the small
   event `accepted`. This checks the 2,048-byte enforce posture; a different limit
   or log-only posture is a failed expectation, not proof of a server defect.
4. A valid SDK event whose Authorization header is removed at the transport
   boundary. Only HTTP 401 or 403 counts as the expected refusal.
5. A deliberately recovered Go panic through `CapturePanic`.
6. A synthetic native-address JSON crash with a module map through `EmitFatal`.
7. A synthetic text-stack JSON crash through `EmitFatal`.

The SDK supports Go panic capture and caller-supplied pre-symbolicated,
native-address and raw-text crash envelopes. The sender demonstrates those wire
forms; it does not crash the OS or capture a native signal. The Go panic provides
the pre-symbolicated case. There is no Go SDK ANR/hang detector or dedicated
minidump, Android tombstone or Unreal crash-context uploader here, and none is
simulated as though that platform capture existed. The default-off debug-id fill
and all-goroutine capture opt-ins stay off. The native fixture's module identity
has no matching uploaded symbols: successful ingestion does not certify
symbolication, symbol upload or object-storage reachability.

Every attempted exchange prints a JSON line with case, method, route, synthetic
request body/bytes, status, response body, request id (empty when absent), and
latency including the bounded body read. Configured credentials, including their
Go JSON-canonical, percent-encoded and JSON-Unicode forms, are redacted. JSON
comparisons decode Unicode (including surrogate pairs) and standard escapes;
percent comparisons accept mixed hex case, literal/escaped bytes and `+`/`%20`
spaces. Matches map back to original spans, preserving unrelated evidence bytes.
At most one JSON pass and one percent pass are composed, in either order. The
equivalence set is raw, JSON-canonical, percent and JSON-Unicode; arbitrary further
nesting or other encodings such as base64 are outside the sender's claim.
Authorization is never printed.
Response bodies are capped at 64 KiB; truncation/read failures fail the case.
The terminal case line includes SDK and expectation errors. The final line
summarizes all cases. Keep the run id and per-event/crash ids for later readback.

| Binary exit code | Meaning |
|---|---|
| `0` | All seven transport and response expectations passed. |
| `1` | At least one request, evidence write or expectation failed. |
| `2` | Missing/malformed configuration or failure to generate the run id. |

An HTTP 202 alone is insufficient: every analytics event must have exactly one
matching verdict. Observed-only, duplicate, suppressed, unknown and missing
verdicts fail the normal admission cases. Crash replies must echo the sent id,
carry a fingerprint with non-whitespace content and not be suppressed. A successful run still requires
separate Console/backend readback to establish storage, projection and visible
product behavior. It does not prove any endpoint that it did not call.
An oversized normal fixture still requires acceptance; `event_too_large` is a
passing rejection only in the deliberate mixed-size case.

`go test ./examples/evidence` uses an in-memory RoundTripper. It opens no listener
and makes no network request. It exercises accepted controls and false-success
responses, including oversize acceptance, unauthenticated acceptance, suppression,
missing/wrong event ids, crash failure and credential echo.
