# Wire conformance

`go test -v -count=1 -run '^TestConformance' .` verifies the complete public
`shardpilot/developers` corpus at `d5d4164efe19dcf30485a0723135f385b7d1e245`.
The unchanged 79-file `conformance/v1` tree lives in `testdata/conformance/v1`;
`source.json` binds its metadata and corpus hashes. Corpus SHA-256:
`bd63843c26a31f1c0ef7750352f983a3c8fe0321d1ce192e331fdb373fbe7340`.

Every one of the 45 case IDs produces a named request and response disposition.
An in-memory HTTP transport delivers captured status, approved headers and raw
response bytes to a fresh SDK client for each of 33 required cases. This proves
SDK response handling, not a live receiver or acceptance of a substituted request.

Request PASS covers the method, complete target, all seven approved headers,
credential kind and raw body. It applies to the eleven publishable-key Remote
Config cases. The credential-free 401 request is UNSUPPORTED: Go requires a
publishable key; its captured refusal is still handled. Arbitrary other HTTP
headers, credential values and receiver-side storage are outside this claim.

The other 21 requests are UNSUPPORTED, with response handling still asserted:

- Eight batch cases: public intake reserves `app.*` event names. Valid probe names
  preserve case event IDs, identity, session and properties. The SDK stamps ingest
  time; malformed JSON and retired scope members cannot be emitted.
- Eight consent cases: receipt times and keys are generated; actor kind follows
  configuration. Client JWT credentials and a forged client actor kind cannot be
  represented. Actual receipt fields and decoded outcomes are checked.
- Five crash cases: intake requires UUIDv7 IDs and module load addresses. Valid
  probes retain the captured anonymous identity; retired and claimed-user fields
  are omitted. The SDK's decoded result or typed refusal is checked.

Six resolver cases are parser-only: the complete parsed plan is compared with
the captured body, and refusals retain `resolver_refused`. No resolver HTTP API
is claimed. Six non-objection cases are N/A because no recording API exists.

ETag cases first seed a fresh client's cache through a separate HTTP delivery;
that setup is excluded from the asserted single case delivery. Identity cases
seed granted consent locally. No sockets, ports, production access or external
services are required. The default full Go tests and vet remain CI gates; both
Go 1.25.x and 1.27.x also print the conformance report.
