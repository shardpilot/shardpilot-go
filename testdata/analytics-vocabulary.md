# Analytics vocabulary capture

`analytics-vocabulary.json` is a mechanical projection of 35 ingest-handler
observations captured on 2026-10-09 with synthetic events. `platform`, `source`
and `country` are request fields; `published` contains those three fields at the
publisher boundary. `status` and `code` come from the actual response. The capture
used the real request handler, authorization rules and event-schema validator,
with a recording publisher. The complete capture SHA-256 is
`53ba3be3217ea37b72f2d25705ad50f4a18270fb67f99b4abfd5712859dc4b49`.

The twelve `canonical/` rows supply the accepted platform values. Empty input
is accepted by the endpoint with an empty platform; the SDK uses the captured
canonical `other` value for its default. The source controls distinguish an
unknown source from a recognized source that disagrees with this client-only
event's schema. Country controls distinguish omitted/uppercase from lowercase
input. These records describe the captured handler's outcomes.
