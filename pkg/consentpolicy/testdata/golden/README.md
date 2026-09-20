# Golden bodies — the resolver's actual bytes

These are not hand-written fixtures. They are the **exact JSON the consent
policy handler emits**, recorded by that handler's own golden test.

| file | what it is |
|---|---|
| `consent-policy-resolved.json` | `200`, a resolved STRICT plan for a valid request — **the wire bytes**, as the handler writes them |
| `consent-policy-refusal.json` | `400`, a refusal carrying `reason: "invalid_scope"` — the wire bytes |
| `consent-policy-resolved.indented.json` | the same response in the **review form** the resolver's own repository stores |
| `consent-policy-refusal.indented.json` | the same, for the refusal |

## Provenance

Recorded from the resolver service's own golden test, from the stored bodies
`internal/httpserver/testdata/consent_policy_resolved_strict.json` (blob
`6a36fcc473fee009baffd9e51c3314b413e0cd12`) and
`internal/httpserver/testdata/consent_policy_refusal_invalid_scope.json` (blob
`b9eacf6f85079ec3a3c6db6b7f8e33b91f539c47`). Those two are the stored review
forms, and the `.indented.json` files here are byte-for-byte copies of them.

The resolved body answers the request `{workspace_id: ws_1, app_id: app_1,
environment_id: env_1, app_version: 1.2.3, store: steam, store_region: null,
locale: en-GB, platform: windows}`; the refusal is the same request with an
invalid `workspace_id`. The clock is fixed at `2026-09-20T12:00:00Z` — the only
seam, and why `expires_at` is `12:05:00Z` with `max_age_seconds` `300`. The
test fixtures refresh that one field and nothing else.

Everything else is the handler's own output, compared as **bytes** rather than
through a struct: a round trip through the type the handler marshalled from
would stay green through a renamed tag, a re-nesting, or a `null` where an
empty array belongs.

## The relation between the two forms, proved rather than asserted

The resolver's golden test re-indents each raw response and compares *that*
against the file it stores, applying the indentation to both sides. So
**indentation is the only transformation** between what goes over the wire and
what is stored for review.

Both forms are vendored here, and `TestTheReviewFormsCompactToTheWireBytes`
proves the relation instead of asking you to believe it: removing insignificant
whitespace from each `.indented.json` — **outside strings only, with no round
trip through a JSON library** — must reproduce the corresponding `.json` byte
for byte. Decoding and re-encoding would prove only that the two files *mean*
the same thing, which is the weaker claim and the one that let this SDK and the
resolver disagree for months. A key reordered, a number respelled or an escape
rewritten fails that scene.

## Why they exist

This SDK and that resolver were written from the same prose and never parsed
each other's bytes: the module validated a **flat** plan in upper case, with
three fields the resolver does not send, while the server answers a **nested**
one whose flag vocabulary is lower case. Every real response would have been
refused as unreadable. A schema in two places is a schema in neither — these
bytes are the one place both sides can be wrong against.

**Five differences worth naming**, because they are what this SDK had wrong:

1. `flags` is **nested**, and its vocabulary is lower case — `off`, `denied`,
   `minimised`. `regime` stays upper case; the two are not the same vocabulary
   and assuming they were is how the first repair attempt went wrong.
2. `operation_blocks` lives **inside** `flags`, and is `[]` — never null, never
   absent.
3. `"signature": null` is present on **every** response, refusals included. A
   closed validator must accept the key with a null value rather than treat it
   as absent, and must tell present-null from absent.
4. A refusal **does** carry every other key, with `scope` as three empty
   strings. What tells a refusal from a plan is the presence of `reason` — not
   the status, which is `400` for four reasons and `200` for
   `policy_unavailable`.
5. Versions carry a solidus: `strict-fallback/1`. The character set this module
   enforced was invented from prose and refused it, so every strict-fallback
   plan the resolver has ever issued would have been rejected as malformed.

## Whitespace is not a detail here

This module scans the **raw text** — for duplicate keys, unknown keys,
container types and required members — before it decodes anything, and JSON
permits insignificant whitespace between every pair of tokens. Until the review
forms were vendored, every golden scene ran on the compact spelling only, so
that scanner had never met a newline or an indent. A proxy that re-serialises,
a future encoder, or a server that starts pretty-printing would all arrive as
whitespace, and a closed validator that has seen one spelling is one layer of
exactly the gap these files exist for.

`TestWhitespaceDoesNotChangeTheParse` feeds the module both forms and compares
the parsed plans, and `TestEveryRecordedKeyIsRequired` enumerates its key list
from these bytes rather than from a hand-written list.

## Re-recording

If the contract moves, re-record from the resolver's stored bodies rather than
editing these by hand, and update the blob ids above in the same change. A file
edited here to make a test pass is the fixture-agrees-with-the-code defect
coming back.
