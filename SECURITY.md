# Security Policy

The ShardPilot Go SDK is pre-v1 software: the API is unstable and may change
before v1.

**Do not use it with production secrets or production customer/player data.**
That prohibition is unchanged; what changed is that this file no longer states
it in terms of an internal release process a reader outside ShardPilot cannot
observe. It lifts when ShardPilot says so, not when you decide you are ready.

## Reporting

Report suspected vulnerabilities privately through this repository's **security
advisory flow** — open a draft advisory from the Security tab, or go straight to
`/security/advisories/new` on this repository. Private reporting is enabled
here, so that route is open to anyone; you do not need a prior contact with us.

Please do not open a public issue describing a suspected vulnerability. If for
any reason the advisory form will not accept your report, open a public issue
that says only that you have a security report and asks for a private channel —
no version, no reproduction, no description of the flaw. A request for a channel
discloses nothing, and we will reply with somewhere private to send the
details.

## Boundaries

- Do not commit tokens, secrets, or real customer/player data.
- The SDK must not log tokens or full event payloads.
- The SDK must not store a durable local queue in v0.
- The SDK must not make provider, model, GitHub, billing, or account-management
  write calls.
- Do not send raw provider payloads, raw player/customer payloads, diffs,
  patches, code/file/archive content, prompts, completions, or unsanitized
  stack/backtrace payloads.
