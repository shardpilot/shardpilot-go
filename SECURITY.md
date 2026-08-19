# Security Policy

The ShardPilot Go SDK is pre-v1 software: the API is unstable and may change
before v1. Assess it against your own production-readiness bar before using it
with production secrets or production customer/player data.

## Reporting

Report suspected vulnerabilities privately through the repository security
advisory flow when available, or contact the maintainers through a private
project channel.

## Boundaries

- Do not commit tokens, secrets, or real customer/player data.
- The SDK must not log tokens or full event payloads.
- The SDK must not store a durable local queue in v0.
- The SDK must not make provider, model, GitHub, billing, or account-management
  write calls.
- Do not send raw provider payloads, raw player/customer payloads, diffs,
  patches, code/file/archive content, prompts, completions, or unsanitized
  stack/backtrace payloads.
