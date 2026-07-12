---
name: verification-discipline
description: >-
  Evidence rules for any verification, testing, or "does it work?" task in
  shardpilot/* repos. Use this WHENEVER you are about to claim that something
  works, is done, is fixed, or passes — after implementing a feature, fixing a
  bug, running an e2e/smoke/manual check, or writing a verification or QA
  report. It defines what counts as proof (observed artifacts only: curl
  request/response captures, psql/clickhouse-client rows, test output, log
  lines, screenshots), the honest statuses (Pass / Fail / Not tested /
  Blocked), the required report metadata, and stack-specific checklists for
  our Go services, Angular console, Postgres, ClickHouse, and Redpanda.
---

# Verification discipline

Adapted from [akovalion/paranoid-qa](https://github.com/akovalion/paranoid-qa)
(MIT License, © 2026 Aleksei Kovalev), curated to the ShardPilot stack. The
full upstream license text is in `LICENSE` next to this file.

Sessions confidently report "everything works" about code that has never
executed. This skill exists to make that impossible: a verdict without an
observed artifact is not a verdict.

## The four rules

1. **A Pass/Fail verdict comes ONLY from an observed artifact.** A screenshot,
   a captured request/response body, a log line, a DB row, a test run's
   output. If you did not observe an artifact, you do not have a verdict.
   Reading the code, however carefully, is not an artifact.

2. **"Should work by logic" is never a verdict.** Anything you did not check
   is reported as `Not tested` with a reason. Anything you could not check is
   reported as `Blocked` with what blocks it. `Blocked` is not `Fail` — and
   neither is a substitute for silence: every planned check appears in the
   report with one of the four statuses.

3. **Log every deviation from spec immediately, however minor** — a changed
   error message, an extra field, a status code that differs from the
   contract, copy that doesn't match the design. Even if it looks intentional,
   record it; deciding it's intentional is the reviewer's call, not the
   tester's assumption.

4. **Form/API verification inspects the actual payload sent, not the green
   UI.** Capture the request body (DevTools Network tab, curl `--trace-ascii`,
   service logs) and read it: watch for `[object Object]`, empty or
   mis-serialized fields, stringified booleans, unit mistakes
   (cents-vs-dollars, ms-vs-s), IDs where slugs belong and vice versa. A 200
   response and a success toast prove only that something was accepted — not
   that the right thing was sent.

## The live-run rule (why this skill exists)

During the 2026-07-11 pre-flag-flip admin e2e, the mode-b enforcement doors
had **never executed a single real dispatch**, even though every implementing
session believed them done — unit tests were green, the code read correctly.
Three stacked bugs surfaced only on the first live run: a JWKS fetch hatch, a
NUL byte in a lock key, and a slug-passed-as-org-id false apply.

Therefore: **any "it works" claim about an execution path requires an artifact
from a LIVE run of that path.** Adjacent tests, unit tests of its parts, or
code reading do not qualify. If you cannot run the path live, the honest
status is `Not tested (no live run)` — say so.

## Verification reports

Every report carries, at the top:

- **Environment** — local compose stack / develop / staging, and any relevant
  feature-flag state.
- **Build/commit** — the exact SHA(s) the run was executed against, per repo.
- **Date/time** of the run and who/what executed it.

And, always, an explicit **"Not covered"** section listing what was NOT
tested, with reasons. An empty "Not covered" section is a claim — make it
knowingly or not at all. Flaky results are reported as flaky ("passed 2 of 3
runs"), never as Pass.

For each check: `precondition → action → expected → actual → status +
artifact` (path, log excerpt, captured body, or screenshot reference).

## Evidence with our tooling

Pass/Fail artifacts must be obtainable with what we actually have:

| Surface | Evidence |
|---|---|
| HTTP APIs (Fiber v3 services) | `curl --trace-ascii` captures (they record the request body actually SENT) plus the response body; or a saved payload with the service's request-log line, matched by correlation ID |
| Postgres 18 | `psql` rows, probed inside a transaction with the code's RLS posture: `BEGIN; SET LOCAL app.workspace_id = '<ws>'; SELECT ...; COMMIT;` — `SET LOCAL` outside a transaction has no effect. Connect as the confined service runtime role (`NOSUPERUSER`/`NOBYPASSRLS` — e.g. control-plane's `control_plane_runtime`) or `SET ROLE` to it first: superusers and `BYPASSRLS` roles always skip RLS, and a table owner skips it unless the table sets `FORCE ROW LEVEL SECURITY` — any of those yields false evidence |
| ClickHouse | `clickhouse-client` rows; account for merge-time dedup (`FINAL`/`GROUP BY`) before calling a count wrong |
| Redpanda topics | consumed messages (e.g. `rpk topic consume`), consumer-side log lines, DLQ contents |
| Go services | `make ci` output from the worktree, after committing (shardpilot-go has no Makefile — `gofmt -l $(git ls-files '*.go')` must print nothing, then `go test ./...` + `go vet ./...`, as its CI does); targeted `go test` output |
| console (Angular 22) | `node:test` + jsdom unit-run output; manual browser runs with screenshots + DevTools Network captures. There is NO Playwright harness in the console repo — do not claim "e2e passed" from console; browser e2e evidence comes from the qa repo's Playwright smokes or a manual run |
| admin-console | Vitest run output (`ng test --watch=false`); same manual-browser rules |

**Evidence handling is fail-closed on secrets AND personal data.** Traces,
logs, screenshots, and DB rows carry both credentials and PII —
`--trace-ascii` records the sent `Authorization` header and cookies; bodies,
rows, and screenshots carry emails, OAuth identities, and customer/workspace
business data. Redact secrets (bearer tokens, cookies, API keys) and
personal/customer-like data — or run with synthetic fixtures — before
attaching any artifact to a report; if redaction cannot be guaranteed, do
not attach the artifact.

Run each repo's full gate (`make ci`, or the repo's aggregate check) locally
from the worktree **after committing** and treat that output as the artifact —
a green remote check can pass vacuously on an empty diff.

### Per-repo gates (SDK, docs, and ops repos)

**The authoritative gate for any repo is its own `.github/workflows/ci.yml` —
not this table.** Before reporting "gate passed", open that workflow,
enumerate its jobs, and account for every one with one of the four statuses:
`Pass` or `Fail` with its output as the artifact, `Blocked` (cannot run here
— say why), or `Not tested` (with the reason). The rows below are pointers,
not the contract.

The table above covers Go/Angular/data surfaces, but this skill also ships to
repos with entirely different gates. Run the gate commands locally from the
worktree after committing and treat their output as the evidence. Jobs that
are license- or infra-gated and did not run are reported `Blocked` with the
gating reason — never assumed `Pass`; a probe/preflight job that exists only
to gate an optional job is accounted for together with the job it gates.
shardpilot-unity, shardpilot-unreal, and marketing-site each also run a
gitleaks secret-scan job — run the same scan locally (the
`ghcr.io/gitleaks/gitleaks` Docker image works) or report it
`Blocked (gitleaks not installable here)`.

| Repo | Gate (evidence = its output) |
|---|---|
| shardpilot-unity | JSON-parse `package.json` + every `*.asmdef`; `./tools~/headless-tests/run.sh`; gitleaks secret scan (see above). The dotnet-format job self-skips while the package ships no `.csproj`/`.editorconfig` outside `tools~/` — report it `Not tested (no format inputs; CI job self-skips)`, and if inputs appear, run the variant CI selects. The GameCI editmode/playmode jobs require `UNITY_LICENSE`/`UNITY_EMAIL`/`UNITY_PASSWORD` secrets and normally skip — report them `Blocked (no Unity license secrets)`, not Pass |
| shardpilot-unreal | `make test` (portable C++ core); gitleaks secret scan (`gitleaks detect --source . --redact --verbose`) — report it `Blocked` if gitleaks isn't installable locally. The in-engine "Unreal Engine smoke" job (`Automation RunTests ShardPilot`) requires `EPIC_GHCR_TOKEN` plus a configured engine image/runner and normally skips — report it `Blocked (no Unreal engine container/runner)`, not Pass |
| shardpilot-defold | `bash -n scripts/*.sh`; `./scripts/check_library.sh`; `lua5.4 test/test_sdk.lua`, `lua5.4 test/test_crash.lua`, `lua5.4 test/test_remote_config.lua` |
| docs | `make check && make lint` |
| developers | `git diff --check origin/main...HEAD` (whitespace, three-dot against the PR base ref exactly as its CI runs it — a bare `git diff --check` sees only unstaged changes and passes vacuously after committing); `bash -n scripts/*.sh && ./scripts/check_docs.sh` |
| infra | `make ci`, plus `make k8s-lint` (needs kubeconform/helm/kustomize/yq/shellcheck installed) |
| qa | `npm ci`, then `make check && make ci` |
| marketing-site | `npm ci`; `npm audit --audit-level=high`; `npm run lint && npm run check && npm run build && npm run check:links`; `npm run build:live` (live-mode build); gitleaks secret scan (see above). The Cloudflare Workers preview/production deploy jobs run only in CI — account for them as `Blocked (CI-only deploy)` |

## References — read the relevant file in full before planning the run

| File | When |
|---|---|
| `references/backend.md` | Any Go service / API / DB / queue verification |
| `references/frontend.md` | Any console or admin-console UI verification |
| `references/cross-cutting.md` | Almost always — UI↔backend consistency, errors, sessions, TZ, files, search |
| `references/common-misses.md` | Always, last — final self-check before producing the report |

A plan made without reading the relevant reference counts as incomplete.
These checklists remind you of check classes; the ticket/ADR/spec remains the
source of truth for expected behavior.
