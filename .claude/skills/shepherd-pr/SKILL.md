---
name: shepherd-pr
description: >-
  Drive a GitHub pull request through the Codex review cycle to a mergeable state
  (shardpilot/* repos). Request a review with `@codex review`, confirm Codex
  acknowledged it (👀 reaction; re-ask up to 5× if it doesn't appear within 5
  min), detect when the verdict lands (handling the reviewed/commented duality,
  pagination, and GraphQL reviewThreads), triage and resolve findings, watch
  GitHub Actions, and merge when the gates pass. Use this WHENEVER the user asks to
  shepherd/babysit/drive a PR, request or kick off a Codex review, get a PR
  reviewed, take a PR to merge, or check whether Codex has finished — even if
  they only give a PR number or say things like "погнать на ревью", "закажи
  ревью у codex", "доведи PR до merge", "проверь, отревьюил ли codex". Scope
  follows the ask: a status-only or review-only request ("check whether Codex
  finished", "request a review") ends after reporting the state. The
  autonomous-merge step (6) applies only when the user asked to shepherd the
  PR to merge — explicitly, or via the standing policy for a PR this session
  itself opened; then merge is autonomous once the clean-verdict, thread, and
  CI gates pass.
---

# Shepherd a PR through Codex review

Every `shardpilot/*` PR goes through a Codex review before merge. This is run
constantly, and the failure mode is always the same: a poll concludes "clean" or
"no verdict" when the truth is the opposite, because one of a handful of GitHub
API quirks was forgotten. **The quirks are encoded in the helper script — lean on
it instead of hand-rolling `gh api` calls each time.**

**Match the scope to the ask.** A status-only or review-only request ("check
whether Codex finished", "request a review", "закажи ревью") ends after
reporting the resulting state — do not continue to merge. Step 6 applies only
when the user asked to shepherd the PR to merge, explicitly or via the
standing policy for a PR this session itself opened.

When shepherding to merge, the goal is to take the PR all the way to merged and
report the evidence. **Merge is autonomous once the gates pass** — clean verdict
on HEAD, 0 unresolved non-outdated threads from **any** author (human threads
hold the gate too), green checks, and `mergeStateStatus == CLEAN` (see step 6).
No per-PR human go-ahead is required; this is the standing ShardPilot merge
policy (authorized 2026-07-07).

## The helper script

`scripts/codex-review.sh` — checked in at `.claude/skills/shepherd-pr/scripts/codex-review.sh`
relative to the repo root (that is the canonical path from a fresh checkout; if
you have installed the skill as a personal skill it also works from
`~/.claude/skills/shepherd-pr/scripts/codex-review.sh`).
Requires `gh` (authenticated) + `jq`. The second argument is always `owner/repo`
(e.g. `shardpilot/integrations`).

**Run the helper from a trusted ref, never from the PR's checkout.** A PR can
modify this very script (and this file), so executing the copy in the PR
worktree means running code the PR author controls — with your `gh`
credentials. Before reviewing/merging a third-party PR, materialize the helper
from the default branch and run that copy:

```
git fetch origin
git show origin/<default-branch>:.claude/skills/shepherd-pr/scripts/codex-review.sh \
  > /tmp/codex-review-trusted.sh && chmod +x /tmp/codex-review-trusted.sh
```

The PR-head copy is trustworthy only when the shepherd session itself authored
every commit on the branch.

| command | does | run how |
|---|---|---|
| `request <owner/repo> <pr>` | posts `@codex review`, waits for 👀, re-asks up to 5× (5 min apart) | **background** |
| `await <owner/repo> <pr> [after-comment-id]` | polls for the verdict on HEAD, ≤20 min, with the inline-lag settle; pass the floor comment id printed by `request` so a stale same-sha verdict from before the trigger is ignored | **background** |
| `status <owner/repo> <pr> [after-comment-id]` | one-shot snapshot (HEAD, reviewed-commit, verdict, threads, checks, mergeable); same verdict floor as `await` after a re-trigger, and same settle window — a verdict younger than ~3.5 min reads SETTLING, not CLEAN | foreground |
| `threads <owner/repo> <pr>` | lists unresolved review threads (id / author / path / body) for triage | foreground |
| `resolve <owner/repo> <pr> <threadId...>` | marks addressed threads resolved (refuses IDs not belonging to that PR) | foreground |
| `checks <owner/repo> <pr>` | GitHub Actions check-runs on HEAD | foreground |

**`request` and `await` sleep for minutes — always launch them with the Bash
tool's `run_in_background: true`.** A foreground multi-minute `sleep` is blocked by
the harness and would freeze the turn; in the background the script runs detached
and you're notified when it exits. The exit code tells you what happened,
per command:

`request` 0 = acknowledged (👀 or fresh Codex output), 3 = never acked after 5
attempts (Codex may be down — stop) · `await` 0 = clean on HEAD, 2 = findings,
4 = timed out >20 min (review likely never started — check manually) ·
`status` 0 = settled CLEAN on HEAD **with verifiably green checks**, 1 =
anything else (findings / no verdict / unknown / settling / CI not verifiably
green) · `checks` 0 = all green, 1 = not verifiably green ·
`threads` 1 = thread read failed · `resolve` 1 = any thread not actually
resolved · `64` usage/preflight error.

## The cycle

**0. Identify repo + PR.** From the multi-repo workspace root `gh pr ...` fails
("not a git repository") — the script uses `gh api` with the full `owner/repo`
path, which works anywhere. No need to `cd`.

**1. Request the review and confirm Codex picked it up.** Run `request` in the
background. Codex acknowledges with a 👀 reaction on the `@codex review` comment
(sometimes on the PR) — the script polls both. Note Codex **removes the 👀 when
the review completes**, so its presence means "review running now" and its absence
means "not started yet *or* already done" — you can't treat a missing 👀 as
"didn't start". That's why the script *also* treats any fresh Codex output since
the request — a new verdict comment, or new unresolved threads — as
acknowledgment, and won't re-ask while a review is actually running (or has just
finished). Only if **nothing** happens within 5 min does it
re-ask (a posted comment occasionally just doesn't register). After 5 silent
attempts (~25 min) it exits `3`: assume Codex is down, **stop and tell the user**
rather than spinning.

> On a non-draft PR Codex may auto-review on open, but an initial CLEAN pass can
> leave only a 👍 reaction on the PR body — without the SHA-scoped
> `Reviewed commit` comment that `await` gates on — so skipping straight to
> `await` after a clean auto-review stalls to TIMEOUT. Always run `request`
> first: it detects an already-finished pass via fresh output and hands off to
> `await` with the right verdict floor.

**2. Wait for the verdict.** Run `await` in the background. It resolves to one of:
- **CLEAN** (exit 0) — a "Didn't find any major issues" verdict whose
  `Reviewed commit` matches HEAD — either the full 40-char sha or, as Codex
  normally posts, an abbreviated ~10-hex id that is a case-insensitive prefix
  of HEAD (ids shorter than 10 hex chars fail closed; the merge-time
  `--match-head-commit` pin in step 6 backstops the residual prefix-collision
  risk) — with 0 unresolved review threads from **any** author — a human
  thread holds the gate too. Go to step 5.
- **FINDINGS** (exit 2) — one or more unresolved inline threads. Go to step 3.
- **TIMEOUT** (exit 4) — >20 min with no verdict. A real review almost never
  takes that long, so the review probably never started (was there a 👀?). Run
  `status` once, and if still nothing, re-`request` or tell the user.

**3. Triage findings.** Read the threads (`threads`, or they're printed by
`await`). Findings are P1/P2/P3 badged. For each: decide **fix** or **justify as
not-applicable**. Don't fix reflexively — Codex findings are usually real and
worth many rounds, but a confident-sounding P1 can be a false positive; verify
the claim against the actual code (and across repos if it spans them) before
acting. If a suggestion conflicts with an ADR or a prompt/spec, escalate to the
user rather than silently complying. The repos are greenfield — follow each
repo's `AGENTS.md`.

**4. Apply fixes, resolve every addressed thread, then re-review.**
- Commit + push the fixes. Run the repo's full CI locally from the worktree
  **after committing** and treat *that* as the gate — several repos' boundary
  checks pass *vacuously* on GitHub Actions (a `changedFiles()` diff against a
  missing `origin/main` comes back empty; untracked files are invisible to
  `git diff`), so a green GitHub check can hide a real failure.
- **Resolve every conversation you addressed — one Resolve per comment.** Get the
  thread IDs from `threads` (or `status`), then `resolve <owner/repo> <pr> <id...>`
  (it takes several at once). This is **load-bearing, not just tidiness**: the
  CLEAN signal is *0 unresolved review threads*, so an addressed-but-unresolved
  thread keeps `await`/`status` reporting FINDINGS and the cycle never converges —
  besides leaving the reviewer/user unsure what was handled. For a finding you're
  **declining** rather than fixing, post a short reply saying why, *then* resolve
  it too, so it reads as a deliberate decision and not a silently dropped comment.
- Go back to step 1 (`request`) to re-trigger. Repeat until clean — budget for
  **many rounds** (5–9 is normal on sensitive PRs). After a push, old finding
  threads become `outdated` and stay pinned to the old commit; judge the new
  review by `Reviewed commit == new HEAD` and the fresh verdict, not by leftover
  thread counts.

**5. Confirm GitHub Actions.** `checks` lists the check-runs on HEAD. All must
pass; fix any failures (which loops back through 3–4).

6. **Merge — autonomous when clean AND in scope.** This step applies only when
   the task is to shepherd the PR to merge (see the scope check at the top); on
   a status-only or review-only request, report the gate state and stop. When
   the verdict is clean on HEAD, all check-runs are green, and mergeStateStatus
   is CLEAN, squash-merge with the head pinned to the exact sha the gate
   evidence was observed on:

   ```
   gh pr merge <n> --repo <owner/repo> --squash --match-head-commit <gate-sha>
   ```

   `--match-head-commit` makes the gate→merge step atomic: if any commit lands
   after the gates were read, GitHub refuses the merge instead of merging
   unreviewed code. A mismatch rejection means the head moved — restart the
   cycle at step 1 on the new head. Keep the advisory pre-merge re-read too:
   re-read the PR head sha immediately before merging and only issue the merge
   if it still equals `<gate-sha>` (a cheap early catch; the `--match-head-commit`
   pin is what actually closes the race). No human go-ahead is needed.

   **Branch deletion:** add `--delete-branch` only for a work branch this
   shepherd session itself created and pushed for this PR. Any other branch —
   including `claude/…`, `wave/…`, `policy/…` automation prefixes — may be
   deleted only when the repo's own instructions (its AGENTS.md or
   contributing docs) actually authorize deleting that class of branch; do not
   assume such authorization exists. Otherwise merge without deleting and
   report that the branch was left for its owner.

## Detection traps (why naive polling lies)

These are the specific bugs that have caused false "clean"/"no verdict" calls.
The script handles them; this is so you recognize them if you read raw API output.

- **The bot login has a `[bot]` suffix in REST: `chatgpt-codex-connector[bot]`.**
  Filtering by the bare name silently returns empty (→ "no verdict" while a clean
  comment exists). GraphQL uses `chatgpt-codex-connector` (no suffix). Match the
  two EXACT logins (`== "chatgpt-codex-connector" or == "chatgpt-codex-connector[bot]"`)
  — never a prefix match, which a lookalike account (`chatgpt-codex-connector-x`)
  could satisfy to influence the merge gate.
- **Paginate everything.** Comment/review lists default to 30 items and have
  hidden findings before. Use `?per_page=100 --paginate`. Note: `gh` rejects
  `--slurp` together with `--jq` — pipe `--paginate --slurp` into a standalone
  `jq 'add | …'` (the script does this).
- **Unresolved findings = GraphQL `reviewThreads`, never REST `/pulls/<n>/comments`.**
  The REST review-comments endpoint lags and has read **0** while 9 findings (2 P1)
  already existed in `reviewThreads`. Gate on the GraphQL surface.
- **A `COMMENTED` review is NOT "findings".** Codex is an app and GitHub forbids
  apps self-`APPROVE`-ing, so *both* a clean pass and a findings review carry
  state `COMMENTED`. Decide by **unresolved inline threads**, not review state.
- **The clean verdict is an issue-comment, not a review object.** "Codex Review:
  Didn't find any major issues. …" lands in `issues/<n>/comments` with a
  `**Reviewed commit:** \`<sha>\`` line, while the review *object* may stay
  `COMMENTED` on an *older* commit. The authoritative "reviewed THIS commit" signal
  is that `Reviewed commit` line matching HEAD — and Codex writes it as an
  **abbreviated ~10-hex id**, so the script prefix-matches it against the full
  head sha (≥10 hex chars required; shorter fails closed). Demanding a full
  40-char equality here false-times-out genuinely clean reviews. The trailing
  sign-off is **random**
  (*Chef's kiss*, *Bravo*, *Swish!*, *Keep it up!*, …) — match the stable
  "Didn't find any major issues", never the sign-off.
- **Inline-lag settle.** A review object can appear 3–5 min *before* its inline
  comments finish posting. Never call "clean" the instant a verdict shows — wait a
  few minutes and re-check (the script settles ~3.5 min).
- **>20 min ⇒ stop.** A real review almost never exceeds 20 min, so a longer wait
  means the poll logic is wrong or the review never started. The script caps at 20
  min and exits `4`.
- **Never `&&`-chain a gate-check with `gh pr merge`.** The gate is a read-and-decide
  step; merge is a separate command issued only after reading the gate output. A
  finding that lands *between* the check and the merge won't stop an `&&` chain.

## Raw `gh` commands (fallback, if the script doesn't fit)

The fallbacks must keep the script's protections — pagination, exact bot logins,
ownership checks — or they silently reintroduce the traps above.

- Post / re-trigger: `gh api -X POST repos/<o>/<r>/issues/<n>/comments -f body='@codex review'`
- 👀 on the request comment (paginate + exact login, like the script):
  `gh api "repos/<o>/<r>/issues/comments/<cid>/reactions?per_page=100" --paginate --slurp | jq '[(add // [])[]|select(.user.login=="chatgpt-codex-connector[bot]" or .user.login=="chatgpt-codex-connector")|.content]|index("eyes")'`
- Unresolved threads (authoritative): GraphQL `repository.pullRequest.reviewThreads.nodes{ id isResolved isOutdated comments(first:1){nodes{author{login} body path line}} }`
- Reply to a thread (e.g. to justify a declined finding before resolving): GraphQL `mutation{ addPullRequestReviewThreadReply(input:{pullRequestReviewThreadId:"<id>", body:"…"}){ comment{ id } } }`
- Resolve — **verify ownership FIRST**: thread node IDs are global, so a pasted ID
  from another PR/repo would mutate that foreign conversation. Query
  `node(id:"<id>"){ ... on PullRequestReviewThread{ pullRequest{ number repository{ nameWithOwner } } } }`
  and proceed only if it matches your `<o>/<r>` and `<n>`; then
  `mutation{ resolveReviewThread(input:{threadId:"<id>"}){ thread{ isResolved } } }`
- HEAD sha: `gh api repos/<o>/<r>/pulls/<n> --jq .head.sha`
- Checks (paginate before judging CI — a failure past page 1 must not read as green;
  only `conclusion=="success"` passes):
  `gh api "repos/<o>/<r>/commits/<sha>/check-runs?per_page=100" --paginate --slurp | jq '[.[].check_runs[]|{name,status,conclusion}]'`

Note: `mergeStateStatus == UNKNOWN` just means GitHub hasn't computed mergeability
yet — re-run `status` and it resolves.
