---
name: shepherd-pr
description: >-
  Drive a GitHub pull request through the Codex review cycle to a mergeable state
  (shardpilot/* repos). Request a review with `@codex review`, confirm Codex
  acknowledged it (👀 reaction; re-ask up to 5× if it doesn't appear within 5
  min), detect when the verdict lands (handling the reviewed/commented duality,
  pagination, and GraphQL reviewThreads), triage and resolve findings, watch
  GitHub Actions, and optionally merge. Use this WHENEVER the user asks to
  shepherd/babysit/drive a PR, request or kick off a Codex review, get a PR
  reviewed, take a PR to merge, or check whether Codex has finished — even if
  they only give a PR number or say things like "погнать на ревью", "закажи
  ревью у codex", "доведи PR до merge", "проверь, отревьюил ли codex". Merge is
  the optional, human-gated last step.
---

# Shepherd a PR through Codex review

Every `shardpilot/*` PR goes through a Codex review before merge. This is run
constantly, and the failure mode is always the same: a poll concludes "clean" or
"no verdict" when the truth is the opposite, because one of a handful of GitHub
API quirks was forgotten. **The quirks are encoded in the helper script — lean on
it instead of hand-rolling `gh api` calls each time.**

The goal is to take a PR to a *mergeable* state and report the evidence. **Merge
itself is the optional last step and is the user's call** — sometimes they want to
do extra manual checks first, and a `gh pr merge` is permission-gated anyway. Do
not merge unless the user explicitly says go.

## The helper script

`scripts/codex-review.sh` (full path: `~/.claude/skills/shepherd-pr/scripts/codex-review.sh`).
Requires `gh` (authenticated) + `jq`. The second argument is always `owner/repo`
(e.g. `shardpilot/integrations`).

| command | does | run how |
|---|---|---|
| `request <owner/repo> <pr>` | posts `@codex review`, waits for 👀, re-asks up to 5× (5 min apart) | **background** |
| `await <owner/repo> <pr>` | polls for the verdict on HEAD, ≤20 min, with the inline-lag settle | **background** |
| `status <owner/repo> <pr>` | one-shot snapshot (HEAD, reviewed-commit, verdict, threads, checks, mergeable) | foreground |
| `threads <owner/repo> <pr>` | lists unresolved Codex threads (id / path / body) for triage | foreground |
| `resolve <owner/repo> <threadId...>` | marks one or more addressed threads resolved | foreground |
| `checks <owner/repo> <pr>` | GitHub Actions check-runs on HEAD | foreground |

**`request` and `await` sleep for minutes — always launch them with the Bash
tool's `run_in_background: true`.** A foreground multi-minute `sleep` is blocked by
the harness and would freeze the turn; in the background the script runs detached
and you're notified when it exits. The exit code tells you what happened:

`0` clean on HEAD · `2` findings · `3` Codex never acked (it may be down — stop) ·
`4` timed out >20 min (review likely never started — check manually) · `64` usage.

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

> If you opened a non-draft PR, Codex auto-reviews and you can often skip straight
> to `await`. Use `request` when the auto-trigger didn't fire or you pushed a fix.

**2. Wait for the verdict.** Run `await` in the background. It resolves to one of:
- **CLEAN** (exit 0) — a "Didn't find any major issues" verdict whose
  `Reviewed commit` matches HEAD, with 0 unresolved Codex threads. Go to step 5.
- **FINDINGS** (exit 2) — one or more unresolved Codex inline threads. Go to step 3.
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
  thread IDs from `threads` (or `status`), then `resolve <owner/repo> <id...>`
  (it takes several at once). This is **load-bearing, not just tidiness**: the
  CLEAN signal is *0 unresolved Codex threads*, so an addressed-but-unresolved
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

6. **Merge — autonomous when clean.** When the verdict is clean on HEAD, all check-runs are green, and mergeStateStatus is CLEAN, squash-merge and delete the branch. No human go-ahead is needed.

## Detection traps (why naive polling lies)

These are the specific bugs that have caused false "clean"/"no verdict" calls.
The script handles them; this is so you recognize them if you read raw API output.

- **The bot login has a `[bot]` suffix in REST: `chatgpt-codex-connector[bot]`.**
  Filtering by the bare name silently returns empty (→ "no verdict" while a clean
  comment exists). GraphQL uses `chatgpt-codex-connector` (no suffix). Match with
  `startswith("chatgpt-codex-connector")` everywhere.
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
  is that `Reviewed commit` line == HEAD. The trailing sign-off is **random**
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

- Post / re-trigger: `gh api -X POST repos/<o>/<r>/issues/<n>/comments -f body='@codex review'`
- 👀 on the request comment: `gh api repos/<o>/<r>/issues/comments/<cid>/reactions --jq '[.[].content]'`
- Unresolved threads (authoritative): GraphQL `repository.pullRequest.reviewThreads.nodes{ id isResolved isOutdated comments(first:1){nodes{author{login} body path line}} }`
- Reply to a thread (e.g. to justify a declined finding before resolving): GraphQL `mutation{ addPullRequestReviewThreadReply(input:{pullRequestReviewThreadId:"<id>", body:"…"}){ comment{ id } } }`
- Resolve: GraphQL `mutation{ resolveReviewThread(input:{threadId:"<id>"}){ thread{ isResolved } } }`
- HEAD sha: `gh api repos/<o>/<r>/pulls/<n> --jq .head.sha`
- Checks: `gh api repos/<o>/<r>/commits/<sha>/check-runs --jq '[.check_runs[]|{name,status,conclusion}]'`

Note: `mergeStateStatus == UNKNOWN` just means GitHub hasn't computed mergeability
yet — re-run `status` and it resolves.
