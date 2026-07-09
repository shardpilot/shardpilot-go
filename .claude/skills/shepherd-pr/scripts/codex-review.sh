#!/usr/bin/env bash
# codex-review.sh — deterministic helpers for the shepherd-pr skill.
#
# This script exists so the easy-to-forget mechanics of driving a PR through a
# Codex review live in CODE, not in the model's memory. Specifically it bakes in:
#   - pagination on every comment/review read (default GitHub page caps at 30 and
#     has silently hidden Codex findings before — always --paginate --slurp)
#   - the chatgpt-codex-connector[bot] login suffix (filtering by the bare name
#     returns EMPTY, so a real verdict reads as "no verdict")
#   - GraphQL reviewThreads as the AUTHORITATIVE unresolved-findings surface
#     (the REST /pulls/<n>/comments endpoint lags/diverges and has read 0 while
#     9 findings already existed)
#   - the 👀-acknowledgement retry loop (re-ask @codex review up to 5×, 5 min apart)
#   - the inline-lag settle (the review object lands 3-5 min BEFORE its inline
#     comments finish posting — never call "clean" the instant a verdict appears)
#   - a 20-min hard cap so a stuck poll never hangs all night
#
# Requires: gh (authenticated) + jq.
#
# Usage:
#   codex-review.sh request <owner/repo> <pr>       # post @codex review, wait for 👀 (retry ≤5×). RUN IN BACKGROUND.
#   codex-review.sh await   <owner/repo> <pr> [after-comment-id]  # poll for the verdict on HEAD (≤20 min). RUN IN BACKGROUND.
#                                                   # pass the floor comment id printed by `request` so only a verdict
#                                                   # posted AFTER that comment counts (stale same-sha verdicts ignored).
#   codex-review.sh status  <owner/repo> <pr> [after-comment-id]  # one-shot snapshot — the re-check tool. foreground
#                                                   # same floor semantics as await: with the id set, an older clean
#                                                   # comment cannot make the snapshot read CLEAN after a re-trigger.
#   codex-review.sh threads <owner/repo> <pr>       # list unresolved review threads (id/author/path/body). foreground
#   codex-review.sh resolve <owner/repo> <pr> <threadId...> # mark addressed threads on that PR resolved. foreground
#   codex-review.sh checks  <owner/repo> <pr>       # GitHub Actions check-runs on HEAD.             foreground
#
# Exit codes (per command — the exit status IS the gate signal):
#   request: 0 = acknowledged (👀 or fresh Codex output) · 3 = never acked after 5 attempts
#            (Codex may be down — stop, tell the user) · 64 = preflight/post failed
#   await:   0 = CLEAN on HEAD · 2 = findings (unresolved threads) · 4 = timed out (>20 min —
#            review likely never started or the read is wrong; check manually)
#   status:  0 = settled CLEAN on HEAD with verifiably green checks · 1 = anything else
#            (findings, no verdict on HEAD, unknown/unreadable surfaces, settle window,
#            or CI not verifiably green)
#   threads: 0 = listed · 1 = thread read failed (state unknown — fail closed)
#   checks:  0 = all green · 1 = non-passing, pending, missing, or unreadable (NOT green)
#   resolve: 0 = every requested thread resolved · 1 = any refused or failed
#   64 = usage error (any command)

set -uo pipefail

# Bot identity: EXACT logins only — REST surfaces "chatgpt-codex-connector[bot]",
# GraphQL surfaces the bare "chatgpt-codex-connector". Never prefix-match: a
# lookalike account (e.g. "chatgpt-codex-connector-x") must not be able to
# satisfy the merge gate's bot-identity checks.
IS_CODEX='((.user.login // "") == "chatgpt-codex-connector" or (.user.login // "") == "chatgpt-codex-connector[bot]")'
CLEAN_RE="didn't find any major issues"   # the STABLE verdict text only — sign-offs (Chef's kiss, Bravo, …) are random; never gate on them

ts()  { date +%H:%M:%S; }
die() { echo "error: $*" >&2; exit 64; }

# ---- shared accessors -------------------------------------------------------

head_sha() { gh api "repos/$SLUG/pulls/$PR" --jq '.head.sha' 2>/dev/null; }

# Latest Codex top-level issue-comment as a JSON object (the clean-verdict
# channel) so callers can take .id and .body from ONE fetch — no read race.
# NOTE: gh rejects `--slurp` together with `--jq`, so pipe to a standalone jq.
# `--slurp` gives an array-of-pages; `add` flattens it into one array, so `last`
# is the GLOBAL last bot comment — not the per-page last you'd get from `--jq`
# running once per page under `--paginate`.
latest_bot_comment_json() {
  gh api "repos/$SLUG/issues/$PR/comments?per_page=100" --paginate --slurp 2>/dev/null \
    | jq -c '(add // []) | map(select('"$IS_CODEX"')) | (last // {})' 2>/dev/null
}

latest_bot_comment_body() { latest_bot_comment_json | jq -r '.body // ""' 2>/dev/null; }

# Highest Codex issue-comment id (0 if none) — used to detect *fresh* Codex output
# since a request, regardless of whether the 👀 reaction was caught.
latest_bot_comment_id() {
  gh api "repos/$SLUG/issues/$PR/comments?per_page=100" --paginate --slurp 2>/dev/null \
    | jq -r '[(add // [])[] | select('"$IS_CODEX"') | .id] | (max // 0)' 2>/dev/null
}

# The "Reviewed commit: `<sha>`" line is the authoritative "reviewed THIS commit"
# signal — more reliable than a review object's commit_id, which can stay pinned
# to an older findings review.
reviewed_sha_from() { grep -oiE 'reviewed commit[^0-9a-f]*[0-9a-f]{7,40}' | grep -oiE '[0-9a-f]{7,40}' | head -1; }

# Reviewed-commit gate: Codex verdicts normally carry an ABBREVIATED (~10-hex)
# commit id, not a full 40-char sha — requiring 40 chars made `await`/`status`
# TIMEOUT on genuinely clean reviews. Accept a full 40-char equality match, or
# an abbreviated id of ≥10 hex chars that is a case-insensitive PREFIX of the
# full observed head sha; anything shorter than 10 chars, non-hex, or without
# a full 40-char head to compare against fails CLOSED. On the earlier
# collision objection (a short prefix could collide or be forced): a ≥10-hex
# prefix compared against ONE pinned head sha leaves ~16^-10 per candidate —
# an attacker cannot grind the head sha of a PR they want merged into a chosen
# prefix without pushing commits, which resets the gate — and the merge-time
# `--match-head-commit <gate-sha>` guard (SKILL.md step 6) backstops it: even
# a colliding "clean" read cannot merge a head other than the one gated.
reviewed_matches_head() {  # $1 = reviewed sha (lowercased), $2 = full head sha (lowercased)
  local r="$1" h="$2"
  [ -n "$r" ] && [ ${#h} -eq 40 ] || return 1
  [[ "$r" =~ ^[0-9a-f]+$ ]] || return 1
  if [ ${#r} -eq 40 ]; then [ "$r" = "$h" ]; return $?; fi
  [ ${#r} -ge 10 ] || return 1
  case "$h" in "$r"*) return 0 ;; *) return 1 ;; esac
}

# All check-runs on a sha, across pages (per_page caps at 100; a failure or a
# still-pending run on page 2+ must not slip past the green gate). Same
# --paginate --slurp + standalone-jq pattern as the comment readers.
# Fail CLOSED: on any read/parse failure — or an EMPTY run list — emit nothing
# and return 1. An unreadable Actions surface must read as "not verified", and
# zero check-runs means the workflows have not triggered (yet), not that CI
# passed; the gate stays unverified until the expected runs exist.
check_runs_json() {
  local out
  out=$(gh api "repos/$SLUG/commits/$1/check-runs?per_page=100" --paginate --slurp 2>/dev/null \
    | jq -c '[.[].check_runs[]?]' 2>/dev/null)
  [ -n "$out" ] || return 1
  printf '%s' "$out" | jq -e 'type=="array" and length > 0' >/dev/null 2>&1 || return 1
  printf '%s\n' "$out"
}

# Pages through ALL reviewThreads via pageInfo cursors (GraphQL first/last cap
# at 100, and a finding on page 2+ must not be invisible to the gate). Emits a
# flat JSON array of thread nodes; on ANY API/GraphQL error emits nothing and
# returns 1 so callers fail CLOSED instead of reading "no threads".
graphql_threads() {
  local cursor="" resp page all="[]" has_next
  local -a args
  while :; do
    args=(-f o="$OWNER" -f r="$NAME" -F n="$PR")
    [ -n "$cursor" ] && args+=(-f c="$cursor")
    resp=$(gh api graphql -f query='
      query($o:String!,$r:String!,$n:Int!,$c:String){
        repository(owner:$o,name:$r){
          pullRequest(number:$n){
            reviewThreads(first:100,after:$c){
              pageInfo{ hasNextPage endCursor }
              nodes{
                id isResolved isOutdated
                comments(first:1){ nodes{ author{login} body path line } }
              }
            }
          }
        }
      }' "${args[@]}" 2>/dev/null)
    printf '%s' "$resp" | jq -e '(has("errors")|not) and (.data.repository.pullRequest.reviewThreads.nodes != null)' >/dev/null 2>&1 || return 1
    page=$(printf '%s' "$resp" | jq '.data.repository.pullRequest.reviewThreads.nodes' 2>/dev/null) || return 1
    all=$(jq -n --argjson a "$all" --argjson b "$page" '$a + $b' 2>/dev/null) || return 1
    has_next=$(printf '%s' "$resp" | jq -r '.data.repository.pullRequest.reviewThreads.pageInfo.hasNextPage' 2>/dev/null)
    [ "$has_next" = "true" ] || break
    cursor=$(printf '%s' "$resp" | jq -r '.data.repository.pullRequest.reviewThreads.pageInfo.endCursor' 2>/dev/null)
    [ -n "$cursor" ] && [ "$cursor" != "null" ] || return 1
  done
  printf '%s\n' "$all"
}

# Non-passing run count for an already-fetched check-runs array (stdin) —
# shared by `checks` and `status` so both exit-code gates apply the SAME
# strictness rules (see cmd_checks for the ALLOW_SKIPPED rationale).
nonpassing_count() {
  if [ "${ALLOW_SKIPPED:-0}" = "1" ]; then
    jq '[.[]|select(.conclusion==null or (.conclusion!="success" and .conclusion!="neutral" and .conclusion!="skipped"))]|length' 2>/dev/null
  else
    jq '[.[]|select(.conclusion != "success")]|length' 2>/dev/null
  fi
}

unresolved_count() {
  # Fail CLOSED: on any thread-read failure echo "unknown" and return 1 — the
  # gate must never treat an unreadable findings surface as "0 findings".
  # Counts ALL unresolved non-outdated threads regardless of author — a human
  # or other-bot thread on the current head must hold the merge gate exactly
  # like a Codex finding. Outdated threads are excluded (after a push they stay
  # pinned to the old commit; judge the new review by Reviewed-commit == new
  # HEAD), but list_threads still shows them for triage.
  local resp n
  resp=$(graphql_threads) || { echo "unknown"; return 1; }
  n=$(printf '%s' "$resp" | jq '[.[] | select(.isResolved==false and .isOutdated==false)] | length' 2>/dev/null)
  [[ "${n:-}" =~ ^[0-9]+$ ]] || { echo "unknown"; return 1; }
  echo "$n"
}

# Unresolved non-outdated threads AUTHORED BY CODEX (GraphQL surfaces the bare
# login). Used ONLY by `request`'s fresh-output acknowledgment shortcut, so a
# human (or other-bot) thread posted after the request cannot read as "Codex
# picked it up". The merge gate itself still counts ALL authors via
# unresolved_count. Same fail-CLOSED contract: "unknown" + rc 1 on read failure.
unresolved_codex_count() {
  local resp n
  resp=$(graphql_threads) || { echo "unknown"; return 1; }
  n=$(printf '%s' "$resp" | jq '[.[] | select(.isResolved==false and .isOutdated==false
        and ((.comments.nodes[0].author.login // "") == "chatgpt-codex-connector"))] | length' 2>/dev/null)
  [[ "${n:-}" =~ ^[0-9]+$ ]] || { echo "unknown"; return 1; }
  echo "$n"
}

list_threads() {
  # Print an EXPLICIT error on read failure — silence here would let "listing
  # failed" read as "(no threads)" to a human skimming the output. Returns 1 so
  # `threads` (which is just this) is nonzero when the state is unknown.
  local resp
  resp=$(graphql_threads) || { echo "  ERROR: reviewThreads read FAILED — thread state UNKNOWN (fail closed; do not treat as none)."; return 1; }
  printf '%s\n' "$resp" | jq -r 'map(select(.isResolved==false))
    | if length==0 then "  (none)"
      else .[] | "  • \(.id)  [\(.comments.nodes[0].author.login // "?")]  \(.comments.nodes[0].path // "—"):\(.comments.nodes[0].line // "?")\(if .isOutdated then " (outdated)" else "" end)\n    \((.comments.nodes[0].body // "")|gsub("[\n\r]+";" ")|.[0:240])"
      end' 2>/dev/null
}

# ---- subcommands ------------------------------------------------------------

cmd_request() {
  local attempt cid first_cid="" i eyes_c eyes_p base_cid base_unres new_cid new_unres floor
  # Preflight the OBSERVATION tooling before posting anything: if jq is missing
  # or the comment surface is unreadable (bad auth, wrong repo/PR), posting
  # would fire a review whose acknowledgment we can never observe — and the
  # loop would then misreport a Codex outage. Fail with usage instead.
  command -v jq >/dev/null 2>&1 || { echo "[$(ts)] ERROR: jq not found — cannot observe Codex responses; not posting a request."; return 64; }
  if [ -z "$(latest_bot_comment_json)" ]; then
    echo "[$(ts)] ERROR: cannot read the PR comment surface (gh auth? repo/PR correct?) — not posting a request."
    return 64
  fi
  # Baselines for the fresh-output shortcut. Fail CLOSED when unreadable: an
  # unknown baseline stays EMPTY and that signal is skipped, so historical
  # Codex comments/threads can never read as "fresh output since this request"
  # just because a baseline read errored down to zero. The thread baseline
  # counts CODEX-authored threads only — a human commenting mid-request must
  # not read as Codex acknowledgment.
  base_cid=$(latest_bot_comment_id);   [[ "$base_cid"   =~ ^[0-9]+$ ]] || base_cid=""
  base_unres=$(unresolved_codex_count); [[ "$base_unres" =~ ^[0-9]+$ ]] || base_unres=""
  for attempt in 1 2 3 4 5; do
    cid=$(gh api -X POST "repos/$SLUG/issues/$PR/comments" -f body='@codex review' --jq '.id' 2>/dev/null)
    # Fail IMMEDIATELY if the request comment could not be posted (missing
    # issue-write permission, wrong PR number, rate limit) — otherwise the loop
    # would poll a nonexistent comment for up to 25 min and misreport a Codex
    # outage when Codex was never actually asked.
    if ! [[ "${cid:-}" =~ ^[0-9]+$ ]]; then
      echo "[$(ts)] ERROR: could not post '@codex review' (no comment id returned — check token permissions, PR number, rate limits)."
      return 64
    fi
    [ -n "$first_cid" ] || first_cid="$cid"
    # The verdict floor handed to `await` is the PRE-REQUEST Codex-comment
    # baseline when it was readable, else the FIRST request comment of this
    # invocation (never a later retry's higher id). Rationale: any Codex
    # verdict newer than the last pre-request bot comment is fresh output of
    # THIS cycle — including one that lands in the instant between the baseline
    # read and our comment posting, or between retry attempts. Flooring at the
    # request comment's own id would discard such a verdict into a false
    # timeout; flooring at the baseline still excludes every stale pre-request
    # comment.
    floor="${base_cid:-$first_cid}"
    echo "[$(ts)] attempt $attempt/5 — posted '@codex review' (comment $cid)"
    for i in $(seq 1 10); do          # 10 × 30s = 5 min per attempt
      sleep 30
      # Only Codex's OWN 👀 counts as acknowledgment — an eyes reaction from a
      # human or another bot must not read as "review running". Reactions are
      # paginated (default page caps at 30): read EVERY page or a 👀 past the
      # first page reads as "no acknowledgment" and triggers needless re-asks.
      eyes_c=$(gh api "repos/$SLUG/issues/comments/$cid/reactions?per_page=100" --paginate --slurp 2>/dev/null \
        | jq '[(add // [])[]|select('"$IS_CODEX"')|.content]|index("eyes")' 2>/dev/null)
      eyes_p=$(gh api "repos/$SLUG/issues/$PR/reactions?per_page=100" --paginate --slurp 2>/dev/null \
        | jq '[(add // [])[]|select('"$IS_CODEX"')|.content]|index("eyes")' 2>/dev/null)
      new_cid=$(latest_bot_comment_id);    [[ "$new_cid"   =~ ^[0-9]+$ ]] || new_cid=0
      new_unres=$(unresolved_codex_count); [[ "$new_unres" =~ ^[0-9]+$ ]] || new_unres=0
      # Acknowledged if Codex reacted (👀) OR has already produced fresh output
      # since the request — a new bot comment (clean verdict) or new unresolved
      # Codex threads (findings). The 👀 can be transient or skipped, so never
      # rely on it alone, or a working review gets needlessly re-asked up to 5×.
      if { [ -n "${eyes_c:-}" ] && [ "$eyes_c" != "null" ]; } || { [ -n "${eyes_p:-}" ] && [ "$eyes_p" != "null" ]; }; then
        echo "[$(ts)] 👀 Codex acknowledged — review running. Next: codex-review.sh await $SLUG $PR $floor"; return 0
      fi
      # The two fresh-output signals are evaluated INDEPENDENTLY, each gated on
      # its own readable baseline: an unreadable thread baseline must not
      # disable the comment-id signal (or vice versa) — each side fails closed
      # on its own, not jointly.
      if { [ -n "$base_cid" ] && [ "$new_cid" -gt "$base_cid" ]; } || \
         { [ -n "$base_unres" ] && [ "$new_unres" -gt "$base_unres" ]; }; then
        echo "[$(ts)] Codex already responded (fresh output since request) — go to: codex-review.sh await $SLUG $PR $floor"; return 0
      fi
    done
    echo "[$(ts)] no 👀 / no Codex output after 5 min — re-asking."
  done
  echo "[$(ts)] GAVE UP: no 👀 and no Codex output after 5 attempts (~25 min). Codex may be down — stop and tell the user."
  return 3
}

cmd_await() {
  # Optional 3rd arg (after the pr): a comment id — the floor printed by
  # `request` (the pre-request Codex-comment baseline, or the request comment
  # id when that baseline was unreadable). When given, only a bot verdict
  # POSTED AFTER that id counts: an older clean comment on the same sha must
  # not satisfy a re-requested pass (e.g. after resolving threads with no
  # push), or await can report CLEAN while the fresh pass is still running.
  local after_id="${1:-0}"; [[ "$after_id" =~ ^[0-9]+$ ]] || after_id=0
  local start now elapsed sha sha7 sha_full bc bcid body rsha rsha7 clean unresolved settle_sha=""
  start=$(date +%s)
  while :; do
    sha=$(head_sha); sha7=${sha:0:7}
    sha_full=$(printf '%s' "$sha" | tr '[:upper:]' '[:lower:]')
    bc=$(latest_bot_comment_json)
    bcid=$(printf '%s' "$bc" | jq -r '.id // 0' 2>/dev/null); [[ "$bcid" =~ ^[0-9]+$ ]] || bcid=0
    body=$(printf '%s' "$bc" | jq -r '.body // ""' 2>/dev/null)
    if [ "$after_id" -gt 0 ] && [ "$bcid" -le "$after_id" ]; then body=""; fi
    rsha=$(printf '%s' "$body" | reviewed_sha_from | tr '[:upper:]' '[:lower:]'); rsha7=${rsha:0:7}
    if printf '%s' "$body" | grep -qiE "$CLEAN_RE"; then clean=yes; else clean=no; fi
    unresolved=$(unresolved_count)

    # Fail CLOSED: an unreadable findings surface must never read as "clean".
    if [ "${unresolved:-unknown}" = "unknown" ]; then
      echo "[$(ts)] WARNING: reviewThreads read failed — cannot verify findings; NOT declaring clean. Retrying…"
    # Findings are decided by UNRESOLVED INLINE THREADS (GraphQL), never by review
    # state — Codex always uses state COMMENTED (apps can't self-APPROVE), so a
    # COMMENTED review is NOT itself a "findings" signal.
    elif [ "$unresolved" -gt 0 ]; then
      echo "[$(ts)] FINDINGS — $unresolved unresolved review thread(s):"
      list_threads
      return 2
    # Clean = verdict text + Reviewed-commit matches HEAD + 0 unresolved. Settle
    # once (~3.5 min) before declaring it, because inline comments can lag the
    # verdict. The Reviewed-commit match accepts a full 40-char sha or a ≥10-hex
    # abbreviated prefix of HEAD (Codex posts ~10-hex ids); <10 chars fails
    # closed — see reviewed_matches_head for the collision analysis.
    elif [ "$clean" = yes ] && reviewed_matches_head "$rsha" "$sha_full"; then
      # The settle is PER-SHA: if HEAD advanced since the last settle (e.g. a
      # push landed mid-settle and Codex re-reviewed), the new sha gets its own
      # settle window instead of inheriting the old one and skipping the wait.
      if [ "$settle_sha" != "$sha_full" ]; then
        echo "[$(ts)] clean verdict on HEAD ($sha7) — settling 3.5 min for any lagging inline comments…"
        settle_sha="$sha_full"; sleep 210; continue
      fi
      echo "[$(ts)] CLEAN — Codex verdict on HEAD $sha7, 0 unresolved threads:"
      echo "----"; printf '%s\n' "$body" | head -6
      return 0
    fi

    # The timeout is checked AFTER verdict processing (not at loop top) so a
    # clean verdict that lands near the cap still gets its post-settle re-check
    # instead of being misreported as TIMEOUT.
    now=$(date +%s); elapsed=$((now - start))
    if [ "$elapsed" -gt 1200 ]; then
      echo "[$(ts)] TIMEOUT (>20 min). A real review almost never exceeds 20 min — it likely never started (no 👀?) or the read is wrong. Check manually with: codex-review.sh status $SLUG $PR"
      return 4
    fi

    echo "[$(ts)] waiting… HEAD=$sha7 reviewed=${rsha7:-none} clean=$clean unresolved=${unresolved:-0} (elapsed ${elapsed}s)"
    sleep 45
  done
}

cmd_status() {
  # Optional after-comment-id, same semantics as `await`: when a fresh pass
  # was just re-triggered on the SAME sha, an older clean comment must not
  # make the snapshot read CLEAN — pass the request-comment id as the floor.
  local after_id="${1:-0}"; [[ "$after_id" =~ ^[0-9]+$ ]] || after_id=0
  local sha sha7 sha_full bc bcid body rsha rsha7 clean unresolved runs nonpass checks_green=no
  sha=$(head_sha); sha7=${sha:0:7}
  sha_full=$(printf '%s' "$sha" | tr '[:upper:]' '[:lower:]')
  bc=$(latest_bot_comment_json)
  bcid=$(printf '%s' "$bc" | jq -r '.id // 0' 2>/dev/null); [[ "$bcid" =~ ^[0-9]+$ ]] || bcid=0
  body=$(printf '%s' "$bc" | jq -r '.body // ""' 2>/dev/null)
  if [ "$after_id" -gt 0 ] && [ "$bcid" -le "$after_id" ]; then body=""; fi
  rsha=$(printf '%s' "$body" | reviewed_sha_from | tr '[:upper:]' '[:lower:]'); rsha7=${rsha:0:7}
  if printf '%s' "$body" | grep -qiE "$CLEAN_RE"; then clean=yes; else clean=no; fi
  unresolved=$(unresolved_count)

  echo "PR $SLUG#$PR"
  echo "  HEAD                 $sha7"
  echo "  Codex reviewed       ${rsha7:-none}  $(reviewed_matches_head "$rsha" "$sha_full" && echo '== HEAD ✓ (full sha or ≥10-hex prefix)' || echo '(not HEAD — re-review pending, none, or unusable sha)')"
  echo "  clean-verdict text   $clean"
  echo "  unresolved threads   ${unresolved:-unknown}"
  echo "  mergeable            $(gh api graphql -f query='query($o:String!,$r:String!,$n:Int!){repository(owner:$o,name:$r){pullRequest(number:$n){mergeStateStatus mergeable}}}' -f o="$OWNER" -f r="$NAME" -F n="$PR" --jq '.data.repository.pullRequest|"\(.mergeable) / mergeStateStatus=\(.mergeStateStatus)"' 2>/dev/null)"
  echo "  --- GitHub Actions ---"
  # Track the CI result for the exit gate below: checks_green=yes only when
  # the run list was readable, non-empty, and every run passes (same rules as
  # `checks` via nonpassing_count). Unreadable/missing stays "no" — fail closed.
  if runs=$(check_runs_json "$sha"); then
    printf '%s' "$runs" | jq -r '.[]|"    \(.conclusion // .status)\t\(.name)"' 2>/dev/null | sort
    nonpass=$(printf '%s' "$runs" | nonpassing_count)
    [[ "${nonpass:-}" =~ ^[0-9]+$ ]] && [ "$nonpass" -eq 0 ] && checks_green=yes
  else
    echo "    (check-run read FAILED or no runs exist on this sha — treat as NOT green, fail closed)"
  fi
  echo "  --- unresolved review threads ---"
  list_threads
  echo ""
  # Verdict + exit code. status shares await's settle discipline: inline
  # comments can lag the verdict comment by 3-5 min, so a snapshot taken
  # inside the settle window (<210s since the verdict posted, matching await's
  # sleep) reports SETTLING — not an instant CLEAN a wrapper could merge on.
  # Exit code IS the signal: 0 only for a settled CLEAN on HEAD with
  # verifiably green checks; findings, no-verdict, unknown, settling, and
  # not-green/unverified-CI snapshots all exit 1 — the snapshot covers the CI
  # surface too, so its 0 must not vouch for less than everything it displays.
  local verdict rc=1 created cepoch age
  if [ "${unresolved:-unknown}" = "unknown" ]; then
    verdict='UNKNOWN — thread read failed (fail closed, not clean)'
  elif [ "$unresolved" -gt 0 ]; then
    verdict='FINDINGS'
  elif [ "$clean" = yes ] && reviewed_matches_head "$rsha" "$sha_full"; then
    created=$(printf '%s' "$bc" | jq -r '.created_at // ""' 2>/dev/null)
    cepoch=""
    if [ -n "$created" ]; then   # GNU date parses "" as *today 00:00* — never feed it an empty string
      cepoch=$(date -u -d "$created" +%s 2>/dev/null) \
        || cepoch=$(date -j -u -f '%Y-%m-%dT%H:%M:%SZ' "$created" +%s 2>/dev/null) \
        || cepoch=""
    fi
    [[ "$cepoch" =~ ^[0-9]+$ ]] || cepoch=""
    age=$(( $(date +%s) - ${cepoch:-0} ))
    if [ -z "$cepoch" ]; then
      verdict='SETTLING? — clean verdict on HEAD but its timestamp is unreadable; use await for the settled verdict (fail closed)'
    elif [ "$age" -lt 210 ]; then
      verdict="SETTLING — clean verdict on HEAD posted ${age}s ago (<210s); inline comments may still be landing. Re-run status after the window, or use await."
    elif [ "$checks_green" != yes ]; then
      verdict='CLEAN on HEAD, but checks NOT verifiably green (failing, pending, missing, or unreadable) — exit stays nonzero (fail closed)'
    else
      verdict='CLEAN on HEAD, checks green'; rc=0
    fi
  else
    verdict='no verdict on HEAD yet'
  fi
  echo "  Verdict line:        $verdict"
  return $rc
}

cmd_threads() { list_threads; }

cmd_checks() {
  local sha runs; sha=$(head_sha)
  echo "HEAD $sha"
  if ! runs=$(check_runs_json "$sha"); then
    echo "ERROR: check-runs unreadable OR none exist on this sha (workflows not triggered yet?) — treat as NOT green (fail closed)."
    return 1
  fi
  printf '%s' "$runs" | jq -r '.[]|"\(.conclusion // .status)\t\(.name)"' 2>/dev/null | sort
  echo "---"
  # STRICT by default: only conclusion=="success" is passing. Pending runs
  # (conclusion=null: queued/in_progress/waiting) always hold the gate, and
  # neutral/skipped do NOT pass either — a required job skipped by a workflow
  # condition or misconfiguration must not read as green. For repos where some
  # jobs are skipped BY DESIGN on PRs (matrix tests, smoke jobs, production
  # deploys), set ALLOW_SKIPPED=1 to accept neutral/skipped; they are still
  # listed above so the operator sees exactly what did not run.
  local nonpass
  nonpass=$(printf '%s' "$runs" | nonpassing_count)
  echo "non-passing check-runs: ${nonpass:-unknown (read failed — treat as NOT green)}"
  # Exit code IS the gate signal: nonzero whenever the surface is not
  # verifiably green (failures, pending runs, or an unreadable count), so a
  # wrapper keying off the exit status cannot mistake red/pending CI for a pass.
  [[ "${nonpass:-}" =~ ^[0-9]+$ ]] && [ "$nonpass" -eq 0 ] && return 0
  return 1
}

cmd_resolve() {
  # Resolve EVERY thread you addressed — pass one or more GraphQL thread IDs
  # (from `threads` / `status`). Resolving is load-bearing, not just tidiness:
  # the clean-gate counts unresolved review threads, so an addressed-but-unresolved
  # thread keeps the PR reading as FINDINGS and the cycle never converges.
  #
  # Ownership guard: resolveReviewThread accepts ANY global node ID, so a
  # pasted ID from another PR or repo would silently mutate a foreign
  # conversation. Verify each thread belongs to $SLUG#$PR first; refuse otherwise.
  #
  # Exit code IS the signal: returns nonzero unless EVERY requested thread was
  # actually resolved — a refused or failed ID must not read as "addressed" to
  # a wrapper keying off the exit status.
  local id resp ok owner_repo pr_num failures=0
  for id in "$@"; do
    resp=$(gh api graphql -f query='query($id:ID!){node(id:$id){... on PullRequestReviewThread{pullRequest{number repository{nameWithOwner}}}}}' \
      -f id="$id" 2>/dev/null)
    owner_repo=$(printf '%s' "$resp" | jq -r '.data.node.pullRequest.repository.nameWithOwner // ""' 2>/dev/null)
    pr_num=$(printf '%s' "$resp" | jq -r '.data.node.pullRequest.number // ""' 2>/dev/null)
    if [ "$owner_repo" != "$SLUG" ] || [ "$pr_num" != "$PR" ]; then
      echo "  $id -> REFUSED (belongs to ${owner_repo:-unknown}#${pr_num:-?}, not $SLUG#$PR)"
      failures=$((failures + 1))
      continue
    fi
    # capture the raw response (gh bypasses --jq and dumps raw JSON on GraphQL
    # errors), then format both the success and error cases cleanly.
    resp=$(gh api graphql -f query='mutation($id:ID!){resolveReviewThread(input:{threadId:$id}){thread{isResolved}}}' \
      -f id="$id" 2>/dev/null)
    ok=$(printf '%s' "$resp" | jq -r 'if .errors then "FAILED ("+(.errors[0].message)+")" elif .data.resolveReviewThread.thread.isResolved then "resolved" else "NOT-RESOLVED" end' 2>/dev/null)
    echo "  $id -> ${ok:-FAILED (no response)}"
    [ "${ok:-}" = "resolved" ] || failures=$((failures + 1))
  done
  if [ "$failures" -gt 0 ]; then
    echo "  $failures thread(s) NOT resolved — fix and re-run before treating them as addressed."
    return 1
  fi
}

# ---- dispatch ---------------------------------------------------------------

[ $# -ge 3 ] || die "usage: codex-review.sh <request|await|status|threads|checks> <owner/repo> <pr>  |  codex-review.sh resolve <owner/repo> <pr> <threadId...>"
CMD="$1"; SLUG="$2"; PR="$3"
[[ "$SLUG" == */* ]] || die "second arg must be owner/repo (got '$SLUG')"
OWNER="${SLUG%%/*}"; NAME="${SLUG##*/}"

case "$CMD" in
  request) cmd_request ;;
  await)   cmd_await "${4:-0}" ;;   # optional 4th arg: only verdicts posted after this comment id count
  status)  cmd_status "${4:-0}" ;;  # optional 4th arg: same verdict floor as await
  threads) cmd_threads ;;
  checks)  cmd_checks ;;
  resolve) [ $# -ge 4 ] || die "usage: codex-review.sh resolve <owner/repo> <pr> <threadId...>"
           shift 3; cmd_resolve "$@" ;;   # remaining args are thread IDs (verified against <owner/repo>#<pr>)
  *) die "unknown command '$CMD'" ;;
esac
