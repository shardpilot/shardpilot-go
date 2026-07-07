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
#   codex-review.sh await   <owner/repo> <pr>       # poll for the verdict on HEAD (≤20 min).      RUN IN BACKGROUND.
#   codex-review.sh status  <owner/repo> <pr>       # one-shot snapshot — the re-check tool.         foreground
#   codex-review.sh threads <owner/repo> <pr>       # list unresolved Codex threads (id/path/body).  foreground
#   codex-review.sh resolve <owner/repo> <threadId...> # mark one or more addressed threads resolved. foreground
#   codex-review.sh checks  <owner/repo> <pr>       # GitHub Actions check-runs on HEAD.             foreground
#
# Exit codes:
#   0  ok / clean verdict on HEAD
#   2  findings present (unresolved Codex threads)
#   3  Codex never acknowledged after 5 attempts (it may be down — stop, tell the user)
#   4  timed out (>20 min) — review likely never started or the read is wrong; check manually
#  64  usage error

set -uo pipefail

CODEX_PREFIX="chatgpt-codex-connector"   # matches both the REST "...[bot]" login and the GraphQL "..." login via startswith()
CLEAN_RE="didn't find any major issues|chef's kiss"

ts()  { date +%H:%M:%S; }
die() { echo "error: $*" >&2; exit 64; }

# ---- shared accessors -------------------------------------------------------

head_sha() { gh api "repos/$SLUG/pulls/$PR" --jq '.head.sha' 2>/dev/null; }

# Latest Codex top-level issue-comment body (the clean-verdict channel).
# NOTE: gh rejects `--slurp` together with `--jq`, so pipe to a standalone jq.
# `--slurp` gives an array-of-pages; `add` flattens it into one array, so `last`
# is the GLOBAL last bot comment — not the per-page last you'd get from `--jq`
# running once per page under `--paginate`.
latest_bot_comment_body() {
  gh api "repos/$SLUG/issues/$PR/comments?per_page=100" --paginate --slurp 2>/dev/null \
    | jq -r '(add // []) | map(select((.user.login // "")|startswith("'"$CODEX_PREFIX"'"))) | (last.body // "")' 2>/dev/null
}

# Highest Codex issue-comment id (0 if none) — used to detect *fresh* Codex output
# since a request, regardless of whether the 👀 reaction was caught.
latest_bot_comment_id() {
  gh api "repos/$SLUG/issues/$PR/comments?per_page=100" --paginate --slurp 2>/dev/null \
    | jq -r '[(add // [])[] | select((.user.login // "")|startswith("'"$CODEX_PREFIX"'")) | .id] | (max // 0)' 2>/dev/null
}

# The "Reviewed commit: `<sha>`" line is the authoritative "reviewed THIS commit"
# signal — more reliable than a review object's commit_id, which can stay pinned
# to an older findings review.
reviewed_sha_from() { grep -oiE 'reviewed commit[^0-9a-f]*[0-9a-f]{7,40}' | grep -oiE '[0-9a-f]{7,40}' | head -1; }

graphql_threads() {
  gh api graphql -f query='
    query($o:String!,$r:String!,$n:Int!){
      repository(owner:$o,name:$r){
        pullRequest(number:$n){
          reviewThreads(first:100){ nodes{
            id isResolved isOutdated
            comments(first:1){ nodes{ author{login} body path line } }
          } }
        }
      }
    }' -f o="$OWNER" -f r="$NAME" -F n="$PR" 2>/dev/null
}

unresolved_count() {
  local n
  n=$(graphql_threads | jq '[(.data.repository.pullRequest.reviewThreads.nodes // [])[]
        | select(.isResolved==false and ((.comments.nodes[0].author.login // "")|startswith("'"$CODEX_PREFIX"'")))] | length' 2>/dev/null)
  [[ "${n:-}" =~ ^[0-9]+$ ]] || n=0
  echo "$n"
}

list_threads() {
  graphql_threads | jq -r '(.data.repository.pullRequest.reviewThreads.nodes // [])
    | map(select(.isResolved==false and ((.comments.nodes[0].author.login // "")|startswith("'"$CODEX_PREFIX"'"))))
    | if length==0 then "  (none)"
      else .[] | "  • \(.id)  \(.comments.nodes[0].path // "—"):\(.comments.nodes[0].line // "?")\(if .isOutdated then " (outdated)" else "" end)\n    \((.comments.nodes[0].body // "")|gsub("[\n\r]+";" ")|.[0:240])"
      end' 2>/dev/null
}

# ---- subcommands ------------------------------------------------------------

cmd_request() {
  local attempt cid i eyes_c eyes_p base_cid base_unres new_cid new_unres
  base_cid=$(latest_bot_comment_id); [[ "$base_cid"   =~ ^[0-9]+$ ]] || base_cid=0
  base_unres=$(unresolved_count);    [[ "$base_unres" =~ ^[0-9]+$ ]] || base_unres=0
  for attempt in 1 2 3 4 5; do
    cid=$(gh api -X POST "repos/$SLUG/issues/$PR/comments" -f body='@codex review' --jq '.id' 2>/dev/null)
    echo "[$(ts)] attempt $attempt/5 — posted '@codex review' (comment ${cid:-?})"
    for i in $(seq 1 10); do          # 10 × 30s = 5 min per attempt
      sleep 30
      eyes_c=$(gh api "repos/$SLUG/issues/comments/${cid:-0}/reactions" --jq '[.[].content]|index("eyes")' 2>/dev/null)
      eyes_p=$(gh api "repos/$SLUG/issues/$PR/reactions"               --jq '[.[].content]|index("eyes")' 2>/dev/null)
      new_cid=$(latest_bot_comment_id); [[ "$new_cid" =~ ^[0-9]+$ ]] || new_cid=0
      new_unres=$(unresolved_count);    [[ "$new_unres" =~ ^[0-9]+$ ]] || new_unres=0
      # Acknowledged if Codex reacted (👀) OR has already produced fresh output
      # since the request — a new bot comment (clean verdict) or new unresolved
      # threads (findings). The 👀 can be transient or skipped, so never rely on
      # it alone, or a working review gets needlessly re-asked up to 5×.
      if { [ -n "${eyes_c:-}" ] && [ "$eyes_c" != "null" ]; } || { [ -n "${eyes_p:-}" ] && [ "$eyes_p" != "null" ]; }; then
        echo "[$(ts)] 👀 Codex acknowledged — review running. Next: codex-review.sh await $SLUG $PR"; return 0
      fi
      if [ "$new_cid" -gt "$base_cid" ] || [ "$new_unres" -gt "$base_unres" ]; then
        echo "[$(ts)] Codex already responded (fresh output since request) — go to: codex-review.sh await $SLUG $PR"; return 0
      fi
    done
    echo "[$(ts)] no 👀 / no Codex output after 5 min — re-asking."
  done
  echo "[$(ts)] GAVE UP: no 👀 and no Codex output after 5 attempts (~25 min). Codex may be down — stop and tell the user."
  return 3
}

cmd_await() {
  local start now elapsed sha sha7 body rsha rsha7 clean unresolved settle_done=0
  start=$(date +%s)
  while :; do
    now=$(date +%s); elapsed=$((now - start))
    if [ "$elapsed" -gt 1200 ]; then
      echo "[$(ts)] TIMEOUT (>20 min). A real review almost never exceeds 20 min — it likely never started (no 👀?) or the read is wrong. Check manually with: codex-review.sh status $SLUG $PR"
      return 4
    fi
    sha=$(head_sha); sha7=${sha:0:7}
    body=$(latest_bot_comment_body)
    rsha=$(printf '%s' "$body" | reviewed_sha_from); rsha7=${rsha:0:7}
    if printf '%s' "$body" | grep -qiE "$CLEAN_RE"; then clean=yes; else clean=no; fi
    unresolved=$(unresolved_count)

    # Findings are decided by UNRESOLVED INLINE THREADS (GraphQL), never by review
    # state — Codex always uses state COMMENTED (apps can't self-APPROVE), so a
    # COMMENTED review is NOT itself a "findings" signal.
    if [ "${unresolved:-0}" -gt 0 ]; then
      echo "[$(ts)] FINDINGS — $unresolved unresolved Codex thread(s):"
      list_threads
      return 2
    fi

    # Clean = verdict text + Reviewed-commit == HEAD + 0 unresolved. Settle once
    # (~3.5 min) before declaring it, because inline comments can lag the verdict.
    if [ "$clean" = yes ] && [ -n "$rsha7" ] && [ "$rsha7" = "$sha7" ]; then
      if [ "$settle_done" -eq 0 ]; then
        echo "[$(ts)] clean verdict on HEAD ($sha7) — settling 3.5 min for any lagging inline comments…"
        settle_done=1; sleep 210; continue
      fi
      echo "[$(ts)] CLEAN — Codex verdict on HEAD $sha7, 0 unresolved threads:"
      echo "----"; printf '%s\n' "$body" | head -6
      return 0
    fi

    echo "[$(ts)] waiting… HEAD=$sha7 reviewed=${rsha7:-none} clean=$clean unresolved=${unresolved:-0} (elapsed ${elapsed}s)"
    sleep 45
  done
}

cmd_status() {
  local sha sha7 body rsha rsha7 clean unresolved
  sha=$(head_sha); sha7=${sha:0:7}
  body=$(latest_bot_comment_body)
  rsha=$(printf '%s' "$body" | reviewed_sha_from); rsha7=${rsha:0:7}
  if printf '%s' "$body" | grep -qiE "$CLEAN_RE"; then clean=yes; else clean=no; fi
  unresolved=$(unresolved_count)

  echo "PR $SLUG#$PR"
  echo "  HEAD                 $sha7"
  echo "  Codex reviewed       ${rsha7:-none}  $([ -n "$rsha7" ] && [ "$rsha7" = "$sha7" ] && echo '== HEAD ✓' || echo '(not HEAD — re-review pending or none)')"
  echo "  clean-verdict text   $clean"
  echo "  unresolved threads   ${unresolved:-0}"
  echo "  mergeable            $(gh api graphql -f query='query($o:String!,$r:String!,$n:Int!){repository(owner:$o,name:$r){pullRequest(number:$n){mergeStateStatus mergeable}}}' -f o="$OWNER" -f r="$NAME" -F n="$PR" --jq '.data.repository.pullRequest|"\(.mergeable) / mergeStateStatus=\(.mergeStateStatus)"' 2>/dev/null)"
  echo "  --- GitHub Actions ---"
  gh api "repos/$SLUG/commits/$sha/check-runs?per_page=100" --jq '.check_runs[]|"    \(.conclusion // .status)\t\(.name)"' 2>/dev/null | sort
  echo "  --- unresolved Codex threads ---"
  list_threads
  echo ""
  echo "  Verdict line:        $([ "$clean" = yes ] && [ "$rsha7" = "$sha7" ] && [ "${unresolved:-0}" -eq 0 ] && echo 'CLEAN on HEAD' || ([ "${unresolved:-0}" -gt 0 ] && echo 'FINDINGS' || echo 'no verdict on HEAD yet'))"
}

cmd_threads() { list_threads; }

cmd_checks() {
  local sha; sha=$(head_sha)
  echo "HEAD $sha"
  gh api "repos/$SLUG/commits/$sha/check-runs?per_page=100" --jq '.check_runs[]|"\(.conclusion // .status)\t\(.name)"' 2>/dev/null | sort
  echo "---"
  gh api "repos/$SLUG/commits/$sha/check-runs?per_page=100" \
    --jq '[.check_runs[]|select(.conclusion!=null and .conclusion!="success" and .conclusion!="neutral" and .conclusion!="skipped")]|length' 2>/dev/null \
    | xargs -I{} echo "non-passing check-runs: {}"
}

cmd_resolve() {
  # Resolve EVERY thread you addressed — pass one or more GraphQL thread IDs
  # (from `threads` / `status`). Resolving is load-bearing, not just tidiness:
  # the clean-gate counts unresolved Codex threads, so an addressed-but-unresolved
  # thread keeps the PR reading as FINDINGS and the cycle never converges.
  local id resp ok
  for id in "$@"; do
    # capture the raw response (gh bypasses --jq and dumps raw JSON on GraphQL
    # errors), then format both the success and error cases cleanly.
    resp=$(gh api graphql -f query='mutation($id:ID!){resolveReviewThread(input:{threadId:$id}){thread{isResolved}}}' \
      -f id="$id" 2>/dev/null)
    ok=$(printf '%s' "$resp" | jq -r 'if .errors then "FAILED ("+(.errors[0].message)+")" elif .data.resolveReviewThread.thread.isResolved then "resolved" else "NOT-RESOLVED" end' 2>/dev/null)
    echo "  $id -> ${ok:-FAILED (no response)}"
  done
}

# ---- dispatch ---------------------------------------------------------------

[ $# -ge 3 ] || die "usage: codex-review.sh <request|await|status|threads|resolve|checks> <owner/repo> <pr | threadId...>"
CMD="$1"; SLUG="$2"; PR="$3"
[[ "$SLUG" == */* ]] || die "second arg must be owner/repo (got '$SLUG')"
OWNER="${SLUG%%/*}"; NAME="${SLUG##*/}"

case "$CMD" in
  request) cmd_request ;;
  await)   cmd_await ;;
  status)  cmd_status ;;
  threads) cmd_threads ;;
  checks)  cmd_checks ;;
  resolve) shift 2; cmd_resolve "$@" ;;   # remaining args are thread IDs
  *) die "unknown command '$CMD'" ;;
esac
