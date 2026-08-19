#!/usr/bin/env bash
# check_public_surface.sh — this repository is PUBLIC, so every tracked byte is
# published. This gate fails when internal ShardPilot material appears in the
# part of the tree it covers.
#
# WHY IT EXISTS. The org-wide publication-readiness check (in the internal `qa`
# repository) enumerates publication CANDIDATES — repositories that are private
# and might be flipped. This repository is not a candidate, because it is
# already public. Nothing scanned it, and that is exactly how an internal
# review-process skill, an internal decision-record id, an internal service
# name and an internal commit sha came to sit in a public repository for
# months. A gate that runs where the exposure already exists is the fix.
#
# ── SCOPE, STATED SO A PASS IS NOT READ AS MORE THAN IT IS ───────────────────
# LANE A is GATED at zero: every tracked file that is not Go source.
# LANE B is REPORTED, NOT GATED: Go source (*.go). Those comments are owned by
#   another workstream (the SDK wire freeze) and editing them here would collide
#   with it. Lane B prints its count on every run so the debt stays visible
#   instead of being implied away by lane A's green.
# Neither lane looks at git HISTORY. Deleting a line does not unpublish the
# commit that carried it.
set -euo pipefail

cd "$(dirname "$0")/.."

# One pattern list, used by both lanes — two spellings is how the second lane
# comes to check something different from the first.
PATTERNS='ADR-[0-9]+|control[-_ ]plane|analytics[-_ ]service|crash[-_ ]symbolicator|metadata[-_ ]api|admin[-_ ]service|work[-_ ]service|ai[-_ ]platform|admindoor|dbmigrate|shardpilot/(docs|qa|infra|integrations|console|marketing-site|design-system|admin-console|billing-service|status-page)|[Pp]roject[-_ ][Tt]ower|thunderstrike|shepherd-pr|verification-discipline|Codex (review|#|unity#|unreal#|godot#|defold#|go#)'

# Prove the matcher before trusting a clean result: a regex that matches
# nothing reports every repository clean, and prints the same line as a pass.
SELFTEST='per ADR-0221 §3
the control-plane assignment endpoint
pinned to analytics-service main @ 7d118c5
see shardpilot/docs for context
Project Tower-specific event names
the shepherd-pr skill
(Codex go#48 round 3)'
selftest_misses=0
while IFS= read -r line; do
  [ -z "$line" ] && continue
  if ! printf '%s\n' "$line" | grep -qE "$PATTERNS"; then
    printf 'SELFTEST MISS: %s\n' "$line" >&2
    selftest_misses=$((selftest_misses + 1))
  fi
done <<EOF
$SELFTEST
EOF
if [ "$selftest_misses" -ne 0 ]; then
  echo "REFUSING: the pattern list failed its own self-test ($selftest_misses miss(es))." >&2
  echo "  A scan that cannot match known-internal strings would report this" >&2
  echo "  repository clean by finding nothing. Fix the patterns, not the test." >&2
  exit 2
fi
echo "matcher self-test: OK (7/7 known-internal strings matched)"

lane_a_hits=""
lane_b_files=0
lane_b_hits=0

while IFS= read -r f; do
  [ -f "$f" ] || continue
  case "$f" in
    scripts/check_public_surface.sh) continue ;;  # this file names them on purpose
  esac
  hits="$(grep -nE "$PATTERNS" "$f" 2>/dev/null || true)"
  [ -z "$hits" ] && continue
  case "$f" in
    *.go)
      lane_b_files=$((lane_b_files + 1))
      lane_b_hits=$((lane_b_hits + $(printf '%s\n' "$hits" | grep -c '' )))
      ;;
    *)
      lane_a_hits="${lane_a_hits}$(printf '%s\n' "$hits" | sed "s|^|${f}:|")
"
      ;;
  esac
done <<EOF
$(git ls-files)
EOF

echo
echo "LANE B (REPORTED, NOT GATED) — Go source: ${lane_b_files} file(s), ${lane_b_hits} line(s)."
if [ "$lane_b_files" -eq 0 ]; then
  echo "  LANE B IS EMPTY. The debt this lane tracked is paid: fold *.go into lane A"
  echo "  and delete this section, so the scope note stops describing a gap that"
  echo "  no longer exists."
else
  echo "  These are doc comments that publish verbatim to pkg.go.dev. They are owed"
  echo "  work, not accepted risk, and they are not gated HERE because this"
  echo "  repository's Go sources are owned by the SDK wire-freeze workstream."
fi

echo
if [ -n "$(printf '%s' "$lane_a_hits" | tr -d '[:space:]')" ]; then
  echo "FAIL — internal material in the published non-source surface:" >&2
  printf '%s' "$lane_a_hits" >&2
  exit 1
fi
echo "LANE A (GATED) — non-source tracked files: clean."
