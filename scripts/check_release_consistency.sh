#!/usr/bin/env bash
# Version-consistency check: README's "latest tag" claim must match the
# topmost released (non-Unreleased) section of CHANGELOG.md, and the
# integration skill's pinned install command must match the README.
#
# The Go SDK has no in-code version constant — Go modules version via git
# tags — so the consistency surface is README ↔ CHANGELOG ↔ the
# customer-facing integration skill (.claude/skills/shardpilot-go-integration/
# SKILL.md). This runs on every CI build and must not require git tags to be
# present (CI checkouts are shallow and tagless).
#
# Usage:
#   scripts/check_release_consistency.sh             # README ↔ CHANGELOG ↔ SKILL.md
#   scripts/check_release_consistency.sh --release   # additionally assert the
#                                                    # git tag itself exists
set -euo pipefail

cd "$(dirname "$0")/.."

release_mode=false
if [ "${1:-}" = "--release" ]; then
  release_mode=true
elif [ "$#" -gt 0 ]; then
  echo "usage: $0 [--release]" >&2
  exit 2
fi

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

semver_re='v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?'

# 1. README's Installation section claims a latest tag: "`vX.Y.Z` is the latest tag."
readme_claims="$(grep -oE "\`${semver_re}\` is the latest tag" README.md || true)"
[ -n "$readme_claims" ] || fail "README.md: no '\`vX.Y.Z\` is the latest tag' claim found"
[ "$(printf '%s\n' "$readme_claims" | wc -l)" -eq 1 ] || \
  fail "README.md: multiple 'is the latest tag' claims found:"$'\n'"$readme_claims"
readme_latest="$(printf '%s\n' "$readme_claims" | grep -oE "$semver_re" | head -n 1)"

# 2. The first `go get ...@vX.Y.Z` in the README installs that same latest tag.
readme_goget="$(grep -oE "go get github\.com/shardpilot/shardpilot-go@${semver_re}" README.md | head -n 1 || true)"
[ -n "$readme_goget" ] || fail "README.md: no 'go get github.com/shardpilot/shardpilot-go@vX.Y.Z' line found"
readme_goget_version="${readme_goget##*@}"

# 3. CHANGELOG.md's topmost released section (first '## v...' heading,
#    i.e. skipping '## Unreleased').
changelog_heading="$(grep -m 1 -E '^## v' CHANGELOG.md || true)"
[ -n "$changelog_heading" ] || fail "CHANGELOG.md: no released '## vX.Y.Z' section heading found"
changelog_latest="$(printf '%s\n' "$changelog_heading" | grep -oE "$semver_re" | head -n 1)"

# 4. The customer-facing integration skill pins the same tag in its install
#    command. Unlike the README (which also documents older pins), the skill
#    must carry exactly ONE install command, so any second/stale pin fails.
skill_md=".claude/skills/shardpilot-go-integration/SKILL.md"
[ -f "$skill_md" ] || fail "$skill_md: file not found (the integration skill must ship in-tree)"
skill_gogets="$(grep -oE "go get github\.com/shardpilot/shardpilot-go@${semver_re}" "$skill_md" || true)"
[ -n "$skill_gogets" ] || fail "$skill_md: no 'go get github.com/shardpilot/shardpilot-go@vX.Y.Z' install command found"
[ "$(printf '%s\n' "$skill_gogets" | wc -l)" -eq 1 ] || \
  fail "$skill_md: multiple install commands found (keep exactly one):"$'\n'"$skill_gogets"
skill_goget_version="${skill_gogets##*@}"

printf '%-29s %s\n' "README latest-tag claim:" "$readme_latest"
printf '%-29s %s\n' "README install command tag:" "$readme_goget_version"
printf '%-29s %s\n' "CHANGELOG topmost release:" "$changelog_latest"
printf '%-29s %s\n' "SKILL.md install command tag:" "$skill_goget_version"

[ "$readme_latest" = "$changelog_latest" ] || \
  fail "README claims latest tag $readme_latest but CHANGELOG's topmost released section is $changelog_latest"
[ "$readme_goget_version" = "$readme_latest" ] || \
  fail "README's first install command targets $readme_goget_version but the latest-tag claim is $readme_latest"
[ "$skill_goget_version" = "$readme_latest" ] || \
  fail "$skill_md pins $skill_goget_version but the README's latest-tag claim is $readme_latest (update the skill's Install section in the release PR)"

if $release_mode; then
  if git rev-parse -q --verify "refs/tags/${readme_latest}" >/dev/null; then
    printf '%-29s %s\n' "git tag exists:" "$readme_latest"
  else
    fail "--release: git tag $readme_latest does not exist (create it at the release commit before publishing)"
  fi
fi

echo "OK: release version references are consistent ($readme_latest)"
