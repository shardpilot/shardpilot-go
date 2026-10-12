#!/usr/bin/env bash
# Version-consistency check: README's "latest tag" claim must match the
# topmost released (non-Unreleased) section of CHANGELOG.md, the integration
# skill's pinned install command must match the README, and the in-code
# version constant (internal/sdkversion, sent in the X-ShardPilot-SDK request
# header) must be that tag without its leading "v".
#
# Go modules version via git tags; the constant exists because the SDK must
# name its own version on the wire. The consistency surface is README ↔
# CHANGELOG ↔ the customer-facing integration skill
# (.claude/skills/shardpilot-go-integration/SKILL.md) ↔ the constant. This runs
# on every CI build and must not require git tags to be present (CI checkouts
# are shallow and tagless).
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

# 5. The skill's user-facing prose claim ("pinned release tag `vX.Y.Z`") must
#    name that same tag — the install command alone is not what readers (or
#    AI tools) treat as the described-version statement. Exactly one such
#    claim must exist so a stale duplicate cannot linger, and it must sit on
#    a single line (like the README's latest-tag sentence) to stay greppable.
skill_claims="$(grep -oE "pinned release tag \`${semver_re}\`" "$skill_md" || true)"
[ -n "$skill_claims" ] || fail "$skill_md: no 'pinned release tag \`vX.Y.Z\`' claim found (the claim must appear on a single line)"
[ "$(printf '%s\n' "$skill_claims" | wc -l)" -eq 1 ] || \
  fail "$skill_md: multiple 'pinned release tag' claims found (keep exactly one):"$'\n'"$skill_claims"
skill_claim_version="$(printf '%s\n' "$skill_claims" | grep -oE "$semver_re" | head -n 1)"

# 6. The in-code version constant the identity header is built from.
version_go="internal/sdkversion/sdkversion.go"
[ -f "$version_go" ] || fail "$version_go: file not found"
code_versions="$(grep -oE '^const Version = "[^"]*"' "$version_go" || true)"
[ "$(printf '%s\n' "$code_versions" | grep -c .)" -eq 1 ] || \
  fail "$version_go: expected exactly one 'const Version = \"...\"' line"
code_version="$(printf '%s\n' "$code_versions" | sed -E 's/^const Version = "([^"]*)"$/\1/')"

printf '%-30s %s\n' "README latest-tag claim:" "$readme_latest"
printf '%-30s %s\n' "README install command tag:" "$readme_goget_version"
printf '%-30s %s\n' "CHANGELOG topmost release:" "$changelog_latest"
printf '%-30s %s\n' "SKILL.md install command tag:" "$skill_goget_version"
printf '%-30s %s\n' "SKILL.md pinned-release claim:" "$skill_claim_version"
printf '%-30s %s\n' "sdkversion.Version constant:" "$code_version"

[ "$readme_latest" = "$changelog_latest" ] || \
  fail "README claims latest tag $readme_latest but CHANGELOG's topmost released section is $changelog_latest"
[ "$readme_goget_version" = "$readme_latest" ] || \
  fail "README's first install command targets $readme_goget_version but the latest-tag claim is $readme_latest"
[ "$skill_goget_version" = "$readme_latest" ] || \
  fail "$skill_md pins $skill_goget_version but the README's latest-tag claim is $readme_latest (update the skill's Install section in the release PR)"
[ "$skill_claim_version" = "$readme_latest" ] || \
  fail "$skill_md's pinned-release claim names $skill_claim_version but the README's latest-tag claim is $readme_latest (update the skill's intro claim in the release PR)"

[ "v$code_version" = "$readme_latest" ] || \
  fail "$version_go sets Version $code_version but the README's latest-tag claim is $readme_latest (bump the constant in the release PR; no leading v, no build metadata)"

if $release_mode; then
  if git rev-parse -q --verify "refs/tags/${readme_latest}" >/dev/null; then
    printf '%-30s %s\n' "git tag exists:" "$readme_latest"
  else
    fail "--release: git tag $readme_latest does not exist (create it at the release commit before publishing)"
  fi
fi

echo "OK: release version references are consistent ($readme_latest)"
