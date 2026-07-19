# Release

The ShardPilot Go SDK is published as semver git tags plus a GitHub Release
per tag (currently `v0.4.0-alpha`). There is no packaging step: Go modules
resolve `go get github.com/shardpilot/shardpilot-go@vX.Y.Z` directly off the
git tag. The SDK carries no in-code version constant, so the version surfaces
that must agree are the README's Installation section, `CHANGELOG.md`, and
the integration skill's install pin
(`.claude/skills/shardpilot-go-integration/SKILL.md`).
This is an early alpha pre-release: the API is unstable and may change before
v1.

## Cutting a release

1. Open a single release PR that:
   - rolls the `## Unreleased` section of `CHANGELOG.md` into a new
     `## vX.Y.Z(-alpha) — YYYY-MM-DD — short summary` section (leave
     `## Unreleased` in place, empty or freshly restarted),
   - updates the README Installation section's latest-tag claims to match:
     the `go get github.com/shardpilot/shardpilot-go@vX.Y.Z` install command
     and the "`vX.Y.Z` is the latest tag." sentence, and
   - updates the single install command in the Install section of
     `.claude/skills/shardpilot-go-integration/SKILL.md` to the same tag
     (and re-verifies the skill's behavioral claims against any surface
     changes shipping in the release).
2. CI enforces the consistency: `scripts/check_release_consistency.sh` (the
   `release-consistency` job in `.github/workflows/ci.yml`) fails any PR
   where the README's latest-tag claim, the README's install command, the
   topmost released CHANGELOG section, or the integration skill's install
   pin disagree.
3. After the release PR merges, tag `vX.Y.Z(-alpha)` at the merge commit and
   publish a GitHub Release for the tag, marked **pre-release** for `-alpha`
   versions, with the release notes copied from that CHANGELOG section.
   `scripts/check_release_consistency.sh --release` additionally asserts the
   tag exists — run it at the tagged commit before publishing the Release.

Tags and GitHub Releases require explicit release authorization per
ADR-0161 in `shardpilot/docs` — do not tag or publish without it.

## Bad versions

Never delete or move a published tag — Go module proxies cache tagged
versions forever. Instead, add a `retract` directive to `go.mod` and release
a corrected version. Precedent: `v0.1.0` is retracted
(`retract v0.1.0 // documentation re-cut; superseded by v0.1.1`) and
consumers were pointed at `v0.1.1`/`v0.1.2`. The retract directive itself
only takes effect once it is part of a released version, so ship it with the
corrective release.
