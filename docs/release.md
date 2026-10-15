# Release

The ShardPilot Go SDK is published as semver git tags plus a GitHub Release
per tag. There is no packaging step: Go modules
resolve `go get github.com/shardpilot/shardpilot-go@vX.Y.Z` directly off the
git tag. The version surfaces that must agree are the README's Installation
section, `CHANGELOG.md`, the integration skill's install pin
(`.claude/skills/shardpilot-go-integration/SKILL.md`), and the in-code
version constant `Version` in `internal/sdkversion/sdkversion.go`, which every
request sends in its `X-ShardPilot-SDK` header.
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
   - updates `.claude/skills/shardpilot-go-integration/SKILL.md` to the same
     tag in both its single install command and its single "pinned release
     tag" intro claim (and re-verifies the skill's behavioral claims against
     any surface changes shipping in the release), and
   - sets `Version` in `internal/sdkversion/sdkversion.go` to the same
     version without the leading `v` (`vX.Y.Z-alpha` becomes `X.Y.Z-alpha`).
2. CI enforces the consistency: `scripts/check_release_consistency.sh` (the
   `release-consistency` job in `.github/workflows/ci.yml`) fails any PR
   where the README's latest-tag claim, the README's install command, the
   topmost released CHANGELOG section, or the integration skill's install
   pin disagree. `TestVersionIsTheReleaseTag` (run by `go test ./...`) fails
   any PR where `Version` disagrees with the README's latest-tag claim.
3. After the release PR merges, tag `vX.Y.Z(-alpha)` at the merge commit and
   publish a GitHub Release for the tag, marked **pre-release** for `-alpha`
   versions, with the release notes copied from that CHANGELOG section.
   `scripts/check_release_consistency.sh --release` additionally asserts the
   tag exists — run it at the tagged commit before publishing the Release.

**A tag and a GitHub Release require explicit release authorization, and must
not be cut without it.**

The reason the requirement exists is in the next section: a tag is not
reversible. Cutting one is the only irreversible step in this procedure.

## Secret-scan corpus

The history scan prints the total reachable commit count and ref count beside its
expected material count and the scanner's observed count. It also binds HEAD, refs
and material commit identities with a before/after snapshot. A missing, ambiguous,
zero or mismatched reading refuses the check. A scanner error or a corpus change
also refuses it. Shallow history is refused before measuring the expected corpus.

History uses `--full-history --all --diff-merges=first-parent --diff-filter=d`.
This includes merge-resolution additions and file type changes. Git's NUL-delimited
numstat provides text-change counts while filenames and rename pairs stay opaque.
A modified file that only loses lines still contributes a text patch. Commits
without text patches, such as empty and metadata-only changes, are counted
separately. The expected patch corpus is measured before invoking gitleaks.

`python3 scripts/test_secret_scan.py` exercises the checker and confirms that the
exact synthetic sanitiser literal retains its exception while a larger containing
value is detected. The CI variant adds `--container`; both use gitleaks 8.30.1.
Workflow trust and exception-review policy remain tracked separately in
[#97](https://github.com/shardpilot/shardpilot-go/issues/97) and
[#102](https://github.com/shardpilot/shardpilot-go/issues/102).

## Bad versions

Never delete or move a published tag — Go module proxies cache tagged
versions forever. Instead, add a `retract` directive to `go.mod` and release
a corrected version. For the current release, the precedent is
`retract [v0.1.0, v0.1.2] // superseded pre-alpha line`, retiring the stale
`v0.1.x` line. Consumers move to the current announced pin in the
[README Installation section](../README.md#installation). Retain that range
in later releases rather than recommending another retired version.
The retract directive itself
only takes effect once it is part of a released version, so ship it with the
corrective release.
