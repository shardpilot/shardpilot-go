#!/usr/bin/env bash
# One control per refusal in the lane B ratchet.
#
# ⚠ WHY THIS EXISTS. Two of the last four findings on the ratchet were checks
# that were CORRECT two rounds earlier and were lost in a restructure: the
# target-format check vanished when the existence probe changed shape, and a
# pipefail fix covered one filter of two. Both survived review of the change
# that broke them, because after changing a mechanism I re-ran the case I had
# just changed rather than the cases the change could break.
#
# check_public_surface.sh has an in-process SELFTEST, but it scans an in-memory
# fixture: it cannot reach refusals that depend on the baseline FILE and on git
# state. So those are exercised here, against the real script in a real
# checkout, one condition at a time.
#
# ⚠ THIS SCRIPT MUTATES THE WORKING TREE AND INDEX and restores them. It refuses
# to start if anything is already dirty, because a failed restore over
# uncommitted work is a worse outcome than not running.
set -euo pipefail
cd "$(dirname "$0")/.."

GATE=scripts/check_public_surface.sh
BASELINE=scripts/public-surface-lane-b-baseline.txt
# ⚠ PINNED, NOT INHERITED. The gate reads LANE_B_BASELINE with a default, so an
# exported value would redirect --write-baseline at whatever the caller named --
# possibly outside this worktree, where neither the saved copy nor the EXIT trap
# could put it back. Every invocation below passes this explicitly, and the
# ambient value is dropped here so nothing can reach the gate by accident.
unset LANE_B_BASELINE
export LANE_B_BASELINE="$BASELINE"

# ⚠ AND THE PIN IS NOT ENOUGH ON ITS OWN. The gate is a bash script, and a
# non-interactive bash sources $BASH_ENV at startup -- AFTER this export -- so a
# startup file assigning LANE_B_BASELINE replaces the pinned path inside the
# child. Reproduced: with BASH_ENV set to a file naming /tmp, the writer
# overwrote that file and the trap could not restore it. ENV is the same hazard
# for a POSIX sh child.
unset BASH_ENV ENV

# ⚠ AND THE COMPARISON REF IS AMBIENT TOO. The gate reads
# PUBLIC_SURFACE_BASE_REF with a default, so an exported value reaches every
# plain `expect` below -- which are the controls meant to exercise the
# CURRENT-TREE behaviour and nothing else. An unresolvable inherited ref turns
# the clean control into an exit 2; a resolvable one with a smaller baseline
# turns it into the anti-cheat refusal. Either way the harness fails for a
# reason that has nothing to do with the control under test. expect_ref supplies
# it explicitly, per call, and is the only thing that may.
unset PUBLIC_SURFACE_BASE_REF
# ⚠ EVERY TARGET IS SYNTHESIZED FROM HEAD, and none is fetched. CI clones at
# depth 1, so `origin/main~1` does not exist there and a harness leaning on it
# would silently skip its own controls on the one machine that matters. Building
# them with plumbing needs no network, no history and no branch: the commits are
# unreachable the moment this run ends.
#
# All of it runs against a TEMPORARY INDEX, so the real one is never touched --
# which is what lets the dirty-tree guard above stay strict.
synth_target() {  # $1 = sed program applied to the baseline, or "" to delete it
  local idx blob tree
  idx="$(mktemp)"
  GIT_INDEX_FILE="$idx" git read-tree HEAD
  if [ -z "$1" ]; then
    GIT_INDEX_FILE="$idx" git update-index --force-remove "$BASELINE"
  else
    blob="$(git show "HEAD:$BASELINE" | sed "$1" | git hash-object -w --stdin)"
    GIT_INDEX_FILE="$idx" git update-index --cacheinfo "100644,$blob,$BASELINE"
  fi
  tree="$(GIT_INDEX_FILE="$idx" git write-tree)"
  rm -f "$idx"
  # ⚠ IDENTITY PASSED INLINE. commit-tree needs a committer, and a fresh CI
  # checkout has none configured -- this exits 128 with "Author identity
  # unknown" there while passing on any developer machine, which is the third
  # shape of "works where it is written, not where it runs" this ratchet has
  # produced. `-c` scopes it to the invocation; nothing in the repository's
  # config is touched, and these commits are unreachable anyway.
  git -c user.name="lane-b-ratchet-test" -c user.email="lane-b-ratchet-test@invalid" \
    commit-tree "$tree" -p HEAD -m "synthetic lane B target"
}

WITH_BASE="$(git rev-parse HEAD)"
WITHOUT_BASE="$(synth_target "")"

if [ -n "$(git status --porcelain)" ]; then
  echo "REFUSING: working tree is dirty; this test mutates it." >&2
  exit 2
fi

# ⚠ `git status` DOES NOT SEE EVERYTHING. A path marked --assume-unchanged or
# --skip-worktree is omitted from --porcelain, so the guard above passes while
# real uncommitted edits exist -- and a restore would then destroy work the
# guard promised was not there. `ls-files -v` prints those flags in LOWERCASE.
# ⚠ LOWERCASE **AND** `S`. `ls-files -v` reports assume-unchanged by lowercasing
# the status letter, but skip-worktree gets its own UPPERCASE `S` -- so a
# lowercase-only test walks straight past it. Measured: with config.go marked
# skip-worktree, `^[a-z]` matches 0 and `^[a-zS]` matches 1. That path is worse
# than assume-unchanged here, because `git checkout HEAD -- <path>` silently
# does nothing for it, so the restore would leave the probe in a developer's
# file. `H` (normal) and the transient M/R/C/K states are deliberately not
# matched: they hide nothing.
if git ls-files -v | grep -q '^[a-zS]'; then
  echo "REFUSING: some tracked paths carry assume-unchanged or skip-worktree." >&2
  echo "  git status cannot see edits to those, so the dirty-tree guard above" >&2
  echo "  is not answering the question it appears to answer." >&2
  git ls-files -v | grep '^[a-zS]' | head -5 >&2
  exit 2
fi

checks=0; failures=0
SAVED="$(mktemp)"; cp "$BASELINE" "$SAVED"

# ⚠ FROM HEAD, NOT FROM THE INDEX. `git checkout -- <path>` restores from the
# INDEX, and the cheat control below stages its own mutation, so an index-based
# restore is a no-op that leaves the tree modified -- and the next run then
# starts from a polluted state and every control fails at once. Observed while
# writing this. `reset --hard` is safe here only because the dirty-tree guard
# above refuses to start over uncommitted work.
# ⚠ ONLY THE PATHS THIS SCRIPT TOUCHES. A repository-wide `reset --hard` has a
# blast radius far larger than the mutations above, and it only looked safe
# because the guard was assumed to see all uncommitted work. Restoring by path
# keeps the damage bounded to what was deliberately broken, whatever the guard
# missed. NEW_FILE is removed rather than restored: HEAD does not have it.
RESTORE_PATHS=("$BASELINE" config.go)
NEW_FILE=zz_lane_b_probe.go

# ⚠ A HARD LINK IS A SECOND NAME FOR ONE INODE, AND GIT KNOWS ONLY THE FIRST.
# Linking config.go or the baseline to a path outside this worktree leaves
# `git status --porcelain` clean, so the dirty-tree guard passes. The appends
# below then write THROUGH the inode and change every name for it, while the
# restore replaces only the worktree entry -- with a NEW inode, since
# `git checkout` writes a fresh file. So the outside name keeps the harness's
# edit permanently, and nothing in this script can reach it to undo that.
#
# `ls -ld` field 2 is the link count on GNU and BSD alike; `stat` spells it
# differently on each, and this gate documents bash 3.2 systems as supported.
for linked in "${RESTORE_PATHS[@]}"; do
  links="$(ls -ld "$linked" 2>/dev/null | awk '{print $2}')"
  case "$links" in
    ''|*[!0-9]*)
      echo "REFUSING: could not read the hard-link count of $linked." >&2
      exit 2
      ;;
  esac
  if [ "$links" -gt 1 ]; then
    echo "REFUSING: $linked has $links hard links." >&2
    echo "  This test writes through the inode, so it would modify every other" >&2
    echo "  name for it, and the restore can only put back this one." >&2
    exit 2
  fi
done

# ⚠ SCRATCH NAMES ARE NOT AUTOMATICALLY FREE. An IGNORED file at one of these
# paths -- via .git/info/exclude or a global ignore -- is invisible to
# `git status --porcelain`, so the dirty-tree guard passes, the redirection
# below truncates it, and the trap then deletes it. The guard cannot see it, so
# the existence check has to be a plain filesystem test.
for scratch in "$NEW_FILE" "$BASELINE.bak" "$BASELINE.real" "$BASELINE.link" "$BASELINE.tmp" "$BASELINE.hard" config.go.tmp lane-b-parent-link escape; do
  if [ -e "$scratch" ] || [ -L "$scratch" ]; then
    echo "REFUSING: $scratch already exists; this test would overwrite and then delete it." >&2
    echo "  git status may not show it (an ignored path is invisible there), so" >&2
    echo "  its absence is checked directly rather than inferred." >&2
    exit 2
  fi
done
restore() {
  # $BASELINE.tmp is listed here as well as in the index cleanup below: the
  # rewrites above redirect into it and then `mv`, so an interruption between
  # the two leaves it UNTRACKED in the worktree, where `git rm --cached` cannot
  # reach it. Removing the record without removing the file is the same half-
  # restore as removing the file without the record.
  rm -f "$NEW_FILE" "$BASELINE.bak" "$BASELINE.real" "$BASELINE.link" "$BASELINE.tmp" "$BASELINE.hard" config.go.tmp lane-b-parent-link
  git checkout -q HEAD -- "${RESTORE_PATHS[@]}" 2>/dev/null || true
  # ⚠ EVERY SCRATCH PATH, NOT JUST THE PROBE. Controls stage before running, so
  # an interruption after the missing- or symlink-baseline setup leaves .bak,
  # .real or .link in the INDEX; deleting the files then leaves staged entries
  # that a later commit would carry. A restore that removes the evidence but not
  # the record is not a restore.
  git rm -q --cached --ignore-unmatch \
    "$NEW_FILE" "$BASELINE.bak" "$BASELINE.real" "$BASELINE.link" "$BASELINE.tmp" \
    "$BASELINE.hard" config.go.tmp lane-b-parent-link \
    >/dev/null 2>&1 || true
}
trap 'restore; rm -f "$SAVED"' EXIT

# judge <label> <rc> <want> <needle> <output> -- ONE judgement, every caller.
# The two halves are reported separately and the exit is checked first, because
# "exit 1 for the wrong reason" and "the right reason at the wrong exit" are
# different defects and a message that merges them names the wrong one. An
# earlier copy of this in expect_ref did exactly that: it printed "reason not
# found" whenever the EXIT disagreed, including when the reason was present.
# must_write_baseline <label> -- a SETUP write, and its status is checked.
# `--write-baseline || true` in a fixture is the quietest way to build a control
# that cannot fail: if the write does not happen, the old baseline stays, the
# gate still disagrees with the tree, and the control observes exactly the exit
# and message it wanted -- from a state it never established. Review found this
# in two of the controls added for this very kind of defect.
must_write_baseline() {
  local label="$1"
  if ! "$GATE" --write-baseline >/dev/null 2>&1; then
    echo "FAIL [$label]: setup --write-baseline failed, so this control would" >&2
    echo "  have judged a state it never reached." >&2
    failures=$((failures + 1))
  fi
}

judge() {
  local label="$1" rc="$2" want="$3" needle="$4" out="$5"
  if [ "$rc" -ne "$want" ]; then
    echo "FAIL [$label]: exit $rc, expected $want" >&2
    failures=$((failures + 1))
    return
  fi
  if [ -n "$needle" ] && ! printf '%s' "$out" | grep -qF -- "$needle"; then
    echo "FAIL [$label]: exit $want as expected, but the reason was not '$needle'" >&2
    echo "  -- an exit code alone does not say WHICH refusal fired, and two" >&2
    echo "     conditions sharing a code is how a check goes missing unnoticed." >&2
    failures=$((failures + 1))
  fi
}

# expect <label> <wanted-exit> <substring> -- runs the gate and judges both.
expect() {
  local label="$1" want="$2" needle="$3" rc=0 out
  checks=$((checks + 1))
  git add -A >/dev/null 2>&1 || true
  out="$("$GATE" 2>&1)" || rc=$?
  judge "$label" "$rc" "$want" "$needle" "$out"
}

expect "clean tree holds"            0 "LANE B RATCHET — held at"
mv "$BASELINE" "$BASELINE.bak"
expect "a missing baseline refuses"  2 "is missing"
mv "$BASELINE.bak" "$BASELINE"
ln -sf /dev/null "$BASELINE.link"; mv "$BASELINE" "$BASELINE.real"; ln -sf "$(basename "$BASELINE.real")" "$BASELINE"
expect "a symlinked baseline refuses" 2 "is a symlink in the index"
rm -f "$BASELINE" "$BASELINE.link"; mv "$BASELINE.real" "$BASELINE"
# `sed -i` with no argument is GNU-only too -- BSD sed reads the next word as
# the backup suffix. Write to a temporary file and move it, everywhere.
sed 's/^# format-version: .*/# format-version: 1/' "$BASELINE" > "$BASELINE.tmp"
mv "$BASELINE.tmp" "$BASELINE"
expect "a format skew refuses"       2 "this script reads"
cp "$SAVED" "$BASELINE"
# awk, not `sed '0,/re/'`: that address is a GNU extension and BSD sed rejects
# it, which would abort the harness before the later controls on exactly the
# macOS systems this gate's bash 3.2 compatibility exists for.
awk 'BEGIN{done=0} /^[0-9]/ && !done {sub(/^/, "9"); done=1} {print}' "$BASELINE" > "$BASELINE.tmp"
mv "$BASELINE.tmp" "$BASELINE"
expect "a disagreeing count fails"   1 "moved and the baseline did not agree"
cp "$SAVED" "$BASELINE"

# expect_env <label> <VAR=value> <wanted-exit> <substring> -- one ambient value,
# supplied per call, the way expect_ref supplies the comparison ref. The harness
# drops these variables globally on purpose, so a control that needs one has to
# hand it over deliberately rather than inherit it.
expect_env() {
  local label="$1" assign="$2" want="$3" needle="$4" rc=0 out
  checks=$((checks + 1))
  git add -A >/dev/null 2>&1 || true
  shift 4
  out="$(env "$assign" "$GATE" "$@" 2>&1)" || rc=$?
  judge "$label" "$rc" "$want" "$needle" "$out"
}

# expect_nostage <label> <wanted-exit> <substring> -- runs WITHOUT `git add -A`.
# Every other control stages first, which is right for them: the gate reads the
# index. But a refusal about the WORKING TREE cannot be driven that way -- stage
# a symlink and the index carries mode 120000, so the index check fires first
# and the working-tree check is never reached. The two refusals exist precisely
# because those are different states.
expect_nostage() {
  local label="$1" want="$2" needle="$3" rc=0 out
  checks=$((checks + 1))
  out="$("$GATE" 2>&1)" || rc=$?
  judge "$label" "$rc" "$want" "$needle" "$out"
}

expect_ref() {
  local label="$1" ref="$2" want="$3" needle="$4" rc=0 out
  checks=$((checks + 1))
  git add -A >/dev/null 2>&1 || true
  out="$(PUBLIC_SURFACE_BASE_REF="$ref" "$GATE" 2>&1)" || rc=$?
  judge "$label" "$rc" "$want" "$needle" "$out"
}

expect_ref "an unresolvable base refuses" "no/such/ref"   2 "does not resolve"
expect_ref "a target with no baseline skips" "$WITHOUT_BASE" 0 "carries no"
expect_ref "a target with one compares"     "$WITH_BASE"     0 "LANE B RATCHET — held at"

# ⚠ THE CHEAT, and the reason the target comparison exists at all: add a match
# AND rewrite the baseline to fit it, so the current tree and its own baseline
# agree perfectly. Every local check passes. Only the target catches it.
# ⚠ THE MARKER IS ASSEMBLED, NOT WRITTEN. This file is not *.go, so it is LANE A
# -- gated at zero -- and a literal reference here would make the harness itself
# the finding. The gate refused exactly that while this was being written. Same
# trick the gate uses on its own fixture data.
# ⚠ THE PAID STATE, which no other control reaches. Every one of them leaves at
# least one numeric row, so the comments-only baseline -- the shape this lane
# exists to arrive at -- is never parsed. Either status-1 allowance in the gate's
# two-filter read could be deleted with every other control green, and a real
# zero-debt baseline would then abort under pipefail. That allowance was one of
# the two silently-lost checks this harness was built for, so leaving it
# uncovered would be the fourth time it missed its own motive.
#
# The tree still carries 33 occurrences, so the correct outcome here is the
# ordinary count disagreement -- NOT an abort, and not a refusal.
grep '^#' "$SAVED" > "$BASELINE"
expect "a comments-only baseline is parsed, not fatal" 1 "moved and the baseline did not agree"
cp "$SAVED" "$BASELINE"

marker="ADR-"'0331'
printf '\n// See %s for the freeze this follows.\n' "$marker" >> config.go
git add -A >/dev/null 2>&1 || true
must_write_baseline "a raised baseline is caught by the target"
expect_ref "a raised baseline is caught by the target" "$WITH_BASE" 1 "the baseline was raised"
restore

# ⚠ A TARGET IN AN OLDER FORMAT, SYNTHESIZED -- because history has none. The
# format marker arrived with the file, so no real commit carries a format-1
# baseline, and without one the target-version check can be deleted and every
# control still passes. It was verified to survive exactly that mutation, which
# is why this exists: the check whose silent loss motivated this harness was the
# one the harness did not cover.
#
# ⚠ A FILE THE TARGET HAS NO ENTRY FOR. The cheat control above adds its match
# to config.go, which both baselines already list, so it exercises only the
# "count rose" branch. The separate refusal for a path ABSENT from the target --
# the first occurrence in a previously clean file -- could be deleted with all
# ten other controls still passing. That is the same gap the format-1 target had:
# a harness tested as working rather than as covering its own motive.
printf 'package shardpilot\n\n// See %s for the freeze this follows.\nfunc zzLaneBProbe() {}\n' "$marker" > "$NEW_FILE"
git add -A >/dev/null 2>&1 || true
must_write_baseline "a file absent from the target is caught"
expect_ref "a file absent from the target is caught" "$WITH_BASE" 1 "is absent from"
restore

# ⚠ REMOVED, NOT RENUMBERED. A real format-1 baseline predates the marker, so it
# has NO `# format-version:` line at all -- and a synthetic target that merely
# changes the value keeps the grep succeeding, leaving the status-1 allowance in
# the target-version read uncovered. That is the fourth time this harness has
# covered the branch it meant to and missed the one next to it.
old_commit="$(synth_target '/^# format-version:/d')"
expect_ref "an older-format target is skipped, not misparsed" "$old_commit" 0 "target baseline is format 1"

# ⚠ A COUNT THAT MUST BE REACHED, same rule the gate applies to itself: a run
# that asserts nothing prints the same closing line as a run that asserts
# everything. Raise this when you add a control; it refuses when one goes.
# ⚠ THE TWO REFUSALS I CLAIMED WERE UNREACHABLE. I wrote into the gate that
# these need a corrupt object store and so could not be driven. That was wrong,
# and it was wrong in the worst way: recorded as a permanent limit, in code.
# Both can be built from valid plumbing that damages nothing.
#
#   ls-tree fails when a SUBtree is missing -- `mktree --missing` accepts an
#   entry pointing at a tree that does not exist, and commit-tree accepts the
#   result (it validates the ROOT tree, which is why a missing root does not
#   work and a missing subtree does).
#
#   git show fails when the blob is missing while the tree still lists the path
#   -- `write-tree --missing-ok` is documented to allow exactly that.
#
# Neither writes a broken object anywhere reachable, and both are unreferenced
# after this run.
missing_sub="$(printf '040000 tree %s\tscripts\n' 0000000000000000000000000000000000000002 | git mktree --missing)"
no_tree_commit="$(git -c user.name="lane-b-ratchet-test" -c user.email="lane-b-ratchet-test@invalid" \
  commit-tree "$missing_sub" -m "synthetic unreadable tree")"
expect_ref "an unreadable tree on the target refuses" "$no_tree_commit" 2 "could not list"

idx_missing="$(mktemp)"
GIT_INDEX_FILE="$idx_missing" git read-tree HEAD
GIT_INDEX_FILE="$idx_missing" git update-index --add \
  --cacheinfo "100644,0000000000000000000000000000000000000001,$BASELINE"
missing_blob_tree="$(GIT_INDEX_FILE="$idx_missing" git write-tree --missing-ok)"
rm -f "$idx_missing"
no_blob_commit="$(git -c user.name="lane-b-ratchet-test" -c user.email="lane-b-ratchet-test@invalid" \
  commit-tree "$missing_blob_tree" -p HEAD -m "synthetic unreadable blob")"
expect_ref "an unreadable baseline blob on the target refuses" "$no_blob_commit" 2 "could not be read"

# ⚠ THE WRITER'S GUARD, DRIVEN THROUGH THE WRITER. --write-baseline redirected
# into LANE_B_BASELINE and then exited, so every check below that branch was
# unreachable from it -- and the path is env-overridable, so an arbitrary target
# reached the redirect. Reproduced against the pre-fix gate: an unrelated file
# was overwritten with the baseline header and the gate exited 0.
#
# The control drives the WRITING path, not the reading one: a guard proven only
# on the read path says nothing about the branch that motivated it.
OUTSIDE="$(mktemp -d)"
printf 'PRECIOUS UNRELATED FILE\n' > "$OUTSIDE/precious.txt"
expect_env "an absolute baseline path refuses the writer" \
  "LANE_B_BASELINE=$OUTSIDE/precious.txt" 2 "must be a plain worktree-relative path" --write-baseline
if [ "$(head -1 "$OUTSIDE/precious.txt")" != "PRECIOUS UNRELATED FILE" ]; then
  echo "FAIL [an absolute baseline path refuses the writer]: the file was written anyway" >&2
  failures=$((failures + 1))
fi

# ⚠ THE FOURTH DOOR, and the reason the check above is an identity test rather
# than a list. The first three guards here -- path shape, a symlink at the final
# component, the link count -- all pass a REGULAR file sitting under a SYMLINKED
# PARENT, and the redirect then lands outside the repository exactly as before.
# Reproduced by review: `LANE_B_BASELINE=lane-b-parent-link/out` with the link
# pointing at a temporary directory, --write-baseline exit 0, external file
# overwritten. Resolving the parent with `pwd -P` is what closes it, and this
# control is what proves the closing is real rather than asserted.
mkdir -p "$OUTSIDE/escape"
printf 'PRECIOUS BEHIND A LINKED PARENT\n' > "$OUTSIDE/out"
ln -s "$OUTSIDE" lane-b-parent-link
expect_env "a symlinked parent directory refuses the writer" \
  "LANE_B_BASELINE=lane-b-parent-link/out" 2 "resolves outside this repository" --write-baseline
if [ "$(head -1 "$OUTSIDE/out")" != "PRECIOUS BEHIND A LINKED PARENT" ]; then
  echo "FAIL [a symlinked parent directory refuses the writer]: the file was written anyway" >&2
  failures=$((failures + 1))
fi

# ⚠ THE LOGICAL-cd SCENE, DRIVEN AS ITS OWN CONTROL. `cd` without -P collapses
# `link/..` textually and lands somewhere the kernel would not, so validation
# and the redirect can disagree about which directory the path names. Measured
# on this exact shape: the guard resolved to repo/escape and passed, while the
# write went to the external escape/ and repo/escape/out was never created.
#
# WHICH refusal fires here is worth being precise about, because it is not the
# containment one. The `..` spelling is turned away earlier -- git cannot
# consume `:a/../b` as an object name, so it is refused for that reason before
# the directory is ever resolved. That earlier guard is what closes this route
# today; `cd -P` stays because it is the correct primitive for the step it
# performs, and because two guards protecting one route must not depend on each
# other's ordering to be correct. This control asserts the OUTCOME the scene is
# about -- the external file untouched -- rather than which guard got there.
mkdir -p escape
printf 'PRECIOUS BEHIND A CLIMB\n' > "$OUTSIDE/escape/out"
ln -s "$OUTSIDE" lane-b-parent-link
expect_env "a path climbing out through a symlink refuses the writer" \
  "LANE_B_BASELINE=lane-b-parent-link/../escape/out" 2 "must be a plain worktree-relative path" --write-baseline
if [ "$(head -1 "$OUTSIDE/escape/out")" != "PRECIOUS BEHIND A CLIMB" ]; then
  echo "FAIL [a path climbing out through a symlink refuses the writer]: the file was written anyway" >&2
  failures=$((failures + 1))
fi
rm -f lane-b-parent-link; rmdir escape 2>/dev/null || true

# ⚠ THE GUARD'S ENUMERATION, CHECKED AGAINST GIT ITSELF. The spelling refusal
# lists what git cannot consume as `:<path>`. A list written from examples is a
# sample, not an enumeration -- measured, the first version missed `a//b` and
# `a/./b`, both of which git rejects and it let through, so --write-baseline
# would succeed and the next run would report the file it had just written as
# missing. This asserts the two agree, spelling by spelling, so a disagreement
# arrives as a failing control instead of as a hole.
checks=$((checks + 1))
spelling_bad=0
for spelling in \
    "$BASELINE" \
    "./$BASELINE" \
    "scripts//public-surface-lane-b-baseline.txt" \
    "scripts/./public-surface-lane-b-baseline.txt" \
    "$PWD/$BASELINE" \
    "scripts/../$BASELINE"; do
  if git cat-file blob ":$spelling" >/dev/null 2>&1; then git_reads=yes; else git_reads=no; fi
  case "$spelling" in
    /*|*/../*|../*|*/..|..|*//*|*/./*) guard_refuses=yes ;;
    *) guard_refuses=no ;;
  esac
  # git can read it  <=>  the guard lets it through
  if [ "$git_reads" = "$guard_refuses" ]; then
    echo "FAIL [the spelling guard matches what git can read]: '$spelling'" >&2
    echo "  git reads it: $git_reads, guard refuses it: $guard_refuses" >&2
    spelling_bad=1
  fi
done
[ "$spelling_bad" -eq 0 ] || failures=$((failures + 1))
rm -f lane-b-parent-link
rm -rf "$OUTSIDE"

# ⚠ A BROKEN COUNTING INSTRUMENT, DRIVEN RATHER THAN DECLARED UNREACHABLE. The
# occurrence passes must not flatten grep's 2 into 1: an I/O error on a file
# that HAS matches would otherwise yield an empty result, a count of zero, and a
# ratchet comparing against a number it never computed -- which reads exactly
# like paid-off debt.
#
# Earlier in this file's history two refusals were written off as impossible to
# drive without corrupting the repository. That was false, and the note saying
# so shipped in the gate where the next person would have believed it. So this
# one gets a route instead of a note: a stub `grep` earlier in PATH that fails
# ONLY for the -o invocations the occurrence counter makes, and defers to the
# real one for every other pass -- so the refusal that fires is this one and not
# some earlier reader's.
STUB="$(mktemp -d)"
REAL_GREP="$(command -v grep)"
{
  echo '#!/bin/sh'
  echo 'case " $* " in *" -aonE "*|*" -aoniE "*) exit 2 ;; esac'
  echo "exec $REAL_GREP \"\$@\""
} > "$STUB/grep"
chmod +x "$STUB/grep"
expect_env "an unreadable occurrence pass refuses" \
  "PATH=$STUB:$PATH" 2 "could not count occurrences"
rm -rf "$STUB"

# A hard link is a second name for one inode and a redirect writes THROUGH it.
ln "$BASELINE" "$BASELINE.hard"
expect "a hard-linked baseline refuses" 2 "hard links"
rm -f "$BASELINE.hard"
restore

# ⚠ NOT STAGED, DELIBERATELY. Staging a symlink puts mode 120000 in the index,
# so the INDEX check fires and this one is never reached. The two refusals exist
# because the index and the working tree can disagree, and a control that stages
# first can only ever drive one of them.
mv "$BASELINE" "$BASELINE.real"
ln -s "$(basename "$BASELINE.real")" "$BASELINE"
expect_nostage "a working-tree symlink refuses" 2 "is a symlink in the working tree"
rm -f "$BASELINE"; mv "$BASELINE.real" "$BASELINE"

# ⚠ A SECOND IDENTIFIER ON AN ALREADY-MATCHING LINE. The records the scan keeps
# are deduplicated by line number, so the old per-file tally ticked once per
# LINE: appending another identifier to a line already carrying one moved no
# number and the ratchet passed with no baseline edit. That is the gate's stated
# rule -- a new occurrence fails -- disagreeing with the unit it counted.
# Two steps, because config.go carries no marker of its own: its lane B count
# comes from other patterns and there is no way to name one of those lines from
# here. So the control MAKES a counted line, records it in the baseline, and
# only then adds the second identifier to that same line -- which is the state
# the defect was about, reached honestly rather than assumed.
printf '\n// See %s for the freeze this follows.\n' "$marker" >> config.go
git add -A >/dev/null 2>&1 || true
must_write_baseline "a second match on an existing line is counted"
awk -v m="$marker" '{ lines[NR] = $0; if (index($0, m) > 0) last = NR }
                    END { for (i = 1; i <= NR; i++)
                            print (i == last ? lines[i] " " m : lines[i]) }' \
  config.go > config.go.tmp
mv config.go.tmp config.go
expect "a second match on an existing line is counted" 1 "moved and the baseline did not agree"
restore

# ⚠ THE BASELINE IS READ FROM THE INDEX, LIKE EVERY OTHER INPUT. scan_tree lists
# paths from `git write-tree` and reads blobs with `git cat-file blob :<path>`;
# the baseline was the one input still read from the working tree, so a staged
# source change plus an UNSTAGED baseline edit compared a staged tree against an
# unstaged baseline and passed -- while the commit being built carried the stale
# one. Not staged here, deliberately: staging is what erases the difference this
# control exists to see.
printf '\n// See %s for the freeze this follows.\n' "$marker" >> config.go
git add config.go >/dev/null 2>&1 || true
must_write_baseline "an unstaged baseline is not the one that would be committed"
expect_nostage "an unstaged baseline is not the one that would be committed" \
  1 "moved and the baseline did not agree"
restore

# ⚠ EXACTLY, NOT AT LEAST, AND THE NUMBER LIVES ONLY ON THE NEXT LINE. A floor
# accepts a stale count: add a control without touching it and the expected
# number silently drifts, after which that control can be deleted and the stale
# number still passes -- the silently-lost-check failure this harness exists to
# detect, reproduced inside the detector. Equality forces the number to move when
# a control is added, so a later removal cannot hide behind it.
#
# The number appears exactly ONCE, in EXPECTED_CHECKS, and the message READS it
# instead of repeating it. The revision before this one spelled "expected
# exactly 14" as a second literal directly beneath this paragraph -- the drift
# this paragraph forbids, one line from where it forbids it, and passing every
# healthy run because a stale expectation only shows up once some other count
# disagrees.
EXPECTED_CHECKS=23
if [ "$checks" -ne "$EXPECTED_CHECKS" ]; then
  echo "REFUSING: $checks control(s) ran, expected exactly $EXPECTED_CHECKS" >&2
  exit 2
fi
if [ "$failures" -ne 0 ]; then
  echo "lane B ratchet: $failures of $checks control(s) FAILED" >&2
  exit 1
fi
echo "lane B ratchet: $checks control(s), 0 failure(s)"
