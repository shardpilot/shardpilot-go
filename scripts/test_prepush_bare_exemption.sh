#!/usr/bin/env bash
#
# The pre-push hook grants ONE exemption from its checkout-owner guard: a bare
# repository has no worktree, so there is no unresolved checkout whose files
# could be on PATH. The hook decides that by PARSING `config` with shell builtins,
# because it runs before the PATH filter that would make an answer from `git`
# trustworthy.
#
# That parser had been corrected twice by the shape of the case that arrived --
# folding the names, then matching them whole rather than by prefix -- and a third
# arrived: an `[include]` overriding `core.bare` to false, which git honours and
# the parser read straight past (shardpilot/shardpilot-go#79 review).
#
# So the question this script asks is not "does it handle include". It is the only
# question that matters about an exemption: IS THE PARSER EVER MORE PERMISSIVE
# THAN GIT? Each case below is a real repository handed to `git rev-parse
# --is-bare-repository`, and git's answer is the authority. A parser that says
# bare where git says otherwise is a hole in the guard. A parser that declines
# where git says bare costs a push from a bare repository and nothing else.
#
# ⚠ IT LIFTS THE HOOK'S OWN LINES. A copy of the loop in this file would pass
# while the hook did something else -- the failure this repository has now paid
# for five times. The markers are the coupling, and a slice that does not parse,
# or does not answer, REFUSES here rather than reporting green.
set -uo pipefail

here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
hook="$here/.githooks/pre-push"
[ -r "$hook" ] || { echo "REFUSING: cannot read $hook" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# ---- lift the parse block out of the hook itself ---------------------------
start="$(grep -n '# >>> BARE-EXEMPTION PARSE' "$hook" | head -1 | cut -d: -f1)"
end="$(grep -n '# <<< BARE-EXEMPTION PARSE' "$hook" | head -1 | cut -d: -f1)"
if [ -z "$start" ] || [ -z "$end" ] || [ "$end" -le "$start" ]; then
  echo "REFUSING: the BARE-EXEMPTION PARSE markers are missing or out of order in" >&2
  echo "  $hook -- this script would otherwise test nothing and report green." >&2
  exit 2
fi

probe="$work/probe.sh"
{
  echo '#!/usr/bin/env bash'
  echo "NL='"
  echo "'"
  echo 'TAB="$(printf "\tX")"; TAB="${TAB%X}"'
  echo 'CR="$(printf "\rX")"; CR="${CR%X}"'
  echo 'wt_common="$1"'
  sed -n "$((start + 1)),$((end - 1))p" "$hook" | sed 's/^    //'
  echo 'printf "bare=%s modelled=%s\n" "$wt_is_bare" "$wt_cfg_modelled"'
} > "$probe"
chmod +x "$probe"
if ! bash -n "$probe" 2>/dev/null; then
  echo "REFUSING: the lifted parse block does not parse on its own. The markers" >&2
  echo "  no longer bound a self-contained block." >&2
  exit 2
fi

# ---- the cases -------------------------------------------------------------
cases="$work/cases"; mkdir -p "$cases"
inc="$work/override.cfg"; printf '[core]\n\tbare = false\n' > "$inc"

mk() { # mk <name> <config text>
  local d="$cases/$1.git"
  mkdir -p "$d/objects" "$d/refs"
  printf 'ref: refs/heads/main\n' > "$d/HEAD"
  printf '%s' "$2" > "$d/config"
}

# Ordinary bare repositories. These must KEEP the exemption, or "never more
# permissive than git" has been achieved by refusing everything.
mk plain_bare      '[core]
	repositoryformatversion = 0
	bare = true
'
mk folded_section  '[Core]
	bare = true
'
mk comments        '# a comment
; another
[core]
	bare = true
'
mk with_remote     '[core]
	bare = true
[remote "origin"]
	url = https://e.example/r.git
'
mk core_subsection '[core]
	bare = true
[core "sub"]
	bare = false
'
mk crlf            "$(printf '[core]\r\n\tbare = true\r\n')"
expect_exempt="plain_bare folded_section comments with_remote core_subsection crlf"

# Not bare, or not knowable. None of these may take the exemption.
mk plain_nonbare   '[core]
	bare = false
'
mk decoy_name      '[core]
	bare = false
	bareRepository = true
'
mk include_flip    "[core]
	bare = true
[include]
	path = $inc
"
mk includeif_flip  "[core]
	bare = true
[includeIf \"gitdir:**\"]
	path = $inc
"
mk sameline_flip   '[core]
	bare = true
[core] bare = false
'
mk stray_line      '[core]
	bare = true
this line is not config
'
# ⚠ THE CONTINUATION CASE IS BUILT SO THE OLD LOOP GETS IT WRONG IN THE DANGEROUS
# DIRECTION. Written the obvious way -- a `bare = fal\` + `se` split -- the old
# parser lands on the right answer by accident, and a case a mutant cannot fail is
# not a case. Here git joins the two lines into ONE value for `x`, so `core.bare`
# stays `false` and the repository is NOT bare; a parser that reads the second line
# as its own setting sees `bare = true` and would take the exemption.
mk continuation    '[core]
	bare = false
	x = a\
	bare = true
'

# ---- judge -----------------------------------------------------------------
failures=0
answered=0
printf '%-18s %-8s %-26s %s\n' CASE GIT PARSER VERDICT
for d in "$cases"/*.git; do
  name="$(basename "$d" .git)"
  git_says="$(git -C "$d" rev-parse --is-bare-repository 2>/dev/null || echo '<git refused>')"
  out="$("$probe" "$d")"
  bare="${out#bare=}"; bare="${bare%% *}"
  modelled="${out##*modelled=}"
  case "$bare$modelled" in
    yesyes|yesno|noyes|nono) answered=$((answered + 1)) ;;
    *) echo "REFUSING: the probe answered neither yes nor no for $name: '$out'" >&2; exit 2 ;;
  esac

  exempt=no
  [ "$bare" = yes ] && [ "$modelled" = yes ] && exempt=yes

  verdict='ok'
  if [ "$exempt" = yes ] && [ "$git_says" != true ]; then
    verdict='FAIL: exemption taken where git says NOT bare'
    failures=$((failures + 1))
  fi
  case " $expect_exempt " in
    *" $name "*)
      if [ "$exempt" != yes ]; then
        verdict='FAIL: an ordinary bare repository lost the exemption'
        failures=$((failures + 1))
      fi
      ;;
  esac
  printf '%-18s %-8s %-26s %s\n' "$name" "$git_says" "$out" "$verdict"
done

# The instrument must have been able to say something about every case.
total="$(find "$cases" -maxdepth 1 -name '*.git' | wc -l | tr -d ' ')"
if [ "$answered" -ne "$total" ]; then
  echo "REFUSING: the probe answered $answered of $total cases." >&2
  exit 2
fi

# ---- the GIT_DIR normal form ------------------------------------------------
#
# The ownership tests further down the hook are LEXICAL, so what they read has to
# be a normal form. The loop that produces it removed only trailing slashes, and
# `git --git-dir=.git/. push` -- an ordinary invocation -- hands the hook the
# literal `.git/.`, which then failed the `*/.git` test and refused every push
# (shardpilot/shardpilot-go#79 review).
#
# Lifted from the hook for the same reason as the parse above: a copy passes while
# the site does something else.
nstart="$(grep -n '# >>> GIT_DIR NORMAL FORM' "$hook" | head -1 | cut -d: -f1)"
nend="$(grep -n '# <<< GIT_DIR NORMAL FORM' "$hook" | head -1 | cut -d: -f1)"
if [ -z "$nstart" ] || [ -z "$nend" ] || [ "$nend" -le "$nstart" ]; then
  echo "REFUSING: the GIT_DIR NORMAL FORM markers are missing or out of order." >&2
  exit 2
fi
norm="$work/norm.sh"
{
  echo '#!/usr/bin/env bash'
  echo 'inherited_git_dir="$1"'
  sed -n "$((nstart + 1)),$((nend - 1))p" "$hook" | sed 's/^  //'
  echo 'printf "%s\n" "$inherited_git_dir"'
} > "$norm"
chmod +x "$norm"
bash -n "$norm" 2>/dev/null || { echo "REFUSING: the lifted normal-form block does not parse." >&2; exit 2; }

# `want` is what the LEXICAL tests downstream need to see. `..` is deliberately NOT
# collapsed: it is not a no-op under symlinks, and it already matches `*/.git`.
nfail=0
ntotal=0
printf '\n%-24s %-24s %s\n' 'GIT_DIR AS GIVEN' 'NORMALISED' VERDICT
while IFS='|' read -r given want; do
  [ -n "$given" ] || continue
  ntotal=$((ntotal + 1))
  got="$("$norm" "$given")"
  v=ok
  if [ "$got" != "$want" ]; then v="FAIL: wanted [$want]"; nfail=$((nfail + 1)); fi
  printf '%-24s %-24s %s\n' "[$given]" "[$got]" "$v"
done <<'SPELLINGS'
/r/.git|/r/.git
/r/.git/|/r/.git
/r/.git/.|/r/.git
/r/.git/./|/r/.git
/r/.git/././|/r/.git
/r/./.git|/r/.git
/r/./././.git|/r/.git
./.git|.git
/|/
/.|/
/r/../.git|/r/../.git
SPELLINGS
if [ "$ntotal" -eq 0 ]; then
  echo "REFUSING: no normal-form spelling was judged." >&2
  exit 2
fi

# ---- every gitfile is read whole -------------------------------------------
#
# A `.git` FILE holds `gitdir: <path>`, and a pathname may contain a newline, so a
# line-oriented `read -r` keeps only its prefix: the gate then resolves the wrong
# common directory from the very file that names it, and an owned checkout reads as
# ownerless (shardpilot/shardpilot-go#79 review).
#
# ⚠ THIS IS A CLASS ASSERTION, NOT AN INSTANCE ONE, BECAUSE THE DEFECT WAS A CLASS.
# Two sites already carried the whole-file read and three did not; the review named
# ONE of the three. Checking that one would have left the other two, and would not
# stop a fourth being added. So the question asked is "does any read of a gitfile
# still stop at a newline", which has no favourite instance.
gitfile_reads="$(grep -n 'read -r[^<]*< *"[^"]*/\.git"' "$hook" || true)"
gf_bad=0
gf_total=0
while IFS= read -r gf_line; do
  [ -n "$gf_line" ] || continue
  gf_total=$((gf_total + 1))
  case "$gf_line" in
    *'read -r -d ""'*) ;;
    *) echo "FAIL: a gitfile is read line-oriented, so a newline in its path truncates it:" >&2
       echo "  $gf_line" >&2
       gf_bad=$((gf_bad + 1)) ;;
  esac
done <<GITFILEREADS
$gitfile_reads
GITFILEREADS
if [ "$gf_total" -eq 0 ]; then
  echo "REFUSING: no gitfile read was found at all, so this check measured nothing." >&2
  echo "  The hook reads a .git file in at least three places; the pattern here has" >&2
  echo "  stopped matching them." >&2
  exit 2
fi

# ---- a checkout at the filesystem root ---------------------------------------
#
# `inside_a_checkout` decides which PATH entries are checkout-controlled, and it
# runs BEFORE the PATH filter -- so what it misses is executed, not merely allowed.
# With the invoking checkout at `/`, the prefix `"$checkout_root"/` is `//` and
# matched no absolute path at all (shardpilot/shardpilot-go#79 review).
istart="$(grep -n '# >>> INSIDE-A-CHECKOUT' "$hook" | head -1 | cut -d: -f1)"
iend="$(grep -n '# <<< INSIDE-A-CHECKOUT' "$hook" | head -1 | cut -d: -f1)"
if [ -z "$istart" ] || [ -z "$iend" ] || [ "$iend" -le "$istart" ]; then
  echo "REFUSING: the INSIDE-A-CHECKOUT markers are missing or out of order." >&2
  exit 2
fi
inside="$work/inside.sh"
{
  echo '#!/usr/bin/env bash'
  echo 'checkout_root="$1"'
  echo 'shift'
  echo 'other_roots=()'
  echo 'for r in ${OTHER_ROOTS:-}; do other_roots+=("$r"); done'
  sed -n "$((istart + 1)),$((iend - 1))p" "$hook"
  echo 'if inside_a_checkout "$1"; then echo inside; else echo outside; fi'
} > "$inside"
chmod +x "$inside"
bash -n "$inside" 2>/dev/null || { echo "REFUSING: the lifted inside_a_checkout does not parse." >&2; exit 2; }

ifail=0
itotal=0
printf '\n%-14s %-22s %s\n' 'CHECKOUT ROOT' 'CANDIDATE' VERDICT
while IFS='|' read -r root cand want; do
  [ -n "$root" ] || continue
  itotal=$((itotal + 1))
  got="$("$inside" "$root" "$cand")"
  v=ok
  if [ "$got" != "$want" ]; then v="FAIL: wanted $want, got $got"; ifail=$((ifail + 1)); fi
  printf '%-14s %-22s %s\n' "$root" "$cand" "$v"
done <<'ROOTS'
/|/workspace/bin|inside
/|/|inside
/r|/r/bin|inside
/r|/other/bin|outside
/r|/r|inside
/r|/rr/bin|outside
ROOTS
# The same question for a LINKED worktree root, which has the identical shape and
# which the review did not name.
if [ "$(OTHER_ROOTS=/ "$inside" /r /workspace/bin)" != inside ]; then
  echo "FAIL: an other_roots entry at / did not match an absolute path either" >&2
  ifail=$((ifail + 1))
fi
itotal=$((itotal + 1))
if [ "$itotal" -eq 0 ]; then
  echo "REFUSING: no checkout-root case was judged." >&2
  exit 2
fi

# ---- nothing from the pushed branch runs ------------------------------------
#
# The hook redirects `core.hooksPath`, which covers repository HOOKS and nothing
# else. `core.fsmonitor` is documented as a hook command and `git diff --quiet HEAD
# --` invokes it, so a tracked program ran before the trusted scanner
# (shardpilot/shardpilot-go#79 review). Measured on the same command with
# `core.fsmonitor` already neutralised, a `diff.<name>.textconv` and a
# `filter.<name>.clean` fired too -- the review named one of three.
#
# ⚠ DRIVEN THROUGH A REAL `git push`, not by calling the hook. The question is what
# GIT does when it runs this file, and a rig that invokes the hook directly answers
# a different one. The positive control below is the point of the section: it runs
# the hook AS COMMITTED AT ITS PARENT and requires the programs to fire, because a
# section where the attack never lands would pass whatever the hook did.
run_push_probe() { # run_push_probe <hook file> ; echoes what executed
  rm -f "$fired"
  cp "$1" "$probe_repo/.git/hooks/pre-push"
  chmod +x "$probe_repo/.git/hooks/pre-push"
  ( cd "$probe_repo" && git push -q origin HEAD:refs/heads/probe >/dev/null 2>&1 ) || true
  ( cd "$probe_repo" && git -C "$probe_remote" update-ref -d refs/heads/probe >/dev/null 2>&1 ) || true
  sort -u "$fired" 2>/dev/null | tr '\n' ' '
}

probe_remote="$work/remote.git"
probe_repo="$work/repo"
fired="$work/fired.txt"
git init -q --bare "$probe_remote"
git init -q "$probe_repo"
for prog in prog clean; do
  printf '#!/bin/sh\necho "%s" >> "%s"\ncat\n' "$prog" "$fired" > "$probe_repo/$prog"
  chmod +x "$probe_repo/$prog"
done
printf '#!/bin/sh\necho fsmonitor >> "%s"\nprintf "/\\0"\n' "$fired" > "$probe_repo/fsmon"
chmod +x "$probe_repo/fsmon"
printf '* diff=dx filter=fx\n' > "$probe_repo/.gitattributes"
printf 'x\n' > "$probe_repo/f"
mkdir -p "$probe_repo/.git/hooks"
printf '#!/bin/sh\nexit 0\n' > "$probe_repo/.git/hooks/check_public_surface.sh"
chmod +x "$probe_repo/.git/hooks/check_public_surface.sh"
( cd "$probe_repo"
  git add -A >/dev/null 2>&1
  git -c user.email=t@t -c user.name=t commit -q -m x >/dev/null 2>&1
  git remote add origin "$probe_remote" >/dev/null 2>&1
  git config core.fsmonitor ./fsmon
  git config diff.dx.textconv ./prog
  git config filter.fx.clean ./clean ) || true

exec_fail=0
# ⚠ THE POSITIVE CONTROL IS A FIXTURE, NOT A REVISION. It read
# `git show HEAD~1:.githooks/pre-push` -- which is the ALREADY FIXED hook on every
# descendant commit, so the control would find no marker and this section would
# refuse on ordinary commits (shardpilot/shardpilot-go#79 review).
#
# ⚠ AND IN CI IT WAS WORSE: `actions/checkout` clones at depth 1, so `HEAD~1` does
# not exist, the `else` branch fired, and the control was SKIPPED -- the whole
# execution section ran green with no evidence, in 5 seconds, for as long as it has
# existed. A check that skips is a silent failure that looks like success.
#
# So the vulnerable behaviour is BUILT here. It is two lines: consult the working
# tree the way the real hook must, with nothing neutralised. Stable under any
# history, any clone depth, and any later commit -- verified in a depth-1 clone.
vuln_hook="$work/vulnerable-pre-push"
cat > "$vuln_hook" <<'VULNEOF'
#!/bin/bash
# A stand-in for this hook WITHOUT its config neutralisation: it consults the
# working tree, which is what makes git run the configured drivers. Its only job is
# to prove the attack lands.
git diff --quiet HEAD -- >/dev/null 2>&1
exit 0
VULNEOF
chmod +x "$vuln_hook"
before="$(run_push_probe "$vuln_hook")"
case "$before" in
  *fsmonitor*) printf '\npositive control: the unneutralised stand-in let [%s] run, as expected.\n' "$before" ;;
  *) echo "REFUSING: the attack did not land on the unneutralised stand-in ([$before])," >&2
     echo "  so this section cannot tell a fix from a rig that never reproduced the" >&2
     echo "  defect. The fixture, not the hook, is what has stopped working." >&2
     exit 2 ;;
esac

after="$(run_push_probe "$hook")"
if [ -n "$after" ]; then
  echo "FAIL: the pushed branch's own programs ran under this hook: [$after]" >&2
  exec_fail=1
else
  printf 'this hook: nothing from the pushed branch ran.\n'
fi

# ⚠ THE LONG-RUNNING FILTER FORM. `filter.<name>.process` is standard and fired
# straight through a rule that read `clean|smudge`, with `core.fsmonitor` already
# neutralised (shardpilot/shardpilot-go#79 review).
( cd "$probe_repo" && git config filter.fx.process ./prog ) || true
after_proc="$(run_push_probe "$hook")"
if [ -n "$after_proc" ]; then
  echo "FAIL: a long-running filter process ran under this hook: [$after_proc]" >&2
  exec_fail=1
else
  printf 'this hook: filter.<driver>.process did not run either.\n'
fi

# ⚠ AND THE ENVIRONMENT OUTRANKS THE FILE. `git -c k=v push` reaches a hook as
# GIT_CONFIG_PARAMETERS -- measured on git 2.50.1, where GIT_CONFIG_COUNT is not set
# at all -- and it beats the GIT_CONFIG_COUNT overrides the hook installs. So the
# neutralisation was bypassable by the very flag the sibling finding was about, and
# no review named this one.
run_push_probe_c() {
  rm -f "$fired"
  cp "$1" "$probe_repo/.git/hooks/pre-push"
  chmod +x "$probe_repo/.git/hooks/pre-push"
  ( cd "$probe_repo" && git -c core.fsmonitor=./fsmon push -q origin HEAD:refs/heads/probe >/dev/null 2>&1 ) || true
  ( cd "$probe_repo" && git -C "$probe_remote" update-ref -d refs/heads/probe >/dev/null 2>&1 ) || true
  sort -u "$fired" 2>/dev/null | tr '\n' ' '
}
if [ -s "$vuln_hook" ]; then
  before_c="$(run_push_probe_c "$vuln_hook")"
  case "$before_c" in
    *fsmonitor*) printf 'positive control: `git -c` let [%s] run on the stand-in.\n' "$before_c" ;;
    *) echo "REFUSING: the -c bypass did not land on the stand-in ([$before_c])," >&2
       echo "  so this check cannot tell a fix from a rig that never reproduced it." >&2
       exit 2 ;;
  esac
fi
# ⚠ THE GLOB CASE IS ASSERTED STRUCTURALLY, AND THE REASON IS WORTH THE LINE. A
# driver name may contain a pattern character -- `filter.x*.clean` is a legal key --
# and the enumeration that neutralises the drivers splits an unquoted list in a
# directory the pushed branch owns, so a TRACKED file named `filter.xZ.clean` makes
# the loop export an override for the FILE's name and leave the real key active
# (shardpilot/shardpilot-go#79 review).
#
# It was MEASURED directly -- without `set -f` the loop iterates `filter.xZ.clean`,
# with it `filter.x*.clean` -- and an end-to-end version of that through a real push
# would not reproduce here. A case that cannot go positive is worse than none, so
# what is asserted is the property that fixes it: the enumeration is bracketed by
# `set -f`. If the brackets move, this refuses rather than reporting green.
enum_start="$(grep -n 'for sp_k in core.fsmonitor' "$hook" | head -1 | cut -d: -f1)"
if [ -z "$enum_start" ]; then
  echo "REFUSING: the config-key enumeration was not found, so its globbing could" >&2
  echo "  not be checked. The pattern here has stopped matching it." >&2
  exit 2
fi
glob_off=no
i=$((enum_start - 1))
while [ "$i" -gt 0 ] && [ "$i" -gt $((enum_start - 25)) ]; do
  case "$(sed -n "${i}p" "$hook")" in
    "set -f") glob_off=yes; break ;;
    "set +f") break ;;
  esac
  i=$((i - 1))
done
if [ "$glob_off" != yes ]; then
  echo "FAIL: the config-key enumeration splits an unquoted list with pathname" >&2
  echo "  generation live, in a directory the pushed branch owns." >&2
  exec_fail=1
else
  printf 'this hook: the key enumeration runs with globbing off.\n'
fi

after_c="$(run_push_probe_c "$hook")"
if [ -n "$after_c" ]; then
  echo "FAIL: 'git -c core.fsmonitor=...' re-enabled a branch program: [$after_c]" >&2
  exec_fail=1
else
  printf 'this hook: a git -c override did not re-enable it.\n'
fi

# ---- the developer's index survives the scan --------------------------------
#
# ⚠ THE ONLY DEFECT IN THIS GATE WHOSE DAMAGE LANDS OUTSIDE THE PUSH IT REFUSES.
# The hook clears every INHERITED repository selector and then resolves and
# re-exports `GIT_DIR` for the repository being pushed -- correctly, because the
# outer commands need it. But `cd "$wt"` changes the directory and nothing else, so
# `read-tree` wrote the INVOKING repository's index and the `update-index` calls and
# scanner fixtures after it kept targeting that repository, replacing staged work
# with a historical tree plus synthetic evidence (shardpilot/shardpilot-go#79
# review).
#
# Asserted on the artefact a developer would lose: a distinctive staged path, still
# staged and alone, after a real push.
idx_repo="$work/idx"
git init -q "$idx_repo"
printf 'a\n' > "$idx_repo/f"
mkdir -p "$idx_repo/.git/hooks" "$idx_repo/scripts"
printf '#!/bin/sh\nexit 0\n' > "$idx_repo/.git/hooks/check_public_surface.sh"
chmod +x "$idx_repo/.git/hooks/check_public_surface.sh"
mkdir -p "$idx_repo/scripts"
printf '#!/bin/sh\nexit 0\n' > "$idx_repo/scripts/check_public_surface.sh"
chmod +x "$idx_repo/scripts/check_public_surface.sh"
( cd "$idx_repo"
  git add -A >/dev/null 2>&1
  git -c user.email=t@t -c user.name=t commit -q -m c1 >/dev/null 2>&1
  printf 'b\n' > f
  git add -A >/dev/null 2>&1
  git -c user.email=t@t -c user.name=t commit -q -m c2 >/dev/null 2>&1
  git remote add origin "$probe_remote" >/dev/null 2>&1 ) || true
idx_probe() { # idx_probe <hook file> ; echoes the invoking repo's index afterwards
  cp "$1" "$idx_repo/.git/hooks/pre-push"; chmod +x "$idx_repo/.git/hooks/pre-push"
  # ⚠ WITH AN EXPLICIT `--git-dir`, which is the condition. An ordinary `git push`
  # passes no GIT_DIR to its hook, so the exported selector this is about does not
  # exist and the damage cannot land -- I ran three configurations that reported
  # clean before finding the one that reaches it.
  ( cd "$idx_repo" && git --git-dir="$idx_repo/.git" --work-tree="$idx_repo" \
      push -q origin HEAD:refs/heads/idxprobe >/dev/null 2>&1 ) || true
  git -C "$probe_remote" update-ref -d refs/heads/idxprobe >/dev/null 2>&1 || true
  ( cd "$idx_repo" && git ls-files | sort | tr '\n' ' ' )
}
idx_fail=0
# ⚠ THE CONTROL TESTS THE CHECK, NOT A MUTANT HOOK. It used to build a vulnerable
# copy by reverting `in_worktree` -- and against the current tree that copy fails
# EARLIER, at the private object store, so the fixture stopped reaching the damage
# and this section refused. It refusing was correct; a fixture that has stopped
# reproducing must not read as a pass. But the section still has to run, so the
# control moved to the thing that can be kept true: the detector must report a
# polluted index when it is shown one.
idx_detect() { # idx_detect <index listing> ; echoes "polluted" or "clean"
  case "$1" in *as-committed.txt*) echo polluted ;; *) echo clean ;; esac
}
if [ "$(idx_detect "f scripts/commit-object-as-committed.txt")" != polluted ] ||
   [ "$(idx_detect "f scripts/check_public_surface.sh")" != clean ]; then
  echo "REFUSING: the index detector does not separate a polluted listing from a" >&2
  echo "  clean one, so this section could not report the damage it exists for." >&2
  exit 2
fi
printf '\npositive control: the index detector reports a polluted listing.\n'
idx_after="$(idx_probe "$hook")"
case "$(idx_detect "$idx_after")" in
  polluted)
    echo "FAIL: the scan staged its own evidence into the invoking repository's" >&2
    echo "  index: [$idx_after]" >&2
    idx_fail=1 ;;
  *) printf 'this hook: the invoking index is untouched [%s].\n' "$idx_after" ;;
esac

# ---- an ordinary push still succeeds ------------------------------------------
#
# ⚠ THE SECTIONS BELOW ASK WHAT EXECUTED, AND NEVER ASKED WHETHER THE PUSH WORKED.
# `run_push_probe` swallows the exit status, so "nothing from the branch ran" was
# reported identically whether the neutralisation worked or the hook DIED at its
# first git call. It died: a leading separator in `GIT_CONFIG_PARAMETERS` made every
# later git fail with "bogus format", and this gate refused every ordinary push for
# two commits while these arms stayed green (shardpilot/shardpilot-go#79 review).
#
# So the first thing asserted is the thing a hook is for: a clean repository, with
# nothing configured to run, must PUSH. Without this the arms below cannot tell a
# working neutralisation from a broken hook.
ord_fail=0
ord_repo="$work/ord"; git init -q "$ord_repo"
mkdir -p "$ord_repo/scripts" "$ord_repo/.git/hooks"
printf '#!/bin/sh\nexit 0\n' > "$ord_repo/scripts/check_public_surface.sh"
printf '#!/bin/sh\nexit 0\n' > "$ord_repo/.git/hooks/check_public_surface.sh"
chmod +x "$ord_repo/scripts/check_public_surface.sh" "$ord_repo/.git/hooks/check_public_surface.sh"
( cd "$ord_repo"
  printf 'a\n' > f; git add -A >/dev/null 2>&1
  git -c user.email=t@t -c user.name=t commit -q -m c1 >/dev/null 2>&1
  git remote add origin "$probe_remote" >/dev/null 2>&1 ) || true
cp "$hook" "$ord_repo/.git/hooks/pre-push"; chmod +x "$ord_repo/.git/hooks/pre-push"
ord_rc=0
( cd "$ord_repo" && git push -q origin HEAD:refs/heads/ordprobe >"$work/ord.out" 2>&1 ) || ord_rc=$?
git -C "$probe_remote" update-ref -d refs/heads/ordprobe >/dev/null 2>&1 || true
if [ "$ord_rc" -ne 0 ]; then
  echo "FAIL: this hook refuses an ORDINARY push of a clean repository (rc=$ord_rc)." >&2
  sed -n '1,4p' "$work/ord.out" >&2
  ord_fail=1
else
  printf '\nthis hook: an ordinary push of a clean repository succeeds.\n'
fi

# ---- the executable-key enumeration covers the names git allows ---------------
#
# The enumeration derives which config keys to neutralise from `git config --list`,
# and its pattern has now been wrong about the NAME three times: the suffix list,
# then a glob character, then a leading dot. `filter..x.clean` is a legal key --
# `.gitattributes` saying `filter=.x` selects it, and git runs it -- and `[^.]+`
# cannot match an empty first component (shardpilot/shardpilot-go#79 review).
#
# ⚠ ASSERTED ON THE PATTERN, NOT THROUGH A PUSH. I could not reproduce this one end
# to end: the hook refuses a dirty tree before reaching its diff, and on a clean
# tree the clean driver is not invoked. What IS measured is that the pattern omitted
# the key and now includes it, which is the thing that was wrong. Said here rather
# than implied by a green push that proves something else.
keyre="$(grep -o '\^(core\\.fsmonitor[^'"'"']*' "$hook" | head -1)"
if [ -z "$keyre" ]; then
  echo "REFUSING: the executable-key pattern was not found in the hook, so what it" >&2
  echo "  matches could not be checked." >&2
  exit 2
fi
kre_fail=0
must_match='filter..x.clean filter.lfs.clean filter.x*.clean diff.dx.textconv merge.m.driver core.fsmonitor'
must_miss='diff.algorithm diff.colorMoved filter.x core.editor'
for k in $must_match; do
  printf '%s\n' "$k" | grep -Eq "$keyre" || {
    echo "FAIL: the executable-key pattern does not match $k, so that driver is left live" >&2
    kre_fail=1; }
done
for k in $must_miss; do
  if printf '%s\n' "$k" | grep -Eq "$keyre"; then
    echo "FAIL: the executable-key pattern matches $k, which configures no program" >&2
    kre_fail=1
  fi
done
[ "$kre_fail" -eq 0 ] && printf '\nthis hook: the executable-key pattern covers dotted, globbed and ordinary names.\n'

# ---- a config key carrying a quote does not poison every later git -------------
#
# `filter.x'y.clean` is a valid key, and interpolated raw into
# `GIT_CONFIG_PARAMETERS` it produced malformed syntax after which EVERY subsequent
# git exited with "bogus format" (shardpilot/shardpilot-go#79 review). The escaper is
# lifted from the hook and handed to a real git, so this asserts what git accepts
# rather than what the sequence looks like.
esc_fail=0
eval "$(grep -E '^sp_q=|^sp_bs=|^sp_qrep=' "$hook")" 2>/dev/null || true
if [ -z "${sp_qrep:-}" ]; then
  echo "REFUSING: the quote-escaping characters were not found in the hook, so the" >&2
  echo "  escaping could not be checked." >&2
  exit 2
fi
esc_repo="$work/esc"; git init -q "$esc_repo"
( cd "$esc_repo"; printf 'a\n' > f; git add -A >/dev/null 2>&1
  git -c user.email=t@t -c user.name=t commit -q -m x >/dev/null 2>&1 ) || true
esc_k="filter.x${sp_q}y.clean"
# The control: the UNESCAPED form must be rejected, or this proves nothing.
gcp_raw="$sp_q$esc_k$sp_q=$sp_q$sp_q"
if ( cd "$esc_repo" && env GIT_CONFIG_PARAMETERS="$gcp_raw" git rev-parse HEAD >/dev/null 2>&1 ); then
  echo "REFUSING: git accepted the unescaped key, so this check cannot tell escaping" >&2
  echo "  from the absence of it." >&2
  exit 2
fi
esc_e="${esc_k//$sp_q/$sp_qrep}"
gcp_esc="$sp_q$esc_e$sp_q=$sp_q$sp_q"
if ( cd "$esc_repo" && env GIT_CONFIG_PARAMETERS="$gcp_esc" git rev-parse HEAD >/dev/null 2>&1 ); then
  printf '\npositive control: git rejects the raw key and accepts the escaped one.\n'
  printf 'this hook: a quote-bearing config key is escaped for GIT_CONFIG_PARAMETERS.\n'
else
  echo "FAIL: the hook's escaping still produces a GIT_CONFIG_PARAMETERS git rejects," >&2
  echo "  which makes every later git in this hook fail." >&2
  esc_fail=1
fi

# ---- the scanner child is neutralised too ------------------------------------
#
# ⚠ `-c` REACHES ONLY THE COMMANDS THIS FILE LAUNCHES. The scanner is a separate
# script running its own bare `git ls-files` and `git write-tree`, and with an
# absolute `core.fsmonitor` those executed the branch's program during the scan --
# the `-c` neutralisation is not inherited (shardpilot/shardpilot-go#79 review).
# The environment half is appended for exactly this crossing.
child_fired="$work/child-fired"
chl_repo="$work/chl"; git init -q "$chl_repo"
mkdir -p "$chl_repo/scripts" "$chl_repo/.git/hooks"
printf '#!/bin/sh\necho FIRED >> "%s"\nprintf "/\\0"\n' "$child_fired" > "$chl_repo/fsmon"
chmod +x "$chl_repo/fsmon"
# A scanner that runs a git command of its own, which is what the real one does.
printf '#!/bin/sh\ngit ls-files >/dev/null 2>&1\nexit 0\n' > "$chl_repo/scripts/check_public_surface.sh"
cp "$chl_repo/scripts/check_public_surface.sh" "$chl_repo/.git/hooks/check_public_surface.sh"
chmod +x "$chl_repo/scripts/check_public_surface.sh" "$chl_repo/.git/hooks/check_public_surface.sh"
( cd "$chl_repo"
  printf 'a\n' > f; git add -A >/dev/null 2>&1
  git -c user.email=t@t -c user.name=t commit -q -m c1 >/dev/null 2>&1
  git remote add origin "$probe_remote" >/dev/null 2>&1
  git config core.fsmonitor "$chl_repo/fsmon" ) || true
chl_probe() {
  rm -f "$child_fired"
  cp "$1" "$chl_repo/.git/hooks/pre-push"; chmod +x "$chl_repo/.git/hooks/pre-push"
  ( cd "$chl_repo" && git push -q origin HEAD:refs/heads/chlprobe >/dev/null 2>&1 ) || true
  git -C "$probe_remote" update-ref -d refs/heads/chlprobe >/dev/null 2>&1 || true
  [ -s "$child_fired" ] && echo fired || echo quiet
}
chl_fail=0
# The control: the unneutralised stand-in must let the child's git run it.
if [ "$(chl_probe "$vuln_hook")" != fired ]; then
  echo "REFUSING: the scanner-child attack did not land on the stand-in, so this" >&2
  echo "  section cannot tell a fix from a rig that never reproduced it." >&2
  exit 2
fi
printf '\npositive control: the stand-in let the scanner child run the program.\n'
if [ "$(chl_probe "$hook")" = fired ]; then
  echo "FAIL: the scanner child ran a branch-controlled program despite the hook's" >&2
  echo "  neutralisation -- the -c form is not inherited by a child script." >&2
  chl_fail=1
else
  printf 'this hook: the scanner child ran nothing from the branch.\n'
fi

# ---- an unreadable back-pointer is refused, not skipped -----------------------
#
# A registered worktree whose `gitdir` file cannot be READ was treated like an empty
# one and silently dropped from the roots this pass filters PATH against
# (shardpilot/shardpilot-go#79 review). `read -d ''` returns non-zero at EOF, so its
# status cannot carry this; readability is asked directly.
bp_fail=0
bp_repo="$work/bp"; git init -q "$bp_repo"
mkdir -p "$bp_repo/scripts" "$bp_repo/.git/hooks"
printf '#!/bin/sh\nexit 0\n' > "$bp_repo/scripts/check_public_surface.sh"
printf '#!/bin/sh\nexit 0\n' > "$bp_repo/.git/hooks/check_public_surface.sh"
chmod +x "$bp_repo/scripts/check_public_surface.sh" "$bp_repo/.git/hooks/check_public_surface.sh"
( cd "$bp_repo"
  printf 'a\n' > f; git add -A >/dev/null 2>&1
  git -c user.email=t@t -c user.name=t commit -q -m c1 >/dev/null 2>&1
  git remote add origin "$probe_remote" >/dev/null 2>&1
  git worktree add -q --detach "$work/bp-linked" HEAD >/dev/null 2>&1 ) || true
bp_gd="$(find "$bp_repo/.git/worktrees" -name gitdir 2>/dev/null | head -1)"
if [ -z "$bp_gd" ]; then
  echo "REFUSING: no linked-worktree back-pointer was created, so this section could" >&2
  echo "  not present the case it exists for." >&2
  exit 2
fi
chmod 000 "$bp_gd"
cp "$hook" "$bp_repo/.git/hooks/pre-push"; chmod +x "$bp_repo/.git/hooks/pre-push"
( cd "$bp_repo" && git push -q origin HEAD:refs/heads/bpprobe >"$work/bp.out" 2>&1 ) || true
chmod 644 "$bp_gd"
git -C "$probe_remote" update-ref -d refs/heads/bpprobe >/dev/null 2>&1 || true
if grep -q 'could not be read' "$work/bp.out" 2>/dev/null; then
  printf 'this hook: an unreadable back-pointer is refused, not skipped.\n'
else
  echo "FAIL: an unreadable worktree back-pointer was skipped silently; that" >&2
  echo "  checkout is then absent from the only pre-command PATH filter." >&2
  sed -n '1,3p' "$work/bp.out" >&2
  bp_fail=1
fi

# ---- the caller's invocation survives ----------------------------------------
#
# ⚠ BOTH OF THESE ARE REGRESSIONS A PREVIOUS ROUND OF MINE INTRODUCED, so they are
# held here rather than trusted. A wide "does not work" strips the working half too.
#
#   1. `--work-tree` is a selector independent of `--git-dir`; restoring only the
#      latter made the hook treat its invocation directory as the worktree and
#      refuse a clean push (shardpilot/shardpilot-go#79 review, reproduced).
#   2. Clearing `GIT_CONFIG_PARAMETERS` to stop `git -c` overrides also removed the
#      caller's command-scoped transport configuration, so the hook's own
#      `ls-remote` and `fetch` lost credentials a push had already authenticated
#      with. It is APPENDED to now, not cleared and not replaced.
inv_fail=0
inv_repo="$work/inv"; git init -q "$inv_repo"
mkdir -p "$inv_repo/scripts" "$inv_repo/.git/hooks"
printf '#!/bin/sh\nexit 0\n' > "$inv_repo/scripts/check_public_surface.sh"
printf '#!/bin/sh\nexit 0\n' > "$inv_repo/.git/hooks/check_public_surface.sh"
chmod +x "$inv_repo/scripts/check_public_surface.sh" "$inv_repo/.git/hooks/check_public_surface.sh"
( cd "$inv_repo"
  printf 'a\n' > f
  git add -A >/dev/null 2>&1
  git -c user.email=t@t -c user.name=t commit -q -m c1 >/dev/null 2>&1
  git remote add origin "$probe_remote" >/dev/null 2>&1 ) || true
cp "$hook" "$inv_repo/.git/hooks/pre-push"; chmod +x "$inv_repo/.git/hooks/pre-push"

# 1. Both selectors given explicitly, from a directory that is neither.
( cd "$work" && git --git-dir="$inv_repo/.git" --work-tree="$inv_repo" \
    push -q origin HEAD:refs/heads/invprobe >"$work/inv.out" 2>&1 ) || true
if grep -q 'index or working tree differs' "$work/inv.out" 2>/dev/null; then
  echo "FAIL: an explicit --work-tree was dropped, so a clean push was refused." >&2
  sed -n '1,3p' "$work/inv.out" >&2
  inv_fail=1
else
  printf '\nthis hook: an explicit --git-dir/--work-tree push is not refused.\n'
fi
git -C "$probe_remote" update-ref -d refs/heads/invprobe >/dev/null 2>&1 || true

# 2. The block that DETECTS the config-injecting environment must not clear it.
#    Extracted precisely: the hook has a second, legitimate `unset` loop for the
#    repository selectors, and a loose grep matched THAT one and reported a failure
#    that was not there.
cfg_blk_start="$(grep -n '^sp_cfg_env=no$' "$hook" | head -1 | cut -d: -f1)"
cfg_blk_end="$(grep -n '^unset gv gv_val$' "$hook" | head -1 | cut -d: -f1)"
if [ -z "$cfg_blk_start" ] || [ -z "$cfg_blk_end" ] || [ "$cfg_blk_end" -le "$cfg_blk_start" ]; then
  echo "REFUSING: the config-environment detection block was not found, so whether" >&2
  echo "  it clears the caller's configuration could not be checked." >&2
  exit 2
fi
if sed -n "${cfg_blk_start},${cfg_blk_end}p" "$hook" | grep -q 'unset "\$gv"'; then
  echo "FAIL: the config-injecting environment is cleared again, which takes the" >&2
  echo "  caller's transport configuration with it." >&2
  inv_fail=1
else
  printf 'this hook: the caller command-scoped configuration is left in place.\n'
fi

printf '\n%d gitfile read(s) found, %d still line-oriented.\n' "$gf_total" "$gf_bad"
printf '%d checkout-root case(s), %d failure(s).\n' "$itotal" "$ifail"
printf '%d case(s) judged, %d failure(s); %d normal-form case(s), %d failure(s).\n' \
  "$total" "$failures" "$ntotal" "$nfail"
[ "$failures" -eq 0 ] && [ "$nfail" -eq 0 ] && [ "$gf_bad" -eq 0 ] && [ "$exec_fail" -eq 0 ] && [ "$ifail" -eq 0 ] && [ "$idx_fail" -eq 0 ] && [ "$inv_fail" -eq 0 ] && [ "$chl_fail" -eq 0 ] && [ "$bp_fail" -eq 0 ] && [ "$kre_fail" -eq 0 ] && [ "$esc_fail" -eq 0 ] && [ "$ord_fail" -eq 0 ] || exit 1
