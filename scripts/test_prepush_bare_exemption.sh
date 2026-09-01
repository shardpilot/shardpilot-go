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
# The pattern matches ANY read redirected from a named file, not only paths spelled
# `.../.git`: the back-pointer inside `worktrees/<n>/gitdir` is the same class and
# the narrower pattern could not see it, so a line-oriented read there would have
# been reported green by a check whose stated subject is the class.
gitfile_reads="$(grep -n 'read -r[^<]*< *"' "$hook" || true)"
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
# The question is CONTAINMENT -- is the nearest bracket above the enumeration a
# `set -f` and not a `set +f` -- so the scan runs to the top of the file. A fixed
# lookback answered a different question, "is `set -f` within N lines", and a
# comment added above the loop pushed the bracket out of the window and reported a
# defect that was not in the hook.
i=$((enum_start - 1))
while [ "$i" -gt 0 ]; do
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






# ---- a resolved command path may contain a newline -----------------------------
#
# The batched `readlink -f` above is newline-delimited, and `readlink` has no
# NUL-delimited form on every platform this runs on -- BSD accepts `-fn` and
# nothing else. Read line by line, a checkout named `/tmp/co<NL>root` split into
# `/tmp/co` and `root/tracked.sh`; neither is inside the checkout, so the
# branch-controlled program ran and the push succeeded
# (shardpilot/shardpilot-go#79 review). The repair reads the batch only when the
# newline count equals the link count -- which holds exactly when no resolved path
# contains one -- and resolves per link, sentinelled, otherwise.
nlc_fail=0
nlc_root="$work/nlc"; rm -rf "$nlc_root"
nlc_co="$nlc_root/co
root"
mkdir -p "$nlc_co" "$nlc_root/safe"
git init -q "$nlc_co" >/dev/null 2>&1
git init -q --bare "$nlc_root/remote.git" >/dev/null 2>&1
nlc_mark="$work/nlc-fired"
( cd "$nlc_co"
  git config user.email t@example.invalid; git config user.name t
  printf '#!/bin/sh\necho FIRED >> "%s"\nexec /usr/bin/mktemp "$@"\n' "$nlc_mark" > tracked.sh
  chmod +x tracked.sh
  git add -A >/dev/null 2>&1
  git commit -qm c1 >/dev/null 2>&1
  git remote add origin "$nlc_root/remote.git" >/dev/null 2>&1 ) >/dev/null 2>&1
mkdir -p "$nlc_co/.git/hooks"
printf '#!/bin/sh\nexit 0\n' > "$nlc_co/.git/hooks/check_public_surface.sh"
chmod +x "$nlc_co/.git/hooks/check_public_surface.sh"
ln -sf "$nlc_co/tracked.sh" "$nlc_root/safe/mktemp"

# the stand-in reads the batch unconditionally, which is what the defect was
nlc_vuln="$work/nlc-vuln-hook"
sed 's|^  if \[ "\$sp_nlines" -eq "\${#sp_links\[@\]}" \]; then$|  if true; then|' "$hook" > "$nlc_vuln"
chmod +x "$nlc_vuln"
if cmp -s "$nlc_vuln" "$hook" || ! bash -n "$nlc_vuln" 2>/dev/null; then
  echo "REFUSING: the stand-in for the newline-delimited batch is identical to the" >&2
  echo "  hook or does not parse, so the control below cannot reproduce it." >&2
  exit 2
fi

nlc_probe() { # $1 = hook
  rm -f "$nlc_mark"
  cp "$1" "$nlc_co/.git/hooks/pre-push"; chmod +x "$nlc_co/.git/hooks/pre-push"
  ( cd "$nlc_co" && PATH="$nlc_root/safe:$PATH" \
      git push origin HEAD:refs/heads/nlcp >/dev/null 2>&1 )
  printf '%s' "$?" > "$work/nlc-rc"
  git -C "$nlc_root/remote.git" update-ref -d refs/heads/nlcp >/dev/null 2>&1 || true
  [ -s "$nlc_mark" ] && echo fired || echo quiet
}

if [ "$(nlc_probe "$nlc_vuln")" != fired ]; then
  echo "REFUSING: the tracked program did not run even with the batch read" >&2
  echo "  unconditionally, so this section cannot tell a fix from a rig that" >&2
  echo "  never reproduced the split." >&2
  exit 2
fi
printf '\npositive control: reading the batch by line ran the branch program.\n'
if [ "$(nlc_probe "$hook")" = fired ]; then
  echo "FAIL: a resolved command path containing a newline was split, and the" >&2
  echo "  branch-controlled program ran." >&2
  nlc_fail=1
else
  printf 'this hook: a resolved command path containing a newline is not split.\n'
fi
rm -f "$nlc_root/safe/mktemp"
if [ "$(nlc_probe "$hook")" = quiet ] && [ "$(cat "$work/nlc-rc" 2>/dev/null || echo 1)" -eq 0 ]; then
  printf 'this hook: an ordinary PATH is still accepted from such a checkout.\n'
else
  echo "FAIL: an ordinary push was refused, so the check above passes by refusing" >&2
  echo "  everything rather than by resolving pathnames whole." >&2
  nlc_fail=1
fi


# ---- `.GIT` is as unreachable from a branch as `.git` --------------------------
#
# Git refuses to track a path with a `.git` component in ANY spelling -- measured
# below rather than assumed -- so `<root>/.GIT/hooks` is beyond a branch's reach.
# The runtime containment check matched only lowercase and classified it as
# trackable, refusing every push from a repository initialised with
# `--separate-git-dir=<root>/.GIT` (shardpilot/shardpilot-go#79 review). The
# installer already folded; this copy did not.
cse_fail=0

# part 1 -- git's rule, asked of git rather than recalled. Runs everywhere.
cse_r="$work/cse-idx"; rm -rf "$cse_r"; mkdir -p "$cse_r"
( cd "$cse_r" && git init -q . && git config user.email t@example.invalid &&
  git config user.name t ) >/dev/null 2>&1
cse_staged=0
for cse_sp in .git .GIT .Git .gIt; do
  mkdir -p "$cse_r/$cse_sp" 2>/dev/null || continue
  printf 'x\n' > "$cse_r/$cse_sp/f" 2>/dev/null || continue
  git -C "$cse_r" add "$cse_sp/f" >/dev/null 2>&1
done
cse_staged="$(git -C "$cse_r" ls-files | wc -l | tr -d ' ')"
if [ "$cse_staged" -eq 0 ]; then
  printf '\nthis git: refuses every spelling of a `.git` path component.\n'
else
  echo "FAIL: git staged $cse_staged path(s) with a .git component, so the folding" >&2
  echo "  below rests on a rule this git does not have." >&2
  cse_fail=1
fi

# part 2 -- the hook's pattern, lifted from the hook so a change cannot drift past
# this. Runs everywhere: it asks about the RULE, not about a filesystem.
cse_pat="$(grep -o '\*/\.\[Gg\]\[Ii\]\[Tt\]/\*' "$hook" | head -1)"
if [ -z "$cse_pat" ]; then
  echo "FAIL: the runtime containment check no longer carries a case-folded .git" >&2
  echo "  component pattern, so a .GIT layout is refused as trackable again." >&2
  cse_fail=1
else
  cse_hit=no
  for cse_rel in '/.git/hooks/' '/.GIT/hooks/' '/.Git/hooks/' '/deep/.GIT/hooks/'; do
    case "$cse_rel" in
      */.[Gg][Ii][Tt]/*) ;;
      *) echo "FAIL: the folded pattern does not match $cse_rel" >&2; cse_hit=yes ;;
    esac
  done
  # and it must still REFUSE what has no such component, or it exempts everything
  case '/.repo/hooks/' in
    */.[Gg][Ii][Tt]/*) echo "FAIL: the folded pattern matches /.repo/hooks/, which is trackable" >&2; cse_hit=yes ;;
  esac
  [ "$cse_hit" = yes ] && cse_fail=1
  [ "$cse_hit" = yes ] || printf 'this hook: every spelling of a `.git` component is exempt, and only those.\n'
fi

# part 3 -- end to end, only where the filesystem can hold `.git` and `.GIT` apart.
# Reported rather than skipped silently: a case-insensitive volume cannot host this.
cse_fs="$work/cse-fs"; rm -rf "$cse_fs"; mkdir -p "$cse_fs"
: > "$cse_fs/aa"; : > "$cse_fs/AA" 2>/dev/null
if [ "$(ls -A "$cse_fs" | wc -l | tr -d ' ')" -lt 2 ]; then
  printf 'note: this filesystem folds case, so the end-to-end .GIT push was NOT run\n'
  printf '  here. The rule and the pattern above were, and CI runs on a\n'
  printf '  case-sensitive filesystem where this arm does execute.\n'
else
  cse_e="$work/cse-e2e"; rm -rf "$cse_e"; mkdir -p "$cse_e"
  git init -q --separate-git-dir="$cse_e/co/.GIT" "$cse_e/co" >/dev/null 2>&1
  git init -q --bare "$cse_e/remote.git" >/dev/null 2>&1
  ( cd "$cse_e/co"
    git config user.email t@example.invalid; git config user.name t
    printf 'a\n' > f.txt
    git add f.txt >/dev/null 2>&1
    git commit -qm c1 >/dev/null 2>&1
    git remote add origin "$cse_e/remote.git" >/dev/null 2>&1 ) >/dev/null 2>&1
  printf '#!/bin/sh\nexit 0\n' > "$cse_e/co/.GIT/hooks/check_public_surface.sh"
  chmod +x "$cse_e/co/.GIT/hooks/check_public_surface.sh"
  cp "$hook" "$cse_e/co/.GIT/hooks/pre-push"; chmod +x "$cse_e/co/.GIT/hooks/pre-push"
  if ( cd "$cse_e/co" && git push origin HEAD:refs/heads/csep >/dev/null 2>&1 ); then
    printf 'this hook: a push from a `--separate-git-dir=<root>/.GIT` checkout succeeds.\n'
  else
    echo "FAIL: a push from a .GIT separate-git-dir checkout was refused." >&2
    cse_fail=1
  fi
  git -C "$cse_e/remote.git" update-ref -d refs/heads/csep >/dev/null 2>&1 || true
fi


# ---- the work tree named explicitly is the root, not the caller's cwd ----------
#
# `git --git-dir=D --work-tree=W push`, run from a third directory, leaves this
# hook in the CALLER'S directory. The root taken from `pwd` was that directory, so
# `W/bin` survived the PATH filter and a tracked `grep` there ran at the
# executable-key scan before the worktree enumeration refused the push
# (shardpilot/shardpilot-go#79 review).
#
# ⚠ THE RIG NEEDS W TO BE UNNAMEABLE BY THE OTHER MECHANISM. With an ordinary
# layout the enumeration derives the main checkout from `<common>/.git` and drops
# `W/bin` anyway -- two rigs reported clean here before this one reproduced
# anything. W is a THIRD directory, which nothing else can name.
wtr_fail=0
wtr="$work/wtr"; rm -rf "$wtr"; mkdir -p "$wtr/elsewhere" "$wtr/alt/bin"
git init -q "$wtr/co" >/dev/null 2>&1
git init -q --bare "$wtr/remote.git" >/dev/null 2>&1
wtr_mark="$work/wtr-fired"
( cd "$wtr/co"
  git config user.email t@example.invalid; git config user.name t
  mkdir -p bin
  printf '#!/bin/sh\necho FIRED >> "%s"\nexec /usr/bin/grep "$@"\n' "$wtr_mark" > bin/grep
  chmod +x bin/grep
  printf 'a\n' > f.txt
  git add -A >/dev/null 2>&1; git commit -qm c1 >/dev/null 2>&1
  git remote add origin "$wtr/remote.git" >/dev/null 2>&1 ) >/dev/null 2>&1
cp -R "$wtr/co/bin" "$wtr/alt/" 2>/dev/null
printf '#!/bin/sh\nexit 0\n' > "$wtr/co/.git/hooks/check_public_surface.sh"
chmod +x "$wtr/co/.git/hooks/check_public_surface.sh"
# the stand-in takes the root from `pwd` whatever the work tree says
wtr_vuln="$work/wtr-vuln-hook"
sed 's|^if \[ -n "\$inherited_work_tree" \]; then$|if false; then|' "$hook" > "$wtr_vuln"
chmod +x "$wtr_vuln"
if cmp -s "$wtr_vuln" "$hook" || ! bash -n "$wtr_vuln" 2>/dev/null; then
  echo "REFUSING: the stand-in for the explicit work tree is identical to the hook" >&2
  echo "  or does not parse, so the control below reproduces nothing." >&2
  exit 2
fi
wtr_probe() { # $1 = hook
  rm -f "$wtr_mark"
  cp "$1" "$wtr/co/.git/hooks/pre-push"; chmod +x "$wtr/co/.git/hooks/pre-push"
  ( cd "$wtr/elsewhere" && PATH="$wtr/alt/bin:$PATH" \
      git --git-dir="$wtr/co/.git" --work-tree="$wtr/alt" \
          push origin HEAD:refs/heads/wtrp >/dev/null 2>&1 )
  printf '%s' "$?" > "$work/wtr-rc"
  git -C "$wtr/remote.git" update-ref -d refs/heads/wtrp >/dev/null 2>&1 || true
  [ -s "$wtr_mark" ] && echo fired || echo quiet
}
if [ "$(wtr_probe "$wtr_vuln")" != fired ]; then
  echo "REFUSING: the tracked program did not run even with the root taken from" >&2
  echo "  pwd, so this section cannot tell a fix from a rig that never" >&2
  echo "  reproduced the execution." >&2
  exit 2
fi
printf '\npositive control: with the root taken from pwd, the branch program ran.\n'
if [ "$(wtr_probe "$hook")" = fired ]; then
  echo "FAIL: a directory named by --work-tree supplied a command to this hook." >&2
  wtr_fail=1
else
  printf 'this hook: the explicitly named work tree is treated as the root.\n'
fi
# ⚠ NO "AND THE PUSH SUCCEEDS" ASSERTION HERE, DELIBERATELY. The declared work
# tree in this rig is a COPY of the checkout, so git's own comparison refuses it
# with "the index or working tree differs from HEAD" -- a refusal that belongs to
# the fixture, not to this repair. A first draft asserted success and passed only
# because the previous probe had refreshed the index, which is a rig artefact
# rather than a property. That the ordinary push path still works is asserted at
# the top of this file, where the fixture can hold it.

# ---- the armed ref is removed before its object store --------------------------
#
# `GIT_OBJECT_DIRECTORY` names `$tmpdir/objects`, so removing `$tmpdir` first left
# `update-ref -d` with no repository to open and `|| true` hid it, leaving
# `refs/prepush-base-*` behind (shardpilot/shardpilot-go#79 review). Asserted as
# ORDER in the cleanup function, plus the mechanism that makes the order matter.
ref_fail=0
cl_s="$(grep -n '^cleanup() {$' "$hook" | head -1 | cut -d: -f1)"
if [ -z "$cl_s" ]; then
  echo "REFUSING: the cleanup function was not found, so the order of its" >&2
  echo "  removals could not be checked." >&2
  exit 2
fi
cl_e="$(awk -v s="$cl_s" 'NR>s && /^}$/ {print NR; exit}' "$hook")"
cl_ref="$(awk -v a="$cl_s" -v b="$cl_e" 'NR>=a && NR<=b && /update-ref -d "\$base_ref"/ {print NR; exit}' "$hook")"
cl_rm="$(awk -v a="$cl_s" -v b="$cl_e" 'NR>=a && NR<=b && /rm -rf "\$tmpdir"/ {print NR; exit}' "$hook")"
if [ -z "$cl_ref" ] || [ -z "$cl_rm" ]; then
  echo "REFUSING: the cleanup no longer both deletes the armed ref and removes the" >&2
  echo "  temporary directory, so their order could not be checked." >&2
  exit 2
else
  if [ "$cl_ref" -lt "$cl_rm" ]; then
    printf 'this hook: the armed ref is deleted before its object store is removed.\n'
  else
    echo "FAIL: the temporary directory is removed before the armed ref is deleted," >&2
    echo "  so update-ref runs with no repository and the ref is left behind." >&2
    ref_fail=1
  fi
fi
# the mechanism, so the order above is not an arbitrary preference
ref_r="$work/refm"; rm -rf "$ref_r"; mkdir -p "$ref_r/tmp/objects"
git init -q "$ref_r/repo" >/dev/null 2>&1
( cd "$ref_r/repo"; git config user.email t@example.invalid; git config user.name t
  printf 'a\n' > f; git add -A >/dev/null 2>&1; git commit -qm c1 >/dev/null 2>&1
  git update-ref refs/prepush-base-test HEAD ) >/dev/null 2>&1
rm -rf "$ref_r/tmp"
( cd "$ref_r/repo" && GIT_OBJECT_DIRECTORY="$ref_r/tmp/objects" \
    git update-ref -d refs/prepush-base-test ) >/dev/null 2>&1
if git -C "$ref_r/repo" rev-parse --verify --quiet refs/prepush-base-test >/dev/null 2>&1; then
  printf 'positive control: deleting the store first does leave the ref behind.\n'
else
  echo "REFUSING: deleting the object store first did NOT strand the ref here, so" >&2
  echo "  the order asserted above guards nothing on this git." >&2
  exit 2
fi


# ---- a direct blob keeps the format its ref names ------------------------------
#
# A blob pushed straight at `refs/archive/logo.xbm` has no tree entry and no
# filename but the ref's last component. Staged under a hard-coded `.txt`, the
# only format signal was gone before the scanner classified it, and an XBM was
# approved though the scanner refuses `*.xbm` (shardpilot/shardpilot-go#79
# review). Asserted on the NAME the evidence is staged under, because that is the
# thing the defect destroys -- a verdict would also depend on the fixture
# satisfying every other rule the scanner has.
blb_fail=0
blb="$work/blb"; rm -rf "$blb"
git init -q "$blb/co" >/dev/null 2>&1
git init -q --bare "$blb/remote.git" >/dev/null 2>&1
blb_seen="$work/blb-staged"
( cd "$blb/co"
  git config user.email t@example.invalid; git config user.name t
  mkdir -p scripts
  printf '#!/bin/sh\nls scripts/ >> "%s" 2>/dev/null\nexit 0\n' "$blb_seen" > scripts/check_public_surface.sh
  chmod +x scripts/check_public_surface.sh
  printf 'a\n' > f.txt
  git add -A >/dev/null 2>&1; git commit -qm c1 >/dev/null 2>&1
  git remote add origin "$blb/remote.git" >/dev/null 2>&1 ) >/dev/null 2>&1
mkdir -p "$blb/co/.git/hooks"
cp "$blb/co/scripts/check_public_surface.sh" "$blb/co/.git/hooks/"
printf '#define logo_width 8\nstatic char logo_bits[] = { 0x00 };\n' > "$blb/logo.xbm"
blb_oid="$(git -C "$blb/co" hash-object -w "$blb/logo.xbm")"
git -C "$blb/co" update-ref refs/archive/logo.xbm "$blb_oid" >/dev/null 2>&1
if [ "$(git -C "$blb/co" cat-file -t refs/archive/logo.xbm 2>/dev/null)" != blob ]; then
  echo "REFUSING: a ref pointing directly at a blob could not be built, so this" >&2
  echo "  section is not exercising the path it describes." >&2
  exit 2
fi
blb_vuln="$work/blb-vuln-hook"
sed 's|"scripts/tag-target-as-committed.\$tt_sfx"|scripts/tag-target-as-committed.txt|' "$hook" > "$blb_vuln"
chmod +x "$blb_vuln"
if cmp -s "$blb_vuln" "$hook" || ! bash -n "$blb_vuln" 2>/dev/null; then
  echo "REFUSING: the stand-in for the staged blob name is identical to the hook or" >&2
  echo "  does not parse, so the control below reproduces nothing." >&2
  exit 2
fi
blb_probe() { # $1 = hook; prints the suffix the tag target was staged under
  : > "$blb_seen"
  cp "$1" "$blb/co/.git/hooks/pre-push"; chmod +x "$blb/co/.git/hooks/pre-push"
  ( cd "$blb/co" && git push origin refs/archive/logo.xbm:refs/archive/logo.xbm ) >/dev/null 2>&1
  git -C "$blb/remote.git" update-ref -d refs/archive/logo.xbm >/dev/null 2>&1 || true
  grep -o 'tag-target-as-committed\.[A-Za-z0-9_]*' "$blb_seen" 2>/dev/null | head -1
}
if [ "$(blb_probe "$blb_vuln")" != "tag-target-as-committed.txt" ]; then
  echo "REFUSING: the stand-in did not stage the tag target under .txt, so this" >&2
  echo "  section cannot tell a fix from a rig that staged nothing at all." >&2
  exit 2
fi
printf '\npositive control: the hard-coded name stages an XBM as .txt.\n'
if [ "$(blb_probe "$hook")" = "tag-target-as-committed.xbm" ]; then
  printf 'this hook: a blob pushed at a ref keeps the format the ref names.\n'
else
  echo "FAIL: the published ref's suffix was discarded before the scanner could" >&2
  echo "  classify the blob." >&2
  blb_fail=1
fi

# ---- a PATH directory may serve a command from inside a checkout ---------------
#
# The PATH filter drops an entry whose DIRECTORY resolves into a checkout. An
# ordinary directory outside every checkout that contains `mktemp` as a symlink to
# a tracked script passes it untouched, and the branch owns the target -- measured
# running twice before the hook read stdin (shardpilot/shardpilot-go#79 review).
# The comment there said "resolved physically, so a symlink pointing into the
# checkout is caught too", which is true of the directory and reads wider.
pcr_fail=0
pcr_s="$(grep -n '^# >>> PATH-COMMAND-RESOLUTION' "$hook" | head -1 | cut -d: -f1)"
pcr_e="$(grep -n '^# <<< PATH-COMMAND-RESOLUTION' "$hook" | head -1 | cut -d: -f1)"
if [ -z "$pcr_s" ] || [ -z "$pcr_e" ] || [ "$pcr_e" -le "$pcr_s" ]; then
  echo "REFUSING: the PATH-COMMAND-RESOLUTION markers are missing or out of order," >&2
  echo "  so the stand-in below cannot be built and this section would test nothing." >&2
  exit 2
fi
pcr_vuln="$work/pcr-vuln-hook"
sed "${pcr_s},${pcr_e}d" "$hook" > "$pcr_vuln"
chmod +x "$pcr_vuln"
if ! bash -n "$pcr_vuln" 2>/dev/null; then
  echo "FAIL: removing the PATH-COMMAND-RESOLUTION block leaves a hook that does" >&2
  echo "  not parse, so the block is entangled with the code around it." >&2
  pcr_fail=1
fi

pcr_root="$work/pcr"; rm -rf "$pcr_root"; mkdir -p "$pcr_root/safe"
git init -q "$pcr_root/co" >/dev/null 2>&1
git init -q --bare "$pcr_root/remote.git" >/dev/null 2>&1
pcr_mark="$work/pcr-fired"
( cd "$pcr_root/co"
  git config user.email t@example.invalid; git config user.name t
  printf '#!/bin/sh\necho FIRED >> "%s"\nexec /usr/bin/mktemp "$@"\n' "$pcr_mark" > tracked.sh
  chmod +x tracked.sh
  git add -A >/dev/null 2>&1
  git commit -qm c1 >/dev/null 2>&1
  git remote add origin "$pcr_root/remote.git" >/dev/null 2>&1 ) >/dev/null 2>&1
mkdir -p "$pcr_root/co/.git/hooks"
printf '#!/bin/sh\nexit 0\n' > "$pcr_root/co/.git/hooks/check_public_surface.sh"
chmod +x "$pcr_root/co/.git/hooks/check_public_surface.sh"
# the directory is OUTSIDE the checkout; only the command it serves is inside
ln -sf "$pcr_root/co/tracked.sh" "$pcr_root/safe/mktemp"

pcr_probe() { # $1 = hook
  rm -f "$pcr_mark"
  cp "$1" "$pcr_root/co/.git/hooks/pre-push"; chmod +x "$pcr_root/co/.git/hooks/pre-push"
  ( cd "$pcr_root/co" && PATH="$pcr_root/safe:$PATH" \
      git push origin HEAD:refs/heads/pcrp >/dev/null 2>&1 )
  # to a FILE, not a variable: this function is called inside `$( )` and an
  # assignment here dies with the subshell -- the same slip this file already
  # carries one fix for, made again three sections later
  printf '%s' "$?" > "$work/pcr-rc"
  git -C "$pcr_root/remote.git" update-ref -d refs/heads/pcrp >/dev/null 2>&1 || true
  [ -s "$pcr_mark" ] && echo fired || echo quiet
}

# the control: without the block the tracked program MUST run, or this section
# cannot tell a fix from a rig that never reproduced the execution
if [ "$(pcr_probe "$pcr_vuln")" != fired ]; then
  echo "REFUSING: the tracked program did not run even with the resolution block" >&2
  echo "  removed, so this section cannot tell a fix from a rig that never" >&2
  echo "  reproduced it." >&2
  exit 2
fi
printf '\npositive control: without the block, a PATH symlink ran the branch program.\n'
if [ "$(pcr_probe "$hook")" = fired ]; then
  echo "FAIL: a PATH directory outside every checkout served a command from inside" >&2
  echo "  one, and the branch-controlled program ran." >&2
  pcr_fail=1
else
  printf 'this hook: a PATH entry serving a command from a checkout is refused.\n'
fi

# and an ordinary PATH must still be accepted -- a gate that refuses every push
# also passes the check above
rm -f "$pcr_root/safe/mktemp"
if [ "$(pcr_probe "$hook")" = quiet ] && [ "$(cat "$work/pcr-rc" 2>/dev/null || echo 1)" -eq 0 ]; then
  printf 'this hook: an ordinary PATH is still accepted.\n'
else
  echo "FAIL: an ordinary push was refused, so the check above passes" >&2
  echo "  by refusing everything rather than by resolving commands." >&2
  pcr_fail=1
fi

# ---- a relative back-pointer names its checkout --------------------------------
#
# `gitdir: .repo` and `gitdir: ../meta` are valid, and git resolves them against
# the directory holding the `.git` file. Three sites here compared that RAW line
# against a value already made absolute, so the match could never succeed and the
# push was refused as coming from a git directory whose checkout cannot be named
# -- while the documented installer reported success
# (shardpilot/shardpilot-go#79 review).
#
# Asserted end to end, because the defect is in what the hook CONCLUDES, not in
# how a line is spelled: a structural check would have passed on all three sites
# while every such push was refused.
rel_fail=0
rel_root="$work/rel"; rm -rf "$rel_root"; mkdir -p "$rel_root"
git init -q --separate-git-dir="$rel_root/meta" "$rel_root/co" >/dev/null 2>&1
git init -q --bare "$rel_root/remote.git" >/dev/null 2>&1
printf '#!/bin/sh\nexit 0\n' > "$rel_root/meta/hooks/check_public_surface.sh"
chmod +x "$rel_root/meta/hooks/check_public_surface.sh"
( cd "$rel_root/co"
  git config user.email t@example.invalid; git config user.name t
  printf 'a\n' > f.txt
  git add -A >/dev/null 2>&1
  git commit -qm c1 >/dev/null 2>&1
  git remote add origin "$rel_root/remote.git" >/dev/null 2>&1 ) >/dev/null 2>&1

# The stand-in restores the defect exactly: `gitdir_target` hands back the RAW
# target, so the comparison is raw text against a resolved value. Disabling only
# the absolutisation was NOT enough -- `physdir` then resolves a relative target
# against the hook's own working directory and lands on the right answer by
# accident, and the control passed while reproducing nothing.
rel_vuln="$work/rel-vuln-hook"
awk '
  /^gitdir_target\(\) \{$/ { skip = 1
    print "gitdir_target() {"
    print "  case \"$1\" in \"gitdir: \"*) printf \"%sX\" \"${1#gitdir: }\" ;; *) return 1 ;; esac"
    print "}"
    next }
  skip && /^\}$/ { skip = 0; next }
  skip { next }
  { print }
' "$hook" > "$rel_vuln"
chmod +x "$rel_vuln"
if cmp -s "$rel_vuln" "$hook" || ! bash -n "$rel_vuln" 2>/dev/null; then
  echo "REFUSING: the stand-in for the relative back-pointer is identical to the" >&2
  echo "  hook or does not parse, so the control below cannot reproduce the" >&2
  echo "  defect. gitdir_target has moved or been rewritten." >&2
  exit 2
fi

rel_probe() { # $1 = hook to install, $2 = the `.git` file's contents
  printf '%s\n' "$2" > "$rel_root/co/.git"
  cp "$1" "$rel_root/meta/hooks/pre-push"; chmod +x "$rel_root/meta/hooks/pre-push"
  ( cd "$rel_root/co" && git push origin HEAD:refs/heads/relp >/dev/null 2>&1 )
  rel_rc=$?
  git -C "$rel_root/remote.git" update-ref -d refs/heads/relp >/dev/null 2>&1 || true
  return $rel_rc
}

# git itself must accept the construction, or the arms below prove nothing
printf 'gitdir: ../meta\n' > "$rel_root/co/.git"
if [ "$(cd "$rel_root/co" && git rev-parse --git-common-dir 2>/dev/null)" = "" ]; then
  echo "REFUSING: git did not resolve a relative back-pointer here, so this" >&2
  echo "  section is testing a repository git itself rejects." >&2
  exit 2
fi

if rel_probe "$rel_vuln" 'gitdir: ../meta'; then
  echo "REFUSING: the stand-in accepted a relative back-pointer, so this section" >&2
  echo "  cannot tell a fix from a rig that never reproduced the refusal." >&2
  exit 2
fi
printf '\npositive control: the stand-in refuses a checkout with a relative back-pointer.\n'

if rel_probe "$hook" 'gitdir: ../meta'; then
  printf 'this hook: a relative back-pointer names its checkout.\n'
else
  echo "FAIL: a push from a checkout whose .git holds a relative target was" >&2
  echo "  refused -- the raw line is being compared against an absolute value." >&2
  rel_fail=1
fi

# and the absolute form, which already worked, must go on working
if rel_probe "$hook" "gitdir: $rel_root/meta"; then
  printf 'this hook: an absolute back-pointer still names its checkout.\n'
else
  echo "FAIL: the absolute back-pointer stopped working, so the repair for the" >&2
  echo "  relative one moved the defect rather than removing it." >&2
  rel_fail=1
fi

# ---- the scanner's fixtures read no user configuration ------------------------
#
# The hook neutralises dangerous config keys by ENUMERATING them, and a
# conditional include is invisible to that enumeration by construction: an
# `includeIf "gitdir:**"` supplying `core.attributesFile` and `filter.<n>.clean`
# activates only once git is inside the scanner's `mktemp -d` fixtures, which is
# after the list was derived (shardpilot/shardpilot-go#79 review). Naming the key
# cannot converge -- the previous round added `init.templatedir` to the seed list
# and the next conditional key walked through the same gap.
#
# What closes it is that those repositories read NO user configuration. That is
# checked in two parts, because either alone would report green over a live hole:
# the mechanism works, and it is applied at every fixture.
cfg_fail=0
cfg_scanner="$here/scripts/check_public_surface.sh"
if [ ! -r "$cfg_scanner" ]; then
  echo "REFUSING: cannot read $cfg_scanner, so its fixtures were not checked." >&2
  exit 2
fi

# part 1 -- APPLIED AT EVERY FIXTURE. Structural, so a fixture added later without
# the call is caught rather than inherited silently.
gi_total=0; gi_bad=0
while IFS= read -r gi_n; do
  [ -n "$gi_n" ] || continue
  gi_total=$((gi_total + 1))
  gi_prev="$(awk -v n="$gi_n" 'NR<n && NF {last=$0} END{print last}' "$cfg_scanner")"
  case "$gi_prev" in
    *sp_fixture_isolation*) ;;
    *) echo "FAIL: a fixture repository is initialised without config isolation at" >&2
       echo "  $cfg_scanner:$gi_n -- a conditional include activates there." >&2
       gi_bad=$((gi_bad + 1)) ;;
  esac
done <<CFGINIT
$(grep -n '^[[:space:]]*git init -q \.$' "$cfg_scanner" | cut -d: -f1)
CFGINIT
if [ "$gi_total" -eq 0 ]; then
  echo "REFUSING: no fixture initialisation was found in the scanner at all, so" >&2
  echo "  whether they isolate configuration could not be checked." >&2
  exit 2
fi
[ "$gi_bad" -eq 0 ] || cfg_fail=1

# part 2 -- THE MECHANISM WORKS. The function is lifted from the scanner rather
# than restated here, so a change that hollows it out is caught.
cfg_s="$(grep -n '^sp_fixture_isolation() {$' "$cfg_scanner" | head -1 | cut -d: -f1)"
cfg_e=""
if [ -n "$cfg_s" ]; then
  cfg_e="$(awk -v s="$cfg_s" 'NR>s && /^}$/ {print NR; exit}' "$cfg_scanner")"
fi
if [ -z "$cfg_s" ] || [ -z "$cfg_e" ]; then
  echo "REFUSING: sp_fixture_isolation was not found in the scanner, so the" >&2
  echo "  mechanism behind the check above could not be exercised." >&2
  exit 2
fi
sed -n "${cfg_s},${cfg_e}p" "$cfg_scanner" > "$work/isolation.sh"
if ! bash -n "$work/isolation.sh" 2>/dev/null; then
  echo "FAIL: the lifted sp_fixture_isolation does not parse." >&2
  cfg_fail=1
fi

# a global configuration whose include activates inside ANY repository, so the
# rig does not depend on where mktemp puts the fixture
cfg_home="$work/poison-home"; mkdir -p "$cfg_home"
cfg_mark="$work/cfg-fired"
printf '#!/bin/sh\necho FIRED >> "%s"\ncat\n' "$cfg_mark" > "$cfg_home/evil.sh"
chmod +x "$cfg_home/evil.sh"
printf '[includeIf "gitdir:**"]\n\tpath = %s/cond\n' "$cfg_home" > "$cfg_home/.gitconfig"
printf '[core]\n\tattributesFile = %s/attrs\n[filter "evil"]\n\tclean = %s/evil.sh\n' \
  "$cfg_home" "$cfg_home" > "$cfg_home/cond"
printf '* filter=evil\n' > "$cfg_home/attrs"

cfg_probe() { # $1 = yes to apply the isolation
  rm -f "$cfg_mark"
  cfg_rep="$work/cfgfix.$$"; rm -rf "$cfg_rep"; mkdir -p "$cfg_rep"
  ( cd "$cfg_rep"
    export HOME="$cfg_home" XDG_CONFIG_HOME="$cfg_home"
    if [ "$1" = yes ]; then . "$work/isolation.sh"; sp_fixture_isolation; fi
    git init -q .
    git config user.email t@example.invalid; git config user.name t
    printf 'hello\n' > f.txt
    git add -A >/dev/null 2>&1 ) >/dev/null 2>&1
  # recorded to a FILE, not a variable: this function is called inside `$( )`,
  # so an assignment here dies with the subshell and the caller read an empty
  # value as zero -- the assertion below then reported a fixture that had in
  # fact staged its file
  git -C "$cfg_rep" ls-files | wc -l | tr -d ' ' > "$work/cfg-staged"
  rm -rf "$cfg_rep"
  [ -s "$cfg_mark" ] && echo fired || echo quiet
}

# the control: without the isolation the conditional filter MUST run here, or the
# check below cannot tell a fix from a rig that never reproduced the attack
if [ "$(cfg_probe no)" != fired ]; then
  echo "REFUSING: the conditional-include filter did not run even without the" >&2
  echo "  isolation, so this section cannot tell a fix from a rig that never" >&2
  echo "  reproduced it. git may no longer honour includeIf gitdir:** here." >&2
  exit 2
fi
printf '\npositive control: a conditional include runs a filter in an unisolated fixture.\n'
if [ "$(cfg_probe yes)" = fired ]; then
  echo "FAIL: a conditional include still executed a program in an isolated" >&2
  echo "  fixture -- the isolation does not stop what the enumeration cannot see." >&2
  cfg_fail=1
else
  printf 'this scanner: an isolated fixture ran nothing from the configuration.\n'
fi
# and the isolation must not cost the fixture its own work
if [ "$(cat "$work/cfg-staged" 2>/dev/null || echo 0)" -lt 1 ]; then
  echo "FAIL: the isolated fixture staged nothing, so the check above passed" >&2
  echo "  because the fixture stopped working, not because nothing ran." >&2
  cfg_fail=1
else
  printf 'this scanner: the isolated fixture still stages its file.\n'
fi

# ---- the README installer, run rather than read -------------------------------
#
# The installer published in README.md is 91 lines of shell that every adopter
# pastes once, and until now nothing executed it. Over this PR's rounds the
# reviewer raised 43 findings against that file against 163 against the hook --
# three times its share of the diff -- and each was repaired by READING, because
# there was nothing to run. A defect survived all of it: on any failure before the
# `sp_rollback` definition, `&&` and `||` associate left-to-right, so the bare
# `|| sp_rollback` operators fired for a failure ten lines earlier and called a
# function that did not exist yet, four times, exiting 127 with no rollback and no
# usable message. Reading did not find that in nine rounds; running it found it in
# one.
#
# ⚠ IT LIFTS THE PUBLISHED TEXT, exactly as the parse block above does. A copy
# here would drift from the README and pass while adopters ran something else.
# If the anchor stops matching, this REFUSES rather than reporting green.
ins_fail=0
ins_src="$here/README.md"
ins="$work/installer.sh"
if [ ! -r "$ins_src" ]; then
  echo "REFUSING: cannot read $ins_src, so the section below had nothing to run." >&2
  exit 2
fi
# The whole FENCED BLOCK is lifted, located by the anchor it contains rather than
# starting at it: the block is wrapped in a subshell, so its first line is no
# longer the anchor, and an extraction that began at the anchor would drop the
# opening parenthesis and lift something that does not parse.
awk '
  /^```/ {
    if (inb) { if (found) { printf "%s", buf; exit } ; inb=0; buf=""; found=0 }
    else { inb=1; buf=""; found=0 }
    next
  }
  inb {
    buf = buf $0 "\n"
    if (index($0, "p_(){ x=\"$(cd -- \"$1\"") == 1) found=1
  }
' "$ins_src" > "$ins"
if [ ! -s "$ins" ]; then
  echo "REFUSING: the installer block was not found in README.md. The anchor here has" >&2
  echo "  stopped matching it, and this section would otherwise test nothing." >&2
  exit 2
fi
if ! bash -n "$ins" 2>/dev/null; then
  echo "FAIL: the installer published in README.md does not parse." >&2
  ins_fail=1
fi

# A `mktemp` and an `mv` that fail on demand, so a failure can be placed at a
# chosen step instead of being waited for.
# ⚠ THE REAL BINARIES ARE RESOLVED BEFORE THE SHIM DIRECTORY EXISTS, and each shim
# execs the absolute path. A shim that ends `exec mv "$@"` re-execs ITSELF, because
# it is what the shim directory put first on PATH: one logical `mv` then consumed
# two shim calls, `SP_MV_FAIL_AT=2` failed the FIRST move rather than the second,
# nothing was ever published, and the assertion that the published hook is
# withdrawn passed over a run in which there was nothing to withdraw.
ins_real_mktemp="$(command -v mktemp)"
ins_real_mv="$(command -v mv)"
if [ -z "$ins_real_mktemp" ] || [ -z "$ins_real_mv" ]; then
  echo "REFUSING: mktemp or mv could not be resolved, so the injected failures" >&2
  echo "  below would re-enter the shim instead of the real program." >&2
  exit 2
fi
mkdir -p "$work/shim"
cat > "$work/shim/mktemp" <<INSSHIM
#!/usr/bin/env bash
n=\$(( \$(cat "\$SP_N" 2>/dev/null || echo 0) + 1 )); printf '%s' "\$n" > "\$SP_N"
[ "\${SP_FAIL_AT:-}" = "\$n" ] && { echo "mktemp: injected failure" >&2; exit 1; }
exec $ins_real_mktemp "\$@"
INSSHIM
cat > "$work/shim/mv" <<INSSHIM
#!/usr/bin/env bash
n=\$(( \$(cat "\$SP_MVN" 2>/dev/null || echo 0) + 1 )); printf '%s' "\$n" > "\$SP_MVN"
if [ "\${SP_MV_FAIL_AT:-}" = "\$n" ]; then
  # record what had already been published when the failure was injected, so the
  # withdrawal asserted below cannot pass over a run that published nothing
  ls "\$(dirname "\$2")" > "\$SP_MVSTATE" 2>/dev/null
  echo "mv: injected failure" >&2; exit 1
fi
exec $ins_real_mv "\$@"
INSSHIM
chmod +x "$work/shim/mktemp" "$work/shim/mv"

ins_fixture(){ # $1 = directory
  rm -rf "$1"; mkdir -p "$1/.githooks" "$1/scripts"
  printf '#!/usr/bin/env bash\nexit 0\n' > "$1/.githooks/pre-push"
  printf '#!/usr/bin/env bash\nexit 0\n' > "$1/scripts/check_public_surface.sh"
  chmod +x "$1/.githooks/pre-push" "$1/scripts/check_public_surface.sh"
  git -C "$1" init -q . >/dev/null 2>&1
  git -C "$1" config user.email t@example.invalid
  git -C "$1" config user.name t
  git -C "$1" add -A >/dev/null 2>&1
  git -C "$1" commit -qm init >/dev/null 2>&1
}
ins_say(){ if [ "$1" = 0 ]; then printf 'this installer: %s\n' "$2"
           else echo "FAIL: installer -- $2" >&2; ins_fail=1; fi; }

# an ordinary checkout installs, and leaves no staging name behind
insA="$work/insA"; ins_fixture "$insA"
( cd "$insA" && bash "$ins" ) >"$work/insA.out" 2>&1
ins_say "$?" "an ordinary checkout installs"
[ -x "$insA/.git/hooks/pre-push" ]; ins_say "$?" "the hook is published and executable"
[ -x "$insA/.git/hooks/check_public_surface.sh" ]; ins_say "$?" "the scanner is published"
# compared against the RESOLVED root: the installer resolves symlinks with `pwd -P`
# and a temporary directory commonly sits under one, so the unresolved name would
# differ from a correct answer.
insA_real="$(cd "$insA" && pwd -P)"
[ "$(git -C "$insA" config --local --get core.hooksPath)" = "$insA_real/.git/hooks" ]
ins_say "$?" "core.hooksPath names the published directory"

# a bare repository installs from HEAD: rather than from a worktree it has not got
insB="$work/insB"; ins_fixture "$work/insBsrc"
git clone -q --bare "$work/insBsrc" "$insB" >/dev/null 2>&1
( cd "$insB" && bash "$ins" ) >"$work/insB.out" 2>&1
ins_say "$?" "a bare repository installs from HEAD:"
[ -x "$insB/hooks/pre-push" ]; ins_say "$?" "the bare arm publishes the hook"

# ---- the failure paths, which are the ones nobody runs by hand ----------------
#
# A staging name left behind is not a cosmetic leak: the installer's own
# occupied-path guard refuses every later attempt until it is removed by hand, so
# one failed install bricks the documented instructions for that checkout.
insC="$work/insC"; ins_fixture "$insC"
: > "$work/n"
( cd "$insC" && SP_N="$work/n" SP_FAIL_AT=3 PATH="$work/shim:$PATH" bash "$ins" ) \
  >"$work/insC.out" 2>&1
[ "$?" -ne 0 ]; ins_say "$?" "a failed temporary file fails the install"
! ls -A "$insC/.git/hooks" 2>/dev/null | grep -q '^\.\(pre-push\|check_public_surface\)'
ins_say "$?" "no staging name is left behind"
( cd "$insC" && bash "$ins" ) >"$work/insCr.out" 2>&1
ins_say "$?" "a retry after that failure still installs"

# A failure BEFORE the handler is defined must not reach for it. This is the
# left-to-right association of `&&` and `||`: every unbracketed `|| sp_rollback`
# is the handler for the whole chain that precedes it, not for the command it
# follows, so the handler must be bracketed to its own command.
insD="$work/insD"; ins_fixture "$insD"
: > "$work/n"
( cd "$insD" && SP_N="$work/n" SP_FAIL_AT=3 PATH="$work/shim:$PATH" bash "$ins" ) \
  >"$work/insD.out" 2>&1
ins_d_rc=$?
! grep -q "command not found" "$work/insD.out"
ins_say "$?" "an early failure does not call a handler that does not exist yet"
[ "$ins_d_rc" -ne 0 ] && [ "$ins_d_rc" -ne 127 ]
ins_say "$?" "an early failure exits with a real status, not 127"

# and a failure AFTER publication must undo what was published, including the
# configuration value it replaced
insE="$work/insE"; ins_fixture "$insE"
git -C "$insE" config --local core.hooksPath /nonexistent/prior
: > "$work/mvn"
( cd "$insE" && SP_MVN="$work/mvn" SP_MVSTATE="$work/mvstate" SP_MV_FAIL_AT=2 \
    PATH="$work/shim:$PATH" bash "$ins" ) >"$work/insE.out" 2>&1
[ "$?" -ne 0 ]; ins_say "$?" "a failure after publication fails the install"
# the failure must have landed AFTER the first move, or the withdrawal below is
# asserted over a directory that never held the hook
grep -q '^pre-push$' "$work/mvstate" 2>/dev/null
ins_say "$?" "the hook was published before the injected failure"
grep -q "rolled back" "$work/insE.out"; ins_say "$?" "the rollback says so"
[ ! -e "$insE/.git/hooks/pre-push" ]; ins_say "$?" "the published hook is withdrawn"
[ "$(git -C "$insE" config --local --get core.hooksPath)" = /nonexistent/prior ]
ins_say "$?" "the prior core.hooksPath is restored"

# ---- the installer on an external git directory, and an honest rollback --------
#
# Two findings against the published installer
# (shardpilot/shardpilot-go#79 review), both reproduced before they were repaired.
#
# `git worktree list` reports the GIT DIRECTORY rather than the checkout for a
# repository made with `--separate-git-dir`, so the ownership test looked beside
# that directory for a back-pointer, did not find one, and refused with "install
# from the main checkout" while running in the main checkout. The back-pointer it
# needed was in the invoking checkout, which the installer had already resolved.
#
# And `sp_rollback` withdrew the published hooks BEFORE restoring
# `core.hooksPath`, then ignored a restore failure and reported success. The
# result is the worst available state: the configuration still names the hooks
# directory, the hooks are gone, and no pre-push hook runs at all while the
# installer says the prior setup was restored.
ins2_fail=0

# ---- an external `--separate-git-dir` checkout can install --------------------
ins2_x="$work/ins2x"; rm -rf "$ins2_x"; mkdir -p "$ins2_x/store"
git init -q --separate-git-dir="$ins2_x/store/meta" "$ins2_x/co" >/dev/null 2>&1
mkdir -p "$ins2_x/co/.githooks" "$ins2_x/co/scripts"
printf '#!/bin/sh\nexit 0\n' > "$ins2_x/co/.githooks/pre-push"
printf '#!/bin/sh\nexit 0\n' > "$ins2_x/co/scripts/check_public_surface.sh"
chmod +x "$ins2_x/co/.githooks/pre-push" "$ins2_x/co/scripts/check_public_surface.sh"
( cd "$ins2_x/co"; git config user.email t@example.invalid; git config user.name t
  git add -A >/dev/null 2>&1; git commit -qm i >/dev/null 2>&1 ) >/dev/null 2>&1

# the stand-in drops the branch that consults the invoking checkout
ins2_vuln="$work/ins2-vuln.sh"
# the stand-in disables the branch that consults the invoking checkout; the
# comparison is an `elif` now, not a `case` arm, so the pattern follows it
sed 's|^        elif test -n "\$rr" && test -n "\$gi_t" && test "\$gi_t" = "\$rr"$|        elif false|' "$ins" > "$ins2_vuln"
if cmp -s "$ins2_vuln" "$ins"; then
  echo "REFUSING: the stand-in for the external git directory is identical to the" >&2
  echo "  published installer, so the control below reproduces nothing." >&2
  exit 2
fi
ins2_run() { # $1 = installer
  rm -f "$ins2_x/store/meta/hooks/pre-push" "$ins2_x/store/meta/hooks/check_public_surface.sh"
  git -C "$ins2_x/co" config --local --unset core.hooksPath >/dev/null 2>&1 || true
  ( cd "$ins2_x/co" && bash "$1" ) >"$work/ins2.out" 2>&1
  printf '%s' "$?" > "$work/ins2-rc"
}
ins2_run "$ins2_vuln"
if [ "$(cat "$work/ins2-rc")" -eq 0 ]; then
  echo "REFUSING: the stand-in installed into an external git directory, so this" >&2
  echo "  section cannot tell a fix from a rig that never reproduced the refusal." >&2
  exit 2
fi
printf '\npositive control: without the invoking-checkout branch the install is refused.\n'
ins2_run "$ins"
if [ "$(cat "$work/ins2-rc")" -eq 0 ] && [ -x "$ins2_x/store/meta/hooks/pre-push" ]; then
  printf 'this installer: an external `--separate-git-dir` checkout installs.\n'
else
  echo "FAIL: the installer refused a repository whose git directory is external," >&2
  echo "  which is the layout its own documentation describes." >&2
  sed 's/^/      /' "$work/ins2.out" >&2
  ins2_fail=1
fi

# ---- a rollback that cannot restore the configuration says so ----------------
#
# The shim fails the post-publication verification, then refuses every local
# config write -- which is what a full filesystem does to the same sequence.
# ⚠ THE REAL `git` IS RESOLVED BEFORE THE SHIM DIRECTORY EXISTS. A shim named
# `git` that ends `exec git "$@"` re-execs ITSELF, because the shim directory is
# what put it first on PATH -- the same slip this file already carries one fix
# for, in the `mv` shim above, made again here.
ins2_real_git="$(command -v git)"
if [ -z "$ins2_real_git" ]; then
  echo "REFUSING: git could not be resolved, so the shim below would re-enter" >&2
  echo "  itself instead of running the real program." >&2
  exit 2
fi
mkdir -p "$work/ins2shim"
cat > "$work/ins2shim/git" <<INS2SHIM
#!/usr/bin/env bash
if [ "\$1" = "-C" ] && [ "\$3" = "rev-parse" ]; then
  : > "\$SP_PHASE"; echo "git: injected verification failure" >&2; exit 1
fi
if [ -f "\$SP_PHASE" ]; then
  for a in "\$@"; do [ "\$a" = "--local" ] && { echo "git: could not write config" >&2; exit 1; }; done
fi
exec $ins2_real_git "\$@"
INS2SHIM
chmod +x "$work/ins2shim/git"
# the stand-in swallows the restore failure, which is what the defect was
ins2_rbv="$work/ins2-rb-vuln.sh"
sed 's|2>/dev/null \|\| sp_done=no|2>/dev/null \|\| true|g' "$ins" > "$ins2_rbv"
if cmp -s "$ins2_rbv" "$ins"; then
  echo "REFUSING: the stand-in for the rollback is identical to the published" >&2
  echo "  installer, so the control below reproduces nothing." >&2
  exit 2
fi
ins2_rb() { # $1 = installer, $2 = tag
  ins2_d="$work/ins2rb$2"; ins_fixture "$ins2_d"
  git -C "$ins2_d" config --local core.hooksPath /prior/path >/dev/null 2>&1
  rm -f "$work/ins2-phase"
  ( cd "$ins2_d" && SP_PHASE="$work/ins2-phase" PATH="$work/ins2shim:$PATH" bash "$1" ) \
    >"$work/ins2rb.out" 2>&1
  [ -e "$ins2_d/.git/hooks/pre-push" ] && echo kept || echo gone
}
if [ "$(ins2_rb "$ins2_rbv" v)" != gone ]; then
  echo "REFUSING: the swallowing stand-in did not withdraw the hook, so this" >&2
  echo "  section cannot tell a fix from a rig that never reproduced the state." >&2
  exit 2
fi
printf 'positive control: swallowing the restore failure withdraws the only hook.\n'
if [ "$(ins2_rb "$ins" f)" = kept ] && grep -q "INCOMPLETE" "$work/ins2rb.out"; then
  printf 'this installer: a rollback that cannot restore the configuration says so,\n'
  printf '  and leaves a hook that still runs.\n'
else
  echo "FAIL: the rollback withdrew the published hook while core.hooksPath still" >&2
  echo "  named it, leaving no pre-push hook running, and did not say so." >&2
  ins2_fail=1
fi

# ---- a refusal must not close the shell that pasted the block -----------------
#
# The README tells a developer to paste this block. Every refusal in it is an
# `exit`, and in an interactive shell `exit` closes THAT shell -- so an occupied
# destination, or any later validation failure, took the terminal and its jobs
# with it (shardpilot/shardpilot-go#79 review). The block is a subshell now.
ins3_d="$work/ins3"; ins_fixture "$ins3_d"
printf 'occupied\n' > "$ins3_d/.git/hooks/pre-push"
ins3_vuln="$work/ins3-vuln.sh"
sed -e '/^($/d' -e '/^)$/d' "$ins" > "$ins3_vuln"
if cmp -s "$ins3_vuln" "$ins"; then
  echo "REFUSING: the installer block carries no subshell parentheses to remove," >&2
  echo "  so the control below cannot reproduce the closed shell." >&2
  exit 2
fi
ins3_paste() { # $1 = block; prints whether the pasting shell lived
  ( cd "$ins3_d" && bash -c '. "$1"; echo LIVED' _ "$1" ) 2>/dev/null | grep -q LIVED &&
    echo lived || echo closed
}
if [ "$(ins3_paste "$ins3_vuln")" != closed ]; then
  echo "REFUSING: the unwrapped block did not close its caller, so this section" >&2
  echo "  cannot tell a fix from a rig that never reproduced it." >&2
  exit 2
fi
printf '\npositive control: unwrapped, a refusal closes the shell that pasted it.\n'
if [ "$(ins3_paste "$ins")" = lived ]; then
  printf 'this installer: a refusal leaves the pasting shell alive.\n'
else
  echo "FAIL: a refusal closed the shell that pasted the block." >&2
  ins2_fail=1
fi

printf '\n%d gitfile read(s) found, %d still line-oriented.\n' "$gf_total" "$gf_bad"
printf '%d checkout-root case(s), %d failure(s).\n' "$itotal" "$ifail"
printf '%d case(s) judged, %d failure(s); %d normal-form case(s), %d failure(s).\n' \
  "$total" "$failures" "$ntotal" "$nfail"
[ "$failures" -eq 0 ] && [ "$nfail" -eq 0 ] && [ "$gf_bad" -eq 0 ] && [ "$exec_fail" -eq 0 ] && [ "$ifail" -eq 0 ] && [ "$idx_fail" -eq 0 ] && [ "$inv_fail" -eq 0 ] && [ "$chl_fail" -eq 0 ] && [ "$bp_fail" -eq 0 ] && [ "$kre_fail" -eq 0 ] && [ "$esc_fail" -eq 0 ] && [ "$ord_fail" -eq 0 ] && [ "$ins_fail" -eq 0 ] && [ "$cfg_fail" -eq 0 ] && [ "$rel_fail" -eq 0 ] && [ "$pcr_fail" -eq 0 ] && [ "$nlc_fail" -eq 0 ] && [ "$cse_fail" -eq 0 ] && [ "$ins2_fail" -eq 0 ] && [ "$wtr_fail" -eq 0 ] && [ "$ref_fail" -eq 0 ] && [ "$blb_fail" -eq 0 ] || exit 1
