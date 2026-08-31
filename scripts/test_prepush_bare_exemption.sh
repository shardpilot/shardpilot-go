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

printf '\n%d case(s) judged, %d failure(s).\n' "$total" "$failures"
[ "$failures" -eq 0 ] || exit 1
