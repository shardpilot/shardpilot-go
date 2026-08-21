#!/usr/bin/env bash
# check_public_surface.sh — this repository is PUBLIC, so every tracked byte is
# published. This gate fails when internal ShardPilot material appears in the
# part of the tree it covers.
#
# WHY IT EXISTS. The org-wide publication-readiness check enumerates
# publication CANDIDATES — repositories that are private and might be flipped.
# This repository is not a candidate, because it is already public. Nothing
# scanned it, and that is exactly how an internal review-process skill,
# internal decision-record ids, an internal service name and an internal commit
# sha came to sit in a public repository for months. A gate that runs where the
# exposure already exists is the fix.
#
# ── ONE CLASS IS NOT ABOUT NAMES AT ALL ─────────────────────────────────────
# NEGATIVE STATEMENTS ABOUT COVERAGE. A sentence naming a test tool, a
# repository, and the fact that the one does not exist in the other. It names
# no service, no host and no credential, and it was the single most valuable
# line in the material this gate was built after: a published map of where our
# testing does not reach. An outsider does not need a secret if they are told
# where nobody is looking.
#
# The example is described rather than quoted, and that is not fastidiousness —
# the first version of this paragraph reproduced the sentence verbatim, so the
# file explaining why such a sentence must not be published was publishing one.
# It went unseen while this file exempted itself from its own scan.
#
# Deleting the file that carried it is not enough, because the next such
# sentence will be written in good faith by someone documenting an honest
# limit. Honesty about limits belongs in internal docs; in a repository that
# publishes, it is reconnaissance. Hence a class rather than a cleanup.
#
# ── THE OTHER PATTERNS ARE SHAPES, NOT A ROSTER ─────────────────────────────
# A gate against internal names, written as a list of internal names, publishes
# that list. The first draft of this file did exactly that: it introduced five
# internal names into this repository that had never appeared in it, and then
# exempted itself from the lane it gates. It was a leak wearing the costume of
# a fix.
#
# So the patterns below match SHAPES wherever a shape works. `ADR-[0-9]+`,
# `GAP-[0-9]{3}` and `main @ <sha>` name no record, no ticket and no branch.
# `ADR-[0-9]+` and `GAP-[0-9]+` name no decision and no ticket. The only
# literals that remain are ones this repository's TREE still carries elsewhere,
# so the file adds nothing to what a clone of it already hands over.
#
# THREE SHAPES WERE TRIED AND WITHDRAWN, because a gate that cries wolf gets
# silenced, and a silenced gate is the one that misses the real thing:
#   `-plane`     — this SDK's own vocabulary says event plane, consent plane,
#                  analytics plane. Narrowed to the single `-plane` name that
#                  is a service rather than a concept; it is in ROSTER above.
#   `-platform`  — fired on "per-platform" and "cross-platform". Dropped; the
#                  only internal `-platform` name appears in zero commits here.
#   `-service`   — fired on "self-service". Replaced by the two service names
#                  this repository's tree still carries elsewhere, which is
#                  the rule the whole list follows.
#
# ⚠ THE GAP THAT LEAVES, NAMED RATHER THAN IMPLIED: a service name that has
# never appeared here would not be caught, and this repository is outside the
# org-wide check that would catch it (it enumerates PRIVATE candidates). Until
# that check covers already-public repositories, a NEW internal name reaching
# this tree is caught by review or not at all.
#   `[0-9a-f]{40}` — fired on a legitimate pinned engine sha1 in CI. Narrowed
#                  to `main @ <sha>`, which is the shape that actually leaked.
# Each withdrawal is a real gap, and each is why the innocent half of the
# self-test exists: re-broadening any of them fails the run instead of
# producing noise somebody turns off. Shapes are chosen against THIS tree's
# prose, not in the abstract.
#
# The cost is stated rather than hidden: a single-word internal name with no
# structural tell (a bare codename, a one-word service) is NOT matched, and
# the shape patterns will occasionally fire on innocent prose. A false
# positive here is a sentence to reword. A false negative is a publication.
#
# ── SCOPE, STATED SO A PASS IS NOT READ AS MORE THAN IT IS ──────────────────
# LANE A is GATED at zero: every tracked file that is not Go source.
# LANE B is REPORTED, NOT GATED: Go source (*.go). Those comments are owned by
#   another workstream (the SDK wire freeze) and editing them here would
#   collide with it. Lane B prints its count on every run so the debt stays
#   visible instead of being implied away by lane A's green.
# Neither lane looks at git HISTORY. Deleting a line does not unpublish the
# commit that carried it.
#
# ⚠ AND THE CORPUS'S OWN COVERAGE SENTENCES ARE NOT BOUNDED BY ANYTHING
# HERE. Identifiers in it are, by shape — nothing real is numbered all
# zeros, nines or f's — but a coverage disclosure has no synthetic form: a
# sentence about missing tests reads identically whether its subject is
# invented or real. What narrows it is that every known-internal line must
# MATCH a class, so the only thing that fits there is another
# coverage-shaped sentence, and that the file is small enough to read. A
# gate cannot close this; a reviewer can.
#

set -euo pipefail

SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
if [ ! -f "$SELF" ]; then
  echo "REFUSING: cannot resolve this script's own path ($SELF)." >&2
  echo "  The self-audit greps read it by path; an unresolvable one makes them" >&2
  echo "  read nothing and report clean." >&2
  exit 2
fi

cd "$(dirname "$0")/.."

# One pattern list, used by both lanes and by the self-test — two spellings is
# how the second consumer comes to check something different from the first.
# ── THE ROSTER HALF, AND THE RULE THAT NOW ENFORCES ITSELF ──────────────────
# These are the only literal internal names in this file. Each is admissible
# under one rule: **this repository's tracked tree must still carry it
# somewhere else**, so the file adds nothing to what a clone already hands over.
#
# That rule started as "published history must carry it", which is weaker in the
# way that matters: history keeps everything, so a name stayed admissible
# forever once it had ever appeared. Under it, SIX of eight entries here existed
# nowhere but this file — the cleanup had removed them everywhere else, and the
# gate had quietly become their sole carrier, shipping in every release archive
# a set of names the cleanup existed to remove. Tree-presence closes both that
# and the original case.
#
# That rule was PROSE until 2026-08-20, and prose does not hold. Measured on
# that date by two independent methods — `git log -S` over every ref, and a
# content grep across every commit — TWO entries in this list appeared in ZERO
# commits of this repository. The gate written to stop internal names reaching
# a public repository was introducing two of them, for the first time, in the
# same commit that claimed the opposite. Both are gone, and
# `roster_is_present_in_the_tree` below runs the check on every invocation so
# the next one fails instead of shipping.
#
# ⚠ AND THEY ARE NOT NAMED IN THIS COMMENT, which took a second attempt to get
# right. The first draft of this paragraph spelled both removed names out in
# order to explain the removal — publishing them in the very edit that took
# them out of the list. A finding about a disclosure is itself a disclosure;
# the detail belongs in the internal record, and here the count is enough.
#
# A name is dropped from this list rather than kept and justified. The cost is
# stated: a name removed here stops being gated at PR time, and until the
# org-wide check covers already-public repositories it is caught by review or
# not at all.
# ⚠ THE EXEMPT REGION BELOW HOLDS DATA ONLY, and these comments sit OUTSIDE it
# deliberately. An exempt region is blanked before the file is scanned, so any
# prose inside it is prose the gate cannot read — and an ordinary ticket
# reference dropped among these comments passed the whole gate. The region now
# contains the assignments that match by construction and nothing else; every
# word of explanation is scanned like any other line in this repository.
#
# The two decision-record ids reserved for fixtures and prose in this file.
# Everything else of that shape is a real record and fails the prose check.


# Derived, never spelled twice — the defect this whole file keeps finding is a
# correction landing in one copy while another keeps the old value. Each
# separator becomes a character class, so a name written with a space or an
# underscore instead of a hyphen is caught too; matching is case-insensitive,
# so a sentence-initial capital is not an evasion.
# `[-_ ]+` rather than `[-_ ]`, and `/` allowed spaces either side: a name
# written with an extra space, or broken across a line by a formatter, is the
# same disclosure as the tidy spelling.
# A SLASH IS A WRAP POINT TOO, and it needed its own rule: `[-_ ]+` cannot
# describe `shardpilot/ integrations`, because the slash must stay literal
# while the space around it must be optional. Text wraps either side of a
# slash, so both are allowed.

# The SHAPE half. These name no record, no ticket, no branch and no service, so
# they are safe to publish in the file that gates against them.

# Files the gate must not read as content: this script is the one place the
# patterns are written down, by construction.

# The by-construction material — patterns, roster, fixture corpora — lives in
# its own file so that THIS file can be scanned end to end with no exemptions.
# See scripts/gate-corpus.sh for why that separation exists.
CORPUS_PATH="$(dirname "$SELF")/gate-corpus.sh"
if [ ! -f "$CORPUS_PATH" ]; then
  echo "REFUSING: $CORPUS_PATH is missing." >&2
  echo "  It holds the class patterns and the fixture corpora; without it the" >&2
  echo "  gate would scan with an empty pattern list and report clean." >&2
  exit 2
fi
# ⚠ AND IT IS READ FROM THE INDEX, like every other tracked file. It was the
# one working-tree exception left: its grammar, its values and its audits all
# read the copy on disk while the scan skipped the staged blob by path, so
# staging a disclosure into it and restoring the disk copy left every validator
# looking at the clean version and the scan looking away.
#
# The consequence is worth stating: while editing this corpus you must `git
# add` it before this gate reflects the change. That is the same discipline as
# running CI after committing rather than before, and it is what makes a green
# run a statement about the commit rather than about the desk it was run on.
CORPUS="$(mktemp)"
CORPUS_REL="scripts/$(basename "$CORPUS_PATH")"
if ! (cd "$(dirname "$CORPUS_PATH")/.." && git cat-file blob ":$CORPUS_REL") > "$CORPUS" 2>/dev/null; then
  echo "REFUSING: the staged blob for $CORPUS_REL could not be read." >&2
  echo "  A commit would carry that blob, so it is the one worth validating;" >&2
  echo "  if the file is newly added, stage it before running this gate." >&2
  exit 2
fi
# ⚠ THE CORPUS IS NEVER SOURCED INTO THIS SHELL. It used to be, before its own
# validators ran — so a single `exit 0` appended to it ended the gate with
# status 0 and NO OUTPUT AT ALL. Not a weakened check: the whole run, silently.
# A file this script skips must not also be a file this script executes.
#
# It is read in a subshell instead, which reports back only variable VALUES via
# `declare -p`. An `exit` there kills the subshell, `declare -p` never runs, the
# dump is empty, and the required-variable check below refuses. The failure mode
# of tampering is a refusal rather than a silent success.
#
# `env -i` for the same reason the name comparison needs it: starting from a
# clean environment means an assignment to an INHERITED name — `HOME=`, say —
# shows up as newly defined instead of being invisible against the parent's
# variables. PATH is carried in so the subshell can find its utilities; it is
# excluded from the comparison by name.
CORPUS_EXPECTED_NAMES='FIXTURE_ACCENT_BODY FIXTURE_ACCENT_NAME FIXTURE_BINARY_BODY
FIXTURE_BINARY_NAME FIXTURE_CLEAN_BODY FIXTURE_CLEAN_NAME FIXTURE_DIRTY_BODY
FIXTURE_DIRTY_NAME FIXTURE_LANEB_BODY FIXTURE_LANEB_NAME FIXTURE_NAMEHIT_BODY
FIXTURE_NAMEHIT_NAME KNOWN_INNOCENT KNOWN_INTERNAL PATTERNS ROSTER'

# ⚠ AN ANSI-C FRAGMENT CAN TRUNCATE ITS OWN VALUE. `$'\0'` is a NUL, and bash
# ends the string there — so everything after it stays in the published file
# and reaches neither `declare -p`, nor the value audit, nor the self-test,
# which goes on counting the fixtures it can still see. The grammar therefore
# admits exactly one escape form inside `$'…'`: three octal digits in the byte
# range that are not 000. The range matters as much as the value — bash reduces
# an octal escape modulo 256, so `\400` is also a NUL and `^[0-7]{3}$` admitted
# it. `\x00`, `\u0000`, `\c@` and a bare `\0` are all refused without being
# named, which is the point — the spellings of NUL are not a list worth keeping.
#
# ⚠ THE GRAMMAR RUNS FIRST, BEFORE ANYTHING SOURCES THIS FILE. A subshell
# contains variables, not the filesystem: `touch`, a redirect or an `rm` in the
# corpus takes effect and only then gets refused, so the checks below were
# reading a tree the file had already been allowed to change. The grammar reads
# raw text and executes nothing, so it is the only check that can go first.
corpus_expected="$(printf '%s\n' $CORPUS_EXPECTED_NAMES | sort | tr '\n' ' ')"

# The corpus is validated by GRAMMAR, not by a list of forbidden shapes. Each
# predicate caught the variant it was written for and left the next one: a
# comment, then a function declaration (bash emits neither a definition under
# xtrace nor a name under `compgen -v`), then an assignment repeated so its
# first value is overwritten before the self-test ever reads it, then a comment
# after a `;`, then a parameter expansion whose payload never reaches a value,
# then an indexed array whose second element nothing expands. They are one
# thing — text in a file no pass reads — so the rule states what a line MAY be
# instead of what it may not, down to the characters admissible outside a
# quoted literal: name characters, `=`, `'`, and `$` only where it opens `$'…'`.
# A construct nobody has thought of yet needs a character to be written in.
if ! corpus_grammar="$(awk -v expected="$corpus_expected" '
  BEGIN {
    sq = sprintf("%c", 39); dl = sprintf("%c", 36); q = ""
    n = split(expected, a, /[ \t\n]+/)
    for (i = 1; i <= n; i++) if (a[i] != "") ok[a[i]] = 1
  }
  function scan(s,   i, c, esc) {
    i = 1
    while (i <= length(s)) {
      c = substr(s, i, 1)
      if (q == "") {
        if (c == sq) { q = sq; ansi = 0 }
        else if (c == dl && substr(s, i + 1, 1) == sq) { q = sq; ansi = 1; i++ }
        else if (c !~ /[A-Za-z0-9_=]/) {
          if (!hit) print NR ": " c " outside a quoted value -- " $0
          hit = 1
        }
      } else if (c == q) q = ""
      else if (ansi && c == "\\") {
        esc = substr(s, i + 1, 3)
        if (esc !~ /^[0-3][0-7][0-7]$/ || esc == "000") {
          if (!hit) print NR ": escape in an ANSI-C fragment is not a non-NUL octal byte -- " $0
          hit = 1
        }
        i += 3
      }
      i++
    }
  }
  !started && /^[[:space:]]*(#|$)/ { next }
  { started = 1 }
  {
    hit = 0
    if (q == "" && $0 ~ /^[[:space:]]*$/) next
    if (q == "") {
      if ($0 !~ /^[A-Za-z_][A-Za-z0-9_]*=/) {
        print NR ": not an assignment -- " $0
      } else {
        name = $0; sub(/=.*/, "", name)
        if (!(name in ok)) print NR ": unexpected name " name
        seen[name]++
        if (seen[name] > 1) print NR ": " name " is assigned again"
      }
    }
    scan($0)
  }
  END {
    if (q != "") print "end of file: a quoted value is never closed"
    for (k in ok) if (!(k in seen)) print "no assignment for " k
  }
' "$CORPUS" 2>&1)"; then
  echo "REFUSING: the corpus grammar check could not run:" >&2
  printf '  %s\n' "$corpus_grammar" >&2
  echo "  A checker that fails to start writes nothing, and nothing reads as" >&2
  echo "  clean. Its status decides, not its output." >&2
  exit 2
fi
if [ -n "$corpus_grammar" ]; then
  echo "REFUSING: $CORPUS does not parse as the assignments it is limited to:" >&2
  printf '  %s\n' "$corpus_grammar" >&2
  echo "  Every line above is text in a file the gate never scans. This file" >&2
  echo "  holds one assignment per expected name and nothing else." >&2
  exit 2
fi


corpus_defined="$(env -i PATH="$PATH" bash -c '
  before=$(compgen -v | sort)
  . "$1" >/dev/null 2>&1
  after=$(compgen -v | sort)
  comm -13 <(printf "%s\n" "$before") <(printf "%s\n" "$after")
' _ "$CORPUS" 2>/dev/null | { grep -vxE 'before|after|PATH|PIPESTATUS|_' || true; } | sort | tr '\n' ' ')"
if [ "$corpus_defined" != "$corpus_expected" ]; then
  echo "REFUSING: $CORPUS defines a different set of names than expected." >&2
  echo "  defined:  $corpus_defined" >&2
  echo "  expected: $corpus_expected" >&2
  echo "  The gate skips this file, so an unexpected assignment is unread text in" >&2
  echo "  a published repository. Adding one means naming it in the gate. An EMPTY" >&2
  echo "  list here also means the file stopped the subshell early." >&2
  exit 2
fi

corpus_ran="$(env -i PATH="$PATH" bash -c 'PS4=@; set -x; . "$1"' \
  _ "$CORPUS" 2>&1 >/dev/null | { grep '^@@' || true; })"
if [ -z "$corpus_ran" ]; then
  echo "REFUSING: $CORPUS ran no statements at all." >&2
  exit 2
fi
corpus_odd="$(printf '%s\n' "$corpus_ran" |
  { grep -vE '^@@[A-Za-z_][A-Za-z0-9_]*=' || true; })"
if [ -n "$corpus_odd" ]; then
  echo "REFUSING: $CORPUS runs statements that are not plain assignments:" >&2
  printf '  %s\n' "$corpus_odd" >&2
  echo "  Every line above is text in a file the gate never scans. A statement" >&2
  echo "  defining no variable clears the name check; a substitution nests one" >&2
  echo "  level deeper. This file holds assignments to the named variables and" >&2
  echo "  nothing else." >&2
  exit 2
fi

corpus_dump="$(env -i PATH="$PATH" bash -c '
  . "$1" >/dev/null 2>&1
  declare -p $2
' _ "$CORPUS" "$CORPUS_EXPECTED_NAMES" 2>/dev/null)"
if [ -z "$corpus_dump" ]; then
  echo "REFUSING: $CORPUS produced no values." >&2
  echo "  Its variables are read in a subshell; an empty result means the file" >&2
  echo "  ended that subshell before reporting them." >&2
  exit 2
fi
eval "$corpus_dump"

# Container signatures, in ONE list, searched for ANYWHERE in a file — which
# subsumes searching at byte zero, so there is nothing here for a second list
# to disagree with. There were two, and they did: the byte-zero check knew 7z
# and RAR while the fallback did not, and the fallback knew one bzip2 level out
# of nine. A container behind a preamble is the whole reason the fallback
# exists, so the shorter list was the one that mattered.
#
# Octal throughout: written out, several of these made this gate refuse itself.
# The PDF header joined them once a polyglot with a shell preamble was shown to
# render text without carrying it as bytes — which is why the literal spelling
# of that header no longer appears anywhere in this file, in prose or
# otherwise. `MZ` alone stays a byte-zero test: two printable characters
# searched for anywhere would refuse ordinary prose, and a DOS executable
# behind a preamble is not a shape anyone here produces.
CONTAINER_SIGS='\120\113\003\004 \120\113\005\006 \037\213\010'
CONTAINER_SIGS="$CONTAINER_SIGS"' \375\067\172\130\132 \050\265\057\375'
CONTAINER_SIGS="$CONTAINER_SIGS"' \102\132\150 \067\172\274\257'
CONTAINER_SIGS="$CONTAINER_SIGS"' \122\141\162\041\032\007 \177\105\114\106'
CONTAINER_SIGS="$CONTAINER_SIGS"' \045\120\104\106'

# Identifiers are admitted by SHAPE. A list of admissible ones was the first
# answer and it cannot fail: the same change that publishes a live record can
# add it to the list excusing it, wherever that list is kept. A shape cannot be
# edited into admitting a live record. Nothing in this organisation is numbered
# all zeros, all nines or all f's, so that is the whole rule, and no reference
# value needs to be written down here.
is_sentinel_run() {
  # ⚠ LENGTH IS PART OF THE SHAPE. A one-character run is uniformly valued by
  # definition, so a real identifier numbered 0 or 9 was vacuously a sentinel
  # in every class whose suffix may be a single digit. Three is the shortest
  # run any synthetic value here uses.
  [ "${#1}" -ge 3 ] || return 1
  case "$1" in *[!0]*) ;; *) return 0 ;; esac
  case "$1" in *[!9]*) ;; *) return 0 ;; esac
  case "$1" in *[!f]*) ;; *) return 0 ;; esac
  return 1
}

AUDIT_CLASSES='ADR-[0-9]+|GAP-[0-9]{3}|SP-[0-9]{3}|AC-[A-Z]{2}-[0-9]+'
AUDIT_CLASSES="$AUDIT_CLASSES"'|[A-Z][A-Z0-9]*(_[A-Z0-9]+)+_(ENABLED|DISABLED|MODE)'
AUDIT_CLASSES="$AUDIT_CLASSES"'|(main|master|HEAD) @ *`?[0-9a-f]{7,40}'
# ⚠ EVERY IDENTIFIER-BEARING ALTERNATIVE THE MATCHER ACCEPTS BELONGS HERE. Two
# were missing: the optional backtick in the commit form, and the review
# reference entirely. A class the matcher rewards and the audit ignores is a
# published live reference the self-test calls correct.
AUDIT_CLASSES="$AUDIT_CLASSES"'|Codex [a-z]*#[0-9]+'

roster_regex() {
  printf '%s' "$ROSTER" | sed -e 's![-_ ]![-_ ]+!g' -e 's!/! */ *!g' | paste -sd'|' -
}
ROSTER_RE="$(roster_regex)"

for required in PATTERNS ROSTER ROSTER_RE KNOWN_INTERNAL KNOWN_INNOCENT; do
  eval "value=\${$required:-}"
  if [ -z "$value" ]; then
    echo "REFUSING: $CORPUS did not define $required." >&2
    echo "  An empty pattern list matches nothing and reports every file clean." >&2
    exit 2
  fi
done

# The corpus is excluded from every search BY PATH, and the comment refusal
# above deliberately skips the header — which left this header as the one
# stretch of prose in the repository that nothing reads. Codex put
# a tracker-style identifier in it and the whole gate still exited 0 — this
# very comment had to be reworded because the gate then caught the literal.
# The exclusion exists for the assignments, whose values match the classes by
# construction; it was never meant to cover prose. So the header is scanned
# with the same classes as every other file, and it is the only part of this
# file that can be.
corpus_header="$(awk '/^[A-Za-z_][A-Za-z0-9_]*=/ { exit } { print NR ": " $0 }' "$CORPUS")"
corpus_header_hits="$( {
    printf '%s\n' "$corpus_header" | { grep -aE  -- "$PATTERNS"  || true; }
    printf '%s\n' "$corpus_header" | { grep -aiE -- "$ROSTER_RE" || true; }
  } | sort -u)"
if [ -n "$corpus_header_hits" ]; then
  echo "REFUSING: $CORPUS carries internal material in its header:" >&2
  printf '  %s\n' "$corpus_header_hits" >&2
  echo "  Nothing else reads this text. Reword it, or move what it says into" >&2
  echo "  the gate, which is scanned end to end." >&2
  exit 2
fi

# ⚠ THIS FILE IS SCANNED END TO END, WITH NO EXEMPTIONS AT ALL.
#
# It used to exempt regions of itself, marked off with comments, and every
# version of that arrangement turned out to be somewhere to hide things — six
# variants in six review rounds: an unterminated region blanked the rest of the
# file; a nested one closed early; a suffixed marker counted without
# contributing a label; a comment inside a region was unreadable; an inline
# comment likewise; and finally an ordinary data line. Each fix produced the
# next variant, because the shape was wrong rather than the implementation.
#
# The material that matches by construction now lives in scripts/gate-corpus.sh
# and is excluded by PATH — a fact about the repository, not a marker anyone can
# write into a file. This one is read like every other file here.
# ---------------------------------------------------------------------------
# scan_tree <root> — runs the REAL scan over one checkout and sets the globals
# below. Factored out so the self-test can exercise THE SCAN, not just the
# regex. The first draft self-tested the regex alone, which cannot detect a
# broken file list or a broken grep invocation — and a broken file list is the
# failure that reports a repository clean by scanning nothing.
# ---------------------------------------------------------------------------
scan_lane_a=""      # "path:line:text" per hit, newline separated
scan_lane_b_files=0
scan_lane_b_lines=0
scan_files=0

scan_tree() {
  local root="$1" f hits status line list
  scan_lane_a=""; scan_lane_b_files=0; scan_lane_b_lines=0; scan_files=0

  # `git ls-files -z` and PROCESS substitution, not a heredoc. A command
  # substitution used as a heredoc body is invisible to `set -e` and to
  # `pipefail`: if git fails, the list is empty, the loop never runs, and the
  # gate reports clean and exits 0. `-z` additionally defeats core.quotePath,
  # which C-quotes any path holding a non-ASCII byte, a newline, a backslash
  # or a quote — such a path arrives as the literal `"caf\303\251.md"`, fails
  # `[ -f ]`, and is skipped in silence.
  #
  # NOT `files=$(git ls-files -z)`: bash discards NUL bytes in a command
  # substitution, so that form loses every delimiter and `read -d ''` never
  # terminates a field. It scans zero files on a HEALTHY repository, which is
  # the same fail-open unconditionally.
  # The list is MATERIALISED with an explicit status check, not streamed from a
  # process substitution. A substitution's exit status is unreachable, so a
  # producer that emits SOME paths and then fails — an index or I/O error part
  # way through enumeration — hands over a short list that the zero-files
  # refusal below cannot see. Measured: a producer emitting one path then
  # exiting 1 yields one scanned file and a green run.
  list="$(mktemp)"
  blob="$(mktemp)"
  if ! (cd "$root" && git ls-files -z) > "$list"; then
    rm -f "$list"
    echo "REFUSING: git ls-files failed in '$root'." >&2
    echo "  A partial or absent file list is an UNSCANNED repository, and this" >&2
    echo "  gate must not report one clean." >&2
    exit 2
  fi

  while IFS= read -r -d '' f; do
    # The corpus is the one path this gate does not read: its entire content
    # matches by construction. Excluded by path rather than by a marker, so
    # nothing written INSIDE a file can extend the exemption.
    [ "$f" = "scripts/gate-corpus.sh" ] && continue
    # THE PATH ITSELF IS PUBLISHED CONTENT. An internal identifier in a file
    # NAME — a decision-record id, a ticket, a service name in a directory —
    # reaches every consumer and appears in no file's body, so scanning only
    # contents misses it entirely.
    if printf '%s\n' "$f" | grep -qE -- "$PATTERNS" \
       || printf '%s\n' "$f" | grep -qiE -- "$ROSTER_RE"; then
      scan_lane_a="${scan_lane_a}${f}:path:${f}"$'\n'
    fi
    # ⚠ EVERY CONTENT CHECK BELOW READS THE INDEX, NOT THE WORKING TREE.
    # `git ls-files` lists the index and `git commit` commits the index; the
    # tree is a third thing that merely usually agrees with both. Staging a
    # file and editing the worktree copy left this reading bytes no commit
    # would contain; staging one and deleting the copy left it reading nothing
    # at all, and that one is not hypothetical — a probe file skipped exactly
    # that way reached a public branch under a clean report. Refusing the
    # second case was the partial answer and has been deleted with this change:
    # the blob git would commit is the only thing worth scanning.
    #
    # It also removes the symlink special case. A symlink's blob IS its target
    # path, so the string the repository publishes arrives here as content,
    # without following anything.
    ls_entry="$(cd "$root" && git ls-files -s -z -- "$f" | tr -d '\000')"
    mode="${ls_entry%% *}"
    if [ "$mode" = 160000 ]; then
      echo "REFUSING: '$f' is a gitlink, so its contents are another repository." >&2
      echo "  Nothing here reads across that boundary, and a clean result would" >&2
      echo "  say nothing about what the submodule publishes." >&2
      exit 2
    fi
    if ! (cd "$root" && git cat-file blob ":$f") > "$blob" 2>/dev/null; then
      echo "REFUSING: the staged blob for '$f' could not be read." >&2
      echo "  It is listed in the index, so a commit would carry it; an" >&2
      echo "  unreadable one cannot be reported clean." >&2
      exit 2
    fi
    scan_files=$((scan_files + 1))
    if [ "$mode" = 120000 ]; then
      link="$(cat "$blob")"
      if printf '%s\n' "$link" | grep -qE -- "$PATTERNS" \
         || printf '%s\n' "$link" | grep -qiE -- "$ROSTER_RE"; then
        scan_lane_a="${scan_lane_a}${f}:link:${link}"$'\n'
      fi
      continue
    fi
    # ⚠ A COMPRESSED TRACKED FILE IS NOT SCANNED BY ANYTHING HERE, and `-a`
    # does not change that: treating a container as text reads its DEFLATE
    # stream, not its contents. So internal material inside a committed .zip,
    # .gz or .jar would pass every pass above and print as clean.
    #
    # This REFUSES rather than decompressing. Neither SDK tree tracks a single
    # compressed artifact today (measured), so a decompressor here would be
    # untested code guarding nothing, and the archive-walking machinery it
    # would need is genuinely large. A refusal costs nothing while the answer
    # is zero and turns into a deliberate decision the day it stops being zero.
    # ⚠ FOUR BYTES IS NOT THE WHOLE QUESTION, and two shapes get past it. A PDF
    # begins with its own four-character header and keeps its text in a Flate
    # stream, so it reads as text and scans as noise. A self-extracting archive begins with an executable
    # preamble — MZ or ELF — and carries the ZIP further in. Both are refused
    # by magic rather than by extension, because the extension is the part an
    # author controls.
    #
    # ⚠ AND AN ASCII RASTER CARRIES NO NUL AT ALL. Netpbm's P1 through P6 hold
    # their pixels as decimal text, so neither the NUL refusal nor a
    # compression signature sees them while the picture renders whatever it
    # renders. Two printable magic bytes, on the same footing as `MZ`.
    #
    # ⚠ A RASTER IMAGE IS A CONTAINER FOR TEXT. A screenshot rendering an
    # internal identifier holds it as pixels, so `grep -a` reads the file,
    # counts it, and reports nothing — a clean line about a file whose contents
    # were never legible. There are zero tracked images in this repository
    # today, which is the same footing the archive refusal stands on: a refusal
    # costs nothing while the answer is zero and becomes a deliberate decision
    # the day it stops being zero. Deciding then means adding OCR or an
    # explicit exception, not discovering the hole afterwards.
    case "$(od -An -tx1 -N4 "$blob" 2>/dev/null | tr -d ' \n')" in
      4d5a*|89504e47|ffd8ff*|47494638|52494646|424d*|49492a00|4d4d002a|\
      5031*|5032*|5033*|5034*|5035*|5036*)
        echo "REFUSING: '$f' begins with container magic (archive, PDF, executable" >&2
        echo "  or raster image)," >&2
        echo "  and this gate reads files as text. No pass here opens a container, so" >&2
        echo "  a clean result would say nothing about what it carries." >&2
        echo "  Remove it from the tracked tree, or extend this gate to walk containers" >&2
        echo "  deliberately." >&2
        exit 2
        ;;
    esac
    # ⚠ AND A CONTAINER CAN HIDE BEHIND ANY PREAMBLE, not just an executable
    # one: a shell script with a ZIP appended begins `#!/bin/sh` and passes
    # every magic test above. Rather than widen the magic list one preamble at
    # a time, refuse on the ZIP local-file signature ANYWHERE in the file.
    #
    # ⚠ AND NOT ONLY ZIP. This searched for the ZIP signature alone, so a gzip
    # stream appended after `#!/bin/sh` passed both tests: the preamble hid it
    # from the magic check and it is not ZIP. Every signature the magic check
    # recognises is searched for anywhere in the file now.
    #
    # Still a refusal, not a parse — the point is that a human looks at it. The
    # false-positive risk is a tracked file that happens to contain one of these
    # sequences. Three of them cannot occur in valid UTF-8 text at all; the
    # bzip2 one is four printable characters, which is why every signature here
    # is written in octal — spelled out, this gate refused itself, correctly.
    for sig in $CONTAINER_SIGS; do
      grep -qaF -- "$(printf "$sig")" "$blob" 2>/dev/null || continue
      echo "REFUSING: '$f' contains a compressed-container signature." >&2
      echo "  Something in this file is a container, whatever its first bytes say," >&2
      echo "  and no pass here reads container contents. Remove it, or extend this" >&2
      echo "  gate to walk containers deliberately." >&2
      exit 2
    done
    # ⚠ A NUL-BEARING FILE IS REFUSED, NOT SCANNED. UTF-16 holds its text as
    # ASCII interleaved with NULs, so a line-oriented ASCII pattern cannot
    # match it — the file reads, counts, and reports clean while carrying the
    # identifier in plain sight of anyone who opens it. Codex demonstrated
    # exactly that with a tracked UTF-16LE document.
    #
    # Refused rather than decoded, on the footing the container refusal already
    # stands on and for the same reason: measured today, both trees hold ZERO
    # NUL-bearing tracked files, so a decoder here would be untested code
    # guarding nothing. This covers UTF-16 and UTF-32 with or without a BOM,
    # which a BOM test would not.
    nul_bytes=$(wc -c < "$blob" | tr -d ' ')
    nul_stripped=$(LC_ALL=C tr -d '\000' < "$blob" | wc -c | tr -d ' ')
    if [ "$nul_bytes" -ne "$nul_stripped" ]; then
      echo "REFUSING: '$f' contains NUL bytes, so it is not the text this reads." >&2
      echo "  UTF-16 and UTF-32 hold ASCII interleaved with NULs and match no" >&2
      echo "  pattern here, so a clean result would say nothing about them." >&2
      echo "  Store it as UTF-8, or extend this gate to decode deliberately." >&2
      exit 2
    fi
    # -a remains for a file with high-bit bytes and no NUL, which GNU grep also
    # calls binary. It is defence behind the refusal above, not the front line.
    # -a treats a NUL-bearing file as text: GNU grep >= 3.5 otherwise prints
    # "binary file matches" to STDERR and nothing to stdout, so a hit inside a
    # committed binary reads as a clean file.
    #
    # ⚠ AND THE FIXTURE FOR THIS ONLY FAILS UNDER THE CI GREP. Dropping -a
    # here was mutation-tested twice: under GNU grep 3.11 (ubuntu-latest, what
    # CI runs) the NUL fixture correctly goes unmatched and the self-test
    # refuses; on the machine this was written on, where `grep` resolves to
    # ugrep 7.5.0, the same mutation passed green. A local green is not
    # evidence for this line — run it in the CI image.
    # -- ends option parsing: a tracked path beginning with `-` is otherwise
    # parsed as option letters.
    # The exit status is TRICHOTOMOUS and all three are handled: 0 = matched,
    # 1 = no match, >=2 = grep ERROR. Collapsing 2 into 1 (the `|| true` the
    # first draft used) turns every unreadable file into a clean one.
    # `| tr -d '\000'` and the explicit `exit`: bash prints
    # "warning: command substitution: ignored null byte in input" for every NUL
    # that reaches a substitution, once per matching binary, straight into CI's
    # log. Stripping it inside the subshell removes the noise; `exit
    # "${PIPESTATUS[0]}"` carries GREP's status out, because the status of the
    # assignment itself would be tr's and tr always succeeds.
    set +e
    # The staged blob, so a reported line number is a line number in what a
    # commit would carry. For a path whose tree copy matches its index entry —
    # every path in a fresh checkout — that is the same file.
    scan_src="$blob"
    hits="$(grep -anE -- "$PATTERNS" "$scan_src" 2>/dev/null \
      | tr -d '\000'; exit "${PIPESTATUS[0]}")"
    status=$?
    # THE ROSTER, in its own pass because it is the one class matched
    # case-insensitively: a name capitalised at the start of a sentence is the
    # same disclosure as the lower-case spelling, and folding `-i` into the
    # shape pass
    # would make `[Tt]here is` and the ALLCAPS flag class match prose they
    # were written to leave alone.
    roster_hits="$(grep -aniE -- "$ROSTER_RE" "$scan_src" 2>/dev/null \
      | tr -d '\000'; exit "${PIPESTATUS[0]}")"
    roster_status=$?
    # ⚠ WHAT THIS DOES NOT SEE, stated because a green run gets read as more
    # than it is: every pass here is LINE-ORIENTED, so a phrase broken across a
    # line break — which is what a formatter does to a long sentence — matches
    # nothing. A collapsed-whitespace pass for that existed and was removed
    # along with the rest of the machinery this file had grown, because no
    # tracked file in this repository contains such a wrap. It comes back as
    # its own change, with its own failing test, on the day one does.
    set -e
    for st in "$status" "$roster_status"; do
      if [ "$st" -ge 2 ]; then
        echo "REFUSING: grep could not read '$f' (exit $st)." >&2
        echo "  An unreadable file is an UNSCANNED file, and this gate must not" >&2
        echo "  report a repository clean on the strength of one." >&2
        exit 2
      fi
    done
    if [ "$status" -ne 0 ] && [ "$roster_status" -ne 0 ]; then
      continue
    fi
    # One corpus from here down, so the lane split below cannot see a
    # different set of hits from the one the statuses were computed over.
    # `sort -u`: a single line can match BOTH passes — a comment carrying an
    # ADR reference next to a service name — and would then be recorded twice,
    # inflating the line count by one per such line. One command, no new pass.
    [ -n "$roster_hits" ] && hits="$(printf '%s\n%s\n' "$hits" "$roster_hits" | grep -v '^$' | sort -u)"
    case "$f" in
      *.go)
        scan_lane_b_files=$((scan_lane_b_files + 1))
        while IFS= read -r line; do
          [ -n "$line" ] && scan_lane_b_lines=$((scan_lane_b_lines + 1))
        done <<< "$hits"
        ;;
      *)
        # No `sed "s|^|$f:|"`: the filename lands in the s-command's delimiter
        # position, so a `|` in a path is a syntax error that aborts the run
        # with exit 1 — the same code a genuine finding uses.
        while IFS= read -r line; do
          [ -n "$line" ] && scan_lane_a="${scan_lane_a}${f}:${line}"$'\n'
        done <<< "$hits"
        ;;
    esac
  done < "$list"
  rm -f "$list"
}

# ---------------------------------------------------------------------------
# SELF-TEST — proves the MATCHER and THE SCAN, on a throwaway git repository.
# A clean result means nothing until the thing producing it has been shown to
# fail, and "the patterns compile" is not that demonstration.
# ---------------------------------------------------------------------------
# ⚠ EVERY IDENTIFIER BELOW IS SYNTHETIC. These fixtures exercise SHAPES, and a
# shape is exercised just as well by a reserved synthetic id as by a real
# decision-record number — while a real one would make this fixture block the same kind of
# roster the ROSTER rule above exists to bound. The earlier version used a live
# ADR id, a live feature-flag name and a live ticket number, none of which the
# test needed.

# roster_is_present_in_the_tree — the rule from the ROSTER block, executed.
#
# Every literal in ROSTER must still appear in this repository's TRACKED TREE
# OUTSIDE this file. Two different failures are covered by that one condition:
#
#   a name that was never here    — the gate would be the first thing to
#                                   publish it (found twice: two internal
#                                   repository names with zero commits here);
#   a name that is no longer here — the cleanup removed it everywhere else and
#                                   the gate became its SOLE carrier, so every
#                                   future release archive ships a name the
#                                   cleanup existed to remove.
#
# The earlier version of this rule asked whether published HISTORY carried the
# literal, which is a weaker question: history keeps everything, so a name
# stayed admissible forever once it had ever appeared. It passed a tree in
# which six of eight roster entries existed nowhere but here.
#
# ⚠ THIS FILE IS EXCLUDED FROM THE SEARCH, and without that the check is
# self-satisfying: the roster itself would count as the occurrence.
#
# The cost, stated rather than implied: a name removed from ROSTER stops being
# gated at PR time in this repository. Its coverage is the org-wide
# publication check, which reads the class library rather than a roster and
# therefore needs no literal in a public file.
# Both halves of the gate, excluded from every presence search as one list.
# The rule asks whether a literal survives ELSEWHERE in the tree; a file that
# exists to hold those literals cannot be part of the answer.
GATE_EXCLUDES=":(exclude)scripts/check_public_surface.sh :(exclude)scripts/gate-corpus.sh"

roster_is_present_in_the_tree() {
  local lit novel=0 found
  found="$(mktemp)"
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    git grep -l -F -- "$lit" -- . $GATE_EXCLUDES > "$found" 2>/dev/null || :
    if [ ! -s "$found" ]; then
      printf 'ROSTER VIOLATION: %s appears nowhere in this tree except this file.\n' "$lit" >&2
      novel=$((novel + 1))
    fi
  done <<EOF
$ROSTER
EOF

  # ⚠ THESE TWO PASSES NOW COVER THE EXEMPT REGIONS, and only those. The main
  # The loaded values, read by every audit below. ⚠ ASSIGNED BEFORE THE FIRST
  # OF THEM: this sat between two audits once, so the earlier one interpolated
  # an unset variable, found nothing extra, and looked exactly like a working
  # fix. Its own probe is what caught it.
  corpus_values="$(for v in $CORPUS_EXPECTED_NAMES; do
    eval "printf '%s\n' \"\${$v:-}\""
  done)"

  # ⚠ THE EXTRACTOR TAKES THE WHOLE TOKEN. It read `[a-z][a-z-]*`, so a name
  # carrying a digit was truncated at the digit and the SHORTER prefix — which
  # exists everywhere in this tree — was what got checked. A repository name
  # ending in a digit passed while the literal stayed published.
  # ⚠ THE PATTERN LIST HOLDS SHAPES, AND THAT IS NOW EXECUTED RATHER THAN
  # ASSERTED. A bare alternative — a name with no metacharacter in it — is a
  # roster entry hiding in the one variable no rule covered: the corpus is
  # excluded by path, the grammar sees a well-formed assignment, and the
  # identifier and repository audits look for their own classes, not for
  # arbitrary words. Adding a literal internal name as an alternative made this
  # file its sole publisher and everything stayed green.
  #
  # Top-level alternatives are split on `|` outside brackets and groups, and
  # each must carry a metacharacter that makes it a SHAPE — a character class,
  # a quantifier or an escape. The disguises are REDUCED first, because each
  # spelling of the same trick got past the previous version: parentheses with
  # nothing to branch between are grouping and come off, and a one-character
  # class is one character. Both reductions were written after a costume got
  # through, which is why the rule below does not rely on this one alone.
  #
  # Names belong in the roster, which is checked against the tree.
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    printf 'PROSE VIOLATION: the pattern list holds a bare literal alternative: %s\n' "$lit" >&2
    novel=$((novel + 1))
  done <<EOF
$(printf '%s' "$PATTERNS" | awk '
  function check(a,   r) {
    if (a == "") return
    r = a
    if (index(r, "|") == 0) gsub(/[()]/, "", r)
    gsub(/\[.\]/, "c", r)
    if (r ~ /[][{}+*?\\|]/) return
    print a
  }
  {
    depth = 0; inbr = 0; alt = ""
    for (i = 1; i <= length($0); i++) {
      c = substr($0, i, 1)
      if (c == "\\") { alt = alt c substr($0, i + 1, 1); i++; continue }
      if (inbr) { if (c == "]") inbr = 0; alt = alt c; continue }
      if (c == "[") { inbr = 1; alt = alt c; continue }
      if (c == "(") { depth++; alt = alt c; continue }
      if (c == ")") { depth--; alt = alt c; continue }
      if (c == "|" && depth == 0) { check(alt); alt = ""; continue }
      alt = alt c
    }
    check(alt)
  }')
EOF

  # ⚠ AND THE SAME QUESTION ASKED OF THE CHARACTERS, NOT THE STRUCTURE. Every
  # hyphenated literal run in the pattern list must exist elsewhere in this
  # tree, exactly as a roster entry must. `[p]rivate-daemon` reduces to a
  # hyphenated run whatever regex syntax surrounds it, so this holds where a
  # structural test can be dressed around. Character classes are removed first
  # so a class's own contents are not read as a name. Measured when written:
  # both pattern lists contain zero such runs, so this costs nothing today and
  # refuses the day one appears.
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    git grep -l -F -- "$lit" -- . $GATE_EXCLUDES > "$found" 2>/dev/null || :
    if [ ! -s "$found" ]; then
      printf 'PROSE VIOLATION: the pattern list carries the literal %s, which is nowhere else in this tree.\n' "$lit" >&2
      novel=$((novel + 1))
    fi
  done <<EOF
$(printf '%s' "$PATTERNS" | sed 's/\[[^]]*\]//g' \
  | grep -oE '[A-Za-z][A-Za-z0-9]*(-[A-Za-z0-9]+)+' | sort -u)
EOF

  # scan reads this file's prose like any other file's. What no pass reads is
  # the corpus, excluded by path — so these checks read IT as well as this
  # file. Both of this gate's own past disclosures were of exactly this kind:
  # two internal repository names in the roster, and a live decision-record id
  # among the fixtures. A header comment naming a repository that exists
  # nowhere else in the tree was demonstrated to pass everything else.
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    git grep -l -F -- "$lit" -- . $GATE_EXCLUDES > "$found" 2>/dev/null || :
    if [ ! -s "$found" ]; then
      printf 'PROSE VIOLATION: %s is written in this file and appears nowhere else in this tree.\n' "$lit" >&2
      novel=$((novel + 1))
    fi
  done <<EOF
$( { grep -hoE 'shardpilot/[A-Za-z0-9][A-Za-z0-9._-]*' "$SELF" "$CORPUS"
     printf '%s\n' "$corpus_values" | grep -oE 'shardpilot/[A-Za-z0-9][A-Za-z0-9._-]*'; } | sort -u )
EOF

  # Identifiers, in every class the patterns name, admitted by shape alone. A
  # fixture is exactly where a live id gets pasted — someone extending the
  # known-internal corpus reaches for whatever they were looking at.
  #
  # ⚠ READ OVER THE VALUES AS WELL AS THE RAW TEXT. A literal split across
  # adjacent quotes is invisible to a grep of the file and perfectly legible to
  # everyone reading the published fixture, so the loaded values are searched
  # too and the two results are merged.
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    case "$lit" in EXAMPLE_*) continue ;; esac
    run="$(printf '%s' "$lit" | tr -d '\140')"
    is_sentinel_run "${run##*[!0-9a-f]}" && continue
    printf 'PROSE VIOLATION: %s is a live identifier written where nothing scans.\n' "$lit" >&2
    novel=$((novel + 1))
  done <<EOF
$( { grep -hoE -- "$AUDIT_CLASSES" "$SELF" "$CORPUS"
     printf '%s\n' "$corpus_values" | grep -oE -- "$AUDIT_CLASSES"; } | sort -u )
EOF

  rm -f "$found"
  if [ "$novel" -ne 0 ]; then
    echo "REFUSING: $novel literal(s) in this file exist nowhere else in this tree." >&2
    echo "  A gate against publishing internal names must not be the only thing" >&2
    echo "  publishing them — every release archive carries this file. Remove them" >&2
    echo "  from ROSTER and say so; the org-wide check covers what leaves here." >&2
    exit 2
  fi
  echo "roster rule: OK — all $(printf '%s\n' "$ROSTER" | grep -c .) literal(s) still present elsewhere in this tree"
}

selftest() {
  local line misses=0 falses=0 tested=0 innocent=0 tmp scanned_a
  local fixname fixval fixseen=
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    tested=$((tested + 1))
    printf '%s\n' "$line" | grep -qE -- "$PATTERNS" || {
      printf 'SELFTEST MISS: %s\n' "$line" >&2; misses=$((misses + 1)); }
  done <<EOF
$KNOWN_INTERNAL
EOF
  # THE ROSTER HALF, with its fixtures DERIVED from ROSTER rather than written
  # out again. A hand-written copy is a second spelling, and the failure it
  # produces is the quiet one: drop a name from ROSTER and a stale fixture goes
  # on passing against a pattern nothing scans with any more.
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    tested=$((tested + 1))
    printf 'a sentence mentioning %s in passing\n' "$line" | grep -qiE -- "$ROSTER_RE" || {
      printf 'SELFTEST MISS (roster): %s\n' "$line" >&2; misses=$((misses + 1)); }
  done <<EOF
$ROSTER
EOF
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    innocent=$((innocent + 1))
    # The SAME exclusion the scan applies, so the two cannot drift.
    # BOTH passes, because an innocent line is only innocent if NEITHER fires.
    #
    # ⚠ EVALUATED SEPARATELY, NOT PIPED. The previous form ran both greps into
    # a group and piped it to `grep -q .`. Under `set -o pipefail` the group's
    # status is the LAST command's — the roster grep — so an innocent line that
    # matched $PATTERNS but not $ROSTER_RE made the pipeline non-zero and the
    # `if` FALSE. That is the ordinary shape of a shape-class false positive,
    # which means this half of the self-test could not detect the thing it
    # exists to detect: every innocent fixture was passing vacuously.
    #
    # Third time this exact inversion has appeared in this file's history. A
    # herestring avoids it structurally — no pipeline, so no status to invert.
    fp=0
    grep -qE  -- "$PATTERNS"  <<<"$line" && fp=1
    grep -qiE -- "$ROSTER_RE" <<<"$line" && fp=1
    if [ "$fp" -eq 1 ]; then
      printf 'SELFTEST FALSE POSITIVE: %s\n' "$line" >&2; falses=$((falses + 1))
    fi
  done <<EOF
$KNOWN_INNOCENT
EOF
  if [ "$misses" -ne 0 ] || [ "$falses" -ne 0 ]; then
    echo "REFUSING: the pattern list failed its own self-test ($misses miss(es), $falses false positive(s))." >&2
    echo "  A scan that cannot match known-internal strings would report this" >&2
    echo "  repository clean by finding nothing, and print the same line as a pass." >&2
    exit 2
  fi

  # THE SCAN ITSELF, over a fixture whose expected answer is known. This is the
  # half the first draft did not have: the regex can be perfect while the file
  # list is empty, and only this catches that.
  # What the fixture files below are for, kept OUTSIDE the region because a
  # region is blanked before scanning and prose inside one is prose the gate
  # cannot read:
  #   the notes file    nothing internal in its BODY — the hit is the NAME,
  #                     which is why the path itself is scanned
  #   the accented file C-quoted by git ls-files, so it proves -z is honoured
  #   the binary file   carries a NUL, and is scanned in a tree of its own
  #                     because it must REFUSE rather than report
  #
  # The filenames are described rather than written: this prose is scanned like
  # any other, and naming the fixtures here would put their identifiers into a
  # part of the file the gate reads.
  # Every name and body comes from the corpus file. This block used to carry
  # them inline and needed its own exemption; with the literals gone it is
  # ordinary code and is scanned like the rest of this file.
  # ⚠ THESE NAMES BECOME REDIRECTION TARGETS. A fixture name carrying a slash
  # — or an absolute path — writes outside the temporary repository, into the
  # real workspace, before anything is scanned. Verified: an absolute name
  # overwrote a staged disclosure with clean prose, after which the self-test
  # reported 6/6 and the gate exited 0 while the index still carried it.
  for fixname in $CORPUS_EXPECTED_NAMES; do
    case "$fixname" in *_NAME) ;; *) continue ;; esac
    eval "fixval=\${$fixname:-}"
    case "$fixval" in
      ''|.|..|*/*|-*)
        echo "REFUSING: $CORPUS gives $fixname a name that is not a plain basename." >&2
        echo "  The self-test writes these; a path component would put a fixture" >&2
        echo "  outside the temporary repository and into the real workspace." >&2
        exit 2 ;;
    esac
    if [ "$(printf '%s\n' "$fixseen" | grep -cxF -- "$fixval")" -ne 0 ]; then
      echo "REFUSING: $CORPUS reuses the fixture name $fixval." >&2
      echo "  Two fixtures writing one path leave the self-test asserting over" >&2
      echo "  whichever was written last." >&2
      exit 2
    fi
    fixseen="$fixseen$fixval"$'\n'
  done

  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' RETURN
  (
    cd "$tmp"
    git init -q .
    git config user.email t@t; git config user.name t
    printf '%s\n' "$FIXTURE_CLEAN_BODY"   > "$FIXTURE_CLEAN_NAME"
    printf '%s\n' "$FIXTURE_NAMEHIT_BODY" > "$FIXTURE_NAMEHIT_NAME"
    printf '%s\n' "$FIXTURE_DIRTY_BODY"   > "$FIXTURE_DIRTY_NAME"
    printf '%s\n' "$FIXTURE_LANEB_BODY"   > "$FIXTURE_LANEB_NAME"
    printf '%s\n' "$FIXTURE_ACCENT_BODY"  > "$FIXTURE_ACCENT_NAME"
    git add -A >/dev/null 2>&1
  )
  scan_tree "$tmp"
  scanned_a="$scan_lane_a"
  rm -rf "$tmp"; trap - RETURN

  local fixture_fail=0
  printf '%s' "$scanned_a" | grep -q '^dirty\.md:' || {
    echo "SELFTEST: the scan missed dirty.md" >&2; fixture_fail=1; }
  printf '%s' "$scanned_a" | grep -q 'caf' || {
    echo "SELFTEST: the scan missed the non-ASCII path (core.quotePath)" >&2; fixture_fail=1; }
  # The NUL fixture gets its own tree: a refusal ends the run it happens in,
  # so it cannot sit beside the fixtures whose results are read afterwards. The
  # scan runs in a subshell precisely so its exit status can be read.
  nul_tmp="$(mktemp -d)"
  (
    cd "$nul_tmp"
    git init -q .
    git config user.email t@t; git config user.name t
    printf 'x\000%s\n' "$FIXTURE_BINARY_BODY" > "$FIXTURE_BINARY_NAME"
    git add -A >/dev/null 2>&1
  )
  # `|| nul_status=$?` rather than a bare call: under `set -e` the failing
  # subshell ends the whole run before its status can be read, which killed
  # this gate with no output at all the first time it was written.
  nul_status=0
  ( scan_tree "$nul_tmp" ) >/dev/null 2>&1 || nul_status=$?
  [ "$nul_status" -eq 2 ] || {
    echo "SELFTEST: a NUL-bearing tracked file was not refused" >&2; fixture_fail=1; }
  rm -rf "$nul_tmp"
  printf '%s' "$scanned_a" | grep -qF -- "$FIXTURE_NAMEHIT_NAME:path:" || {
    echo "SELFTEST: the scan missed an internal identifier in a PATH NAME" >&2; fixture_fail=1; }
  printf '%s' "$scanned_a" | grep -q '^clean\.md:' && {
    echo "SELFTEST: the scan flagged clean.md" >&2; fixture_fail=1; }
  [ "$scan_lane_b_files" -eq 1 ] || {
    echo "SELFTEST: lane B counted $scan_lane_b_files files, expected 1" >&2; fixture_fail=1; }
  if [ "$fixture_fail" -ne 0 ]; then
    echo "REFUSING: the scan failed its own fixture." >&2
    exit 2
  fi
  echo "self-test: OK — $tested known-internal string(s) matched, $innocent innocent string(s) passed, scan fixture 6/6"
}

roster_is_present_in_the_tree
selftest

# ---------------------------------------------------------------------------
# THE REAL SCAN
# ---------------------------------------------------------------------------
scan_tree "$PWD"

# An empty file list is the shape of every fail-open above, and it is the ONE
# symptom they all share — so it is checked directly rather than only through
# the exit status of the command that produced it.
if [ "$scan_files" -eq 0 ]; then
  echo "REFUSING: the scan processed zero files." >&2
  echo "  A real checkout is never empty, so this means git ls-files failed or" >&2
  echo "  this is not a checkout. Reporting 'clean' here would be a gate that" >&2
  echo "  passes by looking at nothing." >&2
  exit 2
fi

echo
echo "LANE B (REPORTED, NOT GATED) — Go source: ${scan_lane_b_files} file(s) WITH A MATCH, ${scan_lane_b_lines} matching line(s). Not a count of Go files scanned: a clean one is not counted here."
if [ "$scan_lane_b_files" -eq 0 ]; then
  echo "  LANE B IS EMPTY. The debt this lane tracked is paid: fold *.go into lane A"
  echo "  and delete this section, so the scope note stops describing a gap that"
  echo "  no longer exists."
else
  echo "  Doc comments here publish verbatim to pkg.go.dev. They are owed work, not"
  echo "  accepted risk, and they are not gated HERE because this repository's Go"
  echo "  sources are owned by the SDK wire-freeze workstream. The count is lines"
  echo "  MATCHING THE PATTERNS ABOVE — not a total of internal material, which the"
  echo "  scope note above says these shapes cannot bound."
fi

echo
if [ -n "$scan_lane_a" ]; then
  echo "FAIL — internal material in the published non-source surface:" >&2
  printf '%s' "$scan_lane_a" >&2
  exit 1
fi
echo "LANE A (GATED) — clean. ${scan_files} tracked file(s) were read; a run that scanned none refuses above rather than reporting this line."
