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

# gate-self-exempt:begin definitions
RESERVED_ADR_IDS='ADR-0000 ADR-0999'
ROSTER='analytics-service
control-plane'
roster_regex() {
  printf '%s' "$ROSTER" | sed -e 's![-_ ]![-_ ]+!g' -e 's!/! */ *!g' | paste -sd'|' -
}
ROSTER_RE="$(roster_regex)"
PATTERNS='ADR-[0-9]+|§[0-9]|[Tt]here (is|are) [Nn][Oo] [A-Za-z][A-Za-z-]*( [A-Za-z-]+){0,2} (harness|harnesses|coverage|tests?|suites?)|(is|are|was|were)(n.{1,3}t| not) (tested|covered|scanned|audited|monitored)|(is|are|was|were|remains?) (largely |entirely |still |completely |mostly )?(untested|unmonitored|unaudited|unscanned)|[Nn]o( [A-Za-z][A-Za-z-]*){0,3} (tests?|coverage|scanning|monitoring|harness|harnesses|suites?)( (for|of|in)|[.,;]|$)|[Nn]obody (looks|checks|monitors)|[Ll]acks( any| automated| an?)* ?[A-Za-z-]*[ ]?(harness|harnesses|coverage|tests?|suites?|monitoring)|(does|do|did)( not|n.{1,3}t) have( any| automated| an?)*( [A-Za-z][A-Za-z-]*){0,2} (harness|harnesses|coverage|tests?|suites?|monitoring)|GAP-[0-9]{3}|\bSP-[0-9]{3}\b|\bAC-[A-Z]{2}-[0-9]+|Codex (review|#|[a-z]+#)|[A-Z][A-Z0-9]*(_[A-Z0-9]+)+_(ENABLED|DISABLED|MODE)|\b(main|master|HEAD) @ *`?[0-9a-f]{7,40}'
# gate-self-exempt:end

# ⚠ THIS FILE IS SCANNED TOO, with only two regions removed first.
#
# It used to exempt itself entirely, and that exemption is where every one of
# this file's own disclosures hid: a paragraph naming two internal repositories
# while announcing their removal, a live decision-record id in a fixture, and a
# published statement of where our testing does not reach. Each was found by a
# reviewer rather than by the gate, because the gate could not read its own
# prose. Two bespoke checks were bolted on for two of those shapes, and the
# rest stayed unreadable — a comment here carrying an ordinary ticket
# reference passed the whole gate.
#
# So the exemption is now the SMALLEST thing that cannot be scanned: the
# pattern definitions and the synthetic fixture corpus, both delimited by
# `gate-self-exempt` markers below. They match by construction; every other
# line of this file is ordinary published prose and is treated as such. That
# also retires the two bespoke passes, because the real classes now cover
# what they covered.
self_scan_body() {
  local begins ends kept total
  # ⚠ ONE ANCHOR, USED BY BOTH THE COUNT AND THE STRIPPER. They were written
  # separately and disagreed: the count accepted a marker with leading
  # whitespace, awk demanded column zero. An indented marker therefore counted
  # as balanced while the stripper never saw it — the same silent over-blanking
  # this check was added to catch, reintroduced by the check itself.
  # ⚠ ONE MARKER GRAMMAR, USED BY ALL THREE READERS. Every hole this mechanism
  # has had came from readers disagreeing about what a marker IS. The counters
  # and the stripper accepted any line STARTING with the token, while the label
  # extractor required a space and a label after it — so a block written
  # `:beginX` / `:endX` counted, was blanked by the stripper, and contributed no
  # label, leaving the label set looking exactly right while a fourth region
  # quietly swallowed whatever it wrapped.
  #
  # The grammar is now exact and shared: a begin marker is the token, one space
  # and a lower-case label to end of line; an end marker is the token and
  # nothing else. Anything that merely resembles one is not a marker at all —
  # it is an ordinary comment, which means it is SCANNED rather than trusted.
  BEGIN_RE='^[[:space:]]*# gate-self-exempt:begin [a-z][a-z ]*$'
  END_RE='^[[:space:]]*# gate-self-exempt:end$'
  begins="$(grep -cE "$BEGIN_RE" "$1" || true)"
  ends="$(grep -cE "$END_RE" "$1" || true)"
  # ⚠ BALANCED, AND CHECKED — because an unterminated block fails SILENTLY and
  # generously. `skip` never resets, every line after the opener is blanked,
  # and the scan reports a file it did not read as clean. Both blocks were
  # unterminated when this function was introduced (one marker concatenated
  # onto a comment, the other never written), 117 of 609 lines survived, and a
  # mutation test still passed because the planted line sat above the first
  # marker. A guard whose failure mode is "sees less" must say so.
  # `|| true`, like the two counts above. A no-match grep exits 1, and whether
  # that aborts under `set -euo pipefail` before the explicit check below can
  # run depends on how bash treats a failing substitution in an assignment —
  # empirically it does not here, but a refusal that depends on that reading is
  # a refusal resting on a subtlety. The explicit check is meant to be the
  # thing that reports; this makes sure it is the thing that runs.
  labels="$(grep -oE "$BEGIN_RE" "$1" \
    | sed -E 's/.*:begin //' | sort | tr '\n' ',' || true)"
  if [ "$labels" != "definitions,fixtures,scan fixture," ]; then
    echo "REFUSING: $1 has exempt regions [$labels], expected exactly" >&2
    echo "  [definitions,fixtures,scan fixture,]. Balanced and non-nested was not" >&2
    echo "  enough — any number of well-formed blocks passed, so a new one could" >&2
    echo "  blank arbitrary prose silently. Adding a region means naming it here." >&2
    exit 2
  fi
  if [ "$begins" != "$ends" ] || [ "$begins" -eq 0 ]; then
    echo "REFUSING: $1 has $begins gate-self-exempt:begin marker(s) and $ends end marker(s)." >&2
    echo "  An unterminated block blanks the rest of the file and the scan then" >&2
    echo "  reports what it never read as clean. Markers must be balanced and" >&2
    echo "  each must start its own line." >&2
    exit 2
  fi
  # ⚠ NO PROSE INSIDE AN EXEMPT REGION. Shrinking the regions to data reduced
  # this surface; it did not close it, because a comment inserted among the
  # assignments is still blanked and therefore still unreadable. A region
  # exists to hold text that matches BY CONSTRUCTION — assignments and fixture
  # corpora — and a sentence is never that. INLINE comments count: the whole
  # line is blanked either way, so a sentence appended to the end of a fixture
  # line is exactly as unreadable as one on its own line. Refusing both makes
  # "hide prose in the exemption" impossible rather than merely unlikely, and
  # it costs one pass.
  if awk -v b="$BEGIN_RE" -v e="$END_RE" '
    $0 ~ b { inside = 1; next }
    $0 ~ e { inside = 0; next }
    inside && $0 ~ /^[[:space:]]*#|[[:space:]]#/ { print NR ": " $0; found = 1 }
    END { exit !found }
  ' "$1"; then
    echo "REFUSING: $1 has comment lines inside a gate-self-exempt region (above)." >&2
    echo "  Those lines are blanked before scanning, so they are prose the gate" >&2
    echo "  cannot read. A region holds text that matches by construction; move" >&2
    echo "  the explanation outside the markers, where it is scanned like any" >&2
    echo "  other line." >&2
    exit 2
  fi

  if ! awk -v b="$BEGIN_RE" -v e="$END_RE" '
    $0 ~ b {
      depth++
      if (depth > 1) { print "nested exempt region at line " NR > "/dev/stderr"; exit 3 }
    }
    { if (!depth) print; else print "" }
    $0 ~ e {
      depth--
      if (depth < 0) { print "unopened end marker at line " NR > "/dev/stderr"; exit 3 }
    }
    END { if (depth != 0) { print "unterminated exempt region" > "/dev/stderr"; exit 3 } }
  ' "$1" > "$2"; then
    echo "REFUSING: $1 has a malformed gate-self-exempt region (see above)." >&2
    echo "  Nesting is refused rather than supported: one exempt region has no" >&2
    echo "  reason to contain another, and the stripper closing on the first end" >&2
    echo "  marker would blank prose after it while the counts still balanced." >&2
    exit 2
  fi
  # And a floor on how much survives. Balanced markers can still swallow the
  # file if a block is opened early and closed late; this is cheap and catches
  # that without pretending to know the right number.
  total="$(grep -c '' "$1")"
  kept="$(grep -c . "$2" || true)"
  if [ "$kept" -lt $(( total / 4 )) ]; then
    echo "REFUSING: stripping the exempt regions of $1 left $kept non-blank lines of $total." >&2
    echo "  That is not an exemption, it is the file disappearing." >&2
    exit 2
  fi
}

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
  if ! (cd "$root" && git ls-files -z) > "$list"; then
    rm -f "$list"
    echo "REFUSING: git ls-files failed in '$root'." >&2
    echo "  A partial or absent file list is an UNSCANNED repository, and this" >&2
    echo "  gate must not report one clean." >&2
    exit 2
  fi

  while IFS= read -r -d '' f; do
    # THE PATH ITSELF IS PUBLISHED CONTENT. An internal identifier in a file
    # NAME — a decision-record id, a ticket, a service name in a directory —
    # reaches every consumer and appears in no file's body, so scanning only
    # contents misses it entirely.
    if printf '%s\n' "$f" | grep -qE -- "$PATTERNS" \
       || printf '%s\n' "$f" | grep -qiE -- "$ROSTER_RE"; then
      scan_lane_a="${scan_lane_a}${f}:path:${f}"$'\n'
    fi
    [ -f "$root/$f" ] || continue
    scan_files=$((scan_files + 1))
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
    # begins `%PDF` and keeps its text in a Flate stream, so it reads as text
    # and scans as noise. A self-extracting archive begins with an executable
    # preamble — MZ or ELF — and carries the ZIP further in. Both are refused
    # by magic rather than by extension, because the extension is the part an
    # author controls.
    case "$(od -An -tx1 -N4 "$root/$f" 2>/dev/null | tr -d ' \n')" in
      1f8b*|504b0304|504b0506|fd377a58|425a68*|28b52ffd|25504446|4d5a*|7f454c46|377abcaf|52617221)
        echo "REFUSING: '$f' begins with container magic (archive, PDF or executable)," >&2
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
    # Still a refusal, not a parse — the point is that a human looks at it. The
    # false-positive risk is a tracked file that happens to contain those four
    # bytes, which is a thing worth looking at in a text-only tree anyway.
    if grep -qa -- "$(printf 'PK\003\004')" "$root/$f" 2>/dev/null; then
      echo "REFUSING: '$f' contains a ZIP local-file signature." >&2
      echo "  Something in this file is a container, whatever its first bytes say," >&2
      echo "  and no pass here reads container contents. Remove it, or extend this" >&2
      echo "  gate to walk containers deliberately." >&2
      exit 2
    fi
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
    # This file is read with its definition and fixture regions blanked out;
    # every other file is read as-is. `self_scan_body` preserves line numbering
    # by emitting an empty line per removed line, so reported line numbers stay
    # true to the file on disk.
    if [ "$f" = "scripts/check_public_surface.sh" ]; then
      scan_src="$(mktemp)"; self_scan_body "$root/$f" "$scan_src"
    else
      scan_src="$root/$f"
    fi
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
    [ "$scan_src" = "$root/$f" ] || rm -f "$scan_src"
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
# gate-self-exempt:begin fixtures
KNOWN_INTERNAL='per ADR-0000 §3
There are no Playwright tests for the console.
There are no end-to-end tests for the purchase flow.
The console has no end-to-end tests for purchase callbacks.
The console does not have automated tests.
The console has no tests.
The crash path is untested.
There is NO Playwright harness in the console repo
the crash path is not covered by automated tests
no automated scanning for that class of input
a bare §7c left behind when a record id was stripped
tracked as GAP-000 internally
pinned to main @ 0000000
nobody looks at that dashboard
tracked as SP-123 in the internal board
filed as AC-QA-7 during triage
the console lacks automated tests
(Codex go#48 round 3)
EXAMPLE_SYNTHETIC_FLAG_MODE=off'
KNOWN_INTERNAL="$KNOWN_INTERNAL
The crash path isn$(printf '\047')t tested.
The console doesn$(printf '\342\200\231')t have automated tests."

KNOWN_INNOCENT='go get github.com/shardpilot/shardpilot-go@v0.6.0-alpha
IngestURL: os.Getenv("SHARDPILOT_INGEST_URL")
POST {IngestURL}/v1/events:batch
https://localhost:8080 during local development
a documented per-platform adaptation, not drift
DEFOLD_SHA1="f735c12192bf95684e6ae1ae27c400b8170fc6d8"
a self-service signup flow, a micro-service boundary
the event plane and the consent plane are separate
an analytics-plane request, zero event batches'
# gate-self-exempt:end

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
roster_is_present_in_the_tree() {
  local lit novel=0 found
  found="$(mktemp)"
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    git grep -l -F -- "$lit" -- . ':(exclude)scripts/check_public_surface.sh' > "$found" 2>/dev/null || :
    if [ ! -s "$found" ]; then
      printf 'ROSTER VIOLATION: %s appears nowhere in this tree except this file.\n' "$lit" >&2
      novel=$((novel + 1))
    fi
  done <<EOF
$ROSTER
EOF

  # ⚠ THESE TWO PASSES NOW COVER THE EXEMPT REGIONS, and only those. The main
  # scan reads this file's prose like any other file's; what it cannot read is
  # the pattern-definition and fixture blocks, which match by construction and
  # are blanked out before scanning. So these are not a duplicate of the main
  # scan — they are the part of this file the main scan is blind to, and both
  # of this file's own past disclosures lived exactly there: two internal
  # repository names in the ROSTER block, and a live decision-record id among
  # the fixtures.
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    git grep -l -F -- "$lit" -- . ':(exclude)scripts/check_public_surface.sh' > "$found" 2>/dev/null || :
    if [ ! -s "$found" ]; then
      printf 'PROSE VIOLATION: %s is written in this file and appears nowhere else in this tree.\n' "$lit" >&2
      novel=$((novel + 1))
    fi
  done <<EOF
$(grep -oE 'shardpilot/[a-z][a-z-]*' "$SELF" | sort -u)
EOF

  # Decision-record ids: only the two reserved synthetic ones are admissible.
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    reserved=no
    for ok in $RESERVED_ADR_IDS; do [ "$lit" = "$ok" ] && reserved=yes; done
    [ "$reserved" = yes ] && continue
    printf 'PROSE VIOLATION: %s is a real decision-record id written in this file.\n' "$lit" >&2
    novel=$((novel + 1))
  done <<EOF
$(grep -oE 'ADR-[0-9]+' "$SELF" | sort -u)
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
  #   the binary file   carries a NUL, so grep calls it binary without -a
  #
  # The filenames are described rather than written: this prose is scanned like
  # any other, and naming the fixtures here would put their identifiers into a
  # part of the file the gate reads.
  # gate-self-exempt:begin scan fixture
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' RETURN
  (
    cd "$tmp"
    git init -q .
    git config user.email t@t; git config user.name t
    printf 'clean customer prose\n' > clean.md
    printf 'nothing internal in the body\n' > ADR-0999-notes.md
    printf 'see ADR-0000 for context\n' > dirty.md
    printf '// GAP-000 note\npackage x\n' > lane_b.go
    printf 'internal: control-plane\n' > "café.md"
    printf 'x\0see ADR-0999 here\n' > binary.bin
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
  printf '%s' "$scanned_a" | grep -q '^binary\.bin:' || {
    echo "SELFTEST: the scan missed the NUL-bearing file (grep -a)" >&2; fixture_fail=1; }
  printf '%s' "$scanned_a" | grep -q '^ADR-0999-notes\.md:path:' || {
    echo "SELFTEST: the scan missed an internal identifier in a PATH NAME" >&2; fixture_fail=1; }
  printf '%s' "$scanned_a" | grep -q '^clean\.md:' && {
    echo "SELFTEST: the scan flagged clean.md" >&2; fixture_fail=1; }
  [ "$scan_lane_b_files" -eq 1 ] || {
    echo "SELFTEST: lane B counted $scan_lane_b_files files, expected 1" >&2; fixture_fail=1; }
  # gate-self-exempt:end
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
