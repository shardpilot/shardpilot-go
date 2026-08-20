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
# NEGATIVE STATEMENTS ABOUT COVERAGE. "There is NO Playwright harness in the
# console repo" names no service, no host and no credential, and it was the
# single most valuable line in the material this gate was built after. It is
# a published map of where our testing does not reach — an outsider does not
# need a secret if they are told where nobody is looking.
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
# literals that remain are ones this repository's published history already
# carries, so the file discloses nothing that a `git log` does not.
#
# THREE SHAPES WERE TRIED AND WITHDRAWN, because a gate that cries wolf gets
# silenced, and a silenced gate is the one that misses the real thing:
#   `-plane`     — this SDK's own vocabulary says event plane, consent plane,
#                  analytics plane. Narrowed to `control-plane`, the one that
#                  is a service.
#   `-platform`  — fired on "per-platform" and "cross-platform". Dropped; the
#                  only internal `-platform` name appears in zero commits here.
#   `-service`   — fired on "self-service". Replaced by the two service names
#                  this repository's own history already carries, which is
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
# ⚠ ONE KNOWN FALSE POSITIVE IS LEFT STANDING, deliberately. A licence citation
# that wraps between its name and its section — `Apache-2.0` ending one line and
# `§4(d)` starting the next — leaves a bare `§4(d)` that the line-oriented pass
# reports, because a bare section reference naming a document the reader cannot
# open is precisely what the `§` class is for. The collapsed pass applies the
# citation exception and the line pass cannot, since the evidence that it IS a
# citation is on the previous line.
#
# Suppressing it would mean deciding a line's fate from its neighbours, and
# every attempt to give this file a second opinion about a line has produced a
# bug: the first citation exception dropped the WHOLE line and turned a
# false-positive fix into an evasion. The cost of the false positive is
# rewrapping one sentence. The cost of the machinery has already been measured
# twice, and it was higher.
set -euo pipefail

cd "$(dirname "$0")/.."

# One pattern list, used by both lanes and by the self-test — two spellings is
# how the second consumer comes to check something different from the first.
# ── THE ROSTER HALF, AND THE RULE THAT NOW ENFORCES ITSELF ──────────────────
# These are the only literal internal names in this file. Each is admissible
# under one rule: **this repository's published history must already carry it**,
# so the file discloses nothing a `git log` of a repository anyone can clone
# does not already give up.
#
# That rule was PROSE until 2026-08-20, and prose does not hold. Measured on
# that date by two independent methods — `git log -S` over every ref, and a
# content grep across every commit — TWO entries in this list appeared in ZERO
# commits of this repository. The gate written to stop internal names reaching
# a public repository was introducing two of them, for the first time, in the
# same commit that claimed the opposite. Both are gone, and
# `roster_is_already_published` below runs the check on every invocation so the
# next one fails instead of shipping.
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
ROSTER='analytics-service
crash-symbolicator
control-plane
shardpilot/docs
shardpilot/integrations
Project Tower
shepherd-pr
verification-discipline'

# Derived, never spelled twice — the defect this whole file keeps finding is a
# correction landing in one copy while another keeps the old value. Each
# separator becomes a character class, so a name written with a space or an
# underscore instead of a hyphen is caught too; matching is case-insensitive,
# so a sentence-initial capital is not an evasion.
# `[-_ ]+`, not `[-_ ]`. A hyphenated name wraps AT THE HYPHEN — that is where
# a formatter breaks it — so the collapsed copy reads `control- plane`, two
# separator characters where the source had one. A single-character class
# missed exactly the wrap this pass exists to catch, while matching the
# multiword names it wraps less often. Verified by mutation in both shapes.
# A SLASH IS A WRAP POINT TOO, and it needed its own rule: `[-_ ]+` cannot
# describe `shardpilot/ integrations`, because the slash must stay literal
# while the space around it must be optional. Text wraps either side of a
# slash, so both are allowed.
roster_regex() {
  printf '%s' "$ROSTER" | sed -e 's![-_ ]![-_ ]+!g' -e 's!/! */ *!g' | paste -sd'|' -
}
ROSTER_RE="$(roster_regex)"

# The SHAPE half. These name no record, no ticket, no branch and no service, so
# they are safe to publish in the file that gates against them.
PATTERNS='ADR-[0-9]+|§[0-9]|[Tt]here (is|are) [Nn][Oo] [A-Za-z][A-Za-z-]*( [A-Za-z-]+){0,2} (harness|harnesses|coverage|tests?|suites?)|(is|are) not (tested|covered|scanned|audited|monitored)|[Nn]o (automated )?(tests?|coverage|scanning|monitoring) (for|of|in)|[Nn]obody (looks|checks|monitors)|GAP-[0-9]{3}|\bSP-[0-9]{3}\b|\bAC-[A-Z]{2}-[0-9]+|Codex (review|#|[a-z]+#)|[A-Z][A-Z0-9]*(_[A-Z0-9]+)+_(ENABLED|DISABLED|MODE)|\b(main|master|HEAD) @ *`?[0-9a-f]{7,40}'

# The one legitimate collision with the `§` class, in ONE place so the scan
# and the self-test cannot disagree about it. The first attempt put the
# exclusion only in the scan; the innocent self-test then failed, correctly,
# because it was testing a different rule from the one that runs.
# ⚠ THE EXCEPTION APPLIES TO THE CITATION, NOT TO THE LINE. The first version
# filtered with `grep -v "$EXCLUDE"`, which drops the WHOLE line — so
# `see ADR-0000; Apache-2.0 §4(d)` matched PATTERNS, was then discarded for
# containing a licence citation, and lane A reported clean. That turned a
# false-positive fix into an EVASION: any internal identifier sharing a line
# with the word Apache became invisible.
#
# So the citation is blanked out of a COPY of the line and the remainder is
# re-tested. A line that is only a licence citation drops out; a line that is a
# licence citation AND something else is still reported, in full.
# ⚠ NO ARBITRARY GAP BEFORE THE `§`. The previous form had an alternative
# `Apache License[^§]{0,40}§[0-9]+`, and `[^§]{0,40}` swallows whatever sits
# between — INCLUDING the forbidden identifier. Reproduced: the line
# `Apache License applies; see ADR-9999 §7` was removed wholesale, the ADR
# reference with it, and the gate exited clean. That is the same EVASION the
# whole-line `grep -v` produced two rounds ago, rebuilt in a narrower-looking
# shape: any exception permitted to consume text it was not written to describe
# will eventually consume a finding.
#
# So the exception now spans only what a citation actually contains — the
# licence name, an optional version phrase, and the section — with nothing but
# spaces and commas between. Measured on both trees at the time of writing:
# ZERO Apache section citations exist in either, so this exception currently
# protects nothing and could only cost. It is kept narrow rather than deleted
# because the case it was written for was real in a sibling repository.
CITATION='Apache([ -]License)?([, ]+Version)?[ -]2\.0[, ]*§[0-9]+(\([a-z]\))?|LICENSE §[0-9]+(\([a-z]\))?'

# strip_citations — reads lines, prints those that still match PATTERNS once
# every legitimate licence citation has been removed from the copy under test.
# ⚠ sed + grep, NOT awk. `PATTERNS` uses `\b` for `SP-123`, `AC-AB-7` and the
# `main @ <sha>` pin. POSIX awk has NO word-boundary operator — `\b` there is
# an undefined escape, which gawk reads as a backspace and mawk drops — so this
# second pass stopped matching exactly the three classes that depend on it,
# while every other class kept working and the run stayed green. A line whose
# only finding was `SP-123` went through the gate. Keeping every pass in grep
# means one regex dialect for the whole file, which is the only version of this
# that a reader can check by eye.
#
# The line NUMBER survives because sed is a substitution, not a filter: the
# input to this function is already `<n>:<text>` from `grep -n`, and blanking a
# citation inside the text cannot renumber anything.
strip_citations() {
  sed -E "s/${CITATION}//g" | grep -E -- "$PATTERNS"
}

# Files the gate must not read as content: this script is the one place the
# patterns are written down, by construction.
is_exempt() { [ "$1" = "scripts/check_public_surface.sh" ]; }

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
scan_lane_a_files=0  # files LANE A actually gates, which is not the total

scan_tree() {
  local root="$1" f hits status line list
  scan_lane_a=""; scan_lane_b_files=0; scan_lane_b_lines=0; scan_files=0; scan_lane_a_files=0

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
    is_exempt "$f" && continue
    # THE PATH ITSELF IS PUBLISHED CONTENT. An internal identifier in a file
    # NAME — a decision-record id, a ticket, a service name in a directory —
    # reaches every consumer and appears in no file's body, so scanning only
    # contents misses it entirely.
    # ⚠ WHITESPACE COLLAPSED FIRST. A path may legally contain a newline, and
    # these passes are line-oriented: an identifier straddling one is split
    # into two lines and matches nothing. `git ls-files -z` hands the raw name
    # over precisely so such a path is not lost, and then this check would have
    # dropped it anyway.
    # ⚠ AND REJOINED, for the same reason the content pass needs it: a name
    # wraps AT its separator, so flattening alone turns `control-<newline>plane`
    # into `control- plane`. Both forms are tested — the flattened one for
    # phrases, the rejoined one for identifiers.
    f_flat="$(printf '%s' "$f" | tr -s '[:space:]' ' ')"
    f_join="$(printf '%s' "$f_flat" | sed -E 's/([-_/]) /\1/g')"
    if printf '%s\n' "$f_flat" | grep -qE -- "$PATTERNS" \
       || printf '%s\n' "$f_flat" | grep -qiE -- "$ROSTER_RE" \
       || printf '%s\n' "$f_join" | grep -qE -- "$PATTERNS" \
       || printf '%s\n' "$f_join" | grep -qiE -- "$ROSTER_RE"; then
      scan_lane_a="${scan_lane_a}${f}:path:${f}"$'\n'
    fi
    # -h: a symlink's published blob content IS its target path, so read the
    # link rather than skipping it or following it out of the checkout.
    if [ -L "$root/$f" ]; then
      # BOTH passes. A symlink's published blob content IS its target path, and
      # a target naming a roster entry is the same disclosure as one naming a
      # shape — checking only $PATTERNS here left the roster half blind to an
      # entire file type.
      link_flat="$(readlink "$root/$f" | tr -s '[:space:]' ' ')"
      hits="$( { printf '%s\n' "$link_flat" | grep -nE -- "$PATTERNS" || true
                 printf '%s\n' "$link_flat" | grep -niE -- "$ROSTER_RE" || true; } )"
      [ -n "$hits" ] && scan_lane_a="${scan_lane_a}${f}:symlink-target:$(readlink "$root/$f")"$'\n'
      scan_files=$((scan_files + 1))
      # A symlink IS gated by lane A — its target is reported into
      # `scan_lane_a` three lines up — so it has to be counted there too.
      # This branch `continue`d before the lane counter below, which meant a
      # gated file was missing from the number lane A publishes about itself.
      case "$f" in
        *.go) ;;
        *) scan_lane_a_files=$((scan_lane_a_files + 1)) ;;
      esac
      continue
    fi
    [ -f "$root/$f" ] || continue
    scan_files=$((scan_files + 1))
    # ⚠ COUNTED PER LANE, BEFORE THE SCAN. The lane A line used to print
    # `scan_files`, the total of everything read — 90 on a tree where lane A
    # gates 12. A gate that overstates its own coverage sevenfold is worse than
    # one that says nothing: the number is what a reader checks when deciding
    # whether the green means anything. Counted here rather than at the lane
    # split below, because that split only runs for files that HIT.
    case "$f" in
      *.go) ;;
      *) scan_lane_a_files=$((scan_lane_a_files + 1)) ;;
    esac
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
      1f8b*|504b0304|504b0506|fd377a58|425a68*|28b52ffd|25504446|4d5a*|7f454c46)
        echo "REFUSING: '$f' begins with container magic (archive, PDF or executable)," >&2
        echo "  and this gate reads files as text. Its real contents are not scanned by" >&2
        echo "  any pass here, so a clean result would say nothing about what it carries." >&2
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
    # ⚠ ONE LEGITIMATE COLLISION WITH THE `§` CLASS, AND IT RECURS.
    # "Apache-2.0 §4(d)" is a correct public citation of a public document, and
    # it turns up wherever a NOTICE obligation is explained — it fired three
    # times in a sibling repository. The class exists for a DANGLING internal
    # section: a bare `§7c` left behind when a decision-record id was stripped,
    # naming a document the reader cannot open. Keeping the false positive
    # would train a reader to skim § hits, which is how the real one gets
    # waved through.
    # ⚠ NUL IS STRIPPED BEFORE awk, NOT AFTER. awk truncates a record at a NUL
    # byte, so running the citation filter first silently dropped the match
    # inside a NUL-bearing file — the scan fixture caught it immediately, which
    # is the whole reason that fixture carries a binary.
    hits="$(grep -anE -- "$PATTERNS" "$root/$f" 2>/dev/null \
      | tr -d '\000' \
      | strip_citations; exit "${PIPESTATUS[0]}")"
    status=$?
    # THE ROSTER, in its own pass because it is the one class matched
    # case-insensitively: a name capitalised at the start of a sentence is the
    # same disclosure as the lower-case spelling, and folding `-i` into the
    # shape pass
    # would make `[Tt]here is` and the ALLCAPS flag class match prose they
    # were written to leave alone.
    roster_hits="$(grep -aniE -- "$ROSTER_RE" "$root/$f" 2>/dev/null \
      | tr -d '\000'; exit "${PIPESTATUS[0]}")"
    roster_status=$?
    # WRAPPED PROSE. Every pass above is line-oriented, and the classes that
    # matter most here are PHRASES — "there is no X harness", "is not covered
    # by automated tests", "main @ <sha>". A phrase that wraps across a line
    # break matches none of them, and wrapping is not exotic: it is what a
    # formatter does to a long sentence. Measured in a sibling repository, the
    # phrase `hashed at ingest` moving onto a new line silently dropped a
    # literal-pin check that had been green for weeks.
    #
    # So the file is also read with its whitespace collapsed to single spaces,
    # which cannot manufacture a hit for the single-token shape classes and
    # restores every multi-word one. `-o` because a whole-file-on-one-line
    # match would otherwise print the entire file.
    #
    # ⚠ BOTH PATTERN SETS, AND CITATIONS STRIPPED FIRST. The first version of
    # this pass ran $PATTERNS alone over raw collapsed text, which got both
    # halves wrong at once: a wrapped ROSTER name — the multiword ones wrap
    # most readily — went unmatched, and a wrapped `Apache-2.0 §4(d)` was
    # reported as a finding because the citation exception the line-oriented
    # pass applies was not applied here. A new pass that does not inherit the
    # exceptions of the pass it supplements manufactures false positives, and
    # a false positive in a publication gate is how the gate gets switched off.
    #
    # ⚠ AND NO PIPESTATUS INDEX. The earlier form ended `exit "${PIPESTATUS[1]}"`
    # believing [1] was the grep; the pipeline was tr|tr|grep|sort, so [1] is
    # the SECOND tr — a command that essentially cannot fail. The status was
    # therefore always 0 and the trichotomous grep-error check downstream was
    # reading a constant. Readability is already established by the
    # line-oriented pass above, whose status IS checked for >= 2 on this exact
    # file, so this pass decides on content alone and says so.
    collapsed="$(tr -s '[:space:]' ' ' < "$root/$f" 2>/dev/null | tr -d '\000' \
      | sed -E "s/${CITATION}//g")"
    # ⚠ AND A SECOND COLLAPSED COPY WITH SEPARATORS REJOINED. Collapsing turns
    # a line break into a space, which is right for a phrase and wrong for an
    # identifier: text wraps AT the separator, so `ADR-` ending a line and
    # `0999` beginning the next becomes `ADR- 0999`, which `ADR-[0-9]+` cannot
    # match. The roster half already needed this and solved it inside its own
    # derivation; the shape half has no derivation to change, so the input is
    # repaired instead.
    #
    # Rejoining is safe here because this copy is used ONLY to detect wrapping:
    # every shape class needs a literal prefix (ADR-, GAP-, SP-, AC-) before the
    # separator, so ordinary prose like `the plan - 2026` cannot be turned into
    # one of them.
    rejoined="$(printf '%s' "$collapsed" | sed -E 's/([-_/]) /\1/g')"
    wrapped_raw="$( { printf '%s\n' "$collapsed" | grep -aoE -- "$PATTERNS" || true
                      printf '%s\n' "$collapsed" | grep -aoiE -- "$ROSTER_RE" || true
                      printf '%s\n' "$rejoined"  | grep -aoE -- "$PATTERNS" || true; } )"
    wrapped_hits="$(printf '%s\n' "$wrapped_raw" | sort -u)"
    # ⚠ A DEDUPLICATOR THAT FAILS MUST NOT LOOK LIKE A CLEAN FILE. If `sort`
    # cannot write its output, the substitution above is empty and every
    # forbidden phrase it consumed disappears with it — the exact fail-open
    # shape this gate refuses everywhere else.
    if [ -n "$wrapped_raw" ] && [ -z "$wrapped_hits" ]; then
      echo "REFUSING: deduplicating the wrapped-scan results for '$f' produced nothing" >&2
      echo "  from a non-empty input. Those matches would be dropped in silence." >&2
      exit 2
    fi
    if [ -n "$wrapped_hits" ]; then wrapped_status=0; else wrapped_status=1; fi
    set -e
    for st in "$status" "$roster_status"; do
      if [ "$st" -ge 2 ]; then
        echo "REFUSING: grep could not read '$f' (exit $st)." >&2
        echo "  An unreadable file is an UNSCANNED file, and this gate must not" >&2
        echo "  report a repository clean on the strength of one." >&2
        exit 2
      fi
    done
    # A wrapped hit is only NEWS if the line-oriented pass did not already see
    # it; otherwise every ordinary finding would be reported twice.
    # ⚠ AN EMPTY RESULT IS NOT A HIT, whatever the status says. `status` is the
    # FIRST grep's, preserved on purpose so a read error survives the pipeline —
    # but that grep matching means only that the raw line matched, and
    # strip_citations may then have removed every one of them. The file was
    # counted anyway, so a file whose only matches were legitimate licence
    # citations inflated the lane's FILE count while contributing no lines.
    [ -n "$hits" ] || status=1
    if [ "$wrapped_status" -eq 0 ]; then
      new_wrapped=""
      # ⚠ AGAINST BOTH LINE-ORIENTED PASSES, not just the shape one. A
      # roster-only identifier lands in $roster_hits, never in $hits, so
      # comparing against $hits alone re-reported every ordinary roster hit as
      # "wrapped" — double-counting each one in the lane totals and telling a
      # reader to look for a line break that is not there.
      while IFS= read -r w; do
        [ -n "$w" ] || continue
        printf '%s\n%s\n' "$hits" "$roster_hits" | grep -qF -- "$w" \
          || new_wrapped="${new_wrapped}0:wrapped across a line break: ${w}"$'\n'
      done <<< "$wrapped_hits"
      wrapped_hits="$new_wrapped"
      [ -n "$wrapped_hits" ] || wrapped_status=1
    fi
    if [ "$status" -ne 0 ] && [ "$roster_status" -ne 0 ] && [ "$wrapped_status" -ne 0 ]; then
      continue
    fi
    # One corpus from here down, so the lane split below cannot see a
    # different set of hits from the one the statuses were computed over.
    for extra in "$roster_hits" "$wrapped_hits"; do
      [ -n "$extra" ] && hits="${hits:+${hits}$'\n'}${extra}"
    done
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
# shape is exercised just as well by `ADR-0000` as by a real decision-record
# number — while a real one would make this fixture block the same kind of
# roster the ROSTER rule above exists to bound. The earlier version used a live
# ADR id, a live feature-flag name and a live ticket number, none of which the
# test needed.
KNOWN_INTERNAL='per ADR-0000 §3
There are no Playwright tests for the console.
There are no end-to-end tests for the purchase flow.
see ADR-0000; Apache-2.0 §4(d) applies too
There is NO Playwright harness in the console repo
the crash path is not covered by automated tests
no automated scanning for that class of input
a bare §7c left behind by a citation strip
tracked as GAP-000 internally
pinned to main @ 0000000
(Codex go#48 round 3)
EXAMPLE_SYNTHETIC_FLAG_MODE=off'
KNOWN_INNOCENT='go get github.com/shardpilot/shardpilot-go@v0.6.0-alpha
IngestURL: os.Getenv("SHARDPILOT_INGEST_URL")
POST {IngestURL}/v1/events:batch
https://localhost:8080 during local development
a documented per-platform adaptation, not drift
DEFOLD_SHA1="f735c12192bf95684e6ae1ae27c400b8170fc6d8"
a self-service signup flow, a micro-service boundary
Apache-2.0 §4(d) obliges a redistributor to carry the NOTICE
the event plane and the consent plane are separate
an analytics-plane request, zero event batches'

# roster_is_already_published — the rule from the ROSTER block, executed.
#
# Every literal in ROSTER must appear in this repository's PUBLISHED history.
# If it does not, this file is the first thing to publish it, and the gate is
# committing the disclosure it exists to prevent.
#
# ⚠ THIS FILE IS EXCLUDED FROM THE SEARCH, and without that the check is
# self-satisfying: add a name here, commit, and `git log -S` finds it — in this
# very file — from the next run onward. The exclusion is what makes the answer
# be about the rest of the repository.
#
# ⚠ AND IT REFUSES ON A SHALLOW CLONE rather than reporting names unpublished.
# A truncated history makes every literal look novel, which would fail the run
# with a message pointing at the roster instead of at the checkout — sending
# the reader to delete correct entries.
roster_is_already_published() {
  local lit novel=0
  if [ "$(git rev-parse --is-shallow-repository 2>/dev/null)" = "true" ]; then
    echo "REFUSING: shallow checkout — the roster rule needs full history." >&2
    echo "  Every literal would read as unpublished and the run would blame" >&2
    echo "  the roster for a property of the clone. Use fetch-depth: 0." >&2
    exit 2
  fi
  # ⚠ NO PIPE INTO `grep -q`, AND THIS EXACT FUNCTION IS WHY. The first version
  # asked `git log … | grep -q .`. `grep -q` exits at the FIRST match and closes
  # the pipe, `git` then dies of SIGPIPE with status 141, and `set -o pipefail`
  # reports the pipeline as failed — so `if !` turned every MATCH into a miss.
  # It declared all eight roster names unpublished in a repository whose history
  # carries every one of them, and the BETTER the match the sooner it fired.
  # A guard that fails loudly on a correct tree is a guard somebody deletes.
  #
  # Materialised to a file and tested with `-s`: no pipeline, so no status to
  # invert.
  local found; found="$(mktemp)"
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    git log --all --format=%H -S"$lit" -- . ':(exclude)scripts/check_public_surface.sh' > "$found" 2>/dev/null || :
    if [ ! -s "$found" ]; then
      printf 'ROSTER VIOLATION: %s appears in no commit of this repository outside this file.\n' "$lit" >&2
      novel=$((novel + 1))
    fi
  done <<EOF
$ROSTER
EOF

  # ⚠ THE SAME RULE, APPLIED TO THIS FILE'S OWN PROSE — because the scan
  # exempts this file, and that exemption is exactly where the last one hid.
  # The comment announcing the removal of two internal repository names spelled
  # both of them out, and no pass in this gate reads its own comments.
  #
  # A shape rather than a list, so it needs no roster of its own: every
  # `shardpilot/<name>` written ANYWHERE in this file must satisfy the rule
  # above. Both offenders were exactly that shape.
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    git log --all --format=%H -S"$lit" -- . ':(exclude)scripts/check_public_surface.sh' > "$found" 2>/dev/null || :
    if [ ! -s "$found" ]; then
      printf 'PROSE VIOLATION: %s is written in this file and appears in no commit of this repository.\n' "$lit" >&2
      novel=$((novel + 1))
    fi
  done <<EOF
$(grep -oE 'shardpilot/[a-z][a-z-]*' "$0" | sort -u)
EOF

  rm -f "$found"
  if [ "$novel" -ne 0 ]; then
    echo "REFUSING: the roster introduces $novel internal name(s) this public" >&2
    echo "  repository has never carried. A gate against publishing internal" >&2
    echo "  names must not be the commit that publishes them — remove them from" >&2
    echo "  ROSTER. They stop being gated here; say so rather than keeping them." >&2
    exit 2
  fi
  echo "roster rule: OK — all $(printf '%s\n' "$ROSTER" | grep -c .) literal(s) already present in published history"
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
    # The SAME filter the scan applies, so the two cannot drift.
    # BOTH passes, because an innocent line is only innocent if NEITHER fires.
    # Testing the shape pass alone let the roster's case-insensitive pass go
    # unchecked against every fixture written to prove a false positive gone.
    if { printf '%s\n' "$line" | grep -E -- "$PATTERNS" | strip_citations
         printf '%s\n' "$line" | grep -iE -- "$ROSTER_RE"; } | grep -q .; then
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
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' RETURN
  (
    cd "$tmp"
    git init -q .
    git config user.email t@t; git config user.name t
    printf 'clean customer prose\n' > clean.md
    printf 'nothing internal in the body\n' > ADR-0999-notes.md   # hit is the NAME
    printf 'see ADR-0000 for context\n' > dirty.md
    printf '// GAP-000 note\npackage x\n' > lane_b.go
    printf 'internal: control-plane\n' > "café.md"   # C-quoted by git ls-files
    printf 'x\0see ADR-0999 here\n' > binary.bin      # NUL: "binary" to grep
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
  if [ "$fixture_fail" -ne 0 ]; then
    echo "REFUSING: the scan failed its own fixture." >&2
    exit 2
  fi
  echo "self-test: OK — $tested known-internal string(s) matched, $innocent innocent string(s) passed, scan fixture 6/6"
}

roster_is_already_published
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
echo "LANE B (REPORTED, NOT GATED) — Go source: ${scan_lane_b_files} file(s), ${scan_lane_b_lines} matching line(s)."
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
echo "LANE A (GATED) — non-source tracked files: clean (${scan_lane_a_files} of ${scan_files} tracked file(s) scanned; the rest are Go source, reported in lane B)."
