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
# LANE B is REPORTED AT ZERO, GATED ON CHANGE: Go source (*.go). Those comments
#   are owned by another workstream (the SDK wire freeze) and editing them here
#   would collide with it, so existing debt does not fail the run -- but the
#   per-file counts are held against a baseline, and a count that RISES, or that
#   FALLS without the baseline being updated, does fail. Two different things
#   share the word "gated" and the distinction is the whole design: not gated at
#   zero, gated on change.
# Neither lane looks at git HISTORY. Deleting a line does not unpublish the
# commit that carried it.
#
# ⚠ REFUSALS COME IN TWO KINDS AND A CENSUS MUST NOT CONFLATE THEM.
#   HAZARD refusals are about content that can reach the public surface with
#   nobody intending it — an internal name, a decision-record id, a service
#   name in prose, an archive in the tree. These are kept whatever their firing
#   count: zero firings means "has not happened yet", not "does not protect".
#   STRUCTURAL refusals say nothing about content. They say this run is not in
#   a state to report on content — the listing failed, no file was read, a blob
#   was unreadable, the pattern list is empty. They are what separates "passed"
#   from "never ran", and a dead run reports nothing and reads exactly like a
#   clean one. Each is marked at its site.
#
# ⚠ AND THE FIXTURE COVERAGE SENTENCES ARE NOT BOUNDED BY ANYTHING HERE.
# Identifiers in them are, by shape — nothing real is numbered all zeros, nines
# or f's — but a coverage disclosure has no synthetic form: a sentence about
# missing tests reads identically whether its subject is invented or real. What
# narrows it is that every known-internal line must MATCH a class, so the only
# thing that fits there is another coverage-shaped sentence. A gate cannot
# close this; a reviewer can.
#
# ⚠ SOURCE AND RENDERED PAGE ARE DIFFERENT SURFACES, and the published one is
# the page. THIS GATE DOES NOT MODEL THE RENDERER. It compares against a form
# that DOMINATES any output the renderer could produce — a copy with every
# inline marker character removed — so whatever a renderer would join, that
# copy joins too. There is no false miss along this axis by construction rather
# than by testing, and nothing to add when the next construct appears.
#
# That line is the whole design and it replaced a normalisation pipeline that
# tried to predict the rendered string. The pipeline was correct five times
# over five rounds of review and wrong on the sixth construct every time,
# because the list of constructs is CommonMark's to close and not this file's.
# Anyone reading a Markdown form this gate did not catch should reach for the
# dominating form, not for another rule.
#
# The criterion is therefore "can a reader SEE the identifier", not "does the
# page render it contiguously". A record id whose digits are wrapped in
# emphasis shows the number to whoever reads the page. Measured before the
# change: over every lane-A file in both trees the marker-free copy adds ZERO
# matches the raw text does not already have, and the measurement was itself
# checked against a planted decorated identifier so that a silent instrument
# could not read as a clean result.
#
# The one instance this gate has ever caught in already-committed content sits
# inside that class — `**not**` in a sentence about coverage, where a person
# wrote two asterisks as people do and the meaning changed on the page.
#
# ⚠ AND WHAT IS DELIBERATELY ENCODED IS OUT OF SCOPE — SO IT IS REFUSED AT THE
# DOOR RATHER THAN READ. This replaces an earlier answer that was removed for a
# good reason and reinstated for a better one, and both belong on the record.
#
# The removal was right about its own target. Eight refusals that tried to SEE
# THROUGH deliberate encoding, while this paragraph declared deliberate
# encoding out of scope, was one file saying two things — the exact defect the
# dominating form was written to remove.
#
# What the removal left behind was measured, not argued. On a clone of a public
# repository: an internal record id written plainly is refused; the same id with
# its first letter written as a numeric character reference PASSES, and the page
# renders both identically. A gate whose scope note says "out of scope" and
# whose behaviour says "published" is the same two-things problem seen from the
# other side, and this one leaves a hole in an irreversible surface.
#
# The reinstated answer is not the removed one. It does not decode anything and
# it cannot be seen through, because it never accepts the input: a file
# carrying a numeric character reference is REFUSED, with the remedy in the
# message. Refusing to read a form is coherent with declaring it unread. Trying
# to read it was not.
#
# THE NUMERIC FAMILY IS REFUSED WHOLE, including references that decode to
# characters no identifier here could contain. Carving those out means decoding
# the number to find out, which is the pipeline this gate does not have — and
# then the carve-out needs the hexadecimal form, the leading-zero form and the
# terminator-less form that browsers also accept. That list grows by one per
# review round; the whole family does not. Writing the character itself costs
# one keystroke.
#
# NAMED references are ALL refused when terminated, and the narrow policy this
# paragraph used to describe is gone. Picking the ones decoding into the
# identifier alphabet was the first revision, and review found its edge in both
# directions inside one round. Every narrowing needs a decode table, and the
# table grew by an entry per round.
#
# The cost of refusing them whole was measured rather than feared: across the 26
# repositories in this workspace, character references appear in markdown twice,
# and in this repository the only occurrence is this gate's own fixture.
#
# ⚠ THE PUBLISHING SURFACE DOES RENDER THEM, MEASURED. Against GitHub's own
# GFM endpoint on 2026-08-21, every one of these produced a contiguous
# identifier on the page: emphasis, a backslash escape, a numeric character
# reference, an inline element, an HTML comment, and link syntax. One did not:
# a named reference for a hyphen yields U+2010, not the ASCII character, so it
# cannot form an identifier in any class here at all. The boundary below is
# therefore NOT "the surface does not render it" — it does. It is "a person did
# not do it by accident", and against the other kind stands review.
#
# What is consequently NOT read: anything a renderer assembles that emphasis
# and escape normalisation do not undo. Images are still refused, because
# committing a screenshot IS an accident.
#
# ⚠ AND THIS GATE DOES NOT CATCH A NAME HIDDEN DELIBERATELY. Its subject is
# material that reaches the public surface WITHOUT ANYONE INTENDING IT. A
# single-word internal name placed inside a branch of the pattern list is
# indistinguishable from the English words that list legitimately contains, and
# both ways of telling them apart were measured and rejected: requiring every
# long word to exist elsewhere in the tree refuses a clean repository —
# thirteen ordinary words here appear nowhere else — and a dictionary of
# admissible words is a list the same change can extend, which is the defect
# the identifier allowlist was removed for.
#
# What stands there instead is review: this corpus is one small file, the
# pattern list is asserted against fixtures on every run, and a name added to
# either is a visible line in a diff. That is a stated BOUNDARY, not an
# oversight — a green run is not evidence about it.
#
# ⚠ AND A COMMENT IN THIS FILE LIVES ON THE SCANNED SURFACE. Three times while
# this was written, explaining a rule by quoting the literal it looks for made
# the gate fail on itself — correctly. Describe the shape; do not spell it.
#

set -euo pipefail

SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
if [ ! -f "$SELF" ]; then
  # refusal:structural
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
# describe an owner and a repository separated by a slash with a space beside
# it, because the slash must stay literal while the space around it must be
# optional. Text wraps either side of a slash, so both are allowed.
#
# ⚠ THE EXAMPLE THAT STOOD HERE WAS A LIVE INTERNAL REPOSITORY NAME. It was
# published by this file and by nothing else in either tree, and it was
# invisible to the audit below for exactly the reason this paragraph
# describes — one space after the slash. It survived every round of review of
# the change that introduced it. Illustrate the SHAPE, never an instance: an
# example is the one place a real name gets written down without anybody
# reading it as a disclosure.

# The SHAPE half. These name no record, no ticket, no branch and no service, so
# they are safe to publish in the file that gates against them.

# Files the gate must not read as content: this script is the one place the
# patterns are written down, by construction.

# The by-construction material — patterns, roster, fixture corpora — is INLINE
# below. It lived in a second file, excluded by PATH, until that exclusion cost
# more than it bought: eight refusals existed only to police the excluded
# file's grammar and went out with it. No path is exempt from the scan now; the
# visible break is what keeps these values from matching themselves.
# Every temporary file this gate makes, removed on every exit path. They were
# created in four places and removed in one, so a successful run left five
# behind — including copies of staged repository content.
# An ARRAY, because a temporary directory may contain whitespace: a
# space-delimited list with an unquoted expansion would split one path into
# several and remove neither.
GATE_TMPFILES=()
# ⚠ IT SETS A VARIABLE RATHER THAN PRINTING ONE. Called as `$(gate_tmp)` the
# function would run in a subshell, so every path it recorded would be
# discarded with that subshell and this trap would remove an empty list — a
# cleanup that reads as working and frees nothing. It was written that way
# first, and the file count is what showed it.
gate_tmp() {
  GATE_TMP="$(mktemp)" || return 1
  GATE_TMPFILES+=("$GATE_TMP")
}
# ⚠ AND A RUN THAT DID NOT REACH ITS OWN END EXITS NON-ZERO, whatever the
# shell says. Measured on bash 3.2: after a `set -u` failure the EXIT trap is
# entered with `$?` ALREADY ZERO — the status of the last command that
# completed — so capturing it recovers nothing and the gate reports success
# having died mid-run. A flag set on the last line is the only thing that
# distinguishes finishing from stopping, so that is what the trap reads.
gate_finished=no

# ⚠ THE TRAP PRESERVES THE STATUS, and does not touch an EMPTY array.
# Measured on bash 3.2: a `set -u` failure followed by an EXIT trap whose last
# command SUCCEEDS exits 0 — the shell dies mid-run, prints nothing on stdout,
# and reports success. That is a dead run reading as a clean one, in the one
# control here that must fail closed. An explicit `exit 2` was preserved, so
# every refusal looked fine and only the fatal path was silent. The status is
# captured before the cleanup and restored after it.
#
# `${a[@]+"${a[@]}"}` rather than `"${a[@]}"`: on that shell an EMPTY array is
# itself an unbound variable, so the cleanup would fail before it could restore
# anything.
trap 'gate_rc=$?; rm -f ${GATE_TMPFILES[@]+"${GATE_TMPFILES[@]}"};
      if [ "$gate_finished" != yes ] && [ "$gate_rc" = 0 ]; then
        echo "REFUSING: this gate stopped before its own last line and the shell" >&2
        echo "  reported success. Nothing here has been established; read the" >&2
        echo "  error above this line, not the absence of findings." >&2
        gate_rc=3
      fi
      exit "$gate_rc"' EXIT

SELF_REL="scripts/$(basename "$SELF")"
gate_tmp; SELF_BLOB="$GATE_TMP"
if ! (cd "$(dirname "$SELF")/.." && git cat-file blob ":$SELF_REL") > "$SELF_BLOB" 2>/dev/null; then
  # refusal:structural
  echo "REFUSING: the staged blob for $SELF_REL could not be read." >&2
  echo "  The audits below read this script for literals it alone would" >&2
  echo "  publish, and a commit carries the staged copy, not this one." >&2
  exit 2
fi
# ⚠ THE RUNNING COPY MUST BE THE STAGED ONE. Everything this gate reads comes
# from the index — every tracked blob, and this script for its own audits — but
# the DATA below is read by bash from the file it is executing, which is the
# working copy. Stage a change to the matcher or the fixtures, restore the
# working copy, and the values that run are not the values that would be
# committed: a local green run describing a different tree, which is the exact
# failure this gate spent a day removing everywhere else.
#
# refusal:structural
if ! cmp -s "$SELF" "$SELF_BLOB"; then
  echo "REFUSING: this script differs from its staged copy." >&2
  echo "  Its data is read by the shell from the file being executed, while" >&2
  echo "  everything else here reads the index. Running one and reporting on" >&2
  echo "  the other is how a green run stops describing the commit." >&2
  echo "  Stage this file, then run again." >&2
  exit 2
fi

# ⚠ AND NO ANSI-C QUOTED FRAGMENT ANYWHERE IN IT. `$'\400'` is a NUL, and bash
# truncates a value there — everything after it stays in the published file and
# reaches neither the decode, nor the audits, nor the self-test, which goes on
# counting the fixtures it can still see. Rather than validate escape
# spellings, of which there are more than anyone enumerates, the construct that
# admits them is refused outright: measured, this script contains none, and an
# apostrophe is written `'\''` instead.
#
# refusal:structural
# The needle is BUILT, not written: spelling it here would put the construct
# into the file this check reads, and the gate refused itself the first time.
# IN THE DATA BLOCK, not the whole script: the code below uses the same
# construct legitimately to append a newline, and refusing that would be a
# rule against an idiom rather than against a hazard.
ansi_c="$(printf '\044\047')"
# ⚠ THE SENTINEL MUST BE THE LAST ONE, not the first. A second `unset
# gate_var` inserted right after the names ends this range before the
# assignments it exists to validate — the range walked past its own subject.
# Counted first: exactly one closing sentinel, or this cannot delimit anything.
sentinels="$(grep -c '^unset gate_var$' "$SELF_BLOB" || true)"
if [ "$sentinels" != 1 ]; then
  # refusal:structural
  echo "REFUSING: the data block has $sentinels closing sentinel(s), expected 1." >&2
  echo "  The validated range is delimited by it; more than one ends the range" >&2
  echo "  early and leaves the assignments after it unchecked." >&2
  exit 2
fi
data_block="$(sed -n '/^GATE_DATA_NAMES=/,/^unset gate_var$/p' "$SELF_BLOB")"
if printf '%s' "$data_block" | grep -qF -- "$ansi_c"; then
  echo "REFUSING: the data block contains an ANSI-C quoted fragment." >&2
  { printf '%s' "$data_block" | grep -nF -- "$ansi_c" || true; } | head -5 | sed 's/^/    /' >&2
  echo "  Those admit escapes that produce a NUL, which truncates a value and" >&2
  echo "  leaves the rest of it published and unread. Use '\'' for an" >&2
  echo "  apostrophe and write other characters literally." >&2
  exit 2
fi

# ── THE MATERIAL THAT MATCHES BY CONSTRUCTION, IN THIS FILE AND SCANNED ─────
# It lived in a separate file excluded from the scan BY PATH, and that
# exclusion was the whole problem: an unscanned file is somewhere to put
# things, and guarding it took eight refusals that caught nothing but probes.
# Guarding an exclusion with regular expressions is a weakened version of the
# scan you are declining to run.
#
# So there is no exclusion. Each value that would match its own classes carries
# a VISIBLE BREAK — `[]`, which occurs nowhere in this prose — and the loader
# removes it. The text stays legible to anyone reading the file, and the scan
# reads these bytes exactly as it reads every other line here, so nothing can
# hide in them that could not hide anywhere else in this script.
#
# The break placement was GENERATED, not hand-made, and two properties were
# verified over every value before it landed: no broken value matches
# `$PATTERNS` or the roster class, and every decode is byte-identical to the
# original. The second matters as much as the first — a break that also changed
# the text would leave the self-test asserting something other than it claims.
#
# `$PATTERNS` and `$KNOWN_INNOCENT` carry no break: measured, neither matches
# the classes, so they are written plainly.
GATE_DATA_NAMES='ROSTER KNOWN_INTERNAL KNOWN_INNOCENT FIXTURE_ACCENT_BODY FIXTURE_ACCENT_NAME FIXTURE_BINARY_BODY FIXTURE_BINARY_NAME FIXTURE_CLEAN_BODY FIXTURE_CLEAN_NAME FIXTURE_DIRTY_BODY FIXTURE_DIRTY_NAME FIXTURE_EMPHASIS_BODY FIXTURE_EMPHASIS_NAME FIXTURE_ESCAPE_BODY FIXTURE_ESCAPE_NAME FIXTURE_ENTITY_BODY FIXTURE_ENTITY_NAME FIXTURE_AMPPROSE_BODY FIXTURE_AMPPROSE_NAME FIXTURE_NBSPPHRASE_BODY FIXTURE_NBSPPHRASE_NAME FIXTURE_RAWHTMLENT_BODY FIXTURE_RAWHTMLENT_NAME FIXTURE_LEGACYSECT_BODY FIXTURE_LEGACYSECT_NAME FIXTURE_ENTITYLANEB_BODY FIXTURE_ENTITYLANEB_NAME FIXTURE_FLAG_BODY FIXTURE_FLAG_NAME FIXTURE_LANEB_BODY FIXTURE_LANEB_NAME FIXTURE_NAMEHIT_BODY FIXTURE_NAMEHIT_NAME'

PATTERNS='ADR-[0-9]+|§[0-9]|[Tt]here (is|are) [Nn][Oo] [A-Za-z][A-Za-z-]*( [A-Za-z-]+){0,2} (harness|harnesses|coverage|tests?|suites?)|(is|are|was|were)(n.{1,3}t| not| never) (tested|covered|scanned|audited|monitored)|(is|are|was|were|remains?) (largely |entirely |still |completely |mostly )?(untested|unmonitored|unaudited|unscanned)|[Nn]o( [A-Za-z][A-Za-z-]*){0,3} (tests?|coverage|scanning|monitoring|harness|harnesses|suites?)( (exists?|existed|remains?|remained|runs?|ran|covers?|covered|exercises?|exercised|guards?|guarded))?( (for|of|in)|[.,;]|$)|[Tt]here (is|are)(n.{1,3}t| not) (any |no )?(harness|harnesses|coverage|tests?|suites?)|[Tt]here (is|are) zero( [A-Za-z][A-Za-z-]*){0,3} (harness|harnesses|coverage|tests?|suites?)|(has|have|had) zero( [A-Za-z][A-Za-z-]*){0,3} (harness|harnesses|coverage|tests?|suites?|monitoring)( (for|of|in)|[.,;]|$)|[Ww]ithout( (automated|manual|unit|integration|end-to-end|regression|any|meaningful))* (harness|harnesses|coverage|tests?|suites?|monitoring)( (for|of|in)|[.,;]|$)|[Nn]obody (looks|checks|monitors)( at| on)?( [A-Za-z][A-Za-z-]*){0,3} (dashboard|dashboards|alert|alerts|log|logs|metric|metrics|queue|queues|report|reports|test|tests|coverage|monitoring)( (for|of|in)|[.,;]|$)|(is|are|was|were)(n.{1,3}t| not| never) under (test|testing|coverage|monitoring|observation)( (for|of|in)|[.,;]|$)|[Ll]acks( any| automated| an?)*( [A-Za-z][A-Za-z-]*)? (harness|harnesses|coverage|tests?|suites?|monitoring)( (for|of|in)|[.,;]|$)|(has|have|had)(n.{1,3}t| not| never) been (tested|covered|scanned|audited|monitored)|(does|do|did)( not|n.{1,3}t) have( any| automated| an?)*( [A-Za-z][A-Za-z-]*){0,2} (harness|harnesses|coverage|tests?|suites?|monitoring)( (for|of|in)|[.,;]|$)|GAP-[0-9]{3}|\bSP-[0-9]{3}\b|\bAC-[A-Z]{2}-[0-9]+|Codex (review|#|[a-z]+#)|[A-Z][A-Z0-9]*(_[A-Z0-9]+)+_(ENABLED|DISABLED|MODE)|\b(main|master|HEAD) @ *`?[0-9a-f]{7,40}'
ROSTER='analytic[]s-service
contro[]l-plane'
KNOWN_INTERNAL='per ADR-[]0000 §[]000
There are no P[]laywright tests for the console.
There are no e[]nd-to-end tests for the purchase flow.
The console has no end-to-e[]nd tests for purchase callbacks.
The console does not have []automated tests.
The console has no t[]ests.
The crash path is un[]tested.
There is NO Pla[]ywright harness in the console repo
the crash path is not []covered by automated tests
no automated[] scanning for that class of input
a bare §[]999c left behind when a record id was stripped
tracked as GAP[]-000 internally
pinned to main @ []0000000
nobody[] looks at that dashboard
tracked as SP-[]999 in the internal board
filed as AC-Q[]A-999 during triage
the console lacks auto[]mated tests
Codex []review
EXAMPLE_SYNTH[]ETIC_FL[]AG_MODE=off
The crash path isn'"'"'t []tested.
The crash path hasn'"'"'t be[]en tested.
The console doesn’t have a[]utomated tests.
The console has never b[]een audited.
The crash path was neve[]r tested.
There are []zero tests for the payment parser.
The payment parser has zero t[]ests.
The parser ships without t[]ests.
The payment parser is not under t[]est.
Without t[]ests for the parser this is a guess.
The crash path is released without automated c[]overage.
There aren'"'"'[][]t any tests for the payment parser.'
KNOWN_INNOCENT='go get github.com/shardpilot/shardpilot-go@v0.6.0-alpha
IngestURL: os.Getenv("SHARDPILOT_INGEST_URL")
POST {IngestURL}/v1/events:batch
https://localhost:8080 during local development
a documented per-platform adaptation, not drift
DEFOLD_SHA1="f735c12192bf95684e6ae1ae27c400b8170fc6d8"
a self-service signup flow, a micro-service boundary
the event plane and the consent plane are separate
an analytics-plane request, zero event batches
No tests fail in CI.
The suite has zero test failures.
The parser runs without test failures in CI.
The parser runs without failing tests in CI.
The release does not have test failures.
The release lacks any test failures.
The tournament lacks contests.
The park lacks protests.
Nobody checks out until the payment transaction succeeds.
The property is not under testamentary restriction.
Nobody monitors tests more closely than the CI team.
Every test suite runs on both toolchains.'
FIXTURE_ACCENT_BODY='internal: contro[]l-plane'
FIXTURE_ACCENT_NAME='café.md'
FIXTURE_BINARY_BODY='see ADR-[]9999 here'
FIXTURE_BINARY_NAME=binary.bin
FIXTURE_CLEAN_BODY='clean *customer* prose about `throughput` and _latency_'
FIXTURE_CLEAN_NAME=clean.md
FIXTURE_DIRTY_BODY='see ADR-[]0000 for context'
FIXTURE_DIRTY_NAME=dirty.md
FIXTURE_EMPHASIS_BODY='see ADR-[]_0000_ for context'
FIXTURE_EMPHASIS_NAME=emphasis.md
FIXTURE_ESCAPE_BODY='see ADR-\*[]0000 for context'
FIXTURE_ESCAPE_NAME=escape.md
FIXTURE_ENTITY_BODY='see &#[]65;DR-[]0000 for context'
FIXTURE_ENTITY_NAME=entity.md
FIXTURE_AMPPROSE_BODY='latency &a[]mp; throughput and a café'
FIXTURE_AMPPROSE_NAME=ampprose.md
FIXTURE_NBSPPHRASE_BODY='There&nb[]sp;are&nb[]sp;no&nb[]sp;tests for the parser.'
FIXTURE_NBSPPHRASE_NAME=nbspphrase.md
FIXTURE_RAWHTMLENT_BODY='<span>&#[]65DR-[]0417</span>'
FIXTURE_RAWHTMLENT_NAME=rawhtmlent.md
FIXTURE_LEGACYSECT_BODY='<span>There&nb[]spare&nb[]spno&nb[]sptests for the parser.</span>'
FIXTURE_LEGACYSECT_NAME=legacysect.md
FIXTURE_ENTITYLANEB_BODY='// GAP[]-000 note, beside the bytes of a reference
package x

const s = "&#[]65;"'
FIXTURE_ENTITYLANEB_NAME=entity_lane_b.go
FIXTURE_FLAG_BODY='EXAMPLE_SYNTH[]ETIC_FL[]AG_ENABLED is off'
FIXTURE_FLAG_NAME=flag.md
FIXTURE_LANEB_BODY='// GAP[]-000 note
package x'
FIXTURE_LANEB_NAME=lane_b.go
FIXTURE_NAMEHIT_BODY='nothing internal in the body'
FIXTURE_NAMEHIT_NAME='ADR-[]9999-notes.md'
for gate_var in $GATE_DATA_NAMES; do
  eval "$gate_var=\"\${$gate_var//\[\]/}\""
done
unset gate_var

# ⚠ EVERY ASSIGNMENT IN THE BLOCK MUST BE ENUMERATED. The audits downstream
# read the DECODED values, and they reach them through the name list — so an
# assignment added to the block and forgotten in the list is decoded by nothing
# and audited by nothing, while the raw scan sees only its broken spelling and
# passes. A fixture carrying a live identifier could sit here indefinitely.
# Checked against the block read from the INDEX, like everything else here.
# ⚠ AND IT MUST READ THE QUOTING, not the line shape. A line INSIDE a
# multi-line value can look exactly like an assignment — one of the innocent
# strings is a pinned revision written as `NAME="..."` — and a line-shape test
# called it an unlisted variable and refused the clean tree on its first run.
# A BACKSLASH ESCAPES INSIDE DOUBLE QUOTES AND NOT INSIDE SINGLE ONES, and a
# machine that ignored that left `\"` looking like a closing quote — the decode
# loop's own `eval` line put it permanently inside a string, so every
# assignment after it was invisible and the check passed anything added there.
# The state machine below carries BOTH quote states across lines: the
# `'\''` idiom leaves single-quote parity looking balanced when it is not, and
# a machine that watched only one of the two ended a multi-line value early
# and refused the clean tree. Only a name at quote depth zero counts.
data_block_names="$(printf '%s\n' "$data_block" | awk '
  BEGIN { inq = 0; ind = 0; sq = sprintf("%c", 39); dq = sprintf("%c", 34); bs = sprintf("%c", 92) }
  {
    # ⚠ AND AN INDENTED ASSIGNMENT IS STILL AN ASSIGNMENT. Bash executes
    # `  NAME=value` exactly as it executes the unindented form, while a
    # start-anchored parser sees nothing — the value is published, decoded by
    # nothing and audited by nothing. The block grammar is one UNINDENTED
    # assignment per line, so an indented one is reported rather than parsed.
    if (inq == 0 && ind == 0 && $0 ~ /^[[:space:]]+[A-Za-z_][A-Za-z0-9_]*=/) {
      iname = $0; sub(/^[[:space:]]+/, "", iname); sub(/=.*/, "", iname)
      print "!CTRL " iname
      assign_line = 0
    } else if (inq == 0 && ind == 0 && $0 ~ /^[A-Za-z_][A-Za-z0-9_]*=/) {
      name = $0; sub(/=.*/, "", name); print name
      assign_line = 1; bare = ""
    } else assign_line = 0
    # Reset unconditionally: a continuation line must start with an EMPTY tail,
    # otherwise it inherits the opener from the line that began the value and
    # reports that as a command after the closing quote.
    bare0 = ""
    i = 1
    while (i <= length($0)) {
      c = substr($0, i, 1)
      if (inq) { if (c == sq) inq = 0; i++; continue }
      if (ind) { if (c == bs) { i += 2; continue } ; if (c == dq) ind = 0; i++; continue }
      if (c == bs) { i += 2; continue }
      # ⚠ AN APOSTROPHE IN A COMMENT IS NOT A QUOTE. The shell stops reading at
      # an unquoted `#`, and a machine that did not desynchronised on the first
      # comment carrying one — every assignment after it vanished from the
      # list, and the duplicate and unlisted checks went blind while the gate
      # stayed green.
      #
      # ⚠ BUT ONLY AT THE START OF A WORD. `#` also trims a parameter
      # expansion, and `${name#prefix}` is not a comment — treating it as one
      # truncates the line there and loses whatever follows, which is the same
      # blindness arriving from the opposite direction. The rule the shell uses is
      # word-start, and that is the rule here too: line start, or after a space.
      if (c == "#" && (i == 1 || substr($0, i - 1, 1) == " " || substr($0, i - 1, 1) == "\t")) break
      # ⚠ ONE ASSIGNMENT PER LINE. Only the assignment at the START of a line
      # is recorded, so a second one on the same line runs, publishes its
      # value, and appears in no list — undecoded and unaudited. The parser
      # cannot follow shell grammar, so the grammar is restricted instead: an
      # assignment line carries no unquoted control operator AND no second
      # `NAME=` behind whitespace, which the shell accepts just as happily.
      if (assign_line && (c == ";" || c == "&" || c == "|")) { print "!CTRL " name; assign_line = 0 }
      if (assign_line) bare = bare c
      # Depth-zero characters of EVERY line, not only an assignment line: the
      # line that CLOSES a multi-line value can carry a command after the
      # closing quote, and the whole-line tests above skip it because it began
      # inside a quote.
      bare0 = bare0 c
      if (c == sq) inq = 1
      else if (c == dq) ind = 1
      i++
    }
    if (assign_line && bare ~ /[[:space:]][A-Za-z_][A-Za-z0-9_]*=/) print "!CTRL " name
    # ⚠ AND THE TAIL OF A LINE THAT CLOSED A MULTI-LINE VALUE. It began inside
    # a quote, so every whole-line test above skipped it, and everything after
    # the closing quote ran unexamined — an assignment appended there is
    # executed, listed nowhere and decoded by nothing.
    # Not conditioned on where the line ENDS: the tail after a closing quote can
    # open a quote of its own, and requiring the line to finish at depth zero
    # let exactly that shape through. (No example is written here: an
    # apostrophe in this comment closes the shell quoting of this very awk
    # program, which is how the check was silently disabled once already.)
    if (was_open == 1 \
        && (bare0 ~ /[;&|]/ || bare0 ~ /[A-Za-z_][A-Za-z0-9_]*=/)) {
      tail = bare0; sub(/^[[:space:]]+/, "", tail)
      print "!CMD " tail
    }
    # ⚠ AND NOTHING BUT ASSIGNMENTS LIVES HERE. Seven rounds of review found
    # seven ways to hide a second assignment from a parser that only looked at
    # the shapes it expected; the eighth would be a plain shell command, which
    # nothing above rejects. So the grammar is stated in full and everything
    # outside it refuses: at quote depth zero this block holds blank lines,
    # comments, unindented assignments, and the four lines of the decode loop.
    # A continuation of a multi-line value is inside a quote and never reaches
    # this test.
    if (inq == 0 && ind == 0 && was_open == 0) {
      probe = $0
      sub(/[[:space:]]+$/, "", probe)
      if (probe != "" \
          && probe !~ /^[[:space:]]*#/ \
          && probe !~ /^[A-Za-z_][A-Za-z0-9_]*=/ \
          && probe !~ /^[[:space:]]+[A-Za-z_][A-Za-z0-9_]*=/ \
          && probe != "for gate_var in $GATE_DATA_NAMES; do" \
          && probe != "done" \
          && probe != "unset gate_var" \
          && probe !~ /^[[:space:]]*eval "\$gate_var=/) {
        print "!CMD " probe
      }
    }
    was_open = (inq || ind)
  }' | sort)"
# ⚠ ASSIGNED ONCE, NOT MERELY LISTED. Deduplicating the names here let a second
# assignment overwrite the first before the values are read: the raw scan sees
# only the broken spelling of the shadowed line and no decoded audit ever reads
# it, so a live identifier could sit in the block indefinitely. The duplicate is
# the finding, so it is reported instead of collapsed.
data_block_dups="$(printf '%s\n' "$data_block_names" | uniq -d | grep . || true)"
if [ -n "$data_block_dups" ]; then
  # refusal:structural
  echo "REFUSING: these data-block names are assigned more than once:" >&2
  printf '%s\n' "$data_block_dups" | sed 's/^/    /' >&2
  echo "  The later assignment wins and the earlier value is decoded by nothing," >&2
  echo "  so anything written in it is published and read by no audit here." >&2
  exit 2
fi
data_block_names="$(printf '%s\n' "$data_block_names" | sort -u)"
# ⚠ THE PARSER MUST HAVE PRODUCED SOMETHING. An awk that fails to parse writes
# nothing and its status is swallowed by the `sort` beside it, so the checks
# below — enumeration, duplicates, one-assignment-per-line, the grammar — all
# go silent together and the gate reports clean. That has happened twice in
# this file, both times from an apostrophe inside a comment in the program. The
# list always contains the name of the list itself; if it does not, the parser
# did not run.
case "
$data_block_names
" in
  *"
GATE_DATA_NAMES
"*) ;;
  *)
    # refusal:structural
    echo "REFUSING: the data-block parser produced no usable names." >&2
    echo "  It always finds GATE_DATA_NAMES at minimum, so an output without it" >&2
    echo "  means the parser did not run — and every check that reads its output" >&2
    echo "  is silent rather than satisfied." >&2
    exit 2
    ;;
esac

while IFS= read -r data_name; do
  [ -n "$data_name" ] || continue
  # ⚠ TWO NAMES ARE EXEMPT, AND FOR STATED REASONS. `GATE_DATA_NAMES` is the
  # list itself. `PATTERNS` carries no visible break — measured: it matches
  # none of the classes — and it is not decoded by the loop, so it cannot be in
  # the list; it is audited instead by the pattern-list rules below, which read
  # it more strictly than any fixture. Nothing else is exempt.
  case "$data_name" in
    '!CMD '*)
      # refusal:structural
      echo "REFUSING: the data block holds a line that is not an assignment:" >&2
      echo "    ${data_name#!CMD }" >&2
      echo "  This block is DATA. Its grammar is blank lines, comments," >&2
      echo "  unindented NAME=value assignments, and the decode loop — nothing" >&2
      echo "  else, because anything else runs while being decoded by nothing" >&2
      echo "  and audited by nothing." >&2
      exit 2
      ;;
    '!CTRL '*)
      # refusal:structural
      echo "REFUSING: the data-block assignment ${data_name#!CTRL } does not stand alone" >&2
      echo "  at the start of its own line. Only such an assignment is recorded," >&2
      echo "  so anything indented, or behind a ';', '&', '|' or a space, runs and" >&2
      echo "  publishes its value while being decoded and audited by nothing." >&2
      echo "  The block grammar is ONE UNINDENTED assignment per line." >&2
      exit 2
      ;;
  esac
  case " GATE_DATA_NAMES PATTERNS $GATE_DATA_NAMES " in
    *" $data_name "*) ;;
    *)
      # refusal:structural
      echo "REFUSING: '$data_name' is assigned inside the data block but is not" >&2
      echo "  listed in GATE_DATA_NAMES. Nothing decodes it and no audit reads" >&2
      echo "  its value, so a live identifier written there would publish and" >&2
      echo "  this gate would report clean. Add it to the list or move it out." >&2
      exit 2
      ;;
  esac
done <<EOF
$data_block_names
EOF


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
CONTAINER_SIGS="$CONTAINER_SIGS"' \067\172\274\257'
# Printable ones, held apart: in source they are ordinary string constants, and
# a refusal ends the run before the lane split could report rather than gate.
# ⚠ EACH PRINTABLE ONE CARRIES THE BYTES THAT MUST FOLLOW IT. Four characters
# are a sentence, not a stream: prose saying that a compressed file begins with
# a particular four-byte marker was refused as an archive. A real stream of
# that kind continues with a fixed block header, and a document about the
# format does not, so the block header is part of the signature here.
# An EMPTY archive carries only the end-of-stream marker, which falls outside
# these nine and is not worth a tenth entry; the tree holds no such file.
# ⚠ THE PRINTABLE COMPRESSION SIGNATURE IS GONE, for the reason the document
# header lost its anywhere-search: ten printable characters are also a quotation.
# Documentation about the format writes the whole block prefix, and a search
# anywhere in the file cannot tell that from a stream. What covers the real
# thing instead: a compressed stream carries NUL bytes and is refused as binary
# below, and the extension is refused by name above. Both are checked; neither
# depends on a sentence not mentioning a format.
CONTAINER_SIGS_TXT=""
# ⚠ THE DOCUMENT HEADER IS NOT IN THIS LIST, and its version bytes did not
# rescue it: seven printable characters are also a sentence about the format,
# and documentation quotes the version too. It is checked separately below,
# where a second invariant can be required alongside it.

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

# EVERY character reference, refused. Not a chosen subset — the first version
# of this picked the ones decoding into the identifier alphabet, and review
# found the edge in both directions within one round: the named reference for
# the section sign reaches a pattern class the alphabet does not contain, and a
# sentence spaced with non-breaking references renders as a gated phrase whose
# raw bytes hold no spaces at all. Every narrowing needs a decode table, and
# the table grows by one entry per round. This does not.
#
# THE COST WAS MEASURED, NOT ESTIMATED, because refusing an innocent entity is
# a real cost to a writer. Across all 26 repositories in this workspace,
# character references appear in markdown TWICE. In these two repositories the
# only occurrence is this gate's own fixture. The input is closed, so refusing
# the family whole costs nothing a writer will notice, and it ends the class
# instead of trading one edge for the next.
#
# THE TERMINATOR IS OPTIONAL ON THE NUMERIC ARMS AND REQUIRED ON THE NAMED ONE,
# and that asymmetry is the third round of review deciding it rather than a
# preference. Round two said a semicolon-less reference is rendered literally by
# GFM, so refusing it is a refusal over text the page shows as typed — true in
# Markdown text. Round three said an HTML parser decodes it anyway, so inside
# raw HTML an unterminated numeric escape still assembles the identifier beside
# it — also true. Which one applies
# depends on whether the position is inside raw HTML, and deciding that is the
# renderer model this gate does not keep.
#
# So the numeric arms refuse both spellings. `&#` followed by digits is not
# something prose contains by accident, so the refusal costs nothing real. The
# named arm keeps the terminator, because an unterminated `&word` IS ordinary
# prose — `AT&Tea` would otherwise be refused.
#
# EVERY LEGACY NAME IS REFUSED UNTERMINATED — all 106 of them — and the three
# revisions it took to get here are the argument for why there is no smaller
# set. Each one picked a criterion and review found a name outside it:
#
#   characters IN the alphabet          -> one name; missed the section sign's
#                                          neighbours entirely
#   characters a READER cannot tell     -> seven names; missed the non-breaking
#     from the alphabet                    space rebuilding a gated PHRASE
#   ...and then the quote, because the phrase classes contain WILDCARDS. With
#   `.{1,3}` in a pattern, ANY character can complete a match, so there is no
#   subset of the table that is safe and no criterion that bounds it.
#
# So the criterion is gone and the enumeration is complete: every named
# reference an HTML parser decodes without a terminator. That is a closed set
# fixed by the standard, not a list anybody maintains, and it is regenerated by
# taking the HTML5 entity table and keeping the names with no trailing
# semicolon. Ordinary prose survives it because a word has to BE one of those
# names — an ampersand followed by "Tea" or "Dept" is not.
#
# DIGIT COUNTS ARE NOT BOUNDED, and the attempt to bound them is why this
# paragraph exists. Review asked for the grammar's limits — seven decimal, six
# hexadecimal — so that an undecodable run is not refused. The bound cannot hold
# beside an optional terminator: a twelve-digit run contains a seven-digit
# prefix, so the bounded pattern matches it anyway and the refusal happens
# regardless. Enforcing the bound needs a trailing-boundary assertion, which is
# the grammar model again. Measured cost of leaving it out: zero, since `&#`
# followed by digits does not occur in this workspace outside this file.
# The alternation below is self-defusing: an opening parenthesis sits where a
# letter would have to be, so this file does not refuse itself over its own
# pattern. That has happened four times in this file's history, and it is why
# every fixture above carries markers.
ENTITY_RE='&#[0-9]+;?|&#[xX][0-9a-fA-F]+;?|&[A-Za-z][A-Za-z0-9]{1,31};|&(Aacute|Agrave|Atilde|Ccedil|Eacute|Egrave|Iacute|Igrave|Ntilde|Oacute|Ograve|Oslash|Otilde|Uacute|Ugrave|Yacute|aacute|agrave|atilde|brvbar|ccedil|curren|divide|eacute|egrave|frac12|frac14|frac34|iacute|igrave|iquest|middot|ntilde|oacute|ograve|oslash|otilde|plusmn|uacute|ugrave|yacute|AElig|Acirc|Aring|Ecirc|Icirc|Ocirc|THORN|Ucirc|acirc|acute|aelig|aring|cedil|ecirc|icirc|iexcl|laquo|micro|ocirc|pound|raquo|szlig|thorn|times|ucirc|Auml|COPY|Euml|Iuml|Ouml|QUOT|Uuml|auml|cent|copy|euml|iuml|macr|nbsp|ordf|ordm|ouml|para|quot|sect|sup1|sup2|sup3|uuml|yuml|AMP|ETH|REG|amp|deg|eth|not|reg|shy|uml|yen|GT|LT|gt|lt)'

AUDIT_CLASSES='ADR-[0-9]+|GAP-[0-9]{3}|SP-[0-9]{3}|AC-[A-Z]{2}-[0-9]+|§[0-9]+'
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
    # ⚠ STRUCTURAL REFUSAL. See the category note in the scope block: it says
    # nothing about content, it says this run is not in a state to report on
    # content. An empty pattern list matches nothing and reports every file
    # clean, which is indistinguishable from a clean repository.
    # refusal:structural
    echo "REFUSING: $required is empty after loading." >&2
    echo "  An empty pattern list matches nothing and reports every file clean." >&2
    exit 2
  fi
done

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
# The material that matches by construction is INLINE in this file and carries
# a visible break instead, so no path is excluded and no marker has to be
# trusted. Region markers were tried first and produced a new variant every
# round; a break is data rather than syntax, and has produced none.
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
scan_lane_b_counts=""
scan_files=0

scan_tree() {
  local root="$1" f hits status line list
  scan_lane_a=""; scan_lane_b_files=0; scan_lane_b_lines=0; scan_files=0
  scan_lane_b_counts=""

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
  gate_tmp; list="$GATE_TMP"
  gate_tmp; blob="$GATE_TMP"
  gate_tmp; md_blob="$GATE_TMP"
  # ⚠ THE TREE A COMMIT WOULD WRITE, NOT THE INDEX LISTING. `git ls-files`
  # includes an INTENT-TO-ADD placeholder — `git add -N draft.png` — which
  # `git write-tree` and `git commit` both omit, so the gate refused a picture
  # no commit was going to carry. Reading the written tree answers the question
  # this file actually asks: what would this commit publish.
  if ! (cd "$root" && git ls-tree -r -z --name-only "$(git write-tree)") > "$list"; then
    rm -f "$list"
    # refusal:structural
    echo "REFUSING: git ls-files failed in '$root'." >&2
    echo "  A partial or absent file list is an UNSCANNED repository, and this" >&2
    echo "  gate must not report one clean." >&2
    exit 2
  fi

  while IFS= read -r -d '' f; do
    # NO PATH IS EXEMPT FROM THIS LOOP, including this script. The
    # by-construction material is inline and broken so that it cannot match
    # itself, which is why nothing here needs an exemption to stay green.
    # THE PATH ITSELF IS PUBLISHED CONTENT. An internal identifier in a file
    # NAME — a decision-record id, a ticket, a service name in a directory —
    # reaches every consumer and appears in no file's body, so scanning only
    # contents misses it entirely.
    # ⚠ AND THE SAME DOMINATING FORM, because a path is read by a person the
    # way a line of prose is. A file NAMED with markers between the characters
    # of a record id was scanned raw and passed, while the identical string in
    # the body was reported — one criterion, two answers, depending on where
    # the bytes sat.
    f_stripped="$(printf '%s' "$f" | tr -d '*_`~\\')"
    if printf '%s\n' "$f" | grep -qE -- "$PATTERNS" \
       || printf '%s\n' "$f" | grep -qiE -- "$ROSTER_RE" \
       || printf '%s\n' "$f_stripped" | grep -qE -- "$PATTERNS" \
       || printf '%s\n' "$f_stripped" | grep -qiE -- "$ROSTER_RE"; then
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
    # ⚠ EVERY EXTENSION TEST USES THIS, NOT `$f`. Shell patterns are
    # case-sensitive and a file system is not: `probe.XPM` and `probe.MD`
    # walked past the image refusal and the Markdown normalisation
    # respectively, which is a rename away from any file that would be caught.
    # ⚠ `x` AND THEN STRIP IT: command substitution removes trailing newlines,
    # and this script goes to lengths elsewhere to carry NUL-delimited paths
    # that may contain them. `notes.PNG` followed by a newline is not a PNG,
    # and folding it into one would refuse a file whose name it misread.
    # `${f,,}` would be simpler and needs bash 4; the local shell here is 3.2.
    flc="$(printf '%s' "$f" | tr '[:upper:]' '[:lower:]'; printf x)"
    flc="${flc%x}"

    # ⚠ AND A PRINTABLE SIGNATURE IN SOURCE IS A STRING CONSTANT. Code that
    # writes a PDF, or names a compression header, holds those characters
    # legitimately — and a refusal ends the run before the lane split, so an
    # unconditional test on printable bytes silently outranks this file's own
    # promise that source is REPORTED rather than gated. The refusals that rest
    # on NON-printable bytes are unambiguous and still apply everywhere; the
    # printable ones are skipped for the reported lane, which is the narrowest
    # place to draw the line.
    #
    # ⚠ DECIDED BEFORE THE FIRST REFUSAL THAT CONSULTS IT. Placed after one, it
    # was an unbound variable under `set -u` and the run died there — with the
    # probes reading that as a pass, because a dead run refuses nothing.
    # ⚠ THE SAME PREDICATE THE go SPLIT USES, deliberately case-SENSITIVE
    # and deliberately `$f`. Folding it here while the split reads `$f` made a
    # file named `logo.GO` exempt from the printable checks and gated by
    # lane A at the same time — a raster renamed that way passed both the
    # extension refusal and the magic one. Two predicates for one question is
    # the defect; which one wins matters less than that they agree.
    printable_sigs=yes
    case "$f" in
      *.go) printable_sigs=no ;;
    esac
    ls_entry="$(cd "$root" && git ls-files -s -z -- "$f" | tr -d '\000')"
    mode="${ls_entry%% *}"
    if [ "$mode" = 160000 ]; then
      # refusal:hazard
      echo "REFUSING: '$f' is a gitlink, so its contents are another repository." >&2
      echo "  Nothing here reads across that boundary, and a clean result would" >&2
      echo "  say nothing about what the submodule publishes." >&2
      exit 2
    fi
    if ! (cd "$root" && git cat-file blob ":$f") > "$blob" 2>/dev/null; then
      # refusal:structural
      echo "REFUSING: the staged blob for '$f' could not be read." >&2
      echo "  It is listed in the index, so a commit would carry it; an" >&2
      echo "  unreadable one cannot be reported clean." >&2
      exit 2
    fi
    scan_files=$((scan_files + 1))
    if [ "$mode" = 120000 ]; then
      link="$(cat "$blob")"
      # ⚠ AND THE MARKER-FREE FORM, like a path and like a body. This branch
      # returns before the content pass, so it was the one place the criterion
      # did not reach: a target decorating an identifier with markers was
      # published in plain sight and reported clean.
      link_stripped="$(printf '%s' "$link" | tr -d '*_`~\\')"
      if printf '%s\n' "$link" | grep -qE -- "$PATTERNS" \
         || printf '%s\n' "$link" | grep -qiE -- "$ROSTER_RE" \
         || printf '%s\n' "$link_stripped" | grep -qE -- "$PATTERNS" \
         || printf '%s\n' "$link_stripped" | grep -qiE -- "$ROSTER_RE"; then
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
    # ⚠ AND AN ASCII RASTER CARRIES NO NUL AT ALL. XPM comes in two shapes and
    # both are printable: the C-source form opening `/* X`, and XPM2 opening
    # `! XPM2` — plain text throughout, so neither the NUL refusal nor a
    # compression signature sees either. XPM is printable C source
    # from its `/* X` header to its pixel array; Netpbm's P1 through P6 hold
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
    # ⚠ AN IMAGE IS REFUSED BY ITS NAME, NOT ONLY BY ITS FIRST BYTES. XPM alone
    # has four valid headers — C source, natural XPM2, XPM1 `#define`, and XPM2
    # Lisp — and two of them are ordinary text a magic test cannot claim
    # without refusing C headers and shell comments as well. Enumerating header
    # spellings is the losing shape; the extension is what someone writes when
    # they commit a picture without thinking, which is the case this gate is
    # for. Measured: ZERO tracked files carry any of these extensions in either
    # tree today.
    #
    # The magic tests stay for a file that lies about its extension. What
    # neither catches is an image whose extension AND first bytes are both
    # innocent — stated, not solved.
    case "$flc" in
      *.png|*.jpg|*.jpeg|*.gif|*.webp|*.bmp|*.tif|*.tiff|*.ico|*.svg|*.xpm|\
      *.xbm|*.ppm|*.pgm|*.pbm|*.pnm|*.pam|*.pcx|*.tga|*.psd|*.ai|*.eps|*.heic|\
      *.heif|*.avif)
        # refusal:hazard
        echo "REFUSING: '$f' is an image, and this gate reads files as text." >&2
        echo "  Pixels are not searchable prose: an identifier drawn in a" >&2
        echo "  picture reads as clean bytes and discloses on the page." >&2
        echo "  Remove it, or extend this gate to read images deliberately." >&2
        exit 2
        ;;
    esac
    # ⚠ THE ONE COMPRESSED FORMAT WITH NO SIGNATURE TO FIND. Every other
    # container here is caught by bytes searched anywhere in the file; a Brotli
    # stream has no header at all, so an extension is the only handle there is.
    # In practice such a blob carries NUL bytes and is refused as binary before
    # this, but "in practice" is not a rule, and this one costs three lines.
    # Measured: zero tracked files carry it in either tree today.
    case "$flc" in
      *.br)
        # refusal:hazard
        echo "REFUSING: '$f' is a Brotli stream, and this gate reads files as text." >&2
        echo "  No pass here decompresses it, so a clean result would say nothing" >&2
        echo "  about what it carries. Remove it, or extend this gate to walk" >&2
        echo "  containers deliberately." >&2
        exit 2
        ;;
    esac
    magic16="$(od -An -tx1 -N16 "$blob" 2>/dev/null | tr -d ' \n')"
    magic4="${magic16:0:8}"
    # The BINARY headers, which cannot be a string constant in readable source
    # and so apply to every file, and the PRINTABLE ones, which can and do not.
    #
    # ⚠ EVERY PRINTABLE SIGNATURE IS A PREFIX OF ORDINARY ENGLISH, so each one
    # below is confirmed by a byte BEYOND the prefix. Four notes in a row were
    # refused as pictures — one opening `! XP`, one opening `P1`, one opening
    # `! XPM2`, one opening `BM` — because a prefix was read as a decision. A
    # false refusal blocks a merge over prose, which is the expensive
    # direction; a container that also lies about its extension is the cheap
    # one, and the extension list above is what actually carries that case.
    case "$magic4" in
      89504e47|ffd8ff*|49492a00|4d4d002a) magic_hit=yes ;;
      # ⚠ `BM` OPENS A SENTENCE ABOUT RANKING. A bitmap's two reserved 16-bit
      # fields at offset 6 are zero in every writer's output; prose has text
      # there.
      424d*)
        case "${magic16:12:8}" in
          00000000) magic_hit="$printable_sigs" ;;
          *)        magic_hit=no ;;
        esac ;;
      # ⚠ THE IMAGE SIGNATURE IS GONE FOR THE SAME REASON AS THE OTHER TWO
      # PRINTABLE ONES. Six printable characters are also a quotation, and a
      # document demonstrating the format writes them in full — the bytes of a
      # quotation and of a header are identical, so nothing further at the byte
      # level can separate them. The extension above refuses this format by name, and a
      # real one carries NUL bytes and is refused as binary below. Third
      # printable signature retired on this reasoning; the pattern is that a
      # signature a document can QUOTE belongs to the extension list, not to a
      # byte search.
      # ⚠ `RIFF` IS ALSO A WORD, AND A SENTENCE ABOUT THE FORMAT CAN CARRY AN
      # UPPER-CASE FOURCC: `RIFF is WEBP format` puts `WEBP` at offset 8
      # exactly where a real one does. A form type is a guess; the SIZE FIELD
      # is the format's own invariant — bytes 4..7 count the bytes that follow
      # them, so a container agrees with its own length and prose does not.
      #
      # A real container whose size field is wrong (a truncated download) is
      # missed by this, and the extension list above is what carries that case.
      52494646)
        magic_riff=no
        if [ ${#magic16} -ge 32 ]; then
          riff_declared=$(( 16#${magic16:14:2}${magic16:12:2}${magic16:10:2}${magic16:8:2} ))
          riff_actual=$(( $(wc -c < "$blob") - 8 ))
          if [ "$riff_declared" -eq "$riff_actual" ]; then magic_riff=yes; fi
        fi
        case "$magic_riff" in
          yes) magic_hit="$printable_sigs" ;;
          *)   magic_hit=no ;;
        esac ;;
      # An executable carries a NUL inside its first 64 bytes. Prose opening
      # `MZ` does not. Read BYTE-ALIGNED: `00` also spans two neighbouring
      # bytes in a flat hex string, which is a match that means nothing.
      4d5a*)
        magic_nul="$(od -An -tx1 -N64 "$blob" 2>/dev/null | tr -s ' \n' ' ')"
        case " $magic_nul " in
          *" 00 "*) magic_hit="$printable_sigs" ;;
          *)        magic_hit=no ;;
        esac ;;
      # ⚠ `/* X` ALSO OPENS AN ORDINARY COMMENT. The XPM C header is `/* XPM */`
      # and all nine bytes are read.
      2f2a2058)
        case "${magic16:0:18}" in
          2f2a2058504d202a2f) magic_hit="$printable_sigs" ;;
          *)                  magic_hit=no ;;
        esac ;;
      # ⚠ FOUR BYTES ARE NOT THE XPM2 SIGNATURE. `! XP` also opens an ordinary
      # note — `! XPrivacy` — and refusing on the prefix blocked a merge over
      # prose. The header is six bytes and six are read.
      21205850)
        case "${magic16:0:12}" in
          212058504d32)
            # ⚠ AND THE MARKER MUST END THE LINE. `! XPM2Factor notes` shares
            # all six bytes and is a note, not a picture. An empty seventh byte
            # is a file that is nothing but the marker, which no prose is.
            case "${magic16:12:2}" in
              0a|0d) magic_hit="$printable_sigs" ;;
              "")    magic_hit="$printable_sigs" ;;
              *)     magic_hit=no ;;
            esac ;;
          *) magic_hit=no ;;
        esac ;;
      # ⚠ NETPBM NEEDS ITS DELIMITER. `P1` through `P6` are a magic number only
      # when whitespace follows; without that test an ordinary note opening
      # `P1-priority planning` was refused as a raster — a false refusal on
      # prose, which is the expensive direction for a gate that blocks merges.
      503[1-6]*)
        # ⚠ EVERY WHITESPACE BYTE THE FORMAT ALLOWS, not the four that came to
        # mind: vertical tab and form feed are legal delimiters too, and a
        # picture using one walked past this while an ordinary note did not.
        case "${magic16:4:2}" in
          09|0a|0b|0c|0d|20)
            # ⚠ AND THE DIMENSIONS MUST FOLLOW. `P1 planning notes` satisfies
            # the delimiter and is a note, not a raster: a Netpbm header
            # continues with an ASCII decimal width. The first non-blank byte
            # after the delimiter decides. (A header comment between the two is
            # legal and is not handled — a picture committed by accident does
            # not carry one, and the extension list above is what covers the
            # one that does.)
            # ⚠ AND BOTH DIMENSIONS, NOT JUST A DIGIT. `P1 2026 planning
            # priorities` opens with a level and a YEAR, which satisfies "a
            # number follows the delimiter" exactly as a width does. A raster
            # header carries a width AND a height, both decimal; the note
            # carries a word where the height would be.
            magic_np=no
            magic_fields=0
            magic_indigit=0
            for magic_i in 6 8 10 12 14 16 18 20 22 24 26 28 30; do
              case "${magic16:$magic_i:2}" in
                09|0a|0b|0c|0d|20)
                  if [ "$magic_indigit" -eq 1 ]; then
                    magic_fields=$((magic_fields + 1)); magic_indigit=0
                  fi ;;
                3[0-9]) magic_indigit=1 ;;
                *)
                  magic_indigit=0; magic_fields=0; break ;;
              esac
              if [ "$magic_fields" -ge 2 ]; then magic_np=yes; break; fi
            done
            if [ "$magic_fields" -ge 2 ]; then magic_np=yes; fi
            case "$magic_np" in
              yes) magic_hit="$printable_sigs" ;;
              *)   magic_hit=no ;;
            esac ;;
          *) magic_hit=no ;;
        esac ;;
      *) magic_hit=no ;;
    esac
    case "$magic_hit" in
      yes)
        # refusal:hazard
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
    sigs="$CONTAINER_SIGS"
    [ "$printable_sigs" = yes ] && sigs="$sigs $CONTAINER_SIGS_TXT"
    # The extension carries the commit-by-accident case for this format, the
    # way it does for pictures — narrowing the signature above is what makes
    # that necessary rather than merely tidy.
    case "$flc" in
      *.bz2|*.gz|*.zip|*.xz|*.zst|*.7z|*.tar|*.tgz|*.rar)
        # refusal:hazard
        echo "REFUSING: '$f' is a compressed archive, and this gate reads files as text." >&2
        echo "  No pass here opens it, so a clean result would say nothing about" >&2
        echo "  what it carries. Remove it, or extend this gate to walk containers" >&2
        echo "  deliberately." >&2
        exit 2
        ;;
      *.pdf)
        # refusal:hazard
        echo "REFUSING: '$f' is a rendered document, and this gate reads files as text." >&2
        echo "  No pass here reads document contents, so a clean result would say" >&2
        echo "  nothing about what it carries. Remove it, or extend this gate to" >&2
        echo "  walk containers deliberately." >&2
        exit 2
        ;;
    esac
    # ⚠ THE DOCUMENT HEADER IS POSITIONAL, and two rounds of trying to make it
    # work anywhere in the file were both wrong. The header alone matched prose
    # about the format; the header plus a terminator matched a note that
    # mentions BOTH tokens, which documentation about the format naturally
    # does. They are common examples, not evidence about the blob.
    #
    # At BYTE ZERO the question is different: a file that literally begins with
    # the header is one. What that gives up is the polyglot — a document behind
    # a preamble — and that is covered in practice by the NUL refusal below,
    # since a real one carries binary streams, and by name through the
    # extension above. Stated, not assumed.
    if [ "$printable_sigs" = yes ] \
       && [ "${magic16:0:10}" = "255044462d" ]; then
      # refusal:hazard
      echo "REFUSING: '$f' BEGINS with a rendered-document header." >&2
      echo "  A file whose first bytes are that header is one, and no pass here" >&2
      echo "  reads document contents. Remove it, or extend this gate to" >&2
      echo "  walk containers deliberately." >&2
      exit 2
    fi
    for sig in $sigs; do
      grep -qaF -- "$(printf "$sig")" "$blob" 2>/dev/null || continue
      # refusal:hazard
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
      # refusal:hazard
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
    # ⚠ A CARRIAGE RETURN SITS BEFORE END-OF-LINE, so every alternative
    # anchored on `$` stops matching in a file with Windows line endings — `No
    # tests` at the end of a CRLF line goes unseen while the same sentence
    # followed by a full stop is caught. The self-test only ever built LF
    # fixtures, so nothing here would have shown it. Stripped per line before
    # any pattern runs, which leaves line COUNT untouched and reported numbers
    # true.
    # ⚠ AND A CR-ONLY FILE HAS NO LINE ENDINGS AT ALL as far as this reads.
    # Classic Mac text separates lines with a bare CR, so the whole blob
    # arrives as ONE line: every alternative anchored on `$` sees only the last
    # phrase in the file, and a disclosure followed by a CR and more prose is
    # invisible. Translated to newlines FIRST, and only when the blob carries a
    # CR and no LF at all — doing it unconditionally would double every line of
    # a CRLF file and make every reported line number wrong.
    if sed 's/\r$//' "$blob" > "$md_blob"; then
      cat "$md_blob" > "$blob"
    else
      # refusal:structural
      echo "REFUSING: could not normalise line endings in '$f'." >&2
      exit 2
    fi
    # ⚠ AND ANY CR STILL STANDING IS A SEPARATOR. The pass above removes the CR
    # of a CRLF pair; whatever is left is a bare CR, which classic Mac text uses
    # as a line ending and which a MIXED file uses beside LF. Either way the
    # bytes after it are a new line to a renderer and were a mid-line
    # continuation to `sed`, so an end-anchored alternative could not see a
    # disclosure that had one behind it. Translated unconditionally, which is a
    # no-op for the LF and CRLF files that make up every tracked file today.
    if LC_ALL=C tr '\015' '\012' < "$blob" > "$md_blob"; then
      cat "$md_blob" > "$blob"
    else
      # refusal:structural
      echo "REFUSING: could not normalise bare carriage returns in '$f'." >&2
      exit 2
    fi
    # The staged blob, so a reported line number is a line number in what a
    # commit would carry. For a path whose tree copy matches its index entry —
    # every path in a fresh checkout — that is the same file.
    scan_src="$blob"
    # ⚠ REFUSED, NOT DECODED — see the scope note. A file carrying a character
    # reference from the family above is refused here, before any pass reads
    # it, because every pass below reads bytes and the page reads the decoded
    # character. Measured on a clone of a public repository before this landed:
    # a plainly-written internal record id is refused and the same id with one
    # letter encoded passes, while the page shows the same string for both.
    #
    # The message says what to do, which is the whole reason this can be a
    # refusal instead of a miss: the remedy is one keystroke, so a false
    # refusal costs a contributor a character rather than an argument.
    # ⚠ LANE B IS REPORTED, NOT GATED, and this refusal has to honour that or it
    # silently promotes a whole lane. A source file may legitimately carry the
    # bytes of a character reference inside a string literal, and a source
    # viewer shows those bytes rather than a decoded character — the hazard
    # this refuses does not exist there. The signature check above already
    # takes the same exemption, on the same line-shape.
    entity_status=1
    case "$f" in
      *.go) ;;
      *)
        entity_hits="$(grep -anoE -- "$ENTITY_RE" "$scan_src" 2>/dev/null)"
        entity_status=$?
        ;;
    esac
    if [ "$entity_status" -gt 1 ]; then
      # refusal:structural
      echo "REFUSING: the character-reference scan of '$f' failed (grep exit $entity_status)." >&2
      echo "  A scan that did not run is not a scan that found nothing." >&2
      exit 2
    fi
    if [ "$entity_status" -eq 0 ]; then
      # refusal:hazard
      echo "REFUSING: '$f' writes a character as a character reference." >&2
      printf '%s\n' "$entity_hits" | head -3 | sed 's/^/    /' >&2
      echo "  Every pass here reads the bytes; the page reads the decoded" >&2
      echo "  character. So an identifier assembled this way is invisible to" >&2
      echo "  this gate and plain to a reader, which is the one shape it must" >&2
      echo "  not let through." >&2
      echo "  Write the character itself. EVERY character reference is refused" >&2
      echo "  here, including harmless ones: choosing a subset needs a decode" >&2
      echo "  table, and two rounds of review each found another entry it was" >&2
      echo "  missing. Across this whole workspace, references appear in" >&2
      echo "  markdown twice, so the whole family costs less than the table." >&2
      echo "  Source files on the reported lane are exempt — their bytes are" >&2
      echo "  what a source viewer shows." >&2
      exit 2
    fi
    # ⚠ THIS GATE DOES NOT MODEL THE RENDERER. It compares against a form that
    # DOMINATES any output the renderer could produce: a copy with every inline
    # marker character removed. Whatever a renderer joins, the marker-free copy
    # joins too, so there is no false miss along this axis BY CONSTRUCTION
    # rather than by testing — and no rule to add when the next construct
    # appears.
    #
    # What stood here before was a normalisation pipeline that tried to predict
    # the rendered string: emphasis pairing, run lengths, whitespace flanking,
    # code-span boundaries, backslash escapes. Five rounds of review, every
    # finding correct, each fix exposing the next construct — because the list
    # of constructs is CommonMark's to close, not this file's. In the same
    # period the pipeline caught ONE real disclosure.
    #
    # The criterion moved with it. It is no longer "does the page render a
    # CONTIGUOUS identifier" but "can a reader SEE one": a record id whose
    # digits are wrapped in emphasis shows the number to anyone reading the
    # page, decorated. A match in the marker-free copy can only happen when the
    # identifier's own characters stand in order, which is the hazard restated.
    #
    # ⚠ AND THIS COMMENT CANNOT CARRY AN EXAMPLE. Under the new criterion the
    # scan sees through the markers, so a decorated id written here would be a
    # disclosure the way an undecorated one is — which the gate demonstrated by
    # refusing the first draft of this paragraph.
    #
    # BOTH copies are scanned, not just the stripped one: the feature-flag class
    # is built from underscores and only survives in the raw text. Measured
    # before this landed — over every lane-A file in both trees, the stripped
    # copy adds ZERO matches the raw text does not already have, and the
    # measurement was itself checked against a planted decorated identifier so
    # that a silent instrument could not read as a clean result.
    #
    # Link syntax is NOT in the stripped set: it is the deliberate-encoding
    # class the scope note puts out of scope, and widening the set here would
    # reopen a decided question sideways. Character references are not in it
    # either, and cannot be — a file carrying one was refused above, so no
    # copy made here ever sees one.
    gate_tmp; strip_blob="$GATE_TMP"
    if ! tr -d '*_`~\\' < "$blob" > "$strip_blob"; then
      # refusal:structural
      echo "REFUSING: could not build the marker-free copy of '$f'." >&2
      exit 2
    fi
    hits_raw="$(grep -anE -- "$PATTERNS" "$scan_src" 2>/dev/null \
      | tr -d '\000'; exit "${PIPESTATUS[0]}")"
    status=$?
    hits_strip="$(grep -anE -- "$PATTERNS" "$strip_blob" 2>/dev/null \
      | tr -d '\000'; exit "${PIPESTATUS[0]}")"
    strip_status=$?
    # ⚠ grep's 1 MEANS "NO MATCH", NOT "FAILED", so the two statuses do not
    # merge by taking the worse: that turned a hit in the raw text into a miss
    # whenever the stripped copy had none, and the fixtures caught it
    # immediately. Error wins over match, match wins over no-match.
    # ⚠ ANY STATUS ABOVE "NO MATCH" IS AN ERROR, not just 2. A grep killed by
    # the kernel exits 137, which is neither 0 nor 2 — treated as "no match" it
    # would let an unscanned file report clean, which is the one outcome this
    # gate must never produce.
    if [ "$status" -gt 1 ] || [ "$strip_status" -gt 1 ]; then status=2
    elif [ "$status" -eq 0 ] || [ "$strip_status" -eq 0 ]; then status=0
    else status=1
    fi
    # The RAW text wins a shared line number, so the report shows what the file
    # actually says rather than a stripped rendering of it.
    # ⚠ AND A FAILURE HERE MUST NOT LOOK LIKE "NO HITS". This runs under
    # `set +e`, so a killed `awk` would empty the list while the grep status
    # above still says 0 — a matched file reported clean by the post-processing
    # of its own match. The status is read and a non-zero one refuses.
    hits="$( { printf '%s\n' "$hits_raw"; printf '%s\n' "$hits_strip"; } \
      | grep -v '^$' | awk -F: '!seen[$1]++' )"
    merge_status=$?
    if [ "$merge_status" -gt 1 ]; then
      set -e
      # refusal:structural
      echo "REFUSING: could not post-process the matches for '$f' (exit $merge_status)." >&2
      echo "  The scan found this file readable and the merge then failed, so an" >&2
      echo "  empty result here means nothing about the file." >&2
      exit 2
    fi
    # THE ROSTER, in its own pass because it is the one class matched
    # case-insensitively: a name capitalised at the start of a sentence is the
    # same disclosure as the lower-case spelling, and folding `-i` into the
    # shape pass
    # would make `[Tt]here is` and the ALLCAPS flag class match prose they
    # were written to leave alone.
    roster_hits_raw="$(grep -aniE -- "$ROSTER_RE" "$scan_src" 2>/dev/null \
      | tr -d '\000'; exit "${PIPESTATUS[0]}")"
    roster_status=$?
    roster_hits_strip="$(grep -aniE -- "$ROSTER_RE" "$strip_blob" 2>/dev/null \
      | tr -d '\000'; exit "${PIPESTATUS[0]}")"
    roster_strip_status=$?
    if [ "$roster_status" -gt 1 ] || [ "$roster_strip_status" -gt 1 ]; then roster_status=2
    elif [ "$roster_status" -eq 0 ] || [ "$roster_strip_status" -eq 0 ]; then roster_status=0
    else roster_status=1
    fi
    roster_hits="$( { printf '%s\n' "$roster_hits_raw"; printf '%s\n' "$roster_hits_strip"; } \
      | grep -v '^$' | awk -F: '!seen[$1]++' )"
    # ⚠ WHAT THIS DOES NOT SEE, stated because a green run gets read as more
    # than it is. The dominating form covers MARKER characters, not WHITESPACE,
    # and a renderer moves whitespace too: a code span drops one space from each
    # end, so an identifier written with spaces inside one renders contiguously
    # and the marker-free copy still has the spaces. Removing whitespace as well
    # would join every neighbouring word in the tree, which is a different and
    # much worse rule. Measured: no tracked file carries that shape.
    #
    # For the same reason every pass here is LINE-ORIENTED, so a phrase broken
    # across a line break — which is what a formatter does to a long sentence —
    # matches nothing. A collapsed-whitespace pass for that existed and was removed
    # along with the rest of the machinery this file had grown, because no
    # tracked file in this repository contains such a wrap. It comes back as
    # its own change, with its own failing test, on the day one does.
    set -e
    for st in "$status" "$roster_status"; do
      if [ "$st" -ge 2 ]; then
        # refusal:structural
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
    # ⚠ DEDUPLICATED BY LINE NUMBER, not by text. One physical line can match
    # the shape pass on its raw bytes and the roster pass on its marker-free
    # copy, and the two records differ as strings — `sort -u` kept both and the
    # lane counts read one line as two.
    [ -n "$roster_hits" ] && hits="$(printf '%s\n%s\n' "$hits" "$roster_hits" \
      | grep -v '^$' | sort -t: -k1,1n | awk -F: '!seen[$1]++')"
    case "$f" in
      *.go)
        scan_lane_b_files=$((scan_lane_b_files + 1))
        while IFS= read -r line; do
          [ -n "$line" ] && scan_lane_b_lines=$((scan_lane_b_lines + 1))
        done <<< "$hits"
        # ⚠ OCCURRENCES, NOT MATCHING LINES, AND THAT WAS A REAL HOLE. The
        # records above are deduplicated by LINE NUMBER -- necessarily, because
        # one physical line can match the shape pass on its raw bytes and the
        # roster pass on its marker-free copy -- and the per-file tally then
        # ticked once per surviving record. So appending a SECOND identifier to
        # an already-matching line moved no number, and the ratchet passed with
        # no baseline edit, because that line's debt was already recorded. The
        # stated rule is that a new occurrence fails; the unit being counted
        # said otherwise, and the two had drifted apart unnoticed.
        #
        # The key that fixes it is (line, marker-free match text): the raw and
        # the marker-free spelling of ONE identifier normalise to the same
        # string and collapse, while two DIFFERENT identifiers on one line stay
        # apart. -o gives the matched text rather than the line.
        #
        # ⚠ MAX ACROSS PASSES, NOT SUM. Every pass reads the same physical line,
        # so summing would multiply one occurrence by however many passes saw
        # it. Taking the largest count any single pass reports for a given
        # (line, text) keeps a genuine repeat -- the same identifier twice on
        # one line -- at two, without inventing copies out of the passes.
        # ⚠ grep's 1 IS "NO MATCH"; ANYTHING ABOVE IT IS A BROKEN INSTRUMENT --
        # the same rule the passes above already keep, and the first draft of
        # this block broke it. `|| [ $? -eq 1 ]` flattens every status into 1,
        # so an I/O error on a file that HAS matches would have produced an
        # empty result, a count of zero, and a ratchet comparing against a
        # number it never managed to compute.
        set +e
        lane_b_occ_1="$(grep -aonE -- "$PATTERNS" "$scan_src" 2>/dev/null | tr -d '\000'; exit "${PIPESTATUS[0]}")"
        lane_b_st_1=$?
        lane_b_occ_2="$(grep -aonE -- "$PATTERNS" "$strip_blob" 2>/dev/null | tr -d '\000'; exit "${PIPESTATUS[0]}")"
        lane_b_st_2=$?
        lane_b_occ_3="$(grep -aoniE -- "$ROSTER_RE" "$scan_src" 2>/dev/null | tr -d '\000'; exit "${PIPESTATUS[0]}")"
        lane_b_st_3=$?
        lane_b_occ_4="$(grep -aoniE -- "$ROSTER_RE" "$strip_blob" 2>/dev/null | tr -d '\000'; exit "${PIPESTATUS[0]}")"
        lane_b_st_4=$?
        set -e
        for st in "$lane_b_st_1" "$lane_b_st_2" "$lane_b_st_3" "$lane_b_st_4"; do
          if [ "$st" -ge 2 ]; then
            # refusal:structural
            echo "REFUSING: grep could not count occurrences in '$f' (exit $st)." >&2
            echo "  A file whose occurrences could not be counted is an UNCOUNTED" >&2
            echo "  file, and this ratchet must not compare against a number it" >&2
            echo "  failed to produce -- least of all the zero that a swallowed" >&2
            echo "  error produces, which reads exactly like paid-off debt." >&2
            exit 2
          fi
        done
        scan_lane_b_this_file="$(
          for pass in 1 2 3 4; do
            eval "printf '%s\n' \"\$lane_b_occ_$pass\"" | sed "s/^/$pass:/"
          done | awk -F: '
            NF > 2 {
              pass = $1; n = $2
              m = substr($0, index($0, ":") + 1)
              m = substr(m, index(m, ":") + 1)
              gsub(/[*_`~\\]/, "", m)
              if (m != "") seen[pass SUBSEP n ":" m]++
            }
            END {
              for (k in seen) {
                split(k, a, SUBSEP)
                if (seen[k] > best[a[2]]) best[a[2]] = seen[k]
              }
              total = 0
              for (k in best) total += best[k]
              print total
            }'
        )"
        # PER-FILE tally, for the ratchet below. Counting per file rather than
        # one grand total is what makes the baseline diffable and the failure
        # legible: a reviewer sees WHICH file grew, not that a number moved.
        #
        # ⚠ COUNT FIRST, PATH LAST, and that order is the point. git permits a
        # path to contain spaces, and `read -r path count` on "a.go 1 b.go 1"
        # would bind count to "1 b.go 1"; the later `-gt` then errors, evaluates
        # FALSE inside its `if`, and the gate passes over a real increase. With
        # the count leading, `read -r count path` gives the rest of the line to
        # the path and the arithmetic always sees a number.
        # $'\n' and not "$(printf '\n')": command substitution STRIPS trailing
        # newlines, so the latter is the empty string and the pattern *""*
        # matches every path. Caught by this gate refusing on a perfectly
        # ordinary filename, which is the failure mode fail-closed buys you.
        case "$f" in
          *$'\n'*)
            # refusal:structural — a newline in a path breaks any line-oriented
            # record, and silently mis-parsing one is how this gate would lie.
            echo "REFUSING: tracked path contains a newline: $f" >&2
            exit 2
            ;;
        esac
        scan_lane_b_counts="${scan_lane_b_counts}${scan_lane_b_this_file} ${f}"$'\n'
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
# ⚠ EVERY PRESENCE SEARCH ASKS THE INDEX (`--cached`), because everything else
# here does. Asked of the working tree it answered about files a commit would
# not contain: staging the removal of the last real occurrences of a roster
# name while leaving the old copies on disk left the search satisfied and the
# corpus about to become that name's sole publisher.
#
# Both halves of the gate, excluded from every presence search as one list.
# The rule asks whether a literal survives ELSEWHERE in the tree; a file that
# exists to hold those literals cannot be part of the answer.
GATE_EXCLUDES=":(exclude)scripts/check_public_surface.sh"

SHAPE_CANARY=zzzcanaryzzz
SHAPE_AWK='  function distinct(body,   j, ch, nx, set, k, cnt) {
    delete set
    j = 1
    while (j <= length(body)) {
      ch = substr(body, j, 1)
      if (substr(body, j + 1, 1) == "-" && j + 2 <= length(body)) {
        nx = substr(body, j + 2, 1)
        if (ch != nx) return 2
        set[ch] = 1; j += 3; continue
      }
      set[ch] = 1; j++
    }
    cnt = 0
    for (k in set) cnt++
    return cnt
  }
  # ⚠ A BRACKET EXPRESSION CAN CONTAIN `]` WITHOUT ENDING. POSIX collating
  # symbols and equivalence classes — `[.a.]`, `[=a=]` — carry their own `]`,
  # so a scan that stops at the first one reads `novel[[.a.]]service` as a
  # class and calls the alternative structural, while the expression matches
  # exactly the literal `novelaservice`. The sub-forms are skipped whole, and a
  # collating symbol standing for one character is reduced to that character
  # before the class is counted. A NAMED class (`[:alpha:]`) is left alone: it
  # really does match more than one character.
  function unsub(b,   o, j, k, kind) {
    o = ""; j = 1
    while (j <= length(b)) {
      if (substr(b, j, 2) == "[." || substr(b, j, 2) == "[=") {
        kind = substr(b, j + 1, 1)
        k = index(substr(b, j + 2), kind "]")
        if (k == 0) { o = o substr(b, j, 2); j += 2; continue }
        o = o substr(b, j + 2, k - 1)
        j = j + 2 + k + 1
        continue
      }
      o = o substr(b, j, 1); j++
    }
    return o
  }
  function shrink(r,   out, i, ch, e, body) {
    out = ""; i = 1
    while (i <= length(r)) {
      ch = substr(r, i, 1)
      if (ch != "[") { out = out ch; i++; continue }
      e = i + 1
      if (substr(r, e, 1) == "^") e++
      if (substr(r, e, 1) == "]") e++
      while (e <= length(r) && substr(r, e, 1) != "]") {
        if (substr(r, e, 2) == "[." || substr(r, e, 2) == "[=" || substr(r, e, 2) == "[:") {
          ch2 = substr(r, e + 1, 1)
          k2 = index(substr(r, e + 2), ch2 "]")
          if (k2 > 0) { e = e + 2 + k2 + 1; continue }
        }
        e++
      }
      body = unsub(substr(r, i + 1, e - i - 1))
      # ⚠ AND A CLASS REPEATED ZERO TIMES CONSUMES NOTHING. `novel[ab]{0}-service`
      # matches exactly the bare literal it is dressed as, while leaving a `[`
      # behind for a test that only looks for one. The class and its quantifier
      # are dropped together.
      if (substr(r, e + 1) ~ /^\{0(,0)?\}/) {
        k3 = index(substr(r, e + 1), "}")
        i = e + 1 + k3
        continue
      }
      if (distinct(body) <= 1) out = out substr(body, 1, 1)
      else out = out "[" body "]"
      i = e + 1
    }
    return out
  }
  # `cpos`/`opos`, NOT `close`/`open`: `close` is an awk BUILT-IN, and using it
  # as a parameter name is a PARSE error — which writes nothing, exits 2, and
  # leaves the caller reading an empty result as a clean pattern list. This
  # whole audit was silent that way and every run stayed green.
  function collapse(r,   cpos, opos, i, body, n, parts, seen, cnt, rep) {
    while (1) {
      cpos = index(r, ")")
      if (cpos == 0) break
      opos = 0
      for (i = cpos - 1; i >= 1; i--) if (substr(r, i, 1) == "(") { opos = i; break }
      if (opos == 0) break
      body = substr(r, opos + 1, cpos - opos - 1)
      if (body ~ /\[/) rep = "|"
      else {
        n = split(body, parts, "|")
        delete seen; cnt = 0
        # ⚠ COMPARED BY WHAT THEY RECOGNISE, NOT BY THEIR TEXT. `-` and `-{1}`
        # are different strings and the same single character, so a literal
        # could be dressed as a branch by quantifying one arm. The cheap
        # normalisations that cover the dodges: a `{1}` repeat means nothing,
        # and a backslash before an ordinary character is the character.
        for (i = 1; i <= n; i++) {
          arm = parts[i]
          gsub(/\{0*1(,0*1)?\}/, "", arm)
          gsub(/\\([^bBwWsSdD<>])/, "\\1", arm)
          if (!(arm in seen)) { seen[arm] = 1; cnt++ }
        }
        rep = (cnt <= 1) ? parts[1] : "|"
      }
      r = substr(r, 1, opos - 1) rep substr(r, cpos + 1)
    }
    return r
  }
  function check(a,   r) {
    if (a == "") return
    r = collapse(shrink(a))
    if (r ~ /[[|]/) return
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
  }'


roster_is_present_in_the_tree() {
  local lit novel=0 found
  gate_tmp; found="$GATE_TMP"
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    # ⚠ CASE-INSENSITIVELY, BECAUSE THE SCAN THAT USES THIS ROSTER IS. The
    # matcher runs `grep -qiE`, so a name capitalised anywhere in the tree is
    # still caught — but a case-SENSITIVE presence lookup would find none of
    # those occurrences and refuse a clean tree for a literal it is scanning
    # for perfectly well. Same defect as the repository lookup below, and it
    # was left here once already on the argument that both halves agreed.
    git grep --cached -l -iF -- "$lit" -- . $GATE_EXCLUDES > "$found" 2>/dev/null || :
    # ⚠ AND A SECOND LOOK AT THE MARKER-FREE FORM, because the SCAN reads that
    # form. Run from THIS repository, not from a scan root: this function runs
    # before any scan and `$root` does not exist here — under `set -u` naming it
    # would have killed the gate outright, in a branch nothing reaches on a
    # tree where the raw lookup succeeds. An unexercised branch is where an
    # undefined name hides. If every occurrence elsewhere in the tree is written with markers
    # between its characters, the raw lookup finds none and a clean tree is
    # refused for a name the matcher is catching perfectly well. Only reached
    # when the raw lookup came back empty, which is rare enough to afford
    # reading the tree once more.
    if [ ! -s "$found" ]; then
      while IFS= read -r cand; do
        [ -n "$cand" ] || continue
        hit="$(git cat-file blob ":$cand" 2>/dev/null \
          | tr -d '*_`~\\' | grep -ciF -- "$lit" || true )"
        if [ "${hit:-0}" -gt 0 ]; then printf '%s\n' "$cand" > "$found"; break; fi
      done <<EOF
$(git ls-files -- . $GATE_EXCLUDES)
EOF
    fi
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
  corpus_values="$(for v in $GATE_DATA_NAMES; do
    eval "printf '%s\n' \"\${$v:-}\""
  done)"

  # ⚠ THE EXTRACTOR TAKES THE WHOLE TOKEN. It read `[a-z][a-z-]*`, so a name
  # carrying a digit was truncated at the digit and the SHORTER prefix — which
  # exists everywhere in this tree — was what got checked. A repository name
  # ending in a digit passed while the literal stayed published.
  # ⚠ THE PATTERN LIST HOLDS SHAPES, AND THAT IS NOW EXECUTED RATHER THAN
  # ASSERTED. A bare alternative — a name with no metacharacter in it — is a
  # roster entry hiding in the one variable no rule covered: the grammar sees a
  # well-formed assignment, and the identifier and repository audits look for
  # their own classes, not for arbitrary words. Adding a literal internal name as an alternative made this
  # file its sole publisher and everything stayed green.
  #
  # ⚠ AND A BRANCH WHOSE ARMS ARE THE SAME IS NOT A BRANCH. `(-|-)` put a `|`
  # in the text without letting the pattern match anything a plain hyphen could
  # not, so a literal name could be dressed as a shape and the character-level
  # audit below could not reconstruct it across the group syntax. Groups are
  # now collapsed innermost-first: identical arms reduce to the arm, and only a
  # group that can genuinely match two different things survives as a branch.
  #
  # Top-level alternatives are split on `|` outside brackets and groups, and
  # each must contain a CHARACTER CLASS THAT CAN MATCH MORE THAN ONE CHARACTER,
  # or a BRANCH. That is a whitelist of what makes a pattern a shape, and it
  # replaced a blacklist of disguises that lost six times running: grouping
  # with nothing to branch between, a one-character class, a backslash before
  # an ordinary character, an exact-count quantifier of one, the same
  # quantifier zero-padded, and a class listing one character twice. Each was
  # approved as proof of shape while grep reconstructed the plain name, and
  # each fix bought exactly one round.
  #
  # Asking for a real class or a branch ends that: a quantifier, an escape and
  # a group are none of those things however they are spelled, and a class is
  # COUNTED rather than merely seen — its distinct characters are expanded,
  # ranges included, so a class of one character reduces to that character.
  #
  # The cost, stated: a structural pattern built only from escapes would be
  # refused and must be written with a class instead. Every alternative in the
  # list today satisfies this on its own.
  #
  # Names belong in the roster, which is checked against the tree.
  # ⚠ ONE SPELLING OF THIS PROGRAM, AND IT IS ASKED TO FLAG SOMETHING ON EVERY
  # RUN. An awk that fails to PARSE writes nothing and exits non-zero into a
  # command substitution, where the status is discarded — so the loop below
  # reads an empty result and reports a clean pattern list. That is exactly
  # what happened: a built-in name used as a parameter silenced this audit
  # completely while every run stayed green, and no output could distinguish it
  # from a pattern list with nothing wrong. A canary that MUST be reported is
  # the only thing that tells those two apart.
  shape_canary="$(printf '%s' "$SHAPE_CANARY" | awk "$SHAPE_AWK" 2>/dev/null || true)"
  if [ "$shape_canary" != "$SHAPE_CANARY" ]; then
    # refusal:structural
    echo "REFUSING: the pattern-shape audit did not report its own canary." >&2
    echo "  It is handed one bare literal alternative that must come back, and" >&2
    echo "  it came back as '$shape_canary'. The audit is not running, so its" >&2
    echo "  silence on the real pattern list means nothing." >&2
    exit 2
  fi

  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    printf 'PROSE VIOLATION: the pattern list holds a bare literal alternative: %s\n' "$lit" >&2
    novel=$((novel + 1))
  done <<EOF
$(printf '%s' "$PATTERNS" | awk "$SHAPE_AWK")
EOF

  # ⚠ READ FROM THE INDEX, both of them. These audits look for literals this
  # gate alone would publish, so they must read the copy a commit carries: a
  # novel repository name staged here and removed from the working copy was
  # invisible to them while the scan read the staged blob.
  #
  # ⚠ AND THE SAME QUESTION ASKED OF THE CHARACTERS, NOT THE STRUCTURE. Every
  # hyphenated literal run in the pattern list must exist elsewhere in this
  # tree, exactly as a roster entry must. `[p]rivate-daemon` reduces to a
  # hyphenated run whatever regex syntax surrounds it, so this holds where a
  # structural test can be dressed around. Character classes are removed first
  # so a class's own contents are not read as a name, and a backslash before an
  # ordinary character is dropped for the same reason the structural test drops
  # it. Measured when written:
  # both pattern lists contain zero such runs, so this costs nothing today and
  # refuses the day one appears.
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    git grep --cached -l -F -- "$lit" -- . $GATE_EXCLUDES > "$found" 2>/dev/null || :
    if [ ! -s "$found" ]; then
      printf 'PROSE VIOLATION: the pattern list carries the literal %s, which is nowhere else in this tree.\n' "$lit" >&2
      novel=$((novel + 1))
    fi
  done <<EOF
$(printf '%s' "$PATTERNS" | sed -e 's/\[[^]]*\]//g' -e 's/\\\([^bBwWsSdD<>]\)/\1/g' \
  | grep -oE '[A-Za-z][A-Za-z0-9]*(-[A-Za-z0-9]+)+' | sort -u)
EOF

  # scan reads this file's prose like any other file's, and the loaded VALUES
  # are read beside it, so a literal that only exists after the break is
  # removed is covered too. Both of this gate's own past disclosures were of
  # exactly this kind:
  # two internal repository names in the roster, and a live decision-record id
  # among the fixtures. A header comment naming a repository that exists
  # nowhere else in the tree was demonstrated to pass everything else.
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
  # ⚠ WHITESPACE AROUND THE SLASH IS REMOVED FIRST. An owner and a repository
  # written with a space beside the slash are as legible to a reader as the
  # tight spelling and matched neither this audit nor the ordinary patterns, so
  # the one file that could report such a name was its sole publisher and
  # stayed green. Not hypothetical: the first run of this rule found one that
  # had been standing in the header comment above, on the default branch of
  # both repositories. Canonicalised before extraction rather than added as a
  # second pattern, so there is one spelling to keep correct.
  #
  # ⚠ CASE-INSENSITIVELY, BECAUSE THE EXTRACTION ABOVE IS. A repository
    # spelled with capitals here and in the ordinary lower case everywhere else
    # would be found nowhere by a case-sensitive lookup, and a clean tree would
    # be refused as a novel disclosure. Repository names are case-insensitive
    # on the host, so the two halves must agree.
    git grep --cached -l -iF -- "$lit" -- . $GATE_EXCLUDES > "$found" 2>/dev/null || :
    if [ ! -s "$found" ]; then
      printf 'PROSE VIOLATION: %s is written in this file and appears nowhere else in this tree.\n' "$lit" >&2
      novel=$((novel + 1))
    fi
  done <<EOF
$( { sed -E 's#([A-Za-z0-9])[[:space:]]+/[[:space:]]*#\1/#g; s#([A-Za-z0-9])/[[:space:]]+#\1/#g' "$SELF_BLOB"
     printf '%s\n' "$corpus_values" \
       | sed -E 's#([A-Za-z0-9])[[:space:]]+/[[:space:]]*#\1/#g; s#([A-Za-z0-9])/[[:space:]]+#\1/#g'
   } | grep -hoiE 'shardpilot/[A-Za-z0-9][A-Za-z0-9._-]*' | sort -u )
EOF

  # Identifiers, in every class the patterns name, admitted by shape alone. A
  # fixture is exactly where a live id gets pasted — someone extending the
  # known-internal corpus reaches for whatever they were looking at.
  #
  # ⚠ READ OVER THE VALUES AS WELL AS THE RAW TEXT. A literal split across
  # adjacent quotes is invisible to a grep of the file and perfectly legible to
  # everyone reading the published fixture, so the loaded values are searched
  # too and the two results are merged.
  # ⚠ AND THIS AUDIT IS ASKED TO EXTRACT SOMETHING ON EVERY RUN. Both greps
  # below feed a `sort -u` whose success masks theirs, so an invalid class list
  # would produce an empty result and the loop would report no live identifiers
  # — indistinguishable from a file that carries none. The canary is written
  # with the visible break, so this line is not itself a disclosure.
  audit_canary="$(printf '%s' 'ADR-[]0000' | sed 's/\[\]//')"
  if [ "$(printf '%s' "$audit_canary" | grep -oE -- "$AUDIT_CLASSES" || true)" != "$audit_canary" ]; then
    # refusal:structural
    echo "REFUSING: the identifier audit could not extract its own canary." >&2
    echo "  The class list is not matching, so this audit finding nothing says" >&2
    echo "  nothing about the file." >&2
    exit 2
  fi
  while IFS= read -r lit; do
    [ -n "$lit" ] || continue
    case "$lit" in EXAMPLE_*) continue ;; esac
    run="$(printf '%s' "$lit" | tr -d '\140')"
    is_sentinel_run "${run##*[!0-9a-f]}" && continue
    printf 'PROSE VIOLATION: %s is a live identifier written where nothing scans.\n' "$lit" >&2
    novel=$((novel + 1))
  done <<EOF
$( { grep -hoE -- "$AUDIT_CLASSES" "$SELF_BLOB"
     printf '%s\n' "$corpus_values" | grep -oE -- "$AUDIT_CLASSES"
     # ⚠ AND THE MARKER-FREE FORM OF THE VALUES. The decorated fixtures added
     # for the rendered-surface probes hold their identifiers with markers
     # between the characters, so the raw read sees no class at all — the one
     # place a live id could be pasted into a fixture and audited by nothing,
     # which is exactly the paste this audit exists to catch.
     printf '%s\n' "$corpus_values" | tr -d '*_`~\\' | grep -oE -- "$AUDIT_CLASSES"; } | sort -u )
EOF

  rm -f "$found"
  if [ "$novel" -ne 0 ]; then
    # refusal:hazard
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
    # refusal:structural
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
  #   the emphasis file an identifier split by an underscore pair, which the
  #                     publishing surface renders contiguously
  #   the escape file   an identifier written with an ESCAPED marker. The page
  #                     shows the marker, so this is not a CONTIGUOUS render —
  #                     and it is still a disclosure, because a reader sees the
  #                     record number. It flipped from must-not-match to
  #                     must-match when the criterion moved, and it is kept
  #                     because it is the case the two criteria disagree on
  #   the flag file     a flag name, whose underscores must survive the
  #                     emphasis pass or the one class built from them stops
  #                     being scannable
  #
  # The filenames are described rather than written: this prose is scanned like
  # any other, and naming the fixtures here would put their identifiers into a
  # part of the file the gate reads.
  # Every name and body comes from the data block above, where the visible
  # break keeps them from matching themselves. This block used to carry the
  # literals directly and needed its own exemption; with them gone it is
  # ordinary code and is scanned like the rest of this file.
  # ⚠ THESE NAMES BECOME REDIRECTION TARGETS. A fixture name carrying a slash
  # — or an absolute path — writes outside the temporary repository, into the
  # real workspace, before anything is scanned. Verified: an absolute name
  # overwrote a staged disclosure with clean prose, after which the self-test
  # reported 6/6 and the gate exited 0 while the index still carried it.
  for fixname in $GATE_DATA_NAMES; do
    case "$fixname" in *_NAME) ;; *) continue ;; esac
    eval "fixval=\${$fixname:-}"
    case "$fixval" in
      ''|.|..|*/*|-*)
        # refusal:structural
        echo "REFUSING: $fixname is not a plain basename." >&2
        echo "  The self-test writes these; a path component would put a fixture" >&2
        echo "  outside the temporary repository and into the real workspace." >&2
        exit 2 ;;
    esac
    if [ "$(printf '%s\n' "$fixseen" | grep -cxF -- "$fixval")" -ne 0 ]; then
      # refusal:structural
      echo "REFUSING: the fixture name $fixval is used twice." >&2
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
    printf '%s\n' "$FIXTURE_EMPHASIS_BODY" > "$FIXTURE_EMPHASIS_NAME"
    printf '%s\n' "$FIXTURE_ESCAPE_BODY"   > "$FIXTURE_ESCAPE_NAME"
    printf '%s\n' "$FIXTURE_FLAG_BODY"     > "$FIXTURE_FLAG_NAME"
    printf '%s\n' "$FIXTURE_ENTITYLANEB_BODY" > "$FIXTURE_ENTITYLANEB_NAME"
    git add -A >/dev/null 2>&1
  )
  scan_tree "$tmp"
  scanned_a="$scan_lane_a"
  rm -rf "$tmp"; trap - RETURN

  local fixture_fail=0 fixture_checks=0
  fixture_checks=$((fixture_checks + 1))
  printf '%s' "$scanned_a" | grep -q '^dirty\.md:' || {
    echo "SELFTEST: the scan missed dirty.md" >&2; fixture_fail=1; }
  # ⚠ THE THREE NORMALISATION FIXTURES, IN BOTH DIRECTIONS. A pass that only
  # asserts what must be FOUND cannot see a normalisation that invents an
  # identifier out of clean prose, and that is the failure this gate pays for
  # in blocked merges.
  fixture_checks=$((fixture_checks + 1))
  printf '%s' "$scanned_a" | grep -q '^emphasis\.md:' || {
    echo "SELFTEST: the scan missed an identifier split by underscore emphasis" >&2; fixture_fail=1; }
  fixture_checks=$((fixture_checks + 1))
  printf '%s' "$scanned_a" | grep -q '^flag\.md:' || {
    echo "SELFTEST: the emphasis pass destroyed a flag name's separators" >&2; fixture_fail=1; }
  fixture_checks=$((fixture_checks + 1))
  printf '%s' "$scanned_a" | grep -q '^escape\.md:' || {
    echo "SELFTEST: the scan missed an identifier written with an escaped marker" >&2; fixture_fail=1; }
  fixture_checks=$((fixture_checks + 1))
  printf '%s' "$scanned_a" | grep -q 'caf' || {
    echo "SELFTEST: the scan missed the non-ASCII path (core.quotePath)" >&2; fixture_fail=1; }
  # ⚠ THE OTHER DIRECTION, AND IT IS THE LANE BOUNDARY. Refusing every character
  # reference is only tolerable while it stays out of the lane this gate reports
  # rather than gates: a source file may carry those bytes in a string literal,
  # and a source viewer shows the bytes. The main tree above contains exactly
  # such a file, so if the refusal ignored the lane split this whole fixture run
  # would have been refused before reaching here — and the arm below would never
  # print. That is why the assertion is on the lane count rather than on a
  # message: the failure it guards against is silence.
  #
  # No pipe here, deliberately: `printf | grep -q` can die on SIGPIPE under
  # pipefail and report 141, which reads as "did not match". For an arm
  # asserting PRESENCE that is harmless; for one asserting absence it would turn
  # a real hit into a pass, which is the direction that matters.
  fixture_checks=$((fixture_checks + 1))
  case "$scanned_a" in
    *"$FIXTURE_ENTITYLANEB_NAME"*)
      echo "SELFTEST: a lane-B source file was reported on the gated lane" >&2
      fixture_fail=1 ;;
  esac
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
  # Its own accumulator and its own trap: the subshell cannot add to the
  # parent's list, and clearing the inherited copy keeps its trap from removing
  # files the parent still needs.
  ( GATE_TMPFILES=(); trap 'gate_rc=$?; rm -f ${GATE_TMPFILES[@]+"${GATE_TMPFILES[@]}"}; exit "$gate_rc"' EXIT
    scan_tree "$nul_tmp" ) >/dev/null 2>&1 || nul_status=$?
  fixture_checks=$((fixture_checks + 1))
  [ "$nul_status" -eq 2 ] || {
    echo "SELFTEST: a NUL-bearing tracked file was not refused" >&2; fixture_fail=1; }
  rm -rf "$nul_tmp"
  # ⚠ THE FIXTURE THAT MADE THIS RULE EXIST. Before the refusal above, this
  # exact tree PASSED while the same identifier written plainly was refused,
  # measured on a clone of a public repository. It gets its own tree for the
  # same reason the NUL one does: a refusal ends the run it happens in.
  # Each in its own tree, for the same reason the NUL fixture gets one: a
  # refusal ends the run it happens in. The second and third are the inputs
  # review produced against the first version of this rule, which refused only
  # the family reaching the identifier alphabet:
  #
  #   - a sentence spaced with non-breaking references renders as a gated
  #     phrase whose raw bytes contain no spaces at those positions;
  #   - an ordinary ampersand in prose is refused too, and that is the stated
  #     cost of refusing the family whole rather than a bug to fix later.
  for entity_case in \
      "$FIXTURE_ENTITY_NAME|$FIXTURE_ENTITY_BODY|an identifier written with a character reference" \
      "$FIXTURE_NBSPPHRASE_NAME|$FIXTURE_NBSPPHRASE_BODY|a gated phrase spaced with non-breaking references" \
      "$FIXTURE_AMPPROSE_NAME|$FIXTURE_AMPPROSE_BODY|an ordinary ampersand entity in prose" \
      "$FIXTURE_RAWHTMLENT_NAME|$FIXTURE_RAWHTMLENT_BODY|a terminator-less numeric reference an HTML parser decodes" \
      "$FIXTURE_LEGACYSECT_NAME|$FIXTURE_LEGACYSECT_BODY|an unterminated legacy reference an HTML parser decodes"; do
    entity_name="${entity_case%%|*}"
    entity_rest="${entity_case#*|}"
    entity_body="${entity_rest%|*}"
    entity_label="${entity_case##*|}"
    entity_tmp="$(mktemp -d)"
    (
      cd "$entity_tmp"
      git init -q .
      git config user.email t@t; git config user.name t
      printf '%s\n' "$entity_body" > "$entity_name"
      git add -A >/dev/null 2>&1
    )
    entity_selftest_status=0
    ( GATE_TMPFILES=(); trap 'gate_rc=$?; rm -f ${GATE_TMPFILES[@]+"${GATE_TMPFILES[@]}"}; exit "$gate_rc"' EXIT
      scan_tree "$entity_tmp" ) >/dev/null 2>&1 || entity_selftest_status=$?
    fixture_checks=$((fixture_checks + 1))
    [ "$entity_selftest_status" -eq 2 ] || {
      echo "SELFTEST: $entity_label was not refused (status $entity_selftest_status)" >&2
      fixture_fail=1; }
    rm -rf "$entity_tmp"
  done

  fixture_checks=$((fixture_checks + 1))
  printf '%s' "$scanned_a" | grep -qF -- "$FIXTURE_NAMEHIT_NAME:path:" || {
    echo "SELFTEST: the scan missed an internal identifier in a PATH NAME" >&2; fixture_fail=1; }
  fixture_checks=$((fixture_checks + 1))
  printf '%s' "$scanned_a" | grep -q '^clean\.md:' && {
    echo "SELFTEST: the scan flagged clean.md" >&2; fixture_fail=1; }
  fixture_checks=$((fixture_checks + 1))
  [ "$scan_lane_b_files" -eq 2 ] || {
    echo "SELFTEST: lane B counted $scan_lane_b_files files, expected 2" >&2; fixture_fail=1; }
  # The ratchet reads scan_lane_b_counts, so the fixture pins that it is
  # actually populated. A tally that silently stayed empty would make the
  # ratchet compare nothing against nothing and report "held" forever -- a gate
  # passing by looking at zero occurrences, which is the failure this script
  # refuses elsewhere by name.
  fixture_checks=$((fixture_checks + 1))
  [ "$(printf '%s' "$scan_lane_b_counts" | grep -c .)" -eq 2 ] || {
    echo "SELFTEST: the per-file lane B tally holds $(printf '%s' "$scan_lane_b_counts" | grep -c .) row(s), expected 2" >&2
    fixture_fail=1; }
  fixture_checks=$((fixture_checks + 1))
  [ "$(printf '%s' "$scan_lane_b_counts" | awk '{n += $1} END {print n + 0}')" -eq "$scan_lane_b_lines" ] || {
    echo "SELFTEST: the per-file tally does not sum to the lane B line count" >&2; fixture_fail=1; }
  # ⚠ A COUNT THAT MUST BE REACHED. Every assertion above is invisible when it
  # is deleted, and a run that asserts nothing prints the same closing line as
  # a run that asserted everything. The floor moves up when assertions are
  # added and refuses when they go.
  [ "$fixture_checks" -ge 17 ] || {
    echo "SELFTEST: only $fixture_checks scan assertion(s) ran, expected at least 17" >&2
    fixture_fail=1; }
  if [ "$fixture_fail" -ne 0 ]; then
    # refusal:structural
    echo "REFUSING: the scan failed its own fixture." >&2
    exit 2
  fi
  echo "self-test: OK — $tested known-internal string(s) matched, $innocent innocent string(s) passed, $fixture_checks scan assertion(s)"
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
  # refusal:structural
  echo "REFUSING: the scan processed zero files." >&2
  echo "  A real checkout is never empty, so this means git ls-files failed or" >&2
  echo "  this is not a checkout. Reporting 'clean' here would be a gate that" >&2
  echo "  passes by looking at nothing." >&2
  exit 2
fi

echo
echo "LANE B (REPORTED AT ZERO, GATED ON CHANGE) — Go source: ${scan_lane_b_files} file(s) WITH A MATCH, ${scan_lane_b_lines} matching line(s). The RATCHET holds occurrences, which is the larger number when a line carries more than one; both are printed because only one of them is gated. Not a count of Go files scanned: a clean one is not counted here."
if [ "$scan_lane_b_files" -eq 0 ]; then
  echo "  LANE B IS EMPTY. The debt this lane tracked is paid: fold *.go into lane A"
  echo "  and delete this section, so the scope note stops describing a gap that"
  echo "  no longer exists."
else
  echo "  Doc comments here publish verbatim to pkg.go.dev. They are owed work, not"
  echo "  accepted risk, and they are not gated AT ZERO here because this repository's Go"
  echo "  sources are owned by the SDK wire-freeze workstream. The count is lines"
  echo "  MATCHING THE PATTERNS ABOVE — not a total of internal material, which the"
  echo "  scope note above says these shapes cannot bound."
fi

echo
echo
# ⚠ LANE A IS JUDGED FIRST, and the ratchet below depends on that ordering.
# --write-baseline exits early on success, so running it here rather than above
# means the command recommended for paying lane B debt can no longer report
# success over an unreported lane A violation on the gated surface.
if [ -n "$scan_lane_a" ]; then
  echo "FAIL — internal material in the published non-source surface:" >&2
  printf '%s' "$scan_lane_a" >&2
  exit 1
fi

# ── LANE B RATCHET ────────────────────────────────────────────────────────────
# Lane B cannot be gated AT ZERO today: 33 matching lines already exist, and
# failing on them would break every build until that debt is paid. That is why
# this lane reports. But a report is only useful if someone can see it, and a
# 34th line arriving among 33 existing ones is invisible — not because nobody
# looked, but because there is nothing there to see. That is the actual
# mechanism by which internal paths reached this public SDK twice.
#
# So the lane is gated on CHANGE instead of on zero. The number may fall and may
# not rise. Existing debt costs nothing; a new occurrence fails immediately.
#
# ⚠ WHAT THIS DOES NOT CATCH, stated rather than implied: the counts are PER
# FILE, so removing one matching line and adding another in the SAME file in the
# same change nets to zero and passes. Identity pinning (hash per occurrence)
# would close that, at the price of failing every reword of an already-grandfathered
# line. The swap is a deliberate act visible in the diff; the reword is an
# accident that would train people to edit the baseline. This trades the rarer
# hole for the commoner false alarm, on purpose.
#
# ⚠ THE BASELINE IS NOT AN ESCAPE HATCH. Raising a number in it is checked
# against the merge target below, because a ratchet whose baseline can be edited
# upward in the same change is not a ratchet — it is a comment.
LANE_B_BASELINE="${LANE_B_BASELINE:-scripts/public-surface-lane-b-baseline.txt}"

# ⚠ VALIDATED HERE BECAUSE --write-baseline NEVER REACHES WHAT FOLLOWS IT. The
# writer redirects into this path and then `exit 0`s, so every check placed
# below that branch is not LATE for the writing path -- it is UNREACHABLE from
# it. And the path is env-overridable one line above, so an arbitrary target
# reaches the redirect.
#
# ⚠ THE SPELLING IS DERIVED, NOT VETTED. This was a list of forms git cannot
# consume as `:<path>` -- and it grew a fifth entry in as many rounds, because a
# list of refusals is open by construction: `//`, `/./`, `..`, an absolute path,
# a trailing slash, and whatever the next reader finds. Two of those I found in
# my own list an hour after writing it.
#
# So the guard stops judging the spelling and computes the canonical one instead.
# From the directory it has already resolved, `git rev-parse --show-prefix` gives
# the worktree-relative prefix, and the leaf completes it. Measured -- every
# spelling below collapses to the same path, which git then reads:
#
#   scripts/b.txt · ./scripts/b.txt · scripts//b.txt · scripts/./b.txt
#   scripts/../scripts/b.txt · /abs/.../scripts/b.txt · scripts/b.txt/
#
# A refusal for each of those is a patch; deriving one answer is a shape. What
# remains is not a spelling question at all: the result must name a FILE, and a
# path whose leaf is a directory (`scripts/`) derives to a directory -- caught by
# the regular-file check below, which exists anyway.
# ⚠ A SYMLINK IS NOT A BASELINE, and `-f` cannot tell you so -- it FOLLOWS the
# link. Replace this file with a link to another tracked baseline and every
# current-tree read follows it, so the change passes; but `git show <ref>:<path>`
# on the merge target returns the LINK BLOB, the target's pathname, and every
# later PR is then reported as adding paths the target does not have. This one
# is an INDEX question, so it is asked of the index and stays out of the
# filesystem guard below.

lane_b_root="$(cd -P "$(git rev-parse --show-toplevel)" && pwd -P)"
# ⚠ BOTH ADMINISTRATIVE DIRECTORIES. `--git-dir` is the per-worktree one; with a
# linked worktree it can sit OUTSIDE the checkout while `--git-common-dir` --
# holding config, objects and refs -- sits inside it. Excluding only the first
# leaves the shared half writable through this override. They are the same path
# in an ordinary checkout, which is why one test looked sufficient.
lane_b_gitdir="$(cd -P "$(git rev-parse --git-dir)" && pwd -P)"
lane_b_commondir="$(cd -P "$(git rev-parse --git-common-dir)" && pwd -P)"

# ⚠ ONE SUBSHELL VALIDATES AND WRITES, AND THAT IS THE POINT -- not tidiness.
#
# Two defects share this shape and one movement closes both:
#
#   `cd` IS LOGICAL BY DEFAULT. It collapses `link/..` textually and lands
#   somewhere else than the kernel would, while `pwd -P` then honestly prints
#   the physical path OF THE WRONG DIRECTORY. Measured: with `link -> outside`
#   and `escape/` existing on both sides, `link/../escape` resolved to
#   `repo/escape` -- containment passed -- while the redirect wrote to
#   `outside/escape/out`, and `repo/escape/out` was never created. The `-P`
#   belongs on `cd`, which chooses the directory, not only on `pwd`, which
#   reports it.
#
#   AND A VALIDATED PATHNAME IS NOT A VALIDATED DIRECTORY. Between the check and
#   the redirect, another process can rename the in-tree parent and put a
#   symlink in its place; re-resolving the same pathname later then reaches
#   somewhere else. So the write does not re-resolve: after `cd -P` the
#   directory is held by the process's own working directory -- a kernel
#   reference, not a name -- and only the final component is handed to the
#   redirect.
#
# That is a hazard REMOVED rather than assumed away, which is the order this
# repository keeps: establish, refuse, remove, and only then assume.
lane_b_guard() {  # $1 = check | write
  (
    cd -P "$(dirname "$LANE_B_BASELINE")" 2>/dev/null || {
      echo "REFUSING: could not resolve the directory holding $LANE_B_BASELINE." >&2
      exit 2
    }
    lane_b_here="$(pwd -P)"
    case "$lane_b_here/" in
      "$lane_b_root"/*) : ;;
      *)
        # refusal:structural
        echo "REFUSING: LANE_B_BASELINE resolves outside this repository." >&2
        echo "  Given:    $LANE_B_BASELINE" >&2
        echo "  Resolves: $lane_b_here" >&2
        echo "  --write-baseline redirects into it, so a path that leaves the" >&2
        echo "  worktree -- directly, or through a symlinked parent -- would" >&2
        echo "  overwrite a file this gate has no business touching." >&2
        exit 2
        ;;
    esac
    # ⚠ INSIDE THE WORKTREE IS NOT ENOUGH: `.git` resolves beneath the root too.
    # `LANE_B_BASELINE=.git/index` passes containment, carries no index mode, and
    # is a one-link regular file -- so the writer would truncate git's live index
    # and report success. Measured. The administrative directory is excluded by
    # its resolved path, not by its name, so a `.git` file or a linked worktree
    # is covered by the same test.
    case "$lane_b_here/" in
      "$lane_b_gitdir"/*|"$lane_b_gitdir/"|"$lane_b_commondir"/*|"$lane_b_commondir/")
        # refusal:structural
        echo "REFUSING: LANE_B_BASELINE resolves inside the git directory." >&2
        echo "  Resolves: $lane_b_here" >&2
        echo "  --write-baseline would truncate repository administrative state." >&2
        exit 2
        ;;
    esac
    lane_b_leaf="$(basename "$LANE_B_BASELINE")"
    if [ -L "$lane_b_leaf" ]; then
      # refusal:structural
      echo "REFUSING: $LANE_B_BASELINE is a symlink in the working tree." >&2
      echo "  --write-baseline would follow it and write through to its target." >&2
      exit 2
    fi
    if [ -e "$lane_b_leaf" ]; then
      if [ ! -f "$lane_b_leaf" ]; then
        # refusal:structural
        echo "REFUSING: $LANE_B_BASELINE is not a regular file." >&2
        exit 2
      fi
      # A hard link is a second name for one inode, and a redirect writes
      # THROUGH it: the other name changes too, and nothing here can put it
      # back. `ls -ld` field 2 is the link count on GNU and BSD alike; `stat`
      # spells it differently on each.
      lane_b_links="$(ls -ld "$lane_b_leaf" 2>/dev/null | awk '{print $2}')"
      case "${lane_b_links:-}" in
        ''|*[!0-9]*)
          # refusal:structural
          echo "REFUSING: could not read the hard-link count of $LANE_B_BASELINE." >&2
          exit 2
          ;;
      esac
      if [ "$lane_b_links" -gt 1 ]; then
        # refusal:structural
        echo "REFUSING: $LANE_B_BASELINE has $lane_b_links hard links." >&2
        echo "  --write-baseline writes through the inode, so every other name" >&2
        echo "  for it would change and this gate could put none of them back." >&2
        exit 2
      fi
    fi
    if [ "$1" = canon ]; then
      # ⚠ THE PREFIX COMES FROM THE PAIR WE ALREADY HOLD, NOT FROM A FRESH
      # DISCOVERY. `git rev-parse --show-prefix` run from the resolved directory
      # asks "which repository am I in", and inside a submodule or an embedded
      # repository the answer is the NEARER one -- so `a/q/baseline` under a
      # nested repo at `a` derives as `q/baseline`, which the outer root then
      # resolves to a different file entirely. Containment above has already
      # established that this directory is under the outer root, both physically
      # resolved, so the prefix is the difference between two strings we hold and
      # no repository needs to be discovered again.
      # ⚠ QUOTED, BECAUSE THE RIGHT SIDE OF # IS A PATTERN, NOT A STRING. An
      # unquoted expansion here is glob-matched: a repository living under a
      # path containing `[`...`]` fails to strip at all, and the derived value
      # keeps the whole absolute prefix. Measured -- `br[ack]ets` in the root
      # path left the canonical path as the entire absolute location minus its
      # leading slash, which git then cannot resolve. `*` and `?` happened to
      # survive, which is worse than failing: it made the bug look absent.
      lane_b_prefix="${lane_b_here#"$lane_b_root"}"
      lane_b_prefix="${lane_b_prefix#/}"
      if [ -n "$lane_b_prefix" ]; then
        printf '%s/%s\n' "$lane_b_prefix" "$lane_b_leaf"
      else
        printf '%s\n' "$lane_b_leaf"
      fi
    fi
    if [ "$1" = write ]; then
      # ⚠ WRITTEN ASIDE AND RENAMED, NOT OPENED BY NAME. The held cwd anchors the
      # PARENT; the leaf is still resolved at redirect time, so a process that
      # swaps it for a symlink after the checks above would have the write follow
      # it out of the tree. `mv` renames over the name itself and never follows a
      # symlink standing there, so the swap loses instead of winning.
      {
        echo "# Lane B occurrences per file, as of the commit that wrote this."
        echo "# Written by: scripts/check_public_surface.sh --write-baseline"
        echo "#"
        echo "# THIS FILE ONLY EVER SHRINKS. The scan fails when a count here rises,"
        echo "# when a file appears that is not here, and when a number here is higher"
        echo "# than the same number on the merge target. It also fails when a count"
        echo "# FALLS without this file being updated, so paying debt is recorded"
        echo "# rather than silently banked as slack."
        echo "#"
        echo "# format-version: 3"
        echo "# Format: <occurrences> <path> -- count FIRST so a path may contain spaces"
        echo "#"
        echo "# format 3 counts OCCURRENCES; format 2 counted MATCHING LINES. Two"
        echo "# identifiers on one line were one tick in format 2, so a second one"
        echo "# could be added to an existing line without any number moving. The"
        echo "# version moved because the numbers mean something different, not"
        echo "# because the layout changed -- reading a format-2 file with this"
        echo "# parser would understate every count that has such a line."
        printf '%s\n' "$lane_b_now"
      } > "$lane_b_leaf.tmp.$$" || {
        # ⚠ CHECKED EXPLICITLY, BECAUSE `set -e` IS NOT IN FORCE HERE. This
        # function is invoked on the left of `||`, which suspends errexit for
        # its whole body -- so a serialisation that dies partway (ENOSPC, a
        # quota, a full pipe) would fall through to the rename below, and the
        # rename SUCCEEDS on a partially written file. The gate would then print
        # WROTE and exit 0 over a truncated baseline, which is worse than any
        # refusal: the next run compares against a number nobody computed and
        # reads the missing rows as debt that was paid.
        echo "REFUSING: could not write the baseline (serialisation failed)." >&2
        echo "  The partial file is removed rather than renamed into place." >&2
        rm -f "$lane_b_leaf.tmp.$$"
        exit 2
      }
      mv -f "$lane_b_leaf.tmp.$$" "$lane_b_leaf" || {
        echo "REFUSING: could not put the new baseline in place." >&2
        rm -f "$lane_b_leaf.tmp.$$"
        exit 2
      }
    fi
  )
}
lane_b_canon="$(lane_b_guard canon)" || exit $?
# Everything downstream -- the index read, the mode probe, every message -- now
# speaks the derived spelling rather than whatever was handed in.
LANE_B_BASELINE="$lane_b_canon"

# ⚠ PROBED WITH THE DERIVED SPELLING, AND ONLY AFTER DERIVING IT. Asked of the
# spelling as given, `git ls-files -s -- "scripts/baseline.txt/"` returns no
# entry at all -- so an index carrying a symlink while the working tree carries a
# regular file slipped past this refusal entirely, and the writer then succeeded
# against the working file while the index still held the link blob. The probe is
# about the index, so it has to speak the name the index uses.
lane_b_mode="$(git ls-files -s -- "$LANE_B_BASELINE" | awk '{print $1}' || true)"
case "${lane_b_mode:-}" in
  120000)
    # refusal:structural
    echo "REFUSING: $LANE_B_BASELINE is a symlink in the index." >&2
    echo "  The working tree would follow it and the merge target would not," >&2
    echo "  so the two sides of this ratchet would compare different files." >&2
    exit 2
    ;;
esac

lane_b_now="$(printf '%s' "$scan_lane_b_counts" | { grep -v '^$' || [ $? -eq 1 ]; } | sort -k2)"



if [ "${1:-}" = "--write-baseline" ]; then
  lane_b_guard write || exit $?
  echo "WROTE $LANE_B_BASELINE"
  # The EXIT trap treats rc=0 without this as a run that died mid-flight, which
  # is exactly right for every other early return here. Writing the baseline is
  # the one legitimate short path, so it says so rather than tripping the
  # dead-run detector and printing a refusal over a success.
  gate_finished=yes
  exit 0
fi

# ⚠ READ FROM THE INDEX, LIKE EVERY OTHER INPUT THIS GATE COMPARES. scan_tree
# lists paths from `git write-tree` and reads each blob with
# `git cat-file blob :<path>` -- deliberately, because the question this file
# asks is what a commit would PUBLISH. The baseline was the one input still read
# from the working tree, so staging a source change while leaving the matching
# baseline edit unstaged compared a staged tree against an unstaged baseline and
# passed -- while the commit being built still carried the stale one. Two sides,
# two universes, and the gate reported on neither.
gate_tmp; lane_b_base_blob="$GATE_TMP"
if ! git cat-file blob ":$LANE_B_BASELINE" > "$lane_b_base_blob" 2>/dev/null; then
  # refusal:structural — fail closed. An absent baseline is indistinguishable
  # from a deleted one, and treating it as "nothing to compare" would let anyone
  # disable this gate with rm. Absent FROM THE INDEX is the reading that matters
  # now: a baseline written but never staged is not yet the file this commit
  # would carry, and saying so is more useful than comparing against it.
  echo "REFUSING: $LANE_B_BASELINE is missing from the index." >&2
  echo "  Regenerate it with: $0 --write-baseline" >&2
  echo "  If you just regenerated it, stage it: the gate reads what would be" >&2
  echo "  committed, not what is sitting in the working tree." >&2
  exit 2
fi

# Same rule, and this is the read the P2 above is really about: `-f` passed, so
# the file exists; a failure here is permission or I/O, not absence.
# ⚠ THE TARGET'S COPY IS FROM AN EARLIER COMMIT, so it can predate a change to
# this format. Version 1 was "<path> <occurrences>"; reading one of those with
# the version 2 parser binds the PATH to the count field and reports about the
# wrong file -- observed, not imagined, while testing this very change. A
# marker turns that into a refusal instead of a confident wrong answer.
LANE_B_FORMAT=3
# Same rule as above: grep's 1 is "no such line" and is legitimate (a format-1
# file has no marker); anything higher is a read failure and must not arrive
# here disguised as an old format.
lane_b_version="$(grep -m1 '^# format-version:' "$lane_b_base_blob" || [ $? -eq 1 ])"
lane_b_version="$(printf '%s' "$lane_b_version" | tr -dc '0-9')"
if [ "${lane_b_version:-1}" != "$LANE_B_FORMAT" ]; then
  # refusal:structural
  echo "REFUSING: $LANE_B_BASELINE is format ${lane_b_version:-1}, this script reads $LANE_B_FORMAT." >&2
  echo "  Regenerate it with: $0 --write-baseline" >&2
  exit 2
fi

# ⚠ BOTH GREPS, NOT JUST THE SECOND. A comments-only baseline -- the paid state
# this lane exists to reach -- makes the FIRST filter exit 1, and pipefail kills
# the assignment before the second one's careful handling is ever reached. Fixing
# the downstream filter and leaving the upstream one was fixing the half I was
# looking at.
lane_b_base="$({ grep -v '^#' "$lane_b_base_blob" || [ $? -eq 1 ]; } \
  | { grep -v '^$' || [ $? -eq 1 ]; } | sort -k2)"

if [ "$lane_b_now" != "$lane_b_base" ]; then
  echo "FAIL — lane B moved and the baseline did not agree:" >&2
  # Two directions, named separately, because the fix differs.
  diff <(printf '%s\n' "$lane_b_base") <(printf '%s\n' "$lane_b_now") \
    | grep -E '^[<>]' >&2 || true
  echo "  '>' is what the scan found now; '<' is what the baseline expects." >&2
  echo "  A count that ROSE, or a file that is new here, is new internal material" >&2
  echo "  in a public SDK: remove the reference. Do not re-baseline it." >&2
  echo "  A count that FELL is debt you paid: rerun with --write-baseline and" >&2
  echo "  commit the smaller file in this same change." >&2
  exit 1
fi

# The anti-cheat. Without this, the two rules above are satisfiable by editing
# the baseline upward in the same commit that adds the occurrence.
if [ -n "${PUBLIC_SURFACE_BASE_REF:-}" ]; then
  # ⚠ TWO REASONS THE COMPARISON CAN BE UNAVAILABLE, and they are not the same
  # fact. A ref that does not RESOLVE is a broken instrument -- a shallow clone
  # that never fetched the base -- and reporting "skipped" for it would let the
  # anti-cheat evaporate exactly where it is needed. A ref that resolves but
  # carries no baseline is a true statement about the world: the target predates
  # this file, which is the case on the change that introduces it.
  if ! git rev-parse --verify --quiet "${PUBLIC_SURFACE_BASE_REF}^{commit}" >/dev/null; then
    # refusal:structural
    echo "REFUSING: PUBLIC_SURFACE_BASE_REF=${PUBLIC_SURFACE_BASE_REF} does not resolve." >&2
    echo "  The baseline-vs-target check cannot run, and skipping it silently is" >&2
    echo "  how a ratchet becomes a comment. Fetch the base ref, or unset the" >&2
    echo "  variable deliberately if there is genuinely nothing to compare." >&2
    exit 2
  fi
  # ⚠ SET BEFORE ASKED. `set -u` makes an unassigned base_copy fatal at the
  # first expansion, and the branch that leaves it unassigned is precisely the
  # advertised legal skip -- a target that predates this file. So the skip
  # aborted the run instead of skipping, on every change that introduces the
  # baseline, this one included. Observed as a red `public-surface` on the merge
  # run while the branch run stayed green, because only the merge run has a base.
  base_copy=""

  # ⚠ THE PROBE ITSELF MUST BE UNAMBIGUOUS -- this is the sixth time on this
  # change that a "nothing came back" was read as "there is nothing". Patching
  # each site was not working, so the SHAPE of the question changed instead.
  #
  # `cat-file -e` fails for BOTH a missing path and a broken lookup, so its
  # nonzero says nothing on its own. `ls-tree <ref> -- <path>` separates them:
  # it exits 0 whether or not the path is there and prints a line only when it
  # is, so success-with-empty-output IS absence and a nonzero IS an instrument
  # failure. One question, one meaning per answer.
  if ! base_listing="$(git ls-tree --name-only "${PUBLIC_SURFACE_BASE_REF}" -- "${LANE_B_BASELINE}")"; then
    # refusal:structural
    echo "REFUSING: could not list ${LANE_B_BASELINE} on ${PUBLIC_SURFACE_BASE_REF}." >&2
    echo "  That is a broken object store or an unreadable tree, not an absent" >&2
    echo "  baseline, and skipping the anti-cheat on it is how this gate stops" >&2
    echo "  being one." >&2
    exit 2
  fi
  if [ -n "$base_listing" ]; then
    if ! base_copy="$(git show "${PUBLIC_SURFACE_BASE_REF}:${LANE_B_BASELINE}")"; then
      # refusal:structural
      echo "REFUSING: ${PUBLIC_SURFACE_BASE_REF}:${LANE_B_BASELINE} is listed but could not be read." >&2
      exit 2
    fi
  fi
  # ⚠ THE TARGET'S FORMAT, ASKED BEFORE ITS ROWS ARE READ. I had this check and
  # lost it while restructuring the probe: a format-1 target parsed by the
  # format-2 reader binds every path to the count field, so every current path
  # looks absent and an untouched PR fails. A skew is not a finding about the
  # code; it is announced and skipped, which is the same treatment a target that
  # predates the file gets.
  if [ -n "$base_copy" ]; then
    base_version="$(printf '%s' "$base_copy" | { grep -m1 '^# format-version:' || [ $? -eq 1 ]; } | tr -dc '0-9')"
    if [ "${base_version:-1}" != "$LANE_B_FORMAT" ]; then
      echo "  (baseline-vs-target check skipped: target baseline is format ${base_version:-1}, this script reads $LANE_B_FORMAT)"
      base_copy=""
    fi
  fi
  if [ -n "$base_copy" ]; then
    # ⚠ IFS= AND A MANUAL SPLIT. `read -r count path` uses the default
    # whitespace IFS, which strips LEADING spaces from the last field: a file
    # named " config.go" serializes as "7  config.go" and would be read as
    # path "config.go", matching the ordinary file's target entry and passing.
    # git permits that filename. The count is everything before the first
    # space; the path is everything after it, byte for byte.
    while IFS= read -r row; do
      [ -n "$row" ] || continue
      count="${row%% *}"
      path="${row#* }"
      [ -n "$path" ] || continue
      # ⚠ NO awk -v FOR THE PATH. A -v assignment PROCESSES BACKSLASH ESCAPES,
      # so a tracked `foo\141.go` would be decoded to `fooa.go` and borrow that
      # file's count -- an increase passing as an existing entry. The comparison
      # is done in the shell instead, where the two strings are compared byte
      # for byte and nothing interprets anything.
      was=""
      while IFS= read -r brow; do
        case "$brow" in '#'*|'') continue ;; esac
        if [ "${brow#* }" = "$path" ]; then
          was="${brow%% *}"
          break
        fi
      done <<< "$base_copy"
      if [ -z "$was" ]; then
        # ⚠ ABSENT IS ZERO, NOT "NOTHING TO COMPARE". Skipping here was the hole:
        # a change that puts the first matching line into a previously CLEAN file
        # and regenerates the baseline produces an entry with no counterpart, so
        # the scan agrees with the baseline and the target check had nothing to
        # object to. That is the same cheat this block exists to stop, entering
        # by a different door. A renamed file lands here too, and that is right:
        # carrying internal material to a new path is a decision, not a move.
        echo "FAIL — $path carries $count lane B occurrence(s) and is absent from" >&2
        echo "  the target's baseline. A file with no entry there had none: this is" >&2
        echo "  new internal material in a public SDK, however the baseline reads." >&2
        exit 1
      fi
      if [ "$count" -gt "$was" ]; then
        echo "FAIL — the baseline was raised for $path ($was -> $count)." >&2
        echo "  Editing this file upward is not a way to introduce internal material." >&2
        exit 1
      fi
    done <<< "$lane_b_base"
  else
    # Named, not swallowed: the target may predate the baseline.
    echo "  (baseline-vs-target check skipped: ${PUBLIC_SURFACE_BASE_REF} carries no $LANE_B_BASELINE)"
  fi
else
  echo "  (baseline-vs-target check skipped: PUBLIC_SURFACE_BASE_REF unset — CI sets it)"
fi

echo "LANE B RATCHET — held at $(printf '%s' "$lane_b_now" | grep -c . ) file(s), $(printf '%s' "$lane_b_now" | awk '{n += $1} END {print n + 0}') occurrence(s). The number may fall and may not rise."

gate_finished=yes
echo "LANE A (GATED) — clean. ${scan_files} tracked file(s) were read; a run that scanned none refuses above rather than reporting this line."
