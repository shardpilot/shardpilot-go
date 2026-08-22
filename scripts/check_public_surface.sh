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
# ⚠ AND WHAT IS DELIBERATELY ENCODED IS OUT OF SCOPE, which is why the
# character-reference refusal, the inline-tag normalisation and the XML, RTF
# and PostScript refusals were REMOVED rather than extended. Nobody types
# `&#65;DR-0000` by accident — and the number in that example is a SENTINEL
# because nothing else here could be: the encoding this file has just declared
# out of scope is the encoding its own audits cannot see through, so an example
# carrying a live id would be published by the one file that could not report
# it. That is how two internal repository names got here the first time.
# Defending against it with eight refusals while
# the paragraph below declares deliberate concealment out of scope is the same
# document-says-one-thing-machinery-does-another this whole change exists to
# remove — only inside a single file.
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
GATE_DATA_NAMES='ROSTER KNOWN_INTERNAL KNOWN_INNOCENT FIXTURE_ACCENT_BODY FIXTURE_ACCENT_NAME FIXTURE_BINARY_BODY FIXTURE_BINARY_NAME FIXTURE_CLEAN_BODY FIXTURE_CLEAN_NAME FIXTURE_DIRTY_BODY FIXTURE_DIRTY_NAME FIXTURE_EMPHASIS_BODY FIXTURE_EMPHASIS_NAME FIXTURE_ESCAPE_BODY FIXTURE_ESCAPE_NAME FIXTURE_FLAG_BODY FIXTURE_FLAG_NAME FIXTURE_LANEB_BODY FIXTURE_LANEB_NAME FIXTURE_NAMEHIT_BODY FIXTURE_NAMEHIT_NAME'

PATTERNS='ADR-[0-9]+|§[0-9]|[Tt]here (is|are) [Nn][Oo] [A-Za-z][A-Za-z-]*( [A-Za-z-]+){0,2} (harness|harnesses|coverage|tests?|suites?)|(is|are|was|were)(n.{1,3}t| not| never) (tested|covered|scanned|audited|monitored)|(is|are|was|were|remains?) (largely |entirely |still |completely |mostly )?(untested|unmonitored|unaudited|unscanned)|[Nn]o( [A-Za-z][A-Za-z-]*){0,3} (tests?|coverage|scanning|monitoring|harness|harnesses|suites?)( (exists?|existed|remains?|remained|runs?|ran|covers?|covered|exercises?|exercised|guards?|guarded))?( (for|of|in)|[.,;]|$)|[Tt]here (is|are)(n.{1,3}t| not) (any |no )?(harness|harnesses|coverage|tests?|suites?)|[Tt]here (is|are) zero( [A-Za-z][A-Za-z-]*){0,3} (harness|harnesses|coverage|tests?|suites?)|(has|have|had) zero( [A-Za-z][A-Za-z-]*){0,3} (harness|harnesses|coverage|tests?|suites?|monitoring)( (for|of|in)|[.,;]|$)|[Ww]ithout( (automated|manual|unit|integration|end-to-end|regression|any|meaningful))* (harness|harnesses|coverage|tests?|suites?|monitoring)( (for|of|in)|[.,;]|$)|[Nn]obody (looks|checks|monitors)( at| on)?( [A-Za-z][A-Za-z-]*){0,3} (dashboard|dashboards|alert|alerts|log|logs|metric|metrics|queue|queues|report|reports|test|tests|coverage|monitoring)( (for|of|in)|[.,;]|$)|(is|are|was|were)(n.{1,3}t| not| never) under (test|testing|coverage|monitoring|observation)( (for|of|in)|[.,;]|$)|[Ll]acks( any| automated| an?)*( [A-Za-z][A-Za-z-]*)? (harness|harnesses|coverage|tests?|suites?|monitoring)( (for|of|in)|[.,;]|$)|(has|have|had)(n.{1,3}t| not| never) been (tested|covered|scanned|audited|monitored)|(does|do|did)( not|n.{1,3}t) have( any| automated| an?)*( [A-Za-z][A-Za-z-]*){0,2} (harness|harnesses|coverage|tests?|suites?|monitoring)|GAP-[0-9]{3}|\bSP-[0-9]{3}\b|\bAC-[A-Z]{2}-[0-9]+|Codex (review|#|[a-z]+#)|[A-Z][A-Z0-9]*(_[A-Z0-9]+)+_(ENABLED|DISABLED|MODE)|\b(main|master|HEAD) @ *`?[0-9a-f]{7,40}'
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
    if (inq == 0 && ind == 0 && $0 ~ /^[A-Za-z_][A-Za-z0-9_]*=/) {
      name = $0; sub(/=.*/, "", name); print name
      assign_line = 1; bare = ""
    } else assign_line = 0
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
      if (c == "#") break
      # ⚠ ONE ASSIGNMENT PER LINE. Only the assignment at the START of a line
      # is recorded, so a second one on the same line runs, publishes its
      # value, and appears in no list — undecoded and unaudited. The parser
      # cannot follow shell grammar, so the grammar is restricted instead: an
      # assignment line carries no unquoted control operator AND no second
      # `NAME=` behind whitespace, which the shell accepts just as happily.
      if (assign_line && (c == ";" || c == "&" || c == "|")) { print "!CTRL " name; assign_line = 0 }
      if (assign_line) bare = bare c
      if (c == sq) inq = 1
      else if (c == dq) ind = 1
      i++
    }
    if (assign_line && bare ~ /[[:space:]][A-Za-z_][A-Za-z0-9_]*=/) print "!CTRL " name
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
while IFS= read -r data_name; do
  [ -n "$data_name" ] || continue
  # ⚠ TWO NAMES ARE EXEMPT, AND FOR STATED REASONS. `GATE_DATA_NAMES` is the
  # list itself. `PATTERNS` carries no visible break — measured: it matches
  # none of the classes — and it is not decoded by the loop, so it cannot be in
  # the list; it is audited instead by the pattern-list rules below, which read
  # it more strictly than any fixture. Nothing else is exempt.
  case "$data_name" in
    '!CTRL '*)
      # refusal:structural
      echo "REFUSING: the data-block assignment ${data_name#!CTRL } shares its line with" >&2
      echo "  a control operator. Only the assignment at the start of a line is" >&2
      echo "  recorded, so anything behind a ';', '&' or '|' runs, publishes its" >&2
      echo "  value, and is decoded and audited by nothing. One assignment per line." >&2
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
BZBLK='\061\101\131\046\123\131'
CONTAINER_SIGS_TXT="\102\132\150\061$BZBLK \102\132\150\062$BZBLK \102\132\150\063$BZBLK"
CONTAINER_SIGS_TXT="$CONTAINER_SIGS_TXT \102\132\150\064$BZBLK \102\132\150\065$BZBLK"
CONTAINER_SIGS_TXT="$CONTAINER_SIGS_TXT \102\132\150\066$BZBLK \102\132\150\067$BZBLK"
CONTAINER_SIGS_TXT="$CONTAINER_SIGS_TXT \102\132\150\070$BZBLK \102\132\150\071$BZBLK"
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
  gate_tmp; list="$GATE_TMP"
  gate_tmp; blob="$GATE_TMP"
  gate_tmp; md_blob="$GATE_TMP"
  if ! (cd "$root" && git ls-files -z) > "$list"; then
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
      # The GIF version follows the signature and is 87a or 89a, nothing else.
      47494638)
        case "${magic16:8:4}" in
          3761|3961) magic_hit="$printable_sigs" ;;
          *)         magic_hit=no ;;
        esac ;;
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
            magic_np=no
            for magic_i in 6 8 10 12 14 16 18 20 22 24 26 28 30; do
              case "${magic16:$magic_i:2}" in
                09|0a|0b|0c|0d|20) continue ;;
                3[0-9])            magic_np=yes ;;
              esac
              break
            done
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
      echo "REFUSING: '$f' carries a rendered-document stream." >&2
      echo "  Its header and its object terminator are both present, and no pass" >&2
      echo "  here reads document contents. Remove it, or extend this gate to" >&2
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
    if ! LC_ALL=C grep -qa "$(printf '\012')" "$blob" 2>/dev/null \
       && LC_ALL=C grep -qa "$(printf '\015')" "$blob" 2>/dev/null; then
      if LC_ALL=C tr '\015' '\012' < "$blob" > "$md_blob"; then
        cat "$md_blob" > "$blob"
      else
        # refusal:structural
        echo "REFUSING: could not normalise CR-only line endings in '$f'." >&2
        exit 2
      fi
    fi
    if sed 's/\r$//' "$blob" > "$md_blob"; then
      cat "$md_blob" > "$blob"
    else
      # refusal:structural
      echo "REFUSING: could not normalise line endings in '$f'." >&2
      exit 2
    fi
    # The staged blob, so a reported line number is a line number in what a
    # commit would carry. For a path whose tree copy matches its index entry —
    # every path in a fresh checkout — that is the same file.
    scan_src="$blob"
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
    # Link syntax and character references are NOT in the stripped set. Those
    # are the deliberate-encoding class the scope note puts out of scope, and
    # widening the set here would reopen a decided question sideways.
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
    hits="$( { printf '%s\n' "$hits_raw"; printf '%s\n' "$hits_strip"; } \
      | grep -v '^$' | awk -F: '!seen[$1]++' )"
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
  function shrink(r,   out, i, ch, e, body) {
    out = ""; i = 1
    while (i <= length(r)) {
      ch = substr(r, i, 1)
      if (ch != "[") { out = out ch; i++; continue }
      e = i + 1
      if (substr(r, e, 1) == "^") e++
      if (substr(r, e, 1) == "]") e++
      while (e <= length(r) && substr(r, e, 1) != "]") e++
      body = substr(r, i + 1, e - i - 1)
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
          gsub(/\{0*1\}/, "", arm)
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
     printf '%s\n' "$corpus_values" | grep -oE -- "$AUDIT_CLASSES"; } | sort -u )
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
  fixture_checks=$((fixture_checks + 1))
  printf '%s' "$scanned_a" | grep -qF -- "$FIXTURE_NAMEHIT_NAME:path:" || {
    echo "SELFTEST: the scan missed an internal identifier in a PATH NAME" >&2; fixture_fail=1; }
  fixture_checks=$((fixture_checks + 1))
  printf '%s' "$scanned_a" | grep -q '^clean\.md:' && {
    echo "SELFTEST: the scan flagged clean.md" >&2; fixture_fail=1; }
  fixture_checks=$((fixture_checks + 1))
  [ "$scan_lane_b_files" -eq 1 ] || {
    echo "SELFTEST: lane B counted $scan_lane_b_files files, expected 1" >&2; fixture_fail=1; }
  # ⚠ A COUNT THAT MUST BE REACHED. Every assertion above is invisible when it
  # is deleted, and a run that asserts nothing prints the same closing line as
  # a run that asserted everything. The floor moves up when assertions are
  # added and refuses when they go.
  [ "$fixture_checks" -ge 9 ] || {
    echo "SELFTEST: only $fixture_checks scan assertion(s) ran, expected at least 9" >&2
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
gate_finished=yes
echo "LANE A (GATED) — clean. ${scan_files} tracked file(s) were read; a run that scanned none refuses above rather than reporting this line."
