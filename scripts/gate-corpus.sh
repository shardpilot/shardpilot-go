#!/usr/bin/env bash
# gate-corpus.sh — the material check_public_surface.sh must NOT scan, because
# it matches the gate's own classes BY CONSTRUCTION: the class patterns, the
# roster of internal names, and the fixture corpora the self-test runs against.
#
# WHY THIS IS A SEPARATE FILE. It used to live inside the gate as regions
# marked off with comments, and the gate skipped those regions before scanning
# itself. Every version of that arrangement turned out to be a place to hide
# things, six times over: an unterminated region blanked the rest of the file;
# a nested one closed early; a suffixed marker counted without contributing a
# label; a comment inside a region was unreadable; an INLINE comment likewise;
# and finally an ordinary data line. Each fix produced the next variant,
# because the shape was wrong rather than the implementation — an exempt region
# inside a scanned file is somewhere to put things.
#
# So the gate now scans itself END TO END with no exemptions at all, and the
# by-construction material lives here. This file is excluded by PATH, which is
# a fact about the repository rather than a marker anyone can write.
#
# WHAT KEEPS THIS FILE HONEST. The gate reads this file with a GRAMMAR rather
# than a list of forbidden shapes, because the list approach caught one variant
# per round and left the next: a comment, then a function declaration, then an
# assignment repeated so its first value never reaches the self-test, then a
# comment after a semicolon. The rule is now what a line MAY be — below this
# header, it either continues an open quoted value or begins an assignment to
# one expected name, each name exactly once, and outside a quoted literal only
# name characters, `=`, `'` and `$'` may appear. Anything else is refused —
# including an expansion or an array, both of which put text in this file that
# no value ever carries.
#
# Every name below is consumed by the script that sources this file. The linter
# cannot see that consumer from here, hence the blanket SC2034 below. It is a
# file-level directive because the gate refuses comments BELOW this header, and
# a per-line one would be exactly that.
#
# (Note the wrapping above: a comment line BEGINNING with the linter's name is
# parsed as a directive, so this paragraph keeps it mid-sentence. That cost a
# round.)
# shellcheck disable=SC2034
ROSTER='analytics-service
control-plane'
PATTERNS='ADR-[0-9]+|§[0-9]|[Tt]here (is|are) [Nn][Oo] [A-Za-z][A-Za-z-]*( [A-Za-z-]+){0,2} (harness|harnesses|coverage|tests?|suites?)|(is|are|was|were)(n.{1,3}t| not) (tested|covered|scanned|audited|monitored)|(is|are|was|were|remains?) (largely |entirely |still |completely |mostly )?(untested|unmonitored|unaudited|unscanned)|[Nn]o( [A-Za-z][A-Za-z-]*){0,3} (tests?|coverage|scanning|monitoring|harness|harnesses|suites?)( (for|of|in)|[.,;]|$)|[Nn]obody (looks|checks|monitors)|[Ll]acks( any| automated| an?)* ?[A-Za-z-]*[ ]?(harness|harnesses|coverage|tests?|suites?|monitoring)|(has|have|had)(n.{1,3}t| not| never) been (tested|covered|scanned|audited|monitored)|(does|do|did)( not|n.{1,3}t) have( any| automated| an?)*( [A-Za-z][A-Za-z-]*){0,2} (harness|harnesses|coverage|tests?|suites?|monitoring)|GAP-[0-9]{3}|\bSP-[0-9]{3}\b|\bAC-[A-Z]{2}-[0-9]+|Codex (review|#|[a-z]+#)|[A-Z][A-Z0-9]*(_[A-Z0-9]+)+_(ENABLED|DISABLED|MODE)|\b(main|master|HEAD) @ *`?[0-9a-f]{7,40}'

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
EXAMPLE_SYNTHETIC_FLAG_MODE=off
The crash path isn'$'\047''t tested.
The crash path hasn'$'\047''t been tested.
The console doesn'$'\342\200\231''t have automated tests.
The console has never been audited.'

KNOWN_INNOCENT='go get github.com/shardpilot/shardpilot-go@v0.6.0-alpha
IngestURL: os.Getenv("SHARDPILOT_INGEST_URL")
POST {IngestURL}/v1/events:batch
https://localhost:8080 during local development
a documented per-platform adaptation, not drift
DEFOLD_SHA1="f735c12192bf95684e6ae1ae27c400b8170fc6d8"
a self-service signup flow, a micro-service boundary
the event plane and the consent plane are separate
an analytics-plane request, zero event batches'
FIXTURE_CLEAN_NAME='clean.md'
FIXTURE_CLEAN_BODY='clean customer prose'
FIXTURE_NAMEHIT_NAME='ADR-0999-notes.md'
FIXTURE_NAMEHIT_BODY='nothing internal in the body'
FIXTURE_DIRTY_NAME='dirty.md'
FIXTURE_DIRTY_BODY='see ADR-0000 for context'
FIXTURE_LANEB_NAME='lane_b.go'
FIXTURE_LANEB_BODY='// GAP-000 note
package x'
FIXTURE_ACCENT_NAME='café.md'
FIXTURE_ACCENT_BODY='internal: control-plane'
FIXTURE_BINARY_NAME='binary.bin'
FIXTURE_BINARY_BODY='see ADR-0999 here'
