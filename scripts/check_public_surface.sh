#!/usr/bin/env bash
# check_public_surface.sh — this repository is PUBLIC, so every tracked byte is
# published. This gate fails when internal ShardPilot material appears in the
# part of the tree it covers.
#
# WHY IT EXISTS. The org-wide publication-readiness check (in the internal `qa`
# repository) enumerates publication CANDIDATES — repositories that are private
# and might be flipped. This repository is not a candidate, because it is
# already public. Nothing scanned it, and that is exactly how an internal
# review-process skill, internal decision-record ids, an internal service name
# and an internal commit sha came to sit in a public repository for months. A
# gate that runs where the exposure already exists is the fix.
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
set -euo pipefail

cd "$(dirname "$0")/.."

# One pattern list, used by both lanes and by the self-test — two spellings is
# how the second consumer comes to check something different from the first.
PATTERNS='ADR-[0-9]+|§[0-9]|[Tt]here is (no|NO) [A-Za-z]+ (harness|coverage|test|suite)|(is|are) not (tested|covered|scanned|audited|monitored)|no (automated )?(tests?|coverage|scanning|monitoring) (for|of|in)|nobody (looks|checks|monitors)|GAP-[0-9]{3}|\bSP-[0-9]{3}\b|\bAC-[A-Z]{2}-[0-9]+|analytics[-_ ]service|crash[-_ ]symbolicator|control[-_ ]plane|shardpilot/(docs|qa|infra|integrations)|[Pp]roject[-_ ][Tt]ower|shepherd-pr|verification-discipline|Codex (review|#|[a-z]+#)|[A-Z][A-Z0-9]*(_[A-Z0-9]+)+_(ENABLED|DISABLED|MODE)|\b(main|master|HEAD) @ *`?[0-9a-f]{7,40}'

# The one legitimate collision with the `§` class, in ONE place so the scan
# and the self-test cannot disagree about it. The first attempt put the
# exclusion only in the scan; the innocent self-test then failed, correctly,
# because it was testing a different rule from the one that runs.
EXCLUDE='Apache[- ]2\.0 §|Apache License[^§]{0,40}§|LICENSE §'

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
    is_exempt "$f" && continue
    # THE PATH ITSELF IS PUBLISHED CONTENT. An internal identifier in a file
    # NAME — a decision-record id, a ticket, a service name in a directory —
    # reaches every consumer and appears in no file's body, so scanning only
    # contents misses it entirely.
    if printf '%s\n' "$f" | grep -qE -- "$PATTERNS"; then
      scan_lane_a="${scan_lane_a}${f}:path:${f}"$'\n'
    fi
    # -h: a symlink's published blob content IS its target path, so read the
    # link rather than skipping it or following it out of the checkout.
    if [ -L "$root/$f" ]; then
      hits="$(readlink "$root/$f" | grep -nE -- "$PATTERNS" || true)"
      [ -n "$hits" ] && scan_lane_a="${scan_lane_a}${f}:symlink-target:$(readlink "$root/$f")"$'\n'
      scan_files=$((scan_files + 1))
      continue
    fi
    [ -f "$root/$f" ] || continue
    scan_files=$((scan_files + 1))
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
    hits="$(grep -anE -- "$PATTERNS" "$root/$f" 2>/dev/null \
      | grep -avE -- "$EXCLUDE" \
      | tr -d '\000'; exit "${PIPESTATUS[0]}")"
    status=$?
    set -e
    if [ "$status" -ge 2 ]; then
      echo "REFUSING: grep could not read '$f' (exit $status)." >&2
      echo "  An unreadable file is an UNSCANNED file, and this gate must not" >&2
      echo "  report a repository clean on the strength of one." >&2
      exit 2
    fi
    [ "$status" -ne 0 ] && continue
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
KNOWN_INTERNAL='per ADR-0221 §3
There is NO Playwright harness in the console repo
the crash path is not covered by automated tests
no automated scanning for that class of input
a bare §7c left behind by a citation strip
tracked as GAP-075 internally
the control-plane assignment endpoint
pinned to analytics-service main @ 7d118c5
see shardpilot/docs for context
Project Tower-specific event names
the shepherd-pr skill
the verification-discipline references
(Codex go#48 round 3)
INGEST_CONSENT_KIND_MODE=off'
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
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    innocent=$((innocent + 1))
    # The SAME exclusion the scan applies, so the two cannot drift.
    if printf '%s\n' "$line" | grep -E -- "$PATTERNS" | grep -qvE -- "$EXCLUDE"; then
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
    printf 'see ADR-0221 for context\n' > dirty.md
    printf '// GAP-075 note\npackage x\n' > lane_b.go
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
echo "LANE A (GATED) — non-source tracked files: clean (${scan_files} file(s) scanned)."
