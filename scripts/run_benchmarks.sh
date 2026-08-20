#!/usr/bin/env bash
# Run the committed benchmarks and PUBLISH the numbers.
#
# The benchmarks have existed in this repository for some time and have never
# run in CI. The difference between "committed" and "shown executing" is the
# whole point: a committed benchmark that nothing runs is a file, not a
# measurement, and it rots exactly as silently as an unrun test.
#
# WHY THIS IS A SCRIPT AND NOT A `run:` LINE
# ------------------------------------------
# `go test -bench . ./...` **exits 0 when it matches no benchmarks at all**.
# Rename one, move it behind a build tag, or mistype the pattern, and the CI
# step stays green over zero measurements — a check that has stopped checking,
# reporting success. So this asserts that every benchmark that exists actually
# reported a result, and names the ones that did not.
#
# It deliberately does NOT gate on the numbers themselves. A performance
# threshold needs a baseline measured on hardware comparable to the runner, and
# a shared CI runner's variance would either be set so wide it catches nothing
# or so tight it fails on a noisy neighbour. What this produces is a
# reproducible, published measurement; a regression gate is a separate decision
# with a baseline behind it.
#
# THREE AXES, BECAUSE THERE ARE THREE WAYS TO STOP MEASURING
# ----------------------------------------------------------
# This began with a hand-written array of expected names. That catches a
# DELETED benchmark (the array still names it) and misses an ADDED one (nobody
# updates the array, so it runs unasserted forever). Replacing the array with
# an enumeration of the tree inverts the hole exactly: an added benchmark is
# picked up automatically, and a deleted one simply shrinks the expected set,
# so every remaining name matches and CI reports success. An enumeration
# checked against the same tree it enumerates cannot notice a subtraction.
#
# And an enumeration made in ONE build configuration cannot notice a benchmark
# that exists in another. `go test -list` answers for the platform and tags it
# is invoked with; a `//go:build windows` benchmark is invisible to it on a
# Linux runner — not run, not asserted, not reported missing.
#
# So:
#
#   1. DECLARED vs MANIFEST — did the SET change? `declared` is what the
#      toolchain can see here PLUS the benchmarks in tracked test files the
#      toolchain did not compile here. The manifest is committed, so adding or
#      deleting a benchmark fails until the same change updates it. That is one
#      line of reviewable diff, and the only place a human states "yes, this
#      benchmark is meant to be gone".
#   2. UNBUILDABLE ⊆ EXEMPT — a benchmark this configuration cannot even
#      compile can never report a result here, so it must be named in
#      BENCHMARKS_NOT_RUN_IN_CI. Otherwise "it did not run" and "it cannot run"
#      are the same silence.
#   3. RUNNABLE vs RESULTS — did every benchmark that could run actually run?
#      This catches one going unreachable, skipped, or filtered out while still
#      sitting in the tree.
#
# THE ENUMERATOR IS THE GO TOOLCHAIN, NOT grep
# --------------------------------------------
# `go test -list` is the compiler's own answer to "what benchmarks are here",
# which a text scan only approximates. `func /* reason */ BenchmarkFoo(...)` is
# a legal, gofmt-clean declaration that `^func Benchmark` never matches — and
# `go test` runs it. Listing also comes back grouped BY PACKAGE, which a flat
# name scan cannot give: two packages may define the same benchmark name, and a
# repository-wide name match would let either one satisfy both.
#
# Text scanning survives in exactly one place: the files the toolchain told us
# it did not compile, which it therefore cannot enumerate for us. That set is
# small, explicitly identified, and scanned with a deliberately OVER-broad
# pattern — over-matching there costs one line of declaration, under-matching
# would restore the hole.
#
# Usage:
#   scripts/run_benchmarks.sh                  # run, assert, print
#   scripts/run_benchmarks.sh out/bench.txt    # …and write the raw output there
set -euo pipefail

cd "$(dirname "$0")/.."

MANIFEST="scripts/benchmarks.manifest"

# The escape hatch, for a benchmark that deliberately cannot run here — one
# needing hardware this runner lacks, or sitting behind a build tag this
# platform does not satisfy. Naming it is a reviewable diff; the alternative is
# discovering the omission from a number that quietly stopped being measured.
#
# Entries are "<import path> <BenchmarkName>" — PACKAGE-QUALIFIED, because two
# packages may define the same benchmark name and only one of them may need the
# opt-out. Excluding by bare name would drop the other package's perfectly
# runnable benchmark from the run and the artifact with nothing to show for it.
#
# An exempted benchmark is removed from that package's `-bench` selector, so it
# is not merely unasserted but genuinely not run: an exclusion added because a
# benchmark hangs or eats a scarce resource would be worthless if the run still
# executed it. Empty on purpose, and expected to stay that way.
BENCHMARKS_NOT_RUN_IN_CI=()

output_file="${1:-}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

raw="$tmp/raw"
listing="$tmp/listing"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# ---------------------------------------------------------------------------
# 1. Enumerate what the toolchain can see here, as "package<TAB>BenchmarkName".
#
# `go test -list` prints the matching names for a package, then the package's
# own `ok`/`?`/`FAIL` line — so names are attributed to the package whose
# terminator follows them.
# ---------------------------------------------------------------------------
if ! go test -list '^Benchmark' ./... >"$listing" 2>&1; then
  cat "$listing" >&2
  fail "go test -list failed — the benchmark set could not be enumerated"
fi

enumerated="$(
  awk '
    /^(ok|FAIL|\?)[ \t]/ {
      for (i = 1; i <= n; i++) print $2 "\t" names[i]
      n = 0
      next
    }
    /^Benchmark/ { names[++n] = $1 }
  ' "$listing" | sort
)"

# Vacuity guard on the enumeration itself: reading zero names would make every
# check below pass over nothing and report success, which is the exact shape of
# failure this script exists to refuse.
if [ -z "$enumerated" ]; then
  fail "go test -list found no benchmarks at all — refusing to report a pass over nothing"
fi

# ---------------------------------------------------------------------------
# 2. Find the benchmarks this configuration CANNOT see.
#
# Every tracked *_test.go that the toolchain did not compile here — excluded by
# a build constraint, in a directory `./...` does not walk, in a package whose
# every file is constrained away. `go list` reports what it compiled; git
# reports what exists. The difference is the blind spot, and it is scanned
# rather than enumerated because there is no toolchain that will enumerate a
# configuration it is not running in.
# ---------------------------------------------------------------------------
if ! MODULE="$(go list -m 2>"$tmp/moderr")"; then
  cat "$tmp/moderr" >&2
  fail "go list -m failed — cannot map directories to import paths"
fi

if ! go list -e -f '{{.ImportPath}}{{"\t"}}{{.Dir}}{{"\t"}}{{join .TestGoFiles " "}}{{"\t"}}{{join .XTestGoFiles " "}}' ./... >"$tmp/golist" 2>"$tmp/golisterr"; then
  cat "$tmp/golisterr" >&2
  fail "go list failed — the set of compiled test files could not be determined"
fi

awk -F'\t' -v root="$PWD/" '
  {
    dir = $2
    if (substr(dir, 1, length(root)) == root) dir = substr(dir, length(root) + 1)
    else if (dir "/" == root)                 dir = ""
    for (col = 3; col <= 4; col++) {
      n = split($col, files, " ")
      for (i = 1; i <= n; i++)
        if (files[i] != "") print (dir == "" ? files[i] : dir "/" files[i])
    }
  }
' "$tmp/golist" | sort -u >"$tmp/compiled"

# NUL-delimited: a path containing a space or a tab must survive intact. A path
# containing a NEWLINE cannot survive a line-oriented set comparison at all, so
# it is refused rather than silently mis-compared — the count of NULs and the
# count of lines have to agree.
if ! git ls-files -z -- '*_test.go' >"$tmp/tracked_z"; then
  fail "git ls-files failed — cannot determine which test files are tracked"
fi
nul_count="$(tr -dc '\0' <"$tmp/tracked_z" | wc -c | tr -d ' ')"
tr '\0' '\n' <"$tmp/tracked_z" | sed '/^$/d' | sort >"$tmp/tracked"
line_count="$(wc -l <"$tmp/tracked" | tr -d ' ')"
[ "$nul_count" = "$line_count" ] || \
  fail "a tracked test file name contains a newline — refusing to compare file sets line by line"

unconsidered="$(comm -23 "$tmp/tracked" "$tmp/compiled")"

unbuildable="$(
  printf '%s\n' "$unconsidered" | while IFS= read -r file; do
    [ -n "$file" ] || continue
    [ -f "$file" ] || fail "tracked test file $file is missing from the worktree"
    dir="$(dirname "$file")"
    if [ "$dir" = "." ]; then pkg="$MODULE"; else pkg="$MODULE/$dir"; fi
    # Newlines are flattened first so a declaration wrapped across lines is
    # still seen, and `[^(){}]*` lets a comment sit between `func` and the
    # identifier without letting the match wander into a function body.
    tr '\n' ' ' <"$file" \
      | grep -oE 'func[^(){}]*Benchmark[A-Za-z0-9_]*' \
      | grep -oE 'Benchmark[A-Za-z0-9_]*$' \
      | awk -v p="$pkg" '{ print p "\t" $0 }' || true
  done | sort -u
)"

declared="$(printf '%s\n%s\n' "$enumerated" "$unbuildable" | sed '/^$/d' | sort -u)"

# ---------------------------------------------------------------------------
# 3. Normalise the escape hatch into "package<TAB>name" rows.
# ---------------------------------------------------------------------------
exempt=""
for entry in ${BENCHMARKS_NOT_RUN_IN_CI[@]+"${BENCHMARKS_NOT_RUN_IN_CI[@]}"}; do
  read -r xpkg xname xrest <<< "$entry"
  [ -n "${xpkg:-}" ] && [ -n "${xname:-}" ] && [ -z "${xrest:-}" ] || \
    fail "BENCHMARKS_NOT_RUN_IN_CI entry '$entry' is not '<import path> <BenchmarkName>'"
  exempt="${exempt}${exempt:+$'\n'}${xpkg}"$'\t'"${xname}"
done
exempt="$(printf '%s\n' "$exempt" | sed '/^$/d' | sort -u)"

# ---------------------------------------------------------------------------
# 4. The SET check: everything declared in the tree vs the committed manifest.
# ---------------------------------------------------------------------------
[ -f "$MANIFEST" ] || fail "$MANIFEST is missing — it is the record of which benchmarks are meant to exist"

grep -vE '^[[:space:]]*(#|$)' "$MANIFEST" | sort -u >"$tmp/manifest" || true
printf '%s\n' "$declared" >"$tmp/declared"

if ! manifest_diff="$(diff "$tmp/declared" "$tmp/manifest" 2>&1)"; then
  echo >&2
  echo "The benchmark SET changed. '<' is in the tree, '>' is in $MANIFEST:" >&2
  printf '%s\n' "$manifest_diff" >&2
  echo >&2
  echo "  Adding a benchmark? Add its line. Deleting one? Remove its line, in the" >&2
  echo "  SAME change — that removal is the only place anyone states the benchmark" >&2
  echo "  is meant to be gone rather than accidentally missing. Apply the diff" >&2
  echo "  above: '<' lines belong in $MANIFEST, '>' lines do not." >&2
  fail "tree and $MANIFEST disagree about which benchmarks exist"
fi

# An exemption naming a benchmark that no longer exists is a stale opt-out, and
# a stale opt-out is how a benchmark comes back and stays unmeasured.
stale="$(printf '%s\n' "$exempt" | sed '/^$/d' | comm -23 - "$tmp/manifest")"
if [ -n "$stale" ]; then
  echo >&2
  echo "BENCHMARKS_NOT_RUN_IN_CI names benchmarks that are not in $MANIFEST:" >&2
  printf '%s\n' "$stale" | sed 's/^/    /' >&2
  fail "stale entries in BENCHMARKS_NOT_RUN_IN_CI"
fi

# ---------------------------------------------------------------------------
# 5. The UNBUILDABLE check: what cannot compile here must say so.
# ---------------------------------------------------------------------------
if [ -n "$unbuildable" ]; then
  undeclared="$(printf '%s\n' "$unbuildable" | comm -23 - <(printf '%s\n' "$exempt" | sed '/^$/d'))"
  if [ -n "$undeclared" ]; then
    echo >&2
    echo "These benchmarks are in tracked test files this configuration does not compile:" >&2
    printf '%s\n' "$undeclared" | sed 's/^/    /' >&2
    echo >&2
    echo "  A build tag, or a directory './...' does not walk, puts them outside" >&2
    echo "  everything below — they cannot run here and cannot be reported missing," >&2
    echo "  so nothing would ever notice them. Add each to BENCHMARKS_NOT_RUN_IN_CI" >&2
    echo "  as '<import path> <BenchmarkName>' and say why." >&2
    fail "$(printf '%s\n' "$undeclared" | wc -l | tr -d ' ') benchmark(s) cannot be built here and are not declared"
  fi
fi

# ---------------------------------------------------------------------------
# 6. Run — one invocation per package, so selection is package-scoped.
#
# `-bench` is a global name filter with no package qualifier, so a single
# `./...` run cannot express "this name here but not there". Per-package runs
# can, which is what makes a package-qualified exemption mean anything.
# ---------------------------------------------------------------------------
to_run="$(printf '%s\n' "$enumerated" | comm -23 - <(printf '%s\n' "$exempt" | sed '/^$/d'))"

[ -n "$to_run" ] || fail "every enumerated benchmark is in BENCHMARKS_NOT_RUN_IN_CI — nothing would be measured"

# `-benchtime=100x` rather than the default 1s per benchmark: a fixed iteration
# count keeps the CI step bounded and the allocation figures (the part worth
# comparing across runs) exact, while wall-clock ns/op on a shared runner is
# indicative at best either way.
BENCHTIME="${SHARDPILOT_BENCHTIME:-100x}"

echo "running benchmarks (-benchtime=${BENCHTIME}, -benchmem) …" >&2

: >"$raw"
while IFS= read -r pkg; do
  [ -n "$pkg" ] || continue
  names="$(printf '%s\n' "$to_run" | awk -F'\t' -v p="$pkg" '$1 == p { print $2 }' | sort -u | paste -sd '|' -)"
  # Anchored, so `BenchmarkFoo` cannot also select `BenchmarkFooBar`.
  # `-run '^$'` so no TEST runs here — this step measures, the test step tests,
  # and mixing them makes a benchmark failure look like a test failure.
  if ! go test -run '^$' -bench "^(${names})"'$' -benchmem -benchtime="$BENCHTIME" "$pkg" >>"$raw" 2>&1; then
    cat "$raw" >&2
    fail "benchmark run exited non-zero for $pkg"
  fi
done <<< "$(printf '%s\n' "$to_run" | cut -f1 | sort -u)"

cat "$raw"

if [ -n "$output_file" ]; then
  mkdir -p "$(dirname "$output_file")"
  cp "$raw" "$output_file"
  echo "wrote $output_file" >&2
fi

# ---------------------------------------------------------------------------
# 7. The EXECUTION check: every benchmark that should have run, reported —
#    matched WITHIN its own package's section of the output.
#
# A result line is the name, optionally a `/sub` path when the benchmark uses
# b.Run (Go emits `BenchmarkParent/child-8` and NO bare parent line, so
# demanding one would fail a benchmark that ran perfectly well), optionally a
# `-<GOMAXPROCS>` suffix, then the iteration count.
# ---------------------------------------------------------------------------
missing="$(
  awk -v want="$to_run" '
    BEGIN {
      n = split(want, rows, "\n")
      for (i = 1; i <= n; i++) {
        split(rows[i], f, "\t")
        expected[f[1] SUBSEP f[2]] = 1
      }
    }
    /^pkg:[ \t]/ { pkg = $2; next }
    /^Benchmark/ {
      name = $1
      sub(/-[0-9]+$/, "", name)
      sub(/\/.*$/, "", name)
      if (NF >= 2 && $2 ~ /^[0-9]+$/) seen[pkg SUBSEP name] = 1
    }
    END {
      for (k in expected) if (!(k in seen)) { split(k, f, SUBSEP); print f[1] "\t" f[2] }
    }
  ' "$raw" | sort
)"

if [ -n "$missing" ]; then
  echo >&2
  echo "These benchmarks exist but reported no result:" >&2
  printf '%s\n' "$missing" | sed 's/^/    /' >&2
  echo >&2
  echo "  'go test -bench' passes over a benchmark it cannot reach and says" >&2
  echo "  nothing. Usual causes: a b.Skip at the top, or a benchmark whose body" >&2
  echo "  never reaches the measured loop. If it genuinely cannot run here, add" >&2
  echo "  '<import path> <BenchmarkName>' to BENCHMARKS_NOT_RUN_IN_CI in $0 and" >&2
  echo "  say why." >&2
  fail "$(printf '%s\n' "$missing" | wc -l | tr -d ' ') benchmark(s) reported no result"
fi

ran="$(printf '%s\n' "$to_run" | wc -l | tr -d ' ')"
echo >&2
echo "OK: ${ran} benchmark(s) executed and reported results." >&2
