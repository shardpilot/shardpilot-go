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
# reporting success. So this asserts that every benchmark the toolchain can see
# actually reported a result, and names the ones that did not.
#
# It deliberately does NOT gate on the numbers themselves. A performance
# threshold needs a baseline measured on hardware comparable to the runner, and
# a shared CI runner's variance would either be set so wide it catches nothing
# or so tight it fails on a noisy neighbour. What this produces is a
# reproducible, published measurement; a regression gate is a separate decision
# with a baseline behind it.
#
# TWO ASSERTIONS, ON TWO DIFFERENT AXES — and neither alone is enough
# -------------------------------------------------------------------
# This began with a hand-written array of expected names. That catches a
# DELETED benchmark (the array still names it) and misses an ADDED one (nobody
# updates the array, so it runs unasserted forever). Replacing the array with
# an enumeration of the tree inverts the hole exactly: an added benchmark is
# picked up automatically, and a deleted one simply shrinks the expected set,
# so every remaining name matches and CI reports success. An enumeration
# checked against the same tree it enumerates cannot notice a subtraction.
#
# So there are two checks, because there are two failures:
#
#   1. ENUMERATION vs MANIFEST — did the SET change? The manifest is committed,
#      so adding or deleting a benchmark fails until the same change updates
#      it. That is one line of reviewable diff, and it is the only place a
#      human states "yes, this benchmark is meant to be gone".
#   2. ENUMERATION vs RESULTS — did every benchmark that exists actually run?
#      This is the one that catches a benchmark going unreachable, skipped, or
#      filtered out while still sitting in the tree.
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
# Usage:
#   scripts/run_benchmarks.sh                  # run, assert, print
#   scripts/run_benchmarks.sh out/bench.txt    # …and write the raw output there
set -euo pipefail

cd "$(dirname "$0")/.."

MANIFEST="scripts/benchmarks.manifest"

# The escape hatch, for a benchmark that deliberately cannot run here — one
# needing hardware or a build tag CI does not have. Naming it is a reviewable
# diff; the alternative is discovering the omission from a number that quietly
# stopped being measured. Excluded names are removed from the `-bench` selector
# too, so an opted-out benchmark is not merely unasserted but genuinely not
# run: an exclusion added because a benchmark hangs or eats a scarce resource
# would be worthless if the run still executed it.
#
# Exclusion is BY NAME, across packages. The manifest is package-qualified, so
# an exclusion that silently covers a same-named benchmark in a second package
# is visible there rather than implied here. Empty on purpose.
BENCHMARKS_NOT_RUN_IN_CI=()

output_file="${1:-}"
raw="$(mktemp)"
listing="$(mktemp)"
trap 'rm -f "$raw" "$listing"' EXIT INT TERM

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# ---------------------------------------------------------------------------
# 1. Enumerate, via the toolchain, as "package<TAB>BenchmarkName".
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
# 2. The SET check: enumeration vs the committed manifest.
# ---------------------------------------------------------------------------
[ -f "$MANIFEST" ] || fail "$MANIFEST is missing — it is the record of which benchmarks are meant to exist"

if ! manifest_diff="$(diff <(printf '%s\n' "$enumerated") <(grep -vE '^\s*(#|$)' "$MANIFEST" | sort) 2>&1)"; then
  echo >&2
  echo "The benchmark SET changed. '<' is in the tree, '>' is in $MANIFEST:" >&2
  printf '%s\n' "$manifest_diff" >&2
  echo >&2
  echo "  Adding a benchmark? Add its line. Deleting one? Remove its line, in the" >&2
  echo "  SAME change — that removal is the only place anyone states the benchmark" >&2
  echo "  is meant to be gone rather than accidentally missing. Regenerate with:" >&2
  echo "      go test -list '^Benchmark' ./... " >&2
  fail "tree and $MANIFEST disagree about which benchmarks exist"
fi

# ---------------------------------------------------------------------------
# 3. Run — selecting exactly the benchmarks that are supposed to run here.
# ---------------------------------------------------------------------------
to_run=""
while IFS=$'\t' read -r pkg name; do
  [ -n "${name:-}" ] || continue
  excluded=false
  for skip in ${BENCHMARKS_NOT_RUN_IN_CI[@]+"${BENCHMARKS_NOT_RUN_IN_CI[@]}"}; do
    [ "$name" = "$skip" ] && excluded=true && break
  done
  $excluded || to_run="${to_run}${to_run:+$'\n'}${pkg}"$'\t'"${name}"
done <<< "$enumerated"

[ -n "$to_run" ] || fail "every enumerated benchmark is in BENCHMARKS_NOT_RUN_IN_CI — nothing would be measured"

# An explicit alternation rather than `.`, so an excluded benchmark is not
# executed at all. Anchored, so `BenchmarkFoo` cannot select `BenchmarkFooBar`.
selector="$(printf '%s\n' "$to_run" | cut -f2 | sort -u | paste -sd '|' -)"
selector="^(${selector})$"

# `-benchtime=100x` rather than the default 1s per benchmark: a fixed iteration
# count keeps the CI step bounded and the allocation figures (the part worth
# comparing across runs) exact, while wall-clock ns/op on a shared runner is
# indicative at best either way.
BENCHTIME="${SHARDPILOT_BENCHTIME:-100x}"

echo "running benchmarks (-benchtime=${BENCHTIME}, -benchmem) …" >&2

# `-run '^$'` so no TEST runs here — this step measures, the test step tests,
# and mixing them makes a benchmark failure look like a test failure.
if ! go test -run '^$' -bench "$selector" -benchmem -benchtime="$BENCHTIME" ./... >"$raw" 2>&1; then
  cat "$raw" >&2
  fail "benchmark run exited non-zero"
fi

cat "$raw"

if [ -n "$output_file" ]; then
  mkdir -p "$(dirname "$output_file")"
  cp "$raw" "$output_file"
  echo "wrote $output_file" >&2
fi

# ---------------------------------------------------------------------------
# 4. The EXECUTION check: every benchmark that should have run, reported —
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
  echo "  nothing. Usual causes: a build tag excluding it on this platform, a" >&2
  echo "  t.Skip/b.Skip at the top, or a package './...' does not walk. If it" >&2
  echo "  genuinely cannot run here, add its NAME to BENCHMARKS_NOT_RUN_IN_CI" >&2
  echo "  in $0 and say why." >&2
  fail "$(printf '%s\n' "$missing" | wc -l | tr -d ' ') benchmark(s) reported no result"
fi

ran="$(printf '%s\n' "$to_run" | wc -l | tr -d ' ')"
echo >&2
echo "OK: ${ran} benchmark(s) executed and reported results." >&2
