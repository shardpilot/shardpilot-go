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
# Rename `BenchmarkEnqueueMapPayload`, move it behind a build tag, or mistype
# the pattern, and the CI step stays green over zero measurements — a check
# that has stopped checking, reporting success. So this asserts a MINIMUM
# COUNT of benchmarks that actually reported a result, and fails loudly below
# it. That assertion is the whole reason the file exists; everything else is
# plumbing.
#
# It deliberately does NOT gate on the numbers themselves. A performance
# threshold needs a baseline measured on hardware comparable to the runner, and
# a shared CI runner's variance would either be set so wide it catches nothing
# or so tight it fails on a noisy neighbour — the same trap the plan's timing
# work names. What this produces is a reproducible, published measurement; a
# regression gate is a separate decision with a baseline behind it.
#
# Usage:
#   scripts/run_benchmarks.sh                  # run, assert, print
#   scripts/run_benchmarks.sh out/bench.txt    # …and write the raw output there
set -euo pipefail

cd "$(dirname "$0")/.."

# ⚠ THE EXPECTED SET IS ENUMERATED FROM THE SOURCE, NOT LISTED HERE.
#
# This was a hand-written array, for a stated and good reason: a LIST names
# which benchmark disappeared where a COUNT only says one did. That reasoning
# is right and is kept — the failure below still names them. What changed is
# where the list comes from, because a hand-maintained one has a failure mode
# of its own, and it is the same one this script exists to close, one level up:
# add a benchmark and forget to add it here, and it runs unasserted forever;
# if it later stops running, nothing notices. The array cannot tell "this
# benchmark is gone" from "this benchmark was never added to the array".
#
# The same lesson has been learned elsewhere the hard way: a sibling project's
# lint step shellchecks an ENUMERATED file set, having previously carried a
# hand-maintained one that left a 1000-line script unchecked while CI reported a
# clean pass over files it had never opened. A list that must be updated by hand
# to stay honest will eventually not be.
#
# So: `git ls-files` for the names (tracked files only, so an untracked scratch
# test cannot add a phantom expectation), and an explicit exception list that is
# empty and expected to stay empty.
EXPECTED_BENCHMARKS=()
while IFS= read -r name; do
  [ -n "$name" ] || continue
  EXPECTED_BENCHMARKS+=("$name")
done < <(
  git ls-files '*_test.go' \
    | xargs -r grep -ho '^func Benchmark[A-Za-z0-9_]*' \
    | sed 's/^func //' \
    | sort -u
)

# The escape hatch, for a benchmark deliberately not run in CI — one needing
# hardware or a build tag CI does not have. Naming it here is a reviewable
# diff; the alternative is discovering the omission from a number that quietly
# stopped being measured. Empty on purpose.
BENCHMARKS_NOT_RUN_IN_CI=()
if [ "${#BENCHMARKS_NOT_RUN_IN_CI[@]}" -gt 0 ]; then
  kept=()
  for name in "${EXPECTED_BENCHMARKS[@]}"; do
    skip=false
    for excluded in "${BENCHMARKS_NOT_RUN_IN_CI[@]}"; do
      [ "$name" = "$excluded" ] && skip=true && break
    done
    $skip || kept+=("$name")
  done
  EXPECTED_BENCHMARKS=("${kept[@]+"${kept[@]}"}")
fi

# Vacuity guard on the enumeration itself: reading zero names would make the
# assertion below pass over nothing and report success, which is exactly the
# shape of failure this script was written to refuse.
if [ "${#EXPECTED_BENCHMARKS[@]}" -eq 0 ]; then
  echo "FAIL: no Benchmark functions found in tracked *_test.go files." >&2
  echo "      Either every benchmark was deleted, or the enumeration is broken." >&2
  echo "      Refusing to report a pass over nothing." >&2
  exit 1
fi

# `-benchtime=100x` rather than the default 1s per benchmark: a fixed iteration
# count keeps the CI step bounded and the allocation figures (the part worth
# comparing across runs) exact, while wall-clock ns/op on a shared runner is
# indicative at best either way.
BENCHTIME="${SHARDPILOT_BENCHTIME:-100x}"

output_file="${1:-}"
raw="$(mktemp)"
trap 'rm -f "$raw"' EXIT INT TERM

echo "running benchmarks (-benchtime=${BENCHTIME}, -benchmem) …" >&2

# `-run '^$'` so no TEST runs here — this step measures, the test step tests,
# and mixing them makes a benchmark failure look like a test failure.
if ! go test -run '^$' -bench . -benchmem -benchtime="$BENCHTIME" ./... >"$raw" 2>&1; then
  cat "$raw" >&2
  echo "FAIL: benchmark run exited non-zero" >&2
  exit 1
fi

cat "$raw"

if [ -n "$output_file" ]; then
  mkdir -p "$(dirname "$output_file")"
  cp "$raw" "$output_file"
  echo "wrote $output_file" >&2
fi

# THE ASSERTION. A benchmark that ran prints a result line beginning with its
# name followed by the iteration count; a benchmark that no longer exists
# prints nothing at all, and `go test` says nothing about it either.
missing=()
for name in "${EXPECTED_BENCHMARKS[@]}"; do
  if ! grep -Eq "^${name}(-[0-9]+)?[[:space:]]+[0-9]+" "$raw"; then
    missing+=("$name")
  fi
done

if [ "${#missing[@]}" -gt 0 ]; then
  echo >&2
  echo "FAIL: ${#missing[@]} expected benchmark(s) reported no result: ${missing[*]}" >&2
  echo "      A benchmark that is renamed, deleted or build-tagged out makes 'go test -bench' " >&2
  echo "      pass over nothing. If a benchmark was deleted on purpose this clears itself — " >&2
  echo "      the expected set is read from the tracked sources. If it must exist but cannot " >&2
  echo "      run here, add it to BENCHMARKS_NOT_RUN_IN_CI in $0 and say why." >&2
  exit 1
fi

echo >&2
echo "OK: ${#EXPECTED_BENCHMARKS[@]} benchmark(s) executed and reported results." >&2
