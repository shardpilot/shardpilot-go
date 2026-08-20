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
# that exists in another. Anything that enumerates by building answers only for
# the platform and tags it runs under; a `//go:build windows` benchmark is
# invisible to it on a Linux runner — not run, not asserted, not reported
# missing.
#
# So:
#
#   1. DECLARED vs MANIFEST — did the SET change? `declared` is what the
#      toolchain can see here PLUS the benchmarks in tracked test files the
#      toolchain did not compile here. The manifest is committed, so adding or
#      deleting a benchmark fails until the same change updates it. That is one
#      line of reviewable diff, and the only place a human states "yes, this
#      benchmark is meant to be gone".
#   2. UNBUILDABLE ⊆ EXEMPT — a benchmark that exists but that this
#      configuration cannot build can never report a result here, so it must be
#      named in BENCHMARKS_NOT_RUN_IN_CI. Otherwise "it did not run" and "it
#      cannot run" are the same silence.
#   3. RUNNABLE vs RESULTS — did every benchmark that could run actually run?
#      This catches one going unreachable, skipped, or filtered out while still
#      sitting in the tree.
#
# THE ENUMERATOR IS THE GO GRAMMAR, NOT grep AND NOT STDOUT
# ---------------------------------------------------------
# What EXISTS comes from go/parser, over every tracked test file
# (scripts/benchlist.go). What RUNS HERE comes from `go list`, which already
# knows what this configuration compiles — build tags, -race, CGO_ENABLED, GOOS
# and the rest — so nothing here re-derives it and gets one of them wrong.
#
# Not a pattern, because `func /* requires API (Windows) */ BenchmarkFoo` is
# legal and gofmt-clean, and a pattern that skips the comment has to decide
# what a comment may contain. The answer is "anything", including the
# delimiters the pattern stops at. Under-reading is silent: the benchmark lands
# outside every check and CI reports success.
#
# Not `go test -list`, because its answer shares stdout with the package under
# test, so a diagnostic beginning with "Benchmark" becomes a declaration.
#
# Identity is package-qualified throughout. Two packages may define the same
# benchmark name, and a repository-wide name match would let either one satisfy
# both.
#
# WHAT THIS CANNOT DO
# -------------------
# Every byte the execution check reads is written by the package under test.
# `go test` offers its stdout and an exit status, and both are the program's.
# So a package that sets out to lie — printing result-shaped rows while
# bypassing the harness — cannot be caught by any amount of parsing, and these
# checks are calibrated for ACCIDENTS: renames, build tags, skips, unreachable
# packages, a TestMain that forgets m.Run. The one signal that is not the
# benchmark's to write is the harness's own `--- SKIP:` marker, and even that
# exists only once the harness has started.
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

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# ---------------------------------------------------------------------------
# 1. Establish what EXISTS, and what THIS configuration can run.
#
# Both answers come from scripts/benchlist.go: it parses every tracked test
# file with go/parser and asks go/build whether `go test ./...` would compile
# each one here. Rows come back tagged `A` (active) or `I` (inactive), one row
# per DECLARATION rather than per identity.
#
# `go test -list` is deliberately not the enumerator. It writes its answer to
# the same stdout the package under test writes to, so a package printing a
# line that begins with "Benchmark" while listing invents a declaration nobody
# wrote — and the manifest check then fails over a benchmark that does not
# exist. go/build answers the same question without running anything.
# ---------------------------------------------------------------------------

# One module, asserted rather than assumed. `go test ./...` from the root does
# not reach a nested module, while every import path here is derived from the
# ROOT module — so a nested module's benchmarks would be demanded under an
# import path that does not exist, and would never run even once declared.
# Refusing is honest; attributing them correctly and still never running them
# would merely look handled.
# 'go.mod' '*/go.mod' rather than '*go.mod': git's wildcard matches any run of
# characters, so '*go.mod' also selects an ordinary tracked file called
# something like fixtures/notgo.mod and would refuse the repository over it.
if ! git ls-files -- 'go.mod' '*/go.mod' >"$tmp/gomods"; then
  fail "git ls-files failed — cannot check for nested modules"
fi
nested_modules="$(grep -v '^go\.mod$' "$tmp/gomods" || true)"
if [ -n "$nested_modules" ]; then
  echo >&2
  echo "This repository has modules below the root:" >&2
  printf '%s\n' "$nested_modules" | sed 's/^/    /' >&2
  echo >&2
  echo "  Benchmarks inside them are not reached by 'go test ./...' here, and" >&2
  echo "  this script would file them under a root-derived import path that" >&2
  echo "  does not exist. Teach it to walk each module before adding one." >&2
  fail "nested modules are not supported"
fi

# -json=false on every go command whose stdout is read as text here: GOFLAGS
# may carry `-json`, which turns `go env` and `go list` into JSON objects and
# `go test` into a stream of test2json events. Inheriting that silently would
# make MODULE a JSON blob and every derived import path meaningless.
if ! MODULE="$(GOWORK=off go list -m -json=false 2>"$tmp/moderr")"; then
  cat "$tmp/moderr" >&2
  fail "go list -m failed — cannot map directories to import paths"
fi

# GOWORK=off is load-bearing, and is the whole reason this is not a bare
# `go list -m`: inside a Go workspace that prints EVERY module the workspace
# uses, and a multi-line module path yields import paths that no manifest or
# opt-out entry can ever match — so every benchmark outside the active build
# would read as undeclared, forever. `go.work` is gitignored here, so a
# contributor having one is expected rather than exotic.

# NUL-separated, so a test file whose name contains a space, a tab or a newline
# reaches the parser intact. Splitting on whitespace anywhere in this path
# would turn one real file into several that do not exist, and its benchmarks
# would drop out of every check below without failing anything.
if ! git ls-files -z -- '*_test.go' >"$tmp/tracked_z"; then
  fail "git ls-files failed — cannot determine which test files are tracked"
fi

# The effective GOFLAGS, read only so an `-overlay` can be refused: it makes
# `go test` build content that is not what is on disk, while everything here is
# read from the tracked working tree. `go env` rather than $GOFLAGS so a value
# set by `go env -w` counts too, and the string is passed on whole because
# GOFLAGS may legally be quoted — parsing it belongs where that grammar is.
if ! goflags="$(go env -json=false GOFLAGS 2>"$tmp/flagerr")"; then
  cat "$tmp/flagerr" >&2
  fail "go env GOFLAGS failed — cannot determine the build flags in effect"
fi

# Which test files THIS configuration compiles, asked of the go command rather
# than worked out here. That answer depends on -tags (whose value has its own
# space-separated grammar), -race, a CGO_ENABLED persisted by `go env -w`,
# GOOS, GOARCH — and on whatever is added next. Re-deriving it was a second
# description of the same set, free to disagree with the one that matters.
#
# One NUL around each path, so a file name containing a space, a tab or a
# newline survives, and `go list`'s own per-package newline lands between two
# NULs as a record of its own rather than glued to a path.
# shellcheck disable=SC2016  # $d is a Go template variable, not a shell one
if ! go list -e -json=false -f '{{$d := .Dir}}{{range .TestGoFiles}}{{"\x00"}}{{$d}}/{{.}}{{"\x00"}}{{end}}{{range .XTestGoFiles}}{{"\x00"}}{{$d}}/{{.}}{{"\x00"}}{{end}}' ./... >"$tmp/compiled_z" 2>"$tmp/listerr"; then
  cat "$tmp/listerr" >&2
  fail "go list failed — the set of compiled test files could not be determined"
fi

# The helper is compiled by the go command, so an overlay can REPLACE it — and
# the guard that refuses overlays would then be supplied by the overlay it is
# meant to refuse. Appending `-overlay=` makes the last assignment empty, so
# this one invocation is built from the tracked source; the captured GOFLAGS
# still goes in as an argument, so what gets validated is the real
# configuration rather than this deliberately weakened one.
if ! GOFLAGS="$goflags -overlay=" go run scripts/benchlist.go "$MODULE" "$goflags" "$tmp/compiled_z" <"$tmp/tracked_z" >"$tmp/rows" 2>"$tmp/rowserr"; then
  cat "$tmp/rowserr" >&2
  fail "scripts/benchlist.go refused or failed — see above"
fi

# `declared` is one row per DECLARATION, carrying the file. The manifest
# records declarations rather than identities because a package may define one
# benchmark name in mutually exclusive platform files, and collapsing those to
# a single row makes deleting either of them invisible — the exact deletion the
# manifest exists to expose. Identities are derived from it where identity is
# what matters: what runs, and what must be opted out.
declared="$(awk -F'\t' 'NF >= 4 { print $2 "\t" $3 "\t" $4 }' "$tmp/rows" | sort -u)"
declared_ids="$(printf '%s\n' "$declared" | cut -f1,2 | sed '/^$/d' | sort -u)"
active_rows="$(awk -F'\t' '$1 == "A" { print $2 "\t" $3 }' "$tmp/rows")"
no_harness="$(awk -F'\t' '$1 == "X" { print $2 }' "$tmp/rows" | sort -u)"
enumerated="$(printf '%s\n' "$active_rows" | sed '/^$/d' | sort -u)"

# Vacuity guard on the enumeration itself: reading zero names would make every
# check below pass over nothing and report success, which is the exact shape of
# failure this script exists to refuse.
if [ -z "$enumerated" ]; then
  fail "no runnable benchmarks found at all — refusing to report a pass over nothing"
fi

# TWO ACTIVE declarations of one identity. An internal and an external test
# package in the same directory may both define `BenchmarkFoo`; `go test` runs
# both and reports both under that one name. Either result would then satisfy
# the identity's single row, so deleting one implementation would go unnoticed
# — the deletion this whole manifest exists to catch. Nothing in the output
# distinguishes the two, so this refuses rather than pretending to.
dupes="$(printf '%s\n' "$active_rows" | sed '/^$/d' | sort | uniq -d)"
if [ -n "$dupes" ]; then
  echo >&2
  echo "These benchmark identities are declared twice in the same package:" >&2
  printf '%s\n' "$dupes" | sed 's/^/    /' >&2
  echo >&2
  echo "  Both run, both report under one name, and one result would satisfy" >&2
  echo "  the other — so losing either would be invisible here. Rename one." >&2
  fail "duplicate active benchmark declarations"
fi

unbuildable="$(printf '%s\n' "$declared_ids" | comm -23 - <(printf '%s\n' "$enumerated"))"

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
  echo "  above: '<' lines belong in $MANIFEST, '>' lines do not. Rows are one" >&2
  echo "  per DECLARATION and carry the file, so deleting one platform's copy of" >&2
  echo "  a name shows up even while another platform's copy still exists." >&2
  fail "tree and $MANIFEST disagree about which benchmarks exist"
fi

# An exemption naming a benchmark that no longer exists is a stale opt-out, and
# a stale opt-out is how a benchmark comes back and stays unmeasured.
cut -f1,2 "$tmp/manifest" | sort -u >"$tmp/manifest_ids"
stale="$(printf '%s\n' "$exempt" | sed '/^$/d' | comm -23 - "$tmp/manifest_ids")"
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

# A package whose TestMain never calls m.Run runs no benchmarks at all: the go
# command calls TestMain INSTEAD of the harness, so it exits zero having
# measured nothing — and with the harness never started there are no skip
# markers either, so anything the package prints reads as a result.
#
# This catches the honest mistake, a TestMain that forgets the harness. It does
# not close the class; see WHAT THIS CANNOT DO at the top of this file.
if [ -n "$no_harness" ]; then
  affected="$(printf '%s\n' "$no_harness" | comm -12 - <(printf '%s\n' "$to_run" | cut -f1 | sort -u))"
  if [ -n "$affected" ]; then
    echo >&2
    echo "These packages define a TestMain that never calls m.Run:" >&2
    printf '%s\n' "$affected" | sed 's/^/    /' >&2
    echo >&2
    echo "  go test calls TestMain INSTEAD of the harness, so their benchmarks" >&2
    echo "  never run and nothing reports them missing. Call m.Run." >&2
    fail "a TestMain would bypass the benchmark harness"
  fi
fi

# `-benchtime=100x` rather than the default 1s per benchmark: a fixed iteration
# count keeps the CI step bounded and the allocation figures (the part worth
# comparing across runs) exact, while wall-clock ns/op on a shared runner is
# indicative at best either way.
BENCHTIME="${SHARDPILOT_BENCHTIME:-100x}"

echo "running benchmarks (-benchtime=${BENCHTIME}, -benchmem) …" >&2

: >"$raw"
missing=""
while IFS= read -r pkg; do
  [ -n "$pkg" ] || continue
  names="$(printf '%s\n' "$to_run" | awk -F'\t' -v p="$pkg" '$1 == p { print $2 }' | sort -u | paste -sd '|' -)"
  # Anchored, so `BenchmarkFoo` cannot also select `BenchmarkFooBar`.
  # `-run '^$'` so no TEST runs here — this step measures, the test step tests,
  # and mixing them makes a benchmark failure look like a test failure.
  # -v so the harness emits `--- SKIP:` for a benchmark that skips. A result
  # row proves nothing on its own — the benchmark owns stdout and can print
  # one — but it cannot suppress its own skip marker, so the two together are
  # evidence where the row alone is not. The only cost is a bare name line per
  # benchmark in the published output.
  if ! go test -json=false -v -run '^$' -bench "^(${names})"'$' -benchmem -benchtime="$BENCHTIME" "$pkg" >"$tmp/pkgout" 2>&1; then
    cat "$raw" "$tmp/pkgout" >&2
    fail "benchmark run exited non-zero for $pkg"
  fi
  cat "$tmp/pkgout" >>"$raw"

  # -------------------------------------------------------------------------
  # The EXECUTION check, for THIS package, against the package name we invoked.
  #
  # Reading the package from a `pkg:` line in the output would be trusting the
  # package under test to tell the truth about itself: a benchmark printing
  # "pkg: anything" reassigns it, and every genuine result after that line is
  # filed under a package nobody asked for and reported missing. Packages are
  # run one at a time here, so the answer is already known.
  #
  # A result row is the name, optionally a `/sub` path when the benchmark uses
  # b.Run (Go emits `BenchmarkParent/child-8` and NO bare parent line, so
  # demanding one would fail a benchmark that ran perfectly well), optionally a
  # `-<GOMAXPROCS>` suffix, then the iteration count and the ns/op column.
  # -------------------------------------------------------------------------
  pkg_missing="$(
    awk -v want="$to_run" -v pkg="$pkg" '
      BEGIN {
        n = split(want, rows, "\n")
        for (i = 1; i <= n; i++) {
          split(rows[i], f, "\t")
          if (f[1] == pkg) expected[f[2]] = 1
        }
      }
      # Written by the harness, and a benchmark cannot suppress its own — which
      # is what makes it usable as evidence when a printed result row is not.
      #
      # Looked for ANYWHERE in the line, not just at its start: a benchmark that
      # prints without a trailing newline leaves the marker appended to its own
      # output, as in `BenchmarkX-8  100  1 ns/op--- SKIP: BenchmarkX`. An
      # anchored match misses exactly the case where the row is a forgery.
      #
      # Recorded under exactly the name printed, deliberately: a skipped
      # SUB-benchmark is marked `Parent/child`, which never matches the
      # top-level row `Parent`. So a child skipping cannot disqualify a parent
      # that measured its other children, and that falls out of the naming
      # rather than needing a rule of its own.
      {
        at = index($0, "--- SKIP: ")
        if (at > 0) {
          split(substr($0, at + 10), sf, /[ \t]/)
          if (sf[1] != "") skipped[sf[1]] = 1
        }
      }
      /^Benchmark/ {
        name = $1
        sub(/-[0-9]+$/, "", name)
        sub(/\/.*$/, "", name)
        # A harness row is: name, iteration count, a number, a unit. That
        # SHAPE is what separates a result from an ordinary diagnostic — not
        # any particular unit. Demanding `ns/op` specifically called a
        # benchmark reporting only custom metrics "no result", and
        # b.ReportMetric(0, "ns/op") suppressing the built-in unit is a
        # supported thing to do, so that was a false failure rather than a
        # strictness worth keeping.
        #
        # None of this makes the row TRUSTWORTHY — the benchmark owns stdout
        # and can print a perfectly shaped one — which is why the skip marker
        # above is what actually decides.
        # The value may be negative or non-finite: b.ReportMetric takes any
        # float64, so a benchmark reporting only custom metrics can legitimately
        # print `-1.000 items/op`, `NaN score` or `+Inf score`. Demanding a
        # leading digit called those "no result".
        if (NF >= 4 && $2 ~ /^[0-9]+$/ &&
            $3 ~ /^[-+]?([0-9]|[Nn][Aa][Nn]|[Ii][Nn][Ff])/ && $4 ~ /^[^0-9]/) reported[name] = 1
      }
      END {
        for (k in expected) if (!(k in reported) || (k in skipped)) print pkg "\t" k
      }
    ' "$tmp/pkgout"
  )"
  if [ -n "$pkg_missing" ]; then
    missing="${missing}${missing:+$'\n'}${pkg_missing}"
  fi
done <<< "$(printf '%s\n' "$to_run" | cut -f1 | sort -u)"

cat "$raw"

if [ -n "$output_file" ]; then
  mkdir -p "$(dirname "$output_file")"
  cp "$raw" "$output_file"
  echo "wrote $output_file" >&2
fi

missing="$(printf '%s\n' "$missing" | sed '/^$/d' | sort)"

if [ -n "$missing" ]; then
  echo >&2
  echo "These benchmarks exist but reported no result:" >&2
  printf '%s\n' "$missing" | sed 's/^/    /' >&2
  echo >&2
  echo "  'go test -bench' passes over a benchmark it cannot reach and says" >&2
  echo "  nothing. Usual causes: a b.Skip at the top, a benchmark whose body" >&2
  echo "  never reaches the measured loop, or one printing to stdout while it" >&2
  echo "  runs, which splits its own result row. If it genuinely cannot run" >&2
  echo "  here, add '<import path> <BenchmarkName>' to BENCHMARKS_NOT_RUN_IN_CI" >&2
  echo "  in $0 and say why." >&2
  fail "$(printf '%s\n' "$missing" | wc -l | tr -d ' ') benchmark(s) reported no result"
fi

ran="$(printf '%s\n' "$to_run" | wc -l | tr -d ' ')"
echo >&2
echo "OK: ${ran} benchmark(s) executed and reported results." >&2
