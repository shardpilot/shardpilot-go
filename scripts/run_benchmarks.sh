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
# The text output of `go test` is ONE stream that the harness and the package
# under test both write to, and it does not record which of them wrote a line.
# Every rule this script had over that stream was wrong in one direction or the
# other: anchored, it missed a `--- SKIP:` marker that landed mid-line; searched
# for anywhere, any benchmark printing that substring could disqualify another.
#
# So the run does not read text. `go test -json` puts the binary in test2json
# mode, where the harness's own actions are EVENTS a package cannot emit, and
# everything a package prints is attributed to whatever benchmark was running.
# A skip is {"Action":"skip"}; a printed marker is just that benchmark's output;
# a result row printed for someone else counts for nobody.
#
# What that leaves is narrow and worth stating exactly. Rows arrive attributed,
# so a package can no longer claim a measurement for a benchmark that did not
# run — rows printed with no benchmark running belong to no one, which is what
# a TestMain bypassing the harness produces. What remains is that the NUMBERS
# are the benchmark's own: a benchmark whose measured loop does nothing really
# did run, really does report, and no parsing distinguishes it from a fast one.
# That is a question about what the code measures, not about whether it ran,
# and this script only claims the latter.
#
# The rule that follows, learned the expensive way: a check earns its place by
# what it makes impossible for an ACCIDENT, because that is the only class it
# can decide from the outside. A check aimed at a liar buys a step of
# inconvenience at the price of refusing honest code, and this file has paid
# that price more than once.
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
# -z, for the same reason the test-file list uses it: WITHOUT it git C-quotes
# any path containing an unusual character — a newline, a quote, a non-ASCII
# byte under core.quotePath — wrapping the whole name in double quotes. A
# fixture at `testdata/fixture<newline>name/go.mod` then arrives as the literal
# `"testdata/fixture\nname/go.mod"`, whose first path component is `"testdata`
# rather than `testdata`, so the walk rules below miss it and the repository is
# refused over a module the go tool would never look at.
if ! git ls-files -z -- 'go.mod' '*/go.mod' >"$tmp/gomods_z"; then
  fail "git ls-files failed — cannot check for nested modules"
fi
# Only modules in directories `./...` can actually reach. `go help packages` is
# explicit that the go tool ignores directories named testdata and those
# beginning with `.` or `_`, so a module fixture under one of them is not a
# module this script would ever walk into — refusing the repository over
# testdata/example/go.mod protected nothing and blocked a normal way to write a
# test. Vendored trees follow the same rule benchlist.go uses: a `vendor`
# element ABOVE the module directory means the module is inside vendored code,
# while a go.mod in a directory that is itself named vendor is an ordinary
# nested module and still refused.
nested_modules=""
while IFS= read -r -d '' modfile; do
  [ "$modfile" = "go.mod" ] && continue

  # Split the DIRECTORY into components by hand rather than with `read -a`: a
  # here-string stops at the first newline, which is precisely the path this
  # check has to survive.
  segs=()
  rest="${modfile%/go.mod}"
  while :; do
    case "$rest" in
      */*)
        segs+=("${rest%%/*}")
        rest="${rest#*/}"
        ;;
      *)
        segs+=("$rest")
        break
        ;;
    esac
  done

  reachable=1
  for ((i = 0; i < ${#segs[@]}; i++)); do
    case "${segs[$i]}" in
      testdata | .* | _*)
        reachable=0
        break
        ;;
    esac
    # A `vendor` element ABOVE the module's own directory means the module sits
    # inside vendored code. A go.mod in a directory that is ITSELF named vendor
    # is an ordinary nested module — `cmd/vendor` is a command named vendor —
    # and stays refused. Same rule benchlist.go applies to packages.
    if [ "${segs[$i]}" = "vendor" ] && [ "$i" -lt "$((${#segs[@]} - 1))" ]; then
      reachable=0
      break
    fi
  done

  [ "$reachable" -eq 1 ] && nested_modules="${nested_modules}${nested_modules:+$'\n'}${modfile}"
done <"$tmp/gomods_z"
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

# There is deliberately no TestMain check here. A TestMain that never calls
# m.Run does bypass the harness — but the package then reports nothing, and the
# execution check below already names every benchmark that failed to report.
# The only case the syntactic check added was a package ALSO printing forged
# rows, and one unreachable `m.Run()` satisfied it while the harness still
# never started. It refused a legitimate TestMain that delegates through a
# helper, which is the cost it was charging for that. See WHAT THIS CANNOT DO.

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
  printf '%s\n' "$to_run" | awk -F'\t' -v p="$pkg" '$1 == p { print $2 }' | sort -u >"$tmp/expected"
  names="$(paste -sd '|' - <"$tmp/expected")"
  # Anchored, so `BenchmarkFoo` cannot also select `BenchmarkFooBar`.
  # `-run '^$'` so no TEST runs here — this step measures, the test step tests,
  # and mixing them makes a benchmark failure look like a test failure.
  #
  # `-json`, because the text output is one stream that the harness and the
  # package under test SHARE, and no rule written over it can tell which of them
  # wrote a line. Under -json the harness's own events are events — a skip is
  # {"Action":"skip"}, which a package cannot emit — and everything the package
  # prints is attributed to the benchmark that was running. scripts/benchcheck.go
  # reads that stream; the reasoning is in its header.
  #
  # -v is gone with it: -json puts the binary in test2json mode, which is
  # verbose by construction, so asking for both only duplicated the framing.
  #
  # A command-line flag beats GOFLAGS, so `GOFLAGS=-json=false` cannot quietly
  # turn the stream back into text and leave benchcheck reading nothing.
  set +e
  go test -json -run '^$' -bench "^(${names})"'$' -benchmem -benchtime="$BENCHTIME" "$pkg" 2>"$tmp/pkgerr" |
    GOFLAGS="$goflags -overlay=" go run scripts/benchcheck.go "$raw" "$tmp/expected" >"$tmp/pkgmissing" 2>"$tmp/checkerr"
  rc=("${PIPESTATUS[@]}")
  set -e
  if [ "${rc[0]}" -ne 0 ]; then
    cat "$raw" "$tmp/pkgerr" >&2
    fail "benchmark run exited non-zero for $pkg"
  fi
  if [ "${rc[1]}" -ne 0 ]; then
    cat "$tmp/checkerr" >&2
    fail "scripts/benchcheck.go failed for $pkg — see above"
  fi
  cat "$tmp/pkgerr" >>"$raw"
  pkg_missing="$(sed "s|^|${pkg}\t|" "$tmp/pkgmissing")"

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
