//go:build ignore

// benchcheck reads a `go test -json` event stream and answers one question per
// package: which of the benchmarks we asked for did NOT report a measurement.
//
// WHY JSON AND NOT THE TEXT OUTPUT
// --------------------------------
// `go test`'s text output is a SINGLE stream that the harness and the package
// under test both write to. Every rule written over that stream can be fooled,
// and in both directions:
//
//   - anchored at the start of a line, a `--- SKIP:` marker is missed when the
//     benchmark printed without a trailing newline, so the marker lands mid-line;
//   - searched for anywhere in the line, ANY benchmark printing that substring
//     marks another benchmark skipped — a benchmark that measured perfectly well
//     is then reported as never having run.
//
// Both were real findings against the awk version of this check. They are not
// two bugs, they are one: the text stream does not say who wrote a line.
//
// The JSON stream does. `go test -json` runs the binary in test2json mode, in
// which the harness frames its own writes, so:
//
//   - a genuine skip arrives as {"Action":"skip","Test":"BenchmarkX"} — an
//     EVENT, not a line of text, and the package cannot emit one;
//   - anything the package prints arrives as {"Action":"output"} ATTRIBUTED to
//     the benchmark that was running when it printed.
//
// So a relayed or forged marker is just output belonging to the benchmark that
// printed it, and a result row printed for someone else does not count for
// them. That last part narrows the forgery this tool cannot see: a benchmark
// can still print a plausible row for ITSELF, but no longer for its neighbours.
//
// Usage:
//
//	go test -json … | go run scripts/benchcheck.go <text-out> <expected-file>
//
// <text-out> is appended with the reconstructed human-readable output — the
// artifact CI publishes — and the names of benchmarks that reported nothing are
// printed to stdout, one per line.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// event is the subset of test2json's schema this needs. Unknown fields are
// ignored by encoding/json, so a future addition cannot break it.
type event struct {
	Action string `json:"Action"`
	Test   string `json:"Test"`
	Output string `json:"Output"`
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: benchcheck <text-out> <expected-file>")
		os.Exit(2)
	}

	expected, err := readExpected(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchcheck:", err)
		os.Exit(1)
	}

	text, err := os.OpenFile(os.Args[1], os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchcheck:", err)
		os.Exit(1)
	}
	out := bufio.NewWriter(text)

	skipped := map[string]bool{}
	reported := map[string]bool{}

	// An output EVENT is not a LINE. test2json emits one event per write from
	// the test binary, and the harness writes a result row in pieces — the
	// padded name first, the numbers after — so `BenchmarkX-4  \t` and
	// `100\t 47.87 ns/op` can arrive as two events. Whether they do is a matter
	// of timing, which made an event-at-a-time check pass or fail at random on
	// the same code. Lines are reassembled per test before anything reads them.
	pending := map[string]string{}

	in := bufio.NewScanner(os.Stdin)
	// A single output event can carry a long line; the default 64 KiB limit
	// would turn a chatty benchmark into a parse error.
	in.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for in.Scan() {
		line := in.Bytes()
		if len(line) == 0 {
			continue
		}

		var ev event
		if err := json.Unmarshal(line, &ev); err != nil {
			// Not an event: `go test` writes its own errors (a build failure,
			// say) to stderr as plain text. Keep it in the artifact rather than
			// discarding it, and let the caller judge by the exit status.
			fmt.Fprintf(out, "%s\n", line)

			continue
		}

		switch ev.Action {
		case "output":
			if _, err := out.WriteString(ev.Output); err != nil {
				fmt.Fprintln(os.Stderr, "benchcheck:", err)
				os.Exit(1)
			}
			buf := pending[ev.Test] + ev.Output
			for {
				i := strings.IndexByte(buf, '\n')
				if i < 0 {
					break
				}
				if name := reportedBy(ev.Test, buf[:i]); name != "" {
					reported[name] = true
				}
				buf = buf[i+1:]
			}
			pending[ev.Test] = buf
		case "skip":
			// EXACTLY the name, deliberately. A skipped sub-benchmark is
			// `Parent/child`, and a parent that measured its other children ran
			// perfectly well — so only the parent's own skip disqualifies it.
			if ev.Test != "" {
				skipped[ev.Test] = true
			}
		}
	}
	if err := in.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "benchcheck: reading the event stream:", err)
		os.Exit(1)
	}

	// A last line with no trailing newline is still a line.
	for test, buf := range pending {
		if name := reportedBy(test, buf); name != "" {
			reported[name] = true
		}
	}

	if err := out.Flush(); err != nil {
		fmt.Fprintln(os.Stderr, "benchcheck:", err)
		os.Exit(1)
	}
	if err := text.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "benchcheck:", err)
		os.Exit(1)
	}

	for _, name := range expected {
		if !reported[name] || skipped[name] {
			fmt.Println(name)
		}
	}
}

// readExpected reads the benchmark names this package was asked to run, one per
// line. An empty list is an error: with nothing expected every check below
// passes over nothing, which is the shape of failure this whole script exists
// to refuse.
func readExpected(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var names []string
	for _, line := range strings.Split(string(data), "\n") {
		if line != "" {
			names = append(names, line)
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("%s: no benchmark names to check", path)
	}

	return names, nil
}

// reportedBy reports which TOP-LEVEL benchmark an output line counts as a
// result for, or "" if it is not a result row.
//
// The attribution comes from the event, not from the text: `test` is the
// benchmark the harness says was running. The row must also NAME that same
// benchmark, so a line merely printed while it ran is not mistaken for its
// result.
func reportedBy(test, output string) string {
	if test == "" {
		return ""
	}

	// A sub-benchmark's row belongs to its parent: `b.Run` emits
	// `Parent/child-8` and no bare parent line, so demanding one would fail a
	// benchmark that ran.
	top := test
	if i := strings.Index(top, "/"); i >= 0 {
		top = top[:i]
	}

	fields := strings.Fields(output)
	// name, iteration count, a value, a unit.
	if len(fields) < 4 {
		return ""
	}

	name := strings.TrimSuffix(fields[0], "\n")
	// The `-<GOMAXPROCS>` suffix, then any `/sub` path.
	if i := strings.LastIndex(name, "-"); i > 0 {
		if _, err := strconv.Atoi(name[i+1:]); err == nil {
			name = name[:i]
		}
	}
	if i := strings.Index(name, "/"); i >= 0 {
		name = name[:i]
	}
	if name != top {
		return ""
	}

	if _, err := strconv.Atoi(fields[1]); err != nil {
		return ""
	}
	// b.ReportMetric takes any float64, so the value may be signed, NaN or Inf;
	// the unit is any non-empty token without whitespace, `3widgets/op`
	// included, which strings.Fields has already guaranteed.
	if _, err := strconv.ParseFloat(fields[2], 64); err != nil {
		return ""
	}

	return top
}
