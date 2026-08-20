//go:build ignore

// benchlist answers one question for scripts/run_benchmarks.sh: which
// benchmark functions are declared in these Go files?
//
// It reads NUL-separated, repository-relative paths on stdin and prints one
// "<import path>\tBenchmarkName" line per declaration found.
//
// WHY THIS IS NOT A REGULAR EXPRESSION
// ------------------------------------
// The script needs the answer for files the toolchain did NOT compile — a
// build tag this platform does not satisfy, a directory `./...` does not walk.
// `go test -list` answers only for the configuration it is running in, and
// nothing will enumerate a configuration it is not running in, so those files
// have to be read directly.
//
// Reading them with a pattern does not work, and the failure is silent. Both
// of these are legal, gofmt-clean, and run under `go test`:
//
//	func /* reason */ BenchmarkFoo(b *testing.B)
//	func /* requires API (Windows) */ BenchmarkBar(b *testing.B)
//
// Any pattern that skips the comment has to decide what a comment may contain.
// The answer is "anything" — including the parentheses, braces and newlines
// such a pattern uses as its stopping conditions. A pattern that under-reads
// leaves a benchmark outside every check while CI reports success, which is
// the exact failure run_benchmarks.sh exists to refuse. go/parser already
// knows the grammar; this defers to it.
//
// Build-tagged `ignore` so it stays out of the module's package graph: it is a
// tool this repository runs, not part of the SDK it publishes. `go run
// scripts/benchlist.go` runs it anyway.
package main

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: benchlist <module-path> < NUL-separated-paths")
		os.Exit(2)
	}
	module := os.Args[1]

	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchlist: reading paths:", err)
		os.Exit(1)
	}

	out := bufio.NewWriter(os.Stdout)
	fset := token.NewFileSet()
	failed := false

	for _, rel := range strings.Split(string(data), "\x00") {
		if rel == "" || skipped(rel) {
			continue
		}

		// A tracked file that cannot be read or parsed is a loud failure, never
		// a file that quietly contributes no benchmarks. Silence here would put
		// its benchmarks outside every check with nothing to show for it.
		file, err := parser.ParseFile(fset, rel, nil, parser.SkipObjectResolution)
		if err != nil {
			fmt.Fprintln(os.Stderr, "benchlist:", err)
			failed = true

			continue
		}

		pkg := module
		if dir := path.Dir(rel); dir != "." {
			pkg = module + "/" + dir
		}

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name == nil {
				continue
			}
			if isBenchmarkName(fn.Name.Name) {
				fmt.Fprintf(out, "%s\t%s\n", pkg, fn.Name.Name)
			}
		}
	}

	if err := out.Flush(); err != nil {
		fmt.Fprintln(os.Stderr, "benchlist: writing results:", err)
		os.Exit(1)
	}
	if failed {
		os.Exit(1)
	}
}

// skipped mirrors the one exclusion the go command applies that this tool must
// respect: `./...` never walks a vendor directory. Vendored code is not
// authored here, so demanding that the repository declare its benchmarks would
// be noise rather than a signal. `testdata` is deliberately NOT skipped — the
// go command ignores it, which is exactly why a benchmark sitting there needs
// someone to say out loud that it is not meant to run.
func skipped(rel string) bool {
	for _, seg := range strings.Split(rel, "/") {
		if seg == "vendor" {
			return true
		}
	}

	return false
}

// isBenchmarkName mirrors cmd/go's own rule for which functions `go test`
// treats as benchmarks: the prefix, then either the end of the name or a
// character that is not a lower-case letter. `Benchmarkfoo` is not a benchmark,
// and reporting it as one would demand a manifest line for something that can
// never run.
func isBenchmarkName(name string) bool {
	const prefix = "Benchmark"

	if !strings.HasPrefix(name, prefix) {
		return false
	}
	if len(name) == len(prefix) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(name[len(prefix):])

	return !unicode.IsLower(r)
}
