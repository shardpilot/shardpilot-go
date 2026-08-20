//go:build ignore

// benchlist answers one question for scripts/run_benchmarks.sh: which
// benchmark functions are declared in these Go files, and which of them can
// this build configuration actually run?
//
// It reads NUL-separated, repository-relative paths on stdin and prints one
// line per declaration found:
//
//	A\t<import path>\t<BenchmarkName>   this configuration compiles the file
//	I\t<import path>\t<BenchmarkName>   it does not
//
// Rows are printed once per DECLARATION, not per identity, so the caller can
// see a name declared twice in one package — which `go test` permits (an
// internal and an external test package may both define it), runs twice, and
// reports under one name.
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
// The active/inactive split comes from go/build rather than from `go test
// -list`, for a related reason: `-list` writes its answer to the same stdout
// the package under test writes to, so a package that prints a line beginning
// with "Benchmark" during listing invents a declaration that does not exist.
// go/build.MatchFile answers the same question about a file without running
// anything.
//
// Build-tagged `ignore` so it stays out of the module's package graph: it is a
// tool this repository runs, not part of the SDK it publishes. `go run
// scripts/benchlist.go` runs it anyway.
package main

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path"
	"path/filepath"
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

		dir := path.Dir(rel)
		pkg := module
		if dir != "." {
			pkg = module + "/" + dir
		}

		active, err := isActive(rel, dir)
		if err != nil {
			fmt.Fprintln(os.Stderr, "benchlist:", err)
			failed = true

			continue
		}
		tag := "I"
		if active {
			tag = "A"
		}

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name == nil {
				continue
			}
			if isBenchmarkFunc(fn) {
				fmt.Fprintf(out, "%s\t%s\t%s\n", tag, pkg, fn.Name.Name)
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
// respect completely: `./...` never walks a vendor directory. Vendored code is
// not authored here, so demanding that the repository declare its benchmarks
// would be noise rather than a signal.
func skipped(rel string) bool {
	for _, seg := range strings.Split(rel, "/") {
		if seg == "vendor" {
			return true
		}
	}

	return false
}

// isActive reports whether `go test ./...` would compile this file here.
//
// Two things have to be true. The go command must WALK the directory: it skips
// `testdata` and any element beginning with `.` or `_`. And the build context
// must ACCEPT the file: `//go:build` constraints and the `_windows_test.go`
// style name suffixes, both of which go/build already evaluates.
//
// `testdata` is deliberately reported inactive rather than skipped outright.
// The go command ignores it, which is exactly why a benchmark sitting there
// needs someone to say out loud that it is not meant to run — the difference
// between "excluded on purpose" and "moved here and forgotten".
func isActive(rel, dir string) (bool, error) {
	for _, seg := range strings.Split(rel, "/") {
		if seg == "testdata" || strings.HasPrefix(seg, ".") || strings.HasPrefix(seg, "_") {
			return false, nil
		}
	}

	match, err := build.Default.MatchFile(filepath.FromSlash(dir), path.Base(rel))
	if err != nil {
		return false, fmt.Errorf("%s: %w", rel, err)
	}

	return match, nil
}

// isBenchmarkFunc mirrors cmd/go's own test for which declarations `go test`
// registers as benchmarks — the name, and then the signature.
//
// The signature half is not pedantry. In a file the toolchain compiles, a
// benchmark-shaped declaration with the wrong signature is a build error, so
// checking the name alone is safe there. This tool exists precisely for the
// files the toolchain does NOT compile, where nothing rejects
// `func BenchmarkFixture()` — and treating that helper as a benchmark would
// demand a manifest line and an opt-out for something that can never run.
func isBenchmarkFunc(fn *ast.FuncDecl) bool {
	if !isBenchmarkName(fn.Name.Name) {
		return false
	}
	// cmd/go rejects a generic test function outright rather than registering
	// it, so it is not a benchmark either.
	if fn.Type.TypeParams != nil {
		return false
	}
	if fn.Type.Results != nil && len(fn.Type.Results.List) > 0 {
		return false
	}
	if fn.Type.Params == nil || len(fn.Type.Params.List) != 1 ||
		len(fn.Type.Params.List[0].Names) > 1 {
		return false
	}

	// The parameter must be a pointer to something named B. Which `testing`
	// it came from is unknowable from one file — the import may be aliased —
	// and cmd/go does not try to know either.
	ptr, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	if name, ok := ptr.X.(*ast.Ident); ok {
		return name.Name == "B"
	}
	if sel, ok := ptr.X.(*ast.SelectorExpr); ok {
		return sel.Sel.Name == "B"
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
