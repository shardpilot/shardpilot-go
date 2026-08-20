//go:build ignore

// benchlist answers one question for scripts/run_benchmarks.sh: which
// benchmark functions are declared in these Go files, and which of them can
// this build configuration actually run?
//
// It reads NUL-separated, repository-relative paths on stdin and prints one
// line per declaration found:
//
//	A\t<import path>\t<BenchmarkName>\t<file>   this configuration compiles it
//	I\t<import path>\t<BenchmarkName>\t<file>   it does not
//	X\t<import path>\t<file>                    a compiled TestMain never calls m.Run
//
// The file is there because the manifest records DECLARATIONS, not identities.
// One package may define the same benchmark name in mutually exclusive
// platform files; collapsing those to one row makes deleting either of them
// invisible, which is the deletion the manifest exists to expose.
//
// The active/inactive split is NOT decided here. The third argument names a
// file holding the test files `go list` says this configuration compiles, and
// a declaration is active exactly when its file is in that set.
//
// That indirection is the point. Deciding it here meant re-deriving "what
// would go test compile", and every input that answer depends on is another
// chance to get it wrong: -tags (whose value has its own space-separated
// grammar), -race, a CGO_ENABLED persisted with `go env -w`, and whatever is
// added next. The go command already computes it, and its answer cannot
// disagree with itself.
//
// The second argument, the effective GOFLAGS, is read only to refuse
// `-overlay`: it makes the built content differ from the tracked files this
// tool reads, and those two answers cannot be reconciled here.
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
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: benchlist <module-path> <GOFLAGS> <compiled-list-file> < NUL-separated-paths")
		os.Exit(2)
	}
	module := os.Args[1]

	if err := refuseOverlay(os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, "benchlist:", err)
		os.Exit(1)
	}

	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchlist: cwd:", err)
		os.Exit(1)
	}

	compiled, err := compiledSet(os.Args[3], cwd)
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchlist:", err)
		os.Exit(1)
	}

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

		tag := "I"
		if compiled[rel] {
			tag = "A"
		}

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name == nil {
				continue
			}
			if !isBenchmarkFunc(fn) {
				continue
			}

			// The manifest is line- and tab-separated and records the declaring
			// file, so a DECLARATION in a file whose name carries either
			// delimiter has no representable row. The refusal belongs here, at
			// the declaration, and not at the file: an ordinary test in a
			// tab-named file needs no manifest row and `go test` runs it
			// perfectly well, so refusing the repository over it was a refusal
			// justified by the shape of a name rather than by a consequence.
			if strings.ContainsAny(rel, "\t\n") {
				fmt.Fprintf(os.Stderr, "benchlist: %q: a benchmark declared in a file whose name "+
					"contains a tab or a newline cannot be recorded in the manifest\n", rel)
				failed = true

				continue
			}

			fmt.Fprintf(out, "%s\t%s\t%s\t%s\n", tag, pkg, fn.Name.Name, rel)
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

// splitQuoted mirrors cmd/internal/quoted.Split, which is what the go command
// uses to read GOFLAGS.
//
// The rule that is easy to get wrong — and that I did get wrong — is WHERE a
// quote counts. A quote is a delimiter only at the start of a field; one
// further in is an ordinary character, as the upstream comment says outright:
// "Quotes further inside the string do not count." So
//
//	-ldflags=-X=example.com/x.Version='dev
//
// is one perfectly valid field that `go test` accepts, not an unterminated
// string. Treating that apostrophe as an opening quote made this refuse a
// configuration the go command is happy with.
func splitQuoted(s string) ([]string, error) {
	var fields []string

	for len(s) > 0 {
		for len(s) > 0 && isSpaceByte(s[0]) {
			s = s[1:]
		}
		if len(s) == 0 {
			break
		}

		if s[0] == '"' || s[0] == '\'' {
			quote := s[0]
			s = s[1:]
			i := 0
			for i < len(s) && s[i] != quote {
				i++
			}
			if i >= len(s) {
				return nil, fmt.Errorf("unterminated %c string", quote)
			}
			fields = append(fields, s[:i])
			s = s[i+1:]

			continue
		}

		i := 0
		for i < len(s) && !isSpaceByte(s[i]) {
			i++
		}
		fields = append(fields, s[:i])
		s = s[i:]
	}

	return fields, nil
}

func isSpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// compiledSet reads the paths `go list` reported for this configuration and
// returns them as repository-relative names, matching the form the tracked
// file list arrives in.
func compiledSet(listFile, cwd string) (map[string]bool, error) {
	data, err := os.ReadFile(listFile)
	if err != nil {
		return nil, fmt.Errorf("reading the compiled-file list: %w", err)
	}

	set := map[string]bool{}
	for _, p := range strings.Split(string(data), "\x00") {
		// `go list -f` writes a newline after each package's template output,
		// which lands between two NULs as a record of its own.
		if strings.TrimSpace(p) == "" {
			continue
		}
		rel, err := filepath.Rel(cwd, p)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		set[filepath.ToSlash(rel)] = true
	}
	// No vacuity guard here on purpose: an empty set makes every declaration
	// inactive, and refusing to report a pass over nothing is already the
	// caller's job one step later.
	return set, nil
}

// refuseOverlay rejects a GOFLAGS carrying -overlay.
//
// An overlay makes `go test` build file contents that are not the ones on
// disk, while everything here is read from the tracked working tree. The two
// answers about which benchmarks exist would then differ with nothing to
// reconcile them: an overlay that adds a benchmark leaves it undeclared,
// unselected and unasserted, and CI reports success over it.
//
// GOFLAGS is split with the grammar cmd/internal/quoted.Split implements,
// which is what the go command uses to read it — `GOFLAGS="'-overlay=x.json'"`
// is a valid way to set it, and a plain whitespace split would not see the
// flag through the quotes.
//
// The caller must build THIS FILE with overlays disabled. Otherwise an overlay
// can replace the guard with something that approves of it, which is not a
// hypothetical: swapping this file for one that printed the expected rows made
// a run carrying an extra overlaid benchmark report OK over it.
func refuseOverlay(goflags string) error {
	fields, err := splitQuoted(goflags)
	if err != nil {
		return fmt.Errorf("GOFLAGS: %w", err)
	}

	// Last assignment wins, as it does in Go's own flag parsing: with
	// `-overlay=x.json -overlay=` the empty one is effective and no overlay is
	// applied, so refusing on the earlier field would reject a configuration
	// under which disk and build contents cannot differ.
	//
	// A bare `-overlay` with no value is left alone: the go command rejects it
	// as a usage error, and its message is the accurate one.
	overlay := ""
	for _, f := range fields {
		for _, prefix := range []string{"-overlay=", "--overlay="} {
			if v, ok := strings.CutPrefix(f, prefix); ok {
				overlay = v
			}
		}
	}
	if overlay != "" {
		return fmt.Errorf("GOFLAGS sets -overlay=%s; this reads the tracked files on disk, so "+
			"an overlay's benchmarks would be built but never declared or asserted", overlay)
	}

	return nil
}

// skipped reports whether a file sits INSIDE a vendored tree, which `./...`
// never walks. Vendored code is not authored here, so demanding that the
// repository declare its benchmarks would be noise rather than a signal.
//
// "Inside" is the whole subtlety. A directory named `vendor` that itself holds
// code is an ordinary package — cmd/go's own documentation notes that
// `cmd/vendor` is a command named vendor, not a vendored one — and `go list
// ./...` reports it and runs its benchmarks. Only a package BELOW such a
// directory is vendored, so the `vendor` element has to be followed by at
// least one more directory before the file name.
//
// This is the only walk rule left here. `testdata`, and directories beginning
// with `.` or `_`, need none: the go command does not walk them either, so
// their files are simply absent from the compiled set and come out inactive —
// which is what should happen, since a benchmark sitting in one needs someone
// to say out loud that it is not meant to run.
func skipped(rel string) bool {
	segs := strings.Split(rel, "/")

	// len(segs)-1 is the file name and len(segs)-2 is its own directory; a
	// `vendor` element at or before that directory's parent is a vendored tree.
	for i := 0; i < len(segs)-2; i++ {
		if segs[i] == "vendor" {
			return true
		}
	}

	return false
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
