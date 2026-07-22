package crash

import (
	"runtime"
	"strconv"
	"strings"
)

// All-goroutine capture (ADR-0297 §7d): with
// ClientOptions.AllGoroutineCaptureEnabled on, auto-capture snapshots every
// goroutine at panic time (runtime.Stack all) and ships the OTHER goroutines as
// additional pre-symbolicated threads[] beside the precise crashed thread. The
// text dump is parsed into structured frames — the wire contract has no
// attachment lane, and structured threads group/symbolicate like every other
// thread — under the event's existing caps: 64 threads and 256 total frames,
// nearest-first in dump order, with each non-crashing goroutine truncated to
// perGoroutineFrameCap frames.

const (
	// perGoroutineFrameCap bounds frames kept per NON-crashing goroutine. The top
	// frames carry a parked goroutine's identity (its park point and call path);
	// the deep tail mostly repeats scheduler plumbing, and the 256-total-frame
	// event cap has to be shared across every captured goroutine.
	perGoroutineFrameCap = 16

	// allGoroutineDumpStart / allGoroutineDumpCap bound the runtime.Stack(all)
	// buffer: start at 64 KiB, double while truncated, give up growing at 1 MiB —
	// the parser tolerates a truncated tail (the incomplete last record is
	// dropped), and the thread/frame caps mean a larger dump could not ship
	// anyway.
	allGoroutineDumpStart = 64 << 10
	allGoroutineDumpCap   = 1 << 20
)

// extraGoroutineThreads captures and parses the all-goroutine dump into Threads
// for every goroutine EXCEPT the calling one (the precise crashed thread already
// represents it), spending at most frameBudget frames across at most
// maxThreads-1 threads.
func extraGoroutineThreads(frameBudget int) []Thread {
	if frameBudget <= 0 {
		return nil
	}
	self := currentGoroutineID()
	records := parseGoroutineDump(captureAllGoroutineDump())
	threads := make([]Thread, 0, len(records))
	for _, rec := range records {
		if rec.id == self {
			continue
		}
		if len(threads) >= maxThreads-1 || frameBudget <= 0 {
			break
		}
		frames := rec.frames
		if len(frames) > perGoroutineFrameCap {
			frames = frames[:perGoroutineFrameCap]
		}
		if len(frames) > frameBudget {
			frames = frames[:frameBudget]
		}
		if len(frames) == 0 {
			continue
		}
		for i := range frames {
			frames[i].Index = i
		}
		frameBudget -= len(frames)
		threads = append(threads, Thread{
			ID:     "goroutine-" + rec.id,
			Name:   rec.state,
			Frames: frames,
		})
	}
	return threads
}

// captureAllGoroutineDump returns the runtime.Stack(all) text, growing the
// buffer until the dump fits or the cap is reached.
func captureAllGoroutineDump() string {
	size := allGoroutineDumpStart
	for {
		buf := make([]byte, size)
		n := runtime.Stack(buf, true)
		if n < len(buf) || size >= allGoroutineDumpCap {
			return string(buf[:n])
		}
		size *= 2
	}
}

// currentGoroutineID parses the calling goroutine's id from its own stack
// header ("goroutine 123 [running]:"). Empty on an unexpected header shape.
func currentGoroutineID() string {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := strings.TrimPrefix(string(buf[:n]), "goroutine ")
	if i := strings.IndexByte(s, ' '); i > 0 {
		return s[:i]
	}
	return ""
}

type goroutineRecord struct {
	id     string
	state  string
	frames []Frame
}

// parseGoroutineDump parses runtime.Stack(all) text into per-goroutine records.
// Each record is a "goroutine <id> [<state>]:" header followed by call pairs — a
// function line ("pkg.fn(args)") and its "\tfile.go:123 +0xoff" location — and
// optionally a trailing "created by <fn> in goroutine <id>" pair naming the
// spawn site. Function names are rendered exactly like the precise capture
// (shortFuncName), files pass trimBuildPath, and leading runtime.* park frames
// are trimmed per goroutine so the application-level position leads — unless the
// goroutine is ALL runtime frames (a system goroutine), which is kept whole.
// Unrecognized lines (elided-frame markers, a truncated tail) are skipped;
// malformed input never panics (this runs inside crash capture).
func parseGoroutineDump(dump string) []goroutineRecord {
	var records []goroutineRecord
	lines := strings.Split(dump, "\n")
	i := 0
	for i < len(lines) {
		id, state, ok := parseGoroutineHeader(lines[i])
		if !ok {
			i++
			continue
		}
		i++
		var frames []Frame
		for i < len(lines) {
			line := lines[i]
			if line == "" {
				i++
				break
			}
			if _, _, isHeader := parseGoroutineHeader(line); isHeader {
				break
			}
			fn, ok := parseDumpFunctionLine(line)
			i++
			if !ok {
				continue
			}
			frame := Frame{Function: fn}
			if i < len(lines) && strings.HasPrefix(lines[i], "\t") {
				frame.File, frame.Line = parseDumpFileLine(lines[i])
				i++
			}
			frames = append(frames, frame)
		}
		records = append(records, goroutineRecord{
			id:     id,
			state:  state,
			frames: trimLeadingRuntimeFrames(frames),
		})
	}
	return records
}

// parseGoroutineHeader matches "goroutine <id> [<state>]:" — the state may carry
// qualifiers ("chan receive, 5 minutes", "select, locked to thread").
func parseGoroutineHeader(line string) (id, state string, ok bool) {
	rest, found := strings.CutPrefix(line, "goroutine ")
	if !found || !strings.HasSuffix(rest, ":") {
		return "", "", false
	}
	sp := strings.IndexByte(rest, ' ')
	if sp <= 0 {
		return "", "", false
	}
	id = rest[:sp]
	if _, err := strconv.ParseUint(id, 10, 64); err != nil {
		return "", "", false
	}
	open := strings.IndexByte(rest, '[')
	closing := strings.LastIndexByte(rest, ']')
	if open < 0 || closing < open {
		return "", "", false
	}
	return id, rest[open+1 : closing], true
}

// parseDumpFunctionLine extracts the function symbol from a dump call line. A
// regular call line ends with its argument list ("pkg.(*T).work(0xc000010000)");
// a "created by pkg.spawn in goroutine 1" line names the spawn site. Anything
// else (elided-frame markers, truncated tails) is skipped.
func parseDumpFunctionLine(line string) (string, bool) {
	s := strings.TrimSpace(line)
	if target, found := strings.CutPrefix(s, "created by "); found {
		if at := strings.Index(target, " in goroutine "); at >= 0 {
			target = target[:at]
		}
		target = shortFuncName(strings.TrimSpace(target))
		return target, target != ""
	}
	if !strings.HasSuffix(s, ")") {
		return "", false
	}
	// The LAST '(' opens the argument list; earlier parens belong to method
	// receivers ("pkg.(*T).work").
	cut := strings.LastIndexByte(s, '(')
	if cut <= 0 {
		return "", false
	}
	fn := shortFuncName(strings.TrimSpace(s[:cut]))
	return fn, fn != ""
}

// parseDumpFileLine parses "\t/abs/path/file.go:123 +0x2f" into the
// build-path-trimmed file and line. Zero-valued on malformed input.
func parseDumpFileLine(line string) (string, int) {
	s := strings.TrimSpace(line)
	if at := strings.LastIndex(s, " +0x"); at >= 0 {
		s = s[:at]
	}
	colon := strings.LastIndexByte(s, ':')
	if colon <= 0 {
		return "", 0
	}
	num, err := strconv.Atoi(s[colon+1:])
	if err != nil || num < 0 {
		return "", 0
	}
	return trimBuildPath(s[:colon]), num
}

// trimLeadingRuntimeFrames drops the runtime park/scheduler frames above a
// goroutine's application-level position, keeping the record whole when it has
// no application frames at all (a runtime system goroutine).
func trimLeadingRuntimeFrames(frames []Frame) []Frame {
	trimmed := frames
	for len(trimmed) > 0 && strings.HasPrefix(trimmed[0].Function, "runtime.") {
		trimmed = trimmed[1:]
	}
	if len(trimmed) == 0 {
		return frames
	}
	return trimmed
}
