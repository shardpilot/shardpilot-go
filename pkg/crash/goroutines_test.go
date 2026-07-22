package crash

import (
	"strings"
	"sync"
	"testing"
)

// A runtime.Stack(all)-shaped fixture: a running goroutine, a parked one with a
// duration-qualified state + a created-by pair, an IO-wait one whose leading
// runtime park frames must be trimmed, an all-runtime one that must be kept
// whole (with an elided-frames marker and an offset-less file line), and a
// truncated trailing header that must be dropped.
const goroutineDumpFixture = "goroutine 1 [running]:\n" +
	"main.main()\n" +
	"\t/home/build/work/app/main.go:10 +0x1a\n" +
	"\n" +
	"goroutine 18 [chan receive, 3 minutes]:\n" +
	"github.com/acme/game/server.(*Loop).run(0xc000010000, {0x5f2e80, 0xc00001c030})\n" +
	"\t/home/build/work/app/server/loop.go:42 +0x25\n" +
	"created by github.com/acme/game/server.Start in goroutine 1\n" +
	"\t/home/build/work/app/server/start.go:12 +0x39\n" +
	"\n" +
	"goroutine 33 [IO wait]:\n" +
	"runtime.gopark(0xc000049718?, 0x2?, 0x8?, 0x0?, 0xc0000497b4?)\n" +
	"\t/usr/local/go/src/runtime/proc.go:402 +0xce\n" +
	"internal/poll.(*pollDesc).wait(0xc000106000?, 0xc000118000?, 0x0)\n" +
	"\t/usr/local/go/src/internal/poll/fd_poll_runtime.go:84 +0x27\n" +
	"net.(*conn).Read(0xc000012008, {0xc000118000?, 0x0?, 0x0?})\n" +
	"\t/usr/local/go/src/net/net.go:179 +0x45\n" +
	"\n" +
	"goroutine 40 [sleep]:\n" +
	"runtime.goparkunlock(...)\n" +
	"\t/usr/local/go/src/runtime/proc.go:408\n" +
	"...additional frames elided...\n" +
	"\n" +
	"goroutine 51 [runn"

func TestParseGoroutineDump(t *testing.T) {
	records := parseGoroutineDump(goroutineDumpFixture)
	if len(records) != 4 {
		t.Fatalf("parsed %d records, want 4 (the truncated header must be dropped): %+v", len(records), records)
	}

	if records[0].id != "1" || records[0].state != "running" {
		t.Fatalf("record 0 header: %+v", records[0])
	}
	if len(records[0].frames) != 1 || records[0].frames[0] != (Frame{Function: "main.main", File: "app/main.go", Line: 10}) {
		t.Fatalf("record 0 frames: %+v", records[0].frames)
	}

	if records[1].id != "18" || records[1].state != "chan receive, 3 minutes" {
		t.Fatalf("record 1 header: %+v", records[1])
	}
	wantParked := []Frame{
		{Function: "server.(*Loop).run", File: "server/loop.go", Line: 42},
		{Function: "server.Start", File: "server/start.go", Line: 12},
	}
	if len(records[1].frames) != len(wantParked) {
		t.Fatalf("record 1 frames: %+v", records[1].frames)
	}
	for i, want := range wantParked {
		if records[1].frames[i] != want {
			t.Fatalf("record 1 frame %d = %+v, want %+v", i, records[1].frames[i], want)
		}
	}

	// Leading runtime park frames trimmed: the application-level position leads.
	if records[2].id != "33" || len(records[2].frames) != 2 {
		t.Fatalf("record 2: %+v", records[2])
	}
	if records[2].frames[0].Function != "poll.(*pollDesc).wait" || records[2].frames[1].Function != "net.(*conn).Read" {
		t.Fatalf("record 2 frames: %+v", records[2].frames)
	}

	// An ALL-runtime goroutine keeps its frames (there is nothing beneath to
	// lead), the offset-less file line parses, and the elided marker is skipped.
	if records[3].id != "40" || records[3].state != "sleep" {
		t.Fatalf("record 3 header: %+v", records[3])
	}
	if len(records[3].frames) != 1 || records[3].frames[0] != (Frame{Function: "runtime.goparkunlock", File: "runtime/proc.go", Line: 408}) {
		t.Fatalf("record 3 frames: %+v", records[3].frames)
	}
}

func TestCurrentGoroutineIDIsNumeric(t *testing.T) {
	id := currentGoroutineID()
	if id == "" {
		t.Fatal("currentGoroutineID is empty")
	}
	for _, r := range id {
		if r < '0' || r > '9' {
			t.Fatalf("currentGoroutineID %q is not numeric", id)
		}
	}
}

// parkedForGoroutineTest is the distinctive park point the live-capture
// assertions look for on a captured extra thread.
func parkedForGoroutineTest(release <-chan struct{}, started *sync.WaitGroup) {
	started.Done()
	<-release
}

func TestExtraGoroutineThreadsExcludesSelfAndBudgets(t *testing.T) {
	release := make(chan struct{})
	var started sync.WaitGroup
	for i := 0; i < 5; i++ {
		started.Add(1)
		go parkedForGoroutineTest(release, &started)
	}
	started.Wait()
	defer close(release)

	if got := extraGoroutineThreads(0); got != nil {
		t.Fatalf("zero budget must capture nothing, got %d threads", len(got))
	}

	// A tiny budget truncates deterministically no matter which goroutines the
	// scheduler lists first.
	tight := 0
	for _, thread := range extraGoroutineThreads(3) {
		tight += len(thread.Frames)
	}
	if tight > 3 {
		t.Fatalf("tight budget of 3 captured %d frames", tight)
	}

	// A generous budget captures every live goroutine, including the parked ones.
	budget := 200
	threads := extraGoroutineThreads(budget)
	if len(threads) == 0 {
		t.Fatal("no extra goroutine threads captured")
	}
	self := currentGoroutineID()
	total := 0
	seen := map[string]bool{}
	foundParked := false
	for _, thread := range threads {
		if thread.ID == "goroutine-"+self {
			t.Fatalf("captured the calling goroutine %q as an extra thread", thread.ID)
		}
		if !strings.HasPrefix(thread.ID, "goroutine-") {
			t.Fatalf("thread id %q lacks the goroutine- prefix", thread.ID)
		}
		if seen[thread.ID] {
			t.Fatalf("duplicate thread id %q", thread.ID)
		}
		seen[thread.ID] = true
		if thread.Crashed {
			t.Fatalf("extra thread %q marked crashed", thread.ID)
		}
		if len(thread.Frames) == 0 || len(thread.Frames) > perGoroutineFrameCap {
			t.Fatalf("thread %q frame count %d outside (0, %d]", thread.ID, len(thread.Frames), perGoroutineFrameCap)
		}
		total += len(thread.Frames)
		for _, frame := range thread.Frames {
			if strings.Contains(frame.Function, "parkedForGoroutineTest") {
				foundParked = true
			}
		}
	}
	if total > budget {
		t.Fatalf("captured %d frames across extras, budget was %d", total, budget)
	}
	if !foundParked {
		t.Fatalf("no captured thread parks in parkedForGoroutineTest; threads: %+v", threads)
	}
}
