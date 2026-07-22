package crash

import (
	"encoding/binary"
	"os"
	"regexp"
	"runtime"
	"testing"
	"time"
)

// buildNote encodes one ELF note record (namesz/descsz/type header, NUL-padded
// name, 4-byte-aligned desc) in little-endian, the fixture shape for
// parseELFNotes.
func buildNote(name string, typ uint32, desc []byte) []byte {
	nameBytes := append([]byte(name), 0)
	out := make([]byte, 12)
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(nameBytes)))
	binary.LittleEndian.PutUint32(out[4:8], uint32(len(desc)))
	binary.LittleEndian.PutUint32(out[8:12], typ)
	out = append(out, nameBytes...)
	for len(out)%4 != 0 {
		out = append(out, 0)
	}
	out = append(out, desc...)
	for len(out)%4 != 0 {
		out = append(out, 0)
	}
	return out
}

func TestParseELFNotesWalksConcatenatedRecords(t *testing.T) {
	gnuDesc := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a}
	goDesc := []byte("abc/def")
	data := append(buildNote("GNU", noteTypeGNUBuildID, gnuDesc), buildNote("Go", noteTypeGoBuildID, goDesc)...)

	notes := parseELFNotes(data, binary.LittleEndian)
	if len(notes) != 2 {
		t.Fatalf("parsed %d notes, want 2: %+v", len(notes), notes)
	}
	if notes[0].name != "GNU" || notes[0].typ != noteTypeGNUBuildID || string(notes[0].desc) != string(gnuDesc) {
		t.Fatalf("GNU note mismatch: %+v", notes[0])
	}
	if notes[1].name != "Go" || notes[1].typ != noteTypeGoBuildID || string(notes[1].desc) != string(goDesc) {
		t.Fatalf("Go note mismatch: %+v", notes[1])
	}
}

func TestParseELFNotesToleratesMalformedInput(t *testing.T) {
	gnu := buildNote("GNU", noteTypeGNUBuildID, []byte{0xaa, 0xbb})

	// A truncated trailing header yields the well-formed prefix.
	notes := parseELFNotes(append(append([]byte(nil), gnu...), 0x01, 0x02, 0x03), binary.LittleEndian)
	if len(notes) != 1 || notes[0].name != "GNU" {
		t.Fatalf("truncated tail: got %+v, want the one GNU note", notes)
	}

	// A desc size overrunning the buffer drops that record without panicking.
	overrun := make([]byte, 12)
	binary.LittleEndian.PutUint32(overrun[0:4], 0)
	binary.LittleEndian.PutUint32(overrun[4:8], 1<<30)
	binary.LittleEndian.PutUint32(overrun[8:12], 1)
	if notes := parseELFNotes(overrun, binary.LittleEndian); len(notes) != 0 {
		t.Fatalf("overrun desc: got %+v, want none", notes)
	}

	if notes := parseELFNotes(nil, binary.LittleEndian); len(notes) != 0 {
		t.Fatalf("nil input: got %+v, want none", notes)
	}
}

// TestReadSelfModuleOnTestBinary exercises the real self-read against the running
// test binary: a Go-linked ELF always carries the Go build-id note (a GNU note
// only when the linker was asked for one), so the resolved debug_id is lowercase
// hex either way — the GNU note bytes or the SHA-256 of the Go build id.
func TestReadSelfModuleOnTestBinary(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("self-module fill reads the ELF binary; GOOS=%s", runtime.GOOS)
	}
	module, ok := readSelfModule()
	if !ok {
		exe, _ := os.Executable()
		t.Fatalf("readSelfModule not ok for test binary %q", exe)
	}
	if module.Name == "" {
		t.Fatal("self-module name is empty")
	}
	hexID := regexp.MustCompile(`^[0-9a-f]{40,64}$`)
	if !hexID.MatchString(module.DebugID) {
		t.Fatalf("self-module debug_id %q is not 40-64 lowercase hex chars", module.DebugID)
	}
	if module.LoadAddress != selfModuleLoadAddress {
		t.Fatalf("self-module load_address = %q, want %q", module.LoadAddress, selfModuleLoadAddress)
	}
	if module.BuildID != "" {
		t.Fatalf("self-module build_id = %q, want empty (identity travels in debug_id)", module.BuildID)
	}

	// The module must survive the SDK sanitizer untouched and satisfy event
	// validation next to a pre-symbolicated frame — a scrub-mangled or invalid
	// self-module would drop the whole crash.
	event := Event{
		CrashID:    mustNewCrashID(t),
		OccurredAt: time.Now().UTC(),
		App:        AppInfo{ID: "fortress-fury"},
		Platform:   "linux",
		Exception:  ExceptionInfo{Type: "panic"},
		Modules:    []Module{module},
		Threads:    []Thread{{ID: "main", Crashed: true, Frames: []Frame{{Function: "app.run", File: "app/run.go", Line: 7}}}},
	}
	sanitized, err := sanitizeEvent(event, true)
	if err != nil {
		t.Fatalf("sanitizeEvent: %v", err)
	}
	if len(sanitized.Modules) != 1 || sanitized.Modules[0] != module {
		t.Fatalf("sanitizer changed the self-module: %+v", sanitized.Modules)
	}
	if err := validateEvent(sanitized); err != nil {
		t.Fatalf("validateEvent: %v", err)
	}
}

func mustNewCrashID(t *testing.T) string {
	t.Helper()
	id, err := newCrashID()
	if err != nil {
		t.Fatalf("newCrashID: %v", err)
	}
	return id
}
