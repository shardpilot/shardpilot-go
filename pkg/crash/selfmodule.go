package crash

import (
	"crypto/sha256"
	"debug/elf"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
)

// Self-module identity (ADR-0297 §7d): with ClientOptions.DebugIDFillEnabled on,
// NewClient resolves the RUNNING BINARY's identity once and every auto-captured
// event carries it as the single modules[] entry. The debug_id is read from the
// binary itself, preferring the ELF GNU build-id note (rendered as lowercase hex —
// the exact identity `dump_syms` emits for ELF, so a crash joins `elf` symbols
// uploaded under that id) and falling back to the SHA-256 of the Go build id note
// (also lowercase hex; reproducible from the shipped binary via
// `printf %s "$(go tool buildid <binary>)" | sha256sum` — printf %s, because
// piping `go tool buildid` straight into sha256sum would hash its trailing
// newline, which the note bytes do not carry). Both renderings are deliberately
// hex-only: the raw Go build id is a '/'-separated string that path-shaped PII
// scrubbers (the SDK's own and the ingest server's) would mangle, which would
// invalidate the module and drop the whole crash.

const (
	// noteTypeGNUBuildID is NT_GNU_BUILD_ID in a ".note.gnu.build-id" ELF note.
	noteTypeGNUBuildID = 3
	// noteTypeGoBuildID is the Go linker's build-id note type in ".note.go.buildid".
	noteTypeGoBuildID = 4

	// selfModuleLoadAddress is the schema-required placeholder load address for the
	// self-module. Go auto-capture frames are pre-symbolicated (function/file/line,
	// never an instruction address), so no frame is ever resolved against this
	// module map — but the wire contract requires a load_address on every module.
	// 0x0 states "no address resolution here" without inventing a fake mapping.
	selfModuleLoadAddress = "0x0"

	// selfModuleFallbackName keeps the self-module valid when the executable's base
	// name would be blanked by the PII scrub (e.g. a binary named with a
	// raw-identifier prefix like user_service) — a blanked module name fails
	// validation and would drop the whole crash.
	selfModuleFallbackName = "main"
)

// readSelfModule resolves the running binary's self-module. ok is false when the
// binary is not readable ELF (non-ELF platforms, unlinked/replaced executable) or
// carries no identity note — capture then proceeds exactly as with the fill off.
func readSelfModule() (Module, bool) {
	exe, err := os.Executable()
	if err != nil {
		return Module{}, false
	}
	debugID, ok := readELFDebugID(exe)
	if !ok {
		return Module{}, false
	}
	name := sanitizeString(filepath.Base(exe))
	if name == "" {
		name = selfModuleFallbackName
	}
	return Module{
		Name:        name,
		DebugID:     debugID,
		LoadAddress: selfModuleLoadAddress,
	}, true
}

// readELFDebugID extracts the identity of an ELF binary: the GNU build-id note as
// lowercase hex when present, else the SHA-256 (lowercase hex) of the Go build id
// note. ok is false for non-ELF files and for ELF files with neither note.
func readELFDebugID(path string) (string, bool) {
	f, err := elf.Open(path)
	if err != nil {
		return "", false
	}
	defer f.Close()

	if desc, ok := sectionNoteDesc(f, ".note.gnu.build-id", "GNU", noteTypeGNUBuildID); ok && len(desc) > 0 {
		return hex.EncodeToString(desc), true
	}
	if desc, ok := sectionNoteDesc(f, ".note.go.buildid", "Go", noteTypeGoBuildID); ok && len(desc) > 0 {
		sum := sha256.Sum256(desc)
		return hex.EncodeToString(sum[:]), true
	}
	return "", false
}

// sectionNoteDesc returns the desc bytes of the first note with the wanted owner
// name and type in the named section.
func sectionNoteDesc(f *elf.File, section, wantName string, wantType uint32) ([]byte, bool) {
	s := f.Section(section)
	if s == nil {
		return nil, false
	}
	data, err := s.Data()
	if err != nil {
		return nil, false
	}
	for _, note := range parseELFNotes(data, f.ByteOrder) {
		if note.name == wantName && note.typ == wantType {
			return note.desc, true
		}
	}
	return nil, false
}

type elfNote struct {
	name string
	typ  uint32
	desc []byte
}

// parseELFNotes walks a SHT_NOTE section's payload: repeated
// namesz(4)/descsz(4)/type(4) headers followed by the NUL-padded name and the
// desc, each 4-byte aligned (the alignment both the GNU build-id and the Go
// build-id notes use). Malformed or truncated input yields the well-formed
// prefix — never a panic (this runs inside crash capture).
func parseELFNotes(data []byte, bo binary.ByteOrder) []elfNote {
	var notes []elfNote
	for len(data) >= 12 {
		nameSize := int(bo.Uint32(data[0:4]))
		descSize := int(bo.Uint32(data[4:8]))
		typ := bo.Uint32(data[8:12])
		data = data[12:]

		paddedName := align4(nameSize)
		if nameSize < 0 || paddedName < 0 || paddedName > len(data) {
			break
		}
		name := string(trimTrailingNULs(data[:nameSize]))
		data = data[paddedName:]

		paddedDesc := align4(descSize)
		if descSize < 0 || paddedDesc < 0 || paddedDesc > len(data) {
			break
		}
		desc := append([]byte(nil), data[:descSize]...)
		data = data[paddedDesc:]

		notes = append(notes, elfNote{name: name, typ: typ, desc: desc})
	}
	return notes
}

func align4(n int) int {
	return (n + 3) &^ 3
}

func trimTrailingNULs(b []byte) []byte {
	for len(b) > 0 && b[len(b)-1] == 0 {
		b = b[:len(b)-1]
	}
	return b
}
