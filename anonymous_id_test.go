package shardpilot

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
)

func TestLoadOrCreateAnonymousIDRoundTripAndPerms(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shardpilot", "anonymous_id")

	created, err := LoadOrCreateAnonymousID(path)
	if err != nil {
		t.Fatalf("LoadOrCreateAnonymousID create: %v", err)
	}
	if !uuidv7.IsValid(created) {
		t.Fatalf("expected UUIDv7 anonymous ID, got %q", created)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat anonymous ID file: %v", err)
	}
	if runtime.GOOS != "windows" {
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Fatalf("expected 0600 anonymous ID file permissions, got %o", perm)
		}
	}

	loaded, err := LoadOrCreateAnonymousID(path)
	if err != nil {
		t.Fatalf("LoadOrCreateAnonymousID load: %v", err)
	}
	if loaded != created {
		t.Fatalf("expected stable anonymous ID round-trip, got %q then %q", created, loaded)
	}
}

func TestLoadOrCreateAnonymousIDRejectsBadInput(t *testing.T) {
	if _, err := LoadOrCreateAnonymousID(" "); err == nil {
		t.Fatal("expected empty path to be rejected")
	}

	corrupted := filepath.Join(t.TempDir(), "anonymous_id")
	if err := os.WriteFile(corrupted, []byte("not-a-uuid\n"), 0o600); err != nil {
		t.Fatalf("seed corrupted file: %v", err)
	}
	if _, err := LoadOrCreateAnonymousID(corrupted); err == nil {
		t.Fatal("expected corrupted anonymous ID file to be rejected")
	}
	data, err := os.ReadFile(corrupted)
	if err != nil || string(data) != "not-a-uuid\n" {
		t.Fatalf("expected corrupted file to be left untouched, got %q (err %v)", data, err)
	}
}

func TestCreateAnonymousIDLostRaceReturnsWinner(t *testing.T) {
	// Exercise the O_CREATE|O_EXCL EEXIST branch directly: the file appears
	// after the caller's initial read saw ErrNotExist (another process won
	// the creation race), so the winner's ID must be returned and the file
	// left untouched.
	path := filepath.Join(t.TempDir(), "anonymous_id")
	winner, err := uuidv7.New()
	if err != nil {
		t.Fatalf("generate winner ID: %v", err)
	}
	if err := os.WriteFile(path, []byte(winner+"\n"), 0o600); err != nil {
		t.Fatalf("seed winner file: %v", err)
	}

	got, err := createAnonymousID(path)
	if err != nil {
		t.Fatalf("createAnonymousID after lost race: %v", err)
	}
	if got != winner {
		t.Fatalf("expected the race winner's ID %q, got %q", winner, got)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != winner+"\n" {
		t.Fatalf("expected the winner's file to be left untouched, got %q (err %v)", data, err)
	}

	// A corrupt winner file surfaces an error instead of being overwritten.
	corrupt := filepath.Join(t.TempDir(), "anonymous_id")
	if err := os.WriteFile(corrupt, []byte("not-a-uuid\n"), 0o600); err != nil {
		t.Fatalf("seed corrupt winner file: %v", err)
	}
	if _, err := createAnonymousID(corrupt); err == nil {
		t.Fatal("expected a corrupt winner file to be rejected")
	}
	data, err = os.ReadFile(corrupt)
	if err != nil || string(data) != "not-a-uuid\n" {
		t.Fatalf("expected the corrupt file to be left untouched, got %q (err %v)", data, err)
	}
}

func TestCreateAnonymousIDRemovesPartialFileOnWriteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "anonymous_id")
	injected := errors.New("injected write failure: no space left on device")

	_, err := createAnonymousIDWith(path, func(file *os.File, s string) (int, error) {
		// Simulate the ENOSPC shape: part of the ID lands on disk before
		// the write fails, leaving truncated junk behind.
		if _, writeErr := file.WriteString(s[:4]); writeErr != nil {
			t.Fatalf("seed partial content: %v", writeErr)
		}
		return 0, injected
	})
	if !errors.Is(err, injected) {
		t.Fatalf("expected the injected write failure, got %v", err)
	}

	// The final path must never have appeared (the partial content only ever
	// lived in the temp file), and the temp file must be cleaned up: an
	// orphan would make every future LoadOrCreateAnonymousID fail as corrupt
	// or leak junk into the directory.
	if _, statErr := os.Stat(path); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("expected no anonymous ID file after the failed write, stat returned %v", statErr)
	}
	entries, readErr := os.ReadDir(filepath.Dir(path))
	if readErr != nil {
		t.Fatalf("read anonymous ID directory: %v", readErr)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("expected the temp file to be removed after the failed write, found %v", names)
	}

	// And with nothing left behind, a later call recovers with a fresh ID.
	id, err := LoadOrCreateAnonymousID(path)
	if err != nil {
		t.Fatalf("LoadOrCreateAnonymousID after failed create: %v", err)
	}
	if !uuidv7.IsValid(id) {
		t.Fatalf("expected a valid UUIDv7 after recovery, got %q", id)
	}
}

func TestLoadOrCreateAnonymousIDConcurrentCreatorsConverge(t *testing.T) {
	// N processes-worth of concurrent first runs race on the same path. With
	// link-based publish the final path only ever appears fully written, so
	// every creator must converge on the single winner's ID with zero
	// corrupt-file errors (the O_EXCL-on-the-final-path scheme this replaced
	// let a reader observe an empty or partial file mid-create).
	const creators = 32
	path := filepath.Join(t.TempDir(), "shardpilot", "anonymous_id")

	ids := make([]string, creators)
	errs := make([]error, creators)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < creators; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ids[i], errs[i] = LoadOrCreateAnonymousID(path)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < creators; i++ {
		if errs[i] != nil {
			t.Fatalf("creator %d errored (no concurrent creator may ever see a partial file): %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("creators diverged: creator %d got %q, creator 0 got %q", i, ids[i], ids[0])
		}
	}
	if !uuidv7.IsValid(ids[0]) {
		t.Fatalf("expected a valid UUIDv7 winner, got %q", ids[0])
	}

	// The published file holds exactly the winner's ID and every losing
	// creator's temp file has been cleaned up.
	data, err := os.ReadFile(path)
	if err != nil || string(data) != ids[0]+"\n" {
		t.Fatalf("expected the published file to hold the winner's ID %q, got %q (err %v)", ids[0], data, err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatalf("read anonymous ID directory: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("expected only the published anonymous ID file to remain, found %v", names)
	}
}
