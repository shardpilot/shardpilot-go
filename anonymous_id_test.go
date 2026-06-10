package shardpilot

import (
	"os"
	"path/filepath"
	"runtime"
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
