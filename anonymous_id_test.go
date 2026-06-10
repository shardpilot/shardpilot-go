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
