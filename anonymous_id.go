package shardpilot

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
)

// LoadOrCreateAnonymousID loads a persisted anonymous identifier from the
// given file path, or generates a fresh UUIDv7 and persists it there.
//
// On first use it creates the parent directory (0700) and writes the ID with
// 0600 permissions. The file is created atomically (O_CREATE|O_EXCL): when
// two processes race to create it, exactly one generated ID wins and both
// callers return it. Subsequent calls with the same path return the same ID.
// If the file exists but does not contain a valid UUIDv7, an error is
// returned and the file is left untouched.
//
// This helper is strictly opt-in: the SDK never calls it implicitly and
// never writes files on its own, so server integrations that do not want
// on-disk state simply never call it. The returned ID is typically wired
// into Config.AnonymousID.
func LoadOrCreateAnonymousID(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("%w: anonymous ID path is required", ErrInvalidConfig)
	}

	id, err := readAnonymousID(path)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	return createAnonymousID(path)
}

// readAnonymousID reads and validates a persisted anonymous ID. The returned
// error preserves fs.ErrNotExist for errors.Is checks.
func readAnonymousID(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read anonymous ID file: %w", err)
	}
	id := strings.TrimSpace(string(data))
	if !uuidv7.IsValid(id) {
		return "", fmt.Errorf("anonymous ID file %q does not contain a valid UUIDv7", path)
	}
	return id, nil
}

// createAnonymousID generates a fresh ID and persists it with an exclusive
// create. If another process won the creation race (O_EXCL fails with
// fs.ErrExist), the winner's file is read back and its ID returned instead,
// so concurrent first runs converge on a single identifier.
func createAnonymousID(path string) (string, error) {
	return createAnonymousIDWith(path, (*os.File).WriteString)
}

// createAnonymousIDWith is createAnonymousID with an injectable write step,
// so write failures (ENOSPC and friends) can be exercised deterministically
// in tests.
func createAnonymousIDWith(path string, write func(*os.File, string) (int, error)) (string, error) {
	id, err := uuidv7.New()
	if err != nil {
		return "", fmt.Errorf("generate anonymous ID: %w", err)
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", fmt.Errorf("create anonymous ID directory: %w", err)
		}
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return readAnonymousID(path)
		}
		return "", fmt.Errorf("create anonymous ID file: %w", err)
	}
	if _, err := write(file, id+"\n"); err != nil {
		_ = file.Close()
		// Best-effort cleanup: a partially written file would make every
		// future LoadOrCreateAnonymousID fail as corrupt, so remove what
		// this call just created (O_EXCL guarantees ownership) instead of
		// leaving the orphan behind.
		_ = os.Remove(path)
		return "", fmt.Errorf("write anonymous ID file: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("write anonymous ID file: %w", err)
	}
	return id, nil
}
