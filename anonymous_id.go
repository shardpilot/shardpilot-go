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
// 0600 permissions. The ID is written to a private temp file first and then
// published to the final path atomically without overwriting (a hard link),
// so the final path only ever appears fully written: when two processes race
// to create it, exactly one generated ID wins and both callers return it,
// and a concurrent reader can never observe an empty or partial file.
// Subsequent calls with the same path return the same ID. If the file exists
// but does not contain a valid UUIDv7, an error is returned and the file is
// left untouched.
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

// createAnonymousID generates a fresh ID, writes it fully to a private temp
// file, and publishes it to the final path atomically without overwriting.
// If another process won the publish race (os.Link fails with fs.ErrExist),
// the winner's file is read back and its ID returned instead, so concurrent
// first runs converge on a single identifier.
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
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", fmt.Errorf("create anonymous ID directory: %w", err)
		}
	}

	// Write the full ID to a private temp file (same directory, unique name,
	// 0600 from os.CreateTemp) before the final path exists at all, so a
	// concurrent reader can never observe the final path empty or partially
	// written and error out as corrupt instead of converging on the winner.
	temp, err := os.CreateTemp(dir, ".anonymous_id-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create anonymous ID temp file: %w", err)
	}
	tempPath := temp.Name()
	// The temp file is removed on every path out of this function: on
	// failure no orphan is left behind, and on success the published final
	// path keeps the data alive through its own directory entry.
	defer func() { _ = os.Remove(tempPath) }()

	if _, err := write(temp, id+"\n"); err != nil {
		_ = temp.Close()
		return "", fmt.Errorf("write anonymous ID file: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return "", fmt.Errorf("write anonymous ID file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return "", fmt.Errorf("write anonymous ID file: %w", err)
	}

	// Publish atomically WITHOUT overwriting: os.Link creates the final path
	// as a second name for the fully written, flushed, and closed temp file,
	// and fails with EEXIST when another process already published — unlike
	// os.Rename it can never replace a winner's file. Because the link is
	// the final path's first appearance, the file is complete from the very
	// first moment it is observable. Hard links are supported on every
	// platform this SDK is tested on (Linux and macOS CI); no Windows
	// fallback is provided.
	if err := os.Link(tempPath, path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			// Lost the publish race: converge on the winner's ID. With
			// link-based publish the winner's file is necessarily complete,
			// so a corrupt-content error here means the file was produced
			// outside this helper and is still surfaced, never overwritten.
			return readAnonymousID(path)
		}
		return "", fmt.Errorf("publish anonymous ID file: %w", err)
	}
	return id, nil
}
