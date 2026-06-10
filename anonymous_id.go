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
// 0600 permissions. Subsequent calls with the same path return the same ID.
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

	data, err := os.ReadFile(path)
	if err == nil {
		id := strings.TrimSpace(string(data))
		if !uuidv7.IsValid(id) {
			return "", fmt.Errorf("anonymous ID file %q does not contain a valid UUIDv7", path)
		}
		return id, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("read anonymous ID file: %w", err)
	}

	id, err := uuidv7.New()
	if err != nil {
		return "", fmt.Errorf("generate anonymous ID: %w", err)
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", fmt.Errorf("create anonymous ID directory: %w", err)
		}
	}
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("write anonymous ID file: %w", err)
	}
	return id, nil
}
