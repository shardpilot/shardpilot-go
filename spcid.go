package shardpilot

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
)

// experimentSubjectKeyGrammar is the server-enforced grammar for a client_id
// experiment subject key: the spcid_ prefix and a 20-64 character body of
// URL-safe characters. A subject key outside it is a permanent 400 on every
// assignment fetch.
const experimentSubjectKeyGrammar = `^spcid_[A-Za-z0-9_-]{20,64}$`

var experimentSubjectKeyPattern = regexp.MustCompile(experimentSubjectKeyGrammar)

// validExperimentSubjectKey reports whether key conforms to the spcid
// grammar the assignment endpoint enforces.
func validExperimentSubjectKey(key string) bool {
	return experimentSubjectKeyPattern.MatchString(key)
}

// newExperimentSubjectKey mints a fresh spcid subject key: "spcid_" + a
// UUIDv7, matching this SDK's anonymous-ID construction idiom. The 36-char
// UUID body (hex + hyphens) sits inside the grammar's 20-64 URL-safe-char
// bound. The value is a dedicated installation identifier — high-entropy,
// time-ordered, and never derived from the anonymous ID.
func newExperimentSubjectKey() (string, error) {
	id, err := uuidv7.New()
	if err != nil {
		return "", fmt.Errorf("generate experiment subject key: %w", err)
	}
	return "spcid_" + id, nil
}

// LoadOrCreateExperimentSubjectKey loads a persisted experiment subject key
// (an `spcid_...` installation identifier) from the given file path, or
// mints a fresh one and persists it there. The returned value is typically
// wired into Config.ExperimentSubjectKey — it is the `subject_key` every
// assignment fetch sends.
//
// It follows LoadOrCreateAnonymousID's publish discipline exactly: the key is
// written fully to a private temp file (parent directories created 0700,
// file 0600) and published to the final path atomically WITHOUT overwriting
// (a hard link), so when two processes race to create it exactly one minted
// key wins and both callers return it, and a reader can never observe a
// partial file. Subsequent calls with the same path return the same key. A
// file that exists but does not hold a grammar-valid spcid key is an error
// and is left untouched — in particular, pointing this helper at the
// anonymous-ID file fails here rather than conflating the two identifiers:
// the subject key is a DEDICATED id, stored beside (never replacing) the
// anonymous ID.
//
// The helper is strictly opt-in: the SDK never calls it implicitly and never
// writes files on its own.
func LoadOrCreateExperimentSubjectKey(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("%w: experiment subject key path is required", ErrInvalidConfig)
	}

	key, err := readExperimentSubjectKey(path)
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	return createExperimentSubjectKey(path)
}

// readExperimentSubjectKey reads and validates a persisted subject key. The
// returned error preserves fs.ErrNotExist for errors.Is checks.
func readExperimentSubjectKey(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read experiment subject key file: %w", err)
	}
	key := strings.TrimSpace(string(data))
	if !validExperimentSubjectKey(key) {
		return "", fmt.Errorf("experiment subject key file %q does not contain a valid spcid subject key", path)
	}
	return key, nil
}

func createExperimentSubjectKey(path string) (string, error) {
	return createExperimentSubjectKeyWith(path, (*os.File).WriteString)
}

// createExperimentSubjectKeyWith is createExperimentSubjectKey with an
// injectable write step, so write failures (ENOSPC and friends) can be
// exercised deterministically in tests — the same seam shape as
// createAnonymousIDWith.
func createExperimentSubjectKeyWith(path string, write func(*os.File, string) (int, error)) (string, error) {
	key, err := newExperimentSubjectKey()
	if err != nil {
		return "", err
	}
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", fmt.Errorf("create experiment subject key directory: %w", err)
		}
	}

	// Full write to a private temp file before the final path exists at all,
	// so a concurrent reader can never observe the final path empty or
	// partially written (see createAnonymousIDWith for the full rationale).
	temp, err := os.CreateTemp(dir, ".experiment_subject_key-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create experiment subject key temp file: %w", err)
	}
	tempPath := temp.Name()
	defer func() { _ = os.Remove(tempPath) }()

	if _, err := write(temp, key+"\n"); err != nil {
		_ = temp.Close()
		return "", fmt.Errorf("write experiment subject key file: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return "", fmt.Errorf("write experiment subject key file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return "", fmt.Errorf("write experiment subject key file: %w", err)
	}

	// Publish atomically WITHOUT overwriting: os.Link fails with EEXIST when
	// another process already published, so exactly one minted key ever
	// exists and a winner's file is never replaced.
	if err := os.Link(tempPath, path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			// Lost the publish race: converge on the winner's key. A corrupt
			// content error here means the file was produced outside this
			// helper and is still surfaced, never overwritten.
			return readExperimentSubjectKey(path)
		}
		return "", fmt.Errorf("publish experiment subject key file: %w", err)
	}
	return key, nil
}
