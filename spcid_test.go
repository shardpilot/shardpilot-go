package shardpilot

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
)

func TestExperimentSubjectKeyGrammar(t *testing.T) {
	valid := []string{
		"spcid_" + strings.Repeat("a", 20),
		"spcid_" + strings.Repeat("a", 64),
		"spcid_0192f3a1-7b1c-7def-8123-4567890abcde", // spcid_ + UUIDv7 shape
		"spcid_AZaz09_-AZaz09_-AZaz09_-",
	}
	for _, key := range valid {
		if !validExperimentSubjectKey(key) {
			t.Fatalf("expected %q to be a valid experiment subject key", key)
		}
	}
	invalid := []string{
		"",
		"spcid_",
		"spcid_" + strings.Repeat("a", 19), // body below 20
		"spcid_" + strings.Repeat("a", 65), // body above 64
		"SPCID_" + strings.Repeat("a", 24), // prefix is case-sensitive
		"spcid-" + strings.Repeat("a", 24), // wrong separator
		"spcid_" + strings.Repeat("a", 10) + "!" + strings.Repeat("a", 10), // out-of-set char
		"spcid_ " + strings.Repeat("a", 24),                                // whitespace
		"0192f3a1-7b1c-7def-8123-4567890abcde",                             // a bare UUID (an anonymous ID) is NOT a subject key
		" spcid_" + strings.Repeat("a", 24),
	}
	for _, key := range invalid {
		if validExperimentSubjectKey(key) {
			t.Fatalf("expected %q to be rejected as an experiment subject key", key)
		}
	}
}

func TestLoadOrCreateExperimentSubjectKeyRoundTripAndPerms(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shardpilot", "experiment_subject_key")

	created, err := LoadOrCreateExperimentSubjectKey(path)
	if err != nil {
		t.Fatalf("LoadOrCreateExperimentSubjectKey create: %v", err)
	}
	if !validExperimentSubjectKey(created) {
		t.Fatalf("expected a grammar-valid spcid subject key, got %q", created)
	}
	// House construction: spcid_ + a UUIDv7 — dedicated, never a bare UUID.
	if !strings.HasPrefix(created, "spcid_") || !uuidv7.IsValid(strings.TrimPrefix(created, "spcid_")) {
		t.Fatalf("expected spcid_ + UUIDv7 construction, got %q", created)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat experiment subject key file: %v", err)
	}
	if runtime.GOOS != "windows" {
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Fatalf("expected 0600 file permissions, got %o", perm)
		}
	}

	loaded, err := LoadOrCreateExperimentSubjectKey(path)
	if err != nil {
		t.Fatalf("LoadOrCreateExperimentSubjectKey load: %v", err)
	}
	if loaded != created {
		t.Fatalf("expected a stable subject key round-trip, got %q then %q", created, loaded)
	}
}

func TestLoadOrCreateExperimentSubjectKeyRejectsBadInput(t *testing.T) {
	if _, err := LoadOrCreateExperimentSubjectKey(" "); err == nil {
		t.Fatal("expected empty path to be rejected")
	}

	// A file holding an anonymous ID (a bare UUID) is NOT a subject key:
	// pointing the helper at the anonymous-ID file must error, never
	// conflate — or overwrite — the two identifiers.
	anonPath := filepath.Join(t.TempDir(), "anonymous_id")
	anonID, err := LoadOrCreateAnonymousID(anonPath)
	if err != nil {
		t.Fatalf("seed anonymous ID: %v", err)
	}
	if _, err := LoadOrCreateExperimentSubjectKey(anonPath); err == nil {
		t.Fatal("expected the anonymous-ID file to be rejected as a subject key")
	}
	data, err := os.ReadFile(anonPath)
	if err != nil || string(data) != anonID+"\n" {
		t.Fatalf("expected the anonymous-ID file to be left untouched, got %q (err %v)", data, err)
	}

	corrupted := filepath.Join(t.TempDir(), "experiment_subject_key")
	if err := os.WriteFile(corrupted, []byte("spcid_short\n"), 0o600); err != nil {
		t.Fatalf("seed corrupted file: %v", err)
	}
	if _, err := LoadOrCreateExperimentSubjectKey(corrupted); err == nil {
		t.Fatal("expected a corrupted subject key file to be rejected")
	}
	data, err = os.ReadFile(corrupted)
	if err != nil || string(data) != "spcid_short\n" {
		t.Fatalf("expected the corrupted file to be left untouched, got %q (err %v)", data, err)
	}
}

func TestCreateExperimentSubjectKeyLostRaceReturnsWinner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "experiment_subject_key")
	winner := "spcid_" + strings.Repeat("b", 24)
	if err := os.WriteFile(path, []byte(winner+"\n"), 0o600); err != nil {
		t.Fatalf("seed winner file: %v", err)
	}

	got, err := createExperimentSubjectKey(path)
	if err != nil {
		t.Fatalf("createExperimentSubjectKey after lost race: %v", err)
	}
	if got != winner {
		t.Fatalf("expected the race winner's key %q, got %q", winner, got)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != winner+"\n" {
		t.Fatalf("expected the winner's file to be left untouched, got %q (err %v)", data, err)
	}
}

func TestCreateExperimentSubjectKeyRemovesPartialFileOnWriteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "experiment_subject_key")
	injected := errors.New("injected write failure: no space left on device")

	_, err := createExperimentSubjectKeyWith(path, func(file *os.File, s string) (int, error) {
		if _, writeErr := file.WriteString(s[:4]); writeErr != nil {
			t.Fatalf("seed partial content: %v", writeErr)
		}
		return 0, injected
	})
	if !errors.Is(err, injected) {
		t.Fatalf("expected the injected write failure, got %v", err)
	}

	if _, statErr := os.Stat(path); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("expected no subject key file after the failed write, stat returned %v", statErr)
	}
	entries, readErr := os.ReadDir(filepath.Dir(path))
	if readErr != nil {
		t.Fatalf("read subject key directory: %v", readErr)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("expected the temp file to be removed after the failed write, found %v", names)
	}

	key, err := LoadOrCreateExperimentSubjectKey(path)
	if err != nil {
		t.Fatalf("LoadOrCreateExperimentSubjectKey after failed create: %v", err)
	}
	if !validExperimentSubjectKey(key) {
		t.Fatalf("expected a valid subject key after recovery, got %q", key)
	}
}

func TestLoadOrCreateExperimentSubjectKeyConcurrentCreatorsConverge(t *testing.T) {
	const creators = 32
	path := filepath.Join(t.TempDir(), "shardpilot", "experiment_subject_key")

	keys := make([]string, creators)
	errs := make([]error, creators)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < creators; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			keys[i], errs[i] = LoadOrCreateExperimentSubjectKey(path)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < creators; i++ {
		if errs[i] != nil {
			t.Fatalf("creator %d errored (no concurrent creator may ever see a partial file): %v", i, errs[i])
		}
		if keys[i] != keys[0] {
			t.Fatalf("creators diverged: creator %d got %q, creator 0 got %q", i, keys[i], keys[0])
		}
	}
	if !validExperimentSubjectKey(keys[0]) {
		t.Fatalf("expected a valid winner, got %q", keys[0])
	}

	data, err := os.ReadFile(path)
	if err != nil || string(data) != keys[0]+"\n" {
		t.Fatalf("expected the published file to hold the winner's key %q, got %q (err %v)", keys[0], data, err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatalf("read subject key directory: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("expected only the published subject key file to remain, found %v", names)
	}
}
