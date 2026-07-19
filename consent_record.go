package shardpilot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Persisted consent decision and owed-wipe marker for the opt-in disk spool
// (Config.SpoolDir). The record exists so the spool can prove, ACROSS
// restarts, that the actor's last explicit decision was a grant: the spool
// loads at start only from a persisted grant, and any other persisted state
// (absent, denied, unreadable) purges the record at init. The record is
// scoped to the actor/scope tuple the decision covered — a grant written for
// one configured actor never authorizes disk participation for another
// (logout/login, tenant switch, workspace switch over a reused SpoolDir).
// The record is written on every SetConsent when SpoolDir is set; it never
// feeds the LIVE consent state, which keeps its documented in-memory,
// open-by-default posture. RemoteConfigCachePath alone never enables consent
// persistence.

const (
	consentRecordFileName = "consent.json"
	spoolWipeOwedFileName = "spool-wipe-owed"
)

// consentRecordWire is the consent.json payload:
// {"consent_analytics":"granted"|"denied","actor_digest":"<sha256 hex>"}.
// An absent file means no decision. actor_digest scopes the decision to the
// actor tuple it covered (see consentActorDigest) — a digest, never the
// verbatim identifiers, so the record stays fixed-size and holds no
// plaintext identity material.
type consentRecordWire struct {
	ConsentAnalytics string `json:"consent_analytics"`
	ActorDigest      string `json:"actor_digest"`
}

// consentActorDigest canonically digests the actor/scope tuple a persisted
// consent decision covers: the same identity fields SetConsent's decision is
// about — the configured workspace/environment scope and the configured
// UserID/AnonymousID actor identity. Fields are length-prefixed before
// hashing, so distinct tuples can never collide by concatenation.
func consentActorDigest(cfg Config) string {
	h := sha256.New()
	for _, field := range []string{cfg.WorkspaceID, cfg.EnvironmentID, cfg.UserID, cfg.AnonymousID} {
		fmt.Fprintf(h, "%d:%s\n", len(field), field)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func consentRecordPath(dir string) string {
	return filepath.Join(dir, consentRecordFileName)
}

func spoolWipeOwedPath(dir string) string {
	return filepath.Join(dir, spoolWipeOwedFileName)
}

// loadConsentRecord reads the persisted consent decision for the given actor
// digest. ok is false when no usable decision exists FOR THAT ACTOR — the
// file is absent, unreadable, carries an unknown value, or was written for a
// different actor/scope tuple — which the spool treats exactly like an
// explicit denial (fail toward purging, never toward loading).
func loadConsentRecord(dir, actorDigest string) (ConsentState, bool) {
	data, err := os.ReadFile(consentRecordPath(dir))
	if err != nil {
		return ConsentUnknown, false
	}
	var record consentRecordWire
	if json.Unmarshal(data, &record) != nil {
		return ConsentUnknown, false
	}
	if record.ActorDigest != actorDigest {
		return ConsentUnknown, false
	}
	switch record.ConsentAnalytics {
	case "granted":
		return ConsentGranted, true
	case "denied":
		return ConsentDenied, true
	default:
		return ConsentUnknown, false
	}
}

// saveConsentRecord persists a consent decision, stamped with the actor
// digest it covers, with the SDK's private-file discipline (0700 dir —
// tightened when it pre-exists looser — 0600 file, full temp write + atomic
// rename). rename and chmod are injectable so tests can exercise persist and
// refused-tighten failures deterministically.
func saveConsentRecord(dir string, granted bool, actorDigest string, rename func(oldpath, newpath string) error, chmod func(name string, mode os.FileMode) error) error {
	decision := "denied"
	if granted {
		decision = "granted"
	}
	payload, err := json.Marshal(consentRecordWire{ConsentAnalytics: decision, ActorDigest: actorDigest})
	if err != nil {
		return err
	}
	return writePrivateFileAtomic(consentRecordPath(dir), payload, rename, chmod)
}

// createWipeOwedMarker records that a spool purge failed and a wipe is still
// owed, so the fail-closed state survives a restart. Presence is the flag;
// the file carries no content.
func createWipeOwedMarker(dir string) error {
	if err := ensurePrivateDir(dir, os.Chmod); err != nil {
		return err
	}
	file, err := os.OpenFile(spoolWipeOwedPath(dir), os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	return file.Close()
}

func removeWipeOwedMarker(dir string) error {
	err := os.Remove(spoolWipeOwedPath(dir))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func wipeOwedMarkerExists(dir string) bool {
	_, err := os.Stat(spoolWipeOwedPath(dir))
	return err == nil
}
