package shardpilot

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Persisted consent decision and owed-wipe marker for the opt-in disk spool
// (Config.SpoolDir). The record exists so the spool can prove, ACROSS
// restarts, that the actor's last explicit decision was a grant: the spool
// loads at start only from a persisted grant, and any other persisted state
// (absent, denied, unreadable) purges the record at init. The record is
// written on every SetConsent when SpoolDir is set; it never feeds the LIVE
// consent state, which keeps its documented in-memory, open-by-default
// posture. RemoteConfigCachePath alone never enables consent persistence.

const (
	consentRecordFileName = "consent.json"
	spoolWipeOwedFileName = "spool-wipe-owed"
)

// consentRecordWire is the consent.json payload:
// {"consent_analytics":"granted"|"denied"}. An absent file means no decision.
type consentRecordWire struct {
	ConsentAnalytics string `json:"consent_analytics"`
}

func consentRecordPath(dir string) string {
	return filepath.Join(dir, consentRecordFileName)
}

func spoolWipeOwedPath(dir string) string {
	return filepath.Join(dir, spoolWipeOwedFileName)
}

// loadConsentRecord reads the persisted consent decision. ok is false when
// no usable decision exists — the file is absent, unreadable, or carries an
// unknown value — which the spool treats exactly like an explicit denial
// (fail toward purging, never toward loading).
func loadConsentRecord(dir string) (ConsentState, bool) {
	data, err := os.ReadFile(consentRecordPath(dir))
	if err != nil {
		return ConsentUnknown, false
	}
	var record consentRecordWire
	if json.Unmarshal(data, &record) != nil {
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

// saveConsentRecord persists a consent decision with the SDK's private-file
// discipline (0700 dir, 0600 file, full temp write + atomic rename). rename
// is injectable so tests can exercise persist failures deterministically.
func saveConsentRecord(dir string, granted bool, rename func(oldpath, newpath string) error) error {
	decision := "denied"
	if granted {
		decision = "granted"
	}
	payload, err := json.Marshal(consentRecordWire{ConsentAnalytics: decision})
	if err != nil {
		return err
	}
	return writePrivateFileAtomic(consentRecordPath(dir), payload, rename)
}

// createWipeOwedMarker records that a spool purge failed and a wipe is still
// owed, so the fail-closed state survives a restart. Presence is the flag;
// the file carries no content.
func createWipeOwedMarker(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
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
