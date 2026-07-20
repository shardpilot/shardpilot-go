package shardpilot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
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

	// consentRecordReadLimit bounds how much of consent.json is ever read
	// back. The record is tiny and fixed-shape (two short fields, one a
	// 64-hex digest — well under 1 KiB); 8 KiB is generous by an order of
	// magnitude, and a larger file is not a record this SDK could have
	// written. The limit keeps NewClient from allocating unboundedly for a
	// stale/corrupt/planted file in an existing SpoolDir, mirroring the
	// bounded spool and remote-config cache reads; an over-limit file is
	// simply no usable decision (the corrupt-record path: fail toward
	// purging, never toward loading).
	consentRecordReadLimit = 8 << 10
)

// consentRecordWire is the consent.json payload:
// {"consent_analytics":"granted"|"denied"|"denied_forced_minor",
// "actor_digest":"<sha256 hex>","decided_at":"<RFC3339Nano>","floor":bool}.
// An absent file means no decision. actor_digest scopes the decision to the
// actor tuple it covered (see consentActorDigest) — a digest, never the
// verbatim identifiers, so the record stays fixed-size and holds no
// plaintext identity material. decided_at is the DECISION's stamp — the
// same instant the decision's receipt carries — so the floor reload can
// order the record against retained receipts (only a receipt STRICTLY
// newer than the record's decision may override it; a stale
// acked-but-unpruned receipt re-read from a failed outbox rewrite must
// never flip back a newer decision). floor marks FLOOR PROVENANCE: the
// record was authored under Config.ConsentFloor, where a grant is written
// only with its receipt trail durably down. A granted record WITHOUT the
// mark (the floor-off fire-and-forget era — its POST may have failed, no
// receipt exists) is unproven and never becomes live floor state. A
// floor-marked record additionally REQUIRES a parseable RFC3339Nano
// decided_at — the two fields are always written together, and without the
// stamp the receipt-ordering rule cannot run — so a floor-marked record
// with a missing or garbled stamp reads as ABSENT (fail-closed), never as
// an unorderable decision. The
// forced-minor value is written only through SetConsentDecision; an SDK
// build that predates it reads the value as "no usable decision", which
// fails toward purging — the safe direction, as with every unknown field
// shape.
type consentRecordWire struct {
	ConsentAnalytics string `json:"consent_analytics"`
	ActorDigest      string `json:"actor_digest"`
	DecidedAt        string `json:"decided_at,omitempty"`
	Floor            bool   `json:"floor,omitempty"`
}

// consentRecordInfo is a loaded record's full shape for the floor reload:
// the decision state plus the ordering stamp and floor provenance
// (decidedAt empty and floor false for a legacy record that predates the
// fields).
type consentRecordInfo struct {
	state     ConsentState
	decidedAt string
	floor     bool
}

// consentActorDigest canonically digests the actor/scope tuple a persisted
// consent decision covers: the same identity fields SetConsent's decision is
// about — the configured workspace/APP/environment scope and the configured
// UserID/AnonymousID actor identity. AppID is part of the tuple because the
// record can become LIVE floor state at reload: a SpoolDir reused across
// apps in one workspace/environment must not let another app's record rule
// this app's floor (its receipt was delivered for the other app's scope).
// A record written by an earlier build (digest without AppID) reads as "no
// usable decision" — fail-closed: the floor starts undecided and the spool
// purges once, exactly like any digest mismatch. Fields are length-prefixed
// before hashing, so distinct tuples can never collide by concatenation.
func consentActorDigest(cfg Config) string {
	h := sha256.New()
	for _, field := range []string{cfg.WorkspaceID, cfg.AppID, cfg.EnvironmentID, cfg.UserID, cfg.AnonymousID} {
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
// digest, through a hard size limit (consentRecordReadLimit) so an oversized
// file can never make client construction read it whole. ok is false when no
// usable decision exists FOR THAT ACTOR — the file is absent, unreadable,
// over the read limit, carries an unknown value, or was written for a
// different actor/scope tuple — which the spool treats exactly like an
// explicit denial (fail toward purging, never toward loading).
func loadConsentRecord(dir, actorDigest string) (ConsentState, bool) {
	info, ok := loadConsentRecordInfo(dir, actorDigest)
	return info.state, ok
}

// loadConsentRecordInfo is loadConsentRecord returning the record's full
// shape — the floor reload needs the ordering stamp and floor provenance
// alongside the state.
func loadConsentRecordInfo(dir, actorDigest string) (consentRecordInfo, bool) {
	none := consentRecordInfo{state: ConsentUnknown}
	file, err := os.Open(consentRecordPath(dir))
	if err != nil {
		return none, false
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, consentRecordReadLimit+1))
	if err != nil || len(data) > consentRecordReadLimit {
		return none, false
	}
	var record consentRecordWire
	if json.Unmarshal(data, &record) != nil {
		return none, false
	}
	if record.ActorDigest != actorDigest {
		return none, false
	}
	info := consentRecordInfo{decidedAt: record.DecidedAt, floor: record.Floor}
	switch record.ConsentAnalytics {
	case "granted":
		info.state = ConsentGranted
	case "denied":
		info.state = ConsentDenied
	case "denied_forced_minor":
		info.state = ConsentDeniedForcedMinor
	default:
		return none, false
	}
	if record.Floor {
		// A floor-authored record ALWAYS carries its decision's stamp (the
		// save writes both fields together), and the stamp is what orders
		// the record against retained receipts at reload. A floor-marked
		// record whose decided_at is empty or unparsable is corrupt — the
		// strictly-newer compare can never run against it — and the two
		// decision flavors fail in OPPOSITE directions:
		//   - a corrupt-stamped GRANT reads as ABSENT: kept, an unorderable
		//     grant would beat a durable newer deny receipt purely because
		//     the file was damaged (fail-closed = no grant);
		//   - a corrupt-stamped DENIAL is PRESERVED as denied with the
		//     garbled stamp cleared (denied-with-unknown-stamp): read as
		//     absent, a stale retained grant receipt would apply
		//     unconditionally and reopen the floor against a durable denial
		//     (fail-closed = keep the denial). The cleared stamp keeps the
		//     record un-overridable by comparison — floor-marked stampless
		//     is never superseded, unlike the legacy stampless shape — and
		//     the next decision or trail heal rewrites it whole.
		// Legacy records (no floor mark) keep loading with their empty
		// stamp: the provenance rule vets their grants, their denials are
		// honored, and a validly-stamped in-scope proof supersedes them in
		// both directions (they predate the stamping build).
		if _, err := time.Parse(time.RFC3339Nano, record.DecidedAt); err != nil {
			if info.state == ConsentGranted {
				return none, false
			}
			info.decidedAt = ""
		}
	}
	return info, true
}

// saveConsentRecord persists a consent decision, stamped with the actor
// digest it covers, the decision's own decided-at instant (for the floor
// reload's receipt-ordering rule), and floor provenance, with the SDK's
// private-file discipline (0700 dir — tightened when it pre-exists looser —
// 0600 file, full temp write + atomic rename). rename and chmod are
// injectable so tests can exercise persist and refused-tighten failures
// deterministically.
func saveConsentRecord(dir string, decision ConsentDecision, actorDigest, decidedAt string, floorAuthored bool, rename func(oldpath, newpath string) error, chmod func(name string, mode os.FileMode) error) error {
	payload, err := json.Marshal(consentRecordWire{
		ConsentAnalytics: string(decision),
		ActorDigest:      actorDigest,
		DecidedAt:        decidedAt,
		Floor:            floorAuthored,
	})
	if err != nil {
		return err
	}
	return writePrivateFileAtomic(consentRecordPath(dir), payload, rename, chmod)
}

// createWipeOwedMarker records that a spool purge failed and a wipe is still
// owed, so the fail-closed state survives a restart. Presence is the flag;
// the file carries no content. The directory is synced after the create for
// the same reason writePrivateFileAtomic syncs it after a rename: the marker
// IS the persisted fail-closed state, and a crash that forgets the entry
// would reopen the spool with the condemned record still owed.
func createWipeOwedMarker(dir string) error {
	if err := ensurePrivateDir(dir, os.Chmod); err != nil {
		return err
	}
	file, err := os.OpenFile(spoolWipeOwedPath(dir), os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return syncDir(dir)
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
