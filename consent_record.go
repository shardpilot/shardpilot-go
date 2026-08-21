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

// consentAnalyticsUnwitnessed is the decision value written for a WITHHELD
// grant. No released build recognises it, so every decoder — including this
// one's predecessors — refuses to treat the record as authorization.
const consentAnalyticsUnwitnessed = "unwitnessed"

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
	// ConsentAnalytics carries the decision. A grant that has been WITHHELD
	// as unprovable is written as consentAnalyticsUnwitnessed instead, and
	// its real state moves to WithheldAnalytics — see the note there.
	ConsentAnalytics string `json:"consent_analytics"`
	// WithheldAnalytics holds the decision a withheld record would carry if
	// it were usable. It exists because an ADDITIVE flag cannot fail closed
	// against an OLDER decoder: a rollback to any previously released build
	// ignores an unknown key and reads `consent_analytics:"granted"` as
	// ordinary authorization, resurrecting exactly the grant the mark
	// withheld — and by then maintenance may have rewritten the outbox
	// clean, so nothing else objects either.
	//
	// So the fail-closed part is encoded in a field every decoder ALREADY
	// consults: `consent_analytics` becomes a value no build recognises.
	// An older decoder hits its `default:` arm and reports no usable record;
	// this build recognises the sentinel and recovers the real state from
	// here. Fail-closed by construction rather than by the reader's version.
	//
	// This is affordable because there is no installed base to migrate: a
	// shape that is RIGHT once one exists beats a shape that is cheap to
	// migrate from today.
	WithheldAnalytics string `json:"withheld_analytics,omitempty"`
	ActorDigest       string `json:"actor_digest"`
	DecidedAt         string `json:"decided_at,omitempty"`
	Floor             bool   `json:"floor,omitempty"`
	// Unwitnessed marks a GRANT whose receipt trail could not be proven
	// intact when it was last read. It lives HERE, on the per-scope record,
	// rather than on the shared outbox, and the placement is the whole
	// point: the outbox file is one per SpoolDir and is shared across every
	// scope using that directory, so a mark kept there is cleared by a
	// fresh decision belonging to a DIFFERENT workspace, app or actor —
	// which supersedes nothing about this scope's unknown trail. The record
	// is keyed by ActorDigest, so a mark on it is per-scope by
	// construction, and a fresh decision for this scope rewrites the record
	// whole and clears it for free.
	Unwitnessed bool `json:"unwitnessed,omitempty"`
	// UnwitnessedAt orders the mark against decisions and receipts. It is
	// the newest DecidedAt observed in the trail when the mark was applied,
	// so anything STRICTLY NEWER is provably a decision the mark could not
	// have been about, and may supersede it.
	//
	// Without an order the mark is a boolean, and a boolean can only be a
	// veto over everything or over nothing. Both are wrong: vetoing
	// everything loses a decision that was already durable (record a fresh
	// grant, its receipt lands, the process crashes before the record write
	// — the next start sees the old marked record beside a clean, strictly
	// newer receipt and refuses it forever), while vetoing nothing reopens
	// the defect the mark exists to close. Eight review rounds widened
	// "which paths must consult the boolean"; the ninth showed the guard
	// was too STRONG, which is the signal that the shape was wrong rather
	// than the coverage.
	//
	// ABSENT MEANS INFINITELY NEW, and the direction is load-bearing.
	// Records written before this field existed carry no stamp, and reading
	// that as infinitely OLD would let every retained receipt supersede the
	// mark — reopening the P0 through the exact mechanism it closed, on
	// every upgraded client at once. Infinitely NEW keeps those records
	// behaving as they do today: they block, and only a fresh decision
	// clears them. Absence does not resolve to the permissive answer; that
	// is the same rule the readers above follow.
	//
	// FORMAT: additive at the SAME record version, deliberately. A version
	// bump would make every existing record unreadable, and unreadable now
	// means UNUSABLE, which would withhold every persisted grant in the
	// fleet on upgrade — a privacy fix turned into a fleet refusal, the
	// failure mode this whole change is built to avoid. An older build
	// reading a newer record ignores the unknown key and sees the boolean
	// alone, which is the pre-stamp behaviour: strictly more blocking, and
	// so safe in the direction that matters.
	//
	// This is local SDK state on the device's own disk. It is not part of
	// any wire contract, is never transmitted, and no server reads it.
	//
	// CLOCKS GO BACKWARD, and this compares stamps across restarts using a
	// device clock. A backward jump yields a decision that is genuinely
	// newer than the mark but does not compare newer, so the mark STICKS
	// and the grant stays withheld until the host records another decision.
	// That is the safe direction and it is chosen, not overlooked: the
	// alternative — trusting a clock that just moved backward — is how a
	// stale grant would supersede a live mark.
	UnwitnessedAt string `json:"unwitnessed_at,omitempty"`
}

// consentRecordInfo is a loaded record's full shape for the floor reload:
// the decision state plus the ordering stamp and floor provenance
// (decidedAt empty and floor false for a legacy record that predates the
// fields).
type consentRecordInfo struct {
	state         ConsentState
	decidedAt     string
	floor         bool
	unwitnessed   bool
	unwitnessedAt string
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
	if ok && info.state == ConsentGranted && info.unwitnessed {
		// A grant a floor-enabled run marked UNPROVABLE is not authorization
		// for anyone, and least of all for a later run started with
		// ConsentFloor unset. This is the ordinary loader — initSpool's
		// floor-off path uses it to decide whether a persisted grant may
		// load and resend spool.json — so returning the mark as a usable
		// grant would let disabling the live floor turn an explicit
		// "unprovable" back into permission. Floor-off already honors
		// persisted DENIALS and already requires a persisted grant for disk
		// loads; refusing a marked grant keeps both properties.
		//
		// The full-info loader deliberately still returns it, because the
		// floor needs to SEE the mark to withhold and to recover from it.
		return ConsentUnknown, false
	}
	return info.state, ok
}

// consentRecordRead classifies what a read of the decision record LEARNED,
// by the same rule the outbox reader follows and for the same reason: a
// failure that is indistinguishable from a determinate answer is how an
// emergency stop silently stops stopping. "No record" and "a record I could
// not read" lead to OPPOSITE safe actions, so they may not share a value.
type consentRecordRead uint8

const (
	// consentRecordReadParsed: read and understood.
	consentRecordReadParsed consentRecordRead = iota
	// consentRecordReadAbsent: no record file, or one belonging to a
	// DIFFERENT actor digest. Both are honestly "nothing recorded for this
	// scope" — a fresh install and a shared SpoolDir respectively.
	consentRecordReadAbsent
	// consentRecordReadUnusable: a record exists for this scope and could
	// not be understood. The decision it held is unknown, and it may have
	// been a denial.
	consentRecordReadUnusable
	// consentRecordReadForeign: the file exists and is well-formed but
	// carries a DIFFERENT actor digest. Nothing is recorded for THIS scope
	// — and, crucially, that other scope's write DESTROYED whatever this
	// scope had, because the record is one file per SpoolDir overwritten
	// whole. So a foreign record is its own tombstone: it cannot be ruled
	// out that it replaced an unwitnessed mark.
	consentRecordReadForeign
)

// loadConsentRecordInfo is loadConsentRecord returning the record's full
// shape — the floor reload needs the ordering stamp and floor provenance
// alongside the state. It preserves the two-value signature every existing
// caller uses; callers that must act on the DIFFERENCE between absent and
// unreadable take loadConsentRecordRead instead.
func loadConsentRecordInfo(dir, actorDigest string) (consentRecordInfo, bool) {
	info, ok, _ := loadConsentRecordRead(dir, actorDigest)
	return info, ok
}

// loadConsentRecordRead is loadConsentRecordInfo plus what the read learned.
// The third value exists because `!ok` is NOT "there is no record": it is
// also every opaque failure, and initConsentFloor's trail-tail override
// treats a false ok as "no record at all" and applies a receipt
// UNCONDITIONALLY — including a stale grant receipt over a denial it cannot
// read, which it then HEALS onto disk, destroying the denial rather than
// merely ignoring it.
func loadConsentRecordRead(dir, actorDigest string) (consentRecordInfo, bool, consentRecordRead) {
	none := consentRecordInfo{state: ConsentUnknown}
	file, err := os.Open(consentRecordPath(dir))
	if err != nil {
		// Same distinction as the outbox door: a dangling symlink makes
		// Open return ENOENT while the directory entry exists, and that
		// entry is a record whose target we cannot read. Lstat does not
		// follow the link.
		if errors.Is(err, fs.ErrNotExist) && !pathExists(consentRecordPath(dir)) {
			return none, false, consentRecordReadAbsent
		}
		return none, false, consentRecordReadUnusable
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, consentRecordReadLimit+1))
	if err != nil || len(data) > consentRecordReadLimit {
		return none, false, consentRecordReadUnusable
	}
	var record consentRecordWire
	if json.Unmarshal(data, &record) != nil {
		return none, false, consentRecordReadUnusable
	}
	// The mark is captured BEFORE anything else can discard it. Every later
	// failure path returns `none`, and a `none` that dropped the mark turns
	// damage to an unrelated field into permission: the grant-tail block
	// stops seeing record.unwitnessed, `!recordOK` lets a retained receipt
	// apply unconditionally, and the heal rewrites an unmarked granted
	// record. Corruption anywhere in this file must not become a way to
	// launder a deliberate withholding.
	none.unwitnessed = record.Unwitnessed
	none.unwitnessedAt = record.UnwitnessedAt

	// SHAPE before identity. `null`, `{}`, and a record with a missing
	// actor_digest or decision all unmarshal cleanly, and comparing the
	// digest first classified them as ANOTHER scope's record — honestly
	// absent — when they are in fact this file damaged. Absent lets the
	// trail-tail override apply a retained grant UNCONDITIONALLY and heal
	// over the file, so a damaged DENIAL for this very scope would be
	// replaced by a grant.
	if record.ActorDigest == "" || record.ConsentAnalytics == "" {
		return none, false, consentRecordReadUnusable
	}
	if record.ActorDigest != actorDigest {
		// Carry the foreign record's OWN stamp out with it. It is the only
		// thing that dates the scope switch, and a caller that must decide
		// whether a receipt predates or postdates that switch has nothing
		// else to compare against. Discarding it makes every foreign record
		// an unbounded veto.
		none.decidedAt = record.DecidedAt
		// A well-formed record for a DIFFERENT actor records nothing about
		// THIS scope — but it is not the same as no file at all, and an
		// earlier revision of this code collapsed the two and said so in a
		// comment that was wrong. consentRecordPath takes only the
		// directory: it is ONE file per SpoolDir, stamped with a digest
		// rather than keyed by one, and saveConsentRecord overwrites it
		// whole. So a sibling scope's decision — a login that sets a
		// UserID, a tenant switch, a second AppID — erases this scope's
		// unwitnessed mark, and the same sibling's merging outbox save
		// sanitizes the unreadable trail into a clean record. Both pieces
		// of evidence vanish and the grant a previous start refused is
		// promoted AND healed to disk. FOREIGN keeps the two apart.
		return none, false, consentRecordReadForeign
	}
	info := consentRecordInfo{decidedAt: record.DecidedAt, floor: record.Floor, unwitnessed: record.Unwitnessed, unwitnessedAt: record.UnwitnessedAt}
	if record.ConsentAnalytics == consentAnalyticsUnwitnessed {
		// A withheld record. The real decision lives in WithheldAnalytics;
		// the mark and its stamp ride as usual, so recovery works exactly as
		// it does for a record marked the additive way.
		record.ConsentAnalytics = record.WithheldAnalytics
		record.Unwitnessed = true
		none.unwitnessed = true
	}
	switch record.ConsentAnalytics {
	case "granted":
		info.state = ConsentGranted
	case "denied":
		info.state = ConsentDenied
	case "denied_forced_minor":
		info.state = ConsentDeniedForcedMinor
	default:
		// An unknown decision value is OPAQUE, not absent: a newer build's
		// record read by an older one lands here, and what it decided is
		// exactly what we cannot tell.
		return none, false, consentRecordReadUnusable
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
				// `none` already carries the mark (captured above), so a
				// damaged stamp cannot strip a deliberate withholding off a
				// grant on its way out.
				// ABSENT, deliberately, and NOT unusable: this failure is
				// not opaque. The decision was read — it said granted — and
				// only its STAMP is unorderable, so discarding it hides no
				// denial, and the readable trail may decide. Reporting it
				// unusable here would block a legitimate proof-driven
				// recovery (an unorderable grant record beside a valid
				// grant receipt) for no safety gain. The opaque classes
				// above are the ones where what the record SAID is unknown.
				return none, false, consentRecordReadAbsent
			}
			info.decidedAt = ""
		}
	}
	return info, true, consentRecordReadParsed
}

// saveConsentRecord persists a consent decision, stamped with the actor
// digest it covers, the decision's own decided-at instant (for the floor
// reload's receipt-ordering rule), and floor provenance, with the SDK's
// private-file discipline (0700 dir — tightened when it pre-exists looser —
// 0600 file, full temp write + atomic rename). rename and chmod are
// injectable so tests can exercise persist and refused-tighten failures
// deterministically.
// markConsentRecordUnwitnessed re-writes an existing record with the
// unwitnessed mark set, preserving its decision, stamp and provenance. It is
// how the withholding of an unprovable GRANT becomes DURABLE: without it the
// refusal lives only in this process, and the next start — reading the same
// record with a trail that has since been pruned clean — promotes exactly
// the grant this start refused. saveConsentRecord writes the mark as false,
// so any FRESH decision for this scope clears it for free, which is the
// documented way back.
// consentMarkSupersededBy reports whether a decision stamped decidedAt is
// provably NEWER than the mark on info, and so may clear it.
//
// The absent-stamp case is the one that matters: a record written before the
// stamp existed reads as INFINITELY NEW, so nothing supersedes it and it
// behaves exactly as it does today. Reading absence the other way would let
// any retained receipt clear the mark and reopen the defect fleet-wide on
// upgrade.
// consentDecisionSupersedes reports whether a decision stamped `at` is
// provably newer than one stamped `than`. An absent or unparsable `than`
// reads as INFINITELY NEW — nothing supersedes it — the same direction the
// mark uses, and for the same reason: absence must not resolve permissively.
func consentDecisionSupersedes(at string, than string) bool {
	if than == "" || at == "" {
		return false
	}
	base, err := time.Parse(time.RFC3339Nano, than)
	if err != nil {
		return false
	}
	when, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return false
	}
	return when.After(base)
}

func consentMarkSupersededBy(info consentRecordInfo, decidedAt string) bool {
	if !info.unwitnessed {
		return true
	}
	if info.unwitnessedAt == "" || decidedAt == "" {
		return false
	}
	markAt, err := time.Parse(time.RFC3339Nano, info.unwitnessedAt)
	if err != nil {
		// An unparsable stamp is not an old one. Same rule again.
		return false
	}
	at, err := time.Parse(time.RFC3339Nano, decidedAt)
	if err != nil {
		return false
	}
	return at.After(markAt)
}

func markConsentRecordUnwitnessed(dir string, info consentRecordInfo, actorDigest string, unwitnessedAt string, rename func(oldpath, newpath string) error, chmod func(name string, mode os.FileMode) error) error {
	var decision ConsentDecision
	switch info.state {
	case ConsentGranted:
		decision = ConsentDecisionGranted
	case ConsentDenied:
		decision = ConsentDecisionDenied
	case ConsentDeniedForcedMinor:
		decision = ConsentDecisionDeniedForcedMinor
	default:
		return nil
	}
	wire := consentRecordWire{
		ConsentAnalytics: string(decision),
		ActorDigest:      actorDigest,
		DecidedAt:        info.decidedAt,
		Floor:            info.floor,
		Unwitnessed:      true,
		UnwitnessedAt:    unwitnessedAt,
	}
	if decision == ConsentDecisionGranted {
		// Only a GRANT needs the older-decoder guard: a withheld DENIAL is
		// already the restrictive answer, and rewriting it into a sentinel
		// would make an old build read no record where it would otherwise
		// have honoured the denial.
		wire.WithheldAnalytics = string(decision)
		wire.ConsentAnalytics = consentAnalyticsUnwitnessed
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return err
	}
	return writePrivateFileAtomic(consentRecordPath(dir), payload, rename, chmod)
}

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
