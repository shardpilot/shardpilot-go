package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
)

// ExperimentSubjectConsent is one player's consent for the experiment
// plane, as the host knows it. Only Granted admits that player's fetches,
// applications and facts.
type ExperimentSubjectConsent int

const (
	// ExperimentSubjectConsentUnknown is the zero value: the player has not
	// decided. It refuses like Denied.
	ExperimentSubjectConsentUnknown ExperimentSubjectConsent = iota
	ExperimentSubjectConsentGranted
	ExperimentSubjectConsentDenied
)

func (consent ExperimentSubjectConsent) valid() bool {
	switch consent {
	case ExperimentSubjectConsentUnknown, ExperimentSubjectConsentGranted, ExperimentSubjectConsentDenied:
		return true
	}
	return false
}

// ExperimentSubjectConfig names one player for ExperimentSubject.
type ExperimentSubjectConfig struct {
	// SubjectKey is the player's experiment installation id: the spcid_ id
	// the player's client SDK minted (^spcid_[A-Za-z0-9_-]{20,64}$). It is
	// sent as subject_key on this handle's fetches and applications and
	// never in an analytics event.
	SubjectKey string
	// AnonymousID is the player's analytics identity. This handle's
	// experiment facts carry it as anonymous_id; Config.AnonymousID is never
	// used for them. Without it the handle's assignments are served but not
	// recorded (counted not_recordable).
	AnonymousID string
	// Consent is the player's consent; see SetConsent.
	Consent ExperimentSubjectConsent

	// There is no UserID: ingest refuses user_id on experiment facts, so
	// every experiment fact omits it and a handle's would have no effect.
}

// ExperimentSubject is the experiment plane for one player, for a process
// that serves many players. Its assignments, applications and facts belong
// to its SubjectKey alone; the client's own subject (the spcid_ id the SDK
// mints for FetchExperimentAssignment) is never sent for it and never
// replaces it.
//
// State is in memory, owned by the handle, and ends with Close: nothing is
// written under SpoolDir, and nothing is fetched in the background — a
// cached assignment changes only when the host calls Fetch. Applications
// are sealed and delivered by the client's background lane, like the
// client's own, and Close makes one bounded pass over what is still owed.
//
// A handle refuses (ErrConsentDenied or ErrConsentUnknown, no request)
// unless the player's consent is Granted and the client's consent admits
// the experiment plane. A 401/403 on a handle latches the plane for every
// subject, the client's included, as on the client's own fetches; a
// Retry-After paces the plane.
//
// Methods are safe for concurrent use.
type ExperimentSubject struct {
	c           *Client
	subjectKey  string
	anonymousID string
	// session is this handle's session id, carried by its facts.
	session string

	// deliverMu lets one delivery pass run at a time (the lane, Close).
	deliverMu sync.Mutex
	// commitMu orders a consent change against a fact's hand-off to the
	// queue: a fact is handed off under the consent that admitted it.
	commitMu sync.Mutex

	// withdrawSeq numbers this handle's withdrawals of facts already handed
	// to the queue: a denial (deniedAt) or an age refusal of one experiment
	// (ageWithdrawnAt). A fact carries the number at hand-off; a later
	// withdrawal that covers it drops it at the send point
	// (subjectFactWithdrawn). ageMu is a leaf lock.
	withdrawSeq    atomic.Uint64
	deniedAt       atomic.Uint64
	ageMu          sync.Mutex
	ageWithdrawnAt map[string]uint64

	mu sync.Mutex
	// consent and consentSeq: consentSeq counts SetConsent calls, so a fetch
	// in flight across a consent change installs nothing.
	consent    ExperimentSubjectConsent
	consentSeq uint64
	closed     bool
	// rejected: the platform refused the subject; the handle never sends it again.
	rejected error
	// fetchSeq numbers this handle's fetches at dispatch; settled is the
	// newest authoritative fetch applied per experiment, so an older answer
	// landing later is discarded (the client's per-key fence).
	fetchSeq uint64
	settled  map[string]uint64
	entries  map[string]*expEntry
	// exposed holds the (experiment, version, subject) tuples this handle
	// has recorded or counted: an application is recorded once per handle.
	exposed map[string]bool
	owed    []*expSubjectOwed
	// lastApplied is the recorded application an outcome follows, per
	// experiment.
	lastApplied map[string]expLastApplication
	// declaring holds a non-adult age declaration whose fetch is in flight,
	// per experiment: an age-gated assignment of it is neither served nor
	// recorded until the server's answer to the declaration is applied.
	declaring map[string]*expSubjectDeclaration
}

// expSubjectDeclaration is one handle fetch's non-adult age declaration.
type expSubjectDeclaration struct {
	// fenceSeq is the last fetch dispatched before the declaration; seq is
	// the declaring fetch's own.
	fenceSeq, seq uint64
	// decided is set when the server's answer to the declaring fetch was
	// applied: a verdict, a drop or an install.
	decided bool
}

// expSubjectOwed is one owed application of a handle's assignment, or an
// outcome following one.
type expSubjectOwed struct {
	experimentKey string
	entry         *expEntry
	app           expApplication
	outcome       *expOutcomeRecord
	sealed        *expSealedFact
}

// copy is the record as the client's apply hop takes it.
func (owed *expSubjectOwed) copy(session string) expOwedCopy {
	return expOwedCopy{entry: owed.entry, session: session, app: owed.app, outcome: owed.outcome}
}

// countSubjectOwedLocked reports dropped owed records for reason, each in
// its kind's map: Stats.ExperimentExposureDrops or ExperimentOutcomeDrops.
func countSubjectOwedLocked(e *experimentsState, reason string, records []*expSubjectOwed) {
	for _, record := range records {
		if record.outcome != nil {
			e.countOutcomeDropLocked(reason, 1)
		} else {
			e.countDropLocked(reason, 1)
		}
	}
}

// ExperimentSubject returns the experiment plane for one player. It returns
// ErrExperimentsNotConfigured when ExperimentsEnabled is off, ErrClosed
// after Close, and ErrInvalidExperimentSubject for a SubjectKey outside the
// spcid_ grammar, an AnonymousID over 512 bytes, or an unknown Consent.
func (c *Client) ExperimentSubject(cfg ExperimentSubjectConfig) (*ExperimentSubject, error) {
	if c.exp == nil {
		return nil, ErrExperimentsNotConfigured
	}
	if !validExperimentSubjectID(cfg.SubjectKey) {
		return nil, fmt.Errorf("%w: SubjectKey must match ^spcid_[A-Za-z0-9_-]{20,64}$", ErrInvalidExperimentSubject)
	}
	if len(cfg.AnonymousID) > maxConsentIdentifierBytes {
		return nil, fmt.Errorf("%w: AnonymousID is over %d bytes", ErrInvalidExperimentSubject, maxConsentIdentifierBytes)
	}
	if !cfg.Consent.valid() {
		return nil, fmt.Errorf("%w: unknown Consent value %d", ErrInvalidExperimentSubject, cfg.Consent)
	}
	session, err := uuidv7.New()
	if err != nil {
		return nil, fmt.Errorf("shardpilot experiments: subject session id: %w", err)
	}
	s := &ExperimentSubject{
		c:           c,
		subjectKey:  cfg.SubjectKey,
		anonymousID: cfg.AnonymousID,
		session:     session,
		consent:     cfg.Consent,
		settled:     make(map[string]uint64),
		entries:     make(map[string]*expEntry),
		exposed:     make(map[string]bool),
		lastApplied: make(map[string]expLastApplication),
		declaring:   make(map[string]*expSubjectDeclaration),

		ageWithdrawnAt: make(map[string]uint64),
	}
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.closed.Load() {
		return nil, ErrClosed
	}
	c.expSubjectsMu.Lock()
	if c.expSubjects == nil {
		c.expSubjects = make(map[*ExperimentSubject]struct{})
	}
	c.expSubjects[s] = struct{}{}
	c.expSubjectsMu.Unlock()
	return s, nil
}

// SetConsent records the player's consent. Anything but Granted withdraws
// this handle's cached assignments and owed applications (counted
// consent_withdrawn), drops the player's facts not yet sent (counted in
// Stats.Dropped, like queued events a consent denial purges), and refuses
// the handle's fetches and applications until a later Granted. It returns
// ErrInvalidExperimentSubject for an unknown value.
func (s *ExperimentSubject) SetConsent(consent ExperimentSubjectConsent) error {
	if !consent.valid() {
		return fmt.Errorf("%w: unknown Consent value %d", ErrInvalidExperimentSubject, consent)
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consent = consent
	s.consentSeq++
	if consent != ExperimentSubjectConsentGranted {
		s.countDrops(expDropConsentWithdrawn, s.withdrawAllLocked())
		// The player's facts already in the client's queue, its worker's
		// batch or a retained retry are dropped at the next send point.
		s.deniedAt.Store(s.withdrawSeq.Add(1))
	}
	return nil
}

// refusalLocked is why this handle may not fetch or serve now, or nil.
func (s *ExperimentSubject) refusalLocked() error {
	if s.closed || s.c.closed.Load() {
		return ErrClosed
	}
	if s.rejected != nil {
		return s.rejected
	}
	switch s.consent {
	case ExperimentSubjectConsentGranted:
	case ExperimentSubjectConsentDenied:
		return ErrConsentDenied
	default:
		return ErrConsentUnknown
	}
	return s.c.experimentConsentRefusal()
}

// withdrawAllLocked drops every cached assignment, owed record and
// application an outcome could follow, and returns the dropped records for
// the caller to count.
func (s *ExperimentSubject) withdrawAllLocked() []*expSubjectOwed {
	dropped := s.owed
	s.owed = nil
	s.entries = make(map[string]*expEntry)
	s.exposed = make(map[string]bool)
	s.lastApplied = make(map[string]expLastApplication)
	return dropped
}

// countDrops reports dropped owed records for reason, taking the plane's
// lock (callers must not hold it).
func (s *ExperimentSubject) countDrops(reason string, records []*expSubjectOwed) {
	e := s.c.exp
	e.mu.Lock()
	countSubjectOwedLocked(e, reason, records)
	e.mu.Unlock()
}

// withdrawAgeLocked is an age refusal of one experiment on this handle, the
// client's package for it: the cached assignment (when dropEntry), the
// owed exposures and outcomes, the application an outcome would follow, its
// recorded-once marks, and the facts of the experiment already handed to
// the queue, which the send point drops. It returns the dropped owed
// records for the caller to count.
func (s *ExperimentSubject) withdrawAgeLocked(experimentKey string, dropEntry bool) []*expSubjectOwed {
	if dropEntry {
		delete(s.entries, experimentKey)
	}
	delete(s.lastApplied, experimentKey)
	prefix := escapeRemoteConfigSegment(experimentKey) + rcScopeSeparator
	for tuple := range s.exposed {
		if strings.HasPrefix(tuple, prefix) {
			delete(s.exposed, tuple)
		}
	}
	var dropped, kept []*expSubjectOwed
	for _, owed := range s.owed {
		if owed.experimentKey == experimentKey {
			dropped = append(dropped, owed)
			continue
		}
		kept = append(kept, owed)
	}
	s.owed = kept
	seq := s.withdrawSeq.Add(1)
	s.ageMu.Lock()
	s.ageWithdrawnAt[experimentKey] = seq
	s.ageMu.Unlock()
	return dropped
}

// Fetch asks the platform for this player's assignment of an experiment
// and caches an assigned verdict on the handle. Results and errors are
// FetchExperimentAssignment's, except that a subject fault the platform
// reports (the subject-grammar or PII-marker 400) returns an error wrapping
// ErrInvalidExperimentSubject and is never retried with another subject.
// Attributes are normalized as there; declare an age with "age_band" (see
// FetchWithAgeBand). A non-adult declaration takes effect as on the client:
// an age-gated assignment of the experiment stops serving and recording at
// once, the server's answer then governs, and a fetch that ends without one
// withdraws what the handle owes for the experiment.
func (s *ExperimentSubject) Fetch(ctx context.Context, experimentKey string, attributes map[string]string) (ExperimentAssignmentResult, error) {
	c := s.c
	c.lifecycleMu.Lock()
	if c.closed.Load() {
		c.lifecycleMu.Unlock()
		return ExperimentAssignmentResult{}, ErrClosed
	}
	dispatchConsentEpoch := c.consentEpoch.Load()
	c.trackWG.Add(1)
	defer c.trackWG.Done()
	c.lifecycleMu.Unlock()

	experimentKey = strings.TrimSpace(experimentKey)
	if experimentKey == "" {
		return ExperimentAssignmentResult{}, expFetchError("experiment_key_required")
	}
	s.mu.Lock()
	if err := s.refusalLocked(); err != nil {
		s.mu.Unlock()
		return ExperimentAssignmentResult{}, err
	}
	consentSeq := s.consentSeq
	s.fetchSeq++
	seq := s.fetchSeq
	declaration := s.beginDeclarationLocked(experimentKey, attributes, seq)
	s.mu.Unlock()
	if declaration != nil {
		defer s.endDeclaration(experimentKey, declaration)
	}

	normalized, dropped := normalizeExperimentAssignmentAttributes(attributes)
	if dropped > 0 {
		c.logf("shardpilot experiments: dropped %d targeting attribute(s) outside the vocabulary or value bounds for experiment %q", dropped, experimentKey)
	}
	e := c.exp
	e.mu.Lock()
	authEpoch := e.authEpoch
	scope := expRequestScope{appKey: e.appKey, envKey: e.envKey, experimentKey: experimentKey}
	fetchURL := buildExperimentAssignmentURL(e.baseURL, e.appKey, e.envKey, experimentKey, s.subjectKey, normalized)
	e.mu.Unlock()

	fetchCtx, cancel := contextWithDefaultTimeout(ctx, c.cfg.HTTPTimeout)
	defer cancel()
	// The client's denial aborts the request in flight, as for its own
	// fetches: the gate is loaded before the pre-wire re-check.
	gate := c.consentGate.Load()
	if gate != nil {
		var cancelOnDenial context.CancelFunc
		fetchCtx, cancelOnDenial = context.WithCancel(fetchCtx)
		defer cancelOnDenial()
		stop := context.AfterFunc(gate.ctx, cancelOnDenial)
		defer stop()
	}
	if err := c.experimentConsentRefusal(); err != nil {
		return ExperimentAssignmentResult{}, err
	}
	resp, err := c.transport.FetchRemoteConfig(fetchCtx, remoteConfigRequest{
		url:    fetchURL,
		bearer: c.cfg.PublishableKey,
	})
	if err != nil {
		if gate != nil && gate.ctx.Err() != nil && errors.Is(err, context.Canceled) {
			if refusal := c.experimentConsentRefusal(); refusal != nil {
				return ExperimentAssignmentResult{}, refusal
			}
			return ExperimentAssignmentResult{}, ErrConsentDenied
		}
		if ctx != nil {
			if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				return ExperimentAssignmentResult{}, err
			}
		}
		if resp.status == 0 {
			resp = remoteConfigResponse{}
		}
	}
	nowMS := c.clock.Now().UnixMilli()

	// The commit point, under both locks (handle first): the fences the
	// client's own settle applies, for this handle's state.
	s.mu.Lock()
	e.mu.Lock()
	refusal := s.refusalLocked()
	if refusal == nil && (s.consentSeq != consentSeq || c.consentEpoch.Load() != dispatchConsentEpoch) {
		// A consent change while the request was in flight: the answer
		// predates it and installs nothing.
		refusal = ErrConsentUnknown
		if s.consent == ExperimentSubjectConsentDenied {
			refusal = ErrConsentDenied
		}
	}
	// A 401/403 latched the plane since dispatch: the answer installs
	// nothing and moves no plane state; only its destructive half lands.
	epochStale := authEpoch != e.authEpoch
	// A newer authoritative answer for this experiment already settled.
	fencedOut := seq <= s.settled[experimentKey]
	// A transient serves the cached entry only to a request with the same
	// attribute set; a stale epoch reads as no entry.
	var serveEntry *expEntry
	if !epochStale {
		if entry := s.entries[experimentKey]; entry != nil && experimentAttributesEqual(entry.Attributes, normalized) {
			serveEntry = entry
		}
	}
	result, outcome, failure := applyExperimentAssignment(serveEntry, resp, scope, nowMS)
	constructive := refusal == nil && !epochStale && !fencedOut
	applied, installed := false, false
	if outcome.remint && s.rejected == nil {
		// A subject fault is terminal whatever the fences: withdraw everything.
		s.rejected = fmt.Errorf("%w: the platform refused the subject (%s)", ErrInvalidExperimentSubject, experimentBodyErrorCode(resp.body, resp.bodyIncomplete))
		countSubjectOwedLocked(e, expDropApplyRefused, s.withdrawAllLocked())
	}
	if !fencedOut {
		if outcome.authoritative && !epochStale {
			s.settled[experimentKey] = seq
		}
		// Withdrawals land whatever the consent or epoch; an install only
		// under the consent and credential state that sent the request.
		switch {
		case outcome.dropAll:
			countSubjectOwedLocked(e, expDropRealSubjectsDisabled, s.withdrawAllLocked())
			applied = true
		case outcome.ageRefusal:
			countSubjectOwedLocked(e, expDropAgeIneligible, s.withdrawAgeLocked(experimentKey, true))
			applied = true
		case outcome.dropEntry:
			delete(s.entries, experimentKey)
			applied = outcome.authoritative && !outcome.transient && !outcome.authBlocked
		case outcome.newEntry != nil && constructive:
			entry := outcome.newEntry
			entry.SubjectKey = s.subjectKey
			entry.Attributes = normalized
			s.entries[experimentKey] = entry
			applied, installed = true, true
		}
	}
	// The server's answer to this fetch's own non-adult declaration was
	// applied: it governs from here.
	pending := s.declaring[experimentKey]
	if pending != nil && pending.seq == seq && applied {
		pending.decided = true
	}
	// The plane's shared state: a refused, stale or fenced-out answer
	// neither paces nor lifts the latch. A 401/403 latches unless it is
	// stale or refused; the sentinel latches unless it is fenced out.
	switch {
	case outcome.authBlocked:
		if !fencedOut && (outcome.dropAll || (!epochStale && refusal == nil)) {
			e.latchAuthLocked()
		}
	case outcome.transient:
		if constructive {
			e.paceTransientLocked(nowMS, outcome.retryAfterSeconds, outcome.retryAfterPresent)
		}
	case outcome.authoritative && !outcome.preserveAuth:
		if constructive {
			e.liftAuthLatchLocked()
		}
	}
	// A fenced-out authoritative answer's caller gets the settled current
	// state, never the discarded variant.
	var supersededResult ExperimentAssignmentResult
	supersededFailure := ""
	if fencedOut && outcome.authoritative && !epochStale {
		current := s.entries[experimentKey]
		if current != nil && (e.authBlocked || !experimentAttributesEqual(current.Attributes, normalized) || (pending != nil && current.ageGated())) {
			current = nil
		}
		supersededResult, supersededFailure = serveExperimentEntryOrFail(current, "superseded")
	}
	// A pending non-adult declaration hands no caller an age-gated variant:
	// not another fetch's, and not the declaring fetch's own cached
	// fallback, which its end withdraws. Only the declaring fetch's applied
	// answer is served.
	suppress, suppressCode := false, "superseded"
	if pending != nil && !(pending.seq == seq && pending.decided) {
		source := serveEntry
		if installed {
			source = s.entries[experimentKey]
		}
		suppress = source == nil || source.ageGated()
		if pending.seq == seq && result.Code != "" {
			suppressCode = result.Code
		}
	}
	e.mu.Unlock()
	s.mu.Unlock()

	switch {
	case refusal != nil:
		return ExperimentAssignmentResult{}, refusal
	case epochStale && !outcome.authBlocked:
		return ExperimentAssignmentResult{}, expFetchError("unauthorized")
	case fencedOut && outcome.authoritative:
		if supersededFailure != "" {
			return ExperimentAssignmentResult{}, expFetchError(supersededFailure)
		}
		return supersededResult, nil
	case outcome.remint:
		return ExperimentAssignmentResult{}, fmt.Errorf("%w: the platform refused the subject (%s)", ErrInvalidExperimentSubject, experimentBodyErrorCode(resp.body, resp.bodyIncomplete))
	case failure != "":
		return ExperimentAssignmentResult{}, expFetchError(failure)
	case suppress && result.Assigned:
		return ExperimentAssignmentResult{}, expFetchError(suppressCode)
	}
	return result, nil
}

// FetchWithAgeBand is Fetch with an age_band declaration, as
// FetchExperimentAssignmentWithAgeBand is for the client: only adult can
// qualify for a client-id assignment. The caller's attributes are copied;
// an unknown band or a conflicting age_band returns
// ErrInvalidExperimentAgeBand.
func (s *ExperimentSubject) FetchWithAgeBand(ctx context.Context, experimentKey string, band ExperimentAgeBand, attributes map[string]string) (ExperimentAssignmentResult, error) {
	switch band {
	case ExperimentAgeBandUnknown, ExperimentAgeBandUnderThreshold, ExperimentAgeBandAdult:
	default:
		return ExperimentAssignmentResult{}, ErrInvalidExperimentAgeBand
	}
	if existing, ok := attributes["age_band"]; ok && existing != string(band) {
		return ExperimentAssignmentResult{}, ErrInvalidExperimentAgeBand
	}
	declared := make(map[string]string, len(attributes)+1)
	for name, value := range attributes {
		declared[name] = value
	}
	declared["age_band"] = string(band)
	return s.Fetch(ctx, experimentKey, declared)
}

// beginDeclarationLocked records a fetch's age declaration for the
// experiment, as the client does: a non-adult one stops an age-gated
// assignment serving and recording at once; an adult one replaces a pending
// non-adult one, and a fetch without one changes nothing. It returns the
// non-adult declaration, or nil.
func (s *ExperimentSubject) beginDeclarationLocked(experimentKey string, attributes map[string]string, seq uint64) *expSubjectDeclaration {
	if !declaresNonAdultAge(attributes) {
		if declaresAdultAge(attributes) {
			delete(s.declaring, experimentKey)
		}
		return nil
	}
	declaration := &expSubjectDeclaration{fenceSeq: seq - 1, seq: seq}
	s.declaring[experimentKey] = declaration
	return declaration
}

// endDeclaration runs when a declaring fetch ends, on every path, as the
// client's endNonAdultDeclaration. An applied answer simply ends the
// declaration. Without one, the declaration alone makes the player
// ineligible for a client-id assignment, so it is the age refusal's
// package; the key's fence is raised past every fetch dispatched before the
// declaration, so an earlier adult answer installs nothing when it lands. A
// synthetic-subject assignment has no age gate and keeps serving. A later
// declaration replaced this one: nothing happens.
func (s *ExperimentSubject) endDeclaration(experimentKey string, declaration *expSubjectDeclaration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.declaring[experimentKey] != declaration {
		return
	}
	delete(s.declaring, experimentKey)
	if declaration.decided {
		return
	}
	if s.settled[experimentKey] < declaration.fenceSeq {
		s.settled[experimentKey] = declaration.fenceSeq
	}
	live := s.entries[experimentKey]
	s.countDrops(expDropAgeIneligible, s.withdrawAgeLocked(experimentKey, live == nil || live.ageGated()))
	s.c.logf("shardpilot experiments: a subject handle declared a non-adult age for experiment %q and the fetch ended without the server's answer; withdrew what it owes for the experiment", experimentKey)
}

// servedEntryLocked is the entry this handle serves and records against for
// an experiment, or nil: none while the handle refuses or the plane's
// credential latch is set, and no age-gated one while a non-adult
// declaration of the experiment is pending.
func (s *ExperimentSubject) servedEntryLocked(experimentKey string) *expEntry {
	if s.refusalLocked() != nil {
		return nil
	}
	e := s.c.exp
	e.mu.Lock()
	blocked := e.authBlocked
	e.mu.Unlock()
	if blocked {
		return nil
	}
	entry := s.entries[experimentKey]
	if entry != nil && s.declaring[experimentKey] != nil && entry.ageGated() {
		return nil
	}
	return entry
}

// recordable reports whether the handle can record an application of the
// entry: a client_id assignment with serving state, on a handle with an
// AnonymousID.
func (s *ExperimentSubject) recordable(entry *expEntry) bool {
	return entry.AssignmentUnit == experimentAssignmentUnitClientID && entry.Served != nil && s.anonymousID != ""
}

// Variant returns this player's cached assigned variant for an experiment,
// or "". It records nothing.
func (s *ExperimentSubject) Variant(experimentKey string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.servedEntryLocked(strings.TrimSpace(experimentKey)); entry != nil {
		return entry.VariantKey
	}
	return ""
}

// VariantPayload returns a copy of this player's cached assigned variant's
// payload, or nil. It records nothing.
func (s *ExperimentSubject) VariantPayload(experimentKey string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.servedEntryLocked(strings.TrimSpace(experimentKey)); entry != nil {
		return deepCopyJSONMap(entry.VariantPayload, 0)
	}
	return nil
}

// Apply returns this player's cached assigned variant and a copy of its
// payload, as Variant serves them, and records the host's application:
// once per (experiment, version) per handle, delivered by the client's
// background lane like ApplyExperimentVariant's. An assignment that cannot
// be recorded (no serving state, not a client_id assignment, or a handle
// without an AnonymousID) is still served, and counted not_recordable once.
func (s *ExperimentSubject) Apply(experimentKey string) (string, map[string]any) {
	experimentKey = strings.TrimSpace(experimentKey)
	now := s.c.clock.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.servedEntryLocked(experimentKey)
	if entry == nil {
		return "", nil
	}
	variant, payload := entry.VariantKey, deepCopyJSONMap(entry.VariantPayload, 0)
	tuple := exposureTupleKey(experimentKey, entry)
	if s.exposed[tuple] {
		return variant, payload
	}
	s.exposed[tuple] = true
	e := s.c.exp
	app, err := newExperimentApplication(now)
	if !s.recordable(entry) || err != nil {
		e.mu.Lock()
		e.countDropLocked(expDropNotRecordable, 1)
		e.mu.Unlock()
		return variant, payload
	}
	s.appendOwedLocked(&expSubjectOwed{experimentKey: experimentKey, entry: entry, app: app})
	s.lastApplied[experimentKey] = expLastApplication{entry: entry, app: app}
	return variant, payload
}

// TrackOutcome records an outcome of this player's experiment, as
// TrackExperimentOutcome does for the client: it follows the handle's most
// recent recorded Apply of the experiment, is sealed through the outcome
// apply endpoint by the background lane, and its fact carries the handle's
// AnonymousID. An age refusal withdraws the applications it could follow.
// The key and value rules and the errors are TrackExperimentOutcome's, and
// a handle that refuses returns its refusal; an outcome the handle drops
// before it reaches the analytics queue is counted in
// Stats.ExperimentOutcomeDrops.
func (s *ExperimentSubject) TrackOutcome(experimentKey, outcomeKey string, outcomeValue float64) error {
	experimentKey = strings.TrimSpace(experimentKey)
	outcomeKey = strings.TrimSpace(outcomeKey)
	if experimentKey == "" {
		return fmt.Errorf("%w: experiment key is required", ErrInvalidExperimentFact)
	}
	if !validExperimentOutcomeKey(outcomeKey) {
		return fmt.Errorf("%w: outcome key must be 1-128 of A-Z a-z 0-9 . _ : - and not an IP address", ErrInvalidExperimentFact)
	}
	value, ok := experimentOutcomeInteger(outcomeValue)
	if !ok {
		return fmt.Errorf("%w: outcome value must be an integer of magnitude at most 2^53", ErrInvalidExperimentFact)
	}
	outcomeID, err := newExperimentApplicationID()
	if err != nil {
		return ErrExperimentFactUnavailable
	}
	now := s.c.clock.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refusalLocked(); err != nil {
		return err
	}
	applied, ok := s.lastApplied[experimentKey]
	if ok && s.declaring[experimentKey] != nil && applied.entry.ageGated() {
		// A pending non-adult declaration records nothing under the
		// earlier one.
		return ErrExperimentNoAssignment
	}
	if !ok {
		entry := s.servedEntryLocked(experimentKey)
		switch {
		case entry == nil:
			return ErrExperimentNoAssignment
		case !s.recordable(entry):
			e := s.c.exp
			e.mu.Lock()
			e.countOutcomeDropLocked(expDropNotRecordable, 1)
			e.mu.Unlock()
			return ErrExperimentFactUnavailable
		default:
			return ErrExperimentNotApplied
		}
	}
	if appliedAt, err := time.Parse(time.RFC3339Nano, applied.app.appliedAt); err != nil || now.Before(appliedAt) {
		return fmt.Errorf("%w: the outcome is earlier than the application it follows", ErrInvalidExperimentFact)
	}
	s.appendOwedLocked(&expSubjectOwed{experimentKey: experimentKey, entry: applied.entry, app: applied.app, outcome: &expOutcomeRecord{
		outcomeID:  outcomeID,
		occurredAt: now.UTC().Format(time.RFC3339Nano),
		key:        outcomeKey,
		value:      value,
	}})
	return nil
}

// appendOwedLocked queues an application or an outcome, keeping at most
// expMaxOwedExposures per experiment: a new record displaces the oldest of
// its experiment, counted owed_bound_exceeded in that record's kind's map.
func (s *ExperimentSubject) appendOwedLocked(record *expSubjectOwed) {
	count := 0
	for _, owed := range s.owed {
		if owed.experimentKey == record.experimentKey {
			count++
		}
	}
	if count >= expMaxOwedExposures {
		for i, owed := range s.owed {
			if owed.experimentKey == record.experimentKey {
				s.owed = append(s.owed[:i], s.owed[i+1:]...)
				s.countDrops(expDropOwedBoundExceeded, []*expSubjectOwed{owed})
				break
			}
		}
	}
	s.owed = append(s.owed, record)
}

// Close ends the handle: later calls refuse with ErrClosed. Applications
// still owed get one delivery pass, bounded by ctx; what remains is
// counted unsealed_at_close (never sealed) or undelivered_at_close (sealed,
// not handed to the queue) and in Stats.Dropped. Close is idempotent.
func (s *ExperimentSubject) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	if s.c.experimentConsentRefusal() == nil {
		s.deliver(ctx, true, false, nil)
	}
	s.discardOwed()
	s.c.expSubjectsMu.Lock()
	delete(s.c.expSubjects, s)
	s.c.expSubjectsMu.Unlock()
	return nil
}

// discardOwed counts and drops every application still owed, and the
// cached assignments.
func (s *ExperimentSubject) discardOwed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	remaining := len(s.owed)
	if remaining > 0 {
		e := s.c.exp
		e.mu.Lock()
		for _, owed := range s.owed {
			reason := expDropUndeliveredAtClose
			if owed.sealed == nil {
				reason = expDropUnsealedAtClose
			}
			countSubjectOwedLocked(e, reason, []*expSubjectOwed{owed})
		}
		e.mu.Unlock()
		s.c.stats.dropped.Add(uint64(remaining))
		s.c.stats.setLastError("experiment_exposures_discarded_at_close")
		s.c.logf("shardpilot experiments: %d owed experiment fact(s) of a subject handle discarded at close", remaining)
	}
	s.owed = nil
	s.entries = make(map[string]*expEntry)
	return remaining
}

func (s *ExperimentSubject) owedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.owed)
}

// deliver hands this handle's owed applications to the analytics queue,
// oldest first, having the platform seal each first when network is
// allowed (the lane, Close). It stops at the first application it must
// keep. budget, when set, bounds the apply requests.
func (s *ExperimentSubject) deliver(ctx context.Context, network, atClose bool, budget *int) {
	s.deliverMu.Lock()
	defer s.deliverMu.Unlock()
	c := s.c
	e := c.exp
	for {
		if ctx != nil && ctx.Err() != nil {
			return
		}
		s.mu.Lock()
		if len(s.owed) == 0 || s.consent != ExperimentSubjectConsentGranted {
			s.mu.Unlock()
			return
		}
		head := s.owed[0]
		sealed := head.sealed
		s.mu.Unlock()
		if c.experimentConsentRefusal() != nil {
			return
		}
		if sealed == nil {
			if !network {
				return
			}
			nowMS := c.clock.Now().UnixMilli()
			e.mu.Lock()
			paused := e.applyBlocked || (e.retryAfterMS != 0 && nowMS < e.retryAfterMS)
			e.mu.Unlock()
			if paused {
				return
			}
			if budget != nil {
				if *budget == 0 {
					return
				}
				*budget--
			}
			var status expSubjectSealStatus
			var reason string
			sealed, status, reason = s.seal(ctx, head)
			switch status {
			case expSubjectSealKept:
				return
			case expSubjectSealSettled:
				continue
			case expSubjectSealDropped:
				s.retire(head, reason)
				continue
			}
			s.mu.Lock()
			if len(s.owed) > 0 && s.owed[0] == head {
				head.sealed = sealed
			}
			s.mu.Unlock()
		}
		event, scoped := c.sealedExposureEvent(s.session, sealed, c.expFactPurgeEpoch.Load())
		if !scoped {
			c.logf("shardpilot experiments: a sealed %s for experiment %q names another workspace, app or environment than this client's; dropped (foreign_scope)", head.copy(s.session).kind(), head.experimentKey)
			s.retire(head, expDropForeignScope)
			continue
		}
		event.AnonymousID = s.anonymousID
		event.subject = s
		event.expKeyWithdrawEpoch = c.expKeyWithdrawEpoch.Load()
		if !s.commit(head, event, atClose) {
			return
		}
	}
}

// commit hands one sealed fact to the queue under the consent that admits
// it, and removes its application. It reports false when the application
// must be kept (a consent refusal, a full queue).
func (s *ExperimentSubject) commit(head *expSubjectOwed, event Event, atClose bool) bool {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	s.mu.Lock()
	// The stamp is read with the head check, under the lock every
	// withdrawal takes: a withdrawal after it covers the fact at the send
	// point, one before it removed the head.
	event.subjectStamp = s.withdrawSeq.Load()
	current := s.consent == ExperimentSubjectConsentGranted && len(s.owed) > 0 && s.owed[0] == head
	s.mu.Unlock()
	if !current {
		return true // withdrawn meanwhile: the next pass reads the queue again
	}
	if _, err := s.c.enqueueExperimentFact(event, atClose); err != nil {
		if errors.Is(err, ErrEventTooLarge) {
			// Ingest rejects this fact on every attempt: it leaves the
			// queue, counted once, and never holds the handle's later facts.
			s.c.logf("shardpilot experiments: a subject handle's sealed %s for experiment %q exceeds the 2048-byte ingest limit; dropped (event_too_large)", head.copy(s.session).kind(), head.experimentKey)
			s.retire(head, expDropEventTooLarge)
			return true
		}
		return false
	}
	s.mu.Lock()
	if len(s.owed) > 0 && s.owed[0] == head {
		s.owed = s.owed[1:]
	}
	s.mu.Unlock()
	return true
}

// retire drops the head application, counted for reason, if it is still
// the head.
func (s *ExperimentSubject) retire(head *expSubjectOwed, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.owed) == 0 || s.owed[0] != head {
		return
	}
	s.owed = s.owed[1:]
	s.countDrops(reason, []*expSubjectOwed{head})
}

type expSubjectSealStatus int

const (
	expSubjectSealKept    expSubjectSealStatus = iota // owed; retry later
	expSubjectSealOK                                  // sealed
	expSubjectSealDropped                             // drop the application, counted for the reason
	expSubjectSealSettled                             // a withdrawal already removed and counted it
)

// seal sends one application or outcome to its apply endpoint and
// classifies the answer as the client's own hop does, for this handle's
// state.
func (s *ExperimentSubject) seal(ctx context.Context, head *expSubjectOwed) (*expSealedFact, expSubjectSealStatus, string) {
	c := s.c
	e := c.exp
	owed := head.copy(s.session)
	resp, err := c.postExposureApplication(ctx, head.experimentKey, owed)
	if errors.Is(err, errExperimentApplyConsentRefused) {
		return nil, expSubjectSealKept, ""
	}
	if err != nil && resp.status == 0 {
		if ctx != nil && ctx.Err() != nil {
			return nil, expSubjectSealKept, ""
		}
		resp = remoteConfigResponse{}
	}
	code := experimentBodyErrorCode(resp.body, resp.bodyIncomplete)
	switch {
	case resp.status >= 200 && resp.status < 300 && !resp.bodyIncomplete:
		if parsed := parseSealedExposure(resp.body, owed.eventName()); parsed != nil {
			e.mu.Lock()
			e.backoffAttempt = 0
			e.mu.Unlock()
			return parsed, expSubjectSealOK, ""
		}
	case resp.status == 403 && code == "experiment_apply_disabled":
		e.mu.Lock()
		e.applyBlocked = true
		e.mu.Unlock()
		return nil, expSubjectSealKept, ""
	case resp.status == 403 && code == expSentinelRealSubjectsDisabled:
		s.mu.Lock()
		s.countDrops(expDropRealSubjectsDisabled, s.withdrawAllLocked())
		s.mu.Unlock()
		// The plane fails closed for every subject, as on the fetch route.
		e.mu.Lock()
		e.latchAuthLocked()
		e.mu.Unlock()
		c.logf("shardpilot experiments: the platform disabled real-subject assignment (apply endpoint); dropped a subject handle's assignments and owed applications and latched the plane")
		return nil, expSubjectSealSettled, ""
	case resp.status == 409 && experimentApplyAgeRefusal(resp.body, resp.bodyIncomplete):
		s.mu.Lock()
		s.countDrops(expDropAgeIneligible, s.withdrawAgeLocked(head.experimentKey, true))
		s.mu.Unlock()
		return nil, expSubjectSealSettled, ""
	case resp.status >= 300 && resp.status < 500 && resp.status != 408 && resp.status != 429:
		c.logf("shardpilot experiments: the platform refused a subject handle's %s for experiment %q (HTTP %d); dropped (apply_refused)", owed.kind(), head.experimentKey, resp.status)
		return nil, expSubjectSealDropped, expDropApplyRefused
	}
	seconds, present := 0, false
	if resp.status == 429 || resp.status >= 500 {
		seconds, present = parseRemoteConfigRetryAfter(resp.retryAfterRaw)
	}
	e.mu.Lock()
	e.paceTransientLocked(c.clock.Now().UnixMilli(), seconds, present)
	e.mu.Unlock()
	return nil, expSubjectSealKept, ""
}

// openExperimentSubjects returns the handles not yet closed.
func (c *Client) openExperimentSubjects() []*ExperimentSubject {
	c.expSubjectsMu.Lock()
	defer c.expSubjectsMu.Unlock()
	subjects := make([]*ExperimentSubject, 0, len(c.expSubjects))
	for s := range c.expSubjects {
		subjects = append(subjects, s)
	}
	return subjects
}

// sweepExperimentSubjects delivers every handle's owed applications.
func (c *Client) sweepExperimentSubjects(ctx context.Context, network, atClose bool, budget *int) {
	for _, s := range c.openExperimentSubjects() {
		s.deliver(ctx, network, atClose, budget)
	}
}

func (c *Client) owedExperimentSubjectCount() int {
	count := 0
	for _, s := range c.openExperimentSubjects() {
		count += s.owedCount()
	}
	return count
}

// purgeExperimentSubjects is the client consent denial's share for the
// handles: every owed application and outcome is withdrawn (counted
// consent_withdrawn); a later application records again. Callers hold
// lifecycleMu.
func (c *Client) purgeExperimentSubjects() {
	for _, s := range c.openExperimentSubjects() {
		s.mu.Lock()
		s.countDrops(expDropConsentWithdrawn, s.owed)
		s.owed = nil
		s.exposed = make(map[string]bool)
		s.mu.Unlock()
	}
}

// closeExperimentSubjects ends every handle at client Close, after the
// close path's delivery passes, counting what is still owed.
func (c *Client) closeExperimentSubjects() int {
	remaining := 0
	for _, s := range c.openExperimentSubjects() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		remaining += s.discardOwed()
	}
	c.expSubjectsMu.Lock()
	c.expSubjects = nil
	c.expSubjectsMu.Unlock()
	return remaining
}

// subjectFactWithdrawn reports a subject handle's fact withdrawn since it
// was handed to the queue: its player denied consent, or an age refusal
// withdrew the experiment it records. The fleet consent rule purges
// retained analytics on denial, and an age refusal withdraws the subject's
// facts of the experiment; for a handle's facts, which share the client's
// queue, the purge is this check at every send point.
func subjectFactWithdrawn(subject *ExperimentSubject, stamp uint64, props map[string]any) bool {
	if subject == nil {
		return false
	}
	if stamp < subject.deniedAt.Load() {
		return true
	}
	experimentKey, _ := props["experiment_key"].(string)
	subject.ageMu.Lock()
	defer subject.ageMu.Unlock()
	return stamp < subject.ageWithdrawnAt[experimentKey]
}

// dropDeniedSubjectFacts is the worker-batch leg: at every dispatch point it
// drops the held batch's facts of a player who denied since their hand-off,
// counted in Stats.Dropped like queued events a consent purge clears, and
// the same members from the retained wire bytes, so the survivors keep
// their exact bytes. Each fact carries its own stamp, so every batch is
// checked: a denial leaves the shared queue untouched, and a batch pulled
// from it after the denial must still be filtered. Worker goroutine only.
func (c *Client) dropDeniedSubjectFacts(batch []Event, backoffAttempt *int) []Event {
	kept := batch[:0]
	removedIDs := make(map[string]struct{})
	for _, event := range batch {
		if subjectFactWithdrawn(event.subject, event.subjectStamp, event.Props) {
			removedIDs[strings.TrimSpace(event.ID)] = struct{}{}
			continue
		}
		kept = append(kept, event)
	}
	if len(removedIDs) == 0 {
		return kept
	}
	c.stats.dropped.Add(uint64(len(removedIDs)))
	c.logf("shardpilot experiments: dropped %d queued fact(s) of subject handles withdrawn by a consent denial or an age refusal", len(removedIDs))
	c.retainedRequest = withoutEnvelopeIDs(c.retainedRequest, removedIDs)
	if len(kept) == 0 {
		*backoffAttempt = 0
	}
	return kept
}

// dropDeniedSubjectBuiltBatch re-checks a BUILT batch immediately before its
// transport handoff, for a handle denial that landed between the
// dispatch-point check and the build. The batch and the request are
// filtered by position, so they stay aligned; a misaligned pair filters the
// request by its own members. Worker goroutine only.
func (c *Client) dropDeniedSubjectBuiltBatch(request batchRequest, batch []Event, backoffAttempt *int) (batchRequest, []Event) {
	aligned := len(request.Events) == len(batch) && len(request.rawEvents) == len(batch)
	kept := batch[:0]
	var envelopes []eventEnvelope
	var raws []json.RawMessage
	removed := 0
	for i, event := range batch {
		if subjectFactWithdrawn(event.subject, event.subjectStamp, event.Props) {
			removed++
			continue
		}
		kept = append(kept, event)
		if aligned {
			envelopes = append(envelopes, request.Events[i])
			raws = append(raws, request.rawEvents[i])
		}
	}
	if removed == 0 {
		return request, kept
	}
	c.stats.dropped.Add(uint64(removed))
	c.logf("shardpilot experiments: withheld %d built batch member(s) at the transport handoff (withdrawn on a subject handle by a consent denial or an age refusal)", removed)
	if aligned {
		request.Events = envelopes
		request.rawEvents = raws
	} else {
		ids := make(map[string]struct{})
		for _, envelope := range request.Events {
			if subjectFactWithdrawn(envelope.subject, envelope.subjectStamp, envelope.Props) {
				ids[envelope.EventID] = struct{}{}
			}
		}
		request = withoutEnvelopeIDs(request, ids)
	}
	if len(kept) == 0 {
		*backoffAttempt = 0
	}
	return request, kept
}
