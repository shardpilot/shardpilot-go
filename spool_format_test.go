package shardpilot

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The v3 append-only format's acceptance, per docs/SPOOL_APPEND_ONLY_DESIGN.md.
//
// These are assertions about BEHAVIOUR against real files, deliberately not
// about wall-clock: the latency numbers are machine-specific and live in
// docs/SPOOL_OVERFLOW_LATENCY_BOUND.md, where they cannot make CI flaky.

func formatTestSpool(t *testing.T, dir string, maxEvents int, maxBytes int) *diskSpool {
	t.Helper()
	return newDiskSpool(Config{
		SpoolDir:       dir,
		SpoolMaxEvents: maxEvents,
		SpoolMaxBytes:  maxBytes,
		WorkspaceID:    "workspace-test",
		EnvironmentID:  "develop",
		AnonymousID:    "anon-format",
	})
}

// formatAppendPadded appends an envelope inflated to put the BYTE cap in
// charge — see TestFileNeverOutgrowsTheReadBound for why that matters.
func formatAppendPadded(t *testing.T, s *diskSpool, id string, now time.Time, pad int) {
	t.Helper()
	raw := spoolTestEnvelope(t, id, now)
	inflated := append([]byte(nil), raw[:len(raw)-1]...)
	inflated = append(inflated, []byte(fmt.Sprintf(`,"pad":%q}`, strings.Repeat("x", pad)))...)
	entry := spoolEntry{id: id, ts: now.UTC().Format(time.RFC3339Nano), raw: inflated}
	refused, _, _, _, persistFailed := s.append([]spoolEntry{entry}, 0, false, now, func() bool { return true })
	if refused || persistFailed {
		t.Fatalf("append %s: refused=%v persistFailed=%v", id, refused, persistFailed)
	}
}

func formatAppend(t *testing.T, s *diskSpool, id string, now time.Time) {
	t.Helper()
	entry := spoolEntry{id: id, ts: now.UTC().Format(time.RFC3339Nano), raw: spoolTestEnvelope(t, id, now)}
	refused, _, _, _, persistFailed := s.append([]spoolEntry{entry}, 0, false, now, func() bool { return true })
	if refused || persistFailed {
		t.Fatalf("append %s: refused=%v persistFailed=%v", id, refused, persistFailed)
	}
}

// TestAppendsAreAppends is acceptance (a)'s first half: across successive
// overflows the existing bytes are UNCHANGED and only new bytes are added.
//
// ⚠ THE INODE IS THE DISCRIMINATOR, AND A PREFIX CHECK IS NOT. This test first
// asserted only that the previous bytes were a prefix of the new ones — which a
// full REWRITE also satisfies, because rewriting an append-ordered list emits
// the same bytes in the same order and then one more record. The mutation that
// disables the fast path passed it. The rewrite path is temp-file-plus-rename,
// so it replaces the inode; an append does not. That is what actually
// distinguishes the two, and it is what this now checks.
func TestAppendsAreAppends(t *testing.T) {
	dir := t.TempDir()
	s := formatTestSpool(t, dir, 2000, 1<<20)
	now := time.Now()
	path := filepath.Join(dir, spoolFileName)

	formatAppend(t, s, "evt-append-0", now)
	previous, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	inode := fileIdentity(t, path)

	for i := 1; i <= 25; i++ {
		formatAppend(t, s, fmt.Sprintf("evt-append-%d", i), now)
		current, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if got := fileIdentity(t, path); got != inode {
			t.Fatalf("append %d REPLACED the file (inode %d -> %d): that is a rewrite, not an append",
				i, inode, got)
		}
		if !bytes.HasPrefix(current, previous) {
			t.Fatalf("append %d changed existing bytes: the previous %d are not a prefix of the new %d",
				i, len(previous), len(current))
		}
		if len(current) <= len(previous) {
			t.Fatalf("append %d added no bytes", i)
		}
		previous = current
	}
}

// fileIdentity is the inode. The spool's rewrite path is temp-file-plus-rename,
// which allocates a new one; an append keeps it.
func fileIdentity(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("no inode on this platform")
	}
	return stat.Ino
}

// TestSpoolStaysBoundedAcrossSustainedOverflow is acceptance (a)'s third half,
// and the one append-only most easily breaks.
//
// ⚠ APPEND-ONLY REMOVES THE REWRITE COST; IT MUST NOT BUY THAT WITH UNBOUNDED
// GROWTH. Eviction here is LOGICAL — the evicted entry leaves the backlog and
// its bytes stay behind as a dead record — so a client that stays offline
// appends for ever against a backlog that never grows. Compaction is what
// bounds it, and this is the test that says so.
func TestSpoolStaysBoundedAcrossSustainedOverflow(t *testing.T) {
	dir := t.TempDir()
	const cap = 50
	s := formatTestSpool(t, dir, cap, 1<<20)
	now := time.Now()
	path := filepath.Join(dir, spoolFileName)

	var peak int64
	for i := 0; i < cap*40; i++ {
		formatAppend(t, s, fmt.Sprintf("evt-bound-%05d", i), now)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %d: %v", i, err)
		}
		if info.Size() > peak {
			peak = info.Size()
		}
	}

	// The live backlog is still exactly the cap…
	count, ok := spoolRecordEventCount(dir)
	if !ok || count != cap {
		t.Fatalf("live backlog = %d (ok=%v), want the cap %d", count, ok, cap)
	}
	// …and the file never ran away. The design states the bound as ~1.5x the
	// live content plus the header allowance; 2000 appends against a 50-entry
	// backlog would be ~40x that without compaction.
	live := s.liveContentBytesLocked()
	ceiling := live*spoolCompactionSlack/2 + spoolHeaderAllowance
	if peak > ceiling {
		t.Fatalf("file peaked at %d bytes, above the %d ceiling for %d bytes of live content",
			peak, ceiling, live)
	}
}

// TestEvictionDoesNotRewrite is the defect this implementation shipped and the
// bound caught: the fast path was gated on "no eviction", so at a FULL spool —
// where every append evicts — every append took the rewrite.
//
// ⚠ THAT IS THE ONE WINDOW THE BOUND MEASURES. It read 234x against a 2x bound,
// while a measurement that stopped at the cap saw 0.3x and looked finished. The
// latency itself is machine-specific and lives in
// docs/SPOOL_OVERFLOW_LATENCY_BOUND.md, so what is asserted here is the
// mechanism underneath it: an evicting append must not replace the file.
func TestEvictionDoesNotRewrite(t *testing.T) {
	dir := t.TempDir()
	const cap = 20
	s := formatTestSpool(t, dir, cap, 1<<20)
	now := time.Now()
	path := filepath.Join(dir, spoolFileName)

	// Fill to the cap, so every further append evicts.
	for i := 0; i < cap; i++ {
		formatAppend(t, s, fmt.Sprintf("evt-eviction-%03d", i), now)
	}
	inode := fileIdentity(t, path)

	rewrites := 0
	const evicting = 10
	for i := 0; i < evicting; i++ {
		formatAppend(t, s, fmt.Sprintf("evt-eviction-over-%03d", i), now)
		if got := fileIdentity(t, path); got != inode {
			rewrites++
			inode = got
		}
	}
	// Compaction is allowed to rewrite occasionally — that is the amortised
	// O(live) cost the design budgets. What must not happen is a rewrite per
	// evicting append, which is what gating the fast path on eviction produced.
	if rewrites >= evicting {
		t.Fatalf("%d of %d evicting appends rewrote the file: eviction is supposed to be LOGICAL", rewrites, evicting)
	}
}

// TestTornTailCostsOneRecordAndForcesReconcile is requirement 2: a crash during
// an append may not cost more than the record being appended.
//
// ⚠ AND THE SECOND HALF IS THE ONE THAT BITES. Dropping the unterminated line
// keeps the records, but its bytes are still at the end of the file — so an
// append onto them would concatenate the new record to a half-written one and
// the new terminator would turn the pair into a COMPLETE malformed line, which
// the next launch discards the whole backlog for. A crash that should have cost
// one record would cost every record, through the mechanism chosen to prevent
// exactly that.
func TestTornTailCostsOneRecordAndForcesReconcile(t *testing.T) {
	dir := t.TempDir()
	s := formatTestSpool(t, dir, 2000, 1<<20)
	now := time.Now()
	path := filepath.Join(dir, spoolFileName)

	for i := 0; i < 5; i++ {
		formatAppend(t, s, fmt.Sprintf("evt-torn-%d", i), now)
	}
	whole, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	// Simulate a torn append: a partial record with no terminator.
	torn := append(append([]byte{}, whole...), []byte(`{"raw":{"event_id":"evt-torn-5","ev`)...)
	if err := os.WriteFile(path, torn, 0o600); err != nil {
		t.Fatalf("write torn: %v", err)
	}

	doc := parseSpoolDocument(torn)
	if !doc.ok {
		t.Fatalf("a torn tail must not discard the document")
	}
	if len(doc.records) != 5 {
		t.Fatalf("torn tail cost %d records, want exactly the one being appended (5 kept)", 5-len(doc.records))
	}
	if !doc.tornTail {
		t.Fatalf("the tear must be REPORTED, or nothing forces the reconcile")
	}

	// A fresh spool loading this file must reconcile before extending it.
	reloaded := formatTestSpool(t, dir, 2000, 1<<20)
	reloaded.mu.Lock()
	reloaded.tornTailOwed = doc.tornTail
	reloaded.fileAppendable = true // the state a naive implementation would be in
	ok, _ := reloaded.appendRecordsLocked([]spoolEntry{{id: "x", raw: []byte(`{"event_id":"x"}`)}}, nil)
	reloaded.mu.Unlock()
	if ok {
		t.Fatalf("an owed reconcile must refuse the append fast path — otherwise the new record fuses to the torn bytes")
	}
}

// TestRaisedCapCannotResurrectEvictedRecords: a later run with a larger cap
// must not resurrect what an earlier run dropped and dead-lettered.
//
// This used to hold because the file stated the caps it was written under and
// the reader re-trimmed to them. It now holds for a stronger reason: the drops
// are RECORDED, so no cap the reader knows about — old or new — can put an
// evicted record back.
func TestRaisedCapCannotResurrectEvictedRecords(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()

	// Run 1 writes under a cap of 3 and evicts down to it.
	small := formatTestSpool(t, dir, 3, 1<<20)
	for i := 0; i < 6; i++ {
		formatAppend(t, small, fmt.Sprintf("evt-cap-%d", i), now)
	}
	if count, _ := spoolRecordEventCount(dir); count != 3 {
		t.Fatalf("run 1 backlog = %d, want 3", count)
	}

	// Run 2 raises the cap. The records run 1 dropped must stay dropped: they
	// were reported as drops, and resending them would deliver events this SDK
	// has already counted gone.
	data, err := os.ReadFile(filepath.Join(dir, spoolFileName))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	doc := parseSpoolDocument(data)
	if !doc.ok {
		t.Fatalf("parse failed")
	}
	got := idsOf(doc.live())
	if len(got) != 3 {
		t.Fatalf("run 1's file holds %v, want the 3 it kept — the other 3 were dead-lettered", got)
	}

	// Run 2 opens the same directory with a cap twice as large.
	big := formatTestSpool(t, dir, 6, 1<<20)
	big.mu.Lock()
	live := idsOf(big.readRecordEntriesLocked())
	big.mu.Unlock()
	if len(live) != 3 {
		t.Fatalf("a raised cap resurrected drops: backlog = %v, want the same 3 run 1 kept", live)
	}
}

// TestRecordedDropsAreAppliedNotInferred is the format's whole contract in one
// case: the reader applies what the writer recorded and decides nothing.
//
// ⚠ THE READER USED TO RE-DERIVE THIS FROM THE CAPS, and being a second
// implementation of eviction is what made it wrong four times. Here the same
// history is read under a header that states NO caps at all — a reader that
// still inferred evictions would have nothing to infer from and would hand
// back every record, including the two the writer dropped.
func TestRecordedDropsAreAppliedNotInferred(t *testing.T) {
	header := `{"version":3}` + "\n"
	rec := func(id string) string {
		return fmt.Sprintf(`{"raw":{"event_id":%q,"event_ts":"2026-08-25T00:00:00Z"}}`, id) + "\n"
	}
	drop := func(id string) string { return fmt.Sprintf(`{"drop":%q}`, id) + "\n" }

	// The writer held A,B,C,D, evicted A and B, then re-appended A.
	data := []byte(header + rec("A") + rec("B") + rec("C") + rec("D") +
		drop("A") + drop("B") + rec("A"))

	doc := parseSpoolDocument(data)
	if !doc.ok {
		t.Fatalf("parse failed")
	}
	// A's SECOND generation is live and holds the position it was appended at;
	// B stays dropped.
	assertIDs(t, doc.live(), []string{"C", "D", "A"},
		"a drop record removes the generation live at that point, and a later re-append is a new one")
}

// TestADropRecordOutlivesTheCapsThatCausedIt: the drops must survive a reader
// configured differently from the writer, in both directions.
func TestADropRecordOutlivesTheCapsThatCausedIt(t *testing.T) {
	header := `{"version":3,"max_events":2,"max_bytes":64}` + "\n"
	rec := func(id string) string {
		return fmt.Sprintf(`{"raw":{"event_id":%q,"event_ts":"2026-08-25T00:00:00Z"}}`, id) + "\n"
	}
	// One record, explicitly dropped. Any cap in the header leaves room for it,
	// so ONLY the drop record can remove it.
	data := []byte(header + rec("A") + fmt.Sprintf(`{"drop":%q}`, "A") + "\n")

	doc := parseSpoolDocument(data)
	if got := idsOf(doc.live()); len(got) != 0 {
		t.Fatalf("live = %v, want empty — A was dropped and the caps had room for it", got)
	}
}

func idsOf(entries []spoolEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.id)
	}
	return out
}

func assertIDs(t *testing.T, entries []spoolEntry, want []string, why string) {
	t.Helper()
	got := idsOf(entries)
	if len(got) != len(want) {
		t.Fatalf("ids = %v, want %v — %s", got, want, why)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ids = %v, want %v — %s", got, want, why)
		}
	}
}

// TestV2FileStillLoads is the migration path: a file written by the previous
// release must not be discarded.
func TestV2FileStillLoads(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	legacy := spoolRecordWire{Version: spoolRecordVersion}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("evt-v2-%d", i)
		legacy.Events = append(legacy.Events, spoolEventWire{Raw: spoolTestEnvelope(t, id, now)})
	}
	payload, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, spoolFileName), payload, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	doc := parseSpoolDocument(payload)
	if !doc.ok || !doc.legacy {
		t.Fatalf("a v2 document must load and be marked legacy (ok=%v legacy=%v)", doc.ok, doc.legacy)
	}
	if len(doc.records) != 3 {
		t.Fatalf("v2 load produced %d entries, want 3", len(doc.records))
	}
}

// TestFileNeverOutgrowsTheReadBound is the one that would have deleted a live
// backlog: compaction is relative to the live content, the loader's bound was
// relative to the live content too — but they were derived differently, and the
// file could legitimately exceed what the loader would read. A restart in that
// window classifies this spool's OWN file as oversized and removes it.
func TestFileNeverOutgrowsTheReadBound(t *testing.T) {
	// ⚠ THE SPOOL HAS TO BE BYTE-CAP DOMINANT, and two earlier versions of this
	// test were not — they passed against the unfixed code and proved nothing.
	//
	// The loader's old bound was max_bytes + 64 KiB + 40 x max_events. For the
	// file to exceed it, 1.5x the live content must be larger than that, which
	// needs the BYTE cap to be the one that binds: with small caps the fixed
	// 64 KiB dominates, and with the default 219-byte test envelope the EVENT
	// cap binds at 438 KB of live content, half the byte cap. Padding the
	// envelopes to ~600 bytes puts the byte cap in charge, which is the
	// configuration the finding describes.
	dir := t.TempDir()
	const maxEvents = 2000
	s := formatTestSpool(t, dir, maxEvents, 1<<20)
	now := time.Now()
	path := filepath.Join(dir, spoolFileName)

	s.mu.Lock()
	limit := s.readLimitLocked()
	s.mu.Unlock()

	for i := 0; i < 6000; i++ {
		formatAppendPadded(t, s, fmt.Sprintf("evt-readbound-%05d", i), now, 420)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %d: %v", i, err)
		}
		if info.Size() > limit {
			t.Fatalf("append %d left the file at %d bytes, past the loader's %d limit — the next start would delete it",
				i, info.Size(), limit)
		}
	}

	// And it still loads: the bound is not met by writing something unreadable.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	doc := parseSpoolDocument(data)
	live := doc.live()
	if !doc.ok || len(live) == 0 {
		t.Fatalf("the file must still load and hold a backlog, got ok=%v live=%d", doc.ok, len(live))
	}
	// The BYTE cap binds here, not the event cap — 8 KiB of ~230-byte
	// envelopes is well under 60 — so the check is that the backlog fills the
	// cap it is actually bound by, not that it reaches maxEvents.
	total := 0
	for _, e := range live {
		total += len(e.raw)
	}
	// The caps live in the header as DIAGNOSTICS — production never reads them
	// back — so this test reads that line itself rather than through the
	// document.
	var head struct {
		MaxBytes int `json:"max_bytes"`
	}
	if err := json.Unmarshal(bytes.SplitN(data, []byte{'\n'}, 2)[0], &head); err != nil {
		t.Fatalf("header: %v", err)
	}
	if total > head.MaxBytes {
		t.Fatalf("live backlog is %d bytes, over the file's own %d cap", total, head.MaxBytes)
	}

}

// TestLoosenedPermissionsFallBackToTheRewrite: O_APPEND ignores the mode on an
// existing file, so an append would keep adding event payloads to a
// world-readable spool where the atomic rewrite replaced it with 0600 every
// time.
func TestLoosenedPermissionsFallBackToTheRewrite(t *testing.T) {
	dir := t.TempDir()
	s := formatTestSpool(t, dir, 2000, 1<<20)
	now := time.Now()
	path := filepath.Join(dir, spoolFileName)

	formatAppend(t, s, "evt-perm-0", now)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	formatAppend(t, s, "evt-perm-1", now)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("the spool is %v after an append onto a loosened file; the rewrite must have re-established 0600", info.Mode().Perm())
	}
}

// TestReloadMatchesTheWritersBacklog is the invariant every replay defect broke,
// asserted directly instead of one worked example at a time.
//
// ⚠ FOUR SEPARATE DEFECTS SHARED ONE STATEMENT: what a restart loads is not
// what the writing run held. Each was found by a reviewer constructing the
// history that exposed it — dedup-before-trim, a re-appended id, a batch member
// evicted on arrival, and the position a settled id left behind — and each was
// fixed without the next one becoming any less likely, because the reader was
// re-deriving decisions the writer never wrote down.
//
// So this drives the REAL writer through appends, batches, re-appends and acks
// against caps tight enough that most appends evict, and after every operation
// asserts the file reloads to exactly the mirror's backlog, in order. It is
// deliberately not a table of cases: the cases were never the problem.
func TestReloadMatchesTheWritersBacklog(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	// Tight caps so eviction is the common case, not the edge.
	s := formatTestSpool(t, dir, 6, 3<<10)

	// A deterministic, non-uniform sequence: batch sizes and paddings vary so
	// byte pressure and count pressure take turns binding, and ids repeat so
	// re-appends land on both live and evicted generations.
	seq := 0
	nextID := func() string { seq++; return fmt.Sprintf("evt-%d", seq) }
	var acked []string

	check := func(step string) {
		t.Helper()
		s.mu.Lock()
		want := idsOf(s.entries)
		got := idsOf(s.readRecordEntriesLocked())
		s.mu.Unlock()
		if len(want) != len(got) {
			t.Fatalf("%s: reload holds %v, writer holds %v", step, got, want)
		}
		for i := range want {
			if want[i] != got[i] {
				t.Fatalf("%s: reload holds %v, writer holds %v (differ at %d)", step, got, want, i)
			}
		}
	}

	for round := 0; round < 40; round++ {
		// Batches of 1..4, padded on a rotating schedule.
		batch := make([]spoolEntry, 0, 4)
		for i := 0; i <= round%4; i++ {
			id := nextID()
			raw := spoolTestEnvelope(t, id, now)
			pad := (round * 37) % 900
			inflated := append([]byte(nil), raw[:len(raw)-1]...)
			inflated = append(inflated, []byte(fmt.Sprintf(`,"pad":%q}`, strings.Repeat("x", pad)))...)
			batch = append(batch, spoolEntry{id: id, ts: now.UTC().Format(time.RFC3339Nano), raw: inflated})
		}
		refused, added, _, _, persistFailed := s.append(batch, 0, false, now, func() bool { return true })
		if refused || persistFailed {
			t.Fatalf("round %d: refused=%v persistFailed=%v", round, refused, persistFailed)
		}
		check(fmt.Sprintf("round %d append", round))

		// Every third round, ack the oldest live record — this is what leaves a
		// settled id behind for a later generation to collide with.
		if round%3 == 2 {
			s.mu.Lock()
			var victim string
			if len(s.entries) > 0 {
				victim = s.entries[0].id
			}
			s.mu.Unlock()
			if victim != "" {
				s.ack([]string{victim})
				acked = append(acked, victim)
				check(fmt.Sprintf("round %d ack %s", round, victim))
			}
		}

		// Every fourth round, re-append an id already used — sometimes still
		// live, sometimes long evicted, sometimes acked.
		if round%4 == 3 && len(added) > 0 && len(acked) > 0 {
			id := acked[len(acked)-1]
			raw := spoolTestEnvelope(t, id, now)
			re := []spoolEntry{{id: id, ts: now.UTC().Format(time.RFC3339Nano), raw: raw}}
			if refused, _, _, _, pf := s.append(re, 0, false, now, func() bool { return true }); refused || pf {
				t.Fatalf("round %d re-append: refused=%v persistFailed=%v", round, refused, pf)
			}
			check(fmt.Sprintf("round %d re-append %s", round, id))
		}
	}

	// The run has to have exercised eviction, or the whole thing proves nothing.
	s.mu.Lock()
	held := len(s.entries)
	s.mu.Unlock()
	if seq <= held {
		t.Fatalf("appended %d records and still hold %d — eviction never ran, so this asserted nothing", seq, held)
	}
}

// TestAnEvictionIsReportedExactlyOnce: append RETURNS its evictions, so it must
// not also queue them.
//
// ⚠ THE TWO QUEUES ARE FOR DIFFERENT CALLERS. pendingCapacityDrops carries
// evictions the caller never sees — the ones a merging save makes against
// foreign records — and the client drains it after every operation. Putting
// append's own evictions there too means the client dead-letters and counts
// each dropped event twice: once from the return value, once from the drain.
func TestAnEvictionIsReportedExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	s := formatTestSpool(t, dir, 3, 1<<20)

	for i := 0; i < 3; i++ {
		formatAppend(t, s, fmt.Sprintf("evt-once-%d", i), now)
	}
	// This one evicts evt-once-0 on the fast path.
	entry := spoolEntry{id: "evt-once-3", ts: now.UTC().Format(time.RFC3339Nano), raw: spoolTestEnvelope(t, "evt-once-3", now)}
	_, _, _, evicted, persistFailed := s.append([]spoolEntry{entry}, 0, false, now, func() bool { return true })
	if persistFailed {
		t.Fatalf("append failed")
	}
	if len(evicted) != 1 || evicted[0].id != "evt-once-0" {
		t.Fatalf("returned evictions = %v, want [evt-once-0]", idsOf(evicted))
	}

	queued := s.takeCapacityDrops()
	if len(queued) != 0 {
		t.Fatalf("the same eviction was queued as well as returned (%v) — the client reports it twice", idsOf(queued))
	}
}

// TestFailedCompactionKeepsSyncedAppendsCounted: the append's durability is not
// the compaction's.
//
// ⚠ THE RECORDS ARE FSYNCED BEFORE COMPACTION IS EVEN CONSIDERED. A rewrite
// that then fails has reclaimed nothing and undone nothing, so treating its
// failure as the append's marked durable records uncounted — and a disk-full
// Close would then report events as discarded that the next start loads.
func TestFailedCompactionKeepsSyncedAppendsCounted(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	s := formatTestSpool(t, dir, 2000, 1<<20)

	// Grow the file to just under the three-quarter compaction threshold with
	// real appends — faking fileBytes would trip the foreign-writer check
	// instead — so that the ONE append below crosses it.
	for i := 0; ; i++ {
		s.mu.Lock()
		room := s.readLimitLocked()*3/4 - s.fileBytes
		s.mu.Unlock()
		if room < 2<<10 {
			break
		}
		if i > 20000 {
			t.Fatalf("never approached the compaction threshold")
		}
		formatAppendPadded(t, s, fmt.Sprintf("evt-cf-fill-%d", i), now, 2<<10)
	}
	// Only the REWRITE fails; the append seam is untouched.
	rewrites := 0
	s.mu.Lock()
	s.renameFn = func(string, string) error { rewrites++; return errors.New("disk full") }
	s.mu.Unlock()

	// Padded so it CROSSES the threshold rather than merely approaching it.
	raw := spoolTestEnvelope(t, "evt-cf-1", now)
	inflated := append(append([]byte(nil), raw[:len(raw)-1]...),
		[]byte(fmt.Sprintf(`,"pad":%q}`, strings.Repeat("x", 4<<10)))...)
	entry := spoolEntry{id: "evt-cf-1", ts: now.UTC().Format(time.RFC3339Nano), raw: inflated}
	_, added, _, _, persistFailed := s.append([]spoolEntry{entry}, 0, false, now, func() bool { return true })
	if len(added) != 1 {
		t.Fatalf("added = %d, want 1", len(added))
	}
	if persistFailed {
		t.Fatalf("the append synced; a failed compaction must not report it as not durable")
	}
	s.mu.Lock()
	uncounted := len(s.uncountedIDs)
	s.mu.Unlock()
	if uncounted != 0 {
		t.Fatalf("%d addition(s) marked uncounted for a failure that did not touch them", uncounted)
	}
	// ⚠ AND THE COMPACTION MUST ACTUALLY HAVE BEEN ATTEMPTED. Without this the
	// test passes for a spool that never crossed the threshold, which is how
	// three earlier tests in this change passed against the unfixed code.
	if rewrites == 0 {
		t.Fatalf("the failing rewrite never ran — this asserted nothing")
	}
}

// TestReloadOfALargeSpoolStaysLinear guards the cost of reconstruction, which
// the recorded-drop format makes O(n) and the re-derivation did not.
//
// ⚠ THE BOUND IS FIXED HERE, NOT CHOSEN FROM THE RESULT: a 100,000-record spool
// — the top of the range this repository's caps already allow — must reload in
// under a second. Measured against the re-deriving reader this change removes:
// 10k took 41ms, 40k 1.14s and 80k 7.22s, quadratic, extrapolating to roughly
// 11s at 100k, so the first restart after a large spool stalled startup. The
// same inputs on the recorded-drop reader took 1.4ms, 7.7ms and 27ms.
func TestReloadOfALargeSpoolStaysLinear(t *testing.T) {
	if testing.Short() {
		t.Skip("timing bound")
	}
	const n = 100000
	const bound = time.Second

	var buf []byte
	buf = append(buf, []byte(`{"version":3,"max_events":100000,"max_bytes":268435456}`+"\n")...)
	for i := 0; i < n; i++ {
		buf = append(buf, []byte(fmt.Sprintf(
			`{"raw":{"event_id":"evt-scale-%d","event_ts":"2026-08-25T00:00:00Z"}}`, i)+"\n")...)
	}
	doc := parseSpoolDocument(buf)
	if !doc.ok || len(doc.records) != n {
		t.Fatalf("fixture did not parse: ok=%v records=%d", doc.ok, len(doc.records))
	}

	start := time.Now()
	live := doc.live()
	elapsed := time.Since(start)

	if len(live) != n {
		t.Fatalf("live = %d, want %d — the fixture must actually be reconstructed", len(live), n)
	}
	if elapsed > bound {
		t.Fatalf("reconstructing %d records took %v, over the %v bound — reconstruction is superlinear again", n, elapsed, bound)
	}
}

// TestOneBatchCannotCrossTheReadBound: compaction runs AFTER the append, so it
// cannot rescue a batch big enough to cross the loader's bound by itself.
//
// ⚠ THE RECORDS ARE FSYNCED BEFORE COMPACTION IS CONSIDERED. A crash in that
// window leaves a file the next startup reads as oversized, removes, and loses
// the entire live backlog for — a change meant to be latency-only deleting a
// backlog. The batch has to be refused BEFORE the write, which routes it to the
// rewrite, whose size is bounded by the live set.
func TestOneBatchCannotCrossTheReadBound(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	// A byte-cap-dominant spool: the payload one batch may carry is bounded by
	// maxBytes, so only a large one can cross the loader's allowance in a
	// single append.
	s := formatTestSpool(t, dir, 10, 1<<20)

	big := func(id string) spoolEntry {
		raw := spoolTestEnvelope(t, id, now)
		inflated := append(append([]byte(nil), raw[:len(raw)-1]...),
			[]byte(fmt.Sprintf(`,"pad":%q}`, strings.Repeat("x", 100<<10)))...)
		return spoolEntry{id: id, ts: now.UTC().Format(time.RFC3339Nano), raw: inflated}
	}

	// ⚠ THE END STATE IS NOT THE INVARIANT — the compaction that runs after the
	// append shrinks the file back before any assertion here could see it, and
	// the whole point is the window BETWEEN the two, where a crash finds the
	// file. So watch the size each append itself leaves behind.
	var peak int64
	inner := s.appendFn
	s.appendFn = func(p string, b []byte) error {
		err := inner(p, b)
		if info, statErr := os.Stat(p); statErr == nil && info.Size() > peak {
			peak = info.Size()
		}
		return err
	}

	// Grow the file with dead records until it sits just below the compaction
	// threshold, then let one full batch land on top of it.
	for i := 0; i < 200; i++ {
		s.mu.Lock()
		near := s.fileBytes*10 > s.readLimitLocked()*7
		s.mu.Unlock()
		if near {
			break
		}
		if refused, _, _, _, pf := s.append([]spoolEntry{big(fmt.Sprintf("evt-fill-%d", i))},
			0, false, now, func() bool { return true }); refused || pf {
			t.Fatalf("fill %d: refused=%v persistFailed=%v", i, refused, pf)
		}
	}

	s.mu.Lock()
	before, limit := s.fileBytes, s.readLimitLocked()
	s.mu.Unlock()
	batch := make([]spoolEntry, 0, 10)
	payload := 0
	for i := 0; i < 10; i++ {
		e := big(fmt.Sprintf("evt-bound-%d", i))
		payload += len(e.raw)
		batch = append(batch, e)
	}
	if int64(payload)+before <= limit {
		t.Fatalf("fixture too small: %d bytes onto %d does not reach the %d bound", payload, before, limit)
	}

	if refused, _, _, _, pf := s.append(batch, 0, false, now, func() bool { return true }); refused || pf {
		t.Fatalf("append: refused=%v persistFailed=%v", refused, pf)
	}

	s.mu.Lock()
	limit = s.readLimitLocked()
	s.mu.Unlock()
	if peak > limit {
		t.Fatalf("an append left the file at %d bytes, past the loader's %d limit — a crash before the rewrite would delete the backlog", peak, limit)
	}
	// And the backlog is still there: the bound is not met by writing nothing.
	s.mu.Lock()
	live := len(s.readRecordEntriesLocked())
	s.mu.Unlock()
	if live == 0 {
		t.Fatalf("the file holds no backlog at all")
	}
}

// TestAFreshGenerationInABatchIsTheOneWritten: when a batch carries an expired
// occurrence of an id followed by a fresh one, the fresh envelope is what
// reaches the file.
//
// ⚠ REBUILDING THE APPEND PAYLOAD BY ID TAKES THE FIRST OCCURRENCE, which is
// the stale one here. The insert loop rejects the expired copy and stores the
// fresh one, so a payload reconstructed from the original batch disagrees with
// the mirror: the call reports the fresh event durable while the file holds the
// copy the next start expires and drops.
func TestAFreshGenerationInABatchIsTheOneWritten(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	s := formatTestSpool(t, dir, 2000, 1<<20)

	stale := spoolEntry{
		id:  "evt-gen",
		ts:  now.Add(-30 * 24 * time.Hour).UTC().Format(time.RFC3339Nano),
		raw: spoolTestEnvelope(t, "evt-gen", now.Add(-30*24*time.Hour)),
	}
	fresh := spoolEntry{
		id:  "evt-gen",
		ts:  now.UTC().Format(time.RFC3339Nano),
		raw: spoolTestEnvelope(t, "evt-gen", now),
	}
	if string(stale.raw) == string(fresh.raw) {
		t.Fatalf("fixture cannot distinguish the generations — both envelopes are identical")
	}

	refused, added, expired, _, pf := s.append([]spoolEntry{stale, fresh}, 0, false, now, func() bool { return true })
	if refused || pf {
		t.Fatalf("append: refused=%v persistFailed=%v", refused, pf)
	}
	if len(expired) != 1 {
		t.Fatalf("the stale copy must be reported expired, got %d", len(expired))
	}
	if len(added) != 1 || string(added[0].raw) != string(fresh.raw) {
		t.Fatalf("added reports the wrong generation")
	}

	s.mu.Lock()
	onDisk := s.readRecordEntriesLocked()
	s.mu.Unlock()
	if len(onDisk) != 1 {
		t.Fatalf("file holds %d records, want 1", len(onDisk))
	}
	if string(onDisk[0].raw) != string(fresh.raw) {
		t.Fatalf("the file holds the STALE envelope while the call reported the fresh one durable:\n on disk: %s\n wanted:  %s",
			onDisk[0].raw, fresh.raw)
	}
}

// TestTheMirrorsEnvelopeSurvivesAFailedRemoval: a settled generation left on
// disk by a failed rewrite must not overwrite the live one that replaced it.
//
// ⚠ SHARING AN ID IS NOT BEING THE SAME GENERATION. An ack whose rewrite failed
// leaves the settled envelope on disk; an id reused before that retry lands
// makes both exist at once. Merging the disk copy there persists the OLD
// envelope while reporting the new one durable, and a restart resends the
// settled payload and loses its replacement.
func TestTheMirrorsEnvelopeSurvivesAFailedRemoval(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	s := formatTestSpool(t, dir, 2000, 1<<20)

	oldRaw := spoolTestEnvelope(t, "evt-reuse", now.Add(-time.Hour))
	formatAppendRaw(t, s, "evt-reuse", now.Add(-time.Hour), oldRaw)

	// Ack it, but the removing rewrite fails — the settled copy stays on disk.
	s.mu.Lock()
	s.renameFn = func(string, string) error { return errors.New("disk full") }
	s.appendFn = func(string, []byte) error { return errors.New("disk full") }
	s.mu.Unlock()
	if _, pf := s.ack([]string{"evt-reuse"}); !pf {
		t.Fatalf("the ack's rewrite was supposed to fail — fixture cannot reach the state it describes")
	}
	s.mu.Lock()
	stillOnDisk := len(s.readRecordEntriesLocked())
	s.mu.Unlock()
	if stillOnDisk != 1 {
		t.Fatalf("the settled copy must still be on disk, got %d records", stillOnDisk)
	}

	// Writes recover, and the id is reused with a DIFFERENT envelope.
	s.mu.Lock()
	s.renameFn = os.Rename
	s.appendFn = appendPrivateFile
	s.mu.Unlock()
	newRaw := spoolTestEnvelope(t, "evt-reuse", now)
	if string(newRaw) == string(oldRaw) {
		t.Fatalf("fixture cannot distinguish the generations")
	}
	formatAppendRaw(t, s, "evt-reuse", now, newRaw)

	s.mu.Lock()
	onDisk := s.readRecordEntriesLocked()
	s.mu.Unlock()
	if len(onDisk) != 1 {
		t.Fatalf("file holds %d records, want 1", len(onDisk))
	}
	if string(onDisk[0].raw) != string(newRaw) {
		t.Fatalf("the settled generation overwrote the live one:\n on disk: %s\n wanted:  %s",
			onDisk[0].raw, newRaw)
	}
}

// TestIDLessV3RecordIsAccountedNotSwallowed: an envelope with no usable
// event_id is unprovable, not absent.
//
// ⚠ FILTERING IT IN THE PARSER MAKES IT VANISH WITH NO ACCOUNTING. The
// documented behaviour is a reported fail-closed drop — load classifies it,
// SpoolExpired counts it, OnSpoolDeadLetter surfaces it — and the v2 path has
// always passed it through. A silent parser-level drop is a different outcome
// wearing the same green suite.
func TestIDLessV3RecordIsAccountedNotSwallowed(t *testing.T) {
	header := `{"version":3}` + "\n"
	withID := `{"raw":{"event_id":"evt-ok","event_ts":"2026-08-25T00:00:00Z"}}` + "\n"
	noID := `{"raw":{"event_ts":"2026-08-25T00:00:00Z"}}` + "\n"

	doc := parseSpoolDocument([]byte(header + withID + noID))
	if !doc.ok {
		t.Fatalf("parse failed")
	}
	live := doc.live()
	if len(live) != 2 {
		t.Fatalf("live = %d records, want 2 — the id-less record must reach load to be classified there, not be dropped here", len(live))
	}
	blank := 0
	for _, e := range live {
		if e.id == "" {
			blank++
		}
	}
	if blank != 1 {
		t.Fatalf("expected exactly one id-less record carried through, got %d", blank)
	}
}

func formatAppendRaw(t *testing.T, s *diskSpool, id string, now time.Time, raw json.RawMessage) {
	t.Helper()
	entry := spoolEntry{id: id, ts: now.UTC().Format(time.RFC3339Nano), raw: raw}
	refused, _, _, _, persistFailed := s.append([]spoolEntry{entry}, 0, false, time.Now(), func() bool { return true })
	if refused || persistFailed {
		t.Fatalf("append %s: refused=%v persistFailed=%v", id, refused, persistFailed)
	}
}
