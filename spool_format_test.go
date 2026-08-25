package shardpilot

import (
	"bytes"
	"encoding/json"
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
	if len(doc.entries) != 5 {
		t.Fatalf("torn tail cost %d records, want exactly the one being appended (5 kept)", 5-len(doc.entries))
	}
	if !doc.tornTail {
		t.Fatalf("the tear must be REPORTED, or nothing forces the reconcile")
	}

	// A fresh spool loading this file must reconcile before extending it.
	reloaded := formatTestSpool(t, dir, 2000, 1<<20)
	reloaded.mu.Lock()
	reloaded.tornTailOwed = doc.tornTail
	reloaded.fileAppendable = true // the state a naive implementation would be in
	ok, _ := reloaded.appendRecordsLocked([]spoolEntry{{id: "x", raw: []byte(`{"event_id":"x"}`)}})
	reloaded.mu.Unlock()
	if ok {
		t.Fatalf("an owed reconcile must refuse the append fast path — otherwise the new record fuses to the torn bytes")
	}
}

// TestRaisedCapCannotResurrectEvictedRecords is why the caps are in the header.
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
	if doc.headerMaxEvents != 3 {
		t.Fatalf("the file must state the caps it was written under, got max_events=%d", doc.headerMaxEvents)
	}
}

// TestLastOccurrenceWins is the dedup rule, and first-occurrence is wrong in
// BOTH directions at once — it resurrects a record the writing run dropped
// while losing one it still held.
func TestLastOccurrenceWins(t *testing.T) {
	header := `{"version":3,"max_events":3,"max_bytes":1048576}` + "\n"
	rec := func(id string) string {
		return fmt.Sprintf(`{"raw":{"event_id":%q,"event_ts":"2026-08-25T00:00:00Z"}}`, id) + "\n"
	}
	// A,B,C,D,A on disk under a cap of 3 is the backlog C,D,A.
	data := []byte(header + rec("A") + rec("B") + rec("C") + rec("D") + rec("A"))

	doc := parseSpoolDocument(data)
	if !doc.ok {
		t.Fatalf("parse failed")
	}
	// The design's own worked example: under a cap of 3, A,B,C,D,A is C,D,A.
	assertIDs(t, doc.liveUnderHeaderCaps(), []string{"C", "D", "A"},
		"first-occurrence gives [A B C D], which trims to [B C D] — resurrecting A's dead copy while losing the live one")
}

// TestByteCapReplayedInAppendOrder is the byte-cap half, and dedup-then-trim
// gets it wrong for a different reason than the count cap does.
//
// ⚠ REMOVING AN ID'S EARLIER COPY ERASES THE BYTE PRESSURE IT EXERTED — pressure
// that had already evicted other records. The trim then runs against a total
// the writing run never saw, and lets back in something that was dropped and
// dead-lettered. Reconstruction has to REPLAY, because eviction depends on the
// order and size of everything before it.
func TestByteCapReplayedInAppendOrder(t *testing.T) {
	// Envelopes sized to the example: X(1), A(9), C(2), A(1) under a 10-byte cap.
	// The writer ends holding C,A.
	header := `{"version":3,"max_events":100,"max_bytes":10}` + "\n"
	rec := func(id string, pad int) string {
		return fmt.Sprintf(`{"raw":{"event_id":%q,"p":%q}}`, id, strings.Repeat("x", pad)) + "\n"
	}
	// Sizes are the raw envelope lengths; exact padding does not matter, only
	// that A's first copy is much larger than its second.
	data := []byte(header + rec("X", 0) + rec("A", 40) + rec("C", 4) + rec("A", 0))

	doc := parseSpoolDocument(data)
	if !doc.ok {
		t.Fatalf("parse failed")
	}
	live := doc.liveUnderHeaderCaps()
	for _, e := range live {
		if e.id == "X" {
			t.Fatalf("X was evicted by A's first copy and dead-lettered; a reload must not resurrect it (got %v)", idsOf(live))
		}
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
	if len(doc.entries) != 3 {
		t.Fatalf("v2 load produced %d entries, want 3", len(doc.entries))
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
	live := doc.liveUnderHeaderCaps()
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
	if total > doc.headerMaxBytes {
		t.Fatalf("live backlog is %d bytes, over the file's own %d cap", total, doc.headerMaxBytes)
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
