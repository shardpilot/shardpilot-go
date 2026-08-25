# Spool Append-Only — Acceptance (Go SDK, Phase 1)

Phase 1 is the **format change alone**, as
`SPOOL_APPEND_ONLY_DESIGN.md` (shipped with the Unity SDK, the reference
implementation) prescribes: it removes the O(spool-size) growth while the write
stays synchronous, so durability is exactly as strong as before and the
exactly-once hole asynchrony creates does not exist yet. Phase 2 — the worker —
opens that hole, so it is not opened until the thing that closes it is built.

## The bound, evaluated

`SPOOL_OVERFLOW_LATENCY_BOUND.md` fixed these before the implementation existed.

Re-measured 2026-08-25 against the final format (three runs; the spread is the
run-to-run variation, not a range of protocols). 2000 appends growing to the
caps, then 2000 more **past** them so every append evicts.

| | baseline | after | bound | |
|---|---|---|---|---|
| p95 full ÷ p95 empty | **6.7x** | **0.9 – 1.3x** | ≤ 2x | ✅ |
| p99 at a full spool | **22.6 ms** | **0.34 – 0.38 ms** | ≤ 1.0 ms | ✅ |
| total, 2000 appends | 18 602.8 ms | 291 – 371 ms | — | ~58x |
| single append at a full spool | 19.6 ms | ~0.17 ms | — | |

### ⚠ The total moved, and the reason is a durability fix — not a regression

An earlier revision of this table read **84.6 ms** and **220x**. That number was
measured before `appendPrivateFile` called `Sync`. The atomic rewrite it
replaced synced its temp file before renaming, so a write that returned meant
the bytes were on the device; an append that only wrote and closed returned as
soon as the kernel held the page. The change was therefore not latency-only —
it had traded a durability guarantee for speed while claiming it had not, which
is worse than the trade, because the claim is what a reader relies on.

Syncing every append costs roughly 3.5x of the post-change total. It is bought
back in Phase 2, where the sync moves off the caller. **The bound is about the
SHAPE of the cost — constant in the backlog — and that is what the ratio and the
p99 measure; both still pass with room.**

Part 3 (no caller-side wait bought back) belongs to Phase 2 and is not
evaluated here.

## ⚠ The first measurement was taken in the one regime that hides the defect

The first run appended exactly `SpoolMaxEvents` events, so **nothing ever
evicted**. It read p95-ratio 0.3x and 788x faster, and looked finished.

The fast path was gated on `len(evicted) == 0`. Eviction happens when the spool
is **full** — which is the window the bound measures p95 in — so at a full spool
every append evicted, every eviction took the rewrite, and the O(spool) cost was
exactly where it started. Measured past the cap:

```
p95 empty 0.030 ms   p95 full 7.036 ms   ratio 234.5x     (bound: 2x)
```

The design document names this trap in as many words and the implementation
walked into it anyway. Eviction is now **logical** — the entry leaves the mirror,
its bytes stay as a dead record, compaction reclaims them amortised — which is
what the 0.9x above is measured against.

The lesson is about the measurement, not the code: **a benchmark that stops at
the cap cannot see the cost at the cap.**

## Committed tests

Behaviour against real files, in `spool_format_test.go`. Deliberately no
wall-clock assertions: those numbers are machine-specific and would make CI
flaky, so they live in the bound document instead.

| Test | Pins |
|---|---|
| `TestAppendsAreAppends` | the file is EXTENDED, not replaced — by inode |
| `TestEvictionDoesNotRewrite` | an evicting append does not replace the file |
| `TestSpoolStaysBoundedAcrossSustainedOverflow` | 2000 appends against a 50-entry cap stay inside the ceiling |
| `TestTornTailCostsOneRecordAndForcesReconcile` | a tear costs one record, and owes a reconcile before extending |
| `TestRaisedCapCannotResurrectEvictedRecords` | a larger cap in a later run cannot resurrect a drop |
| `TestRecordedDropsAreAppliedNotInferred` | the reader applies recorded drops and infers nothing |
| `TestADropRecordOutlivesTheCapsThatCausedIt` | a drop holds even where the caps had room |
| `TestReloadMatchesTheWritersBacklog` | reload == the writer's backlog, across 40 rounds of appends, acks and re-appends |
| `TestAnEvictionIsReportedExactlyOnce` | an eviction is returned or queued, never both |
| `TestFailedCompactionKeepsSyncedAppendsCounted` | a failed rewrite does not un-count a synced append |
| `TestOneBatchCannotCrossTheReadBound` | no single append leaves the file unloadable |
| `TestReloadOfALargeSpoolStaysLinear` | 100k records reload in under a second |
| `TestAFreshGenerationInABatchIsTheOneWritten` | a stale copy earlier in the batch is not the one appended |
| `TestTheMirrorsEnvelopeSurvivesAFailedRemoval` | a settled copy left by a failed rewrite does not overwrite its replacement |
| `TestIDLessV3RecordIsAccountedNotSwallowed` | an unprovable envelope reaches load to be classified, not dropped in the parser |
| `TestV2FileStillLoads` | the previous release's file is not discarded |

### Mutations

Each fix reversed, the test re-run, and the failure confirmed to be for the
stated reason:

| Mutation | Result |
|---|---|
| fast path disabled | `TestAppendsAreAppends` — *"append 1 REPLACED the file (inode 2133010 -> 2133011)"* |
| compaction disabled | `TestSpoolStaysBoundedAcrossSustainedOverflow` fails |
| eviction gated back to the rewrite | `TestEvictionDoesNotRewrite` fails |
| torn tail not reported | `TestTornTailCostsOneRecordAndForcesReconcile` fails |
| evictions not recorded as drop records | `TestReloadMatchesTheWritersBacklog` — *"round 3 append: reload holds [evt-2 … evt-10], writer holds [evt-5 … evt-10]"* |
| settled suppression ignores the live generation | `TestReloadMatchesTheWritersBacklog` — *"reload holds [evt-10 evt-11 evt-12 evt-13 evt-1], writer holds [evt-10 evt-1 evt-11 evt-12 evt-13]"* |
| read bound not checked before appending | `TestOneBatchCannotCrossTheReadBound` — *"an append left the file at 2053090 bytes, past the loader's 1639000 limit"* |
| evictions queued as well as returned | `TestAnEvictionIsReportedExactlyOnce` fails |
| compaction failure treated as append failure | `TestFailedCompactionKeepsSyncedAppendsCounted` fails |
| a per-record rescan of the replay | `TestReloadOfALargeSpoolStaysLinear` — *"took 2.98s, over the 1s bound"* |
| append payload rebuilt by id from the batch | `TestAFreshGenerationInABatchIsTheOneWritten` — *"added reports the wrong generation"* |
| disk copy assumed to be the live generation | `TestTheMirrorsEnvelopeSurvivesAFailedRemoval` — *"the settled generation overwrote the live one"* |
| id-less v3 record filtered in the parser | `TestIDLessV3RecordIsAccountedNotSwallowed` — *"live = 1 records, want 2"* |

⚠ **Five of these mutations survived the first time, and every one of them was a
wrong TEST rather than wrong code — always the same shape: the assertion was
right and the fixture could not reach the condition it described.**

- `TestOneBatchCannotCrossTheReadBound` passed twice against unfixed code. The
  first fixture used caps small enough that the loader's fixed 64 KiB overhead
  dominated its bound; the second asserted the file's size *after* the append,
  where the compaction that follows had already shrunk it back — hiding the
  exact window the defect lives in. It now watches the peak size an append
  leaves behind, on a byte-cap-dominant spool.
- `TestFailedCompactionKeepsSyncedAppendsCounted` passed against a spool that
  never crossed the compaction threshold, so the failing rewrite never ran. It
  now asserts the rewrite was attempted.
- `TestAppendsAreAppends` asserted only that the previous
  bytes were a **prefix** of the new ones — which a full rewrite also satisfies,
  because rewriting an append-ordered list emits the same bytes and then one
  more. It now checks the **inode**, which is what actually separates the two:
  the rewrite path is temp-file-plus-rename.
- `TestSpoolStaysBoundedAcrossSustainedOverflow` passed with compaction disabled
  because eviction was still rewriting, which kept the file small for the wrong
  reason — the same defect the bound caught, showing up as an assertion that
  could not fail.

## Review round 2 — five findings, two of them severe

| Finding | What it was |
|---|---|
| **no `fsync`** | The append wrote and closed. The atomic rewrite it replaces syncs before renaming, so "durably spooled" had quietly become "the kernel has the page" — falsifying the one claim the Phase 1 / Phase 2 split rests on. |
| **file could outgrow the loader's own bound** | Compaction let the file reach ~1.5x live; `readRecordBytesLocked` rejected above `max_bytes + 64 KiB + 40 x max_events`. In between, a restart classifies this spool's OWN file as oversized, deletes it, and loses the whole backlog. |
| double dead-letter | An evicting append that then failed its compaction queued the drop twice — immediately, and again deferred. |
| permissions | `O_APPEND` ignores the mode on an existing file and never touches the directory, where the rewrite re-established both on every write. |
| byte-cap replay | De-duplicating before applying `max_bytes` erases the byte pressure the removed copy exerted, so a reload resurrects a record that was already capacity-dead-lettered. |

The last one is the fourth thing logical eviction cost that was not anticipated,
after the three the design document lists. The reconstruction now **replays** the
append sequence under the file's caps rather than de-duplicating and trimming,
because eviction depends on the order and size of everything before it — which
is exactly what a set-then-trim throws away.

### ⚠ Three versions of one test proved nothing

`TestFileNeverOutgrowsTheReadBound` passed against the **unfixed** code twice
before it reproduced anything.

- At 8 KiB caps the loader's fixed 64 KiB overhead dominates its bound, so 1.5x
  the live content never approaches it.
- At the default caps with the standard 219-byte test envelope, the **event**
  cap binds at 438 KB of live content — half the byte cap — so again the file
  stays small.

The finding needs a **byte-cap-dominant** spool. Padding the envelopes to ~640
bytes puts the byte cap in charge, and the pre-fix code then fails on append
1803 at 1 194 300 bytes against a 1 194 112 limit.

The pattern across this review is now unmistakable and worth stating once: **the
tests that failed to fail were all tests whose fixture could not reach the
condition they described.** Not wrong assertions — unreachable ones.

### The fsync is verified by inspection

Observing `fsync` from Go needs a syscall seam, and adding a mechanism to
exercise a one-line fix is the wrong trade — the seam would then be the thing
needing its own assertions. `appendPrivateFile` is instead read against the path
it replaces: `writePrivateFileAtomic` syncs the temp file before renaming, and
the append now syncs before closing, with a failed sync failing the append.
Recording that here keeps the surrounding green suite from implying more than it
checked.

## What is owed and not produced here

- **Phase 2**: the write moves off the caller's goroutine, and with it the
  exactly-once obligation across a kill. Restart-and-drain is covered today by
  `TestSpoolResendBeforeFreshAndByteIdenticalAcrossRestart`, which passes —
  under a synchronous write, where the guarantee is inherited rather than built.
- **The four other SDKs.** A6 is fleet-wide. Unity is done; this is Go; Godot,
  Unreal and Defold still rewrite.


## ⚠ The format diverges from the Unity reference design, deliberately

`SPOOL_APPEND_ONLY_DESIGN.md` (shardpilot-unity) specifies that the reader
reconstructs the writing run's backlog by **replaying the caps the file states**
in the header, with a last-occurrence-wins rule for repeated ids. This
implementation does not do that, and the four remaining SDK ports should not
either.

**The reader was a second implementation of eviction.** Replaying the caps means
the loader must reach the same decisions the writer reached, from strictly less
information than the writer had. Every place the two copies disagreed was a
defect, and they disagreed four times, one review round apart:

| round | how the copies disagreed |
|---|---|
| 1 | de-duplicating before applying `max_bytes` erased the byte pressure an evicted copy had exerted, resurrecting a record already dead-lettered |
| 2 | a re-appended id was reconstructed at the wrong position, so a later cut removed the wrong record |
| 3 | batch members evicted on arrival were never written, so their pressure was invisible to the replay |
| 3 | the replay cost O(n²) — about 11 s to load a 100k-record spool |

Each fix taught the copy one more thing the original already knew, and none of
them made the next disagreement less likely. So the writer now **records the
decision** instead: an eviction appends a `{"drop":"<id>"}` line, and the reader
applies drops in order. It consults no caps and decides nothing, which is why
there is nothing left for it to disagree about.

The header still carries `max_events` / `max_bytes`, now purely as diagnostics —
**nothing reads them back.**

### What this costs and what it buys

One extra line per eviction, reclaimed by the same compaction that reclaims dead
records; reconstruction drops from O(n²) to O(n); and a drop survives a reader
configured differently from the writer, which cap-replay never could.

### Follow-up owed

The Unity design document is the reference the other four ports are written
against, so it needs this change before Godot, Unreal and Defold implement the
older rule. Tracked as part of A6-spool rather than done here: this repository
does not own that document, and editing another SDK's design doc from inside a
Go PR would be the wrong place to make a cross-SDK format decision visible.


## ⚠ Sharing an id is not being the same generation

Three of round 4's findings are one mistake in three places: **an event id is not
a version.** The same id can exist twice at once — a settled copy a failed
rewrite left on disk beside the live one that replaced it, an expired copy
earlier in the same batch as a fresh one — and every place that indexed *by id*
and then acted on the entry it found could act on the wrong one:

| where | what it picked | what it should have |
|---|---|---|
| the append payload | the first batch occurrence of the id | the entry actually inserted |
| the merge | the disk occurrence, on the grounds the mirror held that id | the mirror's entry, positioned by the disk's |
| round 3's settled filter | suppressed by id, ignoring generation | keyed to the live entry |

The rule that falls out, and the one the four remaining ports need: **an id
identifies which record, never which version of it.** Where a version matters,
carry the entry, not the id.
