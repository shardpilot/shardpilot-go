# Spool Append-Only — Acceptance (Go SDK, Phase 1)

Phase 1 is the **format change alone**, as
`SPOOL_APPEND_ONLY_DESIGN.md` (shipped with the Unity SDK, the reference
implementation) prescribes: it removes the O(spool-size) growth while the write
stays synchronous, so durability is exactly as strong as before and the
exactly-once hole asynchrony creates does not exist yet. Phase 2 — the worker —
opens that hole, so it is not opened until the thing that closes it is built.

## The bound, evaluated

`SPOOL_OVERFLOW_LATENCY_BOUND.md` fixed these before the implementation existed.

| | baseline | after | bound | |
|---|---|---|---|---|
| p95 full ÷ p95 empty | **6.7x** | **0.9x** | ≤ 2x | ✅ |
| p99 at a full spool | **22.6 ms** | **0.063 ms** | ≤ 1.0 ms | ✅ |
| total, 2000 appends | 18 602.8 ms | 84.6 ms | — | 220x |
| single append at a full spool | 19.6 ms | 0.017 ms | — | |

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
| `TestRaisedCapCannotResurrectEvictedRecords` | the file states the caps it was written under |
| `TestLastOccurrenceWins` | the dedup rule, which is wrong in both directions if reversed |
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
| first-occurrence dedup | `TestLastOccurrenceWins` fails |
| caps omitted from the header | `TestRaisedCapCannotResurrectEvictedRecords` fails |

⚠ **Two of these mutations survived the first time, and both tests were wrong
rather than the code.** `TestAppendsAreAppends` asserted only that the previous
bytes were a **prefix** of the new ones — which a full rewrite also satisfies,
because rewriting an append-ordered list emits the same bytes and then one more.
It now checks the **inode**, which is what actually separates the two: the
rewrite path is temp-file-plus-rename. And boundedness passed with compaction
disabled because eviction was still rewriting, which kept the file small for the
wrong reason — the same defect the bound caught, showing up as a test that could
not fail.

## What is owed and not produced here

- **Phase 2**: the write moves off the caller's goroutine, and with it the
  exactly-once obligation across a kill. Restart-and-drain is covered today by
  `TestSpoolResendBeforeFreshAndByteIdenticalAcrossRestart`, which passes —
  under a synchronous write, where the guarantee is inherited rather than built.
- **The four other SDKs.** A6 is fleet-wide. Unity is done; this is Go; Godot,
  Unreal and Defold still rewrite.
