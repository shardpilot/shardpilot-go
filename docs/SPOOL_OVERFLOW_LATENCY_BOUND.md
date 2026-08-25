# Spool Overflow Latency Bound — Go SDK

**Fixed 2026-08-25, before any of the append-only implementation was written.**

A6 requires this in those words, and the reason is worth restating because it
is a trap rather than a formality: the cheapest way to pass a durability
requirement is to move the I/O to a worker and then block the caller until it
has persisted. That satisfies "no calling-thread file I/O" — the I/O genuinely
happens elsewhere — and it satisfies a restart capture, while the cost this
item exists to remove is preserved exactly, now as a caller-side wait. A
threshold chosen after seeing the fixed run is chosen to be met.

So the numbers below are the baseline as it stands today, and the bound derived
from it, both recorded before the fix exists.

## Baseline — the defect, measured

`diskSpool.append` calls `saveLocked`, which **re-reads the whole spool file,
merges it with the in-memory mirror, re-marshals every record, and rewrites the
file** via temp-and-rename — on the calling goroutine, on every append. Go's
spool is therefore O(2 × spool size) per appended event, where Unity's was
O(spool size): it pays for a read as well as a write.

2000 successive single-event overflow appends, each one event, growing the
spool to its 2000-entry / 1 MiB caps. Harness: the package's own test binary
against real files in `t.TempDir()`, Go 1.27, x86-64 Linux, warm page cache.

```
append #    1:    7.603 ms   spool file      254 bytes
append #  100:    1.585 ms   spool file    23024 bytes
append #  250:    2.454 ms   spool file    57524 bytes
append #  500:    4.401 ms   spool file   115024 bytes
append # 1000:    7.801 ms   spool file   230024 bytes
append # 1500:   11.990 ms   spool file   345024 bytes
append # 1999:   19.635 ms   spool file   459794 bytes

n=2000  min 0.678  p50 8.468  p95 18.527  p99 22.621  max 272.621 ms
mean 9.301 ms   total 18602.8 ms across 2000 overflow appends
p95 empty(first 100) 3.395 ms   p95 full(last 100) 22.663 ms   ratio 6.7x
```

Three facts decide the bound.

**The cost grows with the backlog — 12x from 100 entries to 1999.** 1.585 ms at
100, 19.635 ms at 1999. That is the read-merge-rewrite showing up exactly as the
shape predicts, and it is the property "appends are appends" removes.

**18.6 seconds of caller time** to spool 2000 events.

**A single append at a full spool costs 19.6 ms.** The Go SDK is a server-side
client, so there is no frame budget to blow — but there is a request handler,
and `Track` runs on it. 19.6 ms is longer than most of the requests a game
backend serves, spent inside one that merely emitted an event.

## The bound

Three parts. The first is the load-bearing one.

### 1. Constant in the backlog (machine-independent)

> p95 of the overflow append at a FULL spool (>= 1900 entries) must be within
> **2x** of p95 at an EMPTY spool (<= 10 entries).

Today that ratio is **6.7x**. This is the structural claim — an append costs
what one record costs, not what the backlog costs — and it is the part that
does not depend on the machine the measurement runs on. A ratio is
reproducible where a millisecond figure is not.

### 2. Absolute ceiling at a full spool (this harness)

> p99 of the overflow append at a full spool must be **at or under 1.0 ms** on
> the harness above.

Today p99 is 22.6 ms. The figure is harness-specific and stated as such; it
exists so that a change satisfying the ratio by making the empty case *slower*
is not accepted.

### 3. No caller-side wait bought back in Phase 2

> When the write moves off the calling goroutine, the bound is measured on the
> same `append` call the caller makes — not on the worker's write.

This is what stops durability being restored by blocking the caller. It is
recorded here, with the other two, before the worker exists.

## Why the baseline test is not committed

The profile above was produced by a throwaway test in this package. It is not
committed, because a measurement harness that runs in CI becomes a flaky test
the first time a runner is loaded, and the number it guards is machine-specific
(part 2). The reproduction is in this document instead: 2000 single-event
appends through `diskSpool.append` against `t.TempDir()`, timing each call.

What IS committed alongside the implementation are the file-level acceptance
tests — appends are appends, the spool stays bounded, kill/restart/drain
persists exactly once — which are assertions about behaviour rather than about
wall-clock, and are the ones that must not be flaky.
