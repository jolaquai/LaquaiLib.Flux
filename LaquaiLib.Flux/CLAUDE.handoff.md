# Flux session handoff

Snapshot of repo state and in-flight efforts as of 2026-08-06 (second session). Companion to
[CLAUDE.dataflow-parity-plan.md](CLAUDE.dataflow-parity-plan.md) (**what stands between here and a defensible
"drop-in replacement" claim - read this before pitching the library**),
[CLAUDE.stub-blocks-plan.md](CLAUDE.stub-blocks-plan.md) (block implementation, complete),
[CLAUDE.flux-plan.md](CLAUDE.flux-plan.md) (v1 scope) and [CLAUDE.fanout-multilink-plan.md](CLAUDE.fanout-multilink-plan.md)
(shipped).

Scope split: **this file owns repo state, benchmark numbers and perf open items; the parity plan owns API-surface
gaps.** Perf items appear in both, quantified here and prioritized there.

## Repo state

- Branch `wip.0.1.0-v3`; PRs usually target `wip-0.1.0`.
- Working tree clean.
- **112 tests, 0 failures** (re-verified with the pooled builder change). Solution builds Release across
  net8/9/10/11 with 0 warnings.

Commits (newest first):

| Commit | What |
|---|---|
| `fabbbdb` | Add CLAUDE.handoff.md and minor doc update |
| `f8231ce` | Add multi-producer batch benchmark |
| `1223041` | Make BufferBlock single-channel |
| `6b09b82` | (user) Change docs in preparation for behavior change |
| `5330239` | (user) Add `PooledBatch<T>.ToArray()` |
| `11ba1c8` | Optimize BatchBlock buffer growth and add steady-state batch benchmark |
| `c8bea95` | (prior session) Implement ActionBlock, BufferBlock, BatchBlock, and tests |

## What shipped in the session before this one

**BatchBlock grow-on-demand buffer** (`11ba1c8`). Was renting a `batchSize`-length array up front and again on
every flush. Now starts at `min(batchSize, 16)` and doubles up to `batchSize` only as batches actually fill
(`AppendCore`/`GrowBuffer`/`TryDetachBuffer`). Also elides `_batchLock` entirely on the per-item path when there
is no timer loop, since the reader loop is then the sole toucher of `_buffer`/`_capacity`/`_count`.
Throughput-neutral; it is an allocation fix for large-cap hybrid policies.

**BufferBlock single-channel** (`1223041`). The headline change. `PropagatorFluxBlockBase` gained an
`aliasOutputToInput` ctor flag making `_output` the same channel object as `_input`; BufferBlock has no pump loop
and no second channel. Rationale: Flux's architecture had already absorbed most of Dataflow's reasons for a
BufferBlock (every block owns a bounded input channel, fan-out is a link-level `FanOutMode` concern), leaving it
as the pipeline **ingress adapter** - and an ingress adapter that relays items between two of its own buffers is
paying a channel hop for nothing. Full reasoning and the three non-obvious consequences are in the plan file's
"Resolved: single-channel `BufferBlock`" section.

**Two new benchmark classes**: `BatchBlocks/SteadyStateBenchmarks` (concurrent drain, where pooling actually
shows) and `BatchBlocks/MultiProducerBenchmarks` (1/4/8 concurrent producers).

## What shipped this session

**Pooled async state machine boxes on the send slow path.** `TargetFluxBlockBase.SendAsyncSlow` is annotated
`[AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]`. Under multi-producer contention a large
fraction of sends suspend, and the default builder heap-allocates a state machine box per suspension. Isolated by
probe at ~62 B per suspension; the builder recovers essentially all of it. Effect on
`BatchBlocks/MultiProducer` (Flux absolutes, paired before/after runs):

| producers | alloc before | alloc after | Gen0 before -> after |
|---|---|---|---|
| 1 | 32.0 KB | **7.5 KB** (-76%) | 0 -> 0 |
| 4 | 240.0 KB | **89.8 KB** (-63%) | 7.81 -> 0 |
| 8 | 478.2 KB | **233.0 KB** (-51%) | 31.25 -> 15.63 |

Time is *not* measurably affected (better in 5 of 6 paired comparisons, all inside the noise). The type exists
since .NET 6, so all four TFMs are covered, and it is AOT-compatible. Audited every `SendAsync` call site for
double-consumption of the returned `ValueTask` before applying it - the only non-trivial one is
`PropagatorFluxBlockBase`'s `IsCompletedSuccessfully` probe then exactly one `.Result`-or-await, which is the
canonical legal pattern.

**Two new benchmark classes**: `BufferBlocks/MultiProducerBenchmarks` and
`TransformBlocks/MultiProducerBenchmarks`, mirroring the BatchBlocks one, to find out how wide the multi-producer
issue actually was (answer below: narrower than believed).

**Benchmarks csproj cleanup**: dropped a redundant `System.Threading.Tasks.Dataflow` 9.0.0 `PackageReference`
(and its `NU1510` suppression). Verified harmless first - no `System.Threading.Tasks.Dataflow.dll` was landing in
`bin/`, so the baseline always resolved to the in-box net10 assembly.

## Current benchmark picture

Ratios are Flux relative to Dataflow; **lower is better**, `<1.00` means Flux wins. AMD 7900X, net10.0 host.
All figures below are current as of this session.

| Suite | Config | Time | Alloc |
|---|---|---|---|
| `ActionBlocks/Throughput` | DOP 1 | **0.58x** | 0.36x |
| `ActionBlocks/Throughput` | DOP 4 | **0.66x** | **0.06x** |
| `ActionBlocks/Backpressure` | - | **0.54x** | 0.41x |
| `TransformBlocks/Throughput` | DOP 1 unordered | 0.84x | 0.48x |
| `TransformBlocks/Throughput` | DOP 4 unordered | 0.78x | 0.92x |
| `TransformBlocks/Throughput` | DOP 4 ordered | 0.97x | 1.80x |
| `TransformBlocks/Backpressure` | - | **0.60x** | 0.18x |
| `TransformBlocks/Pipeline` | - | **0.49x** | 0.46x |
| `TransformBlocks/SendAsync` | SendAsync | **0.50x** | 0.17x |
| `TransformBlocks/LinkedDispatch` | Broadcast-4 | **0.11x** | **0.11x** |
| `TransformBlocks/LinkedDispatch` | SingleLink | **0.48x** | 0.37x |
| `BufferBlocks/Throughput` | burst | **0.68x** | 1.96x |
| `BufferBlocks/Backpressure` | concurrent | **0.33x** | **0.18x** |
| `BatchBlocks/SteadyState` | bs=10 | 0.88x | 0.24x |
| `BatchBlocks/SteadyState` | bs=100 | 1.79x | **0.05x** |
| `BatchBlocks/SteadyState` | bs=1000 | 2.37x | **0.08x** |
| `BatchBlocks/Throughput` | burst bs=100 | 2.09x | 0.25x |
| `BatchBlocks/TimeFlush` | hybrid | 1.00x | 0.84x |
| `BatchBlocks/MultiProducer` | 1 / 4 / 8 producers | 1.01x / 1.67x / 1.93x | **0.02x** / 0.17x / 0.25x |
| `BufferBlocks/MultiProducer` | 1 / 4 / 8 producers | **0.68x** / 1.07x / 1.18x | **0.02x** / 0.22x / 0.36x |
| `TransformBlocks/MultiProducer` | 1 / 4 / 8 producers | **0.58x** / 0.90x / **0.98x** | **0.02x** / 0.33x / 0.50x |

Three benchmark-shape traps that produced misleading numbers, all now documented:

- **Burst benchmarks measure deque growth, not the block.** Sending 50_000 items before draining forces whichever
  channel holds the backlog to grow an internal deque to 50_000 entries. A raw single bounded-channel hop
  allocates 9.7 KB at `BoundedCapacity = 1_000` but 179 KB at `1_000_000`, with no block involved at all. This is
  why `BufferBlocks/Throughput` allocation barely improved (2.26x -> 1.96x) despite removing a whole channel.
- **Burst benchmarks also defeat pooling.** At small batch sizes, send-all-then-drain leaves thousands of rented
  arrays live simultaneously, far more than `ArrayPool<T>.Shared` retains, so nearly every rent misses the pool.
  That made the "headline allocation benchmark" report Flux allocating *more* than Dataflow (1.35x at bs=10);
  under concurrent drain the same config is 0.24x. Prefer `SteadyState` over `Throughput` when reasoning about
  the allocation story.
- **`MultiProducer` ratios are not comparable across BDN runs.** Its *Dataflow baseline* swung 1.225 / 1.382 /
  1.896 / 2.038 ms at one producer across four runs of byte-identical code - a 65% spread. Any before/after
  conclusion from this suite must come from paired runs, and a single alarming run means nothing. This is what
  produced the bogus numbers described in the resolved item below.

## Open items, highest value first

### 1. Nothing blocking

Both items carried into this session are closed (see "Resolved this session"). The remaining known gaps are
`BatchBlock`'s time cost (items 3 and 4 below), and neither has a fix that is clearly worth its cost.

### 2. Resolved this session

**Re-benchmark TransformBlock and ActionBlock** (was: verify the shared-base changes did not regress anything).
Ran all 32 TransformBlock/ActionBlock benchmarks. **No regression anywhere; every suite improved** over the stale
2026-07-08 figures. The old table claimed Transform Throughput DOP1 at 1.04x time; it is now 0.84x. Verification
gap closed, numbers folded into the table above.

**Multi-producer send scaling** - the premise was largely an artifact, but a real, smaller finding sits under it.

- The recorded table (1p 1.95x time / 0.03x alloc, 4p 2.85x / 2.46x, 8p 5.06x / **5.53x**) **does not reproduce**.
  Four re-runs on byte-identical library code never showed Flux allocating more than Dataflow at any producer
  count, and the time ratio plateaus instead of climbing. The cause is the baseline instability noted above. The
  "unexplained ~44 B/item" therefore never existed; the real Flux-vs-raw-channel delta was ~5.3 B/item.
- The state-machine-box hypothesis was **correct in mechanism but not in magnitude**: ~62 B per suspension, with
  only ~5-6% of sends suspending at 8 producers, so ~3.2 B/item rather than 44. Shipped anyway - it is a free
  50-76% allocation cut (table above).
- **"It affects every block" is half right.** By ratio there is no crisis: Flux wins at one producer on all three
  blocks and is at parity at eight on TransformBlock. But the *slope* is genuinely worse. Flux 1p -> 8p degrades
  3.5x (Batch) / 4.4x (Buffer) / 3.1x (Transform) in absolute time where Dataflow degrades 1.9x / 2.5x / 1.8x -
  consistently about twice as steep. That matches the earlier probe finding that a bare `BoundedChannel` with 8
  concurrent writers already degrades 2.7x with **zero Flux code involved**. The slope is inherited from
  `System.Threading.Channels`, not from anything in `TargetFluxBlockBase`; Flux simply starts far enough ahead on
  Transform/Buffer to stay at parity anyway. Not addressable without replacing the primitive - a much larger call
  that should not be made off this evidence alone.
- **Ruled out** (do not re-derive): the thundering-herd theory about `SendAsyncSlow`'s `WaitToWriteAsync` +
  `TryWrite` retry loop. Probed head-to-head against `WriteAsync`, which hands a freed slot to exactly one waiter:
  14.2 ms / 514 KB versus 14.8 ms / 649 KB at 8 producers. No meaningful difference, `WriteAsync` marginally
  worse.

### 3. BatchBlock's single-producer time gap - do NOT pursue the obvious fix

BatchBlock is ~2x Dataflow's time in the burst and large-batch steady-state shapes (at one producer under
concurrent drain it is now at parity, 1.01x). The tempting fix is to accumulate inside `TryOffer` on the caller's
thread, the way Dataflow does. **The evidence argues against it:**

- The cost is not the handoff. Probed: one raw bounded-channel hop of 50_000 items is ~3.2 ms and the whole Flux
  BatchBlock is ~3.35 ms, so accumulation and flushing are nearly free.
- It would worsen multi-producer behavior, the one place BatchBlock still trails (1.67x/1.93x at 4/8 producers):
  it moves accumulation under `_batchLock`, which N producers would then contend on per item, replacing a
  currently-elided uncontended lock.
- It also breaks the backpressure story. `BoundedCapacity` would silently change meaning from "queued items" to
  "queued batches", the opposite direction from the Dataflow-parity work already done.
- `TryOffer` is synchronous, returns `bool`, and is documented as a genuine zero-wait accept-or-decline;
  emitting a full batch into a bounded `_output` may need to await.

### 4. BatchBlock is the weakest block on multi-producer time (small, unexplained)

At 4/8 producers BatchBlock sits at 1.67x/1.93x where BufferBlock is 1.07x/1.18x and TransformBlock is
0.90x/0.98x - and this is *after* accounting for the shared `System.Threading.Channels` scaling slope, which all
three pay equally. Something BatchBlock-specific costs extra under concurrent producers. `_writeGate`
(`SemaphoreSlim`) serializing flush writes is the obvious suspect, but this has **not** been probed. Low priority:
the allocation story is 0.02x-0.25x and the absolute times are small.

## Invariants that must not be broken

- **`BatchBlock._hasTimer` must be assigned before `_readerLoop` is queued.** The reader loop reads it per item
  to decide whether `_batchLock` is needed. Testing `_timerLoop is not null` instead would be read by a loop
  started before that field's assignment, with no happens-before edge making it visible - a real data race on
  ARM64 even though x64's memory model would hide it.
- **`aliasOutputToInput` is BufferBlock-only.** Any block that transforms, batches, or otherwise decouples input
  rate from output rate needs two channels to apply backpressure independently. It is only legal at all because
  `TIn == TOut` and the block does no work between in and out.
- **`BufferBlock.FinalizeAsync` is the block's only cancellation observer.** Every other block learns its token
  fired by having a loop awaiting `WaitToReadAsync(_loopToken)`; BufferBlock has no such loop. If that await is
  ever removed or its `WaitAsync(_loopToken)` wrapper dropped, `Completion` **hangs** instead of resolving
  `Canceled` - it will not fail loudly. Covered by
  `CancellationToken_CanceledAfterItemsBuffered_CompletionResolvesCanceled`.
- **`BufferBlock.Fault` must discard the buffer.** A bounded channel withholds its reader-side completion signal
  until the queue empties *even when completed with an error*, so without the discard a faulted block sits
  pending. This is also what `IFluxTarget<T>.Fault`'s own XML doc promises and what Dataflow does.
- **`_loopToken` is `CancellationToken.None` whenever the options token can never fire.** This is deliberate (it
  lets channels reuse pooled async operations instead of allocating per suspension). Anything awaiting on it must
  keep the `CanBeCanceled` branch rather than unconditionally wrapping.
- **`BoundedCapacity` sizes both `_input` and `_output`** for non-aliased blocks. A test or benchmark with a small
  capacity that sends more than a couple of items without draining output concurrently **deadlocks**. This bit a
  probe during this session and is a recurring trap.
- **Dataflow's `BatchBlock` rejects `BoundedCapacity < batchSize`**, so any comparison benchmark must scale
  capacity with batch size rather than using a constant.
- **`PooledBatch<T>` must be disposed exactly once**, and broadcasting one to multiple targets is unsafe by
  design (same rented array, N would-be disposers). `ToArray()` is the escape hatch.
- **The `ValueTask` returned by `SendAsync` may be consumed exactly once.** This was always the `ValueTask`
  contract, but `SendAsyncSlow`'s pooling builder makes violations *actively* corrupting rather than merely
  undefined, because the backing object is recycled. The one legal multi-step pattern in the codebase is
  `PropagatorFluxBlockBase`'s: store the `ValueTask`, probe `IsCompletedSuccessfully`, then take `.Result` **or**
  await it, never both, never twice.

## Running things

Tests - `dotnet test` intermittently reports "Zero tests ran" under MTP; run the exe directly:

```bash
dotnet build LaquaiLib.Flux.Tests -c Release && ./LaquaiLib.Flux.Tests/bin/Release/net11.0/LaquaiLib.Flux.Tests.exe
```

The test exe links its **own** copy of the library DLL, so the Tests project must be rebuilt after any library
change or a clean-looking pass is stale.

Benchmarks (targets net10.0; BenchmarkDotNet 0.15.8 cannot launch net11 preview):

```bash
dotnet run -c Release --project LaquaiLib.Flux.Benchmarks -- --filter "*BatchBlocks*"
```

Reports land in `BenchmarkDotNet.Artifacts/results/` **relative to the shell's cwd**, not the project - so they
scatter depending on where the command was run from. The directory is gitignored. Full suite ~5-6 min, one class
~1-2 min; `*MultiProducer*` (3 classes, 18 benchmarks) is ~8 min.

**Never conclude anything from a single `MultiProducer` run.** Its Dataflow baseline is unstable enough across
runs (65% spread at one producer on identical code) to invent regressions and wins that are not there. Compare
before/after only from paired runs, and prefer absolute Flux numbers over ratios when the baseline moved.

For ad-hoc perf work, a standalone console referencing the library with `GC.GetTotalAllocatedBytes(true)` around a
run iterates far faster than BDN. Note it inflates absolute numbers roughly 2x versus BDN (it measured BufferBlock
at 6.2 ms where BDN measured 2.87 ms), so read probe output as shape and ratio, not as directly comparable. At 4+
producers the probe's *time* numbers are dominated by thread-pool injection and are worthless (it measured a raw
channel at 14.5 ms where BDN measured a whole Dataflow block at 4.1 ms); only its allocation numbers transfer.
