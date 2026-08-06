# Flux session handoff

Snapshot of repo state and in-flight efforts as of 2026-08-06. Companion to
[CLAUDE.stub-blocks-plan.md](CLAUDE.stub-blocks-plan.md) (the live plan, kept current),
[CLAUDE.flux-plan.md](CLAUDE.flux-plan.md) (v1 scope) and [CLAUDE.fanout-multilink-plan.md](CLAUDE.fanout-multilink-plan.md)
(shipped).

## Repo state

- Branch `wip.0.1.0-v3`; PRs usually target `wip-0.1.0`.
- Working tree effectively clean. One uncommitted trivial doc edit in
  [FluxBlockOptions.cs](FluxBlockOptions.cs): a `<see cref="System.Threading.CancellationToken.None"/>` shortened
  to `<see cref="CancellationToken.None"/>`. Not mine, no behavior.
- **112 tests, 0 failures, stable over 10 consecutive runs.** Solution builds Release across net8/9/10/11 with 0
  warnings.

Commits this session (newest first):

| Commit | What |
|---|---|
| `f8231ce` | Add multi-producer batch benchmark |
| `1223041` | Make BufferBlock single-channel |
| `6b09b82` | (user) Change docs in preparation for behavior change |
| `5330239` | (user) Add `PooledBatch<T>.ToArray()` |
| `11ba1c8` | Optimize BatchBlock buffer growth and add steady-state batch benchmark |
| `c8bea95` | (prior session) Implement ActionBlock, BufferBlock, BatchBlock, and tests |

## What shipped this session

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

## Current benchmark picture

Ratios are Flux relative to Dataflow; **lower is better**, `<1.00` means Flux wins. AMD 7900X, net10.0 host.

| Suite | Config | Time | Alloc |
|---|---|---|---|
| `TransformBlocks/Throughput` * | DOP 1 unordered | 1.04x | 0.42x |
| `TransformBlocks/Throughput` * | DOP 4 unordered | 0.88x | 0.73x |
| `BufferBlocks/Throughput` | burst | **0.68x** | 1.96x |
| `BufferBlocks/Backpressure` | concurrent | **0.33x** | **0.18x** |
| `BatchBlocks/SteadyState` | bs=10 | 0.88x | 0.24x |
| `BatchBlocks/SteadyState` | bs=100 | 1.79x | **0.05x** |
| `BatchBlocks/SteadyState` | bs=1000 | 2.37x | **0.08x** |
| `BatchBlocks/Throughput` | burst bs=100 | 2.09x | 0.25x |
| `BatchBlocks/TimeFlush` | hybrid | 1.00x | 0.84x |
| `BatchBlocks/MultiProducer` | 1 producer | 1.95x | **0.03x** |
| `BatchBlocks/MultiProducer` | 4 producers | 2.85x | 2.46x |
| `BatchBlocks/MultiProducer` | 8 producers | **5.06x** | **5.53x** |

`*` **TransformBlocks and ActionBlocks numbers are stale** - from 2026-07-08, before this session touched the
shared base. See open item 1.

Two benchmark-shape traps that produced misleading numbers earlier, both now documented in the plan file:

- **Burst benchmarks measure deque growth, not the block.** Sending 50_000 items before draining forces whichever
  channel holds the backlog to grow an internal deque to 50_000 entries. A raw single bounded-channel hop
  allocates 9.7 KB at `BoundedCapacity = 1_000` but 179 KB at `1_000_000`, with no block involved at all. This is
  why `BufferBlocks/Throughput` allocation barely improved (2.26x -> 1.96x) despite removing a whole channel.
- **Burst benchmarks also defeat pooling.** At small batch sizes, send-all-then-drain leaves thousands of rented
  arrays live simultaneously, far more than `ArrayPool<T>.Shared` retains, so nearly every rent misses the pool.
  That made the "headline allocation benchmark" report Flux allocating *more* than Dataflow (1.35x at bs=10);
  under concurrent drain the same config is 0.24x. Prefer `SteadyState` over `Throughput` when reasoning about
  the allocation story.

## Open items, highest value first

### 1. Re-benchmark TransformBlock and ActionBlock (unblocked, do this first)

The BufferBlock work modified `TargetFluxBlockBase.TryOffer` (added a `_countAcceptedAsProcessed` branch) and
made `Fault` virtual. Both are on shared paths used by every block. Tests confirm correctness, but **no benchmark
was re-run for TransformBlock or ActionBlock afterward**, so the 0.42x/0.88x figures above predate the change.
The branch is field-first and predicted-not-taken, so a regression is not expected - but "not expected" is not
"measured".

```bash
dotnet run -c Release --project LaquaiLib.Flux.Benchmarks -- --filter "*TransformBlocks*"
```

### 2. Multi-producer send scaling (the big open finding)

`BatchBlocks/MultiProducer` shows Flux degrading far worse than Dataflow as producers increase: 1.95x -> 2.85x ->
5.06x on time, with allocation flipping from a 33x win at one producer to a 5.5x loss at eight. This lives in
`TargetFluxBlockBase`'s send path, **so it applies to every block**, and no other benchmark in the suite uses more
than one producer.

Established by probing (standalone console, `GC.GetTotalAllocatedBytes(true)` around a run):

- A bare `BoundedChannel` with 8 concurrent writers at capacity 200 already costs ~10.5 B/item and degrades 2.7x
  from 1 to 8 writers, with zero Flux code involved. Dataflow's own queue handling beats
  `System.Threading.Channels` outright in this shape. A large part of the gap is inherited from the primitive.
- **Ruled out:** the theory that `SendAsyncSlow`'s `WaitToWriteAsync` + `TryWrite` retry loop causes a thundering
  herd (all N waiters wake, one wins, N-1 re-await and re-allocate). Probed head-to-head against `WriteAsync`,
  which hands a freed slot to exactly one waiter: 14.2 ms / 514 KB versus 14.8 ms / 649 KB at 8 producers. No
  meaningful difference, `WriteAsync` marginally worse. **Do not "fix" this.**

Still open: Flux allocates ~54 B/item at 8 producers against the raw channel's ~10.5. That ~44 B delta is
Flux-side and unexplained.

**Untested hypothesis** (flagged as such deliberately - two confident cause-calls this session were both wrong):
the delta is the async state machine box `SendAsyncSlow` allocates per suspension, and under this much contention
nearly every send suspends. If so, `[AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]` on that
method should pool the boxes and most of the delta should vanish. Measure before implementing.

Then: add multi-producer variants for BufferBlock and TransformBlock to find out how wide the problem is.

### 3. BatchBlock's single-producer time gap - do NOT pursue the obvious fix

BatchBlock is ~2x Dataflow's time at one producer. The tempting fix is to accumulate inside `TryOffer` on the
caller's thread, the way Dataflow does. **The evidence now argues against it:**

- The cost is not the handoff. Probed: one raw bounded-channel hop of 50_000 items is ~3.2 ms and the whole Flux
  BatchBlock is ~3.35 ms, so accumulation and flushing are nearly free.
- The real problem is multi-producer admission (item 2), which that rewrite would not touch and would likely
  worsen: it moves accumulation under `_batchLock`, which N producers would then contend on per item, replacing a
  currently-elided uncontended lock.
- It also breaks the backpressure story. `BoundedCapacity` would silently change meaning from "queued items" to
  "queued batches", the opposite direction from the Dataflow-parity work just done.
- `TryOffer` is synchronous, returns `bool`, and is documented as a genuine zero-wait accept-or-decline;
  emitting a full batch into a bounded `_output` may need to await.

Revisit only if item 2 is resolved and a gap remains.

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
~1-2 min.

For ad-hoc perf work, a standalone console referencing the library with `GC.GetTotalAllocatedBytes(true)` around a
run iterates far faster than BDN. Note it inflates absolute numbers roughly 2x versus BDN (it measured BufferBlock
at 6.2 ms where BDN measured 2.87 ms), so read probe output as shape and ratio, not as directly comparable.
