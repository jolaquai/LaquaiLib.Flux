# Flux stub blocks - implementation plan

Fills in the three `throw new NotImplementedException(...)` stubs so a Flux pipeline can actually terminate and
buffer/batch, not just transform: [ActionBlock.cs](ActionBlock.cs:33), [BufferBlock.cs](BufferBlock.cs:18),
[BatchBlock.cs](BatchBlock.cs:33). Companion to [CLAUDE.flux-plan.md](CLAUDE.flux-plan.md) (v1 scope) and
[CLAUDE.fanout-multilink-plan.md](CLAUDE.fanout-multilink-plan.md) (already shipped).

Every block gets a dedicated xUnit.v3 test class and a full BenchmarkDotNet suite against its direct
`System.Threading.Tasks.Dataflow` equivalent (all three have one: `ActionBlock<T>`, `BufferBlock<T>`,
`BatchBlock<T>`).

## Decisions already locked

- **`BatchBlock<T>` emits a pooled ownership type**, not `T[]`. Output type changes from the stub's
  `PropagatorFluxBlockBase<T, T[]>` to `PropagatorFluxBlockBase<T, PooledBatch<T>>`. This is what delivers the
  plan's "no per-batch heap array" pitch; the cost is a dispose-to-return contract on consumers (see risks).
- **`BufferBlock<T>` uses a pump loop** across the base's existing two-channel (`_input` -> `_output`) layout,
  not a bespoke single-channel rewrite. Keeps it consistent with every other block for metrics/completion/fault
  and links; the single-channel optimization is noted as out-of-scope future work.
- **`ActionBlock<TIn>` is a pure sink** on `TargetFluxBlockBase<TIn>` (no `_output`, no links), mirroring
  `TransformBlock`'s unordered worker/finalize pattern minus the output write.

## Shared architecture recap (what the concrete block owns)

From [TargetFluxBlockBase.cs](Primitives/TargetFluxBlockBase.cs) and
[PropagatorFluxBlockBase.cs](Primitives/PropagatorFluxBlockBase.cs), already true and unchanged:

- Base gives every block `_input` (bounded channel), `_options`, `_loopToken` (the alloc-free None-token trick),
  `_tags`, `TrySetFault`/`ObservedFault`, `ResolveCompletion(fault)`, and `Complete`/`Fault` on the input writer.
  `TryOffer`/`SendAsync` and the `ItemsAccepted` metric are handled by the base; concrete blocks never touch them.
- A concrete block owns: its worker/pump/accumulation loop(s) reading `_input`; producing output (propagators
  only) into `_output`; and resolving its own `Completion` via `ResolveCompletion` after draining, plus completing
  `_output`'s writer (propagators only). The propagator base's `DispatchLoopAsync` then propagates output ->
  links and completion/fault -> links with no further work from the concrete block.
- `ComputeSingleReader`/`ComputeInputSingleReader` = `MaxDegreeOfParallelism == 1`. `outputSingleWriter` = whether
  only one loop ever writes `_output` (non-concurrently).

---

## 1. ActionBlock&lt;TIn&gt;

Pure sink: N workers pull from `_input`, invoke the action, produce nothing. Simplest of the three.

### Fields / ctor

- `private readonly Func<TIn, ValueTask> _action;`
- `private readonly int _maxDegreeOfParallelism;`
- `private readonly Task[] _workers;`
- Async ctor (the one that currently throws): null-check action, assign `_action`, compute DOP via a local
  `Math.Clamp(options.MaxDegreeOfParallelism, 1, ...)` helper (mirror `TransformBlock.GetMaxDegreeOfParallelism`),
  allocate `_workers`, start `Task.Run(WorkerLoopAsync)` per slot, then `_ = FinalizeAsync();`. The sync ctor
  already delegates through `WrapSync` and stays as-is.
- `ComputeSingleReader` already present and correct (single input reader iff DOP 1).

### Loops

```
private async Task WorkerLoopAsync()
{
    var reader = _input.Reader;
    try
    {
        while (await reader.WaitToReadAsync(_loopToken).ConfigureAwait(false))
            while (reader.TryRead(out var item))
            {
                await ActionMeasuredAsync(item).ConfigureAwait(false);   // StageLatency-gated, see below
                if (FluxMetrics.ItemsProcessed.Enabled) FluxMetrics.ItemsProcessed.Add(1, _tags);
            }
    }
    catch (Exception ex) { TrySetFault(ex); _input.Writer.TryComplete(ex); }   // wake+unwind peers
}

private async Task FinalizeAsync()
{
    await Task.WhenAll(_workers).ConfigureAwait(false);
    ResolveCompletion(ObservedFault);   // RanToCompletion / Faulted / Canceled per base contract
}
```

- `ActionMeasuredAsync` mirrors [TransformBlock.TransformMeasuredAsync](TransformBlock.cs:110): if
  `FluxMetrics.StageLatency.Enabled`, time the invocation; else call `_action(item)` directly (no `Stopwatch`).
- No `_output`, no `ResolveCompletion` ordering concerns, no links. `Completion` resolves once every worker has
  drained `_input`. Fault-on-throw and explicit `Fault` both go through the base's fault field, so first-exception
  -wins and the "clean-complete-then-late-throw" race are already handled by the base.

### Notes

- DOP > 1 is unordered by nature (side effects, no output to reorder) - matches Dataflow's `ActionBlock`, which
  also does not order side effects.

---

## 2. BufferBlock&lt;T&gt;

Order-preserving passthrough. Single pump loop moves `_input` -> `_output` unchanged; the base pushes `_output`
to links / `ReceiveAllAsync`.

### Ctor

- Stub already calls `base(options, inputSingleReader: true, outputSingleWriter: true)` - both correct: exactly
  one pump reads input and exactly one pump writes output. Remove the throw, start `_ = PumpLoopAsync();` and
  `_ = FinalizeAsync();` (or fold finalize into the pump's tail; keep them split for parity with the others).
- `MaxDegreeOfParallelism` is deliberately ignored (buffering is a single ordered queue). Document on the type.

### Loop

```
private async Task PumpLoopAsync()
{
    var reader = _input.Reader; var writer = _output.Writer;
    try
    {
        while (await reader.WaitToReadAsync(_loopToken).ConfigureAwait(false))
            while (reader.TryRead(out var item))
            {
                if (!writer.TryWrite(item)) await writer.WriteAsync(item, _loopToken).ConfigureAwait(false);
                if (FluxMetrics.ItemsProcessed.Enabled) FluxMetrics.ItemsProcessed.Add(1, _tags);
            }
    }
    catch (Exception ex) { TrySetFault(ex); _input.Writer.TryComplete(ex); }
}

private async Task FinalizeAsync()
{
    await _pumpTask.ConfigureAwait(false);
    var fault = ObservedFault;
    _output.Writer.TryComplete(fault);
    ResolveCompletion(fault);
}
```

- Identical in spirit to `TransformBlock`'s unordered path with an identity transform at DOP 1, but without the
  transform delegate indirection. `ItemsProcessed` counts forwarded items (keeps the "every stage reports"
  observability contract intact; a pure buffer still shows throughput/queue-depth).

### Out of scope (noted, not built)

- A single-channel `BufferBlock` (writers hit `_output` directly, no pump, tie `Completion` to
  `_output.Reader.Completion`) would shave the second buffer, but the base unconditionally allocates `_input`, so
  a real single-channel buffer needs a new base type. Only pursue if the benchmark shows the pump lagging
  Dataflow (unlikely given ValueTask vs Task-per-send).
  **Status: the benchmark has since shown exactly that - see "Open decision: single-channel `BufferBlock`" below.**

---

## 3. BatchBlock&lt;T&gt;

Groups items into pooled batches, flushed by count OR by a `PeriodicTimer` interval OR on input completion.
Output type becomes `PooledBatch<T>`.

### New public type: PooledBatch&lt;T&gt;

`readonly struct PooledBatch<T> : IDisposable` (new file `PooledBatch.cs`):

- Fields: `private readonly T[] _array; private readonly int _count;` (ctor `internal`).
- `public int Count => _count;`
- `public Memory<T> Memory => new(_array, 0, _count);` - the **primary handout**, correctly sized to `_count`.
- `public Span<T> Span => new(_array, 0, _count);` - zero-cost view for a synchronous consuming scope.
- `public T this[int index]` with a bounds check against `_count` (NOT `_array.Length`).
- `public void Dispose() => ArrayPool<T>.Shared.Return(_array, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());`
- **Why the slice-to-`_count` is load-bearing, not just for partial batches:** `ArrayPool<T>.Shared.Rent(batchSize)`
  returns an array of length `>= batchSize` (bucket-rounded, typically the next power of two), so `_array.Length`
  exceeds `_count` even for a *full* batch. The consumer must never see the slack; every accessor slices to
  `_count`.
- **Why `Memory<T>` is primary over `Span<T>`:** batches are routinely consumed in async contexts (async
  `ActionBlock`, a `LinkTo` to an async block, `await foreach`). `Span<T>` is a ref struct and cannot survive an
  `await`, so a Span-only surface would force fully-synchronous per-batch processing. `Memory<T>` crosses await
  boundaries, carries the correct length intrinsically (`.Length == Count`), and yields `.Span` on demand. `Span`
  stays as a sync-scope convenience.
- **Mutable, not `ReadOnly`:** each batch is a distinct rented array handed to exactly one owner (except Broadcast
  fan-out, see risks), so in-place mutation/processing is safe and the extra capability is free. Trade-off
  accepted: `Memory<T>` -> `ReadOnlyMemory<T>` later would be breaking, whereas the reverse is additive; fine
  pre-1.0.
- XML doc states the contract loudly: dispose exactly once, do not touch `Memory`/`Span`/indexer after dispose (a
  retained `Memory<T>` outlives the return-to-pool as a dangling view, same hazard as the span, wider window -
  `.ToArray()` is the escape hatch to keep data past dispose). `clearArray` is gated so a `PooledBatch<int>` (the
  benchmark case) skips the wipe while a `PooledBatch<SomeRef>` does not root the referenced objects.
- Rationale for a struct over `IMemoryOwner<T>`/`MemoryPool<T>`: `MemoryPool<T>.Shared.Rent` allocates a small
  owner object per rent, reintroducing a per-batch heap allocation. The struct is the only truly zero-per-batch
  option. The double-dispose footgun is the accepted cost of the locked decision (see risks).

### Fields / ctor

- `private readonly int _batchSize;`
- `private readonly TimeSpan _flushInterval;` (`Timeout.InfiniteTimeSpan` => count-only)
- `private T[] _buffer;` (rented, current in-progress batch), `private int _count;`
- `private readonly Lock _batchLock;` (guards `_buffer`/`_count` add + detach; fast, no await under it)
- `private readonly SemaphoreSlim _writeGate;` (serializes flush writes so batch ORDER is preserved across the
  reader-loop flush and the timer-loop flush; because it guarantees writes are never concurrent, `outputSingleWriter:
  true` stays valid)
- `private PeriodicTimer _timer;` and `private Task _timerLoop;` (only when `_flushInterval` is finite)
- Ctor: keep `ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1)`, remove the throw. Rent the first
  `_buffer`, start `ReaderLoopAsync`; if `_flushInterval` finite, create the `PeriodicTimer` and start
  `TimerLoopAsync`; start `FinalizeAsync`. Base flags stay `inputSingleReader: true, outputSingleWriter: true`.

### Flush (the ordering-critical core)

```
private async ValueTask FlushAsync()
{
    await _writeGate.WaitAsync(_loopToken).ConfigureAwait(false);   // acquire BEFORE detach => detach order == write order
    try
    {
        T[] buf; int n;
        lock (_batchLock)
        {
            if (_count == 0) return;              // nothing to flush (peer already took it)
            buf = _buffer; n = _count;
            _buffer = ArrayPool<T>.Shared.Rent(_batchSize);   // swap in a fresh buffer for the next batch
            _count = 0;
        }
        var batch = new PooledBatch<T>(buf, n);
        if (!_output.Writer.TryWrite(batch)) await _output.Writer.WriteAsync(batch, _loopToken).ConfigureAwait(false);
        if (FluxMetrics.ItemsProcessed.Enabled) FluxMetrics.ItemsProcessed.Add(n, _tags);
    }
    finally { _writeGate.Release(); }
}
```

- Add path (reader loop): under `_batchLock`, `_buffer[_count++] = item; full = _count == _batchSize;` then, if
  full, `await FlushAsync()` (outside the lock). If the timer wins the gate first and detaches the full batch, the
  reader's subsequent `FlushAsync` sees `_count == 0` and no-ops - single flush, no reorder, no loss.
- Backpressure: a full `_output` makes `WriteAsync` await while holding `_writeGate`, which stalls both the next
  count-flush and any timer-flush, which stalls input. Correct.

### Loops / lifecycle

```
private async Task ReaderLoopAsync()
{
    var reader = _input.Reader;
    try
    {
        while (await reader.WaitToReadAsync(_loopToken).ConfigureAwait(false))
            while (reader.TryRead(out var item))
            {
                bool full; lock (_batchLock) { _buffer[_count++] = item; full = _count == _batchSize; }
                if (full) await FlushAsync().ConfigureAwait(false);
            }
    }
    catch (Exception ex) { TrySetFault(ex); _input.Writer.TryComplete(ex); }
}

private async Task TimerLoopAsync()
{
    try { while (await _timer.WaitForNextTickAsync(_loopToken).ConfigureAwait(false)) await FlushAsync().ConfigureAwait(false); }
    catch (OperationCanceledException) { }   // disposal/fault cancels; not an error
}

private async Task FinalizeAsync()
{
    await _readerLoop.ConfigureAwait(false);        // input drained (or faulted)
    _timer?.Dispose();                              // stop the timer loop
    if (_timerLoop is not null) await _timerLoop.ConfigureAwait(false);
    var fault = ObservedFault;
    if (fault is null) await FlushAsync().ConfigureAwait(false);   // emit the final partial batch
    else ReturnBufferOnFault();                     // return the un-emitted rented buffer to the pool
    _output.Writer.TryComplete(fault);
    ResolveCompletion(fault);
}
```

- Timer disposed before the final flush so there is no concurrent timer-flush racing the finalizer's flush.
- `ReturnBufferOnFault`: on fault, return the current in-progress `_buffer` to the pool (under `_batchLock`, guard
  against a concurrent already-detached state). Batches already sitting in `_output` when it faults are dropped by
  the channel; their arrays simply become GC garbage (a lost-pooling event, not a memory leak) - acceptable and
  documented.
- `MaxDegreeOfParallelism` ignored (accumulation is inherently serial).

---

## Tests (xUnit.v3, one class per block, sibling to TransformBlockTests / DiagnosticsTests)

Reuse the existing `RecordingTarget<T>`/`GateableTarget<T>` patterns where a target is needed. New helpers as
needed (e.g. a concurrent side-effect sink for ActionBlock).

### ActionBlockTests
- Ctor: null sync action throws; null async action throws; default options name contains "ActionBlock"; named
  options honored.
- Sync action runs for every item; async action runs for every item (assert collected side effects).
- DOP 1 preserves side-effect order; DOP > 1 processes all items (order not asserted, count/set asserted).
- `SendAsync` returns false after `Complete`.
- Backpressure: `BoundedCapacity` small + a gated action => a send past capacity genuinely suspends (mirror the
  TransformBlock backpressure test; ActionBlock has no output channel, so no output-drain deadlock risk).
- `Completion` resolves RanToCompletion only after all queued items processed (gated action).
- Fault: action throws => `Completion` faulted with same exception; explicit `Fault` faulted; null-exception
  throws; fault idempotent (first wins).
- Cancellation via options token => `Completion` Canceled.

### BufferBlockTests
- Ctor default/named.
- Passthrough preserves order (pull via `ReceiveAllAsync`).
- `LinkTo` pushes buffered items to a target; multi-item, in order.
- Completion propagation to linked target (PropagateCompletion true and false); fault propagation regardless of
  the flag (mirrors TransformBlock's linked-completion tests).
- Backpressure with small capacity + concurrent produce/consume (no deadlock).
- Cancellation => Canceled. `SendAsync` false after `Complete`.

### BatchBlockTests
- Ctor `batchSize < 1` throws; null options ok.
- Emits full batches of `batchSize`; batch contents correct and in order (assert via `Memory.ToArray()`/
  `Span.ToArray()`), disposing each received batch.
- Exact multiple: `N * batchSize` items => exactly N batches, no trailing empty batch.
- Final partial batch emitted on `Complete` (e.g. 5 items, size 2 => [2],[2],[1]).
- Empty input + `Complete` => zero batches, `Completion` resolves RanToCompletion.
- Time flush: finite `flushInterval`, feed fewer than `batchSize` items, assert a partial batch surfaces before
  any count trigger (generous interval, poll with the existing `WaitUntilAsync` helper to stay non-flaky); assert
  `Timeout.InfiniteTimeSpan` never time-flushes (partial stays buffered until Complete).
- `LinkTo` to a `PooledBatch<T>` target receives batches; completion/fault propagation.
- Backpressure: small output capacity, batches back up, no deadlock (concurrent drain).
- Cancellation => Canceled; on fault, no hang.
- `PooledBatch<T>`: `Count`/`Memory`/`Span`/indexer correct and all sliced to `Count` (assert `Memory.Length ==
  Count` even when the block internally over-rented); indexer out-of-range (>= `Count`) throws; single `Dispose`
  does not throw.
  (Pooling/return-to-pool is asserted in the benchmark via MemoryDiagnoser, not a unit test - ArrayPool identity
  is not observable enough for a stable unit assertion.)

Run convention unchanged: build `LaquaiLib.Flux.Tests -c Release`, run the exe directly (MTP "zero tests" quirk),
repeat-run the timing-sensitive ones for flakiness. See [[running-tests-and-benchmarks]] and
[[flux-diagnostics-and-testing-gotchas]] (esp. the shared input/output capacity deadlock note - relevant to the
BufferBlock/BatchBlock backpressure tests, which MUST drain output concurrently).

## Benchmarks (BenchmarkDotNet, vs direct Dataflow equivalents, Dataflow = Baseline)

New folders mirroring `TransformBlocks/`, each `[Config(typeof(BenchmarkConfig))] [MemoryDiagnoser]`, same
50_000-item / large-capacity convention, "build fresh pipeline per invocation, send all, Complete, await full
drain/Completion":

- **ActionBlocks/ThroughputBenchmarks.cs** - `[Params(1, 4)] DegreeOfParallelism`; sync action (trivial counter
  sink); Flux `ActionBlock<int>` vs Dataflow `ActionBlock<int>`. Measure = send all + Complete + await Completion.
- **ActionBlocks/BackpressureBenchmarks.cs** - small capacity, gated/slow action, concurrent produce; Flux vs
  Dataflow.
- **BufferBlocks/ThroughputBenchmarks.cs** - passthrough send + drain output; Flux `BufferBlock<int>` vs Dataflow
  `BufferBlock<int>`.
- **BufferBlocks/BackpressureBenchmarks.cs** - small capacity, concurrent produce/consume (mirror the existing
  TransformBlocks/BackpressureBenchmarks structure).
- **BatchBlocks/ThroughputBenchmarks.cs** - `[Params(10, 100, 1000)] BatchSize`, count-only flush, large capacity;
  Flux `BatchBlock<int>` vs Dataflow `BatchBlock<int>`. Headline allocation benchmark: the Flux drain loop MUST
  `Dispose()` each `PooledBatch` (returns arrays to the pool) so MemoryDiagnoser shows Flux's near-zero
  per-batch allocation against Dataflow's one `int[]` per batch. Forgetting the dispose in the drain loop would
  silently erase the win - call it out in the benchmark comment.
- **BatchBlocks/TimeFlushBenchmarks.cs** - the built-in count+time hybrid has no direct Dataflow analogue
  (Dataflow needs a manual `Timer` + `TriggerBatch`). Provide a best-effort Dataflow harness
  (`BatchBlock` + `System.Threading.Timer` calling `TriggerBatch`) as the baseline, or mark Flux-only if the
  harness proves too noisy to be a fair comparison. Decide during implementation from the first run's variance.

## Risks / call-outs

- **`PooledBatch<T>` double-dispose is undefined and corrupts the pool** (same array returned twice -> two future
  rents alias). Mitigation is contract + loud XML doc, not a runtime guard (a guard needs a heap owner object,
  which reintroduces the per-batch allocation this whole choice exists to avoid). Consumers on `LinkTo` chains
  must dispose in exactly one place. This is the accepted cost of the locked "pooled ownership" decision.
- **Broadcast fan-out of a `BatchBlock` is the one unsafe routing mode** and must be documented as such:
  `BroadcastAsync` hands the same `PooledBatch<T>` value (same underlying array) to every linked target, so N
  targets means N would-be disposers (double-dispose corruption) or none (leak), plus - now that `Memory<T>` is
  mutable - a shared mutable view across targets. Single-owner modes (single link, `FirstAvailable`, `RoundRobin`,
  or plain `ReceiveAllAsync`) hand each batch to exactly one owner and are the safe/supported path. Do NOT build
  refcounting for `PooledBatch` to make broadcast safe (allocation + complexity for a discouraged pattern); a
  `BatchBlock` XML-doc remark steering broadcast users to `.ToArray()`-then-own, or away from broadcasting pooled
  batches at all, is the resolution.
- **Fault drops un-consumed pooled batches** as GC garbage (lost pooling, not a leak). Fine.
- **BatchBlock `outputSingleWriter: true` validity rests entirely on `_writeGate`** serializing all writes. If a
  future change adds a second unsynchronized writer to `_output`, flip it to `false`. Note this in code.
- **BufferBlock double-buffering** may trail Dataflow on raw passthrough latency; expected to still win on
  allocation. The benchmark decides whether the single-channel refactor is worth pursuing.

## Sequencing

1. `ActionBlock` (simplest, unblocks "pipeline can terminate") + tests + benchmarks; run + verify.
2. `BufferBlock` + tests + benchmarks; run + verify.
3. `PooledBatch<T>` then `BatchBlock` + tests + benchmarks; run + verify (batch alloc win is the marquee result).
4. Full-solution Release build across all TFMs; full test-suite repeat-runs; full benchmark run; analyze.
```

## Implementation checklist

- [x] `ActionBlock<TIn>`: async ctor body, `WorkerLoopAsync`, `ActionMeasuredAsync`, `FinalizeAsync`, DOP helper
- [x] `BufferBlock<T>`: ctor body, `PumpLoopAsync`, `FinalizeAsync`; document DOP-ignored
- [x] `PooledBatch<T>` struct (new file) + XML-doc contract
- [x] `BatchBlock<T>`: retype base to `<T, PooledBatch<T>>`, ctor body, `ReaderLoopAsync`, `TimerLoopAsync`,
      `FlushAsync`, `FinalizeAsync`, `ReturnBufferOnFault`
- [x] `ActionBlockTests`, `BufferBlockTests`, `BatchBlockTests` (101 tests total, stable over 5 repeat runs)
- [x] `ActionBlocks/`, `BufferBlocks/`, `BatchBlocks/` benchmark suites
- [x] Metrics: confirm `ItemsProcessed` (+ `StageLatency` for ActionBlock) fire; extend `DiagnosticsTests` if a
      block exposes a metric path the TransformBlock tests do not already cover (e.g. batch `ItemsProcessed` adds
      N at once)

### Post-implementation optimization pass

- [x] `BatchBlock<T>` grow-on-demand buffer: rent at `min(batchSize, 16)` and double up to `batchSize` instead of
      renting `batchSize` up front, so a large-cap hybrid policy never rents more than it fills (`AppendCore`/
      `GrowBuffer`/`TryDetachBuffer`).
- [x] `BatchBlock<T>` lock elision: with no timer loop the reader loop is the sole toucher of
      `_buffer`/`_capacity`/`_count`, so `_batchLock` is skipped entirely on the per-item path. Gated on a
      `readonly bool _hasTimer` assigned *before* the reader loop is queued - testing `_timerLoop is not null`
      instead would be read by a loop started before that field's assignment, with no happens-before edge making
      it visible.
- [x] `BatchBlocks/SteadyStateBenchmarks` added: the existing `ThroughputBenchmarks` sends all 50_000 items before
      draining any, which structurally cannot show a pooling win (see findings below).
- [x] `TargetFluxBlockBase.SendAsyncSlow` pooled state machine box:
      `[AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]`. Under multi-producer contention a
      large fraction of sends suspend and the default builder heap-allocates a box per suspension (~62 B,
      isolated by probe). Cuts `BatchBlocks/MultiProducer` allocation by 76% / 63% / 51% at 1 / 4 / 8 producers
      and drops Gen0 to zero at 1 and 4. Time unaffected. Tightens the `SendAsync` single-consumption rule from
      "undefined if violated" to "corrupting if violated" - see the handoff doc's invariants.
- [x] `BufferBlocks/MultiProducerBenchmarks` and `TransformBlocks/MultiProducerBenchmarks` added, to establish how
      wide the multi-producer gap is. Answer: narrower than believed - Flux wins at one producer on all three
      blocks and is at parity at eight on TransformBlock. See the handoff doc's open item 2.

## Benchmark findings (post-implementation, AMD 7900X, net10.0 host)

**These are the figures as of the initial implementation and are kept for the reasoning they support, not as
current numbers.** The live benchmark picture lives in [CLAUDE.handoff.md](CLAUDE.handoff.md).

Flux wins decisively on allocation and is at parity-or-better on time **when per-item work is non-trivial**, and
loses on time **when the per-item work is trivial enough that channel plumbing is the whole cost**:

| Suite | Flux time vs Dataflow | Flux alloc vs Dataflow |
|---|---|---|
| `TransformBlocks/Throughput` (DOP 1 / 4) | 0.88x - 1.07x | 0.42x - 0.73x |
| `BatchBlocks/SteadyState` (bs 10 / 100 / 1000) | 0.88x / 1.79x / 2.37x | **0.24x / 0.05x / 0.08x** |
| `BatchBlocks/Throughput` (burst, bs 10 / 100 / 1000) | ~2.0x / 2.1x / 2.2x | 1.35x / 0.25x / 0.14x |
| `BufferBlocks/Throughput` | 1.44x | **2.26x** |

- **The steady-state batch numbers are the marquee result**, not the burst ones. `ThroughputBenchmarks` sends
  everything before draining, leaving thousands of rented arrays live at small batch sizes - far more than
  `ArrayPool<T>.Shared` retains, so nearly every rent misses the pool and every return is dropped to the GC. That
  is why burst-mode `bs=10` shows Flux allocating *more* (1.35x) than Dataflow. Under concurrent drain the same
  config allocates 0.24x, and `bs=100` collapses to 0.05x.
- **The ~2x time gap on `BatchBlock` is architectural, not a defect.** Probed directly: one raw bounded-channel
  hop of 50_000 items costs ~3.2 ms, and the whole Flux `BatchBlock` costs ~3.35 ms - i.e. accumulation and
  flushing are nearly free and the cost *is* the input-channel hop. Dataflow's `BatchBlock` accumulates on the
  caller's thread under its own lock and never pays a cross-thread queue handoff. Closing this would mean
  batching inside `TryOffer` rather than in a reader loop, abandoning the uniform channel architecture. Not
  worth it; documented instead.
- **Bounded-channel queue growth dominates Flux's remaining allocation.** A raw single hop allocates 9.7 KB at
  `BoundedCapacity = 1_000`, but 179 KB at `1_000_000` - the channel's internal deque doubling to hold the
  backlog. Benchmarks that pair a huge capacity with a burst producer are measuring deque growth, not the block.

## Resolved: single-channel `BufferBlock` (built)

The "out of scope (noted, not built)" item above was explicitly gated on *"Only pursue if the benchmark shows the
pump lagging Dataflow."* That trigger fired - BufferBlock was 1.44x slower *and* 2.26x the allocation, the only
block in the suite losing on both axes - so it was built.

`PropagatorFluxBlockBase` gained an `aliasOutputToInput` ctor flag that makes `_output` the very same channel
object as `_input` (`(Channel<TOut>)(object)_input`, guarded by a `Debug.Assert` on `TIn == TOut`). `BufferBlock`
has no pump loop and no second channel: producers write the channel that `DispatchLoopAsync` and
`ReceiveAllAsync` read. **Deliberately BufferBlock-only** - any block that transforms, batches, or otherwise
decouples its input rate from its output rate needs two channels to apply backpressure independently.

| BufferBlock vs Dataflow | before | after |
|---|---|---|
| `Throughput` (burst) time | 1.44x | **0.68x** |
| `Throughput` (burst) alloc | 2.26x | 1.96x |
| `Backpressure` (concurrent) time | 0.92x | **0.33x** |
| `Backpressure` (concurrent) alloc | 0.48x | **0.18x** |

The burst-mode allocation barely moved, and that is expected rather than a miss: with 50_000 items sent before
anything drains, whichever channel holds the backlog must grow a deque to 50_000 entries, and that single deque
dominates. Removing the *second* channel removed only the shallower of the two. The concurrent numbers - 3x
faster, 5.5x leaner - are the ones that reflect the change.

Three things the pump loop was silently doing besides moving items, all of which had to be replaced:

1. **It was the block's only cancellation observer.** Every Flux block learns its options token fired by having a
   loop awaiting `WaitToReadAsync(_loopToken)`. With no pump, nothing completes the channel on cancellation and
   `Completion` would hang instead of resolving `Canceled`. `FinalizeAsync` now awaits `_input.Reader.Completion`,
   wrapped in `WaitAsync(_loopToken)` only when `_loopToken.CanBeCanceled` - which keeps the None-token
   allocation trick intact for the common case.
2. **It made `Fault` look decisive.** A bounded channel withholds its reader-side completion signal until the
   queue empties *even when completed with an error*, so a faulted block with buffered items would sit pending
   until something drained it. `Fault` is now `virtual` on `TargetFluxBlockBase` and `BufferBlock` overrides it to
   discard the buffer - which is what `IFluxTarget<T>.Fault`'s own XML doc already promised, and what Dataflow
   does ("faulting a block... causes buffered messages... to be lost").
3. **It was the `ItemsProcessed` firing site.** Replaced by a `_countAcceptedAsProcessed` flag on
   `TargetFluxBlockBase`, checked field-first in `TryOffer` so every other block pays a predicted-not-taken
   branch that never loads the instrument. For a passthrough, accepted and processed are the same count.

Two deliberate behavior changes, both moving *toward* Dataflow parity rather than away from it:

- `BoundedCapacity = N` now means exactly N items resident, not up to 2N across two channels.
- `Completion` resolves once the block is completed *and* drained, so a completed buffer with no consumer and no
  link stays pending. `Fault` is exempt, per above.

Coverage added on top of the existing BufferBlock tests, since BufferBlock is now the one block whose dispatch
loop reads the same channel its producers write to: multi-link `Broadcast`/`RoundRobin`/`FirstAvailable`,
per-link filters, `LinkTo` against an already-buffered backlog, unlink-then-relink, 8-way concurrent producers
under a capacity of 16, plus the changed `Complete`/`Fault`/cancellation semantics and BufferBlock's
`ItemsProcessed`. 112 tests, stable over 10 repeat runs.
