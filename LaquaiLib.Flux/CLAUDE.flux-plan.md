# LaquaiLib.Flux - plan

Dataflow-adjacent pipeline library. Goal: genuine reasons to pick over `System.Threading.Tasks.Dataflow`, not just a rewrite.

## Why Dataflow is beatable

- Built on pre-`Channel<T>` internals; `SendAsync`/`ReceiveAsync` allocate a `Task` per call instead of `ValueTask`.
- `EnsureOrdered` defaults to `true`, silently taxes throughput.
- No `IAsyncEnumerable<T>` bridge; bespoke `ISourceBlock`/`ITargetBlock` adapters required.
- No observability: no queue depth, per-stage latency, drop/backpressure events without hand-instrumenting every block.
- Not NativeAOT/trimming-first (works, but not designed for it).
- Per-item dispatch cost comes from delegate invocation through runtime-built block graph; no path to static/monomorphized dispatch.

## v1 scope (runtime core, must be benchmarkable against Dataflow)

- Blocks backed by `Channel<T>` (bounded, `SingleReader`/`SingleWriter` flags set correctly per topology).
- `ValueTask`-based APIs throughout; avoid `Task` allocation on the sync-completion path.
- Ordering opt-in, not default. Unordered fan-out/fan-in is the default fast path.
- Batching: pooled-array batches (`ArrayPool<T>`) with hybrid count+time flush (`PeriodicTimer`-driven), no per-batch heap array like `BatchBlock`.
- `System.Diagnostics.Metrics` (`Meter`/`Counter`/`Histogram`) wired into every stage by default: queue depth, per-stage latency, drop/backpressure counts. Must work with `dotnet-counters`/OTel exporter with zero extra config.
- NativeAOT/trimming compatible: `IsAotCompatible=true`, no reflection, no `DynamicallyAccessedMembers` gymnastics.
- Target latest/preview C#, net11.0 preview. `<Nullable>disable</Nullable>`. No nullable annotations anywhere.
- Aggressive perf defaults per house style: `Span<T>`/`ReadOnlySpan<T>`/scoped params where applicable, `stackalloc` for sub-2KB buffers, `[MethodImpl(AggressiveInlining)]` on hot-path library calls, skip validation on hot paths (validate only at public API boundaries).

## v2 differentiator (the actual pitch)

- Source-generator-based static pipeline wiring: when the pipeline shape is known at compile time (chain of stages declared via attributes or a fluent builder captured by the generator), emit a specialized, monomorphized dispatch loop, no delegate indirection, no boxing.
- This is the feature Dataflow cannot structurally offer (its graph is built at runtime). Only ship this after the runtime core proves out in benchmarks.

## Adoption bar

Dataflow ships in the BCL for free; that inertia only breaks with numbers. Before claiming any win, produce reproducible benchmarks:

- Allocations/op
- ns/op
- p99 latency under backpressure

Compare directly against equivalent `TransformBlock`/`BatchBlock`/`ActionBlock` graphs. No blog-post claims without a benchmark project backing them (`BenchmarkDotNet`, checked into repo).

## Explicit non-goals for v1

- No general "wrap any sequential processor in 10 lines" magic API surface, that's scope creep back into Dataflow's own generality tax.
- No support for exotic block topologies (priority queues, weighted fair scheduling) until core is validated.
