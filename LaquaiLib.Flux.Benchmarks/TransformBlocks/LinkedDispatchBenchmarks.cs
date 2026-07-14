using System.Threading.Tasks.Dataflow;

using BenchmarkDotNet.Configs;

using FluxTransform = LaquaiLib.Flux.TransformBlock<int, int>;
using DataflowTransform = System.Threading.Tasks.Dataflow.TransformBlock<int, int>;
using DataflowAction = System.Threading.Tasks.Dataflow.ActionBlock<int>;

namespace LaquaiLib.Flux.Benchmarks.TransformBlocks;

/// <summary>
/// Cost of dispatching produced items to linked targets under each <see cref="FluxFanOutMode"/>. Every benchmark
/// builds a fresh pipeline per invocation (construction cost included, same convention as
/// <see cref="ThroughputBenchmarks"/>), pushes <see cref="ItemCount"/> items through it, completes the source and
/// waits for every linked target to observe completion - so the measured time genuinely covers dispatch, not just
/// enqueueing into the source's own input channel.
/// <para/>
/// Grouped by <see cref="BenchmarkLogicalGroupRule.ByCategory"/> instead of one global baseline: a single baseline
/// makes every ratio relative to whichever scenario happens to be marked <c>Baseline = true</c>, which reads fine
/// for that one row and misleadingly for everything else (e.g. a 4-link number silently compared against a 2-link
/// baseline). Each category below pairs exactly one Flux benchmark with the idiomatic Dataflow equivalent for that
/// same scenario, so every printed ratio is a same-shape, same-<see cref="ItemCount"/> comparison:
/// <list type="bullet">
/// <item><b>SingleLink</b> - no fan-out at all: one linked target apiece.</item>
/// <item><b>Broadcast-2</b>/<b>Broadcast-4</b> - duplicate every item to every target. Dataflow has no built-in
/// broadcast on a plain propagator block, so the comparison point is a <see cref="BroadcastBlock{T}"/> stage.</item>
/// <item><b>FirstAvailable-2</b> - link order is priority order. Dataflow's own multi-link <c>LinkTo</c> already
/// behaves this way: a message is offered to linked targets in link order, consumed by the first to accept.</item>
/// <item><b>LoadBalance-2</b> - spread work adaptively across 2 concurrently-available consumers. Dataflow has no
/// fan-out-to-N-separate-targets balancer (plain multi-link <c>LinkTo</c> degenerates to "always link 0" once it
/// never declines, exactly what <b>FirstAvailable-2</b> demonstrates); its idiomatic answer to "spread this work
/// over N concurrent workers" is <see cref="ExecutionDataflowBlockOptions.MaxDegreeOfParallelism"/> on a single
/// block, not N linked block instances. That is not a structural match for
/// <see cref="Flux_RoundRobin_2Links"/> (one block's internal worker slots vs. two separate linked
/// <see cref="IFluxTarget{TIn}"/> instances), but it is the fair comparison for the question this category
/// actually asks: what each library charges to get <see cref="ItemCount"/> items processed by 2
/// concurrently-available workers rather than 1.</item>
/// </list>
/// </summary>
[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class LinkedDispatchBenchmarks
{
    private const int ItemCount = 50_000;
    private const int Capacity = 1_000_000;

    [BenchmarkCategory("SingleLink")]
    [Benchmark(Baseline = true)]
    // Equivalent to RunDataflowFirstAvailableAsync(1): with exactly one linked target, "first available" and
    // "just send it" are the same thing. Named for what this category tests, not which helper implements it.
    public Task Dataflow_SingleLink() => RunDataflowFirstAvailableAsync(1);

    [BenchmarkCategory("SingleLink")]
    [Benchmark]
    public Task Flux_SingleLink() => RunFluxAsync(FluxFanOutMode.Broadcast, 1);

    [BenchmarkCategory("Broadcast-2")]
    [Benchmark(Baseline = true)]
    public Task Dataflow_Broadcast_2Links() => RunDataflowBroadcastAsync(2);

    [BenchmarkCategory("Broadcast-2")]
    [Benchmark]
    public Task Flux_Broadcast_2Links() => RunFluxAsync(FluxFanOutMode.Broadcast, 2);

    [BenchmarkCategory("Broadcast-4")]
    [Benchmark(Baseline = true)]
    public Task Dataflow_Broadcast_4Links() => RunDataflowBroadcastAsync(4);

    [BenchmarkCategory("Broadcast-4")]
    [Benchmark]
    public Task Flux_Broadcast_4Links() => RunFluxAsync(FluxFanOutMode.Broadcast, 4);

    [BenchmarkCategory("FirstAvailable-2")]
    [Benchmark(Baseline = true)]
    public Task Dataflow_FirstAvailable_2Links() => RunDataflowFirstAvailableAsync(2);

    [BenchmarkCategory("FirstAvailable-2")]
    [Benchmark]
    public Task Flux_FirstAvailable_2Links() => RunFluxAsync(FluxFanOutMode.FirstAvailable, 2);

    [BenchmarkCategory("LoadBalance-2")]
    [Benchmark(Baseline = true)]
    public Task Dataflow_ParallelWorker_2Way() => RunDataflowParallelWorkerAsync(2);

    [BenchmarkCategory("LoadBalance-2")]
    [Benchmark]
    public Task Flux_RoundRobin_2Links() => RunFluxAsync(FluxFanOutMode.RoundRobin, 2);

    private static async Task RunFluxAsync(FluxFanOutMode mode, int linkCount)
    {
        var block = new FluxTransform(static x => x, new FluxBlockOptions { BoundedCapacity = Capacity, FanOutMode = mode });
        var targets = new NoOpTarget[linkCount];
        for (var i = 0; i < linkCount; i++)
        {
            targets[i] = new NoOpTarget();
            block.LinkTo(targets[i]);
        }

        for (var i = 0; i < ItemCount; i++)
        {
            await block.SendAsync(i).ConfigureAwait(false);
        }
        block.Complete();

        await Task.WhenAll(Array.ConvertAll(targets, static t => t.Completion)).ConfigureAwait(false);
    }

    // BroadcastBlock is the idiomatic Dataflow way to clone one stream to multiple targets - a plain propagator's
    // LinkTo only delivers each message to the first accepting target, never every linked target.
    private static async Task RunDataflowBroadcastAsync(int linkCount)
    {
        var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };
        var source = new DataflowTransform(static x => x, new ExecutionDataflowBlockOptions { BoundedCapacity = Capacity });
        var broadcast = new BroadcastBlock<int>(static x => x, new DataflowBlockOptions { BoundedCapacity = Capacity });
        source.LinkTo(broadcast, linkOptions);

        var actions = new DataflowAction[linkCount];
        for (var i = 0; i < linkCount; i++)
        {
            actions[i] = new DataflowAction(static _ => { }, new ExecutionDataflowBlockOptions { BoundedCapacity = Capacity });
            broadcast.LinkTo(actions[i], linkOptions);
        }

        for (var i = 0; i < ItemCount; i++)
        {
            await source.SendAsync(i).ConfigureAwait(false);
        }
        source.Complete();

        await Task.WhenAll(Array.ConvertAll(actions, static a => a.Completion)).ConfigureAwait(false);
    }

    // Default Dataflow multi-link behavior: a message is offered to linked targets in link order and consumed by
    // the first to accept. With both ActionBlocks always able to accept immediately (huge bounded capacity, never
    // full), that is link 0 for effectively every item - the same routing Flux's FirstAvailable produces when its
    // first link never declines. Called with linkCount = 1 doubles as the "no fan-out" SingleLink baseline.
    private static async Task RunDataflowFirstAvailableAsync(int linkCount)
    {
        var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };
        var source = new DataflowTransform(static x => x, new ExecutionDataflowBlockOptions { BoundedCapacity = Capacity });

        var actions = new DataflowAction[linkCount];
        for (var i = 0; i < linkCount; i++)
        {
            actions[i] = new DataflowAction(static _ => { }, new ExecutionDataflowBlockOptions { BoundedCapacity = Capacity });
            source.LinkTo(actions[i], linkOptions);
        }

        for (var i = 0; i < ItemCount; i++)
        {
            await source.SendAsync(i).ConfigureAwait(false);
        }
        source.Complete();

        await Task.WhenAll(Array.ConvertAll(actions, static a => a.Completion)).ConfigureAwait(false);
    }

    // Dataflow's idiomatic answer to "spread this work across N concurrent workers" is one block's internal
    // MaxDegreeOfParallelism, not N linked target block instances (plain multi-link LinkTo always favors the
    // first target able to accept - see RunDataflowFirstAvailableAsync). Not a structural match for Flux's
    // RoundRobin (one block's internal worker slots vs. two separately-linked IFluxTarget instances), but it
    // answers the same practical question: what it costs to get every item to whichever of N workers is free.
    private static async Task RunDataflowParallelWorkerAsync(int degreeOfParallelism)
    {
        var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };
        var source = new DataflowTransform(static x => x, new ExecutionDataflowBlockOptions { BoundedCapacity = Capacity });
        var action = new DataflowAction(static _ => { }, new ExecutionDataflowBlockOptions
        {
            BoundedCapacity = Capacity,
            MaxDegreeOfParallelism = degreeOfParallelism,
        });
        source.LinkTo(action, linkOptions);

        for (var i = 0; i < ItemCount; i++)
        {
            await source.SendAsync(i).ConfigureAwait(false);
        }
        source.Complete();

        await action.Completion.ConfigureAwait(false);
    }

    /// <summary>
    /// Minimal always-accepting <see cref="IFluxTarget{TIn}"/> sink for isolating dispatch overhead without the
    /// allocation noise a recording/storing target would introduce. <see cref="Completion"/> is genuinely backed
    /// by a <see cref="TaskCompletionSource"/> (not a constant completed task) so callers can wait for the
    /// dispatch loop to have actually finished handing off every item to this target.
    /// </summary>
    private sealed class NoOpTarget : IFluxTarget<int>
    {
        private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private long _count;

        public Task Completion => _tcs.Task;
        public string Name => "NoOpTarget";
        public long Count => Volatile.Read(ref _count);

        public ValueTask<bool> SendAsync(int item, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _count);
            return new ValueTask<bool>(true);
        }

        public bool TryOffer(int item)
        {
            Interlocked.Increment(ref _count);
            return true;
        }

        public void Complete() => _tcs.TrySetResult();

        public void Fault(Exception exception) => _tcs.TrySetException(exception);
    }
}
