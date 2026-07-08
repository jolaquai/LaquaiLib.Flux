using System.Threading.Tasks.Dataflow;

using FluxAction = LaquaiLib.Flux.ActionBlock<int>;
using DataflowAction = System.Threading.Tasks.Dataflow.ActionBlock<int>;

namespace LaquaiLib.Flux.Benchmarks.ActionBlocks;

/// <summary>
/// Bulk throughput: push <see cref="ItemCount"/> items through a single, unlinked ActionBlock and await full
/// completion, at varying <see cref="FluxBlockOptions.MaxDegreeOfParallelism"/>. Both sides use a deliberately
/// large bounded capacity so this isolates processing throughput rather than backpressure interaction - see
/// <see cref="BackpressureBenchmarks"/> for that. ActionBlock has no output channel, so the measured span is
/// send-all + Complete + await Completion (no drain loop).
/// </summary>
[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class ThroughputBenchmarks
{
    private const int ItemCount = 50_000;
    private const int Capacity = 1_000_000;

    /// <summary>The degree of parallelism applied to both sides for a given benchmark run.</summary>
    [Params(1, 4)]
    public int DegreeOfParallelism { get; set; }

    private static void Action(int x)
    {
        var acc = x;
        for (var i = 0; i < 50; i++)
        {
            acc = (acc * 31) ^ i;
        }
    }

    [Benchmark(Baseline = true)]
    public Task Dataflow() => RunAsync(new DataflowAction(Action, new ExecutionDataflowBlockOptions
    {
        BoundedCapacity = Capacity,
        MaxDegreeOfParallelism = DegreeOfParallelism,
    }));

    [Benchmark]
    public Task Flux() => RunAsync(new FluxAction(Action, new FluxBlockOptions
    {
        BoundedCapacity = Capacity,
        MaxDegreeOfParallelism = DegreeOfParallelism,
    }));

    private static async Task RunAsync(FluxAction block)
    {
        for (var i = 0; i < ItemCount; i++)
        {
            await block.SendAsync(i).ConfigureAwait(false);
        }
        block.Complete();
        await block.Completion.ConfigureAwait(false);
    }

    private static async Task RunAsync(DataflowAction block)
    {
        for (var i = 0; i < ItemCount; i++)
        {
            await block.SendAsync(i).ConfigureAwait(false);
        }
        block.Complete();
        await block.Completion.ConfigureAwait(false);
    }
}
