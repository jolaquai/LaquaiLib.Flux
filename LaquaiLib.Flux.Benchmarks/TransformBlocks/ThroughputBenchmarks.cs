using System.Threading.Tasks.Dataflow;

using FluxTransform = LaquaiLib.Flux.TransformBlock<int, int>;
using DataflowTransform = System.Threading.Tasks.Dataflow.TransformBlock<int, int>;

namespace LaquaiLib.Flux.Benchmarks.TransformBlocks;

/// <summary>
/// Bulk throughput: push <see cref="ItemCount"/> items through a single, unlinked TransformBlock and drain all
/// output, across both unordered (the Flux default) and ordered (Dataflow's default) modes at varying
/// <see cref="FluxBlockOptions.MaxDegreeOfParallelism"/>. Both sides use a deliberately large bounded capacity so
/// this isolates processing throughput rather than backpressure interaction - see
/// <see cref="BackpressureBenchmarks"/> for that.
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

    private static int Transform(int x)
    {
        var acc = x;
        for (var i = 0; i < 50; i++)
        {
            acc = (acc * 31) ^ i;
        }
        return acc;
    }

    [Benchmark(Baseline = true)]
    public Task<int> Dataflow_Unordered() => RunAsync(new DataflowTransform(Transform, new ExecutionDataflowBlockOptions
    {
        BoundedCapacity = Capacity,
        MaxDegreeOfParallelism = DegreeOfParallelism,
        EnsureOrdered = false,
    }));

    [Benchmark]
    public Task<int> Flux_Unordered() => RunAsync(new FluxTransform(Transform, new FluxBlockOptions
    {
        BoundedCapacity = Capacity,
        MaxDegreeOfParallelism = DegreeOfParallelism,
        EnsureOrdered = false,
    }));

    [Benchmark]
    public Task<int> Dataflow_Ordered() => RunAsync(new DataflowTransform(Transform, new ExecutionDataflowBlockOptions
    {
        BoundedCapacity = Capacity,
        MaxDegreeOfParallelism = DegreeOfParallelism,
        EnsureOrdered = true,
    }));

    [Benchmark]
    public Task<int> Flux_Ordered() => RunAsync(new FluxTransform(Transform, new FluxBlockOptions
    {
        BoundedCapacity = Capacity,
        MaxDegreeOfParallelism = DegreeOfParallelism,
        EnsureOrdered = true,
    }));

    private static async Task<int> RunAsync(FluxTransform block)
    {
        for (var i = 0; i < ItemCount; i++)
        {
            await block.SendAsync(i).ConfigureAwait(false);
        }
        block.Complete();

        var count = 0;
        await foreach (var _ in block.ReceiveAllAsync())
        {
            count++;
        }
        return count;
    }

    private static async Task<int> RunAsync(DataflowTransform block)
    {
        for (var i = 0; i < ItemCount; i++)
        {
            await block.SendAsync(i).ConfigureAwait(false);
        }
        block.Complete();

        var count = 0;
        while (await block.OutputAvailableAsync().ConfigureAwait(false))
        {
            while (block.TryReceive(out _))
            {
                count++;
            }
        }
        return count;
    }
}
