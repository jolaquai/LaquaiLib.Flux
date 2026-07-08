using System.Threading.Tasks.Dataflow;

using FluxBuffer = LaquaiLib.Flux.BufferBlock<int>;
using DataflowBuffer = System.Threading.Tasks.Dataflow.BufferBlock<int>;

namespace LaquaiLib.Flux.Benchmarks.BufferBlocks;

/// <summary>
/// Bulk throughput: push <see cref="ItemCount"/> items through a single, unlinked BufferBlock and drain all
/// output. Both sides use a deliberately large bounded capacity so this isolates passthrough throughput rather
/// than backpressure interaction - see <see cref="BackpressureBenchmarks"/> for that.
/// </summary>
[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class ThroughputBenchmarks
{
    private const int ItemCount = 50_000;
    private const int Capacity = 1_000_000;

    [Benchmark(Baseline = true)]
    public Task<int> Dataflow() => RunAsync(new DataflowBuffer(new DataflowBlockOptions { BoundedCapacity = Capacity }));

    [Benchmark]
    public Task<int> Flux() => RunAsync(new FluxBuffer(new FluxBlockOptions { BoundedCapacity = Capacity }));

    private static async Task<int> RunAsync(FluxBuffer block)
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

    private static async Task<int> RunAsync(DataflowBuffer block)
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
