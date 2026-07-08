using System.Threading.Tasks.Dataflow;

using FluxBatch = LaquaiLib.Flux.BatchBlock<int>;
using DataflowBatch = System.Threading.Tasks.Dataflow.BatchBlock<int>;

namespace LaquaiLib.Flux.Benchmarks.BatchBlocks;

/// <summary>
/// Bulk throughput: push <see cref="ItemCount"/> items through a single, unlinked BatchBlock (count-only flush,
/// large bounded capacity) and drain every emitted batch.
/// <para/>
/// This is the headline allocation benchmark for <see cref="LaquaiLib.Flux.BatchBlock{T}"/>: the Flux drain loop
/// below disposes every <see cref="PooledBatch{T}"/> it receives, returning its rented array to
/// <see cref="ArrayPool{T}.Shared"/> immediately. Forgetting that <c>Dispose()</c> call would silently erase the
/// entire allocation win this type exists for - <see cref="MemoryDiagnoser"/> would then show Flux allocating
/// just as much as Dataflow's per-batch <c>int[]</c>, defeating the whole point of pooling.
/// </summary>
[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class ThroughputBenchmarks
{
    private const int ItemCount = 50_000;
    private const int Capacity = 1_000_000;

    [Params(10, 100, 1000)]
    public int BatchSize { get; set; }

    [Benchmark(Baseline = true)]
    public Task<int> Dataflow() => RunAsync(new DataflowBatch(BatchSize, new GroupingDataflowBlockOptions { BoundedCapacity = Capacity }));

    [Benchmark]
    public Task<int> Flux() => RunAsync(new FluxBatch(BatchSize, Timeout.InfiniteTimeSpan, new FluxBlockOptions { BoundedCapacity = Capacity }));

    private static async Task<int> RunAsync(FluxBatch block)
    {
        for (var i = 0; i < ItemCount; i++)
        {
            await block.SendAsync(i).ConfigureAwait(false);
        }
        block.Complete();

        var count = 0;
        await foreach (var batch in block.ReceiveAllAsync())
        {
            count += batch.Count;
            // MANDATORY: without this Dispose, the rented array is never returned to the pool and the
            // MemoryDiagnoser allocation numbers below would no longer reflect real steady-state usage.
            batch.Dispose();
        }
        return count;
    }

    private static async Task<int> RunAsync(DataflowBatch block)
    {
        for (var i = 0; i < ItemCount; i++)
        {
            await block.SendAsync(i).ConfigureAwait(false);
        }
        block.Complete();

        var count = 0;
        while (await block.OutputAvailableAsync().ConfigureAwait(false))
        {
            while (block.TryReceive(out var batch))
            {
                count += batch.Length;
            }
        }
        return count;
    }
}
