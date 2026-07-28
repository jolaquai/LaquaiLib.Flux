using System.Threading.Tasks.Dataflow;

using FluxBatch = LaquaiLib.Flux.BatchBlock<int>;
using DataflowBatch = System.Threading.Tasks.Dataflow.BatchBlock<int>;

namespace LaquaiLib.Flux.Benchmarks.BatchBlocks;

/// <summary>
/// Steady-state batching: a modest bounded capacity with production and draining running concurrently, so only a
/// handful of batches are ever in flight at once.
/// <para/>
/// This, not <see cref="ThroughputBenchmarks"/>, is where <see cref="PooledBatch{T}"/>'s pooling actually shows
/// up. <see cref="ThroughputBenchmarks"/> deliberately sends all 50_000 items before draining any of them, which
/// at a small batch size leaves thousands of rented arrays live simultaneously - far more than
/// <see cref="ArrayPool{T}.Shared"/> retains, so nearly every rent misses the pool and allocates, and nearly every
/// return is dropped straight to the GC. That measures burst behavior honestly, but it structurally cannot show a
/// pooling win. Here the drain loop disposes each batch while the producer is still running, so the same few
/// arrays cycle through the pool for the whole run and the per-batch allocation collapses to ~0.
/// <para/>
/// The <c>Dispose()</c> in the Flux drain loop is what returns each array to the pool; dropping it turns this
/// benchmark back into <see cref="ThroughputBenchmarks"/>'s allocation profile.
/// </summary>
[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class SteadyStateBenchmarks
{
    private const int ItemCount = 50_000;

    [Params(10, 100, 1000)]
    public int BatchSize { get; set; }

    // Small enough that the producer genuinely has to wait on the drain loop, so in-flight batches stay bounded,
    // but Dataflow's BatchBlock rejects any BoundedCapacity below its batch size outright, so this cannot be a
    // flat constant - it has to scale with BatchSize to stay legal at every param value.
    private int Capacity => BatchSize * 2;

    [Benchmark(Baseline = true)]
    public async Task<int> Dataflow()
    {
        var block = new DataflowBatch(BatchSize, new GroupingDataflowBlockOptions { BoundedCapacity = Capacity });

        var produce = Task.Run(async () =>
        {
            for (var i = 0; i < ItemCount; i++)
            {
                await block.SendAsync(i).ConfigureAwait(false);
            }
            block.Complete();
        });

        var count = 0;
        var consume = Task.Run(async () =>
        {
            while (await block.OutputAvailableAsync().ConfigureAwait(false))
            {
                while (block.TryReceive(out var batch))
                {
                    count += batch.Length;
                }
            }
        });

        await Task.WhenAll(produce, consume).ConfigureAwait(false);
        return count;
    }

    [Benchmark]
    public async Task<int> Flux()
    {
        var block = new FluxBatch(BatchSize, Timeout.InfiniteTimeSpan, new FluxBlockOptions { BoundedCapacity = Capacity });

        var produce = Task.Run(async () =>
        {
            for (var i = 0; i < ItemCount; i++)
            {
                await block.SendAsync(i).ConfigureAwait(false);
            }
            block.Complete();
        });

        var count = 0;
        var consume = Task.Run(async () =>
        {
            await foreach (var batch in block.ReceiveAllAsync().ConfigureAwait(false))
            {
                count += batch.Count;
                batch.Dispose();
            }
        });

        await Task.WhenAll(produce, consume).ConfigureAwait(false);
        return count;
    }
}
