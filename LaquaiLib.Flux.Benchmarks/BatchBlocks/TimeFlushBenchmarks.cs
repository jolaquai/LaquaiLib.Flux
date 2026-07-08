using System.Threading.Tasks.Dataflow;

using FluxBatch = LaquaiLib.Flux.BatchBlock<int>;
using DataflowBatch = System.Threading.Tasks.Dataflow.BatchBlock<int>;

namespace LaquaiLib.Flux.Benchmarks.BatchBlocks;

/// <summary>
/// Exercises the hybrid count-or-time flush path: batch size is set high enough that it is never reached by
/// count alone, so every emitted batch must come from the time-based flush. Items arrive in small waves spaced
/// further apart than the flush interval, so a partial batch is genuinely sitting idle when the interval elapses.
/// <para/>
/// <see cref="System.Threading.Tasks.Dataflow.BatchBlock{T}"/> has no built-in time-based flush - it only ever
/// batches by count, or when explicitly told to via <see cref="System.Threading.Tasks.Dataflow.BatchBlock{T}.TriggerBatch"/>.
/// <see cref="Dataflow"/> below is therefore a best-effort hand-rolled equivalent: a <see cref="System.Threading.Timer"/>
/// ticking at the same interval and calling <see cref="System.Threading.Tasks.Dataflow.BatchBlock{T}.TriggerBatch"/>,
/// which is a no-op whenever nothing is currently buffered (mirroring <see cref="FluxBatch"/>'s own "peer already
/// flushed it" no-op). This is not a like-for-like API comparison - it is here purely to show Flux's built-in
/// hybrid policy does not cost meaningfully more than assembling the same behavior by hand against Dataflow.
/// </summary>
[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class TimeFlushBenchmarks
{
    private const int WaveCount = 10;
    private const int ItemsPerWave = 3;

    // Large enough that count-based flushing can never trigger (WaveCount * ItemsPerWave is far below it), but
    // deliberately modest: BatchBlock<T> eagerly rents an array sized to the batch size up front and again on
    // every flush (see BatchBlock<T>'s ctor/FlushAsync), so an oversized value here (int.MaxValue throws
    // OutOfMemoryException outright; even 1_000_000 rents a ~4 MB int[] per flush) would dominate the allocation
    // numbers with pool-priming noise unrelated to what this benchmark is actually measuring.
    private const int UnreachableBatchSize = 10_000;

    private static readonly TimeSpan FlushInterval = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan WaveDelay = TimeSpan.FromMilliseconds(15);

    [Benchmark(Baseline = true)]
    public async Task<int> Dataflow()
    {
        var block = new DataflowBatch(UnreachableBatchSize, new GroupingDataflowBlockOptions { BoundedCapacity = DataflowBlockOptions.Unbounded });
        using var timer = new Timer(_ => block.TriggerBatch(), null, FlushInterval, FlushInterval);

        var drainCount = 0;
        var drain = Task.Run(async () =>
        {
            while (await block.OutputAvailableAsync().ConfigureAwait(false))
            {
                while (block.TryReceive(out var batch))
                {
                    drainCount += batch.Length;
                }
            }
        });

        var value = 0;
        for (var w = 0; w < WaveCount; w++)
        {
            for (var i = 0; i < ItemsPerWave; i++)
            {
                await block.SendAsync(value++).ConfigureAwait(false);
            }
            await Task.Delay(WaveDelay).ConfigureAwait(false);
        }
        block.Complete();

        await drain.ConfigureAwait(false);
        return drainCount;
    }

    [Benchmark]
    public async Task<int> Flux()
    {
        var block = new FluxBatch(UnreachableBatchSize, FlushInterval);

        var drainCount = 0;
        var drain = Task.Run(async () =>
        {
            await foreach (var batch in block.ReceiveAllAsync().ConfigureAwait(false))
            {
                drainCount += batch.Count;
                batch.Dispose();
            }
        });

        var value = 0;
        for (var w = 0; w < WaveCount; w++)
        {
            for (var i = 0; i < ItemsPerWave; i++)
            {
                await block.SendAsync(value++).ConfigureAwait(false);
            }
            await Task.Delay(WaveDelay).ConfigureAwait(false);
        }
        block.Complete();

        await drain.ConfigureAwait(false);
        return drainCount;
    }
}
