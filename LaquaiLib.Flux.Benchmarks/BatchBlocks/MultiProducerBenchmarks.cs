using System.Threading.Tasks.Dataflow;

using FluxBatch = LaquaiLib.Flux.BatchBlock<int>;
using DataflowBatch = System.Threading.Tasks.Dataflow.BatchBlock<int>;

namespace LaquaiLib.Flux.Benchmarks.BatchBlocks;

/// <summary>
/// The same steady-state batching as <see cref="SteadyStateBenchmarks"/>, but with the fixed item count split
/// across a varying number of concurrent producers.
/// <para/>
/// This exists because every other batch benchmark here sends from a single thread, and that is precisely the
/// shape that flatters Dataflow: its <see cref="DataflowBatch"/> accumulates into the batch on the calling
/// thread, so one producer never pays a cross-thread handoff, while <see cref="FluxBatch"/> always hands items to
/// its reader loop through the input channel. Measuring only that shape would overstate the gap as a property of
/// the block rather than of the workload.
/// <para/>
/// Raising <see cref="ProducerCount"/> inverts the pressure: Dataflow's callers now contend with each other on
/// the block's own lock to accumulate, whereas Flux's accumulation stays single-threaded in its reader loop and
/// the producers only contend on the channel write. Whether the single-producer gap survives concurrency is the
/// question this answers.
/// </summary>
[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class MultiProducerBenchmarks
{
    private const int ItemCount = 50_000;
    private const int BatchSize = 100;
    // Dataflow rejects any BoundedCapacity below its batch size, so this has to clear BatchSize while staying
    // small enough that producers genuinely wait on the drain loop.
    private const int Capacity = BatchSize * 2;

    // Divides ItemCount exactly at every value, so total work is identical across params.
    [Params(1, 4, 8)]
    public int ProducerCount { get; set; }

    private int PerProducer => ItemCount / ProducerCount;

    [Benchmark(Baseline = true)]
    public async Task<int> Dataflow()
    {
        var block = new DataflowBatch(BatchSize, new GroupingDataflowBlockOptions { BoundedCapacity = Capacity });

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

        var perProducer = PerProducer;
        var producers = new Task[ProducerCount];
        for (var p = 0; p < producers.Length; p++)
        {
            var offset = p * perProducer;
            producers[p] = Task.Run(async () =>
            {
                for (var i = 0; i < perProducer; i++)
                {
                    await block.SendAsync(offset + i).ConfigureAwait(false);
                }
            });
        }

        await Task.WhenAll(producers).ConfigureAwait(false);
        block.Complete();
        await consume.ConfigureAwait(false);
        return count;
    }

    [Benchmark]
    public async Task<int> Flux()
    {
        var block = new FluxBatch(BatchSize, Timeout.InfiniteTimeSpan, new FluxBlockOptions { BoundedCapacity = Capacity });

        var count = 0;
        var consume = Task.Run(async () =>
        {
            await foreach (var batch in block.ReceiveAllAsync().ConfigureAwait(false))
            {
                count += batch.Count;
                batch.Dispose();
            }
        });

        var perProducer = PerProducer;
        var producers = new Task[ProducerCount];
        for (var p = 0; p < producers.Length; p++)
        {
            var offset = p * perProducer;
            producers[p] = Task.Run(async () =>
            {
                for (var i = 0; i < perProducer; i++)
                {
                    await block.SendAsync(offset + i).ConfigureAwait(false);
                }
            });
        }

        await Task.WhenAll(producers).ConfigureAwait(false);
        block.Complete();
        await consume.ConfigureAwait(false);
        return count;
    }
}
