using System.Threading.Tasks.Dataflow;

using FluxTransform = LaquaiLib.Flux.TransformBlock<int, int>;
using DataflowTransform = System.Threading.Tasks.Dataflow.TransformBlock<int, int>;

namespace LaquaiLib.Flux.Benchmarks.TransformBlocks;

/// <summary>
/// The same steady-state transform as <see cref="ThroughputBenchmarks"/>, but with the fixed item count split
/// across a varying number of concurrent producers, all contending on a bounded capacity while a single consumer
/// drains concurrently. Both sides run at <see cref="FluxBlockOptions.MaxDegreeOfParallelism"/> 1 in unordered
/// mode, so this isolates concurrent-writer contention on the input side rather than parallel processing
/// throughput.
/// </summary>
[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class MultiProducerBenchmarks
{
    private const int ItemCount = 50_000;
    private const int Capacity = 200;

    // Divides ItemCount exactly at every value, so total work is identical across params.
    [Params(1, 4, 8)]
    public int ProducerCount { get; set; }

    private int PerProducer => ItemCount / ProducerCount;

    private static int Transform(int x) => x * 2;

    [Benchmark(Baseline = true)]
    public async Task<int> Dataflow()
    {
        var block = new DataflowTransform(Transform, new ExecutionDataflowBlockOptions
        {
            BoundedCapacity = Capacity,
            MaxDegreeOfParallelism = 1,
            EnsureOrdered = false,
        });

        var count = 0;
        var consume = Task.Run(async () =>
        {
            while (await block.OutputAvailableAsync().ConfigureAwait(false))
            {
                while (block.TryReceive(out _))
                {
                    count++;
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
        var block = new FluxTransform(Transform, new FluxBlockOptions
        {
            BoundedCapacity = Capacity,
            MaxDegreeOfParallelism = 1,
            EnsureOrdered = false,
        });

        var count = 0;
        var consume = Task.Run(async () =>
        {
            await foreach (var _ in block.ReceiveAllAsync().ConfigureAwait(false))
            {
                count++;
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
