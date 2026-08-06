using System.Threading.Tasks.Dataflow;

using FluxBuffer = LaquaiLib.Flux.BufferBlock<int>;
using DataflowBuffer = System.Threading.Tasks.Dataflow.BufferBlock<int>;

namespace LaquaiLib.Flux.Benchmarks.BufferBlocks;

/// <summary>
/// The same steady-state passthrough as <see cref="ThroughputBenchmarks"/>, but with the fixed item count split
/// across a varying number of concurrent producers, all contending on a bounded capacity while a single consumer
/// drains concurrently. This isolates how each block's input side behaves under concurrent writers rather than a
/// single calling thread.
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

    [Benchmark(Baseline = true)]
    public async Task<int> Dataflow()
    {
        var block = new DataflowBuffer(new DataflowBlockOptions { BoundedCapacity = Capacity });

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
        var block = new FluxBuffer(new FluxBlockOptions { BoundedCapacity = Capacity });

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
