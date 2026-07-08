using System.Threading.Tasks.Dataflow;

using FluxBuffer = LaquaiLib.Flux.BufferBlock<int>;
using DataflowBuffer = System.Threading.Tasks.Dataflow.BufferBlock<int>;

namespace LaquaiLib.Flux.Benchmarks.BufferBlocks;

/// <summary>
/// Throughput under sustained backpressure: a deliberately small bounded capacity on both sides (far smaller
/// than <see cref="ItemCount"/>), so most sends actually suspend waiting for room rather than completing
/// synchronously.
/// <para/>
/// Producing and draining run concurrently via <see cref="Task.WhenAll(Task, Task)"/>, not send-everything-then-
/// drain: with capacity this small, nothing would ever consume the buffered items until the send loop finished,
/// but the send loop can never finish because the buffer fills and stays full with nobody pulling from it -
/// a real deadlock, not just a slow run.
/// </summary>
[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class BackpressureBenchmarks
{
    private const int ItemCount = 2_000;
    private const int Capacity = 16;

    [Benchmark(Baseline = true)]
    public async Task<int> Dataflow()
    {
        var block = new DataflowBuffer(new DataflowBlockOptions { BoundedCapacity = Capacity });

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
                while (block.TryReceive(out _))
                {
                    count++;
                }
            }
        });

        await Task.WhenAll(produce, consume).ConfigureAwait(false);
        return count;
    }

    [Benchmark]
    public async Task<int> Flux()
    {
        var block = new FluxBuffer(new FluxBlockOptions { BoundedCapacity = Capacity });

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
            await foreach (var _ in block.ReceiveAllAsync())
            {
                count++;
            }
        });

        await Task.WhenAll(produce, consume).ConfigureAwait(false);
        return count;
    }
}
