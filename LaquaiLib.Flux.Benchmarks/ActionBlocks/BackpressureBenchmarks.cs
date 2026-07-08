using System.Threading.Tasks.Dataflow;

using FluxAction = LaquaiLib.Flux.ActionBlock<int>;
using DataflowAction = System.Threading.Tasks.Dataflow.ActionBlock<int>;

namespace LaquaiLib.Flux.Benchmarks.ActionBlocks;

/// <summary>
/// Throughput under sustained backpressure: a deliberately small bounded capacity on both sides (far smaller
/// than <see cref="ItemCount"/>) paired with a genuinely slow action, so most sends actually suspend waiting for
/// room rather than completing synchronously. Unlike a propagator block, ActionBlock has no output channel to
/// back up or drain - the producer alone suspending on <c>SendAsync</c> while the action drains slowly is the
/// entire backpressure story, so producing runs to completion and then <c>Completion</c> is awaited, no separate
/// consumer task required (no deadlock risk).
/// </summary>
[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class BackpressureBenchmarks
{
    private const int ItemCount = 2_000;
    private const int Capacity = 16;

    private static async Task SlowActionAsync(int _) => await Task.Yield();
    private static async ValueTask SlowActionValueAsync(int _) => await Task.Yield();

    [Benchmark(Baseline = true)]
    public async Task Dataflow()
    {
        var block = new DataflowAction(SlowActionAsync, new ExecutionDataflowBlockOptions { BoundedCapacity = Capacity });

        for (var i = 0; i < ItemCount; i++)
        {
            await block.SendAsync(i).ConfigureAwait(false);
        }
        block.Complete();
        await block.Completion.ConfigureAwait(false);
    }

    [Benchmark]
    public async Task Flux()
    {
        var block = new FluxAction(SlowActionValueAsync, new FluxBlockOptions { BoundedCapacity = Capacity });

        for (var i = 0; i < ItemCount; i++)
        {
            await block.SendAsync(i).ConfigureAwait(false);
        }
        block.Complete();
        await block.Completion.ConfigureAwait(false);
    }
}
