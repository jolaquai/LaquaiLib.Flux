using System.Threading.Tasks.Dataflow;

using FluxTransform = LaquaiLib.Flux.TransformBlock<int, int>;
using DataflowTransform = System.Threading.Tasks.Dataflow.TransformBlock<int, int>;

namespace LaquaiLib.Flux.Benchmarks.TransformBlocks;

/// <summary>
/// End-to-end throughput through a linked multi-stage pipeline (a <see cref="StageCount"/>-deep LinkTo chain),
/// not just a single unlinked block - exercises the dispatch-loop/completion-propagation machinery against
/// Dataflow's own LinkTo/completion-propagation machinery, rather than just per-block processing.
/// </summary>
[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class PipelineBenchmarks
{
    private const int ItemCount = 20_000;
    private const int StageCount = 4;
    private const int Capacity = 1_000_000;

    private static int Stage(int x) => x + 1;

    [Benchmark(Baseline = true)]
    public async Task<int> Dataflow()
    {
        var stages = new DataflowTransform[StageCount];
        for (var i = 0; i < StageCount; i++)
        {
            stages[i] = new DataflowTransform(Stage, new ExecutionDataflowBlockOptions { BoundedCapacity = Capacity });
        }
        for (var i = 0; i < StageCount - 1; i++)
        {
            stages[i].LinkTo(stages[i + 1], new DataflowLinkOptions { PropagateCompletion = true });
        }

        for (var i = 0; i < ItemCount; i++)
        {
            await stages[0].SendAsync(i).ConfigureAwait(false);
        }
        stages[0].Complete();

        var last = stages[^1];
        var count = 0;
        while (await last.OutputAvailableAsync().ConfigureAwait(false))
        {
            while (last.TryReceive(out _))
            {
                count++;
            }
        }
        return count;
    }

    [Benchmark]
    public async Task<int> Flux()
    {
        var stages = new FluxTransform[StageCount];
        for (var i = 0; i < StageCount; i++)
        {
            stages[i] = new FluxTransform(Stage, new FluxBlockOptions { BoundedCapacity = Capacity });
        }
        for (var i = 0; i < StageCount - 1; i++)
        {
            stages[i].LinkTo(stages[i + 1]);
        }

        for (var i = 0; i < ItemCount; i++)
        {
            await stages[0].SendAsync(i).ConfigureAwait(false);
        }
        stages[0].Complete();

        var last = stages[^1];
        var count = 0;
        await foreach (var _ in last.ReceiveAllAsync())
        {
            count++;
        }
        return count;
    }
}
