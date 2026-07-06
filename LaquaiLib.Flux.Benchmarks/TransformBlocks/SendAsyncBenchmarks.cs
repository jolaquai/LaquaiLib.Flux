using System.Threading.Tasks.Dataflow;

using FluxTransform = LaquaiLib.Flux.TransformBlock<int, int>;
using DataflowTransform = System.Threading.Tasks.Dataflow.TransformBlock<int, int>;

namespace LaquaiLib.Flux.Benchmarks.TransformBlocks;

/// <summary>
/// Micro-benchmarks isolating the cost of a single accept call when capacity is always available - the exact
/// fast path the plan is built around: Flux's <see cref="ValueTask{TResult}"/>-based <c>SendAsync</c> should
/// complete synchronously without allocating, unlike Dataflow's <c>SendAsync</c>, which allocates a
/// <see cref="Task{TResult}"/> per call regardless of whether the target had room. <c>Post</c>/<c>TryOffer</c>
/// isolate the fully synchronous accept-or-decline path on both sides.
/// </summary>
[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class SendAsyncBenchmarks
{
    private FluxTransform _flux;
    private DataflowTransform _dataflow;

    [GlobalSetup]
    public void Setup()
    {
        // Effectively unbounded capacity with an always-draining consumer so neither side ever has to wait -
        // isolates per-call overhead rather than backpressure behavior (see BackpressureBenchmarks for that).
        _flux = new FluxTransform(static x => x, new FluxBlockOptions { BoundedCapacity = 1_000_000 });
        _ = DrainForeverAsync(_flux);

        _dataflow = new DataflowTransform(static x => x, new ExecutionDataflowBlockOptions { BoundedCapacity = 1_000_000 });
        _ = DrainForeverAsync(_dataflow);
    }

    private static async Task DrainForeverAsync(FluxTransform block)
    {
        await foreach (var _ in block.ReceiveAllAsync())
        {
        }
    }

    private static async Task DrainForeverAsync(DataflowTransform block)
    {
        while (await block.OutputAvailableAsync().ConfigureAwait(false))
        {
            while (block.TryReceive(out _))
            {
            }
        }
    }

    [Benchmark(Baseline = true)]
    public Task<bool> Dataflow_SendAsync() => _dataflow.SendAsync(1);

    [Benchmark]
    public ValueTask<bool> Flux_SendAsync() => _flux.SendAsync(1);

    [Benchmark]
    public bool Dataflow_Post() => _dataflow.Post(1);

    [Benchmark]
    public bool Flux_Post() => _flux.Post(1);
}
