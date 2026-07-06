using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;

namespace LaquaiLib.Flux.Benchmarks;

/// <summary>
/// Shared BenchmarkDotNet configuration for every benchmark in this suite: memory diagnostics (allocations/op)
/// plus p90/p95/max columns, since the adoption bar this project holds itself to explicitly calls for
/// allocations/op, ns/op, and tail latency under backpressure - not just mean throughput.
/// <see cref="StatisticColumn"/> tops out at <see cref="StatisticColumn.P95"/> below <see cref="StatisticColumn.Max"/>
/// (no built-in p99), so <see cref="StatisticColumn.Max"/> stands in as the tail-most percentile available.
/// <para/>
/// Runs out-of-process (BenchmarkDotNet's default toolchain) on net10.0: BenchmarkDotNet 0.15.8's SDK/runtime
/// validator doesn't yet recognize net11.0 preview as a launchable runtime moniker, and the in-process toolchain
/// that sidesteps that validation refuses to run anything slow enough to risk stalling the harness's own
/// process - which several of these benchmarks (backpressure, multi-stage pipelines) genuinely are.
/// </summary>
public sealed class BenchmarkConfig : ManualConfig
{
    public BenchmarkConfig()
    {
        AddDiagnoser(MemoryDiagnoser.Default);
        AddColumn(StatisticColumn.P90);
        AddColumn(StatisticColumn.P95);
        AddColumn(StatisticColumn.Max);
    }
}
