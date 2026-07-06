namespace LaquaiLib.Flux.Diagnostics;

/// <summary>
/// Holds the process-wide <see cref="Meter"/> and every instrument Flux blocks report through. The meter is a
/// <see langword="static readonly"/> singleton created unconditionally at type load, not behind any opt-in flag,
/// so <c>dotnet-counters monitor LaquaiLib.Flux</c> or any OpenTelemetry <see cref="Meter"/>-name-based exporter
/// picks these up with zero extra configuration.
/// </summary>
internal static class FluxMetrics
{
    /// <summary>The name under which the shared <see cref="Meter"/> is registered.</summary>
    public const string MeterName = "LaquaiLib.Flux";

    /// <summary>
    /// The version tagged on the shared <see cref="Meter"/>. Kept in sync by hand with the package
    /// <c>&lt;Version&gt;</c> in the csproj; not read via reflection, per the project's AOT/trimming constraints.
    /// </summary>
    public const string MeterVersion = "0.1.0";

    /// <summary>The shared <see cref="Meter"/> instance every Flux block instrument is created from.</summary>
    public static readonly Meter Meter = new(MeterName, MeterVersion);

    /// <summary>Items accepted by a block via <see cref="IFluxTarget{TIn}.SendAsync"/>.</summary>
    public static readonly Counter<long> ItemsAccepted = Meter.CreateCounter<long>("flux.block.items_accepted", unit: "{item}", description: "Items accepted via SendAsync.");

    /// <summary>Items that finished processing within a block (transform/action completed).</summary>
    public static readonly Counter<long> ItemsProcessed = Meter.CreateCounter<long>("flux.block.items_processed", unit: "{item}", description: "Items that finished processing.");

    /// <summary>Items discarded, e.g. because a linked target could no longer accept them.</summary>
    public static readonly Counter<long> ItemsDropped = Meter.CreateCounter<long>("flux.block.items_dropped", unit: "{item}", description: "Items discarded without being fully processed.");

    /// <summary>Number of times <see cref="IFluxTarget{TIn}.SendAsync"/> suspended waiting for input channel capacity.</summary>
    public static readonly Counter<long> BackpressureEvents = Meter.CreateCounter<long>("flux.block.backpressure_events", unit: "{event}", description: "Times SendAsync suspended waiting for input channel capacity.");

    /// <summary>Wall-clock time to process a single item within a stage, in milliseconds.</summary>
    public static readonly Histogram<double> StageLatency = Meter.CreateHistogram<double>("flux.block.stage_latency", unit: "ms", description: "Time to process a single item within a stage.");

    /// <summary>Current count of items queued in a block's input channel, tagged by block name.</summary>
    public static readonly ObservableGauge<int> InputQueueDepth = Meter.CreateObservableGauge("flux.block.input_queue_depth", FluxBlockDiagnostics.ObserveInputQueueDepths, unit: "{item}", description: "Current count of items queued in a block's input channel.");

    /// <summary>Current count of items queued in a block's output channel, tagged by block name.</summary>
    public static readonly ObservableGauge<int> OutputQueueDepth = Meter.CreateObservableGauge("flux.block.output_queue_depth", FluxBlockDiagnostics.ObserveOutputQueueDepths, unit: "{item}", description: "Current count of items queued in a block's output channel.");
}
