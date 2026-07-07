namespace LaquaiLib.Flux;

/// <summary>
/// Configures the behavior of a single <see cref="IFluxSource{TOut}.LinkTo"/> link.
/// </summary>
public sealed class FluxLinkOptions
{
    /// <summary>
    /// Gets a shared instance carrying every default.
    /// </summary>
    public static readonly FluxLinkOptions Default = new();

    /// <summary>
    /// Gets or sets whether the upstream source completing automatically propagates to the linked
    /// target by calling its <see cref="IFluxTarget{TIn}.Complete"/> once all in-flight items have drained.
    /// Defaults to <see langword="true"/>.
    /// <para/>
    /// Fault and cancellation propagation are never gated by this flag: a faulted or canceled upstream always
    /// faults the linked target, since leaving a downstream block waiting forever after an upstream fault is never correct.
    /// </summary>
    public bool PropagateCompletion { get; init; } = true;
}
