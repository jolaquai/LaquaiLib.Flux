namespace LaquaiLib.Flux.Primitives;

/// <summary>
/// Implemented by a <see cref="PropagatorFluxBlockBase{TIn, TOut}"/> so a <see cref="FluxLink{TOut}"/> can detach
/// itself on <see cref="IDisposable.Dispose"/> without needing to know the owner's input element type.
/// </summary>
/// <typeparam name="TOut">The linked element type.</typeparam>
internal interface IFluxLinkOwner<TOut>
{
    /// <summary>Removes <paramref name="link"/> from this source's active links.</summary>
    public void Unlink(FluxLink<TOut> link);
}

/// <summary>
/// The <see cref="IDisposable"/> handle returned by <see cref="IFluxSource{TOut}.LinkTo"/>. Modeled as an entry in
/// a list (owned by the source) rather than a single field, even though v1 enforces at most one live entry per
/// source, so v2's real load-balanced fan-out only changes the internal dispatch loop that iterates the list,
/// never this type or the public <see cref="IFluxSource{TOut}.LinkTo"/>/<see cref="FluxLinkOptions"/> contract.
/// </summary>
/// <typeparam name="TOut">The linked element type.</typeparam>
internal sealed class FluxLink<TOut>(IFluxLinkOwner<TOut> owner, IFluxTarget<TOut> target, FluxLinkOptions options) : IDisposable
{
    /// <summary>The target this link pushes items to.</summary>
    public IFluxTarget<TOut> Target { get; } = target;

    /// <summary>The options this link was created with.</summary>
    public FluxLinkOptions Options { get; } = options;

    private int _disposed;

    /// <summary>Detaches this link from its owning source. Idempotent.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            owner.Unlink(this);
        }
    }
}
