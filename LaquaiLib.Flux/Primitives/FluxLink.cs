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
/// The <see cref="IDisposable"/> handle returned by <see cref="IFluxSource{TOut}.LinkTo"/>: an entry in a list
/// owned by the source. Multiple links may be active simultaneously; <see cref="FluxBlockOptions.FanOutMode"/>
/// governs how the dispatch loop routes items across them.
/// </summary>
/// <typeparam name="TOut">The linked element type.</typeparam>
internal sealed class FluxLink<TOut>(IFluxLinkOwner<TOut> owner, IFluxTarget<TOut> target, FluxLinkOptions options, Func<TOut, bool> filter) : IDisposable
{
    /// <summary>The target this link pushes items to.</summary>
    public IFluxTarget<TOut> Target { get; } = target;

    /// <summary>The options this link was created with.</summary>
    public FluxLinkOptions Options { get; } = options;

    /// <summary>The predicate an item must satisfy to be routed through this link, or <see langword="null"/> to match every item.</summary>
    public Func<TOut, bool> Filter { get; } = filter;

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
