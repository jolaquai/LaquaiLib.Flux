namespace LaquaiLib.Flux;

/// <summary>
/// Represents the output side of a Flux block: something that produces items of type <typeparamref name="TOut"/>,
/// either for direct pull-based consumption or for push-based delivery to a linked <see cref="IFluxTarget{TIn}"/>.
/// </summary>
/// <typeparam name="TOut">The type of item produced by this source.</typeparam>
public interface IFluxSource<out TOut> : IFluxBlock
{
    /// <summary>
    /// Links this source's output to <paramref name="target"/>, so every produced item is pushed to it directly
    /// instead of requiring a manual <see cref="ReceiveAllAsync"/> consumer loop.
    /// <para/>
    /// Multiple links may be active on the same source simultaneously (including more than one link to the same
    /// <paramref name="target"/> instance, e.g. distinguished only by <paramref name="filter"/>). Once more than
    /// one link is active, <see cref="FluxBlockOptions.FanOutMode"/> determines how each produced item is routed
    /// across them; with 0 or 1 links active, behavior is unaffected by that setting.
    /// </summary>
    /// <param name="target">The target to link to.</param>
    /// <param name="linkOptions">Options controlling completion propagation for this link. <see langword="null"/> uses <see cref="FluxLinkOptions.Default"/>.</param>
    /// <param name="filter">
    /// An optional predicate an item must satisfy to be routed to <paramref name="target"/> via this link.
    /// <see langword="null"/> (the default) matches every item. An item matching no active link's filter is
    /// dropped, same as a link whose target has permanently completed or faulted.
    /// </param>
    /// <returns>An <see cref="IDisposable"/> that, when disposed, unlinks <paramref name="target"/> from this source.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="target"/> is <see langword="null"/>.</exception>
    public IDisposable LinkTo(IFluxTarget<TOut> target, FluxLinkOptions linkOptions = null, Func<TOut, bool> filter = null);

    /// <summary>
    /// Returns an <see cref="IAsyncEnumerable{T}"/> that pulls items directly from this source, bypassing the
    /// push-based <see cref="LinkTo"/> mechanism. Intended for terminal consumption (e.g. <c>await foreach</c>)
    /// rather than for wiring further blocks; use <see cref="LinkTo"/> for block-to-block composition.
    /// <para/>
    /// Enumerating this while a link is simultaneously active races the linked target for items; do not mix
    /// the two consumption modes against the same source instance.
    /// </summary>
    /// <param name="cancellationToken">A token observed while awaiting the next available item.</param>
    /// <returns>An async sequence over every item this source produces, terminating when the source completes and faulting the enumeration if the source faults.</returns>
    public IAsyncEnumerable<TOut> ReceiveAllAsync(CancellationToken cancellationToken = default);
}
