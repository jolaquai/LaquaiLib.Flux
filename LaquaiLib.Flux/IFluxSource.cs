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
    /// v1 restriction: a source may have at most one active link at a time. Calling this while a link is already
    /// active throws; dispose the <see cref="IDisposable"/> returned by the prior call first.
    /// </summary>
    /// <param name="target">The target to link to.</param>
    /// <param name="linkOptions">Options controlling completion propagation for this link. <see langword="null"/> uses <see cref="FluxLinkOptions.Default"/>.</param>
    /// <returns>An <see cref="IDisposable"/> that, when disposed, unlinks <paramref name="target"/> from this source.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="target"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">This source already has an active link. v1 permits exactly one linked target per source.</exception>
    public IDisposable LinkTo(IFluxTarget<TOut> target, FluxLinkOptions linkOptions = null);

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
