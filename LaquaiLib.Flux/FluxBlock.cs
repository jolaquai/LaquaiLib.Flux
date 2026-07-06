namespace LaquaiLib.Flux;

/// <summary>
/// Convenience members for <see cref="IFluxTarget{TIn}"/> and <see cref="IFluxSource{TOut}"/>, kept separate from
/// those interfaces so implementing a custom target or source only ever requires the minimal core contract -
/// mirrors how <see cref="System.Threading.Tasks.Dataflow.DataflowBlock"/> layers <c>Post</c>/<c>Receive</c>/etc.
/// on top of the minimal <c>ITargetBlock{T}</c>/<c>ISourceBlock{T}</c> protocol instead of growing those interfaces.
/// </summary>
public static class FluxBlock
{
    extension<TIn>(IFluxTarget<TIn> target)
    {
        /// <summary>
        /// Dataflow-familiar alias for <see cref="IFluxTarget{TIn}.TryOffer"/>: synchronously offers
        /// <paramref name="item"/> to <paramref name="target"/> without ever waiting for capacity.
        /// </summary>
        /// <param name="item">The item to offer.</param>
        /// <returns><see langword="true"/> if the item was accepted immediately; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Post(TIn item) => target.TryOffer(item);
    }
}
