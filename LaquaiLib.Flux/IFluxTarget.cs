namespace LaquaiLib.Flux;

/// <summary>
/// Represents the input side of a Flux block: something that accepts items of type <typeparamref name="TIn"/>.
/// </summary>
/// <typeparam name="TIn">The type of item accepted by this target.</typeparam>
public interface IFluxTarget<in TIn> : IFluxBlock
{
    /// <summary>
    /// Asynchronously offers <paramref name="item"/> to this target.
    /// <para/>
    /// Completes synchronously, without allocating, whenever the target's internal channel has free capacity;
    /// only suspends when the channel is full (backpressure).
    /// </summary>
    /// <param name="item">The item to send.</param>
    /// <param name="cancellationToken">A token that cancels the wait for capacity. Does not cancel or roll back an item that was already accepted.</param>
    /// <returns>
    /// A <see cref="ValueTask{TResult}"/> resolving to <see langword="true"/> if the item was accepted, or
    /// <see langword="false"/> if this target already completed or faulted and can no longer accept items.
    /// </returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled while waiting for capacity.</exception>
    public ValueTask<bool> SendAsync(TIn item, CancellationToken cancellationToken = default);

    /// <summary>
    /// Synchronously offers <paramref name="item"/> to this target without ever waiting for capacity: this is the
    /// genuine, zero-wait accept-or-decline primitive <see cref="SendAsync"/>'s fast path is itself built on, not
    /// a wrapper over it. A decline is immediate and final - nothing continues in the background afterward.
    /// </summary>
    /// <param name="item">The item to offer.</param>
    /// <returns><see langword="true"/> if the item was accepted immediately; otherwise, <see langword="false"/>.</returns>
    public bool TryOffer(TIn item);

    /// <summary>
    /// Signals that no further items will be sent to this target. Items already accepted continue draining normally.
    /// Idempotent: calling this more than once has no additional effect.
    /// </summary>
    public void Complete();

    /// <summary>
    /// Transitions this target to a faulted state, discarding any items still queued internally and propagating
    /// <paramref name="exception"/> to <see cref="IFluxBlock.Completion"/> and to any linked downstream target
    /// (see <see cref="IFluxSource{TOut}.LinkTo"/>) regardless of that link's completion-propagation setting.
    /// Idempotent: only the first call has any effect, and only its exception is observed on <see cref="IFluxBlock.Completion"/>.
    /// </summary>
    /// <param name="exception">The exception describing the fault.</param>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is <see langword="null"/>.</exception>
    public void Fault(Exception exception);
}
