using LaquaiLib.Flux.Primitives;

namespace LaquaiLib.Flux;

/// <summary>
/// A Flux block that buffers items without transformation.
/// Note that most features of the library (parallelization capabilities built into blocks, many-to-one composition semantics, etc.) make this block effectively a "no-op" in many scenarios.
/// Its primary use is as the ingress point for a Flux pipeline.
/// <para/>
/// Because the block does no work between input and output, it holds exactly <b>one</b> channel: producers write
/// it, and <see cref="IFluxSource{TOut}.ReceiveAllAsync"/> and linked targets read that same channel. There is no
/// pump loop relaying items between two buffers, so <see cref="FluxBlockOptions.BoundedCapacity"/> means precisely
/// that many items resident, and an item costs one enqueue and one dequeue for its whole trip through the block.
/// <para/>
/// <see cref="FluxBlockOptions.MaxDegreeOfParallelism"/> is deliberately ignored: there is nothing to parallelize
/// when the block's entire job is holding items.
/// <para/>
/// <see cref="IFluxBlock.Completion"/> resolves once the block has been completed <i>and</i> its buffer has
/// drained - a completed buffer still holding items nobody has taken is not yet done. Consequently a
/// <see cref="IFluxTarget{TIn}.Complete"/>d block with neither a linked target nor a
/// <see cref="IFluxSource{TOut}.ReceiveAllAsync"/> consumer stays pending until something drains it.
/// <see cref="Fault"/> is not subject to this: it discards what is buffered, per
/// <see cref="IFluxTarget{TIn}.Fault"/>'s contract, and resolves immediately.
/// </summary>
/// <typeparam name="T">The type of item buffered by this block.</typeparam>
public sealed class BufferBlock<T> : PropagatorFluxBlockBase<T, T>
{
    /// <summary>
    /// Initializes a new <see cref="BufferBlock{T}"/>.
    /// </summary>
    /// <param name="options">Options controlling capacity and diagnostics. <see langword="null"/> uses <see cref="FluxBlockOptions.Default"/>.</param>
    public BufferBlock(FluxBlockOptions options = null)
        : base(options, inputSingleReader: true, outputSingleWriter: false, aliasOutputToInput: true, countAcceptedAsProcessed: true)
    {
        _ = FinalizeAsync();
    }

    /// <inheritdoc/>
    public override void Fault(Exception exception)
    {
        base.Fault(exception);
        DiscardBuffered();
    }

    /// <summary>
    /// Empties the buffer so a faulted or canceled block settles immediately.
    /// <para/>
    /// A bounded <see cref="Channel{T}"/> defers its reader-side completion signal until its queue is empty even
    /// when it was completed <i>with</i> an error, so without this a faulted block would sit pending until some
    /// consumer happened to drain it. Discarding is also exactly what <see cref="IFluxTarget{TIn}.Fault"/> already
    /// promises, and matches Dataflow, where faulting a block loses its buffered messages.
    /// </summary>
    private void DiscardBuffered()
    {
        var reader = _input.Reader;
        while (reader.TryRead(out _))
        {
        }
    }

    private async Task FinalizeAsync()
    {
        try
        {
            var completion = _input.Reader.Completion;
            // With no pump loop awaiting the channel on _loopToken, this is the block's ONLY chance to observe
            // cancellation - nothing else here would ever complete the channel, so Completion would hang instead
            // of resolving Canceled. _loopToken is None precisely when nothing can ever cancel (see
            // TargetFluxBlockBase._loopToken), so the wrapper is only paid for when there is a token to watch.
            await (_loopToken.CanBeCanceled ? completion.WaitAsync(_loopToken) : completion).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            TrySetFault(ex);
            _input.Writer.TryComplete(ex);
            DiscardBuffered();
        }
        ResolveCompletion(ObservedFault);
    }
}
