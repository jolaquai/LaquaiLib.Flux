using LaquaiLib.Flux.Primitives;

namespace LaquaiLib.Flux;

/// <summary>
/// A Flux block that groups accepted items into pooled-array batches, flushed by count or by a hybrid timer.
/// </summary>
/// <typeparam name="T">The type of item accepted and batched by this block.</typeparam>
public sealed class BatchBlock<T> : PropagatorFluxBlockBase<T, T[]>
{
    /// <summary>
    /// Initializes a new <see cref="BatchBlock{T}"/> with the given batch size and no time-based flush.
    /// </summary>
    /// <param name="batchSize">The maximum number of items per emitted batch.</param>
    public BatchBlock(int batchSize) : this(batchSize, Timeout.InfiniteTimeSpan, FluxBlockOptions.Default)
    {
    }

    /// <summary>
    /// Initializes a new <see cref="BatchBlock{T}"/> with a hybrid count/time flush policy.
    /// </summary>
    /// <param name="batchSize">The maximum number of items per emitted batch.</param>
    /// <param name="flushInterval">
    /// The maximum time to wait before flushing a partial batch, driven by an internal <see cref="PeriodicTimer"/>.
    /// <see cref="Timeout.InfiniteTimeSpan"/> disables time-based flushing (count-only, matching <see cref="System.Threading.Tasks.Dataflow.BatchBlock{T}"/> semantics).
    /// </param>
    /// <param name="options">Options controlling capacity and diagnostics. <see langword="null"/> uses <see cref="FluxBlockOptions.Default"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="batchSize"/> is less than 1.</exception>
    public BatchBlock(int batchSize, TimeSpan flushInterval, FluxBlockOptions options = null)
        : base(options, inputSingleReader: true, outputSingleWriter: true)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        throw new NotImplementedException("BatchBlock<T> is not yet implemented.");
    }
}
