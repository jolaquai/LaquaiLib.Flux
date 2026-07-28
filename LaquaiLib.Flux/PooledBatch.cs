namespace LaquaiLib.Flux;

/// <summary>
/// A batch of items backed by an array rented from <see cref="ArrayPool{T}.Shared"/>, produced by <see cref="BatchBlock{T}"/>.
/// <para/>
/// <b>Ownership contract:</b> each <see cref="PooledBatch{T}"/> value owns a distinct rented array and must be
/// disposed <i>exactly once</i>, by whichever single consumer ends up owning it, to return that array to the
/// pool. Do not read <see cref="Memory"/>, <see cref="Span"/>, or the indexer after calling <see cref="Dispose"/> -
/// the backing array may already have been re-rented to an unrelated caller by then. Disposing more than once is
/// undefined behavior: the same array could be returned to the pool twice, so two unrelated future rents could
/// alias the same array. If a batch's data needs to outlive disposal (or outlive the batch being handed to more
/// than one owner), use <see cref="ToArray"/>.
/// <para/>
/// This is a mutable, mutable-underlying-array struct, not a <c>ReadOnly</c> view: a given rented array is handed
/// to exactly one logical owner under normal (non-broadcast) routing. See <see cref="BatchBlock{T}"/>'s remarks
/// for why broadcasting a <see cref="PooledBatch{T}"/> to multiple targets is unsafe and unsupported.
/// </summary>
/// <typeparam name="T">The type of item contained in the batch.</typeparam>
public readonly struct PooledBatch<T> : IDisposable
{
    private readonly T[] _array;
    private readonly int _count;

    internal PooledBatch(T[] array, int count)
    {
        _array = array;
        _count = count;
    }

    /// <summary>Gets the number of items in this batch.</summary>
    public int Count => _count;

    /// <summary>
    /// Gets a <see cref="Memory{T}"/> view over this batch's items, sized to exactly <see cref="Count"/>
    /// regardless of the rented backing array's actual (possibly larger, bucket-rounded) length.
    /// <para/>
    /// <see cref="Memory{T}"/> is the primary handout rather than <see cref="Span"/> because batches are
    /// typically consumed in async contexts (e.g. after an <see langword="await"/> in a drain loop); unlike
    /// <see cref="Span{T}"/>, <see cref="Memory{T}"/> is not a ref struct and can cross an await boundary.
    /// </summary>
    public Memory<T> Memory => new(_array, 0, _count);

    /// <summary>
    /// Gets a <see cref="Span{T}"/> view over this batch's items, sized to exactly <see cref="Count"/>. A
    /// zero-cost view intended for a purely synchronous consuming scope; being a ref struct, it cannot be stored
    /// in a field or survive an <see langword="await"/> - use <see cref="Memory"/> for that.
    /// </summary>
    public Span<T> Span => new(_array, 0, _count);

    /// <summary>Gets the item at <paramref name="index"/>.</summary>
    /// <param name="index">The zero-based index of the item to get.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="index"/> is negative or not less than <see cref="Count"/>.</exception>
    public T this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _count);
            return _array[index];
        }
    }

    /// <summary>
    /// Copies the elements of this batch into a new array of <typeparamref name="T"/> and returns that array.
    /// </summary>
    /// <returns>The created array containing the elements of this batch.</returns>
    public T[] ToArray() => Span.ToArray();

    /// <summary>
    /// Returns this batch's backing array to <see cref="ArrayPool{T}.Shared"/>. Must be called exactly once; see
    /// the type-level remarks for the double-dispose and post-dispose access hazards.
    /// </summary>
    public void Dispose() => ArrayPool<T>.Shared.Return(_array, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
}
