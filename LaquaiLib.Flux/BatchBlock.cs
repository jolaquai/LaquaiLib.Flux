using LaquaiLib.Flux.Diagnostics;
using LaquaiLib.Flux.Primitives;

namespace LaquaiLib.Flux;

/// <summary>
/// A Flux block that groups accepted items into pooled-array batches, flushed by count or by a hybrid timer.
/// <para/>
/// Each emitted <see cref="PooledBatch{T}"/> owns a distinct array rented from <see cref="ArrayPool{T}.Shared"/>;
/// consumers must dispose every batch they receive exactly once (see <see cref="PooledBatch{T}"/>'s remarks).
/// <para/>
/// <b>Broadcast fan-out warning:</b> <see cref="FluxFanOutMode.Broadcast"/> hands the very same
/// <see cref="PooledBatch{T}"/> value - the same underlying rented array - to every linked target. With more than
/// one target this means either every target disposing it (double-dispose, corrupting the pool) or no target
/// disposing it (a leaked rental), plus a shared mutable view across targets in the meantime. Do not broadcast a
/// <see cref="BatchBlock{T}"/>'s output to more than one target; single-link, <see cref="FluxFanOutMode.FirstAvailable"/>,
/// <see cref="FluxFanOutMode.RoundRobin"/>, and plain <see cref="IFluxSource{TOut}.ReceiveAllAsync"/> consumption
/// are all single-owner and safe. If multiple independent consumers of the same batch are unavoidable, have the
/// single receiving target call <see cref="PooledBatch{T}.Memory"/>.ToArray() and hand out copies instead.
/// <para/>
/// <see cref="FluxBlockOptions.MaxDegreeOfParallelism"/> is ignored: accumulating items into a batch is inherently
/// serial (there is one in-progress batch at a time).
/// <para/>
/// The in-progress buffer starts small and grows (doubling, capped at <c>batchSize</c>) only as far as batches
/// actually get filled, rather than always renting a <c>batchSize</c>-length array up front: a hybrid count/time
/// policy with a large count cap but consistently small time-triggered batches never rents more than it actually
/// uses. A workload that reliably fills batches to capacity converges to renting at <c>batchSize</c> after a
/// handful of amortized grow-copies and stays there for the rest of its lifetime.
/// </summary>
/// <typeparam name="T">The type of item accepted and batched by this block.</typeparam>
public sealed class BatchBlock<T> : PropagatorFluxBlockBase<T, PooledBatch<T>>
{
    private const int InitialCapacity = 16;

    private readonly int _batchSize;
    private readonly TimeSpan _flushInterval;

    private T[] _buffer;
    private int _capacity;
    private int _count;

    private readonly LockType _batchLock = new();
    private readonly SemaphoreSlim _writeGate = new(1, 1);

    // Read per item by ReaderLoopAsync to decide whether _batchLock is needed at all, so it must be assigned
    // before that loop is even queued - a plain `_timerLoop is not null` test would be read by a loop that was
    // started before _timerLoop's assignment, with no happens-before edge to make that assignment visible.
    private readonly bool _hasTimer;

    private readonly PeriodicTimer _timer;
    private readonly Task _readerLoop;
    private readonly Task _timerLoop;

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
        _batchSize = batchSize;
        _flushInterval = flushInterval;
        _capacity = Math.Min(_batchSize, InitialCapacity);
        _buffer = ArrayPool<T>.Shared.Rent(_capacity);
        _hasTimer = flushInterval != Timeout.InfiniteTimeSpan;
        if (_hasTimer)
        {
            _timer = new PeriodicTimer(_flushInterval);
        }

        _readerLoop = Task.Run(ReaderLoopAsync);
        if (_hasTimer)
        {
            _timerLoop = Task.Run(TimerLoopAsync);
        }

        _ = FinalizeAsync();
    }

    private async Task ReaderLoopAsync()
    {
        var reader = _input.Reader;
        try
        {
            while (await reader.WaitToReadAsync(_loopToken).ConfigureAwait(false))
            {
                while (reader.TryRead(out var item))
                {
                    // No timer loop means ReaderLoopAsync is the only thing that will ever touch _buffer/_capacity/
                    // _count - including via FlushAsync, which it only ever calls in-line, never concurrently with
                    // itself - so the lock has nothing to guard against and is skipped entirely.
                    bool full;
                    if (_hasTimer)
                    {
                        lock (_batchLock)
                        {
                            full = AppendCore(item);
                        }
                    }
                    else
                    {
                        full = AppendCore(item);
                    }
                    if (full)
                    {
                        await FlushAsync().ConfigureAwait(false);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            TrySetFault(ex);
            _input.Writer.TryComplete(ex);
        }
    }

    /// <summary>
    /// Appends <paramref name="item"/> to <see cref="_buffer"/>, growing it first if it is currently at capacity.
    /// Caller is responsible for holding <see cref="_batchLock"/> if a timer loop exists (see call sites).
    /// </summary>
    private bool AppendCore(T item)
    {
        if (_count == _capacity)
        {
            GrowBuffer();
        }
        _buffer[_count++] = item;
        return _count == _batchSize;
    }

    /// <summary>
    /// Doubles <see cref="_buffer"/>'s logical capacity (capped at <see cref="_batchSize"/>) and migrates the
    /// in-progress items into the new rental. Never called with <see cref="_capacity"/> already at
    /// <see cref="_batchSize"/>: reaching that count always flushes (via <see cref="AppendCore"/>'s return value)
    /// before another item can arrive to grow into.
    /// </summary>
    private void GrowBuffer()
    {
        var newCapacity = Math.Min(_capacity * 2, _batchSize);
        var newBuffer = ArrayPool<T>.Shared.Rent(newCapacity);
        Array.Copy(_buffer, newBuffer, _count);
        ArrayPool<T>.Shared.Return(_buffer, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
        _buffer = newBuffer;
        _capacity = newCapacity;
    }

    private async Task TimerLoopAsync()
    {
        try
        {
            while (await _timer.WaitForNextTickAsync(_loopToken).ConfigureAwait(false))
            {
                await FlushAsync().ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Disposal of _timer or cancellation of _loopToken during shutdown/fault; not an error.
        }
    }

    private async ValueTask FlushAsync()
    {
        // Acquired before detaching the in-progress buffer so detach order always matches write order: the
        // reader loop's count-triggered flush and the timer loop's time-triggered flush can race to get here,
        // but only one of them will ever see a non-empty buffer to detach.
        await _writeGate.WaitAsync(_loopToken).ConfigureAwait(false);
        try
        {
            T[] buf;
            int n;
            if (_hasTimer)
            {
                lock (_batchLock)
                {
                    if (!TryDetachBuffer(out buf, out n))
                    {
                        // Nothing to flush - the peer (reader-loop count-flush vs timer-loop time-flush) already took it.
                        return;
                    }
                }
            }
            else if (!TryDetachBuffer(out buf, out n))
            {
                return;
            }

            var batch = new PooledBatch<T>(buf, n);
            if (!_output.Writer.TryWrite(batch))
            {
                await _output.Writer.WriteAsync(batch, _loopToken).ConfigureAwait(false);
            }
            if (FluxMetrics.ItemsProcessed.Enabled)
            {
                FluxMetrics.ItemsProcessed.Add(n, _tags);
            }
        }
        finally
        {
            _writeGate.Release();
        }
    }

    /// <summary>
    /// Detaches the in-progress buffer for emission and rents its replacement, sized to the (possibly already
    /// grown) current <see cref="_capacity"/> rather than <see cref="_batchSize"/>. Caller is responsible for
    /// holding <see cref="_batchLock"/> if a timer loop exists (see call sites).
    /// </summary>
    private bool TryDetachBuffer(out T[] buf, out int n)
    {
        if (_count == 0)
        {
            buf = null;
            n = 0;
            return false;
        }
        buf = _buffer;
        n = _count;
        _buffer = ArrayPool<T>.Shared.Rent(_capacity);
        _count = 0;
        return true;
    }

    private void ReturnBufferOnFault()
    {
        lock (_batchLock)
        {
            if (_buffer is not null)
            {
                ArrayPool<T>.Shared.Return(_buffer, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
                _buffer = null;
                _count = 0;
            }
        }
    }

    private async Task FinalizeAsync()
    {
        await _readerLoop.ConfigureAwait(false);

        // Stop the timer before the final flush so no concurrent timer-triggered flush can race the finalizer's
        // own flush below.
        _timer?.Dispose();
        if (_timerLoop is not null)
        {
            await _timerLoop.ConfigureAwait(false);
        }

        var fault = ObservedFault;
        if (fault is null)
        {
            await FlushAsync().ConfigureAwait(false);
        }
        else
        {
            ReturnBufferOnFault();
        }

        _output.Writer.TryComplete(fault);
        ResolveCompletion(fault);
    }
}
