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
/// </summary>
/// <typeparam name="T">The type of item accepted and batched by this block.</typeparam>
public sealed class BatchBlock<T> : PropagatorFluxBlockBase<T, PooledBatch<T>>
{
    private readonly int _batchSize;
    private readonly TimeSpan _flushInterval;

    private T[] _buffer;
    private int _count;

    private readonly LockType _batchLock = new();
    private readonly SemaphoreSlim _writeGate = new(1, 1);

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
        _buffer = ArrayPool<T>.Shared.Rent(_batchSize);

        _readerLoop = Task.Run(ReaderLoopAsync);

        if (_flushInterval != Timeout.InfiniteTimeSpan)
        {
            _timer = new PeriodicTimer(_flushInterval);
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
                    bool full;
                    lock (_batchLock)
                    {
                        _buffer[_count++] = item;
                        full = _count == _batchSize;
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
            lock (_batchLock)
            {
                if (_count == 0)
                {
                    // Nothing to flush - the peer (reader-loop count-flush vs timer-loop time-flush) already took it.
                    return;
                }
                buf = _buffer;
                n = _count;
                _buffer = ArrayPool<T>.Shared.Rent(_batchSize);
                _count = 0;
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
