using System.Numerics;

using LaquaiLib.Flux.Diagnostics;
using LaquaiLib.Flux.Primitives;

namespace LaquaiLib.Flux;

/// <summary>
/// A Flux block that asynchronously transforms each accepted <typeparamref name="TIn"/> item into a
/// <typeparamref name="TOut"/> item, optionally processing multiple items concurrently via <see cref="FluxBlockOptions.MaxDegreeOfParallelism"/>.
/// <para/>
/// By default (<see cref="FluxBlockOptions.EnsureOrdered"/> <see langword="false"/>), output order is unrelated
/// to input order once <see cref="FluxBlockOptions.MaxDegreeOfParallelism"/> exceeds 1 - this is the fast path,
/// and it carries zero extra bookkeeping. Opting into <see cref="FluxBlockOptions.EnsureOrdered"/> at
/// <see cref="FluxBlockOptions.MaxDegreeOfParallelism"/> greater than 1 still runs transforms concurrently, but
/// funnels results back into input order through a fixed-size ring of reorder slots that never allocates per
/// item (see remarks on <see cref="StartOrderedPipeline"/>).
/// </summary>
/// <typeparam name="TIn">The type of item accepted by this block.</typeparam>
/// <typeparam name="TOut">The type of item produced by this block.</typeparam>
public sealed class TransformBlock<TIn, TOut> : PropagatorFluxBlockBase<TIn, TOut>
{
    private readonly Func<TIn, ValueTask<TOut>> _transform;
    private readonly int _maxDegreeOfParallelism;
    private readonly bool _ensureOrdered;

    private readonly Task[] _workers;

    // Ordered path only - see StartOrderedPipeline for the full design.
    private OrderedSlot[] _slots;
    private int _ringMask;
    private SemaphoreSlim _slotSem;
    private long _readSeq;   // next seq to assign on claim; mutated only under _seqLock
    private long _emitSeq;   // next seq to emit; mutated only by whichever worker currently owns the drain
    private int _drainOwned; // 0/1 CAS flag electing a single drainer at a time
    private LockType _seqLock;

    /// <summary>
    /// One reorder slot. Workers write <see cref="Value"/> first and then publish it with a release-store of 1
    /// into <see cref="State"/>; the drainer acquires via <see cref="State"/> and resets it to 0 once consumed.
    /// </summary>
    private struct OrderedSlot
    {
        public TOut Value;
        public int State;
    }

    /// <summary>
    /// Initializes a new <see cref="TransformBlock{TIn, TOut}"/> using a synchronous transform delegate.
    /// </summary>
    /// <param name="transform">The synchronous transform applied to each accepted item.</param>
    /// <param name="options">Options controlling capacity, parallelism, ordering and diagnostics. <see langword="null"/> uses <see cref="FluxBlockOptions.Default"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="transform"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public TransformBlock(Func<TIn, TOut> transform, FluxBlockOptions options = null)
        : this(WrapSync(transform), options)
    {
    }

    /// <summary>
    /// Initializes a new <see cref="TransformBlock{TIn, TOut}"/> using an asynchronous transform delegate.
    /// </summary>
    /// <param name="transform">The asynchronous transform applied to each accepted item.</param>
    /// <param name="options">Options controlling capacity, parallelism, ordering and diagnostics. <see langword="null"/> uses <see cref="FluxBlockOptions.Default"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="transform"/> is <see langword="null"/>.</exception>
    public TransformBlock(Func<TIn, ValueTask<TOut>> transform, FluxBlockOptions options = null)
        : base(options, ComputeInputSingleReader(options), ComputeOutputSingleWriter(options))
    {
        ArgumentNullException.ThrowIfNull(transform);
        _transform = transform;
        _maxDegreeOfParallelism = GetMaxDegreeOfParallelism(_options);
        _ensureOrdered = _options.EnsureOrdered && _maxDegreeOfParallelism > 1;

        _workers = new Task[_maxDegreeOfParallelism];
        if (_ensureOrdered)
        {
            StartOrderedPipeline();
        }
        else
        {
            for (var i = 0; i < _workers.Length; i++)
            {
                _workers[i] = Task.Run(UnorderedWorkerLoopAsync);
            }
            _ = FinalizeUnorderedAsync();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Func<TIn, ValueTask<TOut>> WrapSync(Func<TIn, TOut> transform)
    {
        ArgumentNullException.ThrowIfNull(transform);
        return item => new ValueTask<TOut>(transform(item));
    }

    // The upper clamp only exists so the ordered ring's power-of-two size math cannot overflow; a DOP anywhere
    // near it is unrunnable anyway (the worker Task array alone would be gigabytes).
    private static int GetMaxDegreeOfParallelism(FluxBlockOptions options) => Math.Clamp((options ?? FluxBlockOptions.Default).MaxDegreeOfParallelism, 1, 1 << 20);

    // Input channel keeps SingleReader = true only at DOP 1: both the unordered DOP > 1 path and the ordered path
    // now have every worker reading (and, when idle, waiting on) the input channel concurrently.
    private static bool ComputeInputSingleReader(FluxBlockOptions options) => GetMaxDegreeOfParallelism(options) == 1;

    // Output channel keeps SingleWriter = true whenever only one internal loop ever writes to it: at DOP 1, or in
    // ordered mode (the emitter loop is the sole writer). Only the unordered, DOP > 1 case has multiple
    // concurrent workers genuinely writing to _output independently.
    private static bool ComputeOutputSingleWriter(FluxBlockOptions options) => GetMaxDegreeOfParallelism(options) == 1 || (options ?? FluxBlockOptions.Default).EnsureOrdered;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask<TOut> TransformMeasuredAsync(TIn item)
    {
        if (FluxMetrics.StageLatency.Enabled)
        {
            return TransformTimedAsync(item);
        }
        // No listener: skip the two Stopwatch.GetTimestamp calls entirely; on trivial transforms they cost as
        // much as the transform itself.
        return _transform(item);
    }

    private async ValueTask<TOut> TransformTimedAsync(TIn item)
    {
        var start = Stopwatch.GetTimestamp();
        var result = await _transform(item).ConfigureAwait(false);
        FluxMetrics.StageLatency.Record(Stopwatch.GetElapsedTime(start).TotalMilliseconds, _tags);
        return result;
    }

    // ─────────────────────────────────────────────
    //  Unordered path (default) - N independent workers, no ordering bookkeeping at all.
    // ─────────────────────────────────────────────

    private async Task UnorderedWorkerLoopAsync()
    {
        var reader = _input.Reader;
        var writer = _output.Writer;
        try
        {
            while (await reader.WaitToReadAsync(_loopToken).ConfigureAwait(false))
            {
                while (reader.TryRead(out var item))
                {
                    var result = await TransformMeasuredAsync(item).ConfigureAwait(false);
                    if (!writer.TryWrite(result))
                    {
                        await writer.WriteAsync(result, _loopToken).ConfigureAwait(false);
                    }
                    if (FluxMetrics.ItemsProcessed.Enabled)
                    {
                        FluxMetrics.ItemsProcessed.Add(1, _tags);
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

    private async Task FinalizeUnorderedAsync()
    {
        await Task.WhenAll(_workers).ConfigureAwait(false);
        var fault = ObservedFault;
        _output.Writer.TryComplete(fault);
        ResolveCompletion(fault);
    }

    // ─────────────────────────────────────────────
    //  Ordered path (EnsureOrdered + DOP > 1) - workers claim (item, seq) pairs from the input under a tiny lock,
    //  transform concurrently, and publish results into a power-of-two ring of slots indexed by seq. There is no
    //  dedicated emitter: after publishing, whichever worker completes the item the drain is stalled on becomes
    //  the sole drainer (elected by a CAS) and flushes contiguously ready slots to _output in order. This avoids
    //  the per-item continuation hop a separate emitter would pay - emitting is far cheaper than a transform, so a
    //  dedicated emitter would suspend on almost every item. On the common path (output has room) draining runs
    //  fully synchronously and allocates nothing; the previous Task<TOut>-per-item hand-off allocated ~100 B/item.
    // ─────────────────────────────────────────────

    // Reorder-ring memory is capped here regardless of a very large BoundedCapacity. A stream longer than the ring
    // that arrives faster than it drains will pin the window at the ring and force a worker to block (allocating)
    // per claim, so the ring wants to be at least as large as the realistic in-flight burst; past ~2^16 the win
    // flattens while the array cost keeps growing, so this is the trade-off point.
    private const int MaxOrderedRing = 1 << 16;

    private void StartOrderedPipeline()
    {
        // Size the reorder ring to what the caller already asked to buffer (BoundedCapacity), not a fixed small
        // constant: workers claim as far ahead as the input lets them, so a ring smaller than that in-flight burst
        // just converts into per-claim blocking (and an allocation per block). Floor keeps it clear of DOP; cap
        // bounds worst-case array memory. Power of two throughout for mask indexing.
        var floor = BitOperations.RoundUpToPowerOf2(Math.Max(64u, 2u * (uint)_maxDegreeOfParallelism));
        var wanted = Math.Min(BitOperations.RoundUpToPowerOf2((uint)_options.BoundedCapacity), (uint)MaxOrderedRing);
        var ringSize = (int)Math.Max(floor, wanted);
        _ringMask = ringSize - 1;
        _slots = new OrderedSlot[ringSize];

        // Slot reuse is bounded by a counting semaphore: a worker acquires one of ringSize permits before
        // claiming a seq and the drainer releases one per emit, so the live range [emitSeq, readSeq) never
        // exceeds ringSize and two seqs never alias onto one slot. A counting semaphore is used deliberately - a
        // binary "ring advanced" signal cannot wake N blocked workers without coalescing (and losing) wakeups.
        _slotSem = new SemaphoreSlim(ringSize, ringSize);
        _seqLock = new LockType();

        for (var i = 0; i < _workers.Length; i++)
        {
            _workers[i] = Task.Run(OrderedWorkerLoopAsync);
        }
        _ = FinalizeOrderedAsync();
    }

    private async Task OrderedWorkerLoopAsync()
    {
        var reader = _input.Reader;
        try
        {
            while (true)
            {
                // Reserve a slot before claiming a seq. On fault _completionCts is canceled, throwing here so a
                // worker parked with the ring full still unwinds. This wait observes the CTS rather than
                // _loopToken precisely so the fault path needs no extra wake plumbing for it.
                await _slotSem.WaitAsync(_completionCts.Token).ConfigureAwait(false);

                TIn item;
                long seq;
                lock (_seqLock)
                {
                    if (reader.TryRead(out item))
                    {
                        seq = _readSeq;
                        Volatile.Write(ref _readSeq, seq + 1);
                    }
                    else
                    {
                        seq = -1;
                    }
                }

                if (seq < 0)
                {
                    _slotSem.Release();
                    if (!await reader.WaitToReadAsync(_loopToken).ConfigureAwait(false))
                    {
                        break;
                    }
                    continue;
                }

                var result = await TransformMeasuredAsync(item).ConfigureAwait(false);

                var slot = (int)(seq & _ringMask);
                _slots[slot].Value = result;
                Volatile.Write(ref _slots[slot].State, 1);

                // Publish done - now try to drain in order. Whichever worker completes the item the drain is
                // currently stalled on becomes the drainer and flushes as far as it can; the others just return.
                // There is no dedicated emitter loop, so nothing suspends per item: on the common path (output has
                // room) DrainInOrderAsync runs start to finish synchronously and allocates nothing.
                await DrainInOrderAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            FaultOrderedPipeline(ex);
        }
    }

    // Drains contiguously ready slots from _emitSeq to _output, in order, on whichever worker thread calls in.
    // At most one thread drains at a time (the _drainOwned CAS), so _output sees strictly serialized writes and
    // _emitSeq is only ever advanced by the current owner. Consecutive owners are ordered by the release-store /
    // CAS-acquire on _drainOwned, so each new owner observes the prior owner's slot and channel writes.
    private async Task DrainInOrderAsync()
    {
        var writer = _output.Writer;
        while (true)
        {
            if (Interlocked.CompareExchange(ref _drainOwned, 1, 0) != 0)
            {
                // Someone else is draining; they will pick up whatever this worker just published.
                return;
            }

            try
            {
                while (true)
                {
                    var emit = _emitSeq;
                    var slot = (int)(emit & _ringMask);
                    if (Volatile.Read(ref _slots[slot].State) == 0)
                    {
                        break;
                    }

                    var value = _slots[slot].Value;
                    if (RuntimeHelpers.IsReferenceOrContainsReferences<TOut>())
                    {
                        _slots[slot].Value = default;
                    }
                    Volatile.Write(ref _slots[slot].State, 0);
                    Volatile.Write(ref _emitSeq, emit + 1);
                    // Free the slot; this is what unblocks a worker parked in WaitAsync with the ring full.
                    _slotSem.Release();

                    if (!writer.TryWrite(value))
                    {
                        // Observe _completionCts (not _loopToken): a fault elsewhere cancels it, and unlike the
                        // input-side waits this one is not covered by channel completion (the output is only
                        // completed once the workers have unwound), so without this a drainer parked on output
                        // backpressure would hang the fault. Costs nothing unless it actually suspends here.
                        await writer.WriteAsync(value, _completionCts.Token).ConfigureAwait(false);
                    }
                    if (FluxMetrics.ItemsProcessed.Enabled)
                    {
                        FluxMetrics.ItemsProcessed.Add(1, _tags);
                    }
                }
            }
            finally
            {
                Volatile.Write(ref _drainOwned, 0);
            }

            // Re-check after releasing: a worker may have published the next in-order slot while this thread held
            // the drain and then found _drainOwned set and returned. If it is now ready, loop and re-acquire so
            // that item is not stranded; otherwise there is genuinely nothing more to do.
            var nextSlot = (int)(Volatile.Read(ref _emitSeq) & _ringMask);
            if (Volatile.Read(ref _slots[nextSlot].State) == 0)
            {
                return;
            }
        }
    }

    private void FaultOrderedPipeline(Exception exception)
    {
        TrySetFault(exception);
        _input.Writer.TryComplete(exception);
        // Cancel to wake any worker parked on the slot semaphore so it unwinds; on the ordered path this is the
        // only wait that does not already observe input completion. Idempotent, so repeated fault calls are fine.
        _completionCts.Cancel();
    }

    private async Task FinalizeOrderedAsync()
    {
        await Task.WhenAll(_workers).ConfigureAwait(false);
        var fault = ObservedFault;
        if (fault is null)
        {
            // Clean completion: every claimed seq has been published, but the worker that published last may have
            // lost the drain re-check race. Flush the remainder here, uncontended (no workers remain).
            await DrainInOrderAsync().ConfigureAwait(false);
        }
        _output.Writer.TryComplete(fault);
        ResolveCompletion(fault);
    }
}
