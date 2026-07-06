using System.Collections.Concurrent;

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
/// funnels their results back into input order through a single ordered hand-off (see remarks on <see cref="StartOrderedPipeline"/>).
/// <para/>
/// The ordered+parallel combination is the one path in this type that does not yet beat
/// <see cref="System.Threading.Tasks.Dataflow.TransformBlock{TInput, TOutput}"/> on throughput - several attempts
/// at a lock-free reorder buffer (a <see cref="ConcurrentDictionary{TKey, TValue}"/> plus a CAS-elected single
/// drainer) each measured slower than this simpler per-item-Task hand-off, despite allocating less, so this is
/// the version actually in use pending a real profiler-driven investigation rather than further guesswork.
/// </summary>
/// <typeparam name="TIn">The type of item accepted by this block.</typeparam>
/// <typeparam name="TOut">The type of item produced by this block.</typeparam>
public sealed class TransformBlock<TIn, TOut> : PropagatorFluxBlockBase<TIn, TOut>
{
    private readonly Func<TIn, ValueTask<TOut>> _transform;
    private readonly int _maxDegreeOfParallelism;
    private readonly bool _ensureOrdered;

    // Unordered path only.
    private readonly Task[] _workers;

    // Ordered path only.
    private SemaphoreSlim _concurrencyGate;
    private Channel<Task<TOut>> _orderedHandoff;

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
        _maxDegreeOfParallelism = Math.Max(1, _options.MaxDegreeOfParallelism);
        _ensureOrdered = _options.EnsureOrdered && _maxDegreeOfParallelism > 1;

        if (_ensureOrdered)
        {
            StartOrderedPipeline();
        }
        else
        {
            _workers = new Task[_maxDegreeOfParallelism];
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

    private static int GetMaxDegreeOfParallelism(FluxBlockOptions options) => Math.Max(1, (options ?? FluxBlockOptions.Default).MaxDegreeOfParallelism);

    // Input channel keeps SingleReader = true whenever only one internal loop ever reads it directly: at DOP 1
    // (a single unordered worker) or in ordered mode (the dedicated producer loop is the sole reader even though
    // transforms themselves still run concurrently downstream of it).
    private static bool ComputeInputSingleReader(FluxBlockOptions options) => GetMaxDegreeOfParallelism(options) == 1 || (options ?? FluxBlockOptions.Default).EnsureOrdered;

    // Output channel keeps SingleWriter = true whenever only one internal loop ever writes to it: at DOP 1, or in
    // ordered mode (the dedicated consumer loop is the sole writer). Only the unordered, DOP > 1 case has multiple
    // concurrent workers genuinely writing to _output independently.
    private static bool ComputeOutputSingleWriter(FluxBlockOptions options) => GetMaxDegreeOfParallelism(options) == 1 || (options ?? FluxBlockOptions.Default).EnsureOrdered;

    // ─────────────────────────────────────────────
    //  Unordered path (default) - N independent workers, no ordering bookkeeping at all.
    // ─────────────────────────────────────────────

    private async Task UnorderedWorkerLoopAsync()
    {
        try
        {
            while (await _input.Reader.WaitToReadAsync(_completionCts.Token).ConfigureAwait(false))
            {
                while (_input.Reader.TryRead(out var item))
                {
                    var start = Stopwatch.GetTimestamp();
                    var result = await _transform(item).ConfigureAwait(false);
                    FluxMetrics.StageLatency.Record(Stopwatch.GetElapsedTime(start).TotalMilliseconds, _tags);
                    await _output.Writer.WriteAsync(result, _completionCts.Token).ConfigureAwait(false);
                    FluxMetrics.ItemsProcessed.Add(1, _tags);
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
    //  Ordered path (EnsureOrdered + DOP > 1) - a producer loop hands each item's in-flight Task<TOut> to a
    //  single consumer loop, in order; the consumer awaits each Task in turn (not racing them), which preserves
    //  input order while still allowing up to MaxDegreeOfParallelism transforms to run concurrently in the
    //  background. There is exactly one writer to _output (the consumer), so no extra synchronization is needed
    //  there either.
    // ─────────────────────────────────────────────

    private void StartOrderedPipeline()
    {
        _concurrencyGate = new SemaphoreSlim(_maxDegreeOfParallelism, _maxDegreeOfParallelism);
        _orderedHandoff = Channel.CreateBounded<Task<TOut>>(new BoundedChannelOptions(_maxDegreeOfParallelism)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });

        var producer = Task.Run(OrderedProducerLoopAsync);
        var consumer = Task.Run(OrderedConsumerLoopAsync);
        _ = FinalizeOrderedAsync(producer, consumer);
    }

    private async Task OrderedProducerLoopAsync()
    {
        try
        {
            while (await _input.Reader.WaitToReadAsync(_completionCts.Token).ConfigureAwait(false))
            {
                while (_input.Reader.TryRead(out var item))
                {
                    await _concurrencyGate.WaitAsync(_completionCts.Token).ConfigureAwait(false);
                    var task = TransformAndReleaseAsync(item);
                    await _orderedHandoff.Writer.WriteAsync(task, _completionCts.Token).ConfigureAwait(false);
                }
            }
            _orderedHandoff.Writer.TryComplete();
        }
        catch (Exception ex)
        {
            FaultOrderedPipeline(ex);
        }
    }

    private async Task<TOut> TransformAndReleaseAsync(TIn item)
    {
        try
        {
            var start = Stopwatch.GetTimestamp();
            var result = await _transform(item).ConfigureAwait(false);
            FluxMetrics.StageLatency.Record(Stopwatch.GetElapsedTime(start).TotalMilliseconds, _tags);
            return result;
        }
        finally
        {
            _concurrencyGate.Release();
        }
    }

    private async Task OrderedConsumerLoopAsync()
    {
        try
        {
            await foreach (var task in _orderedHandoff.Reader.ReadAllAsync(_completionCts.Token).ConfigureAwait(false))
            {
                var result = await task.ConfigureAwait(false);
                await _output.Writer.WriteAsync(result, _completionCts.Token).ConfigureAwait(false);
                FluxMetrics.ItemsProcessed.Add(1, _tags);
            }
        }
        catch (Exception ex)
        {
            FaultOrderedPipeline(ex);
        }
    }

    private void FaultOrderedPipeline(Exception exception)
    {
        TrySetFault(exception);
        _input.Writer.TryComplete(exception);
        _orderedHandoff.Writer.TryComplete(exception);
    }

    private async Task FinalizeOrderedAsync(Task producer, Task consumer)
    {
        await Task.WhenAll(producer, consumer).ConfigureAwait(false);
        var fault = ObservedFault;
        _output.Writer.TryComplete(fault);
        ResolveCompletion(fault);
    }
}
