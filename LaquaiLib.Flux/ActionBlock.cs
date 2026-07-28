using LaquaiLib.Flux.Diagnostics;
using LaquaiLib.Flux.Primitives;

namespace LaquaiLib.Flux;

/// <summary>
/// A Flux block that asynchronously invokes an action for each accepted item and produces no output.
/// </summary>
/// <typeparam name="TIn">The type of item accepted by this block.</typeparam>
public sealed class ActionBlock<TIn> : TargetFluxBlockBase<TIn>
{
    private readonly Func<TIn, ValueTask> _action;
    private readonly Task[] _workers;

    /// <summary>
    /// Initializes a new <see cref="ActionBlock{TIn}"/> using a synchronous action delegate.
    /// </summary>
    /// <param name="action">The synchronous action invoked for each accepted item.</param>
    /// <param name="options">Options controlling capacity, parallelism and diagnostics. <see langword="null"/> uses <see cref="FluxBlockOptions.Default"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ActionBlock(Action<TIn> action, FluxBlockOptions options = null)
        : this(WrapSync(action), options)
    {
    }

    /// <summary>
    /// Initializes a new <see cref="ActionBlock{TIn}"/> using an asynchronous action delegate.
    /// </summary>
    /// <param name="action">The asynchronous action invoked for each accepted item.</param>
    /// <param name="options">Options controlling capacity, parallelism and diagnostics. <see langword="null"/> uses <see cref="FluxBlockOptions.Default"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    public ActionBlock(Func<TIn, ValueTask> action, FluxBlockOptions options = null)
        : base(options, ComputeSingleReader(options))
    {
        ArgumentNullException.ThrowIfNull(action);
        _action = action;
        _workers = new Task[GetMaxDegreeOfParallelism(_options)];
        for (var i = 0; i < _workers.Length; i++)
        {
            _workers[i] = Task.Run(WorkerLoopAsync);
        }
        _ = FinalizeAsync();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Func<TIn, ValueTask> WrapSync(Action<TIn> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        return item =>
        {
            action(item);
            return ValueTask.CompletedTask;
        };
    }

    private static int GetMaxDegreeOfParallelism(FluxBlockOptions options) => Math.Clamp((options ?? FluxBlockOptions.Default).MaxDegreeOfParallelism, 1, 1 << 20);

    private static bool ComputeSingleReader(FluxBlockOptions options) => GetMaxDegreeOfParallelism(options) == 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask ActionMeasuredAsync(TIn item)
    {
        if (FluxMetrics.StageLatency.Enabled)
        {
            return ActionTimedAsync(item);
        }
        return _action(item);
    }

    private async ValueTask ActionTimedAsync(TIn item)
    {
        var start = Stopwatch.GetTimestamp();
        await _action(item).ConfigureAwait(false);
        FluxMetrics.StageLatency.Record(Stopwatch.GetElapsedTime(start).TotalMilliseconds, _tags);
    }

    private async Task WorkerLoopAsync()
    {
        var reader = _input.Reader;
        try
        {
            while (await reader.WaitToReadAsync(_loopToken).ConfigureAwait(false))
            {
                while (reader.TryRead(out var item))
                {
                    await ActionMeasuredAsync(item).ConfigureAwait(false);
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

    private async Task FinalizeAsync()
    {
        await Task.WhenAll(_workers).ConfigureAwait(false);
        ResolveCompletion(ObservedFault);
    }
}
