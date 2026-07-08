using LaquaiLib.Flux.Primitives;

namespace LaquaiLib.Flux;

/// <summary>
/// A Flux block that asynchronously invokes an action for each accepted item and produces no output.
/// </summary>
/// <typeparam name="TIn">The type of item accepted by this block.</typeparam>
public sealed class ActionBlock<TIn> : TargetFluxBlockBase<TIn>
{
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
        throw new NotImplementedException("ActionBlock<TIn> is not yet implemented.");
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

    private static bool ComputeSingleReader(FluxBlockOptions options) => Math.Max(1, (options ?? FluxBlockOptions.Default).MaxDegreeOfParallelism) == 1;
}
