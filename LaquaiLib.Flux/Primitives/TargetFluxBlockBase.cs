using LaquaiLib.Flux.Diagnostics;

namespace LaquaiLib.Flux.Primitives;

/// <summary>
/// Shared input-side plumbing for every Flux block: the bounded input <see cref="Channel{T}"/>, the
/// <see cref="IFluxBlock.Completion"/> state machine, and per-instance metrics tagging.
/// <para/>
/// Faulting is tracked in a dedicated field (<see cref="TrySetFault"/>/<see cref="ObservedFault"/>), not by
/// inspecting <see cref="ChannelReader{T}.Completion"/>: <see cref="Complete"/> and a fault both ultimately call
/// <see cref="ChannelWriter{T}.TryComplete(Exception)"/> on the same input channel, and only the first such call
/// has any effect. If <see cref="Complete"/> already completed the channel cleanly before an in-flight item goes
/// on to throw, the channel's own completion state would stay clean forever - silently swallowing the fault -
/// unless the fault is captured independently of that race, which is exactly what this field does.
/// <para/>
/// Public only because a <see langword="public sealed"/> block type cannot derive from an <see langword="internal"/>
/// base class; the constructor is <see langword="private protected"/>, so this cannot actually be subclassed
/// outside this assembly.
/// </summary>
/// <typeparam name="TIn">The type of item accepted by this block.</typeparam>
public abstract class TargetFluxBlockBase<TIn> : IFluxTarget<TIn>
{
    private protected readonly Channel<TIn> _input;
    private protected readonly FluxBlockOptions _options;
    private protected readonly CancellationTokenSource _completionCts;
    private protected readonly TagList _tags;

    /// <summary>
    /// The token internal read/dispatch loops pass to channel waits. This is <see cref="CancellationToken.None"/>
    /// whenever the options token can never fire: a cancelable token forces every suspended channel operation to
    /// allocate a fresh async operation plus a cancellation registration instead of reusing the channel's pooled
    /// one, which is pure per-suspension overhead when nothing can ever cancel. <see cref="_completionCts"/> is
    /// never canceled independently of the options token today, so nothing is lost by not observing it here; if
    /// that ever changes, this must go back to always being <see cref="_completionCts"/>'s token.
    /// </summary>
    private protected readonly CancellationToken _loopToken;

    private readonly TaskCompletionSource _completionTcs;
    private Exception _fault;

    /// <summary>
    /// Whether accepting an item should also count as processing it. Set only by a pure passthrough block, which
    /// has no processing loop of its own to report <see cref="FluxMetrics.ItemsProcessed"/> from, yet must not
    /// leave a hole in the "every stage reports throughput" contract. For such a block the two counts are
    /// identical by definition.
    /// </summary>
    private readonly bool _countAcceptedAsProcessed;

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public Task Completion => _completionTcs.Task;

    private protected TargetFluxBlockBase(FluxBlockOptions options, bool singleReader, bool countAcceptedAsProcessed = false)
    {
        _countAcceptedAsProcessed = countAcceptedAsProcessed;
        _options = options ?? FluxBlockOptions.Default;
        Name = _options.Name ?? FluxNameGenerator.Generate(GetType());
        _tags = new TagList { { "flux.block.name", Name } };

        _input = Channel.CreateBounded<TIn>(new BoundedChannelOptions(_options.BoundedCapacity)
        {
            SingleReader = singleReader,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait,
        });

        _completionCts = _options.CancellationToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(_options.CancellationToken)
            : new CancellationTokenSource();
        _loopToken = _options.CancellationToken.CanBeCanceled ? _completionCts.Token : default;
        _completionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        FluxBlockDiagnostics.RegisterInput(Name, _input.Reader);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<bool> SendAsync(TIn item, CancellationToken cancellationToken = default)
    {
        if (TryOffer(item))
        {
            return new ValueTask<bool>(true);
        }
        return SendAsyncSlow(item, cancellationToken);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryOffer(TIn item)
    {
        if (_input.Writer.TryWrite(item))
        {
            // Enabled-gated so the no-listener case pays a single branch instead of the full Add call.
            if (FluxMetrics.ItemsAccepted.Enabled)
            {
                FluxMetrics.ItemsAccepted.Add(1, _tags);
            }
            // Field first: it is false for every block but a passthrough, so this stays a predicted-not-taken
            // branch that never even loads the instrument.
            if (_countAcceptedAsProcessed && FluxMetrics.ItemsProcessed.Enabled)
            {
                FluxMetrics.ItemsProcessed.Add(1, _tags);
            }
            return true;
        }
        return false;
    }

    private async ValueTask<bool> SendAsyncSlow(TIn item, CancellationToken cancellationToken)
    {
        while (true)
        {
            if (FluxMetrics.BackpressureEvents.Enabled)
            {
                FluxMetrics.BackpressureEvents.Add(1, _tags);
            }
            if (!await _input.Writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
            {
                return false;
            }
            if (TryOffer(item))
            {
                return true;
            }
        }
    }

    /// <inheritdoc/>
    public void Complete() => _input.Writer.TryComplete();

    /// <inheritdoc/>
    /// <remarks>
    /// <see langword="virtual"/> purely so a block whose read loop is not the thing that discards its queued items
    /// can add that discard here; this is a cold, once-per-block path, so the virtual dispatch costs nothing that
    /// matters. Overrides must call <see langword="base"/>.
    /// </remarks>
    public virtual void Fault(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        TrySetFault(exception);
        _input.Writer.TryComplete(exception);
    }

    /// <summary>
    /// Records <paramref name="exception"/> as this block's terminal fault if none has been recorded yet.
    /// Idempotent: only the first call - whether from an explicit <see cref="Fault"/> call or a read loop
    /// catching a per-item exception - has any effect, matching the "first exception wins" contract on
    /// <see cref="Fault"/>.
    /// </summary>
    private protected void TrySetFault(Exception exception) => Interlocked.CompareExchange(ref _fault, exception, null);

    /// <summary>Gets the fault recorded via <see cref="TrySetFault"/>, or <see langword="null"/> if none has occurred.</summary>
    private protected Exception ObservedFault => Volatile.Read(ref _fault);

    /// <summary>
    /// Resolves <see cref="Completion"/>. <paramref name="exception"/> of <see langword="null"/> resolves
    /// <see cref="TaskStatus.RanToCompletion"/>; an <see cref="OperationCanceledException"/> resolves
    /// <see cref="TaskStatus.Canceled"/>; any other exception resolves <see cref="TaskStatus.Faulted"/>.
    /// Called exactly once by the derived type's read loop once it has finished draining <see cref="_input"/>.
    /// </summary>
    private protected void ResolveCompletion(Exception exception)
    {
        if (exception is null)
        {
            _completionTcs.TrySetResult();
        }
        else if (exception is OperationCanceledException oce)
        {
            _completionTcs.TrySetCanceled(oce.CancellationToken);
        }
        else
        {
            _completionTcs.TrySetException(exception);
        }
    }
}
