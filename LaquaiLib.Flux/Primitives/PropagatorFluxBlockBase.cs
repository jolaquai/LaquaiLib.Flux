using LaquaiLib.Flux;
using LaquaiLib.Flux.Diagnostics;

namespace LaquaiLib.Flux.Primitives;

/// <summary>
/// Shared output-side plumbing for Flux blocks that both accept input and produce output: the bounded output
/// <see cref="Channel{T}"/>, the link store described in <see cref="FluxLink{TOut}"/>, and the lazily started
/// dispatch loop that pushes produced items to every linked target according to <see cref="FluxBlockOptions.FanOutMode"/>.
/// <para/>
/// Resolving this block's own <see cref="IFluxBlock.Completion"/> is <b>not</b> this type's responsibility - it
/// belongs to the concrete block (e.g. <see cref="TransformBlock{TIn, TOut}"/>), which is the only thing that
/// knows when it has itself finished producing output and can complete <see cref="_output"/>'s writer. This type
/// only owns propagating that completion (or a fault) to whatever is linked, once the dispatch loop has actually
/// finished draining <see cref="_output"/> to it - so a linked target never sees <see cref="IFluxTarget{TIn}.Complete"/>
/// before every item destined for it has already been handed off.
/// <para/>
/// Public only because a <see langword="public sealed"/> block type cannot derive from an <see langword="internal"/>
/// base class; the constructor is <see langword="private protected"/>, so this cannot actually be subclassed
/// outside this assembly.
/// </summary>
/// <typeparam name="TIn">The type of item accepted by this block.</typeparam>
/// <typeparam name="TOut">The type of item produced by this block.</typeparam>
public abstract class PropagatorFluxBlockBase<TIn, TOut> : TargetFluxBlockBase<TIn>, IFluxSource<TOut>, IFluxLinkOwner<TOut>
{
    private protected readonly Channel<TOut> _output;

    private readonly
#if NET9_0_OR_GREATER
        Lock
#else
        object
#endif
        _linksLock = new();
    private readonly List<FluxLink<TOut>> _links = new(capacity: 1);
    // Immutable snapshot of _links, republished under _linksLock on every LinkTo/Unlink. The dispatch loop reads
    // this once per item instead of materializing a fresh array per item under the lock - snapshotting per item
    // was, by itself, the single largest allocation source in multi-stage pipelines.
    private FluxLink<TOut>[] _linksSnapshot = [];
    private int _dispatchStarted;
    // RoundRobin's rotating sweep start. Never needs Interlocked: DispatchLoopAsync is the sole drainer of
    // _output (constructed with SingleReader = true), so exactly one thread ever touches this.
    private int _rrCursor;

    private protected PropagatorFluxBlockBase(FluxBlockOptions options, bool inputSingleReader, bool outputSingleWriter)
        : base(options, inputSingleReader)
    {
        _output = Channel.CreateBounded<TOut>(new BoundedChannelOptions(_options.BoundedCapacity)
        {
            SingleReader = true,
            SingleWriter = outputSingleWriter,
            FullMode = BoundedChannelFullMode.Wait,
        });

        FluxBlockDiagnostics.RegisterOutput(Name, _output.Reader);
    }

    /// <inheritdoc/>
    public IDisposable LinkTo(IFluxTarget<TOut> target, FluxLinkOptions linkOptions = null, Func<TOut, bool> filter = null)
    {
        ArgumentNullException.ThrowIfNull(target);

        var link = new FluxLink<TOut>(this, target, linkOptions ?? FluxLinkOptions.Default, filter);
        lock (_linksLock)
        {
            _links.Add(link);
            Volatile.Write(ref _linksSnapshot, [.. _links]);
        }

        if (Interlocked.Exchange(ref _dispatchStarted, 1) == 0)
        {
            _ = DispatchLoopAsync();
        }

        return link;
    }

    /// <inheritdoc/>
    void IFluxLinkOwner<TOut>.Unlink(FluxLink<TOut> link)
    {
        lock (_linksLock)
        {
            _links.Remove(link);
            Volatile.Write(ref _linksSnapshot, _links.Count == 0 ? [] : [.. _links]);
        }
    }

    /// <inheritdoc/>
    public IAsyncEnumerable<TOut> ReceiveAllAsync(CancellationToken cancellationToken = default) => _output.Reader.ReadAllAsync(cancellationToken);

    private async Task DispatchLoopAsync()
    {
        var reader = _output.Reader;
        Exception fault = null;
        try
        {
            while (await reader.WaitToReadAsync(_loopToken).ConfigureAwait(false))
            {
                while (reader.TryRead(out var item))
                {
                    var links = Volatile.Read(ref _linksSnapshot);
                    if (links.Length == 0)
                    {
                        if (FluxMetrics.ItemsDropped.Enabled)
                        {
                            FluxMetrics.ItemsDropped.Add(1, _tags);
                        }
                        continue;
                    }

                    await DispatchItemAsync(item, links).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            fault = ex;
        }

        foreach (var link in Volatile.Read(ref _linksSnapshot))
        {
            if (fault is not null)
            {
                link.Target.Fault(fault);
            }
            else if (link.Options.PropagateCompletion)
            {
                link.Target.Complete();
            }
        }
    }

    /// <summary>
    /// Routes <paramref name="item"/> to the appropriate target(s) among <paramref name="links"/> (guaranteed
    /// non-empty by <see cref="DispatchLoopAsync"/>). A single active link always takes the fast path regardless
    /// of <see cref="FluxBlockOptions.FanOutMode"/>: with only one candidate, every mode degenerates to the same
    /// "send to this one, respecting its filter" behavior anyway.
    /// </summary>
    private ValueTask DispatchItemAsync(TOut item, FluxLink<TOut>[] links) => _options.FanOutMode switch
    {
        _ when links.Length == 1 => DispatchSingleAsync(item, links[0]),
        FluxFanOutMode.FirstAvailable => FirstAvailableAsync(item, links),
        FluxFanOutMode.RoundRobin => RoundRobinAsync(item, links),
        _ => BroadcastAsync(item, links),
    };

    // Single-link fast path. Mirrors the SendAsync/SendAsyncSlow split in TargetFluxBlockBase: only builds an
    // async continuation when the target genuinely suspends, so the common case (target has room) never pays
    // for more than a synchronously-resolved ValueTask.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask DispatchSingleAsync(TOut item, FluxLink<TOut> link)
    {
        if (link.Filter is not null && !link.Filter(item))
        {
            if (FluxMetrics.ItemsDropped.Enabled)
            {
                FluxMetrics.ItemsDropped.Add(1, _tags);
            }
            return default;
        }

        var task = link.Target.SendAsync(item, _loopToken);
        if (task.IsCompletedSuccessfully)
        {
            if (!task.Result && FluxMetrics.ItemsDropped.Enabled)
            {
                FluxMetrics.ItemsDropped.Add(1, _tags);
            }
            return default;
        }
        return AwaitSingleAsync(task);
    }

    private async ValueTask AwaitSingleAsync(ValueTask<bool> task)
    {
        var accepted = await task.ConfigureAwait(false);
        if (!accepted && FluxMetrics.ItemsDropped.Enabled)
        {
            FluxMetrics.ItemsDropped.Add(1, _tags);
        }
    }

    // Broadcast: every filter-matching target gets the item. Targets that can accept synchronously (the common
    // case in a healthy pipeline) are handled with zero allocation via TryOffer; only targets that decline
    // synchronously (full or permanently done) get promoted to a genuinely-awaited SendAsync, and those run
    // concurrently rather than being awaited one at a time - N targets' backpressure waits overlap instead of
    // stacking serially. The pooled buffer only gets rented at all when at least one target needs it.
    private async ValueTask BroadcastAsync(TOut item, FluxLink<TOut>[] links)
    {
        ValueTask<bool>[] pending = null;
        var pendingCount = 0;
        var matched = false;
        for (var i = 0; i < links.Length; i++)
        {
            var link = links[i];
            if (link.Filter is not null && !link.Filter(item))
            {
                continue;
            }
            matched = true;
            if (link.Target.TryOffer(item))
            {
                continue;
            }
            pending ??= ArrayPool<ValueTask<bool>>.Shared.Rent(links.Length);
            pending[pendingCount++] = link.Target.SendAsync(item, _loopToken);
        }

        if (pending is null)
        {
            if (!matched && FluxMetrics.ItemsDropped.Enabled)
            {
                FluxMetrics.ItemsDropped.Add(1, _tags);
            }
            return;
        }

        try
        {
            for (var i = 0; i < pendingCount; i++)
            {
                var accepted = await pending[i].ConfigureAwait(false);
                if (!accepted && FluxMetrics.ItemsDropped.Enabled)
                {
                    FluxMetrics.ItemsDropped.Add(1, _tags);
                }
            }
        }
        finally
        {
            ArrayPool<ValueTask<bool>>.Shared.Return(pending, clearArray: true);
        }
    }

    // FirstAvailable: link order is priority order. Sweep for anyone free right now first; only if nobody is
    // does it fall back to waiting, still in priority order. Because a dead target's SendAsync returns false
    // instantly rather than hanging, a faulted/completed higher-priority link can never stall delivery to a
    // healthy lower-priority one.
    private async ValueTask FirstAvailableAsync(TOut item, FluxLink<TOut>[] links)
    {
        foreach (var link in links)
        {
            if ((link.Filter is null || link.Filter(item)) && link.Target.TryOffer(item))
            {
                return;
            }
        }

        foreach (var link in links)
        {
            if (link.Filter is not null && !link.Filter(item))
            {
                continue;
            }
            if (await link.Target.SendAsync(item, _loopToken).ConfigureAwait(false))
            {
                return;
            }
        }

        // Every matching link permanently done (or none matched at all) -> drop.
        if (FluxMetrics.ItemsDropped.Enabled)
        {
            FluxMetrics.ItemsDropped.Add(1, _tags);
        }
    }

    // RoundRobin: adaptive load balancing, not strict positional partitioning. Sweep TryOffer starting from a
    // rotating cursor (so ties never keep favoring link 0); first free matching target wins. Only if the entire
    // sweep finds nobody free does it block, still starting from the same rotated position. A faster consumer is
    // statistically more often "the one with room," regardless of which position it was linked at.
    private async ValueTask RoundRobinAsync(TOut item, FluxLink<TOut>[] links)
    {
        var n = links.Length;
        // Cast through uint before the modulo: _rrCursor is a plain, ever-incrementing int with no reset, so it
        // eventually wraps to negative. Signed modulo of a negative dividend can yield a negative index; unsigned
        // modulo cannot, and the bit-reinterpreting cast is well-defined regardless of how far it has wrapped.
        var start = (int)((uint)_rrCursor++ % (uint)n);

        for (var i = 0; i < n; i++)
        {
            var link = links[(start + i) % n];
            if (link.Filter is not null && !link.Filter(item))
            {
                continue;
            }
            if (link.Target.TryOffer(item))
            {
                return;
            }
        }
        for (var i = 0; i < n; i++)
        {
            var link = links[(start + i) % n];
            if (link.Filter is not null && !link.Filter(item))
            {
                continue;
            }
            if (await link.Target.SendAsync(item, _loopToken).ConfigureAwait(false))
            {
                return;
            }
        }

        // Every matching link permanently done (or none matched at all) -> drop.
        if (FluxMetrics.ItemsDropped.Enabled)
        {
            FluxMetrics.ItemsDropped.Add(1, _tags);
        }
    }
}
