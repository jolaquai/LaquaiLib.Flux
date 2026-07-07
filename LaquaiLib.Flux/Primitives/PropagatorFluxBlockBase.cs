using LaquaiLib.Flux;
using LaquaiLib.Flux.Diagnostics;

namespace LaquaiLib.Flux.Primitives;

/// <summary>
/// Shared output-side plumbing for Flux blocks that both accept input and produce output: the bounded output
/// <see cref="Channel{T}"/>, the single-link store described in <see cref="FluxLink{TOut}"/>, and the lazily
/// started dispatch loop that pushes produced items to a linked target.
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
    public IDisposable LinkTo(IFluxTarget<TOut> target, FluxLinkOptions linkOptions = null)
    {
        ArgumentNullException.ThrowIfNull(target);

        var link = new FluxLink<TOut>(this, target, linkOptions ?? FluxLinkOptions.Default);
        lock (_linksLock)
        {
            if (_links.Count > 0)
            {
                throw new InvalidOperationException($"'{Name}' already has an active link. v1 permits at most one linked target per source; dispose the IDisposable returned by the prior LinkTo call first.");
            }
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

                    foreach (var link in links)
                    {
                        var accepted = await link.Target.SendAsync(item, _loopToken).ConfigureAwait(false);
                        if (!accepted && FluxMetrics.ItemsDropped.Enabled)
                        {
                            FluxMetrics.ItemsDropped.Add(1, _tags);
                        }
                    }
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
}
