namespace LaquaiLib.Flux;

/// <summary>
/// Configures the construction-time behavior of a Flux block: buffering, parallelism, ordering and diagnostics.
/// </summary>
public sealed class FluxBlockOptions
{
    /// <summary>
    /// Gets a shared instance carrying every default.
    /// </summary>
    public static readonly FluxBlockOptions Default = new();

    /// <summary>
    /// Gets or sets the bounded capacity of the block's internal input channel. Must be at least 1.
    /// Defaults to <c>1024</c>. Callers offering items via <see cref="IFluxTarget{TIn}.SendAsync"/> suspend once
    /// this many items are queued and not yet claimed by a worker.
    /// </summary>
    public int BoundedCapacity { get; init; } = 1024;

    /// <summary>
    /// Gets or sets the maximum number of concurrent workers processing items pulled from the input channel.
    /// Defaults to <c>1</c>. Values greater than 1 imply unordered output by default unless <see cref="EnsureOrdered"/> is set.
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = 1;

    /// <summary>
    /// Gets or sets whether output must preserve the relative order in which items were accepted on input,
    /// even when <see cref="MaxDegreeOfParallelism"/> exceeds 1. Defaults to <see langword="false"/>: Flux blocks
    /// are unordered by default, unlike <see cref="System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions"/>.
    /// <para/>
    /// Has no observable effect when <see cref="MaxDegreeOfParallelism"/> is 1, since a single worker is
    /// inherently ordered and the reordering machinery is never constructed in that case.
    /// </summary>
    public bool EnsureOrdered { get; init; }

    /// <summary>
    /// Gets or sets the <see cref="System.Threading.CancellationToken"/> observed by this block. Cancellation
    /// transitions <see cref="IFluxBlock.Completion"/> to the canceled state and stops accepting and processing further items.
    /// Defaults to <see cref="System.Threading.CancellationToken.None"/>.
    /// </summary>
    public CancellationToken CancellationToken { get; init; }

    /// <summary>
    /// Gets or sets the name used to tag this block instance in emitted metrics and in <see cref="IFluxBlock.Name"/>.
    /// Defaults to <see langword="null"/>, in which case an automatically generated name of the form
    /// <c>"{TypeName}-{instance counter}"</c> is assigned at construction.
    /// </summary>
    public string Name { get; init; }

    /// <summary>
    /// Gets or sets how a block with more than one active link (see <see cref="IFluxSource{TOut}.LinkTo"/>)
    /// dispatches each produced item. Defaults to <see cref="FluxFanOutMode.Broadcast"/>. Has no observable
    /// effect while 0 or 1 links are active. Construction-time and immutable for the block's lifetime, like
    /// <see cref="EnsureOrdered"/> and <see cref="MaxDegreeOfParallelism"/>: the dispatch loop reads this once
    /// per item against a concurrently-mutating link set, so changing it mid-flight would be a correctness
    /// hazard for no real benefit.
    /// </summary>
    public FluxFanOutMode FanOutMode { get; init; } = FluxFanOutMode.Broadcast;
}
