namespace LaquaiLib.Flux;

/// <summary>
/// Defines members shared by every Flux block, independent of the input or output element type it operates on.
/// </summary>
public interface IFluxBlock
{
    /// <summary>
    /// Gets the <see cref="Task"/> that represents the completion state of this block.
    /// <para/>
    /// Resolves to <see cref="TaskStatus.RanToCompletion"/> once the block has finished processing all input
    /// accepted before <see cref="IFluxTarget{TIn}.Complete"/> was called and, for blocks that also produce
    /// output, once that output has been fully handed off to any linked target.
    /// <para/>
    /// Resolves to <see cref="TaskStatus.Faulted"/> if the block was faulted via <see cref="IFluxTarget{TIn}.Fault"/>
    /// or if an unhandled exception occurred while processing an item.
    /// <para/>
    /// Resolves to <see cref="TaskStatus.Canceled"/> if the <see cref="CancellationToken"/> configured via
    /// <see cref="FluxBlockOptions.CancellationToken"/> was signaled before the block completed naturally.
    /// </summary>
    public Task Completion { get; }

    /// <summary>
    /// Gets the name assigned to this block instance, as configured via <see cref="FluxBlockOptions.Name"/>,
    /// or an automatically generated identifier if none was supplied. Used to tag metrics emitted for this instance.
    /// </summary>
    public string Name { get; }
}
