using LaquaiLib.Flux.Primitives;

namespace LaquaiLib.Flux;

/// <summary>
/// A Flux block that buffers items without transformation, exposing them for linking or pull-based consumption.
/// </summary>
/// <typeparam name="T">The type of item buffered by this block.</typeparam>
public sealed class BufferBlock<T> : PropagatorFluxBlockBase<T, T>
{
    /// <summary>
    /// Initializes a new <see cref="BufferBlock{T}"/>.
    /// </summary>
    /// <param name="options">Options controlling capacity and diagnostics. <see langword="null"/> uses <see cref="FluxBlockOptions.Default"/>.</param>
    public BufferBlock(FluxBlockOptions options = null)
        : base(options, inputSingleReader: true, outputSingleWriter: true)
    {
        throw new NotImplementedException("BufferBlock<T> is a v1 surface stub; full implementation lands in a follow-up pass.");
    }
}
