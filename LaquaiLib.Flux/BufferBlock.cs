using LaquaiLib.Flux.Diagnostics;
using LaquaiLib.Flux.Primitives;

namespace LaquaiLib.Flux;

/// <summary>
/// A Flux block that buffers items without transformation, exposing them for linking or pull-based consumption.
/// <para/>
/// A single internal pump loop moves items from the input channel to the output channel unchanged, preserving
/// order. <see cref="FluxBlockOptions.MaxDegreeOfParallelism"/> is deliberately ignored: there is nothing to
/// parallelize when the "work" is just moving an item from one channel to another.
/// </summary>
/// <typeparam name="T">The type of item buffered by this block.</typeparam>
public sealed class BufferBlock<T> : PropagatorFluxBlockBase<T, T>
{
    private readonly Task _pumpTask;

    /// <summary>
    /// Initializes a new <see cref="BufferBlock{T}"/>.
    /// </summary>
    /// <param name="options">Options controlling capacity and diagnostics. <see langword="null"/> uses <see cref="FluxBlockOptions.Default"/>.</param>
    public BufferBlock(FluxBlockOptions options = null)
        : base(options, inputSingleReader: true, outputSingleWriter: true)
    {
        _pumpTask = Task.Run(PumpLoopAsync);
        _ = FinalizeAsync();
    }

    private async Task PumpLoopAsync()
    {
        var reader = _input.Reader;
        var writer = _output.Writer;
        try
        {
            while (await reader.WaitToReadAsync(_loopToken).ConfigureAwait(false))
            {
                while (reader.TryRead(out var item))
                {
                    if (!writer.TryWrite(item))
                    {
                        await writer.WriteAsync(item, _loopToken).ConfigureAwait(false);
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

    private async Task FinalizeAsync()
    {
        await _pumpTask.ConfigureAwait(false);
        var fault = ObservedFault;
        _output.Writer.TryComplete(fault);
        ResolveCompletion(fault);
    }
}
