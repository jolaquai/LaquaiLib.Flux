using System.Collections.Concurrent;

using LaquaiLib.Flux;

namespace LaquaiLib.Flux.Primitives;

/// <summary>
/// Generates auto-assigned <see cref="IFluxBlock.Name"/> values, keyed by concrete block type rather than by
/// any single generic base type, so e.g. <c>TransformBlock&lt;int,string&gt;</c> and <c>TransformBlock&lt;int,double&gt;</c>
/// are counted independently of each other.
/// </summary>
internal static class FluxNameGenerator
{
    private static readonly ConcurrentDictionary<Type, int> _counters = new();

    /// <summary>
    /// Generates the next auto-assigned name for <paramref name="blockType"/>, of the form <c>"{TypeName}-{n}"</c>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string Generate(Type blockType)
    {
        var n = _counters.AddOrUpdate(blockType, 1, static (_, count) => count + 1);
        return $"{blockType.Name}-{n}";
    }
}
