namespace LaquaiLib.Flux.Diagnostics;

/// <summary>
/// Tracks queue depth for every live Flux block via weak references to their channel readers, so the
/// <see cref="ObservableGauge{T}"/> instruments in <see cref="FluxMetrics"/> can report per-instance depth without
/// rooting block instances (or their channels) for the lifetime of the process.
/// </summary>
internal static class FluxBlockDiagnostics
{
    private readonly struct Entry(string name, Func<int?> countGetter)
    {
        public readonly string Name = name;
        public readonly Func<int?> CountGetter = countGetter;
    }

    private static readonly LockType _inputLock = new();
    private static readonly List<Entry> _inputEntries = [];

    private static readonly LockType _outputLock = new();
    private static readonly List<Entry> _outputEntries = [];

    /// <summary>Registers <paramref name="reader"/> as the input-side queue-depth source for the block named <paramref name="name"/>.</summary>
    public static void RegisterInput<T>(string name, ChannelReader<T> reader)
    {
        var weakRef = new WeakReference<ChannelReader<T>>(reader);
        lock (_inputLock)
            _inputEntries.Add(new Entry(name, () => weakRef.TryGetTarget(out var r) ? r.Count : null));
    }

    /// <summary>Registers <paramref name="reader"/> as the output-side queue-depth source for the block named <paramref name="name"/>.</summary>
    public static void RegisterOutput<T>(string name, ChannelReader<T> reader)
    {
        var weakRef = new WeakReference<ChannelReader<T>>(reader);
        lock (_outputLock)
            _outputEntries.Add(new Entry(name, () => weakRef.TryGetTarget(out var r) ? r.Count : null));
    }

    /// <summary>Observable-gauge callback backing <see cref="FluxMetrics.InputQueueDepth"/>.</summary>
    public static IEnumerable<Measurement<int>> ObserveInputQueueDepths() => Observe(_inputLock, _inputEntries);

    /// <summary>Observable-gauge callback backing <see cref="FluxMetrics.OutputQueueDepth"/>.</summary>
    public static IEnumerable<Measurement<int>> ObserveOutputQueueDepths() => Observe(_outputLock, _outputEntries);

    // Never holds the registry lock across a yield return: a suspended iterator only resumes whenever the metrics
    // consumer (dotnet-counters/OTel export cadence) decides to pull the next value, which could be arbitrarily
    // delayed, so holding the lock across that suspension point would be a real deadlock risk against concurrent
    // Register calls, not just a style nit. Dead entries (block was GC'd) are purged under the lock up front,
    // before the snapshot is taken, so the list self-prunes without any finalizer or IDisposable dependency.
    private static IEnumerable<Measurement<int>> Observe(LockType registryLock, List<Entry> entries)
    {
        Entry[] snapshot;
        lock (registryLock)
        {
            entries.RemoveAll(static e => e.CountGetter() is null);
            snapshot = [.. entries];
        }

        foreach (var entry in snapshot)
        {
            if (entry.CountGetter() is int count)
                yield return new Measurement<int>(count, new KeyValuePair<string, object>("flux.block.name", entry.Name));
        }
    }
}
