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
