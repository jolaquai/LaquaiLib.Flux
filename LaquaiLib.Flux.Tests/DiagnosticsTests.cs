using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

using LaquaiLib.Flux.Diagnostics;

namespace LaquaiLib.Flux;

/// <summary>
/// Audits the library's observability pitch (see <c>CLAUDE.flux-plan.md</c>) - queue depth, per-stage latency,
/// drop/backpressure counts, discoverable by <c>dotnet-counters</c>/an OpenTelemetry exporter with zero extra
/// configuration - by actually subscribing a <see cref="MeterListener"/> the same way those tools do and
/// verifying every instrument reports correctly, rather than trusting that the <c>Enabled</c>-gated recording
/// call sites are wired correctly just because they compile.
/// </summary>
public sealed class DiagnosticsTests
{
    private sealed record LongMeasurement(Instrument Instrument, long Value, string BlockName);
    private sealed record DoubleMeasurement(Instrument Instrument, double Value, string BlockName);
    private sealed record IntMeasurement(Instrument Instrument, int Value, string BlockName);

    /// <summary>
    /// Subscribes to every instrument on the shared Flux <see cref="Meter"/> exactly the way an external listener
    /// (dotnet-counters, an OTel exporter) would, and buckets every measurement by instrument identity and the
    /// <c>flux.block.name</c> tag. Every test below constructs its own instance and gives its block(s) a unique
    /// name, so filtering captured measurements by that name is safe even though <see cref="MeterListener"/> is
    /// process-wide and other tests' blocks may be producing measurements concurrently.
    /// </summary>
    private sealed class MetricsCapture : IDisposable
    {
        private readonly MeterListener _listener;

        public ConcurrentBag<LongMeasurement> Longs { get; } = [];
        public ConcurrentBag<DoubleMeasurement> Doubles { get; } = [];
        public ConcurrentBag<IntMeasurement> Ints { get; } = [];

        public MetricsCapture()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (instrument.Meter.Name == FluxMetrics.MeterName)
                    {
                        listener.EnableMeasurementEvents(instrument);
                    }
                },
            };
            _listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) => Longs.Add(new(instrument, measurement, GetBlockNameTag(tags))));
            _listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, _) => Doubles.Add(new(instrument, measurement, GetBlockNameTag(tags))));
            _listener.SetMeasurementEventCallback<int>((instrument, measurement, tags, _) => Ints.Add(new(instrument, measurement, GetBlockNameTag(tags))));
            _listener.Start();
        }

        /// <summary>Pulls a fresh sample from every <see cref="ObservableGauge{T}"/> (queue depth) instrument - these are pull-based and never report otherwise.</summary>
        public void RecordObservableInstruments() => _listener.RecordObservableInstruments();

        public void Dispose() => _listener.Dispose();
    }

    private static string GetBlockNameTag(ReadOnlySpan<KeyValuePair<string, object>> tags)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == "flux.block.name")
            {
                return tag.Value as string;
            }
        }
        return null;
    }

    /// <summary>Always-accepting sink used purely to give a block something to link to; not itself under test.</summary>
    private sealed class DiscardTarget<T> : IFluxTarget<T>
    {
        private readonly TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string Name => "DiscardTarget";
        public Task Completion => _completionTcs.Task;
        public ValueTask<bool> SendAsync(T item, CancellationToken cancellationToken = default) => new(true);
        public bool TryOffer(T item) => true;
        public void Complete() => _completionTcs.TrySetResult();
        public void Fault(Exception exception) => _completionTcs.TrySetException(exception);
    }

    [Fact]
    public void FluxMeter_Name_MatchesTheLiteralExternalConsumersWouldOtherwiseHaveToHardcode()
    {
        // FluxMetrics (the actual instruments) is internal, so before FluxMeter existed, an external OTel/
        // dotnet-counters config had no compile-time-checked way to reference the meter name - just the magic
        // string "LaquaiLib.Flux" from documentation, silently driftable on a rename. This pins the public
        // constant to that exact literal so such a drift fails a build here, not silently in a consumer.
        Assert.Equal("LaquaiLib.Flux", FluxMeter.Name);
    }

    [Fact]
    public async Task Meter_IsDiscoverableByPublicFluxMeterName_WithoutAnyInternalTypeReference()
    {
        // Mirrors exactly what dotnet-counters/an OTel exporter does: subscribe purely by the public meter name,
        // with zero reference to any internal Flux type (FluxMetrics itself, or its instrument fields).
        var discovered = new HashSet<string>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == FluxMeter.Name)
                {
                    discovered.Add(instrument.Name);
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.Start();

        // FluxMetrics's instruments are all static readonly fields on one class, created together the first time
        // anything actually touches it - which nothing does until an item is sent (TryOffer is the first
        // reference). Merely constructing a block never triggers this, so exercise it for real.
        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { Name = nameof(Meter_IsDiscoverableByPublicFluxMeterName_WithoutAnyInternalTypeReference) });
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        block.Complete();
        await block.Completion;

        listener.RecordObservableInstruments();

        Assert.Contains("flux.block.items_accepted", discovered);
        Assert.Contains("flux.block.items_processed", discovered);
        Assert.Contains("flux.block.items_dropped", discovered);
        Assert.Contains("flux.block.backpressure_events", discovered);
        Assert.Contains("flux.block.stage_latency", discovered);
        Assert.Contains("flux.block.input_queue_depth", discovered);
        Assert.Contains("flux.block.output_queue_depth", discovered);
    }

    [Fact]
    public async Task ItemsAccepted_RecordsOnePerAcceptedItem_TaggedWithBlockName()
    {
        var blockName = nameof(ItemsAccepted_RecordsOnePerAcceptedItem_TaggedWithBlockName);
        using var capture = new MetricsCapture();

        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { Name = blockName });
        for (var i = 0; i < 5; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();
        await block.Completion;

        var mine = capture.Longs.Where(m => m.Instrument == FluxMetrics.ItemsAccepted && m.BlockName == blockName).ToList();
        Assert.Equal(5, mine.Count);
        Assert.All(mine, m => Assert.Equal(1, m.Value));
    }

    [Fact]
    public async Task ItemsAccepted_TwoConcurrentBlocks_AreTaggedIndependently()
    {
        var nameA = nameof(ItemsAccepted_TwoConcurrentBlocks_AreTaggedIndependently) + "-A";
        var nameB = nameof(ItemsAccepted_TwoConcurrentBlocks_AreTaggedIndependently) + "-B";
        using var capture = new MetricsCapture();

        var blockA = new TransformBlock<int, int>(item => item, new FluxBlockOptions { Name = nameA });
        var blockB = new TransformBlock<int, int>(item => item, new FluxBlockOptions { Name = nameB });

        await blockA.SendAsync(1, TestContext.Current.CancellationToken);
        await blockB.SendAsync(1, TestContext.Current.CancellationToken);
        await blockB.SendAsync(2, TestContext.Current.CancellationToken);
        blockA.Complete();
        blockB.Complete();
        await blockA.Completion;
        await blockB.Completion;

        var acceptedA = capture.Longs.Count(m => m.Instrument == FluxMetrics.ItemsAccepted && m.BlockName == nameA);
        var acceptedB = capture.Longs.Count(m => m.Instrument == FluxMetrics.ItemsAccepted && m.BlockName == nameB);
        Assert.Equal(1, acceptedA);
        Assert.Equal(2, acceptedB);
    }

    [Fact]
    public async Task ItemsProcessed_RecordsOnePerProcessedItem()
    {
        var blockName = nameof(ItemsProcessed_RecordsOnePerProcessedItem);
        using var capture = new MetricsCapture();

        var block = new TransformBlock<int, int>(item => item * 2, new FluxBlockOptions { Name = blockName });
        for (var i = 0; i < 7; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();
        await block.Completion;

        var mine = capture.Longs.Where(m => m.Instrument == FluxMetrics.ItemsProcessed && m.BlockName == blockName).ToList();
        Assert.Equal(7, mine.Count);
    }

    [Fact]
    public async Task ItemsProcessed_BufferBlock_RecordsOnePerItemDespiteHavingNoProcessingLoop()
    {
        // BufferBlock is a pure passthrough with a single channel and no pump loop to report from, so accepting
        // is what counts as processing for it. Without that, a pipeline's per-stage throughput would show a hole
        // wherever a buffer sits.
        var blockName = nameof(ItemsProcessed_BufferBlock_RecordsOnePerItemDespiteHavingNoProcessingLoop);
        using var capture = new MetricsCapture();

        var block = new BufferBlock<int>(new FluxBlockOptions { Name = blockName });
        for (var i = 0; i < 5; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();
        await foreach (var _ in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
        }
        await block.Completion;

        var processed = capture.Longs.Count(m => m.Instrument == FluxMetrics.ItemsProcessed && m.BlockName == blockName);
        Assert.Equal(5, processed);
    }

    [Fact]
    public async Task ItemsDropped_RecordsWhenNoLinkedFilterMatches()
    {
        var blockName = nameof(ItemsDropped_RecordsWhenNoLinkedFilterMatches);
        using var capture = new MetricsCapture();

        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { Name = blockName });
        var target = new DiscardTarget<int>();
        using var link = block.LinkTo(target, filter: _ => false);

        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);
        block.Complete();

        await block.Completion;
        await target.Completion;

        var mine = capture.Longs.Where(m => m.Instrument == FluxMetrics.ItemsDropped && m.BlockName == blockName).ToList();
        Assert.Equal(2, mine.Count);
        Assert.All(mine, m => Assert.Equal(1, m.Value));
    }

    [Fact]
    public async Task BackpressureEvents_RecordsWhenSendAsyncGenuinelySuspends()
    {
        var blockName = nameof(BackpressureEvents_RecordsWhenSendAsyncGenuinelySuspends);
        using var capture = new MetricsCapture();

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var block = new TransformBlock<int, int>(async item =>
        {
            await gate.Task;
            return item;
        }, new FluxBlockOptions { Name = blockName, BoundedCapacity = 1 });

        // BoundedCapacity sizes the output channel identically to the input channel, so with nobody draining
        // output, the worker would itself deadlock trying to write a second processed result into an
        // already-full output channel - drain it concurrently, same as a real consumer would.
        var results = new List<int>();
        var drain = Task.Run(async () =>
        {
            await foreach (var result in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
            {
                results.Add(result);
            }
        }, TestContext.Current.CancellationToken);

        // Item 0 is dequeued and stuck on the gate; item 1 fills the one-slot input buffer.
        await block.SendAsync(0, TestContext.Current.CancellationToken);
        await block.SendAsync(1, TestContext.Current.CancellationToken);

        // No capacity left - this must genuinely suspend until the gate opens and item 0 drains.
        var sendTask = block.SendAsync(2, TestContext.Current.CancellationToken);
        await Task.Delay(30, TestContext.Current.CancellationToken);
        Assert.False(sendTask.IsCompleted);

        gate.SetResult();
        Assert.True(await sendTask);

        block.Complete();
        await block.Completion;
        await drain;

        var mine = capture.Longs.Where(m => m.Instrument == FluxMetrics.BackpressureEvents && m.BlockName == blockName).ToList();
        Assert.NotEmpty(mine);
    }

    [Fact]
    public async Task StageLatency_RecordsFiniteNonNegativeValue_PerProcessedItem()
    {
        var blockName = nameof(StageLatency_RecordsFiniteNonNegativeValue_PerProcessedItem);
        using var capture = new MetricsCapture();

        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { Name = blockName });
        for (var i = 0; i < 4; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();
        await block.Completion;

        var mine = capture.Doubles.Where(m => m.Instrument == FluxMetrics.StageLatency && m.BlockName == blockName).ToList();
        Assert.Equal(4, mine.Count);
        Assert.All(mine, m => Assert.True(double.IsFinite(m.Value) && m.Value >= 0));
    }

    [Fact]
    public async Task InputQueueDepth_ReflectsItemsPendingInInputChannel()
    {
        var blockName = nameof(InputQueueDepth_ReflectsItemsPendingInInputChannel);
        using var capture = new MetricsCapture();

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var block = new TransformBlock<int, int>(async item =>
        {
            await gate.Task;
            return item;
        }, new FluxBlockOptions { Name = blockName, BoundedCapacity = 10 });

        // DOP defaults to 1: the single worker dequeues item 0 and blocks on the gate, so items 1-3 pile up in
        // the input channel itself (Channel<T>.Reader.Count only counts items still sitting in the channel, not
        // the one already handed to the blocked worker).
        await block.SendAsync(0, TestContext.Current.CancellationToken);
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);
        await block.SendAsync(3, TestContext.Current.CancellationToken);

        await Task.Delay(30, TestContext.Current.CancellationToken); // let the worker actually dequeue item 0 first
        capture.RecordObservableInstruments();

        gate.SetResult();
        block.Complete();
        await block.Completion;

        var mine = capture.Ints.Where(m => m.Instrument == FluxMetrics.InputQueueDepth && m.BlockName == blockName).ToList();
        Assert.Single(mine);
        Assert.Equal(3, mine[0].Value);
    }

    [Fact]
    public async Task OutputQueueDepth_ReflectsItemsPendingInOutputChannel()
    {
        var blockName = nameof(OutputQueueDepth_ReflectsItemsPendingInOutputChannel);
        using var capture = new MetricsCapture();

        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { Name = blockName, BoundedCapacity = 10 });

        // No link, no ReceiveAllAsync consumer yet - produced items just sit in the output channel.
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);
        await block.SendAsync(3, TestContext.Current.CancellationToken);

        await Task.Delay(30, TestContext.Current.CancellationToken); // let the (trivial, instant) transform actually publish all three
        capture.RecordObservableInstruments();

        block.Complete();
        var results = new List<int>();
        await foreach (var result in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            results.Add(result);
        }

        var mine = capture.Ints.Where(m => m.Instrument == FluxMetrics.OutputQueueDepth && m.BlockName == blockName).ToList();
        Assert.Single(mine);
        Assert.Equal(3, mine[0].Value);
    }
}
