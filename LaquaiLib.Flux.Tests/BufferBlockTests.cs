namespace LaquaiLib.Flux;

public sealed class BufferBlockTests
{
    private sealed class RecordingTarget<T> : IFluxTarget<T>
    {
        private readonly TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private volatile bool _declining;

        public List<T> Received { get; } = [];
        public bool Completed { get; private set; }

        public string Name => "RecordingTarget";
        public Task Completion => _completionTcs.Task;

        public ValueTask<bool> SendAsync(T item, CancellationToken cancellationToken = default) => new(TryOffer(item));

        public bool TryOffer(T item)
        {
            if (_declining)
            {
                return false;
            }
            lock (Received)
            {
                Received.Add(item);
            }
            return true;
        }

        public void Complete()
        {
            Completed = true;
            _declining = true;
            _completionTcs.TrySetResult();
        }

        public void Fault(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);
            _declining = true;
            _completionTcs.TrySetException(exception);
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(5, cancellationToken);
        }
    }

    // ─────────────────────────────────────────────
    //  Construction
    // ─────────────────────────────────────────────

    [Fact]
    public void Ctor_DefaultOptions_UsesDefaults()
    {
        var block = new BufferBlock<int>();
        Assert.Contains("BufferBlock", block.Name);
    }

    [Fact]
    public void Ctor_NamedOptions_UsesProvidedName()
    {
        var block = new BufferBlock<int>(new FluxBlockOptions { Name = "custom-buffer" });
        Assert.Equal("custom-buffer", block.Name);
    }

    // ─────────────────────────────────────────────
    //  Passthrough correctness
    // ─────────────────────────────────────────────

    [Fact]
    public async Task SendAsync_MultipleItems_PreservesOrder()
    {
        var block = new BufferBlock<int>();
        for (var i = 0; i < 10; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        var results = new List<int>();
        await foreach (var item in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            results.Add(item);
        }

        Assert.Equal([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], results);
    }

    [Fact]
    public async Task LinkTo_MultipleItems_PushesBufferedItemsInOrder()
    {
        var block = new BufferBlock<int>();
        var target = new RecordingTarget<int>();
        using var link = block.LinkTo(target);

        for (var i = 0; i < 5; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        await target.Completion;

        Assert.Equal([0, 1, 2, 3, 4], target.Received);
        Assert.True(target.Completed);
    }

    // ─────────────────────────────────────────────
    //  Fan-out and link management
    //
    //  The fan-out logic itself lives in PropagatorFluxBlockBase and is covered exhaustively by
    //  TransformBlockTests. It is re-covered here because BufferBlock is the one block whose dispatch loop reads
    //  the very same channel its producers write to, so "the dispatch loop sees every produced item" is a
    //  structurally different claim for it than for every other block.
    // ─────────────────────────────────────────────

    [Fact]
    public async Task LinkTo_Broadcast_MultiLink_EveryTargetReceivesEveryItem()
    {
        var block = new BufferBlock<int>(new FluxBlockOptions { FanOutMode = FluxFanOutMode.Broadcast });
        var first = new RecordingTarget<int>();
        var second = new RecordingTarget<int>();
        using var linkA = block.LinkTo(first);
        using var linkB = block.LinkTo(second);

        for (var i = 0; i < 3; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        await first.Completion;
        await second.Completion;

        Assert.Equal([0, 1, 2], first.Received);
        Assert.Equal([0, 1, 2], second.Received);
    }

    [Fact]
    public async Task LinkTo_RoundRobin_MultiLink_DistributesAcrossTargets()
    {
        var block = new BufferBlock<int>(new FluxBlockOptions { FanOutMode = FluxFanOutMode.RoundRobin });
        var first = new RecordingTarget<int>();
        var second = new RecordingTarget<int>();
        using var linkA = block.LinkTo(first);
        using var linkB = block.LinkTo(second);

        for (var i = 0; i < 4; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        await first.Completion;
        await second.Completion;

        // Both targets always accept synchronously, so the rotating cursor alternates deterministically.
        Assert.Equal([0, 2], first.Received);
        Assert.Equal([1, 3], second.Received);
    }

    [Fact]
    public async Task LinkTo_FirstAvailable_MultiLink_HighestPriorityTargetTakesEverything()
    {
        var block = new BufferBlock<int>(new FluxBlockOptions { FanOutMode = FluxFanOutMode.FirstAvailable });
        var primary = new RecordingTarget<int>();
        var fallback = new RecordingTarget<int>();
        using var linkA = block.LinkTo(primary);
        using var linkB = block.LinkTo(fallback);

        for (var i = 0; i < 3; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        await primary.Completion;
        await fallback.Completion;

        Assert.Equal([0, 1, 2], primary.Received);
        Assert.Empty(fallback.Received);
    }

    [Fact]
    public async Task LinkTo_PerLinkFilter_RoutesOnlyMatchingItemsToEachTarget()
    {
        var block = new BufferBlock<int>(new FluxBlockOptions { FanOutMode = FluxFanOutMode.Broadcast });
        var evens = new RecordingTarget<int>();
        var odds = new RecordingTarget<int>();
        using var linkA = block.LinkTo(evens, filter: static i => i % 2 == 0);
        using var linkB = block.LinkTo(odds, filter: static i => i % 2 != 0);

        for (var i = 0; i < 6; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        await evens.Completion;
        await odds.Completion;

        Assert.Equal([0, 2, 4], evens.Received);
        Assert.Equal([1, 3, 5], odds.Received);
    }

    [Fact]
    public async Task LinkTo_AfterItemsAlreadyBuffered_DispatchDrainsTheBacklog()
    {
        // The dispatch loop only starts on the first LinkTo, and with one shared channel the backlog it has to
        // pick up is sitting in the very channel producers wrote to.
        var block = new BufferBlock<int>();
        for (var i = 0; i < 4; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }

        var target = new RecordingTarget<int>();
        using var link = block.LinkTo(target);
        block.Complete();

        await target.Completion;
        Assert.Equal([0, 1, 2, 3], target.Received);
    }

    [Fact]
    public async Task LinkTo_UnlinkedMidStream_StopsReceiving_AndARelinkedTargetResumes()
    {
        var block = new BufferBlock<int>();
        var first = new RecordingTarget<int>();
        var link = block.LinkTo(first);

        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await WaitUntilAsync(() => first.Received.Count == 1, TestContext.Current.CancellationToken);
        link.Dispose();

        var second = new RecordingTarget<int>();
        using var relink = block.LinkTo(second);
        await block.SendAsync(2, TestContext.Current.CancellationToken);
        block.Complete();

        await second.Completion;
        Assert.Equal([1], first.Received);
        Assert.Equal([2], second.Received);
    }

    [Fact]
    public async Task SendAsync_ConcurrentProducers_EveryItemArrivesExactlyOnce()
    {
        // The shared channel is written by arbitrary producers and read directly by the consumer, so its
        // multi-writer configuration is doing real work here that a pump loop used to hide.
        const int producerCount = 8;
        const int perProducer = 500;
        var block = new BufferBlock<int>(new FluxBlockOptions { BoundedCapacity = 16 });

        var results = new List<int>();
        var consume = Task.Run(async () =>
        {
            await foreach (var item in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
            {
                results.Add(item);
            }
        }, TestContext.Current.CancellationToken);

        var producers = new Task[producerCount];
        for (var p = 0; p < producerCount; p++)
        {
            var offset = p * perProducer;
            producers[p] = Task.Run(async () =>
            {
                for (var i = 0; i < perProducer; i++)
                {
                    await block.SendAsync(offset + i, TestContext.Current.CancellationToken);
                }
            }, TestContext.Current.CancellationToken);
        }

        await Task.WhenAll(producers);
        block.Complete();
        await consume;

        Assert.Equal(producerCount * perProducer, results.Count);
        Assert.Equal(producerCount * perProducer, results.Distinct().Count());
    }

    // ─────────────────────────────────────────────
    //  Completion propagation
    // ─────────────────────────────────────────────

    [Fact]
    public async Task Complete_LinkedTarget_PropagateCompletionTrue_CompletesTarget()
    {
        var block = new BufferBlock<int>();
        var target = new RecordingTarget<int>();
        using var link = block.LinkTo(target, new FluxLinkOptions { PropagateCompletion = true });

        await block.SendAsync(1, TestContext.Current.CancellationToken);
        block.Complete();

        await target.Completion;
        Assert.True(target.Completed);
    }

    [Fact]
    public async Task Complete_LinkedTarget_PropagateCompletionFalse_DoesNotCompleteTarget()
    {
        var block = new BufferBlock<int>();
        var target = new RecordingTarget<int>();
        using var link = block.LinkTo(target, new FluxLinkOptions { PropagateCompletion = false });

        await block.SendAsync(1, TestContext.Current.CancellationToken);
        block.Complete();

        await block.Completion;
        await WaitUntilAsync(() => target.Received.Count > 0, TestContext.Current.CancellationToken);

        Assert.Equal([1], target.Received);
        Assert.False(target.Completed);
    }

    [Fact]
    public async Task Fault_LinkedTarget_PropagatesRegardlessOfPropagateCompletionFlag()
    {
        var block = new BufferBlock<int>();
        var target = new RecordingTarget<int>();
        using var link = block.LinkTo(target, new FluxLinkOptions { PropagateCompletion = false });

        var ex = new InvalidOperationException("boom");
        block.Fault(ex);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await target.Completion);
    }

    [Fact]
    public async Task Complete_ItemsStillBuffered_CompletionPendingUntilDrained()
    {
        // The block holds one buffer, so it is not done while that buffer still holds items nobody has taken.
        // Nothing is linked and nothing is consuming here, so Complete() alone must not resolve Completion.
        var block = new BufferBlock<int>();
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);
        block.Complete();

        var delay = Task.Delay(200, TestContext.Current.CancellationToken);
        Assert.Same(delay, await Task.WhenAny(block.Completion, delay));
        Assert.False(block.Completion.IsCompleted);

        var results = new List<int>();
        await foreach (var item in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            results.Add(item);
        }

        await block.Completion;
        Assert.Equal([1, 2], results);
        Assert.Equal(TaskStatus.RanToCompletion, block.Completion.Status);
    }

    [Fact]
    public async Task Fault_ItemsStillBuffered_DiscardsAndResolvesWithoutADrain()
    {
        // Unlike Complete, Fault is decisive: IFluxTarget<T>.Fault promises queued items are discarded, and a
        // bounded channel would otherwise withhold its completion signal until someone drained them.
        var block = new BufferBlock<int>();
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);

        var ex = new InvalidOperationException("boom");
        block.Fault(ex);

        var faulted = await Assert.ThrowsAsync<InvalidOperationException>(async () => await block.Completion);
        Assert.Same(ex, faulted);

        var results = new List<int>();
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var item in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
            {
                results.Add(item);
            }
        });
        Assert.Empty(results);
    }

    [Fact]
    public async Task CancellationToken_CanceledAfterItemsBuffered_CompletionResolvesCanceled()
    {
        // Exercises the cancellation path against a live, non-empty channel. There is no pump loop awaiting the
        // token, so the block's finalizer is the only thing that can observe it - if that ever regresses, this
        // hangs rather than fails.
        using var cts = new CancellationTokenSource();
        var block = new BufferBlock<int>(new FluxBlockOptions { CancellationToken = cts.Token });
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);

        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await block.Completion);
        Assert.Equal(TaskStatus.Canceled, block.Completion.Status);
    }

    // ─────────────────────────────────────────────
    //  Backpressure
    // ─────────────────────────────────────────────

    [Fact]
    public async Task SendAsync_SmallCapacity_ConcurrentProduceAndConsume_NoDeadlock()
    {
        var block = new BufferBlock<int>(new FluxBlockOptions { BoundedCapacity = 4 });
        const int itemCount = 500;

        var results = new List<int>();
        var consume = Task.Run(async () =>
        {
            await foreach (var item in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
            {
                results.Add(item);
            }
        }, TestContext.Current.CancellationToken);

        var produce = Task.Run(async () =>
        {
            for (var i = 0; i < itemCount; i++)
            {
                await block.SendAsync(i, TestContext.Current.CancellationToken);
            }
            block.Complete();
        }, TestContext.Current.CancellationToken);

        await Task.WhenAll(produce, consume);

        Assert.Equal(itemCount, results.Count);
        for (var i = 0; i < itemCount; i++)
        {
            Assert.Equal(i, results[i]);
        }
    }

    // ─────────────────────────────────────────────
    //  SendAsync / Post semantics
    // ─────────────────────────────────────────────

    [Fact]
    public async Task SendAsync_ReturnsFalse_AfterComplete()
    {
        var block = new BufferBlock<int>();
        block.Complete();
        var result = await block.SendAsync(1, TestContext.Current.CancellationToken);
        Assert.False(result);
    }

    [Fact]
    public void Post_ReturnsTrue_WhenCapacityAvailable()
    {
        var block = new BufferBlock<int>();
        Assert.True(block.Post(1));
    }

    // ─────────────────────────────────────────────
    //  Cancellation
    // ─────────────────────────────────────────────

    [Fact]
    public async Task CancellationToken_Canceled_CompletionResolvesCanceled()
    {
        using var cts = new CancellationTokenSource();
        var block = new BufferBlock<int>(new FluxBlockOptions { CancellationToken = cts.Token });
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await block.Completion);
        Assert.Equal(TaskStatus.Canceled, block.Completion.Status);
    }
}
