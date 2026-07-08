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
