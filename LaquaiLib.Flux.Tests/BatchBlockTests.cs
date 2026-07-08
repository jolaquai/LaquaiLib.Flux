namespace LaquaiLib.Flux;

public sealed class BatchBlockTests
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
    public void Ctor_BatchSizeLessThan1_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new BatchBlock<int>(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new BatchBlock<int>(-1, Timeout.InfiniteTimeSpan));
    }

    [Fact]
    public void Ctor_NullOptions_Ok()
    {
        var block = new BatchBlock<int>(4, Timeout.InfiniteTimeSpan, null);
        Assert.Contains("BatchBlock", block.Name);
    }

    [Fact]
    public void Ctor_DefaultOptions_UsesDefaults()
    {
        var block = new BatchBlock<int>(4);
        Assert.Contains("BatchBlock", block.Name);
    }

    [Fact]
    public void Ctor_NamedOptions_UsesProvidedName()
    {
        var block = new BatchBlock<int>(4, Timeout.InfiniteTimeSpan, new FluxBlockOptions { Name = "custom-batch" });
        Assert.Equal("custom-batch", block.Name);
    }

    // ─────────────────────────────────────────────
    //  Basic batching correctness
    // ─────────────────────────────────────────────

    [Fact]
    public async Task SendAsync_ExactBatchSize_EmitsSingleFullBatch()
    {
        var block = new BatchBlock<int>(4);
        for (var i = 0; i < 4; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        var batches = new List<int[]>();
        await foreach (var batch in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            batches.Add(batch.Memory.ToArray());
            batch.Dispose();
        }

        var single = Assert.Single(batches);
        Assert.Equal([0, 1, 2, 3], single);
    }

    [Fact]
    public async Task SendAsync_ExactMultipleOfBatchSize_EmitsExactCount_NoTrailingEmptyBatch()
    {
        const int batchSize = 10;
        const int batchCount = 5;
        var block = new BatchBlock<int>(batchSize);

        for (var i = 0; i < batchSize * batchCount; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        var batches = new List<int[]>();
        await foreach (var batch in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            Assert.Equal(batchSize, batch.Count);
            batches.Add(batch.Memory.ToArray());
            batch.Dispose();
        }

        Assert.Equal(batchCount, batches.Count);
        for (var b = 0; b < batchCount; b++)
        {
            for (var i = 0; i < batchSize; i++)
            {
                Assert.Equal(b * batchSize + i, batches[b][i]);
            }
        }
    }

    [Fact]
    public async Task Complete_PartialBatch_EmittedAsFinalShorterBatch()
    {
        // 5 items, size 2 => [2],[2],[1]
        var block = new BatchBlock<int>(2);
        for (var i = 0; i < 5; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        var batches = new List<int[]>();
        await foreach (var batch in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            batches.Add(batch.Memory.ToArray());
            batch.Dispose();
        }

        Assert.Equal(3, batches.Count);
        Assert.Equal([0, 1], batches[0]);
        Assert.Equal([2, 3], batches[1]);
        Assert.Equal([4], batches[2]);
    }

    [Fact]
    public async Task Complete_NoItemsSent_ZeroBatchesEmitted_CompletionRanToCompletion()
    {
        var block = new BatchBlock<int>(4);
        block.Complete();

        var batches = new List<int[]>();
        await foreach (var batch in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            batches.Add(batch.Memory.ToArray());
            batch.Dispose();
        }

        Assert.Empty(batches);
        await block.Completion;
        Assert.Equal(TaskStatus.RanToCompletion, block.Completion.Status);
    }

    // ─────────────────────────────────────────────
    //  Time-based flush
    // ─────────────────────────────────────────────

    [Fact]
    public async Task TimeFlush_FiniteInterval_PartialBatchSurfacesBeforeComplete()
    {
        var block = new BatchBlock<int>(100, TimeSpan.FromMilliseconds(50));
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);

        int[] batch = null;
        await using var enumerator = block.ReceiveAllAsync(TestContext.Current.CancellationToken).GetAsyncEnumerator();

        var moveNextTask = enumerator.MoveNextAsync().AsTask();
        var completed = await Task.WhenAny(moveNextTask, Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken));
        Assert.Same(moveNextTask, completed);
        Assert.True(await moveNextTask);

        batch = enumerator.Current.Memory.ToArray();
        enumerator.Current.Dispose();

        Assert.Equal([1, 2], batch);
        block.Complete();
    }

    [Fact]
    public async Task TimeFlush_InfiniteTimeSpan_NeverTimeFlushes_PartialStaysBufferedUntilComplete()
    {
        var block = new BatchBlock<int>(100, Timeout.InfiniteTimeSpan);
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);

        await using var enumerator = block.ReceiveAllAsync(TestContext.Current.CancellationToken).GetAsyncEnumerator();
        var moveNextTask = enumerator.MoveNextAsync().AsTask();

        // Give a generous window during which a (mis-)firing timer would have surfaced a batch; assert it does not.
        var delay = Task.Delay(300, TestContext.Current.CancellationToken);
        var completed = await Task.WhenAny(moveNextTask, delay);
        Assert.Same(delay, completed);
        Assert.False(moveNextTask.IsCompleted);

        block.Complete();
        Assert.True(await moveNextTask);
        var batch = enumerator.Current.Memory.ToArray();
        enumerator.Current.Dispose();
        Assert.Equal([1, 2], batch);
    }

    // ─────────────────────────────────────────────
    //  Linking / completion / fault propagation
    // ─────────────────────────────────────────────

    [Fact]
    public async Task LinkTo_PushesBatchesToTarget()
    {
        var block = new BatchBlock<int>(2);
        var target = new RecordingTarget<PooledBatch<int>>();
        using var link = block.LinkTo(target);

        for (var i = 0; i < 4; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        await target.Completion;

        Assert.Equal(2, target.Received.Count);
        Assert.Equal([0, 1], target.Received[0].Memory.ToArray());
        Assert.Equal([2, 3], target.Received[1].Memory.ToArray());
        Assert.True(target.Completed);

        foreach (var batch in target.Received)
        {
            batch.Dispose();
        }
    }

    [Fact]
    public async Task Fault_LinkedTarget_PropagatesRegardlessOfPropagateCompletionFlag()
    {
        var block = new BatchBlock<int>(2);
        var target = new RecordingTarget<PooledBatch<int>>();
        using var link = block.LinkTo(target, new FluxLinkOptions { PropagateCompletion = false });

        var ex = new InvalidOperationException("boom");
        block.Fault(ex);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await target.Completion);
    }

    // ─────────────────────────────────────────────
    //  Backpressure
    // ─────────────────────────────────────────────

    [Fact]
    public async Task SendAsync_SmallOutputCapacity_ConcurrentProduceAndConsume_NoDeadlock()
    {
        var block = new BatchBlock<int>(4, Timeout.InfiniteTimeSpan, new FluxBlockOptions { BoundedCapacity = 2 });
        const int itemCount = 400;

        var batches = new List<int[]>();
        var consume = Task.Run(async () =>
        {
            await foreach (var batch in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
            {
                batches.Add(batch.Memory.ToArray());
                batch.Dispose();
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

        Assert.Equal(itemCount, batches.Sum(b => b.Length));
        var flattened = batches.SelectMany(b => b).ToList();
        for (var i = 0; i < itemCount; i++)
        {
            Assert.Equal(i, flattened[i]);
        }
    }

    // ─────────────────────────────────────────────
    //  Cancellation
    // ─────────────────────────────────────────────

    [Fact]
    public async Task CancellationToken_Canceled_CompletionResolvesCanceled()
    {
        using var cts = new CancellationTokenSource();
        var block = new BatchBlock<int>(4, Timeout.InfiniteTimeSpan, new FluxBlockOptions { CancellationToken = cts.Token });
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await block.Completion);
        Assert.Equal(TaskStatus.Canceled, block.Completion.Status);
    }

    // ─────────────────────────────────────────────
    //  PooledBatch<T>
    // ─────────────────────────────────────────────

    [Fact]
    public async Task PooledBatch_CountMemorySpanIndexer_SlicedToCountEvenWhenOverRented()
    {
        // batchSize deliberately small and not a typical ArrayPool bucket size, so the rented array is
        // likely larger than requested - every accessor must still report/slice to Count, not the array's length.
        var block = new BatchBlock<int>(3);
        for (var i = 0; i < 3; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        await using var enumerator = block.ReceiveAllAsync(TestContext.Current.CancellationToken).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        var batch = enumerator.Current;

        Assert.Equal(3, batch.Count);
        Assert.Equal(3, batch.Memory.Length);
        Assert.Equal(3, batch.Span.Length);
        Assert.Equal(0, batch[0]);
        Assert.Equal(1, batch[1]);
        Assert.Equal(2, batch[2]);

        Assert.Throws<ArgumentOutOfRangeException>(() => batch[3]);
        Assert.Throws<ArgumentOutOfRangeException>(() => batch[-1]);

        // Single dispose must not throw.
        var exception = Record.Exception(batch.Dispose);
        Assert.Null(exception);
    }
}
