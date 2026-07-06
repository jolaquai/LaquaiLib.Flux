using System.Collections.Concurrent;

namespace LaquaiLib.Flux;

public sealed class TransformBlockTests
{
    private sealed class RecordingTarget<T> : IFluxTarget<T>
    {
        private readonly TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public List<T> Received { get; } = [];
        public bool Completed { get; private set; }

        public string Name => "RecordingTarget";
        public Task Completion => _completionTcs.Task;

        public ValueTask<bool> SendAsync(T item, CancellationToken cancellationToken = default) => new(TryOffer(item));

        public bool TryOffer(T item)
        {
            lock (Received)
            {
                Received.Add(item);
            }
            return true;
        }

        public void Complete()
        {
            Completed = true;
            _completionTcs.TrySetResult();
        }

        public void Fault(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);
            _completionTcs.TrySetException(exception);
        }
    }

    private static Func<int, ValueTask<int>> GatedTransform(ConcurrentDictionary<int, TaskCompletionSource> gates) => async item =>
    {
        var tcs = gates.GetOrAdd(item, static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        await tcs.Task;
        return item;
    };

    private static void Release(ConcurrentDictionary<int, TaskCompletionSource> gates, int item)
        => gates.GetOrAdd(item, static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).SetResult();

    // ───────────────────────────────────────────────
    //  Construction
    // ───────────────────────────────────────────────

    [Fact]
    public void Ctor_SyncTransform_NullDelegate_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new TransformBlock<int, int>((Func<int, int>)null));
    }

    [Fact]
    public void Ctor_AsyncTransform_NullDelegate_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new TransformBlock<int, int>((Func<int, ValueTask<int>>)null));
    }

    [Fact]
    public void Ctor_DefaultOptions_UsesDefaults()
    {
        var block = new TransformBlock<int, int>(item => item);
        Assert.NotNull(block.Name);
        Assert.Contains("TransformBlock", block.Name);
    }

    [Fact]
    public void Ctor_NamedOptions_UsesProvidedName()
    {
        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { Name = "my-stage" });
        Assert.Equal("my-stage", block.Name);
    }

    // ───────────────────────────────────────────────
    //  Basic transform correctness
    // ───────────────────────────────────────────────

    [Fact]
    public async Task SendAsync_SingleItem_SyncTransform_ProducesExpectedOutput()
    {
        var block = new TransformBlock<int, int>(item => item * 2);
        await block.SendAsync(21, TestContext.Current.CancellationToken);
        block.Complete();

        var results = new List<int>();
        await foreach (var result in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            results.Add(result);
        }

        Assert.Equal([42], results);
    }

    [Fact]
    public async Task SendAsync_SingleItem_AsyncTransform_ProducesExpectedOutput()
    {
        var block = new TransformBlock<int, int>(item => new ValueTask<int>(item * 2));
        await block.SendAsync(21, TestContext.Current.CancellationToken);
        block.Complete();

        var results = new List<int>();
        await foreach (var result in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            results.Add(result);
        }

        Assert.Equal([42], results);
    }

    [Fact]
    public async Task SendAsync_MultipleItems_MaxDegreeOfParallelism1_AllProcessed()
    {
        var block = new TransformBlock<int, int>(item => item * 2);
        for (var i = 0; i < 5; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        var results = new List<int>();
        await foreach (var result in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            results.Add(result);
        }

        Assert.Equal([0, 2, 4, 6, 8], results);
    }

    [Fact]
    public async Task SendAsync_ReturnsTrue_WhenAccepted()
    {
        var block = new TransformBlock<int, int>(item => item);
        Assert.True(await block.SendAsync(1, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SendAsync_ReturnsFalse_AfterComplete()
    {
        var block = new TransformBlock<int, int>(item => item);
        block.Complete();
        Assert.False(await block.SendAsync(1, TestContext.Current.CancellationToken));
    }

    [Fact]
    public void Post_ReturnsTrue_WhenCapacityAvailable()
    {
        var block = new TransformBlock<int, int>(item => item);
        Assert.True(block.Post(1));
    }

    [Fact]
    public async Task Post_ReturnsFalse_WhenNoCapacityAvailable_AndNeverAcceptsItLater()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var block = new TransformBlock<int, int>(async item =>
        {
            await gate.Task;
            return item;
        }, new FluxBlockOptions { BoundedCapacity = 1 });

        // Same setup as the SendAsync backpressure test: item 0 is dequeued and gets stuck on the gate, item 1
        // fills the one-slot buffer, so a third item has nowhere to go and Post must decline it outright - not
        // just decline synchronously while quietly still accepting it once room frees up.
        await block.SendAsync(0, TestContext.Current.CancellationToken);
        await block.SendAsync(1, TestContext.Current.CancellationToken);

        Assert.False(block.Post(2));

        gate.SetResult();
        block.Complete();

        var results = new List<int>();
        await foreach (var result in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            results.Add(result);
        }

        Assert.Equal([0, 1], results);
    }

    [Fact]
    public async Task Post_UnderConcurrentContention_NeverOverAcceptsAndNeverLateAccepts()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var block = new TransformBlock<int, int>(async item =>
        {
            await gate.Task;
            return item;
        }, new FluxBlockOptions { BoundedCapacity = 1 });

        // Deterministically drive the block to a known-full state first (same setup as the single-threaded
        // backpressure test): item 0 is dequeued and stuck on the gate, item 1 fills the one-slot buffer. From
        // here on the block cannot accept anything else until the gate opens, regardless of who's asking.
        await block.SendAsync(0, TestContext.Current.CancellationToken);
        await block.SendAsync(1, TestContext.Current.CancellationToken);

        // Now hammer the already-full block from many threads at once - every single one of these must
        // decline, and none of those declines may quietly turn into an acceptance once the gate later opens.
        // Real Threads rather than Task.Run/the thread pool: forcing the pool to grow to N live threads via its
        // hill-climbing injector just to reach the barrier would make this a slow test of thread-pool ramp-up,
        // not of Post under contention.
        const int concurrentCallers = 64;
        var barrier = new Barrier(concurrentCallers);
        var accepted = new ConcurrentBag<int>();

        var threads = Enumerable.Range(2, concurrentCallers).Select(i => new Thread(() =>
        {
            barrier.SignalAndWait();
            if (block.Post(i))
            {
                accepted.Add(i);
            }
        })).ToArray();
        foreach (var thread in threads)
        {
            thread.Start();
        }
        foreach (var thread in threads)
        {
            thread.Join();
        }

        Assert.Empty(accepted);

        gate.SetResult();
        block.Complete();

        var results = new List<int>();
        await foreach (var result in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            results.Add(result);
        }

        Assert.Equal([0, 1], results);
    }

    // ───────────────────────────────────────────────
    //  Linking
    // ───────────────────────────────────────────────

    [Fact]
    public void LinkTo_NullTarget_Throws()
    {
        var block = new TransformBlock<int, int>(item => item);
        Assert.Throws<ArgumentNullException>(() => block.LinkTo(null));
    }

    [Fact]
    public void LinkTo_SecondLinkWhileActive_Throws()
    {
        var block = new TransformBlock<int, int>(item => item);
        using var link = block.LinkTo(new RecordingTarget<int>());
        Assert.Throws<InvalidOperationException>(() => block.LinkTo(new RecordingTarget<int>()));
    }

    [Fact]
    public void LinkTo_AfterUnlinkDispose_AllowsNewLink()
    {
        var block = new TransformBlock<int, int>(item => item);
        var link = block.LinkTo(new RecordingTarget<int>());
        link.Dispose();

        using var link2 = block.LinkTo(new RecordingTarget<int>());
    }

    [Fact]
    public async Task LinkTo_PushesOutputToLinkedTarget()
    {
        var block = new TransformBlock<int, int>(item => item * 2);
        var target = new RecordingTarget<int>();
        using var link = block.LinkTo(target);

        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);
        block.Complete();

        await block.Completion;
        await target.Completion;

        Assert.Equal([2, 4], target.Received);
    }

    // ───────────────────────────────────────────────
    //  Completion propagation
    // ───────────────────────────────────────────────

    [Fact]
    public async Task Complete_NoLink_CompletionResolvesRanToCompletion_AfterDrain()
    {
        var block = new TransformBlock<int, int>(item => item);
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        block.Complete();

        await block.Completion;
        Assert.Equal(TaskStatus.RanToCompletion, block.Completion.Status);
    }

    [Fact]
    public async Task Complete_WithLinkPropagateTrue_PropagatesCompleteToTarget()
    {
        var block = new TransformBlock<int, int>(item => item);
        var target = new RecordingTarget<int>();
        using var link = block.LinkTo(target);

        block.Complete();

        await target.Completion;
        Assert.True(target.Completed);
    }

    [Fact]
    public async Task Complete_WithLinkPropagateFalse_DoesNotCompleteTarget()
    {
        var block = new TransformBlock<int, int>(item => item);
        var target = new RecordingTarget<int>();
        using var link = block.LinkTo(target, new FluxLinkOptions { PropagateCompletion = false });

        block.Complete();
        await block.Completion;

        Assert.False(target.Completed);
    }

    [Fact]
    public async Task Completion_ResolvesOnlyAfterAllQueuedItemsProcessed()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var block = new TransformBlock<int, int>(async item =>
        {
            await gate.Task;
            return item;
        });

        await block.SendAsync(1, TestContext.Current.CancellationToken);
        block.Complete();

        await Task.Delay(20, TestContext.Current.CancellationToken);
        Assert.False(block.Completion.IsCompleted);

        gate.SetResult();
        await block.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, block.Completion.Status);
    }

    // ───────────────────────────────────────────────
    //  Fault propagation
    // ───────────────────────────────────────────────

    [Fact]
    public async Task Fault_ExplicitCall_CompletionResolvesFaulted_WithSameException()
    {
        var block = new TransformBlock<int, int>(item => item);
        var exception = new InvalidOperationException("boom");
        block.Fault(exception);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await block.Completion);
        Assert.Same(exception, thrown);
    }

    [Fact]
    public void Fault_ExplicitCall_NullException_Throws()
    {
        var block = new TransformBlock<int, int>(item => item);
        Assert.Throws<ArgumentNullException>(() => block.Fault(null));
    }

    [Fact]
    public async Task TransformDelegateThrows_FaultsBlock_CompletionIsFaulted()
    {
        var block = new TransformBlock<int, int>(new Func<int, int>(_ => throw new InvalidOperationException("transform boom")));
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        block.Complete();

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await block.Completion);
    }

    [Fact]
    public async Task Fault_PropagatesToLinkedTarget_RegardlessOfPropagateCompletionFlag()
    {
        var block = new TransformBlock<int, int>(item => item);
        var target = new RecordingTarget<int>();
        using var link = block.LinkTo(target, new FluxLinkOptions { PropagateCompletion = false });

        var exception = new InvalidOperationException("boom");
        block.Fault(exception);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await target.Completion);
        Assert.Same(exception, thrown);
    }

    [Fact]
    public async Task Fault_Idempotent_SecondFaultCallIgnored()
    {
        var block = new TransformBlock<int, int>(item => item);
        var first = new InvalidOperationException("first");
        var second = new InvalidOperationException("second");

        block.Fault(first);
        block.Fault(second);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await block.Completion);
        Assert.Same(first, thrown);
    }

    // ───────────────────────────────────────────────
    //  Cancellation
    // ───────────────────────────────────────────────

    [Fact]
    public async Task CancellationToken_Canceled_CompletionResolvesCanceled()
    {
        using var cts = new CancellationTokenSource();
        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { CancellationToken = cts.Token });

        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await block.Completion);
        Assert.Equal(TaskStatus.Canceled, block.Completion.Status);
    }

    [Fact]
    public async Task SendAsync_CanceledToken_ThrowsOperationCanceledException_WhenWaitingForCapacity()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var block = new TransformBlock<int, int>(async item =>
        {
            await gate.Task;
            return item;
        }, new FluxBlockOptions { BoundedCapacity = 1 });

        // Item 0 is dequeued by the single worker and gets stuck awaiting the gate; item 1 then fills the
        // one-slot buffer, so a third send has no capacity to write into and must wait.
        await block.SendAsync(0, TestContext.Current.CancellationToken);
        await block.SendAsync(1, TestContext.Current.CancellationToken);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await block.SendAsync(2, cts.Token));
    }

    // ───────────────────────────────────────────────
    //  Ordering — unordered by default vs opted-in
    // ───────────────────────────────────────────────

    [Fact]
    public async Task EnsureOrdered_False_Default_OutputMayBeUnordered()
    {
        var gates = new ConcurrentDictionary<int, TaskCompletionSource>();
        var block = new TransformBlock<int, int>(GatedTransform(gates), new FluxBlockOptions { MaxDegreeOfParallelism = 2 });

        await block.SendAsync(0, TestContext.Current.CancellationToken);
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        block.Complete();

        // Item 0's gate is never released until after item 1 is observed, so item 1 must surface first
        // if (and only if) there is no artificial reordering forcing input order back onto the output.
        Release(gates, 1);

        var results = new List<int>();
        await foreach (var result in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            results.Add(result);
            if (results.Count == 1)
            {
                Release(gates, 0);
            }
        }

        Assert.Equal([1, 0], results);
    }

    [Fact]
    public async Task EnsureOrdered_True_MaxDegreeOfParallelism1_OutputMatchesInputOrder()
    {
        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { EnsureOrdered = true });
        await block.SendAsync(0, TestContext.Current.CancellationToken);
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);
        block.Complete();

        var results = new List<int>();
        await foreach (var result in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            results.Add(result);
        }

        Assert.Equal([0, 1, 2], results);
    }

    [Fact]
    public async Task EnsureOrdered_True_MaxDegreeOfParallelism_Greater1_OutputMatchesInputOrder()
    {
        var gates = new ConcurrentDictionary<int, TaskCompletionSource>();
        var block = new TransformBlock<int, int>(GatedTransform(gates), new FluxBlockOptions { MaxDegreeOfParallelism = 2, EnsureOrdered = true });

        await block.SendAsync(0, TestContext.Current.CancellationToken);
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        block.Complete();

        // Release item 1 (the one that would finish first without ordering) before item 0, and still expect
        // input order on the way out - the ordered consumer only ever awaits item 0's task before item 1's.
        Release(gates, 1);
        Release(gates, 0);

        var results = new List<int>();
        await foreach (var result in block.ReceiveAllAsync(TestContext.Current.CancellationToken))
        {
            results.Add(result);
        }

        Assert.Equal([0, 1], results);
    }
}
