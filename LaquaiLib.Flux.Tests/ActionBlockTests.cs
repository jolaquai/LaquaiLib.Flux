using System.Collections.Concurrent;

namespace LaquaiLib.Flux;

public sealed class ActionBlockTests
{
    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(5, cancellationToken);
        }
    }

    // ───────────────────────────────────────────────
    //  Construction
    // ───────────────────────────────────────────────

    [Fact]
    public void Ctor_SyncAction_NullDelegate_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new ActionBlock<int>((Action<int>)null));
    }

    [Fact]
    public void Ctor_AsyncAction_NullDelegate_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new ActionBlock<int>((Func<int, ValueTask>)null));
    }

    [Fact]
    public void Ctor_DefaultOptions_UsesDefaults()
    {
        var block = new ActionBlock<int>(_ => { });
        Assert.NotNull(block.Name);
        Assert.Contains("ActionBlock", block.Name);
    }

    [Fact]
    public void Ctor_NamedOptions_UsesProvidedName()
    {
        var block = new ActionBlock<int>(_ => { }, new FluxBlockOptions { Name = "my-sink" });
        Assert.Equal("my-sink", block.Name);
    }

    // ───────────────────────────────────────────────
    //  Basic action correctness
    // ───────────────────────────────────────────────

    [Fact]
    public async Task SendAsync_SingleItem_SyncAction_InvokesAction()
    {
        var received = new ConcurrentBag<int>();
        var block = new ActionBlock<int>(item => received.Add(item));

        await block.SendAsync(21, TestContext.Current.CancellationToken);
        block.Complete();

        await block.Completion;

        Assert.Equal([21], received);
    }

    [Fact]
    public async Task SendAsync_SingleItem_AsyncAction_InvokesAction()
    {
        var received = new ConcurrentBag<int>();
        var block = new ActionBlock<int>(item =>
        {
            received.Add(item);
            return ValueTask.CompletedTask;
        });

        await block.SendAsync(21, TestContext.Current.CancellationToken);
        block.Complete();

        await block.Completion;

        Assert.Equal([21], received);
    }

    [Fact]
    public async Task SendAsync_MultipleItems_MaxDegreeOfParallelism1_PreservesSideEffectOrder()
    {
        var received = new List<int>();
        var block = new ActionBlock<int>(item => received.Add(item));

        for (var i = 0; i < 5; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        await block.Completion;

        Assert.Equal([0, 1, 2, 3, 4], received);
    }

    [Fact]
    public async Task SendAsync_MultipleItems_MaxDegreeOfParallelism_Greater1_ProcessesAllItems()
    {
        var received = new ConcurrentBag<int>();
        var block = new ActionBlock<int>(item => received.Add(item), new FluxBlockOptions { MaxDegreeOfParallelism = 4 });

        var expected = Enumerable.Range(0, 100).ToArray();
        foreach (var i in expected)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        await block.Completion;

        Assert.Equal(expected.Length, received.Count);
        Assert.Equal(expected.ToHashSet(), received.ToHashSet());
    }

    [Fact]
    public async Task SendAsync_ReturnsTrue_WhenAccepted()
    {
        var block = new ActionBlock<int>(_ => { });
        Assert.True(await block.SendAsync(1, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SendAsync_ReturnsFalse_AfterComplete()
    {
        var block = new ActionBlock<int>(_ => { });
        block.Complete();
        Assert.False(await block.SendAsync(1, TestContext.Current.CancellationToken));
    }

    [Fact]
    public void Post_ReturnsTrue_WhenCapacityAvailable()
    {
        var block = new ActionBlock<int>(_ => { });
        Assert.True(block.Post(1));
    }

    // ───────────────────────────────────────────────
    //  Backpressure
    // ───────────────────────────────────────────────

    [Fact]
    public async Task SendAsync_BeyondCapacity_GenuinelySuspends_WhileActionIsStuck()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var block = new ActionBlock<int>(async item =>
        {
            await gate.Task;
        }, new FluxBlockOptions { BoundedCapacity = 1 });

        // Item 0 is dequeued by the single worker and gets stuck awaiting the gate; item 1 then fills the
        // one-slot input buffer, so a third send has no capacity to write into and must genuinely suspend.
        // No output-drain deadlock risk since ActionBlock has no output channel to back up.
        await block.SendAsync(0, TestContext.Current.CancellationToken);
        await block.SendAsync(1, TestContext.Current.CancellationToken);

        var sendTask = block.SendAsync(2, TestContext.Current.CancellationToken).AsTask();
        await Task.Delay(30, TestContext.Current.CancellationToken);
        Assert.False(sendTask.IsCompleted);

        gate.SetResult();
        Assert.True(await sendTask);
        block.Complete();
        await block.Completion;
    }

    [Fact]
    public async Task Completion_ResolvesRanToCompletion_OnlyAfterAllQueuedItemsProcessed()
    {
        var processed = new ConcurrentBag<int>();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var block = new ActionBlock<int>(async item =>
        {
            await gate.Task;
            processed.Add(item);
        });

        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);
        block.Complete();

        await Task.Delay(30, TestContext.Current.CancellationToken);
        Assert.False(block.Completion.IsCompleted);

        gate.SetResult();
        await block.Completion;

        Assert.Equal(2, processed.Count);
    }

    // ───────────────────────────────────────────────
    //  Fault propagation
    // ───────────────────────────────────────────────

    [Fact]
    public async Task ActionDelegateThrows_FaultsBlock_CompletionIsFaulted()
    {
        var block = new ActionBlock<int>(new Action<int>(_ => throw new InvalidOperationException("action boom")));
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        block.Complete();

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await block.Completion);
        Assert.Equal("action boom", thrown.Message);
    }

    [Fact]
    public async Task Fault_ExplicitCall_CompletionResolvesFaulted_WithSameException()
    {
        var block = new ActionBlock<int>(_ => { });
        var exception = new InvalidOperationException("boom");
        block.Fault(exception);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () => await block.Completion);
        Assert.Same(exception, thrown);
    }

    [Fact]
    public void Fault_ExplicitCall_NullException_Throws()
    {
        var block = new ActionBlock<int>(_ => { });
        Assert.Throws<ArgumentNullException>(() => block.Fault(null));
    }

    [Fact]
    public async Task Fault_Idempotent_SecondFaultCallIgnored()
    {
        var block = new ActionBlock<int>(_ => { });
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
        var block = new ActionBlock<int>(_ => { }, new FluxBlockOptions { CancellationToken = cts.Token });

        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await block.Completion);
        Assert.Equal(TaskStatus.Canceled, block.Completion.Status);
    }

    [Fact]
    public async Task SendAsync_CanceledToken_ThrowsOperationCanceledException_WhenWaitingForCapacity()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var block = new ActionBlock<int>(async item =>
        {
            await gate.Task;
        }, new FluxBlockOptions { BoundedCapacity = 1 });

        await block.SendAsync(0, TestContext.Current.CancellationToken);
        await block.SendAsync(1, TestContext.Current.CancellationToken);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await block.SendAsync(2, cts.Token));
    }
}
