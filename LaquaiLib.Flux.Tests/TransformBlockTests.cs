using System.Collections.Concurrent;

namespace LaquaiLib.Flux;

public sealed class TransformBlockTests
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

    /// <summary>
    /// A target that starts unable to accept anything - both <see cref="TryOffer"/> and any parked
    /// <see cref="SendAsync"/> decline/wait - until <see cref="Open"/> is called, at which point it accepts every
    /// item unconditionally. Models a target that is transiently full, as opposed to <see cref="RecordingTarget{T}"/>'s
    /// <see cref="RecordingTarget{T}.Complete"/>/<see cref="RecordingTarget{T}.Fault"/> which model a target that
    /// is permanently done - so fan-out tests can exercise the genuinely-awaited second pass in
    /// <c>BroadcastAsync</c>/<c>FirstAvailableAsync</c>/<c>RoundRobinAsync</c> instead of only their synchronous
    /// sweep, which every other fan-out test so far only ever hits.
    /// </summary>
    private sealed class GateableTarget<T> : IFluxTarget<T>
    {
        private readonly TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _openGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private volatile bool _open;
        private volatile bool _declining;
        private int _tryOfferAttempts;

        public List<T> Received { get; } = [];

        public string Name => "GateableTarget";
        public Task Completion => _completionTcs.Task;

        /// <summary>Number of <see cref="TryOffer"/> calls observed so far, success or decline - lets a test wait for a specific sweep to have actually touched this target before mutating its state.</summary>
        public int TryOfferAttempts => Volatile.Read(ref _tryOfferAttempts);

        /// <summary>Opens the gate: every future <see cref="TryOffer"/> succeeds, and any parked <see cref="SendAsync"/> wakes to accept.</summary>
        public void Open()
        {
            _open = true;
            _openGate.TrySetResult();
        }

        public bool TryOffer(T item)
        {
            Interlocked.Increment(ref _tryOfferAttempts);
            if (_declining || !_open)
            {
                return false;
            }
            lock (Received)
            {
                Received.Add(item);
            }
            return true;
        }

        public async ValueTask<bool> SendAsync(T item, CancellationToken cancellationToken = default)
        {
            if (TryOffer(item))
            {
                return true;
            }
            await _openGate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return TryOffer(item);
        }

        public void Complete()
        {
            _declining = true;
            _openGate.TrySetResult();
            _completionTcs.TrySetResult();
        }

        public void Fault(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);
            _declining = true;
            _openGate.TrySetResult();
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
    public void LinkTo_SecondLinkWhileActive_DoesNotThrow()
    {
        var block = new TransformBlock<int, int>(item => item);
        using var link1 = block.LinkTo(new RecordingTarget<int>());
        using var link2 = block.LinkTo(new RecordingTarget<int>());
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
    //  Fan-out (multi-link dispatch)
    // ───────────────────────────────────────────────

    [Fact]
    public void LinkTo_SameTargetTwiceWithDifferentFilters_DoesNotThrow()
    {
        var block = new TransformBlock<int, int>(item => item);
        var target = new RecordingTarget<int>();
        using var link1 = block.LinkTo(target, filter: i => i % 2 == 0);
        using var link2 = block.LinkTo(target, filter: i => i % 2 != 0);
    }

    [Fact]
    public async Task LinkTo_Broadcast_DuplicatesToEveryLinkedTarget()
    {
        var block = new TransformBlock<int, int>(item => item);
        var targetA = new RecordingTarget<int>();
        var targetB = new RecordingTarget<int>();
        using var linkA = block.LinkTo(targetA);
        using var linkB = block.LinkTo(targetB);

        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);
        block.Complete();

        await block.Completion;
        await targetA.Completion;
        await targetB.Completion;

        Assert.Equal([1, 2], targetA.Received);
        Assert.Equal([1, 2], targetB.Received);
    }

    [Fact]
    public async Task LinkTo_Broadcast_PerLinkFilter_RoutesOnlyMatchingItems()
    {
        var block = new TransformBlock<int, int>(item => item);
        var evens = new RecordingTarget<int>();
        var odds = new RecordingTarget<int>();
        using var linkEvens = block.LinkTo(evens, filter: i => i % 2 == 0);
        using var linkOdds = block.LinkTo(odds, filter: i => i % 2 != 0);

        foreach (var i in Enumerable.Range(1, 4))
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        await block.Completion;
        await evens.Completion;
        await odds.Completion;

        Assert.Equal([2, 4], evens.Received);
        Assert.Equal([1, 3], odds.Received);
    }

    [Fact]
    public async Task LinkTo_FirstAvailable_OnlyFirstLinkedTargetReceivesEachItem()
    {
        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { FanOutMode = FluxFanOutMode.FirstAvailable });
        var first = new RecordingTarget<int>();
        var second = new RecordingTarget<int>();
        using var linkFirst = block.LinkTo(first);
        using var linkSecond = block.LinkTo(second);

        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);
        block.Complete();

        await block.Completion;
        await first.Completion;
        await second.Completion;

        Assert.Equal([1, 2], first.Received);
        Assert.Empty(second.Received);
    }

    [Fact]
    public async Task LinkTo_FirstAvailable_PermanentlyDoneHigherPriorityLink_FailsOverToNext()
    {
        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { FanOutMode = FluxFanOutMode.FirstAvailable });
        var first = new RecordingTarget<int>();
        var second = new RecordingTarget<int>();
        using var linkFirst = block.LinkTo(first);
        using var linkSecond = block.LinkTo(second);

        first.Complete(); // retire the higher-priority target before any items flow

        await block.SendAsync(1, TestContext.Current.CancellationToken);
        await block.SendAsync(2, TestContext.Current.CancellationToken);
        block.Complete();

        await block.Completion;
        await second.Completion;

        Assert.Empty(first.Received);
        Assert.Equal([1, 2], second.Received);
    }

    [Fact]
    public async Task LinkTo_RoundRobin_AlternatesAcrossTargets_WhenBothAlwaysFree()
    {
        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { FanOutMode = FluxFanOutMode.RoundRobin });
        var targetA = new RecordingTarget<int>();
        var targetB = new RecordingTarget<int>();
        using var linkA = block.LinkTo(targetA);
        using var linkB = block.LinkTo(targetB);

        const int itemCount = 20;
        for (var i = 0; i < itemCount; i++)
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        await block.Completion;
        await targetA.Completion;
        await targetB.Completion;

        // No drops, no duplicates.
        Assert.Equal(itemCount, targetA.Received.Count + targetB.Received.Count);
        Assert.Empty(targetA.Received.Intersect(targetB.Received));
        // Both targets always have room and MaxDegreeOfParallelism defaults to 1 (strict output order), so the
        // rotating cursor deterministically alternates evenly between exactly two always-free targets.
        Assert.Equal(itemCount / 2, targetA.Received.Count);
        Assert.Equal(itemCount / 2, targetB.Received.Count);
    }

    [Fact]
    public async Task LinkTo_Broadcast_TransientlyFullTarget_StillReceivesOnceItOpens_WithoutBlockingOtherTarget()
    {
        var block = new TransformBlock<int, int>(item => item);
        var fast = new RecordingTarget<int>();
        var slow = new GateableTarget<int>(); // starts closed: synchronous TryOffer declines until Open()
        using var linkFast = block.LinkTo(fast);
        using var linkSlow = block.LinkTo(slow);

        await block.SendAsync(1, TestContext.Current.CancellationToken);
        block.Complete();

        // BroadcastAsync's synchronous sweep runs fast's TryOffer to completion before it ever awaits slow's
        // SendAsync (the pending-array promotion only happens for links that declined synchronously), so fast
        // must already have the item well before slow's gate opens - this is not a race.
        await WaitUntilAsync(() => fast.Received.Count > 0, TestContext.Current.CancellationToken);
        Assert.Equal([1], fast.Received);
        Assert.Empty(slow.Received); // still parked awaiting slow's gate, but not dropped

        slow.Open();

        await block.Completion;
        await fast.Completion;
        await slow.Completion;

        Assert.Equal([1], fast.Received);
        Assert.Equal([1], slow.Received);
    }

    [Fact]
    public async Task LinkTo_FirstAvailable_TransientlyFullHigherPriorityLink_IsWaitedOn_NotSkippedForLowerPriorityLinkThatOpensFirst()
    {
        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { FanOutMode = FluxFanOutMode.FirstAvailable });
        var first = new GateableTarget<int>();
        var second = new GateableTarget<int>();
        using var linkFirst = block.LinkTo(first);
        using var linkSecond = block.LinkTo(second);

        await block.SendAsync(1, TestContext.Current.CancellationToken);
        block.Complete();

        // Wait for the synchronous sweep to have actually tried (and declined) both links before touching
        // either target's state - opening `second` too early would let it win during the sweep itself instead
        // of exercising the awaiting second pass this test targets.
        await WaitUntilAsync(() => first.TryOfferAttempts > 0 && second.TryOfferAttempts > 0, TestContext.Current.CancellationToken);

        second.Open();
        await Task.Delay(30, TestContext.Current.CancellationToken);
        Assert.Empty(first.Received);
        Assert.Empty(second.Received); // second is open, but dispatch is still parked awaiting `first` specifically - link order is priority order even while waiting

        first.Open();

        await block.Completion;
        await first.Completion;
        await second.Completion;

        Assert.Equal([1], first.Received);
        Assert.Empty(second.Received);
    }

    [Fact]
    public async Task LinkTo_FirstAvailable_HigherPriorityLinkFaultsWhileAwaited_FailsOverToNext()
    {
        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { FanOutMode = FluxFanOutMode.FirstAvailable });
        var first = new GateableTarget<int>();
        var second = new GateableTarget<int>();
        using var linkFirst = block.LinkTo(first);
        using var linkSecond = block.LinkTo(second);

        await block.SendAsync(1, TestContext.Current.CancellationToken);
        block.Complete();

        await WaitUntilAsync(() => first.TryOfferAttempts > 0 && second.TryOfferAttempts > 0, TestContext.Current.CancellationToken);

        second.Open(); // second is free now, but dispatch is still parked awaiting `first`'s SendAsync specifically
        first.Fault(new InvalidOperationException("first died mid-wait")); // wakes first's SendAsync -> resolves false -> failover to second

        await block.Completion;
        await second.Completion;

        Assert.Empty(first.Received);
        Assert.Equal([1], second.Received);
    }

    [Fact]
    public async Task LinkTo_RoundRobin_TransientlyFullTargetAtRotatedStart_IsWaitedOn_NotSkipped()
    {
        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { FanOutMode = FluxFanOutMode.RoundRobin });
        var first = new GateableTarget<int>();
        var second = new GateableTarget<int>();
        using var linkFirst = block.LinkTo(first);
        using var linkSecond = block.LinkTo(second);

        // The first item's rotating start is index 0 (first) - same priority-during-wait semantic as
        // FirstAvailable, just reached through RoundRobinAsync's separately-implemented sweep/await passes.
        await block.SendAsync(1, TestContext.Current.CancellationToken);
        block.Complete();

        await WaitUntilAsync(() => first.TryOfferAttempts > 0 && second.TryOfferAttempts > 0, TestContext.Current.CancellationToken);

        second.Open();
        await Task.Delay(30, TestContext.Current.CancellationToken);
        Assert.Empty(second.Received);

        first.Open();

        await block.Completion;
        await first.Completion;
        await second.Completion;

        Assert.Equal([1], first.Received);
        Assert.Empty(second.Received);
    }

    [Fact]
    public async Task LinkTo_FirstAvailable_PerLinkFilter_SkipsNonMatchingHigherPriorityLink()
    {
        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { FanOutMode = FluxFanOutMode.FirstAvailable });
        var evensOnly = new RecordingTarget<int>();
        var catchAll = new RecordingTarget<int>();
        using var linkEvens = block.LinkTo(evensOnly, filter: i => i % 2 == 0); // higher priority, but only matches evens
        using var linkCatchAll = block.LinkTo(catchAll);

        await block.SendAsync(1, TestContext.Current.CancellationToken); // odd -> must skip evensOnly entirely
        await block.SendAsync(2, TestContext.Current.CancellationToken); // even -> evensOnly wins despite the filter check
        block.Complete();

        await block.Completion;
        await evensOnly.Completion;
        await catchAll.Completion;

        Assert.Equal([2], evensOnly.Received);
        Assert.Equal([1], catchAll.Received);
    }

    [Fact]
    public async Task LinkTo_RoundRobin_PerLinkFilter_OnlyRoutesToMatchingTarget()
    {
        var block = new TransformBlock<int, int>(item => item, new FluxBlockOptions { FanOutMode = FluxFanOutMode.RoundRobin });
        var evens = new RecordingTarget<int>();
        var odds = new RecordingTarget<int>();
        using var linkEvens = block.LinkTo(evens, filter: i => i % 2 == 0);
        using var linkOdds = block.LinkTo(odds, filter: i => i % 2 != 0);

        foreach (var i in Enumerable.Range(1, 6))
        {
            await block.SendAsync(i, TestContext.Current.CancellationToken);
        }
        block.Complete();

        await block.Completion;
        await evens.Completion;
        await odds.Completion;

        // Disjoint filters partition every item to exactly one target regardless of the rotating cursor - the
        // filter check happens before a candidate is even considered for TryOffer/SendAsync.
        Assert.Equal([2, 4, 6], evens.Received);
        Assert.Equal([1, 3, 5], odds.Received);
    }

    [Fact]
    public async Task Complete_MultiLink_PropagatesToEveryLinkedTarget()
    {
        var block = new TransformBlock<int, int>(item => item);
        var targetA = new RecordingTarget<int>();
        var targetB = new RecordingTarget<int>();
        using var linkA = block.LinkTo(targetA);
        using var linkB = block.LinkTo(targetB);

        block.Complete();

        await targetA.Completion;
        await targetB.Completion;

        Assert.True(targetA.Completed);
        Assert.True(targetB.Completed);
    }

    [Fact]
    public async Task Fault_MultiLink_PropagatesToEveryLinkedTarget()
    {
        var block = new TransformBlock<int, int>(item => item);
        var targetA = new RecordingTarget<int>();
        var targetB = new RecordingTarget<int>();
        using var linkA = block.LinkTo(targetA);
        using var linkB = block.LinkTo(targetB);

        var exception = new InvalidOperationException("boom");
        block.Fault(exception);

        var thrownA = await Assert.ThrowsAsync<InvalidOperationException>(async () => await targetA.Completion);
        var thrownB = await Assert.ThrowsAsync<InvalidOperationException>(async () => await targetB.Completion);
        Assert.Same(exception, thrownA);
        Assert.Same(exception, thrownB);
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
