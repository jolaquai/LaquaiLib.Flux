# Flux v2 - fan-out and multi-link dispatch

Companion to [CLAUDE.flux-plan.md](CLAUDE.flux-plan.md). Removes v1's "at most one linked target per source"
restriction and adds a chosen fan-out strategy for what happens once a source has more than one.
[FluxLink.cs](Primitives/FluxLink.cs) already calls this out as the intended seam: v1 keeps a `List<FluxLink<TOut>>`
(capacity 1) instead of a single field specifically so this change never touches the public `LinkTo`/`FluxLinkOptions`
contract, only the internal dispatch loop that iterates the list.

## Load-bearing fact this whole design leans on

`IFluxTarget<TIn>.SendAsync` returning `false` means the target has **permanently** completed or faulted - not
"currently full." Transient fullness always waits it out (see [IFluxTarget.cs:17-22](IFluxTarget.cs:17)). Every
strategy below relies on this to distinguish "skip forever" from "wait here" without needing a two-phase
reserve/consume protocol (what real TPL Dataflow needs internally for its postponed-message machinery). This is
what keeps all three modes implementable directly on the existing `TryOffer`/`SendAsync` primitives.

## API surface changes

1. **`FluxBlockOptions`** - new property:
   ```csharp
   public FluxFanOutMode FanOutMode { get; init; } = FluxFanOutMode.Broadcast;
   ```
   New enum `FluxFanOutMode { Broadcast, FirstAvailable, RoundRobin }`. Construction-time, immutable for the
   block's lifetime - consistent with `EnsureOrdered`/`MaxDegreeOfParallelism`, and necessary because the dispatch
   loop reads it on every item against a concurrently-mutating `_linksSnapshot`; flipping it mid-flight is a
   correctness hazard for no real benefit. Default `Broadcast` preserves v1 behavior exactly when 0-1 links exist.

2. **`IFluxSource<TOut>.LinkTo`** - gains a trailing filter parameter, mirroring
   `DataflowBlock.LinkTo<TOutput>(ISourceBlock<TOutput>, ITargetBlock<TOutput>, DataflowLinkOptions, Predicate<TOutput>)`:
   ```csharp
   public IDisposable LinkTo(IFluxTarget<TOut> target, FluxLinkOptions linkOptions = null, Func<TOut, bool> filter = null);
   ```
   Deliberately **not** a property on `FluxLinkOptions`: that type is non-generic (shared across every `TOut`), so a
   `Func<TOut, bool>` cannot live there without genericizing `FluxLinkOptions<TOut>` - a breaking change that ripples
   into every existing call site for zero benefit. Dataflow's own `DataflowLinkOptions` avoids this for the identical
   reason. `FluxLinkOptions` stays untouched.

3. **`FluxLink<TOut>`** (internal, already generic over `TOut`) - gains:
   ```csharp
   public Func<TOut, bool> Filter { get; }
   ```
   populated from `LinkTo`'s new parameter, defaulting to `null` (matches everything).

4. **`PropagatorFluxBlockBase<TIn, TOut>.LinkTo`** - remove the `_links.Count > 0` throw
   ([PropagatorFluxBlockBase.cs:63-66](Primitives/PropagatorFluxBlockBase.cs:63)). Multi-link becomes unconditionally
   legal. Optionally guard against linking the exact same `IFluxTarget<TOut>` instance twice (pointless in
   `FirstAvailable`/`RoundRobin`, genuinely double-delivers in `Broadcast`) - low priority, can throw or silently
   allow; decide during implementation, not blocking.

5. **`DispatchLoopAsync`** - the current hardcoded `foreach (var link in links) await link.Target.SendAsync(...)`
   ([PropagatorFluxBlockBase.cs:112-119](Primitives/PropagatorFluxBlockBase.cs:112)) is replaced by a switch to one
   of three per-mode methods, each with the filter check folded in inline (no separate candidate-list pass, no
   extra allocation):
   ```csharp
   private ValueTask DispatchItemAsync(TOut item, FluxLink<TOut>[] links) => _options.FanOutMode switch
   {
       _ when links.Length == 1 => DispatchSingleAsync(item, links[0]),   // fast path, bypasses mode entirely
       FluxFanOutMode.FirstAvailable => FirstAvailableAsync(item, links),
       FluxFanOutMode.RoundRobin     => RoundRobinAsync(item, links),
       _                             => BroadcastAsync(item, links),
   };
   ```

## The three modes

### Broadcast (default)

Duplicate every item to every currently-linked, filter-matching target. Difference from today: fire all matching
`SendAsync` calls concurrently instead of sequentially awaiting one at a time, so N targets' backpressure waits
overlap instead of stacking serially. A `false` return from any one target is non-fatal (existing
`FluxMetrics.ItemsDropped` accounting), same as today.

Open implementation detail: the N>1 path needs a small buffer to hold the in-flight `ValueTask<bool>`s before
awaiting them all. `stackalloc` doesn't work here (`ValueTask<bool>` isn't unmanaged) - use a pooled array
(`ArrayPool<ValueTask<bool>>` or similar), same spirit as the existing `_linksSnapshot` caching. Resolve the exact
mechanism during implementation, not here.

Footnote for docs, not a v2 feature: broadcasting a reference-type item hands the *same instance* to every target
(no cloning), identical caveat to `System.Threading.Tasks.Dataflow.BroadcastBlock<T>` without its optional cloning
function. Not proposing we add a cloning hook now.

### FirstAvailable

Link order is priority order. First acceptor - synchronous or eventually-waiting - wins, in that order:

```csharp
private async ValueTask FirstAvailableAsync(TOut item, FluxLink<TOut>[] links)
{
    foreach (var link in links)                                          // sweep: anyone free right now?
        if ((link.Filter is null || link.Filter(item)) && link.Target.TryOffer(item))
            return;

    foreach (var link in links)                                          // nobody free: wait, in priority order.
    {
        if (link.Filter is not null && !link.Filter(item)) continue;
        if (await link.Target.SendAsync(item, _loopToken).ConfigureAwait(false)) return;
    }
    // every matching link permanently done -> drop (existing ItemsDropped metric path)
}
```

Because a dead target's `SendAsync` returns `false` instantly rather than hanging, a faulted/completed
higher-priority link can never stall delivery to a healthy lower-priority one - no liveness tracking required.
Gives clean primary/fallback semantics (link a fast primary first, a spillover target second) with zero extra
state. Starvation of low-priority links while the primary keeps up is intended behavior for this mode, not a bug.

### RoundRobin

Adaptive load balancing, not strict positional partitioning: sweep `TryOffer` starting from a rotating cursor (so
ties never keep favoring link 0), first free matching target wins; only if the entire sweep finds nobody free does
it block on the current-turn target specifically.

```csharp
private int _rrCursor; // dispatch loop is single-threaded (_output is always SingleReader=true) - plain int is fine.

private async ValueTask RoundRobinAsync(TOut item, FluxLink<TOut>[] links)
{
    var n = links.Length;
    var start = _rrCursor++ % n;
    for (var i = 0; i < n; i++)
    {
        var link = links[(start + i) % n];
        if (link.Filter is not null && !link.Filter(item)) continue;
        if (link.Target.TryOffer(item)) return;
    }
    for (var i = 0; i < n; i++)
    {
        var link = links[(start + i) % n];
        if (link.Filter is not null && !link.Filter(item)) continue;
        if (await link.Target.SendAsync(item, _loopToken).ConfigureAwait(false)) return;
    }
    // every matching link permanently done -> drop
}
```

This is what actually reacts to differing consumer speeds: a faster consumer is statistically more often "the one
with room," regardless of which position it was linked at. `_rrCursor` needs no `Interlocked` - re-verify this
still holds if `DispatchLoopAsync` is ever parallelized; today there is exactly one drainer per propagator.

Not building the strict-positional variant (`item i -> link[i % n]`, no availability check) unless a concrete need
for deterministic partitioning regardless of consumer speed shows up later - the adaptive version is strictly more
useful for the same implementation cost.

## Concurrency/safety summary

- No target ever has the same item concurrently offered via two committing calls (`TryOffer`/`SendAsync`) - every
  mode either scans synchronously (no race window) or commits to exactly one target's `SendAsync` at a time. This
  is what avoids the double-accept hazard that a naive `Task.WhenAny(SendAsync, SendAsync, ...)` race would have.
- `_linksSnapshot`'s existing immutable-republish-on-LinkTo/Unlink pattern needs no changes: every mode reads the
  snapshot once per item and indexes into it, so a link added/removed mid-item never observes a torn state, and a
  rotating cursor computed as `_rrCursor % links.Length` (recomputed against the *current* snapshot length each
  item) stays correct across snapshot swaps with no identity tracking needed.
- Completion/fault propagation loop at the bottom of `DispatchLoopAsync` (per-link `Fault`/`Complete` once the loop
  exits) is unaffected by any of this - it already treats every entry in `_linksSnapshot` uniformly regardless of
  what dispatch mode was used to reach that point.

## Deferred / explicitly out of scope for this pass

- **Keyed/sticky routing** (hash a key selector to a target). Real feature, but dynamic membership (a link added
  or removed mid-stream) reshuffles the keyspace unless backed by consistent hashing - a materially bigger lift.
  Revisit as its own pass if a use case needing per-key affinity downstream shows up.
- **Explicit `Priority`/`Weight` fields.** Link order already expresses priority for `FirstAvailable`; no evidence
  yet that it needs to be decoupled from call order.
- **Pluggable custom strategy interface** (`IFluxFanOutStrategy<TOut>`). The switch-based dispatch doesn't foreclose
  adding this later, but three concrete named modes cover every case raised so far - don't build the extensibility
  point speculatively.
- **Broadcast cloning function** for reference-type fan-out safety. Mentioned only as a docs footnote above.

## Implementation checklist (for whenever this is picked up)

- [ ] `FluxFanOutMode` enum (new file or alongside `FluxBlockOptions`)
- [ ] `FluxBlockOptions.FanOutMode` property + XML doc
- [ ] `IFluxSource<TOut>.LinkTo` - add `filter` parameter + XML doc update
- [ ] `FluxLink<TOut>` - add `Filter` property, threaded through its constructor
- [ ] `PropagatorFluxBlockBase<TIn, TOut>.LinkTo` - drop single-link guard, pass `filter` into `new FluxLink<TOut>(...)`
- [ ] `PropagatorFluxBlockBase<TIn, TOut>.DispatchLoopAsync` - replace inline foreach with `DispatchItemAsync` switch
- [ ] `DispatchSingleAsync` / `BroadcastAsync` / `FirstAvailableAsync` / `RoundRobinAsync` methods
- [ ] Decide N>1 `Broadcast` buffer strategy (pooled array vs. alternative)
- [ ] Decide whether to guard against linking the same target instance twice
- [ ] Tests: multi-link Broadcast delivery, FirstAvailable priority/fallback-on-fault, RoundRobin fairness under
      mixed-speed consumers, filter interaction with each mode, completion/fault propagation to all links
- [ ] Benchmarks: single-link fast path must show zero regression vs. current v1 numbers - multi-link overhead
      only acceptable when multi-link is actually in use
