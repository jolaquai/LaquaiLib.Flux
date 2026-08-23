# Flux -> Dataflow parity plan

What has to be true before "drop-in alternative to `System.Threading.Tasks.Dataflow`" is a claim rather than an
aspiration. Companion to [CLAUDE.handoff.md](CLAUDE.handoff.md) (live repo/benchmark state),
[CLAUDE.stub-blocks-plan.md](CLAUDE.stub-blocks-plan.md) (block implementation, complete),
[CLAUDE.flux-plan.md](CLAUDE.flux-plan.md) (v1 scope) and
[CLAUDE.fanout-multilink-plan.md](CLAUDE.fanout-multilink-plan.md) (shipped).

Derived from a full public-API inventory taken 2026-08-06 against `System.Threading.Tasks.Dataflow`'s surface.
Ordered by what would break the pitch first, not by implementation cost.

## Where the perf story actually stands

Not a pain point, recorded here so the parity work is not mistaken for a rescue effort. Current measured ratios
(Flux/Dataflow, lower is better, AMD 7900X / net10.0) are in the handoff doc. Summary: Flux wins on time in every
`TransformBlock` and `ActionBlock` suite (0.11x-0.97x) and on allocation nearly everywhere (0.02x-0.5x, with
`ActionBlock` DOP 4 at 0.06x and `BatchBlock` steady-state at 0.05x). The library is fast. It is the *surface*
that is not yet a replacement.

The four measured exceptions are tracked as item 8.

---

## 1. No interop layer - this is what makes "drop-in" false today

**Status: not started. Highest priority.**

The library contains zero references to `IDataflowBlock`, `ISourceBlock<T>`, `ITargetBlock<T>` or
`IPropagatorBlock<TIn,TOut>`. The only mentions of Dataflow anywhere in the library are five prose/`<see cref>`
doc comments. There is no package reference either.

Consequences, in order of how quickly a user hits them:

- A Flux block cannot be linked to a Dataflow block in either direction. **Migration is all-or-nothing per
  pipeline**, which is the opposite of what "drop-in" implies.
- Any user who does not own one end of their pipeline (a Dataflow block handed to them by a framework or a
  dependency) is blocked outright, with no workaround.
- Every existing `ITargetBlock<T>`-typed field, parameter or return in user code fails to compile against Flux.

### Options

1. **Adapter extension methods** - `.AsDataflowTarget()` / `.AsDataflowSource()` returning wrapper objects that
   implement the TPL interfaces over a Flux block, plus the reverse (`.AsFluxTarget()` over an `ITargetBlock<T>`).
   Additive, no impact on the core types, costs an allocation per adapter (once per link, not per item).
   Requires a `System.Threading.Tasks.Dataflow` package/framework reference, which is in-box on net8+.
2. **Direct implementation** - `TargetFluxBlockBase<TIn> : ITargetBlock<TIn>` and
   `PropagatorFluxBlockBase<TIn,TOut> : ISourceBlock<TOut>`. Best ergonomics, zero adapter allocation, but drags
   `DataflowMessageHeader`, `ISourceBlock.ReserveMessage`/`ConsumeMessage`/`ReleaseReservation` and the two-phase
   offer protocol into the core types. That protocol is the thing Flux exists to avoid; implementing it on the
   hot path would be a real cost.
3. **Separate `LaquaiLib.Flux.Dataflow` interop package** - keeps the core assembly free of the dependency and
   makes the cost opt-in.

**Recommendation: 1, optionally shipped as 3.** The two-phase consume protocol must be honored by the adapter, but
it stays off Flux's own hot path, which is the whole point.

- [ ] Decide adapter vs direct implementation vs separate package
- [ ] `IFluxTarget<T>` -> `ITargetBlock<T>` adapter (must honor `OfferMessage` / `DataflowMessageStatus`)
- [ ] `IFluxSource<T>` -> `ISourceBlock<T>` adapter (must honor `ReserveMessage`/`ConsumeMessage`/`ReleaseReservation`)
- [ ] `ITargetBlock<T>` -> `IFluxTarget<T>` adapter
- [ ] `ISourceBlock<T>` -> `IFluxSource<T>` adapter
- [ ] Round-trip tests: Flux -> Dataflow -> Flux pipeline, completion and fault propagating across both boundaries
- [ ] Benchmark the boundary hop so the cost is documented rather than discovered

---

## 2. Missing block types

**Status: not started.**

| Dataflow block | Flux |
|---|---|
| `ActionBlock<T>` | present |
| `BufferBlock<T>` | present |
| `TransformBlock<TIn,TOut>` | present |
| `BatchBlock<T>` | present, but see item 5 |
| `TransformManyBlock<TIn,TOut>` | **absent, no workaround** |
| `BroadcastBlock<T>` | partial, see below |
| `WriteOnceBlock<T>` | absent |
| `JoinBlock<T1,T2>` / `<T1,T2,T3>` | absent |
| `BatchedJoinBlock<T1,T2>` / `<T1,T2,T3>` | absent |

- **`TransformManyBlock` is the priority.** It is arguably the second-most-used block in real Dataflow code and
  there is genuinely no workaround: a Flux `TransformBlock` emits exactly one output per input, so
  one-to-many fan-out cannot be expressed at all. Fits the existing `PropagatorFluxBlockBase` shape cleanly
  (worker loop writes N items instead of 1); the only new design question is what ordering means at DOP > 1.
- **`BroadcastBlock` is only half-covered** by `FluxFanOutMode.Broadcast`. That is a link-level dispatch policy;
  Dataflow's `BroadcastBlock` additionally has "overwrite the pending item, always offer the latest value, new
  links immediately receive the current value" semantics. Different thing. Decide whether to build it or to
  document `FanOutMode.Broadcast` as the intended substitute and name the difference.
- `JoinBlock` / `BatchedJoinBlock` are the largest builds (multi-input, greedy vs non-greedy modes) and the
  least-used. Reasonable to defer past 1.0 with an explicit "not supported" note.

- [ ] `TransformManyBlock<TIn,TOut>` (+ tests + benchmark vs Dataflow)
- [ ] Decide: build `BroadcastBlock<T>` or document `FanOutMode.Broadcast` as the deliberate substitute
- [ ] `WriteOnceBlock<T>` (small; cheap parity win)
- [ ] Decide whether `JoinBlock` / `BatchedJoinBlock` are in scope pre-1.0

---

## 3. Silent behavior changes on migration - highest-risk category

**Status: undecided. This is a decision, not a bug.**

These compile without error and change runtime behavior:

| Setting | Dataflow default | Flux default | Effect of a naive swap |
|---|---|---|---|
| `BoundedCapacity` | `-1` (unbounded) | **`1024`** | Fire-and-forget `Post()` past 1024 items starts returning `false`, so items are dropped. `SendAsync` starts suspending where it never did. |
| `EnsureOrdered` | **`true`** | `false` | Output order silently changes at DOP > 1 |

Worse: **Flux has no unbounded mode at all.** `FluxBlockOptions.BoundedCapacity` is documented "Must be at least
1", so `BoundedCapacity = -1` is not merely a different default, it is unrepresentable. Any Dataflow pipeline
relying on unbounded buffering has no equivalent configuration.

Also absent from `FluxBlockOptions` versus `ExecutionDataflowBlockOptions`: `MaxMessagesPerTask`, `TaskScheduler`,
`SingleProducerConstrained`, `NameFormat`.

The defaults question is genuinely two-sided. Matching Dataflow makes migration safe and the defaults worse.
Keeping Flux's makes the defaults better and every migration a silent-breakage risk. **Pick one deliberately and
write the reasoning down** - the current state is that they differ without a recorded decision.

- [ ] Decide: match Dataflow's defaults, or keep Flux's and ship a loud migration table
- [ ] Support unbounded (`BoundedCapacity = -1` or `int.MaxValue`) - needed either way, since it is currently
      unrepresentable rather than just non-default
- [ ] Decide per-option whether `MaxMessagesPerTask` / `TaskScheduler` / `SingleProducerConstrained` /
      `NameFormat` are in scope, no-ops, or explicitly unsupported
- [ ] A migration section in the README listing every semantic difference, whatever the decision

---

## 4. The `DataflowBlock` static surface is almost entirely missing

**Status: not started. Cheapest high-value item on this list.**

Flux ships exactly one helper: `FluxBlock.Post` (a C# extension block over `IFluxTarget<TIn>`).

| `DataflowBlock` member | Flux |
|---|---|
| `Post` | present |
| `SendAsync` | present, but as an instance method on `IFluxTarget<TIn>` rather than a static helper |
| `Receive` / `ReceiveAsync` | **absent** |
| `TryReceive` / `TryReceiveAll` | **absent** |
| `OutputAvailableAsync` | **absent** |
| `Choose` | absent |
| `Encapsulate` | absent (see item 6) |
| `AsObservable` / `AsObserver` | absent |
| `NullTarget<T>` | absent |

`OutputAvailableAsync` + `TryReceive` is *the* canonical Dataflow consumer loop - it is what the Dataflow side of
Flux's own benchmarks uses to drain. `ReceiveAllAsync` is a nicer API but not a substitute: it cannot express
"take one item" or "check availability without consuming".

`NullTarget<T>` is close to a one-liner and its absence is conspicuous.

- [ ] `TryReceive` / `TryReceiveAll` on `IFluxSource<TOut>`
- [ ] `OutputAvailableAsync`
- [ ] `ReceiveAsync` (+ sync `Receive`, or a documented decision not to ship a blocking API)
- [ ] `NullTarget<T>`
- [ ] Decide on `Choose` / `AsObservable` / `AsObserver` (defer-with-note is defensible)

---

## 5. `BatchBlock<T>` is not signature-compatible

**Status: known and deliberate. Needs a decision on how it is presented.**

Flux's `BatchBlock<T>` emits `PooledBatch<T>`; Dataflow's emits `T[]`. Every downstream consumer must change, and
must adopt the dispose-exactly-once contract, which has no runtime guard by design (a guard needs a heap owner
object, which reintroduces the per-batch allocation the type exists to avoid). Broadcasting a batch to multiple
targets is unsafe by design.

This is a good design - it is what buys the 0.05x steady-state allocation ratio. But it means `BatchBlock` cannot
be called drop-in under any definition, and pretending otherwise will burn the first person who tries.

Options: rename the type so the difference is visible at the call site; ship a `T[]`-emitting variant alongside it
for migrators; or accept the break and document it prominently. Not mutually exclusive.

- [ ] Decide: rename, ship an array-emitting sibling, or document the break
- [ ] Whatever is chosen, the README migration table must call this out by name

---

## 6. No third-party extensibility

**Status: not started.**

`TargetFluxBlockBase<TIn>` and `PropagatorFluxBlockBase<TIn,TOut>` are public types with `private protected`
constructors, so **nobody outside the assembly can write a custom block.** Dataflow users can, by implementing
`IPropagatorBlock<TIn,TOut>` and composing with `Encapsulate`.

Combined with item 2, a user who needs a block Flux does not ship has no escape hatch whatsoever. That is the
difference between "missing a few blocks" and "cannot be used for my pipeline".

Note this is currently load-bearing: the base classes carry invariants (see the handoff doc's invariants section)
that a subclass could silently violate. Opening them up needs those invariants expressed as protected contract
rather than as comments.

- [ ] Decide: open the base ctors to `protected`, or ship an `Encapsulate` equivalent, or both
- [ ] If opening the bases: convert the load-bearing invariants into enforced protected API
- [ ] Document the custom-block contract

---

## 7. Link options thinner than `DataflowLinkOptions`

**Status: not started. Small.**

`FluxLinkOptions` carries only `PropagateCompletion`. Missing `MaxMessages` (used for take-N and one-shot links)
and `Append`. `LinkTo` has one overload where Dataflow has three; Flux folds filter and options into optional
parameters on the single signature, which is fine ergonomically but does not cover `MaxMessages` semantics at all.

- [ ] `MaxMessages` on `FluxLinkOptions` (link auto-unlinks after N items)
- [ ] Decide whether `Append` (link ordering control) is meaningful given Flux's `FluxFanOutMode` model

---

## 8. Measured performance gaps

**Status: known, quantified. Ordered by how likely someone is to find them.**

- [ ] **`BatchBlock` time: 1.79x (bs=100) to 2.37x (bs=1000) steady-state, 1.93x at 8 producers.** The only block
      that loses on time, and it is the block with the largest allocation win, so it is the one people will
      benchmark. Untested suspect: `_writeGate` (`SemaphoreSlim`) contention. See handoff open item 4. Note the
      obvious fix (accumulate inside `TryOffer`) is explicitly rejected with four arguments in the handoff doc.
- [ ] **`TransformBlock` DOP 4 ordered: 1.80x allocation.** The only allocation loss in a non-degenerate shape.
      The ordering reassembly path is the weakest allocation site in the library.
- [ ] **Multi-producer scaling slope ~2x steeper than Dataflow's** across all three benchmarked blocks. Ratios
      stay acceptable (parity to 1.93x at 8 producers) because Dataflow degrades too, but Flux degrades faster.
      Traced to `System.Threading.Channels` itself (a bare `BoundedChannel` with 8 writers degrades 2.7x with zero
      Flux code), so not cheaply fixable. **State it up front rather than let it be discovered.**
- [ ] **Burst `BufferBlock` allocation: 1.96x.** A benchmark-shape artifact (channel deque growth under
      send-all-then-drain, documented in the handoff doc's traps section), but reproducible and quotable against
      the project. Worth a pre-emptive explanation in whatever comparison material ships.

---

## 9. Validation breadth

**Status: 112 tests across 5 files, all green over 10 repeat runs, one machine.**

Gaps that matter for a library whose entire value proposition is concurrency:

- [ ] **No ARM64 run**, despite at least one documented memory-model-sensitive invariant
      (`BatchBlock._hasTimer` must be assigned before `_readerLoop` is queued). x64's stronger model hides exactly
      this class of bug. Every correctness claim is currently x64-only.
- [ ] **No soak or stress testing.** All 112 tests are short and deterministic. Nothing exercises sustained
      multi-hour load, pool churn, or high contention over time.
- [ ] **No test for the `SendAsync` single-consumption invariant.** The pooling builder added in `22fd94f` made
      violations actively corrupting (the backing object is recycled) rather than merely undefined. The invariant
      is recorded in the handoff doc but not enforced or tested.
- [ ] Concurrency correctness rests on hand-reasoned invariants in a markdown file. Consider a per-block chaos or
      stress harness that would actually fail if one were broken.
- [ ] Benchmarks are single-machine (AMD 7900X, net10.0 host). No core-count sweep, no server-GC variation.

---

## 10. Packaging and first impression

**Status: package builds on Release; nuget.org would reject it.**

- [ ] `PackageDescription` is **empty** - nuget.org requires a non-empty description
- [ ] `Authors` unset
- [ ] `PackageTags` unset
- [ ] `Copyright` unset
- [ ] `PackageProjectUrl` unset (only `RepositoryUrl` is set)
- [ ] **README is 7 lines** and is the entire pitch surface. Needs: what it is, the honest comparison table, the
      migration/semantic-difference table from item 3, the known-gaps table from items 2/4/5, a quickstart.

Already correct: `LICENSE` and `README.md` exist and are wired via `PackageLicenseFile`/`PackageReadmeFile`;
`IsAotCompatible=true`; `GenerateDocumentationFile=true`; symbol package as `.snupkg`; multi-targets
net8.0/9.0/10.0/11.0. XML doc coverage on the public surface is effectively 100 percent, which is a genuine
strength worth advertising.

---

## Sequencing

**Gate A - "alternative" is honest, "drop-in" is not yet claimed.** Items 3 (decide + unbounded support), 4
(receive helpers), 10 (package metadata + README with an honest gaps table). Cheap, and it makes the project
presentable without overclaiming.

**Gate B - "drop-in" becomes defensible.** Item 1 (interop) and item 2's `TransformManyBlock`. These two are what
convert "rewrite your pipeline" into "migrate incrementally". Item 5's presentation decision rides along.

**Gate C - completeness.** Rest of item 2, items 6 and 7.

**Continuous.** Items 8 and 9 in parallel with all of the above; neither blocks a pitch as long as the numbers are
stated honestly rather than discovered by a skeptic.
