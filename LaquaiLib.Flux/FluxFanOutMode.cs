namespace LaquaiLib.Flux;

/// <summary>
/// Selects how a Flux block with more than one active link dispatches each produced item. Has no observable
/// effect while 0 or 1 links are active.
/// </summary>
public enum FluxFanOutMode
{
    /// <summary>
    /// Duplicates every produced item to every currently-linked, filter-matching target. The default.
    /// A target declining (permanently completed or faulted) is non-fatal to delivery on the others.
    /// </summary>
    Broadcast,

    /// <summary>
    /// Link order is priority order: the first filter-matching target able to accept the item - synchronously,
    /// or eventually by waiting - receives it, and no other linked target sees that item. A higher-priority
    /// target that has permanently completed or faulted can never stall delivery to a lower-priority one.
    /// </summary>
    FirstAvailable,

    /// <summary>
    /// Adaptively load-balances across filter-matching targets: a rotating cursor sweeps for the first
    /// currently-free target starting from a different position each item, so a faster consumer ends up
    /// receiving proportionally more items than a slower one without any explicit speed tracking.
    /// </summary>
    RoundRobin,
}
