namespace QuantaCandle.Core.Trading;

/// <summary>
/// Describes the high-level result of one bounded trade gap healing attempt.
/// </summary>
public enum TradeGapHealStatus
{
    /// <summary>
    /// The requested gap was fully covered by the fetched batch and at least one new trade was persisted.
    /// </summary>
    Full,

    /// <summary>
    /// Only part of the requested gap was covered by the fetched batch and any returned valid trades were still persisted.
    /// </summary>
    Partial,

    /// <summary>
    /// The healing attempt completed without changing the local dataset.
    /// </summary>
    NoChange,

    /// <summary>
    /// The healing attempt completed with a non-throwing failure outcome.
    /// </summary>
    Failed,
}
