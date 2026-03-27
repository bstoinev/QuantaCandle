namespace QuantaCandle.Core.Trading;

/// <summary>
/// Describes the lifecycle stage of a persisted trade gap.
/// </summary>
public enum TradeGapStatus
{
    Open = 0,
    Bounded = 1,
    Resolved = 2,
}
