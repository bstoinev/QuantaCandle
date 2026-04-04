namespace QuantaCandle.Core.Trading;

/// <summary>
/// Describes one trade range that was identified or modified during gap scan or healing work.
/// </summary>
public sealed record TradeGapAffectedRange(TradeWatermark FromInclusive, TradeWatermark ToInclusive)
{
    /// <summary>
    /// Gets the inclusive lower bound of the affected trade range.
    /// </summary>
    public TradeWatermark FromInclusive { get; } = FromInclusive;

    /// <summary>
    /// Gets the inclusive upper bound of the affected trade range.
    /// </summary>
    public TradeWatermark ToInclusive { get; } = ToInclusive.Timestamp < FromInclusive.Timestamp
        ? throw new ArgumentException("ToInclusive must not be earlier than FromInclusive.", nameof(ToInclusive))
        : ToInclusive;

    /// <summary>
    /// Gets the optional file and line location for the lower boundary trade.
    /// </summary>
    public TradeGapBoundaryLocation? FromLocation { get; } = FromLocation;

    /// <summary>
    /// Gets the optional file and line location for the upper boundary trade.
    /// </summary>
    public TradeGapBoundaryLocation? ToLocation { get; } = ToLocation;
}
