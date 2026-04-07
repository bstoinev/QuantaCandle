using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Describes the prepared checkpoint persistence payload and any continuity gaps discovered while preparing it.
/// </summary>
public sealed class TradeCheckpointBatchPreparation(
    IReadOnlyList<TradeInfo> preparedTrades,
    IReadOnlyList<TradeGap> detectedGaps)
{
    /// <summary>
    /// Gets the normalized trade payload that should be persisted after the checkpoint-time batch stage.
    /// </summary>
    public IReadOnlyList<TradeInfo> PreparedTrades { get; } = preparedTrades;

    /// <summary>
    /// Gets the continuity gaps that were detected while preparing the checkpoint batch.
    /// </summary>
    public IReadOnlyList<TradeGap> DetectedGaps { get; } = detectedGaps;
}
