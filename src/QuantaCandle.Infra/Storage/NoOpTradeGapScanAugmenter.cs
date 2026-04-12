using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Storage;

/// <summary>
/// Leaves a trade gap scan result unchanged when no scan-only augmentation is required.
/// </summary>
public sealed class NoOpTradeGapScanAugmenter : ITradeGapScanAugmenter
{
    /// <summary>
    /// Returns the supplied scan result without modification.
    /// </summary>
    public ValueTask<TradeGapScanResult> Augment(
        TradeGapScanRequest request,
        TradeGapScanResult scanResult,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(scanResult);

        return ValueTask.FromResult(scanResult);
    }
}
