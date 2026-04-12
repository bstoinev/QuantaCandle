namespace QuantaCandle.Core.Trading;

/// <summary>
/// Augments a completed read-only trade gap scan result with additional scan-only findings.
/// </summary>
public interface ITradeGapScanAugmenter
{
    /// <summary>
    /// Adds scan-only findings to an existing scan result without mutating local trade files.
    /// </summary>
    ValueTask<TradeGapScanResult> Augment(
        TradeGapScanRequest request,
        TradeGapScanResult scanResult,
        CancellationToken cancellationToken);
}
