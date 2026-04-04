namespace QuantaCandle.Core.Trading;

/// <summary>
/// Scans a trade history segment and reports any detected gaps without attempting healing.
/// </summary>
public interface ITradeGapScanner
{
    /// <summary>
    /// Executes a gap scan for the requested exchange instrument scope.
    /// </summary>
    ValueTask<TradeGapScanResult> Scan(TradeGapScanRequest request, CancellationToken cancellationToken);
}
