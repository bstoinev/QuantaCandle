namespace QuantaCandle.Core.Trading;

/// <summary>
/// Fetches exchange trades for a known bounded missing trade identifier range.
/// </summary>
public interface ITradeGapFetchClient
{
    /// <summary>
    /// Fetches trades for the requested instrument and inclusive missing trade identifier range.
    /// </summary>
    ValueTask<IReadOnlyList<TradeInfo>> Fetch(Instrument instrument, long missingTradeIdStart, long missingTradeIdEnd, CancellationToken cancellationToken);
}
