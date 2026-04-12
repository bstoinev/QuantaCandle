namespace QuantaCandle.Core.Trading;

/// <summary>
/// Consumes downloaded trade pages during gap healing.
/// </summary>
public interface ITradeGapFetchedPageSink
{
    /// <summary>
    /// Accepts one sequential downloaded page and persists or processes it immediately.
    /// </summary>
    ValueTask AcceptPage(IReadOnlyList<TradeInfo> pageTrades, CancellationToken cancellationToken);
}
