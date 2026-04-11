using QuantaCandle.Core.Trading;

namespace QuantaCandle.Exchange.Binance;

/// <summary>
/// Resolves Binance raw trades for UTC day-boundary calculations.
/// </summary>
public interface IBinanceRawTradeLookupClient
{
    /// <summary>
    /// Searches through Binance API to find the first raw trade at or after the requested UTC timestamp.
    /// </summary>
    ValueTask<TradeInfo> FindFirstTradeAt(DateTimeOffset timestampUtc, Instrument instrument, CancellationToken cancellationToken);

    /// <summary>
    /// Verifies that the requested Binance raw trade identifier exists exactly.
    /// </summary>
    ValueTask<bool> TryVerifyRawTradeId(Instrument instrument, long tradeId, CancellationToken cancellationToken);
}
