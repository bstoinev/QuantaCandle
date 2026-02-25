using System;

namespace QuantaCandle.Core.Trading;

public readonly record struct TradeKey
{
    public TradeKey(ExchangeId exchange, Instrument symbol, string tradeId)
    {
        if (string.IsNullOrWhiteSpace(tradeId))
        {
            throw new ArgumentException("TradeId cannot be null or whitespace.", nameof(tradeId));
        }

        Exchange = exchange;
        Symbol = symbol;
        TradeId = tradeId.Trim();
    }

    public ExchangeId Exchange { get; }

    public Instrument Symbol { get; }

    /// <summary>
    /// Exchange-native trade identifier (opaque).
    /// </summary>
    public string TradeId { get; }
}
