using QuantaCandle.Core.Trading;

namespace QuantaCandle.Exchange.Binance.Internal;

internal static class BinanceSymbol
{
    public static string ToRestSymbol(Instrument instrument)
    {
        return $"{instrument.BaseSymbol}{instrument.QuoteSymbol}".ToUpperInvariant();
    }

    public static string ToStreamSymbol(Instrument instrument)
    {
        return $"{instrument.BaseSymbol}{instrument.QuoteSymbol}".ToLowerInvariant();
    }
}

