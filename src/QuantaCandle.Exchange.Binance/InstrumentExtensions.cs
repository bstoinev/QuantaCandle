using QuantaCandle.Core.Trading;

namespace QuantaCandle.Exchange.Binance;

internal static class InstrumentExtensions
{
    public static string ToStreamSymbol(Instrument instrument)
    {
        return $"{instrument.BaseSymbol}{instrument.QuoteSymbol}".ToLowerInvariant();
    }
}

