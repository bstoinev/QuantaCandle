using QuantaCandle.Exchange.Binance;

namespace QuantaCandle.Infra;

/// <summary>
/// Holds the source-specific registrations for the trade recorder container.
/// </summary>
public sealed record TradeRecorderSourceRegistration(BinanceTradeSourceOptions? BinanceOptions, TradeSourceStubOptions? StubOptions);
