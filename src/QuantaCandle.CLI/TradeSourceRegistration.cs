using QuantaCandle.Exchange.Binance;
using QuantaCandle.Service.Stubs;

namespace QuantaCandle.CLI;

/// <summary>
/// Holds the source-specific registrations for the collector composition root.
/// </summary>
public sealed record TradeSourceRegistration(BinanceTradeSourceOptions? BinanceOptions, TradeSourceStubOptions? StubOptions);
