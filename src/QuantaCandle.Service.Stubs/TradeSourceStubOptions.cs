using QuantaCandle.Core.Trading;

namespace QuantaCandle.Service.Stubs;

public sealed record TradeSourceStubOptions(
    ExchangeId Exchange,
    int TradesPerSecond,
    decimal StartPrice,
    decimal PriceStep,
    decimal Quantity);
