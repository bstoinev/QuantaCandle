using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra;

public sealed record TradeSourceStubOptions(
    ExchangeId Exchange,
    int TradesPerSecond,
    decimal StartPrice,
    decimal PriceStep,
    decimal Quantity);
