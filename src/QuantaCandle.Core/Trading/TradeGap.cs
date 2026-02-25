using System;

namespace QuantaCandle.Core.Trading;

public sealed record TradeGap(
    ExchangeId Exchange,
    Instrument Symbol,
    TradeWatermark FromExclusive,
    TradeWatermark ToInclusive,
    DateTimeOffset ObservedAt);
