using QuantaCandle.Core.Trading;

namespace QuantaCandle.Service.Pipeline;

public interface ITradeDeduplicator
{
    bool TryAccept(TradeKey key);
}

