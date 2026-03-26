using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

public interface ITradeDeduplicator
{
    bool TryAccept(TradeKey key);
}

