using System.Threading.Channels;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

public static class BoundedChannelFactory
{
    public static Channel<TradeInfo> CreateTradeChannel(int capacity)
    {
        BoundedChannelOptions options = new BoundedChannelOptions(capacity)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
            AllowSynchronousContinuations = false,
        };

        return Channel.CreateBounded<TradeInfo>(options);
    }
}
