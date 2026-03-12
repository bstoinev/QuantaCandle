using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using QuantaCandle.Core;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Service.Stubs;

public sealed class TradeSourceStub : ITradeSource
{
    private readonly TradeSourceStubOptions options;
    private readonly IClock clock;

    public TradeSourceStub(TradeSourceStubOptions options, IClock clock)
    {
        this.options = options;
        this.clock = clock;
    }

    public ExchangeId Exchange
    {
        get
        {
            return options.Exchange;
        }
    }

    public async IAsyncEnumerable<TradeInfo> GetLiveTradesAsync(
        Instrument symbol,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (options.TradesPerSecond <= 0)
        {
            yield break;
        }

        TimeSpan interval = TimeSpan.FromSeconds(1d / options.TradesPerSecond);
        DateTimeOffset start = clock.UtcNow;

        long sequence = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            string tradeId = sequence.ToString();
            DateTimeOffset timestamp = start + TimeSpan.FromTicks(interval.Ticks * sequence);
            decimal price = options.StartPrice + (options.PriceStep * sequence);

            TradeKey key = new TradeKey(options.Exchange, symbol, tradeId);
            yield return new TradeInfo(key, timestamp, price, options.Quantity);

            sequence++;
            await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
        }
    }

    public async IAsyncEnumerable<TradeInfo> GetBackfillTradesAsync(
        Instrument symbol,
        TradeWatermark? fromExclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}
