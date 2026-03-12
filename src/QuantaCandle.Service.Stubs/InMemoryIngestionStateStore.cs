using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Service.Stubs;

public sealed class InMemoryIngestionStateStore : IIngestionStateStore
{
    private readonly ConcurrentDictionary<(ExchangeId Exchange, Instrument Symbol), TradeWatermark> watermarks;

    public InMemoryIngestionStateStore()
    {
        watermarks = new ConcurrentDictionary<(ExchangeId, Instrument), TradeWatermark>();
    }

    public ValueTask<TradeWatermark?> GetWatermarkAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (watermarks.TryGetValue((exchange, symbol), out TradeWatermark watermark))
        {
            return ValueTask.FromResult<TradeWatermark?>(watermark);
        }

        return ValueTask.FromResult<TradeWatermark?>(null);
    }

    public ValueTask SetWatermarkAsync(ExchangeId exchange, Instrument symbol, TradeWatermark watermark, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        watermarks[(exchange, symbol)] = watermark;
        return ValueTask.CompletedTask;
    }

    public ValueTask RecordGapAsync(TradeGap gap, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.CompletedTask;
    }
}
