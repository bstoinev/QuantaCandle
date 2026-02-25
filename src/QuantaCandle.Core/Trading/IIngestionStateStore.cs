using System.Threading;
using System.Threading.Tasks;

namespace QuantaCandle.Core.Trading;

public interface IIngestionStateStore
{
    /// <summary>
    /// Gets the last committed ingestion watermark for an exchange+symbol, if any.
    /// </summary>
    ValueTask<TradeWatermark?> GetWatermarkAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken);

    /// <summary>
    /// Sets (commits) the ingestion watermark for an exchange+symbol.
    /// </summary>
    ValueTask SetWatermarkAsync(ExchangeId exchange, Instrument symbol, TradeWatermark watermark, CancellationToken cancellationToken);

    /// <summary>
    /// Records an observed gap between two watermarks.
    /// </summary>
    ValueTask RecordGapAsync(TradeGap gap, CancellationToken cancellationToken);
}
