using System.Threading;
using System.Threading.Tasks;

namespace QuantaCandle.Core.Trading;

public interface IIngestionStateStore
{
    /// <summary>
    /// Gets the startup resume lower bound for an exchange+symbol, if any.
    /// </summary>
    ValueTask<ResumeBoundary?> GetResumeBoundaryAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken);

    /// <summary>
    /// Gets the last committed ingestion watermark for an exchange+symbol, if any.
    /// </summary>
    ValueTask<TradeWatermark?> GetWatermarkAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken);

    /// <summary>
    /// Sets (commits) the ingestion watermark for an exchange+symbol.
    /// </summary>
    ValueTask SetWatermarkAsync(ExchangeId exchange, Instrument symbol, TradeWatermark watermark, CancellationToken cancellationToken);

    /// <summary>
    /// Persists the current state of a trade gap for one instrument stream.
    /// </summary>
    ValueTask RecordGapAsync(TradeGap gap, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the persisted gap state snapshots for an exchange+symbol.
    /// </summary>
    ValueTask<IReadOnlyList<TradeGap>> GetGapsAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken);
}
