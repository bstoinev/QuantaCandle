using System.Collections.Concurrent;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra;

/// <summary>
/// Restores startup resume watermarks from local trade files, including scratch checkpoints, and keeps runtime updates in memory.
/// </summary>
public sealed class LocalFileIngestionStateStore(string localRootDirectory, IClock clock) : IIngestionStateStore
{
    private readonly ConcurrentDictionary<(ExchangeId Exchange, Instrument Symbol), IReadOnlyList<TradeGap>> gapsByInstrument = [];
    private readonly ConcurrentDictionary<(ExchangeId Exchange, Instrument Symbol), ResumeBoundary> resumeBoundaries = [];
    private readonly ConcurrentDictionary<(ExchangeId Exchange, Instrument Symbol), TradeWatermark> watermarks = [];

    /// <summary>
    /// Gets the latest known startup recovery lower bound for an instrument.
    /// </summary>
    public async ValueTask<ResumeBoundary?> GetResumeBoundaryAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
    {
        ResumeBoundary? result = null;

        if (resumeBoundaries.TryGetValue((exchange, symbol), out var resumeBoundary))
        {
            result = resumeBoundary;
        }
        else
        {
            result = await TradeJsonlFile.TryReadLatestResumeBoundaryAsync(localRootDirectory, symbol, clock.UtcNow, cancellationToken).ConfigureAwait(false);

            if (result is not null)
            {
                resumeBoundaries.TryAdd((exchange, symbol), result.Value);
            }
        }

        return result;
    }

    /// <summary>
    /// Gets the latest committed runtime watermark for an instrument.
    /// </summary>
    public ValueTask<TradeWatermark?> GetWatermarkAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
    {
        TradeWatermark? result = null;

        if (watermarks.TryGetValue((exchange, symbol), out var watermark))
        {
            result = watermark;
        }

        return ValueTask.FromResult(result);
    }

    /// <summary>
    /// Stores the latest committed runtime watermark in memory.
    /// </summary>
    public ValueTask SetWatermarkAsync(ExchangeId exchange, Instrument symbol, TradeWatermark watermark, CancellationToken cancellationToken)
    {
        watermarks[(exchange, symbol)] = watermark;
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Stores the latest gap snapshot for the instrument stream.
    /// </summary>
    public ValueTask RecordGapAsync(TradeGap gap, CancellationToken cancellationToken)
    {
        gapsByInstrument.AddOrUpdate(
            (gap.Exchange, gap.Symbol),
            _ => new[] { gap },
            (_, existing) =>
            {
                var result = existing.ToList();
                result.Add(gap);
                return result;
            });

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Returns the current in-memory gap snapshot for the instrument stream.
    /// </summary>
    public ValueTask<IReadOnlyList<TradeGap>> GetGapsAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
    {
        IReadOnlyList<TradeGap> result = Array.Empty<TradeGap>();

        if (gapsByInstrument.TryGetValue((exchange, symbol), out var gaps))
        {
            result = gaps;
        }

        return ValueTask.FromResult(result);
    }
}
