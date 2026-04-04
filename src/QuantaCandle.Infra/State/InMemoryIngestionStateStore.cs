using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra;

public sealed class InMemoryIngestionStateStore : IIngestionStateStore
{
    private readonly ConcurrentDictionary<(ExchangeId Exchange, Instrument Symbol), ResumeBoundary> resumeBoundaries;
    private readonly ConcurrentDictionary<(ExchangeId Exchange, Instrument Symbol), TradeWatermark> watermarks;
    private readonly ConcurrentDictionary<(ExchangeId Exchange, Instrument Symbol), ConcurrentDictionary<Guid, TradeGap>> gaps;

    public InMemoryIngestionStateStore()
    {
        resumeBoundaries = new ConcurrentDictionary<(ExchangeId, Instrument), ResumeBoundary>();
        watermarks = new ConcurrentDictionary<(ExchangeId, Instrument), TradeWatermark>();
        gaps = new ConcurrentDictionary<(ExchangeId, Instrument), ConcurrentDictionary<Guid, TradeGap>>();
    }

    public ValueTask<ResumeBoundary?> GetResumeBoundaryAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var result = ValueTask.FromResult<ResumeBoundary?>(null);

        if (resumeBoundaries.TryGetValue((exchange, symbol), out ResumeBoundary resumeBoundary))
        {
            result = ValueTask.FromResult<ResumeBoundary?>(resumeBoundary);
        }

        return result;
    }

    public ValueTask<TradeWatermark?> GetWatermarkAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var result = ValueTask.FromResult<TradeWatermark?>(null);

        if (watermarks.TryGetValue((exchange, symbol), out TradeWatermark watermark))
        {
            result = ValueTask.FromResult<TradeWatermark?>(watermark);
        }

        return result;
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

        var instrumentGaps = gaps.GetOrAdd((gap.Exchange, gap.Symbol), _ => new ConcurrentDictionary<Guid, TradeGap>());

        instrumentGaps[gap.GapId] = gap;

        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<TradeGap>> GetGapsAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        IReadOnlyList<TradeGap> result;
        if (gaps.TryGetValue((exchange, symbol), out ConcurrentDictionary<Guid, TradeGap>? instrumentGaps))
        {
            result = instrumentGaps.Values
                .OrderBy(gap => gap.ObservedAt)
                .ToArray();
        }
        else
        {
            result = [];
        }

        return ValueTask.FromResult(result);
    }

    /// <summary>
    /// Stores a startup resume lower bound for one instrument stream.
    /// </summary>
    public void SetResumeBoundary(ExchangeId exchange, Instrument symbol, ResumeBoundary resumeBoundary)
    {
        resumeBoundaries[(exchange, symbol)] = resumeBoundary;
    }
}
