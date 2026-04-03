using System.Globalization;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Detects live-stream continuity gaps without coupling them to durable sink checkpoint updates.
/// </summary>
public sealed class TradeGapDetector(IIngestionStateStore ingestionStateStore)
{

    private static bool TryGetTradeId(TradeInfo trade, out long tradeId)
    {
        return TryGetTradeId(trade.Key.TradeId, out tradeId);
    }

    private static bool TryGetTradeId(string tradeIdText, out long tradeId)
    {
        return long.TryParse(tradeIdText, NumberStyles.None, CultureInfo.InvariantCulture, out tradeId);
    }

    private static TradeWatermark ToWatermark(TradeInfo trade) => new(trade.Key.TradeId, trade.Timestamp);

    private static bool TryCreateMissingTradeIdRange(string fromExclusiveTradeId, string toInclusiveTradeId, out MissingTradeIdRange? range)
    {
        range = null;

        var hasBeginning = TryGetTradeId(fromExclusiveTradeId, out var fromTradeId);
        var hasEnd = TryGetTradeId(toInclusiveTradeId, out var toTradeId);
        var thereIsGap = hasBeginning && hasEnd && toTradeId > fromTradeId + 1;

        if (thereIsGap)
        {
            range = new MissingTradeIdRange(fromTradeId + 1, toTradeId - 1);
        }

        return range is not null;
    }

    private readonly Dictionary<(ExchangeId Exchange, Instrument Symbol), InstrumentGapState> _states = [];

    private sealed class InstrumentGapState(Task<TradeWatermark?> resumeBoundaryTask)
    {
        public Task<TradeWatermark?> ResumeBoundaryTask { get; } = resumeBoundaryTask;

        public bool ResumeBoundaryApplied { get; set; }

        public TradeInfo? FirstObservedTrade { get; set; }

        public long? LastSequencedTradeId { get; set; }

        public TradeWatermark? LastSequencedWatermark { get; set; }
    }

    /// <summary>
    /// Starts loading the durable resume boundary for an instrument in the background.
    /// </summary>
    public void StartTracking(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
    {
        var key = (exchange, symbol);
        if (!_states.ContainsKey(key))
        {
            var loadBoundaryJob = ingestionStateStore.GetWatermarkAsync(exchange, symbol, cancellationToken).AsTask();

            _states[key] = new InstrumentGapState(loadBoundaryJob);
        }
    }

    /// <summary>
    /// Observes one trade and persists any newly detected gap state transitions.
    /// </summary>
    public async ValueTask Observe(TradeInfo trade, CancellationToken cancellationToken)
    {
        StartTracking(trade.Key.Exchange, trade.Key.Symbol, cancellationToken);

        var state = _states[(trade.Key.Exchange, trade.Key.Symbol)];

        state.FirstObservedTrade ??= trade;

        await TryApplyResumeBoundary(state, trade.Key.Exchange, trade.Key.Symbol, cancellationToken).ConfigureAwait(false);

        if (TryGetTradeId(trade, out long currentTradeId))
        {
            if (currentTradeId > (state.LastSequencedTradeId ?? long.MinValue))
            {
                state.LastSequencedTradeId ??= currentTradeId + 1;
                var gapDetected = currentTradeId > state.LastSequencedTradeId + 1;

                if (gapDetected)
                {
                    var previousWatermark = state.LastSequencedWatermark ?? ToWatermark(trade);
                    await PersistDetectedGap(trade.Key.Exchange, trade.Key.Symbol, previousWatermark, ToWatermark(trade), cancellationToken).ConfigureAwait(false);
                }

                state.LastSequencedTradeId = currentTradeId;
                state.LastSequencedWatermark = ToWatermark(trade);
            }
        }
    }

    /// <summary>
    /// Re-checks any pending initial resume boundary comparison once loading completes.
    /// </summary>
    public async ValueTask FlushPending(CancellationToken cancellationToken)
    {
        foreach (var pair in _states)
        {
            await TryApplyResumeBoundary(pair.Value, pair.Key.Exchange, pair.Key.Symbol, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask TryApplyResumeBoundary(
        InstrumentGapState state,
        ExchangeId exchange,
        Instrument symbol,
        CancellationToken cancellationToken)
    {
        if (state.ResumeBoundaryApplied || state.FirstObservedTrade is null || !state.ResumeBoundaryTask.IsCompletedSuccessfully)
        {
            return;
        }

        state.ResumeBoundaryApplied = true;

        var resumeBoundary = await state.ResumeBoundaryTask.ConfigureAwait(false);
        if (resumeBoundary is not null && TryGetTradeId(resumeBoundary.Value.TradeId, out long resumeTradeId))
        {
            var firstTrade = state.FirstObservedTrade.Value;
            if (TryGetTradeId(firstTrade, out long firstTradeId) && firstTradeId > resumeTradeId + 1)
            {
                await PersistDetectedGap(exchange, symbol, resumeBoundary.Value, ToWatermark(firstTrade), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask PersistDetectedGap(
        ExchangeId exchange,
        Instrument symbol,
        TradeWatermark fromExclusive,
        TradeWatermark toInclusive,
        CancellationToken cancellationToken)
    {
        var openGap = TradeGap.CreateOpen(Guid.NewGuid(), exchange, symbol, fromExclusive, toInclusive.Timestamp);
        await ingestionStateStore.RecordGapAsync(openGap, cancellationToken).ConfigureAwait(false);

        var persistedGap = TryCreateMissingTradeIdRange(fromExclusive.TradeId, toInclusive.TradeId, out MissingTradeIdRange? missingTradeIds)
            ? openGap.ToBounded(toInclusive, missingTradeIds)
            : openGap.ToBounded(toInclusive, null);

        await ingestionStateStore.RecordGapAsync(persistedGap, cancellationToken).ConfigureAwait(false);
    }
}
