using System.Globalization;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Detects live-stream continuity gaps without coupling them to durable sink checkpoint updates.
/// </summary>
public sealed class TradeGapDetector(IIngestionStateStore ingestionStateStore)
{
    private readonly Dictionary<(ExchangeId Exchange, Instrument Symbol), InstrumentGapState> _states = [];

    /// <summary>
    /// Starts loading the durable resume boundary for an instrument in the background.
    /// </summary>
    public void StartTracking(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
    {
        var key = (exchange, symbol);
        if (!_states.ContainsKey(key))
        {
            _states[key] = new InstrumentGapState(LoadResumeBoundary(exchange, symbol, cancellationToken));
        }
    }

    /// <summary>
    /// Observes one trade and persists any newly detected gap state transitions.
    /// </summary>
    public async ValueTask Observe(TradeInfo trade, CancellationToken cancellationToken)
    {
        StartTracking(trade.Key.Exchange, trade.Key.Symbol, cancellationToken);

        var state = _states[(trade.Key.Exchange, trade.Key.Symbol)];

        if (state.FirstObservedTrade is null)
        {
            state.FirstObservedTrade = trade;
        }

        await TryApplyResumeBoundary(state, trade.Key.Exchange, trade.Key.Symbol, cancellationToken).ConfigureAwait(false);

        if (!TryGetTradeId(trade, out long currentTradeId))
        {
            return;
        }

        if (state.LastSequencedTradeId is null)
        {
            state.LastSequencedTradeId = currentTradeId;
            state.LastSequencedWatermark = ToWatermark(trade);
            return;
        }

        if (currentTradeId <= state.LastSequencedTradeId.Value)
        {
            return;
        }

        if (currentTradeId > state.LastSequencedTradeId.Value + 1)
        {
            var previousWatermark = state.LastSequencedWatermark ?? ToWatermark(trade);
            await PersistDetectedGap(trade.Key.Exchange, trade.Key.Symbol, previousWatermark, ToWatermark(trade), cancellationToken).ConfigureAwait(false);
        }

        state.LastSequencedTradeId = currentTradeId;
        state.LastSequencedWatermark = ToWatermark(trade);
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

        TradeWatermark? resumeBoundary = await state.ResumeBoundaryTask.ConfigureAwait(false);
        if (resumeBoundary is null || !TryGetTradeId(resumeBoundary.Value.TradeId, out long resumeTradeId))
        {
            return;
        }

        var firstTrade = state.FirstObservedTrade.Value;
        if (!TryGetTradeId(firstTrade, out long firstTradeId))
        {
            return;
        }

        if (firstTradeId > resumeTradeId + 1)
        {
            await PersistDetectedGap(exchange, symbol, resumeBoundary.Value, ToWatermark(firstTrade), cancellationToken).ConfigureAwait(false);
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

        TradeGap persistedGap = openGap;
        if (TryCreateMissingTradeIdRange(fromExclusive.TradeId, toInclusive.TradeId, out MissingTradeIdRange? missingTradeIds))
        {
            persistedGap = openGap.ToBounded(toInclusive, missingTradeIds);
        }
        else
        {
            persistedGap = openGap.ToBounded(toInclusive, null);
        }

        await ingestionStateStore.RecordGapAsync(persistedGap, cancellationToken).ConfigureAwait(false);
    }

    private Task<TradeWatermark?> LoadResumeBoundary(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
    {
        return ingestionStateStore
            .GetWatermarkAsync(exchange, symbol, cancellationToken)
            .AsTask();
    }

    private static bool TryGetTradeId(TradeInfo trade, out long tradeId)
    {
        return TryGetTradeId(trade.Key.TradeId, out tradeId);
    }

    private static bool TryGetTradeId(string tradeIdText, out long tradeId)
    {
        return long.TryParse(tradeIdText, NumberStyles.None, CultureInfo.InvariantCulture, out tradeId);
    }

    private static TradeWatermark ToWatermark(TradeInfo trade)
    {
        var watermark = new TradeWatermark(trade.Key.TradeId, trade.Timestamp);
        return watermark;
    }

    private static bool TryCreateMissingTradeIdRange(string fromExclusiveTradeId, string toInclusiveTradeId, out MissingTradeIdRange? range)
    {
        range = null;
        if (!TryGetTradeId(fromExclusiveTradeId, out long fromTradeId) || !TryGetTradeId(toInclusiveTradeId, out long toTradeId))
        {
            return false;
        }

        if (toTradeId <= fromTradeId + 1)
        {
            return false;
        }

        range = new MissingTradeIdRange(fromTradeId + 1, toTradeId - 1);
        return true;
    }

    private sealed class InstrumentGapState
    {
        public InstrumentGapState(Task<TradeWatermark?> resumeBoundaryTask)
        {
            ResumeBoundaryTask = resumeBoundaryTask;
        }

        public Task<TradeWatermark?> ResumeBoundaryTask { get; }

        public bool ResumeBoundaryApplied { get; set; }

        public TradeInfo? FirstObservedTrade { get; set; }

        public long? LastSequencedTradeId { get; set; }

        public TradeWatermark? LastSequencedWatermark { get; set; }
    }
}
