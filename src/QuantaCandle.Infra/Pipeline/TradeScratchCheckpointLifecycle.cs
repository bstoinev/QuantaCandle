using System.Globalization;

using LogMachina;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Persists recorder-owned scratch checkpoints while retaining the latest trade only in memory.
/// </summary>
public sealed class TradeScratchCheckpointLifecycle(
    string localRootDirectory,
    ITradeFinalizedFileDispatcher tradeFinalizedFileDispatcher,
    IIngestionStateStore ingestionStateStore,
    ILogMachina<TradeScratchCheckpointLifecycle> log) : ITradeCheckpointLifecycle
{
    private readonly Lock _gate = new();
    private readonly Dictionary<(ExchangeId Exchange, Instrument Symbol), List<TradeInfo>> _pendingTradesByInstrument = [];
    private readonly Dictionary<(ExchangeId Exchange, Instrument Symbol), InstrumentScratchState> _scratchStatesByInstrument = [];

    /// <summary>
    /// Tracks trades that should participate in scratch checkpoints.
    /// </summary>
    public ValueTask TrackAppendedTrades(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            foreach (var trade in trades)
            {
                var key = (trade.Key.Exchange, trade.Key.Symbol);
                if (!_pendingTradesByInstrument.TryGetValue(key, out var pendingTrades))
                {
                    pendingTrades = [];
                    _pendingTradesByInstrument[key] = pendingTrades;
                }

                pendingTrades.Add(trade);
            }
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Persists every currently checkpointable trade to the scratch file and retains only the latest trade in memory.
    /// </summary>
    public async ValueTask<bool> CheckpointActive(CancellationToken cancellationToken)
    {
        Dictionary<(ExchangeId Exchange, Instrument Symbol), TradeCheckpointSlice> checkpointSlices;

        lock (_gate)
        {
            checkpointSlices = BuildCheckpointSlices();
        }

        foreach (var pair in checkpointSlices)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (pair.Value.TradesToPersist.Count == 0)
            {
                continue;
            }

            var persistenceState = await PersistScratchStateAsync(pair.Key.Exchange, pair.Key.Symbol, pair.Value.TradesToPersist, cancellationToken).ConfigureAwait(false);
            await RecordCurrentBatchGapsAsync(pair.Key.Exchange, pair.Key.Symbol, pair.Value.TradesToPersist, cancellationToken).ConfigureAwait(false);

            lock (_gate)
            {
                _scratchStatesByInstrument[pair.Key] = persistenceState;
            }
        }

        lock (_gate)
        {
            ApplyCheckpointSlices(checkpointSlices);
        }

        return true;
    }

    /// <summary>
    /// Persists the current scratch checkpoint state during graceful shutdown.
    /// </summary>
    public async ValueTask FlushOnShutdown(CancellationToken cancellationToken)
    {
        await CheckpointActive(cancellationToken).ConfigureAwait(false);
    }

    private Dictionary<(ExchangeId Exchange, Instrument Symbol), TradeCheckpointSlice> BuildCheckpointSlices()
    {
        var result = new Dictionary<(ExchangeId Exchange, Instrument Symbol), TradeCheckpointSlice>();

        foreach (var pair in _pendingTradesByInstrument)
        {
            var pendingTrades = pair.Value;
            var originalTradesToPersist = pendingTrades.Count > 1
                ? pendingTrades.Take(pendingTrades.Count - 1).ToArray()
                : [];
            var tradesToPersist = NormalizeCheckpointTrades(originalTradesToPersist);
            var currentScratchState = _scratchStatesByInstrument.TryGetValue(pair.Key, out var scratchState)
                ? scratchState
                : InstrumentScratchState.Empty;

            result[pair.Key] = new TradeCheckpointSlice(originalTradesToPersist.Length, tradesToPersist, currentScratchState);
        }

        return result;
    }

    private void ApplyCheckpointSlices(Dictionary<(ExchangeId Exchange, Instrument Symbol), TradeCheckpointSlice> checkpointSlices)
    {
        foreach (var pair in checkpointSlices)
        {
            if (!_pendingTradesByInstrument.TryGetValue(pair.Key, out var pendingTrades))
            {
                continue;
            }

            if (pair.Value.PersistedTradeCount > 0)
            {
                pendingTrades.RemoveRange(0, Math.Min(pair.Value.PersistedTradeCount, pendingTrades.Count));
            }
        }
    }

    private async Task<InstrumentScratchState> PersistScratchStateAsync(
        ExchangeId exchange,
        Instrument instrument,
        IReadOnlyList<TradeInfo> tradesToPersist,
        CancellationToken cancellationToken)
    {
        var result = InstrumentScratchState.Empty;
        var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRootDirectory, instrument);
        var checkpointState = _scratchStatesByInstrument.TryGetValue((exchange, instrument), out var currentScratchState)
            ? currentScratchState
            : InstrumentScratchState.Empty;
        var currentScratchUtcDate = checkpointState.ActiveScratchUtcDate ?? DateOnly.FromDateTime(tradesToPersist[0].Timestamp.UtcDateTime);
        var remainingTrades = tradesToPersist.ToList();
        TradeInfo? lastRecordedTrade = checkpointState.LastRecordedTrade;

        while (remainingTrades.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var newestBatchUtcDate = DateOnly.FromDateTime(remainingTrades[^1].Timestamp.UtcDateTime);
            if (currentScratchUtcDate == newestBatchUtcDate)
            {
                await AppendTradesToScratchAsync(instrument, scratchPath, remainingTrades, cancellationToken).ConfigureAwait(false);
                lastRecordedTrade = remainingTrades[^1];
                result = new InstrumentScratchState(lastRecordedTrade, currentScratchUtcDate);
                remainingTrades.Clear();
            }
            else
            {
                var finalizedTrades = remainingTrades
                    .TakeWhile(trade => DateOnly.FromDateTime(trade.Timestamp.UtcDateTime) == currentScratchUtcDate)
                    .ToArray();

                if (finalizedTrades.Length == 0)
                {
                    currentScratchUtcDate = DateOnly.FromDateTime(remainingTrades[0].Timestamp.UtcDateTime);
                    continue;
                }

                await AppendTradesToScratchAsync(instrument, scratchPath, finalizedTrades, cancellationToken).ConfigureAwait(false);
                lastRecordedTrade = finalizedTrades[^1];
                var finalizedPath = await FinalizeScratchAsync(instrument, currentScratchUtcDate, scratchPath, cancellationToken).ConfigureAwait(false);
                await DispatchFinalizedFileAsync(instrument, currentScratchUtcDate, finalizedPath, cancellationToken).ConfigureAwait(false);

                remainingTrades.RemoveRange(0, finalizedTrades.Length);
                currentScratchUtcDate = remainingTrades.Count > 0
                    ? DateOnly.FromDateTime(remainingTrades[0].Timestamp.UtcDateTime)
                    : default;
                result = remainingTrades.Count > 0
                    ? new InstrumentScratchState(lastRecordedTrade, currentScratchUtcDate)
                    : new InstrumentScratchState(lastRecordedTrade, null);
            }
        }

        return result;
    }

    private async Task AppendTradesToScratchAsync(
        Instrument instrument,
        string scratchPath,
        IReadOnlyList<TradeInfo> trades,
        CancellationToken cancellationToken)
    {
        log.Info($"Trade scratch checkpoint append: instrument={instrument}, path={scratchPath}, tradeCount={trades.Count}.");
        await TradeJsonlFile.AppendTradesAsync(scratchPath, trades, cancellationToken).ConfigureAwait(false);
    }

    private async Task<string> FinalizeScratchAsync(
        Instrument instrument,
        DateOnly utcDate,
        string scratchPath,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var finalizedPath = TradeLocalDailyFilePath.Build(localRootDirectory, instrument, utcDate);
        var finalizedDirectory = Path.GetDirectoryName(finalizedPath);

        if (!string.IsNullOrWhiteSpace(finalizedDirectory))
        {
            Directory.CreateDirectory(finalizedDirectory);
        }

        if (File.Exists(finalizedPath))
        {
            throw new InvalidOperationException($"The finalized day file already exists: {finalizedPath}.");
        }

        log.Info($"Trade scratch checkpoint finalize: instrument={instrument}, utcDate={utcDate:yyyy-MM-dd}, from={scratchPath}, to={finalizedPath}.");
        File.Move(scratchPath, finalizedPath);

        var result = finalizedPath;
        return result;
    }

    private async Task DispatchFinalizedFileAsync(
        Instrument instrument,
        DateOnly utcDate,
        string finalizedPath,
        CancellationToken cancellationToken)
    {
        await tradeFinalizedFileDispatcher.DispatchAsync(instrument, utcDate, finalizedPath, cancellationToken).ConfigureAwait(false);
    }

    private async Task RecordCurrentBatchGapsAsync(
        ExchangeId exchange,
        Instrument instrument,
        IReadOnlyList<TradeInfo> tradesToPersist,
        CancellationToken cancellationToken)
    {
        for (var i = 1; i < tradesToPersist.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var previousTrade = tradesToPersist[i - 1];
            var currentTrade = tradesToPersist[i];
            var hasPreviousTradeId = TryGetTradeId(previousTrade.Key.TradeId, out var previousTradeId);
            var hasCurrentTradeId = TryGetTradeId(currentTrade.Key.TradeId, out var currentTradeId);
            var hasGap = hasPreviousTradeId
                && hasCurrentTradeId
                && currentTradeId > previousTradeId + 1;

            if (hasGap)
            {
                var gapId = Guid.NewGuid();
                var fromExclusive = new TradeWatermark(previousTrade.Key.TradeId, previousTrade.Timestamp);
                var toInclusive = new TradeWatermark(currentTrade.Key.TradeId, currentTrade.Timestamp);
                var openGap = TradeGap.CreateOpen(gapId, exchange, instrument, fromExclusive, currentTrade.Timestamp);
                var missingTradeIds = new MissingTradeIdRange(previousTradeId + 1, currentTradeId - 1);

                await ingestionStateStore.RecordGapAsync(openGap, cancellationToken).ConfigureAwait(false);
                await ingestionStateStore.RecordGapAsync(openGap.ToBounded(toInclusive, missingTradeIds), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static bool TryGetTradeId(string tradeIdText, out long tradeId)
    {
        var result = long.TryParse(tradeIdText, NumberStyles.None, CultureInfo.InvariantCulture, out tradeId);
        return result;
    }

    private static TradeInfo[] NormalizeCheckpointTrades(IReadOnlyList<TradeInfo> trades)
    {
        var orderedTrades = trades
            .OrderBy(trade => trade.Timestamp)
            .ThenBy(trade => trade.Key.TradeId, StringComparer.Ordinal);
        var seenTradeKeys = new HashSet<TradeKey>();
        var result = new List<TradeInfo>(trades.Count);

        foreach (var trade in orderedTrades)
        {
            if (seenTradeKeys.Add(trade.Key))
            {
                result.Add(trade);
            }
        }

        return result.ToArray();
    }

    /// <summary>
    /// Describes the active scratch state for one exchange instrument stream.
    /// </summary>
    private sealed class InstrumentScratchState(TradeInfo? lastRecordedTrade, DateOnly? activeScratchUtcDate)
    {
        public static InstrumentScratchState Empty { get; } = new(null, null);

        public DateOnly? ActiveScratchUtcDate { get; } = activeScratchUtcDate;

        public TradeInfo? LastRecordedTrade { get; } = lastRecordedTrade;
    }

    /// <summary>
    /// Describes the trades that should be appended to a scratch checkpoint file for one instrument.
    /// </summary>
    private sealed class TradeCheckpointSlice(
        int persistedTradeCount,
        IReadOnlyList<TradeInfo> tradesToPersist,
        InstrumentScratchState scratchState)
    {
        /// <summary>
        /// Gets the number of trades that were persisted from the front of the pending in-memory sequence.
        /// </summary>
        public int PersistedTradeCount { get; } = persistedTradeCount;

        /// <summary>
        /// Gets the recorder-owned scratch state for the instrument before the checkpoint begins.
        /// </summary>
        public InstrumentScratchState ScratchState { get; } = scratchState;

        /// <summary>
        /// Gets the trades that should be appended to the instrument scratch file.
        /// </summary>
        public IReadOnlyList<TradeInfo> TradesToPersist { get; } = tradesToPersist;
    }
}
