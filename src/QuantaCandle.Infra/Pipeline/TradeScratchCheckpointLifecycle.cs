using System.Globalization;

using LogMachina;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Persists recorder-owned scratch checkpoints while retaining the latest trade only in memory.
/// </summary>
public sealed class TradeScratchCheckpointLifecycle(
    IClock clock,
    string localRootDirectory,
    ITradeFinalizedFileDispatcher tradeFinalizedFileDispatcher,
    ITradeSnapshotFileDispatcher tradeSnapshotFileDispatcher,
    IIngestionStateStore ingestionStateStore,
    ILogMachina<TradeScratchCheckpointLifecycle> log) : ITradeCheckpointLifecycle
{
    private readonly Lock _gate = new();
    private readonly Dictionary<(ExchangeId Exchange, Instrument Symbol), List<TradeInfo>> _pendingTradesByInstrument = [];
    private readonly Dictionary<(ExchangeId Exchange, Instrument Symbol), InstrumentScratchState> _scratchStatesByInstrument = [];
    private IReadOnlyList<ActiveScratchSnapshotContext> _lastCheckpointSnapshotContexts = [];

    /// <summary>
    /// Tracks trades that should participate in checkpoints.
    /// </summary>
    public ValueTask<int> TrackAppendedTrades(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var result = 0;

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

            result = _pendingTradesByInstrument.Sum(pair => pair.Value.Count);
        }

        return ValueTask.FromResult(result);
    }

    /// <summary>
    /// Persists every currently checkpointable trade to the scratch file and retains only the latest trade in memory.
    /// </summary>
    public async ValueTask<bool> CheckpointActive(CancellationToken cancellationToken)
    {
        Dictionary<(ExchangeId Exchange, Instrument Symbol), TradeCheckpointSlice> checkpointSlices;
        var checkpointSnapshotContexts = new List<ActiveScratchSnapshotContext>();

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

            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRootDirectory, pair.Key.Exchange, pair.Key.Symbol);
            var persistenceState = await PersistScratchStateAsync(pair.Key.Exchange, pair.Key.Symbol, scratchPath, pair.Value.TradesToPersist, cancellationToken).ConfigureAwait(false);
            await RecordCurrentBatchGapsAsync(pair.Key.Exchange, pair.Key.Symbol, pair.Value.TradesToPersist, cancellationToken).ConfigureAwait(false);

            lock (_gate)
            {
                _scratchStatesByInstrument[pair.Key] = persistenceState;
            }

            if (File.Exists(scratchPath))
            {
                checkpointSnapshotContexts.Add(new ActiveScratchSnapshotContext(pair.Key.Exchange, pair.Key.Symbol, scratchPath));
            }
        }

        lock (_gate)
        {
            ApplyCheckpointSlices(checkpointSlices);
            _lastCheckpointSnapshotContexts = checkpointSnapshotContexts;
        }

        return true;
    }

    /// <summary>
    /// Exports point-in-time copies of the active persisted scratch files and dispatches them through the dedicated snapshot path.
    /// </summary>
    public async ValueTask<bool> DispatchActiveSnapshot(CancellationToken cancellationToken)
    {
        var snapshotUtcTimestamp = clock.UtcNow;
        List<ActiveScratchSnapshotContext> snapshotContexts;

        lock (_gate)
        {
            snapshotContexts = [.. _lastCheckpointSnapshotContexts];
        }

        var snapshotPaths = new List<(ExchangeId Exchange, Instrument Instrument, string SnapshotPath)>(snapshotContexts.Count);

        foreach (var snapshotContext in snapshotContexts)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var snapshotPath = TradeLocalDailyFilePath.BuildSnapshot(localRootDirectory, snapshotContext.Exchange, snapshotContext.Instrument, snapshotUtcTimestamp);
            var snapshotDirectory = Path.GetDirectoryName(snapshotPath);
            if (!string.IsNullOrWhiteSpace(snapshotDirectory))
            {
                Directory.CreateDirectory(snapshotDirectory);
            }

            log.Info($"Trade scratch snapshot export: exchange={snapshotContext.Exchange}, instrument={snapshotContext.Instrument}, from={snapshotContext.ScratchPath}, to={snapshotPath}.");
            File.Copy(snapshotContext.ScratchPath, snapshotPath, overwrite: false);
            snapshotPaths.Add((snapshotContext.Exchange, snapshotContext.Instrument, snapshotPath));
        }

        foreach (var snapshotPath in snapshotPaths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await tradeSnapshotFileDispatcher.DispatchAsync(snapshotPath.Exchange, snapshotPath.Instrument, snapshotPath.SnapshotPath, cancellationToken).ConfigureAwait(false);
        }

        return snapshotPaths.Count > 0;
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
            result[pair.Key] = new TradeCheckpointSlice(originalTradesToPersist.Length, tradesToPersist);
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
        string scratchPath,
        IReadOnlyList<TradeInfo> tradesToPersist,
        CancellationToken cancellationToken)
    {
        var result = InstrumentScratchState.Empty;
        var checkpointState = await GetScratchStateAsync(exchange, instrument, scratchPath, cancellationToken).ConfigureAwait(false);
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
                    if (File.Exists(scratchPath))
                    {
                        var recoveredFinalizedPath = await FinalizeScratchAsync(exchange, instrument, currentScratchUtcDate, scratchPath, cancellationToken).ConfigureAwait(false);
                        await DispatchFinalizedFileAsync(exchange, instrument, currentScratchUtcDate, recoveredFinalizedPath, cancellationToken).ConfigureAwait(false);
                    }

                    currentScratchUtcDate = DateOnly.FromDateTime(remainingTrades[0].Timestamp.UtcDateTime);
                    continue;
                }

                await AppendTradesToScratchAsync(instrument, scratchPath, finalizedTrades, cancellationToken).ConfigureAwait(false);
                lastRecordedTrade = finalizedTrades[^1];
                var finalizedPath = await FinalizeScratchAsync(exchange, instrument, currentScratchUtcDate, scratchPath, cancellationToken).ConfigureAwait(false);
                await DispatchFinalizedFileAsync(exchange, instrument, currentScratchUtcDate, finalizedPath, cancellationToken).ConfigureAwait(false);

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

    /// <summary>
    /// Loads the in-memory or persisted scratch checkpoint baseline for one instrument.
    /// </summary>
    private async Task<InstrumentScratchState> GetScratchStateAsync(
        ExchangeId exchange,
        Instrument instrument,
        string scratchPath,
        CancellationToken cancellationToken)
    {
        InstrumentScratchState? result = null;

        lock (_gate)
        {
            _scratchStatesByInstrument.TryGetValue((exchange, instrument), out result);
        }

        if (result is not null)
        {
            return result;
        }

        if (!File.Exists(scratchPath))
        {
            return InstrumentScratchState.Empty;
        }

        log.Info($"Trade scratch checkpoint recovery: instrument={instrument}, path={scratchPath}.");
        var metadata = await TradeJsonlFile.TryReadScratchCheckpointMetadata(scratchPath, cancellationToken).ConfigureAwait(false);
        if (metadata is null)
        {
            log.Warn($"Trade scratch checkpoint recovery found an empty scratch file: instrument={instrument}, path={scratchPath}.");
            return InstrumentScratchState.Empty;
        }

        result = new InstrumentScratchState(metadata.LastRecordedTrade, metadata.ActiveScratchUtcDate);
        log.Debug($"Trade scratch checkpoint recovered: instrument={instrument}, activeScratchUtcDate={metadata.ActiveScratchUtcDate:yyyy-MM-dd}, lastTradeId={metadata.LastRecordedTrade.Key.TradeId}, lastTradeTimestampUtc={metadata.LastRecordedTrade.Timestamp:O}.");

        lock (_gate)
        {
            _scratchStatesByInstrument[(exchange, instrument)] = result;
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
        await TradeJsonlFile.AppendTrades(scratchPath, trades, cancellationToken).ConfigureAwait(false);
    }

    private async Task<string> FinalizeScratchAsync(
        ExchangeId exchange,
        Instrument instrument,
        DateOnly utcDate,
        string scratchPath,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var finalizedPath = TradeLocalDailyFilePath.Build(localRootDirectory, exchange, instrument, utcDate);
        var finalizedDirectory = Path.GetDirectoryName(finalizedPath);

        if (!string.IsNullOrWhiteSpace(finalizedDirectory))
        {
            Directory.CreateDirectory(finalizedDirectory);
        }

        if (File.Exists(finalizedPath))
        {
            throw new InvalidOperationException($"The finalized day file already exists: {finalizedPath}.");
        }

        log.Info($"Trade scratch checkpoint finalize: exchange={exchange}, instrument={instrument}, utcDate={utcDate:yyyy-MM-dd}, from={scratchPath}, to={finalizedPath}.");
        File.Move(scratchPath, finalizedPath);

        var result = finalizedPath;
        return result;
    }

    private async Task DispatchFinalizedFileAsync(
        ExchangeId exchange,
        Instrument instrument,
        DateOnly utcDate,
        string finalizedPath,
        CancellationToken cancellationToken)
    {
        await tradeFinalizedFileDispatcher.DispatchAsync(exchange, instrument, utcDate, finalizedPath, cancellationToken).ConfigureAwait(false);
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
        IReadOnlyList<TradeInfo> tradesToPersist)
    {
        /// <summary>
        /// Gets the number of trades that were persisted from the front of the pending in-memory sequence.
        /// </summary>
        public int PersistedTradeCount { get; } = persistedTradeCount;

        /// <summary>
        /// Gets the trades that should be appended to the instrument scratch file.
        /// </summary>
        public IReadOnlyList<TradeInfo> TradesToPersist { get; } = tradesToPersist;
    }

    /// <summary>
    /// Describes one persisted active scratch file that can be exported as a point-in-time snapshot.
    /// </summary>
    private sealed class ActiveScratchSnapshotContext(ExchangeId exchange, Instrument instrument, string scratchPath)
    {
        public ExchangeId Exchange { get; } = exchange;

        public Instrument Instrument { get; } = instrument;

        public string ScratchPath { get; } = scratchPath;
    }
}
