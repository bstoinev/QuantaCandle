using System.Globalization;

using LogMachina;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Storage;

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
    ITradeGapScanner? tradeGapScanner,
    ITradeGapHealer? tradeGapHealer,
    ITradeDayBoundaryResolver? tradeDayBoundaryResolver,
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
        var partialFinalizedPath = TradeLocalDailyFilePath.BuildPartial(localRootDirectory, exchange, instrument, utcDate);
        var finalizedDirectory = Path.GetDirectoryName(finalizedPath);

        if (!string.IsNullOrWhiteSpace(finalizedDirectory))
        {
            Directory.CreateDirectory(finalizedDirectory);
        }

        if (File.Exists(finalizedPath) || File.Exists(partialFinalizedPath))
        {
            throw new InvalidOperationException($"The finalized day file already exists for UTC day {utcDate:yyyy-MM-dd}: {finalizedPath} or {partialFinalizedPath}.");
        }

        log.Info($"Trade scratch checkpoint finalize: exchange={exchange}, instrument={instrument}, utcDate={utcDate:yyyy-MM-dd}, from={scratchPath}, to={finalizedPath}.");
        File.Move(scratchPath, finalizedPath);

        var result = await TryHealRolloverFinalizedDayAsync(exchange, instrument, utcDate, finalizedPath, cancellationToken).ConfigureAwait(false);
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

    /// <summary>
    /// Runs one deterministic rollover healing pass for the finalized UTC day and downgrades the file name to partial when gaps remain.
    /// </summary>
    private async Task<string> TryHealRolloverFinalizedDayAsync(
        ExchangeId exchange,
        Instrument instrument,
        DateOnly utcDate,
        string finalizedPath,
        CancellationToken cancellationToken)
    {
        var result = finalizedPath;

        if (tradeGapScanner is null || tradeGapHealer is null)
        {
            return result;
        }

        try
        {
            var repairPlans = await BuildRolloverRepairPlansAsync(exchange, instrument, utcDate, finalizedPath, cancellationToken).ConfigureAwait(false);
            if (repairPlans.Count > 0)
            {
                log.Info($"Trade rollover healing start: exchange={exchange}, instrument={instrument}, utcDate={utcDate:yyyy-MM-dd}, repairPlanCount={repairPlans.Count}, path={finalizedPath}.");

                foreach (var repairPlan in repairPlans)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await HealPlannedGapAsync(exchange, instrument, finalizedPath, repairPlan, cancellationToken).ConfigureAwait(false);
                }
            }

            var remainingRepairPlans = await BuildRolloverRepairPlansAsync(exchange, instrument, utcDate, finalizedPath, cancellationToken).ConfigureAwait(false);
            if (remainingRepairPlans.Count > 0)
            {
                var partialFinalizedPath = TradeLocalDailyFilePath.BuildPartial(localRootDirectory, exchange, instrument, utcDate);
                var remainingGapText = string.Join(", ", remainingRepairPlans.Select(static plan => $"{plan.Gap.MissingTradeIds?.FirstTradeId}-{plan.Gap.MissingTradeIds?.LastTradeId}"));

                log.Warn($"Trade rollover healing incomplete: exchange={exchange}, instrument={instrument}, utcDate={utcDate:yyyy-MM-dd}, remainingGapCount={remainingRepairPlans.Count}, remainingGaps=[{remainingGapText}], finalPath={partialFinalizedPath}.");
                File.Move(finalizedPath, partialFinalizedPath);
                result = partialFinalizedPath;
            }
        }
        catch (Exception ex)
        {
            var partialFinalizedPath = TradeLocalDailyFilePath.BuildPartial(localRootDirectory, exchange, instrument, utcDate);

            log.Warn($"Trade rollover healing failed: exchange={exchange}, instrument={instrument}, utcDate={utcDate:yyyy-MM-dd}. Finalizing partial day file.");
            log.Error(ex);

            if (File.Exists(finalizedPath) && !File.Exists(partialFinalizedPath))
            {
                File.Move(finalizedPath, partialFinalizedPath);
            }

            result = File.Exists(partialFinalizedPath)
                ? partialFinalizedPath
                : finalizedPath;
        }

        return result;
    }

    /// <summary>
    /// Builds the one-pass rollover repair plan by combining interior local gaps with BestEffort UTC day boundary gaps.
    /// </summary>
    private async Task<IReadOnlyList<TradeGapRepairPlan>> BuildRolloverRepairPlansAsync(
        ExchangeId exchange,
        Instrument instrument,
        DateOnly utcDate,
        string finalizedPath,
        CancellationToken cancellationToken)
    {
        var candidateFile = CreateCandidateFile(finalizedPath, utcDate);
        var scanRequest = new TradeGapScanRequest(localRootDirectory, [candidateFile], []);
        var scanResult = await tradeGapScanner!
            .Scan(scanRequest, cancellationToken)
            .ConfigureAwait(false);
        var result = CreateRepairPlans(scanResult);

        var boundaryTradePair = await TryReadBoundaryTradePairAsync(finalizedPath, cancellationToken).ConfigureAwait(false);
        if (boundaryTradePair is not null && tradeDayBoundaryResolver is not null)
        {
            try
            {
                var boundary = await tradeDayBoundaryResolver
                    .Resolve(
                        exchange,
                        instrument,
                        utcDate,
                        TradeDayBoundaryResolutionMode.BestEffort,
                        cancellationToken)
                    .ConfigureAwait(false);

                if (!string.IsNullOrWhiteSpace(boundary.Warning))
                {
                    log.Warn($"Trade rollover boundary verification inconsistency: exchange={exchange}, instrument={instrument}, utcDate={utcDate:yyyy-MM-dd}. {boundary.Warning}");
                }

                var startBoundaryGap = TradeDayBoundaryGapPlanner.CreateStartBoundaryGap(
                    boundary,
                    boundaryTradePair.Value.FirstTradeId,
                    boundaryTradePair.Value.FirstNumericTradeId,
                    boundaryTradePair.Value.FirstTimestamp,
                    candidateFile.Path,
                    boundaryTradePair.Value.FirstLineNumber,
                    boundaryTradePair.Value.FirstTimestamp);
                if (startBoundaryGap is not null)
                {
                    result.Add(new TradeGapRepairPlan(startBoundaryGap.Value.Gap, startBoundaryGap.Value.AffectedRange));
                }

                var endBoundaryGap = TradeDayBoundaryGapPlanner.CreateEndBoundaryGap(
                    boundary,
                    boundaryTradePair.Value.LastTradeId,
                    boundaryTradePair.Value.LastNumericTradeId,
                    boundaryTradePair.Value.LastTimestamp,
                    candidateFile.Path,
                    boundaryTradePair.Value.LastLineNumber,
                    boundaryTradePair.Value.LastTimestamp);
                if (endBoundaryGap is not null)
                {
                    result.Add(new TradeGapRepairPlan(endBoundaryGap.Value.Gap, endBoundaryGap.Value.AffectedRange));
                }
            }
            catch (Exception ex)
            {
                log.Warn($"Trade rollover boundary resolution failed: exchange={exchange}, instrument={instrument}, utcDate={utcDate:yyyy-MM-dd}. Continuing without boundary repair planning.");
                log.Error(ex);
            }
        }

        result = result
            .OrderBy(static plan => plan.Gap.MissingTradeIds?.FirstTradeId ?? long.MaxValue)
            .ThenBy(static plan => plan.Gap.MissingTradeIds?.LastTradeId ?? long.MaxValue)
            .ToList();
        return result;
    }

    /// <summary>
    /// Heals one planned rollover gap using the existing local fetch and splice flow.
    /// </summary>
    private async Task HealPlannedGapAsync(
        ExchangeId exchange,
        Instrument instrument,
        string finalizedPath,
        TradeGapRepairPlan repairPlan,
        CancellationToken cancellationToken)
    {
        if (repairPlan.Gap.MissingTradeIds is null)
        {
            return;
        }

        var missingTradeIds = repairPlan.Gap.MissingTradeIds.Value;
        var candidateFile = CreateCandidateFile(finalizedPath, DateOnly.FromDateTime(repairPlan.Gap.FromExclusive.Timestamp.UtcDateTime));

        try
        {
            var request = new TradeGapHealRequest(
                localRootDirectory,
                exchange,
                instrument,
                missingTradeIds.FirstTradeId,
                missingTradeIds.LastTradeId,
                [candidateFile],
                repairPlan.AffectedRange,
                [missingTradeIds]);
            var healResult = await tradeGapHealer!
                .Heal(request, cancellationToken)
                .ConfigureAwait(false);

            log.Info($"Trade rollover healing attempt completed: exchange={exchange}, instrument={instrument}, path={finalizedPath}, requestedRange={missingTradeIds.FirstTradeId}-{missingTradeIds.LastTradeId}, outcome={healResult.Outcome}, fullCoverage={healResult.HasFullRequestedCoverage}.");
        }
        catch (Exception ex)
        {
            log.Warn($"Trade rollover healing attempt failed: exchange={exchange}, instrument={instrument}, path={finalizedPath}, requestedRange={missingTradeIds.FirstTradeId}-{missingTradeIds.LastTradeId}.");
            log.Error(ex);
        }
    }

    /// <summary>
    /// Reads the local first and last trade identifiers for the finalized UTC day file.
    /// </summary>
    private static async Task<BoundaryTradePair?> TryReadBoundaryTradePairAsync(string finalizedPath, CancellationToken cancellationToken)
    {
        BoundaryTrade? firstTrade = null;
        BoundaryTrade? lastTrade = null;
        var lineNumber = 0;

        await foreach (var line in File.ReadLinesAsync(finalizedPath, cancellationToken).ConfigureAwait(false))
        {
            lineNumber++;
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var trade = LocalTradeJsonLineParser.ParseTrade(line, finalizedPath, lineNumber);
            var hasTradeId = TryGetTradeId(trade.Key.TradeId, out var numericTradeId);
            if (!hasTradeId)
            {
                throw new InvalidOperationException($"TradeId '{trade.Key.TradeId}' in '{finalizedPath}' must be numeric for rollover healing.");
            }

            var boundaryTrade = new BoundaryTrade(
                trade.Key.TradeId,
                numericTradeId,
                trade.Timestamp.ToUniversalTime(),
                lineNumber);
            if (firstTrade is null)
            {
                firstTrade = boundaryTrade;
            }

            lastTrade = boundaryTrade;
        }

        BoundaryTradePair? result = firstTrade is not null && lastTrade is not null
            ? new BoundaryTradePair(firstTrade.Value, lastTrade.Value)
            : null;
        return result;
    }

    /// <summary>
    /// Converts a scan result into gap repair plans aligned by gap index and affected range index.
    /// </summary>
    private static List<TradeGapRepairPlan> CreateRepairPlans(TradeGapScanResult scanResult)
    {
        var result = new List<TradeGapRepairPlan>(scanResult.DetectedGaps.Count);

        for (var i = 0; i < scanResult.DetectedGaps.Count; i++)
        {
            var affectedRange = i < scanResult.AffectedRanges.Count
                ? scanResult.AffectedRanges[i]
                : null;
            result.Add(new TradeGapRepairPlan(scanResult.DetectedGaps[i], affectedRange));
        }

        return result;
    }

    private TradeGapAffectedFile CreateCandidateFile(string finalizedPath, DateOnly utcDate)
    {
        var relativePath = Path.GetRelativePath(localRootDirectory, finalizedPath);
        var result = new TradeGapAffectedFile(relativePath, utcDate);
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

    /// <summary>
    /// Describes one local first or last trade used during rollover boundary planning.
    /// </summary>
    private readonly record struct BoundaryTrade(string TradeId, long NumericTradeId, DateTimeOffset Timestamp, int LineNumber);

    /// <summary>
    /// Describes the local first and last trade ids for one finalized UTC day file.
    /// </summary>
    private readonly record struct BoundaryTradePair(BoundaryTrade First, BoundaryTrade Last)
    {
        public string FirstTradeId => First.TradeId;

        public long FirstNumericTradeId => First.NumericTradeId;

        public DateTimeOffset FirstTimestamp => First.Timestamp;

        public int FirstLineNumber => First.LineNumber;

        public string LastTradeId => Last.TradeId;

        public long LastNumericTradeId => Last.NumericTradeId;

        public DateTimeOffset LastTimestamp => Last.Timestamp;

        public int LastLineNumber => Last.LineNumber;
    }

    /// <summary>
    /// Describes one bounded rollover repair request.
    /// </summary>
    private sealed class TradeGapRepairPlan(TradeGap gap, TradeGapAffectedRange? affectedRange)
    {
        public TradeGapAffectedRange? AffectedRange { get; } = affectedRange;

        public TradeGap Gap { get; } = gap;
    }
}
