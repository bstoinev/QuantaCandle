using LogMachina;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Persists recorder-owned scratch checkpoints while retaining the latest trade only in memory.
/// </summary>
public sealed class TradeScratchCheckpointLifecycle(
    string localRootDirectory,
    ILogMachina<TradeScratchCheckpointLifecycle> log) : ITradeCheckpointLifecycle
{
    private readonly Lock _gate = new();
    private readonly Dictionary<(ExchangeId Exchange, Instrument Symbol), List<TradeInfo>> _pendingTradesByInstrument = [];

    /// <summary>
    /// Tracks trades that were appended to the destination sink and should participate in scratch checkpoints.
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

            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRootDirectory, pair.Key.Symbol);
            var existingScratchTrades = await TradeJsonlFile.ReadTradesAsync(scratchPath, cancellationToken).ConfigureAwait(false);
            var combinedPersistedTrades = existingScratchTrades
                .Concat(pair.Value.TradesToPersist)
                .OrderBy(trade => trade.Timestamp)
                .ThenBy(trade => trade.Key.TradeId, StringComparer.Ordinal)
                .ToArray();

            await PersistScratchState(pair.Key.Symbol, scratchPath, combinedPersistedTrades, cancellationToken).ConfigureAwait(false);
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
            var tradesToPersist = pendingTrades.Count > 1
                ? pendingTrades.Take(pendingTrades.Count - 1).ToArray()
                : [];

            result[pair.Key] = new TradeCheckpointSlice(tradesToPersist);
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

    private async Task PersistScratchState(
        Instrument instrument,
        string scratchPath,
        IReadOnlyList<TradeInfo> combinedPersistedTrades,
        CancellationToken cancellationToken)
    {
        if (combinedPersistedTrades.Count == 0)
        {
            await TradeJsonlFile.RewritePayloadAsync(scratchPath, string.Empty, cancellationToken).ConfigureAwait(false);
            return;
        }

        var oldestUtcDate = DateOnly.FromDateTime(combinedPersistedTrades[0].Timestamp.UtcDateTime);
        var newestUtcDate = DateOnly.FromDateTime(combinedPersistedTrades[^1].Timestamp.UtcDateTime);

        if (oldestUtcDate == newestUtcDate)
        {
            var scratchPayload = TradeJsonlFile.BuildPayload(combinedPersistedTrades);
            log.Info($"Trade scratch checkpoint write: instrument={instrument}, path={scratchPath}, tradeCount={combinedPersistedTrades.Count}.");
            await TradeJsonlFile.RewritePayloadAsync(scratchPath, scratchPayload, cancellationToken).ConfigureAwait(false);
            return;
        }

        var finalizedTrades = combinedPersistedTrades
            .Where(trade => DateOnly.FromDateTime(trade.Timestamp.UtcDateTime) == oldestUtcDate)
            .ToArray();
        var continuedScratchTrades = combinedPersistedTrades
            .Where(trade => DateOnly.FromDateTime(trade.Timestamp.UtcDateTime) > oldestUtcDate)
            .ToArray();
        var finalizedPath = TradeLocalDailyFilePath.Build(localRootDirectory, instrument, oldestUtcDate);
        var finalizedPayload = TradeJsonlFile.BuildPayload(finalizedTrades);
        var scratchPayloadAfterSplit = TradeJsonlFile.BuildPayload(continuedScratchTrades);

        log.Info($"Trade scratch checkpoint finalize: instrument={instrument}, utcDate={oldestUtcDate:yyyy-MM-dd}, path={finalizedPath}, tradeCount={finalizedTrades.Length}.");
        await TradeJsonlFile.RewritePayloadAsync(finalizedPath, finalizedPayload, cancellationToken).ConfigureAwait(false);
        log.Info($"Trade scratch checkpoint rollover: instrument={instrument}, path={scratchPath}, tradeCount={continuedScratchTrades.Length}.");
        await TradeJsonlFile.RewritePayloadAsync(scratchPath, scratchPayloadAfterSplit, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Describes the trades that should be appended to a scratch checkpoint file for one instrument.
    /// </summary>
    private sealed class TradeCheckpointSlice(IReadOnlyList<TradeInfo> tradesToPersist)
    {
        /// <summary>
        /// Gets the trades that should be appended to the instrument scratch file.
        /// </summary>
        public IReadOnlyList<TradeInfo> TradesToPersist { get; } = tradesToPersist;

        /// <summary>
        /// Gets the number of trades that were persisted from the front of the pending in-memory sequence.
        /// </summary>
        public int PersistedTradeCount => TradesToPersist.Count;
    }
}
