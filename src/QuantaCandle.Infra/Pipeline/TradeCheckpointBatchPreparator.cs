using System.Globalization;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Performs the minimal truthful checkpoint-time batch healing that the current architecture supports.
/// </summary>
public sealed class TradeCheckpointBatchPreparator : ITradeCheckpointBatchPreparator
{
    /// <summary>
    /// Normalizes, de-duplicates, merges, and scans the supplied checkpointable batch without inventing missing trades.
    /// </summary>
    public ValueTask<TradeCheckpointBatchPreparation> Prepare(
        ExchangeId exchange,
        Instrument symbol,
        IReadOnlyList<TradeInfo> persistedTrades,
        IReadOnlyList<TradeInfo> checkpointableTrades,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var normalizedPersistedTrades = NormalizeTrades(persistedTrades);
        var persistedTradeKeys = normalizedPersistedTrades
            .Select(trade => trade.Key)
            .ToHashSet();
        var normalizedCheckpointableTrades = NormalizeTrades(checkpointableTrades);
        var newCheckpointableTrades = normalizedCheckpointableTrades
            .Where(trade => !persistedTradeKeys.Contains(trade.Key))
            .ToArray();
        var checkpointableTradeKeys = newCheckpointableTrades
            .Select(trade => trade.Key)
            .ToHashSet();
        var preparedTrades = NormalizeTrades(normalizedPersistedTrades.Concat(newCheckpointableTrades));
        var detectedGaps = DetectGaps(exchange, symbol, preparedTrades, checkpointableTradeKeys);

        var result = new TradeCheckpointBatchPreparation(preparedTrades, detectedGaps);
        return ValueTask.FromResult(result);
    }

    /// <summary>
    /// Orders trades deterministically and keeps only one instance of each trade key.
    /// </summary>
    private static TradeInfo[] NormalizeTrades(IEnumerable<TradeInfo> trades)
    {
        var orderedTrades = trades
            .OrderBy(trade => trade.Timestamp)
            .ThenBy(trade => trade.Key.TradeId, StringComparer.Ordinal);
        var seenTradeKeys = new HashSet<TradeKey>();
        var result = new List<TradeInfo>();

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
    /// Detects boundary and internal gaps that remain visible after merging the new checkpoint batch into persisted state.
    /// </summary>
    private static TradeGap[] DetectGaps(
        ExchangeId exchange,
        Instrument symbol,
        IReadOnlyList<TradeInfo> preparedTrades,
        IReadOnlySet<TradeKey> checkpointableTradeKeys)
    {
        var result = new List<TradeGap>();

        for (var i = 1; i < preparedTrades.Count; i++)
        {
            var previousTrade = preparedTrades[i - 1];
            var currentTrade = preparedTrades[i];
            var pairTouchesCheckpointableTrade = checkpointableTradeKeys.Contains(previousTrade.Key)
                || checkpointableTradeKeys.Contains(currentTrade.Key);

            if (!pairTouchesCheckpointableTrade
                || !TryGetTradeId(previousTrade.Key.TradeId, out var previousTradeId)
                || !TryGetTradeId(currentTrade.Key.TradeId, out var currentTradeId)
                || currentTradeId <= previousTradeId + 1)
            {
                continue;
            }

            var gapId = Guid.NewGuid();
            var fromExclusive = new TradeWatermark(previousTrade.Key.TradeId, previousTrade.Timestamp);
            var toInclusive = new TradeWatermark(currentTrade.Key.TradeId, currentTrade.Timestamp);
            var openGap = TradeGap.CreateOpen(gapId, exchange, symbol, fromExclusive, currentTrade.Timestamp);
            var missingTradeIds = new MissingTradeIdRange(previousTradeId + 1, currentTradeId - 1);

            result.Add(openGap);
            result.Add(openGap.ToBounded(toInclusive, missingTradeIds));
        }

        return result.ToArray();
    }

    /// <summary>
    /// Parses an exchange trade id into a numeric sequence value when possible.
    /// </summary>
    private static bool TryGetTradeId(string tradeIdText, out long tradeId)
    {
        return long.TryParse(tradeIdText, NumberStyles.None, CultureInfo.InvariantCulture, out tradeId);
    }
}
