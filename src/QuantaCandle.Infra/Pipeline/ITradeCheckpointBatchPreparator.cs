using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Normalizes one checkpointable trade batch against the already persisted local tail before checkpoint persistence.
/// </summary>
public interface ITradeCheckpointBatchPreparator
{
    /// <summary>
    /// Normalizes, de-duplicates, merges, and scans the supplied checkpointable batch without inventing missing trades.
    /// </summary>
    ValueTask<TradeCheckpointBatchPreparation> Prepare(
        ExchangeId exchange,
        Instrument symbol,
        IReadOnlyList<TradeInfo> persistedTrades,
        IReadOnlyList<TradeInfo> checkpointableTrades,
        CancellationToken cancellationToken);
}
