using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Owns recorder-side checkpoint persistence independent of the destination trade sink.
/// </summary>
public interface ITradeCheckpointLifecycle
{
    /// <summary>
    /// Tracks trades that were accepted by the ingest pipeline and are eligible for checkpointing.
    /// Returns the current total count of trades retained in the recorder-owned in-memory checkpoint cache.
    /// </summary>
    ValueTask<int> TrackAppendedTrades(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken);

    /// <summary>
    /// Persists the current checkpoint state.
    /// Returns <see langword="true"/> when the checkpoint completed successfully.
    /// </summary>
    ValueTask<bool> CheckpointActive(CancellationToken cancellationToken);

    /// <summary>
    /// Persists the current checkpoint state during graceful shutdown.
    /// </summary>
    ValueTask FlushOnShutdown(CancellationToken cancellationToken);
}
