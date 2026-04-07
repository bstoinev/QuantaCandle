using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Owns recorder-side checkpoint persistence independent of the destination trade sink.
/// </summary>
public interface ITradeCheckpointLifecycle
{
    /// <summary>
    /// Tracks trades that were durably appended to the destination sink and are eligible for checkpointing.
    /// </summary>
    ValueTask TrackAppendedTrades(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken);

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
