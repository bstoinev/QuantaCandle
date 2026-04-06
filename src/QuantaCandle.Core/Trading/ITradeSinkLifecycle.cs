namespace QuantaCandle.Core.Trading;

/// <summary>
/// Describes optional lifecycle hooks for trade sinks that manage active in-memory state.
/// </summary>
public interface ITradeSinkLifecycle
{
    /// <summary>
    /// Persists any due active-day checkpoints and finalizes completed UTC days when appropriate.
    /// Returns <see langword="true"/> only when a checkpoint completed successfully.
    /// </summary>
    ValueTask<bool> CheckpointActive(CancellationToken cancellationToken);

    /// <summary>
    /// Flushes active in-memory state to durable local storage during graceful shutdown.
    /// </summary>
    ValueTask FlushOnShutdown(CancellationToken cancellationToken);
}
