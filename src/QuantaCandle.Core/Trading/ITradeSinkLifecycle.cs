namespace QuantaCandle.Core.Trading;

/// <summary>
/// Describes optional lifecycle hooks for trade sinks that manage active in-memory state.
/// </summary>
public interface ITradeSinkLifecycle
{
    /// <summary>
    /// Persists any due active-day checkpoints and finalizes completed UTC days when appropriate.
    /// </summary>
    ValueTask CheckpointActive(CancellationToken cancellationToken);

    /// <summary>
    /// Flushes active in-memory state to durable local storage during graceful shutdown.
    /// </summary>
    ValueTask FlushOnShutdown(CancellationToken cancellationToken);
}
