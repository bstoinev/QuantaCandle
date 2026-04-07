namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Coordinates shared checkpoint requests across the recorder runtime.
/// </summary>
public interface ICheckpointSignal
{
    /// <summary>
    /// Gets the current checkpoint signal version observed by listeners.
    /// </summary>
    long CurrentVersion { get; }

    /// <summary>
    /// Signals that all listeners should trigger a checkpoint as soon as possible.
    /// </summary>
    void Signal();

    /// <summary>
    /// Waits until a newer checkpoint signal than <paramref name="observedVersion"/> is published.
    /// </summary>
    ValueTask<long> WaitForNextSignalAsync(long observedVersion, CancellationToken cancellationToken);
}
