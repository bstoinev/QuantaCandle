namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Publishes checkpoint requests to all active listeners using a monotonically increasing version.
/// </summary>
public sealed class CheckpointSignal : ICheckpointSignal
{
    private readonly Lock _gate = new();
    private TaskCompletionSource<long> _nextSignal = CreateSignalSource();
    private long _currentVersion;

    /// <summary>
    /// Gets the latest published checkpoint signal version.
    /// </summary>
    public long CurrentVersion => Interlocked.Read(ref _currentVersion);

    /// <summary>
    /// Signals all current listeners that a checkpoint should run.
    /// </summary>
    public void Signal()
    {
        TaskCompletionSource<long> nextSignal;
        long currentVersion;

        lock (_gate)
        {
            currentVersion = Interlocked.Increment(ref _currentVersion);
            nextSignal = _nextSignal;
            _nextSignal = CreateSignalSource();
        }

        nextSignal.TrySetResult(currentVersion);
    }

    /// <summary>
    /// Waits until a newer checkpoint signal is published.
    /// </summary>
    public ValueTask<long> WaitForNextSignalAsync(long observedVersion, CancellationToken cancellationToken)
    {
        Task<long> result;

        lock (_gate)
        {
            if (_currentVersion == observedVersion)
            {
                result = _nextSignal.Task.WaitAsync(cancellationToken);
            }
            else
            {
                result = Task.FromResult(_currentVersion);
            }
        }

        return new ValueTask<long>(result);
    }

    private static TaskCompletionSource<long> CreateSignalSource()
    {
        var result = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        return result;
    }
}
