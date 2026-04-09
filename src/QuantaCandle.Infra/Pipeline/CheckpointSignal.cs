namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Publishes checkpoint requests to all active listeners using a monotonically increasing version.
/// </summary>
public sealed class CheckpointSignal : ICheckpointSignal
{
    private readonly Lock _gate = new();
    private TaskCompletionSource<CheckpointSignalNotification> _nextSignal = CreateSignalSource();
    private long _currentVersion;
    private CheckpointRequestKind _currentRequestKind = CheckpointRequestKind.Checkpoint;

    /// <summary>
    /// Gets the latest published checkpoint signal version.
    /// </summary>
    public long CurrentVersion => Interlocked.Read(ref _currentVersion);

    /// <summary>
    /// Signals all current listeners that a checkpoint should run.
    /// </summary>
    public void Signal(CheckpointRequestKind requestKind = CheckpointRequestKind.Checkpoint)
    {
        TaskCompletionSource<CheckpointSignalNotification> nextSignal;
        CheckpointSignalNotification notification;

        lock (_gate)
        {
            var currentVersion = Interlocked.Increment(ref _currentVersion);
            _currentRequestKind = requestKind;
            notification = new CheckpointSignalNotification(currentVersion, requestKind);
            nextSignal = _nextSignal;
            _nextSignal = CreateSignalSource();
        }

        nextSignal.TrySetResult(notification);
    }

    /// <summary>
    /// Waits until a newer checkpoint signal is published.
    /// </summary>
    public ValueTask<CheckpointSignalNotification> WaitForNextSignalAsync(long observedVersion, CancellationToken cancellationToken)
    {
        Task<CheckpointSignalNotification> result;

        lock (_gate)
        {
            if (_currentVersion == observedVersion)
            {
                result = _nextSignal.Task.WaitAsync(cancellationToken);
            }
            else
            {
                result = Task.FromResult(new CheckpointSignalNotification(_currentVersion, _currentRequestKind));
            }
        }

        return new ValueTask<CheckpointSignalNotification>(result);
    }

    private static TaskCompletionSource<CheckpointSignalNotification> CreateSignalSource()
    {
        var result = new TaskCompletionSource<CheckpointSignalNotification>(TaskCreationOptions.RunContinuationsAsynchronously);
        return result;
    }
}
