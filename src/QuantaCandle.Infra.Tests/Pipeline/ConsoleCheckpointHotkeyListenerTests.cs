using QuantaCandle.Infra.Pipeline;

namespace QuantaCandle.Infra.Tests.Pipeline;

/// <summary>
/// Verifies that the console hotkey listener translates recorder hotkeys into manual checkpoint requests.
/// </summary>
public sealed class ConsoleCheckpointHotkeyListenerTests
{
    [Fact]
    public async Task CtrlSRequestsManualCheckpoint()
    {
        var checkpointSignal = new CheckpointSignal();
        var observedVersion = checkpointSignal.CurrentVersion;
        var consoleKeyReader = new StubConsoleKeyReader(
            isSupported: true,
            [new ConsoleKeyInfo('s', ConsoleKey.S, shift: false, alt: false, control: true)]);
        var listener = new ConsoleCheckpointHotkeyListener(consoleKeyReader, checkpointSignal);
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(250));

        var listenerTask = listener.Run(cancellationTokenSource.Token);
        var notification = await checkpointSignal.WaitForNextSignalAsync(observedVersion, cancellationTokenSource.Token);

        cancellationTokenSource.Cancel();
        await listenerTask;

        Assert.True(notification.Version > observedVersion);
        Assert.Equal(CheckpointRequestKind.Checkpoint, notification.RequestKind);
    }

    [Fact]
    public async Task CtrlPRequestsSnapshotCheckpoint()
    {
        var checkpointSignal = new CheckpointSignal();
        var observedVersion = checkpointSignal.CurrentVersion;
        var consoleKeyReader = new StubConsoleKeyReader(
            isSupported: true,
            [new ConsoleKeyInfo('p', ConsoleKey.P, shift: false, alt: false, control: true)]);
        var listener = new ConsoleCheckpointHotkeyListener(consoleKeyReader, checkpointSignal);
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(250));

        var listenerTask = listener.Run(cancellationTokenSource.Token);
        var notification = await checkpointSignal.WaitForNextSignalAsync(observedVersion, cancellationTokenSource.Token);

        cancellationTokenSource.Cancel();
        await listenerTask;

        Assert.True(notification.Version > observedVersion);
        Assert.Equal(CheckpointRequestKind.Snapshot, notification.RequestKind);
    }

    private sealed class StubConsoleKeyReader(bool isSupported, IReadOnlyList<ConsoleKeyInfo> keys) : IConsoleKeyReader
    {
        private readonly Queue<ConsoleKeyInfo> _keys = new(keys);

        public bool IsSupported { get; } = isSupported;

        public bool KeyAvailable => _keys.Count > 0;

        public ConsoleKeyInfo ReadKey(bool intercept)
        {
            var result = _keys.Dequeue();
            return result;
        }
    }
}
