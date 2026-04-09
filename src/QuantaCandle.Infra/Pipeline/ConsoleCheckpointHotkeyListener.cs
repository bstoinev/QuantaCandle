namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Watches console key input and requests manual checkpoint actions from recorder hotkeys.
/// </summary>
public sealed class ConsoleCheckpointHotkeyListener(
    IConsoleKeyReader consoleKeyReader,
    ICheckpointSignal checkpointSignal)
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Starts monitoring console key input until cancellation is requested.
    /// </summary>
    public async Task Run(CancellationToken cancellationToken)
    {
        if (!consoleKeyReader.IsSupported)
        {
            return;
        }

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (consoleKeyReader.KeyAvailable)
                {
                    var key = consoleKeyReader.ReadKey(intercept: true);
                    if (TryGetCheckpointRequestKind(key, out var requestKind))
                    {
                        checkpointSignal.Signal(requestKind);
                    }
                }
                else
                {
                    await Task.Delay(PollInterval, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (InvalidOperationException)
        {
        }
    }

    /// <summary>
    /// Determines whether the supplied key press should trigger a manual checkpoint action.
    /// </summary>
    public static bool TryGetCheckpointRequestKind(ConsoleKeyInfo key, out CheckpointRequestKind requestKind)
    {
        var isControlPressed = key.Modifiers.HasFlag(ConsoleModifiers.Control);
        var hasNoAdditionalModifiers = !key.Modifiers.HasFlag(ConsoleModifiers.Shift) && !key.Modifiers.HasFlag(ConsoleModifiers.Alt);
        var canHandle = isControlPressed && hasNoAdditionalModifiers && key.Key is ConsoleKey.S or ConsoleKey.P;

        requestKind = key.Key == ConsoleKey.P
            ? CheckpointRequestKind.Snapshot
            : CheckpointRequestKind.Checkpoint;

        var result = canHandle;
        return result;
    }
}
