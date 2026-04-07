namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Watches console key input and requests a manual checkpoint when Ctrl+S is pressed.
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
                    if (IsManualCheckpointHotkey(key))
                    {
                        checkpointSignal.Signal();
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
    /// Determines whether the supplied key press should trigger a manual checkpoint.
    /// </summary>
    public static bool IsManualCheckpointHotkey(ConsoleKeyInfo key) => key.Key == ConsoleKey.S && key.Modifiers.HasFlag(ConsoleModifiers.Control);
}
