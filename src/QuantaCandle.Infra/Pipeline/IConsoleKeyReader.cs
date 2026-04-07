namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Reads console key input for background hotkey processing.
/// </summary>
public interface IConsoleKeyReader
{
    /// <summary>
    /// Gets whether interactive console key reading is supported in the current environment.
    /// </summary>
    bool IsSupported { get; }

    /// <summary>
    /// Gets whether a key press is waiting to be read.
    /// </summary>
    bool KeyAvailable { get; }

    /// <summary>
    /// Reads the next available key press.
    /// </summary>
    ConsoleKeyInfo ReadKey(bool intercept);
}
