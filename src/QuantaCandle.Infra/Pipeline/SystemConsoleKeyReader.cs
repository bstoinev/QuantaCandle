namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Reads interactive console key input from <see cref="Console"/>.
/// </summary>
public sealed class SystemConsoleKeyReader : IConsoleKeyReader
{
    /// <summary>
    /// Gets whether interactive console key reading is supported in the current environment.
    /// </summary>
    public bool IsSupported => !Console.IsInputRedirected;

    /// <summary>
    /// Gets whether a key press is waiting to be read.
    /// </summary>
    public bool KeyAvailable => Console.KeyAvailable;

    /// <summary>
    /// Reads the next available key press.
    /// </summary>
    public ConsoleKeyInfo ReadKey(bool intercept) => Console.ReadKey(intercept);
}
