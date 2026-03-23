namespace QuantaCandle.Core.Logging;

/// <summary>
/// Provides mechanisms to emit diagnostics messages at various levels (Debug, Error, Info, Trace, Warn).
/// </summary>
public interface ILogMachina
{
    void Debug(string message);

    void Error(string message);

    void Error(Exception ex);

    void Info(string message);

    void Trace(string message);

    void Warn(string message);
}
