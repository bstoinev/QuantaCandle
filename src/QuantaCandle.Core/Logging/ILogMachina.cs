namespace QuantaCandle.Core.Logging;

/// <summary>
/// Provides mechanisms to emit diagnostics messages at various levels (Debug, Error, Info, Trace, Warn) and specifies the type of the emitting component.
/// </summary>
/// <typeparam name="TSource">The type, emitting the messages.</typeparam>
public interface ILogMachina<TSource> : ILogMachina
    where TSource : class
{
}
