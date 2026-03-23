using NLog;
using QuantaCandle.Core.Logging;

using LogLevel = NLog.LogLevel;

namespace QuantaCandle.Infra.Logging;

public class NLogLogMachina<TSource> : ILogMachina<TSource>
    where TSource : class
{
    private readonly Logger Logger;
    private readonly Type LoggerType = typeof(TSource);
    private readonly Type WrapperType = typeof(NLogLogMachina<TSource>);

    public NLogLogMachina()
    {
        ArgumentNullException.ThrowIfNull(LoggerType?.FullName, nameof(LoggerType));
        Logger = LogManager.GetLogger(LoggerType.FullName);
    }

    public void Debug(string message)
    {
        var info = new LogEventInfo(LogLevel.Debug, LoggerType.Name, message);
        Logger.Log(WrapperType, info);
    }

    public void Error(Exception ex)
    {
        var info = new LogEventInfo(LogLevel.Error, LoggerType.Name, ex.ToString());
        Logger.Log(WrapperType, info);
    }

    public void Error(string message, Exception ex)
    {
        var info = new LogEventInfo(LogLevel.Error, LoggerType.Name, $"{message}: {ex}");
        Logger.Log(WrapperType, info);
    }

    public void Error(string message)
    {
        var info = new LogEventInfo(LogLevel.Error, LoggerType.Name, message);
        Logger.Log(WrapperType, info);
    }

    public void Info(string message)
    {
        var info = new LogEventInfo(LogLevel.Info, LoggerType.Name, message);
        Logger.Log(WrapperType, info);
    }

    public void Trace(string message)
    {
        var info = new LogEventInfo(LogLevel.Trace, LoggerType.Name, message);
        Logger.Log(WrapperType, info);
    }

    public void Warn(string message)
    {
        var info = new LogEventInfo(LogLevel.Warn, LoggerType.Name, message);
        Logger.Log(WrapperType, info);
    }
}
