using Microsoft.Extensions.Logging;

namespace QuantaCandle.Core.Logging;

public abstract class LogMachinaBase<T> : ILogMachina<T> where T : class
{
    protected readonly ILoggerFactory _loggerFactory;

    protected LogMachinaBase(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    public abstract ILogger<T> GetLogger();
}
