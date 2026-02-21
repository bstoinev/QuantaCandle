using Microsoft.Extensions.Logging;

namespace QuantaCandle.Infra.Logging;

public class NLogLogMachinaFactory : ILogMachinaFactory
{
    private readonly ILoggerFactory _loggerFactory;

    public NLogLogMachinaFactory(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    public ILogMachina<T> Create<T>() where T : class
    {
        return new NLogLogMachina<T>(_loggerFactory);
    }
}
