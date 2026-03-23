using Microsoft.Extensions.Logging;

using QuantaCandle.Core.Logging;

namespace QuantaCandle.Infra.Logging;

public class NLogLogMachinaFactory(ILoggerFactory loggerFactory) : ILogMachinaFactory
{
    private readonly ILoggerFactory _loggerFactory = loggerFactory;

    public ILogMachina<T> Create<T>() where T : class => new NLogLogMachina<T>();
}
