using QuantaCandle.Core.Logging;
using Microsoft.Extensions.Logging;

namespace QuantaCandle.Service.Logging;

public sealed class HostLogMachinaFactory : ILogMachinaFactory
{
    private readonly ILoggerFactory loggerFactory;

    public HostLogMachinaFactory(ILoggerFactory loggerFactory)
    {
        this.loggerFactory = loggerFactory;
    }

    public ILogMachina<T> Create<T>() where T : class
    {
        return new HostLogMachina<T>(loggerFactory);
    }
}

