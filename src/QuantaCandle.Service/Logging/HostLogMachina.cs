using Microsoft.Extensions.Logging;
using QuantaCandle.Core.Logging;

namespace QuantaCandle.Service.Logging;

public sealed class HostLogMachina<T> : LogMachinaBase<T> where T : class
{
    private readonly ILogger<T> wrapperType;

    public HostLogMachina(ILoggerFactory loggerFactory) : base(loggerFactory)
    {
        wrapperType = loggerFactory.CreateLogger<T>();
    }

    public override ILogger<T> GetLogger()
    {
        return wrapperType;
    }
}

