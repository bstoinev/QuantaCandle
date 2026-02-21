using Microsoft.Extensions.Logging;

namespace QuantaCandle.Infra.Logging;

public class NLogLogMachina<T> : ILogMachinaBase<T> where T : class
{
    private readonly ILogger<T> _wrapperType;

    public NLogLogMachina(ILoggerFactory loggerFactory) : base(loggerFactory)
    {
        _wrapperType = loggerFactory.CreateLogger<T>();
    }

    public override ILogger<T> GetLogger() => _wrapperType;
}
