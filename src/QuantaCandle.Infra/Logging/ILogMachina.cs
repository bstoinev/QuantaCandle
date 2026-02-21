using Microsoft.Extensions.Logging;

namespace QuantaCandle.Infra.Logging;

public interface ILogMachina<T> where T : class
{
    ILogger<T> GetLogger();
}
