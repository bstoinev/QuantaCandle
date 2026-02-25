using Microsoft.Extensions.Logging;

namespace QuantaCandle.Core.Logging;

public interface ILogMachina<T> where T : class
{
    ILogger<T> GetLogger();
}
