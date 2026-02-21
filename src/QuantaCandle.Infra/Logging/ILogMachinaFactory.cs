using Microsoft.Extensions.Logging;

namespace QuantaCandle.Infra.Logging;

public interface ILogMachinaFactory
{
    ILogMachina<T> Create<T>() where T : class;
}
