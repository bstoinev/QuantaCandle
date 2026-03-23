namespace QuantaCandle.Core.Logging;

public interface ILogMachinaFactory
{
    ILogMachina<T> Create<T>() where T : class;
}
