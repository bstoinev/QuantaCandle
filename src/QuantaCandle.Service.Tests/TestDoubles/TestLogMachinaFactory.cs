using Microsoft.Extensions.Logging.Abstractions;
using QuantaCandle.Core.Logging;

namespace QuantaCandle.Service.Tests.TestDoubles;

public sealed class TestLogMachinaFactory : ILogMachinaFactory
{
    public ILogMachina<T> Create<T>() where T : class
    {
        return new TestLogMachina<T>();
    }

    private sealed class TestLogMachina<T> : ILogMachina<T> where T : class
    {
        public Microsoft.Extensions.Logging.ILogger<T> GetLogger()
        {
            return NullLogger<T>.Instance;
        }
    }
}

