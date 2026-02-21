using SimpleInjector;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;
using QuantaCandle.Infra.Logging;

namespace QuantaCandle.CLI;

public static class CompositionRoot
{
    public static Container ConfigureServices()
    {
        Container container = new Container();

        ILoggerFactory loggingFactory = LoggerFactory.Create(builder =>
        {
            builder.ClearProviders();
            builder.AddNLog();
        });

        container.RegisterInstance(loggingFactory);

        container.Register(typeof(ILogMachina<>), typeof(NLogLogMachina<>), Lifestyle.Singleton);

        container.Verify();

        return container;
    }
}
