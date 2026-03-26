using LogMachina.SimpleInjector;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance;
using QuantaCandle.Service.Options;
using QuantaCandle.Service.Pipeline;
using QuantaCandle.Service.Stubs;
using QuantaCandle.Service.Time;

using SimpleInjector;
using SimpleInjector.Lifestyles;

namespace QuantaCandle.CLI;

public static class CompositionRoot
{
    public static Container ConfigureCollector(
        CollectorOptions collectorOptions,
        RetryOptions retryOptions,
        TradeSourceRegistration tradeSourceRegistration,
        TradeSinkRegistration tradeSinkRegistration)
    {
        var result = new Container();
        result.Options.DefaultScopedLifestyle = new AsyncScopedLifestyle();

        result.AddLogMachina();
        result.RegisterInstance(collectorOptions);
        result.RegisterInstance(retryOptions);

        result.RegisterSingleton<IClock, SystemClock>();
        result.RegisterSingleton<TradePipelineStats>();
        result.RegisterSingleton<ITradeDeduplicator, InMemoryTradeDeduplicator>();

        RegisterTradeSource(result, tradeSourceRegistration);
        RegisterTradeSink(result, tradeSinkRegistration);

        result.RegisterSingleton<TradeIngestWorker>();

        return result;
    }

    private static void RegisterTradeSource(Container container, TradeSourceRegistration tradeSourceRegistration)
    {
        if (tradeSourceRegistration.BinanceOptions is not null)
        {
            container.RegisterInstance(tradeSourceRegistration.BinanceOptions);
            container.RegisterSingleton<ITradeSource, BinanceTradeSource>();
        }
        else
        {
            var stubOptions = tradeSourceRegistration.StubOptions ?? throw new InvalidOperationException("Stub options must be configured for the stub trade source.");
            container.RegisterInstance(stubOptions);
            container.RegisterSingleton<ITradeSource, TradeSourceStub>();
        }

        container.RegisterSingleton<IIngestionStateStore, InMemoryIngestionStateStore>();
    }

    private static void RegisterTradeSink(Container container, TradeSinkRegistration tradeSinkRegistration)
    {
        if (tradeSinkRegistration.FileOptions is not null)
        {
            container.RegisterInstance(tradeSinkRegistration.FileOptions);
            container.RegisterSingleton<ITradeSink, TradeSinkFileSimple>();
        }
        else if (tradeSinkRegistration.S3Options is not null)
        {
            var s3Options = tradeSinkRegistration.S3Options;
            container.RegisterInstance(s3Options);
            container.RegisterSingleton<IS3ObjectUploader, AwsSdkS3ObjectUploader>();
            container.RegisterSingleton<ITradeSink, TradeSinkS3Simple>();
        }
        else
        {
            container.RegisterSingleton<ITradeSink, TradeSinkNull>();
        }
    }
}
