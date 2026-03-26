using LogMachina.SimpleInjector;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance;
using QuantaCandle.Infra.Options;
using QuantaCandle.Infra.Pipeline;
using QuantaCandle.Infra;
using QuantaCandle.Infra.Time;

using SimpleInjector;

namespace QuantaCandle.CLI;

/// <summary>
/// Configures the collector runtime object graph in Simple Injector.
/// </summary>
public static class CompositionRoot
{
    /// <summary>
    /// Registers the collector dependencies into the supplied container.
    /// </summary>
    public static void ConfigureCollector(
        Container container,
        CollectorOptions collectorOptions,
        RetryOptions retryOptions,
        TradeSourceRegistration tradeSourceRegistration,
        TradeSinkRegistration tradeSinkRegistration)
    {
        container.AddLogMachina();
        container.RegisterInstance(collectorOptions);
        container.RegisterInstance(retryOptions);

        container.RegisterSingleton<IClock, SystemClock>();
        container.RegisterSingleton<TradePipelineStats>();
        container.RegisterSingleton<ITradeDeduplicator, InMemoryTradeDeduplicator>();

        RegisterTradeSource(container, tradeSourceRegistration);
        RegisterTradeSink(container, tradeSinkRegistration);

        container.RegisterSingleton<TradeIngestWorker>();
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
