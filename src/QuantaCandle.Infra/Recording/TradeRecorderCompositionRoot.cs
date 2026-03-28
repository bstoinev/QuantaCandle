using LogMachina.SimpleInjector;

using Amazon.S3;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance;
using QuantaCandle.Infra.Options;
using QuantaCandle.Infra.Pipeline;
using QuantaCandle.Infra.Time;

using SimpleInjector;

namespace QuantaCandle.Infra;

/// <summary>
/// Configures the reusable trade recorder runtime graph in Simple Injector.
/// </summary>
public static class TradeRecorderCompositionRoot
{
    /// <summary>
    /// Registers the recorder dependencies into the supplied container.
    /// </summary>
    public static void Configure(Container container, TradeRecorderRunOptions options)
    {
        ArgumentNullException.ThrowIfNull(container);
        ArgumentNullException.ThrowIfNull(options);

        container.AddLogMachina();
        container.RegisterInstance(options.CollectorOptions);
        container.RegisterInstance(options.RetryOptions);

        container.RegisterSingleton<IClock, SystemClock>();
        container.RegisterSingleton<TradePipelineStats>();
        container.RegisterSingleton<ITradeDeduplicator, InMemoryTradeDeduplicator>();

        RegisterTradeSource(container, options.SourceRegistration);
        RegisterTradeSink(container, options.SinkRegistration);

        container.RegisterSingleton<TradeIngestWorker>();
    }

    private static void RegisterTradeSource(Container container, TradeRecorderSourceRegistration tradeSourceRegistration)
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

    private static void RegisterTradeSink(Container container, TradeRecorderSinkRegistration tradeSinkRegistration)
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
            container.RegisterSingleton<IAmazonS3>(() => new AmazonS3Client());
            container.RegisterSingleton<IS3ObjectUploader, AwsS3Uploader>();
            container.RegisterSingleton<ITradeSink, TradeSinkS3Simple>();
        }
        else
        {
            container.RegisterSingleton<ITradeSink, TradeSinkNull>();
        }
    }
}
