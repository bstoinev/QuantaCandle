using Amazon.S3;

using LogMachina;
using LogMachina.SimpleInjector;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance;
using QuantaCandle.Infra.Pipeline;
using QuantaCandle.Infra.Storage;
using QuantaCandle.Infra.Time;

using SimpleInjector;

namespace QuantaCandle.Infra.Recording;

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

        container.AddLogMachina(c => c.WithNLog(Lifestyle.Singleton));

        container.RegisterInstance(options.CollectorOptions);
        container.RegisterInstance(new TradeCheckpointTriggerOptions(options.CacheSize));
        container.RegisterInstance(options.RetryOptions);

        container.RegisterSingleton<IClock, SystemClock>();
        container.RegisterSingleton<ICheckpointSignal, CheckpointSignal>();
        container.RegisterSingleton<IConsoleKeyReader, SystemConsoleKeyReader>();
        container.RegisterSingleton<ConsoleCheckpointHotkeyListener>();
        container.RegisterSingleton<TradePipelineStats>();
        container.RegisterSingleton<ITradeDeduplicator, InMemoryTradeDeduplicator>();
        container.RegisterSingleton<ITradeGapScanner, LocalFileTradeGapScanner>();

        RegisterTradeSource(container, options.SourceRegistration);
        RegisterTradeSink(container, options.SinkRegistration);

        container.RegisterSingleton<TradeIngestWorker>();
    }

    private static void RegisterTradeSource(Container container, TradeRecorderSourceRegistration tradeSourceRegistration)
    {
        if (tradeSourceRegistration.BinanceOptions is not null)
        {
            container.RegisterInstance(tradeSourceRegistration.BinanceOptions);
            container.RegisterSingleton(() => new HttpClient());
            container.RegisterSingleton<ITradeGapFetchClient, BinanceTradeGapFetchClient>();
            container.RegisterSingleton<IBinanceRawTradeLookupClient, BinanceRawTradeLookupClient>();
            container.RegisterSingleton<ITradeDayBoundaryResolver, TradeDayBoundaryResolver>();
            container.RegisterSingleton<ITradeGapHealer, LocalFileTradeGapHealer>();
            container.RegisterSingleton<ITradeSource, BinanceTradeSource>();
        }
        else
        {
            var stubOptions = tradeSourceRegistration.StubOptions ?? throw new InvalidOperationException("Stub options must be configured for the stub trade source.");
            container.RegisterInstance(stubOptions);
            container.RegisterSingleton<ITradeSource, TradeSourceStub>();
        }

    }

    private static void RegisterTradeSink(Container container, TradeRecorderSinkRegistration tradeSinkRegistration)
    {
        if (tradeSinkRegistration.FileOptions is not null)
        {
            var fileOptions = tradeSinkRegistration.FileOptions;
            container.RegisterInstance(fileOptions);
            container.RegisterSingleton<IIngestionStateStore>(() => new LocalFileIngestionStateStore(fileOptions.OutputDirectory, container.GetInstance<IClock>()));
            container.RegisterSingleton<TradeSinkFileSimple>();
            container.RegisterSingleton<ITradeFinalizedFileDispatcher>(() => container.GetInstance<TradeSinkFileSimple>());
            container.RegisterSingleton<ITradeSnapshotFileDispatcher>(() => container.GetInstance<TradeSinkFileSimple>());
            container.RegisterSingleton<ITradeRecorderStartupTask>(() => new TradeFinalizedFileStartupDispatcher(fileOptions.OutputDirectory, container.GetInstance<ITradeFinalizedFileDispatcher>(), container.GetInstance<ILogMachina<TradeFinalizedFileStartupDispatcher>>()));
            container.RegisterSingleton<ITradeCheckpointLifecycle>(() => new TradeScratchCheckpointLifecycle(container.GetInstance<IClock>(), fileOptions.OutputDirectory, container.GetInstance<ITradeFinalizedFileDispatcher>(), container.GetInstance<ITradeSnapshotFileDispatcher>(), container.GetInstance<IIngestionStateStore>(), container.GetInstance<ITradeGapScanner>(), GetOptionalInstance<ITradeGapHealer>(container), GetOptionalInstance<ITradeDayBoundaryResolver>(container), container.GetInstance<ILogMachina<TradeScratchCheckpointLifecycle>>()));
        }
        else if (tradeSinkRegistration.S3Options is not null)
        {
            var s3Options = tradeSinkRegistration.S3Options;
            container.RegisterInstance(s3Options);
            container.RegisterSingleton<IAmazonS3>(() => new AmazonS3Client());
            container.RegisterSingleton<IS3ObjectUploader, AwsS3Uploader>();
            container.RegisterSingleton<IIngestionStateStore>(() => new LocalFileIngestionStateStore(s3Options.LocalRootDirectory, container.GetInstance<IClock>()));
            container.RegisterSingleton<TradeSinkS3Simple>();
            container.RegisterSingleton<ITradeFinalizedFileDispatcher>(() => container.GetInstance<TradeSinkS3Simple>());
            container.RegisterSingleton<ITradeSnapshotFileDispatcher>(() => container.GetInstance<TradeSinkS3Simple>());
            container.RegisterSingleton<ITradeRecorderStartupTask>(() => new TradeFinalizedFileStartupDispatcher(s3Options.LocalRootDirectory, container.GetInstance<ITradeFinalizedFileDispatcher>(), container.GetInstance<ILogMachina<TradeFinalizedFileStartupDispatcher>>()));
            container.RegisterSingleton<ITradeCheckpointLifecycle>(() => new TradeScratchCheckpointLifecycle(container.GetInstance<IClock>(), s3Options.LocalRootDirectory, container.GetInstance<ITradeFinalizedFileDispatcher>(), container.GetInstance<ITradeSnapshotFileDispatcher>(), container.GetInstance<IIngestionStateStore>(), container.GetInstance<ITradeGapScanner>(), GetOptionalInstance<ITradeGapHealer>(container), GetOptionalInstance<ITradeDayBoundaryResolver>(container), container.GetInstance<ILogMachina<TradeScratchCheckpointLifecycle>>()));
        }
        else
        {
            container.RegisterSingleton<IIngestionStateStore, InMemoryIngestionStateStore>();
            container.RegisterSingleton<ITradeRecorderStartupTask, NullTradeRecorderStartupTask>();
            container.RegisterSingleton<ITradeCheckpointLifecycle, NullTradeCheckpointLifecycle>();
            container.RegisterSingleton<TradeSinkNull>();
            container.RegisterSingleton<ITradeFinalizedFileDispatcher>(() => container.GetInstance<TradeSinkNull>());
            container.RegisterSingleton<ITradeSnapshotFileDispatcher>(() => container.GetInstance<TradeSinkNull>());
        }
    }

    private static TService? GetOptionalInstance<TService>(Container container)
        where TService : class
    {
        TService? result = null;
        var registration = container.GetRegistration(typeof(TService), throwOnFailure: false);

        if (registration is not null)
        {
            result = container.GetInstance<TService>();
        }

        return result;
    }
}
