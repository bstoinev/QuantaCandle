using SimpleInjector;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Options;
using QuantaCandle.Infra.Pipeline;
using QuantaCandle.Infra.Recording;

namespace QuantaCandle.Infra.Tests.Recording;

/// <summary>
/// Verifies Simple Injector can compose the recorder runtime for the S3 sink.
/// </summary>
public sealed class TradeRecorderCompositionRootTests
{
    [Fact]
    public void ConfigureUsesFileSinkWhenSinkOptionIsOmitted()
    {
        using var container = new Container();
        var options = TradeRecorderCommand.Parse(
        [
            "BTCUSDT",
            "--exchange", "stub",
        ]);

        TradeRecorderCompositionRoot.Configure(container, options);

        container.Verify();

        var checkpointLifecycle = container.GetInstance<ITradeCheckpointLifecycle>();
        var startupTask = container.GetInstance<ITradeRecorderStartupTask>();
        var sink = container.GetInstance<ITradeFinalizedFileDispatcher>();
        var snapshotDispatcher = container.GetInstance<ITradeSnapshotFileDispatcher>();

        Assert.IsType<TradeScratchCheckpointLifecycle>(checkpointLifecycle);
        Assert.IsType<TradeFinalizedFileStartupDispatcher>(startupTask);
        Assert.IsType<TradeSinkFileSimple>(sink);
        Assert.IsType<TradeSinkFileSimple>(snapshotDispatcher);
    }

    [Fact]
    public void ConfigureUsesNullSinkWhenExplicitNullSinkIsRequested()
    {
        using var container = new Container();
        var options = TradeRecorderCommand.Parse(
        [
            "BTCUSDT",
            "--exchange", "stub",
            "--sink", "null",
        ]);

        TradeRecorderCompositionRoot.Configure(container, options);

        container.Verify();

        var checkpointLifecycle = container.GetInstance<ITradeCheckpointLifecycle>();
        var startupTask = container.GetInstance<ITradeRecorderStartupTask>();
        var sink = container.GetInstance<ITradeFinalizedFileDispatcher>();
        var snapshotDispatcher = container.GetInstance<ITradeSnapshotFileDispatcher>();

        Assert.IsType<NullTradeCheckpointLifecycle>(checkpointLifecycle);
        Assert.IsType<NullTradeRecorderStartupTask>(startupTask);
        Assert.IsType<TradeSinkNull>(sink);
        Assert.IsType<TradeSinkNull>(snapshotDispatcher);
    }

    [Fact]
    public void ConfigureAllowsResolvingS3SinkComposition()
    {
        var previousAwsRegion = Environment.GetEnvironmentVariable("AWS_REGION");

        Environment.SetEnvironmentVariable("AWS_REGION", "us-east-1");

        try
        {
            using var container = new Container();
            var options = new TradeRecorderRunOptions(Duration: null,
                CacheSize: 1024,
                new CollectorOptions(Instruments: ["BTC-USDT"], ChannelCapacity: 100, BatchSize: 100, FlushInterval: TimeSpan.FromSeconds(1), CheckpointInterval: TimeSpan.FromHours(1)),
                new RetryOptions(InitialDelay: TimeSpan.FromMilliseconds(100), MaxDelay: TimeSpan.FromSeconds(5)),
                new TradeRecorderSourceRegistration(BinanceOptions: null,
                    StubOptions: new TradeSourceStubOptions(Exchange: new ExchangeId("Stub"), TradesPerSecond: 1, StartPrice: 50_000m, PriceStep: 0.01m, Quantity: 0.001m)),
                new TradeRecorderSinkRegistration(FileOptions: null, S3Options: new TradeSinkS3SimpleOptions(BucketName: "bucket-name", Prefix: "trades", LocalRootDirectory: "trades-out", CheckpointInterval: TimeSpan.FromHours(1))));

            TradeRecorderCompositionRoot.Configure(container, options);

            container.Verify();

            var ingestionStateStore = container.GetInstance<IIngestionStateStore>();
            var checkpointLifecycle = container.GetInstance<ITradeCheckpointLifecycle>();
            var startupTask = container.GetInstance<ITradeRecorderStartupTask>();
            var uploader = container.GetInstance<IS3ObjectUploader>();
            var sink = container.GetInstance<ITradeFinalizedFileDispatcher>();
            var snapshotDispatcher = container.GetInstance<ITradeSnapshotFileDispatcher>();

            Assert.IsType<LocalFileIngestionStateStore>(ingestionStateStore);
            Assert.IsType<TradeScratchCheckpointLifecycle>(checkpointLifecycle);
            Assert.IsType<TradeFinalizedFileStartupDispatcher>(startupTask);
            Assert.IsType<AwsS3Uploader>(uploader);
            Assert.IsType<TradeSinkS3Simple>(sink);
            Assert.IsType<TradeSinkS3Simple>(snapshotDispatcher);
        }
        finally
        {
            Environment.SetEnvironmentVariable("AWS_REGION", previousAwsRegion);
        }
    }
}
