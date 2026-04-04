using System.Threading.Channels;

using LogMachina;

using Moq;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Options;
using QuantaCandle.Infra.Pipeline;

namespace QuantaCandle.Infra.Tests.Pipeline;

public sealed class TradeIngestWorkerTests
{
    [Fact]
    public async Task FlushesWhenBatchSizeIsReached()
    {
        var options = CreateOptions(batchSize: 3);
        var appends = new List<IReadOnlyList<TradeInfo>>();
        var worker = CreateWorker(options, appends, new InMemoryIngestionStateStore(), out var stats);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("0", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("1", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("2", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)));

        channel.Writer.Complete();
        await run;

        Assert.Single(appends);
        Assert.Equal(3, appends[0].Count);

        var snapshot = stats.GetSnapshot();
        Assert.Equal(3, snapshot.TradesReceived);
        Assert.Equal(3, snapshot.TradesWritten);
        Assert.Equal(0, snapshot.DuplicatesDropped);
        Assert.Equal(1, snapshot.BatchesFlushed);
    }

    [Fact]
    public async Task FlushesRemainingTradesOnShutdown()
    {
        var options = CreateOptions(batchSize: 3);
        var appends = new List<IReadOnlyList<TradeInfo>>();
        var worker = CreateWorker(options, appends, new InMemoryIngestionStateStore(), out var stats);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("0", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("1", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));

        channel.Writer.Complete();
        await run;

        Assert.Single(appends);
        Assert.Equal(2, appends[0].Count);

        var snapshot = stats.GetSnapshot();
        Assert.Equal(2, snapshot.TradesReceived);
        Assert.Equal(2, snapshot.TradesWritten);
        Assert.Equal(0, snapshot.DuplicatesDropped);
        Assert.Equal(1, snapshot.BatchesFlushed);
    }

    [Fact]
    public async Task InvokesSinkShutdownFlushWhenLifecycleIsAvailable()
    {
        var options = CreateOptions(batchSize: 3);
        var lifecycleSink = new RecordingLifecycleTradeSink();
        var worker = CreateWorker(options, lifecycleSink, new InMemoryIngestionStateStore(), out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("0", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        channel.Writer.Complete();
        await run;

        Assert.Equal(1, lifecycleSink.ShutdownFlushCallCount);
    }

    [Fact]
    public async Task LiveIngestAppendsTradesAndCheckpointsWatermarks()
    {
        var options = CreateOptions(batchSize: 2);
        var stateStore = new InMemoryIngestionStateStore();
        var appends = new List<IReadOnlyList<TradeInfo>>();
        var worker = CreateWorker(options, appends, stateStore, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("101", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("102", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));
        channel.Writer.Complete();
        await run;

        Assert.Single(appends);
        Assert.Equal(2, appends[0].Count);

        var checkpoint = await stateStore.GetWatermarkAsync(new ExchangeId("Stub"), options.Instruments[0], CancellationToken.None);
        Assert.Equal("102", checkpoint?.TradeId);
        Assert.Equal(new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero), checkpoint?.Timestamp);
    }

    [Fact]
    public async Task LiveIngestDoesNotReadOrRecordGapState()
    {
        var options = CreateOptions(batchSize: 2);
        var channel = Channel.CreateUnbounded<TradeInfo>();
        var tradeSinkMoq = new Mock<ITradeSink>(MockBehavior.Strict);
        var stateStoreMoq = new Mock<IIngestionStateStore>(MockBehavior.Strict);
        var stats = new TradePipelineStats();
        var deduplicator = new InMemoryTradeDeduplicator(options);
        var logMoq = new Mock<ILogMachina<TradeIngestWorker>>();

        tradeSinkMoq
            .Setup(mock => mock.Append(It.IsAny<IReadOnlyList<TradeInfo>>(), It.IsAny<CancellationToken>()))
            .Returns((IReadOnlyList<TradeInfo> trades, CancellationToken _) =>
                ValueTask.FromResult(new TradeAppendResult(InsertedCount: trades.Count, DuplicateCount: 0)));

        stateStoreMoq
            .Setup(mock => mock.SetWatermarkAsync(
                It.IsAny<ExchangeId>(),
                It.IsAny<Instrument>(),
                It.IsAny<TradeWatermark>(),
                It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        var worker = new TradeIngestWorker(tradeSinkMoq.Object, stateStoreMoq.Object, deduplicator, stats, logMoq.Object);
        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("101", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("102", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));

        channel.Writer.Complete();
        await run;

        stateStoreMoq.Verify(
            mock => mock.GetResumeBoundaryAsync(It.IsAny<ExchangeId>(), It.IsAny<Instrument>(), It.IsAny<CancellationToken>()),
            Times.Never);
        stateStoreMoq.Verify(
            mock => mock.GetWatermarkAsync(It.IsAny<ExchangeId>(), It.IsAny<Instrument>(), It.IsAny<CancellationToken>()),
            Times.Never);
        stateStoreMoq.Verify(
            mock => mock.RecordGapAsync(It.IsAny<TradeGap>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    private static CollectorOptions CreateOptions(int batchSize)
    {
        var options = new CollectorOptions(
            Instruments: ["BTC-USDT"],
            ChannelCapacity: 10,
            BatchSize: batchSize,
            FlushInterval: TimeSpan.FromHours(1));
        return options;
    }

    private static TradeIngestWorker CreateWorker(
        CollectorOptions options,
        List<IReadOnlyList<TradeInfo>> appends,
        IIngestionStateStore ingestionStateStore,
        out TradePipelineStats stats,
        TaskCompletionSource? appendObserved = null)
    {
        var tradeSinkMoq = new Mock<ITradeSink>();
        tradeSinkMoq
            .Setup(mock => mock.Append(It.IsAny<IReadOnlyList<TradeInfo>>(), It.IsAny<CancellationToken>()))
            .Returns((IReadOnlyList<TradeInfo> trades, CancellationToken _) =>
            {
                appends.Add(trades);
                appendObserved?.TrySetResult();
                return ValueTask.FromResult(new TradeAppendResult(InsertedCount: trades.Count, DuplicateCount: 0));
            });

        return CreateWorker(options, tradeSinkMoq.Object, ingestionStateStore, out stats);
    }

    private static TradeIngestWorker CreateWorker(
        CollectorOptions options,
        ITradeSink tradeSink,
        IIngestionStateStore ingestionStateStore,
        out TradePipelineStats stats)
    {
        stats = new TradePipelineStats();
        var deduplicator = new InMemoryTradeDeduplicator(options);
        var logMoq = new Mock<ILogMachina<TradeIngestWorker>>();

        var worker = new TradeIngestWorker(tradeSink, ingestionStateStore, deduplicator, stats, logMoq.Object);
        return worker;
    }

    private static TradeInfo CreateTrade(string tradeId, Instrument instrument, DateTimeOffset timestamp)
    {
        var key = new TradeKey(new ExchangeId("Stub"), instrument, tradeId);
        return new TradeInfo(key, timestamp, price: 1m, quantity: 1m);
    }

    private sealed class RecordingLifecycleTradeSink : ITradeSink, ITradeSinkLifecycle
    {
        public int ShutdownFlushCallCount { get; private set; }

        public ValueTask<TradeAppendResult> Append(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(new TradeAppendResult(trades.Count, DuplicateCount: 0));
        }

        public ValueTask CheckpointActive(CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask FlushOnShutdown(CancellationToken cancellationToken)
        {
            ShutdownFlushCallCount++;
            return ValueTask.CompletedTask;
        }
    }
}
