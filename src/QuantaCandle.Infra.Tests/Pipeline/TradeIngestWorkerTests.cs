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
        var worker = CreateWorker(options, appends, out _, out var stats);
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
        var worker = CreateWorker(options, appends, out _, out var stats);
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
        out Mock<IIngestionStateStore> stateStoreMock,
        out TradePipelineStats stats)
    {
        var tradeSinkMoq = new Mock<ITradeSink>();
        tradeSinkMoq
            .Setup(mock => mock.Append(It.IsAny<IReadOnlyList<TradeInfo>>(), It.IsAny<CancellationToken>()))
            .Returns((IReadOnlyList<TradeInfo> trades, CancellationToken _) =>
            {
                appends.Add(trades);
                return ValueTask.FromResult(new TradeAppendResult(InsertedCount: trades.Count, DuplicateCount: 0));
            });

        stateStoreMock = new Mock<IIngestionStateStore>();
        stateStoreMock
            .Setup(mock => mock.SetWatermarkAsync(It.IsAny<ExchangeId>(), It.IsAny<Instrument>(), It.IsAny<TradeWatermark>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        stats = new TradePipelineStats();
        var deduplicator = new InMemoryTradeDeduplicator(options);
        var logMoq = new Mock<ILogMachina<TradeIngestWorker>>();

        var worker = new TradeIngestWorker(tradeSinkMoq.Object, stateStoreMock.Object, deduplicator, stats, logMoq.Object);
        return worker;
    }

    private static TradeInfo CreateTrade(string tradeId, Instrument instrument, DateTimeOffset timestamp)
    {
        var key = new TradeKey(new ExchangeId("Stub"), instrument, tradeId);
        return new TradeInfo(key, timestamp, price: 1m, quantity: 1m);
    }
}
