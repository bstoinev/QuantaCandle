using System.Threading.Channels;

using LogMachina;

using Moq;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Options;
using QuantaCandle.Infra.Pipeline;

namespace QuantaCandle.Infra.Tests.Pipeline;

public sealed class TradeDeduplicationTests
{
    private readonly List<IReadOnlyList<TradeInfo>> _appends = new();
    private readonly TradePipelineStats _stats = new();
    private readonly CollectorOptions _options = new CollectorOptions(["BTC-USDT"], 10, 1000, TimeSpan.FromHours(1), 100);
    private readonly TradeIngestWorker _worker;
    private readonly DateTimeOffset _t0 = new(2026, 3, 12, 0, 0, 0, TimeSpan.Zero);

    Channel<TradeInfo> _channel = Channel.CreateUnbounded<TradeInfo>();

    public TradeDeduplicationTests()
    {
        _worker = CreateWorker(_options, _appends, _stats);
    }

    [Fact]
    public async Task DropsDuplicateTradesWithSameTradeKey()
    {
        var run = _worker.Run(_channel.Reader, _options, CancellationToken.None);

        var instrument = _options.Instruments[0];
        var t0 = new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero);

        await _channel.Writer.WriteAsync(CreateTrade("0", instrument, t0));
        await _channel.Writer.WriteAsync(CreateTrade("0", instrument, t0));
        await _channel.Writer.WriteAsync(CreateTrade("1", instrument, t0.AddSeconds(1)));

        _channel.Writer.Complete();
        await run;

        Assert.Single(_appends);
        Assert.Equal(2, _appends[0].Count);

        var snapshot = _stats.GetSnapshot();
        Assert.Equal(3, snapshot.TradesReceived);
        Assert.Equal(2, snapshot.TradesWritten);
        Assert.Equal(1, snapshot.DuplicatesDropped);
        Assert.Equal(1, snapshot.BatchesFlushed);
    }

    [Fact]
    public async Task DistinctTradesPassThrough()
    {
        var test = _worker.Run(_channel.Reader, _options, CancellationToken.None);

        var instrument = _options.Instruments[0];

        await _channel.Writer.WriteAsync(CreateTrade("0", instrument, _t0));
        await _channel.Writer.WriteAsync(CreateTrade("1", instrument, _t0.AddSeconds(1)));
        await _channel.Writer.WriteAsync(CreateTrade("2", instrument, _t0.AddSeconds(2)));

        _channel.Writer.Complete();
        await test;

        Assert.Single(_appends);
        Assert.Equal(3, _appends[0].Count);

        var snapshot = _stats.GetSnapshot();

        Assert.Equal(3, snapshot.TradesReceived);
        Assert.Equal(3, snapshot.TradesWritten);
        Assert.Equal(0, snapshot.DuplicatesDropped);
    }

    [Fact]
    public void BoundedCacheEvictionAllowsOldDuplicatesAfterWindow()
    {
        var instrument = _options.Instruments[0];

        var testOptions = new CollectorOptions(
            Instruments: [instrument],
            ChannelCapacity: 10,
            BatchSize: 10,
            FlushInterval: TimeSpan.FromHours(1),
            DeduplicationCapacity: 2);

        var deduplicator = new InMemoryTradeDeduplicator(testOptions);

        var k1 = new TradeKey(new ExchangeId("Stub"), instrument, "1");
        var k2 = new TradeKey(new ExchangeId("Stub"), instrument, "2");
        var k3 = new TradeKey(new ExchangeId("Stub"), instrument, "3");

        Assert.True(deduplicator.TryAccept(k1));
        Assert.True(deduplicator.TryAccept(k2));
        Assert.False(deduplicator.TryAccept(k1));

        Assert.True(deduplicator.TryAccept(k3));
        Assert.True(deduplicator.TryAccept(k1));
    }

    [Fact]
    public async Task ShutdownFlushStillWritesUniques()
    {
        var test = _worker.Run(_channel.Reader, _options, CancellationToken.None);

        var instrument = _options.Instruments[0];

        await _channel.Writer.WriteAsync(CreateTrade("0", instrument, _t0));
        await _channel.Writer.WriteAsync(CreateTrade("0", instrument, _t0));
        await _channel.Writer.WriteAsync(CreateTrade("1", instrument, _t0.AddSeconds(1)));

        _channel.Writer.Complete();
        await test;

        Assert.Single(_appends);
        Assert.Equal(2, _appends[0].Count);

        var snapshot = _stats.GetSnapshot();

        Assert.Equal(2, snapshot.TradesWritten);
        Assert.Equal(1, snapshot.DuplicatesDropped);
    }

    private static TradeIngestWorker CreateWorker(
        CollectorOptions options,
        List<IReadOnlyList<TradeInfo>> appends,
        TradePipelineStats stats)
    {
        var tradeSinkMoq = new Mock<ITradeSink>();
        tradeSinkMoq
            .Setup(mock => mock.Append(It.IsAny<IReadOnlyList<TradeInfo>>(), It.IsAny<CancellationToken>()))
            .Returns((IReadOnlyList<TradeInfo> trades, CancellationToken _) =>
            {
                appends.Add(trades);
                return ValueTask.FromResult(new TradeAppendResult(InsertedCount: trades.Count, DuplicateCount: 0));
            });

        var stateStoreMoq = new Mock<IIngestionStateStore>();
        stateStoreMoq
            .Setup(mock => mock.GetResumeBoundaryAsync(It.IsAny<ExchangeId>(), It.IsAny<Instrument>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.FromResult<ResumeBoundary?>(null));
        stateStoreMoq
            .Setup(mock => mock.SetWatermarkAsync(It.IsAny<ExchangeId>(), It.IsAny<Instrument>(), It.IsAny<TradeWatermark>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        var deduplicator = new InMemoryTradeDeduplicator(options);
        var logMoq = new Mock<ILogMachina<TradeIngestWorker>>();

        var worker = new TradeIngestWorker(tradeSinkMoq.Object, stateStoreMoq.Object, deduplicator, stats, logMoq.Object);
        return worker;
    }

    private static TradeInfo CreateTrade(string tradeId, Instrument instrument, DateTimeOffset timestamp)
    {
        var key = new TradeKey(new ExchangeId("Stub"), instrument, tradeId);
        return new TradeInfo(key, timestamp, price: 1m, quantity: 1m);
    }
}

