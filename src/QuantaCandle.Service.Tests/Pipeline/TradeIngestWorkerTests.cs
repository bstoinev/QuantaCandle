using System.Threading.Channels;

using QuantaCandle.Core.Trading;
using QuantaCandle.Service.Options;
using QuantaCandle.Service.Pipeline;
using QuantaCandle.Service.Tests.TestDoubles;

namespace QuantaCandle.Service.Tests.Pipeline;

public sealed class TradeIngestWorkerTests
{
    private readonly TestLogMachinaFactory _logFactory = new();

    private readonly RecordingTradeSink _sink = new();
    private readonly NoOpIngestionStateStore _stateStore = new();
    private readonly TradePipelineStats _stats = new();

    private readonly CollectorOptions _options = new(
        Instruments: ["BTC-USDT"],
        ChannelCapacity: 10,
        BatchSize: 3,
        FlushInterval: TimeSpan.FromHours(1));

    private readonly InMemoryTradeDeduplicator _deduplicator;
    private readonly TradeIngestWorker _worker;

    private readonly Channel<TradeInfo> _channel = Channel.CreateUnbounded<TradeInfo>();

    public TradeIngestWorkerTests()
    {
        _deduplicator = new InMemoryTradeDeduplicator(_options);
        _worker = new TradeIngestWorker(_sink, _stateStore, _deduplicator, _stats, _logFactory.Create<TradeIngestWorker>());
    }

    [Fact]
    public async Task FlushesWhenBatchSizeIsReached()
    {
        var test = _worker.Run(_channel.Reader, _options, CancellationToken.None);

        await _channel.Writer.WriteAsync(CreateTrade("0", _options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await _channel.Writer.WriteAsync(CreateTrade("1", _options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));
        await _channel.Writer.WriteAsync(CreateTrade("2", _options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)));

        _channel.Writer.Complete();
        await test;

        Assert.Single(_sink.Appends);
        Assert.Equal(3, _sink.Appends[0].Count);

        TradePipelineStatsSnapshot snapshot = _stats.GetSnapshot();
        Assert.Equal(3, snapshot.TradesReceived);
        Assert.Equal(3, snapshot.TradesWritten);
        Assert.Equal(0, snapshot.DuplicatesDropped);
        Assert.Equal(1, snapshot.BatchesFlushed);
    }

    [Fact]
    public async Task FlushesRemainingTradesOnShutdown()
    {
        var test = _worker.Run(_channel.Reader, _options, CancellationToken.None);

        await _channel.Writer.WriteAsync(CreateTrade("0", _options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await _channel.Writer.WriteAsync(CreateTrade("1", _options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));

        _channel.Writer.Complete();
        await test;

        Assert.Single(_sink.Appends);
        Assert.Equal(2, _sink.Appends[0].Count);

        TradePipelineStatsSnapshot snapshot = _stats.GetSnapshot();
        Assert.Equal(2, snapshot.TradesReceived);
        Assert.Equal(2, snapshot.TradesWritten);
        Assert.Equal(0, snapshot.DuplicatesDropped);
        Assert.Equal(1, snapshot.BatchesFlushed);
    }

    private static TradeInfo CreateTrade(string tradeId, Instrument instrument, DateTimeOffset timestamp)
    {
        TradeKey key = new TradeKey(new ExchangeId("Stub"), instrument, tradeId);
        return new TradeInfo(key, timestamp, price: 1m, quantity: 1m);
    }
}
