using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using QuantaCandle.Core.Trading;
using QuantaCandle.Service.Options;
using QuantaCandle.Service.Pipeline;
using QuantaCandle.Service.Tests.TestDoubles;

namespace QuantaCandle.Service.Tests.Pipeline;

public sealed class TradeIngestWorkerTests
{
    [Fact]
    public async Task Flushes_when_batch_size_is_reached()
    {
        RecordingTradeSink sink = new RecordingTradeSink();
        NoOpIngestionStateStore stateStore = new NoOpIngestionStateStore();
        TradePipelineStats stats = new TradePipelineStats();

        CollectorOptions options = new CollectorOptions(
            Instruments: new[] { Instrument.Parse("BTC-USDT") },
            ChannelCapacity: 10,
            BatchSize: 3,
            FlushInterval: TimeSpan.FromHours(1));

        InMemoryTradeDeduplicator deduplicator = new InMemoryTradeDeduplicator(options);
        TradeIngestWorker worker = new TradeIngestWorker(sink, stateStore, deduplicator, stats, new TestLogMachinaFactory());

        Channel<TradeInfo> channel = Channel.CreateUnbounded<TradeInfo>();

        Task run = worker.RunAsync(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("0", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("1", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("2", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)));

        channel.Writer.Complete();
        await run;

        Assert.Single(sink.Appends);
        Assert.Equal(3, sink.Appends[0].Count);

        TradePipelineStatsSnapshot snapshot = stats.GetSnapshot();
        Assert.Equal(3, snapshot.TradesReceived);
        Assert.Equal(3, snapshot.TradesWritten);
        Assert.Equal(0, snapshot.DuplicatesDropped);
        Assert.Equal(1, snapshot.BatchesFlushed);
    }

    [Fact]
    public async Task Flushes_remaining_trades_on_shutdown()
    {
        RecordingTradeSink sink = new RecordingTradeSink();
        NoOpIngestionStateStore stateStore = new NoOpIngestionStateStore();
        TradePipelineStats stats = new TradePipelineStats();

        CollectorOptions options = new CollectorOptions(
            Instruments: new[] { Instrument.Parse("BTC-USDT") },
            ChannelCapacity: 10,
            BatchSize: 10,
            FlushInterval: TimeSpan.FromHours(1));

        InMemoryTradeDeduplicator deduplicator = new InMemoryTradeDeduplicator(options);
        TradeIngestWorker worker = new TradeIngestWorker(sink, stateStore, deduplicator, stats, new TestLogMachinaFactory());

        Channel<TradeInfo> channel = Channel.CreateUnbounded<TradeInfo>();

        Task run = worker.RunAsync(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("0", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("1", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));

        channel.Writer.Complete();
        await run;

        Assert.Single(sink.Appends);
        Assert.Equal(2, sink.Appends[0].Count);

        TradePipelineStatsSnapshot snapshot = stats.GetSnapshot();
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
