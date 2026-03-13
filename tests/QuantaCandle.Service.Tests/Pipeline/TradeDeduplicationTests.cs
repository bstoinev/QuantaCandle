using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using QuantaCandle.Core.Trading;
using QuantaCandle.Service.Options;
using QuantaCandle.Service.Pipeline;
using QuantaCandle.Service.Tests.TestDoubles;

namespace QuantaCandle.Service.Tests.Pipeline;

public sealed class TradeDeduplicationTests
{
    [Fact]
    public async Task Drops_duplicate_trades_with_same_TradeKey()
    {
        RecordingTradeSink sink = new RecordingTradeSink();
        NoOpIngestionStateStore stateStore = new NoOpIngestionStateStore();
        TradePipelineStats stats = new TradePipelineStats();

        CollectorOptions options = new CollectorOptions(
            Instruments: new[] { Instrument.Parse("BTC-USDT") },
            ChannelCapacity: 10,
            BatchSize: 10,
            FlushInterval: TimeSpan.FromHours(1),
            DeduplicationCapacity: 100);

        InMemoryTradeDeduplicator deduplicator = new InMemoryTradeDeduplicator(options);
        TradeIngestWorker worker = new TradeIngestWorker(sink, stateStore, deduplicator, stats, new TestLogMachinaFactory());

        Channel<TradeInfo> channel = Channel.CreateUnbounded<TradeInfo>();
        Task run = worker.RunAsync(channel.Reader, options, CancellationToken.None);

        Instrument instrument = options.Instruments[0];
        DateTimeOffset t0 = new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero);

        await channel.Writer.WriteAsync(CreateTrade("0", instrument, t0));
        await channel.Writer.WriteAsync(CreateTrade("0", instrument, t0));
        await channel.Writer.WriteAsync(CreateTrade("1", instrument, t0.AddSeconds(1)));

        channel.Writer.Complete();
        await run;

        Assert.Single(sink.Appends);
        Assert.Equal(2, sink.Appends[0].Count);

        TradePipelineStatsSnapshot snapshot = stats.GetSnapshot();
        Assert.Equal(3, snapshot.TradesReceived);
        Assert.Equal(2, snapshot.TradesWritten);
        Assert.Equal(1, snapshot.DuplicatesDropped);
        Assert.Equal(1, snapshot.BatchesFlushed);
    }

    [Fact]
    public async Task Distinct_trades_pass_through()
    {
        RecordingTradeSink sink = new RecordingTradeSink();
        NoOpIngestionStateStore stateStore = new NoOpIngestionStateStore();
        TradePipelineStats stats = new TradePipelineStats();

        CollectorOptions options = new CollectorOptions(
            Instruments: new[] { Instrument.Parse("BTC-USDT") },
            ChannelCapacity: 10,
            BatchSize: 10,
            FlushInterval: TimeSpan.FromHours(1),
            DeduplicationCapacity: 100);

        InMemoryTradeDeduplicator deduplicator = new InMemoryTradeDeduplicator(options);
        TradeIngestWorker worker = new TradeIngestWorker(sink, stateStore, deduplicator, stats, new TestLogMachinaFactory());

        Channel<TradeInfo> channel = Channel.CreateUnbounded<TradeInfo>();
        Task run = worker.RunAsync(channel.Reader, options, CancellationToken.None);

        Instrument instrument = options.Instruments[0];
        DateTimeOffset t0 = new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero);

        await channel.Writer.WriteAsync(CreateTrade("0", instrument, t0));
        await channel.Writer.WriteAsync(CreateTrade("1", instrument, t0.AddSeconds(1)));
        await channel.Writer.WriteAsync(CreateTrade("2", instrument, t0.AddSeconds(2)));

        channel.Writer.Complete();
        await run;

        Assert.Single(sink.Appends);
        Assert.Equal(3, sink.Appends[0].Count);

        TradePipelineStatsSnapshot snapshot = stats.GetSnapshot();
        Assert.Equal(3, snapshot.TradesReceived);
        Assert.Equal(3, snapshot.TradesWritten);
        Assert.Equal(0, snapshot.DuplicatesDropped);
    }

    [Fact]
    public void Bounded_cache_eviction_allows_old_duplicates_after_window()
    {
        Instrument instrument = Instrument.Parse("BTC-USDT");

        CollectorOptions options = new CollectorOptions(
            Instruments: new[] { instrument },
            ChannelCapacity: 10,
            BatchSize: 10,
            FlushInterval: TimeSpan.FromHours(1),
            DeduplicationCapacity: 2);

        InMemoryTradeDeduplicator deduplicator = new InMemoryTradeDeduplicator(options);

        TradeKey k1 = new TradeKey(new ExchangeId("Stub"), instrument, "1");
        TradeKey k2 = new TradeKey(new ExchangeId("Stub"), instrument, "2");
        TradeKey k3 = new TradeKey(new ExchangeId("Stub"), instrument, "3");

        Assert.True(deduplicator.TryAccept(k1));
        Assert.True(deduplicator.TryAccept(k2));
        Assert.False(deduplicator.TryAccept(k1));

        Assert.True(deduplicator.TryAccept(k3));
        Assert.True(deduplicator.TryAccept(k1));
    }

    [Fact]
    public async Task Shutdown_flush_still_writes_uniques()
    {
        RecordingTradeSink sink = new RecordingTradeSink();
        NoOpIngestionStateStore stateStore = new NoOpIngestionStateStore();
        TradePipelineStats stats = new TradePipelineStats();

        CollectorOptions options = new CollectorOptions(
            Instruments: new[] { Instrument.Parse("BTC-USDT") },
            ChannelCapacity: 10,
            BatchSize: 1000,
            FlushInterval: TimeSpan.FromHours(1),
            DeduplicationCapacity: 100);

        InMemoryTradeDeduplicator deduplicator = new InMemoryTradeDeduplicator(options);
        TradeIngestWorker worker = new TradeIngestWorker(sink, stateStore, deduplicator, stats, new TestLogMachinaFactory());

        Channel<TradeInfo> channel = Channel.CreateUnbounded<TradeInfo>();
        Task run = worker.RunAsync(channel.Reader, options, CancellationToken.None);

        Instrument instrument = options.Instruments[0];
        DateTimeOffset t0 = new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero);

        await channel.Writer.WriteAsync(CreateTrade("0", instrument, t0));
        await channel.Writer.WriteAsync(CreateTrade("0", instrument, t0));
        await channel.Writer.WriteAsync(CreateTrade("1", instrument, t0.AddSeconds(1)));

        channel.Writer.Complete();
        await run;

        Assert.Single(sink.Appends);
        Assert.Equal(2, sink.Appends[0].Count);

        TradePipelineStatsSnapshot snapshot = stats.GetSnapshot();
        Assert.Equal(2, snapshot.TradesWritten);
        Assert.Equal(1, snapshot.DuplicatesDropped);
    }

    private static TradeInfo CreateTrade(string tradeId, Instrument instrument, DateTimeOffset timestamp)
    {
        TradeKey key = new TradeKey(new ExchangeId("Stub"), instrument, tradeId);
        return new TradeInfo(key, timestamp, price: 1m, quantity: 1m);
    }
}

