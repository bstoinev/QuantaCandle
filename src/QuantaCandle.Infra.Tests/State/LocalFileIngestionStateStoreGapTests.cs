using System.Collections.Concurrent;
using System.Reflection;
using System.Threading.Channels;

using LogMachina;

using Moq;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Options;
using QuantaCandle.Infra.Pipeline;

namespace QuantaCandle.Infra.Tests.State;

/// <summary>
/// Verifies compact runtime gap retention in the local-file ingestion state store.
/// </summary>
public sealed class LocalFileIngestionStateStoreGapTests
{
    [Fact]
    public async Task RecordGapAsyncKeepsOnlyLatestSnapshotPerGapId()
    {
        var store = new LocalFileIngestionStateStore(CreateTempDirectory(), CreateClockMoq().Object);
        var gapId = Guid.NewGuid();
        var openGap = TradeGap.CreateOpen(
            gapId,
            new ExchangeId("Stub"),
            Instrument.Parse("BTC-USDT"),
            new TradeWatermark("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
            new DateTimeOffset(2026, 3, 12, 0, 0, 5, TimeSpan.Zero));
        var boundedGap = openGap.ToBounded(
            new TradeWatermark("4", new DateTimeOffset(2026, 3, 12, 0, 0, 8, TimeSpan.Zero)),
            new MissingTradeIdRange(2, 3));

        await store.RecordGapAsync(openGap, TestContext.Current.CancellationToken);
        await store.RecordGapAsync(boundedGap, TestContext.Current.CancellationToken);

        var gaps = await store.GetGapsAsync(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), TestContext.Current.CancellationToken);
        var gap = Assert.Single(gaps);

        Assert.Equal(TradeGapStatus.Bounded, gap.Status);
        Assert.Equal(gapId, gap.GapId);
        Assert.Equal(new MissingTradeIdRange(2, 3), gap.MissingTradeIds);
    }

    [Fact]
    public void LocalFileIngestionStateStoreUsesConcurrentDictionaryGapCacheInsteadOfAppendOnlyLists()
    {
        var field = typeof(LocalFileIngestionStateStore).GetField("gapsByInstrument", BindingFlags.Instance | BindingFlags.NonPublic);

        Assert.NotNull(field);
        Assert.Equal(
            typeof(ConcurrentDictionary<(ExchangeId Exchange, Instrument Symbol), ConcurrentDictionary<Guid, TradeGap>>),
            field!.FieldType);
    }

    [Fact]
    public async Task TradeIngestWorkerStillExposesBoundedGapStateThroughLocalFileIngestionStateStore()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var instrument = Instrument.Parse("BTC-USDT");
            var options = new CollectorOptions(
                Instruments: [instrument],
                ChannelCapacity: 10,
                BatchSize: 2,
                FlushInterval: TimeSpan.FromHours(1),
                CheckpointInterval: TimeSpan.FromHours(1));
            var store = new LocalFileIngestionStateStore(localRoot, CreateClockMoq().Object);
            var worker = new TradeIngestWorker(
                store,
                new CheckpointSignal(),
                new RecordingCheckpointLifecycle(),
                new TradeCheckpointTriggerOptions(1024),
                new InMemoryTradeDeduplicator(options),
                new TradePipelineStats(),
                CreateLogMoq().Object);
            var channel = Channel.CreateUnbounded<TradeInfo>();

            var run = worker.Run(channel.Reader, options, TestContext.Current.CancellationToken);

            await channel.Writer.WriteAsync(CreateTrade("1", instrument, new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)), TestContext.Current.CancellationToken);
            await channel.Writer.WriteAsync(CreateTrade("4", instrument, new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)), TestContext.Current.CancellationToken);

            channel.Writer.Complete();
            await run;

            var gaps = await store.GetGapsAsync(new ExchangeId("Stub"), instrument, TestContext.Current.CancellationToken);
            var gap = Assert.Single(gaps);

            Assert.Equal(TradeGapStatus.Bounded, gap.Status);
            Assert.Equal("1", gap.FromExclusive.TradeId);
            Assert.Equal("4", gap.ToInclusive?.TradeId);
            Assert.Equal(new MissingTradeIdRange(2, 3), gap.MissingTradeIds);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    private static Mock<IClock> CreateClockMoq()
    {
        var result = new Mock<IClock>();
        result
            .SetupGet(mock => mock.UtcNow)
            .Returns(new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero));
        return result;
    }

    private static Mock<ILogMachina<TradeIngestWorker>> CreateLogMoq()
    {
        var result = new Mock<ILogMachina<TradeIngestWorker>>();
        return result;
    }

    private static TradeInfo CreateTrade(string tradeId, Instrument instrument, DateTimeOffset timestamp)
    {
        var key = new TradeKey(new ExchangeId("Stub"), instrument, tradeId);
        var result = new TradeInfo(key, timestamp, 1m, 1m);
        return result;
    }

    private static string CreateTempDirectory()
    {
        var result = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(result);
        return result;
    }

    private static void DeleteDirectory(string path)
    {
        if (Directory.Exists(path))
        {
            Directory.Delete(path, recursive: true);
        }
    }

    /// <summary>
    /// Minimal lifecycle stub for exercising gap persistence through the worker.
    /// </summary>
    private sealed class RecordingCheckpointLifecycle : ITradeCheckpointLifecycle
    {
        private int _trackedTradeCount;

        public ValueTask<int> TrackAppendedTrades(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
        {
            _trackedTradeCount += trades.Count;
            return ValueTask.FromResult(_trackedTradeCount);
        }

        public ValueTask<bool> CheckpointActive(CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(false);
        }

        public ValueTask<bool> DispatchActiveSnapshot(CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(false);
        }

        public ValueTask FlushOnShutdown(CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }
    }
}
