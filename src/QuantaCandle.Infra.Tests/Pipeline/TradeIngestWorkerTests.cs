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
    public async Task LiveIngestionBeginsImmediatelyWithoutWaitingForRecovery()
    {
        var options = CreateOptions(batchSize: 1);
        var appendObserved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stateStore = new BlockingIngestionStateStore();
        var worker = CreateWorker(options, new List<IReadOnlyList<TradeInfo>>(), stateStore, out _, appendObserved);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("101", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));

        await appendObserved.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.True(stateStore.GetWatermarkStarted);

        stateStore.ReleaseResumeBoundary();
        channel.Writer.Complete();
        await run;
    }

    [Fact]
    public async Task DiscontinuityOpensAndPersistsGapRecord()
    {
        var options = CreateOptions(batchSize: 2);
        var stateStore = new RecordingIngestionStateStore();
        var worker = CreateWorker(options, new List<IReadOnlyList<TradeInfo>>(), stateStore, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("1", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("4", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));

        channel.Writer.Complete();
        await run;

        Assert.Equal(2, stateStore.RecordedGaps.Count);
        Assert.Equal(TradeGapStatus.Open, stateStore.RecordedGaps[0].Status);
        Assert.Equal(TradeGapStatus.Bounded, stateStore.RecordedGaps[1].Status);
    }

    [Fact]
    public async Task GapBecomesBoundedWhenBothBordersAreKnown()
    {
        var options = CreateOptions(batchSize: 2);
        var stateStore = new InMemoryIngestionStateStore();
        var worker = CreateWorker(options, new List<IReadOnlyList<TradeInfo>>(), stateStore, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("1", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("4", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));

        channel.Writer.Complete();
        await run;

        var gaps = await stateStore.GetGapsAsync(new ExchangeId("Stub"), options.Instruments[0], CancellationToken.None);

        var gap = Assert.Single(gaps);
        Assert.Equal(TradeGapStatus.Bounded, gap.Status);
        Assert.NotNull(gap.ToInclusive);
        Assert.Equal("1", gap.FromExclusive.TradeId);
        Assert.Equal("4", gap.ToInclusive?.TradeId);
        Assert.Equal(new MissingTradeIdRange(2, 3), gap.MissingTradeIds);
    }

    [Fact]
    public async Task CheckpointSemanticsRemainIndependentFromBoundedGaps()
    {
        var options = CreateOptions(batchSize: 1);
        var stateStore = new InMemoryIngestionStateStore();
        await stateStore.SetWatermarkAsync(
            new ExchangeId("Stub"),
            options.Instruments[0],
            new TradeWatermark("100", new DateTimeOffset(2026, 3, 11, 23, 59, 59, TimeSpan.Zero)),
            CancellationToken.None);

        var worker = CreateWorker(options, new List<IReadOnlyList<TradeInfo>>(), stateStore, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("105", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));

        channel.Writer.Complete();
        await run;

        var checkpoint = await stateStore.GetWatermarkAsync(new ExchangeId("Stub"), options.Instruments[0], CancellationToken.None);
        var gaps = await stateStore.GetGapsAsync(new ExchangeId("Stub"), options.Instruments[0], CancellationToken.None);

        Assert.Equal("105", checkpoint?.TradeId);
        Assert.Single(gaps);
        Assert.Equal(TradeGapStatus.Bounded, gaps[0].Status);
        Assert.Equal(new MissingTradeIdRange(101, 104), gaps[0].MissingTradeIds);
    }

    [Fact]
    public async Task DuplicateOrOutOfOrderEventsDoNotCreateFalseGaps()
    {
        var options = CreateOptions(batchSize: 4);
        var stateStore = new InMemoryIngestionStateStore();
        var worker = CreateWorker(options, new List<IReadOnlyList<TradeInfo>>(), stateStore, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("101", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("101", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("100", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("102", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)));

        channel.Writer.Complete();
        await run;

        var gaps = await stateStore.GetGapsAsync(new ExchangeId("Stub"), options.Instruments[0], CancellationToken.None);

        Assert.Empty(gaps);
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

        stats = new TradePipelineStats();
        var deduplicator = new InMemoryTradeDeduplicator(options);
        var logMoq = new Mock<ILogMachina<TradeIngestWorker>>();

        var worker = new TradeIngestWorker(tradeSinkMoq.Object, ingestionStateStore, deduplicator, stats, logMoq.Object);
        return worker;
    }

    private static TradeInfo CreateTrade(string tradeId, Instrument instrument, DateTimeOffset timestamp)
    {
        var key = new TradeKey(new ExchangeId("Stub"), instrument, tradeId);
        return new TradeInfo(key, timestamp, price: 1m, quantity: 1m);
    }

    private sealed class BlockingIngestionStateStore : IIngestionStateStore
    {
        private readonly TaskCompletionSource<TradeWatermark?> resumeBoundary;

        public BlockingIngestionStateStore()
        {
            resumeBoundary = new TaskCompletionSource<TradeWatermark?>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public bool GetWatermarkStarted { get; private set; }

        public ValueTask<TradeWatermark?> GetWatermarkAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
        {
            GetWatermarkStarted = true;
            return new ValueTask<TradeWatermark?>(resumeBoundary.Task.WaitAsync(cancellationToken));
        }

        public ValueTask SetWatermarkAsync(ExchangeId exchange, Instrument symbol, TradeWatermark watermark, CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask RecordGapAsync(TradeGap gap, CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask<IReadOnlyList<TradeGap>> GetGapsAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult<IReadOnlyList<TradeGap>>(Array.Empty<TradeGap>());
        }

        public void ReleaseResumeBoundary()
        {
            resumeBoundary.TrySetResult(null);
        }
    }

    private sealed class RecordingIngestionStateStore : IIngestionStateStore
    {
        public List<TradeGap> RecordedGaps { get; } = new();

        public ValueTask<TradeWatermark?> GetWatermarkAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult<TradeWatermark?>(null);
        }

        public ValueTask SetWatermarkAsync(ExchangeId exchange, Instrument symbol, TradeWatermark watermark, CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask RecordGapAsync(TradeGap gap, CancellationToken cancellationToken)
        {
            RecordedGaps.Add(gap);
            return ValueTask.CompletedTask;
        }

        public ValueTask<IReadOnlyList<TradeGap>> GetGapsAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult<IReadOnlyList<TradeGap>>(RecordedGaps);
        }
    }
}
