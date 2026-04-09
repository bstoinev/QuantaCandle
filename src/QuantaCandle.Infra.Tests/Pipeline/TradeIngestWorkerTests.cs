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
        var checkpointLifecycle = new RecordingCheckpointLifecycle();
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), new CheckpointSignal(), checkpointLifecycle, 1024, out var stats, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("0", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("1", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("2", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)));

        channel.Writer.Complete();
        await run;

        Assert.Single(checkpointLifecycle.TrackedTradeBatches);
        Assert.Equal(3, checkpointLifecycle.TrackedTradeBatches[0].Count);

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
        var checkpointLifecycle = new RecordingCheckpointLifecycle();
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), new CheckpointSignal(), checkpointLifecycle, 1024, out var stats, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("0", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("1", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));

        channel.Writer.Complete();
        await run;

        Assert.Single(checkpointLifecycle.TrackedTradeBatches);
        Assert.Equal(2, checkpointLifecycle.TrackedTradeBatches[0].Count);

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
        var checkpointLifecycle = new RecordingCheckpointLifecycle();
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), new CheckpointSignal(), checkpointLifecycle, cacheSize: 10, out _, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("0", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        channel.Writer.Complete();
        await run;

        Assert.Equal(1, checkpointLifecycle.ShutdownFlushCallCount);
    }

    [Fact]
    public async Task SuccessfulCheckpointLogsOneStatisticsMessage()
    {
        using var stoppingCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var options = CreateOptions(batchSize: 3, checkpointInterval: TimeSpan.FromMilliseconds(25));
        var checkpointLifecycle = new RecordingCheckpointLifecycle
        {
            CheckpointResult = true,
            OnCheckpoint = stoppingCts.Cancel,
        };
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), new CheckpointSignal(), checkpointLifecycle, 1024, out _, out var logMoq);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        await worker.Run(channel.Reader, options, stoppingCts.Token);

        logMoq.Verify(
            mock => mock.Info(
                It.Is<string>(message =>
                    message.Contains("Trades received:", StringComparison.Ordinal)
                    && message.Contains("Trades written:", StringComparison.Ordinal)
                    && message.Contains("Duplicates dropped:", StringComparison.Ordinal)
                    && message.Contains("Batches flushed:", StringComparison.Ordinal))),
            Times.Once);
    }

    [Fact]
    public async Task CheckpointThatDoesNotCompleteDoesNotLogStatistics()
    {
        using var stoppingCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var options = CreateOptions(batchSize: 3, checkpointInterval: TimeSpan.FromMilliseconds(25));
        var checkpointLifecycle = new RecordingCheckpointLifecycle
        {
            CheckpointResult = false,
            OnCheckpoint = stoppingCts.Cancel,
        };
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), new CheckpointSignal(), checkpointLifecycle, 1024, out _, out var logMoq);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        await worker.Run(channel.Reader, options, stoppingCts.Token);

        logMoq.Verify(mock => mock.Info(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task NotReachedCheckpointDoesNotLogStatistics()
    {
        using var stoppingCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var options = CreateOptions(batchSize: 3, checkpointInterval: TimeSpan.FromHours(1));
        var checkpointLifecycle = new RecordingCheckpointLifecycle();
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), new CheckpointSignal(), checkpointLifecycle, cacheSize: 10, out _, out var logMoq);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        await worker.Run(channel.Reader, options, stoppingCts.Token);

        Assert.Equal(0, checkpointLifecycle.CheckpointCallCount);
        logMoq.Verify(mock => mock.Info(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task PeriodicCheckpointUsesCheckpointIntervalInsteadOfFlushInterval()
    {
        using var stoppingCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var options = CreateOptions(
            batchSize: 3,
            flushInterval: TimeSpan.FromMilliseconds(1),
            checkpointInterval: TimeSpan.FromMilliseconds(25));
        var checkpointLifecycle = new RecordingCheckpointLifecycle
        {
            OnCheckpoint = stoppingCts.Cancel,
        };
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), new CheckpointSignal(), checkpointLifecycle, 1024, out _, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        await worker.Run(channel.Reader, options, stoppingCts.Token);

        Assert.Equal(1, checkpointLifecycle.CheckpointCallCount);
    }

    [Fact]
    public async Task ManualCheckpointSignalCausesImmediateCheckpointWithoutWaitingForTimer()
    {
        using var stoppingCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var checkpointSignal = new CheckpointSignal();
        var options = CreateOptions(batchSize: 3, checkpointInterval: TimeSpan.FromHours(1));
        var checkpointLifecycle = new RecordingCheckpointLifecycle
        {
            OnCheckpoint = stoppingCts.Cancel,
        };
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), checkpointSignal, checkpointLifecycle, cacheSize: 10, out _, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, stoppingCts.Token);

        checkpointSignal.Signal();

        await run;

        Assert.Equal(1, checkpointLifecycle.CheckpointCallCount);
    }

    [Fact]
    public async Task CacheThresholdDoesNotCheckpointBelowLimit()
    {
        var options = CreateOptions(batchSize: 1, checkpointInterval: TimeSpan.FromHours(1));
        var checkpointLifecycle = new RecordingCheckpointLifecycle();
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), new CheckpointSignal(), checkpointLifecycle, cacheSize: 3, out _, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("0", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("1", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));

        channel.Writer.Complete();
        await run;

        Assert.Equal(0, checkpointLifecycle.CheckpointCallCount);
    }

    [Fact]
    public async Task CacheThresholdTriggersCheckpointExactlyAtLimit()
    {
        using var stoppingCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var options = CreateOptions(batchSize: 1, checkpointInterval: TimeSpan.FromHours(1));
        var checkpointLifecycle = new RecordingCheckpointLifecycle
        {
            OnCheckpoint = stoppingCts.Cancel,
        };
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), new CheckpointSignal(), checkpointLifecycle, cacheSize: 2, out _, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, stoppingCts.Token);

        await channel.Writer.WriteAsync(CreateTrade("0", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("1", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));

        await run;

        Assert.Equal(1, checkpointLifecycle.CheckpointCallCount);
    }

    [Fact]
    public async Task CacheThresholdQueuesOnlyOneCheckpointBeforeFirstRequestIsHandled()
    {
        using var stoppingCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var options = CreateOptions(batchSize: 1, checkpointInterval: TimeSpan.FromHours(1));
        var checkpointLifecycle = new RecordingCheckpointLifecycle
        {
            OnCheckpoint = stoppingCts.Cancel,
        };
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), new CheckpointSignal(), checkpointLifecycle, cacheSize: 2, out _, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, stoppingCts.Token);

        await channel.Writer.WriteAsync(CreateTrade("0", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("1", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)));
        await channel.Writer.WriteAsync(CreateTrade("2", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)));

        await run;

        Assert.Equal(1, checkpointLifecycle.CheckpointCallCount);
    }

    [Fact]
    public async Task ManualCheckpointSignalStillRunsBelowCacheThreshold()
    {
        using var stoppingCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var checkpointSignal = new CheckpointSignal();
        var options = CreateOptions(batchSize: 1, checkpointInterval: TimeSpan.FromHours(1));
        var checkpointLifecycle = new RecordingCheckpointLifecycle
        {
            OnCheckpoint = stoppingCts.Cancel,
        };
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), checkpointSignal, checkpointLifecycle, cacheSize: 3, out _, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, stoppingCts.Token);

        await channel.Writer.WriteAsync(CreateTrade("0", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        checkpointSignal.Signal();

        await run;

        Assert.Equal(1, checkpointLifecycle.CheckpointCallCount);
    }

    [Fact]
    public async Task SnapshotCheckpointRunsNormalCheckpointBeforeSnapshotDispatch()
    {
        using var stoppingCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var checkpointSignal = new CheckpointSignal();
        var options = CreateOptions(batchSize: 1, checkpointInterval: TimeSpan.FromHours(1));
        var checkpointLifecycle = new RecordingCheckpointLifecycle
        {
            CheckpointResult = true,
            OnSnapshotDispatch = stoppingCts.Cancel,
        };
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), checkpointSignal, checkpointLifecycle, cacheSize: 3, out _, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, stoppingCts.Token);

        await channel.Writer.WriteAsync(CreateTrade("0", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));
        checkpointSignal.Signal(CheckpointRequestKind.Snapshot);

        await run;

        Assert.Equal(["TrackAppendedTrades", "CheckpointActive", "DispatchActiveSnapshot", "FlushOnShutdown"], checkpointLifecycle.Events);
        Assert.Equal(1, checkpointLifecycle.CheckpointCallCount);
        Assert.Equal(1, checkpointLifecycle.SnapshotDispatchCallCount);
    }

    [Fact]
    public async Task SnapshotCheckpointDoesNotDispatchSnapshotWhenCheckpointFails()
    {
        using var stoppingCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var checkpointSignal = new CheckpointSignal();
        var options = CreateOptions(batchSize: 1, checkpointInterval: TimeSpan.FromHours(1));
        var checkpointLifecycle = new RecordingCheckpointLifecycle
        {
            CheckpointResult = false,
            OnCheckpoint = stoppingCts.Cancel,
        };
        var worker = CreateWorker(options, new InMemoryIngestionStateStore(), checkpointSignal, checkpointLifecycle, cacheSize: 3, out _, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, stoppingCts.Token);

        checkpointSignal.Signal(CheckpointRequestKind.Snapshot);

        await run;

        Assert.Equal(1, checkpointLifecycle.CheckpointCallCount);
        Assert.Equal(0, checkpointLifecycle.SnapshotDispatchCallCount);
    }

    [Fact]
    public async Task LiveIngestionBeginsImmediatelyWithoutWaitingForRecovery()
    {
        var options = CreateOptions(batchSize: 1);
        var appendObserved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stateStore = new BlockingIngestionStateStore();
        var checkpointLifecycle = new RecordingCheckpointLifecycle { AppendObserved = appendObserved };
        var worker = CreateWorker(options, stateStore, new CheckpointSignal(), checkpointLifecycle, 1024, out _, out _);
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
        var worker = CreateWorker(options, stateStore, out _, out _);
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
        var worker = CreateWorker(options, stateStore, out _, out _);
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

        var worker = CreateWorker(options, stateStore, out _, out _);
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
    public async Task StartupResumeBoundaryCreatesGapWithoutManufacturingFakeTrade()
    {
        var options = CreateOptions(batchSize: 1);
        var stateStore = new InMemoryIngestionStateStore();
        stateStore.SetResumeBoundary(
            new ExchangeId("Stub"),
            options.Instruments[0],
            new ResumeBoundary(
                new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero),
                new DateOnly(2026, 3, 12),
                "LatestLocalDailyFile"));

        var worker = CreateWorker(options, stateStore, out _, out _);
        var channel = Channel.CreateUnbounded<TradeInfo>();

        var run = worker.Run(channel.Reader, options, CancellationToken.None);

        await channel.Writer.WriteAsync(CreateTrade("105", options.Instruments[0], new DateTimeOffset(2026, 3, 12, 0, 0, 5, TimeSpan.Zero)));

        channel.Writer.Complete();
        await run;

        var gaps = await stateStore.GetGapsAsync(new ExchangeId("Stub"), options.Instruments[0], CancellationToken.None);

        var gap = Assert.Single(gaps);
        Assert.Equal(TradeGapStatus.Bounded, gap.Status);
        Assert.Equal("105", gap.FromExclusive.TradeId);
        Assert.Equal(new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero), gap.FromExclusive.Timestamp);
        Assert.Equal("105", gap.ToInclusive?.TradeId);
        Assert.Null(gap.MissingTradeIds);
    }

    [Fact]
    public async Task DuplicateOrOutOfOrderEventsDoNotCreateFalseGaps()
    {
        var options = CreateOptions(batchSize: 4);
        var stateStore = new InMemoryIngestionStateStore();
        var worker = CreateWorker(options, stateStore, out _, out _);
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

    private static CollectorOptions CreateOptions(int batchSize, TimeSpan? flushInterval = null, TimeSpan? checkpointInterval = null)
    {
        var options = new CollectorOptions(
            Instruments: ["BTC-USDT"],
            ChannelCapacity: 10,
            BatchSize: batchSize,
            FlushInterval: flushInterval ?? TimeSpan.FromHours(1),
            CheckpointInterval: checkpointInterval ?? TimeSpan.FromHours(1));
        return options;
    }

    private static TradeIngestWorker CreateWorker(
        CollectorOptions options,
        IIngestionStateStore ingestionStateStore,
        ICheckpointSignal checkpointSignal,
        ITradeCheckpointLifecycle tradeCheckpointLifecycle,
        int cacheSize,
        out TradePipelineStats stats,
        out Mock<ILogMachina<TradeIngestWorker>> logMoq)
    {
        stats = new TradePipelineStats();
        var deduplicator = new InMemoryTradeDeduplicator(options);
        logMoq = new Mock<ILogMachina<TradeIngestWorker>>();

        var worker = new TradeIngestWorker(ingestionStateStore, checkpointSignal, tradeCheckpointLifecycle, new TradeCheckpointTriggerOptions(cacheSize), deduplicator, stats, logMoq.Object);

        return worker;
    }

    private static TradeIngestWorker CreateWorker(
        CollectorOptions options,
        IIngestionStateStore ingestionStateStore,
        out TradePipelineStats stats,
        out Mock<ILogMachina<TradeIngestWorker>> logMoq)
    {
        return CreateWorker(options, ingestionStateStore, new CheckpointSignal(), new RecordingCheckpointLifecycle(), 1024, out stats, out logMoq);
    }

    private static TradeInfo CreateTrade(string tradeId, Instrument instrument, DateTimeOffset timestamp)
    {
        var key = new TradeKey(new ExchangeId("Stub"), instrument, tradeId);
        return new TradeInfo(key, timestamp, price: 1m, quantity: 1m);
    }

    private sealed class BlockingIngestionStateStore : IIngestionStateStore
    {
        private readonly TaskCompletionSource<ResumeBoundary?> resumeBoundary;

        public BlockingIngestionStateStore()
        {
            resumeBoundary = new TaskCompletionSource<ResumeBoundary?>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public bool GetWatermarkStarted { get; private set; }

        public ValueTask<ResumeBoundary?> GetResumeBoundaryAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
        {
            GetWatermarkStarted = true;
            return new ValueTask<ResumeBoundary?>(resumeBoundary.Task.WaitAsync(cancellationToken));
        }

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

        public ValueTask<ResumeBoundary?> GetResumeBoundaryAsync(ExchangeId exchange, Instrument symbol, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult<ResumeBoundary?>(null);
        }

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

    private sealed class RecordingCheckpointLifecycle : ITradeCheckpointLifecycle
    {
        private int _trackedTradeCount;

        public bool CheckpointResult { get; init; }

        public Action? OnCheckpoint { get; init; }

        public Action? OnSnapshotDispatch { get; init; }

        public int CheckpointCallCount { get; private set; }

        public int SnapshotDispatchCallCount { get; private set; }

        public int ShutdownFlushCallCount { get; private set; }

        public int TrackAppendedTradesCallCount { get; private set; }

        public TaskCompletionSource? AppendObserved { get; init; }

        public List<string> Events { get; } = [];

        public List<IReadOnlyList<TradeInfo>> TrackedTradeBatches { get; } = [];

        public ValueTask<int> TrackAppendedTrades(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
        {
            Events.Add(nameof(TrackAppendedTrades));
            TrackAppendedTradesCallCount++;
            TrackedTradeBatches.Add(trades.ToArray());
            _trackedTradeCount += trades.Count;
            AppendObserved?.TrySetResult();
            return ValueTask.FromResult(_trackedTradeCount);
        }

        public ValueTask<bool> CheckpointActive(CancellationToken cancellationToken)
        {
            Events.Add(nameof(CheckpointActive));
            CheckpointCallCount++;
            if (_trackedTradeCount > 0)
            {
                _trackedTradeCount = 1;
            }

            OnCheckpoint?.Invoke();

            var result = ValueTask.FromResult(CheckpointResult);
            return result;
        }

        public ValueTask<bool> DispatchActiveSnapshot(CancellationToken cancellationToken)
        {
            Events.Add(nameof(DispatchActiveSnapshot));
            SnapshotDispatchCallCount++;
            OnSnapshotDispatch?.Invoke();

            var result = ValueTask.FromResult(true);
            return result;
        }

        public ValueTask FlushOnShutdown(CancellationToken cancellationToken)
        {
            Events.Add(nameof(FlushOnShutdown));
            ShutdownFlushCallCount++;
            return ValueTask.CompletedTask;
        }
    }

}
