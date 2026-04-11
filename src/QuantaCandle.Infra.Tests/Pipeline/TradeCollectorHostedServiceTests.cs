using System.Runtime.CompilerServices;
using LogMachina;

using Moq;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Options;
using QuantaCandle.Infra.Pipeline;

namespace QuantaCandle.Infra.Tests.Pipeline;

/// <summary>
/// Verifies recorder startup work executes before live collection begins.
/// </summary>
public sealed class TradeCollectorHostedServiceTests
{
    [Fact]
    public async Task StartupDiscoveryRunsBeforeLiveIngestBegins()
    {
        var options = new CollectorOptions(
            Instruments: ["BTC-USDT"],
            ChannelCapacity: 10,
            BatchSize: 1,
            FlushInterval: TimeSpan.FromHours(1),
            CheckpointInterval: TimeSpan.FromHours(1));
        var startupTask = new RecordingStartupTask();
        var tradeSource = new ObservingTradeSource(startupTask);
        var retryOptions = new RetryOptions(TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(10));
        var ingestWorker = CreateWorker(options);
        var logMoq = new Mock<ILogMachina<TradeCollectorHostedService>>();
        using var hostedService = new TradeCollectorHostedService(options, retryOptions, tradeSource, startupTask, ingestWorker, logMoq.Object);

        await hostedService.StartAsync(CancellationToken.None);
        await tradeSource.CollectionStarted.Task.WaitAsync(TimeSpan.FromSeconds(3));
        await hostedService.StopAsync(CancellationToken.None);

        Assert.Equal(1, startupTask.RunCallCount);
        Assert.Equal(new ExchangeId("Stub"), startupTask.Exchange);
        Assert.True(tradeSource.StartupCompletedWhenCollectionStarted);
    }

    [Fact]
    public async Task IngestFaultDuringRuntimeCancelsCollectorWork()
    {
        var options = new CollectorOptions(
            Instruments: ["BTC-USDT"],
            ChannelCapacity: 10,
            BatchSize: 1,
            FlushInterval: TimeSpan.FromHours(1),
            CheckpointInterval: TimeSpan.FromHours(1));
        var startupTask = new RecordingStartupTask();
        var tradeSource = new RuntimeFailureObservingTradeSource();
        var retryOptions = new RetryOptions(TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(10));
        var failure = new InvalidOperationException("Ingest worker failed.");
        var ingestWorker = CreateWorker(options, new InMemoryIngestionStateStore(), new CheckpointSignal(), new ThrowingCheckpointLifecycle(failure), 1024, out _, out _);
        var logMoq = new Mock<ILogMachina<TradeCollectorHostedService>>();
        using var hostedService = new TradeCollectorHostedService(options, retryOptions, tradeSource, startupTask, ingestWorker, logMoq.Object);

        await hostedService.StartAsync(CancellationToken.None);
        await tradeSource.CollectionStarted.Task.WaitAsync(TimeSpan.FromSeconds(3));

        await tradeSource.CollectionCanceled.Task.WaitAsync(TimeSpan.FromSeconds(3));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => hostedService.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(3)));

        Assert.Same(failure, exception);
    }

    [Fact]
    public async Task IngestFaultDuringRuntimeDoesNotWaitForShutdownToEscapeHostedServicePath()
    {
        var options = new CollectorOptions(
            Instruments: ["BTC-USDT"],
            ChannelCapacity: 10,
            BatchSize: 1,
            FlushInterval: TimeSpan.FromHours(1),
            CheckpointInterval: TimeSpan.FromHours(1));
        var startupTask = new RecordingStartupTask();
        var tradeSource = new RuntimeFailureObservingTradeSource();
        var retryOptions = new RetryOptions(TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(10));
        var failure = new InvalidOperationException("Ingest worker failed.");
        var ingestWorker = CreateWorker(options, new InMemoryIngestionStateStore(), new CheckpointSignal(), new ThrowingCheckpointLifecycle(failure), 1024, out _, out _);
        var logMoq = new Mock<ILogMachina<TradeCollectorHostedService>>();
        using var hostedService = new TradeCollectorHostedService(options, retryOptions, tradeSource, startupTask, ingestWorker, logMoq.Object);

        await hostedService.StartAsync(CancellationToken.None);
        await tradeSource.CollectionStarted.Task.WaitAsync(TimeSpan.FromSeconds(3));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => hostedService.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(3)));

        Assert.Same(failure, exception);
        Assert.True(tradeSource.CollectionCanceled.Task.IsCompleted);
    }

    private static TradeIngestWorker CreateWorker(CollectorOptions options)
    {
        var stateStore = new InMemoryIngestionStateStore();
        var checkpointSignal = new CheckpointSignal();
        var checkpointLifecycle = new NullTradeCheckpointLifecycle();
        var deduplicator = new InMemoryTradeDeduplicator(options);
        var stats = new TradePipelineStats();
        var logMoq = new Mock<ILogMachina<TradeIngestWorker>>();
        var result = new TradeIngestWorker(stateStore, checkpointSignal, checkpointLifecycle, new TradeCheckpointTriggerOptions(1024), deduplicator, stats, logMoq.Object);
        return result;
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

        var result = new TradeIngestWorker(ingestionStateStore, checkpointSignal, tradeCheckpointLifecycle, new TradeCheckpointTriggerOptions(cacheSize), deduplicator, stats, logMoq.Object);
        return result;
    }

    private sealed class RecordingStartupTask : ITradeRecorderStartupTask
    {
        public bool Completed { get; private set; }

        public ExchangeId? Exchange { get; private set; }

        public int RunCallCount { get; private set; }

        public ValueTask Run(ExchangeId exchange, IReadOnlyList<Instrument> instruments, CancellationToken cancellationToken)
        {
            RunCallCount++;
            Exchange = exchange;
            Completed = true;

            var result = ValueTask.CompletedTask;
            return result;
        }
    }

    private sealed class ObservingTradeSource(RecordingStartupTask startupTask) : ITradeSource
    {
        public ExchangeId Exchange { get; } = new("Stub");

        public TaskCompletionSource CollectionStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool StartupCompletedWhenCollectionStarted { get; private set; }

        public async IAsyncEnumerable<TradeInfo> GetLiveTrades(Instrument instrument, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            StartupCompletedWhenCollectionStarted = startupTask.Completed;
            CollectionStarted.TrySetResult();

            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }

            yield break;
        }

        public async IAsyncEnumerable<TradeInfo> GetBackfillTrades(Instrument instrument, TradeWatermark? fromExclusive, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private sealed class RuntimeFailureObservingTradeSource : ITradeSource
    {
        public ExchangeId Exchange { get; } = new("Stub");

        public TaskCompletionSource CollectionCanceled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource CollectionStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async IAsyncEnumerable<TradeInfo> GetLiveTrades(Instrument instrument, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            CollectionStarted.TrySetResult();
            yield return new TradeInfo(new TradeKey(Exchange, instrument, "1"), new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero), 1m, 1m);

            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                CollectionCanceled.TrySetResult();
            }
        }

        public async IAsyncEnumerable<TradeInfo> GetBackfillTrades(Instrument instrument, TradeWatermark? fromExclusive, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private sealed class ThrowingCheckpointLifecycle(Exception exception) : ITradeCheckpointLifecycle
    {
        public ValueTask<bool> CheckpointActive(CancellationToken cancellationToken)
        {
            var result = ValueTask.FromResult(false);
            return result;
        }

        public ValueTask<bool> DispatchActiveSnapshot(CancellationToken cancellationToken)
        {
            var result = ValueTask.FromResult(false);
            return result;
        }

        public ValueTask FlushOnShutdown(CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask<int> TrackAppendedTrades(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
        {
            throw exception;
        }
    }
}
