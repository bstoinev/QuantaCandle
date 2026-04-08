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
        Assert.True(tradeSource.StartupCompletedWhenCollectionStarted);
    }

    private static TradeIngestWorker CreateWorker(CollectorOptions options)
    {
        var stateStore = new InMemoryIngestionStateStore();
        var checkpointSignal = new CheckpointSignal();
        var checkpointLifecycle = new NullTradeCheckpointLifecycle();
        var deduplicator = new InMemoryTradeDeduplicator(options);
        var stats = new TradePipelineStats();
        var logMoq = new Mock<ILogMachina<TradeIngestWorker>>();
        var result = new TradeIngestWorker(stateStore, checkpointSignal, checkpointLifecycle, deduplicator, stats, logMoq.Object);
        return result;
    }

    private sealed class RecordingStartupTask : ITradeRecorderStartupTask
    {
        public bool Completed { get; private set; }

        public int RunCallCount { get; private set; }

        public ValueTask Run(IReadOnlyList<Instrument> instruments, CancellationToken cancellationToken)
        {
            RunCallCount++;
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

        public async IAsyncEnumerable<TradeInfo> GetLiveTradesAsync(Instrument instrument, [EnumeratorCancellation] CancellationToken cancellationToken)
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

        public async IAsyncEnumerable<TradeInfo> GetBackfillTradesAsync(Instrument instrument, TradeWatermark? fromExclusive, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }
    }
}
