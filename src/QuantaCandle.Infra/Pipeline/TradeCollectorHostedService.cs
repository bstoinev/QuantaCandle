using System.Threading.Channels;
using System.Runtime.ExceptionServices;
using LogMachina;
using Microsoft.Extensions.Hosting;
using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Options;

namespace QuantaCandle.Infra.Pipeline;

public sealed class TradeCollectorHostedService(
    CollectorOptions options,
    RetryOptions retryOptions,
    ITradeSource tradeSource,
    ITradeRecorderStartupTask tradeRecorderStartupTask,
    TradeIngestWorker ingestWorker,
    ILogMachina<TradeCollectorHostedService> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await tradeRecorderStartupTask.Run(tradeSource.Exchange, options.Instruments, stoppingToken).ConfigureAwait(false);

        using var collectorCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var collectorToken = collectorCts.Token;

        var pipelines = new List<(Channel<TradeInfo> Channel, Task CollectorTask, Task IngestTask)>(options.Instruments.Count);

        foreach (Instrument instrument in options.Instruments)
        {
            var channel = BoundedChannelFactory.CreateTradeChannel(options.ChannelCapacity);
            var collectorTask = CollectInstrument(instrument, channel.Writer, collectorToken);
            var ingestTask = ingestWorker.Run(channel.Reader, options, stoppingToken);
            pipelines.Add((channel, collectorTask, ingestTask));
        }

        await MonitorRuntime(pipelines, collectorCts, stoppingToken).ConfigureAwait(false);
    }

    private async Task MonitorRuntime(
        IReadOnlyList<(Channel<TradeInfo> Channel, Task CollectorTask, Task IngestTask)> pipelines,
        CancellationTokenSource collectorCts,
        CancellationToken stoppingToken)
    {
        var activeCollectors = pipelines.Select(static pipeline => pipeline.CollectorTask).ToHashSet();
        var activeIngests = pipelines.Select(static pipeline => pipeline.IngestTask).ToHashSet();
        var shutdownTask = Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);

        while (activeCollectors.Count != 0 || activeIngests.Count != 0)
        {
            if (stoppingToken.IsCancellationRequested)
            {
                await Shutdown(pipelines, collectorCts).ConfigureAwait(false);
                break;
            }

            var monitoredTasks = new List<Task>(activeCollectors.Count + activeIngests.Count + 1);
            monitoredTasks.AddRange(activeCollectors);
            monitoredTasks.AddRange(activeIngests);
            monitoredTasks.Add(shutdownTask);

            var completedTask = await Task.WhenAny(monitoredTasks).ConfigureAwait(false);
            if (completedTask == shutdownTask || stoppingToken.IsCancellationRequested)
            {
                await Shutdown(pipelines, collectorCts).ConfigureAwait(false);
                break;
            }

            var pipeline = pipelines.First(pipeline => pipeline.CollectorTask == completedTask || pipeline.IngestTask == completedTask);
            if (completedTask == pipeline.CollectorTask)
            {
                activeCollectors.Remove(pipeline.CollectorTask);
                pipeline.Channel.Writer.TryComplete();

                var fatalException = GetFatalException(pipeline.CollectorTask, stoppingToken);
                if (fatalException is not null)
                {
                    await FailRuntime(pipelines, collectorCts, pipeline.CollectorTask, fatalException).ConfigureAwait(false);
                }
            }
            else
            {
                activeIngests.Remove(pipeline.IngestTask);

                var fatalException = GetFatalException(pipeline.IngestTask, stoppingToken);
                if (fatalException is not null)
                {
                    await FailRuntime(pipelines, collectorCts, pipeline.IngestTask, fatalException).ConfigureAwait(false);
                }
            }
        }
    }

    private static Exception? GetFatalException(Task task, CancellationToken stoppingToken)
    {
        Exception? result = null;

        if (task.IsFaulted)
        {
            result = task.Exception?.InnerExceptions.Count == 1
                ? task.Exception.InnerException
                : task.Exception;
        }
        else if (task.IsCanceled && !stoppingToken.IsCancellationRequested)
        {
            result = new TaskCanceledException(task);
        }

        return result;
    }

    private async Task FailRuntime(
        IReadOnlyList<(Channel<TradeInfo> Channel, Task CollectorTask, Task IngestTask)> pipelines,
        CancellationTokenSource collectorCts,
        Task failedTask,
        Exception fatalException)
    {
        collectorCts.Cancel();

        foreach ((Channel<TradeInfo> channel, _, _) in pipelines)
        {
            channel.Writer.TryComplete();
        }

        await AwaitTasksForCleanup(pipelines.Select(static pipeline => pipeline.CollectorTask), "Collector task failed during cleanup after a fatal runtime error.").ConfigureAwait(false);
        await AwaitTasksForCleanup(pipelines.Select(static pipeline => pipeline.IngestTask).Where(task => task != failedTask), "Ingest worker failed during cleanup after a fatal runtime error.").ConfigureAwait(false);

        ExceptionDispatchInfo.Capture(fatalException).Throw();
    }

    private async Task Shutdown(
        IReadOnlyList<(Channel<TradeInfo> Channel, Task CollectorTask, Task IngestTask)> pipelines,
        CancellationTokenSource collectorCts)
    {
        collectorCts.Cancel();

        await AwaitTasksForCleanup(pipelines.Select(static pipeline => pipeline.CollectorTask), "Collector task failed during shutdown.").ConfigureAwait(false);

        foreach ((Channel<TradeInfo> channel, _, _) in pipelines)
        {
            channel.Writer.TryComplete();
        }

        await AwaitTasksForCleanup(pipelines.Select(static pipeline => pipeline.IngestTask), "Ingest worker failed during shutdown.").ConfigureAwait(false);
    }

    private async Task AwaitTasksForCleanup(IEnumerable<Task> tasks, string warningMessage)
    {
        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            log.Warn(warningMessage);
            log.Error(ex);
        }
    }

    private async Task CollectInstrument(Instrument instrument, ChannelWriter<TradeInfo> writer, CancellationToken stoppingToken)
    {
        TimeSpan delay = retryOptions.InitialDelay;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await foreach (TradeInfo trade in tradeSource.GetLiveTrades(instrument, stoppingToken))
                {
                    await writer.WriteAsync(trade, stoppingToken).ConfigureAwait(false);
                }

                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                log.Warn($"Trade source failed for {instrument}; retrying in {delay} with the following exception: {ex}");
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);

                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, retryOptions.MaxDelay.TotalMilliseconds));
            }
        }
    }
}
