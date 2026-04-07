using System.Threading.Channels;

using LogMachina;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Options;

namespace QuantaCandle.Infra.Pipeline;

public sealed class TradeIngestWorker(
    ITradeSink tradeSink,
    IIngestionStateStore ingestionStateStore,
    ICheckpointSignal checkpointSignal,
    ITradeCheckpointLifecycle tradeCheckpointLifecycle,
    ITradeDeduplicator deduplicator,
    TradePipelineStats stats,
    ILogMachina<TradeIngestWorker> log)
{
    public async Task Run(ChannelReader<TradeInfo> reader, CollectorOptions options, CancellationToken stoppingToken)
    {
        Task<bool>? waitToReadTask = null;

        var batch = new List<TradeInfo>(options.BatchSize);
        var gapDetector = new TradeGapDetector(ingestionStateStore);
        using var timer = new PeriodicTimer(options.CheckpointInterval);
        var manualCheckpointVersion = checkpointSignal.CurrentVersion;
        var manualCheckpointTask = checkpointSignal.WaitForNextSignalAsync(manualCheckpointVersion, stoppingToken).AsTask();
        var tickTask = timer.WaitForNextTickAsync(stoppingToken).AsTask();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                waitToReadTask ??= reader.WaitToReadAsync(stoppingToken).AsTask();

                Task completed = await Task.WhenAny(waitToReadTask, tickTask, manualCheckpointTask).ConfigureAwait(false);
                if (completed == tickTask)
                {
                    await TriggerCheckpoint(batch, stoppingToken).ConfigureAwait(false);
                    tickTask = timer.WaitForNextTickAsync(stoppingToken).AsTask();
                    continue;
                }

                if (completed == manualCheckpointTask)
                {
                    manualCheckpointVersion = await manualCheckpointTask.ConfigureAwait(false);
                    manualCheckpointTask = checkpointSignal.WaitForNextSignalAsync(manualCheckpointVersion, stoppingToken).AsTask();
                    await TriggerCheckpoint(batch, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                bool canRead = await waitToReadTask.ConfigureAwait(false);
                waitToReadTask = null;

                if (canRead)
                {
                    while (reader.TryRead(out TradeInfo trade))
                    {
                        stats.OnTradeReceived(trade.Timestamp);

                        if (deduplicator.TryAccept(trade.Key))
                        {
                            batch.Add(trade);
                            if (batch.Count >= options.BatchSize)
                            {
                                await FlushBatch(batch, stoppingToken).ConfigureAwait(false);
                            }
                        }
                        else
                        {
                            stats.OnDuplicateDropped();
                        }
                    }
                }
                else
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            log.Error(ex);
            log.Warn("Ingest worker crashed.");
            throw;
        }
        finally
        {
            while (reader.TryRead(out TradeInfo trade))
            {
                stats.OnTradeReceived(trade.Timestamp);

                if (!deduplicator.TryAccept(trade.Key))
                {
                    stats.OnDuplicateDropped();
                    continue;
                }

                batch.Add(trade);
                if (batch.Count >= options.BatchSize)
                {
                    await FlushBatch(batch, CancellationToken.None).ConfigureAwait(false);
                }
            }

            await FlushBatch(batch, CancellationToken.None).ConfigureAwait(false);
            await FlushSinkOnShutdown(CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task FlushBatch(List<TradeInfo> batch, CancellationToken cancellationToken)
    {
        if (batch.Count != 0)
        {
            var snapshot = batch.ToArray();
            batch.Clear();

            var appendResult = await tradeSink.Append(snapshot, cancellationToken).ConfigureAwait(false);
            stats.OnBatchFlushed(appendResult.InsertedCount);

            var latestByInstrument = new Dictionary<(ExchangeId Exchange, Instrument Symbol), TradeWatermark>();

            foreach (TradeInfo trade in snapshot)
            {
                latestByInstrument[(trade.Key.Exchange, trade.Key.Symbol)] = new TradeWatermark(trade.Key.TradeId, trade.Timestamp);
            }

            foreach (var kvp in latestByInstrument)
            {
                await ingestionStateStore.SetWatermarkAsync(kvp.Key.Exchange, kvp.Key.Symbol, kvp.Value, cancellationToken).ConfigureAwait(false);
            }

            await tradeCheckpointLifecycle.TrackAppendedTrades(snapshot, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Flushes the current batch and routes checkpoint work through the existing sink lifecycle path.
    /// </summary>
    private async Task TriggerCheckpoint(List<TradeInfo> batch, CancellationToken cancellationToken)
    {
        await FlushBatch(batch, cancellationToken).ConfigureAwait(false);
        await CheckpointSink(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs any due sink checkpoint work without coupling the worker to a specific sink implementation.
    /// </summary>
    private async ValueTask CheckpointSink(CancellationToken cancellationToken)
    {
        var checkpointCompleted = await tradeCheckpointLifecycle.CheckpointActive(cancellationToken).ConfigureAwait(false);

        if (checkpointCompleted)
        {
            var msg = TradePipelineStatsLogFormatter.Format(stats.GetSnapshot());
            log.Info(msg);
        }
    }

    /// <summary>
    /// Flushes any remaining active sink state during graceful shutdown.
    /// </summary>
    private ValueTask FlushSinkOnShutdown(CancellationToken cancellationToken)
    {
        var result = tradeCheckpointLifecycle.FlushOnShutdown(cancellationToken);
        return result;
    }
}
