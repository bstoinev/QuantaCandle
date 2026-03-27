using System.Threading.Channels;

using LogMachina;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Options;

namespace QuantaCandle.Infra.Pipeline;

public sealed class TradeIngestWorker(
    ITradeSink tradeSink,
    IIngestionStateStore ingestionStateStore,
    ITradeDeduplicator deduplicator,
    TradePipelineStats stats,
    ILogMachina<TradeIngestWorker> log)
{
    public async Task Run(ChannelReader<TradeInfo> reader, CollectorOptions options, CancellationToken stoppingToken)
    {
        Task<bool>? waitToReadTask = null;

        var batch = new List<TradeInfo>(options.BatchSize);
        var gapDetector = new TradeGapDetector(ingestionStateStore);
        using var timer = new PeriodicTimer(options.FlushInterval);
        var tickTask = timer.WaitForNextTickAsync(stoppingToken).AsTask();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                waitToReadTask ??= reader.WaitToReadAsync(stoppingToken).AsTask();

                Task<bool> completed = await Task.WhenAny(waitToReadTask, tickTask).ConfigureAwait(false);
                if (completed == tickTask)
                {
                    await FlushBatch(batch, stoppingToken).ConfigureAwait(false);
                    tickTask = timer.WaitForNextTickAsync(stoppingToken).AsTask();
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
                            await gapDetector.Observe(trade, stoppingToken).ConfigureAwait(false);
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
            await gapDetector.FlushPending(CancellationToken.None).ConfigureAwait(false);

            while (reader.TryRead(out TradeInfo trade))
            {
                stats.OnTradeReceived(trade.Timestamp);

                if (!deduplicator.TryAccept(trade.Key))
                {
                    stats.OnDuplicateDropped();
                    continue;
                }

                await gapDetector.Observe(trade, CancellationToken.None).ConfigureAwait(false);
                batch.Add(trade);
                if (batch.Count >= options.BatchSize)
                {
                    await FlushBatch(batch, CancellationToken.None).ConfigureAwait(false);
                }
            }

            await gapDetector.FlushPending(CancellationToken.None).ConfigureAwait(false);
            await FlushBatch(batch, CancellationToken.None).ConfigureAwait(false);
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
        }
    }
}
