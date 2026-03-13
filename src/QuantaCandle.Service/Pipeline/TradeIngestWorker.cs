using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using QuantaCandle.Core.Logging;
using QuantaCandle.Core.Trading;
using QuantaCandle.Service.Options;

namespace QuantaCandle.Service.Pipeline;

public sealed class TradeIngestWorker
{
    private readonly ITradeSink tradeSink;
    private readonly IIngestionStateStore ingestionStateStore;
    private readonly ITradeDeduplicator deduplicator;
    private readonly TradePipelineStats stats;
    private readonly ILogMachina<TradeIngestWorker> logMachina;

    public TradeIngestWorker(
        ITradeSink tradeSink,
        IIngestionStateStore ingestionStateStore,
        ITradeDeduplicator deduplicator,
        TradePipelineStats stats,
        ILogMachinaFactory logMachinaFactory)
    {
        this.tradeSink = tradeSink;
        this.ingestionStateStore = ingestionStateStore;
        this.deduplicator = deduplicator;
        this.stats = stats;
        logMachina = logMachinaFactory.Create<TradeIngestWorker>();
    }

    public async Task RunAsync(ChannelReader<TradeInfo> reader, CollectorOptions options, CancellationToken stoppingToken)
    {
        var logger = logMachina.GetLogger();
        List<TradeInfo> batch = new List<TradeInfo>(options.BatchSize);
        using PeriodicTimer timer = new PeriodicTimer(options.FlushInterval);
        Task<bool>? waitToReadTask = null;
        Task<bool> tickTask = timer.WaitForNextTickAsync(stoppingToken).AsTask();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                waitToReadTask ??= reader.WaitToReadAsync(stoppingToken).AsTask();

                Task<bool> completed = await Task.WhenAny(waitToReadTask, tickTask).ConfigureAwait(false);
                if (completed == tickTask)
                {
                    await FlushBatchAsync(batch, stoppingToken).ConfigureAwait(false);
                    tickTask = timer.WaitForNextTickAsync(stoppingToken).AsTask();
                    continue;
                }

                bool canRead = await waitToReadTask.ConfigureAwait(false);
                waitToReadTask = null;
                if (!canRead)
                {
                    break;
                }

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
                        await FlushBatchAsync(batch, stoppingToken).ConfigureAwait(false);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Ingest worker crashed.");
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
                    await FlushBatchAsync(batch, CancellationToken.None).ConfigureAwait(false);
                }
            }

            await FlushBatchAsync(batch, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task FlushBatchAsync(List<TradeInfo> batch, CancellationToken cancellationToken)
    {
        if (batch.Count == 0)
        {
            return;
        }

        IReadOnlyList<TradeInfo> snapshot = batch.ToArray();
        batch.Clear();

        TradeAppendResult appendResult = await tradeSink.AppendAsync(snapshot, cancellationToken).ConfigureAwait(false);
        stats.OnBatchFlushed(appendResult.InsertedCount);

        Dictionary<(ExchangeId Exchange, Instrument Symbol), TradeWatermark> latestByInstrument = new Dictionary<(ExchangeId, Instrument), TradeWatermark>();
        foreach (TradeInfo trade in snapshot)
        {
            latestByInstrument[(trade.Key.Exchange, trade.Key.Symbol)] = new TradeWatermark(trade.Key.TradeId, trade.Timestamp);
        }

        foreach (KeyValuePair<(ExchangeId Exchange, Instrument Symbol), TradeWatermark> kvp in latestByInstrument)
        {
            await ingestionStateStore.SetWatermarkAsync(kvp.Key.Exchange, kvp.Key.Symbol, kvp.Value, cancellationToken).ConfigureAwait(false);
        }
    }
}
