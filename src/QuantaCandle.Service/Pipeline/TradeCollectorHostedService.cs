using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using QuantaCandle.Core.Logging;
using QuantaCandle.Core.Trading;
using QuantaCandle.Service.Options;

namespace QuantaCandle.Service.Pipeline;

public sealed class TradeCollectorHostedService : BackgroundService
{
    private readonly CollectorOptions options;
    private readonly RetryOptions retryOptions;
    private readonly ITradeSource tradeSource;
    private readonly TradeIngestWorker ingestWorker;
    private readonly ILogMachina<TradeCollectorHostedService> logMachina;

    public TradeCollectorHostedService(
        CollectorOptions options,
        RetryOptions retryOptions,
        ITradeSource tradeSource,
        TradeIngestWorker ingestWorker,
        ILogMachinaFactory logMachinaFactory)
    {
        this.options = options;
        this.retryOptions = retryOptions;
        this.tradeSource = tradeSource;
        this.ingestWorker = ingestWorker;
        logMachina = logMachinaFactory.Create<TradeCollectorHostedService>();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var logger = logMachina.GetLogger();

        using CancellationTokenSource collectorCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        CancellationToken collectorToken = collectorCts.Token;

        List<Task> collectors = new List<Task>(options.Instruments.Count);
        List<Channel<TradeInfo>> channels = new List<Channel<TradeInfo>>(options.Instruments.Count);
        List<Task> ingests = new List<Task>(options.Instruments.Count);

        foreach (Instrument instrument in options.Instruments)
        {
            Channel<TradeInfo> channel = BoundedChannelFactory.CreateTradeChannel(options.ChannelCapacity);
            channels.Add(channel);

            collectors.Add(CollectInstrumentAsync(instrument, channel.Writer, collectorToken));
            ingests.Add(ingestWorker.RunAsync(channel.Reader, options, stoppingToken));
        }

        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            collectorCts.Cancel();
        }

        try
        {
            await Task.WhenAll(collectors).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Collector task failed during shutdown.");
        }
        finally
        {
            foreach (Channel<TradeInfo> channel in channels)
            {
                channel.Writer.TryComplete();
            }
        }

        try
        {
            await Task.WhenAll(ingests).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Ingest worker failed during shutdown.");
        }
    }

    private async Task CollectInstrumentAsync(Instrument instrument, ChannelWriter<TradeInfo> writer, CancellationToken stoppingToken)
    {
        var logger = logMachina.GetLogger();
        TimeSpan delay = retryOptions.InitialDelay;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await foreach (TradeInfo trade in tradeSource.GetLiveTradesAsync(instrument, stoppingToken))
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
                logger.LogWarning(ex, "Trade source failed for {instrument}; retrying in {delay}.", instrument, delay);
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);

                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, retryOptions.MaxDelay.TotalMilliseconds));
            }
        }
    }
}
