using System.Threading.Channels;
using LogMachina;
using Microsoft.Extensions.Hosting;
using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Options;

namespace QuantaCandle.Infra.Pipeline;

public sealed class TradeCollectorHostedService(
    CollectorOptions options,
    RetryOptions retryOptions,
    ITradeSource tradeSource,
    TradeIngestWorker ingestWorker,
    ILogMachina<TradeCollectorHostedService> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
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
            ingests.Add(ingestWorker.Run(channel.Reader, options, stoppingToken));
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
            log.Warn($"Collector task failed during shutdown with the following exception: {ex}");
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
            log.Warn($"Ingest worker failed during shutdown with the following exception: {ex}");
        }
    }

    private async Task CollectInstrumentAsync(Instrument instrument, ChannelWriter<TradeInfo> writer, CancellationToken stoppingToken)
    {
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
                log.Warn($"Trade source failed for {instrument}; retrying in {delay} with the following exception: {ex}");
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);

                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, retryOptions.MaxDelay.TotalMilliseconds));
            }
        }
    }
}
