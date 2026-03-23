using System.Threading.Channels;
using QuantaCandle.Core.Logging;
using QuantaCandle.Core.Trading;
using QuantaCandle.Service.Options;

namespace QuantaCandle.Service.Pipeline;

public sealed class TradeCollectorHostedService : BackgroundService
{
    private readonly CollectorOptions _options;
    private readonly RetryOptions _retryOptions;
    private readonly ITradeSource _tradeSource;
    private readonly TradeIngestWorker _ingestWorker;
    private readonly ILogMachina<TradeCollectorHostedService> _log;

    public TradeCollectorHostedService(
        CollectorOptions options,
        RetryOptions retryOptions,
        ITradeSource tradeSource,
        TradeIngestWorker ingestWorker,
        ILogMachina<TradeCollectorHostedService> log)
    {
        _options = options;
        _retryOptions = retryOptions;
        _tradeSource = tradeSource;
        _ingestWorker = ingestWorker;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using CancellationTokenSource collectorCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        CancellationToken collectorToken = collectorCts.Token;

        List<Task> collectors = new List<Task>(_options.Instruments.Count);
        List<Channel<TradeInfo>> channels = new List<Channel<TradeInfo>>(_options.Instruments.Count);
        List<Task> ingests = new List<Task>(_options.Instruments.Count);

        foreach (Instrument instrument in _options.Instruments)
        {
            Channel<TradeInfo> channel = BoundedChannelFactory.CreateTradeChannel(_options.ChannelCapacity);
            channels.Add(channel);

            collectors.Add(CollectInstrumentAsync(instrument, channel.Writer, collectorToken));
            ingests.Add(_ingestWorker.RunAsync(channel.Reader, _options, stoppingToken));
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
            _log.Warn($"Collector task failed during shutdown with the following exception: {ex}");
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
            _log.Warn($"Ingest worker failed during shutdown with the following exception: {ex}");
        }
    }

    private async Task CollectInstrumentAsync(Instrument instrument, ChannelWriter<TradeInfo> writer, CancellationToken stoppingToken)
    {
        TimeSpan delay = _retryOptions.InitialDelay;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await foreach (TradeInfo trade in _tradeSource.GetLiveTradesAsync(instrument, stoppingToken))
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
                _log.Warn($"Trade source failed for {instrument}; retrying in {delay} with the following exception: {ex}");
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);

                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, _retryOptions.MaxDelay.TotalMilliseconds));
            }
        }
    }
}
