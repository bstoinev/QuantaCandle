using LogMachina;
using LogMachina.DependencyInjection;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;
using QuantaCandle.Service.Options;
using QuantaCandle.Service.Pipeline;
using QuantaCandle.Service.Stubs;
using QuantaCandle.Service.Time;

namespace QuantaCandle.Service;

public class Program
{
    public static void Main(string[] args)
    {
        CreateHostBuilder(args).Build().Run();
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureServices(services =>
            {
                services.AddLogMachina();

                services.AddSingleton<IClock, SystemClock>();

                services.AddSingleton<TradePipelineStats>();

                services.AddSingleton(new CollectorOptions(
                    Instruments: [Instrument.Parse("BTC-USDT")],
                    ChannelCapacity: 10_000,
                    BatchSize: 500,
                    FlushInterval: TimeSpan.FromSeconds(1)));

                services.AddSingleton<ITradeDeduplicator, InMemoryTradeDeduplicator>();

                services.AddSingleton(new RetryOptions(
                    InitialDelay: TimeSpan.FromSeconds(1),
                    MaxDelay: TimeSpan.FromSeconds(30)));

                services.AddSingleton(new TradeSourceStubOptions(
                    Exchange: new ExchangeId("Stub"),
                    TradesPerSecond: 10,
                    StartPrice: 50_000m,
                    PriceStep: 0.01m,
                    Quantity: 0.001m));

                services.AddSingleton<ITradeSource, TradeSourceStub>();
                services.AddSingleton<ITradeSink, TradeSinkNull>();
                services.AddSingleton<IIngestionStateStore, InMemoryIngestionStateStore>();

                services.AddSingleton<TradeIngestWorker>();
                services.AddHostedService<TradeCollectorHostedService>();
            });
}
