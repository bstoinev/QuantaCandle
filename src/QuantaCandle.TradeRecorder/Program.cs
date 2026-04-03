using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using QuantaCandle.Infra;
using QuantaCandle.Infra.Pipeline;

using SimpleInjector;

return await Run(args).ConfigureAwait(false);

static async Task<int> Run(string[] args)
{
    if (TradeRecorderCommand.IsHelpRequest(args))
    {
        TradeRecorderCommand.WriteHelp(Console.Out);
        return 0;
    }

    TradeRecorderRunOptions runOptions;

    try
    {
        runOptions = TradeRecorderCommand.Parse(args);
    }
    catch (ArgumentException ex)
    {
        Console.Error.WriteLine(ex.Message);
        return 2;
    }

    using var stopCts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        stopCts.Cancel();
    };

    var container = new Container();

    using IHost host = Host.CreateDefaultBuilder()
        .ConfigureLogging(builder =>
        {
            builder.ClearProviders();
            builder.AddSimpleConsole(options =>
            {
                options.SingleLine = true;
                options.TimestampFormat = "HH:mm:ss ";
            });
        })
        .ConfigureServices(services => services.AddSimpleInjector(container, simpleInjector => simpleInjector.AddHostedService<TradeCollectorHostedService>()))
        .UseConsoleLifetime()
        .Build();

    host.UseSimpleInjector(container);

    TradeRecorderCompositionRoot.Configure(container, runOptions);

    container.Verify();

    await host.StartAsync(stopCts.Token).ConfigureAwait(false);

    if (runOptions.Duration is { } duration)
    {
        stopCts.CancelAfter(duration);
    }

    try
    {
        await host.WaitForShutdownAsync(stopCts.Token).ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine($"Trade recorder is shutting down...");
    }
    finally
    {
        await host.StopAsync(CancellationToken.None).ConfigureAwait(false);
    }

    var snapshot = container.GetInstance<TradePipelineStats>().GetSnapshot();

    Console.WriteLine($"Trades received: ".PadLeft(22) + snapshot.TradesReceived);
    Console.WriteLine($"Trades written:".PadLeft(22) + snapshot.TradesWritten);
    Console.WriteLine($"Duplicates dropped:".PadLeft(22) + snapshot.DuplicatesDropped);
    Console.WriteLine($"Batches flushed:".PadLeft(22) + snapshot.BatchesFlushed);
    Console.WriteLine($"Min timestamp:".PadLeft(22) + $"{snapshot.MinTimestamp:O}");
    Console.WriteLine($"Max timestamp:".PadLeft(22) + $"{snapshot.MaxTimestamp:O}");
    Console.WriteLine();
    Console.WriteLine("Trade collection completed successfully.");

    return 0;
}
