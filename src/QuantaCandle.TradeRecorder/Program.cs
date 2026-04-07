using LogMachina;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using QuantaCandle.Infra;
using QuantaCandle.Infra.Pipeline;
using QuantaCandle.Infra.Recording;

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

    var ioc = new Container();

    using IHost host = Host.CreateDefaultBuilder()
        .ConfigureLogging(builder => builder.ClearProviders())
        .ConfigureServices(services => services.AddSimpleInjector(ioc, simpleInjector => simpleInjector.AddHostedService<TradeCollectorHostedService>()))
        .UseConsoleLifetime()
        .Build();

    host.UseSimpleInjector(ioc);

    TradeRecorderCompositionRoot.Configure(ioc, runOptions);

    ioc.Verify();

    if (runOptions.Duration is { } duration)
    {
        stopCts.CancelAfter(duration);
    }

    var log = ioc.GetInstance<ILogMachina<TradeCollectorHostedService>>();

    var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
    lifetime.ApplicationStarted.Register(() =>
    {
        var env = host.Services.GetRequiredService<IHostEnvironment>();
        var ver = typeof(Program).Assembly.GetName().Version;

        log.Info($"Trade Recorder {ver} started successfully.");
        log.Info($"Environment: {env.EnvironmentName}");
        log.Info("Press Ctrl+C to shut down.");
    });

    await host.StartAsync(stopCts.Token).ConfigureAwait(false);

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

    var stats = ioc.GetInstance<TradePipelineStats>();
    var msg = TradePipelineStatsLogFormatter.Format(stats.GetSnapshot());

    log.Info(msg);

    log.Info("Trade Recorder execution completed successfully.");

    return 0;
}
