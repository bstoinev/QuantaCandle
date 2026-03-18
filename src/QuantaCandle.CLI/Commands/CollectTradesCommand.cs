using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using QuantaCandle.Core.Logging;
using QuantaCandle.Core;
using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance;
using QuantaCandle.Service.Logging;
using QuantaCandle.Service.Options;
using QuantaCandle.Service.Pipeline;
using QuantaCandle.Service.Stubs;
using QuantaCandle.Service.Time;

namespace QuantaCandle.CLI.Commands;

public static class CollectTradesCommand
{
    public static async Task<int> RunAsync(string[] args)
    {
        if (args.Length == 0 || args[0].Equals("--help", StringComparison.OrdinalIgnoreCase) || args[0].Equals("-h", StringComparison.OrdinalIgnoreCase))
        {
            PrintHelp();
            return 0;
        }

        string command = args[0];
        if (!command.Equals("collect-trades", StringComparison.OrdinalIgnoreCase) && !command.Equals("collect", StringComparison.OrdinalIgnoreCase))
        {
            PrintHelp();
            return 2;
        }

        Dictionary<string, string> options = ParseOptions(args.Skip(1));

        TimeSpan duration = GetDurationOption(options, "duration", TimeSpan.FromMinutes(1));
        int capacity = GetIntOption(options, "capacity", 10_000);
        int batchSize = GetIntOption(options, "batchSize", 500);
        TimeSpan flushInterval = GetDurationOption(options, "flushInterval", TimeSpan.FromSeconds(1));
        int tradesPerSecond = GetIntOption(options, "rate", 10);
        string sink = GetStringOption(options, "sink", "null");
        string outputDir = GetStringOption(options, "outDir", "trades-out");
        string s3Bucket = GetStringOptionOrEnvironment(options, "s3Bucket", "QUANTA_CANDLE_S3_BUCKET", "QUANTA_S3_BUCKET", "S3_BUCKET");
        string s3Prefix = GetStringOptionOrEnvironment(options, "s3Prefix", "QUANTA_CANDLE_S3_PREFIX", "QUANTA_S3_PREFIX", "S3_PREFIX");
        string source = GetStringOption(options, "source", "stub");
        string binanceWsBase = GetStringOption(options, "binanceWsBase", BinanceTradeSourceOptions.Default.BaseWebSocketUrl);

        IReadOnlyList<Instrument> instruments = GetInstruments(options);

        if (sink.Equals("s3", StringComparison.OrdinalIgnoreCase) && string.IsNullOrWhiteSpace(s3Bucket))
        {
            Console.Error.WriteLine("The --s3Bucket option (or QUANTA_CANDLE_S3_BUCKET env var) is required when --sink s3 is used.");
            return 2;
        }

        using CancellationTokenSource stopCts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            stopCts.Cancel();
        };

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
            .ConfigureServices(services =>
            {
                services.AddSingleton<ILogMachinaFactory, HostLogMachinaFactory>();
                services.AddSingleton(typeof(ILogMachina<>), typeof(HostLogMachina<>));

                services.AddSingleton<IClock, SystemClock>();
                services.AddSingleton<TradePipelineStats>();
                services.AddSingleton(new CollectorOptions(
                    Instruments: instruments,
                    ChannelCapacity: capacity,
                    BatchSize: batchSize,
                    FlushInterval: flushInterval,
                    MaxTradesPerSecond: tradesPerSecond));
                services.AddSingleton(new RetryOptions(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)));
                services.AddSingleton<ITradeDeduplicator, InMemoryTradeDeduplicator>();

                if (source.Equals("binance", StringComparison.OrdinalIgnoreCase))
                {
                    services.AddSingleton(new BinanceTradeSourceOptions(
                        BaseWebSocketUrl: binanceWsBase,
                        InitialReconnectDelay: BinanceTradeSourceOptions.Default.InitialReconnectDelay,
                        MaxReconnectDelay: BinanceTradeSourceOptions.Default.MaxReconnectDelay,
                        ReceiveBufferSize: BinanceTradeSourceOptions.Default.ReceiveBufferSize));
                    services.AddSingleton<ITradeSource, BinanceTradeSource>();
                    services.AddSingleton<IIngestionStateStore, InMemoryIngestionStateStore>();
                }
                else
                {
                    services.AddSingleton(new TradeSourceStubOptions(new ExchangeId("Stub"), tradesPerSecond, 50_000m, 0.01m, 0.001m));
                    services.AddSingleton<ITradeSource, TradeSourceStub>();
                    services.AddSingleton<IIngestionStateStore, InMemoryIngestionStateStore>();
                }

                if (sink.Equals("file", StringComparison.OrdinalIgnoreCase))
                {
                    services.AddSingleton(new TradeSinkFileSimpleOptions(outputDir));
                    services.AddSingleton<ITradeSink, TradeSinkFileSimple>();
                }
                else if (sink.Equals("s3", StringComparison.OrdinalIgnoreCase))
                {
                    services.AddSingleton(new TradeSinkS3SimpleOptions(s3Bucket, s3Prefix));
                    services.AddSingleton<IS3ObjectUploader, AwsSdkS3ObjectUploader>();
                    services.AddSingleton<ITradeSink, TradeSinkS3Simple>();
                }
                else
                {
                    services.AddSingleton<ITradeSink, TradeSinkNull>();
                }

                services.AddSingleton<TradeIngestWorker>();
                services.AddHostedService<TradeCollectorHostedService>();
            })
            .UseConsoleLifetime()
            .Build();

        await host.StartAsync(stopCts.Token).ConfigureAwait(false);
        stopCts.CancelAfter(duration);

        try
        {
            await host.WaitForShutdownAsync(stopCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            await host.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }

        TradePipelineStatsSnapshot snapshot = host.Services.GetRequiredService<TradePipelineStats>().GetSnapshot();
        Console.WriteLine($"Trades received: {snapshot.TradesReceived}");
        Console.WriteLine($"Trades written:  {snapshot.TradesWritten}");
        Console.WriteLine($"Duplicates dropped: {snapshot.DuplicatesDropped}");
        Console.WriteLine($"Batches flushed: {snapshot.BatchesFlushed}");
        Console.WriteLine($"Min timestamp:   {snapshot.MinTimestamp:O}");
        Console.WriteLine($"Max timestamp:   {snapshot.MaxTimestamp:O}");

        return 0;
    }

    private static IReadOnlyList<Instrument> GetInstruments(IReadOnlyDictionary<string, string> options)
    {
        string raw = GetStringOption(options, "instrument", string.Empty);
        if (string.IsNullOrWhiteSpace(raw))
        {
            raw = GetStringOption(options, "instruments", "BTC-USDT");
        }

        string[] parts = raw.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

        List<Instrument> instruments = new List<Instrument>(parts.Length);
        foreach (string part in parts)
        {
            instruments.Add(ParseInstrument(part));
        }

        return instruments;
    }

    private static Dictionary<string, string> ParseOptions(IEnumerable<string> args)
    {
        Dictionary<string, string> options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        string? pendingKey = null;

        foreach (string arg in args)
        {
            if (arg.StartsWith("--", StringComparison.Ordinal))
            {
                pendingKey = arg.Substring(2);
                options[pendingKey] = "true";
                continue;
            }

            if (pendingKey is not null)
            {
                options[pendingKey] = arg;
                pendingKey = null;
            }
        }

        return options;
    }

    private static int GetIntOption(IReadOnlyDictionary<string, string> options, string name, int defaultValue)
    {
        if (options.TryGetValue(name, out string? value) && int.TryParse(value, out int parsed))
        {
            return parsed;
        }

        return defaultValue;
    }

    private static TimeSpan GetDurationOption(IReadOnlyDictionary<string, string> options, string name, TimeSpan defaultValue)
    {
        if (options.TryGetValue(name, out string? value))
        {
            return ParseDuration(value, defaultValue);
        }

        return defaultValue;
    }

    private static Instrument ParseInstrument(string raw)
    {
        try
        {
            return Instrument.Parse(raw);
        }
        catch
        {
            string normalized = raw.Trim().ToUpperInvariant();
            if (normalized.Contains('-', StringComparison.Ordinal))
            {
                throw;
            }

            string[] commonQuotes = new[] { "USDT", "USDC", "USD", "BTC", "ETH", "EUR" };
            foreach (string quote in commonQuotes)
            {
                if (normalized.EndsWith(quote, StringComparison.Ordinal))
                {
                    string baseSymbol = normalized.Substring(0, normalized.Length - quote.Length);
                    return Instrument.Parse($"{baseSymbol}-{quote}");
                }
            }

            throw;
        }
    }

    private static TimeSpan ParseDuration(string raw, TimeSpan defaultValue)
    {
        if (TimeSpan.TryParse(raw, out TimeSpan parsed))
        {
            return parsed;
        }

        string normalized = raw.Trim();
        if (normalized.EndsWith("ms", StringComparison.OrdinalIgnoreCase) && double.TryParse(normalized[..^2], out double ms))
        {
            return TimeSpan.FromMilliseconds(ms);
        }

        if (normalized.EndsWith('s') && double.TryParse(normalized[..^1], out double s))
        {
            return TimeSpan.FromSeconds(s);
        }

        if (normalized.EndsWith('m') && double.TryParse(normalized[..^1], out double m))
        {
            return TimeSpan.FromMinutes(m);
        }

        if (normalized.EndsWith('h') && double.TryParse(normalized[..^1], out double h))
        {
            return TimeSpan.FromHours(h);
        }

        return defaultValue;
    }

    private static string GetStringOption(IReadOnlyDictionary<string, string> options, string name, string defaultValue)
    {
        if (options.TryGetValue(name, out string? value))
        {
            return value;
        }

        return defaultValue;
    }

    private static string GetStringOptionOrEnvironment(IReadOnlyDictionary<string, string> options, string optionName, params string[] environmentVariableNames)
    {
        if (options.TryGetValue(optionName, out string? optionValue) && !string.IsNullOrWhiteSpace(optionValue))
        {
            return optionValue;
        }

        foreach (string environmentVariableName in environmentVariableNames)
        {
            string? environmentValue = Environment.GetEnvironmentVariable(environmentVariableName);
            if (!string.IsNullOrWhiteSpace(environmentValue))
            {
                return environmentValue;
            }
        }

        return string.Empty;
    }

    private static void PrintHelp()
    {
        Console.WriteLine("QuantaCandle.CLI");
        Console.WriteLine();
        Console.WriteLine("Commands:");
        Console.WriteLine("  collect-trades --source stub|binance --instrument BTCUSDT --duration 10m [--rate 10] [--capacity 10000] [--batchSize 500] [--flushInterval 1s] [--sink null|file|s3] [--outDir trades-out]");
        Console.WriteLine("    S3 sink options: --s3Bucket my-bucket [--s3Prefix trades/raw] (env: QUANTA_CANDLE_S3_BUCKET, QUANTA_CANDLE_S3_PREFIX)");
        Console.WriteLine("    Binance options: [--binanceWsBase wss://stream.binance.com:9443] (try wss://stream.binance.us:9443 in the US)");
    }
}
