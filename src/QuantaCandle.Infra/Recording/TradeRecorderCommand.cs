using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance;
using QuantaCandle.Infra.Options;

namespace QuantaCandle.Infra;

/// <summary>
/// Parses the trade recorder command line into reusable runtime options.
/// </summary>
public static class TradeRecorderCommand
{
    /// <summary>
    /// Determines whether the supplied arguments request help output.
    /// </summary>
    public static bool IsHelpRequest(string[] args)
    {
        var isHelp = args.Length == 0;

        if (!isHelp)
        {
            isHelp = IsHelpArgument(args[0]);
        }

        if (!isHelp && args.Length > 1 && IsCollectCommand(args[0]))
        {
            isHelp = IsHelpArgument(args[1]);
        }

        return isHelp;
    }

    /// <summary>
    /// Parses recorder arguments and returns the runtime options needed by the executable.
    /// </summary>
    public static TradeRecorderRunOptions Parse(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);

        var optionArgs = RemoveCommandName(args);
        var options = ParseOptions(optionArgs);

        var sink = GetStringOption(options, "sink", "null");
        var s3Bucket = GetStringOptionOrEnvironment(options, "s3Bucket", "QUANTA_CANDLE_S3_BUCKET", "QUANTA_S3_BUCKET", "S3_BUCKET");

        if (sink.Equals("s3", StringComparison.OrdinalIgnoreCase) && string.IsNullOrWhiteSpace(s3Bucket))
        {
            throw new ArgumentException("The --s3Bucket option (or QUANTA_CANDLE_S3_BUCKET env var) is required when --sink s3 is used.");
        }

        var duration = GetDurationOption(options, "duration", TimeSpan.FromMinutes(1));
        var capacity = GetIntOption(options, "capacity", 10_000);
        var batchSize = GetIntOption(options, "batchSize", 500);
        var flushInterval = GetDurationOption(options, "flushInterval", TimeSpan.FromSeconds(1));
        var tradesPerSecond = GetIntOption(options, "rate", 10);
        var outputDir = GetStringOption(options, "outDir", "trades-out");
        var s3Prefix = GetStringOptionOrEnvironment(options, "s3Prefix", "QUANTA_CANDLE_S3_PREFIX", "QUANTA_S3_PREFIX", "S3_PREFIX");
        var source = GetStringOption(options, "source", "stub");
        var binanceWsBase = GetStringOption(options, "binanceWsBase", BinanceTradeSourceOptions.Default.BaseWebSocketUrl);
        var instruments = GetInstruments(options);
        var collectorOptions = new CollectorOptions(
            Instruments: instruments,
            ChannelCapacity: capacity,
            BatchSize: batchSize,
            FlushInterval: flushInterval,
            MaxTradesPerSecond: tradesPerSecond);
        var retryOptions = new RetryOptions(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30));

        var runOptions = new TradeRecorderRunOptions(
            duration,
            collectorOptions,
            retryOptions,
            CreateTradeSourceRegistration(source, binanceWsBase, tradesPerSecond),
            CreateTradeSinkRegistration(sink, outputDir, s3Bucket, s3Prefix));

        return runOptions;
    }

    /// <summary>
    /// Writes the recorder help text to the supplied writer.
    /// </summary>
    public static void WriteHelp(TextWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);

        writer.WriteLine("QuantaCandle.TradeRecorder");
        writer.WriteLine();
        writer.WriteLine("Usage:");
        writer.WriteLine("  --source stub|binance --instrument BTCUSDT --duration 10m [--rate 10] [--capacity 10000] [--batchSize 500] [--flushInterval 1s] [--sink null|file|s3] [--outDir trades-out]");
        writer.WriteLine("  collect-trades --source stub|binance --instrument BTCUSDT --duration 10m [--rate 10] [--capacity 10000] [--batchSize 500] [--flushInterval 1s] [--sink null|file|s3] [--outDir trades-out]");
        writer.WriteLine("    S3 sink options: --s3Bucket my-bucket [--s3Prefix trades/raw] (env: QUANTA_CANDLE_S3_BUCKET, QUANTA_CANDLE_S3_PREFIX)");
        writer.WriteLine("    Binance options: [--binanceWsBase wss://stream.binance.com:9443] (try wss://stream.binance.us:9443 in the US)");
    }

    private static string[] RemoveCommandName(string[] args)
    {
        var result = args;

        if (args.Length > 0 && IsCollectCommand(args[0]))
        {
            result = args[1..];
        }

        return result;
    }

    private static bool IsCollectCommand(string value)
    {
        var isCollectCommand = value.Equals("collect-trades", StringComparison.OrdinalIgnoreCase);

        if (!isCollectCommand)
        {
            isCollectCommand = value.Equals("collect", StringComparison.OrdinalIgnoreCase);
        }

        return isCollectCommand;
    }

    private static bool IsHelpArgument(string value)
    {
        var isHelpArgument = value.Equals("--help", StringComparison.OrdinalIgnoreCase);

        if (!isHelpArgument)
        {
            isHelpArgument = value.Equals("-h", StringComparison.OrdinalIgnoreCase);
        }

        return isHelpArgument;
    }

    private static TradeRecorderSourceRegistration CreateTradeSourceRegistration(string source, string binanceWsBase, int tradesPerSecond)
    {
        TradeRecorderSourceRegistration registration;

        if (source.Equals("binance", StringComparison.OrdinalIgnoreCase))
        {
            registration = new TradeRecorderSourceRegistration(
                new BinanceTradeSourceOptions(
                    BaseWebSocketUrl: binanceWsBase,
                    InitialReconnectDelay: BinanceTradeSourceOptions.Default.InitialReconnectDelay,
                    MaxReconnectDelay: BinanceTradeSourceOptions.Default.MaxReconnectDelay,
                    ReceiveBufferSize: BinanceTradeSourceOptions.Default.ReceiveBufferSize),
                null);
        }
        else
        {
            registration = new TradeRecorderSourceRegistration(
                null,
                new TradeSourceStubOptions(new ExchangeId("Stub"), tradesPerSecond, 50_000m, 0.01m, 0.001m));
        }

        return registration;
    }

    private static TradeRecorderSinkRegistration CreateTradeSinkRegistration(string sink, string outputDir, string s3Bucket, string s3Prefix)
    {
        TradeRecorderSinkRegistration registration;

        if (sink.Equals("file", StringComparison.OrdinalIgnoreCase))
        {
            registration = new TradeRecorderSinkRegistration(new TradeSinkFileSimpleOptions(outputDir), null);
        }
        else if (sink.Equals("s3", StringComparison.OrdinalIgnoreCase))
        {
            registration = new TradeRecorderSinkRegistration(null, new TradeSinkS3SimpleOptions(s3Bucket, s3Prefix));
        }
        else
        {
            registration = new TradeRecorderSinkRegistration(null, null);
        }

        return registration;
    }

    private static IReadOnlyList<Instrument> GetInstruments(IReadOnlyDictionary<string, string> options)
    {
        var raw = GetStringOption(options, "instrument", string.Empty);
        if (string.IsNullOrWhiteSpace(raw))
        {
            raw = GetStringOption(options, "instruments", "BTC-USDT");
        }

        var parts = raw.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        var instruments = new List<Instrument>(parts.Length);

        foreach (var part in parts)
        {
            instruments.Add(ParseInstrument(part));
        }

        return instruments;
    }

    private static Dictionary<string, string> ParseOptions(IEnumerable<string> args)
    {
        var options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        string? pendingKey = null;

        foreach (var arg in args)
        {
            if (arg.StartsWith("--", StringComparison.Ordinal))
            {
                pendingKey = arg.Substring(2);
                options[pendingKey] = "true";
            }
            else if (pendingKey is not null)
            {
                options[pendingKey] = arg;
                pendingKey = null;
            }
        }

        return options;
    }

    private static int GetIntOption(IReadOnlyDictionary<string, string> options, string name, int defaultValue)
    {
        var result = defaultValue;

        if (options.TryGetValue(name, out var value) && int.TryParse(value, out var parsed))
        {
            result = parsed;
        }

        return result;
    }

    private static TimeSpan GetDurationOption(IReadOnlyDictionary<string, string> options, string name, TimeSpan defaultValue)
    {
        var result = defaultValue;

        if (options.TryGetValue(name, out var value))
        {
            result = ParseDuration(value, defaultValue);
        }

        return result;
    }

    private static Instrument ParseInstrument(string raw)
    {
        try
        {
            return Instrument.Parse(raw);
        }
        catch
        {
            var normalized = raw.Trim().ToUpperInvariant();
            if (normalized.Contains('-', StringComparison.Ordinal))
            {
                throw;
            }

            var commonQuotes = new[] { "USDT", "USDC", "USD", "BTC", "ETH", "EUR" };
            foreach (var quote in commonQuotes)
            {
                if (normalized.EndsWith(quote, StringComparison.Ordinal))
                {
                    var baseSymbol = normalized.Substring(0, normalized.Length - quote.Length);
                    return Instrument.Parse($"{baseSymbol}-{quote}");
                }
            }

            throw;
        }
    }

    private static TimeSpan ParseDuration(string raw, TimeSpan defaultValue)
    {
        var result = defaultValue;

        if (TimeSpan.TryParse(raw, out var parsed))
        {
            result = parsed;
        }
        else
        {
            var normalized = raw.Trim();
            if (normalized.EndsWith("ms", StringComparison.OrdinalIgnoreCase) && double.TryParse(normalized[..^2], out var ms))
            {
                result = TimeSpan.FromMilliseconds(ms);
            }
            else if (normalized.EndsWith('s') && double.TryParse(normalized[..^1], out var s))
            {
                result = TimeSpan.FromSeconds(s);
            }
            else if (normalized.EndsWith('m') && double.TryParse(normalized[..^1], out var m))
            {
                result = TimeSpan.FromMinutes(m);
            }
            else if (normalized.EndsWith('h') && double.TryParse(normalized[..^1], out var h))
            {
                result = TimeSpan.FromHours(h);
            }
        }

        return result;
    }

    private static string GetStringOption(IReadOnlyDictionary<string, string> options, string name, string defaultValue)
    {
        var result = defaultValue;

        if (options.TryGetValue(name, out var value))
        {
            result = value;
        }

        return result;
    }

    private static string GetStringOptionOrEnvironment(IReadOnlyDictionary<string, string> options, string optionName, params string[] environmentVariableNames)
    {
        var result = string.Empty;

        if (options.TryGetValue(optionName, out var optionValue) && !string.IsNullOrWhiteSpace(optionValue))
        {
            result = optionValue;
        }
        else
        {
            foreach (var environmentVariableName in environmentVariableNames)
            {
                var environmentValue = Environment.GetEnvironmentVariable(environmentVariableName);
                if (!string.IsNullOrWhiteSpace(environmentValue))
                {
                    result = environmentValue;
                    break;
                }
            }
        }

        return result;
    }
}
