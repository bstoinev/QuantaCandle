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
        var result = args.Length == 0;

        if (!result)
        {
            result = IsHelpArgument(args[0]);
        }

        return result;
    }

    /// <summary>
    /// Parses recorder arguments and returns the runtime options needed by the executable.
    /// </summary>
    public static TradeRecorderRunOptions Parse(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);

        ValidateArguments(args);

        var instrument = ParseInstrument(args[0]);
        var options = ParseOptions(args.Skip(1));
        var exchange = GetRequiredExchangeOption(options);
        var sink = GetStringOption(options, "sink", "file");
        var s3Bucket = GetStringOptionOrEnvironment(options, "s3Bucket", "QUANTA_CANDLE_S3_BUCKET", "QUANTA_S3_BUCKET", "S3_BUCKET");

        if (sink.Equals("s3", StringComparison.OrdinalIgnoreCase) && string.IsNullOrWhiteSpace(s3Bucket))
        {
            throw new ArgumentException("The --s3Bucket option (or QUANTA_CANDLE_S3_BUCKET env var) is required when --sink s3 is used.");
        }

        var duration = GetOptionalDurationOption(options, "duration");
        var cacheSize = GetPositiveIntOption(options, "cacheSize", 1024);
        var capacity = GetIntOption(options, "capacity", 10_000);
        var batchSize = GetIntOption(options, "batchSize", 500);
        var flushInterval = GetDurationOption(options, "flushInterval", TimeSpan.FromSeconds(1));
        var checkpointInterval = GetDurationOption(options, "checkpointInterval", TimeSpan.FromHours(1));
        var tradesPerSecond = GetIntOption(options, "rate", 10);
        var outputDir = GetStringOption(options, "outDir", "trade-data");
        var s3Prefix = GetStringOptionOrEnvironment(options, "s3Prefix", "QUANTA_CANDLE_S3_PREFIX", "QUANTA_S3_PREFIX", "S3_PREFIX");
        var binanceWsBase = GetStringOption(options, "binanceWsBase", BinanceTradeSourceOptions.Default.BaseWebSocketUrl);
        var instruments = new List<Instrument>(1) { instrument };
        var collectorOptions = new CollectorOptions(
            Instruments: instruments,
            ChannelCapacity: capacity,
            BatchSize: batchSize,
            FlushInterval: flushInterval,
            CheckpointInterval: checkpointInterval,
            MaxTradesPerSecond: tradesPerSecond);
        var retryOptions = new RetryOptions(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30));

        var runOptions = new TradeRecorderRunOptions(
            duration,
            cacheSize,
            collectorOptions,
            retryOptions,
            CreateTradeSourceRegistration(exchange, binanceWsBase, tradesPerSecond),
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
        writer.WriteLine("  BTCUSDT --exchange Binance|-x Binance [--duration 10m] [--capacity 10000] [--batchSize 500] [--flushInterval 1s] [--checkpointInterval 1h] [--cacheSize 1024] [--sink file|s3|null|-to file|s3|null] [--outDir trade-data]");
        writer.WriteLine("    Omit --duration to keep recording until the host or process is stopped.");
        writer.WriteLine("    Stub/testing source option: [--rate 10]. It controls Stub source trades per second only; it does not throttle Binance websocket ingestion.");
        writer.WriteLine("    Default sink: file. Use --sink null to disable durable trade output intentionally.");
        writer.WriteLine("    S3 sink options: --s3Bucket my-bucket [--s3Prefix trades/raw] (env: QUANTA_CANDLE_S3_BUCKET, QUANTA_CANDLE_S3_PREFIX)");
        writer.WriteLine("    Binance options: [--binanceWsBase wss://stream.binance.com:9443] (try wss://stream.binance.us:9443 in the US)");
        writer.WriteLine("    Local trade output: <working-dir>\\trade-data\\<exchange>\\<instrument>\\yyyy-MM-dd.jsonl");
    }

    private static bool IsHelpArgument(string value)
    {
        var result = value.Equals("--help", StringComparison.OrdinalIgnoreCase);

        if (!result)
        {
            result = value.Equals("-h", StringComparison.OrdinalIgnoreCase);
        }

        return result;
    }

    private static void ValidateArguments(string[] args)
    {
        if (args.Length == 0 || IsNamedOptionToken(args[0]))
        {
            throw new ArgumentException("The instrument argument is required and must be the first positional argument.");
        }
    }

    private static TradeRecorderSourceRegistration CreateTradeSourceRegistration(string exchange, string binanceWsBase, int tradesPerSecond)
    {
        TradeRecorderSourceRegistration registration;

        if (exchange.Equals("Binance", StringComparison.Ordinal))
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
            registration = new TradeRecorderSinkRegistration(null, new TradeSinkS3SimpleOptions(s3Bucket, s3Prefix, outputDir, TimeSpan.FromHours(1)));
        }
        else
        {
            registration = new TradeRecorderSinkRegistration(null, null);
        }

        return registration;
    }

    private static Dictionary<string, string> ParseOptions(IEnumerable<string> args)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        using var enumerator = args.GetEnumerator();

        while (enumerator.MoveNext())
        {
            var token = enumerator.Current;
            if (!IsNamedOptionToken(token))
            {
                throw new ArgumentException($"Unexpected argument '{token}'. The instrument must be the first positional argument and all remaining values must use named options.");
            }

            var optionName = NormalizeOptionName(token);
            if (!enumerator.MoveNext() || IsNamedOptionToken(enumerator.Current))
            {
                throw new ArgumentException($"Option '{token}' requires a value.");
            }

            result[optionName] = enumerator.Current;
        }

        return result;
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

    private static int GetPositiveIntOption(IReadOnlyDictionary<string, string> options, string name, int defaultValue)
    {
        var result = defaultValue;

        if (options.TryGetValue(name, out var value))
        {
            if (!int.TryParse(value, out var parsed))
            {
                throw new ArgumentException($"The --{name} option must be a positive integer.");
            }

            if (parsed <= 0)
            {
                throw new ArgumentException($"The --{name} option must be greater than zero.");
            }

            result = parsed;
        }

        return result;
    }

    private static TimeSpan? GetOptionalDurationOption(IReadOnlyDictionary<string, string> options, string name)
    {
        TimeSpan? result = null;

        if (options.TryGetValue(name, out var value))
        {
            result = ParseDuration(value, TimeSpan.Zero);
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
        Instrument result;

        try
        {
            result = Instrument.Parse(raw);
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
                    result = Instrument.Parse($"{baseSymbol}-{quote}");
                    return result;
                }
            }

            throw;
        }

        return result;
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

    private static string GetRequiredStringOption(IReadOnlyDictionary<string, string> options, string name)
    {
        string result;

        if (!options.TryGetValue(name, out var value) || string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"The --{name} option is required.");
        }

        result = value;
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

    private static string GetRequiredExchangeOption(IReadOnlyDictionary<string, string> options)
    {
        var rawExchange = GetRequiredStringOption(options, "exchange");
        var result = NormalizeExchange(rawExchange);

        return result;
    }

    private static bool IsNamedOptionToken(string value)
    {
        var result = value.StartsWith("--", StringComparison.Ordinal);

        if (!result && value.StartsWith("-", StringComparison.Ordinal) && value.Length > 1)
        {
            result = !char.IsDigit(value[1]);
        }

        return result;
    }

    private static string NormalizeExchange(string value)
    {
        string result;

        if (value.Equals("binance", StringComparison.OrdinalIgnoreCase))
        {
            result = "Binance";
        }
        else if (value.Equals("stub", StringComparison.OrdinalIgnoreCase))
        {
            result = "Stub";
        }
        else
        {
            throw new ArgumentException($"Unknown exchange '{value}'. Supported exchanges: Binance, Stub.");
        }

        return result;
    }

    private static string NormalizeOptionName(string token)
    {
        string result;

        if (token.Equals("--exchange", StringComparison.OrdinalIgnoreCase) || token.Equals("-x", StringComparison.OrdinalIgnoreCase))
        {
            result = "exchange";
        }
        else if (token.Equals("--sink", StringComparison.OrdinalIgnoreCase) || token.Equals("-to", StringComparison.OrdinalIgnoreCase))
        {
            result = "sink";
        }
        else if (token.Equals("--instrument", StringComparison.OrdinalIgnoreCase)
            || token.Equals("--source", StringComparison.OrdinalIgnoreCase)
            || token.Equals("--instruments", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"Legacy option '{token}' is not supported by QuantaCandle.TradeRecorder.");
        }
        else if (token.StartsWith("--", StringComparison.Ordinal))
        {
            result = token[2..];
        }
        else
        {
            throw new ArgumentException($"Unknown option '{token}'.");
        }

        return result;
    }
}
