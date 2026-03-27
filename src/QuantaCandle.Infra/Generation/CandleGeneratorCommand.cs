namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Parses the candle generator command line into reusable generator options.
/// </summary>
public static class CandleGeneratorCommand
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

        return isHelp;
    }

    /// <summary>
    /// Parses generator arguments and returns the runtime options needed by the executable.
    /// </summary>
    public static CandleGeneratorRunOptions Parse(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);

        var options = ParseOptions(args);
        var runOptions = new CandleGeneratorRunOptions(
            GetStringOption(options, "source", "binance"),
            GetStringOption(options, "timeframe", "1m"),
            GetStringOption(options, "inDir", "trades-out"),
            GetStringOption(options, "outDir", "candles-out"),
            GetStringOption(options, "format", "csv"));

        return runOptions;
    }

    /// <summary>
    /// Writes the generator help text to the supplied writer.
    /// </summary>
    public static void WriteHelp(TextWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);

        writer.WriteLine("QuantaCandle.CandleGenerator");
        writer.WriteLine();
        writer.WriteLine("Usage:");
        writer.WriteLine("  --source binance --timeframe 1m [--format csv|jsonl] [--inDir trades-out] [--outDir candles-out]");
        writer.WriteLine();
        writer.WriteLine("Notes:");
        writer.WriteLine("  - Reads trade JSONL files produced by QuantaCandle.TradeRecorder with --sink file.");
        writer.WriteLine("  - Default output format is CSV.");
        writer.WriteLine("  - CSV output path:   <outDir>/<source>/<timeframe>/<INSTRUMENT>/<yyyy-MM-dd>.csv");
        writer.WriteLine("  - JSONL output path: <outDir>/<source>/<timeframe>/<INSTRUMENT>/<yyyy-MM-dd>.jsonl (use --format jsonl)");
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
            else
            {
                throw new ArgumentException($"Unexpected argument '{arg}'. QuantaCandle.CandleGenerator accepts only generator options.");
            }
        }

        if (pendingKey is not null && (pendingKey.Equals("help", StringComparison.OrdinalIgnoreCase) || pendingKey.Equals("h", StringComparison.OrdinalIgnoreCase)))
        {
            options[pendingKey] = "true";
        }

        return options;
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
}
