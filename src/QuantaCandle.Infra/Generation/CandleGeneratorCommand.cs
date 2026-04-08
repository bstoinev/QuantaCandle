namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Parses the candle generator command line into reusable executable options.
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
        var mode = GetModeOption(options);
        var scanDates = GetScanDates(options);
        ValidateOptions(mode, options);
        var runOptions = new CandleGeneratorRunOptions(
            mode,
            GetStringOption(options, "source", "binance"),
            GetStringOption(options, "timeframe", "1m"),
            GetStringOption(options, "inDir", "trades-out"),
            GetStringOption(options, "outDir", "candles-out"),
            GetStringOption(options, "format", "csv"),
            scanDates);

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
        writer.WriteLine("  --mode generate-candles --source binance --timeframe 1m [--format csv|jsonl] [--inDir trades-out] [--outDir candles-out]");
        writer.WriteLine("  --mode scan-gaps --inDir trades-out [--date yyyy-MM-dd|yyyyMMdd] [--dates yyyy-MM-dd,yyyyMMdd,...]");
        writer.WriteLine();
        writer.WriteLine("Notes:");
        writer.WriteLine("  - Omit --mode to default to generate-candles for backward compatibility.");
        writer.WriteLine("  - Reads trade JSONL files produced by QuantaCandle.TradeRecorder with --sink file.");
        writer.WriteLine("  - Default output format is CSV.");
        writer.WriteLine("  - Scan mode reports gaps without modifying files or failing on detected gaps.");
        writer.WriteLine("  - Scan mode date filters select files by trading-day file name.");
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

    private static CandleGeneratorMode GetModeOption(IReadOnlyDictionary<string, string> options)
    {
        var rawMode = GetStringOption(options, "mode", "generate-candles");
        CandleGeneratorMode result;

        if (rawMode.Equals("generate-candles", StringComparison.OrdinalIgnoreCase))
        {
            result = CandleGeneratorMode.GenerateCandles;
        }
        else if (rawMode.Equals("scan-gaps", StringComparison.OrdinalIgnoreCase))
        {
            result = CandleGeneratorMode.ScanGaps;
        }
        else
        {
            throw new ArgumentException($"Unsupported mode '{rawMode}'. Supported modes are generate-candles and scan-gaps.");
        }

        return result;
    }

    private static void ValidateOptions(CandleGeneratorMode mode, IReadOnlyDictionary<string, string> options)
    {
        if (mode == CandleGeneratorMode.ScanGaps
            && (!options.TryGetValue("inDir", out var inputDirectory) || string.IsNullOrWhiteSpace(inputDirectory)))
        {
            throw new ArgumentException("The --inDir option is required when --mode scan-gaps is used.");
        }
    }

    private static IReadOnlyList<DateOnly> GetScanDates(IReadOnlyDictionary<string, string> options)
    {
        var result = new List<DateOnly>();

        if (options.TryGetValue("date", out var singleDateText) && !string.IsNullOrWhiteSpace(singleDateText))
        {
            result.Add(ParseDate(singleDateText));
        }

        if (options.TryGetValue("dates", out var dateListText) && !string.IsNullOrWhiteSpace(dateListText))
        {
            var parts = dateListText.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            foreach (var part in parts)
            {
                result.Add(ParseDate(part));
            }
        }

        return result
            .Distinct()
            .OrderBy(static value => value)
            .ToArray();
    }

    private static DateOnly ParseDate(string value)
    {
        DateOnly result;

        if (!DateOnly.TryParseExact(value, "yyyy-MM-dd", out result)
            && !DateOnly.TryParseExact(value, "yyyyMMdd", out result))
        {
            throw new ArgumentException($"Invalid date '{value}'. Accepted formats are yyyy-MM-dd and yyyyMMdd.");
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
}
