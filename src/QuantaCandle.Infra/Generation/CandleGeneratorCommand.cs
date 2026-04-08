using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Parses the CLI command line into reusable executable options.
/// </summary>
public static class CandleGeneratorCommand
{
    private static readonly string[] SupportedCommands =
    [
        "candlize",
        "scan",
        "heal",
    ];

    /// <summary>
    /// Determines whether the supplied arguments request help output.
    /// </summary>
    public static bool IsHelpRequest(string[] args)
    {
        var result = false;

        if (args.Length > 0)
        {
            result = IsHelpArgument(args[0]);
        }

        return result;
    }

    /// <summary>
    /// Parses CLI arguments and returns the runtime options needed by the executable.
    /// </summary>
    public static CandleGeneratorRunOptions Parse(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);

        ValidateArguments(args);

        var mode = ParseMode(args[0]);
        var instrument = ParseInstrument(args[1]);
        var options = ParseOptions(args.Skip(2));
        var dates = GetDates(options);
        var exchange = GetStringOption(options, "exchange", "Binance");
        var workDirectory = GetStringOption(options, "workDir", Directory.GetCurrentDirectory());
        var result = new CandleGeneratorRunOptions(mode, exchange, instrument.ToString(), workDirectory, dates);

        return result;
    }

    /// <summary>
    /// Writes the CLI help text to the supplied writer.
    /// </summary>
    public static void WriteHelp(TextWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);

        writer.WriteLine("QuantaCandle.CLI");
        writer.WriteLine();
        writer.WriteLine("Usage:");
        writer.WriteLine("  qc candlize <instrument> [--workDir <path>|-dir <path>] [--exchange <name>|-x <name>] [--dates <yyyy-MM-dd|yyyyMMdd,...>|-on <yyyy-MM-dd|yyyyMMdd,...>]");
        writer.WriteLine("  qc scan <instrument> [--workDir <path>|-dir <path>] [--exchange <name>|-x <name>] [--dates <yyyy-MM-dd|yyyyMMdd,...>|-on <yyyy-MM-dd|yyyyMMdd,...>]");
        writer.WriteLine("  qc heal <instrument> [--workDir <path>|-dir <path>] [--exchange <name>|-x <name>] [--dates <yyyy-MM-dd|yyyyMMdd,...>|-on <yyyy-MM-dd|yyyyMMdd,...>]");
        writer.WriteLine();
        writer.WriteLine("Notes:");
        writer.WriteLine("  - Commands: candlize, scan, heal.");
        writer.WriteLine("  - <instrument> must use BASE-QUOTE format, for example BTC-USDT.");
        writer.WriteLine("  - --workDir defaults to the current directory.");
        writer.WriteLine("  - --exchange defaults to Binance.");
        writer.WriteLine("  - Trade inputs are read from <workDir>\\trades-out\\<INSTRUMENT>\\yyyy-MM-dd.jsonl.");
        writer.WriteLine("  - Candle outputs are written to <workDir>\\candles-out\\<exchange>\\1m\\<INSTRUMENT>\\yyyy-MM-dd.csv.");
        writer.WriteLine("  - --dates and -on accept one date or a comma-separated list using yyyy-MM-dd or yyyyMMdd.");
        writer.WriteLine("  - scan reports gaps without modifying files.");
        writer.WriteLine("  - heal scans the requested instrument scope and heals each bounded gap it finds.");
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
        if (args.Length == 0 || IsOptionToken(args[0]))
        {
            throw new ArgumentException("The command argument is required. Supported commands: candlize, scan, heal.");
        }

        if (args.Length == 1 || IsOptionToken(args[1]))
        {
            throw new ArgumentException("The instrument argument is required and must follow the command.");
        }
    }

    private static CandleGeneratorMode ParseMode(string value)
    {
        CandleGeneratorMode result;

        if (value.Equals("candlize", StringComparison.OrdinalIgnoreCase))
        {
            result = CandleGeneratorMode.Candlize;
        }
        else if (value.Equals("scan", StringComparison.OrdinalIgnoreCase))
        {
            result = CandleGeneratorMode.Scan;
        }
        else if (value.Equals("heal", StringComparison.OrdinalIgnoreCase))
        {
            result = CandleGeneratorMode.Heal;
        }
        else
        {
            throw new ArgumentException($"Unknown command '{value}'. Supported commands: {string.Join(", ", SupportedCommands)}.");
        }

        return result;
    }

    private static Instrument ParseInstrument(string value)
    {
        Instrument result;

        try
        {
            result = Instrument.Parse(value);
        }
        catch (ArgumentException ex)
        {
            throw new ArgumentException($"Invalid instrument '{value}'. {ex.Message}", ex);
        }

        return result;
    }

    private static Dictionary<string, string> ParseOptions(IEnumerable<string> args)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        using var enumerator = args.GetEnumerator();

        while (enumerator.MoveNext())
        {
            var token = enumerator.Current;
            if (!IsOptionToken(token))
            {
                throw new ArgumentException($"Unexpected argument '{token}'. Options must use --workDir/-dir, --exchange/-x, or --dates/-on.");
            }

            var optionName = NormalizeOptionName(token);
            if (!enumerator.MoveNext() || IsOptionToken(enumerator.Current))
            {
                throw new ArgumentException($"Option '{token}' requires a value.");
            }

            result[optionName] = enumerator.Current;
        }

        return result;
    }

    private static IReadOnlyList<DateOnly> GetDates(IReadOnlyDictionary<string, string> options)
    {
        var result = Array.Empty<DateOnly>();

        if (options.TryGetValue("dates", out var dateListText) && !string.IsNullOrWhiteSpace(dateListText))
        {
            result = dateListText
                .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                .Select(ParseDate)
                .Distinct()
                .OrderBy(static value => value)
                .ToArray();
        }

        return result;
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

    private static bool IsOptionToken(string value)
    {
        var result = value.StartsWith("-", StringComparison.Ordinal);
        return result;
    }

    private static string NormalizeOptionName(string token)
    {
        string result;

        if (token.Equals("--workDir", StringComparison.OrdinalIgnoreCase) || token.Equals("-dir", StringComparison.OrdinalIgnoreCase))
        {
            result = "workDir";
        }
        else if (token.Equals("--exchange", StringComparison.OrdinalIgnoreCase) || token.Equals("-x", StringComparison.OrdinalIgnoreCase))
        {
            result = "exchange";
        }
        else if (token.Equals("--dates", StringComparison.OrdinalIgnoreCase) || token.Equals("-on", StringComparison.OrdinalIgnoreCase))
        {
            result = "dates";
        }
        else if (token.Equals("--mode", StringComparison.OrdinalIgnoreCase)
            || token.Equals("--instrument", StringComparison.OrdinalIgnoreCase)
            || token.Equals("--source", StringComparison.OrdinalIgnoreCase)
            || token.Equals("--date", StringComparison.OrdinalIgnoreCase)
            || token.Equals("--inDir", StringComparison.OrdinalIgnoreCase)
            || token.Equals("--outDir", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"Legacy option '{token}' is not supported by QuantaCandle.CLI.");
        }
        else
        {
            throw new ArgumentException($"Unknown option '{token}'. Supported options are --workDir/-dir, --exchange/-x, and --dates/-on.");
        }

        return result;
    }
}
