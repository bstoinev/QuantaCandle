using QuantaCandle.Infra;

namespace QuantaCandle.CLI.Commands;

public static class GenerateCandlesCommand
{
    public static async Task<int> RunAsync(string[] args)
    {
        if (args.Length == 0 || args[0].Equals("--help", StringComparison.OrdinalIgnoreCase) || args[0].Equals("-h", StringComparison.OrdinalIgnoreCase))
        {
            PrintHelp();
            return 0;
        }

        string command = args[0];
        if (!command.Equals("generate-candles", StringComparison.OrdinalIgnoreCase) && !command.Equals("generate", StringComparison.OrdinalIgnoreCase))
        {
            PrintHelp();
            return 2;
        }

        Dictionary<string, string> options = ParseOptions(args.Skip(1));
        if (options.ContainsKey("help") || options.ContainsKey("h"))
        {
            PrintHelp();
            return 0;
        }

        string source = GetStringOption(options, "source", "binance");
        string timeframe = GetStringOption(options, "timeframe", "1m");
        string inputDirectory = GetStringOption(options, "inDir", "trades-out");
        string outputDirectory = GetStringOption(options, "outDir", "candles-out");
        string format = GetStringOption(options, "format", "csv");

        TradeToCandleGenerator generator = new TradeToCandleGenerator();

        try
        {
            CandleGenerationResult result = await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(inputDirectory, outputDirectory, source, timeframe, format),
                CancellationToken.None).ConfigureAwait(false);

            Console.WriteLine($"Input trades:       {result.InputTradeCount}");
            Console.WriteLine($"Unique trades:      {result.UniqueTradeCount}");
            Console.WriteLine($"Duplicates dropped: {result.DuplicatesDropped}");
            Console.WriteLine($"Candles written:    {result.CandleCount}");
            Console.WriteLine($"Output files:       {result.OutputFileCount}");
            return 0;
        }
        catch (NotSupportedException ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 2;
        }
        catch (ArgumentException ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 2;
        }
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

    private static string GetStringOption(IReadOnlyDictionary<string, string> options, string name, string defaultValue)
    {
        if (options.TryGetValue(name, out string? value))
        {
            return value;
        }

        return defaultValue;
    }

    private static void PrintHelp()
    {
        Console.WriteLine("generate-candles");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  generate-candles --source binance --timeframe 1m [--format csv|jsonl] [--inDir trades-out] [--outDir candles-out]");
        Console.WriteLine();
        Console.WriteLine("Notes:");
        Console.WriteLine("  - Reads trade JSONL files produced by collect-trades --sink file.");
        Console.WriteLine("  - Default output format is CSV.");
        Console.WriteLine("  - CSV output path:   <outDir>/<source>/<timeframe>/<INSTRUMENT>/<yyyy-MM-dd>.csv");
        Console.WriteLine("  - JSONL output path: <outDir>/<source>/<timeframe>/<INSTRUMENT>/<yyyy-MM-dd>.jsonl (use --format jsonl)");
    }
}
