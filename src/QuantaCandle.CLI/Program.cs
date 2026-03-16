using QuantaCandle.CLI.Commands;

return await RunAsync(args).ConfigureAwait(false);

static async Task<int> RunAsync(string[] args)
{
    if (args.Length == 0 || IsHelpArgument(args[0]))
    {
        PrintHelp();
        return 0;
    }

    string command = args[0];
    if (command.Equals("collect-trades", StringComparison.OrdinalIgnoreCase) || command.Equals("collect", StringComparison.OrdinalIgnoreCase))
    {
        return await CollectTradesCommand.RunAsync(args).ConfigureAwait(false);
    }

    if (command.Equals("generate-candles", StringComparison.OrdinalIgnoreCase) || command.Equals("generate", StringComparison.OrdinalIgnoreCase))
    {
        return await GenerateCandlesCommand.RunAsync(args).ConfigureAwait(false);
    }

    PrintHelp();
    return 2;
}

static bool IsHelpArgument(string value)
{
    return value.Equals("--help", StringComparison.OrdinalIgnoreCase) || value.Equals("-h", StringComparison.OrdinalIgnoreCase);
}

static void PrintHelp()
{
    Console.WriteLine("QuantaCandle.CLI");
    Console.WriteLine();
    Console.WriteLine("Commands:");
    Console.WriteLine("  collect-trades --source stub|binance --instrument BTCUSDT --duration 10m [--sink null|file] [--outDir trades-out]");
    Console.WriteLine("  generate-candles --source binance --timeframe 1m [--inDir trades-out] [--outDir candles-out]");
}
