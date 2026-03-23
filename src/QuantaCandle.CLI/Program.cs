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
    Console.WriteLine("  collect-trades --source stub|binance --instrument BTCUSDT --duration 10m [--rate 10] [--capacity 10000] [--batchSize 500] [--flushInterval 1s] [--sink null|file|s3] [--outDir trades-out]");
    Console.WriteLine("    S3 sink options: --s3Bucket my-bucket [--s3Prefix trades/raw] (env: QUANTA_CANDLE_S3_BUCKET, QUANTA_CANDLE_S3_PREFIX)");
    Console.WriteLine("    Binance options: [--binanceWsBase wss://stream.binance.com:9443] (try wss://stream.binance.us:9443 in the US)");
    Console.WriteLine("  generate-candles --source binance --timeframe 1m [--format csv|jsonl] [--inDir trades-out] [--outDir candles-out]");
}
