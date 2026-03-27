using QuantaCandle.Infra.Generation;

return await Run(args).ConfigureAwait(false);

static async Task<int> Run(string[] args)
{
    if (CandleGeneratorCommand.IsHelpRequest(args))
    {
        CandleGeneratorCommand.WriteHelp(Console.Out);
        return 0;
    }

    CandleGeneratorRunOptions runOptions;

    try
    {
        runOptions = CandleGeneratorCommand.Parse(args);
    }
    catch (ArgumentException ex)
    {
        Console.Error.WriteLine(ex.Message);
        return 2;
    }

    var generator = new TradeToCandleGenerator();
    var generatorOptions = new TradeToCandleGeneratorOptions(runOptions.InputDirectory, runOptions.OutputDirectory, runOptions.Source, runOptions.Timeframe, runOptions.Format);

    try
    {
        var result = await generator.GenerateAsync(generatorOptions,CancellationToken.None).ConfigureAwait(false);

        Console.WriteLine($"Input trades:".PadLeft(20) + result.InputTradeCount);
        Console.WriteLine($"Unique trades:".PadLeft(20) + result.UniqueTradeCount);
        Console.WriteLine($"Duplicates dropped:".PadLeft(20) + result.DuplicatesDropped);
        Console.WriteLine($"Candles written:".PadLeft(20) + result.CandleCount);
        Console.WriteLine($"Output files:".PadLeft(20) + result.OutputFileCount);
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

    return 0;
}
