using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Dispatches the CLI entrypoint into candle generation, gap scanning, or gap healing modes.
/// </summary>
public sealed class CandleGeneratorApplication(
    ICandleGenerationRunner candleGenerationRunner,
    ITradeGapScanner tradeGapScanner,
    ITradeGapHealer tradeGapHealer)
{
    private readonly ICandleGenerationRunner _candleGenerationRunner = candleGenerationRunner ?? throw new ArgumentNullException(nameof(candleGenerationRunner));
    private readonly ITradeGapScanner _tradeGapScanner = tradeGapScanner ?? throw new ArgumentNullException(nameof(tradeGapScanner));
    private readonly ITradeGapHealer _tradeGapHealer = tradeGapHealer ?? throw new ArgumentNullException(nameof(tradeGapHealer));

    /// <summary>
    /// Parses arguments, executes the selected mode, writes console output, and returns a process exit code.
    /// </summary>
    public async Task<int> Run(
        string[] args,
        TextWriter outputWriter,
        TextWriter errorWriter,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(outputWriter);
        ArgumentNullException.ThrowIfNull(errorWriter);

        var result = 0;
        CandleGeneratorRunOptions runOptions;

        if (CandleGeneratorCommand.IsHelpRequest(args))
        {
            CandleGeneratorCommand.WriteHelp(outputWriter);
        }
        else
        {
            try
            {
                runOptions = CandleGeneratorCommand.Parse(args);
            }
            catch (ArgumentException ex)
            {
                await errorWriter.WriteLineAsync(ex.Message).ConfigureAwait(false);
                result = 2;
                runOptions = new CandleGeneratorRunOptions(CandleGeneratorMode.Candlize, string.Empty, string.Empty, string.Empty, []);
            }

            if (result == 0)
            {
                try
                {
                    if (runOptions.Mode == CandleGeneratorMode.Scan)
                    {
                        result = await RunScanAsync(runOptions, outputWriter, cancellationToken).ConfigureAwait(false);
                    }
                    else if (runOptions.Mode == CandleGeneratorMode.Heal)
                    {
                        result = await RunHealAsync(runOptions, outputWriter, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        result = await RunCandlizeAsync(runOptions, outputWriter, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (NotSupportedException ex)
                {
                    await errorWriter.WriteLineAsync(ex.Message).ConfigureAwait(false);
                    result = 2;
                }
                catch (ArgumentException ex)
                {
                    await errorWriter.WriteLineAsync(ex.Message).ConfigureAwait(false);
                    result = 2;
                }
                catch (InvalidOperationException ex)
                {
                    await errorWriter.WriteLineAsync(ex.Message).ConfigureAwait(false);
                    result = 2;
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Runs candle generation and writes the current summary output.
    /// </summary>
    private async Task<int> RunCandlizeAsync(
        CandleGeneratorRunOptions runOptions,
        TextWriter outputWriter,
        CancellationToken cancellationToken)
    {
        var tradeRootDirectory = GetTradeRootDirectory(runOptions);
        var candidateFiles = ResolveCandidateFiles(tradeRootDirectory, runOptions.Instrument, runOptions.Dates);
        var stagingDirectory = CreateTemporaryStagingDirectory();

        try
        {
            CopyCandidateFilesToStagingDirectory(tradeRootDirectory, stagingDirectory, candidateFiles);

            var generatorOptions = new TradeToCandleGeneratorOptions(
                stagingDirectory,
                GetCandleRootDirectory(runOptions),
                runOptions.Exchange,
                "1m",
                "csv");
            var generationResult = await _candleGenerationRunner.GenerateAsync(generatorOptions, cancellationToken).ConfigureAwait(false);

            await outputWriter.WriteLineAsync($"Input trades:".PadLeft(20) + generationResult.InputTradeCount).ConfigureAwait(false);
            await outputWriter.WriteLineAsync($"Unique trades:".PadLeft(20) + generationResult.UniqueTradeCount).ConfigureAwait(false);
            await outputWriter.WriteLineAsync($"Duplicates dropped:".PadLeft(20) + generationResult.DuplicatesDropped).ConfigureAwait(false);
            await outputWriter.WriteLineAsync($"Candles written:".PadLeft(20) + generationResult.CandleCount).ConfigureAwait(false);
            await outputWriter.WriteLineAsync($"Output files:".PadLeft(20) + generationResult.OutputFileCount).ConfigureAwait(false);
        }
        finally
        {
            DeleteDirectoryIfPresent(stagingDirectory);
        }

        return 0;
    }

    /// <summary>
    /// Runs local gap scanning and writes a per-gap summary without treating gaps as failures.
    /// </summary>
    private async Task<int> RunScanAsync(
        CandleGeneratorRunOptions runOptions,
        TextWriter outputWriter,
        CancellationToken cancellationToken)
    {
        var tradeRootDirectory = GetTradeRootDirectory(runOptions);
        var candidateFiles = ResolveCandidateFiles(tradeRootDirectory, runOptions.Instrument, runOptions.Dates);
        var scanResult = await _tradeGapScanner
            .Scan(new TradeGapScanRequest(tradeRootDirectory, candidateFiles, []), cancellationToken)
            .ConfigureAwait(false);
        var requestedExchange = new ExchangeId(runOptions.Exchange);
        var requestedInstrument = Instrument.Parse(runOptions.Instrument);
        var filteredGaps = FilterGaps(scanResult, requestedExchange, requestedInstrument);

        await outputWriter.WriteLineAsync($"Files scanned:".PadLeft(20) + scanResult.TotalFilesScanned).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Trades scanned:".PadLeft(20) + scanResult.TotalTradesScanned).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Gaps found:".PadLeft(20) + filteredGaps.Count).ConfigureAwait(false);

        for (var i = 0; i < filteredGaps.Count; i++)
        {
            var gapWithRange = filteredGaps[i];
            var gap = gapWithRange.Gap;
            var missingRange = gap.MissingTradeIds is null
                ? "unknown"
                : $"{gap.MissingTradeIds.Value.FirstTradeId}-{gap.MissingTradeIds.Value.LastTradeId}";
            var affectedFileInfo = FormatAffectedFileInfo(gapWithRange.Range);

            await outputWriter.WriteLineAsync(
                    $"Gap {i + 1}: exchange={gap.Exchange} instrument={gap.Symbol} missing={missingRange} files={affectedFileInfo}")
                .ConfigureAwait(false);
        }

        return 0;
    }

    /// <summary>
    /// Runs local gap healing by scanning first and then healing each bounded gap in the requested scope.
    /// </summary>
    private async Task<int> RunHealAsync(
        CandleGeneratorRunOptions runOptions,
        TextWriter outputWriter,
        CancellationToken cancellationToken)
    {
        EnsureSupportedExchange(runOptions.Exchange);

        var tradeRootDirectory = GetTradeRootDirectory(runOptions);
        var candidateFiles = ResolveCandidateFiles(tradeRootDirectory, runOptions.Instrument, runOptions.Dates);
        var scanResult = await _tradeGapScanner
            .Scan(new TradeGapScanRequest(tradeRootDirectory, candidateFiles, []), cancellationToken)
            .ConfigureAwait(false);
        var requestedExchange = new ExchangeId(runOptions.Exchange);
        var requestedInstrument = Instrument.Parse(runOptions.Instrument);
        var filteredGaps = FilterGaps(scanResult, requestedExchange, requestedInstrument);
        var fullHealCount = 0;
        var partialHealCount = 0;
        var noChangeCount = 0;

        foreach (var gapWithRange in filteredGaps)
        {
            if (gapWithRange.Gap.MissingTradeIds is null)
            {
                continue;
            }

            var healResult = await _tradeGapHealer
                .Heal(
                    new TradeGapHealRequest(
                        tradeRootDirectory,
                        requestedExchange,
                        requestedInstrument,
                        gapWithRange.Gap.MissingTradeIds.Value.FirstTradeId,
                        gapWithRange.Gap.MissingTradeIds.Value.LastTradeId,
                        candidateFiles,
                        gapWithRange.Range),
                    cancellationToken)
                .ConfigureAwait(false);

            if (healResult.Outcome == TradeGapHealStatus.Full)
            {
                fullHealCount++;
            }
            else if (healResult.Outcome == TradeGapHealStatus.Partial)
            {
                partialHealCount++;
            }
            else
            {
                noChangeCount++;
            }
        }

        await outputWriter.WriteLineAsync($"Files scanned:".PadLeft(20) + scanResult.TotalFilesScanned).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Trades scanned:".PadLeft(20) + scanResult.TotalTradesScanned).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Gaps found:".PadLeft(20) + filteredGaps.Count).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Gaps healed full:".PadLeft(20) + fullHealCount).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Gaps healed partial:".PadLeft(20) + partialHealCount).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Gaps unchanged:".PadLeft(20) + noChangeCount).ConfigureAwait(false);

        return 0;
    }

    /// <summary>
    /// Resolves requested dates into candidate files under the supplied trade root directory and instrument scope.
    /// </summary>
    private static IReadOnlyList<TradeGapAffectedFile> ResolveCandidateFiles(string tradeRootDirectory, string instrument, IReadOnlyList<DateOnly> dates)
    {
        var result = new List<TradeGapAffectedFile>();
        var instrumentPath = Instrument.Parse(instrument).ToString();
        var instrumentDirectory = Path.Combine(tradeRootDirectory, instrumentPath);

        if (!Directory.Exists(instrumentDirectory))
        {
            return result;
        }

        if (dates.Count > 0)
        {
            foreach (var date in dates.OrderBy(static value => value))
            {
                var relativePath = Path.Combine(instrumentPath, date.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture) + ".jsonl");
                var fullPath = Path.Combine(tradeRootDirectory, relativePath);
                if (File.Exists(fullPath))
                {
                    result.Add(new TradeGapAffectedFile(relativePath, date));
                }
            }
        }
        else
        {
            foreach (var fullPath in Directory.EnumerateFiles(instrumentDirectory, "*.jsonl", SearchOption.TopDirectoryOnly).OrderBy(static path => path, StringComparer.Ordinal))
            {
                var relativePath = Path.GetRelativePath(tradeRootDirectory, fullPath);
                var tradingDay = DateOnly.ParseExact(Path.GetFileNameWithoutExtension(fullPath), "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
                result.Add(new TradeGapAffectedFile(relativePath, tradingDay));
            }
        }

        return result;
    }

    private static void CopyCandidateFilesToStagingDirectory(string tradeRootDirectory, string stagingDirectory, IReadOnlyList<TradeGapAffectedFile> candidateFiles)
    {
        foreach (var candidateFile in candidateFiles)
        {
            var sourcePath = Path.Combine(tradeRootDirectory, candidateFile.Path);
            var destinationPath = Path.Combine(stagingDirectory, candidateFile.Path);
            var destinationDirectory = Path.GetDirectoryName(destinationPath);

            if (!string.IsNullOrWhiteSpace(destinationDirectory))
            {
                Directory.CreateDirectory(destinationDirectory);
            }

            File.Copy(sourcePath, destinationPath, true);
        }
    }

    private static string CreateTemporaryStagingDirectory()
    {
        var result = Path.Combine(Path.GetTempPath(), "QuantaCandle.CLI", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(result);
        return result;
    }

    private static void DeleteDirectoryIfPresent(string path)
    {
        if (Directory.Exists(path))
        {
            Directory.Delete(path, true);
        }
    }

    private static List<(TradeGap Gap, TradeGapAffectedRange? Range)> FilterGaps(
        TradeGapScanResult scanResult,
        ExchangeId requestedExchange,
        Instrument requestedInstrument)
    {
        var result = new List<(TradeGap Gap, TradeGapAffectedRange? Range)>();

        for (var i = 0; i < scanResult.DetectedGaps.Count; i++)
        {
            var gap = scanResult.DetectedGaps[i];
            if (!gap.Exchange.Value.Equals(requestedExchange.Value, StringComparison.OrdinalIgnoreCase)
                || !gap.Symbol.Equals(requestedInstrument))
            {
                continue;
            }

            var range = i < scanResult.AffectedRanges.Count
                ? scanResult.AffectedRanges[i]
                : null;
            result.Add((gap, range));
        }

        return result;
    }

    private static string GetTradeRootDirectory(CandleGeneratorRunOptions runOptions)
    {
        var result = Path.Combine(Path.GetFullPath(runOptions.WorkDirectory), "trades-out");
        return result;
    }

    private static string GetCandleRootDirectory(CandleGeneratorRunOptions runOptions)
    {
        var result = Path.Combine(Path.GetFullPath(runOptions.WorkDirectory), "candles-out");
        return result;
    }

    private static void EnsureSupportedExchange(string exchange)
    {
        if (!exchange.Equals("Binance", StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException("Only exchange 'Binance' is currently supported for healing.");
        }
    }

    private static string FormatAffectedFileInfo(TradeGapAffectedRange? range)
    {
        var result = "unknown";

        if (range?.FromLocation is not null && range.ToLocation is not null)
        {
            if (range.FromLocation.FilePath.Equals(range.ToLocation.FilePath, StringComparison.Ordinal))
            {
                result = $"{range.FromLocation.FilePath}:{range.FromLocation.LineNumber}-{range.ToLocation.LineNumber}";
            }
            else
            {
                result = $"{range.FromLocation.FilePath}:{range.FromLocation.LineNumber} -> {range.ToLocation.FilePath}:{range.ToLocation.LineNumber}";
            }
        }
        else if (range?.FromLocation is not null)
        {
            result = $"{range.FromLocation.FilePath}:{range.FromLocation.LineNumber}";
        }
        else if (range?.ToLocation is not null)
        {
            result = $"{range.ToLocation.FilePath}:{range.ToLocation.LineNumber}";
        }

        return result;
    }
}
