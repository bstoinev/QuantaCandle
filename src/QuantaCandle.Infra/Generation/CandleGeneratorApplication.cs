using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Dispatches the candle generator executable into candle generation or local gap scanning modes.
/// </summary>
public sealed class CandleGeneratorApplication(ICandleGenerationRunner candleGenerationRunner, ITradeGapScanner tradeGapScanner)
{
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
                runOptions = new CandleGeneratorRunOptions(CandleGeneratorMode.GenerateCandles, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, []);
            }

            if (result == 0)
            {
                try
                {
                    if (runOptions.Mode == CandleGeneratorMode.ScanGaps)
                    {
                        result = await RunScanGapsAsync(runOptions, outputWriter, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        result = await RunGenerateCandlesAsync(runOptions, outputWriter, cancellationToken).ConfigureAwait(false);
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
    private async Task<int> RunGenerateCandlesAsync(
        CandleGeneratorRunOptions runOptions,
        TextWriter outputWriter,
        CancellationToken cancellationToken)
    {
        var generatorOptions = new TradeToCandleGeneratorOptions(
            runOptions.InputDirectory,
            runOptions.OutputDirectory,
            runOptions.Source,
            runOptions.Timeframe,
            runOptions.Format);
        var result = await candleGenerationRunner.GenerateAsync(generatorOptions, cancellationToken).ConfigureAwait(false);

        await outputWriter.WriteLineAsync($"Input trades:".PadLeft(20) + result.InputTradeCount).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Unique trades:".PadLeft(20) + result.UniqueTradeCount).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Duplicates dropped:".PadLeft(20) + result.DuplicatesDropped).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Candles written:".PadLeft(20) + result.CandleCount).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Output files:".PadLeft(20) + result.OutputFileCount).ConfigureAwait(false);

        return 0;
    }

    /// <summary>
    /// Runs local gap scanning and writes a per-gap summary without treating gaps as failures.
    /// </summary>
    private async Task<int> RunScanGapsAsync(
        CandleGeneratorRunOptions runOptions,
        TextWriter outputWriter,
        CancellationToken cancellationToken)
    {
        var candidateFiles = ResolveCandidateFiles(runOptions.InputDirectory, runOptions.ScanDates);
        var scanResult = await tradeGapScanner
            .Scan(new TradeGapScanRequest(runOptions.InputDirectory, candidateFiles, []), cancellationToken)
            .ConfigureAwait(false);

        await outputWriter.WriteLineAsync($"Files scanned:".PadLeft(20) + scanResult.TotalFilesScanned).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Trades scanned:".PadLeft(20) + scanResult.TotalTradesScanned).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Gaps found:".PadLeft(20) + scanResult.DetectedGaps.Count).ConfigureAwait(false);

        for (var i = 0; i < scanResult.DetectedGaps.Count; i++)
        {
            var gap = scanResult.DetectedGaps[i];
            var range = i < scanResult.AffectedRanges.Count ? scanResult.AffectedRanges[i] : null;
            var missingRange = gap.MissingTradeIds is null
                ? "unknown"
                : $"{gap.MissingTradeIds.Value.FirstTradeId}-{gap.MissingTradeIds.Value.LastTradeId}";
            var affectedFileInfo = FormatAffectedFileInfo(range);

            await outputWriter.WriteLineAsync(
                    $"Gap {i + 1}: exchange={gap.Exchange} instrument={gap.Symbol} missing={missingRange} files={affectedFileInfo}")
                .ConfigureAwait(false);
        }

        return 0;
    }

    /// <summary>
    /// Resolves requested scan dates into candidate files under the supplied root directory.
    /// </summary>
    private static IReadOnlyList<TradeGapAffectedFile> ResolveCandidateFiles(string rootDirectory, IReadOnlyList<DateOnly> scanDates)
    {
        var result = new List<TradeGapAffectedFile>();

        if (scanDates.Count > 0 && Directory.Exists(rootDirectory))
        {
            var requestedFileNames = scanDates
                .Select(static date => date.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture) + ".jsonl")
                .ToHashSet(StringComparer.Ordinal);

            foreach (var filePath in Directory.EnumerateFiles(rootDirectory, "*.jsonl", SearchOption.AllDirectories).OrderBy(path => path, StringComparer.Ordinal))
            {
                var fileName = Path.GetFileName(filePath);
                if (requestedFileNames.Contains(fileName))
                {
                    var relativePath = Path.GetRelativePath(rootDirectory, filePath);
                    var tradingDay = DateOnly.ParseExact(Path.GetFileNameWithoutExtension(filePath), "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
                    result.Add(new TradeGapAffectedFile(relativePath, tradingDay));
                }
            }
        }

        return result;
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
