using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Dispatches the CLI entrypoint into candle generation, gap scanning, or gap healing modes.
/// </summary>
public sealed class CliApplication(
    ICandleGenerationRunner candleGenerationRunner,
    ITradeGapScanner tradeGapScanner,
    ITradeGapHealer tradeGapHealer)
{
    /// <summary>
    /// Represents candidate file resolution for an optionally date-scoped scan or heal request.
    /// </summary>
    private sealed record CandidateFileResolution(
        IReadOnlyList<TradeGapAffectedFile> ResolvedFiles,
        IReadOnlyList<DateOnly> MissingDates,
        string ExpectedPathExample);

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
        CliOptions runOptions;

        if (CliCommand.IsHelpRequest(args))
        {
            CliCommand.WriteHelp(outputWriter);
        }
        else
        {
            try
            {
                runOptions = CliCommand.Parse(args);
            }
            catch (ArgumentException ex)
            {
                await errorWriter.WriteLineAsync(ex.Message).ConfigureAwait(false);
                result = 2;
                runOptions = new CliOptions(CliMode.Unknown, string.Empty, string.Empty, string.Empty, string.Empty, []);
            }

            if (result == 0)
            {
                try
                {
                    if (runOptions.Mode is CliMode.Scan)
                    {
                        result = await RunScan(runOptions, outputWriter, cancellationToken).ConfigureAwait(false);
                    }
                    else if (runOptions.Mode is CliMode.Heal)
                    {
                        result = await RunHeal(runOptions, outputWriter, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        result = await RunCandlize(runOptions, outputWriter, cancellationToken).ConfigureAwait(false);
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
    private async Task<int> RunCandlize(
        CliOptions runOptions,
        TextWriter outputWriter,
        CancellationToken cancellationToken)
    {
        var generatorOptions = new CliOptions(
            runOptions.Mode,
            runOptions.WorkDirectory,
            runOptions.Exchange,
            runOptions.Instrument,
            "1m",
            runOptions.Dates,
            "csv");
        var generationResult = await _candleGenerationRunner.GenerateAsync(generatorOptions, cancellationToken).ConfigureAwait(false);

        await outputWriter.WriteLineAsync($"Input trades:".PadLeft(20) + generationResult.InputTradeCount).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Unique trades:".PadLeft(20) + generationResult.UniqueTradeCount).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Duplicates dropped:".PadLeft(20) + generationResult.DuplicatesDropped).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Candles written:".PadLeft(20) + generationResult.CandleCount).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Output files:".PadLeft(20) + generationResult.OutputFileCount).ConfigureAwait(false);

        return 0;
    }

    /// <summary>
    /// Runs local gap scanning and writes a per-gap summary without treating gaps as failures.
    /// </summary>
    private async Task<int> RunScan(
        CliOptions runOptions,
        TextWriter outputWriter,
        CancellationToken cancellationToken)
    {
        var tradeRootDirectory = GetTradeRootDirectory(runOptions);
        var candidateFileResolution = ResolveCandidateFiles(tradeRootDirectory, runOptions.Exchange, runOptions.Instrument, runOptions.Dates);
        EnsureRequestedDatesWereResolved(runOptions, tradeRootDirectory, candidateFileResolution);
        var scanResult = await _tradeGapScanner
            .Scan(new TradeGapScanRequest(tradeRootDirectory, candidateFileResolution.ResolvedFiles, []), cancellationToken)
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
    private async Task<int> RunHeal(
        CliOptions runOptions,
        TextWriter outputWriter,
        CancellationToken cancellationToken)
    {
        EnsureSupportedExchange(runOptions.Exchange);

        var tradeRootDirectory = GetTradeRootDirectory(runOptions);
        var candidateFileResolution = ResolveCandidateFiles(tradeRootDirectory, runOptions.Exchange, runOptions.Instrument, runOptions.Dates);
        EnsureRequestedDatesWereResolved(runOptions, tradeRootDirectory, candidateFileResolution);
        var scanResult = await _tradeGapScanner
            .Scan(new TradeGapScanRequest(tradeRootDirectory, candidateFileResolution.ResolvedFiles, []), cancellationToken)
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
                        candidateFileResolution.ResolvedFiles,
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
    private static CandidateFileResolution ResolveCandidateFiles(
        string tradeRootDirectory,
        string exchange,
        string instrument,
        IReadOnlyList<DateOnly> dates)
    {
        var result = new List<TradeGapAffectedFile>();
        var missingDates = new List<DateOnly>();
        var exchangeId = new ExchangeId(exchange);
        var parsedInstrument = Instrument.Parse(instrument);
        var expectedPathExampleDate = dates.Count > 0
            ? dates.OrderBy(static value => value).First()
            : DateOnly.FromDateTime(DateTime.UtcNow);
        var expectedPathExample = TradeLocalDailyFilePath.Build(tradeRootDirectory, exchangeId, parsedInstrument, expectedPathExampleDate);

        if (dates.Count > 0)
        {
            foreach (var date in dates.OrderBy(static value => value))
            {
                var fullPath = TradeLocalDailyFilePath.Build(tradeRootDirectory, exchangeId, parsedInstrument, date);
                if (File.Exists(fullPath))
                {
                    var relativePath = Path.GetRelativePath(tradeRootDirectory, fullPath);
                    result.Add(new TradeGapAffectedFile(relativePath, date));
                }
                else
                {
                    missingDates.Add(date);
                }
            }
        }
        else
        {
            foreach (var completedFile in TradeLocalDailyFilePath.DiscoverCompleted(tradeRootDirectory, exchangeId, parsedInstrument))
            {
                var relativePath = Path.GetRelativePath(tradeRootDirectory, completedFile.Path);
                result.Add(new TradeGapAffectedFile(relativePath, completedFile.UtcDate));
            }
        }

        var resolution = new CandidateFileResolution(result, missingDates, expectedPathExample);
        return resolution;
    }

    /// <summary>
    /// Throws when one or more explicitly requested trade dates do not resolve to local files.
    /// </summary>
    private static void EnsureRequestedDatesWereResolved(
        CliOptions runOptions,
        string tradeRootDirectory,
        CandidateFileResolution candidateFileResolution)
    {
        if (runOptions.Dates.Count == 0 || candidateFileResolution.MissingDates.Count == 0)
        {
            return;
        }

        var requestedDates = string.Join(", ", runOptions.Dates.Select(static date => date.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture)));
        var missingDates = string.Join(", ", candidateFileResolution.MissingDates.Select(static date => date.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture)));

        throw new ArgumentException(
            $"Requested trade file scope could not be resolved for exchange '{runOptions.Exchange}', instrument '{runOptions.Instrument}', requested date(s) [{requestedDates}], missing date(s) [{missingDates}], root directory '{tradeRootDirectory}'. Expected path example: '{candidateFileResolution.ExpectedPathExample}'.");
    }

    /// <summary>
    /// Gets the configured trade root directory exactly as supplied by the caller.
    /// </summary>
    private static string GetTradeRootDirectory(CliOptions runOptions)
    {
        var result = Path.GetFullPath(runOptions.WorkDirectory);
        return result;
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
