using QuantaCandle.Core.Trading;
using QuantaCandle.Infra;

namespace QuantaCandle.CLI;

internal class QuantaCandleRunner(
    ITradeGapScanner gapScanner,
    ITradeGapHealer gapHealer,
    ITradeGapScanAugmenter tradeGapScanAugmenter) : IQuantaCandleRunner
{
    private enum ScanGapKind
    {
        Interior = 1,
        StartBoundary = 2,
        EndBoundary = 3,
        Unknown = 4,
    }

    private readonly ITradeGapScanner _gapScanner = gapScanner ?? throw new ArgumentNullException(nameof(gapScanner));
    private readonly ITradeGapHealer _gapHealer = gapHealer ?? throw new ArgumentNullException(nameof(gapHealer));
    private readonly ITradeGapScanAugmenter _tradeGapScanAugmenter = tradeGapScanAugmenter ?? throw new ArgumentNullException(nameof(tradeGapScanAugmenter));

    public async Task<int> Candlize(CliOptions runOptions, TextWriter outputWriter, CancellationToken cancellationToken)
    {
        var generatorOptions = new CliOptions(
            runOptions.Mode,
            runOptions.WorkDirectory,
            runOptions.Exchange,
            runOptions.Instrument,
            "1m",
            runOptions.Dates,
            "csv");
        var generationResult = await TradeToCandleGenerator.Run(generatorOptions, cancellationToken).ConfigureAwait(false);

        await outputWriter.WriteLineAsync($"Input trades:".PadLeft(20) + generationResult.InputTradeCount).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Unique trades:".PadLeft(20) + generationResult.UniqueTradeCount).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Duplicates dropped:".PadLeft(20) + generationResult.DuplicatesDropped).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Candles written:".PadLeft(20) + generationResult.CandleCount).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Output files:".PadLeft(20) + generationResult.OutputFileCount).ConfigureAwait(false);

        return 0;
    }

    public async Task<int> Heal(CliOptions runOptions, TextWriter outputWriter, CancellationToken cancellationToken)
    {
        EnsureSupportedExchange(runOptions.Exchange);

        var tradeRootDirectory = GetTradeRootDirectory(runOptions);
        var candidateFileResolution = ResolveCandidateFiles(tradeRootDirectory, runOptions.Exchange, runOptions.Instrument, runOptions.Dates);
        EnsureRequestedDatesWereResolved(runOptions, tradeRootDirectory, candidateFileResolution);
        var scanResult = await _gapScanner
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

            var healResult = await _gapHealer
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

    public async Task<int> Scan(CliOptions runOptions, TextWriter outputWriter, CancellationToken terminator)
    {
        var tradeRootDirectory = GetTradeRootDirectory(runOptions);
        var candidateFileResolution = ResolveCandidateFiles(tradeRootDirectory, runOptions.Exchange, runOptions.Instrument, runOptions.Dates);
        EnsureRequestedDatesWereResolved(runOptions, tradeRootDirectory, candidateFileResolution);
        var scanRequest = new TradeGapScanRequest(tradeRootDirectory, candidateFileResolution.ResolvedFiles, []);
        var scanResult = await _gapScanner
            .Scan(scanRequest, terminator)
            .ConfigureAwait(false);
        scanResult = await _tradeGapScanAugmenter
            .Augment(scanRequest, scanResult, terminator)
            .ConfigureAwait(false);
        var requestedExchange = new ExchangeId(runOptions.Exchange);
        var requestedInstrument = Instrument.Parse(runOptions.Instrument);
        var filteredGaps = FilterGaps(scanResult, requestedExchange, requestedInstrument);

        await outputWriter.WriteLineAsync($"Files scanned:".PadLeft(20) + scanResult.TotalFilesScanned).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Trades scanned:".PadLeft(20) + scanResult.TotalTradesScanned).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Gaps found:".PadLeft(20) + filteredGaps.Count).ConfigureAwait(false);
        await WriteVerboseScanFileSummary(outputWriter, tradeRootDirectory, scanResult.AffectedFiles, filteredGaps).ConfigureAwait(false);

        for (var i = 0; i < filteredGaps.Count; i++)
        {
            var gapWithRange = filteredGaps[i];
            var gap = gapWithRange.Gap;
            var missingRange = gap.MissingTradeIds is null
                ? "unknown"
                : $"{gap.MissingTradeIds.Value.FirstTradeId}-{gap.MissingTradeIds.Value.LastTradeId}";
            var affectedFileInfo = FormatAffectedFileInfo(gapWithRange.Range);
            var gapKind = DescribeGapKind(gapWithRange.Gap, gapWithRange.Range);

            await outputWriter
                .WriteLineAsync($"Gap {i + 1}: kind={FormatGapKind(gapKind)} exchange={gap.Exchange} instrument={gap.Symbol} missing={missingRange} files={affectedFileInfo}")
                .ConfigureAwait(false);
        }

        return 0;
    }

    /// <summary>
    /// Writes per-file scan details including file existence and boundary-check outcomes.
    /// </summary>
    private static async Task WriteVerboseScanFileSummary(
        TextWriter outputWriter,
        string tradeRootDirectory,
        IReadOnlyList<TradeGapAffectedFile> affectedFiles,
        IReadOnlyList<(TradeGap Gap, TradeGapAffectedRange? Range)> filteredGaps)
    {
        for (var i = 0; i < affectedFiles.Count; i++)
        {
            var affectedFile = affectedFiles[i];
            var fullPath = Path.GetFullPath(Path.Combine(tradeRootDirectory, affectedFile.Path));
            var fileExists = File.Exists(fullPath);
            var startBoundaryGap = FindBoundaryGapForFile(affectedFile.Path, filteredGaps, ScanGapKind.StartBoundary);
            var endBoundaryGap = FindBoundaryGapForFile(affectedFile.Path, filteredGaps, ScanGapKind.EndBoundary);
            var interiorGapCount = CountInteriorGapsForFile(affectedFile.Path, filteredGaps);
            var tradingDay = affectedFile.TradingDay?.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture) ?? "unknown";

            await outputWriter
                .WriteLineAsync($"File {i + 1}: path={affectedFile.Path} exists={fileExists.ToString().ToLowerInvariant()} tradingDay={tradingDay}")
                .ConfigureAwait(false);
            await outputWriter
                .WriteLineAsync($"         boundary-start={FormatBoundaryStatus(startBoundaryGap)} boundary-end={FormatBoundaryStatus(endBoundaryGap)} interior-gaps={interiorGapCount}")
                .ConfigureAwait(false);
        }
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

    private static string FormatBoundaryStatus((TradeGap Gap, TradeGapAffectedRange? Range)? gapWithRange)
    {
        var result = "ok";

        if (gapWithRange is not null)
        {
            var gap = gapWithRange.Value.Gap;
            result = gap.MissingTradeIds is null
                ? "gap unknown"
                : $"gap {gap.MissingTradeIds.Value.FirstTradeId}-{gap.MissingTradeIds.Value.LastTradeId}";
        }

        return result;
    }

    private static string FormatGapKind(ScanGapKind gapKind)
    {
        var result = gapKind switch
        {
            ScanGapKind.StartBoundary => "start-boundary",
            ScanGapKind.EndBoundary => "end-boundary",
            ScanGapKind.Interior => "interior",
            _ => "unknown",
        };
        return result;
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
            var isRequested = gap.Exchange.Value.Equals(requestedExchange.Value, StringComparison.OrdinalIgnoreCase)
                || gap.Symbol.Equals(requestedInstrument);

            if (isRequested)
            {
                var range = i < scanResult.AffectedRanges.Count ? scanResult.AffectedRanges[i] : null;
                result.Add((gap, range));
            }
        }

        return result;
    }

    private static int CountInteriorGapsForFile(
        string relativePath,
        IReadOnlyList<(TradeGap Gap, TradeGapAffectedRange? Range)> filteredGaps)
    {
        var result = 0;

        for (var i = 0; i < filteredGaps.Count; i++)
        {
            var gapWithRange = filteredGaps[i];
            if (DescribeGapKind(gapWithRange.Gap, gapWithRange.Range) != ScanGapKind.Interior)
            {
                continue;
            }

            if (GapTouchesFile(gapWithRange.Range, relativePath))
            {
                result++;
            }
        }

        return result;
    }

    private static ScanGapKind DescribeGapKind(TradeGap gap, TradeGapAffectedRange? range)
    {
        var result = ScanGapKind.Unknown;

        if (range is not null
            && range.FromLocation is not null
            && range.ToLocation is not null
            && range.FromLocation.FilePath.Equals(range.ToLocation.FilePath, StringComparison.Ordinal)
            && range.FromLocation.LineNumber == range.ToLocation.LineNumber
            && gap.ToInclusive is not null)
        {
            if (gap.ToInclusive.Value.TradeId.Equals(range.ToInclusive.TradeId, StringComparison.Ordinal))
            {
                result = ScanGapKind.StartBoundary;
            }
            else if (gap.FromExclusive.TradeId.Equals(range.FromInclusive.TradeId, StringComparison.Ordinal))
            {
                result = ScanGapKind.EndBoundary;
            }
        }
        else if (range is not null)
        {
            result = ScanGapKind.Interior;
        }

        return result;
    }

    private static (TradeGap Gap, TradeGapAffectedRange? Range)? FindBoundaryGapForFile(
        string relativePath,
        IReadOnlyList<(TradeGap Gap, TradeGapAffectedRange? Range)> filteredGaps,
        ScanGapKind expectedKind)
    {
        (TradeGap Gap, TradeGapAffectedRange? Range)? result = null;

        for (var i = 0; i < filteredGaps.Count; i++)
        {
            var gapWithRange = filteredGaps[i];
            if (DescribeGapKind(gapWithRange.Gap, gapWithRange.Range) != expectedKind)
            {
                continue;
            }

            if (GapTouchesFile(gapWithRange.Range, relativePath))
            {
                result = gapWithRange;
                break;
            }
        }

        return result;
    }

    private static bool GapTouchesFile(TradeGapAffectedRange? range, string relativePath)
    {
        var result = false;

        if (range?.FromLocation is not null && range.FromLocation.FilePath.Equals(relativePath, StringComparison.Ordinal))
        {
            result = true;
        }
        else if (range?.ToLocation is not null && range.ToLocation.FilePath.Equals(relativePath, StringComparison.Ordinal))
        {
            result = true;
        }

        return result;
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
}
