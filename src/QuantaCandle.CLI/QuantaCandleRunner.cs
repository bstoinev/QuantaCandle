using QuantaCandle.Core.Trading;
using QuantaCandle.Infra;

namespace QuantaCandle.CLI;

internal class QuantaCandleRunner(
    ITradeGapScanner gapScanner,
    ITradeGapHealer gapHealer,
    ITradeGapScanAugmenter tradeGapScanAugmenter,
    ITradeDayFileBootstrapper? tradeDayFileBootstrapper = null) : IQuantaCandleRunner
{
    private const int CandlizeFileProgressBarWidth = 24;

    private enum ScanGapKind
    {
        Interior = 1,
        StartBoundary = 2,
        EndBoundary = 3,
        Unknown = 4,
    }

    /// <summary>
    /// Represents one file-local healing pass with exact missing trade ranges.
    /// </summary>
    private sealed record HealPass(
        TradeGapAffectedFile CandidateFile,
        ScanGapKind GapKind,
        IReadOnlyList<MissingTradeIdRange> MissingTradeRanges,
        TradeGapAffectedRange? AffectedRange)
    {
        /// <summary>
        /// Gets the inclusive fetch envelope start for the pass.
        /// </summary>
        public long MissingTradeIdStart { get; } = MissingTradeRanges[0].FirstTradeId;

        /// <summary>
        /// Gets the inclusive fetch envelope end for the pass.
        /// </summary>
        public long MissingTradeIdEnd { get; } = MissingTradeRanges[^1].LastTradeId;
    }

    /// <summary>
    /// Aggregates CLI heal counters across independent file scans and healing passes.
    /// </summary>
    private sealed class HealSummary
    {
        public int FilesScanned { get; set; }

        public int TradesScanned { get; set; }

        public int GapsFound { get; set; }

        public int GapsHealedFull { get; set; }

        public int GapsHealedPartial { get; set; }

        public int GapsUnchanged { get; set; }
    }

    private readonly ITradeGapScanner _gapScanner = gapScanner ?? throw new ArgumentNullException(nameof(gapScanner));
    private readonly ITradeGapHealer _gapHealer = gapHealer ?? throw new ArgumentNullException(nameof(gapHealer));
    private readonly ITradeGapScanAugmenter _tradeGapScanAugmenter = tradeGapScanAugmenter ?? throw new ArgumentNullException(nameof(tradeGapScanAugmenter));
    private readonly ITradeDayFileBootstrapper? _tradeDayFileBootstrapper = tradeDayFileBootstrapper;

    public async Task<int> Candlize(CliOptions runOptions, TextWriter outputWriter, CancellationToken cancellationToken)
    {
        var tradeRootDirectory = CliPathRootResolver.GetTradeDataRoot(runOptions.WorkDirectory);
        var generationResult = await TradeToCandleGenerator
            .Run(
                runOptions with { Format = "csv" },
                async (filePath, currentFileCount, totalFileCount, isCompleted, reporterCancellationToken) =>
                {
                    await WriteCandlizeFileProgress(
                            outputWriter,
                            tradeRootDirectory,
                            filePath,
                            currentFileCount,
                            totalFileCount,
                            isCompleted,
                            reporterCancellationToken)
                        .ConfigureAwait(false);
                },
                cancellationToken)
            .ConfigureAwait(false);

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
        EnsureTradeInputDirectoryExists(tradeRootDirectory, runOptions.Exchange, runOptions.Instrument);
        await outputWriter.WriteLineAsync($"Scanning local trade files for {runOptions.Exchange}:{runOptions.Instrument}...").ConfigureAwait(false);
        var candidateFileResolution = ResolveCandidateFiles(tradeRootDirectory, runOptions.Exchange, runOptions.Instrument, runOptions.Dates);
        var requestedExchange = new ExchangeId(runOptions.Exchange);
        var requestedInstrument = Instrument.Parse(runOptions.Instrument);
        var candidateFiles = await ResolveHealCandidateFiles(
                tradeRootDirectory,
                requestedExchange,
                requestedInstrument,
                runOptions.Dates,
                candidateFileResolution,
                outputWriter,
                cancellationToken)
            .ConfigureAwait(false);
        var summary = new HealSummary();
        var totalFileCount = candidateFiles.Count;

        for (var fileIndex = 0; fileIndex < totalFileCount; fileIndex++)
        {
            var candidateFile = candidateFiles[fileIndex];
            await outputWriter
                .WriteLineAsync($"File {fileIndex + 1}/{totalFileCount}: scanning '{candidateFile.Path}'.")
                .ConfigureAwait(false);

            var scanRequest = new TradeGapScanRequest(tradeRootDirectory, [candidateFile], []);
            var scanResult = await _gapScanner
                .Scan(scanRequest, cancellationToken)
                .ConfigureAwait(false);
            scanResult = await _tradeGapScanAugmenter
                .Augment(scanRequest, scanResult, cancellationToken)
                .ConfigureAwait(false);

            var filteredGaps = FilterGaps(scanResult, requestedExchange, requestedInstrument);
            var healPasses = BuildHealPasses(filteredGaps, candidateFile);

            summary.FilesScanned += scanResult.TotalFilesScanned;
            summary.TradesScanned += scanResult.TotalTradesScanned;
            summary.GapsFound += filteredGaps.Count;

            await outputWriter
                .WriteLineAsync($"File {fileIndex + 1}/{totalFileCount}: found {filteredGaps.Count} bounded gap(s) and scheduled {healPasses.Count} healing pass(es).")
                .ConfigureAwait(false);

            for (var passIndex = 0; passIndex < healPasses.Count; passIndex++)
            {
                var healPass = healPasses[passIndex];
                var requestedRangeSummary = string.Join(", ", healPass.MissingTradeRanges.Select(FormatMissingTradeRange));
                var passLabel = $"{FormatGapKind(healPass.GapKind)} pass {passIndex + 1}/{healPasses.Count}";
                var progressPrefix = $"File {fileIndex + 1}/{totalFileCount} {passLabel}: ";

                await outputWriter
                    .WriteLineAsync(
                        $"File {fileIndex + 1}/{totalFileCount} {passLabel}: healing '{healPass.CandidateFile.Path}' with exact missing range(s) [{requestedRangeSummary}] via fetch envelope {healPass.MissingTradeIdStart}-{healPass.MissingTradeIdEnd}.")
                    .ConfigureAwait(false);

                var healResult = await _gapHealer
                    .Heal(
                        new TradeGapHealRequest(
                            tradeRootDirectory,
                            requestedExchange,
                            requestedInstrument,
                            healPass.MissingTradeIdStart,
                            healPass.MissingTradeIdEnd,
                            [healPass.CandidateFile],
                            healPass.AffectedRange,
                            healPass.MissingTradeRanges,
                            new TextWriterTradeGapProgressReporter(outputWriter, progressPrefix)),
                        cancellationToken)
                    .ConfigureAwait(false);

                await outputWriter
                    .WriteLineAsync(
                        $"File {fileIndex + 1}/{totalFileCount} {passLabel} result: outcome={healResult.Outcome} fetched={healResult.FetchedTradeCount} inserted={healResult.InsertedTradeCount} unresolved={healResult.UnresolvedTradeRanges.Count}.")
                    .ConfigureAwait(false);

                AccumulateHealOutcome(summary, healResult);
            }
        }

        await outputWriter.WriteLineAsync($"Files scanned:".PadLeft(20) + summary.FilesScanned).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Trades scanned:".PadLeft(20) + summary.TradesScanned).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Gaps found:".PadLeft(20) + summary.GapsFound).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Gaps healed full:".PadLeft(20) + summary.GapsHealedFull).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Gaps healed partial:".PadLeft(20) + summary.GapsHealedPartial).ConfigureAwait(false);
        await outputWriter.WriteLineAsync($"Gaps unchanged:".PadLeft(20) + summary.GapsUnchanged).ConfigureAwait(false);

        return 0;
    }

    public async Task<int> Scan(CliOptions runOptions, TextWriter outputWriter, CancellationToken terminator)
    {
        var tradeRootDirectory = GetTradeRootDirectory(runOptions);
        EnsureTradeInputDirectoryExists(tradeRootDirectory, runOptions.Exchange, runOptions.Instrument);
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
        var result = CliPathRootResolver.GetTradeDataRoot(runOptions.WorkDirectory);
        return result;
    }

    /// <summary>
    /// Resolves the per-file heal scope, bootstrapping only explicitly requested missing day files.
    /// </summary>
    private async ValueTask<List<TradeGapAffectedFile>> ResolveHealCandidateFiles(
        string tradeRootDirectory,
        ExchangeId exchange,
        Instrument instrument,
        IReadOnlyList<DateOnly> requestedDates,
        CandidateFileResolution candidateFileResolution,
        TextWriter outputWriter,
        CancellationToken cancellationToken)
    {
        var result = candidateFileResolution.ResolvedFiles
            .OrderBy(static file => file.Path, StringComparer.Ordinal)
            .ToList();

        if (requestedDates.Count == 0 || candidateFileResolution.MissingFiles.Count == 0)
        {
            return result;
        }

        if (_tradeDayFileBootstrapper is null)
        {
            throw new InvalidOperationException("Trade day file bootstrapper is not configured.");
        }

        foreach (var missingFile in candidateFileResolution.MissingFiles.OrderBy(static file => file.Path, StringComparer.Ordinal))
        {
            var utcDate = missingFile.TradingDay
                ?? throw new InvalidOperationException($"Unable to bootstrap requested trade file '{missingFile.Path}' because its UTC day is unknown.");

            await outputWriter
                .WriteLineAsync($"Bootstrapping missing requested UTC day '{utcDate:yyyy-MM-dd}' into '{missingFile.Path}'.")
                .ConfigureAwait(false);

            var bootstrappedFile = await _tradeDayFileBootstrapper
                .Bootstrap(tradeRootDirectory, exchange, instrument, utcDate, cancellationToken)
                .ConfigureAwait(false);

            result.Add(bootstrappedFile);
        }

        result = result
            .OrderBy(static file => file.TradingDay)
            .ThenBy(static file => file.Path, StringComparer.Ordinal)
            .ToList();

        return result;
    }

    private static void EnsureTradeInputDirectoryExists(string tradeRootDirectory, string exchange, string instrument)
    {
        var expectedDirectory = Path.Combine(tradeRootDirectory, exchange, instrument);
        if (!Directory.Exists(expectedDirectory))
        {
            var expectedPathExample = Path.Combine(expectedDirectory, "2026-03-28.jsonl");
            throw new ArgumentException(
                $"Expected trade input structure '<workDir>\\trade-data\\<exchange>\\<instrument>\\yyyy-MM-dd.jsonl'. Missing directory '{expectedDirectory}'. Example path: '{expectedPathExample}'.");
        }
    }

    /// <summary>
    /// Writes deterministic per-file candlize progress using the existing CLI text style.
    /// </summary>
    private static async Task WriteCandlizeFileProgress(
        TextWriter outputWriter,
        string tradeRootDirectory,
        string filePath,
        int currentFileCount,
        int totalFileCount,
        bool isCompleted,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var relativePath = Path.GetRelativePath(tradeRootDirectory, filePath);
        if (!isCompleted)
        {
            await outputWriter
                .WriteLineAsync($"File {currentFileCount + 1}/{totalFileCount}: processing '{relativePath}'.")
                .ConfigureAwait(false);
        }
        else
        {
            var renderedProgress = RenderCandlizeFileProgress(currentFileCount, totalFileCount);
            await outputWriter
                .WriteLineAsync($"Candlize progress: {renderedProgress} '{relativePath}'")
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Renders the pseudo-graphic file progress line for candlize execution.
    /// </summary>
    private static string RenderCandlizeFileProgress(int completedFileCount, int totalFileCount)
    {
        var completedRatio = totalFileCount == 0
            ? 1m
            : Math.Clamp((decimal)completedFileCount / totalFileCount, 0m, 1m);
        var filledWidth = (int)Math.Round(completedRatio * CandlizeFileProgressBarWidth, MidpointRounding.AwayFromZero);
        var bar = "[" + new string('#', filledWidth) + new string('-', CandlizeFileProgressBarWidth - filledWidth) + "]";
        var percent = (completedRatio * 100m).ToString("0", System.Globalization.CultureInfo.InvariantCulture).PadLeft(3);
        var result = $"{bar} {percent}% files {completedFileCount}/{totalFileCount}";
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
                && gap.Symbol.Equals(requestedInstrument);

            if (isRequested)
            {
                var range = i < scanResult.AffectedRanges.Count ? scanResult.AffectedRanges[i] : null;
                result.Add((gap, range));
            }
        }

        return result;
    }

    /// <summary>
    /// Builds ordered healing passes for one scanned candidate file.
    /// Boundary gaps execute before interior gaps.
    /// </summary>
    private static List<HealPass> BuildHealPasses(
        IReadOnlyList<(TradeGap Gap, TradeGapAffectedRange? Range)> filteredGaps,
        TradeGapAffectedFile candidateFile)
    {
        var boundaryRanges = new List<MissingTradeIdRange>();
        var interiorRanges = new List<MissingTradeIdRange>();
        TradeGapAffectedRange? firstBoundaryAffectedRange = null;
        TradeGapAffectedRange? firstInteriorAffectedRange = null;

        foreach (var gapWithRange in filteredGaps)
        {
            if (gapWithRange.Gap.MissingTradeIds is null)
            {
                continue;
            }

            var gapKind = DescribeGapKind(gapWithRange.Gap, gapWithRange.Range);
            if (gapKind == ScanGapKind.StartBoundary || gapKind == ScanGapKind.EndBoundary)
            {
                firstBoundaryAffectedRange ??= gapWithRange.Range;
                boundaryRanges.Add(gapWithRange.Gap.MissingTradeIds.Value);
            }
            else if (gapKind == ScanGapKind.Interior)
            {
                firstInteriorAffectedRange ??= gapWithRange.Range;
                interiorRanges.Add(gapWithRange.Gap.MissingTradeIds.Value);
            }
        }

        var result = new List<HealPass>();
        if (boundaryRanges.Count > 0)
        {
            result.Add(new HealPass(candidateFile, ScanGapKind.StartBoundary, boundaryRanges.MergeContiguous(), firstBoundaryAffectedRange));
        }

        if (interiorRanges.Count > 0)
        {
            result.Add(new HealPass(candidateFile, ScanGapKind.Interior, interiorRanges.MergeContiguous(), firstInteriorAffectedRange));
        }

        return result;
    }

    private static string FormatMissingTradeRange(MissingTradeIdRange range)
    {
        var result = range.FirstTradeId == range.LastTradeId
            ? range.FirstTradeId.ToString(System.Globalization.CultureInfo.InvariantCulture)
            : $"{range.FirstTradeId}-{range.LastTradeId}";
        return result;
    }

    private static void AccumulateHealOutcome(HealSummary summary, TradeGapHealResult healResult)
    {
        if (healResult.Outcome == TradeGapHealStatus.Full)
        {
            summary.GapsHealedFull++;
        }
        else if (healResult.Outcome == TradeGapHealStatus.Partial)
        {
            summary.GapsHealedPartial++;
        }
        else
        {
            summary.GapsUnchanged++;
        }
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
        var missingFiles = new List<TradeGapAffectedFile>();
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
                    var relativePath = Path.GetRelativePath(tradeRootDirectory, fullPath);
                    missingFiles.Add(new TradeGapAffectedFile(relativePath, date));
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

        var resolution = new CandidateFileResolution(result, missingFiles, missingDates, expectedPathExample);
        return resolution;
    }
}
