using System.Globalization;

using LogMachina;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Storage;

/// <summary>
/// Augments a read-only scan result with strict UTC day-boundary gap findings for local daily raw-trade files.
/// </summary>
public sealed class StrictTradeDayBoundaryScanAugmenter(
    ITradeDayBoundaryResolver tradeDayBoundaryResolver,
    ILogMachina<StrictTradeDayBoundaryScanAugmenter> log) : ITradeGapScanAugmenter
{
    private readonly ILogMachina<StrictTradeDayBoundaryScanAugmenter> _log = log ?? throw new ArgumentNullException(nameof(log));
    private readonly ITradeDayBoundaryResolver _tradeDayBoundaryResolver = tradeDayBoundaryResolver ?? throw new ArgumentNullException(nameof(tradeDayBoundaryResolver));

    private readonly record struct BoundaryTrade(
        ExchangeId Exchange,
        Instrument Symbol,
        string TradeId,
        long NumericTradeId,
        DateTimeOffset Timestamp,
        string RelativeFilePath,
        int LineNumber);

    private readonly record struct BoundaryTradePair(BoundaryTrade FirstTrade, BoundaryTrade LastTrade);

    private readonly record struct ScannedFile(string FullPath, string RelativePath, DateOnly? TradingDay);

    /// <summary>
    /// Resolves strict UTC day boundaries per scanned file and appends any missing start or end boundary gaps.
    /// </summary>
    public async ValueTask<TradeGapScanResult> Augment(
        TradeGapScanRequest request,
        TradeGapScanResult scanResult,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(scanResult);

        var scannedFiles = ResolveFilesToScan(request.RootDirectory, request.CandidateFiles);
        if (scannedFiles.Count == 0)
        {
            return scanResult;
        }

        var detectedGaps = scanResult.DetectedGaps.ToList();
        var affectedRanges = scanResult.AffectedRanges.ToList();

        foreach (var file in scannedFiles)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var tradingDay = file.TradingDay
                ?? throw new InvalidOperationException($"Unable to resolve UTC trading day for scanned file '{file.RelativePath}'.");
            var tradePair = await ReadBoundaryTrades(file, cancellationToken).ConfigureAwait(false);

            _log.Trace($"Resolving strict UTC boundary gap scan for {tradePair.FirstTrade.Exchange}:{tradePair.FirstTrade.Symbol} on {tradingDay:yyyy-MM-dd} from '{file.RelativePath}'.");
            var boundary = await _tradeDayBoundaryResolver
                .Resolve(
                    tradePair.FirstTrade.Exchange,
                    tradePair.FirstTrade.Symbol,
                    tradingDay,
                    TradeDayBoundaryResolutionMode.Strict,
                    cancellationToken)
                .ConfigureAwait(false);

            AppendStartBoundaryGap(boundary, tradePair.FirstTrade, detectedGaps, affectedRanges);
            AppendEndBoundaryGap(boundary, tradePair.LastTrade, detectedGaps, affectedRanges);
        }

        _log.Info($"Boundary scan augmentation appended {detectedGaps.Count - scanResult.DetectedGaps.Count} gap(s) across {scannedFiles.Count} file(s).");

        return new TradeGapScanResult(
            scanResult.TotalFilesScanned,
            scanResult.TotalTradesScanned,
            scanResult.SkippedNonNumericTradeCount,
            detectedGaps,
            scanResult.AffectedFiles,
            affectedRanges);
    }

    private static void AppendEndBoundaryGap(
        TradeDayBoundary boundary,
        BoundaryTrade lastTrade,
        List<TradeGap> detectedGaps,
        List<TradeGapAffectedRange> affectedRanges)
    {
        if (!boundary.HasExpectedLastTradeId || lastTrade.NumericTradeId >= boundary.ExpectedLastTradeId!.Value)
        {
            return;
        }

        var missingTradeIds = new MissingTradeIdRange(lastTrade.NumericTradeId + 1, boundary.ExpectedLastTradeId.Value);
        var localWatermark = new TradeWatermark(lastTrade.TradeId, lastTrade.Timestamp);
        var syntheticUpperBoundary = new TradeWatermark(
            boundary.ExpectedLastTradeId.Value.ToString(CultureInfo.InvariantCulture),
            CreateNextUtcDayStart(boundary.UtcDate));
        var gap = TradeGap
            .CreateOpen(Guid.NewGuid(), boundary.Exchange, boundary.Symbol, localWatermark, lastTrade.Timestamp)
            .ToBounded(syntheticUpperBoundary, missingTradeIds);

        detectedGaps.Add(gap);
        affectedRanges.Add(
            new TradeGapAffectedRange(
                localWatermark,
                localWatermark,
                new TradeGapBoundaryLocation(lastTrade.RelativeFilePath, lastTrade.LineNumber),
                new TradeGapBoundaryLocation(lastTrade.RelativeFilePath, lastTrade.LineNumber)));
    }

    private static void AppendStartBoundaryGap(
        TradeDayBoundary boundary,
        BoundaryTrade firstTrade,
        List<TradeGap> detectedGaps,
        List<TradeGapAffectedRange> affectedRanges)
    {
        if (firstTrade.NumericTradeId <= boundary.ExpectedFirstTradeId)
        {
            return;
        }

        var missingTradeIds = new MissingTradeIdRange(boundary.ExpectedFirstTradeId, firstTrade.NumericTradeId - 1);
        var syntheticLowerBoundary = new TradeWatermark(
            (boundary.ExpectedFirstTradeId - 1L).ToString(CultureInfo.InvariantCulture),
            CreateUtcDayStart(boundary.UtcDate));
        var localWatermark = new TradeWatermark(firstTrade.TradeId, firstTrade.Timestamp);
        var gap = TradeGap
            .CreateOpen(Guid.NewGuid(), boundary.Exchange, boundary.Symbol, syntheticLowerBoundary, firstTrade.Timestamp)
            .ToBounded(localWatermark, missingTradeIds);

        detectedGaps.Add(gap);
        affectedRanges.Add(
            new TradeGapAffectedRange(
                localWatermark,
                localWatermark,
                new TradeGapBoundaryLocation(firstTrade.RelativeFilePath, firstTrade.LineNumber),
                new TradeGapBoundaryLocation(firstTrade.RelativeFilePath, firstTrade.LineNumber)));
    }

    private static DateTimeOffset CreateNextUtcDayStart(DateOnly tradingDay) => CreateUtcDayStart(tradingDay).AddDays(1);

    private static DateTimeOffset CreateUtcDayStart(DateOnly tradingDay) => new(tradingDay.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc));

    private static long ParseNumericTradeId(string tradeId, string filePath, int lineNumber)
    {
        if (!long.TryParse(tradeId, NumberStyles.None, CultureInfo.InvariantCulture, out var result))
        {
            throw new InvalidOperationException($"TradeId '{tradeId}' at line {lineNumber} in '{filePath}' is not numeric.");
        }

        return result;
    }

    private static async ValueTask<BoundaryTradePair> ReadBoundaryTrades(ScannedFile file, CancellationToken cancellationToken)
    {
        BoundaryTrade? firstTrade = null;
        BoundaryTrade? lastTrade = null;
        var lineNumber = 0;

        await foreach (var line in File.ReadLinesAsync(file.FullPath, cancellationToken).ConfigureAwait(false))
        {
            lineNumber++;
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var trade = LocalTradeJsonLineParser.ParseTrade(line, file.FullPath, lineNumber);
            var boundaryTrade = new BoundaryTrade(
                trade.Key.Exchange,
                trade.Key.Symbol,
                trade.Key.TradeId,
                ParseNumericTradeId(trade.Key.TradeId, file.FullPath, lineNumber),
                trade.Timestamp.ToUniversalTime(),
                file.RelativePath,
                lineNumber);

            if (firstTrade is null)
            {
                firstTrade = boundaryTrade;
            }

            lastTrade = boundaryTrade;
        }

        if (firstTrade is null || lastTrade is null)
        {
            throw new InvalidOperationException($"Scanned trade file '{file.RelativePath}' is empty.");
        }

        return new BoundaryTradePair(firstTrade.Value, lastTrade.Value);
    }

    private static List<ScannedFile> ResolveFilesToScan(string rootDirectory, IReadOnlyList<TradeGapAffectedFile> candidateFiles)
    {
        var result = new List<ScannedFile>();
        if (!Directory.Exists(rootDirectory))
        {
            return result;
        }

        if (candidateFiles.Count > 0)
        {
            foreach (var candidateFile in candidateFiles.OrderBy(static file => file.Path, StringComparer.Ordinal))
            {
                var fullPath = Path.GetFullPath(Path.Combine(rootDirectory, candidateFile.Path));
                if (!File.Exists(fullPath))
                {
                    continue;
                }

                result.Add(new ScannedFile(fullPath, candidateFile.Path, candidateFile.TradingDay ?? TryParseTradingDay(fullPath)));
            }
        }
        else
        {
            foreach (var fullPath in Directory.EnumerateFiles(rootDirectory, "*.jsonl", SearchOption.AllDirectories).OrderBy(static path => path, StringComparer.Ordinal))
            {
                var relativePath = Path.GetRelativePath(rootDirectory, fullPath);
                result.Add(new ScannedFile(fullPath, relativePath, TryParseTradingDay(fullPath)));
            }
        }

        return result;
    }

    private static DateOnly? TryParseTradingDay(string filePath)
    {
        DateOnly? result = null;
        var fileName = Path.GetFileNameWithoutExtension(filePath);

        if (!string.IsNullOrWhiteSpace(fileName)
            && DateOnly.TryParseExact(fileName, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var tradingDay))
        {
            result = tradingDay;
        }

        return result;
    }
}
