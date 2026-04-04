using System.Globalization;
using System.Text.Json;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Storage;

/// <summary>
/// Scans local recorder-style JSONL trade files and reports numeric trade identifier gaps without modifying files.
/// </summary>
public sealed class LocalFileTradeGapScanner : ITradeGapScanner
{
    private readonly record struct StreamKey(ExchangeId Exchange, Instrument Symbol);

    private readonly record struct ScannedTrade(
        ExchangeId Exchange,
        Instrument Symbol,
        string TradeId,
        long NumericTradeId,
        DateTimeOffset Timestamp,
        string RelativeFilePath,
        int LineNumber);

    /// <summary>
    /// Executes a scan over the requested local file set.
    /// </summary>
    public async ValueTask<TradeGapScanResult> Scan(TradeGapScanRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        var rootDirectory = request.RootDirectory;
        var scannedFiles = ResolveFilesToScan(rootDirectory, request.CandidateFiles);
        var tradesByStream = new Dictionary<StreamKey, List<ScannedTrade>>();
        var affectedFiles = new List<TradeGapAffectedFile>(scannedFiles.Count);
        var affectedRanges = new List<TradeGapAffectedRange>();
        var detectedGaps = new List<TradeGap>();
        var totalTradesScanned = 0;
        var skippedNonNumericTradeCount = 0;

        foreach (var file in scannedFiles)
        {
            cancellationToken.ThrowIfCancellationRequested();

            affectedFiles.Add(new TradeGapAffectedFile(file.RelativePath, file.TradingDay));

            var lineNumber = 0;
            await foreach (var line in File.ReadLinesAsync(file.FullPath, cancellationToken).ConfigureAwait(false))
            {
                lineNumber++;
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                var trade = ParseTrade(line, file.FullPath, lineNumber);
                totalTradesScanned++;

                if (!long.TryParse(trade.Key.TradeId, NumberStyles.None, CultureInfo.InvariantCulture, out var numericTradeId))
                {
                    throw new InvalidOperationException($"TradeId '{trade.Key.TradeId}' at line {lineNumber} in '{file.FullPath}' is not numeric.");
                }

                var streamKey = new StreamKey(trade.Key.Exchange, trade.Key.Symbol);
                if (!tradesByStream.TryGetValue(streamKey, out var streamTrades))
                {
                    streamTrades = [];
                    tradesByStream[streamKey] = streamTrades;
                }

                streamTrades.Add(new ScannedTrade(
                    trade.Key.Exchange,
                    trade.Key.Symbol,
                    trade.Key.TradeId,
                    numericTradeId,
                    trade.Timestamp.ToUniversalTime(),
                    file.RelativePath,
                    lineNumber));
            }
        }

        foreach (var pair in tradesByStream.OrderBy(pair => pair.Key.Symbol.ToString(), StringComparer.Ordinal).ThenBy(pair => pair.Key.Exchange.ToString(), StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            AppendDetectedGaps(pair.Value, detectedGaps, affectedRanges);
        }

        var result = new TradeGapScanResult(
            scannedFiles.Count,
            totalTradesScanned,
            skippedNonNumericTradeCount,
            detectedGaps,
            affectedFiles,
            affectedRanges);
        return result;
    }

    private static TradeInfo ParseTrade(string line, string filePath, int lineNumber)
    {
        try
        {
            using var document = JsonDocument.Parse(line);
            var root = document.RootElement;

            var exchangeText = root.GetProperty("exchange").GetString() ?? string.Empty;
            var instrumentText = root.GetProperty("instrument").GetString() ?? string.Empty;
            var tradeId = root.GetProperty("tradeId").GetString() ?? string.Empty;
            var timestamp = root.GetProperty("timestamp").GetDateTimeOffset().ToUniversalTime();
            var price = root.GetProperty("price").GetDecimal();
            var quantity = root.GetProperty("quantity").GetDecimal();

            if (string.IsNullOrWhiteSpace(exchangeText))
            {
                throw new InvalidOperationException("Exchange is missing.");
            }

            if (string.IsNullOrWhiteSpace(instrumentText))
            {
                throw new InvalidOperationException("Instrument is missing.");
            }

            if (string.IsNullOrWhiteSpace(tradeId))
            {
                throw new InvalidOperationException("TradeId is missing.");
            }

            if (price <= 0)
            {
                throw new InvalidOperationException("Price must be positive.");
            }

            if (quantity <= 0)
            {
                throw new InvalidOperationException("Quantity must be positive.");
            }

            var key = new TradeKey(
                new ExchangeId(exchangeText.Trim().ToLowerInvariant()),
                Instrument.Parse(instrumentText.Trim().ToUpperInvariant()),
                tradeId.Trim());

            var result = new TradeInfo(key, timestamp, price, quantity);
            return result;
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException or InvalidOperationException or JsonException or KeyNotFoundException)
        {
            throw new InvalidOperationException($"Failed to parse trade at line {lineNumber} in '{filePath}'.", ex);
        }
    }

    private static void AppendDetectedGaps(
        List<ScannedTrade> trades,
        List<TradeGap> detectedGaps,
        List<TradeGapAffectedRange> affectedRanges)
    {
        trades.Sort(static (left, right) =>
        {
            var result = left.NumericTradeId.CompareTo(right.NumericTradeId);
            if (result != 0)
            {
                return result;
            }

            result = left.Timestamp.CompareTo(right.Timestamp);
            if (result != 0)
            {
                return result;
            }

            result = string.Compare(left.RelativeFilePath, right.RelativeFilePath, StringComparison.Ordinal);
            if (result != 0)
            {
                return result;
            }

            return left.LineNumber.CompareTo(right.LineNumber);
        });

        ScannedTrade? previousTrade = null;
        foreach (var trade in trades)
        {
            if (previousTrade is null)
            {
                previousTrade = trade;
                continue;
            }

            if (trade.NumericTradeId == previousTrade.Value.NumericTradeId)
            {
                continue;
            }

            if (trade.NumericTradeId > previousTrade.Value.NumericTradeId + 1)
            {
                var fromExclusive = new TradeWatermark(previousTrade.Value.TradeId, previousTrade.Value.Timestamp);
                var toInclusive = new TradeWatermark(trade.TradeId, trade.Timestamp);
                var missingTradeIds = new MissingTradeIdRange(previousTrade.Value.NumericTradeId + 1, trade.NumericTradeId - 1);

                var gap = TradeGap
                    .CreateOpen(Guid.NewGuid(), trade.Exchange, trade.Symbol, fromExclusive, trade.Timestamp)
                    .ToBounded(toInclusive, missingTradeIds);

                detectedGaps.Add(gap);
                affectedRanges.Add(
                    new TradeGapAffectedRange(
                        fromExclusive,
                        toInclusive,
                        new TradeGapBoundaryLocation(previousTrade.Value.RelativeFilePath, previousTrade.Value.LineNumber),
                        new TradeGapBoundaryLocation(trade.RelativeFilePath, trade.LineNumber)));
            }

            previousTrade = trade;
        }
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
            foreach (var candidateFile in candidateFiles.OrderBy(file => file.Path, StringComparer.Ordinal))
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
            foreach (var fullPath in Directory.EnumerateFiles(rootDirectory, "*.jsonl", SearchOption.AllDirectories).OrderBy(path => path, StringComparer.Ordinal))
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

    /// <summary>
    /// Represents one local JSONL file selected for scanning.
    /// </summary>
    private sealed record ScannedFile(string FullPath, string RelativePath, DateOnly? TradingDay);
}
