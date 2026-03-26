using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace QuantaCandle.Infra;

public sealed class TradeToCandleGenerator
{
    public async Task<CandleGenerationResult> GenerateAsync(TradeToCandleGeneratorOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);

        string source = NormalizeSource(options.Source);
        string timeframe = NormalizeTimeframe(options.Timeframe);
        string format = NormalizeFormat(options.Format);
        string inputDirectory = Path.GetFullPath(options.InputDirectory);
        string outputRootDirectory = GetOutputRootDirectory(options.OutputDirectory, source, timeframe);

        if (PathsEqual(inputDirectory, outputRootDirectory))
        {
            throw new ArgumentException("Input and output directories must be different.");
        }

        List<TradeRow> trades = await LoadTradesAsync(inputDirectory, source, cancellationToken).ConfigureAwait(false);
        trades.Sort(TradeRowComparer.Instance);

        int inputTradeCount = trades.Count;
        List<TradeRow> uniqueTrades = DeduplicateTrades(trades, out int duplicatesDropped);

        ResetOutputDirectory(outputRootDirectory);

        if (uniqueTrades.Count == 0)
        {
            return new CandleGenerationResult(
                InputTradeCount: inputTradeCount,
                UniqueTradeCount: 0,
                DuplicatesDropped: duplicatesDropped,
                CandleCount: 0,
                OutputFileCount: 0);
        }

        Dictionary<string, List<CandleRow>> candlesByPath = BuildCandlesByOutputPath(uniqueTrades, source, timeframe, format, outputRootDirectory);
        string[] outputPaths = candlesByPath.Keys.OrderBy(path => path, StringComparer.Ordinal).ToArray();

        int candleCount = 0;
        foreach (string outputPath in outputPaths)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string? directory = Path.GetDirectoryName(outputPath);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            List<CandleRow> candles = candlesByPath[outputPath];
            candleCount += candles.Count;

            string[] lines = format.Equals("csv", StringComparison.Ordinal)
                ? BuildCsvLines(candles)
                : BuildJsonlLines(candles);

            string payload = string.Join(Environment.NewLine, lines) + Environment.NewLine;
            await File.WriteAllTextAsync(outputPath, payload, cancellationToken).ConfigureAwait(false);
        }

        return new CandleGenerationResult(
            InputTradeCount: inputTradeCount,
            UniqueTradeCount: uniqueTrades.Count,
            DuplicatesDropped: duplicatesDropped,
            CandleCount: candleCount,
            OutputFileCount: outputPaths.Length);
    }

    private static string[] BuildJsonlLines(IReadOnlyList<CandleRow> candles)
    {
        string[] lines = new string[candles.Count];
        for (int i = 0; i < candles.Count; i++)
        {
            CandleRow candle = candles[i];
            lines[i] = JsonSerializer.Serialize(new
            {
                source = candle.Source,
                instrument = candle.Instrument,
                timeframe = candle.Timeframe,
                openTime = candle.OpenTime,
                open = candle.Open,
                high = candle.High,
                low = candle.Low,
                close = candle.Close,
                volume = candle.Volume,
                tradeCount = candle.TradeCount,
            });
        }

        return lines;
    }

    private static string[] BuildCsvLines(IReadOnlyList<CandleRow> candles)
    {
        string[] lines = new string[candles.Count + 1];
        lines[0] = "OpenTimeUtc,Instrument,Open,High,Low,Close,Volume,TradeCount";

        for (int i = 0; i < candles.Count; i++)
        {
            CandleRow candle = candles[i];
            string openTimeUtc = candle.OpenTime.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);
            string open = candle.Open?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
            string high = candle.High?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
            string low = candle.Low?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
            string close = candle.Close?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
            string volume = candle.Volume.ToString(CultureInfo.InvariantCulture);
            string tradeCount = candle.TradeCount.ToString(CultureInfo.InvariantCulture);

            lines[i + 1] = $"{openTimeUtc},{candle.Instrument},{open},{high},{low},{close},{volume},{tradeCount}";
        }

        return lines;
    }

    private static bool PathsEqual(string left, string right)
    {
        string normalizedLeft = Path.GetFullPath(left).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        string normalizedRight = Path.GetFullPath(right).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        return string.Equals(normalizedLeft, normalizedRight, StringComparison.OrdinalIgnoreCase);
    }

    private static string GetOutputRootDirectory(string outputDirectory, string source, string timeframe)
    {
        if (string.IsNullOrWhiteSpace(outputDirectory))
        {
            throw new ArgumentException("Output directory must be provided.", nameof(outputDirectory));
        }

        return Path.GetFullPath(Path.Combine(outputDirectory.Trim(), source, timeframe));
    }

    private static void ResetOutputDirectory(string outputRootDirectory)
    {
        if (Directory.Exists(outputRootDirectory))
        {
            Directory.Delete(outputRootDirectory, recursive: true);
        }

        Directory.CreateDirectory(outputRootDirectory);
    }

    private static string NormalizeSource(string source)
    {
        if (string.IsNullOrWhiteSpace(source))
        {
            throw new ArgumentException("Source must be provided.", nameof(source));
        }

        string normalized = source.Trim().ToLowerInvariant();
        if (!normalized.Equals("binance", StringComparison.Ordinal))
        {
            throw new NotSupportedException("Only source 'binance' is currently supported.");
        }

        return normalized;
    }

    private static string NormalizeTimeframe(string timeframe)
    {
        if (string.IsNullOrWhiteSpace(timeframe))
        {
            throw new ArgumentException("Timeframe must be provided.", nameof(timeframe));
        }

        string normalized = timeframe.Trim().ToLowerInvariant();
        if (!normalized.Equals("1m", StringComparison.Ordinal))
        {
            throw new NotSupportedException("Only timeframe '1m' is currently supported.");
        }

        return normalized;
    }

    private static string NormalizeFormat(string format)
    {
        if (string.IsNullOrWhiteSpace(format))
        {
            return "csv";
        }

        string normalized = format.Trim().ToLowerInvariant();
        if (normalized.Equals("csv", StringComparison.Ordinal) || normalized.Equals("jsonl", StringComparison.Ordinal))
        {
            return normalized;
        }

        throw new NotSupportedException("Only output formats 'csv' and 'jsonl' are currently supported.");
    }

    private static async Task<List<TradeRow>> LoadTradesAsync(string inputDirectory, string source, CancellationToken cancellationToken)
    {
        List<TradeRow> trades = new List<TradeRow>();
        if (!Directory.Exists(inputDirectory))
        {
            return trades;
        }

        string[] files = Directory.GetFiles(inputDirectory, "*.jsonl", SearchOption.AllDirectories);
        Array.Sort(files, StringComparer.Ordinal);

        foreach (string file in files)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int lineNumber = 0;
            await foreach (string line in File.ReadLinesAsync(file, cancellationToken).ConfigureAwait(false))
            {
                lineNumber++;

                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                TradeRow row = ParseTradeRow(line, file, lineNumber);
                if (!row.Exchange.Equals(source, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                trades.Add(row);
            }
        }

        return trades;
    }

    private static TradeRow ParseTradeRow(string line, string file, int lineNumber)
    {
        try
        {
            using JsonDocument doc = JsonDocument.Parse(line);
            JsonElement root = doc.RootElement;

            string exchange = root.GetProperty("exchange").GetString() ?? string.Empty;
            string instrument = root.GetProperty("instrument").GetString() ?? string.Empty;
            string tradeId = root.GetProperty("tradeId").GetString() ?? string.Empty;
            DateTimeOffset timestamp = root.GetProperty("timestamp").GetDateTimeOffset().ToUniversalTime();
            decimal price = root.GetProperty("price").GetDecimal();
            decimal quantity = root.GetProperty("quantity").GetDecimal();

            if (string.IsNullOrWhiteSpace(exchange))
            {
                throw new InvalidOperationException("Exchange is missing.");
            }

            if (string.IsNullOrWhiteSpace(instrument))
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

            return new TradeRow(exchange.Trim().ToLowerInvariant(), instrument.Trim().ToUpperInvariant(), tradeId.Trim(), timestamp, price, quantity);
        }
        catch (Exception ex) when (ex is KeyNotFoundException or InvalidOperationException or JsonException or FormatException)
        {
            throw new InvalidOperationException($"Failed to parse trade at '{file}' line {lineNumber}.", ex);
        }
    }

    private static List<TradeRow> DeduplicateTrades(List<TradeRow> sortedTrades, out int duplicatesDropped)
    {
        HashSet<TradeIdentity> seen = new HashSet<TradeIdentity>();
        List<TradeRow> uniqueTrades = new List<TradeRow>(sortedTrades.Count);
        duplicatesDropped = 0;

        foreach (TradeRow trade in sortedTrades)
        {
            TradeIdentity identity = new TradeIdentity(trade.Exchange, trade.Instrument, trade.TradeId);
            if (seen.Add(identity))
            {
                uniqueTrades.Add(trade);
            }
            else
            {
                duplicatesDropped++;
            }
        }

        return uniqueTrades;
    }

    private static Dictionary<string, List<CandleRow>> BuildCandlesByOutputPath(
        IReadOnlyList<TradeRow> uniqueTrades,
        string source,
        string timeframe,
        string format,
        string outputRootDirectory)
    {
        Dictionary<string, List<CandleRow>> candlesByPath = new Dictionary<string, List<CandleRow>>(StringComparer.Ordinal);
        string extension = format.Equals("csv", StringComparison.Ordinal) ? ".csv" : ".jsonl";

        int index = 0;
        while (index < uniqueTrades.Count)
        {
            string instrument = uniqueTrades[index].Instrument;
            int start = index;
            while (index < uniqueTrades.Count && uniqueTrades[index].Instrument.Equals(instrument, StringComparison.Ordinal))
            {
                index++;
            }

            AppendInstrumentCandles(uniqueTrades, start, index, source, timeframe, instrument, extension, outputRootDirectory, candlesByPath);
        }

        return candlesByPath;
    }

    private static void AppendInstrumentCandles(
        IReadOnlyList<TradeRow> uniqueTrades,
        int startInclusive,
        int endExclusive,
        string source,
        string timeframe,
        string instrument,
        string extension,
        string outputRootDirectory,
        Dictionary<string, List<CandleRow>> candlesByPath)
    {
        DateTimeOffset firstBucket = FloorToMinute(uniqueTrades[startInclusive].TimestampUtc);
        DateTimeOffset lastBucket = FloorToMinute(uniqueTrades[endExclusive - 1].TimestampUtc);

        int tradeIndex = startInclusive;
        DateTimeOffset currentBucket = firstBucket;
        while (currentBucket <= lastBucket)
        {
            int bucketStart = tradeIndex;
            while (tradeIndex < endExclusive && FloorToMinute(uniqueTrades[tradeIndex].TimestampUtc) == currentBucket)
            {
                tradeIndex++;
            }

            CandleRow candle = BuildCandleForBucket(uniqueTrades, bucketStart, tradeIndex, source, timeframe, instrument, currentBucket);

            string day = currentBucket.UtcDateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            string outputPath = Path.Combine(outputRootDirectory, instrument, $"{day}{extension}");
            if (!candlesByPath.TryGetValue(outputPath, out List<CandleRow>? list))
            {
                list = new List<CandleRow>();
                candlesByPath[outputPath] = list;
            }

            list.Add(candle);
            currentBucket = currentBucket.AddMinutes(1);
        }
    }

    private static CandleRow BuildCandleForBucket(
        IReadOnlyList<TradeRow> uniqueTrades,
        int startInclusive,
        int endExclusive,
        string source,
        string timeframe,
        string instrument,
        DateTimeOffset openTime)
    {
        if (startInclusive == endExclusive)
        {
            return new CandleRow(
                source,
                instrument,
                timeframe,
                openTime,
                Open: null,
                High: null,
                Low: null,
                Close: null,
                Volume: 0m,
                TradeCount: 0);
        }

        decimal open = uniqueTrades[startInclusive].Price;
        decimal close = uniqueTrades[endExclusive - 1].Price;
        decimal high = open;
        decimal low = open;
        decimal volume = 0m;

        for (int i = startInclusive; i < endExclusive; i++)
        {
            TradeRow trade = uniqueTrades[i];
            if (trade.Price > high)
            {
                high = trade.Price;
            }

            if (trade.Price < low)
            {
                low = trade.Price;
            }

            volume += trade.Quantity;
        }

        return new CandleRow(
            source,
            instrument,
            timeframe,
            openTime,
            open,
            high,
            low,
            close,
            volume,
            endExclusive - startInclusive);
    }

    private static DateTimeOffset FloorToMinute(DateTimeOffset timestamp)
    {
        DateTime utc = timestamp.UtcDateTime;
        DateTime floored = new DateTime(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, 0, DateTimeKind.Utc);
        return new DateTimeOffset(floored);
    }

    private readonly record struct TradeIdentity(string Exchange, string Instrument, string TradeId);

    private readonly record struct TradeRow(
        string Exchange,
        string Instrument,
        string TradeId,
        DateTimeOffset TimestampUtc,
        decimal Price,
        decimal Quantity);

    private readonly record struct CandleRow(
        string Source,
        string Instrument,
        string Timeframe,
        DateTimeOffset OpenTime,
        decimal? Open,
        decimal? High,
        decimal? Low,
        decimal? Close,
        decimal Volume,
        int TradeCount);

    private sealed class TradeRowComparer : IComparer<TradeRow>
    {
        public static TradeRowComparer Instance { get; } = new TradeRowComparer();

        public int Compare(TradeRow left, TradeRow right)
        {
            int result = string.Compare(left.Instrument, right.Instrument, StringComparison.Ordinal);
            if (result != 0)
            {
                return result;
            }

            result = left.TimestampUtc.CompareTo(right.TimestampUtc);
            if (result != 0)
            {
                return result;
            }

            result = string.Compare(left.TradeId, right.TradeId, StringComparison.Ordinal);
            if (result != 0)
            {
                return result;
            }

            result = left.Price.CompareTo(right.Price);
            if (result != 0)
            {
                return result;
            }

            return left.Quantity.CompareTo(right.Quantity);
        }
    }
}
