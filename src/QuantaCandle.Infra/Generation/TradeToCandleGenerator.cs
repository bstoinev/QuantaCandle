using System.Globalization;
using System.Text.Json;

namespace QuantaCandle.Infra.Generation;

public sealed class TradeToCandleGenerator
{
    public async Task<CandleGenerationResult> GenerateAsync(TradeToCandleGeneratorOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);

        var source = NormalizeSource(options.Source);
        var timeframe = NormalizeTimeframe(options.Timeframe);
        var format = NormalizeFormat(options.Format);
        var inputDirectory = Path.GetFullPath(options.InputDirectory);
        var outputRootDirectory = GetOutputRootDirectory(options.OutputDirectory, source, timeframe);

        if (PathsEqual(inputDirectory, outputRootDirectory))
        {
            throw new ArgumentException("Input and output directories must be different.");
        }

        var trades = await LoadTradesAsync(inputDirectory, source, cancellationToken).ConfigureAwait(false);
        trades.Sort(TradeRowComparer.Instance);

        var inputTradeCount = trades.Count;
        var uniqueTrades = DeduplicateTrades(trades, out int duplicatesDropped);

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

        var candlesByPath = BuildCandlesByOutputPath(uniqueTrades, source, timeframe, format, outputRootDirectory);
        var outputPaths = candlesByPath.Keys.OrderBy(path => path, StringComparer.Ordinal).ToArray();

        var candleCount = 0;
        foreach (string outputPath in outputPaths)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var directory = Path.GetDirectoryName(outputPath);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var candles = candlesByPath[outputPath];
            candleCount += candles.Count;

            var lines = format.Equals("csv", StringComparison.Ordinal)
                ? BuildCsvLines(candles)
                : BuildJsonlLines(candles);

            var payload = string.Join(Environment.NewLine, lines) + Environment.NewLine;
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
            var candle = candles[i];
            var openTimeUtc = candle.OpenTime.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);
            var open = candle.Open?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
            var high = candle.High?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
            var low = candle.Low?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
            var close = candle.Close?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
            var volume = candle.Volume.ToString(CultureInfo.InvariantCulture);
            var tradeCount = candle.TradeCount.ToString(CultureInfo.InvariantCulture);

            lines[i + 1] = $"{openTimeUtc},{candle.Instrument},{open},{high},{low},{close},{volume},{tradeCount}";
        }

        return lines;
    }

    private static bool PathsEqual(string left, string right)
    {
        var normalizedLeft = Path.GetFullPath(left).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var normalizedRight = Path.GetFullPath(right).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
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

            var lineNumber = 0;
            await foreach (var line in File.ReadLinesAsync(file, cancellationToken).ConfigureAwait(false))
            {
                lineNumber++;

                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                var row = ParseTradeRow(line, file, lineNumber);
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

            var exchange = root.GetProperty("exchange").GetString() ?? string.Empty;
            var instrument = root.GetProperty("instrument").GetString() ?? string.Empty;
            var tradeId = root.GetProperty("tradeId").GetString() ?? string.Empty;
            var timestamp = root.GetProperty("timestamp").GetDateTimeOffset().ToUniversalTime();
            var price = root.GetProperty("price").GetDecimal();
            var quantity = root.GetProperty("quantity").GetDecimal();

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
        var seen = new HashSet<TradeIdentity>();
        var uniqueTrades = new List<TradeRow>(sortedTrades.Count);
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
        var candlesByPath = new Dictionary<string, List<CandleRow>>(StringComparer.Ordinal);
        var extension = format.Equals("csv", StringComparison.Ordinal) ? ".csv" : ".jsonl";

        var index = 0;
        while (index < uniqueTrades.Count)
        {
            var instrument = uniqueTrades[index].Instrument;
            var start = index;
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
        var firstBucket = FloorToMinute(uniqueTrades[startInclusive].TimestampUtc);
        var lastBucket = FloorToMinute(uniqueTrades[endExclusive - 1].TimestampUtc);

        var tradeIndex = startInclusive;
        var currentBucket = firstBucket;
        while (currentBucket <= lastBucket)
        {
            var bucketStart = tradeIndex;
            while (tradeIndex < endExclusive && FloorToMinute(uniqueTrades[tradeIndex].TimestampUtc) == currentBucket)
            {
                tradeIndex++;
            }

            var candle = BuildCandleForBucket(uniqueTrades, bucketStart, tradeIndex, source, timeframe, instrument, currentBucket);

            var day = currentBucket.UtcDateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            var outputPath = Path.Combine(outputRootDirectory, instrument, $"{day}{extension}");
            if (!candlesByPath.TryGetValue(outputPath, out List<CandleRow>? list))
            {
                list = [];
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

        var open = uniqueTrades[startInclusive].Price;
        var close = uniqueTrades[endExclusive - 1].Price;
        var high = open;
        var low = open;
        var volume = 0m;

        for (int i = startInclusive; i < endExclusive; i++)
        {
            var trade = uniqueTrades[i];
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
        var utc = timestamp.UtcDateTime;
        var floored = new DateTime(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, 0, DateTimeKind.Utc);
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
            var result = string.Compare(left.Instrument, right.Instrument, StringComparison.Ordinal);
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
