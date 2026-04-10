using System.Globalization;
using System.Text.Json;

namespace QuantaCandle.CLI;

/// <summary>
/// Generates candles directly from the in-place trade file layout under one work directory.
/// </summary>
public sealed class TradeToCandleGenerator
{
    /// <summary>
    /// Generates candles for the requested exchange, instrument, and optional date scope.
    /// </summary>
    public static async Task<CliResult> Run(CliOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);

        var exchange = NormalizeSource(options.Exchange);
        var instrument = NormalizeInstrument(options.Instrument);
        var timeframe = NormalizeTimeframe(options.Timeframe);
        var format = NormalizeFormat(options.Format);
        var workDirectory = Path.GetFullPath(options.WorkDirectory);
        var tradeRootDirectory = GetTradeRootDirectory(workDirectory);
        var outputRootDirectory = GetOutputRootDirectory(workDirectory, exchange, timeframe);
        var inputFiles = ResolveInputFiles(tradeRootDirectory, exchange, instrument, options.Dates);

        if (PathsEqual(tradeRootDirectory, outputRootDirectory))
        {
            throw new ArgumentException("Input and output directories must be different.");
        }

        var trades = await LoadTradesAsync(inputFiles, exchange, instrument, cancellationToken).ConfigureAwait(false);
        trades.Sort(TradeRowComparer.Instance);

        var inputTradeCount = trades.Count;
        var uniqueTrades = DeduplicateTrades(trades, out var duplicatesDropped);

        if (uniqueTrades.Count == 0)
        {
            var emptyResult = new CliResult(
                InputTradeCount: inputTradeCount,
                UniqueTradeCount: 0,
                DuplicatesDropped: duplicatesDropped,
                CandleCount: 0,
                OutputFileCount: 0);
            return emptyResult;
        }

        var candlesByPath = BuildCandlesByOutputPath(uniqueTrades, exchange, timeframe, format, outputRootDirectory);
        var outputPaths = candlesByPath.Keys.OrderBy(static path => path, StringComparer.Ordinal).ToArray();
        var candleCount = 0;

        foreach (var outputPath in outputPaths)
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

        var result = new CliResult(
            InputTradeCount: inputTradeCount,
            UniqueTradeCount: uniqueTrades.Count,
            DuplicatesDropped: duplicatesDropped,
            CandleCount: candleCount,
            OutputFileCount: outputPaths.Length);
        return result;
    }

    private static string[] BuildJsonlLines(IReadOnlyList<CandleRow> candles)
    {
        var result = new string[candles.Count];

        for (var i = 0; i < candles.Count; i++)
        {
            var candle = candles[i];
            result[i] = JsonSerializer.Serialize(new
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

        return result;
    }

    private static string[] BuildCsvLines(IReadOnlyList<CandleRow> candles)
    {
        var result = new string[candles.Count + 1];
        result[0] = "OpenTimeUtc,Instrument,Open,High,Low,Close,Volume,TradeCount";

        for (var i = 0; i < candles.Count; i++)
        {
            var candle = candles[i];
            var openTimeUtc = candle.OpenTime.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);
            var open = candle.Open?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
            var high = candle.High?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
            var low = candle.Low?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
            var close = candle.Close?.ToString(CultureInfo.InvariantCulture) ?? string.Empty;
            var volume = candle.Volume.ToString(CultureInfo.InvariantCulture);
            var tradeCount = candle.TradeCount.ToString(CultureInfo.InvariantCulture);

            result[i + 1] = $"{openTimeUtc},{candle.Instrument},{open},{high},{low},{close},{volume},{tradeCount}";
        }

        return result;
    }

    private static List<string> ResolveInputFiles(string tradeRootDirectory, string exchange, string instrument, IReadOnlyList<DateOnly> dates)
    {
        var result = new List<string>();
        var instrumentDirectory = Path.Combine(tradeRootDirectory, exchange, instrument);

        if (!Directory.Exists(instrumentDirectory))
        {
            return result;
        }

        if (dates.Count > 0)
        {
            foreach (var date in dates.OrderBy(static value => value))
            {
                var path = Path.Combine(instrumentDirectory, date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + ".jsonl");
                if (File.Exists(path))
                {
                    result.Add(path);
                }
            }
        }
        else
        {
            result.AddRange(Directory.EnumerateFiles(instrumentDirectory, "*.jsonl", SearchOption.TopDirectoryOnly).OrderBy(static path => path, StringComparer.Ordinal));
        }

        return result;
    }

    private static bool PathsEqual(string left, string right)
    {
        var normalizedLeft = Path.GetFullPath(left).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var normalizedRight = Path.GetFullPath(right).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var result = string.Equals(normalizedLeft, normalizedRight, StringComparison.OrdinalIgnoreCase);
        return result;
    }

    private static string GetOutputRootDirectory(string workDirectory, string exchange, string timeframe)
    {
        var result = Path.GetFullPath(Path.Combine(workDirectory, "candles-out", exchange, timeframe));
        return result;
    }

    private static string GetTradeRootDirectory(string workDirectory)
    {
        var result = Path.GetFullPath(Path.Combine(workDirectory, "trades-out"));
        return result;
    }

    private static string NormalizeInstrument(string instrument)
    {
        if (string.IsNullOrWhiteSpace(instrument))
        {
            throw new ArgumentException("Instrument must be provided.", nameof(instrument));
        }

        var result = instrument.Trim().ToUpperInvariant();
        return result;
    }

    private static string NormalizeSource(string source)
    {
        if (string.IsNullOrWhiteSpace(source))
        {
            throw new ArgumentException("Source must be provided.", nameof(source));
        }

        var result = source.Trim().ToLowerInvariant();
        if (result.Contains(Path.DirectorySeparatorChar, StringComparison.Ordinal)
            || result.Contains(Path.AltDirectorySeparatorChar, StringComparison.Ordinal))
        {
            throw new ArgumentException("Source must be a single directory name.", nameof(source));
        }

        return result;
    }

    private static string NormalizeTimeframe(string timeframe)
    {
        if (string.IsNullOrWhiteSpace(timeframe))
        {
            throw new ArgumentException("Timeframe must be provided.", nameof(timeframe));
        }

        var result = timeframe.Trim().ToLowerInvariant();
        if (!result.Equals("1m", StringComparison.Ordinal))
        {
            throw new NotSupportedException("Only timeframe '1m' is currently supported.");
        }

        return result;
    }

    private static string NormalizeFormat(string format)
    {
        if (string.IsNullOrWhiteSpace(format))
        {
            return "csv";
        }

        var result = format.Trim().ToLowerInvariant();
        if (result.Equals("csv", StringComparison.Ordinal) || result.Equals("jsonl", StringComparison.Ordinal))
        {
            return result;
        }

        throw new NotSupportedException("Only output formats 'csv' and 'jsonl' are currently supported.");
    }

    private static async Task<List<TradeRow>> LoadTradesAsync(
        IReadOnlyList<string> files,
        string exchange,
        string instrument,
        CancellationToken cancellationToken)
    {
        var result = new List<TradeRow>();

        foreach (var file in files)
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
                if (!row.Exchange.Equals(exchange, StringComparison.OrdinalIgnoreCase)
                    || !row.Instrument.Equals(instrument, StringComparison.Ordinal))
                {
                    continue;
                }

                result.Add(row);
            }
        }

        return result;
    }

    private static TradeRow ParseTradeRow(string line, string file, int lineNumber)
    {
        try
        {
            using var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;

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

            var result = new TradeRow(exchange.Trim().ToLowerInvariant(), instrument.Trim().ToUpperInvariant(), tradeId.Trim(), timestamp, price, quantity);
            return result;
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

        foreach (var trade in sortedTrades)
        {
            var identity = new TradeIdentity(trade.Exchange, trade.Instrument, trade.TradeId);
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
        string exchange,
        string timeframe,
        string format,
        string outputRootDirectory)
    {
        var result = new Dictionary<string, List<CandleRow>>(StringComparer.Ordinal);
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

            AppendInstrumentCandles(uniqueTrades, start, index, exchange, timeframe, instrument, extension, outputRootDirectory, result);
        }

        return result;
    }

    private static void AppendInstrumentCandles(
        IReadOnlyList<TradeRow> uniqueTrades,
        int startInclusive,
        int endExclusive,
        string exchange,
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

            var candle = BuildCandleForBucket(uniqueTrades, bucketStart, tradeIndex, exchange, timeframe, instrument, currentBucket);
            var day = currentBucket.UtcDateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            var outputPath = Path.Combine(outputRootDirectory, instrument, $"{day}{extension}");

            if (!candlesByPath.TryGetValue(outputPath, out var list))
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
        string exchange,
        string timeframe,
        string instrument,
        DateTimeOffset openTime)
    {
        CandleRow result;

        if (startInclusive == endExclusive)
        {
            result = new CandleRow(
                exchange,
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
        else
        {
            var open = uniqueTrades[startInclusive].Price;
            var close = uniqueTrades[endExclusive - 1].Price;
            var high = open;
            var low = open;
            var volume = 0m;

            for (var i = startInclusive; i < endExclusive; i++)
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

            result = new CandleRow(
                exchange,
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

        return result;
    }

    private static DateTimeOffset FloorToMinute(DateTimeOffset timestamp)
    {
        var utc = timestamp.UtcDateTime;
        var floored = new DateTime(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, 0, DateTimeKind.Utc);
        var result = new DateTimeOffset(floored);
        return result;
    }

    /// <summary>
    /// Identifies one trade row uniquely for deduplication.
    /// </summary>
    private readonly record struct TradeIdentity(string Exchange, string Instrument, string TradeId);

    /// <summary>
    /// Represents one parsed trade row used for candle generation.
    /// </summary>
    private readonly record struct TradeRow(
        string Exchange,
        string Instrument,
        string TradeId,
        DateTimeOffset TimestampUtc,
        decimal Price,
        decimal Quantity);

    /// <summary>
    /// Represents one generated candle row.
    /// </summary>
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

    /// <summary>
    /// Orders trades deterministically before deduplication and candle bucketing.
    /// </summary>
    private sealed class TradeRowComparer : IComparer<TradeRow>
    {
        public static TradeRowComparer Instance { get; } = new();

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

            result = left.Quantity.CompareTo(right.Quantity);
            return result;
        }
    }
}
