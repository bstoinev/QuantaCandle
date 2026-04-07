using System.Globalization;
using System.Text.Json;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra;

/// <summary>
/// Provides JSONL helpers for trade persistence files.
/// </summary>
public static class TradeJsonlFile
{
    /// <summary>
    /// Builds the full JSONL payload for the supplied trades.
    /// </summary>
    public static string BuildPayload(IReadOnlyList<TradeInfo> trades)
    {
        var lines = new string[trades.Count];

        for (var i = 0; i < trades.Count; i++)
        {
            var trade = trades[i];

            var record = new
            {
                exchange = trade.Key.Exchange.ToString(),
                instrument = trade.Key.Symbol.ToString(),
                tradeId = trade.Key.TradeId,
                timestamp = trade.Timestamp,
                price = trade.Price,
                quantity = trade.Quantity,
            };

            lines[i] = JsonSerializer.Serialize(record);
        }

        var result = string.Join(Environment.NewLine, lines) + Environment.NewLine;
        return result;
    }

    /// <summary>
    /// Loads every trade record from the specified local JSONL file.
    /// </summary>
    public static async Task<IReadOnlyList<TradeInfo>> ReadTradesAsync(string path, CancellationToken cancellationToken)
    {
        var result = new List<TradeInfo>();

        if (!File.Exists(path))
        {
            return result;
        }

        string[] lines = await File.ReadAllLinesAsync(path, cancellationToken).ConfigureAwait(false);
        foreach (var line in lines)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            using var document = JsonDocument.Parse(line);
            var root = document.RootElement;

            var exchange = new ExchangeId(root.GetProperty("exchange").GetString() ?? string.Empty);
            var instrument = Instrument.Parse(root.GetProperty("instrument").GetString() ?? string.Empty);
            var tradeId = root.GetProperty("tradeId").GetString() ?? string.Empty;
            var timestamp = root.GetProperty("timestamp").GetDateTimeOffset();
            var price = root.GetProperty("price").GetDecimal();
            var quantity = root.GetProperty("quantity").GetDecimal();

            var key = new TradeKey(exchange, instrument, tradeId);
            result.Add(new TradeInfo(key, timestamp, price, quantity));
        }

        return result;
    }

    /// <summary>
    /// Finds the most recent resume watermark available from the instrument local files.
    /// </summary>
    public static async Task<ResumeBoundary?> TryReadLatestResumeBoundaryAsync(
        string localRootDirectory,
        Instrument instrument,
        DateTimeOffset utcNow,
        CancellationToken cancellationToken)
    {
        ResumeBoundary? result = null;
        var instrumentDirectory = Path.Combine(localRootDirectory, instrument.ToString());

        if (Directory.Exists(instrumentDirectory))
        {
            var candidateFiles = Directory
                .EnumerateFiles(instrumentDirectory, "*.jsonl", SearchOption.TopDirectoryOnly)
                .ToList();

            foreach (var candidateFile in candidateFiles)
            {
                var origin = GetResumeBoundaryOrigin(candidateFile);
                if (origin is null)
                {
                    continue;
                }

                var resumeBoundary = await TryReadLatestResumeBoundaryFromFileAsync(candidateFile, origin, cancellationToken).ConfigureAwait(false);
                if (resumeBoundary is not null
                    && (result is null || resumeBoundary.Value.TimestampUtc >= result.Value.TimestampUtc))
                {
                    result = resumeBoundary;
                }
            }
        }

        if (result is null)
        {
            var utcDayStart = new DateTimeOffset(utcNow.UtcDateTime.Date, TimeSpan.Zero);
            result = new ResumeBoundary(utcDayStart, DateOnly.FromDateTime(utcDayStart.UtcDateTime), "CurrentDayUtcStartFallback");
        }

        return result;
    }

    /// <summary>
    /// Writes the full payload to the specified JSONL file path.
    /// </summary>
    public static async Task WriteFullPayloadAsync(string path, string payload, CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await File.WriteAllTextAsync(path, payload, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Appends the supplied payload to the specified JSONL file path.
    /// </summary>
    public static async Task AppendPayloadAsync(string path, string payload, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(payload))
        {
            return;
        }

        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await File.AppendAllTextAsync(path, payload, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Rewrites the specified JSONL file with the supplied payload, or deletes it when the payload is empty.
    /// </summary>
    public static async Task RewritePayloadAsync(string path, string payload, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(payload))
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }

            return;
        }

        await WriteFullPayloadAsync(path, payload, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<ResumeBoundary?> TryReadLatestResumeBoundaryFromFileAsync(string path, string origin, CancellationToken cancellationToken)
    {
        ResumeBoundary? result = null;
        string[] lines = await File.ReadAllLinesAsync(path, cancellationToken).ConfigureAwait(false);

        foreach (var line in lines)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            using var document = JsonDocument.Parse(line);
            var root = document.RootElement;
            var timestamp = root.GetProperty("timestamp").GetDateTimeOffset();
            var utcTimestamp = timestamp.ToUniversalTime();
            var utcDate = DateOnly.FromDateTime(utcTimestamp.UtcDateTime);

            if (result is null || utcTimestamp >= result.Value.TimestampUtc)
            {
                result = new ResumeBoundary(utcTimestamp, utcDate, origin);
            }
        }

        return result;
    }

    private static string? GetResumeBoundaryOrigin(string path)
    {
        string? result = null;
        var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(path);

        if (string.Equals(fileNameWithoutExtension, "qc-scratch", StringComparison.OrdinalIgnoreCase))
        {
            result = "LatestScratchFile";
        }
        else if (TryParseUtcDate(fileNameWithoutExtension) is not null)
        {
            result = "LatestLocalDailyFile";
        }

        return result;
    }

    private static DateOnly? TryParseUtcDate(string? fileNameWithoutExtension)
    {
        DateOnly? result = null;

        if (!string.IsNullOrWhiteSpace(fileNameWithoutExtension)
            && DateOnly.TryParseExact(
                fileNameWithoutExtension,
                "yyyy-MM-dd",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var utcDate))
        {
            result = utcDate;
        }

        return result;
    }
}
