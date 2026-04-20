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
                isBuyerMaker = trade.BuyerIsMaker,
            };

            lines[i] = JsonSerializer.Serialize(record);
        }

        var result = string.Join(Environment.NewLine, lines) + Environment.NewLine;
        return result;
    }

    /// <summary>
    /// Loads every trade record from the specified local JSONL file.
    /// </summary>
    public static async Task<IReadOnlyList<TradeInfo>> ReadTrades(string path, CancellationToken cancellationToken)
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
            var buyerIsMaker = root.GetProperty("isBuyerMaker").GetBoolean();

            var key = new TradeKey(exchange, instrument, tradeId);
            result.Add(new TradeInfo(key, timestamp, price, quantity, buyerIsMaker));
        }

        return result;
    }

    /// <summary>
    /// Finds the most recent resume watermark available from the instrument local files.
    /// </summary>
    public static async Task<ResumeBoundary?> TryReadLatestResumeBoundary(
        string localRootDirectory,
        Instrument instrument,
        DateTimeOffset utcNow,
        CancellationToken cancellationToken)
    {
        ResumeBoundary? result = null;
        var instrumentDirectory = Path.Combine(localRootDirectory, instrument.ToString());

        if (Directory.Exists(instrumentDirectory))
        {
            foreach (var candidateFile in Directory.EnumerateFiles(instrumentDirectory, "*.jsonl", SearchOption.TopDirectoryOnly))
            {
                var origin = GetResumeBoundaryOrigin(candidateFile);
                if (origin is null)
                {
                    continue;
                }

                var resumeBoundary = await TryReadLatestResumeBoundaryFromFile(candidateFile, origin, cancellationToken).ConfigureAwait(false);
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
    public static async Task WriteFullPayload(string path, string payload, CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await File.WriteAllTextAsync(path, payload, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Appends the supplied trades to the specified JSONL file path without rebuilding the whole file payload.
    /// </summary>
    public static async Task AppendTrades(string path, IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (trades.Count == 0)
        {
            return;
        }

        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await using var stream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.None);
        await using var writer = new StreamWriter(stream);

        foreach (var trade in trades)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await writer.WriteLineAsync(SerializeTrade(trade)).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Appends the supplied payload to the specified JSONL file path.
    /// </summary>
    public static async Task AppendPayload(string path, string payload, CancellationToken cancellationToken)
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
    public static async Task RewritePayload(string path, string payload, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(payload))
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }

            return;
        }

        await WriteFullPayload(path, payload, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Streams JSONL records and keeps only the latest resume boundary candidate.
    /// </summary>
    internal static async Task<ResumeBoundary?> TryReadLatestResumeBoundaryFromReader(TextReader reader, string origin, CancellationToken cancellationToken)
    {
        ResumeBoundary? result = null;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (line is null)
            {
                break;
            }

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

    private static async Task<ResumeBoundary?> TryReadLatestResumeBoundaryFromFile(string path, string origin, CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        using var reader = new StreamReader(stream);

        var result = await TryReadLatestResumeBoundaryFromReader(reader, origin, cancellationToken).ConfigureAwait(false);
        return result;
    }

    /// <summary>
    /// Reads only the scratch checkpoint metadata needed to resume rollover after restart.
    /// </summary>
    public static async Task<ScratchCheckpointMetadata?> TryReadScratchCheckpointMetadata(string path, CancellationToken cancellationToken)
    {
        ScratchCheckpointMetadata? result = null;

        if (!File.Exists(path))
        {
            return result;
        }

        await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        using var reader = new StreamReader(stream);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (line is null)
            {
                break;
            }

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
            var buyerIsMaker = root.GetProperty("isBuyerMaker").GetBoolean();
            var key = new TradeKey(exchange, instrument, tradeId);
            var lastRecordedTrade = new TradeInfo(key, timestamp, price, quantity, buyerIsMaker);
            var activeScratchUtcDate = DateOnly.FromDateTime(timestamp.UtcDateTime);

            result = new ScratchCheckpointMetadata(activeScratchUtcDate, lastRecordedTrade);
        }

        return result;
    }

    internal static string SerializeTrade(TradeInfo trade)
    {
        var record = new
        {
            exchange = trade.Key.Exchange.ToString(),
            instrument = trade.Key.Symbol.ToString(),
            tradeId = trade.Key.TradeId,
            timestamp = trade.Timestamp,
            price = trade.Price,
            quantity = trade.Quantity,
            isBuyerMaker = trade.BuyerIsMaker,
        };

        var result = JsonSerializer.Serialize(record);
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

    /// <summary>
    /// Describes the minimal scratch metadata required to resume checkpoint rollover after restart.
    /// </summary>
    public sealed record ScratchCheckpointMetadata(DateOnly ActiveScratchUtcDate, TradeInfo LastRecordedTrade);
}
