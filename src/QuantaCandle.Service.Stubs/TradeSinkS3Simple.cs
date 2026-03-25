using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using QuantaCandle.Core;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Service.Stubs;

public sealed class TradeSinkS3Simple : ITradeSink
{
    private readonly TradeSinkS3SimpleOptions options;
    private readonly IS3ObjectUploader uploader;
    private readonly IClock clock;

    public TradeSinkS3Simple(TradeSinkS3SimpleOptions options, IS3ObjectUploader uploader, IClock clock)
    {
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.uploader = uploader ?? throw new ArgumentNullException(nameof(uploader));
        this.clock = clock ?? throw new ArgumentNullException(nameof(clock));

        if (string.IsNullOrWhiteSpace(this.options.BucketName))
        {
            throw new ArgumentException("BucketName is required for the S3 trade sink.", nameof(options));
        }
    }

    public async ValueTask<TradeAppendResult> Append(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
    {
        int insertedCount = trades.Count;
        if (insertedCount == 0)
        {
            return new TradeAppendResult(InsertedCount: 0, DuplicateCount: 0);
        }

        DateTimeOffset runTimestamp = clock.UtcNow;

        Dictionary<(string Exchange, string Instrument, string Day), List<TradeInfo>> byPartition =
            new Dictionary<(string Exchange, string Instrument, string Day), List<TradeInfo>>();

        foreach (TradeInfo trade in trades)
        {
            string exchange = trade.Key.Exchange.ToString();
            string instrument = trade.Key.Symbol.ToString();
            string day = trade.Timestamp.UtcDateTime.ToString("yyyy-MM-dd");

            (string Exchange, string Instrument, string Day) partition = (exchange, instrument, day);
            if (!byPartition.TryGetValue(partition, out List<TradeInfo>? partitionTrades))
            {
                partitionTrades = new List<TradeInfo>();
                byPartition[partition] = partitionTrades;
            }

            partitionTrades.Add(trade);
        }

        foreach (KeyValuePair<(string Exchange, string Instrument, string Day), List<TradeInfo>> partitionEntry in byPartition
                     .OrderBy(item => item.Key.Exchange, StringComparer.Ordinal)
                     .ThenBy(item => item.Key.Instrument, StringComparer.Ordinal)
                     .ThenBy(item => item.Key.Day, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();

            List<TradeInfo> sortedTrades = partitionEntry.Value
                .OrderBy(trade => trade.Timestamp)
                .ThenBy(trade => trade.Key.TradeId, StringComparer.Ordinal)
                .ToList();

            string payload = BuildJsonlPayload(sortedTrades);
            string objectKey = BuildObjectKey(partitionEntry.Key, sortedTrades, runTimestamp);

            await uploader.UploadTextAsync(options.BucketName, objectKey, payload, cancellationToken).ConfigureAwait(false);
        }

        return new TradeAppendResult(insertedCount, DuplicateCount: 0);
    }

    private string BuildObjectKey((string Exchange, string Instrument, string Day) partition, IReadOnlyList<TradeInfo> trades, DateTimeOffset runTimestamp)
    {
        DateTimeOffset minTimestamp = trades[0].Timestamp;
        DateTimeOffset maxTimestamp = trades[^1].Timestamp;
        string hash = ComputeDeterministicHash(trades);
        string fileName =
            $"{runTimestamp.UtcDateTime:yyyyMMddTHHmmssfffZ}_{minTimestamp.UtcDateTime:HHmmssfff}_{maxTimestamp.UtcDateTime:HHmmssfff}_{trades.Count}_{hash}.jsonl";
        string partitionPath = $"{partition.Exchange}/{partition.Instrument}/{partition.Day}/{fileName}";

        string prefix = NormalizePrefix(options.Prefix);
        return string.IsNullOrEmpty(prefix)
            ? partitionPath
            : $"{prefix}/{partitionPath}";
    }

    private static string BuildJsonlPayload(IReadOnlyList<TradeInfo> trades)
    {
        string[] lines = new string[trades.Count];
        for (int i = 0; i < trades.Count; i++)
        {
            TradeInfo trade = trades[i];

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

        return string.Join(Environment.NewLine, lines) + Environment.NewLine;
    }

    private static string NormalizePrefix(string? prefix)
    {
        if (string.IsNullOrWhiteSpace(prefix))
        {
            return string.Empty;
        }

        return prefix.Replace('\\', '/').Trim('/');
    }

    private static string ComputeDeterministicHash(IReadOnlyList<TradeInfo> trades)
    {
        StringBuilder content = new StringBuilder();
        foreach (TradeInfo trade in trades)
        {
            content.Append(trade.Key.Exchange);
            content.Append('|');
            content.Append(trade.Key.Symbol);
            content.Append('|');
            content.Append(trade.Key.TradeId);
            content.Append('|');
            content.Append(trade.Timestamp.UtcDateTime.ToString("O"));
            content.Append('|');
            content.Append(trade.Price.ToString(CultureInfo.InvariantCulture));
            content.Append('|');
            content.Append(trade.Quantity.ToString(CultureInfo.InvariantCulture));
            content.Append('\n');
        }

        byte[] hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(content.ToString()));
        return Convert.ToHexString(hashBytes).ToLowerInvariant()[..12];
    }
}
