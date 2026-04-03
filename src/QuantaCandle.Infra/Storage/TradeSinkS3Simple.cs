using System.Text.Json;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra;

public sealed class TradeSinkS3Simple : ITradeSink
{
    private readonly TradeSinkS3SimpleOptions options;
    private readonly IS3ObjectUploader uploader;
    private readonly Dictionary<(Instrument Instrument, DateOnly UtcDate), List<TradeInfo>> bufferedTradesByDay = [];

    public TradeSinkS3Simple(TradeSinkS3SimpleOptions options, IS3ObjectUploader uploader, IClock clock)
    {
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.uploader = uploader ?? throw new ArgumentNullException(nameof(uploader));
        ArgumentNullException.ThrowIfNull(clock);

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

        HashSet<(Instrument Instrument, DateOnly UtcDate)> touchedBuffers = [];

        foreach (TradeInfo trade in trades)
        {
            var bufferKey = (trade.Key.Symbol, DateOnly.FromDateTime(trade.Timestamp.UtcDateTime));
            if (!bufferedTradesByDay.TryGetValue(bufferKey, out List<TradeInfo>? dailyTrades))
            {
                dailyTrades = [];
                bufferedTradesByDay[bufferKey] = dailyTrades;
            }

            dailyTrades.Add(trade);
            touchedBuffers.Add(bufferKey);
        }

        foreach (var touchedBuffer in touchedBuffers
                     .OrderBy(item => item.Instrument.ToString(), StringComparer.Ordinal)
                     .ThenBy(item => item.UtcDate))
        {
            cancellationToken.ThrowIfCancellationRequested();

            List<TradeInfo> sortedTrades = bufferedTradesByDay[touchedBuffer]
                .OrderBy(trade => trade.Timestamp)
                .ThenBy(trade => trade.Key.TradeId, StringComparer.Ordinal)
                .ToList();

            string payload = BuildJsonlPayload(sortedTrades);
            string objectKey = TradeSinkS3DailyObjectKey.Build(
                options.Prefix,
                touchedBuffer.Instrument.ToString(),
                touchedBuffer.UtcDate);

            await uploader.UploadTextAsync(options.BucketName, objectKey, payload, cancellationToken).ConfigureAwait(false);
        }

        return new TradeAppendResult(insertedCount, DuplicateCount: 0);
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
}
