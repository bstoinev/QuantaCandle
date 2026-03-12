using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Service.Stubs;

public sealed class TradeSinkFileSimple : ITradeSink
{
    private readonly TradeSinkFileSimpleOptions options;

    public TradeSinkFileSimple(TradeSinkFileSimpleOptions options)
    {
        this.options = options;
    }

    public async ValueTask<TradeAppendResult> AppendAsync(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
    {
        int insertedCount = trades.Count;

        Dictionary<string, List<TradeInfo>> byPath = new Dictionary<string, List<TradeInfo>>(StringComparer.OrdinalIgnoreCase);
        foreach (TradeInfo trade in trades)
        {
            string instrumentDirectory = Path.Combine(options.OutputDirectory, trade.Key.Symbol.ToString());
            string day = trade.Timestamp.UtcDateTime.ToString("yyyy-MM-dd");
            string fileName = $"{day}.jsonl";
            string path = Path.Combine(instrumentDirectory, fileName);

            if (!byPath.TryGetValue(path, out List<TradeInfo>? list))
            {
                list = new List<TradeInfo>();
                byPath[path] = list;
            }

            list.Add(trade);
        }

        foreach (KeyValuePair<string, List<TradeInfo>> kvp in byPath)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string? directory = Path.GetDirectoryName(kvp.Key);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            List<TradeInfo> list = kvp.Value;
            string[] lines = new string[list.Count];
            for (int i = 0; i < list.Count; i++)
            {
                TradeInfo trade = list[i];

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

            string payload = string.Join(Environment.NewLine, lines) + Environment.NewLine;
            await File.AppendAllTextAsync(kvp.Key, payload, cancellationToken).ConfigureAwait(false);
        }

        return new TradeAppendResult(insertedCount, DuplicateCount: 0);
    }
}
