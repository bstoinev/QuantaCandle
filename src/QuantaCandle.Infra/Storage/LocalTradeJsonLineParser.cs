using System.Text.Json;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Storage;

/// <summary>
/// Parses recorder-style trade JSONL rows into normalized trade records.
/// </summary>
internal static class LocalTradeJsonLineParser
{
    /// <summary>
    /// Parses one JSONL line into a trade record and validates the required contract fields.
    /// </summary>
    public static TradeInfo ParseTrade(string line, string filePath, int lineNumber)
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
}
