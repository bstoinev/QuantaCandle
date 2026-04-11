using System.Globalization;
using System.Text.Json;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Exchange.Binance.Internal;

/// <summary>
/// Provides shared parsing helpers for Binance raw-trade REST payloads.
/// </summary>
internal static class BinanceHelper
{
    public const int MAX_TRADES_PER_REQUEST = 1000;

    public static readonly Uri RestBaseAddress = new("https://api.binance.com");
    public static readonly ExchangeId Signature = new("Binance");

    /// <summary>
    /// Reads a required 64-bit integer property from a Binance payload object.
    /// </summary>
    public static long GetInt64(JsonElement payload, string propertyName, int index, string payloadName)
    {
        if (!payload.TryGetProperty(propertyName, out var property))
        {
            throw new InvalidOperationException($"Binance {payloadName} payload at index {index} is missing required property '{propertyName}'.");
        }

        if (property.ValueKind != JsonValueKind.Number || !property.TryGetInt64(out var result))
        {
            throw new InvalidOperationException($"Binance {payloadName} payload at index {index} has non-numeric '{propertyName}'.");
        }

        return result;
    }

    /// <summary>
    /// Reads an optional string property from a Binance payload object.
    /// </summary>
    public static string? GetOptionalString(JsonElement payload, string propertyName, int index, string payloadName)
    {
        string? result = null;

        if (payload.TryGetProperty(propertyName, out var property))
        {
            if (property.ValueKind != JsonValueKind.String)
            {
                throw new InvalidOperationException($"Binance {payloadName} payload at index {index} has non-string '{propertyName}'.");
            }

            result = property.GetString();
        }

        return result;
    }

    /// <summary>
    /// Reads a required decimal string property from a Binance payload object.
    /// </summary>
    public static decimal GetRequiredDecimal(JsonElement payload, string propertyName, int index, string payloadName)
    {
        if (!payload.TryGetProperty(propertyName, out var property))
        {
            throw new InvalidOperationException($"Binance {payloadName} payload at index {index} is missing required property '{propertyName}'.");
        }

        if (property.ValueKind != JsonValueKind.String)
        {
            throw new InvalidOperationException($"Binance {payloadName} payload at index {index} has non-string '{propertyName}'.");
        }

        var rawValue = property.GetString();
        if (!decimal.TryParse(rawValue, NumberStyles.Float, CultureInfo.InvariantCulture, out var result))
        {
            throw new InvalidOperationException($"Binance {payloadName} payload at index {index} has invalid decimal '{propertyName}'.");
        }

        return result;
    }

    /// <summary>
    /// Parses a normalized trade identifier as a 64-bit integer.
    /// </summary>
    public static long GetTradeId(TradeInfo trade)
    {
        if (!long.TryParse(trade.Key.TradeId, NumberStyles.None, CultureInfo.InvariantCulture, out var result))
        {
            throw new InvalidOperationException($"Trade '{trade.Key.TradeId}' is not numeric.");
        }

        return result;
    }

    /// <summary>
    /// Ensures the payload is a JSON array before enumerating raw-trade items.
    /// </summary>
    public static void ValidateArray(JsonElement payload, string payloadDescription)
    {
        if (payload.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException($"Binance {payloadDescription} payload must be a JSON array.");
        }
    }

    /// <summary>
    /// Ensures the payload item is a JSON object before reading raw-trade properties.
    /// </summary>
    public static void ValidateObject(JsonElement payload, int index, string payloadName)
    {
        if (payload.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException($"Binance {payloadName} payload at index {index} must be a JSON object.");
        }
    }
}
