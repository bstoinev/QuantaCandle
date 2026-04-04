using System.Globalization;
using System.Text.Json;

using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance.Internal;

namespace QuantaCandle.Exchange.Binance;

/// <summary>
/// Fetches bounded missing trade ranges from the Binance spot REST API.
/// </summary>
/// <remarks>
/// Initializes the Binance bounded trade gap fetch client.
/// </remarks>
public sealed class BinanceTradeGapFetchClient(HttpClient httpClient) : ITradeGapFetchClient
{
    private const int MaxTradesPerRequest = 1000;
    private static readonly Uri DefaultRestBaseAddress = new("https://api.binance.com");
    private static readonly ExchangeId BinanceExchange = new("Binance");

    private readonly HttpClient Http = httpClient ?? throw new ArgumentNullException(nameof(httpClient));

    /// <summary>
    /// Fetches normalized trade items for the requested bounded trade identifier range.
    /// </summary>
    public async ValueTask<IReadOnlyList<TradeInfo>> Fetch(
        Instrument instrument,
        long missingTradeIdStart,
        long missingTradeIdEnd,
        CancellationToken cancellationToken)
    {
        ValidateRequestedRange(missingTradeIdStart, missingTradeIdEnd);

        var requestedSymbol = BinanceSymbol.ToStreamSymbol(instrument);
        var fetchedTrades = new List<TradeInfo>();
        var nextTradeId = missingTradeIdStart;

        while (nextTradeId <= missingTradeIdEnd)
        {
            var remainingTradeCount = missingTradeIdEnd - nextTradeId + 1;
            var requestLimit = (int)Math.Min(MaxTradesPerRequest, remainingTradeCount);

            using var request = CreateRequest(requestedSymbol, nextTradeId, requestLimit);
            using var response = await Http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            response.EnsureSuccessStatusCode();

            await using var payloadStream = await response.Content.ReadAsStreamAsync(cancellationToken);
            using var payloadDocument = await JsonDocument.ParseAsync(payloadStream, cancellationToken: cancellationToken);
            var pageTrades = ParseTrades(payloadDocument.RootElement, instrument, requestedSymbol, nextTradeId, missingTradeIdEnd);

            if (pageTrades.Count == 0)
            {
                break;
            }

            SortTradesByNumericTradeId(pageTrades);
            fetchedTrades.AddRange(pageTrades);
            nextTradeId = GetTradeId(pageTrades[^1]) + 1;
        }

        SortTradesByNumericTradeId(fetchedTrades);
        EnsureUniqueTradeIds(fetchedTrades);

        IReadOnlyList<TradeInfo> result = fetchedTrades;
        return result;
    }

    private HttpRequestMessage CreateRequest(string requestedSymbol, long fromTradeId, int requestLimit)
    {
        var requestUri = BuildRequestUri(requestedSymbol, fromTradeId, requestLimit);
        var result = new HttpRequestMessage(HttpMethod.Get, requestUri);
        return result;
    }

    private Uri BuildRequestUri(string requestedSymbol, long fromTradeId, int requestLimit)
    {
        var path = FormattableString.Invariant($"/api/v3/historicalTrades?symbol={Uri.EscapeDataString(requestedSymbol)}&limit={requestLimit}&fromId={fromTradeId}");

        var result = new Uri(Http.BaseAddress ?? DefaultRestBaseAddress, path);

        return result;
    }

    private static void EnsureUniqueTradeIds(List<TradeInfo> trades)
    {
        for (var i = 1; i < trades.Count; i++)
        {
            var currentTradeId = GetTradeId(trades[i]);
            var previousTradeId = GetTradeId(trades[i - 1]);
            if (currentTradeId == previousTradeId)
            {
                throw new InvalidOperationException($"Binance gap fetch returned duplicate trade id '{currentTradeId}'.");
            }
        }
    }

    private static long GetInt64(JsonElement payload, string propertyName, int index)
    {
        if (!payload.TryGetProperty(propertyName, out var property))
        {
            throw new InvalidOperationException($"Binance trade payload at index {index} is missing required property '{propertyName}'.");
        }

        if (property.ValueKind != JsonValueKind.Number || !property.TryGetInt64(out var result))
        {
            throw new InvalidOperationException($"Binance trade payload at index {index} has non-numeric '{propertyName}'.");
        }

        return result;
    }

    private static string? GetOptionalString(JsonElement payload, string propertyName, int index)
    {
        string? result = null;

        if (payload.TryGetProperty(propertyName, out var property))
        {
            if (property.ValueKind != JsonValueKind.String)
            {
                throw new InvalidOperationException($"Binance trade payload at index {index} has non-string '{propertyName}'.");
            }

            result = property.GetString();
        }

        return result;
    }

    private static decimal GetRequiredDecimal(JsonElement payload, string propertyName, int index)
    {
        if (!payload.TryGetProperty(propertyName, out var property))
        {
            throw new InvalidOperationException($"Binance trade payload at index {index} is missing required property '{propertyName}'.");
        }

        if (property.ValueKind != JsonValueKind.String)
        {
            throw new InvalidOperationException($"Binance trade payload at index {index} has non-string '{propertyName}'.");
        }

        var rawValue = property.GetString();
        if (!decimal.TryParse(rawValue, NumberStyles.Float, CultureInfo.InvariantCulture, out var result))
        {
            throw new InvalidOperationException($"Binance trade payload at index {index} has invalid decimal '{propertyName}'.");
        }

        return result;
    }

    private static long GetTradeId(TradeInfo trade)
    {
        if (!long.TryParse(trade.Key.TradeId, NumberStyles.None, CultureInfo.InvariantCulture, out var result))
        {
            throw new InvalidOperationException($"Trade '{trade.Key.TradeId}' is not numeric.");
        }

        return result;
    }

    private static List<TradeInfo> ParseTrades(
        JsonElement payload,
        Instrument instrument,
        string requestedSymbol,
        long requestedStartTradeId,
        long requestedEndTradeId)
    {
        if (payload.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException("Binance historical trades payload must be a JSON array.");
        }

        var result = new List<TradeInfo>(payload.GetArrayLength());
        var index = 0;
        foreach (var item in payload.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException($"Binance trade payload at index {index} must be a JSON object.");
            }

            var tradeId = GetInt64(item, "id", index);
            ValidateTradeIdBounds(tradeId, requestedStartTradeId, requestedEndTradeId, index);
            ValidateOptionalInstrumentData(item, requestedSymbol, index);

            var timestampUnixMilliseconds = GetInt64(item, "time", index);
            var price = GetRequiredDecimal(item, "price", index);
            var quantity = GetRequiredDecimal(item, "qty", index);
            var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(timestampUnixMilliseconds);
            var tradeKey = new TradeKey(BinanceExchange, instrument, tradeId.ToString(CultureInfo.InvariantCulture));
            var trade = new TradeInfo(tradeKey, timestamp, price, quantity);
            result.Add(trade);
            index++;
        }

        return result;
    }

    private static void SortTradesByNumericTradeId(List<TradeInfo> trades)
    {
        trades.Sort(static (left, right) => GetTradeId(left).CompareTo(GetTradeId(right)));
    }

    private static void ValidateOptionalInstrumentData(JsonElement payload, string requestedSymbol, int index)
    {
        var payloadExchange = GetOptionalString(payload, "exchange", index);
        if (payloadExchange is not null && !payloadExchange.Equals(BinanceExchange.Value, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Binance trade payload at index {index} has unexpected exchange '{payloadExchange}'. Expected '{BinanceExchange.Value}'.");
        }

        var payloadSymbol = GetOptionalString(payload, "symbol", index);
        if (payloadSymbol is not null && !payloadSymbol.Equals(requestedSymbol, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Binance trade payload at index {index} has unexpected symbol '{payloadSymbol}'. Expected '{requestedSymbol}'.");
        }
    }

    private static void ValidateRequestedRange(long missingTradeIdStart, long missingTradeIdEnd)
    {
        if (missingTradeIdStart <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(missingTradeIdStart), missingTradeIdStart, "Missing trade id start must be positive.");
        }

        if (missingTradeIdEnd < missingTradeIdStart)
        {
            throw new ArgumentOutOfRangeException(nameof(missingTradeIdEnd), missingTradeIdEnd, "Missing trade id end must be greater than or equal to missing trade id start.");
        }
    }

    private static void ValidateTradeIdBounds(long tradeId, long requestedStartTradeId, long requestedEndTradeId, int index)
    {
        if (tradeId < requestedStartTradeId || tradeId > requestedEndTradeId)
        {
            throw new InvalidOperationException(
                $"Binance trade payload at index {index} returned out-of-range trade id '{tradeId}'. Requested range is {requestedStartTradeId}-{requestedEndTradeId}.");
        }
    }
}
