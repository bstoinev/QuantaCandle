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
    private readonly HttpClient _http = httpClient ?? throw new ArgumentNullException(nameof(httpClient));

    /// <summary>
    /// Fetches normalized trade items for the requested bounded trade identifier range.
    /// </summary>
    public async ValueTask<IReadOnlyList<TradeInfo>> Fetch(Instrument instrument, long startId, long endId, CancellationToken terminator)
    {
        ValidateRequestedRange(startId, endId);

        var requestedSymbol = BinanceSymbol.ToRestSymbol(instrument);
        var result = new List<TradeInfo>();
        var nextTradeId = startId;

        while (nextTradeId <= endId)
        {
            var remainingTradeCount = endId - nextTradeId + 1;
            var requestLimit = (int)Math.Min(BinanceHelper.MAX_TRADES_PER_REQUEST, remainingTradeCount);

            using var request = CreateRequest(requestedSymbol, nextTradeId, requestLimit);
            using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, terminator).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            await using var payloadStream = await response.Content.ReadAsStreamAsync(terminator).ConfigureAwait(false);
            using var payloadDocument = await JsonDocument.ParseAsync(payloadStream, cancellationToken: terminator).ConfigureAwait(false);
            var pageTrades = ParseTrades(payloadDocument.RootElement, instrument, requestedSymbol, nextTradeId, endId);

            if (pageTrades.Count == 0)
            {
                break;
            }

            result.AddRange(pageTrades);
            nextTradeId = BinanceHelper.GetTradeId(pageTrades[^1]) + 1;
        }
        var firstId = BinanceHelper.GetTradeId(result[0]);
        var lastId = BinanceHelper.GetTradeId(result[^1]);

        if (firstId != startId)
        {
            throw new Exception($"Unexpected first trade ID of '{firstId}' is returned by the exchange. Expected '{startId}'.");
        }
        else if (lastId != endId)
        {
            throw new Exception($"Unexpected last trade ID of '{lastId}' is returned by the exchange. Expected '{endId}'.");
        }
        else if (lastId != startId + result.Count - 1)
        {
            throw new Exception($"Unexpected trade sequence retruned by the exchange. First ID is '{firstId}', last ID is '{lastId}', but expected {endId - startId + 1} trades.");
        }

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

        var result = new Uri(_http.BaseAddress ?? BinanceHelper.RestBaseAddress, path);

        return result;
    }

    private static List<TradeInfo> ParseTrades(
        JsonElement payload,
        Instrument instrument,
        string requestedSymbol,
        long requestedStartTradeId,
        long requestedEndTradeId)
    {
        BinanceHelper.ValidateArray(payload, "historical trades");

        var result = new List<TradeInfo>(payload.GetArrayLength());
        var index = 0;
        foreach (var item in payload.EnumerateArray())
        {
            BinanceHelper.ValidateObject(item, index, "trade");

            var tradeId = BinanceHelper.GetInt64(item, "id", index, "trade");
            ValidateTradeIdBounds(tradeId, requestedStartTradeId, requestedEndTradeId, index);
            ValidateOptionalInstrumentData(item, requestedSymbol, index);

            var timestampUnixMilliseconds = BinanceHelper.GetInt64(item, "time", index, "trade");
            var price = BinanceHelper.GetRequiredDecimal(item, "price", index, "trade");
            var quantity = BinanceHelper.GetRequiredDecimal(item, "qty", index, "trade");
            var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(timestampUnixMilliseconds);
            var tradeKey = new TradeKey(BinanceHelper.Signature, instrument, tradeId.ToString(System.Globalization.CultureInfo.InvariantCulture));
            var trade = new TradeInfo(tradeKey, timestamp, price, quantity);
            result.Add(trade);
            index++;
        }

        return result;
    }

    private static void ValidateOptionalInstrumentData(JsonElement payload, string requestedSymbol, int index)
    {
        var payloadExchange = BinanceHelper.GetOptionalString(payload, "exchange", index, "trade");
        if (payloadExchange is not null && !payloadExchange.Equals(BinanceHelper.Signature.Value, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Binance trade payload at index {index} has unexpected exchange '{payloadExchange}'. Expected '{BinanceHelper.Signature.Value}'.");
        }

        var payloadSymbol = BinanceHelper.GetOptionalString(payload, "symbol", index, "trade");
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
            throw new ArgumentOutOfRangeException(nameof(missingTradeIdStart), missingTradeIdStart, "Missing trade ID start must be positive.");
        }

        if (missingTradeIdEnd < missingTradeIdStart)
        {
            throw new ArgumentOutOfRangeException(nameof(missingTradeIdEnd), missingTradeIdEnd, "Missing trade ID end must be greater than or equal to missing trade ID start.");
        }
    }

    private static void ValidateTradeIdBounds(long tradeId, long requestedStartTradeId, long requestedEndTradeId, int index)
    {
        if (tradeId < requestedStartTradeId || tradeId > requestedEndTradeId)
        {
            throw new InvalidOperationException(
                $"Binance trade payload at index {index} returned out-of-range trade ID '{tradeId}'. Requested range is {requestedStartTradeId}-{requestedEndTradeId}.");
        }
    }
}
