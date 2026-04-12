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
    public async ValueTask Fetch(
        Instrument instrument,
        long startId,
        long endId,
        ITradeGapFetchedPageSink pageSink,
        ITradeGapProgressReporter? progressReporter,
        CancellationToken terminator)
    {
        ValidateRequestedRange(startId, endId);
        ArgumentNullException.ThrowIfNull(pageSink);

        var requestedSymbol = BinanceSymbol.ToRestSymbol(instrument);
        var nextTradeId = startId;
        var expectedTradeCount = endId - startId + 1;
        var pageNumber = 0;
        long completedTradeCount = 0;

        if (progressReporter is not null)
        {
            await progressReporter
                .Report(
                    new TradeGapProgressUpdate("starting", 0, expectedTradeCount, 0, false),
                    terminator)
                .ConfigureAwait(false);
        }

        while (nextTradeId <= endId)
        {
            var remainingTradeCount = endId - nextTradeId + 1;
            var requestLimit = (int)Math.Min(BinanceHelper.MAX_TRADES_PER_REQUEST, remainingTradeCount);
            pageNumber++;

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

            await pageSink.AcceptPage(pageTrades, terminator).ConfigureAwait(false);
            nextTradeId = BinanceHelper.GetTradeId(pageTrades[^1]) + 1;
            completedTradeCount += pageTrades.Count;

            if (progressReporter is not null)
            {
                await progressReporter
                    .Report(
                        new TradeGapProgressUpdate("downloading", completedTradeCount, expectedTradeCount, pageNumber, false),
                        terminator)
                    .ConfigureAwait(false);
            }
        }

        if (progressReporter is not null)
        {
            await progressReporter
                .Report(
                    new TradeGapProgressUpdate(
                        completedTradeCount == expectedTradeCount ? "complete" : "partial",
                        completedTradeCount,
                        expectedTradeCount,
                        pageNumber,
                        true),
                    terminator)
                .ConfigureAwait(false);
        }
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
