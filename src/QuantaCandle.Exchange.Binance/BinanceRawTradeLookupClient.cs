using System.Text.Json; 

using LogMachina;

using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance.Internal;

namespace QuantaCandle.Exchange.Binance;

/// <summary>
/// Resolves Binance raw trades needed to calculate UTC day boundaries.
/// </summary>
public sealed class BinanceRawTradeLookupClient(
    HttpClient httpClient,
    ILogMachina<BinanceRawTradeLookupClient> log) : IBinanceRawTradeLookupClient
{
    private const int PAGE_SIZE = 100;

    private readonly HttpClient _http = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
    private readonly ILogMachina<BinanceRawTradeLookupClient> _log = log ?? throw new ArgumentNullException(nameof(log));

    public async ValueTask<TradeInfo> FindFirstTradeAt(DateTimeOffset timestampUtc, Instrument instrument, CancellationToken cancellationToken)
    {
        ValidateTimestampUtc(timestampUtc);

        var requestedSymbol = BinanceSymbol.ToStreamSymbol(instrument);
        var (seedTradeId, seedLastTradeId) = await GetAggregateSeedTradeRange(requestedSymbol, timestampUtc, cancellationToken).ConfigureAwait(false);
        _log.Trace($"Resolving first Binance raw trade at or after {timestampUtc:O} for {instrument} using aggregate seed {seedTradeId}-{seedLastTradeId}.");

        /*
            If the first returned aggregate trade spans multiple raw trades, and that aggregate
            is the first one whose aggregate timestamp qualifies, some raw trades inside it can
            still be before `timestampUtc`. In that case, `f` can be earlier than the desired
            boundary, and you must scan forward through raw trades to find the true first raw trade
            at or after the timestamp.
         */

        var nextTradeId = seedTradeId;
        var hasResult = false;
        TradeInfo result = default;

        while (!hasResult)
        {
            var pageSize = (int)(seedLastTradeId - nextTradeId + 1);
            var trades = await FetchRawTrades(requestedSymbol, instrument, nextTradeId, Math.Max(pageSize, PAGE_SIZE), cancellationToken).ConfigureAwait(false);
            if (trades.Count == 0)
            {
                throw new InvalidOperationException($"Binance raw trade lookup returned no trades from id '{nextTradeId}' for {instrument}.");
            }

            for (var i = 0; i < trades.Count; i++)
            {
                if (trades[i].Timestamp >= timestampUtc)
                {
                    result = trades[i];
                    hasResult = true;
                    break;
                }
            }

            nextTradeId = BinanceHelper.GetTradeId(trades[^1]) + 1;
        }

        _log.Debug($"Resolved first Binance raw trade at or after {timestampUtc:O} for {instrument}: {result.Key.TradeId}.");
        return result;
    }

    /// <summary>
    /// Verifies that the requested Binance raw trade identifier exists exactly.
    /// </summary>
    public async ValueTask<bool> TryVerifyRawTradeId(Instrument instrument, long tradeId, CancellationToken cancellationToken)
    {
        if (tradeId <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(tradeId), tradeId, "Trade id must be positive.");
        }

        var requestedSymbol = BinanceSymbol.ToStreamSymbol(instrument);
        var trades = await FetchRawTrades(requestedSymbol, instrument, tradeId, 1, cancellationToken).ConfigureAwait(false);
        var verified = trades.Count == 1 && BinanceHelper.GetTradeId(trades[0]) == tradeId;

        _log.Debug($"Verified Binance raw trade id {tradeId} for {instrument}: {verified}.");
        return verified;
    }

    private Uri BuildAggregateTradeRequestUri(string requestedSymbol, DateTimeOffset timestampUtc)
    {
        var path = FormattableString.Invariant($"/api/v3/aggTrades?symbol={Uri.EscapeDataString(requestedSymbol)}&limit=1&startTime={timestampUtc.ToUnixTimeMilliseconds()}");
        var result = new Uri(_http.BaseAddress ?? BinanceHelper.RestBaseAddress, path);
        return result;
    }

    private Uri BuildHistoricalTradeRequestUri(string requestedSymbol, long fromTradeId, int requestLimit)
    {
        var path = FormattableString.Invariant($"/api/v3/historicalTrades?symbol={Uri.EscapeDataString(requestedSymbol)}&limit={requestLimit}&fromId={fromTradeId}");
        var result = new Uri(_http.BaseAddress ?? BinanceHelper.RestBaseAddress, path);
        return result;
    }

    private async ValueTask<(long FirstTradeId, long LastTradeId)> GetAggregateSeedTradeRange(
        string requestedSymbol,
        DateTimeOffset timestampUtc,
        CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, BuildAggregateTradeRequestUri(requestedSymbol, timestampUtc));
        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        await using var payloadStream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        using var payloadDocument = await JsonDocument.ParseAsync(payloadStream, cancellationToken: cancellationToken).ConfigureAwait(false);

        BinanceHelper.ValidateArray(payloadDocument.RootElement, "aggregate trades");

        if (payloadDocument.RootElement.GetArrayLength() == 0)
        {
            throw new InvalidOperationException($"Binance aggregate trades lookup returned no trades at or after {timestampUtc:O}.");
        }

        var item = payloadDocument.RootElement[0];
        BinanceHelper.ValidateObject(item, 0, "aggregate trades");

        var firstTradeId = BinanceHelper.GetInt64(item, "f", 0, "aggregate trades");
        var lastTradeId = BinanceHelper.GetInt64(item, "l", 0, "aggregate trades");
        var result = (firstTradeId, lastTradeId);
        return result;
    }

    private async ValueTask<IReadOnlyList<TradeInfo>> FetchRawTrades(
        string requestedSymbol,
        Instrument instrument,
        long fromTradeId,
        int requestLimit,
        CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, BuildHistoricalTradeRequestUri(requestedSymbol, fromTradeId, requestLimit));
        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        await using var payloadStream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        using var payloadDocument = await JsonDocument.ParseAsync(payloadStream, cancellationToken: cancellationToken).ConfigureAwait(false);

        var result = ParseRawTrades(payloadDocument.RootElement, instrument);
        return result;
    }

    private static List<TradeInfo> ParseRawTrades(JsonElement payload, Instrument instrument)
    {
        BinanceHelper.ValidateArray(payload, "historical trades");

        var result = new List<TradeInfo>(payload.GetArrayLength());
        var index = 0;

        foreach (var item in payload.EnumerateArray())
        {
            BinanceHelper.ValidateObject(item, index, "trade");

            var tradeId = BinanceHelper.GetInt64(item, "id", index, "trade");
            var timestampUnixMilliseconds = BinanceHelper.GetInt64(item, "time", index, "trade");
            var price = BinanceHelper.GetRequiredDecimal(item, "price", index, "trade");
            var quantity = BinanceHelper.GetRequiredDecimal(item, "qty", index, "trade");
            var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(timestampUnixMilliseconds);
            var tradeKey = new TradeKey(_binanceId, instrument, tradeId.ToString(System.Globalization.CultureInfo.InvariantCulture));
            var trade = new TradeInfo(tradeKey, timestamp, price, quantity);

            result.Add(trade);
            index++;
        }

        return result;
    }

    private static void ValidateTimestampUtc(DateTimeOffset timestampUtc)
    {
        if (timestampUtc == default)
        {
            throw new ArgumentException("TimestampUtc must be non-default.", nameof(timestampUtc));
        }

        if (timestampUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("TimestampUtc must be expressed in UTC.", nameof(timestampUtc));
        }
    }
}
