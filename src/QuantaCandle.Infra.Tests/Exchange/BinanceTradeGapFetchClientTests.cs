using System.Net;
using System.Net.Http;
using System.Text;

using Moq;
using Moq.Protected;

using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance;

namespace QuantaCandle.Infra.Tests.Exchange;

/// <summary>
/// Verifies bounded Binance trade gap fetch behavior and contract validation.
/// </summary>
public sealed class BinanceTradeGapFetchClientTests
{
    [Fact]
    public async Task FetchReturnsBoundedTrades()
    {
        using var httpClient = CreateHttpClient(
            """
            [
              { "id": 100, "price": "100.10", "qty": "0.50", "time": 1710000000000, "symbol": "BTCUSDT", "exchange": "Binance" },
              { "id": 101, "price": "100.20", "qty": "0.60", "time": 1710000001000, "symbol": "BTCUSDT", "exchange": "Binance" },
              { "id": 102, "price": "100.30", "qty": "0.70", "time": 1710000002000, "symbol": "BTCUSDT", "exchange": "Binance" }
            ]
            """);

        var fetchClient = new BinanceTradeGapFetchClient(httpClient);
        var sink = new RecordingPageSink();

        await fetchClient.Fetch("BTC-USDT", 100, 102, sink, null, CancellationToken.None);

        var result = sink.Trades;
        Assert.Equal(3, result.Count);
        Assert.Equal(["100", "101", "102"], result.Select(static trade => trade.Key.TradeId).ToArray());
        Assert.All(result, static trade => Assert.Equal("Binance", trade.Key.Exchange.ToString()));
        Assert.All(result, static trade => Assert.Equal("BTC-USDT", trade.Key.Symbol.ToString()));
    }

    [Fact]
    public async Task FetchReturnsEmptyWhenNoTradesAreAvailable()
    {
        using var httpClient = CreateHttpClient("[]");
        var fetchClient = new BinanceTradeGapFetchClient(httpClient);
        var sink = new RecordingPageSink();

        await fetchClient.Fetch("BTC-USDT", 100, 102, sink, null, CancellationToken.None);

        Assert.Empty(sink.Trades);
    }

    [Fact]
    public async Task FetchRejectsNonNumericTradeIds()
    {
        using var httpClient = CreateHttpClient(
            """
            [
              { "id": "not-numeric", "price": "100.10", "qty": "0.50", "time": 1710000000000, "symbol": "BTCUSDT", "exchange": "Binance" }
            ]
            """);

        var fetchClient = new BinanceTradeGapFetchClient(httpClient);
        var sink = new RecordingPageSink();
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await fetchClient.Fetch("BTC-USDT", 100, 102, sink, null, CancellationToken.None));

        Assert.Contains("non-numeric 'id'", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FetchRejectsOutOfRangeTradeIds()
    {
        using var httpClient = CreateHttpClient(
            """
            [
              { "id": 103, "price": "100.10", "qty": "0.50", "time": 1710000000000, "symbol": "BTCUSDT", "exchange": "Binance" }
            ]
            """);

        var fetchClient = new BinanceTradeGapFetchClient(httpClient);
        var sink = new RecordingPageSink();
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await fetchClient.Fetch("BTC-USDT", 100, 102, sink, null, CancellationToken.None));

        Assert.Contains("out-of-range trade ID '103'", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FetchRejectsWrongInstrumentData()
    {
        using var httpClient = CreateHttpClient(
            """
            [
              { "id": 100, "price": "100.10", "qty": "0.50", "time": 1710000000000, "symbol": "ETHUSDT", "exchange": "Binance" }
            ]
            """);

        var fetchClient = new BinanceTradeGapFetchClient(httpClient);
        var sink = new RecordingPageSink();
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await fetchClient.Fetch("BTC-USDT", 100, 102, sink, null, CancellationToken.None));

        Assert.Contains("unexpected symbol 'ETHUSDT'", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FetchRejectsUnexpectedUnorderedPayload()
    {
        using var httpClient = CreateHttpClient(
            """
            [
              { "id": 102, "price": "100.30", "qty": "0.70", "time": 1710000002000, "symbol": "BTCUSDT", "exchange": "Binance" },
              { "id": 100, "price": "100.10", "qty": "0.50", "time": 1710000000000, "symbol": "BTCUSDT", "exchange": "Binance" },
              { "id": 101, "price": "100.20", "qty": "0.60", "time": 1710000001000, "symbol": "BTCUSDT", "exchange": "Binance" }
            ]
            """);

        var fetchClient = new BinanceTradeGapFetchClient(httpClient);
        var sink = new RecordingPageSink();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await fetchClient.Fetch("BTC-USDT", 100, 102, sink, null, CancellationToken.None));

        Assert.NotNull(exception);
    }

    private static HttpClient CreateHttpClient(string responsePayload)
    {
        var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Loose);
        handlerMock
            .Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(() => CreateResponse(responsePayload));

        var result = new HttpClient(handlerMock.Object)
        {
            BaseAddress = new Uri("https://unit-test.invalid"),
        };

        return result;
    }

    private static HttpResponseMessage CreateResponse(string responsePayload)
    {
        var result = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(responsePayload, Encoding.UTF8, "application/json"),
        };

        return result;
    }

    /// <summary>
    /// Records flattened fetched trades from each accepted page.
    /// </summary>
    private sealed class RecordingPageSink : ITradeGapFetchedPageSink
    {
        /// <summary>
        /// Gets the flattened trades accepted by the sink.
        /// </summary>
        public List<TradeInfo> Trades { get; } = [];

        /// <summary>
        /// Stores the supplied page trades for later assertions.
        /// </summary>
        public ValueTask AcceptPage(IReadOnlyList<TradeInfo> pageTrades, CancellationToken cancellationToken)
        {
            Trades.AddRange(pageTrades);
            return ValueTask.CompletedTask;
        }
    }
}
