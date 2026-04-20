using System.Net;
using System.Net.Http;
using System.Text;

using LogMachina;

using Moq;
using Moq.Protected;

using QuantaCandle.Exchange.Binance;

namespace QuantaCandle.Infra.Tests.Exchange;

/// <summary>
/// Verifies Binance raw trade lookup endpoint usage for UTC day-boundary resolution.
/// </summary>
public sealed class BinanceRawTradeLookupClientTests
{
    [Fact]
    public async Task TryVerifyRawTradeIdUsesHistoricalTradesEndpoint()
    {
        var requestUris = new List<Uri>();
        using var httpClient = CreateHttpClient(
            requestUris,
            _ =>
                """
                [
                  { "id": 200, "price": "100.10", "qty": "0.50", "time": 1775779200001, "isBuyerMaker": false }
                ]
                """);
        var client = new BinanceRawTradeLookupClient(httpClient, CreateLog());

        var result = await client.TryVerifyRawTradeId("BTC-USDT", 200, CancellationToken.None);

        Assert.True(result);
        Assert.Single(requestUris);
        Assert.Equal("/api/v3/historicalTrades", requestUris[0].AbsolutePath);
        Assert.Contains("symbol=BTCUSDT", requestUris[0].Query, StringComparison.Ordinal);
        Assert.Contains("fromId=200", requestUris[0].Query, StringComparison.Ordinal);
        Assert.DoesNotContain("aggTrades", requestUris[0].ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task FindFirstTradeAtUsesAggregateTradesEndpointWithRestSymbol()
    {
        var requestUris = new List<Uri>();
        using var httpClient = CreateHttpClient(
            requestUris,
            request => request.RequestUri!.AbsolutePath.Contains("/api/v3/aggTrades", StringComparison.Ordinal)
                ? """
                [
                  { "f": 200, "l": 200 }
                ]
                """
                : """
                [
                  { "id": 200, "price": "100.10", "qty": "0.50", "time": 1775779200001, "isBuyerMaker": false }
                ]
                """);
        var client = new BinanceRawTradeLookupClient(httpClient, CreateLog());

        var result = await client.FindFirstTradeAt(
            new DateTimeOffset(2026, 4, 9, 0, 0, 0, TimeSpan.Zero),
            "BTC-USDT",
            CancellationToken.None);

        Assert.Equal("200", result.Key.TradeId);
        Assert.Equal(2, requestUris.Count);
        Assert.Equal("/api/v3/aggTrades", requestUris[0].AbsolutePath);
        Assert.Contains("symbol=BTCUSDT", requestUris[0].Query, StringComparison.Ordinal);
        Assert.Equal("/api/v3/historicalTrades", requestUris[1].AbsolutePath);
        Assert.Contains("symbol=BTCUSDT", requestUris[1].Query, StringComparison.Ordinal);
    }

    private static HttpClient CreateHttpClient(ICollection<Uri> requestUris, Func<HttpRequestMessage, string> payloadFactory)
    {
        var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Loose);
        handlerMock
            .Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync((HttpRequestMessage request, CancellationToken _) =>
            {
                requestUris.Add(request.RequestUri!);
                return CreateResponse(payloadFactory(request));
            });

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

    private static ILogMachina<BinanceRawTradeLookupClient> CreateLog()
    {
        var result = new Mock<ILogMachina<BinanceRawTradeLookupClient>>().Object;
        return result;
    }
}
