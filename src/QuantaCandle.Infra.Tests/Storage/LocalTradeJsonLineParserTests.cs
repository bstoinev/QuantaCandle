using QuantaCandle.Infra.Storage;

namespace QuantaCandle.Infra.Tests.Storage;

/// <summary>
/// Verifies normalized local JSONL trade-line parsing behavior.
/// </summary>
public sealed class LocalTradeJsonLineParserTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ParseTradeReadsIsBuyerMakerWhenPresent(bool buyerIsMaker)
    {
        var line = $$"""
            {"exchange":"Binance","instrument":"BTC-USDT","tradeId":"101","timestamp":"2026-03-12T09:30:00+00:00","price":100.0,"quantity":1.0,"isBuyerMaker":{{buyerIsMaker.ToString().ToLowerInvariant()}}}
            """;

        var result = LocalTradeJsonLineParser.ParseTrade(line, "BTC-USDT\\2026-03-12.jsonl", 1);

        Assert.Equal(buyerIsMaker, result.BuyerIsMaker);
    }

    [Fact]
    public void ParseTradeThrowsWhenIsBuyerMakerIsMissing()
    {
        var line = """
            {"exchange":"Binance","instrument":"BTC-USDT","tradeId":"101","timestamp":"2026-03-12T09:30:00+00:00","price":100.0,"quantity":1.0}
            """;

        var exception = Assert.Throws<InvalidOperationException>(() => LocalTradeJsonLineParser.ParseTrade(line, "BTC-USDT\\2026-03-12.jsonl", 1));

        Assert.Contains("Failed to parse trade", exception.Message, StringComparison.Ordinal);
    }
}
