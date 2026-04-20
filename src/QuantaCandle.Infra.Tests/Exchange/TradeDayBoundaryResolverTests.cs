using LogMachina;

using Moq;

using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance;
using QuantaCandle.Exchange.Binance.Internal;

namespace QuantaCandle.Infra.Tests.Exchange;

/// <summary>
/// Verifies Binance raw trade UTC day-boundary resolution behavior.
/// </summary>
public sealed class TradeDayBoundaryResolverTests
{
    [Fact]
    public async Task ResolveReturnsExpectedFirstAndLastTradeIdsWhenVerificationSucceeds()
    {
        var instrument = Instrument.Parse("BTC-USDT");
        var utcDate = new DateOnly(2026, 4, 10);
        var lookupClientMoq = new Mock<IBinanceRawTradeLookupClient>(MockBehavior.Strict);
        var resolver = CreateResolver(lookupClientMoq);

        lookupClientMoq
            .Setup(client => client.FindFirstTradeAt(
                new DateTimeOffset(2026, 4, 10, 0, 0, 0, TimeSpan.Zero),
                instrument,
                It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeInfo>(CreateTrade(101, "2026-04-10T00:00:00.001Z", instrument)));
        lookupClientMoq
            .Setup(client => client.FindFirstTradeAt(
                new DateTimeOffset(2026, 4, 11, 0, 0, 0, TimeSpan.Zero),
                instrument,
                It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeInfo>(CreateTrade(201, "2026-04-11T00:00:00.001Z", instrument)));
        lookupClientMoq
            .Setup(client => client.TryVerifyRawTradeId(instrument, 200, It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<bool>(true));

        var result = await resolver.Resolve(new ExchangeId("Binance"), instrument, utcDate, TradeDayBoundaryResolutionMode.Strict, CancellationToken.None);

        Assert.Equal(101, result.ExpectedFirstTradeId);
        Assert.Equal(200, result.ExpectedLastTradeId);
        Assert.True(result.HasExpectedLastTradeId);
        Assert.Null(result.Warning);
    }

    [Fact]
    public async Task ResolveStrictThrowsWhenCandidateLastTradeIdCannotBeVerified()
    {
        var instrument = Instrument.Parse("BTC-USDT");
        var utcDate = new DateOnly(2026, 4, 10);
        var lookupClientMoq = new Mock<IBinanceRawTradeLookupClient>(MockBehavior.Strict);
        var resolver = CreateResolver(lookupClientMoq);

        SetupCommonLookups(lookupClientMoq, instrument);
        lookupClientMoq
            .Setup(client => client.TryVerifyRawTradeId(instrument, 200, It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<bool>(false));

        var exception = await Assert.ThrowsAsync<TradeDayBoundaryVerificationException>(
            async () => await resolver.Resolve(new ExchangeId("Binance"), instrument, utcDate, TradeDayBoundaryResolutionMode.Strict, CancellationToken.None));

        Assert.Equal(200, exception.CandidateExpectedLastTradeId);
        Assert.Equal(utcDate, exception.UtcDate);
    }

    [Fact]
    public async Task ResolveBestEffortReturnsWarningWhenCandidateLastTradeIdCannotBeVerified()
    {
        var instrument = Instrument.Parse("BTC-USDT");
        var utcDate = new DateOnly(2026, 4, 10);
        var lookupClientMoq = new Mock<IBinanceRawTradeLookupClient>(MockBehavior.Strict);
        var resolver = CreateResolver(lookupClientMoq);

        SetupCommonLookups(lookupClientMoq, instrument);
        lookupClientMoq
            .Setup(client => client.TryVerifyRawTradeId(instrument, 200, It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<bool>(false));

        var result = await resolver.Resolve(new ExchangeId("Binance"), instrument, utcDate, TradeDayBoundaryResolutionMode.BestEffort, CancellationToken.None);

        Assert.Equal(101, result.ExpectedFirstTradeId);
        Assert.Null(result.ExpectedLastTradeId);
        Assert.False(result.HasExpectedLastTradeId);
        Assert.Contains("200", result.Warning, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ResolveUsesUtcMidnightForTargetDayAndNextDay()
    {
        var instrument = Instrument.Parse("BTC-USDT");
        var utcDate = new DateOnly(2026, 4, 10);
        var lookupClientMoq = new Mock<IBinanceRawTradeLookupClient>(MockBehavior.Strict);
        var resolver = CreateResolver(lookupClientMoq);
        var capturedTimestamps = new List<DateTimeOffset>();

        lookupClientMoq
            .Setup(client => client.FindFirstTradeAt(It.IsAny<DateTimeOffset>(), instrument, It.IsAny<CancellationToken>()))
            .Returns((DateTimeOffset timestampUtc, Instrument _, CancellationToken _) =>
            {
                capturedTimestamps.Add(timestampUtc);
                var trade = capturedTimestamps.Count == 1
                    ? CreateTrade(101, "2026-04-10T00:00:00.001Z", instrument)
                    : CreateTrade(201, "2026-04-11T00:00:00.001Z", instrument);
                return new ValueTask<TradeInfo>(trade);
            });
        lookupClientMoq
            .Setup(client => client.TryVerifyRawTradeId(instrument, 200, It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<bool>(true));

        _ = await resolver.Resolve(BinanceHelper.Signature, instrument, utcDate, TradeDayBoundaryResolutionMode.Strict, CancellationToken.None);

        Assert.Equal(
            [
                new DateTimeOffset(2026, 4, 10, 0, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2026, 4, 11, 0, 0, 0, TimeSpan.Zero),
            ],
            capturedTimestamps);
    }

    [Fact]
    public async Task ResolveVerifiesExactRawTradeIdCandidate()
    {
        var instrument = Instrument.Parse("BTC-USDT");
        var utcDate = new DateOnly(2026, 4, 10);
        var lookupClientMoq = new Mock<IBinanceRawTradeLookupClient>(MockBehavior.Strict);
        var resolver = CreateResolver(lookupClientMoq);

        SetupCommonLookups(lookupClientMoq, instrument);
        lookupClientMoq
            .Setup(client => client.TryVerifyRawTradeId(instrument, 200, It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<bool>(true));

        _ = await resolver.Resolve(new ExchangeId("Binance"), instrument, utcDate, TradeDayBoundaryResolutionMode.Strict, CancellationToken.None);

        lookupClientMoq.Verify(client => client.TryVerifyRawTradeId(instrument, 200, It.IsAny<CancellationToken>()), Times.Once);
        lookupClientMoq.Verify(client => client.TryVerifyRawTradeId(instrument, 201, It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ResolveAcceptsExchangeNameCaseInsensitively()
    {
        var instrument = Instrument.Parse("BTC-USDT");
        var utcDate = new DateOnly(2026, 4, 10);
        var lookupClientMoq = new Mock<IBinanceRawTradeLookupClient>(MockBehavior.Strict);
        var resolver = CreateResolver(lookupClientMoq);

        SetupCommonLookups(lookupClientMoq, instrument);
        lookupClientMoq
            .Setup(client => client.TryVerifyRawTradeId(instrument, 200, It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<bool>(true));

        var result = await resolver.Resolve(new ExchangeId("binance"), instrument, utcDate, TradeDayBoundaryResolutionMode.Strict, CancellationToken.None);

        Assert.Equal(101, result.ExpectedFirstTradeId);
        Assert.Equal(200, result.ExpectedLastTradeId);
    }

    private static TradeDayBoundaryResolver CreateResolver(Mock<IBinanceRawTradeLookupClient> lookupClientMoq)
    {
        var log = new Mock<ILogMachina<TradeDayBoundaryResolver>>().Object;
        var result = new TradeDayBoundaryResolver(lookupClientMoq.Object, log);
        return result;
    }

    private static TradeInfo CreateTrade(long tradeId, string timestampUtc, Instrument instrument)
    {
        var result = new TradeInfo(
            new TradeKey(new ExchangeId("Binance"), instrument, tradeId.ToString()),
            DateTimeOffset.Parse(timestampUtc, null, System.Globalization.DateTimeStyles.AdjustToUniversal),
            100.0m,
            1.0m,
            buyerIsMaker: false);
        return result;
    }

    private static void SetupCommonLookups(Mock<IBinanceRawTradeLookupClient> lookupClientMoq, Instrument instrument)
    {
        lookupClientMoq
            .Setup(client => client.FindFirstTradeAt(
                new DateTimeOffset(2026, 4, 10, 0, 0, 0, TimeSpan.Zero), instrument,
                It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeInfo>(CreateTrade(101, "2026-04-10T00:00:00.001Z", instrument)));
        lookupClientMoq
            .Setup(client => client.FindFirstTradeAt(
                new DateTimeOffset(2026, 4, 11, 0, 0, 0, TimeSpan.Zero), instrument,
                It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeInfo>(CreateTrade(201, "2026-04-11T00:00:00.001Z", instrument)));
    }
}
