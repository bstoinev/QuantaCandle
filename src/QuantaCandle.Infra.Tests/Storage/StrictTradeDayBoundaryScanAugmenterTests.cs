using System.Globalization;
using System.Text.Json;

using LogMachina;

using Moq;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Storage;

namespace QuantaCandle.Infra.Tests.Storage;

/// <summary>
/// Verifies strict UTC day-boundary scan augmentation for local daily raw-trade files.
/// </summary>
public sealed class StrictTradeDayBoundaryScanAugmenterTests
{
    [Fact]
    public async Task ReportsStartBoundaryGapWhenFirstLocalTradeIdIsAfterExpectedFirstTradeId()
    {
        var root = CreateTempRoot();
        var tradingDay = new DateOnly(2026, 3, 12);
        var request = CreateRequest(tradingDay);

        try
        {
            await WriteTradeFile(
                root,
                tradingDay,
                Trade("binance", "BTC-USDT", "102", "2026-03-12T00:00:02Z"),
                Trade("binance", "BTC-USDT", "103", "2026-03-12T00:00:03Z"));

            var resolverMoq = CreateResolverMock();
            resolverMoq
                .Setup(mock => mock.Resolve(new ExchangeId("binance"), Instrument.Parse("BTC-USDT"), tradingDay, TradeDayBoundaryResolutionMode.Strict, It.IsAny<CancellationToken>()))
                .Returns(new ValueTask<TradeDayBoundary>(new TradeDayBoundary(new ExchangeId("binance"), Instrument.Parse("BTC-USDT"), tradingDay, 100, 103, null)));
            var sut = CreateSut(resolverMoq);

            var result = await ScanWithBoundaryAugmentation(root, request, sut);

            var gap = Assert.Single(result.DetectedGaps);
            Assert.NotNull(gap.MissingTradeIds);
            Assert.Equal(100, gap.MissingTradeIds.Value.FirstTradeId);
            Assert.Equal(101, gap.MissingTradeIds.Value.LastTradeId);

            var range = Assert.Single(result.AffectedRanges);
            Assert.Equal(Path.Combine("Binance", "BTC-USDT", "2026-03-12.jsonl"), range.FromLocation!.FilePath);
            Assert.Equal(1, range.FromLocation.LineNumber);
            Assert.Equal(Path.Combine("Binance", "BTC-USDT", "2026-03-12.jsonl"), range.ToLocation!.FilePath);
            Assert.Equal(1, range.ToLocation.LineNumber);
        }
        finally
        {
            DeleteDirectory(root);
        }
    }

    [Fact]
    public async Task ReportsEndBoundaryGapWhenLastLocalTradeIdIsBeforeExpectedLastTradeId()
    {
        var root = CreateTempRoot();
        var tradingDay = new DateOnly(2026, 3, 12);
        var request = CreateRequest(tradingDay);

        try
        {
            await WriteTradeFile(
                root,
                tradingDay,
                Trade("binance", "BTC-USDT", "100", "2026-03-12T00:00:02Z"),
                Trade("binance", "BTC-USDT", "101", "2026-03-12T00:00:03Z"));

            var resolverMoq = CreateResolverMock();
            resolverMoq
                .Setup(mock => mock.Resolve(new ExchangeId("binance"), Instrument.Parse("BTC-USDT"), tradingDay, TradeDayBoundaryResolutionMode.Strict, It.IsAny<CancellationToken>()))
                .Returns(new ValueTask<TradeDayBoundary>(new TradeDayBoundary(new ExchangeId("binance"), Instrument.Parse("BTC-USDT"), tradingDay, 100, 103, null)));
            var sut = CreateSut(resolverMoq);

            var result = await ScanWithBoundaryAugmentation(root, request, sut);

            var gap = Assert.Single(result.DetectedGaps);
            Assert.NotNull(gap.MissingTradeIds);
            Assert.Equal(102, gap.MissingTradeIds.Value.FirstTradeId);
            Assert.Equal(103, gap.MissingTradeIds.Value.LastTradeId);

            var range = Assert.Single(result.AffectedRanges);
            Assert.Equal(Path.Combine("Binance", "BTC-USDT", "2026-03-12.jsonl"), range.FromLocation!.FilePath);
            Assert.Equal(2, range.FromLocation.LineNumber);
            Assert.Equal(Path.Combine("Binance", "BTC-USDT", "2026-03-12.jsonl"), range.ToLocation!.FilePath);
            Assert.Equal(2, range.ToLocation.LineNumber);
        }
        finally
        {
            DeleteDirectory(root);
        }
    }

    [Fact]
    public async Task ReportsBothBoundaryGapsWhenBothSidesAreMissing()
    {
        var root = CreateTempRoot();
        var tradingDay = new DateOnly(2026, 3, 12);
        var request = CreateRequest(tradingDay);

        try
        {
            await WriteTradeFile(
                root,
                tradingDay,
                Trade("binance", "BTC-USDT", "102", "2026-03-12T00:00:02Z"),
                Trade("binance", "BTC-USDT", "103", "2026-03-12T00:00:03Z"));

            var resolverMoq = CreateResolverMock();
            resolverMoq
                .Setup(mock => mock.Resolve(new ExchangeId("binance"), Instrument.Parse("BTC-USDT"), tradingDay, TradeDayBoundaryResolutionMode.Strict, It.IsAny<CancellationToken>()))
                .Returns(new ValueTask<TradeDayBoundary>(new TradeDayBoundary(new ExchangeId("binance"), Instrument.Parse("BTC-USDT"), tradingDay, 100, 105, null)));
            var sut = CreateSut(resolverMoq);

            var result = await ScanWithBoundaryAugmentation(root, request, sut);

            Assert.Equal(2, result.DetectedGaps.Count);
            Assert.Equal((100, 101), ExtractMissingRange(result.DetectedGaps[0]));
            Assert.Equal((104, 105), ExtractMissingRange(result.DetectedGaps[1]));
        }
        finally
        {
            DeleteDirectory(root);
        }
    }

    [Fact]
    public async Task DoesNotReportBoundaryGapsWhenLocalFirstAndLastMatchResolvedBoundaries()
    {
        var root = CreateTempRoot();
        var tradingDay = new DateOnly(2026, 3, 12);
        var request = CreateRequest(tradingDay);

        try
        {
            await WriteTradeFile(
                root,
                tradingDay,
                Trade("binance", "BTC-USDT", "100", "2026-03-12T00:00:02Z"),
                Trade("binance", "BTC-USDT", "101", "2026-03-12T00:00:03Z"),
                Trade("binance", "BTC-USDT", "102", "2026-03-12T00:00:04Z"));

            var resolverMoq = CreateResolverMock();
            resolverMoq
                .Setup(mock => mock.Resolve(new ExchangeId("binance"), Instrument.Parse("BTC-USDT"), tradingDay, TradeDayBoundaryResolutionMode.Strict, It.IsAny<CancellationToken>()))
                .Returns(new ValueTask<TradeDayBoundary>(new TradeDayBoundary(new ExchangeId("binance"), Instrument.Parse("BTC-USDT"), tradingDay, 100, 102, null)));
            var sut = CreateSut(resolverMoq);

            var result = await ScanWithBoundaryAugmentation(root, request, sut);

            Assert.Empty(result.DetectedGaps);
            Assert.Empty(result.AffectedRanges);
        }
        finally
        {
            DeleteDirectory(root);
        }
    }

    [Fact]
    public async Task PreservesInteriorGapDetectionAlongsideBoundaryGapReporting()
    {
        var root = CreateTempRoot();
        var tradingDay = new DateOnly(2026, 3, 12);
        var request = CreateRequest(tradingDay);

        try
        {
            await WriteTradeFile(
                root,
                tradingDay,
                Trade("binance", "BTC-USDT", "102", "2026-03-12T00:00:02Z"),
                Trade("binance", "BTC-USDT", "104", "2026-03-12T00:00:04Z"),
                Trade("binance", "BTC-USDT", "106", "2026-03-12T00:00:06Z"));

            var resolverMoq = CreateResolverMock();
            resolverMoq
                .Setup(mock => mock.Resolve(new ExchangeId("binance"), Instrument.Parse("BTC-USDT"), tradingDay, TradeDayBoundaryResolutionMode.Strict, It.IsAny<CancellationToken>()))
                .Returns(new ValueTask<TradeDayBoundary>(new TradeDayBoundary(new ExchangeId("binance"), Instrument.Parse("BTC-USDT"), tradingDay, 100, 107, null)));
            var sut = CreateSut(resolverMoq);

            var result = await ScanWithBoundaryAugmentation(root, request, sut);

            Assert.Equal(4, result.DetectedGaps.Count);
            Assert.Equal((103, 103), ExtractMissingRange(result.DetectedGaps[0]));
            Assert.Equal((105, 105), ExtractMissingRange(result.DetectedGaps[1]));
            Assert.Equal((100, 101), ExtractMissingRange(result.DetectedGaps[2]));
            Assert.Equal((107, 107), ExtractMissingRange(result.DetectedGaps[3]));
        }
        finally
        {
            DeleteDirectory(root);
        }
    }

    [Fact]
    public async Task PropagatesStrictBoundaryResolutionFailure()
    {
        var root = CreateTempRoot();
        var tradingDay = new DateOnly(2026, 3, 12);
        var request = CreateRequest(tradingDay);

        try
        {
            await WriteTradeFile(
                root,
                tradingDay,
                Trade("binance", "BTC-USDT", "100", "2026-03-12T00:00:02Z"));

            var resolverMoq = CreateResolverMock();
            resolverMoq
                .Setup(mock => mock.Resolve(new ExchangeId("binance"), Instrument.Parse("BTC-USDT"), tradingDay, TradeDayBoundaryResolutionMode.Strict, It.IsAny<CancellationToken>()))
                .ThrowsAsync(new TradeDayBoundaryVerificationException(new ExchangeId("binance"), Instrument.Parse("BTC-USDT"), tradingDay, 101));
            var sut = CreateSut(resolverMoq);

            await Assert.ThrowsAsync<TradeDayBoundaryVerificationException>(() => ScanWithBoundaryAugmentation(root, request, sut));
        }
        finally
        {
            DeleteDirectory(root);
        }
    }

    [Fact]
    public async Task EmptyScannedFileFailsClearly()
    {
        var root = CreateTempRoot();
        var tradingDay = new DateOnly(2026, 3, 12);
        var request = CreateRequest(tradingDay);

        try
        {
            var filePath = Path.Combine(root, "Binance", "BTC-USDT");
            Directory.CreateDirectory(filePath);
            await File.WriteAllTextAsync(Path.Combine(filePath, "2026-03-12.jsonl"), string.Empty, CancellationToken.None);

            var resolverMoq = CreateResolverMock();
            var sut = CreateSut(resolverMoq);

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => ScanWithBoundaryAugmentation(root, request, sut));

            Assert.Contains("is empty", exception.Message, StringComparison.Ordinal);
            resolverMoq.Verify(
                mock => mock.Resolve(It.IsAny<ExchangeId>(), It.IsAny<Instrument>(), It.IsAny<DateOnly>(), It.IsAny<TradeDayBoundaryResolutionMode>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }
        finally
        {
            DeleteDirectory(root);
        }
    }

    private static StrictTradeDayBoundaryScanAugmenter CreateSut(Mock<ITradeDayBoundaryResolver> resolverMoq)
    {
        var result = new StrictTradeDayBoundaryScanAugmenter(resolverMoq.Object, new Mock<ILogMachina<StrictTradeDayBoundaryScanAugmenter>>().Object);
        return result;
    }

    private static TradeGapScanRequest CreateRequest(DateOnly tradingDay)
    {
        var result = new TradeGapScanRequest(
            ".",
            [
                new TradeGapAffectedFile(Path.Combine("Binance", "BTC-USDT", tradingDay.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + ".jsonl"), tradingDay),
            ],
            []);
        return result;
    }

    private static Mock<ITradeDayBoundaryResolver> CreateResolverMock()
    {
        var result = new Mock<ITradeDayBoundaryResolver>(MockBehavior.Strict);
        return result;
    }

    private static string CreateTempRoot()
    {
        var result = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(result);
        return result;
    }

    private static void DeleteDirectory(string path)
    {
        if (Directory.Exists(path))
        {
            Directory.Delete(path, recursive: true);
        }
    }

    private static (long FirstTradeId, long LastTradeId) ExtractMissingRange(TradeGap gap)
    {
        Assert.NotNull(gap.MissingTradeIds);
        return (gap.MissingTradeIds.Value.FirstTradeId, gap.MissingTradeIds.Value.LastTradeId);
    }

    private static async Task<TradeGapScanResult> ScanWithBoundaryAugmentation(
        string root,
        TradeGapScanRequest request,
        StrictTradeDayBoundaryScanAugmenter sut)
    {
        var scanRequest = new TradeGapScanRequest(root, request.CandidateFiles, request.CandidateRanges);
        var interiorResult = await new LocalFileTradeGapScanner().Scan(scanRequest, CancellationToken.None);
        var result = await sut.Augment(scanRequest, interiorResult, CancellationToken.None);
        return result;
    }

    private static object Trade(string exchange, string instrument, string tradeId, string timestamp)
    {
        var result = new
        {
            exchange,
            instrument,
            tradeId,
            timestamp = DateTimeOffset.Parse(timestamp, CultureInfo.InvariantCulture),
            price = 100m,
            quantity = 1m,
            isBuyerMaker = false,
        };
        return result;
    }

    private static async Task WriteTradeFile(string root, DateOnly tradingDay, params object[] trades)
    {
        var directory = Path.Combine(root, "Binance", "BTC-USDT");
        Directory.CreateDirectory(directory);

        var path = Path.Combine(directory, tradingDay.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + ".jsonl");
        var lines = trades.Select(static trade => JsonSerializer.Serialize(trade)).ToArray();
        var payload = string.Join(Environment.NewLine, lines) + Environment.NewLine;

        await File.WriteAllTextAsync(path, payload, CancellationToken.None);
    }
}
