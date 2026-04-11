using Moq;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Tests.State;

public sealed class LocalFileIngestionStateStoreTests
{
    [Fact]
    public async Task StartupRecoveryReturnsExplicitBoundaryFromLatestLocalDailyFile()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var instrumentDirectory = Path.Combine(localRoot, "BTC-USDT");
            Directory.CreateDirectory(instrumentDirectory);

            await File.WriteAllTextAsync(
                Path.Combine(instrumentDirectory, "2026-03-11.jsonl"),
                TradeJsonlFile.BuildPayload([CreateTrade("10", new DateTimeOffset(2026, 3, 11, 23, 59, 0, TimeSpan.Zero))]),
                CancellationToken.None);

            await File.WriteAllTextAsync(
                Path.Combine(instrumentDirectory, "2026-03-12.jsonl"),
                TradeJsonlFile.BuildPayload([CreateTrade("20", new DateTimeOffset(2026, 3, 12, 9, 30, 0, TimeSpan.Zero))]),
                CancellationToken.None);

            var utcNow = new DateTimeOffset(2026, 3, 13, 8, 0, 0, TimeSpan.Zero);
            var clockMoq = CreateClockMoq(() => utcNow);
            var store = new LocalFileIngestionStateStore(localRoot, clockMoq.Object);

            var resumeBoundary = await store.GetResumeBoundaryAsync(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), CancellationToken.None);

            Assert.NotNull(resumeBoundary);
            Assert.Equal("LatestLocalDailyFile", resumeBoundary.Value.Origin);
            Assert.Equal(new DateOnly(2026, 3, 12), resumeBoundary.Value.UtcDate);
            Assert.Equal(new DateTimeOffset(2026, 3, 12, 9, 30, 0, TimeSpan.Zero), resumeBoundary.Value.TimestampUtc);
            Assert.Null(await store.GetWatermarkAsync(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), CancellationToken.None));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task StartupRecoveryPrefersNewestTradeFromScratchFile()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var instrumentDirectory = Path.Combine(localRoot, "BTC-USDT");
            Directory.CreateDirectory(instrumentDirectory);

            await File.WriteAllTextAsync(
                Path.Combine(instrumentDirectory, "2026-03-12.jsonl"),
                TradeJsonlFile.BuildPayload([CreateTrade("20", new DateTimeOffset(2026, 3, 12, 9, 30, 0, TimeSpan.Zero))]),
                CancellationToken.None);

            await File.WriteAllTextAsync(
                Path.Combine(instrumentDirectory, "qc-scratch.jsonl"),
                TradeJsonlFile.BuildPayload([CreateTrade("21", new DateTimeOffset(2026, 3, 12, 9, 45, 0, TimeSpan.Zero))]),
                CancellationToken.None);

            var utcNow = new DateTimeOffset(2026, 3, 13, 8, 0, 0, TimeSpan.Zero);
            var clockMoq = CreateClockMoq(() => utcNow);
            var store = new LocalFileIngestionStateStore(localRoot, clockMoq.Object);

            var resumeBoundary = await store.GetResumeBoundaryAsync(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), CancellationToken.None);

            Assert.NotNull(resumeBoundary);
            Assert.Equal("LatestScratchFile", resumeBoundary.Value.Origin);
            Assert.Equal(new DateOnly(2026, 3, 12), resumeBoundary.Value.UtcDate);
            Assert.Equal(new DateTimeOffset(2026, 3, 12, 9, 45, 0, TimeSpan.Zero), resumeBoundary.Value.TimestampUtc);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task StartupRecoveryUsesLatestTradeAcrossStreamingDailyAndScratchFiles()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var instrumentDirectory = Path.Combine(localRoot, "BTC-USDT");
            Directory.CreateDirectory(instrumentDirectory);

            await File.WriteAllTextAsync(
                Path.Combine(instrumentDirectory, "2026-03-12.jsonl"),
                TradeJsonlFile.BuildPayload(
                [
                    CreateTrade("20", new DateTimeOffset(2026, 3, 12, 9, 30, 0, TimeSpan.Zero)),
                    CreateTrade("21", new DateTimeOffset(2026, 3, 12, 9, 45, 0, TimeSpan.Zero)),
                ]),
                CancellationToken.None);

            await File.WriteAllTextAsync(
                Path.Combine(instrumentDirectory, "qc-scratch.jsonl"),
                TradeJsonlFile.BuildPayload(
                [
                    CreateTrade("22", new DateTimeOffset(2026, 3, 12, 10, 0, 0, TimeSpan.Zero)),
                    CreateTrade("23", new DateTimeOffset(2026, 3, 12, 10, 15, 0, TimeSpan.Zero)),
                ]),
                CancellationToken.None);

            var utcNow = new DateTimeOffset(2026, 3, 13, 8, 0, 0, TimeSpan.Zero);
            var clockMoq = CreateClockMoq(() => utcNow);
            var store = new LocalFileIngestionStateStore(localRoot, clockMoq.Object);

            var resumeBoundary = await store.GetResumeBoundaryAsync(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), CancellationToken.None);

            Assert.NotNull(resumeBoundary);
            Assert.Equal("LatestScratchFile", resumeBoundary.Value.Origin);
            Assert.Equal(new DateOnly(2026, 3, 12), resumeBoundary.Value.UtcDate);
            Assert.Equal(new DateTimeOffset(2026, 3, 12, 10, 15, 0, TimeSpan.Zero), resumeBoundary.Value.TimestampUtc);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task StartupRecoveryFallsBackToExplicitCurrentDayUtcMidnightBoundaryWhenNoLocalFileExists()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var utcNow = new DateTimeOffset(2026, 3, 12, 8, 45, 0, TimeSpan.Zero);
            var clockMoq = CreateClockMoq(() => utcNow);
            var store = new LocalFileIngestionStateStore(localRoot, clockMoq.Object);

            var resumeBoundary = await store.GetResumeBoundaryAsync(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), CancellationToken.None);

            Assert.NotNull(resumeBoundary);
            Assert.Equal("CurrentDayUtcStartFallback", resumeBoundary.Value.Origin);
            Assert.Equal(new DateOnly(2026, 3, 12), resumeBoundary.Value.UtcDate);
            Assert.Equal(new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero), resumeBoundary.Value.TimestampUtc);
            Assert.Null(await store.GetWatermarkAsync(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), CancellationToken.None));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    private static TradeInfo CreateTrade(string tradeId, DateTimeOffset timestamp)
    {
        var key = new TradeKey(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), tradeId);
        return new TradeInfo(key, timestamp, price: 1m, quantity: 1m);
    }

    private static Mock<IClock> CreateClockMoq(Func<DateTimeOffset> utcNowProvider)
    {
        var clockMoq = new Mock<IClock>();
        clockMoq
            .SetupGet(mock => mock.UtcNow)
            .Returns(utcNowProvider);

        return clockMoq;
    }

    private static string CreateTempDirectory()
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
}
