using System.Text.Json;

using Moq;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Tests.Storage;

public sealed class TradeSinkS3SimpleTests
{
    private const string StartupBoundaryTradeId = "_startup-day-boundary";

    [Fact]
    public async Task PeriodicCheckpointWritesFullAccumulatedJsonlToLocalDailyFile()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var uploaderMoq = CreateUploaderMoq(out var uploads);
            var utcNow = new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero);
            var clockMoq = CreateClockMoq(() => utcNow);
            var sink = new TradeSinkS3Simple(
                new TradeSinkS3SimpleOptions("my-bucket", "trades/raw", localRoot, TimeSpan.FromHours(1)),
                uploaderMoq.Object,
                clockMoq.Object);

            await sink.Append([CreateTrade("123", new DateTimeOffset(2026, 3, 12, 0, 15, 0, TimeSpan.Zero), 10m)], CancellationToken.None);

            utcNow = new DateTimeOffset(2026, 3, 12, 1, 0, 0, TimeSpan.Zero);
            await sink.CheckpointActive(CancellationToken.None);

            await sink.Append([CreateTrade("124", new DateTimeOffset(2026, 3, 12, 1, 15, 0, TimeSpan.Zero), 11m)], CancellationToken.None);

            utcNow = new DateTimeOffset(2026, 3, 12, 2, 0, 0, TimeSpan.Zero);
            await sink.CheckpointActive(CancellationToken.None);

            var localPath = Path.Combine(localRoot, "BTC-USDT", "2026-03-12.jsonl");
            Assert.True(File.Exists(localPath));
            Assert.Empty(uploads);

            var tradeIds = ParseTradeIds(await File.ReadAllTextAsync(localPath, CancellationToken.None));
            Assert.Equal(["123", "124"], tradeIds);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task UtcRolloverWritesCompletedDayLocallyUploadsToS3AndRemovesItFromActiveMemory()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var uploaderMoq = CreateUploaderMoq(out var uploads);
            var utcNow = new DateTimeOffset(2026, 3, 28, 23, 59, 0, TimeSpan.Zero);
            var clockMoq = CreateClockMoq(() => utcNow);
            var sink = new TradeSinkS3Simple(
                new TradeSinkS3SimpleOptions("my-bucket", "trades/raw", localRoot, TimeSpan.FromHours(1)),
                uploaderMoq.Object,
                clockMoq.Object);

            await sink.Append(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 28, 23, 59, 59, TimeSpan.Zero), 10m),
                CreateTrade("2", new DateTimeOffset(2026, 3, 29, 0, 0, 0, TimeSpan.Zero), 11m),
            ], CancellationToken.None);

            utcNow = new DateTimeOffset(2026, 3, 29, 1, 0, 0, TimeSpan.Zero);
            await sink.CheckpointActive(CancellationToken.None);

            Assert.Single(uploads);
            Assert.Equal("trades/raw/BTC-USDT/2026-03-28.jsonl", uploads[0].ObjectKey);
            Assert.True(File.Exists(Path.Combine(localRoot, "BTC-USDT", "2026-03-28.jsonl")));
            Assert.True(File.Exists(Path.Combine(localRoot, "BTC-USDT", "2026-03-29.jsonl")));

            utcNow = new DateTimeOffset(2026, 3, 29, 2, 0, 0, TimeSpan.Zero);
            await sink.CheckpointActive(CancellationToken.None);

            Assert.Single(uploads);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task ShutdownFlushWritesActiveInMemoryDataToLocalDiskWithoutUploadingIncompleteCurrentDay()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var uploaderMoq = CreateUploaderMoq(out var uploads);
            var utcNow = new DateTimeOffset(2026, 3, 12, 10, 0, 0, TimeSpan.Zero);
            var clockMoq = CreateClockMoq(() => utcNow);
            var sink = new TradeSinkS3Simple(
                new TradeSinkS3SimpleOptions("my-bucket", "trades/raw", localRoot, TimeSpan.FromHours(1)),
                uploaderMoq.Object,
                clockMoq.Object);

            await sink.Append([CreateTrade("123", new DateTimeOffset(2026, 3, 12, 10, 15, 0, TimeSpan.Zero), 10m)], CancellationToken.None);

            await sink.FlushOnShutdown(CancellationToken.None);

            var localPath = Path.Combine(localRoot, "BTC-USDT", "2026-03-12.jsonl");
            Assert.True(File.Exists(localPath));
            Assert.Empty(uploads);

            var tradeIds = ParseTradeIds(await File.ReadAllTextAsync(localPath, CancellationToken.None));
            Assert.Equal(["123"], tradeIds);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task UploadUsesDeterministicDailyObjectKeyWithoutLeadingSlash()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var uploaderMoq = CreateUploaderMoq(out var uploads);
            var utcNow = new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero);
            var clockMoq = CreateClockMoq(() => utcNow);
            var sink = new TradeSinkS3Simple(
                new TradeSinkS3SimpleOptions("my-bucket", string.Empty, localRoot, TimeSpan.FromHours(1)),
                uploaderMoq.Object,
                clockMoq.Object);

            await sink.Append([CreateTrade("123", new DateTimeOffset(2026, 3, 11, 23, 59, 59, TimeSpan.Zero), 10m)], CancellationToken.None);

            utcNow = new DateTimeOffset(2026, 3, 12, 1, 0, 0, TimeSpan.Zero);
            await sink.CheckpointActive(CancellationToken.None);

            Assert.Single(uploads);
            Assert.Equal("BTC-USDT/2026-03-11.jsonl", uploads[0].ObjectKey);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task UploadedPayloadContainsOnlyRealTrades()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var uploaderMoq = CreateUploaderMoq(out var uploads);
            var utcNow = new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero);
            var clockMoq = CreateClockMoq(() => utcNow);
            var sink = new TradeSinkS3Simple(
                new TradeSinkS3SimpleOptions("my-bucket", "trades/raw", localRoot, TimeSpan.FromHours(1)),
                uploaderMoq.Object,
                clockMoq.Object);

            await sink.Append([CreateTrade("123", new DateTimeOffset(2026, 3, 11, 23, 59, 59, TimeSpan.Zero), 10m)], CancellationToken.None);

            utcNow = new DateTimeOffset(2026, 3, 12, 1, 0, 0, TimeSpan.Zero);
            await sink.CheckpointActive(CancellationToken.None);

            var upload = Assert.Single(uploads);
            Assert.DoesNotContain(StartupBoundaryTradeId, upload.Payload, StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    private static Mock<IS3ObjectUploader> CreateUploaderMoq(out List<UploadCall> uploads)
    {
        var capturedUploads = new List<UploadCall>();
        var uploaderMoq = new Mock<IS3ObjectUploader>();
        uploaderMoq
            .Setup(mock => mock.UploadTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<string, string, string, CancellationToken>((bucketName, objectKey, payload, _) => capturedUploads.Add(new UploadCall(bucketName, objectKey, payload)))
            .Returns(Task.CompletedTask);

        uploads = capturedUploads;
        return uploaderMoq;
    }

    private static Mock<IClock> CreateClockMoq(Func<DateTimeOffset> utcNowProvider)
    {
        var clockMoq = new Mock<IClock>();
        clockMoq
            .SetupGet(mock => mock.UtcNow)
            .Returns(utcNowProvider);

        return clockMoq;
    }

    private static TradeInfo CreateTrade(string tradeId, DateTimeOffset timestamp, decimal price)
    {
        var key = new TradeKey(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), tradeId);
        return new TradeInfo(key, timestamp, price, quantity: 0.5m);
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

    private static List<string?> ParseTradeIds(string payload)
    {
        var result = new List<string?>();
        var lines = payload.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

        foreach (var line in lines)
        {
            using var document = JsonDocument.Parse(line);
            result.Add(document.RootElement.GetProperty("tradeId").GetString());
        }

        return result;
    }

    private sealed record UploadCall(string BucketName, string ObjectKey, string Payload);
}
