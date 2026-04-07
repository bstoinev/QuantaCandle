using System.Text.Json;

using LogMachina;
using Moq;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Tests.Storage;

public sealed class TradeSinkS3SimpleTests
{
    [Fact]
    public async Task AppendWritesToDeterministicLocalDailyFile()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var uploaderMoq = CreateUploaderMoq(out var uploads);
            var clockMoq = CreateClockMoq(() => new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero));
            var sink = new TradeSinkS3Simple(
                new TradeSinkS3SimpleOptions("my-bucket", "trades/raw", localRoot, TimeSpan.FromHours(1)),
                uploaderMoq.Object,
                clockMoq.Object,
                CreateLogMoq().Object);

            await sink.Append([CreateTrade("123", new DateTimeOffset(2026, 3, 12, 0, 15, 0, TimeSpan.Zero), 10m)], CancellationToken.None);

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
    public async Task AppendUploadsCompletedLocalDayFileWithoutOwningCheckpointLifecycle()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var uploaderMoq = CreateUploaderMoq(out var uploads);
            var clockMoq = CreateClockMoq(() => new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero));
            var sink = new TradeSinkS3Simple(
                new TradeSinkS3SimpleOptions("my-bucket", "trades/raw", localRoot, TimeSpan.FromHours(1)),
                uploaderMoq.Object,
                clockMoq.Object,
                CreateLogMoq().Object);

            await sink.Append([CreateTrade("123", new DateTimeOffset(2026, 3, 11, 23, 59, 59, TimeSpan.Zero), 10m)], CancellationToken.None);

            var upload = Assert.Single(uploads);
            Assert.Equal("trades/raw/BTC-USDT/2026-03-11.jsonl", upload.ObjectKey);
            Assert.False(typeof(ITradeSinkLifecycle).IsAssignableFrom(typeof(TradeSinkS3Simple)));
            Assert.False(File.Exists(Path.Combine(localRoot, "BTC-USDT", "2026-03-11.jsonl")));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task FailedUploadKeepsCompletedLocalFile()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var uploaderMoq = new Mock<IS3ObjectUploader>();
            uploaderMoq
                .Setup(mock => mock.UploadTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("S3 unavailable"));

            var clockMoq = CreateClockMoq(() => new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero));
            var sink = new TradeSinkS3Simple(
                new TradeSinkS3SimpleOptions("my-bucket", "trades/raw", localRoot, TimeSpan.FromHours(1)),
                uploaderMoq.Object,
                clockMoq.Object,
                CreateLogMoq().Object);

            await Assert.ThrowsAsync<InvalidOperationException>(() => sink.Append([CreateTrade("123", new DateTimeOffset(2026, 3, 11, 23, 59, 59, TimeSpan.Zero), 10m)], CancellationToken.None).AsTask());

            var localPath = Path.Combine(localRoot, "BTC-USDT", "2026-03-11.jsonl");
            Assert.True(File.Exists(localPath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task AppendIgnoresScratchFileWhenUploadingCompletedFiles()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var uploaderMoq = CreateUploaderMoq(out var uploads);
            var clockMoq = CreateClockMoq(() => new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero));
            var sink = new TradeSinkS3Simple(
                new TradeSinkS3SimpleOptions("my-bucket", "trades/raw", localRoot, TimeSpan.FromHours(1)),
                uploaderMoq.Object,
                clockMoq.Object,
                CreateLogMoq().Object);

            var instrument = Instrument.Parse("BTC-USDT");
            var instrumentDirectory = Path.Combine(localRoot, instrument.ToString());
            Directory.CreateDirectory(instrumentDirectory);

            await File.WriteAllTextAsync(
                Path.Combine(instrumentDirectory, "qc-scratch.jsonl"),
                TradeJsonlFile.BuildPayload([CreateTrade("999", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero), 10m)]),
                CancellationToken.None);

            await sink.Append([CreateTrade("123", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero), 10m)], CancellationToken.None);

            var upload = Assert.Single(uploads);
            Assert.Equal("trades/raw/BTC-USDT/2026-03-12.jsonl", upload.ObjectKey);
            Assert.Contains("\"tradeId\":\"123\"", upload.Payload, StringComparison.Ordinal);
            Assert.DoesNotContain("\"tradeId\":\"999\"", upload.Payload, StringComparison.Ordinal);
            Assert.True(File.Exists(Path.Combine(instrumentDirectory, "qc-scratch.jsonl")));
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

    private static Mock<ILogMachina<TradeSinkS3Simple>> CreateLogMoq()
    {
        var result = new Mock<ILogMachina<TradeSinkS3Simple>>();
        return result;
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
