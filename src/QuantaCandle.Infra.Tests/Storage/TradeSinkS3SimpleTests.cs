using System.Reflection;
using LogMachina;

using Moq;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Tests.Storage;

public sealed class TradeSinkS3SimpleTests
{
    [Fact]
    public async Task DispatchUploadsOnlyTheSuppliedFinalizedFilePath()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var uploaderMoq = CreateUploaderMoq(out var uploads);
            var sink = new TradeSinkS3Simple(
                new TradeSinkS3SimpleOptions("my-bucket", "trades/raw", localRoot, TimeSpan.FromHours(1)),
                uploaderMoq.Object,
                CreateLogMoq().Object);
            var instrument = Instrument.Parse("BTC-USDT");
            var dispatchPath = TradeLocalDailyFilePath.Build(localRoot, instrument, new DateOnly(2026, 3, 11));
            var ignoredPath = TradeLocalDailyFilePath.Build(localRoot, instrument, new DateOnly(2026, 3, 10));

            await WriteTradeFileAsync(dispatchPath, [CreateTrade("123", new DateTimeOffset(2026, 3, 11, 23, 59, 59, TimeSpan.Zero), 10m)]);
            await WriteTradeFileAsync(ignoredPath, [CreateTrade("122", new DateTimeOffset(2026, 3, 10, 23, 59, 59, TimeSpan.Zero), 9m)]);

            await sink.DispatchAsync(instrument, new DateOnly(2026, 3, 11), dispatchPath, CancellationToken.None);

            var upload = Assert.Single(uploads);
            Assert.Equal("my-bucket", upload.BucketName);
            Assert.Equal("trades/raw/BTC-USDT/2026-03-11.jsonl", upload.ObjectKey);
            Assert.Contains("\"tradeId\":\"123\"", upload.Payload, StringComparison.Ordinal);
            Assert.DoesNotContain("\"tradeId\":\"122\"", upload.Payload, StringComparison.Ordinal);
            Assert.False(File.Exists(dispatchPath));
            Assert.True(File.Exists(ignoredPath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task DispatchDeletesLocalFileOnlyAfterSuccessfulUpload()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var uploadCompleted = false;
            var uploaderMoq = new Mock<IS3ObjectUploader>();
            uploaderMoq
                .Setup(mock => mock.UploadTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Callback(() => uploadCompleted = true)
                .Returns(Task.CompletedTask);

            var sink = new TradeSinkS3Simple(
                new TradeSinkS3SimpleOptions("my-bucket", "trades/raw", localRoot, TimeSpan.FromHours(1)),
                uploaderMoq.Object,
                CreateLogMoq().Object);
            var instrument = Instrument.Parse("BTC-USDT");
            var dispatchPath = TradeLocalDailyFilePath.Build(localRoot, instrument, new DateOnly(2026, 3, 11));

            await WriteTradeFileAsync(dispatchPath, [CreateTrade("123", new DateTimeOffset(2026, 3, 11, 23, 59, 59, TimeSpan.Zero), 10m)]);

            await sink.DispatchAsync(instrument, new DateOnly(2026, 3, 11), dispatchPath, CancellationToken.None);

            Assert.True(uploadCompleted);
            Assert.False(File.Exists(dispatchPath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task FailedDispatchKeepsLocalFile()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var uploaderMoq = new Mock<IS3ObjectUploader>();
            uploaderMoq
                .Setup(mock => mock.UploadTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("S3 unavailable"));

            var sink = new TradeSinkS3Simple(
                new TradeSinkS3SimpleOptions("my-bucket", "trades/raw", localRoot, TimeSpan.FromHours(1)),
                uploaderMoq.Object,
                CreateLogMoq().Object);
            var instrument = Instrument.Parse("BTC-USDT");
            var dispatchPath = TradeLocalDailyFilePath.Build(localRoot, instrument, new DateOnly(2026, 3, 11));

            await WriteTradeFileAsync(dispatchPath, [CreateTrade("123", new DateTimeOffset(2026, 3, 11, 23, 59, 59, TimeSpan.Zero), 10m)]);

            await Assert.ThrowsAsync<InvalidOperationException>(() => sink.DispatchAsync(instrument, new DateOnly(2026, 3, 11), dispatchPath, CancellationToken.None).AsTask());

            Assert.True(File.Exists(dispatchPath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public void TradeSinkS3SimpleDoesNotAcceptTradeBatchInputs()
    {
        var acceptsTradeBatch = typeof(TradeSinkS3Simple)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .SelectMany(method => method.GetParameters())
            .Any(parameter => parameter.ParameterType == typeof(IReadOnlyList<TradeInfo>));

        Assert.False(acceptsTradeBatch);
        Assert.True(typeof(ITradeFinalizedFileDispatcher).IsAssignableFrom(typeof(TradeSinkS3Simple)));
        Assert.False(typeof(ITradeSinkLifecycle).IsAssignableFrom(typeof(TradeSinkS3Simple)));
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

    private static TradeInfo CreateTrade(string tradeId, DateTimeOffset timestamp, decimal price)
    {
        var key = new TradeKey(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), tradeId);
        return new TradeInfo(key, timestamp, price, quantity: 0.5m);
    }

    private static async Task WriteTradeFileAsync(string path, IReadOnlyList<TradeInfo> trades)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await File.WriteAllTextAsync(path, TradeJsonlFile.BuildPayload(trades), CancellationToken.None);
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

    private sealed record UploadCall(string BucketName, string ObjectKey, string Payload);
}
