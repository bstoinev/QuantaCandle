using System.Text.Json;

using Moq;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Tests.Storage;

public sealed class TradeSinkS3SimpleTests
{
    [Fact]
    public async Task AppendUploadsFullDailyJsonlToSameKeyAcrossFlushes()
    {
        var uploaderMoq = new Mock<IS3ObjectUploader>();

        var uploads = new List<UploadCall>();
        uploaderMoq
            .Setup(mock => mock.UploadTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<string, string, string, CancellationToken>((bucketName, objectKey, payload, _) => uploads.Add(new UploadCall(bucketName, objectKey, payload)))
            .Returns(Task.CompletedTask);

        var sink = new TradeSinkS3Simple(
            new TradeSinkS3SimpleOptions(BucketName: "my-bucket", Prefix: "/trades/raw/"),
            uploaderMoq.Object);

        var firstTrade = CreateTrade(tradeId: "123", timestamp: new DateTimeOffset(2026, 3, 12, 13, 37, 0, TimeSpan.Zero), price: 10m);
        var secondTrade = CreateTrade(tradeId: "124", timestamp: new DateTimeOffset(2026, 3, 12, 13, 38, 0, TimeSpan.Zero), price: 11m);

        var firstResult = await sink.Append([firstTrade], CancellationToken.None);
        var secondResult = await sink.Append([secondTrade], CancellationToken.None);

        Assert.Equal(1, firstResult.InsertedCount);
        Assert.Equal(1, secondResult.InsertedCount);
        Assert.Equal(2, uploads.Count);

        Assert.Equal("my-bucket", uploads[0].BucketName);
        Assert.Equal("trades/raw/BTC-USDT/2026-03-12.jsonl", uploads[0].ObjectKey);
        Assert.Equal(uploads[0].ObjectKey, uploads[1].ObjectKey);

        var firstUploadTradeIds = ParseTradeIds(uploads[0].Payload);
        var secondUploadTradeIds = ParseTradeIds(uploads[1].Payload);

        Assert.Single(firstUploadTradeIds);
        Assert.Equal(2, secondUploadTradeIds.Count);
        Assert.Equal("123", firstUploadTradeIds[0]);
        Assert.Equal("123", secondUploadTradeIds[0]);
        Assert.Equal("124", secondUploadTradeIds[1]);
    }

    [Fact]
    public async Task AppendUsesDifferentKeysAcrossUtcDateRollover()
    {
        var uploaderMoq = new Mock<IS3ObjectUploader>();

        var uploads = new List<UploadCall>();
        uploaderMoq
            .Setup(mock => mock.UploadTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<string, string, string, CancellationToken>((bucketName, objectKey, payload, _) => uploads.Add(new UploadCall(bucketName, objectKey, payload)))
            .Returns(Task.CompletedTask);

        var sink = new TradeSinkS3Simple(
            new TradeSinkS3SimpleOptions(BucketName: "my-bucket", Prefix: "trades/raw"),
            uploaderMoq.Object);

        var march28Trade = CreateTrade(tradeId: "1", timestamp: new DateTimeOffset(2026, 3, 28, 23, 59, 59, TimeSpan.Zero), price: 10m);
        var march29Trade = CreateTrade(tradeId: "2", timestamp: new DateTimeOffset(2026, 3, 29, 0, 0, 0, TimeSpan.Zero), price: 11m);

        await sink.Append([march28Trade, march29Trade], CancellationToken.None);

        Assert.Equal(2, uploads.Count);
        Assert.Equal("trades/raw/BTC-USDT/2026-03-28.jsonl", uploads[0].ObjectKey);
        Assert.Equal("trades/raw/BTC-USDT/2026-03-29.jsonl", uploads[1].ObjectKey);
    }

    [Fact]
    public async Task AppendOmitsLeadingSlashWhenPrefixIsEmpty()
    {
        var uploaderMoq = new Mock<IS3ObjectUploader>();

        var uploads = new List<UploadCall>();
        uploaderMoq
            .Setup(mock => mock.UploadTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<string, string, string, CancellationToken>((bucketName, objectKey, payload, _) => uploads.Add(new UploadCall(bucketName, objectKey, payload)))
            .Returns(Task.CompletedTask);

        var sink = new TradeSinkS3Simple(
            new TradeSinkS3SimpleOptions(BucketName: "my-bucket", Prefix: string.Empty),
            uploaderMoq.Object);

        var trade = CreateTrade(tradeId: "123", timestamp: new DateTimeOffset(2026, 3, 12, 13, 37, 0, TimeSpan.Zero), price: 10m);

        await sink.Append([trade], CancellationToken.None);

        Assert.Single(uploads);
        Assert.Equal("BTC-USDT/2026-03-12.jsonl", uploads[0].ObjectKey);
    }

    private static TradeInfo CreateTrade(string tradeId, DateTimeOffset timestamp, decimal price)
    {
        var key = new TradeKey(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), tradeId);
        return new TradeInfo(key, timestamp, price, quantity: 0.5m);
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
