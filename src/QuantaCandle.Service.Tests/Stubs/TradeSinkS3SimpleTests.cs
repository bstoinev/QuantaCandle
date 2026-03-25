using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

using Moq;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;
using QuantaCandle.Service.Stubs;

namespace QuantaCandle.Service.Tests.Stubs;

public sealed class TradeSinkS3SimpleTests
{
    [Fact]
    public async Task AppendUploadsJsonlToPrefixedPartitionedKey()
    {
        var uploaderMoq = new Mock<IS3ObjectUploader>();
        var clockMoq = new Mock<IClock>();
        clockMoq
            .SetupGet(mock => mock.UtcNow)
            .Returns(new DateTimeOffset(2026, 3, 17, 12, 34, 56, 789, TimeSpan.Zero));

        var uploads = new List<UploadCall>();
        uploaderMoq
            .Setup(mock => mock.UploadTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<string, string, string, CancellationToken>((bucketName, objectKey, payload, _) => uploads.Add(new UploadCall(bucketName, objectKey, payload)))
            .Returns(Task.CompletedTask);

        var sink = new TradeSinkS3Simple(
            new TradeSinkS3SimpleOptions(BucketName: "my-bucket", Prefix: "/trades/raw/"),
            uploaderMoq.Object,
            clockMoq.Object);

        var trade = CreateTrade(tradeId: "123", timestamp: new DateTimeOffset(2026, 3, 12, 13, 37, 0, TimeSpan.Zero), price: 10m);

        var result = await sink.Append(new[] { trade }, CancellationToken.None);

        Assert.Equal(1, result.InsertedCount);
        Assert.Single(uploads);

        var upload = uploads[0];
        Assert.Equal("my-bucket", upload.BucketName);
        Assert.Matches(
            new Regex("^trades/raw/Stub/BTC-USDT/2026-03-12/20260317T123456789Z_133700000_133700000_1_[0-9a-f]{12}\\.jsonl$", RegexOptions.CultureInvariant),
            upload.ObjectKey);

        var line = upload.Payload.TrimEnd('\r', '\n');
        using var doc = JsonDocument.Parse(line);
        Assert.Equal("Stub", doc.RootElement.GetProperty("exchange").GetString());
        Assert.Equal("BTC-USDT", doc.RootElement.GetProperty("instrument").GetString());
        Assert.Equal("123", doc.RootElement.GetProperty("tradeId").GetString());
    }

    [Fact]
    public async Task AppendGeneratesDeterministicObjectKeyForSameInputAndClock()
    {
        var uploaderMoq = new Mock<IS3ObjectUploader>();
        var clockMoq = new Mock<IClock>();
        clockMoq
            .SetupGet(mock => mock.UtcNow)
            .Returns(new DateTimeOffset(2026, 3, 17, 12, 34, 56, 789, TimeSpan.Zero));

        var uploads = new List<UploadCall>();
        uploaderMoq
            .Setup(mock => mock.UploadTextAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<string, string, string, CancellationToken>((bucketName, objectKey, payload, _) => uploads.Add(new UploadCall(bucketName, objectKey, payload)))
            .Returns(Task.CompletedTask);

        var sink = new TradeSinkS3Simple(
            new TradeSinkS3SimpleOptions(BucketName: "my-bucket", Prefix: "trades/raw"),
            uploaderMoq.Object,
            clockMoq.Object);

        var earlyTrade = CreateTrade(tradeId: "1", timestamp: new DateTimeOffset(2026, 3, 12, 13, 37, 0, TimeSpan.Zero), price: 10m);
        var lateTrade = CreateTrade(tradeId: "2", timestamp: new DateTimeOffset(2026, 3, 12, 13, 37, 1, TimeSpan.Zero), price: 11m);

        await sink.Append(new[] { lateTrade, earlyTrade }, CancellationToken.None);
        await sink.Append(new[] { lateTrade, earlyTrade }, CancellationToken.None);

        Assert.Equal(2, uploads.Count);
        Assert.Equal(uploads[0].ObjectKey, uploads[1].ObjectKey);

        var firstPayload = uploads[0].Payload;
        var lines = firstPayload.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(2, lines.Length);

        using var firstLine = JsonDocument.Parse(lines[0]);
        using var secondLine = JsonDocument.Parse(lines[1]);
        Assert.Equal("1", firstLine.RootElement.GetProperty("tradeId").GetString());
        Assert.Equal("2", secondLine.RootElement.GetProperty("tradeId").GetString());
    }

    private static TradeInfo CreateTrade(string tradeId, DateTimeOffset timestamp, decimal price)
    {
        var key = new TradeKey(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), tradeId);
        return new TradeInfo(key, timestamp, price, quantity: 0.5m);
    }

    private sealed record UploadCall(string BucketName, string ObjectKey, string Payload);
}
