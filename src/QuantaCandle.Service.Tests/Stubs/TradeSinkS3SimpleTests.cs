using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using QuantaCandle.Core;
using QuantaCandle.Core.Trading;
using QuantaCandle.Service.Stubs;

namespace QuantaCandle.Service.Tests.Stubs;

public sealed class TradeSinkS3SimpleTests
{
    [Fact]
    public async Task AppendAsync_uploads_jsonl_to_prefixed_partitioned_key()
    {
        RecordingS3Uploader uploader = new RecordingS3Uploader();
        FixedClock clock = new FixedClock(new DateTimeOffset(2026, 3, 17, 12, 34, 56, 789, TimeSpan.Zero));
        TradeSinkS3Simple sink = new TradeSinkS3Simple(
            new TradeSinkS3SimpleOptions(BucketName: "my-bucket", Prefix: "/trades/raw/"),
            uploader,
            clock);

        TradeInfo trade = CreateTrade(tradeId: "123", timestamp: new DateTimeOffset(2026, 3, 12, 13, 37, 0, TimeSpan.Zero), price: 10m);

        TradeAppendResult result = await sink.Append(new[] { trade }, CancellationToken.None);

        Assert.Equal(1, result.InsertedCount);
        Assert.Single(uploader.Uploads);

        UploadCall upload = uploader.Uploads[0];
        Assert.Equal("my-bucket", upload.BucketName);
        Assert.Matches(
            new Regex("^trades/raw/Stub/BTC-USDT/2026-03-12/20260317T123456789Z_133700000_133700000_1_[0-9a-f]{12}\\.jsonl$", RegexOptions.CultureInvariant),
            upload.ObjectKey);

        string line = upload.Payload.TrimEnd('\r', '\n');
        using JsonDocument doc = JsonDocument.Parse(line);
        Assert.Equal("Stub", doc.RootElement.GetProperty("exchange").GetString());
        Assert.Equal("BTC-USDT", doc.RootElement.GetProperty("instrument").GetString());
        Assert.Equal("123", doc.RootElement.GetProperty("tradeId").GetString());
    }

    [Fact]
    public async Task AppendAsync_generates_deterministic_object_key_for_same_input_and_clock()
    {
        RecordingS3Uploader uploader = new RecordingS3Uploader();
        FixedClock clock = new FixedClock(new DateTimeOffset(2026, 3, 17, 12, 34, 56, 789, TimeSpan.Zero));
        TradeSinkS3Simple sink = new TradeSinkS3Simple(
            new TradeSinkS3SimpleOptions(BucketName: "my-bucket", Prefix: "trades/raw"),
            uploader,
            clock);

        TradeInfo earlyTrade = CreateTrade(tradeId: "1", timestamp: new DateTimeOffset(2026, 3, 12, 13, 37, 0, TimeSpan.Zero), price: 10m);
        TradeInfo lateTrade = CreateTrade(tradeId: "2", timestamp: new DateTimeOffset(2026, 3, 12, 13, 37, 1, TimeSpan.Zero), price: 11m);

        await sink.Append(new[] { lateTrade, earlyTrade }, CancellationToken.None);
        await sink.Append(new[] { lateTrade, earlyTrade }, CancellationToken.None);

        Assert.Equal(2, uploader.Uploads.Count);
        Assert.Equal(uploader.Uploads[0].ObjectKey, uploader.Uploads[1].ObjectKey);

        string firstPayload = uploader.Uploads[0].Payload;
        string[] lines = firstPayload.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(2, lines.Length);

        using JsonDocument firstLine = JsonDocument.Parse(lines[0]);
        using JsonDocument secondLine = JsonDocument.Parse(lines[1]);
        Assert.Equal("1", firstLine.RootElement.GetProperty("tradeId").GetString());
        Assert.Equal("2", secondLine.RootElement.GetProperty("tradeId").GetString());
    }

    private static TradeInfo CreateTrade(string tradeId, DateTimeOffset timestamp, decimal price)
    {
        TradeKey key = new TradeKey(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), tradeId);
        return new TradeInfo(key, timestamp, price, quantity: 0.5m);
    }

    private sealed class FixedClock : IClock
    {
        public FixedClock(DateTimeOffset utcNow)
        {
            UtcNow = utcNow;
        }

        public DateTimeOffset UtcNow { get; }
    }

    private sealed class RecordingS3Uploader : IS3ObjectUploader
    {
        public List<UploadCall> Uploads { get; } = new List<UploadCall>();

        public Task UploadTextAsync(string bucketName, string objectKey, string payload, CancellationToken cancellationToken)
        {
            Uploads.Add(new UploadCall(bucketName, objectKey, payload));
            return Task.CompletedTask;
        }
    }

    private sealed record UploadCall(string BucketName, string ObjectKey, string Payload);
}
