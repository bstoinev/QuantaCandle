using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using QuantaCandle.Core.Trading;
using QuantaCandle.Service.Stubs;

namespace QuantaCandle.Service.Tests.Stubs;

public sealed class TradeSinkFileSimpleTests
{
    [Fact]
    public async Task Writes_to_instrument_and_day_partition_path()
    {
        string root = Path.Combine(Path.GetTempPath(), "QuantaCandle.Service.Tests", Guid.NewGuid().ToString("N"));
        try
        {
            TradeSinkFileSimple sink = new TradeSinkFileSimple(new TradeSinkFileSimpleOptions(root));

            Instrument instrument = Instrument.Parse("BTC-USDT");
            DateTimeOffset timestamp = new DateTimeOffset(2026, 3, 12, 13, 37, 0, TimeSpan.Zero);
            TradeKey key = new TradeKey(new ExchangeId("Stub"), instrument, "123");
            TradeInfo trade = new TradeInfo(key, timestamp, price: 10m, quantity: 0.5m);

            TradeAppendResult result = await sink.Append(new[] { trade }, CancellationToken.None);
            Assert.Equal(1, result.InsertedCount);

            string expectedPath = Path.Combine(root, "BTC-USDT", "2026-03-12.jsonl");
            Assert.True(File.Exists(expectedPath));

            string[] lines = await File.ReadAllLinesAsync(expectedPath, CancellationToken.None);
            Assert.Single(lines);

            using JsonDocument doc = JsonDocument.Parse(lines[0]);
            Assert.Equal("Stub", doc.RootElement.GetProperty("exchange").GetString());
            Assert.Equal("BTC-USDT", doc.RootElement.GetProperty("instrument").GetString());
            Assert.Equal("123", doc.RootElement.GetProperty("tradeId").GetString());
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }
}
