using System.Text.Json;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Tests.Storage;

public sealed class TradeSinkFileSimpleTests
{
    private const string StartupBoundaryTradeId = "_startup-day-boundary";

    [Fact]
    public async Task Writes_to_instrument_and_day_partition_path()
    {
        string root = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
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

    [Fact]
    public async Task PersistedJsonlContainsOnlyRealTrades()
    {
        string root = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        try
        {
            TradeSinkFileSimple sink = new TradeSinkFileSimple(new TradeSinkFileSimpleOptions(root));

            Instrument instrument = Instrument.Parse("BTC-USDT");
            TradeInfo trade = new TradeInfo(
                new TradeKey(new ExchangeId("Stub"), instrument, "123"),
                new DateTimeOffset(2026, 3, 12, 13, 37, 0, TimeSpan.Zero),
                price: 10m,
                quantity: 0.5m);

            await sink.Append([trade], CancellationToken.None);

            string expectedPath = Path.Combine(root, "BTC-USDT", "2026-03-12.jsonl");
            string payload = await File.ReadAllTextAsync(expectedPath, CancellationToken.None);

            Assert.DoesNotContain(StartupBoundaryTradeId, payload, StringComparison.Ordinal);
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
