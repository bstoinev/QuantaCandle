using System.Globalization;
using System.Text.Json;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Storage;

namespace QuantaCandle.Infra.Tests.Storage;

public sealed class LocalFileTradeGapScannerTests
{
    [Fact]
    public async Task ContinuousTradeIdsProduceNoGaps()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(
                root,
                "BTC-USDT",
                "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "100", "2026-03-12T00:00:01Z"),
                Trade("binance", "BTC-USDT", "101", "2026-03-12T00:00:02Z"),
                Trade("binance", "BTC-USDT", "102", "2026-03-12T00:00:03Z"));

            var scanner = new LocalFileTradeGapScanner();
            var result = await scanner.Scan(new TradeGapScanRequest(root, [], []), CancellationToken.None);

            Assert.Equal(1, result.TotalFilesScanned);
            Assert.Equal(3, result.TotalTradesScanned);
            Assert.Equal(0, result.SkippedNonNumericTradeCount);
            Assert.Empty(result.DetectedGaps);
            Assert.Single(result.AffectedFiles);
            Assert.Equal(Path.Combine("BTC-USDT", "2026-03-12.jsonl"), result.AffectedFiles[0].Path);
        }
        finally
        {
            DeleteDirectory(root);
        }
    }

    [Fact]
    public async Task MissingRangeProducesOneReportedGap()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(
                root,
                "BTC-USDT",
                "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "100", "2026-03-12T00:00:01Z"),
                Trade("binance", "BTC-USDT", "103", "2026-03-12T00:00:04Z"));

            var scanner = new LocalFileTradeGapScanner();
            var result = await scanner.Scan(new TradeGapScanRequest(root, [], []), CancellationToken.None);

            var gap = Assert.Single(result.DetectedGaps);
            Assert.Equal("binance", gap.Exchange.ToString());
            Assert.Equal("BTC-USDT", gap.Symbol.ToString());
            Assert.Equal(TradeGapStatus.Bounded, gap.Status);
            Assert.NotNull(gap.MissingTradeIds);
            Assert.Equal(101, gap.MissingTradeIds.Value.FirstTradeId);
            Assert.Equal(102, gap.MissingTradeIds.Value.LastTradeId);

            var affectedRange = Assert.Single(result.AffectedRanges);
            Assert.Equal("100", affectedRange.FromInclusive.TradeId);
            Assert.Equal("103", affectedRange.ToInclusive.TradeId);
            Assert.NotNull(affectedRange.FromLocation);
            Assert.NotNull(affectedRange.ToLocation);
            Assert.Equal(Path.Combine("BTC-USDT", "2026-03-12.jsonl"), affectedRange.FromLocation!.FilePath);
            Assert.Equal(1, affectedRange.FromLocation.LineNumber);
            Assert.Equal(Path.Combine("BTC-USDT", "2026-03-12.jsonl"), affectedRange.ToLocation!.FilePath);
            Assert.Equal(2, affectedRange.ToLocation.LineNumber);
        }
        finally
        {
            DeleteDirectory(root);
        }
    }

    [Fact]
    public async Task DuplicateTradeIdsDoNotProduceFalseGaps()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(
                root,
                "BTC-USDT",
                "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "100", "2026-03-12T00:00:01Z"),
                Trade("binance", "BTC-USDT", "100", "2026-03-12T00:00:02Z"),
                Trade("binance", "BTC-USDT", "101", "2026-03-12T00:00:03Z"),
                Trade("binance", "BTC-USDT", "101", "2026-03-12T00:00:04Z"),
                Trade("binance", "BTC-USDT", "102", "2026-03-12T00:00:05Z"));

            var scanner = new LocalFileTradeGapScanner();
            var result = await scanner.Scan(new TradeGapScanRequest(root, [], []), CancellationToken.None);

            Assert.Equal(5, result.TotalTradesScanned);
            Assert.Empty(result.DetectedGaps);
        }
        finally
        {
            DeleteDirectory(root);
        }
    }

    [Fact]
    public async Task NonNumericTradeIdsFailLoudly()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(
                root,
                "BTC-USDT",
                "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "100", "2026-03-12T00:00:01Z"),
                Trade("binance", "BTC-USDT", "not-numeric", "2026-03-12T00:00:02Z"),
                Trade("binance", "BTC-USDT", "101", "2026-03-12T00:00:03Z"));

            var scanner = new LocalFileTradeGapScanner();
            var path = Path.Combine(root, "BTC-USDT", "2026-03-12.jsonl");
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await scanner.Scan(new TradeGapScanRequest(root, [], []), CancellationToken.None));

            Assert.Contains("not-numeric", exception.Message, StringComparison.Ordinal);
            Assert.Contains(path, exception.Message, StringComparison.Ordinal);
            Assert.Contains("line 2", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectory(root);
        }
    }

    [Fact]
    public async Task MultipleFilesForSameInstrumentStillDetectGapAcrossFiles()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(
                root,
                "BTC-USDT",
                "2026-03-12-a.jsonl",
                Trade("binance", "BTC-USDT", "100", "2026-03-12T00:00:01Z"),
                Trade("binance", "BTC-USDT", "101", "2026-03-12T00:00:02Z"));

            await WriteTradeFileAsync(
                root,
                "BTC-USDT",
                "2026-03-12-b.jsonl",
                Trade("binance", "BTC-USDT", "104", "2026-03-12T00:00:05Z"),
                Trade("binance", "BTC-USDT", "105", "2026-03-12T00:00:06Z"));

            var scanner = new LocalFileTradeGapScanner();
            var result = await scanner.Scan(new TradeGapScanRequest(root, [], []), CancellationToken.None);

            Assert.Equal(2, result.TotalFilesScanned);
            Assert.Equal(4, result.TotalTradesScanned);
            var gap = Assert.Single(result.DetectedGaps);
            Assert.NotNull(gap.MissingTradeIds);
            Assert.Equal(102, gap.MissingTradeIds.Value.FirstTradeId);
            Assert.Equal(103, gap.MissingTradeIds.Value.LastTradeId);
        }
        finally
        {
            DeleteDirectory(root);
        }
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

    private static async Task WriteTradeFileAsync(string root, string instrument, string fileName, params object[] trades)
    {
        var directory = Path.Combine(root, instrument);
        Directory.CreateDirectory(directory);

        var path = Path.Combine(directory, fileName);
        var lines = trades.Select(static trade => JsonSerializer.Serialize(trade)).ToArray();
        var payload = string.Join(Environment.NewLine, lines) + Environment.NewLine;

        await File.WriteAllTextAsync(path, payload, CancellationToken.None);
    }
}
