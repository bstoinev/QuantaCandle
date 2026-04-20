using QuantaCandle.Core.Trading;
using System.Text.Json;

namespace QuantaCandle.Infra.Tests.Storage;

/// <summary>
/// Verifies streaming resume-boundary recovery from JSONL files.
/// </summary>
public sealed class TradeJsonlFileTests
{
    [Fact]
    public async Task AppendTradesWritesIsBuyerMakerIntoJsonlRows()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var rootDirectory = Path.Combine(Path.GetTempPath(), "QuantaCandle.TradeJsonlFileTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(rootDirectory);

        try
        {
            var path = Path.Combine(rootDirectory, "BTC-USDT", "qc-scratch.jsonl");
            var trades = new[]
            {
                new TradeInfo(
                    new TradeKey(new ExchangeId("Binance"), Instrument.Parse("BTC-USDT"), "101"),
                    new DateTimeOffset(2026, 3, 12, 9, 30, 0, TimeSpan.Zero),
                    100m,
                    1m,
                    buyerIsMaker: true),
                new TradeInfo(
                    new TradeKey(new ExchangeId("Binance"), Instrument.Parse("BTC-USDT"), "102"),
                    new DateTimeOffset(2026, 3, 12, 9, 31, 0, TimeSpan.Zero),
                    101m,
                    2m,
                    buyerIsMaker: false),
            };

            await TradeJsonlFile.AppendTrades(path, trades, cancellationToken);

            var payload = await File.ReadAllTextAsync(path, cancellationToken);

            Assert.Equal([true, false], ParseBuyerIsMakerValues(payload));
        }
        finally
        {
            if (Directory.Exists(rootDirectory))
            {
                Directory.Delete(rootDirectory, true);
            }
        }
    }

    [Fact]
    public async Task ResumeBoundaryReaderStreamsScratchLinesWithoutWholeFileMaterialization()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var reader = new StreamingOnlyTextReader(
        [
            string.Empty,
            SerializeTrade("10", new DateTimeOffset(2026, 3, 11, 23, 59, 0, TimeSpan.Zero)),
            "   ",
            SerializeTrade("11", new DateTimeOffset(2026, 3, 12, 0, 1, 0, TimeSpan.Zero)),
        ]);

        var resumeBoundary = await TradeJsonlFile.TryReadLatestResumeBoundaryFromReader(reader, "LatestScratchFile", cancellationToken);

        Assert.NotNull(resumeBoundary);
        Assert.Equal("LatestScratchFile", resumeBoundary.Value.Origin);
        Assert.Equal(new DateOnly(2026, 3, 12), resumeBoundary.Value.UtcDate);
        Assert.Equal(new DateTimeOffset(2026, 3, 12, 0, 1, 0, TimeSpan.Zero), resumeBoundary.Value.TimestampUtc);
        Assert.Equal(5, reader.ReadLineCallCount);
    }

    [Fact]
    public async Task ResumeBoundaryReaderStreamsDailyLinesWithoutWholeFileMaterialization()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var reader = new StreamingOnlyTextReader(
        [
            SerializeTrade("20", new DateTimeOffset(2026, 3, 12, 9, 30, 0, TimeSpan.Zero)),
            SerializeTrade("21", new DateTimeOffset(2026, 3, 12, 9, 45, 0, TimeSpan.Zero)),
            SerializeTrade("22", new DateTimeOffset(2026, 3, 12, 10, 0, 0, TimeSpan.Zero)),
        ]);

        var resumeBoundary = await TradeJsonlFile.TryReadLatestResumeBoundaryFromReader(reader, "LatestLocalDailyFile", cancellationToken);

        Assert.NotNull(resumeBoundary);
        Assert.Equal("LatestLocalDailyFile", resumeBoundary.Value.Origin);
        Assert.Equal(new DateOnly(2026, 3, 12), resumeBoundary.Value.UtcDate);
        Assert.Equal(new DateTimeOffset(2026, 3, 12, 10, 0, 0, TimeSpan.Zero), resumeBoundary.Value.TimestampUtc);
        Assert.Equal(4, reader.ReadLineCallCount);
    }

    [Fact]
    public async Task RoundtripPreservesIsBuyerMaker()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var rootDirectory = Path.Combine(Path.GetTempPath(), "QuantaCandle.TradeJsonlFileTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(rootDirectory);

        try
        {
            var path = Path.Combine(rootDirectory, "BTC-USDT", "2026-03-12.jsonl");
            var expectedTrades = new[]
            {
                new TradeInfo(
                    new TradeKey(new ExchangeId("Binance"), Instrument.Parse("BTC-USDT"), "101"),
                    new DateTimeOffset(2026, 3, 12, 9, 30, 0, TimeSpan.Zero),
                    100m,
                    1m,
                    buyerIsMaker: true),
                new TradeInfo(
                    new TradeKey(new ExchangeId("Binance"), Instrument.Parse("BTC-USDT"), "102"),
                    new DateTimeOffset(2026, 3, 12, 9, 31, 0, TimeSpan.Zero),
                    101m,
                    2m,
                    buyerIsMaker: false),
            };

            await TradeJsonlFile.WriteFullPayload(path, TradeJsonlFile.BuildPayload(expectedTrades), cancellationToken);

            var actualTrades = await TradeJsonlFile.ReadTrades(path, cancellationToken);

            Assert.Equal(expectedTrades, actualTrades);
        }
        finally
        {
            if (Directory.Exists(rootDirectory))
            {
                Directory.Delete(rootDirectory, true);
            }
        }
    }

    private static string SerializeTrade(string tradeId, DateTimeOffset timestamp)
    {
        var trade = new TradeInfo(
            new TradeKey(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), tradeId),
            timestamp,
            price: 1m,
            quantity: 1m,
            buyerIsMaker: false);
        var result = TradeJsonlFile.BuildPayload([trade]).TrimEnd();
        return result;
    }

    private static List<bool> ParseBuyerIsMakerValues(string payload)
    {
        var result = new List<bool>();
        var lines = payload.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

        foreach (var line in lines)
        {
            using var document = JsonDocument.Parse(line);
            result.Add(document.RootElement.GetProperty("isBuyerMaker").GetBoolean());
        }

        return result;
    }

    /// <summary>
    /// Simulates a line-by-line reader and rejects bulk or whole-file reads.
    /// </summary>
    private sealed class StreamingOnlyTextReader(IReadOnlyList<string> lines) : TextReader
    {
        private int _index;

        public int ReadLineCallCount { get; private set; }

        public override Task<string?> ReadLineAsync()
        {
            throw new NotSupportedException("Use the cancellation-aware line reader.");
        }

        public override ValueTask<string?> ReadLineAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ReadLineCallCount++;

            string? result = null;

            if (_index < lines.Count)
            {
                result = lines[_index];
                _index++;
            }

            return ValueTask.FromResult(result);
        }

        public override int Read(char[] buffer, int index, int count)
        {
            throw new NotSupportedException("Bulk reads are not allowed in this test.");
        }

        public override int Read(Span<char> buffer)
        {
            throw new NotSupportedException("Bulk reads are not allowed in this test.");
        }

        public override ValueTask<int> ReadAsync(Memory<char> buffer, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("Bulk reads are not allowed in this test.");
        }

        public override Task<int> ReadAsync(char[] buffer, int index, int count)
        {
            throw new NotSupportedException("Bulk reads are not allowed in this test.");
        }

        public override string ReadToEnd()
        {
            throw new NotSupportedException("Whole-file reads are not allowed in this test.");
        }

        public override Task<string> ReadToEndAsync()
        {
            throw new NotSupportedException("Whole-file reads are not allowed in this test.");
        }
    }
}
