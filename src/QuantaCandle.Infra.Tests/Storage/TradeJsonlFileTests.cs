using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Tests.Storage;

/// <summary>
/// Verifies streaming resume-boundary recovery from JSONL files.
/// </summary>
public sealed class TradeJsonlFileTests
{
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

    private static string SerializeTrade(string tradeId, DateTimeOffset timestamp)
    {
        var trade = new TradeInfo(
            new TradeKey(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), tradeId),
            timestamp,
            price: 1m,
            quantity: 1m);
        var result = TradeJsonlFile.BuildPayload([trade]).TrimEnd();
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
