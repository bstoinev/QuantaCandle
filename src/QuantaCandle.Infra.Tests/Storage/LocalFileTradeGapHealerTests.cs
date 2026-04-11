using LogMachina;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra;
using QuantaCandle.Infra.Storage;

using Moq;

namespace QuantaCandle.Infra.Tests.Storage;

/// <summary>
/// Verifies local JSONL trade gap healing behavior for bounded single-gap repair scenarios.
/// </summary>
public sealed class LocalFileTradeGapHealerTests
{
    private static readonly ExchangeId BinanceExchange = new("Binance");

    [Fact]
    public async Task Heals_one_bounded_gap_fully()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(103)]);

            var healer = new LocalFileTradeGapHealer(new StubTradeGapFetchClient([CreateTrade(101), CreateTrade(102)]), CreateLog());
            var result = await healer.Heal(CreateRequest(rootDirectory, 101, 102, relativePath), CancellationToken.None);

            Assert.Equal(TradeGapHealStatus.Full, result.Outcome);
            Assert.Equal(2, result.FetchedTradeCount);
            Assert.Equal(2, result.InsertedTradeCount);
            Assert.True(result.HasFullRequestedCoverage);
            Assert.Empty(result.UnresolvedTradeRanges);

            var trades = await ReadTradesAsync(rootDirectory, relativePath);
            Assert.Equal(["100", "101", "102", "103"], trades.Select(static trade => trade.Key.TradeId).ToArray());
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task Partial_heal_persists_returned_trades_and_reports_partial()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(105)]);

            var healer = new LocalFileTradeGapHealer(new StubTradeGapFetchClient([CreateTrade(101), CreateTrade(102)]), CreateLog());
            var result = await healer.Heal(CreateRequest(rootDirectory, 101, 104, relativePath), CancellationToken.None);

            Assert.Equal(TradeGapHealStatus.Partial, result.Outcome);
            Assert.Equal(2, result.InsertedTradeCount);
            Assert.False(result.HasFullRequestedCoverage);
            Assert.Equal([103L, 104L], [result.UnresolvedTradeRanges[0].FirstTradeId, result.UnresolvedTradeRanges[0].LastTradeId]);

            var trades = await ReadTradesAsync(rootDirectory, relativePath);
            Assert.Equal(["100", "101", "102", "105"], trades.Select(static trade => trade.Key.TradeId).ToArray());
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task Merges_fetched_trades_in_deterministic_trade_id_order()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(104)]);

            var healer = new LocalFileTradeGapHealer(
                new StubTradeGapFetchClient([CreateTrade(103), CreateTrade(101), CreateTrade(102)]),
                CreateLog());
            await healer.Heal(CreateRequest(rootDirectory, 101, 103, relativePath), CancellationToken.None);

            var trades = await ReadTradesAsync(rootDirectory, relativePath);
            Assert.Equal(["100", "101", "102", "103", "104"], trades.Select(static trade => trade.Key.TradeId).ToArray());
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task Duplicate_fetched_trades_do_not_create_duplicates_in_output()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(103)]);

            var healer = new LocalFileTradeGapHealer(
                new StubTradeGapFetchClient([CreateTrade(101), CreateTrade(101), CreateTrade(102)]),
                CreateLog());
            var result = await healer.Heal(CreateRequest(rootDirectory, 101, 102, relativePath), CancellationToken.None);

            Assert.Equal(3, result.FetchedTradeCount);
            Assert.Equal(2, result.InsertedTradeCount);

            var trades = await ReadTradesAsync(rootDirectory, relativePath);
            Assert.Equal(["100", "101", "102", "103"], trades.Select(static trade => trade.Key.TradeId).ToArray());
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task Original_file_remains_intact_on_hard_failure()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(103)]);
            var originalContents = await File.ReadAllTextAsync(Path.Combine(rootDirectory, relativePath), CancellationToken.None);

            var invalidTrade = new TradeInfo(
                new TradeKey(new ExchangeId("Other"), Instrument.Parse("BTC-USDT"), "101"),
                CreateTimestamp(101),
                201m,
                1.01m);
            var healer = new LocalFileTradeGapHealer(new StubTradeGapFetchClient([invalidTrade]), CreateLog());

            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await healer.Heal(CreateRequest(rootDirectory, 101, 101, relativePath), CancellationToken.None));

            var currentContents = await File.ReadAllTextAsync(Path.Combine(rootDirectory, relativePath), CancellationToken.None);
            Assert.Equal(originalContents, currentContents);
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task Internal_gaps_in_fetched_batch_produce_warning_without_failing_heal()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(106)]);

            var healer = new LocalFileTradeGapHealer(
                new StubTradeGapFetchClient([CreateTrade(101), CreateTrade(103), CreateTrade(104)]),
                CreateLog());
            var result = await healer.Heal(CreateRequest(rootDirectory, 101, 105, relativePath), CancellationToken.None);

            Assert.Equal(TradeGapHealStatus.Partial, result.Outcome);
            Assert.Contains(result.Warnings, static warning => warning.Contains("internal gap", StringComparison.OrdinalIgnoreCase));

            var trades = await ReadTradesAsync(rootDirectory, relativePath);
            Assert.Equal(["100", "101", "103", "104", "106"], trades.Select(static trade => trade.Key.TradeId).ToArray());
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    private static TradeGapHealRequest CreateRequest(string rootDirectory, long missingTradeIdStart, long missingTradeIdEnd, string relativePath)
    {
        var affectedFile = new TradeGapAffectedFile(relativePath, new DateOnly(2026, 3, 28));
        var affectedRange = new TradeGapAffectedRange(
            new TradeWatermark((missingTradeIdStart - 1).ToString(), CreateTimestamp(missingTradeIdStart - 1)),
            new TradeWatermark((missingTradeIdEnd + 1).ToString(), CreateTimestamp(missingTradeIdEnd + 1)),
            new TradeGapBoundaryLocation(relativePath, 1),
            new TradeGapBoundaryLocation(relativePath, 2));
        var result = new TradeGapHealRequest(rootDirectory, BinanceExchange, Instrument.Parse("BTC-USDT"), missingTradeIdStart, missingTradeIdEnd, [affectedFile], affectedRange);
        return result;
    }

    private static TradeInfo CreateTrade(long tradeId)
    {
        var result = new TradeInfo(
            new TradeKey(BinanceExchange, Instrument.Parse("BTC-USDT"), tradeId.ToString()),
            CreateTimestamp(tradeId),
            100m + tradeId,
            1m);
        return result;
    }

    private static DateTimeOffset CreateTimestamp(long tradeId)
    {
        var result = new DateTimeOffset(2026, 3, 28, 0, 0, 0, TimeSpan.Zero).AddSeconds(tradeId);
        return result;
    }

    private static async Task<IReadOnlyList<TradeInfo>> ReadTradesAsync(string rootDirectory, string relativePath)
    {
        var result = await TradeJsonlFile.ReadTrades(Path.Combine(rootDirectory, relativePath), CancellationToken.None);
        return result;
    }

    private static async Task WriteTradesAsync(string rootDirectory, string relativePath, IReadOnlyList<TradeInfo> trades)
    {
        await TradeJsonlFile.WriteFullPayload(Path.Combine(rootDirectory, relativePath), TradeJsonlFile.BuildPayload(trades), CancellationToken.None);
    }

    private static string CreateRootDirectory()
    {
        var result = Path.Combine(Path.GetTempPath(), "QuantaCandle.LocalFileTradeGapHealerTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(result);
        return result;
    }

    private static void DeleteRootDirectory(string rootDirectory)
    {
        if (Directory.Exists(rootDirectory))
        {
            Directory.Delete(rootDirectory, true);
        }
    }

    private static ILogMachina<LocalFileTradeGapHealer> CreateLog()
    {
        var result = new Mock<ILogMachina<LocalFileTradeGapHealer>>().Object;
        return result;
    }

    /// <summary>
    /// Returns a predefined fetched trade batch for healer tests.
    /// </summary>
    private sealed class StubTradeGapFetchClient(IReadOnlyList<TradeInfo> trades) : ITradeGapFetchClient
    {
        private readonly IReadOnlyList<TradeInfo> _trades = trades ?? throw new ArgumentNullException(nameof(trades));

        /// <summary>
        /// Returns the configured fetched trade batch without additional processing.
        /// </summary>
        public ValueTask<IReadOnlyList<TradeInfo>> Fetch(Instrument instrument, long missingTradeIdStart, long missingTradeIdEnd, CancellationToken cancellationToken)
        {
            IReadOnlyList<TradeInfo> result = _trades;
            return ValueTask.FromResult(result);
        }
    }
}
