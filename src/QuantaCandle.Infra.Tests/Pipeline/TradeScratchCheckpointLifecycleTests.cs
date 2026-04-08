using System.Text.Json;

using LogMachina;
using Moq;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Pipeline;

namespace QuantaCandle.Infra.Tests.Pipeline;

public sealed class TradeScratchCheckpointLifecycleTests
{
    [Fact]
    public async Task CheckpointWritesQcScratchJsonl()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, _) = CreateLifecycle(localRoot);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, Instrument.Parse("BTC-USDT"));
            Assert.True(File.Exists(scratchPath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task CheckpointBatchIsNormalizedBeforePersistence()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, _) = CreateLifecycle(localRoot);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("3", new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)),
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 12, 0, 0, 3, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var payload = await File.ReadAllTextAsync(TradeLocalDailyFilePath.BuildScratch(localRoot, Instrument.Parse("BTC-USDT")), CancellationToken.None);
            Assert.Equal(["1", "2", "3"], ParseTradeIds(payload));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task DuplicatesInsideCheckpointableRangeAreRemovedBeforePersistence()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, _) = CreateLifecycle(localRoot);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var payload = await File.ReadAllTextAsync(TradeLocalDailyFilePath.BuildScratch(localRoot, Instrument.Parse("BTC-USDT")), CancellationToken.None);
            Assert.Equal(["1", "2"], ParseTradeIds(payload));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task PersistedTailAndCheckpointableRangeBoundaryAreMergedWithoutDuplicates()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, _) = CreateLifecycle(localRoot);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 12, 0, 0, 3, TimeSpan.Zero)),
                CreateTrade("5", new DateTimeOffset(2026, 3, 12, 0, 0, 4, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            var payload = await File.ReadAllTextAsync(TradeLocalDailyFilePath.BuildScratch(localRoot, Instrument.Parse("BTC-USDT")), CancellationToken.None);
            Assert.Equal(["1", "2", "3", "4"], ParseTradeIds(payload));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task CheckpointCrossingUtcMidnightFinalizesOlderDateIntoDailyFile()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, _) = CreateLifecycle(localRoot);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var finalizedPath = TradeLocalDailyFilePath.Build(localRoot, Instrument.Parse("BTC-USDT"), new DateOnly(2026, 3, 12));
            Assert.True(File.Exists(finalizedPath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task CheckpointCrossingUtcMidnightDispatchesTheFinalizedDailyFile()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var (lifecycle, _) = CreateLifecycle(localRoot, dispatcher);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var dispatch = Assert.Single(dispatcher.Dispatches);
            Assert.Equal(Instrument.Parse("BTC-USDT"), dispatch.Instrument);
            Assert.Equal(new DateOnly(2026, 3, 12), dispatch.UtcDate);
            Assert.Equal(TradeLocalDailyFilePath.Build(localRoot, dispatch.Instrument, dispatch.UtcDate), dispatch.FinalizedFilePath);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task CrossMidnightRolloverStillWorksAfterBatchPreparation()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, _) = CreateLifecycle(localRoot);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var finalizedPayload = await File.ReadAllTextAsync(
                TradeLocalDailyFilePath.Build(localRoot, Instrument.Parse("BTC-USDT"), new DateOnly(2026, 3, 12)),
                CancellationToken.None);

            Assert.Equal(["1", "2"], ParseTradeIds(finalizedPayload));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task ScratchAfterSplitContainsOnlyNewerDatePersistedTrades()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, _) = CreateLifecycle(localRoot);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var scratchPayload = await File.ReadAllTextAsync(
                TradeLocalDailyFilePath.BuildScratch(localRoot, Instrument.Parse("BTC-USDT")),
                CancellationToken.None);

            Assert.Equal(["3"], ParseTradeIds(scratchPayload));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task LatestTradeIsStillExcludedFromPersistence()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, _) = CreateLifecycle(localRoot);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var finalizedPayload = await File.ReadAllTextAsync(
                TradeLocalDailyFilePath.Build(localRoot, Instrument.Parse("BTC-USDT"), new DateOnly(2026, 3, 12)),
                CancellationToken.None);
            var scratchPayload = await File.ReadAllTextAsync(
                TradeLocalDailyFilePath.BuildScratch(localRoot, Instrument.Parse("BTC-USDT")),
                CancellationToken.None);

            Assert.DoesNotContain("4", ParseTradeIds(finalizedPayload).OfType<string>());
            Assert.DoesNotContain("4", ParseTradeIds(scratchPayload).OfType<string>());
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task RepeatedCheckpointsAfterSplitContinueFromNewScratch()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, _) = CreateLifecycle(localRoot);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);
            await lifecycle.TrackAppendedTrades([CreateTrade("5", new DateTimeOffset(2026, 3, 13, 0, 0, 2, TimeSpan.Zero))], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            var finalizedPayload = await File.ReadAllTextAsync(
                TradeLocalDailyFilePath.Build(localRoot, Instrument.Parse("BTC-USDT"), new DateOnly(2026, 3, 12)),
                CancellationToken.None);
            var scratchPayload = await File.ReadAllTextAsync(
                TradeLocalDailyFilePath.BuildScratch(localRoot, Instrument.Parse("BTC-USDT")),
                CancellationToken.None);

            Assert.Equal(["1", "2"], ParseTradeIds(finalizedPayload));
            Assert.Equal(["3", "4"], ParseTradeIds(scratchPayload));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task LatestTradeRemainsInMemoryAcrossCheckpoint()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, _) = CreateLifecycle(localRoot);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);
            await lifecycle.TrackAppendedTrades([CreateTrade("4", new DateTimeOffset(2026, 3, 12, 0, 0, 3, TimeSpan.Zero))], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            var payload = await File.ReadAllTextAsync(TradeLocalDailyFilePath.BuildScratch(localRoot, Instrument.Parse("BTC-USDT")), CancellationToken.None);
            Assert.Equal(["1", "2", "3"], ParseTradeIds(payload));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task RepeatedCheckpointsDoNotDuplicateAlreadyPersistedTrades()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, _) = CreateLifecycle(localRoot);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            var payload = await File.ReadAllTextAsync(TradeLocalDailyFilePath.BuildScratch(localRoot, Instrument.Parse("BTC-USDT")), CancellationToken.None);
            Assert.Equal(["1"], ParseTradeIds(payload));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task MissingRangeGapIsRecordedButNotReconstructed()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, stateStore) = CreateLifecycle(localRoot);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 12, 0, 0, 3, TimeSpan.Zero)),
                CreateTrade("5", new DateTimeOffset(2026, 3, 12, 0, 0, 4, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var payload = await File.ReadAllTextAsync(TradeLocalDailyFilePath.BuildScratch(localRoot, Instrument.Parse("BTC-USDT")), CancellationToken.None);
            var gaps = await stateStore.GetGapsAsync(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), CancellationToken.None);

            Assert.Equal(["1", "4"], ParseTradeIds(payload));
            var gap = Assert.Single(gaps);
            Assert.Equal(TradeGapStatus.Bounded, gap.Status);
            Assert.Equal(new MissingTradeIdRange(2, 3), gap.MissingTradeIds);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    private static (TradeScratchCheckpointLifecycle Lifecycle, InMemoryIngestionStateStore StateStore) CreateLifecycle(
        string localRoot,
        ITradeFinalizedFileDispatcher? tradeFinalizedFileDispatcher = null)
    {
        var logMoq = new Mock<ILogMachina<TradeScratchCheckpointLifecycle>>();
        var stateStore = new InMemoryIngestionStateStore();
        var preparator = new TradeCheckpointBatchPreparator();
        var result = new TradeScratchCheckpointLifecycle(localRoot, tradeFinalizedFileDispatcher ?? new TradeSinkNull(), preparator, stateStore, logMoq.Object);
        return (result, stateStore);
    }

    private static TradeInfo CreateTrade(string tradeId, DateTimeOffset timestamp)
    {
        var key = new TradeKey(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), tradeId);
        return new TradeInfo(key, timestamp, 1m, 1m);
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

    private sealed class RecordingTradeFinalizedFileDispatcher : ITradeFinalizedFileDispatcher
    {
        public List<DispatchCall> Dispatches { get; } = [];

        public ValueTask DispatchAsync(Instrument instrument, DateOnly utcDate, string finalizedFilePath, CancellationToken cancellationToken)
        {
            Dispatches.Add(new DispatchCall(instrument, utcDate, finalizedFilePath));
            return ValueTask.CompletedTask;
        }
    }

    private sealed record DispatchCall(Instrument Instrument, DateOnly UtcDate, string FinalizedFilePath);
}
