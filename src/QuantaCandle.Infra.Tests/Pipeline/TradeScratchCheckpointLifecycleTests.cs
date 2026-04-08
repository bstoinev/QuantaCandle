using System.Text.Json;

using LogMachina;

using Moq;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Pipeline;

namespace QuantaCandle.Infra.Tests.Pipeline;

/// <summary>
/// Verifies recorder-owned scratch checkpoint persistence and UTC day rollover behavior.
/// </summary>
public sealed class TradeScratchCheckpointLifecycleTests
{
    [Fact]
    public async Task SameDayCheckpointAppendsOnlyToQcScratchJsonl()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var (lifecycle, _) = CreateLifecycle(localRoot, dispatcher);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var instrument = Instrument.Parse("BTC-USDT");
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, instrument);
            var finalizedPath = TradeLocalDailyFilePath.Build(localRoot, instrument, new DateOnly(2026, 3, 12));
            var payload = await File.ReadAllTextAsync(scratchPath, CancellationToken.None);

            Assert.True(File.Exists(scratchPath));
            Assert.False(File.Exists(finalizedPath));
            Assert.Equal(["1", "2"], ParseTradeIds(payload));
            Assert.Empty(dispatcher.Dispatches);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task RestartCheckpointFinalizesRecoveredScratchBeforeStartingNewDay()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var instrument = Instrument.Parse("BTC-USDT");
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, instrument);
            var priorDayTrades = new[]
            {
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
            };

            Directory.CreateDirectory(Path.GetDirectoryName(scratchPath)!);
            await File.WriteAllTextAsync(scratchPath, TradeJsonlFile.BuildPayload(priorDayTrades), CancellationToken.None);

            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var (lifecycle, _) = CreateLifecycle(localRoot, dispatcher);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("3", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var finalizedPath = TradeLocalDailyFilePath.Build(localRoot, instrument, new DateOnly(2026, 3, 12));
            var finalizedPayload = await File.ReadAllTextAsync(finalizedPath, CancellationToken.None);
            var scratchPayload = await File.ReadAllTextAsync(scratchPath, CancellationToken.None);
            var dispatch = Assert.Single(dispatcher.Dispatches);

            Assert.Equal(["1", "2"], ParseTradeIds(finalizedPayload));
            Assert.Equal(["3"], ParseTradeIds(scratchPayload));
            Assert.Equal(finalizedPath, dispatch.FinalizedFilePath);
            Assert.True(dispatch.FileExistedAtDispatch);
            Assert.Equal(new DateOnly(2026, 3, 12), dispatch.UtcDate);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task CrossMidnightCheckpointFinalizesOlderScratchThenStartsNewScratch()
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

            var instrument = Instrument.Parse("BTC-USDT");
            var finalizedPath = TradeLocalDailyFilePath.Build(localRoot, instrument, new DateOnly(2026, 3, 12));
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, instrument);
            var finalizedPayload = await File.ReadAllTextAsync(finalizedPath, CancellationToken.None);
            var scratchPayload = await File.ReadAllTextAsync(scratchPath, CancellationToken.None);
            var dispatch = Assert.Single(dispatcher.Dispatches);

            Assert.Equal(["1", "2"], ParseTradeIds(finalizedPayload));
            Assert.Equal(["3"], ParseTradeIds(scratchPayload));
            Assert.Equal(finalizedPath, dispatch.FinalizedFilePath);
            Assert.True(dispatch.FileExistedAtDispatch);
            Assert.Equal(new DateOnly(2026, 3, 12), dispatch.UtcDate);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task DispatcherReceivesOnlyFinalizedDailyFiles()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var (lifecycle, _) = CreateLifecycle(localRoot, dispatcher);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("3", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("5", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            var dispatch = Assert.Single(dispatcher.Dispatches);

            Assert.Equal("2026-03-12.jsonl", Path.GetFileName(dispatch.FinalizedFilePath));
            Assert.DoesNotContain("qc-scratch", dispatch.FinalizedFilePath, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task LatestTradeRetentionStillWorksAcrossRepeatedSameDayCheckpoints()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, _) = CreateLifecycle(localRoot);
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, Instrument.Parse("BTC-USDT"));

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("3", new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 12, 0, 0, 3, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            var payload = await File.ReadAllTextAsync(scratchPath, CancellationToken.None);

            Assert.Equal(["1", "2", "3"], ParseTradeIds(payload));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task NewDayScratchContinuesAfterRollover()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, _) = CreateLifecycle(localRoot);
            var instrument = Instrument.Parse("BTC-USDT");
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, instrument);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            var firstPayload = await File.ReadAllTextAsync(scratchPath, CancellationToken.None);

            Assert.Equal(["3"], ParseTradeIds(firstPayload));

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("4", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
                CreateTrade("5", new DateTimeOffset(2026, 3, 13, 0, 0, 2, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            var payload = await File.ReadAllTextAsync(scratchPath, CancellationToken.None);

            Assert.Equal(["3", "4"], ParseTradeIds(payload));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task GapTracingUsesOnlyCurrentBatchBeingFlushed()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var (lifecycle, stateStore) = CreateLifecycle(localRoot);
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, Instrument.Parse("BTC-USDT"));

            Directory.CreateDirectory(Path.GetDirectoryName(scratchPath)!);
            await File.WriteAllTextAsync(
                scratchPath,
                TradeJsonlFile.BuildPayload(
                [
                    CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                    CreateTrade("4", new DateTimeOffset(2026, 3, 12, 0, 0, 3, TimeSpan.Zero)),
                ]),
                CancellationToken.None);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("5", new DateTimeOffset(2026, 3, 12, 0, 0, 4, TimeSpan.Zero)),
                CreateTrade("6", new DateTimeOffset(2026, 3, 12, 0, 0, 5, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            var gaps = await stateStore.GetGapsAsync(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), CancellationToken.None);

            Assert.Empty(gaps);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task GapTracingStillRecordsMissingIdsInsideCurrentCheckpointBatch()
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

            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, Instrument.Parse("BTC-USDT"));
            var payload = await File.ReadAllTextAsync(scratchPath, CancellationToken.None);
            var gaps = await stateStore.GetGapsAsync(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), CancellationToken.None);
            var gap = Assert.Single(gaps);

            Assert.Equal(["1", "4"], ParseTradeIds(payload));
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
        var result = new TradeScratchCheckpointLifecycle(localRoot, tradeFinalizedFileDispatcher ?? new TradeSinkNull(), stateStore, logMoq.Object);
        return (result, stateStore);
    }

    private static TradeInfo CreateTrade(string tradeId, DateTimeOffset timestamp)
    {
        var key = new TradeKey(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), tradeId);
        var result = new TradeInfo(key, timestamp, 1m, 1m);
        return result;
    }

    private static List<string?> ParseTradeIds(string payload)
    {
        var result = new List<string?>();
        var lines = payload.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

        foreach (var line in lines)
        {
            if (!line.StartsWith("{", StringComparison.Ordinal))
            {
                continue;
            }

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
            Dispatches.Add(new DispatchCall(instrument, utcDate, finalizedFilePath, File.Exists(finalizedFilePath)));
            return ValueTask.CompletedTask;
        }
    }

    private sealed record DispatchCall(Instrument Instrument, DateOnly UtcDate, string FinalizedFilePath, bool FileExistedAtDispatch);
}
