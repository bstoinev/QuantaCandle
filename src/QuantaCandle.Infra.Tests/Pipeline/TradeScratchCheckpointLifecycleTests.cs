using System.Text.Json;

using LogMachina;

using Moq;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Pipeline;
using QuantaCandle.Infra.Storage;

namespace QuantaCandle.Infra.Tests.Pipeline;

/// <summary>
/// Verifies recorder-owned scratch checkpoint persistence and UTC day rollover behavior.
/// </summary>
public sealed class TradeScratchCheckpointLifecycleTests
{
    private static readonly ExchangeId StubExchange = new("Stub");

    [Fact]
    public async Task SameDayCheckpointAppendsOnlyToQcScratchJsonl()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var (lifecycle, _, _) = CreateLifecycle(localRoot, dispatcher);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var instrument = Instrument.Parse("BTC-USDT");
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, StubExchange, instrument);
            var finalizedPath = TradeLocalDailyFilePath.Build(localRoot, StubExchange, instrument, new DateOnly(2026, 3, 12));
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
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, StubExchange, instrument);
            var priorDayTrades = new[]
            {
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
            };

            Directory.CreateDirectory(Path.GetDirectoryName(scratchPath)!);
            await File.WriteAllTextAsync(scratchPath, TradeJsonlFile.BuildPayload(priorDayTrades), CancellationToken.None);

            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var (lifecycle, _, _) = CreateLifecycle(localRoot, dispatcher);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("3", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var finalizedPath = TradeLocalDailyFilePath.Build(localRoot, StubExchange, instrument, new DateOnly(2026, 3, 12));
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
            var (lifecycle, _, _) = CreateLifecycle(localRoot, dispatcher);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var instrument = Instrument.Parse("BTC-USDT");
            var finalizedPath = TradeLocalDailyFilePath.Build(localRoot, StubExchange, instrument, new DateOnly(2026, 3, 12));
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, StubExchange, instrument);
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
            var (lifecycle, _, _) = CreateLifecycle(localRoot, dispatcher);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            Assert.Empty(dispatcher.Dispatches);

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
    public async Task RolloverHealsMissingStartBoundaryAndFinalizesJsonlWhenComplete()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var resolverMoq = CreateBoundaryResolverMock(new TradeDayBoundary(StubExchange, Instrument.Parse("BTC-USDT"), new DateOnly(2026, 3, 12), 100, 102, null));
            var fetchClient = new RecordingTradeGapFetchClient([CreateTrade("100", new DateTimeOffset(2026, 3, 12, 23, 59, 57, TimeSpan.Zero))]);
            var healer = new LocalFileTradeGapHealer(fetchClient, new Mock<ILogMachina<LocalFileTradeGapHealer>>().Object);
            var (lifecycle, _, _) = CreateLifecycle(localRoot, dispatcher, tradeGapHealer: healer, tradeDayBoundaryResolver: resolverMoq.Object);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("101", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("102", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("103", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("104", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var finalizedPath = TradeLocalDailyFilePath.Build(localRoot, StubExchange, Instrument.Parse("BTC-USDT"), new DateOnly(2026, 3, 12));
            var payload = await File.ReadAllTextAsync(finalizedPath, CancellationToken.None);

            Assert.True(File.Exists(finalizedPath));
            Assert.False(File.Exists(TradeLocalDailyFilePath.BuildPartial(localRoot, StubExchange, Instrument.Parse("BTC-USDT"), new DateOnly(2026, 3, 12))));
            Assert.Equal(["100", "101", "102"], ParseTradeIds(payload));
            Assert.Equal(1, fetchClient.FetchCallCount);
            Assert.Equal("2026-03-12.jsonl", Path.GetFileName(Assert.Single(dispatcher.Dispatches).FinalizedFilePath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task RolloverHealsMissingEndBoundaryAndFinalizesJsonlWhenComplete()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var resolverMoq = CreateBoundaryResolverMock(new TradeDayBoundary(StubExchange, Instrument.Parse("BTC-USDT"), new DateOnly(2026, 3, 12), 100, 102, null));
            var fetchClient = new RecordingTradeGapFetchClient([CreateTrade("102", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero))]);
            var healer = new LocalFileTradeGapHealer(fetchClient, new Mock<ILogMachina<LocalFileTradeGapHealer>>().Object);
            var (lifecycle, _, _) = CreateLifecycle(localRoot, dispatcher, tradeGapHealer: healer, tradeDayBoundaryResolver: resolverMoq.Object);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("100", new DateTimeOffset(2026, 3, 12, 23, 59, 57, TimeSpan.Zero)),
                CreateTrade("101", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("103", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("104", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var finalizedPath = TradeLocalDailyFilePath.Build(localRoot, StubExchange, Instrument.Parse("BTC-USDT"), new DateOnly(2026, 3, 12));
            var payload = await File.ReadAllTextAsync(finalizedPath, CancellationToken.None);

            Assert.True(File.Exists(finalizedPath));
            Assert.Equal(["100", "101", "102"], ParseTradeIds(payload));
            Assert.Equal(1, fetchClient.FetchCallCount);
            Assert.Equal("2026-03-12.jsonl", Path.GetFileName(Assert.Single(dispatcher.Dispatches).FinalizedFilePath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task RolloverLeavesPartialJsonlWhenGapsRemainAfterOneHealAttempt()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var resolverMoq = CreateBoundaryResolverMock(new TradeDayBoundary(StubExchange, Instrument.Parse("BTC-USDT"), new DateOnly(2026, 3, 12), 100, 102, null));
            var fetchClient = new RecordingTradeGapFetchClient([]);
            var healer = new LocalFileTradeGapHealer(fetchClient, new Mock<ILogMachina<LocalFileTradeGapHealer>>().Object);
            var (lifecycle, _, _) = CreateLifecycle(localRoot, dispatcher, tradeGapHealer: healer, tradeDayBoundaryResolver: resolverMoq.Object);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("101", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("102", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("103", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("104", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var partialPath = TradeLocalDailyFilePath.BuildPartial(localRoot, StubExchange, Instrument.Parse("BTC-USDT"), new DateOnly(2026, 3, 12));
            var payload = await File.ReadAllTextAsync(partialPath, CancellationToken.None);

            Assert.True(File.Exists(partialPath));
            Assert.False(File.Exists(TradeLocalDailyFilePath.Build(localRoot, StubExchange, Instrument.Parse("BTC-USDT"), new DateOnly(2026, 3, 12))));
            Assert.Equal(["101", "102"], ParseTradeIds(payload));
            Assert.Equal(1, fetchClient.FetchCallCount);
            Assert.Equal("2026-03-12.partial.jsonl", Path.GetFileName(Assert.Single(dispatcher.Dispatches).FinalizedFilePath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task BoundaryVerificationInconsistencyInBestEffortLogsWarningAndDoesNotCrash()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var instrument = Instrument.Parse("BTC-USDT");
            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var warning = "Unable to verify Binance raw candidate last trade id 102 for BTC-USDT on 2026-03-12.";
            var resolverMoq = CreateBoundaryResolverMock(new TradeDayBoundary(StubExchange, instrument, new DateOnly(2026, 3, 12), 100, null, warning));
            var healer = new LocalFileTradeGapHealer(new RecordingTradeGapFetchClient([]), new Mock<ILogMachina<LocalFileTradeGapHealer>>().Object);
            var (lifecycle, _, logMoq) = CreateLifecycle(localRoot, dispatcher, tradeGapHealer: healer, tradeDayBoundaryResolver: resolverMoq.Object);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("100", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("101", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("103", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("104", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var finalizedPath = TradeLocalDailyFilePath.Build(localRoot, StubExchange, instrument, new DateOnly(2026, 3, 12));

            Assert.True(File.Exists(finalizedPath));
            Assert.Single(dispatcher.Dispatches);
            logMoq.Verify(log => log.Warn(It.Is<string>(message => message.Contains("boundary verification inconsistency", StringComparison.Ordinal) && message.Contains(warning, StringComparison.Ordinal))), Times.Exactly(2));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task InteriorAndBoundaryGapsStillUseOneRolloverRepairPassAndChooseCorrectFilename()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var instrument = Instrument.Parse("BTC-USDT");
            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var resolverMoq = CreateBoundaryResolverMock(new TradeDayBoundary(StubExchange, instrument, new DateOnly(2026, 3, 12), 100, 104, null));
            var fetchClient = new RecordingTradeGapFetchClient([
                CreateTrade("100", new DateTimeOffset(2026, 3, 12, 23, 59, 56, TimeSpan.Zero)),
                CreateTrade("102", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
            ]);
            var healer = new LocalFileTradeGapHealer(fetchClient, new Mock<ILogMachina<LocalFileTradeGapHealer>>().Object);
            var (lifecycle, _, _) = CreateLifecycle(localRoot, dispatcher, tradeGapHealer: healer, tradeDayBoundaryResolver: resolverMoq.Object);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("101", new DateTimeOffset(2026, 3, 12, 23, 59, 57, TimeSpan.Zero)),
                CreateTrade("103", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("105", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("106", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var partialPath = TradeLocalDailyFilePath.BuildPartial(localRoot, StubExchange, instrument, new DateOnly(2026, 3, 12));
            var payload = await File.ReadAllTextAsync(partialPath, CancellationToken.None);

            Assert.True(File.Exists(partialPath));
            Assert.Equal(["100", "101", "102", "103"], ParseTradeIds(payload));
            Assert.Equal(3, fetchClient.FetchCallCount);
            Assert.Equal("2026-03-12.partial.jsonl", Path.GetFileName(Assert.Single(dispatcher.Dispatches).FinalizedFilePath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task CompleteDayWithoutGapsKeepsExistingRolloverBehavior()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var instrument = Instrument.Parse("BTC-USDT");
            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var resolverMoq = CreateBoundaryResolverMock(new TradeDayBoundary(StubExchange, instrument, new DateOnly(2026, 3, 12), 100, 102, null));
            var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
            var (lifecycle, _, _) = CreateLifecycle(localRoot, dispatcher, tradeGapHealer: healerMoq.Object, tradeDayBoundaryResolver: resolverMoq.Object);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("100", new DateTimeOffset(2026, 3, 12, 23, 59, 57, TimeSpan.Zero)),
                CreateTrade("101", new DateTimeOffset(2026, 3, 12, 23, 59, 58, TimeSpan.Zero)),
                CreateTrade("102", new DateTimeOffset(2026, 3, 12, 23, 59, 59, TimeSpan.Zero)),
                CreateTrade("103", new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("104", new DateTimeOffset(2026, 3, 13, 0, 0, 1, TimeSpan.Zero)),
            ], CancellationToken.None);

            await lifecycle.CheckpointActive(CancellationToken.None);

            var finalizedPath = TradeLocalDailyFilePath.Build(localRoot, StubExchange, instrument, new DateOnly(2026, 3, 12));
            var payload = await File.ReadAllTextAsync(finalizedPath, CancellationToken.None);

            Assert.True(File.Exists(finalizedPath));
            Assert.Equal(["100", "101", "102"], ParseTradeIds(payload));
            Assert.Equal("2026-03-12.jsonl", Path.GetFileName(Assert.Single(dispatcher.Dispatches).FinalizedFilePath));
            healerMoq.VerifyNoOtherCalls();
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
            var (lifecycle, _, _) = CreateLifecycle(localRoot);
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, StubExchange, Instrument.Parse("BTC-USDT"));

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
            var (lifecycle, _, _) = CreateLifecycle(localRoot);
            var instrument = Instrument.Parse("BTC-USDT");
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, StubExchange, instrument);

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
            var (lifecycle, stateStore, _) = CreateLifecycle(localRoot);
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, StubExchange, "BTC-USDT");

            Directory.CreateDirectory(Path.GetDirectoryName(scratchPath)!);
            var payload = TradeJsonlFile.BuildPayload(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 12, 0, 0, 3, TimeSpan.Zero)),
            ]);

            await File.WriteAllTextAsync(scratchPath, payload, CancellationToken.None);

            var appendedTrades = new TradeInfo[] {
                CreateTrade("5", new DateTimeOffset(2026, 3, 12, 0, 0, 4, TimeSpan.Zero)),
                CreateTrade("6", new DateTimeOffset(2026, 3, 12, 0, 0, 5, TimeSpan.Zero)),
            };

            await lifecycle.TrackAppendedTrades(appendedTrades, CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            var gaps = await stateStore.GetGapsAsync(StubExchange, Instrument.Parse("BTC-USDT"), CancellationToken.None);

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
            var (lifecycle, stateStore, _) = CreateLifecycle(localRoot);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("4", new DateTimeOffset(2026, 3, 12, 0, 0, 3, TimeSpan.Zero)),
                CreateTrade("5", new DateTimeOffset(2026, 3, 12, 0, 0, 4, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, StubExchange, Instrument.Parse("BTC-USDT"));
            var payload = await File.ReadAllTextAsync(scratchPath, CancellationToken.None);
            var gaps = await stateStore.GetGapsAsync(StubExchange, Instrument.Parse("BTC-USDT"), CancellationToken.None);
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

    [Fact]
    public async Task SnapshotDispatchCopiesPersistedScratchAfterCheckpointWithoutMutatingScratch()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var snapshotDispatcher = new RecordingTradeSnapshotFileDispatcher();
            var clock = new StubClock(new DateTimeOffset(2026, 3, 12, 14, 15, 16, 789, TimeSpan.Zero));
            var (lifecycle, _, _) = CreateLifecycle(localRoot, tradeSnapshotFileDispatcher: snapshotDispatcher, clock: clock);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            var instrument = Instrument.Parse("BTC-USDT");
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, StubExchange, instrument);
            var scratchPayloadBeforeSnapshot = await File.ReadAllTextAsync(scratchPath, CancellationToken.None);

            var snapshotDispatched = await lifecycle.DispatchActiveSnapshot(CancellationToken.None);

            var dispatch = Assert.Single(snapshotDispatcher.Dispatches);
            var snapshotPayload = await File.ReadAllTextAsync(dispatch.SnapshotFilePath, CancellationToken.None);
            var scratchPayloadAfterSnapshot = await File.ReadAllTextAsync(scratchPath, CancellationToken.None);

            Assert.True(snapshotDispatched);
            Assert.Equal("2026-03-12.141516789.jsonl", Path.GetFileName(dispatch.SnapshotFilePath));
            Assert.Equal(["1", "2"], ParseTradeIds(snapshotPayload));
            Assert.Equal(scratchPayloadBeforeSnapshot, snapshotPayload);
            Assert.Equal(scratchPayloadBeforeSnapshot, scratchPayloadAfterSnapshot);
            Assert.True(File.Exists(scratchPath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task SnapshotDispatchIgnoresOtherScratchFilesUnderSameLocalRoot()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var snapshotDispatcher = new RecordingTradeSnapshotFileDispatcher();
            var clock = new StubClock(new DateTimeOffset(2026, 3, 12, 14, 15, 16, 789, TimeSpan.Zero));
            var (lifecycle, _, _) = CreateLifecycle(localRoot, tradeSnapshotFileDispatcher: snapshotDispatcher, clock: clock);
            var ignoredScratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, StubExchange, Instrument.Parse("ETH-USDT"));

            Directory.CreateDirectory(Path.GetDirectoryName(ignoredScratchPath)!);
            await File.WriteAllTextAsync(
                ignoredScratchPath,
                TradeJsonlFile.BuildPayload(
                [
                    CreateTrade("999", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero), "ETH-USDT"),
                ]),
                CancellationToken.None);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            await lifecycle.DispatchActiveSnapshot(CancellationToken.None);

            var dispatch = Assert.Single(snapshotDispatcher.Dispatches);

            Assert.Equal(StubExchange, dispatch.Exchange);
            Assert.Equal("BTC-USDT", dispatch.Instrument.ToString());
            Assert.DoesNotContain("ETH-USDT", dispatch.SnapshotFilePath, StringComparison.Ordinal);
            Assert.True(File.Exists(ignoredScratchPath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task SnapshotDispatchDoesNotUseFinalizedDispatcher()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var finalizedDispatcher = new RecordingTradeFinalizedFileDispatcher();
            var snapshotDispatcher = new RecordingTradeSnapshotFileDispatcher();
            var clock = new StubClock(new DateTimeOffset(2026, 3, 12, 14, 15, 16, 789, TimeSpan.Zero));
            var (lifecycle, _, _) = CreateLifecycle(localRoot, finalizedDispatcher, snapshotDispatcher, clock);

            await lifecycle.TrackAppendedTrades(
            [
                CreateTrade("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
                CreateTrade("2", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                CreateTrade("3", new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero)),
            ], CancellationToken.None);
            await lifecycle.CheckpointActive(CancellationToken.None);

            await lifecycle.DispatchActiveSnapshot(CancellationToken.None);

            Assert.Empty(finalizedDispatcher.Dispatches);
            Assert.Single(snapshotDispatcher.Dispatches);
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    private static (TradeScratchCheckpointLifecycle Lifecycle, InMemoryIngestionStateStore StateStore, Mock<ILogMachina<TradeScratchCheckpointLifecycle>> LogMoq) CreateLifecycle(
        string localRoot,
        ITradeFinalizedFileDispatcher? tradeFinalizedFileDispatcher = null,
        ITradeSnapshotFileDispatcher? tradeSnapshotFileDispatcher = null,
        IClock? clock = null,
        ITradeGapScanner? tradeGapScanner = null,
        ITradeGapHealer? tradeGapHealer = null,
        ITradeDayBoundaryResolver? tradeDayBoundaryResolver = null)
    {
        var logMoq = new Mock<ILogMachina<TradeScratchCheckpointLifecycle>>();
        var stateStore = new InMemoryIngestionStateStore();
        var result = new TradeScratchCheckpointLifecycle(
            clock ?? new StubClock(new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
            localRoot,
            tradeFinalizedFileDispatcher ?? new TradeSinkNull(),
            tradeSnapshotFileDispatcher ?? new TradeSinkNull(),
            stateStore,
            tradeGapScanner ?? new LocalFileTradeGapScanner(),
            tradeGapHealer,
            tradeDayBoundaryResolver,
            logMoq.Object);
        return (result, stateStore, logMoq);
    }

    private static Mock<ITradeDayBoundaryResolver> CreateBoundaryResolverMock(TradeDayBoundary boundary)
    {
        var result = new Mock<ITradeDayBoundaryResolver>(MockBehavior.Strict);
        result
            .Setup(resolver => resolver.Resolve(StubExchange, boundary.Symbol, boundary.UtcDate, TradeDayBoundaryResolutionMode.BestEffort, It.IsAny<CancellationToken>()))
            .ReturnsAsync(boundary);
        return result;
    }

    private static TradeInfo CreateTrade(string tradeId, DateTimeOffset timestamp, string instrumentText = "BTC-USDT")
    {
        var key = new TradeKey(StubExchange, Instrument.Parse(instrumentText), tradeId);
        var result = new TradeInfo(key, timestamp, 1m, 1m, buyerIsMaker: false);
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

        public ValueTask DispatchAsync(ExchangeId exchange, Instrument instrument, DateOnly utcDate, string finalizedFilePath, CancellationToken cancellationToken)
        {
            Dispatches.Add(new DispatchCall(exchange, instrument, utcDate, finalizedFilePath, File.Exists(finalizedFilePath)));
            return ValueTask.CompletedTask;
        }
    }

    private sealed record DispatchCall(ExchangeId Exchange, Instrument Instrument, DateOnly UtcDate, string FinalizedFilePath, bool FileExistedAtDispatch);

    private sealed class RecordingTradeSnapshotFileDispatcher : ITradeSnapshotFileDispatcher
    {
        public List<SnapshotDispatchCall> Dispatches { get; } = [];

        public ValueTask DispatchAsync(ExchangeId exchange, Instrument instrument, string snapshotFilePath, CancellationToken cancellationToken)
        {
            Dispatches.Add(new SnapshotDispatchCall(exchange, instrument, snapshotFilePath, File.Exists(snapshotFilePath)));
            return ValueTask.CompletedTask;
        }
    }

    private sealed class StubClock(DateTimeOffset utcNow) : IClock
    {
        public DateTimeOffset UtcNow { get; } = utcNow;
    }

    private sealed record SnapshotDispatchCall(ExchangeId Exchange, Instrument Instrument, string SnapshotFilePath, bool FileExistedAtDispatch);

    private sealed class RecordingTradeGapFetchClient(IReadOnlyList<TradeInfo> trades) : ITradeGapFetchClient
    {
        public int FetchCallCount { get; private set; }

        public async ValueTask Fetch(
            Instrument instrument,
            long startId,
            long endId,
            ITradeGapFetchedPageSink pageSink,
            ITradeGapProgressReporter? progressReporter,
            CancellationToken terminator)
        {
            terminator.ThrowIfCancellationRequested();
            FetchCallCount++;

            var matchingTrades = trades
                .Where(trade => trade.Key.Symbol == instrument)
                .Where(trade => long.Parse(trade.Key.TradeId) >= startId && long.Parse(trade.Key.TradeId) <= endId)
                .OrderBy(trade => long.Parse(trade.Key.TradeId))
                .ToArray();

            if (matchingTrades.Length > 0)
            {
                await pageSink.AcceptPage(matchingTrades, terminator);
            }
        }
    }
}
