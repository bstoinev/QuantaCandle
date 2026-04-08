using LogMachina;

using Moq;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra.Pipeline;

namespace QuantaCandle.Infra.Tests.Pipeline;

/// <summary>
/// Verifies one-time startup discovery and dispatch of finalized local daily trade files.
/// </summary>
public sealed class TradeFinalizedFileStartupDispatcherTests
{
    [Fact]
    public async Task StartupDiscoveryDispatchesAlreadyExistingFinalizedFilesForConfiguredInstrument()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var configuredInstrument = Instrument.Parse("BTC-USDT");
            var otherInstrument = Instrument.Parse("ETH-USDT");
            var configuredPath = TradeLocalDailyFilePath.Build(localRoot, configuredInstrument, new DateOnly(2026, 3, 11));
            var otherPath = TradeLocalDailyFilePath.Build(localRoot, otherInstrument, new DateOnly(2026, 3, 11));

            await WriteTradeFileAsync(configuredPath, CreateTrade("1", configuredInstrument, new DateTimeOffset(2026, 3, 11, 0, 0, 0, TimeSpan.Zero)));
            await WriteTradeFileAsync(otherPath, CreateTrade("2", otherInstrument, new DateTimeOffset(2026, 3, 11, 0, 0, 0, TimeSpan.Zero)));

            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var startupDispatcher = CreateStartupDispatcher(localRoot, dispatcher);

            await startupDispatcher.Run([configuredInstrument], CancellationToken.None);

            var dispatch = Assert.Single(dispatcher.Dispatches);
            Assert.Equal(configuredInstrument, dispatch.Instrument);
            Assert.Equal(new DateOnly(2026, 3, 11), dispatch.UtcDate);
            Assert.Equal(configuredPath, dispatch.FinalizedFilePath);
            Assert.True(File.Exists(configuredPath));
            Assert.True(File.Exists(otherPath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task StartupDiscoveryExcludesQcScratchJsonl()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var instrument = Instrument.Parse("BTC-USDT");
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, instrument);

            await WriteTradeFileAsync(scratchPath, CreateTrade("1", instrument, new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));

            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var startupDispatcher = CreateStartupDispatcher(localRoot, dispatcher);

            await startupDispatcher.Run([instrument], CancellationToken.None);

            Assert.Empty(dispatcher.Dispatches);
            Assert.True(File.Exists(scratchPath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task StartupDiscoveryDispatchesMultipleFinalizedFilesInAscendingUtcDateOrder()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var instrument = Instrument.Parse("BTC-USDT");
            var newestPath = TradeLocalDailyFilePath.Build(localRoot, instrument, new DateOnly(2026, 3, 13));
            var oldestPath = TradeLocalDailyFilePath.Build(localRoot, instrument, new DateOnly(2026, 3, 11));
            var middlePath = TradeLocalDailyFilePath.Build(localRoot, instrument, new DateOnly(2026, 3, 12));

            await WriteTradeFileAsync(newestPath, CreateTrade("3", instrument, new DateTimeOffset(2026, 3, 13, 0, 0, 0, TimeSpan.Zero)));
            await WriteTradeFileAsync(oldestPath, CreateTrade("1", instrument, new DateTimeOffset(2026, 3, 11, 0, 0, 0, TimeSpan.Zero)));
            await WriteTradeFileAsync(middlePath, CreateTrade("2", instrument, new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)));

            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var startupDispatcher = CreateStartupDispatcher(localRoot, dispatcher);

            await startupDispatcher.Run([instrument], CancellationToken.None);

            Assert.Equal(
            [
                new DateOnly(2026, 3, 11),
                new DateOnly(2026, 3, 12),
                new DateOnly(2026, 3, 13),
            ],
                dispatcher.Dispatches.Select(item => item.UtcDate).ToArray());
            Assert.Equal(
            [
                oldestPath,
                middlePath,
                newestPath,
            ],
                dispatcher.Dispatches.Select(item => item.FinalizedFilePath).ToArray());
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    [Fact]
    public async Task StartupDiscoveryDoesNotForceFinalizeRecoveredSameDayScratch()
    {
        var localRoot = CreateTempDirectory();

        try
        {
            var instrument = Instrument.Parse("BTC-USDT");
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(localRoot, instrument);
            var finalizedPath = TradeLocalDailyFilePath.Build(localRoot, instrument, new DateOnly(2026, 3, 12));

            await WriteTradeFileAsync(scratchPath, CreateTrade("1", instrument, new DateTimeOffset(2026, 3, 12, 12, 0, 0, TimeSpan.Zero)));

            var dispatcher = new RecordingTradeFinalizedFileDispatcher();
            var startupDispatcher = CreateStartupDispatcher(localRoot, dispatcher);

            await startupDispatcher.Run([instrument], CancellationToken.None);

            Assert.Empty(dispatcher.Dispatches);
            Assert.True(File.Exists(scratchPath));
            Assert.False(File.Exists(finalizedPath));
        }
        finally
        {
            DeleteDirectory(localRoot);
        }
    }

    private static TradeFinalizedFileStartupDispatcher CreateStartupDispatcher(
        string localRoot,
        ITradeFinalizedFileDispatcher dispatcher)
    {
        var logMoq = new Mock<ILogMachina<TradeFinalizedFileStartupDispatcher>>();
        var result = new TradeFinalizedFileStartupDispatcher(localRoot, dispatcher, logMoq.Object);
        return result;
    }

    private static TradeInfo CreateTrade(string tradeId, Instrument instrument, DateTimeOffset timestamp)
    {
        var key = new TradeKey(new ExchangeId("Stub"), instrument, tradeId);
        var result = new TradeInfo(key, timestamp, 1m, 1m);
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

    private static async Task WriteTradeFileAsync(string path, TradeInfo trade)
    {
        var directory = Path.GetDirectoryName(path);

        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await File.WriteAllTextAsync(path, TradeJsonlFile.BuildPayload([trade]), CancellationToken.None);
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
