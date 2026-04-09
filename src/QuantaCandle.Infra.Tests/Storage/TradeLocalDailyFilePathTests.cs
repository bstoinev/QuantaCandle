using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Tests.Storage;

/// <summary>
/// Verifies exchange-aware recorder file path construction and discovery.
/// </summary>
public sealed class TradeLocalDailyFilePathTests
{
    private static readonly ExchangeId BinanceExchange = new("Binance");

    [Fact]
    public void BuildMethodsPlaceRecorderFilesUnderExchangeAndInstrumentDirectories()
    {
        var root = Path.Combine("C:\\data", "trade-data");
        var instrument = Instrument.Parse("BTC-USDT");

        var finalizedPath = TradeLocalDailyFilePath.Build(root, BinanceExchange, instrument, new DateOnly(2026, 3, 12));
        var scratchPath = TradeLocalDailyFilePath.BuildScratch(root, BinanceExchange, instrument);
        var snapshotPath = TradeLocalDailyFilePath.BuildSnapshot(root, BinanceExchange, instrument, new DateTimeOffset(2026, 3, 12, 14, 15, 16, 789, TimeSpan.Zero));

        Assert.Equal(Path.Combine(root, "Binance", "BTC-USDT", "2026-03-12.jsonl"), finalizedPath);
        Assert.Equal(Path.Combine(root, "Binance", "BTC-USDT", "qc-scratch.jsonl"), scratchPath);
        Assert.Equal(Path.Combine(root, "Binance", "BTC-USDT", "2026-03-12.141516789.jsonl"), snapshotPath);
    }

    [Fact]
    public async Task DiscoverCompletedScansOnlyTheExchangeAwareInstrumentDirectory()
    {
        var root = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));

        try
        {
            var instrument = Instrument.Parse("BTC-USDT");
            var expectedPath = TradeLocalDailyFilePath.Build(root, BinanceExchange, instrument, new DateOnly(2026, 3, 12));
            var wrongExchangePath = TradeLocalDailyFilePath.Build(root, new ExchangeId("Bybit"), instrument, new DateOnly(2026, 3, 12));
            var wrongInstrumentPath = TradeLocalDailyFilePath.Build(root, BinanceExchange, Instrument.Parse("ETH-USDT"), new DateOnly(2026, 3, 12));
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(root, BinanceExchange, instrument);

            Directory.CreateDirectory(Path.GetDirectoryName(expectedPath)!);
            Directory.CreateDirectory(Path.GetDirectoryName(wrongExchangePath)!);
            Directory.CreateDirectory(Path.GetDirectoryName(wrongInstrumentPath)!);
            await File.WriteAllTextAsync(expectedPath, "{}", CancellationToken.None);
            await File.WriteAllTextAsync(wrongExchangePath, "{}", CancellationToken.None);
            await File.WriteAllTextAsync(wrongInstrumentPath, "{}", CancellationToken.None);
            await File.WriteAllTextAsync(scratchPath, "{}", CancellationToken.None);

            var discoveredFiles = TradeLocalDailyFilePath.DiscoverCompleted(root, BinanceExchange, instrument);

            var discoveredFile = Assert.Single(discoveredFiles);
            Assert.Equal(BinanceExchange, discoveredFile.Exchange);
            Assert.Equal(instrument, discoveredFile.Instrument);
            Assert.Equal(new DateOnly(2026, 3, 12), discoveredFile.UtcDate);
            Assert.Equal(expectedPath, discoveredFile.Path);
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
