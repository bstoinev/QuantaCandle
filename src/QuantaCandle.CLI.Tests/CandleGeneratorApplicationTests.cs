using Moq;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra;

namespace QuantaCandle.CLI.Tests;

/// <summary>
/// Verifies command dispatch for the CLI workflow.
/// </summary>
public sealed class CandleGeneratorApplicationTests
{
    [Fact]
    public async Task ScanModeDispatchesToScannerAndPrintsGapSummary()
    {
        var workDirectory = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        var exchangeDirectory = Path.Combine(workDirectory, "Binance");
        var instrumentDirectory = Path.Combine(exchangeDirectory, "BTC-USDT");
        var generationRunnerMoq = new Mock<ICandleGenerationRunner>(MockBehavior.Strict);
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanResult = new TradeGapScanResult(
            2,
            4,
            0,
            [
                TradeGap
                    .CreateOpen(
                        Guid.NewGuid(),
                        new ExchangeId("binance"),
                        Instrument.Parse("BTC-USDT"),
                        new TradeWatermark("100", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                        new DateTimeOffset(2026, 3, 12, 0, 0, 4, TimeSpan.Zero))
                    .ToBounded(
                        new TradeWatermark("103", new DateTimeOffset(2026, 3, 12, 0, 0, 4, TimeSpan.Zero)),
                        new MissingTradeIdRange(101, 102)),
            ],
            [
                new TradeGapAffectedFile(Path.Combine("BTC-USDT", "2026-03-12-a.jsonl"), null),
                new TradeGapAffectedFile(Path.Combine("BTC-USDT", "2026-03-12-b.jsonl"), null),
            ],
            [
                new TradeGapAffectedRange(
                    new TradeWatermark("100", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                    new TradeWatermark("103", new DateTimeOffset(2026, 3, 12, 0, 0, 4, TimeSpan.Zero)),
                    new TradeGapBoundaryLocation(Path.Combine("BTC-USDT", "2026-03-12-a.jsonl"), 2),
                    new TradeGapBoundaryLocation(Path.Combine("BTC-USDT", "2026-03-12-b.jsonl"), 1)),
            ]);
        TradeGapScanRequest? capturedRequest = null;

        scannerMoq
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapScanRequest, CancellationToken>((request, _) => capturedRequest = request)
            .Returns(new ValueTask<TradeGapScanResult>(scanResult));

        var app = new CliApplication(generationRunnerMoq.Object, scannerMoq.Object, healerMoq.Object);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-12.jsonl"), string.Empty, CancellationToken.None);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-13.jsonl"), string.Empty, CancellationToken.None);

            var exitCode = await app.Run(
                ["scan", "btc-usdt", "--workDir", workDirectory],
                outputWriter,
                errorWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.NotNull(capturedRequest);
            Assert.Equal(Path.GetFullPath(workDirectory), capturedRequest!.RootDirectory);
            Assert.All(capturedRequest.CandidateFiles, file => Assert.StartsWith(Path.Combine("Binance", "BTC-USDT"), file.Path, StringComparison.Ordinal));
            Assert.Contains("Files scanned:", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("Trades scanned:", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("Gaps found:", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("exchange=binance", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("instrument=BTC-USDT", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("missing=101-102", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains(
                Path.Combine("BTC-USDT", "2026-03-12-a.jsonl") + ":2 -> " + Path.Combine("BTC-USDT", "2026-03-12-b.jsonl") + ":1",
                outputWriter.ToString(),
                StringComparison.Ordinal);
            Assert.Equal(string.Empty, errorWriter.ToString());
            generationRunnerMoq.Verify(
                mock => mock.GenerateAsync(It.IsAny<CliOptions>(), It.IsAny<CancellationToken>()),
                Times.Never);
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Once);
            healerMoq.Verify(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        }
        finally
        {
            if (Directory.Exists(workDirectory))
            {
                Directory.Delete(workDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task ScanModeResolvesRequestedDatesIntoCandidateFiles()
    {
        var workDirectory = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        var instrumentDirectory = Path.Combine(workDirectory, "Binance", "BTC-USDT");
        var generationRunnerMoq = new Mock<ICandleGenerationRunner>(MockBehavior.Strict);
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        TradeGapScanRequest? capturedRequest = null;

        scannerMoq
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapScanRequest, CancellationToken>((request, _) => capturedRequest = request)
            .Returns(new ValueTask<TradeGapScanResult>(new TradeGapScanResult(1, 0, 0, [], [], [])));

        var app = new CliApplication(generationRunnerMoq.Object, scannerMoq.Object, healerMoq.Object);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-12.jsonl"), string.Empty, CancellationToken.None);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-13.jsonl"), string.Empty, CancellationToken.None);

            var exitCode = await app.Run(
                ["scan", "btc-usdt", "--workDir", workDirectory, "--dates", "20260312"],
                outputWriter,
                errorWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.NotNull(capturedRequest);
            var candidateFile = Assert.Single(capturedRequest!.CandidateFiles);
            Assert.Equal(Path.Combine("Binance", "BTC-USDT", "2026-03-12.jsonl"), candidateFile.Path);
            Assert.Equal(new DateOnly(2026, 3, 12), candidateFile.TradingDay);
        }
        finally
        {
            if (Directory.Exists(workDirectory))
            {
                Directory.Delete(workDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task HealModeDispatchesBoundedGapsToHealer()
    {
        var workDirectory = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        var instrumentDirectory = Path.Combine(workDirectory, "Binance", "BTC-USDT");
        var generationRunnerMoq = new Mock<ICandleGenerationRunner>(MockBehavior.Strict);
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        TradeGapHealRequest? capturedRequest = null;
        var gap = TradeGap
            .CreateOpen(
                Guid.NewGuid(),
                new ExchangeId("Binance"),
                Instrument.Parse("BTC-USDT"),
                new TradeWatermark("100", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                new DateTimeOffset(2026, 3, 12, 0, 0, 4, TimeSpan.Zero))
            .ToBounded(
                new TradeWatermark("103", new DateTimeOffset(2026, 3, 12, 0, 0, 4, TimeSpan.Zero)),
                new MissingTradeIdRange(101, 102));
        var scanResult = new TradeGapScanResult(
            1,
            4,
            0,
            [gap],
            [new TradeGapAffectedFile(Path.Combine("BTC-USDT", "2026-03-12.jsonl"), new DateOnly(2026, 3, 12))],
            [
                new TradeGapAffectedRange(
                    new TradeWatermark("100", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                    new TradeWatermark("103", new DateTimeOffset(2026, 3, 12, 0, 0, 4, TimeSpan.Zero)),
                    new TradeGapBoundaryLocation(Path.Combine("BTC-USDT", "2026-03-12.jsonl"), 2),
                    new TradeGapBoundaryLocation(Path.Combine("BTC-USDT", "2026-03-12.jsonl"), 3)),
            ]);

        scannerMoq
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(scanResult));
        healerMoq
            .Setup(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapHealRequest, CancellationToken>((request, _) => capturedRequest = request)
            .Returns(
                new ValueTask<TradeGapHealResult>(
                    new TradeGapHealResult(
                        new ExchangeId("Binance"),
                        Instrument.Parse("BTC-USDT"),
                        TradeGapHealStatus.Full,
                        new MissingTradeIdRange(101, 102),
                        2,
                        2,
                        true,
                        [],
                        [],
                        [new TradeGapAffectedFile(Path.Combine("BTC-USDT", "2026-03-12.jsonl"), new DateOnly(2026, 3, 12))],
                        [])));

        var app = new CliApplication(generationRunnerMoq.Object, scannerMoq.Object, healerMoq.Object);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-12.jsonl"), string.Empty, CancellationToken.None);

            var exitCode = await app.Run(
                ["heal", "btc-usdt", "--workDir", workDirectory],
                outputWriter,
                errorWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.NotNull(capturedRequest);
            Assert.Equal(Path.GetFullPath(workDirectory), capturedRequest!.RootDirectory);
            Assert.Equal(new ExchangeId("Binance"), capturedRequest.Exchange);
            Assert.Equal(Instrument.Parse("BTC-USDT"), capturedRequest.Symbol);
            Assert.Equal(101, capturedRequest.MissingTradeIdStart);
            Assert.Equal(102, capturedRequest.MissingTradeIdEnd);
            Assert.Contains("Gaps healed full:", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Equal(string.Empty, errorWriter.ToString());
        }
        finally
        {
            if (Directory.Exists(workDirectory))
            {
                Directory.Delete(workDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task ScanModeWithRequestedDatesFailsExplicitlyWhenFilesAreMissing()
    {
        var workDirectory = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        var generationRunnerMoq = new Mock<ICandleGenerationRunner>(MockBehavior.Strict);
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var app = new CliApplication(generationRunnerMoq.Object, scannerMoq.Object, healerMoq.Object);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(Path.Combine(workDirectory, "Binance", "BTC-USDT"));

            var exitCode = await app.Run(
                ["scan", "btc-usdt", "--workDir", workDirectory, "--dates", "2026-04-09"],
                outputWriter,
                errorWriter,
                CancellationToken.None);

            Assert.Equal(2, exitCode);
            Assert.Equal(string.Empty, outputWriter.ToString());
            Assert.Contains("exchange 'Binance'", errorWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("instrument 'BTC-USDT'", errorWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("requested date(s) [2026-04-09]", errorWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("missing date(s) [2026-04-09]", errorWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains($"root directory '{Path.GetFullPath(workDirectory)}'", errorWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains(
                TradeLocalDailyFilePath.Build(Path.GetFullPath(workDirectory), new ExchangeId("Binance"), Instrument.Parse("BTC-USDT"), new DateOnly(2026, 4, 9)),
                errorWriter.ToString(),
                StringComparison.Ordinal);
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        }
        finally
        {
            if (Directory.Exists(workDirectory))
            {
                Directory.Delete(workDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task ScanModeWithPartialRequestedDatesFailsAndMentionsMissingDate()
    {
        var workDirectory = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        var instrumentDirectory = Path.Combine(workDirectory, "Binance", "BTC-USDT");
        var generationRunnerMoq = new Mock<ICandleGenerationRunner>(MockBehavior.Strict);
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var app = new CliApplication(generationRunnerMoq.Object, scannerMoq.Object, healerMoq.Object);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-04-09.jsonl"), string.Empty, CancellationToken.None);

            var exitCode = await app.Run(
                ["scan", "btc-usdt", "--workDir", workDirectory, "--dates", "2026-04-09,2026-04-10"],
                outputWriter,
                errorWriter,
                CancellationToken.None);

            Assert.Equal(2, exitCode);
            Assert.Equal(string.Empty, outputWriter.ToString());
            Assert.Contains("requested date(s) [2026-04-09, 2026-04-10]", errorWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("missing date(s) [2026-04-10]", errorWriter.ToString(), StringComparison.Ordinal);
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        }
        finally
        {
            if (Directory.Exists(workDirectory))
            {
                Directory.Delete(workDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task HealModeWithRequestedDatesFailsExplicitlyWhenFilesAreMissing()
    {
        var workDirectory = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        var generationRunnerMoq = new Mock<ICandleGenerationRunner>(MockBehavior.Strict);
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var app = new CliApplication(generationRunnerMoq.Object, scannerMoq.Object, healerMoq.Object);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(Path.Combine(workDirectory, "Binance", "BTC-USDT"));

            var exitCode = await app.Run(
                ["heal", "btc-usdt", "--workDir", workDirectory, "--dates", "2026-04-09"],
                outputWriter,
                errorWriter,
                CancellationToken.None);

            Assert.Equal(2, exitCode);
            Assert.Equal(string.Empty, outputWriter.ToString());
            Assert.Contains("exchange 'Binance'", errorWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("instrument 'BTC-USDT'", errorWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("missing date(s) [2026-04-09]", errorWriter.ToString(), StringComparison.Ordinal);
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Never);
            healerMoq.Verify(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        }
        finally
        {
            if (Directory.Exists(workDirectory))
            {
                Directory.Delete(workDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task ScanModeWithoutDatesRemainsDiscoveryModeWhenNoFilesExist()
    {
        var workDirectory = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        var generationRunnerMoq = new Mock<ICandleGenerationRunner>(MockBehavior.Strict);
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        TradeGapScanRequest? capturedRequest = null;

        scannerMoq
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapScanRequest, CancellationToken>((request, _) => capturedRequest = request)
            .Returns(new ValueTask<TradeGapScanResult>(new TradeGapScanResult(0, 0, 0, [], [], [])));

        var app = new CliApplication(generationRunnerMoq.Object, scannerMoq.Object, healerMoq.Object);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        try
        {
            var exitCode = await app.Run(
                ["scan", "btc-usdt", "--workDir", workDirectory],
                outputWriter,
                errorWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.NotNull(capturedRequest);
            Assert.Empty(capturedRequest!.CandidateFiles);
            Assert.Contains("Files scanned:", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Equal(string.Empty, errorWriter.ToString());
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Once);
        }
        finally
        {
            if (Directory.Exists(workDirectory))
            {
                Directory.Delete(workDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task HelpWritesNewCliUsage()
    {
        var generationRunnerMoq = new Mock<ICandleGenerationRunner>(MockBehavior.Strict);
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var app = new CliApplication(generationRunnerMoq.Object, scannerMoq.Object, healerMoq.Object);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        var exitCode = await app.Run(["--help"], outputWriter, errorWriter, CancellationToken.None);

        Assert.Equal(0, exitCode);
        Assert.Contains("Quanta Candle CLI", outputWriter.ToString(), StringComparison.Ordinal);
        Assert.Contains("qc heal <instrument>", outputWriter.ToString(), StringComparison.Ordinal);
        Assert.Equal(string.Empty, errorWriter.ToString());
    }

    [Fact]
    public async Task CandlizeModePassesWorkDirectoryInstrumentAndDatesWithoutTempStaging()
    {
        var workDirectory = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        var legacyTempDirectory = Path.Combine(Path.GetTempPath(), "QuantaCandle.CLI");
        var generationRunnerMoq = new Mock<ICandleGenerationRunner>(MockBehavior.Strict);
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        CliOptions? capturedOptions = null;

        generationRunnerMoq
            .Setup(mock => mock.GenerateAsync(It.IsAny<CliOptions>(), It.IsAny<CancellationToken>()))
            .Callback<CliOptions, CancellationToken>((options, _) => capturedOptions = options)
            .Returns(Task.FromResult(new CliResult(2, 2, 0, 2, 2)));

        var app = new CliApplication(generationRunnerMoq.Object, scannerMoq.Object, healerMoq.Object);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        try
        {
            if (Directory.Exists(legacyTempDirectory))
            {
                Directory.Delete(legacyTempDirectory, recursive: true);
            }

            var exitCode = await app.Run(
                ["candlize", "btc-usdt", "--workDir", workDirectory, "--dates", "20260330,20260401"],
                outputWriter,
                errorWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.NotNull(capturedOptions);
            Assert.Equal(workDirectory, capturedOptions!.WorkDirectory);
            Assert.Equal("Binance", capturedOptions.Exchange);
            Assert.Equal("BTC-USDT", capturedOptions.Instrument);
            Assert.Equal(
                [
                    new DateOnly(2026, 3, 30),
                    new DateOnly(2026, 4, 1),
                ],
                capturedOptions.Dates);
            Assert.False(Directory.Exists(legacyTempDirectory));
            Assert.Equal(string.Empty, errorWriter.ToString());
        }
        finally
        {
            if (Directory.Exists(workDirectory))
            {
                Directory.Delete(workDirectory, recursive: true);
            }

            if (Directory.Exists(legacyTempDirectory))
            {
                Directory.Delete(legacyTempDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task CandlizeModeDefaultsExchangeToBinanceWhenOmitted()
    {
        var workDirectory = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        var generationRunnerMoq = new Mock<ICandleGenerationRunner>(MockBehavior.Strict);
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        CliOptions? capturedOptions = null;

        generationRunnerMoq
            .Setup(mock => mock.GenerateAsync(It.IsAny<CliOptions>(), It.IsAny<CancellationToken>()))
            .Callback<CliOptions, CancellationToken>((options, _) => capturedOptions = options)
            .Returns(Task.FromResult(new CliResult(0, 0, 0, 0, 0)));

        var app = new CliApplication(generationRunnerMoq.Object, scannerMoq.Object, healerMoq.Object);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        try
        {
            var exitCode = await app.Run(
                ["candlize", "btc-usdt", "--workDir", workDirectory],
                outputWriter,
                errorWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.NotNull(capturedOptions);
            Assert.Equal("Binance", capturedOptions!.Exchange);
            Assert.Equal(string.Empty, errorWriter.ToString());
        }
        finally
        {
            if (Directory.Exists(workDirectory))
            {
                Directory.Delete(workDirectory, recursive: true);
            }
        }
    }
}
