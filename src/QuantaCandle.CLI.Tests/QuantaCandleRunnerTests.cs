using System.Globalization;
using System.Text.Json;

using Moq;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra;

namespace QuantaCandle.CLI.Tests;

/// <summary>
/// Verifies runner behavior independently from CLI parsing and entrypoint dispatch.
/// </summary>
public sealed class QuantaCandleRunnerTests
{
    [Fact]
    public async Task CandlizeWritesGenerationStatistics()
    {
        var root = CreateTempRoot();
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            await WriteTradeFileAsync(
                root,
                "Binance",
                "BTC-USDT",
                "2026-03-12.jsonl",
                Trade("Binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.5m),
                Trade("Binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.5m),
                Trade("Binance", "BTC-USDT", "2", "2026-03-12T12:00:40Z", 101m, 0.25m));

            var exitCode = await sut.Candlize(
                new CliOptions(CliMode.Candlize, root, "Binance", "BTC-USDT", string.Empty, []),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.Contains("Input trades:", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("Unique trades:", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("Duplicates dropped:", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("Candles written:", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("Output files:", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("3", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("2", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("1", outputWriter.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task ScanResolvesRequestedDatesIntoCandidateFiles()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "Binance", "BTC-USDT");
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        TradeGapScanRequest? capturedRequest = null;

        scannerMoq
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapScanRequest, CancellationToken>((request, _) => capturedRequest = request)
            .Returns(new ValueTask<TradeGapScanResult>(new TradeGapScanResult(1, 0, 0, [], [], [])));

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-12.jsonl"), string.Empty, CancellationToken.None);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-13.jsonl"), string.Empty, CancellationToken.None);

            var exitCode = await sut.Scan(
                new CliOptions(CliMode.Scan, workDirectory, "Binance", "BTC-USDT", string.Empty, [new DateOnly(2026, 3, 12)]),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.NotNull(capturedRequest);
            var candidateFile = Assert.Single(capturedRequest!.CandidateFiles);
            Assert.Equal(Path.Combine("Binance", "BTC-USDT", "2026-03-12.jsonl"), candidateFile.Path);
            Assert.Equal(new DateOnly(2026, 3, 12), candidateFile.TradingDay);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task ScanPrintsGapSummary()
    {
        TradeGapScanRequest? capturedRequest = null;
        var scanResult = new TradeGapScanResult(
            2,
            4,
            0,
            [
                CreateBoundedGap("binance", "BTC-USDT", 101, 102),
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
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();

        scannerMoq
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapScanRequest, CancellationToken>((request, _) => capturedRequest = request)
            .Returns(new ValueTask<TradeGapScanResult>(scanResult));

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();
        var workDirectory = CreateTempRoot();

        try
        {
            var exitCode = await sut.Scan(
                new CliOptions(CliMode.Scan, workDirectory, "Binance", "BTC-USDT", string.Empty, []),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.NotNull(capturedRequest);
            Assert.Equal(Path.GetFullPath(workDirectory), capturedRequest!.RootDirectory);
            Assert.Contains("Files scanned:", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("Trades scanned:", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("Gaps found:", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("File 1: path=", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("exists=false", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("boundary-start=ok", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("boundary-end=ok", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("kind=interior", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("exchange=binance", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("instrument=BTC-USDT", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("missing=101-102", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains(
                Path.Combine("BTC-USDT", "2026-03-12-a.jsonl") + ":2 -> " + Path.Combine("BTC-USDT", "2026-03-12-b.jsonl") + ":1",
                outputWriter.ToString(),
                StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task HealDispatchesBoundedGapsToHealer()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "Binance", "BTC-USDT");
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        TradeGapHealRequest? capturedRequest = null;
        var gap = CreateBoundedGap("Binance", "BTC-USDT", 101, 102);
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

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-12.jsonl"), string.Empty, CancellationToken.None);

            var exitCode = await sut.Heal(
                new CliOptions(CliMode.Heal, workDirectory, "Binance", "BTC-USDT", string.Empty, []),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.NotNull(capturedRequest);
            Assert.Equal(Path.GetFullPath(workDirectory), capturedRequest!.RootDirectory);
            Assert.Equal(new ExchangeId("Binance"), capturedRequest.Exchange);
            Assert.Equal(Instrument.Parse("BTC-USDT"), capturedRequest.Symbol);
            Assert.Equal(101, capturedRequest.MissingTradeIdStart);
            Assert.Equal(102, capturedRequest.MissingTradeIdEnd);
            Assert.Contains("Gaps healed full:", outputWriter.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task ScanWithRequestedDatesFailsExplicitlyWhenFilesAreMissing()
    {
        var workDirectory = CreateTempRoot();
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(Path.Combine(workDirectory, "Binance", "BTC-USDT"));

            var exception = await Assert.ThrowsAsync<ArgumentException>(() => sut.Scan(
                new CliOptions(CliMode.Scan, workDirectory, "Binance", "BTC-USDT", string.Empty, [new DateOnly(2026, 4, 9)]),
                outputWriter,
                CancellationToken.None));

            Assert.Contains("exchange 'Binance'", exception.Message, StringComparison.Ordinal);
            Assert.Contains("instrument 'BTC-USDT'", exception.Message, StringComparison.Ordinal);
            Assert.Contains("requested date(s) [2026-04-09]", exception.Message, StringComparison.Ordinal);
            Assert.Contains("missing date(s) [2026-04-09]", exception.Message, StringComparison.Ordinal);
            Assert.Contains($"root directory '{Path.GetFullPath(workDirectory)}'", exception.Message, StringComparison.Ordinal);
            Assert.Contains(
                TradeLocalDailyFilePath.Build(Path.GetFullPath(workDirectory), new ExchangeId("Binance"), Instrument.Parse("BTC-USDT"), new DateOnly(2026, 4, 9)),
                exception.Message,
                StringComparison.Ordinal);
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task ScanWithPartialRequestedDatesFailsAndMentionsMissingDate()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "Binance", "BTC-USDT");
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-04-09.jsonl"), string.Empty, CancellationToken.None);

            var exception = await Assert.ThrowsAsync<ArgumentException>(() => sut.Scan(
                new CliOptions(
                    CliMode.Scan,
                    workDirectory,
                    "Binance",
                    "BTC-USDT",
                    string.Empty,
                    [
                        new DateOnly(2026, 4, 9),
                        new DateOnly(2026, 4, 10),
                    ]),
                outputWriter,
                CancellationToken.None));

            Assert.Contains("requested date(s) [2026-04-09, 2026-04-10]", exception.Message, StringComparison.Ordinal);
            Assert.Contains("missing date(s) [2026-04-10]", exception.Message, StringComparison.Ordinal);
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task HealWithRequestedDatesFailsExplicitlyWhenFilesAreMissing()
    {
        var workDirectory = CreateTempRoot();
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(Path.Combine(workDirectory, "Binance", "BTC-USDT"));

            var exception = await Assert.ThrowsAsync<ArgumentException>(() => sut.Heal(
                new CliOptions(CliMode.Heal, workDirectory, "Binance", "BTC-USDT", string.Empty, [new DateOnly(2026, 4, 9)]),
                outputWriter,
                CancellationToken.None));

            Assert.Contains("exchange 'Binance'", exception.Message, StringComparison.Ordinal);
            Assert.Contains("instrument 'BTC-USDT'", exception.Message, StringComparison.Ordinal);
            Assert.Contains("missing date(s) [2026-04-09]", exception.Message, StringComparison.Ordinal);
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Never);
            healerMoq.Verify(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task ScanWithoutDatesRemainsDiscoveryModeWhenNoFilesExist()
    {
        var workDirectory = CreateTempRoot();
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        TradeGapScanRequest? capturedRequest = null;

        scannerMoq
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapScanRequest, CancellationToken>((request, _) => capturedRequest = request)
            .Returns(new ValueTask<TradeGapScanResult>(new TradeGapScanResult(0, 0, 0, [], [], [])));

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            var exitCode = await sut.Scan(
                new CliOptions(CliMode.Scan, workDirectory, "Binance", "BTC-USDT", string.Empty, []),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.NotNull(capturedRequest);
            Assert.Empty(capturedRequest!.CandidateFiles);
            Assert.Contains("Files scanned:", outputWriter.ToString(), StringComparison.Ordinal);
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Once);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    private static TradeGap CreateBoundedGap(string exchange, string instrument, long missingTradeIdStart, long missingTradeIdEnd)
    {
        var result = TradeGap
            .CreateOpen(
                Guid.NewGuid(),
                new ExchangeId(exchange),
                Instrument.Parse(instrument),
                new TradeWatermark("100", new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero)),
                new DateTimeOffset(2026, 3, 12, 0, 0, 4, TimeSpan.Zero))
            .ToBounded(
                new TradeWatermark("103", new DateTimeOffset(2026, 3, 12, 0, 0, 4, TimeSpan.Zero)),
                new MissingTradeIdRange(missingTradeIdStart, missingTradeIdEnd));

        return result;
    }

    private static Mock<ITradeGapScanAugmenter> CreatePassThroughScanAugmenterMock()
    {
        var result = new Mock<ITradeGapScanAugmenter>(MockBehavior.Strict);
        result
            .Setup(mock => mock.Augment(It.IsAny<TradeGapScanRequest>(), It.IsAny<TradeGapScanResult>(), It.IsAny<CancellationToken>()))
            .Returns<TradeGapScanRequest, TradeGapScanResult, CancellationToken>((_, scanResult, _) => ValueTask.FromResult(scanResult));
        return result;
    }

    private static string CreateTempRoot()
    {
        var result = Path.Combine(Path.GetTempPath(), "QuantaCandle.CLI.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(result);
        return result;
    }

    private static void DeleteDirectoryIfExists(string path)
    {
        if (Directory.Exists(path))
        {
            Directory.Delete(path, recursive: true);
        }
    }

    private static object Trade(string exchange, string instrument, string tradeId, string timestamp, decimal price, decimal quantity)
    {
        var result = new
        {
            exchange,
            instrument,
            tradeId,
            timestamp = DateTimeOffset.Parse(timestamp, CultureInfo.InvariantCulture),
            price,
            quantity,
        };

        return result;
    }

    private static async Task WriteTradeFileAsync(string workDirectory, string exchange, string instrument, string fileName, params object[] trades)
    {
        var directory = Path.Combine(workDirectory, "trades-out", exchange, instrument);
        Directory.CreateDirectory(directory);

        var path = Path.Combine(directory, fileName);
        var lines = trades.Select(static trade => JsonSerializer.Serialize(trade)).ToArray();
        var payload = string.Join(Environment.NewLine, lines) + Environment.NewLine;
        await File.WriteAllTextAsync(path, payload, CancellationToken.None).ConfigureAwait(false);
    }
}
