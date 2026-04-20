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
                new CliOptions(CliMode.Candlize, root, "Binance", "BTC-USDT", "1m", []),
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
    public async Task CandlizeWithMultipleFilesReportsProcessedFilesInOrder()
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
                Trade("Binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.5m));
            await WriteTradeFileAsync(
                root,
                "Binance",
                "BTC-USDT",
                "2026-03-13.jsonl",
                Trade("Binance", "BTC-USDT", "2", "2026-03-13T12:00:05Z", 101m, 0.25m));

            var exitCode = await sut.Candlize(
                new CliOptions(CliMode.Candlize, root, "Binance", "BTC-USDT", "1m", []),
                outputWriter,
                CancellationToken.None);

            var output = outputWriter.ToString();
            var firstLine = $"File 1/2: processing '{Path.Combine("binance", "BTC-USDT", "2026-03-12.jsonl")}'.";
            var secondLine = $"File 2/2: processing '{Path.Combine("binance", "BTC-USDT", "2026-03-13.jsonl")}'.";
            Assert.Equal(0, exitCode);
            Assert.True(output.IndexOf(firstLine, StringComparison.Ordinal) >= 0);
            Assert.True(output.IndexOf(secondLine, StringComparison.Ordinal) > output.IndexOf(firstLine, StringComparison.Ordinal));
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task CandlizeWithMultipleFilesAdvancesProgressIndicatorAsFilesComplete()
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
                Trade("Binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.5m));
            await WriteTradeFileAsync(
                root,
                "Binance",
                "BTC-USDT",
                "2026-03-13.jsonl",
                Trade("Binance", "BTC-USDT", "2", "2026-03-13T12:00:05Z", 101m, 0.25m));

            var exitCode = await sut.Candlize(
                new CliOptions(CliMode.Candlize, root, "Binance", "BTC-USDT", "1m", []),
                outputWriter,
                CancellationToken.None);

            var output = outputWriter.ToString();
            Assert.Equal(0, exitCode);
            Assert.Contains("Candlize progress: [############------------]  50% files 1/2", output, StringComparison.Ordinal);
            Assert.Contains("Candlize progress: [########################] 100% files 2/2", output, StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task CandlizeWithSingleFileReportsSensibleProgressOutput()
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
                Trade("Binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.5m));

            var exitCode = await sut.Candlize(
                new CliOptions(CliMode.Candlize, root, "Binance", "BTC-USDT", "1m", [new DateOnly(2026, 3, 12)]),
                outputWriter,
                CancellationToken.None);

            var output = outputWriter.ToString();
            Assert.Equal(0, exitCode);
            Assert.Contains($"File 1/1: processing '{Path.Combine("binance", "BTC-USDT", "2026-03-12.jsonl")}'.", output, StringComparison.Ordinal);
            Assert.Contains("Candlize progress: [########################] 100% files 1/1", output, StringComparison.Ordinal);
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
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");
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
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            var exitCode = await sut.Scan(
                new CliOptions(CliMode.Scan, workDirectory, "Binance", "BTC-USDT", string.Empty, []),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.NotNull(capturedRequest);
            Assert.Equal(Path.Combine(Path.GetFullPath(workDirectory), "trade-data"), capturedRequest!.RootDirectory);
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
    public async Task HealDispatchesSingleDateGapToHealer()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");
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
            Assert.Equal(Path.Combine(Path.GetFullPath(workDirectory), "trade-data"), capturedRequest!.RootDirectory);
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
    public async Task HealRunsBoundaryPassBeforeInteriorPassForSingleFile()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = new Mock<ITradeGapScanAugmenter>(MockBehavior.Strict);
        var healRequests = new List<TradeGapHealRequest>();
        var relativePath = Path.Combine("Binance", "BTC-USDT", "2026-03-12.jsonl");
        var interiorGap = CreateGapWithRange(
            "Binance",
            "BTC-USDT",
            103,
            103,
            relativePath,
            2,
            3,
            "102",
            "104");
        var startBoundaryGap = CreateGapWithRange(
            "Binance",
            "BTC-USDT",
            100,
            101,
            relativePath,
            1,
            1,
            "102",
            "102");
        var endBoundaryGap = CreateGapWithRange(
            "Binance",
            "BTC-USDT",
            105,
            106,
            relativePath,
            4,
            4,
            "104",
            "104",
            toInclusiveTradeId: "106");
        var baseScanResult = new TradeGapScanResult(
            1,
            4,
            0,
            [interiorGap.Gap],
            [new TradeGapAffectedFile(relativePath, new DateOnly(2026, 3, 12))],
            [interiorGap.Range!]);
        var augmentedScanResult = new TradeGapScanResult(
            1,
            4,
            0,
            [interiorGap.Gap, startBoundaryGap.Gap, endBoundaryGap.Gap],
            [new TradeGapAffectedFile(relativePath, new DateOnly(2026, 3, 12))],
            [interiorGap.Range!, startBoundaryGap.Range!, endBoundaryGap.Range!]);

        scannerMoq
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(baseScanResult));
        scanAugmenterMoq
            .Setup(mock => mock.Augment(It.IsAny<TradeGapScanRequest>(), baseScanResult, It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(augmentedScanResult));
        healerMoq
            .Setup(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapHealRequest, CancellationToken>((request, _) => healRequests.Add(request))
            .Returns<TradeGapHealRequest, CancellationToken>((request, _) => new ValueTask<TradeGapHealResult>(CreateHealResult(request, TradeGapHealStatus.Full)));

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-12.jsonl"), "{}", CancellationToken.None);

            var exitCode = await sut.Heal(
                new CliOptions(CliMode.Heal, workDirectory, "Binance", "BTC-USDT", string.Empty, [new DateOnly(2026, 3, 12)]),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.Equal(2, healRequests.Count);
            Assert.Equal(100L, healRequests[0].MissingTradeIdStart);
            Assert.Equal(106L, healRequests[0].MissingTradeIdEnd);
            Assert.Equal([100L, 105L], healRequests[0].RequestedMissingTradeRanges.Select(static range => range.FirstTradeId).ToArray());
            Assert.Equal([101L, 106L], healRequests[0].RequestedMissingTradeRanges.Select(static range => range.LastTradeId).ToArray());
            Assert.Equal(103L, healRequests[1].MissingTradeIdStart);
            Assert.Equal(103L, healRequests[1].MissingTradeIdEnd);
            Assert.Equal([103L], healRequests[1].RequestedMissingTradeRanges.Select(static range => range.FirstTradeId).ToArray());
            Assert.Equal([103L], healRequests[1].RequestedMissingTradeRanges.Select(static range => range.LastTradeId).ToArray());
            Assert.Equal(relativePath, Assert.Single(healRequests[0].CandidateFiles).Path);
            Assert.Equal(relativePath, Assert.Single(healRequests[1].CandidateFiles).Path);
            Assert.Contains("start-boundary pass 1/2", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("interior pass 2/2", outputWriter.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task HealWithoutDatesProcessesMultipleFilesIndependently()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        var healRequests = new List<TradeGapHealRequest>();
        var scanRequests = new List<TradeGapScanRequest>();
        var firstRelativePath = Path.Combine("Binance", "BTC-USDT", "2026-03-12.jsonl");
        var secondRelativePath = Path.Combine("Binance", "BTC-USDT", "2026-03-13.jsonl");
        var firstGap = CreateGapWithRange("Binance", "BTC-USDT", 101, 102, firstRelativePath, 2, 3, "100", "103");
        var secondGap = CreateGapWithRange("Binance", "BTC-USDT", 201, 201, secondRelativePath, 5, 6, "200", "202");
        var firstScanResult = new TradeGapScanResult(
            1,
            4,
            0,
            [firstGap.Gap],
            [new TradeGapAffectedFile(firstRelativePath, new DateOnly(2026, 3, 12))],
            [firstGap.Range!]);
        var secondScanResult = new TradeGapScanResult(
            1,
            4,
            0,
            [secondGap.Gap],
            [new TradeGapAffectedFile(secondRelativePath, new DateOnly(2026, 3, 13))],
            [secondGap.Range!]);
        var sequence = new MockSequence();

        scannerMoq
            .InSequence(sequence)
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapScanRequest, CancellationToken>((request, _) => scanRequests.Add(request))
            .Returns(new ValueTask<TradeGapScanResult>(firstScanResult));
        scannerMoq
            .InSequence(sequence)
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapScanRequest, CancellationToken>((request, _) => scanRequests.Add(request))
            .Returns(new ValueTask<TradeGapScanResult>(secondScanResult));
        healerMoq
            .Setup(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapHealRequest, CancellationToken>((request, _) => healRequests.Add(request))
            .Returns<TradeGapHealRequest, CancellationToken>((request, _) => new ValueTask<TradeGapHealResult>(CreateHealResult(request, TradeGapHealStatus.Full)));

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-12.jsonl"), "{}", CancellationToken.None);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-13.jsonl"), "{}", CancellationToken.None);

            var exitCode = await sut.Heal(
                new CliOptions(CliMode.Heal, workDirectory, "Binance", "BTC-USDT", string.Empty, []),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.Equal(2, scanRequests.Count);
            Assert.Equal(firstRelativePath, Assert.Single(scanRequests[0].CandidateFiles).Path);
            Assert.Equal(secondRelativePath, Assert.Single(scanRequests[1].CandidateFiles).Path);
            Assert.Equal(2, healRequests.Count);
            Assert.Equal(firstRelativePath, Assert.Single(healRequests[0].CandidateFiles).Path);
            Assert.Equal(secondRelativePath, Assert.Single(healRequests[1].CandidateFiles).Path);
            Assert.Equal([101L], healRequests[0].RequestedMissingTradeRanges.Select(static range => range.FirstTradeId).ToArray());
            Assert.Equal([201L], healRequests[1].RequestedMissingTradeRanges.Select(static range => range.FirstTradeId).ToArray());
            Assert.Contains("File 1/2: scanning", outputWriter.ToString(), StringComparison.Ordinal);
            Assert.Contains("File 2/2: scanning", outputWriter.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task HealTreatsBoundaryMismatchAsFileLocalIssue()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        var healRequests = new List<TradeGapHealRequest>();
        var boundaryRelativePath = Path.Combine("Binance", "BTC-USDT", "2026-03-12.jsonl");
        var neighborRelativePath = Path.Combine("Binance", "BTC-USDT", "2026-03-13.jsonl");
        var boundaryGap = CreateGapWithRange(
            "Binance",
            "BTC-USDT",
            100,
            101,
            boundaryRelativePath,
            1,
            1,
            "102",
            "102");
        var noGapResult = new TradeGapScanResult(
            1,
            4,
            0,
            [],
            [
                new TradeGapAffectedFile(boundaryRelativePath, new DateOnly(2026, 3, 12)),
            ],
            []);
        var boundaryResult = new TradeGapScanResult(
            1,
            2,
            0,
            [boundaryGap.Gap],
            [new TradeGapAffectedFile(boundaryRelativePath, new DateOnly(2026, 3, 12))],
            [boundaryGap.Range!]);
        var sequence = new MockSequence();

        scannerMoq
            .InSequence(sequence)
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(boundaryResult));
        scannerMoq
            .InSequence(sequence)
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(noGapResult));
        healerMoq
            .Setup(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapHealRequest, CancellationToken>((request, _) => healRequests.Add(request))
            .Returns<TradeGapHealRequest, CancellationToken>((request, _) => new ValueTask<TradeGapHealResult>(CreateHealResult(request, TradeGapHealStatus.Full)));

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-12.jsonl"), "{}", CancellationToken.None);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-13.jsonl"), "{}", CancellationToken.None);

            var exitCode = await sut.Heal(
                new CliOptions(
                    CliMode.Heal,
                    workDirectory,
                    "Binance",
                    "BTC-USDT",
                    string.Empty,
                    [new DateOnly(2026, 3, 12), new DateOnly(2026, 3, 13)]),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            var request = Assert.Single(healRequests);
            Assert.Equal(boundaryRelativePath, Assert.Single(request.CandidateFiles).Path);
            Assert.Equal(100L, request.MissingTradeIdStart);
            Assert.Equal(101L, request.MissingTradeIdEnd);
            Assert.DoesNotContain(neighborRelativePath, request.CandidateFiles.Select(static file => file.Path));
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task HealTreatsInteriorGapAsFileLocalIssue()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        var healRequests = new List<TradeGapHealRequest>();
        var firstRelativePath = Path.Combine("Binance", "BTC-USDT", "2026-03-12.jsonl");
        var secondRelativePath = Path.Combine("Binance", "BTC-USDT", "2026-03-13.jsonl");
        var secondGap = CreateGapWithRange("Binance", "BTC-USDT", 201, 203, secondRelativePath, 5, 6, "200", "204");
        var firstScanResult = new TradeGapScanResult(1, 2, 0, [], [new TradeGapAffectedFile(firstRelativePath, new DateOnly(2026, 3, 12))], []);
        var secondScanResult = new TradeGapScanResult(1, 2, 0, [secondGap.Gap], [new TradeGapAffectedFile(secondRelativePath, new DateOnly(2026, 3, 13))], [secondGap.Range!]);
        var sequence = new MockSequence();

        scannerMoq
            .InSequence(sequence)
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(firstScanResult));
        scannerMoq
            .InSequence(sequence)
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(secondScanResult));
        healerMoq
            .Setup(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapHealRequest, CancellationToken>((request, _) => healRequests.Add(request))
            .Returns<TradeGapHealRequest, CancellationToken>((request, _) => new ValueTask<TradeGapHealResult>(CreateHealResult(request, TradeGapHealStatus.Full)));

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-12.jsonl"), "{}", CancellationToken.None);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-13.jsonl"), "{}", CancellationToken.None);

            var exitCode = await sut.Heal(
                new CliOptions(CliMode.Heal, workDirectory, "Binance", "BTC-USDT", string.Empty, []),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            var request = Assert.Single(healRequests);
            Assert.Equal(secondRelativePath, Assert.Single(request.CandidateFiles).Path);
            Assert.Equal(201L, request.MissingTradeIdStart);
            Assert.Equal(203L, request.MissingTradeIdEnd);
            Assert.DoesNotContain(firstRelativePath, request.CandidateFiles.Select(static file => file.Path));
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task HealReportsMixedMultiFileTotalsWhenOnlyOneFileHasGaps()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        var firstRelativePath = Path.Combine("Binance", "BTC-USDT", "2026-03-12.jsonl");
        var secondRelativePath = Path.Combine("Binance", "BTC-USDT", "2026-03-13.jsonl");
        var secondGap = CreateGapWithRange("Binance", "BTC-USDT", 201, 201, secondRelativePath, 5, 6, "200", "202");
        var firstScanResult = new TradeGapScanResult(1, 3, 0, [], [new TradeGapAffectedFile(firstRelativePath, new DateOnly(2026, 3, 12))], []);
        var secondScanResult = new TradeGapScanResult(1, 4, 0, [secondGap.Gap], [new TradeGapAffectedFile(secondRelativePath, new DateOnly(2026, 3, 13))], [secondGap.Range!]);
        var sequence = new MockSequence();

        scannerMoq
            .InSequence(sequence)
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(firstScanResult));
        scannerMoq
            .InSequence(sequence)
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(secondScanResult));
        healerMoq
            .Setup(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()))
            .Returns<TradeGapHealRequest, CancellationToken>((request, _) => new ValueTask<TradeGapHealResult>(CreateHealResult(request, TradeGapHealStatus.Full)));

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-12.jsonl"), "{}", CancellationToken.None);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-13.jsonl"), "{}", CancellationToken.None);

            var exitCode = await sut.Heal(
                new CliOptions(CliMode.Heal, workDirectory, "Binance", "BTC-USDT", string.Empty, []),
                outputWriter,
                CancellationToken.None);

            var output = outputWriter.ToString();
            Assert.Equal(0, exitCode);
            Assert.Equal("2", FindLineValue(output, "Files scanned:"));
            Assert.Equal("7", FindLineValue(output, "Trades scanned:"));
            Assert.Equal("1", FindLineValue(output, "Gaps found:"));
            Assert.Equal("1", FindLineValue(output, "Gaps healed full:"));
            Assert.Equal("0", FindLineValue(output, "Gaps healed partial:"));
            Assert.Equal("0", FindLineValue(output, "Gaps unchanged:"));
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task HealDoesNotDispatchBoundaryRepairWhenAugmentedScanFindsNoGaps()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = new Mock<ITradeGapScanAugmenter>(MockBehavior.Strict);
        var scanResult = new TradeGapScanResult(
            1,
            2,
            0,
            [],
            [new TradeGapAffectedFile(Path.Combine("Binance", "BTC-USDT", "2026-03-12.jsonl"), new DateOnly(2026, 3, 12))],
            []);

        scannerMoq
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(scanResult));
        scanAugmenterMoq
            .Setup(mock => mock.Augment(It.IsAny<TradeGapScanRequest>(), scanResult, It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(scanResult));

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-12.jsonl"), "{}", CancellationToken.None);

            var exitCode = await sut.Heal(
                new CliOptions(CliMode.Heal, workDirectory, "Binance", "BTC-USDT", string.Empty, [new DateOnly(2026, 3, 12)]),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            healerMoq.Verify(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task HealPropagatesBoundaryResolutionFailureFromAugmenter()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = new Mock<ITradeGapScanAugmenter>(MockBehavior.Strict);
        var scanResult = new TradeGapScanResult(
            1,
            2,
            0,
            [],
            [new TradeGapAffectedFile(Path.Combine("Binance", "BTC-USDT", "2026-03-12.jsonl"), new DateOnly(2026, 3, 12))],
            []);

        scannerMoq
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(scanResult));
        scanAugmenterMoq
            .Setup(mock => mock.Augment(It.IsAny<TradeGapScanRequest>(), scanResult, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TradeDayBoundaryVerificationException(new ExchangeId("Binance"), Instrument.Parse("BTC-USDT"), new DateOnly(2026, 3, 12), 123));

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-03-12.jsonl"), "{}", CancellationToken.None);

            await Assert.ThrowsAsync<TradeDayBoundaryVerificationException>(() => sut.Heal(
                new CliOptions(CliMode.Heal, workDirectory, "Binance", "BTC-USDT", string.Empty, [new DateOnly(2026, 3, 12)]),
                outputWriter,
                CancellationToken.None));

            healerMoq.Verify(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()), Times.Never);
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
            Directory.CreateDirectory(Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT"));

            var exception = await Assert.ThrowsAsync<ArgumentException>(() => sut.Scan(
                new CliOptions(CliMode.Scan, workDirectory, "Binance", "BTC-USDT", string.Empty, [new DateOnly(2026, 4, 9)]),
                outputWriter,
                CancellationToken.None));

            Assert.Contains("exchange 'Binance'", exception.Message, StringComparison.Ordinal);
            Assert.Contains("instrument 'BTC-USDT'", exception.Message, StringComparison.Ordinal);
            Assert.Contains("requested date(s) [2026-04-09]", exception.Message, StringComparison.Ordinal);
            Assert.Contains("missing date(s) [2026-04-09]", exception.Message, StringComparison.Ordinal);
            Assert.Contains($"root directory '{Path.Combine(Path.GetFullPath(workDirectory), "trade-data")}'", exception.Message, StringComparison.Ordinal);
            Assert.Contains(
                TradeLocalDailyFilePath.Build(Path.Combine(Path.GetFullPath(workDirectory), "trade-data"), new ExchangeId("Binance"), Instrument.Parse("BTC-USDT"), new DateOnly(2026, 4, 9)),
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
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");
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
    public async Task HealWithRequestedMissingDateBootstrapsFileAndRunsNormalHealFlow()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        var bootstrapperMoq = new Mock<ITradeDayFileBootstrapper>(MockBehavior.Strict);
        var relativePath = Path.Combine("Binance", "BTC-USDT", "2026-04-09.jsonl");
        var fullPath = Path.Combine(instrumentDirectory, "2026-04-09.jsonl");
        var gap = CreateGapWithRange("Binance", "BTC-USDT", 1002, 1999, relativePath, 1, 1, "1001", "1001", "2000");

        bootstrapperMoq
            .Setup(mock => mock.Bootstrap(It.IsAny<string>(), It.IsAny<ExchangeId>(), It.IsAny<Instrument>(), new DateOnly(2026, 4, 9), It.IsAny<CancellationToken>()))
            .Callback<string, ExchangeId, Instrument, DateOnly, CancellationToken>((rootDirectory, exchange, instrument, utcDate, _) =>
            {
                var directory = Path.Combine(rootDirectory, exchange.ToString(), instrument.ToString());
                Directory.CreateDirectory(directory);
                var anchorTrade = new[]
                {
                    Trade(exchange.ToString(), instrument.ToString(), "1001", $"{utcDate:yyyy-MM-dd}T00:00:00Z", 100m, 0.25m),
                };
                var payload = string.Join(Environment.NewLine, anchorTrade.Select(static trade => JsonSerializer.Serialize(trade))) + Environment.NewLine;
                File.WriteAllText(Path.Combine(directory, $"{utcDate:yyyy-MM-dd}.jsonl"), payload);
            })
            .Returns<string, ExchangeId, Instrument, DateOnly, CancellationToken>((rootDirectory, exchange, instrument, utcDate, _) =>
            {
                var bootstrappedFullPath = TradeLocalDailyFilePath.Build(rootDirectory, exchange, instrument, utcDate);
                return ValueTask.FromResult(new TradeGapAffectedFile(Path.GetRelativePath(rootDirectory, bootstrappedFullPath), utcDate));
            });
        scannerMoq
            .Setup(mock => mock.Scan(It.Is<TradeGapScanRequest>(request => request.CandidateFiles.Count == 1 && request.CandidateFiles[0].Path == relativePath), It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(
                new TradeGapScanResult(
                    1,
                    1,
                    0,
                    [gap.Gap],
                    [new TradeGapAffectedFile(relativePath, new DateOnly(2026, 4, 9))],
                    [gap.Range!])));
        healerMoq
            .Setup(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()))
            .Returns<TradeGapHealRequest, CancellationToken>((request, _) => new ValueTask<TradeGapHealResult>(CreateHealResult(request, TradeGapHealStatus.Full)));

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object, bootstrapperMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);

            var exitCode = await sut.Heal(
                new CliOptions(CliMode.Heal, workDirectory, "Binance", "BTC-USDT", string.Empty, [new DateOnly(2026, 4, 9)]),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.True(File.Exists(fullPath));
            Assert.Contains("Bootstrapping missing requested UTC day '2026-04-09'", outputWriter.ToString(), StringComparison.Ordinal);
            bootstrapperMoq.Verify(mock => mock.Bootstrap(It.IsAny<string>(), It.IsAny<ExchangeId>(), It.IsAny<Instrument>(), new DateOnly(2026, 4, 9), It.IsAny<CancellationToken>()), Times.Once);
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Once);
            healerMoq.Verify(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()), Times.Once);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task HealWithExistingExplicitDateKeepsCurrentBehaviorWithoutBootstrap()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        var bootstrapperMoq = new Mock<ITradeDayFileBootstrapper>(MockBehavior.Strict);
        var relativePath = Path.Combine("Binance", "BTC-USDT", "2026-04-09.jsonl");
        var gap = CreateGapWithRange("Binance", "BTC-USDT", 101, 102, relativePath, 1, 2, "100", "103");

        scannerMoq
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Returns(new ValueTask<TradeGapScanResult>(
                new TradeGapScanResult(
                    1,
                    2,
                    0,
                    [gap.Gap],
                    [new TradeGapAffectedFile(relativePath, new DateOnly(2026, 4, 9))],
                    [gap.Range!])));
        healerMoq
            .Setup(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()))
            .Returns<TradeGapHealRequest, CancellationToken>((request, _) => new ValueTask<TradeGapHealResult>(CreateHealResult(request, TradeGapHealStatus.Full)));

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object, bootstrapperMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-04-09.jsonl"), "{}", CancellationToken.None);

            var exitCode = await sut.Heal(
                new CliOptions(CliMode.Heal, workDirectory, "Binance", "BTC-USDT", string.Empty, [new DateOnly(2026, 4, 9)]),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            bootstrapperMoq.Verify(mock => mock.Bootstrap(It.IsAny<string>(), It.IsAny<ExchangeId>(), It.IsAny<Instrument>(), It.IsAny<DateOnly>(), It.IsAny<CancellationToken>()), Times.Never);
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Once);
            healerMoq.Verify(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()), Times.Once);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task HealWithMixedExplicitDatesBootstrapsOnlyMissingFiles()
    {
        var workDirectory = CreateTempRoot();
        var instrumentDirectory = Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT");
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        var bootstrapperMoq = new Mock<ITradeDayFileBootstrapper>(MockBehavior.Strict);
        var firstRelativePath = Path.Combine("Binance", "BTC-USDT", "2026-04-09.jsonl");
        var secondRelativePath = Path.Combine("Binance", "BTC-USDT", "2026-04-10.jsonl");
        var capturedScanPaths = new List<string>();

        bootstrapperMoq
            .Setup(mock => mock.Bootstrap(It.IsAny<string>(), It.IsAny<ExchangeId>(), It.IsAny<Instrument>(), new DateOnly(2026, 4, 10), It.IsAny<CancellationToken>()))
            .Callback<string, ExchangeId, Instrument, DateOnly, CancellationToken>((rootDirectory, exchange, instrument, utcDate, _) =>
            {
                var directory = Path.Combine(rootDirectory, exchange.ToString(), instrument.ToString());
                Directory.CreateDirectory(directory);
                File.WriteAllText(
                    Path.Combine(directory, $"{utcDate:yyyy-MM-dd}.jsonl"),
                    JsonSerializer.Serialize(Trade(exchange.ToString(), instrument.ToString(), "200", $"{utcDate:yyyy-MM-dd}T00:00:00Z", 101m, 0.5m)) + Environment.NewLine);
            })
            .Returns<string, ExchangeId, Instrument, DateOnly, CancellationToken>((rootDirectory, exchange, instrument, utcDate, _) =>
            {
                var bootstrappedFullPath = TradeLocalDailyFilePath.Build(rootDirectory, exchange, instrument, utcDate);
                return ValueTask.FromResult(new TradeGapAffectedFile(Path.GetRelativePath(rootDirectory, bootstrappedFullPath), utcDate));
            });
        scannerMoq
            .Setup(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()))
            .Callback<TradeGapScanRequest, CancellationToken>((request, _) => capturedScanPaths.Add(Assert.Single(request.CandidateFiles).Path))
            .Returns<TradeGapScanRequest, CancellationToken>((request, _) =>
            {
                var candidateFile = Assert.Single(request.CandidateFiles);
                var gap = candidateFile.Path.EndsWith("2026-04-09.jsonl", StringComparison.Ordinal)
                    ? CreateGapWithRange("Binance", "BTC-USDT", 101, 102, firstRelativePath, 1, 2, "100", "103")
                    : CreateGapWithRange("Binance", "BTC-USDT", 201, 202, secondRelativePath, 1, 2, "200", "203");
                return new ValueTask<TradeGapScanResult>(
                    new TradeGapScanResult(
                        1,
                        2,
                        0,
                        [gap.Gap],
                        [new TradeGapAffectedFile(candidateFile.Path, candidateFile.TradingDay)],
                        [gap.Range!]));
            });
        healerMoq
            .Setup(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()))
            .Returns<TradeGapHealRequest, CancellationToken>((request, _) => new ValueTask<TradeGapHealResult>(CreateHealResult(request, TradeGapHealStatus.Full)));

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object, bootstrapperMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            Directory.CreateDirectory(instrumentDirectory);
            await File.WriteAllTextAsync(Path.Combine(instrumentDirectory, "2026-04-09.jsonl"), "{}", CancellationToken.None);

            var exitCode = await sut.Heal(
                new CliOptions(
                    CliMode.Heal,
                    workDirectory,
                    "Binance",
                    "BTC-USDT",
                    string.Empty,
                    [
                        new DateOnly(2026, 4, 9),
                        new DateOnly(2026, 4, 10),
                    ]),
                outputWriter,
                CancellationToken.None);

            Assert.Equal(0, exitCode);
            Assert.Equal([firstRelativePath, secondRelativePath], capturedScanPaths);
            bootstrapperMoq.Verify(mock => mock.Bootstrap(It.IsAny<string>(), It.IsAny<ExchangeId>(), It.IsAny<Instrument>(), new DateOnly(2026, 4, 10), It.IsAny<CancellationToken>()), Times.Once);
            bootstrapperMoq.Verify(mock => mock.Bootstrap(It.IsAny<string>(), It.IsAny<ExchangeId>(), It.IsAny<Instrument>(), new DateOnly(2026, 4, 9), It.IsAny<CancellationToken>()), Times.Never);
            healerMoq.Verify(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task HealWithRequestedMissingDateFailsLoudlyWhenBootstrapCannotResolveAnchor()
    {
        var workDirectory = CreateTempRoot();
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        var bootstrapperMoq = new Mock<ITradeDayFileBootstrapper>(MockBehavior.Strict);
        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object, bootstrapperMoq.Object);
        using var outputWriter = new StringWriter();

        bootstrapperMoq
            .Setup(mock => mock.Bootstrap(It.IsAny<string>(), It.IsAny<ExchangeId>(), It.IsAny<Instrument>(), new DateOnly(2026, 4, 9), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Unable to bootstrap missing trade day file 'Binance\\BTC-USDT\\2026-04-09.jsonl' because no Binance anchor trade could be resolved for UTC day 2026-04-09."));

        try
        {
            Directory.CreateDirectory(Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT"));

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => sut.Heal(
                new CliOptions(CliMode.Heal, workDirectory, "Binance", "BTC-USDT", string.Empty, [new DateOnly(2026, 4, 9)]),
                outputWriter,
                CancellationToken.None));

            Assert.Equal("Unable to bootstrap missing trade day file 'Binance\\BTC-USDT\\2026-04-09.jsonl' because no Binance anchor trade could be resolved for UTC day 2026-04-09.", exception.Message);
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Never);
            healerMoq.Verify(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task ScanWithoutDatesFailsExplicitlyWhenTradeDataStructureIsMissing()
    {
        var workDirectory = CreateTempRoot();
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();

        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            var exception = await Assert.ThrowsAsync<ArgumentException>(() => sut.Scan(
                new CliOptions(CliMode.Scan, workDirectory, "Binance", "BTC-USDT", string.Empty, []),
                outputWriter,
                CancellationToken.None));

            Assert.Contains("<workDir>\\trade-data\\<exchange>\\<instrument>\\yyyy-MM-dd.jsonl", exception.Message, StringComparison.Ordinal);
            Assert.Contains(Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT"), exception.Message, StringComparison.Ordinal);
            Assert.Contains(Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT", "2026-03-28.jsonl"), exception.Message, StringComparison.Ordinal);
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        }
        finally
        {
            DeleteDirectoryIfExists(workDirectory);
        }
    }

    [Fact]
    public async Task HealWithoutDatesFailsExplicitlyWhenTradeDataStructureIsMissing()
    {
        var workDirectory = CreateTempRoot();
        var scannerMoq = new Mock<ITradeGapScanner>(MockBehavior.Strict);
        var healerMoq = new Mock<ITradeGapHealer>(MockBehavior.Strict);
        var scanAugmenterMoq = CreatePassThroughScanAugmenterMock();
        var sut = new QuantaCandleRunner(scannerMoq.Object, healerMoq.Object, scanAugmenterMoq.Object);
        using var outputWriter = new StringWriter();

        try
        {
            var exception = await Assert.ThrowsAsync<ArgumentException>(() => sut.Heal(
                new CliOptions(CliMode.Heal, workDirectory, "Binance", "BTC-USDT", string.Empty, []),
                outputWriter,
                CancellationToken.None));

            Assert.Contains("<workDir>\\trade-data\\<exchange>\\<instrument>\\yyyy-MM-dd.jsonl", exception.Message, StringComparison.Ordinal);
            Assert.Contains(Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT"), exception.Message, StringComparison.Ordinal);
            Assert.Contains(Path.Combine(workDirectory, "trade-data", "Binance", "BTC-USDT", "2026-03-28.jsonl"), exception.Message, StringComparison.Ordinal);
            scannerMoq.Verify(mock => mock.Scan(It.IsAny<TradeGapScanRequest>(), It.IsAny<CancellationToken>()), Times.Never);
            healerMoq.Verify(mock => mock.Heal(It.IsAny<TradeGapHealRequest>(), It.IsAny<CancellationToken>()), Times.Never);
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

    private static (TradeGap Gap, TradeGapAffectedRange? Range) CreateGapWithRange(
        string exchange,
        string instrument,
        long missingTradeIdStart,
        long missingTradeIdEnd,
        string relativePath,
        int fromLine,
        int toLine,
        string fromTradeId,
        string toRangeTradeId,
        string? toInclusiveTradeId = null)
    {
        var fromTimestamp = new DateTimeOffset(2026, 3, 12, 0, 0, Math.Max(fromLine, 1), TimeSpan.Zero);
        var toTimestamp = new DateTimeOffset(2026, 3, 12, 0, 0, Math.Max(toLine, 1) + 1, TimeSpan.Zero);
        var gap = TradeGap
            .CreateOpen(
                Guid.NewGuid(),
                new ExchangeId(exchange),
                Instrument.Parse(instrument),
                new TradeWatermark(fromTradeId, fromTimestamp),
                toTimestamp)
            .ToBounded(
                new TradeWatermark(toInclusiveTradeId ?? toRangeTradeId, toTimestamp),
                new MissingTradeIdRange(missingTradeIdStart, missingTradeIdEnd));
        var range = new TradeGapAffectedRange(
            new TradeWatermark(fromTradeId, fromTimestamp),
            new TradeWatermark(toRangeTradeId, toTimestamp),
            new TradeGapBoundaryLocation(relativePath, fromLine),
            new TradeGapBoundaryLocation(relativePath, toLine));
        return (gap, range);
    }

    private static TradeGapHealResult CreateHealResult(TradeGapHealRequest request, TradeGapHealStatus outcome)
    {
        var result = new TradeGapHealResult(
            request.Exchange,
            request.Symbol,
            outcome,
            new MissingTradeIdRange(request.MissingTradeIdStart, request.MissingTradeIdEnd),
            request.MissingTradeIdEnd - request.MissingTradeIdStart + 1 > int.MaxValue
                ? throw new InvalidOperationException("Requested range is unexpectedly large.")
                : (int)(request.MissingTradeIdEnd - request.MissingTradeIdStart + 1),
            request.MissingTradeIdEnd - request.MissingTradeIdStart + 1 > int.MaxValue
                ? throw new InvalidOperationException("Requested range is unexpectedly large.")
                : (int)(request.MissingTradeIdEnd - request.MissingTradeIdStart + 1),
            outcome == TradeGapHealStatus.Full,
            [],
            [],
            request.CandidateFiles,
            []);
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

    private static string FindLineValue(string output, string prefix)
    {
        var result = output
            .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries)
            .Select(static line => line.TrimEnd())
            .Where(line => line.TrimStart().StartsWith(prefix, StringComparison.Ordinal))
            .Select(line => line[(line.IndexOf(prefix, StringComparison.Ordinal) + prefix.Length)..].Trim())
            .Single();
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
            isBuyerMaker = false,
        };

        return result;
    }

    private static async Task WriteTradeFileAsync(string workDirectory, string exchange, string instrument, string fileName, params object[] trades)
    {
        var directory = Path.Combine(workDirectory, "trade-data", exchange, instrument);
        Directory.CreateDirectory(directory);

        var path = Path.Combine(directory, fileName);
        var lines = trades.Select(static trade => JsonSerializer.Serialize(trade)).ToArray();
        var payload = string.Join(Environment.NewLine, lines) + Environment.NewLine;
        await File.WriteAllTextAsync(path, payload, CancellationToken.None).ConfigureAwait(false);
    }
}
