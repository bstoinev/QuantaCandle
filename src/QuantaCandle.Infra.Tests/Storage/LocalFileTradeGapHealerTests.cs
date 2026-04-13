using LogMachina;

using Moq;

using QuantaCandle.Core.Trading;
using QuantaCandle.Infra;
using QuantaCandle.Infra.Storage;

namespace QuantaCandle.Infra.Tests.Storage;

/// <summary>
/// Verifies streamed local JSONL trade gap healing behavior.
/// </summary>
public sealed class LocalFileTradeGapHealerTests
{
    private static readonly ExchangeId BinanceExchange = new("Binance");

    [Fact]
    public async Task InsertsMissingStartBoundaryViaStreamingSplice()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(102), CreateTrade(103)]);

            var fetchClient = new RecordingTradeGapFetchClient([CreateTrade(100), CreateTrade(101)]);
            var healer = new LocalFileTradeGapHealer(fetchClient, CreateLog());
            var result = await healer.Heal(CreateRequest(rootDirectory, 100, 101, relativePath), CancellationToken.None);

            Assert.Equal(TradeGapHealStatus.Full, result.Outcome);
            Assert.Equal(2, result.FetchedTradeCount);
            Assert.Equal(2, result.InsertedTradeCount);
            Assert.True(result.HasFullRequestedCoverage);
            Assert.Empty(result.UnresolvedTradeRanges);

            var trades = await ReadTradesAsync(rootDirectory, relativePath);
            Assert.Equal(["100", "101", "102", "103"], trades.Select(static trade => trade.Key.TradeId).ToArray());
            Assert.Equal([(100L, 101L)], fetchClient.RequestedRanges);
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task InsertsMissingEndBoundaryViaStreamingSplice()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(101)]);

            var fetchClient = new RecordingTradeGapFetchClient([CreateTrade(102), CreateTrade(103)]);
            var healer = new LocalFileTradeGapHealer(fetchClient, CreateLog());
            var result = await healer.Heal(CreateRequest(rootDirectory, 102, 103, relativePath), CancellationToken.None);

            Assert.Equal(TradeGapHealStatus.Full, result.Outcome);
            Assert.Equal(2, result.InsertedTradeCount);

            var trades = await ReadTradesAsync(rootDirectory, relativePath);
            Assert.Equal(["100", "101", "102", "103"], trades.Select(static trade => trade.Key.TradeId).ToArray());
            Assert.Equal([(102L, 103L)], fetchClient.RequestedRanges);
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task InsertsInteriorGapViaStreamingSplice()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(104)]);

            var healer = new LocalFileTradeGapHealer(
                new RecordingTradeGapFetchClient([CreateTrade(101), CreateTrade(102), CreateTrade(103)]),
                CreateLog());
            var result = await healer.Heal(CreateRequest(rootDirectory, 101, 103, relativePath), CancellationToken.None);

            Assert.Equal(TradeGapHealStatus.Full, result.Outcome);

            var trades = await ReadTradesAsync(rootDirectory, relativePath);
            Assert.Equal(["100", "101", "102", "103", "104"], trades.Select(static trade => trade.Key.TradeId).ToArray());
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task PartialCoverageStillReportsUnresolvedRangesCorrectly()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(105)]);

            var healer = new LocalFileTradeGapHealer(new RecordingTradeGapFetchClient([CreateTrade(101), CreateTrade(102)]), CreateLog());
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
    public async Task EqualFetchedAndLocalTradeIdFailsLoudly()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(103)]);

            var healer = new LocalFileTradeGapHealer(
                new RecordingTradeGapFetchClient([CreateTrade(101), CreateTrade(103)]),
                CreateLog());
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await healer.Heal(CreateRequest(rootDirectory, 101, 103, relativePath), CancellationToken.None));

            Assert.Contains("equals existing local trade id", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task BoundaryAndInteriorGapsStillWorkAcrossExactStreamingPasses()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(102), CreateTrade(104), CreateTrade(107)]);

            var healer = new LocalFileTradeGapHealer(new RecordingTradeGapFetchClient([CreateTrade(100), CreateTrade(101)]), CreateLog());
            _ = await healer.Heal(CreateRequest(rootDirectory, 100, 101, relativePath), CancellationToken.None);
            healer = new LocalFileTradeGapHealer(new RecordingTradeGapFetchClient([CreateTrade(103)]), CreateLog());
            _ = await healer.Heal(CreateRequest(rootDirectory, 103, 103, relativePath), CancellationToken.None);
            healer = new LocalFileTradeGapHealer(new RecordingTradeGapFetchClient([CreateTrade(105), CreateTrade(106)]), CreateLog());
            _ = await healer.Heal(CreateRequest(rootDirectory, 105, 106, relativePath), CancellationToken.None);

            var trades = await ReadTradesAsync(rootDirectory, relativePath);
            Assert.Equal(["100", "101", "102", "103", "104", "105", "106", "107"], trades.Select(static trade => trade.Key.TradeId).ToArray());
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task MultipleDisjointRangesFetchExactRangesWithoutOuterEnvelope()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(102), CreateTrade(104)]);

            var fetchClient = new RecordingTradeGapFetchClient([
                CreateTrade(100),
                CreateTrade(101),
                CreateTrade(103),
                CreateTrade(105),
                CreateTrade(106),
            ]);
            var healer = new LocalFileTradeGapHealer(fetchClient, CreateLog());
            var result = await healer.Heal(
                CreateRequest(
                    rootDirectory,
                    100,
                    106,
                    relativePath,
                    [new MissingTradeIdRange(100, 101), new MissingTradeIdRange(103, 103), new MissingTradeIdRange(105, 106)]),
                CancellationToken.None);

            Assert.Equal(TradeGapHealStatus.Full, result.Outcome);
            Assert.Equal(5, result.FetchedTradeCount);
            Assert.Equal(5, result.InsertedTradeCount);
            Assert.Empty(result.UnresolvedTradeRanges);
            Assert.Equal([(100L, 101L), (103L, 103L), (105L, 106L)], fetchClient.RequestedRanges);

            var trades = await ReadTradesAsync(rootDirectory, relativePath);
            Assert.Equal(["100", "101", "102", "103", "104", "105", "106"], trades.Select(static trade => trade.Key.TradeId).ToArray());
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task FlushesStagingFileAfterEachFetchedPageBeforeCommit()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var checkpointKinds = new List<StagingCheckpointKind>();
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(104)]);

            var observer = new Mock<IStagingObserver>(MockBehavior.Strict);
            observer
                .Setup(mock => mock.OnCheckpoint(It.IsAny<string>(), It.IsAny<StagingCheckpointKind>(), It.IsAny<CancellationToken>()))
                .Returns<string, StagingCheckpointKind, CancellationToken>(async (tempPath, checkpointKind, cancellationToken) =>
                {
                    checkpointKinds.Add(checkpointKind);
                    if (checkpointKind == StagingCheckpointKind.Page && checkpointKinds.Count(kind => kind == StagingCheckpointKind.Page) == 1)
                    {
                        var stagedTrades = await ReadTradesFromFullPathAsync(tempPath);
                        var originalTrades = await ReadTradesAsync(rootDirectory, relativePath);
                        Assert.Equal(["100", "101", "102"], stagedTrades.Select(static trade => trade.Key.TradeId).ToArray());
                        Assert.Equal(["100", "104"], originalTrades.Select(static trade => trade.Key.TradeId).ToArray());
                    }
                });

            var healer = new LocalFileTradeGapHealer(
                new PagedRecordingTradeGapFetchClient([
                    [CreateTrade(101), CreateTrade(102)],
                    [CreateTrade(103)],
                ]),
                CreateLog(),
                observer.Object);
            var result = await healer.Heal(CreateRequest(rootDirectory, 101, 103, relativePath), CancellationToken.None);

            Assert.Equal(TradeGapHealStatus.Full, result.Outcome);
            Assert.Contains(StagingCheckpointKind.Page, checkpointKinds);
            Assert.Contains(StagingCheckpointKind.Commit, checkpointKinds);
            Assert.True(
                checkpointKinds.IndexOf(StagingCheckpointKind.Page)
                < checkpointKinds.IndexOf(StagingCheckpointKind.Commit));

            var trades = await ReadTradesAsync(rootDirectory, relativePath);
            Assert.Equal(["100", "101", "102", "103", "104"], trades.Select(static trade => trade.Key.TradeId).ToArray());
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task FlushesStagingFileWhenMissingRangeCompletesBeforeNextRangeStarts()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var checkpointKinds = new List<StagingCheckpointKind>();
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(102), CreateTrade(104)]);

            var observer = new Mock<IStagingObserver>(MockBehavior.Strict);
            observer
                .Setup(mock => mock.OnCheckpoint(It.IsAny<string>(), It.IsAny<StagingCheckpointKind>(), It.IsAny<CancellationToken>()))
                .Returns<string, StagingCheckpointKind, CancellationToken>(async (tempPath, checkpointKind, cancellationToken) =>
                {
                    checkpointKinds.Add(checkpointKind);
                    if (checkpointKind == StagingCheckpointKind.MissingRange && checkpointKinds.Count(kind => kind == StagingCheckpointKind.MissingRange) == 1)
                    {
                        var stagedTrades = await ReadTradesFromFullPathAsync(tempPath);
                        var originalTrades = await ReadTradesAsync(rootDirectory, relativePath);
                        Assert.Equal(["100", "101"], stagedTrades.Select(static trade => trade.Key.TradeId).ToArray());
                        Assert.Equal(["100", "102", "104"], originalTrades.Select(static trade => trade.Key.TradeId).ToArray());
                    }
                });

            var healer = new LocalFileTradeGapHealer(
                new RecordingTradeGapFetchClient([CreateTrade(101), CreateTrade(103)]),
                CreateLog(),
                observer.Object);
            var result = await healer.Heal(
                CreateRequest(
                    rootDirectory,
                    101,
                    103,
                    relativePath,
                    [new MissingTradeIdRange(101, 101), new MissingTradeIdRange(103, 103)]),
                CancellationToken.None);

            Assert.Equal(TradeGapHealStatus.Full, result.Outcome);
            Assert.Contains(StagingCheckpointKind.MissingRange, checkpointKinds);
            Assert.Contains(StagingCheckpointKind.Commit, checkpointKinds);
            Assert.True(
                checkpointKinds.IndexOf(StagingCheckpointKind.MissingRange)
                < checkpointKinds.IndexOf(StagingCheckpointKind.Commit));

            var trades = await ReadTradesAsync(rootDirectory, relativePath);
            Assert.Equal(["100", "101", "102", "103", "104"], trades.Select(static trade => trade.Key.TradeId).ToArray());
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task FetchedBatchInternalGapProducesWarningWithoutFailingHeal()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(106)]);

            var healer = new LocalFileTradeGapHealer(
                new RecordingTradeGapFetchClient([CreateTrade(101), CreateTrade(103), CreateTrade(104)]),
                CreateLog());
            var result = await healer.Heal(CreateRequest(rootDirectory, 101, 105, relativePath), CancellationToken.None);

            Assert.Equal(TradeGapHealStatus.Partial, result.Outcome);
            Assert.Contains(result.Warnings, static warning => warning.Contains("Unresolved range(s): [102, 105]", StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task DuplicateFetchedTradeIdFailsLoudly()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(103)]);

            var healer = new LocalFileTradeGapHealer(
                new RecordingTradeGapFetchClient([CreateTrade(101), CreateTrade(101), CreateTrade(102)]),
                CreateLog());
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await healer.Heal(CreateRequest(rootDirectory, 101, 102, relativePath), CancellationToken.None));

            Assert.Contains("duplicate trade id", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task ConflictingOverlapTradeFailsLoudly()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(103)]);
            var conflictingTrade = new TradeInfo(
                new TradeKey(BinanceExchange, Instrument.Parse("BTC-USDT"), "103"),
                CreateTimestamp(103),
                999m,
                1m);

            var healer = new LocalFileTradeGapHealer(
                new RecordingTradeGapFetchClient([CreateTrade(101), CreateTrade(102), conflictingTrade]),
                CreateLog());
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await healer.Heal(CreateRequest(rootDirectory, 101, 103, relativePath), CancellationToken.None));

            Assert.Contains("equals existing local trade id", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task LocalOutOfOrderTradeIdsFailLoudly()
    {
        var rootDirectory = CreateRootDirectory();
        try
        {
            var relativePath = Path.Combine("BTC-USDT", "2026-03-28.jsonl");
            await WriteTradesAsync(rootDirectory, relativePath, [CreateTrade(100), CreateTrade(104), CreateTrade(103)]);

            var healer = new LocalFileTradeGapHealer(
                new RecordingTradeGapFetchClient([CreateTrade(101), CreateTrade(102)]),
                CreateLog());
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await healer.Heal(CreateRequest(rootDirectory, 101, 102, relativePath), CancellationToken.None));

            Assert.Contains("not strictly ascending", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            DeleteRootDirectory(rootDirectory);
        }
    }

    [Fact]
    public async Task OriginalFileRemainsIntactOnHardFailure()
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
            var healer = new LocalFileTradeGapHealer(new RecordingTradeGapFetchClient([invalidTrade]), CreateLog());

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
    public async Task ImplementationNoLongerContainsFullLocalLoadHelpers()
    {
        var sourcePath = FindProjectFile("QuantaCandle.Infra", Path.Combine("Storage", "LocalFileTradeGapHealer.cs"));
        var source = await File.ReadAllTextAsync(sourcePath, CancellationToken.None);

        Assert.DoesNotContain("ReadExistingTradesAsync", source, StringComparison.Ordinal);
        Assert.DoesNotContain("MergeTrades(", source, StringComparison.Ordinal);
    }

    private static TradeGapHealRequest CreateRequest(
        string rootDirectory,
        long missingTradeIdStart,
        long missingTradeIdEnd,
        string relativePath,
        IReadOnlyList<MissingTradeIdRange>? requestedMissingTradeRanges = null)
    {
        var affectedFile = new TradeGapAffectedFile(relativePath, new DateOnly(2026, 3, 28));
        var affectedRange = new TradeGapAffectedRange(
            new TradeWatermark((missingTradeIdStart - 1).ToString(), CreateTimestamp(missingTradeIdStart - 1)),
            new TradeWatermark((missingTradeIdEnd + 1).ToString(), CreateTimestamp(missingTradeIdEnd + 1)),
            new TradeGapBoundaryLocation(relativePath, 1),
            new TradeGapBoundaryLocation(relativePath, 2));
        return new TradeGapHealRequest(rootDirectory, BinanceExchange, Instrument.Parse("BTC-USDT"), missingTradeIdStart, missingTradeIdEnd, [affectedFile], affectedRange, requestedMissingTradeRanges);
    }

    private static TradeInfo CreateTrade(long tradeId)
    {
        return new TradeInfo(
            new TradeKey(BinanceExchange, Instrument.Parse("BTC-USDT"), tradeId.ToString()),
            CreateTimestamp(tradeId),
            100m + tradeId,
            1m);
    }

    private static DateTimeOffset CreateTimestamp(long tradeId)
    {
        return new DateTimeOffset(2026, 3, 28, 0, 0, 0, TimeSpan.Zero).AddSeconds(tradeId);
    }

    private static async Task<IReadOnlyList<TradeInfo>> ReadTradesAsync(string rootDirectory, string relativePath)
    {
        return await TradeJsonlFile.ReadTrades(Path.Combine(rootDirectory, relativePath), CancellationToken.None);
    }

    private static async Task<IReadOnlyList<TradeInfo>> ReadTradesFromFullPathAsync(string fullPath)
    {
        var result = new List<TradeInfo>();

        await using var stream = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = new StreamReader(stream);

        while (true)
        {
            var line = await reader.ReadLineAsync(CancellationToken.None);
            if (line is null)
            {
                break;
            }

            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            result.Add(ParseTrade(line));
        }

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
        return new Mock<ILogMachina<LocalFileTradeGapHealer>>().Object;
    }

    private static TradeInfo ParseTrade(string line)
    {
        using var document = System.Text.Json.JsonDocument.Parse(line);
        var root = document.RootElement;
        var exchange = new ExchangeId(root.GetProperty("exchange").GetString() ?? string.Empty);
        var instrument = Instrument.Parse(root.GetProperty("instrument").GetString() ?? string.Empty);
        var tradeId = root.GetProperty("tradeId").GetString() ?? string.Empty;
        var timestamp = root.GetProperty("timestamp").GetDateTimeOffset();
        var price = root.GetProperty("price").GetDecimal();
        var quantity = root.GetProperty("quantity").GetDecimal();

        return new TradeInfo(new TradeKey(exchange, instrument, tradeId), timestamp, price, quantity);
    }

    private static string FindProjectFile(string projectDirectoryName, string relativePath)
    {
        var currentDirectory = new DirectoryInfo(AppContext.BaseDirectory);
        while (currentDirectory is not null)
        {
            var candidate = Path.Combine(currentDirectory.FullName, projectDirectoryName, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }

            var srcCandidate = Path.Combine(currentDirectory.FullName, "src", projectDirectoryName, relativePath);
            if (File.Exists(srcCandidate))
            {
                return srcCandidate;
            }

            currentDirectory = currentDirectory.Parent;
        }

        throw new InvalidOperationException($"Unable to locate source file '{projectDirectoryName}{Path.DirectorySeparatorChar}{relativePath}'.");
    }

    /// <summary>
    /// Returns a predefined fetched trade batch for healer tests.
    /// </summary>
    private sealed class RecordingTradeGapFetchClient(IReadOnlyList<TradeInfo> trades) : ITradeGapFetchClient
    {
        private readonly IReadOnlyList<TradeInfo> _trades = trades ?? throw new ArgumentNullException(nameof(trades));
        public List<(long Start, long End)> RequestedRanges { get; } = [];

        /// <summary>
        /// Returns the configured fetched trade batch without additional processing.
        /// </summary>
        public async ValueTask Fetch(
            Instrument instrument,
            long missingTradeIdStart,
            long missingTradeIdEnd,
            ITradeGapFetchedPageSink pageSink,
            ITradeGapProgressReporter? progressReporter,
            CancellationToken cancellationToken)
        {
            RequestedRanges.Add((missingTradeIdStart, missingTradeIdEnd));
            var pageTrades = _trades
                .Where(trade =>
                {
                    var tradeId = long.Parse(trade.Key.TradeId);
                    return tradeId >= missingTradeIdStart && tradeId <= missingTradeIdEnd;
                })
                .ToArray();
            await pageSink.AcceptPage(pageTrades, cancellationToken);
        }
    }

    /// <summary>
    /// Returns predefined pages for one requested range to exercise page-level staging flushes.
    /// </summary>
    private sealed class PagedRecordingTradeGapFetchClient(IReadOnlyList<IReadOnlyList<TradeInfo>> pages) : ITradeGapFetchClient
    {
        private readonly IReadOnlyList<IReadOnlyList<TradeInfo>> _pages = pages ?? throw new ArgumentNullException(nameof(pages));
        public List<(long Start, long End)> RequestedRanges { get; } = [];

        /// <summary>
        /// Streams the configured pages in order without changing the requested fetch boundaries.
        /// </summary>
        public async ValueTask Fetch(
            Instrument instrument,
            long missingTradeIdStart,
            long missingTradeIdEnd,
            ITradeGapFetchedPageSink pageSink,
            ITradeGapProgressReporter? progressReporter,
            CancellationToken cancellationToken)
        {
            RequestedRanges.Add((missingTradeIdStart, missingTradeIdEnd));
            foreach (var page in _pages)
            {
                var pageTrades = page
                    .Where(trade =>
                    {
                        var tradeId = long.Parse(trade.Key.TradeId);
                        return tradeId >= missingTradeIdStart && tradeId <= missingTradeIdEnd;
                    })
                    .ToArray();
                await pageSink.AcceptPage(pageTrades, cancellationToken);
            }
        }
    }
}
