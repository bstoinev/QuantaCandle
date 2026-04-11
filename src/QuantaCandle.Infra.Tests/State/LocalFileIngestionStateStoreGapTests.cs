using System.Collections.Concurrent;
using System.Reflection;

using Moq;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Tests.State;

/// <summary>
/// Verifies compact runtime gap retention in the local-file ingestion state store.
/// </summary>
public sealed class LocalFileIngestionStateStoreGapTests
{
    [Fact]
    public async Task RecordGapAsyncKeepsOnlyLatestSnapshotPerGapId()
    {
        var store = new LocalFileIngestionStateStore(CreateTempDirectory(), CreateClockMoq().Object);
        var gapId = Guid.NewGuid();
        var openGap = TradeGap.CreateOpen(
            gapId,
            new ExchangeId("Stub"),
            Instrument.Parse("BTC-USDT"),
            new TradeWatermark("1", new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero)),
            new DateTimeOffset(2026, 3, 12, 0, 0, 5, TimeSpan.Zero));
        var boundedGap = openGap.ToBounded(
            new TradeWatermark("4", new DateTimeOffset(2026, 3, 12, 0, 0, 8, TimeSpan.Zero)),
            new MissingTradeIdRange(2, 3));

        await store.RecordGapAsync(openGap, TestContext.Current.CancellationToken);
        await store.RecordGapAsync(boundedGap, TestContext.Current.CancellationToken);

        var gaps = await store.GetGapsAsync(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), TestContext.Current.CancellationToken);
        var gap = Assert.Single(gaps);

        Assert.Equal(TradeGapStatus.Bounded, gap.Status);
        Assert.Equal(gapId, gap.GapId);
        Assert.Equal(new MissingTradeIdRange(2, 3), gap.MissingTradeIds);
    }

    [Fact]
    public void LocalFileIngestionStateStoreUsesConcurrentDictionaryGapCacheInsteadOfAppendOnlyLists()
    {
        var field = typeof(LocalFileIngestionStateStore).GetField("gapsByInstrument", BindingFlags.Instance | BindingFlags.NonPublic);

        Assert.NotNull(field);
        Assert.Equal(
            typeof(ConcurrentDictionary<(ExchangeId Exchange, Instrument Symbol), ConcurrentDictionary<Guid, TradeGap>>),
            field!.FieldType);
    }

    private static Mock<IClock> CreateClockMoq()
    {
        var result = new Mock<IClock>();
        result
            .SetupGet(mock => mock.UtcNow)
            .Returns(new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero));
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
}
