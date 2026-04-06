using LogMachina;

using Moq;

using QuantaCandle.Infra.Pipeline;

namespace QuantaCandle.Infra.Tests.Pipeline;

public sealed class TradePipelineStatsTests
{
    [Fact]
    public void Tracks_received_written_batches_and_min_max()
    {
        TradePipelineStats stats = new TradePipelineStats();

        DateTimeOffset t1 = new DateTimeOffset(2026, 3, 12, 0, 0, 2, TimeSpan.Zero);
        DateTimeOffset t2 = new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero);
        DateTimeOffset t3 = new DateTimeOffset(2026, 3, 12, 0, 0, 1, TimeSpan.Zero);

        stats.OnTradeReceived(t1);
        stats.OnTradeReceived(t2);
        stats.OnTradeReceived(t3);

        stats.OnBatchFlushed(insertedCount: 2);
        stats.OnBatchFlushed(insertedCount: 1);

        TradePipelineStatsSnapshot snapshot = stats.GetSnapshot();
        Assert.Equal(3, snapshot.TradesReceived);
        Assert.Equal(3, snapshot.TradesWritten);
        Assert.Equal(0, snapshot.DuplicatesDropped);
        Assert.Equal(2, snapshot.BatchesFlushed);
        Assert.Equal(t2, snapshot.MinTimestamp);
        Assert.Equal(t1, snapshot.MaxTimestamp);
    }

    [Fact]
    public void ShutdownStatisticsUseTheStandardizedSnapshotMessage()
    {
        var stats = new TradePipelineStats();
        stats.OnTradeReceived(new DateTimeOffset(2026, 3, 12, 0, 0, 0, TimeSpan.Zero));
        stats.OnBatchFlushed(insertedCount: 1);
        stats.OnDuplicateDropped();

        var logMoq = new Mock<ILogMachina<TradePipelineStats>>();

        var msg = TradePipelineStatsLogFormatter.Format(stats.GetSnapshot());
        logMoq.Object.Info(msg);

        logMoq.Verify(mock => mock.Info(
                It.Is<string>(message =>
                    message.Contains("Trades received:", StringComparison.Ordinal)
                    && message.Contains("Trades written:", StringComparison.Ordinal)
                    && message.Contains("Duplicates dropped:", StringComparison.Ordinal)
                    && message.Contains("Batches flushed:", StringComparison.Ordinal)
                    && message.Contains("Min timestamp:", StringComparison.Ordinal)
                    && message.Contains("Max timestamp:", StringComparison.Ordinal))),
            Times.Once);
    }

}
