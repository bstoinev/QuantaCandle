using System;
using QuantaCandle.Service.Pipeline;

namespace QuantaCandle.Service.Tests.Pipeline;

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
}
