using System;
using System.Threading;

namespace QuantaCandle.Service.Pipeline;

public sealed class TradePipelineStats
{
    private long tradesReceived;
    private long tradesWritten;
    private long batchesFlushed;

    private long minUtcTicks;
    private long maxUtcTicks;

    public TradePipelineStats()
    {
        minUtcTicks = long.MaxValue;
        maxUtcTicks = long.MinValue;
    }

    public void OnTradeReceived(DateTimeOffset timestamp)
    {
        Interlocked.Increment(ref tradesReceived);
        UpdateMinMax(timestamp.UtcTicks);
    }

    public void OnBatchFlushed(int insertedCount)
    {
        Interlocked.Increment(ref batchesFlushed);
        Interlocked.Add(ref tradesWritten, insertedCount);
    }

    public TradePipelineStatsSnapshot GetSnapshot()
    {
        long received = Interlocked.Read(ref tradesReceived);
        long written = Interlocked.Read(ref tradesWritten);
        long flushed = Interlocked.Read(ref batchesFlushed);

        long min = Interlocked.Read(ref minUtcTicks);
        long max = Interlocked.Read(ref maxUtcTicks);

        DateTimeOffset? minTimestamp = min == long.MaxValue ? null : new DateTimeOffset(min, TimeSpan.Zero);
        DateTimeOffset? maxTimestamp = max == long.MinValue ? null : new DateTimeOffset(max, TimeSpan.Zero);

        return new TradePipelineStatsSnapshot(received, written, flushed, minTimestamp, maxTimestamp);
    }

    private void UpdateMinMax(long utcTicks)
    {
        long currentMin = Volatile.Read(ref minUtcTicks);
        while (utcTicks < currentMin)
        {
            long previous = Interlocked.CompareExchange(ref minUtcTicks, utcTicks, currentMin);
            if (previous == currentMin)
            {
                break;
            }

            currentMin = previous;
        }

        long currentMax = Volatile.Read(ref maxUtcTicks);
        while (utcTicks > currentMax)
        {
            long previous = Interlocked.CompareExchange(ref maxUtcTicks, utcTicks, currentMax);
            if (previous == currentMax)
            {
                break;
            }

            currentMax = previous;
        }
    }
}

