namespace QuantaCandle.Infra.Pipeline;

public sealed class TradePipelineStats
{
    private long _tradesReceived;
    private long _tradesWritten;
    private long _duplicatesDropped;
    private long _batchesFlushed;

    private long _minUtcTicks;
    private long _maxUtcTicks;

    public TradePipelineStats()
    {
        _minUtcTicks = long.MaxValue;
        _maxUtcTicks = long.MinValue;
    }

    public void OnTradeReceived(DateTimeOffset timestamp)
    {
        Interlocked.Increment(ref _tradesReceived);
        UpdateMinMax(timestamp.UtcTicks);
    }

    public void OnBatchFlushed(int insertedCount)
    {
        Interlocked.Increment(ref _batchesFlushed);
        Interlocked.Add(ref _tradesWritten, insertedCount);
    }

    public void OnDuplicateDropped()
    {
        Interlocked.Increment(ref _duplicatesDropped);
    }

    public TradePipelineStatsSnapshot GetSnapshot()
    {
        var received = Interlocked.Read(ref _tradesReceived);
        var written = Interlocked.Read(ref _tradesWritten);
        var dropped = Interlocked.Read(ref _duplicatesDropped);
        var flushed = Interlocked.Read(ref _batchesFlushed);

        var min = Interlocked.Read(ref _minUtcTicks);
        var max = Interlocked.Read(ref _maxUtcTicks);

        DateTimeOffset? minTimestamp = min == long.MaxValue ? null : new DateTimeOffset(min, TimeSpan.Zero);
        DateTimeOffset? maxTimestamp = max == long.MinValue ? null : new DateTimeOffset(max, TimeSpan.Zero);

        return new TradePipelineStatsSnapshot(received, written, dropped, flushed, minTimestamp, maxTimestamp);
    }

    private void UpdateMinMax(long utcTicks)
    {
        var currentMin = Volatile.Read(ref _minUtcTicks);
        while (utcTicks < currentMin)
        {
            var previous = Interlocked.CompareExchange(ref _minUtcTicks, utcTicks, currentMin);
            if (previous == currentMin)
            {
                break;
            }

            currentMin = previous;
        }

        var currentMax = Volatile.Read(ref _maxUtcTicks);
        while (utcTicks > currentMax)
        {
            long previous = Interlocked.CompareExchange(ref _maxUtcTicks, utcTicks, currentMax);
            if (previous == currentMax)
            {
                break;
            }

            currentMax = previous;
        }
    }
}
